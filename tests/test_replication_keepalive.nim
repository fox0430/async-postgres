## Replication keepalive auto-reply E2E tests using the in-process mock server.
##
## Verifies that when `startReplication` is invoked with `autoKeepaliveReply = true`
## (the default), the library responds to `PrimaryKeepalive(replyRequested=true)`
## messages automatically, using the most recently received `XLogData.endLsn`
## (never the keepalive's `walEnd`). Also verifies the opt-out path.

import std/unittest

import ../async_postgres/[async_backend, pg_protocol, pg_replication]
import ../async_postgres/pg_connection {.all.}

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc buildCopyBothResponse(): seq[byte] =
  ## CopyBothResponse: format(1=binary) + numCols(0).
  var body: seq[byte]
  body.add(1'u8)
  body.addInt16(0'i16)
  buildBackendMsg('W', body)

proc buildCopyData(payload: openArray[byte]): seq[byte] =
  buildBackendMsg('d', payload)

proc buildXLogData(startLsn, endLsn, sendTime: int64, walData: seq[byte]): seq[byte] =
  ## CopyData('w' + startLsn + endLsn + sendTime + walData).
  var payload: seq[byte]
  payload.add(byte('w'))
  payload.addInt64(startLsn)
  payload.addInt64(endLsn)
  payload.addInt64(sendTime)
  payload.add(walData)
  buildCopyData(payload)

proc buildKeepalive(walEnd, sendTime: int64, replyRequested: bool): seq[byte] =
  ## CopyData('k' + walEnd + sendTime + replyRequested).
  var payload: seq[byte]
  payload.add(byte('k'))
  payload.addInt64(walEnd)
  payload.addInt64(sendTime)
  payload.add(if replyRequested: 1'u8 else: 0'u8)
  buildCopyData(payload)

proc buildCopyDone(): seq[byte] =
  @[byte('c'), 0'u8, 0'u8, 0'u8, 4'u8]

proc decodeStandbyStatusReceiveLsn(body: seq[byte]): int64 =
  ## Frontend CopyData body for a Standby Status Update: 'r' + receiveLsn(8) + ...
  doAssert body.len == 1 + 8 + 8 + 8 + 8 + 1, "unexpected standby status size"
  doAssert body[0] == byte('r'), "expected standby status type byte 'r'"
  decodeInt64(body, 1)

const
  testEndLsn = 0x0000_0000_0000_1000'i64
  testWalEnd = 0x0000_0000_0000_9999'i64

# Captured by the closure-typed `ReplicationCallback`. Kept at module scope so
# the callback body can mutate state without forcing a non-gcsafe seq capture.
var observedReceiveLsn: int64 = -1
var observedReplyMsgType: char = '\0'
var callbackKinds: seq[ReplicationMessageKind]
var keepaliveSeen: bool
var unexpectedFrontendMsgType: char = '\0'

suite "Replication: auto keepalive reply":
  test "auto-reply uses last XLogData endLsn, not server walEnd":
    observedReceiveLsn = -1
    observedReplyMsgType = '\0'
    callbackKinds.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        # Drain START_REPLICATION query.
        discard await drainFrontendMessage(st)
        # Send CopyBothResponse + XLogData + Keepalive(replyRequested=1).
        var burst: seq[byte]
        burst.add(buildCopyBothResponse())
        burst.add(buildXLogData(0x100, testEndLsn, 0, @[1'u8, 2, 3]))
        burst.add(buildKeepalive(testWalEnd, 0, replyRequested = true))
        await sendBytes(st, burst)
        # Expect the client to auto-reply with a Standby Status Update.
        let reply = await drainFrontendMessage(st)
        observedReplyMsgType = reply.msgType
        if reply.msgType == 'd':
          observedReceiveLsn = decodeStandbyStatusReceiveLsn(reply.body)
        # End the stream cleanly.
        var tail: seq[byte]
        tail.add(buildCopyDone())
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        # Client will reply with CopyDone before draining; consume it.
        discard await drainFrontendMessage(st)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          callbackKinds.add(msg.kind)

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check observedReplyMsgType == 'd'
    check observedReceiveLsn == testEndLsn
    check callbackKinds == @[rmkXLogData, rmkPrimaryKeepalive]

  test "auto-reply disabled: library does not send Standby Status":
    keepaliveSeen = false
    unexpectedFrontendMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst: seq[byte]
        burst.add(buildCopyBothResponse())
        burst.add(buildXLogData(0x100, testEndLsn, 0, @[1'u8, 2, 3]))
        burst.add(buildKeepalive(testWalEnd, 0, replyRequested = true))
        await sendBytes(st, burst)
        # The library must NOT auto-reply with a Standby Status Update when
        # autoKeepaliveReply = false. Observe this directly by attempting to
        # read a frontend message with a timeout: the read MUST time out.
        try:
          let reply = await wait(drainFrontendMessage(st), milliseconds(300))
          unexpectedFrontendMsgType = reply.msgType
        except AsyncTimeoutError:
          discard # expected: nothing sent
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          if msg.kind == rmkPrimaryKeepalive:
            keepaliveSeen = true

      try:
        await conn.startReplication(
          "test_slot", autoKeepaliveReply = false, callback = cb
        )
      except CatchableError:
        discard # expected: server closes after the burst
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check keepaliveSeen
    check unexpectedFrontendMsgType == '\0'
