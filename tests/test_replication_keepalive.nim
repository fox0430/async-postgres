## Replication keepalive auto-reply E2E tests using the in-process mock server.
##
## Verifies that when `startReplication` is invoked with `autoKeepaliveReply = true`
## (the default), the library responds to `PrimaryKeepalive(replyRequested=true)`
## messages automatically, reporting the highest `receivedEndLsn`
## (`XLogData.startLsn + data.len`) observed so far in the *receive* field —
## never the server's `walEnd` (neither the keepalive's nor the XLogData's).
## Also verifies that flush/apply only reflect the LSN confirmed durable via
## `confirmFlushed` (so merely-received WAL does not advance
## `confirmed_flush_lsn`, preserving at-least-once delivery) and the opt-out path.

import std/unittest

import ../async_postgres/[async_backend, pg_replication]
import ../async_postgres/pg_connection {.all.}

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

const
  # startLsn of the XLogData burst the mock server sends.
  testStartLsn = 0x0000_0000_0000_1000'i64
  # WAL bytes carried by the XLogData. The receivedEndLsn the client should
  # acknowledge is testStartLsn + testWalData.len.
  testWalData: seq[byte] = @[1'u8, 2, 3]
  testReceivedEndLsn = testStartLsn + testWalData.len
  # XLogData.walEnd and PrimaryKeepalive.walEnd are both the server's current
  # WAL end; they may be far ahead of what the message actually contains.
  # The client must NOT acknowledge these.
  testXLogWalEnd = 0x0000_0000_0000_5000'i64
  testKeepaliveWalEnd = 0x0000_0000_0000_9999'i64

# Captured by the closure-typed `ReplicationCallback`. Kept at module scope so
# the callback body can mutate state without forcing a non-gcsafe seq capture.
var observedReceiveLsn: int64 = -1
var observedFlushLsn: int64 = -1
var observedApplyLsn: int64 = -1
var observedReplyMsgType: char = '\0'
var callbackKinds: seq[ReplicationMessageKind]
var keepaliveSeen: bool
var unexpectedFrontendMsgType: char = '\0'

suite "Replication: auto keepalive reply":
  test "auto-reply uses receivedEndLsn (startLsn+data.len), not any walEnd":
    observedReceiveLsn = -1
    observedFlushLsn = -1
    observedApplyLsn = -1
    observedReplyMsgType = '\0'
    callbackKinds.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        # Send CopyBothResponse + XLogData + Keepalive(replyRequested=1), capture
        # the auto-reply, and end the stream cleanly. XLogData.walEnd is set far
        # ahead of startLsn+data.len so the test fails if it is acknowledged.
        let ssu = await runAutoKeepaliveServer(
          st, testStartLsn, testXLogWalEnd, testKeepaliveWalEnd, testWalData
        )
        observedReplyMsgType = ssu.msgType
        observedReceiveLsn = ssu.receive
        observedFlushLsn = ssu.flush
        observedApplyLsn = ssu.apply
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
    check observedReceiveLsn == testReceivedEndLsn
    # Regression guard: must not be either walEnd value.
    check observedReceiveLsn != testXLogWalEnd
    check observedReceiveLsn != testKeepaliveWalEnd
    # Regression: flush/apply must NOT advance to merely-received WAL.
    # No confirmFlushed was called and startLsn defaulted to 0/0, so the auto
    # reply must report 0/0 for flush/apply — PostgreSQL reads it as "position
    # unknown" and will not move confirmed_flush_lsn past unprocessed WAL.
    check observedFlushLsn == 0
    check observedApplyLsn == 0
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
        burst.add(buildXLogData(testStartLsn, testXLogWalEnd, 0, testWalData))
        burst.add(buildKeepalive(testKeepaliveWalEnd, 0, replyRequested = true))
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

  test "confirmFlushed advances the auto-reply flush/apply LSN":
    observedReceiveLsn = -1
    observedFlushLsn = -1
    observedApplyLsn = -1
    observedReplyMsgType = '\0'
    callbackKinds.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        # XLogData then a keepalive(replyRequested). The client confirms only
        # part of the received range as durable (startLsn, deliberately below the
        # received end LSN) in its XLogData callback, so the auto-reply to the
        # following keepalive must carry that confirmed LSN as flush/apply while
        # still reporting the full received LSN as receive.
        let ssu = await runAutoKeepaliveServer(
          st, testStartLsn, testXLogWalEnd, testKeepaliveWalEnd, testWalData
        )
        observedReplyMsgType = ssu.msgType
        observedReceiveLsn = ssu.receive
        observedFlushLsn = ssu.flush
        observedApplyLsn = ssu.apply
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          callbackKinds.add(msg.kind)
          if msg.kind == rmkXLogData:
            # Confirm durability only up to startLsn — deliberately *below* the
            # received end LSN — so the auto-reply's flush/apply differ from its
            # receive field and we prove they track confirmFlushed, not receipt.
            discard conn.confirmFlushed(msg.xlogData.startLsn)

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check observedReplyMsgType == 'd'
    # The receive field still reports the full received LSN (this resets
    # wal_sender_timeout on the server).
    check observedReceiveLsn == testReceivedEndLsn
    # flush/apply report only the lower LSN confirmed durable via confirmFlushed
    # (testStartLsn), proving they track the confirmed position independently of
    # receive — not merely echo the received LSN as the removed flush=receive
    # behavior did. This also makes the apply assertion meaningful: apply follows
    # flush, not the (larger) receive LSN.
    check observedFlushLsn == testStartLsn
    check observedApplyLsn == testStartLsn
    check callbackKinds == @[rmkXLogData, rmkPrimaryKeepalive]

  test "confirmFlushed ignores backward and duplicate confirmations":
    observedFlushLsn = -1
    observedApplyLsn = -1
    observedReplyMsgType = '\0'
    callbackKinds.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        let ssu = await runAutoKeepaliveServer(
          st, testStartLsn, testXLogWalEnd, testKeepaliveWalEnd, testWalData
        )
        observedReplyMsgType = ssu.msgType
        observedFlushLsn = ssu.flush
        observedApplyLsn = ssu.apply
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          callbackKinds.add(msg.kind)
          if msg.kind == rmkXLogData:
            # Confirm the full received range, then attempt a duplicate and a
            # backward confirmation. The confirmed position must stay at the
            # highest valid value.
            discard conn.confirmFlushed(msg.xlogData.receivedEndLsn)
            discard conn.confirmFlushed(msg.xlogData.receivedEndLsn)
            discard conn.confirmFlushed(msg.xlogData.startLsn)

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check observedReplyMsgType == 'd'
    check observedFlushLsn == testReceivedEndLsn
    check observedApplyLsn == testReceivedEndLsn
    check callbackKinds == @[rmkXLogData, rmkPrimaryKeepalive]

  test "confirmFlushed clamps an LSN beyond the WAL actually received":
    observedReceiveLsn = -1
    observedFlushLsn = -1
    observedApplyLsn = -1
    observedReplyMsgType = '\0'
    callbackKinds.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        # The callback confirms one byte past received WAL; confirmFlushed clamps
        # it to received and does NOT raise, so the auto-reply still fires.
        let ssu = await runAutoKeepaliveServer(
          st, testStartLsn, testXLogWalEnd, testKeepaliveWalEnd, testWalData
        )
        observedReplyMsgType = ssu.msgType
        observedReceiveLsn = ssu.receive
        observedFlushLsn = ssu.flush
        observedApplyLsn = ssu.apply
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          callbackKinds.add(msg.kind)
          if msg.kind == rmkXLogData:
            # receivedEndLsn is testStartLsn + testWalData.len. Confirming one
            # byte past that must clamp to received WAL — never advancing flush
            # beyond it — and must not raise (an uncaught raise would strand the
            # connection in csReplicating).
            discard
              conn.confirmFlushed(Lsn(uint64(msg.xlogData.receivedEndLsn) + 1'u64))

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check observedReplyMsgType == 'd'
    # receive still reports the full received LSN (resets wal_sender_timeout).
    check observedReceiveLsn == testReceivedEndLsn
    # The over-range confirmation was clamped to received WAL: flush/apply equal
    # the received end LSN, never the (received + 1) value passed in.
    check observedFlushLsn == testReceivedEndLsn
    check observedApplyLsn == testReceivedEndLsn
    check callbackKinds == @[rmkXLogData, rmkPrimaryKeepalive]

  test "confirmFlushed returns whether the position advanced":
    var observedMsgType: char = '\0'
    var firstAdvanced: bool = false
    var secondAdvanced: bool = true
    var thirdAdvanced: bool = true

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        let ssu = await runAutoKeepaliveServer(
          st, testStartLsn, testXLogWalEnd, testKeepaliveWalEnd, testWalData
        )
        observedMsgType = ssu.msgType
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          if msg.kind == rmkXLogData:
            # First confirmation moves the position forward.
            firstAdvanced = conn.confirmFlushed(msg.xlogData.startLsn)
            # Second confirmation to the same LSN is a no-op.
            secondAdvanced = conn.confirmFlushed(msg.xlogData.startLsn)
            # Backward confirmation is also a no-op.
            thirdAdvanced =
              conn.confirmFlushed(Lsn(uint64(msg.xlogData.startLsn) - 1'u64))

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check observedMsgType == 'd'
    check firstAdvanced
    check not secondAdvanced
    check not thirdAdvanced

suite "Replication: proactive status interval":
  test "statusInterval sends a Standby Status without a reply-requested keepalive":
    # A server with wal_sender_timeout = 0 never requests a reply, so the slot
    # only advances if the standby sends status updates on its own. With a
    # positive statusInterval the library must emit a Standby Status Update
    # (receive = received LSN, flush/apply = confirmFlushed) even though the
    # server set replyRequested only never. Works on both backends: chronos via a
    # timed idle wake, asyncdispatch via the post-message path nudged below.
    observedReceiveLsn = -1
    observedFlushLsn = -1
    observedApplyLsn = -1
    observedReplyMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst: seq[byte]
        burst.add(buildCopyBothResponse())
        # XLogData only — crucially, no PrimaryKeepalive(replyRequested=true).
        burst.add(buildXLogData(testStartLsn, testXLogWalEnd, 0, testWalData))
        await sendBytes(st, burst)
        # Let the status interval (50ms) elapse, then send a non-reply keepalive
        # to unblock the asyncdispatch read (which cannot wake on a timer);
        # chronos has already emitted updates on its own by now.
        await sleepAsync(milliseconds(150))
        await sendBytes(
          st, buildKeepalive(testKeepaliveWalEnd, 0, replyRequested = false)
        )
        let reply = await drainFrontendMessage(st)
        observedReplyMsgType = reply.msgType
        if reply.msgType == 'd':
          let ssu = decodeStandbyStatus(reply.body)
          observedReceiveLsn = ssu.receive
          observedFlushLsn = ssu.flush
          observedApplyLsn = ssu.apply
        # End the stream. Drain any further proactive updates plus the client's
        # CopyDone so its CopyDone send sees an open socket.
        var tail: seq[byte]
        tail.add(buildCopyDone())
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        while true:
          let m =
            try:
              await drainFrontendMessage(st)
            except CatchableError:
              break
          if m.msgType == 'c': # client's CopyDone
            break
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          if msg.kind == rmkXLogData:
            discard conn.confirmFlushed(msg.xlogData.receivedEndLsn)

      await conn.startReplication(
        "test_slot", statusInterval = milliseconds(50), callback = cb
      )
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    # A proactive Standby Status Update arrived even though the server never set
    # replyRequested: receive carries the received LSN, flush/apply the confirmed
    # position (here equal, since the callback confirmed the full received range).
    check observedReplyMsgType == 'd'
    check observedReceiveLsn == testReceivedEndLsn
    check observedFlushLsn == testReceivedEndLsn
    check observedApplyLsn == testReceivedEndLsn

var poisonRaised: bool
var poisonFinalState: PgConnState

suite "Replication: callback exception invalidates the connection":
  test "a raising callback poisons the connection (csClosed) and propagates":
    # A user callback raising mid-stream must not strand the connection in
    # csReplicating (where every later call would raise a misleading
    # PgStateError). startReplication invalidates it (csClosed) and re-raises so
    # the caller can reconnect and resume from the last confirmed LSN.
    poisonRaised = false
    poisonFinalState = csReady

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst: seq[byte]
        burst.add(buildCopyBothResponse())
        burst.add(buildXLogData(testStartLsn, testXLogWalEnd, 0, testWalData))
        await sendBytes(st, burst)
        # The client errors out of the stream and never sends CopyDone; close.
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          discard msg
          raise newException(ValueError, "boom from replication callback")

      try:
        await conn.startReplication("test_slot", callback = cb)
      except ValueError:
        {.cast(gcsafe).}:
          poisonRaised = true
      {.cast(gcsafe).}:
        poisonFinalState = conn.state
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check poisonRaised
    check poisonFinalState == csClosed
