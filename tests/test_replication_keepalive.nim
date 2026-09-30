## Replication keepalive auto-reply E2E tests using the in-process mock server.
##
## Verifies that with `autoKeepaliveReply = true` (the default) the library
## responds to `PrimaryKeepalive(replyRequested=true)` messages automatically,
## reporting the highest received position in the *receive* field:
## `receivedEndLsn` on a physical stream (never a `walEnd`), and on a logical
## stream `XLogData.startLsn` or the keepalive's `walEnd` (the sent position).
## Also verifies that flush/apply only reflect the LSN confirmed durable via
## `confirmFlushed` (so merely-received WAL does not advance
## `confirmed_flush_lsn`, preserving at-least-once delivery) and the opt-out path.

import std/[strutils, unittest]

when defined(posix):
  import std/posix

import ../async_postgres/[async_backend, pg_replication]
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/types

import mock_pg_server

import std/importutils
privateAccess(PgConnection)

when hasChronos:
  from std/times import cpuTime
  import ../async_postgres/pg_connection/buffer_io

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

const
  # startLsn of the XLogData burst the mock server sends.
  testStartLsn = 0x0000_0000_0000_1000'i64
  # WAL bytes carried by the XLogData. On a physical stream the client should
  # acknowledge receivedEndLsn = testStartLsn + testWalData.len.
  testWalData: seq[byte] = @[1'u8, 2, 3]
  testReceivedEndLsn = testStartLsn + testWalData.len
  # Physical walEnd values, which may be far ahead of what the message
  # contains; a physical client must NOT acknowledge these. A logical XLogData
  # carries walEnd == startLsn, so logical tests pass testStartLsn instead.
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

      await conn.startPhysicalReplication(InvalidLsn, "test_slot", callback = cb)
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

  test "logical: keepalive walEnd is received; confirmFlushed clamps to startLsn":
    observedReceiveLsn = -1
    observedFlushLsn = -1
    observedApplyLsn = -1
    observedReplyMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        let ssu = await runAutoKeepaliveServer(
          st, testStartLsn, testStartLsn, testKeepaliveWalEnd, testWalData
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
          if msg.kind == rmkXLogData:
            discard conn.confirmFlushed(msg.xlogData.receivedEndLsn)

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check observedReplyMsgType == 'd'
    check observedReceiveLsn == testKeepaliveWalEnd
    check observedFlushLsn == testStartLsn
    check observedApplyLsn == testStartLsn

  test "logical: XLogData counts startLsn, not startLsn + data.len":
    observedReceiveLsn = -1
    observedFlushLsn = -1
    observedReplyMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        # Keepalive walEnd at startLsn so it cannot mask the XLogData bound.
        let ssu = await runAutoKeepaliveServer(
          st, testStartLsn, testStartLsn, testStartLsn, testWalData
        )
        observedReplyMsgType = ssu.msgType
        observedReceiveLsn = ssu.receive
        observedFlushLsn = ssu.flush
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          if msg.kind == rmkXLogData:
            discard conn.confirmFlushed(msg.xlogData.receivedEndLsn)

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check observedReplyMsgType == 'd'
    check observedReceiveLsn == testStartLsn
    check observedFlushLsn == testStartLsn

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

      await conn.startPhysicalReplication(InvalidLsn, "test_slot", callback = cb)
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

      await conn.startPhysicalReplication(InvalidLsn, "test_slot", callback = cb)
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

      await conn.startPhysicalReplication(InvalidLsn, "test_slot", callback = cb)
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
  test "logical: statusInterval sends a Standby Status without a reply-requested keepalive":
    # A server with wal_sender_timeout = 0 never requests a reply, so the slot
    # only advances if the standby sends status updates on its own. With a
    # positive statusInterval the library must emit a Standby Status Update
    # (receive = received LSN, flush/apply = confirmFlushed) even though the
    # server set replyRequested only never. Works on both backends: chronos via a
    # timed idle wake, asyncdispatch via the post-message path nudged below.
    # The physical variant lives in test_physical_replication.
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
        burst.add(buildXLogData(testStartLsn, testStartLsn, 0, testWalData))
        await sendBytes(st, burst)
        # Let the status interval (50ms) elapse, then send a non-reply keepalive
        # to unblock the asyncdispatch read (which cannot wake on a timer);
        # chronos has already emitted updates on its own by now. Its walEnd is
        # startLsn so the received LSN is the same whichever backend replies.
        await sleepAsync(milliseconds(150))
        await sendBytes(st, buildKeepalive(testStartLsn, 0, replyRequested = false))
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
    # replyRequested: receive carries the received LSN (startLsn on a logical
    # stream), flush/apply the confirmed position clamped to it.
    check observedReplyMsgType == 'd'
    check observedReceiveLsn == testStartLsn
    check observedFlushLsn == testStartLsn
    check observedApplyLsn == testStartLsn

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

var stopFrontendMsgs: seq[char]

suite "Replication: client-initiated stop":
  test "stopReplication does not double-send CopyDone":
    # Regression: the recv-loop `bmkCopyDone` handler used to mirror the
    # server's CopyDone unconditionally, so after stopReplication the client
    # sent [status, CopyDone] twice. No CopyData or CopyDone may follow the
    # client's CopyDone (PostgreSQL ignores it once out of COPY mode).
    stopFrontendMsgs.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendBytes(st, buildCopyBothResponse())
        # stopReplication first flushes a Standby Status ('d'), then CopyDone ('c').
        let m1 = await drainFrontendMessage(st)
        let m2 = await drainFrontendMessage(st)
        {.cast(gcsafe).}:
          stopFrontendMsgs.add(m1.msgType)
          stopFrontendMsgs.add(m2.msgType)
        var tail: seq[byte]
        tail.add(buildCopyDone())
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        # After ReadyForQuery only Terminate ('X') from conn.close is expected.
        # A pre-fix client would send another 'd' then 'c' here.
        while true:
          let m =
            try:
              await drainFrontendMessage(st)
            except CatchableError:
              break
          {.cast(gcsafe).}:
            stopFrontendMsgs.add(m.msgType)
          if m.msgType == 'X':
            break
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        discard msg

      proc stopper() {.async.} =
        while conn.state != csReplicating:
          await sleepAsync(milliseconds(1))
        await conn.stopReplication()

      let stopFut = stopper()
      await conn.startReplication("test_slot", callback = cb)
      await stopFut
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check stopFrontendMsgs == @['d', 'c', 'X']

  test "sends past the stop's final status are dropped, a concurrent stop shares it":
    # The walsender stops reading at the client's CopyDone, so a later status
    # is dropped rather than raised: a callback acking a message still in
    # flight from before the stop must not end the stream.
    stopFrontendMsgs.setLen(0)
    var stillReplicating, bothStopped, statusDropped, copyDropped = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendBytes(st, buildCopyBothResponse())
        let m1 = await drainFrontendMessage(st)
        let m2 = await drainFrontendMessage(st)
        {.cast(gcsafe).}:
          stopFrontendMsgs.add(m1.msgType)
          stopFrontendMsgs.add(m2.msgType)
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
          {.cast(gcsafe).}:
            stopFrontendMsgs.add(m.msgType)
          if m.msgType == 'X':
            break
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        discard msg

      proc stopper() {.async.} =
        while conn.state != csReplicating:
          await sleepAsync(milliseconds(1))
        # Both calls start before any suspension: the second waits for the
        # first instead of sending its own status and CopyDone.
        let first = conn.stopReplication()
        let second = conn.stopReplication()
        stillReplicating = conn.state == csReplicating
        # The idle queue took the final status at once: too late for these.
        await conn.sendStandbyStatus(Lsn(1))
        statusDropped = true
        await conn.sendCopyData([byte('x')])
        copyDropped = true
        await first
        await second
        bothStopped = true

      let stopFut = stopper()
      await conn.startReplication("test_slot", callback = cb)
      await stopFut
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check stillReplicating
    check statusDropped
    check copyDropped
    check bothStopped
    check stopFrontendMsgs == @['d', 'c', 'X']

  test "a callback acking XLogData that arrives after the stop keeps the connection":
    # The walsender keeps sending what it had in flight until it reads the
    # client's CopyDone; a callback acking each message must not end the stream.
    stopFrontendMsgs.setLen(0)
    var acks = 0
    var readyAfter = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendBytes(st, buildCopyBothResponse())
        let m1 = await drainFrontendMessage(st)
        let m2 = await drainFrontendMessage(st)
        {.cast(gcsafe).}:
          stopFrontendMsgs.add(m1.msgType)
          stopFrontendMsgs.add(m2.msgType)
        var tail = buildXLogData(0x1000, 0x1010, 0, newSeq[byte](16))
        tail.add(buildCopyDone())
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        while true:
          let m =
            try:
              await drainFrontendMessage(st)
            except CatchableError:
              break
          {.cast(gcsafe).}:
            stopFrontendMsgs.add(m.msgType)
          if m.msgType == 'X':
            break
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        if msg.kind == rmkXLogData:
          await conn.sendStandbyStatus(msg.xlogData.startLsn)
          inc acks

      proc stopper() {.async.} =
        while conn.state != csReplicating:
          await sleepAsync(milliseconds(1))
        await conn.stopReplication()

      let stopFut = stopper()
      await conn.startReplication("test_slot", callback = cb)
      await stopFut
      readyAfter = conn.state == csReady
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check acks == 1
    check readyAfter
    check stopFrontendMsgs == @['d', 'c', 'X']

  test "a status sent after the stop goes out ahead of its final status":
    # The stop's status waits behind a frame still being written, so a report
    # made after stopReplication is still written, verbatim, before it.
    var flushes: seq[int64]
    var seen: seq[char]
    var reportDone = false
    const late = 0x7000'i64

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendBytes(st, buildCopyBothResponse())
        # Read nothing for a while so the bulk frame stays in flight.
        await sleepAsync(milliseconds(500))
        while true:
          let m =
            try:
              await drainFrontendMessage(st)
            except CatchableError:
              break
          {.cast(gcsafe).}:
            seen.add(m.msgType)
            if m.msgType == 'd' and m.body.len > 0 and m.body[0] == byte('r'):
              flushes.add(decodeStandbyStatus(m.body).flush)
          if m.msgType == 'c':
            var tail: seq[byte]
            tail.add(buildCopyDone())
            tail.add(buildReadyForQuery('I'))
            await sendBytes(st, tail)
          if m.msgType == 'X':
            break
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        discard msg

      proc stopper() {.async.} =
        while conn.state != csReplicating:
          await sleepAsync(milliseconds(1))
        let bulk = conn.sendCopyData(newSeq[byte](32 * 1024 * 1024))
        let stop = conn.stopReplication()
        await conn.sendStandbyStatus(Lsn(late))
        reportDone = true
        await stop
        await bulk

      let stopFut = stopper()
      await conn.startReplication("test_slot", callback = cb)
      await stopFut
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check reportDone
    check flushes == @[late, late]
    check seen == @['d', 'd', 'd', 'c', 'X']

  test "an error ending the stream settles writes before handing back":
    # The server leaves COPY with ErrorResponse while a frame is still being
    # written. The stream must not return the connection mid-frame, a write
    # queued behind it must fail rather than go out on an idle connection, and
    # so must one issued while the stream waits out the frame.
    var bulkDone, queuedRefused, gotQueryError, readyAfter = false
    var lateRefused, lateWhileReplicating = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendBytes(st, buildCopyBothResponse())
        await sleepAsync(milliseconds(100))
        var tail = buildErrorResponse("XX000", "walsender failed")
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        # Only now drain the client's writes.
        await sleepAsync(milliseconds(300))
        while true:
          let m =
            try:
              await drainFrontendMessage(st)
            except CatchableError:
              break
          if m.msgType == 'X':
            break
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        discard msg
      var bulk, queued: Future[void]

      proc writer() {.async.} =
        while conn.state != csReplicating:
          await sleepAsync(milliseconds(1))
        bulk = conn.sendCopyData(newSeq[byte](32 * 1024 * 1024))
        queued = conn.sendCopyData([byte('h')])
        # The stream closes to writes before it waits out the bulk frame.
        while conn.replWritesOpen:
          await sleepAsync(milliseconds(1))
        lateWhileReplicating = conn.state == csReplicating
        try:
          await conn.sendStandbyStatus(Lsn(1))
        except PgStateError:
          lateRefused = true

      let writerFut = writer()
      try:
        await conn.startReplication("test_slot", callback = cb)
      except PgQueryError:
        gotQueryError = true
      await writerFut
      bulkDone = bulk.finished
      readyAfter = conn.state == csReady
      try:
        await queued
      except PgStateError:
        queuedRefused = true
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check gotQueryError
    check bulkDone
    check queuedRefused
    check readyAfter
    check lateWhileReplicating
    check lateRefused

  test "a write failing while an error ends the stream leaves it closed":
    # The server errors out of COPY and drops the connection while a frame is
    # still being written. If the write fails during the wait, the connection
    # must stay closed rather than be handed back as ready; if it went through,
    # the server's error ends the stream as usual.
    var gotConnError, gotQueryError, closedAfter, bulkFailed = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendBytes(st, buildCopyBothResponse())
        await sleepAsync(milliseconds(100))
        var tail = buildErrorResponse("XX000", "walsender failed")
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        await closeClient(st) # unread client bytes turn into a reset

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        discard msg
      var bulk: Future[void]

      proc writer() {.async.} =
        while conn.state != csReplicating:
          await sleepAsync(milliseconds(1))
        bulk = conn.sendCopyData(newSeq[byte](32 * 1024 * 1024))

      let writerFut = writer()
      try:
        await conn.startReplication("test_slot", callback = cb)
      except PgConnectionError:
        gotConnError = true
      except PgQueryError:
        gotQueryError = true
      await writerFut
      closedAfter = conn.state == csClosed
      try:
        await bulk
      except CatchableError:
        bulkFailed = true
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    # chronos reliably sees the reset on the pending write; asyncdispatch may
    # finish it into the socket buffer first.
    when hasChronos:
      check bulkFailed
    check gotConnError == bulkFailed
    check closedAfter == bulkFailed
    check gotQueryError == not bulkFailed

  when hasChronos:
    test "a cancelled stop still goes out whole and ends the stream":
      # Cancelling a stop only ends that caller's wait: the queued status and
      # CopyDone are written anyway, a concurrent stop completes with them, and
      # the connection stays usable.
      var ownerCancelled, waiterDone, streamEnded, readyAfter = false
      var seen: seq[char]

      proc testBody() {.async.} =
        let ms = startMockServer()

        proc serverHandler() {.async.} =
          let st = await acceptAndReady(ms)
          discard await drainFrontendMessage(st) # START_REPLICATION
          await sendBytes(st, buildCopyBothResponse())
          # Read nothing for a while, so the client's writes back up behind a
          # full socket buffer and the cancel lands before the stop is written.
          await sleepAsync(seconds(2))
          while true:
            let m =
              try:
                await drainFrontendMessage(st)
              except CatchableError:
                break
            {.cast(gcsafe).}:
              seen.add(m.msgType)
            if m.msgType == 'c':
              var tail: seq[byte]
              tail.add(buildCopyDone())
              tail.add(buildReadyForQuery('I'))
              await sendBytes(st, tail)
            if m.msgType == 'X':
              break
          await closeClient(st)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        let cb = makeReplicationCallback:
          discard msg

        proc stopper() {.async.} =
          while conn.state != csReplicating:
            await sleepAsync(milliseconds(1))
          # Fill the socket so the stop stays queued behind this frame.
          let bulk = conn.sendCopyData(newSeq[byte](32 * 1024 * 1024))
          let owner = conn.stopReplication()
          let waiter = conn.stopReplication()
          await owner.cancelAndWait()
          ownerCancelled = owner.cancelled
          await waiter
          waiterDone = true
          await bulk

        let stopFut = stopper()
        await conn.startReplication("test_slot", callback = cb)
        streamEnded = true
        readyAfter = conn.state == csReady
        await stopFut
        await conn.close()
        await serverFut
        await closeServer(ms)

      waitFor testBody().wait(seconds(10))
      check ownerCancelled
      check waiterDone
      check streamEnded
      check readyAfter
      check seen == @['d', 'd', 'c', 'X']

  when hasChronos:
    test "dropping the transport fails writes queued behind a stuck one":
      # The write in flight is stuck on a socket the server never drains; once
      # the transport is gone it fails, and so does the stop queued behind it,
      # rather than waiting on a write that never completes.
      var waiterFailed, waiterTimedOut, holderFailed, causeKept = false
      var laterStopCause, streamCause = false

      proc testBody() {.async.} =
        let ms = startMockServer()
        let serverDone = newFuture[void]("serverDone")

        proc serverHandler() {.async.} =
          let st = await acceptAndReady(ms)
          discard await drainFrontendMessage(st) # START_REPLICATION
          await sendBytes(st, buildCopyBothResponse())
          await serverDone # read nothing more
          await closeClient(st)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        let cb = makeReplicationCallback:
          discard msg

        proc driver() {.async.} =
          while conn.state != csReplicating:
            await sleepAsync(milliseconds(1))
          let holder = conn.sendCopyData(newSeq[byte](32 * 1024 * 1024)) # stuck
          let waiter = conn.stopReplication() # queued behind it
          await conn.closeTransport()
          try:
            await waiter.wait(seconds(2))
          except AsyncTimeoutError:
            waiterTimedOut = true
          except PgConnectionError as e:
            waiterFailed = true
            # The transport failure that ended the stream stays attached.
            causeKept = e.parent != nil
          try:
            await holder
          except PgConnectionError:
            holderFailed = true
          # A stop issued afterwards still names the write failure as cause.
          try:
            await conn.stopReplication()
          except PgConnectionError as e:
            laterStopCause = e.parent != nil

        let driverFut = driver()
        try:
          await conn.startReplication("test_slot", callback = cb)
        except PgConnectionError as e:
          streamCause = e.parent != nil
        except CatchableError:
          discard
        await driverFut
        serverDone.complete()
        await serverFut
        await closeServer(ms)

      waitFor testBody().wait(seconds(10))
      check waiterFailed
      check not waiterTimedOut
      check holderFailed
      check causeKept
      check laterStopCause
      check streamCause

  when hasChronos and defined(posix):
    test "a failed write stops the callback for messages still buffered":
      # Only the write direction is shut down, so the buffered messages stay
      # readable while the write fails.
      var calls = 0
      var streamCause = false

      proc testBody() {.async.} =
        let ms = startMockServer()
        let serverDone = newFuture[void]("serverDone")

        proc serverHandler() {.async.} =
          let st = await acceptAndReady(ms)
          discard await drainFrontendMessage(st) # START_REPLICATION
          var burst = buildCopyBothResponse()
          for i in 0 ..< 3:
            let lsn = testStartLsn + i * 0x100
            burst.add(buildXLogData(lsn, lsn, 0, testWalData))
          await sendBytes(st, burst)
          await serverDone
          await closeClient(st)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        var sent: Future[void]
        let cb = makeReplicationCallback:
          {.cast(gcsafe).}:
            inc calls
            if calls == 1:
              doAssert shutdown(SocketHandle(conn.transport.fd), SHUT_WR) == 0
              sent = conn.sendCopyData(@[byte 1, 2, 3])
        try:
          await conn.startReplication("test_slot", callback = cb)
        except PgConnectionError as e:
          streamCause = e.parent != nil
        try:
          await sent
        except CatchableError:
          discard
        serverDone.complete()
        await serverFut
        await closeServer(ms)

      waitFor testBody().wait(seconds(10))
      check calls == 1
      check streamCause

    test "dropping the transport under a statusInterval read is a connection error":
      # The read in flight across status timer wakes must fail as a lost
      # connection, not as a cancellation.
      var gotConnError, gotCancel = false

      proc testBody() {.async.} =
        let ms = startMockServer()
        let serverDone = newFuture[void]("serverDone")

        proc serverHandler() {.async.} =
          let st = await acceptAndReady(ms)
          discard await drainFrontendMessage(st) # START_REPLICATION
          await sendBytes(st, buildCopyBothResponse())
          await serverDone # read nothing more
          await closeClient(st)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        let cb = makeReplicationCallback:
          discard msg

        proc driver() {.async.} =
          while conn.state != csReplicating:
            await sleepAsync(milliseconds(1))
          await sleepAsync(milliseconds(50)) # the detached read is in flight
          let holder = conn.sendCopyData(newSeq[byte](32 * 1024 * 1024)) # stuck
          await conn.closeTransport()
          try:
            await holder
          except CatchableError:
            discard

        let driverFut = driver()
        try:
          await conn.startReplication(
            "test_slot", statusInterval = seconds(10), callback = cb
          )
        except CancelledError:
          gotCancel = true
        except PgConnectionError:
          gotConnError = true
        await driverFut
        serverDone.complete()
        await serverFut
        await closeServer(ms)

      waitFor testBody().wait(seconds(10))
      check gotConnError
      check not gotCancel

  proc statusFrame(lsn: int64): seq[byte] =
    ## A hand-built Standby Status Update reporting ``lsn`` in every field.
    var f = @[byte('r')]
    for _ in 0 ..< 3:
      for i in countdown(7, 0):
        f.add(byte((lsn shr (i * 8)) and 0xff))
    for _ in 0 ..< 8:
      f.add(0'u8)
    f.add(0'u8)
    f

  type ReportVia = enum
    rvStatus # sendStandbyStatus
    rvCopyData # a hand-built status via sendCopyData

  proc runReportThenStop(
      reports: seq[int64],
      confirm: bool,
      autoReply: bool,
      via = rvStatus,
      overlap = false,
  ): tuple[msgs: seq[char], last: tuple[receive, flush, apply: int64]] =
    ## On the XLogData the callback sends each of ``reports`` as a status
    ## (after a lower confirmFlushed when ``confirm``) and stops. With
    ## ``overlap`` the stop starts before the reports' writes are awaited.
    ## Records frontend messages up to CopyDone and the positions of the last
    ## status.
    var res: tuple[msgs: seq[char], last: tuple[receive, flush, apply: int64]] =
      (@[], (-1'i64, -1'i64, -1'i64))

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst = buildCopyBothResponse()
        burst.add(buildXLogData(testStartLsn, testStartLsn, 0, testWalData))
        await sendBytes(st, burst)
        while true:
          let m = await drainFrontendMessage(st)
          {.cast(gcsafe).}:
            res.msgs.add(m.msgType)
            if m.msgType == 'd':
              res.last = decodeStandbyStatus(m.body)
          if m.msgType == 'c':
            break
        var tail = buildCopyDone()
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        discard await drainFrontendMessage(st) # Terminate
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        if msg.kind == rmkXLogData:
          if confirm:
            discard conn.confirmFlushed(msg.xlogData.startLsn)
          var pending: seq[Future[void]]
          for lsn in reports:
            let fut =
              case via
              of rvStatus:
                conn.sendStandbyStatus(Lsn(lsn))
              of rvCopyData:
                conn.sendCopyData(statusFrame(lsn))
            if overlap:
              pending.add(fut)
            else:
              await fut
          await conn.stopReplication()
          for fut in pending:
            await fut

      await conn.startReplication(
        "test_slot", autoKeepaliveReply = autoReply, callback = cb
      )
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    res

  # The library's own status must not report below the caller's last report:
  # on a physical slot PostgreSQL takes flush as the new restart_lsn.
  test "manual mode: the stop's status keeps the reported flush":
    let r =
      runReportThenStop(@[testKeepaliveWalEnd], confirm = false, autoReply = false)
    check r.msgs == @['d', 'd', 'c']
    check r.last == (testKeepaliveWalEnd, testKeepaliveWalEnd, testKeepaliveWalEnd)

  test "manual mode with a lower confirmFlushed: the reported flush still wins":
    let r = runReportThenStop(@[testKeepaliveWalEnd], confirm = true, autoReply = false)
    check r.msgs == @['d', 'd', 'c']
    check r.last.flush == testKeepaliveWalEnd

  test "auto-reply mode: a manual report is not undercut by the stop":
    let r = runReportThenStop(@[testKeepaliveWalEnd], confirm = true, autoReply = true)
    check r.msgs == @['d', 'd', 'c']
    check r.last.flush == testKeepaliveWalEnd

  test "a status sent via sendCopyData is kept too":
    let r = runReportThenStop(
      @[testKeepaliveWalEnd], confirm = false, autoReply = false, via = rvCopyData
    )
    check r.msgs == @['d', 'd', 'c']
    check r.last.flush == testKeepaliveWalEnd

  test "a report still being written when the stop starts is not undercut":
    # The stop's status is computed only once the report's write is through.
    let r = runReportThenStop(
      @[testKeepaliveWalEnd], confirm = false, autoReply = false, overlap = true
    )
    check r.msgs == @['d', 'd', 'c']
    check r.last.flush == testKeepaliveWalEnd

  test "a deliberately lower report is kept, not the earlier higher one":
    let lower = testStartLsn + 0x100
    let r = runReportThenStop(
      @[testKeepaliveWalEnd, lower], confirm = false, autoReply = false
    )
    check r.msgs == @['d', 'd', 'd', 'c']
    check r.last.flush == lower

  test "the keepalive reply and the server-stop mirror keep the reported position":
    # Receive, flush and apply of each library status stay at or above the
    # caller's report.
    var replies: seq[tuple[receive, flush, apply: int64]]

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst = buildCopyBothResponse()
        burst.add(buildXLogData(testStartLsn, testStartLsn, 0, testWalData))
        await sendBytes(st, burst)
        discard await drainFrontendMessage(st) # the caller's report
        await sendBytes(st, buildKeepalive(testStartLsn, 0, replyRequested = true))
        let reply = await drainFrontendMessage(st)
        {.cast(gcsafe).}:
          replies.add(decodeStandbyStatus(reply.body))
        var tail = buildCopyDone() # server-initiated stop
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        let mirror = await drainFrontendMessage(st) # status before its CopyDone
        {.cast(gcsafe).}:
          replies.add(decodeStandbyStatus(mirror.body))
        discard await drainFrontendMessage(st) # CopyDone
        discard await drainFrontendMessage(st) # Terminate
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        if msg.kind == rmkXLogData:
          await conn.sendStandbyStatus(
            Lsn(testKeepaliveWalEnd), applyLsn = Lsn(testStartLsn)
          )

      await conn.startReplication("test_slot", callback = cb)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check replies.len == 2
    for r in replies:
      check r.receive == testKeepaliveWalEnd
      check r.flush == testKeepaliveWalEnd
      check r.apply == testStartLsn

when hasChronos:
  suite "Replication: idle wakeup rate":
    test "statusInterval + autoKeepaliveReply=false does not busy-spin while idle":
      # Regression: replFillRecvBuf entered its 1 ms timer race whenever
      # statusInterval > 0. With autoKeepaliveReply=false the paired
      # maybeSendPeriodicStatus is a no-op, so lastStatusSent never advanced
      # past the statusInterval and the race rearmed at ~1 kHz for the whole
      # idle period. The fix drops the effective interval to ZeroDuration in
      # that mode so the read blocks until data arrives.
      var callbackCalls = 0
      var elapsedCpu = 0.0
      var elapsedWallMs: int64 = 0

      proc testBody() {.async.} =
        let ms = startMockServer()

        proc serverHandler() {.async.} =
          let st = await acceptAndReady(ms)
          discard await drainFrontendMessage(st) # START_REPLICATION
          var burst: seq[byte]
          burst.add(buildCopyBothResponse())
          burst.add(buildXLogData(testStartLsn, testXLogWalEnd, 0, testWalData))
          await sendBytes(st, burst)
          # The whole point of the test: stay silent long enough that a
          # ~1 kHz rearm would burn hundreds of iterations.
          await sleepAsync(milliseconds(400))
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
            if m.msgType == 'c':
              break
          await closeClient(st)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        let cb = makeReplicationCallback:
          {.cast(gcsafe).}:
            discard msg
            callbackCalls.inc

        let cpuBefore = cpuTime()
        let wallBefore = Moment.now()
        await conn.startReplication(
          "test_slot",
          autoKeepaliveReply = false,
          statusInterval = milliseconds(50),
          callback = cb,
        )
        elapsedCpu = cpuTime() - cpuBefore
        elapsedWallMs = (Moment.now() - wallBefore).milliseconds

        await conn.close()
        await serverFut
        await closeServer(ms)

      waitFor testBody()
      # Only the initial XLogData is delivered; no keepalive requested a reply.
      check callbackCalls == 1
      # Sanity: the 400 ms idle period actually elapsed on the wall clock.
      check elapsedWallMs >= 300
      # Regression guard: pre-fix, CPU time tracked wall time (≈ 400 ms) because
      # the timer race rearmed every ~1 ms. Post-fix, an idle stream costs only
      # what one XLogData handler plus the connection close spends. 100 ms of
      # CPU here is well over an order of magnitude above the fixed cost and
      # well below the busy-spin regime, so it survives CI jitter cleanly.
      check elapsedCpu < 0.1

  var cancelReplFutCancelled: bool
  var cancelConnStateAfterCancel: PgConnState
  var cancelRecvBufLenAfterCancel: int
  var cancelRecvBufLenAfterMarker: int

  suite "Replication: cancellation with statusInterval":
    test "cancel while idle drops the locally-spawned detached read":
      # Regression: chronos race() does not cancel its children. When the
      # replication future is cancelled while blocked in
      # replFillRecvBuf's race(read, timer) on the very first idle wait,
      # the freshly-spawned `read` is not yet visible to the caller's
      # cleanup — it would stay in flight, holding the AsyncStream reader
      # and eventually committing bytes into recvBuf after the connection
      # was already marked csClosed. The fix cancels the locally-owned
      # read on the CancelledError path.
      cancelReplFutCancelled = false
      cancelConnStateAfterCancel = csReady
      cancelRecvBufLenAfterCancel = -1
      cancelRecvBufLenAfterMarker = -1

      proc testBody() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient

        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          discard await drainFrontendMessage(serverClient) # START_REPLICATION
          # Send only CopyBothResponse; stay silent so the client sits in
          # race(read, timer) with a locally-spawned read. statusInterval is
          # set well above the test's cancel delay so the timer cannot win.
          await sendBytes(serverClient, buildCopyBothResponse())

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))

        let cb = makeReplicationCallback:
          {.cast(gcsafe).}:
            discard msg

        let replFut =
          conn.startReplication("test_slot", statusInterval = seconds(5), callback = cb)
        # Let the client enter its recvLoop and spawn the detached read. The
        # server handler drains START_REPLICATION and sends CopyBothResponse
        # during this window, so serverFut becomes ready to await.
        await sleepAsync(milliseconds(80))
        await serverFut
        await replFut.cancelAndWait()
        cancelReplFutCancelled = replFut.cancelled()
        cancelConnStateAfterCancel = conn.state
        cancelRecvBufLenAfterCancel = conn.recvBuf.len
        # Push a distinctive marker from the server side. Pre-fix, the
        # orphaned readOnce would consume these bytes into replReadScratch
        # and fillRecvBufDetached's tail would append them to conn.recvBuf.
        # Post-fix, cancelSoon() releases the reader before the marker is
        # sent, so the bytes stay queued at the transport instead. Inlined
        # to avoid a captured GC seq tripping chronos' gcsafe check.
        await sendBytes(
          serverClient, @[0xDE'u8, 0xAD, 0xBE, 0xEF, 0x42, 0x42, 0x42, 0x42]
        )
        for _ in 0 .. 8:
          await sleepAsync(milliseconds(5))
        cancelRecvBufLenAfterMarker = conn.recvBuf.len

        await closeClient(serverClient)
        try:
          await conn.close()
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor testBody()
      check cancelReplFutCancelled
      check cancelConnStateAfterCancel == csClosed
      # The core assertion: no bytes were committed into recvBuf after the
      # cancel. Pre-fix this would grow by the marker length.
      check cancelRecvBufLenAfterMarker == cancelRecvBufLenAfterCancel

var observedStartQuery: string

proc runStartReplicationCapture(slot: string, options: seq[(string, string)]): string =
  ## Drive startReplication against a mock server that just observes the
  ## START_REPLICATION query bytes, then cleanly ends the stream.
  observedStartQuery = ""

  proc testBody() {.async.} =
    let ms = startMockServer()

    proc serverHandler() {.async.} =
      let st = await acceptAndReady(ms)
      let m = await drainFrontendMessage(st)
      # Query body is a NUL-terminated SQL string; strip the trailing NUL.
      if m.msgType == 'Q' and m.body.len > 0:
        var s = newString(m.body.len - 1)
        for i in 0 ..< s.len:
          s[i] = char(m.body[i])
        {.cast(gcsafe).}:
          observedStartQuery = s
      var tail: seq[byte]
      tail.add(buildCopyBothResponse())
      tail.add(buildCopyDone())
      tail.add(buildReadyForQuery('I'))
      await sendBytes(st, tail)
      discard await drainFrontendMessage(st) # client's CopyDone
      await closeClient(st)

    let serverFut = serverHandler()
    let conn = await connect(mockConfig(ms.port))
    let cb = makeReplicationCallback:
      {.cast(gcsafe).}:
        discard msg

    await conn.startReplication(slot, options = options, callback = cb)
    await conn.close()
    await serverFut
    await closeServer(ms)

  waitFor testBody()
  observedStartQuery

suite "Replication: pgoutput proto_version defensive injection":
  test "publication_names without proto_version pins proto_version '1'":
    let q = runStartReplicationCapture("test_slot", @[("publication_names", "p1")])
    check "publication_names 'p1'" in q
    check "proto_version '1'" in q

  test "explicit proto_version is preserved and not duplicated":
    let q = runStartReplicationCapture(
      "test_slot", @[("proto_version", "1"), ("publication_names", "p1")]
    )
    check q.count("proto_version") == 1

  test "no publication_names => no proto_version injected":
    # Guard for non-pgoutput plugins (test_decoding, wal2json, ...): they do
    # not understand proto_version and would reject an injected value.
    let q = runStartReplicationCapture("test_slot", @[])
    check "proto_version" notin q

  test "option values are single-quoted against injection":
    let q = runStartReplicationCapture(
      "test_slot", @[("publication_names", "p1'); DROP TABLE t; --")]
    )
    # Single-quoted with embedded quotes doubled — no unquoted breakout.
    check "publication_names 'p1''); DROP TABLE t; --'" in q
    check "publication_names 'p1'); DROP" notin q

  test "empty option value stays flag-only (no quoted empty string)":
    let q = runStartReplicationCapture("test_slot", @[("binary", "")])
    check "(binary)" in q
    check "binary ''" notin q

  test "backslash in an option value stays literal (no E'' form)":
    # The walsender scanner has no E'' rule, so the value must keep its plain
    # single-quoted spelling even though `quoteLiteral` would switch forms.
    let q = runStartReplicationCapture("test_slot", @[("publication_names", "a\\b")])
    check "publication_names 'a\\b'" in q
    check "E'" notin q
