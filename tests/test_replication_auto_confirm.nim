## `startReplication(autoConfirm = true)` tests using the in-process mock server.
##
## The library confirms a pgoutput Commit's endLsn once the callback returned for
## it, and a keepalive's walEnd outside a transaction (reporting it right away),
## never once the stop's final status is encoded.

import std/[strutils, unittest]

import ../async_postgres/[async_backend, pg_protocol, pg_replication]
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/[types]

import mock_pg_server

import std/importutils
privateAccess(PgConnection)

when hasChronos:
  import ../async_postgres/pg_connection/simple_query
  import ../async_postgres/pg_errors

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

const
  startPos = 0x1000'i64
  pubOptions = @{"publication_names": "p1"}

proc pgBegin(): seq[byte] =
  result.add(byte('B'))
  result.addInt64(0) # final LSN
  result.addInt64(0) # commit time
  result.addInt32(1) # xid

proc pgCommit(endLsn: int64): seq[byte] =
  result.add(byte('C'))
  result.add(0'u8) # flags
  result.addInt64(endLsn) # commit LSN
  result.addInt64(endLsn)
  result.addInt64(0) # commit time

proc pgChange(): seq[byte] =
  # Opaque to the library, which only inspects Begin/Commit.
  @[byte('I'), 0, 0, 0, 1]

proc xlog(lsn: int64, data: seq[byte]): seq[byte] =
  buildXLogData(lsn, lsn, 0, data)

# Server-side observations, kept at module scope for the gcsafe callbacks.
var startQuery: string
var statusFlushes: seq[int64] # flush field of each Standby Status Update
var afterCopyDone: seq[char] # frontend messages after the client's CopyDone
var seenConfirmed: seq[Lsn] # conn.confirmedFlushLsn observed in the callback

proc resetObserved() =
  startQuery = ""
  statusFlushes.setLen(0)
  afterCopyDone.setLen(0)
  seenConfirmed.setLen(0)

proc serveStream(
    st: MockClient, burst: seq[byte], slotLsn = "", lookup = true
): Future[void] {.async.} =
  ## Answer the slot lookup with `slotLsn` (no row when empty) unless `lookup`
  ## is off, then stream `burst` and end with CopyDone + ReadyForQuery,
  ## recording the client's status updates up to its CopyDone and whatever it
  ## sends after it.
  var m: tuple[msgType: char, body: seq[byte]]
  if lookup:
    m = await drainFrontendMessage(st)
    doAssert m.msgType == 'Q' and "pg_replication_slots" in queryText(m.body)
    var reply: seq[byte]
    reply.add(buildRowDescription("confirmed_flush_lsn"))
    if slotLsn.len > 0:
      reply.add(buildDataRow(slotLsn))
      reply.add(buildCommandComplete("SELECT 1"))
    else:
      reply.add(buildCommandComplete("SELECT 0"))
    reply.add(buildReadyForQuery('I'))
    await sendBytes(st, reply)
  m = await drainFrontendMessage(st)
  {.cast(gcsafe).}:
    startQuery = queryText(m.body)
  var all = buildCopyBothResponse()
  all.add(burst)
  all.add(buildCopyDone())
  all.add(buildReadyForQuery('I'))
  await sendBytes(st, all)
  while true:
    let f = await drainFrontendMessage(st)
    if f.msgType == 'c':
      break
    if f.msgType == 'd':
      {.cast(gcsafe).}:
        statusFlushes.add(decodeStandbyStatus(f.body).flush)
  # The client's Terminate on close, or anything it wrongly sent before it.
  while true:
    let f =
      try:
        await drainFrontendMessage(st)
      except CatchableError:
        break
    {.cast(gcsafe).}:
      afterCopyDone.add(f.msgType)
    if f.msgType == 'X':
      break
  await closeClient(st)

template runStreamWith(
    burst: seq[byte], startLsn: Lsn, slotLsn: string, auto: bool, cbBody: untyped
) =
  proc testBody(data: seq[byte], lookup: string) {.async.} =
    let ms = startMockServer()
    proc serverHandler() {.async.} =
      let st = await acceptAndReady(ms)
      await serveStream(st, data, lookup, auto)

    let serverFut = serverHandler()
    let conn {.inject.} = await connect(mockConfig(ms.port))
    let cb = makeReplicationCallback:
      {.cast(gcsafe).}:
        cbBody
    await conn.startReplication(
      "test_slot", startLsn, options = pubOptions, autoConfirm = auto, callback = cb
    )
    await conn.close()
    await serverFut
    await closeServer(ms)

  waitFor testBody(burst, slotLsn)

template runStream(burst: seq[byte], startLsn: Lsn, slotLsn: string, cbBody: untyped) =
  runStreamWith(burst, startLsn, slotLsn, true, cbBody)

suite "Replication: autoConfirm":
  test "a Commit is confirmed at its endLsn":
    resetObserved()
    var burst = xlog(startPos, pgBegin())
    burst.add(xlog(0x1800, pgChange()))
    burst.add(xlog(0x2000, pgCommit(0x2000)))
    runStream(burst, Lsn(startPos), ""):
      discard msg
    # Only the stop mirror reports, carrying the commit.
    check statusFlushes == @[0x2000'i64]

  test "the Commit is confirmed only after its callback returned":
    resetObserved()
    var burst = xlog(startPos, pgBegin())
    burst.add(xlog(0x2000, pgCommit(0x2000)))
    runStream(burst, Lsn(startPos), ""):
      if msg.kind == rmkXLogData and msg.xlogData.data[0] == byte('C'):
        seenConfirmed.add(conn.confirmedFlushLsn)
    check seenConfirmed == @[Lsn(startPos)]
    check statusFlushes == @[0x2000'i64]

  test "a keepalive inside a transaction is not confirmed":
    resetObserved()
    var burst = xlog(startPos, pgBegin())
    burst.add(xlog(0x1800, pgChange()))
    burst.add(buildKeepalive(0x1800, 0, replyRequested = true))
    burst.add(xlog(0x2000, pgCommit(0x2000)))
    runStream(burst, Lsn(startPos), ""):
      discard msg
    check statusFlushes == @[startPos, 0x2000'i64]

  test "a keepalive outside a transaction is confirmed and reported at once":
    resetObserved()
    var burst = buildKeepalive(0x3000, 0, replyRequested = false)
    # Not advancing: no second immediate report.
    burst.add(buildKeepalive(0x3000, 0, replyRequested = false))
    runStream(burst, Lsn(startPos), ""):
      discard msg
    check statusFlushes == @[0x3000'i64, 0x3000'i64]

  test "a Commit confirmed since the last status is reported on the next keepalive":
    # An idle walsender's keepalive walEnd equals the commit's end, so it does
    # not advance the confirmed position itself.
    resetObserved()
    var burst = xlog(startPos, pgBegin())
    burst.add(xlog(0x2000, pgCommit(0x2000)))
    burst.add(buildKeepalive(0x2000, 0, replyRequested = false))
    burst.add(buildKeepalive(0x2000, 0, replyRequested = false))
    runStream(burst, Lsn(startPos), ""):
      discard msg
    # Once reported, the repeated keepalive sends nothing; then the stop mirror.
    check statusFlushes == @[0x2000'i64, 0x2000'i64]

  test "a Commit with a zero endLsn still ends the transaction":
    resetObserved()
    var burst = xlog(startPos, pgBegin())
    burst.add(xlog(0x1800, pgCommit(0)))
    burst.add(buildKeepalive(0x3000, 0, replyRequested = false))
    runStream(burst, Lsn(startPos), ""):
      discard msg
    check statusFlushes == @[0x3000'i64, 0x3000'i64]

  test "nothing is confirmed or reported after stopReplication":
    resetObserved()
    var burst = xlog(startPos, pgBegin())
    burst.add(xlog(0x2000, pgCommit(0x2000)))
    burst.add(buildKeepalive(0x2000, 0, replyRequested = false))
    # Buffered behind the stop: must be neither confirmed nor reported.
    burst.add(xlog(0x2800, pgBegin()))
    burst.add(xlog(0x3000, pgCommit(0x3000)))
    # Asks for a reply: the auto-reply must not follow the client's CopyDone.
    burst.add(buildKeepalive(0x4000, 0, replyRequested = true))
    var stopped = false
    var manualConfirms: seq[bool]
    runStream(burst, Lsn(startPos), ""):
      if stopped and msg.kind == rmkXLogData:
        # The manual API refuses too: the position could no longer be reported.
        manualConfirms.add(conn.confirmFlushed(msg.xlogData.startLsn))
      if msg.kind == rmkPrimaryKeepalive:
        if stopped:
          seenConfirmed.add(conn.confirmedFlushLsn)
        else:
          stopped = true
          await conn.stopReplication()
    # The keepalive's report of the commit, then the stop's own status.
    check statusFlushes == @[0x2000'i64, 0x2000'i64]
    check seenConfirmed == @[Lsn(0x2000)]
    check manualConfirms == @[false, false]
    check afterCopyDone == @['X']

  test "a stop from a Commit's callback still reports that Commit":
    resetObserved()
    var burst = xlog(startPos, pgBegin())
    burst.add(xlog(0x2000, pgCommit(0x2000)))
    # Buffered behind the stop: must not be confirmed.
    burst.add(buildKeepalive(0x3000, 0, replyRequested = false))
    var stopReturned = false
    runStream(burst, Lsn(startPos), ""):
      if msg.kind == rmkXLogData and msg.xlogData.data[0] == byte('C'):
        await conn.stopReplication()
        stopReturned = true
    check stopReturned
    check statusFlushes == @[0x2000'i64]
    check afterCopyDone == @['X']

  test "confirmFlushed after stopReplication in the same callback is reported":
    # Without autoConfirm too: the final status waits for the callback.
    resetObserved()
    let burst = xlog(0x2000, pgChange())
    var manualConfirms: seq[bool]
    runStreamWith(burst, Lsn(startPos), "", false):
      if msg.kind == rmkXLogData:
        await conn.stopReplication()
        manualConfirms.add(conn.confirmFlushed(msg.xlogData.startLsn))
    check manualConfirms == @[true]
    check statusFlushes == @[0x2000'i64]
    check afterCopyDone == @['X']

  test "InvalidLsn starts from the slot's confirmed_flush_lsn":
    # A keepalive trailing the slot's position (the walsender re-reading WAL
    # from restart_lsn) must not be reported as flushed.
    resetObserved()
    let burst = buildKeepalive(0x4000, 0, replyRequested = true)
    runStream(burst, InvalidLsn, "0/5000"):
      discard msg
    check "LOGICAL 0/5000" in startQuery
    check statusFlushes == @[0x5000'i64, 0x5000'i64]

  test "an explicit startLsn older than the slot starts from the slot":
    resetObserved()
    let burst = buildKeepalive(0x4000, 0, replyRequested = true)
    runStream(burst, Lsn(0x1000), "0/5000"):
      discard msg
    check "LOGICAL 0/5000" in startQuery
    check statusFlushes == @[0x5000'i64, 0x5000'i64]

  test "an explicit startLsn past the slot is kept":
    resetObserved()
    let burst = buildKeepalive(0x6000, 0, replyRequested = true)
    runStream(burst, Lsn(0x6000), "0/5000"):
      discard msg
    check "LOGICAL 0/6000" in startQuery
    check statusFlushes == @[0x6000'i64, 0x6000'i64]

  test "a start past the slot is reported without being asked":
    # The server still holds the slot's position, so the start counts as news.
    resetObserved()
    let burst = buildKeepalive(0x6000, 0, replyRequested = false)
    runStream(burst, Lsn(0x6000), "0/5000"):
      discard msg
    check statusFlushes == @[0x6000'i64, 0x6000'i64]

  test "callback stays callable as the last positional argument":
    # autoConfirm follows callback, so calls written before it still compile.
    let cb = makeReplicationCallback:
      discard msg
    check compiles(
      PgConnection().startReplication(
        "test_slot", InvalidLsn, pubOptions, true, ZeroDuration, cb
      )
    )

  test "requires publication_names and autoKeepaliveReply":
    proc attempt(options: seq[(string, string)], autoReply: bool) {.async.} =
      let cb = makeReplicationCallback:
        discard msg
      await PgConnection().startReplication(
        "test_slot",
        options = options,
        autoKeepaliveReply = autoReply,
        autoConfirm = true,
        callback = cb,
      )

    expect ValueError:
      waitFor attempt(@[], true)
    # An invalid key fails before the slot lookup would touch the wire.
    expect ValueError:
      waitFor attempt(@{"publication_names": "p1", "bad-key": "x"}, true)
    # So does a value the query string cannot carry.
    expect ValueError:
      waitFor attempt(@{"publication_names": "p1\0"}, true)
    expect ValueError:
      waitFor attempt(pubOptions, false)

var foldFlushes: seq[int64]

suite "Replication: autoConfirm and a pending stop":
  test "a Commit returning while the stop's status waits is still reported":
    # The stop is queued behind a frame still being written, so its final
    # status is not encoded yet and carries the Commit confirmed after it.
    foldFlushes.setLen(0)
    var bulk, stop: Future[void]

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # slot lookup
        var reply: seq[byte]
        reply.add(buildRowDescription("confirmed_flush_lsn"))
        reply.add(buildCommandComplete("SELECT 0"))
        reply.add(buildReadyForQuery('I'))
        await sendBytes(st, reply)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst = buildCopyBothResponse()
        burst.add(xlog(startPos, pgBegin()))
        burst.add(xlog(0x2000, pgCommit(0x2000)))
        await sendBytes(st, burst)
        # Read nothing for a while so the bulk frame stays in flight.
        await sleepAsync(milliseconds(300))
        while true:
          let m =
            try:
              await drainFrontendMessage(st)
            except CatchableError:
              break
          if m.msgType == 'd' and m.body.len > 0 and m.body[0] == byte('r'):
            {.cast(gcsafe).}:
              foldFlushes.add(decodeStandbyStatus(m.body).flush)
          if m.msgType == 'c':
            var tail = buildCopyDone()
            tail.add(buildReadyForQuery('I'))
            await sendBytes(st, tail)
          if m.msgType == 'X':
            break
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        if msg.kind == rmkXLogData and msg.xlogData.data[0] == byte('B'):
          {.cast(gcsafe).}:
            bulk = conn.sendCopyData(newSeq[byte](32 * 1024 * 1024))
            stop = conn.stopReplication()
      await conn.startReplication(
        "test_slot",
        Lsn(startPos),
        options = pubOptions,
        autoConfirm = true,
        callback = cb,
      )
      {.cast(gcsafe).}:
        await bulk
        await stop
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check foldFlushes == @[0x2000'i64]

when hasChronos:
  # asyncdispatch drains the callback queue, callbacks added while draining
  # included, before polling I/O, so a task spinning on yieldTick starves it.
  proc yieldTick(): Future[void] =
    ## Resume on the next event loop tick, between other ready continuations.
    let fut = newFuture[void]("yieldTick")
    scheduleSoon(
      proc() {.gcsafe, raises: [].} =
        fut.complete()
    )
    fut

  var lookupAnswered = false

  suite "Replication: autoConfirm slot lookup":
    test "a query taking the connection after the lookup is not trampled":
      # The lookup leaves the connection ready across a suspension. A task that
      # takes it then must make startReplication fail, not have START_REPLICATION
      # interleaved with its query.
      lookupAnswered = false
      var startFailed, otherSucceeded = false

      proc testBody() {.async.} =
        let ms = startMockServer()

        proc serverHandler() {.async.} =
          let st = await acceptAndReady(ms)
          discard await drainFrontendMessage(st) # slot lookup
          var reply: seq[byte]
          reply.add(buildRowDescription("confirmed_flush_lsn"))
          reply.add(buildDataRow("0/5000"))
          reply.add(buildCommandComplete("SELECT 1"))
          reply.add(buildReadyForQuery('I'))
          {.cast(gcsafe).}:
            lookupAnswered = true
          await sendBytes(st, reply)
          while true:
            let m =
              try:
                await drainFrontendMessage(st)
              except CatchableError:
                break
            if m.msgType == 'X':
              break
            if m.msgType == 'Q':
              var ok: seq[byte]
              ok.add(buildCommandComplete("SELECT 0"))
              ok.add(buildReadyForQuery('I'))
              await sendBytes(st, ok)
          await closeClient(st)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))

        proc interloper() {.async.} =
          while not (lookupAnswered and conn.state == csReady):
            await yieldTick()
          discard await conn.simpleQuery("SELECT 1")
          otherSucceeded = true

        let otherFut = interloper()
        let cb = makeReplicationCallback:
          discard msg
        try:
          await conn.startReplication(
            "test_slot", options = pubOptions, autoConfirm = true, callback = cb
          )
        except PgStateError:
          startFailed = true
        await otherFut
        await conn.close()
        await serverFut
        await closeServer(ms)

      waitFor testBody()
      check startFailed
      check otherSucceeded
