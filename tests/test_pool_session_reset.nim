## The pool's session reset: the default `resetQuery` on each release, raw
## releases resetting in the background, the client-side state a release puts
## back, and the reset that follows `afterConnect`.

import std/[unittest, sets, strutils, importutils, deques]

import ../async_postgres/async_backend
import ../async_postgres/[pg_protocol, pg_connection]
import ../async_postgres/pg_connection/[types, simple_query]
import ../async_postgres/pg_pool {.all.}

import mock_pg_server

privateAccess(PgPool)
privateAccess(PooledConn)

type Script = ref object ## How `serve` answers, and what it saw.
  log: seq[string]
  gotReset: Future[void] ## Completed when a reset Query arrives.
  replyReset: Future[void] ## Awaited before answering one; nil answers at once.
  failReset: bool
  zoneAfterReset: string ## TimeZone a reset reports; "" reports none.
  notifyInReset: bool ## Lead a reset's reply with a NotificationResponse.

proc newScript(): Script =
  Script(gotReset: newFuture[void]("gotReset"))

proc resetReply(sc: Script): seq[byte] =
  if sc.failReset:
    return buildErrorResponse("XX000", "reset failed") & buildReadyForQuery('I')
  if sc.notifyInReset:
    result.add(buildNotificationResponse(1, "ch", "late"))
  for tag in ["CLOSE CURSOR ALL", "SET"]:
    result.add(buildCommandComplete(tag))
  if sc.zoneAfterReset.len > 0:
    result.add(buildParameterStatus("TimeZone", sc.zoneAfterReset))
  result.add(buildCommandComplete("RESET"))
  result.add(buildCommandComplete("UNLISTEN"))
  result.add(buildRowDescription("pg_advisory_unlock_all"))
  result.add(buildDataRow("t"))
  result.add(buildCommandComplete("SELECT 1"))
  for tag in ["DISCARD TEMP", "DISCARD SEQUENCES"]:
    result.add(buildCommandComplete(tag))
  result.add(buildReadyForQuery('I'))

proc serve(ms: MockServer, sc: Script, params: seq[(string, string)] = @[]) {.async.} =
  ## Answer one connection's simple Queries, logging each, then its Terminate.
  let st = await acceptAndReady(ms, params = params)
  try:
    while true:
      let (msgType, body) = await drainFrontendMessage(st)
      if msgType == 'X':
        sc.log.add("<terminate>")
        break
      if msgType != 'Q':
        continue
      let sql = queryText(body)
      sc.log.add(sql)
      var resp: seq[byte]
      if sql == DefaultResetQuery:
        if not sc.gotReset.finished:
          sc.gotReset.complete()
        if sc.replyReset != nil:
          await sc.replyReset
        resp = sc.resetReply()
      elif sql.startsWith("SET TimeZone TO '"):
        let zone = sql["SET TimeZone TO '".len ..^ 2]
        resp =
          buildParameterStatus("TimeZone", zone) & buildCommandComplete("SET") &
          buildReadyForQuery('I')
      else:
        resp = buildCommandComplete("SET") & buildReadyForQuery('I')
      await sendBytes(st, resp)
  except CatchableError:
    discard
  await closeClient(st)

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

template eventually(cond: untyped): bool =
  ## `cond` within 5 s, polled.
  block:
    let deadline = Moment.now() + seconds(5)
    while not (cond) and Moment.now() < deadline:
      await sleepAsync(milliseconds(5))
    cond

suite "Pool session reset":
  test "a PoolConfig built directly bounds its reset":
    let cfg = PoolConfig(connConfig: mockConfig(5432), maxSize: 1)
    doAssert cfg.resetQuery == DefaultResetQuery
    doAssert cfg.resetQueryTimeout == seconds(5)

  test "withConnection returns before its reset, which runs once":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.replyReset = newFuture[void]("replyReset")
      let serverFut = serve(ms, sc)
      let pool = await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0))
      proc borrow() {.async.} =
        pool.withConnection(c):
          discard await c.simpleExec("SET ROLE app")

      await borrow().wait(seconds(5))
      await sc.gotReset.wait(seconds(5))
      doAssert pool.idle.len == 0
      doAssert pool.active == 1
      sc.replyReset.complete()
      doAssert eventually(pool.idle.len == 1)
      doAssert pool.active == 0
      doAssert sc.log == @["SET ROLE app", DefaultResetQuery], $sc.log
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a raw release resets in the background and holds the slot until it ends":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.replyReset = newFuture[void]("replyReset")
      let serverFut = serve(ms, sc)
      let pool = await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0))
      let conn = await pool.acquire()
      conn.release()
      doAssert pool.idle.len == 0
      doAssert pool.active == 1
      await sc.gotReset.wait(seconds(5))
      doAssert pool.idle.len == 0
      doAssert pool.active == 1
      sc.replyReset.complete()
      doAssert eventually(pool.idle.len == 1)
      doAssert pool.active == 0
      doAssert sc.log == @[DefaultResetQuery], $sc.log
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a queued acquire gets a released connection only after its reset":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.replyReset = newFuture[void]("replyReset")
      let serverFut = serve(ms, sc)
      let pool =
        await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0, maxSize = 1))
      let conn = await pool.acquire()
      let waiter = pool.acquire()
      doAssert not waiter.finished
      conn.release()
      await sc.gotReset.wait(seconds(5))
      doAssert not waiter.finished
      sc.replyReset.complete()
      let again = await waiter.wait(seconds(5))
      doAssert again == conn
      await pool.resetSessionAndRelease(again)
      doAssert eventually(pool.idle.len == 1)
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a failed background reset closes the connection and reaches onPoolCloseError":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.failReset = true
      let serverFut = serve(ms, sc)
      let errs = new seq[string]
      let tracer = PgTracer()
      tracer.onPoolCloseError = proc(
          data: TracePoolCloseErrorData
      ) {.gcsafe, raises: [].} =
        errs[].add(data.err.msg)
      let pool = await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0))
      pool.config.tracer = tracer
      let conn = await pool.acquire()
      conn.release()
      doAssert eventually(pool.active == 0)
      doAssert pool.idle.len == 0
      doAssert pool.metrics.closeCount == 1
      doAssert errs[].len == 1, $errs[]
      doAssert "reset failed" in errs[][0], errs[][0]
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "an empty resetQuery parks a raw release at once":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      let serverFut = serve(ms, sc)
      let pool =
        await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0, resetQuery = ""))
      let conn = await pool.acquire()
      conn.release()
      doAssert pool.idle.len == 1
      doAssert pool.active == 0
      await pool.close()
      await serverFut.wait(seconds(5))
      doAssert sc.log == @["<terminate>"], $sc.log
      await closeServer(ms)

    waitFor t()

  test "close cuts a background reset short and closes the connection":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.replyReset = newFuture[void]("replyReset")
      let serverFut = serve(ms, sc)
      # Unbounded, so only the cut can end the reset.
      let pool = await newPool(
        initPoolConfig(
          mockConfig(ms.port), minSize = 0, resetQueryTimeout = ZeroDuration
        )
      )
      let conn = await pool.acquire()
      conn.release()
      await sc.gotReset.wait(seconds(5))
      let closeFut = pool.close()
      when hasChronos:
        await closeFut.wait(seconds(5))
        doAssert conn.state == csClosed
      else:
        # asyncdispatch cannot cancel the reset: close waits for it.
        await sleepAsync(milliseconds(50))
        doAssert not closeFut.finished
      sc.replyReset.complete()
      await closeFut.wait(seconds(5))
      await serverFut.wait(seconds(5))
      when hasChronos:
        doAssert sc.log == @[DefaultResetQuery], $sc.log
      else:
        doAssert sc.log == @[DefaultResetQuery, "<terminate>"], $sc.log
      doAssert pool.idle.len == 0
      doAssert pool.active == 0
      await closeServer(ms)

    waitFor t()

  test "the default reset unlocks leaked session locks in its own round trip":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      let serverFut = serve(ms, sc)
      let leaks = new int
      let tracer = PgTracer()
      tracer.onLeakedSessionLocks = proc(
          data: TraceLeakedSessionLocksData
      ) {.gcsafe, raises: [].} =
        inc leaks[]
      let pool = await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0))
      pool.config.tracer = tracer
      pool.withConnection(c):
        c.noteSessionLockAcquired()
      doAssert eventually(pool.idle.len == 1)
      doAssert sc.log == @[DefaultResetQuery], $sc.log
      doAssert leaks[] == 1
      doAssert not pool.idle[0].conn.sessionLockDirty
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a raw release unlocks leaked session locks even with no resetQuery":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      let serverFut = serve(ms, sc)
      let pool =
        await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0, resetQuery = ""))
      let conn = await pool.acquire()
      conn.noteSessionLockAcquired()
      conn.release()
      doAssert pool.idle.len == 0
      doAssert eventually(pool.idle.len == 1)
      doAssert pool.active == 0
      doAssert pool.metrics.closeCount == 0
      doAssert not conn.sessionLockDirty
      doAssert sc.log == @["SELECT pg_catalog.pg_advisory_unlock_all()"], $sc.log
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a release puts back the callbacks afterConnect left and drops notifications":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      let serverFut = serve(ms, sc)
      let hookNotice: NoticeCallback = proc(n: Notice) {.gcsafe, raises: [].} =
        discard
      let hook = makeAfterConnectHook:
        conn.onNotice(hookNotice)
      let pool = await newPool(
        initPoolConfig(
          mockConfig(ms.port), minSize = 0, resetQuery = "", afterConnect = hook
        )
      )
      let conn = await pool.acquire()
      doAssert conn.noticeCallback == hookNotice
      conn.onNotice(
        proc(n: Notice) {.gcsafe, raises: [].} =
          discard
      )
      conn.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          discard
      )
      conn.listenChannels.incl("ch")
      conn.enqueueNotification(Notification(channel: "ch", payload: "p"))
      conn.release()
      doAssert pool.idle.len == 1
      doAssert conn.noticeCallback == hookNotice
      doAssert conn.notifyCallback == nil
      doAssert conn.listenChannels.len == 0
      doAssert not conn.hasNotification
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a notification the background reset reads skips the borrower's callback":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      let serverFut = serve(ms, sc)
      let hookGot = new int
      let borrowerGot = new int
      let hook = makeAfterConnectHook:
        conn.onNotify(
          proc(n: Notification) {.gcsafe, raises: [].} =
            inc hookGot[]
        )
      let pool = await newPool(
        initPoolConfig(mockConfig(ms.port), minSize = 0, afterConnect = hook)
      )
      let conn = await pool.acquire()
      conn.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          inc borrowerGot[]
      )
      sc.notifyInReset = true
      conn.release()
      doAssert eventually(pool.idle.len == 1)
      doAssert borrowerGot[] == 0
      doAssert hookGot[] == 1
      doAssert not conn.hasNotification
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "afterConnect is followed by a reset before the first borrower":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      let serverFut = serve(ms, sc)
      let hook = makeAfterConnectHook:
        discard await conn.simpleExec("SET search_path TO app")
      let pool = await newPool(
        initPoolConfig(mockConfig(ms.port), minSize = 0, afterConnect = hook)
      )
      let conn = await pool.acquire()
      doAssert sc.log == @["SET search_path TO app", DefaultResetQuery], $sc.log
      await pool.resetSessionAndRelease(conn)
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "the reset after afterConnect sets the hook's TimeZone back":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.zoneAfterReset = "UTC"
      let serverFut = serve(ms, sc, params = @[("TimeZone", "UTC")])
      let hook = makeAfterConnectHook:
        discard await conn.simpleExec("SET TimeZone TO 'Asia/Tokyo'")
      let pool = await newPool(
        initPoolConfig(mockConfig(ms.port), minSize = 0, afterConnect = hook)
      )
      let conn = await pool.acquire()
      let zoneSet = "SET TimeZone TO 'Asia/Tokyo'"
      doAssert sc.log == @[zoneSet, DefaultResetQuery, zoneSet], $sc.log
      doAssert not conn.timeZoneChanged
      await pool.resetSessionAndRelease(conn)
      doAssert eventually(pool.idle.len == 1)
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a failed reset after afterConnect fails the acquire":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.failReset = true
      let serverFut = serve(ms, sc)
      let hook = makeAfterConnectHook:
        discard
      let pool = await newPool(
        initPoolConfig(mockConfig(ms.port), minSize = 0, afterConnect = hook)
      )
      var err: ref PgPoolError
      try:
        discard await pool.acquire()
      except PgPoolError as e:
        err = e
      doAssert err != nil
      doAssert err.kind == pekAfterConnectFailed, $err.kind
      doAssert err.parent != nil
      doAssert "session reset after afterConnect failed" in err.parent.msg,
        err.parent.msg
      doAssert pool.active == 0
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a timeout after a pool proc succeeded cannot land on its reset":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.replyReset = newFuture[void]("replyReset")
      let serverFut = serve(ms, sc)
      let pool = await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0))
      let r = await pool.simpleExec("SET ROLE app").wait(milliseconds(500))
      doAssert r.commandTag == "SET"
      await sc.gotReset.wait(seconds(5))
      doAssert pool.active == 1
      sc.replyReset.complete()
      doAssert eventually(pool.idle.len == 1)
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()

  test "a failed resetSession and the release report leaked locks once":
    proc t() {.async.} =
      let ms = startMockServer()
      let sc = newScript()
      sc.failReset = true
      let serverFut = serve(ms, sc)
      let leaks = new int
      let tracer = PgTracer()
      tracer.onLeakedSessionLocks = proc(
          data: TraceLeakedSessionLocksData
      ) {.gcsafe, raises: [].} =
        inc leaks[]
      let pool = await newPool(initPoolConfig(mockConfig(ms.port), minSize = 0))
      pool.config.tracer = tracer
      let conn = await pool.acquire()
      conn.noteSessionLockAcquired()
      await pool.resetSession(conn)
      doAssert conn.state == csClosed
      conn.release()
      doAssert leaks[] == 1
      doAssert pool.active == 0
      await pool.close()
      await serverFut.wait(seconds(5))
      await closeServer(ms)

    waitFor t()
