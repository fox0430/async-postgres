## Regression tests for the CancelledError paths of the transaction macros.
## chronos-only: asyncdispatch has no real cancellation on these paths.

import ../async_postgres/async_backend

when hasChronos:
  import std/[deques, unittest, tables, importutils]

  import pkg/chronos/streams/asyncstream

  import ../async_postgres/[pg_protocol, pg_connection, pg_client]
  import ../async_postgres/pg_pool {.all.}
  # Not on `pg_connection`'s re-export whitelist: the cancel-path invalidation
  # is reached by the macros through `bindSym`, never by name from user code.
  from ../async_postgres/pg_connection/simple_query import invalidateOnCancel

  import mock_pg_server

  privateAccess(PgConnection)
  privateAccess(PgTracer)
  privateAccess(PgPool)
  privateAccess(PooledConn)
  privateAccess(Waiter)

  proc makeScriptedConn(): Future[(PgConnection, StreamServer, StreamTransport)] {.
      async
  .} =
    ## PgConnection over a localhost TCP pair; caller drives the server side via
    ## the returned transport. Requests hang until the test writes a reply.
    let server = createStreamServer(initTAddress("127.0.0.1", 0))
    let transport = await connect(server.localAddress())
    let serverTransport = await server.accept()
    let reader = newAsyncStreamReader(transport)
    let writer = newAsyncStreamWriter(transport)
    var conn = PgConnection(
      transport: transport,
      baseReader: reader,
      baseWriter: writer,
      reader: reader,
      writer: writer,
      recvBuf: @[],
      state: csReady,
      txStatus: tsIdle,
      serverParams: initTable[string, string](),
      createdAt: Moment.now(),
    )
    return (conn, server, serverTransport)

  proc cleanupScripted(
      server: StreamServer, serverTransport: StreamTransport
  ) {.async.} =
    await serverTransport.closeWait()
    server.stop()
    server.close()
    await server.join()

  proc preBufferBeginReply(serverTransport: StreamTransport) {.async.} =
    ## Pre-enqueue a BEGIN reply so simpleExec("BEGIN") completes on first await.
    discard await serverTransport.write(
      buildCommandComplete("BEGIN") & buildReadyForQuery('T')
    )

  proc makeMinimalPool(resetQuery = ""): PgPool =
    ## Minimal PgPool wiring for `pool.withTransaction`; no maintenance/waiters.
    ## `resetQuery` (with its timeout left at the zero default = no timeout)
    ## lets tests hang the release path on a scripted server.
    PgPool(
      config: PoolConfig(
        connConfig: ConnConfig(host: "127.0.0.1", port: 5432),
        minSize: 0,
        maxSize: 1,
        maxWaiters: -1,
        maintenanceInterval: seconds(30),
        resetQuery: resetQuery,
      ),
      idle: initDeque[PooledConn](),
      active: 0,
      waiters: initDeque[Waiter](),
      waiterCount: 0,
      closed: false,
    )

  suite "withTransaction cancellation paths":
    test "body raising CancelledError skips ROLLBACK cleanup":
      # Without the dedicated `except CancelledError`, cleanup would run
      # ROLLBACK on the hanging conn and `wait()` would fire AsyncTimeoutError.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        await preBufferBeginReply(sTx)

        {.push warning[UnreachableCode]: off.}
        proc runTx() {.async.} =
          conn.withTransaction:
            raise newException(CancelledError, "body cancelled")

        {.pop.}

        var sawCancel = false
        var sawTimeout = false
        try:
          await wait(runTx(), seconds(1))
        except CancelledError:
          sawCancel = true
        except AsyncTimeoutError:
          sawTimeout = true

        doAssert sawCancel, "expected CancelledError to propagate immediately"
        doAssert not sawTimeout, "cleanup ran and hung on ROLLBACK"

        await cleanupScripted(server, sTx)

      waitFor t()

    test "cleanup CancelledError does not mask the body error":
      # Body raises ValueError; ROLLBACK hangs; outer cancel cancels ROLLBACK.
      # Cleanup-cancel must be swallowed so ValueError propagates instead of a
      # fresh chronos CancelledError. Trace records it as csrCleanupFailed.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        await preBufferBeginReply(sTx)

        var skipped: seq[tuple[reason: CleanupSkipReason, errName: string]]
        # fireCleanupSkipped reads from conn.config.tracer, not conn.tracer.
        conn.config.tracer = PgTracer(
          onCleanupSkipped: proc(data: TraceCleanupSkippedData) {.gcsafe, raises: [].} =
            skipped.add(
              (
                reason: data.reason,
                errName: (if data.err.isNil: "" else: $data.err.name),
              )
            )
        )

        {.push warning[UnreachableCode]: off.}
        proc runTx() {.async.} =
          conn.withTransaction:
            raise newException(ValueError, "body boom")

        {.pop.}

        let fut = runTx()
        # Let BEGIN complete and ROLLBACK dispatch; the scripted server hangs.
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished, "runTx should be blocked on the hung ROLLBACK"
        await fut.cancelAndWait()

        doAssert fut.finished, "inner future should be settled after cancellation"

        let err = fut.readError()
        doAssert err != nil
        doAssert err of ValueError,
          "body error should propagate, not the cleanup-cancel; got " & $err.name
        doAssert err.msg == "body boom"

        doAssert skipped.len == 1
        doAssert skipped[0].reason == csrCleanupFailed
        doAssert skipped[0].errName == "CancelledError"

        await cleanupScripted(server, sTx)

      waitFor t()

    test "withTransaction cancel of in-flight request marks conn csClosed":
      # body issues a simpleExec that hangs; an external cancel aborts it.
      # The cancel handler must mark csClosed (and best-effort CancelRequest)
      # because the server has a pending query — reusing the conn would
      # interleave a stale reply with the next borrower's request.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        await preBufferBeginReply(sTx)

        proc runTx() {.async.} =
          conn.withTransaction:
            discard await conn.simpleExec("SELECT 1")

        let fut = runTx()
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished, "runTx should be blocked on the hung SELECT"
        doAssert conn.state == csBusy,
          "expected csBusy while awaiting SELECT reply; got " & $conn.state
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError
        doAssert conn.state == csClosed,
          "cancel path must csClose the conn; got " & $conn.state

        await cleanupScripted(server, sTx)

      waitFor t()

    test "withSavepoint cancel of in-flight request marks conn csClosed":
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        # Pre-buffer SAVEPOINT reply so the savepoint frame is established.
        discard
          await sTx.write(buildCommandComplete("SAVEPOINT") & buildReadyForQuery('T'))
        # Simulate we're already inside a transaction: mark tsInTransaction.
        conn.txStatus = tsInTransaction

        proc runSp() {.async.} =
          conn.withSavepoint("sp1"):
            discard await conn.simpleExec("SELECT 1")

        let fut = runSp()
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished, "runSp should be blocked on the hung SELECT"
        doAssert conn.state == csBusy
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError
        doAssert conn.state == csClosed,
          "withSavepoint cancel path must csClose the conn; got " & $conn.state

        await cleanupScripted(server, sTx)

      waitFor t()

    test "withTransactionDeadline external cancel marks conn csClosed":
      # External cancel of the outer wait (buildDeadlineAwaitAndTimeout path).
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        await preBufferBeginReply(sTx)

        proc runTx() {.async.} =
          conn.withTransactionDeadline(seconds(60)):
            discard await conn.simpleExec("SELECT 1")

        let fut = runTx()
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished
        doAssert conn.state == csBusy
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError
        doAssert conn.state == csClosed,
          "withTransactionDeadline cancel path must csClose the conn; got " & $conn.state

        await cleanupScripted(server, sTx)

      waitFor t()

    test "withTransactionRetry cancel of in-flight request marks conn csClosed":
      # buildRetryTxLoop cancel path.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        await preBufferBeginReply(sTx)

        proc runTx() {.async.} =
          conn.withTransactionRetry(RetryOptions(maxAttempts: 3)):
            discard await conn.simpleExec("SELECT 1")

        let fut = runTx()
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished
        doAssert conn.state == csBusy
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError
        doAssert conn.state == csClosed,
          "withTransactionRetry cancel path must csClose the conn; got " & $conn.state

        await cleanupScripted(server, sTx)

      waitFor t()

    test "withTransactionRetryDeadline external cancel marks conn csClosed":
      # buildRetryDeadlineLoop cancel path (connForStateCheck != nil branch).
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        await preBufferBeginReply(sTx)

        proc runTx() {.async.} =
          conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), seconds(60)):
            discard await conn.simpleExec("SELECT 1")

        let fut = runTx()
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished
        doAssert conn.state == csBusy
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError
        doAssert conn.state == csClosed,
          "withTransactionRetryDeadline cancel path must csClose the conn; got " &
            $conn.state

        await cleanupScripted(server, sTx)

      waitFor t()

    test "pool.withTransaction body-cancel skips ROLLBACK cleanup":
      # Same guarantee as the conn-side variant on pool.withTransaction's inner try.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        await preBufferBeginReply(sTx)

        let pool = makeMinimalPool()
        conn.ownerPool = pool
        # Pre-seed so acquire() hands back the scripted conn instead of dialing.
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

        {.push warning[UnreachableCode]: off.}
        proc runTx() {.async.} =
          pool.withTransaction(txConn):
            raise newException(CancelledError, "body cancelled")

        {.pop.}

        var sawCancel = false
        var sawTimeout = false
        try:
          await wait(runTx(), seconds(1))
        except CancelledError:
          sawCancel = true
        except AsyncTimeoutError:
          sawTimeout = true

        doAssert sawCancel, "expected CancelledError to propagate immediately"
        doAssert not sawTimeout, "cleanup ran and hung on ROLLBACK"

        await cleanupScripted(server, sTx)

      waitFor t()

  suite "pool with* macro external cancellation":
    test "withConnection: cancel during release re-raises CancelledError":
      # buildReleaseAndReraise's `except CancelledError` arm: a cancel landing
      # on the release (resetQuery) await must propagate to the caller, and
      # the half-reset conn must be retired (csClosed) instead of returning
      # to the idle queue.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        let pool = makeMinimalPool(resetQuery = "SELECT 1")
        conn.ownerPool = pool
        # Pre-seed so acquire() hands back the scripted conn instead of dialing.
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

        proc run() {.async.} =
          pool.withConnection(conn):
            discard

        let fut = run()
        # The body completes instantly; the release now awaits the resetQuery
        # reply, which the scripted server never sends.
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished, "run should be blocked on the reset query"
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError,
          "release-path cancel must propagate, not be swallowed; got " &
            (if err.isNil: "nil" else: $err.name)
        doAssert conn.state == csClosed,
          "release-path cancel must retire the conn; got " & $conn.state
        doAssert pool.active == 0, "pool slot must not leak"
        doAssert pool.idle.len == 0, "half-reset conn must not return to idle"

        await cleanupScripted(server, sTx)

      waitFor t()

    test "withConnection: cancel of in-flight body releases the conn":
      # A cancel landing on the body's await is captured as the body error;
      # the release still runs (discarding the busy conn) and CancelledError
      # reaches the caller.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        let pool = makeMinimalPool()
        conn.ownerPool = pool
        # Pre-seed so acquire() hands back the scripted conn instead of dialing.
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

        proc run() {.async.} =
          pool.withConnection(conn):
            discard await conn.simpleQuery("SELECT 1")

        let fut = run()
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished, "run should be blocked on the hung SELECT"
        doAssert conn.state == csBusy
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError
        doAssert pool.active == 0, "pool slot must not leak"
        doAssert pool.idle.len == 0, "busy conn must be discarded, not idle"

        await cleanupScripted(server, sTx)

      waitFor t()

    test "runAndRelease: cancel during release re-raises CancelledError":
      # runAndReleaseImpl's `except CancelledError` arm — the shared release
      # path of the pooled exec/query family. A completed body plus a hung
      # resetQuery pins the cancel to the release phase.
      proc t() {.async.} =
        let (conn, server, sTx) = await makeScriptedConn()
        let pool = makeMinimalPool(resetQuery = "SELECT 1")
        conn.ownerPool = pool
        # Pre-seed so acquire() hands back the scripted conn instead of dialing.
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

        var bodyFut = newFuture[void]()
        bodyFut.complete()

        # runAndRelease does not acquire; the caller hands it a borrowed conn.
        let acquired = await pool.acquire()
        doAssert acquired == conn

        let fut = pool.runAndRelease(acquired, bodyFut)
        await sleepAsync(milliseconds(100))
        doAssert not fut.finished, "runAndRelease should be blocked on the reset query"
        await fut.cancelAndWait()

        doAssert fut.finished
        let err = fut.readError()
        doAssert err != nil and err of CancelledError,
          "release-path cancel must propagate; got " &
            (if err.isNil: "nil" else: $err.name)
        doAssert conn.state == csClosed,
          "release-path cancel must retire the conn; got " & $conn.state
        doAssert pool.active == 0, "pool slot must not leak"
        doAssert pool.idle.len == 0, "half-reset conn must not return to idle"

        await cleanupScripted(server, sTx)

      waitFor t()

  suite "invalidation is idempotent across nested cancel frames":
    proc startCancelCounter(counter: ref int): Future[StreamServer] {.async.} =
      ## Accepts CancelRequest dials and counts them. `cancel` opens one socket
      ## per dispatch, so the socket count is the dispatch count.
      let server = createStreamServer(initTAddress("127.0.0.1", 0))

      proc acceptLoop() {.async.} =
        while true:
          try:
            let t = await server.accept()
            counter[].inc
            await t.closeWait()
          except CatchableError:
            break

      asyncSpawn acceptLoop()
      return server

    test "a cancel that never reached the wire keeps the connection":
      # `awaitOrInvalidate` wraps the whole operation, so a cancellation can
      # land after the connection was taken busy but before its first write.
      # Nothing is outstanding on the server and the stream is untouched, so
      # retiring the connection would throw away a healthy one -- which for a
      # pooled conn under a cancelled `race` means throwing away the pool.
      proc t() {.async.} =
        var counter = new(int)
        let cancelServer = await startCancelCounter(counter)
        let (conn, server, sTx) = await makeScriptedConn()
        conn.host = "127.0.0.1"
        conn.port = int(cancelServer.localAddress().port)

        conn.state = csBusy
        conn.invalidateOnCancel()

        doAssert conn.state == csReady,
          "a cancel with nothing on the wire must hand the conn back; got " & $conn.state
        await sleepAsync(milliseconds(200))
        doAssert counter[] == 0,
          "nothing was sent, so there is nothing to cancel; got " & $counter[]

        await cleanupScripted(server, sTx)
        cancelServer.stop()
        cancelServer.close()
        await cancelServer.join()

      waitFor t()

    test "an expired deadline dispatches exactly one CancelRequest":
      # The deadline cancels the inner frame and times out the outer one, so
      # both `invalidateOnCancel` and `invalidateOnTimeout` run for a single
      # dead round trip. Without a shared guard each dialled its own socket.
      proc t() {.async.} =
        var counter = new(int)
        let cancelServer = await startCancelCounter(counter)
        let (conn, server, sTx) = await makeScriptedConn()
        conn.host = "127.0.0.1"
        conn.port = int(cancelServer.localAddress().port)
        await preBufferBeginReply(sTx)

        proc runTx() {.async.} =
          conn.withTransactionDeadline(milliseconds(100)):
            discard await conn.simpleExec("SELECT 1")

        var errName = "<none>"
        try:
          await runTx()
        except PgTimeoutError:
          errName = "PgTimeoutError"
        except CatchableError as e:
          errName = $e.name

        doAssert errName == "PgTimeoutError",
          "expired deadline must surface as PgTimeoutError; got " & errName
        doAssert conn.state == csClosed,
          "the dead round trip must retire the conn; got " & $conn.state

        # `cancelNoWait` is fire-and-forget: let every dispatch land before
        # counting, so a regression shows up as 2 rather than a flake.
        await sleepAsync(milliseconds(300))
        doAssert counter[] == 1, "expected exactly one CancelRequest, got " & $counter[]

        await cleanupScripted(server, sTx)
        cancelServer.stop()
        cancelServer.close()
        await cancelServer.join()

      waitFor t()
