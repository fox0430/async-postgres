## Regression tests for the CancelledError paths of the transaction macros.
## chronos-only: asyncdispatch has no real cancellation on these paths.

import ../async_postgres/async_backend

when hasChronos:
  import std/[deques, unittest, tables, importutils]

  import pkg/chronos/streams/asyncstream

  import ../async_postgres/[pg_protocol, pg_connection, pg_client]
  import ../async_postgres/pg_pool {.all.}

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

  proc makeMinimalPool(): PgPool =
    ## Minimal PgPool wiring for `pool.withTransaction`; no maintenance/waiters.
    PgPool(
      config: PoolConfig(
        connConfig: ConnConfig(host: "127.0.0.1", port: 5432),
        minSize: 0,
        maxSize: 1,
        maxWaiters: -1,
        maintenanceInterval: seconds(30),
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
