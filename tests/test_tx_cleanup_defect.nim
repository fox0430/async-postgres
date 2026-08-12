## Regression tests for the Defect-handling arms of the transaction cleanup
## generators and the pool deadline macros' release path.
##
## A `Defect` raised by the cleanup SQL itself (emulated by a writer whose
## `write` raises `AssertionDefect`) must be swallowed and reported via
## `onCleanupSkipped(reason = csrCleanupFailed)` — it must never mask the
## original body error the enclosing `except` arm re-raises. The pool
## deadline macros must additionally wrap a release-path Defect in
## `PgPoolError` (the Defect as `parent`).
##
## The mock server replies `ReadyForQuery('T')` to the BEGIN / SAVEPOINT
## query so the cleanup guard admits the ROLLBACK attempt; the failing
## writer stops the cleanup SQL before it reaches the wire.

import std/[unittest, importutils]

import ../async_postgres/async_backend

import ../async_postgres/[pg_client, pg_connection]
import ../async_postgres/pg_pool {.all.}

import mock_pg_server

privateAccess(PgConnection)
privateAccess(PgPool)
privateAccess(PooledConn)

when hasChronos:
  type CleanupSkippedRec = object
    kind: CleanupKind
    reason: CleanupSkipReason
    parentMsg: string

  proc cleanupSkippedTracer(): (PgTracer, ref seq[CleanupSkippedRec]) =
    ## Records `onCleanupSkipped` events (kind + reason) and, for
    ## `csrCleanupFailed`, the message of the failure's parent exception
    ## (the cleanup Defect wrapped in the reported `PgError`).
    let log = new seq[CleanupSkippedRec]
    let tracer = PgTracer()
    tracer.onCleanupSkipped = proc(
        data: TraceCleanupSkippedData
    ) {.gcsafe, raises: [].} =
      var parentMsg = ""
      let err = data.err
      if err != nil:
        let pe = (ref PgError)(err)
        if pe.parent != nil:
          parentMsg = pe.parent.msg
      log[].add(
        CleanupSkippedRec(kind: data.kind, reason: data.reason, parentMsg: parentMsg)
      )
    (tracer, log)

  proc mockConfig(port: int, tracer: PgTracer): ConnConfig =
    ConnConfig(
      host: "127.0.0.1",
      port: port,
      user: "test",
      database: "test",
      sslMode: sslDisable,
      tracer: tracer,
    )

  proc makePool(
      maxSize: int = 5, resetQuery: string = "", pipelined: bool = false
  ): PgPool =
    PgPool(
      config: PoolConfig(
        connConfig: ConnConfig(host: "127.0.0.1", port: 5432),
        minSize: 0,
        maxSize: maxSize,
        maxWaiters: -1,
        maintenanceInterval: seconds(30),
        resetQuery: resetQuery,
        resetQueryTimeout: seconds(5),
        pipelined: pipelined,
      ),
      idle: initDeque[PooledConn](),
      active: 0,
      waiters: initDeque[Waiter](),
      waiterCount: 0,
      closed: false,
    )

  proc toPooled(conn: PgConnection): PooledConn =
    PooledConn(conn: conn, lastUsedAt: Moment.now())

  proc readQueryMessage(client: MockClient): Future[seq[byte]] {.async.} =
    ## Read one simple-protocol query message ('Q' + int32 length + body).
    let hdr = await client.readN(5)
    doAssert hdr[0] == 'Q'.byte, "expected a simple-query message"
    let bodyLen =
      (int(hdr[1]) shl 24) or (int(hdr[2]) shl 16) or (int(hdr[3]) shl 8) or int(hdr[4])
    await client.readN(bodyLen - 4)

  proc buildExecReplies(n: int): seq[byte] =
    ## Extended-protocol replies for `n` sequential exec ops with per-op SYNC
    ## (executeIsolated): ParseComplete + BindComplete + CommandComplete +
    ## ReadyForQuery per op. The mock writer swallows the client's Parse/Bind,
    ## so the server pre-sends the sequence for the client's pump (an extra
    ## ParseComplete is harmless when a shared statement skips its Parse).
    for i in 0 ..< n:
      result.add(buildBackendMsg('1', @[]))
      result.add(buildBackendMsg('2', @[]))
      result.add(buildCommandComplete("SELECT 1"))
      result.add(buildReadyForQuery('I'))

  suite "Transaction cleanup Defect arms":
    test "withTransaction: ROLLBACK Defect is reported and never masks the body error":
      proc t() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # Read BEGIN (delivered by the real writer) and reply in-transaction
          # so the cleanup guard admits the ROLLBACK attempt.
          discard await readQueryMessage(serverClient)
          await serverClient.sendBytes(buildReadyForQuery('T'))

        let serverFut = serverHandler()
        let (tracer, skipped) = cleanupSkippedTracer()
        let conn = await connect(mockConfig(ms.port, tracer))

        var caught: ref ValueError = nil
        try:
          conn.withTransaction:
            conn.writer = defectWriter()
            raise newException(ValueError, "body boom")
        except ValueError as e:
          caught = e
        await serverFut

        doAssert caught != nil, "the body error must propagate"
        doAssert caught.msg == "body boom"
        doAssert skipped[].len == 1,
          "the cleanup failure must be reported exactly once (was " & $skipped[].len &
            ")"
        doAssert skipped[][0].kind == ckTxRollback
        doAssert skipped[][0].reason == csrCleanupFailed
        doAssert skipped[][0].parentMsg == "boom",
          "the cleanup Defect must be preserved as parent of the reported error"

        await closeClient(serverClient)
        try:
          await conn.close()
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor t()

    test "withSavepoint: ROLLBACK TO SAVEPOINT Defect is reported and never masks the body error":
      proc t() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # Read SAVEPOINT (delivered by the real writer); reply
          # in-transaction so the guard admits the ROLLBACK TO SAVEPOINT.
          discard await readQueryMessage(serverClient)
          await serverClient.sendBytes(buildReadyForQuery('T'))

        let serverFut = serverHandler()
        let (tracer, skipped) = cleanupSkippedTracer()
        let conn = await connect(mockConfig(ms.port, tracer))

        var caught: ref ValueError = nil
        try:
          conn.withSavepoint("sp"):
            conn.writer = defectWriter()
            raise newException(ValueError, "sp body boom")
        except ValueError as e:
          caught = e
        await serverFut

        doAssert caught != nil, "the body error must propagate"
        doAssert caught.msg == "sp body boom"
        doAssert skipped[].len == 1,
          "the cleanup failure must be reported exactly once (was " & $skipped[].len &
            ")"
        doAssert skipped[][0].kind == ckSavepointRollback
        doAssert skipped[][0].reason == csrCleanupFailed
        doAssert skipped[][0].parentMsg == "boom",
          "the cleanup Defect must be preserved as parent of the reported error"

        await closeClient(serverClient)
        try:
          await conn.close()
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor t()

    test "pool.withTransactionDeadline: release-path Defect wraps in PgPoolError with the Defect as parent":
      proc t() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # Read BEGIN (delivered by the real writer). COMMIT is swallowed by
          # the counting mock writer, so pre-send its idle reply too.
          discard await readQueryMessage(serverClient)
          await serverClient.sendBytes(buildReadyForQuery('T'))
          await serverClient.sendBytes(buildReadyForQuery('I'))

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port, nil))
        let origWriter = conn.writer

        let pool = makePool()
        conn.ownerPool = pool
        pool.idle.addLast(conn.toPooled())

        var caught: ref PgPoolError = nil
        try:
          pool.withTransactionDeadline(c, seconds(5)):
            # COMMIT is the first write after the swap; the release-path
            # unlock_all (session reset) is the second — it raises.
            c.sessionLockDirty = true
            c.writer = countingWriter(2)
        except PgPoolError as e:
          caught = e
        await serverFut

        doAssert caught != nil, "the release-path Defect must surface as PgPoolError"
        doAssert caught.parent of AssertionDefect,
          "the Defect must be preserved as parent"
        doAssert caught.parent.msg == "boom"
        # The conn was discarded (session reset failed while dirty), not parked.
        doAssert pool.idle.len == 0
        doAssert pool.active == 0

        conn.writer = origWriter
        await pool.close()
        await closeClient(serverClient)
        await closeServer(ms)

      waitFor t()

    test "pool.withTransactionRetryDeadline: release-path Defect wraps in PgPoolError and is never retried":
      proc t() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        var beginCount = 0
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # Read BEGIN (delivered by the real writer). COMMIT is swallowed by
          # the counting mock writer, so pre-send its idle reply too.
          discard await readQueryMessage(serverClient)
          inc beginCount
          await serverClient.sendBytes(buildReadyForQuery('T'))
          await serverClient.sendBytes(buildReadyForQuery('I'))

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port, nil))
        let origWriter = conn.writer

        let pool = makePool()
        conn.ownerPool = pool
        pool.idle.addLast(conn.toPooled())

        var caught: ref PgPoolError = nil
        try:
          pool.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), c, seconds(5)):
            # COMMIT is the first write after the swap; the release-path
            # unlock_all (session reset) is the second — it raises.
            c.sessionLockDirty = true
            c.writer = countingWriter(2)
        except PgPoolError as e:
          caught = e
        await serverFut

        doAssert caught != nil, "the release-path Defect must surface as PgPoolError"
        doAssert caught.parent of AssertionDefect,
          "the Defect must be preserved as parent"
        doAssert caught.parent.msg == "boom"
        doAssert beginCount == 1,
          "a Defect must never be retried (BEGIN ran " & $beginCount & " times)"
        # The conn was discarded (session reset failed while dirty), not parked.
        doAssert pool.idle.len == 0
        doAssert pool.active == 0

        conn.writer = origWriter
        await pool.close()
        await closeClient(serverClient)
        await closeServer(ms)

      waitFor t()

  suite "Pipelined dispatch release-path Defect":
    test "single-op release-path Defect is swallowed (dispatchHomogeneous)":
      # Regression: the pipelined single-op path must swallow a release-path
      # Defect exactly like executeBatch — the op's response was fully
      # consumed before the reset ran, so the op's future must succeed (not
      # fail with a PgPoolError or hang) and the conn is discarded.
      proc t() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # The counting mock writer swallows the op's Parse/Bind before it
          # reaches the wire, so pre-send the replies for the client's pump.
          await serverClient.sendBytes(buildExecReplies(1))

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port, nil))
        let origWriter = conn.writer
        # The op send is write #1 (succeeds); the release-path resetQuery is
        # write #2 (raises a Defect).
        conn.writer = countingWriter(2)

        let pool = makePool(resetQuery = "SELECT 1", pipelined = true)
        conn.ownerPool = pool
        pool.idle.addLast(conn.toPooled())

        var raised = false
        try:
          discard await pool.exec("SELECT 1")
        except PgPoolError:
          raised = true
        await serverFut

        doAssert not raised,
          "a release-path Defect must not fail a successful pipelined op (raised=" &
            $raised & ")"
        # The conn was discarded (reset send left csBusy), not parked. The
        # release runs in the spawned dispatch task, so poll for it.
        var released = false
        for _ in 0 ..< 200:
          if pool.active == 0:
            released = true
            break
          await sleepAsync(milliseconds(10))
        doAssert released, "connection must be released after the Defect"
        doAssert pool.idle.len == 0

        conn.writer = origWriter
        await pool.close()
        await closeClient(serverClient)
        await closeServer(ms)

      waitFor t()

    test "batch release-path Defect is swallowed (executeBatch)":
      # Regression: a release-path Defect must not fail a batch whose ops
      # already completed — both futures succeed and the conn is discarded.
      proc t() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # Two ops, one pipeline; the counting mock writer swallows the batch
          # send, so pre-send the replies for both ops.
          await serverClient.sendBytes(buildExecReplies(2))

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port, nil))
        let origWriter = conn.writer
        # The batch send is write #1 (succeeds); the release-path resetQuery
        # is write #2 (raises a Defect).
        conn.writer = countingWriter(2)

        # maxSize 2 → cap = max(1, 2 div 2) = 1 → both ops on one connection.
        let pool = makePool(maxSize = 2, resetQuery = "SELECT 1", pipelined = true)
        conn.ownerPool = pool
        pool.idle.addLast(conn.toPooled())

        # Enqueue both ops synchronously so one dispatch drains them as a
        # batch (dispatch runs on the next event loop tick).
        let f1 = pool.exec("SELECT 1")
        doAssert pool.pendingOps.len == 1
        let f2 = pool.exec("SELECT 1")
        doAssert pool.pendingOps.len == 2,
          "both ops must be queued before dispatch drains them as a batch"
        var raised = 0
        for f in [f1, f2]:
          try:
            discard await f
          except PgPoolError:
            inc raised
        await serverFut

        doAssert raised == 0,
          "a release-path Defect must not fail completed batch ops (raised=" & $raised &
            ")"
        var released = false
        for _ in 0 ..< 200:
          if pool.active == 0:
            released = true
            break
          await sleepAsync(milliseconds(10))
        doAssert released, "connection must be released after the Defect"
        doAssert pool.idle.len == 0

        conn.writer = origWriter
        await pool.close()
        await closeClient(serverClient)
        await closeServer(ms)

      waitFor t()

    test "runAndRelease swallows a release-path Defect (non-pipelined)":
      # Regression: a release-path reset Defect must not surface as PgPoolError
      # for a successful op — matching the pipelined dispatch paths' swallow.
      # The op's result is valid and the conn is discarded (the reset send
      # leaves csBusy), so nothing broken is reused.
      proc t() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # The counting mock writer swallows the op Query before it reaches
          # the wire, so pre-send the op's idle reply for the client's pump.
          await serverClient.sendBytes(buildReadyForQuery('I'))

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port, nil))
        let origWriter = conn.writer
        # The op Query is write #1 (succeeds); the release-path resetQuery is
        # write #2 (raises a Defect).
        conn.writer = countingWriter(2)

        let pool = makePool(resetQuery = "SELECT 1")
        conn.ownerPool = pool
        pool.idle.addLast(conn.toPooled())

        var raised = false
        try:
          discard await pool.simpleExec("SELECT 1")
        except PgPoolError:
          raised = true
        await serverFut

        doAssert not raised,
          "a release-path Defect must not fail a successful op (raised=" & $raised & ")"
        # The conn was discarded (reset send left csBusy), not parked.
        doAssert pool.active == 0
        doAssert pool.idle.len == 0

        conn.writer = origWriter
        await pool.close()
        await closeClient(serverClient)
        await closeServer(ms)

      waitFor t()
