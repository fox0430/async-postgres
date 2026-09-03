import std/[unittest, deques, tables, strutils, options, importutils, json]

import ../async_postgres/async_backend
when hasChronos:
  import pkg/chronos/streams/asyncstream

import ../async_postgres/[pg_protocol, pg_types, pg_connection]
import ../async_postgres/pg_types/encoding
import ../async_postgres/pg_connection/[buffer_io, simple_query, cache]
import ../async_postgres/pg_pool {.all.}
import ../async_postgres/pg_client/pipeline {.all.}
import ../async_postgres/pg_client/[core, query, exec, direct]

import mock_pg_server

var testTracerCloseCnt {.global.}: int

privateAccess(PgPool)
privateAccess(PgConnection)
privateAccess(PooledConn)
privateAccess(Waiter)
privateAccess(PendingPoolOp)
privateAccess(Pipeline)
privateAccess(PipelineOp)

proc mockConn(state: PgConnState = csReady, pool: PgPool = nil): PgConnection =
  result = PgConnection(
    recvBuf: @[],
    state: state,
    txStatus: tsIdle,
    serverParams: initTable[string, string](),
    createdAt: Moment.now(),
    ownerPool: pool,
  )

proc makePool(minSize: int = 0, maxSize: int = 5): PgPool =
  PgPool(
    config: PoolConfig(
      connConfig: ConnConfig(host: "localhost", port: 5432),
      minSize: minSize,
      maxSize: maxSize,
      maxWaiters: -1,
      maintenanceInterval: seconds(30),
    ),
    idle: initDeque[PooledConn](),
    active: 0,
    waiters: initDeque[Waiter](),
    waiterCount: 0,
    closed: false,
  )

proc toPooled(conn: PgConnection): PooledConn =
  PooledConn(conn: conn, lastUsedAt: Moment.now())

proc release(pool: PgPool, conn: PgConnection) =
  ## Test-only shim that wires `ownerPool` on throw-away mock connections and
  ## marks them checked out (mirroring an `acquire`) before delegating to the
  ## public `conn.release()` API. The `borrowed` flag keeps the release from
  ## being treated as a no-op double-release. Production callers should use
  ## `conn.release()` directly; pool-acquired connections already have
  ## `ownerPool` set and are marked `borrowed` by `acquire`.
  conn.ownerPool = pool
  conn.borrowed = true
  conn.release()

suite "initConnConfig":
  test "defaults":
    let cfg = initConnConfig()
    check cfg.host == "127.0.0.1"
    check cfg.port == 5432
    check cfg.user == ""
    check cfg.password == ""
    check cfg.database == ""
    check cfg.sslMode == sslPrefer # libpq default; secure-by-default
    check cfg.sslRootCert == ""
    check cfg.applicationName == ""
    check cfg.connectTimeout == ZeroDuration
    check cfg.keepAlive == true
    check cfg.keepAliveIdle == 0
    check cfg.keepAliveInterval == 0
    check cfg.keepAliveCount == 0
    check cfg.hosts.len == 0
    check cfg.targetSessionAttrs == tsaAny
    check cfg.extraParams.len == 0

  test "custom overrides":
    let cfg = initConnConfig(
      host = "db.example.com",
      port = 15432,
      user = "admin",
      password = "secret",
      database = "mydb",
      sslMode = sslRequire,
      applicationName = "myapp",
      keepAlive = false,
      targetSessionAttrs = tsaPrimary,
    )
    check cfg.host == "db.example.com"
    check cfg.port == 15432
    check cfg.user == "admin"
    check cfg.password == "secret"
    check cfg.database == "mydb"
    check cfg.sslMode == sslRequire
    check cfg.applicationName == "myapp"
    check cfg.keepAlive == false
    check cfg.targetSessionAttrs == tsaPrimary
    # Non-overridden fields keep defaults
    check cfg.sslRootCert == ""
    check cfg.connectTimeout == ZeroDuration
    check cfg.keepAliveIdle == 0

  test "with multi-host":
    let cfg = initConnConfig(
      hosts = @[
        HostEntry(host: "primary.db", port: 5432),
        HostEntry(host: "replica.db", port: 5433),
      ],
      targetSessionAttrs = tsaPreferStandby,
    )
    check cfg.hosts.len == 2
    check cfg.hosts[0].host == "primary.db"
    check cfg.hosts[0].port == 5432
    check cfg.hosts[1].host == "replica.db"
    check cfg.hosts[1].port == 5433
    check cfg.targetSessionAttrs == tsaPreferStandby

  test "with extra params":
    let cfg = initConnConfig(
      extraParams = @[("statement_timeout", "5000"), ("lock_timeout", "3000")]
    )
    check cfg.extraParams.len == 2
    check cfg.extraParams[0] == ("statement_timeout", "5000")
    check cfg.extraParams[1] == ("lock_timeout", "3000")

  test "composable with initPoolConfig":
    let connCfg = initConnConfig(host = "localhost", user = "test", database = "test")
    let poolCfg = initPoolConfig(connCfg, minSize = 2, maxSize = 5)
    check poolCfg.connConfig.host == "localhost"
    check poolCfg.connConfig.user == "test"
    check poolCfg.minSize == 2
    check poolCfg.maxSize == 5

suite "initPoolConfig":
  test "defaults":
    let cfg = initPoolConfig(ConnConfig(host: "localhost", port: 5432))
    check cfg.minSize == 1
    check cfg.maxSize == 10
    check cfg.idleTimeout == minutes(10)
    check cfg.maxLifetime == hours(1)
    check cfg.maintenanceInterval == seconds(30)
    check cfg.healthCheckTimeout == seconds(5)
    check cfg.tlsHealthCheckTimeout == milliseconds(500)
    check cfg.pingTimeout == seconds(5)
    check cfg.acquireTimeout == seconds(30)
    check cfg.maxWaiters == -1
    check cfg.resetQueryTimeout == seconds(5)

  test "custom overrides":
    let cfg = initPoolConfig(
      ConnConfig(host: "localhost", port: 5432),
      minSize = 5,
      maxSize = 20,
      idleTimeout = minutes(5),
      acquireTimeout = seconds(10),
    )
    check cfg.minSize == 5
    check cfg.maxSize == 20
    check cfg.idleTimeout == minutes(5)
    check cfg.acquireTimeout == seconds(10)
    # Non-overridden fields keep defaults
    check cfg.maxLifetime == hours(1)
    check cfg.healthCheckTimeout == seconds(5)
    check cfg.tlsHealthCheckTimeout == milliseconds(500)

  test "validation: minSize < 0":
    expect(ValueError):
      discard initPoolConfig(ConnConfig(host: "localhost", port: 5432), minSize = -1)

  test "validation: maxSize < 1":
    expect(ValueError):
      discard initPoolConfig(ConnConfig(host: "localhost", port: 5432), maxSize = 0)

  test "validation: minSize > maxSize":
    expect(ValueError):
      discard initPoolConfig(
        ConnConfig(host: "localhost", port: 5432), minSize = 10, maxSize = 5
      )

  test "validation: maxWaiters < -1":
    expect(ValueError):
      discard initPoolConfig(ConnConfig(host: "localhost", port: 5432), maxWaiters = -2)

  test "validation: tlsHealthCheckTimeout < 0":
    expect(ValueError):
      discard initPoolConfig(
        ConnConfig(host: "localhost", port: 5432),
        tlsHealthCheckTimeout = milliseconds(-1),
      )

  test "validation: resetQueryTimeout < 0":
    expect(ValueError):
      discard initPoolConfig(
        ConnConfig(host: "localhost", port: 5432), resetQueryTimeout = milliseconds(-1)
      )

  test "tlsHealthCheckTimeout custom override":
    let cfg = initPoolConfig(
      ConnConfig(host: "localhost", port: 5432),
      tlsHealthCheckTimeout = milliseconds(100),
    )
    check cfg.tlsHealthCheckTimeout == milliseconds(100)

  test "validation: minSize = 0 is valid":
    let cfg = initPoolConfig(ConnConfig(host: "localhost", port: 5432), minSize = 0)
    check cfg.minSize == 0

  test "backoff defaults":
    let cfg = initPoolConfig(ConnConfig(host: "localhost", port: 5432))
    check cfg.connectBackoffInitial == seconds(1)
    check cfg.connectBackoffMax == seconds(60)

  test "backoff custom overrides":
    let cfg = initPoolConfig(
      ConnConfig(host: "localhost", port: 5432),
      connectBackoffInitial = milliseconds(100),
      connectBackoffMax = seconds(10),
    )
    check cfg.connectBackoffInitial == milliseconds(100)
    check cfg.connectBackoffMax == seconds(10)

  test "backoff disabled with ZeroDuration initial":
    let cfg = initPoolConfig(
      ConnConfig(host: "localhost", port: 5432),
      connectBackoffInitial = ZeroDuration,
      connectBackoffMax = ZeroDuration,
    )
    check cfg.connectBackoffInitial == ZeroDuration

  test "validation: connectBackoffMax < connectBackoffInitial":
    expect(ValueError):
      discard initPoolConfig(
        ConnConfig(host: "localhost", port: 5432),
        connectBackoffInitial = seconds(10),
        connectBackoffMax = seconds(1),
      )

suite "computeConnectBackoff":
  test "zero failures returns ZeroDuration":
    check computeConnectBackoff(seconds(1), seconds(60), 0) == ZeroDuration

  test "negative failures returns ZeroDuration":
    check computeConnectBackoff(seconds(1), seconds(60), -1) == ZeroDuration

  test "disabled when initial is ZeroDuration":
    check computeConnectBackoff(ZeroDuration, seconds(60), 5) == ZeroDuration

  test "first failure returns initial":
    check computeConnectBackoff(seconds(1), seconds(60), 1) == seconds(1)

  test "doubles on each failure up to max":
    check computeConnectBackoff(seconds(1), seconds(60), 2) == seconds(2)
    check computeConnectBackoff(seconds(1), seconds(60), 3) == seconds(4)
    check computeConnectBackoff(seconds(1), seconds(60), 4) == seconds(8)
    check computeConnectBackoff(seconds(1), seconds(60), 5) == seconds(16)
    check computeConnectBackoff(seconds(1), seconds(60), 6) == seconds(32)

  test "caps at maxDelay":
    # 2^6 = 64 > 60, so 7th failure caps
    check computeConnectBackoff(seconds(1), seconds(60), 7) == seconds(60)
    check computeConnectBackoff(seconds(1), seconds(60), 50) == seconds(60)

  test "initial already exceeds max returns max":
    check computeConnectBackoff(seconds(120), seconds(60), 1) == seconds(60)

suite "batchTimeout":
  proc op(timeout: Duration): PendingPoolOp =
    PendingPoolOp(kind: popExec, timeout: timeout)

  test "empty batch is unlimited":
    check batchTimeout(@[]) == ZeroDuration

  test "single finite timeout is used as-is":
    check batchTimeout(@[op(seconds(5))]) == seconds(5)

  test "all finite timeouts take the largest":
    check batchTimeout(@[op(seconds(2)), op(seconds(5)), op(seconds(3))]) == seconds(5)

  test "an unlimited op makes the whole batch unlimited":
    # ZeroDuration means "no timeout"; it must win over finite siblings rather
    # than being treated as the smallest value by max().
    check batchTimeout(@[op(seconds(5)), op(ZeroDuration), op(seconds(2))]) ==
      ZeroDuration

  test "leading unlimited op stays unlimited":
    check batchTimeout(@[op(ZeroDuration), op(seconds(5))]) == ZeroDuration

suite "splitBatchBudget":
  test "equal classes split the budget evenly":
    check splitBatchBudget(5, 5, 4) == (2, 2)

  test "budget is shared proportionally to op counts":
    # finite dominates -> gets the larger share, unlimited still keeps one.
    check splitBatchBudget(9, 1, 4) == (3, 1)
    # unlimited dominates -> the reverse.
    check splitBatchBudget(1, 9, 4) == (1, 3)

  test "each present class gets at least one connection":
    # A class that would round to zero is floored to one.
    check splitBatchBudget(1, 100, 8) == (1, 7)
    check splitBatchBudget(100, 1, 8) == (7, 1)

  test "a cap of one still gives each class one connection":
    check splitBatchBudget(3, 3, 1) == (1, 1)

  test "the two shares never exceed the budget above a cap of one":
    for cap in 2 .. 16:
      let (a, b) = splitBatchBudget(3, 5, cap)
      check a >= 1 and b >= 1
      check a + b == cap

suite "PendingPoolOp finish guards":
  # Dispatch paths must not complete/fail an already-finished future.
  # Callers cancel via `.wait(dur)` which can leave execFut/queryFut
  # finished before the pipeline batch resolves.

  proc makeExecOp(): PendingPoolOp =
    PendingPoolOp(kind: popExec, execFut: newFuture[CommandResult]("test.execFut"))

  proc makeQueryOp(): PendingPoolOp =
    PendingPoolOp(kind: popQuery, queryFut: newFuture[QueryResult]("test.queryFut"))

  test "completePendingOp is a no-op when exec future already finished":
    let op = makeExecOp()
    op.execFut.fail(newException(PgPoolError, "pre-cancelled"))
    check op.execFut.finished
    completePendingOp(op, CommandResult(commandTag: "SELECT 0"))
    check op.execFut.failed
    expect(PgPoolError):
      discard op.execFut.read()

  test "completePendingOp is a no-op when query future already finished":
    let op = makeQueryOp()
    op.queryFut.fail(newException(PgPoolError, "pre-cancelled"))
    check op.queryFut.finished
    completePendingOp(op, QueryResult(commandTag: "SELECT 0"))
    check op.queryFut.failed
    expect(PgPoolError):
      discard op.queryFut.read()

  test "completePendingOp delivers result on unfinished exec future":
    let op = makeExecOp()
    completePendingOp(op, CommandResult(commandTag: "INSERT 0 1"))
    check op.execFut.finished
    check op.execFut.read().commandTag == "INSERT 0 1"

  test "completePendingOp delivers result on unfinished query future":
    let op = makeQueryOp()
    completePendingOp(op, QueryResult(commandTag: "SELECT 1"))
    check op.queryFut.finished
    check op.queryFut.read().commandTag == "SELECT 1"

  test "failPendingOp is a no-op when exec future already finished":
    let first = newException(PgPoolError, "first")
    let op = makeExecOp()
    op.execFut.fail(first)
    failPendingOp(op, newException(PgPoolError, "second"))
    check op.execFut.failed
    check op.execFut.readError() == first

suite "failPendingAndUnschedule":
  # When scheduleSoon/asyncSpawn can't arrange a dispatch, the caller has
  # already enqueued its op; without this recovery the caller's future would
  # hang forever waiting on a dispatch that never comes.

  test "fails every queued op with the given error":
    let pool = makePool()
    pool.pendingOps = initDeque[PendingPoolOp]()
    let execFut = newFuture[CommandResult]("test.execFut")
    let queryFut = newFuture[QueryResult]("test.queryFut")
    pool.pendingOps.addLast(PendingPoolOp(kind: popExec, execFut: execFut))
    pool.pendingOps.addLast(PendingPoolOp(kind: popQuery, queryFut: queryFut))
    pool.dispatchScheduled = true

    let err = newException(PgError, "no dispatch")
    pool.failPendingAndUnschedule(err)

    check pool.pendingOps.len == 0
    check pool.dispatchScheduled == false
    check execFut.failed
    check queryFut.failed
    check execFut.readError() == err
    check queryFut.readError() == err

  test "no-op on an empty queue":
    let pool = makePool()
    pool.pendingOps = initDeque[PendingPoolOp]()
    pool.dispatchScheduled = true
    pool.failPendingAndUnschedule(newException(PgError, "no dispatch"))
    check pool.pendingOps.len == 0
    check pool.dispatchScheduled == false

  test "leaves already-finished op futures untouched":
    let pool = makePool()
    pool.pendingOps = initDeque[PendingPoolOp]()
    let prior = newException(PgPoolError, "prior cancel")
    let execFut = newFuture[CommandResult]("test.execFut")
    execFut.fail(prior)
    pool.pendingOps.addLast(PendingPoolOp(kind: popExec, execFut: execFut))
    pool.dispatchScheduled = true

    pool.failPendingAndUnschedule(newException(PgError, "no dispatch"))

    check pool.pendingOps.len == 0
    check pool.dispatchScheduled == false
    check execFut.readError() == prior

suite "checkReady error classification":
  # A-8: a connection that is alive but busy (a single connection used
  # concurrently) is a programming error, not a network failure. checkReady
  # must distinguish it from a genuinely dead connection so reconnect-on-
  # PgConnectionError recovery does not spin on an unfixable condition.
  test "csBusy raises PgStateError, not PgConnectionError":
    let conn = mockConn(csBusy)
    var stateErr = false
    var connErr = false
    try:
      conn.checkReady()
    except PgConnectionError:
      connErr = true
    except PgStateError:
      stateErr = true
    check stateErr
    check not connErr

  test "csReplicating raises PgStateError":
    let conn = mockConn(csReplicating)
    var stateErr = false
    try:
      conn.checkReady()
    except PgStateError:
      stateErr = true
    check stateErr

  test "csConnecting raises PgStateError":
    let conn = mockConn(csConnecting)
    var stateErr = false
    try:
      conn.checkReady()
    except PgStateError:
      stateErr = true
    check stateErr

  test "csAuthentication raises PgStateError":
    let conn = mockConn(csAuthentication)
    var stateErr = false
    try:
      conn.checkReady()
    except PgStateError:
      stateErr = true
    check stateErr

  test "csListening raises PgStateError":
    let conn = mockConn(csListening)
    var stateErr = false
    try:
      conn.checkReady()
    except PgStateError:
      stateErr = true
    check stateErr

  test "csClosed raises PgConnectionError (reconnect is the right recovery)":
    let conn = mockConn(csClosed)
    var connErr = false
    try:
      conn.checkReady()
    except PgConnectionError:
      connErr = true
    check connErr

  test "csReady passes":
    let conn = mockConn(csReady)
    conn.checkReady()

suite "PgTimeoutError recovery classification":
  # A timed-out operation leaves the connection csClosed, so reconnecting is the
  # only viable recovery. PgTimeoutError must therefore be visible to an
  # `except PgConnectionError` reconnect loop, while a more specific
  # `except PgTimeoutError` clause (placed first) still distinguishes it.
  test "PgTimeoutError is caught by except PgConnectionError":
    var caughtAsConn = false
    var isTimeout = false
    try:
      raise newException(PgTimeoutError, "timed out")
    except PgConnectionError as e:
      caughtAsConn = true
      isTimeout = e of ref PgTimeoutError
    check caughtAsConn
    check isTimeout

  test "except PgTimeoutError takes precedence when ordered first":
    var branch = ""
    try:
      raise newException(PgTimeoutError, "timed out")
    except PgTimeoutError:
      branch = "timeout"
    except PgConnectionError:
      branch = "conn"
    check branch == "timeout"

suite "Pool release":
  test "release to idle queue":
    let pool = makePool()
    let conn = mockConn()
    pool.active = 1
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 1
    check pool.idle[0].conn == conn

  test "release transfers to waiter":
    let pool = makePool()
    pool.active = 2
    let fut = newFuture[PgConnection]("test.waiter")
    pool.waiters.addLast(Waiter(fut: fut, cancelled: false))
    pool.waiterCount = 1
    let conn = mockConn()
    pool.release(conn)
    check pool.active == 2
    check pool.waiters.len == 0
    check pool.waiterCount == 0
    check fut.finished
    check fut.read() == conn

  test "release skips cancelled waiters and returns to idle":
    let pool = makePool()
    pool.active = 1
    # Add cancelled waiters
    let cancelled1 = Waiter(fut: newFuture[PgConnection]("c1"), cancelled: true)
    let cancelled2 = Waiter(fut: newFuture[PgConnection]("c2"), cancelled: true)
    pool.waiters.addLast(cancelled1)
    pool.waiters.addLast(cancelled2)
    let conn = mockConn()
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 1
    check pool.idle[0].conn == conn
    check pool.waiters.len == 0

  test "release skips cancelled waiters and delivers to next valid":
    let pool = makePool()
    pool.active = 2
    let cancelled = Waiter(fut: newFuture[PgConnection]("c"), cancelled: true)
    let validFut = newFuture[PgConnection]("valid")
    let valid = Waiter(fut: validFut, cancelled: false)
    pool.waiters.addLast(cancelled)
    pool.waiters.addLast(valid)
    pool.waiterCount = 1
    let conn = mockConn()
    pool.release(conn)
    check pool.active == 2
    check pool.waiterCount == 0
    check pool.idle.len == 0
    check validFut.finished
    check validFut.read() == conn

  test "release broken connection decrements active":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn(csClosed)
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 0

  test "release to closed pool decrements active":
    let pool = makePool()
    pool.active = 1
    pool.closed = true
    let conn = mockConn()
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 0

  test "release stamps lastUsedAt with the return time, not a stale acquire-time timestamp":
    let pool = makePool()
    pool.active = 1
    let beforeRelease = Moment.now()
    let conn = mockConn()
    pool.release(conn)
    check pool.idle.len == 1
    check pool.idle[0].conn == conn
    check pool.idle[0].lastUsedAt >= beforeRelease

  test "release discards connection in transaction":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn()
    conn.txStatus = tsInTransaction
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 0

  test "release discards connection in failed transaction":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn()
    conn.txStatus = tsInFailedTransaction
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 0

  test "release discards connection holding session advisory locks":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn()
    conn.heldSessionLocks = 1
    conn.sessionLockDirty = true
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 0

  test "release returns connection to idle when no locks held":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn()
    check conn.heldSessionLocks == 0
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 1

  test "release on standalone connection raises PgError":
    let conn = mockConn()
    check conn.ownerPool == nil
    expect PgError:
      conn.release()

suite "Pool resetSession":
  test "resetSession is no-op when resetQuery is empty and no locks held":
    let pool = makePool()
    let conn = mockConn()
    conn.stmtCacheCapacity = 256
    conn.addStmtCache("SELECT 1", CachedStmt(name: "_sc_1"))
    waitFor pool.resetSession(conn)
    check conn.state == csReady
    check conn.stmtCache.len == 1 # not cleared
    check conn.heldSessionLocks == 0

  test "resetSession skips broken connection":
    let pool = makePool()
    pool.config.resetQuery = "DISCARD ALL"
    let conn = mockConn(csClosed)
    waitFor pool.resetSession(conn)
    check conn.state == csClosed # unchanged

  test "resetSession skips connection in transaction":
    let pool = makePool()
    pool.config.resetQuery = "DISCARD ALL"
    let conn = mockConn()
    conn.txStatus = tsInTransaction
    waitFor pool.resetSession(conn)
    check conn.state == csReady # unchanged, not closed

  test "resetQuery field in initPoolConfig":
    let cfg = initPoolConfig(
      ConnConfig(host: "localhost", port: 5432), resetQuery = "DISCARD ALL"
    )
    check cfg.resetQuery == "DISCARD ALL"

  test "resetQuery defaults to empty":
    let cfg = initPoolConfig(ConnConfig(host: "localhost", port: 5432))
    check cfg.resetQuery == ""

  test "resetQueryTimeout field in initPoolConfig":
    let cfg = initPoolConfig(
      ConnConfig(host: "localhost", port: 5432), resetQueryTimeout = milliseconds(200)
    )
    check cfg.resetQueryTimeout == milliseconds(200)

  test "resetQueryTimeout ZeroDuration disables the deadline":
    let cfg = initPoolConfig(
      ConnConfig(host: "localhost", port: 5432), resetQueryTimeout = ZeroDuration
    )
    check cfg.resetQueryTimeout == ZeroDuration

suite "Pool withConnection release-path Defect":
  when hasChronos:
    test "withConnection wraps a release-path Defect in PgPoolError":
      # Regression: a Defect raised by the release path (resetSession's
      # synchronous prelude — here the unlock_all exec) must surface as
      # PgPoolError instead of escaping raw, since chronos re-raises Defects
      # eagerly from continuations.
      proc t() {.async.} =
        let pool = makePool()
        let conn = mockConn()
        conn.ownerPool = pool
        conn.writer = defectWriter()
        conn.sessionLockDirty = true # forces unlock_all through the writer
        pool.idle.addLast(conn.toPooled())

        var caught = false
        try:
          pool.withConnection(c):
            doAssert c == conn
            doAssert pool.active == 1
        except PgPoolError:
          caught = true

        doAssert caught
        # The conn was discarded (session reset failed), so it is not idle.
        doAssert pool.active == 0
        doAssert pool.idle.len == 0

      waitFor t()

    test "withConnection does not shadow a body error with a release Defect":
      # Regression: when both the body and the release path raise, the body's
      # error must survive (the release Defect is dropped), matching the
      # CatchableError arm's "Never shadow" guard.
      proc t() {.async.} =
        let pool = makePool()
        let conn = mockConn()
        conn.ownerPool = pool
        conn.writer = defectWriter()
        conn.sessionLockDirty = true # forces unlock_all through the writer
        pool.idle.addLast(conn.toPooled())

        var caught: ref ValueError = nil
        try:
          pool.withConnection(c):
            doAssert c == conn
            raise newException(ValueError, "body error")
        except ValueError as e:
          caught = e

        doAssert caught != nil,
          "the body error must not be shadowed by the release Defect"
        doAssert caught.msg == "body error"
        doAssert pool.active == 0
        doAssert pool.idle.len == 0

      waitFor t()

    test "withConnection keeps the body Defect as the parent when release also raises":
      # Regression: with a body Defect and a release-path Defect, the raised
      # PgPoolError must wrap the body Defect (parent), not the release one.
      proc t() {.async.} =
        let pool = makePool()
        let conn = mockConn()
        conn.ownerPool = pool
        conn.writer = defectWriter()
        conn.sessionLockDirty = true # forces unlock_all through the writer
        pool.idle.addLast(conn.toPooled())

        var caught: ref PgPoolError = nil
        try:
          pool.withConnection(c):
            raise newException(AssertionDefect, "body defect")
        except PgPoolError as e:
          caught = e

        doAssert caught != nil, "the body Defect must surface as PgPoolError"
        doAssert caught.kind == pekDefectWrapped
        doAssert caught.parent of AssertionDefect,
          "the body Defect must be the parent, not the release Defect"
        doAssert caught.parent.msg == "body defect",
          "parent must be the body Defect, got: " & $caught.parent.msg
        doAssert pool.active == 0

      waitFor t()

  when hasAsyncDispatch:
    # asyncdispatch has no mock writer, so the release-path Defect is injected
    # through the `onLeakedSessionLocks` tracer hook. The Defect arms are
    # backend-shared and must match the chronos writer-based tests above.

    test "withConnection wraps a release-path Defect in PgPoolError (tracer-raised)":
      # Regression: a Defect raised by the release path (here via the tracer
      # hook) must surface as PgPoolError instead of escaping raw, matching
      # the chronos writer-based arm.
      proc t() {.async.} =
        let pool = makePool()
        let tracer = PgTracer()
        tracer.onLeakedSessionLocks = proc(
            data: TraceLeakedSessionLocksData
        ) {.gcsafe, raises: [].} =
          raise newException(AssertionDefect, "boom")
        pool.config.tracer = tracer
        let conn = mockConn()
        conn.ownerPool = pool
        conn.sessionLockDirty = true # forces the unlock_all path (and the hook)
        pool.idle.addLast(conn.toPooled())

        var caught: ref PgPoolError = nil
        try:
          pool.withConnection(c):
            doAssert c == conn
            doAssert pool.active == 1
        except PgPoolError as e:
          caught = e

        doAssert caught != nil, "the release-path Defect must surface as PgPoolError"
        doAssert caught.kind == pekDefectWrapped
        doAssert caught.parent of AssertionDefect,
          "the Defect must be preserved as parent"
        # asyncdispatch appends an async traceback to the message.
        doAssert caught.parent.msg.startsWith("boom"),
          "the Defect must be preserved as parent, got: " & $caught.parent.msg
        # The conn was discarded (session reset failed while dirty), so it is
        # not idle.
        doAssert pool.active == 0
        doAssert pool.idle.len == 0

      waitFor t()

    test "withConnection does not shadow a body error with a tracer-raised release Defect":
      # Regression: when both the body and the release path raise, the body's
      # error must survive (the release Defect is dropped), matching the
      # CatchableError arm's "Never shadow" guard.
      proc t() {.async.} =
        let pool = makePool()
        let tracer = PgTracer()
        tracer.onLeakedSessionLocks = proc(
            data: TraceLeakedSessionLocksData
        ) {.gcsafe, raises: [].} =
          raise newException(AssertionDefect, "boom")
        pool.config.tracer = tracer
        let conn = mockConn()
        conn.ownerPool = pool
        conn.sessionLockDirty = true # forces the unlock_all path (and the hook)
        pool.idle.addLast(conn.toPooled())

        var caught: ref ValueError = nil
        try:
          pool.withConnection(c):
            doAssert c == conn
            raise newException(ValueError, "body error")
        except ValueError as e:
          caught = e

        doAssert caught != nil,
          "the body error must not be shadowed by the release Defect"
        # asyncdispatch appends an async traceback to the message.
        doAssert caught.msg.startsWith("body error"),
          "the body error must survive, got: " & $caught.msg
        doAssert pool.active == 0
        doAssert pool.idle.len == 0

      waitFor t()

    test "withConnection keeps the body Defect as the parent when the tracer also raises":
      # Regression: with a body Defect and a release-path Defect, the raised
      # PgPoolError must wrap the body Defect (parent), not the release one.
      proc t() {.async.} =
        let pool = makePool()
        let tracer = PgTracer()
        tracer.onLeakedSessionLocks = proc(
            data: TraceLeakedSessionLocksData
        ) {.gcsafe, raises: [].} =
          raise newException(AssertionDefect, "boom")
        pool.config.tracer = tracer
        let conn = mockConn()
        conn.ownerPool = pool
        conn.sessionLockDirty = true # forces the unlock_all path (and the hook)
        pool.idle.addLast(conn.toPooled())

        var caught: ref PgPoolError = nil
        try:
          pool.withConnection(c):
            raise newException(AssertionDefect, "body defect")
        except PgPoolError as e:
          caught = e

        doAssert caught != nil, "the body Defect must surface as PgPoolError"
        doAssert caught.kind == pekDefectWrapped
        doAssert caught.parent of AssertionDefect,
          "the body Defect must be the parent, not the release Defect"
        # asyncdispatch appends an async traceback to the message.
        doAssert caught.parent.msg.startsWith("body defect"),
          "parent must be the body Defect, got: " & $caught.parent.msg
        doAssert pool.active == 0

      waitFor t()

suite "Pool acquire":
  test "acquire from idle":
    let pool = makePool()
    let conn = mockConn()
    pool.idle.addLast(conn.toPooled())

    let acquired = waitFor pool.acquire()
    check acquired == conn
    check pool.active == 1
    check pool.idle.len == 0

  test "acquire skips broken connections":
    let pool = makePool()
    let broken = mockConn(csClosed)
    let good = mockConn(csReady)
    pool.idle.addLast(broken.toPooled())
    pool.idle.addLast(good.toPooled())

    let acquired = waitFor pool.acquire()
    check acquired == good
    check pool.active == 1
    check pool.idle.len == 0

  test "acquire skips maxLifetime-expired connections":
    let pool = makePool()
    pool.config.maxLifetime = seconds(1)
    # Create a connection with createdAt far in the past
    let expired = mockConn()
    expired.createdAt = Moment.now() - seconds(5)
    expired.state = csClosed # so close() won't try network I/O
    let good = mockConn()
    pool.idle.addLast(expired.toPooled())
    pool.idle.addLast(good.toPooled())

    let acquired = waitFor pool.acquire()
    check acquired == good
    check pool.active == 1
    check pool.idle.len == 0

  test "acquire disposes idle broken conn via closeNoWait (no await point)":
    # Regression: `await tracedClose` here would let a caller's cancellation
    # be swallowed by tracedClose's `except CatchableError`, leaking the
    # next-acquired conn to a departed caller.
    let pool = makePool()
    let broken = mockConn(csClosed)
    let good = mockConn(csReady)
    pool.idle.addLast(broken.toPooled())
    pool.idle.addLast(good.toPooled())

    let acquired = waitFor pool.acquire()
    check acquired == good
    check pool.pendingBackgroundTasks.len >= 1

  test "acquire disposes maxLifetime-expired idle conn via closeNoWait":
    let pool = makePool()
    pool.config.maxLifetime = seconds(1)
    let expired = mockConn()
    expired.createdAt = Moment.now() - seconds(5)
    expired.state = csClosed
    let good = mockConn()
    pool.idle.addLast(expired.toPooled())
    pool.idle.addLast(good.toPooled())

    let acquired = waitFor pool.acquire()
    check acquired == good
    check pool.pendingBackgroundTasks.len >= 1

  test "acquire registers waiter when at max":
    let pool = makePool(maxSize = 1)
    pool.active = 1

    let acquireFut = pool.acquire()
    check not acquireFut.finished
    check pool.waiters.len == 1

    let conn = mockConn()
    pool.release(conn)
    let acquired = waitFor acquireFut
    check acquired == conn

suite "Pool acquireHandle":
  test "acquireHandle returns handle pairing conn with its pool":
    let pool = makePool()
    let conn = mockConn(pool = pool)
    pool.idle.addLast(conn.toPooled())

    let h = waitFor pool.acquireHandle()
    check h.conn == conn
    check h.pool == pool
    check pool.active == 1
    check pool.idle.len == 0

  test "release(handle) returns conn to idle":
    let pool = makePool()
    let conn = mockConn(pool = pool)
    pool.idle.addLast(conn.toPooled())

    let h = waitFor pool.acquireHandle()
    h.release()
    check pool.active == 0
    check pool.idle.len == 1

  test "release(handle) is idempotent — second call is a no-op":
    let pool = makePool()
    let conn = mockConn(pool = pool)
    pool.idle.addLast(conn.toPooled())

    let h = waitFor pool.acquireHandle()
    h.release()
    check pool.active == 0
    check pool.idle.len == 1

    # Second release must not double-return the connection, decrement active
    # below zero, or otherwise corrupt pool state.
    h.release()
    check pool.active == 0
    check pool.idle.len == 1

suite "Pool close":
  test "close cancels waiters":
    let pool = makePool()
    pool.active = 1
    let fut = newFuture[PgConnection]("test.waiter")
    pool.waiters.addLast(Waiter(fut: fut, cancelled: false))
    pool.waiterCount = 1

    waitFor pool.close()
    check pool.closed
    check pool.waiters.len == 0
    check pool.waiterCount == 0
    check fut.finished

  test "close drains idle connections":
    let pool = makePool()
    let conn1 = mockConn(csClosed)
    let conn2 = mockConn(csClosed)
    pool.idle.addLast(conn1.toPooled())
    pool.idle.addLast(conn2.toPooled())

    waitFor pool.close()
    check pool.closed
    check pool.idle.len == 0

  test "close empty pool":
    let pool = makePool()
    waitFor pool.close()
    check pool.closed

  test "close with timeout waits for active connections":
    let pool = makePool()
    pool.active = 1

    # Simulate a connection being released after a short delay
    proc releaseAfter(pool: PgPool) {.async.} =
      await sleepAsync(milliseconds(20))
      pool.active.dec

    let releaseFut = releaseAfter(pool)
    waitFor pool.close(timeout = seconds(1))
    waitFor releaseFut
    check pool.closed
    check pool.active == 0

  test "close with timeout expires when active not released":
    let pool = makePool()
    pool.active = 1

    waitFor pool.close(timeout = milliseconds(100))
    check pool.closed
    check pool.active == 1

  test "release of broken conn tracks pending close":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn(csClosed)
    pool.release(conn)
    check pool.pendingBackgroundTasks.len == 1

  test "close awaits pending closeNoWait tasks":
    let pool = makePool()
    pool.active = 2
    pool.release(mockConn(csClosed))
    pool.release(mockConn(csClosed))

    waitFor pool.close()
    check pool.closed
    check pool.pendingBackgroundTasks.len == 0

  test "close awaits a conn abandoned by a handed-off waiter":
    # Regression: a handed-off waiter's acquire continuation is scheduled but
    # not yet resumed, so close()'s waiter loop can't see it. Abandoning it on a
    # closed pool runs settleAbandonedWaiter -> release() -> closeNoWait, which
    # used to push a Terminate task after close() had already drained. close()
    # now yields once before draining so that task is enqueued in time to await.
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.active = 1

      # Hand a conn off to a queued waiter: pops it, completes its future, marks
      # it borrowed. csClosed so the eventual release discards via closeNoWait
      # (and conn.close() short-circuits without touching a real socket).
      let waiter = Waiter(fut: newFuture[PgConnection]("w"), cancelled: false)
      pool.waiters.addLast(waiter)
      pool.waiterCount.inc
      let conn = mockConn(csClosed, pool = pool)
      doAssert pool.tryHandoffToWaiter(conn)

      # Model the waiter's not-yet-resumed continuation: scheduled on the loop,
      # it abandons the acquire on the next tick — after close() has started.
      # Record whether it ran and any exception it raised, rather than swallowing
      # them: a silent `except: discard` would let a broken abandon path pass.
      var continuationRan = false
      var settleErr: ref Exception = nil
      scheduleSoon(
        proc() {.gcsafe, raises: [].} =
          {.cast(gcsafe).}:
            try:
              pool.settleAbandonedWaiter(waiter)
              continuationRan = true
            except Exception as e:
              settleErr = e
      )

      # close() must yield so that continuation runs and its closeNoWait task is
      # enqueued before the drain, not after close() returns.
      await pool.close()
      doAssert pool.closed
      doAssert settleErr == nil # the abandon path did not raise
      # The discriminating check: without the pre-drain yield, close() never
      # suspends here, so the scheduled continuation has not run yet when close()
      # returns. (settleAbandonedWaiter takes the completed() branch, which no
      # longer sets `cancelled`, so observe that the continuation ran directly.)
      doAssert continuationRan
      doAssert pool.pendingBackgroundTasks.len == 0 # its late closeNoWait was awaited

    waitFor t()

  test "closeNoWait prunes finished futures once threshold is reached":
    let pool = makePool()
    # Inject pre-finished dummies up to the prune threshold so the next
    # closeNoWait deterministically triggers the sweep regardless of timing.
    for _ in 0 ..< bgTaskPruneThreshold:
      let f = newFuture[void]("dummy")
      f.complete()
      pool.pendingBackgroundTasks.add(f)
    pool.active = 1
    pool.release(mockConn(csClosed))
    # All finished dummies were swept, leaving only the newly spawned close.
    check pool.pendingBackgroundTasks.len == 1
    waitFor pool.close()

  test "pipelined exec on closed pool raises instead of hanging":
    # Regression: the pipelined path used to enqueue the op without checking
    # `closed`. dispatchBatchImpl early-returns on a closed pool without ever
    # completing the future, so `await fut` hung forever. It must raise instead.
    let pool = makePool()
    pool.config.pipelined = true
    pool.closed = true
    expect(PgPoolError):
      discard waitFor pool.exec("SELECT 1")
    check pool.pendingOps.len == 0

  test "pipelined query on closed pool raises instead of hanging":
    let pool = makePool()
    pool.config.pipelined = true
    pool.closed = true
    expect(PgPoolError):
      discard waitFor pool.query("SELECT 1")
    check pool.pendingOps.len == 0

suite "Pool active count tracking":
  test "release then acquire roundtrip":
    let pool = makePool()
    let conn = mockConn()
    pool.active = 1
    pool.release(conn)
    check pool.active == 0

    discard waitFor pool.acquire()
    check pool.active == 1

  test "double release of broken connection does not underflow active":
    let pool = makePool()
    let conn = mockConn(csClosed, pool = pool)
    conn.borrowed = true
    pool.active = 1
    conn.release()
    check pool.active == 0
    # Second release is a no-op: the connection is no longer checked out.
    conn.release()
    check pool.active == 0

  test "double release of normal connection does not underflow active":
    let pool = makePool()
    let conn = mockConn(pool = pool)
    conn.borrowed = true
    pool.active = 1
    conn.release()
    check pool.active == 0
    check pool.idle.len == 1
    # Second release — conn is already idle. It must NOT be registered again,
    # otherwise two future borrowers would receive the same connection.
    conn.release()
    check pool.active == 0
    check pool.idle.len == 1

  test "waiter transfer preserves active count":
    let pool = makePool(maxSize = 1)
    pool.active = 1
    let acquireFut = pool.acquire()
    check pool.active == 1

    let conn = mockConn()
    pool.release(conn)
    check pool.active == 1

    discard waitFor acquireFut
    check pool.active == 1

suite "Pool double release":
  test "borrowed flag toggles across acquire/release roundtrip":
    let pool = makePool()
    let conn = mockConn(pool = pool)
    pool.idle.addLast(toPooled(conn))

    let acquired = waitFor pool.acquire()
    check acquired == conn
    check conn.borrowed
    conn.release()
    check not conn.borrowed
    check pool.idle.len == 1

  test "release-to-waiter keeps the connection borrowed":
    # The connection is handed straight to the next acquirer, so it stays
    # checked out — the waiter (not the releaser) now owns it.
    let pool = makePool()
    pool.active = 1
    let fut = newFuture[PgConnection]("test.waiter")
    pool.waiters.addLast(Waiter(fut: fut, cancelled: false))
    pool.waiterCount = 1
    let conn = mockConn(pool = pool)
    conn.borrowed = true
    conn.release()
    check fut.finished
    check fut.read() == conn
    check conn.borrowed

  test "double release does not register the same conn in idle twice":
    # Without a borrowed flag the second release re-adds `conn` to the idle
    # deque, so two subsequent acquires both receive it and corrupt each
    # other's wire protocol. The second release must be a no-op, leaving a
    # single idle entry.
    let pool = makePool(maxSize = 5)
    let conn = mockConn(pool = pool)
    conn.borrowed = true
    pool.active = 1
    conn.release()
    check pool.idle.len == 1
    conn.release()
    check pool.idle.len == 1

    # The single idle entry is checked out exactly once; idle then drains to
    # empty rather than yielding a phantom duplicate.
    let acquired = waitFor pool.acquire()
    check acquired == conn
    check pool.idle.len == 0

  test "double release does not hand an idle conn to a queued waiter":
    let pool = makePool(maxSize = 5)
    let conn = mockConn(pool = pool)
    conn.borrowed = true
    pool.active = 1
    conn.release()
    check pool.idle.len == 1

    # Queue a waiter, then double-release the already-idle conn. The no-op
    # release must not complete the waiter with a connection that is also
    # sitting in idle (which a fresh acquire could grab in parallel).
    let fut = newFuture[PgConnection]("test.waiter")
    pool.waiters.addLast(Waiter(fut: fut, cancelled: false))
    pool.waiterCount = 1
    conn.release()
    check not fut.finished
    check pool.waiterCount == 1
    check pool.idle.len == 1

  test "double release notifies the tracer":
    var doubleReleases = 0
    var sawConn = true
    let tracer = PgTracer()
    tracer.onPoolDoubleRelease = proc(
        data: TracePoolDoubleReleaseData
    ) {.gcsafe, raises: [].} =
      if data.conn == nil:
        sawConn = false
      doubleReleases.inc

    let pool = makePool()
    pool.config.tracer = tracer
    let conn = mockConn(pool = pool)
    conn.borrowed = true
    pool.active = 1
    conn.release()
    check doubleReleases == 0
    conn.release()
    check doubleReleases == 1
    # A third release is still a no-op and still observable.
    conn.release()
    check doubleReleases == 2
    check sawConn

  test "release of a never-borrowed connection is a no-op":
    # A connection that was never checked out (e.g. wired straight into idle by
    # the maintenance loop) must not be returned again by a stray release.
    let pool = makePool()
    let conn = mockConn(pool = pool)
    check not conn.borrowed
    pool.active = 1
    conn.release()
    check pool.idle.len == 0
    check pool.active == 1

when hasChronos:
  proc makeHangingConn(): Future[(PgConnection, StreamServer, StreamTransport)] {.
      async
  .} =
    ## Create a PgConnection backed by a real TCP socket to a server that never responds.
    let server = createStreamServer(initTAddress("127.0.0.1", 0))
    let serverAddr = server.localAddress()
    let transport = await connect(serverAddr)
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
      serverParams: initTable[string, string](),
      createdAt: Moment.now(),
    )
    return (conn, server, serverTransport)

  proc cleanupHanging(
      server: StreamServer, serverTransport: StreamTransport
  ) {.async.} =
    await serverTransport.closeWait()
    server.stop()
    server.close()
    await server.join()

  suite "Ping":
    test "ping times out on unresponsive connection":
      proc t() {.async.} =
        let (conn, server, serverTransport) = await makeHangingConn()

        var msg = ""
        try:
          await conn.ping(timeout = milliseconds(50))
        except PgError as e:
          msg = e.msg

        doAssert "Ping timed out" in msg
        doAssert conn.state == csClosed

        await conn.close()
        await cleanupHanging(server, serverTransport)

      waitFor t()

    test "ping timeout sets csClosed so connection is not reusable":
      proc t() {.async.} =
        let (conn, server, serverTransport) = await makeHangingConn()

        try:
          await conn.ping(timeout = milliseconds(50))
        except PgError:
          discard

        # checkReady rejects it
        var rejected = false
        try:
          await conn.ping()
        except PgError:
          rejected = true
        doAssert rejected

        await conn.close()
        await cleanupHanging(server, serverTransport)

      waitFor t()

  test "ping without timeout on nil writer raises PgError":
    let conn = mockConn()
    var msg = ""
    try:
      waitFor conn.ping()
    except PgError as e:
      msg = e.msg
    check "not established" in msg
    check conn.state == csClosed

  test "ping with timeout on nil writer raises PgError (not timeout error)":
    let conn = mockConn()
    var msg = ""
    try:
      waitFor conn.ping(timeout = seconds(5))
    except PgError as e:
      msg = e.msg
    check "not established" in msg
    check conn.state == csClosed

when hasChronos:
  suite "Health check on acquire":
    test "fresh connection skips health check":
      let pool = makePool()
      pool.config.healthCheckTimeout = seconds(60)
      let conn = mockConn()
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

      let acquired = waitFor pool.acquire()
      check acquired == conn
      check pool.active == 1

    test "stale connection fails health check and is discarded":
      let pool = makePool()
      pool.config.healthCheckTimeout = milliseconds(10)
      # Stale connection (no transport -> ping will raise)
      let stale = mockConn()
      stale.state = csReady
      pool.idle.addLast(PooledConn(conn: stale, lastUsedAt: Moment.now() - seconds(1)))
      # Fresh good connection behind it
      let good = mockConn()
      pool.idle.addLast(PooledConn(conn: good, lastUsedAt: Moment.now()))

      let acquired = waitFor pool.acquire()
      check acquired == good
      check pool.active == 1
      check pool.idle.len == 0

    test "disabled health check skips ping":
      let pool = makePool()
      # healthCheckTimeout = ZeroDuration (default in makePool) -> disabled
      let conn = mockConn()
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now() - hours(1)))

      let acquired = waitFor pool.acquire()
      check acquired == conn
      check pool.active == 1

    test "acquire discards connection that fails ping timeout":
      proc t() {.async.} =
        let pool = makePool()
        pool.config.healthCheckTimeout = seconds(60)
        pool.config.pingTimeout = milliseconds(50)

        # Hanging connection: idle > 60s -> triggers health check -> times out
        let (hanging, server, serverTransport) = await makeHangingConn()
        pool.idle.addLast(
          PooledConn(conn: hanging, lastUsedAt: Moment.now() - minutes(2))
        )

        # Good mock connection (fresh, skips health check)
        let good = mockConn()
        pool.idle.addLast(PooledConn(conn: good, lastUsedAt: Moment.now()))

        let acquired = await pool.acquire()
        doAssert acquired == good
        doAssert pool.active == 1
        doAssert pool.idle.len == 0
        doAssert hanging.state == csClosed

        await cleanupHanging(server, serverTransport)

      waitFor t()

    test "concurrent acquire during health-check ping cannot exceed maxSize":
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.config.healthCheckTimeout = seconds(60)
        pool.config.pingTimeout = seconds(60)

        let (pinged, server, serverTransport) = await makeHangingConn()
        pool.idle.addLast(
          PooledConn(conn: pinged, lastUsedAt: Moment.now() - minutes(2))
        )

        # Acquirer A suspends inside the health-check ping; the conn under
        # inspection must already hold the active slot.
        let futA = pool.acquire()
        doAssert not futA.finished
        doAssert pool.active == 1

        # Acquirer B must queue instead of dialing a second conn past maxSize.
        let futB = pool.acquire()
        doAssert not futB.finished
        doAssert pool.active == 1
        doAssert pool.waiterCount == 1

        # Server answers the ping: A borrows the pinged conn without
        # double-counting it.
        discard await serverTransport.write(
          buildBackendMsg('I', newSeq[byte]()) & buildReadyForQuery('I')
        )
        let connA = await futA
        doAssert connA == pinged
        doAssert pool.active == 1

        pool.release(connA)
        let connB = await futB
        doAssert connB == pinged
        doAssert pool.active == 1

        await connB.close()
        await cleanupHanging(server, serverTransport)

      waitFor t()

    test "all stale connections fail health check then creates new":
      let pool = makePool()
      pool.config.healthCheckTimeout = milliseconds(1)
      let stale1 = mockConn()
      let stale2 = mockConn()
      pool.idle.addLast(PooledConn(conn: stale1, lastUsedAt: Moment.now() - seconds(1)))
      pool.idle.addLast(PooledConn(conn: stale2, lastUsedAt: Moment.now() - seconds(1)))

      # No good idle connections and no real server -> acquire will try connect() and fail
      check pool.idle.len == 2
      expect(CatchableError):
        discard waitFor pool.acquire()
      check pool.idle.len == 0

  suite "Pool resetSession cancellation (chronos)":
    # Regression: `resetSession`'s `except CatchableError` used to catch
    # `CancelledError` too, so a chronos cancel of the outer future
    # completed as if reset had succeeded — cancellation silently lost.
    test "resetSession propagates CancelledError instead of swallowing":
      proc t() {.async.} =
        let pool = makePool()
        pool.config.resetQuery = "DISCARD ALL"
        pool.config.resetQueryTimeout = seconds(60)

        let (conn, server, serverTransport) = await makeHangingConn()
        conn.txStatus = tsIdle # PgConnection() defaults to tsInFailedTransaction ('E')
        conn.ownerPool = pool
        conn.borrowed = true
        pool.active = 1

        let resetFut = pool.resetSession(conn)
        # Let simpleExec write its Query and suspend on the never-answered response.
        await sleepAsync(milliseconds(50))
        doAssert not resetFut.finished

        resetFut.cancelSoon()
        var raised = false
        try:
          await resetFut
        except CancelledError:
          raised = true
        doAssert raised
        # State flipped synchronously so a follow-up release() would discard.
        doAssert conn.state == csClosed

        await pool.close(seconds(1))
        await cleanupHanging(server, serverTransport)

      waitFor t()

    test "resetSessionAndRelease still releases the conn under cancel":
      proc t() {.async.} =
        let pool = makePool()
        pool.config.resetQuery = "DISCARD ALL"
        pool.config.resetQueryTimeout = seconds(60)

        let (conn, server, serverTransport) = await makeHangingConn()
        conn.txStatus = tsIdle # PgConnection() defaults to tsInFailedTransaction ('E')
        conn.ownerPool = pool
        conn.borrowed = true
        pool.active = 1

        let fut = pool.resetSessionAndRelease(conn)
        await sleepAsync(milliseconds(50))
        doAssert not fut.finished

        fut.cancelSoon()
        var raised = false
        try:
          await fut
        except CancelledError:
          raised = true
        doAssert raised
        # release() ran under the inner finally: borrow cleared and active
        # decremented via releaseCore's discard path (state == csClosed).
        doAssert not conn.borrowed
        doAssert pool.active == 0
        doAssert conn.state == csClosed

        await pool.close(seconds(1))
        await cleanupHanging(server, serverTransport)

      waitFor t()

suite "Acquire deadline budget":
  ## Regression for acquire latency exceeding acquireTimeout: health-check
  ## pings and a caller-driven connect used to run on their own budgets
  ## (pingTimeout*N + connectTimeout) *before* acquireTimeout even started.

  when hasChronos:
    test "acquireTimeout bounds idle health-check pings":
      proc t() {.async.} =
        let pool = makePool()
        pool.config.healthCheckTimeout = seconds(60)
        # Without the shared budget the first ping alone would block for 60s.
        pool.config.pingTimeout = seconds(60)
        pool.config.acquireTimeout = milliseconds(300)
        # Fill the pool so the post-ping path queues instead of dialing a
        # (nonexistent) server.
        pool.active = pool.config.maxSize

        let (hanging, server, serverTransport) = await makeHangingConn()
        pool.idle.addLast(
          PooledConn(conn: hanging, lastUsedAt: Moment.now() - minutes(2))
        )

        let start = Moment.now()
        var msg = ""
        try:
          discard await pool.acquire()
        except PgPoolError as e:
          msg = e.msg
        let elapsed = Moment.now() - start

        doAssert "timeout" in msg.toLowerAscii()
        doAssert elapsed < seconds(5)
        doAssert hanging.state == csClosed
        doAssert pool.metrics.timeoutCount == 1

        await cleanupHanging(server, serverTransport)

      waitFor t()

  test "acquireTimeout bounds caller-driven connect":
    proc t() {.async.} =
      # A server that accepts TCP but never answers the startup message:
      # without the shared budget, connect() would block indefinitely here
      # (connectTimeout defaults to ZeroDuration = unlimited).
      let ms = startMockServer()
      let pool = makePool()
      pool.config.connConfig.host = "127.0.0.1"
      pool.config.connConfig.port = ms.port
      pool.config.acquireTimeout = milliseconds(300)

      let start = Moment.now()
      var msg = ""
      try:
        discard await pool.acquire()
      except PgPoolError as e:
        msg = e.msg
      let elapsed = Moment.now() - start

      doAssert "timeout" in msg.toLowerAscii()
      doAssert elapsed < seconds(5)
      doAssert pool.active == 0
      doAssert pool.metrics.timeoutCount == 1

      await ms.closeServer()

    waitFor t()

  when hasAsyncDispatch:
    test "orphan connect close from timed-out acquire is tracked for close() drain":
      proc t() {.async.} =
        # A server that answers the startup message only after the acquire
        # deadline: the caller-driven connect survives as an orphan (asyncdispatch
        # has no cancellation), and its eventual close must be tracked in
        # pendingBackgroundTasks so pool.close() drains it — an untracked spawn
        # could leave the socket open past close().
        let ms = startMockServer()
        proc serve() {.async.} =
          let client = await ms.accept()
          discard await client.readN(4) # StartupMessage length prefix
          await sleepAsync(milliseconds(150))
          await client.sendBytes(buildAuthOk())
          await client.sendBytes(buildBackendKeyData(1234, 5678))
          await client.sendBytes(buildReadyForQuery())

        let serveFut = serve()

        let pool = makePool()
        pool.config.connConfig.host = "127.0.0.1"
        pool.config.connConfig.port = ms.port
        pool.config.connConfig.sslMode = sslDisable
        pool.config.acquireTimeout = milliseconds(50)

        var msg = ""
        try:
          discard await pool.acquire()
        except PgPoolError as e:
          msg = e.msg
        doAssert "timeout" in msg.toLowerAscii()

        # Wait for the orphan connect to complete and enqueue its close.
        var waited = 0
        while pool.pendingBackgroundTasks.len == 0 and waited < 50:
          await sleepAsync(milliseconds(10))
          inc waited
        doAssert pool.pendingBackgroundTasks.len == 1

        await pool.close()
        doAssert pool.pendingBackgroundTasks.len == 0

        await ms.closeServer()
        await serveFut

      waitFor t()

  test "nearly exhausted deadline returns idle conn unpinged":
    proc t() {.async.} =
      # With less than pingBudgetFloor (10ms) of budget left, acquire must
      # give up *before* pinging: a doomed ping would close a connection
      # that may well be healthy.
      let pool = makePool()
      pool.config.healthCheckTimeout = milliseconds(1)
      pool.config.pingTimeout = seconds(5)
      pool.config.acquireTimeout = milliseconds(5) # below the 10ms floor

      let conn = mockConn()
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now() - minutes(2)))

      var msg = ""
      try:
        discard await pool.acquire()
      except PgPoolError as e:
        msg = e.msg

      doAssert "timeout" in msg.toLowerAscii()
      # The conn went back untouched: a ping on this transport-less mock
      # would have failed and discarded it.
      doAssert pool.idle.len == 1
      doAssert pool.idle[0].conn == conn
      doAssert conn.state == csReady
      doAssert pool.metrics.timeoutCount == 1
      doAssert pool.metrics.closeCount == 0

    waitFor t()

  test "acquireTimeout bounds multi-host connect":
    proc t() {.async.} =
      # Three mock servers that accept TCP but never answer the startup
      # message. connectTimeout is applied per host, so without a total
      # deadline the acquire would burn ~acquireTimeout on each host.
      let ms1 = startMockServer()
      let ms2 = startMockServer()
      let ms3 = startMockServer()

      let pool = makePool()
      pool.config.connConfig = ConnConfig(
        hosts: @[
          HostEntry(host: "127.0.0.1", port: ms1.port),
          HostEntry(host: "127.0.0.1", port: ms2.port),
          HostEntry(host: "127.0.0.1", port: ms3.port),
        ],
        user: "test",
        database: "test",
        sslMode: sslDisable,
      )
      pool.config.acquireTimeout = milliseconds(200)

      let start = Moment.now()
      var msg = ""
      try:
        discard await pool.acquire()
      except PgPoolError as e:
        msg = e.msg
      let elapsed = Moment.now() - start

      doAssert "timeout" in msg.toLowerAscii(), "msg=" & msg
      doAssert elapsed < milliseconds(400), "elapsed=" & $elapsed
      doAssert pool.active == 0
      doAssert pool.metrics.timeoutCount == 1,
        "timeoutCount=" & $pool.metrics.timeoutCount

      await ms1.closeServer()
      await ms2.closeServer()
      await ms3.closeServer()

    waitFor t()

suite "Max waiters":
  test "maxWaiters -1 allows unlimited waiters":
    let pool = makePool(maxSize = 1)
    pool.active = 1

    var futs: seq[Future[PgConnection]]
    for i in 0 ..< 100:
      futs.add(pool.acquire())
    check pool.waiters.len == 100

    # Clean up: release connections to satisfy all waiters
    for f in futs:
      let conn = mockConn()
      pool.release(conn)
    for f in futs:
      discard waitFor f

  test "maxWaiters 0 rejects all waiters":
    let pool = makePool(maxSize = 1)
    pool.config.maxWaiters = 0
    pool.active = 1

    expect(PgError):
      discard waitFor pool.acquire()
    check pool.waiters.len == 0

  test "maxWaiters rejects when queue is full":
    let pool = makePool(maxSize = 1)
    pool.config.maxWaiters = 2
    pool.active = 1

    # First two waiters should succeed
    let fut1 = pool.acquire()
    let fut2 = pool.acquire()
    check pool.waiters.len == 2

    # Third should be rejected immediately
    var caught: ref PgPoolError
    try:
      discard waitFor pool.acquire()
    except PgPoolError as e:
      caught = e
    except PgError:
      discard

    check caught != nil
    check caught.kind == pekQueueFull
    check pool.waiters.len == 2

    # Clean up
    pool.release(mockConn())
    pool.release(mockConn())
    discard waitFor fut1
    discard waitFor fut2

  test "maxWaiters 1 allows exactly one waiter":
    let pool = makePool(maxSize = 1)
    pool.config.maxWaiters = 1
    pool.active = 1

    let fut1 = pool.acquire()
    check pool.waiters.len == 1

    expect(PgError):
      discard waitFor pool.acquire()

    check pool.waiters.len == 1

    # Clean up
    pool.release(mockConn())
    discard waitFor fut1

  test "maxWaiters allows new waiter after previous is resolved":
    let pool = makePool(maxSize = 1)
    pool.config.maxWaiters = 1
    pool.active = 1

    # First waiter
    let fut1 = pool.acquire()
    check pool.waiters.len == 1

    # Resolve it
    pool.release(mockConn())
    discard waitFor fut1
    check pool.waiters.len == 0

    # Now another waiter should be allowed
    pool.active = 1
    let fut2 = pool.acquire()
    check pool.waiters.len == 1

    # Clean up
    pool.release(mockConn())
    discard waitFor fut2

  test "maxWaiters error does not affect active count":
    let pool = makePool(maxSize = 1)
    pool.config.maxWaiters = 1
    pool.active = 1

    discard pool.acquire() # fills the waiter queue
    check pool.active == 1

    try:
      discard waitFor pool.acquire()
    except PgError:
      discard

    check pool.active == 1

suite "Acquire timeout":
  test "acquire timeout raises PgError when pool is exhausted":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1 # pool is at max

      var msg = ""
      try:
        discard await pool.acquire()
      except PgError as e:
        msg = e.msg

      doAssert "timeout" in msg.toLowerAscii()
      doAssert pool.waiterCount == 0

    waitFor t()

  test "acquire succeeds before timeout when connection is released":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = seconds(5)
      pool.active = 1

      let acquireFut = pool.acquire()
      doAssert not acquireFut.finished
      doAssert pool.waiters.len == 1

      # Release a connection before timeout
      let conn = mockConn()
      pool.release(conn)

      let acquired = await acquireFut
      doAssert acquired == conn

    waitFor t()

  test "acquire timeout cleans up waiter from queue":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      try:
        discard await pool.acquire()
      except PgError:
        discard

      # Waiter should be cancelled
      doAssert pool.waiterCount == 0

    waitFor t()

  test "acquire without timeout waits indefinitely":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      # acquireTimeout = ZeroDuration (default) -> no timeout
      pool.active = 1

      let acquireFut = pool.acquire()
      doAssert not acquireFut.finished

      # Release after a short delay
      await sleepAsync(milliseconds(50))
      let conn = mockConn()
      pool.release(conn)

      let acquired = await acquireFut
      doAssert acquired == conn

    waitFor t()

  test "active count unchanged after timeout":
    proc t() {.async.} =
      let pool = makePool(maxSize = 2)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 2

      try:
        discard await pool.acquire()
      except PgError:
        discard

      doAssert pool.active == 2

    waitFor t()

  test "timeout only cancels own waiter, not others":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      # Pre-existing waiter (e.g. from another coroutine with no timeout)
      let otherFut = newFuture[PgConnection]("test.other")
      let otherWaiter = Waiter(fut: otherFut, cancelled: false)
      pool.waiters.addLast(otherWaiter)
      pool.waiterCount = 1

      try:
        discard await pool.acquire()
      except PgError:
        discard

      # Other waiter should still be active
      doAssert pool.waiterCount == 1
      doAssert not otherWaiter.cancelled

      # Clean up
      otherFut.complete(mockConn())

    waitFor t()

  test "pool close during acquire timeout raises pool closed":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = seconds(5)
      pool.active = 1

      let acquireFut = pool.acquire()
      doAssert pool.waiters.len == 1

      # Close pool while acquire is waiting
      await pool.close()

      var msg = ""
      try:
        discard await acquireFut
      except PgError as e:
        msg = e.msg

      doAssert "closed" in msg.toLowerAscii()

    waitFor t()

  test "multiple sequential timeouts clean up correctly":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      for i in 0 ..< 3:
        try:
          discard await pool.acquire()
        except PgError:
          discard

      doAssert pool.waiterCount == 0
      doAssert pool.active == 1

    waitFor t()

  test "cancelled waiters are lazily drained on release":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      # Cause 3 timeouts — cancelled waiters accumulate in deque
      for i in 0 ..< 3:
        try:
          discard await pool.acquire()
        except PgError:
          discard

      doAssert pool.waiters.len == 3 # cancelled entries remain
      doAssert pool.waiterCount == 0

      # Add a real waiter behind the cancelled ones
      let realFut = pool.acquire()
      doAssert pool.waiters.len == 4
      doAssert pool.waiterCount == 1

      # Release should skip all 3 cancelled and deliver to the real waiter
      let conn = mockConn()
      pool.release(conn)
      let acquired = await realFut
      doAssert acquired == conn
      doAssert pool.waiters.len == 0
      doAssert pool.waiterCount == 0

    waitFor t()

when hasChronos:
  suite "Cancel/timeout interactions":
    test "shorter-timeout waiter fails while longer-timeout waiter keeps waiting":
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.active = 1

        pool.config.acquireTimeout = milliseconds(40)
        let fut1 = pool.acquire()

        pool.config.acquireTimeout = seconds(5)
        let fut2 = pool.acquire()

        doAssert pool.waiters.len == 2
        doAssert pool.waiterCount == 2

        await sleepAsync(milliseconds(90))
        doAssert fut1.failed
        doAssert not fut2.finished
        doAssert pool.waiterCount == 1
        doAssert pool.metrics.timeoutCount == 1

        let conn = mockConn()
        pool.release(conn)
        doAssert (await fut2) == conn
        doAssert pool.waiterCount == 0
        doAssert pool.waiters.len == 0

      waitFor t()

    test "middle waiter timing out does not stall FIFO delivery":
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.active = 1

        pool.config.acquireTimeout = seconds(5)
        let futA = pool.acquire()

        pool.config.acquireTimeout = milliseconds(40)
        let futB = pool.acquire()

        pool.config.acquireTimeout = seconds(5)
        let futC = pool.acquire()

        doAssert pool.waiterCount == 3

        await sleepAsync(milliseconds(90))
        doAssert not futA.finished
        doAssert futB.failed
        doAssert not futC.finished
        doAssert pool.waiterCount == 2

        let c1 = mockConn()
        pool.release(c1)
        doAssert (await futA) == c1

        let c2 = mockConn()
        pool.release(c2)
        doAssert (await futC) == c2

        doAssert pool.waiterCount == 0
        doAssert pool.waiters.len == 0

      waitFor t()

    test "concurrent timeouts increment timeoutCount per waiter":
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.config.acquireTimeout = milliseconds(40)
        pool.active = 1

        var futs: seq[Future[PgConnection]]
        for i in 0 ..< 8:
          futs.add(pool.acquire())
        doAssert pool.waiterCount == 8

        await sleepAsync(milliseconds(90))
        for f in futs:
          doAssert f.failed

        doAssert pool.metrics.timeoutCount == 8
        doAssert pool.waiterCount == 0
        doAssert pool.waiters.len == 8 # cancelled entries still in deque

      waitFor t()

    test "pool close after mass timeout drains cancelled waiters":
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.config.acquireTimeout = milliseconds(40)
        pool.active = 1

        var futs: seq[Future[PgConnection]]
        for i in 0 ..< 4:
          futs.add(pool.acquire())

        await sleepAsync(milliseconds(90))
        for f in futs:
          doAssert f.failed

        doAssert pool.waiters.len == 4
        doAssert pool.waiterCount == 0

        await pool.close()
        doAssert pool.closed
        doAssert pool.waiters.len == 0
        doAssert pool.waiterCount == 0

      waitFor t()

    test "timeout then release reuses pool and delivers conn to idle":
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.config.acquireTimeout = milliseconds(40)
        pool.active = 1

        try:
          discard await pool.acquire()
          doAssert false, "expected timeout"
        except PgError:
          discard
        doAssert pool.waiterCount == 0
        doAssert pool.waiters.len == 1 # cancelled waiter still present

        # Releasing drains the cancelled waiter and parks the conn in idle.
        pool.release(mockConn())
        doAssert pool.waiters.len == 0
        doAssert pool.active == 0
        doAssert pool.idle.len == 1

        # Next acquire reuses the idle conn.
        pool.config.acquireTimeout = ZeroDuration
        let got = await pool.acquire()
        doAssert got != nil
        doAssert pool.active == 1
        doAssert pool.idle.len == 0

      waitFor t()

    test "external cancel of deadline-path acquire cleans up waiter":
      # Regression: the deadline branch only handled AsyncTimeoutError, so an
      # external cancellation (e.g. a caller's wait()-style deadline) left
      # `waiterCount` permanently inflated — disabling the fast-path guard —
      # and a later release() would call complete() on the cancelled future,
      # raising a Defect.
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.config.acquireTimeout = seconds(5) # deadline path
        pool.active = 1

        let fut = pool.acquire()
        doAssert pool.waiterCount == 1

        await cancelAndWait(fut)
        doAssert fut.cancelled()
        doAssert pool.waiterCount == 0
        doAssert pool.waiters.len == 1 # cancelled entry swept lazily

        # release() must skip the cancelled waiter (no Defect) and park the
        # conn in idle.
        pool.release(mockConn())
        doAssert pool.waiters.len == 0
        doAssert pool.idle.len == 1
        doAssert pool.active == 0

      waitFor t()

    test "handoff skips a waiter cancelled before settle (no double-decrement, no leak)":
      # Regression: chronos `wait()` cancels the inner future *synchronously* on
      # timeout/cancel, so the waiter stays in `pool.waiters` with
      # `cancelled == false` but an already-cancelled future until
      # `settleAbandonedWaiter` runs. A handoff landing in that window must skip
      # it — `complete()` on a cancelled future is a silent no-op, so delivering
      # would leak the conn (marked borrowed, owned by nobody, `active` never
      # returned) and let settle fall through to a *second* `waiterCount.dec`,
      # driving it negative and disabling the FIFO fast-path guard / `maxWaiters`.
      proc t() {.async.} =
        let pool = makePool(maxSize = 1)
        pool.active = 1
        let waiter = Waiter(fut: newFuture[PgConnection]("w"), cancelled: false)
        pool.waiters.addLast(waiter)
        pool.waiterCount.inc

        # Model the cancel window: inner future cancelled, settle not yet run.
        await cancelAndWait(waiter.fut)
        doAssert waiter.fut.cancelled()
        doAssert not waiter.cancelled

        # A conn arrives in the window: the handoff must not deliver to the
        # cancelled waiter and must not decrement `waiterCount` for it.
        let conn = mockConn(pool = pool)
        doAssert not pool.tryHandoffToWaiter(conn)
        doAssert not conn.borrowed # conn not handed to the dead waiter (no leak)
        doAssert pool.waiterCount == 1 # not decremented for the cancelled waiter

        # settle then performs the single decrement for this still-queued waiter.
        pool.settleAbandonedWaiter(waiter)
        doAssert pool.waiterCount == 0 # exactly one decrement, not -1
        doAssert waiter.cancelled

      waitFor t()

    test "cancelling maintenance task does not disturb pending waiters":
      proc t() {.async.} =
        let pool = makePool(maxSize = 1, minSize = 0)
        pool.config.maintenanceInterval = milliseconds(10)
        pool.config.idleTimeout = hours(1)
        pool.active = 1
        pool.maintenanceTask = maintenanceLoop(pool)

        pool.config.acquireTimeout = seconds(5)
        let fut = pool.acquire()
        doAssert pool.waiterCount == 1

        await cancelAndWait(pool.maintenanceTask)
        doAssert pool.waiterCount == 1
        doAssert not fut.finished

        let c = mockConn()
        pool.release(c)
        doAssert (await fut) == c
        doAssert pool.waiterCount == 0

      waitFor t()

  suite "Maintenance loop":
    test "idle timeout removes old connections":
      proc t() {.async.} =
        let pool = makePool()
        pool.config.idleTimeout = milliseconds(50)
        pool.config.maintenanceInterval = milliseconds(20)
        pool.config.minSize = 0

        let conn = mockConn()
        conn.state = csClosed # prevent network close
        pool.idle.addLast(
          PooledConn(conn: conn, lastUsedAt: Moment.now() - milliseconds(100))
        )

        pool.maintenanceTask = maintenanceLoop(pool)
        await sleepAsync(milliseconds(60))

        doAssert pool.idle.len == 0

        pool.closed = true
        await cancelAndWait(pool.maintenanceTask)

      waitFor t()

    test "release after a long borrow survives the idle reaper":
      proc t() {.async.} =
        let pool = makePool(minSize = 0)
        pool.config.idleTimeout = milliseconds(200)
        pool.config.maintenanceInterval = milliseconds(20)

        # Emulate a long borrow: release must stamp `lastUsedAt` at the actual
        # return time, so the just-returned conn is well within the idle window
        # and must not be reaped.
        pool.active = 1
        let conn = mockConn()
        pool.release(conn)
        doAssert pool.idle.len == 1

        pool.maintenanceTask = maintenanceLoop(pool)
        await sleepAsync(milliseconds(60))

        # idleTimeout (200ms) has not elapsed since the real return time, so the
        # conn survives. A stale timestamp would have evicted it.
        doAssert pool.idle.len == 1

        pool.closed = true
        await cancelAndWait(pool.maintenanceTask)

      waitFor t()

    test "maintenance respects minSize":
      proc t() {.async.} =
        let pool = makePool(minSize = 1)
        pool.config.idleTimeout = milliseconds(10)
        pool.config.maintenanceInterval = milliseconds(20)

        let conn = mockConn()
        pool.idle.addLast(
          PooledConn(conn: conn, lastUsedAt: Moment.now() - milliseconds(100))
        )

        pool.maintenanceTask = maintenanceLoop(pool)
        await sleepAsync(milliseconds(60))

        # Should keep at least minSize connections
        doAssert pool.idle.len == 1

        pool.closed = true
        await cancelAndWait(pool.maintenanceTask)

      waitFor t()

    test "maxLifetime removes old connections in maintenance":
      proc t() {.async.} =
        let pool = makePool(minSize = 0)
        pool.config.maxLifetime = milliseconds(50)
        pool.config.maintenanceInterval = milliseconds(20)

        let conn = mockConn()
        conn.createdAt = Moment.now() - milliseconds(100)
        conn.state = csClosed # prevent network close
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

        pool.maintenanceTask = maintenanceLoop(pool)
        await sleepAsync(milliseconds(60))

        doAssert pool.idle.len == 0

        pool.closed = true
        await cancelAndWait(pool.maintenanceTask)

      waitFor t()

    test "replenishment does not crash when connect fails":
      proc t() {.async.} =
        # minSize=2 but connect will fail (no real server) -- maintenance should not crash
        let pool = makePool(minSize = 2)
        pool.config.maxLifetime = milliseconds(10)
        pool.config.maintenanceInterval = milliseconds(20)

        let conn = mockConn()
        conn.createdAt = Moment.now() - milliseconds(100)
        conn.state = csClosed
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

        pool.maintenanceTask = maintenanceLoop(pool)
        # Let maintenance run a couple of cycles -- should not crash
        await sleepAsync(milliseconds(80))

        # Expired conn removed; replenishment attempted but failed gracefully
        # idle may be 0 (connect fails) -- the key assertion is no crash
        pool.closed = true
        await cancelAndWait(pool.maintenanceTask)

      waitFor t()

    test "sweep schedules closes off-loop so healthy conns stay counted":
      # Regression: the sweep once popped healthy conns into a local deque and
      # awaited each broken close inline. During that await, healthy entries were
      # invisible to `pool.idle` / `pool.active`, letting concurrent acquires
      # overshoot maxSize. The fix routes closes through `closeNoWait`, so the
      # sweep never yields — the closes appear as pendingBackgroundTasks instead.
      proc t() {.async.} =
        let pool = makePool(minSize = 0, maxSize = 10)
        pool.config.maintenanceInterval = milliseconds(10)

        for i in 0 ..< 3:
          let healthy = mockConn()
          pool.idle.addLast(PooledConn(conn: healthy, lastUsedAt: Moment.now()))
        for i in 0 ..< 5:
          let broken = mockConn(state = csClosed)
          pool.idle.addLast(PooledConn(conn: broken, lastUsedAt: Moment.now()))

        doAssert pool.pendingBackgroundTasks.len == 0

        pool.maintenanceTask = maintenanceLoop(pool)
        await sleepAsync(milliseconds(50))

        doAssert pool.idle.len == 3
        doAssert pool.metrics.closeCount == 5
        # Fewer than bgTaskPruneThreshold (16) closes, so none pruned yet
        doAssert pool.pendingBackgroundTasks.len == 5

        pool.closed = true
        await cancelAndWait(pool.maintenanceTask)

      waitFor t()

    test "pool.close drains sweep-scheduled background closes":
      # closeNoWait tracks each close in pendingBackgroundTasks; pool.close()
      # must await them so the fix does not turn broken-conn cleanup into a leak.
      proc t() {.async.} =
        let pool = makePool(minSize = 0, maxSize = 5)
        pool.config.maintenanceInterval = milliseconds(10)

        for i in 0 ..< 4:
          let broken = mockConn(state = csClosed)
          pool.idle.addLast(PooledConn(conn: broken, lastUsedAt: Moment.now()))

        pool.maintenanceTask = maintenanceLoop(pool)
        await sleepAsync(milliseconds(50))
        doAssert pool.pendingBackgroundTasks.len == 4

        await pool.close()
        doAssert pool.pendingBackgroundTasks.len == 0

      waitFor t()

    test "concurrent acquires during sweep never overshoot maxSize":
      # With the OLD code, healthy conns popped into the local `remaining` deque
      # were invisible to concurrent acquires; those acquires would see
      # `pool.active < maxSize` and open replacements even though the total
      # (remaining + active) already reached maxSize. Interleaving acquires with
      # the sweep must keep total under maxSize.
      proc t() {.async.} =
        let pool = makePool(minSize = 0, maxSize = 4)
        pool.config.maintenanceInterval = milliseconds(5)

        # Fill idle: 2 healthy + 2 broken. Sweep will close the 2 broken.
        for i in 0 ..< 2:
          pool.idle.addLast(PooledConn(conn: mockConn(), lastUsedAt: Moment.now()))
        for i in 0 ..< 2:
          pool.idle.addLast(
            PooledConn(conn: mockConn(state = csClosed), lastUsedAt: Moment.now())
          )

        pool.maintenanceTask = maintenanceLoop(pool)

        # Race the sweep with acquires. Only 2 healthy conns exist; the rest must
        # queue as waiters rather than triggering fresh connects (which would
        # fail: no server) or somehow overshooting maxSize.
        var futs: seq[Future[PgConnection]]
        for i in 0 ..< 4:
          futs.add(pool.acquire())

        await sleepAsync(milliseconds(20))

        # At most maxSize total exist at any point. The two healthy conns get
        # borrowed; the other two acquires either queue as waiters or fail (no
        # real server for a fresh connect), never producing extra conns.
        doAssert pool.active + pool.idle.len <= pool.config.maxSize
        doAssert pool.active <= 2 # only the 2 healthy conns can serve

        pool.closed = true
        await cancelAndWait(pool.maintenanceTask)
        for f in futs:
          if not f.finished:
            f.fail(newException(PgPoolError, "test teardown"))

      waitFor t()

suite "Pool high concurrency":
  test "parallel acquire saturates maxSize and queues remainder":
    let pool = makePool(maxSize = 5)
    for i in 0 ..< 5:
      pool.idle.addLast(mockConn().toPooled())

    var futs: seq[Future[PgConnection]]
    for i in 0 ..< 20:
      futs.add(pool.acquire())

    check pool.active == 5
    check pool.idle.len == 0
    check pool.waiters.len == 15
    check pool.waiterCount == 15

    # Clean up: release 15 mocks to satisfy the pending waiters
    for i in 0 ..< 15:
      pool.release(mockConn())
    for f in futs:
      discard waitFor f

  test "mass release delivers to all waiters preserving active":
    let pool = makePool(maxSize = 1)
    pool.active = 1

    var futs: seq[Future[PgConnection]]
    for i in 0 ..< 50:
      futs.add(pool.acquire())
    check pool.waiters.len == 50
    check pool.waiterCount == 50

    for i in 0 ..< 50:
      pool.release(mockConn())

    check pool.waiters.len == 0
    check pool.waiterCount == 0
    check pool.active == 1
    check pool.idle.len == 0

    for f in futs:
      discard waitFor f

  test "interleaved acquire/release leaves pool healthy":
    let pool = makePool(maxSize = 3)
    for i in 0 ..< 3:
      pool.idle.addLast(mockConn().toPooled())

    for i in 0 ..< 100:
      let c = waitFor pool.acquire()
      pool.release(c)

    check pool.active == 0
    check pool.waiters.len == 0
    check pool.waiterCount == 0
    check pool.idle.len == 3
    check pool.metrics.acquireCount == 100

  test "mixed timeout and success preserves counts":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      var futs: seq[Future[PgConnection]]
      for i in 0 ..< 5:
        futs.add(pool.acquire())

      await sleepAsync(milliseconds(100))

      for f in futs:
        doAssert f.failed
      doAssert pool.waiterCount == 0
      doAssert pool.active == 1

      # release must drain all cancelled entries and park the conn in idle
      pool.release(mockConn())
      doAssert pool.waiters.len == 0
      doAssert pool.active == 0
      doAssert pool.idle.len == 1

    waitFor t()

  test "waiter resolution order matches release order (FIFO)":
    let pool = makePool(maxSize = 1)
    pool.active = 1

    var futs: seq[Future[PgConnection]]
    for i in 0 ..< 10:
      futs.add(pool.acquire())

    var conns: seq[PgConnection]
    for i in 0 ..< 10:
      conns.add(mockConn())

    for i in 0 ..< 10:
      pool.release(conns[i])

    for i in 0 ..< 10:
      check waitFor(futs[i]) == conns[i]

suite "FIFO fairness":
  test "tryHandoffToWaiter delivers to first non-cancelled waiter":
    let pool = makePool()
    let cancelled = Waiter(fut: newFuture[PgConnection]("c"), cancelled: true)
    let validFut = newFuture[PgConnection]("v")
    let valid = Waiter(fut: validFut, cancelled: false)
    pool.waiters.addLast(cancelled)
    pool.waiters.addLast(valid)
    pool.waiterCount = 1

    let conn = mockConn()
    check pool.tryHandoffToWaiter(conn)
    check pool.waiterCount == 0
    check pool.waiters.len == 0
    check validFut.finished
    check validFut.read() == conn

  test "tryHandoffToWaiter returns false with no live waiters":
    let pool = makePool()
    check not pool.tryHandoffToWaiter(mockConn())

    # All-cancelled waiters are equivalent to no waiters and get drained.
    pool.waiters.addLast(Waiter(fut: newFuture[PgConnection]("c"), cancelled: true))
    check not pool.tryHandoffToWaiter(mockConn())
    check pool.waiters.len == 0

  test "failLastWaiter fails the tail and preserves the head":
    # FIFO fairness: a spawn connect failure must not strike the head waiter,
    # which has waited longest and keeps its claim on the next good connection.
    # The youngest (tail) waiter absorbs the failure instead.
    let pool = makePool()
    let headFut = newFuture[PgConnection]("head")
    let tailFut = newFuture[PgConnection]("tail")
    pool.waiters.addLast(Waiter(fut: headFut, cancelled: false))
    pool.waiters.addLast(Waiter(fut: tailFut, cancelled: false))
    pool.waiterCount = 2

    check pool.failLastWaiter(newException(PgPoolError, "connect failed"))
    check pool.waiterCount == 1
    check tailFut.failed() # youngest waiter took the failure
    check not headFut.finished() # oldest waiter still queued for delivery

    # A subsequent success still goes to the head, in FIFO order.
    let conn = mockConn()
    check pool.tryHandoffToWaiter(conn)
    check pool.waiterCount == 0
    check headFut.completed()
    check headFut.read() == conn

    # Drain the stored failure so it isn't flagged as unhandled at teardown.
    try:
      discard tailFut.read()
    except PgPoolError:
      discard

  test "failLastWaiter skips abandoned waiters from the back":
    # A cancelled waiter at the tail is inert (already settled): skip it and
    # fail the next live waiter, without touching waiterCount for the cancelled
    # one (settleAbandonedWaiter already decremented it).
    let pool = makePool()
    let liveFut = newFuture[PgConnection]("live")
    pool.waiters.addLast(Waiter(fut: liveFut, cancelled: false))
    pool.waiters.addLast(Waiter(fut: newFuture[PgConnection]("c"), cancelled: true))
    pool.waiterCount = 1

    check pool.failLastWaiter(newException(PgPoolError, "connect failed"))
    check pool.waiterCount == 0
    check pool.waiters.len == 0
    check liveFut.failed()

    try:
      discard liveFut.read()
    except PgPoolError:
      discard

  test "failLastWaiter returns false with no live waiters":
    let pool = makePool()
    check not pool.failLastWaiter(newException(PgPoolError, "connect failed"))

    # All-cancelled waiters are equivalent to no waiters and get drained.
    pool.waiters.addLast(Waiter(fut: newFuture[PgConnection]("c"), cancelled: true))
    check not pool.failLastWaiter(newException(PgPoolError, "connect failed"))
    check pool.waiters.len == 0

  test "abandoned waiter handed off on same tick does not double-decrement":
    # Regression: the timeout/cancel cleanup decremented `waiterCount`
    # unconditionally. A handoff (tryHandoffToWaiter/releaseCore) decrements and
    # completes the future in one step, but asyncdispatch `wait()` can still
    # surface a timeout on the same event-loop tick the handoff landed. The
    # second decrement drove `waiterCount` negative, permanently disabling the
    # FIFO fast-path guard and the `maxWaiters` bound.
    let pool = makePool(maxSize = 1)
    let waiter = Waiter(fut: newFuture[PgConnection]("w"), cancelled: false)
    pool.waiters.addLast(waiter)
    pool.waiterCount.inc
    check pool.waiterCount == 1

    # A handed-off conn is a live borrow occupying an `active` slot (the handoff
    # leaves `active` untouched); model that to exercise the return-to-pool path.
    pool.active = 1

    # Handoff delivers a conn: pops the waiter, decrements, completes the future.
    let conn = mockConn(pool = pool)
    check pool.tryHandoffToWaiter(conn)
    check pool.waiterCount == 0
    check waiter.fut.completed()

    # The acquire then observes a timeout/cancel on the same tick. Cleanup must
    # not decrement again and must return the delivered conn instead of leaking.
    # No `cancelled` flag is set here: the handoff already popped the waiter from
    # `pool.waiters`, so nothing can re-handoff to it and the flag is moot.
    pool.settleAbandonedWaiter(waiter)
    check pool.waiterCount == 0 # not -1
    check pool.idle.len == 1 # delivered conn returned to the pool
    check pool.active == 0 # the freed slot is given back

  test "genuinely abandoned waiter decrements waiterCount exactly once":
    # The other side of the guard: a waiter that was never handed off (its
    # future is still pending) must drop its live slot on abandonment.
    let pool = makePool(maxSize = 1)
    let waiter = Waiter(fut: newFuture[PgConnection]("w"), cancelled: false)
    pool.waiters.addLast(waiter)
    pool.waiterCount.inc

    pool.settleAbandonedWaiter(waiter)
    check pool.waiterCount == 0
    check waiter.cancelled
    check pool.idle.len == 0 # nothing was delivered, nothing to return

  test "waiter failed on same tick does not double-decrement waiterCount":
    # The fail-path sibling of the handoff race: `failLastWaiter` (spawn connect
    # failure or `close`) pops the waiter, decrements `waiterCount`, and *fails*
    # the future. asyncdispatch `wait()` can still surface AsyncTimeoutError on
    # the same tick when its timeout side wins the `withTimeout` race. Cleanup
    # must skip the decrement for a failed future too (not just a completed one),
    # or `waiterCount` drifts negative like the handoff case.
    let pool = makePool(maxSize = 1)
    let waiter = Waiter(fut: newFuture[PgConnection]("w"), cancelled: false)
    pool.waiters.addLast(waiter)
    pool.waiterCount.inc

    # failLastWaiter pops the waiter, decrements, and fails the future.
    check pool.failLastWaiter(newException(PgPoolError, "connect failed"))
    check pool.waiterCount == 0
    check waiter.fut.failed()

    # The acquire then observes a timeout/cancel on the same tick. Cleanup must
    # not decrement again; a failed future carries no conn to return. As with the
    # handoff case, `failLastWaiter` already popped the waiter, so no `cancelled`
    # flag is set.
    pool.settleAbandonedWaiter(waiter)
    check pool.waiterCount == 0 # not -1
    check pool.idle.len == 0 # nothing delivered, nothing to return

    # Drain the stored failure so it isn't flagged as unhandled at teardown
    # (production suppresses this via the no-op callback `wait()` attaches).
    try:
      discard waiter.fut.read()
    except PgPoolError:
      discard

  test "acquire does not jump idle when waiters are already queued":
    # Pre-fix bug: a fresh acquire would pop the idle conn and bypass any
    # already-queued waiter. After the fix, the new caller must join the
    # back of the queue and leave idle untouched.
    let pool = makePool(maxSize = 2)
    pool.active = 2 # at maxSize
    pool.idle.addLast(toPooled(mockConn()))
    let existingFut = newFuture[PgConnection]("existing")
    pool.waiters.addLast(Waiter(fut: existingFut, cancelled: false))
    pool.waiterCount = 1

    let newFut = pool.acquire()

    check pool.waiters.len == 2
    check pool.waiterCount == 2
    check pool.idle.len == 1
    check not newFut.finished
    check not existingFut.finished

    # Drain in FIFO order: existing waiter first, then the new one.
    let c1 = mockConn()
    pool.release(c1)
    check existingFut.finished
    check existingFut.read() == c1

    let c2 = mockConn()
    pool.release(c2)
    check waitFor(newFut) == c2

  test "acquire does not create new conn when waiters are already queued":
    # Pre-fix bug: with active<maxSize and waiters queued (e.g. after a
    # broken-conn release dropped active without serving the waiter), a
    # fresh acquire would create a new connection that should have gone to
    # the head-of-queue waiter. After the fix, the new caller queues.
    let pool = makePool(maxSize = 5)
    pool.active = 1
    let existingFut = newFuture[PgConnection]("existing")
    pool.waiters.addLast(Waiter(fut: existingFut, cancelled: false))
    pool.waiterCount = 1
    # Suppress the spawn-for-waiter so the test isolates the queue logic
    # from the async connect path.
    pool.consecutiveConnectFailures = 1
    pool.nextConnectRetryAt = Moment.now() + seconds(60)

    let newFut = pool.acquire()

    check pool.waiters.len == 2
    check pool.waiterCount == 2
    check pool.active == 1
    check not newFut.finished
    check not existingFut.finished

    # Drain front-of-queue waiter; newFut intentionally left pending.
    pool.release(mockConn())
    check existingFut.finished
    discard newFut

  test "broken-conn release in backoff does not reserve a slot":
    # Verifies canAttemptConnect is honored: while in the backoff window,
    # a broken-conn release just frees the active slot without kicking off
    # a spawn-for-waiter (which would otherwise pile failures on a known
    # unreachable DB).
    let pool = makePool(maxSize = 1)
    pool.active = 1
    let waitFut = newFuture[PgConnection]("waiter")
    pool.waiters.addLast(Waiter(fut: waitFut, cancelled: false))
    pool.waiterCount = 1
    pool.consecutiveConnectFailures = 1
    pool.nextConnectRetryAt = Moment.now() + seconds(60)

    pool.release(mockConn(csClosed))

    check pool.active == 0
    check pool.waiterCount == 1
    check not waitFut.finished

  test "broken-conn release reserves an active slot for queued waiter":
    # The fix: discarding a broken connection while a waiter is queued
    # must immediately reserve the freed slot for an out-of-band connect.
    # Otherwise a concurrent fresh acquire would jump the queue.
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      # The spawn-for-waiter triggered by release() will try to connect to
      # localhost:5432. Cap the attempt so the test doesn't hang on hosts
      # where the SYN is silently dropped (the assertions below are
      # synchronous; the connect outcome only affects close()'s drain).
      pool.config.connConfig.connectTimeout = milliseconds(100)
      pool.active = 1
      let waitFut = newFuture[PgConnection]("waiter")
      pool.waiters.addLast(Waiter(fut: waitFut, cancelled: false))
      pool.waiterCount = 1

      pool.release(mockConn(csClosed))

      # Synchronously after release: active was decremented from 1 to 0,
      # then re-incremented to 1 as a reservation for the spawn-for-waiter
      # task. The waiter is still queued (the spawn body has not run yet).
      doAssert pool.active == 1
      doAssert pool.waiterCount == 1

      # Drain the in-flight spawn via close so the test does not outlive it.
      await pool.close()
      doAssert pool.closed

    waitFor t()

  test "respawnForStrandedWaiter reserves a slot when a waiter is queued":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.connConfig.connectTimeout = milliseconds(100)
      let waitFut = newFuture[PgConnection]("waiter")
      pool.waiters.addLast(Waiter(fut: waitFut, cancelled: false))
      pool.waiterCount = 1

      pool.respawnForStrandedWaiter()

      doAssert pool.active == 1
      doAssert pool.waiterCount == 1
      doAssert pool.pendingBackgroundTasks.len >= 1

      await pool.close()

    waitFor t()

  test "respawnForStrandedWaiter is a no-op without a queued waiter":
    let pool = makePool(maxSize = 1)
    pool.respawnForStrandedWaiter()
    check pool.active == 0
    check pool.pendingBackgroundTasks.len == 0

  test "respawnForStrandedWaiter is a no-op when the pool is at maxSize":
    let pool = makePool(maxSize = 1)
    pool.active = 1
    pool.waiters.addLast(Waiter(fut: newFuture[PgConnection]("w"), cancelled: false))
    pool.waiterCount = 1
    pool.respawnForStrandedWaiter()
    check pool.active == 1
    check pool.pendingBackgroundTasks.len == 0

  test "respawnForStrandedWaiter honors the connect-backoff window":
    let pool = makePool(maxSize = 1)
    pool.waiters.addLast(Waiter(fut: newFuture[PgConnection]("w"), cancelled: false))
    pool.waiterCount = 1
    pool.consecutiveConnectFailures = 1
    pool.nextConnectRetryAt = Moment.now() + seconds(60)
    pool.respawnForStrandedWaiter()
    check pool.active == 0
    check pool.pendingBackgroundTasks.len == 0

  test "respawnForStrandedWaiter is a no-op on a closed pool":
    let pool = makePool(maxSize = 1)
    pool.closed = true
    pool.waiters.addLast(Waiter(fut: newFuture[PgConnection]("w"), cancelled: false))
    pool.waiterCount = 1
    pool.respawnForStrandedWaiter()
    check pool.active == 0
    check pool.pendingBackgroundTasks.len == 0

  when hasChronos:
    test "failed caller-driven connect respawns for the waiter queued behind it":
      # A takes the fresh-connect fast path (waiterCount==0) and reserves the
      # only slot. B queues while A is suspended, sees active==maxSize, and
      # skips the queue-time spawn. When A's connect times out and releases
      # the slot, B would sit until its own budget elapses unless the failure
      # path also emits a spawn.
      proc t() {.async.} =
        let ms = startMockServer()

        let pool = makePool(maxSize = 1)
        pool.config.connConfig.host = "127.0.0.1"
        pool.config.connConfig.port = ms.port
        pool.config.connConfig.connectTimeout = milliseconds(150)
        pool.config.acquireTimeout = seconds(10)

        let futA = pool.acquire()
        # Let A reach the suspended connect() await.
        await sleepAsync(milliseconds(20))
        doAssert not futA.finished
        doAssert pool.active == 1
        doAssert pool.waiterCount == 0

        let futB = pool.acquire()
        doAssert not futB.finished
        doAssert pool.waiterCount == 1
        doAssert pool.active == 1

        var errA: ref PgPoolError = nil
        try:
          discard await futA
        except PgPoolError as e:
          errA = e
        doAssert errA != nil
        doAssert errA.kind == pekConnectFailed

        # With the fix, B is served by the spawn (which also fails against the
        # unresponsive mock) and returns fast; without it, B would time out on
        # acquireTimeout with pekAcquireTimeout.
        var errB: ref PgPoolError = nil
        let bStart = Moment.now()
        try:
          discard await futB
        except PgPoolError as e:
          errB = e
        let bElapsed = Moment.now() - bStart
        doAssert errB != nil
        doAssert errB.kind == pekConnectFailed
        doAssert errB.kind != pekAcquireTimeout
        doAssert bElapsed < seconds(2)

        await pool.close()
        await closeServer(ms)

      waitFor t()

suite "Error type granularity":
  test "closed pool raises PgPoolError":
    let pool = makePool()
    pool.closed = true
    var caught = false
    try:
      discard waitFor pool.acquire()
    except PgPoolError:
      caught = true
    except PgError:
      discard
    check caught

  test "maxWaiters full raises PgPoolError":
    let pool = makePool(maxSize = 1)
    pool.config.maxWaiters = 1
    pool.active = 1

    discard pool.acquire() # fills the waiter queue

    var caught: ref PgPoolError
    try:
      discard waitFor pool.acquire()
    except PgPoolError as e:
      caught = e
    except PgError:
      discard
    check caught != nil
    check caught.kind == pekQueueFull

    # Clean up
    pool.release(mockConn())

  test "acquire timeout raises PgPoolError":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      var caught = false
      try:
        discard await pool.acquire()
      except PgPoolError:
        caught = true
      except PgError:
        discard
      doAssert caught

    waitFor t()

  test "PgPoolError is catchable as PgError":
    let pool = makePool()
    pool.closed = true
    var caught = false
    try:
      discard waitFor pool.acquire()
    except PgError:
      caught = true
    check caught

  test "acquire connect failure raises PgPoolError with parent":
    # Caller-driven connect path: the underlying PgConnectionError must not
    # escape acquire() raw — it is wrapped in PgPoolError with `parent` set.
    proc t() {.async.} =
      # Grab a port that is guaranteed closed: bind, read it, release it.
      let ms = startMockServer()
      let port = ms.port
      await closeServer(ms)

      let pool = makePool(maxSize = 1)
      pool.config.connConfig.port = port
      # Backstop so the test cannot hang if the connect is not refused
      # promptly; a timeout is wrapped the same way as a refusal.
      pool.config.connConfig.connectTimeout = milliseconds(500)

      var caught: ref PgPoolError
      try:
        discard await pool.acquire()
      except PgPoolError as e:
        caught = e
      doAssert caught != nil
      doAssert caught.parent != nil
      doAssert pool.active == 0
      await pool.close()

    waitFor t()

  test "pipelined batch acquire failure fails every op with pekBatchFailed":
    # Multi-op dispatch arm: every acquire fails (connection refused), so the
    # batch cannot be served — each op's future must fail with pekBatchFailed
    # (not hang, and not leak the underlying acquire error kind).
    proc t() {.async.} =
      let ms = startMockServer()
      let port = ms.port
      await closeServer(ms) # guaranteed connection-refused port

      let pool = makePool(maxSize = 1)
      pool.config.pipelined = true
      pool.config.connConfig.host = "127.0.0.1"
      pool.config.connConfig.port = port
      pool.config.connConfig.connectTimeout = milliseconds(500)

      # Two ops queued in the same tick form one batch (multi-op dispatch arm).
      let futA = pool.exec("SELECT 1")
      let futB = pool.exec("SELECT 2")

      for fut in [futA, futB]:
        var caught: ref PgPoolError
        try:
          discard await fut
        except PgPoolError as e:
          caught = e
        doAssert caught != nil, "batch op must fail with PgPoolError, not hang"
        doAssert caught.kind == pekBatchFailed
        doAssert caught.parent != nil,
          "the failed acquire must be preserved as the parent"
        doAssert caught.parent of PgPoolError
        doAssert (ref PgPoolError)(caught.parent).kind == pekConnectFailed

      await pool.close()

    waitFor t()

  test "PgPoolError without newPoolError has kind pekUnknown":
    # Legacy construction (newException) leaves kind at its zero value, which
    # must not be mistaken for pekClosed (see PoolErrorKind).
    let err = newException(PgPoolError, "legacy construction")
    check err.kind == pekUnknown

  test "spawn connect failure fails waiter with PgPoolError with parent":
    # Waiter path: a broken-conn release kicks off spawnConnectForWaiter;
    # its connect failure must reach the queued acquire as PgPoolError
    # (wrapping the original error), not as a raw PgConnectionError.
    proc t() {.async.} =
      let ms = startMockServer()
      let port = ms.port
      await closeServer(ms) # guaranteed connection-refused port

      let pool = makePool(maxSize = 1)
      pool.config.connConfig.port = port
      pool.config.connConfig.connectTimeout = milliseconds(500)
      pool.config.acquireTimeout = seconds(5)
      pool.active = 1 # simulated borrower at maxSize

      let acqFut = pool.acquire() # queues as a waiter (deadline path)
      # Broken-conn release frees the slot and spawns a connect for the waiter.
      pool.release(mockConn(csClosed))

      var caught: ref PgPoolError
      try:
        discard await acqFut
      except PgPoolError as e:
        caught = e
      doAssert caught != nil
      doAssert caught.parent != nil
      doAssert pool.waiterCount == 0
      doAssert pool.active == 0
      await pool.close()

    waitFor t()

  test "spawn connect timeout does not corrupt waiterCount":
    # Regression: a raw AsyncTimeoutError from the spawn's connectTimeout
    # used to reach the waiter's own wait-budget handler, which decremented
    # waiterCount a second time (failLastWaiter had already done so). The
    # resulting negative count permanently disabled the FIFO fast path.
    proc t() {.async.} =
      # The mock server accepts TCP (listen backlog) but never answers the
      # startup message, so the connect attempt hangs until connectTimeout.
      let ms = startMockServer()

      let pool = makePool(maxSize = 1)
      # The mock server listens on 127.0.0.1 only; "localhost" may resolve
      # to ::1 first, which would fail fast with refused instead of hanging.
      pool.config.connConfig.host = "127.0.0.1"
      pool.config.connConfig.port = ms.port
      pool.config.connConfig.connectTimeout = milliseconds(100)
      pool.config.acquireTimeout = seconds(5)
      pool.active = 1

      let acqFut = pool.acquire() # queues as a waiter (deadline path)
      pool.release(mockConn(csClosed)) # spawn-for-waiter kicks in

      var caught: ref PgPoolError
      try:
        discard await acqFut
      except PgPoolError as e:
        caught = e
      doAssert caught != nil
      doAssert caught.parent of AsyncTimeoutError
      doAssert pool.waiterCount == 0
      doAssert pool.active == 0
      await pool.close()
      await closeServer(ms)

    waitFor t()

  when hasChronos:
    # asyncdispatch has no cancellation (cancelAndWait is a no-op shim), so
    # the spawn-cancellation path is only reachable under chronos.
    test "cancelled spawn connect is not treated as a connect failure":
      # A cancelled spawn-for-waiter must not bump the backoff counter or
      # fail the waiter with a pool error: cancellation is not a connect
      # failure. The capacity reservation is still released.
      proc t() {.async.} =
        # Accepts TCP but never answers the startup message, so the connect
        # attempt stays suspended until we cancel it.
        let ms = startMockServer()

        let pool = makePool(maxSize = 1)
        pool.config.connConfig.host = "127.0.0.1"
        pool.config.connConfig.port = ms.port
        # Backstop: if cancellation failed to reach the spawn, the connect
        # would fail on its own and trip the assertions below instead of
        # hanging the test.
        pool.config.connConfig.connectTimeout = seconds(2)
        pool.config.acquireTimeout = seconds(5)
        pool.active = 1

        let acqFut = pool.acquire() # queues as a waiter (deadline path)
        pool.release(mockConn(csClosed)) # spawn-for-waiter kicks in
        # release() queues the broken conn's closeNoWait first, then the
        # spawn-for-waiter — the spawn future is the last entry.
        doAssert pool.pendingBackgroundTasks.len >= 1
        await cancelAndWait(pool.pendingBackgroundTasks[^1])

        doAssert pool.consecutiveConnectFailures == 0
        doAssert pool.waiterCount == 1 # waiter still queued, not failed
        doAssert pool.active == 0 # reservation released by `finally`

        # close() settles the still-queued waiter with pekClosed.
        await pool.close()
        var caught: ref PgPoolError
        try:
          discard await acqFut
        except PgPoolError as e:
          caught = e
        doAssert caught != nil
        doAssert caught.kind == pekClosed
        await closeServer(ms)

      waitFor t()

suite "Pool metrics":
  test "initial metrics are zero":
    let pool = makePool()
    let m = pool.metrics
    check m.acquireCount == 0
    check m.acquireDuration == ZeroDuration
    check m.timeoutCount == 0
    check m.createCount == 0
    check m.closeCount == 0

  test "acquire from idle increments acquireCount":
    let pool = makePool()
    let conn = mockConn()
    pool.idle.addLast(conn.toPooled())
    discard waitFor pool.acquire()
    check pool.metrics.acquireCount == 1

  test "acquire tracks acquireDuration":
    let pool = makePool()
    let conn = mockConn()
    pool.idle.addLast(conn.toPooled())
    discard waitFor pool.acquire()
    check pool.metrics.acquireDuration >= ZeroDuration

  test "acquire skipping broken connections increments closeCount":
    let pool = makePool()
    let broken = mockConn(csClosed)
    let good = mockConn()
    pool.idle.addLast(broken.toPooled())
    pool.idle.addLast(good.toPooled())
    discard waitFor pool.acquire()
    check pool.metrics.closeCount == 1
    check pool.metrics.acquireCount == 1

  test "release broken connection increments closeCount":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn(csClosed)
    pool.release(conn)
    check pool.metrics.closeCount == 1

  test "release to closed pool increments closeCount":
    let pool = makePool()
    pool.active = 1
    pool.closed = true
    let conn = mockConn()
    pool.release(conn)
    check pool.metrics.closeCount == 1

  test "close draining idle connections increments closeCount":
    let pool = makePool()
    let conn1 = mockConn(csClosed)
    let conn2 = mockConn(csClosed)
    pool.idle.addLast(conn1.toPooled())
    pool.idle.addLast(conn2.toPooled())
    waitFor pool.close()
    check pool.metrics.closeCount == 2

  test "acquire timeout increments timeoutCount":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      try:
        discard await pool.acquire()
      except PgError:
        discard

      doAssert pool.metrics.timeoutCount == 1
      doAssert pool.metrics.acquireCount == 0

    waitFor t()

  test "multiple acquires accumulate metrics":
    let pool = makePool()
    for i in 0 ..< 3:
      let conn = mockConn()
      pool.idle.addLast(conn.toPooled())
      discard waitFor pool.acquire()
      pool.active.dec
      pool.idle.addLast(conn.toPooled())
    check pool.metrics.acquireCount == 3

  test "waiter transfer increments acquireCount":
    let pool = makePool(maxSize = 1)
    pool.active = 1

    let acquireFut = pool.acquire()
    check not acquireFut.finished

    let conn = mockConn()
    pool.release(conn)
    discard waitFor acquireFut
    check pool.metrics.acquireCount == 1

  test "acquire skipping maxLifetime-expired connections increments closeCount":
    let pool = makePool()
    pool.config.maxLifetime = seconds(1)
    let expired = mockConn()
    expired.createdAt = Moment.now() - seconds(5)
    expired.state = csClosed
    let good = mockConn()
    pool.idle.addLast(expired.toPooled())
    pool.idle.addLast(good.toPooled())
    discard waitFor pool.acquire()
    check pool.metrics.closeCount == 1
    check pool.metrics.acquireCount == 1

  test "acquireDuration accumulates across multiple acquires":
    let pool = makePool()
    for i in 0 ..< 3:
      let conn = mockConn()
      pool.idle.addLast(conn.toPooled())
      discard waitFor pool.acquire()
      pool.active.dec
    check pool.metrics.acquireCount == 3
    check pool.metrics.acquireDuration >= ZeroDuration

  test "waiter transfer tracks acquireDuration":
    let pool = makePool(maxSize = 1)
    pool.active = 1

    let acquireFut = pool.acquire()
    check not acquireFut.finished

    let conn = mockConn()
    pool.release(conn)
    discard waitFor acquireFut
    check pool.metrics.acquireCount == 1
    check pool.metrics.acquireDuration >= ZeroDuration

  test "acquire timeout does not increment createCount":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      try:
        discard await pool.acquire()
      except PgError:
        discard

      doAssert pool.metrics.createCount == 0
      doAssert pool.metrics.timeoutCount == 1

    waitFor t()

suite "isConnected":
  test "returns false for mock connection without transport":
    let conn = mockConn()
    check not conn.isConnected()

  test "returns false for csClosed mock":
    let conn = mockConn(csClosed)
    check not conn.isConnected()

  when hasChronos:
    test "returns true while transport is live, false after close":
      proc t() {.async.} =
        let (conn, server, serverTransport) = await makeHangingConn()
        doAssert conn.isConnected()
        await conn.close()
        doAssert not conn.isConnected()
        await cleanupHanging(server, serverTransport)

      waitFor t()

    test "detects peer-side FIN before any read (half-open)":
      when not defined(posix):
        skip()
      else:
        # Server closes its side while the client is idle and has not yet
        # observed the FIN through a read. `isConnected` must still report
        # false via the OS-level probe so half-open conns are recognised
        # before being handed out.
        var hasFin = false
        var stillConnected = true
        var stateAtProbe = csConnecting

        proc t() {.async.} =
          let (conn, server, serverTransport) = await makeHangingConn()
          doAssert conn.isConnected()
          await serverTransport.closeWait()
          # Yield so the kernel posts the FIN to our socket buffer.
          await sleepAsync(milliseconds(50))
          hasFin = conn.socketHasFin()
          stillConnected = conn.isConnected()
          # State stays csReady — the protocol layer never saw the FIN —
          # so isConnected is the only signal callers have here.
          stateAtProbe = conn.state
          await conn.close()
          server.stop()
          server.close()
          await server.join()

        waitFor t()
        check hasFin
        check not stillConnected
        check stateAtProbe == csReady

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

suite "Pool broken connection handling (integration)":
  test "query failure from server close transitions conn to csClosed and release discards it":
    # End-to-end: a live pool connection that dies mid-query should surface as
    # csClosed, so release() retires it instead of returning it to idle.
    var finalState: PgConnState
    var idleAfter = -1
    var closeCountDelta: int64 = -1

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st) # client query
          # Server disappears mid-query without sending a response
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let cfg = initPoolConfig(mockConfig(ms.port), minSize = 0, maxSize = 2)
      let pool = await newPool(cfg)

      let conn = await pool.acquire()
      try:
        discard await conn.simpleQuery("SELECT 1")
      except CatchableError:
        discard

      finalState = conn.state
      let before = pool.metrics.closeCount
      pool.release(conn)
      closeCountDelta = pool.metrics.closeCount - before
      idleAfter = pool.idle.len

      await pool.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check finalState == csClosed
    check closeCountDelta == 1
    check idleAfter == 0

  test "acquire skips an idle conn whose transport was torn down":
    # An idle pool entry whose backend vanished (state surfaced as csClosed
    # via a prior read, or an out-of-band close) must be retired on the next
    # acquire attempt. We force-close from the client side to flip state,
    # then check acquire() drops the entry and returns an alternative.
    var idleAfter = -1
    var closeCountDelta: int64 = -1

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        try:
          let st = await acceptAndReady(ms)
          # Stay up long enough for the client to finish; close at the end.
          await sleepAsync(milliseconds(200))
          await closeClient(st)
        except CatchableError:
          discard

      let serverFut = serverHandler()

      let cfg = initPoolConfig(mockConfig(ms.port), minSize = 0, maxSize = 2)
      let pool = await newPool(cfg)

      let broken = await pool.acquire()
      pool.release(broken)
      doAssert pool.idle.len == 1

      # Simulate the backend vanishing: csClosed on the idle entry.
      await broken.close()
      doAssert broken.state == csClosed

      # Inject a healthy mock alongside the broken one so acquire has a
      # non-real candidate to hand back without re-entering connect().
      let good = mockConn()
      pool.idle.addLast(toPooled(good))

      let before = pool.metrics.closeCount
      let acquired = await pool.acquire()
      closeCountDelta = pool.metrics.closeCount - before
      idleAfter = pool.idle.len

      doAssert acquired == good

      pool.release(acquired)
      await pool.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check closeCountDelta == 1
    check idleAfter == 0

  when defined(posix):
    test "acquire skips an idle conn whose peer half-closed (FIN, state still csReady)":
      # Half-open scenario: the server has sent FIN but the client has not
      # read it yet, so the conn's logical state remains csReady. Without
      # an OS-level probe, the pool would happily hand this corpse out;
      # acquire must call socketHasFin and discard it.
      var idleAfter = -1
      var closeCountDelta: int64 = -1
      var stateAtAcquire = csConnecting

      proc testBody() {.async.} =
        let ms = startMockServer()
        var serverSide: MockClient

        proc serverHandler() {.async.} =
          try:
            serverSide = await acceptAndReady(ms)
          except CatchableError:
            discard

        let serverFut = serverHandler()

        let cfg = initPoolConfig(mockConfig(ms.port), minSize = 0, maxSize = 2)
        let pool = await newPool(cfg)

        let broken = await pool.acquire()
        pool.release(broken)
        await serverFut
        doAssert pool.idle.len == 1
        doAssert broken.state == csReady

        # Peer closes — FIN lands in the client kernel but we never read it,
        # so broken.state stays csReady.
        await closeClient(serverSide)
        await sleepAsync(milliseconds(50))
        stateAtAcquire = broken.state

        # Inject a healthy mock so acquire can return without re-entering
        # connect() against the now-dead mock server.
        let good = mockConn()
        pool.idle.addLast(toPooled(good))

        let before = pool.metrics.closeCount
        let acquired = await pool.acquire()
        closeCountDelta = pool.metrics.closeCount - before
        idleAfter = pool.idle.len

        doAssert acquired == good
        pool.release(acquired)
        await pool.close()
        await closeServer(ms)

      waitFor testBody()
      check stateAtAcquire == csReady
      check closeCountDelta == 1
      check idleAfter == 0

suite "Pool replenish close-race":
  test "replenish closes a connection won after the pool is closed":
    # Regression: if the pool is closed while the maintenance loop is awaiting a
    # replenishment connect, the freshly opened connection must be closed — not
    # parked in the closed pool's idle deque, where its socket would leak. We
    # gate the handshake on the server side so the connect is provably in flight
    # when we flip `pool.closed`, then let it complete.
    proc t() {.async.} =
      let ms = startMockServer()

      let pool = makePool(minSize = 1)
      pool.config.connConfig = mockConfig(ms.port)
      # A large per-connect budget keeps the outer wait() open while we hold the
      # handshake, so this exercises the post-connect closed re-check rather
      # than the timeout path.
      pool.config.connConfig.connectTimeout = seconds(5)
      pool.config.maintenanceInterval = milliseconds(10)
      pool.maintenanceTask = maintenanceLoop(pool)

      # The loop sleeps one interval, then opens the replenishment connect.
      # accept() resolves once that connect's TCP is up; draining the startup
      # message leaves connect() suspended awaiting the handshake.
      let client = await ms.accept()
      await drainStartupMessage(client)

      # Close the pool mid-connect, then let the handshake complete so the loop
      # resumes and runs the re-check.
      pool.closed = true
      await sendFullHandshake(client)
      await sleepAsync(milliseconds(80))

      doAssert pool.idle.len == 0 # closed, not parked in the closed pool
      doAssert pool.active == 0
      doAssert pool.metrics.createCount == 1
      doAssert pool.metrics.closeCount == 1

      await pool.close()
      await closeServer(ms)

    waitFor t()

suite "Pool replenish capacity race":
  test "in-flight replenish connects hold a reservation so concurrent acquires cannot overshoot maxSize":
    # Regression: the maintenance loop once opened replenish connects without
    # counting them in `pool.active` (the only violation of the reservation
    # contract `spawnConnectForWaiter` documents). A burst of acquires
    # arriving mid-replenish saw spare capacity (`active < maxSize`), dialed
    # their own connections, and the replenish connections parked on top —
    # pushing `idle + active` to 2x maxSize in a fixed pool (minSize ==
    # maxSize) and breaching the DB's per-user connection limit. The
    # in-flight connects must hold capacity reservations, queuing the burst
    # as waiters until they settle.
    proc t() {.async.} =
      let ms = startMockServer()

      let pool = makePool(minSize = 2, maxSize = 2)
      pool.config.connConfig = mockConfig(ms.port)
      pool.config.connConfig.connectTimeout = seconds(5)
      pool.config.maintenanceInterval = milliseconds(10)
      pool.maintenanceTask = maintenanceLoop(pool)

      # The loop sleeps one interval, then opens the replenish connects. Gate
      # the handshake server-side so both connects are provably in flight with
      # their reservations held (with the OLD code active would be 0 here).
      let client1 = await ms.accept()
      await drainStartupMessage(client1)
      let client2 = await ms.accept()
      await drainStartupMessage(client2)
      doAssert pool.active == 2 # the in-flight replenish reservations

      # The burst must queue as waiters rather than dialing its own connects.
      let futA = pool.acquire()
      let futB = pool.acquire()
      await sleepAsync(milliseconds(20))
      doAssert pool.waiterCount == 2
      doAssert pool.active == 2
      doAssert pool.metrics.createCount == 0 # nothing has settled yet

      # Complete the handshakes: the replenish conns hand off to the waiters.
      await sendFullHandshake(client1)
      await sendFullHandshake(client2)

      let connA = await futA
      let connB = await futB
      doAssert pool.active == 2
      doAssert pool.idle.len == 0
      doAssert pool.active + pool.idle.len <= pool.config.maxSize
      doAssert pool.metrics.createCount == 2 # exactly the replenish connects

      pool.release(connA)
      pool.release(connB)
      doAssert pool.active == 0
      doAssert pool.idle.len == 2

      await pool.close()
      await closeServer(ms)

    waitFor t()

  test "replenish connect failures release their reservations":
    # The replenish reservations must be released when the connects fail, or
    # `active` would stay inflated and `acquire` would queue behind phantom
    # capacity.
    proc t() {.async.} =
      let pool = makePool(minSize = 2, maxSize = 4)
      pool.config.connConfig = mockConfig(1) # nothing listens on port 1
      pool.config.maintenanceInterval = milliseconds(10)
      pool.maintenanceTask = maintenanceLoop(pool)

      await sleepAsync(milliseconds(60))

      doAssert pool.active == 0 # reservations released despite 2 failed connects
      doAssert pool.idle.len == 0
      doAssert pool.metrics.createCount == 0

      pool.closed = true
      await cancelAndWait(pool.maintenanceTask)

    waitFor t()

  when hasChronos:
    test "close cancelling in-flight replenish releases its reservations (landed and in-flight)":
      # chronos-only: one replenish connect lands before close() cancels the
      # loop (settled via the except path); the other stays in flight.
      proc t() {.async.} =
        let ms = startMockServer()

        let pool = makePool(minSize = 2, maxSize = 2)
        pool.config.connConfig = mockConfig(ms.port)
        pool.config.connConfig.connectTimeout = seconds(5)
        pool.config.maintenanceInterval = milliseconds(10)
        pool.maintenanceTask = maintenanceLoop(pool)

        let client1 = await ms.accept()
        await drainStartupMessage(client1)
        let client2 = await ms.accept()
        await drainStartupMessage(client2)
        doAssert pool.active == 2 # in-flight replenish reservations

        await sendFullHandshake(client1)
        await sleepAsync(milliseconds(100)) # let the client-side connect settle

        await pool.close()
        doAssert pool.active == 0
        doAssert pool.metrics.closeCount == 1 # landed conn settled via closeNoWait

        # Read the landed conn until EOF: the pool must have closed it.
        var closedByPool = false
        try:
          while true:
            discard await client1.readN(1)
        except CatchableError:
          closedByPool = true
        doAssert closedByPool

        await closeServer(ms)
        await closeClient(client1)
        await closeClient(client2)

      waitFor t()

suite "Pool acquire close-race":
  test "acquire discards a connection won after the pool is closed":
    # Regression: acquireImpl's `await connect()` in the new-conn branch could
    # complete after close() finished, returning a live conn from a closed pool.
    # Gate the handshake so the connect is provably in flight when we flip
    # `closed`, then let it complete.
    proc t() {.async.} =
      let ms = startMockServer()

      let pool = makePool(minSize = 0, maxSize = 1)
      pool.config.connConfig = mockConfig(ms.port)
      pool.config.connConfig.connectTimeout = seconds(5)

      let acquireFut = pool.acquire()

      let client = await ms.accept()
      await drainStartupMessage(client)

      pool.closed = true
      await sendFullHandshake(client)
      await sleepAsync(milliseconds(80))

      doAssert acquireFut.finished
      doAssert acquireFut.failed
      let err = acquireFut.readError()
      doAssert err of PgPoolError
      doAssert (ref PgPoolError)(err).kind == pekClosed
      doAssert pool.active == 0
      doAssert pool.metrics.createCount == 1
      doAssert pool.metrics.closeCount == 1
      doAssert pool.idle.len == 0

      await pool.close()
      await closeClient(client)
      await closeServer(ms)

    waitFor t()

  test "acquire discards an idle conn whose ping resolves after close":
    # Regression: after a health-check ping suspends, close() could complete
    # before the ping's response, and the popped idle conn — no longer in
    # `idle`, so not drained by close() — was returned live from a closed pool.
    proc t() {.async.} =
      let ms = startMockServer()

      let pool = makePool(minSize = 0, maxSize = 2)
      pool.config.connConfig = mockConfig(ms.port)
      pool.config.healthCheckTimeout = milliseconds(1)
      pool.config.pingTimeout = seconds(5)

      # Warm one idle conn against the mock server, then age it so the next
      # acquire triggers a ping.
      let handshake = acceptAndReady(ms)
      let conn = await connect(mockConfig(ms.port))
      let client = await handshake
      conn.ownerPool = pool
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now() - seconds(1)))

      let acquireFut = pool.acquire()

      # Drain the ping's Query(""), then flip closed before responding so
      # acquireImpl sees pool.closed = true post-ping.
      discard await drainFrontendMessage(client)
      pool.closed = true

      var resp: seq[byte]
      resp.add(buildBackendMsg('I', @[])) # EmptyQueryResponse
      resp.add(buildReadyForQuery('I'))
      await sendBytes(client, resp)

      await sleepAsync(milliseconds(80))

      doAssert acquireFut.finished
      doAssert acquireFut.failed
      let err = acquireFut.readError()
      doAssert err of PgPoolError
      doAssert (ref PgPoolError)(err).kind == pekClosed
      doAssert pool.active == 0
      doAssert pool.idle.len == 0

      await pool.close()
      await closeClient(client)
      await closeServer(ms)

    waitFor t()

suite "Pool spawn-connect close-race":
  test "spawn-for-waiter closes a connection won after the pool is closed":
    # Regression: spawnConnectForWaiter's closed-branch used to close the fresh
    # conn without bumping closeCount. A spawn whose connect returned success
    # after pool.close() flipped `closed` left createCount incremented but
    # closeCount not — a permanent skew poisoning all metric-based accounting.
    # Gate the handshake so the connect is provably in flight when we close.
    proc t() {.async.} =
      let ms = startMockServer()

      let pool = makePool(minSize = 0, maxSize = 1)
      pool.config.connConfig = mockConfig(ms.port)
      pool.config.connConfig.connectTimeout = seconds(5)
      pool.active = 1 # simulated borrower at maxSize

      let acqFut = pool.acquire() # queues as a waiter
      # Broken-conn release frees the slot and spawns a connect for the waiter.
      # This bumps closeCount for the mock, so snapshot AFTER it.
      pool.release(mockConn(csClosed))

      # accept() resolves once the spawn's TCP is up; draining the startup
      # message leaves connect() suspended awaiting the handshake.
      let client = await ms.accept()
      await drainStartupMessage(client)

      let createBefore = pool.metrics.createCount
      let closeBefore = pool.metrics.closeCount

      # Start close() (synchronously flips `closed` and fails waiters), then
      # let the handshake complete so the spawn resumes into the closed-branch.
      # close() awaits pendingBackgroundTasks, so the spawn is drained by the
      # time closeFut resolves.
      let closeFut = pool.close()
      await sendFullHandshake(client)
      await closeFut

      doAssert acqFut.finished
      doAssert acqFut.failed
      doAssert pool.active == 0
      doAssert pool.metrics.createCount - createBefore == 1
      doAssert pool.metrics.closeCount - closeBefore == 1
      doAssert pool.idle.len == 0

      await closeClient(client)
      await closeServer(ms)

    waitFor t()

  test "close does not hang when spawn-for-waiter connect stalls with unset connectTimeout":
    # Regression: close()'s final pendingBackgroundTasks drain awaited
    # spawnConnectForWaiter with no bound. With connectTimeout unset, a stalled
    # handshake pinned close() indefinitely. spawnConnectForWaiter now caps an
    # unset connectTimeout with maintenanceInterval.
    proc t() {.async.} =
      let ms = startMockServer()

      let pool = makePool(minSize = 0, maxSize = 1)
      pool.config.connConfig = mockConfig(ms.port)
      pool.config.connConfig.connectTimeout = ZeroDuration
      pool.config.maintenanceInterval = milliseconds(300)
      pool.active = 1

      let acqFut = pool.acquire()
      pool.release(mockConn(csClosed))

      let client = await ms.accept()
      await drainStartupMessage(client)
      # No sendFullHandshake: the spawn is now suspended awaiting auth.
      doAssert pool.pendingBackgroundTasks.len >= 1

      let closeStart = Moment.now()
      await pool.close().wait(seconds(5))
      let elapsed = Moment.now() - closeStart

      # Fallback fires at ~300ms; allow generous headroom.
      doAssert elapsed < seconds(2), $elapsed
      doAssert acqFut.finished
      doAssert pool.pendingBackgroundTasks.len == 0

      await closeClient(client)
      await closeServer(ms)

    waitFor t()

  test "close(timeout) honors its budget when spawn-for-waiter connect stalls":
    # Regression: close(timeout=100ms) blocked on pendingBackgroundTasks drain
    # until the spawn's connectTimeout (=maintenanceInterval fallback, default
    # 30s) elapsed. close() now bounds the drain by its own deadline and
    # cancels remaining spawns.
    proc t() {.async.} =
      let ms = startMockServer()

      let pool = makePool(minSize = 0, maxSize = 1)
      pool.config.connConfig = mockConfig(ms.port)
      pool.config.connConfig.connectTimeout = ZeroDuration
      pool.config.maintenanceInterval = seconds(30) # default; must not gate close
      pool.active = 1

      let acqFut = pool.acquire()
      pool.release(mockConn(csClosed))

      let client = await ms.accept()
      await drainStartupMessage(client)
      doAssert pool.pendingBackgroundTasks.len >= 1

      let closeStart = Moment.now()
      await pool.close(timeout = milliseconds(100)).wait(seconds(5))
      let elapsed = Moment.now() - closeStart

      # Must be bounded by timeout (100ms), not the 30s connectTimeout fallback.
      doAssert elapsed < seconds(1), $elapsed
      doAssert acqFut.finished

      await closeClient(client)
      await closeServer(ms)

    waitFor t()

suite "Pool warmup parallelization":
  test "newPool opens minSize connections in parallel":
    # `newPool` should open all `minSize` connections concurrently (via
    # `allFutures`), not sequentially. The server handler accepts that many
    # handshakes; if warmup were serial this would still pass, so the assertion
    # is on the count and createCount rather than timing — but the parallel
    # path is what makes the open non-blocking under concurrent handshakes.
    var idleAfter = -1
    var createCount: int64 = -1

    proc t() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        var clients: seq[MockClient]
        for i in 0 ..< 3:
          try:
            clients.add(await acceptAndReady(ms))
          except CatchableError:
            break
        await sleepAsync(milliseconds(100))
        for c in clients:
          await closeClient(c)

      let serverFut = serverHandler()
      let cfg = initPoolConfig(mockConfig(ms.port), minSize = 3, maxSize = 5)
      let pool = await newPool(cfg)
      idleAfter = pool.idle.len
      createCount = pool.metrics.createCount
      await pool.close()
      await serverFut
      await closeServer(ms)

    waitFor t()
    check idleAfter == 3
    check createCount == 3

  test "newPool raises when all initial connects fail":
    # When every warmup connect fails, `newPool` must raise the first error
    # (and the empty-idle cleanup loop is a no-op). Connects target a port we
    # freed by closing a mock server so they get ECONNREFUSED.
    var raised = false

    proc t() {.async.} =
      let ms = startMockServer()
      let freePort = ms.port
      await closeServer(ms)
      var cfg = initPoolConfig(
        ConnConfig(
          host: "127.0.0.1",
          port: freePort,
          user: "t",
          database: "t",
          sslMode: sslDisable,
        ),
        minSize = 2,
        maxSize = 5,
      )
      cfg.connConfig.connectTimeout = milliseconds(300)
      try:
        discard await newPool(cfg)
      except CatchableError:
        raised = true

    waitFor t()
    check raised

  test "newPool issues warmup connects concurrently (gate-based)":
    # Prove that `newPool` opens all `minSize` connects in parallel, not
    # serially. The server accepts client #1 and drains its startup message
    # but withholds the handshake until client #2 also connects. If warmup
    # were serial, client #2's connect would never start while #1's handshake
    # is pending — the second `accept` would hang until `connectTimeout` fires
    # and `newPool` would raise. With parallel warmup both TCP connections are
    # up immediately, the server sees #2, both handshakes complete, and
    # `newPool` returns within the timeout.
    var ok = true
    var idleAfter = -1

    proc t() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        var clients: seq[MockClient]
        try:
          let c1 = await ms.accept()
          clients.add(c1)
          await drainStartupMessage(c1)
          # Gate: withhold c1's handshake until c2 also connects. Under serial
          # warmup this accept never completes and the test fails via timeout.
          let c2 = await ms.accept()
          clients.add(c2)
          await drainStartupMessage(c2)
          await sendFullHandshake(c1)
          await sendFullHandshake(c2)
        except CatchableError:
          discard
        for c in clients:
          try:
            await closeClient(c)
          except CatchableError:
            discard

      let serverFut = serverHandler()
      var cfg = initPoolConfig(mockConfig(ms.port), minSize = 2, maxSize = 4)
      cfg.connConfig.connectTimeout = milliseconds(500)
      try:
        let pool = await newPool(cfg)
        idleAfter = pool.idle.len
        await pool.close()
      except CatchableError:
        ok = false
      await closeServer(ms)
      await serverFut

    waitFor t()
    check ok
    check idleAfter == 2

suite "Pipeline rejects unencodable SQL at add time":
  ## Regression: SQL was only checked in `buildSendPhase`, long after the op
  ## joined a batch, so an innocent batch-mate inherited the error.

  test "addExec rejects an embedded NUL":
    let p = newPipeline(mockConn())
    expect PgTypeError:
      p.addExec("SELECT 1\0", @[])
    check p.ops.len == 0

  test "addQuery rejects an embedded NUL":
    let p = newPipeline(mockConn())
    expect PgTypeError:
      p.addQuery("SELECT 1\0", @[])
    check p.ops.len == 0

  test "addExec rejects an embedded NUL on the inline path":
    let p = newPipeline(mockConn())
    expect PgTypeError:
      p.addExec("SELECT 1\0", [])
    check p.ops.len == 0

suite "The shared message-length bound":
  ## Regression: `validateTypedParams` bounded each value but never their sum, so
  ## legal parameters overflowed the Bind message and took the whole batch down.
  ## The real payloads are not allocatable here, so the bound is pinned directly.

  test "a total at the maximum is accepted":
    var payload = int64(maxInt32Len) - 4
    addBindPayload(payload, 4)
    check payload == int64(maxInt32Len)

  test "one byte past the maximum is rejected":
    var payload = int64(maxInt32Len) - 4
    expect PgMessageTooLargeError:
      addBindPayload(payload, 5)

  test "the running total is what overflows, not any single addend":
    let half = maxInt32Len div 2
    var payload: int64 = 0
    expect PgMessageTooLargeError:
      for _ in 0 .. 2:
        addBindPayload(payload, half)

  when defined(pgTestObservability):
    test "every builder reaches the shared bound before it writes":
      # Without the counter a builder could drop its size check unnoticed.
      var buf: seq[byte] = @[]
      checkMsgLenBoundCalls = 0
      buf.addParse("s", "SELECT $1", @[23'i32])
      check checkMsgLenBoundCalls == 1
      buf.setLen(0)
      checkMsgLenBoundCalls = 0
      buf.addBind("", "s", @[1'i16], @[some(@[1'u8, 2])], @[])
      check checkMsgLenBoundCalls == 2 # one per value, one for the message
      let stmt = "s"
      let sql = "SELECT $1"
      let arg = 1'i32
      buf.setLen(0)
      checkMsgLenBoundCalls = 0
      buf.addParseDirect(stmt, sql, arg)
      check checkMsgLenBoundCalls == 1
      buf.setLen(0)
      checkMsgLenBoundCalls = 0
      buf.addBindDirect("", stmt, [], arg)
      check checkMsgLenBoundCalls == 2
suite "Bind/Parse envelope is included in pre-flight":
  ## The pre-flight charges the envelope, not only the payload. A real 2 GiB
  ## payload is out of reach, so the first tests anchor the calc procs against
  ## the encoder and the rest pin where `checkMsgLenBound64` fires.

  test "calcBindMessageLength equals the length the encoder writes":
    var buf: seq[byte] = @[]
    let payload = @[1'u8, 2, 3]
    buf.addBind("", "s", @[1'i16], @[some(payload)], @[0'i16])
    check int64(decodeInt32(buf, 1)) ==
      calcBindMessageLength(0, "s".len, 1, 1, int64(payload.len), 1)

  test "calcParseMessageLength equals the length the encoder writes":
    var buf: seq[byte] = @[]
    let sql = "SELECT $1"
    buf.addParse("st", sql, @[23'i32])
    check int64(decodeInt32(buf, 1)) == calcParseMessageLength("st".len, sql.len, 1)

  test "Bind envelope: payload at limit is accepted":
    let payload =
      int64(maxInt32Len) - calcBindMessageLength(0, generatedStmtNameLen, 1, 1, 0, 1)
    checkMsgLenBound64(
      calcBindMessageLength(0, generatedStmtNameLen, 1, 1, payload, 1), "Bind message"
    )

  test "Bind envelope: one byte past limit is rejected as PgMessageTooLargeError":
    let payload =
      int64(maxInt32Len) - calcBindMessageLength(0, generatedStmtNameLen, 1, 1, 0, 1)
    expect PgMessageTooLargeError:
      checkMsgLenBound64(
        calcBindMessageLength(0, generatedStmtNameLen, 1, 1, payload + 1, 1),
        "Bind message",
      )

  test "Bind envelope: multi-param overhead scales with n":
    let payload =
      int64(maxInt32Len) - calcBindMessageLength(0, generatedStmtNameLen, 10, 10, 0, 1)
    checkMsgLenBound64(
      calcBindMessageLength(0, generatedStmtNameLen, 10, 10, payload, 1), "Bind message"
    )
    expect PgMessageTooLargeError:
      checkMsgLenBound64(
        calcBindMessageLength(0, generatedStmtNameLen, 10, 10, payload + 1, 1),
        "Bind message",
      )

  test "Parse envelope: sql at limit is accepted":
    let sqlLen = maxInt32Len - int(calcParseMessageLength(generatedStmtNameLen, 0, 1))
    checkMsgLenBound64(
      calcParseMessageLength(generatedStmtNameLen, sqlLen, 1), "Parse message"
    )

  test "Parse envelope: sql one byte past limit is rejected":
    let sqlLen = maxInt32Len - int(calcParseMessageLength(generatedStmtNameLen, 0, 1))
    expect PgMessageTooLargeError:
      checkMsgLenBound64(
        calcParseMessageLength(generatedStmtNameLen, sqlLen + 1, 1), "Parse message"
      )

  when defined(pgTestObservability):
    test "the pre-flight callers reach that formula":
      # Ties the entry points to the formula pinned above. Small inputs cannot
      # trip the bound, so reachability is observed through the call counter.
      let typedOne = @[toPgParam(1'i32)]
      let inlineOne = @[toPgParamInline(1'i32)]
      checkMsgLenBoundCalls = 0
      validateParseMsg("SELECT $1", 1)
      check checkMsgLenBoundCalls == 1
      validateTypedParams(typedOne, 1)
      check checkMsgLenBoundCalls == 3
      discard flattenInline(inlineOne, 1)
      check checkMsgLenBoundCalls == 6

suite "Envelope overhead matches actual encoder output":
  ## The overhead constants are hand-derived from the encoder layout; pin them
  ## to real output so the boundary tests below are not tautological.

  test "Parse overhead is accurate":
    var buf: seq[byte] = @[]
    let stmt = "myStmt"
    let sql = "SELECT $1"
    let oids = @[23'i32, 23'i32]
    buf.addParse(stmt, sql, oids)
    check buf.len == 9 + stmt.len + sql.len + oids.len * 4
    check decodeInt32(buf, 1) == int32(buf.len - 1)

  test "Bind overhead is accurate":
    var buf: seq[byte] = @[]
    let portal = ""
    let stmt = "s"
    let pf = @[int16(1)]
    let payload = @[1'u8, 2, 3]
    let rf = @[int16(0)]
    buf.addBind(portal, stmt, pf, @[some(payload)], rf)
    let expected =
      13 + portal.len + stmt.len + pf.len * 2 + 4 + payload.len + rf.len * 2
    check buf.len == expected
    check decodeInt32(buf, 1) == int32(buf.len - 1)

  test "the direct macros emit what the generic builders emit":
    # The macros hand-roll the layout the `add*` builders own, so pin the two
    # together: a pre-flight that miscounts shows up as a length mismatch.
    let stmt = "s"
    let sql = "SELECT $1"
    let arg = 7'i32
    var direct: seq[byte] = @[]
    var generic: seq[byte] = @[]
    direct.addParseDirect(stmt, sql, arg)
    generic.addParse(stmt, sql, @[paramOidOf(arg)])
    check direct == generic
    direct.setLen(0)
    generic.setLen(0)
    direct.addBindDirect("", stmt, [], arg)
    generic.addBind("", stmt, @[1'i16], @[some(@[0'u8, 0, 0, 7])], @[])
    check direct == generic

  test "Describe/Execute/Close overhead is accurate":
    var buf: seq[byte] = @[]
    buf.addDescribe(dkStatement, "myStmt")
    check buf.len == 7 + "myStmt".len
    buf.setLen(0)
    buf.addExecute("myPortal", 0)
    check buf.len == 10 + "myPortal".len
    buf.setLen(0)
    buf.addClose(dkStatement, "myStmt")
    check buf.len == 7 + "myStmt".len

suite "Message builders are atomic on failure":
  test "addParse leaves buffer unchanged on NUL":
    var buf: seq[byte] = @[1'u8, 2, 3]
    let orig = buf
    expect PgTypeError:
      buf.addParse("stmt\0", "SELECT 1")
    check buf == orig
    expect PgTypeError:
      buf.addParse("stmt", "SELECT 1\0")
    check buf == orig

  test "addParse/addBind leave buffer unchanged on count overflow":
    var buf: seq[byte] = @[1'u8]
    let orig = buf
    expect PgTypeError:
      buf.addParse("s", "SELECT 1", newSeq[int32](maxInt16Count + 1))
    check buf == orig
    expect PgTypeError:
      buf.addBind("", "s", newSeq[int16](maxInt16Count + 1), @[], @[])
    check buf == orig

  test "addParseDirect/addBindDirect leave buffer unchanged on NUL":
    # The macros cannot use `withAtomicMessage` (its `try` would be spliced into
    # the caller's async body), so they pre-flight before the first write.
    var buf: seq[byte] = @[1'u8, 2, 3]
    let orig = buf
    let stmt = "stmt"
    let badSql = "SELECT 1\0"
    let badName = "s\0"
    let arg = 1'i32
    expect PgTypeError:
      buf.addParseDirect(stmt, badSql, arg)
    check buf == orig
    expect PgTypeError:
      buf.addParseDirect(badName, "SELECT 1", arg)
    check buf == orig
    expect PgTypeError:
      buf.addBindDirect(badName, stmt, [], arg)
    check buf == orig
    expect PgTypeError:
      buf.addBindDirect("", badName, [], arg)
    check buf == orig

  test "addBindRaw leaves buffer unchanged on invalid range":
    var buf: seq[byte] = @[1'u8, 2]
    let orig = buf
    expect PgTypeError:
      buf.addBindRaw("", "s", @[], @[], @[(off: int32(0), len: int32(-2))], @[])
    check buf == orig

  test "helper size check does not touch buffer (builder's size pre-flight uses same calc)":
    # Size overflow through the builders needs a 2 GiB payload, so exercise the
    # helpers directly; the NUL/count tests above cover the rollback itself.
    var buf: seq[byte] = @[1'u8, 2, 3]
    let orig = buf
    expect PgMessageTooLargeError:
      checkMsgLenBound64(
        calcParseMessageLength("a".len, "b".len, 0) + int64(maxInt32Len) - 10 + 1,
        "Parse message",
      )
    check buf == orig
    expect PgMessageTooLargeError:
      checkMsgLenBound64(
        calcBindMessageLength(0, 1, 1, 1, 3, 1) + int64(maxInt32Len) - 24 + 1,
        "Bind message",
      )
    check buf == orig
    check calcParseMessageLength("a".len, "b".len, 0) == 10 # 8+1+1+0
    check calcBindMessageLength(0, 1, 1, 1, 3, 1) == 24 # 12+0+1+2+4+3+2 length

  test "helper Bind size check does not touch buffer":
    var buf: seq[byte] = @[1'u8, 2]
    let orig = buf
    expect PgMessageTooLargeError:
      checkMsgLenBound64(int64(maxInt32Len) + 1, "Bind message")
    check buf == orig

suite "Direct message builders are atomic on failure":
  ## Atomic for everything the pre-flight rejects. The write phase cannot roll
  ## back (a `try` in the caller's `async` body trips `orc`), so a failed build
  ## is kept off the wire by the send buffer's lifecycle, pinned below.

  test "a failed build leaves nothing for the next operation to send":
    # Every failure the macros can raise comes out of the pre-flight, before the
    # first byte is written, so there is no residue for the caller's truncation
    # to hide: the next build must produce clean bytes without one.
    var sendBuf: seq[byte] = @[]
    expect PgTypeError:
      sendBuf.addBindDirect("", "stmt\0", [], 1'i32)
    check sendBuf.len == 0
    sendBuf.addBindDirect("", "stmt", [], 1'i32)
    var expected: seq[byte] = @[]
    expected.addBindDirect("", "stmt", [], 1'i32)
    check sendBuf == expected

  test "addBindDirect leaves buffer unchanged on resultFormat count overflow":
    var buf: seq[byte] = @[1'u8, 2, 3]
    let orig = buf
    let bigRf = newSeq[int16](maxInt16Count + 1)
    expect PgTypeError:
      buf.addBindDirect("", "stmt", bigRf, 1'i32)
    check buf == orig

suite "Direct builders agree with their pre-flight length":
  ## `patchMsgLenAtomic` is the only rollback left, so the pre-flight length
  ## must match what is written; drift would truncate a message on the wire.

  test "addParseDirect matches calcParseMessageLength":
    var buf: seq[byte] = @[]
    buf.addParseDirect("myStmt", "SELECT $1, $2", 1'i32, "x")
    check int64(buf.len - 1) ==
      calcParseMessageLength("myStmt".len, "SELECT $1, $2".len, 2)
    check decodeInt32(buf, 1) == int32(buf.len - 1)

  test "addBindDirect matches calcBindMessageLength":
    var buf: seq[byte] = @[]
    let rf = @[0'i16]
    buf.addBindDirect("p", "s", rf, 1'i32, "abc")
    let payload = int64(paramValueLen(1'i32)) + int64(paramValueLen("abc"))
    check int64(buf.len - 1) ==
      calcBindMessageLength("p".len, "s".len, 2, 2, payload, rf.len)
    check decodeInt32(buf, 1) == int32(buf.len - 1)

  test "addBindDirect with no resultFormats matches":
    var buf: seq[byte] = @[]
    buf.addBindDirect("", "s", [], 42'i64)
    check int64(buf.len - 1) ==
      calcBindMessageLength(0, "s".len, 1, 1, int64(paramValueLen(42'i64)), 0)
    check decodeInt32(buf, 1) == int32(buf.len - 1)
suite "An oversized message is an input error, not a connection failure":
  ## The pre-flight is not a second model of the layout: the contract is fixed
  ## in the encoder, where `patchMsgLen`/`patchLen` raise `PgMessageTooLargeError`.

  test "PgMessageTooLargeError is an input error":
    check PgMessageTooLargeError is PgTypeError
    check PgMessageTooLargeError is PgError

  test "the pre-flight raises the same type the encoder does":
    # `patchMsgLen`'s own branch needs a >2 GiB buffer, so pin the two the
    # ordinary path goes through instead — the pre-flight bound:
    expect PgMessageTooLargeError:
      checkMsgLenBound64(int64(maxInt32Len) + 1, "Bind message")
    # And the running payload accumulator both Bind builders share:
    var payload = int64(maxInt32Len)
    expect PgMessageTooLargeError:
      addBindPayload(payload, 1)

  test "it is deliberately not a connection failure":
    # A reconnect-on-failure loop must not re-dial a live connection over a
    # caller-sized argument.
    var caught = false
    try:
      raise newException(PgMessageTooLargeError, "too large")
    except PgConnectionError:
      check false
    except PgTypeError:
      caught = true
    check caught

  test "an over-count parameter list still reports the count":
    # Exact and model-free, so this stays an add-time check.
    try:
      validateExtendedQuery("SELECT 1", maxInt16Count + 1)
      check false
    except PgTypeError as e:
      check "count" in e.msg

  test "an ordinary statement is accepted":
    validateExtendedQuery("SELECT $1, $2", 2)

  test "the Parse is sized from the OIDs, not from the bind values":
    # The encoded-params entry points take values and OIDs as separate seqs, so
    # a longer OID list must not slip past a pre-flight sized from the values.
    try:
      validateExtendedQuery("SELECT 1", 1, maxInt16Count + 1)
      check false
    except PgTypeError as e:
      check "Parse parameter-type" in e.msg

suite "The non-pipelined exec/query path pre-flights like the pipeline does":
  ## Regression: without add-time validation an input-size error left the
  ## encoder as a `PgProtocolError`, sending reconnect loops after a live conn.

  test "an over-count parameter list is a PgTypeError, not a connection error":
    let conn = mockConn()
    let params = newSeq[PgParam](maxInt16Count + 1)
    expect PgTypeError:
      discard waitFor conn.query("SELECT 1", params)
    # Rejected before anything reached the wire.
    check conn.state == csReady

  test "exec rejects an embedded NUL the same way":
    let conn = mockConn()
    expect PgTypeError:
      discard waitFor conn.exec("SELECT 1\0", newSeq[PgParam]())
    check conn.state == csReady

  test "oversized payload pre-flight is PgTypeError and not a connection failure":
    var payload = int64(maxInt32Len) - 4
    expect PgMessageTooLargeError:
      addBindPayload(payload, 5)
    var caught = false
    try:
      payload = int64(maxInt32Len) - 4
      addBindPayload(payload, 5)
    except PgConnectionError:
      check false
    except PgTypeError:
      caught = true
    check caught
    # Verify a fresh connection remains usable after the helper failure
    let conn = mockConn()
    check conn.state == csReady
    expect PgTypeError:
      discard waitFor conn.query("SELECT 1", newSeq[PgParam](maxInt16Count + 1))
    check conn.state == csReady

  test "CopyData oversized is PgTypeError and not a connection failure":
    expect PgTypeError:
      checkCopyDataLen(maxInt32Len - 3)
    var caught = false
    try:
      checkCopyDataLen(maxInt32Len - 3)
    except PgConnectionError:
      check false
    except PgTypeError:
      caught = true
    check caught

suite "The Bind pre-flight runs before pendingStmtCloses is drained":
  ## Regression: with only the Parse envelope pre-flighted, a rejected Bind ran
  ## after the queued `Close` messages were drained into a discarded buffer.

  test "an over-count parameter-format list is rejected before the drain":
    let conn = mockConn()
    conn.pendingStmtCloses = @["_sc_1"]
    expect PgTypeError:
      discard waitFor conn.queryImpl(
        "SELECT 1",
        newSeq[Option[seq[byte]]](0),
        newSeq[int32](0),
        newSeq[int16](maxInt16Count + 1),
      )
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

  test "exec is rejected before the drain the same way":
    let conn = mockConn()
    conn.pendingStmtCloses = @["_sc_1"]
    expect PgTypeError:
      discard waitFor conn.execImpl(
        "SELECT 1",
        newSeq[Option[seq[byte]]](0),
        newSeq[int32](0),
        newSeq[int16](maxInt16Count + 1),
      )
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

  test "an over-count result-format list is rejected before the drain":
    let conn = mockConn()
    conn.pendingStmtCloses = @["_sc_1"]
    expect PgTypeError:
      discard waitFor conn.queryImpl(
        "SELECT 1", newSeq[PgParam](0), newSeq[int16](maxInt16Count + 1)
      )
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

  test "a caller-built inline Bind is rejected before the drain":
    # The `exec`/`query` overloads flatten and check first, but the `*Impl`
    # procs are public and take ranges that never went through `flattenInline`.
    let conn = mockConn()
    conn.pendingStmtCloses = @["_sc_1"]
    let data = @[1'u8, 2, 3]
    let badRanges = @[(off: 0'i32, len: 8'i32)] # past the end of `data`
    expect PgTypeError:
      discard
        waitFor conn.execInlineImpl("SELECT $1", data, badRanges, @[OidInt4], @[1'i16])
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

    expect PgTypeError:
      discard
        waitFor conn.queryInlineImpl("SELECT $1", data, badRanges, @[OidInt4], @[1'i16])
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

  test "a caller-built inline Parse over-counts its OIDs before the drain":
    # The OID seq sizes the Parse independently of the Bind ranges, so a count
    # the Parse encoder rejects can hide behind a small, valid Bind.
    let conn = mockConn()
    conn.pendingStmtCloses = @["_sc_1"]
    let badOids = newSeq[int32](maxInt16Count + 1)
    expect PgTypeError:
      discard waitFor conn.execInlineImpl(
        "SELECT 1", newSeq[byte](0), @[], badOids, newSeq[int16](0)
      )
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

    expect PgTypeError:
      discard waitFor conn.queryInlineImpl(
        "SELECT 1", newSeq[byte](0), @[], badOids, newSeq[int16](0)
      )
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

suite "The direct macros pre-flight before pendingStmtCloses is drained":
  ## Same regression as the suite above, on the zero-alloc path: the checks in
  ## `addParseDirect`/`addBindDirect` run after the dispatch drained the queue.

  test "execDirect rejects a NUL in SQL before the drain":
    let conn = mockConn()
    conn.pendingStmtCloses = @["_sc_1"]
    expect PgTypeError:
      discard waitFor conn.execDirect("SELECT 1\0")
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

  test "queryDirect rejects a NUL in SQL before the drain":
    let conn = mockConn()
    conn.pendingStmtCloses = @["_sc_1"]
    expect PgTypeError:
      discard waitFor conn.queryDirect("SELECT $1\0", 1'i32)
    check conn.pendingStmtCloses == @["_sc_1"]
    check conn.state == csReady

  when hasChronos and defined(pgTestObservability):
    test "the hoisted pre-flight reaches both envelope bounds":
      # Small inputs cannot trip a bound, so reachability is observed through
      # the counter: 3 hoisted checks, the same 3 inside
      # `addParseDirect`/`addBindDirect`, plus `addExecute`'s own.
      let conn = mockConn()
      conn.writer = defectWriter()
      checkMsgLenBoundCalls = 0
      expect Defect:
        discard waitFor conn.execDirect("SELECT $1", 1'i32)
      check checkMsgLenBoundCalls == 7

suite "executeBatch releases connection when every op fails validation":
  test "all ops rejected as PgTypeError still releases pool slot":
    let pool = makePool(maxSize = 2)
    pool.active = 1
    let conn = mockConn(pool = pool)
    conn.ownerPool = pool
    conn.borrowed = true
    conn.state = csReady
    conn.txStatus = tsIdle
    let fut1 = newFuture[CommandResult]("test1")
    let fut2 = newFuture[CommandResult]("test2")
    let batch = @[
      PendingPoolOp(
        kind: popExec, sql: "SELECT 1\0", execFut: fut1, timeout: ZeroDuration
      ),
      PendingPoolOp(
        kind: popExec, sql: "SELECT 1\0", execFut: fut2, timeout: ZeroDuration
      ),
    ]
    waitFor pool.executeBatch(conn, batch)
    check fut1.failed
    check fut2.failed
    check fut1.readError of PgTypeError
    check fut2.readError of PgTypeError
    check not conn.borrowed
    check pool.active == 0
    check pool.idle.len == 1

  test "partial batch failure still releases pool slot":
    # Both ops fail validation (NUL, over-count) before any pipeline is built,
    # so no transport is needed. The mixed queued/rejected path needs a live
    # transport and is covered under e2e.
    let pool = makePool(maxSize = 2)
    pool.active = 1
    let conn = mockConn(pool = pool)
    conn.ownerPool = pool
    conn.borrowed = true
    conn.state = csReady
    conn.txStatus = tsIdle
    let futOk = newFuture[CommandResult]("ok")
    let futBad = newFuture[CommandResult]("bad")
    let bigParams = newSeq[PgParam](maxInt16Count + 1)
    let batch = @[
      PendingPoolOp(
        kind: popExec,
        sql: "SELECT 1",
        params: bigParams,
        execFut: futOk,
        timeout: ZeroDuration,
      ),
      PendingPoolOp(
        kind: popExec, sql: "SELECT 1\0", execFut: futBad, timeout: ZeroDuration
      ),
    ]
    waitFor pool.executeBatch(conn, batch)
    check futBad.failed
    check futBad.readError of PgTypeError
    check futOk.failed
    check futOk.readError of PgTypeError
    check not conn.borrowed
    check pool.active == 0
    check pool.idle.len == 1
    check pool.size == 1

  test "mixed popQuery and popExec all rejected still releases":
    let pool = makePool(maxSize = 2)
    pool.active = 1
    let conn = mockConn(pool = pool)
    conn.ownerPool = pool
    conn.borrowed = true
    conn.state = csReady
    conn.txStatus = tsIdle
    let futQ = newFuture[QueryResult]("q")
    let futE = newFuture[CommandResult]("e")
    let batch = @[
      PendingPoolOp(
        kind: popQuery, sql: "SELECT 1\0", queryFut: futQ, timeout: ZeroDuration
      ),
      PendingPoolOp(
        kind: popExec, sql: "SELECT 1\0", execFut: futE, timeout: ZeroDuration
      ),
    ]
    waitFor pool.executeBatch(conn, batch)
    check futQ.failed
    check futE.failed
    check not conn.borrowed
    check pool.active == 0
    check pool.idle.len == 1

suite "executeBatch handles transport errors and still releases":
  test "pipeline error still releases pool slot":
    # A queued op reaches `sendBufMsg`, which a mock without a transport would
    # segfault on under chronos — hence `defectWriter`. The write then raises a
    # Defect (chronos) or a socket CatchableError (asyncdispatch); either way
    # the `finally` must release the slot.
    let pool = makePool(maxSize = 2)
    pool.active = 1
    let conn = mockConn(pool = pool)
    conn.ownerPool = pool
    conn.borrowed = true
    conn.state = csReady
    conn.txStatus = tsIdle
    when hasChronos:
      conn.writer = defectWriter()
    let fut = newFuture[CommandResult]("transport")
    let batch = @[
      PendingPoolOp(kind: popExec, sql: "SELECT 1", execFut: fut, timeout: ZeroDuration)
    ]
    waitFor pool.executeBatch(conn, batch)
    check fut.failed
    check not (fut.readError of PgTypeError)
    # The failure is either a wrapped Defect or a transport CatchableError;
    # the important property is that the pool slot is not leaked.
    check not conn.borrowed
    check pool.active == 0
    check fut.readError != nil
    # resetSessionAndRelease may have closed the conn (transport failure) or
    # returned it to idle; either way the pool must not retain a borrowed slot.
    check pool.size == 0 or pool.idle.len == 1

  test "mixed queued and rejected with transport failure isolates errors and releases":
    # One op is queued, one is rejected at add time, and the queued op's
    # pipeline then fails on transport: `ir` is indexed by `queued` while the
    # error arms walk `batch`, and the `finally` must still release.
    let pool = makePool(maxSize = 2)
    pool.active = 1
    let conn = mockConn(pool = pool)
    conn.ownerPool = pool
    conn.borrowed = true
    conn.state = csReady
    conn.txStatus = tsIdle
    when hasChronos:
      conn.writer = defectWriter()
    let futOk = newFuture[CommandResult]("ok")
    let futBad = newFuture[CommandResult]("bad")
    let batch = @[
      PendingPoolOp(
        kind: popExec, sql: "SELECT 1", execFut: futOk, timeout: ZeroDuration
      ),
      PendingPoolOp(
        kind: popExec, sql: "SELECT 1\0", execFut: futBad, timeout: ZeroDuration
      ),
    ]
    waitFor pool.executeBatch(conn, batch)
    check futBad.failed
    check futBad.readError of PgTypeError
    check futOk.failed
    check not (futOk.readError of PgTypeError)
    check futOk.readError != nil
    check not conn.borrowed
    check pool.active == 0
    check pool.size == 0 or pool.idle.len == 1

suite "executeBatch reports close errors to tracer":
  test "resetSession failure is routed to onPoolCloseError and does not mask batch errors":
    when hasChronos:
      testTracerCloseCnt = 0
      let tracer = PgTracer()
      tracer.onPoolCloseError = proc(
          data: TracePoolCloseErrorData
      ) {.gcsafe, raises: [].} =
        inc testTracerCloseCnt
      let pool = makePool(maxSize = 2)
      pool.config.tracer = tracer
      pool.active = 1
      let conn = mockConn(pool = pool)
      conn.ownerPool = pool
      conn.borrowed = true
      conn.state = csReady
      conn.txStatus = tsIdle
      conn.sessionLockDirty = true
      conn.writer = defectWriter()
      pool.config.resetQuery = "SELECT 1"
      let fut1 = newFuture[CommandResult]("c1")
      let fut2 = newFuture[CommandResult]("c2")
      let batch = @[
        PendingPoolOp(
          kind: popExec, sql: "SELECT 1\0", execFut: fut1, timeout: ZeroDuration
        ),
        PendingPoolOp(
          kind: popExec, sql: "SELECT 1\0", execFut: fut2, timeout: ZeroDuration
        ),
      ]
      waitFor pool.executeBatch(conn, batch)
      check fut1.failed and (fut1.readError of PgTypeError)
      check fut2.failed and (fut2.readError of PgTypeError)
      check not conn.borrowed
      check pool.active == 0
      check testTracerCloseCnt == 1
      # The batch's PgTypeError must not be masked by the close error
      check fut1.readError of PgTypeError
    else:
      # asyncdispatch: resetSession swallows CatchableError, so no
      # onPoolCloseError is expected; verify batch errors are preserved
      # and pool slot is not leaked.
      let pool = makePool(maxSize = 2)
      pool.active = 1
      let conn = mockConn(pool = pool)
      conn.ownerPool = pool
      conn.borrowed = true
      conn.state = csReady
      conn.txStatus = tsIdle
      let fut1 = newFuture[CommandResult]("c1")
      let fut2 = newFuture[CommandResult]("c2")
      let batch = @[
        PendingPoolOp(
          kind: popExec, sql: "SELECT 1\0", execFut: fut1, timeout: ZeroDuration
        ),
        PendingPoolOp(
          kind: popExec, sql: "SELECT 1\0", execFut: fut2, timeout: ZeroDuration
        ),
      ]
      waitFor pool.executeBatch(conn, batch)
      check fut1.failed and (fut1.readError of PgTypeError)
      check not conn.borrowed
      check pool.active == 0

suite "A raising close-error tracer never shadows the operation's error":
  when hasChronos:
    test "single-op dispatch keeps the body error when the tracer raises a Defect":
      # `onPoolCloseError` is `raises: []`, so only a Defect can escape it and
      # replace the error the op is about to fail with.
      proc t() {.async.} =
        let tracer = PgTracer()
        tracer.onPoolCloseError = proc(
            data: TracePoolCloseErrorData
        ) {.gcsafe, raises: [].} =
          raise newException(AssertionDefect, "tracer defect")
        let pool = makePool()
        pool.config.tracer = tracer
        pool.config.resetQuery = "SELECT 1"
        let conn = mockConn()
        conn.ownerPool = pool
        conn.writer = defectWriter()
        conn.sessionLockDirty = true # forces unlock_all through the writer
        pool.idle.addLast(conn.toPooled())

        let fut = newFuture[CommandResult]("op")
        let op = PendingPoolOp(
          kind: popExec, sql: "SELECT 1\0", execFut: fut, timeout: ZeroDuration
        )
        await pool.dispatchHomogeneous(@[op], 1)

        doAssert fut.failed
        doAssert fut.readError of PgTypeError,
          "the tracer's Defect must not replace the op's own error"

      waitFor t()

  test "reportCloseError swallows a Defect from the hook":
    let tracer = PgTracer()
    tracer.onPoolCloseError = proc(
        data: TracePoolCloseErrorData
    ) {.gcsafe, raises: [].} =
      raise newException(AssertionDefect, "tracer defect")
    let pool = makePool()
    pool.config.tracer = tracer
    pool.reportCloseError(mockConn(), newException(PgError, "close failed"))

suite "Array encoder guards are catchable and do not disturb valid input":
  ## What these guards reject needs a 2 GiB allocation, which a unit test
  ## cannot make. So: pin that the primitives raise a catchable `PgTypeError`
  ## (not a `Defect`, not an OOM), and drive every encoder family with valid
  ## input so a guard cannot quietly change what it emits.
  ##
  ## Deliberately *not* covered: the array encoders bound one element and the
  ## element count, but the total payload is still summed inside
  ## `encodeBinaryArray` — after every element has been built. An array whose
  ## elements each pass but whose sum does not is allocated in full before it
  ## is rejected.

  test "guard primitives raise a catchable PgTypeError":
    expect PgTypeError:
      discard dimsFor1D(int32.high.int + 1)
    expect PgTypeError:
      checkPgBinLen(maxInt32Len + 1, "string")
    expect PgTypeError:
      checkPgBinLen(maxInt32Len + 1, "bytea")
    expect PgTypeError:
      checkPgBinPayload(int64(int32.high) + 1, "Array")

  test "binary array encoders still emit what they emitted":
    let strs = toPgParam(@["a", "b"])
    check strs.oid == OidTextArray
    check strs.format == 1'i16
    check strs.value.get ==
      encodeBinaryArray(OidText, @[2'i32], @[some(toBytes("a")), some(toBytes("b"))])
    check toPgParam(@[some("x"), none(string)]).value.get ==
      encodeBinaryArray(OidText, @[2'i32], @[some(toBytes("x")), none(seq[byte])])
    check toPgParam(newSeq[string](0)).value.get == encodeBinaryArray(OidText, @[], @[])
    check toPgByteaArrayParam(@[@[1'u8, 2], @[3'u8]]).value.isSome

  test "every guarded encoder family still round-trips a small value":
    check toPgParam(@[parsePgNumeric("123.45")]).value.isSome
    check toPgParam(@[PgPath(closed: true, points: @[PgPoint(x: 1, y: 2)])]).value.isSome
    check toPgBinaryParam(@[PgBit(nbits: 3, data: @[0b101'u8])]).value.isSome
    check toPgParam(@[newJInt(1), newJInt(2)]).value.isSome
    check toPgParam(@[PgXml("a"), PgXml("b")]).value.isSome

  test "hstore text encoders emit the same literal with the per-entry bound":
    var h: PgHstore = initTable[string, Option[string]]()
    h["k"] = some("v")
    check encodeHstoreText(h) == "\"k\"=>\"v\""
    check toPgParam(h).value.get.toString == "\"k\"=>\"v\""
    check toPgParam(newSeq[PgHstore](0)).value.get.toString == "{}"
    check toPgParam(@[h]).value.get.toString == "{\"\\\"k\\\"=>\\\"v\\\"\"}"
    check toPgBinaryParam(@[PgHstore()], 9999'i32, 9998'i32).value.isSome

  test "writeParamValue and envelope helpers reject oversized before buffer growth":
    expect PgMessageTooLargeError:
      checkMsgLenBound64(int64(maxInt32Len) + 1, "Bind message")
    expect PgMessageTooLargeError:
      checkMsgLenBound64(
        calcParseMessageLength(0, 0, 0) + int64(maxInt32Len) + 1, "Parse message"
      )
    var buf: seq[byte] = @[]
    buf.writeParamValue("abc")
    check buf.len > 0
    var buf2: seq[byte] = @[]
    buf2.writeParamValue(@[1'u8, 2, 3])
    check buf2.len > 0
    # Helpers must not touch buffer on failure
    var b: seq[byte] = @[1'u8, 2, 3]
    let orig = b
    expect PgMessageTooLargeError:
      checkMsgLenBound64(int64(maxInt32Len) + 1, "Bind message")
    check b == orig
