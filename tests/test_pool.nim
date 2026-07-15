import std/[unittest, deques, tables, strutils, importutils]

import ../async_postgres/async_backend
when hasChronos:
  import pkg/chronos/streams/asyncstream

import ../async_postgres/pg_protocol
import ../async_postgres/pg_connection
import ../async_postgres/pg_pool {.all.}

import ./mock_pg_server

privateAccess(PgPool)
privateAccess(PgConnection)
privateAccess(PooledConn)
privateAccess(Waiter)
privateAccess(PendingPoolOp)

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
    var msg = ""
    try:
      discard waitFor pool.acquire()
    except PgError as e:
      msg = e.msg

    check "queue full" in msg.toLowerAscii()
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

        var errA = ""
        try:
          discard await futA
        except PgPoolError as e:
          errA = e.msg
        doAssert "Pool connect failed" in errA

        # With the fix, B is served by the spawn (which also fails against the
        # unresponsive mock) and returns fast; without it, B would time out on
        # acquireTimeout with "Pool acquire timeout".
        var errB = ""
        let bStart = Moment.now()
        try:
          discard await futB
        except PgPoolError as e:
          errB = e.msg
        let bElapsed = Moment.now() - bStart
        doAssert "Pool connect for waiter failed" in errB
        doAssert "acquire timeout" notin errB
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

    var caught = false
    try:
      discard waitFor pool.acquire()
    except PgPoolError:
      caught = true
    except PgError:
      discard
    check caught

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

        # close() settles the still-queued waiter with "Pool closed".
        await pool.close()
        var caught: ref PgPoolError
        try:
          discard await acqFut
        except PgPoolError as e:
          caught = e
        doAssert caught != nil
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
      doAssert "closed" in err.msg
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
      doAssert "closed" in err.msg
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
