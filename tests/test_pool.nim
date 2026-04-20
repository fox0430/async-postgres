import std/[unittest, deques, tables, strutils, importutils]

import ../async_postgres/async_backend
when hasChronos:
  import pkg/chronos/streams/asyncstream

import ../async_postgres/pg_protocol
import ../async_postgres/pg_connection
import ../async_postgres/pg_pool {.all.}

privateAccess(PgPool)
privateAccess(PgConnection)
privateAccess(PooledConn)
privateAccess(Waiter)

proc mockConn(state: PgConnState = csReady): PgConnection =
  result = PgConnection(
    recvBuf: @[],
    state: state,
    txStatus: tsIdle,
    serverParams: initTable[string, string](),
    createdAt: Moment.now(),
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

suite "initConnConfig":
  test "defaults":
    let cfg = initConnConfig()
    check cfg.host == "127.0.0.1"
    check cfg.port == 5432
    check cfg.user == ""
    check cfg.password == ""
    check cfg.database == ""
    check cfg.sslMode == sslDisable
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

  test "validation: minSize = 0 is valid":
    let cfg = initPoolConfig(ConnConfig(host: "localhost", port: 5432), minSize = 0)
    check cfg.minSize == 0

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

  test "release stores PooledConn with cachedNow as lastUsedAt":
    let pool = makePool()
    pool.active = 1
    pool.cachedNow = Moment.now()
    let conn = mockConn()
    pool.release(conn)
    check pool.idle.len == 1
    check pool.idle[0].conn == conn
    check pool.idle[0].lastUsedAt == pool.cachedNow

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

suite "Pool resetSession":
  test "resetSession is no-op when resetQuery is empty":
    let pool = makePool()
    let conn = mockConn()
    conn.stmtCacheCapacity = 256
    conn.addStmtCache("SELECT 1", CachedStmt(name: "_sc_1"))
    waitFor pool.resetSession(conn)
    check conn.state == csReady
    check conn.stmtCache.len == 1 # not cleared

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
    check pool.pendingCloses.len == 1

  test "close awaits pending closeNoWait tasks":
    let pool = makePool()
    pool.active = 2
    pool.release(mockConn(csClosed))
    pool.release(mockConn(csClosed))

    waitFor pool.close()
    check pool.closed
    check pool.pendingCloses.len == 0

  test "closeNoWait prunes finished futures once threshold is reached":
    let pool = makePool()
    # Inject pre-finished dummies up to the prune threshold so the next
    # closeNoWait deterministically triggers the sweep regardless of timing.
    for _ in 0 ..< closePruneThreshold:
      let f = newFuture[void]("dummy")
      f.complete()
      pool.pendingCloses.add(f)
    pool.active = 1
    pool.release(mockConn(csClosed))
    # All finished dummies were swept, leaving only the newly spawned close.
    check pool.pendingCloses.len == 1
    waitFor pool.close()

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
    let conn = mockConn(csClosed)
    pool.active = 1
    pool.release(conn)
    check pool.active == 0
    pool.release(conn)
    check pool.active == 0

  test "double release of normal connection does not underflow active":
    let pool = makePool()
    let conn = mockConn()
    pool.active = 1
    pool.release(conn)
    check pool.active == 0
    check pool.idle.len == 1
    # Second release — conn is now in idle, but active is already 0
    pool.release(conn)
    check pool.active == 0

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
