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
      maintenanceInterval: seconds(30),
    ),
    idle: initDeque[PooledConn](),
    active: 0,
    waiters: initDeque[Future[PgConnection]](),
    closed: false,
  )

proc toPooled(conn: PgConnection): PooledConn =
  PooledConn(conn: conn, lastUsedAt: Moment.now())

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
    check cfg.maxWaiters == 0

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
    let waiter = newFuture[PgConnection]("test.waiter")
    pool.waiters.addLast(waiter)
    let conn = mockConn()
    pool.release(conn)
    check pool.active == 2
    check pool.waiters.len == 0
    check waiter.finished
    check waiter.read() == conn

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

  test "release stores PooledConn with lastUsedAt":
    let pool = makePool()
    pool.active = 1
    let conn = mockConn()
    let before = Moment.now()
    pool.release(conn)
    check pool.idle.len == 1
    check pool.idle[0].conn == conn
    check pool.idle[0].lastUsedAt >= before

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
    let waiter = newFuture[PgConnection]("test.waiter")
    pool.waiters.addLast(waiter)

    waitFor pool.close()
    check pool.closed
    check pool.waiters.len == 0
    check waiter.finished

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

suite "Pool active count tracking":
  test "release then acquire roundtrip":
    let pool = makePool()
    let conn = mockConn()
    pool.active = 1
    pool.release(conn)
    check pool.active == 0

    discard waitFor pool.acquire()
    check pool.active == 1

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
  test "maxWaiters 0 allows unlimited waiters":
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
      doAssert pool.waiters.len == 0

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

      # Waiter should be removed from queue
      doAssert pool.waiters.len == 0

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

  test "timeout only removes own waiter from queue":
    proc t() {.async.} =
      let pool = makePool(maxSize = 1)
      pool.config.acquireTimeout = milliseconds(50)
      pool.active = 1

      # Pre-existing waiter (e.g. from another coroutine with no timeout)
      let otherWaiter = newFuture[PgConnection]("test.other")
      pool.waiters.addLast(otherWaiter)

      try:
        discard await pool.acquire()
      except PgError:
        discard

      doAssert pool.waiters.len == 1
      doAssert pool.waiters[0] == otherWaiter

      # Clean up
      otherWaiter.complete(mockConn())

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

      doAssert pool.waiters.len == 0
      doAssert pool.active == 1

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
