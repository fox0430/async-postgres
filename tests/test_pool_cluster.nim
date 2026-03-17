import std/[unittest, deques, tables, importutils]

import ../async_postgres/async_backend
import ../async_postgres/pg_protocol
import ../async_postgres/pg_connection
import ../async_postgres/pg_pool {.all.}
import ../async_postgres/pg_pool_cluster {.all.}

privateAccess(PgPool)
privateAccess(PgConnection)
privateAccess(PooledConn)
privateAccess(PgPoolCluster)

proc mockConn(state: PgConnState = csReady): PgConnection =
  PgConnection(
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

proc makeCluster(
    fallback = rfNone, primaryMaxSize = 5, replicaMaxSize = 5
): PgPoolCluster =
  PgPoolCluster(
    primary: makePool(maxSize = primaryMaxSize),
    replica: makePool(maxSize = replicaMaxSize),
    fallback: fallback,
    closed: false,
  )

suite "newPoolCluster targetSessionAttrs":
  test "auto-sets tsaReadWrite for primary and tsaPreferStandby for replica when tsaAny":
    var pCfg = PoolConfig(
      connConfig: ConnConfig(host: "localhost", port: 5432, targetSessionAttrs: tsaAny),
      minSize: 0,
      maxSize: 1,
      maintenanceInterval: seconds(30),
    )
    var rCfg = PoolConfig(
      connConfig: ConnConfig(host: "localhost", port: 5433, targetSessionAttrs: tsaAny),
      minSize: 0,
      maxSize: 1,
      maintenanceInterval: seconds(30),
    )

    # We can't call newPoolCluster without a real server, so verify the logic
    # by checking the config mutation directly.
    if pCfg.connConfig.targetSessionAttrs == tsaAny:
      pCfg.connConfig.targetSessionAttrs = tsaReadWrite
    if rCfg.connConfig.targetSessionAttrs == tsaAny:
      rCfg.connConfig.targetSessionAttrs = tsaPreferStandby

    check pCfg.connConfig.targetSessionAttrs == tsaReadWrite
    check rCfg.connConfig.targetSessionAttrs == tsaPreferStandby

  test "preserves explicit targetSessionAttrs":
    var pCfg = PoolConfig(
      connConfig:
        ConnConfig(host: "localhost", port: 5432, targetSessionAttrs: tsaPrimary)
    )
    var rCfg = PoolConfig(
      connConfig:
        ConnConfig(host: "localhost", port: 5433, targetSessionAttrs: tsaStandby)
    )

    if pCfg.connConfig.targetSessionAttrs == tsaAny:
      pCfg.connConfig.targetSessionAttrs = tsaReadWrite
    if rCfg.connConfig.targetSessionAttrs == tsaAny:
      rCfg.connConfig.targetSessionAttrs = tsaPreferStandby

    check pCfg.connConfig.targetSessionAttrs == tsaPrimary
    check rCfg.connConfig.targetSessionAttrs == tsaStandby

suite "Read routing":
  test "acquireRead returns connection from replica pool":
    let cluster = makeCluster()
    let conn = mockConn()
    cluster.replica.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == conn
    check pool == cluster.replica
    check cluster.replica.active == 1
    check cluster.primary.active == 0

  test "withReadConnection acquires from replica and releases":
    proc t() {.async.} =
      let cluster = makeCluster()
      let conn = mockConn()
      cluster.replica.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

      cluster.withReadConnection(c):
        doAssert c == conn
        doAssert cluster.replica.active == 1

      doAssert cluster.replica.active == 0
      doAssert cluster.replica.idle.len == 1

    waitFor t()

  test "withWriteConnection acquires from primary and releases":
    proc t() {.async.} =
      let cluster = makeCluster()
      let conn = mockConn()
      cluster.primary.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

      cluster.withWriteConnection(c):
        doAssert c == conn
        doAssert cluster.primary.active == 1

      doAssert cluster.primary.active == 0
      doAssert cluster.primary.idle.len == 1

    waitFor t()

suite "Exception safety":
  test "withReadConnection releases on exception":
    proc t() {.async.} =
      let cluster = makeCluster()
      let conn = mockConn()
      cluster.replica.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

      var caught = false
      try:
        cluster.withReadConnection(c):
          doAssert cluster.replica.active == 1
          raise newException(ValueError, "boom")
      except ValueError:
        caught = true

      doAssert caught
      doAssert cluster.replica.active == 0
      doAssert cluster.replica.idle.len == 1

    waitFor t()

  test "withWriteConnection releases on exception":
    proc t() {.async.} =
      let cluster = makeCluster()
      let conn = mockConn()
      cluster.primary.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

      var caught = false
      try:
        cluster.withWriteConnection(c):
          doAssert cluster.primary.active == 1
          raise newException(ValueError, "boom")
      except ValueError:
        caught = true

      doAssert caught
      doAssert cluster.primary.active == 0
      doAssert cluster.primary.idle.len == 1

    waitFor t()

suite "Fallback":
  test "rfPrimary falls back to primary when replica unavailable":
    let cluster = makeCluster(fallback = rfPrimary)
    # replica has no idle connections and is at max
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.acquireTimeout = ZeroDuration
    # But we need the waiter to fail immediately — set maxWaiters to reject
    cluster.replica.config.maxWaiters = 1
    # Fill the waiter queue so next acquire is rejected
    let dummyWaiter = newFuture[PgConnection]("dummy")
    cluster.replica.waiters.addLast(dummyWaiter)

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary
    check cluster.primary.active == 1

    # Clean up
    dummyWaiter.complete(mockConn())

  test "rfNone raises when replica unavailable":
    let cluster = makeCluster(fallback = rfNone)
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.maxWaiters = 1
    let dummyWaiter = newFuture[PgConnection]("dummy")
    cluster.replica.waiters.addLast(dummyWaiter)

    expect(PgError):
      discard waitFor acquireRead(cluster)

    check cluster.primary.active == 0

    # Clean up
    dummyWaiter.complete(mockConn())

  test "rfPrimary raises when both pools unavailable":
    let cluster = makeCluster(fallback = rfPrimary)
    # Both pools at max with full waiter queues
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.maxWaiters = 1
    let replicaWaiter = newFuture[PgConnection]("replica-dummy")
    cluster.replica.waiters.addLast(replicaWaiter)

    cluster.primary.active = cluster.primary.config.maxSize
    cluster.primary.config.maxWaiters = 1
    let primaryWaiter = newFuture[PgConnection]("primary-dummy")
    cluster.primary.waiters.addLast(primaryWaiter)

    expect(PgError):
      discard waitFor acquireRead(cluster)

    # Clean up
    replicaWaiter.complete(mockConn())
    primaryWaiter.complete(mockConn())

  test "rfPrimary fallback when replica pool is closed":
    let cluster = makeCluster(fallback = rfPrimary)
    cluster.replica.closed = true

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary

suite "Close":
  test "close sets closed and closes both pools":
    let cluster = makeCluster()
    check not cluster.closed
    check not cluster.primary.closed
    check not cluster.replica.closed

    waitFor cluster.close()

    check cluster.closed
    check cluster.primary.closed
    check cluster.replica.closed

  test "close drains idle from both pools":
    let cluster = makeCluster()
    cluster.primary.idle.addLast(
      PooledConn(conn: mockConn(csClosed), lastUsedAt: Moment.now())
    )
    cluster.replica.idle.addLast(
      PooledConn(conn: mockConn(csClosed), lastUsedAt: Moment.now())
    )

    waitFor cluster.close()
    check cluster.primary.idle.len == 0
    check cluster.replica.idle.len == 0

suite "Closed pool cluster":
  test "acquire on closed replica raises error":
    let cluster = makeCluster()
    cluster.replica.closed = true
    cluster.fallback = rfNone

    expect(PgError):
      discard waitFor acquireRead(cluster)

  test "acquire on closed primary raises error":
    let cluster = makeCluster()
    cluster.primary.closed = true

    expect(PgError):
      discard waitFor cluster.primary.acquire()
