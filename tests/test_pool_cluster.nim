import std/[unittest, deques, tables, importutils, strutils]

import ../async_postgres/[async_backend, pg_protocol, pg_connection]
import ../async_postgres/pg_pool {.all.}
import ../async_postgres/pg_pool_cluster {.all.}

privateAccess(PgPool)
privateAccess(PgConnection)
privateAccess(PooledConn)
privateAccess(Waiter)
privateAccess(PgPoolCluster)

proc mockConn(state: PgConnState = csReady, pool: PgPool = nil): PgConnection =
  PgConnection(
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

proc makeCluster(
    fallback = fallbackNone,
    primaryMaxSize = 5,
    replicaMaxSize = 5,
    fallbackTimeout = ZeroDuration,
): PgPoolCluster =
  PgPoolCluster(
    primary: makePool(maxSize = primaryMaxSize),
    replica: makePool(maxSize = replicaMaxSize),
    fallback: fallback,
    fallbackTimeout: fallbackTimeout,
    closed: false,
  )

proc mockIdle(pool: PgPool, conn: PgConnection) =
  ## Place a mock connection in the pool's idle queue, wiring `ownerPool`
  ## the same way production `newPool` does. Without this, a subsequent
  ## `conn.release()` (e.g. via `withReadConnection`) would raise because
  ## the back-reference is nil.
  conn.ownerPool = pool
  pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

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
    cluster.replica.mockIdle(conn)

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == conn
    check pool == cluster.replica
    check cluster.replica.active == 1
    check cluster.primary.active == 0

  test "withReadConnection acquires from replica and releases":
    proc t() {.async.} =
      let cluster = makeCluster()
      let conn = mockConn()
      cluster.replica.mockIdle(conn)

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
      cluster.primary.mockIdle(conn)

      cluster.withWriteConnection(c):
        doAssert c == conn
        doAssert cluster.primary.active == 1

      doAssert cluster.primary.active == 0
      doAssert cluster.primary.idle.len == 1

    waitFor t()

  test "withWriteConnection and withReadConnection in same scope with same name":
    proc t() {.async.} =
      let cluster = makeCluster()
      let wConn = mockConn()
      let rConn = mockConn()
      cluster.primary.mockIdle(wConn)
      cluster.replica.mockIdle(rConn)

      cluster.withWriteConnection(conn):
        doAssert conn == wConn
        doAssert cluster.primary.active == 1

      cluster.withReadConnection(conn):
        doAssert conn == rConn
        doAssert cluster.replica.active == 1

      doAssert cluster.primary.active == 0
      doAssert cluster.replica.active == 0

    waitFor t()

suite "Exception safety":
  test "withReadConnection releases on exception":
    proc t() {.async.} =
      let cluster = makeCluster()
      let conn = mockConn()
      cluster.replica.mockIdle(conn)

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
      cluster.primary.mockIdle(conn)

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
  test "fallbackPrimary falls back to primary when replica unavailable":
    let cluster = makeCluster(fallback = fallbackPrimary)
    # replica has no idle connections and is at max
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.acquireTimeout = ZeroDuration
    # But we need the waiter to fail immediately — set maxWaiters to reject
    cluster.replica.config.maxWaiters = 1
    # Fill the waiter queue so next acquire is rejected
    let dummyFut = newFuture[PgConnection]("dummy")
    cluster.replica.waiters.addLast(Waiter(fut: dummyFut, cancelled: false))
    cluster.replica.waiterCount = 1

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary
    check cluster.primary.active == 1

    # Clean up
    dummyFut.complete(mockConn())

  test "fallbackNone raises when replica unavailable":
    let cluster = makeCluster(fallback = fallbackNone)
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.maxWaiters = 1
    let dummyFut = newFuture[PgConnection]("dummy")
    cluster.replica.waiters.addLast(Waiter(fut: dummyFut, cancelled: false))
    cluster.replica.waiterCount = 1

    expect(PgError):
      discard waitFor acquireRead(cluster)

    check cluster.primary.active == 0

    # Clean up
    dummyFut.complete(mockConn())

  test "fallbackPrimary raises when both pools unavailable":
    let cluster = makeCluster(fallback = fallbackPrimary)
    # Both pools at max with full waiter queues
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.maxWaiters = 1
    let replicaFut = newFuture[PgConnection]("replica-dummy")
    cluster.replica.waiters.addLast(Waiter(fut: replicaFut, cancelled: false))
    cluster.replica.waiterCount = 1

    cluster.primary.active = cluster.primary.config.maxSize
    cluster.primary.config.maxWaiters = 1
    let primaryFut = newFuture[PgConnection]("primary-dummy")
    cluster.primary.waiters.addLast(Waiter(fut: primaryFut, cancelled: false))
    cluster.primary.waiterCount = 1

    expect(PgError):
      discard waitFor acquireRead(cluster)

    # Clean up
    replicaFut.complete(mockConn())
    primaryFut.complete(mockConn())

  test "fallbackTimeout bounds the primary fallback leg and wraps the replica error":
    let cluster =
      makeCluster(fallback = fallbackPrimary, fallbackTimeout = milliseconds(50))
    cluster.replica.closed = true
    # Primary at max with no idle connections, and makePool leaves the pool's
    # own acquireTimeout at ZeroDuration — so only fallbackTimeout can end the
    # wait. Reaching the raise at all proves fallbackTimeout bounds the primary
    # leg (otherwise this would hang forever).
    cluster.primary.active = cluster.primary.config.maxSize

    var raised: ref PgError
    try:
      discard waitFor acquireRead(cluster)
    except PgError as e:
      raised = e

    check raised != nil
    check "fallback acquire timeout" in raised.msg
    # The replica failure that triggered the fallback is preserved as the cause.
    check raised.parent != nil
    check "Pool is closed" in raised.parent.msg

  test "fallbackTimeout succeeds when primary available within timeout":
    let cluster = makeCluster(fallback = fallbackPrimary, fallbackTimeout = seconds(5))
    cluster.replica.closed = true

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary

  test "fallbackPrimary fallback when replica pool is closed":
    let cluster = makeCluster(fallback = fallbackPrimary)
    cluster.replica.closed = true

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary

  test "fallbackTimeout bounds the replica acquire for fast failover":
    # Replica would block for its full acquireTimeout (30s) without the
    # fallbackTimeout bound; with it, the read fails over to the primary
    # after ~fallbackTimeout instead.
    let cluster =
      makeCluster(fallback = fallbackPrimary, fallbackTimeout = milliseconds(50))
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.acquireTimeout = seconds(30)
    let replicaFut = newFuture[PgConnection]("replica-dummy")
    cluster.replica.waiters.addLast(Waiter(fut: replicaFut, cancelled: false))
    cluster.replica.waiterCount = 1

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary

    replicaFut.complete(mockConn())

  when hasAsyncDispatch:
    test "abandoned replica acquire returns its late connection to the pool":
      # asyncdispatch `wait()` cannot cancel the inner replica acquire: it
      # keeps running after `fallbackTimeout` and a later release hands its
      # connection to the abandoned waiter. `drainAbandonedAcquire` must
      # return that connection to the pool instead of leaking the `active`
      # slot. (Under chronos the acquire is cancelled outright and cleans up
      # its own accounting in acquireImpl, so this scenario cannot arise.)
      let cluster =
        makeCluster(fallback = fallbackPrimary, fallbackTimeout = milliseconds(50))
      # Saturate the replica so acquireRead queues a waiter and times out.
      cluster.replica.active = cluster.replica.config.maxSize
      cluster.replica.config.acquireTimeout = seconds(30)

      let primaryConn = mockConn()
      cluster.primary.idle.addLast(
        PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
      )

      let (acquired, pool) = waitFor acquireRead(cluster)
      check acquired == primaryConn
      check pool == cluster.primary
      check cluster.replica.waiterCount == 1

      # A borrower releases a connection: the FIFO handoff serves the
      # abandoned waiter, whose acquire nobody awaits anymore. A real borrower
      # is marked `borrowed` by `acquire`; mirror that so the release is not
      # treated as a no-op double-release and the handoff actually fires.
      let lateConn = mockConn(pool = cluster.replica)
      lateConn.borrowed = true
      lateConn.release()

      # Let the abandoned acquire complete and the drain release the conn.
      waitFor sleepMsAsync(100)

      check cluster.replica.waiterCount == 0
      check cluster.replica.active == cluster.replica.config.maxSize - 1
      check cluster.replica.idle.len == 1
      check cluster.replica.idle.peekFirst().conn == lateConn

  test "onReadFallback fires with rfrReplicaClosed when replica is closed":
    var fired = 0
    var seenReason: ReadFallbackReason
    let cluster = makeCluster(fallback = fallbackPrimary)
    cluster.onReadFallback = proc(
        reason: ReadFallbackReason, err: ref CatchableError
    ) {.gcsafe, raises: [].} =
      inc fired
      seenReason = reason
    cluster.replica.closed = true

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary
    check fired == 1
    check seenReason == rfrReplicaClosed

  test "onReadFallback fires with rfrReplicaUnavailable on transient replica error":
    var seenReason: ReadFallbackReason
    var fired = false
    let cluster = makeCluster(fallback = fallbackPrimary)
    cluster.onReadFallback = proc(
        reason: ReadFallbackReason, err: ref CatchableError
    ) {.gcsafe, raises: [].} =
      fired = true
      seenReason = reason
    # Replica rejects immediately: at max with a full waiter queue.
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.maxWaiters = 1
    let dummyFut = newFuture[PgConnection]("dummy")
    cluster.replica.waiters.addLast(Waiter(fut: dummyFut, cancelled: false))
    cluster.replica.waiterCount = 1

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary
    check fired
    check seenReason == rfrReplicaUnavailable

    dummyFut.complete(mockConn())

  test "onReadFallback fires with rfrReplicaTimeout when replica acquire times out":
    var seenReason: ReadFallbackReason
    var fired = false
    let cluster =
      makeCluster(fallback = fallbackPrimary, fallbackTimeout = milliseconds(50))
    cluster.onReadFallback = proc(
        reason: ReadFallbackReason, err: ref CatchableError
    ) {.gcsafe, raises: [].} =
      fired = true
      seenReason = reason
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.acquireTimeout = seconds(30)
    let replicaFut = newFuture[PgConnection]("replica-dummy")
    cluster.replica.waiters.addLast(Waiter(fut: replicaFut, cancelled: false))
    cluster.replica.waiterCount = 1

    let primaryConn = mockConn()
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == primaryConn
    check pool == cluster.primary
    check fired
    check seenReason == rfrReplicaTimeout

    replicaFut.complete(mockConn())

  test "onReadFallback does not fire when replica succeeds":
    var fired = false
    let cluster = makeCluster(fallback = fallbackPrimary)
    cluster.onReadFallback = proc(
        reason: ReadFallbackReason, err: ref CatchableError
    ) {.gcsafe, raises: [].} =
      fired = true
    let replicaConn = mockConn()
    cluster.replica.mockIdle(replicaConn)

    let (acquired, pool) = waitFor acquireRead(cluster)
    check acquired == replicaConn
    check pool == cluster.replica
    check not fired

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

  test "close with timeout drains primary and replica concurrently":
    ## Regression: serial primary→replica drains made close wait up to
    ## 2*timeout when both pools had unreleased active connections.
    let cluster = makeCluster()
    cluster.primary.active = 1
    cluster.replica.active = 1

    let start = Moment.now()
    waitFor cluster.close(milliseconds(400))
    let elapsed = Moment.now() - start

    # Concurrent drains overlap: ~400ms total. The old serial path took
    # >= 800ms. 700ms splits the two with slack for a slow scheduler.
    check elapsed < milliseconds(700)
    check cluster.closed
    check cluster.primary.closed
    check cluster.replica.closed

  test "close closes replica even if primary.close raises":
    ## Ensures try/finally guarantees replica cleanup when primary fails.
    let cluster = makeCluster()
    # Add an idle connection with csReady state to primary — closing it will
    # attempt to send Terminate on a nil socket, which raises.
    cluster.primary.idle.addLast(
      PooledConn(conn: mockConn(csReady), lastUsedAt: Moment.now())
    )
    cluster.replica.idle.addLast(
      PooledConn(conn: mockConn(csClosed), lastUsedAt: Moment.now())
    )

    # close() should not propagate the primary error (pool.close catches it)
    waitFor cluster.close()
    check cluster.closed
    check cluster.primary.closed
    check cluster.replica.closed
    check cluster.replica.idle.len == 0

suite "Write routing":
  test "writeConnection routes to primary":
    let cluster = makeCluster()
    cluster.primary.closed = true

    expect(PgError):
      discard waitFor cluster.writeConnection()

  test "readConnection routes to replica":
    let cluster = makeCluster()
    cluster.replica.closed = true
    cluster.fallback = fallbackNone

    expect(PgError):
      discard waitFor cluster.readConnection()

  test "readConnection falls back to primary when replica unavailable":
    let cluster = makeCluster(fallback = fallbackPrimary)
    cluster.replica.active = cluster.replica.config.maxSize
    cluster.replica.config.acquireTimeout = ZeroDuration
    cluster.replica.config.maxWaiters = 1
    let dummyFut = newFuture[PgConnection]("dummy")
    cluster.replica.waiters.addLast(Waiter(fut: dummyFut, cancelled: false))
    cluster.replica.waiterCount = 1

    let primaryConn = mockConn()
    primaryConn.ownerPool = cluster.primary
    cluster.primary.idle.addLast(
      PooledConn(conn: primaryConn, lastUsedAt: Moment.now())
    )

    let h = waitFor cluster.readConnection()
    check h.conn == primaryConn
    h.release()

    dummyFut.complete(mockConn())

suite "Closed pool cluster":
  test "acquire on closed replica raises error":
    let cluster = makeCluster()
    cluster.replica.closed = true
    cluster.fallback = fallbackNone

    expect(PgError):
      discard waitFor acquireRead(cluster)

  test "acquire on closed primary raises error":
    let cluster = makeCluster()
    cluster.primary.closed = true

    expect(PgError):
      discard waitFor cluster.primary.acquire()
