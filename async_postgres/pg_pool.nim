import std/[deques, macros, options]

import async_backend, pg_protocol, pg_connection, pg_types, pg_client

const closePruneThreshold = 16
  ## Sweep `pool.pendingCloses` for finished futures only once its length
  ## reaches this threshold, so the O(n) prune is amortized across calls.

type
  PoolConfig* = object
    ## Configuration for the connection pool. Create via `initPoolConfig`.
    connConfig*: ConnConfig
    minSize*: int ## Minimum idle connections (default 1)
    maxSize*: int ## Maximum total connections (default 10)
    idleTimeout*: Duration
      ## Close idle connections after this duration (default 10min, ZeroDuration=disabled)
    maxLifetime*: Duration
      ## Max connection lifetime (default 1hr, ZeroDuration=disabled)
    maintenanceInterval*: Duration ## Maintenance loop interval (default 30s)
    healthCheckTimeout*: Duration
      ## Ping idle connections older than this before returning (default 5s, ZeroDuration=disabled)
    pingTimeout*: Duration
      ## Max time to wait for a health check ping response (default 5s, ZeroDuration=no timeout)
    acquireTimeout*: Duration
      ## Max time to wait for an available connection (default 30s, ZeroDuration=no timeout)
    maxWaiters*: int = -1
      ## Max queued acquire waiters (default -1=unlimited, 0=no waiting). Rejects with PgPoolError when full.
    resetQuery*: string
      ## SQL to execute when returning a connection to the pool (default ""=disabled).
      ## Common values: "DISCARD ALL" (full reset, recommended for PgBouncer),
      ## "DEALLOCATE ALL" (clear prepared statements only),
      ## "RESET ALL" (reset session parameters only).
      ## On failure, the connection is discarded.
    tracer*: PgTracer ## Optional tracer for pool-level hooks (acquire/release)
    pipelined*: bool
      ## Enable implicit query batching for pool.exec/query (default false).
      ## When enabled, concurrent calls within the same event loop tick are
      ## batched into a single TCP write per connection using per-query SYNC
      ## for error isolation.
    maxPipelineSize*: int
      ## Max operations per pipeline batch per connection (default 0=unlimited).
      ## Only used when `pipelined` is true.

  PooledConn = object
    ## An idle connection held by the pool with its last-used timestamp.
    conn: PgConnection
    lastUsedAt: Moment

  Waiter = ref object
    fut: Future[PgConnection]
    cancelled: bool

  PoolMetrics* = object ## Cumulative pool statistics.
    acquireCount*: int64 ## Total successful acquires
    acquireDuration*: Duration ## Total time spent waiting in acquire
    timeoutCount*: int64 ## Number of acquire timeouts
    createCount*: int64 ## Number of new connections created
    closeCount*: int64 ## Number of connections closed/discarded

  PendingOpKind = enum
    popExec
    popQuery

  PendingPoolOp = ref object
    kind: PendingOpKind
    sql: string
    params: seq[PgParam]
    resultFormat: ResultFormat ## Only used for popQuery
    timeout: Duration
    execFut: Future[CommandResult] ## Non-nil for popExec
    queryFut: Future[QueryResult] ## Non-nil for popQuery

  PgPool* = ref object of PgPoolOwner
    ## Connection pool that manages a set of PostgreSQL connections.
    config: PoolConfig
    idle: Deque[PooledConn]
    active: int
    waiters: Deque[Waiter]
    waiterCount: int ## Number of non-cancelled waiters
    closed: bool
    maintenanceTask: Future[void]
    cachedNow: Moment
      ## Updated on acquire(); reused by release() to avoid extra syscalls
    metrics: PoolMetrics
    pendingOps: Deque[PendingPoolOp] ## Queue for implicit pipeline batching
    dispatchScheduled: bool ## Whether a dispatch callback is pending
    pendingCloses: seq[Future[void]]
      ## Fire-and-forget close tasks spawned by closeNoWait, awaited on pool.close()

proc initPoolConfig*(
    connConfig: ConnConfig,
    minSize = 1,
    maxSize = 10,
    idleTimeout = minutes(10),
    maxLifetime = hours(1),
    maintenanceInterval = seconds(30),
    healthCheckTimeout = seconds(5),
    pingTimeout = seconds(5),
    acquireTimeout = seconds(30),
    maxWaiters = -1,
    resetQuery = "",
    pipelined = false,
    maxPipelineSize = 0,
): PoolConfig =
  ## Create a pool configuration with sensible defaults.
  ## `minSize` idle connections are maintained; up to `maxSize` total.
  ## Set `resetQuery` to clean session state on release (e.g. "DISCARD ALL" for PgBouncer).
  ## Set `pipelined` to true to enable implicit query batching for `pool.exec`/`pool.query`.
  ##
  ## Raises `ValueError` if parameters are invalid.
  if minSize < 0:
    raise newException(ValueError, "minSize must be >= 0, got " & $minSize)
  if maxSize < 1:
    raise newException(ValueError, "maxSize must be >= 1, got " & $maxSize)
  if minSize > maxSize:
    raise newException(
      ValueError, "minSize (" & $minSize & ") must be <= maxSize (" & $maxSize & ")"
    )
  if maxWaiters < -1:
    raise newException(ValueError, "maxWaiters must be >= -1, got " & $maxWaiters)

  PoolConfig(
    connConfig: connConfig,
    minSize: minSize,
    maxSize: maxSize,
    idleTimeout: idleTimeout,
    maxLifetime: maxLifetime,
    maintenanceInterval: maintenanceInterval,
    healthCheckTimeout: healthCheckTimeout,
    pingTimeout: pingTimeout,
    acquireTimeout: acquireTimeout,
    maxWaiters: maxWaiters,
    resetQuery: resetQuery,
    pipelined: pipelined,
    maxPipelineSize: maxPipelineSize,
  )

proc poolConfig*(pool: PgPool): PoolConfig =
  ## The pool configuration.
  pool.config

proc idleCount*(pool: PgPool): int =
  ## Number of idle connections currently in the pool.
  pool.idle.len

proc activeCount*(pool: PgPool): int =
  ## Number of connections currently checked out from the pool.
  pool.active

proc size*(pool: PgPool): int =
  ## Total number of connections (idle + active).
  pool.idle.len + pool.active

proc pendingAcquires*(pool: PgPool): int =
  ## Number of non-cancelled waiters queued for a connection.
  pool.waiterCount

proc isClosed*(pool: PgPool): bool =
  ## Whether the pool has been closed.
  pool.closed

proc metrics*(pool: PgPool): PoolMetrics =
  ## Cumulative pool metrics.
  pool.metrics

proc reportCloseError(pool: PgPool, conn: PgConnection, err: ref CatchableError) =
  ## Route a swallowed pool-initiated close error to the tracer. The pool
  ## cannot propagate these errors to a caller (close runs from synchronous
  ## cleanup paths and fire-and-forget tasks), so tracing is the only signal
  ## operators have for leak detection.
  if pool.config.tracer != nil and pool.config.tracer.onPoolCloseError != nil:
    pool.config.tracer.onPoolCloseError(TracePoolCloseErrorData(conn: conn, err: err))

proc tracedClose(pool: PgPool, conn: PgConnection) {.async.} =
  ## Close `conn`, reporting any close error via `reportCloseError`.
  try:
    await conn.close()
  except CatchableError as e:
    pool.reportCloseError(conn, e)

proc closeNoWait(pool: PgPool, conn: PgConnection) =
  ## Schedule connection close without waiting. For use in non-async contexts
  ## (e.g. `release()` is synchronous). The spawned task is tracked in
  ## `pool.pendingCloses` so `pool.close()` can await its completion for
  ## graceful shutdown.
  ##
  ## Note on asyncdispatch: a close scheduled here may race with an inflight
  ## request future that the previous timeout could not cancel (see
  ## `invalidateOnTimeout`). That future will observe a closed fd and fail
  ## quietly — `tracedClose` catches the error and routes it to the
  ## `onPoolCloseError` tracer hook (nil when unconfigured). The connection
  ## is not reused either way.
  pool.metrics.closeCount.inc
  proc doClose() {.async.} =
    await pool.tracedClose(conn)

  # Prune only once the seq grows past the threshold so the sweep is amortized
  # instead of O(n) on every call. Uses swap-remove (constant-time delete that
  # reorders) since order among pending closes is irrelevant.
  if pool.pendingCloses.len >= closePruneThreshold:
    var n = pool.pendingCloses.len
    var i = 0
    while i < n:
      if pool.pendingCloses[i].finished:
        pool.pendingCloses[i] = pool.pendingCloses[n - 1]
        dec n
      else:
        inc i
    pool.pendingCloses.setLen(n)

  let fut = doClose()
  pool.pendingCloses.add(fut)
  asyncSpawn fut

proc resetSession*(pool: PgPool, conn: PgConnection) {.async.} =
  ## Execute the configured reset query on a connection before returning it
  ## to the pool. On failure, closes the connection so that release() will
  ## discard it.
  if pool.config.resetQuery.len > 0 and conn.state == csReady and conn.txStatus == tsIdle:
    try:
      discard await conn.simpleExec(pool.config.resetQuery)
      conn.clearStmtCache()
    except CatchableError:
      await pool.tracedClose(conn)

proc maintenanceLoop(pool: PgPool) {.async.} =
  while not pool.closed:
    await sleepAsync(pool.config.maintenanceInterval)
    if pool.closed:
      break

    var remaining = initDeque[PooledConn]()
    let now = Moment.now()

    while pool.idle.len > 0:
      var pc = pool.idle.popFirst()

      # Always close broken or in-transaction connections (unusable)
      if pc.conn.state != csReady or pc.conn.txStatus != tsIdle:
        pool.metrics.closeCount.inc
        await pool.tracedClose(pc.conn)
        continue

      # Always close max-lifetime-exceeded connections (acquire rejects them anyway)
      if pool.config.maxLifetime > ZeroDuration and
          now - pc.conn.createdAt > pool.config.maxLifetime:
        pool.metrics.closeCount.inc
        await pool.tracedClose(pc.conn)
        continue

      # Idle timeout respects minSize
      if pool.config.idleTimeout > ZeroDuration and
          now - pc.lastUsedAt > pool.config.idleTimeout:
        let totalCount = remaining.len + pool.idle.len + pool.active
        if totalCount >= pool.config.minSize:
          pool.metrics.closeCount.inc
          await pool.tracedClose(pc.conn)
          continue

      remaining.addLast(pc)

    pool.idle = remaining

    # Replenish to minSize (best-effort)
    let currentTotal = pool.idle.len + pool.active
    let needed = max(0, pool.config.minSize - currentTotal)
    # Use connectTimeout if set, otherwise cap at maintenanceInterval to avoid blocking
    let replenishTimeout =
      if pool.config.connConfig.connectTimeout > ZeroDuration:
        pool.config.connConfig.connectTimeout
      else:
        pool.config.maintenanceInterval
    for i in 0 ..< needed:
      if pool.closed:
        break
      try:
        let conn = await connect(pool.config.connConfig).wait(replenishTimeout)
        conn.ownerPool = pool
        pool.metrics.createCount.inc
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: now))
      except CatchableError:
        break # best-effort, retry next interval

proc newPool*(config: PoolConfig): Future[PgPool] {.async.} =
  ## Create a new connection pool and establish `minSize` initial connections.
  ## Raises if any initial connection fails (all opened connections are closed on error).
  var cfg = config
  if cfg.maintenanceInterval == ZeroDuration:
    cfg.maintenanceInterval = seconds(30)

  var pool = PgPool(
    config: cfg,
    idle: initDeque[PooledConn](),
    active: 0,
    waiters: initDeque[Waiter](),
    waiterCount: 0,
    closed: false,
    pendingOps: initDeque[PendingPoolOp](),
    dispatchScheduled: false,
  )

  try:
    pool.cachedNow = Moment.now()
    for i in 0 ..< cfg.minSize:
      let conn = await connect(cfg.connConfig)
      conn.ownerPool = pool
      pool.metrics.createCount.inc
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: pool.cachedNow))
  except CatchableError as e:
    while pool.idle.len > 0:
      let pc = pool.idle.popFirst()
      await pool.tracedClose(pc.conn)
    raise e

  pool.maintenanceTask = maintenanceLoop(pool)
  return pool

proc releaseImpl(pool: PgPool, conn: PgConnection) =
  ## Implementation of `release(conn)`; called once the owning pool is known.
  ## Returns the connection to the pool. If the connection is broken or in
  ## a transaction, it is closed instead. If waiters are queued, the
  ## connection is handed directly to the next waiter.
  ##
  ## Discard criteria (`conn.state != csReady`):
  ## - A timed-out request reaches us via `invalidateOnTimeout` with
  ##   `state = csClosed`. Under asyncdispatch this is load-bearing: the
  ##   inner future is still alive and may write to the socket, so the
  ##   connection MUST be retired from the pool.
  ## - Any listening/replication/COPY state is also not reusable.
  ## Transaction-in-progress (`txStatus != tsIdle`) is treated as failure
  ## to reset the session, so the connection is closed rather than leaking
  ## transaction state to the next borrower.
  var traceCtx: TraceContext
  if pool.config.tracer != nil and pool.config.tracer.onPoolReleaseStart != nil:
    traceCtx =
      pool.config.tracer.onPoolReleaseStart(TracePoolReleaseStartData(conn: conn))

  var wasClosed = false
  var handedToWaiter = false
  if pool.closed or conn.state != csReady or conn.txStatus != tsIdle:
    if pool.active > 0:
      pool.active.dec
    pool.closeNoWait(conn)
    wasClosed = true
  else:
    block dispatch:
      while pool.waiters.len > 0:
        let waiter = pool.waiters.popFirst()
        if waiter.cancelled:
          continue
        pool.waiterCount.dec
        waiter.fut.complete(conn)
        handedToWaiter = true
        break dispatch
      if pool.active > 0:
        pool.active.dec
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: pool.cachedNow))

  if pool.config.tracer != nil and pool.config.tracer.onPoolReleaseEnd != nil:
    pool.config.tracer.onPoolReleaseEnd(
      traceCtx,
      TracePoolReleaseEndData(wasClosed: wasClosed, handedToWaiter: handedToWaiter),
    )

proc release*(conn: PgConnection) =
  ## Return a connection to its owning pool. If the connection is broken or
  ## in a transaction, it is closed instead; if waiters are queued, it is
  ## handed directly to the next waiter.
  ##
  ## The owning pool is tracked on `conn.ownerPool`, set automatically when
  ## the connection is acquired from a `PgPool` (including pools inside a
  ## `PgPoolCluster`). For standalone connections created with `connect`
  ## this field is `nil` and calling `release` raises `PgError` — use
  ## `conn.close()` instead.
  ##
  ## `withConnection`, `withReadConnection`, `withWriteConnection`,
  ## `withPipeline`, and `withTransaction` call this automatically; direct
  ## callers only need it when they manage `acquire`/`release` manually.
  if conn.ownerPool == nil:
    raise newException(
      PgError, "release() called on a standalone connection; use conn.close() instead"
    )
  PgPool(conn.ownerPool).releaseImpl(conn)

type AcquireResult = tuple[conn: PgConnection, wasCreated: bool]

proc acquireImpl(pool: PgPool): Future[AcquireResult] {.async.} =
  if pool.closed:
    raise newException(PgPoolError, "Pool is closed")

  pool.cachedNow = Moment.now()
  let acquireStart = pool.cachedNow

  template recordAcquire() =
    pool.metrics.acquireCount.inc
    pool.metrics.acquireDuration =
      pool.metrics.acquireDuration + (Moment.now() - acquireStart)

  # Try to get an idle connection
  while pool.idle.len > 0:
    let pc = pool.idle.popFirst()
    if pc.conn.state != csReady:
      pool.metrics.closeCount.inc
      await pool.tracedClose(pc.conn)
      continue
    if pool.config.maxLifetime > ZeroDuration and
        pool.cachedNow - pc.conn.createdAt > pool.config.maxLifetime:
      pool.metrics.closeCount.inc
      await pool.tracedClose(pc.conn)
      continue
    # Health check: ping connections that have been idle too long
    if pool.config.healthCheckTimeout > ZeroDuration and
        pool.cachedNow - pc.lastUsedAt > pool.config.healthCheckTimeout:
      try:
        await pc.conn.ping(pool.config.pingTimeout)
      except CatchableError:
        pool.metrics.closeCount.inc
        await pool.tracedClose(pc.conn)
        continue
    pool.active.inc
    recordAcquire()
    return (pc.conn, false)

  # No idle connections; create new if under limit
  if pool.active < pool.config.maxSize:
    pool.active.inc
    try:
      let conn = await connect(pool.config.connConfig)
      conn.ownerPool = pool
      pool.metrics.createCount.inc
      recordAcquire()
      return (conn, true)
    except CatchableError as e:
      pool.active.dec
      raise e

  # Max connections reached; wait for one to be released
  if pool.config.maxWaiters >= 0 and pool.waiterCount >= pool.config.maxWaiters:
    raise newException(
      PgPoolError,
      "Pool acquire queue full (maxWaiters=" & $pool.config.maxWaiters & ")",
    )
  let fut = newFuture[PgConnection]("PgPool.acquire")
  let waiter = Waiter(fut: fut, cancelled: false)
  pool.waiters.addLast(waiter)
  pool.waiterCount.inc
  if pool.config.acquireTimeout > ZeroDuration:
    try:
      let conn = await fut.wait(pool.config.acquireTimeout)
      recordAcquire()
      return (conn, false)
    except AsyncTimeoutError:
      waiter.cancelled = true
      pool.waiterCount.dec
      pool.metrics.timeoutCount.inc
      # In single-threaded async, no preemption occurs between completed()
      # and read(), so this sequence is race-free. If release() completed
      # the future just before the timeout fired, return the connection
      # to the pool instead of leaking it.
      if fut.completed():
        fut.read().release()
      raise newException(PgPoolError, "Pool acquire timeout")
  else:
    let conn = await fut
    recordAcquire()
    return (conn, false)

proc acquire*(pool: PgPool): Future[PgConnection] {.async.} =
  ## Acquire a connection from the pool. Tries idle connections first (with
  ## health checks), creates a new one if under `maxSize`, or waits for a
  ## release. Raises `PgPoolError` on timeout or if the pool is closed.
  var ar: AcquireResult
  withTracing(
    pool.config.tracer,
    onPoolAcquireStart,
    onPoolAcquireEnd,
    TracePoolAcquireStartData(
      idleCount: pool.idle.len, activeCount: pool.active, maxSize: pool.config.maxSize
    ),
    TracePoolAcquireEndData,
    TracePoolAcquireEndData(conn: ar.conn, wasCreated: ar.wasCreated),
  ):
    ar = await pool.acquireImpl()
  return ar.conn

template withConnection*(pool: PgPool, conn, body: untyped) =
  ## Acquire a connection, execute `body`, then release it back to the pool.
  ## The connection is available as `conn` inside the body.
  ## If `resetQuery` is configured, session state is reset before release.
  let conn = await pool.acquire()
  try:
    body
  finally:
    await pool.resetSession(conn)
    conn.release()

proc failPendingOp(op: PendingPoolOp, e: ref CatchableError) =
  ## Fail a pending op's future if not already finished.
  case op.kind
  of popExec:
    if not op.execFut.finished:
      op.execFut.fail(e)
  of popQuery:
    if not op.queryFut.finished:
      op.queryFut.fail(e)

proc failAllPending(pool: PgPool, e: ref CatchableError) {.raises: [].} =
  ## Fail every queued op with `e`. Marked `raises: []` so the compiler
  ## proves the loop cannot leak into an `asyncSpawn`ed caller — any future
  ## change to `failPendingOp` or the underlying `Future.fail` that could
  ## raise will be caught here at compile time. `Exception` is used (not
  ## `CatchableError`) because asyncdispatch's `Future.fail` has inferred
  ## effect `Exception` via its callback chain.
  try:
    while pool.pendingOps.len > 0:
      let op = pool.pendingOps.popFirst()
      failPendingOp(op, e)
  except Exception:
    discard

proc executeBatch(
    pool: PgPool, conn: PgConnection, batch: seq[PendingPoolOp]
): Future[void] {.async.} =
  ## Execute a batch of pending operations on a single connection via pipeline.
  let batchTimeout = block:
    var t = ZeroDuration
    for op in batch:
      if op.timeout > t:
        t = op.timeout
    t
  try:
    let pipeline = newPipeline(conn)
    for op in batch:
      case op.kind
      of popExec:
        pipeline.addExec(op.sql, op.params)
      of popQuery:
        pipeline.addQuery(op.sql, op.params, op.resultFormat)
    let ir = await pipeline.executeIsolated(batchTimeout)
    for i in 0 ..< batch.len:
      let op = batch[i]
      if ir.errors[i] != nil:
        case op.kind
        of popExec:
          op.execFut.fail(ir.errors[i])
        of popQuery:
          op.queryFut.fail(ir.errors[i])
      else:
        case op.kind
        of popExec:
          op.execFut.complete(ir.results[i].commandResult)
        of popQuery:
          op.queryFut.complete(ir.results[i].queryResult)
  except CatchableError as e:
    for op in batch:
      failPendingOp(op, e)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc dispatchBatchImpl(pool: PgPool) {.async.} =
  ## Drain the pending ops queue and execute them via pipelined connections.
  pool.dispatchScheduled = false
  if pool.pendingOps.len == 0 or pool.closed:
    return

  # Drain queue (respect maxPipelineSize)
  var ops: seq[PendingPoolOp]
  let maxOps = pool.config.maxPipelineSize
  while pool.pendingOps.len > 0:
    if maxOps > 0 and ops.len >= maxOps:
      break
    ops.add(pool.pendingOps.popFirst())

  # Fast path: single op, skip pipeline overhead
  if ops.len == 1:
    let op = ops[0]
    try:
      let conn = await pool.acquire()
      try:
        case op.kind
        of popExec:
          let r = await conn.exec(op.sql, op.params, timeout = op.timeout)
          op.execFut.complete(r)
        of popQuery:
          let r = await conn.query(
            op.sql, op.params, resultFormat = op.resultFormat, timeout = op.timeout
          )
          op.queryFut.complete(r)
      finally:
        await pool.resetSession(conn)
        conn.release()
    except CatchableError as e:
      failPendingOp(op, e)
    return

  # Multi-op path: acquire connections and distribute.
  # Limit to at most half the pool to avoid starving other users.
  var conns: seq[PgConnection]
  let maxConns = min(ops.len, max(1, pool.config.maxSize div 2))
  for i in 0 ..< maxConns:
    try:
      let conn = await pool.acquire()
      conns.add(conn)
    except CatchableError:
      break

  if conns.len == 0:
    let err = newException(PgPoolError, "Failed to acquire connection for batch")
    for op in ops:
      failPendingOp(op, err)
    return

  # Distribute ops round-robin across connections
  var connOps = newSeq[seq[PendingPoolOp]](conns.len)
  for i in 0 ..< ops.len:
    connOps[i mod conns.len].add(ops[i])

  # Execute each connection's batch in parallel
  var batchFuts: seq[Future[void]]
  for ci in 0 ..< conns.len:
    if connOps[ci].len == 0:
      await pool.resetSession(conns[ci])
      conns[ci].release()
      continue
    batchFuts.add(executeBatch(pool, conns[ci], connOps[ci]))

  await allFutures(batchFuts)

proc scheduleDispatch(pool: PgPool) {.gcsafe, raises: [].} =
  ## Schedule a batch dispatch on the next event loop tick.
  if pool.dispatchScheduled:
    return
  pool.dispatchScheduled = true
  let p = pool
  proc cb() {.gcsafe, raises: [].} =
    proc run(pool: PgPool) {.async.} =
      try:
        await pool.dispatchBatchImpl()
      except CatchableError as e:
        # Fail any ops still in the queue so their futures don't hang forever.
        pool.failAllPending(e)
      # Re-schedule if there are remaining ops
      if pool.pendingOps.len > 0:
        pool.scheduleDispatch()

    {.gcsafe.}:
      try:
        asyncSpawn p.run()
      except Exception as e:
        # asyncSpawn should not raise in practice, but the compiler cannot
        # prove it.  Fail any pending ops so their futures do not hang.
        let err = newException(PgError, "Pipeline dispatch failed: " & e.msg)
        p.failAllPending(err)
        p.dispatchScheduled = false

  try:
    scheduleSoon(cb)
  except CatchableError:
    pool.dispatchScheduled = false

proc exec*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with typed parameters using a pooled connection.
  ## When `pipelined` is enabled, the operation is batched with other concurrent
  ## calls and sent in a single TCP write.
  if pool.config.pipelined:
    let fut = newFuture[CommandResult]("PgPool.exec.pipelined")
    pool.pendingOps.addLast(
      PendingPoolOp(
        kind: popExec, sql: sql, params: params, timeout: timeout, execFut: fut
      )
    )
    pool.scheduleDispatch()
    return await fut
  let conn = await pool.acquire()
  try:
    return await conn.exec(sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc query*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with typed parameters using a pooled connection.
  ## When `pipelined` is enabled, the operation is batched with other concurrent
  ## calls and sent in a single TCP write.
  if pool.config.pipelined:
    let fut = newFuture[QueryResult]("PgPool.query.pipelined")
    pool.pendingOps.addLast(
      PendingPoolOp(
        kind: popQuery,
        sql: sql,
        params: params,
        resultFormat: resultFormat,
        timeout: timeout,
        queryFut: fut,
      )
    )
    pool.scheduleDispatch()
    return await fut
  let conn = await pool.acquire()
  try:
    return await conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc queryEach*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a query with typed parameters using a pooled connection, invoking `callback` once per row.
  ##
  ## Row lifetime: the `Row` passed to `callback` is only valid for the
  ## duration of that single invocation. To retain a row beyond the callback,
  ## call `row.clone()` to get a detached copy.
  let conn = await pool.acquire()
  try:
    return await conn.queryEach(sql, params, callback, resultFormat, timeout)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc queryRowOpt*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query and return the first row, or `none` if no rows.
  let qr = await pool.query(sql, params, resultFormat, timeout)
  if qr.rowCount > 0:
    if qr.fields.len > 0 and qr.data.fields.len == 0:
      qr.data.fields = qr.fields
    return some(initRow(qr.data, 0))
  return none(Row)

proc queryRow*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Row] {.async.} =
  ## Execute a query and return the first row.
  ## Raises `PgError` if no rows are returned.
  let row =
    await pool.queryRowOpt(sql, params, resultFormat = resultFormat, timeout = timeout)
  if row.isNone:
    raise newException(PgError, "Query returned no rows")
  return row.get

proc queryValue*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Raises `PgError` if no rows or the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgError, "Query returned no rows")
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    raise newException(PgError, "Query returned NULL")
  return row.getStr(0)

proc queryValue*[T](
    pool: PgPool,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Raises `PgError` if no rows or the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgError, "Query returned no rows")
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    raise newException(PgError, "Query returned NULL")
  return row.get(0, T)

proc queryValueOpt*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[string]] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `none` if no rows or the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return none(string)
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return none(string)
  return some(row.getStr(0))

proc queryValueOpt*[T](
    pool: PgPool,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[T]] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Returns `none` if no rows or the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return none(T)
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return none(T)
  return some(row.get(0, T))

proc queryValueOrDefault*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `default` if no rows or the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return default
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return default
  return row.getStr(0)

proc queryValueOrDefault*[T](
    pool: PgPool,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Returns `default` if no rows or the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return default
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return default
  return row.get(0, T)

proc queryExists*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a query and return whether any rows exist.
  let qr = await pool.query(sql, params, timeout = timeout)
  return qr.rowCount > 0

proc queryColumn*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  ## Execute a query and return the first column of all rows as strings.
  ## Raises PgTypeError if any value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  for i in 0 ..< qr.rowCount:
    let row = initRow(qr.data, i)
    if row.isNull(0):
      raise newException(PgTypeError, "NULL value in column")
    result.add(row.getStr(0))

proc simpleQuery*(pool: PgPool, sql: string): Future[seq[QueryResult]] {.async.} =
  ## Execute one or more SQL statements via the simple query protocol using a
  ## pooled connection. See ``PgConnection.simpleQuery`` for semantics —
  ## multi-statement, no parameters, no plan cache.
  let conn = await pool.acquire()
  try:
    return await conn.simpleQuery(sql)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc simpleExec*(
    pool: PgPool, sql: string, timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute a side-effect SQL command via the simple query protocol using a
  ## pooled connection. See ``PgConnection.simpleExec`` for semantics — no
  ## parameters, no plan cache, last command tag returned.
  let conn = await pool.acquire()
  try:
    return await conn.simpleExec(sql, timeout)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc execInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined transaction with typed parameters.
  let conn = await pool.acquire()
  try:
    return await conn.execInTransaction(sql, params, timeout)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc queryInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with typed parameters.
  let conn = await pool.acquire()
  try:
    return await conn.queryInTransaction(sql, params, resultFormat, timeout)
  finally:
    await pool.resetSession(conn)
    conn.release()

proc notify*(
    pool: PgPool,
    channel: string,
    payload: string = "",
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Send a NOTIFY on `channel` with optional `payload` using a pooled connection.
  let conn = await pool.acquire()
  try:
    await conn.notify(channel, payload, timeout)
  finally:
    await pool.resetSession(conn)
    conn.release()

macro withTransaction*(pool: PgPool, args: varargs[untyped]): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction using a pooled connection.
  ## On exception, ROLLBACK is issued automatically.
  ## Using `return` inside the body is a compile-time error.
  ##
  ## Usage:
  ##   pool.withTransaction(conn):
  ##     conn.exec(...)
  ##   pool.withTransaction(conn, seconds(5)):
  ##     conn.exec(...)
  ##   pool.withTransaction(conn, TransactionOptions(isolation: ilSerializable)):
  ##     conn.exec(...)
  ##   pool.withTransaction(conn, opts, seconds(5)):
  ##     conn.exec(...)
  ##
  ## **Warning:** Inside the body, use `conn.exec(...)` / `conn.query(...)`
  ## directly — not `pool.exec(...)` / `pool.query(...)`. Pool methods acquire
  ## a separate connection, so those statements would run outside this transaction.
  var connIdent, body: NimNode
  var beginSql: NimNode
  var txTimeout: NimNode
  case args.len
  of 2:
    connIdent = args[0]
    body = args[1]
    beginSql = newStrLitNode("BEGIN")
    txTimeout = bindSym"ZeroDuration"
  of 3:
    connIdent = args[0]
    body = args[2]
    (beginSql, txTimeout) = buildTxBeginAndTimeout(args[1])
  of 4:
    connIdent = args[0]
    let opts = args[1]
    txTimeout = args[2]
    body = args[3]
    beginSql = newCall(bindSym"buildBeginSql", opts)
  else:
    error(
      "withTransaction expects (conn, body), (conn, timeout, body), (conn, opts, body), or (conn, opts, timeout, body)",
      args[0],
    )

  if hasReturnStmt(body):
    error(
      "'return' inside withTransaction is not allowed: COMMIT/ROLLBACK would be skipped",
      body,
    )

  let poolExpr = pool
  let poolSym = genSym(nskLet, "pool")
  let eSym = genSym(nskLet, "e")
  let resetSessionSym = bindSym"resetSession"
  result = quote:
    let `poolSym` = `poolExpr`
    let `connIdent` = await `poolSym`.acquire()
    try:
      discard await `connIdent`.simpleExec(`beginSql`, timeout = `txTimeout`)
      try:
        `body`
        discard await `connIdent`.simpleExec("COMMIT", timeout = `txTimeout`)
      except CatchableError as `eSym`:
        try:
          discard await `connIdent`.simpleExec("ROLLBACK", timeout = `txTimeout`)
        except CatchableError:
          discard
        raise `eSym`
    finally:
      await `resetSessionSym`(`poolSym`, `connIdent`)
      `connIdent`.release()

template withPipeline*(pool: PgPool, pipeline, body: untyped) =
  ## Acquire a connection, create a Pipeline, execute body, then release.
  ## The `pipeline` identifier is a `Pipeline` available in body.
  let conn = await pool.acquire()
  try:
    let pipeline = newPipeline(conn)
    body
  finally:
    await pool.resetSession(conn)
    conn.release()

proc close*(pool: PgPool, timeout = ZeroDuration): Future[void] {.async.} =
  ## Close the pool: stop the maintenance loop, cancel all waiters, and close
  ## all idle and active connections.
  ##
  ## When `timeout > ZeroDuration`, waits up to `timeout` for active
  ## connections to be released. Unreleased connections are closed when they
  ## are eventually returned to the pool. Without a timeout (or
  ## `ZeroDuration`), active connections are closed on release.
  pool.closed = true

  # Stop maintenance loop
  if pool.maintenanceTask != nil and not pool.maintenanceTask.finished:
    await cancelAndWait(pool.maintenanceTask)

  # Cancel all waiters
  while pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    if not waiter.cancelled:
      waiter.fut.fail(newException(PgError, "Pool closed"))
  pool.waiterCount = 0

  # Fail all pending pipeline ops
  pool.dispatchScheduled = false
  let closeErr = newException(PgError, "Pool closed")
  while pool.pendingOps.len > 0:
    let op = pool.pendingOps.popFirst()
    failPendingOp(op, closeErr)

  # Wait for active connections to drain
  if timeout > ZeroDuration and pool.active > 0:
    let deadline = Moment.now() + timeout
    while pool.active > 0 and Moment.now() < deadline:
      await sleepAsync(milliseconds(50))

  # Close all idle connections
  while pool.idle.len > 0:
    let pc = pool.idle.popFirst()
    pool.metrics.closeCount.inc
    await pool.tracedClose(pc.conn)

  # Wait for any fire-and-forget closes spawned via closeNoWait so the server
  # observes Terminate and fds are released before this proc returns. A late
  # release() from another task may push more entries while we await, so loop
  # with snapshot-and-clear to avoid discarding unfinished futures.
  while pool.pendingCloses.len > 0:
    let pending = pool.pendingCloses
    pool.pendingCloses.setLen(0)
    await allFutures(pending)
