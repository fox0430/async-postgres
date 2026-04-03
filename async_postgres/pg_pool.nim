import std/[deques, macros, options]

import async_backend, pg_protocol, pg_connection, pg_types

import pg_client

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

  PgPool* = ref object ## Connection pool that manages a set of PostgreSQL connections.
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
): PoolConfig =
  ## Create a pool configuration with sensible defaults.
  ## `minSize` idle connections are maintained; up to `maxSize` total.
  ## Set `resetQuery` to clean session state on release (e.g. "DISCARD ALL" for PgBouncer).
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

proc closeNoWait(pool: PgPool, conn: PgConnection) =
  ## Schedule connection close without waiting. For use in non-async contexts.
  pool.metrics.closeCount.inc
  proc doClose() {.async.} =
    try:
      await conn.close()
    except CatchableError:
      discard

  asyncSpawn doClose()

proc resetSession*(pool: PgPool, conn: PgConnection) {.async.} =
  ## Execute the configured reset query on a connection before returning it
  ## to the pool. On failure, closes the connection so that release() will
  ## discard it.
  if pool.config.resetQuery.len > 0 and conn.state == csReady and conn.txStatus == tsIdle:
    try:
      discard await conn.simpleExec(pool.config.resetQuery)
      conn.clearStmtCache()
      conn.rowDataBuf = nil
    except CatchableError:
      try:
        await conn.close()
      except CatchableError:
        discard

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
        try:
          await pc.conn.close()
        except CatchableError:
          discard
        continue

      # Always close max-lifetime-exceeded connections (acquire rejects them anyway)
      if pool.config.maxLifetime > ZeroDuration and
          now - pc.conn.createdAt > pool.config.maxLifetime:
        pool.metrics.closeCount.inc
        try:
          await pc.conn.close()
        except CatchableError:
          discard
        continue

      # Idle timeout respects minSize
      if pool.config.idleTimeout > ZeroDuration and
          now - pc.lastUsedAt > pool.config.idleTimeout:
        let totalCount = remaining.len + pool.idle.len + pool.active
        if totalCount >= pool.config.minSize:
          pool.metrics.closeCount.inc
          try:
            await pc.conn.close()
          except CatchableError:
            discard
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
  )

  try:
    pool.cachedNow = Moment.now()
    for i in 0 ..< cfg.minSize:
      let conn = await connect(cfg.connConfig)
      pool.metrics.createCount.inc
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: pool.cachedNow))
  except CatchableError as e:
    while pool.idle.len > 0:
      let pc = pool.idle.popFirst()
      try:
        await pc.conn.close()
      except CatchableError:
        discard
    raise e

  pool.maintenanceTask = maintenanceLoop(pool)
  return pool

proc release*(pool: PgPool, conn: PgConnection) =
  ## Return a connection to the pool. If the connection is broken or in a
  ## transaction, it is closed instead. If waiters are queued, the connection
  ## is handed directly to the next waiter.
  var traceCtx: TraceContext
  if pool.config.tracer != nil and pool.config.tracer.onPoolReleaseStart != nil:
    traceCtx =
      pool.config.tracer.onPoolReleaseStart(TracePoolReleaseStartData(conn: conn))

  var wasClosed = false
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
        break dispatch
      if pool.active > 0:
        pool.active.dec
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: pool.cachedNow))

  if pool.config.tracer != nil and pool.config.tracer.onPoolReleaseEnd != nil:
    pool.config.tracer.onPoolReleaseEnd(
      traceCtx, TracePoolReleaseEndData(wasClosed: wasClosed)
    )

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
      try:
        await pc.conn.close()
      except CatchableError:
        discard
      continue
    if pool.config.maxLifetime > ZeroDuration and
        pool.cachedNow - pc.conn.createdAt > pool.config.maxLifetime:
      pool.metrics.closeCount.inc
      try:
        await pc.conn.close()
      except CatchableError:
        discard
      continue
    # Health check: ping connections that have been idle too long
    if pool.config.healthCheckTimeout > ZeroDuration and
        pool.cachedNow - pc.lastUsedAt > pool.config.healthCheckTimeout:
      try:
        await pc.conn.ping(pool.config.pingTimeout)
      except CatchableError:
        pool.metrics.closeCount.inc
        try:
          await pc.conn.close()
        except CatchableError:
          discard
        continue
    pool.active.inc
    recordAcquire()
    return (pc.conn, false)

  # No idle connections; create new if under limit
  if pool.active < pool.config.maxSize:
    pool.active.inc
    try:
      let conn = await connect(pool.config.connConfig)
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
        pool.release(fut.read())
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
    pool.release(conn)

proc exec*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with typed parameters using a pooled connection.
  let conn = await pool.acquire()
  try:
    return await conn.exec(sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc query*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with typed parameters using a pooled connection.
  let conn = await pool.acquire()
  try:
    return await conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryEach*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a query with typed parameters using a pooled connection, invoking `callback` once per row.
  let conn = await pool.acquire()
  try:
    return await conn.queryEach(sql, params, callback, resultFormat, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryOne*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query and return the first row, or `none` if no rows.
  let conn = await pool.acquire()
  try:
    return await conn.queryOne(sql, params, resultFormat, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryValue*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Raises `PgError` if no rows or the value is NULL.
  let conn = await pool.acquire()
  try:
    return await conn.queryValue(sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryValue*[T](
    pool: PgPool,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Raises `PgError` if no rows or the value is NULL.
  let conn = await pool.acquire()
  try:
    return await conn.queryValue(T, sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryValueOpt*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[string]] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `none` if no rows or the value is NULL.
  let conn = await pool.acquire()
  try:
    return await conn.queryValueOpt(sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryValueOpt*[T](
    pool: PgPool,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[T]] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Returns `none` if no rows or the value is NULL.
  let conn = await pool.acquire()
  try:
    return await conn.queryValueOpt(T, sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryValueOrDefault*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `default` if no rows or the value is NULL.
  let conn = await pool.acquire()
  try:
    return await conn.queryValueOrDefault(sql, params, default, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

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
  let conn = await pool.acquire()
  try:
    return await conn.queryValueOrDefault(T, sql, params, default, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryExists*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a query and return whether any rows exist.
  let conn = await pool.acquire()
  try:
    return await conn.queryExists(sql, params, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryColumn*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  ## Execute a query and return the first column of all rows as strings.
  let conn = await pool.acquire()
  try:
    return await conn.queryColumn(sql, params, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc simpleQuery*(pool: PgPool, sql: string): Future[seq[QueryResult]] {.async.} =
  ## Execute one or more SQL statements via simple query protocol using a pooled connection.
  let conn = await pool.acquire()
  try:
    return await conn.simpleQuery(sql)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc simpleExec*(
    pool: PgPool, sql: string, timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute a SQL statement via simple query protocol using a pooled connection.
  ## Returns the command result.
  let conn = await pool.acquire()
  try:
    return await conn.simpleExec(sql, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

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
    pool.release(conn)

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
    pool.release(conn)

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
    pool.release(conn)

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
      `poolSym`.release(`connIdent`)

template withPipeline*(pool: PgPool, pipeline, body: untyped) =
  ## Acquire a connection, create a Pipeline, execute body, then release.
  ## The `pipeline` identifier is a `Pipeline` available in body.
  let conn = await pool.acquire()
  try:
    let pipeline = newPipeline(conn)
    body
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

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

  # Wait for active connections to drain
  if timeout > ZeroDuration and pool.active > 0:
    let deadline = Moment.now() + timeout
    while pool.active > 0 and Moment.now() < deadline:
      await sleepAsync(milliseconds(50))

  # Close all idle connections
  while pool.idle.len > 0:
    let pc = pool.idle.popFirst()
    pool.metrics.closeCount.inc
    try:
      await pc.conn.close()
    except CatchableError:
      discard
