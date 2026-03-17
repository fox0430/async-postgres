import std/[deques, macros, options]

import async_backend, pg_protocol, pg_connection, pg_client, pg_types

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
    maxWaiters*: int
      ## Max queued acquire waiters (default 0=unlimited). Rejects with PgError when full.
    resetQuery*: string
      ## SQL to execute when returning a connection to the pool (default ""=disabled).
      ## Common values: "DISCARD ALL" (full reset, recommended for PgBouncer),
      ## "DEALLOCATE ALL" (clear prepared statements only),
      ## "RESET ALL" (reset session parameters only).
      ## On failure, the connection is discarded.

  PooledConn* = object
    ## An idle connection held by the pool with its last-used timestamp.
    conn*: PgConnection
    lastUsedAt*: Moment

  PgPool* = ref object ## Connection pool that manages a set of PostgreSQL connections.
    config*: PoolConfig
    idle*: Deque[PooledConn]
    active*: int
    waiters*: Deque[Future[PgConnection]]
    closed*: bool
    maintenanceTask*: Future[void]
    cachedNow*: Moment
      ## Updated on acquire(); reused by release() to avoid extra syscalls

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
    resetQuery = "",
): PoolConfig =
  ## Create a pool configuration with sensible defaults.
  ## `minSize` idle connections are maintained; up to `maxSize` total.
  ## Set `resetQuery` to clean session state on release (e.g. "DISCARD ALL" for PgBouncer).
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
    resetQuery: resetQuery,
  )

proc closeNoWait(conn: PgConnection) =
  ## Schedule connection close without waiting. For use in non-async contexts.
  proc doClose() {.async.} =
    try:
      await conn.close()
    except CatchableError:
      discard

  asyncSpawn doClose()

proc resetSession(pool: PgPool, conn: PgConnection) {.async.} =
  ## Execute the configured reset query on a connection before returning it
  ## to the pool. On failure, closes the connection so that release() will
  ## discard it.
  if pool.config.resetQuery.len > 0 and conn.state == csReady and conn.txStatus == tsIdle:
    try:
      discard await conn.simpleExec(pool.config.resetQuery)
      conn.clearStmtCache()
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
        try:
          await pc.conn.close()
        except CatchableError:
          discard
        continue

      # Always close max-lifetime-exceeded connections (acquire rejects them anyway)
      if pool.config.maxLifetime > ZeroDuration and
          now - pc.conn.createdAt > pool.config.maxLifetime:
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
    for i in 0 ..< needed:
      if pool.closed:
        break
      try:
        let conn = await connect(pool.config.connConfig)
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
    waiters: initDeque[Future[PgConnection]](),
    closed: false,
  )

  try:
    pool.cachedNow = Moment.now()
    for i in 0 ..< cfg.minSize:
      let conn = await connect(cfg.connConfig)
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
  if pool.closed or conn.state != csReady or conn.txStatus != tsIdle:
    pool.active.dec
    conn.closeNoWait()
    return

  if pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    waiter.complete(conn)
  else:
    pool.active.dec
    pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: pool.cachedNow))

proc acquire*(pool: PgPool): Future[PgConnection] {.async.} =
  ## Acquire a connection from the pool. Tries idle connections first (with
  ## health checks), creates a new one if under `maxSize`, or waits for a
  ## release. Raises `PgError` on timeout or if the pool is closed.
  if pool.closed:
    raise newException(PgError, "Pool is closed")

  pool.cachedNow = Moment.now()

  # Try to get an idle connection
  while pool.idle.len > 0:
    let pc = pool.idle.popFirst()
    if pc.conn.state != csReady:
      try:
        await pc.conn.close()
      except CatchableError:
        discard
      continue
    if pool.config.maxLifetime > ZeroDuration and
        pool.cachedNow - pc.conn.createdAt > pool.config.maxLifetime:
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
        try:
          await pc.conn.close()
        except CatchableError:
          discard
        continue
    pool.active.inc
    return pc.conn

  # No idle connections; create new if under limit
  if pool.active < pool.config.maxSize:
    pool.active.inc
    try:
      return await connect(pool.config.connConfig)
    except CatchableError as e:
      pool.active.dec
      raise e

  # Max connections reached; wait for one to be released
  if pool.config.maxWaiters > 0 and pool.waiters.len >= pool.config.maxWaiters:
    raise newException(
      PgError, "Pool acquire queue full (maxWaiters=" & $pool.config.maxWaiters & ")"
    )
  let fut = newFuture[PgConnection]("PgPool.acquire")
  pool.waiters.addLast(fut)
  if pool.config.acquireTimeout > ZeroDuration:
    try:
      return await fut.wait(pool.config.acquireTimeout)
    except AsyncTimeoutError:
      # Remove from waiters if still queued
      var cleaned = initDeque[Future[PgConnection]]()
      for w in pool.waiters:
        if w != fut:
          cleaned.addLast(w)
      pool.waiters = cleaned
      # If release() completed the future in a race, put the connection back
      if fut.completed():
        pool.release(fut.read())
      raise newException(PgError, "Pool acquire timeout")
  else:
    return await fut

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
    params: seq[Option[seq[byte]]] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a SQL statement using a pooled connection, returning the command tag.
  let conn = await pool.acquire()
  try:
    return await conn.exec(sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc query*(
    pool: PgPool,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a SQL query using a pooled connection, returning rows.
  let conn = await pool.acquire()
  try:
    return
      await conn.query(sql, params, resultFormats = resultFormats, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc exec*(
    pool: PgPool, sql: string, params: seq[PgParam], timeout: Duration = ZeroDuration
): Future[string] {.async.} =
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
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with typed parameters using a pooled connection.
  let conn = await pool.acquire()
  try:
    return
      await conn.query(sql, params, resultFormats = resultFormats, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryOne*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query and return the first row, or `none` if no rows.
  let conn = await pool.acquire()
  try:
    return await conn.queryOne(sql, params, resultFormats, timeout)
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

proc execAffected*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a statement and return the number of affected rows.
  let conn = await pool.acquire()
  try:
    return await conn.execAffected(sql, params, timeout)
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
): Future[string] {.async.} =
  ## Execute a SQL statement via simple query protocol using a pooled connection.
  ## Returns the command tag.
  let conn = await pool.acquire()
  try:
    return await conn.simpleExec(sql, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc execInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a statement inside a pipelined BEGIN/COMMIT transaction using a pooled connection.
  let conn = await pool.acquire()
  try:
    return await conn.execInTransaction(sql, params, paramOids, paramFormats, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc execInTransaction*(
    pool: PgPool, sql: string, params: seq[PgParam], timeout: Duration = ZeroDuration
): Future[string] {.async.} =
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
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined BEGIN/COMMIT transaction using a pooled connection.
  let conn = await pool.acquire()
  try:
    return await conn.queryInTransaction(
      sql, params, paramOids, paramFormats, resultFormats, timeout
    )
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc queryInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with typed parameters.
  let conn = await pool.acquire()
  try:
    return await conn.queryInTransaction(sql, params, resultFormats, timeout)
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
  ## **Note:** Do not use `return` inside the body.
  ##
  ## Usage:
  ##   pool.withTransaction(conn):
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
    let opts = args[1]
    body = args[2]
    beginSql = newCall(bindSym"buildBeginSql", opts)
    txTimeout = bindSym"ZeroDuration"
  of 4:
    connIdent = args[0]
    let opts = args[1]
    txTimeout = args[2]
    body = args[3]
    beginSql = newCall(bindSym"buildBeginSql", opts)
  else:
    error(
      "withTransaction expects (conn, body), (conn, opts, body), or (conn, opts, timeout, body)",
      args[0],
    )

  let poolSym = pool
  let eSym = genSym(nskLet, "e")
  let resetSessionSym = bindSym"resetSession"
  result = quote:
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
  ## The `pipeline` identifier is a `var Pipeline` available in body.
  let conn = await pool.acquire()
  try:
    var pipeline = newPipeline(conn)
    body
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc close*(pool: PgPool): Future[void] {.async.} =
  ## Close the pool: stop the maintenance loop, cancel all waiters, and close
  ## all idle connections. Active connections are closed when released.
  pool.closed = true

  # Stop maintenance loop
  if pool.maintenanceTask != nil and not pool.maintenanceTask.finished:
    await cancelAndWait(pool.maintenanceTask)

  # Cancel all waiters
  while pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    waiter.fail(newException(PgError, "Pool closed"))

  # Close all idle connections
  while pool.idle.len > 0:
    let pc = pool.idle.popFirst()
    try:
      await pc.conn.close()
    except CatchableError:
      discard
