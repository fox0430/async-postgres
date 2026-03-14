import std/[deques, macros, options]

import async_backend, pg_protocol, pg_connection, pg_client, pg_types

type
  PoolConfig* = object
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

  PooledConn* = object
    conn*: PgConnection
    lastUsedAt*: Moment

  PgPool* = ref object
    config*: PoolConfig
    idle*: Deque[PooledConn]
    active*: int
    waiters*: Deque[Future[PgConnection]]
    closed*: bool
    maintenanceTask*: Future[void]

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
): PoolConfig =
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
  )

proc closeNoWait(conn: PgConnection) =
  ## Schedule connection close without waiting. For use in non-async contexts.
  proc doClose() {.async.} =
    try:
      await conn.close()
    except CatchableError:
      discard

  asyncSpawn doClose()

proc isExpired(pool: PgPool, conn: PgConnection): bool =
  pool.config.maxLifetime > ZeroDuration and
    Moment.now() - conn.createdAt > pool.config.maxLifetime

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
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))
      except CatchableError:
        break # best-effort, retry next interval

proc newPool*(config: PoolConfig): Future[PgPool] {.async.} =
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
    let now = Moment.now()
    for i in 0 ..< cfg.minSize:
      let conn = await connect(cfg.connConfig)
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: now))
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
  if pool.closed or conn.state != csReady or conn.txStatus != tsIdle:
    pool.active.dec
    conn.closeNoWait()
    return

  if pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    waiter.complete(conn)
  else:
    pool.active.dec
    pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))

proc acquire*(pool: PgPool): Future[PgConnection] {.async.} =
  if pool.closed:
    raise newException(PgError, "Pool is closed")

  # Try to get an idle connection
  while pool.idle.len > 0:
    let pc = pool.idle.popFirst()
    if pc.conn.state != csReady:
      try:
        await pc.conn.close()
      except CatchableError:
        discard
      continue
    if pool.isExpired(pc.conn):
      try:
        await pc.conn.close()
      except CatchableError:
        discard
      continue
    # Health check: ping connections that have been idle too long
    if pool.config.healthCheckTimeout > ZeroDuration and
        Moment.now() - pc.lastUsedAt > pool.config.healthCheckTimeout:
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
  let conn = await pool.acquire()
  try:
    body
  finally:
    pool.release(conn)

proc exec*(
    pool: PgPool,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.exec(sql, params, timeout = timeout)
  finally:
    pool.release(conn)

proc query*(
    pool: PgPool,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  let conn = await pool.acquire()
  try:
    return
      await conn.query(sql, params, resultFormats = resultFormats, timeout = timeout)
  finally:
    pool.release(conn)

proc exec*(
    pool: PgPool, sql: string, params: seq[PgParam], timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.exec(sql, params, timeout = timeout)
  finally:
    pool.release(conn)

proc query*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  let conn = await pool.acquire()
  try:
    return
      await conn.query(sql, params, resultFormats = resultFormats, timeout = timeout)
  finally:
    pool.release(conn)

proc queryOne*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryOne(sql, params, resultFormats, timeout)
  finally:
    pool.release(conn)

proc queryValue*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryValue(sql, params, timeout = timeout)
  finally:
    pool.release(conn)

proc queryValueOrDefault*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryValueOrDefault(sql, params, default, timeout)
  finally:
    pool.release(conn)

proc queryExists*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryExists(sql, params, timeout)
  finally:
    pool.release(conn)

proc execAffected*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.execAffected(sql, params, timeout)
  finally:
    pool.release(conn)

proc queryColumn*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryColumn(sql, params, timeout)
  finally:
    pool.release(conn)

proc simpleQuery*(pool: PgPool, sql: string): Future[seq[QueryResult]] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.simpleQuery(sql)
  finally:
    pool.release(conn)

proc simpleExec*(
    pool: PgPool, sql: string, timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.simpleExec(sql, timeout)
  finally:
    pool.release(conn)

proc execInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.execInTransaction(sql, params, paramOids, paramFormats, timeout)
  finally:
    pool.release(conn)

proc execInTransaction*(
    pool: PgPool, sql: string, params: seq[PgParam], timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.execInTransaction(sql, params, timeout)
  finally:
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
  let conn = await pool.acquire()
  try:
    return await conn.queryInTransaction(
      sql, params, paramOids, paramFormats, resultFormats, timeout
    )
  finally:
    pool.release(conn)

proc queryInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryInTransaction(sql, params, resultFormats, timeout)
  finally:
    pool.release(conn)

proc notify*(
    pool: PgPool,
    channel: string,
    payload: string = "",
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  let conn = await pool.acquire()
  try:
    await conn.notify(channel, payload, timeout)
  finally:
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
      `poolSym`.release(`connIdent`)

proc close*(pool: PgPool): Future[void] {.async.} =
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
