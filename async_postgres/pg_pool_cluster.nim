import std/options

import async_backend, pg_protocol, pg_connection, pg_types, pg_pool

import pg_client {.all.}

type
  ReplicaFallback* = enum
    rfNone ## Error when replica is unavailable
    rfPrimary ## Fall back to primary when replica is unavailable

  PgPoolCluster* = ref object
    ## Connection pool cluster with explicit read/write routing.
    ##
    ## - `read*` methods route to the replica pool (read-only queries).
    ## - `write*` methods route to the primary pool (writes, `SELECT FOR UPDATE`, etc.).
    ##
    ## For transactions, use `cluster.primaryPool.withTransaction` directly.
    primary: PgPool
    replica: PgPool
    fallback: ReplicaFallback
    closed: bool

proc primaryPool*(cluster: PgPoolCluster): PgPool =
  ## The primary (read-write) pool.
  cluster.primary

proc replicaPool*(cluster: PgPoolCluster): PgPool =
  ## The replica (read-only) pool.
  cluster.replica

proc replicaFallback*(cluster: PgPoolCluster): ReplicaFallback =
  ## The configured replica fallback behavior.
  cluster.fallback

proc isClosed*(cluster: PgPoolCluster): bool =
  ## Whether the cluster has been closed.
  cluster.closed

proc newPoolCluster*(
    primaryConfig: PoolConfig, replicaConfig: PoolConfig, fallback = rfNone
): Future[PgPoolCluster] {.async.} =
  ## Create a new pool cluster with separate primary and replica pools.
  ## If `connConfig.targetSessionAttrs` is `tsaAny` (the default), it is
  ## automatically set to `tsaReadWrite` for primary and `tsaPreferStandby`
  ## for replica.
  var pCfg = primaryConfig
  if pCfg.connConfig.targetSessionAttrs == tsaAny:
    pCfg.connConfig.targetSessionAttrs = tsaReadWrite

  var rCfg = replicaConfig
  if rCfg.connConfig.targetSessionAttrs == tsaAny:
    rCfg.connConfig.targetSessionAttrs = tsaPreferStandby

  let pPool = await newPool(pCfg)
  var rPool: PgPool
  try:
    rPool = await newPool(rCfg)
  except CatchableError as e:
    await pPool.close()
    raise e

  return
    PgPoolCluster(primary: pPool, replica: rPool, fallback: fallback, closed: false)

proc acquireRead(
    cluster: PgPoolCluster
): Future[tuple[conn: PgConnection, pool: PgPool]] {.async.} =
  ## Acquire a connection for read operations. Tries replica first;
  ## falls back to primary if configured.
  try:
    let conn = await cluster.replica.acquire()
    return (conn, cluster.replica)
  except CatchableError as e:
    if cluster.fallback == rfPrimary:
      let conn = await cluster.primary.acquire()
      return (conn, cluster.primary)
    raise e

template withReadConnection*(cluster: PgPoolCluster, conn, body: untyped) =
  ## Acquire a read connection (from replica, with optional primary fallback),
  ## execute `body`, then release.
  let (conn, pool) = await acquireRead(cluster)
  try:
    body
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

template withWriteConnection*(cluster: PgPoolCluster, conn, body: untyped) =
  ## Acquire a write connection from the primary pool, execute `body`, then release.
  let conn = await cluster.primary.acquire()
  try:
    body
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

# Read operations → replica

proc readQuery*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a read-only query with typed parameters routed to the replica pool.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryOne*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a read-only query routed to the replica pool and return the first row.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryOne(sql, params, resultFormat, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryValue*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a read-only query routed to the replica pool and return the first column of the first row.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryValue(sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a read-only query routed to the replica pool and return the first column as `T`.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryValue(T, sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryValueOpt*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[string]] {.async.} =
  ## Execute a read-only query routed to the replica pool; return `none` if no rows or NULL.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryValueOpt(sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[T]] {.async.} =
  ## Execute a read-only query routed to the replica pool; return `none` if no rows or NULL.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryValueOpt(T, sql, params, timeout = timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a read-only query routed to the replica pool; return default if no rows or NULL.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryValueOrDefault(sql, params, default, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a read-only query routed to the replica pool; return default if no rows or NULL.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryValueOrDefault(T, sql, params, default, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryExists*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a read-only query routed to the replica pool and return whether any rows exist.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryExists(sql, params, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryColumn*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  ## Execute a read-only query routed to the replica pool and return the first column of all rows.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryColumn(sql, params, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readQueryEach*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a read-only query with typed parameters routed to the replica pool,
  ## invoking `callback` once per row.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.queryEach(sql, params, callback, resultFormat, timeout)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

proc readSimpleQuery*(
    cluster: PgPoolCluster, sql: string
): Future[seq[QueryResult]] {.async.} =
  ## Execute via simple query protocol routed to the replica pool.
  let (conn, pool) = await acquireRead(cluster)
  try:
    return await conn.simpleQuery(sql)
  finally:
    await pool.resetSession(conn)
    pool.release(conn)

# Write operations → primary

proc writeExec(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a SQL statement routed to the primary pool.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.exec(sql, params, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeExec*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with typed parameters routed to the primary pool.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.exec(sql, params, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeExecAffected*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a statement routed to the primary pool and return affected row count.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.execAffected(sql, params, timeout)

proc writeQuery(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query routed to the primary pool (e.g. SELECT FOR UPDATE, INSERT RETURNING).
  let conn = await cluster.primary.acquire()
  try:
    return
      await conn.query(sql, params, resultFormats = resultFormats, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQuery*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with typed parameters routed to the primary pool
  ## (e.g. SELECT FOR UPDATE, INSERT RETURNING).
  let conn = await cluster.primary.acquire()
  try:
    return await conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryOne*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query routed to the primary pool and return the first row.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryOne(sql, params, resultFormat, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryValue*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query routed to the primary pool and return the first column of the first row.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryValue(sql, params, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query routed to the primary pool and return the first column as `T`.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryValue(T, sql, params, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryValueOpt*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[string]] {.async.} =
  ## Execute a query routed to the primary pool; return `none` if no rows or NULL.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryValueOpt(sql, params, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[T]] {.async.} =
  ## Execute a query routed to the primary pool; return `none` if no rows or NULL.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryValueOpt(T, sql, params, timeout = timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query routed to the primary pool; return default if no rows or NULL.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryValueOrDefault(sql, params, default, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query routed to the primary pool; return default if no rows or NULL.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryValueOrDefault(T, sql, params, default, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryExists*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a query routed to the primary pool and return whether any rows exist.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryExists(sql, params, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryColumn*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  ## Execute a query routed to the primary pool and return the first column of all rows.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryColumn(sql, params, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryEach*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a query with typed parameters routed to the primary pool,
  ## invoking `callback` once per row.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryEach(sql, params, callback, resultFormat, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeExecInTransaction*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement in a pipelined transaction with typed parameters, routed to primary.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.execInTransaction(sql, params, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeQueryInTransaction*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query in a pipelined transaction with typed parameters, routed to primary.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.queryInTransaction(sql, params, resultFormat, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeSimpleQuery*(
    cluster: PgPoolCluster, sql: string
): Future[seq[QueryResult]] {.async.} =
  ## Execute via simple query protocol routed to the primary pool.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.simpleQuery(sql)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeSimpleExec*(
    cluster: PgPoolCluster, sql: string, timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute via simple query protocol routed to the primary pool.
  let conn = await cluster.primary.acquire()
  try:
    return await conn.simpleExec(sql, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

proc writeNotify*(
    cluster: PgPoolCluster,
    channel: string,
    payload: string = "",
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Send a NOTIFY routed to the primary pool.
  let conn = await cluster.primary.acquire()
  try:
    await conn.notify(channel, payload, timeout)
  finally:
    await cluster.primary.resetSession(conn)
    cluster.primary.release(conn)

template withPipeline*(cluster: PgPoolCluster, pipeline, body: untyped) =
  ## Create a pipeline on the primary pool.
  cluster.primary.withPipeline(pipeline):
    body

proc close*(cluster: PgPoolCluster): Future[void] {.async.} =
  ## Close both primary and replica pools.
  cluster.closed = true
  await cluster.primary.close()
  await cluster.replica.close()
