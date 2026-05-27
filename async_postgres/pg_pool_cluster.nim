import std/macros

import async_backend, pg_protocol, pg_connection, pg_types, pg_pool, pg_client

type
  ReplicaFallback* = enum
    fallbackNone ## Error when replica is unavailable
    fallbackPrimary ## Fall back to primary when replica is unavailable

  PgPoolCluster* = ref object
    ## Connection pool cluster with explicit read/write routing.
    ##
    ## - `readConnection()` / `withReadConnection` route to the replica pool.
    ## - `writeConnection()` / `withWriteConnection` route to the primary pool.
    ##
    ## For transactions, use `cluster.withTransaction`.
    primary: PgPool
    replica: PgPool
    fallback: ReplicaFallback
    fallbackTimeout: Duration
      ## Max time to wait for a fallback acquire (ZeroDuration = use pool's own acquireTimeout)
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

proc fallbackTimeout*(cluster: PgPoolCluster): Duration =
  ## The configured fallback timeout for read operations.
  ## When `fallbackPrimary` is set and the replica acquire fails, this limits
  ## how long the fallback primary acquire may wait. `ZeroDuration` means the
  ## primary pool's own `acquireTimeout` is used as-is.
  cluster.fallbackTimeout

proc newPoolCluster*(
    primaryConfig: PoolConfig,
    replicaConfig: PoolConfig,
    fallback = fallbackNone,
    fallbackTimeout = ZeroDuration,
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

  return PgPoolCluster(
    primary: pPool,
    replica: rPool,
    fallback: fallback,
    fallbackTimeout: fallbackTimeout,
    closed: false,
  )

proc acquireRead(
    cluster: PgPoolCluster
): Future[tuple[conn: PgConnection, pool: PgPool]] {.async.} =
  ## Acquire a connection for read operations. Tries replica first;
  ## falls back to primary if configured.
  try:
    let conn = await cluster.replica.acquire()
    return (conn, cluster.replica)
  except CatchableError as e:
    if cluster.fallback == fallbackPrimary:
      if cluster.fallbackTimeout > ZeroDuration:
        try:
          let conn = await cluster.primary.acquire().wait(cluster.fallbackTimeout)
          return (conn, cluster.primary)
        except AsyncTimeoutError:
          raise newException(
            PgPoolError,
            "Pool cluster fallback acquire timeout (replica error: " & e.msg & ")",
            e,
          )
      else:
        let conn = await cluster.primary.acquire()
        return (conn, cluster.primary)
    raise e

proc readConnection*(cluster: PgPoolCluster): Future[PooledConnHandle] {.async.} =
  ## Acquire a read connection from the replica pool (with optional primary
  ## fallback per `ReplicaFallback`) wrapped in a `PooledConnHandle`.
  ##
  ## The caller must release the handle — typically via `defer: h.release()`.
  ## Forgetting to release leaks the connection until the pool is closed.
  ## `h.pool` reflects which pool actually served the acquire (replica or
  ## primary on fallback), so use it for any pool-level operations on the
  ## borrowed connection.
  ##
  ## **No session reset on release.** Prefer `withReadConnection` when the
  ## body is a single block and automatic `resetSession` is desired; use
  ## `readConnection` when the handle must outlive a single lexical scope
  ## (e.g. stored in a struct, passed across `await` boundaries through
  ## multiple helpers, or selected dynamically).
  let (conn, pool) = await cluster.acquireRead()
  return PooledConnHandle(conn: conn, pool: pool)

proc writeConnection*(cluster: PgPoolCluster): Future[PooledConnHandle] {.async.} =
  ## Acquire a write connection from the primary pool, wrapped in a
  ## `PooledConnHandle`.
  ##
  ## The caller must release the handle — typically via `defer: h.release()`.
  ## Forgetting to release leaks the connection until the pool is closed.
  ##
  ## **No session reset on release.** Prefer `withWriteConnection` when the
  ## body is a single block and automatic `resetSession` is desired; use
  ## `writeConnection` when the handle must outlive a single lexical scope.
  ## For transactional work, use `cluster.withTransaction` instead — handles
  ## are not transaction-aware.
  let conn = await cluster.primary.acquire()
  return PooledConnHandle(conn: conn, pool: cluster.primary)

template withReadConnection*(cluster: PgPoolCluster, conn, body: untyped) =
  ## Acquire a read connection (from replica, with optional primary fallback),
  ## execute `body`, then release.
  block:
    let (conn, connPool) = await acquireRead(cluster)
    try:
      body
    finally:
      await connPool.resetSession(conn)
      conn.release()

template withWriteConnection*(cluster: PgPoolCluster, conn, body: untyped) =
  ## Acquire a write connection from the primary pool, execute `body`, then release.
  block:
    let conn = await cluster.primary.acquire()
    try:
      body
    finally:
      await cluster.primary.resetSession(conn)
      conn.release()

macro withTransaction*(cluster: PgPoolCluster, args: varargs[untyped]): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction on the primary pool.
  ## On exception, ROLLBACK is issued automatically.
  ## Using `return` inside the body is a compile-time error.
  ##
  ## Usage:
  ##   cluster.withTransaction(conn):
  ##     conn.exec(...)
  ##   cluster.withTransaction(conn, seconds(5)):
  ##     conn.exec(...)
  ##   cluster.withTransaction(conn, TransactionOptions(isolation: ilSerializable)):
  ##     conn.exec(...)
  ##   cluster.withTransaction(conn, opts, seconds(5)):
  ##     conn.exec(...)
  ##
  ## **Warning:** Inside the body, run statements on the bound `conn`
  ## directly (`conn.exec(...)` / `conn.query(...)`). Calling
  ## `cluster.writeConnection()` / `cluster.readConnection()` inside the body
  ## acquires a *separate* connection from the pool, so any statements issued
  ## through that handle run outside this transaction.
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

  let clusterExpr = cluster
  let clusterSym = genSym(nskLet, "cluster")
  let eSym = genSym(nskLet, "e")
  let resetSessionSym = bindSym"resetSession"
  result = quote:
    let `clusterSym` = `clusterExpr`
    let `connIdent` = await `clusterSym`.primary.acquire()
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
      await `resetSessionSym`(`clusterSym`.primary, `connIdent`)
      `connIdent`.release()

macro withTransactionRetry*(
    cluster: PgPoolCluster, retryOpts: RetryOptions, args: varargs[untyped]
): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction on the primary pool,
  ## re-running the whole transaction when it fails with a retryable error
  ## (by default the serialization_failure / deadlock_detected SQLSTATEs — see
  ## `RetryOptions`). The primary connection is acquired once and reused across
  ## attempts. On a non-retryable error, or once `maxAttempts` is exhausted, the
  ## last exception propagates. Using `return` inside the body is a compile-time
  ## error.
  ##
  ## Usage:
  ##   cluster.withTransactionRetry(RetryOptions(maxAttempts: 3), conn):
  ##     await conn.exec(...)
  ##   cluster.withTransactionRetry(RetryOptions(...), conn, seconds(5)):
  ##     await conn.exec(...)
  ##   cluster.withTransactionRetry(RetryOptions(...), conn, opts, seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **Idempotency:** `body` runs once per attempt; non-database side effects are
  ## repeated on every retry. See `withTransaction` for the in-body `conn`
  ## warning.
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
    (beginSql, txTimeout) = buildTxBeginAndTimeout(args[1], "withTransactionRetry")
  of 4:
    connIdent = args[0]
    let opts = args[1]
    txTimeout = args[2]
    body = args[3]
    beginSql = newCall(bindSym"buildBeginSql", opts)
  else:
    error(
      "withTransactionRetry expects (retryOpts, conn, body), (retryOpts, conn, timeout, body), (retryOpts, conn, opts, body), or (retryOpts, conn, opts, timeout, body)",
      args[0],
    )

  if hasReturnStmt(body):
    error(
      "'return' inside withTransactionRetry is not allowed: COMMIT/ROLLBACK would be skipped",
      body,
    )

  let clusterExpr = cluster
  let clusterSym = genSym(nskLet, "cluster")
  let retryOptsSym = genSym(nskLet, "retryOpts")
  let resetSessionSym = bindSym"resetSession"
  # cluster mirrors its non-retry `withTransaction`: a best-effort ROLLBACK that
  # swallows errors (no `onCleanupSkipped` wiring), hence useCleanupSkipped = false.
  let loop = buildRetryTxLoop(
    connIdent, retryOptsSym, beginSql, txTimeout, body, useCleanupSkipped = false
  )
  result = quote:
    let `clusterSym` = `clusterExpr`
    let `connIdent` = await `clusterSym`.primary.acquire()
    let `retryOptsSym` = `retryOpts`
    try:
      `loop`
    finally:
      await `resetSessionSym`(`clusterSym`.primary, `connIdent`)
      `connIdent`.release()

template withPipeline*(cluster: PgPoolCluster, pipeline, body: untyped) =
  ## Create a pipeline on the primary pool.
  cluster.primary.withPipeline(pipeline):
    body

proc close*(cluster: PgPoolCluster, timeout = ZeroDuration): Future[void] {.async.} =
  ## Close both primary and replica pools.
  cluster.closed = true
  var firstErr: ref CatchableError
  try:
    await cluster.primary.close(timeout)
  except CatchableError as e:
    firstErr = e
  try:
    await cluster.replica.close(timeout)
  except CatchableError as e:
    if firstErr == nil:
      firstErr = e
  if firstErr != nil:
    raise firstErr
