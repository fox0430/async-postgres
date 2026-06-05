import std/macros

import async_backend, pg_protocol, pg_connection, pg_types, pg_pool, pg_client

type
  ReplicaFallback* = enum
    fallbackNone ## Error when replica is unavailable
    fallbackPrimary ## Fall back to primary when replica is unavailable

  ReadFallbackReason* = enum
    ## Why `acquireRead` fell back from the replica pool to the primary pool.
    rfrReplicaUnavailable
      ## The replica `acquire()` failed before `fallbackTimeout` elapsed
      ## (e.g. saturation, acquire-queue full, connect failure).
    rfrReplicaClosed
      ## The replica pool was closed (`isClosed`). A permanently closed
      ## replica routes *every* read to the primary — observe this to detect
      ## a cluster that has silently degraded to primary-only.
    rfrReplicaTimeout
      ## The replica `acquire()` exceeded `fallbackTimeout` and was abandoned
      ## in favour of the primary.

  ReadFallbackCallback* =
    proc(reason: ReadFallbackReason, err: ref CatchableError) {.gcsafe, raises: [].}
    ## Advisory hook fired when `acquireRead` (and therefore `readConnection` /
    ## `withReadConnection`) falls back from the replica pool to the primary
    ## pool. Fires only when `fallback == fallbackPrimary`, once per read that
    ## falls back, *before* the primary acquire is attempted. `err` is the
    ## replica failure that triggered the fallback. Advisory only — routing is
    ## unchanged. Use it to detect a replica that is down or saturated before
    ## all read load silently lands on the primary. Must not raise.
    ##
    ## **Fires per fallback, not per state change.** A permanently closed or
    ## saturated replica fires this on *every* read for as long as the
    ## condition lasts (`rfrReplicaClosed` / `rfrReplicaTimeout`), which can be
    ## very high frequency under load. This suits counters/metrics, where each
    ## fallback should be counted; for logging or alerting, dedupe or
    ## rate-limit on the observer side.

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
      ## Max time to wait for *each* acquire leg of a fallback read — the
      ## replica acquire and the fallback primary acquire. `ZeroDuration`
      ## leaves each pool's own `acquireTimeout` in force.
    onReadFallback: ReadFallbackCallback
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
  ## When `fallbackPrimary` is set, this bounds *both* the replica acquire and
  ## the fallback primary acquire, so a saturated replica cannot stall a read
  ## for its full `acquireTimeout` before the fallback engages. `ZeroDuration`
  ## leaves each pool's own `acquireTimeout` in force (the replica acquire then
  ## blocks up to the replica pool's `acquireTimeout` before falling back).
  cluster.fallbackTimeout

proc onReadFallback*(cluster: PgPoolCluster): ReadFallbackCallback =
  ## The configured replica->primary read-fallback observability hook
  ## (see `ReadFallbackCallback`).
  cluster.onReadFallback

proc newPoolCluster*(
    primaryConfig: PoolConfig,
    replicaConfig: PoolConfig,
    fallback = fallbackNone,
    fallbackTimeout = ZeroDuration,
    onReadFallback: ReadFallbackCallback = nil,
): Future[PgPoolCluster] {.async.} =
  ## Create a new pool cluster with separate primary and replica pools.
  ## If `connConfig.targetSessionAttrs` is `tsaAny` (the default), it is
  ## automatically set to `tsaReadWrite` for primary and `tsaPreferStandby`
  ## for replica.
  ##
  ## With `fallback == fallbackPrimary`, `fallbackTimeout` (when > `ZeroDuration`)
  ## bounds *both* the replica and the fallback primary acquire so a saturated
  ## replica fails over promptly instead of blocking for its full
  ## `acquireTimeout`. `onReadFallback` is an optional advisory hook invoked on
  ## each replica->primary read fallback (see `ReadFallbackCallback`).
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
    onReadFallback: onReadFallback,
    closed: false,
  )

proc fireReadFallback(
    cluster: PgPoolCluster, reason: ReadFallbackReason, err: ref CatchableError
) =
  ## Route a replica->primary read fallback to the cluster's advisory hook.
  ## Nil hook is a no-op.
  if cluster.onReadFallback != nil:
    cluster.onReadFallback(reason, err)

proc acquireRead(
    cluster: PgPoolCluster
): Future[tuple[conn: PgConnection, pool: PgPool]] {.async.} =
  ## Acquire a connection for read operations. Tries the replica first; on
  ## failure, falls back to the primary when `fallback == fallbackPrimary`.
  ##
  ## When `fallbackTimeout > ZeroDuration` it bounds *both* the replica and the
  ## fallback primary acquire, so a saturated replica fails over after
  ## `fallbackTimeout` instead of blocking for the replica pool's full
  ## `acquireTimeout`. Each fallback fires `onReadFallback` (if set) before the
  ## primary acquire is attempted.
  if cluster.fallback != fallbackPrimary:
    let conn = await cluster.replica.acquire()
    return (conn, cluster.replica)

  # fallbackPrimary: bound the replica leg too (when fallbackTimeout is set) so
  # a saturated replica does not burn its full acquireTimeout before failover.
  var replicaErr: ref CatchableError
  var timedOut = false
  try:
    if cluster.fallbackTimeout > ZeroDuration:
      let conn = await cluster.replica.acquire().wait(cluster.fallbackTimeout)
      return (conn, cluster.replica)
    else:
      let conn = await cluster.replica.acquire()
      return (conn, cluster.replica)
  except AsyncTimeoutError as e:
    replicaErr = e
    timedOut = true
  except CatchableError as e:
    replicaErr = e

  let reason =
    if timedOut:
      rfrReplicaTimeout
    elif cluster.replica.isClosed:
      rfrReplicaClosed
    else:
      rfrReplicaUnavailable
  cluster.fireReadFallback(reason, replicaErr)

  if cluster.fallbackTimeout > ZeroDuration:
    try:
      let conn = await cluster.primary.acquire().wait(cluster.fallbackTimeout)
      return (conn, cluster.primary)
    except AsyncTimeoutError:
      raise newException(
        PgPoolError,
        "Pool cluster fallback acquire timeout (replica error: " & replicaErr.msg & ")",
        replicaErr,
      )
  else:
    let conn = await cluster.primary.acquire()
    return (conn, cluster.primary)

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
  # Mirror the conn/pool `withTransaction` cleanup: skip ROLLBACK on an
  # invalidated connection or once the server already ended the transaction,
  # and surface both the skip and any swallowed ROLLBACK failure through
  # `onCleanupSkipped` instead of silently discarding them.
  let cleanup = buildRollbackCleanup(connIdent, txTimeout)
  result = quote:
    let `clusterSym` = `clusterExpr`
    let `connIdent` = await `clusterSym`.primary.acquire()
    try:
      discard await `connIdent`.simpleExec(`beginSql`, timeout = `txTimeout`)
      try:
        `body`
        discard await `connIdent`.simpleExec("COMMIT", timeout = `txTimeout`)
      except CatchableError as `eSym`:
        `cleanup`
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
  # cluster mirrors its non-retry `withTransaction`: the shared
  # `onCleanupSkipped`-wired ROLLBACK from `buildRetryTxLoop`.
  let loop = buildRetryTxLoop(connIdent, retryOptsSym, beginSql, txTimeout, body)
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
  ## Close both primary and replica pools. Both closes run concurrently so
  ## their drain windows overlap and the total wait stays bounded by
  ## `timeout` instead of doubling when both pools have active connections.
  cluster.closed = true
  let
    primaryFut = cluster.primary.close(timeout)
    replicaFut = cluster.replica.close(timeout)
  var firstErr: ref CatchableError
  try:
    await primaryFut
  except CatchableError as e:
    firstErr = e
  try:
    await replicaFut
  except CatchableError as e:
    if firstErr == nil:
      firstErr = e
  if firstErr != nil:
    raise firstErr
