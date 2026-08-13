import std/[deques, macros, options, importutils]

import async_backend, pg_protocol, pg_connection, pg_types, pg_client

privateAccess(PgConnection)

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
      ## Ping idle connections older than this before returning (default 5s, ZeroDuration=disabled).
      ## Applies to plaintext connections. For TLS connections, see `tlsHealthCheckTimeout`.
    tlsHealthCheckTimeout*: Duration
      ## Same as `healthCheckTimeout` but for TLS connections (default 500ms,
      ## ZeroDuration=disabled).
      ## MSG_PEEK-based liveness detection is blind to TLS alerts and to any
      ## ErrorResponse already encrypted into the TCP buffer, so TLS pools
      ## need a much shorter idle window than plaintext to stay correct.
    pingTimeout*: Duration
      ## Max time to wait for a health check ping response (default 5s, ZeroDuration=no timeout)
    acquireTimeout*: Duration
      ## Deadline for the entire `acquire` call (default 30s, ZeroDuration=no
      ## timeout). Idle health-check pings, a caller-driven connect, and the
      ## wait for a released connection all draw from this one budget, so
      ## acquire latency is bounded by ~`acquireTimeout` rather than
      ## `pingTimeout*N + connectTimeout + acquireTimeout`.
    maxWaiters*: int = -1
      ## Max queued acquire waiters (default -1=unlimited, 0=no waiting). Rejects with PgPoolError when full.
    resetQuery*: string
      ## SQL to execute when returning a connection to the pool (default ""=disabled).
      ## Common values: "DISCARD ALL" (full reset, recommended for PgBouncer),
      ## "DEALLOCATE ALL" (clear prepared statements only),
      ## "RESET ALL" (reset session parameters only).
      ## On failure, the connection is discarded.
    resetQueryTimeout*: Duration
      ## Deadline for each server round-trip in `resetSession` — covers both
      ## `pg_advisory_unlock_all` (when session locks are dirty) and
      ## `resetQuery` (default 5s, ZeroDuration=no timeout). A hung server
      ## would otherwise stall the release path and starve the pool; on
      ## timeout the connection is closed and the release proceeds.
    tracer*: PgTracer ## Optional tracer for pool-level hooks (acquire/release)
    pipelined*: bool
      ## Enable implicit query batching for pool.exec/query (default false).
      ## When enabled, concurrent calls within the same event loop tick are
      ## batched into a single TCP write per connection using per-query SYNC
      ## for error isolation.
    maxPipelineSize*: int
      ## Max operations per pipeline batch per connection (default 0=unlimited).
      ## Only used when `pipelined` is true.
    connectBackoffInitial*: Duration
      ## First backoff after a maintenance-loop connect failure (default 1s,
      ## ZeroDuration=disabled, falls back to fixed `maintenanceInterval` retries).
    connectBackoffMax*: Duration
      ## Cap for exponential backoff growth (default 60s). Doubles each failure
      ## until reaching this value.

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

  PooledConnHandle* = ref object
    ## A pool-borrowed connection paired with the pool it came from.
    ##
    ## Returned by `PgPool.acquireHandle` and `PgPoolCluster.readConnection` /
    ## `writeConnection`. The handle must be released with `release(h)` to
    ## return the connection to the pool — typically via `defer: h.release()`.
    ## Forgetting to release leaks the connection until the pool is closed.
    ##
    ## **No session reset:** unlike `withConnection` / `withReadConnection` /
    ## `withWriteConnection`, `release(h)` does **not** call `resetSession`,
    ## so a configured `resetQuery` will not run and any session-level
    ## advisory locks acquired through the typed API will not be released
    ## via `pg_advisory_unlock_all`. Use the `with*Connection` templates when
    ## you want automatic session cleanup, or call `pool.resetSession(h.conn)`
    ## yourself before `release(h)`.
    ##
    ## `pool` is the pool the connection was actually borrowed from. For
    ## `PgPoolCluster.readConnection` with `fallbackPrimary`, this can be
    ## either the replica or the primary depending on which served the
    ## acquire.
    conn*: PgConnection
    pool*: PgPool
    released: bool

  PendingOpKind = enum
    popExec
    popQuery

  PendingPoolOp = ref object
    kind: PendingOpKind
    sql: string
    params: seq[PgParam]
    paramsInline: seq[PgParamInline]
      ## Populated instead of `params` when `hasInline` is true — routes the
      ## op through the pipeline's / connection's `PgParamInline` fast path.
    hasInline: bool
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
    metrics: PoolMetrics
    pendingOps: Deque[PendingPoolOp] ## Queue for implicit pipeline batching
    dispatchScheduled: bool ## Whether a dispatch callback is pending
    pendingBackgroundTasks: seq[Future[void]]
      ## Fire-and-forget tasks tracked so `pool.close()` can drain them
      ## before returning. Populated by `closeNoWait` (connection closes)
      ## and `spawnConnectForWaiter` (FIFO-driven background connects).
    consecutiveConnectFailures: int
      ## Counter for exponential backoff in the maintenance loop. Reset to 0
      ## whenever a connect succeeds (in maintenance or acquire).
    nextConnectRetryAt: Moment
      ## Monotonic deadline before the maintenance loop is allowed to retry
      ## opening a new connection. Zero means "no pending backoff".

const bgTaskPruneThreshold = 16
  ## Sweep `pool.pendingBackgroundTasks` for finished futures only once its
  ## length reaches this threshold, so the O(n) prune is amortized across calls.

const pingBudgetFloor = milliseconds(10)
  ## Minimum remaining acquire-deadline budget required to start a
  ## health-check ping. Once a ping is on the wire, a timeout forces a close
  ## (the connection cannot be safely reused mid-ping), so a ping started
  ## with less than a realistic round trip's worth of budget would just burn
  ## a healthy connection on an acquire that is about to time out anyway.

proc initPoolConfig*(
    connConfig: ConnConfig,
    minSize = 1,
    maxSize = 10,
    idleTimeout = minutes(10),
    maxLifetime = hours(1),
    maintenanceInterval = seconds(30),
    healthCheckTimeout = seconds(5),
    tlsHealthCheckTimeout = milliseconds(500),
    pingTimeout = seconds(5),
    acquireTimeout = seconds(30),
    maxWaiters = -1,
    resetQuery = "",
    resetQueryTimeout = seconds(5),
    pipelined = false,
    maxPipelineSize = 0,
    connectBackoffInitial = seconds(1),
    connectBackoffMax = seconds(60),
): PoolConfig =
  ## Create a pool configuration with sensible defaults.
  ## `minSize` idle connections are maintained; up to `maxSize` total.
  ## Set `resetQuery` to clean session state on release (e.g. "DISCARD ALL" for PgBouncer).
  ## Set `pipelined` to true to enable implicit query batching for `pool.exec`/`pool.query`.
  ## When the maintenance loop fails to open a connection, subsequent retries
  ## use exponential backoff starting at `connectBackoffInitial`, doubling up to
  ## `connectBackoffMax`. Set `connectBackoffInitial = ZeroDuration` to disable
  ## backoff and fall back to fixed-interval retries.
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
  if connectBackoffInitial < ZeroDuration:
    raise newException(ValueError, "connectBackoffInitial must be >= 0")
  if connectBackoffMax < connectBackoffInitial:
    raise newException(ValueError, "connectBackoffMax must be >= connectBackoffInitial")
  if healthCheckTimeout < ZeroDuration:
    raise newException(ValueError, "healthCheckTimeout must be >= 0")
  if tlsHealthCheckTimeout < ZeroDuration:
    raise newException(ValueError, "tlsHealthCheckTimeout must be >= 0")
  if resetQueryTimeout < ZeroDuration:
    raise newException(ValueError, "resetQueryTimeout must be >= 0")

  PoolConfig(
    connConfig: connConfig,
    minSize: minSize,
    maxSize: maxSize,
    idleTimeout: idleTimeout,
    maxLifetime: maxLifetime,
    maintenanceInterval: maintenanceInterval,
    healthCheckTimeout: healthCheckTimeout,
    tlsHealthCheckTimeout: tlsHealthCheckTimeout,
    pingTimeout: pingTimeout,
    acquireTimeout: acquireTimeout,
    maxWaiters: maxWaiters,
    resetQuery: resetQuery,
    resetQueryTimeout: resetQueryTimeout,
    pipelined: pipelined,
    maxPipelineSize: maxPipelineSize,
    connectBackoffInitial: connectBackoffInitial,
    connectBackoffMax: connectBackoffMax,
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

proc pruneBackgroundTasks(pool: PgPool) =
  ## Sweep finished futures out of `pool.pendingBackgroundTasks`. Skipped until
  ## the seq grows past `bgTaskPruneThreshold` so the O(n) walk is amortized
  ## across calls. Uses swap-remove (constant-time delete that reorders)
  ## since order among pending tasks is irrelevant.
  if pool.pendingBackgroundTasks.len < bgTaskPruneThreshold:
    return
  var n = pool.pendingBackgroundTasks.len
  var i = 0
  while i < n:
    if pool.pendingBackgroundTasks[i].finished:
      pool.pendingBackgroundTasks[i] = pool.pendingBackgroundTasks[n - 1]
      dec n
    else:
      inc i
  pool.pendingBackgroundTasks.setLen(n)

proc closeNoWait(pool: PgPool, conn: PgConnection) =
  ## Schedule connection close without waiting. For use in non-async contexts
  ## (e.g. `release()` is synchronous). The spawned task is tracked in
  ## `pool.pendingBackgroundTasks` so `pool.close()` can await its completion
  ## for graceful shutdown.
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

  pool.pruneBackgroundTasks()
  let fut = doClose()
  pool.pendingBackgroundTasks.add(fut)
  asyncSpawn fut

proc resetSession*(pool: PgPool, conn: PgConnection) {.async.} =
  ## Reset session-affecting state on a connection before returning it to the
  ## pool. Releases any session-level advisory locks acquired through the
  ## typed API, then runs the configured `resetQuery` (if any). On failure,
  ## closes the connection so that release() will discard it.
  ##
  ## Always safe to call: returns immediately when the connection is unusable
  ## (broken / mid-transaction) or has nothing to clean up (no `resetQuery`
  ## and no advisory locks held). Callers don't need to gate on the pool
  ## config.
  ##
  ## Swallows `CatchableError` (invoked from `finally`, so a raised reset
  ## error would mask the body's original exception) but re-raises
  ## `CancelledError` — chronos requires cancellation to propagate. A `Defect`
  ## is re-raised as `PgPoolError` (the Defect as `parent`) after marking
  ## `csClosed` (the reset did not complete). Callers must chain `release()`
  ## under `try/finally` (or use `resetSessionAndRelease`) to keep pool
  ## accounting balanced on cancel.
  if conn.state != csReady or conn.txStatus != tsIdle:
    return
  if pool.config.resetQuery.len == 0 and not conn.sessionLockDirty:
    return
  try:
    if conn.sessionLockDirty:
      let t = pool.config.tracer
      if t != nil and t.onLeakedSessionLocks != nil:
        # Fire on the sticky flag rather than the counter so a raw acquire
        # released through the typed API — which decrements the counter for a
        # lock it never tracked — cannot silence the leak hook.
        t.onLeakedSessionLocks(
          TraceLeakedSessionLocksData(conn: conn, count: conn.heldSessionLocks)
        )
      discard await conn.simpleExec(
        "SELECT pg_advisory_unlock_all()", timeout = pool.config.resetQueryTimeout
      )
      conn.heldSessionLocks = 0
      conn.sessionLockDirty = false
    if pool.config.resetQuery.len > 0:
      discard await conn.simpleExec(
        pool.config.resetQuery, timeout = pool.config.resetQueryTimeout
      )
      conn.clearStmtCache()
  except CancelledError as e:
    # Split from the generic handler so cancellation propagates. Flip state
    # synchronously so the subsequent release() routes to releaseCore's discard
    # path. The actual socket close is performed by releaseCore's closeNoWait;
    # calling it here as well would double-count metrics.
    conn.state = csClosed
    raise e
  except CatchableError:
    # Defer close to releaseCore's closeNoWait to avoid double-counting metrics.
    conn.state = csClosed
  except Defect as d:
    # Incomplete reset: mark csClosed so the conn is discarded, then re-raise
    # wrapped (a raw Defect cannot cross a chronos async boundary).
    conn.state = csClosed
    raise newPoolError(pekDefectWrapped, d.msg, d)

proc computeConnectBackoff*(initial, maxDelay: Duration, failures: int): Duration =
  ## Exponential backoff for repeated connect failures: returns
  ## `initial * 2^(failures-1)` capped at `maxDelay`. Returns `ZeroDuration`
  ## when backoff is disabled (`initial == ZeroDuration`) or `failures <= 0`.
  if failures <= 0 or initial == ZeroDuration:
    return ZeroDuration
  result = initial
  for _ in 1 ..< failures:
    if result >= maxDelay:
      return maxDelay
    result = result + result
  if result > maxDelay:
    result = maxDelay

proc isAbandoned(waiter: Waiter): bool =
  ## True when the pool must not hand a connection (or a failure) to `waiter`.
  ##
  ## `cancelled` is the struct flag `settleAbandonedWaiter` sets once a
  ## timeout/external-cancel has been observed. `fut.finished()` additionally
  ## covers the window *before* that flag is set: under chronos `wait()` cancels
  ## the inner future synchronously on timeout/cancel, so the waiter sits in
  ## `pool.waiters` with `cancelled == false` but an already-finished (cancelled)
  ## future. `complete()`/`fail()` on such a future is a silent no-op, so a
  ## handoff that trusted only the flag would mark the conn borrowed, claim a
  ## delivery that never happened (leaking the conn and skipping the `active`
  ## accounting), and let the later `settleAbandonedWaiter` fall through to a
  ## second `waiterCount.dec`. Under asyncdispatch `wait()` never touches the
  ## inner future, so a still-pending waiter stays deliverable and only the flag
  ## marks abandonment.
  waiter.cancelled or waiter.fut.finished()

proc tryHandoffToWaiter(pool: PgPool, conn: PgConnection): bool =
  ## Hand `conn` to the next live (non-abandoned) waiter, if any. Returns true on
  ## delivery; the caller is responsible for accounting `pool.active` since
  ## the connection is now owned by the waiter.
  while pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    if waiter.isAbandoned:
      continue
    pool.waiterCount.dec
    conn.borrowed = true
    waiter.fut.complete(conn)
    return true
  return false

proc failLastWaiter(pool: PgPool, err: ref CatchableError): bool =
  ## Fail the most-recently-queued live (non-abandoned) waiter with `err`,
  ## scanning from the back. Returns true if a waiter was failed.
  ##
  ## FIFO fairness: charge failures to the tail, never the head. A spawn failure
  ## isn't bound to any waiter, so failing the head would hand its turn to a
  ## younger waiter once a sibling spawn or release delivers from the front.
  while pool.waiters.len > 0:
    let waiter = pool.waiters.popLast()
    if waiter.isAbandoned:
      continue
    pool.waiterCount.dec
    waiter.fut.fail(err)
    return true
  return false

proc canAttemptConnect(pool: PgPool): bool =
  ## Whether a new connection may be opened right now. Respects the
  ## exponential backoff window driven by `consecutiveConnectFailures` /
  ## `nextConnectRetryAt`.
  pool.consecutiveConnectFailures == 0 or Moment.now() >= pool.nextConnectRetryAt

proc spawnConnectForWaiter(pool: PgPool) =
  ## Open a connection asynchronously and hand it to the next queued waiter
  ## (FIFO). The caller MUST have already incremented `pool.active` as a
  ## capacity reservation before invoking this proc; the spawn rebalances
  ## that reservation based on the outcome:
  ## - delivered to a waiter: reservation stays consumed (the conn is the
  ##   waiter's active connection)
  ## - connect failed / no waiter remained / pool closed: reservation is
  ##   released via `active.dec`
  ##
  ## On connect failure the most-recently-queued (tail) waiter is failed with a
  ## `PgPoolError` wrapping the underlying error (available via `parent`) so the
  ## caller's `acquire` returns promptly rather than waiting for `acquireTimeout`.
  ## Failing the tail rather than the head preserves FIFO fairness (see
  ## `failLastWaiter`).
  ## The spawned future is tracked in
  ## `pendingBackgroundTasks` so `pool.close()` drains it before returning.
  # Bound an unset connectTimeout with maintenanceInterval so a stuck TCP
  # connect can't hold close()'s final pendingBackgroundTasks drain open
  # indefinitely. Mirrors maintenanceLoop's fallback.
  var connCfg = pool.config.connConfig
  if connCfg.connectTimeout == ZeroDuration:
    connCfg.connectTimeout = pool.config.maintenanceInterval

  proc run() {.async.} =
    var consumed = false
    try:
      let conn = await connect(connCfg)
      conn.ownerPool = pool
      pool.metrics.createCount.inc
      pool.consecutiveConnectFailures = 0
      if pool.closed:
        pool.metrics.closeCount.inc
        await pool.tracedClose(conn)
        return
      if pool.tryHandoffToWaiter(conn):
        consumed = true
        return
      # No waiter remained (all cancelled); park the conn in idle so it
      # is not lost.
      pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))
    except CancelledError:
      # Task cancellation is not a connect failure: don't bump the backoff
      # counter or fail the waiter with a pool error. Nothing cancels these
      # spawns today (close() awaits them via `pendingBackgroundTasks`), so
      # this is defensive. Swallowed rather than re-raised: this future is
      # `asyncSpawn`ed, and chronos treats a spawned task that finishes
      # cancelled as a FutureDefect. The reservation is released by
      # `finally`; the waiter stays queued, bounded by its own wait budget
      # or failed by close().
      discard
    except CatchableError as e:
      pool.consecutiveConnectFailures.inc
      let delay = computeConnectBackoff(
        pool.config.connectBackoffInitial, pool.config.connectBackoffMax,
        pool.consecutiveConnectFailures,
      )
      if delay > ZeroDuration:
        pool.nextConnectRetryAt = Moment.now() + delay
      # Only one waiter is failed here: this spawn reserved capacity for a
      # single queue slot, and other waiters may still be served by existing
      # borrowers' releases. We don't blanket-fail the queue or re-spawn for
      # siblings — `canAttemptConnect()` is now false during the backoff window,
      # so further spawns are deferred until backoff expires (then triggered by
      # the next acquire or release). The tail waiter is failed (see
      # `failLastWaiter`) to keep the head's FIFO claim on the next connection.
      #
      # Wrap in PgPoolError before failing the waiter: acquire() documents
      # PgPoolError for every failure mode, and a raw AsyncTimeoutError from
      # `connConfig.connectTimeout` would otherwise be indistinguishable from
      # the waiter's own wait-budget timeout in acquireImpl — whose handler
      # decrements `waiterCount` a second time (failLastWaiter below already
      # did) and permanently corrupts the FIFO fast-path guard.
      discard pool.failLastWaiter(
        newPoolError(pekConnectFailed, "Pool connect for waiter failed", e)
      )
    finally:
      if not consumed and pool.active > 0:
        pool.active.dec

  pool.pruneBackgroundTasks()
  let fut = run()
  pool.pendingBackgroundTasks.add(fut)
  asyncSpawn fut

proc respawnForStrandedWaiter(pool: PgPool) =
  ## Kick a background connect for the head waiter after a caller-driven
  ## acquire failure released its reserved `active` slot. Without this, a
  ## waiter that queued while the failed caller held the slot has no spawn
  ## attached (queue-time spawn was skipped because `active == maxSize`) and
  ## no borrower to release, so it would sit until its own wait budget elapses.
  if not pool.closed and pool.waiterCount > 0 and pool.active < pool.config.maxSize and
      pool.canAttemptConnect():
    pool.active.inc
    pool.spawnConnectForWaiter()

proc maintenanceLoop(pool: PgPool) {.async.} =
  while not pool.closed:
    await sleepAsync(pool.config.maintenanceInterval)
    if pool.closed:
      break

    # closeNoWait keeps the sweep yield-free: a yield here would leave healthy
    # entries popped into `remaining` but uncounted, so concurrent acquires
    # could overshoot maxSize opening replacements.
    var remaining = initDeque[PooledConn]()
    let now = Moment.now()

    while pool.idle.len > 0:
      var pc = pool.idle.popFirst()

      # Always close broken or in-transaction connections (unusable)
      if pc.conn.state != csReady or pc.conn.txStatus != tsIdle:
        pool.closeNoWait(pc.conn)
        continue

      # Always close max-lifetime-exceeded connections (acquire rejects them anyway)
      if pool.config.maxLifetime > ZeroDuration and
          now - pc.conn.createdAt > pool.config.maxLifetime:
        pool.closeNoWait(pc.conn)
        continue

      # Idle timeout respects minSize
      if pool.config.idleTimeout > ZeroDuration and
          now - pc.lastUsedAt > pool.config.idleTimeout:
        let totalCount = remaining.len + pool.idle.len + pool.active
        if totalCount >= pool.config.minSize:
          pool.closeNoWait(pc.conn)
          continue

      remaining.addLast(pc)

    pool.idle = remaining

    # Skip the replenish phase while we are inside a backoff window from a
    # recent failure. Idle pruning above still runs every interval — only the
    # connect attempts are throttled, so a backed-off pool keeps closing dead
    # idle/expired connections normally.
    if pool.consecutiveConnectFailures > 0 and Moment.now() < pool.nextConnectRetryAt:
      continue

    # Replenish to minSize (best-effort). Open all needed connections
    # concurrently via `allFutures` so they complete in the time of the
    # slowest one rather than sequentially. A failure in one connect does
    # not prevent the others from completing.
    let currentTotal = pool.idle.len + pool.active
    let needed = max(0, pool.config.minSize - currentTotal)
    if needed > 0:
      if pool.closed:
        break
      var connectFuts: seq[Future[PgConnection]]
      for i in 0 ..< needed:
        var connCfg = pool.config.connConfig
        if connCfg.connectTimeout == ZeroDuration:
          connCfg.connectTimeout = pool.config.maintenanceInterval
        connectFuts.add(connect(connCfg))
      await allFutures(connectFuts)
      for f in connectFuts:
        if not f.failed():
          let conn = f.read()
          conn.ownerPool = pool
          pool.metrics.createCount.inc
          pool.consecutiveConnectFailures = 0
          # The pool may have been closed while we awaited connect. Parking a
          # fresh conn in a closed pool leaks its socket — re-check and close.
          # closeNoWait (not `await tracedClose`) because a pending cancellation
          # would interrupt a fresh await before the close runs.
          if pool.closed:
            pool.closeNoWait(conn)
          elif pool.tryHandoffToWaiter(conn):
            pool.active.inc
          else:
            pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: now))
      for f in connectFuts:
        if f.failed():
          pool.consecutiveConnectFailures.inc
      if pool.consecutiveConnectFailures > 0:
        let delay = computeConnectBackoff(
          pool.config.connectBackoffInitial, pool.config.connectBackoffMax,
          pool.consecutiveConnectFailures,
        )
        if delay > ZeroDuration:
          pool.nextConnectRetryAt = Moment.now() + delay

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
    let now = Moment.now()
    # Open all `minSize` connections concurrently. `allFutures` waits for
    # every connect to settle (success or failure) without short-circuiting,
    # so a failure in one does not abandon the others mid-handshake — the
    # server observes a clean Terminate for each socket that did come up.
    # Successful connections are parked in `idle`; the first failure (if any)
    # is raised so the except branch closes the ones that succeeded.
    var connectFuts: seq[Future[PgConnection]]
    for i in 0 ..< cfg.minSize:
      connectFuts.add(connect(cfg.connConfig))
    await allFutures(connectFuts)
    var firstErr: ref CatchableError = nil
    for f in connectFuts:
      if f.failed():
        if firstErr == nil:
          # `connect` only raises `CatchableError`, so the stored exception is
          # always safe to downcast from `ref Exception` (asyncdispatch) /
          # `ref CatchableError` (chronos) to the typed `ref CatchableError`.
          firstErr = cast[ref CatchableError](f.error)
      else:
        let conn = f.read()
        conn.ownerPool = pool
        pool.metrics.createCount.inc
        pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: now))
    if firstErr != nil:
      raise firstErr
  except CatchableError as e:
    var closeFuts: seq[Future[void]]
    while pool.idle.len > 0:
      let pc = pool.idle.popFirst()
      closeFuts.add(pool.tracedClose(pc.conn))
    await allFutures(closeFuts)
    raise e

  pool.maintenanceTask = maintenanceLoop(pool)
  return pool

proc releaseCore(
    pool: PgPool, conn: PgConnection
): tuple[wasClosed, handedToWaiter: bool] =
  ## Core release logic shared by the traced and non-traced paths of
  ## `releaseImpl`. Returns flags describing the disposition of `conn` so
  ## the caller can report them to the tracer.
  if pool.closed or conn.state != csReady or conn.txStatus != tsIdle or
      conn.sessionLockDirty:
    if pool.active > 0:
      pool.active.dec
    pool.closeNoWait(conn)
    # A discarded conn frees an `active` slot without serving a waiter;
    # spawn a replacement so the head waiter is not stranded.
    pool.respawnForStrandedWaiter()
    return (true, false)
  # FIFO handoff: serve the head waiter directly with the released conn.
  # `active` is intentionally not decremented — the conn is still in use, just
  # by a different borrower. Waiters behind the head are covered by the spawns
  # emitted at queue-time (acquireImpl), by broken-release replacements, or by
  # `respawnForStrandedWaiter` on caller-driven connect failures.
  if pool.tryHandoffToWaiter(conn):
    return (false, true)
  if pool.active > 0:
    pool.active.dec
  pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: Moment.now()))
  return (false, false)

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
  ## Session-level advisory locks (`sessionLockDirty`) likewise force the
  ## connection to be discarded: callers who route through `resetSession`
  ## clear them ahead of time, so anything reaching here still dirty has
  ## bypassed that path and must not return to the idle queue.
  ##
  ## Double-release guard: a connection that is not currently checked out
  ## (`borrowed == false`) has already been returned to the pool — or never
  ## came from this pool's `acquire`. Returning it again would register the
  ## same connection in `idle` a second time, so two future borrowers would
  ## receive it and corrupt each other's wire protocol. Such a release is a
  ## no-op, surfaced through the tracer's `onPoolDoubleRelease` so the
  ## borrow-site bug stays observable. Cleared here (the releaser no longer
  ## holds the connection); the FIFO handoff in `releaseCore` re-sets it for
  ## the waiter that takes over.
  ##
  ## Limitation: the flag only catches a double release of an already-idle
  ## connection. If the first release handed the connection to a queued
  ## waiter, `releaseCore` re-marks it `borrowed` for that waiter, so a
  ## back-to-back second release from the original caller passes this guard
  ## and can re-route the now-in-use connection. The raw `acquire` /
  ## `release(conn)` API cannot close this gap (it carries no per-borrow
  ## token); `PooledConnHandle` and the `with*` templates are the fully safe
  ## paths.
  let tracer = pool.config.tracer
  if not conn.borrowed:
    if tracer != nil and tracer.onPoolDoubleRelease != nil:
      try:
        tracer.onPoolDoubleRelease(TracePoolDoubleReleaseData(conn: conn))
      except CatchableError:
        discard
      except Defect:
        discard
    return
  conn.borrowed = false
  if conn.sessionLockDirty and tracer != nil and tracer.onLeakedSessionLocks != nil:
    # Fire on the sticky flag rather than the counter so a raw acquire
    # released through the typed API — which decrements the counter for a
    # lock it never tracked — cannot silence the leak hook.
    # Tracer hooks are observation-only: a hook that raises must not abort
    # the release, or the pool accounting below is skipped and the slot leaks.
    try:
      tracer.onLeakedSessionLocks(
        TraceLeakedSessionLocksData(conn: conn, count: conn.heldSessionLocks)
      )
    except CatchableError:
      discard
    except Defect:
      discard
  if tracer == nil:
    discard pool.releaseCore(conn)
    return

  var traceCtx: TraceContext
  if tracer.onPoolReleaseStart != nil:
    try:
      traceCtx = tracer.onPoolReleaseStart(TracePoolReleaseStartData(conn: conn))
    except CatchableError:
      discard
    except Defect:
      discard

  let (wasClosed, handedToWaiter) = pool.releaseCore(conn)

  if tracer.onPoolReleaseEnd != nil:
    try:
      tracer.onPoolReleaseEnd(
        traceCtx,
        TracePoolReleaseEndData(wasClosed: wasClosed, handedToWaiter: handedToWaiter),
      )
    except CatchableError:
      discard
    except Defect:
      discard

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

proc release*(h: PooledConnHandle) =
  ## Return the borrowed connection to its pool. Idempotent — safe to call
  ## twice (e.g. once explicitly and once via `defer`).
  ##
  ## **Does not run `resetSession`.** Session state (`SET`/`SET LOCAL` outside
  ## a transaction, prepared statements, advisory locks acquired via the typed
  ## API, etc.) on the connection is **not** cleared before it returns to the
  ## pool, so subsequent borrowers may observe it. If that matters, use
  ## `withConnection` / `withReadConnection` / `withWriteConnection` instead,
  ## or call `await h.pool.resetSession(h.conn)` yourself before `release(h)`.
  if not h.released and h.conn != nil:
    h.released = true
    h.conn.release()

proc resetSessionAndRelease*(pool: PgPool, conn: PgConnection) {.async.} =
  ## `resetSession` + `release`, wrapped so `release()` still runs when the
  ## reset propagates `CancelledError` (chronos cancel would otherwise skip
  ## the follow-up `release()` and leak a pool slot).
  try:
    await pool.resetSession(conn)
  finally:
    conn.release()

proc settleAbandonedWaiter(pool: PgPool, waiter: Waiter) =
  ## Clean up a waiter whose acquire is being abandoned via timeout or external
  ## cancellation.
  ##
  ## Skip the decrement when the pool already settled this waiter — a handoff
  ## (`completed()`) or a `failLastWaiter`/`close` (`failed()`) already
  ## decremented `waiterCount`, and a second decrement would drive it negative,
  ## permanently disabling the FIFO fast-path guard and the `maxWaiters` bound.
  ## Such a settle can still race with this cleanup: under asyncdispatch `wait()`
  ## can't cancel its inner future, so `withTimeout` may surface AsyncTimeoutError
  ## on the same tick the future already completed *or* failed.
  ##
  ## The `else` branch decrements (and marks the waiter cancelled so a later
  ## handoff/release skips this still-queued entry). It covers a still-pending
  ## future under asyncdispatch *and* a chronos `wait()` that already *cancelled*
  ## the inner future: in the chronos case the handoff side skips finished
  ## futures (see `isAbandoned`), so it never decremented for this waiter, leaving
  ## the decrement to us. Only a completed future carries a connection to hand
  ## back (race-free: no preemption between `completed()` and `read()`).
  ##
  ## Guard the decrement with `waiterCount > 0`: `close()` resets the counter to
  ## 0 after failing waiters, and a chronos waiter whose inner future was already
  ## cancelled before close() ran may still reach this cleanup afterwards.
  if waiter.fut.completed():
    waiter.fut.read().release()
  elif waiter.fut.failed():
    discard # failLastWaiter/close already decremented; no conn was delivered
  else:
    waiter.cancelled = true
    if pool.waiterCount > 0:
      pool.waiterCount.dec

type AcquireResult = tuple[conn: PgConnection, wasCreated: bool]

proc acquireImpl(pool: PgPool): Future[AcquireResult] {.async.} =
  if pool.closed:
    raise newPoolError(pekClosed, "Pool is closed")

  let now = Moment.now()
  let acquireStart = now

  # `acquireTimeout` is a deadline for the whole acquire: idle health-check
  # pings, a caller-driven connect, and the final waiter wait all draw from
  # this one budget. Without it, acquire latency could reach
  # pingTimeout*N + connectTimeout + acquireTimeout.
  let hasDeadline = pool.config.acquireTimeout > ZeroDuration
  let deadline = acquireStart + pool.config.acquireTimeout
    # only meaningful when hasDeadline

  template remainingBudget(): Duration =
    deadline - Moment.now()

  template raiseAcquireTimeout() =
    pool.metrics.timeoutCount.inc
    raise newPoolError(pekAcquireTimeout, "Pool acquire timeout")

  template raisePoolClosed() =
    raise newPoolError(pekClosed, "Pool is closed")

  template recordAcquire() =
    pool.metrics.acquireCount.inc
    pool.metrics.acquireDuration =
      pool.metrics.acquireDuration + (Moment.now() - acquireStart)

  # FIFO fairness: skip the idle / new-conn fast paths when waiters are
  # already queued, otherwise a fresh caller would jump the queue. Cancelled
  # waiters don't count (they're swept lazily by release/handoff), so the
  # `waiterCount` field — which tracks only live waiters — is the guard.
  if pool.waiterCount == 0:
    # Try to get an idle connection
    while pool.idle.len > 0:
      let pc = pool.idle.popFirst()
      if pc.conn.state != csReady or pc.conn.socketHasFin():
        # closeNoWait: avoid an await point where a cancellation could be
        # swallowed by tracedClose and leak the next acquired conn (see ping guard).
        pool.closeNoWait(pc.conn)
        continue
      if pool.config.maxLifetime > ZeroDuration and
          now - pc.conn.createdAt > pool.config.maxLifetime:
        pool.closeNoWait(pc.conn)
        continue
      # Health check: ping connections that have been idle too long.
      # TLS connections use the tighter `tlsHealthCheckTimeout` window because
      # the MSG_PEEK probe above cannot see TLS alerts or any ErrorResponse
      # already encrypted into the TCP buffer — only a real round-trip can.
      let idleThreshold =
        if pc.conn.sslEnabled:
          pool.config.tlsHealthCheckTimeout
        else:
          pool.config.healthCheckTimeout
      if idleThreshold > ZeroDuration and now - pc.lastUsedAt > idleThreshold:
        var pingBudget = pool.config.pingTimeout
        if hasDeadline:
          let rem = remainingBudget()
          # Don't start a ping unless a realistic round trip's worth of
          # budget remains (`pingBudgetFloor`, or the user's own tighter
          # `pingTimeout`): a ping doomed to time out would discard a
          # connection that may well be healthy. Put it back for the next
          # acquirer instead and report the timeout.
          let pingFloor =
            if pingBudget > ZeroDuration:
              min(pingBudget, pingBudgetFloor)
            else:
              pingBudgetFloor
          if rem < pingFloor:
            pool.idle.addFirst(pc)
            raiseAcquireTimeout()
          pingBudget =
            if pingBudget == ZeroDuration:
              rem
            else:
              min(pingBudget, rem)
        # Count the conn as active across the ping so a concurrent acquire
        # can't overshoot maxSize while it is off `idle`.
        pool.active.inc
        try:
          await pc.conn.ping(pingBudget)
        except CancelledError as e:
          # CancelledError is a CatchableError; the generic handler below would
          # swallow it, letting acquire run on and return a connection the
          # departed canceller leaks. The interrupted ping leaves the protocol
          # out of sync, so close the popped conn (closeNoWait, since the pending
          # cancellation would interrupt a fresh await) and re-raise.
          pool.active.dec
          pool.closeNoWait(pc.conn)
          pool.respawnForStrandedWaiter()
          raise e
        except CatchableError:
          pool.active.dec
          pool.closeNoWait(pc.conn)
          continue
        # A close() during the ping didn't drain the popped conn — discard it.
        if pool.closed:
          pool.active.dec
          pool.closeNoWait(pc.conn)
          raisePoolClosed()
        pc.conn.borrowed = true
        recordAcquire()
        return (pc.conn, false)
      if pool.closed:
        pool.closeNoWait(pc.conn)
        raisePoolClosed()
      pool.active.inc
      pc.conn.borrowed = true
      recordAcquire()
      return (pc.conn, false)

    # No idle connections; create new if under limit
    if pool.active < pool.config.maxSize:
      let connCfg = pool.config.connConfig
      var rem = ZeroDuration
      if hasDeadline:
        rem = remainingBudget()
        if rem <= ZeroDuration:
          raiseAcquireTimeout()
      pool.active.inc
      var newConn: PgConnection
      try:
        if hasDeadline:
          # Bound the whole connect (across multi-host failover) by the acquire
          # budget: per-host `connectTimeout` alone lets total wait reach
          # `connectTimeout * hosts`. Under asyncdispatch the orphan close
          # mirrors `attemptHostTimed`'s handling.
          let attempt = connect(connCfg)
          when hasAsyncDispatch:
            newConn = await attempt.wait(
              rem,
              onOrphan = proc(fut: Future[PgConnection]) =
                if fut.completed():
                  # Track the orphan close so pool.close()'s drain awaits it.
                  pool.pruneBackgroundTasks()
                  let closeFut = (
                    proc() {.async.} =
                      try:
                        let orphan = fut.read()
                        if orphan != nil:
                          await orphan.close()
                      except CatchableError:
                        discard
                  )()
                  pool.pendingBackgroundTasks.add(closeFut)
                  asyncSpawn closeFut
              ,
            )
          else:
            newConn = await attempt.wait(rem)
        else:
          newConn = await connect(connCfg)
      except CancelledError as e:
        # Cancellation (e.g. a caller's wait()-style deadline) must propagate
        # unwrapped so the canceller's machinery sees it.
        pool.active.dec
        pool.respawnForStrandedWaiter()
        raise e
      except CatchableError as e:
        pool.active.dec
        pool.respawnForStrandedWaiter()
        # `perform()` aggregates per-host errors into `PgConnectionError`, so
        # the exception type alone can't tell an acquire timeout from a run of
        # connect failures — the deadline is authoritative. The 1ms slack
        # absorbs asyncdispatch's timer imprecision (it can fire hundreds of
        # microseconds early).
        if hasDeadline and remainingBudget() <= milliseconds(1):
          pool.metrics.timeoutCount.inc
          raise newPoolError(pekAcquireTimeout, "Pool acquire timeout", e)
        raise newPoolError(pekConnectFailed, "Pool connect failed", e)
      pool.metrics.createCount.inc
      # A successful caller-driven connect signals the DB is reachable —
      # let the maintenance loop resume immediate replenishment.
      pool.consecutiveConnectFailures = 0
      if pool.closed:
        pool.active.dec
        pool.closeNoWait(newConn)
        raisePoolClosed()
      newConn.ownerPool = pool
      newConn.borrowed = true
      recordAcquire()
      return (newConn, true)

  # Either max connections are reached or waiters are queued ahead of us;
  # queue up and wait for delivery.
  if pool.config.maxWaiters >= 0 and pool.waiterCount >= pool.config.maxWaiters:
    raise newPoolError(
      pekQueueFull,
      "Pool acquire queue full (maxWaiters=" & $pool.config.maxWaiters & ")",
    )
  # Compute the remaining budget before queueing; whatever the idle
  # health-check pings consumed comes out of the waiter wait below.
  var waitBudget = ZeroDuration
  if hasDeadline:
    waitBudget = remainingBudget()
    if waitBudget <= ZeroDuration:
      raiseAcquireTimeout()
  # If `close()` finished during any await above, its waiter sweep is done and
  # nothing will fail a waiter enqueued after it — `await fut` would hang.
  if pool.closed:
    raisePoolClosed()
  let fut = newFuture[PgConnection]("PgPool.acquire")
  let waiter = Waiter(fut: fut, cancelled: false)
  pool.waiters.addLast(waiter)
  pool.waiterCount.inc

  # FIFO fairness: if the pool still has spare capacity but we're queued
  # behind others, open an out-of-band connection so the front waiter is
  # served promptly. Without this, the queue would only drain when an
  # existing borrower releases — broken-conn releases (which discard
  # instead of handing off) could otherwise leave waiters stalled even
  # though `active < maxSize`.
  if pool.active < pool.config.maxSize and pool.canAttemptConnect():
    pool.active.inc
    pool.spawnConnectForWaiter()

  if hasDeadline:
    try:
      let conn = await fut.wait(waitBudget)
      recordAcquire()
      return (conn, false)
    except AsyncTimeoutError:
      pool.metrics.timeoutCount.inc
      pool.settleAbandonedWaiter(waiter)
      raise newPoolError(pekAcquireTimeout, "Pool acquire timeout")
    except CancelledError as e:
      # External cancellation (e.g. a caller's `wait()`-style deadline such as
      # pool.withTransactionDeadline or a cluster fallback timeout) can land
      # here too: `fut.wait()` propagates cancellation into `fut`. Same cleanup
      # as the no-deadline branch below.
      pool.settleAbandonedWaiter(waiter)
      raise e
  else:
    try:
      let conn = await fut
      recordAcquire()
      return (conn, false)
    except CancelledError as e:
      # The caller's `wait()`-style timeout (e.g. pool.withTransactionDeadline)
      # cancelled this acquire. Mirrors the AsyncTimeoutError path above.
      pool.settleAbandonedWaiter(waiter)
      raise e

proc acquire*(pool: PgPool): Future[PgConnection] {.async.} =
  ## Acquire a connection from the pool. Tries idle connections first (with
  ## health checks), creates a new one if under `maxSize`, or waits for a
  ## release. Raises `PgPoolError` on every failure mode: acquire timeout,
  ## pool closed, waiter queue full, or a failed connect attempt — for
  ## connect failures the underlying error (e.g. `PgConnectionError`) is
  ## preserved as the `parent` of the raised `PgPoolError`. Use the `kind`
  ## field (`PoolErrorKind`) to distinguish the failure mode programmatically.
  if pool.config.tracer == nil:
    let ar = await pool.acquireImpl()
    return ar.conn

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

proc acquireHandle*(pool: PgPool): Future[PooledConnHandle] {.async.} =
  ## Acquire a connection wrapped in a `PooledConnHandle`. Equivalent to
  ## `acquire`, but the returned handle pairs the connection with its owning
  ## pool and provides an idempotent `release(h)`.
  ##
  ## The caller is responsible for releasing — typically via
  ## `defer: h.release()`. Forgetting to release leaks the connection.
  ## `release(h)` does **not** run `resetSession`; prefer `withConnection`
  ## when automatic session cleanup is desired.
  let conn = await pool.acquire()
  return PooledConnHandle(conn: conn, pool: pool)

when hasChronos:
  # chronos's `async` requires the closure's raises to be listed; the
  # asyncdispatch `async` macro emits a bare `except:`, which infers
  # `Exception` for every async proc and would reject the annotation.
  type RunAndReleaseBody[T] =
    proc(): Future[T] {.closure, gcsafe, raises: [CatchableError, CancelledError].}

else:
  type RunAndReleaseBody[T] = proc(): Future[T] {.closure, gcsafe.}

proc runAndReleaseImpl[T](
    pool: PgPool, conn: PgConnection, body: RunAndReleaseBody[T]
): Future[T] {.async.} =
  ## asyncdispatch-safe `acquire → body → resetSessionAndRelease`: the body
  ## error is captured and the connection released outside `finally`, so a
  ## failing release can't replace the body's in-flight exception. Release
  ## failures are swallowed (the op's result is already valid, and a reset-path
  ## Defect leaves the connection unusable — the reset's send leaves `csBusy`,
  ## so `releaseCore` discards it), except a release-path `CancelledError`,
  ## which is always re-raised: the caller is cancelling the whole operation.
  ## A body Defect is re-raised wrapped in `PgPoolError` (Defect as `parent`),
  ## since chronos re-raises raw Defects from continuations eagerly.
  ##
  ## `runAndRelease` releases exactly the passed `conn`; `T = void` bodies
  ## (e.g. `notify`) work via `when` guards.
  var bodyErr: ref CatchableError = nil
  var bodyDefect: ref Defect = nil
  var bodyFut: Future[T]
  when T isnot void:
    var res: T
  try:
    bodyFut = body()
    when T isnot void:
      res = await bodyFut
    else:
      await bodyFut
  except CatchableError as e:
    bodyErr = e
  except Defect as d:
    bodyDefect = d
  try:
    await pool.resetSessionAndRelease(conn)
  except CancelledError as e:
    raise e
  except Defect:
    # Same-frame Defect from the release path: swallowed like the arm below —
    # never shadow the body error (see the doc comment).
    discard
  except CatchableError:
    discard
  if bodyErr != nil:
    raise bodyErr
  if bodyDefect != nil:
    raise newPoolError(pekDefectWrapped, bodyDefect.msg, bodyDefect)
  when T isnot void:
    return res

template runAndRelease*[T](
    pool: PgPool, conn: PgConnection, body: Future[T]
): Future[T] =
  ## `acquire → body → resetSessionAndRelease` for pooled operations. `body`
  ## is evaluated lazily inside a try so a synchronous raise from its async
  ## prelude (parameter encoding, guards, …) still releases `conn`; see
  ## `runAndReleaseImpl` for the error-handling semantics.
  runAndReleaseImpl(
    pool,
    conn,
    proc(): Future[T] {.closure.} =
      body,
  )

proc buildReleaseAndReraise*(releaseCall, bodyErrSym, bodyDefectSym: NimNode): NimNode =
  ## Build the release-and-re-raise block shared by the pooled `with*` macros
  ## and the cluster's `withReadConnection` / `withWriteConnection`. The caller
  ## captures the body's error into `bodyErrSym` / `bodyDefectSym`, then
  ## splices this block to run `releaseCall` (an `await`ed
  ## `resetSessionAndRelease`) outside a `finally`, so a failing release can't
  ## mask the body error. A release failure is re-raised only when the body
  ## succeeded — the body error verbatim, its Defect wrapped in `PgPoolError`
  ## (the Defect is `parent`). A release-path `CancelledError` is always
  ## re-raised: the caller is cancelling the whole macro.
  let releaseErrSym = genSym(nskLet, "releaseErr")
  let releaseDefectSym = genSym(nskLet, "releaseDefect")
  let cancelSym = genSym(nskLet, "cancel")
  result = quote:
    try:
      await `releaseCall`
    except CancelledError as `cancelSym`:
      # Always re-raise, body error or not: the caller is cancelling the whole
      # macro and cancellation must propagate.
      raise `cancelSym`
    except Defect as `releaseDefectSym`:
      # Same-frame Defect from the release path: wrap like the body Defect
      # (see runAndReleaseImpl), unless it would shadow the body error.
      if `bodyErrSym` == nil and `bodyDefectSym` == nil:
        raise newPoolError(pekDefectWrapped, `releaseDefectSym`.msg, `releaseDefectSym`)
    except CatchableError as `releaseErrSym`:
      # Never shadow a body error with a release failure.
      if `bodyErrSym` == nil and `bodyDefectSym` == nil:
        raise `releaseErrSym`
    if `bodyErrSym` != nil:
      raise `bodyErrSym`
    if `bodyDefectSym` != nil:
      # Wrap the Defect (see runAndReleaseImpl): chronos re-raises raw
      # Defects eagerly.
      raise newPoolError(pekDefectWrapped, `bodyDefectSym`.msg, `bodyDefectSym`)

macro withConnection*(pool: PgPool, conn, body: untyped): untyped =
  ## Acquire a connection, execute `body`, then release it back to the pool.
  ## The connection is available as `conn` inside the body.
  ## `resetSession` runs before release, so a configured `resetQuery` is
  ## applied and any session-level advisory locks acquired through the typed
  ## API are released via `pg_advisory_unlock_all`.
  ##
  ## Release runs outside `finally` (a failing `await` in an asyncdispatch
  ## `finally` masks the body error), so `return` / `break` / `continue`
  ## escaping the body are rejected at compile time.
  checkNoBodyEscape(body, "withConnection", "the connection release")
  let poolSym = genSym(nskLet, "pool")
  let bodyErrSym = genSym(nskVar, "bodyErr")
  let bodyDefectSym = genSym(nskVar, "bodyDefect")
  let releaseCall = quote:
    `poolSym`.resetSessionAndRelease(`conn`)
  let releaseBlock = buildReleaseAndReraise(releaseCall, bodyErrSym, bodyDefectSym)
  result = quote:
    let `poolSym` = `pool`
    let `conn` = await `poolSym`.acquire()
    var `bodyErrSym`: ref CatchableError = nil
    var `bodyDefectSym`: ref Defect = nil
    try:
      `body`
    except CatchableError as e:
      `bodyErrSym` = e
    except Defect as d:
      `bodyDefectSym` = d
    `releaseBlock`
    checkNoBodyEscapePost(
      block:
        `body`,
      "withConnection",
      "the connection release",
    )

proc failPendingOp(op: PendingPoolOp, e: ref CatchableError) =
  ## Fail a pending op's future if not already finished.
  case op.kind
  of popExec:
    if not op.execFut.finished:
      op.execFut.fail(e)
  of popQuery:
    if not op.queryFut.finished:
      op.queryFut.fail(e)

proc completePendingOp(op: PendingPoolOp, r: CommandResult) =
  # Caller may have cancelled via `.wait(dur)` before dispatch resolved;
  # completing a finished future raises FutureDefect and crashes the process.
  if not op.execFut.finished:
    op.execFut.complete(r)

proc completePendingOp(op: PendingPoolOp, r: QueryResult) =
  if not op.queryFut.finished:
    op.queryFut.complete(r)

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

proc batchTimeout(batch: seq[PendingPoolOp]): Duration =
  ## Timeout applied to a pipelined batch, derived from its ops.
  ##
  ## `ZeroDuration` means "no timeout". A batch runs as a single pipeline, so if
  ## any op opted out of a deadline the whole batch must be unlimited —
  ## otherwise the max() below would clamp that op to a sibling's finite
  ## timeout. Only when every op has a finite timeout does the batch run under
  ## the largest of them.
  result = ZeroDuration
  for op in batch:
    if op.timeout == ZeroDuration:
      return ZeroDuration
    if op.timeout > result:
      result = op.timeout

proc splitBatchBudget(
    finiteLen, unlimitedLen, cap: int
): tuple[finite, unlimited: int] =
  ## Divide a connection budget between the two timeout classes of a mixed
  ## batch. The split is proportional to each class's op count, and every
  ## present class is guaranteed at least one connection so neither is starved
  ## behind the other. With `cap == 1` both still get one (total two), which a
  ## pool that small can spare for the duration of a batch.
  let total = finiteLen + unlimitedLen
  let finite = max(1, min(cap - 1, cap * finiteLen div total))
  result = (finite, max(1, cap - finite))

proc executeBatch(
    pool: PgPool, conn: PgConnection, batch: seq[PendingPoolOp]
): Future[void] {.async.} =
  ## Execute a batch of pending operations on a single connection via pipeline.
  let timeout = batchTimeout(batch)
  # No re-raise: every op's outcome is reported via `failPendingOp` below.
  try:
    let pipeline = newPipeline(conn)
    for op in batch:
      case op.kind
      of popExec:
        if op.hasInline:
          pipeline.addExec(op.sql, op.paramsInline)
        else:
          pipeline.addExec(op.sql, op.params)
      of popQuery:
        if op.hasInline:
          pipeline.addQuery(op.sql, op.paramsInline, op.resultFormat)
        else:
          pipeline.addQuery(op.sql, op.params, op.resultFormat)
    let ir = await pipeline.executeIsolated(timeout)
    for i in 0 ..< batch.len:
      let op = batch[i]
      if ir.errors[i] != nil:
        failPendingOp(op, ir.errors[i])
      else:
        case op.kind
        of popExec:
          completePendingOp(op, ir.results[i].commandResult)
        of popQuery:
          completePendingOp(op, ir.results[i].queryResult)
  except CatchableError as e:
    for op in batch:
      failPendingOp(op, e)
  except Defect as d:
    for op in batch:
      failPendingOp(op, newPoolError(pekDefectWrapped, d.msg, d))
  try:
    await pool.resetSessionAndRelease(conn)
  except CancelledError as e:
    raise e
  except Defect:
    # Swallowed like the arm below: ops are already settled and the raise
    # would be dropped by the caller's allFutures anyway.
    discard
  except CatchableError:
    discard

proc dispatchHomogeneous(
    pool: PgPool, ops: seq[PendingPoolOp], maxConns: int
) {.async.} =
  ## Execute a set of ops that share one timeout class (all finite, or all
  ## unlimited). A pipeline runs under a single timeout, so a batch must stay
  ## single-class: mixing a finite op with an unlimited one would force one
  ## onto the other's deadline (see `batchTimeout`). A lone op skips the
  ## pipeline; otherwise ops are spread round-robin over up to `maxConns`
  ## connections and run in parallel.
  if ops.len == 0:
    return

  # Fast path: single op, skip pipeline overhead
  if ops.len == 1:
    let op = ops[0]
    try:
      let conn = await pool.acquire()
      # asyncdispatch-safe release: capture the body error and release outside
      # `finally`, so a failing release can't mask it.
      var bodyErr: ref CatchableError = nil
      var bodyDefect: ref Defect = nil
      try:
        case op.kind
        of popExec:
          var r: CommandResult
          if op.hasInline:
            r = await conn.exec(op.sql, op.paramsInline, timeout = op.timeout)
          else:
            r = await conn.exec(op.sql, op.params, timeout = op.timeout)
          completePendingOp(op, r)
        of popQuery:
          var r: QueryResult
          if op.hasInline:
            r = await conn.query(
              op.sql,
              op.paramsInline,
              resultFormat = op.resultFormat,
              timeout = op.timeout,
            )
          else:
            r = await conn.query(
              op.sql, op.params, resultFormat = op.resultFormat, timeout = op.timeout
            )
          completePendingOp(op, r)
      except CatchableError as e:
        bodyErr = e
      except Defect as d:
        bodyDefect = d
      try:
        await pool.resetSessionAndRelease(conn)
      except CancelledError as e:
        raise e
      except Defect:
        # Same-frame Defect from the release path: swallowed like the arm
        # below — never shadow the body error.
        discard
      except CatchableError:
        discard
      if bodyErr != nil:
        raise bodyErr
      if bodyDefect != nil:
        raise bodyDefect
    except CancelledError as e:
      # Cancellation must propagate, but fail the op first: it has left
      # `pendingOps`, so the generic `failAllPending` cannot reach it.
      failPendingOp(op, e)
      raise e
    except CatchableError as e:
      failPendingOp(op, e)
    except Defect as d:
      # Wrap the Defect so the op's future fails instead of hanging.
      failPendingOp(op, newPoolError(pekDefectWrapped, d.msg, d))
    return

  # Multi-op path: acquire connections and distribute.
  var conns: seq[PgConnection]
  var acquireErr: ref Exception
  let nConns = min(ops.len, max(1, maxConns))
  for i in 0 ..< nConns:
    try:
      let conn = await pool.acquire()
      conns.add(conn)
    except CatchableError as e:
      acquireErr = e
      break

  if conns.len == 0:
    let err =
      newPoolError(pekBatchFailed, "Failed to acquire connection for batch", acquireErr)
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
      await pool.resetSessionAndRelease(conns[ci])
      continue
    batchFuts.add(executeBatch(pool, conns[ci], connOps[ci]))

  await allFutures(batchFuts)

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

  # Segregate finite-timeout ops from unlimited ones. A pipelined batch runs
  # under a single timeout, so a finite op sharing a batch with an unlimited
  # sibling would be widened to no deadline at all (see `batchTimeout`).
  # Running each class as its own batch keeps every op under its own bound.
  var finiteOps, unlimitedOps: seq[PendingPoolOp]
  for op in ops:
    if op.timeout > ZeroDuration:
      finiteOps.add(op)
    else:
      unlimitedOps.add(op)

  # Cap total concurrency at half the pool to avoid starving other users; when
  # both classes are present, split that budget between them.
  let cap = max(1, pool.config.maxSize div 2)
  if finiteOps.len == 0 or unlimitedOps.len == 0:
    await pool.dispatchHomogeneous(ops, cap)
  else:
    let (finiteCap, unlimitedCap) =
      splitBatchBudget(finiteOps.len, unlimitedOps.len, cap)
    await allFutures(
      @[
        pool.dispatchHomogeneous(finiteOps, finiteCap),
        pool.dispatchHomogeneous(unlimitedOps, unlimitedCap),
      ]
    )

proc failPendingAndUnschedule(pool: PgPool, err: ref CatchableError) {.raises: [].} =
  ## Recovery for the "no dispatch is coming" case: fail every queued op
  ## and clear the scheduled flag so a later caller can re-arm.
  pool.failAllPending(err)
  pool.dispatchScheduled = false

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
        # prove it.
        let err = newException(PgError, "Pipeline dispatch failed: " & e.msg)
        p.failPendingAndUnschedule(err)

  try:
    scheduleSoon(cb)
  except CatchableError as e:
    let err = newException(PgError, "Pipeline dispatch schedule failed: " & e.msg)
    pool.failPendingAndUnschedule(err)

proc exec*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with typed parameters using a pooled connection.
  ## When `pipelined` is enabled, the operation is batched with other concurrent
  ## calls and sent in a single TCP write.
  ##
  ## In pipelined mode a batch runs under a single timeout, so a finite
  ## `timeout` may be widened to the largest finite timeout among the ops it is
  ## batched with. An op with no timeout (`ZeroDuration`) is batched separately
  ## and stays unlimited.
  if pool.config.pipelined:
    if pool.closed:
      raise newPoolError(pekClosed, "Pool is closed")
    let fut = newFuture[CommandResult]("PgPool.exec.pipelined")
    pool.pendingOps.addLast(
      PendingPoolOp(
        kind: popExec, sql: sql, params: params, timeout: timeout, execFut: fut
      )
    )
    pool.scheduleDispatch()
    return await fut
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.exec(sql, params, timeout = timeout))

proc exec*(
    pool: PgPool,
    sql: string,
    params: seq[PgParamInline],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with heap-alloc-free inline parameters using a pooled
  ## connection. Batches through the pipelined path when `pipelined` is enabled;
  ## see the `seq[PgParam]` overload for the batch timeout semantics.
  if pool.config.pipelined:
    if pool.closed:
      raise newPoolError(pekClosed, "Pool is closed")
    let fut = newFuture[CommandResult]("PgPool.exec.pipelined")
    pool.pendingOps.addLast(
      PendingPoolOp(
        kind: popExec,
        sql: sql,
        paramsInline: params,
        hasInline: true,
        timeout: timeout,
        execFut: fut,
      )
    )
    pool.scheduleDispatch()
    return await fut
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.exec(sql, params, timeout = timeout))

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
  ##
  ## In pipelined mode a batch runs under a single timeout, so a finite
  ## `timeout` may be widened to the largest finite timeout among the ops it is
  ## batched with. An op with no timeout (`ZeroDuration`) is batched separately
  ## and stays unlimited.
  if pool.config.pipelined:
    if pool.closed:
      raise newPoolError(pekClosed, "Pool is closed")
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
  return await pool.runAndRelease(
    conn, conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  )

proc query*(
    pool: PgPool,
    sql: string,
    params: seq[PgParamInline],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with heap-alloc-free inline parameters using a pooled
  ## connection. Batches through the pipelined path when `pipelined` is enabled;
  ## see the `seq[PgParam]` overload for the batch timeout semantics.
  if pool.config.pipelined:
    if pool.closed:
      raise newPoolError(pekClosed, "Pool is closed")
    let fut = newFuture[QueryResult]("PgPool.query.pipelined")
    pool.pendingOps.addLast(
      PendingPoolOp(
        kind: popQuery,
        sql: sql,
        paramsInline: params,
        hasInline: true,
        resultFormat: resultFormat,
        timeout: timeout,
        queryFut: fut,
      )
    )
    pool.scheduleDispatch()
    return await fut
  let conn = await pool.acquire()
  return await pool.runAndRelease(
    conn, conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  )

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
  return await pool.runAndRelease(
    conn, conn.queryEach(sql, params, callback, resultFormat, timeout)
  )

proc queryRowOpt*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query and return the first row, or `none` if no rows.
  let conn = await pool.acquire()
  return
    await pool.runAndRelease(conn, conn.queryRowOpt(sql, params, resultFormat, timeout))

proc queryRow*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Row] {.async.} =
  ## Execute a query and return the first row.
  ## Raises `PgNoRowsError` if no rows are returned.
  let conn = await pool.acquire()
  return
    await pool.runAndRelease(conn, conn.queryRow(sql, params, resultFormat, timeout))

proc queryValue*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Raises `PgNoRowsError` if no rows are returned, or `PgNullError` if the value is NULL.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.queryValue(sql, params, timeout))

proc queryValue*[T](
    pool: PgPool,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Raises `PgNoRowsError` if no rows are returned, or `PgNullError` if the value is NULL.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.queryValue(T, sql, params, timeout))

proc queryValueOpt*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[string]] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `none` if no rows or the value is NULL.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.queryValueOpt(sql, params, timeout))

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
  return await pool.runAndRelease(conn, conn.queryValueOpt(T, sql, params, timeout))

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
  return await pool.runAndRelease(
    conn, conn.queryValueOrDefault(sql, params, default, timeout)
  )

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
  return await pool.runAndRelease(
    conn, conn.queryValueOrDefault(T, sql, params, default, timeout)
  )

proc queryValueOrDefault*[T](
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`,
  ## inferring `T` from `default`.
  ## Returns `default` if no rows or the value is NULL.
  let conn = await pool.acquire()
  return await pool.runAndRelease(
    conn, conn.queryValueOrDefault(sql, params, default, timeout)
  )

proc queryExists*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a query and return whether any rows exist.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.queryExists(sql, params, timeout))

proc queryColumn*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  ## Execute a query and return the first column of all rows as strings.
  ## Raises `PgNullError` if any value is NULL.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.queryColumn(sql, params, timeout))

proc simpleQuery*(
    pool: PgPool, sql: string, timeout: Duration = ZeroDuration
): Future[seq[QueryResult]] {.async.} =
  ## Execute one or more SQL statements via the simple query protocol using a
  ## pooled connection. See ``PgConnection.simpleQuery`` for semantics —
  ## multi-statement, no parameters, no plan cache.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.simpleQuery(sql, timeout))

proc simpleExec*(
    pool: PgPool, sql: string, timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute a side-effect SQL command via the simple query protocol using a
  ## pooled connection. See ``PgConnection.simpleExec`` for semantics — no
  ## parameters, no plan cache, last command tag returned.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.simpleExec(sql, timeout))

proc execInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined transaction with typed parameters.
  let conn = await pool.acquire()
  return await pool.runAndRelease(conn, conn.execInTransaction(sql, params, timeout))

proc execInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    opts: TransactionOptions,
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined transaction with options
  ## (isolation / access mode / deferrable) applied to the BEGIN.
  let conn = await pool.acquire()
  return
    await pool.runAndRelease(conn, conn.execInTransaction(sql, params, opts, timeout))

proc queryInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with typed parameters.
  let conn = await pool.acquire()
  return await pool.runAndRelease(
    conn, conn.queryInTransaction(sql, params, resultFormat, timeout)
  )

proc queryInTransaction*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    opts: TransactionOptions,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with options
  ## (isolation / access mode / deferrable) applied to the BEGIN.
  let conn = await pool.acquire()
  return await pool.runAndRelease(
    conn, conn.queryInTransaction(sql, params, opts, resultFormat, timeout)
  )

proc notify*(
    pool: PgPool,
    channel: string,
    payload: string = "",
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Send a NOTIFY on `channel` with optional `payload` using a pooled connection.
  let conn = await pool.acquire()
  await pool.runAndRelease(conn, conn.notify(channel, payload, timeout))

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
  ##
  ## **Timeout semantics:** The `timeout` argument applies *per-call* to
  ## BEGIN, COMMIT, and ROLLBACK only — it does **not** bound `body` operations
  ## or `pool.acquire()`. Worst-case wall-clock = acquire(unbounded) +
  ## BEGIN(≤timeout) + body(unbounded) + COMMIT(≤timeout)
  ## \[+ ROLLBACK(≤timeout) on failure\]. Use `withTransactionDeadline` for a
  ## single wall-clock deadline covering acquire, BEGIN, body, and COMMIT.
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

  checkNoBodyEscape(body, "withTransaction", "COMMIT/ROLLBACK")

  let poolExpr = pool
  let poolSym = genSym(nskLet, "pool")
  let eSym = genSym(nskLet, "e")
  let dSym = genSym(nskLet, "d")
  let cancelSym = genSym(nskLet, "cancel")
  let bodyErrSym = genSym(nskVar, "bodyErr")
  let bodyDefectSym = genSym(nskVar, "bodyDefect")
  let resetSessionAndReleaseSym = bindSym"resetSessionAndRelease"
  let csReadySym = bindSym"csReady"
  let csClosedSym = bindSym"csClosed"
  let cancelNoWaitSym = bindSym"cancelNoWait"
  let bodyCleanup = buildRollbackCleanup(connIdent, txTimeout)
  let releaseCall = quote:
    `resetSessionAndReleaseSym`(`poolSym`, `connIdent`)
  let releaseBlock = buildReleaseAndReraise(releaseCall, bodyErrSym, bodyDefectSym)
  # asyncdispatch-safe release (see `buildReleaseAndReraise`).
  result = quote:
    let `poolSym` = `poolExpr`
    let `connIdent` = await `poolSym`.acquire()
    var `bodyErrSym`: ref CatchableError = nil
    var `bodyDefectSym`: ref Defect = nil
    try:
      discard await `connIdent`.simpleExec(`beginSql`, timeout = `txTimeout`)
      try:
        `body`
        discard await `connIdent`.simpleExec("COMMIT", timeout = `txTimeout`)
      except CancelledError as `cancelSym`:
        # Skip ROLLBACK on cancel (a fresh await would just re-cancel), but
        # abort server-side via CancelRequest and mark csClosed so the server
        # tx does not linger holding locks and the conn is discarded by
        # release() instead of silently reused.
        if `connIdent`.state notin {`csReadySym`, `csClosedSym`}:
          `cancelNoWaitSym`(`connIdent`)
          `connIdent`.state = `csClosedSym`
        raise `cancelSym`
      except CatchableError as `eSym`:
        `bodyCleanup`
        raise `eSym`
      except Defect as `dSym`:
        `bodyCleanup`
        `bodyDefectSym` = `dSym`
    except CatchableError as `eSym`:
      `bodyErrSym` = `eSym`
    except Defect as `dSym`:
      `bodyDefectSym` = `dSym`
    `releaseBlock`
    checkNoBodyEscapePost(
      block:
        `body`,
      "withTransaction",
      "COMMIT/ROLLBACK",
    )

macro withTransactionRetry*(
    pool: PgPool, retryOpts: RetryOptions, args: varargs[untyped]
): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction on a pooled connection,
  ## re-running the whole transaction when it fails with a retryable error
  ## (by default the serialization_failure / deadlock_detected SQLSTATEs — see
  ## `RetryOptions`). The pooled connection is acquired once and reused across
  ## attempts; a ROLLBACK between attempts returns it to a clean `tsIdle` state.
  ## On a non-retryable error, or once `maxAttempts` is exhausted, the last
  ## exception propagates. Using `return` inside the body is a compile-time error.
  ##
  ## Usage:
  ##   pool.withTransactionRetry(RetryOptions(maxAttempts: 3), conn):
  ##     await conn.exec(...)
  ##   pool.withTransactionRetry(RetryOptions(...), conn, seconds(5)):
  ##     await conn.exec(...)
  ##   pool.withTransactionRetry(RetryOptions(...), conn, TransactionOptions(isolation: ilSerializable)):
  ##     await conn.exec(...)
  ##   pool.withTransactionRetry(RetryOptions(...), conn, opts, seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **Idempotency:** `body` runs once per attempt, so it must be safe to re-run;
  ## non-database side effects are repeated on every retry. See `withTransaction`
  ## for the timeout semantics and the in-body `conn.exec(...)` warning.
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

  checkNoBodyEscape(body, "withTransactionRetry", "COMMIT/ROLLBACK")

  let poolExpr = pool
  let poolSym = genSym(nskLet, "pool")
  let retryOptsSym = genSym(nskLet, "retryOpts")
  let bodyErrSym = genSym(nskVar, "bodyErr")
  let bodyDefectSym = genSym(nskVar, "bodyDefect")
  let eSym = genSym(nskLet, "e")
  let dSym = genSym(nskLet, "d")
  let resetSessionAndReleaseSym = bindSym"resetSessionAndRelease"
  let loop = buildRetryTxLoop(connIdent, retryOptsSym, beginSql, txTimeout, body)
  let releaseCall = quote:
    `resetSessionAndReleaseSym`(`poolSym`, `connIdent`)
  let releaseBlock = buildReleaseAndReraise(releaseCall, bodyErrSym, bodyDefectSym)
  # Evaluate retryOpts before acquire(): a raise would otherwise leak the
  # pooled connection. Release is asyncdispatch-safe (see `buildReleaseAndReraise`).
  result = quote:
    let `poolSym` = `poolExpr`
    let `retryOptsSym` = `retryOpts`
    let `connIdent` = await `poolSym`.acquire()
    var `bodyErrSym`: ref CatchableError = nil
    var `bodyDefectSym`: ref Defect = nil
    try:
      `loop`
    except CatchableError as `eSym`:
      `bodyErrSym` = `eSym`
    except Defect as `dSym`:
      `bodyDefectSym` = `dSym`
    `releaseBlock`
    checkNoBodyEscapePost(
      block:
        `body`,
      "withTransactionRetry",
      "COMMIT/ROLLBACK",
    )

macro withTransactionDeadline*(pool: PgPool, args: varargs[untyped]): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction bounded by a single
  ## wall-clock deadline that covers `pool.acquire()`, BEGIN, the body, and
  ## COMMIT together.
  ##
  ## Usage:
  ##   pool.withTransactionDeadline(conn, seconds(5)):
  ##     await conn.exec(...)
  ##   pool.withTransactionDeadline(conn, TransactionOptions(...), seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **On deadline exceeded:** `PgTimeoutError` is raised. Under chronos the
  ## body future is cancelled: a connection with a request in flight is
  ## invalidated (server-side CancelRequest, dropped on release), while one
  ## that unwinds cleanly (grace ROLLBACK succeeded) returns to the pool
  ## healthy. Under asyncdispatch the still-running body keeps the connection;
  ## it is invalidated via `invalidateOnTimeout` and dropped on its eventual
  ## release. If the timeout fires while still waiting for `acquire()`, the
  ## waiter remains queued (cancelled best-effort) until the underlying
  ## acquire future settles; this is unavoidable under asyncdispatch.
  ##
  ## **Edge case — acquire-completion race:** under asyncdispatch the only
  ## preemption point is `await`, but the outer `wait` may still fire its
  ## timeout on the same tick the body finishes. To avoid a false-positive
  ## `PgTimeoutError` in that window, the timeout handler checks
  ## `bodyFut.completed()` (success only) and, when true, returns normally
  ## instead of reporting a timeout. A still-running or failed body falls
  ## through to the standard invalidate-and-raise path. This narrows but
  ## does not eliminate the race — a `PgTimeoutError` from this macro
  ## still does **not** guarantee the transaction was rolled back if the
  ## body was mid-flight when the timer won; it only guarantees the
  ## *caller* gave up waiting.
  ##
  ## **On other body exceptions:** ROLLBACK runs with `rollbackGrace`. A body
  ## `Defect` is re-raised wrapped in `PgPoolError` (Defect as `parent`).
  ##
  ## **Warning:** Inside the body, use `conn.exec(...)` / `conn.query(...)`
  ## directly — not `pool.exec(...)` / `pool.query(...)`. Pool methods acquire
  ## a separate connection, so those statements would run outside this transaction.
  var connIdent, body: NimNode
  var beginSql: NimNode
  var deadline: NimNode
  case args.len
  of 3:
    # (conn, deadline, body)
    connIdent = args[0]
    deadline = args[1]
    body = args[2]
    beginSql = newStrLitNode("BEGIN")
  of 4:
    # (conn, opts, deadline, body)
    connIdent = args[0]
    beginSql = newCall(bindSym"buildBeginSql", args[1])
    deadline = args[2]
    body = args[3]
  else:
    error(
      "withTransactionDeadline expects (conn, deadline, body) or (conn, opts, deadline, body)",
      args[0],
    )

  checkNoBodyEscape(body, "withTransactionDeadline", "COMMIT/ROLLBACK")

  let poolExpr = pool
  let poolSym = genSym(nskLet, "pool")
  let eSym = genSym(nskLet, "e")
  let dSym = genSym(nskLet, "d")
  let cancelSym = genSym(nskLet, "cancel")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "poolTxBodyDeadline")
  let bodyFutSym = genSym(nskLet, "bodyFut")
  let connOptSym = genSym(nskVar, "connOpt")
  let releasedSym = genSym(nskVar, "released")
  let cancelledSym = genSym(nskLet, "cancelled")
  let bodyErrSym = genSym(nskVar, "bodyErr")
  let bodyDefectSym = genSym(nskVar, "bodyDefect")
  let releaseErrSym = genSym(nskLet, "releaseErr")
  let resetSessionAndReleaseSym = bindSym"resetSessionAndRelease"
  let csReadySym = bindSym"csReady"
  let csClosedSym = bindSym"csClosed"
  let cancelNoWaitSym = bindSym"cancelNoWait"
  let timeoutErrSym = bindSym"AsyncTimeoutError"
  let waitSym = bindSym"wait"
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let invalidateSym = bindSym"invalidateOnTimeout"
  let bodyCleanup = buildRollbackCleanup(connIdent, graceSym)

  # asyncdispatch-safe release (see `buildReleaseAndReraise`): `releasedSym`
  # is set on release, success or failure, as the old try/finally did.
  result = quote:
    let `poolSym` = `poolExpr`
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    var `connOptSym` = none(PgConnection)
    var `releasedSym` = false
    proc `bodyFnSym`(): Future[void] {.async.} =
      let `connIdent` = await `poolSym`.acquire()
      `connOptSym` = some(`connIdent`)
      var `bodyErrSym`: ref CatchableError = nil
      var `bodyDefectSym`: ref Defect = nil
      try:
        discard await `connIdent`.simpleExec(
          `beginSql`, timeout = `remainingSym`(`deadlineMomentSym`)
        )
        try:
          `body`
          discard await `connIdent`.simpleExec(
            "COMMIT", timeout = `remainingSym`(`deadlineMomentSym`)
          )
        except CancelledError as `cancelSym`:
          # Skip ROLLBACK on body-cancel; the outer handler aborts server-side.
          raise `cancelSym`
        except CatchableError as `eSym`:
          `bodyCleanup`
          raise `eSym`
        except Defect as `dSym`:
          `bodyCleanup`
          `bodyDefectSym` = `dSym`
      except CancelledError as `cancelledSym`:
        # Cancelled mid-request (chronos deadline): abort it server-side and
        # mark csClosed so release() discards the conn instead of reusing it.
        if `connIdent`.state notin {`csReadySym`, `csClosedSym`}:
          `cancelNoWaitSym`(`connIdent`)
          `connIdent`.state = `csClosedSym`
        `bodyErrSym` = `cancelledSym`
      except CatchableError as `eSym`:
        `bodyErrSym` = `eSym`
      except Defect as `dSym`:
        `bodyDefectSym` = `dSym`
      try:
        await `resetSessionAndReleaseSym`(`poolSym`, `connIdent`)
      except CancelledError as `cancelSym`:
        # Already released: mark so the outer handler doesn't double-invalidate.
        `releasedSym` = true
        raise `cancelSym`
      except Defect as `dSym`:
        `releasedSym` = true
        # Wrap the Defect (see runAndReleaseImpl) unless it shadows the body error.
        if `bodyErrSym` == nil and `bodyDefectSym` == nil:
          raise newPoolError(pekDefectWrapped, `dSym`.msg, `dSym`)
      except CatchableError as `releaseErrSym`:
        # Already released: re-raise only when the body succeeded.
        `releasedSym` = true
        if `bodyErrSym` == nil and `bodyDefectSym` == nil:
          raise `releaseErrSym`
      `releasedSym` = true
      if `bodyErrSym` != nil:
        raise `bodyErrSym`
      if `bodyDefectSym` != nil:
        # Wrap the Defect (see runAndReleaseImpl): chronos re-raises raw
        # Defects eagerly.
        raise newPoolError(pekDefectWrapped, `bodyDefectSym`.msg, `bodyDefectSym`)
      checkNoBodyEscapePost(
        block:
          `body`,
        "withTransactionDeadline",
        "COMMIT/ROLLBACK",
      )

    let `bodyFutSym` = `bodyFnSym`()
    try:
      await `waitSym`(`bodyFutSym`, `totalDurSym`)
    except `timeoutErrSym`:
      # Use `completed()` (= finished and *not* failed), not `finished()`:
      # under chronos, `wait` cancels the inner future before raising
      # `AsyncTimeoutError`, leaving it in finished+failed (CancelledError)
      # state. Treating that as "done" would re-raise CancelledError
      # instead of the intended PgTimeoutError. Only a genuine success-on-
      # the-same-tick should suppress the timeout report.
      if `bodyFutSym`.completed():
        discard
      elif `connOptSym`.isNone:
        raise newException(
          PgTimeoutError, "withTransactionDeadline (pool): acquire timed out"
        )
      elif `releasedSym`:
        # chronos: cancellation already ran bodyFn's release (set
        # `releasedSym`), so the conn is back in the pool and must not be
        # invalidated here.
        raise newException(PgTimeoutError, "withTransactionDeadline (pool) exceeded")
      else:
        # asyncdispatch: bodyFn still owns the conn; invalidateOnTimeout marks
        # it csClosed (and raises PgTimeoutError) so release() will discard it.
        `connOptSym`.get.`invalidateSym`("withTransactionDeadline (pool) exceeded")

macro withTransactionRetryDeadline*(
    pool: PgPool, retryOpts: RetryOptions, args: varargs[untyped]
): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction on a pooled connection,
  ## bounded by a single wall-clock deadline that is **shared across all retry
  ## attempts** (covering `acquire()`, BEGIN, body, and COMMIT of every attempt),
  ## re-running the whole transaction on a retryable error while budget remains.
  ##
  ## Usage:
  ##   pool.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), conn, seconds(5)):
  ##     await conn.exec(...)
  ##   pool.withTransactionRetryDeadline(RetryOptions(...), conn, TransactionOptions(...), seconds(5)):
  ##     await conn.exec(...)
  ##
  ## Each attempt acquires a *fresh* connection (the previous one is released by
  ## the per-attempt `finally`), so a failed/poisoned connection is dropped by
  ## the pool's health check rather than reused. Worst-case wall-clock is
  ## `deadline`, not `maxAttempts * deadline`.
  ##
  ## **On deadline exceeded:** `PgTimeoutError` is raised — never retried; the
  ## in-flight connection is handled as in `withTransactionDeadline`.
  ## **On a retryable error:**
  ## ROLLBACK runs with `rollbackGrace` and the transaction is retried if budget
  ## remains. See `withTransactionDeadline` for the acquire-race / `completed()`
  ## rationale and the in-body `conn.exec(...)` warning. **Idempotency:** `body`
  ## runs once per attempt; non-database side effects repeat. Using `return`
  ## inside the body is a compile-time error.
  ##
  ## **On a `Defect` raised by the body:** re-raised wrapped in `PgPoolError`
  ## (`parent` = Defect), never retried; see `withTransactionDeadline`.
  var connIdent, body: NimNode
  var beginSql: NimNode
  var deadline: NimNode
  case args.len
  of 3:
    connIdent = args[0]
    deadline = args[1]
    body = args[2]
    beginSql = newStrLitNode("BEGIN")
  of 4:
    connIdent = args[0]
    beginSql = newCall(bindSym"buildBeginSql", args[1])
    deadline = args[2]
    body = args[3]
  else:
    error(
      "withTransactionRetryDeadline expects (retryOpts, conn, deadline, body) or (retryOpts, conn, opts, deadline, body)",
      args[0],
    )

  checkNoBodyEscape(body, "withTransactionRetryDeadline", "COMMIT/ROLLBACK")

  let poolExpr = pool
  let poolSym = genSym(nskLet, "pool")
  let retryOptsSym = genSym(nskLet, "retryOpts")
  let eSym = genSym(nskLet, "e")
  let dSym = genSym(nskLet, "d")
  let cancelSym = genSym(nskLet, "cancel")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "poolTxBodyRetryDeadline")
  let connOptSym = genSym(nskVar, "connOpt")
  let releasedSym = genSym(nskVar, "released")
  let cancelledSym = genSym(nskLet, "cancelled")
  let bodyErrSym = genSym(nskVar, "bodyErr")
  let bodyDefectSym = genSym(nskVar, "bodyDefect")
  let releaseErrSym = genSym(nskLet, "releaseErr")
  let resetSessionAndReleaseSym = bindSym"resetSessionAndRelease"
  let csReadySym = bindSym"csReady"
  let csClosedSym = bindSym"csClosed"
  let cancelNoWaitSym = bindSym"cancelNoWait"
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let invalidateSym = bindSym"invalidateOnTimeout"
  # The pool variant acquires a fresh connection per attempt, so its cleanup
  # (ROLLBACK + release) happens inside bodyFn; the outer loop adds no cleanup
  # and omits the conn-state retry gate (connForStateCheck = nil).
  # See withTransactionDeadline for the released/isNone branch rationale.
  let timeoutElse = quote:
    if `connOptSym`.isNone:
      raise newException(
        PgTimeoutError, "withTransactionRetryDeadline (pool): acquire timed out"
      )
    elif `releasedSym`:
      raise newException(PgTimeoutError, "withTransactionRetryDeadline (pool) exceeded")
    else:
      `connOptSym`.get.`invalidateSym`("withTransactionRetryDeadline (pool) exceeded")
  let loop = buildRetryDeadlineLoop(
    bodyFnSym,
    retryOptsSym,
    deadlineMomentSym,
    connForStateCheck = nil,
    timeoutElse = timeoutElse,
    catchableCleanup = newStmtList(),
  )
  let bodyCleanup = buildRollbackCleanup(connIdent, graceSym)
  result = quote:
    let `poolSym` = `poolExpr`
    let `retryOptsSym` = `retryOpts`
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    var `connOptSym` = none(PgConnection)
    var `releasedSym` = false
    proc `bodyFnSym`(): Future[void] {.async.} =
      `connOptSym` = none(PgConnection)
      `releasedSym` = false
      let `connIdent` = await `poolSym`.acquire()
      `connOptSym` = some(`connIdent`)
      # asyncdispatch-safe release (see withTransactionDeadline).
      var `bodyErrSym`: ref CatchableError = nil
      var `bodyDefectSym`: ref Defect = nil
      try:
        discard await `connIdent`.simpleExec(
          `beginSql`, timeout = `remainingSym`(`deadlineMomentSym`)
        )
        try:
          `body`
          discard await `connIdent`.simpleExec(
            "COMMIT", timeout = `remainingSym`(`deadlineMomentSym`)
          )
        except CancelledError as `cancelSym`:
          # See withTransactionDeadline — skip ROLLBACK on body-cancel.
          raise `cancelSym`
        except CatchableError as `eSym`:
          `bodyCleanup`
          raise `eSym`
        except Defect as `dSym`:
          `bodyCleanup`
          `bodyDefectSym` = `dSym`
      except CancelledError as `cancelledSym`:
        # Cancelled mid-request (chronos deadline): abort it server-side and
        # mark csClosed so release() discards the conn instead of reusing it.
        if `connIdent`.state notin {`csReadySym`, `csClosedSym`}:
          `cancelNoWaitSym`(`connIdent`)
          `connIdent`.state = `csClosedSym`
        `bodyErrSym` = `cancelledSym`
      except CatchableError as `eSym`:
        `bodyErrSym` = `eSym`
      except Defect as `dSym`:
        `bodyDefectSym` = `dSym`
      try:
        await `resetSessionAndReleaseSym`(`poolSym`, `connIdent`)
      except CancelledError as `cancelSym`:
        # Already released: mark so the outer handler doesn't double-invalidate.
        `releasedSym` = true
        raise `cancelSym`
      except Defect as `dSym`:
        `releasedSym` = true
        # Wrap the Defect (see runAndReleaseImpl) unless it shadows the body error.
        if `bodyErrSym` == nil and `bodyDefectSym` == nil:
          raise newPoolError(pekDefectWrapped, `dSym`.msg, `dSym`)
      except CatchableError as `releaseErrSym`:
        # Already released: re-raise only when the body succeeded.
        `releasedSym` = true
        if `bodyErrSym` == nil and `bodyDefectSym` == nil:
          raise `releaseErrSym`
      `releasedSym` = true
      if `bodyErrSym` != nil:
        raise `bodyErrSym`
      if `bodyDefectSym` != nil:
        # Wrap the Defect (see runAndReleaseImpl): chronos re-raises raw
        # Defects eagerly.
        raise newPoolError(pekDefectWrapped, `bodyDefectSym`.msg, `bodyDefectSym`)
      checkNoBodyEscapePost(
        block:
          `body`,
        "withTransactionRetryDeadline",
        "COMMIT/ROLLBACK",
      )

    `loop`

macro withPipeline*(pool: PgPool, pipeline, body: untyped): untyped =
  ## Acquire a connection, create a Pipeline, execute body, then release.
  ## `pipeline` and the acquired `conn` are available in the body.
  ##
  ## Body `return` / `break` / `continue` escaping to an enclosing loop are
  ## rejected at compile time (see `withConnection`).
  checkNoBodyEscape(body, "withPipeline", "the connection release")
  let poolSym = genSym(nskLet, "pool")
  let connId = ident("conn")
  let bodyErrSym = genSym(nskVar, "bodyErr")
  let bodyDefectSym = genSym(nskVar, "bodyDefect")
  let releaseCall = quote:
    `poolSym`.resetSessionAndRelease(`connId`)
  let releaseBlock = buildReleaseAndReraise(releaseCall, bodyErrSym, bodyDefectSym)
  result = quote:
    block:
      let `poolSym` = `pool`
      let `connId` = await `poolSym`.acquire()
      let `pipeline` = newPipeline(`connId`)
      var `bodyErrSym`: ref CatchableError = nil
      var `bodyDefectSym`: ref Defect = nil
      try:
        `body`
      except CatchableError as e:
        `bodyErrSym` = e
      except Defect as d:
        `bodyDefectSym` = d
      `releaseBlock`
      checkNoBodyEscapePost(
        block:
          `body`,
        "withPipeline",
        "the connection release",
      )

proc close*(pool: PgPool, timeout = ZeroDuration): Future[void] {.async.} =
  ## Close the pool: stop the maintenance loop, cancel all waiters, and close
  ## all idle and active connections.
  ##
  ## When `timeout > ZeroDuration`, `timeout` is a single deadline shared by
  ## both the active-connection drain and the background-task drain. Pending
  ## background spawns still in flight when the deadline elapses are
  ## cancelled so close() returns promptly. Without a timeout (or
  ## `ZeroDuration`), active connections are closed on release and the
  ## background drain waits unbounded.
  pool.closed = true

  let hasDeadline = timeout > ZeroDuration
  let closeDeadline =
    if hasDeadline:
      Moment.now() + timeout
    else:
      Moment.now()

  # Stop maintenance loop
  if pool.maintenanceTask != nil and not pool.maintenanceTask.finished:
    await cancelAndWait(pool.maintenanceTask)

  # Cancel all waiters. PgPoolError (not bare PgError) so a waiter failed
  # by close() matches acquire()'s documented error contract. Skip waiters whose
  # future is already finished (e.g. a chronos timeout/cancel that hasn't yet run
  # `settleAbandonedWaiter`): `fail()` on a finished future is a no-op on chronos
  # and a Defect on asyncdispatch.
  while pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    if not waiter.isAbandoned:
      waiter.fut.fail(newPoolError(pekClosed, "Pool closed"))
  pool.waiterCount = 0

  # Fail all pending pipeline ops
  pool.dispatchScheduled = false
  let closeErr = newPoolError(pekClosed, "Pool closed")
  while pool.pendingOps.len > 0:
    let op = pool.pendingOps.popFirst()
    failPendingOp(op, closeErr)

  # Wait for active connections to drain (bounded by closeDeadline)
  if hasDeadline and pool.active > 0:
    while pool.active > 0 and Moment.now() < closeDeadline:
      await sleepAsync(milliseconds(50))

  # Close all idle connections in parallel. `tracedClose` swallows its own
  # errors (routing them to the tracer), so a failure in one close does not
  # short-circuit the rest or escape this proc.
  var closeFuts: seq[Future[void]]
  while pool.idle.len > 0:
    let pc = pool.idle.popFirst()
    pool.metrics.closeCount.inc
    closeFuts.add(pool.tracedClose(pc.conn))
  await allFutures(closeFuts)

  # Yield once after closing idle connections and before draining background
  # tasks, but only when a borrow is still outstanding. A conn handed off to a
  # waiter on the same tick its acquire was abandoned leaves that acquire's
  # continuation scheduled but not yet resumed, so the loops above can't see it;
  # when it runs it does settleAbandonedWaiter -> release() -> closeNoWait,
  # pushing a fresh Terminate task. A yield lets that continuation enqueue the
  # task before the drain below rather than after close() returns.
  #
  # Such a not-yet-resumed acquire holds an `active` slot, so `active == 0` means
  # there is no abandoned-handoff continuation to wait for and the yield is pure
  # overhead. This covers the same-tick case the drain must not miss; a later
  # release of a still-live borrow follows the documented `close()` contract
  # (closed on release) and is not awaited here.
  if pool.active > 0:
    await sleepAsync(ZeroDuration)

  # Drain fire-and-forget tasks (closeNoWait, spawnConnectForWaiter). Late
  # release() may push more mid-await; snapshot-and-clear preserves them.
  # Bounded by closeDeadline: without it, a stuck spawn's connect pins close()
  # up to `maintenanceInterval` (30s default) past `timeout`.
  while pool.pendingBackgroundTasks.len > 0:
    let pending = pool.pendingBackgroundTasks
    pool.pendingBackgroundTasks.setLen(0)
    if hasDeadline:
      let remaining = closeDeadline - Moment.now()
      if remaining > ZeroDuration:
        try:
          await allFutures(pending).wait(remaining)
        except AsyncTimeoutError:
          discard
      for f in pending:
        if not f.finished:
          await cancelAndWait(f)
    else:
      await allFutures(pending)
