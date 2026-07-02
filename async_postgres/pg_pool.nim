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
  ## Never propagates `CatchableError`: this is invoked from `finally` blocks
  ## in the `with*` helpers (and the per-call cleanup path of `exec` / `query`
  ## etc.), where a raised reset error would mask the body's original
  ## exception. Cleanup errors — including any raised from the close path's
  ## tracer hook — are swallowed.
  if conn.state != csReady or conn.txStatus != tsIdle:
    return
  if pool.config.resetQuery.len == 0 and conn.heldSessionLocks == 0:
    return
  try:
    if conn.heldSessionLocks > 0:
      let t = pool.config.tracer
      if t != nil and t.onLeakedSessionLocks != nil:
        t.onLeakedSessionLocks(
          TraceLeakedSessionLocksData(conn: conn, count: conn.heldSessionLocks)
        )
      discard await conn.simpleExec("SELECT pg_advisory_unlock_all()")
      conn.heldSessionLocks = 0
    if pool.config.resetQuery.len > 0:
      discard await conn.simpleExec(pool.config.resetQuery)
      conn.clearStmtCache()
  except CatchableError:
    try:
      await pool.tracedClose(conn)
    except CatchableError:
      discard

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
  proc run() {.async.} =
    var consumed = false
    try:
      let conn = await connect(pool.config.connConfig)
      conn.ownerPool = pool
      pool.metrics.createCount.inc
      pool.consecutiveConnectFailures = 0
      if pool.closed:
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
        newException(PgPoolError, "Pool connect for waiter failed", e)
      )
    finally:
      if not consumed and pool.active > 0:
        pool.active.dec

  pool.pruneBackgroundTasks()
  let fut = run()
  pool.pendingBackgroundTasks.add(fut)
  asyncSpawn fut

proc closeLateConnect(pool: PgPool, connectFut: Future[PgConnection]) =
  ## Guard against asyncdispatch's non-cancelling `wait()`: when a bounded
  ## `connect()` times out, the inner future keeps running and may still yield
  ## a live connection that nobody is awaiting. Register a callback to close
  ## such an orphan rather than leaking its socket and a server-side backend
  ## slot. No-op on chronos, whose `wait()` actually cancels the connect, so
  ## the future never reaches `completed()`.
  when hasAsyncDispatch:
    proc closeOrphan() {.async.} =
      try:
        let orphan = connectFut.read()
        if orphan != nil:
          await pool.tracedClose(orphan)
      except CatchableError:
        discard

    connectFut.addCallback(
      proc() =
        if connectFut.completed():
          asyncSpawn closeOrphan()
    )
  else:
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

    # Skip the replenish phase while we are inside a backoff window from a
    # recent failure. Idle pruning above still runs every interval — only the
    # connect attempts are throttled, so a backed-off pool keeps closing dead
    # idle/expired connections normally.
    if pool.consecutiveConnectFailures > 0 and Moment.now() < pool.nextConnectRetryAt:
      continue

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
      let connectFut = connect(pool.config.connConfig)
      try:
        let conn = await connectFut.wait(replenishTimeout)
        conn.ownerPool = pool
        pool.metrics.createCount.inc
        pool.consecutiveConnectFailures = 0
        # The pool may have been closed while we awaited connect (and on
        # chronos this loop can be cancelled then resumed from an
        # already-completed connect). Parking a fresh conn in a closed pool's
        # idle deque — or handing it to a waiter close() has already failed —
        # leaks its socket, so re-check first, mirroring spawnConnectForWaiter.
        # closeNoWait (not `await tracedClose`) because a pending cancellation
        # would interrupt a fresh await before the close runs.
        if pool.closed:
          pool.closeNoWait(conn)
          break
        # FIFO fairness: if a waiter is already queued, hand the freshly
        # opened connection to them directly rather than parking it in
        # idle (which would let a later acquire jump the queue).
        if pool.tryHandoffToWaiter(conn):
          pool.active.inc
        else:
          pool.idle.addLast(PooledConn(conn: conn, lastUsedAt: now))
      except CatchableError:
        # asyncdispatch's wait() cannot cancel connectFut: after the timeout it
        # keeps running and may still produce a live connection nobody awaits.
        # Close that orphan so it leaks neither a socket nor a server slot.
        pool.closeLateConnect(connectFut)
        pool.consecutiveConnectFailures.inc
        let delay = computeConnectBackoff(
          pool.config.connectBackoffInitial, pool.config.connectBackoffMax,
          pool.consecutiveConnectFailures,
        )
        if delay > ZeroDuration:
          pool.nextConnectRetryAt = Moment.now() + delay
        break # best-effort, retry next interval (or after backoff)

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
      conn.heldSessionLocks > 0:
    if pool.active > 0:
      pool.active.dec
    pool.closeNoWait(conn)
    # FIFO fairness: a discarded conn does not serve a waiter, but it frees
    # an `active` slot. If a waiter is queued and the pool can still grow,
    # open a replacement out-of-band so the front waiter is not stranded
    # until the next release or maintenance tick.
    if not pool.closed and pool.waiterCount > 0 and pool.active < pool.config.maxSize and
        pool.canAttemptConnect():
      pool.active.inc
      pool.spawnConnectForWaiter()
    return (true, false)
  # FIFO handoff: serve the head waiter directly with the released conn.
  # `active` is intentionally not decremented — the conn is still in use, just
  # by a different borrower. Any remaining waiters behind the head are not
  # spawned for here: each one already had a `spawnConnectForWaiter`
  # reservation kicked off at queue-time (acquireImpl) or at the broken-release
  # that freed a slot, so the queue is already covered by in-flight spawns or
  # by the next release.
  while pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    if waiter.isAbandoned:
      continue
    pool.waiterCount.dec
    conn.borrowed = true
    waiter.fut.complete(conn)
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
  ## Session-level advisory locks (`heldSessionLocks > 0`) likewise force the
  ## connection to be discarded: callers who route through `resetSession`
  ## clear them ahead of time, so anything reaching here with locks still
  ## held has bypassed that path and must not return to the idle queue.
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
      tracer.onPoolDoubleRelease(TracePoolDoubleReleaseData(conn: conn))
    return
  conn.borrowed = false
  if conn.heldSessionLocks > 0 and tracer != nil and tracer.onLeakedSessionLocks != nil:
    tracer.onLeakedSessionLocks(
      TraceLeakedSessionLocksData(conn: conn, count: conn.heldSessionLocks)
    )
  if tracer == nil:
    discard pool.releaseCore(conn)
    return

  var traceCtx: TraceContext
  if tracer.onPoolReleaseStart != nil:
    traceCtx = tracer.onPoolReleaseStart(TracePoolReleaseStartData(conn: conn))

  let (wasClosed, handedToWaiter) = pool.releaseCore(conn)

  if tracer.onPoolReleaseEnd != nil:
    tracer.onPoolReleaseEnd(
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
    raise newException(PgPoolError, "Pool is closed")

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
    raise newException(PgPoolError, "Pool acquire timeout")

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
        pool.metrics.closeCount.inc
        await pool.tracedClose(pc.conn)
        continue
      if pool.config.maxLifetime > ZeroDuration and
          now - pc.conn.createdAt > pool.config.maxLifetime:
        pool.metrics.closeCount.inc
        await pool.tracedClose(pc.conn)
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
        try:
          await pc.conn.ping(pingBudget)
        except CancelledError as e:
          # CancelledError is a CatchableError; the generic handler below would
          # swallow it, letting acquire run on and return a connection the
          # departed canceller leaks. The interrupted ping leaves the protocol
          # out of sync, so close the popped conn (closeNoWait, since the pending
          # cancellation would interrupt a fresh await) and re-raise.
          pool.closeNoWait(pc.conn)
          raise e
        except CatchableError:
          pool.metrics.closeCount.inc
          await pool.tracedClose(pc.conn)
          continue
      pool.active.inc
      pc.conn.borrowed = true
      recordAcquire()
      return (pc.conn, false)

    # No idle connections; create new if under limit
    if pool.active < pool.config.maxSize:
      var connCfg = pool.config.connConfig
      var cappedByDeadline = false
      if hasDeadline:
        let rem = remainingBudget()
        if rem <= ZeroDuration:
          raiseAcquireTimeout()
        if connCfg.connectTimeout == ZeroDuration or rem < connCfg.connectTimeout:
          connCfg.connectTimeout = rem
          cappedByDeadline = true
      pool.active.inc
      try:
        let conn = await connect(connCfg)
        conn.ownerPool = pool
        conn.borrowed = true
        pool.metrics.createCount.inc
        # A successful caller-driven connect signals the DB is reachable —
        # let the maintenance loop resume immediate replenishment.
        pool.consecutiveConnectFailures = 0
        recordAcquire()
        return (conn, true)
      except CancelledError as e:
        # Cancellation (e.g. a caller's wait()-style deadline) must propagate
        # unwrapped so the canceller's machinery sees it.
        pool.active.dec
        raise e
      except CatchableError as e:
        pool.active.dec
        # A connect cut short by the acquire deadline surfaces as the same
        # timeout error as the waiter path; any other connect failure
        # (including a user-configured connectTimeout firing before the
        # deadline) is wrapped so acquire() keeps its documented PgPoolError
        # contract — the original error stays available via `parent`.
        if cappedByDeadline and e of AsyncTimeoutError:
          pool.metrics.timeoutCount.inc
          raise newException(PgPoolError, "Pool acquire timeout", e)
        raise newException(PgPoolError, "Pool connect failed", e)

  # Either max connections are reached or waiters are queued ahead of us;
  # queue up and wait for delivery.
  if pool.config.maxWaiters >= 0 and pool.waiterCount >= pool.config.maxWaiters:
    raise newException(
      PgPoolError,
      "Pool acquire queue full (maxWaiters=" & $pool.config.maxWaiters & ")",
    )
  # Compute the remaining budget before queueing; whatever the idle
  # health-check pings consumed comes out of the waiter wait below.
  var waitBudget = ZeroDuration
  if hasDeadline:
    waitBudget = remainingBudget()
    if waitBudget <= ZeroDuration:
      raiseAcquireTimeout()
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
      raise newException(PgPoolError, "Pool acquire timeout")
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
  ## preserved as the `parent` of the raised `PgPoolError`.
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

template withConnection*(pool: PgPool, conn, body: untyped) =
  ## Acquire a connection, execute `body`, then release it back to the pool.
  ## The connection is available as `conn` inside the body.
  ## `resetSession` runs before release, so a configured `resetQuery` is
  ## applied and any session-level advisory locks acquired through the typed
  ## API are released via `pg_advisory_unlock_all`.
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
  try:
    let pipeline = newPipeline(conn)
    for op in batch:
      case op.kind
      of popExec:
        pipeline.addExec(op.sql, op.params)
      of popQuery:
        pipeline.addQuery(op.sql, op.params, op.resultFormat)
    let ir = await pipeline.executeIsolated(timeout)
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
  var conns: seq[PgConnection]
  let nConns = min(ops.len, max(1, maxConns))
  for i in 0 ..< nConns:
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
  ##
  ## In pipelined mode a batch runs under a single timeout, so a finite
  ## `timeout` may be widened to the largest finite timeout among the ops it is
  ## batched with. An op with no timeout (`ZeroDuration`) is batched separately
  ## and stays unlimited.
  if pool.config.pipelined:
    if pool.closed:
      raise newException(PgPoolError, "Pool is closed")
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
  ##
  ## In pipelined mode a batch runs under a single timeout, so a finite
  ## `timeout` may be widened to the largest finite timeout among the ops it is
  ## batched with. An op with no timeout (`ZeroDuration`) is batched separately
  ## and stays unlimited.
  if pool.config.pipelined:
    if pool.closed:
      raise newException(PgPoolError, "Pool is closed")
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
  ## Raises `PgNoRowsError` if no rows are returned.
  let row =
    await pool.queryRowOpt(sql, params, resultFormat = resultFormat, timeout = timeout)
  if row.isNone:
    raise newException(PgNoRowsError, "Query returned no rows")
  return row.get

proc queryValue*(
    pool: PgPool,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Raises `PgNoRowsError` if no rows are returned, or `PgNullError` if the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgNoRowsError, "Query returned no rows")
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    raise newException(PgNullError, "Query returned NULL")
  return row.getStr(0)

proc queryValue*[T](
    pool: PgPool,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Raises `PgNoRowsError` if no rows are returned, or `PgNullError` if the value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgNoRowsError, "Query returned no rows")
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    raise newException(PgNullError, "Query returned NULL")
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
  ## Raises `PgNullError` if any value is NULL.
  let qr = await pool.query(sql, params, timeout = timeout)
  for i in 0 ..< qr.rowCount:
    let row = initRow(qr.data, i)
    if row.isNull(0):
      raise newException(PgNullError, "NULL value in column")
    result.add(row.getStr(0))

proc simpleQuery*(
    pool: PgPool, sql: string, timeout: Duration = ZeroDuration
): Future[seq[QueryResult]] {.async.} =
  ## Execute one or more SQL statements via the simple query protocol using a
  ## pooled connection. See ``PgConnection.simpleQuery`` for semantics —
  ## multi-statement, no parameters, no plan cache.
  let conn = await pool.acquire()
  try:
    return await conn.simpleQuery(sql, timeout)
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
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let resetSessionSym = bindSym"resetSession"
  let csReadySym = bindSym"csReady"
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let fireCleanupSkippedSym = bindSym"fireCleanupSkipped"
  let ckTxRollbackSym = bindSym"ckTxRollback"
  let csrConnInvalidatedSym = bindSym"csrConnInvalidated"
  let csrCleanupFailedSym = bindSym"csrCleanupFailed"
  result = quote:
    let `poolSym` = `poolExpr`
    let `connIdent` = await `poolSym`.acquire()
    try:
      discard await `connIdent`.simpleExec(`beginSql`, timeout = `txTimeout`)
      try:
        `body`
        discard await `connIdent`.simpleExec("COMMIT", timeout = `txTimeout`)
      except CatchableError as `eSym`:
        # Mirror the per-call rationale from pg_client's `withTransaction`:
        # skip ROLLBACK on an already-invalidated connection (per-call
        # timeout marked it csClosed but txStatus may still be stale) and
        # avoid a futile cleanup when the server has already ended the
        # transaction. The csClosed skip and any swallowed inner-ROLLBACK
        # failure are surfaced through `onCleanupSkipped` so the diagnostic
        # asymmetry with the body-error path goes away.
        if `connIdent`.state != `csReadySym`:
          `fireCleanupSkippedSym`(
            `connIdent`, `ckTxRollbackSym`, `csrConnInvalidatedSym`
          )
        elif `connIdent`.txStatus in {`tsInTxSym`, `tsInFailedSym`}:
          try:
            discard await `connIdent`.simpleExec("ROLLBACK", timeout = `txTimeout`)
          except CatchableError as `cleanupErrSym`:
            `fireCleanupSkippedSym`(
              `connIdent`, `ckTxRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
            )
        raise `eSym`
    finally:
      await `resetSessionSym`(`poolSym`, `connIdent`)
      `connIdent`.release()

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
  let resetSessionSym = bindSym"resetSession"
  let loop = buildRetryTxLoop(connIdent, retryOptsSym, beginSql, txTimeout, body)
  result = quote:
    let `poolSym` = `poolExpr`
    let `connIdent` = await `poolSym`.acquire()
    let `retryOptsSym` = `retryOpts`
    try:
      `loop`
    finally:
      await `resetSessionSym`(`poolSym`, `connIdent`)
      `connIdent`.release()

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
  ## **On deadline exceeded:** if a connection was already acquired, it is
  ## invalidated via `invalidateOnTimeout` (marked `csClosed`) and the pool
  ## drops it on release. ROLLBACK is *not* attempted. `PgTimeoutError` is
  ## raised. If the timeout fires while still waiting for `acquire()`, the
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
  ## **On other body exceptions:** ROLLBACK is issued with
  ## `rollbackGrace` per-call timeout.
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
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "poolTxBodyDeadline")
  let bodyFutSym = genSym(nskLet, "bodyFut")
  let connOptSym = genSym(nskVar, "connOpt")
  let resetSessionSym = bindSym"resetSession"
  let csReadySym = bindSym"csReady"
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let timeoutErrSym = bindSym"AsyncTimeoutError"
  let waitSym = bindSym"wait"
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let invalidateSym = bindSym"invalidateOnTimeout"
  let fireCleanupSkippedSym = bindSym"fireCleanupSkipped"
  let ckTxRollbackSym = bindSym"ckTxRollback"
  let csrConnInvalidatedSym = bindSym"csrConnInvalidated"
  let csrCleanupFailedSym = bindSym"csrCleanupFailed"

  result = quote:
    let `poolSym` = `poolExpr`
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    var `connOptSym` = none(PgConnection)
    proc `bodyFnSym`(): Future[void] {.async.} =
      let `connIdent` = await `poolSym`.acquire()
      `connOptSym` = some(`connIdent`)
      try:
        discard await `connIdent`.simpleExec(
          `beginSql`, timeout = `remainingSym`(`deadlineMomentSym`)
        )
        try:
          `body`
          discard await `connIdent`.simpleExec(
            "COMMIT", timeout = `remainingSym`(`deadlineMomentSym`)
          )
        except CatchableError as `eSym`:
          # Mirror pg_client's `withTransactionDeadline`: skip ROLLBACK on a
          # csClosed connection (per-call body timeout invalidated it) and
          # report the skip / any swallowed inner-ROLLBACK failure through
          # `onCleanupSkipped` so the timeout path is observable.
          if `connIdent`.state != `csReadySym`:
            `fireCleanupSkippedSym`(
              `connIdent`, `ckTxRollbackSym`, `csrConnInvalidatedSym`
            )
          elif `connIdent`.txStatus in {`tsInTxSym`, `tsInFailedSym`}:
            try:
              discard await `connIdent`.simpleExec("ROLLBACK", timeout = `graceSym`)
            except CatchableError as `cleanupErrSym`:
              `fireCleanupSkippedSym`(
                `connIdent`, `ckTxRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
              )
          raise `eSym`
      finally:
        await `resetSessionSym`(`poolSym`, `connIdent`)
        `connIdent`.release()

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
      elif `connOptSym`.isSome:
        # invalidateOnTimeout marks the connection csClosed and raises
        # PgTimeoutError — control does not return from this call.
        # Ordering note: under chronos, `wait` cancels `bodyFut`, which runs
        # `bodyFn`'s `finally` (resetSession + release) before this handler
        # gets control. The connection has therefore already been returned
        # to the pool when we invalidate it here. That is safe: the pool's
        # next `acquire` health-check drops `csClosed` connections, so the
        # bad conn cannot escape back to a caller.
        `connOptSym`.get.`invalidateSym`("withTransactionDeadline (pool) exceeded")
      else:
        raise newException(
          PgTimeoutError, "withTransactionDeadline (pool): acquire timed out"
        )

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
  ## **On deadline exceeded:** the in-flight connection (if any) is invalidated
  ## and `PgTimeoutError` is raised — never retried. **On a retryable error:**
  ## ROLLBACK runs with `rollbackGrace` and the transaction is retried if budget
  ## remains. See `withTransactionDeadline` for the acquire-race / `completed()`
  ## rationale and the in-body `conn.exec(...)` warning. **Idempotency:** `body`
  ## runs once per attempt; non-database side effects repeat. Using `return`
  ## inside the body is a compile-time error.
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
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "poolTxBodyRetryDeadline")
  let connOptSym = genSym(nskVar, "connOpt")
  let resetSessionSym = bindSym"resetSession"
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let invalidateSym = bindSym"invalidateOnTimeout"
  # The pool variant acquires a fresh connection per attempt, so its cleanup
  # (ROLLBACK + release) happens inside bodyFn; the outer loop adds no cleanup
  # and omits the conn-state retry gate (connForStateCheck = nil).
  let timeoutElse = quote:
    if `connOptSym`.isSome:
      `connOptSym`.get.`invalidateSym`("withTransactionRetryDeadline (pool) exceeded")
    else:
      raise newException(
        PgTimeoutError, "withTransactionRetryDeadline (pool): acquire timed out"
      )
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
    proc `bodyFnSym`(): Future[void] {.async.} =
      `connOptSym` = none(PgConnection)
      let `connIdent` = await `poolSym`.acquire()
      `connOptSym` = some(`connIdent`)
      try:
        discard await `connIdent`.simpleExec(
          `beginSql`, timeout = `remainingSym`(`deadlineMomentSym`)
        )
        try:
          `body`
          discard await `connIdent`.simpleExec(
            "COMMIT", timeout = `remainingSym`(`deadlineMomentSym`)
          )
        except CatchableError as `eSym`:
          `bodyCleanup`
          raise `eSym`
      finally:
        await `resetSessionSym`(`poolSym`, `connIdent`)
        `connIdent`.release()

    `loop`

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

  # Cancel all waiters. PgPoolError (not bare PgError) so a waiter failed
  # by close() matches acquire()'s documented error contract. Skip waiters whose
  # future is already finished (e.g. a chronos timeout/cancel that hasn't yet run
  # `settleAbandonedWaiter`): `fail()` on a finished future is a no-op on chronos
  # and a Defect on asyncdispatch.
  while pool.waiters.len > 0:
    let waiter = pool.waiters.popFirst()
    if not waiter.isAbandoned:
      waiter.fut.fail(newException(PgPoolError, "Pool closed"))
  pool.waiterCount = 0

  # Fail all pending pipeline ops
  pool.dispatchScheduled = false
  let closeErr = newException(PgPoolError, "Pool closed")
  while pool.pendingOps.len > 0:
    let op = pool.pendingOps.popFirst()
    failPendingOp(op, closeErr)

  # Wait for active connections to drain
  if timeout > ZeroDuration and pool.active > 0:
    let deadline = Moment.now() + timeout
    while pool.active > 0 and Moment.now() < deadline:
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

  # Wait for any fire-and-forget tasks (closeNoWait, spawnConnectForWaiter) so
  # the server observes Terminate / connects unwind before this proc returns. A
  # late release() from another task may push more entries while we await, so
  # loop with snapshot-and-clear to avoid discarding unfinished futures.
  while pool.pendingBackgroundTasks.len > 0:
    let pending = pool.pendingBackgroundTasks
    pool.pendingBackgroundTasks.setLen(0)
    await allFutures(pending)
