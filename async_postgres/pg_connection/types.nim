## Internal building blocks shared by every `pg_connection/` submodule.
## Contains the `PgConnection` ref type, `ConnConfig`, tracing data types,
## public read-only/read-write accessors, internal accessors for cross-module
## use within the library, and the tracing helper templates.
##
## Re-exported through `pg_connection.nim`; submodules import this module
## directly.

import std/[tables, sets, deques, lists]
when defined(posix):
  import std/posix

import ../[async_backend, pg_errors, pg_protocol, pg_types]

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet

# TCP keepalive socket options (not exported by posix module)
when defined(linux):
  var
    TCP_KEEPIDLE* {.importc, header: "<netinet/tcp.h>".}: cint
    TCP_KEEPINTVL* {.importc, header: "<netinet/tcp.h>".}: cint
    TCP_KEEPCNT* {.importc, header: "<netinet/tcp.h>".}: cint
elif defined(macosx):
  var TCP_KEEPALIVE* {.importc, header: "<netinet/tcp.h>".}: cint
  const
    TCP_KEEPINTVL* = cint(0x101)
    TCP_KEEPCNT* = cint(0x102)
else:
  {.
    warning:
      "TCP keepalive timing options (idle/interval/count) are not supported on this platform and will be ignored"
  .}

type
  PgConnState* = enum
    ## Connection lifecycle state.
    csConnecting
    csAuthentication
    csReady
    csBusy
    csListening
    csReplicating
    csClosed

  SslMode* = enum
    ## SSL/TLS negotiation mode for the connection.
    ##
    ## Note: `sslDisable` is the enum's zero value, so a zero-initialized
    ## `ConnConfig` has SSL disabled. The constructor helpers `parseDsn` and
    ## `initConnConfig`, however, default to `sslPrefer` to match libpq and
    ## avoid silently sending credentials in plaintext.
    sslDisable ## Disable SSL
    sslAllow ## Try plaintext; fall back to SSL if refused
    sslPrefer ## Try SSL; fall back to plaintext if refused (libpq default)
    sslRequire ## Require SSL (no certificate verification)
    sslVerifyCa ## Require SSL + verify CA chain (no hostname verification)
    sslVerifyFull ## Require SSL + verify CA chain and hostname

  ChannelBindingMode* = enum
    ## SCRAM channel binding policy (libpq-compatible).
    cbPrefer ## Use SCRAM-SHA-256-PLUS when SSL and server support it (default).
    cbDisable ## Never use SCRAM-SHA-256-PLUS; only SCRAM-SHA-256.
    cbRequire ## Require SCRAM-SHA-256-PLUS; fail if unavailable.

  AuthMethod* = enum
    ## Individual authentication methods for `ConnConfig.requireAuth`
    ## allowlisting (libpq `require_auth` parity).
    amNone ## AuthenticationOk with no challenge (trust/peer/ident)
    amPassword ## cleartext password (libpq: "password")
    amMd5 ## MD5 challenge (libpq: "md5")
    amScramSha256 ## SASL SCRAM-SHA-256 (libpq: "scram-sha-256")
    amScramSha256Plus ## SASL SCRAM-SHA-256-PLUS (libpq: "scram-sha-256-plus")

  TargetSessionAttrs* = enum
    ## Target server type for multi-host failover (libpq compatible).
    tsaAny ## Connect to any server (default)
    tsaReadWrite ## Read-write server (primary)
    tsaReadOnly ## Read-only server (standby)
    tsaPrimary ## Primary server
    tsaStandby ## Standby server
    tsaPreferStandby ## Prefer standby, fall back to any

  LoadBalanceHosts* = enum
    ## Host connection ordering for a multi-host connection (libpq compatible).
    lbhDisable ## Try hosts in the configured order (default)
    lbhRandom
      ## Shuffle the configured host list once per connection so a pool of
      ## connections spreads across hosts (e.g. read replicas). Only the
      ## multi-host list is reordered — multiple addresses behind a single host
      ## name are not shuffled. See `orderedHosts` for the seeding and
      ## thread-safety details.

  HostEntry* = object ## A single host:port entry for multi-host connection.
    host*: string ## Host name (or Unix socket dir); used for SSL verification
    hostaddr*: string
      ## Numeric address dialed instead of resolving `host` (libpq `hostaddr`).
      ## Empty = resolve `host`.
    port*: int

  ConnConfig* = object
    ## Connection configuration. Construct via `parseDsn` or set fields directly.
    host*: string
    port*: int # default 5432
    hostaddr*: string
      ## Numeric address dialed instead of resolving `host` (libpq `hostaddr`).
      ## `host` is still the name used for SSL certificate verification.
    user*: string
    password*: string
    database*: string
    sslMode*: SslMode
      ## SSL/TLS negotiation mode. `parseDsn` and `initConnConfig` default this
      ## to `sslPrefer` (libpq parity); a raw zero-initialized `ConnConfig` has
      ## `sslDisable`.
    sslRootCert*: string ## PEM-encoded CA certificate(s) for sslVerifyCa/sslVerifyFull
    channelBinding*: ChannelBindingMode
      ## SCRAM channel binding policy (default cbPrefer). `cbRequire` fails the
      ## connection if SCRAM-SHA-256-PLUS cannot actually be used (libpq parity).
    requireAuth*: set[AuthMethod]
      ## Allowlist of auth methods the client will accept. An empty set
      ## (default) means "allow any" — matching libpq when `require_auth` is
      ## unset. If the server requests a method outside this set, connect
      ## fails with `PgConnectionError`. For SASL, advertised mechanisms are
      ## filtered and the selected mechanism is validated.
      ##
      ## Note: libpq's `!`-prefix negation syntax (e.g. `!password`) is not
      ## yet supported by `parseRequireAuth` — specify the allowed methods
      ## positively instead.
    applicationName*: string
    connectTimeout*: Duration ## TCP connect timeout (default: no timeout)
    keepAlive*: bool ## Enable TCP keepalive (default true via parseDsn)
    keepAliveIdle*: int ## Seconds before first probe (0 = OS default)
    keepAliveInterval*: int ## Seconds between probes (0 = OS default)
    keepAliveCount*: int ## Number of probes before giving up (0 = OS default)
    hosts*: seq[HostEntry] ## Multiple hosts for failover (empty = use host/port)
    targetSessionAttrs*: TargetSessionAttrs ## Target server type (default tsaAny)
    loadBalanceHosts*: LoadBalanceHosts
      ## Host ordering for multi-host connections (libpq `load_balance_hosts`);
      ## see `LoadBalanceHosts`. `lbhDisable` (default) preserves the configured
      ## order.
    extraParams*: seq[(string, string)] ## Additional startup parameters
    maxMessageSize*: int
      ## Upper bound (in bytes) on a single backend message including
      ## its 1-byte type and 4-byte length header. A server claiming a
      ## larger message is rejected with `PgProtocolError` before any
      ## further recv-buffer growth, capping memory exposure to a
      ## misbehaving or malicious peer. ``0`` (default) selects
      ## `DefaultMaxBackendMessageLen` (1 GiB).
    tracer*: PgTracer ## Optional tracer for connection-level hooks

  Notification* = object ## A NOTIFY message received from PostgreSQL.
    pid*: int32
    channel*: string
    payload*: string

  NotifyCallback* = proc(notification: Notification) {.gcsafe, raises: [].}
    ## Callback invoked when a NOTIFY message arrives.

  Notice* = object ## A notice or warning message from the server (not an error).
    fields*: seq[ErrorField]

  NoticeCallback* = proc(notice: Notice) {.gcsafe, raises: [].}
    ## Callback invoked when a notice/warning message arrives.

  CachedStmt* = ref object ## A cached prepared statement in the LRU statement cache.
    name*: string ## Server-side statement name ("_sc_1", "_sc_2", ...)
    fields*: seq[FieldDescription] ## From Describe(Statement), formatCode=0
    paramOids*: seq[int32]
      ## Input parameter type OIDs from ParameterDescription. Used to detect
      ## type-mismatch on cache hit: if a later call binds the same SQL with
      ## different parameter OIDs, the server would interpret the bytes using
      ## the original parse-time types, silently corrupting results. The
      ## cache-hit path checks these against the caller's OIDs and falls back
      ## to a re-parse when they diverge. Empty for parameter-less SQL —
      ## an empty-vs-empty comparison matches trivially and the cache entry
      ## is reused.
    resultFormats*: seq[int16] ## Cached buildResultFormats() output
    colFmts*: seq[int16] ## Per-column format codes for RowData
    colOids*: seq[int32] ## Per-column type OIDs for RowData
    lruNode*: DoublyLinkedNode[string] ## Embedded LRU list node

  PgPoolOwner* = ref object of RootObj
    ## Opaque base for pool-ownership back-references on `PgConnection`.
    ## The concrete type is `PgPool` (defined in `pg_pool`); this base lives
    ## here to avoid a circular import. Consumers should not subclass this.

  PgConnection* = ref object
    ## A single PostgreSQL connection with buffered I/O and statement caching.
    when hasChronos:
      transport*: StreamTransport
      baseReader*: AsyncStreamReader
      baseWriter*: AsyncStreamWriter
      reader*: AsyncStreamReader
      writer*: AsyncStreamWriter
      tlsStream*: TLSAsyncStream
      trustAnchorBufs*: seq[seq[byte]] ## Backing memory for custom trust anchor pointers
      x509Capture*: X509CertCaptureContext ## X509 wrapper for cert capture
    elif hasAsyncDispatch:
      socket*: AsyncSocket
    serverCertDer*: seq[byte] ## DER-encoded server certificate for SCRAM channel binding
    sslEnabled*: bool
    recvBuf*: seq[byte]
    recvBufStart*: int ## Read pointer into recvBuf; bytes before this are consumed
    state*: PgConnState
    pid*: int32
    secretKey*: int32
    serverParams*: Table[string, string]
    txStatus*: TransactionStatus
    notifyCallback*: NotifyCallback
    noticeCallback*: NoticeCallback
    listenChannels*: HashSet[string]
    listenTask*: Future[void]
    listenStopRequested*: bool
      ## Set by `stopListening` to ask the background pump to exit. The pump
      ## checks it at every yield point of its auto-reconnect loop so a stop
      ## requested while the transport is being rebuilt is honored instead of
      ## being lost when a successful reconnect restores `csListening`.
    listenReconnecting*: bool
      ## True while the pump is inside its auto-reconnect loop. Tells
      ## `stopListening` that the empty-query unblock it normally uses would race
      ## the reconnect's own LISTEN round trips, so it must wait for the pump to
      ## observe `listenStopRequested` instead of sending the query.
    host*: string
    port*: int
    createdAt*: Moment
    portalCounter*: int
    config*: ConnConfig
    notifyQueue*: Deque[Notification]
    notifyMaxQueue*: int
      ## Pull-API (`waitNotification`) queue cap. Default 1024. A positive value
      ## bounds the queue and drops the oldest notifications on overflow
      ## (tracked via `notifyDropped`). `0` or negative means unbounded — the
      ## queue grows until drained, matching libpq/psycopg and Python's
      ## `queue.Queue(maxsize<=0)`. The push API (`onNotify`) is unaffected by
      ## this setting and always fires.
    notifyWaiter*: Future[void]
    sendBuf*: seq[byte] ## Reusable send buffer for COPY IN batching
    notifyDropped*: int ## Count of notifications dropped due to queue overflow
    listenError*: ref PgListenError ## Set when listen pump fails permanently
    listenReconnectMaxAttempts*: int
      ## Max reconnect attempts on listen pump failure. Default 10.
      ## 0 or negative = unlimited retries (retry until close()).
    listenReconnectMaxBackoff*: int
      ## Max seconds between reconnect attempts (backoff cap). Default 30.
    reconnectCallback*: proc() {.gcsafe, raises: [].}
    notifyOverflowCallback*: proc(dropped: int) {.gcsafe, raises: [].}
    listenErrorCallback*: proc(err: ref PgListenError) {.gcsafe, raises: [].}
      ## Invoked when the listen pump dies permanently (reconnection failed or
      ## the connection was lost with nothing left to re-subscribe). Lets push
      ## API (`onNotify`) users learn the pump is gone — the pull API surfaces
      ## the same failure through `waitNotification`.
    stmtCache*: Table[string, CachedStmt]
    stmtCacheLru*: DoublyLinkedList[string] ## LRU order: oldest at head, newest at tail
    stmtCounter*: int
    stmtCacheCapacity*: int ## 0=disabled, default 256
    pendingStmtCloses*: seq[string]
      ## Server-side prepared statement names whose ``Close`` was not bundled
      ## with the operation that evicted them. Populated when the defensive
      ## eviction loop in ``addStmtCache`` fires (caller skipped the
      ## pre-eviction step, or ``stmtCacheCapacity`` was shrunk below the
      ## current cache size). Flushed by ``flushPendingStmtCloses`` at the
      ## start of the next Extended Query send phase so the leak is bounded
      ## to the gap until the next operation.
    heldSessionLocks*: int
      ## Count of session-level `pg_advisory_lock` acquires through the typed
      ## API. The pool releases or discards connections with a non-zero count
      ## so that locks never leak to subsequent borrowers. Raw-SQL acquires
      ## (`conn.exec("SELECT pg_advisory_lock(...)")`) bypass this counter.
    tracer*: PgTracer ## Inherited from ConnConfig on connect
    ownerPool*: PgPoolOwner
      ## Owning pool back-reference. Set when this connection is managed by
      ## a `PgPool` (or a pool inside `PgPoolCluster`); `nil` for standalone
      ## connections created via `connect`. Used by `release(conn)` to route
      ## the connection back to the correct pool.
    borrowed*: bool
      ## Whether this connection is currently checked out from its owning pool.
      ## Set when `acquire` hands it to a caller (or directly to a queued
      ## waiter) and cleared when it returns to the pool's idle set or is
      ## discarded. `release(conn)` uses it to turn a duplicate release of an
      ## already-returned connection into a no-op (reported via the tracer's
      ## `onPoolDoubleRelease`) instead of registering the same connection in
      ## the idle deque twice — which would otherwise hand one connection to
      ## two borrowers and corrupt their wire protocol. Always false for
      ## standalone connections created via `connect`.
      ##
      ## This guards the common double release — a connection that is already
      ## sitting idle. It does NOT catch a back-to-back double release whose
      ## first release served a queued waiter: the handoff re-marks the
      ## connection `borrowed` for the waiter, so an erroneous second release
      ## passes the guard and can hand the in-use connection to yet another
      ## borrower. Raw `acquire` / `release(conn)` callers carry that risk;
      ## `PooledConnHandle` (its own `released` flag) and the `with*` templates
      ## are the fully safe paths.
    replConfirmedFlushLsnRaw: uint64
      ## Internal replication state: raw LSN value up to which the application
      ## has confirmed received WAL is durably flushed during a replication
      ## stream. Public API users go through `confirmFlushed` /
      ## `confirmedFlushLsn` in `pg_replication`, which add the `Lsn` typing,
      ## the monotonic guard, and the received-WAL bound check. Manipulated
      ## via the exported helpers in this module (see below).
    replMaxReceivedLsnRaw: uint64
      ## Internal replication state: highest WAL position actually received
      ## from the wire during the current stream (`XLogData` end LSN, not the
      ## server's `walEnd`). Initialised to the stream's `startLsn` and updated
      ## by `startReplication` / `startPhysicalReplication`. Used by
      ## `confirmFlushed` to reject confirmations beyond received WAL.
      ## Manipulated via the exported helpers in this module (see below).
    replReadScratch*: seq[byte]
      ## Reusable scratch buffer for `fillRecvBufDetached` (chronos only). The
      ## proactive status-interval path keeps a single read in flight across
      ## timer wakes; reading into this private buffer (instead of growing
      ## `recvBuf` up front like `fillRecvBuf`) keeps `recvBuf` parseable while
      ## that read is still pending. Allocated lazily to `RecvBufSize` and reused.

  QueryResult* = object
    ## Result of a query: field descriptions, row data, and command tag.
    fields*: seq[FieldDescription]
    data*: RowData
    rowCount*: int32
    commandTag*: string

  CopyResult* = object
    ## Result of a buffered COPY OUT operation: all rows collected in memory.
    format*: CopyFormat
    columnFormats*: seq[int16]
    data*: seq[seq[byte]]
    commandTag*: string

  CopyOutInfo* = object ## Metadata returned when a streaming COPY OUT begins.
    format*: CopyFormat
    columnFormats*: seq[int16]
    commandTag*: string

  CopyInInfo* = object ## Metadata returned when a streaming COPY IN begins.
    format*: CopyFormat
    columnFormats*: seq[int16]
    commandTag*: string

  # Tracing types
  TraceContext* = RootRef
    ## Opaque correlation token returned by trace Start hooks and passed to End hooks.
    ## Users subtype RootObj (e.g. ``type Span = ref object of RootObj``) and return
    ## it from Start hooks; End hooks downcast via ``Span(ctx)``.

  TraceCopyDirection* = enum
    tcdIn
    tcdOut

  TraceConnectStartData* = object ## Data passed to the connect start hook.
    hosts*: seq[HostEntry]

  TraceConnectEndData* = object ## Data passed to the connect end hook.
    conn*: PgConnection
    err*: ref CatchableError

  TraceQueryStartData* = object ## Data passed to the query/exec start hook.
    sql*: string
    params*: seq[PgParam]
      ## Populated when the caller used a `seq[PgParam]` overload. Mutually
      ## exclusive with `paramsInline`: exactly one of the two is non-empty
      ## per call (or both are empty if the query has no bound parameters).
    paramsInline*: seq[PgParamInline]
      ## Populated when the caller used a `PgParamInline` overload. Mutually
      ## exclusive with `params` (see above). Tracers that want a single view
      ## should branch on whichever field is non-empty.
    isExec*: bool ## true for exec, false for query

  TraceQueryEndData* = object ## Data passed to the query/exec end hook.
    commandTag*: string
    rowCount*: int64
    err*: ref CatchableError

  TracePrepareStartData* = object ## Data passed to the prepare start hook.
    name*: string
    sql*: string

  TracePrepareEndData* = object ## Data passed to the prepare end hook.
    err*: ref CatchableError

  TracePipelineStartData* = object ## Data passed to the pipeline start hook.
    opCount*: int

  TracePipelineEndData* = object ## Data passed to the pipeline end hook.
    err*: ref CatchableError

  TraceCopyStartData* = object ## Data passed to the copy start hook.
    sql*: string
    direction*: TraceCopyDirection

  TraceCopyEndData* = object ## Data passed to the copy end hook.
    commandTag*: string
    err*: ref CatchableError

  TracePoolAcquireStartData* = object ## Data passed to the pool acquire start hook.
    idleCount*: int
    activeCount*: int
    maxSize*: int

  TracePoolAcquireEndData* = object ## Data passed to the pool acquire end hook.
    conn*: PgConnection
    err*: ref CatchableError
    wasCreated*: bool ## true if a new connection was created

  TracePoolReleaseStartData* = object ## Data passed to the pool release start hook.
    conn*: PgConnection

  TracePoolReleaseEndData* = object ## Data passed to the pool release end hook.
    wasClosed*: bool ## true if connection was closed instead of returned to pool
    handedToWaiter*: bool ## true if connection was given directly to a waiting acquirer

  TracePoolDoubleReleaseData* = object
    ## Data passed to the pool double-release hook. Fired when `release(conn)`
    ## is called on a connection that is not currently checked out — a
    ## duplicate release, or a connection that never came from this pool's
    ## `acquire`. The release is a no-op (the connection is left untouched), so
    ## this hook is the only signal that a borrow-site bug double-returned a
    ## connection.
    conn*: PgConnection

  TracePoolCloseErrorData* = object
    ## Data passed to the pool close-error hook. Fired when a pool-initiated
    ## `conn.close()` raises — these errors are otherwise swallowed because
    ## close runs from non-async cleanup paths and fire-and-forget tasks,
    ## making leaks hard to observe without tracing.
    conn*: PgConnection
    err*: ref CatchableError

  TransportCloseStage* = enum
    ## Which transport resource raised during connection teardown.
    tcsTlsReader
    tcsTlsWriter
    tcsBaseReader
    tcsBaseWriter
    tcsTransport

  TraceTransportCloseErrorData* = object
    ## Data passed to the transport close-error hook. Fired when a chronos
    ## ``closeWait()`` call raises while ``closeTransport`` is releasing
    ## connection resources. These errors are otherwise swallowed because
    ## teardown must release every transport resource regardless of
    ## individual failures, leaving operators with no signal for half-closed
    ## TLS sessions, BearSSL ``close_notify`` mismatches, or peer RSTs.
    conn*: PgConnection
    stage*: TransportCloseStage
    err*: ref CatchableError

  CleanupKind* = enum
    ## Which automatic cleanup operation was skipped or whose failure was
    ## swallowed. Reported through `onCleanupSkipped` so operators can
    ## distinguish outer-transaction cleanup from savepoint cleanup.
    ckTxRollback ## Outer `ROLLBACK` (withTransaction / withTransactionDeadline)
    ckSavepointRollback
      ## `ROLLBACK TO SAVEPOINT` (withSavepoint / withSavepointDeadline)

  CleanupSkipReason* = enum
    ## Why an automatic cleanup operation did not run to completion.
    csrConnInvalidated
      ## The connection was already `csClosed` (typically because a per-call
      ## timeout invalidated it via `invalidateOnTimeout`). The cleanup SQL
      ## was *never dispatched* — `err` on the event is nil.
    csrCleanupFailed
      ## The cleanup SQL was dispatched but raised. The failure was
      ## swallowed so it cannot mask the original body/COMMIT error;
      ## `err` carries the cleanup failure.

  TraceCleanupSkippedData* = object
    ## Advisory notification fired from `withTransaction*` / `withSavepoint*`
    ## error-cleanup paths when ROLLBACK is either skipped (connection
    ## already invalidated) or attempted but failed (failure swallowed to
    ## preserve the original error). Useful for surfacing the diagnostic
    ## asymmetry between the timeout path (silent skip) and the body-error
    ## path (visible ROLLBACK simpleExec event).
    conn*: PgConnection
    kind*: CleanupKind
    reason*: CleanupSkipReason
    err*: ref CatchableError
      ## Cleanup-SQL failure when `reason == csrCleanupFailed`; nil when
      ## `reason == csrConnInvalidated` (nothing was dispatched).

  TraceLeakedSessionLocksData* = object
    ## Advisory notification that a pool connection returned while still
    ## holding session-level advisory locks acquired through the typed API.
    ## The pool handles cleanup itself — either running
    ## ``pg_advisory_unlock_all`` from ``resetSession`` and reusing the
    ## connection, or discarding it on ``release`` when ``resetSession`` was
    ## bypassed. Use this hook to detect missing ``advisoryUnlock`` /
    ## ``advisoryUnlockAll`` calls at the borrow site, since silent cleanup
    ## would otherwise mask the leak.
    ##
    ## A failed ``withAdvisoryLock*`` unlock (see ``onAdvisoryUnlockFailed``)
    ## does not decrement ``heldSessionLocks`` — the lock may still be held
    ## server-side — so the same lock is reported again here when its
    ## connection is returned to the pool, where ``pg_advisory_unlock_all``
    ## finally clears it. The two hooks are distinct observation points
    ## (unlock attempt vs. pool-return detection); de-duplicate per ``conn``
    ## if a single event is wanted.
    conn*: PgConnection
    count*: int ## Value of ``heldSessionLocks`` at detection time

  TraceInsecureAuthData* = object
    ## Advisory notification that a server-requested auth method is
    ## considered insecure in the current transport context. Currently fires
    ## for cleartext password over a non-SSL connection. The connection is
    ## NOT aborted — use `ConnConfig.requireAuth` for actual enforcement.
    conn*: PgConnection
    authMethod*: AuthMethod ## The method the server requested
    sslEnabled*: bool ## Transport state at the time of the auth step

  TraceDeprecatedAuthData* = object
    ## Advisory notification that a server-requested auth method is
    ## considered cryptographically weak / deprecated regardless of
    ## transport. Currently fires for MD5 (PostgreSQL recommends
    ## SCRAM-SHA-256 since v10). The connection is NOT aborted — use
    ## `ConnConfig.requireAuth` for actual enforcement.
    conn*: PgConnection
    authMethod*: AuthMethod ## The method the server requested

  TraceAdvisoryUnlockFailedData* = object
    ## Advisory notification that an explicit advisory unlock initiated by a
    ## ``withAdvisoryLock*`` macro failed. The failure is swallowed so the
    ## original exception raised by ``body`` is not masked. Session-level
    ## advisory locks are released server-side when the connection closes,
    ## so the macro's behaviour is unchanged. Use this hook to observe
    ## unlock failures that would otherwise be invisible.
    ##
    ## A failed unlock does not decrement ``heldSessionLocks``, so the same
    ## lock is also reported through ``onLeakedSessionLocks`` when its
    ## connection is later returned to the pool (where it is finally cleared).
    conn*: PgConnection
    key*: int64
      ## Lock identifier for single-key variants. Zero when ``twoKey`` is
      ## ``true``.
    key1*: int32 ## First key for two-key variants. Zero for single-key variants.
    key2*: int32 ## Second key for two-key variants. Zero for single-key variants.
    shared*: bool
      ## ``true`` for ``withAdvisoryLockShared*``, ``false`` for
      ## ``withAdvisoryLock*``.
    twoKey*: bool ## ``true`` for two-key variants, ``false`` for single-key variants.
    err*: ref CatchableError
      ## The exception raised by ``advisoryUnlock*`` /
      ## ``advisoryUnlockShared*``. ``nil`` when the unlock query itself
      ## succeeded but the server reported the lock was not held
      ## (``pg_advisory_unlock*`` returned ``false``).

  PgTracer* = ref object
    ## Tracing hooks for async-postgres operations.
    ## Set only the callbacks you need; nil callbacks are skipped with zero overhead.
    ##
    ## Start hooks return a ``TraceContext`` (opaque pointer) that is passed to the
    ## corresponding End hook for correlation (e.g. timing, span linking).
    ## Return nil from Start if you don't need correlation.
    onConnectStart*:
      proc(data: TraceConnectStartData): TraceContext {.gcsafe, raises: [].}
    onConnectEnd*:
      proc(ctx: TraceContext, data: TraceConnectEndData) {.gcsafe, raises: [].}
    onQueryStart*: proc(conn: PgConnection, data: TraceQueryStartData): TraceContext {.
      gcsafe, raises: []
    .}
    onQueryEnd*: proc(ctx: TraceContext, conn: PgConnection, data: TraceQueryEndData) {.
      gcsafe, raises: []
    .}
    onPrepareStart*: proc(conn: PgConnection, data: TracePrepareStartData): TraceContext {.
      gcsafe, raises: []
    .}
    onPrepareEnd*: proc(
      ctx: TraceContext, conn: PgConnection, data: TracePrepareEndData
    ) {.gcsafe, raises: [].}
    onPipelineStart*: proc(
      conn: PgConnection, data: TracePipelineStartData
    ): TraceContext {.gcsafe, raises: [].}
    onPipelineEnd*: proc(
      ctx: TraceContext, conn: PgConnection, data: TracePipelineEndData
    ) {.gcsafe, raises: [].}
    onCopyStart*: proc(conn: PgConnection, data: TraceCopyStartData): TraceContext {.
      gcsafe, raises: []
    .}
    onCopyEnd*: proc(ctx: TraceContext, conn: PgConnection, data: TraceCopyEndData) {.
      gcsafe, raises: []
    .}
    onPoolAcquireStart*:
      proc(data: TracePoolAcquireStartData): TraceContext {.gcsafe, raises: [].}
    onPoolAcquireEnd*:
      proc(ctx: TraceContext, data: TracePoolAcquireEndData) {.gcsafe, raises: [].}
    onPoolReleaseStart*:
      proc(data: TracePoolReleaseStartData): TraceContext {.gcsafe, raises: [].}
    onPoolReleaseEnd*:
      proc(ctx: TraceContext, data: TracePoolReleaseEndData) {.gcsafe, raises: [].}
    onPoolDoubleRelease*: proc(data: TracePoolDoubleReleaseData) {.gcsafe, raises: [].}
      ## Fires when `release(conn)` is called on a connection that is not
      ## currently checked out (a duplicate release, or a connection not
      ## borrowed from this pool). Advisory only — the duplicate release is a
      ## no-op, which is what keeps the same connection from being handed to
      ## two borrowers. Use this to surface double-`release` bugs at the
      ## borrow site.
    onPoolCloseError*: proc(data: TracePoolCloseErrorData) {.gcsafe, raises: [].}
    onTransportCloseError*:
      proc(data: TraceTransportCloseErrorData) {.gcsafe, raises: [].}
      ## Fires when a transport ``closeWait()`` raises during teardown.
      ## Advisory only — ``closeTransport`` continues releasing the remaining
      ## resources regardless. Use this to surface half-closed TLS sessions
      ## or peer RSTs that would otherwise be invisible.
    onLeakedSessionLocks*:
      proc(data: TraceLeakedSessionLocksData) {.gcsafe, raises: [].}
      ## Fires when a pool connection returns holding session-level advisory
      ## locks acquired through the typed API. Advisory only — the pool
      ## handles cleanup as described in `TraceLeakedSessionLocksData`. Use
      ## this to surface missing ``advisoryUnlock`` calls at the borrow site.
      ## May also fire for a lock whose ``withAdvisoryLock*`` unlock already
      ## failed via `onAdvisoryUnlockFailed`; see that type for details.
    onCleanupSkipped*: proc(data: TraceCleanupSkippedData) {.gcsafe, raises: [].}
      ## Fires from `withTransaction*` / `withSavepoint*` error paths when
      ## an automatic ROLLBACK is either skipped (connection already
      ## `csClosed`, e.g. after a per-call timeout) or attempted but failed
      ## (failure swallowed to keep the original error). Advisory only —
      ## the macro's behaviour is unchanged. Use this to close the
      ## diagnostic gap between the timeout path (silent) and the body-
      ## error path (visible ROLLBACK simpleExec event).
      ##
      ## **Nested macros may fire this hook more than once per failure.**
      ## When `withSavepoint*` is nested inside `withTransaction*` and the
      ## connection becomes `csClosed`, the savepoint's error handler
      ## fires `ckSavepointRollback` first, then the original exception
      ## propagates to the outer transaction's handler which sees the same
      ## `csClosed` state and fires `ckTxRollback`. Both events refer to
      ## the same underlying cause; observers that aggregate by failure
      ## (not by cleanup attempt) should dedupe.
    onInsecureAuth*: proc(data: TraceInsecureAuthData) {.gcsafe, raises: [].}
      ## Fires when an auth method is used over an insecure transport
      ## (currently: cleartext password without SSL). Advisory only; does
      ## not abort the connection. Use `ConnConfig.requireAuth` to enforce.
    onDeprecatedAuth*: proc(data: TraceDeprecatedAuthData) {.gcsafe, raises: [].}
      ## Fires when a server-requested auth method is cryptographically
      ## weak / deprecated regardless of transport (currently: MD5).
      ## Advisory only; does not abort the connection. Use
      ## `ConnConfig.requireAuth` to enforce.
    onAdvisoryUnlockFailed*:
      proc(data: TraceAdvisoryUnlockFailedData) {.gcsafe, raises: [].}
      ## Fires when ``withAdvisoryLock*`` / ``withAdvisoryLockShared*``
      ## swallows an ``advisoryUnlock*`` failure to preserve the original
      ## exception from ``body``. The unlock is considered failed when it
      ## raises (``data.err`` non-nil) or returns ``false`` (``data.err``
      ## nil). Advisory only — the macro's behaviour is unchanged. Use this
      ## to observe unlock failures that would otherwise be invisible.

type RowCallback* = proc(row: Row) {.raises: [CatchableError], gcsafe.}
  ## Callback invoked once per row during `queryEach`. The `Row` is only valid
  ## inside the callback — its backing buffer is reused for the next row.

when hasChronos:
  type CopyOutCallback* = proc(data: sink seq[byte]): Future[void] {.
    async: (raises: [CatchableError]), gcsafe
  .}
    ## Callback receiving each chunk during streaming COPY OUT. ``data`` is
    ## ``sink`` so the receive buffer moves in without a copy.

  type CopyInCallback* =
    proc(): Future[seq[byte]] {.async: (raises: [CatchableError]), gcsafe.}
    ## Callback supplying data chunks during streaming COPY IN. Return empty seq to finish.

else:
  type CopyOutCallback* = proc(data: sink seq[byte]): Future[void] {.gcsafe.}
    ## Callback receiving each chunk during streaming COPY OUT. ``data`` is
    ## ``sink`` so the receive buffer moves in without a copy.

  type CopyInCallback* = proc(): Future[seq[byte]] {.gcsafe.}
    ## Callback supplying data chunks during streaming COPY IN. Return empty seq to finish.

const RecvBufSize* = 131072 ## Size of the temporary read buffer for recv operations

# HostEntry accessors

func dialAddr*(entry: HostEntry): string {.inline.} =
  ## The address actually dialed: `hostaddr` bypasses name resolution when
  ## given, otherwise `host` is resolved (libpq semantics).
  if entry.hostaddr.len > 0: entry.hostaddr else: entry.host

func displayHost*(entry: HostEntry): string {.inline.} =
  ## Host name for display and back-compat scalars: `host`, falling back to
  ## `hostaddr` (mirrors libpq's PQhost()).
  if entry.host.len > 0: entry.host else: entry.hostaddr

# Internal accessor for cross-module use within the library

func effectiveMaxMessageSize*(conn: PgConnection): int {.inline.} =
  ## Effective per-message recv cap for this connection. Resolves the
  ## ``ConnConfig.maxMessageSize`` default (0) to ``DefaultMaxBackendMessageLen``.
  if conn.config.maxMessageSize > 0:
    conn.config.maxMessageSize
  else:
    DefaultMaxBackendMessageLen

# Internal replication LSN plumbing (cross-module use within the library)
#
# `pg_replication` owns the typed `Lsn` API (`confirmFlushed`,
# `confirmedFlushLsn`); `types` stores raw `uint64` values to avoid a dependency
# cycle. The procedures below are exported so `pg_replication` can use them
# without `{.all.}`, but they are intentionally low-level: the public way to
# advance the confirmed-flush position remains `confirmFlushed` in
# `pg_replication`, which adds the `Lsn` typing and the received-WAL bound check.

proc initReplLsnTracking*(conn: PgConnection, startLsn: uint64) =
  ## Reset the per-stream confirmed-flush and max-received positions to
  ## `startLsn`. Called at the beginning of each replication stream so a reused
  ## connection never inherits stale values from a previous stream.
  conn.replConfirmedFlushLsnRaw = startLsn
  conn.replMaxReceivedLsnRaw = startLsn

proc updateReplMaxReceivedLsn*(conn: PgConnection, received: uint64): bool =
  ## Advance the highest-received WAL position if `received` is greater than
  ## the current value. Returns `true` if the value was updated.
  if received > conn.replMaxReceivedLsnRaw:
    conn.replMaxReceivedLsnRaw = received
    true
  else:
    false

func replConfirmedFlushLsn*(conn: PgConnection): uint64 =
  ## Raw confirmed-flush LSN. Public API users should use `confirmedFlushLsn`
  ## in `pg_replication`.
  conn.replConfirmedFlushLsnRaw

func replMaxReceivedLsn*(conn: PgConnection): uint64 =
  ## Raw max-received LSN. Public API users should use the `confirmFlushed`
  ## bounds via the typed API in `pg_replication`.
  conn.replMaxReceivedLsnRaw

proc confirmReplFlushed*(conn: PgConnection, lsn: uint64): bool =
  ## Clamp `lsn` to the max-received WAL position and advance the
  ## confirmed-flush LSN only monotonically. Returns `true` if the position
  ## moved forward. This is the raw, untyped helper used by `confirmFlushed`
  ## in `pg_replication`; public callers should use that typed API.
  let bounded =
    if lsn > conn.replMaxReceivedLsnRaw: conn.replMaxReceivedLsnRaw else: lsn
  if bounded > conn.replConfirmedFlushLsnRaw:
    conn.replConfirmedFlushLsnRaw = bounded
    true
  else:
    false

proc newPgQueryError*(fields: seq[ErrorField]): ref PgQueryError =
  ## Create a PgQueryError from server ErrorResponse fields.
  let sqlState = getErrorField(fields, 'C')
  let severity = getErrorField(fields, 'S')
  let detail = getErrorField(fields, 'D')
  let hint = getErrorField(fields, 'H')
  result = (ref PgQueryError)(
    msg: formatError(fields),
    sqlState: sqlState,
    severity: severity,
    detail: detail,
    hint: hint,
    fields: fields,
  )

# Tracer fire helpers (cross-module use)

proc fireInsecureAuth*(conn: PgConnection, authMethod: AuthMethod) =
  let t = conn.config.tracer
  if t != nil and t.onInsecureAuth != nil:
    t.onInsecureAuth(
      TraceInsecureAuthData(
        conn: conn, authMethod: authMethod, sslEnabled: conn.sslEnabled
      )
    )

proc fireDeprecatedAuth*(conn: PgConnection, authMethod: AuthMethod) =
  let t = conn.config.tracer
  if t != nil and t.onDeprecatedAuth != nil:
    t.onDeprecatedAuth(TraceDeprecatedAuthData(conn: conn, authMethod: authMethod))

proc fireAdvisoryUnlockFailed*(
    conn: PgConnection,
    key: int64,
    key1, key2: int32,
    shared, twoKey: bool,
    err: ref CatchableError,
) =
  ## Route a swallowed ``withAdvisoryLock*`` / ``withAdvisoryLockShared*``
  ## unlock failure to the tracer. Reads from ``conn.config.tracer`` so the
  ## event fires regardless of the runtime ``conn.tracer`` alias. Nil hook
  ## is a no-op.
  let t = conn.config.tracer
  if t != nil and t.onAdvisoryUnlockFailed != nil:
    t.onAdvisoryUnlockFailed(
      TraceAdvisoryUnlockFailedData(
        conn: conn,
        key: key,
        key1: key1,
        key2: key2,
        shared: shared,
        twoKey: twoKey,
        err: err,
      )
    )

proc fireCleanupSkipped*(
    conn: PgConnection,
    kind: CleanupKind,
    reason: CleanupSkipReason,
    err: ref CatchableError = nil,
) =
  ## Route a `withTransaction*` / `withSavepoint*` ROLLBACK skip-or-swallow
  ## event to the tracer. Reads from ``conn.config.tracer`` so events fire
  ## regardless of the runtime ``conn.tracer`` alias. Nil hook is a no-op.
  let t = conn.config.tracer
  if t != nil and t.onCleanupSkipped != nil:
    t.onCleanupSkipped(
      TraceCleanupSkippedData(conn: conn, kind: kind, reason: reason, err: err)
    )

when hasChronos:
  proc fireTransportCloseError*(
      conn: PgConnection, stage: TransportCloseStage, err: ref CatchableError
  ) =
    ## Route a swallowed transport close error to the tracer. ``closeTransport``
    ## must continue releasing the remaining resources, so the error cannot be
    ## propagated to a caller — tracing is the only signal operators have.
    ## Reads from ``conn.config.tracer`` so events fire even when teardown
    ## happens before the runtime tracer alias has been assigned.
    let t = conn.config.tracer
    if t != nil and t.onTransportCloseError != nil:
      t.onTransportCloseError(
        TraceTransportCloseErrorData(conn: conn, stage: stage, err: err)
      )

# Tracing helper templates

template withConnTracing*(
    conn: PgConnection,
    startHook, endHook: untyped,
    startData: typed,
    EndDataType: typedesc,
    endDataExpr: typed,
    body: untyped,
) =
  ## Wrap an operation with connection-scoped tracing hooks.
  var traceCtx {.inject.}: TraceContext
  if conn.tracer != nil and conn.tracer.startHook != nil:
    traceCtx = conn.tracer.startHook(conn, startData)
  try:
    body
  except CatchableError as e:
    if conn.tracer != nil and conn.tracer.endHook != nil:
      conn.tracer.endHook(traceCtx, conn, EndDataType(err: e))
    raise e
  if conn.tracer != nil and conn.tracer.endHook != nil:
    conn.tracer.endHook(traceCtx, conn, endDataExpr)

template withTracing*(
    tracer: PgTracer,
    startHook, endHook: untyped,
    startData: typed,
    EndDataType: typedesc,
    endDataExpr: typed,
    body: untyped,
) =
  ## Wrap an operation with non-connection tracing hooks (connect, pool).
  var traceCtx {.inject.}: TraceContext
  if tracer != nil and tracer.startHook != nil:
    traceCtx = tracer.startHook(startData)
  try:
    body
  except CatchableError as e:
    if tracer != nil and tracer.endHook != nil:
      tracer.endHook(traceCtx, EndDataType(err: e))
    raise e
  if tracer != nil and tracer.endHook != nil:
    tracer.endHook(traceCtx, endDataExpr)
