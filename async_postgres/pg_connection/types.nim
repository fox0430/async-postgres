## Shared building blocks for ``pg_connection`` submodules (``PgConnection``, ``ConnConfig``, tracing).
##
## Internal module: not part of the public API. Import the `pg_connection` hub
## instead; what it re-exports is the supported surface (see
## `tests/api_surface.golden`).

import std/[tables, sets, deques, lists]
when defined(posix):
  import std/posix

import ../[async_backend, pg_auth, pg_errors, pg_protocol, pg_types]

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

const closedByUserMsg* = "Connection closed by the application"
  ## Shared by `failNotifyWaiter` and `checkListenAlive` so a deliberate
  ## `close()` reports the same thing whether a waiter was parked or not.

var listenReconnectStopWaitMs* = 10_000
  ## Max wait (ms) for a listen pump stuck in a blocking `connect()`; it is
  ## orphaned on timeout. Not re-exported through `pg_connection`, so call
  ## sites cannot set it to 0 via the aggregate import and disable orphan safety.

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

  PgClosedReason* = enum
    ## Why a connection is unusable (see `closedReason`).
    crOpen ## Not closed.
    crClosedByUser ## `close()` was called by the application.
    crClosed ## The connection died on its own.

  SslMode* = enum
    ## SSL mode. Zero value is ``sslDisable`` (raw ``ConnConfig``); ``parseDsn``/
    ## ``initConnConfig`` default to ``sslPrefer`` (libpq parity).
    ## Backend divergence: chronos/BearSSL rejects expired certs and lacks IP SAN
    ## for ``sslVerifyFull``; asyncdispatch matches libpq.
    sslDisable ## Disable SSL
    sslAllow ## Try plaintext; fall back to SSL if refused
    sslPrefer ## Try SSL; fall back to plaintext if refused (libpq default)
    sslRequire ## Require SSL (no certificate verification)
    sslVerifyCa ## Require SSL + verify CA chain (no hostname verification)
    sslVerifyFull ## Require SSL + verify CA chain and hostname

  SslNegotiation* = enum
    ## SSL negotiation method for the connection.
    sslnPostgres ## Traditional SSLRequest negotiation (default)
    sslnDirect ## Direct SSL: start TLS immediately without SSLRequest (PostgreSQL 17+)

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
    ## Host ordering (libpq ``load_balance_hosts``).
    lbhDisable ## Configured order (default)
    lbhRandom ## Shuffle host list per connection (replica spread)

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
    sslNegotiation*: SslNegotiation
      ## SSL negotiation method (default: `sslnPostgres`). A raw zero-initialized
      ## `ConnConfig` matches because `sslnPostgres` is the enum's zero value,
      ## which is locked in by a `static:` assertion on `SslNegotiation`.
    sslRootCert*: string ## PEM-encoded CA certificate(s) for sslVerifyCa/sslVerifyFull
    sslCert*: string
      ## PEM-encoded client certificate (and any intermediates) for mutual TLS.
      ## Must be paired with ``sslKey``; ``sslMode`` must also be ``sslPrefer``
      ## or stronger, otherwise TLS would not be negotiated and the credential
      ## would be silently unused — config validation rejects that.
    sslKey*: string
      ## PEM-encoded client private key for mutual TLS. The key must be
      ## **unencrypted** on both backends (no passphrase callback is wired up).
      ## On chronos/BearSSL specifically it must be PKCS#8 (RSA or EC); PKCS#1
      ## is not supported. Must be paired with ``sslCert``.
    sslSni*: bool ## Send TLS SNI (default true; suppressed for IP/empty host).
    channelBinding*: ChannelBindingMode
      ## SCRAM channel binding policy (default cbPrefer). `cbRequire` fails the
      ## connection if SCRAM-SHA-256-PLUS cannot actually be used (libpq parity).
    requireAuth*: set[AuthMethod]
      ## Allowed auth methods; empty = any (libpq ``require_auth`` parity).
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
      ## Max backend message size (0 = 1 GiB default); larger → ``PgProtocolError``.
    maxScramIterations*: int
      ## Upper bound on the server-requested SCRAM iteration count
      ## (PostgreSQL 16+ `scram_iterations`), capping CPU spent in PBKDF2.
      ## ``0`` (default) selects `DefaultMaxScramIterations` (10,000,000).
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

  ReconnectCallback* = proc() {.gcsafe, raises: [].}
    ## Callback invoked after the listen pump reconnects and re-subscribes.

  NotifyOverflowCallback* = proc(dropped: int) {.gcsafe, raises: [].}
    ## Callback invoked when the pull-API queue overflows. ``dropped`` counts
    ## what this one arrival discarded; ``notifyDropped`` is the running count
    ## since the last overflow ``waitNotification`` reported.

  ListenErrorCallback* = proc(err: ref PgListenError) {.gcsafe, raises: [].}
    ## Callback invoked when the listen pump dies permanently.

  CachedStmt* = ref object ## Cached prepared statement (LRU).
    name*: string ## Server-side name (``_sc_*``)
    fields*: seq[FieldDescription] ## Describe result
    paramOids*: seq[int32]
      ## Parse-time param OIDs; mismatch → re-parse (empty = no params)
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
      transport: StreamTransport
      baseReader: AsyncStreamReader
      baseWriter: AsyncStreamWriter
      reader: AsyncStreamReader
      writer: AsyncStreamWriter
      tlsStream: TLSAsyncStream
      trustAnchorBufs: seq[seq[byte]] ## Backing memory for custom trust anchor pointers
      x509Capture: X509CertCaptureContext ## X509 wrapper for cert capture
    elif hasAsyncDispatch:
      socket: AsyncSocket
    serverCertDer: seq[byte] ## DER-encoded server certificate for SCRAM channel binding
    sslEnabled: bool
    recvBuf: seq[byte]
    recvBufStart: int ## Read pointer into recvBuf; bytes before this are consumed
    state: PgConnState
    pendingSyncs: int
      ## ``ReadyForQuery`` replies the backend still owes: one per simple
      ## ``Query`` and per ``Sync`` written, less one per reply read. A count,
      ## not a flag: a pipelined batch owes several. Read via ``wireSettled``.
    unsyncedWrite: bool
      ## Replies were asked for past the last sync point, so nothing on the
      ## wire will end them (an extended-query batch stopping at ``Flush``, as
      ## every cursor round trip does). Only a later write carrying a sync
      ## point clears it — a reply owed for an earlier one says nothing about
      ## requests written after it.
      ##
      ## Both are separate from ``state``: the read path marks ``csClosed`` as
      ## soon as a read dies, while the requests it was reading for are still
      ## running server-side and are what a ``CancelRequest`` must abort.
    pid: int32
    secretKey: int32
    serverParams: Table[string, string]
    negotiatedMinorVersion: int32
      ## Highest minor version the server supports, from `NegotiateProtocolVersion`.
      ## Zero when no such message was seen.
    unrecognizedStartupOptions: seq[string]
      ## `_pq_.*` options the server rejected, from `NegotiateProtocolVersion`.
    txStatus: TransactionStatus
    notifyCallback: NotifyCallback
    noticeCallback: NoticeCallback
    listenChannels: HashSet[string]
    borrowedByUser: bool
      ## Handed to the application (not a pool-internal borrow); see ``release``.
    stagedStmtCloses: seq[string]
      ## Names taken out of ``pendingStmtCloses`` by a build that wrote their
      ## ``Close`` into its buffer. Held until that send succeeds, so an
      ## aborted build leaves them owed rather than lost.
    when defined(pgStateChecks):
      sendBufStaged: bool
        ## Debug-only: a build has staged its ``Close`` messages and the send
        ## that clears them has not run yet. See ``requireStaged``.
    transportCloseFut: Future[void]
      ## In-flight ``closeTransport``; a second teardown awaits it.
    listenTask: Future[void]
    listenStopRequested: bool
      ## Set by `stopListening` or `close()` to ask the background pump to exit.
      ## The pump checks it at every yield point of its auto-reconnect loop, and
      ## `reconnectInPlace` checks it right after `connect()` returns to discard
      ## the fresh transport instead of grafting it. Left set deliberately when a
      ## pump is orphaned on timeout — clearing it would rearm that graft.
    listenReconnecting: bool
      ## True while the pump is inside its auto-reconnect loop. Tells
      ## `stopListening` that the empty-query unblock it normally uses would race
      ## the reconnect's own LISTEN round trips, so it must wait for the pump to
      ## observe `listenStopRequested` instead of sending the query.
    host: string
    port: int
    createdAt: Moment
    portalCounter: int
    config: ConnConfig
    notifyQueue: Deque[Notification]
    notifyMaxQueue: int
      ## Pull-API queue cap (1024 default; <=0 = unbounded). A handoff and a
      ## requeue of it sit outside the queue, so at most ``notifyMaxQueue + 1``
      ## notifications are retained.
    notifyWaiter: Future[void]
    notifyHandoff: Notification
      ## Reserved handoff for completed ``notifyWaiter`` (see ``hasNotifyHandoff``).
    hasNotifyHandoff: bool
    sendBuf: seq[byte] ## Reusable send buffer for COPY IN batching
    notifyDropped: int ## Count of notifications dropped due to queue overflow
    listenError: ref PgListenError ## Set when listen pump fails permanently
    closedByUser: bool
      ## Set by `close()`, one-way. Keeps a deliberate close out of
      ## `PgConnectionError` reconnect loops (see `closedReason`).
    listenReconnectMaxAttempts: int
      ## Max reconnect attempts on listen pump failure. Default 10.
      ## 0 or negative = unlimited retries (retry until close()).
    listenReconnectMaxBackoff: int
      ## Max seconds between reconnect attempts (backoff cap). Default 30.
    reconnectCallback: ReconnectCallback
    notifyOverflowCallback: NotifyOverflowCallback
    listenErrorCallback: ListenErrorCallback
      ## Invoked when the listen pump dies permanently (reconnection failed or
      ## the connection was lost with nothing left to re-subscribe). Lets push
      ## API (`onNotify`) users learn the pump is gone — the pull API surfaces
      ## the same failure through `waitNotification`.
    stmtCache: Table[string, CachedStmt]
    stmtCacheLru: DoublyLinkedList[string] ## LRU order: oldest at head, newest at tail
    stmtCounter: int
    stmtCacheCapacity: int ## 0=disabled, default 256
    pendingStmtCloses: seq[string]
      ## Server-side prepared statement names whose ``Close`` was not bundled
      ## with the operation that evicted them. Populated when the defensive
      ## eviction loop in ``addStmtCache`` fires (caller skipped the
      ## pre-eviction step, or ``stmtCacheCapacity`` was shrunk below the
      ## current cache size). Staged by ``stagePendingStmtCloses`` at the
      ## start of the next Extended Query send phase so the leak is bounded
      ## to the gap until the next operation, which moves them to
      ## ``stagedStmtCloses``.
    heldSessionLocks: int
      ## Count of session-level `pg_advisory_lock` acquires through the typed
      ## API, minus tracked releases. Reported (best-effort) by the
      ## `onLeakedSessionLocks` tracer hook. Raw-SQL acquires
      ## (`conn.exec("SELECT pg_advisory_lock(...)")`) bypass the increment,
      ## but a typed `advisoryUnlock` of a raw-acquired key still decrements
      ## the counter when the server reports the release — under mixed
      ## typed/raw usage the counter therefore *under-reports* the true
      ## number of tracked locks still held. The pool's reset/discard
      ## decision and the leak-hook trigger both key off `sessionLockDirty`
      ## instead, so a mis-decremented counter cannot leak a tracked lock
      ## into the next borrower or silence the leak signal.
    sessionLockDirty: bool
      ## Sticky flag: set on any tracked session-level acquire, cleared only
      ## by `advisoryUnlockAll` or connection reset. Drives the pool's
      ## reset/discard decision so `pg_advisory_unlock_all` runs whenever a
      ## tracked acquire ever happened, even if the tracked counter was
      ## decremented back to zero by a typed unlock of a raw-acquired key.
    tracer: PgTracer ## Inherited from ConnConfig on connect
    ownerPool: PgPoolOwner
      ## Owning pool back-reference. Set when this connection is managed by
      ## a `PgPool` (or a pool inside `PgPoolCluster`); `nil` for standalone
      ## connections created via `connect`. Used by `release(conn)` to route
      ## the connection back to the correct pool.
    borrowed: bool
      ## Checked-out from pool? Guards idle double-release (not waiter-handoff double-release; use ``PooledConnHandle``).
    replConfirmedFlushLsnRaw: uint64
      ## Replication: confirmed flush LSN (raw; see ``pg_replication``).
    replMaxReceivedLsnRaw: uint64 ## Replication: max received LSN (raw).
    replReadScratch: seq[byte] ## Chronos scratch for ``fillRecvBufDetached``.
    replCopyDoneSent: bool ## Client already sent CopyDone (skip mirror).

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

  TracePoolDoubleReleaseData* = object ## Double-release hook data (no-op release).
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

  TraceTransportCloseErrorData* = object ## Transport close-error hook data.
    conn*: PgConnection
    stage*: TransportCloseStage
    err*: ref CatchableError

  CleanupKind* = enum ## Cleanup operation kind.
    ckTxRollback ## Outer ``ROLLBACK``
    ckSavepointRollback ## ``ROLLBACK TO SAVEPOINT``

  CleanupSkipReason* = enum ## Why cleanup didn't complete.
    csrConnInvalidated ## Already ``csClosed`` — not dispatched (``err=nil``)
    csrCleanupFailed ## Dispatched but raised (``err`` carries failure)

  TraceCleanupSkippedData* = object ## ROLLBACK skipped/failed advisory (advisory only).
    conn*: PgConnection
    kind*: CleanupKind
    reason*: CleanupSkipReason
    err*: ref CatchableError

  TraceLeakedSessionLocksData* = object
    ## Leaked advisory-lock advisory (fires on ``sessionLockDirty``).
    conn*: PgConnection
    count*: int ## ``heldSessionLocks`` at detection (0 = counter unreliable).

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

  TraceAdvisoryUnlockFailedData* = object ## Swallowed unlock-failure advisory.
    conn*: PgConnection
    key*: int64 ## Single-key id (0 if ``twoKey``)
    key1*: int32 ## First key (two-key only)
    key2*: int32 ## Second key (two-key only)
    shared*: bool ## Shared lock?
    twoKey*: bool ## Two-key variant?
    err*: ref CatchableError ## Nil = unlock returned false (not held)

  PgTracer* = ref object
    ## Tracing hooks (nil = skipped; Start → ``TraceContext`` → End).
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
      ## Duplicate release (no-op).
    onPoolCloseError*: proc(data: TracePoolCloseErrorData) {.gcsafe, raises: [].}
    onTransportCloseError*:
      proc(data: TraceTransportCloseErrorData) {.gcsafe, raises: [].}
      ## Swallowed ``closeWait`` error.
    onLeakedSessionLocks*:
      proc(data: TraceLeakedSessionLocksData) {.gcsafe, raises: [].}
      ## Leaked advisory locks on pool return.
    onCleanupSkipped*: proc(data: TraceCleanupSkippedData) {.gcsafe, raises: [].}
      ## Skipped/failed ROLLBACK (may fire twice when nested).
    onInsecureAuth*: proc(data: TraceInsecureAuthData) {.gcsafe, raises: [].}
      ## Insecure auth over plaintext.
    onDeprecatedAuth*: proc(data: TraceDeprecatedAuthData) {.gcsafe, raises: [].}
      ## Weak auth (MD5).
    onAdvisoryUnlockFailed*:
      proc(data: TraceAdvisoryUnlockFailedData) {.gcsafe, raises: [].}
      ## Swallowed unlock failure.

static:
  # Zero-initialized ConnConfig must default to sslnPostgres.
  doAssert ord(sslnPostgres) == 0

type RowCallback* = proc(row: Row) {.raises: [CatchableError], gcsafe.}
  ## Callback invoked once per row during `queryEach`. The `Row` is only valid
  ## inside the callback — its backing buffer is reused for the next row.

declareAsyncCallback(
  CopyOutCallback, proc(data: sink seq[byte]): Future[void],
  "Callback receiving each chunk during streaming COPY OUT. `data` is `sink` so the receive buffer moves in without a copy.",
)

declareAsyncCallback(
  CopyInCallback, proc(): Future[seq[byte]],
  "Callback supplying data chunks during streaming COPY IN. Return empty seq to finish.",
)

const
  RecvBufSize* = 131072 ## Size of the temporary read buffer for recv operations

  ClientCertPairingErrorMsg* =
    "sslcert and sslkey must be provided together for client certificate auth"
    ## Shared so the wording can't drift between the config-time and
    ## connect-time checks.

proc warnStderr*(msg: string) =
  ## Connection-path warnings must never fail the connection: stderr may be
  ## closed or broken (e.g. daemonized process), so swallow the IOError.
  try:
    stderr.writeLine msg
  except IOError:
    discard

proc validateClientCertConfig*(config: ConnConfig) =
  ## Reject inconsistent client certificate configurations early (at config
  ## build time, before any connection is opened). Both halves of an mTLS
  ## credential must be present together, and the SSL mode must actually
  ## negotiate TLS — otherwise the cert/key would be silently ignored.
  if (config.sslCert.len > 0) xor (config.sslKey.len > 0):
    raise newException(PgConfigError, ClientCertPairingErrorMsg)
  if (config.sslCert.len > 0 or config.sslKey.len > 0) and
      config.sslMode in {sslDisable, sslAllow}:
    raise newException(
      PgConfigError,
      "sslcert/sslkey require sslmode of prefer or stronger (got " & $config.sslMode &
        "); they would otherwise be silently unused",
    )

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

func effectiveMaxScramIterations*(config: ConnConfig): int {.inline.} =
  ## Resolves the ``ConnConfig.maxScramIterations`` default (0) to
  ## ``DefaultMaxScramIterations``.
  if config.maxScramIterations > 0:
    config.maxScramIterations
  else:
    DefaultMaxScramIterations

# Replication LSN plumbing (low-level; public API is ``pg_replication.confirmFlushed``).

proc initReplLsnTracking*(conn: PgConnection, startLsn: uint64) =
  ## Reset per-stream LSN tracking to ``startLsn``.
  conn.replConfirmedFlushLsnRaw = startLsn
  conn.replMaxReceivedLsnRaw = startLsn

proc updateReplMaxReceivedLsn*(conn: PgConnection, received: uint64): bool =
  ## Advance max-received LSN if ``received`` is greater; return if updated.
  if received > conn.replMaxReceivedLsnRaw:
    conn.replMaxReceivedLsnRaw = received
    true
  else:
    false

func replConfirmedFlushLsn*(conn: PgConnection): uint64 =
  ## Raw flush LSN (use typed API).
  conn.replConfirmedFlushLsnRaw
func replMaxReceivedLsn*(conn: PgConnection): uint64 = ## Raw max-received LSN.
  conn.replMaxReceivedLsnRaw

proc confirmReplFlushed*(conn: PgConnection, lsn: uint64): bool =
  ## Clamp to max-received and advance flush monotonically (raw helper).
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

# Connection state transitions

func wireSettled*(conn: PgConnection): bool {.inline.} =
  ## True when the backend owes nothing: every request written has been
  ## answered to its ``ReadyForQuery``, so the stream is parked on a message
  ## boundary and the connection is safe to hand to someone else.
  ##
  ## Sole reader of the two fields behind it, so no caller can settle for the
  ## half of the question that suits it.
  conn.pendingSyncs == 0 and not conn.unsyncedWrite

when defined(pgStateChecks):
  proc checkBorrowable*(conn: PgConnection) {.raises: [].} =
    ## Debug-only guard on what a borrower finding ``csReady`` assumes, compiled
    ## in with ``-d:pgStateChecks`` (the test suite; see ``tests/config.nims``).
    ##
    ## Checked at ``checkReady`` — where the assumption is used — not where the
    ## connection was handed back: a frame may leave the wire in a shape it
    ## cleans up itself, and only ``csReady`` promises anything to the next
    ## borrower. ``invalidateWire`` asks ``wireSettled`` before handing a
    ## connection back; this asserts it on the path that never asks — an
    ## operation that finished normally.
    doAssert conn.wireSettled,
      "csReady with " & $conn.pendingSyncs & " reply(s) owed and unsyncedWrite=" &
        $conn.unsyncedWrite & ": the next read would land mid-reply"

proc markState*(conn: PgConnection, next: PgConnState) {.inline, raises: [].} =
  ## Sole writer of ``state``, the field every reuse decision reads. Use
  ## ``markReady`` / ``markBusy`` / ``markClosed`` for the query path; this one
  ## is for the states a single owner drives (``csConnecting``,
  ## ``csAuthentication``, ``csListening``, ``csReplicating``).
  conn.state = next

proc markReady*(conn: PgConnection) {.inline, raises: [].} =
  ## Give the connection back: no operation owns the wire any more. See
  ## ``checkBorrowable`` for what the next borrower assumes.
  conn.markState(csReady)

proc markBusy*(conn: PgConnection) {.inline, raises: [].} =
  ## Take the wire for one operation. Held until that operation reads its last
  ## reply (``markReady``) or dies on the wire (``markClosed``).
  conn.markState(csBusy)

proc markClosed*(conn: PgConnection) {.inline, raises: [].} =
  ## Retire the connection: the wire is unusable, whether the transport is torn
  ## down yet or not.
  conn.markState(csClosed)

# Staged statement-close bookkeeping
#
# Only a completed send licenses forgetting the staged names. Staging and
# sending sit in different procs (different files for the direct macros), so
# the pairing cannot be a lexical scope; these three make it checkable.

proc markStaged*(conn: PgConnection) {.inline, raises: [].} =
  ## A build staged its queued ``Close`` messages into the bytes about to go out.
  when defined(pgStateChecks):
    conn.sendBufStaged = true

proc requireStaged*(conn: PgConnection, what: string) {.inline, raises: [].} =
  ## Reject `what` when nothing was staged for it: dropping names whose
  ## ``Close`` was never written leaks those statements for the session, and
  ## does so silently.
  when defined(pgStateChecks):
    doAssert conn.sendBufStaged,
      what & " with no staging behind it: " &
        "the queued statement Closes were never written"

proc clearStaged*(conn: PgConnection) {.inline, raises: [].} =
  ## The staged bytes went out and their names have been forgotten.
  ##
  ## Only the drop disarms; abandoning the queue (``clearStmtCache``) leaves it
  ## armed, so a later drop still finds an empty staged list rather than
  ## tripping this guard.
  when defined(pgStateChecks):
    conn.sendBufStaged = false

# Public accessors
#
# The record's fields are private; everything an application is meant to read
# or tune goes through this section, so the supported surface is one list
# rather than "whatever happens to be exported".

func pid*(conn: PgConnection): int32 {.inline.} =
  ## Backend process id from ``BackendKeyData``; 0 until startup completes.
  conn.pid

func host*(conn: PgConnection): lent string {.inline.} =
  ## Host this connection actually reached (a multi-host DSN picks one).
  conn.host

func port*(conn: PgConnection): int {.inline.} =
  ## Port this connection actually reached.
  conn.port

func config*(conn: PgConnection): lent ConnConfig {.inline.} =
  ## Configuration this connection was opened with.
  conn.config

func createdAt*(conn: PgConnection): Moment {.inline.} =
  ## When the connection was established.
  conn.createdAt

func sslEnabled*(conn: PgConnection): bool {.inline.} =
  ## Whether the transport is TLS-wrapped.
  conn.sslEnabled

func serverParams*(conn: PgConnection): lent Table[string, string] {.inline.} =
  ## ``ParameterStatus`` values reported by the server (``server_version``,
  ## ``client_encoding``, ...), kept current as the server re-sends them.
  conn.serverParams

func serverParam*(conn: PgConnection, name: string): string =
  ## One ``ParameterStatus`` value, or ``""`` when the server never sent it.
  conn.serverParams.getOrDefault(name, "")

func notifyDropped*(conn: PgConnection): int {.inline.} =
  ## Notifications dropped by pull-API queue overflow since the last
  ## ``PgNotifyOverflowError``. Not a lifetime total: ``waitNotification``
  ## reports the count in that error and resets it to zero.
  conn.notifyDropped

func listenError*(conn: PgConnection): ref PgListenError {.inline.} =
  ## Why the listen pump died permanently, or ``nil`` while it is alive.
  conn.listenError

func notifyMaxQueue*(conn: PgConnection): int {.inline.} =
  ## Pull-API queue cap; see `notifyMaxQueue=`.
  conn.notifyMaxQueue

proc `notifyMaxQueue=`*(conn: PgConnection, value: int) {.inline.} =
  ## Cap the pull-API queue (1024 default; <=0 = unbounded). Overflow drops
  ## the oldest entry and fires `onNotifyOverflow`.
  conn.notifyMaxQueue = value

func listenReconnectMaxAttempts*(conn: PgConnection): int {.inline.} =
  ## Reconnect attempt budget; see `listenReconnectMaxAttempts=`.
  conn.listenReconnectMaxAttempts

proc `listenReconnectMaxAttempts=`*(conn: PgConnection, value: int) {.inline.} =
  ## Max reconnect attempts on listen-pump failure (10 default; <=0 = retry
  ## until `close`).
  conn.listenReconnectMaxAttempts = value

func listenReconnectMaxBackoff*(conn: PgConnection): int {.inline.} =
  ## Backoff cap in seconds; see `listenReconnectMaxBackoff=`.
  conn.listenReconnectMaxBackoff

proc `listenReconnectMaxBackoff=`*(conn: PgConnection, value: int) {.inline.} =
  ## Cap the seconds between listen-pump reconnect attempts (30 default).
  conn.listenReconnectMaxBackoff = value

func stmtCacheCapacity*(conn: PgConnection): int {.inline.} =
  ## Statement-cache capacity; see `stmtCacheCapacity=`.
  conn.stmtCacheCapacity

proc `stmtCacheCapacity=`*(conn: PgConnection, value: int) {.inline.} =
  ## Resize the client-side prepared-statement cache (256 default; 0 disables
  ## it). Shrinking below the current size leaves the excess to the next
  ## operation's eviction pass, which bundles the server-side ``Close``.
  conn.stmtCacheCapacity = value

func state*(conn: PgConnection): PgConnState {.inline.} =
  ## Current state (read-only; see `isConnected` / `closedReason`).
  conn.state

func txStatus*(conn: PgConnection): TransactionStatus {.inline.} =
  ## Tx status from last ``ReadyForQuery`` (read-only, via `bindSym`).
  conn.txStatus

proc sendBuf*(conn: PgConnection): var seq[byte] {.inline.} =
  ## Send buffer for `queryDirect` / `execDirect` (via `bindSym`).
  conn.sendBuf

proc nextPortalName*(conn: PgConnection, prefix: string): string =
  ## Fresh portal/savepoint name; owns counter so macro scope stays sealed.
  inc conn.portalCounter
  prefix & $conn.portalCounter

func closedReason*(conn: PgConnection): PgClosedReason {.inline.} =
  ## Why unusable (``crClosedByUser`` outranks ``crClosed``).
  if conn.closedByUser:
    crClosedByUser
  elif conn.state == csClosed:
    crClosed
  else:
    crOpen

proc checkNotClosed*(conn: PgConnection) {.inline.} =
  ## Reject if closed: ``PgStateError`` for deliberate ``close()``, else ``PgConnectionError``.
  case conn.closedReason
  of crOpen:
    discard
  of crClosedByUser:
    raise newException(PgStateError, closedByUserMsg)
  of crClosed:
    raise newException(PgConnectionError, "Connection is closed")

proc raiseClosedConnection*(conn: PgConnection, msg: string) {.noreturn.} =
  ## Like ``checkNotClosed`` with a custom ``crClosed`` message.
  # On ``crClosedByUser`` `msg` moves to `parent`: `closedByUserMsg` is public
  # and matched exactly, so it cannot carry a custom message.
  if conn.closedReason == crClosedByUser:
    raise (ref PgStateError)(
      msg: closedByUserMsg, parent: newException(PgConnectionError, msg)
    )
  raise newException(PgConnectionError, msg)

proc raiseTransportFailure*(
    conn: PgConnection, what: string, e: ref CatchableError
) {.noreturn.} =
  ## Fold backend transport error into ``PgError`` (``closedByUser`` wins).
  if conn.closedReason == crClosedByUser:
    raise (ref PgStateError)(msg: closedByUserMsg, parent: e)
  if e of PgError:
    raise e
  raise newException(PgConnectionError, what & ": " & e.msg, e)

proc failNotifyWaiter*(conn: PgConnection, err: ref PgError = nil) {.raises: [].} =
  ## Fail parked waiter: ``closedByUser``→``PgStateError``, else ``err``/``csClosed``/stopped. Pass fresh ``err``.
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    let e: ref PgError =
      if conn.closedByUser:
        # Ahead of ``err``, as ``checkNotClosed`` does: a pump death racing
        # ``close()`` passes a ``PgListenError`` that would revive reconnect loops.
        (ref PgStateError)(msg: closedByUserMsg)
      elif err != nil:
        err
      elif conn.state == csClosed:
        (ref PgConnectionError)(msg: "Connection is closed")
      else:
        (ref PgStateError)(msg: "Listener stopped")
    # asyncdispatch types `Future.fail`'s callback chain as raising `Exception`,
    # so catching it is what keeps this proc `raises: []`; nothing real is masked.
    try:
      conn.notifyWaiter.fail(e)
    except Exception:
      discard
