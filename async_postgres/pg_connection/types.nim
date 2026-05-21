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
    sslDisable ## Disable SSL (default)
    sslAllow ## Try plaintext; fall back to SSL if refused
    sslPrefer ## Try SSL; fall back to plaintext if refused
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

  HostEntry* = object ## A single host:port entry for multi-host connection.
    host*: string
    port*: int

  ConnConfig* = object
    ## Connection configuration. Construct via `parseDsn` or set fields directly.
    host*: string
    port*: int # default 5432
    user*: string
    password*: string
    database*: string
    sslMode*: SslMode
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
    extraParams*: seq[(string, string)] ## Additional startup parameters
    maxMessageSize*: int
      ## Upper bound (in bytes) on a single backend message including
      ## its 1-byte type and 4-byte length header. A server claiming a
      ## larger message is rejected with `ProtocolError` before any
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

  CachedStmt* = object ## A cached prepared statement in the LRU statement cache.
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
    host*: string
    port*: int
    createdAt*: Moment
    portalCounter*: int
    config*: ConnConfig
    notifyQueue*: Deque[Notification]
    notifyMaxQueue*: int
    notifyWaiter*: Future[void]
    sendBuf*: seq[byte] ## Reusable send buffer for COPY IN batching
    notifyDropped*: int ## Count of notifications dropped due to queue overflow
    listenErrorMsg*: string ## Set when listen pump fails permanently
    listenReconnectMaxAttempts*: int
      ## Max reconnect attempts on listen pump failure. Default 10.
      ## 0 or negative = unlimited retries (retry until close()).
    listenReconnectMaxBackoff*: int
      ## Max seconds between reconnect attempts (backoff cap). Default 30.
    reconnectCallback*: proc() {.gcsafe, raises: [].}
    notifyOverflowCallback*: proc(dropped: int) {.gcsafe, raises: [].}
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
    hstoreOid*: int32 ## Dynamic OID for hstore extension type; 0 if not available
    hstoreArrayOid*: int32 ## Dynamic OID for hstore[] array; 0 if not available
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

  TraceLeakedSessionLocksData* = object
    ## Advisory notification that a pool connection returned while still
    ## holding session-level advisory locks acquired through the typed API.
    ## The pool handles cleanup itself — either running
    ## ``pg_advisory_unlock_all`` from ``resetSession`` and reusing the
    ## connection, or discarding it on ``release`` when ``resetSession`` was
    ## bypassed. Use this hook to detect missing ``advisoryUnlock`` /
    ## ``advisoryUnlockAll`` calls at the borrow site, since silent cleanup
    ## would otherwise mask the leak.
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
    onInsecureAuth*: proc(data: TraceInsecureAuthData) {.gcsafe, raises: [].}
      ## Fires when an auth method is used over an insecure transport
      ## (currently: cleartext password without SSL). Advisory only; does
      ## not abort the connection. Use `ConnConfig.requireAuth` to enforce.
    onDeprecatedAuth*: proc(data: TraceDeprecatedAuthData) {.gcsafe, raises: [].}
      ## Fires when a server-requested auth method is cryptographically
      ## weak / deprecated regardless of transport (currently: MD5).
      ## Advisory only; does not abort the connection. Use
      ## `ConnConfig.requireAuth` to enforce.

when hasChronos:
  type RowCallback* = proc(row: Row) {.raises: [CatchableError], gcsafe.}
    ## Callback invoked once per row during `queryEach`. The `Row` is only valid
    ## inside the callback — its backing buffer is reused for the next row.

else:
  type RowCallback* = proc(row: Row) {.gcsafe.}
    ## Callback invoked once per row during `queryEach`. The `Row` is only valid
    ## inside the callback — its backing buffer is reused for the next row.

when hasChronos:
  type CopyOutCallback* =
    proc(data: seq[byte]): Future[void] {.async: (raises: [CatchableError]), gcsafe.}
    ## Callback receiving each chunk during streaming COPY OUT.

  type CopyInCallback* =
    proc(): Future[seq[byte]] {.async: (raises: [CatchableError]), gcsafe.}
    ## Callback supplying data chunks during streaming COPY IN. Return empty seq to finish.

else:
  type CopyOutCallback* = proc(data: seq[byte]): Future[void] {.gcsafe.}
    ## Callback receiving each chunk during streaming COPY OUT.

  type CopyInCallback* = proc(): Future[seq[byte]] {.gcsafe.}
    ## Callback supplying data chunks during streaming COPY IN. Return empty seq to finish.

const RecvBufSize* = 131072 ## Size of the temporary read buffer for recv operations

# Convenience overloads for hstore parameter encoding using a connection's
# discovered OIDs (so callers don't have to plumb them through manually).

proc toPgBinaryParam*(conn: PgConnection, v: PgHstore): PgParam {.inline.} =
  ## Convenience overload: encode hstore in binary using ``conn.hstoreOid``.
  ## Raises ``PgTypeError`` if the hstore extension OID has not been discovered
  ## (e.g. extension not installed on the server).
  if conn.hstoreOid == 0:
    raise newException(PgTypeError, "hstore OID not available on this connection")
  toPgBinaryParam(v, conn.hstoreOid)

proc toPgBinaryParam*(conn: PgConnection, v: seq[PgHstore]): PgParam {.inline.} =
  ## Convenience overload: encode ``hstore[]`` in binary using
  ## ``conn.hstoreOid`` and ``conn.hstoreArrayOid``. Raises ``PgTypeError`` if
  ## either OID has not been discovered.
  if conn.hstoreOid == 0 or conn.hstoreArrayOid == 0:
    raise
      newException(PgTypeError, "hstore/hstore[] OIDs not available on this connection")
  toPgBinaryParam(v, conn.hstoreOid, conn.hstoreArrayOid)

# Internal accessor for cross-module use within the library

func effectiveMaxMessageSize*(conn: PgConnection): int {.inline.} =
  ## Effective per-message recv cap for this connection. Resolves the
  ## ``ConnConfig.maxMessageSize`` default (0) to ``DefaultMaxBackendMessageLen``.
  if conn.config.maxMessageSize > 0:
    conn.config.maxMessageSize
  else:
    DefaultMaxBackendMessageLen

# Error builder

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
