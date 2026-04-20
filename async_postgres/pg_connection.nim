import std/[tables, sets, strutils, uri, deques, options, lists]
when defined(posix):
  import std/posix

import async_backend, pg_protocol, pg_auth, pg_types

when hasChronos:
  import chronos/streams/tlsstream
  import pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet
  from std/nativesockets import Domain, SockType, Protocol
  when defined(ssl):
    import std/[net, openssl, tempfiles, os]

export PgError

# TCP keepalive socket options (not exported by posix module)
when defined(linux):
  var
    TCP_KEEPIDLE {.importc, header: "<netinet/tcp.h>".}: cint
    TCP_KEEPINTVL {.importc, header: "<netinet/tcp.h>".}: cint
    TCP_KEEPCNT {.importc, header: "<netinet/tcp.h>".}: cint
elif defined(macosx):
  var TCP_KEEPALIVE {.importc, header: "<netinet/tcp.h>".}: cint
  const
    TCP_KEEPINTVL = cint(0x101)
    TCP_KEEPCNT = cint(0x102)
else:
  {.
    warning:
      "TCP keepalive timing options (idle/interval/count) are not supported on this platform and will be ignored"
  .}

type
  PgConnectionError* = object of PgError
    ## Connection failures, disconnections, SSL/auth errors.

  PgQueryError* = object of PgError
    ## SQL execution errors from the server (ErrorResponse).
    sqlState*: string ## 5-char SQLSTATE code (e.g. "42P01"), empty if unavailable.
    severity*: string ## e.g. "ERROR", "FATAL"
    detail*: string ## DETAIL field, empty if not present.
    hint*: string ## HINT field, empty if not present.

  PgTimeoutError* = object of PgError ## Operation timed out.

  PgPoolError* = object of PgError ## Pool exhaustion, pool closed, or acquire timeout.

  PgNotifyOverflowError* = object of PgError
    dropped*: int ## Number of notifications dropped due to queue overflow

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
    applicationName*: string
    connectTimeout*: Duration ## TCP connect timeout (default: no timeout)
    keepAlive*: bool ## Enable TCP keepalive (default true via parseDsn)
    keepAliveIdle*: int ## Seconds before first probe (0 = OS default)
    keepAliveInterval*: int ## Seconds between probes (0 = OS default)
    keepAliveCount*: int ## Number of probes before giving up (0 = OS default)
    hosts*: seq[HostEntry] ## Multiple hosts for failover (empty = use host/port)
    targetSessionAttrs*: TargetSessionAttrs ## Target server type (default tsaAny)
    extraParams*: seq[(string, string)] ## Additional startup parameters
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
    resultFormats*: seq[int16] ## Cached buildResultFormats() output
    colFmts*: seq[int16] ## Per-column format codes for RowData
    colOids*: seq[int32] ## Per-column type OIDs for RowData
    lruNode*: DoublyLinkedNode[string] ## Embedded LRU list node

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
    pid: int32
    secretKey: int32
    serverParams: Table[string, string]
    txStatus: TransactionStatus
    notifyCallback: NotifyCallback
    noticeCallback: NoticeCallback
    listenChannels: HashSet[string]
    listenTask: Future[void]
    host: string
    port: int
    createdAt: Moment
    portalCounter: int
    config: ConnConfig
    notifyQueue: Deque[Notification]
    notifyMaxQueue: int
    notifyWaiter: Future[void]
    sendBuf: seq[byte] ## Reusable send buffer for COPY IN batching
    notifyDropped: int ## Count of notifications dropped due to queue overflow
    listenErrorMsg: string ## Set when listen pump fails permanently
    reconnectCallback: proc() {.gcsafe, raises: [].}
    notifyOverflowCallback: proc(dropped: int) {.gcsafe, raises: [].}
    stmtCache: Table[string, CachedStmt]
    stmtCacheLru: DoublyLinkedList[string] ## LRU order: oldest at head, newest at tail
    stmtCounter: int
    stmtCacheCapacity: int ## 0=disabled, default 256
    hstoreOid: int32 ## Dynamic OID for hstore extension type; 0 if not available
    hstoreArrayOid: int32 ## Dynamic OID for hstore[] array; 0 if not available
    tracer: PgTracer ## Inherited from ConnConfig on connect

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

# Public API: read-only getters

func pid*(conn: PgConnection): int32 {.inline.} =
  ## The backend process ID for this connection.
  conn.pid

func sslEnabled*(conn: PgConnection): bool {.inline.} =
  ## Whether SSL/TLS is active on this connection.
  conn.sslEnabled

func serverParams*(conn: PgConnection): lent Table[string, string] {.inline.} =
  ## Server parameters reported during startup (e.g. server_version).
  conn.serverParams

func listenChannels*(conn: PgConnection): lent HashSet[string] {.inline.} =
  ## The set of channels this connection is currently LISTENing on.
  conn.listenChannels

func notifyDropped*(conn: PgConnection): int {.inline.} =
  ## Count of notifications dropped due to queue overflow.
  conn.notifyDropped

func config*(conn: PgConnection): lent ConnConfig {.inline.} =
  ## The connection configuration used to establish this connection.
  conn.config

func secretKey*(conn: PgConnection): int32 {.inline.} =
  ## The backend secret key (used for cancel requests).
  conn.secretKey

func hstoreOid*(conn: PgConnection): int32 {.inline.} =
  ## Dynamic OID for hstore extension type; 0 if not available.
  conn.hstoreOid

func hstoreArrayOid*(conn: PgConnection): int32 {.inline.} =
  ## Dynamic OID for hstore[] array type; 0 if not available.
  conn.hstoreArrayOid

proc toPgBinaryParam*(conn: PgConnection, v: PgHstore): PgParam {.inline.} =
  ## Convenience overload: encode hstore in binary using ``conn.hstoreOid``.
  ## Raises ``PgTypeError`` if the hstore extension OID has not been discovered
  ## (e.g. extension not installed on the server).
  if conn.hstoreOid == 0:
    raise newException(PgTypeError, "hstore OID not available on this connection")
  toPgBinaryParam(v, conn.hstoreOid)

proc toPgBinaryParam*(conn: PgConnection, v: seq[PgHstore]): PgParam {.inline.} =
  ## Convenience overload: encode hstore[] in binary using ``conn.hstoreOid``
  ## and ``conn.hstoreArrayOid``. Raises ``PgTypeError`` if either OID has not
  ## been discovered.
  if conn.hstoreOid == 0 or conn.hstoreArrayOid == 0:
    raise
      newException(PgTypeError, "hstore/hstore[] OIDs not available on this connection")
  toPgBinaryParam(v, conn.hstoreOid, conn.hstoreArrayOid)

func notifyCallback*(conn: PgConnection): NotifyCallback {.inline.} =
  ## The callback invoked when a NOTIFY message arrives.
  conn.notifyCallback

func notifyMaxQueue*(conn: PgConnection): int {.inline.} =
  ## The maximum notification queue size (0 = unlimited).
  conn.notifyMaxQueue

# Public API: read-write accessors

func state*(conn: PgConnection): var PgConnState {.inline.} =
  ## The current connection state.
  conn.state

proc `state=`*(conn: PgConnection, val: PgConnState) {.inline.} =
  conn.state = val

func txStatus*(conn: PgConnection): var TransactionStatus {.inline.} =
  ## The current transaction status.
  conn.txStatus

proc `txStatus=`*(conn: PgConnection, val: TransactionStatus) {.inline.} =
  conn.txStatus = val

func stmtCacheCapacity*(conn: PgConnection): var int {.inline.} =
  ## Statement cache capacity (0 = disabled, default 256).
  conn.stmtCacheCapacity

proc `stmtCacheCapacity=`*(conn: PgConnection, val: int) {.inline.} =
  conn.stmtCacheCapacity = val

func createdAt*(conn: PgConnection): var Moment {.inline.} =
  ## When this connection was established.
  conn.createdAt

proc `createdAt=`*(conn: PgConnection, val: Moment) {.inline.} =
  conn.createdAt = val

func tracer*(conn: PgConnection): var PgTracer {.inline.} =
  ## The tracing context for this connection.
  conn.tracer

proc `tracer=`*(conn: PgConnection, val: PgTracer) {.inline.} =
  conn.tracer = val

func notifyQueue*(conn: PgConnection): var Deque[Notification] {.inline.} =
  ## The notification queue for buffered notifications.
  conn.notifyQueue

func listenTask*(conn: PgConnection): var Future[void] {.inline.} =
  ## The background listen pump task.
  conn.listenTask

proc `listenTask=`*(conn: PgConnection, val: Future[void]) {.inline.} =
  conn.listenTask = val

# Public API: setters for callback/config fields

proc `noticeCallback=`*(conn: PgConnection, cb: NoticeCallback) {.inline.} =
  ## Set the callback invoked when a notice/warning message arrives.
  conn.noticeCallback = cb

proc `notifyMaxQueue=`*(conn: PgConnection, val: int) {.inline.} =
  ## Set the maximum notification queue size (0 = unlimited).
  conn.notifyMaxQueue = val

proc `notifyOverflowCallback=`*(
    conn: PgConnection, cb: proc(dropped: int) {.gcsafe, raises: [].}
) {.inline.} =
  ## Set the callback invoked when notifications are dropped due to queue overflow.
  conn.notifyOverflowCallback = cb

proc `reconnectCallback=`*(
    conn: PgConnection, cb: proc() {.gcsafe, raises: [].}
) {.inline.} =
  ## Set the callback invoked after a successful automatic reconnect.
  conn.reconnectCallback = cb

# Internal accessors for cross-module use within the library

func recvBuf*(conn: PgConnection): var seq[byte] {.inline.} =
  conn.recvBuf
proc `recvBuf=`*(conn: PgConnection, val: seq[byte]) {.inline.} =
  conn.recvBuf = val

func recvBufStart*(conn: PgConnection): var int {.inline.} =
  conn.recvBufStart
proc `recvBufStart=`*(conn: PgConnection, val: int) {.inline.} =
  conn.recvBufStart = val

func sendBuf*(conn: PgConnection): var seq[byte] {.inline.} =
  conn.sendBuf
func stmtCache*(conn: PgConnection): var Table[string, CachedStmt] {.inline.} =
  conn.stmtCache
func portalCounter*(conn: PgConnection): var int {.inline.} =
  conn.portalCounter

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

proc isUnixSocket*(host: string): bool {.inline.} =
  ## True if `host` represents a Unix socket directory (starts with '/').
  ## Compatible with libpq behavior.
  host.len > 0 and host[0] == '/'

proc unixSocketPath*(host: string, port: int): string =
  ## Build the libpq-compatible Unix socket file path: ``{dir}/.s.PGSQL.{port}``.
  host & "/.s.PGSQL." & $port

proc getHosts*(config: ConnConfig): seq[HostEntry] =
  ## Return the list of hosts to try. If `hosts` is populated, return it;
  ## otherwise synthesize a single entry from `host`/`port`.
  if config.hosts.len > 0:
    config.hosts
  else:
    @[HostEntry(host: config.host, port: if config.port == 0: 5432 else: config.port)]

proc len*(qr: QueryResult): int {.inline.} =
  ## Return the number of rows in the query result.
  int(qr.rowCount)

proc columnIndex*(qr: QueryResult, name: string): int =
  ## Find the index of a column by name in a query result.
  qr.fields.columnIndex(name)

proc rows*(qr: QueryResult): seq[Row] =
  ## Return all rows as lightweight Row views into the flat buffer.
  if qr.data == nil:
    return @[]
  if qr.fields.len > 0 and qr.data.fields.len == 0:
    qr.data.fields = qr.fields
  result = newSeq[Row](qr.rowCount)
  for i in 0 ..< qr.rowCount:
    result[i] = initRow(qr.data, i)

iterator items*(qr: QueryResult): Row =
  ## Iterate over all rows in the query result.
  if qr.data != nil:
    if qr.fields.len > 0 and qr.data.fields.len == 0:
      qr.data.fields = qr.fields
    for i in 0 ..< qr.rowCount:
      yield initRow(qr.data, i)

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

template makeCopyOutCallback*(body: untyped): CopyOutCallback =
  ## Create a ``CopyOutCallback`` that works with both asyncdispatch and chronos.
  ## Inside ``body``, the current chunk is available as ``data: seq[byte]``.
  ##
  ## .. code-block:: nim
  ##   var chunks: seq[seq[byte]]
  ##   let cb = makeCopyOutCallback:
  ##     chunks.add(data)
  block:
    when hasChronos:
      let r: CopyOutCallback = proc(
          data {.inject.}: seq[byte]
      ) {.async: (raises: [CatchableError]).} =
        body
      r
    else:
      let r: CopyOutCallback = proc(data {.inject.}: seq[byte]) {.async.} =
        body
      r

template makeCopyInCallback*(body: untyped): CopyInCallback =
  ## Create a ``CopyInCallback`` that works with both asyncdispatch and chronos.
  ## ``body`` must evaluate to ``seq[byte]``. Return an empty seq to signal completion.
  ##
  ## With asyncdispatch, anonymous async procs cannot return non-void types,
  ## so this template wraps the body in manual ``Future`` construction.
  ##
  ## .. code-block:: nim
  ##   var idx = 0
  ##   let rows = @["1\tAlice\n".toBytes(), "2\tBob\n".toBytes()]
  ##   let cb = makeCopyInCallback:
  ##     if idx < rows.len:
  ##       let chunk = rows[idx]
  ##       inc idx
  ##       chunk
  ##     else:
  ##       newSeq[byte]()
  block:
    when hasChronos:
      let r: CopyInCallback = proc(): Future[seq[byte]] {.
          async: (raises: [CatchableError])
      .} =
        body
      r
    else:
      # asyncdispatch's {.async.} doesn't support non-void return types on
      # anonymous procs. Wrap in manual Future construction instead.
      # Note: body must be synchronous (no await).
      let r: CopyInCallback = proc(): Future[seq[byte]] {.gcsafe.} =
        let fut = newFuture[seq[byte]]("makeCopyInCallback")
        try:
          let res: seq[byte] = body
          fut.complete(res)
        except CatchableError as e:
          fut.fail(e)
        return fut
      r

when hasChronos:
  type RowCallback* = proc(row: Row) {.raises: [CatchableError], gcsafe.}
    ## Callback invoked once per row during `queryEach`. The `Row` is only valid
    ## inside the callback — its backing buffer is reused for the next row.

else:
  type RowCallback* = proc(row: Row) {.gcsafe.}
    ## Callback invoked once per row during `queryEach`. The `Row` is only valid
    ## inside the callback — its backing buffer is reused for the next row.

const RecvBufSize = 131072 ## Size of the temporary read buffer for recv operations

proc dispatchNotification*(conn: PgConnection, msg: BackendMessage) =
  let notif = Notification(
    pid: msg.notifPid, channel: msg.notifChannel, payload: msg.notifPayload
  )
  if conn.notifyMaxQueue > 0:
    var droppedNow = 0
    while conn.notifyQueue.len >= conn.notifyMaxQueue:
      discard conn.notifyQueue.popFirst()
      if conn.notifyDropped < high(int):
        conn.notifyDropped.inc
      droppedNow.inc
    conn.notifyQueue.addLast(notif)
    if droppedNow > 0 and conn.notifyOverflowCallback != nil:
      conn.notifyOverflowCallback(droppedNow)
    if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
      conn.notifyWaiter.complete()
  if conn.notifyCallback != nil:
    conn.notifyCallback(notif)

proc dispatchNotice*(conn: PgConnection, msg: BackendMessage) =
  if conn.noticeCallback != nil:
    conn.noticeCallback(Notice(fields: msg.noticeFields))

proc nextStmtName*(conn: PgConnection): string =
  ## Generate the next unique prepared statement name for the statement cache.
  inc conn.stmtCounter
  "_sc_" & $conn.stmtCounter

proc clearStmtCache*(conn: PgConnection) =
  ## Clear the client-side statement cache. Does not close server-side statements.
  conn.stmtCache.clear()
  conn.stmtCacheLru = initDoublyLinkedList[string]()

proc lookupStmtCache*(conn: PgConnection, sql: string): ptr CachedStmt =
  ## Look up a cached prepared statement by SQL text, updating LRU order on hit.
  ## Returns nil on miss. The returned pointer is valid until the next cache mutation.
  if conn.stmtCacheCapacity <= 0:
    return nil
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
    conn.stmtCacheLru.append(entry.lruNode)
    return addr entry[]
  return nil

proc evictStmtCache*(conn: PgConnection): CachedStmt =
  ## Evict the least recently used entry from the cache. Returns the evicted entry.
  let node = conn.stmtCacheLru.head
  let oldSql = node.value
  conn.stmtCacheLru.remove(node)
  result = conn.stmtCache[oldSql]
  conn.stmtCache.del(oldSql)

proc addStmtCache*(conn: PgConnection, sql: string, cached: CachedStmt) =
  ## Add a prepared statement to the cache with auto-computed result formats.
  if conn.stmtCacheCapacity <= 0:
    return
  if conn.stmtCache.len >= conn.stmtCacheCapacity:
    return # caller should have evicted; skip if still full
  var entry = cached
  if entry.resultFormats.len == 0 and entry.fields.len > 0:
    var extraOids: seq[int32]
    if conn.hstoreOid != 0:
      extraOids.add(conn.hstoreOid)
    if conn.hstoreArrayOid != 0:
      extraOids.add(conn.hstoreArrayOid)
    entry.resultFormats = buildResultFormats(entry.fields, extraOids)
    entry.colFmts = newSeq[int16](entry.fields.len)
    entry.colOids = newSeq[int32](entry.fields.len)
    for i in 0 ..< entry.fields.len:
      entry.colOids[i] = entry.fields[i].typeOid
      entry.colFmts[i] = entry.resultFormats[i]
  let node = newDoublyLinkedNode(sql)
  entry.lruNode = node
  conn.stmtCache[sql] = entry
  conn.stmtCacheLru.append(node)

proc removeStmtCache*(conn: PgConnection, sql: string) =
  ## Remove a statement from the cache by its SQL text.
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
  conn.stmtCache.del(sql)

when hasAsyncDispatch:
  proc sendRawData(socket: AsyncSocket, p: pointer, len: int): Future[void] =
    ## Send raw bytes via asyncdispatch socket. Copies data into a string once.
    if len == 0:
      var fut = newFuture[void]("sendRawData")
      fut.complete()
      return fut
    var s = newString(len)
    copyMem(addr s[0], p, len)
    socket.send(move s)

  proc sendRawBytes(socket: AsyncSocket, data: seq[byte]): Future[void] =
    ## Send ``seq[byte]`` via asyncdispatch socket.
    if data.len == 0:
      var fut = newFuture[void]("sendRawBytes")
      fut.complete()
      return fut
    sendRawData(socket, unsafeAddr data[0], data.len)

proc compactRecvBuf(conn: PgConnection) {.inline.} =
  ## Shift unconsumed data to the front of recvBuf, reclaiming space consumed
  ## by the read pointer.  Called only before reading new data from the socket.
  let start = conn.recvBufStart
  if start == 0:
    return
  let remaining = conn.recvBuf.len - start
  if remaining == 0:
    conn.recvBuf.setLen(0)
  else:
    moveMem(addr conn.recvBuf[0], addr conn.recvBuf[start], remaining)
    conn.recvBuf.setLen(remaining)
  conn.recvBufStart = 0

proc fillRecvBuf*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Read data from socket into buffer. The only await point for message reception.
  conn.compactRecvBuf()
  when hasChronos:
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + RecvBufSize)
    try:
      let n =
        if timeout == ZeroDuration:
          await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize)
        else:
          await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize).wait(
            timeout
          )
      if n == 0:
        conn.state = csClosed
        raise newException(PgConnectionError, "Connection closed by server")
      conn.recvBuf.setLen(oldLen + n)
    except AsyncTimeoutError as e:
      conn.recvBuf.setLen(oldLen)
      raise e
  elif hasAsyncDispatch:
    let data =
      if timeout == ZeroDuration:
        await conn.socket.recv(RecvBufSize)
      else:
        await conn.socket.recv(RecvBufSize).wait(timeout)
    if data.len == 0:
      conn.state = csClosed
      raise newException(PgConnectionError, "Connection closed by server")
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + data.len)
    copyMem(addr conn.recvBuf[oldLen], unsafeAddr data[0], data.len)

proc nextMessage*(
    conn: PgConnection, rowData: RowData = nil, rowCount: ptr int32 = nil
): Option[BackendMessage] =
  ## Synchronously parse the next message from the receive buffer.
  ## Returns none if the buffer doesn't contain a complete message.
  ## Notification/Notice messages are dispatched internally.
  ## DataRow messages are counted (if rowCount != nil) and consumed.
  var pos = conn.recvBufStart
  while true:
    var consumed: int
    let res = parseBackendMessage(
      conn.recvBuf.toOpenArray(pos, conn.recvBuf.len - 1), consumed, rowData
    )
    if res.state == psIncomplete:
      return none(BackendMessage)
    pos += consumed
    conn.recvBufStart = pos
    if res.state == psDataRow:
      # DataRow already parsed in-place into rowData; just count it
      if rowCount != nil:
        rowCount[] += 1
      continue
    if res.message.kind == bmkNotificationResponse:
      conn.dispatchNotification(res.message)
      continue
    if res.message.kind == bmkNoticeResponse:
      conn.dispatchNotice(res.message)
      continue
    if res.message.kind == bmkDataRow and rowCount != nil:
      rowCount[] += 1
      continue
    return some(res.message)

proc recvMessage*(
    conn: PgConnection,
    timeout = ZeroDuration,
    rowData: RowData = nil,
    rowCount: ptr int32 = nil,
): Future[BackendMessage] {.async.} =
  ## Receive a single backend message from the connection.
  ## Thin wrapper around nextMessage + fillRecvBuf for backward compatibility.
  while true:
    let opt = conn.nextMessage(rowData, rowCount)
    if opt.isSome:
      return opt.get
    await conn.fillRecvBuf(timeout)

proc sendMsg*(conn: PgConnection, data: seq[byte]): Future[void] {.async.} =
  ## Send raw bytes to the PostgreSQL server over the connection.
  when hasChronos:
    await conn.writer.write(data)
  elif hasAsyncDispatch:
    if data.len > 0:
      await conn.socket.sendRawBytes(data)

proc sendBufMsg*(conn: PgConnection): Future[void] {.async.} =
  ## Send conn.sendBuf to the server without copying the seq.
  ## Safe because conn.state == csBusy prevents concurrent access to sendBuf.
  when hasChronos:
    if conn.sendBuf.len > 0:
      await conn.writer.write(unsafeAddr conn.sendBuf[0], conn.sendBuf.len)
  elif hasAsyncDispatch:
    if conn.sendBuf.len > 0:
      await conn.socket.sendRawData(unsafeAddr conn.sendBuf[0], conn.sendBuf.len)

proc closeTransport(conn: PgConnection) {.async.} =
  ## Close transport resources without sending Terminate.
  when hasChronos:
    if conn.tlsStream != nil:
      try:
        await conn.tlsStream.reader.closeWait()
      except CatchableError:
        discard
      try:
        await conn.tlsStream.writer.closeWait()
      except CatchableError:
        discard
      conn.tlsStream = nil
    if conn.baseReader != nil:
      try:
        await conn.baseReader.closeWait()
      except CatchableError:
        discard
      try:
        await conn.baseWriter.closeWait()
      except CatchableError:
        discard
      conn.baseReader = nil
      conn.baseWriter = nil
    if conn.transport != nil:
      try:
        await conn.transport.closeWait()
      except CatchableError:
        discard
      conn.transport = nil
  elif hasAsyncDispatch:
    if not conn.socket.isNil:
      conn.socket.close()
      conn.socket = nil

proc negotiateSSL(conn: PgConnection, config: ConnConfig) {.async.} =
  ## Send SSLRequest and negotiate TLS if server accepts.
  let sslReq = encodeSSLRequest()
  var respChar: char

  when hasChronos:
    discard await conn.transport.write(sslReq)
    var response: array[1, byte]
    let n = await conn.transport.readOnce(addr response[0], 1)
    if n == 0:
      raise newException(PgConnectionError, "Connection closed during SSL negotiation")
    respChar = char(response[0])
  elif hasAsyncDispatch:
    await conn.socket.sendRawBytes(sslReq)
    let respStr = await conn.socket.recv(1)
    if respStr.len == 0:
      raise newException(PgConnectionError, "Connection closed during SSL negotiation")
    respChar = respStr[0]

  case respChar
  of 'S':
    when hasChronos:
      conn.baseReader = newAsyncStreamReader(conn.transport)
      conn.baseWriter = newAsyncStreamWriter(conn.transport)

      let flags =
        case config.sslMode
        of sslVerifyFull:
          {}
        of sslVerifyCa:
          {TLSFlags.NoVerifyServerName}
        else:
          {TLSFlags.NoVerifyHost, TLSFlags.NoVerifyServerName}

      let serverName = if config.sslMode == sslVerifyFull: config.host else: ""

      if config.sslRootCert.len > 0:
        let parsed = parseTrustAnchors(config.sslRootCert)
        conn.trustAnchorBufs = parsed.backing
          # Must outlive TLS session (see parseTrustAnchors doc)
        conn.tlsStream = newTLSClientAsyncStream(
          conn.baseReader,
          conn.baseWriter,
          serverName,
          flags = flags,
          minVersion = TLSVersion.TLS12,
          maxVersion = TLSVersion.TLS12,
          trustAnchors = parsed.store,
        )
      else:
        conn.tlsStream = newTLSClientAsyncStream(
          conn.baseReader,
          conn.baseWriter,
          serverName,
          flags = flags,
          minVersion = TLSVersion.TLS12,
          maxVersion = TLSVersion.TLS12,
        )
      installX509Capture(
        conn.x509Capture, conn.tlsStream.ccontext.eng, addr conn.serverCertDer
      )
      await conn.tlsStream.handshake()
      conn.reader = conn.tlsStream.reader
      conn.writer = conn.tlsStream.writer
      conn.sslEnabled = true
    elif hasAsyncDispatch:
      when defined(ssl):
        let verifyMode =
          case config.sslMode
          of sslVerifyCa, sslVerifyFull: SslCVerifyMode.CVerifyPeer
          else: SslCVerifyMode.CVerifyNone

        var ctx: SslContext
        var tmpPath: string
        if config.sslRootCert.len > 0:
          let (tmpFile, tp) = createTempFile("pg_ca_", ".pem")
          tmpPath = tp
          try:
            tmpFile.write(config.sslRootCert)
            tmpFile.close()
            ctx = newContext(verifyMode = verifyMode, caFile = tmpPath)
          except:
            removeFile(tmpPath)
            raise
        else:
          ctx = newContext(verifyMode = verifyMode)

        try:
          let hostname = if config.sslMode == sslVerifyFull: config.host else: ""
          wrapConnectedSocket(ctx, conn.socket, handshakeAsClient, hostname)
          conn.sslEnabled = true
          # Extract server certificate DER for SCRAM-SHA-256-PLUS channel binding
          let peerCert = SSL_get_peer_certificate(conn.socket.sslHandle)
          if peerCert != nil:
            try:
              let derStr = i2d_X509(peerCert)
              if derStr.len > 0:
                conn.serverCertDer = newSeq[byte](derStr.len)
                for i in 0 ..< derStr.len:
                  conn.serverCertDer[i] = byte(derStr[i])
            finally:
              X509_free(peerCert)
        finally:
          if tmpPath.len > 0:
            removeFile(tmpPath)
      else:
        raise
          newException(PgConnectionError, "SSL support requires compiling with -d:ssl")
  of 'N':
    if config.sslMode in {sslRequire, sslVerifyCa, sslVerifyFull}:
      raise newException(PgConnectionError, "Server does not support SSL")
    # sslPrefer: server refused SSL – connection will proceed unencrypted.
    # WARNING: This is vulnerable to MITM downgrade attacks. A network
    # attacker can intercept the SSLRequest and reply 'N' to force
    # plaintext. Use sslRequire or stronger if security is needed.
    stderr.writeLine "pg_connection: SSL refused by server, falling back to plaintext (sslmode=prefer)"
  else:
    raise newException(PgConnectionError, "Unexpected SSL response: " & $respChar)

when defined(posix):
  var TCP_NODELAY {.importc, header: "<netinet/tcp.h>".}: cint

  proc configureTcpNoDelay(fd: posix.SocketHandle) =
    ## Disable Nagle's algorithm for low-latency sends.
    var optval: cint = 1
    discard setsockopt(
      fd, cint(posix.IPPROTO_TCP), TCP_NODELAY, addr optval, sizeof(optval).SockLen
    )

  proc configureKeepalive(fd: posix.SocketHandle, config: ConnConfig) =
    ## Set TCP keepalive options on the socket.
    if not config.keepAlive:
      return
    var optval: cint = 1
    if setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, addr optval, sizeof(optval).SockLen) < 0:
      raise newException(
        PgConnectionError, "Failed to set SO_KEEPALIVE: " & $strerror(errno)
      )
    when defined(linux):
      if config.keepAliveIdle > 0:
        optval = cint(config.keepAliveIdle)
        if setsockopt(
          fd, cint(posix.IPPROTO_TCP), TCP_KEEPIDLE, addr optval, sizeof(optval).SockLen
        ) < 0:
          raise newException(
            PgConnectionError, "Failed to set TCP_KEEPIDLE: " & $strerror(errno)
          )
      if config.keepAliveInterval > 0:
        optval = cint(config.keepAliveInterval)
        if setsockopt(
          fd,
          cint(posix.IPPROTO_TCP),
          TCP_KEEPINTVL,
          addr optval,
          sizeof(optval).SockLen,
        ) < 0:
          raise newException(
            PgConnectionError, "Failed to set TCP_KEEPINTVL: " & $strerror(errno)
          )
      if config.keepAliveCount > 0:
        optval = cint(config.keepAliveCount)
        if setsockopt(
          fd, cint(posix.IPPROTO_TCP), TCP_KEEPCNT, addr optval, sizeof(optval).SockLen
        ) < 0:
          raise newException(
            PgConnectionError, "Failed to set TCP_KEEPCNT: " & $strerror(errno)
          )
    elif defined(macosx):
      if config.keepAliveIdle > 0:
        optval = cint(config.keepAliveIdle)
        if setsockopt(
          fd,
          cint(posix.IPPROTO_TCP),
          TCP_KEEPALIVE,
          addr optval,
          sizeof(optval).SockLen,
        ) < 0:
          raise newException(
            PgConnectionError, "Failed to set TCP_KEEPALIVE: " & $strerror(errno)
          )
      if config.keepAliveInterval > 0:
        optval = cint(config.keepAliveInterval)
        if setsockopt(
          fd,
          cint(posix.IPPROTO_TCP),
          TCP_KEEPINTVL,
          addr optval,
          sizeof(optval).SockLen,
        ) < 0:
          raise newException(
            PgConnectionError, "Failed to set TCP_KEEPINTVL: " & $strerror(errno)
          )
      if config.keepAliveCount > 0:
        optval = cint(config.keepAliveCount)
        if setsockopt(
          fd, cint(posix.IPPROTO_TCP), TCP_KEEPCNT, addr optval, sizeof(optval).SockLen
        ) < 0:
          raise newException(
            PgConnectionError, "Failed to set TCP_KEEPCNT: " & $strerror(errno)
          )
    else:
      if config.keepAliveIdle > 0 or config.keepAliveInterval > 0 or
          config.keepAliveCount > 0:
        {.
          warning:
            "TCP keepalive timing options (idle/interval/count) are not supported on this platform and will be ignored"
        .}

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

proc selectScramMechanism(
    sslEnabled: bool,
    serverCertDer: openArray[byte],
    saslMechanisms: seq[string],
    mode: ChannelBindingMode,
): tuple[mechanism: string, cbType: string, cbData: seq[byte]] =
  ## Pick the SCRAM mechanism and channel-binding material for a SASL
  ## authentication attempt. Raises `PgConnectionError` when the server-offered
  ## mechanisms cannot satisfy `mode`.
  let serverHasPlus = "SCRAM-SHA-256-PLUS" in saslMechanisms
  let serverHasScram = "SCRAM-SHA-256" in saslMechanisms
  let canUsePlus = sslEnabled and serverCertDer.len > 0 and serverHasPlus
  case mode
  of cbRequire:
    if not sslEnabled:
      raise newException(
        PgConnectionError, "channel binding is required, but SSL is not in use"
      )
    if not serverHasPlus:
      raise newException(
        PgConnectionError,
        "channel binding is required, but server did not offer SCRAM-SHA-256-PLUS",
      )
    if serverCertDer.len == 0:
      raise newException(
        PgConnectionError,
        "channel binding is required, but server certificate is unavailable",
      )
    result.mechanism = "SCRAM-SHA-256-PLUS"
    result.cbType = "tls-server-end-point"
    result.cbData = computeTlsServerEndpoint(serverCertDer)
  of cbPrefer:
    if canUsePlus:
      result.mechanism = "SCRAM-SHA-256-PLUS"
      result.cbType = "tls-server-end-point"
      result.cbData = computeTlsServerEndpoint(serverCertDer)
    elif serverHasScram:
      result.mechanism = "SCRAM-SHA-256"
    else:
      raise newException(
        PgConnectionError, "server doesn't support SCRAM-SHA-256 or SCRAM-SHA-256-PLUS"
      )
  of cbDisable:
    if serverHasScram:
      result.mechanism = "SCRAM-SHA-256"
    else:
      raise newException(
        PgConnectionError,
        "channel binding is disabled, but server only offered SCRAM-SHA-256-PLUS",
      )

proc connectToHost(
    config: ConnConfig, hostAddr: string, hostPort: int
): Future[PgConnection] {.async.} =
  ## Connect to a single PostgreSQL host. Internal helper for multi-host connect.

  if config.sslMode == sslAllow:
    # sslAllow: try plaintext first, then fall back to SSL.
    var plainConfig = config
    plainConfig.sslMode = sslDisable
    try:
      return await connectToHost(plainConfig, hostAddr, hostPort)
    except CancelledError as e:
      raise e
    except CatchableError:
      # Plaintext failed — retry with SSL.
      # WARNING: This is vulnerable to MITM downgrade attacks. A network
      # attacker can force the first attempt to fail and then intercept
      # the SSL connection. Use sslRequire or stronger if security is needed.
      stderr.writeLine "pg_connection: plaintext connection failed, retrying with SSL (sslmode=allow)"
      var sslConfig = config
      sslConfig.sslMode = sslPrefer
      return await connectToHost(sslConfig, hostAddr, hostPort)

  var conn: PgConnection

  let isUnix = isUnixSocket(hostAddr)

  when hasChronos:
    let transport =
      if isUnix:
        when defined(posix):
          await connect(initTAddress(unixSocketPath(hostAddr, hostPort)))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        let addresses = resolveTAddress(hostAddr, Port(hostPort))
        if addresses.len == 0:
          raise newException(PgConnectionError, "Could not resolve host: " & hostAddr)
        await connect(addresses[0])
    when defined(posix):
      if not isUnix:
        try:
          configureTcpNoDelay(posix.SocketHandle(transport.fd))
          configureKeepalive(posix.SocketHandle(transport.fd), config)
        except CatchableError as e:
          try:
            await noCancel transport.closeWait()
          except CatchableError:
            discard
          raise newException(PgConnectionError, e.msg, e)
    conn = PgConnection(
      transport: transport,
      recvBuf: @[],
      state: csConnecting,
      serverParams: initTable[string, string](),
      host: hostAddr,
      port: hostPort,
      config: config,
      notifyMaxQueue: 1024,
      stmtCacheCapacity: 256,
    )
  elif hasAsyncDispatch:
    let sock =
      if isUnix:
        when defined(posix):
          newAsyncSocket(
            Domain.AF_UNIX, SockType.SOCK_STREAM, Protocol.IPPROTO_IP, buffered = false
          )
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        newAsyncSocket(buffered = false)
    try:
      if isUnix:
        when defined(posix):
          await sock.connectUnix(unixSocketPath(hostAddr, hostPort))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        await sock.connect(hostAddr, Port(hostPort))
        when defined(posix):
          configureTcpNoDelay(posix.SocketHandle(sock.getFd()))
          configureKeepalive(posix.SocketHandle(sock.getFd()), config)
    except CatchableError:
      sock.close()
      raise
    conn = PgConnection(
      socket: sock,
      recvBuf: @[],
      state: csConnecting,
      serverParams: initTable[string, string](),
      host: hostAddr,
      port: hostPort,
      config: config,
      notifyMaxQueue: 1024,
      stmtCacheCapacity: 256,
    )

  try:
    # SSL negotiation (before StartupMessage) — skip for Unix sockets
    if config.sslMode != sslDisable and not isUnix:
      await negotiateSSL(conn, config)

    when hasChronos:
      # If SSL was not established, create plain streams
      if conn.reader.isNil:
        conn.baseReader = newAsyncStreamReader(conn.transport)
        conn.baseWriter = newAsyncStreamWriter(conn.transport)
        conn.reader = conn.baseReader
        conn.writer = conn.baseWriter

    # Send StartupMessage
    var startupParams = config.extraParams
    if config.applicationName.len > 0:
      startupParams.add(("application_name", config.applicationName))
    await conn.sendMsg(encodeStartup(config.user, config.database, startupParams))
    conn.state = csAuthentication

    # Authentication loop
    var scramState: ScramState
    block authLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkAuthenticationOk:
            break authLoop
          of bmkAuthenticationCleartextPassword:
            await conn.sendMsg(encodePassword(config.password))
          of bmkAuthenticationMD5Password:
            let hash = md5AuthHash(config.user, config.password, msg.md5Salt)
            await conn.sendMsg(encodePassword(hash))
          of bmkAuthenticationSASL:
            let choice = selectScramMechanism(
              conn.sslEnabled, conn.serverCertDer, msg.saslMechanisms,
              config.channelBinding,
            )
            let clientFirst = scramClientFirstMessage(
              config.user, scramState, choice.cbType, choice.cbData
            )
            await conn.sendMsg(encodeSASLInitialResponse(choice.mechanism, clientFirst))
          of bmkAuthenticationSASLContinue:
            let clientFinal =
              scramClientFinalMessage(config.password, msg.saslData, scramState)
            await conn.sendMsg(encodeSASLResponse(clientFinal))
          of bmkAuthenticationSASLFinal:
            if not scramVerifyServerFinal(msg.saslFinalData, scramState):
              raise newException(
                PgConnectionError, "SCRAM server signature verification failed"
              )
          of bmkErrorResponse:
            raise newException(PgConnectionError, formatError(msg.errorFields))
          else:
            discard
        await conn.fillRecvBuf()

    # Collect ParameterStatus, BackendKeyData until ReadyForQuery
    block readyLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkParameterStatus:
            conn.serverParams[msg.paramName] = msg.paramValue
          of bmkBackendKeyData:
            conn.pid = msg.backendPid
            conn.secretKey = msg.backendSecretKey
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            break readyLoop
          of bmkErrorResponse:
            raise newException(PgConnectionError, formatError(msg.errorFields))
          else:
            discard
        await conn.fillRecvBuf()

    # Discover extension type OIDs (hstore, etc.)
    conn.state = csBusy
    await conn.sendMsg(
      encodeQuery("SELECT oid, typarray FROM pg_type WHERE typname = 'hstore' LIMIT 1")
    )
    block discoverLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkRowDescription:
            discard
          of bmkDataRow:
            if msg.columns.len > 0 and msg.columns[0].isSome:
              try:
                conn.hstoreOid = int32(parseInt(bytesToString(msg.columns[0].get)))
              except ValueError:
                discard
            if msg.columns.len > 1 and msg.columns[1].isSome:
              try:
                conn.hstoreArrayOid = int32(parseInt(bytesToString(msg.columns[1].get)))
              except ValueError:
                discard
          of bmkCommandComplete, bmkEmptyQueryResponse:
            discard
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            break discoverLoop
          of bmkErrorResponse:
            discard
          else:
            discard
        await conn.fillRecvBuf()

    conn.createdAt = Moment.now()
    return conn
  except CatchableError as e:
    await conn.closeTransport()
    raise e

proc checkReady*(conn: PgConnection) =
  ## Assert that the connection is in `csReady` state. Raises `PgConnectionError` otherwise.
  if conn.state != csReady:
    raise newException(
      PgConnectionError, "Connection is not ready (state: " & $conn.state & ")"
    )

proc quoteIdentifier*(s: string): string =
  ## Quote a SQL identifier (e.g. table/channel name) with double quotes, escaping embedded quotes.
  "\"" & s.replace("\"", "\"\"") & "\""

proc simpleQuery*(conn: PgConnection, sql: string): Future[seq[QueryResult]] {.async.} =
  ## Execute one or more SQL statements via simple query protocol.
  ## Returns one `QueryResult` per statement. Supports multiple statements separated by semicolons.
  conn.checkReady()

  var results: seq[QueryResult]
  var totalRows: int32
  var lastTag: string
  # For multi-statement queries (e.g. "SELECT 1; SELECT 2"), the trace end hook
  # receives the aggregated row count and only the last command tag.
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: lastTag, rowCount: totalRows),
  ):
    conn.state = csBusy
    await conn.sendMsg(encodeQuery(sql))

    var current = QueryResult()
    var queryError: ref PgQueryError

    block recvLoop:
      while true:
        while (
          let opt = conn.nextMessage(current.data, addr current.rowCount)
          opt.isSome
        )
        :
          let msg = opt.get
          case msg.kind
          of bmkRowDescription:
            current =
              QueryResult(fields: msg.fields, data: newRowData(int16(msg.fields.len)))
          of bmkCommandComplete:
            current.commandTag = msg.commandTag
            results.add(current)
            current = QueryResult()
          of bmkEmptyQueryResponse:
            results.add(QueryResult())
          of bmkErrorResponse:
            queryError = newPgQueryError(msg.errorFields)
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            if queryError != nil:
              raise queryError
            break recvLoop
          else:
            discard
        await conn.fillRecvBuf()

    for r in results:
      totalRows += r.rowCount
      if r.commandTag.len > 0:
        lastTag = r.commandTag

  return results

proc simpleExecImpl(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))
  var commandTag = ""
  var queryError: ref PgQueryError
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCommandComplete:
          commandTag = msg.commandTag
        of bmkRowDescription, bmkDataRow, bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)
  return commandTag

proc cancel*(conn: PgConnection): Future[void] {.async.} =
  ## Send a CancelRequest over a separate connection to abort the running query.
  let isUnix = isUnixSocket(conn.host)
  when hasChronos:
    let transport =
      if isUnix:
        when defined(posix):
          await connect(initTAddress(unixSocketPath(conn.host, conn.port)))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        let addresses = resolveTAddress(conn.host, Port(conn.port))
        if addresses.len == 0:
          raise newException(PgConnectionError, "Could not resolve host: " & conn.host)
        await connect(addresses[0])
    try:
      let msg = encodeCancelRequest(conn.pid, conn.secretKey)
      discard await transport.write(msg)
    finally:
      await transport.closeWait()
  elif hasAsyncDispatch:
    let sock =
      if isUnix:
        when defined(posix):
          newAsyncSocket(
            Domain.AF_UNIX, SockType.SOCK_STREAM, Protocol.IPPROTO_IP, buffered = false
          )
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        newAsyncSocket(buffered = false)
    try:
      if isUnix:
        when defined(posix):
          await sock.connectUnix(unixSocketPath(conn.host, conn.port))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        await sock.connect(conn.host, Port(conn.port))
      let msg = encodeCancelRequest(conn.pid, conn.secretKey)
      await sock.sendRawBytes(msg)
    finally:
      sock.close()

proc cancelNoWait*(conn: PgConnection) =
  ## Schedule a best-effort CancelRequest without waiting. For use in timeout handlers.
  proc doCancel() {.async.} =
    try:
      await conn.cancel()
    except CatchableError:
      discard

  asyncSpawn doCancel()

proc invalidateOnTimeout*(conn: PgConnection, reason: string) =
  ## Timeout recovery for a connection whose last request may have left the
  ## protocol out of sync. Schedules a best-effort CancelRequest via
  ## `cancelNoWait`, marks the connection `csClosed` so it cannot be reused,
  ## and raises `PgTimeoutError` with `reason`.
  ##
  ## Under asyncdispatch this is the **only** safe recovery path: the inner
  ## future keeps running in the background after `wait()` fires, and may
  ## still write to the socket. Reusing the connection would interleave its
  ## stale write with a new request and corrupt the protocol stream. chronos
  ## cancels the inner future properly, but we still invalidate unconditionally
  ## — the server may have processed the request partially and the cached
  ## session state (prepared statements, portals, transaction status) is no
  ## longer reliable.
  conn.cancelNoWait()
  conn.state = csClosed
  raise newException(PgTimeoutError, reason)

proc simpleExec*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute a SQL statement via simple query protocol, returning the command result.
  ## Lighter than `exec` for parameter-less commands (no Parse/Bind/Describe overhead).
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    if timeout > ZeroDuration:
      try:
        tag = await simpleExecImpl(conn, sql, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("simpleExec timed out")
    else:
      tag = await simpleExecImpl(conn, sql)
  return initCommandResult(tag)

proc isConnected(conn: PgConnection): bool =
  when hasChronos:
    not conn.writer.isNil
  elif hasAsyncDispatch:
    not conn.socket.isNil

proc ping*(conn: PgConnection, timeout = ZeroDuration): Future[void] =
  ## Lightweight health check using an empty simple query.
  ## Sends Query("") -> expects EmptyQueryResponse + ReadyForQuery.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  proc perform(): Future[void] {.async.} =
    conn.checkReady()
    if not conn.isConnected():
      conn.state = csClosed
      raise newException(PgConnectionError, "Connection is not established")
    conn.state = csBusy
    await conn.sendMsg(encodeQuery(""))

    var queryError: ref PgQueryError
    block recvLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkEmptyQueryResponse:
            discard
          of bmkErrorResponse:
            queryError = newPgQueryError(msg.errorFields)
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            if queryError != nil:
              raise queryError
            break recvLoop
          else:
            discard
        await conn.fillRecvBuf(timeout)

  if timeout > ZeroDuration:
    proc withTimeout(): Future[void] {.async.} =
      try:
        await perform().wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Ping timed out")

    withTimeout()
  else:
    perform()

proc close*(conn: PgConnection): Future[void] {.async.} =
  ## Close the connection. Idempotent: safe to call multiple times.
  # Stop background listen pump if running
  if conn.listenTask != nil and not conn.listenTask.finished:
    await cancelAndWait(conn.listenTask)
  conn.listenTask = nil
  # Only send Terminate if we haven't already detected the connection is dead
  if conn.state != csClosed and conn.isConnected():
    try:
      await conn.sendMsg(encodeTerminate())
    except CatchableError:
      discard
  conn.state = csClosed
  # Fail any pending notification waiter
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    conn.notifyWaiter.fail(newException(PgError, "Connection closed"))
  await conn.closeTransport()

proc checkSessionAttrs(
    conn: PgConnection, attrs: TargetSessionAttrs
): Future[bool] {.async.} =
  ## Check whether a connection matches the desired target_session_attrs.
  ## Uses `SHOW transaction_read_only` to determine server role.
  if attrs == tsaAny:
    return true
  let results = await conn.simpleQuery("SHOW transaction_read_only")
  var readOnly = false
  if results.len > 0 and results[0].rowCount > 0:
    let val = results[0].rows[0][0]
    if val.isSome:
      readOnly = bytesToString(val.get) == "on"
  case attrs
  of tsaAny:
    true # unreachable, handled above
  of tsaReadWrite, tsaPrimary:
    not readOnly
  of tsaReadOnly, tsaStandby, tsaPreferStandby:
    readOnly

proc connect*(config: ConnConfig): Future[PgConnection] =
  ## Establish a new connection to a PostgreSQL server.
  ## Supports multi-host failover: tries each host in order.
  ## Respects `targetSessionAttrs` to select the appropriate server type.
  ## The `connectTimeout` wraps the entire multi-host connection attempt.
  proc perform(): Future[PgConnection] {.async.} =
    let hosts = config.getHosts()
    var errors: seq[string]

    if config.targetSessionAttrs == tsaPreferStandby:
      # First pass: look for a standby
      for entry in hosts:
        try:
          let conn = await connectToHost(config, entry.host, entry.port)
          if await conn.checkSessionAttrs(tsaStandby):
            return conn
          await conn.close()
        except CancelledError as e:
          raise e
        except CatchableError as e:
          errors.add(entry.host & ":" & $entry.port & ": " & e.msg)
      # Second pass: accept any server
      for entry in hosts:
        try:
          let conn = await connectToHost(config, entry.host, entry.port)
          return conn
        except CancelledError as e:
          raise e
        except CatchableError as e:
          errors.add(entry.host & ":" & $entry.port & ": " & e.msg)
    else:
      for entry in hosts:
        try:
          let conn = await connectToHost(config, entry.host, entry.port)
          if config.targetSessionAttrs == tsaAny or
              await conn.checkSessionAttrs(config.targetSessionAttrs):
            return conn
          await conn.close()
          errors.add(
            entry.host & ":" & $entry.port &
              ": server does not match target_session_attrs " &
              $config.targetSessionAttrs
          )
        except CancelledError as e:
          raise e
        except CatchableError as e:
          errors.add(entry.host & ":" & $entry.port & ": " & e.msg)

    raise newException(
      PgConnectionError, "Could not connect to any host: " & errors.join("; ")
    )

  proc wrapped(): Future[PgConnection] {.async.} =
    let hosts = config.getHosts()
    var conn: PgConnection
    withTracing(
      config.tracer,
      onConnectStart,
      onConnectEnd,
      TraceConnectStartData(hosts: hosts),
      TraceConnectEndData,
      TraceConnectEndData(conn: conn),
    ):
      conn =
        if config.connectTimeout != default(Duration):
          await perform().wait(config.connectTimeout)
        else:
          await perform()
      conn.tracer = config.tracer
    return conn

  wrapped()

proc onNotify*(conn: PgConnection, callback: NotifyCallback) =
  ## Set a callback invoked for each incoming NOTIFY message.
  conn.notifyCallback = callback

proc reconnectInPlace(conn: PgConnection) {.async.} =
  ## Reconnect using stored config, re-LISTENing on all channels.
  await conn.closeTransport()
  conn.recvBuf.setLen(0)
  conn.recvBufStart = 0
  conn.clearStmtCache()
  conn.state = csConnecting
  var newConn: PgConnection
  try:
    newConn = await connect(conn.config)
  except CatchableError as e:
    conn.state = csClosed
    raise e
  when hasChronos:
    conn.transport = newConn.transport
    conn.baseReader = newConn.baseReader
    conn.baseWriter = newConn.baseWriter
    conn.reader = newConn.reader
    conn.writer = newConn.writer
    conn.tlsStream = newConn.tlsStream
    conn.trustAnchorBufs = newConn.trustAnchorBufs
  elif hasAsyncDispatch:
    conn.socket = newConn.socket
  conn.sslEnabled = newConn.sslEnabled
  conn.recvBuf = newConn.recvBuf
  conn.pid = newConn.pid
  conn.secretKey = newConn.secretKey
  conn.serverParams = newConn.serverParams
  conn.txStatus = newConn.txStatus
  conn.state = csReady
  conn.createdAt = newConn.createdAt
  conn.hstoreOid = newConn.hstoreOid
  conn.hstoreArrayOid = newConn.hstoreArrayOid
  for ch in conn.listenChannels:
    discard await conn.simpleQuery("LISTEN " & quoteIdentifier(ch))

proc listenPump(conn: PgConnection) {.async.} =
  ## Background loop: repeatedly receives messages, dispatching notifications.
  ## Non-notification messages are discarded (recvMessage handles dispatch).
  ## On connection failure, attempts automatic reconnection with exponential
  ## backoff (up to 10 attempts) and re-subscribes to all channels.
  ## Exits cleanly when state changes from csListening (via stopListening
  ## sending an empty query), then drains until ReadyForQuery.
  while true:
    try:
      while conn.state == csListening:
        discard await conn.recvMessage()
      # State changed: drain the stop-signal query response until ReadyForQuery
      block drainLoop:
        while true:
          while (let opt = conn.nextMessage(); opt.isSome):
            let msg = opt.get
            if msg.kind == bmkReadyForQuery:
              conn.txStatus = msg.txStatus
              break drainLoop
          await conn.fillRecvBuf()
      return # Clean exit via stopListening
    except CancelledError:
      return # Cancelled from close()
    except CatchableError:
      if conn.listenChannels.len == 0:
        conn.state = csClosed
        if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
          conn.notifyWaiter.fail(newException(PgError, "Connection closed"))
        return
      # Auto-reconnect with exponential backoff
      var reconnected = false
      var backoff = 1
      for attempt in 0 ..< 10:
        try:
          await sleepAsync(seconds(backoff))
          await conn.reconnectInPlace()
          conn.state = csListening
          reconnected = true
          if conn.reconnectCallback != nil:
            conn.reconnectCallback()
          break
        except CancelledError:
          return
        except CatchableError:
          backoff = min(backoff * 2, 30)
      if not reconnected:
        conn.listenErrorMsg =
          "Listen connection lost: reconnection failed after 10 attempts"
        conn.state = csClosed
        if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
          conn.notifyWaiter.fail(newException(PgError, conn.listenErrorMsg))
        return

proc startListening(conn: PgConnection) =
  conn.state = csListening
  conn.listenTask = conn.listenPump()

proc stopListening*(conn: PgConnection) {.async.} =
  ## Stop the background listen pump and return the connection to `csReady`.
  if conn.listenTask == nil or conn.listenTask.finished:
    conn.listenTask = nil
    if conn.state == csListening:
      conn.state = csReady
    return
  # Signal pump to exit by changing state, then send empty query to unblock read
  conn.state = csBusy
  try:
    await conn.sendMsg(encodeQuery(""))
    # Wait for pump to drain and exit naturally
    await conn.listenTask
  except CancelledError as e:
    raise e
  except CatchableError:
    # Send or pump failed: connection is dead
    if conn.listenTask != nil and not conn.listenTask.finished:
      await cancelAndWait(conn.listenTask)
    conn.listenTask = nil
    conn.state = csClosed
    return
  conn.listenTask = nil
  # Preserve csClosed if pump detected a connection error
  if conn.state != csClosed:
    conn.state = csReady

proc listen*(conn: PgConnection, channel: string): Future[void] {.async.} =
  ## Subscribe to a LISTEN channel and start the background notification pump.
  if conn.state == csListening:
    await conn.stopListening()
  conn.checkReady()
  discard await conn.simpleQuery("LISTEN " & quoteIdentifier(channel))
  conn.listenChannels.incl(channel)
  conn.startListening()

proc unlisten*(conn: PgConnection, channel: string): Future[void] {.async.} =
  ## Unsubscribe from a LISTEN channel. Stops the pump if no channels remain.
  if conn.state == csListening:
    await conn.stopListening()
  conn.checkReady()
  discard await conn.simpleQuery("UNLISTEN " & quoteIdentifier(channel))
  conn.listenChannels.excl(channel)
  if conn.listenChannels.len > 0:
    conn.startListening()

proc checkNotifyOverflow(conn: PgConnection) =
  ## Raise PgNotifyOverflowError if notifications were dropped since last check.
  if conn.notifyDropped > 0:
    let dropped = conn.notifyDropped
    conn.notifyDropped = 0
    let err = (ref PgNotifyOverflowError)(
      msg: "Dropped " & $dropped & " notifications due to queue overflow",
      dropped: dropped,
    )
    raise err

proc checkListenAlive(conn: PgConnection) =
  ## Raise if the listen pump has died permanently.
  if conn.listenErrorMsg.len > 0:
    raise newException(PgConnectionError, conn.listenErrorMsg)
  if conn.state == csClosed:
    raise newException(PgConnectionError, "Connection is closed")

proc waitNotification*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[Notification] {.async.} =
  ## Wait for the next notification from the buffer.
  ## If the buffer is empty, blocks until a notification arrives or timeout expires.
  ## Raises PgNotifyOverflowError if notifications were dropped due to queue overflow.
  ## Raises PgError if the listen pump has died (e.g. reconnection failed).
  conn.checkNotifyOverflow()
  conn.checkListenAlive()
  if conn.notifyQueue.len > 0:
    return conn.notifyQueue.popFirst()
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    raise newException(PgError, "Another waitNotification is already active")
  conn.notifyWaiter = newFuture[void]("waitNotification")
  try:
    if timeout > ZeroDuration:
      try:
        await conn.notifyWaiter.wait(timeout)
      except AsyncTimeoutError:
        raise newException(PgTimeoutError, "Wait for notification timed out")
    else:
      await conn.notifyWaiter
  finally:
    conn.notifyWaiter = nil
  conn.checkNotifyOverflow()
  if conn.notifyQueue.len > 0:
    return conn.notifyQueue.popFirst()
  raise newException(PgError, "No notification available")

proc parseSslMode(s: string): SslMode =
  case s
  of "disable":
    sslDisable
  of "allow":
    sslAllow
  of "prefer":
    sslPrefer
  of "require":
    sslRequire
  of "verify-ca":
    sslVerifyCa
  of "verify-full":
    sslVerifyFull
  else:
    raise newException(PgError, "Invalid sslmode: " & s)

proc parseChannelBindingMode(s: string): ChannelBindingMode =
  case s
  of "disable":
    cbDisable
  of "prefer":
    cbPrefer
  of "require":
    cbRequire
  else:
    raise newException(PgError, "Invalid channel_binding: " & s)

proc parseTargetSessionAttrs(s: string): TargetSessionAttrs =
  case s
  of "any":
    tsaAny
  of "read-write":
    tsaReadWrite
  of "read-only":
    tsaReadOnly
  of "primary":
    tsaPrimary
  of "standby":
    tsaStandby
  of "prefer-standby":
    tsaPreferStandby
  else:
    raise newException(PgError, "Invalid target_session_attrs: " & s)

proc parsePort(s: string): int =
  try:
    result = parseInt(s)
  except ValueError:
    raise newException(PgError, "Invalid port in DSN: " & s)
  if result < 1 or result > 65535:
    raise newException(PgError, "Port out of range (1-65535): " & s)

proc applyParam(result: var ConnConfig, key, val: string) =
  ## Apply a single connection parameter to a ConnConfig.
  case key
  of "host", "hostaddr":
    result.host = val
  of "port":
    result.port = parsePort(val)
  of "dbname":
    result.database = val
  of "user":
    result.user = val
  of "password":
    result.password = val
  of "sslmode":
    result.sslMode = parseSslMode(val)
  of "channel_binding":
    result.channelBinding = parseChannelBindingMode(val)
  of "application_name":
    result.applicationName = val
  of "connect_timeout":
    try:
      result.connectTimeout = seconds(parseInt(val))
    except ValueError:
      raise newException(PgError, "Invalid connect_timeout: " & val)
  of "sslrootcert":
    try:
      result.sslRootCert = readFile(val)
    except IOError:
      raise newException(PgError, "Cannot read sslrootcert file: " & val)
  of "keepalives":
    try:
      result.keepAlive = parseInt(val) != 0
    except ValueError:
      raise newException(PgError, "Invalid keepalives: " & val)
  of "keepalives_idle":
    try:
      result.keepAliveIdle = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid keepalives_idle: " & val)
    if result.keepAliveIdle < 0:
      raise newException(PgError, "keepalives_idle must be non-negative: " & val)
  of "keepalives_interval":
    try:
      result.keepAliveInterval = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid keepalives_interval: " & val)
    if result.keepAliveInterval < 0:
      raise newException(PgError, "keepalives_interval must be non-negative: " & val)
  of "keepalives_count":
    try:
      result.keepAliveCount = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid keepalives_count: " & val)
    if result.keepAliveCount < 0:
      raise newException(PgError, "keepalives_count must be non-negative: " & val)
  of "target_session_attrs":
    result.targetSessionAttrs = parseTargetSessionAttrs(val)
  else:
    result.extraParams.add((key, val))

proc parseKeyValueDsn(dsn: string): ConnConfig =
  ## Parse a libpq keyword=value connection string into a ConnConfig.
  ##
  ## Format: ``host=localhost port=5432 dbname=test user=myuser``
  ##
  ## Values may be single-quoted: ``password='has spaces'``
  ## Within quoted values, ``\'`` and ``\\`` are escape sequences.
  result.keepAlive = true
  result.host = "127.0.0.1"
  result.port = 5432

  # Tokenize into (key, value) pairs
  var pairs: seq[(string, string)]
  var i = 0
  while i < dsn.len:
    # Skip whitespace
    while i < dsn.len and dsn[i] in {' ', '\t', '\n', '\r'}:
      inc i
    if i >= dsn.len:
      break

    # Read key
    var key = ""
    while i < dsn.len and dsn[i] notin {'=', ' ', '\t', '\n', '\r'}:
      key.add dsn[i]
      inc i
    if key.len == 0:
      raise newException(PgError, "Empty key in connection string")

    # Skip whitespace around '='
    while i < dsn.len and dsn[i] in {' ', '\t'}:
      inc i
    if i >= dsn.len or dsn[i] != '=':
      raise newException(
        PgError, "Expected '=' after key '" & key & "' in connection string"
      )
    inc i # skip '='
    while i < dsn.len and dsn[i] in {' ', '\t'}:
      inc i

    # Read value
    var val = ""
    if i < dsn.len and dsn[i] == '\'':
      # Quoted value
      inc i # skip opening quote
      var closed = false
      while i < dsn.len:
        if dsn[i] == '\\' and i + 1 < dsn.len:
          # Escape sequence
          val.add dsn[i + 1]
          i += 2
        elif dsn[i] == '\'':
          inc i # skip closing quote
          closed = true
          break
        else:
          val.add dsn[i]
          inc i
      if not closed:
        raise newException(PgError, "Unterminated quoted value for key '" & key & "'")
    else:
      # Unquoted value
      while i < dsn.len and dsn[i] notin {' ', '\t', '\n', '\r'}:
        val.add dsn[i]
        inc i

    pairs.add((key, val))

  for (key, val) in pairs:
    result.applyParam(key, val)

  if result.host.len == 0:
    result.host = "127.0.0.1"

proc parseUriDsn(dsn: string): ConnConfig =
  ## Parse a PostgreSQL URI connection string into a ConnConfig.
  result.keepAlive = true
  let scheme =
    if dsn.startsWith("postgresql://"):
      "postgresql"
    elif dsn.startsWith("postgres://"):
      "postgres"
    else:
      raise newException(
        PgError, "Invalid DSN scheme: expected postgresql:// or postgres://"
      )

  # Strip scheme prefix
  let rest = dsn[scheme.len + 3 .. ^1] # skip "scheme://"

  # Split query string
  var body: string
  var queryStr: string
  let qpos = rest.find('?')
  if qpos >= 0:
    body = rest[0 ..< qpos]
    queryStr = rest[qpos + 1 .. ^1]
  else:
    body = rest

  # Split userinfo and hostpath by '@'
  var userinfo, hostpath: string
  let apos = body.rfind('@')
  if apos >= 0:
    userinfo = body[0 ..< apos]
    hostpath = body[apos + 1 .. ^1]
  else:
    hostpath = body

  # Parse user:password
  if userinfo.len > 0:
    let cpos = userinfo.find(':')
    if cpos >= 0:
      result.user = decodeUrl(userinfo[0 ..< cpos])
      result.password = decodeUrl(userinfo[cpos + 1 .. ^1])
    else:
      result.user = decodeUrl(userinfo)

  # Parse host:port/database
  var hostport, dbpath: string
  let spos = hostpath.find('/')
  if spos >= 0:
    hostport = hostpath[0 ..< spos]
    dbpath = hostpath[spos + 1 .. ^1]
  else:
    hostport = hostpath

  if dbpath.len > 0:
    result.database = decodeUrl(dbpath)

  # Parse host(s) and port(s) — supports comma-separated multi-host syntax
  proc parseHostEntry(entry: string): HostEntry =
    if entry.startsWith("["):
      # IPv6: [::1]:5432
      let bracket = entry.find(']')
      if bracket < 0:
        raise newException(PgError, "Invalid IPv6 address in DSN")
      result.host = entry[1 ..< bracket]
      let afterBracket = entry[bracket + 1 .. ^1]
      if afterBracket.startsWith(":"):
        result.port = parsePort(afterBracket[1 .. ^1])
      else:
        result.port = 5432
    else:
      let cpos = entry.rfind(':')
      if cpos >= 0:
        result.host = entry[0 ..< cpos]
        result.port = parsePort(entry[cpos + 1 .. ^1])
      else:
        result.host = entry
        result.port = 5432

  if hostport.len > 0:
    let parts = hostport.split(',')
    for part in parts:
      if part.len > 0:
        result.hosts.add(parseHostEntry(part))
    # Back-compat: set host/port from first entry
    result.host = result.hosts[0].host
    result.port = result.hosts[0].port
  else:
    result.host = "127.0.0.1"
    result.port = 5432
    result.hosts = @[HostEntry(host: "127.0.0.1", port: 5432)]

  if result.host.len == 0:
    result.host = "127.0.0.1"

  # Parse query parameters
  if queryStr.len > 0:
    for pair in queryStr.split('&'):
      let epos = pair.find('=')
      if epos < 0:
        continue
      let key = decodeUrl(pair[0 ..< epos])
      let val = decodeUrl(pair[epos + 1 .. ^1])
      result.applyParam(key, val)

proc initConnConfig*(
    host = "127.0.0.1",
    port = 5432,
    user = "",
    password = "",
    database = "",
    sslMode = sslDisable,
    sslRootCert = "",
    channelBinding = cbPrefer,
    applicationName = "",
    connectTimeout = ZeroDuration,
    keepAlive = true,
    keepAliveIdle = 0,
    keepAliveInterval = 0,
    keepAliveCount = 0,
    hosts: seq[HostEntry] = @[],
    targetSessionAttrs = tsaAny,
    extraParams: seq[(string, string)] = @[],
): ConnConfig =
  ## Create a connection configuration with sensible defaults.
  ## For DSN-based configuration, use `parseDsn` instead.
  ConnConfig(
    host: host,
    port: port,
    user: user,
    password: password,
    database: database,
    sslMode: sslMode,
    sslRootCert: sslRootCert,
    channelBinding: channelBinding,
    applicationName: applicationName,
    connectTimeout: connectTimeout,
    keepAlive: keepAlive,
    keepAliveIdle: keepAliveIdle,
    keepAliveInterval: keepAliveInterval,
    keepAliveCount: keepAliveCount,
    hosts: hosts,
    targetSessionAttrs: targetSessionAttrs,
    extraParams: extraParams,
  )

proc parseDsn*(dsn: string): ConnConfig =
  ## Parse a PostgreSQL connection string into a ConnConfig.
  ##
  ## Supports two formats:
  ## - URI: ``postgresql://[user[:password]@][host[:port]][/database][?param=value&...]``
  ## - keyword=value: ``host=localhost port=5432 dbname=test`` (libpq compatible)
  ##
  ## Both ``postgresql://`` and ``postgres://`` schemes are accepted for URI format.
  if dsn.startsWith("postgresql://") or dsn.startsWith("postgres://"):
    parseUriDsn(dsn)
  else:
    parseKeyValueDsn(dsn)

proc connect*(dsn: string): Future[PgConnection] =
  ## Shorthand for ``connect(parseDsn(dsn))``.
  connect(parseDsn(dsn))
