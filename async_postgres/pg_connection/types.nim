## Shared building blocks for ``pg_connection`` submodules (``PgConnection``,
## ``ConnConfig``, tracing) and the state operations that keep its private
## fields consistent (see the sections below).
##
## Internal module: not part of the public API. Import the `pg_connection` hub instead.

import std/[tables, sets, deques, lists, macros, options]
from std/strutils import
  isAlphaNumeric, toLowerAscii, cmpIgnoreCase, split, strip, startsWith, Whitespace
when defined(posix):
  import std/posix

import ../[async_backend, pg_auth, pg_errors, pg_gensym, pg_protocol, pg_types]
import ../pg_types/encoding

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet
  from std/nativesockets import Domain, Port

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

const
  closedByUserMsg* = "Connection closed by the application"
    ## Shared by `failNotifyWaiter` and `checkListenAlive` so a deliberate
    ## `close()` reports the same thing whether a waiter was parked or not.
  DefaultNotifyMaxQueue* = 1024 ## Connect-time pull-API count cap.
  DefaultNotifyMaxQueueBytes* = 16 * 1024 * 1024
    ## Connect-time pull-API byte cap. 1024 × (channel + ≤8000-byte payload)
    ## fits with headroom.

const ForkFailureText* = "could not fork new process for connection: "
  ## Start of the postmaster's error when it cannot fork a backend (untranslated).

var listenReconnectStopWaitMs* = 10_000
  ## Max wait (ms) for a listen pump stuck in a blocking `connect()`; it is
  ## orphaned on timeout. Not re-exported through `pg_connection`, so call
  ## sites cannot set it to 0 via the aggregate import and disable orphan safety.

when hasChronos:
  type DialTarget* = TransportAddress
    ## One address to dial: an IP and port, or a Unix socket path.

elif hasAsyncDispatch:
  when defined(posix):
    type DialTarget* =
      tuple[
        domain: Domain,
        address: string,
        port: Port,
        sa: Sockaddr_storage,
        saLen: SockLen,
      ]
      ## One address to dial: an IP and port, or for ``AF_UNIX`` a socket path.
      ## ``address`` is for display (zone kept); ``sa`` is what is dialed.

  else:
    type DialTarget* = tuple[domain: Domain, address: string, port: Port]
      ## One address to dial: an IP (its zone kept) and port.

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
    amScramSha256
      ## SASL SCRAM-SHA-256, with or without channel binding (libpq:
      ## "scram-sha-256")
    amScramSha256Plus
      ## SASL SCRAM-SHA-256-PLUS alone ("scram-sha-256-plus", not in libpq)

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
    host*: string
      ## Host name (or Unix socket dir); used for SSL verification, except
      ## `127.0.0.1` alongside a `hostaddr` (see `effectiveHost`).
    hostaddr*: string
      ## Address dialed instead of resolving `host` (libpq `hostaddr`).
      ## Empty = resolve `host`.
    port*: int

  ConnConfig* = object
    ## Connection configuration. Construct via `parseDsn` or set fields directly.
    host*: string
    port*: int # default 5432
    hostaddr*: string
      ## Address dialed instead of resolving `host` (libpq `hostaddr`).
      ## `host` is still the name used for SSL certificate verification,
      ## except `127.0.0.1` (see `effectiveHost`).
    user*: string
    password*: string
      ## Cleartext password (libpq ``password``), held in plaintext in memory.
      ## DSN parse/validation errors never echo parameter values, but callers
      ## must not log it either.
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
      ## PKCS#8, PKCS#1 (``RSA PRIVATE KEY``) and SEC1 (``EC PRIVATE KEY``) PEM
      ## are accepted. Must be paired with ``sslCert``.
    sslSni*: bool
      ## Send TLS SNI (default true; suppressed for IP/empty host). chronos
      ## ignores it: it sends SNI only under `sslVerifyFull`, where BearSSL
      ## checks the same name.
    channelBinding*: ChannelBindingMode
      ## SCRAM channel binding policy (default cbPrefer). `cbRequire` fails the
      ## connection if SCRAM-SHA-256-PLUS cannot actually be used (libpq parity).
    requireAuth*: set[AuthMethod]
      ## Allowed auth methods; empty = any (libpq ``require_auth`` parity).
      ## Cleartext password and MD5 are allowed by default; set this (e.g. to
      ## ``{amScramSha256}``) to reject them.
    applicationName*: string
    connectTimeout*: Duration
      ## Timeout per address a host resolves to, dial to ready (libpq
      ## ``connect_timeout``); ``ZeroDuration`` (default, or negative) = none.
    keepAlive*: bool
      ## Enable TCP keepalive (default true via parseDsn). POSIX only: Windows
      ## has no keepalive path, so this and the timing options below are ignored there.
    keepAliveIdle*: int
      ## Seconds before first probe (0 = OS default; at most 32767 on Linux).
      ## POSIX only.
    keepAliveInterval*: int
      ## Seconds between probes (0 = OS default; at most 32767 on Linux). POSIX only.
    keepAliveCount*: int
      ## Number of probes before giving up (0 = OS default; at most 127 on Linux).
      ## POSIX only.
    hosts*: seq[HostEntry] ## Multiple hosts for failover (empty = use host/port)
    targetSessionAttrs*: TargetSessionAttrs ## Target server type (default tsaAny)
    loadBalanceHosts*: LoadBalanceHosts
      ## Host ordering for multi-host connections (libpq `load_balance_hosts`);
      ## see `LoadBalanceHosts`. `lbhDisable` (default) preserves the configured
      ## order.
    extraParams*: seq[(string, string)]
      ## Additional StartupMessage parameters (unknown DSN keys land here).
      ## Forwarded verbatim; treat as trusted config — typos are not rejected.
      ## Exception: ``client_encoding`` is always sent as UTF8, and another
      ## value (also via ``-c`` in ``options``) raises ``PgConfigError``.
      ## Likewise ``DateStyle`` is always sent with the ISO output style, so it
      ## overrides a role or database one, field order included (the order from
      ## the server's configuration or a ``-c`` in ``options`` stays). One here
      ## may set only the field order (``DMY``, ...); another output style
      ## raises ``PgConfigError`` the same way. As startup values, both survive
      ## ``RESET`` and ``DISCARD ALL``; a later ``SET`` of either to a value the
      ## decoders cannot read closes the connection with ``PgProtocolError``.
      ## ``TimeZone`` is sent as UTC unless set here or via ``-c`` in
      ## ``options``; ``DEFAULT`` sends none, keeping the server's, database's
      ## or role's zone. A session asked for UTC that reports another zone (a
      ## proxy dropped the startup value) fails ``connect`` with
      ## ``PgConnectionError``. In another zone, the ``DateTime`` encoders that
      ## send ``timestamp`` (``toPgParam``, ``toPgBinaryParam``,
      ## ``toPgTimestampArrayParam``) shift by its offset when bound to
      ## ``timestamptz``; the ``TimestampTz`` ones do not. The ``DateTime``
      ## range and multirange encoders shift the same way when a ``tsrange`` or
      ## ``tsmultirange`` value is bound to its ``tstz`` counterpart.
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
    noData*: bool ## Describe answered ``NoData``: no rows, unlike a zero-column result.
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
    serverParamsBytes: int
      ## Running sum of ``name.len + value.len`` over ``serverParams`` entries.
      ## Enforced against ``MaxServerParamsBytes`` on every ``ParameterStatus``.
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
    cancelTarget: seq[DialTarget]
      ## The address dialed, where ``cancel`` sends its request; empty if never
      ## dialed.
    sslHost: string
      ## Name the session's TLS was checked against (``HostEntry.host``);
      ## ``cancel`` checks its own TLS against the same name.
    createdAt: Moment
    portalCounter: int
    config: ConnConfig
    notifyQueue: Deque[Notification]
    notifyMaxQueue: int
      ## Pull-API count cap (`DefaultNotifyMaxQueue`; <=0 = unbounded). A
      ## handoff and a requeue of it sit outside the queue, so at most
      ## ``notifyMaxQueue + 1`` until the next arrival trims.
    notifyMaxQueueBytes: int
      ## Pull-API byte cap (`DefaultNotifyMaxQueueBytes`; <=0 = unbounded).
      ## Queued ``channel.len + payload.len`` only; same overshoot as count.
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
    fatalServerError: ref PgQueryError
      ## The FATAL/PANIC ErrorResponse that ended the session; attached to
      ## closed-connection errors as ``serverError``.
    txAbortFields: seq[ErrorField]
      ## Fields of the last ErrorResponse received while not in a failed block,
      ## so in one, the error that failed it. Kept past that block's end, so a
      ## COMMIT answered with ROLLBACK can name the cause, until that COMMIT
      ## takes it or a ReadyForQuery with no failed block on either side.
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
    stmtCacheResetGen: int ## Bumped by ``clearStmtCache``.
    pendingStmtCloses: seq[string]
      ## Server-side prepared statement names whose ``Close`` was not bundled
      ## with the operation that evicted them. Populated when the defensive
      ## eviction loop in ``addStmtCache`` fires (a pipeline never
      ## pre-evicts), when ``stmtCacheCapacity=`` shrinks the cache, when an
      ## entry is invalidated or replaced, and for statements no entry holds
      ## (a cache miss the server Parsed but the cache did not keep). Staged by
      ## ``stagePendingStmtCloses`` at the start of the next Extended Query
      ## send phase so the leak is bounded to the gap until the next
      ## operation, which moves them to ``stagedStmtCloses``.
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
    connectTimeZone: string
      ## ``TimeZone`` the pool restores on release: the zone the session started
      ## with, or — with ``timeZoneFollowsServer`` — the zone the server
      ## reported after its last ``RESET TimeZone``.
    timeZoneFollowsServer: bool
      ## No startup ``TimeZone`` (``DEFAULT``): the server's, database's or
      ## role's zone applies, so the pool restores it with ``RESET``.
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
    replWrites: Deque[ReplWrite]
      ## Replication writes not yet started, in wire order (see ``pg_replication``).
    replFlusher: Future[void] ## The task draining ``replWrites``; nil when idle.
    replFinalStatus: ReplWrite
      ## The stop's last status, written once ``replWrites`` is drained.
    replWriteFailure: ref CatchableError
      ## The write failure that ended the stream, kept as the cause of later errors.
    replPendingStatus: ReplWrite ## The library's status queued and not yet encoded.
    replWritesOpen: bool ## The stream accepts writes; cleared as it ends.
    replCopyDone: ReplWrite
      ## The client's CopyDone once requested, written after ``replFinalStatus``.
    replAutoConfirm: bool ## Logical stream confirms progress itself (``autoConfirm``).
    replInTxn: bool ## ``autoConfirm``: between a pgoutput Begin and Commit.
    replInCallback: bool
      ## The stream's callback is running; the stop's final status waits for it.
    replReportedRaw: tuple[receive, flush, apply: uint64]
      ## Replication: positions of the caller's last Standby Status Update (raw).
    replSentFlushRaw: uint64
      ## Replication: flush of the library's last Standby Status Update (raw).

  ReplWriteState* = enum
    ## Lifecycle of a queued replication write (internal).
    rwQueued ## Waiting in the queue.
    rwWriting ## Taken off the queue; a library status is encoded by then.
    rwWritten
    rwFailed ## Not written, whether it had started or not.

  ReplWrite* = ref object
    ## A queued replication write (internal; see ``pg_replication``).
    frame*: seq[byte] ## Encoded frame; empty for the library's status.
    waiters*: seq[Future[void]] ## Completed once written, failed if not.
    state*: ReplWriteState

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
    err*: ref CatchableError
      ## Nil = unlock returned false (not held). An unlock `Defect` arrives
      ## wrapped in `PgError` (`parent` = the Defect).

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
      ## Cleartext password sent over plaintext. Fires only for a request the
      ## client answers, after the order and require_auth checks.
    onDeprecatedAuth*: proc(data: TraceDeprecatedAuthData) {.gcsafe, raises: [].}
      ## MD5 password sent. Fires only for a request the client answers, after
      ## the order and require_auth checks.
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

func dsnName(mode: SslMode): string {.inline.} =
  ## libpq `sslmode=` spelling of `mode` for error messages.
  case mode
  of sslDisable: "disable"
  of sslAllow: "allow"
  of sslPrefer: "prefer"
  of sslRequire: "require"
  of sslVerifyCa: "verify-ca"
  of sslVerifyFull: "verify-full"

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
      "sslcert/sslkey require sslmode of prefer or stronger (got " &
        config.sslMode.dsnName & "); they would otherwise be silently unused",
    )

proc validateTlsConfig*(
    config: ConnConfig, overTcp = true
) {.raises: [PgConfigError].} =
  ## Reject TLS settings no server can satisfy (as libpq): a TLS requirement in a
  ## build without TLS (asyncdispatch without ``-d:ssl``), or, when ``overTcp``,
  ## a verified sslmode without ``sslrootcert`` (a Unix socket skips TLS).
  when not hasTls:
    const hint = ", which this build lacks (compile with -d:ssl)"
    if config.sslMode in {sslRequire, sslVerifyCa, sslVerifyFull}:
      raise newException(
        PgConfigError, "sslmode=" & config.sslMode.dsnName & " needs TLS" & hint
      )
    if config.channelBinding == cbRequire:
      raise newException(PgConfigError, "channel_binding=require needs TLS" & hint)
    if config.requireAuth == {amScramSha256Plus}:
      raise newException(
        PgConfigError,
        "require_auth allows only SCRAM-SHA-256-PLUS, which needs TLS" & hint,
      )
  if overTcp and config.sslMode in {sslVerifyCa, sslVerifyFull} and
      config.sslRootCert.len == 0:
    # Both backends would fall back to a Web PKI store, and verify-ca skips the
    # hostname check, so any publicly issued cert could MITM. Fail closed.
    raise newException(
      PgConfigError, "sslmode=verify-ca/verify-full requires sslrootcert to be set"
    )

# HostEntry accessors

const DefaultHost* = "127.0.0.1" ## Target when neither host nor hostaddr is given.

func effectiveHost*(entry: HostEntry): string {.inline.} =
  ## `host` as the name to verify. With a `hostaddr`, `127.0.0.1` is dropped:
  ## it may be the default left behind when `hostaddr` is set later, and an
  ## explicit one looks the same.
  if entry.hostaddr.len > 0 and entry.host == DefaultHost: "" else: entry.host

func dialAddr*(entry: HostEntry): string {.inline.} =
  ## The address actually dialed: `hostaddr` when given, otherwise `host`.
  ## Unlike libpq, a name in `hostaddr` is resolved rather than rejected.
  if entry.hostaddr.len > 0: entry.hostaddr else: entry.host

func displayHost*(entry: HostEntry): string {.inline.} =
  ## Host name for display and back-compat scalars: `host`, falling back to
  ## `hostaddr` (mirrors libpq's PQhost()).
  if entry.host.len > 0: entry.host else: entry.hostaddr

# Internal accessors
#
# Sibling modules reach the private record through these rather than
# `privateAccess`, which is reserved for tests. The hub re-exports none of
# them; the promised ones are the "Public accessors" section below. A field
# with a public getter gets a setter here instead: a `var` overload cannot
# share the getter's name. `state` and `config` are the two exceptions: the
# former is written only through ``markState``, the latter is an immutable
# `lent` view.
#
# Fields that have to move together get named operations instead of per-field
# `var` accessors, so no sibling can update one half of an invariant:
#
# - receive buffer (`recvBuf` / `recvBufStart`): read through ``recvBuf``,
#   ``recvBufStart`` and ``recvBufLen``, advance with ``consumeRecv``; the
#   fills live in the "Receive buffer" section.
# - wire debt (`pendingSyncs` / `unsyncedWrite`): ``noteWrite``,
#   ``settlePendingSync``, ``clearWireDebt`` and ``resetWireState``.
# - statement cache, its LRU list, counters and Close queues: the
#   "Statement cache" section.
# - notification queue with its waiter and handoff slot: the "Notification
#   queue" section.
# - replication write queue and stream flags: the "Replication write queue"
#   section.

proc newPgConnection*(host: string, port: int, config: ConnConfig): PgConnection =
  ## A ``csConnecting`` record with the default tunables, before the caller
  ## attaches its transport.
  PgConnection(
    state: csConnecting,
    serverParams: initTable[string, string](),
    host: host,
    port: port,
    config: config,
    notifyMaxQueue: DefaultNotifyMaxQueue,
    notifyMaxQueueBytes: DefaultNotifyMaxQueueBytes,
    stmtCacheCapacity: 256,
    listenReconnectMaxAttempts: 10,
    listenReconnectMaxBackoff: 30,
  )

when hasChronos:
  func transport*(conn: PgConnection): StreamTransport {.inline.} =
    conn.transport

  func baseReader*(conn: PgConnection): AsyncStreamReader {.inline.} =
    conn.baseReader

  func baseWriter*(conn: PgConnection): AsyncStreamWriter {.inline.} =
    conn.baseWriter

  func reader*(conn: PgConnection): AsyncStreamReader {.inline.} =
    conn.reader

  func writer*(conn: PgConnection): AsyncStreamWriter {.inline.} =
    conn.writer

  func tlsStream*(conn: PgConnection): TLSAsyncStream {.inline.} =
    conn.tlsStream

  proc attachTransport*(
      conn: PgConnection, t: StreamTransport, target: DialTarget, sslHost: string
  ) {.inline.} =
    ## Attach a freshly dialled transport with what ``cancel`` needs to reach
    ## the same server: its address and TLS name.
    conn.transport = t
    conn.cancelTarget = @[target]
    conn.sslHost = sslHost

  proc initPlainStreams*(conn: PgConnection) {.inline.} =
    ## Wire plaintext ``reader``/``writer`` from the transport when none exist.
    if conn.reader.isNil:
      conn.baseReader = newAsyncStreamReader(conn.transport)
      conn.baseWriter = newAsyncStreamWriter(conn.transport)
      conn.reader = conn.baseReader
      conn.writer = conn.baseWriter

  proc beginTlsBase*(conn: PgConnection) {.inline.} =
    ## Create the base streams a TLS handshake wraps.
    conn.baseReader = newAsyncStreamReader(conn.transport)
    conn.baseWriter = newAsyncStreamWriter(conn.transport)

  proc installTlsStream*(
      conn: PgConnection, stream: TLSAsyncStream, backing: sink seq[seq[byte]]
  ) =
    ## Adopt ``stream`` with its trust-anchor backing and point the X509
    ## capture at this record's own ``serverCertDer`` storage. ``backing`` must
    ## be moved in: the anchors point into its inner buffers, which a copy
    ## would not preserve.
    conn.trustAnchorBufs = backing
    conn.tlsStream = stream
    installX509Capture(
      conn.x509Capture, conn.tlsStream.ccontext.eng, addr conn.serverCertDer
    )

  proc finishTls*(conn: PgConnection) {.inline.} =
    ## Switch ``reader``/``writer`` to the established TLS stream.
    conn.reader = conn.tlsStream.reader
    conn.writer = conn.tlsStream.writer
    conn.sslEnabled = true

  proc graftTransportFrom*(conn, src: PgConnection) =
    ## Move the transport cluster from ``src`` and rebind the X509 capture.
    conn.transport = src.transport
    conn.baseReader = src.baseReader
    conn.baseWriter = src.baseWriter
    conn.reader = src.reader
    conn.writer = src.writer
    conn.tlsStream = src.tlsStream
    conn.trustAnchorBufs = move(src.trustAnchorBufs)
    conn.x509Capture = src.x509Capture
    conn.sslEnabled = src.sslEnabled
    conn.serverCertDer = src.serverCertDer
    if conn.tlsStream != nil:
      rebindX509Capture(
        conn.x509Capture, conn.tlsStream.ccontext.eng, addr conn.serverCertDer
      )

  proc detachTransport*(
      conn: PgConnection
  ): tuple[
    tls: TLSAsyncStream,
    baseReader: AsyncStreamReader,
    baseWriter: AsyncStreamWriter,
    transport: StreamTransport,
  ] =
    ## Detach all transport handles for the caller to close.
    result = (conn.tlsStream, conn.baseReader, conn.baseWriter, conn.transport)
    conn.tlsStream = nil
    conn.baseReader = nil
    conn.baseWriter = nil
    conn.transport = nil
    conn.reader = nil
    conn.writer = nil

elif hasAsyncDispatch:
  func socket*(conn: PgConnection): AsyncSocket {.inline.} =
    conn.socket

  proc attachTransport*(
      conn: PgConnection, s: AsyncSocket, target: DialTarget, sslHost: string
  ) {.inline.} =
    ## Attach a freshly dialled socket with what ``cancel`` needs to reach the
    ## same server: its address and TLS name.
    conn.socket = s
    conn.cancelTarget = @[target]
    conn.sslHost = sslHost

  proc graftTransportFrom*(conn, src: PgConnection) {.inline.} =
    ## Move the socket and its channel-binding certificate.
    conn.socket = src.socket
    conn.sslEnabled = src.sslEnabled
    conn.serverCertDer = src.serverCertDer

  proc detachTransport*(conn: PgConnection): AsyncSocket {.inline.} =
    ## Detach the socket for the caller to close.
    result = conn.socket
    conn.socket = nil

func serverCertDer*(conn: PgConnection): lent seq[byte] {.inline.} =
  conn.serverCertDer

proc setServerCertDer*(conn: PgConnection, der: seq[byte]) {.inline.} =
  ## Store the server certificate DER for SCRAM channel binding.
  conn.serverCertDer = der

func cancelTarget*(conn: PgConnection): lent seq[DialTarget] {.inline.} =
  conn.cancelTarget

func sslHost*(conn: PgConnection): string {.inline.} =
  conn.sslHost

func secretKey*(conn: PgConnection): int32 {.inline.} =
  conn.secretKey

proc noteBackendKeyData*(conn: PgConnection, pid, secret: int32) {.inline.} =
  ## Record ``BackendKeyData``: the pid and secret move together for ``cancel``.
  conn.pid = pid
  conn.secretKey = secret

proc graftReconnectedSession*(conn, src: PgConnection) =
  ## Move a fresh connection's wire and session identity onto ``conn`` for an
  ## in-place reconnect. The caller owns state, the statement cache, session
  ## locks and the fatal error around it.
  conn.graftTransportFrom(src)
  conn.host = src.host
  conn.port = src.port
  conn.cancelTarget = src.cancelTarget
  conn.sslHost = src.sslHost
  conn.pid = src.pid
  conn.secretKey = src.secretKey
  conn.serverParams = src.serverParams
  conn.serverParamsBytes = src.serverParamsBytes
  conn.connectTimeZone = src.connectTimeZone
  conn.timeZoneFollowsServer = src.timeZoneFollowsServer
  conn.txStatus = src.txStatus
  conn.createdAt = src.createdAt
  conn.recvBuf = src.recvBuf
  conn.recvBufStart = src.recvBufStart

func recvBuf*(conn: PgConnection): lent seq[byte] {.inline.} =
  ## The receive buffer, parsed prefix included: everything before
  ## ``recvBufStart`` is spent. Read-only; the writing side lives below.
  conn.recvBuf

func recvBufStart*(conn: PgConnection): int {.inline.} =
  ## First unparsed byte in ``recvBuf``.
  conn.recvBufStart

func recvBufLen*(conn: PgConnection): int {.inline.} =
  ## Number of unparsed bytes buffered.
  conn.recvBuf.len - conn.recvBufStart

proc consumeRecv*(conn: PgConnection, count: int) {.inline.} =
  ## Mark ``count`` unparsed bytes parsed. Callers pass what one parse
  ## consumed; nothing may be skipped without being parsed.
  conn.recvBufStart += count

proc noteWrite*(conn: PgConnection, data: openArray[byte]) {.inline.} =
  ## Book what these bytes leave the backend owing. Before the write, not
  ## after: a failed or cancelled write may still have reached the wire, and a
  ## ``CancelRequest`` at an idle backend is a harmless no-op.
  let owed = outstandingReplies(data)
  conn.pendingSyncs += owed.syncPoints
  if owed.syncPoints > 0:
    # A sync point ends every request written before it, so only what follows
    # the last one stays unended.
    conn.unsyncedWrite = owed.unsynced
  elif owed.unsynced:
    conn.unsyncedWrite = true

proc settlePendingSync*(conn: PgConnection) {.inline.} =
  ## One ``ReadyForQuery`` reply has been read, so the backend owes one reply
  ## less. Counts down rather than clearing: a pipelined batch owes several.
  if conn.pendingSyncs > 0:
    dec conn.pendingSyncs

proc clearWireDebt*(conn: PgConnection) {.inline.} =
  ## Forget which replies the previous frame was waiting for, without touching
  ## the buffers. For a frame that takes over a half-read wire — it is the
  ## reply count a ``CancelRequest`` must abort, and only the first frame to
  ## claim it may dial.
  conn.pendingSyncs = 0
  conn.unsyncedWrite = false

proc resetWireState*(conn: PgConnection) =
  ## Forget what the wire's previous life left behind: the buffered bytes on
  ## both sides and the replies the old backend owed.
  ##
  ## Sole owner of that reset: a stale count carried onto a fresh backend would
  ## dial a ``CancelRequest`` at an unrelated PID and retire a healthy
  ## connection.
  conn.recvBuf.setLen(0)
  conn.recvBufStart = 0
  conn.sendBuf.setLen(0)
  conn.pendingSyncs = 0
  conn.unsyncedWrite = false

proc noteNegotiatedProtocol*(
    conn: PgConnection, minor: int32, opts: seq[string]
) {.inline.} =
  ## Record a ``NegotiateProtocolVersion`` reply; the two fields arrive together.
  conn.negotiatedMinorVersion = minor
  conn.unrecognizedStartupOptions = opts

proc notifyCallback*(conn: PgConnection): var NotifyCallback {.inline.} =
  conn.notifyCallback

proc noticeCallback*(conn: PgConnection): var NoticeCallback {.inline.} =
  conn.noticeCallback

proc listenChannels*(conn: PgConnection): var HashSet[string] {.inline.} =
  conn.listenChannels

proc borrowedByUser*(conn: PgConnection): var bool {.inline.} =
  conn.borrowedByUser

proc transportCloseFut*(conn: PgConnection): var Future[void] {.inline.} =
  conn.transportCloseFut

func listenTask*(conn: PgConnection): Future[void] {.inline.} =
  conn.listenTask

func listenStopRequested*(conn: PgConnection): bool {.inline.} =
  conn.listenStopRequested

func listenReconnecting*(conn: PgConnection): bool {.inline.} =
  conn.listenReconnecting

proc noteListenPumpStarted*(conn: PgConnection, task: Future[void]) {.inline.} =
  ## Adopt ``task`` as the pump and clear any previous stop/reconnect state.
  conn.listenStopRequested = false
  conn.listenReconnecting = false
  conn.listenTask = task

proc clearListenTask*(conn: PgConnection) {.inline.} =
  ## Forget the pump task without touching the stop flag.
  conn.listenTask = nil

proc requestListenStop*(conn: PgConnection) {.inline.} =
  conn.listenStopRequested = true

proc clearListenStop*(conn: PgConnection) {.inline.} =
  conn.listenStopRequested = false

proc setListenReconnecting*(conn: PgConnection, value: bool) {.inline.} =
  conn.listenReconnecting = value

proc closedByUser*(conn: PgConnection): var bool {.inline.} =
  conn.closedByUser

proc fatalServerError*(conn: PgConnection): var ref PgQueryError {.inline.} =
  conn.fatalServerError

proc txAbortFields*(conn: PgConnection): var seq[ErrorField] {.inline.} =
  conn.txAbortFields

proc reconnectCallback*(conn: PgConnection): var ReconnectCallback {.inline.} =
  conn.reconnectCallback

proc notifyOverflowCallback*(
    conn: PgConnection
): var NotifyOverflowCallback {.inline.} =
  conn.notifyOverflowCallback

proc listenErrorCallback*(conn: PgConnection): var ListenErrorCallback {.inline.} =
  conn.listenErrorCallback

func heldSessionLocks*(conn: PgConnection): int {.inline.} =
  conn.heldSessionLocks

func sessionLockDirty*(conn: PgConnection): bool {.inline.} =
  conn.sessionLockDirty

proc noteSessionLockAcquired*(conn: PgConnection) {.inline.} =
  ## Track one session-level acquire; the sticky flag outlives the counter.
  inc conn.heldSessionLocks
  conn.sessionLockDirty = true

proc noteSessionLockReleased*(conn: PgConnection, released: bool) {.inline.} =
  ## Track a server-confirmed release, clamping at zero for mixed raw/typed use.
  if released and conn.heldSessionLocks > 0:
    dec conn.heldSessionLocks

proc clearSessionLocks*(conn: PgConnection) {.inline.} =
  ## Forget session-lock tracking for a fresh or reset session.
  conn.heldSessionLocks = 0
  conn.sessionLockDirty = false

proc tracer*(conn: PgConnection): var PgTracer {.inline.} =
  conn.tracer

proc ownerPool*(conn: PgConnection): var PgPoolOwner {.inline.} =
  conn.ownerPool

proc borrowed*(conn: PgConnection): var bool {.inline.} =
  conn.borrowed

proc `pid=`*(conn: PgConnection, value: int32) {.inline.} =
  conn.pid = value

proc `host=`*(conn: PgConnection, value: string) {.inline.} =
  conn.host = value

proc `port=`*(conn: PgConnection, value: int) {.inline.} =
  conn.port = value

proc `createdAt=`*(conn: PgConnection, value: Moment) {.inline.} =
  conn.createdAt = value

proc `sslEnabled=`*(conn: PgConnection, value: bool) {.inline.} =
  conn.sslEnabled = value

proc `serverParams=`*(conn: PgConnection, value: Table[string, string]) {.inline.} =
  conn.serverParams = value

proc setServerParam*(conn: PgConnection, name, value: string) {.inline.} =
  ## Store one ``ParameterStatus`` value. Prefer ``recordParameterStatus``,
  ## which also maintains the bounds and ``serverParamsBytes``.
  conn.serverParams[name] = value

proc `notifyDropped=`*(conn: PgConnection, value: int) {.inline.} =
  conn.notifyDropped = value

proc `listenError=`*(conn: PgConnection, value: ref PgListenError) {.inline.} =
  conn.listenError = value

proc `txStatus=`*(conn: PgConnection, value: TransactionStatus) {.inline.} =
  conn.txStatus = value

func sendBuf*(conn: PgConnection): lent seq[byte] {.inline.} =
  ## Read-only view of the send buffer: raw sends read it, and the message
  ## builders below append to it. Nothing outside this module gets a mutable
  ## view, so no caller can splice bytes in behind the staging bookkeeping.
  conn.sendBuf

proc sendBufVar(conn: PgConnection): var seq[byte] {.inline.} =
  ## Mutable view for this module's direct-encoding macros (``sendBuf`` is
  ## read-only, and a macro cannot emit a field access into caller scope).
  conn.sendBuf

proc clearSendBuf*(conn: PgConnection) {.inline.} =
  ## Empty the send buffer without touching staged statement Closes.
  conn.sendBuf.setLen(0)

func sendBufLen*(conn: PgConnection): int {.inline.} =
  conn.sendBuf.len

proc appendCopyData*(conn: PgConnection, data: openArray[byte]) {.inline.} =
  ## Append one ``CopyData`` frame to the send buffer.
  encodeCopyData(conn.sendBuf, data)

proc appendCopyDone*(conn: PgConnection) {.inline.} =
  conn.sendBuf.addCopyDone()

# Extended Query assembly
#
# The builders append to the connection's own send buffer, so no caller needs
# a mutable view of it. The buffer moves as one unit with the staged-Close
# bookkeeping: `beginSendBuf` empties and stages, the builders append, and the
# staged names drop only once the bytes are on the wire.

proc addParse*(
    conn: PgConnection,
    stmtName: string,
    sql: string,
    paramTypeOids: openArray[int32] = [],
) {.inline.} =
  ## Append a Parse message to the send buffer.
  conn.sendBuf.addParse(stmtName, sql, paramTypeOids)

proc addParse*(
    conn: PgConnection, stmtName: string, sql: string, params: openArray[PgParam]
) {.inline.} =
  ## Append a Parse message to the send buffer, taking the parameter OIDs from
  ## ``params`` without reshaping them into a ``seq[int32]``.
  conn.sendBuf.addParse(stmtName, sql, params)

proc addBind*(
    conn: PgConnection,
    portalName: string,
    stmtName: string,
    paramFormats: openArray[int16],
    paramValues: openArray[Option[seq[byte]]],
    resultFormats: openArray[int16] = [],
) {.inline.} =
  ## Append a Bind message to the send buffer.
  conn.sendBuf.addBind(portalName, stmtName, paramFormats, paramValues, resultFormats)

proc addBind*(
    conn: PgConnection,
    portalName: string,
    stmtName: string,
    params: openArray[PgParam],
    resultFormats: openArray[int16] = [],
) {.inline.} =
  ## Append a Bind message to the send buffer, writing straight out of
  ## ``params`` instead of flattening them into values and formats.
  conn.sendBuf.addBind(portalName, stmtName, params, resultFormats)

proc addBindRaw*(
    conn: PgConnection,
    portalName: string,
    stmtName: string,
    paramFormats: openArray[int16],
    paramData: openArray[byte],
    paramRanges: openArray[tuple[off: int32, len: int32]],
    resultFormats: openArray[int16] = [],
) {.inline.} =
  ## Append a Bind message built from raw parameter bytes and ranges.
  conn.sendBuf.addBindRaw(
    portalName, stmtName, paramFormats, paramData, paramRanges, resultFormats
  )

proc addDescribe*(conn: PgConnection, kind: DescribeKind, name: string) {.inline.} =
  ## Append a Describe message to the send buffer.
  conn.sendBuf.addDescribe(kind, name)

proc addExecute*(
    conn: PgConnection, portalName: string, maxRows: int32 = 0
) {.inline.} =
  ## Append an Execute message to the send buffer.
  conn.sendBuf.addExecute(portalName, maxRows)

proc addClose*(conn: PgConnection, kind: DescribeKind, name: string) {.inline.} =
  ## Append a Close message to the send buffer.
  conn.sendBuf.addClose(kind, name)

proc addSync*(conn: PgConnection) {.inline.} =
  ## Append a Sync message to the send buffer.
  conn.sendBuf.addSync()

proc addFlush*(conn: PgConnection) {.inline.} =
  ## Append a Flush message to the send buffer.
  conn.sendBuf.addFlush()

macro addParseDirect*(
    conn: PgConnection, stmtName: string, sql: string, args: varargs[untyped]
): untyped =
  ## Connection-level form of ``pg_types/encoding.addParseDirect``: encodes the
  ## Parse straight into the send buffer, with parameter OIDs taken from the
  ## argument types. The operand rules are the encoding macro's.
  let inner = bindSym"addParseDirect"
  result = newCall(inner, newCall(bindSym"sendBufVar", conn), stmtName, sql)
  for arg in args:
    result.add(arg)

macro addBindDirect*(
    conn: PgConnection,
    portalName: string,
    stmtName: string,
    resultFormats: untyped,
    args: varargs[untyped],
): untyped =
  ## Connection-level form of ``pg_types/encoding.addBindDirect``: encodes the
  ## Bind straight into the send buffer with per-argument format codes and no
  ## intermediate ``seq[byte]``. The operand rules are the encoding macro's.
  let inner = bindSym"addBindDirect"
  result = newCall(
    inner, newCall(bindSym"sendBufVar", conn), portalName, stmtName, resultFormats
  )
  for arg in args:
    result.add(arg)

proc nextPortalName*(conn: PgConnection, prefix: string): string =
  ## Fresh portal/savepoint name; owns counter so macro scope stays sealed.
  inc conn.portalCounter
  prefix & $conn.portalCounter

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
  conn.replReportedRaw = (0'u64, 0'u64, 0'u64)

proc updateReplMaxReceivedLsn*(conn: PgConnection, received: uint64): bool =
  ## Advance max-received LSN if ``received`` is greater; return if updated.
  if received > conn.replMaxReceivedLsnRaw:
    conn.replMaxReceivedLsnRaw = received
    true
  else:
    false

proc noteReplReported*(conn: PgConnection, receive, flush, apply: uint64) =
  ## Record the positions a caller's Standby Status Update just sent. The
  ## latest report wins, so a caller may deliberately report lower.
  conn.replReportedRaw = (receive, flush, apply)

func replReported*(conn: PgConnection): tuple[receive, flush, apply: uint64] =
  ## Raw reported positions (see ``noteReplReported``).
  conn.replReportedRaw

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

func errorSeverity*(fields: seq[ErrorField]): string =
  ## Severity of an ErrorResponse: 'V' (PG 9.6+) is never localized; 'S' may be.
  result = getErrorField(fields, 'V')
  if result.len == 0:
    result = getErrorField(fields, 'S')

proc newPgQueryError*(fields: seq[ErrorField]): ref PgQueryError =
  ## Create a PgQueryError from server ErrorResponse fields.
  let sqlState = getErrorField(fields, 'C')
  let severity = errorSeverity(fields)
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

proc copyServerError*(se: ref PgQueryError): ref PgQueryError {.raises: [].} =
  ## ``se``'s own copy for one error, nil for nil. A ref shared between errors
  ## would pile every raise's stack trace onto each of them.
  if se != nil:
    result = (ref PgQueryError)(
      msg: se.msg,
      sqlState: se.sqlState,
      severity: se.severity,
      detail: se.detail,
      hint: se.hint,
      fields: se.fields,
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

template injectTraceCtx(ctx: untyped, macroName: static string) =
  # The body's `traceCtx` is a template: a routine never becomes a closure env
  # field (see `macroSym`). Like the variable it replaced, it may not take over
  # a name the scope already has.
  when declaredInScope(traceCtx):
    {.error: macroName & ": `traceCtx` is already declared in this scope".}
  template traceCtx(): untyped {.inject, used.} =
    ctx

template traceStart(tracer: PgTracer, hook, conn, data: untyped): TraceContext =
  # A `nil` conn: the hook is not connection-scoped.
  when typeof(conn) is typeof(nil):
    tracer.hook(data)
  else:
    tracer.hook(conn, data)

template traceEnd(tracer: PgTracer, hook, ctx, conn, data: untyped) =
  when typeof(conn) is typeof(nil):
    tracer.hook(ctx, data)
  else:
    tracer.hook(ctx, conn, data)

template traced(
    tracer: PgTracer,
    conn: untyped,
    startHook, endHook: untyped,
    startData: typed,
    EndDataType: typedesc,
    endDataExpr: typed,
    macroName: static string,
    body: untyped,
) {.macroSymLocals.} =
  # Shared by `withConnTracing` and `withTracing`, which passes a `nil` conn.
  var ctx: TraceContext
  if tracer != nil and tracer.startHook != nil:
    ctx = traceStart(tracer, startHook, conn, startData)
  injectTraceCtx(ctx, macroName)
  try:
    body
  except CatchableError as e:
    if tracer != nil and tracer.endHook != nil:
      traceEnd(tracer, endHook, ctx, conn, EndDataType(err: e))
    raise e
  if tracer != nil and tracer.endHook != nil:
    traceEnd(tracer, endHook, ctx, conn, endDataExpr)

template withConnTracing*(
    conn: PgConnection,
    startHook, endHook: untyped,
    startData: typed,
    EndDataType: typedesc,
    endDataExpr: typed,
    body: untyped,
) =
  ## Wrap an operation with connection-scoped tracing hooks. `body` sees the
  ## start hook's context as `traceCtx`.
  traced(
    conn.tracer, conn, startHook, endHook, startData, EndDataType, endDataExpr,
    "withConnTracing", body,
  )

template withTracing*(
    tracer: PgTracer,
    startHook, endHook: untyped,
    startData: typed,
    EndDataType: typedesc,
    endDataExpr: typed,
    body: untyped,
) =
  ## Wrap an operation with non-connection tracing hooks (connect, pool).
  ## `traceCtx` works as in `withConnTracing`.
  traced(
    tracer, nil, startHook, endHook, startData, EndDataType, endDataExpr, "withTracing",
    body,
  )

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

proc isUtf8EncodingName*(val: string): bool =
  ## Whether ``val`` is one of the server's spellings of UTF8
  ## (``pg_char_to_encoding`` ignores case and punctuation).
  var name = ""
  for c in val:
    if c.isAlphaNumeric:
      name.add(c.toLowerAscii)
  name in ["utf8", "unicode"]

proc isGucName*(key, name: string): bool =
  ## GUC names are case-insensitive.
  cmpIgnoreCase(key, name) == 0

proc namesNonIsoDateStyle*(val: string): bool =
  ## Whether a ``DateStyle`` value picks an output style other than ISO.
  ## ``DEFAULT`` counts: it is the server's, possibly non-ISO. The server
  ## takes any token starting with ``postgres`` as that style.
  for item in val.split(','):
    let token = item.strip(chars = Whitespace + {'"'}).toLowerAscii
    if token in ["sql", "german", "default"] or token.startsWith("postgres"):
      return true
  false

proc reportsIsoDateStyle*(value: string): bool =
  ## Whether a reported ``DateStyle`` has the ISO output style the text
  ## decoders parse. The server reports the style first: ``ISO, MDY``.
  value.toLowerAscii.startsWith("iso")

proc isZeroOffsetSpec(spec: string): bool =
  ## Whether a POSIX-style offset spec — ``[+-]hh[:mm[:ss]]``, a field may
  ## carry a decimal fraction — names zero: every digit is ``0``. A sign
  ## alone, an empty field, a fourth field, a stray character or a non-zero
  ## digit fails, so a DST tail cannot slip through.
  var i = 0
  if i < spec.len and spec[i] in {'+', '-'}:
    inc i
  var separators = 0
  var sawValue = false
  while i < spec.len:
    var digits = 0
    while i < spec.len and spec[i] in {'0' .. '9'}:
      if spec[i] != '0':
        return false
      inc digits
      inc i
    if i < spec.len and spec[i] == '.':
      inc i
      while i < spec.len and spec[i] in {'0' .. '9'}:
        if spec[i] != '0':
          return false
        inc digits
        inc i
    if digits == 0:
      return false
    sawValue = true
    if i < spec.len and spec[i] == ':':
      if separators >= 2:
        return false
      inc separators
      inc i
      continue
    break
  sawValue and i == spec.len

proc isUtcZoneName*(val: string): bool =
  ## Whether ``val`` names a zone fixed at UTC+0, in any case and under the
  ## spellings the server accepts and reports: the tzdata links (``UTC``,
  ## ``Etc/UTC``, ``GMT0``, ``Greenwich``, ...), POSIX forms of those with a
  ## zero offset (``UTC0``, ``GMT+00:00``, ...), a bare zero offset (``0``,
  ## ``+00``, ``0.0``) and the synthetic bracketed name of a numeric zone
  ## (``<+00>-00``). Any non-zero offset fails, so ``Etc/GMT+1``,
  ## ``GMT+00:01`` and ``<+09>-09`` are not UTC.
  const utcNames = ["utc", "uct", "universal", "zulu", "gmt", "gmt0", "greenwich"]
  const etcPrefix = "etc/"
  var name = val.toLowerAscii
  if name.len > etcPrefix.len and name.startsWith(etcPrefix):
    name = name[etcPrefix.len .. ^1]
  if name in utcNames:
    return true
  var head = ""
  var tail = ""
  var bracketed = false
  if name.len > 0 and name[0] == '<':
    bracketed = true
    var close = -1
    for i in 1 ..< name.len:
      if name[i] == '>':
        close = i
        break
    if close < 0:
      return false
    tail = name[close + 1 .. ^1]
  else:
    for i in 0 ..< name.len:
      if name[i] in {'+', '-', '.'} or name[i] in {'0' .. '9'}:
        head = name[0 ..< i]
        tail = name[i .. ^1]
        break
    if tail.len == 0:
      # No offset part: only the pure tzdata links above are UTC.
      return false
  if not bracketed and head.len > 0 and head notin utcNames:
    return false
  isZeroOffsetSpec(tail)

proc startupDateStyle*(val: string): string =
  ## The startup ``DateStyle`` for a caller's ``val`` (empty if none), which
  ## names no other output style: ISO, then any field order ``val`` sets. The
  ## server accepts a repeated ``ISO``.
  if val.strip.len == 0:
    "ISO"
  else:
    "ISO, " & val

proc recordParameterStatus*(
    conn: PgConnection, name, value: string
) {.raises: [PgProtocolError].} =
  ## Store one ``ParameterStatus`` under ``MaxServerParams`` /
  ## ``MaxServerParamsBytes``. Exceeding either cap is treated as a broken
  ## peer: the connection is closed and ``PgProtocolError`` is raised. Updates
  ## to an existing key are always admitted when the resulting byte total fits.
  let newEntryBytes = name.len + value.len
  if conn.serverParams.hasKey(name):
    let oldLen = conn.serverParams.getOrDefault(name).len
    let delta = value.len - oldLen
    if delta > 0 and conn.serverParamsBytes > MaxServerParamsBytes - delta:
      conn.markClosed()
      raise newException(
        PgProtocolError,
        "ParameterStatus: serverParams byte total would exceed maximum of " &
          $MaxServerParamsBytes,
      )
    conn.serverParamsBytes += delta
    conn.setServerParam(name, value)
  else:
    if conn.serverParams.len >= MaxServerParams:
      conn.markClosed()
      raise newException(
        PgProtocolError,
        "ParameterStatus: serverParams key count exceeds maximum of " & $MaxServerParams,
      )
    if newEntryBytes > MaxServerParamsBytes - conn.serverParamsBytes:
      conn.markClosed()
      raise newException(
        PgProtocolError,
        "ParameterStatus: serverParams byte total would exceed maximum of " &
          $MaxServerParamsBytes,
      )
    conn.setServerParam(name, value)
    conn.serverParamsBytes += newEntryBytes

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

# Statement cache
#
# The cache table, its LRU list, the name counter and the two Close queues move
# as one unit: an entry and its list node must be added or dropped together, and
# a name enters ``pendingStmtCloses`` only when the entry it belonged to is gone
# (or never landed). The operations below are the only way in; send-phase
# callers stage and drop through them, never by touching the queues.

const stmtNamePrefix* = "_sc_"
  ## Reserved for the cache's names: `prepare` rejects a name that starts with it.

proc nextStmtName*(conn: PgConnection): string =
  ## Generate the next unique prepared statement name for the statement cache.
  inc conn.stmtCounter
  stmtNamePrefix & $conn.stmtCounter

proc lookupStmtCache*(conn: PgConnection, sql: string): CachedStmt =
  ## Look up a cached prepared statement by SQL text, updating LRU order on hit.
  ## Returns ``nil`` on miss. The returned ``ref`` stays valid across later
  ## mutations.
  if conn.stmtCacheCapacity <= 0:
    return nil
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
    conn.stmtCacheLru.append(entry.lruNode)
    return entry[]
  return nil

proc evictStmtCache*(conn: PgConnection): CachedStmt =
  ## Evict the least recently used entry from the cache. Returns the evicted entry.
  let node = conn.stmtCacheLru.head
  let oldSql = node.value
  conn.stmtCacheLru.remove(node)
  result = conn.stmtCache[oldSql]
  conn.stmtCache.del(oldSql)

proc queueStmtClose*(conn: PgConnection, stmtName: string) =
  ## Queue a server-side ``Close`` for a statement the cache no longer tracks
  ## (a dropped entry, or a cache miss the server Parsed but the cache did not
  ## keep). The next Extended Query send carries it at its head.
  ##
  ## Only for names this cache Parsed: a Close for a name whose Parse failed
  ## would drop whoever else holds it.
  conn.pendingStmtCloses.add(stmtName)

proc removeStmtCache*(conn: PgConnection, sql: string) =
  ## Remove a statement from the cache by its SQL text. The caller queues the
  ## server-side ``Close`` (see ``queueStmtClose``).
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
  conn.stmtCache.del(sql)

proc invalidateStmtCache*(conn: PgConnection, sql, stmtName: string) =
  ## Drop a cache entry the server invalidated and queue its ``Close`` together.
  ## A no-op once ``sql`` no longer maps to ``stmtName``: whatever dropped that
  ## entry already queued its Close.
  let entry = conn.stmtCache.getOrDefault(sql)
  if entry == nil or entry.name != stmtName:
    return
  conn.removeStmtCache(sql)
  conn.queueStmtClose(stmtName)

proc invalidateAllStmtCache*(conn: PgConnection, sql, stmtName: string) =
  ## Drop every entry and queue its ``Close``: a hit's statement vanished with
  ## no reset tag seen, so the rest likely went with it. A ``Close`` for a name
  ## already gone is a backend no-op. A no-op once ``sql`` no longer maps to
  ## ``stmtName``, as in ``invalidateStmtCache``.
  let entry = conn.stmtCache.getOrDefault(sql)
  if entry == nil or entry.name != stmtName:
    return
  for cachedSql in conn.stmtCacheLru:
    conn.queueStmtClose(conn.stmtCache[cachedSql].name)
  conn.stmtCache.clear()
  conn.stmtCacheLru = initDoublyLinkedList[string]()

proc addStmtCache*(conn: PgConnection, sql: string, cached: CachedStmt) =
  ## Add a prepared statement to the cache with auto-computed result formats.
  ## Single-statement callers pre-evict (``evictForInsert``) so the Close
  ## rides along. A pipeline would have to Close mid-batch, where a failing op
  ## gets it skipped, so it leaves eviction to the loop below, which queues
  ## the names for the next Extended Query send.
  ##
  ## Every name handed in ends up cached or queued for ``Close``: a statement
  ## the cache cannot keep (caching turned off while it was in flight) or an
  ## entry it replaces is closed, not forgotten.
  if conn.stmtCacheCapacity <= 0:
    conn.queueStmtClose(cached.name)
    return
  let existing = conn.stmtCache.getOrDefault(sql)
  if existing != nil:
    conn.removeStmtCache(sql)
    if existing.name != cached.name:
      conn.queueStmtClose(existing.name)
  while conn.stmtCache.len >= conn.stmtCacheCapacity:
    let evicted = conn.evictStmtCache()
    conn.queueStmtClose(evicted.name)
  if cached.resultFormats.len == 0 and cached.fields.len > 0:
    cached.resultFormats = buildResultFormats(cached.fields)
    cached.colFmts = newSeq[int16](cached.fields.len)
    cached.colOids = newSeq[int32](cached.fields.len)
    for i in 0 ..< cached.fields.len:
      cached.colOids[i] = cached.fields[i].typeOid
      cached.colFmts[i] = cached.resultFormats[i]
  let node = newDoublyLinkedNode(sql)
  cached.lruNode = node
  conn.stmtCache[sql] = cached
  conn.stmtCacheLru.append(node)

proc clearStmtCache*(conn: PgConnection) =
  ## Forget every cached statement and owed ``Close``: the session no longer
  ## holds them. A miss Parsed before this settles as gone (``stmtCacheResetGen``).
  conn.stmtCache.clear()
  conn.stmtCacheLru = initDoublyLinkedList[string]()
  conn.pendingStmtCloses.setLen(0)
  conn.stagedStmtCloses.setLen(0)
  inc conn.stmtCacheResetGen

func stmtCacheResetGen*(conn: PgConnection): int {.inline.} =
  conn.stmtCacheResetGen

proc noteCommandTag*(conn: PgConnection, tag: string) =
  ## Drop the cache once the session's prepared statements are gone. A run
  ## inside a function sends no such tag; the next cache hit's 26000 catches
  ## that (see ``retryStmtCacheInvalidation``).
  if tag in ["DISCARD ALL", "DEALLOCATE ALL"]:
    conn.clearStmtCache()

proc stagePendingStmtCloses*(conn: PgConnection, buf: var seq[byte]) =
  ## Append a ``Close`` for every owed statement name to ``buf`` so they ride
  ## along with this operation's ``Sync``, moving them from the queue to
  ## ``stagedStmtCloses``.
  ##
  ## Only `sendStagedBufMsg` / `sendStagedMsg` drop them once on the wire;
  ## a re-sent ``Close`` is a backend no-op.
  ##
  ## ``buf`` must already be emptied, or the build truncates the Closes away.
  if conn.stagedStmtCloses.len > 0:
    # A previous build staged these and never sent them. Owed again, ahead of
    # anything queued since.
    conn.pendingStmtCloses = conn.stagedStmtCloses & conn.pendingStmtCloses
    conn.stagedStmtCloses.setLen(0)
  for name in conn.pendingStmtCloses:
    buf.addClose(dkStatement, name)
  conn.stagedStmtCloses = move(conn.pendingStmtCloses)
  conn.markStaged()

proc stagePendingStmtCloses*(conn: PgConnection) {.inline.} =
  ## Stage every owed statement ``Close`` into the connection's own send
  ## buffer (the assembly path; the two-argument form serves callers building
  ## a separate batch).
  conn.stagePendingStmtCloses(conn.sendBuf)

when defined(pgStateChecks):
  func onlyCloses(buf: openArray[byte]): bool =
    var i = 0
    while i < buf.len:
      if buf[i] != byte('C'):
        return false
      i += 1 + int(decodeInt32(buf, i + 1))
    true

proc stageEvictedClose*(conn: PgConnection, buf: var seq[byte], name: string) =
  ## Stage the ``Close`` for a statement the build itself evicted. Staged, not
  ## queued: the cache no longer remembers the name, and an aborted build
  ## leaves staged names owed just as the queue would.
  ##
  ## Only ahead of the build's first Parse/Bind/Describe/Execute: behind a
  ## failing one the backend skips it up to ``Sync``, yet the name is dropped
  ## once the bytes are sent.
  when defined(pgStateChecks):
    doAssert onlyCloses(buf), "eviction Close staged behind a message that can fail"
  conn.requireStaged("staging an eviction Close")
  conn.stagedStmtCloses.add name
  buf.addClose(dkStatement, name)

proc dropStagedStmtCloses*(conn: PgConnection) =
  ## Forget the names whose ``Close`` is now on the wire. Names queued since the
  ## staging are in ``pendingStmtCloses`` and untouched by this.
  conn.requireStaged("dropping the staged statement Closes")
  conn.clearStaged()
  conn.stagedStmtCloses.setLen(0)

proc beginSendBuf*(conn: PgConnection) =
  ## Start a new operation's send buffer: empty it, then stage the queued
  ## ``Close`` messages into it. The order matters: staging first would be
  ## truncated by the emptying.
  conn.sendBuf.setLen(0)
  conn.stagePendingStmtCloses(conn.sendBuf)

proc evictForInsert*(conn: PgConnection, buf: var seq[byte]) =
  ## Make room for one more cache entry, staging the evicted ``Close`` into
  ## ``buf`` (the buffer this operation assembles).
  if conn.stmtCacheCapacity <= 0 or conn.stmtCache.len < conn.stmtCacheCapacity:
    return
  let evicted = conn.evictStmtCache()
  conn.stageEvictedClose(buf, evicted.name)

proc evictForInsert*(conn: PgConnection) {.inline.} =
  ## Make room for one more cache entry, staging the evicted ``Close`` into the
  ## connection's own send buffer.
  conn.evictForInsert(conn.sendBuf)

proc stmtCachingEnabled*(conn: PgConnection): bool {.inline.} =
  ## Whether prepared statements are cached on this connection.
  conn.stmtCacheCapacity > 0

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
  ## Bounded by ``MaxServerParams`` distinct keys and ``MaxServerParamsBytes``
  ## total name+value bytes; exceeding either closes the connection.
  conn.serverParams

func serverParam*(conn: PgConnection, name: string): string =
  ## One ``ParameterStatus`` value, or ``""`` when the server never sent it.
  conn.serverParams.getOrDefault(name, "")

proc noteConnectTimeZone*(conn: PgConnection, followsServer: bool) {.inline.} =
  ## Take the reported ``TimeZone`` as the one the pool restores, and how.
  conn.connectTimeZone = conn.serverParam("TimeZone")
  conn.timeZoneFollowsServer = followsServer

func connectTimeZone*(conn: PgConnection): string {.inline.} =
  conn.connectTimeZone

func timeZoneFollowsServer*(conn: PgConnection): bool {.inline.} =
  conn.timeZoneFollowsServer

func timeZoneChanged*(conn: PgConnection): bool {.inline.} =
  ## Whether ``TimeZone`` has moved to a zone a ``DateTime`` param would read
  ## differently under. A UTC+0 spelling of the connect zone (``Etc/UTC`` for
  ## ``UTC``) shifts nothing, so it counts as unchanged.
  let zone = conn.serverParam("TimeZone")
  zone != conn.connectTimeZone and
    not (isUtcZoneName(zone) and isUtcZoneName(conn.connectTimeZone))

func notifyDropped*(conn: PgConnection): int {.inline.} =
  ## Notifications dropped by pull-API queue overflow since the last
  ## ``PgNotifyOverflowError``. Not a lifetime total: ``waitNotification``
  ## reports the count in that error and resets it to zero.
  conn.notifyDropped

func listenError*(conn: PgConnection): ref PgListenError {.inline.} =
  ## Why the listen pump died permanently, or ``nil`` while it is alive.
  conn.listenError

func notifyMaxQueue*(conn: PgConnection): int {.inline.} =
  ## Pull-API count cap; see `notifyMaxQueue=`.
  conn.notifyMaxQueue

proc `notifyMaxQueue=`*(conn: PgConnection, value: int) {.inline.} =
  ## Cap the pull-API queue by count (1024 default; <=0 = unbounded count).
  ## Overflow drops the oldest entry and fires `onNotifyOverflow`.
  conn.notifyMaxQueue = value

func notifyMaxQueueBytes*(conn: PgConnection): int {.inline.} =
  ## Pull-API byte cap; see `notifyMaxQueueBytes=`.
  conn.notifyMaxQueueBytes

proc `notifyMaxQueueBytes=`*(conn: PgConnection, value: int) {.inline.} =
  ## Cap queued ``channel.len + payload.len`` (16 MiB default; <=0 = unbounded).
  ## Overflow drops oldest; a notification larger than the cap is not queued.
  conn.notifyMaxQueueBytes = value

func listenReconnectMaxAttempts*(conn: PgConnection): int {.inline.} =
  ## Reconnect attempt budget; see `listenReconnectMaxAttempts=`.
  conn.listenReconnectMaxAttempts

proc `listenReconnectMaxAttempts=`*(conn: PgConnection, value: int) {.inline.} =
  ## Max reconnect attempts on listen-pump failure (10 default; <=0 = retry
  ## until `close`). A refusal — a wrong password, say — or a config fault
  ## ends the pump at once, whatever the budget left.
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

proc `stmtCacheCapacity=`*(conn: PgConnection, value: int) =
  ## Resize the client-side prepared-statement cache (256 default; 0 disables
  ## it). Shrinking evicts the least recently used excess at once; their
  ## server-side ``Close`` rides along with the next Extended Query operation.
  conn.stmtCacheCapacity = value
  while conn.stmtCache.len > max(value, 0):
    conn.queueStmtClose(conn.evictStmtCache().name)

func state*(conn: PgConnection): PgConnState {.inline.} =
  ## Current state (read-only; see `isConnected`).
  conn.state

func txStatus*(conn: PgConnection): TransactionStatus {.inline.} =
  ## Tx status from last ``ReadyForQuery`` (via `bindSym`). Read-only to
  ## applications; library modules update it through the internal `txStatus=`.
  conn.txStatus

# Closed-connection checks (internal)

func closedReason*(conn: PgConnection): PgClosedReason {.inline.} =
  ## Why unusable (``crClosedByUser`` outranks ``crClosed``).
  if conn.closedByUser:
    crClosedByUser
  elif conn.state == csClosed:
    crClosed
  else:
    crOpen

proc newClosedError*(
    conn: PgConnection, msg: string, parent: ref Exception = nil
): ref PgConnectionError =
  ## ``PgUnavailableError`` for a connection that died, carrying the server's
  ## FATAL ErrorResponse (if any) as ``serverError``.
  (ref PgUnavailableError)(
    msg: msg, parent: parent, serverError: copyServerError(conn.fatalServerError)
  )

proc checkNotClosed*(conn: PgConnection) {.inline.} =
  ## Reject if closed: ``PgStateError`` for deliberate ``close()``, else ``PgConnectionError``.
  case conn.closedReason
  of crOpen:
    discard
  of crClosedByUser:
    raise newException(PgStateError, closedByUserMsg)
  of crClosed:
    raise conn.newClosedError("Connection is closed")

proc raiseClosedConnection*(conn: PgConnection, msg: string) {.noreturn.} =
  ## Like ``checkNotClosed`` with a custom ``crClosed`` message.
  # On ``crClosedByUser`` `msg` moves to `parent`: `closedByUserMsg` is public
  # and matched exactly, so it cannot carry a custom message.
  if conn.closedReason == crClosedByUser:
    raise (ref PgStateError)(
      msg: closedByUserMsg, parent: newException(PgConnectionError, msg)
    )
  raise conn.newClosedError(msg)

proc raiseTransportFailure*(
    conn: PgConnection, what: string, e: ref CatchableError
) {.noreturn.} =
  ## Fold backend transport error into ``PgError`` (``closedByUser`` wins).
  if conn.closedReason == crClosedByUser:
    raise (ref PgStateError)(msg: closedByUserMsg, parent: e)
  if e of PgError:
    raise e
  raise conn.newClosedError(what & ": " & e.msg, e)

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
        conn.newClosedError("Connection is closed")
      else:
        (ref PgStateError)(msg: "Listener stopped")
    # asyncdispatch types `Future.fail`'s callback chain as raising `Exception`,
    # so catching it is what keeps this proc `raises: []`; nothing real is masked.
    try:
      conn.notifyWaiter.fail(e)
    except Exception:
      discard

# Receive buffer

proc compactRecvBuf*(conn: PgConnection) =
  ## Move the unparsed bytes to the front, freeing the space already parsed.
  ## Only safe before reading new data from the socket: it moves bytes an
  ## in-flight read still points at.
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

proc adoptRecvBuf*(conn, other: PgConnection) =
  ## Take over ``other``'s buffered bytes and read pointer. For an in-place
  ## reconnect, which grafts the fresh connection's wire state onto the old
  ## record; the two fields must move as a pair or the pointer describes the
  ## wrong buffer.
  conn.recvBuf = other.recvBuf
  conn.recvBufStart = other.recvBufStart

proc fillRecvBuf*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Read into recvBuf. ``AsyncTimeoutError``: caller handles state; other errors
  ## → ``csClosed`` + ``raiseTransportFailure``.
  # An orphaned pump can revive here after a timeout or cancellation handler
  # retired the connection; refuse a socket read on one we've given up on.
  if conn.state == csClosed:
    conn.raiseClosedConnection("fillRecvBuf: connection is closed (csClosed)")
  conn.compactRecvBuf()
  when hasChronos:
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + RecvBufSize)
    var n: int
    try:
      n =
        if timeout == ZeroDuration:
          await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize)
        else:
          await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize).wait(
            timeout
          )
    except AsyncTimeoutError as e:
      conn.recvBuf.setLen(oldLen)
      raise e
    except CancelledError as e:
      # csClosed as for any other failure: the read may have consumed bytes, so
      # the stream is no longer parseable. Only the exception type is preserved.
      conn.recvBuf.setLen(oldLen)
      conn.markClosed()
      raise e
    except CatchableError as e:
      conn.recvBuf.setLen(oldLen)
      conn.markClosed()
      conn.raiseTransportFailure("fillRecvBuf", e)
    if n == 0:
      conn.recvBuf.setLen(oldLen)
      conn.markClosed()
      conn.raiseClosedConnection("Connection closed by server")
    # An orphan read settling after csClosed must not re-extend the buffer.
    if conn.state == csClosed:
      conn.recvBuf.setLen(oldLen)
      conn.raiseClosedConnection("fillRecvBuf: connection was closed during readOnce")
    conn.recvBuf.setLen(oldLen + n)
  elif hasAsyncDispatch:
    # On timeout, `wait()` cannot cancel `recvInto` — the orphan may still write
    # into `recvBuf[oldLen..]` after we truncate. Safe because `recvMessage`, the
    # only caller that passes a timeout, marks csClosed itself before any later
    # read can be issued, and seq shrink keeps capacity.
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + RecvBufSize)
    var n: int
    try:
      n =
        if timeout == ZeroDuration:
          await conn.socket.recvInto(addr conn.recvBuf[oldLen], RecvBufSize)
        else:
          await conn.socket.recvInto(addr conn.recvBuf[oldLen], RecvBufSize).wait(
            timeout
          )
    except AsyncTimeoutError as e:
      conn.recvBuf.setLen(oldLen)
      raise e
    except CancelledError as e:
      # csClosed as for any other failure: the read may have consumed bytes, so
      # the stream is no longer parseable. Only the exception type is preserved.
      conn.recvBuf.setLen(oldLen)
      conn.markClosed()
      raise e
    except CatchableError as e:
      conn.recvBuf.setLen(oldLen)
      conn.markClosed()
      conn.raiseTransportFailure("fillRecvBuf", e)
    if n == 0:
      conn.recvBuf.setLen(oldLen)
      conn.markClosed()
      conn.raiseClosedConnection("Connection closed by server")
    # An orphan `recvInto` settling after csClosed must not re-extend the buffer.
    if conn.state == csClosed:
      conn.recvBuf.setLen(oldLen)
      conn.raiseClosedConnection("fillRecvBuf: connection was closed during recvInto")
    conn.recvBuf.setLen(oldLen + n)

when hasChronos:
  proc fillRecvBufDetached*(conn: PgConnection): Future[void] {.async.} =
    ## Read into scratch then append to ``recvBuf`` (keeps ``recvBuf`` parseable while pending); errors → ``csClosed``.
    # Entrance guard, as in ``fillRecvBuf``: no fresh read on csClosed.
    if conn.state == csClosed:
      conn.raiseClosedConnection("fillRecvBufDetached: connection is closed (csClosed)")
    if conn.replReadScratch.len < RecvBufSize:
      conn.replReadScratch.setLen(RecvBufSize)
    let n =
      try:
        await conn.reader.readOnce(addr conn.replReadScratch[0], RecvBufSize)
      except CancelledError as e:
        conn.markClosed()
        raise e
      except CatchableError as e:
        conn.markClosed()
        conn.raiseTransportFailure("fillRecvBufDetached", e)
    if n == 0:
      conn.markClosed()
      conn.raiseClosedConnection("Connection closed by server")
    # Exit guard: a read settling after the caller flipped csClosed must not
    # re-extend recvBuf.
    if conn.state == csClosed:
      conn.raiseClosedConnection(
        "fillRecvBufDetached: connection was closed during readOnce"
      )
    conn.compactRecvBuf()
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + n)
    copyMem(addr conn.recvBuf[oldLen], addr conn.replReadScratch[0], n)

# Notification queue
#
# notifyQueue, its parked waiter and the handoff slot move together: a
# completed waiter's notification is reserved in notifyHandoff (never left in
# the capped queue) and must be claimed by the waiter that owns it. The
# operations below are the only way in, so a half-delivered notification cannot
# be observed from outside.

func notifyEntryBytes(n: Notification): int64 {.inline.} =
  n.channel.len.int64 + n.payload.len.int64

proc noteNotifyDrop(conn: PgConnection, droppedNow: var int) {.inline, raises: [].} =
  if conn.notifyDropped < high(int): # saturating; reset once reported
    conn.notifyDropped = conn.notifyDropped + 1
  droppedNow.inc

proc dropOldestNotification(
    conn: PgConnection, queuedBytes: var int64, droppedNow: var int
) {.inline, raises: [].} =
  let oldest = conn.notifyQueue.popFirst()
  queuedBytes -= notifyEntryBytes(oldest)
  if queuedBytes < 0:
    queuedBytes = 0
  conn.noteNotifyDrop(droppedNow)

proc enqueueNotification*(conn: PgConnection, notif: Notification) {.raises: [].} =
  ## Enqueue under ``notifyMaxQueue`` and ``notifyMaxQueueBytes`` (either
  ## ``<=0`` = unbounded). Overflow drops oldest; oversize is not queued.
  ## Push ``onNotify`` still sees every arrival.
  # Caps queued entries only: an outstanding handoff is already claimed.
  var droppedNow = 0
  let incoming = notifyEntryBytes(notif)
  let maxN = conn.notifyMaxQueue
  let maxB = conn.notifyMaxQueueBytes
  var queuedBytes: int64 = 0
  if maxB > 0:
    for n in conn.notifyQueue:
      queuedBytes += notifyEntryBytes(n)

  # Trim a requeued overshoot before considering the arrival.
  if maxN > 0 or maxB > 0:
    while conn.notifyQueue.len > 0:
      let countOver = maxN > 0 and conn.notifyQueue.len > maxN
      let bytesOver = maxB > 0 and queuedBytes > maxB.int64
      if not countOver and not bytesOver:
        break
      conn.dropOldestNotification(queuedBytes, droppedNow)

  if maxB > 0 and incoming > maxB.int64:
    conn.noteNotifyDrop(droppedNow)
    # noteNotifyDrop always increments, so `droppedNow > 0` holds here.
    if droppedNow > 0:
      let overflow = conn.notifyOverflowCallback
      if overflow != nil:
        overflow(droppedNow)
    return

  while conn.notifyQueue.len > 0:
    let countFull = maxN > 0 and conn.notifyQueue.len >= maxN
    let bytesFull = maxB > 0 and queuedBytes + incoming > maxB.int64
    if not countFull and not bytesFull:
      break
    conn.dropOldestNotification(queuedBytes, droppedNow)

  conn.notifyQueue.addLast(notif)
  if droppedNow > 0:
    # Read once: the accessor is a call, and this path runs per NOTIFY.
    let overflow = conn.notifyOverflowCallback
    if overflow != nil:
      overflow(droppedNow)

proc requeueHandoff*(conn: PgConnection, notif: Notification) {.raises: [].} =
  ## Requeue an unconsumed handoff at the front.
  # Trims nothing, keeping the drop policy in one place: the queue may sit one
  # over the cap until the next arrival's drop-oldest reaches this entry.
  conn.notifyQueue.addFirst(notif)

proc reclaimHandoff*(conn: PgConnection) {.raises: [].} =
  ## Requeue a handoff whose waiter will never claim it, so an abandoned frame
  ## cannot make the notification unreachable.
  if conn.hasNotifyHandoff:
    conn.hasNotifyHandoff = false
    conn.requeueHandoff(move conn.notifyHandoff)

func hasNotification*(conn: PgConnection): bool {.inline.} =
  ## Whether the pull-API queue holds a notification (handoff slot excluded).
  conn.notifyQueue.len > 0

proc popNotification*(conn: PgConnection): Notification {.inline.} =
  ## Take the oldest queued notification; caller checks ``hasNotification``.
  conn.notifyQueue.popFirst()

func hasNotifyWaiter*(conn: PgConnection): bool {.inline.} =
  ## Whether a ``waitNotification`` is parked.
  conn.notifyWaiter != nil

proc registerNotifyWaiter*(conn: PgConnection, waiter: Future[void]) =
  ## Park the single allowed waiter. A second would never be woken, so it is
  ## rejected here rather than left to hang.
  if conn.notifyWaiter != nil:
    raise newException(PgStateError, "Another waitNotification is already active")
  conn.notifyWaiter = waiter

proc unregisterNotifyWaiter*(conn: PgConnection, waiter: Future[void]): bool =
  ## Drop ``waiter``'s registration if it is still the parked one. False means
  ## a later waiter owns the slot now; only the owner may claim the handoff.
  if conn.notifyWaiter == waiter:
    conn.notifyWaiter = nil
    true
  else:
    false

proc takeNotifyHandoff*(conn: PgConnection): Option[Notification] {.inline.} =
  ## Claim the reserved handoff, if any. Only the registered waiter may call
  ## this; the handoff is returned directly and never re-enters the capped
  ## queue.
  if conn.hasNotifyHandoff:
    conn.hasNotifyHandoff = false
    some(move conn.notifyHandoff)
  else:
    none(Notification)

proc reclaimStaleNotifyWaiter*(conn: PgConnection) =
  ## Drop a finished waiter's registration and put back its unclaimed handoff.
  # A waiter that never resumes would otherwise strand the notification in
  # ``notifyHandoff`` and block every later waiter.
  if conn.notifyWaiter != nil and conn.notifyWaiter.finished:
    conn.notifyWaiter = nil
    conn.reclaimHandoff()

proc dispatchNotification*(conn: PgConnection, msg: BackendMessage) {.raises: [].} =
  let notif = Notification(
    pid: msg.notifPid, channel: msg.notifChannel, payload: msg.notifPayload
  )
  # Handed directly to an unresumed waiter: parking it in the shared queue
  # instead would make it the first thing the overflow drop discards.
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    conn.notifyHandoff = notif
    conn.hasNotifyHandoff = true
    # asyncdispatch's `Future.complete` has inferred effect `Exception`
    # via the callback chain; swallow it to keep this proc `raises: []`.
    try:
      conn.notifyWaiter.complete()
    except Exception:
      # The waiter will never resume, so nothing would ever move the handoff
      # back: queue it here instead of losing it.
      conn.hasNotifyHandoff = false
      conn.notifyHandoff = Notification()
      conn.enqueueNotification(notif)
  else:
    conn.enqueueNotification(notif)
  let notifyCb = conn.notifyCallback
  if notifyCb != nil:
    notifyCb(notif)

proc dispatchNotice*(conn: PgConnection, msg: BackendMessage) {.raises: [].} =
  let noticeCb = conn.noticeCallback
  if noticeCb != nil:
    noticeCb(Notice(fields: msg.noticeFields))

# Replication write queue
#
# ``replWrites``, the drain task, the stop's final status and CopyDone, and the
# stream flags are one cluster: what is still queued decides whether the
# library may report a position at all, and the final status may only be
# promoted once nothing can be queued behind it. ``pg_replication`` owns the
# drain loop (it needs the wire); everything the queue means lives here.

proc replClosedError*(conn: PgConnection, msg = "Connection is closed"): ref PgError =
  ## ``raiseClosedConnection``'s error, or nil while open, keeping the
  ## replication write that killed the connection as the cause.
  case conn.closedReason
  of crOpen:
    nil
  of crClosedByUser:
    (ref PgStateError)(
      msg: closedByUserMsg,
      parent: newException(PgConnectionError, msg, conn.replWriteFailure),
    )
  of crClosed:
    conn.newClosedError(msg, conn.replWriteFailure)

proc replWriteError*(conn: PgConnection): ref CatchableError =
  ## A fresh error per waiter of a write that did not make it: one raised
  ## exception cannot be shared between futures.
  result = conn.replClosedError()
  if result == nil:
    result = newException(
      PgStateError, "the replication stream ended before this write",
      conn.replWriteFailure,
    )

proc settleReplWrite*(conn: PgConnection, w: ReplWrite, ok: bool) =
  w.state = if ok: rwWritten else: rwFailed
  for fut in w.waiters:
    if fut.finished: # the caller cancelled its wait
      continue
    if ok:
      fut.complete()
    else:
      fut.fail(conn.replWriteError())
  w.waiters.setLen(0)

proc waitReplWrite*(w: ReplWrite): Future[void] =
  result = newFuture[void]("replWrite")
  w.waiters.add(result)

proc failQueuedReplWrites*(conn: PgConnection) =
  ## Fail every write not yet started. The one being written settles itself.
  conn.replPendingStatus = nil
  while conn.replWrites.len > 0:
    conn.settleReplWrite(conn.replWrites.popFirst(), ok = false)
  for w in [conn.replFinalStatus, conn.replCopyDone]:
    if w != nil and w.state == rwQueued:
      conn.settleReplWrite(w, ok = false)

proc closeReplWrites*(conn: PgConnection) =
  ## The stream is ending: accept no more writes and fail those not started.
  conn.replWritesOpen = false
  conn.failQueuedReplWrites()

proc nextReplWrite*(conn: PgConnection): ReplWrite =
  ## The queue in order, then the stop's final status and CopyDone, so nothing
  ## queued before the final status is encoded can land after CopyDone. Taking
  ## the library's status off the queue retires its pending mark, so the next
  ## one queued is a fresh entry.
  if conn.replWrites.len > 0:
    result = conn.replWrites.popFirst()
    if result == conn.replPendingStatus:
      conn.replPendingStatus = nil
    return
  for w in [conn.replFinalStatus, conn.replCopyDone]:
    if w != nil and w.state == rwQueued:
      if w == conn.replFinalStatus and conn.replInCallback:
        # The running callback may still confirm a position for it.
        return nil
      return w

proc replQueueWrite*(conn: PgConnection, w: ReplWrite) {.inline.} =
  ## Append a caller's write to the drain queue, in call order.
  conn.replWrites.addLast(w)

proc tailPendingReplStatus*(conn: PgConnection): ReplWrite =
  ## The library's status still queued at the tail, or nil. It is encoded when
  ## written, so it already carries anything newer and can stand for another.
  let pending = conn.replPendingStatus
  if pending != nil and conn.replWrites.peekLast == pending:
    return pending

proc queueReplStatus*(conn: PgConnection): ReplWrite =
  ## Queue the library's status, or share the one at the tail.
  result = conn.tailPendingReplStatus()
  if result != nil:
    return
  result = ReplWrite()
  conn.replWrites.addLast(result)
  conn.replPendingStatus = result

proc replCanStillReport*(conn: PgConnection): bool =
  ## Whether a position recorded now still reaches the server: always before
  ## the client's stop, then only until the stop's final status is encoded,
  ## which waits for a running callback to return.
  conn.replWritesOpen and
    (conn.replCopyDone == nil or conn.replFinalStatus.state == rwQueued)

proc confirmReplReportable*(conn: PgConnection, lsn: uint64): bool =
  ## ``confirmReplFlushed`` while the position can still reach the server.
  conn.replCanStillReport() and conn.confirmReplFlushed(lsn)

proc replQueueStop*(conn: PgConnection): ReplWrite =
  ## Queue the client's end of the stream — a last status, then CopyDone — and
  ## return the CopyDone future's write. Once queued, a later stop waits on
  ## that same CopyDone and shares its outcome.
  let pending = conn.tailPendingReplStatus()
  if pending != nil:
    # A library status still queued at the tail becomes the final one.
    discard conn.replWrites.popLast()
    conn.replPendingStatus = nil
    conn.replFinalStatus = pending
  else:
    conn.replFinalStatus = ReplWrite()
  result = ReplWrite(frame: @copyDoneMsg)
  conn.replCopyDone = result

proc replResetStream*(conn: PgConnection, autoConfirm: bool, serverFlush: uint64) =
  ## Reset the write cluster for a new stream, so a reused connection never
  ## inherits a final status or a failure from the previous one.
  conn.replFinalStatus = nil
  conn.replWriteFailure = nil
  conn.replPendingStatus = nil
  conn.replCopyDone = nil
  conn.replWritesOpen = true
  conn.replAutoConfirm = autoConfirm
  conn.replInTxn = false
  conn.replInCallback = false
  # What the server already holds, not the stream's start: autoConfirm reports
  # anything above it, a start ahead of the slot included.
  conn.replSentFlushRaw = serverFlush

proc replEnterTxn*(conn: PgConnection) {.inline.} =
  ## ``autoConfirm``: a pgoutput ``Begin`` was seen; a commit now waits for the
  ## callback to process it.
  conn.replInTxn = true

proc replExitTxn*(conn: PgConnection) {.inline.} =
  ## ``autoConfirm``: the commit is processed, so positions may be reported
  ## again.
  conn.replInTxn = false

proc setReplInCallback*(conn: PgConnection, value: bool) {.inline.} =
  ## Mark the stream's callback as running or returned; the stop's final status
  ## waits for a true one to clear.
  conn.replInCallback = value

func replAutoConfirm*(conn: PgConnection): bool {.inline.} =
  ## Whether the logical stream confirms progress itself.
  conn.replAutoConfirm

func replInTxn*(conn: PgConnection): bool {.inline.} =
  ## ``autoConfirm``: between a pgoutput Begin and Commit.
  conn.replInTxn

func replInCallback*(conn: PgConnection): bool {.inline.} =
  ## Whether the stream's callback is running.
  conn.replInCallback

func replWritesOpen*(conn: PgConnection): bool {.inline.} =
  ## Whether the stream still accepts writes.
  conn.replWritesOpen

func replCopyDoneQueued*(conn: PgConnection): bool {.inline.} =
  ## Whether the client's CopyDone has been queued.
  conn.replCopyDone != nil

func replCopyDone*(conn: PgConnection): ReplWrite {.inline.} =
  ## The queued CopyDone write, or nil. Read-only: ``replQueueStop`` creates it.
  conn.replCopyDone

func replPendingStatusQueued*(conn: PgConnection): bool {.inline.} =
  ## Whether a library status is queued and not yet encoded.
  conn.replPendingStatus != nil

func replStopStatusQueued*(conn: PgConnection): bool {.inline.} =
  ## Whether the stop's final status is still waiting to be encoded.
  conn.replFinalStatus != nil and conn.replFinalStatus.state == rwQueued and
    conn.replWritesOpen

func replWriteFailure*(conn: PgConnection): ref CatchableError {.inline.} =
  ## The write failure that ended the stream, kept as the cause of later
  ## errors.
  conn.replWriteFailure

proc noteReplWriteFailure*(conn: PgConnection, e: ref CatchableError) {.inline.} =
  ## Latch the write failure that ended the stream.
  conn.replWriteFailure = e

func replSentFlush*(conn: PgConnection): uint64 {.inline.} =
  ## Flush position of the library's last Standby Status Update (raw).
  conn.replSentFlushRaw

proc noteReplSentFlush*(conn: PgConnection, flush: uint64) {.inline.} =
  ## Record the flush position the library's status is about to send.
  conn.replSentFlushRaw = flush

func replFlusher*(conn: PgConnection): Future[void] {.inline.} =
  ## The task draining ``replWrites``, nil when idle. Read-only; a new drain
  ## starts through ``replFlusherIdle`` + ``setReplFlusher`` so two tasks never
  ## share the queue.
  conn.replFlusher

func replFlusherIdle*(conn: PgConnection): bool {.inline.} =
  ## Whether a drain task may be started.
  conn.replFlusher == nil or conn.replFlusher.finished

proc setReplFlusher*(conn: PgConnection, fut: Future[void]) {.inline.} =
  ## Adopt ``fut`` as the drain task; only valid right after
  ## ``replFlusherIdle`` returned true.
  conn.replFlusher = fut
