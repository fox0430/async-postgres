## PostgreSQL connection lifecycle, I/O, simple-query protocol, statement
## cache and LISTEN/NOTIFY support.
##
## This module is a thin re-export hub. The actual implementation lives in
## the `pg_connection/` subdirectory:
##
## - `pg_connection/types`        — `PgConnection`, `ConnConfig`, the
##                                  tracing data types, the `PgTracer`
##                                  hook record, and the tracing helper
##                                  templates (`withConnTracing`,
##                                  `withTracing`).
## - `pg_connection/dsn`          — DSN parsing (URI and libpq
##                                  keyword=value formats) plus
##                                  `initConnConfig` and `parseDsn`.
## - `pg_connection/buffer_io`    — recv/send buffering (`fillRecvBuf`,
##                                  `nextMessage`, `recvMessage`,
##                                  `sendMsg`), TCP keepalive,
##                                  `closeTransport`, notification/notice
##                                  dispatch, `isConnected` /
##                                  `socketHasFin`, and the `getHosts`
##                                  host helper.
## - `pg_connection/ssl`          — SSL negotiation (`negotiateSSL`) for
##                                  chronos+BearSSL and asyncdispatch+OpenSSL.
## - `pg_connection/cache`        — client-side prepared-statement LRU.
## - `pg_connection/simple_query` — simple-query / simple-exec / ping,
##                                  `cancel` / `invalidateOnTimeout`,
##                                  `checkSessionAttrs`, `quoteIdentifier`,
##                                  `QueryResult` helpers.
## - `pg_connection/lifecycle`    — `connect` / `connectToHost` / `close`,
##                                  `orderedHosts` (load-balanced host
##                                  ordering) and the SCRAM/require_auth
##                                  helpers.
## - `pg_connection/notify`       — LISTEN/NOTIFY pump, `waitNotification`,
##                                  `reconnectInPlace`.
## - `pg_connection/type_lookup`  — `lookupTypeOids` generic helper to
##                                  resolve type names to OIDs via
##                                  `to_regtype` (extension types like
##                                  `hstore`, `citext`, etc.).
##
## Only the public API listed below is re-exported. Anything not listed stays
## in its defining submodule (e.g. `pg_connection/buffer_io`) and must be
## imported from there directly, not through `import pg_connection`.

import pg_errors
import
  pg_connection/[types, dsn, buffer_io, simple_query, lifecycle, notify, type_lookup]

export pg_errors

# `types` — public types, the tracer hook data types and the tracing helpers.
export types.PgConnState
export types.SslMode
export types.SslNegotiation
export types.ChannelBindingMode
export types.AuthMethod
export types.TargetSessionAttrs
export types.LoadBalanceHosts
export types.HostEntry
export types.ConnConfig
export types.PgTracer
export types.Notification
export types.NotifyCallback
export types.Notice
export types.NoticeCallback
export types.CachedStmt
export types.dialAddr
export types.displayHost
export types.QueryResult
export types.CopyResult
export types.CopyOutInfo
export types.CopyInInfo
export types.CopyOutCallback
export types.CopyInCallback
export types.PgPoolOwner
export types.PgConnection
export types.RowCallback
export types.ClientCertPairingErrorMsg
export types.TraceContext
export types.TraceCopyDirection
export types.TraceConnectStartData
export types.TraceConnectEndData
export types.TraceQueryStartData
export types.TraceQueryEndData
export types.TracePrepareStartData
export types.TracePrepareEndData
export types.TracePipelineStartData
export types.TracePipelineEndData
export types.TraceCopyStartData
export types.TraceCopyEndData
export types.TracePoolAcquireStartData
export types.TracePoolAcquireEndData
export types.TracePoolReleaseStartData
export types.TracePoolReleaseEndData
export types.TracePoolDoubleReleaseData
export types.TracePoolCloseErrorData
export types.TraceTransportCloseErrorData
export types.TransportCloseStage
export types.CleanupKind
export types.CleanupSkipReason
export types.TraceCleanupSkippedData
export types.TraceLeakedSessionLocksData
export types.TraceInsecureAuthData
export types.TraceDeprecatedAuthData
export types.TraceAdvisoryUnlockFailedData
export types.withConnTracing
export types.withTracing

# `dsn` — the documented DSN entry points.
export dsn.initConnConfig
export dsn.parseDsn

# `buffer_io` — public connection I/O and keepalive surface.
export buffer_io.isUnixSocket
export buffer_io.unixSocketPath
export buffer_io.getHosts
export buffer_io.makeCopyOutCallback
export buffer_io.makeCopyInCallback
export buffer_io.socketHasFin
export buffer_io.socketHasPendingData
export buffer_io.isConnected

# `simple_query` — the simple-query protocol and query-result helpers.
export simple_query.len
export simple_query.columnIndex
export simple_query.rows
export simple_query.items
export simple_query.quoteIdentifier
export simple_query.cancel
export simple_query.cancelNoWait
export simple_query.invalidateOnTimeout
export simple_query.simpleExec
export simple_query.simpleQuery
export simple_query.ping
export simple_query.checkSessionAttrs

# `lifecycle` — connect / close and host ordering.
export lifecycle.close
export lifecycle.orderedHosts
export lifecycle.connect
export lifecycle.connectToHost

# `notify` — LISTEN / NOTIFY pump and waiters.
export notify.onNotify
export notify.onListenError
export notify.listen
export notify.unlisten
export notify.waitNotification

# `type_lookup` — extension type OID resolution.
export type_lookup.TypeOidInfo
export type_lookup.lookupTypeOids
