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
## Every public symbol previously defined in this file is re-exported from
## here, so existing `import async_postgres/pg_connection` (or the
## bundled `import pkg/async_postgres`) call sites keep working without
## changes. Test files that previously used `import pg_connection
## {.all.}` to reach private helpers must now import the specific
## submodule directly, e.g.
## `import pg_connection/buffer_io {.all.}`.

import pg_errors
import
  pg_connection/
    [types, dsn, buffer_io, ssl, cache, simple_query, lifecycle, notify, type_lookup]

export pg_errors
export types, dsn, buffer_io, ssl, cache, simple_query, lifecycle, notify, type_lookup
