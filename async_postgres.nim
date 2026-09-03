## Async PostgreSQL client for Nim.
##
## Implements the PostgreSQL wire protocol v3 with full support for the
## extended query protocol, connection pooling, SSL/TLS, and binary format
## optimization.
##
## Async Backend
## =============
## Select at compile time with ``-d:asyncBackend=asyncdispatch`` (default) or
## ``-d:asyncBackend=chronos``.
##
## Quick Start
## ===========
##
## .. code-block:: nim
##   import pkg/async_postgres
##
##   proc main() {.async.} =
##     let conn = await connect("postgresql://myuser:mypass@127.0.0.1:5432/mydb")
##     defer: await conn.close()
##
##     # Insert with typed parameters
##     let name = "Alice"
##     let age = 30'i32
##     let cr = await conn.exec(sql"INSERT INTO users (name, age) VALUES ({name}, {age})")
##     echo "Inserted: ", cr.affectedRows
##
##     # Query multiple rows
##     let minAge = 25'i32
##     let row = await conn.query(sql"SELECT id, name, age FROM users WHERE age > {minAge}")
##     for r in row:
##       echo r.getStr("name"), " age=", r.getInt("age")
##
##     # Query a single value
##     let count = await conn.queryValueOrDefault("SELECT count(*) FROM users", default = "0")
##     echo "Total users: ", count
##
##   waitFor main()
##
## Choosing a query API
## ====================
## Four parameterised entry points are provided. They differ in how parameters
## are supplied and in the amount of per-call allocation:
##
## 1. `sql"..."` macro — compile-time ``{expr}`` interpolation
## ----------------------------------------------------------------------
## Most readable. ``{expr}`` placeholders are rewritten to ``$1, $2, …`` at
## compile time and the expressions are collected into a ``seq[PgParam]``.
## Works with `query`, `exec`, `pool.query`, `cluster.withReadConnection`, etc.
##
## .. code-block:: nim
##   let name = "Alice"
##   await conn.query(sql"SELECT id FROM users WHERE name = {name}")
##
## - Pros: concise; compile-time placeholder rewriting; SQL injection-safe.
## - Cons: still allocates a ``seq[PgParam]`` per call; ``{expr}`` must be a
##   compile-time-visible expression (not an ``openArray`` spread).
## - Use when: SQL is a literal and ergonomics matter more than zero-alloc.
##
## 2. `query`/`exec` with an explicit ``seq[PgParam]``
## ----------------------------------------------------------------------
## The baseline runtime API. Use when the SQL or the parameter list is
## constructed dynamically (e.g. conditional ``WHERE`` clauses).
##
## .. code-block:: nim
##   var params: seq[PgParam]
##   params.add name.toPgParam
##   params.add age.toPgParam
##   await conn.query("SELECT id FROM users WHERE name = $1 AND age > $2", params)
##
## The ``pgParams(a, b, c)`` macro builds a ``seq[PgParam]`` in one call. A
## second overload takes ``seq[PgParamInline]`` — constructed manually as
## ``@[toPgParamInline(a), toPgParamInline(b)]`` — which avoids per-parameter
## heap allocations for scalar types.
##
## 3. `queryDirect`/`execDirect` — zero-allocation macros
## ----------------------------------------------------------------------
## Encodes parameters directly into the connection's send buffer at compile
## time; no intermediate ``seq[PgParam]`` or ``seq[byte]`` is built.
##
## .. code-block:: nim
##   let qr = await conn.queryDirect("SELECT name FROM users WHERE id = $1", myId)
##
## - Pros: no per-call allocations for the parameter path; same statement
##   cache semantics as `query`.
## - Cons: SQL must be a string literal/compile-time constant; arguments are
##   positional (``$1, $2, …``), no ``{expr}`` sugar.
## - Use when: the call site is on a hot path and params are scalars.
##
## 4. `simpleQuery`/`simpleExec` — simple query protocol
## ----------------------------------------------------------------------
## Parameter-less, text-only, single round trip. Allows multiple
## ``;``-separated statements and session-only commands that the extended
## protocol rejects (``SET``, ``LISTEN``, ``VACUUM``, …).
##
## .. code-block:: nim
##   discard await conn.simpleExec("SET search_path TO myschema, public")
##
## Quick decision table
## --------------------
## =========================  ===================================================
## Situation                   Prefer
## =========================  ===================================================
## Literal SQL, readability    ``sql"..."`` macro
## Dynamic SQL or params       ``query(sql, params)`` / ``exec``
## Hot path, scalar params     ``queryDirect`` / ``execDirect``
## ``SET`` / multi-statement   ``simpleQuery`` / ``simpleExec``
## =========================  ===================================================
##
## ``sql"..."``, ``query``/``exec``, and ``queryDirect``/``execDirect`` share
## the per-connection prepared-statement cache; ``simpleQuery``/``simpleExec``
## use the simple protocol and are not cached. A ``timeout`` parameter is
## accepted by ``query``/``exec``, ``queryDirect``/``execDirect``, and
## ``simpleQuery``/``simpleExec``; on timeout the connection is marked
## closed because the wire protocol desynchronises.
##
## Modules
## =======
## - `pg_connection <async_postgres/pg_connection.html>`_ — Connection management, DSN parsing, SSL, LISTEN/NOTIFY
## - `pg_client <async_postgres/pg_client.html>`_ — Query execution, prepared statements, cursors, pipelines, transactions, COPY, zero-alloc macros (``queryDirect``/``execDirect``)
## - `pg_pool <async_postgres/pg_pool.html>`_ — Connection pooling with health checks and maintenance
## - `pg_pool_cluster <async_postgres/pg_pool_cluster.html>`_ — Read replica pool cluster with automatic query routing
## - `pg_types <async_postgres/pg_types.html>`_ — Type conversions (``toPgParam``, row accessors, arrays, ranges, composites, enums)
## - `pg_protocol <async_postgres/pg_protocol.html>`_ — Wire protocol encoding/decoding
## - `pg_auth <async_postgres/pg_auth.html>`_ — MD5 and SCRAM-SHA-256 authentication
## - `pg_largeobject <async_postgres/pg_largeobject.html>`_ — Large Object API for streaming binary data
## - `pg_advisory_lock <async_postgres/pg_advisory_lock.html>`_ — Advisory lock API (session/transaction, exclusive/shared)
## - `pg_replication <async_postgres/pg_replication.html>`_ — Logical replication streaming with pgoutput decoder
## - `async_backend <async_postgres/async_backend.html>`_ — Async framework abstraction (asyncdispatch / chronos)

import
  async_postgres/[
    async_backend, pg_protocol, pg_auth, pg_types, pg_connection, pg_client, pg_pool,
    pg_pool_cluster, pg_largeobject, pg_advisory_lock, pg_sql, pg_replication,
  ]

# `pg_types`/`pg_connection`/`pg_client` whitelist themselves; the other
# modules expose only their public API surface.
export pg_types, pg_connection, pg_client
export pg_pool_cluster, pg_largeobject, pg_advisory_lock, pg_sql, pg_replication
export pg_auth

# `pg_pool` — public pool API (internal gauges/helpers stay in the module).
export pg_pool.PoolConfig
export pg_pool.PoolMetrics
export pg_pool.PooledConnHandle
export pg_pool.PgPool
export pg_pool.initPoolConfig
export pg_pool.idleCount
export pg_pool.activeCount
export pg_pool.size
export pg_pool.isClosed
export pg_pool.metrics
export pg_pool.resetSession
export pg_pool.newPool
export pg_pool.release
export pg_pool.resetSessionAndRelease
export pg_pool.acquire
export pg_pool.runAndRelease
export pg_pool.withConnection
export pg_pool.exec
export pg_pool.query
export pg_pool.queryEach
export pg_pool.queryRow
export pg_pool.queryRowOpt
export pg_pool.queryValue
export pg_pool.queryValueOpt
export pg_pool.queryValueOrDefault
export pg_pool.queryExists
export pg_pool.queryColumn
export pg_pool.simpleQuery
export pg_pool.simpleExec
export pg_pool.execInTransaction
export pg_pool.queryInTransaction
export pg_pool.notify
export pg_pool.withTransaction
export pg_pool.withTransactionRetry
export pg_pool.withTransactionDeadline
export pg_pool.withTransactionRetryDeadline
export pg_pool.withPipeline
export pg_pool.close

# `async_backend` is exported wholesale; it also re-exports the selected
# backend (asyncdispatch / chronos), supplying `async`, `waitFor`, etc.
export async_backend

# `pg_protocol` — the wire protocol codec and entry points. The inbound
# decoders and leaf encoders stay internal; the send-buffer helpers are
# re-exported because the `addParseDirect`/`addBindDirect` macros and advanced
# call sites resolve them in the caller's scope.
export pg_protocol.FrontendMessageKind
export pg_protocol.BackendMessageKind
export pg_protocol.DescribeKind
export pg_protocol.TransactionStatus
export pg_protocol.FieldDescription
export pg_protocol.CopyFormat
export pg_protocol.BackendMessage
export pg_protocol.ParseState
export pg_protocol.ParseResult
export pg_protocol.RowData
export pg_protocol.Row
export pg_protocol.syncMsg
export pg_protocol.flushMsg
export pg_protocol.copyDoneMsg
export pg_protocol.BinarySafeOids
export pg_protocol.maxInt16Count
export pg_protocol.maxInt32Len
export pg_protocol.DefaultMaxBackendMessageLen
export pg_protocol.MaxNegotiateProtocolOptions
export pg_protocol.MaxErrorOrNoticeFields
export pg_protocol.MaxSaslMechanisms
export pg_protocol.initRow
export pg_protocol.data
export pg_protocol.rowIdx
export pg_protocol.isBinarySafeOid
export pg_protocol.addInt16
export pg_protocol.addInt32
export pg_protocol.addCount16
export pg_protocol.addLen32
export pg_protocol.addCString
export pg_protocol.patchMsgLen
export pg_protocol.patchMsgLenAtomic
export pg_protocol.preflightParseDirect
export pg_protocol.preflightBindDirect
export pg_protocol.encodeStartup
export pg_protocol.encodeSSLRequest
export pg_protocol.encodePassword
export pg_protocol.encodeSASLInitialResponse
export pg_protocol.encodeSASLResponse
export pg_protocol.encodeQuery
export pg_protocol.addParse
export pg_protocol.addBind
export pg_protocol.addBindRaw
export pg_protocol.addDescribe
export pg_protocol.addExecute
export pg_protocol.addClose
export pg_protocol.addSync
export pg_protocol.addFlush
export pg_protocol.addCopyDone
export pg_protocol.encodeParse
export pg_protocol.encodeBind
export pg_protocol.encodeDescribe
export pg_protocol.encodeExecute
export pg_protocol.encodeClose
export pg_protocol.encodeSync
export pg_protocol.encodeFlush
export pg_protocol.encodeTerminate
export pg_protocol.encodeCancelRequest
export pg_protocol.encodeCopyData
export pg_protocol.encodeCopyDone
export pg_protocol.encodeCopyFail
export pg_protocol.newRowData
export pg_protocol.reuseRowData
export pg_protocol.clone
export pg_protocol.buildResultFormats
export pg_protocol.parseDataRowInto
export pg_protocol.parseBackendMessage
export pg_protocol.formatError
export pg_protocol.addCopyBinaryHeader
export pg_protocol.addCopyBinaryTrailer
export pg_protocol.addCopyTupleStart
export pg_protocol.addCopyFieldNull
export pg_protocol.addCopyFieldInt16
export pg_protocol.addCopyFieldInt32
export pg_protocol.addCopyFieldInt64
export pg_protocol.addCopyFieldFloat64
export pg_protocol.addCopyFieldFloat32
export pg_protocol.addCopyFieldBool
export pg_protocol.addCopyFieldText
export pg_protocol.addCopyFieldString
export pg_protocol.encodeStandbyStatusUpdate
