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
## accepted by ``query``/``exec`` and ``simpleExec``; on timeout the
## connection is marked closed because the wire protocol desynchronises.
## ``queryDirect``/``execDirect`` and ``simpleQuery`` currently do not accept
## a timeout.
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

export
  async_backend, pg_protocol, pg_auth, pg_types, pg_connection, pg_client, pg_pool,
  pg_pool_cluster, pg_largeobject, pg_advisory_lock, pg_sql, pg_replication
