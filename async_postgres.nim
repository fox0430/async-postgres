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
##     let conn = await connect(parseDsn("postgresql://user:pass@localhost:5432/mydb"))
##     defer: await conn.close()
##
##     # Execute with typed parameters
##     discard await conn.exec(
##       "INSERT INTO users (name, age) VALUES ($1, $2)",
##       @[toPgParam("Alice"), toPgParam(30'i32)],
##     )
##
##     # Query rows
##     let result = await conn.query(
##       "SELECT id, name, age FROM users WHERE age > $1",
##       @[toPgParam(25'i32)],
##     )
##     for row in result:
##       echo row.getStr(1), " age=", row.getInt(2)
##
##   waitFor main()
##
## Modules
## =======
## - `pg_connection <async_postgres/pg_connection.html>`_ — Connection management, DSN parsing, SSL, LISTEN/NOTIFY
## - `pg_client <async_postgres/pg_client.html>`_ — Query execution, prepared statements, cursors, pipelines, transactions, COPY
## - `pg_pool <async_postgres/pg_pool.html>`_ — Connection pooling with health checks and maintenance
## - `pg_pool_cluster <async_postgres/pg_pool_cluster.html>`_ — Read replica pool cluster with automatic query routing
## - `pg_types <async_postgres/pg_types.html>`_ — Type conversions (``toPgParam``, row accessors, arrays, ranges, composites, enums)
## - `pg_protocol <async_postgres/pg_protocol.html>`_ — Wire protocol encoding/decoding
## - `pg_auth <async_postgres/pg_auth.html>`_ — MD5 and SCRAM-SHA-256 authentication
## - `async_backend <async_postgres/async_backend.html>`_ — Async framework abstraction (asyncdispatch / chronos)

import
  async_postgres/[
    async_backend, pg_protocol, pg_auth, pg_types, pg_connection, pg_client, pg_pool,
    pg_pool_cluster,
  ]

export
  async_backend, pg_protocol, pg_auth, pg_types, pg_connection, pg_client, pg_pool,
  pg_pool_cluster
