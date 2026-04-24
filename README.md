# async_postgres

Async PostgreSQL client in Nim.

## Features

### Protocol & Connection
- PostgreSQL wire protocol v3
- Simple Query and Extended Query Protocol
- Pipeline mode — batch multiple operations in a single network round trip
- Connection pooling with health checks and maintenance (broken connections discarded on acquire/release)
- Pool cluster with read replica routing
- SSL/TLS support (disable, allow, prefer, require, verify-ca, verify-full)
- MD5, SCRAM-SHA-256 and SCRAM-SHA-256-PLUS authentication
- `channel_binding` policy (disable, prefer, require) to harden SCRAM against downgrade
- DSN connection string parsing
- Unix socket connection
- Multi-host failover
- Target session attributes (any, read-write, read-only, primary, standby, prefer-standby)

### Queries & Statements
- `sql` macro — compile-time `{expr}` placeholder extraction with automatic parameterization
- `?`-style placeholders (`sqlParams`) for db_connector compatibility
- Prepared statements with server-side statement cache (LRU eviction)
- Server-side cursors (streaming row chunks)
- Transactions (`withTransaction`)
- COPY IN / COPY OUT (buffered and streaming)
- Large Object API (streaming binary data)
- LISTEN/NOTIFY with auto-reconnect
- Advisory locks (session/transaction, exclusive/shared)
- Logical replication with pgoutput decoder

### Types
- Typed parameters (`pgParams` / `toPgParam`) and row accessors (`getStr`, `getInt`, ...)
- UUID type (`PgUuid`)
- Numeric type (`PgNumeric` — arbitrary-precision decimal)
- JSON/JSONB (`JsonNode`)
- Array types with binary format support
- Range and multirange types (`int4range`, `tsrange`, `daterange`, ...)
- Composite types (user-defined row types via `pgComposite` macro)
- Enum types (user-defined enums via `pgEnum` macro)
- Domain types (user-defined domains via `pgDomain` macro)
- Network types (`inet`, `cidr`, `macaddr`, `macaddr8`)
- Geometric types (`point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`)
- Text search types (`tsvector`, `tsquery`)
- Bit string types (`bit`, `varbit`)
- Interval type (`PgInterval`)
- hstore type (`PgHstore`)
- XML type (`PgXml`)

### Performance
- Automatic binary format selection for known-safe types
- Compile-time binary format lookup tables
- Zero-copy binary array decoding
- Optimized protocol encoding with minimized allocations

### Observability
- Tracing hooks for connection, query, prepared statement, pipeline, COPY, and pool operations

### Platform
- Async backend: [asyncdispatch](https://nim-lang.org/docs/asyncdispatch.html) (default) or [chronos](https://github.com/status-im/nim-chronos)

## Requirements

- Nim >= 2.2.4

## Installation

```sh
nimble install async_postgres
```

## Basic Usage

```nim
import pkg/async_postgres

proc main() {.async.} =
  let conn = await connect("postgresql://myuser:mypass@127.0.0.1:5432/mydb")
  defer: await conn.close()

  # Insert with typed parameters
  let name = "Alice"
  let age = 30'i32
  let cr = await conn.exec(sql"INSERT INTO users (name, age) VALUES ({name}, {age})")
  echo "Inserted: ", cr.affectedRows

  # Query multiple rows
  let minAge = 25'i32
  let row = await conn.query(sql"SELECT id, name, age FROM users WHERE age > {minAge}")
  for r in row:
    echo r.getStr("name"), " age=", r.getInt("age")

  # Query a single value
  let count = await conn.queryValueOrDefault("SELECT count(*) FROM users", default = "0")
  echo "Total users: ", count

waitFor main()
```

## Reconnection Policy

- **Connection pool (`PgPool` / `PgPoolCluster`)** — broken connections are detected and discarded automatically. On `acquire`, entries whose state is not `csReady` (or that fail the optional `ping` health check) are retired and replaced. On `release`, connections left in a non-ready or in-transaction state are also closed rather than returned to the idle queue. Configure `healthCheckTimeout` / `pingTimeout` to tune idle-connection probing.
- **Direct `PgConnection`** — no automatic reconnection for regular queries. Per-query retry would be unsafe for non-idempotent statements and in-flight transactions, so a closed connection must be replaced by calling `connect(...)` again (or by using the pool). Inspect `conn.isConnected()` or `conn.state` to decide whether a handle is still usable.
- **LISTEN/NOTIFY** — this is the one exception. The listen pump reconnects with exponential backoff (up to 10 attempts, 30 s cap) and re-subscribes to all channels. Register a `reconnectCallback` if you need to resynchronise application state after a reconnect.

## Async Backend

By default, asyncdispatch is used. To use chronos:

```sh
# asyncdispatch (default)
nim c your_app.nim
# asyncdispatch with ssl
nim c -d:ssl your_app.nim

# chronos
nim c -d:asyncBackend=chronos your_app.nim
```

**chronos is recommended.** chronos supports proper future cancellation, which enables reliable timeout handling and clean connection teardown. asyncdispatch lacks real cancellation — timed-out futures continue running in the background, and `cancelAndWait` is a no-op.

SSL backend differs by async backend:
- asyncdispatch: OpenSSL (requires `-d:ssl`)
- chronos: BearSSL (via [nim-bearssl](https://github.com/status-im/nim-bearssl))

## Examples

The [examples](examples/) directory contains runnable samples:

- [basic_query](examples/basic_query.nim) — Connect, insert, and query rows
- [query_variants](examples/query_variants.nim) — `queryExists` / `queryValueOrDefault` / `queryValueOpt` / `queryRowOpt` / `queryColumn` / `queryEach` / `simpleExec` / `simpleQuery`
- [query_direct](examples/query_direct.nim) — Zero-allocation `queryDirect` / `execDirect` macros for hot paths
- [prepared_statement](examples/prepared_statement.nim) — Server-side prepared statements
- [transaction](examples/transaction.nim) — Transaction control with rollback and isolation levels
- [cursor](examples/cursor.nim) — Server-side cursors for streaming large result sets
- [pipeline](examples/pipeline.nim) — Batch multiple operations in a single round trip
- [copy](examples/copy.nim) — Bulk import/export with COPY protocol
- [large_object](examples/large_object.nim) — Large Object API for binary data
- [listen_notify](examples/listen_notify.nim) — LISTEN/NOTIFY
- [pool](examples/pool.nim) — Connection pooling
- [pool_cluster](examples/pool_cluster.nim) — Read/write splitting with pool cluster
- [advisory_lock](examples/advisory_lock.nim) — Application-level distributed locking
- [replication](examples/replication.nim) — Logical replication with pgoutput

## Documentation

https://fox0430.github.io/async-postgres/async_postgres.html

## License

MIT
