# async_postgres

Async PostgreSQL client in Nim.

## Features

### Protocol & Connection
- PostgreSQL wire protocol v3
- Simple Query and Extended Query Protocol
- Pipeline mode — batch multiple operations in a single network round trip
- Connection pooling with health checks and maintenance
- Pool cluster with read replica routing
- SSL/TLS support (disable, prefer, require, verify-ca, verify-full)
- MD5 and SCRAM-SHA-256 authentication
- DSN connection string parsing

### Queries & Statements
- `sql` macro — compile-time `{expr}` placeholder extraction with automatic parameterization
- `?`-style placeholders (`sqlParams`) for db_connector compatibility
- Prepared statements with server-side statement cache (LRU eviction)
- Server-side cursors (streaming row chunks)
- Transactions (`withTransaction`)
- COPY IN / COPY OUT (buffered and streaming)
- Large Object API (streaming binary data)
- LISTEN/NOTIFY with auto-reconnect

### Types
- Typed parameters (`pgParams` / `toPgParam`) and row accessors (`getStr`, `getInt`, ...)
- Array types with binary format support
- Range and multirange types (`int4range`, `tsrange`, `daterange`, ...)
- Composite types (user-defined row types via `pgComposite` macro)
- Enum types (user-defined enums via `pgEnum` macro)
- Network types (`inet`, `cidr`, `macaddr`, `macaddr8`)
- Interval type (`PgInterval`)

### Performance
- Automatic binary format selection for known-safe types
- Compile-time binary format lookup tables
- Zero-copy binary array decoding
- Optimized protocol encoding with minimized allocations

### Platform
- Async backend: [asyncdispatch](https://nim-lang.org/docs/asyncdispatch.html) (default) or [chronos](https://github.com/status-im/nim-chronos)

## Requirements

- Nim >= 2.2.0

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

## Documentation

https://fox0430.github.io/async-postgres/async_postgres.html

## License

MIT
