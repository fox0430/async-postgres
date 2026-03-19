# async_postgres

Async PostgreSQL client in Nim.

## Features

### Protocol & Connection
- PostgreSQL wire protocol v3
- Simple Query and Extended Query Protocol
- Pipeline mode — batch multiple operations in a single network round trip
- Connection pooling with health checks and maintenance
- SSL/TLS support (disable, prefer, require, verify-ca, verify-full)
- MD5 and SCRAM-SHA-256 authentication
- DSN connection string parsing

### Queries & Statements
- Prepared statements with server-side statement cache (LRU eviction)
- Server-side cursors (streaming row chunks)
- Transactions (`withTransaction`)
- COPY IN / COPY OUT (buffered and streaming)
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

## Basic Usage

```nim
import pkg/async_postgres

proc main() {.async.} =
  let conn = await connect(parseDsn("postgresql://myuser:mypass@127.0.0.1:5432/mydb"))
  defer: await conn.close()

  # Insert and get affected row count
  let affected = await conn.execAffected(
    "INSERT INTO users (name, age) VALUES ($1, $2)",
    pgParams("Alice", 30'i32),
  )
  echo "Inserted: ", affected

  # Query multiple rows
  let result = await conn.query(
    "SELECT id, name, age FROM users WHERE age > $1",
    pgParams(25'i32),
  )
  for row in result.rows:
    echo row.getStr(1), " age=", row.getInt(2)

  # Query a single value
  let count = await conn.queryValue(
    "SELECT count(*) FROM users", default = "0",
  )
  echo "Total users: ", count

waitFor main()
```

## Async Backend

By default, asyncdispatch is used. To use chronos:

```sh
# asyncdispatch (default)
nim c -r your_app.nim

# chronos
nim c -r -d:asyncBackend=chronos your_app.nim
```

**chronos is recommended.** chronos supports proper future cancellation, which enables reliable timeout handling and clean connection teardown. asyncdispatch lacks real cancellation — timed-out futures continue running in the background, and `cancelAndWait` is a no-op.

SSL backend differs by async backend:
- **chronos**: BearSSL (via nim-bearssl)
- **asyncdispatch**: OpenSSL (requires `-d:ssl`)

## Documents

https://fox0430.github.io/async-postgres/async_postgres.html

## License

MIT
