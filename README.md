# async_postgres

Async PostgreSQL client in Nim.

## Features

- PostgreSQL wire protocol v3
- Simple Query and Extended Query Protocol
- Prepared statements
- Server-side cursors (streaming row chunks)
- Connection pooling with health checks and maintenance
- LISTEN/NOTIFY with auto-reconnect
- COPY IN / COPY OUT (buffered and streaming)
- SSL/TLS support (disable, prefer, require, verify-ca, verify-full)
- MD5 and SCRAM-SHA-256 authentication
- DSN connection string parsing
- Typed parameters (`toPgParam`) and row accessors (`getStr`, `getInt`, ...)
- Text and binary format support
- Transactions (`withTransaction`)
- Async backend: [asyncdispatch](https://nim-lang.org/docs/asyncdispatch.html) (default) or [chronos](https://github.com/status-im/nim-chronos)

## Requirements

- Nim >= 2.2.0

## Basic Usage

```nim
import async_postgres

proc main() {.async.} =
  let conn = await connect(parseDsn("postgresql://myuser:mypass@127.0.0.1:5432/mydb"))
  defer: await conn.close()

  # Insert and get affected row count
  let affected = await conn.execAffected(
    "INSERT INTO users (name, age) VALUES ($1, $2)",
    @[toPgParam("Alice"), toPgParam(30'i32)],
  )
  echo "Inserted: ", affected

  # Query multiple rows
  let result = await conn.query(
    "SELECT id, name, age FROM users WHERE age > $1",
    @[toPgParam(25'i32)],
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

SSL backend differs by async backend:
- **chronos**: BearSSL (via nim-bearssl)
- **asyncdispatch**: OpenSSL (requires `-d:ssl`)

## Documents

https://fox0430.github.io/async-postgres/async_postgres.html

## License

MIT
