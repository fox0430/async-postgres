## Zero-allocation query macros.
##
## Demonstrates `queryDirect` and `execDirect`, which encode parameters into
## the connection's send buffer at compile time. They avoid the intermediate
## `seq[PgParam]` and per-parameter `seq[byte]` allocations that the regular
## `query` / `exec` path incurs, making them suitable for hot paths where
## SQL is a literal and parameters are scalars.
##
## Constraints (vs. regular `query` / `exec`):
## - SQL must be a string literal or compile-time constant.
## - Arguments are positional (`$1, $2, …`) — no `{expr}` sugar.
## - No `timeout` parameter.
## - Same per-connection statement cache as `query` / `exec`; the statement
##   is parsed once server-side and rebound on subsequent calls.
##
## Usage:
##   nim c -r examples/query_direct.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  discard await conn.exec(
    """
    CREATE TEMP TABLE metrics (
      id serial PRIMARY KEY,
      host text NOT NULL,
      cpu float8 NOT NULL,
      ts int8 NOT NULL
    )
  """
  )

  # execDirect: zero-alloc INSERT in a tight loop. The SQL literal is parsed
  # once (server-side plan cached), and each call re-binds scalars directly
  # into the send buffer without heap allocations for the parameter list.
  let host = "worker-1"
  for i in 0 ..< 5:
    let cpu = 0.1 * float64(i)
    let ts = int64(1_700_000_000 + i)
    discard await conn.execDirect(
      "INSERT INTO metrics (host, cpu, ts) VALUES ($1, $2, $3)", host, cpu, ts
    )

  # queryDirect: zero-alloc read. Returns the same QueryResult shape as `query`.
  let threshold = 0.2'f64
  let qr = await conn.queryDirect(
    "SELECT host, cpu, ts FROM metrics WHERE cpu >= $1 ORDER BY ts", threshold
  )
  echo "Rows above ", threshold, ":"
  for row in qr.rows:
    echo "  host=",
      row.getStr("host"), " cpu=", row.getStr("cpu"), " ts=", row.getInt("ts")

  # queryDirect drives `queryValue` the same way: wrap it manually since
  # `queryValue[T]` takes a regular `seq[PgParam]`. For a single scalar,
  # `queryDirect` + `rows[0].get(0, T)` is zero-alloc end-to-end.
  let targetHost = "worker-1"
  let count = await conn.queryDirect(
    "SELECT count(*)::int8 FROM metrics WHERE host = $1", targetHost
  )
  echo "Rows for ", targetHost, ": ", count.rows[0].get(0, int64)

waitFor main()
