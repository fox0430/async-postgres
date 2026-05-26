## Pool cluster example.
##
## Demonstrates read/write splitting using a pool cluster with
## primary and replica configurations. In this example, both pools
## point to the same server for simplicity.
##
## Usage:
##   nim c -r examples/pool_cluster.nim

import pkg/async_postgres

proc main() {.async.} =
  let connConfig = initConnConfig(
    host = "127.0.0.1",
    port = 15432,
    user = "test",
    password = "test",
    database = "test",
    sslMode = sslDisable,
  )

  let primaryConfig = initPoolConfig(connConfig, minSize = 1, maxSize = 3)
  let replicaConfig = initPoolConfig(connConfig, minSize = 1, maxSize = 3)

  # fallbackPrimary: if replica is unavailable, fall back to primary for reads
  let cluster =
    await newPoolCluster(primaryConfig, replicaConfig, fallback = fallbackPrimary)
  defer:
    await cluster.close()

  # Write operations go to the primary. `withWriteConnection` is the safest
  # default: it auto-releases and runs `resetSession` afterwards.
  cluster.withWriteConnection(conn):
    discard await conn.exec(
      "CREATE TABLE IF NOT EXISTS messages (id serial PRIMARY KEY, body text NOT NULL)"
    )
    discard await conn.exec("TRUNCATE messages")
    discard await conn.exec("INSERT INTO messages (body) VALUES ('hello'), ('world')")

  # Read operations go to the replica (or primary as fallback).
  cluster.withReadConnection(conn):
    let res = await conn.query("SELECT id, body FROM messages ORDER BY id")
    echo "Messages:"
    for row in res.rows:
      echo "  id=", row.getInt("id"), " body=", row.getStr("body")

    let count = await conn.queryValue(int64, "SELECT count(*) FROM messages")
    echo "\nTotal messages: ", count

  # Handle pattern: use `readConnection` / `writeConnection` when the borrowed
  # connection must outlive a single lexical scope — e.g. selected dynamically,
  # stored on a struct, or threaded through multiple helpers. The caller must
  # `defer: h.release()`, and `resetSession` is NOT run automatically.
  block:
    let useReplica = true
    let h =
      if useReplica:
        await cluster.readConnection()
      else:
        await cluster.writeConnection()
    defer:
      h.release()
    let body = await h.conn.queryValue(string, "SELECT body FROM messages LIMIT 1")
    echo "\nDynamic selection: ", body

  # Cleanup
  cluster.withWriteConnection(conn):
    discard await conn.exec("DROP TABLE messages")

waitFor main()
