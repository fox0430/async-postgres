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
  let connConfig = ConnConfig(
    host: "127.0.0.1",
    port: 15432,
    user: "test",
    password: "test",
    database: "test",
    sslMode: sslDisable,
  )

  let primaryConfig = PoolConfig(connConfig: connConfig, minSize: 1, maxSize: 3)
  let replicaConfig = PoolConfig(connConfig: connConfig, minSize: 1, maxSize: 3)

  # fallbackPrimary: if replica is unavailable, fall back to primary for reads
  let cluster =
    await newPoolCluster(primaryConfig, replicaConfig, fallback = fallbackPrimary)
  defer:
    await cluster.close()

  # Write operations go to the primary
  discard await cluster.writeExec(
    "CREATE TABLE IF NOT EXISTS messages (id serial PRIMARY KEY, body text NOT NULL)"
  )
  discard await cluster.writeExec("TRUNCATE messages")
  discard
    await cluster.writeExec("INSERT INTO messages (body) VALUES ('hello'), ('world')")

  # Read operations go to the replica (or primary as fallback)
  let res = await cluster.readQuery("SELECT id, body FROM messages ORDER BY id")
  echo "Messages:"
  for row in res.rows:
    echo "  id=", row.getInt("id"), " body=", row.getStr("body")

  # Using withWriteConnection / withReadConnection for multi-statement operations
  cluster.withWriteConnection(conn):
    discard
      await conn.exec("INSERT INTO messages (body) VALUES ('from withWriteConnection')")

  cluster.withReadConnection(readConn):
    let count = await readConn.queryValue(int64, "SELECT count(*) FROM messages")
    echo "\nTotal messages: ", count

  # Cleanup
  discard await cluster.writeExec("DROP TABLE messages")

waitFor main()
