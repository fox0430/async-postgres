## Connection pool example.
##
## Demonstrates using a connection pool with concurrent queries
## and transactions.
##
## Usage:
##   nim c -r examples/pool.nim

import std/options
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

  let pool = await newPool(PoolConfig(connConfig: connConfig, minSize: 2, maxSize: 5))
  defer:
    await pool.close()

  # Setup
  discard await pool.exec(
    "CREATE TABLE IF NOT EXISTS products (id serial PRIMARY KEY, name text NOT NULL, price int4 NOT NULL)"
  )
  discard await pool.exec("TRUNCATE products")

  # Insert within a transaction (no params needed for literal values)
  discard await pool.execInTransaction(
    "INSERT INTO products (name, price) VALUES ('Apple', 120), ('Banana', 200), ('Cherry', 350)"
  )

  # Query directly through the pool
  let res = await pool.query("SELECT name, price FROM products ORDER BY price")
  echo "Products:"
  for row in res.rows:
    echo "  ", row.getStr("name"), ": ", row.getInt("price"), " yen"

  # Run concurrent queries using withConnection
  proc countExpensive(): Future[int64] {.async.} =
    pool.withConnection(conn):
      return await conn.queryValue(
        int64, "SELECT count(*) FROM products WHERE price > $1", @[toPgParam(150'i32)]
      )

  proc cheapest(): Future[string] {.async.} =
    pool.withConnection(conn):
      let row = await conn.queryOne("SELECT name FROM products ORDER BY price LIMIT 1")
      return options.get(row).getStr("name")

  # Launch concurrently, then await each result
  let expCountFut = countExpensive()
  let cheapNameFut = cheapest()
  echo "\nExpensive items (>150): ", await expCountFut
  echo "Cheapest item: ", await cheapNameFut

  echo "\nPool stats: idle=", pool.idleCount, " active=", pool.activeCount

  # Cleanup
  discard await pool.exec("DROP TABLE products")

waitFor main()
