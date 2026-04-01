## Prepared statement example.
##
## Demonstrates creating a server-side prepared statement, executing it
## multiple times with different parameters, and closing it.
##
## Usage:
##   nim c -r examples/prepared_statement.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  # Create a temporary table
  discard await conn.exec(
    """
    CREATE TEMP TABLE products (
      id serial PRIMARY KEY,
      name text NOT NULL,
      price int4 NOT NULL
    )
  """
  )

  # Prepare a named statement for inserting rows
  let insertStmt = await conn.prepare(
    "insert_product", "INSERT INTO products (name, price) VALUES ($1, $2)"
  )

  # Execute the prepared statement multiple times with different parameters
  discard await insertStmt.execute(@[toPgParam("Apple"), toPgParam(120'i32)])
  discard await insertStmt.execute(@[toPgParam("Banana"), toPgParam(200'i32)])
  discard await insertStmt.execute(@[toPgParam("Cherry"), toPgParam(350'i32)])

  await insertStmt.close()

  # Prepare a query statement with a parameter
  let selectStmt = await conn.prepare(
    "select_by_price", "SELECT name, price FROM products WHERE price >= $1"
  )

  let res = await selectStmt.execute(@[toPgParam(150'i32)])
  echo "Products with price >= 150:"
  for row in res.rows:
    echo "  ", row.getStr("name"), ": ", row.getInt("price")

  await selectStmt.close()

waitFor main()
