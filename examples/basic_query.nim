## Basic query example.
##
## Demonstrates connecting to PostgreSQL, creating a table, inserting rows,
## and querying data using the extended query protocol.
##
## Usage:
##   nim c -r examples/basic_query.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  # Create a temporary table
  discard await conn.exec(
    """
    CREATE TEMP TABLE users (
      id serial PRIMARY KEY,
      name text NOT NULL,
      age int4 NOT NULL
    )
  """
  )

  # Insert rows using the sql macro (compile-time parameter binding)
  let names = ["Alice", "Bob", "Charlie"]
  let ages = [30'i32, 25, 35]
  for i in 0 ..< names.len:
    let name = names[i]
    let age = ages[i]
    discard await conn.exec(sql"INSERT INTO users (name, age) VALUES ({name}, {age})")

  # Query all rows
  let res = await conn.query("SELECT id, name, age FROM users ORDER BY id")
  echo "All users:"
  for row in res.rows:
    echo "  id=",
      row.getInt("id"), " name=", row.getStr("name"), " age=", row.getInt("age")

  # Query with a parameter
  let minAge = 28'i32
  let older = await conn.query(sql"SELECT name, age FROM users WHERE age >= {minAge}")
  echo "\nUsers with age >= ", minAge, ":"
  for row in older.rows:
    echo "  ", row.getStr("name"), " (", row.getInt("age"), ")"

  # Query a single value
  let count = await conn.queryValue(int64, "SELECT count(*) FROM users")
  echo "\nTotal users: ", count

waitFor main()
