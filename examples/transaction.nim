## Transaction example.
##
## Demonstrates explicit transaction control using the withTransaction macro,
## including automatic rollback on exceptions and custom transaction options.
##
## Usage:
##   nim c -r examples/transaction.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  discard await conn.exec(
    """
    CREATE TEMP TABLE accounts (
      name text PRIMARY KEY,
      balance int4 NOT NULL
    )
  """
  )

  # Insert initial data
  discard await conn.exec("INSERT INTO accounts VALUES ('Alice', 1000), ('Bob', 500)")

  # Successful transaction: transfer funds
  conn.withTransaction:
    discard await conn.exec(
      sql"UPDATE accounts SET balance = balance - {200'i32} WHERE name = 'Alice'"
    )
    discard await conn.exec(
      sql"UPDATE accounts SET balance = balance + {200'i32} WHERE name = 'Bob'"
    )

  echo "After transfer:"
  let res = await conn.query("SELECT name, balance FROM accounts ORDER BY name")
  for row in res.rows:
    echo "  ", row.getStr("name"), ": ", row.getInt("balance")

  # Failed transaction: automatically rolled back on exception
  try:
    conn.withTransaction:
      discard await conn.exec(
        sql"UPDATE accounts SET balance = balance - {9999'i32} WHERE name = 'Alice'"
      )
      raise newException(CatchableError, "something went wrong")
  except CatchableError:
    echo "\nTransaction rolled back due to error"

  echo "Balances unchanged:"
  let res2 = await conn.query("SELECT name, balance FROM accounts ORDER BY name")
  for row in res2.rows:
    echo "  ", row.getStr("name"), ": ", row.getInt("balance")

  # Transaction with custom options (serializable, read-only)
  conn.withTransaction(
    TransactionOptions(isolation: ilSerializable, access: amReadOnly)
  ):
    let total = await conn.queryValue(int64, "SELECT sum(balance) FROM accounts")
    echo "\nTotal balance (read-only tx): ", total

waitFor main()
