## Server-side cursor example.
##
## Demonstrates streaming large result sets using server-side cursors
## with chunk-based fetching.
##
## Usage:
##   nim c -r examples/cursor.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  discard await conn.exec(
    """
    CREATE TEMP TABLE numbers (
      id serial PRIMARY KEY,
      value int4 NOT NULL
    )
  """
  )

  # Insert sample data
  for i in 1'i32 .. 25:
    discard await conn.exec(sql"INSERT INTO numbers (value) VALUES ({i})")

  # Open a cursor with a chunk size of 10 rows
  let cursor =
    await conn.openCursor("SELECT id, value FROM numbers ORDER BY id", chunkSize = 10)

  # Fetch rows in chunks until exhausted
  var total = 0
  while not cursor.exhausted:
    let rows = await cursor.fetchNext()
    echo "Fetched ", rows.len, " rows:"
    for row in rows:
      echo "  id=", row.getInt("id"), " value=", row.getInt("value")
    total += rows.len

  echo "\nTotal rows fetched: ", total

  await cursor.close()

waitFor main()
