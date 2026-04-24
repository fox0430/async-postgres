## Query variants example.
##
## Demonstrates query convenience procs beyond the basic `query` / `exec` /
## `queryValue` entry points: optional-result accessors, existence checks,
## row callbacks, and the simple-protocol variants for session-level commands
## or multi-statement batches.
##
## Usage:
##   nim c -r examples/query_variants.nim

import std/options

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  # simpleExec: session-level command via the simple query protocol.
  # Appropriate for SET / VACUUM / single-shot DDL where the extended
  # protocol's Parse/Bind round trip and plan cache entry would be wasted.
  discard await conn.simpleExec("SET TIME ZONE 'UTC'")

  discard await conn.exec(
    """
    CREATE TEMP TABLE employees (
      id serial PRIMARY KEY,
      name text NOT NULL,
      team text,
      salary int4
    )
  """
  )
  discard await conn.exec(
    """
    INSERT INTO employees (name, team, salary) VALUES
      ('Alice',   'platform', 800),
      ('Bob',     'platform', 650),
      ('Charlie', 'data',     NULL),
      ('Dave',    NULL,        700)
  """
  )

  # queryExists: boolean "does any row match" without fetching data.
  let dataTeam = "data"
  let hasData =
    await conn.queryExists(sql"SELECT 1 FROM employees WHERE team = {dataTeam}")
  echo "Has data team? ", hasData

  # queryValueOrDefault: return a fallback when the row or value is NULL.
  # Here Charlie has no salary, so the default kicks in.
  let charlieName = "Charlie"
  let charlieSalary = await conn.queryValueOrDefault(
    int32, sql"SELECT salary FROM employees WHERE name = {charlieName}", default = 0'i32
  )
  echo "Charlie's salary (0 = unknown): ", charlieSalary

  # queryValueOpt: distinguish "no row / NULL" from a real value.
  let daveName = "Dave"
  let maybeTeam =
    await conn.queryValueOpt(sql"SELECT team FROM employees WHERE name = {daveName}")
  if maybeTeam.isSome:
    echo "Dave's team: ", maybeTeam.get
  else:
    echo "Dave has no team"

  # queryRowOpt: single-row lookup that tolerates a missing row.
  let nobody = "Nobody"
  let missing = await conn.queryRowOpt(
    sql"SELECT name, salary FROM employees WHERE name = {nobody}"
  )
  echo "Missing lookup isSome: ", missing.isSome

  # queryColumn: collect one column across all rows as `seq[string]`.
  let names = await conn.queryColumn("SELECT name FROM employees ORDER BY id")
  echo "Names: ", names

  # queryEach: stream rows through a callback without building a QueryResult.
  # The Row is only valid during the callback — copy out what you need.
  var totalSalary = 0'i64
  let rowCb: RowCallback = proc(row: Row) =
    if not row.isNull("salary"):
      totalSalary += row.getInt("salary")
  let processed = await conn.queryEach("SELECT salary FROM employees", callback = rowCb)
  echo "Processed ", processed, " rows, total salary: ", totalSalary

  # simpleQuery: multiple `;`-separated statements in one round trip.
  # Returns one QueryResult per statement. Handy for read-only introspection
  # batches; not usable for parameterised queries.
  let multi = await conn.simpleQuery(
    "SELECT count(*) FROM employees; SELECT count(DISTINCT team) FROM employees"
  )
  echo "Total employees: ", multi[0].rows[0].getStr(0)
  echo "Distinct teams:  ", multi[1].rows[0].getStr(0)

waitFor main()
