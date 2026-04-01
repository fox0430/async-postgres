## Pipeline example.
##
## Demonstrates batching multiple queries and commands into a single
## round trip using the pipeline API for improved performance.
##
## Usage:
##   nim c -r examples/pipeline.nim

import std/strutils
import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  discard await conn.exec(
    """
    CREATE TEMP TABLE tasks (
      id serial PRIMARY KEY,
      title text NOT NULL,
      done bool NOT NULL DEFAULT false
    )
  """
  )

  # Build a pipeline: multiple operations sent in a single round trip
  let p = conn.newPipeline()

  p.addExec("INSERT INTO tasks (title) VALUES ($1)", @[toPgParam("Write docs")])
  p.addExec("INSERT INTO tasks (title) VALUES ($1)", @[toPgParam("Fix bug")])
  p.addExec(
    "INSERT INTO tasks (title, done) VALUES ($1, $2)",
    @[toPgParam("Ship v1"), toPgParam(true)],
  )
  p.addQuery("SELECT id, title, done FROM tasks ORDER BY id")
  p.addQuery("SELECT count(*) FROM tasks WHERE done = true")

  # Execute all operations at once
  let results = await p.execute()

  for i, r in results:
    case r.kind
    of prkExec:
      echo "Operation ", i, ": ", r.commandResult.commandTag
    of prkQuery:
      echo "Operation ", i, " (", r.queryResult.rowCount, " rows):"
      for row in r.queryResult.rows:
        var cols: seq[string]
        for j in 0 ..< r.queryResult.fields.len:
          cols.add(r.queryResult.fields[j].name & "=" & row.getStr(j))
        echo "  ", cols.join(", ")

waitFor main()
