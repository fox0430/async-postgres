import std/[unittest, options, strutils, math, importutils, net]

import
  ../async_postgres/[async_backend, pg_protocol, pg_types, pg_client, pg_connection]

import e2e_common

privateAccess(PgConnection)

suite "E2E: Simple Query Protocol":
  test "SELECT 1":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("SELECT 1 AS num")
      doAssert results.len == 1
      doAssert results[0].fields.len == 1
      doAssert results[0].fields[0].name == "num"
      doAssert results[0].rows.len == 1
      doAssert results[0].rows[0][0].get().toString() == "1"
      await conn.close()

    waitFor t()

  test "multiple rows with generate_series":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("SELECT generate_series(1,3)")
      doAssert results.len == 1
      doAssert results[0].rows.len == 3
      doAssert results[0].rows[0][0].get().toString() == "1"
      doAssert results[0].rows[1][0].get().toString() == "2"
      doAssert results[0].rows[2][0].get().toString() == "3"
      await conn.close()

    waitFor t()

  test "empty query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("")
      doAssert results.len == 1
      doAssert results[0].fields.len == 0
      doAssert results[0].rows.len == 0
      doAssert results[0].commandTag == ""
      await conn.close()

    waitFor t()

  test "invalid SQL raises PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.simpleQuery("INVALID SQL STATEMENT")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "invalid SQL raises PgQueryError with SQLSTATE":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sqlState = ""
      var severity = ""
      try:
        discard await conn.simpleQuery("SELECT FROM nonexistent_table_xyz")
      except PgQueryError as e:
        sqlState = e.sqlState
        severity = e.severity
      doAssert sqlState.len == 5, "expected 5-char SQLSTATE, got: " & sqlState
      doAssert severity == "ERROR"
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

suite "E2E: Extended Query Protocol":
  test "exec CREATE TABLE and INSERT":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_e2e")
      let createTag = await conn.exec(
        "CREATE TABLE test_e2e (id serial PRIMARY KEY, name text NOT NULL)"
      )
      doAssert "CREATE TABLE" in createTag
      let insertTag = await conn.exec(
        "INSERT INTO test_e2e (name) VALUES ($1)", @[toPgParam("alice")]
      )
      doAssert "INSERT" in insertTag
      discard await conn.exec("DROP TABLE test_e2e")
      await conn.close()

    waitFor t()

  test "query with parameters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_e2e_q")
      discard
        await conn.exec("CREATE TABLE test_e2e_q (id serial PRIMARY KEY, name text)")
      discard await conn.exec(
        "INSERT INTO test_e2e_q (name) VALUES ($1), ($2)",
        @[toPgParam("alice"), toPgParam("bob")],
      )
      let res = await conn.query(
        "SELECT name FROM test_e2e_q WHERE name = $1", @[toPgParam("bob")]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0][0].get().toString() == "bob"
      discard await conn.exec("DROP TABLE test_e2e_q")
      await conn.close()

    waitFor t()

  test "prepare, execute, and close statement":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_e2e_ps")
      discard
        await conn.exec("CREATE TABLE test_e2e_ps (id serial PRIMARY KEY, val text)")
      discard await conn.exec(
        "INSERT INTO test_e2e_ps (val) VALUES ($1), ($2)",
        @[toPgParam("x"), toPgParam("y")],
      )

      let stmt =
        await conn.prepare("my_stmt", "SELECT val FROM test_e2e_ps WHERE val = $1")
      doAssert stmt.name == "my_stmt"
      doAssert stmt.fields.len == 1
      doAssert stmt.paramOids.len == 1

      let res = await stmt.execute(@[toPgParam("x")])
      doAssert res.rows.len == 1
      doAssert res.rows[0][0].get().toString() == "x"

      await stmt.close()

      # Connection still usable after statement close
      let r2 = await conn.query("SELECT 1 AS check_col")
      doAssert r2.rows.len == 1

      discard await conn.exec("DROP TABLE test_e2e_ps")
      await conn.close()

    waitFor t()

suite "E2E: Multi-Statement and Large Results":
  test "simpleQuery multiple statements":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("SELECT 1 AS a; SELECT 2 AS b, 3 AS c")
      doAssert results.len == 2
      doAssert results[0].fields.len == 1
      doAssert results[0].rows.len == 1
      doAssert results[0].rows[0][0].get().toString() == "1"
      doAssert results[1].fields.len == 2
      doAssert results[1].rows.len == 1
      doAssert results[1].rows[0][0].get().toString() == "2"
      doAssert results[1].rows[0][1].get().toString() == "3"
      await conn.close()

    waitFor t()

  test "query 10000 rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT g FROM generate_series(1, 10000) AS g")
      doAssert res.rows.len == 10000
      doAssert res.rows[0].getStr(0) == "1"
      doAssert res.rows[9999].getStr(0) == "10000"
      await conn.close()

    waitFor t()

  test "cursor over 10000 rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let cursor = await conn.openCursor(
        "SELECT g FROM generate_series(1, 10000) AS g", chunkSize = 100
      )

      var totalRows = 0
      while true:
        let chunk = await cursor.fetchNext()
        if chunk.len == 0:
          break
        doAssert chunk.len <= 100
        totalRows += chunk.len

      doAssert totalRows == 10000
      doAssert cursor.exhausted
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

suite "E2E: Prepared Statement Edge Cases":
  test "20 parameters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var placeholders: seq[string]
      var params: seq[PgParam]
      for i in 1 .. 20:
        placeholders.add("$" & $i & "::int")
        params.add(toPgParam(int32(i * 10)))
      let sql = "SELECT " & placeholders.join(", ")
      let stmt = await conn.prepare("stmt_20_params", sql)
      let res = await stmt.execute(params)
      doAssert res.rows.len == 1
      for i in 0 ..< 20:
        doAssert res.rows[0].getInt(i) == int32((i + 1) * 10)
      await stmt.close()
      await conn.close()

    waitFor t()

  test "duplicate name raises error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt1 = await conn.prepare("dup_stmt", "SELECT 1")
      var raised = false
      try:
        let stmt2 = await conn.prepare("dup_stmt", "SELECT 2")
        discard stmt2
      except PgError:
        raised = true
      doAssert raised
      # Connection should still be usable
      doAssert conn.state == csReady
      let res = await conn.query("SELECT 42")
      doAssert res.rows[0].getStr(0) == "42"
      await stmt1.close()
      await conn.close()

    waitFor t()
