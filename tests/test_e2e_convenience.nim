import std/[unittest, options, tables, math, importutils, net]

import
  ../async_postgres/
    [async_backend, pg_protocol, pg_types, pg_client, pg_pool, pg_connection]
import ../async_postgres/pg_client/core

when hasAsyncDispatch:
  import std/strutils

import e2e_common

privateAccess(PgConnection)

suite "E2E: Column Name Access":
  test "columnIndex on QueryResult":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 42::int4 AS id, 'alice'::text AS name")
      doAssert res.columnIndex("id") == 0
      doAssert res.columnIndex("name") == 1
      doAssert res.rows[0].getInt(res.columnIndex("id")) == 42'i32
      doAssert res.rows[0].getStr(res.columnIndex("name")) == "alice"
      await conn.close()

    waitFor t()

  test "columnMap for repeated access":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT 1::int4 AS a, 'x'::text AS b UNION ALL SELECT 2, 'y'")
      let cols = res.fields.columnMap()
      doAssert res.rows[0].getInt(cols["a"]) == 1'i32
      doAssert res.rows[0].getStr(cols["b"]) == "x"
      doAssert res.rows[1].getInt(cols["a"]) == 2'i32
      doAssert res.rows[1].getStr(cols["b"]) == "y"
      await conn.close()

    waitFor t()

  test "columnIndex on PreparedStatement":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt =
        await conn.prepare("col_idx_stmt", "SELECT $1::int4 AS val, $2::text AS label")
      doAssert stmt.columnIndex("val") == 0
      doAssert stmt.columnIndex("label") == 1
      let res = await stmt.execute(@[toPgParam(99'i32), toPgParam("test")])
      doAssert res.rows[0].getInt(stmt.columnIndex("val")) == 99'i32
      doAssert res.rows[0].getStr(stmt.columnIndex("label")) == "test"
      await stmt.close()
      await conn.close()

    waitFor t()

  test "columnIndex raises for missing column":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 1 AS x")
      var raised = false
      try:
        discard res.columnIndex("nonexistent")
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "name-based row accessors via rows()":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT 42::int4 AS id, 'alice'::text AS name, true::bool AS active, 3.14::float8 AS score"
      )
      doAssert res.rows.len == 1
      let row = res.rows[0]
      doAssert row.getInt("id") == 42'i32
      doAssert row.getStr("name") == "alice"
      doAssert row.getBool("active") == true
      doAssert abs(row.getFloat("score") - 3.14) < 0.001
      doAssert row.isNull("name") == false
      await conn.close()

    waitFor t()

  test "name-based row accessors via items iterator":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT 1::int4 AS v UNION ALL SELECT 2 UNION ALL SELECT 3")
      var vals: seq[int32]
      for row in res:
        vals.add(row.getInt("v"))
      doAssert vals == @[1'i32, 2'i32, 3'i32]
      await conn.close()

    waitFor t()

  test "name-based Opt accessors":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 10::int4 AS a, NULL::text AS b")
      let row = res.rows[0]
      doAssert row.getIntOpt("a") == some(10'i32)
      doAssert row.getStrOpt("b").isNone
      await conn.close()

    waitFor t()

  test "name-based queryRowOpt accessors":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let rowOpt =
        await conn.queryRowOpt("SELECT 99::int8 AS big, 'hello'::text AS msg")
      doAssert rowOpt.isSome
      let row = rowOpt.get
      doAssert row.getInt64("big") == 99'i64
      doAssert row.getStr("msg") == "hello"
      await conn.close()

    waitFor t()

  test "name-based accessor raises on missing column":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 1::int4 AS x")
      let row = res.rows[0]
      var raised = false
      try:
        discard row.getStr("nonexistent")
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "name-based accessor raises without field metadata":
    proc t() {.async.} =
      # Row constructed manually without fields
      let row: Row = @[some(@[byte(49)])]
      var raised = false
      try:
        discard row.getStr("x")
      except PgTypeError:
        raised = true
      doAssert raised

    waitFor t()

suite "E2E: Convenience Query Methods":
  test "queryRowOpt returns first row":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row = await conn.queryRowOpt("SELECT 1 AS a, 'hello' AS b")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "1"
      doAssert row.get.getStr(1) == "hello"
      await conn.close()

    waitFor t()

  test "queryRowOpt returns none for empty result":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row = await conn.queryRowOpt("SELECT 1 WHERE false")
      doAssert row.isNone
      await conn.close()

    waitFor t()

  test "queryRow returns first row":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row = await conn.queryRow("SELECT 1 AS a, 'hello' AS b")
      doAssert row.getStr(0) == "1"
      doAssert row.getStr(1) == "hello"
      await conn.close()

    waitFor t()

  test "queryRow raises on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryRow("SELECT 1 WHERE false")
      except PgNoRowsError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValue returns scalar":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValue("SELECT 42")
      doAssert val == "42"
      await conn.close()

    waitFor t()

  test "queryValue raises on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue("SELECT 1 WHERE false")
      except PgNoRowsError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValue raises on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue("SELECT NULL::text")
      except PgNullError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValueOrDefault returns value":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOrDefault("SELECT 'yes'")
      doAssert val == "yes"
      await conn.close()

    waitFor t()

  test "queryValueOrDefault returns default on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOrDefault("SELECT 1 WHERE false", default = "nope")
      doAssert val == "nope"
      await conn.close()

    waitFor t()

  test "queryValueOrDefault returns default on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val =
        await conn.queryValueOrDefault("SELECT NULL::text", default = "fallback")
      doAssert val == "fallback"
      await conn.close()

    waitFor t()

  test "queryValue with typedesc returns typed value":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValue(int64, "SELECT 42")
      doAssert val == 42'i64
      let fval = await conn.queryValue(float64, "SELECT 3.14::float8")
      doAssert abs(fval - 3.14) < 0.001
      let bval = await conn.queryValue(bool, "SELECT true")
      doAssert bval == true
      let sval = await conn.queryValue(string, "SELECT 'hello'")
      doAssert sval == "hello"
      await conn.close()

    waitFor t()

  test "queryValue with typedesc raises on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue(int32, "SELECT 1 WHERE false")
      except PgNoRowsError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValue with typedesc raises on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue(int64, "SELECT NULL::int8")
      except PgNullError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValueOrDefault with typedesc":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val =
        await conn.queryValueOrDefault(int64, "SELECT 1 WHERE false", default = -1'i64)
      doAssert val == -1'i64
      let val2 = await conn.queryValueOrDefault(int64, "SELECT 99", default = 0'i64)
      doAssert val2 == 99'i64
      await conn.close()

    waitFor t()

  test "queryValueOrDefault infers type from default":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOrDefault("SELECT 1 WHERE false", default = -1'i64)
      doAssert val == -1'i64
      let val2 = await conn.queryValueOrDefault("SELECT 42", default = 0'i32)
      doAssert val2 == 42'i32
      await conn.close()

    waitFor t()

  test "queryValueOpt returns some on value":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt("SELECT 'hello'")
      doAssert val == some("hello")
      await conn.close()

    waitFor t()

  test "queryValueOpt returns none on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt("SELECT 1 WHERE false")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryValueOpt returns none on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt("SELECT NULL::text")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryValueOpt with typedesc returns some":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt(int64, "SELECT 42")
      doAssert val == some(42'i64)
      await conn.close()

    waitFor t()

  test "queryValueOpt with typedesc returns none on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt(int32, "SELECT 1 WHERE false")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryValueOpt with typedesc returns none on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt(int64, "SELECT NULL::int8")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryExists returns true when rows exist":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let exists = await conn.queryExists("SELECT 1")
      doAssert exists
      await conn.close()

    waitFor t()

  test "queryExists returns false when no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let exists = await conn.queryExists("SELECT 1 WHERE false")
      doAssert not exists
      await conn.close()

    waitFor t()

  test "exec().affectedRows returns affected row count":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE ea_test (id int)")
      discard await conn.exec("INSERT INTO ea_test VALUES (1), (2), (3)")
      let cr = await conn.exec("DELETE FROM ea_test WHERE id > 1", newSeq[PgParam]())
      doAssert cr.affectedRows == 2
      await conn.close()

    waitFor t()

  test "queryColumn returns column values":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let vals = await conn.queryColumn("SELECT generate_series(1,3)::text")
      doAssert vals == @["1", "2", "3"]
      await conn.close()

    waitFor t()

  test "queryColumn raises on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryColumn("SELECT NULL::text")
      except PgNullError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryRowOpt returns only first row from multiple":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row = await conn.queryRowOpt("SELECT generate_series(10,12)::text AS v")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "10"
      await conn.close()

    waitFor t()

  test "queryColumn returns empty seq for no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let vals = await conn.queryColumn("SELECT 1::text WHERE false")
      doAssert vals.len == 0
      await conn.close()

    waitFor t()

  test "exec().affectedRows returns 0 when no rows affected":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE ea_zero (id int)")
      let cr = await conn.exec("DELETE FROM ea_zero WHERE id = 999", newSeq[PgParam]())
      doAssert cr.affectedRows == 0
      await conn.close()

    waitFor t()

  test "queryRowOpt with params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row =
        await conn.queryRowOpt("SELECT $1::int + $2::int", @[3.toPgParam, 4.toPgParam])
      doAssert row.isSome
      doAssert row.get.getStr(0) == "7"
      await conn.close()

    waitFor t()

  test "query Row survives subsequent queries (lifetime bug)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr1 = await conn.query("SELECT 'x'")
      let qr2 = await conn.query("SELECT 'y'")
      doAssert qr1.rowCount == 1
      doAssert qr2.rowCount == 1
      let row1 = initRow(qr1.data, 0)
      let row2 = initRow(qr2.data, 0)
      doAssert row1.getStr(0) == "x", "qr1 data was invalidated by qr2"
      doAssert row2.getStr(0) == "y"
      await conn.close()

    waitFor t()

  test "pool queryRowOpt":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let row = await pool.queryRowOpt("SELECT 'pooled'")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "pooled"
      await pool.close()

    waitFor t()

  test "pool queryRow":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let row = await pool.queryRow("SELECT 'pooled' AS v")
      doAssert row.getStr("v") == "pooled"
      await pool.close()

    waitFor t()

  test "pool queryRow raises on no rows":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      var raised = false
      try:
        discard await pool.queryRow("SELECT 1 WHERE false")
      except PgNoRowsError:
        raised = true
      doAssert raised
      await pool.close()

    waitFor t()

  test "pool queryValue":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValue("SELECT 99")
      doAssert val == "99"
      await pool.close()

    waitFor t()

  test "pool queryValue raises on no rows":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      var raised = false
      try:
        discard await pool.queryValue("SELECT 1 WHERE false")
      except PgNoRowsError:
        raised = true
      doAssert raised
      await pool.close()

    waitFor t()

  test "pool queryValue raises on NULL":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      var raised = false
      try:
        discard await pool.queryValue("SELECT NULL::text")
      except PgNullError:
        raised = true
      doAssert raised
      await pool.close()

    waitFor t()

  test "pool queryExists":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      doAssert (await pool.queryExists("SELECT 1"))
      doAssert not (await pool.queryExists("SELECT 1 WHERE false"))
      await pool.close()

    waitFor t()

  test "pool exec().affectedRows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE pool_ea2 (id int)")
      discard await conn.exec("INSERT INTO pool_ea2 VALUES (1), (2)")
      let cr = await conn.exec("DELETE FROM pool_ea2", newSeq[PgParam]())
      doAssert cr.affectedRows == 2
      await conn.close()

    waitFor t()

  test "pool queryColumn":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let vals = await pool.queryColumn("SELECT generate_series(10,12)::text")
      doAssert vals == @["10", "11", "12"]
      await pool.close()

    waitFor t()

  test "pool queryValueOrDefault":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValueOrDefault("SELECT 1 WHERE false", default = "x")
      doAssert val == "x"
      let val2 = await pool.queryValueOrDefault("SELECT 'ok'")
      doAssert val2 == "ok"
      await pool.close()

    waitFor t()

  test "pool queryValue with typedesc":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValue(int64, "SELECT 123")
      doAssert val == 123'i64
      await pool.close()

    waitFor t()

  test "pool queryValue with typedesc raises on no rows":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      var raised = false
      try:
        discard await pool.queryValue(int32, "SELECT 1 WHERE false")
      except PgNoRowsError:
        raised = true
      doAssert raised
      await pool.close()

    waitFor t()

  test "pool queryValue with typedesc raises on NULL":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      var raised = false
      try:
        discard await pool.queryValue(int64, "SELECT NULL::int8")
      except PgNullError:
        raised = true
      doAssert raised
      await pool.close()

    waitFor t()

  test "pool queryValueOrDefault with typedesc":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val =
        await pool.queryValueOrDefault(int32, "SELECT 1 WHERE false", default = -1'i32)
      doAssert val == -1'i32
      let val2 = await pool.queryValueOrDefault(int32, "SELECT 7", default = 0'i32)
      doAssert val2 == 7'i32
      await pool.close()

    waitFor t()

  test "pool queryValueOrDefault infers type from default":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValueOrDefault("SELECT 1 WHERE false", default = -1'i32)
      doAssert val == -1'i32
      let val2 = await pool.queryValueOrDefault("SELECT 7", default = 0'i32)
      doAssert val2 == 7'i32
      await pool.close()

    waitFor t()

  test "pool queryValueOpt":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValueOpt("SELECT 'ok'")
      doAssert val == some("ok")
      let none_val = await pool.queryValueOpt("SELECT 1 WHERE false")
      doAssert none_val.isNone
      await pool.close()

    waitFor t()

  test "pool queryValueOpt with typedesc":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValueOpt(int64, "SELECT 123")
      doAssert val == some(123'i64)
      let none_val = await pool.queryValueOpt(int32, "SELECT 1 WHERE false")
      doAssert none_val.isNone
      await pool.close()

    waitFor t()

  test "stmt cache: repeated query uses cache":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.stmtCacheCapacity == 256
      doAssert conn.stmtCache.len == 0

      # First call: cache miss -> populates cache
      let r1 = await conn.query("SELECT 1 AS v")
      doAssert r1.rows[0].getStr(0) == "1"
      doAssert conn.stmtCache.len == 1

      # Second call: cache hit
      let r2 = await conn.query("SELECT 1 AS v")
      doAssert r2.rows[0].getStr(0) == "1"
      doAssert conn.stmtCache.len == 1 # no new entry

      await conn.close()

    waitFor t()

  test "stmt cache: repeated exec uses cache":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      discard await conn.exec("SELECT 1")
      doAssert conn.stmtCache.len == 1

      discard await conn.exec("SELECT 1")
      doAssert conn.stmtCache.len == 1

      # Different SQL gets its own entry
      discard await conn.exec("SELECT 2")
      doAssert conn.stmtCache.len == 2

      await conn.close()

    waitFor t()

  test "stmt cache: query with params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query(
        "SELECT $1::int + $2::int AS sum", @[toPgParam("1"), toPgParam("2")]
      )
      doAssert r1.rows[0].getStr(0) == "3"
      doAssert conn.stmtCache.len == 1

      # Same SQL, different params: cache hit
      let r2 = await conn.query(
        "SELECT $1::int + $2::int AS sum", @[toPgParam("3"), toPgParam("4")]
      )
      doAssert r2.rows[0].getStr(0) == "7"
      doAssert conn.stmtCache.len == 1

      await conn.close()

    waitFor t()

  test "stmt cache: binary result format works with cache":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query("SELECT 42::int4", resultFormat = rfBinary)
      doAssert r1.rows[0].getInt(0) == 42
      doAssert conn.stmtCache.len == 1

      # Cache hit with binary format
      let r2 = await conn.query("SELECT 42::int4", resultFormat = rfBinary)
      doAssert r2.rows[0].getInt(0) == 42

      await conn.close()

    waitFor t()

  test "stmt cache: switching result format on cache hit decodes correctly":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Cache the statement in binary format first.
      let rBin = await conn.query("SELECT 42::int4 AS v", resultFormat = rfBinary)
      doAssert rBin.rows[0].getInt(0) == 42
      doAssert conn.stmtCache.len == 1

      # Cache hit, but now request text. The decoder must follow the text wire
      # format this Bind requested, not the binary format cached at first Parse;
      # otherwise the ASCII bytes are reinterpreted as a big-endian int.
      let rText = await conn.query("SELECT 42::int4 AS v", resultFormat = rfText)
      doAssert conn.stmtCache.len == 1 # still a hit, no re-parse
      doAssert rText.rows[0].getStr(0) == "42"
      doAssert rText.rows[0].getInt(0) == 42

      # Switch back to binary on the same cached statement.
      let rBin2 = await conn.query("SELECT 42::int4 AS v", resultFormat = rfBinary)
      doAssert conn.stmtCache.len == 1
      doAssert rBin2.rows[0].getInt(0) == 42

      await conn.close()

    waitFor t()

  test "stmt cache: queryEach follows requested format on cache hit":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Cache the statement in binary format via query.
      let rBin = await conn.query("SELECT 7::int4 AS v", resultFormat = rfBinary)
      doAssert rBin.rows[0].getInt(0) == 7
      doAssert conn.stmtCache.len == 1

      # Cache hit through queryEach with text: rows must decode as text.
      var values: seq[string]
      let rowCount = await conn.queryEach(
        "SELECT 7::int4 AS v",
        resultFormat = rfText,
        callback = proc(row: Row) =
          values.add(row.getStr(0)),
      )
      doAssert conn.stmtCache.len == 1
      doAssert rowCount == 1
      doAssert values == @["7"]

      await conn.close()

    waitFor t()

  test "queryEach records ParameterStatus into serverParams":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # queryEach dispatches messages through nextMessage, so GUC_REPORT
      # ParameterStatus updates (e.g. application_name) must reach serverParams.
      const newName = "queryeach_param_status_test"
      doAssert conn.serverParams.getOrDefault("application_name", "") != newName
      let rowCount = await conn.queryEach(
        "SET application_name = '" & newName & "'",
        callback = proc(row: Row) =
          discard,
      )
      doAssert rowCount == 0
      doAssert conn.serverParams.getOrDefault("application_name", "") == newName

      await conn.close()

    waitFor t()

  test "stmt cache: format switches do not mutate cached field metadata":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let sql = "SELECT 42::int4 AS v"

      # First call populates the cache.
      let rBin = await conn.query(sql, resultFormat = rfBinary)
      doAssert rBin.rows[0].getInt(0) == 42
      doAssert conn.stmtCache.len == 1
      let cached = conn.stmtCache[sql]
      # Cached metadata must stay in the Describe(Statement) canonical form
      # (formatCode = text/0); the per-call format belongs in resultFormats/colFmts.
      doAssert cached.fields.len == 1
      doAssert cached.fields[0].formatCode == 0

      # QueryResult reflects the actual binary decode format.
      doAssert rBin.fields[0].formatCode == 1

      # Cache hit with a different format must not rewrite cached.fields.
      let rText = await conn.query(sql, resultFormat = rfText)
      doAssert conn.stmtCache.len == 1
      doAssert cached.fields[0].formatCode == 0
      doAssert rText.fields[0].formatCode == 0
      doAssert rText.rows[0].getStr(0) == "42"

      # Switch back to binary; cache metadata is still untouched.
      let rBin2 = await conn.query(sql, resultFormat = rfBinary)
      doAssert conn.stmtCache.len == 1
      doAssert cached.fields[0].formatCode == 0
      doAssert rBin2.fields[0].formatCode == 1
      doAssert rBin2.rows[0].getInt(0) == 42

      await conn.close()

    waitFor t()

  test "stmt cache: pipeline follows requested format on cache hit":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let sql = "SELECT 42::int4 AS v"

      # Populate the cache via query. The cached column formats are binary for
      # int4 (a binary-safe type), independent of the format this call asked for.
      let rBin = await conn.query(sql, resultFormat = rfBinary)
      doAssert rBin.rows[0].getInt(0) == 42
      doAssert conn.stmtCache.len == 1

      # Cache hit through a pipeline requesting text: rows must decode as text,
      # following this op's Bind rather than the cached binary format.
      let p = newPipeline(conn)
      p.addQuery(sql, resultFormat = rfText)
      let rText = await p.execute()
      doAssert conn.stmtCache.len == 1 # still a hit, no re-parse
      doAssert rText[0].queryResult.rows[0].getStr(0) == "42"
      doAssert rText[0].queryResult.rows[0].getInt(0) == 42

      # Cache hit through a pipeline requesting binary still decodes correctly.
      let p2 = newPipeline(conn)
      p2.addQuery(sql, resultFormat = rfBinary)
      let rBin2 = await p2.execute()
      doAssert conn.stmtCache.len == 1
      doAssert rBin2[0].queryResult.rows[0].getInt(0) == 42

      await conn.close()

    waitFor t()

  test "stmt cache: pipeline format switch keeps cached metadata and honors no-override":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let sql = "SELECT 42::int4 AS v"

      # Cache the statement (binary-preferred formats for int4).
      let rBin = await conn.query(sql, resultFormat = rfBinary)
      doAssert rBin.rows[0].getInt(0) == 42
      doAssert conn.stmtCache.len == 1
      let cached = conn.stmtCache[sql]
      # Cached metadata stays in the Describe(Statement) canonical form (text/0).
      doAssert cached.fields[0].formatCode == 0

      # Pipeline cache hit switching to text must not rewrite cached.fields, and
      # the returned QueryResult must reflect the actual text decode format.
      let p = newPipeline(conn)
      p.addQuery(sql, resultFormat = rfText)
      let rText = await p.execute()
      doAssert conn.stmtCache.len == 1
      doAssert cached.fields[0].formatCode == 0
      doAssert rText[0].queryResult.fields[0].formatCode == 0
      doAssert rText[0].queryResult.rows[0].getStr(0) == "42"

      # Pipeline cache hit with NO override (rfAuto) replays the cached
      # binary-preferred format: rows decode as binary and the returned metadata
      # reflects it, without the op freezing into a resolved format.
      let p2 = newPipeline(conn)
      p2.addQuery(sql)
      let rAuto = await p2.execute()
      doAssert conn.stmtCache.len == 1
      doAssert cached.fields[0].formatCode == 0
      doAssert rAuto[0].queryResult.fields[0].formatCode == 1
      doAssert rAuto[0].queryResult.rows[0].getInt(0) == 42

      await conn.close()

    waitFor t()

  test "stmt cache: clearStmtCache works":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      discard await conn.query("SELECT 1")
      discard await conn.query("SELECT 2")
      doAssert conn.stmtCache.len == 2

      conn.clearStmtCache()
      doAssert conn.stmtCache.len == 0

      # After clear, queries still work (cache miss path)
      let r = await conn.query("SELECT 3")
      doAssert r.rows[0].getStr(0) == "3"
      doAssert conn.stmtCache.len == 1

      await conn.close()

    waitFor t()

  test "stmt cache: disabled when capacity=0":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 0

      discard await conn.query("SELECT 1")
      doAssert conn.stmtCache.len == 0

      discard await conn.exec("SELECT 1")
      doAssert conn.stmtCache.len == 0

      await conn.close()

    waitFor t()

  test "stmt cache: full cache evicts LRU entry":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      discard await conn.query("SELECT 1")
      discard await conn.query("SELECT 2")
      doAssert conn.stmtCache.len == 2

      # Cache full, LRU entry ("SELECT 1") is evicted
      let r = await conn.query("SELECT 3")
      doAssert r.rows[0].getStr(0) == "3"
      doAssert conn.stmtCache.len == 2
      doAssert not conn.stmtCache.hasKey("SELECT 1") # evicted
      doAssert conn.stmtCache.hasKey("SELECT 2")
      doAssert conn.stmtCache.hasKey("SELECT 3") # newly cached

      # Access "SELECT 2" to make it most recent, then add new
      discard await conn.query("SELECT 2")
      discard await conn.query("SELECT 4")
      doAssert conn.stmtCache.len == 2
      doAssert not conn.stmtCache.hasKey("SELECT 3") # evicted (was LRU)
      doAssert conn.stmtCache.hasKey("SELECT 2") # kept (was accessed)
      doAssert conn.stmtCache.hasKey("SELECT 4") # newly cached

      await conn.close()

    waitFor t()

  test "stmt cache: addStmtCache evicts when full (defensive guard)":
    # If a caller ever skips the pre-eviction step before calling
    # addStmtCache, the new entry must still be inserted (the cache must
    # not silently drop it, which would leak the corresponding server-side
    # prepared statement). The evicted name is queued in pendingStmtCloses
    # so the next Extended Query operation sends its server-side Close.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      conn.addStmtCache("A", CachedStmt(name: "s1"))
      conn.addStmtCache("B", CachedStmt(name: "s2"))
      doAssert conn.stmtCache.len == 2
      doAssert conn.pendingStmtCloses.len == 0

      # Bypass the caller-side eviction and add a third entry directly.
      conn.addStmtCache("C", CachedStmt(name: "s3"))
      doAssert conn.stmtCache.len == 2
      doAssert not conn.stmtCache.hasKey("A") # LRU evicted
      doAssert conn.stmtCache.hasKey("B")
      doAssert conn.stmtCache.hasKey("C") # newly inserted, not dropped
      doAssert conn.pendingStmtCloses == @["s1"] # queued for next op

      await conn.close()

    waitFor t()

  test "stmt cache: addStmtCache evicts down to capacity after shrink":
    # Shrinking stmtCacheCapacity below the current size leaves the cache
    # over-full; the next addStmtCache must drain it down to the new
    # capacity rather than dropping the new entry or stopping after one
    # eviction. All evicted names are queued in pendingStmtCloses.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 4

      conn.addStmtCache("A", CachedStmt(name: "s1"))
      conn.addStmtCache("B", CachedStmt(name: "s2"))
      conn.addStmtCache("C", CachedStmt(name: "s3"))
      conn.addStmtCache("D", CachedStmt(name: "s4"))
      doAssert conn.stmtCache.len == 4

      conn.stmtCacheCapacity = 2
      conn.addStmtCache("E", CachedStmt(name: "s5"))
      doAssert conn.stmtCache.len == 2
      doAssert conn.stmtCache.hasKey("D") # most-recent kept
      doAssert conn.stmtCache.hasKey("E") # newly inserted
      doAssert conn.pendingStmtCloses == @["s1", "s2", "s3"] # all queued

      await conn.close()

    waitFor t()

  test "stmt cache: next operation drains pending closes":
    # The pending Close queue from defensive eviction is flushed at the
    # start of the next Extended Query send phase. Postgres treats Close
    # on a non-existent prepared statement as a no-op (returns
    # CloseComplete), so using synthetic names here is safe.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      conn.addStmtCache("A", CachedStmt(name: "s1"))
      conn.addStmtCache("B", CachedStmt(name: "s2"))
      conn.addStmtCache("C", CachedStmt(name: "s3")) # evicts A
      doAssert conn.pendingStmtCloses.len == 1

      # Any Extended Query operation drains the queue.
      let r = await conn.query("SELECT 42")
      doAssert r.rows[0].getInt(0) == 42
      doAssert conn.pendingStmtCloses.len == 0

      await conn.close()

    waitFor t()

  test "stmt cache: shrink drains entire pending queue in one op":
    # After a large shrink, multiple pending closes are batched into the
    # next operation's Sync round-trip.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 4

      conn.addStmtCache("A", CachedStmt(name: "s1"))
      conn.addStmtCache("B", CachedStmt(name: "s2"))
      conn.addStmtCache("C", CachedStmt(name: "s3"))
      conn.addStmtCache("D", CachedStmt(name: "s4"))

      conn.stmtCacheCapacity = 2
      conn.addStmtCache("E", CachedStmt(name: "s5"))
      doAssert conn.pendingStmtCloses.len == 3

      discard await conn.query("SELECT 1")
      doAssert conn.pendingStmtCloses.len == 0

      await conn.close()

    waitFor t()

  test "stmt cache: openCursor flushes pending closes":
    # Cursor send paths used to use a local batch buffer that bypassed
    # ``conn.sendBuf``, so any closes queued in ``pendingStmtCloses`` would
    # sit there until the next non-cursor Extended Query op. The cursor
    # batch now drains the queue first.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      conn.addStmtCache("A", CachedStmt(name: "s1"))
      conn.addStmtCache("B", CachedStmt(name: "s2"))
      conn.addStmtCache("C", CachedStmt(name: "s3")) # evicts A, queues s1
      doAssert conn.pendingStmtCloses.len == 1

      let cursor = await conn.openCursor("SELECT generate_series(1,3)", chunkSize = 10)
      doAssert conn.pendingStmtCloses.len == 0
      discard await cursor.fetchNext()
      await cursor.close()

      await conn.close()

    waitFor t()

  test "stmt cache: fetchNext flushes pending closes":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      let cursor = await conn.openCursor("SELECT generate_series(1,30)", chunkSize = 10)
      # The first chunk was buffered inside openCursor; this fetchNext
      # returns that buffer without touching fetchNextImpl.
      let first = await cursor.fetchNext()
      doAssert first.len == 10

      # Inject a pending close mid-cursor and verify the *next* fetchNext
      # (which actually goes through fetchNextImpl) drains the queue.
      conn.pendingStmtCloses.add("nonexistent_stmt_1")
      doAssert conn.pendingStmtCloses.len == 1

      let second = await cursor.fetchNext()
      doAssert second.len == 10
      doAssert conn.pendingStmtCloses.len == 0

      await cursor.close()
      await conn.close()

    waitFor t()

  test "stmt cache: closeCursor flushes pending closes":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      let cursor = await conn.openCursor("SELECT generate_series(1,30)", chunkSize = 5)
      discard await cursor.fetchNext() # not exhausted (30 rows, chunk 5)

      conn.pendingStmtCloses.add("nonexistent_stmt_2")
      doAssert conn.pendingStmtCloses.len == 1

      await cursor.close()
      doAssert conn.pendingStmtCloses.len == 0

      await conn.close()

    waitFor t()

  test "stmt cache: clearStmtCache also clears pendingStmtCloses":
    # ``clearStmtCache`` documents that it does not close server-side
    # statements; that intent extends to any closes queued in
    # ``pendingStmtCloses`` from defensive eviction — they are dropped on
    # the assumption the caller will reset the session externally.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      conn.addStmtCache("A", CachedStmt(name: "s1"))
      conn.addStmtCache("B", CachedStmt(name: "s2"))
      conn.addStmtCache("C", CachedStmt(name: "s3")) # evicts A, queues s1
      doAssert conn.stmtCache.len == 2
      doAssert conn.pendingStmtCloses.len == 1

      conn.clearStmtCache()
      doAssert conn.stmtCache.len == 0
      doAssert conn.pendingStmtCloses.len == 0

      await conn.close()

    waitFor t()

  test "stmt cache: works with pool":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 1))

      # First query populates cache on the pooled connection
      let r1 = await pool.query("SELECT 'cached'")
      doAssert r1.rows[0].getStr(0) == "cached"

      # Second query should hit cache
      let r2 = await pool.query("SELECT 'cached'")
      doAssert r2.rows[0].getStr(0) == "cached"

      await pool.close()

    waitFor t()

  test "stmt cache: paramOids saved from ParameterDescription":
    # The cache-miss path captures the server's ParameterDescription so the
    # cache hit path can validate that a follow-up call binds compatible
    # parameter types. Without this, the server would interpret bind bytes
    # under the original parse-time OIDs even when the caller intended a
    # different type, silently corrupting results.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r = await conn.query("SELECT $1::int8 AS v", @[toPgParam(123'i64)])
      doAssert r.rows[0].getInt64(0) == 123
      doAssert conn.stmtCache.len == 1
      let cached = conn.stmtCache["SELECT $1::int8 AS v"]
      doAssert cached.paramOids == @[OidInt8]

      await conn.close()

    waitFor t()

  test "stmt cache: identical OIDs reuse cached statement":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query("SELECT $1::int8", @[toPgParam(1'i64)])
      doAssert r1.rows[0].getInt64(0) == 1
      let firstName = conn.stmtCache["SELECT $1::int8"].name

      # Same OID set → cache hit, same server-side statement reused.
      let r2 = await conn.query("SELECT $1::int8", @[toPgParam(2'i64)])
      doAssert r2.rows[0].getInt64(0) == 2
      doAssert conn.stmtCache.len == 1
      doAssert conn.stmtCache["SELECT $1::int8"].name == firstName

      await conn.close()

    waitFor t()

  test "stmt cache: mismatched OIDs invalidate and re-parse":
    # Reproduces the silent-corruption pathway: same SQL text bound first
    # with int8 and then with int4. Without invalidation, the server would
    # treat the int4 4-byte payload as part of an int8 statement (or fail
    # on length mismatch). With invalidation, the stale entry's server-side
    # statement is queued for Close and a fresh Parse runs under the new
    # types.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query("SELECT $1 AS v", @[toPgParam(123'i64)])
      doAssert r1.rows[0].getInt64(0) == 123
      let firstName = conn.stmtCache["SELECT $1 AS v"].name
      doAssert conn.stmtCache["SELECT $1 AS v"].paramOids == @[OidInt8]

      # Same SQL, different OID (int4 instead of int8). The cache entry
      # for int8 must be evicted and a new statement parsed for int4.
      let r2 = await conn.query("SELECT $1 AS v", @[toPgParam(7'i32)])
      doAssert r2.rows[0].getInt(0) == 7
      doAssert conn.stmtCache.len == 1
      let entry = conn.stmtCache["SELECT $1 AS v"]
      doAssert entry.paramOids == @[OidInt4]
      doAssert entry.name != firstName # fresh server-side statement

      await conn.close()

    waitFor t()

  test "stmt cache: pending close drain removes statement server-side":
    # End-to-end verification of the eviction → flushPendingStmtCloses path
    # against ``pg_prepared_statements``. An OID-mismatch invalidation
    # queues the cached statement's server name in ``pendingStmtCloses``;
    # the immediate cache-miss send drains the queue, so the Close has to
    # take effect server-side before the second query returns.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query("SELECT $1 AS v", @[toPgParam(1'i64)])
      doAssert r1.rows[0].getInt64(0) == 1
      let firstName = conn.stmtCache["SELECT $1 AS v"].name

      let pre = await conn.query(
        "SELECT count(*)::int FROM pg_prepared_statements WHERE name = $1",
        @[toPgParam(firstName)],
      )
      doAssert pre.rows[0].getInt(0) == 1

      # Same SQL, different parameter OID → invalidateIfOidMismatch queues
      # ``firstName`` into pendingStmtCloses, then the cache-miss path
      # drains it via flushPendingStmtCloses before re-parsing under a
      # new statement name.
      let r2 = await conn.query("SELECT $1 AS v", @[toPgParam(2'i32)])
      doAssert r2.rows[0].getInt(0) == 2
      doAssert conn.pendingStmtCloses.len == 0 # drained in this op
      let secondName = conn.stmtCache["SELECT $1 AS v"].name
      doAssert secondName != firstName

      # Server must have processed the Close: the old name is gone and
      # only the freshly parsed statement remains.
      let post = await conn.query(
        "SELECT name FROM pg_prepared_statements WHERE name IN ($1, $2) ORDER BY name",
        @[toPgParam(firstName), toPgParam(secondName)],
      )
      doAssert post.rows.len == 1
      doAssert post.rows[0].getStr(0) == secondName

      await conn.close()

    waitFor t()

  test "stmt cache: mismatched OIDs work across many type swaps":
    # Stress the invalidation path: repeatedly swap parameter type for the
    # same SQL text. Each swap evicts and re-parses; the cache must stay at
    # size 1 (the SQL key is the same) and results must reflect the new
    # type, not be silently reinterpreted under a stale plan.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query("SELECT $1 AS v", @[toPgParam(1'i32)])
      doAssert r1.rows[0].getInt(0) == 1
      let r2 = await conn.query("SELECT $1 AS v", @[toPgParam(2'i64)])
      doAssert r2.rows[0].getInt64(0) == 2
      let r3 = await conn.query("SELECT $1 AS v", @[toPgParam("hello")])
      doAssert r3.rows[0].getStr(0) == "hello"
      let r4 = await conn.query("SELECT $1 AS v", @[toPgParam(3'i32)])
      doAssert r4.rows[0].getInt(0) == 3
      doAssert conn.stmtCache.len == 1

      await conn.close()

    waitFor t()

  test "stmt cache: exec invalidates on OID mismatch":
    # exec discards rows, so we can swap parameter types without worrying
    # about column compatibility. The point is that the cached server-side
    # statement is replaced when OIDs change — same SQL key, new stmtName.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      discard await conn.exec("SELECT $1", @[toPgParam(1'i32)])
      let sql = "SELECT $1"
      let firstName = conn.stmtCache[sql].name
      doAssert conn.stmtCache[sql].paramOids == @[OidInt4]

      discard await conn.exec(sql, @[toPgParam("hello")])
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidText]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "stmt cache: caller OID 0 is wildcard against cached known OID":
    # ``OidUnknown`` (0) means "let the server infer the type". When the
    # caller leaves the OID unset, ``paramOidsMatch`` must treat it as a
    # wildcard so a previously cached prepared statement (with a concrete
    # parse-time OID) is reused instead of being needlessly evicted and
    # re-parsed.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # First call: explicit OidInt4 → cache stores paramOids=[OidInt4].
      discard await conn.query("SELECT $1::int + 1", @[toPgParam(1'i32)])
      doAssert conn.stmtCache.len == 1
      let firstName = conn.stmtCache["SELECT $1::int + 1"].name
      doAssert conn.stmtCache["SELECT $1::int + 1"].paramOids == @[OidInt4]

      # Second call: caller leaves OID unset (0). The wildcard branch of
      # ``paramOidsMatch`` (``n == 0``) must keep the entry alive.
      let untypedParam = PgParam(oid: 0'i32, format: 0, value: some(toBytes("2")))
      discard await conn.query("SELECT $1::int + 1", @[untypedParam])
      doAssert conn.stmtCache.len == 1
      doAssert conn.stmtCache["SELECT $1::int + 1"].name == firstName

      await conn.close()

    waitFor t()

  test "stmt cache: execInline saves paramOids and invalidates on mismatch":
    # ``execInlineImpl`` has its own recv loop (not the shared template), so
    # it must independently capture ParameterDescription and feed it into
    # ``CachedStmt.paramOids``. Without that, ``invalidateIfOidMismatch``
    # sees an empty cached OID list, length-mismatches against any non-empty
    # caller OIDs, and silently re-parses on every call — defeating the
    # cache. This test pins both halves: the first call stores OIDs, the
    # second call (different OID) replaces the entry.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      discard await conn.exec("SELECT $1", @[toPgParamInline(1'i32)])
      let sql = "SELECT $1"
      let firstName = conn.stmtCache[sql].name
      doAssert conn.stmtCache[sql].paramOids == @[OidInt4]

      # Same OIDs → cache hit, same server-side statement reused.
      discard await conn.exec(sql, @[toPgParamInline(2'i32)])
      doAssert conn.stmtCache.len == 1
      doAssert conn.stmtCache[sql].name == firstName

      # Different OID → cache evicted and re-parsed.
      discard await conn.exec(sql, @[toPgParamInline("hello")])
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidText]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "stmt cache: queryInline saves paramOids and invalidates on mismatch":
    # ``queryInlineImpl`` reuses ``queryRecvLoop``, so this is mostly a sanity
    # check that the inline parameter path threads OIDs through Parse and
    # the recv-loop captures them. Pairs with the execInline test above.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query("SELECT $1 AS v", @[toPgParamInline(1'i32)])
      doAssert r1.rows[0].getInt(0) == 1
      let sql = "SELECT $1 AS v"
      let firstName = conn.stmtCache[sql].name
      doAssert conn.stmtCache[sql].paramOids == @[OidInt4]

      let r2 = await conn.query(sql, @[toPgParamInline("hello")])
      doAssert r2.rows[0].getStr(0) == "hello"
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidText]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "paramOidsMatch: length mismatch and wildcard branches":
    # Unit-style coverage for the matcher itself, separate from the
    # round-trip e2e cases above. Exercises every branch:
    #   - equal length, all matching
    #   - length mismatch → false
    #   - wildcard on either side → true
    #   - genuine mismatch → false
    #   - empty vs empty (parameter-less SQL) → true
    doAssert paramOidsMatch([OidInt4, OidText], [OidInt4, OidText])
    doAssert not paramOidsMatch([OidInt4], [OidInt4, OidText])
    doAssert not paramOidsMatch([OidInt4, OidText], [OidInt4])
    doAssert paramOidsMatch([0'i32], [OidInt4])
    doAssert paramOidsMatch([OidInt4], [0'i32])
    doAssert paramOidsMatch([OidInt4, 0'i32], [0'i32, OidText])
    doAssert not paramOidsMatch([OidInt4], [OidText])
    doAssert not paramOidsMatch([OidInt4, OidText], [OidInt4, OidInt8])
    let emptyOids: seq[int32] = @[]
    doAssert paramOidsMatch(emptyOids, emptyOids)

  test "paramOidsMatch (PgParam overload): reads .oid in place":
    # Pin the ``openArray[PgParam]`` overload added so the ``query``/``exec``
    # cache-hit path can avoid the ``seq[int32]`` projection. Same branches
    # as the int32-vs-int32 case above, just driven through PgParam values.
    proc p(oid: int32): PgParam =
      PgParam(oid: oid, format: 0, value: none(seq[byte]))

    doAssert paramOidsMatch([OidInt4, OidText], [p(OidInt4), p(OidText)])
    doAssert not paramOidsMatch([OidInt4], [p(OidInt4), p(OidText)])
    doAssert not paramOidsMatch([OidInt4, OidText], [p(OidInt4)])
    doAssert paramOidsMatch([0'i32], [p(OidInt4)])
    doAssert paramOidsMatch([OidInt4], [p(0'i32)])
    doAssert not paramOidsMatch([OidInt4], [p(OidText)])
    let emptyParams: seq[PgParam] = @[]
    let emptyOids: seq[int32] = @[]
    doAssert paramOidsMatch(emptyOids, emptyParams)

suite "E2E: simpleExec":
  test "simpleExec returns command tag":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_simpleexec")
      discard await conn.exec(
        "CREATE TABLE test_simpleexec (id serial PRIMARY KEY, val text)"
      )

      let tag = await conn.simpleExec("INSERT INTO test_simpleexec (val) VALUES ('a')")
      doAssert tag == "INSERT 0 1"

      let tag2 =
        await conn.simpleExec("INSERT INTO test_simpleexec (val) VALUES ('b'), ('c')")
      doAssert tag2 == "INSERT 0 2"

      discard await conn.exec("DROP TABLE test_simpleexec")
      await conn.close()

    waitFor t()

  test "simpleExec raises on error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.simpleExec("SELECT * FROM nonexistent_table_xyz")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "pool.simpleExec":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let tag = await pool.simpleExec("SELECT 1")
      doAssert tag == "SELECT 1"
      await pool.close()

    waitFor t()

suite "E2E: queryDirect / execDirect":
  test "queryDirect with int32 param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::int4 + 10", 5'i32)
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 15
      await conn.close()

    waitFor t()

  test "queryDirect with string param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::text || ' world'", "hello")
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getStr(0) == "hello world"
      await conn.close()

    waitFor t()

  test "queryDirect with multiple params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect(
        "SELECT $1::int4 + $2::int4, $3::text", 10'i32, 20'i32, "abc"
      )
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 30
      doAssert qr.rows[0].getStr(1) == "abc"
      await conn.close()

    waitFor t()

  test "queryDirect cache hit on repeated call":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      for i in 0 ..< 5:
        let qr = await conn.queryDirect("SELECT $1::int4 * 2", int32(i))
        doAssert qr.rows[0].getInt(0) == int32(i * 2)
      await conn.close()

    waitFor t()

  test "queryDirect no params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT 42 AS answer")
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 42
      await conn.close()

    waitFor t()

  test "queryDirect Row survives subsequent queries (lifetime bug)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr1 = await conn.queryDirect("SELECT $1::text", "x")
      let qr2 = await conn.queryDirect("SELECT $1::text", "y")
      doAssert qr1.rowCount == 1
      doAssert qr2.rowCount == 1
      let row1 = initRow(qr1.data, 0)
      let row2 = initRow(qr2.data, 0)
      doAssert row1.getStr(0) == "x", "qr1 data was invalidated by qr2"
      doAssert row2.getStr(0) == "y"
      await conn.close()

    waitFor t()

  test "execDirect INSERT and UPDATE":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_exec_direct")
      discard await conn.exec(
        "CREATE TABLE test_exec_direct (id serial PRIMARY KEY, val int NOT NULL)"
      )

      let tag1 =
        await conn.execDirect("INSERT INTO test_exec_direct (val) VALUES ($1)", 100'i32)
      doAssert "INSERT" in tag1

      let tag2 = await conn.execDirect(
        "UPDATE test_exec_direct SET val = $1 WHERE val = $2", 200'i32, 100'i32
      )
      doAssert "UPDATE 1" in tag2

      let qr = await conn.queryDirect(
        "SELECT val FROM test_exec_direct WHERE val = $1", 200'i32
      )
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 200

      discard await conn.exec("DROP TABLE test_exec_direct")
      await conn.close()

    waitFor t()

  test "execDirect cache hit on repeated call":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_exec_direct2")
      discard await conn.exec(
        "CREATE TABLE test_exec_direct2 (id serial PRIMARY KEY, val int NOT NULL)"
      )

      for i in 0 ..< 5:
        discard await conn.execDirect(
          "INSERT INTO test_exec_direct2 (val) VALUES ($1)", int32(i)
        )

      let qr = await conn.query("SELECT count(*) FROM test_exec_direct2")
      doAssert qr.rows[0].getInt64(0) == 5

      discard await conn.exec("DROP TABLE test_exec_direct2")
      await conn.close()

    waitFor t()

  test "execDirect with bool param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::bool", true)
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getBool(0) == true
      await conn.close()

    waitFor t()

  test "execDirect with int64 param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::int8 + 1", 9223372036854775806'i64)
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt64(0) == 9223372036854775807'i64
      await conn.close()

    waitFor t()

  test "execDirect with float64 param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::float8 + 0.5", 1.25'f64)
      doAssert qr.rowCount == 1
      doAssert abs(qr.rows[0].getFloat(0) - 1.75) < 1e-10
      await conn.close()

    waitFor t()

  test "queryDirect timeout=Zero behaves as no-timeout (regression)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::int4", 7'i32, timeout = ZeroDuration)
      doAssert qr.rows[0].getInt(0) == 7
      await conn.close()

    waitFor t()

  test "execDirect timeout=Zero behaves as no-timeout (regression)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_exec_direct_to_zero")
      discard await conn.exec(
        "CREATE TABLE test_exec_direct_to_zero (id serial PRIMARY KEY, val int NOT NULL)"
      )
      let tag = await conn.execDirect(
        "INSERT INTO test_exec_direct_to_zero (val) VALUES ($1)",
        42'i32,
        timeout = ZeroDuration,
      )
      doAssert "INSERT" in tag
      discard await conn.exec("DROP TABLE test_exec_direct_to_zero")
      await conn.close()

    waitFor t()

  test "queryDirect timeout raises PgTimeoutError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var caught = false
      try:
        discard await conn.queryDirect(
          "SELECT pg_sleep($1)", 10'f64, timeout = milliseconds(50)
        )
      except PgTimeoutError:
        caught = true
      except PgError:
        discard
      doAssert caught
      doAssert conn.state == csClosed

    waitFor t()

  test "execDirect timeout raises PgTimeoutError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var caught = false
      try:
        discard await conn.execDirect(
          "SELECT pg_sleep($1)", 10'f64, timeout = milliseconds(50)
        )
      except PgTimeoutError:
        caught = true
      except PgError:
        discard
      doAssert caught
      doAssert conn.state == csClosed

    waitFor t()

  test "queryDirect: mismatched OIDs invalidate and re-parse":
    # ``queryDirect`` synthesizes the parameter OID array at macro-expansion
    # time, so this is a regression guard for the AST builder: hitting the
    # same SQL with two literal types must invalidate the cached prepared
    # statement and produce a fresh server-side ``Parse`` for the new type,
    # not silently bind the new bytes under the old plan.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.queryDirect("SELECT $1 AS v", 123'i64)
      doAssert r1.rows[0].getInt64(0) == 123
      let sql = "SELECT $1 AS v"
      doAssert conn.stmtCache[sql].paramOids == @[OidInt8]
      let firstName = conn.stmtCache[sql].name

      let r2 = await conn.queryDirect("SELECT $1 AS v", 7'i32)
      doAssert r2.rows[0].getInt(0) == 7
      doAssert conn.stmtCache.len == 1
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidInt4]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "execDirect: mismatched OIDs invalidate and re-parse":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      discard await conn.execDirect("SELECT $1", 1'i32)
      let sql = "SELECT $1"
      doAssert conn.stmtCache[sql].paramOids == @[OidInt4]
      let firstName = conn.stmtCache[sql].name

      discard await conn.execDirect("SELECT $1", "hello")
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidText]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "queryDirect: identical OIDs reuse cached statement":
    # Pair with the mismatch test above: same literal type across two calls
    # must keep the cached entry and its server-side ``stmtName``. This pins
    # that ``invalidateIfOidMismatch`` does not over-invalidate when OIDs
    # agree.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.queryDirect("SELECT $1::int8", 1'i64)
      doAssert r1.rows[0].getInt64(0) == 1
      let sql = "SELECT $1::int8"
      let firstName = conn.stmtCache[sql].name

      let r2 = await conn.queryDirect("SELECT $1::int8", 2'i64)
      doAssert r2.rows[0].getInt64(0) == 2
      doAssert conn.stmtCache.len == 1
      doAssert conn.stmtCache[sql].name == firstName

      await conn.close()

    waitFor t()

  test "queryDirect: no-param SQL skips OID-mismatch check":
    # Parameter-less ``queryDirect`` exercises the
    # ``positional.len == 0`` short-circuit in ``buildInvalidateOnOidMismatchStmt``.
    # The cached statement must be reused across repeated calls; both calls
    # must succeed and hit the same server-side statement.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.queryDirect("SELECT 42 AS answer")
      doAssert r1.rows[0].getInt(0) == 42
      let sql = "SELECT 42 AS answer"
      let firstName = conn.stmtCache[sql].name
      doAssert conn.stmtCache[sql].paramOids.len == 0

      let r2 = await conn.queryDirect("SELECT 42 AS answer")
      doAssert r2.rows[0].getInt(0) == 42
      doAssert conn.stmtCache.len == 1
      doAssert conn.stmtCache[sql].name == firstName

      await conn.close()

    waitFor t()

suite "Compile-time: queryDirect / execDirect arity":
  # ---- positive cases ----

  test "queryDirect: matching arity (single param)":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT $1::int4", 1'i32)

    )

  test "queryDirect: matching arity (multiple params)":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect(
            "SELECT $1::int4 + $2::int4, $3::text", 1'i32, 2'i32, "x"
          )

    )

  test "queryDirect: repeated placeholder reference":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard
            await conn.queryDirect("SELECT $1::text, $1::text, $2::text", "x", "y")

    )

  test "queryDirect: zero args + no placeholders":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT 42 AS answer")

    )

  test "queryDirect: $1 inside single-quoted string is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT '$1 here' AS s")

    )

  test "queryDirect: $1 inside dollar-quoted block is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT $tag$$1$tag$ AS s")

    )

  test "queryDirect: $1 inside $$..$$ dollar-quoted block is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT $$$1$$ AS s")

    )

  test "queryDirect: $1 inside double-quoted identifier is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT 1 AS \"$1\"")

    )

  test "queryDirect: $5 inside -- line comment is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT 1 -- $5 unused\n")

    )

  test "queryDirect: $5 inside /* */ block comment is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT 1 /* $5 unused */")

    )

  test "queryDirect: nested block comment skips inner $N":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT 1 /* /* $5 */ still in comment */")

    )

  test "queryDirect: non-literal SQL skips compile-time check":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          let s = "SELECT $1"
          # Arity mismatch is intentional: validator must silently skip when
          # sql is not a string literal (runtime will catch it).
          discard await conn.queryDirect(s)

    )

  test "execDirect: matching arity":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.execDirect(
            "UPDATE t SET a=$1, b=$2 WHERE c=$3", 1'i32, 2'i32, 3'i32
          )

    )

  # ---- negative cases ----

  test "queryDirect: rejects too few args":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT $1::int4, $2::int4", 1'i32)

    )

  test "queryDirect: rejects too many args":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT $1::int4", 1'i32, 2'i32)

    )

  test "queryDirect: rejects gap in numbering":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard
            await conn.queryDirect("SELECT $1::int4, $3::int4", 1'i32, 2'i32, 3'i32)

    )

  test "queryDirect: rejects $0":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT $0::int4", 1'i32)

    )

  test "queryDirect: rejects $N when no args were passed":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT $1::int4")

    )

  test "execDirect: rejects too few args":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard
            await conn.execDirect("UPDATE t SET a=$1, b=$2 WHERE c=$3", 1'i32, 2'i32)

    )

  test "queryDirect: timeout kwarg does not inflate arity":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard
            await conn.queryDirect("SELECT $1::int4", 1'i32, timeout = ZeroDuration)

    )

  test "queryDirect: $1 inside E'...' string is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT E'\\n$1' AS s")

    )

  test "queryDirect: $1 inside E'...' with backslash-escaped $ is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT E'$1\\$2' AS s")

    )

  test "queryDirect: $1 inside '' (doubled-quote escape) is ignored":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT 'it''s $1' AS s")

    )

  test "queryDirect: doubled-quote inside string then placeholder outside":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect("SELECT 'a''b' || $1::text", "x")

    )

  test "queryDirect: const SQL with matching arity":
    doAssert compiles(
      block:
        const SQL_OK = "SELECT $1::int4, $2::int4"
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect(SQL_OK, 1'i32, 2'i32)

    )

  test "queryDirect: const SQL with mismatched arity is rejected":
    doAssert not compiles(
      block:
        const SQL_BAD = "SELECT $1::int4, $2::int4"
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.queryDirect(SQL_BAD, 1'i32)

    )

  test "execDirect: const SQL with mismatched arity is rejected":
    doAssert not compiles(
      block:
        const SQL_BAD_EXEC = "UPDATE t SET a=$1, b=$2 WHERE c=$3"
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          discard await conn.execDirect(SQL_BAD_EXEC, 1'i32, 2'i32)

    )

  test "queryDirect evaluates side-effecting args exactly once per call":
    # The direct macros fan the arg out to paramOidOf / writeParamOid /
    # writeParamFormat / writeParamValue (3-4 sites depending on cache state).
    # Without a single let-binding upfront each site would re-run the source
    # expression, so a counter-bumping arg would advance 3-4× per call.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var evalCount = 0
      proc nextParam(): int32 =
        inc evalCount
        int32(evalCount)

      # First call: cache-miss path (paramOidOf + writeParamOid + Format + Value).
      let r1 = await conn.queryDirect("SELECT $1::int4 AS v", nextParam())
      doAssert evalCount == 1
      doAssert r1.rows[0].getInt(0) == 1

      # Second call: cache-hit path (paramOidOf + Format + Value).
      let r2 = await conn.queryDirect("SELECT $1::int4 AS v", nextParam())
      doAssert evalCount == 2
      doAssert r2.rows[0].getInt(0) == 2

      await conn.close()

    waitFor t()

  test "execDirect evaluates side-effecting args exactly once per call":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_direct_singleeval")
      discard await conn.exec(
        "CREATE TABLE test_direct_singleeval (id serial PRIMARY KEY, val int NOT NULL)"
      )
      var evalCount = 0
      proc nextParam(): int32 =
        inc evalCount
        int32(evalCount * 10)

      discard await conn.execDirect(
        "INSERT INTO test_direct_singleeval (val) VALUES ($1)", nextParam()
      )
      doAssert evalCount == 1

      discard await conn.execDirect(
        "INSERT INTO test_direct_singleeval (val) VALUES ($1)", nextParam()
      )
      doAssert evalCount == 2

      let qr = await conn.query("SELECT val FROM test_direct_singleeval ORDER BY id")
      doAssert qr.rowCount == 2
      doAssert qr.rows[0].getInt(0) == 10
      doAssert qr.rows[1].getInt(0) == 20

      discard await conn.exec("DROP TABLE test_direct_singleeval")
      await conn.close()

    waitFor t()
