import std/[unittest, options, math, importutils, net, times]

import ../async_postgres/[async_backend, pg_types]

when hasAsyncDispatch:
  import std/strutils

import ../async_postgres/pg_client
import ../async_postgres/pg_pool
import ../async_postgres/pg_connection

import e2e_common

privateAccess(PgConnection)

suite "E2E: Type Roundtrip":
  test "integer types roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::int4, $2::int8", @[toPgParam("42"), toPgParam("9999999999")]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 42'i32
      doAssert res.rows[0].getInt64(1) == 9999999999'i64
      await conn.close()

    waitFor t()

  test "float roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::float8", @[toPgParam("3.14")])
      doAssert res.rows.len == 1
      doAssert abs(res.rows[0].getFloat(0) - 3.14) < 1e-10
      await conn.close()

    waitFor t()

  test "bool roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT $1::bool, $2::bool", @[toPgParam("t"), toPgParam("f")])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBool(0) == true
      doAssert res.rows[0].getBool(1) == false
      await conn.close()

    waitFor t()

  test "text roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::text", @[toPgParam("hello world")])
      doAssert res.rows[0].getStr(0) == "hello world"
      await conn.close()

    waitFor t()

  test "NULL handling":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::text, $1::text", @[toPgParam("ok")])
      doAssert res.rows[0].isNull(0)
      doAssert not res.rows[0].isNull(1)
      doAssert res.rows[0].getStr(1) == "ok"
      await conn.close()

    waitFor t()

  test "NULL parameter with Option[T]":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::text IS NULL, $2::int4", @[toPgParam(none(string)), toPgParam("7")]
      )
      doAssert res.rows[0].getStr(0) == "t"
      doAssert res.rows[0].getInt(1) == 7'i32
      await conn.close()

    waitFor t()

suite "E2E: PgParam Typed Parameters":
  test "exec and query with toPgParam (no explicit casts)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_pgparam")
      discard
        await conn.exec("CREATE TABLE test_pgparam (id int, name text, active bool)")

      # Insert using PgParam — OIDs let PostgreSQL infer types without $1::type casts
      discard await conn.exec(
        "INSERT INTO test_pgparam (id, name, active) VALUES ($1, $2, $3)",
        @[toPgParam(42'i32), toPgParam("alice"), toPgParam(true)],
      )

      let res = await conn.query(
        "SELECT id, name, active FROM test_pgparam WHERE id = $1", @[toPgParam(42'i32)]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 42'i32
      doAssert res.rows[0].getStr(1) == "alice"
      doAssert res.rows[0].getBool(2) == true

      discard await conn.exec("DROP TABLE test_pgparam")
      await conn.close()

    waitFor t()

  test "query with int (platform int) parameter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1 + 1", @[toPgParam(99)])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt64(0) == 100'i64
      await conn.close()

    waitFor t()

  test "query with NULL via Option[T]":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1 IS NULL, $2", @[toPgParam(none(string)), toPgParam("ok")]
      )
      doAssert res.rows[0].getStr(0) == "t"
      doAssert res.rows[0].getStr(1) == "ok"
      await conn.close()

    waitFor t()

  test "exec with int64 and float64 params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT $1, $2", @[toPgParam(9999999999'i64), toPgParam(3.14)])
      doAssert res.rows[0].getInt64(0) == 9999999999'i64
      doAssert abs(res.rows[0].getFloat(1) - 3.14) < 1e-10
      await conn.close()

    waitFor t()

  test "pool exec and query with PgParam":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_pgparam_pool")
      discard await pool.exec("CREATE TABLE test_pgparam_pool (id int, val text)")
      discard await pool.exec(
        "INSERT INTO test_pgparam_pool (id, val) VALUES ($1, $2)",
        @[toPgParam(1'i32), toPgParam("pooled")],
      )
      let res = await pool.query(
        "SELECT val FROM test_pgparam_pool WHERE id = $1", @[toPgParam(1'i32)]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pooled"
      discard await pool.exec("DROP TABLE test_pgparam_pool")
      await pool.close()

    waitFor t()

  test "execute prepared statement with PgParam":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt = await conn.prepare("pgparam_stmt", "SELECT $1::int4 + $2::int4")
      let res = await stmt.execute(@[toPgParam(10'i32), toPgParam(20'i32)])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 30'i32
      await stmt.close()
      await conn.close()

    waitFor t()

suite "E2E: Extended Type Roundtrip":
  test "bytea roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE test_bytea (data bytea)")
      let raw = @[0x00'u8, 0xDE, 0xAD, 0xBE, 0xEF, 0xFF]
      discard
        await conn.exec("INSERT INTO test_bytea (data) VALUES ($1)", @[toPgParam(raw)])
      let res = await conn.query(
        "SELECT data FROM test_bytea WHERE data = $1", @[toPgParam(raw)]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBytes(0) == raw
      await conn.close()

    waitFor t()

  test "bytea roundtrip with backslash and hex-prefix patterns":
    # Regression: text-format bytea would either error out or silently
    # collapse \\ → \ and decode \x-prefixed inputs as hex.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE test_bytea_esc (data bytea)")
      let cases: seq[seq[byte]] = @[
        @[0x5C'u8, 0x5C], # \\
        @[0x5C'u8, 0x78, 0x61, 0x62], # \xab
        @[0x5C'u8, 0x30, 0x30, 0x30], # \000
        @[0x00'u8, 0x01, 0x02, 0x7F, 0x80, 0xFE, 0xFF],
        @[], # empty
      ]
      for input in cases:
        discard await conn.exec(
          "INSERT INTO test_bytea_esc (data) VALUES ($1)", @[toPgParam(input)]
        )
        let qr = await conn.query(
          "SELECT data FROM test_bytea_esc WHERE data = $1", @[toPgParam(input)]
        )
        doAssert qr.rows.len == 1
        doAssert qr.rows[0].getBytes(0) == input
        discard await conn.exec("DELETE FROM test_bytea_esc")
      await conn.close()

    waitFor t()

  test "bytea roundtrip via toPgParamInline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE test_bytea_inl (data bytea)")
      let short = @[0x5C'u8, 0x5C, 0x00, 0xFF]
      var long = newSeq[byte](128)
      for i in 0 ..< long.len:
        long[i] = byte(i)
      discard await conn.exec(
        "INSERT INTO test_bytea_inl (data) VALUES ($1)", @[toPgParamInline(short)]
      )
      discard await conn.exec(
        "INSERT INTO test_bytea_inl (data) VALUES ($1)", @[toPgParamInline(long)]
      )
      let qr =
        await conn.query("SELECT data FROM test_bytea_inl ORDER BY octet_length(data)")
      doAssert qr.rows.len == 2
      doAssert qr.rows[0].getBytes(0) == short
      doAssert qr.rows[1].getBytes(0) == long
      await conn.close()

    waitFor t()

  test "timestamp roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2025, mMar, 15, 10, 30, 45, zone = utc())
      let res = await conn.query("SELECT $1::timestamp", @[toPgParam(dt)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTimestamp(0)
      doAssert got.year == 2025
      doAssert got.month == mMar
      doAssert got.monthday == 15
      doAssert got.hour == 10
      doAssert got.minute == 30
      doAssert got.second == 45
      await conn.close()

    waitFor t()

  test "date roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT '2025-06-15'::date")
      doAssert res.rows.len == 1
      let got = res.rows[0].getDate(0)
      doAssert got.year == 2025
      doAssert got.month == mJun
      doAssert got.monthday == 15
      await conn.close()

    waitFor t()

  test "time roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let tm = PgTime(hour: 14, minute: 30, second: 45, microsecond: 123456)
      let res = await conn.query("SELECT $1::time", @[toPgParam(tm)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTime(0)
      doAssert got.hour == 14
      doAssert got.minute == 30
      doAssert got.second == 45
      doAssert got.microsecond == 123456
      await conn.close()

    waitFor t()

  test "timetz roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let tm =
        PgTimeTz(hour: 14, minute: 30, second: 45, microsecond: 0, utcOffset: 18000)
      let res = await conn.query("SELECT $1::timetz", @[toPgParam(tm)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTimeTz(0)
      doAssert got.hour == 14
      doAssert got.minute == 30
      doAssert got.second == 45
      doAssert got.utcOffset == 18000
      await conn.close()

    waitFor t()

  test "date param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2025, mJun, 15, 0, 0, 0, zone = utc())
      let res = await conn.query("SELECT $1::date", @[toPgDateParam(dt)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getDate(0)
      doAssert got.year == 2025
      doAssert got.month == mJun
      doAssert got.monthday == 15
      await conn.close()

    waitFor t()

  test "timestamptz roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2025, mMar, 15, 10, 30, 45, zone = utc())
      let res = await conn.query("SELECT $1::timestamptz", @[toPgTimestampTzParam(dt)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTimestampTz(0)
      doAssert got.utc().year == 2025
      doAssert got.utc().month == mMar
      doAssert got.utc().monthday == 15
      doAssert got.utc().hour == 10
      doAssert got.utc().minute == 30
      doAssert got.utc().second == 45
      await conn.close()

    waitFor t()

  test "UUID roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let uuid = PgUuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
      let res = await conn.query("SELECT $1::uuid", @[toPgParam(uuid)])
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getUuid(0) == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
      await conn.close()

    waitFor t()

  test "int16 and float32 roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::int2, $2::float4", @[toPgParam(42'i16), toPgParam(3.14'f32)]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 42'i32
      doAssert abs(res.rows[0].getFloat(1) - 3.14) < 0.01
      await conn.close()

    waitFor t()

  test "empty string vs NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE test_empty_null (val text)")
      discard await conn.exec(
        "INSERT INTO test_empty_null (val) VALUES ($1)", @[toPgParam("")]
      )
      discard await conn.exec(
        "INSERT INTO test_empty_null (val) VALUES ($1)", @[toPgParam(none(string))]
      )
      let res =
        await conn.query("SELECT val FROM test_empty_null ORDER BY val NULLS LAST")
      doAssert res.rows.len == 2
      doAssert not res.rows[0].isNull(0)
      doAssert res.rows[0].getStr(0) == ""
      doAssert res.rows[1].isNull(0)
      await conn.close()

    waitFor t()

  test "special characters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let values = @["こんにちは世界", "it's a test", "back\\slash", "NULL"]
      for v in values:
        let res = await conn.query("SELECT $1::text", @[toPgParam(v)])
        doAssert res.rows[0].getStr(0) == v
      await conn.close()

    waitFor t()

suite "E2E: JSON and Numeric":
  test "JSON/JSONB as text":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("""SELECT '{"k":"v"}'::json, '{"n":42}'::jsonb""")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "{\"k\":\"v\"}"
      doAssert res.rows[0].getStr(1) == "{\"n\": 42}"
      await conn.close()

    waitFor t()

  test "numeric precision":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT 12345.6789::numeric, 0.00001::numeric, 99999999999999999.12345678901234567890::numeric"
      )
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "12345.6789"
      doAssert $res.rows[0].getNumeric(1) == "0.00001"
      # Precision preserved - float64 would lose digits here
      doAssert $res.rows[0].getNumeric(2) == "99999999999999999.12345678901234567890"
      await conn.close()

    waitFor t()

  test "numeric negative and NaN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT -123.456::numeric, 'NaN'::numeric")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "-123.456"
      doAssert $res.rows[0].getNumeric(1) == "NaN"
      await conn.close()

    waitFor t()

  test "numeric fixed precision":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 1.5::numeric(10,4), 0::numeric(8,2)")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "1.5000"
      doAssert $res.rows[0].getNumeric(1) == "0.00"
      await conn.close()

    waitFor t()

  test "numeric NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::numeric")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getNumericOpt(0) == none(PgNumeric)
      await conn.close()

    waitFor t()

  test "numeric as parameter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_numeric_param")
      discard await conn.exec("CREATE TABLE test_numeric_param (val numeric(20,8))")
      discard await conn.exec(
        "INSERT INTO test_numeric_param VALUES ($1)",
        @[toPgParam(parsePgNumeric("123456789012.56789012"))],
      )
      let res = await conn.query("SELECT val FROM test_numeric_param")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "123456789012.56789012"
      discard await conn.exec("DROP TABLE test_numeric_param")
      await conn.close()

    waitFor t()

  test "numeric large integer":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 99999999999999999999999999999::numeric")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "99999999999999999999999999999"
      await conn.close()

    waitFor t()

suite "E2E: Money":
  test "money binary param and binary result roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      for v in [
        initPgMoney(0),
        initPgMoney(123456),
        initPgMoney(-123456),
        initPgMoney(low(int64)),
        initPgMoney(high(int64)),
      ]:
        let res =
          await conn.query("SELECT $1::money", @[toPgParam(v)], resultFormat = rfBinary)
        doAssert res.rows.len == 1
        doAssert res.rows[0].getMoney(0) == v
      await conn.close()

    waitFor t()

  test "money text result from server":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Force C-locale formatting so the test is deterministic regardless of
      # the server's lc_monetary setting.
      discard await conn.exec("SET lc_monetary = 'C'")
      let res = await conn.query("SELECT 1234.56::money")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getMoney(0) == initPgMoney(123456)
      await conn.close()

    waitFor t()

  test "money stored in table":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_money")
      discard await conn.exec("CREATE TABLE test_money (id int, val money)")
      discard await conn.exec(
        "INSERT INTO test_money VALUES (1, $1), (2, $2), (3, $3)",
        @[
          toPgParam(initPgMoney(0)),
          toPgParam(initPgMoney(-123456)),
          toPgParam(initPgMoney(99999999)),
        ],
      )
      let res = await conn.query(
        "SELECT val FROM test_money ORDER BY id", resultFormat = rfBinary
      )
      doAssert res.rows.len == 3
      doAssert res.rows[0].getMoney(0) == initPgMoney(0)
      doAssert res.rows[1].getMoney(0) == initPgMoney(-123456)
      doAssert res.rows[2].getMoney(0) == initPgMoney(99999999)
      discard await conn.exec("DROP TABLE test_money")
      await conn.close()

    waitFor t()

  test "money NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::money", resultFormat = rfBinary)
      doAssert res.rows.len == 1
      doAssert res.rows[0].getMoneyOpt(0) == none(PgMoney)
      await conn.close()

    waitFor t()

  test "money array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let values = @[initPgMoney(100), initPgMoney(-50), initPgMoney(999999)]
      let res = await conn.query(
        "SELECT $1::money[]", @[toPgParam(values)], resultFormat = rfBinary
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getMoneyArray(0) == values
      await conn.close()

    waitFor t()

suite "E2E: Binary Format":
  test "binary results for int types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT 42::int2, 123456::int4, 9999999999::int8", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      let row = qr.rows[0]
      doAssert row.getInt(0) == 42'i32 # int2 promoted via getInt
      doAssert row.getInt(1) == 123456'i32
      doAssert row.getInt64(2) == 9999999999'i64
      # getInt64 should also work on int2/int4 columns (promotion)
      doAssert row.getInt64(0) == 42'i64
      doAssert row.getInt64(1) == 123456'i64
      await conn.close()

    waitFor t()

  test "binary results for float types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr =
        await conn.query("SELECT 3.14::float8, 1.5::float4", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let row = qr.rows[0]
      doAssert abs(row.getFloat(0) - 3.14) < 1e-10
      doAssert abs(row.getFloat(1) - 1.5) < 1e-5
      await conn.close()

    waitFor t()

  test "binary results for bool":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT true, false", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBool(0) == true
      doAssert qr.rows[0].getBool(1) == false
      await conn.close()

    waitFor t()

  test "binary results for text":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT 'hello'::text, 'world'::varchar", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getStr(0) == "hello"
      doAssert qr.rows[0].getStr(1) == "world"
      await conn.close()

    waitFor t()

  test "binary results for bytea":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT '\\xDEADBEEF'::bytea", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBytes(0) == @[0xDE'u8, 0xAD, 0xBE, 0xEF]
      await conn.close()

    waitFor t()

  test "binary results for timestamp":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT '2024-01-15 10:30:00'::timestamp", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      let dt = qr.rows[0].getTimestamp(0)
      doAssert dt.year == 2024
      doAssert dt.month == mJan
      doAssert dt.monthday == 15
      doAssert dt.hour == 10
      doAssert dt.minute == 30
      await conn.close()

    waitFor t()

  test "binary results for date":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT '2024-01-15'::date", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let dt = qr.rows[0].getDate(0)
      doAssert dt.year == 2024
      doAssert dt.month == mJan
      doAssert dt.monthday == 15
      await conn.close()

    waitFor t()

  test "binary results for uuid":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      let data = qr.rows[0].getBytes(0)
      doAssert data.len == 16
      doAssert data[0] == 0x55'u8
      doAssert data[1] == 0x0e'u8
      await conn.close()

    waitFor t()

  test "binary params and binary results roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[
        toPgBinaryParam(42'i32), toPgBinaryParam(9999999999'i64), toPgBinaryParam(true)
      ]
      let qr = await conn.query(
        "SELECT $1::int4, $2::int8, $3::bool", params, resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      doAssert qr.rows[0].getInt64(1) == 9999999999'i64
      doAssert qr.rows[0].getBool(2) == true
      await conn.close()

    waitFor t()

  test "binary params with text results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[toPgBinaryParam(42'i32)]
      let qr = await conn.query("SELECT $1::int4", params)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      await conn.close()

    waitFor t()

  test "text params with binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[toPgParam(42'i32)]
      let qr = await conn.query("SELECT $1::int4", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      await conn.close()

    waitFor t()

  test "NULL handling in binary mode":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr =
        await conn.query("SELECT NULL::int4, NULL::text", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].isNull(0)
      doAssert qr.rows[0].isNull(1)
      await conn.close()

    waitFor t()

  test "prepared statement with binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt = await conn.prepare("bin_stmt", "SELECT $1::int4 + 10")
      let qr = await stmt.execute(@[toPgBinaryParam(32'i32)], resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      await stmt.close()
      await conn.close()

    waitFor t()

  test "binary timestamp param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
      let params = @[toPgBinaryParam(dt)]
      let qr = await conn.query("SELECT $1::timestamp", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let r = qr.rows[0].getTimestamp(0)
      doAssert r.year == 2024
      doAssert r.month == mJan
      doAssert r.monthday == 15
      doAssert r.hour == 10
      doAssert r.minute == 30
      await conn.close()

    waitFor t()

  test "binary time param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let tm = PgTime(hour: 14, minute: 30, second: 45, microsecond: 123456)
      let params = @[toPgBinaryParam(tm)]
      let qr = await conn.query("SELECT $1::time", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let got = qr.rows[0].getTime(0)
      doAssert got == tm
      await conn.close()

    waitFor t()

  test "binary timetz param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let tm =
        PgTimeTz(hour: 14, minute: 30, second: 45, microsecond: 0, utcOffset: 18000)
      let params = @[toPgBinaryParam(tm)]
      let qr = await conn.query("SELECT $1::timetz", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let got = qr.rows[0].getTimeTz(0)
      doAssert got == tm
      await conn.close()

    waitFor t()

  test "binary date param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2024, mJan, 15, 0, 0, 0, 0, utc())
      let params = @[toPgBinaryDateParam(dt)]
      let qr = await conn.query("SELECT $1::date", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let got = qr.rows[0].getDate(0)
      doAssert got.year == 2024
      doAssert got.month == mJan
      doAssert got.monthday == 15
      await conn.close()

    waitFor t()

  test "binary timestamptz param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
      let params = @[toPgBinaryTimestampTzParam(dt)]
      let qr =
        await conn.query("SELECT $1::timestamptz", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let got = qr.rows[0].getTimestampTz(0)
      doAssert got.year == 2024
      doAssert got.month == mJan
      doAssert got.monthday == 15
      doAssert got.hour == 10
      doAssert got.minute == 30
      await conn.close()

    waitFor t()

  test "binary float roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[toPgBinaryParam(3.14159265358979)]
      let qr = await conn.query("SELECT $1::float8", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert abs(qr.rows[0].getFloat(0) - 3.14159265358979) < 1e-14
      await conn.close()

    waitFor t()

  test "binary bytea param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let data = @[0xDE'u8, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]
      let params = @[toPgBinaryParam(data)]
      let qr = await conn.query("SELECT $1::bytea", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBytes(0) == data
      await conn.close()

    waitFor t()
