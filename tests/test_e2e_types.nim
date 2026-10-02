import std/[unittest, options, times, strutils]

import ../async_postgres/[async_backend, pg_types]

import ../async_postgres/pg_client
import ../async_postgres/pg_pool
import ../async_postgres/pg_connection

import e2e_common

# Composite types must be registered at top level.
type TstzRecord = object
  at: DateTime

pgComposite(TstzRecord)

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

  test "text DateTime params keep the era, years past 9999 and the instant":
    # The binary encoding has no year spelling or offset to get wrong, so the
    # server comparing the two checks the text literal.
    template fixedZone(name: string, west: static int): Timezone =
      proc fromTime(time: Time): ZonedTime {.gensym, nimcall, gcsafe, raises: [].} =
        ZonedTime(isDst: false, utcOffset: west, time: time)

      proc fromAdj(adjTime: Time): ZonedTime {.gensym, nimcall, gcsafe, raises: [].} =
        ZonedTime(
          isDst: false, utcOffset: west, time: adjTime + initDuration(seconds = west)
        )

      newTimezone(name, fromTime, fromAdj)

    proc values(): seq[DateTime] =
      # LMT +09:18:59: `zzz` would drop the seconds. -04:00 at the lower bound:
      # the local date precedes PostgreSQL's first one.
      let lmt = fixedZone("LMT+09:18:59", -(9 * 3600 + 18 * 60 + 59))
      let west4 = fixedZone("-04:00", 4 * 3600)
      @[
        dateTime(-4713, mNov, 24, zone = utc()),
        dateTime(0, mDec, 31, 23, 59, 59, 999_999_000, utc()),
        dateTime(10000, mJan, 1, zone = utc()),
        dateTime(294276, mDec, 31, 23, 59, 59, 999_999_000, utc()),
        dateTime(1850, mJan, 1, 12, 0, 0, zone = utc()).inZone(lmt),
        dateTime(-4713, mNov, 24, 1, 0, 0, zone = utc()).inZone(west4),
      ]

    proc t() {.async.} =
      let conn = await connect(plainConfig())
      for dt in values():
        let res = await conn.query(
          "SELECT $1::timestamp = $2, $3::timestamptz = $4, $5::date = $6",
          @[
            toPgParam(dt),
            toPgBinaryParam(dt),
            toPgTimestampTzParam(dt),
            toPgBinaryTimestampTzParam(dt),
            toPgDateParam(dt),
            toPgBinaryDateParam(dt),
          ],
        )
        let row = res.rows[0]
        doAssert row.getBool(0) and row.getBool(1) and row.getBool(2), $dt
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
      doAssert $qr.rows[0].getUuid(0) == "550e8400-e29b-41d4-a716-446655440000"
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

suite "E2E: Text Search":
  test "tsvector roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgTsVector("'cat':1A 'dog':3")
      let res = await conn.query("SELECT $1::tsvector", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTsVector(0)
      doAssert $got == "'cat':1A 'dog':3"
      await conn.close()

    waitFor t()

  test "to_tsvector function":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT to_tsvector('english', 'The fat cat sat on the mat')")
      doAssert res.rows.len == 1
      let v = res.rows[0].getTsVector(0)
      let s = $v
      doAssert "'cat'" in s
      doAssert "'fat'" in s
      doAssert "'mat'" in s
      doAssert "'sat'" in s
      await conn.close()

    waitFor t()

  test "tsquery roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let q = PgTsQuery("'fat' & 'rat'")
      let res = await conn.query("SELECT $1::tsquery", @[toPgParam(q)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTsQuery(0)
      doAssert "'fat' & 'rat'" == $got
      await conn.close()

    waitFor t()

  test "full-text search with @@ operator":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT to_tsvector('english', 'the fat cat') @@ to_tsquery('english', 'fat & cat')"
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBool(0) == true
      let res2 = await conn.query(
        "SELECT to_tsvector('english', 'the fat cat') @@ to_tsquery('english', 'fat & dog')"
      )
      doAssert res2.rows[0].getBool(0) == false
      await conn.close()

    waitFor t()

  test "NULL tsvector and tsquery":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::tsvector, NULL::tsquery")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getTsVectorOpt(0).isNone
      doAssert res.rows[0].getTsQueryOpt(1).isNone
      await conn.close()

    waitFor t()

  test "tsvector binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT 'cat:1A dog:3'::tsvector", resultFormat = rfBinary)
      doAssert res.rows.len == 1
      let v = res.rows[0].getTsVector(0)
      let s = $v
      doAssert "'cat'" in s
      doAssert "'dog'" in s
      await conn.close()

    waitFor t()

  test "tsquery binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Binary decoding must render exactly what the server prints in text.
      for src in [
        "fat & rat", "fat | rat", "fat <-> rat", "fat <3> rat", "a <-> b <-> c",
        "!(a & b)", "(a | b) <2> (c & !d)", "cat:*AB & !dog:C", "a <-> (b <2> c)",
        "(a <-> b) <2> c", "a & (b & c)", "'it''s' & x", "'a\\\\b' & c",
      ]:
        let res = await conn.query(
          "SELECT $1::tsquery, $1::tsquery::text",
          @[toPgParam(src)],
          resultFormat = rfBinary,
        )
        doAssert $res.rows[0].getTsQuery(0) == res.rows[0].getStr(1),
          src & ": " & $res.rows[0].getTsQuery(0) & " != " & res.rows[0].getStr(1)
      # plainto_tsquery nests one AND per word.
      let deep = await conn.query(
        "SELECT q, q::text FROM (SELECT plainto_tsquery('simple', " &
          "string_agg('w' || i, ' ')) q FROM generate_series(1, 1500) i) s",
        resultFormat = rfBinary,
      )
      doAssert $deep.rows[0].getTsQuery(0) == deep.rows[0].getStr(1)
      # Stopword removal sums distances past 16384 and wraps int16.
      for src in [
        "cat <16000> the <16000> dog", "cat <16384> the <16384> the <16384> dog"
      ]:
        let res = await conn.query(
          "SELECT q, q::text FROM (SELECT to_tsquery('english', $1) q) s",
          @[toPgParam(src)],
          resultFormat = rfBinary,
        )
        doAssert $res.rows[0].getTsQuery(0) == res.rows[0].getStr(1),
          src & ": " & $res.rows[0].getTsQuery(0) & " != " & res.rows[0].getStr(1)
      let vec = await conn.query(
        "SELECT v, v::text FROM (SELECT $$'it''s':1A 'a\\\\b'$$::tsvector v) s",
        resultFormat = rfBinary,
      )
      doAssert $vec.rows[0].getTsVector(0) == vec.rows[0].getStr(1)
      await conn.close()

    waitFor t()

suite "E2E: XML":
  test "xml roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgXml("<root><item>hello</item></root>")
      let res = await conn.query("SELECT $1::xml", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getXml(0)
      doAssert $got == "<root><item>hello</item></root>"
      await conn.close()

    waitFor t()

  test "xmlparse function":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT xmlparse(CONTENT '<item>test</item>')")
      doAssert res.rows.len == 1
      let v = res.rows[0].getXml(0)
      doAssert "<item>test</item>" == $v
      await conn.close()

    waitFor t()

  test "NULL xml":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::xml")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getXmlOpt(0).isNone
      await conn.close()

    waitFor t()

  test "xml binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT '<root>data</root>'::xml", resultFormat = rfBinary)
      doAssert res.rows.len == 1
      let v = res.rows[0].getXml(0)
      doAssert "<root>data</root>" == $v
      await conn.close()

    waitFor t()

suite "E2E: Interval styles":
  const intervalStyles = ["postgres", "postgres_verbose", "sql_standard", "iso_8601"]

  test "interval text decodes like binary under every IntervalStyle":
    const literals = [
      "0", "1 year", "-1 year", "1 mon", "-11 mons", "1 day", "-1 day", "1 hour",
      "-1 hour", "1.5 seconds", "-0.5 seconds", "0.000001 seconds", "-0.000001 seconds",
      "1 year 2 mons 3 days 04:05:06.789", "-1 year -2 mons -3 days -04:05:06.789",
      "-1 year 2 mons", "1 year -3 days", "-3 days 01:00:00", "3 days -01:00:00",
      "1 mon -1 day 01:00:00", "-1 year 2 mons -3 days 4 hours -5 minutes 6 seconds",
      "1 day -1.5 seconds", "-1 day 1.5 seconds", "1 minute -0.5 seconds",
      "-1 minute 0.5 seconds", "100 hours", "-100 hours 0.5 seconds", "178000000 years",
      "-178000000 years", "2147483647 days", "-2147483648 days", "2147483647 mons",
      "-2147483647 mons", "2562047788:00:54.775807", "-2562047788:00:54.775807",
      "infinity", "-infinity",
    ]
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sql = "SELECT "
      for i, lit in literals:
        if i > 0:
          sql.add(", ")
        sql.add("'" & lit & "'::interval")
      for style in intervalStyles:
        discard await conn.exec("SET IntervalStyle = " & style)
        let text = await conn.query(sql, resultFormat = rfText)
        let bin = await conn.query(sql, resultFormat = rfBinary)
        for i, lit in literals:
          let want = bin.rows[0].getInterval(i)
          var got: PgInterval
          try:
            got = text.rows[0].getInterval(i)
          except PgTypeError as e:
            doAssert false,
              style & " '" & text.rows[0].getStr(i) & "' (" & lit & "): " & e.msg
          doAssert got == want,
            style & " '" & text.rows[0].getStr(i) & "' (" & lit & "): got " & $got &
              ", want " & $want
      await conn.close()

    waitFor t()

  test "interval text param keeps its value under every IntervalStyle":
    let values = [
      PgInterval(months: -12, days: 3, microseconds: 3_600_000_000),
      PgInterval(months: -1, days: 0, microseconds: 1),
      PgInterval(months: 0, days: -3, microseconds: 3_600_000_000),
      PgInterval(months: 14, days: -3, microseconds: -1),
      PgInterval(months: -14, days: -3, microseconds: -14706123456),
      PgInterval(months: 0, days: 0, microseconds: -5),
      PgInterval(months: -12, days: -1, microseconds: 0),
      PgInterval(months: int32.high, days: int32.high, microseconds: int64.high),
      PgInterval(months: int32.low, days: int32.low, microseconds: int64.low),
    ]
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      for style in intervalStyles:
        discard await conn.exec("SET IntervalStyle = " & style)
        for v in values:
          let res = await conn.query(
            "SELECT $1::interval", @[toPgParam(v)], resultFormat = rfBinary
          )
          doAssert res.rows[0].getInterval(0) == v,
            style & " '" & $v & "': got " & $res.rows[0].getInterval(0)
      await conn.close()

    waitFor t()

suite "E2E: Time zones":
  test "timestamptz text decodes like binary for unusual offsets":
    # Before a zone adopted standard time, PostgreSQL prints its LMT offset
    # with seconds (`+09:18:59`); a POSIX zone prints hours past 99.
    const cases = [
      (zone: "Asia/Tokyo", lit: "1880-01-01 00:00:00+00", offset: "+09:18:59"),
      (zone: "Africa/Monrovia", lit: "1970-01-01 00:00:00+00", offset: "-00:44:30"),
      (zone: "Asia/Tokyo", lit: "1880-01-01 00:00:00.5+00", offset: "+09:18:59"),
      (zone: "Asia/Tokyo", lit: "0044-03-15 12:00:00+00 BC", offset: "+09:18:59"),
      (zone: "FOO-100:30", lit: "2000-01-01 00:00:00+00", offset: "+100:30"),
      (zone: "FOO-167:59:60", lit: "2000-01-01 00:00:00+00", offset: "+168"),
      (zone: "FOO+167:59:60", lit: "2000-01-01 00:00:00+00", offset: "-168"),
      # Daylight time defaults to an hour east of standard time.
      (zone: "FOO-167:59:60BAR", lit: "2000-07-01 00:00:00+00", offset: "+169"),
    ]
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      try:
        discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_tstz_rec CASCADE")
        discard
          await conn.simpleQuery("CREATE TYPE test_e2e_tstz_rec AS (at timestamptz)")
        for c in cases:
          discard await conn.exec("SET TimeZone = '" & c.zone & "'")
          let sql =
            "SELECT x, ARRAY[x], tstzrange(x, x + interval '1 day'), " &
            "tstzmultirange(tstzrange(x, x + interval '1 day')), " &
            "ROW(x)::test_e2e_tstz_rec FROM (SELECT '" & c.lit & "'::timestamptz AS x) s"
          let text = (await conn.query(sql, resultFormat = rfText)).rows[0]
          let bin = (await conn.query(sql, resultFormat = rfBinary)).rows[0]
          let ctx = c.zone & " '" & text.getStr(0) & "'"
          doAssert c.offset in text.getStr(0), ctx
          try:
            doAssert text.getTimestampTz(0) == bin.getTimestampTz(0), ctx
            doAssert text.getTimestampTzArray(1) == bin.getTimestampTzArray(1), ctx
            doAssert text.getTsTzRange(2) == bin.getTsTzRange(2), ctx
            doAssert text.getTsTzMultirange(3) == bin.getTsTzMultirange(3), ctx
            doAssert getComposite[TstzRecord](text, 4).at ==
              getComposite[TstzRecord](bin, 4).at, ctx
          except PgTypeError as e:
            doAssert false, ctx & ": " & e.msg
      finally:
        discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_tstz_rec")
        await conn.close()

    waitFor t()

  test "timetz decodes offsets past its input bound":
    # timetz input stops at ±15:59:59, but output carries the session zone's
    # offset, and `AT TIME ZONE` an interval stores any int32. The cast from
    # timestamptz reads the zone at execution; a `'12:00'::timetz` literal is
    # fixed at parse time, so a cached statement would keep the previous zone.
    const cases = [
      (
        zone: "FOO-20",
        expr: "'2000-01-01 00:00+00'::timestamptz::timetz",
        offset: "+20",
      ),
      (
        zone: "FOO-100:30",
        expr: "'2000-01-01 00:00+00'::timestamptz::timetz",
        offset: "+100:30",
      ),
      (
        zone: "FOO-167:59:60BAR",
        expr: "'2000-07-01 00:00+00'::timestamptz::timetz",
        offset: "+169",
      ),
      (
        zone: "UTC",
        expr: "'12:00+00'::timetz AT TIME ZONE interval '596523:14:07'",
        offset: "+596523:14:07",
      ),
      (
        zone: "UTC",
        expr: "'12:00+00'::timetz AT TIME ZONE interval '-596523:14:07'",
        offset: "-596523:14:07",
      ),
    ]
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      try:
        for c in cases:
          discard await conn.exec("SET TimeZone = '" & c.zone & "'")
          let sql = "SELECT x, ARRAY[x] FROM (SELECT " & c.expr & " AS x) s"
          let text = (await conn.query(sql, resultFormat = rfText)).rows[0]
          let bin = (await conn.query(sql, resultFormat = rfBinary)).rows[0]
          let ctx = c.zone & " '" & text.getStr(0) & "'"
          doAssert text.getStr(0).endsWith(c.offset), ctx
          try:
            doAssert text.getTimeTz(0) == bin.getTimeTz(0), ctx
            doAssert text.getTimeTzArray(1) == bin.getTimeTzArray(1), ctx
          except PgTypeError as e:
            doAssert false, ctx & ": " & e.msg
      finally:
        await conn.close()

    waitFor t()
