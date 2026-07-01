import std/[unittest, options, strutils, tables, math, importutils, net, json]
from std/times import
  DateTime, dateTime, mMar, mJun, mJan, mDec, utc, year, month, monthday, hour, minute,
  second, toTime, toUnix, nanosecond

import
  ../async_postgres/[async_backend, pg_protocol, pg_types, pg_client, pg_connection]

import e2e_common

privateAccess(PgConnection)

# User-defined type definitions for e2e tests (macros must be at top level)
type
  TestPoint = object
    x: float64
    y: float64

  TestPerson = object
    name: string
    age: int32
    score: float64

  TestNullable = object
    name: string
    age: Option[int32]
    note: Option[string]

pgComposite(TestPoint)
pgComposite(TestPerson)
pgComposite(TestNullable)

type TestMood = enum
  tmHappy = "happy"
  tmSad = "sad"
  tmOk = "ok"

pgEnum(TestMood)

type TestPosInt = distinct int32

pgDomain(TestPosInt, int32)

suite "E2E: Array Types":
  test "int4 array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::int4[]", @[toPgParam(@[1'i32, 2, 3])])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArray(0) == @[1'i32, 2, 3]
      await conn.close()

    waitFor t()

  test "int8 array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT $1::int8[]", @[toPgParam(@[9999999999'i64, -1'i64])])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt64Array(0) == @[9999999999'i64, -1'i64]
      await conn.close()

    waitFor t()

  test "bool array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT $1::bool[]", @[toPgParam(@[true, false, true])])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBoolArray(0) == @[true, false, true]
      await conn.close()

    waitFor t()

  test "text array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::text[]", @[toPgParam(@["hello", "world", "foo bar"])]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStrArray(0) == @["hello", "world", "foo bar"]
      await conn.close()

    waitFor t()

  test "text array with special characters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::text[]", @[toPgParam(@["a\"b", "c\\d", "e,f", ""])]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStrArray(0) == @["a\"b", "c\\d", "e,f", ""]
      await conn.close()

    waitFor t()

  test "empty array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::int4[]", @[toPgParam(newSeq[int32]())])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArray(0).len == 0
      await conn.close()

    waitFor t()

  test "float8 array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::float8[]", @[toPgParam(@[3.14, 2.72])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getFloatArray(0)
      doAssert abs(arr[0] - 3.14) < 1e-10
      doAssert abs(arr[1] - 2.72) < 1e-10
      await conn.close()

    waitFor t()

  test "NULL array column":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::int4[]", @[toPgParam(none(seq[int32]))])
      doAssert res.rows.len == 1
      doAssert res.rows[0].isNull(0)
      doAssert res.rows[0].getIntArrayOpt(0).isNone
      await conn.close()

    waitFor t()

suite "E2E: User-Defined Types":
  test "composite roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_point CASCADE")
      discard
        await conn.simpleQuery("CREATE TYPE test_e2e_point AS (x float8, y float8)")

      let res = await conn.query("SELECT ROW(1.5, 2.5)::test_e2e_point")
      doAssert res.rows.len == 1
      let got = getComposite[TestPoint](res.rows[0], 0)
      doAssert got.x == 1.5
      doAssert got.y == 2.5

      discard await conn.simpleQuery("DROP TYPE test_e2e_point")
      await conn.close()

    waitFor t()

  test "composite with strings":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_person CASCADE")
      discard await conn.simpleQuery(
        "CREATE TYPE test_e2e_person AS (name text, age int4, score float8)"
      )

      let res = await conn.query("SELECT ROW('Alice', 30, 95.5)::test_e2e_person")
      doAssert res.rows.len == 1
      let got = getComposite[TestPerson](res.rows[0], 0)
      doAssert got.name == "Alice"
      doAssert got.age == 30'i32
      doAssert got.score == 95.5

      discard await conn.simpleQuery("DROP TYPE test_e2e_person")
      await conn.close()

    waitFor t()

  test "composite NULL fields":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_nullable CASCADE")
      discard await conn.simpleQuery(
        "CREATE TYPE test_e2e_nullable AS (name text, age int4, note text)"
      )

      let res = await conn.query("SELECT ROW('Bob', NULL, NULL)::test_e2e_nullable")
      doAssert res.rows.len == 1
      let got = getComposite[TestNullable](res.rows[0], 0)
      doAssert got.name == "Bob"
      doAssert got.age.isNone
      doAssert got.note.isNone

      discard await conn.simpleQuery("DROP TYPE test_e2e_nullable")
      await conn.close()

    waitFor t()

  test "NULL composite":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_point2 CASCADE")
      discard
        await conn.simpleQuery("CREATE TYPE test_e2e_point2 AS (x float8, y float8)")

      let res = await conn.query("SELECT NULL::test_e2e_point2")
      doAssert res.rows.len == 1
      let got = getCompositeOpt[TestPoint](res.rows[0], 0)
      doAssert got.isNone

      discard await conn.simpleQuery("DROP TYPE test_e2e_point2")
      await conn.close()

    waitFor t()

  test "composite param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_point3 CASCADE")
      discard
        await conn.simpleQuery("CREATE TYPE test_e2e_point3 AS (x float8, y float8)")

      let v = TestPoint(x: 3.14, y: 2.72)
      let res = await conn.query("SELECT $1::test_e2e_point3", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = getComposite[TestPoint](res.rows[0], 0)
      doAssert got.x == 3.14
      doAssert got.y == 2.72

      discard await conn.simpleQuery("DROP TYPE test_e2e_point3")
      await conn.close()

    waitFor t()

  test "composite literal NULL string param preserved":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_nullable2 CASCADE")
      discard await conn.simpleQuery(
        "CREATE TYPE test_e2e_nullable2 AS (name text, age int4, note text)"
      )

      # The literal string "NULL" must round-trip as a string, not become SQL NULL.
      let v = TestNullable(name: "NULL", age: some(7'i32), note: some("null"))
      let res = await conn.query("SELECT $1::test_e2e_nullable2", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = getComposite[TestNullable](res.rows[0], 0)
      doAssert got.name == "NULL"
      doAssert got.age == some(7'i32)
      doAssert got.note == some("null")

      discard await conn.simpleQuery("DROP TYPE test_e2e_nullable2")
      await conn.close()

    waitFor t()

  test "enum roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_mood CASCADE")
      discard await conn.simpleQuery(
        "CREATE TYPE test_e2e_mood AS ENUM ('happy', 'sad', 'ok')"
      )

      let res = await conn.query("SELECT 'happy'::test_e2e_mood")
      doAssert res.rows.len == 1
      let got = getEnum[TestMood](res.rows[0], 0)
      doAssert got == tmHappy

      discard await conn.simpleQuery("DROP TYPE test_e2e_mood")
      await conn.close()

    waitFor t()

  test "enum param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_mood2 CASCADE")
      discard await conn.simpleQuery(
        "CREATE TYPE test_e2e_mood2 AS ENUM ('happy', 'sad', 'ok')"
      )

      let res = await conn.query("SELECT $1::test_e2e_mood2", @[toPgParam(tmSad)])
      doAssert res.rows.len == 1
      let got = getEnum[TestMood](res.rows[0], 0)
      doAssert got == tmSad

      discard await conn.simpleQuery("DROP TYPE test_e2e_mood2")
      await conn.close()

    waitFor t()

  test "enum in table":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TABLE IF EXISTS test_e2e_mood_tbl CASCADE")
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_mood3 CASCADE")
      discard await conn.simpleQuery(
        "CREATE TYPE test_e2e_mood3 AS ENUM ('happy', 'sad', 'ok')"
      )
      discard await conn.simpleQuery(
        "CREATE TABLE test_e2e_mood_tbl (id serial, mood test_e2e_mood3)"
      )
      discard
        await conn.simpleQuery("INSERT INTO test_e2e_mood_tbl (mood) VALUES ('ok')")

      let res = await conn.query("SELECT mood FROM test_e2e_mood_tbl WHERE id = 1")
      doAssert res.rows.len == 1
      let got = getEnum[TestMood](res.rows[0], 0)
      doAssert got == tmOk

      discard await conn.simpleQuery("DROP TABLE test_e2e_mood_tbl")
      discard await conn.simpleQuery("DROP TYPE test_e2e_mood3")
      await conn.close()

    waitFor t()

  test "NULL enum":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TYPE IF EXISTS test_e2e_mood4 CASCADE")
      discard await conn.simpleQuery(
        "CREATE TYPE test_e2e_mood4 AS ENUM ('happy', 'sad', 'ok')"
      )

      let res = await conn.query("SELECT NULL::test_e2e_mood4")
      doAssert res.rows.len == 1
      let got = getEnumOpt[TestMood](res.rows[0], 0)
      doAssert got.isNone

      discard await conn.simpleQuery("DROP TYPE test_e2e_mood4")
      await conn.close()

    waitFor t()

  test "domain roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP DOMAIN IF EXISTS test_e2e_posint CASCADE")
      discard await conn.simpleQuery(
        "CREATE DOMAIN test_e2e_posint AS int4 CHECK (VALUE > 0)"
      )

      let res =
        await conn.query("SELECT $1::test_e2e_posint", @[toPgParam(TestPosInt(42))])
      doAssert res.rows.len == 1
      let got = getDomain[TestPosInt](res.rows[0], 0)
      doAssert int32(got) == 42'i32

      discard await conn.simpleQuery("DROP DOMAIN test_e2e_posint")
      await conn.close()

    waitFor t()

  test "domain constraint violation":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP DOMAIN IF EXISTS test_e2e_posint2 CASCADE")
      discard await conn.simpleQuery(
        "CREATE DOMAIN test_e2e_posint2 AS int4 CHECK (VALUE > 0)"
      )

      var raised = false
      try:
        discard
          await conn.query("SELECT $1::test_e2e_posint2", @[toPgParam(TestPosInt(-1))])
      except PgError:
        raised = true
      doAssert raised

      discard await conn.simpleQuery("DROP DOMAIN test_e2e_posint2")
      await conn.close()

    waitFor t()

suite "E2E: Network Types":
  test "inet roundtrip IPv4":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgInet(address: parseIpAddress("192.168.1.5"), mask: 24)
      let res = await conn.query("SELECT $1::inet", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInet(0)
      doAssert got.address == parseIpAddress("192.168.1.5")
      doAssert got.mask == 24
      await conn.close()

    waitFor t()

  test "inet roundtrip IPv6":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgInet(address: parseIpAddress("::1"), mask: 128)
      let res = await conn.query("SELECT $1::inet", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInet(0)
      doAssert got.address == parseIpAddress("::1")
      doAssert got.mask == 128
      await conn.close()

    waitFor t()

  test "inet host address without mask":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgInet(address: parseIpAddress("10.0.0.1"), mask: 32)
      let res = await conn.query("SELECT $1::inet", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInet(0)
      doAssert got.address == parseIpAddress("10.0.0.1")
      doAssert got.mask == 32
      await conn.close()

    waitFor t()

  test "cidr roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
      let res = await conn.query("SELECT $1::cidr", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getCidr(0)
      doAssert got.address == parseIpAddress("10.0.0.0")
      doAssert got.mask == 8
      await conn.close()

    waitFor t()

  test "macaddr roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgMacAddr("08:00:2b:01:02:03")
      let res = await conn.query("SELECT $1::macaddr", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getMacAddr(0)
      doAssert $got == "08:00:2b:01:02:03"
      await conn.close()

    waitFor t()

  test "macaddr8 roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgMacAddr8("08:00:2b:01:02:03:04:05")
      let res = await conn.query("SELECT $1::macaddr8", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getMacAddr8(0)
      doAssert $got == "08:00:2b:01:02:03:04:05"
      await conn.close()

    waitFor t()

  test "NULL network types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT NULL::inet, NULL::cidr, NULL::macaddr, NULL::macaddr8")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInetOpt(0).isNone
      doAssert res.rows[0].getCidrOpt(1).isNone
      doAssert res.rows[0].getMacAddrOpt(2).isNone
      doAssert res.rows[0].getMacAddr8Opt(3).isNone
      await conn.close()

    waitFor t()

  test "inet in table":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("DROP TABLE IF EXISTS test_e2e_inet_tbl CASCADE")
      discard
        await conn.simpleQuery("CREATE TABLE test_e2e_inet_tbl (id serial, addr inet)")

      let v = PgInet(address: parseIpAddress("192.168.0.1"), mask: 24)
      discard await conn.exec(
        "INSERT INTO test_e2e_inet_tbl (addr) VALUES ($1)", @[toPgParam(v)]
      )
      let res = await conn.query("SELECT addr FROM test_e2e_inet_tbl WHERE id = 1")
      doAssert res.rows.len == 1
      let got = res.rows[0].getInet(0)
      doAssert got.address == parseIpAddress("192.168.0.1")
      doAssert got.mask == 24

      discard await conn.simpleQuery("DROP TABLE test_e2e_inet_tbl")
      await conn.close()

    waitFor t()

suite "E2E: Geometric Types":
  test "point roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgPoint(x: 1.5, y: 2.5)
      let res = await conn.query("SELECT $1::point", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getPoint(0)
      doAssert got.x == 1.5
      doAssert got.y == 2.5
      await conn.close()

    waitFor t()

  test "line roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgLine(a: 1.0, b: -1.0, c: 0.0)
      let res = await conn.query("SELECT $1::line", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getLine(0)
      doAssert got.a == 1.0
      doAssert got.b == -1.0
      doAssert got.c == 0.0
      await conn.close()

    waitFor t()

  test "lseg roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 3.0, y: 4.0))
      let res = await conn.query("SELECT $1::lseg", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getLseg(0)
      doAssert got.p1.x == 0.0
      doAssert got.p1.y == 0.0
      doAssert got.p2.x == 3.0
      doAssert got.p2.y == 4.0
      await conn.close()

    waitFor t()

  test "box roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0))
      let res = await conn.query("SELECT $1::box", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getBox(0)
      doAssert got.high.x == 3.0
      doAssert got.high.y == 4.0
      doAssert got.low.x == 1.0
      doAssert got.low.y == 2.0
      await conn.close()

    waitFor t()

  test "path roundtrip closed":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgPath(
        closed: true,
        points:
          @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)],
      )
      let res = await conn.query("SELECT $1::path", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getPath(0)
      doAssert got.closed == true
      doAssert got.points.len == 3
      doAssert got.points[0].x == 0.0
      doAssert got.points[1].x == 1.0
      doAssert got.points[2].y == 1.0
      await conn.close()

    waitFor t()

  test "path roundtrip open":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgPath(
        closed: false, points: @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 5.0, y: 5.0)]
      )
      let res = await conn.query("SELECT $1::path", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getPath(0)
      doAssert got.closed == false
      doAssert got.points.len == 2
      doAssert got.points[0].x == 0.0
      doAssert got.points[1].x == 5.0
      await conn.close()

    waitFor t()

  test "polygon roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgPolygon(
        points: @[
          PgPoint(x: 0.0, y: 0.0),
          PgPoint(x: 4.0, y: 0.0),
          PgPoint(x: 4.0, y: 3.0),
          PgPoint(x: 0.0, y: 3.0),
        ]
      )
      let res = await conn.query("SELECT $1::polygon", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getPolygon(0)
      doAssert got.points.len == 4
      doAssert got.points[0].x == 0.0
      doAssert got.points[2].x == 4.0
      doAssert got.points[2].y == 3.0
      await conn.close()

    waitFor t()

  test "circle roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0)
      let res = await conn.query("SELECT $1::circle", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getCircle(0)
      doAssert got.center.x == 1.0
      doAssert got.center.y == 2.0
      doAssert got.radius == 5.0
      await conn.close()

    waitFor t()

  test "NULL geometric types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::point, NULL::line, NULL::circle")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getPointOpt(0).isNone
      doAssert res.rows[0].getLineOpt(1).isNone
      doAssert res.rows[0].getCircleOpt(2).isNone
      await conn.close()

    waitFor t()

  test "geometric distance computation":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT point '(1,2)' <-> point '(4,6)'")
      doAssert res.rows.len == 1
      let dist = parseFloat(res.rows[0].getStr(0))
      doAssert abs(dist - 5.0) < 1e-10
      await conn.close()

    waitFor t()

suite "E2E: Range Types":
  test "int4range roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = rangeOf(1'i32, 10'i32)
      let res = await conn.query("SELECT $1::int4range", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Range(0)
      doAssert got.hasLower
      doAssert got.hasUpper
      doAssert got.lower.value == 1'i32
      doAssert got.lower.inclusive == true
      doAssert got.upper.value == 10'i32
      doAssert got.upper.inclusive == false
      await conn.close()

    waitFor t()

  test "int8range roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = rangeOf(100'i64, 999'i64)
      let res = await conn.query("SELECT $1::int8range", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt8Range(0)
      doAssert got.lower.value == 100'i64
      doAssert got.upper.value == 999'i64
      await conn.close()

    waitFor t()

  test "numrange roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = rangeOf(parsePgNumeric("1.5"), parsePgNumeric("9.5"))
      let res = await conn.query("SELECT $1::numrange", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getNumRange(0)
      doAssert $got.lower.value == "1.5"
      doAssert $got.upper.value == "9.5"
      await conn.close()

    waitFor t()

  test "daterange roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let lower = dateTime(2024, mJan, 1, zone = utc())
      let upper = dateTime(2024, mMar, 1, zone = utc())
      let v = rangeOf(lower, upper)
      let res = await conn.query("SELECT $1::daterange", @[toPgDateRangeParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getDateRange(0)
      doAssert got.hasLower
      doAssert got.hasUpper
      doAssert got.lower.value.year == 2024
      doAssert got.lower.value.month == mJan
      doAssert got.lower.value.monthday == 1
      await conn.close()

    waitFor t()

  test "empty range":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = emptyRange[int32]()
      let res = await conn.query("SELECT $1::int4range", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Range(0)
      doAssert got.isEmpty
      await conn.close()

    waitFor t()

  test "unbounded range":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = unboundedRange[int32]()
      let res = await conn.query("SELECT $1::int4range", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Range(0)
      doAssert not got.isEmpty
      doAssert not got.hasLower
      doAssert not got.hasUpper
      await conn.close()

    waitFor t()

  test "half-bounded range rangeFrom":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = rangeFrom(5'i32)
      let res = await conn.query("SELECT $1::int4range", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Range(0)
      doAssert got.hasLower
      doAssert not got.hasUpper
      doAssert got.lower.value == 5'i32
      doAssert got.lower.inclusive == true
      await conn.close()

    waitFor t()

  test "half-bounded range rangeTo":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = rangeTo(10'i32)
      let res = await conn.query("SELECT $1::int4range", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Range(0)
      doAssert not got.hasLower
      doAssert got.hasUpper
      doAssert got.upper.value == 10'i32
      doAssert got.upper.inclusive == false
      await conn.close()

    waitFor t()

  test "inclusive upper bound normalizes for integers":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # For integer ranges, PostgreSQL normalizes [1,10] to [1,11)
      let v = rangeOf(1'i32, 10'i32, upperInc = true)
      let res = await conn.query("SELECT $1::int4range", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Range(0)
      doAssert got.lower.value == 1'i32
      doAssert got.lower.inclusive == true
      doAssert got.upper.value == 11'i32
      doAssert got.upper.inclusive == false
      await conn.close()

    waitFor t()

  test "NULL range":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::int4range")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt4RangeOpt(0).isNone
      await conn.close()

    waitFor t()

  test "range contains operator":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = rangeOf(1'i32, 10'i32)
      let res = await conn.query("SELECT $1::int4range @> 5::int4", @[toPgParam(v)])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBool(0) == true

      let res2 = await conn.query("SELECT $1::int4range @> 15::int4", @[toPgParam(v)])
      doAssert res2.rows[0].getBool(0) == false
      await conn.close()

    waitFor t()

suite "E2E: Multirange Types":
  test "int4multirange roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32))
      let res = await conn.query("SELECT $1::int4multirange", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Multirange(0)
      doAssert got.len == 2
      doAssert got[0].lower.value == 1'i32
      doAssert got[0].upper.value == 3'i32
      doAssert got[1].lower.value == 5'i32
      doAssert got[1].upper.value == 8'i32
      await conn.close()

    waitFor t()

  test "int8multirange roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = toMultirange(rangeOf(100'i64, 200'i64), rangeOf(300'i64, 400'i64))
      let res = await conn.query("SELECT $1::int8multirange", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt8Multirange(0)
      doAssert got.len == 2
      doAssert got[0].lower.value == 100'i64
      doAssert got[1].upper.value == 400'i64
      await conn.close()

    waitFor t()

  test "empty multirange":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = toMultirange[int32]()
      let res = await conn.query("SELECT $1::int4multirange", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Multirange(0)
      doAssert got.len == 0
      await conn.close()

    waitFor t()

  test "single range multirange":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = toMultirange(rangeOf(10'i32, 20'i32))
      let res = await conn.query("SELECT $1::int4multirange", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getInt4Multirange(0)
      doAssert got.len == 1
      doAssert got[0].lower.value == 10'i32
      doAssert got[0].upper.value == 20'i32
      await conn.close()

    waitFor t()

  test "NULL multirange":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::int4multirange")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt4MultirangeOpt(0).isNone
      await conn.close()

    waitFor t()

suite "E2E: Temporal array types":
  test "timestamp array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt1 = dateTime(2023, mJan, 15, 10, 30, 0, zone = utc())
      let dt2 = dateTime(2024, mJun, 20, 14, 45, 30, zone = utc())
      let res = await conn.query(
        "SELECT $1::timestamp[]", @[toPgTimestampArrayParam(@[dt1, dt2])]
      )
      doAssert res.rows.len == 1
      let arr = res.rows[0].getTimestampArray(0)
      doAssert arr.len == 2
      doAssert arr[0].year == 2023
      doAssert arr[1].year == 2024
      await conn.close()

    waitFor t()

  test "empty timestamp array":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::timestamp[]", @[toPgTimestampArrayParam(newSeq[DateTime]())]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getTimestampArray(0).len == 0
      await conn.close()

    waitFor t()

  test "NULL timestamp array":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::timestamp[]")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getTimestampArrayOpt(0).isNone
      await conn.close()

    waitFor t()

  test "date array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt1 = dateTime(2023, mMar, 10, zone = utc())
      let dt2 = dateTime(2024, mDec, 25, zone = utc())
      let res =
        await conn.query("SELECT $1::date[]", @[toPgDateArrayParam(@[dt1, dt2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getDateArray(0)
      doAssert arr.len == 2
      doAssert arr[0].monthday == 10
      doAssert arr[1].month == mDec
      await conn.close()

    waitFor t()

  test "time array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let t1 = PgTime(hour: 10, minute: 30, second: 0, microsecond: 0)
      let t2 = PgTime(hour: 23, minute: 59, second: 59, microsecond: 123456)
      let res = await conn.query("SELECT $1::time[]", @[toPgParam(@[t1, t2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getTimeArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == t1
      doAssert arr[1] == t2
      await conn.close()

    waitFor t()

  test "timetz array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let t1 =
        PgTimeTz(hour: 10, minute: 30, second: 0, microsecond: 0, utcOffset: 3600)
      let t2 =
        PgTimeTz(hour: 23, minute: 59, second: 59, microsecond: 0, utcOffset: -18000)
      let res = await conn.query("SELECT $1::timetz[]", @[toPgParam(@[t1, t2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getTimeTzArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == t1
      doAssert arr[1] == t2
      await conn.close()

    waitFor t()

  test "interval array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let iv1 = PgInterval(months: 2, days: 3, microseconds: 3600000000)
      let iv2 = PgInterval(months: 0, days: 0, microseconds: 1000000)
      let res = await conn.query("SELECT $1::interval[]", @[toPgParam(@[iv1, iv2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getIntervalArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == iv1
      doAssert arr[1] == iv2
      await conn.close()

    waitFor t()

suite "E2E: Identifier / network array types":
  test "uuid array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let u1 = PgUuid("550e8400-e29b-41d4-a716-446655440000")
      let u2 = PgUuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
      let res = await conn.query("SELECT $1::uuid[]", @[toPgParam(@[u1, u2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getUuidArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == u1
      doAssert arr[1] == u2
      await conn.close()

    waitFor t()

  test "inet array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let i1 = PgInet(address: parseIpAddress("192.168.1.1"), mask: 32)
      let i2 = PgInet(address: parseIpAddress("10.0.0.0"), mask: 8)
      let res = await conn.query("SELECT $1::inet[]", @[toPgParam(@[i1, i2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getInetArray(0)
      doAssert arr.len == 2
      doAssert $arr[0].address == "192.168.1.1"
      doAssert arr[1].mask == 8
      await conn.close()

    waitFor t()

  test "cidr array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let c1 = PgCidr(address: parseIpAddress("192.168.1.0"), mask: 24)
      let c2 = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
      let res = await conn.query("SELECT $1::cidr[]", @[toPgParam(@[c1, c2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getCidrArray(0)
      doAssert arr.len == 2
      doAssert arr[0].mask == 24
      doAssert arr[1].mask == 8
      await conn.close()

    waitFor t()

  test "macaddr array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let m1 = PgMacAddr("08:00:2b:01:02:03")
      let m2 = PgMacAddr("aa:bb:cc:dd:ee:ff")
      let res = await conn.query("SELECT $1::macaddr[]", @[toPgParam(@[m1, m2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getMacAddrArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == m1
      doAssert arr[1] == m2
      await conn.close()

    waitFor t()

suite "E2E: Numeric / binary / JSON array types":
  test "numeric array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let n1 = parsePgNumeric("123.45")
      let n2 = parsePgNumeric("0.001")
      let res = await conn.query("SELECT $1::numeric[]", @[toPgParam(@[n1, n2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getNumericArray(0)
      doAssert arr.len == 2
      doAssert $arr[0] == "123.45"
      doAssert $arr[1] == "0.001"
      await conn.close()

    waitFor t()

  test "hstore array roundtrip (text cast)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("CREATE EXTENSION IF NOT EXISTS hstore;")
      var h1: PgHstore = initTable[string, Option[string]]()
      h1["a"] = some("1")
      var h2: PgHstore = initTable[string, Option[string]]()
      h2["b"] = none(string)
      let res = await conn.query("SELECT $1::hstore[]", @[toPgParam(@[h1, h2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getHstoreArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == h1
      doAssert arr[1] == h2
      await conn.close()

    waitFor t()

  test "hstore array roundtrip (binary)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("CREATE EXTENSION IF NOT EXISTS hstore;")
      let oids = await conn.lookupTypeOids(@["hstore"])
      doAssert oids.hasKey("hstore")
      let info = oids["hstore"]
      doAssert info.oid != 0
      doAssert info.arrayOid != 0
      var h1: PgHstore = initTable[string, Option[string]]()
      h1["x"] = some("y")
      var h2: PgHstore = initTable[string, Option[string]]()
      h2["nul"] = none(string)
      let bin = toPgBinaryParam(@[h1, h2], info.oid, info.arrayOid)
      let res = await conn.query("SELECT $1", @[bin])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getHstoreArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == h1
      doAssert arr[1] == h2
      await conn.close()

    waitFor t()

  test "lookupTypeOids respects search_path":
    # to_regtype('hstore') returns NULL when the connection's search_path
    # excludes the schema where hstore is installed; lookupTypeOids must
    # omit it from the result table.
    proc t() {.async.} =
      let setup = await connect(plainConfig())
      discard await setup.simpleQuery("CREATE EXTENSION IF NOT EXISTS hstore;")
      await setup.close()

      var cfg = plainConfig()
      cfg.extraParams = @[("search_path", "pg_catalog")]
      let conn = await connect(cfg)
      let oids = await conn.lookupTypeOids(@["hstore"])
      doAssert not oids.hasKey("hstore")
      await conn.close()

    waitFor t()

  test "bytea array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let b1 = @[1'u8, 2, 3]
      let b2 = @[0xFF'u8, 0x00]
      let res =
        await conn.query("SELECT $1::bytea[]", @[toPgByteaArrayParam(@[b1, b2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getBytesArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == b1
      doAssert arr[1] == b2
      await conn.close()

    waitFor t()

  test "jsonb array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let j1 = %*{"key": "value"}
      let j2 = %*[1, 2, 3]
      let res = await conn.query("SELECT $1::jsonb[]", @[toPgParam(@[j1, j2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getJsonArray(0)
      doAssert arr.len == 2
      doAssert arr[0]["key"].getStr == "value"
      doAssert arr[1].len == 3
      await conn.close()

    waitFor t()

suite "E2E: Geometric array types":
  test "point array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p1 = PgPoint(x: 1.0, y: 2.0)
      let p2 = PgPoint(x: 3.5, y: 4.5)
      let res = await conn.query("SELECT $1::point[]", @[toPgParam(@[p1, p2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getPointArray(0)
      doAssert arr.len == 2
      doAssert arr[0] == p1
      doAssert arr[1] == p2
      await conn.close()

    waitFor t()

  test "circle array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let c1 = PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0)
      let res = await conn.query("SELECT $1::circle[]", @[toPgParam(@[c1])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getCircleArray(0)
      doAssert arr.len == 1
      doAssert arr[0].center.x == 1.0
      doAssert arr[0].radius == 5.0
      await conn.close()

    waitFor t()

  test "box array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let b1 = PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0))
      let res = await conn.query("SELECT $1::box[]", @[toPgParam(@[b1])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getBoxArray(0)
      doAssert arr.len == 1
      doAssert arr[0].high.x == 3.0
      await conn.close()

    waitFor t()

suite "E2E: Other array types":
  test "xml array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let x1 = PgXml("<root/>")
      let x2 = PgXml("<data>hello</data>")
      let res = await conn.query("SELECT $1::xml[]", @[toPgParam(@[x1, x2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getXmlArray(0)
      doAssert arr.len == 2
      doAssert string(arr[0]) == "<root/>"
      doAssert string(arr[1]) == "<data>hello</data>"
      await conn.close()

    waitFor t()

suite "E2E: Multirange array types":
  test "int4multirange array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let mr1 = toMultirange(rangeOf(1'i32, 10'i32), rangeOf(20'i32, 30'i32))
      let mr2 = toMultirange(rangeOf(100'i32, 200'i32))
      let res =
        await conn.query("SELECT $1::int4multirange[]", @[toPgParam(@[mr1, mr2])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getInt4MultirangeArray(0)
      doAssert arr.len == 2
      doAssert seq[PgRange[int32]](arr[0]).len == 2
      doAssert seq[PgRange[int32]](arr[1]).len == 1
      doAssert seq[PgRange[int32]](arr[0])[0].lower.value == 1'i32
      await conn.close()

    waitFor t()

  test "int8multirange array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let mr1 = toMultirange(rangeOf(100'i64, 200'i64))
      let res = await conn.query("SELECT $1::int8multirange[]", @[toPgParam(@[mr1])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getInt8MultirangeArray(0)
      doAssert arr.len == 1
      await conn.close()

    waitFor t()

  test "nummultirange array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let mr1 = toMultirange(rangeOf(parsePgNumeric("1.5"), parsePgNumeric("3.5")))
      let res = await conn.query("SELECT $1::nummultirange[]", @[toPgParam(@[mr1])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getNumMultirangeArray(0)
      doAssert arr.len == 1
      await conn.close()

    waitFor t()

  test "NULL multirange array":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::int4multirange[]")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt4MultirangeArrayOpt(0).isNone
      await conn.close()

    waitFor t()

suite "E2E: Option/NULL array input and element-level output":
  test "seq[Option[int32]] roundtrip with NULL element":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let arr = @[some(1'i32), none(int32), some(3'i32)]
      let res = await conn.query("SELECT $1::int4[] AS a", pgParams(arr))
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArrayElemOpt(0) == arr
      await conn.close()

    waitFor t()

  test "seq[Option[string]] roundtrip with NULL element":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let arr = @[some("a"), none(string), some("c")]
      let res = await conn.query("SELECT $1::text[] AS a", pgParams(arr))
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStrArrayElemOpt(0) == arr
      await conn.close()

    waitFor t()

  test "seq[Option[bool]] roundtrip with NULL element":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let arr = @[some(true), none(bool), some(false)]
      let res = await conn.query("SELECT $1::bool[] AS a", pgParams(arr))
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBoolArrayElemOpt(0) == arr
      await conn.close()

    waitFor t()

  test "seq[Option[float64]] roundtrip with NULL element":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let arr = @[some(1.5), none(float64), some(3.25)]
      let res = await conn.query("SELECT $1::float8[] AS a", pgParams(arr))
      doAssert res.rows.len == 1
      let got = res.rows[0].getFloatArrayElemOpt(0)
      doAssert got.len == 3
      doAssert got[0].isSome and abs(got[0].get - 1.5) < 1e-10
      doAssert got[1].isNone
      doAssert got[2].isSome and abs(got[2].get - 3.25) < 1e-10
      await conn.close()

    waitFor t()

  test "all-NULL array roundtrips":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let arr = @[none(int32), none(int32)]
      let res = await conn.query("SELECT $1::int4[] AS a", pgParams(arr))
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArrayElemOpt(0) == arr
      await conn.close()

    waitFor t()

  test "empty seq[Option[int32]] works":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let arr: seq[Option[int32]] = @[]
      let res = await conn.query("SELECT $1::int4[] AS a", pgParams(arr))
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArrayElemOpt(0).len == 0
      await conn.close()

    waitFor t()

  test "literal ARRAY[1, NULL, 3] via getIntArrayElemOpt":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT ARRAY[1, NULL, 3]::int4[] AS a")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArrayElemOpt(0) ==
        @[some(1'i32), none(int32), some(3'i32)]
      await conn.close()

    waitFor t()

  test "column NULL via getIntArrayElemOptOpt returns none":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::int4[] AS a")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArrayElemOptOpt(0) == none(seq[Option[int32]])
      await conn.close()

    waitFor t()

type PgE2eMood = enum
  happy2 = "happy2"
  sad2 = "sad2"
  ok2 = "ok2"

pgEnum(PgE2eMood)

suite "E2E: enum arrays":
  test "enum array roundtrip with and without NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TYPE IF EXISTS e2e_mood CASCADE")
      discard await conn.exec("CREATE TYPE e2e_mood AS ENUM ('happy2', 'sad2', 'ok2')")
      block:
        let arr = @[happy2, sad2, ok2]
        let res = await conn.query("SELECT $1::e2e_mood[] AS a", @[toPgParam(arr)])
        doAssert res.rows.len == 1
        doAssert getEnumArray[PgE2eMood](res.rows[0], 0) == arr
      block:
        let arr = @[some(happy2), none(PgE2eMood), some(ok2)]
        let res = await conn.query("SELECT $1::e2e_mood[] AS a", @[toPgParam(arr)])
        doAssert res.rows.len == 1
        doAssert getEnumArrayElemOpt[PgE2eMood](res.rows[0], 0) == arr
      discard await conn.exec("DROP TYPE IF EXISTS e2e_mood CASCADE")
      await conn.close()

    waitFor t()

suite "E2E: lookupTypeOids":
  test "resolves multiple types in one round trip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("CREATE EXTENSION IF NOT EXISTS hstore;")
      let oids = await conn.lookupTypeOids(@["int4", "text", "hstore"])
      doAssert oids.hasKey("int4")
      doAssert oids["int4"].oid == 23
      doAssert oids.hasKey("text")
      doAssert oids["text"].oid == 25
      doAssert oids.hasKey("hstore")
      doAssert oids["hstore"].oid != 0
      doAssert oids["hstore"].arrayOid != 0
      await conn.close()

    waitFor t()

  test "unknown types omitted from result":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let oids = await conn.lookupTypeOids(@["int4", "nonexistent_xyz_type"])
      doAssert oids.hasKey("int4")
      doAssert not oids.hasKey("nonexistent_xyz_type")
      doAssert oids.len == 1
      await conn.close()

    waitFor t()

  test "empty names returns empty table without round trip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let oids = await conn.lookupTypeOids(newSeq[string]())
      doAssert oids.len == 0
      await conn.close()

    waitFor t()

  test "raises PgConnectionError when connection not ready":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      await conn.close()
      var raised = false
      try:
        discard await conn.lookupTypeOids(@["int4"])
      except PgConnectionError:
        raised = true
      doAssert raised

    waitFor t()

  test "rejects type names with unsafe characters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.lookupTypeOids(@["x'; DROP TABLE--"])
      except PgTypeError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "array OID populated for scalar types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let oids = await conn.lookupTypeOids(@["int4"])
      doAssert oids["int4"].arrayOid == 1007 # _int4
      await conn.close()

    waitFor t()

  test "hstore column returned as text decodes via getHstore":
    # cache.nim no longer auto-flips hstore columns to binary; this confirms
    # that hstore values still decode through the text path.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.simpleQuery("CREATE EXTENSION IF NOT EXISTS hstore;")
      let res = await conn.query("SELECT 'a=>1,b=>NULL'::hstore", newSeq[PgParam]())
      doAssert res.rows.len == 1
      let h = res.rows[0].getHstore(0)
      doAssert h["a"] == some("1")
      doAssert h["b"].isNone
      await conn.close()

    waitFor t()
