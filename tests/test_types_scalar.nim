import std/[json, unittest, options, strutils, tables, times, math, net, typetraits]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}

import types_common

suite "OID constants":
  test "standard OID values":
    check OidBool == 16'i32
    check OidInt2 == 21'i32
    check OidInt4 == 23'i32
    check OidInt8 == 20'i32
    check OidFloat4 == 700'i32
    check OidFloat8 == 701'i32
    check OidText == 25'i32
    check OidVarchar == 1043'i32
    check OidBytea == 17'i32
    check OidTimestamp == 1114'i32
    check OidDate == 1082'i32
    check OidTime == 1083'i32
    check OidTimestampTz == 1184'i32
    check OidNumeric == 1700'i32
    check OidJson == 114'i32
    check OidInterval == 1186'i32
    check OidUuid == 2950'i32
    check OidJsonb == 3802'i32
    check OidInet == 869'i32
    check OidCidr == 650'i32
    check OidMacAddr == 829'i32
    check OidMacAddr8 == 774'i32
    check OidTsVector == 3614'i32
    check OidTsQuery == 3615'i32

suite "toPgParam":
  test "string":
    let p = toPgParam("hello")
    check p.oid == OidText
    check p.format == 0
    check p.value.isSome
    check toString(p.value.get) == "hello"

  test "string empty":
    let p = toPgParam("")
    check p.oid == OidText
    check p.value.isSome
    check p.value.get.len == 0

  test "int32":
    let p = toPgParam(42'i32)
    check p.oid == OidInt4
    check p.format == 1
    check p.value.get == @(toBE32(42'i32))

  test "int32 negative":
    let p = toPgParam(-1'i32)
    check p.value.get == @(toBE32(-1'i32))

  test "int32 zero":
    let p = toPgParam(0'i32)
    check p.value.get == @(toBE32(0'i32))

  test "int64":
    let p = toPgParam(9999999999'i64)
    check p.oid == OidInt8
    check p.format == 1
    check p.value.get == @(toBE64(9999999999'i64))

  test "int64 negative":
    let p = toPgParam(-9999999999'i64)
    check p.value.get == @(toBE64(-9999999999'i64))

  test "float64":
    let p = toPgParam(3.14)
    check p.oid == OidFloat8
    check p.format == 1
    check p.value.isSome
    check p.value.get.len == 8
    # Verify roundtrip via fromBE64 + cast
    let bits = fromBE64(p.value.get)
    check abs(cast[float64](bits) - 3.14) < 1e-10

  test "float64 zero":
    let p = toPgParam(0.0)
    let bits = fromBE64(p.value.get)
    check cast[float64](bits) == 0.0

  test "float64 negative":
    let p = toPgParam(-1.5)
    let bits = fromBE64(p.value.get)
    check abs(cast[float64](bits) - (-1.5)) < 1e-10

  test "bool true":
    let p = toPgParam(true)
    check p.oid == OidBool
    check p.format == 1
    check p.value.get == @[1'u8]

  test "bool false":
    let p = toPgParam(false)
    check p.oid == OidBool
    check p.value.get == @[0'u8]

  test "seq[byte]":
    let data = @[0x01'u8, 0x02, 0xFF]
    let p = toPgParam(data)
    check p.oid == OidBytea
    check p.format == 1
    check p.value.isSome
    check p.value.get == data

  test "seq[byte] empty":
    let data: seq[byte] = @[]
    let p = toPgParam(data)
    check p.oid == OidBytea
    check p.value.isSome
    check p.value.get.len == 0

  test "int16":
    let p = toPgParam(100'i16)
    check p.oid == OidInt2
    check p.format == 1
    check p.value.get == @(toBE16(100'i16))

  test "int16 negative":
    let p = toPgParam(-32000'i16)
    check p.value.get == @(toBE16(-32000'i16))

  test "float32":
    let p = toPgParam(1.5'f32)
    check p.oid == OidFloat4
    check p.format == 1
    check p.value.isSome
    check p.value.get.len == 4
    let bits = fromBE32(p.value.get)
    check abs(cast[float32](bits) - 1.5'f32) < 1e-5'f32

  test "float32 zero":
    let p = toPgParam(0.0'f32)
    check p.oid == OidFloat4

  test "DateTime":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgParam(dt)
    check p.oid == OidTimestamp
    check p.format == 0
    let s = toString(p.value.get)
    check s.startsWith("2024-01-15 10:30:00")

  test "toPgParam Option[DateTime] none":
    # Must not prototype via default(DateTime) (uninitialized → AssertionDefect).
    let p = toPgParam(none(DateTime))
    check p.oid == OidTimestamp
    check p.format == 0
    check p.value.isNone

  test "toPgParam Option[DateTime] some":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgParam(some(dt))
    check p.oid == OidTimestamp
    check p.format == 0
    check toString(p.value.get).startsWith("2024-01-15 10:30:00")

  test "DateTime with non-UTC zone encodes the UTC instant":
    # Regression: text OidTimestamp used to serialize the DateTime's local wall
    # clock, so a zoned value stored a different absolute time than the binary
    # encoder produced from the same DateTime. utcOffset counts seconds WEST of
    # UTC, so JST (9h east) has offset -9*3600.
    const jstWest = -9 * 3600
    proc jstFromTime(time: Time): ZonedTime {.nimcall, gcsafe, raises: [].} =
      ZonedTime(isDst: false, utcOffset: jstWest, time: time)

    proc jstFromAdj(adjTime: Time): ZonedTime {.nimcall, gcsafe, raises: [].} =
      ZonedTime(
        isDst: false,
        utcOffset: jstWest,
        time: adjTime + initDuration(seconds = jstWest),
      )

    let jst = newTimezone("JST+09", jstFromTime, jstFromAdj)
    let dt = dateTime(2026, mJul, 15, 21, 0, 0, 0, jst)
    let p = toPgParam(dt)
    check p.oid == OidTimestamp
    check toString(p.value.get) == "2026-07-15 12:00:00.000000"
    # Binary path already encodes the absolute instant; both should agree now.
    let bin = toPgBinaryParam(dt)
    check bin.oid == OidTimestamp
    check bin.format == 1

  test "PgUuid":
    let uuid = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let p = toPgParam(uuid)
    check p.oid == OidUuid
    check p.format == 0
    check toString(p.value.get) == "550e8400-e29b-41d4-a716-446655440000"

suite "fromPgText":
  test "basic string":
    let data = toBytes("hello world")
    check fromPgText(data, OidText) == "hello world"

  test "empty":
    let data: seq[byte] = @[]
    check fromPgText(data, OidText) == ""

  test "numeric as text":
    let data = toBytes("12345")
    check fromPgText(data, OidInt4) == "12345"

  test "preserves bytes":
    let data = @[0xC3'u8, 0xA9] # UTF-8 for 'é'
    let s = fromPgText(data, OidText)
    check s.len == 2
    check byte(s[0]) == 0xC3
    check byte(s[1]) == 0xA9

suite "toPgParam Option[T]":
  test "some string":
    let p = toPgParam(some("hello"))
    check p.oid == OidText
    check p.value.isSome
    check toString(p.value.get) == "hello"

  test "none string":
    let p = toPgParam(none(string))
    check p.value.isNone

  test "some int32":
    let p = toPgParam(some(42'i32))
    check p.oid == OidInt4
    check p.value.get == @(toBE32(42'i32))

  test "none int32":
    let p = toPgParam(none(int32))
    check p.value.isNone

  test "some bool":
    let p = toPgParam(some(true))
    check p.oid == OidBool
    check p.value.get == @[1'u8]

  test "none bool":
    let p = toPgParam(none(bool))
    check p.value.isNone

suite "Row accessors":
  test "getStr":
    let row = @[some(toBytes("hello")), none(seq[byte])]
    check row.getStr(0) == "hello"

  test "getStr NULL raises":
    let row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getStr(0)
    except PgTypeError:
      raised = true
    check raised

  test "isNull":
    let row = @[some(toBytes("x")), none(seq[byte])]
    check not row.isNull(0)
    check row.isNull(1)

  test "column index out of range raises PgTypeError (catchable, not Defect)":
    let row = @[some(toBytes("hello"))]
    expect PgTypeError:
      discard row.getStr(1)
    expect PgTypeError:
      discard row.getStr(-1)
    # PgTypeError is a PgError, so a single `except PgError` clause covers
    # both out-of-range column access and the accessor-level errors below.
    expect PgError:
      discard row.getStr(1)

  test "getInt16":
    let row = @[some(toBytes("123"))]
    check row.getInt16(0) == 123'i16

  test "getInt16 negative":
    let row = @[some(toBytes("-456"))]
    check row.getInt16(0) == -456'i16

  test "getInt16 invalid value raises":
    let row = @[some(toBytes("abc"))]
    expect PgTypeError:
      discard row.getInt16(0)

  test "getInt16 out of range raises (no silent truncation)":
    let row = @[some(toBytes("32768"))]
    expect PgTypeError:
      discard row.getInt16(0)
    let rowNeg = @[some(toBytes("-32769"))]
    expect PgTypeError:
      discard rowNeg.getInt16(0)

  test "getInt":
    let row = @[some(toBytes("42"))]
    check row.getInt(0) == 42'i32

  test "getInt negative":
    let row = @[some(toBytes("-7"))]
    check row.getInt(0) == -7'i32

  test "getInt invalid value raises":
    let row = @[some(toBytes("abc"))]
    expect PgTypeError:
      discard row.getInt(0)

  test "getInt out of range raises (no silent truncation)":
    let row = @[some(toBytes("2147483648"))]
    expect PgTypeError:
      discard row.getInt(0)
    let rowNeg = @[some(toBytes("-2147483649"))]
    expect PgTypeError:
      discard rowNeg.getInt(0)

  test "getInt64":
    let row = @[some(toBytes("9999999999"))]
    check row.getInt64(0) == 9999999999'i64

  test "getInt64 invalid value raises":
    let row = @[some(toBytes("xyz"))]
    expect PgTypeError:
      discard row.getInt64(0)

  test "getInt64 overflow raises PgTypeError (not raw ValueError)":
    # A text/numeric column whose value exceeds int64 makes the non-throwing
    # parseBiggestInt raise a raw ValueError; it must be converted to PgTypeError
    # so it stays catchable via `except PgError`.
    let row = @[some(toBytes("99999999999999999999999999"))]
    expect PgTypeError:
      discard row.getInt64(0)
    let rowNeg = @[some(toBytes("-99999999999999999999999999"))]
    expect PgTypeError:
      discard rowNeg.getInt64(0)

  test "getInt overflow raises PgTypeError (not raw ValueError)":
    let row = @[some(toBytes("99999999999999999999999999"))]
    expect PgTypeError:
      discard row.getInt(0)

  test "getInt16 overflow raises PgTypeError (not raw ValueError)":
    let row = @[some(toBytes("99999999999999999999999999"))]
    expect PgTypeError:
      discard row.getInt16(0)

  test "getInt rejects trailing garbage (full-consumption check)":
    for s in ["123abc", "42 ", "12.5", "1e3"]:
      let row = @[some(toBytes(s))]
      expect PgTypeError:
        discard row.getInt(0)

  test "getInt/getInt16/getInt64 reject digit-group underscore":
    for s in ["1_0", "12_345", "_1", "1_"]:
      let row = @[some(toBytes(s))]
      expect PgTypeError:
        discard row.getInt(0)
      expect PgTypeError:
        discard row.getInt16(0)
      expect PgTypeError:
        discard row.getInt64(0)

  test "getInt16 rejects trailing garbage (full-consumption check)":
    for s in ["123abc", "42 ", "12.5"]:
      let row = @[some(toBytes(s))]
      expect PgTypeError:
        discard row.getInt16(0)

  test "getInt64 rejects trailing garbage (full-consumption check)":
    for s in ["123abc", "42 ", "12.5"]:
      let row = @[some(toBytes(s))]
      expect PgTypeError:
        discard row.getInt64(0)

  # The negative tests above are all text; the binary integer tests are all
  # positive. Binary is what the server sends once a statement is cached.

  test "getInt16 binary int2 negative":
    for v in [-1'i16, -12345'i16, low(int16), high(int16), 0'i16]:
      let row = mkRow(@[some(@(toBE16(v)))], @[mkField(OidInt2, 1'i16)])
      check row.getInt16(0) == v

  test "getInt binary int4 negative":
    for v in [-1'i32, -70000'i32, low(int32), high(int32), 0'i32]:
      let row = mkRow(@[some(@(toBE32(v)))], @[mkField(OidInt4, 1'i16)])
      check row.getInt(0) == v

  test "getInt binary int2 negative promotion":
    for v in [-1'i16, low(int16)]:
      let row = mkRow(@[some(@(toBE16(v)))], @[mkField(OidInt2, 1'i16)])
      check row.getInt(0) == int32(v)

  test "getInt64 binary int8 negative":
    for v in [-1'i64, -5_000_000_000'i64, low(int64), high(int64), 0'i64]:
      let row = mkRow(@[some(@(toBE64(v)))], @[mkField(OidInt8, 1'i16)])
      check row.getInt64(0) == v

  test "getInt64 binary int4/int2 negative promotion":
    let i4 = mkRow(@[some(@(toBE32(-7'i32)))], @[mkField(OidInt4, 1'i16)])
    check i4.getInt64(0) == -7'i64
    let i2 = mkRow(@[some(@(toBE16(-7'i16)))], @[mkField(OidInt2, 1'i16)])
    check i2.getInt64(0) == -7'i64

  test "getStr binary integers negative":
    let i2 = mkRow(@[some(@(toBE16(-1'i16)))], @[mkField(OidInt2, 1'i16)])
    check i2.getStr(0) == "-1"
    let i4 = mkRow(@[some(@(toBE32(low(int32))))], @[mkField(OidInt4, 1'i16)])
    check i4.getStr(0) == $low(int32)
    let i8 = mkRow(@[some(@(toBE64(-1'i64)))], @[mkField(OidInt8, 1'i16)])
    check i8.getStr(0) == "-1"

  test "getFloat32":
    let row = @[some(toBytes("2.5"))]
    check abs(row.getFloat32(0) - 2.5'f32) < 1e-6

  test "getFloat32 invalid value raises":
    let row = @[some(toBytes("notanumber"))]
    expect PgTypeError:
      discard row.getFloat32(0)

  test "getFloat":
    let row = @[some(toBytes("3.14"))]
    check abs(row.getFloat(0) - 3.14) < 1e-10

  test "trailing empty text cell raises PgTypeError not IndexDefect":
    # A trailing empty cell has off == buf.len; bufView must not `addr buf[off]`
    # (Defect would escape `except PgError`).
    let row = @[some(toBytes("hello")), some(newSeq[byte](0))]
    expect PgTypeError:
      discard row.getInt(1)
    expect PgTypeError:
      discard row.getInt16(1)
    expect PgTypeError:
      discard row.getInt64(1)
    expect PgTypeError:
      discard row.getFloat(1)
    expect PgTypeError:
      discard row.getFloat32(1)

  test "getBool true variants":
    check @[some(toBytes("t"))].getBool(0) == true
    check @[some(toBytes("true"))].getBool(0) == true
    check @[some(toBytes("1"))].getBool(0) == true

  test "getBool false variants":
    check @[some(toBytes("f"))].getBool(0) == false
    check @[some(toBytes("false"))].getBool(0) == false
    check @[some(toBytes("0"))].getBool(0) == false

  test "getBool invalid raises":
    var raised = false
    try:
      discard @[some(toBytes("maybe"))].getBool(0)
    except PgTypeError:
      raised = true
    check raised

suite "PgParam format field":
  test "toPgParam uses binary for numeric, bool, and bytea, text for others":
    check toPgParam("x").format == 0
    check toPgParam(1'i16).format == 1
    check toPgParam(1'i32).format == 1
    check toPgParam(1'i64).format == 1
    check toPgParam(1.0'f32).format == 1
    check toPgParam(1.0).format == 1
    check toPgParam(true).format == 1
    check toPgParam(@[1'u8]).format == 1
    check toPgParam(dateTime(2024, mJan, 1, 0, 0, 0, 0, utc())).format == 0
    check toPgParam(PgUuid("test")).format == 0

suite "getBytes accessor":
  test "hex-encoded bytea":
    let row = @[some(toBytes("\\x48656c6c6f"))]
    let b = row.getBytes(0)
    check b == @[0x48'u8, 0x65, 0x6c, 0x6c, 0x6f] # "Hello"

  test "hex-encoded bytea empty":
    let row = @[some(toBytes("\\x"))]
    let b = row.getBytes(0)
    check b.len == 0

  test "escape format passthrough (no backslash)":
    let row = @[some(toBytes("Hello"))]
    check row.getBytes(0) == toBytes("Hello")

  test "escape format decodes \\NNN octal":
    # bytea_output=escape encodes bytes <0x20 or >0x7E as three-digit octal
    let row = @[some(toBytes("\\000A\\377"))]
    check row.getBytes(0) == @[0x00'u8, 0x41'u8, 0xFF'u8]

  test "escape format decodes \\\\ backslash":
    let row = @[some(toBytes("a\\\\b"))]
    check row.getBytes(0) == @[0x61'u8, 0x5C'u8, 0x62'u8]

  test "escape format rejects trailing backslash":
    expect PgTypeError:
      discard (Row @[some(toBytes("abc\\"))]).getBytes(0)

  test "escape format rejects malformed octal":
    expect PgTypeError:
      discard (Row @[some(toBytes("\\4ab"))]).getBytes(0)

  test "NULL raises":
    let row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getBytes(0)
    except PgTypeError:
      raised = true
    check raised

suite "Binary encode/decode helpers":
  test "int16 roundtrip":
    let p = toPgBinaryParam(42'i16)
    check p.oid == OidInt2
    check p.format == 1
    check fromBE16(p.value.get) == 42'i16

  test "int16 negative":
    let p = toPgBinaryParam(-1000'i16)
    check fromBE16(p.value.get) == -1000'i16

  test "int32 roundtrip":
    let p = toPgBinaryParam(123456'i32)
    check p.oid == OidInt4
    check p.format == 1
    check fromBE32(p.value.get) == 123456'i32

  test "int32 negative":
    let p = toPgBinaryParam(-999999'i32)
    check fromBE32(p.value.get) == -999999'i32

  test "int64 roundtrip":
    let p = toPgBinaryParam(9999999999'i64)
    check p.oid == OidInt8
    check p.format == 1
    check fromBE64(p.value.get) == 9999999999'i64

  test "int64 negative":
    let p = toPgBinaryParam(-9999999999'i64)
    check fromBE64(p.value.get) == -9999999999'i64

  test "int roundtrip":
    let p = toPgBinaryParam(42)
    check p.oid == OidInt8
    check p.format == 1
    check fromBE64(p.value.get) == 42'i64

  test "float32 roundtrip":
    let p = toPgBinaryParam(3.14'f32)
    check p.oid == OidFloat4
    check p.format == 1
    let bits = fromBE32(p.value.get)
    check cast[float32](cast[uint32](bits)) == 3.14'f32

  test "float64 roundtrip":
    let p = toPgBinaryParam(3.14159265358979)
    check p.oid == OidFloat8
    check p.format == 1
    let bits = fromBE64(p.value.get)
    check cast[float64](cast[uint64](bits)) == 3.14159265358979

  test "bool true":
    let p = toPgBinaryParam(true)
    check p.oid == OidBool
    check p.format == 1
    check p.value.get == @[1'u8]

  test "bool false":
    let p = toPgBinaryParam(false)
    check p.value.get == @[0'u8]

  test "string":
    let p = toPgBinaryParam("hello")
    check p.oid == OidText
    check p.format == 1
    check toString(p.value.get) == "hello"

  test "seq[byte]":
    let data = @[0xDE'u8, 0xAD, 0xBE, 0xEF]
    let p = toPgBinaryParam(data)
    check p.oid == OidBytea
    check p.format == 1
    check p.value.get == data

  test "DateTime":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgBinaryParam(dt)
    check p.oid == OidTimestamp
    check p.format == 1
    check p.value.get.len == 8

  test "toPgBinaryParam Option[DateTime] none":
    # Must not prototype via default(DateTime) (uninitialized → AssertionDefect).
    let p = toPgBinaryParam(none(DateTime))
    check p.oid == OidTimestamp
    check p.format == 1
    check p.value.isNone

  test "toPgBinaryParam Option[DateTime] some":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgBinaryParam(some(dt))
    check p.oid == OidTimestamp
    check p.format == 1
    check p.value.get.len == 8

  test "PgUuid":
    let uuid = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let p = toPgBinaryParam(uuid)
    check p.oid == OidUuid
    check p.format == 1
    check p.value.get.len == 16
    check p.value.get[0] == 0x55'u8
    check p.value.get[1] == 0x0e'u8

  test "PgUuid binary accepts dashless form":
    let uuid = PgUuid("550e8400e29b41d4a716446655440000")
    let p = toPgBinaryParam(uuid)
    check p.value.get.len == 16
    check p.value.get[0] == 0x55'u8
    check p.value.get[15] == 0x00'u8

  test "PgUuid binary uppercase hex":
    let uuid = PgUuid("AABBCCDD-EEFF-0011-2233-445566778899")
    let p = toPgBinaryParam(uuid)
    check p.value.get[0] == 0xAA'u8
    check p.value.get[7] == 0x11'u8
    check p.value.get[15] == 0x99'u8

  test "PgUuid binary too short raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgUuid("550e8400"))

  test "PgUuid binary too long raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgUuid("550e8400-e29b-41d4-a716-44665544000000"))

  test "PgUuid binary empty raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgUuid(""))

  test "PgUuid binary non-hex char raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgUuid("550e8400-e29b-41d4-a716-44665544zzzz"))

  test "PgUuid text passes through unvalidated (server-side check)":
    # Text format hands the raw string to PostgreSQL, so client-side does NOT
    # raise on invalid input — the server returns the error instead. This
    # contrasts with toPgBinaryParam, which validates locally.
    let p = toPgParam(PgUuid("not-a-uuid"))
    check p.format == 0
    check toString(p.value.get) == "not-a-uuid"

  test "PgUuid $ and ==":
    let a = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let b = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let c = PgUuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    check $a == "550e8400-e29b-41d4-a716-446655440000"
    check a == b
    check a != c

  test "getUuid binary":
    let data: seq[byte] = @[
      0x55'u8, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55,
      0x44, 0x00, 0x00,
    ]
    let fields = @[mkField(OidUuid, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getUuid(0) == "550e8400-e29b-41d4-a716-446655440000"

  test "getUuid text":
    let row: Row = @[some(toBytes("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))]
    check $row.getUuid(0) == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

  test "getUuidOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getUuidOpt(0) == none(PgUuid)

  test "toPgParam Option[PgUuid] none":
    let p = toPgParam(none(PgUuid))
    check p.oid == OidUuid
    check p.format == 0
    check p.value.isNone

  test "toPgBinaryParam Option[PgUuid] none":
    # Must not prototype via default(PgUuid) (empty string → PgTypeError).
    let p = toPgBinaryParam(none(PgUuid))
    check p.oid == OidUuid
    check p.format == 1
    check p.value.isNone

  test "toPgBinaryParam Option[PgUuid] some":
    let uuid = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let p = toPgBinaryParam(some(uuid))
    check p.oid == OidUuid
    check p.format == 1
    check p.value.get.len == 16

  test "Option some":
    let p = toPgBinaryParam(some(42'i32))
    check p.oid == OidInt4
    check p.format == 1
    check fromBE32(p.value.get) == 42'i32

  test "Option none":
    let p = toPgBinaryParam(none(int32))
    check p.oid == OidInt4
    check p.format == 1
    check p.value.isNone

suite "Format-aware binary accessors":
  test "getInt binary":
    let data = @[0'u8, 0, 0, 42]
    let fields = @[mkField(OidInt4, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getInt(0) == 42'i32

  test "getInt text fallback":
    let row = @[some(toBytes("42"))]
    check row.getInt(0) == 42'i32

  test "getInt64 binary int8":
    let data = @[0'u8, 0, 0, 0, 0, 0, 0, 42]
    let fields = @[mkField(OidInt8, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getInt64(0) == 42'i64

  test "getInt64 binary int4 promotion":
    let data = @[0'u8, 0, 0, 42]
    let fields = @[mkField(OidInt4, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getInt64(0) == 42'i64

  test "getInt64 binary int2 promotion":
    let data = @[0'u8, 42]
    let fields = @[mkField(OidInt2, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getInt64(0) == 42'i64

  test "getFloat binary float8":
    let p = toPgBinaryParam(3.14)
    let fields = @[mkField(OidFloat8, 1)]
    let row = mkRow(@[p.value], fields)
    check abs(row.getFloat(0) - 3.14) < 1e-10

  test "getFloat binary float4":
    let p = toPgBinaryParam(1.5'f32)
    let fields = @[mkField(OidFloat4, 1)]
    let row = mkRow(@[p.value], fields)
    check abs(row.getFloat(0) - 1.5) < 1e-5

  test "getBool binary":
    let fields = @[mkField(OidBool, 1)]
    let row = mkRow(@[some(@[1'u8])], fields)
    check row.getBool(0) == true

  test "getBool binary false":
    let fields = @[mkField(OidBool, 1)]
    let row = mkRow(@[some(@[0'u8])], fields)
    check row.getBool(0) == false

  test "getBytes binary":
    let data = @[0xDE'u8, 0xAD, 0xBE, 0xEF]
    let fields = @[mkField(OidBytea, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getBytes(0) == data

  test "getStr binary text":
    let fields = @[mkField(OidText, 1)]
    let row = mkRow(@[some(toBytes("hello"))], fields)
    check row.getStr(0) == "hello"

  test "getStr binary int4":
    let data = @[0'u8, 0, 0, 42]
    let fields = @[mkField(OidInt4, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getStr(0) == "42"

  test "getTimestamp binary":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgBinaryParam(dt)
    let fields = @[mkField(OidTimestamp, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getTimestamp(0)
    check result.year == 2024
    check result.month == mJan
    check result.monthday == 15
    check result.hour == 10
    check result.minute == 30

  test "getDate binary":
    # 2024-01-15 is 8780 days since 2000-01-01
    # (24 years * 365 + 6 leap days + 14 days)
    let pgDays = 8780'i32
    let p = PgParam(
      oid: OidDate,
      format: 1,
      value: some(
        @[
          byte((pgDays shr 24) and 0xFF),
          byte((pgDays shr 16) and 0xFF),
          byte((pgDays shr 8) and 0xFF),
          byte(pgDays and 0xFF),
        ]
      ),
    )
    let fields = @[mkField(OidDate, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getDate(0)
    check result.year == 2024
    check result.month == mJan
    check result.monthday == 15

suite "Binary clen mismatch raises PgTypeError":
  test "getInt binary unexpected length":
    let fields = @[mkField(OidInt4, 1)]
    let row = mkRow(@[some(@[0'u8, 0, 0])], fields) # 3 bytes (expected 2 or 4)
    expect PgTypeError:
      discard row.getInt(0)

  test "getInt64 binary unexpected length":
    let fields = @[mkField(OidInt8, 1)]
    let row = mkRow(@[some(@[0'u8, 0, 0, 0, 0, 0, 0])], fields) # 7 bytes
    expect PgTypeError:
      discard row.getInt64(0)

  test "getFloat binary unexpected length":
    let fields = @[mkField(OidFloat8, 1)]
    let row = mkRow(@[some(@[0'u8, 0, 0])], fields) # 3 bytes (expected 4 or 8)
    expect PgTypeError:
      discard row.getFloat(0)

  test "getFloat invalid text raises (no silent 0.0)":
    let row: Row = @[some(toBytes("not-a-number"))]
    expect PgTypeError:
      discard row.getFloat(0)

  test "getFloat32 binary unexpected length":
    let fields = @[mkField(OidFloat4, 1)]
    let row = mkRow(@[some(@[0'u8, 0, 0, 0, 0])], fields) # 5 bytes (expected 4)
    expect PgTypeError:
      discard row.getFloat32(0)

  test "getBool binary unexpected length":
    let fields = @[mkField(OidBool, 1)]
    let row = mkRow(@[some(newSeq[byte](0))], fields) # 0 bytes
    expect PgTypeError:
      discard row.getBool(0)

  test "getUuid binary unexpected length":
    let fields = @[mkField(OidUuid, 1)]
    let row = mkRow(@[some(newSeq[byte](8))], fields) # 8 bytes (expected 16)
    expect PgTypeError:
      discard row.getUuid(0)

  test "getNumeric binary unexpected length":
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(newSeq[byte](4))], fields) # 4 bytes (expected >= 8)
    expect PgTypeError:
      discard row.getNumeric(0)

  test "getTimestamp binary unexpected length":
    let fields = @[mkField(OidTimestamp, 1)]
    let row = mkRow(@[some(newSeq[byte](4))], fields) # 4 bytes (expected 8)
    expect PgTypeError:
      discard row.getTimestamp(0)

  test "getTimestampTz binary unexpected length":
    let fields = @[mkField(OidTimestampTz, 1)]
    let row = mkRow(@[some(newSeq[byte](4))], fields)
    expect PgTypeError:
      discard row.getTimestampTz(0)

suite "Row type alias":
  test "Row is seq[Option[seq[byte]]]":
    let row: Row = @[some(toBytes("hello")), none(seq[byte])]
    check row.getStr(0) == "hello"
    check row.isNull(1)

  test "toRow raises PgTypeError for too many columns":
    var cells = newSeq[Option[seq[byte]]](int(high(int16)) + 1)
    for i in 0 ..< cells.len:
      cells[i] = none(seq[byte])
    expect PgTypeError:
      discard toRow(cells)

suite "parseAffectedRows":
  test "UPDATE tag":
    check parseAffectedRows("UPDATE 3") == 3

  test "INSERT tag":
    check parseAffectedRows("INSERT 0 1") == 1

  test "DELETE tag":
    check parseAffectedRows("DELETE 0") == 0

  test "SELECT tag":
    check parseAffectedRows("SELECT 5") == 5

  test "empty tag":
    check parseAffectedRows("") == 0

  test "non-numeric tag":
    check parseAffectedRows("CREATE TABLE") == 0

  test "COPY tag":
    check parseAffectedRows("COPY 100") == 100

  test "MERGE tag":
    check parseAffectedRows("MERGE 7") == 7

  test "MOVE tag":
    check parseAffectedRows("MOVE 12") == 12

  test "FETCH tag":
    check parseAffectedRows("FETCH 4") == 4

  test "trailing whitespace falls back to 0":
    check parseAffectedRows("UPDATE 3 ") == 0

  test "overflow falls back to 0":
    check parseAffectedRows("UPDATE 99999999999999999999999999") == 0

  test "single-token DDL tag":
    check parseAffectedRows("BEGIN") == 0

suite "Option accessors":
  test "getStrOpt some":
    let row: Row = @[some(toBytes("hello"))]
    check row.getStrOpt(0) == some("hello")

  test "getStrOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getStrOpt(0) == none(string)

  test "getInt16Opt some":
    let row: Row = @[some(toBytes("123"))]
    check row.getInt16Opt(0) == some(123'i16)

  test "getInt16Opt none":
    let row: Row = @[none(seq[byte])]
    check row.getInt16Opt(0) == none(int16)

  test "getIntOpt some":
    let row: Row = @[some(toBytes("42"))]
    check row.getIntOpt(0) == some(42'i32)

  test "getIntOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getIntOpt(0) == none(int32)

  test "getInt64Opt some":
    let row: Row = @[some(toBytes("9999999999"))]
    check row.getInt64Opt(0) == some(9999999999'i64)

  test "getInt64Opt none":
    let row: Row = @[none(seq[byte])]
    check row.getInt64Opt(0) == none(int64)

  test "getFloat32Opt some":
    let row: Row = @[some(toBytes("2.5"))]
    let v = row.getFloat32Opt(0)
    check v.isSome
    check abs(v.get - 2.5'f32) < 1e-6

  test "getFloat32Opt none":
    let row: Row = @[none(seq[byte])]
    check row.getFloat32Opt(0) == none(float32)

  test "getFloatOpt some":
    let row: Row = @[some(toBytes("3.14"))]
    let v = row.getFloatOpt(0)
    check v.isSome
    check abs(v.get - 3.14) < 1e-10

  test "getFloatOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getFloatOpt(0) == none(float64)

  test "getBoolOpt some":
    let row: Row = @[some(toBytes("t"))]
    check row.getBoolOpt(0) == some(true)

  test "getBoolOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getBoolOpt(0) == none(bool)

suite "JSON support":
  test "toPgParam JsonNode object":
    let j = %*{"key": "value", "num": 42}
    let p = toPgParam(j)
    check p.oid == OidJsonb
    check p.format == 0
    check p.value.isSome
    let parsed = parseJson(toString(p.value.get))
    check parsed["key"].getStr == "value"
    check parsed["num"].getInt == 42

  test "toPgParam JsonNode array":
    let j = %*[1, 2, 3]
    let p = toPgParam(j)
    check p.oid == OidJsonb
    let parsed = parseJson(toString(p.value.get))
    check parsed.len == 3

  test "toPgParam JsonNode null":
    let j = newJNull()
    let p = toPgParam(j)
    check toString(p.value.get) == "null"

  test "toPgBinaryParam JsonNode":
    let j = %*{"key": "value"}
    let p = toPgBinaryParam(j)
    check p.oid == OidJsonb
    check p.format == 1
    check p.value.isSome
    let data = p.value.get
    check data[0] == 1 # version byte
    var jsonStr = newString(data.len - 1)
    for i in 1 ..< data.len:
      jsonStr[i - 1] = char(data[i])
    let parsed = parseJson(jsonStr)
    check parsed["key"].getStr == "value"

  test "getJson text format":
    let row: Row = @[some(toBytes("""{"a":1,"b":"hello"}"""))]
    let j = row.getJson(0)
    check j["a"].getInt == 1
    check j["b"].getStr == "hello"

  test "getJson array":
    let row: Row = @[some(toBytes("[1,2,3]"))]
    let j = row.getJson(0)
    check j.kind == JArray
    check j.len == 3

  test "getJson NULL raises":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getJson(0)
    except PgTypeError:
      raised = true
    check raised

  test "getJson invalid raises":
    let row: Row = @[some(toBytes("not json"))]
    var raised = false
    try:
      discard row.getJson(0)
    except PgTypeError:
      raised = true
    check raised

  test "getJsonOpt some":
    let row: Row = @[some(toBytes("""{"x":true}"""))]
    let opt = row.getJsonOpt(0)
    check opt.isSome
    check opt.get["x"].getBool == true

  test "getJsonOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getJsonOpt(0) == none(JsonNode)

  test "getJson binary jsonb":
    let jsonStr = """{"key":"val"}"""
    var data = newSeq[byte](1 + jsonStr.len)
    data[0] = 1
    for i in 0 ..< jsonStr.len:
      data[i + 1] = byte(jsonStr[i])
    let fields = @[mkField(OidJsonb, 1)]
    let row = mkRow(@[some(data)], fields)
    let j = row.getJson(0)
    check j["key"].getStr == "val"

  test "getJson binary json (no version byte)":
    let fields = @[mkField(OidJson, 1)]
    let row = mkRow(@[some(toBytes("""{"a":1}"""))], fields)
    let j = row.getJson(0)
    check j["a"].getInt == 1

  test "getJson binary text fallback":
    let row: Row = @[some(toBytes("""[1,2]"""))]
    let j = row.getJson(0)
    check j.kind == JArray
    check j.len == 2

  test "toPgParam Option[JsonNode] some":
    let p = toPgParam(some(%*{"a": 1}))
    check p.oid == OidJsonb
    check p.value.isSome

  test "toPgParam Option[JsonNode] none":
    let p = toPgParam(none(JsonNode))
    check p.oid == OidJsonb
    check p.value.isNone

  test "toPgBinaryParam Option[JsonNode] some":
    let p = toPgBinaryParam(some(%*{"a": 1}))
    check p.oid == OidJsonb
    check p.format == 1
    check p.value.isSome
    check p.value.get[0] == 1 # version byte

  test "toPgBinaryParam Option[JsonNode] none":
    let p = toPgBinaryParam(none(JsonNode))
    check p.oid == OidJsonb
    check p.format == 1
    check p.value.isNone

  test "toPgBinaryParam JsonNode null":
    let p = toPgBinaryParam(newJNull())
    check p.oid == OidJsonb
    check p.format == 1
    let data = p.value.get
    check data[0] == 1 # version byte
    check toString(data[1 .. ^1]) == "null"

  test "toPgBinaryParam JsonNode empty object":
    let p = toPgBinaryParam(%*{})
    let data = p.value.get
    check data[0] == 1
    check toString(data[1 .. ^1]) == "{}"

  test "roundtrip text":
    let orig = %*{"key": [1, 2, 3], "nested": {"a": true}}
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = row.getJson(0)
    check decoded["key"].len == 3
    check decoded["nested"]["a"].getBool == true

  test "roundtrip binary":
    let orig = %*{"x": 42, "arr": [1, "two", nil], "flag": false}
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidJsonb, 1)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getJson(0)
    check decoded["x"].getInt == 42
    check decoded["arr"][1].getStr == "two"
    check decoded["arr"][2].kind == JNull
    check decoded["flag"].getBool == false

  test "getJson binary NULL raises":
    let fields = @[mkField(OidJsonb, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getJson(0)
    except PgTypeError:
      raised = true
    check raised

  test "getJson scalar string":
    let row: Row = @[some(toBytes("\"hello world\""))]
    let j = row.getJson(0)
    check j.kind == JString
    check j.getStr == "hello world"

  test "getJson scalar number":
    let row: Row = @[some(toBytes("42"))]
    let j = row.getJson(0)
    check j.kind == JInt
    check j.getInt == 42

  test "getJson scalar bool":
    let row: Row = @[some(toBytes("true"))]
    let j = row.getJson(0)
    check j.kind == JBool
    check j.getBool == true

  test "getJson unicode":
    let row: Row = @[some(toBytes("""{"emoji":"🎉","jp":"日本語"}"""))]
    let j = row.getJson(0)
    check j["emoji"].getStr == "🎉"
    check j["jp"].getStr == "日本語"

  test "roundtrip unicode binary":
    let orig = %*{"text": "日本語テスト"}
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidJsonb, 1)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getJson(0)
    check decoded["text"].getStr == "日本語テスト"

suite "columnIndex and columnMap":
  proc mkFieldDesc(name: string): FieldDescription =
    FieldDescription(
      name: name,
      tableOid: 0,
      columnAttrNum: 0,
      typeOid: OidText,
      typeSize: -1,
      typeMod: -1,
      formatCode: 0,
    )

  let fields = @[mkFieldDesc("id"), mkFieldDesc("name"), mkFieldDesc("email")]

  test "columnIndex finds existing column":
    check fields.columnIndex("id") == 0
    check fields.columnIndex("name") == 1
    check fields.columnIndex("email") == 2

  test "columnIndex raises on missing column":
    var raised = false
    try:
      discard fields.columnIndex("missing")
    except PgTypeError:
      raised = true
    check raised

  test "columnIndex is case-sensitive":
    var raised = false
    try:
      discard fields.columnIndex("Name")
    except PgTypeError:
      raised = true
    check raised

  test "columnMap builds complete mapping":
    let m = fields.columnMap()
    check m.len == 3
    check m["id"] == 0
    check m["name"] == 1
    check m["email"] == 2

  test "columnMap with empty fields":
    let empty: seq[FieldDescription] = @[]
    let m = empty.columnMap()
    check m.len == 0

  test "columnIndex with single field":
    let single = @[mkFieldDesc("only")]
    check single.columnIndex("only") == 0

suite "coerceBinaryParam":
  test "matching OID unchanged":
    let p = toPgParam(42'i32)
    let c = coerceBinaryParam(p, OidInt4)
    check c.oid == OidInt4
    check c.format == 1
    check c.value.get == p.value.get

  test "text format unchanged regardless of OID mismatch":
    let p = toPgParam("hello")
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidText
    check c.format == 0
    check c.value.get == p.value.get

  test "server OID 0 unchanged":
    let p = toPgParam(42'i32)
    let c = coerceBinaryParam(p, 0'i32)
    check c.oid == OidInt4
    check c.value.get == p.value.get

  test "NULL value gets server OID":
    let p = PgParam(oid: OidInt4, format: 1, value: none(seq[byte]))
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidInt8
    check c.value.isNone

  test "int2 -> int4":
    let p = toPgParam(42'i16)
    let c = coerceBinaryParam(p, OidInt4)
    check c.oid == OidInt4
    check c.format == 1
    check c.value.get == @(toBE32(42'i32))

  test "int2 -> int4 negative":
    let p = toPgParam(-100'i16)
    let c = coerceBinaryParam(p, OidInt4)
    check c.oid == OidInt4
    check c.value.get == @(toBE32(-100'i32))

  test "int2 -> int8":
    let p = toPgParam(1000'i16)
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidInt8
    check c.format == 1
    check c.value.get == @(toBE64(1000'i64))

  test "int2 -> int8 negative":
    let p = toPgParam(-1'i16)
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidInt8
    check c.value.get == @(toBE64(-1'i64))

  test "int4 -> int8":
    let p = toPgParam(10'i32)
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidInt8
    check c.format == 1
    check c.value.get == @(toBE64(10'i64))

  test "int4 -> int8 negative":
    let p = toPgParam(-999999'i32)
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidInt8
    check c.value.get == @(toBE64(-999999'i64))

  test "int4 -> int8 max int32":
    let p = toPgParam(high(int32))
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidInt8
    check c.value.get == @(toBE64(int64(high(int32))))

  test "int4 -> int8 min int32":
    let p = toPgParam(low(int32))
    let c = coerceBinaryParam(p, OidInt8)
    check c.oid == OidInt8
    check c.value.get == @(toBE64(int64(low(int32))))

  test "float4 -> float8":
    let p = toPgParam(1.5'f32)
    let c = coerceBinaryParam(p, OidFloat8)
    check c.oid == OidFloat8
    check c.format == 1
    check c.value.get == @(toBE64(cast[int64](float64(1.5'f32))))

  test "float4 -> float8 negative":
    let p = toPgParam(-3.14'f32)
    let c = coerceBinaryParam(p, OidFloat8)
    check c.oid == OidFloat8
    check c.value.get == @(toBE64(cast[int64](float64(-3.14'f32))))

  test "incompatible types raise PgTypeError":
    let p = toPgParam(42'i32)
    var raised = false
    try:
      discard coerceBinaryParam(p, OidText)
    except PgTypeError:
      raised = true
    check raised

  test "int8 -> int4 not supported raises":
    let p = toPgParam(42'i64)
    var raised = false
    try:
      discard coerceBinaryParam(p, OidInt4)
    except PgTypeError:
      raised = true
    check raised

  test "float8 -> float4 not supported raises":
    let p = toPgParam(1.5'f64)
    var raised = false
    try:
      discard coerceBinaryParam(p, OidFloat4)
    except PgTypeError:
      raised = true
    check raised

suite "decodeHexPair / decodeByteaEscape error contract":
  test "decodeHexPair string rejects out-of-range index with PgTypeError":
    expect PgTypeError:
      discard decodeHexPair("ab", 1, "hex")
    expect PgTypeError:
      discard decodeHexPair("ab", -1, "hex")
    expect PgTypeError:
      discard decodeHexPair("", 0, "hex")

  test "decodeHexPair openArray rejects out-of-range index with PgTypeError":
    let buf = @[byte('a'), byte('b')]
    expect PgTypeError:
      discard decodeHexPair(buf, 1, "hex")
    expect PgTypeError:
      discard decodeHexPair(buf, -1, "hex")

  test "decodeHexPair accepts valid pair":
    check decodeHexPair("ff", 0, "hex") == 255'u8
    check decodeHexPair(@[byte('0'), byte('a')], 0, "hex") == 10'u8

  test "decodeByteaEscape trailing backslash raises PgTypeError":
    expect PgTypeError:
      discard decodeByteaEscape(['a', '\\'], "bytea")
