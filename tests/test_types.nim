import std/[json, unittest, options, strutils, times, math]

import ../async_postgres/[pg_protocol, pg_types]

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc toString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

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
    check p.format == 0
    check toString(p.value.get) == "42"

  test "int32 negative":
    let p = toPgParam(-1'i32)
    check toString(p.value.get) == "-1"

  test "int32 zero":
    let p = toPgParam(0'i32)
    check toString(p.value.get) == "0"

  test "int64":
    let p = toPgParam(9999999999'i64)
    check p.oid == OidInt8
    check p.format == 0
    check toString(p.value.get) == "9999999999"

  test "int64 negative":
    let p = toPgParam(-9999999999'i64)
    check toString(p.value.get) == "-9999999999"

  test "float64":
    let p = toPgParam(3.14)
    check p.oid == OidFloat8
    check p.format == 0
    check p.value.isSome
    let s = toString(p.value.get)
    check s.len > 0
    # Verify it parses back to approximately the same value
    check abs(parseFloat(s) - 3.14) < 1e-10

  test "float64 zero":
    let p = toPgParam(0.0)
    check toString(p.value.get) == "0.0"

  test "float64 negative":
    let p = toPgParam(-1.5)
    check abs(parseFloat(toString(p.value.get)) - (-1.5)) < 1e-10

  test "bool true":
    let p = toPgParam(true)
    check p.oid == OidBool
    check p.format == 0
    check toString(p.value.get) == "t"

  test "bool false":
    let p = toPgParam(false)
    check p.oid == OidBool
    check toString(p.value.get) == "f"

  test "seq[byte]":
    let data = @[0x01'u8, 0x02, 0xFF]
    let p = toPgParam(data)
    check p.oid == OidBytea
    check p.format == 0
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
    check p.format == 0
    check toString(p.value.get) == "100"

  test "int16 negative":
    let p = toPgParam(-32000'i16)
    check toString(p.value.get) == "-32000"

  test "float32":
    let p = toPgParam(1.5'f32)
    check p.oid == OidFloat4
    check p.format == 0
    check p.value.isSome
    let s = toString(p.value.get)
    check abs(parseFloat(s) - 1.5) < 1e-5

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
    check toString(p.value.get) == "42"

  test "none int32":
    let p = toPgParam(none(int32))
    check p.value.isNone

  test "some bool":
    let p = toPgParam(some(true))
    check p.oid == OidBool
    check toString(p.value.get) == "t"

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

  test "getInt":
    let row = @[some(toBytes("42"))]
    check row.getInt(0) == 42'i32

  test "getInt negative":
    let row = @[some(toBytes("-7"))]
    check row.getInt(0) == -7'i32

  test "getInt64":
    let row = @[some(toBytes("9999999999"))]
    check row.getInt64(0) == 9999999999'i64

  test "getFloat":
    let row = @[some(toBytes("3.14"))]
    check abs(row.getFloat(0) - 3.14) < 1e-10

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
  test "all toPgParam use text format":
    check toPgParam("x").format == 0
    check toPgParam(1'i16).format == 0
    check toPgParam(1'i32).format == 0
    check toPgParam(1'i64).format == 0
    check toPgParam(1.0'f32).format == 0
    check toPgParam(1.0).format == 0
    check toPgParam(true).format == 0
    check toPgParam(@[1'u8]).format == 0
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

  test "raw bytes (no hex prefix)":
    let raw = @[0x01'u8, 0x02, 0x03]
    let row = @[some(raw)]
    let b = row.getBytes(0)
    check b == raw

  test "NULL raises":
    let row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getBytes(0)
    except PgTypeError:
      raised = true
    check raised

suite "getTimestamp accessor":
  test "timestamp without tz":
    let row = @[some(toBytes("2024-01-15 10:30:00.000000"))]
    let dt = row.getTimestamp(0)
    check dt.year == 2024
    check dt.month == mJan
    check dt.monthday == 15
    check dt.hour == 10
    check dt.minute == 30
    check dt.second == 0

  test "timestamp without fractional seconds":
    let row = @[some(toBytes("2024-06-20 14:05:30"))]
    let dt = row.getTimestamp(0)
    check dt.year == 2024
    check dt.month == mJun
    check dt.monthday == 20
    check dt.hour == 14
    check dt.minute == 5
    check dt.second == 30

  test "timestamp with microseconds":
    let row = @[some(toBytes("2024-01-15 10:30:00.123456"))]
    let dt = row.getTimestamp(0)
    check dt.year == 2024
    check dt.hour == 10

  test "invalid timestamp raises":
    let row = @[some(toBytes("not-a-timestamp"))]
    var raised = false
    try:
      discard row.getTimestamp(0)
    except PgTypeError:
      raised = true
    check raised

  test "NULL raises":
    let row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getTimestamp(0)
    except PgTypeError:
      raised = true
    check raised

suite "getDate accessor":
  test "standard date":
    let row = @[some(toBytes("2024-01-15"))]
    let dt = row.getDate(0)
    check dt.year == 2024
    check dt.month == mJan
    check dt.monthday == 15

  test "invalid date raises":
    let row = @[some(toBytes("not-a-date"))]
    var raised = false
    try:
      discard row.getDate(0)
    except PgTypeError:
      raised = true
    check raised

  test "NULL raises":
    let row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getDate(0)
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

  test "PgUuid":
    let uuid = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let p = toPgBinaryParam(uuid)
    check p.oid == OidUuid
    check p.format == 1
    check p.value.get.len == 16
    check p.value.get[0] == 0x55'u8
    check p.value.get[1] == 0x0e'u8

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
  proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
    FieldDescription(
      name: "test",
      tableOid: 0,
      columnAttrNum: 0,
      typeOid: typeOid,
      typeSize: 0,
      typeMod: 0,
      formatCode: formatCode,
    )

  test "getInt binary":
    let data = @[0'u8, 0, 0, 42]
    let row = @[some(data)]
    let fields = @[mkField(OidInt4, 1)]
    check row.getInt(0, fields) == 42'i32

  test "getInt text fallback":
    let row = @[some(toBytes("42"))]
    let fields = @[mkField(OidInt4, 0)]
    check row.getInt(0, fields) == 42'i32

  test "getInt64 binary int8":
    let data = @[0'u8, 0, 0, 0, 0, 0, 0, 42]
    let row = @[some(data)]
    let fields = @[mkField(OidInt8, 1)]
    check row.getInt64(0, fields) == 42'i64

  test "getInt64 binary int4 promotion":
    let data = @[0'u8, 0, 0, 42]
    let row = @[some(data)]
    let fields = @[mkField(OidInt4, 1)]
    check row.getInt64(0, fields) == 42'i64

  test "getInt64 binary int2 promotion":
    let data = @[0'u8, 42]
    let row = @[some(data)]
    let fields = @[mkField(OidInt2, 1)]
    check row.getInt64(0, fields) == 42'i64

  test "getFloat binary float8":
    let p = toPgBinaryParam(3.14)
    let row = @[p.value]
    let fields = @[mkField(OidFloat8, 1)]
    check abs(row.getFloat(0, fields) - 3.14) < 1e-10

  test "getFloat binary float4":
    let p = toPgBinaryParam(1.5'f32)
    let row = @[p.value]
    let fields = @[mkField(OidFloat4, 1)]
    check abs(row.getFloat(0, fields) - 1.5) < 1e-5

  test "getBool binary":
    let row = @[some(@[1'u8])]
    let fields = @[mkField(OidBool, 1)]
    check row.getBool(0, fields) == true

  test "getBool binary false":
    let row = @[some(@[0'u8])]
    let fields = @[mkField(OidBool, 1)]
    check row.getBool(0, fields) == false

  test "getBytes binary":
    let data = @[0xDE'u8, 0xAD, 0xBE, 0xEF]
    let row = @[some(data)]
    let fields = @[mkField(OidBytea, 1)]
    check row.getBytes(0, fields) == data

  test "getStr binary text":
    let row = @[some(toBytes("hello"))]
    let fields = @[mkField(OidText, 1)]
    check row.getStr(0, fields) == "hello"

  test "getStr binary int4":
    let data = @[0'u8, 0, 0, 42]
    let row = @[some(data)]
    let fields = @[mkField(OidInt4, 1)]
    check row.getStr(0, fields) == "42"

  test "getTimestamp binary":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgBinaryParam(dt)
    let row = @[p.value]
    let fields = @[mkField(OidTimestamp, 1)]
    let result = row.getTimestamp(0, fields)
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
    let row = @[p.value]
    let fields = @[mkField(OidDate, 1)]
    let result = row.getDate(0, fields)
    check result.year == 2024
    check result.month == mJan
    check result.monthday == 15

suite "Row type alias":
  test "Row is seq[Option[seq[byte]]]":
    let row: Row = @[some(toBytes("hello")), none(seq[byte])]
    check row.getStr(0) == "hello"
    check row.isNull(1)

suite "affectedRows":
  test "UPDATE tag":
    check affectedRows("UPDATE 3") == 3

  test "INSERT tag":
    check affectedRows("INSERT 0 1") == 1

  test "DELETE tag":
    check affectedRows("DELETE 0") == 0

  test "SELECT tag":
    check affectedRows("SELECT 5") == 5

  test "empty tag":
    check affectedRows("") == 0

  test "non-numeric tag":
    check affectedRows("CREATE TABLE") == 0

suite "Option accessors":
  test "getStrOpt some":
    let row: Row = @[some(toBytes("hello"))]
    check row.getStrOpt(0) == some("hello")

  test "getStrOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getStrOpt(0) == none(string)

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
    proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
      FieldDescription(
        name: "test",
        tableOid: 0,
        columnAttrNum: 0,
        typeOid: typeOid,
        typeSize: 0,
        typeMod: 0,
        formatCode: formatCode,
      )

    let jsonStr = """{"key":"val"}"""
    var data = newSeq[byte](1 + jsonStr.len)
    data[0] = 1
    for i in 0 ..< jsonStr.len:
      data[i + 1] = byte(jsonStr[i])
    let row: Row = @[some(data)]
    let fields = @[mkField(OidJsonb, 1)]
    let j = row.getJson(0, fields)
    check j["key"].getStr == "val"

  test "getJson binary json (no version byte)":
    proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
      FieldDescription(
        name: "test",
        tableOid: 0,
        columnAttrNum: 0,
        typeOid: typeOid,
        typeSize: 0,
        typeMod: 0,
        formatCode: formatCode,
      )

    let row: Row = @[some(toBytes("""{"a":1}"""))]
    let fields = @[mkField(OidJson, 1)]
    let j = row.getJson(0, fields)
    check j["a"].getInt == 1

  test "getJson binary text fallback":
    proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
      FieldDescription(
        name: "test",
        tableOid: 0,
        columnAttrNum: 0,
        typeOid: typeOid,
        typeSize: 0,
        typeMod: 0,
        formatCode: formatCode,
      )

    let row: Row = @[some(toBytes("""[1,2]"""))]
    let fields = @[mkField(OidJsonb, 0)]
    let j = row.getJson(0, fields)
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
    proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
      FieldDescription(
        name: "test",
        tableOid: 0,
        columnAttrNum: 0,
        typeOid: typeOid,
        typeSize: 0,
        typeMod: 0,
        formatCode: formatCode,
      )

    let orig = %*{"x": 42, "arr": [1, "two", nil], "flag": false}
    let p = toPgBinaryParam(orig)
    let row: Row = @[p.value]
    let fields = @[mkField(OidJsonb, 1)]
    let decoded = row.getJson(0, fields)
    check decoded["x"].getInt == 42
    check decoded["arr"][1].getStr == "two"
    check decoded["arr"][2].kind == JNull
    check decoded["flag"].getBool == false

  test "getJson binary NULL raises":
    proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
      FieldDescription(
        name: "test",
        tableOid: 0,
        columnAttrNum: 0,
        typeOid: typeOid,
        typeSize: 0,
        typeMod: 0,
        formatCode: formatCode,
      )

    let row: Row = @[none(seq[byte])]
    let fields = @[mkField(OidJsonb, 1)]
    var raised = false
    try:
      discard row.getJson(0, fields)
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
    proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
      FieldDescription(
        name: "test",
        tableOid: 0,
        columnAttrNum: 0,
        typeOid: typeOid,
        typeSize: 0,
        typeMod: 0,
        formatCode: formatCode,
      )

    let orig = %*{"text": "日本語テスト"}
    let p = toPgBinaryParam(orig)
    let row: Row = @[p.value]
    let fields = @[mkField(OidJsonb, 1)]
    let decoded = row.getJson(0, fields)
    check decoded["text"].getStr == "日本語テスト"

suite "Array OID constants":
  test "array OID values":
    check OidBoolArray == 1000'i32
    check OidInt2Array == 1005'i32
    check OidInt4Array == 1007'i32
    check OidInt8Array == 1016'i32
    check OidFloat4Array == 1021'i32
    check OidFloat8Array == 1022'i32
    check OidTextArray == 1009'i32
    check OidVarcharArray == 1015'i32

suite "Array toPgParam":
  test "seq[int32]":
    let p = toPgParam(@[1'i32, 2, 3])
    check p.oid == OidInt4Array
    check p.format == 0
    check toString(p.value.get) == "{1,2,3}"

  test "seq[int32] empty":
    let p = toPgParam(newSeq[int32]())
    check p.oid == OidInt4Array
    check toString(p.value.get) == "{}"

  test "seq[int16]":
    let p = toPgParam(@[10'i16, -20'i16])
    check p.oid == OidInt2Array
    check toString(p.value.get) == "{10,-20}"

  test "seq[int64]":
    let p = toPgParam(@[9999999999'i64, -1'i64])
    check p.oid == OidInt8Array
    check toString(p.value.get) == "{9999999999,-1}"

  test "seq[float32]":
    let p = toPgParam(@[1.5'f32, 2.5'f32])
    check p.oid == OidFloat4Array
    check toString(p.value.get).startsWith("{")

  test "seq[float64]":
    let p = toPgParam(@[3.14, 2.72])
    check p.oid == OidFloat8Array
    check toString(p.value.get).startsWith("{")

  test "seq[bool]":
    let p = toPgParam(@[true, false, true])
    check p.oid == OidBoolArray
    check toString(p.value.get) == "{t,f,t}"

  test "seq[string]":
    let p = toPgParam(@["hello", "world"])
    check p.oid == OidTextArray
    check toString(p.value.get) == "{\"hello\",\"world\"}"

  test "seq[string] with escaping":
    let p = toPgParam(@["a\"b", "c\\d"])
    check p.oid == OidTextArray
    check toString(p.value.get) == "{\"a\\\"b\",\"c\\\\d\"}"

  test "seq[string] empty":
    let p = toPgParam(newSeq[string]())
    check toString(p.value.get) == "{}"

  test "Option[seq[int32]] some":
    let p = toPgParam(some(@[1'i32, 2]))
    check p.oid == OidInt4Array
    check p.value.isSome

  test "Option[seq[int32]] none":
    let p = toPgParam(none(seq[int32]))
    check p.oid == OidInt4Array
    check p.value.isNone

suite "parseTextArray":
  test "empty array":
    let elems = parseTextArray("{}")
    check elems.len == 0

  test "simple integers":
    let elems = parseTextArray("{1,2,3}")
    check elems.len == 3
    check elems[0] == some("1")
    check elems[1] == some("2")
    check elems[2] == some("3")

  test "NULL elements":
    let elems = parseTextArray("{1,NULL,3}")
    check elems.len == 3
    check elems[0] == some("1")
    check elems[1].isNone
    check elems[2] == some("3")

  test "quoted strings":
    let elems = parseTextArray("{\"hello\",\"world\"}")
    check elems.len == 2
    check elems[0] == some("hello")
    check elems[1] == some("world")

  test "escaped quotes":
    let elems = parseTextArray("{\"a\\\"b\",\"c\\\\d\"}")
    check elems.len == 2
    check elems[0] == some("a\"b")
    check elems[1] == some("c\\d")

  test "single element":
    let elems = parseTextArray("{42}")
    check elems.len == 1
    check elems[0] == some("42")

  test "all NULL":
    let elems = parseTextArray("{NULL,NULL}")
    check elems.len == 2
    check elems[0].isNone
    check elems[1].isNone

  test "invalid literal raises":
    var raised = false
    try:
      discard parseTextArray("not an array")
    except PgTypeError:
      raised = true
    check raised

suite "Array row accessors":
  test "getIntArray":
    let row: Row = @[some(toBytes("{1,2,3}"))]
    check row.getIntArray(0) == @[1'i32, 2, 3]

  test "getIntArray empty":
    let row: Row = @[some(toBytes("{}"))]
    check row.getIntArray(0).len == 0

  test "getInt16Array":
    let row: Row = @[some(toBytes("{10,-20}"))]
    check row.getInt16Array(0) == @[10'i16, -20'i16]

  test "getInt64Array":
    let row: Row = @[some(toBytes("{9999999999,-1}"))]
    check row.getInt64Array(0) == @[9999999999'i64, -1'i64]

  test "getFloatArray":
    let row: Row = @[some(toBytes("{3.14,2.72}"))]
    let arr = row.getFloatArray(0)
    check arr.len == 2
    check abs(arr[0] - 3.14) < 1e-10
    check abs(arr[1] - 2.72) < 1e-10

  test "getFloat32Array":
    let row: Row = @[some(toBytes("{1.5,2.5}"))]
    let arr = row.getFloat32Array(0)
    check arr.len == 2
    check abs(arr[0] - 1.5'f32) < 1e-5
    check abs(arr[1] - 2.5'f32) < 1e-5

  test "getBoolArray":
    let row: Row = @[some(toBytes("{t,f,t}"))]
    check row.getBoolArray(0) == @[true, false, true]

  test "getStrArray":
    let row: Row = @[some(toBytes("{\"hello\",\"world\"}"))]
    check row.getStrArray(0) == @["hello", "world"]

  test "getStrArray with escapes":
    let row: Row = @[some(toBytes("{\"a\\\"b\",\"c\\\\d\"}"))]
    check row.getStrArray(0) == @["a\"b", "c\\d"]

  test "getIntArray NULL raises":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getIntArray(0)
    except PgTypeError:
      raised = true
    check raised

  test "getIntArray NULL element raises":
    let row: Row = @[some(toBytes("{1,NULL,3}"))]
    var raised = false
    try:
      discard row.getIntArray(0)
    except PgTypeError:
      raised = true
    check raised

suite "Array Opt accessors":
  test "getIntArrayOpt some":
    let row: Row = @[some(toBytes("{1,2}"))]
    let v = row.getIntArrayOpt(0)
    check v.isSome
    check v.get == @[1'i32, 2]

  test "getIntArrayOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getIntArrayOpt(0) == none(seq[int32])

  test "getStrArrayOpt some":
    let row: Row = @[some(toBytes("{\"a\",\"b\"}"))]
    let v = row.getStrArrayOpt(0)
    check v.isSome
    check v.get == @["a", "b"]

  test "getStrArrayOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getStrArrayOpt(0) == none(seq[string])

  test "getBoolArrayOpt some":
    let row: Row = @[some(toBytes("{t,f}"))]
    let v = row.getBoolArrayOpt(0)
    check v.isSome
    check v.get == @[true, false]

  test "getBoolArrayOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getBoolArrayOpt(0) == none(seq[bool])

  test "getInt64ArrayOpt some":
    let row: Row = @[some(toBytes("{100}"))]
    let v = row.getInt64ArrayOpt(0)
    check v.isSome
    check v.get == @[100'i64]

  test "getFloatArrayOpt some":
    let row: Row = @[some(toBytes("{1.5}"))]
    let v = row.getFloatArrayOpt(0)
    check v.isSome
    check abs(v.get[0] - 1.5) < 1e-10

  test "getFloat32ArrayOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getFloat32ArrayOpt(0) == none(seq[float32])

  test "getInt16ArrayOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getInt16ArrayOpt(0) == none(seq[int16])

suite "PgNumeric":
  proc mkField(typeOid: int32, formatCode: int16): FieldDescription =
    FieldDescription(
      name: "test",
      tableOid: 0,
      columnAttrNum: 0,
      typeOid: typeOid,
      typeSize: 0,
      typeMod: 0,
      formatCode: formatCode,
    )

  test "toPgParam PgNumeric":
    let p = toPgParam(PgNumeric("123.456"))
    check p.oid == OidNumeric
    check p.format == 0
    check toString(p.value.get) == "123.456"

  test "toPgBinaryParam PgNumeric uses text format":
    let p = toPgBinaryParam(PgNumeric("99.99"))
    check p.oid == OidNumeric
    check p.format == 0

  test "getNumeric text format":
    let row: Row = @[some(toBytes("12345.6789012345678901234567890"))]
    let v = row.getNumeric(0)
    check $v == "12345.6789012345678901234567890"

  test "getNumericOpt some":
    let row: Row = @[some(toBytes("99.99"))]
    let v = row.getNumericOpt(0)
    check v.isSome
    check $v.get == "99.99"

  test "getNumericOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getNumericOpt(0) == none(PgNumeric)

  test "PgNumeric equality":
    check PgNumeric("100.00") == PgNumeric("100.00")
    check PgNumeric("1.0") != PgNumeric("1.00")

  test "decodeNumericBinary - positive integer":
    # 1234: ndigits=1, weight=0, sign=0, dscale=0, digit=1234
    let data: seq[byte] = @[
      0x00'u8,
      0x01, # ndigits = 1
      0x00,
      0x00, # weight = 0
      0x00,
      0x00, # sign = positive
      0x00,
      0x00, # dscale = 0
      0x04,
      0xD2, # digit = 1234
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "1234"

  test "decodeNumericBinary - positive with decimal":
    # 12345.6789: ndigits=2, weight=1, sign=0, dscale=4, digits=[1, 2345.6789]
    # weight=1 means first digit is 10^(1*4)=10000s place
    # digit0=1 -> 10000, digit1=2345 -> integer part done (weight+1=2 groups)
    # Wait, let me recalculate:
    # 12345.6789 in base-10000:
    #   integer part: 12345 = 1*10000 + 2345 -> digits [1, 2345], weight=1
    #   fractional: .6789 -> digit 6789
    # So: ndigits=3, weight=1, dscale=4, digits=[1, 2345, 6789]
    let data: seq[byte] = @[
      0x00'u8,
      0x03, # ndigits = 3
      0x00,
      0x01, # weight = 1
      0x00,
      0x00, # sign = positive
      0x00,
      0x04, # dscale = 4
      0x00,
      0x01, # digit = 1
      0x09,
      0x29, # digit = 2345
      0x1A,
      0x85, # digit = 6789
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "12345.6789"

  test "decodeNumericBinary - negative":
    # -42.50: ndigits=2, weight=0, sign=0x4000, dscale=2, digits=[42, 5000]
    let data: seq[byte] = @[
      0x00'u8,
      0x02, # ndigits = 2
      0x00,
      0x00, # weight = 0
      0x40,
      0x00, # sign = negative
      0x00,
      0x02, # dscale = 2
      0x00,
      0x2A, # digit = 42
      0x13,
      0x88, # digit = 5000
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "-42.50"

  test "decodeNumericBinary - zero":
    # 0: ndigits=0, weight=0, sign=0, dscale=0
    let data: seq[byte] = @[
      0x00'u8,
      0x00, # ndigits = 0
      0x00,
      0x00, # weight = 0
      0x00,
      0x00, # sign = positive
      0x00,
      0x00, # dscale = 0
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "0"

  test "decodeNumericBinary - NaN":
    let data: seq[byte] = @[
      0x00'u8,
      0x00,
      0x00,
      0x00,
      0xC0,
      0x00, # sign = NaN
      0x00,
      0x00,
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "NaN"

  test "decodeNumericBinary - small decimal 0.00001":
    # 0.00001: weight=-2 (first digit at 10^(-2*4)=10^-8 place)
    # digit = 100 (0.00000100 scaled up, but actually:
    # weight=-2 means group starts at position -2, so 10^(-8)
    # 0.00001 = 10^-5 = 1000 * 10^-8 -> digit at weight=-2 is 100? No.
    # Actually: 0.00001 in base-10000 representation
    # weight = -2 means first digit represents 10^(-2*4) = 10^-8 range
    # Wait, let me think again. weight means the first digit has value digit * 10000^weight
    # So for 0.00001:
    # We need: value = digit * 10000^weight
    # 0.00001 = 0.00001
    # If weight = -1: digit * 10000^-1 = digit * 0.0001, so digit = 0.1 (not integer)
    # If weight = -2: digit * 10000^-2 = digit * 0.00000001, digit = 1000
    # So ndigits=1, weight=-2, dscale=5, digits=[1000]
    let data: seq[byte] = @[
      0x00'u8,
      0x01, # ndigits = 1
      0xFF,
      0xFE, # weight = -2 (as int16)
      0x00,
      0x00, # sign = positive
      0x00,
      0x05, # dscale = 5
      0x03,
      0xE8, # digit = 1000
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "0.00001"

  test "decodeNumericBinary - zero with scale":
    let data: seq[byte] = @[
      0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 # dscale = 2
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "0.00"

  test "decodeNumericBinary - large integer with trailing zero groups":
    # 10000000: weight=1, ndigits=1, digit=1000
    # 1000 * 10000^1 = 10000000
    let data: seq[byte] = @[
      0x00'u8,
      0x01, # ndigits = 1
      0x00,
      0x01, # weight = 1
      0x00,
      0x00, # sign = positive
      0x00,
      0x00, # dscale = 0
      0x03,
      0xE8, # digit = 1000
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "10000000"

  test "decodeNumericBinary - weight=-1 (0.5)":
    # 0.5: weight=-1, ndigits=1, digit=5000, dscale=1
    let data: seq[byte] = @[
      0x00'u8,
      0x01,
      0xFF,
      0xFF, # weight = -1
      0x00,
      0x00,
      0x00,
      0x01, # dscale = 1
      0x13,
      0x88, # digit = 5000
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "0.5"

  test "decodeNumericBinary - multiple fractional groups":
    # 0.123456789012: weight=-1, dscale=12
    # digits: [1234, 5678, 9012]
    let data: seq[byte] = @[
      0x00'u8,
      0x03, # ndigits = 3
      0xFF,
      0xFF, # weight = -1
      0x00,
      0x00,
      0x00,
      0x0C, # dscale = 12
      0x04,
      0xD2, # digit = 1234
      0x16,
      0x2E, # digit = 5678
      0x23,
      0x34, # digit = 9012
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "0.123456789012"

  test "decodeNumericBinary - negative with fractional":
    # -0.0025: weight=-1, sign=0x4000, dscale=4, digit=25
    let data: seq[byte] = @[
      0x00'u8,
      0x01,
      0xFF,
      0xFF, # weight = -1
      0x40,
      0x00, # sign = negative
      0x00,
      0x04, # dscale = 4
      0x00,
      0x19, # digit = 25
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check $row.getNumeric(0, fields) == "-0.0025"

  test "getNumeric text NULL raises":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getNumeric(0)
    except PgTypeError:
      raised = true
    check raised

  test "getNumeric binary NULL raises":
    let row: Row = @[none(seq[byte])]
    let fields = @[mkField(OidNumeric, 1)]
    var raised = false
    try:
      discard row.getNumeric(0, fields)
    except PgTypeError:
      raised = true
    check raised

  test "getNumeric binary text fallback":
    let row: Row = @[some(toBytes("999.123"))]
    let fields = @[mkField(OidNumeric, 0)] # formatCode=0 → text
    check $row.getNumeric(0, fields) == "999.123"

  test "getStr binary with OidNumeric":
    # 42: ndigits=1, weight=0, dscale=0, digit=42
    let data: seq[byte] = @[
      0x00'u8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A # digit = 42
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check row.getStr(0, fields) == "42"

  test "getStr binary OidNumeric with decimal":
    # 3.14: weight=0, dscale=2, digits=[3, 1400]
    let data: seq[byte] = @[
      0x00'u8,
      0x02,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x02, # dscale = 2
      0x00,
      0x03, # digit = 3
      0x05,
      0x78, # digit = 1400
    ]
    let row: Row = @[some(data)]
    let fields = @[mkField(OidNumeric, 1)]
    check row.getStr(0, fields) == "3.14"

  test "$ PgNumeric":
    check $PgNumeric("12345.67890") == "12345.67890"
    check $PgNumeric("0") == "0"
    check $PgNumeric("-999") == "-999"

  test "toPgParam Option[PgNumeric] some":
    let p = toPgParam(some(PgNumeric("42.00")))
    check p.oid == OidNumeric
    check p.format == 0
    check p.value.isSome
    check toString(p.value.get) == "42.00"

  test "toPgParam Option[PgNumeric] none":
    let p = toPgParam(none(PgNumeric))
    check p.oid == OidNumeric
    check p.value.isNone

  test "toPgBinaryParam Option[PgNumeric] some":
    let p = toPgBinaryParam(some(PgNumeric("1.5")))
    check p.oid == OidNumeric
    check p.format == 0 # PgNumeric always uses text format
    check p.value.isSome

  test "toPgBinaryParam Option[PgNumeric] none":
    let p = toPgBinaryParam(none(PgNumeric))
    check p.oid == OidNumeric
    check p.value.isNone
