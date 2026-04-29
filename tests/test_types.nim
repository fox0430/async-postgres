import
  std/[
    json, unittest, options, strutils, tables, times, math, net, typetraits, importutils
  ]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_client {.all.}

type
  UsPostalCode = distinct string
  SmallCount = distinct int16
  PositiveInt = distinct int32
  ProbabilityF = distinct float64
  BigCount = distinct int64
  IsActive = distinct bool

proc `==`(a, b: UsPostalCode): bool {.borrow.}
proc `==`(a, b: SmallCount): bool {.borrow.}
proc `==`(a, b: PositiveInt): bool {.borrow.}
proc `==`(a, b: BigCount): bool {.borrow.}
proc `==`(a, b: IsActive): bool {.borrow.}
proc `$`(v: UsPostalCode): string {.borrow.}
proc `$`(v: SmallCount): string {.borrow.}
proc `$`(v: PositiveInt): string {.borrow.}

pgDomain(UsPostalCode, string)
pgDomain(SmallCount, int16)
pgDomain(PositiveInt, int32)
pgDomain(ProbabilityF, float64, 90001)
pgDomain(BigCount, int64)
pgDomain(IsActive, bool)

proc toString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

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

proc mkRow(cells: seq[Option[seq[byte]]], fields: seq[FieldDescription]): Row =
  ## Build a Row from cell data with format metadata from fields.
  let rd = RowData(
    numCols: int16(cells.len),
    buf: @[],
    cellIndex: newSeq[int32](cells.len * 2),
    colFormats: newSeq[int16](fields.len),
    colTypeOids: newSeq[int32](fields.len),
    fields: fields,
  )
  for i in 0 ..< fields.len:
    rd.colFormats[i] = fields[i].formatCode
    rd.colTypeOids[i] = fields[i].typeOid
  for i, cell in cells:
    if cell.isNone:
      rd.cellIndex[i * 2] = 0'i32
      rd.cellIndex[i * 2 + 1] = -1'i32
    else:
      let data = cell.get
      rd.cellIndex[i * 2] = int32(rd.buf.len)
      rd.cellIndex[i * 2 + 1] = int32(data.len)
      rd.buf.add(data)
  initRow(rd, 0)

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

  test "column index out of range raises IndexDefect":
    let row = @[some(toBytes("hello"))]
    expect IndexDefect:
      discard row.getStr(1)
    expect IndexDefect:
      discard row.getStr(-1)

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

  test "getInt64":
    let row = @[some(toBytes("9999999999"))]
    check row.getInt64(0) == 9999999999'i64

  test "getInt64 invalid value raises":
    let row = @[some(toBytes("xyz"))]
    expect PgTypeError:
      discard row.getInt64(0)

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
  test "toPgParam uses binary for numeric and bool, text for others":
    check toPgParam("x").format == 0
    check toPgParam(1'i16).format == 1
    check toPgParam(1'i32).format == 1
    check toPgParam(1'i64).format == 1
    check toPgParam(1.0'f32).format == 1
    check toPgParam(1.0).format == 1
    check toPgParam(true).format == 1
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

suite "PgTime":
  test "$PgTime without microseconds":
    let t = PgTime(hour: 14, minute: 30, second: 0, microsecond: 0)
    check $t == "14:30:00"

  test "$PgTime with microseconds":
    let t = PgTime(hour: 9, minute: 5, second: 3, microsecond: 123456)
    check $t == "09:05:03.123456"

  test "$PgTime with leading zero microseconds":
    let t = PgTime(hour: 0, minute: 0, second: 0, microsecond: 100)
    check $t == "00:00:00.000100"

  test "toPgParam OID and format":
    let p = toPgParam(PgTime(hour: 10, minute: 20, second: 30))
    check p.oid == OidTime
    check p.format == 0

  test "getTime text without microseconds":
    let row = @[some(toBytes("14:30:00"))]
    let t = row.getTime(0)
    check t.hour == 14
    check t.minute == 30
    check t.second == 0
    check t.microsecond == 0

  test "getTime text with microseconds":
    let row = @[some(toBytes("09:05:03.123456"))]
    let t = row.getTime(0)
    check t.hour == 9
    check t.minute == 5
    check t.second == 3
    check t.microsecond == 123456

  test "getTime text with partial microseconds":
    let row = @[some(toBytes("10:00:00.5"))]
    let t = row.getTime(0)
    check t.microsecond == 500000

  test "getTime invalid raises":
    let row = @[some(toBytes("not-a-time"))]
    var raised = false
    try:
      discard row.getTime(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTime out-of-range hour raises":
    let row = @[some(toBytes("25:00:00"))]
    var raised = false
    try:
      discard row.getTime(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTime out-of-range minute raises":
    let row = @[some(toBytes("12:60:00"))]
    var raised = false
    try:
      discard row.getTime(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTime out-of-range second raises":
    let row = @[some(toBytes("12:00:60"))]
    var raised = false
    try:
      discard row.getTime(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTime NULL raises":
    let row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getTime(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTime binary roundtrip":
    let t = PgTime(hour: 14, minute: 30, second: 45, microsecond: 123456)
    let p = toPgBinaryParam(t)
    check p.oid == OidTime
    check p.format == 1
    let fields = @[mkField(OidTime, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getTime(0)
    check result == t

  test "getTime binary midnight":
    let t = PgTime(hour: 0, minute: 0, second: 0, microsecond: 0)
    let p = toPgBinaryParam(t)
    let fields = @[mkField(OidTime, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getTime(0)
    check result == t

  test "getTimeOpt NULL returns none":
    let row = @[none(seq[byte])]
    check row.getTimeOpt(0).isNone

  test "getTimeOpt returns some":
    let row = @[some(toBytes("10:30:00"))]
    let opt = row.getTimeOpt(0)
    check opt.isSome
    check opt.get().hour == 10

suite "PgTimeTz":
  test "$PgTimeTz positive offset":
    let t = PgTimeTz(hour: 14, minute: 30, second: 0, microsecond: 0, utcOffset: 18000)
    check $t == "14:30:00+05:00"

  test "$PgTimeTz negative offset":
    let t = PgTimeTz(hour: 14, minute: 30, second: 0, microsecond: 0, utcOffset: -12600)
    check $t == "14:30:00-03:30"

  test "$PgTimeTz UTC":
    let t = PgTimeTz(hour: 10, minute: 0, second: 0, microsecond: 0, utcOffset: 0)
    check $t == "10:00:00+00:00"

  test "$PgTimeTz with microseconds":
    let t =
      PgTimeTz(hour: 9, minute: 5, second: 3, microsecond: 123456, utcOffset: 3600)
    check $t == "09:05:03.123456+01:00"

  test "toPgParam OID":
    let p = toPgParam(PgTimeTz(hour: 10, minute: 0, second: 0, utcOffset: 0))
    check p.oid == OidTimeTz
    check p.format == 0

  test "getTimeTz text +HH":
    let row = @[some(toBytes("14:30:00+05"))]
    let t = row.getTimeTz(0)
    check t.hour == 14
    check t.minute == 30
    check t.utcOffset == 18000

  test "getTimeTz text -HH:MM":
    let row = @[some(toBytes("10:00:00.123456-03:30"))]
    let t = row.getTimeTz(0)
    check t.hour == 10
    check t.minute == 0
    check t.microsecond == 123456
    check t.utcOffset == -12600

  test "getTimeTz text +HH:MM:SS":
    let row = @[some(toBytes("12:00:00+05:30:15"))]
    let t = row.getTimeTz(0)
    check t.utcOffset == 19815

  test "getTimeTz invalid raises":
    let row = @[some(toBytes("not-a-time"))]
    var raised = false
    try:
      discard row.getTimeTz(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTimeTz missing offset raises":
    let row = @[some(toBytes("14:30:00"))]
    var raised = false
    try:
      discard row.getTimeTz(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTimeTz binary roundtrip":
    let t =
      PgTimeTz(hour: 14, minute: 30, second: 45, microsecond: 123456, utcOffset: 18000)
    let p = toPgBinaryParam(t)
    check p.oid == OidTimeTz
    check p.format == 1
    let fields = @[mkField(OidTimeTz, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getTimeTz(0)
    check result == t

  test "getTimeTz binary negative offset":
    let t = PgTimeTz(hour: 10, minute: 0, second: 0, microsecond: 0, utcOffset: -12600)
    let p = toPgBinaryParam(t)
    let fields = @[mkField(OidTimeTz, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getTimeTz(0)
    check result == t

  test "getTimeTzOpt NULL returns none":
    let row = @[none(seq[byte])]
    check row.getTimeTzOpt(0).isNone

suite "date parameter encoding":
  test "toPgDateParam OID and format":
    let dt = dateTime(2024, mJan, 15, 0, 0, 0, 0, utc())
    let p = toPgDateParam(dt)
    check p.oid == OidDate
    check p.format == 0
    check p.value.isSome
    let s = cast[string](p.value.get())
    check s == "2024-01-15"

  test "toPgBinaryDateParam roundtrip":
    let dt = dateTime(2024, mJan, 15, 0, 0, 0, 0, utc())
    let p = toPgBinaryDateParam(dt)
    check p.oid == OidDate
    check p.format == 1
    let fields = @[mkField(OidDate, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getDate(0)
    check result.year == 2024
    check result.month == mJan
    check result.monthday == 15

suite "getTimestampTz accessor":
  test "timestamptz with tz":
    let row = @[some(toBytes("2024-01-15 10:30:00.000000+05:00"))]
    let dt = row.getTimestampTz(0)
    check dt.year == 2024
    check dt.month == mJan
    check dt.monthday == 15

  test "timestamptz without fractional seconds":
    let row = @[some(toBytes("2024-06-20 14:05:30+00:00"))]
    let dt = row.getTimestampTz(0)
    check dt.year == 2024
    # The parsed DateTime is converted to local timezone by Nim's parse(),
    # so we compare using UTC.
    check dt.utc().hour == 14

  test "invalid timestamptz raises":
    let row = @[some(toBytes("not-a-timestamp"))]
    var raised = false
    try:
      discard row.getTimestampTz(0)
    except PgTypeError:
      raised = true
    check raised

  test "timestamptz binary roundtrip":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgBinaryTimestampTzParam(dt)
    check p.oid == OidTimestampTz
    check p.format == 1
    let fields = @[mkField(OidTimestampTz, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getTimestampTz(0)
    check result.year == 2024
    check result.month == mJan
    check result.monthday == 15
    check result.hour == 10
    check result.minute == 30

  test "toPgTimestampTzParam OID":
    let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
    let p = toPgTimestampTzParam(dt)
    check p.oid == OidTimestampTz
    check p.format == 0

  test "getTimestampTzOpt NULL returns none":
    let row = @[none(seq[byte])]
    check row.getTimestampTzOpt(0).isNone

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

  test "getDate binary unexpected length":
    let fields = @[mkField(OidDate, 1)]
    let row = mkRow(@[some(newSeq[byte](2))], fields) # 2 bytes (expected 4)
    expect PgTypeError:
      discard row.getDate(0)

  test "getTime binary unexpected length":
    let fields = @[mkField(OidTime, 1)]
    let row = mkRow(@[some(newSeq[byte](4))], fields)
    expect PgTypeError:
      discard row.getTime(0)

  test "getTimeTz binary unexpected length":
    let fields = @[mkField(OidTimeTz, 1)]
    let row = mkRow(@[some(newSeq[byte](8))], fields) # 8 bytes (expected 12)
    expect PgTypeError:
      discard row.getTimeTz(0)

  test "getStr binary int4 size mismatch":
    let fields = @[mkField(OidInt4, 1)]
    let row = mkRow(@[some(@[0'u8, 0, 0])], fields) # 3 bytes
    expect PgTypeError:
      discard row.getStr(0)

  test "getStr binary unknown OID size mismatch is tolerated":
    # text/varchar/bytea: else branch keeps raw-bytes fallback
    let fields = @[mkField(OidText, 1)]
    let row = mkRow(@[some(toBytes("hello"))], fields)
    check row.getStr(0) == "hello"

  test "getIntArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0]] # 3 bytes (expected 4)
    let bin = encodeBinaryArray(OidInt4, elements)
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getIntArray(0)

  test "getInt16Array binary element length mismatch":
    let elements = @[@[0'u8, 0, 0]] # 3 bytes (expected 2)
    let bin = encodeBinaryArray(OidInt2, elements)
    let fields = @[mkField(OidInt2Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getInt16Array(0)

  test "getInt64Array binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0]] # 4 bytes (expected 8)
    let bin = encodeBinaryArray(OidInt8, elements)
    let fields = @[mkField(OidInt8Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getInt64Array(0)

  test "getFloatArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0]] # 3 bytes (expected 4 or 8)
    let bin = encodeBinaryArray(OidFloat8, elements)
    let fields = @[mkField(OidFloat8Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getFloatArray(0)

  test "getFloat32Array binary element length mismatch":
    let elements = @[@[0'u8, 0]] # 2 bytes (expected 4)
    let bin = encodeBinaryArray(OidFloat4, elements)
    let fields = @[mkField(OidFloat4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getFloat32Array(0)

  test "getBoolArray binary element length mismatch":
    let elements = @[@[1'u8, 1]] # 2 bytes (expected 1)
    let bin = encodeBinaryArray(OidBool, elements)
    let fields = @[mkField(OidBoolArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getBoolArray(0)

  test "getMoneyArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0]] # 4 bytes (expected 8)
    let bin = encodeBinaryArray(OidMoney, elements)
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getMoneyArray(0)

  test "getPath binary npts exceeds clen":
    # closed(1) + npts(4) = 5-byte header; claim 100 points (1600 bytes) but pass 5 bytes
    var bin = newSeq[byte](5)
    bin[0] = 0
    bin[1] = 0
    bin[2] = 0
    bin[3] = 0
    bin[4] = 100 # npts = 100
    let fields = @[mkField(OidPath, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getPath(0)

  test "getPath binary header truncated":
    let fields = @[mkField(OidPath, 1)]
    let row = mkRow(@[some(@[0'u8, 0])], fields) # 2 bytes (need >= 5)
    expect PgTypeError:
      discard row.getPath(0)

  test "getPolygon binary npts exceeds clen":
    var bin = newSeq[byte](4)
    bin[0] = 0
    bin[1] = 0
    bin[2] = 0
    bin[3] = 50 # npts = 50, expects 800 bytes of data
    let fields = @[mkField(OidPolygon, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getPolygon(0)

  test "getPolygon binary header truncated":
    let fields = @[mkField(OidPolygon, 1)]
    let row = mkRow(@[some(@[0'u8, 0])], fields) # 2 bytes (need >= 4)
    expect PgTypeError:
      discard row.getPolygon(0)

suite "Row type alias":
  test "Row is seq[Option[seq[byte]]]":
    let row: Row = @[some(toBytes("hello")), none(seq[byte])]
    check row.getStr(0) == "hello"
    check row.isNull(1)

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

suite "Array toPgParam (binary)":
  test "seq[int32]":
    let p = toPgParam(@[1'i32, 2, 3])
    check p.oid == OidInt4Array
    check p.format == 1
    let data = p.value.get
    # Header: ndim=1, has_null=0, elem_oid=OidInt4, dim_len=3, lower_bound=1
    check fromBE32(data[0 .. 3]) == 1'i32 # ndim
    check fromBE32(data[4 .. 7]) == 0'i32 # has_null
    check fromBE32(data[8 .. 11]) == OidInt4 # elem_oid
    check fromBE32(data[12 .. 15]) == 3'i32 # dim_len
    check fromBE32(data[16 .. 19]) == 1'i32 # lower_bound
    # Element 0: len=4, value=1
    check fromBE32(data[20 .. 23]) == 4'i32
    check fromBE32(data[24 .. 27]) == 1'i32
    # Element 1: len=4, value=2
    check fromBE32(data[28 .. 31]) == 4'i32
    check fromBE32(data[32 .. 35]) == 2'i32
    # Element 2: len=4, value=3
    check fromBE32(data[36 .. 39]) == 4'i32
    check fromBE32(data[40 .. 43]) == 3'i32

  test "seq[int32] empty":
    let p = toPgParam(newSeq[int32]())
    check p.oid == OidInt4Array
    check p.format == 1
    let data = p.value.get
    check fromBE32(data[0 .. 3]) == 0'i32 # ndim=0
    check fromBE32(data[8 .. 11]) == OidInt4

  test "seq[int16]":
    let p = toPgParam(@[10'i16, -20'i16])
    check p.oid == OidInt2Array
    check p.format == 1
    let data = p.value.get
    check fromBE32(data[8 .. 11]) == OidInt2
    check fromBE32(data[12 .. 15]) == 2'i32
    # Element 0: len=2, value=10
    check fromBE32(data[20 .. 23]) == 2'i32
    check fromBE16(data[24 .. 25]) == 10'i16
    # Element 1: len=2, value=-20
    check fromBE32(data[26 .. 29]) == 2'i32
    check fromBE16(data[30 .. 31]) == -20'i16

  test "seq[int64]":
    let p = toPgParam(@[9999999999'i64, -1'i64])
    check p.oid == OidInt8Array
    check p.format == 1
    let data = p.value.get
    check fromBE32(data[8 .. 11]) == OidInt8
    # Element 0: len=8
    check fromBE32(data[20 .. 23]) == 8'i32
    check fromBE64(data[24 .. 31]) == 9999999999'i64

  test "seq[float32]":
    let p = toPgParam(@[1.5'f32, 2.5'f32])
    check p.oid == OidFloat4Array
    check p.format == 1

  test "seq[float64]":
    let p = toPgParam(@[3.14, 2.72])
    check p.oid == OidFloat8Array
    check p.format == 1

  test "seq[bool]":
    let p = toPgParam(@[true, false, true])
    check p.oid == OidBoolArray
    check p.format == 1
    let data = p.value.get
    check fromBE32(data[8 .. 11]) == OidBool
    check fromBE32(data[12 .. 15]) == 3'i32
    # Element 0: len=1, value=1
    check fromBE32(data[20 .. 23]) == 1'i32
    check data[24] == 1'u8
    # Element 1: len=1, value=0
    check fromBE32(data[25 .. 28]) == 1'i32
    check data[29] == 0'u8

  test "seq[string]":
    let p = toPgParam(@["hello", "world"])
    check p.oid == OidTextArray
    check p.format == 1
    let data = p.value.get
    check fromBE32(data[8 .. 11]) == OidText
    check fromBE32(data[12 .. 15]) == 2'i32
    # Element 0: len=5, "hello"
    check fromBE32(data[20 .. 23]) == 5'i32

  test "seq[string] with special characters":
    let p = toPgParam(@["a\"b", "c\\d", "e,f", ""])
    check p.oid == OidTextArray
    check p.format == 1

  test "seq[string] empty":
    let p = toPgParam(newSeq[string]())
    check p.format == 1
    let data = p.value.get
    check fromBE32(data[0 .. 3]) == 0'i32 # ndim=0

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

suite "Binary array encode/decode roundtrip":
  test "encodeBinaryArray + decodeBinaryArray roundtrip int32":
    let encoded = encodeBinaryArray(
      OidInt4, @[@(toBE32(1'i32)), @(toBE32(2'i32)), @(toBE32(3'i32))]
    )
    let decoded = decodeBinaryArray(encoded)
    check decoded.elemOid == OidInt4
    check decoded.elements.len == 3
    check fromBE32(
      encoded[
        decoded.elements[0].off ..< decoded.elements[0].off + decoded.elements[0].len
      ]
    ) == 1'i32
    check fromBE32(
      encoded[
        decoded.elements[1].off ..< decoded.elements[1].off + decoded.elements[1].len
      ]
    ) == 2'i32
    check fromBE32(
      encoded[
        decoded.elements[2].off ..< decoded.elements[2].off + decoded.elements[2].len
      ]
    ) == 3'i32

  test "encodeBinaryArrayEmpty roundtrip":
    let encoded = encodeBinaryArrayEmpty(OidInt4)
    let decoded = decodeBinaryArray(encoded)
    check decoded.elemOid == OidInt4
    check decoded.elements.len == 0

  test "getIntArray binary format":
    let encoded = encodeBinaryArray(OidInt4, @[@(toBE32(10'i32)), @(toBE32(-5'i32))])
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getIntArray(0) == @[10'i32, -5'i32]

  test "getIntArray text format fallback":
    let row: Row = @[some(toBytes("{1,2,3}"))]
    check row.getIntArray(0) == @[1'i32, 2, 3]

  test "getInt16Array binary format":
    let encoded = encodeBinaryArray(OidInt2, @[@(toBE16(10'i16)), @(toBE16(-20'i16))])
    let fields = @[mkField(OidInt2Array, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getInt16Array(0) == @[10'i16, -20'i16]

  test "getInt64Array binary format":
    let encoded =
      encodeBinaryArray(OidInt8, @[@(toBE64(9999999999'i64)), @(toBE64(-1'i64))])
    let fields = @[mkField(OidInt8Array, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getInt64Array(0) == @[9999999999'i64, -1'i64]

  test "getFloatArray binary format float8":
    let encoded = encodeBinaryArray(
      OidFloat8, @[@(toBE64(cast[int64](3.14'f64))), @(toBE64(cast[int64](2.72'f64)))]
    )
    let fields = @[mkField(OidFloat8Array, 1)]
    let row = mkRow(@[some(encoded)], fields)
    let arr = row.getFloatArray(0)
    check arr.len == 2
    check abs(arr[0] - 3.14) < 1e-10
    check abs(arr[1] - 2.72) < 1e-10

  test "getFloat32Array binary format":
    let encoded = encodeBinaryArray(
      OidFloat4, @[@(toBE32(cast[int32](1.5'f32))), @(toBE32(cast[int32](2.5'f32)))]
    )
    let fields = @[mkField(OidFloat4Array, 1)]
    let row = mkRow(@[some(encoded)], fields)
    let arr = row.getFloat32Array(0)
    check arr.len == 2
    check abs(arr[0] - 1.5'f32) < 1e-5
    check abs(arr[1] - 2.5'f32) < 1e-5

  test "getBoolArray binary format":
    let encoded = encodeBinaryArray(OidBool, @[@[1'u8], @[0'u8], @[1'u8]])
    let fields = @[mkField(OidBoolArray, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getBoolArray(0) == @[true, false, true]

  test "getStrArray binary format":
    let encoded = encodeBinaryArray(OidText, @[toBytes("hello"), toBytes("world")])
    let fields = @[mkField(OidTextArray, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getStrArray(0) == @["hello", "world"]

  test "getStrArray binary format with special chars":
    let encoded = encodeBinaryArray(
      OidText, @[toBytes("a\"b"), toBytes("c\\d"), toBytes("e,f"), toBytes("")]
    )
    let fields = @[mkField(OidTextArray, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getStrArray(0) == @["a\"b", "c\\d", "e,f", ""]

  test "getIntArray binary empty":
    let encoded = encodeBinaryArrayEmpty(OidInt4)
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getIntArray(0).len == 0

  test "getIntArrayOpt binary some":
    let encoded = encodeBinaryArray(OidInt4, @[@(toBE32(42'i32))])
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(encoded)], fields)
    let v = row.getIntArrayOpt(0)
    check v.isSome
    check v.get == @[42'i32]

  test "getIntArrayOpt binary none":
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getIntArrayOpt(0) == none(seq[int32])

  test "toPgParam seq[int32] roundtrip via decode":
    let p = toPgParam(@[100'i32, 200, 300])
    let decoded = decodeBinaryArray(p.value.get)
    check decoded.elemOid == OidInt4
    check decoded.elements.len == 3
    let data = p.value.get
    check fromBE32(data[decoded.elements[0].off ..< decoded.elements[0].off + 4]) ==
      100'i32
    check fromBE32(data[decoded.elements[1].off ..< decoded.elements[1].off + 4]) ==
      200'i32
    check fromBE32(data[decoded.elements[2].off ..< decoded.elements[2].off + 4]) ==
      300'i32

suite "PgNumeric":
  test "toPgParam PgNumeric":
    let p = toPgParam(parsePgNumeric("123.456"))
    check p.oid == OidNumeric
    check p.format == 0
    check toString(p.value.get) == "123.456"

  test "toPgBinaryParam PgNumeric binary format":
    let p = toPgBinaryParam(parsePgNumeric("99.99"))
    check p.oid == OidNumeric
    check p.format == 1
    # Verify roundtrip: decode the encoded binary
    let decoded = $decodeNumericBinary(p.value.get)
    check decoded == "99.99"

  test "toPgBinaryParam PgNumeric roundtrip variants":
    for s in [
      "0", "-1", "NaN", "0.00", "12345.6789", "-0.001", "100000000", "0.00001",
      "999.999",
    ]:
      let p = toPgBinaryParam(parsePgNumeric(s))
      let decoded = $decodeNumericBinary(p.value.get)
      check decoded == s

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
    check parsePgNumeric("100.00") == parsePgNumeric("100.00")
    check parsePgNumeric("1.0") == parsePgNumeric("1.00") # value-based equality
    check parsePgNumeric("0") == parsePgNumeric("0.00")
    check parsePgNumeric("NaN") == parsePgNumeric("NaN")
    check parsePgNumeric("1") != parsePgNumeric("2")
    check parsePgNumeric("1") != parsePgNumeric("-1")

  test "PgNumeric comparison":
    check parsePgNumeric("1") < parsePgNumeric("2")
    check parsePgNumeric("2") > parsePgNumeric("1")
    check parsePgNumeric("-5") < parsePgNumeric("1")
    check parsePgNumeric("-1") > parsePgNumeric("-2")
    check parsePgNumeric("0") < parsePgNumeric("0.001")
    check parsePgNumeric("0.001") > parsePgNumeric("0")
    check parsePgNumeric("0") > parsePgNumeric("-0.001")
    check parsePgNumeric("1.5") <= parsePgNumeric("1.5")
    check parsePgNumeric("1.5") >= parsePgNumeric("1.5")
    check parsePgNumeric("99999") < parsePgNumeric("100000")
    # NaN sorts highest (PostgreSQL convention)
    check parsePgNumeric("NaN") > parsePgNumeric("999999999")
    check parsePgNumeric("NaN") > parsePgNumeric("-999999999")
    check parsePgNumeric("NaN") >= parsePgNumeric("NaN")
    check parsePgNumeric("NaN") <= parsePgNumeric("NaN")

  test "PgNumeric hash consistency":
    # Equal values must have equal hashes
    check hash(parsePgNumeric("1.0")) == hash(parsePgNumeric("1.00"))
    check hash(parsePgNumeric("0")) == hash(parsePgNumeric("0.00"))
    check hash(parsePgNumeric("NaN")) == hash(parsePgNumeric("NaN"))

  test "parsePgNumeric roundtrip":
    for s in [
      "0", "1", "-1", "0.00", "12345.6789", "-0.001", "NaN", "100000000", "0.00001",
      "999.999",
    ]:
      check $parsePgNumeric(s) == s

  test "parsePgNumeric rejects invalid input":
    expect(PgTypeError):
      discard parsePgNumeric("")
    expect(PgTypeError):
      discard parsePgNumeric("abc")
    expect(PgTypeError):
      discard parsePgNumeric("+1.5")
    expect(PgTypeError):
      discard parsePgNumeric(" 1.5")
    expect(PgTypeError):
      discard parsePgNumeric("1.2.3")
    expect(PgTypeError):
      discard parsePgNumeric("-")
    expect(PgTypeError):
      discard parsePgNumeric(".")

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "1234"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "12345.6789"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "-42.50"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "0"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "NaN"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "0.00001"

  test "decodeNumericBinary - zero with scale":
    let data: seq[byte] = @[
      0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 # dscale = 2
    ]
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "0.00"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "10000000"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "0.5"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "0.123456789012"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check $row.getNumeric(0) == "-0.0025"

  test "getNumeric text NULL raises":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getNumeric(0)
    except PgTypeError:
      raised = true
    check raised

  test "getNumeric binary NULL raises":
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getNumeric(0)
    except PgTypeError:
      raised = true
    check raised

  test "getNumeric binary text fallback":
    let row: Row = @[some(toBytes("999.123"))]
    check $row.getNumeric(0) == "999.123"

  test "getStr binary with OidNumeric":
    # 42: ndigits=1, weight=0, dscale=0, digit=42
    let data: seq[byte] = @[
      0x00'u8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A # digit = 42
    ]
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getStr(0) == "42"

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
    let fields = @[mkField(OidNumeric, 1)]
    let row = mkRow(@[some(data)], fields)
    check row.getStr(0) == "3.14"

  test "$ PgNumeric":
    check $parsePgNumeric("12345.67890") == "12345.67890"
    check $parsePgNumeric("0") == "0"
    check $parsePgNumeric("-999") == "-999"

  test "toPgParam Option[PgNumeric] some":
    let p = toPgParam(some(parsePgNumeric("42.00")))
    check p.oid == OidNumeric
    check p.format == 0
    check p.value.isSome
    check toString(p.value.get) == "42.00"

  test "toPgParam Option[PgNumeric] none":
    let p = toPgParam(none(PgNumeric))
    check p.oid == OidNumeric
    check p.value.isNone

  test "toPgBinaryParam Option[PgNumeric] some":
    let p = toPgBinaryParam(some(parsePgNumeric("1.5")))
    check p.oid == OidNumeric
    check p.format == 1
    check p.value.isSome

  test "toPgBinaryParam Option[PgNumeric] none":
    let p = toPgBinaryParam(none(PgNumeric))
    check p.oid == OidNumeric
    check p.value.isNone

suite "PgMoney":
  test "OID constants":
    check OidMoney == 790'i32
    check OidMoneyArray == 791'i32

  test "initPgMoney defaults and fields":
    let m = initPgMoney(123456'i64)
    check m.amount == 123456'i64
    check m.scale == 2
    let m0 = initPgMoney(1234'i64, scale = 0)
    check m0.amount == 1234'i64
    check m0.scale == 0

  test "initPgMoney rejects invalid scale":
    expect(PgTypeError):
      discard initPgMoney(1'i64, scale = -1)
    expect(PgTypeError):
      discard initPgMoney(1'i64, scale = 19)

  test "$ without currency symbol (scale=2)":
    check $initPgMoney(0) == "0.00"
    check $initPgMoney(7) == "0.07"
    check $initPgMoney(100) == "1.00"
    check $initPgMoney(123456) == "1234.56"
    check $initPgMoney(-1) == "-0.01"
    check $initPgMoney(-123456) == "-1234.56"

  test "$ with alternate scales":
    check $initPgMoney(1234, scale = 0) == "1234"
    check $initPgMoney(-1234, scale = 0) == "-1234"
    check $initPgMoney(1234567, scale = 3) == "1234.567"
    check $initPgMoney(5, scale = 3) == "0.005"
    check $initPgMoney(12345, scale = 4) == "1.2345"

  test "$ handles int64.low":
    # Must not overflow when negating the magnitude.
    let s2 = $initPgMoney(int64.low, scale = 2)
    check s2 == "-92233720368547758.08"
    let s0 = $initPgMoney(int64.low, scale = 0)
    check s0 == "-9223372036854775808"

  test "parsePgMoney roundtrip (C locale style)":
    for s in ["0.00", "0.07", "1.00", "1234.56", "-0.01", "-1234.56"]:
      check $parsePgMoney(s) == s

  test "parsePgMoney accepts optional $ and +":
    check parsePgMoney("1234.56") == initPgMoney(123456)
    check parsePgMoney("$1234.56") == initPgMoney(123456)
    check parsePgMoney("+$1.00") == initPgMoney(100)

  test "parsePgMoney accepts sign after currency symbol":
    check parsePgMoney("$-1.00") == initPgMoney(-100)
    check parsePgMoney("$-1,234.56") == initPgMoney(-123456)
    check parsePgMoney("$+1.00") == initPgMoney(100)
    check parsePgMoney("¥-1,234", scale = 0) == initPgMoney(-1234, scale = 0)

  test "parsePgMoney with en_US thousand separator":
    check parsePgMoney("$1,234.56") == initPgMoney(123456)
    check parsePgMoney("$1,234,567.89") == initPgMoney(123456789)
    check parsePgMoney("-$1,234.56") == initPgMoney(-123456)

  test "parsePgMoney with EU format (comma decimal)":
    check parsePgMoney("1.234,56") == initPgMoney(123456)
    check parsePgMoney("1.234,56 €") == initPgMoney(123456)
    check parsePgMoney("-1.234,56 €") == initPgMoney(-123456)

  test "parsePgMoney strips non-ASCII currency symbols":
    check parsePgMoney("¥12.34") == initPgMoney(1234)
    check parsePgMoney("£1,234.56") == initPgMoney(123456)

  test "parsePgMoney strips surrounding whitespace":
    check parsePgMoney("  $100.00  ") == initPgMoney(10000)

  test "parsePgMoney handles parenthesized negatives":
    check parsePgMoney("($1.00)") == initPgMoney(-100)
    check parsePgMoney("($1,234.56)") == initPgMoney(-123456)
    check parsePgMoney("(1.234,56 €)") == initPgMoney(-123456)

  test "parsePgMoney scale=0 (e.g. JPY)":
    check parsePgMoney("1234", scale = 0) == initPgMoney(1234, scale = 0)
    check parsePgMoney("¥1,234", scale = 0) == initPgMoney(1234, scale = 0)
    check parsePgMoney("-¥1,234,567", scale = 0) == initPgMoney(-1234567, scale = 0)

  test "parsePgMoney scale=3":
    check parsePgMoney("1.234,567", scale = 3) == initPgMoney(1234567, scale = 3)
    check parsePgMoney("0.005", scale = 3) == initPgMoney(5, scale = 3)

  test "parsePgMoney int64.low roundtrip":
    let lo = initPgMoney(int64.low, scale = 2)
    check parsePgMoney($lo, scale = 2) == lo
    let lo0 = initPgMoney(int64.low, scale = 0)
    check parsePgMoney($lo0, scale = 0) == lo0

  test "parsePgMoney rejects invalid input":
    expect(PgTypeError):
      discard parsePgMoney("")
    expect(PgTypeError):
      discard parsePgMoney("abc")
    # scale=2 requires exactly 2 fractional digits after last . or ,
    expect(PgTypeError):
      discard parsePgMoney("$1.5")
    expect(PgTypeError):
      discard parsePgMoney("$1.234")
    expect(PgTypeError):
      discard parsePgMoney("$.56")
    # No decimal separator for scale=2
    expect(PgTypeError):
      discard parsePgMoney("$1234")
    # Parenthesized with extra minus
    expect(PgTypeError):
      discard parsePgMoney("(-$1.00)")

  test "parsePgMoney rejects overflow":
    expect(PgTypeError):
      discard parsePgMoney("99999999999999999999.99")

  test "equality, ordering, and hash":
    check initPgMoney(100) == initPgMoney(100)
    check initPgMoney(100) != initPgMoney(100, scale = 0)
    check initPgMoney(1) < initPgMoney(2)
    check initPgMoney(2) > initPgMoney(1)
    check initPgMoney(1) <= initPgMoney(1)
    check hash(initPgMoney(42)) == hash(initPgMoney(42))
    check hash(initPgMoney(42, scale = 0)) != hash(initPgMoney(42, scale = 2))

  test "ordering rejects mismatched scale":
    expect(PgTypeError):
      discard initPgMoney(100, scale = 0) < initPgMoney(100, scale = 2)
    expect(PgTypeError):
      discard initPgMoney(100, scale = 0) <= initPgMoney(100, scale = 2)

  test "formatPgMoney default = $":
    check formatPgMoney(initPgMoney(123456)) == "1234.56"
    check formatPgMoney(initPgMoney(-123456)) == "-1234.56"

  test "formatPgMoney en_US style":
    check formatPgMoney(initPgMoney(123456), symbol = "$", thousandsSep = ',') ==
      "$1,234.56"
    check formatPgMoney(initPgMoney(123456789), symbol = "$", thousandsSep = ',') ==
      "$1,234,567.89"
    check formatPgMoney(initPgMoney(-123456), symbol = "$", thousandsSep = ',') ==
      "-$1,234.56"
    check formatPgMoney(initPgMoney(1), symbol = "$", thousandsSep = ',') == "$0.01"

  test "formatPgMoney EU style":
    check formatPgMoney(
      initPgMoney(123456),
      symbol = " €",
      decimalSep = ',',
      thousandsSep = '.',
      symbolBefore = false,
    ) == "1.234,56 €"

  test "formatPgMoney scale=0":
    check formatPgMoney(
      initPgMoney(1234567, scale = 0), symbol = "¥", thousandsSep = ','
    ) == "¥1,234,567"

  test "formatPgMoney accounting parens for negatives":
    check formatPgMoney(
      initPgMoney(-123456), symbol = "$", thousandsSep = ',', accountingParens = true
    ) == "($1,234.56)"
    # Positive values unaffected
    check formatPgMoney(
      initPgMoney(123456), symbol = "$", thousandsSep = ',', accountingParens = true
    ) == "$1,234.56"
    # EU style, symbol after
    check formatPgMoney(
      initPgMoney(-123456),
      symbol = " €",
      decimalSep = ',',
      thousandsSep = '.',
      symbolBefore = false,
      accountingParens = true,
    ) == "(1.234,56 €)"
    # Roundtrip through parsePgMoney
    let v = initPgMoney(-123456)
    let s = formatPgMoney(v, symbol = "$", thousandsSep = ',', accountingParens = true)
    check parsePgMoney(s) == v

  test "toPgParam PgMoney":
    let p = toPgParam(initPgMoney(123456))
    check p.oid == OidMoney
    check p.format == 1
    check p.value.get == @(toBE64(123456'i64))

  test "toPgBinaryParam PgMoney":
    let p = toPgBinaryParam(initPgMoney(-500))
    check p.oid == OidMoney
    check p.format == 1
    check p.value.get == @(toBE64(-500'i64))

  test "getMoney binary default scale":
    let fields = @[mkField(OidMoney, 1)]
    let row = mkRow(@[some(@(toBE64(987654'i64)))], fields)
    check row.getMoney(0) == initPgMoney(987654)

  test "getMoney binary with scale=0":
    let fields = @[mkField(OidMoney, 1)]
    let row = mkRow(@[some(@(toBE64(987654'i64)))], fields)
    check row.getMoney(0, scale = 0) == initPgMoney(987654, scale = 0)

  test "getMoney text":
    let row: Row = @[some(toBytes("$1,234.56"))]
    check row.getMoney(0) == initPgMoney(123456)

  test "getMoney text with scale=0":
    let row: Row = @[some(toBytes("¥1,234"))]
    check row.getMoney(0, scale = 0) == initPgMoney(1234, scale = 0)

  test "getMoney NULL raises":
    let fields = @[mkField(OidMoney, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    expect(PgTypeError):
      discard row.getMoney(0)

  test "getMoneyOpt some/none":
    let fields = @[mkField(OidMoney, 1)]
    let rowSome = mkRow(@[some(@(toBE64(42'i64)))], fields)
    check rowSome.getMoneyOpt(0) == some(initPgMoney(42))
    let rowNone = mkRow(@[none(seq[byte])], fields)
    check rowNone.getMoneyOpt(0) == none(PgMoney)

  test "toPgParam seq[PgMoney] empty":
    let p = toPgParam(newSeq[PgMoney]())
    check p.oid == OidMoneyArray
    check p.format == 1
    check p.value.isSome

  test "toPgParam seq[PgMoney] roundtrip (binary)":
    let values = @[initPgMoney(100), initPgMoney(-50), initPgMoney(999999)]
    let p = toPgParam(values)
    check p.oid == OidMoneyArray
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[p.value], fields)
    check row.getMoneyArray(0) == values

  test "getMoneyArray binary with scale=0":
    let values = @[initPgMoney(100, scale = 0), initPgMoney(-50, scale = 0)]
    let p = toPgParam(@[initPgMoney(100), initPgMoney(-50)])
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[p.value], fields)
    check row.getMoneyArray(0, scale = 0) == values

  test "getMoneyArray text format with locale-formatted elements":
    let row: Row = @[some(toBytes("{\"$1,234.56\",\"$2.00\"}"))]
    check row.getMoneyArray(0) == @[initPgMoney(123456), initPgMoney(200)]

  test "getMoneyArrayOpt some/none":
    let values = @[initPgMoney(1), initPgMoney(2)]
    let p = toPgParam(values)
    let fields = @[mkField(OidMoneyArray, 1)]
    let rowSome = mkRow(@[p.value], fields)
    check rowSome.getMoneyArrayOpt(0) == some(values)
    let rowNone = mkRow(@[none(seq[byte])], fields)
    check rowNone.getMoneyArrayOpt(0) == none(seq[PgMoney])

  test "getMoney rejects invalid scale":
    let fields = @[mkField(OidMoney, 1)]
    let row = mkRow(@[some(@(toBE64(1'i64)))], fields)
    expect(PgTypeError):
      discard row.getMoney(0, scale = -1)
    expect(PgTypeError):
      discard row.getMoney(0, scale = 19)

  test "getMoneyArray rejects invalid scale":
    let p = toPgParam(@[initPgMoney(1)])
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[p.value], fields)
    expect(PgTypeError):
      discard row.getMoneyArray(0, scale = -1)
    expect(PgTypeError):
      discard row.getMoneyArray(0, scale = 19)

suite "PgInterval":
  test "$ zero interval":
    let v = PgInterval(months: 0, days: 0, microseconds: 0)
    check $v == "00:00:00"

  test "$ full interval":
    let v = PgInterval(months: 14, days: 3, microseconds: 14706123456)
    check $v == "1 year 2 mons 3 days 04:05:06.123456"

  test "$ months only":
    let v = PgInterval(months: 5, days: 0, microseconds: 0)
    check $v == "5 mons"

  test "$ days only":
    let v = PgInterval(months: 0, days: 1, microseconds: 0)
    check $v == "1 day"

  test "$ time only":
    let v = PgInterval(months: 0, days: 0, microseconds: 3_600_000_000)
    check $v == "01:00:00"

  test "$ negative time":
    let v = PgInterval(months: 0, days: 0, microseconds: -3_600_000_000)
    check $v == "-01:00:00"

  test "$ negative months and days":
    let v = PgInterval(months: -14, days: -3, microseconds: -14706123456)
    check $v == "-1 year -2 mons -3 days -04:05:06.123456"

  test "$ int64.min microseconds does not overflow":
    let v = PgInterval(months: 0, days: 0, microseconds: int64.low)
    let s = $v
    check s.len > 0
    check s.startsWith("-")

  test "$ plural vs singular":
    check $PgInterval(months: 12, days: 0, microseconds: 0) == "1 year"
    check $PgInterval(months: 24, days: 0, microseconds: 0) == "2 years"
    check $PgInterval(months: 1, days: 0, microseconds: 0) == "1 mon"
    check $PgInterval(months: 2, days: 0, microseconds: 0) == "2 mons"
    check $PgInterval(months: 0, days: 1, microseconds: 0) == "1 day"
    check $PgInterval(months: 0, days: 2, microseconds: 0) == "2 days"

  test "== operator":
    let a = PgInterval(months: 1, days: 2, microseconds: 3)
    let b = PgInterval(months: 1, days: 2, microseconds: 3)
    let c = PgInterval(months: 1, days: 2, microseconds: 4)
    check a == b
    check not (a == c)

  test "parseIntervalText basic":
    let v = parseIntervalText("1 year 2 mons 3 days 04:05:06.123456")
    check v.months == 14
    check v.days == 3
    check v.microseconds == 14706123456'i64

  test "parseIntervalText zero":
    let v = parseIntervalText("00:00:00")
    check v == PgInterval(months: 0, days: 0, microseconds: 0)

  test "parseIntervalText negative":
    let v = parseIntervalText("-1 year -2 mons -3 days -04:05:06")
    check v.months == -14
    check v.days == -3
    check v.microseconds == -14706000000'i64

  test "parseIntervalText time only":
    let v = parseIntervalText("01:30:00")
    check v.months == 0
    check v.days == 0
    check v.microseconds == 5_400_000_000'i64

  test "parseIntervalText days and time":
    let v = parseIntervalText("7 days 12:00:00")
    check v.days == 7
    check v.microseconds == 43_200_000_000'i64

  test "toPgParam PgInterval":
    let v = PgInterval(months: 14, days: 3, microseconds: 14706123456)
    let p = toPgParam(v)
    check p.oid == OidInterval
    check p.format == 0
    check toString(p.value.get) == "1 year 2 mons 3 days 04:05:06.123456"

  test "toPgBinaryParam PgInterval":
    let v = PgInterval(months: 14, days: 3, microseconds: 14706123456)
    let p = toPgBinaryParam(v)
    check p.oid == OidInterval
    check p.format == 1
    let data = p.value.get
    check data.len == 16
    check fromBE64(data.toOpenArray(0, 7)) == 14706123456'i64
    check fromBE32(data.toOpenArray(8, 11)) == 3'i32
    check fromBE32(data.toOpenArray(12, 15)) == 14'i32

  test "toPgBinaryParam PgInterval zero":
    let v = PgInterval(months: 0, days: 0, microseconds: 0)
    let p = toPgBinaryParam(v)
    let data = p.value.get
    check data.len == 16
    check fromBE64(data.toOpenArray(0, 7)) == 0'i64
    check fromBE32(data.toOpenArray(8, 11)) == 0'i32
    check fromBE32(data.toOpenArray(12, 15)) == 0'i32

  test "toPgBinaryParam PgInterval negative":
    let v = PgInterval(months: -1, days: -2, microseconds: -3_600_000_000)
    let p = toPgBinaryParam(v)
    let data = p.value.get
    check fromBE64(data.toOpenArray(0, 7)) == -3_600_000_000'i64
    check fromBE32(data.toOpenArray(8, 11)) == -2'i32
    check fromBE32(data.toOpenArray(12, 15)) == -1'i32

  test "getInterval text format":
    let row: Row = @[some(toBytes("1 year 2 mons 3 days 04:05:06.123456"))]
    let v = row.getInterval(0)
    check v == PgInterval(months: 14, days: 3, microseconds: 14706123456)

  test "getInterval binary format":
    var data = newSeq[byte](16)
    let usBytes = toBE64(14706123456'i64)
    copyMem(addr data[0], addr usBytes[0], 8)
    let dayBytes = toBE32(3'i32)
    copyMem(addr data[8], addr dayBytes[0], 4)
    let monBytes = toBE32(14'i32)
    copyMem(addr data[12], addr monBytes[0], 4)
    let fields = @[mkField(OidInterval, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getInterval(0)
    check v == PgInterval(months: 14, days: 3, microseconds: 14706123456)

  test "getInterval binary fallback to text":
    let row: Row = @[some(toBytes("5 days"))]
    let v = row.getInterval(0)
    check v == PgInterval(months: 0, days: 5, microseconds: 0)

  test "getInterval binary NULL raises":
    let fields = @[mkField(OidInterval, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getInterval(0)
    except PgTypeError:
      raised = true
    check raised

  test "getIntervalOpt text some":
    let row: Row = @[some(toBytes("00:00:00"))]
    let v = row.getIntervalOpt(0)
    check v.isSome
    check v.get == PgInterval(months: 0, days: 0, microseconds: 0)

  test "getIntervalOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getIntervalOpt(0) == none(PgInterval)

  test "getIntervalOpt binary some":
    var data = newSeq[byte](16)
    let usBytes = toBE64(1_000_000'i64)
    copyMem(addr data[0], addr usBytes[0], 8)
    let dayBytes = toBE32(0'i32)
    copyMem(addr data[8], addr dayBytes[0], 4)
    let monBytes = toBE32(0'i32)
    copyMem(addr data[12], addr monBytes[0], 4)
    let fields = @[mkField(OidInterval, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getIntervalOpt(0)
    check v.isSome
    check v.get == PgInterval(months: 0, days: 0, microseconds: 1_000_000)

  test "getIntervalOpt binary none":
    let fields = @[mkField(OidInterval, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getIntervalOpt(0) == none(PgInterval)

  test "toPgParam Option[PgInterval] some":
    let p = toPgParam(some(PgInterval(months: 1, days: 0, microseconds: 0)))
    check p.oid == OidInterval
    check p.value.isSome

  test "toPgParam Option[PgInterval] none":
    let p = toPgParam(none(PgInterval))
    check p.oid == OidInterval
    check p.value.isNone

  test "roundtrip text format":
    let orig = PgInterval(months: 14, days: 3, microseconds: 14706123456)
    let parsed = parseIntervalText($orig)
    check parsed == orig

  test "roundtrip binary format":
    let orig = PgInterval(months: -5, days: 10, microseconds: -7_200_000_000)
    let p = toPgBinaryParam(orig)
    let data = p.value.get
    var decoded: PgInterval
    decoded.microseconds = fromBE64(data.toOpenArray(0, 7))
    decoded.days = fromBE32(data.toOpenArray(8, 11))
    decoded.months = fromBE32(data.toOpenArray(12, 15))
    check decoded == orig

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

suite "PgInet":
  test "$ IPv4":
    let v = PgInet(address: parseIpAddress("192.168.1.1"), mask: 24)
    check $v == "192.168.1.1/24"

  test "$ IPv6":
    let v = PgInet(address: parseIpAddress("::1"), mask: 128)
    check $v == "::1/128"

  test "== equality":
    let a = PgInet(address: parseIpAddress("10.0.0.1"), mask: 32)
    let b = PgInet(address: parseIpAddress("10.0.0.1"), mask: 32)
    let c = PgInet(address: parseIpAddress("10.0.0.1"), mask: 24)
    check a == b
    check a != c

  test "toPgParam PgInet":
    let v = PgInet(address: parseIpAddress("192.168.1.1"), mask: 24)
    let p = toPgParam(v)
    check p.oid == OidInet
    check p.format == 0
    check toString(p.value.get) == "192.168.1.1/24"

  test "toPgBinaryParam PgInet IPv4":
    let v = PgInet(address: parseIpAddress("192.168.1.1"), mask: 24)
    let p = toPgBinaryParam(v)
    check p.oid == OidInet
    check p.format == 1
    let data = p.value.get
    check data.len == 8
    check data[0] == 2 # AF_INET
    check data[1] == 24 # mask
    check data[2] == 0 # is_cidr
    check data[3] == 4 # addrlen
    check data[4] == 192
    check data[5] == 168
    check data[6] == 1
    check data[7] == 1

  test "toPgBinaryParam PgInet IPv6":
    let v = PgInet(address: parseIpAddress("::1"), mask: 128)
    let p = toPgBinaryParam(v)
    check p.oid == OidInet
    check p.format == 1
    let data = p.value.get
    check data.len == 20
    check data[0] == 3 # AF_INET6
    check data[1] == 128 # mask
    check data[2] == 0 # is_cidr
    check data[3] == 16 # addrlen
    check data[19] == 1 # last byte of ::1

  test "getInet text format":
    let row: Row = @[some(toBytes("192.168.1.1/24"))]
    let v = row.getInet(0)
    check v.address == parseIpAddress("192.168.1.1")
    check v.mask == 24

  test "getInet text format no mask":
    let row: Row = @[some(toBytes("10.0.0.1"))]
    let v = row.getInet(0)
    check v.address == parseIpAddress("10.0.0.1")
    check v.mask == 32

  test "getInet binary format IPv4":
    let data = @[2'u8, 24, 0, 4, 192, 168, 1, 1]
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getInet(0)
    check v.address == parseIpAddress("192.168.1.1")
    check v.mask == 24

  test "getInet binary format IPv6":
    var data = newSeq[byte](20)
    data[0] = 3 # AF_INET6
    data[1] = 64 # mask
    data[2] = 0 # is_cidr
    data[3] = 16 # addrlen
    # fe80::1
    data[4] = 0xfe
    data[5] = 0x80
    data[19] = 1
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getInet(0)
    check v.address == parseIpAddress("fe80::1")
    check v.mask == 64

  test "getInet binary NULL raises":
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getInet(0)
    except PgTypeError:
      raised = true
    check raised

  test "getInetOpt text some":
    let row: Row = @[some(toBytes("10.0.0.1/32"))]
    let v = row.getInetOpt(0)
    check v.isSome
    check v.get.mask == 32

  test "getInetOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getInetOpt(0) == none(PgInet)

  test "getInetOpt binary some":
    let data = @[2'u8, 32, 0, 4, 10, 0, 0, 1]
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getInetOpt(0)
    check v.isSome
    check v.get.address == parseIpAddress("10.0.0.1")

  test "getInetOpt binary none":
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getInetOpt(0) == none(PgInet)

  test "toPgParam Option[PgInet] some":
    let p = toPgParam(some(PgInet(address: parseIpAddress("10.0.0.1"), mask: 32)))
    check p.oid == OidInet
    check p.value.isSome

  test "toPgParam Option[PgInet] none":
    let p = toPgParam(none(PgInet))
    check p.oid == OidInet
    check p.value.isNone

  test "roundtrip binary IPv4":
    let orig = PgInet(address: parseIpAddress("172.16.0.1"), mask: 16)
    let p = toPgBinaryParam(orig)
    let data = p.value.get
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let decoded = row.getInet(0)
    check decoded == orig

  test "roundtrip binary IPv6":
    let orig = PgInet(address: parseIpAddress("2001:db8::1"), mask: 48)
    let p = toPgBinaryParam(orig)
    let data = p.value.get
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let decoded = row.getInet(0)
    check decoded == orig

suite "PgCidr":
  test "$ PgCidr":
    let v = PgCidr(address: parseIpAddress("192.168.1.0"), mask: 24)
    check $v == "192.168.1.0/24"

  test "== equality":
    let a = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    let b = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    check a == b

  test "toPgParam PgCidr":
    let v = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    let p = toPgParam(v)
    check p.oid == OidCidr
    check p.format == 0

  test "toPgBinaryParam PgCidr IPv4":
    let v = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    let p = toPgBinaryParam(v)
    check p.oid == OidCidr
    let data = p.value.get
    check data[0] == 2 # AF_INET
    check data[1] == 8 # mask
    check data[2] == 1 # is_cidr
    check data[3] == 4

  test "getCidr text format":
    let row: Row = @[some(toBytes("10.0.0.0/8"))]
    let v = row.getCidr(0)
    check v.address == parseIpAddress("10.0.0.0")
    check v.mask == 8

  test "getCidr binary format":
    let data = @[2'u8, 8, 1, 4, 10, 0, 0, 0]
    let fields = @[mkField(OidCidr, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getCidr(0)
    check v.address == parseIpAddress("10.0.0.0")
    check v.mask == 8

  test "getCidrOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getCidrOpt(0) == none(PgCidr)

  test "roundtrip binary":
    let orig = PgCidr(address: parseIpAddress("192.168.0.0"), mask: 16)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidCidr, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let decoded = row.getCidr(0)
    check decoded == orig

suite "PgMacAddr":
  test "$ PgMacAddr":
    let v = PgMacAddr("08:00:2b:01:02:03")
    check $v == "08:00:2b:01:02:03"

  test "== equality":
    let a = PgMacAddr("08:00:2b:01:02:03")
    let b = PgMacAddr("08:00:2b:01:02:03")
    let c = PgMacAddr("08:00:2b:01:02:04")
    check a == b
    check a != c

  test "toPgParam PgMacAddr":
    let v = PgMacAddr("08:00:2b:01:02:03")
    let p = toPgParam(v)
    check p.oid == OidMacAddr
    check p.format == 0
    check toString(p.value.get) == "08:00:2b:01:02:03"

  test "toPgBinaryParam PgMacAddr":
    let v = PgMacAddr("08:00:2b:01:02:03")
    let p = toPgBinaryParam(v)
    check p.oid == OidMacAddr
    check p.format == 1
    let data = p.value.get
    check data.len == 6
    check data[0] == 0x08
    check data[1] == 0x00
    check data[2] == 0x2b
    check data[3] == 0x01
    check data[4] == 0x02
    check data[5] == 0x03

  test "getMacAddr text format":
    let row: Row = @[some(toBytes("08:00:2b:01:02:03"))]
    let v = row.getMacAddr(0)
    check v == PgMacAddr("08:00:2b:01:02:03")

  test "getMacAddr binary format":
    let data = @[0x08'u8, 0x00, 0x2b, 0x01, 0x02, 0x03]
    let fields = @[mkField(OidMacAddr, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getMacAddr(0)
    check v == PgMacAddr("08:00:2b:01:02:03")

  test "getMacAddr binary NULL raises":
    let fields = @[mkField(OidMacAddr, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getMacAddr(0)
    except PgTypeError:
      raised = true
    check raised

  test "getMacAddrOpt text some":
    let row: Row = @[some(toBytes("08:00:2b:01:02:03"))]
    let v = row.getMacAddrOpt(0)
    check v.isSome
    check v.get == PgMacAddr("08:00:2b:01:02:03")

  test "getMacAddrOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getMacAddrOpt(0) == none(PgMacAddr)

  test "roundtrip binary":
    let orig = PgMacAddr("aa:bb:cc:dd:ee:ff")
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidMacAddr, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let decoded = row.getMacAddr(0)
    check decoded == orig

suite "PgMacAddr8":
  test "$ PgMacAddr8":
    let v = PgMacAddr8("08:00:2b:01:02:03:04:05")
    check $v == "08:00:2b:01:02:03:04:05"

  test "== equality":
    let a = PgMacAddr8("08:00:2b:01:02:03:04:05")
    let b = PgMacAddr8("08:00:2b:01:02:03:04:05")
    check a == b

  test "toPgParam PgMacAddr8":
    let v = PgMacAddr8("08:00:2b:01:02:03:04:05")
    let p = toPgParam(v)
    check p.oid == OidMacAddr8
    check p.format == 0

  test "toPgBinaryParam PgMacAddr8":
    let v = PgMacAddr8("08:00:2b:01:02:03:04:05")
    let p = toPgBinaryParam(v)
    check p.oid == OidMacAddr8
    check p.format == 1
    let data = p.value.get
    check data.len == 8
    check data[0] == 0x08
    check data[7] == 0x05

  test "getMacAddr8 text format":
    let row: Row = @[some(toBytes("08:00:2b:01:02:03:04:05"))]
    let v = row.getMacAddr8(0)
    check v == PgMacAddr8("08:00:2b:01:02:03:04:05")

  test "getMacAddr8 binary format":
    let data = @[0x08'u8, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]
    let fields = @[mkField(OidMacAddr8, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getMacAddr8(0)
    check v == PgMacAddr8("08:00:2b:01:02:03:04:05")

  test "getMacAddr8 binary NULL raises":
    let fields = @[mkField(OidMacAddr8, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getMacAddr8(0)
    except PgTypeError:
      raised = true
    check raised

  test "getMacAddr8Opt text none":
    let row: Row = @[none(seq[byte])]
    check row.getMacAddr8Opt(0) == none(PgMacAddr8)

  test "roundtrip binary":
    let orig = PgMacAddr8("aa:bb:cc:dd:ee:ff:00:11")
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidMacAddr8, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let decoded = row.getMacAddr8(0)
    check decoded == orig

# User-defined enum tests

type
  Mood = enum
    happy = "happy"
    sad = "sad"
    ok = "ok"

  Color = enum
    red
    green
    blue

pgEnum(Mood)
pgEnum(Color, 99999)

suite "User-defined enum":
  test "pgEnum generates toPgParam with OID 0":
    let p = toPgParam(happy)
    check p.oid == 0'i32
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "happy"

  test "pgEnum with custom string values":
    let p = toPgParam(sad)
    check toString(p.value.get) == "sad"

  test "pgEnum with explicit OID":
    let p = toPgParam(red)
    check p.oid == 99999'i32
    check p.format == 0'i16
    check toString(p.value.get) == "red"

  test "pgEnum explicit OID all values":
    check toString(toPgParam(green).value.get) == "green"
    check toString(toPgParam(blue).value.get) == "blue"

  test "getEnum text format":
    let row: Row = @[some(toBytes("happy"))]
    check getEnum[Mood](row, 0) == happy

  test "getEnum all values":
    check getEnum[Mood](Row @[some(toBytes("sad"))], 0) == sad
    check getEnum[Mood](Row @[some(toBytes("ok"))], 0) == ok

  test "getEnum raises on invalid value":
    let row: Row = @[some(toBytes("unknown"))]
    var raised = false
    try:
      discard getEnum[Mood](row, 0)
    except ValueError:
      raised = true
    check raised

  test "getEnum raises on NULL":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getEnumOpt some":
    let row: Row = @[some(toBytes("happy"))]
    check getEnumOpt[Mood](row, 0) == some(happy)

  test "getEnumOpt none":
    let row: Row = @[none(seq[byte])]
    check getEnumOpt[Mood](row, 0) == none(Mood)

  test "getEnum binary format":
    let row: Row = @[some(toBytes("sad"))]
    # Use a non-standard OID to simulate a user-defined enum type
    check getEnum[Mood](row, 0) == sad

  test "getEnum binary format falls back to text":
    let row: Row = @[some(toBytes("ok"))]
    check getEnum[Mood](row, 0) == ok

  test "getEnumOpt binary NULL":
    let fields = @[mkField(99999'i32, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getEnumOpt[Mood](row, 0) == none(Mood)

  test "getEnum binary NULL raises":
    let fields = @[mkField(99999'i32, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "Option[Enum] toPgParam some":
    let p = toPgParam(some(happy))
    check p.oid == 0'i32
    check p.format == 0'i16
    check toString(p.value.get) == "happy"

  test "Option[Enum] toPgParam none":
    let p = toPgParam(none(Mood))
    check p.oid == 0'i32
    check p.value.isNone

  test "roundtrip text":
    let orig = ok
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    check getEnum[Mood](row, 0) == orig

  test "roundtrip binary":
    let orig = happy
    let p = toPgParam(orig)
    let fields = @[mkField(99999'i32, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getEnum[Mood](row, 0) == orig

# Composite type tests

type
  PointRecord = object
    x: float64
    y: float64

  PersonRecord = object
    name: string
    age: int32
    score: float64

  NullableRecord = object
    name: string
    age: Option[int32]
    note: Option[string]

pgComposite(PointRecord)
pgComposite(PersonRecord, 50000'i32)
pgComposite(NullableRecord)

suite "Composite text parser":
  test "parseCompositeText simple":
    let parts = parseCompositeText("(1,2,3)")
    check parts.len == 3
    check parts[0] == some("1")
    check parts[1] == some("2")
    check parts[2] == some("3")

  test "parseCompositeText with NULL":
    let parts = parseCompositeText("(hello,,world)")
    check parts.len == 3
    check parts[0] == some("hello")
    check parts[1] == none(string)
    check parts[2] == some("world")

  test "parseCompositeText quoted":
    let parts = parseCompositeText("(\"hello, world\",42)")
    check parts.len == 2
    check parts[0] == some("hello, world")
    check parts[1] == some("42")

  test "parseCompositeText quoted with escaped quote":
    let parts = parseCompositeText("(\"say \"\"hi\"\"\",done)")
    check parts.len == 2
    check parts[0] == some("say \"hi\"")
    check parts[1] == some("done")

  test "parseCompositeText empty string quoted":
    let parts = parseCompositeText("(\"\",42)")
    check parts.len == 2
    check parts[0] == some("")
    check parts[1] == some("42")

  test "parseCompositeText empty":
    let parts = parseCompositeText("()")
    check parts.len == 0

  test "parseCompositeText single NULL":
    let parts = parseCompositeText("(,)")
    check parts.len == 2
    check parts[0] == none(string)
    check parts[1] == none(string)

  test "parseCompositeText invalid raises":
    var raised = false
    try:
      discard parseCompositeText("not a composite")
    except PgTypeError:
      raised = true
    check raised

  test "encodeCompositeText simple":
    let s = encodeCompositeText(@[some("1"), some("2")])
    check s == "(1,2)"

  test "encodeCompositeText with NULL":
    let s = encodeCompositeText(@[some("hello"), none(string), some("world")])
    check s == "(hello,,world)"

  test "encodeCompositeText quoting":
    let s = encodeCompositeText(@[some("hello, world"), some("42")])
    check s == "(\"hello, world\",42)"

  test "encodeCompositeText empty string quoted":
    let s = encodeCompositeText(@[some(""), some("42")])
    check s == "(\"\",42)"

  test "roundtrip text encode/parse":
    let fields = @[some("hello world"), some("42"), none(string), some("with,comma")]
    let encoded = encodeCompositeText(fields)
    let decoded = parseCompositeText(encoded)
    check decoded.len == 4
    check decoded[0] == some("hello world")
    check decoded[1] == some("42")
    check decoded[2] == none(string)
    check decoded[3] == some("with,comma")

suite "Composite binary encoder/decoder":
  test "encodeBinaryComposite":
    let fields = @[
      (oid: OidInt4, data: some(@(toBE32(42'i32)))),
      (oid: OidText, data: some(toBytes("hello"))),
    ]
    let data = encodeBinaryComposite(fields)
    # numFields = 2
    check fromBE32(data[0 .. 3]) == 2'i32
    # field 0: oid=OidInt4, len=4, value=42
    check fromBE32(data[4 .. 7]) == OidInt4
    check fromBE32(data[8 .. 11]) == 4'i32
    check fromBE32(data[12 .. 15]) == 42'i32
    # field 1: oid=OidText, len=5, value="hello"
    check fromBE32(data[16 .. 19]) == OidText
    check fromBE32(data[20 .. 23]) == 5'i32
    check toString(data[24 .. 28]) == "hello"

  test "encodeBinaryComposite with NULL":
    let fields = @[
      (oid: OidInt4, data: some(@(toBE32(1'i32)))),
      (oid: OidText, data: none(seq[byte])),
    ]
    let data = encodeBinaryComposite(fields)
    check fromBE32(data[0 .. 3]) == 2'i32
    # field 1: NULL (len = -1)
    check fromBE32(data[16 .. 19]) == OidText
    check fromBE32(data[20 .. 23]) == -1'i32

  test "decodeBinaryComposite":
    let fields = @[
      (oid: OidInt4, data: some(@(toBE32(99'i32)))),
      (oid: OidText, data: some(toBytes("abc"))),
    ]
    let data = encodeBinaryComposite(fields)
    let decoded = decodeBinaryComposite(data)
    check decoded.len == 2
    check decoded[0].oid == OidInt4
    check decoded[0].len == 4
    check fromBE32(data[decoded[0].off .. decoded[0].off + 3]) == 99'i32
    check decoded[1].oid == OidText
    check decoded[1].len == 3

  test "decodeBinaryComposite with NULL":
    let fields = @[(oid: OidInt4, data: none(seq[byte]))]
    let data = encodeBinaryComposite(fields)
    let decoded = decodeBinaryComposite(data)
    check decoded.len == 1
    check decoded[0].oid == OidInt4
    check decoded[0].len == -1

  test "decodeBinaryComposite empty":
    let data = encodeBinaryComposite(@[])
    let decoded = decodeBinaryComposite(data)
    check decoded.len == 0

suite "User-defined composite":
  test "pgComposite generates toPgParam with OID 0":
    let p = toPgParam(PointRecord(x: 1.5, y: 2.5))
    check p.oid == 0'i32
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "(1.5,2.5)"

  test "pgComposite with explicit OID":
    let p = toPgParam(PersonRecord(name: "Alice", age: 30, score: 95.5))
    check p.oid == 50000'i32
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "(Alice,30,95.5)"

  test "getComposite text format":
    let row: Row = @[some(toBytes("(3.14,2.72)"))]
    let pt = getComposite[PointRecord](row, 0)
    check abs(pt.x - 3.14) < 1e-10
    check abs(pt.y - 2.72) < 1e-10

  test "getComposite text format with string":
    let row: Row = @[some(toBytes("(Bob,25,88.5)"))]
    let p = getComposite[PersonRecord](row, 0)
    check p.name == "Bob"
    check p.age == 25
    check abs(p.score - 88.5) < 1e-10

  test "getComposite text format quoted string":
    let row: Row = @[some(toBytes("(\"Alice, Jr.\",30,95.5)"))]
    let p = getComposite[PersonRecord](row, 0)
    check p.name == "Alice, Jr."
    check p.age == 30

  test "getComposite raises on NULL column":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard getComposite[PointRecord](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getCompositeOpt some":
    let row: Row = @[some(toBytes("(1.0,2.0)"))]
    let opt = getCompositeOpt[PointRecord](row, 0)
    check opt.isSome
    check abs(opt.get.x - 1.0) < 1e-10
    check abs(opt.get.y - 2.0) < 1e-10

  test "getCompositeOpt none":
    let row: Row = @[none(seq[byte])]
    check getCompositeOpt[PointRecord](row, 0) == none(PointRecord)

  test "getComposite binary format":
    # Build binary composite: 2 fields, both float8
    let fields_data = @[
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](3.14'f64))))),
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](2.72'f64))))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let pt = getComposite[PointRecord](row, 0)
    check abs(pt.x - 3.14) < 1e-10
    check abs(pt.y - 2.72) < 1e-10

  test "getComposite binary text fallback":
    let row: Row = @[some(toBytes("(5.0,6.0)"))]
    let pt = getComposite[PointRecord](row, 0)
    check abs(pt.x - 5.0) < 1e-10
    check abs(pt.y - 6.0) < 1e-10

  test "getCompositeOpt binary NULL":
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getCompositeOpt[PointRecord](row, 0) == none(PointRecord)

  test "roundtrip text":
    let orig = PointRecord(x: 1.5, y: -3.7)
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = getComposite[PointRecord](row, 0)
    check abs(decoded.x - orig.x) < 1e-10
    check abs(decoded.y - orig.y) < 1e-10

  test "roundtrip text PersonRecord":
    let orig = PersonRecord(name: "Charlie", age: 42, score: 99.9)
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = getComposite[PersonRecord](row, 0)
    check decoded.name == orig.name
    check decoded.age == orig.age
    check abs(decoded.score - orig.score) < 1e-10

  test "Option field toPgParam with values":
    let r = NullableRecord(name: "Alice", age: some(30'i32), note: some("hi"))
    let p = toPgParam(r)
    check toString(p.value.get) == "(Alice,30,hi)"

  test "Option field toPgParam with none":
    let r = NullableRecord(name: "Bob", age: none(int32), note: none(string))
    let p = toPgParam(r)
    check toString(p.value.get) == "(Bob,,)"

  test "getComposite text with Option fields some":
    let row: Row = @[some(toBytes("(Carol,25,hello)"))]
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Carol"
    check r.age == some(25'i32)
    check r.note == some("hello")

  test "getComposite text with Option fields none":
    let row: Row = @[some(toBytes("(Dave,,)"))]
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Dave"
    check r.age == none(int32)
    check r.note == none(string)

  test "getComposite binary with Option fields none":
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Eve"))),
      (oid: OidInt4, data: none(seq[byte])),
      (oid: OidText, data: none(seq[byte])),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(0'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Eve"
    check r.age == none(int32)
    check r.note == none(string)

  test "getComposite binary with Option fields some":
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Fay"))),
      (oid: OidInt4, data: some(@(toBE32(99'i32)))),
      (oid: OidText, data: some(toBytes("note"))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(0'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Fay"
    check r.age == some(99'i32)
    check r.note == some("note")

  test "Option field roundtrip":
    let orig = NullableRecord(name: "Test", age: some(42'i32), note: none(string))
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = getComposite[NullableRecord](row, 0)
    check decoded.name == orig.name
    check decoded.age == orig.age
    check decoded.note == orig.note

  test "Option[Composite] toPgParam some":
    let p = toPgParam(some(PointRecord(x: 1.0, y: 2.0)))
    check p.oid == 0'i32
    check p.format == 0'i16
    check p.value.isSome

  test "Option[Composite] toPgParam none":
    let p = toPgParam(none(PointRecord))
    check p.oid == 0'i32
    check p.value.isNone

suite "User-defined domain":
  test "pgDomain generates toPgParam with base type OID":
    let p = toPgParam(UsPostalCode("12345"))
    check p.oid == OidText
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "12345"

  test "pgDomain int16 base type":
    let p = toPgParam(SmallCount(7))
    check p.oid == OidInt2
    check p.format == 1'i16
    check p.value.isSome

  test "pgDomain int32 base type":
    let p = toPgParam(PositiveInt(42))
    check p.oid == OidInt4
    check p.format == 1'i16 # binary inherited from int32
    check p.value.isSome

  test "pgDomain with explicit OID":
    let p = toPgParam(ProbabilityF(0.95))
    check p.oid == 90001'i32
    check p.value.isSome

  test "getDomain text format string":
    let row: Row = @[some(toBytes("12345"))]
    check getDomain[UsPostalCode](row, 0) == UsPostalCode("12345")

  test "getDomain text format int16":
    let row: Row = @[some(toBytes("7"))]
    check getDomain[SmallCount](row, 0) == SmallCount(7)

  test "getDomain text format int32":
    let row: Row = @[some(toBytes("42"))]
    check getDomain[PositiveInt](row, 0) == PositiveInt(42)

  test "getDomain text format int64":
    let row: Row = @[some(toBytes("1000000000"))]
    check getDomain[BigCount](row, 0) == BigCount(1000000000'i64)

  test "getDomain text format float64":
    let row: Row = @[some(toBytes("0.95"))]
    let v = getDomain[ProbabilityF](row, 0)
    check abs(float64(v) - 0.95) < 1e-10

  test "getDomain text format bool":
    let rowT: Row = @[some(toBytes("t"))]
    check getDomain[IsActive](rowT, 0) == IsActive(true)
    let rowF: Row = @[some(toBytes("f"))]
    check getDomain[IsActive](rowF, 0) == IsActive(false)

  test "getDomain binary format int16":
    let fields = @[mkField(OidInt2, 1'i16)]
    let row = mkRow(@[some(@(toBE16(3'i16)))], fields)
    check getDomain[SmallCount](row, 0) == SmallCount(3)

  test "getDomain binary format int32":
    let fields = @[mkField(OidInt4, 1'i16)]
    let row = mkRow(@[some(@(toBE32(99'i32)))], fields)
    check getDomain[PositiveInt](row, 0) == PositiveInt(99)

  test "getDomain binary format int64":
    let fields = @[mkField(OidInt8, 1'i16)]
    let row = mkRow(@[some(@(toBE64(1000000000'i64)))], fields)
    check getDomain[BigCount](row, 0) == BigCount(1000000000'i64)

  test "getDomain binary format float64":
    let fields = @[mkField(OidFloat8, 1'i16)]
    let row = mkRow(@[some(@(toBE64(cast[int64](3.14'f64))))], fields)
    let v = getDomain[ProbabilityF](row, 0)
    check abs(float64(v) - 3.14) < 1e-10

  test "getDomain binary format bool":
    let fields = @[mkField(OidBool, 1'i16)]
    let rowT = mkRow(@[some(@[1'u8])], fields)
    check getDomain[IsActive](rowT, 0) == IsActive(true)
    let rowF = mkRow(@[some(@[0'u8])], fields)
    check getDomain[IsActive](rowF, 0) == IsActive(false)

  test "getDomain raises on NULL":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard getDomain[UsPostalCode](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getDomainOpt some":
    let row: Row = @[some(toBytes("12345"))]
    check getDomainOpt[UsPostalCode](row, 0) == some(UsPostalCode("12345"))

  test "getDomainOpt none":
    let row: Row = @[none(seq[byte])]
    check getDomainOpt[UsPostalCode](row, 0) == none(UsPostalCode)

  test "getDomainOpt binary NULL":
    let fields = @[mkField(OidInt4, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getDomainOpt[PositiveInt](row, 0) == none(PositiveInt)

  test "Option[Domain] toPgParam some":
    let p = toPgParam(some(UsPostalCode("12345")))
    check p.oid == OidText
    check p.value.isSome
    check toString(p.value.get) == "12345"

  test "Option[Domain] toPgParam none":
    let p = toPgParam(none(UsPostalCode))
    check p.oid == OidText
    check p.value.isNone

  test "roundtrip text string":
    let orig = UsPostalCode("90210")
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    check getDomain[UsPostalCode](row, 0) == orig

  test "roundtrip binary int16":
    let orig = SmallCount(5)
    let p = toPgParam(orig)
    let fields = @[mkField(OidInt2, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[SmallCount](row, 0) == orig

  test "roundtrip binary int32":
    let orig = PositiveInt(7)
    let p = toPgParam(orig)
    let fields = @[mkField(OidInt4, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[PositiveInt](row, 0) == orig

  test "pgDomain int64 base type":
    let p = toPgParam(BigCount(999999'i64))
    check p.oid == OidInt8
    check p.format == 1'i16
    check p.value.isSome

  test "pgDomain bool base type":
    let p = toPgParam(IsActive(true))
    check p.oid == OidBool
    check p.format == 1'i16
    check p.value.isSome

  test "roundtrip binary int64":
    let orig = BigCount(123456789'i64)
    let p = toPgParam(orig)
    let fields = @[mkField(OidInt8, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[BigCount](row, 0) == orig

  test "roundtrip binary bool":
    let orig = IsActive(true)
    let p = toPgParam(orig)
    let fields = @[mkField(OidBool, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[IsActive](row, 0) == orig

suite "Range OID constants":
  test "range OID values":
    check OidInt4Range == 3904'i32
    check OidNumRange == 3906'i32
    check OidTsRange == 3908'i32
    check OidTsTzRange == 3910'i32
    check OidDateRange == 3912'i32
    check OidInt8Range == 3926'i32

  test "multirange OID values":
    check OidInt4Multirange == 4451'i32
    check OidNumMultirange == 4532'i32
    check OidTsMultirange == 4533'i32
    check OidTsTzMultirange == 4534'i32
    check OidDateMultirange == 4535'i32
    check OidInt8Multirange == 4536'i32

suite "PgRange constructors and display":
  test "emptyRange":
    let r = emptyRange[int32]()
    check r.isEmpty == true
    check $r == "empty"

  test "rangeOf default [lower,upper)":
    let r = rangeOf(1'i32, 10'i32)
    check r.isEmpty == false
    check r.hasLower == true
    check r.hasUpper == true
    check r.lower.value == 1'i32
    check r.lower.inclusive == true
    check r.upper.value == 10'i32
    check r.upper.inclusive == false
    check $r == "[1,10)"

  test "rangeOf (lower,upper]":
    let r = rangeOf(1'i32, 10'i32, lowerInc = false, upperInc = true)
    check $r == "(1,10]"

  test "rangeOf [lower,upper]":
    let r = rangeOf(1'i64, 10'i64, upperInc = true)
    check $r == "[1,10]"

  test "rangeFrom [lower,)":
    let r = rangeFrom(5'i32)
    check r.hasLower == true
    check r.hasUpper == false
    check $r == "[5,)"

  test "rangeTo (,upper)":
    let r = rangeTo(10'i32)
    check r.hasLower == false
    check r.hasUpper == true
    check $r == "(,10)"

  test "rangeTo (,upper]":
    let r = rangeTo(10'i32, inclusive = true)
    check $r == "(,10]"

  test "unboundedRange (,)":
    let r = unboundedRange[int32]()
    check r.hasLower == false
    check r.hasUpper == false
    check $r == "(,)"

  test "equality":
    check rangeOf(1'i32, 5'i32) == rangeOf(1'i32, 5'i32)
    check rangeOf(1'i32, 5'i32) != rangeOf(1'i32, 6'i32)
    check emptyRange[int32]() == emptyRange[int32]()
    check emptyRange[int32]() != rangeOf(1'i32, 5'i32)

  test "quoting special characters":
    let r = rangeOf(parsePgNumeric("1.5"), parsePgNumeric("2.5"))
    check $r == "[1.5,2.5)"

suite "Range text parsing":
  test "parse empty":
    let r = parseRangeText[int32](
      "empty",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check r.isEmpty == true

  test "parse [1,10)":
    let r = parseRangeText[int32](
      "[1,10)",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check r.hasLower == true
    check r.hasUpper == true
    check r.lower.value == 1'i32
    check r.lower.inclusive == true
    check r.upper.value == 10'i32
    check r.upper.inclusive == false

  test "parse (1,10]":
    let r = parseRangeText[int32](
      "(1,10]",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check r.lower.inclusive == false
    check r.upper.inclusive == true

  test "parse [5,)":
    let r = parseRangeText[int32](
      "[5,)",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check r.hasLower == true
    check r.hasUpper == false
    check r.lower.value == 5'i32

  test "parse (,10]":
    let r = parseRangeText[int32](
      "(,10]",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check r.hasLower == false
    check r.hasUpper == true
    check r.upper.value == 10'i32
    check r.upper.inclusive == true

  test "parse (,)":
    let r = parseRangeText[int32](
      "(,)",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check r.hasLower == false
    check r.hasUpper == false
    check r.isEmpty == false

  test "parse with quoted values":
    let r = parseRangeText[string](
      "[\"hello, world\",\"foo\")",
      proc(s: string): string =
        s,
    )
    check r.hasLower == true
    check r.hasUpper == true
    check r.lower.value == "hello, world"
    check r.upper.value == "foo"

  test "roundtrip encode/parse int32":
    let orig = rangeOf(1'i32, 100'i32)
    let parsed = parseRangeText[int32](
      $orig,
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check parsed == orig

  test "roundtrip encode/parse int64":
    let orig = rangeOf(1'i64, 100'i64, upperInc = true)
    let parsed = parseRangeText[int64](
      $orig,
      proc(s: string): int64 =
        parseBiggestInt(s),
    )
    check parsed == orig

suite "Range toPgParam":
  test "int4range":
    let p = toPgParam(rangeOf(1'i32, 10'i32))
    check p.oid == OidInt4Range
    check p.format == 0'i16
    check p.value.get.toString == "[1,10)"

  test "int8range":
    let p = toPgParam(rangeOf(1'i64, 10'i64))
    check p.oid == OidInt8Range
    check p.value.get.toString == "[1,10)"

  test "numrange":
    let p = toPgParam(rangeOf(parsePgNumeric("1.5"), parsePgNumeric("9.5")))
    check p.oid == OidNumRange
    check p.value.get.toString == "[1.5,9.5)"

  test "tsrange":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgParam(rangeOf(dt1, dt2))
    check p.oid == OidTsRange

  test "tstzrange":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgTsTzRangeParam(rangeOf(dt1, dt2))
    check p.oid == OidTsTzRange

  test "daterange":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgDateRangeParam(rangeOf(dt1, dt2))
    check p.oid == OidDateRange
    check p.value.get.toString == "[2023-01-01,2023-12-31)"

  test "empty range":
    let p = toPgParam(emptyRange[int32]())
    check p.oid == OidInt4Range
    check p.value.get.toString == "empty"

  test "custom OID via toPgRangeParam":
    let p = toPgRangeParam(rangeOf(1'i32, 10'i32), 99999'i32)
    check p.oid == 99999'i32

  test "Option[PgRange[int32]] some":
    let p = toPgParam(some(rangeOf(1'i32, 10'i32)))
    check p.oid == OidInt4Range
    check p.value.isSome

  test "Option[PgRange[int32]] none":
    let p = toPgParam(none(PgRange[int32]))
    check p.oid == OidInt4Range
    check p.value.isNone

suite "Range toPgBinaryParam":
  test "empty int4range binary":
    let p = toPgBinaryParam(emptyRange[int32]())
    check p.oid == OidInt4Range
    check p.format == 1'i16
    let data = p.value.get
    check data.len == 1
    check data[0] == rangeEmpty

  test "int4range binary [1,10)":
    let p = toPgBinaryParam(rangeOf(1'i32, 10'i32))
    check p.oid == OidInt4Range
    check p.format == 1'i16
    let data = p.value.get
    # flags byte
    check (data[0] and rangeHasLower) != 0
    check (data[0] and rangeHasUpper) != 0
    check (data[0] and rangeLowerInc) != 0
    check (data[0] and rangeUpperInc) == 0
    # lower: len(4) + int32(4)
    check fromBE32(data.toOpenArray(1, 4)) == 4'i32 # length
    check fromBE32(data.toOpenArray(5, 8)) == 1'i32 # value
    # upper: len(4) + int32(4)
    check fromBE32(data.toOpenArray(9, 12)) == 4'i32
    check fromBE32(data.toOpenArray(13, 16)) == 10'i32

  test "int8range binary":
    let p = toPgBinaryParam(rangeOf(100'i64, 200'i64))
    check p.oid == OidInt8Range
    check p.format == 1'i16

  test "unbounded lower int4range binary":
    let p = toPgBinaryParam(rangeTo[int32](10'i32))
    let data = p.value.get
    check (data[0] and rangeHasLower) == 0
    check (data[0] and rangeHasUpper) != 0

  test "unbounded upper int4range binary":
    let p = toPgBinaryParam(rangeFrom[int32](5'i32))
    let data = p.value.get
    check (data[0] and rangeHasLower) != 0
    check (data[0] and rangeHasUpper) == 0

suite "Range binary decoding (roundtrip)":
  test "int4range roundtrip":
    let orig = rangeOf(1'i32, 10'i32)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4Range, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4Range(0)
    check decoded == orig

  test "int4range empty roundtrip":
    let orig = emptyRange[int32]()
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4Range, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4Range(0)
    check decoded.isEmpty == true

  test "int8range roundtrip":
    let orig = rangeOf(100'i64, 999'i64, upperInc = true)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt8Range, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt8Range(0)
    check decoded == orig

  test "int4range unbounded lower roundtrip":
    let orig = rangeTo[int32](10'i32)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4Range, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4Range(0)
    check decoded == orig

  test "int4range unbounded upper roundtrip":
    let orig = rangeFrom[int32](5'i32)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4Range, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4Range(0)
    check decoded == orig

  test "tsrange roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let orig = rangeOf(dt1, dt2)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidTsRange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getTsRange(0)
    check decoded.hasLower == true
    check decoded.hasUpper == true
    check decoded.lower.inclusive == true
    check decoded.upper.inclusive == false
    check decoded.lower.value.year == 2023
    check decoded.lower.value.month == mJan
    check decoded.upper.value.year == 2023
    check decoded.upper.value.month == mDec

  test "daterange roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let orig = rangeOf(dt1, dt2)
    let p = toPgBinaryDateRangeParam(orig)
    let fields = @[mkField(OidDateRange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getDateRange(0)
    check decoded.hasLower == true
    check decoded.hasUpper == true
    check decoded.lower.value.year == 2023
    check decoded.lower.value.month == mJan
    check decoded.lower.value.monthday == 1

suite "Range row getters":
  test "getInt4Range text":
    let row: Row = @[some(toBytes("[1,10)"))]
    check row.getInt4Range(0) == rangeOf(1'i32, 10'i32)

  test "getInt4Range text empty":
    let row: Row = @[some(toBytes("empty"))]
    check row.getInt4Range(0).isEmpty == true

  test "getInt8Range text":
    let row: Row = @[some(toBytes("[100,200)"))]
    check row.getInt8Range(0) == rangeOf(100'i64, 200'i64)

  test "getNumRange text":
    let row: Row = @[some(toBytes("[1.5,9.5)"))]
    let r = row.getNumRange(0)
    check r.lower.value == parsePgNumeric("1.5")
    check r.upper.value == parsePgNumeric("9.5")

  test "getDateRange text":
    let row: Row = @[some(toBytes("[2023-01-01,2023-12-31)"))]
    let r = row.getDateRange(0)
    check r.hasLower == true
    check r.hasUpper == true
    check r.lower.value.year == 2023
    check r.lower.value.month == mJan
    check r.upper.value.year == 2023
    check r.upper.value.month == mDec

  test "getTsRange text":
    let row: Row = @[some(toBytes("[2023-01-01 00:00:00,2023-12-31 23:59:59)"))]
    let r = row.getTsRange(0)
    check r.hasLower == true
    check r.lower.value.year == 2023

  test "getInt4Range format-aware text fallback":
    let row: Row = @[some(toBytes("[1,10)"))]
    check row.getInt4Range(0) == rangeOf(1'i32, 10'i32)

  test "getInt4RangeOpt text some":
    let row: Row = @[some(toBytes("[1,10)"))]
    let r = row.getInt4RangeOpt(0)
    check r.isSome
    check r.get == rangeOf(1'i32, 10'i32)

  test "getInt4RangeOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getInt4RangeOpt(0).isNone

  test "getInt4RangeOpt format-aware some":
    let p = toPgBinaryParam(rangeOf(1'i32, 10'i32))
    let fields = @[mkField(OidInt4Range, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let r = row.getInt4RangeOpt(0)
    check r.isSome
    check r.get == rangeOf(1'i32, 10'i32)

  test "getInt4RangeOpt format-aware none":
    let fields = @[mkField(OidInt4Range, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getInt4RangeOpt(0).isNone

suite "PgMultirange":
  test "constructor and display":
    let mr = toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32))
    check mr.len == 2
    check mr[0] == rangeOf(1'i32, 3'i32)
    check mr[1] == rangeOf(5'i32, 8'i32)
    check $mr == "{[1,3),[5,8)}"

  test "empty multirange":
    let mr = toMultirange[int32]()
    check mr.len == 0
    check $mr == "{}"

  test "equality":
    let a = toMultirange(rangeOf(1'i32, 3'i32))
    let b = toMultirange(rangeOf(1'i32, 3'i32))
    let c = toMultirange(rangeOf(1'i32, 5'i32))
    check a == b
    check a != c

  test "items iterator":
    let mr = toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32))
    var count = 0
    for r in mr:
      count += 1
    check count == 2

  test "toPgParam int4multirange":
    let mr = toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32))
    let p = toPgParam(mr)
    check p.oid == OidInt4Multirange
    check p.format == 0'i16
    check p.value.get.toString == "{[1,3),[5,8)}"

  test "toPgParam int8multirange":
    let mr = toMultirange(rangeOf(1'i64, 3'i64))
    let p = toPgParam(mr)
    check p.oid == OidInt8Multirange

  test "toPgParam nummultirange":
    let mr = toMultirange(rangeOf(parsePgNumeric("1.0"), parsePgNumeric("5.0")))
    let p = toPgParam(mr)
    check p.oid == OidNumMultirange

  test "toPgParam empty multirange":
    let mr = toMultirange[int32]()
    let p = toPgParam(mr)
    check p.oid == OidInt4Multirange
    check p.value.get.toString == "{}"

suite "Multirange text parsing":
  test "parse int4multirange":
    let mr = parseMultirangeText[int32](
      "{[1,3),[5,8)}",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check mr.len == 2
    check mr[0] == rangeOf(1'i32, 3'i32)
    check mr[1] == rangeOf(5'i32, 8'i32)

  test "parse empty multirange":
    let mr = parseMultirangeText[int32](
      "{}",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check mr.len == 0

  test "parse single range multirange":
    let mr = parseMultirangeText[int32](
      "{[1,10)}",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check mr.len == 1
    check mr[0] == rangeOf(1'i32, 10'i32)

  test "roundtrip int4multirange":
    let orig = toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32))
    let parsed = parseMultirangeText[int32](
      $orig,
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check parsed == orig

suite "Multirange row getters":
  test "getInt4Multirange text":
    let row: Row = @[some(toBytes("{[1,3),[5,8)}"))]
    let mr = row.getInt4Multirange(0)
    check mr.len == 2
    check mr[0] == rangeOf(1'i32, 3'i32)
    check mr[1] == rangeOf(5'i32, 8'i32)

  test "getInt4Multirange text empty":
    let row: Row = @[some(toBytes("{}"))]
    let mr = row.getInt4Multirange(0)
    check mr.len == 0

  test "getInt8Multirange text":
    let row: Row = @[some(toBytes("{[100,200)}"))]
    let mr = row.getInt8Multirange(0)
    check mr.len == 1
    check mr[0] == rangeOf(100'i64, 200'i64)

  test "getInt4MultirangeOpt text some":
    let row: Row = @[some(toBytes("{[1,3)}"))]
    let r = row.getInt4MultirangeOpt(0)
    check r.isSome
    check r.get.len == 1

  test "getInt4MultirangeOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getInt4MultirangeOpt(0).isNone

suite "Multirange binary roundtrip":
  test "int4multirange roundtrip":
    let orig = toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32))
    let p = toPgBinaryParam(orig)
    check p.oid == OidInt4Multirange
    check p.format == 1'i16
    let fields = @[mkField(OidInt4Multirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4Multirange(0)
    check decoded == orig

  test "int8multirange roundtrip":
    let orig = toMultirange(rangeOf(100'i64, 200'i64), rangeOf(300'i64, 400'i64))
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt8Multirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt8Multirange(0)
    check decoded == orig

  test "empty int4multirange roundtrip":
    let orig = toMultirange[int32]()
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4Multirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4Multirange(0)
    check decoded.len == 0

  test "int4multirange format-aware text fallback":
    let row: Row = @[some(toBytes("{[1,3),[5,8)}"))]
    let mr = row.getInt4Multirange(0)
    check mr.len == 2
    check mr[0] == rangeOf(1'i32, 3'i32)

  test "tsrange multirange roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let dt3 = dateTime(2023, mJul, 1, zone = utc())
    let dt4 = dateTime(2023, mDec, 31, zone = utc())
    let orig = toMultirange(rangeOf(dt1, dt2), rangeOf(dt3, dt4))
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidTsMultirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getTsMultirange(0)
    check decoded.len == 2
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan

  test "getInt4MultirangeOpt format-aware some":
    let orig = toMultirange(rangeOf(1'i32, 3'i32))
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4Multirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let r = row.getInt4MultirangeOpt(0)
    check r.isSome
    check r.get == orig

  test "getInt4MultirangeOpt format-aware none":
    let fields = @[mkField(OidInt4Multirange, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getInt4MultirangeOpt(0).isNone

suite "Range array binary roundtrip":
  test "int4range[] roundtrip":
    let orig = @[rangeOf(1'i32, 10'i32), rangeOf(20'i32, 30'i32)]
    let p = toPgBinaryParam(orig)
    check p.oid == OidInt4RangeArray
    check p.format == 1'i16
    let fields = @[mkField(OidInt4RangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4RangeArray(0)
    check decoded == orig

  test "int8range[] roundtrip":
    let orig = @[rangeOf(100'i64, 200'i64), rangeOf(300'i64, 400'i64)]
    let p = toPgBinaryParam(orig)
    check p.oid == OidInt8RangeArray
    let fields = @[mkField(OidInt8RangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt8RangeArray(0)
    check decoded == orig

  test "numrange[] roundtrip":
    let orig = @[
      rangeOf(parsePgNumeric("1.5"), parsePgNumeric("9.5")),
      rangeOf(parsePgNumeric("10.0"), parsePgNumeric("20.0")),
    ]
    let p = toPgBinaryParam(orig)
    check p.oid == OidNumRangeArray
    let fields = @[mkField(OidNumRangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getNumRangeArray(0)
    check decoded == orig

  test "tsrange[] roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let dt3 = dateTime(2023, mJul, 1, zone = utc())
    let dt4 = dateTime(2023, mDec, 31, zone = utc())
    let orig = @[rangeOf(dt1, dt2), rangeOf(dt3, dt4)]
    let p = toPgBinaryParam(orig)
    check p.oid == OidTsRangeArray
    let fields = @[mkField(OidTsRangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getTsRangeArray(0)
    check decoded.len == 2
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan

  test "tstzrange[] roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let orig = @[rangeOf(dt1, dt2)]
    let p = toPgBinaryTsTzRangeArrayParam(orig)
    check p.oid == OidTsTzRangeArray
    let fields = @[mkField(OidTsTzRangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getTsTzRangeArray(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 2023

  test "daterange[] roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let orig = @[rangeOf(dt1, dt2)]
    let p = toPgBinaryDateRangeArrayParam(orig)
    check p.oid == OidDateRangeArray
    let fields = @[mkField(OidDateRangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getDateRangeArray(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan

  test "empty int4range[] roundtrip":
    let orig: seq[PgRange[int32]] = @[]
    let p = toPgBinaryParam(orig)
    check p.oid == OidInt4RangeArray
    let fields = @[mkField(OidInt4RangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4RangeArray(0)
    check decoded.len == 0

  test "int4range[] with empty range element":
    let orig = @[rangeOf(1'i32, 10'i32), emptyRange[int32]()]
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4RangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4RangeArray(0)
    check decoded.len == 2
    check decoded[0] == rangeOf(1'i32, 10'i32)
    check decoded[1].isEmpty == true

suite "Range array row getters":
  test "getInt4RangeArray text":
    let row: Row = @[some(toBytes("{\"[1,10)\",\"[20,30)\"}"))]
    let arr = row.getInt4RangeArray(0)
    check arr.len == 2
    check arr[0] == rangeOf(1'i32, 10'i32)
    check arr[1] == rangeOf(20'i32, 30'i32)

  test "getInt8RangeArray text":
    let row: Row = @[some(toBytes("{\"[100,200)\"}"))]
    let arr = row.getInt8RangeArray(0)
    check arr.len == 1
    check arr[0] == rangeOf(100'i64, 200'i64)

  test "getNumRangeArray text":
    let row: Row = @[some(toBytes("{\"[1.5,9.5)\"}"))]
    let arr = row.getNumRangeArray(0)
    check arr.len == 1
    check arr[0].lower.value == parsePgNumeric("1.5")

  test "getDateRangeArray text":
    let row: Row = @[some(toBytes("{\"[2023-01-01,2023-12-31)\"}"))]
    let arr = row.getDateRangeArray(0)
    check arr.len == 1
    check arr[0].lower.value.year == 2023

  test "getInt4RangeArrayOpt some":
    let p = toPgBinaryParam(@[rangeOf(1'i32, 10'i32)])
    let fields = @[mkField(OidInt4RangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let r = row.getInt4RangeArrayOpt(0)
    check r.isSome
    check r.get.len == 1

  test "getInt4RangeArrayOpt none":
    let fields = @[mkField(OidInt4RangeArray, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getInt4RangeArrayOpt(0).isNone

suite "Geometry types":
  test "OID constants":
    check OidPoint == 600'i32
    check OidLseg == 601'i32
    check OidPath == 602'i32
    check OidBox == 603'i32
    check OidPolygon == 604'i32
    check OidLine == 628'i32
    check OidCircle == 718'i32

  test "PgPoint $ and ==":
    let p = PgPoint(x: 1.5, y: 2.5)
    check $p == "(1.5,2.5)"
    check p == PgPoint(x: 1.5, y: 2.5)
    check p != PgPoint(x: 1.5, y: 3.0)

  test "PgLine $ and ==":
    let l = PgLine(a: 1.0, b: 2.0, c: 3.0)
    check $l == "{1.0,2.0,3.0}"
    check l == PgLine(a: 1.0, b: 2.0, c: 3.0)

  test "PgLseg $ and ==":
    let s = PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 1.0, y: 1.0))
    check $s == "[(0.0,0.0),(1.0,1.0)]"

  test "PgBox $ and ==":
    let b = PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0))
    check $b == "(3.0,4.0),(1.0,2.0)"

  test "PgPath closed $ and ==":
    let p = PgPath(
      closed: true,
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)],
    )
    check $p == "((0.0,0.0),(1.0,0.0),(0.0,1.0))"

  test "PgPath open":
    let p =
      PgPath(closed: false, points: @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 1.0)])
    check $p == "[(0.0,0.0),(1.0,1.0)]"

  test "PgPolygon $":
    let p = PgPolygon(
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)]
    )
    check $p == "((0.0,0.0),(1.0,0.0),(0.0,1.0))"

  test "PgCircle $ and ==":
    let c = PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0)
    check $c == "<(1.0,2.0),5.0>"

  # toPgParam tests
  test "toPgParam PgPoint":
    let p = toPgParam(PgPoint(x: 1.5, y: 2.5))
    check p.oid == OidPoint
    check p.format == 0

  test "toPgParam PgLine":
    let p = toPgParam(PgLine(a: 1.0, b: 2.0, c: 3.0))
    check p.oid == OidLine
    check p.format == 0

  test "toPgParam PgLseg":
    let p = toPgParam(PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 1.0, y: 1.0)))
    check p.oid == OidLseg
    check p.format == 0

  test "toPgParam PgBox":
    let p =
      toPgParam(PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0)))
    check p.oid == OidBox
    check p.format == 0

  test "toPgParam PgPath":
    let p = toPgParam(PgPath(closed: true, points: @[PgPoint(x: 0.0, y: 0.0)]))
    check p.oid == OidPath
    check p.format == 0

  test "toPgParam PgPolygon":
    let p = toPgParam(PgPolygon(points: @[PgPoint(x: 0.0, y: 0.0)]))
    check p.oid == OidPolygon
    check p.format == 0

  test "toPgParam PgCircle":
    let p = toPgParam(PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0))
    check p.oid == OidCircle
    check p.format == 0

  # toPgBinaryParam tests
  test "toPgBinaryParam PgPoint":
    let p = toPgBinaryParam(PgPoint(x: 1.5, y: 2.5))
    check p.oid == OidPoint
    check p.format == 1
    check p.value.get.len == 16

  test "toPgBinaryParam PgLine":
    let p = toPgBinaryParam(PgLine(a: 1.0, b: 2.0, c: 3.0))
    check p.oid == OidLine
    check p.format == 1
    check p.value.get.len == 24

  test "toPgBinaryParam PgLseg":
    let p =
      toPgBinaryParam(PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 1.0, y: 1.0)))
    check p.oid == OidLseg
    check p.format == 1
    check p.value.get.len == 32

  test "toPgBinaryParam PgBox":
    let p = toPgBinaryParam(
      PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0))
    )
    check p.oid == OidBox
    check p.format == 1
    check p.value.get.len == 32

  test "toPgBinaryParam PgPath":
    let pts = @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 1.0)]
    let p = toPgBinaryParam(PgPath(closed: true, points: pts))
    check p.oid == OidPath
    check p.format == 1
    check p.value.get.len == 1 + 4 + 2 * 16

  test "toPgBinaryParam PgPolygon":
    let pts =
      @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)]
    let p = toPgBinaryParam(PgPolygon(points: pts))
    check p.oid == OidPolygon
    check p.format == 1
    check p.value.get.len == 4 + 3 * 16

  test "toPgBinaryParam PgCircle":
    let p = toPgBinaryParam(PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0))
    check p.oid == OidCircle
    check p.format == 1
    check p.value.get.len == 24

  # Text format decoding tests
  test "getPoint text format":
    let row: Row = @[some(toBytes("(1.5,2.5)"))]
    let v = row.getPoint(0)
    check v.x == 1.5
    check v.y == 2.5

  test "getLine text format":
    let row: Row = @[some(toBytes("{1.0,2.0,3.0}"))]
    let v = row.getLine(0)
    check v.a == 1.0
    check v.b == 2.0
    check v.c == 3.0

  test "getLseg text format":
    let row: Row = @[some(toBytes("[(0,0),(1,1)]"))]
    let v = row.getLseg(0)
    check v.p1 == PgPoint(x: 0.0, y: 0.0)
    check v.p2 == PgPoint(x: 1.0, y: 1.0)

  test "getBox text format":
    let row: Row = @[some(toBytes("(3,4),(1,2)"))]
    let v = row.getBox(0)
    check v.high == PgPoint(x: 3.0, y: 4.0)
    check v.low == PgPoint(x: 1.0, y: 2.0)

  test "getPath text format closed":
    let row: Row = @[some(toBytes("((0,0),(1,0),(0,1))"))]
    let v = row.getPath(0)
    check v.closed == true
    check v.points.len == 3

  test "getPath text format open":
    let row: Row = @[some(toBytes("[(0,0),(1,1)]"))]
    let v = row.getPath(0)
    check v.closed == false
    check v.points.len == 2

  test "getPolygon text format":
    let row: Row = @[some(toBytes("((0,0),(1,0),(0,1))"))]
    let v = row.getPolygon(0)
    check v.points.len == 3

  test "getCircle text format":
    let row: Row = @[some(toBytes("<(1,2),5>"))]
    let v = row.getCircle(0)
    check v.center == PgPoint(x: 1.0, y: 2.0)
    check v.radius == 5.0

  # Binary format roundtrip tests
  test "PgPoint binary roundtrip":
    let orig = PgPoint(x: -3.14, y: 2.718)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPoint, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPoint(0)
    check decoded == orig

  test "PgLine binary roundtrip":
    let orig = PgLine(a: 1.0, b: -2.0, c: 3.5)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidLine, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getLine(0)
    check decoded == orig

  test "PgLseg binary roundtrip":
    let orig = PgLseg(p1: PgPoint(x: -1.0, y: 2.0), p2: PgPoint(x: 3.0, y: -4.0))
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidLseg, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getLseg(0)
    check decoded == orig

  test "PgBox binary roundtrip":
    let orig = PgBox(high: PgPoint(x: 5.0, y: 6.0), low: PgPoint(x: 1.0, y: 2.0))
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidBox, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getBox(0)
    check decoded == orig

  test "PgPath binary roundtrip closed":
    let orig = PgPath(
      closed: true,
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)],
    )
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPath, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPath(0)
    check decoded == orig

  test "PgPath binary roundtrip open":
    let orig =
      PgPath(closed: false, points: @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 5.0, y: 5.0)])
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPath, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPath(0)
    check decoded == orig

  test "PgPolygon binary roundtrip":
    let orig = PgPolygon(
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 4.0, y: 0.0), PgPoint(x: 2.0, y: 3.0)]
    )
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPolygon, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPolygon(0)
    check decoded == orig

  test "PgCircle binary roundtrip":
    let orig = PgCircle(center: PgPoint(x: -1.5, y: 2.5), radius: 10.0)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidCircle, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getCircle(0)
    check decoded == orig

  # Opt accessor tests
  test "getPointOpt some":
    let row: Row = @[some(toBytes("(1,2)"))]
    check row.getPointOpt(0).isSome

  test "getPointOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getPointOpt(0).isNone

  test "getCircleOpt binary some":
    let orig = PgCircle(center: PgPoint(x: 0.0, y: 0.0), radius: 1.0)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidCircle, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let r = row.getCircleOpt(0)
    check r.isSome
    check r.get == orig

  test "getCircleOpt binary none":
    let fields = @[mkField(OidCircle, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getCircleOpt(0).isNone

suite "tsvector / tsquery":
  test "toPgParam PgTsVector":
    let v = PgTsVector("'cat':1A 'dog':3")
    let p = toPgParam(v)
    check p.oid == OidTsVector
    check p.format == 0
    check toString(p.value.get) == "'cat':1A 'dog':3"

  test "toPgParam PgTsQuery":
    let q = PgTsQuery("'fat' & 'rat'")
    let p = toPgParam(q)
    check p.oid == OidTsQuery
    check p.format == 0
    check toString(p.value.get) == "'fat' & 'rat'"

  test "toPgBinaryParam PgTsVector sends text format":
    let v = PgTsVector("'cat':1A 'dog':3")
    let p = toPgBinaryParam(v)
    check p.oid == OidTsVector
    check p.format == 0

  test "toPgBinaryParam PgTsQuery sends text format":
    let q = PgTsQuery("'fat' & 'rat'")
    let p = toPgBinaryParam(q)
    check p.oid == OidTsQuery
    check p.format == 0

  test "$ PgTsVector":
    let v = PgTsVector("'cat':1A 'dog':3")
    check $v == "'cat':1A 'dog':3"

  test "$ PgTsQuery":
    let q = PgTsQuery("'fat' & 'rat'")
    check $q == "'fat' & 'rat'"

  test "== PgTsVector":
    check PgTsVector("'a':1") == PgTsVector("'a':1")
    check PgTsVector("'a':1") != PgTsVector("'b':1")

  test "== PgTsQuery":
    check PgTsQuery("'a' & 'b'") == PgTsQuery("'a' & 'b'")
    check PgTsQuery("'a'") != PgTsQuery("'b'")

  test "getTsVector text format":
    let data = toBytes("'cat':1A,2B 'dog':3")
    let fields = @[mkField(OidTsVector, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getTsVector(0) == "'cat':1A,2B 'dog':3"

  test "getTsQuery text format":
    let data = toBytes("'fat' & 'rat'")
    let fields = @[mkField(OidTsQuery, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getTsQuery(0) == "'fat' & 'rat'"

  test "getTsVector binary format":
    # Binary tsvector for 'cat':1A (1 lexeme, 1 position with weight A)
    # Header: nlexemes = 1
    # Lexeme: "cat\0" + npos=1 + position=1 with weight A (3 << 14 | 1 = 0xC001)
    var data: seq[byte] = @[]
    # nlexemes = 1
    data.add(@(toBE32(1'i32)))
    # lexeme "cat" + null terminator
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    # npos = 1
    data.add(@(toBE16(1'i16)))
    # position 1 with weight A (weight=3, so 3 << 14 | 1 = 0xC001)
    data.add(@(toBE16(cast[int16](0xC001'u16))))
    let fields = @[mkField(OidTsVector, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getTsVector(0)
    check "'cat':1A" == $v

  test "getTsVector binary format multiple lexemes":
    # Binary tsvector for 'bar' 'foo':2B
    var data: seq[byte] = @[]
    # nlexemes = 2
    data.add(@(toBE32(2'i32)))
    # lexeme "bar" + null + npos=0
    for c in "bar":
      data.add(byte(c))
    data.add(0'u8)
    data.add(@(toBE16(0'i16)))
    # lexeme "foo" + null + npos=1 + position 2 with weight B (weight=2, 2 << 14 | 2 = 0x8002)
    for c in "foo":
      data.add(byte(c))
    data.add(0'u8)
    data.add(@(toBE16(1'i16)))
    data.add(@(toBE16(cast[int16](0x8002'u16))))
    let fields = @[mkField(OidTsVector, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getTsVector(0)
    check "'bar' 'foo':2B" == $v

  test "getTsQuery binary format simple AND":
    # Binary tsquery for 'cat' & 'dog' (prefix: AND, cat, dog)
    var data: seq[byte] = @[]
    # ntokens = 3
    data.add(@(toBE32(3'i32)))
    # AND operator: type=2, op=2
    data.add(2'u8)
    data.add(2'u8)
    # operand "cat": type=1, weight=0, prefix=0, "cat\0"
    data.add(1'u8) # type
    data.add(0'u8) # weight
    data.add(0'u8) # prefix
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    # operand "dog": type=1, weight=0, prefix=0, "dog\0"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "dog":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "'cat' & 'dog'" == $q

  test "getTsQuery binary format NOT":
    # Binary tsquery for !'cat' (prefix: NOT, cat)
    var data: seq[byte] = @[]
    data.add(@(toBE32(2'i32)))
    # NOT operator: type=2, op=1
    data.add(2'u8)
    data.add(1'u8)
    # operand "cat"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "!'cat'" == $q

  test "getTsQuery binary format PHRASE":
    # Binary tsquery for 'cat' <-> 'dog' (prefix: PHRASE dist=1, cat, dog)
    var data: seq[byte] = @[]
    data.add(@(toBE32(3'i32)))
    # PHRASE operator: type=2, op=4, distance=1
    data.add(2'u8)
    data.add(4'u8)
    data.add(@(toBE16(1'i16)))
    # operand "cat"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    # operand "dog"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "dog":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "'cat' <-> 'dog'" == $q

  test "getTsQuery binary format with weight and prefix":
    # Binary tsquery for 'cat':AB* (single operand with weights A+B and prefix)
    var data: seq[byte] = @[]
    data.add(@(toBE32(1'i32)))
    # operand "cat": type=1, weight=0x0C (A=0x08 + B=0x04), prefix=1
    data.add(1'u8)
    data.add(0x0C'u8) # A + B
    data.add(1'u8) # prefix
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "'cat':AB*" == $q

  test "getTsVectorOpt text some":
    let data = toBytes("'cat':1A")
    let fields = @[mkField(OidTsVector, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getTsVectorOpt(0)
    check r.isSome
    check $r.get == "'cat':1A"

  test "getTsVectorOpt none":
    let fields = @[mkField(OidTsVector, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getTsVectorOpt(0).isNone

  test "getTsQueryOpt text some":
    let data = toBytes("'cat' & 'dog'")
    let fields = @[mkField(OidTsQuery, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getTsQueryOpt(0)
    check r.isSome
    check $r.get == "'cat' & 'dog'"

  test "getTsQueryOpt none":
    let fields = @[mkField(OidTsQuery, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getTsQueryOpt(0).isNone

suite "xml":
  test "OID constant":
    check OidXml == 142'i32

  test "toPgParam PgXml":
    let v = PgXml("<root><item>hello</item></root>")
    let p = toPgParam(v)
    check p.oid == OidXml
    check p.format == 0
    check toString(p.value.get) == "<root><item>hello</item></root>"

  test "toPgBinaryParam PgXml sends binary format":
    let v = PgXml("<root/>")
    let p = toPgBinaryParam(v)
    check p.oid == OidXml
    check p.format == 1

  test "$ PgXml":
    let v = PgXml("<root/>")
    check $v == "<root/>"

  test "== PgXml":
    check PgXml("<a/>") == PgXml("<a/>")
    check PgXml("<a/>") != PgXml("<b/>")

  test "getXml text format":
    let data = toBytes("<root><item>test</item></root>")
    let fields = @[mkField(OidXml, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getXml(0) == "<root><item>test</item></root>"

  test "getXml binary format":
    let data = toBytes("<root><item>test</item></root>")
    let fields = @[mkField(OidXml, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getXml(0) == "<root><item>test</item></root>"

  test "getXmlOpt some":
    let data = toBytes("<root/>")
    let fields = @[mkField(OidXml, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getXmlOpt(0)
    check r.isSome
    check $r.get == "<root/>"

  test "getXmlOpt none":
    let fields = @[mkField(OidXml, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getXmlOpt(0).isNone

suite "hstore":
  test "encodeHstoreText empty":
    let h: PgHstore = initTable[string, Option[string]]()
    check encodeHstoreText(h) == ""

  test "encodeHstoreText single pair":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = some("val")
    check encodeHstoreText(h) == "\"key\"=>\"val\""

  test "encodeHstoreText NULL value":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = none(string)
    check encodeHstoreText(h) == "\"key\"=>NULL"

  test "encodeHstoreText escape":
    var h: PgHstore = initTable[string, Option[string]]()
    h["k\"ey"] = some("v\\al")
    check encodeHstoreText(h) == "\"k\\\"ey\"=>\"v\\\\al\""

  test "parseHstoreText empty":
    let h = parseHstoreText("")
    check h.len == 0

  test "parseHstoreText single pair":
    let h = parseHstoreText("\"key\"=>\"val\"")
    check h.len == 1
    check h["key"] == some("val")

  test "parseHstoreText NULL value":
    let h = parseHstoreText("\"key\"=>NULL")
    check h.len == 1
    check h["key"] == none(string)

  test "parseHstoreText multiple pairs":
    let h = parseHstoreText("\"a\"=>\"1\", \"b\"=>NULL, \"c\"=>\"3\"")
    check h.len == 3
    check h["a"] == some("1")
    check h["b"] == none(string)
    check h["c"] == some("3")

  test "parseHstoreText escaped":
    let h = parseHstoreText("\"k\\\"ey\"=>\"v\\\\al\"")
    check h.len == 1
    check h["k\"ey"] == some("v\\al")

  test "parseHstoreText roundtrip":
    var h: PgHstore = initTable[string, Option[string]]()
    h["hello"] = some("world")
    h["null_val"] = none(string)
    let encoded = encodeHstoreText(h)
    let decoded = parseHstoreText(encoded)
    check decoded == h

  test "encodeHstoreBinary empty":
    let h: PgHstore = initTable[string, Option[string]]()
    let data = encodeHstoreBinary(h)
    check data == @[byte 0, 0, 0, 0] # numPairs = 0

  test "decodeHstoreBinary empty":
    let data = @[byte 0, 0, 0, 0]
    let h = decodeHstoreBinary(data)
    check h.len == 0

  test "encodeHstoreBinary and decodeHstoreBinary roundtrip":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = some("val")
    h["nul"] = none(string)
    let data = encodeHstoreBinary(h)
    let decoded = decodeHstoreBinary(data)
    check decoded == h

  test "decodeHstoreBinary single pair with NULL":
    # numPairs=1, key="a" (len=1), val=NULL (len=-1)
    let data = @[
      byte 0,
      0,
      0,
      1, # numPairs = 1
      0,
      0,
      0,
      1, # keyLen = 1
      byte('a'), # key data
      0xFF,
      0xFF,
      0xFF,
      0xFF, # valLen = -1 (NULL)
    ]
    let h = decodeHstoreBinary(data)
    check h.len == 1
    check h["a"] == none(string)

  test "toPgParam hstore":
    var h: PgHstore = initTable[string, Option[string]]()
    h["k"] = some("v")
    let p = toPgParam(h)
    check p.oid == OidText
    check p.format == 0
    check p.value.isSome
    check toString(p.value.get) == "\"k\"=>\"v\""

  test "toPgBinaryParam hstore":
    var h: PgHstore = initTable[string, Option[string]]()
    h["k"] = some("v")
    let p = toPgBinaryParam(h, 16385'i32)
    check p.oid == 16385'i32
    check p.format == 1
    check p.value.isSome
    let decoded = decodeHstoreBinary(p.value.get)
    check decoded == h

  test "getHstore text format":
    let data = toBytes("\"a\"=>\"1\", \"b\"=>NULL")
    let fields = @[mkField(OidText, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let h = row.getHstore(0)
    check h.len == 2
    check h["a"] == some("1")
    check h["b"] == none(string)

  test "getHstore binary format":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = some("val")
    let data = encodeHstoreBinary(h)
    let fields = @[mkField(16385'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    check row.getHstore(0) == h

  test "getHstoreOpt some":
    let data = toBytes("\"a\"=>\"1\"")
    let fields = @[mkField(OidText, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getHstoreOpt(0)
    check r.isSome
    check r.get["a"] == some("1")

  test "getHstoreOpt none":
    let fields = @[mkField(OidText, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getHstoreOpt(0).isNone

  test "toPgParam seq[PgHstore] text roundtrip":
    var h1: PgHstore = initTable[string, Option[string]]()
    h1["a"] = some("1")
    var h2: PgHstore = initTable[string, Option[string]]()
    h2["b"] = none(string)
    let p = toPgParam(@[h1, h2])
    check p.oid == OidTextArray
    check p.format == 0
    let fields = @[mkField(OidTextArray, 0'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getHstoreArray(0)
    check arr.len == 2
    check arr[0] == h1
    check arr[1] == h2

  test "toPgParam seq[PgHstore] empty":
    let p = toPgParam(newSeq[PgHstore]())
    check p.oid == OidTextArray
    check p.format == 0
    check toString(p.value.get) == "{}"

  test "toPgBinaryParam seq[PgHstore] roundtrip":
    var h1: PgHstore = initTable[string, Option[string]]()
    h1["x"] = some("y")
    var h2: PgHstore = initTable[string, Option[string]]()
    h2["nul"] = none(string)
    let p = toPgBinaryParam(@[h1, h2], 16385'i32, 16386'i32)
    check p.oid == 16386'i32
    check p.format == 1
    let fields = @[mkField(16386'i32, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getHstoreArray(0)
    check arr.len == 2
    check arr[0] == h1
    check arr[1] == h2

  test "toPgBinaryParam seq[PgHstore] empty":
    let p = toPgBinaryParam(newSeq[PgHstore](), 16385'i32, 16386'i32)
    check p.oid == 16386'i32
    check p.format == 1
    let fields = @[mkField(16386'i32, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check row.getHstoreArray(0).len == 0

  test "getHstoreArray text format":
    let row: Row = @[some(toBytes("{\"\\\"a\\\"=>\\\"1\\\"\",\"\\\"b\\\"=>NULL\"}"))]
    let arr = row.getHstoreArray(0)
    check arr.len == 2
    check arr[0]["a"] == some("1")
    check arr[1]["b"] == none(string)

  test "getHstoreArrayOpt some":
    var h1: PgHstore = initTable[string, Option[string]]()
    h1["k"] = some("v")
    let p = toPgParam(@[h1])
    let fields = @[mkField(OidTextArray, 0'i16)]
    let row = mkRow(@[p.value], fields)
    let r = row.getHstoreArrayOpt(0)
    check r.isSome
    check r.get.len == 1
    check r.get[0] == h1

  test "getHstoreArrayOpt none":
    let fields = @[mkField(OidTextArray, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getHstoreArrayOpt(0).isNone

suite "PgBit":
  test "OID constants":
    check OidBit == 1560'i32
    check OidVarbit == 1562'i32
    check OidBitArray == 1561'i32
    check OidVarbitArray == 1563'i32

  test "parseBitString and $ roundtrip":
    let b = parseBitString("10110011")
    check b.nbits == 8
    check b.data == @[0b10110011'u8]
    check $b == "10110011"

  test "parseBitString non-byte-aligned":
    let b = parseBitString("101")
    check b.nbits == 3
    check $b == "101"
    # data should be 0b10100000
    check b.data == @[0b10100000'u8]

  test "parseBitString empty":
    let b = parseBitString("")
    check b.nbits == 0
    check b.data.len == 0
    check $b == ""

  test "== operator":
    check parseBitString("1010") == parseBitString("1010")
    check parseBitString("1010") != parseBitString("1011")
    check parseBitString("10") != parseBitString("1000")

  test "toPgParam PgBit":
    let b = parseBitString("10110011")
    let p = toPgParam(b)
    check p.oid == OidVarbit
    check p.format == 0
    check toString(p.value.get) == "10110011"

  test "toPgBinaryParam PgBit":
    let b = parseBitString("10110011")
    let p = toPgBinaryParam(b)
    check p.oid == OidVarbit
    check p.format == 1
    let data = p.value.get
    check data.len == 5 # 4 bytes for nbits + 1 byte for data
    # nbits = 8 in big-endian
    check data[0 .. 3] == @[0'u8, 0, 0, 8]
    check data[4] == 0b10110011'u8

  test "toPgBinaryParam PgBit non-byte-aligned":
    let b = parseBitString("101")
    let p = toPgBinaryParam(b)
    let data = p.value.get
    check data.len == 5
    # nbits = 3 in big-endian
    check data[0 .. 3] == @[0'u8, 0, 0, 3]
    check data[4] == 0b10100000'u8

  test "getBit text format":
    let data = toBytes("10110011")
    let fields = @[mkField(OidVarbit, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBit(0)
    check b.nbits == 8
    check $b == "10110011"

  test "getBit binary format":
    # Binary: 4 bytes nbits (8) + 1 byte data
    var data: seq[byte] = @[]
    data.add(@(toBE32(8'i32)))
    data.add(0b10110011'u8)
    let fields = @[mkField(OidVarbit, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBit(0)
    check b.nbits == 8
    check $b == "10110011"

  test "getBit binary format non-byte-aligned":
    var data: seq[byte] = @[]
    data.add(@(toBE32(3'i32)))
    data.add(0b10100000'u8)
    let fields = @[mkField(OidBit, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBit(0)
    check b.nbits == 3
    check $b == "101"

  test "getBitOpt with value":
    let data = toBytes("10110011")
    let fields = @[mkField(OidVarbit, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBitOpt(0)
    check b.isSome
    check $b.get == "10110011"

  test "getBitOpt with NULL":
    let fields = @[mkField(OidVarbit, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getBitOpt(0).isNone

  test "toPgParam seq[PgBit]":
    let v = @[parseBitString("1010"), parseBitString("110")]
    let p = toPgParam(v)
    check p.oid == OidVarbitArray
    check p.format == 1

  test "toPgParam seq[PgBit] empty":
    let v: seq[PgBit] = @[]
    let p = toPgParam(v)
    check p.oid == OidVarbitArray
    check p.format == 1

  test "toPgBinaryParam seq[PgBit]":
    let v = @[parseBitString("1010"), parseBitString("110")]
    let p = toPgBinaryParam(v)
    check p.oid == OidVarbitArray
    check p.format == 1

  test "getBitArray text format":
    let data = toBytes("{1010,110,00001111}")
    let fields = @[mkField(OidVarbitArray, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let arr = row.getBitArray(0)
    check arr.len == 3
    check $arr[0] == "1010"
    check $arr[1] == "110"
    check $arr[2] == "00001111"

  test "getBitArray binary format":
    # Encode two elements using toPgBinaryParam
    let v = @[parseBitString("1010"), parseBitString("11001100")]
    let p = toPgBinaryParam(v)
    let data = p.value.get
    let fields = @[mkField(OidVarbitArray, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let arr = row.getBitArray(0)
    check arr.len == 2
    check $arr[0] == "1010"
    check $arr[1] == "11001100"

  test "getBitArrayOpt with value":
    let data = toBytes("{101}")
    let fields = @[mkField(OidVarbitArray, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let arr = row.getBitArrayOpt(0)
    check arr.isSome
    check arr.get.len == 1
    check $arr.get[0] == "101"

  test "getBitArrayOpt with NULL":
    let fields = @[mkField(OidVarbitArray, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getBitArrayOpt(0).isNone

suite "Binary decoder validation":
  test "decodeNumericBinary rejects negative ndigits":
    # ndigits = -1 (0xFFFF as int16)
    var data: seq[byte] = @[
      0xFF'u8,
      0xFF, # ndigits = -1
      0x00,
      0x00, # weight
      0x00,
      0x00, # sign = positive
      0x00,
      0x00, # dscale
    ]
    expect PgTypeError:
      discard decodeNumericBinary(data)

  test "decodeNumericBinary rejects truncated digit data":
    # ndigits = 2 but only 1 digit of data provided
    var data: seq[byte] = @[
      0x00'u8,
      0x02, # ndigits = 2
      0x00,
      0x00, # weight
      0x00,
      0x00, # sign = positive
      0x00,
      0x00, # dscale
      0x00,
      0x01, # only 1 digit (need 2)
    ]
    expect PgTypeError:
      discard decodeNumericBinary(data)

  test "decodeBinaryArray rejects negative dimension length":
    var data: seq[byte] = @[
      0x00'u8,
      0x00,
      0x00,
      0x01, # ndim = 1
      0x00,
      0x00,
      0x00,
      0x00, # has_null
      0x00,
      0x00,
      0x00,
      0x17, # elemOid
      0xFF,
      0xFF,
      0xFF,
      0xFF, # dimLen = -1
      0x00,
      0x00,
      0x00,
      0x01, # lower_bound
    ]
    expect PgTypeError:
      discard decodeBinaryArray(data)

  test "decodeBinaryArray rejects invalid element length":
    var data: seq[byte] = @[
      0x00'u8,
      0x00,
      0x00,
      0x01, # ndim = 1
      0x00,
      0x00,
      0x00,
      0x00, # has_null
      0x00,
      0x00,
      0x00,
      0x17, # elemOid
      0x00,
      0x00,
      0x00,
      0x01, # dimLen = 1
      0x00,
      0x00,
      0x00,
      0x01, # lower_bound
      0xFF,
      0xFF,
      0xFF,
      0xFE, # eLen = -2 (invalid)
    ]
    expect PgTypeError:
      discard decodeBinaryArray(data)

  test "decodeBinaryArray rejects truncated element data":
    var data: seq[byte] = @[
      0x00'u8,
      0x00,
      0x00,
      0x01, # ndim = 1
      0x00,
      0x00,
      0x00,
      0x00, # has_null
      0x00,
      0x00,
      0x00,
      0x17, # elemOid
      0x00,
      0x00,
      0x00,
      0x01, # dimLen = 1
      0x00,
      0x00,
      0x00,
      0x01, # lower_bound
      0x00,
      0x00,
      0x00,
      0x08, # eLen = 8 but no data follows
    ]
    expect PgTypeError:
      discard decodeBinaryArray(data)

  test "decodeRangeBinaryRaw rejects negative bound length":
    var data: seq[byte] = @[
      0x02'u8, # flags: hasLower
      0xFF,
      0xFF,
      0xFF,
      0xFF, # bLen = -1
    ]
    expect PgTypeError:
      discard decodeRangeBinaryRaw(data)

  test "decodeRangeBinaryRaw rejects truncated bound data":
    var data: seq[byte] = @[
      0x02'u8, # flags: hasLower
      0x00,
      0x00,
      0x00,
      0x10, # bLen = 16 but no data
    ]
    expect PgTypeError:
      discard decodeRangeBinaryRaw(data)

  test "decodeMultirangeBinaryRaw rejects negative count":
    var data: seq[byte] = @[
      0xFF'u8, 0xFF, 0xFF, 0xFF # count = -1
    ]
    expect PgTypeError:
      discard decodeMultirangeBinaryRaw(data)

  test "decodeMultirangeBinaryRaw rejects truncated range data":
    var data: seq[byte] = @[
      0x00'u8,
      0x00,
      0x00,
      0x01, # count = 1
      0x00,
      0x00,
      0x00,
      0x20, # rLen = 32 but no data
    ]
    expect PgTypeError:
      discard decodeMultirangeBinaryRaw(data)

  test "decodeBinaryComposite rejects negative field count":
    var data: seq[byte] = @[
      0xFF'u8, 0xFF, 0xFF, 0xFF # numFields = -1
    ]
    expect PgTypeError:
      discard decodeBinaryComposite(data)

  test "decodeBinaryComposite rejects invalid field length":
    var data: seq[byte] = @[
      0x00'u8,
      0x00,
      0x00,
      0x01, # numFields = 1
      0x00,
      0x00,
      0x00,
      0x17, # oid
      0xFF,
      0xFF,
      0xFF,
      0xFE, # flen = -2 (invalid)
    ]
    expect PgTypeError:
      discard decodeBinaryComposite(data)

  test "decodeBinaryComposite rejects truncated field data":
    var data: seq[byte] = @[
      0x00'u8,
      0x00,
      0x00,
      0x01, # numFields = 1
      0x00,
      0x00,
      0x00,
      0x17, # oid
      0x00,
      0x00,
      0x00,
      0x10, # flen = 16 but no data
    ]
    expect PgTypeError:
      discard decodeBinaryComposite(data)

  test "decodeBinaryTsVector rejects negative lexeme count":
    var data: seq[byte] = @[
      0xFF'u8, 0xFF, 0xFF, 0xFF # nlexemes = -1
    ]
    expect PgTypeError:
      discard decodeBinaryTsVector(data)

  test "decodeBinaryTsQuery rejects negative token count":
    var data: seq[byte] = @[
      0xFF'u8, 0xFF, 0xFF, 0xFF # ntokens = -1
    ]
    expect PgTypeError:
      discard decodeBinaryTsQuery(data)

suite "Temporal array types":
  test "toPgTimestampArrayParam roundtrip":
    let dt1 = dateTime(2023, mJan, 15, 10, 30, 0, zone = utc())
    let dt2 = dateTime(2024, mJun, 20, 14, 45, 30, zone = utc())
    let p = toPgTimestampArrayParam(@[dt1, dt2])
    check p.oid == OidTimestampArray
    check p.format == 1'i16
    let fields = @[mkField(OidTimestampArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getTimestampArray(0)
    check arr.len == 2
    check arr[0].year == 2023
    check arr[1].year == 2024

  test "toPgTimestampArrayParam empty":
    let p = toPgTimestampArrayParam(newSeq[DateTime]())
    check p.oid == OidTimestampArray
    let fields = @[mkField(OidTimestampArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check row.getTimestampArray(0).len == 0

  test "getTimestampArray text format":
    let row: Row = @[some(toBytes("{\"2023-01-15 10:30:00\",\"2024-06-20 14:45:30\"}"))]
    let arr = row.getTimestampArray(0)
    check arr.len == 2
    check arr[0].year == 2023
    check arr[1].month == mJun

  test "getTimestampArrayOpt none":
    let fields = @[mkField(OidTimestampArray, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getTimestampArrayOpt(0).isNone

  test "toPgTimestampTzArrayParam roundtrip":
    let dt1 = dateTime(2023, mJan, 15, 10, 30, 0, zone = utc())
    let p = toPgTimestampTzArrayParam(@[dt1])
    check p.oid == OidTimestampTzArray
    let fields = @[mkField(OidTimestampTzArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getTimestampTzArray(0)
    check arr.len == 1
    check arr[0].year == 2023

  test "toPgDateArrayParam roundtrip":
    let dt1 = dateTime(2023, mMar, 10, zone = utc())
    let dt2 = dateTime(2024, mDec, 25, zone = utc())
    let p = toPgDateArrayParam(@[dt1, dt2])
    check p.oid == OidDateArray
    let fields = @[mkField(OidDateArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getDateArray(0)
    check arr.len == 2
    check arr[0].monthday == 10
    check arr[1].month == mDec

  test "getDateArray text format":
    let row: Row = @[some(toBytes("{2023-03-10,2024-12-25}"))]
    let arr = row.getDateArray(0)
    check arr.len == 2
    check arr[0].year == 2023

  test "toPgParam seq[PgTime] roundtrip":
    let t1 = PgTime(hour: 10, minute: 30, second: 0, microsecond: 0)
    let t2 = PgTime(hour: 23, minute: 59, second: 59, microsecond: 123456)
    let p = toPgParam(@[t1, t2])
    check p.oid == OidTimeArray
    let fields = @[mkField(OidTimeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getTimeArray(0)
    check arr.len == 2
    check arr[0] == t1
    check arr[1] == t2

  test "getTimeArray text format":
    let row: Row = @[some(toBytes("{10:30:00,23:59:59.123456}"))]
    let arr = row.getTimeArray(0)
    check arr.len == 2
    check arr[0].hour == 10
    check arr[1].microsecond == 123456

  test "toPgParam seq[PgTimeTz] roundtrip":
    let t1 = PgTimeTz(hour: 10, minute: 30, second: 0, microsecond: 0, utcOffset: 3600)
    let t2 =
      PgTimeTz(hour: 23, minute: 59, second: 59, microsecond: 0, utcOffset: -18000)
    let p = toPgParam(@[t1, t2])
    check p.oid == OidTimeTzArray
    let fields = @[mkField(OidTimeTzArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getTimeTzArray(0)
    check arr.len == 2
    check arr[0] == t1
    check arr[1] == t2

  test "getTimeTzArray text format":
    let row: Row = @[some(toBytes("{10:30:00+01,23:59:59-05}"))]
    let arr = row.getTimeTzArray(0)
    check arr.len == 2
    check arr[0].utcOffset == 3600
    check arr[1].utcOffset == -18000

  test "toPgParam seq[PgInterval] roundtrip":
    let iv1 = PgInterval(months: 2, days: 3, microseconds: 3600000000)
    let iv2 = PgInterval(months: 0, days: 0, microseconds: 1000000)
    let p = toPgParam(@[iv1, iv2])
    check p.oid == OidIntervalArray
    let fields = @[mkField(OidIntervalArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getIntervalArray(0)
    check arr.len == 2
    check arr[0] == iv1
    check arr[1] == iv2

suite "Identifier / network array types":
  test "toPgParam seq[PgUuid] roundtrip":
    let u1 = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let u2 = PgUuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    let p = toPgParam(@[u1, u2])
    check p.oid == OidUuidArray
    let fields = @[mkField(OidUuidArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getUuidArray(0)
    check arr.len == 2
    check arr[0] == u1
    check arr[1] == u2

  test "getUuidArray text format":
    let row: Row = @[
      some(
        toBytes(
          "{550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8}"
        )
      )
    ]
    let arr = row.getUuidArray(0)
    check arr.len == 2
    check arr[0] == PgUuid("550e8400-e29b-41d4-a716-446655440000")

  test "getUuidArrayOpt none":
    let fields = @[mkField(OidUuidArray, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getUuidArrayOpt(0).isNone

  test "toPgParam seq[PgInet] roundtrip":
    let i1 = PgInet(address: parseIpAddress("192.168.1.1"), mask: 32)
    let i2 = PgInet(address: parseIpAddress("10.0.0.0"), mask: 8)
    let p = toPgParam(@[i1, i2])
    check p.oid == OidInetArray
    let fields = @[mkField(OidInetArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getInetArray(0)
    check arr.len == 2
    check $arr[0].address == "192.168.1.1"
    check arr[1].mask == 8

  test "toPgParam seq[PgMacAddr] roundtrip":
    let m1 = PgMacAddr("08:00:2b:01:02:03")
    let m2 = PgMacAddr("aa:bb:cc:dd:ee:ff")
    let p = toPgParam(@[m1, m2])
    check p.oid == OidMacAddrArray
    let fields = @[mkField(OidMacAddrArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getMacAddrArray(0)
    check arr.len == 2
    check arr[0] == m1
    check arr[1] == m2

  test "getMacAddrArray text format":
    let row: Row = @[some(toBytes("{08:00:2b:01:02:03,aa:bb:cc:dd:ee:ff}"))]
    let arr = row.getMacAddrArray(0)
    check arr.len == 2
    check arr[0] == PgMacAddr("08:00:2b:01:02:03")

  test "toPgParam seq[PgMacAddr8] roundtrip":
    let m1 = PgMacAddr8("08:00:2b:01:02:03:04:05")
    let p = toPgParam(@[m1])
    check p.oid == OidMacAddr8Array
    let fields = @[mkField(OidMacAddr8Array, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getMacAddr8Array(0)
    check arr.len == 1
    check arr[0] == m1

suite "Numeric / binary / JSON array types":
  test "toPgParam seq[PgNumeric] roundtrip":
    let n1 = parsePgNumeric("123.45")
    let n2 = parsePgNumeric("0.001")
    let p = toPgParam(@[n1, n2])
    check p.oid == OidNumericArray
    let fields = @[mkField(OidNumericArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getNumericArray(0)
    check arr.len == 2
    check $arr[0] == "123.45"
    check $arr[1] == "0.001"

  test "getNumericArray text format":
    let row: Row = @[some(toBytes("{123.45,0.001}"))]
    let arr = row.getNumericArray(0)
    check arr.len == 2
    check $arr[0] == "123.45"

  test "toPgByteaArrayParam roundtrip":
    let b1 = @[1'u8, 2, 3]
    let b2 = @[0xFF'u8, 0x00]
    let p = toPgByteaArrayParam(@[b1, b2])
    check p.oid == OidByteaArray
    let fields = @[mkField(OidByteaArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getBytesArray(0)
    check arr.len == 2
    check arr[0] == b1
    check arr[1] == b2

  test "toPgByteaArrayParam empty":
    let p = toPgByteaArrayParam(newSeq[seq[byte]]())
    check p.oid == OidByteaArray
    let fields = @[mkField(OidByteaArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check row.getBytesArray(0).len == 0

  test "toPgParam seq[JsonNode] roundtrip":
    let j1 = %*{"key": "value"}
    let j2 = %*[1, 2, 3]
    let p = toPgParam(@[j1, j2])
    check p.oid == OidJsonbArray
    let fields = @[mkField(OidJsonbArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getJsonArray(0)
    check arr.len == 2
    check arr[0]["key"].getStr == "value"
    check arr[1].len == 3

  test "getJsonArray text format":
    let row: Row = @[some(toBytes("""{"{\"a\":1}","{\"b\":2}"}"""))]
    let arr = row.getJsonArray(0)
    check arr.len == 2
    check arr[0]["a"].getInt == 1

suite "Geometric array types":
  test "toPgParam seq[PgPoint] roundtrip":
    let p1 = PgPoint(x: 1.0, y: 2.0)
    let p2 = PgPoint(x: 3.0, y: 4.0)
    let p = toPgParam(@[p1, p2])
    check p.oid == OidPointArray
    let fields = @[mkField(OidPointArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getPointArray(0)
    check arr.len == 2
    check arr[0] == p1
    check arr[1] == p2

  test "getPointArray text format":
    let row: Row = @[some(toBytes("{\"(1,2)\",\"(3,4)\"}"))]
    let arr = row.getPointArray(0)
    check arr.len == 2
    check arr[0].x == 1.0

  test "toPgParam seq[PgCircle] roundtrip":
    let c1 = PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0)
    let p = toPgParam(@[c1])
    check p.oid == OidCircleArray
    let fields = @[mkField(OidCircleArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getCircleArray(0)
    check arr.len == 1
    check arr[0].center.x == 1.0
    check arr[0].radius == 5.0

  test "toPgParam seq[PgLseg] roundtrip":
    let l1 = PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 1.0, y: 1.0))
    let p = toPgParam(@[l1])
    check p.oid == OidLsegArray
    let fields = @[mkField(OidLsegArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getLsegArray(0)
    check arr.len == 1
    check arr[0].p1.x == 0.0
    check arr[0].p2.y == 1.0

  test "toPgParam seq[PgBox] roundtrip":
    let b1 = PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0))
    let p = toPgParam(@[b1])
    check p.oid == OidBoxArray
    let fields = @[mkField(OidBoxArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getBoxArray(0)
    check arr.len == 1
    check arr[0].high.x == 3.0
    check arr[0].low.y == 2.0

  test "toPgParam seq[PgLine] roundtrip":
    let l1 = PgLine(a: 1.0, b: 2.0, c: 3.0)
    let p = toPgParam(@[l1])
    check p.oid == OidLineArray
    let fields = @[mkField(OidLineArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getLineArray(0)
    check arr.len == 1
    check arr[0].a == 1.0
    check arr[0].b == 2.0

suite "Other array types":
  test "toPgParam seq[PgXml] roundtrip":
    let x1 = PgXml("<root/>")
    let x2 = PgXml("<data>hello</data>")
    let p = toPgParam(@[x1, x2])
    check p.oid == OidXmlArray
    let fields = @[mkField(OidXmlArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getXmlArray(0)
    check arr.len == 2
    check string(arr[0]) == "<root/>"
    check string(arr[1]) == "<data>hello</data>"

  test "getXmlArray text format":
    let row: Row = @[some(toBytes("{\"<root/>\",\"<data>hello</data>\"}"))]
    let arr = row.getXmlArray(0)
    check arr.len == 2
    check string(arr[0]) == "<root/>"

  test "toPgParam seq[PgTsVector] roundtrip":
    let tv1 = PgTsVector("'hello':1 'world':2")
    let p = toPgParam(@[tv1])
    check p.oid == OidTsVectorArray
    let fields = @[mkField(OidTsVectorArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getTsVectorArray(0)
    check arr.len == 1

  test "toPgParam seq[PgTsQuery] roundtrip":
    let tq1 = PgTsQuery("hello & world")
    let p = toPgParam(@[tq1])
    check p.oid == OidTsQueryArray
    let fields = @[mkField(OidTsQueryArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getTsQueryArray(0)
    check arr.len == 1

suite "Multirange array types":
  test "toPgParam seq[PgMultirange[int32]] text roundtrip":
    let mr1 = toMultirange(rangeOf(1'i32, 10'i32), rangeOf(20'i32, 30'i32))
    let mr2 = toMultirange(rangeOf(100'i32, 200'i32))
    let p = toPgParam(@[mr1, mr2])
    check p.oid == OidInt4MultirangeArray
    check p.format == 0'i16
    # Text format roundtrip
    let row: Row = @[p.value]
    let arr = row.getInt4MultirangeArray(0)
    check arr.len == 2
    check seq[PgRange[int32]](arr[0]).len == 2
    check seq[PgRange[int32]](arr[1]).len == 1

  test "toPgParam seq[PgMultirange[int32]] empty":
    let p = toPgParam(newSeq[PgMultirange[int32]]())
    check p.oid == OidInt4MultirangeArray
    let row: Row = @[p.value]
    check row.getInt4MultirangeArray(0).len == 0

  test "getInt4MultirangeArrayOpt none":
    let fields = @[mkField(OidInt4MultirangeArray, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getInt4MultirangeArrayOpt(0).isNone

  test "toPgParam seq[PgMultirange[int64]] text roundtrip":
    let mr1 = toMultirange(rangeOf(100'i64, 200'i64))
    let p = toPgParam(@[mr1])
    check p.oid == OidInt8MultirangeArray
    let row: Row = @[p.value]
    let arr = row.getInt8MultirangeArray(0)
    check arr.len == 1

  test "toPgParam seq[PgMultirange[PgNumeric]] text roundtrip":
    let mr1 = toMultirange(rangeOf(parsePgNumeric("1.5"), parsePgNumeric("3.5")))
    let p = toPgParam(@[mr1])
    check p.oid == OidNumMultirangeArray
    let row: Row = @[p.value]
    let arr = row.getNumMultirangeArray(0)
    check arr.len == 1

proc inlinePayload(p: PgParamInline): seq[byte] =
  ## Reconstruct the binary payload a PgParamInline would emit on the wire,
  ## picking between `inlineBuf` and `overflow` based on `len`.
  if p.len == -1:
    return @[]
  if p.len == 0:
    return @[]
  if p.len <= PgInlineBufSize:
    result = newSeq[byte](p.len)
    copyMem(addr result[0], addr p.inlineBuf[0], p.len)
  else:
    result = p.overflow

suite "toPgParamInline":
  test "int16 encodes as binary BE and fits inline":
    let p = toPgParamInline(42'i16)
    check p.oid == OidInt2
    check p.format == 1
    check p.len == 2
    check inlinePayload(p) == @(toBE16(42'i16))

  test "int16 negative":
    let p = toPgParamInline(-1'i16)
    check inlinePayload(p) == @(toBE16(-1'i16))

  test "int32 wire-format matches toPgParam":
    let old = toPgParam(123456'i32)
    let new = toPgParamInline(123456'i32)
    check new.oid == old.oid
    check new.format == old.format
    check inlinePayload(new) == old.value.get

  test "int32 boundaries":
    for v in [0'i32, 1, -1, int32.high, int32.low]:
      let p = toPgParamInline(v)
      check p.len == 4
      check inlinePayload(p) == @(toBE32(v))

  test "int64 fits inline at 8 bytes":
    let p = toPgParamInline(9_999_999_999'i64)
    check p.oid == OidInt8
    check p.len == 8
    check inlinePayload(p) == @(toBE64(9_999_999_999'i64))

  test "int widens to int64":
    let p = toPgParamInline(42.int)
    check p.oid == OidInt8
    check p.len == 8
    check inlinePayload(p) == @(toBE64(42'i64))

  test "float32 wire-format":
    let p = toPgParamInline(3.14'f32)
    check p.oid == OidFloat4
    check p.len == 4
    # Match toPgParam exactly (same cast-to-int32 + BE encoding).
    let old = toPgParam(3.14'f32)
    check inlinePayload(p) == old.value.get

  test "float64 wire-format":
    let p = toPgParamInline(3.14)
    check p.oid == OidFloat8
    check p.len == 8
    let bits = fromBE64(inlinePayload(p))
    check abs(cast[float64](bits) - 3.14) < 1e-10

  test "bool true / false":
    let pt = toPgParamInline(true)
    check pt.oid == OidBool
    check pt.len == 1
    check pt.inlineBuf[0] == 1'u8
    let pf = toPgParamInline(false)
    check pf.inlineBuf[0] == 0'u8

  test "string short fits inline":
    let p = toPgParamInline("hello")
    check p.oid == OidText
    check p.format == 0
    check p.len == 5
    check p.overflow.len == 0
    check inlinePayload(p) == @(toBytes("hello"))

  test "string empty":
    let p = toPgParamInline("")
    check p.len == 0
    check inlinePayload(p).len == 0

  test "string at inline boundary (16 bytes)":
    let s = "0123456789ABCDEF" # exactly 16 chars
    let p = toPgParamInline(s)
    check p.len == 16
    check p.overflow.len == 0
    check inlinePayload(p) == @(toBytes(s))

  test "string above boundary uses overflow":
    let s = "0123456789ABCDEFG" # 17 chars
    let p = toPgParamInline(s)
    check p.len == 17
    check p.overflow.len == 17
    check inlinePayload(p) == @(toBytes(s))

  test "seq[byte] short fits inline":
    let b = @[byte 1, 2, 3, 4]
    let p = toPgParamInline(b)
    check p.oid == OidBytea
    check p.len == 4
    check p.overflow.len == 0
    check inlinePayload(p) == b

  test "seq[byte] above boundary uses overflow":
    var b = newSeq[byte](32)
    for i in 0 ..< 32:
      b[i] = byte(i)
    let p = toPgParamInline(b)
    check p.len == 32
    check p.overflow.len == 32
    check inlinePayload(p) == b

  test "PgUuid matches toPgParam (OidUuid, text, overflow)":
    # UUID string is 36 chars, exceeds PgInlineBufSize.
    let u = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let p = toPgParamInline(u)
    let old = toPgParam(u)
    check p.oid == OidUuid
    check p.oid == old.oid
    check p.format == old.format
    check p.len == 36
    check p.overflow.len == 36
    check inlinePayload(p) == old.value.get
    check inlinePayload(p) == @(toBytes(string(u)))

  test "PgMoney encodes int64 amount only":
    let m = PgMoney(amount: 12345'i64, scale: 2'i8)
    let p = toPgParamInline(m)
    check p.oid == OidMoney
    check p.len == 8
    check inlinePayload(p) == @(toBE64(12345'i64))

  test "Option[int32] some delegates to int32":
    let p = toPgParamInline(some(42'i32))
    check p.oid == OidInt4
    check p.len == 4
    check inlinePayload(p) == @(toBE32(42'i32))

  test "Option[int32] none marks NULL":
    let p = toPgParamInline(none(int32))
    check p.oid == OidInt4
    check p.format == 1
    check p.len == -1

  test "Option[string] none marks NULL with text oid":
    let p = toPgParamInline(none(string))
    check p.oid == OidText
    check p.format == 0
    check p.len == -1

suite "addBindRaw wire-format parity":
  test "single int32 param matches addBind":
    let old = toPgParam(42'i32)
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("p", "s", [int16(1)], [old.value], [])
    var rawBuf: seq[byte] = @[]
    let data = old.value.get
    let ranges = @[(off: int32(0), len: int32(data.len))]
    rawBuf.addBindRaw("p", "s", [int16(1)], data, ranges, [])
    check legacyBuf == rawBuf

  test "three params matches addBind":
    let a = toPgParam(1'i32)
    let b = toPgParam(2'i32)
    let c = toPgParam(3'i32)
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("", "stmt", [int16(1), 1, 1], [a.value, b.value, c.value], [])
    var data: seq[byte] = @[]
    var ranges: seq[tuple[off: int32, len: int32]] = @[]
    for v in [a.value.get, b.value.get, c.value.get]:
      ranges.add (off: int32(data.len), len: int32(v.len))
      data.add v
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw("", "stmt", [int16(1), 1, 1], data, ranges, [])
    check legacyBuf == rawBuf

  test "NULL param matches addBind":
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("", "s", [int16(1)], [none(seq[byte])], [])
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw("", "s", [int16(1)], @[], @[(off: int32(0), len: int32(-1))], [])
    check legacyBuf == rawBuf

  test "mixed NULL and non-NULL matches addBind":
    let a = toPgParam(7'i32)
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("", "s", [int16(1), 1], [a.value, none(seq[byte])], [])
    var data: seq[byte] = a.value.get
    var ranges =
      @[(off: int32(0), len: int32(data.len)), (off: int32(0), len: int32(-1))]
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw("", "s", [int16(1), 1], data, ranges, [])
    check legacyBuf == rawBuf

  test "result formats are preserved":
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("p", "", [int16(0)], [none(seq[byte])], [int16(1), 0, 1])
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw(
      "p", "", [int16(0)], @[], @[(off: int32(0), len: int32(-1))], [int16(1), 0, 1]
    )
    check legacyBuf == rawBuf

suite "addBindRaw range validation":
  test "range len below -1 raises ValueError":
    var buf: seq[byte] = @[]
    expect ValueError:
      buf.addBindRaw("", "", [int16(1)], @[], @[(off: int32(0), len: int32(-2))], [])

  test "negative off with non-zero len raises ValueError":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    expect ValueError:
      buf.addBindRaw("", "", [int16(1)], data, @[(off: int32(-1), len: int32(4))], [])

  test "off + len past paramData.len raises ValueError":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    expect ValueError:
      # off + len == 5, data.len == 4 — reads past end
      buf.addBindRaw("", "", [int16(1)], data, @[(off: int32(1), len: int32(4))], [])

  test "range exactly at end of paramData is valid":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    # off + len == 4, data.len == 4 — boundary is inclusive on the data side
    buf.addBindRaw("", "", [int16(1)], data, @[(off: int32(0), len: int32(4))], [])
    check buf.len > 0

  test "len == 0 with off at end of data is valid (empty string case)":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    # Mirrors flattenInline's encoding for empty strings: off = data.len, len = 0
    buf.addBindRaw("", "", [int16(0)], data, @[(off: int32(4), len: int32(0))], [])
    check buf.len > 0

  test "NULL range with arbitrary off is valid (off ignored when len == -1)":
    var buf: seq[byte] = @[]
    buf.addBindRaw("", "", [int16(1)], @[], @[(off: int32(999), len: int32(-1))], [])
    check buf.len > 0

suite "parseAffectedRowsRaw":
  test "UPDATE / DELETE returns trailing count":
    check parseAffectedRows("UPDATE 3") == 3
    check parseAffectedRows("DELETE 5") == 5
    check parseAffectedRows("SELECT 100") == 100

  test "INSERT 0 N returns N":
    check parseAffectedRows("INSERT 0 1") == 1
    check parseAffectedRows("INSERT 0 42") == 42

  test "trailing whitespace yields 0 (matches legacy split semantics)":
    check parseAffectedRows("UPDATE 3 ") == 0
    check parseAffectedRows("UPDATE 7   ") == 0

  test "empty / non-numeric returns 0":
    check parseAffectedRows("") == 0
    check parseAffectedRows("CREATE TABLE") == 0
    check parseAffectedRows("COMMIT") == 0
    check parseAffectedRows("   ") == 0

  test "single-token numeric tag":
    # Tag that is just a number — not a real PostgreSQL tag but robust for safety.
    check parseAffectedRows("123") == 123

  test "large values":
    check parseAffectedRows("INSERT 0 9999999999") == 9_999_999_999'i64

  test "raw overload on char openArray":
    let s = "UPDATE 42"
    check parseAffectedRowsRaw(s.toOpenArray(0, s.high)) == 42

suite "flattenInline SoA layout":
  test "empty input produces empty SoA":
    let params: seq[PgParamInline] = @[]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 0
    check ranges.len == 0
    check oids.len == 0
    check formats.len == 0

  test "single short int32 param":
    let params = @[toPgParamInline(42'i32)]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data == @(toBE32(42'i32))
    check ranges.len == 1
    check ranges[0].off == 0
    check ranges[0].len == 4
    check oids == @[OidInt4]
    check formats == @[1'i16]

  test "single NULL param (off=0, len=-1, no bytes written)":
    let params = @[none(int32).toPgParamInline]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 0
    check ranges.len == 1
    check ranges[0].len == -1
    check oids == @[OidInt4]
    check formats == @[1'i16]

  test "single empty string (len=0, off points to current data.len)":
    let params = @[toPgParamInline("")]
    let (data, ranges, _, _) = flattenInline(params)
    check data.len == 0
    check ranges.len == 1
    check ranges[0].len == 0
    check ranges[0].off == 0

  test "boundary string (16 bytes fits in inlineBuf path)":
    let params = @[toPgParamInline("0123456789ABCDEF")]
    let (data, ranges, _, _) = flattenInline(params)
    check data.len == 16
    check data == @(toBytes("0123456789ABCDEF"))
    check ranges.len == 1
    check ranges[0].off == 0
    check ranges[0].len == 16

  test "overflow string (17 bytes uses overflow path)":
    let long = "0123456789ABCDEFG" # 17 chars
    let params = @[toPgParamInline(long)]
    let (data, ranges, _, _) = flattenInline(params)
    check data.len == 17
    check data == @(toBytes(long))
    check ranges.len == 1
    check ranges[0].off == 0
    check ranges[0].len == 17

  test "mixed [short, NULL, overflow, empty, short] — offsets are consecutive":
    let long = "abcdefghijklmnopqrst" # 20 chars → overflow
    let params = @[
      toPgParamInline(1'i32), # 4 bytes
      none(int32).toPgParamInline, # NULL
      toPgParamInline(long), # 20 bytes
      toPgParamInline(""), # 0 bytes
      toPgParamInline(7'i32), # 4 bytes
    ]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 4 + 20 + 4 # NULL and empty contribute nothing
    check ranges.len == 5
    # int32(1)
    check ranges[0].off == 0
    check ranges[0].len == 4
    check data[0 ..< 4] == @(toBE32(1'i32))
    # NULL
    check ranges[1].len == -1
    # long overflow string
    check ranges[2].off == 4
    check ranges[2].len == 20
    check data[4 ..< 24] == @(toBytes(long))
    # empty string — off points just past the overflow bytes, len 0
    check ranges[3].off == 24
    check ranges[3].len == 0
    # int32(7)
    check ranges[4].off == 24
    check ranges[4].len == 4
    check data[24 ..< 28] == @(toBE32(7'i32))
    check oids == @[OidInt4, OidInt4, OidText, OidText, OidInt4]
    # int32 uses binary(1), text uses text(0)
    check formats == @[1'i16, 1'i16, 0'i16, 0'i16, 1'i16]

  test "100 int32 params — data size, offsets, oids all consistent":
    var params = newSeqOfCap[PgParamInline](100)
    for i in 0 ..< 100:
      params.add toPgParamInline(int32(i * 3))
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 100 * 4
    check ranges.len == 100
    for i in 0 ..< 100:
      check ranges[i].off == int32(i * 4)
      check ranges[i].len == 4
      check data[i * 4 ..< (i + 1) * 4] == @(toBE32(int32(i * 3)))
    for o in oids:
      check o == OidInt4
    for f in formats:
      check f == 1'i16

suite "Pipeline appendInline SoA layout":
  test "single op: inlineStart/Count correct, ranges point into p.inlineData":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("INSERT", [toPgParamInline(42'i32), toPgParamInline("abc")])
    check p.ops.len == 1
    check p.ops[0].hasInline
    check p.ops[0].inlineStart == 0
    check p.ops[0].inlineCount == 2
    check p.inlineRanges.len == 2
    check p.inlineOids == @[OidInt4, OidText]
    check p.inlineFormats == @[1'i16, 0'i16]
    # Byte layout: 4 bytes for int32 + 3 bytes for "abc"
    check p.inlineData.len == 7
    check p.inlineRanges[0].off == 0
    check p.inlineRanges[0].len == 4
    check p.inlineRanges[1].off == 4
    check p.inlineRanges[1].len == 3

  test "two ops: second op's inlineStart resumes after first":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("INSERT 1", [toPgParamInline(1'i32)])
    p.addExec("INSERT 2", [toPgParamInline(2'i32), toPgParamInline(3'i32)])
    check p.ops.len == 2
    check p.ops[0].inlineStart == 0
    check p.ops[0].inlineCount == 1
    check p.ops[1].inlineStart == 1
    check p.ops[1].inlineCount == 2
    check p.inlineRanges.len == 3
    check p.inlineData.len == 12
    check p.inlineRanges[0].off == 0 # op0 param0
    check p.inlineRanges[1].off == 4 # op1 param0
    check p.inlineRanges[2].off == 8 # op1 param1

  test "mixed NULL + overflow in one op, offsets correct":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    let long = "abcdefghijklmnopqrst" # 20 chars → overflow
    p.addExec(
      "X", [toPgParamInline(9'i32), none(int32).toPgParamInline, toPgParamInline(long)]
    )
    check p.ops[0].inlineCount == 3
    check p.inlineRanges.len == 3
    check p.inlineData.len == 4 + 20 # NULL contributes nothing
    check p.inlineRanges[0].off == 0
    check p.inlineRanges[0].len == 4
    check p.inlineRanges[1].len == -1 # NULL
    check p.inlineRanges[2].off == 4
    check p.inlineRanges[2].len == 20
    check p.inlineData[4 ..< 24] == @(toBytes(long))

  test "addQuery + addExec populate same SoA buffer":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addQuery("SELECT", [toPgParamInline(1'i32)])
    p.addExec("INSERT", [toPgParamInline(2'i32)])
    check p.ops.len == 2
    check p.ops[0].kind == pokQuery
    check p.ops[1].kind == pokExec
    check p.ops[0].hasInline
    check p.ops[1].hasInline
    check p.ops[0].inlineStart == 0
    check p.ops[1].inlineStart == 1
    check p.inlineData.len == 8
    check p.inlineData[0 ..< 4] == @(toBE32(1'i32))
    check p.inlineData[4 ..< 8] == @(toBE32(2'i32))

  test "addExec with empty inline params: hasInline=true, count=0":
    # Edge case: the inline overload is chosen but no params are provided.
    # executeImpl computes `endIdx = inlineStart + inlineCount - 1`, so for
    # count==0 we get an empty `toOpenArray(start, start-1)` view — make sure
    # that view is empty and the SoA buffers are untouched.
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    let empty: seq[PgParamInline] = @[]
    p.addExec("SELECT 1", empty)
    check p.ops.len == 1
    check p.ops[0].hasInline
    check p.ops[0].inlineStart == 0
    check p.ops[0].inlineCount == 0
    check p.inlineRanges.len == 0
    check p.inlineOids.len == 0
    check p.inlineFormats.len == 0
    check p.inlineData.len == 0

  test "addExec empty inline after a populated op: start resumes correctly":
    # A second op with zero inline params must record `inlineStart` at the
    # current tail of the SoA buffers, not 0.
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("A", [toPgParamInline(1'i32), toPgParamInline(2'i32)])
    let empty: seq[PgParamInline] = @[]
    p.addExec("B", empty)
    check p.ops.len == 2
    check p.ops[1].hasInline
    check p.ops[1].inlineStart == 2 # resumes after the two params of op A
    check p.ops[1].inlineCount == 0
    check p.inlineRanges.len == 2 # unchanged by op B

suite "encodeBinaryArray with Option elements":
  test "mixed null and non-null int32":
    let elements = @[some(@(toBE32(1'i32))), none(seq[byte]), some(@(toBE32(3'i32)))]
    let encoded = encodeBinaryArray(OidInt4, elements)
    # header(20) + 3 × len(4) + 2 non-null × payload(4) = 40
    check encoded.len == 40
    check fromBE32(encoded.toOpenArray(0, 3)) == 1'i32 # ndim
    check fromBE32(encoded.toOpenArray(4, 7)) == 1'i32 # has_null = 1
    check fromBE32(encoded.toOpenArray(8, 11)) == OidInt4
    check fromBE32(encoded.toOpenArray(12, 15)) == 3'i32 # dim_len
    # element 0 len
    check fromBE32(encoded.toOpenArray(20, 23)) == 4'i32
    # element 1 len = -1 (NULL)
    check fromBE32(encoded.toOpenArray(28, 31)) == -1'i32
    # element 2 len
    check fromBE32(encoded.toOpenArray(32, 35)) == 4'i32

  test "all-some matches non-optional encoder byte-for-byte":
    let same = encodeBinaryArray(OidInt4, @[@(toBE32(1'i32)), @(toBE32(2'i32))])
    let withOpt =
      encodeBinaryArray(OidInt4, @[some(@(toBE32(1'i32))), some(@(toBE32(2'i32)))])
    check same == withOpt

  test "all-none sets has_null":
    let encoded = encodeBinaryArray(OidInt4, @[none(seq[byte]), none(seq[byte])])
    check fromBE32(encoded.toOpenArray(4, 7)) == 1'i32

suite "toPgParam seq[Option[T]]":
  test "int32 with null":
    let p = toPgParam(@[some(1'i32), none(int32), some(3'i32)])
    check p.oid == OidInt4Array
    check p.format == 1'i16
    check p.value.isSome
    # has_null should be 1
    check fromBE32(p.value.get.toOpenArray(4, 7)) == 1'i32

  test "int16 with null":
    let p = toPgParam(@[some(10'i16), none(int16)])
    check p.oid == OidInt2Array
    check fromBE32(p.value.get.toOpenArray(4, 7)) == 1'i32

  test "int64 with null":
    let p = toPgParam(@[none(int64), some(99'i64)])
    check p.oid == OidInt8Array
    check fromBE32(p.value.get.toOpenArray(4, 7)) == 1'i32

  test "int with null":
    let p = toPgParam(@[some(1), none(int)])
    check p.oid == OidInt8Array

  test "float32 with null":
    let p = toPgParam(@[some(1.5'f32), none(float32)])
    check p.oid == OidFloat4Array

  test "float64 with null":
    let p = toPgParam(@[some(1.5), none(float64)])
    check p.oid == OidFloat8Array

  test "bool with null":
    let p = toPgParam(@[some(true), none(bool), some(false)])
    check p.oid == OidBoolArray

  test "string with null":
    let p = toPgParam(@[some("a"), none(string), some("c")])
    check p.oid == OidTextArray
    check fromBE32(p.value.get.toOpenArray(4, 7)) == 1'i32

  test "empty seq[Option[int32]]":
    let v: seq[Option[int32]] = @[]
    let p = toPgParam(v)
    check p.oid == OidInt4Array
    check p.value.isSome
    # empty array: ndim=0, has_null=0, elem_oid
    check fromBE32(p.value.get.toOpenArray(0, 3)) == 0'i32

  test "all-some int32 matches non-optional":
    let a = toPgParam(@[1'i32, 2, 3])
    let b = toPgParam(@[some(1'i32), some(2'i32), some(3'i32)])
    check a.value.get == b.value.get

suite "getXxxArrayElemOpt":
  test "getIntArrayElemOpt text with NULL":
    let row: Row = @[some(toBytes("{1,NULL,3}"))]
    check row.getIntArrayElemOpt(0) == @[some(1'i32), none(int32), some(3'i32)]

  test "getIntArrayElemOpt text all-some":
    let row: Row = @[some(toBytes("{1,2,3}"))]
    check row.getIntArrayElemOpt(0) == @[some(1'i32), some(2'i32), some(3'i32)]

  test "getIntArrayElemOpt NULL column raises":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard row.getIntArrayElemOpt(0)
    except PgTypeError:
      raised = true
    check raised

  test "getIntArrayElemOptOpt NULL column is none":
    let row: Row = @[none(seq[byte])]
    check row.getIntArrayElemOptOpt(0) == none(seq[Option[int32]])

  test "getIntArrayElemOptOpt some with NULL element":
    let row: Row = @[some(toBytes("{1,NULL}"))]
    let v = row.getIntArrayElemOptOpt(0)
    check v.isSome
    check v.get == @[some(1'i32), none(int32)]

  test "getStrArrayElemOpt text with NULL":
    let row: Row = @[some(toBytes("{\"a\",NULL,\"c\"}"))]
    check row.getStrArrayElemOpt(0) == @[some("a"), none(string), some("c")]

  test "getBoolArrayElemOpt text with NULL":
    let row: Row = @[some(toBytes("{t,NULL,f}"))]
    check row.getBoolArrayElemOpt(0) == @[some(true), none(bool), some(false)]

  test "getInt16ArrayElemOpt text":
    let row: Row = @[some(toBytes("{10,NULL}"))]
    check row.getInt16ArrayElemOpt(0) == @[some(10'i16), none(int16)]

  test "getInt64ArrayElemOpt text":
    let row: Row = @[some(toBytes("{99,NULL}"))]
    check row.getInt64ArrayElemOpt(0) == @[some(99'i64), none(int64)]

  test "getFloatArrayElemOpt text":
    let row: Row = @[some(toBytes("{1.5,NULL}"))]
    let v = row.getFloatArrayElemOpt(0)
    check v.len == 2
    check v[0].isSome
    check abs(v[0].get - 1.5) < 1e-10
    check v[1].isNone

  test "getFloat32ArrayElemOpt text":
    let row: Row = @[some(toBytes("{2.5,NULL}"))]
    let v = row.getFloat32ArrayElemOpt(0)
    check v.len == 2
    check v[0].isSome
    check v[1].isNone

suite "enum arrays":
  test "toPgParam seq[Mood] emits text array literal":
    let p = toPgParam(@[happy, sad, ok])
    check p.format == 0'i16
    check toString(p.value.get) == "{\"happy\",\"sad\",\"ok\"}"

  test "toPgParam seq[Option[Mood]] emits NULL tokens":
    let p = toPgParam(@[some(happy), none(Mood), some(ok)])
    check toString(p.value.get) == "{\"happy\",NULL,\"ok\"}"

  test "toPgParam empty seq[Mood]":
    let v: seq[Mood] = @[]
    let p = toPgParam(v)
    check toString(p.value.get) == "{}"

  test "pgEnum OID=0: array uses OID 0":
    let p = toPgParam(@[happy])
    check p.oid == 0'i32

  test "pgEnum with explicit OID: scalar OID propagates to array with OID 0":
    let p = toPgParam(@[red, green])
    # 2-arg pgEnum keeps arrayOid = 0
    check p.oid == 0'i32

  test "getEnumArray text":
    let row: Row = @[some(toBytes("{happy,sad,ok}"))]
    check getEnumArray[Mood](row, 0) == @[happy, sad, ok]

  test "getEnumArray raises on NULL element":
    let row: Row = @[some(toBytes("{happy,NULL,ok}"))]
    var raised = false
    try:
      discard getEnumArray[Mood](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getEnumArrayElemOpt":
    let row: Row = @[some(toBytes("{happy,NULL,ok}"))]
    check getEnumArrayElemOpt[Mood](row, 0) == @[some(happy), none(Mood), some(ok)]

  test "getEnumArrayOpt NULL column":
    let row: Row = @[none(seq[byte])]
    check getEnumArrayOpt[Mood](row, 0) == none(seq[Mood])

  test "getEnumArrayOpt some":
    let row: Row = @[some(toBytes("{happy}"))]
    check getEnumArrayOpt[Mood](row, 0) == some(@[happy])

  test "getEnumArray invalid label raises":
    let row: Row = @[some(toBytes("{unknown}"))]
    var raised = false
    try:
      discard getEnumArray[Mood](row, 0)
    except ValueError:
      raised = true
    check raised
