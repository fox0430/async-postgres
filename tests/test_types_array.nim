import std/[json, unittest, options, strutils, times, math, net, typetraits]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}
import ../async_postgres/pg_types/ranges {.all.}

import types_common

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

suite "Binary container overflow guards":
  test "checkPgBinLen pins PgTypeError (catchable via PgError)":
    # The pg_errors conventions route oversized wire values through PgTypeError
    # so a single `except PgError` clause sees them; a bare ValueError would
    # escape that recovery path.
    when sizeof(int) >= 8:
      expect PgTypeError:
        checkPgBinLen(int(int32.high) + 1, "Array element")

  test "checkPgBinPayload pins PgTypeError (catchable via PgError)":
    expect PgTypeError:
      checkPgBinPayload(int64(int32.high) + 1, "Array")

  test "seq[int32] payload guard fires before the result buffer is allocated":
    # Drive the real `buildFixedArray` template with the first count whose
    # cumulative payload exceeds int32.high. Its `checkPgBinPayload` runs ahead
    # of the ~2 GiB `newSeq`, so no large allocation happens: the guard raises
    # first and the per-element writer never runs. Deleting the guard would
    # either raise a different error or attempt the huge allocation, so this
    # pins the wiring rather than just the arithmetic.
    const elemSize = 4
    const headerSize = 12 + 8 * 1
    const cnt = (int32.high - headerSize) div (4 + elemSize) + 1
    check int64(headerSize) + int64(cnt - 1) * int64(4 + elemSize) <= int64(int32.high)
    expect PgTypeError:
      buildFixedArray(1'i32, @[int32(cnt)], @[1'i32], cnt, elemSize):
        discard

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

  test "unterminated quoted element raises":
    var raised = false
    try:
      discard parseTextArray("{\"abc}")
    except PgTypeError:
      raised = true
    check raised

  test "unterminated quoted element after valid element raises":
    var raised = false
    try:
      discard parseTextArray("{\"ok\",\"abc}")
    except PgTypeError:
      raised = true
    check raised

  test "unterminated quoted element with trailing backslash raises":
    var raised = false
    try:
      # trailing '\\' consumes final '"' as an escape, so quote is unterminated
      discard parseTextArray("{\"abc\\\"}")
    except PgTypeError:
      raised = true
    check raised

  test "multi-dimensional unquoted raises":
    var raised = false
    var msg = ""
    try:
      discard parseTextArray("{{a,b},{c,d}}")
    except PgTypeError as e:
      raised = true
      msg = e.msg
    check raised
    check "PgArray[T]" in msg

  test "multi-dimensional with quoted subelements raises":
    var raised = false
    try:
      discard parseTextArray("{{\"a\",\"b\"},{\"c\",\"d\"}}")
    except PgTypeError:
      raised = true
    check raised

  test "nested empty subarray raises":
    var raised = false
    try:
      discard parseTextArray("{{}}")
    except PgTypeError:
      raised = true
    check raised

  test "quoted element containing literal brace is preserved":
    # Regression guard: '{' inside a quoted element must NOT trigger the
    # multi-dim reject. Only '{' at element-start position marks nesting.
    let elems = parseTextArray("{\"{a}\"}")
    check elems.len == 1
    check elems[0] == some("{a}")

  test "mixed quoted brace and plain element":
    let elems = parseTextArray("{\"a\",\"{b}\"}")
    check elems.len == 2
    check elems[0] == some("a")
    check elems[1] == some("{b}")

  test "garbage after quoted element raises":
    expect PgTypeError:
      discard parseTextArray("{\"ab\"cd}")

  test "trailing comma raises":
    expect PgTypeError:
      discard parseTextArray("{a,b,}")

  test "quote inside unquoted element raises":
    expect PgTypeError:
      discard parseTextArray("{a\"b,c}")

  test "backslash inside unquoted element raises":
    expect PgTypeError:
      discard parseTextArray("{a\\b,c}")

  test "brace inside unquoted element raises":
    expect PgTypeError:
      discard parseTextArray("{a{b},c}")

suite "Array row accessors":
  test "getIntArray":
    let row: Row = @[some(toBytes("{1,2,3}"))]
    check row.getIntArray(0) == @[1'i32, 2, 3]

  test "getIntArray empty":
    let row: Row = @[some(toBytes("{}"))]
    check row.getIntArray(0).len == 0

  test "getIntArray out of range raises (no silent truncation)":
    let row: Row = @[some(toBytes("{1,2147483648}"))]
    expect PgTypeError:
      discard row.getIntArray(0)

  test "getInt16Array":
    let row: Row = @[some(toBytes("{10,-20}"))]
    check row.getInt16Array(0) == @[10'i16, -20'i16]

  test "getInt16Array out of range raises (no silent truncation)":
    let row: Row = @[some(toBytes("{10,32768}"))]
    expect PgTypeError:
      discard row.getInt16Array(0)

  test "getIntArrayElemOpt out of range raises (no silent truncation)":
    let row: Row = @[some(toBytes("{1,NULL,-2147483649}"))]
    expect PgTypeError:
      discard row.getIntArrayElemOpt(0)

  test "getInt16ArrayElemOpt out of range raises (no silent truncation)":
    let row: Row = @[some(toBytes("{1,NULL,-32769}"))]
    expect PgTypeError:
      discard row.getInt16ArrayElemOpt(0)

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
    let off0 = 0 + decoded.elements[0].off
    let off1 = 0 + decoded.elements[1].off
    let off2 = 0 + decoded.elements[2].off
    check fromBE32(encoded[off0 ..< off0 + decoded.elements[0].len]) == 1'i32
    check fromBE32(encoded[off1 ..< off1 + decoded.elements[1].len]) == 2'i32
    check fromBE32(encoded[off2 ..< off2 + decoded.elements[2].len]) == 3'i32

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

  test "getBoolArray binary treats any non-zero byte as true":
    # PostgreSQL only ever sends 0/1, but decode non-canonical bytes the same
    # way the scalar getBool does (!= 0), not just == 1.
    let encoded = encodeBinaryArray(OidBool, @[@[2'u8], @[0xFF'u8], @[0'u8]])
    let fields = @[mkField(OidBoolArray, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getBoolArray(0) == @[true, true, false]

  test "getBoolArrayElemOpt binary treats any non-zero byte as true":
    let encoded = encodeBinaryArray(OidBool, @[@[2'u8], @[0xFF'u8], @[0'u8]])
    let fields = @[mkField(OidBoolArray, 1)]
    let row = mkRow(@[some(encoded)], fields)
    check row.getBoolArrayElemOpt(0) == @[some(true), some(true), some(false)]

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
    let o0 = 0 + decoded.elements[0].off
    let o1 = 0 + decoded.elements[1].off
    let o2 = 0 + decoded.elements[2].off
    check fromBE32(data[o0 ..< o0 + 4]) == 100'i32
    check fromBE32(data[o1 ..< o1 + 4]) == 200'i32
    check fromBE32(data[o2 ..< o2 + 4]) == 300'i32

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

  test "getTimestampArray text format with trimmed fractional seconds":
    let row: Row = @[
      some(
        toBytes(
          "{\"2023-01-15 10:30:00.5\",\"2024-06-20 14:45:30.123\",\"2025-03-10 08:00:00.123456\"}"
        )
      )
    ]
    let arr = row.getTimestampArray(0)
    check arr.len == 3
    check arr[0].nanosecond == 500_000_000
    check arr[1].nanosecond == 123_000_000
    check arr[2].nanosecond == 123_456_000

  test "getTimestampArray text elements decode as UTC":
    let row: Row = @[some(toBytes("{\"2023-01-15 10:30:00\",\"2024-06-20 14:45:30\"}"))]
    let arr = row.getTimestampArray(0)
    check arr[0].timezone == utc()
    check arr[1].timezone == utc()

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
    let tv2 = PgTsVector("'foo':3")
    let p = toPgParam(@[tv1, tv2])
    check p.oid == OidTsVectorArray
    check p.format == 0'i16
    # Text format roundtrip (binary tsvector is structured, so the
    # parameter is text and decodes via the text path).
    let row: Row = @[p.value]
    let arr = row.getTsVectorArray(0)
    check arr.len == 2
    check string(arr[0]) == string(tv1)
    check string(arr[1]) == string(tv2)

  test "toPgParam seq[PgTsQuery] roundtrip":
    let tq1 = PgTsQuery("hello & world")
    let tq2 = PgTsQuery("foo | bar")
    let p = toPgParam(@[tq1, tq2])
    check p.oid == OidTsQueryArray
    check p.format == 0'i16
    let row: Row = @[p.value]
    let arr = row.getTsQueryArray(0)
    check arr.len == 2
    check string(arr[0]) == string(tq1)
    check string(arr[1]) == string(tq2)

  test "toPgParam seq[PgTsVector] empty":
    let p = toPgParam(newSeq[PgTsVector]())
    check p.oid == OidTsVectorArray
    check p.format == 0'i16
    let row: Row = @[p.value]
    check row.getTsVectorArray(0).len == 0

  test "toPgParam seq[PgTsQuery] empty":
    let p = toPgParam(newSeq[PgTsQuery]())
    check p.oid == OidTsQueryArray
    check p.format == 0'i16
    let row: Row = @[p.value]
    check row.getTsQueryArray(0).len == 0

  test "getTsVectorArray binary decodes structured elements":
    # Same binary body as "getTsVector binary format" ('cat':1A).
    var elem: seq[byte] = @[]
    elem.add(@(toBE32(1'i32)))
    for c in "cat":
      elem.add(byte(c))
    elem.add(0'u8)
    elem.add(@(toBE16(1'i16)))
    elem.add(@(toBE16(cast[int16](0xC001'u16))))
    let payload = encodeBinaryArray(OidTsVector, @[some(elem)])
    let fields = @[mkField(OidTsVectorArray, 1'i16)]
    let row = mkRow(@[some(payload)], fields)
    let arr = row.getTsVectorArray(0)
    check arr.len == 1
    check $arr[0] == "'cat':1A"

  test "getTsQueryArray binary decodes structured elements":
    # Same binary body as "getTsQuery binary format simple AND".
    var elem: seq[byte] = @[]
    elem.add(@(toBE32(3'i32)))
    elem.add(2'u8)
    elem.add(2'u8)
    elem.add(1'u8)
    elem.add(0'u8)
    elem.add(0'u8)
    for c in "cat":
      elem.add(byte(c))
    elem.add(0'u8)
    elem.add(1'u8)
    elem.add(0'u8)
    elem.add(0'u8)
    for c in "dog":
      elem.add(byte(c))
    elem.add(0'u8)
    let payload = encodeBinaryArray(OidTsQuery, @[some(elem)])
    let fields = @[mkField(OidTsQueryArray, 1'i16)]
    let row = mkRow(@[some(payload)], fields)
    let arr = row.getTsQueryArray(0)
    check arr.len == 1
    check $arr[0] == "'cat' & 'dog'"

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

  test "toPgTsMultirangeArrayParam with non-UTC zone encodes the UTC instant":
    # Regression: the ts multirange array text path used $, which serializes the
    # local wall clock rather than UTC.
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
    let dt1 = dateTime(2026, mJul, 15, 21, 0, 0, 0, jst)
    let dt2 = dateTime(2026, mJul, 16, 9, 0, 0, 0, jst)
    let p = toPgTsMultirangeArrayParam(@[toMultirange(rangeOf(dt1, dt2))])
    check p.oid == OidTsMultirangeArray
    check p.value.get.toString ==
      "{\"{[\\\"2026-07-15 12:00:00.000000\\\",\\\"2026-07-16 00:00:00.000000\\\")}\"}"

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

  test "getEnumArray binary rejects int4 elemOid":
    let p = toPgBinaryParam(@[1'i32, 2'i32])
    let row = mkRow(@[p.value], @[mkField(OidInt4Array, 1)])
    expect PgTypeError:
      discard getEnumArray[Mood](row, 0)

  test "getEnumArray binary with unknown elemOid reads labels":
    # Dynamic enum-array element OIDs must keep passing the guard.
    let payload = encodeBinaryArray(99999'i32, @[toBytes("happy"), toBytes("sad")])
    let row = mkRow(@[some(payload)], @[mkField(99999'i32, 1'i16)])
    check getEnumArray[Mood](row, 0) == @[happy, sad]

  test "getEnumArray binary reads a text elemOid":
    let p = toPgBinaryParam(@["happy", "sad"])
    let row = mkRow(@[p.value], @[mkField(OidTextArray, 1)])
    check getEnumArray[Mood](row, 0) == @[happy, sad]

  test "getEnumArrayElemOpt binary reads a text elemOid":
    let p = toPgBinaryParam(@["happy", "sad"])
    let row = mkRow(@[p.value], @[mkField(OidTextArray, 1)])
    check getEnumArrayElemOpt[Mood](row, 0) == @[some(happy), some(sad)]

  test "getEnumArray binary rejects an int elemOid":
    let p = toPgBinaryParam(@[1'i32, 2'i32])
    let row = mkRow(@[p.value], @[mkField(OidInt4Array, 1)])
    expect PgTypeError:
      discard getEnumArray[Mood](row, 0)

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
    except PgTypeError:
      raised = true
    check raised

suite "PgArray[T] construction and validation":
  test "pgArray 1D from non-NULL elements":
    let a = pgArray(@[1'i32, 2, 3, 4])
    check a.dims == @[4'i32]
    check a.lowerBounds == @[1'i32]
    check a.elements.len == 4
    check a.elements[0] == some(1'i32)
    check a.elements[3] == some(4'i32)
    check a.ndim == 1
    check not a.isEmpty

  test "pgArray 1D from Option elements":
    let a = pgArray(@[some(1'i32), none(int32), some(3'i32)])
    check a.dims == @[3'i32]
    check a.elements[1] == none(int32)

  test "pgArray 1D empty":
    let a = pgArray(newSeq[int32]())
    check a.dims.len == 0
    check a.lowerBounds.len == 0
    check a.elements.len == 0
    check a.isEmpty
    check a.ndim == 0

  test "pgArray 2D rectangular":
    let a = pgArray(@[2'i32, 3], @[1'i32, 2, 3, 4, 5, 6])
    check a.dims == @[2'i32, 3]
    check a.lowerBounds == @[1'i32, 1]
    check a.elements.len == 6
    check a.ndim == 2

  test "pgArray 2D with NULL elements":
    let a = pgArray(@[2'i32, 2], @[some(1'i32), none(int32), some(3'i32), some(4'i32)])
    check a.dims == @[2'i32, 2]
    check a.elements[1] == none(int32)

  test "pgArray validates length mismatch":
    expect PgError:
      discard pgArray(@[2'i32, 3], @[1'i32, 2, 3]) # 6 expected, 3 given

  test "pgArray length mismatch with empty elements hints at dims = @[]":
    # Non-empty dims + empty elements is a common API misuse; the error
    # message should point users at the ``dims = @[]`` empty-array form
    # instead of just reporting the count mismatch.
    var raised = false
    try:
      discard pgArray(@[3'i32], newSeq[Option[int32]]())
    except PgError as e:
      raised = true
      check "dims = @[]" in e.msg
    check raised

  test "pgArray validates dims/lowerBounds length mismatch":
    expect PgError:
      discard pgArray(
        @[2'i32, 3],
        @[1'i32],
        @[some(1'i32), some(2'i32), some(3'i32), some(4'i32), some(5'i32), some(6'i32)],
      )

  test "pgArray rejects too many dimensions":
    expect PgError:
      discard pgArray(@[1'i32, 1, 1, 1, 1, 1, 1], @[1'i32])

  test "pgArray rejects zero-sized dimension":
    # Empty arrays must use dims = @[] (ndim = 0), not a zero dim_len.
    expect PgError:
      discard pgArray(@[3'i32, 0], newSeq[Option[int32]]())
    expect PgError:
      discard pgArray(@[0'i32], newSeq[Option[int32]]())

  test "pgArray with explicit lowerBounds":
    let a = pgArray(@[3'i32], @[2'i32], @[some(10'i32), some(20'i32), some(30'i32)])
    check a.lowerBounds == @[2'i32]

  test "expectedElemCount product":
    check expectedElemCount(@[]) == 0
    check expectedElemCount(@[5'i32]) == 5
    check expectedElemCount(@[2'i32, 3]) == 6
    check expectedElemCount(@[2'i32, 3, 4]) == 24

  test "expectedElemCount rejects negative":
    expect PgError:
      discard expectedElemCount(@[-1'i32])

  test "expectedElemCount rejects overflow":
    expect PgError:
      discard expectedElemCount(@[int32.high, 2'i32])

suite "PgArray[T] toPgParam":
  test "toPgParam PgArray[int32] 1D":
    let p = toPgParam(pgArray(@[10'i32, 20, 30]))
    check p.oid == OidInt4Array
    check p.format == 1
    let data = p.value.get
    check fromBE32(data.toOpenArray(0, 3)) == 1'i32 # ndim
    check fromBE32(data.toOpenArray(12, 15)) == 3'i32 # dim_len
    check fromBE32(data.toOpenArray(16, 19)) == 1'i32 # lower_bound
    # decode first element
    let dec = decodeBinaryArray(data)
    check dec.dims == @[3'i32]
    check dec.elements.len == 3

  test "toPgParam PgArray[int32] 2D wire format":
    let a = pgArray(@[2'i32, 2], @[1'i32, 2, 3, 4])
    let p = toPgParam(a)
    check p.oid == OidInt4Array
    let data = p.value.get
    # Header: 12 + 8*2 = 28 bytes
    check fromBE32(data.toOpenArray(0, 3)) == 2'i32 # ndim
    check fromBE32(data.toOpenArray(4, 7)) == 0'i32 # has_null
    check fromBE32(data.toOpenArray(8, 11)) == OidInt4 # elem_oid
    check fromBE32(data.toOpenArray(12, 15)) == 2'i32 # dim_len[0]
    check fromBE32(data.toOpenArray(16, 19)) == 1'i32 # lower_bound[0]
    check fromBE32(data.toOpenArray(20, 23)) == 2'i32 # dim_len[1]
    check fromBE32(data.toOpenArray(24, 27)) == 1'i32 # lower_bound[1]
    # 4 elements * 8 bytes (len + payload) = 32 bytes total payload
    check data.len == 28 + 32

  test "toPgParam empty PgArray[string]":
    let a = pgArray(newSeq[string]())
    let p = toPgParam(a)
    check p.oid == OidTextArray
    let data = p.value.get
    check fromBE32(data.toOpenArray(0, 3)) == 0'i32 # ndim=0
    check data.len == 12

  test "toPgParam PgArray with NULL elements sets has_null":
    let a = pgArray(@[some(1'i32), none(int32), some(3'i32)])
    let p = toPgParam(a)
    let data = p.value.get
    check fromBE32(data.toOpenArray(4, 7)) == 1'i32

  test "toPgParam PgArray[bool] 3D":
    let a =
      pgArray(@[2'i32, 2, 2], @[true, false, true, false, false, true, true, true])
    let p = toPgParam(a)
    check p.oid == OidBoolArray
    let data = p.value.get
    check fromBE32(data.toOpenArray(0, 3)) == 3'i32 # ndim
    # header = 12 + 8*3 = 36, then 8 elements * 5 bytes (len+1) = 40
    check data.len == 36 + 40

suite "PgArray[T] decode and getArrayND":
  test "getArrayND[int32] 1D roundtrip":
    let src = pgArray(@[100'i32, 200, 300])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[int32](row, 0)
    check got == src

  test "getArrayND[int32] 2D roundtrip":
    let src = pgArray(@[2'i32, 3], @[1'i32, 2, 3, 4, 5, 6])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[int32](row, 0)
    check got == src
    check got.dims == @[2'i32, 3]
    check got.elements.len == 6
    check got.elements[5] == some(6'i32)

  test "getArrayND[string] 2D with NULLs":
    let src = pgArray(@[2'i32, 2], @[some("a"), none(string), some("c"), some("d")])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidTextArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[string](row, 0)
    check got.dims == @[2'i32, 2]
    check got.elements[1] == none(string)
    check got.elements[2] == some("c")

  test "getArrayND[int32] empty":
    let src = pgArray(newSeq[int32]())
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[int32](row, 0)
    check got.isEmpty

  test "1D getIntArray rejects multi-dim with clear error":
    let src = pgArray(@[2'i32, 2], @[1'i32, 2, 3, 4])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getIntArray(0)

  test "getArrayND[int32] non-default lowerBounds preserved":
    let src = pgArray(@[3'i32], @[5'i32], @[some(10'i32), some(20'i32), some(30'i32)])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[int32](row, 0)
    check got.lowerBounds == @[5'i32]
    check got.dims == @[3'i32]

  test "getArrayNDOpt none":
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getArrayNDOpt[int32](row, 0) == none(PgArray[int32])

  test "getArrayNDOpt some":
    let src = pgArray(@[42'i32, 7])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    check getArrayNDOpt[int32](row, 0) == some(src)

suite "PgArray[T] geometric type multi-dim round-trip":
  # Exercise the registry wiring for variable-length geometric element types,
  # which take separate ``encodePgArrayElement`` / ``decodePgArrayElement``
  # paths and care about per-element ``elen``.
  test "PgArray[PgPoint] 2D round-trip":
    let pts = @[
      PgPoint(x: 0.0, y: 0.0),
      PgPoint(x: 1.0, y: 2.0),
      PgPoint(x: -3.5, y: 4.25),
      PgPoint(x: 1e10, y: -1e-10),
    ]
    let src = pgArray(@[2'i32, 2], pts)
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidPointArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[PgPoint](row, 0)
    check got.dims == @[2'i32, 2]
    check got == src

  test "PgArray[PgBox] 2D round-trip":
    let boxes = @[
      PgBox(high: PgPoint(x: 1, y: 2), low: PgPoint(x: 0, y: 0)),
      PgBox(high: PgPoint(x: 3, y: 4), low: PgPoint(x: -1, y: -2)),
    ]
    let src = pgArray(@[1'i32, 2], boxes)
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidBoxArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[PgBox](row, 0)
    check got.dims == @[1'i32, 2]
    check got == src

  test "PgArray[PgLseg] 2D round-trip":
    let segs = @[
      PgLseg(p1: PgPoint(x: 0, y: 0), p2: PgPoint(x: 1, y: 1)),
      PgLseg(p1: PgPoint(x: 2, y: 3), p2: PgPoint(x: 4, y: 5)),
    ]
    let src = pgArray(@[2'i32, 1], segs)
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidLsegArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[PgLseg](row, 0)
    check got == src

  test "PgArray[PgLine] 2D round-trip":
    let lines = @[PgLine(a: 1, b: 2, c: 3), PgLine(a: -4, b: 5, c: -6)]
    let src = pgArray(@[1'i32, 2], lines)
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidLineArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[PgLine](row, 0)
    check got == src

  test "PgArray[PgCircle] 2D round-trip":
    let circles = @[
      PgCircle(center: PgPoint(x: 0, y: 0), radius: 1.0),
      PgCircle(center: PgPoint(x: 2, y: -3), radius: 4.5),
    ]
    let src = pgArray(@[2'i32, 1], circles)
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidCircleArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[PgCircle](row, 0)
    check got == src

  test "PgArray[PgPath] 2D round-trip varying point counts":
    # PgPath has variable-size payload; exercise both an open and a closed
    # path with different point counts inside a 2D array.
    let paths = @[
      PgPath(closed: false, points: @[PgPoint(x: 0, y: 0), PgPoint(x: 1, y: 1)]),
      PgPath(
        closed: true,
        points: @[PgPoint(x: 0, y: 0), PgPoint(x: 1, y: 0), PgPoint(x: 1, y: 1)],
      ),
    ]
    let src = pgArray(@[1'i32, 2], paths)
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidPathArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[PgPath](row, 0)
    check got.dims == @[1'i32, 2]
    check got == src

  test "PgArray[PgPolygon] 2D round-trip with NULL element":
    let polys = @[
      some(
        PgPolygon(
          points: @[PgPoint(x: 0, y: 0), PgPoint(x: 1, y: 0), PgPoint(x: 0, y: 1)]
        )
      ),
      none(PgPolygon),
      some(
        PgPolygon(
          points: @[
            PgPoint(x: 0, y: 0),
            PgPoint(x: 2, y: 0),
            PgPoint(x: 2, y: 2),
            PgPoint(x: 0, y: 2),
          ]
        )
      ),
      some(PgPolygon(points: @[])), # zero-point polygon (npts=0)
    ]
    let src = pgArray(@[2'i32, 2], polys)
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidPolygonArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[PgPolygon](row, 0)
    check got.dims == @[2'i32, 2]
    check got.elements[1] == none(PgPolygon)
    check got == src

suite "PgArray[T] registry compile-time errors":
  test "toPgParam(PgArray[DateTime]) does not compile":
    check not compiles(toPgParam(default(PgArray[DateTime])))

  test "toPgParam(PgArray[seq[byte]]) does not compile":
    check not compiles(toPgParam(default(PgArray[seq[byte]])))

  test "toPgParam(PgArray[PgHstore]) does not compile":
    check not compiles(toPgParam(default(PgArray[PgHstore])))

  test "getArrayND[DateTime] does not compile":
    let r: Row = default(Row)
    check not compiles(getArrayND[DateTime](r, 0))

  test "getArrayND[seq[byte]] does not compile":
    let r: Row = default(Row)
    check not compiles(getArrayND[seq[byte]](r, 0))

  test "toPgParam(PgArray[int32]) still compiles":
    # Sanity: registered types remain usable.
    check compiles(toPgParam(default(PgArray[int32])))

  test "toPgParam(PgArray[int]) does not compile":
    check not compiles(toPgParam(default(PgArray[int])))

  test "toPgParam(PgArray[PgTsVector]) does not compile":
    check not compiles(toPgParam(default(PgArray[PgTsVector])))

  test "toPgParam(PgArray[PgTsQuery]) does not compile":
    check not compiles(toPgParam(default(PgArray[PgTsQuery])))

  test "getArrayND[int] does not compile":
    let r: Row = default(Row)
    check not compiles(getArrayND[int](r, 0))

  test "getArrayND[PgMoney] does not compile (use getMoneyArrayND)":
    let r: Row = default(Row)
    check not compiles(getArrayND[PgMoney](r, 0))

  test "toPgParam(PgArray[PgMoney]) does not compile (use toPgMoneyArrayNDParam)":
    check not compiles(toPgParam(default(PgArray[PgMoney])))

  test "getArrayND[PgTsVector] does not compile":
    let r: Row = default(Row)
    check not compiles(getArrayND[PgTsVector](r, 0))

  test "getArrayND[PgTsQuery] does not compile":
    let r: Row = default(Row)
    check not compiles(getArrayND[PgTsQuery](r, 0))

  test "encodePgArrayElement(PgTsVector) is not exposed":
    # Registry entries for PgTsVector/PgTsQuery were removed since the
    # PgArray entrypoint rejects them; direct calls must not compile so
    # users cannot accidentally feed text bytes into encodeBinaryArray.
    check not compiles(encodePgArrayElement(PgTsVector("foo")))
    check not compiles(encodePgArrayElement(PgTsQuery("foo")))

  test "encodePgArrayElement(PgMoney) is not exposed":
    # Removed to force callers through toPgMoneyArrayNDParam, which
    # validates the scale invariant against the server's frac_digits.
    check not compiles(encodePgArrayElement(initPgMoney(0, 2)))

suite "getMoneyArrayND scale":
  test "getMoneyArrayND default scale=2":
    # Build a money[] wire payload with two amounts: 12345 (= $123.45) and 100.
    let src = pgArray(@[initPgMoney(12345, 2), initPgMoney(100, 2)])
    let bin = toPgMoneyArrayNDParam(src).value.get
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getMoneyArrayND(row, 0)
    check got.elements.len == 2
    check got.elements[0].get.amount == 12345
    check got.elements[0].get.scale == 2

  test "getMoneyArrayND honors explicit scale":
    let src = pgArray(@[initPgMoney(12345, 3)])
    let bin = toPgMoneyArrayNDParam(src, scale = 3).value.get
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getMoneyArrayND(row, 0, scale = 3)
    check got.elements[0].get.scale == 3
    check got.elements[0].get.amount == 12345

  test "getMoneyArrayND rejects bad scale":
    let src = pgArray(@[initPgMoney(1, 2)])
    let bin = toPgMoneyArrayNDParam(src).value.get
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard getMoneyArrayND(row, 0, scale = -1)
    expect PgTypeError:
      discard getMoneyArrayND(row, 0, scale = 19)

  test "getMoneyArrayNDOpt returns none for NULL column":
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getMoneyArrayNDOpt(row, 0) == none(PgArray[PgMoney])

  test "getMoneyArrayND by name forwards scale":
    let src = pgArray(@[initPgMoney(12345, 3)])
    let bin = toPgMoneyArrayNDParam(src, scale = 3).value.get
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = row.getMoneyArrayND("test", scale = 3)
    check got.elements[0].get.scale == 3
    check got.elements[0].get.amount == 12345

  test "getMoneyArrayNDOpt by name returns none for NULL":
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getMoneyArrayNDOpt("test") == none(PgArray[PgMoney])

suite "toPgMoneyArrayNDParam":
  test "toPgMoneyArrayNDParam roundtrip with default scale":
    let src = pgArray(@[initPgMoney(100, 2), initPgMoney(-250, 2)])
    let p = toPgMoneyArrayNDParam(src)
    check p.oid == OidMoneyArray
    check p.format == 1
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let got = getMoneyArrayND(row, 0)
    check got.elements.len == 2
    check got.elements[0].get.amount == 100
    check got.elements[1].get.amount == -250
    check got.elements[1].get.scale == 2

  test "toPgMoneyArrayNDParam roundtrip with explicit scale=3":
    let src = pgArray(@[initPgMoney(12345, 3)])
    let p = toPgMoneyArrayNDParam(src, scale = 3)
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let got = getMoneyArrayND(row, 0, scale = 3)
    check got.elements[0].get.amount == 12345
    check got.elements[0].get.scale == 3

  test "toPgMoneyArrayNDParam honors NULLs and multi-dim":
    let src = pgArray(
      @[2'i32, 2],
      @[
        some(initPgMoney(1, 2)),
        none(PgMoney),
        some(initPgMoney(2, 2)),
        some(initPgMoney(3, 2)),
      ],
    )
    let p = toPgMoneyArrayNDParam(src)
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let got = getMoneyArrayND(row, 0)
    check got.dims == @[2'i32, 2]
    check got.elements[1].isNone
    check got.elements[2].get.amount == 2

  test "toPgMoneyArrayNDParam rejects element scale mismatch":
    let src = pgArray(@[initPgMoney(1, 2), initPgMoney(2, 3)])
    expect PgTypeError:
      discard toPgMoneyArrayNDParam(src, scale = 2)

  test "toPgMoneyArrayNDParam rejects bad scale argument":
    let src = pgArray(@[initPgMoney(1, 2)])
    expect PgTypeError:
      discard toPgMoneyArrayNDParam(src, scale = -1)
    expect PgTypeError:
      discard toPgMoneyArrayNDParam(src, scale = 19)

  test "toPgMoneyArrayNDParam empty array":
    let src = pgArray(newSeq[PgMoney]())
    let p = toPgMoneyArrayNDParam(src)
    check p.oid == OidMoneyArray
    let data = p.value.get
    check fromBE32(data.toOpenArray(0, 3)) == 0'i32 # ndim=0
    check data.len == 12

suite "toPgMoneyArrayParam":
  test "toPgParam seq[PgMoney] default scale=2 roundtrip":
    let values = @[initPgMoney(100), initPgMoney(-50), initPgMoney(999999)]
    let p = toPgParam(values)
    check p.oid == OidMoneyArray
    check p.format == 1
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[p.value], fields)
    check row.getMoneyArray(0) == values

  test "toPgParam seq[PgMoney] rejects non-default element scale":
    # The bare toPgParam path declares scale=2; a scale-0 element would be
    # silently encoded as the wrong money value, so it must be rejected.
    expect PgTypeError:
      discard toPgParam(@[initPgMoney(100), initPgMoney(50, scale = 0)])

  test "toPgMoneyArrayParam honors explicit scale=0 roundtrip":
    let values = @[initPgMoney(100, scale = 0), initPgMoney(-50, scale = 0)]
    let p = toPgMoneyArrayParam(values, scale = 0)
    check p.oid == OidMoneyArray
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[p.value], fields)
    check row.getMoneyArray(0, scale = 0) == values

  test "toPgMoneyArrayParam rejects element scale mismatch":
    let values = @[initPgMoney(1, scale = 2), initPgMoney(2, scale = 3)]
    expect PgTypeError:
      discard toPgMoneyArrayParam(values, scale = 2)

  test "toPgMoneyArrayParam rejects bad scale argument":
    let values = @[initPgMoney(1)]
    expect PgTypeError:
      discard toPgMoneyArrayParam(values, scale = -1)
    expect PgTypeError:
      discard toPgMoneyArrayParam(values, scale = 19)

  test "toPgMoneyArrayParam empty array":
    let p = toPgMoneyArrayParam(newSeq[PgMoney]())
    check p.oid == OidMoneyArray
    let data = p.value.get
    check fromBE32(data.toOpenArray(0, 3)) == 0'i32 # ndim=0
    check data.len == 12

suite "PgArray[T] elemOid validation":
  test "getArrayND[int32] rejects text[] column":
    # Encode a text[] payload, try to read as int32[].
    let src = pgArray(@["abcd", "efgh"])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidTextArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard getArrayND[int32](row, 0)

  test "getArrayND[JsonNode] accepts json[] (no version byte)":
    # Build a wire payload manually with OidJson and raw JSON bytes
    # (no jsonb version-byte prefix).
    let body = "\"hello\""
    let elemPayload = toBytes(body)
    var data = newSeq[byte](12 + 8 + 4 + elemPayload.len)
    data.writeBE32(0, 1'i32) # ndim
    data.writeBE32(4, 0'i32) # has_null
    data.writeBE32(8, OidJson) # elem_oid = json
    data.writeBE32(12, 1'i32) # dim_len
    data.writeBE32(16, 1'i32) # lower_bound
    data.writeBE32(20, int32(elemPayload.len)) # element length
    for i, b in elemPayload:
      data[24 + i] = b
    # Field OID isn't consulted by getArrayND (only wire elemOid is); use jsonb[].
    let fields = @[mkField(OidJsonbArray, 1)]
    let row = mkRow(@[some(data)], fields)
    let got = getArrayND[JsonNode](row, 0)
    check got.elements.len == 1
    check got.elements[0].get.getStr == "hello"

  test "getArrayND[JsonNode] accepts jsonb[] (with version byte)":
    let src = pgArray(@[%*"world"])
    let bin = toPgParam(src).value.get
    let fields = @[mkField(OidJsonbArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    let got = getArrayND[JsonNode](row, 0)
    check got.elements[0].get.getStr == "world"

  test "getArrayND[JsonNode] json[] does not strip a leading 0x01 byte":
    # Adversarial payload: an element whose body really starts with 0x01.
    # Bytes 0x01 + "\"a\"" = invalid JSON. The legacy `buf[0] == 1` heuristic
    # would silently strip the 0x01 and parse the remaining `"a"` (valid JSON
    # — JString "a"). The elemOid-driven path keeps the 0x01 in place, so
    # parseJson must raise.
    let elemPayload = @[0x01'u8, byte('"'), byte('a'), byte('"')]
    var data = newSeq[byte](12 + 8 + 4 + elemPayload.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidJson)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elemPayload.len))
    for i, b in elemPayload:
      data[24 + i] = b
    let fields = @[mkField(OidJsonbArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getArrayND[JsonNode](row, 0)

suite "PgArray[T] element decoder validation":
  test "decodePgArrayElement[PgBit] rejects negative nbits":
    # nbits = -1, 4 bytes of "data" (irrelevant)
    var buf = newSeq[byte](8)
    buf.writeBE32(0, -1'i32)
    expect PgTypeError:
      discard decodePgArrayElement(PgBit, buf)

  test "decodePgArrayElement[PgBit] rejects nbits/dataLen mismatch":
    # nbits = 8 → needs 1 data byte, but supply 2.
    var buf = newSeq[byte](6)
    buf.writeBE32(0, 8'i32)
    expect PgTypeError:
      discard decodePgArrayElement(PgBit, buf)

  test "decodePgArrayElement[PgBit] accepts valid nbits=0":
    # nbits = 0 → 0 data bytes. Header-only payload is valid.
    var buf = newSeq[byte](4)
    buf.writeBE32(0, 0'i32)
    let bit = decodePgArrayElement(PgBit, buf)
    check bit.nbits == 0
    check bit.data.len == 0

  test "decodePgArrayElement[bool] treats any non-zero as true":
    # Matches the scalar getBool convention (buf[0] != 0).
    check decodePgArrayElement(bool, @[0'u8]) == false
    check decodePgArrayElement(bool, @[1'u8]) == true
    check decodePgArrayElement(bool, @[2'u8]) == true
    check decodePgArrayElement(bool, @[0xFF'u8]) == true

  test "decodePgArrayElement[PgBit] accepts nbits=12 with 2 data bytes":
    # nbits = 12 → (12 + 7) div 8 = 2 data bytes.
    var buf = newSeq[byte](6)
    buf.writeBE32(0, 12'i32)
    buf[4] = 0xAB
    buf[5] = 0xC0
    let bit = decodePgArrayElement(PgBit, buf)
    check bit.nbits == 12
    check bit.data == @[0xAB'u8, 0xC0'u8]

  test "decodePgArrayElement[PgPath] rejects trailing slack bytes":
    # closed(1) + npts(4)=0 + 0 points = 5 bytes total. A 13-byte buffer with
    # the same header has 8 unused bytes; the strict decoder must reject.
    var buf = newSeq[byte](13)
    buf[0] = 0
    buf.writeBE32(1, 0'i32)
    expect PgTypeError:
      discard decodePgArrayElement(PgPath, buf)

  test "decodePgArrayElement[PgPolygon] rejects trailing slack bytes":
    var buf = newSeq[byte](12)
    buf.writeBE32(0, 0'i32) # npts=0, but buf.len 4+8 trailing
    expect PgTypeError:
      discard decodePgArrayElement(PgPolygon, buf)

  test "decodePgArrayElement[PgPath] rejects npts that overflows int32":
    # npts = 0x10000000: in int32, npts*16 wraps to 0 and would spoof the
    # 5-byte length check. The decoder must widen to int64 and reject.
    var buf = newSeq[byte](5)
    buf[0] = 0
    buf.writeBE32(1, 0x10000000'i32)
    expect PgTypeError:
      discard decodePgArrayElement(PgPath, buf)

  test "decodePgArrayElement[PgPolygon] rejects npts that overflows int32":
    var buf = newSeq[byte](4)
    buf.writeBE32(0, 0x10000000'i32)
    expect PgTypeError:
      discard decodePgArrayElement(PgPolygon, buf)

suite "1-D path/polygon binary element validation":
  test "pathElemFromBinary rejects negative npts via getPathArray":
    # Build a path[] payload whose single element has npts = -1 (0xFFFFFFFF).
    # Element wire: closed(1) + npts(4) = 5 bytes.
    var elem = newSeq[byte](5)
    elem[0] = 0
    elem.writeBE32(1, -1'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32) # ndim=1
    data.writeBE32(4, 0'i32) # has_null
    data.writeBE32(8, OidPath)
    data.writeBE32(12, 1'i32) # dim_len
    data.writeBE32(16, 1'i32) # lower_bound
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPathArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPathArray(0)

  test "pathElemFromBinary rejects oversized npts via getPathArray":
    # npts claims 100 points (1605 bytes needed) but element is only 5 bytes.
    var elem = newSeq[byte](5)
    elem[0] = 0
    elem.writeBE32(1, 100'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidPath)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPathArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPathArray(0)

  test "polygonElemFromBinary rejects negative npts via getPolygonArray":
    var elem = newSeq[byte](4)
    elem.writeBE32(0, -1'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidPolygon)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPolygonArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPolygonArray(0)

  test "polygonElemFromBinary rejects oversized npts via getPolygonArray":
    var elem = newSeq[byte](4)
    elem.writeBE32(0, 100'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidPolygon)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPolygonArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPolygonArray(0)

  test "pathElemFromBinary rejects trailing slack via getPathArray":
    # Element wire: closed(1) + npts(4)=0 + extra(8) = 13 bytes. With strict
    # equality, 13 != 5 + 0*16 → must reject.
    var elem = newSeq[byte](13)
    elem[0] = 0
    elem.writeBE32(1, 0'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidPath)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPathArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPathArray(0)

  test "polygonElemFromBinary rejects trailing slack via getPolygonArray":
    # Element wire: npts(4)=0 + extra(8) = 12 bytes. 12 != 4 + 0*16 → reject.
    var elem = newSeq[byte](12)
    elem.writeBE32(0, 0'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidPolygon)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPolygonArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPolygonArray(0)

  test "pathElemFromBinary rejects npts that overflows int32 via getPathArray":
    # npts = 0x10000000 makes npts*16 wrap to 0 in int32, spoofing the 5-byte
    # element-length check; the int64 check must still reject it.
    var elem = newSeq[byte](5)
    elem[0] = 0
    elem.writeBE32(1, 0x10000000'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidPath)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPathArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPathArray(0)

  test "polygonElemFromBinary rejects npts that overflows int32 via getPolygonArray":
    var elem = newSeq[byte](4)
    elem.writeBE32(0, 0x10000000'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidPolygon)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidPolygonArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getPolygonArray(0)

suite "1-D bit binary element validation":
  test "getBit rejects negative nbits in binary":
    var data = newSeq[byte](4)
    data.writeBE32(0, -1'i32)
    let fields = @[mkField(OidVarbit, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getBit(0)

  test "getBit rejects nbits/dataLen mismatch":
    # nbits=33 needs 5 data bytes; supply only 1.
    var data = newSeq[byte](5)
    data.writeBE32(0, 33'i32)
    let fields = @[mkField(OidVarbit, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getBit(0)

  test "getBitArray rejects negative nbits per element":
    # 1-element bit[] whose payload is nbits=-1 + 0 data bytes (4 bytes total).
    var elem = newSeq[byte](4)
    elem.writeBE32(0, -1'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidVarbit)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidVarbitArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getBitArray(0)

  test "getBitArray rejects nbits/dataLen mismatch per element":
    var elem = newSeq[byte](5)
    elem.writeBE32(0, 33'i32)
    var data = newSeq[byte](20 + 4 + elem.len)
    data.writeBE32(0, 1'i32)
    data.writeBE32(4, 0'i32)
    data.writeBE32(8, OidVarbit)
    data.writeBE32(12, 1'i32)
    data.writeBE32(16, 1'i32)
    data.writeBE32(20, int32(elem.len))
    for i, b in elem:
      data[24 + i] = b
    let fields = @[mkField(OidVarbitArray, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getBitArray(0)

  test "getBit rejects nbits exceeding PgBitMaxBits":
    # nbits = PgBitMaxBits + 1 → reject before any allocation.
    var data = newSeq[byte](4)
    data.writeBE32(0, PgBitMaxBits + 1'i32)
    let fields = @[mkField(OidVarbit, 1)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard row.getBit(0)

  test "decodePgArrayElement[PgBit] rejects nbits exceeding PgBitMaxBits":
    var buf = newSeq[byte](4)
    buf.writeBE32(0, PgBitMaxBits + 1'i32)
    expect PgTypeError:
      discard decodePgArrayElement(PgBit, buf)

suite "expectedElemCount strictness":
  test "expectedElemCount rejects zero-sized dimension":
    expect PgError:
      discard expectedElemCount(@[2'i32, 0])
    expect PgError:
      discard expectedElemCount(@[0'i32])
