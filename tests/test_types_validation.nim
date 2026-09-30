import std/[json, unittest, options, strutils, tables, times, math]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}
import ../async_postgres/pg_types/ranges {.all.}

import types_common

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

  test "decodeNumericBinary rejects out-of-range base-10000 digit":
    # ndigits = 1, digit = 10000 (> 9999) — violates PgNumeric.digits invariant.
    var data: seq[byte] = @[
      0x00'u8,
      0x01, # ndigits = 1
      0x00,
      0x00, # weight
      0x00,
      0x00, # sign = positive
      0x00,
      0x00, # dscale
      0x27,
      0x10, # digit = 10000
    ]
    expect PgTypeError:
      discard decodeNumericBinary(data)

  test "decodeNumericBinary rejects negative base-10000 digit":
    # digit high bit set -> parses as negative int16.
    var data: seq[byte] = @[
      0x00'u8,
      0x01, # ndigits = 1
      0x00,
      0x00, # weight
      0x00,
      0x00, # sign = positive
      0x00,
      0x00, # dscale
      0xFF,
      0xFF, # digit = -1
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

  test "decodeMultirangeBinaryRaw rejects count exceeding data":
    var data: seq[byte] = @[
      0x7F'u8, 0xFF, 0xFF, 0xFF # count = 2147483647 but no range data
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

suite "Text-path parse errors raise PgTypeError (not ValueError)":
  # REVIEW A.1: text-format accessors must convert the standard ``ValueError``
  # raised by the throwing ``parseInt``/``parseFloat``/``parseBiggestInt``/
  # ``parseHexInt``/``parseIpAddress``/``parseEnum`` into the library's
  # ``PgTypeError`` so callers can rely on a single ``except PgError`` clause.
  test "getIntArray non-numeric element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1,abc}"))]).getIntArray(0)

  test "getInt16Array non-numeric element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1,abc}"))]).getInt16Array(0)

  test "getInt64Array non-numeric element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1,abc}"))]).getInt64Array(0)

  test "getFloatArray non-numeric element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1.0,abc}"))]).getFloatArray(0)

  test "getFloat32Array non-numeric element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1.0,abc}"))]).getFloat32Array(0)

  test "getIntArrayElemOpt non-numeric element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1,abc}"))]).getIntArrayElemOpt(0)

  test "getInt4Range non-numeric bound":
    expect PgTypeError:
      discard (Row @[some(toBytes("[1,abc)"))]).getInt4Range(0)

  test "getInt4Range out-of-range bound (no silent truncation)":
    expect PgTypeError:
      discard (Row @[some(toBytes("[1,2147483648)"))]).getInt4Range(0)

  test "getInt8Range non-numeric bound":
    expect PgTypeError:
      discard (Row @[some(toBytes("[1,abc)"))]).getInt8Range(0)

  test "getBytes invalid hex":
    expect PgTypeError:
      discard (Row @[some(toBytes("\\xZZ"))]).getBytes(0)

  test "getBytes odd-length hex":
    expect PgTypeError:
      discard (Row @[some(toBytes("\\xABC"))]).getBytes(0)

  test "getBytesArray odd-length hex element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{\\xABC}"))]).getBytesArray(0)

  test "getBytesArray escape-format element":
    # Text-format array element without \x prefix must go through the
    # escape decoder, not raw passthrough.
    let arr = (Row @[some(toBytes("{\"\\\\000A\"}"))]).getBytesArray(0)
    check arr.len == 1
    check arr[0] == @[0x00'u8, 0x41'u8]

  test "getInet invalid mask":
    expect PgTypeError:
      discard (Row @[some(toBytes("10.0.0.0/xx"))]).getInet(0)

  test "getInet invalid address":
    expect PgTypeError:
      discard (Row @[some(toBytes("999.999.999.999/8"))]).getInet(0)

  # Out-of-range prefix lengths must raise, not silently wrap through
  # ``uint8(parseInt(...))`` (``/300`` -> 44, ``/-5`` -> 251). IPv6 caps at 128.
  test "getInet out-of-range mask (no silent wrap)":
    expect PgTypeError:
      discard (Row @[some(toBytes("10.0.0.0/300"))]).getInet(0)
    expect PgTypeError:
      discard (Row @[some(toBytes("10.0.0.0/-5"))]).getInet(0)
    expect PgTypeError:
      discard (Row @[some(toBytes("2001:db8::/200"))]).getInet(0)

  test "getPoint non-numeric coordinate":
    expect PgTypeError:
      discard (Row @[some(toBytes("(a,b)"))]).getPoint(0)

  test "getComposite non-numeric field":
    expect PgTypeError:
      discard getComposite[PointRecord](Row @[some(toBytes("(a,b)"))], 0)

  test "getTime non-numeric field":
    expect PgTypeError:
      discard (Row @[some(toBytes("aa:bb:cc"))]).getTime(0)

  test "getTimeTz non-numeric offset":
    expect PgTypeError:
      discard (Row @[some(toBytes("12:00:00+ab"))]).getTimeTz(0)

  # Out-of-range / overflow bounds must raise PgTypeError too, not silently
  # wrap (int) or collapse to inf (float32) — see pgParseInt16/pgParseBiggestInt
  # /pgParseFloat32 in core.nim.
  test "getInt16Array out-of-range element (no silent truncation)":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1,32768}"))]).getInt16Array(0)

  test "getInt64Array out-of-range element":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1,9223372036854775808}"))]).getInt64Array(0)

  test "getFloat32Array overflow element (no silent inf)":
    expect PgTypeError:
      discard (Row @[some(toBytes("{1.0,1e40}"))]).getFloat32Array(0)

  test "getFloat32 scalar overflow (no silent inf)":
    expect PgTypeError:
      discard (Row @[some(toBytes("1e40"))]).getFloat32(0)

suite "Float text path accepts PostgreSQL Infinity/-Infinity/NaN":
  # PostgreSQL prints float infinities as the full word ``Infinity``/``-Infinity``
  # (and NaN as ``NaN``). Nim's ``parseFloat`` only takes the ``inf`` form, so the
  # text path must special-case the full word to stay symmetric with the binary
  # path. NaN is already parsed natively.
  test "getFloat scalar Infinity/-Infinity":
    check (Row @[some(toBytes("Infinity"))]).getFloat(0) == Inf
    check (Row @[some(toBytes("-Infinity"))]).getFloat(0) == NegInf

  test "getFloat32 scalar Infinity/-Infinity":
    check (Row @[some(toBytes("Infinity"))]).getFloat32(0) == float32(Inf)
    check (Row @[some(toBytes("-Infinity"))]).getFloat32(0) == float32(NegInf)

  test "getFloat scalar NaN":
    check (Row @[some(toBytes("NaN"))]).getFloat(0).classify == fcNan

  test "getFloat32 scalar NaN":
    check (Row @[some(toBytes("NaN"))]).getFloat32(0).classify == fcNan

  test "getFloatArray Infinity/-Infinity/NaN":
    let v = (Row @[some(toBytes("{Infinity,-Infinity,NaN,1.5}"))]).getFloatArray(0)
    check v[0] == Inf
    check v[1] == NegInf
    check v[2].classify == fcNan
    check v[3] == 1.5

  test "getFloat32Array Infinity (genuine, not a false overflow reject)":
    let v = (Row @[some(toBytes("{Infinity,-Infinity}"))]).getFloat32Array(0)
    check v[0] == float32(Inf)
    check v[1] == float32(NegInf)

suite "Negative column index":
  # isBinaryCol/colTypeOid guard `col >= 0` themselves: many accessors call them
  # before their own cellInfo/isNull bounds check, so without the guard a
  # negative col reaches `colFormats[col]` (raw IndexDefect, or an out-of-bounds
  # read once --checks:off elides the subscript check). cellInfo/isNull now
  # raise `PgTypeError` (catchable via `except PgError`) instead of IndexDefect.
  let binRow = mkRow(@[some(toBytes("x"))], @[mkField(OidText, 1'i16)])
  let textRow = mkRow(@[some(toBytes("x"))], @[mkField(OidText, 0'i16)])
  # colFormats/colTypeOids are empty here, so a negative subscript would hit a
  # nil seq rather than merely reading before the start of a live allocation.
  let bareRow = Row @[some(toBytes("x"))]

  test "isBinaryCol returns false instead of subscripting":
    for row in [binRow, textRow, bareRow]:
      check not row.isBinaryCol(-1)
      check not row.isBinaryCol(-8)
      check not row.isBinaryCol(int.low)

  test "colTypeOid returns 0 instead of subscripting":
    for row in [binRow, textRow, bareRow]:
      check row.colTypeOid(-1) == 0'i32
      check row.colTypeOid(int.low) == 0'i32

  test "isBinaryCol still reports the format for valid columns":
    check binRow.isBinaryCol(0)
    check not textRow.isBinaryCol(0)
    check binRow.colTypeOid(0) == OidText

  test "isBinaryCol-first accessors report the column index":
    # These call isBinaryCol before cellInfo; the guard lets them fall through
    # to the bounds check that produces the intended message.
    expect PgTypeError:
      discard binRow.getUuid(-1)
    expect PgTypeError:
      discard binRow.getDate(-1)
    expect PgTypeError:
      discard binRow.getNumeric(-1)
    expect PgTypeError:
      discard binRow.getPoint(-1)
    expect PgTypeError:
      discard binRow.getIntArray(-1)
    expect PgTypeError:
      discard binRow.getInt4Range(-1)

  test "cellInfo-first accessors are unchanged":
    expect PgTypeError:
      discard binRow.getStr(-1)
    expect PgTypeError:
      discard binRow.getInt(-1)

  test "getArrayND reports the index, not a format mismatch":
    expect PgTypeError:
      discard getArrayND[int32](binRow, -1)
    expect PgTypeError:
      discard binRow.getMoneyArrayND(-1)

  test "negative index message names the column":
    try:
      discard binRow.getUuid(-1)
      check false
    except PgTypeError as e:
      check "column index -1" in e.msg

  test "column index errors are catchable via `except PgError`":
    # Contract: `except PgError` catches every accessor-layer failure,
    # including out-of-range column access. This suppresses the whole
    # `raises: []` × Defect mismatch in one place for library users.
    for row in [binRow, textRow, bareRow]:
      expect PgError:
        discard row.getStr(-1)
      expect PgError:
        discard row.getStr(row.len + 5)
      expect PgError:
        discard row.isNull(-1)
      expect PgError:
        discard row.isNull(row.len + 5)

suite "Text array encoders bound the literal they build":
  # The guards are per element, so an oversized array is rejected while the
  # literal is still bounded. These check the guard did not alter the output.
  test "range array literal is unchanged":
    let p = toPgParam(@[rangeOf(1'i32, 10'i32), rangeOf(20'i32, 30'i32)])
    check p.oid == OidInt4RangeArray
    check p.format == 0'i16
    check p.value.get.toString == """{"[1,10)","[20,30)"}"""

  test "multirange array literal is unchanged":
    let p = toPgParam(@[toMultirange(rangeOf(1'i32, 10'i32))])
    check p.value.get.toString == """{"{[1,10)}"}"""

  test "multirange scalar literal is unchanged":
    let p = toPgParam(toMultirange(rangeOf(1'i32, 10'i32), rangeOf(20'i32, 30'i32)))
    check p.value.get.toString == "{[1,10),[20,30)}"

  test "enum array literal is unchanged":
    check encodeEnumTextArray(@[some("happy"), none(string), some("sad")]) ==
      """{"happy",NULL,"sad"}"""

  test "oversize is reported as a catchable PgError":
    for what in ["range array", "multirange array", "enum array", "multirange"]:
      expect PgError:
        checkPgBinLen(maxInt32Len + 1, what)

suite "1-D array accessors reject a mismatched wire elemOid":
  test "getIntArray rejects int8[] instead of decoding it as int32":
    # The silent-corruption case: 8-byte elements fed to the int32 decoder.
    let bin = encodeBinaryArray(OidInt8, @[@[0'u8, 0, 0, 0, 0, 0, 0, 42]])
    let row = mkRow(@[some(bin)], @[mkField(OidInt8Array, 1)])
    expect PgTypeError:
      discard row.getIntArray(0)

  test "rejection names the accessor and both OIDs":
    let bin = encodeBinaryArray(OidInt4, @[@[0'u8, 0, 0, 7]])
    let row = mkRow(@[some(bin)], @[mkField(OidInt4Array, 1)])
    var msg = ""
    try:
      discard row.getInt64Array(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "getInt64Array: wire elemOid=23 expected 20"

  test "getTimestampArray rejects timestamptz[]":
    let bin = encodeBinaryArray(OidTimestampTz, @[@[0'u8, 0, 0, 0, 0, 0, 0, 0]])
    let row = mkRow(@[some(bin)], @[mkField(OidTimestampTzArray, 1)])
    expect PgTypeError:
      discard row.getTimestampArray(0)

  test "getMoneyArray rejects int8[]":
    let bin = encodeBinaryArray(OidInt8, @[@[0'u8, 0, 0, 0, 0, 0, 0, 1]])
    let row = mkRow(@[some(bin)], @[mkField(OidInt8Array, 1)])
    expect PgTypeError:
      discard row.getMoneyArray(0)

  test "getBoxArray rejects polygon[]":
    let bin = encodeBinaryArray(OidPolygon, @[newSeq[byte](32)])
    let row = mkRow(@[some(bin)], @[mkField(OidPolygonArray, 1)])
    expect PgTypeError:
      discard row.getBoxArray(0)

  test "the ElemOpt variants check too":
    let bin = encodeBinaryArray(OidInt8, @[@[0'u8, 0, 0, 0, 0, 0, 0, 3]])
    let row = mkRow(@[some(bin)], @[mkField(OidInt8Array, 1)])
    expect PgTypeError:
      discard row.getIntArrayElemOpt(0)

  test "getStrArray accepts every character type PostgreSQL may send":
    for oid in [OidText, OidVarchar, OidBpchar, OidName, OidChar]:
      let bin = encodeBinaryArray(oid, @[toBytes("hi")])
      let row = mkRow(@[some(bin)], @[mkField(OidTextArray, 1)])
      check row.getStrArray(0) == @["hi"]

  test "getBitArray accepts both bit and varbit":
    for oid in [OidBit, OidVarbit]:
      let bin = encodeBinaryArray(oid, @[@[0'u8, 0, 0, 1, 0x80]])
      let row = mkRow(@[some(bin)], @[mkField(OidBitArray, 1)])
      check row.getBitArray(0).len == 1

  test "getHstoreArray is exempt: hstore's OID is assigned by the extension":
    # Binary hstore: int32 pair count, then int32-prefixed key/value pairs.
    let payload = @[0'u8, 0, 0, 1, 0, 0, 0, 1, byte('k'), 0, 0, 0, 1, byte('v')]
    let bin = encodeBinaryArray(90123'i32, @[payload])
    let row = mkRow(@[some(bin)], @[mkField(90124'i32, 1)])
    check row.getHstoreArray(0).len == 1

  test "the text format path is unaffected":
    let row = mkRow(@[some(toBytes("{1,2,3}"))], @[mkField(OidInt4Array, 0)])
    check row.getIntArray(0) == @[1'i32, 2, 3]

suite "scalar accessors reject a mismatched binary column OID":
  test "getFloat32 rejects int4 instead of decoding it as 1e-45":
    let row = mkRow(@[some(@(toBE32(1'i32)))], @[mkField(OidInt4, 1)])
    expect PgTypeError:
      discard row.getFloat32(0)

  test "rejection names the accessor and both OIDs":
    let row = mkRow(@[some(@(toBE32(1'i32)))], @[mkField(OidInt4, 1)])
    var msg = ""
    try:
      discard row.getFloat32(0)
    except PgTypeError as e:
      msg = e.msg
    check msg ==
      "getFloat32: wire colOid=23 expected 700 (binary column type mismatch; use the matching accessor or resultFormat = rfText)"

  test "getInt accepts int2 and int4 widening":
    check mkRow(@[some(@(toBE16(7'i16)))], @[mkField(OidInt2, 1)]).getInt(0) == 7'i32
    check mkRow(@[some(@(toBE32(7'i32)))], @[mkField(OidInt4, 1)]).getInt(0) == 7'i32

  test "getInt64 accepts int2, int4, and int8":
    check mkRow(@[some(@(toBE16(7'i16)))], @[mkField(OidInt2, 1)]).getInt64(0) == 7'i64
    check mkRow(@[some(@(toBE32(7'i32)))], @[mkField(OidInt4, 1)]).getInt64(0) == 7'i64
    check mkRow(@[some(@(toBE64(7'i64)))], @[mkField(OidInt8, 1)]).getInt64(0) == 7'i64

  test "getFloat accepts float4 and float8":
    let row4 = mkRow(@[some(@(toBE32(cast[int32](1.5'f32))))], @[mkField(OidFloat4, 1)])
    check row4.getFloat(0) == 1.5'f64
    let row8 = mkRow(@[some(@(toBE64(cast[int64](1.5'f64))))], @[mkField(OidFloat8, 1)])
    check row8.getFloat(0) == 1.5'f64

  test "getTimestamp rejects timestamptz":
    let row = mkRow(@[some(newSeq[byte](8))], @[mkField(OidTimestampTz, 1)])
    expect PgTypeError:
      discard row.getTimestamp(0)

  test "getInet rejects cidr and vice versa":
    var payload = newSeq[byte](8)
    payload[0] = 2
    payload[1] = 32
    payload[2] = 0
    payload[3] = 4
    let rowInet = mkRow(@[some(payload)], @[mkField(OidInet, 1)])
    expect PgTypeError:
      discard rowInet.getCidr(0)
    let rowCidr = mkRow(@[some(payload)], @[mkField(OidCidr, 1)])
    expect PgTypeError:
      discard rowCidr.getInet(0)

  test "getJson accepts json and jsonb":
    let rowJson = mkRow(@[some(toBytes("{}"))], @[mkField(OidJson, 1)])
    check $rowJson.getJson(0) == "{}"
    var jbin: seq[byte] = @[1'u8]
    for c in "{}":
      jbin.add(byte(c))
    let rowJsonb = mkRow(@[some(jbin)], @[mkField(OidJsonb, 1)])
    check $rowJsonb.getJson(0) == "{}"

  test "getJson rejects non-json binary":
    let row = mkRow(@[some(@(toBE32(1'i32)))], @[mkField(OidInt4, 1)])
    expect PgTypeError:
      discard row.getJson(0)

  test "getBit accepts bit and varbit":
    for oid in [OidBit, OidVarbit]:
      let row = mkRow(@[some(@[0'u8, 0, 0, 1, 0x80])], @[mkField(oid, 1)])
      check $row.getBit(0) == "1"

  test "missing colTypeOids skips the check (manual Row)":
    let rd = RowData(
      numCols: 1'i16,
      buf: @(toBE32(1'i32)),
      cellIndex: @[0'i32, 4'i32],
      colFormats: @[1'i16],
      colTypeOids: @[],
    )
    # Fail-open only when OID metadata is absent, not for an explicit wire 0.
    discard initRow(rd, 0).getFloat32(0)

  test "explicit OID 0 is rejected (hostile/unknown type)":
    let rd = RowData(
      numCols: 1'i16,
      buf: @(toBE32(1'i32)),
      cellIndex: @[0'i32, 4'i32],
      colFormats: @[1'i16],
      colTypeOids: @[0'i32],
    )
    var msg = ""
    try:
      discard initRow(rd, 0).getFloat32(0)
    except PgTypeError as e:
      msg = e.msg
    check "unknown" in msg
    check "colOid=0" in msg

  test "the text format path is unaffected":
    let row = mkRow(@[some(toBytes("1"))], @[mkField(OidInt4, 0)])
    check row.getFloat32(0) == 1'f32

suite "range array accessors reject a mismatched wire elemOid":
  test "getDateRangeArray rejects int4range[]":
    let p = toPgBinaryParam(@[rangeOf(1'i32, 10'i32)])
    let row = mkRow(@[p.value], @[mkField(OidInt4RangeArray, 1)])
    var msg = ""
    try:
      discard row.getDateRangeArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "getDateRangeArray: wire elemOid=3904 expected 3912"

  test "getInt4RangeArray rejects daterange[]":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgBinaryDateRangeArrayParam(@[rangeOf(dt1, dt2)])
    let row = mkRow(@[p.value], @[mkField(OidDateRangeArray, 1)])
    expect PgTypeError:
      discard row.getInt4RangeArray(0)

  test "getTsRangeArray rejects int8range[]":
    let p = toPgBinaryParam(@[rangeOf(100'i64, 200'i64)])
    let row = mkRow(@[p.value], @[mkField(OidInt8RangeArray, 1)])
    expect PgTypeError:
      discard row.getTsRangeArray(0)

  test "getTsTzRangeArray rejects tsrange[]":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let p = toPgBinaryParam(@[rangeOf(dt1, dt2)])
    let row = mkRow(@[p.value], @[mkField(OidTsRangeArray, 1)])
    expect PgTypeError:
      discard row.getTsTzRangeArray(0)

  test "the text format path is unaffected":
    let row = mkRow(@[some(toBytes("{\"[1,10)\"}"))], @[mkField(OidInt4RangeArray, 0)])
    check row.getInt4RangeArray(0) == @[rangeOf(1'i32, 10'i32)]

suite "scalar range accessors reject a mismatched wire colOid":
  test "getDateRange rejects int4range binary":
    let p = toPgBinaryParam(rangeOf(1'i32, 10'i32))
    let row = mkRow(@[p.value], @[mkField(OidInt4Range, 1)])
    var msg = ""
    try:
      discard row.getDateRange(0)
    except PgTypeError as e:
      msg = e.msg
    check "getDateRange: wire colOid=3904 expected 3912" in msg

  test "getInt4Range rejects daterange binary":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgBinaryDateRangeParam(rangeOf(dt1, dt2))
    let row = mkRow(@[p.value], @[mkField(OidDateRange, 1)])
    expect PgTypeError:
      discard row.getInt4Range(0)

  test "getInt4Multirange rejects datemultirange binary":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgBinaryDateMultirangeParam(toMultirange(rangeOf(dt1, dt2)))
    let row = mkRow(@[p.value], @[mkField(OidDateMultirange, 1)])
    expect PgTypeError:
      discard row.getInt4Multirange(0)

suite "multirange array accessors reject a mismatched wire elemOid":
  test "getDateMultirangeArray rejects int4multirange[]":
    let p = toPgBinaryParam(@[toMultirange(rangeOf(1'i32, 3'i32))])
    let row = mkRow(@[p.value], @[mkField(OidInt4MultirangeArray, 1)])
    expect PgTypeError:
      discard row.getDateMultirangeArray(0)

  test "getInt4MultirangeArray rejects datemultirange[]":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgBinaryDateMultirangeArrayParam(@[toMultirange(rangeOf(dt1, dt2))])
    let row = mkRow(@[p.value], @[mkField(OidDateMultirangeArray, 1)])
    expect PgTypeError:
      discard row.getInt4MultirangeArray(0)

  test "getTsTzMultirangeArray rejects tsmultirange[]":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let p = toPgBinaryParam(@[toMultirange(rangeOf(dt1, dt2))])
    let row = mkRow(@[p.value], @[mkField(OidTsMultirangeArray, 1)])
    expect PgTypeError:
      discard row.getTsTzMultirangeArray(0)

suite "getArrayND accepts the 1-D character and bit OIDs":
  test "getArrayND[string] accepts varchar[]":
    let bin = encodeBinaryArray(OidVarchar, @[toBytes("hi")])
    let row = mkRow(@[some(bin)], @[mkField(OidVarcharArray, 1)])
    check getArrayND[string](row, 0).elements == @[some("hi")]

  test "getArrayND[PgBit] accepts bit[]":
    let bin = encodeBinaryArray(OidBit, @[@[0'u8, 0, 0, 1, 0x80]])
    let row = mkRow(@[some(bin)], @[mkField(OidBitArray, 1)])
    check getArrayND[PgBit](row, 0).elements.len == 1

suite "type-decode failures omit cell content":
  test "getJson failure reports length, not content":
    # Cell values may hold PII/secrets and exception text lands in
    # logs, so decode failures must not echo the cell.
    const secret = "SECRET_JSON_PAYLOAD_XYZ"
    let row =
      mkRow(@[some(toBytes("{not json " & secret & "}"))], @[mkField(OidJson, 0)])
    var msg = ""
    try:
      discard row.getJson(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getBytesArray failure omits the cell":
    # A malformed hex element is rejected by decodeHexPair, which must report
    # position/len only: bytea cells may hold PII or secrets.
    const secret = "PII_SECRET_XYZ"
    let row = Row @[some(toBytes("{\"\\\\x4142ZZ" & secret & "\"}"))]
    var msg = ""
    try:
      discard row.getBytesArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "at position" in msg
    check "len=" in msg

  test "getJsonArray failure omits the element":
    const secret = "oops-SECRET-JSONARR-XYZ"
    let row: Row = @[some(toBytes("{" & secret & "}"))]
    var msg = ""
    try:
      discard row.getJsonArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg

  test "getLineArray failure omits the element":
    const secret = "oops-SECRET-LINEARR-XYZ"
    let row: Row = @[some(toBytes("{" & secret & "}"))]
    var msg = ""
    try:
      discard row.getLineArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg

  test "getLsegArray failure omits the element":
    const secret = "oops-SECRET-LSEGARR-XYZ"
    let row: Row = @[some(toBytes("{" & secret & "}"))]
    var msg = ""
    try:
      discard row.getLsegArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg

  test "getBoxArray failure omits the element":
    const secret = "oops-SECRET-BOXARR-XYZ"
    let row = mkRow(@[some(toBytes("{" & secret & "}"))], @[mkField(OidBox, 0)])
    var msg = ""
    try:
      discard row.getBoxArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg

  test "getPathArray failure omits the element":
    const secret = "oops-SECRET-PATHARR-XYZ"
    let row: Row = @[some(toBytes("{" & secret & "}"))]
    var msg = ""
    try:
      discard row.getPathArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg

  test "getPolygonArray failure omits the element":
    const secret = "oops-SECRET-POLYARR-XYZ"
    let row: Row = @[some(toBytes("{" & secret & "}"))]
    var msg = ""
    try:
      discard row.getPolygonArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg

  test "getCircleArray failure omits the element":
    const secret = "oops-SECRET-CIRCARR-XYZ"
    let row: Row = @[some(toBytes("{" & secret & "}"))]
    var msg = ""
    try:
      discard row.getCircleArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg

  test "parsePgNumeric failure omits input":
    const bad = "12a34SECRET_NUM_XYZ"
    var msg = ""
    try:
      discard parsePgNumeric(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid numeric (len=" & $bad.len & ")"
    check "SECRET_NUM_XYZ" notin msg

  test "parsePgBoolText failure omits input":
    const bad = "maybeSECRET_BOOL_XYZ"
    var msg = ""
    try:
      discard parsePgBoolText(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid boolean value (len=" & $bad.len & ")"
    check "SECRET_BOOL_XYZ" notin msg

  test "getPoint failure omits input":
    const secret = "SECRET_GEO_XYZ"
    let row = mkRow(@[some(toBytes("(oops " & secret & ")"))], @[mkField(OidPoint, 0)])
    var msg = ""
    try:
      discard row.getPoint(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getEnum failure omits the label, including the ValueError detail":
    # The stdlib parseEnum error echoes the label; pgParseEnum must drop that
    # detail instead of appending it.
    const secret = "bogus-SECRET-ENUM-XYZ"
    let row: Row = @[some(toBytes(secret))]
    var msg = ""
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Mood" in msg
    check "Invalid enum value: " notin msg

  test "getInt out-of-range failure omits the value":
    let row = mkRow(@[some(toBytes("5000000000"))], @[mkField(OidInt4, 0)])
    var msg = ""
    try:
      discard row.getInt(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0: integer value out of int32 range (len=10)"

  test "getInt16 out-of-range failure omits the value":
    let row = mkRow(@[some(toBytes("40000"))], @[mkField(OidInt2, 0)])
    var msg = ""
    try:
      discard row.getInt16(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0: integer value out of int16 range (len=5)"

  test "getInt overflow failure omits the cell":
    # Beyond int64: parseInt itself raises, and its message echoes the cell,
    # so the overflow path must not append it either.
    const big = "1234567890123456789012345"
    let row = mkRow(@[some(toBytes(big))], @[mkField(OidInt4, 0)])
    var msg = ""
    try:
      discard row.getInt(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0: integer value out of range (len=" & $big.len & ")"
    check big notin msg

  test "getInt64 overflow failure omits the cell":
    const big = "1234567890123456789012345"
    let row = mkRow(@[some(toBytes(big))], @[mkField(OidInt8, 0)])
    var msg = ""
    try:
      discard row.getInt64(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0: integer value out of range (len=" & $big.len & ")"
    check big notin msg

  test "getLine failure omits input and names the column":
    const secret = "oops-SECRET-LINE-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidLine, 0)])
    var msg = ""
    try:
      discard row.getLine(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "parseIntervalText failure omits input":
    const bad = "nonsense-SECRET-INTV-XYZ"
    var msg = ""
    try:
      discard parseIntervalText(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid interval (len=" & $bad.len & ")"
    check "SECRET-INTV-XYZ" notin msg

  test "parseRangeText failure omits input":
    const bad = "SECRET-RANGE-XYZ"
    var msg = ""
    try:
      discard parseRangeText[int32](
        bad,
        proc(s: string): int32 =
          int32(parseInt(s)),
      )
    except PgTypeError as e:
      msg = e.msg
    check msg == "range: invalid lower boundary (len=" & $bad.len & ")"
    check bad notin msg

  test "parseMultirangeText failure omits input":
    const bad = "SECRET-MR-XYZ"
    var msg = ""
    try:
      discard parseMultirangeText[int32](
        bad,
        proc(s: string): int32 =
          int32(parseInt(s)),
      )
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid multirange literal (len=" & $bad.len & ")"
    check bad notin msg

  test "parseCompositeText failure omits input":
    const bad = "SECRET-COMP-XYZ"
    var msg = ""
    try:
      discard parseCompositeText(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid composite literal (len=" & $bad.len & ")"
    check bad notin msg

  test "getBox failure omits input and names the column":
    const secret = "oops-SECRET-BOX-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidBox, 0)])
    var msg = ""
    try:
      discard row.getBox(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getPath failure omits input and names the column":
    const secret = "oops-SECRET-PATH-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidPath, 0)])
    var msg = ""
    try:
      discard row.getPath(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getPolygon failure omits input and names the column":
    const secret = "oops-SECRET-POLY-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidPolygon, 0)])
    var msg = ""
    try:
      discard row.getPolygon(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getCircle failure omits input and names the column":
    const secret = "oops-SECRET-CIRCLE-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidCircle, 0)])
    var msg = ""
    try:
      discard row.getCircle(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getInterval failure omits input and names the column":
    const secret = "nonsense-SECRET-INTV-ROW-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidInterval, 0)])
    var msg = ""
    try:
      discard row.getInterval(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getLseg failure omits input and names the column":
    const secret = "oops-SECRET-LSEG-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidLseg, 0)])
    var msg = ""
    try:
      discard row.getLseg(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getTimestamp failure omits input and names the column":
    const secret = "not-a-time-SECRET-TS-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidTimestamp, 0)])
    var msg = ""
    try:
      discard row.getTimestamp(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getDate failure omits input and names the column":
    const secret = "not-a-date-SECRET-DATE-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidDate, 0)])
    var msg = ""
    try:
      discard row.getDate(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getTime failure omits input and names the column":
    const secret = "not-a-time-SECRET-TIME-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidTime, 0)])
    var msg = ""
    try:
      discard row.getTime(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getTimeTz failure omits input and names the column":
    const secret = "not-a-timetz-SECRET-TIMETZ-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidTimeTz, 0)])
    var msg = ""
    try:
      discard row.getTimeTz(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getTimestampTz failure omits input and names the column":
    const secret = "not-a-timestamptz-SECRET-TSTZ-XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidTimestampTz, 0)])
    var msg = ""
    try:
      discard row.getTimestampTz(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg
    check "len=" in msg
    check "Column 0" in msg

  test "getEnum on NULL names the column once":
    let row: Row = @[none(seq[byte])]
    var msg = ""
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0 is NULL"

suite "ValueError-detail paths omit cell content":
  # `pgTypeErrorOnValueError` drops the stdlib `ValueError` detail instead of
  # appending it, so these branches report the context only. Exception text
  # lands in logs while cell values may hold PII/secrets.
  test "pgParseInt failure omits input":
    const bad = "12x34SECRET_INT_XYZ"
    var msg = ""
    try:
      discard pgParseInt(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "invalid integer value"
    check "SECRET_INT_XYZ" notin msg

  test "pgParseBiggestInt failure omits input":
    const bad = "99y99SECRET_BIG_XYZ"
    var msg = ""
    try:
      discard pgParseBiggestInt(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "invalid integer value"
    check "SECRET_BIG_XYZ" notin msg

  test "pgParseHexInt failure omits input":
    const bad = "ZZSECRET_HEX_XYZ"
    var msg = ""
    try:
      discard pgParseHexInt(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "invalid hex value"
    check "SECRET_HEX_XYZ" notin msg

  test "parseTimeText frac failure omits the fraction":
    const bad = "01:02:03.SECRET"
    var msg = ""
    try:
      discard parseTimeText(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid time (len=" & $bad.len & ")"
    check "SECRET" notin msg

  test "parseTimeText hour failure omits the slice":
    const bad = "AB:CD:EF"
    var msg = ""
    try:
      discard parseTimeText(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid time (len=" & $bad.len & ")"
    check "AB" notin msg

  test "parseTimeTzText bad offset omits the offset":
    const bad = "01:02:03+99999999999999999999"
    var msg = ""
    try:
      discard parseTimeTzText(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Invalid timetz offset (len=" & $bad.len & ")"
    check "99999999999999999999" notin msg

  test "parseInetText bad mask omits the mask":
    const bad = "1.2.3.4/SECRET_MASK_XYZ"
    var msg = ""
    try:
      discard parseInetText(bad)
    except PgTypeError as e:
      msg = e.msg
    check msg == "invalid inet value (len=" & $bad.len & ")"
    check "SECRET_MASK_XYZ" notin msg

  test "getInet bad mask omits the mask and names the column":
    const secret = "1.2.3.4/SECRET_INET_XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidInet, 0)])
    var msg = ""
    try:
      discard row.getInet(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0: invalid inet value (len=" & $secret.len & ")"
    check "SECRET_INET_XYZ" notin msg

  test "getCidr bad mask omits the mask and names the column":
    const secret = "1.2.3.4/SECRET_CIDR_XYZ"
    let row = mkRow(@[some(toBytes(secret))], @[mkField(OidCidr, 0)])
    var msg = ""
    try:
      discard row.getCidr(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0: invalid inet value (len=" & $secret.len & ")"
    check "SECRET_CIDR_XYZ" notin msg

  test "parseRangeText element failure omits the bound":
    const secret = "SECRET_RANGE_ELEM_XYZ"
    var msg = ""
    try:
      discard parseRangeText[int32]("[" & secret & ",10)", pgParseInt32)
    except PgTypeError as e:
      msg = e.msg
    check msg.len > 0
    check secret notin msg

  test "getPoint on NULL names the column once":
    let row = mkRow(@[none(seq[byte])], @[mkField(OidPoint, 0)])
    var msg = ""
    try:
      discard row.getPoint(0)
    except PgTypeError as e:
      msg = e.msg
    check msg == "Column 0 is NULL"

suite "wire OID policy: a binary read needs an exact built-in match":
  const
    userRangeOid = 20001'i32
      ## A range type from ``CREATE TYPE ... AS RANGE``: the catalog hands it a
      ## dynamic OID, so no built-in constant can name it.
    userDomainOid = 20002'i32
    userEnumOid = 20003'i32

  test "a range getter rejects a user-defined range type in binary":
    let p = toPgBinaryParam(rangeOf(1'i32, 10'i32))
    let row = mkRow(@[p.value], @[mkField(userRangeOid, 1)])
    var msg = ""
    try:
      discard row.getInt4Range(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.startsWith("getInt4Range: wire colOid=20001 is a user-defined type")
    check "resultFormat = rfText" in msg

  test "a range getter still rejects a mismatched built-in range":
    let p = toPgBinaryParam(rangeOf(1'i32, 10'i32))
    let row = mkRow(@[p.value], @[mkField(OidInt4Range, 1)])
    expect PgTypeError:
      discard row.getDateRange(0)

  test "a scalar getter rejects a domain it cannot distinguish from an enum":
    # int4 and a 4-byte enum label share a length, so a dynamic OID carries no
    # evidence either way; text format stays the escape hatch.
    let p = toPgBinaryParam(42'i32)
    let row = mkRow(@[p.value], @[mkField(userDomainOid, 1)])
    expect PgTypeError:
      discard row.getInt(0)
    let textRow = mkRow(@[some(toBytes("42"))], @[mkField(userDomainOid, 0)])
    check textRow.getInt(0) == 42'i32

  test "a scalar getter does not misread a binary enum label":
    let row = mkRow(@[some(toBytes("warm"))], @[mkField(userEnumOid, 1)])
    expect PgTypeError:
      discard row.getInt(0)
    expect PgTypeError:
      discard row.getFloat32(0)

  test "rfAuto never requests binary for a dynamic OID":
    # Why the strict guard leaves the default path alone: only a forced
    # rfBinary can deliver a catalog-assigned OID in binary format.
    for oid in [userRangeOid, userDomainOid, userEnumOid]:
      check not isBinarySafeOid(oid)

  test "an array getter rejects a dynamic elemOid":
    # Binary array header: ndim, hasnull, elemOid — retarget the element OID.
    var raw = toPgBinaryParam(@[1'i32, 2'i32]).value.get
    for i in 0 .. 3:
      raw[8 + i] = byte((userEnumOid shr (8 * (3 - i))) and 0xff)
    let row = mkRow(@[some(raw)], @[mkField(OidInt4Array, 1)])
    var msg = ""
    try:
      discard row.getIntArray(0)
    except PgTypeError as e:
      msg = e.msg
    check msg.startsWith("getIntArray: wire elemOid=20003 is a user-defined type")

  test "getEnum accepts a dynamic colOid and rejects a stringified built-in":
    let cell = some(toBytes("happy"))
    check getEnum[Mood](mkRow(@[cell], @[mkField(20003'i32, 1)]), 0) == happy
    for oid in [OidInt4, OidBool, OidNumeric, OidInt4Range, OidRecord]:
      var msg = ""
      try:
        discard getEnum[Mood](mkRow(@[cell], @[mkField(oid, 1)]), 0)
      except PgTypeError as e:
        msg = e.msg
      check msg ==
        "getEnum: wire colOid=" & $oid & " is not an enum (binary column type mismatch)"

  test "getEnum rejects an explicit colOid of 0":
    let row = mkRow(@[some(toBytes("happy"))], @[mkField(0'i32, 1)])
    expect PgTypeError:
      discard getEnum[Mood](row, 0)

  test "getEnumArray rejects a built-in elemOid the scalar path also rejects":
    let p = toPgBinaryParam(@[1'i32, 2'i32])
    let row = mkRow(@[p.value], @[mkField(OidInt4Array, 1)])
    var msg = ""
    try:
      discard getEnumArray[Mood](row, 0)
    except PgTypeError as e:
      msg = e.msg
    check msg ==
      "getEnumArray: wire elemOid=" & $OidInt4 &
      " is not an enum (binary column type mismatch)"

  test "getHstore rejects a built-in colOid but takes a dynamic one":
    let p = toPgBinaryParam(42'i32)
    let row = mkRow(@[p.value], @[mkField(OidInt4, 1)])
    expect PgTypeError:
      discard row.getHstore(0)
    # hstore's OID is assigned at CREATE EXTENSION time, so a dynamic
    # (non-built-in) column OID must be accepted on the binary path.
    var h: PgHstore = initTable[string, Option[string]]()
    h["k"] = some("v")
    let dynRow = mkRow(@[some(encodeHstoreBinary(h))], @[mkField(90123'i32, 1)])
    check dynRow.getHstore(0) == h

suite "text parsers follow PostgreSQL's grammar, not Nim's":
  test "getInt rejects digit-group underscores and a leading plus":
    for text in ["1_0", "+5", " 5", "5 "]:
      let row = mkRow(@[some(toBytes(text))], @[mkField(OidInt4, 0)])
      expect PgTypeError:
        discard row.getInt(0)

  test "getFloat rejects digit-group underscores":
    let row = mkRow(@[some(toBytes("1_0.5"))], @[mkField(OidFloat8, 0)])
    expect PgTypeError:
      discard row.getFloat(0)

  test "pgParseHexInt rejects underscores and an 0x prefix":
    for text in ["1_", "0x1f", "g0"]:
      expect PgTypeError:
        discard pgParseHexInt(text)

  test "pgParseHexUInt32 follows the hex grammar, not fromHex":
    check pgParseHexUInt32("0", "ctx") == 0'u32
    check pgParseHexUInt32("FFFFFFFF", "ctx") == 0xFFFF_FFFF'u32
    check pgParseHexUInt32("000000000000000001", "ctx") == 1'u32
    for text in ["", "1_0", "0x10", "#10", "0X10", "g0", "100000000"]:
      expect PgTypeError:
        discard pgParseHexUInt32(text, "ctx")

  test "parseTimeText rejects an underscored component":
    expect PgTypeError:
      discard parseTimeText("1_:30:00")

  test "parseInetText rejects an underscored prefix length":
    expect PgTypeError:
      discard parseInetText("192.168.0.1/1_0")

  test "parseTimeTzText rejects an underscored offset":
    expect PgTypeError:
      discard parseTimeTzText("01:30:00+1_")

  test "pgParseUIntField accepts the full int range without false overflow":
    check pgParseUIntField($int.high, "ctx") == int.high
    check pgParseUIntField("9223372036854775800", "ctx") == 9223372036854775800
    expect PgTypeError:
      discard pgParseUIntField("9223372036854775808", "ctx")
    expect PgTypeError:
      discard pgParseUIntField("99999999999999999999", "ctx")

suite "getBoxArray text format":
  test "parses semicolon-delimited box literals":
    # PostgreSQL's box[] text form uses ';' between elements (not ',') because
    # each box already contains commas. Exercise the dedicated text path.
    let row =
      mkRow(@[some(toBytes("{(3,4),(1,2);(7,8),(5,6)}"))], @[mkField(OidBoxArray, 0)])
    let arr = row.getBoxArray(0)
    check arr.len == 2
    check arr[0].high == PgPoint(x: 3.0, y: 4.0)
    check arr[0].low == PgPoint(x: 1.0, y: 2.0)
    check arr[1].high == PgPoint(x: 7.0, y: 8.0)
    check arr[1].low == PgPoint(x: 5.0, y: 6.0)

  test "empty box array literal":
    let row = mkRow(@[some(toBytes("{}"))], @[mkField(OidBoxArray, 0)])
    check row.getBoxArray(0).len == 0

  test "rejects NULL elements in text form":
    let row = mkRow(@[some(toBytes("{(1,2),(3,4);NULL}"))], @[mkField(OidBoxArray, 0)])
    expect PgTypeError:
      discard row.getBoxArray(0)

suite "row columnIndex by name":
  test "resolves names from row field metadata":
    let fields = @[
      FieldDescription(
        name: "id", typeOid: OidInt4, typeSize: 4, typeMod: -1, formatCode: 0
      ),
      FieldDescription(
        name: "name", typeOid: OidText, typeSize: -1, typeMod: -1, formatCode: 0
      ),
    ]
    let row = mkRow(@[some(toBytes("1")), some(toBytes("alice"))], fields)
    check row.columnIndex("id") == 0
    check row.columnIndex("name") == 1
    check row.getStr("name") == "alice"

  test "raises when field metadata is missing":
    # Manual Row without FieldDescription metadata (converter path).
    let row: Row = @[some(toBytes("x"))]
    var raised = false
    try:
      discard row.columnIndex("x")
    except PgTypeError as e:
      raised = true
      check "field metadata" in e.msg
    check raised

  test "raises when the column name is absent":
    let fields = @[
      FieldDescription(
        name: "id", typeOid: OidInt4, typeSize: 4, typeMod: -1, formatCode: 0
      )
    ]
    let row = mkRow(@[some(toBytes("1"))], fields)
    var raised = false
    try:
      discard row.columnIndex("missing")
    except PgTypeError as e:
      raised = true
      check "Column not found" in e.msg
    check raised

template fixedZone(name: string, west: static int): Timezone =
  ## A zone `west` seconds west of UTC (Nim's utcOffset sign).
  proc fromTime(time: Time): ZonedTime {.gensym, nimcall, gcsafe, raises: [].} =
    ZonedTime(isDst: false, utcOffset: west, time: time)

  proc fromAdj(adjTime: Time): ZonedTime {.gensym, nimcall, gcsafe, raises: [].} =
    ZonedTime(
      isDst: false, utcOffset: west, time: adjTime + initDuration(seconds = west)
    )

  newTimezone(name, fromTime, fromAdj)

suite "Out-of-contract inputs raise PgTypeError, not a Defect":
  # A Defect escapes `except PgError` / `except CatchableError`, so each of these
  # would take the process down instead of failing the one call.
  let unset = default(DateTime)

  test "uninitialized DateTime in every scalar and array encoder":
    expect PgTypeError:
      discard toPgParam(unset)
    expect PgTypeError:
      discard toPgDateParam(unset)
    expect PgTypeError:
      discard toPgTimestampTzParam(unset)
    expect PgTypeError:
      discard toPgBinaryParam(unset)
    expect PgTypeError:
      discard toPgBinaryDateParam(unset)
    expect PgTypeError:
      discard toPgBinaryTimestampTzParam(unset)
    expect PgTypeError:
      discard toPgParam(some(unset))
    expect PgTypeError:
      discard toPgBinaryParam(some(unset))
    expect PgTypeError:
      discard toPgTimestampArrayParam(@[unset])
    expect PgTypeError:
      discard toPgTimestampTzArrayParam(@[unset])
    expect PgTypeError:
      discard toPgDateArrayParam(@[unset])

  test "uninitialized DateTime in range and multirange encoders":
    let dt = dateTime(2024, mJan, 15, zone = utc())
    for r in [rangeOf(unset, dt), rangeOf(dt, unset)]:
      expect PgTypeError:
        discard toPgParam(r)
      expect PgTypeError:
        discard toPgTsTzRangeParam(r)
      expect PgTypeError:
        discard toPgDateRangeParam(r)
      expect PgTypeError:
        discard toPgBinaryParam(r)
      expect PgTypeError:
        discard toPgBinaryDateRangeParam(r)
      expect PgTypeError:
        discard toPgParam(toMultirange(r))
      expect PgTypeError:
        discard toPgBinaryParam(toMultirange(r))

  test "binary timestamp accepts exactly PostgreSQL's range":
    let first = dateTime(-4713, mNov, 24, zone = utc())
    let last = dateTime(294276, mDec, 31, 23, 59, 59, 999_999_000, utc())
    check pgTimestampMicros(first) == -211813488000000000'i64
    check pgTimestampMicros(last) == 9223371331199999999'i64
    # Past the end the scaling used to overflow int64 (OverflowDefect).
    for dt in [
      first - initDuration(microseconds = 1),
      last + initDuration(microseconds = 1),
      dateTime(300000, mJan, 1, zone = utc()),
    ]:
      expect PgTypeError:
        discard toPgBinaryParam(dt)
      expect PgTypeError:
        discard toPgBinaryTimestampTzParam(dt)
      expect PgTypeError:
        discard toPgTimestampArrayParam(@[dt])

  test "binary date accepts exactly PostgreSQL's range":
    let first = dateTime(-4713, mNov, 24, zone = utc())
    let last = dateTime(5874897, mDec, 31, zone = utc())
    check pgDateDays(first) == -2451545'i32
    check pgDateDays(last) == 2145031948'i32
    for dt in [first - initDuration(days = 1), last + initDuration(days = 1)]:
      expect PgTypeError:
        discard toPgBinaryDateParam(dt)
      expect PgTypeError:
        discard toPgDateArrayParam(@[dt])

  test "DateTime years no PostgreSQL type holds are rejected before stdlib math":
    for year in [-4714, 5874898, 100_000_000_000]:
      let dt = dateTime(year, mJan, 1, zone = utc())
      expect PgTypeError:
        discard toPgParam(dt)
      expect PgTypeError:
        discard toPgDateParam(dt)
      expect PgTypeError:
        discard toPgTimestampTzParam(dt)

  test "pgParseFloat bounds the exponent instead of overflowing":
    for s in [
      "1e9223372036854775808", "1e-9223372036854775808", "1.5e99999999999999999999",
      "1e1000000000",
    ]:
      expect PgTypeError:
        discard pgParseFloat(s)
    check pgParseFloat("1e999999999") == Inf
    check pgParseFloat("1e-999999999") == 0.0
    check pgParseFloat("1e0000000000000000000002") == 100.0
    check pgParseFloat("0e0") == 0.0

  test "pgParseFloat bounds the first exponent, the one the stdlib sums":
    # Only the first `e`/`E` starts the exponent the stdlib parses; a later one
    # must not reset the digit counter, or the parsed exponent is left to the
    # stdlib's unchecked int sum (`pgParseFloat` must stay PgTypeError-only).
    for s in [
      "1e9999999999999999999e1", "1e9223372036854775808e1", "1e1111111111e-1",
      "1e1000000000e1",
    ]:
      expect PgTypeError:
        discard pgParseFloat(s)
    # A later `e` cannot rescue a sign-only first exponent either.
    for s in ["1e+e5", "1e-e5", "1e+E5", "1e-e"]:
      expect PgTypeError:
        discard pgParseFloat(s)

  test "pgParseFloat rejects a sign-only exponent before the stdlib parser":
    # The stdlib parses the byte after the sign without a bounds check, so a
    # zero-copy cell ending in `1e+` would read the next cell's first byte and
    # could return the mantissa as if the exponent were 0.
    for s in ["1e+", "1e-", "1E+", "1.5e-", "1e", "0e"]:
      expect PgTypeError:
        discard pgParseFloat(s)

  test "getFloat on a malformed exponent cell does not read the next cell":
    let row: Row = @[some(toBytes("1e+")), some(toBytes("5"))]
    expect PgTypeError:
      discard row.getFloat(0)
    expect PgTypeError:
      discard row.getFloat32(0)

  test "getFloat on a server-sent huge exponent":
    let row = mkRow(@[some(toBytes("1e9223372036854775808"))], @[mkField(OidFloat8, 0)])
    expect PgTypeError:
      discard row.getFloat(0)

  test "getFloat bounds the exponent parsed from a multi-exponent cell":
    # Same mismatch as pgParseFloat: the server cell can hold a second `e`.
    let row =
      mkRow(@[some(toBytes("1e9999999999999999999e1"))], @[mkField(OidFloat8, 0)])
    expect PgTypeError:
      discard row.getFloat(0)
    expect PgTypeError:
      discard row.getFloat32(0)

  test "server-sent text years outside PostgreSQL's range raise PgTypeError":
    # `YYYY` takes any number of digits; past ~2.92e11 the stdlib date math
    # overflowed int64 (OverflowDefect escaped `except PgError`) and years just
    # past a type's end decoded silently.
    let tsRow = mkRow(
      @[some(toBytes("999999999999-01-01 00:00:00"))], @[mkField(OidTimestamp, 0)]
    )
    expect PgTypeError:
      discard tsRow.getTimestamp(0)
    let dateRow = mkRow(@[some(toBytes("5874898-01-01"))], @[mkField(OidDate, 0)])
    expect PgTypeError:
      discard dateRow.getDate(0)
    let rangeRow = mkRow(
      @[some(toBytes("[999999999999-01-01,2000-01-01)"))], @[mkField(OidDateRange, 0)]
    )
    expect PgTypeError:
      discard rangeRow.getDateRange(0)

  test "$PgNumeric at the maximum weight":
    # 131072 integer digits = 32768 base-10000 groups = weight high(int16).
    let s = "1" & repeat('0', 131071)
    let n = parsePgNumeric(s)
    check n.weight == high(int16)
    check $n == s
    check toPgParam(n).value.get.len == s.len
    let neg =
      PgNumeric(weight: high(int16), sign: pgNegative, dscale: 2, digits: @[7'i16])
    check $neg == "-7" & repeat('0', 32767 * 4) & ".00"

  test "row index outside the RowData":
    let row = mkRow(@[some(toBytes("x"))], @[mkField(OidText, 0)])
    for idx in [-1'i32, 1'i32, int32.high]:
      let bad = initRow(row.data, idx)
      expect PgTypeError:
        discard bad.getStr(0)
      expect PgTypeError:
        discard bad.isNull(0)
      expect PgTypeError:
        discard bad.getStrOpt(0)
      expect PgTypeError:
        discard bad.getNumeric(0)
      expect PgTypeError:
        discard bad.clone()

  test "hand-built RowData with a short cellIndex or a cell past the buffer":
    let short = RowData(numCols: 2, buf: toBytes("x"), cellIndex: @[0'i32, 1'i32])
    check initRow(short, 0).getStr(0) == "x"
    expect PgTypeError:
      discard initRow(short, 0).getStr(1)
    expect PgTypeError:
      discard initRow(short, 0).isNull(1)
    # A negative numCols never surfaces as a negative column count.
    check initRow(RowData(numCols: -1, buf: @[], cellIndex: @[]), 0).len == 0
    for cell in [@[0'i32, 2'i32], @[-1'i32, 1'i32], @[0'i32, -2'i32]]:
      let rd = RowData(numCols: 1, buf: toBytes("x"), cellIndex: cell)
      expect PgTypeError:
        discard initRow(rd, 0).getStr(0)

  test "Row without data":
    # Typed getters probe the column format/OID before reading the cell, so
    # each probe must tolerate a nil RowData and leave the error to the read.
    template rejects(body: untyped) =
      expect PgTypeError:
        discard body

    for row in [default(Row), default(Row).clone()]:
      check row.len == 0
      check not row.isBinaryCol(0)
      rejects row[0]
      rejects row.isNull(0)
      rejects row.getStr(0)
      rejects row.getStrOpt(0)
      rejects row.getInt(0)
      rejects row.getInt64(0)
      rejects row.getFloat(0)
      rejects row.getBool(0)
      rejects row.getNumeric(0)
      rejects row.getNumericOpt(0)
      rejects row.getTimestamp(0)
      rejects row.getDate(0)
      rejects row.getUuid(0)
      rejects row.getJson(0)
      rejects row.getBytes(0)
      rejects row.getIntArray(0)
      rejects row.getTsRange(0)

  test "text DateTime literals spell the era and years past 9999":
    # Stdlib's `yyyy` would print 4714 BC as 4714 (AD) and 10000 as `+10000`,
    # which PostgreSQL reads as a zone offset.
    let bc = dateTime(-4713, mNov, 24, zone = utc())
    let ad = dateTime(2000, mJan, 1, zone = utc())
    check toString(toPgParam(bc).value.get) == "4714-11-24 00:00:00.000000 BC"
    check toString(toPgTimestampTzParam(bc).value.get) ==
      "4714-11-24 00:00:00.000000Z BC"
    check toString(toPgDateParam(bc).value.get) == "4714-11-24 BC"
    check toString(toPgDateParam(dateTime(0, mDec, 31, zone = utc())).value.get) ==
      "0001-12-31 BC"
    check toString(toPgDateParam(dateTime(1, mJan, 1, zone = utc())).value.get) ==
      "0001-01-01"
    check toString(toPgParam(dateTime(10000, mJan, 1, zone = utc())).value.get) ==
      "10000-01-01 00:00:00.000000"
    for lit in [
      toString(toPgParam(rangeOf(bc, ad)).value.get),
      toString(toPgTsTzRangeParam(rangeOf(bc, ad)).value.get),
      toString(toPgDateRangeParam(rangeOf(bc, ad)).value.get),
      toString(toPgParam(toMultirange(rangeOf(bc, ad))).value.get),
    ]:
      check "4714-11-24" in lit
      check " BC\"" in lit
    for lit in [
      toString(toPgParam(@[rangeOf(bc, ad)]).value.get),
      toString(toPgTsTzRangeArrayParam(@[rangeOf(bc, ad)]).value.get),
      toString(toPgDateRangeArrayParam(@[rangeOf(bc, ad)]).value.get),
      toString(toPgTsMultirangeArrayParam(@[toMultirange(rangeOf(bc, ad))]).value.get),
      toString(toPgTsTzMultirangeArrayParam(@[toMultirange(rangeOf(bc, ad))]).value.get),
      toString(toPgDateMultirangeArrayParam(@[toMultirange(rangeOf(bc, ad))]).value.get),
    ]:
      check "4714-11-24" in lit
      check " BC" in lit

  test "text timestamp and date check the same range as binary":
    let pastTs = dateTime(294277, mJan, 1, zone = utc())
    let beforeAll = dateTime(-4713, mNov, 23, zone = utc())
    let ad = dateTime(2000, mJan, 1, zone = utc())
    expect PgTypeError:
      discard toPgParam(pastTs)
    expect PgTypeError:
      discard toPgTimestampTzParam(pastTs)
    expect PgTypeError:
      discard toPgParam(rangeOf(ad, pastTs))
    # `date` reaches much further than `timestamp`.
    check toString(toPgDateParam(pastTs).value.get) == "294277-01-01"
    check toString(toPgDateRangeParam(rangeOf(ad, pastTs)).value.get) ==
      "[2000-01-01,294277-01-01)"
    for dt in [beforeAll, dateTime(5874898, mJan, 1, zone = utc())]:
      expect PgTypeError:
        discard toPgParam(dt)
      expect PgTypeError:
        discard toPgDateParam(dt)

  test "text timestamptz keeps the instant of a seconds-bearing offset":
    # `zzz` prints +09:18, which PostgreSQL reads 59 s later than +09:18:59.
    let lmt = fixedZone("LMT+09:18:59", -(9 * 3600 + 18 * 60 + 59))
    let dt = dateTime(1850, mJan, 1, 12, 0, 0, zone = utc()).inZone(lmt)
    check dt.hour == 21 and dt.minute == 18 and dt.second == 59
    check toString(toPgTimestampTzParam(dt).value.get) == "1850-01-01 12:00:00.000000Z"
    check toString(toPgTsTzRangeParam(rangeOf(dt, dt)).value.get) ==
      "[\"1850-01-01 12:00:00.000000Z\",\"1850-01-01 12:00:00.000000Z\")"
    check toString(toPgTsTzMultirangeParam(toMultirange(rangeOf(dt, dt))).value.get) ==
      "{[\"1850-01-01 12:00:00.000000Z\",\"1850-01-01 12:00:00.000000Z\")}"

  test "text timestamptz at the lower bound in a zone west of UTC":
    # The local wall clock is a day before PostgreSQL's first date; the
    # literal is written in UTC, where the checked instant lies.
    let west4 = fixedZone("-04:00", 4 * 3600)
    let dt = dateTime(-4713, mNov, 24, 1, 0, 0, zone = utc()).inZone(west4)
    check dt.monthday == 23
    check toString(toPgTimestampTzParam(dt).value.get) ==
      "4714-11-24 01:00:00.000000Z BC"
