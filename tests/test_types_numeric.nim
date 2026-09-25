import std/[unittest, options, strutils, math, net, typetraits]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}

import types_common

suite "PgNumeric":
  test "paramValueLenBound never under-charges the rendered length":
    # The pre-flight uses the bound so it does not render; under-charging would
    # let an oversized Bind reach the encoder after the Close drain.
    for s in [
      "0", "-1", "NaN", "0.00", "12345.6789", "-0.001", "100000000", "0.00001",
      "999.999", "-123456789012345678901234567890.123456789",
    ]:
      let n = parsePgNumeric(s)
      check paramValueLenBound(n) >= paramValueLen(n)

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

  test "encodeNumericBinary rejects out-of-range base-10000 digit":
    # User-constructed PgNumeric with digits outside [0, 9999] must fail
    # locally instead of sending invalid wire data the server would reject.
    expect PgTypeError:
      discard encodeNumericBinary(
        PgNumeric(weight: 1, sign: pgPositive, dscale: 0, digits: @[12345'i16, 6'i16])
      )
    expect PgTypeError:
      discard encodeNumericBinary(
        PgNumeric(weight: 0, sign: pgPositive, dscale: 0, digits: @[-1'i16])
      )

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
    check parsePgMoney("1.00$") == initPgMoney(100)
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

  test "parsePgMoney accepts ASCII locale currency symbols":
    # sv_SE / hu_HU / pl_PL emit letter symbols, so letters cannot be banned
    # in the symbol region.
    check parsePgMoney("1234,56 kr") == initPgMoney(123456)
    check parsePgMoney("1234,56 Ft") == initPgMoney(123456)
    check parsePgMoney("zł1234,56") == initPgMoney(123456)
    check parsePgMoney("R$1234,56") == initPgMoney(123456)

  test "parsePgMoney accepts the Unicode space and quote separators":
    # Real mon_thousands_sep values: U+202F (fr_FR / ru_RU / sv_SE),
    # U+2019 and ' (de_CH), U+00A0 and a plain space in older locale data.
    check parsePgMoney("1 234.56") == initPgMoney(123456)
    check parsePgMoney("1 234,56 €") == initPgMoney(123456)
    check parsePgMoney("1 234 567,89") == initPgMoney(123456789)
    check parsePgMoney("-1 234,56 kr") == initPgMoney(-123456)
    check parsePgMoney("1 234", scale = 0) == initPgMoney(1234, scale = 0)
    check parsePgMoney("1\u202F234,56 \u20AC") == initPgMoney(123456)
    check parsePgMoney("1\u2019234.56") == initPgMoney(123456)
    check parsePgMoney("1'234.56") == initPgMoney(123456)
    check parsePgMoney("1\u2009234,56") == initPgMoney(123456)
    check parsePgMoney("1\u202F234\u202F567", scale = 0) ==
      initPgMoney(1234567, scale = 0)

  test "parsePgMoney rejects a short group after a space separator":
    # scale=0 has no fractional part, so a space separator is grouping and the
    # last group must be full; otherwise "1 23" would decode as 123.
    expect(PgTypeError):
      discard parsePgMoney("1 23", scale = 0)
    expect(PgTypeError):
      discard parsePgMoney("1\u00A023", scale = 0)

  test "parsePgMoney rejects 4-wide groups without a fractional part":
    # With scale=0 every separator is grouping, and no locale pairs frac_digits
    # = 0 with 4-wide groups -- accepting them would read a 4-decimal "1.2345"
    # from a mismatched lc_monetary as 12345.
    expect(PgTypeError):
      discard parsePgMoney("1.2345", scale = 0)
    expect(PgTypeError):
      discard parsePgMoney("12.3456", scale = 0)
    # 4-wide grouping stays valid where a locale really uses it (zh_TW).
    check parsePgMoney("1,2345.67") == initPgMoney(1234567)

  test "parsePgMoney rejects a separator inside the fractional part":
    expect(PgTypeError):
      discard parsePgMoney("1.2 3")
    expect(PgTypeError):
      discard parsePgMoney("1,2\u00A03")

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
    # Sign in the middle of digits must not be silently dropped.
    expect(PgTypeError):
      discard parsePgMoney("12-34", scale = 0)
    expect(PgTypeError):
      discard parsePgMoney("12-34.56")
    expect(PgTypeError):
      discard parsePgMoney("1.00-")
    # Two signs in the prefix are ambiguous, not "+ wins".
    expect(PgTypeError):
      discard parsePgMoney("+-1.00")
    expect(PgTypeError):
      discard parsePgMoney("-+1.00")
    # scale=0 must not silently truncate fractional digits.
    expect(PgTypeError):
      discard parsePgMoney("1.23", scale = 0)
    expect(PgTypeError):
      discard parsePgMoney("1.5", scale = 0)
    # Junk interrupting the digits must not be dropped.
    expect(PgTypeError):
      discard parsePgMoney("12a34.56")
    expect(PgTypeError):
      discard parsePgMoney("12€34.56")
    expect(PgTypeError):
      discard parsePgMoney("1.00 2")
    expect(PgTypeError):
      discard parsePgMoney("1.00 kr 2.00")

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

  test "comparison is by decimal value across scales":
    # 1.00 and 1.000 are the same amount written at two scales.
    check initPgMoney(100, scale = 2) == initPgMoney(1000, scale = 3)
    check hash(initPgMoney(100, scale = 2)) == hash(initPgMoney(1000, scale = 3))
    # 1.00 < 100 and 100 > 1.00 both hold, instead of raising.
    check initPgMoney(100, scale = 2) < initPgMoney(100, scale = 0)
    check initPgMoney(100, scale = 0) > initPgMoney(100, scale = 2)
    check not (initPgMoney(100, scale = 0) <= initPgMoney(100, scale = 2))
    check initPgMoney(-1, scale = 2) < initPgMoney(0, scale = 0)
    # int64.low must not wrap while being rescaled for comparison.
    check initPgMoney(low(int64), scale = 2) < initPgMoney(0, scale = 0)
    check initPgMoney(low(int64), scale = 2) == initPgMoney(low(int64), scale = 2)
    check initPgMoney(high(int64), scale = 18) > initPgMoney(9, scale = 0)

  test "PgMoney cannot be built without its scale":
    # A partial literal would tag the amount with scale 0, shifting it by two
    # digits; private fields make it fail to compile.
    check not compiles(PgMoney(amountRaw: 123456'i64))
    check not compiles((var m = initPgMoney(1); m.amount = 2))

  test "formatPgMoney default = $":
    check formatPgMoney(initPgMoney(123456)) == "1234.56"
    check formatPgMoney(initPgMoney(-123456)) == "-1234.56"

  test "formatPgMoney en_US style":
    check formatPgMoney(initPgMoney(123456), symbol = "$", thousandsSep = ",") ==
      "$1,234.56"
    check formatPgMoney(initPgMoney(123456789), symbol = "$", thousandsSep = ",") ==
      "$1,234,567.89"
    check formatPgMoney(initPgMoney(-123456), symbol = "$", thousandsSep = ",") ==
      "-$1,234.56"
    check formatPgMoney(initPgMoney(1), symbol = "$", thousandsSep = ",") == "$0.01"

  test "formatPgMoney EU style":
    check formatPgMoney(
      initPgMoney(123456),
      symbol = "€",
      decimalSep = ',',
      thousandsSep = ".",
      symbolBefore = false,
      sepBySpace = true,
    ) == "1.234,56 €"

  test "formatPgMoney scale=0":
    check formatPgMoney(
      initPgMoney(1234567, scale = 0), symbol = "¥", thousandsSep = ","
    ) == "¥1,234,567"

  test "formatPgMoney accounting parens for negatives":
    check formatPgMoney(
      initPgMoney(-123456), symbol = "$", thousandsSep = ",", accountingParens = true
    ) == "($1,234.56)"
    # Positive values unaffected
    check formatPgMoney(
      initPgMoney(123456), symbol = "$", thousandsSep = ",", accountingParens = true
    ) == "$1,234.56"
    # EU style, symbol after
    check formatPgMoney(
      initPgMoney(-123456),
      symbol = "€",
      decimalSep = ',',
      thousandsSep = ".",
      symbolBefore = false,
      accountingParens = true,
      sepBySpace = true,
    ) == "(1.234,56 €)"
    # Roundtrip through parsePgMoney
    let v = initPgMoney(-123456)
    let s = formatPgMoney(v, symbol = "$", thousandsSep = ",", accountingParens = true)
    check parsePgMoney(s) == v

  test "initPgMoneyConventions rejects ambiguous conventions":
    expect(PgTypeError):
      discard initPgMoneyConventions(decimalSep = '.', thousandsSep = ".")
    expect(PgTypeError):
      discard initPgMoneyConventions(decimalSep = '1')
    expect(PgTypeError):
      discard initPgMoneyConventions(symbol = "1$")
    expect(PgTypeError):
      discard initPgMoneyConventions(thousandsSep = "-")
    expect(PgTypeError):
      discard initPgMoneyConventions(fracDigits = 19)
    expect(PgTypeError):
      discard initPgMoneyConventions(fracDigits = -1)

  test "initPgMoneyConventions keeps the symbol separate from its space":
    # A space folded into the symbol would be eaten by whitespace trimming
    # while parsing, so the round-trip would not hold.
    expect(PgTypeError):
      discard initPgMoneyConventions(symbol = " \u20AC")
    expect(PgTypeError):
      discard initPgMoneyConventions(symbol = "\u20AC ")
    expect(PgTypeError):
      discard initPgMoneyConventions(symbol = "\u00A0\u20AC")
    # Parsing strips every whitespace character, not just sepBySpace ones.
    for ws in ["\t", "\n", "\r", "\v", "\f"]:
      expect(PgTypeError):
        discard initPgMoneyConventions(symbol = ws & "$")
      expect(PgTypeError):
        discard initPgMoneyConventions(symbol = "$" & ws, symbolBefore = false)
    expect(PgTypeError):
      discard initPgMoneyConventions(sepBySpace = true)
    # Interior space is a symbol, not a separator, and stays.
    check initPgMoneyConventions(symbol = "R $").symbol == "R $"

  test "formatPgMoney unfolds a space folded into the symbol":
    # The pre-sepBySpace API had no other way to ask for the space.
    let v = initPgMoney(123456)
    check formatPgMoney(v, symbol = " \u20AC", symbolBefore = false) ==
      formatPgMoney(v, symbol = "\u20AC", symbolBefore = false, sepBySpace = true)
    check formatPgMoney(v, symbol = "$ ") == "$ 1234.56"
    # On the outer edge the space faces nothing; parsing strips it either way.
    check formatPgMoney(v, symbol = "\u00A0$") == "$1234.56"
    check parsePgMoney(formatPgMoney(v, symbol = " \u20AC", symbolBefore = false)) == v
    # Interior space still belongs to the symbol.
    check formatPgMoney(v, symbol = "R $") == "R $1234.56"

  test "PgMoneyConventions exposes the locale it was built from":
    let c = initPgMoneyConventions(
      symbol = "\u20AC",
      decimalSep = ',',
      thousandsSep = "\u202F",
      symbolBefore = false,
      accountingParens = true,
      sepBySpace = true,
      fracDigits = 3,
    )
    check c.symbol == "\u20AC"
    check c.decimalSep == ','
    check c.thousandsSep == "\u202F"
    check not c.symbolBefore
    check c.accountingParens
    check c.sepBySpace
    check c.fracDigits == 3
    # Fields are read-only: the constructor is the only way in.
    check not compiles((var v = initPgMoneyConventions(); v.symbol = "$"))
    check not compiles(PgMoneyConventions(symbolRaw: "$"))

  test "formatPgMoney rejects a value whose scale is not the locale's":
    let enUS = initPgMoneyConventions(symbol = "$", thousandsSep = ",")
    expect(PgTypeError):
      discard formatPgMoney(initPgMoney(1234, scale = 0), enUS)
    expect(PgTypeError):
      discard formatPgMoney(initPgMoney(1234, scale = 3), enUS)
    # A conventions value that skipped the constructor must not pass for a
    # fracDigits-0 locale: that would read 12.34 as 1234.
    var zero: PgMoneyConventions
    expect(PgTypeError):
      discard formatPgMoney(initPgMoney(1234, scale = 0), zero)
    expect(PgTypeError):
      discard parsePgMoney("1234", zero)
    expect(PgTypeError):
      discard initPgMoneyConventions(decimalSep = '\0')

  test "inferPgMoneyConventions reads the locale back out of a string":
    let enUS = inferPgMoneyConventions("$1,234.56")
    check enUS.symbol == "$"
    check enUS.symbolBefore
    check not enUS.sepBySpace
    check enUS.thousandsSep == ","
    check enUS.decimalSep == '.'
    check enUS.fracDigits == 2
    let frFR = inferPgMoneyConventions("1\u202F234,56 \u20AC")
    check frFR.symbol == "\u20AC"
    check not frFR.symbolBefore
    check frFR.sepBySpace
    check frFR.thousandsSep == "\u202F"
    check frFR.decimalSep == ','
    let jaJP = inferPgMoneyConventions("\u00A51,234", fracDigits = 0)
    check jaJP.symbol == "\u00A5"
    check jaJP.thousandsSep == ","
    check jaJP.fracDigits == 0
    check inferPgMoneyConventions("($1.00)").accountingParens
    # Inference feeds the same parser the strict overload uses.
    for str in ["$1,234.56", "1\u202F234,56 \u20AC", "($1.00)", "-1.234,56 \u20AC"]:
      check parsePgMoney(str, inferPgMoneyConventions(str)) == parsePgMoney(str)

  test "inferPgMoneyConventions describes only the sign of its sample":
    # A positive sample says nothing about n_sign_posn, so conventions inferred
    # from one need not accept the locale's negatives: infer from a negative
    # sample when reusing the result across a result set.
    let fromPositive = inferPgMoneyConventions("$1,234.56")
    check not fromPositive.accountingParens
    expect(PgTypeError):
      discard parsePgMoney("($1,234.56)", fromPositive)
    let fromNegative = inferPgMoneyConventions("($1,234.56)")
    check fromNegative.accountingParens
    check parsePgMoney("($1,234.56)", fromNegative) == initPgMoney(-123456)
    check parsePgMoney("$1,234.56", fromNegative) == initPgMoney(123456)

  test "inferPgMoneyConventions reports the input length, not the input":
    # Decode failures must not echo cell content (PII/secrets land in
    # logs via exception text). The message carries the length instead.
    for bad in ["1.000.00", "1..00", "(1.00", "1.00)"]:
      try:
        discard parsePgMoney(bad)
        check false
      except PgTypeError as e:
        check e.msg == "Invalid money format (len=" & $bad.len & ")"
        check bad notin e.msg

  test "inferPgMoneyConventions rejects what no single locale explains":
    # Two different group separators.
    expect(PgTypeError):
      discard inferPgMoneyConventions("1.234 567,89")
    # Symbol runs on both sides.
    expect(PgTypeError):
      discard inferPgMoneyConventions("$1.00 kr")
    # Unbalanced accounting parens are junk, not a symbol.
    expect(PgTypeError):
      discard parsePgMoney("(1.00")
    expect(PgTypeError):
      discard parsePgMoney("1.00)")
    expect(PgTypeError):
      discard parsePgMoney(")1.00(")

  test "getMoney with conventions validates instead of inferring":
    let enUS = initPgMoneyConventions(symbol = "$", thousandsSep = ",")
    let good: Row = @[some(toBytes("$1,234.56"))]
    check good.getMoney(0, enUS) == initPgMoney(123456)
    check good.get(0, PgMoney, enUS) == initPgMoney(123456)
    check good.getMoneyOpt(0, enUS) == some(initPgMoney(123456))
    let bad: Row = @[some(toBytes("1.234,56 \u20AC"))]
    check bad.getMoney(0) == initPgMoney(123456) # inference accepts it
    expect(PgTypeError):
      discard bad.getMoney(0, enUS)
    # Binary cells take their scale from the conventions.
    let jaJP =
      initPgMoneyConventions(symbol = "\u00A5", thousandsSep = ",", fracDigits = 0)
    let fields = @[mkField(OidMoney, 1)]
    let binRow = mkRow(@[some(@(toBE64(987654'i64)))], fields)
    check binRow.getMoney(0, jaJP) == initPgMoney(987654, scale = 0)
    check binRow.getMoney("test", jaJP) == initPgMoney(987654, scale = 0)
    check binRow.getMoneyOpt("test", jaJP) == some(initPgMoney(987654, scale = 0))

  test "getMoneyArray with conventions":
    let enUS = initPgMoneyConventions(symbol = "$", thousandsSep = ",")
    let row: Row = @[some(toBytes("{\"$1,234.56\",\"$2.00\"}"))]
    check row.getMoneyArray(0, enUS) == @[initPgMoney(123456), initPgMoney(200)]
    check row.getMoneyArrayOpt(0, enUS) == some(
      @[initPgMoney(123456), initPgMoney(200)]
    )
    let eu: Row = @[some(toBytes("{\"1.234,56\"}"))]
    expect(PgTypeError):
      discard eu.getMoneyArray(0, enUS)

  test "getMoneyArrayND with conventions takes its scale from fracDigits":
    let jaJP =
      initPgMoneyConventions(symbol = "\u00A5", thousandsSep = ",", fracDigits = 0)
    let p = toPgParam(@[initPgMoney(100), initPgMoney(-50)])
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[p.value], fields)
    let nd = row.getMoneyArrayND(0, jaJP)
    check nd.elements ==
      @[some(initPgMoney(100, scale = 0)), some(initPgMoney(-50, scale = 0))]
    check row.getMoneyArrayNDOpt(0, jaJP).get.elements == nd.elements

  test "binary accessors reject uninitialized conventions":
    # fracDigits = 0; the binary paths never reach the text parser, so nothing
    # else would catch it.
    var zero: PgMoneyConventions
    let fields = @[mkField(OidMoney, 1)]
    let binRow = mkRow(@[some(@(toBE64(12345'i64)))], fields)
    expect(PgTypeError):
      discard binRow.getMoney(0, zero)
    expect(PgTypeError):
      discard binRow.getMoney("test", zero)
    expect(PgTypeError):
      discard binRow.getMoneyOpt(0, zero)
    expect(PgTypeError):
      discard binRow.get(0, PgMoney, zero)
    let arrFields = @[mkField(OidMoneyArray, 1)]
    let p = toPgParam(@[initPgMoney(100)])
    let arrRow = mkRow(@[p.value], arrFields)
    expect(PgTypeError):
      discard arrRow.getMoneyArray(0, zero)
    expect(PgTypeError):
      discard arrRow.getMoneyArrayOpt(0, zero)
    expect(PgTypeError):
      discard arrRow.getMoneyArrayND(0, zero)
    expect(PgTypeError):
      discard arrRow.getMoneyArrayNDOpt(0, zero)

  test "Opt accessors validate before the NULL test":
    # A bad scale or zero-initialized conventions must surface on row one, not
    # after the leading NULL rows.
    var zero: PgMoneyConventions
    let nullRow = mkRow(@[none(seq[byte])], @[mkField(OidMoney, 1)])
    expect(PgTypeError):
      discard nullRow.getMoneyOpt(0, zero)
    expect(PgTypeError):
      discard nullRow.getMoneyOpt(0, scale = MaxMoneyScale + 1)
    let nullArrRow = mkRow(@[none(seq[byte])], @[mkField(OidMoneyArray, 1)])
    expect(PgTypeError):
      discard nullArrRow.getMoneyArrayOpt(0, zero)
    expect(PgTypeError):
      discard nullArrRow.getMoneyArrayOpt(0, scale = MaxMoneyScale + 1)
    expect(PgTypeError):
      discard nullArrRow.getMoneyArrayNDOpt(0, zero)
    expect(PgTypeError):
      discard nullArrRow.getMoneyArrayNDOpt(0, scale = MaxMoneyScale + 1)
    # NULL still reads as `none` once the arguments are sound.
    check nullRow.getMoneyOpt(0, initPgMoneyConventions(symbol = "$")) == none(PgMoney)

  test "money with a non-default mon_grouping":
    # PostgreSQL groups by mon_grouping[0]; cmn_TW and friends use 4.
    let twConv =
      initPgMoneyConventions(symbol = "NT$", thousandsSep = ",", groupDigits = 4)
    check twConv.groupDigits == 4
    let v = initPgMoney(1234567890'i64)
    check formatPgMoney(v, twConv) == "NT$1234,5678.90"
    check parsePgMoney("NT$1,2345,6789.00", twConv) == initPgMoney(12345678900'i64)
    check parsePgMoney("NT$1,2345,6789.00") == initPgMoney(12345678900'i64)
    check inferPgMoneyConventions("NT$1,2345,6789.00").groupDigits == 4
    check formatPgMoney(v, symbol = "NT$", thousandsSep = ",", groupDigits = 4) ==
      "NT$1234,5678.90"
    # Groups still have to agree with each other.
    expect(PgTypeError):
      discard parsePgMoney("NT$1,234,5678.00", twConv)
    expect(PgTypeError):
      discard parsePgMoney("1,23,456.00")
    expect(PgTypeError):
      discard initPgMoneyConventions(groupDigits = 0)
    expect(PgTypeError):
      discard initPgMoneyConventions(groupDigits = 7)

  test "formatPgMoney with multi-byte thousands separator":
    let frFR = initPgMoneyConventions(
      symbol = "\u20AC",
      decimalSep = ',',
      thousandsSep = "\u202F",
      symbolBefore = false,
      sepBySpace = true,
    )
    check formatPgMoney(initPgMoney(123456789), frFR) == "1\u202F234\u202F567,89 \u20AC"

  test "parsePgMoney with conventions roundtrips formatPgMoney":
    let convs = [
      initPgMoneyConventions(symbol = "$", thousandsSep = ","),
      initPgMoneyConventions(
        symbol = "\u20AC",
        decimalSep = ',',
        thousandsSep = "\u202F",
        symbolBefore = false,
        sepBySpace = true,
      ),
      initPgMoneyConventions(symbol = "$", thousandsSep = ",", accountingParens = true),
      initPgMoneyConventions(),
    ]
    for c in convs:
      for amount in [123456'i64, -123456, 1, 0, -1, high(int64), low(int64)]:
        let v = initPgMoney(amount)
        check parsePgMoney(formatPgMoney(v, c), c) == v
    let jaJP =
      initPgMoneyConventions(symbol = "\u00A5", thousandsSep = ",", fracDigits = 0)
    for amount in [1234567'i64, -1234567, 0]:
      let v = initPgMoney(amount, scale = 0)
      check parsePgMoney(formatPgMoney(v, jaJP), jaJP) == v

  test "parsePgMoney with conventions accepts sign on either side of symbol":
    let enUS = initPgMoneyConventions(symbol = "$", thousandsSep = ",")
    check parsePgMoney("-$1,234.56", enUS) == initPgMoney(-123456)
    check parsePgMoney("$-1,234.56", enUS) == initPgMoney(-123456)
    check parsePgMoney("$1234.56", enUS) == initPgMoney(123456)

  test "parsePgMoney with conventions rejects what the lenient overload allows":
    let enUS = initPgMoneyConventions(symbol = "$", thousandsSep = ",")
    # A run on one side is an unknown symbol; runs on both sides are no locale.
    check parsePgMoney("1,234.56 junk") == initPgMoney(123456)
    expect(PgTypeError):
      discard parsePgMoney("$1,234.56 junk")
    expect(PgTypeError):
      discard parsePgMoney("$1,234.56 junk", enUS)
    expect(PgTypeError):
      discard parsePgMoney("$1,23,456.78", enUS)
    expect(PgTypeError):
      discard parsePgMoney("$1,2345.67", enUS)
    expect(PgTypeError):
      discard parsePgMoney("1,234.56", enUS)
    expect(PgTypeError):
      discard parsePgMoney("1,234.56$", enUS)
    expect(PgTypeError):
      discard parsePgMoney("$1.234,56", enUS)
    # Parens only when the conventions declare them.
    expect(PgTypeError):
      discard parsePgMoney("($1,234.56)", enUS)
    check parsePgMoney(
      "($1,234.56)",
      initPgMoneyConventions(symbol = "$", thousandsSep = ",", accountingParens = true),
    ) == initPgMoney(-123456)

  test "parsePgMoney with conventions enforces scale exactly":
    let jaJP =
      initPgMoneyConventions(symbol = "\u00A5", thousandsSep = ",", fracDigits = 0)
    check parsePgMoney("\u00A51,234", jaJP) == initPgMoney(1234, scale = 0)
    expect(PgTypeError):
      discard parsePgMoney("\u00A51,234.00", jaJP)
    expect(PgTypeError):
      discard parsePgMoney("\u00A51,23", jaJP)
    expect(PgTypeError):
      discard parsePgMoney("$1.5", initPgMoneyConventions(symbol = "$"))
    expect(PgTypeError):
      discard parsePgMoney("$1.234", initPgMoneyConventions(symbol = "$"))

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

  test "getMoneyOpt forwards scale kwarg":
    # Non-default scales (e.g. ja_JP frac_digits=0) must survive the Opt wrapper.
    let fields = @[mkField(OidMoney, 1)]
    let rowSome = mkRow(@[some(@(toBE64(42'i64)))], fields)
    check rowSome.getMoneyOpt(0, scale = 0) == some(initPgMoney(42, scale = 0))
    check rowSome.getMoneyOpt(0, scale = 3) == some(initPgMoney(42, scale = 3))

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

  test "getMoneyArrayOpt forwards scale kwarg":
    let values0 = @[initPgMoney(1, scale = 0), initPgMoney(2, scale = 0)]
    let p = toPgParam(@[initPgMoney(1), initPgMoney(2)])
    let fields = @[mkField(OidMoneyArray, 1)]
    let rowSome = mkRow(@[p.value], fields)
    check rowSome.getMoneyArrayOpt(0, scale = 0) == some(values0)

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

  test "getMoney by name forwards scale":
    let fields = @[mkField(OidMoney, 1)]
    let row = mkRow(@[some(@(toBE64(987654'i64)))], fields)
    check row.getMoney("test") == initPgMoney(987654)
    check row.getMoney("test", scale = 0) == initPgMoney(987654, scale = 0)

  test "getMoneyArray by name forwards scale":
    let values = @[initPgMoney(100, scale = 0), initPgMoney(-50, scale = 0)]
    let p = toPgParam(@[initPgMoney(100), initPgMoney(-50)])
    let fields = @[mkField(OidMoneyArray, 1)]
    let row = mkRow(@[p.value], fields)
    check row.getMoneyArray("test", scale = 0) == values

  test "toPgParam PgMoney rejects scale mismatch":
    expect PgTypeError:
      discard toPgParam(initPgMoney(100, scale = 0))
    expect PgTypeError:
      discard toPgBinaryParam(initPgMoney(100, scale = 0))
    expect PgTypeError:
      discard toPgParamInline(initPgMoney(100, scale = 0))

  test "toPgParam PgMoney with explicit scale":
    let p = toPgParam(initPgMoney(100, scale = 0), scale = 0)
    check p.oid == OidMoney
    check p.value.get == @(toBE64(100'i64))
    let pb = toPgBinaryParam(initPgMoney(100, scale = 3), scale = 3)
    check pb.value.get == @(toBE64(100'i64))

  test "generic get forwards money scale":
    let fields = @[mkField(OidMoney, 1)]
    let row = mkRow(@[some(@(toBE64(42'i64)))], fields)
    check row.get(0, PgMoney) == initPgMoney(42)
    check row.get(0, PgMoney, scale = 0) == initPgMoney(42, scale = 0)
    let arrFields = @[mkField(OidMoneyArray, 1)]
    let arrRow = mkRow(@[toPgParam(@[initPgMoney(42)]).value], arrFields)
    check arrRow.get(0, seq[PgMoney]) == @[initPgMoney(42)]
    check arrRow.get(0, seq[PgMoney], scale = 0) == @[initPgMoney(42, scale = 0)]

  test "Option[PgMoney] none encodes as NULL without scale check":
    let p = toPgParam(none(PgMoney))
    check p.oid == OidMoney
    check p.format == 1'i16
    check p.value.isNone
    let pb = toPgBinaryParam(none(PgMoney))
    check pb.oid == OidMoney
    check pb.value.isNone
    let pi = toPgParamInline(none(PgMoney))
    check pi.oid == OidMoney
    check pi.len == -1'i32

  test "Option[PgMoney] some validates scale":
    let m0 = initPgMoney(100, scale = 0)
    expect PgTypeError:
      discard toPgParam(some(m0))
    expect PgTypeError:
      discard toPgBinaryParam(some(m0))
    expect PgTypeError:
      discard toPgParamInline(some(m0))
    check toPgParam(some(m0), scale = 0).value.get == @(toBE64(100'i64))
    check toPgBinaryParam(some(m0), scale = 0).value.get == @(toBE64(100'i64))
    let m2 = initPgMoney(100)
    check toPgParam(some(m2)).value.get == @(toBE64(100'i64))
