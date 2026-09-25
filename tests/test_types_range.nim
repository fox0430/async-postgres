import std/[unittest, options, strutils, times, math, net, typetraits]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}
import ../async_postgres/pg_types/ranges {.all.}

import types_common

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

  test "reject missing lower boundary bracket":
    # "[1,2" would previously silently drop the upper element.
    expect PgTypeError:
      discard parseRangeText[int32](
        "[1,2",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "reject missing upper boundary bracket":
    expect PgTypeError:
      discard parseRangeText[int32](
        "1,2]",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "reject non-bracket boundary chars":
    expect PgTypeError:
      discard parseRangeText[int32](
        "X1,2Y",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "reject unterminated quoted lower element":
    # `["abc,def)` — closing quote missing; previous code silently dropped
    # trailing bytes and mis-parsed the upper bound.
    expect PgTypeError:
      discard parseRangeText[string](
        "[\"abc,def)",
        proc(s: string): string =
          s,
      )

  test "reject trailing bytes after quoted element":
    # `["abc"xyz,def)` — the caller previously discarded `pos` from
    # parseRangeElem so `xyz` was silently dropped.
    expect PgTypeError:
      discard parseRangeText[string](
        "[\"abc\"xyz,def)",
        proc(s: string): string =
          s,
      )

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
    check p.value.get.toString ==
      "[\"2023-01-01 00:00:00.000000\",\"2023-12-31 00:00:00.000000\")"

  test "tsrange text preserves microseconds":
    let dt1 = dateTime(2023, mJan, 1, 12, 34, 56, 789_000, utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let p = toPgParam(rangeOf(dt1, dt2))
    let row: Row = @[p.value]
    let decoded = row.getTsRange(0)
    check decoded.lower.value.nanosecond == 789_000

  test "tsrange with non-UTC zone encodes the UTC instant":
    # Regression: the tsrange text path used to serialize the local wall clock,
    # diverging from scalar toPgParam(DateTime). utcOffset counts seconds WEST
    # of UTC, so JST (9h east) has offset -9*3600.
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
    let p = toPgParam(rangeOf(dt1, dt2))
    check p.oid == OidTsRange
    check p.value.get.toString ==
      "[\"2026-07-15 12:00:00.000000\",\"2026-07-16 00:00:00.000000\")"
    let ap = toPgParam(@[rangeOf(dt1, dt2)])
    check ap.oid == OidTsRangeArray
    check ap.value.get.toString ==
      "{\"[\\\"2026-07-15 12:00:00.000000\\\",\\\"2026-07-16 00:00:00.000000\\\")\"}"

  test "tstzrange":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgTsTzRangeParam(rangeOf(dt1, dt2))
    check p.oid == OidTsTzRange
    check p.value.get.toString ==
      "[\"2023-01-01 00:00:00.000000Z\",\"2023-12-31 00:00:00.000000Z\")"

  test "tstzmultirange":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgTsTzMultirangeParam(toMultirange(rangeOf(dt1, dt2)))
    check p.oid == OidTsTzMultirange
    check p.value.get.toString ==
      "{[\"2023-01-01 00:00:00.000000Z\",\"2023-12-31 00:00:00.000000Z\")}"

  test "daterange":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let p = toPgDateRangeParam(rangeOf(dt1, dt2))
    check p.oid == OidDateRange
    check p.value.get.toString == "[2023-01-01,2023-12-31)"

  test "date ranges with non-UTC zone encode the UTC calendar day":
    # Regression: every daterange text encoder (range, array, multirange,
    # multirange array) serialized the DateTime's own wall clock, landing on a
    # different day than the binary pgDateDays path. utcOffset counts seconds
    # WEST of UTC, so JST (9h east) has offset -9*3600.
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
    # Both bounds are at 05:00 JST, still the previous day in UTC.
    let dt1 = dateTime(2024, mJan, 15, 5, 0, 0, 0, jst)
    let dt2 = dateTime(2024, mJan, 20, 5, 0, 0, 0, jst)
    let r = rangeOf(dt1, dt2)

    let p = toPgDateRangeParam(r)
    check p.oid == OidDateRange
    check p.value.get.toString == "[2024-01-14,2024-01-19)"

    let ap = toPgDateRangeArrayParam(@[r])
    check ap.oid == OidDateRangeArray
    check ap.value.get.toString == "{\"[2024-01-14,2024-01-19)\"}"

    let mp = toPgDateMultirangeParam(toMultirange(r))
    check mp.oid == OidDateMultirange
    check mp.value.get.toString == "{[2024-01-14,2024-01-19)}"

    let map = toPgDateMultirangeArrayParam(@[toMultirange(r)])
    check map.oid == OidDateMultirangeArray
    check map.value.get.toString == "{\"{[2024-01-14,2024-01-19)}\"}"

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
    check (data[0] and rangeLbInf) == 0
    check (data[0] and rangeUbInf) == 0
    check (data[0] and rangeLbInc) != 0
    check (data[0] and rangeUbInc) == 0
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
    check (data[0] and rangeLbInf) != 0
    check (data[0] and rangeUbInf) == 0

  test "unbounded upper int4range binary":
    let p = toPgBinaryParam(rangeFrom[int32](5'i32))
    let data = p.value.get
    check (data[0] and rangeLbInf) == 0
    check (data[0] and rangeUbInf) != 0

suite "Range flag bits match PostgreSQL's rangetypes.h":
  ## A round trip through our own encoder and decoder stays green for any
  ## self-consistent bit assignment, which is how an incompatible one survived:
  ## the bits only matter against a real server. Pin the literal values.

  test "the constants are the values PostgreSQL defines":
    check rangeEmpty == 0x01'u8
    check rangeLbInc == 0x02'u8
    check rangeUbInc == 0x04'u8
    check rangeLbInf == 0x08'u8
    check rangeUbInf == 0x10'u8
    check rangeContainEmpty == 0x80'u8

  test "`[1,10)` sends LB_INC alone":
    let data = toPgBinaryParam(rangeOf(1'i32, 10'i32)).value.get
    check data[0] == 0x02'u8

  test "`[1,10]` sends LB_INC and UB_INC":
    let data = toPgBinaryParam(rangeOf(1'i32, 10'i32, upperInc = true)).value.get
    check data[0] == 0x06'u8

  test "an absent bound is spelled as its infinity bit, not as a missing one":
    check toPgBinaryParam(rangeTo[int32](10'i32)).value.get[0] == 0x08'u8
    check toPgBinaryParam(rangeFrom[int32](5'i32)).value.get[0] == 0x12'u8
    check toPgBinaryParam(unboundedRange[int32]()).value.get[0] == 0x18'u8

  test "an infinite bound carries no inclusivity bit":
    # PostgreSQL clears LB_INC/UB_INC for an infinite bound; setting both would
    # make the server read a bound that was never written.
    check (
      toPgBinaryParam(rangeFrom[int32](5'i32, inclusive = true)).value.get[0] and
      rangeUbInc
    ) == 0
    check (
      toPgBinaryParam(rangeTo[int32](10'i32, inclusive = true)).value.get[0] and
      rangeLbInc
    ) == 0

  test "an empty range sends the EMPTY bit alone":
    check toPgBinaryParam(emptyRange[int32]()).value.get == @[0x01'u8]

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

  test "daterange roundtrip before 1970":
    # Lower bound 1969-12-31 12:00 UTC has a negative Unix time. Floor
    # division must keep the encoded day on 1969-12-31 instead of rounding
    # toward zero into 1970-01-01.
    let dt1 = dateTime(1969, mDec, 31, 12, zone = utc())
    let dt2 = dateTime(1970, mJan, 2, zone = utc())
    let orig = rangeOf(dt1, dt2)
    let p = toPgBinaryDateRangeParam(orig)
    let fields = @[mkField(OidDateRange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getDateRange(0)
    check decoded.hasLower == true
    check decoded.lower.value.year == 1969
    check decoded.lower.value.month == mDec
    check decoded.lower.value.monthday == 31

suite "Range binary decoding rejects malformed bLen":
  # Fixed-width range decoders must validate the per-bound length field
  # matches the type's element size instead of blindly slicing a hardcoded
  # window, which would spill into adjacent bytes on malicious/corrupt input.
  test "int4range rejects short lower bLen":
    var data = @[rangeLbInc]
    data.add(toBE32(2'i32)) # bogus: int4 must be 4 bytes
    data.add([0'u8, 0])
    data.add(toBE32(4'i32))
    data.add(toBE32(10'i32))
    expect PgTypeError:
      discard decodeInt4RangeBinary(data)

  test "int4range rejects oversized upper bLen":
    var data = @[rangeLbInc]
    data.add(toBE32(4'i32))
    data.add(toBE32(1'i32))
    data.add(toBE32(8'i32)) # bogus: int4 must be 4 bytes
    data.add(toBE64(10'i64))
    expect PgTypeError:
      discard decodeInt4RangeBinary(data)

  test "int8range rejects wrong bLen":
    var data = @[rangeLbInc]
    data.add(toBE32(4'i32)) # bogus: int8 must be 8 bytes
    data.add(toBE32(1'i32))
    data.add(toBE32(8'i32))
    data.add(toBE64(10'i64))
    expect PgTypeError:
      discard decodeInt8RangeBinary(data)

  test "tsrange rejects wrong bLen":
    var data = @[rangeLbInc]
    data.add(toBE32(4'i32)) # bogus: timestamp must be 8 bytes
    data.add(toBE32(0'i32))
    data.add(toBE32(8'i32))
    data.add(toBE64(0'i64))
    expect PgTypeError:
      discard decodeTsRangeBinary(data)

  test "daterange rejects wrong bLen":
    var data = @[rangeLbInc]
    data.add(toBE32(8'i32)) # bogus: date must be 4 bytes
    data.add(toBE64(0'i64))
    data.add(toBE32(4'i32))
    data.add(toBE32(0'i32))
    expect PgTypeError:
      discard decodeDateRangeBinary(data)

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

  test "toPgParam tsmultirange with non-UTC zone encodes the UTC instant":
    # Regression: the ts multirange text path used to serialize the local wall
    # clock (via $), diverging from scalar toPgParam(DateTime). utcOffset counts
    # seconds WEST of UTC, so JST (9h east) has offset -9*3600.
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
    let p = toPgParam(toMultirange(rangeOf(dt1, dt2)))
    check p.oid == OidTsMultirange
    check p.value.get.toString ==
      "{[\"2026-07-15 12:00:00.000000\",\"2026-07-16 00:00:00.000000\")}"

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

  test "parse multirange with spaces after comma":
    let mr = parseMultirangeText[int32](
      "{[1,2), [3,4)}",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check mr.len == 2
    check mr[0] == rangeOf(1'i32, 2'i32)
    check mr[1] == rangeOf(3'i32, 4'i32)

  test "parse multirange with empty and spaces":
    let mr = parseMultirangeText[int32](
      "{empty, [3,4)}",
      proc(s: string): int32 =
        int32(parseInt(s)),
    )
    check mr.len == 2
    check mr[0].isEmpty
    check mr[1] == rangeOf(3'i32, 4'i32)

  test "reject stray closing bracket":
    # `{1,2)}` previously produced an empty multirange silently.
    expect PgTypeError:
      discard parseMultirangeText[int32](
        "{1,2)}",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "reject trailing bytes after last range":
    expect PgTypeError:
      discard parseMultirangeText[int32](
        "{[1,2)garbage}",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "reject trailing comma after empty":
    expect PgTypeError:
      discard parseMultirangeText[int32](
        "{empty,}",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "reject 'empty' followed by non-comma":
    # `{emptyx}` — prefix-matched "empty" then failed the delimiter check.
    expect PgTypeError:
      discard parseMultirangeText[int32](
        "{emptyx}",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "reject unterminated range":
    expect PgTypeError:
      discard parseMultirangeText[int32](
        "{[1,2}",
        proc(s: string): int32 =
          int32(parseInt(s)),
      )

  test "quoted element containing closing bracket is not mis-split":
    # `{["a]b",c)}` — bracket inside a quoted element must not affect nesting.
    let mr = parseMultirangeText[string](
      "{[\"a]b\",c)}",
      proc(s: string): string =
        s,
    )
    check mr.len == 1
    check mr[0].lower.value == "a]b"
    check mr[0].upper.value == "c"

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

  test "tstzmultirange binary roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let orig = toMultirange(rangeOf(dt1, dt2))
    let p = toPgBinaryTsTzMultirangeParam(orig)
    check p.oid == OidTsTzMultirange
    check p.format == 1'i16
    let fields = @[mkField(OidTsTzMultirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getTsTzMultirange(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan
    check decoded[0].upper.value.month == mJun

  test "datemultirange binary roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let orig = toMultirange(rangeOf(dt1, dt2))
    let p = toPgBinaryDateMultirangeParam(orig)
    check p.oid == OidDateMultirange
    check p.format == 1'i16
    let fields = @[mkField(OidDateMultirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getDateMultirange(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan
    check decoded[0].upper.value.month == mDec

  test "datemultirange binary roundtrip before 1970":
    # Lower bound 1969-12-31 12:00 UTC has a negative Unix time. Floor
    # division must keep the encoded day on 1969-12-31 instead of rounding
    # toward zero into 1970-01-01.
    let dt1 = dateTime(1969, mDec, 31, 12, zone = utc())
    let dt2 = dateTime(1970, mJan, 2, zone = utc())
    let orig = toMultirange(rangeOf(dt1, dt2))
    let p = toPgBinaryDateMultirangeParam(orig)
    let fields = @[mkField(OidDateMultirange, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getDateMultirange(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 1969
    check decoded[0].lower.value.month == mDec
    check decoded[0].lower.value.monthday == 31
    check decoded[0].upper.value.year == 1970
    check decoded[0].upper.value.month == mJan
    check decoded[0].upper.value.monthday == 2

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
    check decoded[0].upper.value.month == mJun
    check decoded[1].lower.value.month == mJul
    check decoded[1].upper.value.month == mDec
    check decoded[1].upper.value.monthday == 31

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
    check decoded[0].lower.value.month == mJan
    check decoded[0].upper.value.year == 2023
    check decoded[0].upper.value.month == mJun

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
    check decoded[0].lower.value.monthday == 1
    check decoded[0].upper.value.year == 2023
    check decoded[0].upper.value.month == mDec
    check decoded[0].upper.value.monthday == 31

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

suite "Range array text roundtrip":
  test "int4range[] text roundtrip":
    let orig = @[rangeOf(1'i32, 10'i32), rangeOf(20'i32, 30'i32)]
    let p = toPgParam(orig)
    check p.oid == OidInt4RangeArray
    check p.format == 0'i16
    let row: Row = @[p.value]
    let decoded = row.getInt4RangeArray(0)
    check decoded == orig

  test "int8range[] text roundtrip":
    let orig = @[rangeOf(100'i64, 200'i64), rangeOf(300'i64, 400'i64)]
    let p = toPgParam(orig)
    check p.oid == OidInt8RangeArray
    let row: Row = @[p.value]
    let decoded = row.getInt8RangeArray(0)
    check decoded == orig

  test "numrange[] text roundtrip":
    let orig = @[
      rangeOf(parsePgNumeric("1.5"), parsePgNumeric("9.5")),
      rangeOf(parsePgNumeric("10.0"), parsePgNumeric("20.0")),
    ]
    let p = toPgParam(orig)
    check p.oid == OidNumRangeArray
    let row: Row = @[p.value]
    let decoded = row.getNumRangeArray(0)
    check decoded == orig

  test "tsrange[] text roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let orig = @[rangeOf(dt1, dt2)]
    let p = toPgParam(orig)
    check p.oid == OidTsRangeArray
    let row: Row = @[p.value]
    let decoded = row.getTsRangeArray(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan
    check decoded[0].upper.value.year == 2023
    check decoded[0].upper.value.month == mJun

  test "tstzrange[] text roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let orig = @[rangeOf(dt1, dt2)]
    let p = toPgTsTzRangeArrayParam(orig)
    check p.oid == OidTsTzRangeArray
    check p.format == 0'i16
    let row: Row = @[p.value]
    let decoded = row.getTsTzRangeArray(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan
    check decoded[0].upper.value.year == 2023
    check decoded[0].upper.value.month == mJun

  test "daterange[] text roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let orig = @[rangeOf(dt1, dt2)]
    let p = toPgDateRangeArrayParam(orig)
    check p.oid == OidDateRangeArray
    check p.format == 0'i16
    let row: Row = @[p.value]
    let decoded = row.getDateRangeArray(0)
    check decoded.len == 1
    check decoded[0].lower.value.year == 2023
    check decoded[0].lower.value.month == mJan
    check decoded[0].lower.value.monthday == 1
    check decoded[0].upper.value.year == 2023
    check decoded[0].upper.value.month == mDec
    check decoded[0].upper.value.monthday == 31

  test "int4range[] text with empty range element":
    let orig = @[rangeOf(1'i32, 10'i32), emptyRange[int32]()]
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = row.getInt4RangeArray(0)
    check decoded.len == 2
    check decoded[0] == rangeOf(1'i32, 10'i32)
    check decoded[1].isEmpty == true

suite "Multirange array binary roundtrip":
  test "int4multirange[] roundtrip":
    let orig = @[
      toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32)),
      toMultirange(rangeOf(10'i32, 20'i32)),
    ]
    let p = toPgBinaryParam(orig)
    check p.oid == OidInt4MultirangeArray
    check p.format == 1'i16
    let fields = @[mkField(OidInt4MultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4MultirangeArray(0)
    check decoded == orig

  test "int8multirange[] roundtrip":
    let orig = @[
      toMultirange(rangeOf(100'i64, 200'i64)),
      toMultirange(rangeOf(300'i64, 400'i64), rangeOf(500'i64, 600'i64)),
    ]
    let p = toPgBinaryParam(orig)
    check p.oid == OidInt8MultirangeArray
    let fields = @[mkField(OidInt8MultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt8MultirangeArray(0)
    check decoded == orig

  test "nummultirange[] roundtrip":
    let orig = @[toMultirange(rangeOf(parsePgNumeric("1.5"), parsePgNumeric("9.5")))]
    let p = toPgBinaryParam(orig)
    check p.oid == OidNumMultirangeArray
    let fields = @[mkField(OidNumMultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getNumMultirangeArray(0)
    check decoded == orig

  test "tsmultirange[] roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let orig = @[toMultirange(rangeOf(dt1, dt2))]
    let p = toPgBinaryParam(orig)
    check p.oid == OidTsMultirangeArray
    let fields = @[mkField(OidTsMultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getTsMultirangeArray(0)
    check decoded.len == 1
    check decoded[0].len == 1
    check decoded[0][0].lower.value.year == 2023
    check decoded[0][0].lower.value.month == mJan
    check decoded[0][0].upper.value.month == mJun

  test "tstzmultirange[] roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let orig = @[toMultirange(rangeOf(dt1, dt2))]
    let p = toPgBinaryTsTzMultirangeArrayParam(orig)
    check p.oid == OidTsTzMultirangeArray
    let fields = @[mkField(OidTsTzMultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getTsTzMultirangeArray(0)
    check decoded.len == 1
    check decoded[0].len == 1
    check decoded[0][0].lower.value.year == 2023
    check decoded[0][0].lower.value.month == mJan
    check decoded[0][0].upper.value.month == mJun

  test "datemultirange[] roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mDec, 31, zone = utc())
    let orig = @[toMultirange(rangeOf(dt1, dt2))]
    let p = toPgBinaryDateMultirangeArrayParam(orig)
    check p.oid == OidDateMultirangeArray
    let fields = @[mkField(OidDateMultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getDateMultirangeArray(0)
    check decoded.len == 1
    check decoded[0].len == 1
    check decoded[0][0].lower.value.year == 2023
    check decoded[0][0].lower.value.month == mJan
    check decoded[0][0].upper.value.month == mDec

  test "empty int4multirange[] roundtrip":
    let orig: seq[PgMultirange[int32]] = @[]
    let p = toPgBinaryParam(orig)
    check p.oid == OidInt4MultirangeArray
    let fields = @[mkField(OidInt4MultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4MultirangeArray(0)
    check decoded.len == 0

  test "int4multirange[] with empty multirange element":
    let orig = @[toMultirange(rangeOf(1'i32, 10'i32)), toMultirange[int32]()]
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidInt4MultirangeArray, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getInt4MultirangeArray(0)
    check decoded.len == 2
    check decoded[0] == toMultirange(rangeOf(1'i32, 10'i32))
    check decoded[1].len == 0

suite "Multirange array text roundtrip":
  test "tstzmultirange[] text roundtrip":
    let dt1 = dateTime(2023, mJan, 1, zone = utc())
    let dt2 = dateTime(2023, mJun, 1, zone = utc())
    let orig = @[toMultirange(rangeOf(dt1, dt2))]
    let p = toPgTsTzMultirangeArrayParam(orig)
    check p.oid == OidTsTzMultirangeArray
    check p.format == 0'i16
    let row: Row = @[p.value]
    let decoded = row.getTsTzMultirangeArray(0)
    check decoded.len == 1
    check decoded[0].len == 1
    check decoded[0][0].lower.value.year == 2023
    check decoded[0][0].lower.value.month == mJan
    check decoded[0][0].upper.value.year == 2023
    check decoded[0][0].upper.value.month == mJun

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
