import std/[unittest, options, strutils, times, math, net, typetraits]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}

import types_common

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

  test "timestamp with trimmed fractional seconds":
    # PG trims trailing zeros: .500000 -> .5, .123000 -> .123, .100 -> .1
    let cases = {
      "2024-01-15 10:30:00.5": 500_000_000,
      "2024-01-15 10:30:00.12": 120_000_000,
      "2024-01-15 10:30:00.123": 123_000_000,
      "2024-01-15 10:30:00.1234": 123_400_000,
      "2024-01-15 10:30:00.12345": 123_450_000,
      "2024-01-15 10:30:00.123456": 123_456_000,
    }
    for (s, ns) in cases:
      let dt = @[some(toBytes(s))].getTimestamp(0)
      check dt.year == 2024
      check dt.nanosecond == ns

  test "zoneless text decodes as UTC (symmetric with binary)":
    # The wall-clock fields are stored verbatim; the zone label is utc() so the
    # resulting absolute instant matches decodeBinaryTimestamp for the same PG
    # value regardless of the host's local zone.
    let dt = @[some(toBytes("2024-01-15 10:30:00.123456"))].getTimestamp(0)
    check dt.timezone == utc()
    check dt == dateTime(2024, mJan, 15, 10, 30, 0, 123_456_000, utc())
    let dt2 = @[some(toBytes("2024-01-15 10:30:00"))].getTimestamp(0)
    check dt2.timezone == utc()
    check dt2 == dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())

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

  test "text date decodes in UTC, matching the binary path":
    # Regression: the text path parsed with the local zone, so the same date
    # decoded to a different absolute instant than decodeBinaryDate. DateTime
    # equality compares instants, so the two paths compared unequal outside UTC.
    let text = @[some(toBytes("2024-01-15"))].getDate(0)
    check text.utcOffset == 0
    let bin = toPgBinaryDateParam(dateTime(2024, mJan, 15, 0, 0, 0, 0, utc()))
    check text == decodeBinaryDate(bin.value.get)

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

  test "toPgParam rejects out-of-range PgTime fields":
    expect PgTypeError:
      discard toPgParam(PgTime(hour: 99, minute: 0, second: 0))
    expect PgTypeError:
      discard toPgBinaryParam(PgTime(hour: 24, minute: 0, second: 1))
    expect PgTypeError:
      discard toPgParam(PgTimeTz(hour: 25, minute: 0, second: 0, utcOffset: 0))

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

  test "getTime text '24:00:00' end-of-day bound":
    # PostgreSQL allows '24:00:00' as the inclusive upper bound of time-of-day.
    let row = @[some(toBytes("24:00:00"))]
    let t = row.getTime(0)
    check t == PgTime(hour: 24, minute: 0, second: 0, microsecond: 0)

  test "getTime binary '24:00:00' roundtrip":
    let t = PgTime(hour: 24, minute: 0, second: 0, microsecond: 0)
    let p = toPgBinaryParam(t)
    let fields = @[mkField(OidTime, 1)]
    let row = mkRow(@[p.value], fields)
    check row.getTime(0) == t

  test "getTime past end-of-day raises":
    # Nothing past '24:00:00' is valid, in either path.
    for bad in ["24:00:01", "24:01:00", "24:00:00.000001", "25:00:00"]:
      let row = @[some(toBytes(bad))]
      var raised = false
      try:
        discard row.getTime(0)
      except PgTypeError:
        raised = true
      check raised
    # Binary: one microsecond past 24:00:00.
    let pastUs = 86_400_000_001'i64
    let fields = @[mkField(OidTime, 1)]
    let row = mkRow(@[some(@(toBE64(pastUs)))], fields)
    var raised = false
    try:
      discard row.getTime(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTimeOpt NULL returns none":
    let row = @[none(seq[byte])]
    check row.getTimeOpt(0).isNone

  test "getTimeOpt returns some":
    let row = @[some(toBytes("10:30:00"))]
    let opt = row.getTimeOpt(0)
    check opt.isSome
    check opt.get().hour == 10

  test "getTime text trailing garbage raises":
    # Anything after HH:MM:SS other than ".ffffff" must be rejected.
    for bad in ["01:23:45X", "01:23:45XYZ garbage", "01:23:45.", "01:23:45.1234567"]:
      let row = @[some(toBytes(bad))]
      var raised = false
      try:
        discard row.getTime(0)
      except PgTypeError:
        raised = true
      check raised

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

  test "getTimeTz text ±15:59:59 TZDISP_LIMIT inclusive max":
    # PostgreSQL TZDISP_LIMIT is exclusive of ±16h; ±15:59:59 is the last valid.
    let pos = @[some(toBytes("00:00:00+15:59:59"))].getTimeTz(0)
    check pos.utcOffset == 15 * 3600 + 59 * 60 + 59
    let neg = @[some(toBytes("00:00:00-15:59:59"))].getTimeTz(0)
    check neg.utcOffset == -(15 * 3600 + 59 * 60 + 59)

  test "getTimeTz text +16 and +16:00 raise":
    for bad in ["00:00:00+16", "00:00:00+16:00", "00:00:00-16", "00:00:00-16:00:00"]:
      let row = @[some(toBytes(bad))]
      expect PgTypeError:
        discard row.getTimeTz(0)

  test "getTimeTz text minutes/seconds out of 0..59 raise":
    # Total seconds of +00:99 is still inside TZDISP_LIMIT; DecodeTimezone
    # rejects the component anyway.
    for bad in ["00:00:00+00:99", "00:00:00+00:00:60", "00:00:00-01:60"]:
      let row = @[some(toBytes(bad))]
      expect PgTypeError:
        discard row.getTimeTz(0)

  test "getTimeTz text signed offset hour raises":
    # parseInt accepts a leading '-' inside the hour field, so "+-5" would
    # otherwise decode as -5h and "--5" would silently flip the sign to +5h.
    for bad in ["00:00:00+-5", "00:00:00+-5:30", "00:00:00--5", "00:00:00--5:00:00"]:
      let row = @[some(toBytes(bad))]
      expect PgTypeError:
        discard row.getTimeTz(0)

  test "getTimeTz text plus-signed offset components raise":
    # parseInt also accepts a leading '+', so "++5" would decode as +5h and
    # "+05:+3" as 5h03m.
    for bad in [
      "00:00:00++5", "00:00:00++5:30", "00:00:00+-0", "00:00:00+05:+3",
      "00:00:00+05:00:+1",
    ]:
      let row = @[some(toBytes(bad))]
      expect PgTypeError:
        discard row.getTimeTz(0)

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

  test "getTimeTz text '24:00:00' end-of-day bound":
    let row = @[some(toBytes("24:00:00+05"))]
    let t = row.getTimeTz(0)
    check t.hour == 24
    check t.minute == 0
    check t.second == 0
    check t.microsecond == 0
    check t.utcOffset == 18000

  test "getTimeTz binary '24:00:00' roundtrip":
    let t = PgTimeTz(hour: 24, minute: 0, second: 0, microsecond: 0, utcOffset: 18000)
    let p = toPgBinaryParam(t)
    let fields = @[mkField(OidTimeTz, 1)]
    let row = mkRow(@[p.value], fields)
    check row.getTimeTz(0) == t

  test "getTimeTz past end-of-day raises":
    let row = @[some(toBytes("24:00:01+05"))]
    var raised = false
    try:
      discard row.getTimeTz(0)
    except PgTypeError:
      raised = true
    check raised

  test "getTimeTzOpt NULL returns none":
    let row = @[none(seq[byte])]
    check row.getTimeTzOpt(0).isNone

  test "toPgBinaryParam PgTimeTz rejects int32.low utcOffset":
    # Negating int32.low would overflow int32 in the encoder; also outside TZDISP_LIMIT.
    let t = PgTimeTz(hour: 10, minute: 0, second: 0, utcOffset: int32.low)
    expect PgTypeError:
      discard toPgBinaryParam(t)

  test "toPgParam/toPgBinaryParam PgTimeTz reject ±16h utcOffset":
    # Literal boundary, not pgTzDispLimit, so a wrong constant cannot make
    # this rejection test tautological.
    check pgTzDispLimit == 16 * 3600
    let over = PgTimeTz(hour: 10, minute: 0, second: 0, utcOffset: 16 * 3600)
    let under = PgTimeTz(hour: 10, minute: 0, second: 0, utcOffset: -16 * 3600)
    expect PgTypeError:
      discard toPgParam(over)
    expect PgTypeError:
      discard toPgParam(under)
    expect PgTypeError:
      discard toPgBinaryParam(over)
    expect PgTypeError:
      discard toPgBinaryParam(under)

  test "toPgParam/toPgBinaryParam PgTimeTz accept ±15:59:59":
    const maxOff = 15 * 3600 + 59 * 60 + 59
    let pos = PgTimeTz(hour: 10, minute: 0, second: 0, utcOffset: maxOff)
    let neg = PgTimeTz(hour: 10, minute: 0, second: 0, utcOffset: -maxOff)
    check $pos == "10:00:00+15:59:59"
    check $neg == "10:00:00-15:59:59"
    check toPgParam(pos).oid == OidTimeTz
    check toPgBinaryParam(neg).format == 1
    let fields = @[mkField(OidTimeTz, 1)]
    check mkRow(@[toPgBinaryParam(pos).value], fields).getTimeTz(0) == pos
    check mkRow(@[toPgBinaryParam(neg).value], fields).getTimeTz(0) == neg

  test "$PgTimeTz int32.low offset does not OverflowDefect":
    let t = PgTimeTz(hour: 0, minute: 0, second: 0, utcOffset: int32.low)
    discard $t

suite "date parameter encoding":
  test "toPgDateParam OID and format":
    let dt = dateTime(2024, mJan, 15, 0, 0, 0, 0, utc())
    let p = toPgDateParam(dt)
    check p.oid == OidDate
    check p.format == 0
    check p.value.isSome
    let s = cast[string](p.value.get())
    check s == "2024-01-15"

  test "toPgDateParam with non-UTC zone encodes the UTC calendar day":
    # Regression: the text date path used to serialize the DateTime's own wall
    # clock, so a zoned value landed on a different day than toPgBinaryDateParam
    # derived from the same instant. utcOffset counts seconds WEST of UTC, so
    # JST (9h east) has offset -9*3600.
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
    # 05:00 JST is still the previous day in UTC.
    let dt = dateTime(2024, mJan, 15, 5, 0, 0, 0, jst)
    let p = toPgDateParam(dt)
    check p.oid == OidDate
    check toString(p.value.get) == "2024-01-14"
    # The binary encoder already used the UTC day; both must now agree.
    let bin = toPgBinaryDateParam(dt)
    let row = mkRow(@[bin.value], @[mkField(OidDate, 1)])
    let decoded = row.getDate(0)
    check decoded.year == 2024
    check decoded.month == mJan
    check decoded.monthday == 14

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

  test "toPgBinaryDateParam roundtrip before 1970":
    # 1969-12-31 12:00 UTC has a negative Unix time (-43200). Truncating
    # division rounds toward zero (yielding day 0 = 1970-01-01); floor
    # division must round down so the date stays on 1969-12-31.
    let dt = dateTime(1969, mDec, 31, 12, 0, 0, 0, utc())
    let p = toPgBinaryDateParam(dt)
    check p.oid == OidDate
    check p.format == 1
    let fields = @[mkField(OidDate, 1)]
    let row = mkRow(@[p.value], fields)
    let result = row.getDate(0)
    check result.year == 1969
    check result.month == mDec
    check result.monthday == 31

  test "toPgBinaryDateParam drops time of day":
    # Binary date encoding must round down to the day, not round to the
    # nearest day. 23:59:59 on 2024-01-15 must still decode as 2024-01-15.
    let dt = dateTime(2024, mJan, 15, 23, 59, 59, 0, utc())
    let p = toPgBinaryDateParam(dt)
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

  test "timestamptz with trimmed fractional seconds":
    let cases = {
      "2024-01-15 10:30:00.5+00": 500_000_000,
      "2024-01-15 10:30:00.123+00:00": 123_000_000,
      "2024-01-15 10:30:00.1234+05:30": 123_400_000,
    }
    for (s, ns) in cases:
      let dt = @[some(toBytes(s))].getTimestampTz(0)
      check dt.year == 2024
      check dt.nanosecond == ns

  test "zoneless text fallback decodes as UTC":
    # Defensive: PG timestamptz text always carries a zone, but if the fallback
    # zoneless format matches, the label must be utc() rather than the host's
    # local zone (see getTimestamp symmetry test).
    let dt = @[some(toBytes("2024-01-15 10:30:00"))].getTimestampTz(0)
    check dt.timezone == utc()

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

suite "Timestamp/date infinity sentinels":
  # PostgreSQL encodes 'infinity'/'-infinity' as int64.high/int64.low (timestamp)
  # and int32.high/int32.low (date). These are not representable as a Nim
  # DateTime, and the timestamp epoch shift would overflow int64 and raise an
  # uncatchable OverflowDefect — so the decoders must reject them with a
  # catchable PgTypeError instead.
  const
    tsInfinity = @[0x7F'u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF] # int64.high
    tsNegInfinity = @[0x80'u8, 0, 0, 0, 0, 0, 0, 0] # int64.low
    dateInfinity = @[0x7F'u8, 0xFF, 0xFF, 0xFF] # int32.high
    dateNegInfinity = @[0x80'u8, 0, 0, 0] # int32.low

  test "decodeBinaryTimestamp infinity raises (no OverflowDefect)":
    expect PgTypeError:
      discard decodeBinaryTimestamp(tsInfinity)

  test "decodeBinaryTimestamp -infinity raises (no OverflowDefect)":
    expect PgTypeError:
      discard decodeBinaryTimestamp(tsNegInfinity)

  test "decodeBinaryTimestamp near-int64.high raises (no OverflowDefect)":
    # int64.high - 1: not the 'infinity' sentinel, but the epoch shift still
    # overflows int64. Must surface as PgTypeError, not crash.
    const tsNearHigh = @[0x7F'u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]
    expect PgTypeError:
      discard decodeBinaryTimestamp(tsNearHigh)

  test "decodeBinaryTimestamp rejects trailing bytes":
    var buf = @(toBE64(0'i64))
    buf.add(0'u8)
    expect PgTypeError:
      discard decodeBinaryTimestamp(buf)

  test "decodeBinaryArray rejects trailing bytes after empty header":
    # Valid 0-dim header is 12 bytes; an extra byte must not be silent.
    var buf = newSeq[byte](13)
    expect PgTypeError:
      discard decodeBinaryArray(buf)

  test "decodeBinaryTimeTz int32.low offset raises (no OverflowDefect)":
    # us = 0 (00:00:00), offset = int32.low. Un-negating int32.low overflows
    # int32, so the decoder must reject it as PgTypeError.
    const tt = @[0'u8, 0, 0, 0, 0, 0, 0, 0, 0x80'u8, 0, 0, 0] # 8 bytes us=0 + int32.low
    expect PgTypeError:
      discard decodeBinaryTimeTz(tt)

  test "decodeBinaryTimeTz TZDISP_LIMIT exclusive ±16h raises":
    # Wire zone is seconds west of UTC; the bound is symmetric.
    proc timetzBin(zone: int32): seq[byte] =
      result = @(toBE64(0'i64))
      result.add @(toBE32(zone))

    expect PgTypeError:
      discard decodeBinaryTimeTz(timetzBin(16 * 3600))
    expect PgTypeError:
      discard decodeBinaryTimeTz(timetzBin(-16 * 3600))
    let maxOff = int32(15 * 3600 + 59 * 60 + 59)
    check decodeBinaryTimeTz(timetzBin(-maxOff)).utcOffset == maxOff
    check decodeBinaryTimeTz(timetzBin(maxOff)).utcOffset == -maxOff

  test "decodeBinaryDate infinity raises":
    expect PgTypeError:
      discard decodeBinaryDate(dateInfinity)

  test "decodeBinaryDate -infinity raises":
    expect PgTypeError:
      discard decodeBinaryDate(dateNegInfinity)

  test "getTimestamp binary infinity raises":
    let fields = @[mkField(OidTimestamp, 1)]
    let row = mkRow(@[some(tsInfinity)], fields)
    expect PgTypeError:
      discard row.getTimestamp(0)

  test "getTimestampTz binary -infinity raises":
    let fields = @[mkField(OidTimestampTz, 1)]
    let row = mkRow(@[some(tsNegInfinity)], fields)
    expect PgTypeError:
      discard row.getTimestampTz(0)

  test "getDate binary infinity raises":
    let fields = @[mkField(OidDate, 1)]
    let row = mkRow(@[some(dateInfinity)], fields)
    expect PgTypeError:
      discard row.getDate(0)

  test "parseTimestampText infinity raises":
    expect PgTypeError:
      discard parseTimestampText("infinity")

  test "parseTimestampText -infinity raises":
    expect PgTypeError:
      discard parseTimestampText("-infinity")

  test "parseTimestampText invalid format raises PgTypeError (catchable via PgError)":
    for bad in ["not-a-timestamp", "2026-13-99", "2026-02-30T25:61:61", ""]:
      var raisedAsPgType = false
      var raisedAsPgError = false
      try:
        discard parseTimestampText(bad)
      except PgTypeError:
        raisedAsPgType = true
      except PgError:
        discard
      try:
        discard parseTimestampText(bad)
      except PgError:
        raisedAsPgError = true
      check raisedAsPgType
      check raisedAsPgError

  test "parseDateText invalid format raises PgTypeError (catchable via PgError)":
    for bad in ["not-a-date", "2026-13-01", "2026-02-30", ""]:
      var raisedAsPgType = false
      var raisedAsPgError = false
      try:
        discard parseDateText(bad)
      except PgTypeError:
        raisedAsPgType = true
      except PgError:
        discard
      try:
        discard parseDateText(bad)
      except PgError:
        raisedAsPgError = true
      check raisedAsPgType
      check raisedAsPgError

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

  test "getStr binary text/varchar/bytea raw-copy":
    let fields = @[mkField(OidText, 1)]
    let row = mkRow(@[some(toBytes("hello"))], fields)
    check row.getStr(0) == "hello"

  test "getStr binary unsupported binary-safe OID raises":
    let fields = @[mkField(OidTimestamp, 1)]
    let row = mkRow(@[some(@[0'u8, 0, 0, 0, 0, 0, 0, 0])], fields)
    expect PgTypeError:
      discard row.getStr(0)

  test "getStr binary user-defined OID raw-copy":
    let fields = @[mkField(99999'i32, 1)]
    let row = mkRow(@[some(toBytes("active"))], fields)
    check row.getStr(0) == "active"

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

  test "getIntArrayElemOpt binary element length mismatch":
    let elements = @[@[0'u8, 0, 0]] # 3 bytes (expected 4)
    let bin = encodeBinaryArray(OidInt4, elements)
    let fields = @[mkField(OidInt4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getIntArrayElemOpt(0)

  test "getInt16ArrayElemOpt binary element length mismatch":
    let elements = @[@[0'u8, 0, 0]] # 3 bytes (expected 2)
    let bin = encodeBinaryArray(OidInt2, elements)
    let fields = @[mkField(OidInt2Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getInt16ArrayElemOpt(0)

  test "getInt64ArrayElemOpt binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0]] # 4 bytes (expected 8)
    let bin = encodeBinaryArray(OidInt8, elements)
    let fields = @[mkField(OidInt8Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getInt64ArrayElemOpt(0)

  test "getFloatArrayElemOpt binary element length mismatch":
    let elements = @[@[0'u8, 0, 0]] # 3 bytes (expected 4 or 8)
    let bin = encodeBinaryArray(OidFloat8, elements)
    let fields = @[mkField(OidFloat8Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getFloatArrayElemOpt(0)

  test "getFloat32ArrayElemOpt binary element length mismatch":
    let elements = @[@[0'u8, 0]] # 2 bytes (expected 4)
    let bin = encodeBinaryArray(OidFloat4, elements)
    let fields = @[mkField(OidFloat4Array, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getFloat32ArrayElemOpt(0)

  test "getBoolArrayElemOpt binary element length mismatch":
    let elements = @[@[1'u8, 1]] # 2 bytes (expected 1)
    let bin = encodeBinaryArray(OidBool, elements)
    let fields = @[mkField(OidBoolArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getBoolArrayElemOpt(0)

  test "getTimestampArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0]] # 4 bytes (expected 8)
    let bin = encodeBinaryArray(OidTimestamp, elements)
    let fields = @[mkField(OidTimestampArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getTimestampArray(0)

  test "getTimestampTzArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0]] # 4 bytes (expected 8)
    let bin = encodeBinaryArray(OidTimestampTz, elements)
    let fields = @[mkField(OidTimestampTzArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getTimestampTzArray(0)

  test "getDateArray binary element length mismatch":
    let elements = @[@[0'u8, 0]] # 2 bytes (expected 4)
    let bin = encodeBinaryArray(OidDate, elements)
    let fields = @[mkField(OidDateArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getDateArray(0)

  test "getTimeArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0]] # 4 bytes (expected 8)
    let bin = encodeBinaryArray(OidTime, elements)
    let fields = @[mkField(OidTimeArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getTimeArray(0)

  test "getTimeTzArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0, 0, 0, 0, 0]] # 8 bytes (expected 12)
    let bin = encodeBinaryArray(OidTimeTz, elements)
    let fields = @[mkField(OidTimeTzArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getTimeTzArray(0)

  test "getPointArray binary element length mismatch":
    let elements = @[@[0'u8, 0, 0, 0, 0, 0, 0, 0]] # 8 bytes (expected 16)
    let bin = encodeBinaryArray(OidPoint, elements)
    let fields = @[mkField(OidPointArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getPointArray(0)

  test "getLsegArray binary element length mismatch":
    let elements = @[newSeq[byte](16)] # 16 bytes (expected 32)
    let bin = encodeBinaryArray(OidLseg, elements)
    let fields = @[mkField(OidLsegArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getLsegArray(0)

  test "getBoxArray binary element length mismatch":
    let elements = @[newSeq[byte](16)] # 16 bytes (expected 32)
    let bin = encodeBinaryArray(OidBox, elements)
    let fields = @[mkField(OidBoxArray, 1)]
    let row = mkRow(@[some(bin)], fields)
    expect PgTypeError:
      discard row.getBoxArray(0)

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
    # |int64.low| = 9223372036854775808us = 2562047788h + 54775808us
    # = 2562047788:00:54.775808. Clamping to int64.high would lose 1us
    # and print .775807 instead.
    check s == "-2562047788:00:54.775808"

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

  test "parseIntervalText malformed raises":
    # Bare garbage, unknown units, bare number, and non-alnum bytes must all
    # raise. "!!" previously spun the parser in an infinite loop.
    for bad in ["junk", "5 fortnights", "1", "!!", "-", "3 days garbage"]:
      var raised = false
      try:
        discard parseIntervalText(bad)
      except PgTypeError:
        raised = true
      check raised

  test "parseIntervalText overflow raises PgTypeError (not Defect)":
    # A malicious/broken server sending oversized numeric fields must fail
    # with a catchable PgTypeError rather than crashing with OverflowDefect
    # or RangeDefect (or silently wrapping in release builds).
    let bads = [
      "999999999999999999999 days", # digit accumulation overflows int64
      "3000000000 years", # 3e9 * 12 overflows int32 months
      "3000000000 mons", # overflows int32 months
      "3000000000 days", # overflows int32 days
      "99999999999999999999:00:00", # hours accumulation overflows int64
      "3000000000000:00:00", # hours * 3_600_000_000 overflows int64
    ]
    for bad in bads:
      var raised = false
      try:
        discard parseIntervalText(bad)
      except PgTypeError:
        raised = true
      check raised

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
