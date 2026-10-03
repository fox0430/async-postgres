import std/[options, strutils, tables, times, net]

import ../pg_bytes
import core, array, encoding

export pg_bytes, array

type TsPrec = enum
  # Order = ascending precedence; `!` binds tightest, `|` loosest.
  tpOr
  tpAnd
  tpPhrase
  tpNot
  tpOperand

proc ensureNoTrailing*(pos, total: int, what: string) {.inline.} =
  ## Reject trailing bytes after a binary value.
  if pos != total:
    raise
      newException(PgTypeError, what & ": trailing data (" & $(total - pos) & " bytes)")

proc decodeHstoreBinary*(data: openArray[byte]): PgHstore {.raises: [PgError].} =
  ## Decode PostgreSQL binary hstore format.
  result = initTable[string, Option[string]]()
  if data.len < 4:
    raise newException(PgTypeError, "hstore binary data too short")

  let numPairs = int(fromBE32(data.toOpenArray(0, 3)))
  if numPairs < 0:
    raise
      newException(PgTypeError, "hstore binary: invalid number of pairs " & $numPairs)
  # Each pair carries at least keyLen (4) + valLen (4) = 8 bytes after the
  # 4-byte count, so numPairs cannot exceed (data.len - 4) div 8. Matches the
  # upfront guard in decodeBinaryComposite / decodeBinaryTsVector.
  if numPairs > (data.len - 4) div 8:
    raise newException(PgTypeError, "hstore binary: pair count exceeds data")

  var pos = 4
  for _ in 0 ..< numPairs:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "hstore binary: truncated key length")
    let keyLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if keyLen < 0 or pos + keyLen > data.len:
      raise newException(PgTypeError, "hstore binary: truncated key data")
    let key = readString(data, pos, keyLen)
    pos += keyLen
    if pos + 4 > data.len:
      raise newException(PgTypeError, "hstore binary: truncated value length")
    let valLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if valLen == -1:
      result[key] = none(string)
    else:
      if valLen < 0 or pos + valLen > data.len:
        raise newException(PgTypeError, "hstore binary: truncated value data")
      let val = readString(data, pos, valLen)
      pos += valLen
      result[key] = some(val)
  ensureNoTrailing(pos, data.len, "hstore binary")

proc fromPgText*(data: seq[byte], oid: int32): string {.raises: [].} =
  ## Convert text-format bytes from PostgreSQL to a Nim string.
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

# Binary decoders needed by both basic and format-aware row accessors.

proc decodeNumericBinary*(data: openArray[byte]): PgNumeric {.raises: [PgError].} =
  ## Decode PostgreSQL binary numeric format into PgNumeric.
  if data.len < 8:
    raise newException(PgTypeError, "Numeric binary data too short: " & $data.len)
  let ndigits = int(fromBE16(data.toOpenArray(0, 1)))
  if ndigits < 0:
    raise newException(PgTypeError, "Numeric binary: invalid ndigits " & $ndigits)
  let weight = int16(fromBE16(data.toOpenArray(2, 3)))
  let signRaw = uint16(fromBE16(data.toOpenArray(4, 5)))
  let dscale = int16(fromBE16(data.toOpenArray(6, 7)))
  let sign =
    case signRaw
    of 0x0000'u16:
      pgPositive
    of 0x4000'u16:
      pgNegative
    of 0xC000'u16:
      pgNaN
    else:
      raise newException(PgTypeError, "Invalid numeric sign: " & $signRaw)
  let expectedLen = 8 + ndigits * 2
  if data.len != expectedLen:
    raise newException(
      PgTypeError,
      "Numeric binary: expected length " & $expectedLen & " got " & $data.len,
    )
  if sign == pgNaN:
    return PgNumeric(sign: pgNaN)
  var digits = newSeq[int16](ndigits)
  for i in 0 ..< ndigits:
    let d = fromBE16(data.toOpenArray(8 + i * 2, 9 + i * 2))
    # Enforce PgNumeric.digits invariant (each 0..9999). Out-of-range values
    # would otherwise silently corrupt `$PgNumeric` and cmp results.
    if d < 0 or d > 9999:
      raise newException(PgTypeError, "Numeric binary: invalid digit " & $d)
    digits[i] = d
  PgNumeric(weight: weight, sign: sign, dscale: dscale, digits: digits)

proc decodeBinaryTimestamp*(data: openArray[byte]): DateTime {.raises: [PgError].} =
  if data.len != 8:
    raise
      newException(PgTypeError, "Binary timestamp: expected 8 bytes, got " & $data.len)
  let pgUs = fromBE64(data)
  # PostgreSQL encodes timestamp/timestamptz 'infinity'/'-infinity' as
  # int64.high/int64.low microseconds since 2000-01-01. Nim's DateTime cannot
  # represent these, and the epoch shift below would overflow int64 (raising an
  # uncatchable OverflowDefect), so reject them with a catchable PgTypeError.
  if pgUs == int64.high:
    raise newException(
      PgTypeError, "Binary timestamp is 'infinity', not representable as a DateTime"
    )
  if pgUs == int64.low:
    raise newException(
      PgTypeError, "Binary timestamp is '-infinity', not representable as a DateTime"
    )
  # Non-sentinel values within ``pgEpochUs`` of ``int64.high`` are not the
  # 'infinity' marker but still overflow the epoch shift below; reject them as a
  # catchable PgTypeError rather than crash with an uncatchable OverflowDefect.
  # Adding the positive ``pgEpochUs`` can only overflow at the high end, so no
  # symmetric low-end guard is needed.
  const pgEpochUs = pgEpochUnix * 1_000_000
  if pgUs > int64.high - pgEpochUs:
    raise
      newException(PgTypeError, "Binary timestamp out of representable range: " & $pgUs)
  let unixUs = pgUs + pgEpochUs
  var unixSec = unixUs div 1_000_000
  var fracUs = unixUs mod 1_000_000
  if fracUs < 0:
    unixSec -= 1
    fracUs += 1_000_000
  initTime(unixSec, int(fracUs * 1000)).utc()

proc decodeBinaryDate*(data: openArray[byte]): DateTime {.raises: [PgError].} =
  if data.len != 4:
    raise newException(PgTypeError, "Binary date: expected 4 bytes, got " & $data.len)
  let pgDays = fromBE32(data)
  # PostgreSQL encodes date 'infinity'/'-infinity' as int32.high/int32.low days
  # since 2000-01-01. DateTime cannot represent these (they would otherwise
  # decode to a meaningless far-future/past date), so reject them explicitly.
  if pgDays == int32.high:
    raise newException(
      PgTypeError, "Binary date is 'infinity', not representable as a DateTime"
    )
  if pgDays == int32.low:
    raise newException(
      PgTypeError, "Binary date is '-infinity', not representable as a DateTime"
    )
  let unixSec = (int64(pgDays) + int64(pgEpochDaysOffset)) * 86400
  initTime(unixSec, 0).utc()

const pgTimeMaxUs = 86_400_000_000'i64
  ## Microseconds for '24:00:00', PostgreSQL's inclusive end-of-day bound for
  ## time-of-day. Valid range is [0, pgTimeMaxUs]; '24:00:00' itself is allowed
  ## but nothing past it.

proc decodeBinaryTime*(data: openArray[byte]): PgTime {.raises: [PgError].} =
  if data.len != 8:
    raise newException(PgTypeError, "Binary time: expected 8 bytes, got " & $data.len)
  let us = fromBE64(data)
  if us < 0 or us > pgTimeMaxUs:
    raise newException(PgTypeError, "Binary time: microseconds out of range " & $us)
  let hours = int32(us div 3_600_000_000)
  let rem1 = us mod 3_600_000_000
  let minutes = int32(rem1 div 60_000_000)
  let rem2 = rem1 mod 60_000_000
  let seconds = int32(rem2 div 1_000_000)
  let microseconds = int32(rem2 mod 1_000_000)
  PgTime(hour: hours, minute: minutes, second: seconds, microsecond: microseconds)

proc decodeBinaryTimeTz*(data: openArray[byte]): PgTimeTz {.raises: [PgError].} =
  if data.len != 12:
    raise
      newException(PgTypeError, "Binary timetz: expected 12 bytes, got " & $data.len)
  let us = fromBE64(data)
  if us < 0 or us > pgTimeMaxUs:
    raise newException(PgTypeError, "Binary timetz: microseconds out of range " & $us)
  let pgOffset = fromBE32(data.toOpenArray(8, 11))
  # ``timetz_send`` emits any stored zone (see ``parseTimeTzText``), but
  # un-negating ``int32.low`` would OverflowDefect.
  if pgOffset == int32.low:
    raise newException(PgTypeError, "Binary timetz: zone displacement out of range")
  let hours = int32(us div 3_600_000_000)
  let rem1 = us mod 3_600_000_000
  let minutes = int32(rem1 div 60_000_000)
  let rem2 = rem1 mod 60_000_000
  let seconds = int32(rem2 div 1_000_000)
  let microseconds = int32(rem2 mod 1_000_000)
  PgTimeTz(
    hour: hours,
    minute: minutes,
    second: seconds,
    microsecond: microseconds,
    utcOffset: -pgOffset, # un-negate PostgreSQL wire format
  )

proc decodeInetBinary*(
    data: openArray[byte]
): tuple[address: IpAddress, mask: uint8] {.raises: [PgError].} =
  ## Decode PostgreSQL binary inet/cidr format:
  ##   1 byte: family (2=IPv4, 3=IPv6)
  ##   1 byte: bits (netmask length)
  ##   1 byte: is_cidr (0 or 1)
  ##   1 byte: addrlen (4 or 16)
  ##   N bytes: address
  if data.len < 4:
    raise newException(PgTypeError, "Binary inet data too short: " & $data.len)
  let family = data[0]
  let bits = data[1]
  # data[2] = is_cidr, ignored for decoding
  let addrlen = data[3]
  if family == 2:
    if addrlen != 4:
      raise newException(PgTypeError, "Binary inet IPv4 addrlen mismatch: " & $addrlen)
    if data.len < 8:
      raise newException(PgTypeError, "Binary inet IPv4 data too short: " & $data.len)
    # Match the text path (parseInetText): reject a netmask wider than the
    # family allows so a malformed wire value surfaces as PgTypeError rather
    # than a plausible-but-wrong mask. ``bits`` is uint8, so 0 is implicit.
    if bits > 32:
      raise newException(PgTypeError, "Binary inet IPv4 mask out of range: " & $bits)
    var ip = IpAddress(family: IpAddressFamily.IPv4)
    for i in 0 ..< 4:
      ip.address_v4[i] = data[4 + i]
    ensureNoTrailing(8, data.len, "Binary inet IPv4")
    (ip, bits)
  elif family == 3:
    if addrlen != 16:
      raise newException(PgTypeError, "Binary inet IPv6 addrlen mismatch: " & $addrlen)
    if data.len < 20:
      raise newException(PgTypeError, "Binary inet IPv6 data too short: " & $data.len)
    if bits > 128:
      raise newException(PgTypeError, "Binary inet IPv6 mask out of range: " & $bits)
    var ip = IpAddress(family: IpAddressFamily.IPv6)
    for i in 0 ..< 16:
      ip.address_v6[i] = data[4 + i]
    ensureNoTrailing(20, data.len, "Binary inet IPv6")
    (ip, bits)
  else:
    raise newException(PgTypeError, "Binary inet unknown family: " & $family)

proc decodePointBinary*(
    data: openArray[byte], off: int
): PgPoint {.raises: [PgError].} =
  ## Decode a point from 16 bytes at offset.
  if off < 0 or off + 16 > data.len:
    raise newException(PgTypeError, "Binary point data truncated at offset " & $off)
  result.x = decodeFloat64BE(data, off)
  result.y = decodeFloat64BE(data, off + 8)

proc decodeBinaryArray*(
    data: openArray[byte]
): tuple[
  elemOid: int32,
  dims: seq[int32],
  lowerBounds: seq[int32],
  elements: seq[tuple[off: RelOff, len: int]],
] {.raises: [PgError].} =
  ## Decode binary array header. ``-1`` len = NULL. Offsets relative to ``data``.
  if data.len < 12:
    raise newException(PgTypeError, "Binary array too short")
  let ndim = fromBE32(data.toOpenArray(0, 3))
  # has_null at offset 4
  result.elemOid = fromBE32(data.toOpenArray(8, 11))
  if ndim < 0:
    raise newException(PgTypeError, "Binary array: invalid ndim " & $ndim)
  if ndim > PgArrayMaxDim:
    raise newException(
      PgTypeError,
      "Binary array: ndim=" & $ndim & " exceeds MAXDIM (" & $PgArrayMaxDim & ")",
    )
  if ndim == 0:
    result.dims = @[]
    result.lowerBounds = @[]
    result.elements = @[]
    ensureNoTrailing(12, data.len, "Binary array")
    return
  let headerSize = 12 + 8 * int(ndim)
  if data.len < headerSize:
    raise newException(PgTypeError, "Binary array header too short")
  result.dims = newSeq[int32](ndim)
  result.lowerBounds = newSeq[int32](ndim)
  for d in 0 ..< int(ndim):
    result.dims[d] = fromBE32(data.toOpenArray(12 + d * 8, 15 + d * 8))
    result.lowerBounds[d] = fromBE32(data.toOpenArray(16 + d * 8, 19 + d * 8))
  # Delegate per-dim ``> 0`` and product-overflow checks to expectedElemCount
  # so encoder and decoder share one source of truth (raises PgTypeError).
  let totalInt = expectedElemCount(result.dims)
  # Each element carries at least a 4-byte length prefix after the header,
  # so totalInt cannot exceed (data.len - headerSize) div 4. This guard
  # stops a crafted header from triggering a multi-GB allocation on
  # malformed input.
  if totalInt > (data.len - headerSize) div 4:
    raise newException(PgTypeError, "Binary array: dimension length exceeds data")
  result.elements = newSeq[tuple[off: RelOff, len: int]](totalInt)
  var pos = headerSize
  for i in 0 ..< totalInt:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary array truncated at element " & $i)
    let eLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if eLen < -1:
      raise newException(PgTypeError, "Binary array: invalid element length " & $eLen)
    elif eLen == -1:
      result.elements[i] = (off: RelOff(0), len: -1)
    else:
      if pos + eLen > data.len:
        raise newException(PgTypeError, "Binary array: element data truncated at " & $i)
      result.elements[i] = (off: RelOff(pos), len: eLen)
      pos += eLen
  ensureNoTrailing(pos, data.len, "Binary array")

proc rejectMultiDim*(
    decoded:
      tuple[
        elemOid: int32,
        dims: seq[int32],
        lowerBounds: seq[int32],
        elements: seq[tuple[off: RelOff, len: int]],
      ]
) {.raises: [PgError].} =
  ## Reject multi-dim array for 1-D accessors. Use ``PgArray[T]`` instead.
  if decoded.dims.len > 1:
    raise newException(
      PgTypeError,
      "Multi-dimensional array (ndim=" & $decoded.dims.len &
        ") cannot be read as seq; use the PgArray[T] accessor instead",
    )

proc decodeBinaryComposite*(
    data: openArray[byte]
): seq[tuple[oid: int32, off: RelOff, len: int]] {.raises: [PgError].} =
  ## Decode binary composite. ``len==-1`` is NULL; offsets relative.
  if data.len < 4:
    raise newException(PgTypeError, "Binary composite too short")
  let numFields = int(fromBE32(data.toOpenArray(0, 3)))
  if numFields < 0:
    raise
      newException(PgTypeError, "Binary composite: invalid field count " & $numFields)
  # Each field carries at least an 8-byte header (oid + len) after the 4-byte
  # count, so numFields cannot exceed (data.len - 4) div 8.
  if numFields > (data.len - 4) div 8:
    raise newException(PgTypeError, "Binary composite: field count exceeds data")
  result = newSeq[tuple[oid: int32, off: RelOff, len: int]](numFields)
  var pos = 4
  for i in 0 ..< numFields:
    if pos + 8 > data.len:
      raise newException(PgTypeError, "Binary composite truncated at field " & $i)
    result[i].oid = fromBE32(data.toOpenArray(pos, pos + 3))
    pos += 4
    let flen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if flen < -1:
      raise newException(PgTypeError, "Binary composite: invalid field length " & $flen)
    elif flen == -1:
      result[i].off = RelOff(0)
      result[i].len = -1
    else:
      if pos + flen > data.len:
        raise
          newException(PgTypeError, "Binary composite: field data truncated at " & $i)
      result[i].off = RelOff(pos)
      result[i].len = flen
      pos += flen
  ensureNoTrailing(pos, data.len, "Binary composite")

func takeChar(s: openArray[char], i: var int, c: char): bool {.inline.} =
  result = i < s.len and s[i] == c
  if result:
    inc i

func takeDigits[T: SomeSignedInt](
    s: openArray[char], i: var int, lo, hi: int, v: var T
): bool =
  ## ``lo`` to ``hi`` ASCII digits at ``s[i]``, read into ``v``. ``hi`` must
  ## fit ``T``; 18 always fits ``int64``.
  var j = i
  v = 0
  while j < s.len and j - i < hi and s[j] in {'0' .. '9'}:
    v = v * 10 + T(ord(s[j]) - ord('0'))
    inc j
  result = j - i >= lo
  if result:
    i = j

func takeDigits[T: SomeSignedInt](
    s: openArray[char], i: var int, n: int, v: var T
): bool {.inline.} =
  takeDigits(s, i, n, n, v)

func takePadded[T: SomeSignedInt](
    s: openArray[char], i: var int, width, hi: int, v: var T
): bool =
  ## A field PostgreSQL zero-pads to ``width`` digits: wider only when the
  ## value needs it, so never with a leading zero.
  let start = i
  takeDigits(s, i, width, hi, v) and (i - start == width or s[start] != '0')

func takeFracMicros[T: SomeSignedInt](s: openArray[char], i: var int, us: var T): bool =
  ## ``.f`` to ``.ffffff`` at ``s[i]`` as microseconds. PostgreSQL trims
  ## trailing zeros and prints at most six digits; a seventh is invalid, so it
  ## can never be re-read as the next field.
  let start = i + 1
  if not (takeChar(s, i, '.') and takeDigits(s, i, 1, 6, us)):
    return false
  if i < s.len and s[i] in {'0' .. '9'}:
    return false
  for _ in i - start ..< 6:
    us *= 10
  true

func takeYmd(s: openArray[char], i: var int, year, month, day: var int): bool =
  ## PostgreSQL's ISO ``YYYY-MM-DD``. Seven year digits reach ``date``'s
  ## 5874897; the caller checks the calendar once the era is known.
  takePadded(s, i, 4, 7, year) and year > 0 and takeChar(s, i, '-') and
    takeDigits(s, i, 2, month) and takeChar(s, i, '-') and takeDigits(s, i, 2, day)

func takeClock(s: openArray[char], i: var int, h, m, sec, us: var int): bool =
  ## PostgreSQL's ``HH:MM:SS[.ffffff]``; the caller bounds the hour.
  us = 0
  takeDigits(s, i, 2, h) and takeChar(s, i, ':') and takeDigits(s, i, 2, m) and m <= 59 and
    takeChar(s, i, ':') and takeDigits(s, i, 2, sec) and sec <= 59 and
    (i >= s.len or s[i] != '.' or takeFracMicros(s, i, us))

func takeTimeOfDay(s: openArray[char], i: var int, h, m, sec, us: var int): bool =
  ## ``takeClock`` up to ``24:00:00``, PostgreSQL's inclusive end of day.
  takeClock(s, i, h, m, sec, us) and
    (h < 24 or (h == 24 and m == 0 and sec == 0 and us == 0))

func takeUtcOffset(s: openArray[char], i: var int, off: var int64): bool =
  ## PostgreSQL's ``±HH[:MM[:SS]]`` offset at ``s[i]``, as seconds east of UTC.
  # EncodeTimezone prints every hour digit; six cover int32 (596523 hours).
  if i >= s.len or s[i] notin {'+', '-'}:
    return false
  var j = i + 1
  var h, m, sec: int64
  if not takePadded(s, j, 2, 6, h):
    return false
  if takeChar(s, j, ':'):
    if not takeDigits(s, j, 2, m) or m > 59:
      return false
    if takeChar(s, j, ':'):
      if not takeDigits(s, j, 2, sec) or sec > 59:
        return false
  off = h * 3600 + m * 60 + sec
  if s[i] == '-':
    off = -off
  i = j
  true

func takeEra(s: openArray[char], i: var int, year: var int): bool =
  ## The optional era suffix: PostgreSQL prints `` BC`` (`` AD`` is read too),
  ## which turns ``year`` into the proleptic ``1 - year``. Matched as a whole,
  ## so a partial `` BC`` cannot fall through as `` AD``.
  if i < s.len and s[i] == ' ':
    if i + 2 >= s.len:
      return false
    if s[i + 1] == 'B' and s[i + 2] == 'C':
      year = 1 - year
    elif not (s[i + 1] == 'A' and s[i + 2] == 'D'):
      return false
    i += 3
  true

proc validYmd(year, month, day: int): bool =
  month in 1 .. 12 and day in 1 .. getDaysInMonth(Month(month), year)

const
  pgTstzMaxEast = 169 * 3600
    ## A POSIX session zone stops at ``167:59:60`` (168h), and its daylight
    ## time defaults to an hour east of that: ``FOO-167:59:60BAR`` prints ``+169``.
  pgTstzMaxWest = 168 * 3600 ## ``FOO+167:59:60`` prints ``-168``.

proc parseTimestampText*(s: string): DateTime {.gcsafe, raises: [PgError].} =
  # Raises ``PgTypeError`` for infinity/unparseable input or a year outside
  # PostgreSQL's timestamp range (under ``PgError``). Accepts the whole output
  # range: unpadded years past 9999 and the ``BC``/``AD`` era suffix, which it
  # prints after the zone.
  if s == "infinity" or s == "-infinity":
    # Known literal, safe to name (mirrors the binary decoder's message).
    raise newException(
      PgTypeError, "Timestamp is '" & s & "', not representable as a DateTime"
    )
  template invalid(): untyped =
    newException(PgTypeError, "Invalid timestamp (len=" & $s.len & ")")

  # Read by hand, not with `times.parse`: that needs a failed try per optional
  # part and raises RangeDefect for minute 99.
  var i = 0
  var year, month, day, hour, minute, second, us: int
  if not (
    takeYmd(s, i, year, month, day) and takeChar(s, i, ' ') and
    takeClock(s, i, hour, minute, second, us) and hour <= 23
  ):
    raise invalid()
  var off: int64
  if i < s.len and s[i] in {'+', '-'}:
    if not takeUtcOffset(s, i, off) or off notin -pgTstzMaxWest .. pgTstzMaxEast:
      raise invalid()
  else:
    discard takeChar(s, i, 'Z')
  if not (takeEra(s, i, year) and i == s.len and validYmd(year, month, day)):
    raise invalid()
  result =
    dateTime(year, Month(month), day, hour, minute, second, us * 1000, utc()) -
    initDuration(seconds = off)
  # Same ends as the encoders (`pgTimestampMicros`), so text past them
  # cannot decode to a DateTime no encoder would accept back.
  discard pgTimestampMicros(result)

proc parseDateText*(s: string): DateTime {.gcsafe, raises: [PgError].} =
  # Raises ``PgTypeError`` for infinity/unparseable or a year outside
  # PostgreSQL's date range; accepts years past 9999 and the era suffix (see
  # ``parseTimestampText``).
  if s == "infinity" or s == "-infinity":
    # Known literal, safe to name (mirrors the binary decoder's message).
    raise
      newException(PgTypeError, "Date is '" & s & "', not representable as a DateTime")
  var i = 0
  var year, month, day: int
  if not (
    takeYmd(s, i, year, month, day) and takeEra(s, i, year) and i == s.len and
    validYmd(year, month, day)
  ):
    raise newException(PgTypeError, "Invalid date (len=" & $s.len & ")")
  # Zone is utc() so a date decodes to the same absolute instant as
  # decodeBinaryDate; the local default would shift it by the UTC offset.
  result = dateTime(year, Month(month), day, zone = utc())
  # `date` reaches further than `timestamp`, so check against its own ends
  # (`pgDateDays`), mirroring the date encoders.
  discard pgDateDays(result)

proc parseTimeText*(s: string): PgTime {.raises: [PgError].} =
  ## Parse PostgreSQL time text format: "HH:mm:ss" or "HH:mm:ss.ffffff".
  var i = 0
  var h, m, sec, us: int
  if not (takeTimeOfDay(s, i, h, m, sec, us) and i == s.len):
    raise newException(PgTypeError, "Invalid time (len=" & $s.len & ")")
  PgTime(hour: int32(h), minute: int32(m), second: int32(sec), microsecond: int32(us))

proc parseTimeTzText*(s: string): PgTimeTz {.raises: [PgError].} =
  var i = 0
  var h, m, sec, us: int
  if not takeTimeOfDay(s, i, h, m, sec, us):
    raise newException(PgTypeError, "Invalid timetz (len=" & $s.len & ")")
  if i == s.len:
    raise newException(PgTypeError, "Invalid timetz (no offset) (len=" & $s.len & ")")
  # Not bounded like timetz input (``pgTzDispLimit``): a session zone prints up
  # to +169, and ``AT TIME ZONE`` an interval stores any int32 (+596523:14:07).
  var off: int64
  if not (takeUtcOffset(s, i, off) and i == s.len and abs(off) <= int32.high):
    raise newException(PgTypeError, "Invalid timetz offset (len=" & $s.len & ")")
  PgTimeTz(
    hour: int32(h),
    minute: int32(m),
    second: int32(sec),
    microsecond: int32(us),
    utcOffset: int32(off),
  )

proc parseHstoreText*(s: string): PgHstore {.raises: [PgError].} =
  ## Parse PostgreSQL hstore text format: ``"key1"=>"val1", "key2"=>NULL``.
  result = initTable[string, Option[string]]()
  if s.len == 0:
    return
  var i = 0
  while i < s.len:
    # Skip whitespace and commas
    while i < s.len and s[i] in {' ', ',', '\t', '\n', '\r'}:
      i += 1
    if i >= s.len:
      break
    # Parse key (must be quoted)
    if s[i] != '"':
      raise newException(PgTypeError, "hstore: expected '\"' at position " & $i)
    i += 1
    var key = ""
    while i < s.len:
      if s[i] == '\\' and i + 1 < s.len:
        i += 1
        key.add(s[i])
      elif s[i] == '"':
        break
      else:
        key.add(s[i])
      i += 1
    if i >= s.len:
      raise newException(PgTypeError, "hstore: unterminated key string")
    i += 1 # skip closing quote
    # Skip whitespace
    while i < s.len and s[i] == ' ':
      i += 1
    # Expect =>
    if i + 1 >= s.len or s[i] != '=' or s[i + 1] != '>':
      raise newException(PgTypeError, "hstore: expected '=>' at position " & $i)
    i += 2
    # Skip whitespace
    while i < s.len and s[i] == ' ':
      i += 1
    # Parse value (NULL or quoted string)
    if i + 3 < s.len and s[i] == 'N' and s[i + 1] == 'U' and s[i + 2] == 'L' and
        s[i + 3] == 'L' and (i + 4 >= s.len or s[i + 4] in {',', ' ', '\t', '\n', '\r'}):
      result[key] = none(string)
      i += 4
    elif i < s.len and s[i] == '"':
      i += 1
      var val = ""
      while i < s.len:
        if s[i] == '\\' and i + 1 < s.len:
          i += 1
          val.add(s[i])
        elif s[i] == '"':
          break
        else:
          val.add(s[i])
        i += 1
      if i >= s.len:
        raise newException(PgTypeError, "hstore: unterminated value string")
      i += 1 # skip closing quote
      result[key] = some(val)
    else:
      raise newException(
        PgTypeError, "hstore: expected NULL or quoted string at position " & $i
      )

proc invalidInterval(s: string): ref PgTypeError =
  newException(PgTypeError, "Invalid interval (len=" & $s.len & ")")

proc intervalOverflow(s: string): ref PgTypeError =
  newException(PgTypeError, "interval field overflow (len=" & $s.len & ")")

type IntervalFields = object
  months, days: int32
  micros: int64

# Numeric accumulation and unit scaling are bounds-checked so a malicious or
# broken server sending oversized fields raises a catchable ``PgTypeError``
# rather than ``OverflowDefect`` / ``RangeDefect`` (or wrapping in release builds).

proc addI32(acc: var int32, v: int64, s: string) =
  # ``v`` is bounded first so the int64 sum itself cannot overflow.
  if v < int64(int32.low) or v > int64(int32.high):
    raise intervalOverflow(s)
  let sum = int64(acc) + v
  if sum < int64(int32.low) or sum > int64(int32.high):
    raise intervalOverflow(s)
  acc = int32(sum)

proc addChecked(acc: var int64, v: int64, s: string) =
  if (v > 0 and acc > int64.high - v) or (v < 0 and acc < int64.low - v):
    raise intervalOverflow(s)
  acc += v

proc scaled(v, unit: int64, s: string): int64 =
  if v > int64.high div unit or v < int64.low div unit:
    raise intervalOverflow(s)
  v * unit

type IntervalUnit = enum
  iuYear
  iuMonth
  iuDay
  iuHour
  iuMinute
  iuSecond

const noFrac = -1'i64

proc addField(
    f: var IntervalFields,
    unit: IntervalUnit,
    v: int64,
    neg: bool,
    s: string,
    frac = noFrac,
) =
  ## Add ``v`` (>= 0) of ``unit`` and, for seconds only, ``frac`` microseconds,
  ## signed by ``neg``. Each part is signed before it is added, so a negative
  ## total reaches ``int64.low``, which no magnitude can hold.
  let sv =
    if neg:
      -v
    else:
      v
  case unit
  of iuYear:
    # Bounding first keeps ``sv * 12`` itself from overflowing.
    if sv < int64(int32.low) div 12 or sv > int64(int32.high) div 12:
      raise intervalOverflow(s)
    f.months.addI32(sv * 12, s)
  of iuMonth:
    f.months.addI32(sv, s)
  of iuDay:
    f.days.addI32(sv, s)
  of iuHour:
    f.micros.addChecked(scaled(sv, 3_600_000_000'i64, s), s)
  of iuMinute:
    f.micros.addChecked(scaled(sv, 60_000_000'i64, s), s)
  of iuSecond:
    f.micros.addChecked(scaled(sv, 1_000_000, s), s)
  if frac != noFrac:
    if unit != iuSecond:
      raise invalidInterval(s)
    f.micros.addChecked(
      if neg:
        -frac
      else:
        frac,
      s,
    )

proc intervalUnit(word, s: string): IntervalUnit =
  ## A unit word of the ``postgres`` and ``postgres_verbose`` styles.
  case word
  of "year", "years":
    iuYear
  of "mon", "mons":
    iuMonth
  of "day", "days":
    iuDay
  of "hour", "hours":
    iuHour
  of "min", "mins":
    iuMinute
  of "sec", "secs":
    iuSecond
  of "":
    # Also guarantees forward progress on a non-unit byte.
    raise invalidInterval(s)
  else:
    raise newException(PgTypeError, "Invalid interval unit (len=" & $s.len & ")")

proc readSign(s: string, i: var int): bool =
  ## Consume an optional sign; true for ``-``.
  if i < s.len and s[i] in {'+', '-'}:
    result = s[i] == '-'
    inc i

proc readUInt(s: string, i: var int): int64 =
  ## One or more decimal digits at ``i``.
  if not takeDigits(s, i, 1, 18, result):
    raise invalidInterval(s)
  # A 19th digit overflows every field it could feed.
  if i < s.len and s[i] in {'0' .. '9'}:
    raise intervalOverflow(s)

proc readFracMicros(s: string, i: var int): int64 =
  ## An optional ``.ffffff`` at ``i`` as microseconds, else ``noFrac``. A
  ## seventh digit is invalid (``takeFracMicros``), never a later field.
  if i >= s.len or s[i] != '.':
    return noFrac
  if not takeFracMicros(s, i, result):
    raise invalidInterval(s)

proc readTime(f: var IntervalFields, s: string, i: var int, neg: bool) =
  ## ``H:MM[:SS[.ffffff]]`` at ``i``, signed by ``neg``.
  f.addField(iuHour, readUInt(s, i), neg, s)
  if i >= s.len or s[i] != ':':
    raise invalidInterval(s)
  inc i
  f.addField(iuMinute, readUInt(s, i), neg, s)
  if i < s.len and s[i] == ':':
    inc i
    let secs = readUInt(s, i)
    f.addField(iuSecond, secs, neg, s, readFracMicros(s, i))

proc parseIntervalPostgres(s: string): IntervalFields =
  ## ``postgres``: "1 year 2 mons 3 days 04:05:06.5", "-1 years +3 days -04:05:06".
  var i = 0
  while i < s.len:
    if s[i] == ' ':
      inc i
      continue
    var j = i
    if s[j] in {'+', '-'}:
      inc j
    while j < s.len and s[j] in {'0' .. '9'}:
      inc j
    let neg = readSign(s, i)
    if j < s.len and s[j] == ':':
      result.readTime(s, i, neg)
      continue
    let v = readUInt(s, i)
    while i < s.len and s[i] == ' ':
      inc i
    let unitStart = i
    while i < s.len and s[i] in {'a' .. 'z'}:
      inc i
    result.addField(intervalUnit(s[unitStart ..< i], s), v, neg, s)

proc parseIntervalVerbose(s: string): IntervalFields =
  ## ``postgres_verbose``: "@ 1 year 2 mons -3 days 4 hours 5 mins 6.5 secs ago".
  ## Fields are signed relative to a trailing ``ago``, which negates them all;
  ## it is applied per field so "@ 2147483648 days ago" fits.
  let ago = s.endsWith(" ago")
  var n = s.len
  if ago:
    n -= 4
  if n == 3 and s.startsWith("@ 0"):
    return
  var i = 1
  var seen = false
  while i < n:
    if s[i] == ' ':
      inc i
      continue
    let neg = readSign(s, i) != ago
    let v = readUInt(s, i)
    let frac = readFracMicros(s, i)
    if i >= n or s[i] != ' ':
      raise invalidInterval(s)
    inc i
    let unitStart = i
    while i < n and s[i] in {'a' .. 'z'}:
      inc i
    result.addField(intervalUnit(s[unitStart ..< i], s), v, neg, s, frac)
    seen = true
  if not seen:
    raise invalidInterval(s)

proc parseIntervalIso8601(s: string): IntervalFields =
  ## ``iso_8601``: "P1Y2M3DT4H5M6.5S", "P-1Y-2M3DT-4H-5M-6.5S", "PT0S".
  var i = 1
  var inTime = false
  var seen = false
  while i < s.len:
    if s[i] == 'T' and not inTime:
      inTime = true
      inc i
      continue
    let neg = readSign(s, i)
    let v = readUInt(s, i)
    let frac = readFracMicros(s, i)
    if i >= s.len:
      raise invalidInterval(s)
    let unit =
      if inTime:
        case s[i]
        of 'H':
          iuHour
        of 'M':
          iuMinute
        of 'S':
          iuSecond
        else:
          raise invalidInterval(s)
      else:
        case s[i]
        of 'Y':
          iuYear
        of 'M':
          iuMonth
        of 'D':
          iuDay
        else:
          raise invalidInterval(s)
    inc i
    result.addField(unit, v, neg, s, frac)
    seen = true
  if not seen:
    raise invalidInterval(s)

proc parseIntervalSqlStandard(s: string): IntervalFields =
  ## ``sql_standard``: "0", "1-2", "3 4:05:06.5", "-4:05:06" with one leading
  ## sign for every field, or "+1-2 -3 +4:05:06" with a sign on each.
  if s == "0":
    return
  var i = 0
  template sep() =
    if i >= s.len or s[i] != ' ':
      raise invalidInterval(s)
    inc i

  template yearMonth(neg: bool) =
    let years = readUInt(s, i)
    if i >= s.len or s[i] != '-':
      raise invalidInterval(s)
    inc i
    let mons = readUInt(s, i)
    result.addField(iuYear, years, neg, s)
    result.addField(iuMonth, mons, neg, s)

  template signedField(): bool =
    if i >= s.len or s[i] notin {'+', '-'}:
      raise invalidInterval(s)
    readSign(s, i)

  case s.count(' ')
  of 2:
    let ymNeg = signedField()
    yearMonth(ymNeg)
    sep()
    let dayNeg = signedField()
    result.addField(iuDay, readUInt(s, i), dayNeg, s)
    sep()
    let timeNeg = signedField()
    result.readTime(s, i, timeNeg)
  of 1:
    let neg = readSign(s, i)
    result.addField(iuDay, readUInt(s, i), neg, s)
    sep()
    result.readTime(s, i, neg)
  of 0:
    let neg = readSign(s, i)
    if ':' in s:
      result.readTime(s, i, neg)
    else:
      yearMonth(neg)
  else:
    raise invalidInterval(s)
  if i != s.len:
    raise invalidInterval(s)

proc parseIntervalText*(s: string): PgInterval {.raises: [PgError].} =
  ## Parse interval text output in any ``IntervalStyle``:
  ##   postgres:         "1 year 2 mons 3 days 04:05:06.123456", "-1 years +3 days"
  ##   postgres_verbose: "@ 1 year 2 mons 3 days 4 hours 5 mins 6.5 secs ago"
  ##   sql_standard:     "1-2", "3 4:05:06.5", "+1-2 -3 +4:05:06"
  ##   iso_8601:         "P1Y2M3DT4H5M6.5S"
  ##
  ## ``infinity`` / ``-infinity`` (PostgreSQL 17+, every style) decode to the
  ## same all-max / all-min fields as the binary format. Oversized fields raise
  ## ``PgTypeError``, never a Defect. Surrounding spaces are ignored.
  let t = s.strip(chars = {' '})
  if t == "infinity":
    return PgInterval(months: int32.high, days: int32.high, microseconds: int64.high)
  if t == "-infinity":
    return PgInterval(months: int32.low, days: int32.low, microseconds: int64.low)
  let f =
    if t.len > 0 and t[0] == '@':
      parseIntervalVerbose(t)
    elif t.len > 0 and t[0] == 'P':
      parseIntervalIso8601(t)
    elif t.contains({'a' .. 'z'}):
      parseIntervalPostgres(t)
    else:
      # Also a postgres time-only value ("-01:00:00"), which reads the same.
      parseIntervalSqlStandard(t)
  PgInterval(months: f.months, days: f.days, microseconds: f.micros)

proc parseInetText*(
    s: string
): tuple[address: IpAddress, mask: uint8] {.raises: [PgError].} =
  # Converts ``ValueError`` to ``PgTypeError``.
  let slashIdx = s.find('/')
  pgTypeErrorOnValueError("invalid inet value (len=" & $s.len & ")"):
    if slashIdx == -1:
      let ip = parseIpAddress(s)
      let defaultMask = if ip.family == IpAddressFamily.IPv4: 32'u8 else: 128'u8
      return (ip, defaultMask)
    let addrStr = s.substr(0, slashIdx - 1)
    let maskStr = s.substr(slashIdx + 1)
    let ip = parseIpAddress(addrStr)
    # ``uint8(parseInt(...))`` would silently wrap an out-of-range prefix length
    # (``/300`` -> 44, ``/-5`` -> 251), so validate against the family's maximum
    # before narrowing; otherwise a malformed mask escapes the ``PgTypeError``
    # contract as a plausible-but-wrong value instead of an error.
    let maxMask = if ip.family == IpAddressFamily.IPv4: 32 else: 128
    let mask = pgParseUIntField(maskStr, "invalid inet value (len=" & $s.len & ")")
    if mask > maxMask:
      raise newException(PgTypeError, "inet mask out of range (len=" & $s.len & ")")
    result = (ip, uint8(mask))

proc addQuotedLexeme(s: var string, data: openArray[byte], first, last: int) =
  ## Append ``data[first ..< last]`` quoted like tsvectorout/tsqueryout.
  ## Byte-wise scanning relies on the pinned UTF8 ``client_encoding``.
  s.add('\'')
  for i in first ..< last:
    let c = char(data[i])
    if c == '\'' or c == '\\':
      s.add(c)
    s.add(c)
  s.add('\'')

proc decodeBinaryTsVector*(data: openArray[byte]): string {.raises: [PgError].} =
  ## Decode PostgreSQL binary tsvector to text representation.
  if data.len < 4:
    raise newException(PgTypeError, "tsvector binary data too short")
  let nlexemes = int(fromBE32(data.toOpenArray(0, 3)))
  if nlexemes < 0:
    raise
      newException(PgTypeError, "tsvector binary: invalid lexeme count " & $nlexemes)
  # Each lexeme needs at least a null terminator (1 byte) + 2-byte position
  # count after the 4-byte count, so nlexemes cannot exceed (data.len - 4) div 3.
  if nlexemes > (data.len - 4) div 3:
    raise newException(PgTypeError, "tsvector binary: lexeme count exceeds data")
  var pos = 4
  var parts = newSeq[string](nlexemes)
  const weightChars = ['D', 'C', 'B', 'A']
  for i in 0 ..< nlexemes:
    # Read null-terminated lexeme
    var lexEnd = pos
    while lexEnd < data.len and data[lexEnd] != 0:
      inc lexEnd
    if lexEnd >= data.len:
      raise newException(PgTypeError, "tsvector binary: lexeme missing null terminator")
    var part = newStringOfCap(lexEnd - pos + 2)
    part.addQuotedLexeme(data, pos, lexEnd)
    pos = lexEnd + 1 # skip null terminator
    # Read positions
    if pos + 1 >= data.len:
      raise newException(PgTypeError, "tsvector binary truncated at position count")
    let npos = int(fromBE16(data.toOpenArray(pos, pos + 1)))
    if npos < 0:
      raise
        newException(PgTypeError, "tsvector binary: invalid position count " & $npos)
    pos += 2
    if npos > 0:
      part.add(':')
      for j in 0 ..< npos:
        if pos + 1 >= data.len:
          raise newException(PgTypeError, "tsvector binary truncated at position")
        let posVal = uint16(fromBE16(data.toOpenArray(pos, pos + 1)))
        pos += 2
        let position = posVal and 0x3FFF
        let weight = int((posVal shr 14) and 0x3)
        if j > 0:
          part.add(',')
        part.add($position)
        if weight > 0:
          part.add(weightChars[weight])
    parts[i] = part
  ensureNoTrailing(pos, data.len, "tsvector binary")
  parts.join(" ")

type TsStep = enum
  tsVisit
  tsInfix
  tsOpen
  tsClose

proc parseTsQueryToken(data: openArray[byte], pos: var int) =
  ## Validate one token and advance past it.
  if pos >= data.len:
    raise newException(PgTypeError, "tsquery binary truncated")
  let tokenType = data[pos]
  inc pos
  case tokenType
  of 1: # operand
    if pos + 2 >= data.len:
      raise newException(PgTypeError, "tsquery operand truncated")
    let weightByte = data[pos]
    if weightByte > 0x0F:
      # tsqueryrecv only accepts the four weight bits.
      raise newException(PgTypeError, "tsquery binary: invalid weight " & $weightByte)
    pos += 2
    while pos < data.len and data[pos] != 0:
      inc pos
    if pos >= data.len:
      raise newException(PgTypeError, "tsquery operand missing null terminator")
    inc pos
  of 2: # operator
    if pos >= data.len:
      raise newException(PgTypeError, "tsquery operator truncated")
    let op = data[pos]
    inc pos
    case op
    of 1, 2, 3: # NOT, AND, OR
      discard
    of 4: # PHRASE
      if pos + 1 >= data.len:
        raise newException(PgTypeError, "tsquery PHRASE distance truncated")
      # No range check: stopword removal sums distances, so the server
      # itself sends values tsqueryrecv would reject.
      pos += 2
    else:
      raise newException(PgTypeError, "Unknown tsquery operator: " & $op)
  else:
    raise newException(PgTypeError, "Unknown tsquery token type: " & $tokenType)

proc tsTokenPrec(data: openArray[byte], off: int): TsPrec {.inline.} =
  if data[off] == 1:
    return tpOperand
  case data[off + 1]
  of 1: tpNot
  of 2: tpAnd
  of 3: tpOr
  else: tpPhrase

proc addTsQueryOperand(s: var string, data: openArray[byte], off: int) =
  ## Render an operand token validated by ``parseTsQueryToken``.
  let weight = data[off + 1]
  let prefix = data[off + 2] != 0
  var strEnd = off + 3
  while data[strEnd] != 0:
    inc strEnd
  s.addQuotedLexeme(data, off + 3, strEnd)
  if weight != 0 or prefix:
    # Same order as the server's tsqueryout: prefix marker before weights.
    s.add(':')
    if prefix:
      s.add('*')
    if (weight and 0x08) != 0:
      s.add('A')
    if (weight and 0x04) != 0:
      s.add('B')
    if (weight and 0x02) != 0:
      s.add('C')
    if (weight and 0x01) != 0:
      s.add('D')

proc decodeBinaryTsQuery*(data: openArray[byte]): string {.raises: [PgError].} =
  ## Decode PostgreSQL binary tsquery (prefix/preorder, right operand first) to
  ## the server's text representation (infix).
  if data.len < 4:
    raise newException(PgTypeError, "tsquery binary data too short")
  let ntokens = int(fromBE32(data.toOpenArray(0, 3)))
  if ntokens < 0:
    raise newException(PgTypeError, "tsquery binary: invalid token count " & $ntokens)
  # Every token takes at least 2 bytes.
  if ntokens > (data.len - 4) div 2:
    raise newException(PgTypeError, "tsquery binary: token count exceeds data")
  # tsqueryrecv's cap: MaxAllocSize / sizeof(QueryItem).
  const maxTokens = 0x3fffffff div 12
  if ntokens > maxTokens:
    raise newException(PgTypeError, "tsquery binary: invalid token count " & $ntokens)
  if ntokens == 0:
    ensureNoTrailing(4, data.len, "tsquery binary")
    return ""

  # Per token: its byte offset, and for a binary operator the index of its
  # left operand. The right operand always follows the operator directly.
  var nodes = newSeq[tuple[off, left: int32]](ntokens)
  var pos = 4
  for i in 0 ..< ntokens:
    nodes[i].off = int32(pos)
    parseTsQueryToken(data, pos)
  ensureNoTrailing(pos, data.len, "tsquery binary")

  # Link without recursion: plainto_tsquery nests one level per word.
  # Scanning backwards, an operator's right operand is on top of the stack.
  var roots: seq[int32]
  for i in countdown(ntokens - 1, 0):
    let arity =
      case tsTokenPrec(data, nodes[i].off)
      of tpOperand: 0
      of tpNot: 1
      else: 2
    if roots.len < arity:
      raise newException(PgTypeError, "tsquery binary: operator missing operand")
    if arity >= 1:
      discard roots.pop()
    if arity == 2:
      nodes[i].left = roots.pop()
    roots.add(int32(i))
  if roots.len != 1:
    raise newException(
      PgTypeError,
      "tsquery binary: token count " & $ntokens & " but " & $roots.len & " trees present",
    )

  var todo = @[(step: tsVisit, node: 0'i32)]
  template pushChild(child: int32, parentPrec: TsPrec, rightOfPhrase: bool) =
    # Parenthesize like tsqueryout's infix(): a looser child, or a
    # right-hand phrase under a phrase (phrase is not associative).
    let prec = tsTokenPrec(data, nodes[child].off)
    let wrap = prec < parentPrec or (rightOfPhrase and prec == tpPhrase)
    if wrap:
      todo.add((tsClose, child))
    todo.add((tsVisit, child))
    if wrap:
      todo.add((tsOpen, child))

  while todo.len > 0:
    let (step, i) = todo.pop()
    let off = int(nodes[i].off)
    case step
    of tsOpen:
      result.add("( ")
    of tsClose:
      result.add(" )")
    of tsInfix:
      case data[off + 1]
      of 2:
        result.add(" & ")
      of 3:
        result.add(" | ")
      else:
        # int16 like tsqueryout's "%d", so wrapped sums print negative.
        let distance = fromBE16(data.toOpenArray(off + 2, off + 3))
        if distance == 1:
          result.add(" <-> ")
        else:
          result.add(" <")
          result.add($distance)
          result.add("> ")
    of tsVisit:
      let prec = tsTokenPrec(data, off)
      case prec
      of tpOperand:
        result.addTsQueryOperand(data, off)
      of tpNot:
        result.add('!')
        pushChild(i + 1, tpNot, false)
      else:
        # Pushed in reverse so the left operand prints first.
        pushChild(i + 1, prec, prec == tpPhrase)
        todo.add((tsInfix, i))
        pushChild(nodes[i].left, prec, false)

# Geometry text format parsers

proc parsePointText*(s: string): PgPoint {.raises: [PgError].} =
  ## Parse "(x,y)" text format.
  var inner = s.strip()
  if inner.len >= 2 and inner[0] == '(' and inner[^1] == ')':
    inner = inner[1 ..^ 2]
  let comma = inner.find(',')
  if comma < 0:
    raise newException(PgTypeError, "Invalid point (len=" & $s.len & ")")
  PgPoint(x: pgParseFloat(inner[0 ..< comma]), y: pgParseFloat(inner[comma + 1 ..^ 1]))

proc parsePointsText*(s: string): seq[PgPoint] {.raises: [PgError].} =
  ## Parse a comma-separated list of points like "(x1,y1),(x2,y2),...".
  var i = 0
  let n = s.len
  while i < n:
    while i < n and s[i] in {' ', ','}:
      i += 1
    if i >= n:
      break
    if s[i] != '(':
      raise newException(
        PgTypeError, "Expected '(' in point list at pos " & $i & " (len=" & $s.len & ")"
      )
    let start = i
    i += 1
    # Find matching ')'
    while i < n and s[i] != ')':
      i += 1
    if i >= n:
      raise
        newException(PgTypeError, "Unmatched '(' in point list (len=" & $s.len & ")")
    i += 1 # skip ')'
    result.add(parsePointText(s[start ..< i]))

# Array text format parser

proc parseTextArray*(s: string): seq[Option[string]] {.raises: [PgError].} =
  ## Parse PostgreSQL 1-D text-format array literal: {elem1,elem2,...}
  ## Returns elements as ``Option[string]`` (none for NULL).
  ## Raises ``PgTypeError`` for multi-dimensional literals; callers that need
  ## multi-dim support should decode via the ``PgArray[T]`` accessors.
  if s.len < 2 or s[0] != '{' or s[^1] != '}':
    raise newException(PgTypeError, "Invalid array literal (len=" & $s.len & ")")
  let inner = s[1 ..^ 2]
  if inner.len == 0:
    return @[]
  var i = 0
  while i < inner.len:
    # A '{' at an element-start position marks a nested subarray. Silently
    # splitting on ',' would yield garbage fragments (e.g. "{{a,b},{c,d}}"
    # → ["{a","b}","{c","d}"]), so raise here to mirror the binary path's
    # rejectMultiDim contract.
    if inner[i] == '{':
      raise newException(
        PgTypeError,
        "Multi-dimensional array text literal cannot be read as seq; " &
          "use the PgArray[T] accessor instead",
      )
    if inner[i] == '"':
      # Quoted element
      i += 1
      var elem = ""
      while i < inner.len:
        if inner[i] == '\\' and i + 1 < inner.len:
          i += 1
          elem.add(inner[i])
        elif inner[i] == '"':
          break
        else:
          elem.add(inner[i])
        i += 1
      if i >= inner.len:
        raise newException(PgTypeError, "array: unterminated quoted element")
      i += 1 # skip closing quote
      # A quoted element must be followed by ',' or the end of the array;
      # anything else (e.g. `{"ab"cd}`) would otherwise split silently.
      if i < inner.len and inner[i] != ',':
        raise newException(PgTypeError, "array: unexpected byte after quoted element")
      result.add(some(elem))
    else:
      # Unquoted element
      var elem = ""
      while i < inner.len and inner[i] != ',':
        # Server-side output quotes elements containing these structural
        # bytes, so an unquoted occurrence is malformed input.
        if inner[i] in {'"', '\\', '{', '}'}:
          raise newException(PgTypeError, "array: unexpected byte in unquoted element")
        elem.add(inner[i])
        i += 1
      if elem == "NULL":
        result.add(none(string))
      else:
        result.add(some(elem))
    if i < inner.len and inner[i] == ',':
      i += 1
      if i == inner.len:
        raise newException(PgTypeError, "array: trailing comma")
