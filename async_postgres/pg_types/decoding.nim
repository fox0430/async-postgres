import std/[options, strutils, tables, times, net]

import ../pg_bytes
import ./core

export pg_bytes

proc decodeHstoreBinary*(data: openArray[byte]): PgHstore =
  ## Decode PostgreSQL binary hstore format.
  result = initTable[string, Option[string]]()
  if data.len < 4:
    raise newException(PgTypeError, "hstore binary data too short")
  let numPairs = int(fromBE32(data.toOpenArray(0, 3)))
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

proc fromPgText*(data: seq[byte], oid: int32): string =
  ## Convert text-format bytes from PostgreSQL to a Nim string.
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

# Binary decoders needed by both basic and format-aware row accessors.

proc decodeNumericBinary*(data: openArray[byte]): PgNumeric =
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
  if sign == pgNaN:
    return PgNumeric(sign: pgNaN)
  if 8 + ndigits * 2 > data.len:
    raise newException(
      PgTypeError, "Numeric binary: data truncated for " & $ndigits & " digits"
    )
  var digits = newSeq[int16](ndigits)
  for i in 0 ..< ndigits:
    digits[i] = fromBE16(data.toOpenArray(8 + i * 2, 9 + i * 2))
  PgNumeric(weight: weight, sign: sign, dscale: dscale, digits: digits)

proc decodeBinaryTimestamp*(data: openArray[byte]): DateTime =
  if data.len < 8:
    raise newException(PgTypeError, "Binary timestamp data too short: " & $data.len)
  let pgUs = fromBE64(data)
  let unixUs = pgUs + pgEpochUnix * 1_000_000
  var unixSec = unixUs div 1_000_000
  var fracUs = unixUs mod 1_000_000
  if fracUs < 0:
    unixSec -= 1
    fracUs += 1_000_000
  initTime(unixSec, int(fracUs * 1000)).utc()

proc decodeBinaryDate*(data: openArray[byte]): DateTime =
  if data.len < 4:
    raise newException(PgTypeError, "Binary date data too short: " & $data.len)
  let pgDays = fromBE32(data)
  let unixSec = (int64(pgDays) + int64(pgEpochDaysOffset)) * 86400
  initTime(unixSec, 0).utc()

const pgTimeMaxUs = 86_400_000_000'i64
  ## PostgreSQL time-of-day is microseconds since midnight in [0, 86_400_000_000).

proc decodeBinaryTime*(data: openArray[byte]): PgTime =
  if data.len < 8:
    raise newException(PgTypeError, "Binary time data too short: " & $data.len)
  let us = fromBE64(data)
  if us < 0 or us >= pgTimeMaxUs:
    raise newException(PgTypeError, "Binary time: microseconds out of range " & $us)
  let hours = int32(us div 3_600_000_000)
  let rem1 = us mod 3_600_000_000
  let minutes = int32(rem1 div 60_000_000)
  let rem2 = rem1 mod 60_000_000
  let seconds = int32(rem2 div 1_000_000)
  let microseconds = int32(rem2 mod 1_000_000)
  PgTime(hour: hours, minute: minutes, second: seconds, microsecond: microseconds)

proc decodeBinaryTimeTz*(data: openArray[byte]): PgTimeTz =
  if data.len < 12:
    raise newException(PgTypeError, "Binary timetz data too short: " & $data.len)
  let us = fromBE64(data)
  if us < 0 or us >= pgTimeMaxUs:
    raise newException(PgTypeError, "Binary timetz: microseconds out of range " & $us)
  let pgOffset = fromBE32(data.toOpenArray(8, 11))
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

proc decodeInetBinary*(data: openArray[byte]): tuple[address: IpAddress, mask: uint8] =
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
  # data[3] = addrlen
  if family == 2:
    if data.len < 8:
      raise newException(PgTypeError, "Binary inet IPv4 data too short: " & $data.len)
    var ip = IpAddress(family: IpAddressFamily.IPv4)
    for i in 0 ..< 4:
      ip.address_v4[i] = data[4 + i]
    (ip, bits)
  else:
    if data.len < 20:
      raise newException(PgTypeError, "Binary inet IPv6 data too short: " & $data.len)
    var ip = IpAddress(family: IpAddressFamily.IPv6)
    for i in 0 ..< 16:
      ip.address_v6[i] = data[4 + i]
    (ip, bits)

proc decodePointBinary*(data: openArray[byte], off: int): PgPoint =
  ## Decode a point from 16 bytes at offset.
  if off < 0 or off + 16 > data.len:
    raise newException(PgTypeError, "Binary point data truncated at offset " & $off)
  let xBits = uint64(
    (uint64(data[off]) shl 56) or (uint64(data[off + 1]) shl 48) or
      (uint64(data[off + 2]) shl 40) or (uint64(data[off + 3]) shl 32) or
      (uint64(data[off + 4]) shl 24) or (uint64(data[off + 5]) shl 16) or
      (uint64(data[off + 6]) shl 8) or uint64(data[off + 7])
  )
  let yBits = uint64(
    (uint64(data[off + 8]) shl 56) or (uint64(data[off + 9]) shl 48) or
      (uint64(data[off + 10]) shl 40) or (uint64(data[off + 11]) shl 32) or
      (uint64(data[off + 12]) shl 24) or (uint64(data[off + 13]) shl 16) or
      (uint64(data[off + 14]) shl 8) or uint64(data[off + 15])
  )
  result.x = cast[float64](xBits)
  result.y = cast[float64](yBits)

proc decodeBinaryArray*(
    data: openArray[byte]
): tuple[elemOid: int32, elements: seq[tuple[off: int, len: int]]] =
  ## Decode a PostgreSQL binary array, returning element OID and (offset, length) pairs.
  ## Offsets are relative to the start of `data`.
  if data.len < 12:
    raise newException(PgTypeError, "Binary array too short")
  let ndim = fromBE32(data.toOpenArray(0, 3))
  # has_null at offset 4
  result.elemOid = fromBE32(data.toOpenArray(8, 11))
  if ndim == 0:
    result.elements = @[]
    return
  if ndim != 1:
    raise
      newException(PgTypeError, "Multi-dimensional arrays not supported, ndim=" & $ndim)
  if data.len < 20:
    raise newException(PgTypeError, "Binary array header too short")
  let dimLen = int(fromBE32(data.toOpenArray(12, 15)))
  if dimLen < 0:
    raise newException(PgTypeError, "Binary array: invalid dimension length " & $dimLen)
  # Each element carries at least a 4-byte length prefix after the 20-byte
  # header, so dimLen cannot exceed (data.len - 20) div 4. This guard stops a
  # crafted header from triggering a multi-GB allocation on malformed input.
  if dimLen > (data.len - 20) div 4:
    raise newException(PgTypeError, "Binary array: dimension length exceeds data")
  # lower_bound at offset 16, ignored
  result.elements = newSeq[tuple[off: int, len: int]](dimLen)
  var pos = 20
  for i in 0 ..< dimLen:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary array truncated at element " & $i)
    let eLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if eLen < -1:
      raise newException(PgTypeError, "Binary array: invalid element length " & $eLen)
    elif eLen == -1:
      result.elements[i] = (off: 0, len: -1)
    else:
      if pos + eLen > data.len:
        raise newException(PgTypeError, "Binary array: element data truncated at " & $i)
      result.elements[i] = (off: pos, len: eLen)
      pos += eLen

proc decodeBinaryComposite*(
    data: openArray[byte]
): seq[tuple[oid: int32, off: int, len: int]] =
  ## Decode a PostgreSQL binary composite value.
  ## Returns (typeOid, offset, length) tuples. offset is relative to `data`.
  ## length of -1 indicates NULL.
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
  result = newSeq[tuple[oid: int32, off: int, len: int]](numFields)
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
      result[i].off = 0
      result[i].len = -1
    else:
      if pos + flen > data.len:
        raise
          newException(PgTypeError, "Binary composite: field data truncated at " & $i)
      result[i].off = pos
      result[i].len = flen
      pos += flen

proc parseTimestampText*(s: string): DateTime =
  const formats = [
    "yyyy-MM-dd HH:mm:ss'.'ffffffzzz", "yyyy-MM-dd HH:mm:ss'.'ffffffzz",
    "yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:sszzz", "yyyy-MM-dd HH:mm:sszz",
    "yyyy-MM-dd HH:mm:ss",
  ]
  for fmt in formats:
    try:
      return parse(s, fmt)
    except TimeParseError, IndexDefect:
      discard
  raise newException(PgTypeError, "Invalid timestamp: " & s)

proc parseTimeText*(s: string): PgTime =
  ## Parse PostgreSQL time text format: "HH:mm:ss" or "HH:mm:ss.ffffff".
  if s.len < 8 or s[2] != ':' or s[5] != ':':
    raise newException(PgTypeError, "Invalid time: " & s)
  var h, m, sec, us: int
  try:
    h = parseInt(s[0 .. 1])
    m = parseInt(s[3 .. 4])
    sec = parseInt(s[6 .. 7])
  except ValueError:
    raise newException(PgTypeError, "Invalid time: " & s)
  if h notin 0 .. 23 or m notin 0 .. 59 or sec notin 0 .. 59:
    raise newException(PgTypeError, "Invalid time: " & s)
  if s.len > 8 and s[8] == '.':
    let frac = s[9 .. ^1]
    if frac.len == 0 or frac.len > 6:
      raise newException(PgTypeError, "Invalid time: " & s)
    try:
      us = parseInt(frac)
    except ValueError:
      raise newException(PgTypeError, "Invalid time: " & s)
    # Pad to 6 digits
    for _ in 0 ..< (6 - frac.len):
      us *= 10
  PgTime(hour: int32(h), minute: int32(m), second: int32(sec), microsecond: int32(us))

proc parseTimeTzText*(s: string): PgTimeTz =
  var tzPos = -1
  for i in 8 ..< s.len:
    if s[i] == '+' or s[i] == '-':
      tzPos = i
      break
  if tzPos < 0:
    raise newException(PgTypeError, "Invalid timetz (no offset): " & s)
  let timePart = s[0 ..< tzPos]
  let t = parseTimeText(timePart)
  let sign = if s[tzPos] == '+': 1 else: -1
  let offStr = s[tzPos + 1 .. ^1]
  var offH, offM, offS: int
  try:
    if offStr.len == 2:
      offH = parseInt(offStr)
    elif offStr.len == 5 and offStr[2] == ':':
      offH = parseInt(offStr[0 .. 1])
      offM = parseInt(offStr[3 .. 4])
    elif offStr.len == 8 and offStr[2] == ':' and offStr[5] == ':':
      offH = parseInt(offStr[0 .. 1])
      offM = parseInt(offStr[3 .. 4])
      offS = parseInt(offStr[6 .. 7])
    else:
      raise newException(PgTypeError, "Invalid timetz offset: " & s)
  except ValueError:
    raise newException(PgTypeError, "Invalid timetz offset: " & s)
  let utcOff = sign * (offH * 3600 + offM * 60 + offS)
  PgTimeTz(
    hour: t.hour,
    minute: t.minute,
    second: t.second,
    microsecond: t.microsecond,
    utcOffset: int32(utcOff),
  )

proc parseHstoreText*(s: string): PgHstore =
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

proc parseIntervalText*(s: string): PgInterval =
  ## Parse PostgreSQL default interval text format:
  ##   "1 year 2 mons 3 days 04:05:06.123456"
  ##   "-1 year -2 mons +3 days -04:05:06"
  ##   "00:00:00"
  var months: int32 = 0
  var days: int32 = 0
  var microseconds: int64 = 0
  var i = 0
  let n = s.len
  while i < n:
    if s[i] == ' ':
      i += 1
      continue
    # Check for time part (starts with optional sign then digit followed eventually by ':')
    var j = i
    if j < n and (s[j] == '-' or s[j] == '+'):
      j += 1
    if j < n and s[j] in '0' .. '9':
      # Look ahead for ':' to distinguish time from number+unit
      var k = j
      while k < n and s[k] in '0' .. '9':
        k += 1
      if k < n and s[k] == ':':
        # Time part: [+-]HH:MM:SS[.ffffff]
        let neg = i < n and s[i] == '-'
        if s[i] == '-' or s[i] == '+':
          i += 1
        var hours: int64 = 0
        while i < n and s[i] in '0' .. '9':
          hours = hours * 10 + int64(ord(s[i]) - ord('0'))
          i += 1
        i += 1 # skip ':'
        var mins: int64 = 0
        while i < n and s[i] in '0' .. '9':
          mins = mins * 10 + int64(ord(s[i]) - ord('0'))
          i += 1
        var secs: int64 = 0
        var frac: int64 = 0
        if i < n and s[i] == ':':
          i += 1
          while i < n and s[i] in '0' .. '9':
            secs = secs * 10 + int64(ord(s[i]) - ord('0'))
            i += 1
          if i < n and s[i] == '.':
            i += 1
            var fracDigits = 0
            while i < n and s[i] in '0' .. '9' and fracDigits < 6:
              frac = frac * 10 + int64(ord(s[i]) - ord('0'))
              fracDigits += 1
              i += 1
            # Pad to 6 digits
            while fracDigits < 6:
              frac *= 10
              fracDigits += 1
            # Skip remaining fractional digits
            while i < n and s[i] in '0' .. '9':
              i += 1
        let us = hours * 3_600_000_000 + mins * 60_000_000 + secs * 1_000_000 + frac
        microseconds =
          if neg:
            -us
          else:
            us
        continue
    # Number + unit
    let neg = i < n and s[i] == '-'
    if s[i] == '-' or s[i] == '+':
      i += 1
    var val: int64 = 0
    while i < n and s[i] in '0' .. '9':
      val = val * 10 + int64(ord(s[i]) - ord('0'))
      i += 1
    if neg:
      val = -val
    # Skip space
    while i < n and s[i] == ' ':
      i += 1
    # Read unit
    var unit = ""
    while i < n and s[i] in 'a' .. 'z':
      unit.add(s[i])
      i += 1
    case unit
    of "year", "years":
      months += int32(val * 12)
    of "mon", "mons":
      months += int32(val)
    of "day", "days":
      days += int32(val)
    else:
      discard
  PgInterval(months: months, days: days, microseconds: microseconds)

proc parseInetText*(s: string): tuple[address: IpAddress, mask: uint8] =
  let slashIdx = s.find('/')
  if slashIdx == -1:
    let ip = parseIpAddress(s)
    let defaultMask = if ip.family == IpAddressFamily.IPv4: 32'u8 else: 128'u8
    return (ip, defaultMask)
  let addrStr = s.substr(0, slashIdx - 1)
  let maskStr = s.substr(slashIdx + 1)
  result = (parseIpAddress(addrStr), uint8(parseInt(maskStr)))

proc decodeBinaryTsVector*(data: openArray[byte]): string =
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
    var lexeme = newString(lexEnd - pos)
    for j in 0 ..< lexEnd - pos:
      lexeme[j] = char(data[pos + j])
    pos = lexEnd + 1 # skip null terminator
    # Read positions
    if pos + 1 >= data.len:
      raise newException(PgTypeError, "tsvector binary truncated at position count")
    let npos = int(fromBE16(data.toOpenArray(pos, pos + 1)))
    if npos < 0:
      raise
        newException(PgTypeError, "tsvector binary: invalid position count " & $npos)
    pos += 2
    var part = "'" & lexeme & "'"
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
  parts.join(" ")

proc parseTsQueryNode(data: openArray[byte], pos: var int): string =
  if pos >= data.len:
    raise newException(PgTypeError, "tsquery binary truncated")
  let tokenType = data[pos]
  inc pos
  case tokenType
  of 1: # operand
    if pos + 2 >= data.len:
      raise newException(PgTypeError, "tsquery operand truncated")
    let weightByte = data[pos]
    inc pos
    let prefix = data[pos] != 0
    inc pos
    var strEnd = pos
    while strEnd < data.len and data[strEnd] != 0:
      inc strEnd
    if strEnd >= data.len:
      raise newException(PgTypeError, "tsquery binary: operand missing null terminator")
    var operand = newString(strEnd - pos)
    for j in 0 ..< strEnd - pos:
      operand[j] = char(data[pos + j])
    pos = strEnd + 1
    var s = "'" & operand & "'"
    var suffix = ""
    if (weightByte and 0x08) != 0:
      suffix.add('A')
    if (weightByte and 0x04) != 0:
      suffix.add('B')
    if (weightByte and 0x02) != 0:
      suffix.add('C')
    if (weightByte and 0x01) != 0:
      suffix.add('D')
    if suffix.len > 0 or prefix:
      s.add(':')
      s.add(suffix)
      if prefix:
        s.add('*')
    s
  of 2: # operator
    if pos >= data.len:
      raise newException(PgTypeError, "tsquery operator truncated")
    let op = data[pos]
    inc pos
    case op
    of 1: # NOT
      let arg = parseTsQueryNode(data, pos)
      "!" & arg
    of 2: # AND
      let left = parseTsQueryNode(data, pos)
      let right = parseTsQueryNode(data, pos)
      left & " & " & right
    of 3: # OR
      let left = parseTsQueryNode(data, pos)
      let right = parseTsQueryNode(data, pos)
      "( " & left & " | " & right & " )"
    of 4: # PHRASE
      if pos + 1 >= data.len:
        raise newException(PgTypeError, "tsquery PHRASE distance truncated")
      let distance = int(fromBE16(data.toOpenArray(pos, pos + 1)))
      pos += 2
      let left = parseTsQueryNode(data, pos)
      let right = parseTsQueryNode(data, pos)
      if distance == 1:
        left & " <-> " & right
      else:
        left & " <" & $distance & "> " & right
    else:
      raise newException(PgTypeError, "Unknown tsquery operator: " & $op)
  else:
    raise newException(PgTypeError, "Unknown tsquery token type: " & $tokenType)

proc decodeBinaryTsQuery*(data: openArray[byte]): string =
  ## Decode PostgreSQL binary tsquery (prefix/preorder) to text representation (infix).
  if data.len < 4:
    raise newException(PgTypeError, "tsquery binary data too short")
  let ntokens = int(fromBE32(data.toOpenArray(0, 3)))
  if ntokens < 0:
    raise newException(PgTypeError, "tsquery binary: invalid token count " & $ntokens)
  if ntokens == 0:
    return ""
  var pos = 4
  parseTsQueryNode(data, pos)

# Geometry text format parsers

proc parsePointText*(s: string): PgPoint =
  ## Parse "(x,y)" text format.
  var inner = s.strip()
  if inner.len >= 2 and inner[0] == '(' and inner[^1] == ')':
    inner = inner[1 ..^ 2]
  let comma = inner.find(',')
  if comma < 0:
    raise newException(PgTypeError, "Invalid point: " & s)
  PgPoint(x: parseFloat(inner[0 ..< comma]), y: parseFloat(inner[comma + 1 ..^ 1]))

proc parsePointsText*(s: string): seq[PgPoint] =
  ## Parse a comma-separated list of points like "(x1,y1),(x2,y2),...".
  var i = 0
  let n = s.len
  while i < n:
    while i < n and s[i] in {' ', ','}:
      i += 1
    if i >= n:
      break
    if s[i] != '(':
      raise newException(PgTypeError, "Expected '(' in point list at pos " & $i)
    let start = i
    i += 1
    # Find matching ')'
    while i < n and s[i] != ')':
      i += 1
    if i >= n:
      raise newException(PgTypeError, "Unmatched '(' in point list")
    i += 1 # skip ')'
    result.add(parsePointText(s[start ..< i]))

# Array text format parser

proc parseTextArray*(s: string): seq[Option[string]] =
  ## Parse PostgreSQL text-format array literal: {elem1,elem2,...}
  ## Returns elements as ``Option[string]`` (none for NULL).
  if s.len < 2 or s[0] != '{' or s[^1] != '}':
    raise newException(PgTypeError, "Invalid array literal: " & s)
  let inner = s[1 ..^ 2]
  if inner.len == 0:
    return @[]
  var i = 0
  while i < inner.len:
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
      i += 1 # skip closing quote
      result.add(some(elem))
    else:
      # Unquoted element
      var elem = ""
      while i < inner.len and inner[i] != ',':
        elem.add(inner[i])
        i += 1
      if elem == "NULL":
        result.add(none(string))
      else:
        result.add(some(elem))
    if i < inner.len and inner[i] == ',':
      i += 1
