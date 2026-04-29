import std/[options, strutils, times]

import ../pg_protocol
import ./core
import ./encoding
import ./decoding
import ./accessors

type
  RangeBinaryInput =
    tuple[
      isEmpty: bool,
      hasLower: bool,
      hasUpper: bool,
      lowerInc: bool,
      upperInc: bool,
      lowerData: seq[byte],
      upperData: seq[byte],
    ]

  RangeBinaryRaw* =
    tuple[
      isEmpty: bool,
      hasLower: bool,
      hasUpper: bool,
      lowerInc: bool,
      upperInc: bool,
      lowerOff: int,
      lowerLen: int,
      upperOff: int,
      upperLen: int,
    ]

proc decodeRangeBinaryRaw*(data: openArray[byte]): RangeBinaryRaw =
  if data.len < 1:
    raise newException(PgTypeError, "Binary range too short")
  let flags = data[0]
  if (flags and rangeEmpty) != 0:
    result.isEmpty = true
    return
  result.hasLower = (flags and rangeHasLower) != 0
  result.hasUpper = (flags and rangeHasUpper) != 0
  result.lowerInc = (flags and rangeLowerInc) != 0
  result.upperInc = (flags and rangeUpperInc) != 0
  var pos = 1
  if result.hasLower:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary range truncated at lower bound length")
    let bLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if bLen < 0 or pos + bLen > data.len:
      raise
        newException(PgTypeError, "Binary range: invalid lower bound length " & $bLen)
    result.lowerOff = pos
    result.lowerLen = bLen
    pos += bLen
  if result.hasUpper:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary range truncated at upper bound length")
    let bLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if bLen < 0 or pos + bLen > data.len:
      raise
        newException(PgTypeError, "Binary range: invalid upper bound length " & $bLen)
    result.upperOff = pos
    result.upperLen = bLen

proc decodeInt4RangeBinary*(data: openArray[byte]): PgRange[int32] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[int32](isEmpty: true)
  if raw.hasLower:
    result.hasLower = true
    result.lower = PgRangeBound[int32](
      value: fromBE32(data.toOpenArray(raw.lowerOff, raw.lowerOff + 3)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    result.hasUpper = true
    result.upper = PgRangeBound[int32](
      value: fromBE32(data.toOpenArray(raw.upperOff, raw.upperOff + 3)),
      inclusive: raw.upperInc,
    )

proc decodeInt8RangeBinary*(data: openArray[byte]): PgRange[int64] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[int64](isEmpty: true)
  if raw.hasLower:
    result.hasLower = true
    result.lower = PgRangeBound[int64](
      value: fromBE64(data.toOpenArray(raw.lowerOff, raw.lowerOff + 7)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    result.hasUpper = true
    result.upper = PgRangeBound[int64](
      value: fromBE64(data.toOpenArray(raw.upperOff, raw.upperOff + 7)),
      inclusive: raw.upperInc,
    )

proc decodeNumRangeBinary*(data: openArray[byte]): PgRange[PgNumeric] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[PgNumeric](isEmpty: true)
  if raw.hasLower:
    result.hasLower = true
    result.lower = PgRangeBound[PgNumeric](
      value: decodeNumericBinary(
        data.toOpenArray(raw.lowerOff, raw.lowerOff + raw.lowerLen - 1)
      ),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    result.hasUpper = true
    result.upper = PgRangeBound[PgNumeric](
      value: decodeNumericBinary(
        data.toOpenArray(raw.upperOff, raw.upperOff + raw.upperLen - 1)
      ),
      inclusive: raw.upperInc,
    )

proc decodeTsRangeBinary*(data: openArray[byte]): PgRange[DateTime] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[DateTime](isEmpty: true)
  if raw.hasLower:
    result.hasLower = true
    result.lower = PgRangeBound[DateTime](
      value: decodeBinaryTimestamp(data.toOpenArray(raw.lowerOff, raw.lowerOff + 7)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    result.hasUpper = true
    result.upper = PgRangeBound[DateTime](
      value: decodeBinaryTimestamp(data.toOpenArray(raw.upperOff, raw.upperOff + 7)),
      inclusive: raw.upperInc,
    )

proc decodeDateRangeBinary*(data: openArray[byte]): PgRange[DateTime] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[DateTime](isEmpty: true)
  if raw.hasLower:
    result.hasLower = true
    result.lower = PgRangeBound[DateTime](
      value: decodeBinaryDate(data.toOpenArray(raw.lowerOff, raw.lowerOff + 3)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    result.hasUpper = true
    result.upper = PgRangeBound[DateTime](
      value: decodeBinaryDate(data.toOpenArray(raw.upperOff, raw.upperOff + 3)),
      inclusive: raw.upperInc,
    )

proc decodeMultirangeBinaryRaw*(
    data: openArray[byte]
): seq[tuple[off: RelOff, len: int]] =
  ## Decode the framing of a binary multirange into ``(off, len)`` pairs for
  ## each contained range. ``off`` is relative to ``data`` (typed as
  ## ``RelOff``); recover the absolute parent-buffer offset with
  ## ``parentOff + p.off``.
  if data.len < 4:
    raise newException(PgTypeError, "Binary multirange too short")
  let count = int(fromBE32(data.toOpenArray(0, 3)))
  if count < 0:
    raise newException(PgTypeError, "Binary multirange: invalid count " & $count)
  result = newSeq[tuple[off: RelOff, len: int]](count)
  var pos = 4
  for i in 0 ..< count:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary multirange truncated at range " & $i)
    let rLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if rLen < 0 or pos + rLen > data.len:
      raise
        newException(PgTypeError, "Binary multirange: invalid range length " & $rLen)
    result[i] = (off: RelOff(pos), len: rLen)
    pos += rLen

# Range type support

proc emptyRange*[T](): PgRange[T] =
  ## Create an empty range.
  PgRange[T](isEmpty: true)

proc rangeOf*[T](lower, upper: T, lowerInc = true, upperInc = false): PgRange[T] =
  ## Create a range with both bounds. Default: ``[lower, upper)``.
  PgRange[T](
    hasLower: true,
    hasUpper: true,
    lower: PgRangeBound[T](value: lower, inclusive: lowerInc),
    upper: PgRangeBound[T](value: upper, inclusive: upperInc),
  )

proc rangeFrom*[T](lower: T, inclusive = true): PgRange[T] =
  ## Create a range with only a lower bound (upper unbounded).
  PgRange[T](hasLower: true, lower: PgRangeBound[T](value: lower, inclusive: inclusive))

proc rangeTo*[T](upper: T, inclusive = false): PgRange[T] =
  ## Create a range with only an upper bound (lower unbounded).
  PgRange[T](hasUpper: true, upper: PgRangeBound[T](value: upper, inclusive: inclusive))

proc unboundedRange*[T](): PgRange[T] =
  ## Create a fully unbounded range ``(,)``.
  PgRange[T]()

proc `==`*[T](a, b: PgRange[T]): bool =
  if a.isEmpty != b.isEmpty:
    return false
  if a.isEmpty:
    return true
  if a.hasLower != b.hasLower or a.hasUpper != b.hasUpper:
    return false
  if a.hasLower:
    if a.lower.value != b.lower.value or a.lower.inclusive != b.lower.inclusive:
      return false
  if a.hasUpper:
    if a.upper.value != b.upper.value or a.upper.inclusive != b.upper.inclusive:
      return false
  true

proc rangeElemNeedsQuoting(s: string): bool =
  if s.len == 0:
    return true
  for c in s:
    if c in {',', '(', ')', '[', ']', '"', '\\', ' '}:
      return true
  false

proc quoteRangeElem(s: string): string =
  if not rangeElemNeedsQuoting(s):
    return s
  result = "\""
  for c in s:
    if c == '"':
      result.add("\\\"")
    elif c == '\\':
      result.add("\\\\")
    else:
      result.add(c)
  result.add('"')

proc `$`*[T](r: PgRange[T]): string =
  if r.isEmpty:
    return "empty"
  result = if r.hasLower and r.lower.inclusive: "[" else: "("
  if r.hasLower:
    result.add(quoteRangeElem($r.lower.value))
  result.add(',')
  if r.hasUpper:
    result.add(quoteRangeElem($r.upper.value))
  result.add(if r.hasUpper and r.upper.inclusive: "]" else: ")")

proc parseRangeElem(
    s: string, start: int, stopChars: set[char]
): tuple[val: string, pos: int] =
  ## Parse a single range element (possibly quoted) starting at `start`.
  var i = start
  if i < s.len and s[i] == '"':
    # Quoted element
    i += 1
    var elem = ""
    while i < s.len:
      if s[i] == '\\' and i + 1 < s.len:
        i += 1
        elem.add(s[i])
      elif s[i] == '"':
        i += 1
        break
      else:
        elem.add(s[i])
      i += 1
    (elem, i)
  else:
    var elem = ""
    while i < s.len and s[i] notin stopChars:
      elem.add(s[i])
      i += 1
    (elem, i)

proc parseRangeText*[T](
    s: string, parseElem: proc(s: string): T {.gcsafe, raises: [CatchableError].}
): PgRange[T] =
  if s == "empty":
    return PgRange[T](isEmpty: true)
  if s.len < 3:
    raise newException(PgTypeError, "Invalid range literal: " & s)
  let lowerInc = s[0] == '['
  let upperInc = s[^1] == ']'
  let inner = s[1 ..^ 2]
  # Find comma separator (respecting quoting)
  var commaPos = -1
  var i = 0
  var inQuote = false
  while i < inner.len:
    if inQuote:
      if inner[i] == '\\' and i + 1 < inner.len:
        i += 2
        continue
      elif inner[i] == '"':
        inQuote = false
    else:
      if inner[i] == '"':
        inQuote = true
      elif inner[i] == ',':
        commaPos = i
        break
    i += 1
  if commaPos == -1:
    raise newException(PgTypeError, "Invalid range literal (no comma): " & s)
  let lowerStr = inner[0 ..< commaPos]
  let upperStr = inner[commaPos + 1 ..^ 1]
  # Parse lower bound
  if lowerStr.len > 0:
    let (val, _) = parseRangeElem(lowerStr, 0, {','})
    result.hasLower = true
    result.lower = PgRangeBound[T](value: parseElem(val), inclusive: lowerInc)
  # Parse upper bound
  if upperStr.len > 0:
    let (val, _) = parseRangeElem(upperStr, 0, {','})
    result.hasUpper = true
    result.upper = PgRangeBound[T](value: parseElem(val), inclusive: upperInc)

proc encodeRangeBinaryImpl(r: RangeBinaryInput): seq[byte] =
  if r.isEmpty:
    return @[rangeEmpty]
  var flags: uint8 = 0
  if r.hasLower:
    flags = flags or rangeHasLower
  if r.hasUpper:
    flags = flags or rangeHasUpper
  if r.lowerInc:
    flags = flags or rangeLowerInc
  if r.upperInc:
    flags = flags or rangeUpperInc
  var size = 1
  if r.hasLower:
    size += 4 + r.lowerData.len
  if r.hasUpper:
    size += 4 + r.upperData.len
  result = newSeq[byte](size)
  result[0] = flags
  var pos = 1
  if r.hasLower:
    result.writeBE32(pos, int32(r.lowerData.len))
    pos += 4
    result.writeBytesAt(pos, r.lowerData)
    pos += r.lowerData.len
  if r.hasUpper:
    result.writeBE32(pos, int32(r.upperData.len))
    pos += 4
    result.writeBytesAt(pos, r.upperData)

# toPgParam for range types (text format)

proc toPgParam*(v: PgRange[int32]): PgParam =
  PgParam(oid: OidInt4Range, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgRange[int64]): PgParam =
  PgParam(oid: OidInt8Range, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgRange[PgNumeric]): PgParam =
  PgParam(oid: OidNumRange, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgRange[DateTime]): PgParam =
  PgParam(oid: OidTsRange, format: 0, value: some(toBytes($v)))

proc toPgTsTzRangeParam*(v: PgRange[DateTime]): PgParam =
  PgParam(oid: OidTsTzRange, format: 0, value: some(toBytes($v)))

proc toPgDateRangeParam*(v: PgRange[DateTime]): PgParam =
  ## Encode a date range. DateTime values are formatted as date-only.
  if v.isEmpty:
    return PgParam(oid: OidDateRange, format: 0, value: some(toBytes("empty")))
  var s = if v.hasLower and v.lower.inclusive: "[" else: "("
  if v.hasLower:
    s.add(v.lower.value.format("yyyy-MM-dd"))
  s.add(',')
  if v.hasUpper:
    s.add(v.upper.value.format("yyyy-MM-dd"))
  s.add(if v.hasUpper and v.upper.inclusive: "]" else: ")")
  PgParam(oid: OidDateRange, format: 0, value: some(toBytes(s)))

proc toPgRangeParam*[T](v: PgRange[T], oid: int32): PgParam =
  PgParam(oid: oid, format: 0, value: some(toBytes($v)))

# Binary encoding helpers

proc encodeBinaryTimestamp(dt: DateTime): seq[byte] =
  let t = dt.toTime()
  let pgUs =
    t.toUnix() * 1_000_000 + int64(t.nanosecond div 1000) - pgEpochUnix * 1_000_000
  @(toBE64(pgUs))

proc encodeBinaryDate(dt: DateTime): seq[byte] =
  let t = dt.toTime()
  let pgDays = int32(t.toUnix() div 86400 - int64(pgEpochDaysOffset))
  @(toBE32(pgDays))

proc encodeRangeBinary[T](
    v: PgRange[T], oid: int32, encodeBound: proc(v: T): seq[byte]
): PgParam =
  var ld, ud: seq[byte]
  if v.hasLower:
    ld = encodeBound(v.lower.value)
  if v.hasUpper:
    ud = encodeBound(v.upper.value)
  let data = encodeRangeBinaryImpl(
    (
      isEmpty: v.isEmpty,
      hasLower: v.hasLower,
      hasUpper: v.hasUpper,
      lowerInc: v.hasLower and v.lower.inclusive,
      upperInc: v.hasUpper and v.upper.inclusive,
      lowerData: ld,
      upperData: ud,
    )
  )
  PgParam(oid: oid, format: 1, value: some(data))

# toPgBinaryParam for range types

proc toPgBinaryParam*(v: PgRange[int32]): PgParam =
  encodeRangeBinary(
    v,
    OidInt4Range,
    proc(x: int32): seq[byte] =
      @(toBE32(x)),
  )

proc toPgBinaryParam*(v: PgRange[int64]): PgParam =
  encodeRangeBinary(
    v,
    OidInt8Range,
    proc(x: int64): seq[byte] =
      @(toBE64(x)),
  )

proc toPgBinaryParam*(v: PgRange[PgNumeric]): PgParam =
  encodeRangeBinary(v, OidNumRange, encodeNumericBinary)

proc toPgBinaryParam*(v: PgRange[DateTime]): PgParam =
  encodeRangeBinary(v, OidTsRange, encodeBinaryTimestamp)

proc toPgBinaryTsTzRangeParam*(v: PgRange[DateTime]): PgParam =
  encodeRangeBinary(v, OidTsTzRange, encodeBinaryTimestamp)

proc toPgBinaryDateRangeParam*(v: PgRange[DateTime]): PgParam =
  encodeRangeBinary(v, OidDateRange, encodeBinaryDate)

# toPgBinaryParam for range array types

proc toPgBinaryParam*(v: seq[PgRange[int32]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt4RangeArray,
      format: 1,
      value: some(encodeBinaryArrayEmpty(OidInt4Range)),
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, r in v:
    elements[i] = toPgBinaryParam(r).value.get
  PgParam(
    oid: OidInt4RangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidInt4Range, elements)),
  )

proc toPgBinaryParam*(v: seq[PgRange[int64]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt8RangeArray,
      format: 1,
      value: some(encodeBinaryArrayEmpty(OidInt8Range)),
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, r in v:
    elements[i] = toPgBinaryParam(r).value.get
  PgParam(
    oid: OidInt8RangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidInt8Range, elements)),
  )

proc toPgBinaryParam*(v: seq[PgRange[PgNumeric]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidNumRangeArray, format: 1, value: some(encodeBinaryArrayEmpty(OidNumRange))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, r in v:
    elements[i] = toPgBinaryParam(r).value.get
  PgParam(
    oid: OidNumRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidNumRange, elements)),
  )

proc toPgBinaryParam*(v: seq[PgRange[DateTime]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidTsRangeArray, format: 1, value: some(encodeBinaryArrayEmpty(OidTsRange))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, r in v:
    elements[i] = toPgBinaryParam(r).value.get
  PgParam(
    oid: OidTsRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidTsRange, elements)),
  )

proc toPgBinaryTsTzRangeArrayParam*(v: seq[PgRange[DateTime]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidTsTzRangeArray,
      format: 1,
      value: some(encodeBinaryArrayEmpty(OidTsTzRange)),
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, r in v:
    elements[i] = toPgBinaryTsTzRangeParam(r).value.get
  PgParam(
    oid: OidTsTzRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidTsTzRange, elements)),
  )

proc toPgBinaryDateRangeArrayParam*(v: seq[PgRange[DateTime]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidDateRangeArray,
      format: 1,
      value: some(encodeBinaryArrayEmpty(OidDateRange)),
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, r in v:
    elements[i] = toPgBinaryDateRangeParam(r).value.get
  PgParam(
    oid: OidDateRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidDateRange, elements)),
  )

# Range text format getters

proc getInt4Range*(row: Row, col: int): PgRange[int32] =
  ## Get a column value as an int4range. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeInt4RangeBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  let s = row.getStr(col)
  parseRangeText[int32](
    s,
    proc(e: string): int32 {.gcsafe, raises: [CatchableError].} =
      int32(parseInt(e)),
  )

proc getInt8Range*(row: Row, col: int): PgRange[int64] =
  ## Get a column value as an int8range. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeInt8RangeBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  let s = row.getStr(col)
  parseRangeText[int64](
    s,
    proc(e: string): int64 {.gcsafe, raises: [CatchableError].} =
      parseBiggestInt(e),
  )

proc getNumRange*(row: Row, col: int): PgRange[PgNumeric] =
  ## Get a column value as a numrange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeNumRangeBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  let s = row.getStr(col)
  parseRangeText[PgNumeric](
    s,
    proc(e: string): PgNumeric {.gcsafe, raises: [CatchableError].} =
      parsePgNumeric(e),
  )

proc getTsRange*(row: Row, col: int): PgRange[DateTime] =
  ## Get a column value as a tsrange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeTsRangeBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  let s = row.getStr(col)
  parseRangeText[DateTime](
    s,
    proc(e: string): DateTime {.gcsafe, raises: [CatchableError].} =
      const formats = ["yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:ss"]
      for fmt in formats:
        try:
          return parse(e, fmt)
        except TimeParseError:
          discard
      raise newException(PgTypeError, "Invalid timestamp in range: " & e),
  )

proc getTsTzRange*(row: Row, col: int): PgRange[DateTime] =
  ## Get a column value as a tstzrange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeTsRangeBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  let s = row.getStr(col)
  parseRangeText[DateTime](
    s,
    proc(e: string): DateTime {.gcsafe, raises: [CatchableError].} =
      const formats = [
        "yyyy-MM-dd HH:mm:ss'.'ffffffzzz", "yyyy-MM-dd HH:mm:ss'.'ffffffzz",
        "yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:sszzz",
        "yyyy-MM-dd HH:mm:sszz", "yyyy-MM-dd HH:mm:ss",
      ]
      for fmt in formats:
        try:
          return parse(e, fmt)
        except TimeParseError:
          discard
      raise newException(PgTypeError, "Invalid timestamptz in range: " & e),
  )

proc getDateRange*(row: Row, col: int): PgRange[DateTime] =
  ## Get a column value as a daterange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeDateRangeBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  let s = row.getStr(col)
  parseRangeText[DateTime](
    s,
    proc(e: string): DateTime {.gcsafe, raises: [CatchableError].} =
      try:
        return parse(e, "yyyy-MM-dd")
      except TimeParseError:
        raise newException(PgTypeError, "Invalid date in range: " & e),
  )

# Range Opt accessors (text format)

optAccessor(getInt4Range, getInt4RangeOpt, PgRange[int32])
optAccessor(getInt8Range, getInt8RangeOpt, PgRange[int64])
optAccessor(getNumRange, getNumRangeOpt, PgRange[PgNumeric])
optAccessor(getTsRange, getTsRangeOpt, PgRange[DateTime])
optAccessor(getTsTzRange, getTsTzRangeOpt, PgRange[DateTime])
optAccessor(getDateRange, getDateRangeOpt, PgRange[DateTime])

# Multirange type support

proc len*[T](mr: PgMultirange[T]): int =
  ## Return the number of ranges in the multirange.
  seq[PgRange[T]](mr).len

proc `[]`*[T](mr: PgMultirange[T], i: int): PgRange[T] =
  seq[PgRange[T]](mr)[i]

iterator items*[T](mr: PgMultirange[T]): PgRange[T] =
  for r in seq[PgRange[T]](mr):
    yield r

proc `==`*[T](a, b: PgMultirange[T]): bool =
  let sa = seq[PgRange[T]](a)
  let sb = seq[PgRange[T]](b)
  if sa.len != sb.len:
    return false
  for i in 0 ..< sa.len:
    if sa[i] != sb[i]:
      return false
  true

proc toMultirange*[T](ranges: varargs[PgRange[T]]): PgMultirange[T] =
  ## Create a multirange from individual ranges.
  PgMultirange[T](@ranges)

proc `$`*[T](mr: PgMultirange[T]): string =
  result = "{"
  let s = seq[PgRange[T]](mr)
  for i, r in s:
    if i > 0:
      result.add(',')
    result.add($r)
  result.add('}')

proc parseMultirangeText*[T](
    s: string, parseElem: proc(s: string): T {.gcsafe, raises: [CatchableError].}
): PgMultirange[T] =
  if s.len < 2 or s[0] != '{' or s[^1] != '}':
    raise newException(PgTypeError, "Invalid multirange literal: " & s)
  let inner = s[1 ..^ 2]
  if inner.len == 0:
    return PgMultirange[T](@[])
  # Split on commas that are between ranges (at bracket depth 0)
  var ranges: seq[PgRange[T]]
  var depth = 0
  var start = 0
  for i in 0 ..< inner.len:
    case inner[i]
    of '[', '(':
      if depth == 0 and i > start:
        discard
      depth += 1
    of ']', ')':
      depth -= 1
      if depth == 0:
        let rangeStr = inner[start .. i]
        ranges.add(parseRangeText[T](rangeStr, parseElem))
        start = i + 1
        # Skip comma
        if start < inner.len and inner[start] == ',':
          start += 1
    else:
      # Handle "empty" ranges inside multirange
      if depth == 0 and i == start and inner.len >= start + 5 and
          inner[start ..< start + 5] == "empty":
        ranges.add(PgRange[T](isEmpty: true))
        start = start + 5
        if start < inner.len and inner[start] == ',':
          start += 1
  PgMultirange[T](ranges)

proc encodeMultirangeBinaryImpl(rangeData: seq[seq[byte]]): seq[byte] =
  var size = 4
  for rd in rangeData:
    size += 4 + rd.len
  result = newSeq[byte](size)
  result.writeBE32(0, int32(rangeData.len))
  var pos = 4
  for rd in rangeData:
    result.writeBE32(pos, int32(rd.len))
    pos += 4
    result.writeBytesAt(pos, rd)
    pos += rd.len

# Multirange toPgParam (text format)

proc toPgParam*(v: PgMultirange[int32]): PgParam =
  PgParam(oid: OidInt4Multirange, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgMultirange[int64]): PgParam =
  PgParam(oid: OidInt8Multirange, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgMultirange[PgNumeric]): PgParam =
  PgParam(oid: OidNumMultirange, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgMultirange[DateTime]): PgParam =
  PgParam(oid: OidTsMultirange, format: 0, value: some(toBytes($v)))

proc toPgTsTzMultirangeParam*(v: PgMultirange[DateTime]): PgParam =
  PgParam(oid: OidTsTzMultirange, format: 0, value: some(toBytes($v)))

proc toPgDateMultirangeParam*(v: PgMultirange[DateTime]): PgParam =
  ## Encode a date multirange. DateTime values are formatted as date-only.
  var s = "{"
  let ranges = seq[PgRange[DateTime]](v)
  for i, r in ranges:
    if i > 0:
      s.add(',')
    if r.isEmpty:
      s.add("empty")
    else:
      s.add(if r.hasLower and r.lower.inclusive: "[" else: "(")
      if r.hasLower:
        s.add(r.lower.value.format("yyyy-MM-dd"))
      s.add(',')
      if r.hasUpper:
        s.add(r.upper.value.format("yyyy-MM-dd"))
      s.add(if r.hasUpper and r.upper.inclusive: "]" else: ")")
  s.add('}')
  PgParam(oid: OidDateMultirange, format: 0, value: some(toBytes(s)))

proc toPgMultirangeParam*[T](v: PgMultirange[T], oid: int32): PgParam =
  PgParam(oid: oid, format: 0, value: some(toBytes($v)))

# Multirange toPgBinaryParam

proc toPgBinaryParam*(v: PgMultirange[int32]): PgParam =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[int32]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidInt4Multirange,
    format: 1,
    value: some(encodeMultirangeBinaryImpl(rangeData)),
  )

proc toPgBinaryParam*(v: PgMultirange[int64]): PgParam =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[int64]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidInt8Multirange,
    format: 1,
    value: some(encodeMultirangeBinaryImpl(rangeData)),
  )

proc toPgBinaryParam*(v: PgMultirange[PgNumeric]): PgParam =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[PgNumeric]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidNumMultirange, format: 1, value: some(encodeMultirangeBinaryImpl(rangeData))
  )

proc toPgBinaryParam*(v: PgMultirange[DateTime]): PgParam =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[DateTime]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidTsMultirange, format: 1, value: some(encodeMultirangeBinaryImpl(rangeData))
  )

# Multirange array encoders

proc encodeMultirangeArrayText[T](v: seq[PgMultirange[T]]): string =
  result = "{"
  for i, x in v:
    if i > 0:
      result.add(',')
    result.add('"')
    let s = $x
    for c in s:
      if c == '"' or c == '\\':
        result.add('\\')
      result.add(c)
    result.add('"')
  result.add('}')

template genMultirangeArrayEncoder(T: typedesc, arrayOid: int32) =
  proc toPgParam*(v: seq[PgMultirange[T]]): PgParam =
    PgParam(
      oid: arrayOid, format: 0, value: some(toBytes(encodeMultirangeArrayText(v)))
    )

genMultirangeArrayEncoder(int32, OidInt4MultirangeArray)
genMultirangeArrayEncoder(int64, OidInt8MultirangeArray)
genMultirangeArrayEncoder(PgNumeric, OidNumMultirangeArray)

proc toPgTsMultirangeArrayParam*(v: seq[PgMultirange[DateTime]]): PgParam =
  PgParam(
    oid: OidTsMultirangeArray,
    format: 0,
    value: some(toBytes(encodeMultirangeArrayText(v))),
  )

proc toPgTsTzMultirangeArrayParam*(v: seq[PgMultirange[DateTime]]): PgParam =
  PgParam(
    oid: OidTsTzMultirangeArray,
    format: 0,
    value: some(toBytes(encodeMultirangeArrayText(v))),
  )

proc toPgDateMultirangeArrayParam*(v: seq[PgMultirange[DateTime]]): PgParam =
  ## Encode date multirange array. DateTime values are formatted as date-only.
  var s = "{"
  for i, x in v:
    if i > 0:
      s.add(',')
    s.add('"')
    var mrStr = "{"
    let ranges = seq[PgRange[DateTime]](x)
    for j, r in ranges:
      if j > 0:
        mrStr.add(',')
      if r.isEmpty:
        mrStr.add("empty")
      else:
        mrStr.add(if r.hasLower and r.lower.inclusive: "[" else: "(")
        if r.hasLower:
          mrStr.add(r.lower.value.format("yyyy-MM-dd"))
        mrStr.add(',')
        if r.hasUpper:
          mrStr.add(r.upper.value.format("yyyy-MM-dd"))
        mrStr.add(if r.hasUpper and r.upper.inclusive: "]" else: ")")
    mrStr.add('}')
    for c in mrStr:
      if c == '"' or c == '\\':
        s.add('\\')
      s.add(c)
    s.add('"')
  s.add('}')
  PgParam(oid: OidDateMultirangeArray, format: 0, value: some(toBytes(s)))

# Multirange text format getters

proc getInt4Multirange*(row: Row, col: int): PgMultirange[int32] =
  ## Get a column value as an int4multirange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let parts = decodeMultirangeBinaryRaw(row.data.buf.toOpenArray(off, off + clen - 1))
    var ranges = newSeq[PgRange[int32]](parts.len)
    for i, p in parts:
      ranges[i] = decodeInt4RangeBinary(
        row.data.buf.toOpenArray(off + p.off, off + p.off + p.len - 1)
      )
    return PgMultirange[int32](ranges)
  let s = row.getStr(col)
  parseMultirangeText[int32](
    s,
    proc(e: string): int32 {.gcsafe, raises: [CatchableError].} =
      int32(parseInt(e)),
  )

proc getInt8Multirange*(row: Row, col: int): PgMultirange[int64] =
  ## Get a column value as an int8multirange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let parts = decodeMultirangeBinaryRaw(row.data.buf.toOpenArray(off, off + clen - 1))
    var ranges = newSeq[PgRange[int64]](parts.len)
    for i, p in parts:
      ranges[i] = decodeInt8RangeBinary(
        row.data.buf.toOpenArray(off + p.off, off + p.off + p.len - 1)
      )
    return PgMultirange[int64](ranges)
  let s = row.getStr(col)
  parseMultirangeText[int64](
    s,
    proc(e: string): int64 {.gcsafe, raises: [CatchableError].} =
      parseBiggestInt(e),
  )

proc getNumMultirange*(row: Row, col: int): PgMultirange[PgNumeric] =
  ## Get a column value as a nummultirange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let parts = decodeMultirangeBinaryRaw(row.data.buf.toOpenArray(off, off + clen - 1))
    var ranges = newSeq[PgRange[PgNumeric]](parts.len)
    for i, p in parts:
      ranges[i] = decodeNumRangeBinary(
        row.data.buf.toOpenArray(off + p.off, off + p.off + p.len - 1)
      )
    return PgMultirange[PgNumeric](ranges)
  let s = row.getStr(col)
  parseMultirangeText[PgNumeric](
    s,
    proc(e: string): PgNumeric {.gcsafe, raises: [CatchableError].} =
      parsePgNumeric(e),
  )

proc getTsMultirange*(row: Row, col: int): PgMultirange[DateTime] =
  ## Get a column value as a tsmultirange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let parts = decodeMultirangeBinaryRaw(row.data.buf.toOpenArray(off, off + clen - 1))
    var ranges = newSeq[PgRange[DateTime]](parts.len)
    for i, p in parts:
      ranges[i] = decodeTsRangeBinary(
        row.data.buf.toOpenArray(off + p.off, off + p.off + p.len - 1)
      )
    return PgMultirange[DateTime](ranges)
  let s = row.getStr(col)
  parseMultirangeText[DateTime](
    s,
    proc(e: string): DateTime {.gcsafe, raises: [CatchableError].} =
      const formats = ["yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:ss"]
      for fmt in formats:
        try:
          return parse(e, fmt)
        except TimeParseError:
          discard
      raise newException(PgTypeError, "Invalid timestamp in multirange: " & e),
  )

proc getTsTzMultirange*(row: Row, col: int): PgMultirange[DateTime] =
  ## Get a column value as a tstzmultirange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let parts = decodeMultirangeBinaryRaw(row.data.buf.toOpenArray(off, off + clen - 1))
    var ranges = newSeq[PgRange[DateTime]](parts.len)
    for i, p in parts:
      ranges[i] = decodeTsRangeBinary(
        row.data.buf.toOpenArray(off + p.off, off + p.off + p.len - 1)
      )
    return PgMultirange[DateTime](ranges)
  let s = row.getStr(col)
  parseMultirangeText[DateTime](
    s,
    proc(e: string): DateTime {.gcsafe, raises: [CatchableError].} =
      const formats = [
        "yyyy-MM-dd HH:mm:ss'.'ffffffzzz", "yyyy-MM-dd HH:mm:ss'.'ffffffzz",
        "yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:sszzz",
        "yyyy-MM-dd HH:mm:sszz", "yyyy-MM-dd HH:mm:ss",
      ]
      for fmt in formats:
        try:
          return parse(e, fmt)
        except TimeParseError:
          discard
      raise newException(PgTypeError, "Invalid timestamptz in multirange: " & e),
  )

proc getDateMultirange*(row: Row, col: int): PgMultirange[DateTime] =
  ## Get a column value as a datemultirange. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let parts = decodeMultirangeBinaryRaw(row.data.buf.toOpenArray(off, off + clen - 1))
    var ranges = newSeq[PgRange[DateTime]](parts.len)
    for i, p in parts:
      ranges[i] = decodeDateRangeBinary(
        row.data.buf.toOpenArray(off + p.off, off + p.off + p.len - 1)
      )
    return PgMultirange[DateTime](ranges)
  let s = row.getStr(col)
  parseMultirangeText[DateTime](
    s,
    proc(e: string): DateTime {.gcsafe, raises: [CatchableError].} =
      try:
        return parse(e, "yyyy-MM-dd")
      except TimeParseError:
        raise newException(PgTypeError, "Invalid date in multirange: " & e),
  )

# Multirange Opt accessors (text format)

optAccessor(getInt4Multirange, getInt4MultirangeOpt, PgMultirange[int32])
optAccessor(getInt8Multirange, getInt8MultirangeOpt, PgMultirange[int64])
optAccessor(getNumMultirange, getNumMultirangeOpt, PgMultirange[PgNumeric])
optAccessor(getTsMultirange, getTsMultirangeOpt, PgMultirange[DateTime])
optAccessor(getTsTzMultirange, getTsTzMultirangeOpt, PgMultirange[DateTime])
optAccessor(getDateMultirange, getDateMultirangeOpt, PgMultirange[DateTime])

# Multirange array type support

proc getInt4MultirangeArray*(row: Row, col: int): seq[PgMultirange[int32]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgMultirange[int32]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in multirange array")
      let parts = decodeMultirangeBinaryRaw(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
      var ranges = newSeq[PgRange[int32]](parts.len)
      for j, p in parts:
        ranges[j] = decodeInt4RangeBinary(
          row.data.buf.toOpenArray(off + e.off + p.off, off + e.off + p.off + p.len - 1)
        )
      result[i] = PgMultirange[int32](ranges)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in multirange array")
    result.add(
      parseMultirangeText[int32](
        e.get,
        proc(x: string): int32 {.gcsafe, raises: [CatchableError].} =
          int32(parseInt(x)),
      )
    )

proc getInt8MultirangeArray*(row: Row, col: int): seq[PgMultirange[int64]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgMultirange[int64]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in multirange array")
      let parts = decodeMultirangeBinaryRaw(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
      var ranges = newSeq[PgRange[int64]](parts.len)
      for j, p in parts:
        ranges[j] = decodeInt8RangeBinary(
          row.data.buf.toOpenArray(off + e.off + p.off, off + e.off + p.off + p.len - 1)
        )
      result[i] = PgMultirange[int64](ranges)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in multirange array")
    result.add(
      parseMultirangeText[int64](
        e.get,
        proc(x: string): int64 {.gcsafe, raises: [CatchableError].} =
          parseBiggestInt(x),
      )
    )

proc getNumMultirangeArray*(row: Row, col: int): seq[PgMultirange[PgNumeric]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgMultirange[PgNumeric]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in multirange array")
      let parts = decodeMultirangeBinaryRaw(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
      var ranges = newSeq[PgRange[PgNumeric]](parts.len)
      for j, p in parts:
        ranges[j] = decodeNumRangeBinary(
          row.data.buf.toOpenArray(off + e.off + p.off, off + e.off + p.off + p.len - 1)
        )
      result[i] = PgMultirange[PgNumeric](ranges)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in multirange array")
    result.add(
      parseMultirangeText[PgNumeric](
        e.get,
        proc(x: string): PgNumeric {.gcsafe, raises: [CatchableError].} =
          parsePgNumeric(x),
      )
    )

proc getTsMultirangeArray*(row: Row, col: int): seq[PgMultirange[DateTime]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgMultirange[DateTime]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in multirange array")
      let parts = decodeMultirangeBinaryRaw(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
      var ranges = newSeq[PgRange[DateTime]](parts.len)
      for j, p in parts:
        ranges[j] = decodeTsRangeBinary(
          row.data.buf.toOpenArray(off + e.off + p.off, off + e.off + p.off + p.len - 1)
        )
      result[i] = PgMultirange[DateTime](ranges)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in multirange array")
    result.add(
      parseMultirangeText[DateTime](
        e.get,
        proc(x: string): DateTime {.gcsafe, raises: [CatchableError].} =
          const formats = ["yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:ss"]
          for fmt in formats:
            try:
              return parse(x, fmt)
            except TimeParseError, IndexDefect:
              discard
          raise newException(PgTypeError, "Invalid timestamp in multirange: " & x),
      )
    )

proc getTsTzMultirangeArray*(row: Row, col: int): seq[PgMultirange[DateTime]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgMultirange[DateTime]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in multirange array")
      let parts = decodeMultirangeBinaryRaw(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
      var ranges = newSeq[PgRange[DateTime]](parts.len)
      for j, p in parts:
        ranges[j] = decodeTsRangeBinary(
          row.data.buf.toOpenArray(off + e.off + p.off, off + e.off + p.off + p.len - 1)
        )
      result[i] = PgMultirange[DateTime](ranges)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in multirange array")
    result.add(
      parseMultirangeText[DateTime](
        e.get,
        proc(x: string): DateTime {.gcsafe, raises: [CatchableError].} =
          const formats = [
            "yyyy-MM-dd HH:mm:ss'.'ffffffzzz", "yyyy-MM-dd HH:mm:ss'.'ffffffzz",
            "yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:sszzz",
            "yyyy-MM-dd HH:mm:sszz", "yyyy-MM-dd HH:mm:ss",
          ]
          for fmt in formats:
            try:
              return parse(x, fmt)
            except TimeParseError, IndexDefect:
              discard
          raise newException(PgTypeError, "Invalid timestamptz in multirange: " & x),
      )
    )

proc getDateMultirangeArray*(row: Row, col: int): seq[PgMultirange[DateTime]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgMultirange[DateTime]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in multirange array")
      let parts = decodeMultirangeBinaryRaw(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
      var ranges = newSeq[PgRange[DateTime]](parts.len)
      for j, p in parts:
        ranges[j] = decodeDateRangeBinary(
          row.data.buf.toOpenArray(off + e.off + p.off, off + e.off + p.off + p.len - 1)
        )
      result[i] = PgMultirange[DateTime](ranges)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in multirange array")
    result.add(
      parseMultirangeText[DateTime](
        e.get,
        proc(x: string): DateTime {.gcsafe, raises: [CatchableError].} =
          try:
            return parse(x, "yyyy-MM-dd")
          except TimeParseError:
            raise newException(PgTypeError, "Invalid date in multirange: " & x),
      )
    )

optAccessor(getInt4MultirangeArray, getInt4MultirangeArrayOpt, seq[PgMultirange[int32]])
optAccessor(getInt8MultirangeArray, getInt8MultirangeArrayOpt, seq[PgMultirange[int64]])
optAccessor(
  getNumMultirangeArray, getNumMultirangeArrayOpt, seq[PgMultirange[PgNumeric]]
)
optAccessor(getTsMultirangeArray, getTsMultirangeArrayOpt, seq[PgMultirange[DateTime]])
optAccessor(
  getTsTzMultirangeArray, getTsTzMultirangeArrayOpt, seq[PgMultirange[DateTime]]
)
optAccessor(
  getDateMultirangeArray, getDateMultirangeArrayOpt, seq[PgMultirange[DateTime]]
)

# Range array type support

proc getInt4RangeArray*(row: Row, col: int): seq[PgRange[int32]] =
  ## Get a column value as an ``int4range[]``. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgRange[int32]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in range array")
      result[i] = decodeInt4RangeBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in range array")
    result.add(
      parseRangeText[int32](
        e.get,
        proc(x: string): int32 {.gcsafe, raises: [CatchableError].} =
          int32(parseInt(x)),
      )
    )

proc getInt8RangeArray*(row: Row, col: int): seq[PgRange[int64]] =
  ## Get a column value as an ``int8range[]``. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgRange[int64]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in range array")
      result[i] = decodeInt8RangeBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in range array")
    result.add(
      parseRangeText[int64](
        e.get,
        proc(x: string): int64 {.gcsafe, raises: [CatchableError].} =
          parseBiggestInt(x),
      )
    )

proc getNumRangeArray*(row: Row, col: int): seq[PgRange[PgNumeric]] =
  ## Get a column value as a ``numrange[]``. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgRange[PgNumeric]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in range array")
      result[i] = decodeNumRangeBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in range array")
    result.add(
      parseRangeText[PgNumeric](
        e.get,
        proc(x: string): PgNumeric {.gcsafe, raises: [CatchableError].} =
          parsePgNumeric(x),
      )
    )

proc getTsRangeArray*(row: Row, col: int): seq[PgRange[DateTime]] =
  ## Get a column value as a ``tsrange[]``. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgRange[DateTime]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in range array")
      result[i] = decodeTsRangeBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in range array")
    result.add(
      parseRangeText[DateTime](
        e.get,
        proc(x: string): DateTime {.gcsafe, raises: [CatchableError].} =
          const formats = ["yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:ss"]
          for fmt in formats:
            try:
              return parse(x, fmt)
            except TimeParseError:
              discard
          raise newException(PgTypeError, "Invalid timestamp in range array: " & x),
      )
    )

proc getTsTzRangeArray*(row: Row, col: int): seq[PgRange[DateTime]] =
  ## Get a column value as a ``tstzrange[]``. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgRange[DateTime]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in range array")
      result[i] = decodeTsRangeBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in range array")
    result.add(
      parseRangeText[DateTime](
        e.get,
        proc(x: string): DateTime {.gcsafe, raises: [CatchableError].} =
          const formats = [
            "yyyy-MM-dd HH:mm:ss'.'ffffffzzz", "yyyy-MM-dd HH:mm:ss'.'ffffffzz",
            "yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:sszzz",
            "yyyy-MM-dd HH:mm:sszz", "yyyy-MM-dd HH:mm:ss",
          ]
          for fmt in formats:
            try:
              return parse(x, fmt)
            except TimeParseError:
              discard
          raise newException(PgTypeError, "Invalid timestamptz in range array: " & x),
      )
    )

proc getDateRangeArray*(row: Row, col: int): seq[PgRange[DateTime]] =
  ## Get a column value as a ``daterange[]``. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgRange[DateTime]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in range array")
      result[i] = decodeDateRangeBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in range array")
    result.add(
      parseRangeText[DateTime](
        e.get,
        proc(x: string): DateTime {.gcsafe, raises: [CatchableError].} =
          try:
            return parse(x, "yyyy-MM-dd")
          except TimeParseError:
            raise newException(PgTypeError, "Invalid date in range array: " & x),
      )
    )

# Range array Opt accessors

optAccessor(getInt4RangeArray, getInt4RangeArrayOpt, seq[PgRange[int32]])
optAccessor(getInt8RangeArray, getInt8RangeArrayOpt, seq[PgRange[int64]])
optAccessor(getNumRangeArray, getNumRangeArrayOpt, seq[PgRange[PgNumeric]])
optAccessor(getTsRangeArray, getTsRangeArrayOpt, seq[PgRange[DateTime]])
optAccessor(getTsTzRangeArray, getTsTzRangeArrayOpt, seq[PgRange[DateTime]])
optAccessor(getDateRangeArray, getDateRangeArrayOpt, seq[PgRange[DateTime]])

# Range/Multirange generic `get*` dispatchers

proc get*(row: Row, col: int, T: typedesc[PgRange[int32]]): PgRange[int32] =
  row.getInt4Range(col)

proc get*(row: Row, col: int, T: typedesc[PgRange[int64]]): PgRange[int64] =
  row.getInt8Range(col)

proc get*(row: Row, col: int, T: typedesc[PgRange[PgNumeric]]): PgRange[PgNumeric] =
  row.getNumRange(col)

proc get*(row: Row, col: int, T: typedesc[PgMultirange[int32]]): PgMultirange[int32] =
  row.getInt4Multirange(col)

proc get*(row: Row, col: int, T: typedesc[PgMultirange[int64]]): PgMultirange[int64] =
  row.getInt8Multirange(col)

proc get*(
    row: Row, col: int, T: typedesc[PgMultirange[PgNumeric]]
): PgMultirange[PgNumeric] =
  row.getNumMultirange(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgRange[int32]]]): seq[PgRange[int32]] =
  row.getInt4RangeArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgRange[int64]]]): seq[PgRange[int64]] =
  row.getInt8RangeArray(col)

proc get*(
    row: Row, col: int, T: typedesc[seq[PgRange[PgNumeric]]]
): seq[PgRange[PgNumeric]] =
  row.getNumRangeArray(col)

proc get*(
    row: Row, col: int, T: typedesc[seq[PgMultirange[int32]]]
): seq[PgMultirange[int32]] =
  row.getInt4MultirangeArray(col)

proc get*(
    row: Row, col: int, T: typedesc[seq[PgMultirange[int64]]]
): seq[PgMultirange[int64]] =
  row.getInt8MultirangeArray(col)

proc get*(
    row: Row, col: int, T: typedesc[seq[PgMultirange[PgNumeric]]]
): seq[PgMultirange[PgNumeric]] =
  row.getNumMultirangeArray(col)
