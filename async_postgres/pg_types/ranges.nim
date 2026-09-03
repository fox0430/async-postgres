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

  RangeBinaryRaw =
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

proc decodeRangeBinaryRaw(data: openArray[byte]): RangeBinaryRaw =
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

proc decodeInt4RangeBinary(data: openArray[byte]): PgRange[int32] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[int32](isEmpty: true)
  if raw.hasLower:
    if raw.lowerLen != 4:
      raise newException(
        PgTypeError, "Binary int4range: invalid lower bound length " & $raw.lowerLen
      )
    result.hasLower = true
    result.lower = PgRangeBound[int32](
      value: fromBE32(data.toOpenArray(raw.lowerOff, raw.lowerOff + 3)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    if raw.upperLen != 4:
      raise newException(
        PgTypeError, "Binary int4range: invalid upper bound length " & $raw.upperLen
      )
    result.hasUpper = true
    result.upper = PgRangeBound[int32](
      value: fromBE32(data.toOpenArray(raw.upperOff, raw.upperOff + 3)),
      inclusive: raw.upperInc,
    )

proc decodeInt8RangeBinary(data: openArray[byte]): PgRange[int64] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[int64](isEmpty: true)
  if raw.hasLower:
    if raw.lowerLen != 8:
      raise newException(
        PgTypeError, "Binary int8range: invalid lower bound length " & $raw.lowerLen
      )
    result.hasLower = true
    result.lower = PgRangeBound[int64](
      value: fromBE64(data.toOpenArray(raw.lowerOff, raw.lowerOff + 7)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    if raw.upperLen != 8:
      raise newException(
        PgTypeError, "Binary int8range: invalid upper bound length " & $raw.upperLen
      )
    result.hasUpper = true
    result.upper = PgRangeBound[int64](
      value: fromBE64(data.toOpenArray(raw.upperOff, raw.upperOff + 7)),
      inclusive: raw.upperInc,
    )

proc decodeNumRangeBinary(data: openArray[byte]): PgRange[PgNumeric] =
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

proc decodeTsRangeBinary(data: openArray[byte]): PgRange[DateTime] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[DateTime](isEmpty: true)
  if raw.hasLower:
    if raw.lowerLen != 8:
      raise newException(
        PgTypeError, "Binary tsrange: invalid lower bound length " & $raw.lowerLen
      )
    result.hasLower = true
    result.lower = PgRangeBound[DateTime](
      value: decodeBinaryTimestamp(data.toOpenArray(raw.lowerOff, raw.lowerOff + 7)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    if raw.upperLen != 8:
      raise newException(
        PgTypeError, "Binary tsrange: invalid upper bound length " & $raw.upperLen
      )
    result.hasUpper = true
    result.upper = PgRangeBound[DateTime](
      value: decodeBinaryTimestamp(data.toOpenArray(raw.upperOff, raw.upperOff + 7)),
      inclusive: raw.upperInc,
    )

proc decodeDateRangeBinary(data: openArray[byte]): PgRange[DateTime] =
  let raw = decodeRangeBinaryRaw(data)
  if raw.isEmpty:
    return PgRange[DateTime](isEmpty: true)
  if raw.hasLower:
    if raw.lowerLen != 4:
      raise newException(
        PgTypeError, "Binary daterange: invalid lower bound length " & $raw.lowerLen
      )
    result.hasLower = true
    result.lower = PgRangeBound[DateTime](
      value: decodeBinaryDate(data.toOpenArray(raw.lowerOff, raw.lowerOff + 3)),
      inclusive: raw.lowerInc,
    )
  if raw.hasUpper:
    if raw.upperLen != 4:
      raise newException(
        PgTypeError, "Binary daterange: invalid upper bound length " & $raw.upperLen
      )
    result.hasUpper = true
    result.upper = PgRangeBound[DateTime](
      value: decodeBinaryDate(data.toOpenArray(raw.upperOff, raw.upperOff + 3)),
      inclusive: raw.upperInc,
    )

proc decodeMultirangeBinaryRaw(
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
  # Each range carries at least a 4-byte length prefix after the 4-byte count,
  # so count cannot exceed (data.len - 4) div 4. This guard stops a crafted
  # count from triggering a multi-GB allocation on malformed input.
  if count > (data.len - 4) div 4:
    raise newException(PgTypeError, "Binary multirange: count exceeds data")
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
    var closed = false
    while i < s.len:
      if s[i] == '\\' and i + 1 < s.len:
        i += 1
        elem.add(s[i])
      elif s[i] == '"':
        i += 1
        closed = true
        break
      else:
        elem.add(s[i])
      i += 1
    if not closed:
      raise newException(PgTypeError, "range: unterminated quoted element")
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
  if s[0] notin {'[', '('}:
    raise newException(PgTypeError, "range: invalid lower boundary: " & s)
  if s[^1] notin {']', ')'}:
    raise newException(PgTypeError, "range: invalid upper boundary: " & s)
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
    let (val, pos) = parseRangeElem(lowerStr, 0, {','})
    if pos != lowerStr.len:
      raise newException(
        PgTypeError, "range: trailing bytes after lower element: " & lowerStr
      )
    result.hasLower = true
    result.lower = PgRangeBound[T](value: parseElem(val), inclusive: lowerInc)
  # Parse upper bound
  if upperStr.len > 0:
    let (val, pos) = parseRangeElem(upperStr, 0, {','})
    if pos != upperStr.len:
      raise newException(
        PgTypeError, "range: trailing bytes after upper element: " & upperStr
      )
    result.hasUpper = true
    result.upper = PgRangeBound[T](value: parseElem(val), inclusive: upperInc)

proc encodeRangeBinaryImpl(
    r: RangeBinaryInput
): seq[byte] {.raises: [PgTypeError, PgProtocolError].} =
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
  var size: int64 = 1
  if r.hasLower:
    checkPgBinLen(r.lowerData.len, "Range bound")
    size += 4'i64 + r.lowerData.len.int64
  if r.hasUpper:
    checkPgBinLen(r.upperData.len, "Range bound")
    size += 4'i64 + r.upperData.len.int64
  checkPgBinPayload(size, "Range")
  result = newSeq[byte](size.int)
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

proc formatDateTimeRangeText(
    v: PgRange[DateTime], fmt: TimeFormat, utc = false
): string =
  ## `utc` formats the UTC wall clock so zoned DateTimes sent as tsrange
  ## (no zone in `fmt`) match the scalar OidTimestamp path.
  if v.isEmpty:
    return "empty"

  proc fmtBound(dt: DateTime): string =
    (if utc: dt.utc else: dt).format(fmt)

  result = if v.hasLower and v.lower.inclusive: "[" else: "("
  if v.hasLower:
    result.add(quoteRangeElem(fmtBound(v.lower.value)))
  result.add(',')
  if v.hasUpper:
    result.add(quoteRangeElem(fmtBound(v.upper.value)))
  result.add(if v.hasUpper and v.upper.inclusive: "]" else: ")")

# Parsed once, so `format` cannot raise `TimeFormatParseError` and the text
# encoders keep a `PgTypeError`-only contract.
const
  pgTsRangeFmt = initTimeFormat("yyyy-MM-dd HH:mm:ss'.'ffffff")
  pgTsTzRangeFmt = initTimeFormat("yyyy-MM-dd HH:mm:ss'.'ffffffzzz")
  pgDateRangeFmt = initTimeFormat("yyyy-MM-dd")

proc toPgParam*(v: PgRange[DateTime]): PgParam =
  PgParam(
    oid: OidTsRange,
    format: 0,
    value: some(toBytes(formatDateTimeRangeText(v, pgTsRangeFmt, utc = true))),
  )

proc toPgTsTzRangeParam*(v: PgRange[DateTime]): PgParam =
  PgParam(
    oid: OidTsTzRange,
    format: 0,
    value: some(toBytes(formatDateTimeRangeText(v, pgTsTzRangeFmt))),
  )

proc toPgDateRangeParam*(v: PgRange[DateTime]): PgParam =
  ## Encode a date range. DateTime values are formatted as date-only, taking the
  ## UTC calendar day so zoned bounds match the binary ``pgDateDays`` path.
  PgParam(
    oid: OidDateRange,
    format: 0,
    value: some(toBytes(formatDateTimeRangeText(v, pgDateRangeFmt, utc = true))),
  )

proc toPgRangeParam*[T](v: PgRange[T], oid: int32): PgParam =
  PgParam(oid: oid, format: 0, value: some(toBytes($v)))

# Binary encoding helpers

proc encodeBinaryTimestamp(dt: DateTime): seq[byte] =
  ## Shares the epoch math with ``encoding.pgTimestampMicros`` so the range and
  ## scalar/array timestamp paths cannot drift.
  @(toBE64(pgTimestampMicros(dt)))

proc encodeBinaryDate(dt: DateTime): seq[byte] =
  ## Shares the epoch math with ``encoding.pgDateDays`` (see above).
  @(toBE32(pgDateDays(dt)))

proc encodeRangeBinary[T](
    v: PgRange[T],
    oid: int32,
    encodeBound: proc(v: T): seq[byte] {.raises: [PgTypeError, PgProtocolError].},
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
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

proc toPgBinaryParam*(
    v: PgRange[int32]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  encodeRangeBinary(
    v,
    OidInt4Range,
    proc(x: int32): seq[byte] =
      @(toBE32(x)),
  )

proc toPgBinaryParam*(
    v: PgRange[int64]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  encodeRangeBinary(
    v,
    OidInt8Range,
    proc(x: int64): seq[byte] =
      @(toBE64(x)),
  )

proc toPgBinaryParam*(
    v: PgRange[PgNumeric]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  encodeRangeBinary(v, OidNumRange, encodeNumericBinary)

proc toPgBinaryParam*(
    v: PgRange[DateTime]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  encodeRangeBinary(v, OidTsRange, encodeBinaryTimestamp)

proc toPgBinaryTsTzRangeParam*(
    v: PgRange[DateTime]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  encodeRangeBinary(v, OidTsTzRange, encodeBinaryTimestamp)

proc toPgBinaryDateRangeParam*(
    v: PgRange[DateTime]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  encodeRangeBinary(v, OidDateRange, encodeBinaryDate)

# toPgBinaryParam for range array types

proc toPgBinaryParam*(
    v: seq[PgRange[int32]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, r in v:
    elements[i] = some(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidInt4RangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidInt4Range, dimsFor1D(v.len), elements)),
  )

proc toPgBinaryParam*(
    v: seq[PgRange[int64]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, r in v:
    elements[i] = some(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidInt8RangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidInt8Range, dimsFor1D(v.len), elements)),
  )

proc toPgBinaryParam*(
    v: seq[PgRange[PgNumeric]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, r in v:
    elements[i] = some(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidNumRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidNumRange, dimsFor1D(v.len), elements)),
  )

proc toPgBinaryParam*(
    v: seq[PgRange[DateTime]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, r in v:
    elements[i] = some(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidTsRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidTsRange, dimsFor1D(v.len), elements)),
  )

proc toPgBinaryTsTzRangeArrayParam*(
    v: seq[PgRange[DateTime]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, r in v:
    elements[i] = some(toPgBinaryTsTzRangeParam(r).value.get)
  PgParam(
    oid: OidTsTzRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidTsTzRange, dimsFor1D(v.len), elements)),
  )

proc toPgBinaryDateRangeArrayParam*(
    v: seq[PgRange[DateTime]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, r in v:
    elements[i] = some(toPgBinaryDateRangeParam(r).value.get)
  PgParam(
    oid: OidDateRangeArray,
    format: 1,
    value: some(encodeBinaryArray(OidDateRange, dimsFor1D(v.len), elements)),
  )

# toPgParam for range array types (text format)

proc appendQuotedArrayElem(s: var string, elem: string) =
  s.add('"')
  for c in elem:
    if c == '"' or c == '\\':
      s.add('\\')
    s.add(c)
  s.add('"')

proc encodeRangeArrayText[T](v: seq[PgRange[T]]): string =
  result = "{"
  for i, r in v:
    if i > 0:
      result.add(',')
    appendQuotedArrayElem(result, $r)
    # Per element, so an oversized array fails before the literal is built whole.
    # `+ 1` reserves the closing brace appended after the loop.
    checkPgBinLen(result.len + 1, "range array")
  result.add('}')

proc toPgParam*(v: seq[PgRange[int32]]): PgParam {.raises: [PgTypeError].} =
  PgParam(
    oid: OidInt4RangeArray, format: 0, value: some(toBytes(encodeRangeArrayText(v)))
  )

proc toPgParam*(v: seq[PgRange[int64]]): PgParam {.raises: [PgTypeError].} =
  PgParam(
    oid: OidInt8RangeArray, format: 0, value: some(toBytes(encodeRangeArrayText(v)))
  )

proc toPgParam*(v: seq[PgRange[PgNumeric]]): PgParam {.raises: [PgTypeError].} =
  PgParam(
    oid: OidNumRangeArray, format: 0, value: some(toBytes(encodeRangeArrayText(v)))
  )

proc encodeDateTimeRangeArrayText(
    v: seq[PgRange[DateTime]], fmt: TimeFormat, utc = false
): string =
  result = "{"
  for i, r in v:
    if i > 0:
      result.add(',')
    appendQuotedArrayElem(result, formatDateTimeRangeText(r, fmt, utc))
    checkPgBinLen(result.len + 1, "range array")
  result.add('}')

proc toPgParam*(v: seq[PgRange[DateTime]]): PgParam {.raises: [PgTypeError].} =
  ## Encode a ``tsrange[]`` (timestamp without time zone, array). For
  ## ``tstzrange[]`` use ``toPgTsTzRangeArrayParam``; for ``daterange[]`` use
  ## ``toPgDateRangeArrayParam``.
  PgParam(
    oid: OidTsRangeArray,
    format: 0,
    value: some(toBytes(encodeDateTimeRangeArrayText(v, pgTsRangeFmt, utc = true))),
  )

proc toPgTsTzRangeArrayParam*(
    v: seq[PgRange[DateTime]]
): PgParam {.raises: [PgTypeError].} =
  ## Encode a ``tstzrange[]`` (timestamp with time zone, array). Disambiguates
  ## from ``toPgParam(seq[PgRange[DateTime]])`` which produces ``tsrange[]``.
  PgParam(
    oid: OidTsTzRangeArray,
    format: 0,
    value: some(toBytes(encodeDateTimeRangeArrayText(v, pgTsTzRangeFmt))),
  )

proc toPgDateRangeArrayParam*(
    v: seq[PgRange[DateTime]]
): PgParam {.raises: [PgTypeError].} =
  ## Encode a ``daterange[]``. DateTime values are formatted as date-only, taking
  ## the UTC calendar day so zoned bounds match the binary ``pgDateDays`` path.
  PgParam(
    oid: OidDateRangeArray,
    format: 0,
    value: some(toBytes(encodeDateTimeRangeArrayText(v, pgDateRangeFmt, utc = true))),
  )

# Range text format getters

template genRangeGetter(name: untyped, T: typedesc, decodeBin, parseElem: untyped) =
  proc name*(row: Row, col: int): PgRange[T] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      return decodeBin(row.data.buf.toOpenArray(off, off + clen - 1))
    let s = row.getStr(col)
    parseRangeText[T](s, parseElem)

genRangeGetter(getInt4Range, int32, decodeInt4RangeBinary, pgParseInt32)
genRangeGetter(getInt8Range, int64, decodeInt8RangeBinary, pgParseBiggestInt)
genRangeGetter(getNumRange, PgNumeric, decodeNumRangeBinary, parsePgNumeric)
genRangeGetter(getTsRange, DateTime, decodeTsRangeBinary, parseTimestampText)
genRangeGetter(getTsTzRange, DateTime, decodeTsRangeBinary, parseTimestampText)
genRangeGetter(getDateRange, DateTime, decodeDateRangeBinary, parseDateText)

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
  var ranges: seq[PgRange[T]]
  var i = 0
  while true:
    # Parse one element: either the literal "empty" or a bracketed range.
    if inner[i] == '[' or inner[i] == '(':
      # Scan to the matching closing bracket, honoring quoted-string state so
      # brackets inside "..." (a quoted element value) don't affect nesting.
      var j = i + 1
      var inQuote = false
      var closed = false
      while j < inner.len:
        let c = inner[j]
        if inQuote:
          if c == '\\' and j + 1 < inner.len:
            j += 2
          elif c == '"':
            inQuote = false
            j += 1
          else:
            j += 1
        else:
          if c == '"':
            inQuote = true
            j += 1
          elif c == ']' or c == ')':
            j += 1
            closed = true
            break
          else:
            j += 1
      if not closed:
        raise newException(PgTypeError, "multirange: unterminated range in: " & s)
      ranges.add(parseRangeText[T](inner[i ..< j], parseElem))
      i = j
    elif i + 5 <= inner.len and inner[i ..< i + 5] == "empty":
      ranges.add(PgRange[T](isEmpty: true))
      i += 5
    else:
      raise newException(PgTypeError, "multirange: expected range or 'empty' in: " & s)
    if i == inner.len:
      break
    if inner[i] != ',':
      raise newException(
        PgTypeError, "multirange: expected ',' or end after element in: " & s
      )
    i += 1
    # `$` in this module emits ", " between ranges; tolerate the optional space.
    while i < inner.len and inner[i] == ' ':
      i += 1
    if i == inner.len:
      raise newException(PgTypeError, "multirange: trailing ',' in: " & s)
  PgMultirange[T](ranges)

proc encodeMultirangeBinaryImpl(
    rangeData: seq[seq[byte]]
): seq[byte] {.raises: [PgTypeError, PgProtocolError].} =
  checkPgBinLen(rangeData.len, "Multirange range count")
  var size: int64 = 4
  for rd in rangeData:
    checkPgBinLen(rd.len, "Multirange range")
    size += 4'i64 + rd.len.int64
    checkPgBinPayload(size, "Multirange")
  result = newSeq[byte](size.int)
  result.writeBE32(0, int32(rangeData.len))
  var pos = 4
  for rd in rangeData:
    result.writeBE32(pos, int32(rd.len))
    pos += 4
    result.writeBytesAt(pos, rd)
    pos += rd.len

# Multirange toPgParam (text format)

proc toPgParam*(v: PgMultirange[int32]): PgParam {.raises: [PgTypeError].} =
  let s = $v
  checkPgBinLen(s.len, "multirange")
  PgParam(oid: OidInt4Multirange, format: 0, value: some(toBytes(s)))

proc toPgParam*(v: PgMultirange[int64]): PgParam {.raises: [PgTypeError].} =
  let s = $v
  checkPgBinLen(s.len, "multirange")
  PgParam(oid: OidInt8Multirange, format: 0, value: some(toBytes(s)))

proc toPgParam*(v: PgMultirange[PgNumeric]): PgParam {.raises: [PgTypeError].} =
  let s = $v
  checkPgBinLen(s.len, "multirange")
  PgParam(oid: OidNumMultirange, format: 0, value: some(toBytes(s)))

proc toPgParam*(v: PgMultirange[DateTime]): PgParam {.raises: [PgTypeError].} =
  ## Encode a ts multirange. DateTime bounds are formatted via UTC so that
  ## zoned values match the scalar ``toPgParam(DateTime)`` path.
  var s = "{"
  let ranges = seq[PgRange[DateTime]](v)
  for i, r in ranges:
    if i > 0:
      s.add(',')
    s.add(formatDateTimeRangeText(r, pgTsRangeFmt, utc = true))
    checkPgBinLen(s.len + 1, "multirange")
  s.add('}')
  PgParam(oid: OidTsMultirange, format: 0, value: some(toBytes(s)))

proc toPgTsTzMultirangeParam*(
    v: PgMultirange[DateTime]
): PgParam {.raises: [PgTypeError].} =
  var s = "{"
  let ranges = seq[PgRange[DateTime]](v)
  for i, r in ranges:
    if i > 0:
      s.add(',')
    s.add(formatDateTimeRangeText(r, pgTsTzRangeFmt))
    checkPgBinLen(s.len + 1, "multirange")
  s.add('}')
  PgParam(oid: OidTsTzMultirange, format: 0, value: some(toBytes(s)))

proc toPgDateMultirangeParam*(
    v: PgMultirange[DateTime]
): PgParam {.raises: [PgTypeError].} =
  ## Encode a date multirange. DateTime values are formatted as date-only, taking
  ## the UTC calendar day so zoned bounds match the binary ``pgDateDays`` path.
  var s = "{"
  let ranges = seq[PgRange[DateTime]](v)
  for i, r in ranges:
    if i > 0:
      s.add(',')
    s.add(formatDateTimeRangeText(r, pgDateRangeFmt, utc = true))
    checkPgBinLen(s.len + 1, "multirange")
  s.add('}')
  PgParam(oid: OidDateMultirange, format: 0, value: some(toBytes(s)))

proc toPgMultirangeParam*[T](
    v: PgMultirange[T], oid: int32
): PgParam {.raises: [PgTypeError].} =
  let s = $v
  checkPgBinLen(s.len, "multirange")
  PgParam(oid: oid, format: 0, value: some(toBytes(s)))

# Multirange toPgBinaryParam

proc toPgBinaryParam*(
    v: PgMultirange[int32]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[int32]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidInt4Multirange,
    format: 1,
    value: some(encodeMultirangeBinaryImpl(rangeData)),
  )

proc toPgBinaryParam*(
    v: PgMultirange[int64]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[int64]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidInt8Multirange,
    format: 1,
    value: some(encodeMultirangeBinaryImpl(rangeData)),
  )

proc toPgBinaryParam*(
    v: PgMultirange[PgNumeric]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[PgNumeric]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidNumMultirange, format: 1, value: some(encodeMultirangeBinaryImpl(rangeData))
  )

proc toPgBinaryParam*(
    v: PgMultirange[DateTime]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[DateTime]](v):
    rangeData.add(toPgBinaryParam(r).value.get)
  PgParam(
    oid: OidTsMultirange, format: 1, value: some(encodeMultirangeBinaryImpl(rangeData))
  )

proc toPgBinaryTsTzMultirangeParam*(
    v: PgMultirange[DateTime]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  ## Encode a ``tstzmultirange``. Disambiguates from
  ## ``toPgBinaryParam(PgMultirange[DateTime])`` which produces
  ## ``tsmultirange``.
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[DateTime]](v):
    rangeData.add(toPgBinaryTsTzRangeParam(r).value.get)
  PgParam(
    oid: OidTsTzMultirange,
    format: 1,
    value: some(encodeMultirangeBinaryImpl(rangeData)),
  )

proc toPgBinaryDateMultirangeParam*(
    v: PgMultirange[DateTime]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  ## Encode a ``datemultirange``. DateTime values are encoded as date-only.
  var rangeData: seq[seq[byte]]
  for r in seq[PgRange[DateTime]](v):
    rangeData.add(toPgBinaryDateRangeParam(r).value.get)
  PgParam(
    oid: OidDateMultirange,
    format: 1,
    value: some(encodeMultirangeBinaryImpl(rangeData)),
  )

# toPgBinaryParam for multirange array types

proc buildMultirangeArrayParam(
    arrayOid, elemOid: int32, elements: seq[seq[byte]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(elements.len)
  var optElements = newSeq[Option[seq[byte]]](elements.len)
  for i, e in elements:
    optElements[i] = some(e)
  PgParam(
    oid: arrayOid,
    format: 1,
    value: some(encodeBinaryArray(elemOid, dimsFor1D(elements.len), optElements)),
  )

proc toPgBinaryParam*(
    v: seq[PgMultirange[int32]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[seq[byte]](v.len)
  for i, mr in v:
    elements[i] = toPgBinaryParam(mr).value.get
  buildMultirangeArrayParam(OidInt4MultirangeArray, OidInt4Multirange, elements)

proc toPgBinaryParam*(
    v: seq[PgMultirange[int64]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[seq[byte]](v.len)
  for i, mr in v:
    elements[i] = toPgBinaryParam(mr).value.get
  buildMultirangeArrayParam(OidInt8MultirangeArray, OidInt8Multirange, elements)

proc toPgBinaryParam*(
    v: seq[PgMultirange[PgNumeric]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  checkArrayLen(v.len)
  var elements = newSeq[seq[byte]](v.len)
  for i, mr in v:
    elements[i] = toPgBinaryParam(mr).value.get
  buildMultirangeArrayParam(OidNumMultirangeArray, OidNumMultirange, elements)

proc toPgBinaryParam*(
    v: seq[PgMultirange[DateTime]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  ## Encode a ``tsmultirange[]`` (timestamp without time zone). For
  ## ``tstzmultirange[]`` use ``toPgBinaryTsTzMultirangeArrayParam``; for
  ## ``datemultirange[]`` use ``toPgBinaryDateMultirangeArrayParam``.
  checkArrayLen(v.len)
  var elements = newSeq[seq[byte]](v.len)
  for i, mr in v:
    elements[i] = toPgBinaryParam(mr).value.get
  buildMultirangeArrayParam(OidTsMultirangeArray, OidTsMultirange, elements)

proc toPgBinaryTsTzMultirangeArrayParam*(
    v: seq[PgMultirange[DateTime]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  ## Encode a ``tstzmultirange[]``. Disambiguates from
  ## ``toPgBinaryParam(seq[PgMultirange[DateTime]])`` which produces
  ## ``tsmultirange[]``.
  checkArrayLen(v.len)
  var elements = newSeq[seq[byte]](v.len)
  for i, mr in v:
    elements[i] = toPgBinaryTsTzMultirangeParam(mr).value.get
  buildMultirangeArrayParam(OidTsTzMultirangeArray, OidTsTzMultirange, elements)

proc toPgBinaryDateMultirangeArrayParam*(
    v: seq[PgMultirange[DateTime]]
): PgParam {.raises: [PgTypeError, PgProtocolError].} =
  ## Encode a ``datemultirange[]``. DateTime values are encoded as date-only.
  checkArrayLen(v.len)
  var elements = newSeq[seq[byte]](v.len)
  for i, mr in v:
    elements[i] = toPgBinaryDateMultirangeParam(mr).value.get
  buildMultirangeArrayParam(OidDateMultirangeArray, OidDateMultirange, elements)

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
    checkPgBinLen(result.len + 1, "multirange array")
  result.add('}')

template genMultirangeArrayEncoder(T: typedesc, arrayOid: int32) =
  proc toPgParam*(v: seq[PgMultirange[T]]): PgParam {.raises: [PgTypeError].} =
    PgParam(
      oid: arrayOid, format: 0, value: some(toBytes(encodeMultirangeArrayText(v)))
    )

genMultirangeArrayEncoder(int32, OidInt4MultirangeArray)
genMultirangeArrayEncoder(int64, OidInt8MultirangeArray)
genMultirangeArrayEncoder(PgNumeric, OidNumMultirangeArray)

proc encodeDateTimeMultirangeArrayText(
    v: seq[PgMultirange[DateTime]], fmt: TimeFormat, utc = false
): string =
  result = "{"
  for i, x in v:
    if i > 0:
      result.add(',')
    result.add('"')
    var mrStr = "{"
    let ranges = seq[PgRange[DateTime]](x)
    for j, r in ranges:
      if j > 0:
        mrStr.add(',')
      mrStr.add(formatDateTimeRangeText(r, fmt, utc))
    mrStr.add('}')
    for c in mrStr:
      if c == '"' or c == '\\':
        result.add('\\')
      result.add(c)
    result.add('"')
    checkPgBinLen(result.len + 1, "multirange array")
  result.add('}')

proc toPgTsMultirangeArrayParam*(
    v: seq[PgMultirange[DateTime]]
): PgParam {.raises: [PgTypeError].} =
  ## Encode a ``tsmultirange[]``. DateTime bounds are formatted via UTC so that
  ## zoned values match the scalar ``toPgParam(DateTime)`` path.
  PgParam(
    oid: OidTsMultirangeArray,
    format: 0,
    value: some(toBytes(encodeDateTimeMultirangeArrayText(v, pgTsRangeFmt, utc = true))),
  )

proc toPgTsTzMultirangeArrayParam*(
    v: seq[PgMultirange[DateTime]]
): PgParam {.raises: [PgTypeError].} =
  PgParam(
    oid: OidTsTzMultirangeArray,
    format: 0,
    value: some(toBytes(encodeDateTimeMultirangeArrayText(v, pgTsTzRangeFmt))),
  )

proc toPgDateMultirangeArrayParam*(
    v: seq[PgMultirange[DateTime]]
): PgParam {.raises: [PgTypeError].} =
  ## Encode date multirange array. DateTime values are formatted as date-only,
  ## taking the UTC calendar day so zoned bounds match the binary
  ## ``pgDateDays`` path.
  PgParam(
    oid: OidDateMultirangeArray,
    format: 0,
    value:
      some(toBytes(encodeDateTimeMultirangeArrayText(v, pgDateRangeFmt, utc = true))),
  )

# Multirange text format getters

template genMultirangeGetter(
    name: untyped, T: typedesc, decodeBin, parseElem: untyped
) =
  proc name*(row: Row, col: int): PgMultirange[T] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let parts =
        decodeMultirangeBinaryRaw(row.data.buf.toOpenArray(off, off + clen - 1))
      var ranges = newSeq[PgRange[T]](parts.len)
      for i, p in parts:
        ranges[i] =
          decodeBin(row.data.buf.toOpenArray(off + p.off, off + p.off + p.len - 1))
      return PgMultirange[T](ranges)
    let s = row.getStr(col)
    parseMultirangeText[T](s, parseElem)

genMultirangeGetter(getInt4Multirange, int32, decodeInt4RangeBinary, pgParseInt32)
genMultirangeGetter(getInt8Multirange, int64, decodeInt8RangeBinary, pgParseBiggestInt)
genMultirangeGetter(getNumMultirange, PgNumeric, decodeNumRangeBinary, parsePgNumeric)
genMultirangeGetter(getTsMultirange, DateTime, decodeTsRangeBinary, parseTimestampText)
genMultirangeGetter(
  getTsTzMultirange, DateTime, decodeTsRangeBinary, parseTimestampText
)
genMultirangeGetter(getDateMultirange, DateTime, decodeDateRangeBinary, parseDateText)

# Multirange Opt accessors (text format)

optAccessor(getInt4Multirange, getInt4MultirangeOpt, PgMultirange[int32])
optAccessor(getInt8Multirange, getInt8MultirangeOpt, PgMultirange[int64])
optAccessor(getNumMultirange, getNumMultirangeOpt, PgMultirange[PgNumeric])
optAccessor(getTsMultirange, getTsMultirangeOpt, PgMultirange[DateTime])
optAccessor(getTsTzMultirange, getTsTzMultirangeOpt, PgMultirange[DateTime])
optAccessor(getDateMultirange, getDateMultirangeOpt, PgMultirange[DateTime])

# Multirange array type support

template genMultirangeArrayGetter(
    name: untyped, T: typedesc, decodeBin, parseElem: untyped
) =
  proc name*(row: Row, col: int): seq[PgMultirange[T]] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      rejectMultiDim(decoded)
      result = newSeq[PgMultirange[T]](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in multirange array")
        let parts = decodeMultirangeBinaryRaw(
          row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
        )
        var ranges = newSeq[PgRange[T]](parts.len)
        for j, p in parts:
          ranges[j] = decodeBin(
            row.data.buf.toOpenArray(
              off + e.off + p.off, off + e.off + p.off + p.len - 1
            )
          )
        result[i] = PgMultirange[T](ranges)
      return
    let s = row.getStr(col)
    let elems = parseTextArray(s)
    for e in elems:
      if e.isNone:
        raise newException(PgTypeError, "NULL element in multirange array")
      result.add(parseMultirangeText[T](e.get, parseElem))

genMultirangeArrayGetter(
  getInt4MultirangeArray, int32, decodeInt4RangeBinary, pgParseInt32
)
genMultirangeArrayGetter(
  getInt8MultirangeArray, int64, decodeInt8RangeBinary, pgParseBiggestInt
)
genMultirangeArrayGetter(
  getNumMultirangeArray, PgNumeric, decodeNumRangeBinary, parsePgNumeric
)
genMultirangeArrayGetter(
  getTsMultirangeArray, DateTime, decodeTsRangeBinary, parseTimestampText
)
genMultirangeArrayGetter(
  getTsTzMultirangeArray, DateTime, decodeTsRangeBinary, parseTimestampText
)
genMultirangeArrayGetter(
  getDateMultirangeArray, DateTime, decodeDateRangeBinary, parseDateText
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

template genRangeArrayGetter(
    name: untyped, T: typedesc, decodeBin, parseElem: untyped
) =
  proc name*(row: Row, col: int): seq[PgRange[T]] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      rejectMultiDim(decoded)
      result = newSeq[PgRange[T]](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in range array")
        result[i] =
          decodeBin(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1))
      return
    let s = row.getStr(col)
    let elems = parseTextArray(s)
    for e in elems:
      if e.isNone:
        raise newException(PgTypeError, "NULL element in range array")
      result.add(parseRangeText[T](e.get, parseElem))

genRangeArrayGetter(getInt4RangeArray, int32, decodeInt4RangeBinary, pgParseInt32)
genRangeArrayGetter(getInt8RangeArray, int64, decodeInt8RangeBinary, pgParseBiggestInt)
genRangeArrayGetter(getNumRangeArray, PgNumeric, decodeNumRangeBinary, parsePgNumeric)
genRangeArrayGetter(getTsRangeArray, DateTime, decodeTsRangeBinary, parseTimestampText)
genRangeArrayGetter(
  getTsTzRangeArray, DateTime, decodeTsRangeBinary, parseTimestampText
)
genRangeArrayGetter(getDateRangeArray, DateTime, decodeDateRangeBinary, parseDateText)

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
