import std/[hashes, options, sequtils, strutils, tables, net]

import ../pg_errors
export pg_errors

type
  RelOff* = distinct int
    ## Offset relative to the start of a slice that was passed into a binary
    ## decoder (e.g. ``decodeBinaryArray``, ``decodeBinaryComposite``,
    ## ``decodeMultirangeBinaryRaw``). Cannot be used directly as an index into
    ## the parent buffer — must be combined with the absolute origin via ``+``
    ## (``int + RelOff -> int``) to recover the absolute offset.

  PgUuid* = distinct string
    ## UUID value stored as its string representation (e.g. "550e8400-e29b-41d4-a716-446655440000").

  PgMoney* = object
    ## PostgreSQL money value. Stores the raw signed 64-bit amount in the
    ## locale's minor currency unit together with ``scale`` — the number of
    ## fractional digits (``frac_digits`` from ``lc_monetary``). The binary
    ## wire format carries only the integer amount, so ``scale`` is a
    ## client-side tag that defaults to 2 when decoding. Callers whose server
    ## runs with a non-default ``lc_monetary`` (e.g. ``ja_JP`` → ``scale=0``)
    ## must pass ``scale`` explicitly to ``parsePgMoney`` / ``getMoney`` /
    ## ``getMoneyArray``. Use ``formatPgMoney`` to render with a currency
    ## symbol and thousand separators; ``$`` emits a plain decimal number.
    amount*: int64 ## raw value in the minor currency unit
    scale*: int8 ## number of fractional digits (``frac_digits``)

  PgNumericSign* = enum
    pgPositive = 0x0000
    pgNegative = 0x4000
    pgNaN = 0xC000

  PgNumeric* = object
    ## Arbitrary-precision numeric value using PostgreSQL's internal base-10000 representation.
    ## Supports comparison operators but not arithmetic.
    ## Use this instead of float64 to avoid precision loss with PostgreSQL numeric/decimal.
    weight*: int16 ## exponent of first digit group (value = digit * 10000^weight)
    sign*: PgNumericSign ## positive, negative, or NaN
    dscale*: int16 ## number of digits after decimal point (display scale)
    digits*: seq[int16] ## base-10000 digit groups, each 0..9999

  PgInterval* = object
    ## PostgreSQL interval value decomposed into months, days, and microseconds.
    months*: int32
    days*: int32
    microseconds*: int64

  PgTime* = object ## PostgreSQL time without time zone.
    hour*: int32 ## 0..23
    minute*: int32 ## 0..59
    second*: int32 ## 0..59
    microsecond*: int32 ## 0..999999

  PgTimeTz* = object ## PostgreSQL time with time zone.
    hour*: int32 ## 0..23
    minute*: int32 ## 0..59
    second*: int32 ## 0..59
    microsecond*: int32 ## 0..999999
    utcOffset*: int32 ## UTC offset in seconds (positive = east of UTC)

  PgInet* = object ## PostgreSQL inet type: an IP address with a subnet mask.
    address*: IpAddress
    mask*: uint8

  PgCidr* = object ## PostgreSQL cidr type: a network address with a subnet mask.
    address*: IpAddress
    mask*: uint8

  PgMacAddr* = distinct string ## MAC address as "08:00:2b:01:02:03"

  PgMacAddr8* = distinct string ## EUI-64 MAC address as "08:00:2b:01:02:03:04:05"

  PgTsVector* = distinct string ## PostgreSQL tsvector (full-text search document).
  PgTsQuery* = distinct string ## PostgreSQL tsquery (full-text search query).

  PgXml* = distinct string ## PostgreSQL xml type.

  PgBit* = object ## PostgreSQL bit / bit varying type.
    nbits*: int32 ## number of bits
    data*: seq[byte] ## packed bit data (MSB first)

  PgPoint* = object ## PostgreSQL point type: (x, y).
    x*: float64
    y*: float64

  PgLine* = object ## PostgreSQL line type: {A, B, C} representing Ax + By + C = 0.
    a*: float64
    b*: float64
    c*: float64

  PgLseg* = object ## PostgreSQL lseg (line segment) type: ``[(x1,y1),(x2,y2)]``.
    p1*: PgPoint
    p2*: PgPoint

  PgBox* = object ## PostgreSQL box type: (upper-right),(lower-left).
    high*: PgPoint
    low*: PgPoint

  PgPath* = object ## PostgreSQL path type: open or closed sequence of points.
    closed*: bool
    points*: seq[PgPoint]

  PgPolygon* = object ## PostgreSQL polygon type: closed sequence of points.
    points*: seq[PgPoint]

  PgCircle* = object ## PostgreSQL circle type: <(x,y),r>.
    center*: PgPoint
    radius*: float64

  PgRangeBound*[T] = object ## One endpoint of a PostgreSQL range value.
    value*: T
    inclusive*: bool

  PgRange*[T] = object ## PostgreSQL range value (e.g. int4range, tsrange).
    isEmpty*: bool
    hasLower*: bool
    hasUpper*: bool
    lower*: PgRangeBound[T]
    upper*: PgRangeBound[T]

  PgMultirange*[T] = distinct seq[PgRange[T]]
    ## PostgreSQL multirange value (PostgreSQL 14+). A sorted set of non-overlapping ranges.

  PgHstore* = Table[string, Option[string]]
    ## PostgreSQL hstore type: a set of key/value pairs where values may be NULL.

  PgParam* = object
    ## A single query parameter in binary wire format, ready to send to PostgreSQL.
    oid*: int32
    format*: int16 # 0=text, 1=binary
    value*: Option[seq[byte]]

  PgParamInline* = object
    ## Heap-alloc-free parameter for scalar types. Binary payloads up to
    ## `PgInlineBufSize` bytes live in `inlineBuf`; longer values spill into
    ## `overflow`. Use `toPgParamInline` to construct; pass to the `openArray
    ## [PgParamInline]` overloads of `exec`, `query`, `addExec`, `addQuery`.
    oid*: int32
    format*: int16 # 0=text, 1=binary
    len*: int32
      ## -1 = NULL; 0..PgInlineBufSize uses `inlineBuf`;
      ## > PgInlineBufSize uses `overflow`.
    inlineBuf*: array[16, byte]
    overflow*: seq[byte]

  ResultFormat* = enum
    ## How result columns should be encoded by the server.
    rfAuto ## Per-column binary-safe detection via statement cache (default).
    rfText ## All columns in text format.
    rfBinary ## All columns in binary format.

  CommandResult* = object
    ## Result of a command execution, wrapping the PostgreSQL command tag.
    commandTag*: string
      ## Raw command tag string (e.g. "INSERT 0 1", "UPDATE 3", "DELETE 5").

const
  ## PostgreSQL type OIDs for scalar and array types.
  OidBool* = 16'i32
  OidInt2* = 21'i32
  OidInt4* = 23'i32
  OidInt8* = 20'i32
  OidFloat4* = 700'i32
  OidFloat8* = 701'i32
  OidText* = 25'i32
  OidVarchar* = 1043'i32
  OidBytea* = 17'i32
  OidTimestamp* = 1114'i32
  OidDate* = 1082'i32
  OidTime* = 1083'i32
  OidTimestampTz* = 1184'i32
  OidTimeTz* = 1266'i32
  OidNumeric* = 1700'i32
  OidMoney* = 790'i32
  OidJson* = 114'i32
  OidInterval* = 1186'i32
  OidUuid* = 2950'i32
  OidJsonb* = 3802'i32
  OidInet* = 869'i32
  OidCidr* = 650'i32
  OidMacAddr* = 829'i32
  OidMacAddr8* = 774'i32
  OidPoint* = 600'i32
  OidLseg* = 601'i32
  OidPath* = 602'i32
  OidBox* = 603'i32
  OidPolygon* = 604'i32
  OidLine* = 628'i32
  OidCircle* = 718'i32
  OidBoolArray* = 1000'i32
  OidInt2Array* = 1005'i32
  OidInt4Array* = 1007'i32
  OidInt8Array* = 1016'i32
  OidFloat4Array* = 1021'i32
  OidFloat8Array* = 1022'i32
  OidTextArray* = 1009'i32
  OidVarcharArray* = 1015'i32

  pgEpochUnix* = 946684800'i64 ## 2000-01-01 00:00:00 UTC in Unix seconds
  pgEpochDaysOffset* = 10957'i32 ## Days from 1970-01-01 to 2000-01-01

  OidRecord* = 2249'i32 ## Composite / anonymous record type OID.

  # Range types
  OidInt4Range* = 3904'i32
  OidNumRange* = 3906'i32
  OidTsRange* = 3908'i32
  OidTsTzRange* = 3910'i32
  OidDateRange* = 3912'i32
  OidInt8Range* = 3926'i32

  # Range array types
  OidInt4RangeArray* = 3905'i32
  OidNumRangeArray* = 3907'i32
  OidTsRangeArray* = 3909'i32
  OidTsTzRangeArray* = 3911'i32
  OidDateRangeArray* = 3913'i32
  OidInt8RangeArray* = 3927'i32

  # Multirange types (PostgreSQL 14+)
  OidInt4Multirange* = 4451'i32
  OidNumMultirange* = 4532'i32
  OidTsMultirange* = 4533'i32
  OidTsTzMultirange* = 4534'i32
  OidDateMultirange* = 4535'i32
  OidInt8Multirange* = 4536'i32

  # Multirange array types (PostgreSQL 14+)
  OidInt4MultirangeArray* = 6150'i32
  OidNumMultirangeArray* = 6151'i32
  OidTsMultirangeArray* = 6152'i32
  OidTsTzMultirangeArray* = 6153'i32
  OidDateMultirangeArray* = 6155'i32
  OidInt8MultirangeArray* = 6157'i32

  # Full-text search types
  OidTsVector* = 3614'i32
  OidTsQuery* = 3615'i32

  OidXml* = 142'i32

  OidBit* = 1560'i32
  OidVarbit* = 1562'i32
  OidByteaArray* = 1001'i32
  OidTimestampArray* = 1115'i32
  OidDateArray* = 1182'i32
  OidTimeArray* = 1183'i32
  OidTimestampTzArray* = 1185'i32
  OidIntervalArray* = 1187'i32
  OidNumericArray* = 1231'i32
  OidMoneyArray* = 791'i32
  OidTimeTzArray* = 1270'i32
  OidUuidArray* = 2951'i32
  OidJsonbArray* = 3807'i32
  OidInetArray* = 1041'i32
  OidCidrArray* = 651'i32
  OidMacAddrArray* = 1040'i32
  OidMacAddr8Array* = 775'i32
  OidPointArray* = 1017'i32
  OidLsegArray* = 1018'i32
  OidPathArray* = 1019'i32
  OidBoxArray* = 1020'i32
  OidPolygonArray* = 1027'i32
  OidLineArray* = 629'i32
  OidCircleArray* = 719'i32
  OidXmlArray* = 143'i32
  OidTsVectorArray* = 3643'i32
  OidTsQueryArray* = 3645'i32
  OidBitArray* = 1561'i32
  OidVarbitArray* = 1563'i32

  rangeEmpty* = 0x01'u8 ## Range flag: range is empty.
  rangeHasLower* = 0x02'u8 ## Range flag: lower bound present.
  rangeHasUpper* = 0x04'u8 ## Range flag: upper bound present.
  rangeLowerInc* = 0x08'u8 ## Range flag: lower bound is inclusive.
  rangeUpperInc* = 0x10'u8 ## Range flag: upper bound is inclusive.

  PgInlineBufSize* = 16
    ## Maximum payload size that fits in `PgParamInline.inlineBuf` without a
    ## heap allocation. Values longer than this are stored in `overflow`.

proc `+`*(a: int, b: RelOff): int {.inline.} =
  ## Combine an absolute parent-buffer origin with a relative decoder offset.
  ## ``RelOff`` cannot be added to itself or used as a buffer index directly,
  ## so any access path that omits the absolute origin fails to compile.
  a + int(b)

proc `==`*(a, b: RelOff): bool {.borrow.}
proc `$`*(v: RelOff): string {.borrow.}

proc `$`*(v: PgUuid): string {.borrow.}
proc `==`*(a, b: PgUuid): bool {.borrow.}
proc hash*(v: PgUuid): Hash {.borrow.}

proc initPgMoney*(amount: int64, scale: int = 2): PgMoney =
  ## Construct a PgMoney. ``amount`` is the raw integer in the minor currency
  ## unit; ``scale`` is the number of fractional digits (default 2).
  if scale < 0 or scale > 18:
    raise newException(PgTypeError, "PgMoney scale out of range: " & $scale)
  PgMoney(amount: amount, scale: int8(scale))

proc `<`*(a, b: PgMoney): bool =
  ## Order by ``amount``. Raises ``PgTypeError`` when ``a.scale != b.scale``
  ## because the raw amounts represent different minor units and comparing
  ## them would yield a nonsensical ordering.
  if a.scale != b.scale:
    raise newException(
      PgTypeError,
      "Cannot compare PgMoney with different scale: " & $a.scale & " vs " & $b.scale,
    )
  a.amount < b.amount

proc `<=`*(a, b: PgMoney): bool =
  ## Order by ``amount``. See ``<`` for the scale-mismatch behavior.
  if a.scale != b.scale:
    raise newException(
      PgTypeError,
      "Cannot compare PgMoney with different scale: " & $a.scale & " vs " & $b.scale,
    )
  a.amount <= b.amount

proc hash*(v: PgMoney): Hash =
  var h: Hash = 0
  h = h !& hash(v.amount)
  h = h !& hash(int(v.scale))
  !$h

proc pow10u64(n: int): uint64 =
  result = 1'u64
  for _ in 0 ..< n:
    result *= 10'u64

proc `$`*(v: PgMoney): string =
  ## Format PgMoney as a plain decimal number with ``scale`` fractional
  ## digits. No currency symbol or thousand separator is emitted:
  ##   * ``PgMoney(amount: 123456, scale: 2)`` -> ``"1234.56"``
  ##   * ``PgMoney(amount: -1, scale: 2)`` -> ``"-0.01"``
  ##   * ``PgMoney(amount: 1234, scale: 0)`` -> ``"1234"``
  ##   * ``PgMoney(amount: 1234567, scale: 3)`` -> ``"1234.567"``
  ## Use ``formatPgMoney`` for currency symbols and thousand separators.
  let c = v.amount
  let scale = int(v.scale)
  let neg = c < 0
  # Avoid overflow on int64.low by working in uint64 for the magnitude.
  let mag =
    if neg:
      uint64(not c) + 1'u64
    else:
      uint64(c)
  result = newStringOfCap(24)
  if neg:
    result.add('-')
  if scale == 0:
    result.add($mag)
    return
  let divisor = pow10u64(scale)
  let whole = mag div divisor
  let frac = mag mod divisor
  result.add($whole)
  result.add('.')
  let fracStr = $frac
  for _ in 0 ..< (scale - fracStr.len):
    result.add('0')
  result.add(fracStr)

proc formatPgMoney*(
    v: PgMoney,
    symbol: string = "",
    decimalSep: char = '.',
    thousandsSep: char = '\0',
    symbolBefore: bool = true,
    accountingParens: bool = false,
): string =
  ## Locale-aware money formatter. ``thousandsSep`` of ``'\0'`` disables
  ## grouping. When ``accountingParens`` is true, negative values are wrapped
  ## in parentheses instead of being prefixed with ``-``. Examples:
  ##   ``formatPgMoney(initPgMoney(123456), symbol = "$")`` -> ``"$1234.56"``
  ##   ``formatPgMoney(initPgMoney(123456), symbol = "$", thousandsSep = ',')``
  ##     -> ``"$1,234.56"``
  ##   ``formatPgMoney(initPgMoney(123456), symbol = " €",
  ##                   decimalSep = ',', thousandsSep = '.',
  ##                   symbolBefore = false)`` -> ``"1.234,56 €"``
  ##   ``formatPgMoney(initPgMoney(-123456), symbol = "$",
  ##                   thousandsSep = ',', accountingParens = true)``
  ##     -> ``"($1,234.56)"``
  let c = v.amount
  let scale = int(v.scale)
  let neg = c < 0
  let mag =
    if neg:
      uint64(not c) + 1'u64
    else:
      uint64(c)
  let divisor = pow10u64(scale)
  let whole = mag div divisor
  let frac = mag mod divisor
  var wholeStr = $whole
  if thousandsSep != '\0' and wholeStr.len > 3:
    var grouped = newStringOfCap(wholeStr.len + wholeStr.len div 3)
    let firstLen = wholeStr.len mod 3
    var idx = 0
    if firstLen > 0:
      grouped.add(wholeStr[0 ..< firstLen])
      idx = firstLen
    while idx < wholeStr.len:
      if grouped.len > 0:
        grouped.add(thousandsSep)
      grouped.add(wholeStr[idx ..< idx + 3])
      idx += 3
    wholeStr = grouped
  result = newStringOfCap(symbol.len + wholeStr.len + scale + 4)
  if neg and accountingParens:
    result.add('(')
  elif neg:
    result.add('-')
  if symbolBefore:
    result.add(symbol)
  result.add(wholeStr)
  if scale > 0:
    result.add(decimalSep)
    let fracStr = $frac
    for _ in 0 ..< (scale - fracStr.len):
      result.add('0')
    result.add(fracStr)
  if not symbolBefore:
    result.add(symbol)
  if neg and accountingParens:
    result.add(')')

proc parsePgMoney*(s: string, scale: int = 2): PgMoney =
  ## Parse a money string with tolerant locale handling. Accepts:
  ##   * Optional currency symbol anywhere (``$``, ``€``, ``¥``, ``£`` etc. —
  ##     any non-digit, non-sign, non-separator characters are ignored)
  ##   * ``.`` or ``,`` as decimal separator; the other is treated as thousand
  ##     separator. For ``scale > 0`` the last ``.``/``,`` is the decimal
  ##     point and must be followed by exactly ``scale`` digits.
  ##   * ``-``/``+`` sign either before or after the currency symbol
  ##     (``-$1.00`` and ``$-1.00`` both work)
  ##   * Accounting-style parenthesized negatives, e.g. ``($1.00)``
  ## Raises ``PgTypeError`` on malformed input. The server's actual
  ## ``frac_digits`` cannot be inferred from the text — pass ``scale``
  ## explicitly when the server uses a non-default ``lc_monetary``.
  if scale < 0 or scale > 18:
    raise newException(PgTypeError, "PgMoney scale out of range: " & $scale)
  var trimmed = s.strip()
  if trimmed.len == 0:
    raise newException(PgTypeError, "Empty money string")
  var neg = false
  # Accounting-style parentheses: treat (...) as negative.
  if trimmed.len >= 2 and trimmed[0] == '(' and trimmed[^1] == ')':
    neg = true
    trimmed = trimmed[1 ..^ 2].strip()
    if trimmed.len == 0:
      raise newException(PgTypeError, "Invalid money format: " & s)
  # Extract a single leading sign, skipping whitespace and currency prefix
  # characters (anything that is not a digit, separator, or sign). This
  # accepts both ``-$1.00`` and ``$-1.00`` forms. Scanning stops at the
  # first digit/separator. The sign character itself is left in ``trimmed``
  # since the cleaning pass below filters non-digit/separator bytes.
  for ch in trimmed:
    if (ch >= '0' and ch <= '9') or ch == '.' or ch == ',':
      break
    if ch == '-':
      if neg:
        raise newException(PgTypeError, "Invalid money format: " & s)
      neg = true
      break
    if ch == '+':
      break
  # Keep only digits and separators (.,). Everything else (currency symbols,
  # whitespace, letters, UTF-8 bytes, sign characters) is discarded.
  var cleaned = newStringOfCap(trimmed.len)
  for ch in trimmed:
    if (ch >= '0' and ch <= '9') or ch == '.' or ch == ',':
      cleaned.add(ch)
  if cleaned.len == 0:
    raise newException(PgTypeError, "Invalid money format: " & s)
  # Separate integer and fractional parts based on expected scale.
  var wholePart: string
  var fracPart: string
  if scale == 0:
    fracPart = ""
    wholePart = newStringOfCap(cleaned.len)
    for ch in cleaned:
      if ch == '.' or ch == ',':
        continue
      wholePart.add(ch)
  else:
    var decIdx = -1
    for i in countdown(cleaned.len - 1, 0):
      if cleaned[i] == '.' or cleaned[i] == ',':
        decIdx = i
        break
    if decIdx == -1:
      raise newException(PgTypeError, "Invalid money format: " & s)
    fracPart = cleaned[decIdx + 1 ..^ 1]
    if fracPart.len != scale:
      raise newException(PgTypeError, "Invalid money format: " & s)
    for ch in fracPart:
      if ch < '0' or ch > '9':
        raise newException(PgTypeError, "Invalid money format: " & s)
    wholePart = newStringOfCap(decIdx)
    for i in 0 ..< decIdx:
      let ch = cleaned[i]
      if ch == '.' or ch == ',':
        continue
      wholePart.add(ch)
  if wholePart.len == 0:
    raise newException(PgTypeError, "Invalid money format: " & s)
  for ch in wholePart:
    if ch < '0' or ch > '9':
      raise newException(PgTypeError, "Invalid money format: " & s)
  # Accumulate magnitude in uint64 with overflow detection. abs(int64.low)
  # equals uint64(int64.high) + 1, so the max allowed magnitude is magMax+1
  # only when negative.
  const magMax = uint64(high(int64))
  var mag: uint64 = 0
  for ch in wholePart:
    let d = uint64(ord(ch) - ord('0'))
    if mag > (high(uint64) - d) div 10'u64:
      raise newException(PgTypeError, "Money value out of range: " & s)
    mag = mag * 10 + d
  for ch in fracPart:
    let d = uint64(ord(ch) - ord('0'))
    if mag > (high(uint64) - d) div 10'u64:
      raise newException(PgTypeError, "Money value out of range: " & s)
    mag = mag * 10 + d
  if neg:
    if mag == magMax + 1'u64:
      return PgMoney(amount: low(int64), scale: int8(scale))
    if mag > magMax:
      raise newException(PgTypeError, "Money value out of range: " & s)
    return PgMoney(amount: -int64(mag), scale: int8(scale))
  else:
    if mag > magMax:
      raise newException(PgTypeError, "Money value out of range: " & s)
    return PgMoney(amount: int64(mag), scale: int8(scale))

proc `$`*(v: PgMacAddr): string {.borrow.}
proc `==`*(a, b: PgMacAddr): bool {.borrow.}

proc `$`*(v: PgMacAddr8): string {.borrow.}
proc `==`*(a, b: PgMacAddr8): bool {.borrow.}

proc `$`*(v: PgTsVector): string {.borrow.}
proc `==`*(a, b: PgTsVector): bool {.borrow.}

proc `$`*(v: PgTsQuery): string {.borrow.}
proc `==`*(a, b: PgTsQuery): bool {.borrow.}

proc `$`*(v: PgXml): string {.borrow.}
proc `==`*(a, b: PgXml): bool {.borrow.}

proc `$`*(v: PgBit): string =
  ## Convert PgBit to a bit string like "10110011".
  result = newStringOfCap(v.nbits)
  for i in 0 ..< v.nbits:
    let byteIdx = i div 8
    let bitIdx = 7 - (i mod 8)
    if (v.data[byteIdx].int shr bitIdx and 1) == 1:
      result.add('1')
    else:
      result.add('0')

proc `==`*(a, b: PgBit): bool =
  a.nbits == b.nbits and a.data == b.data

proc parseBitString*(s: string): PgBit =
  ## Parse a bit string like "10110011" into PgBit.
  let nbits = int32(s.len)
  let nBytes = (nbits + 7) div 8
  var data = newSeq[byte](nBytes)
  for i in 0 ..< nbits:
    if s[i] == '1':
      let byteIdx = i div 8
      let bitIdx = 7 - (i mod 8)
      data[byteIdx] = data[byteIdx] or byte(1 shl bitIdx)
    elif s[i] != '0':
      raise newException(PgTypeError, "Invalid bit character: " & $s[i])
  PgBit(nbits: nbits, data: data)

proc parsePgNumeric*(s: string): PgNumeric =
  ## Parse a decimal string (e.g. "123.45", "-0.001", "NaN") into PgNumeric.
  if s.len == 0:
    raise newException(PgTypeError, "Invalid numeric: empty string")
  if s == "NaN":
    return PgNumeric(sign: pgNaN)
  var src = s
  var sign = pgPositive
  if src[0] == '-':
    sign = pgNegative
    src = src[1 .. ^1]
  if src.len == 0:
    raise newException(PgTypeError, "Invalid numeric: " & s)
  for c in src:
    if c notin {'0' .. '9', '.'}:
      raise newException(PgTypeError, "Invalid numeric: " & s)
  if src.count('.') > 1:
    raise newException(PgTypeError, "Invalid numeric: " & s)
  if src == ".":
    raise newException(PgTypeError, "Invalid numeric: " & s)
  # Split integer and fractional parts
  let dotPos = src.find('.')
  var intPart, fracPart: string
  if dotPos >= 0:
    intPart = src[0 ..< dotPos]
    fracPart = src[dotPos + 1 .. ^1]
  else:
    intPart = src
    fracPart = ""
  let dscale = int16(fracPart.len)
  # Strip leading zeros from integer part (keep at least "")
  var intStripped = intPart.strip(leading = true, trailing = false, chars = {'0'})
  # Pad to multiples of 4 for base-10000 grouping
  var fracPadded = fracPart
  if fracPadded.len mod 4 != 0:
    fracPadded.add(repeat('0', 4 - fracPadded.len mod 4))
  var intPadded = intStripped
  if intPadded.len > 0 and intPadded.len mod 4 != 0:
    intPadded = repeat('0', 4 - intPadded.len mod 4) & intPadded
  # Parse base-10000 digit groups: integer part then fractional part
  var digits: seq[int16]
  for i in countup(0, intPadded.len - 1, 4):
    digits.add(int16(parseInt(intPadded[i ..< i + 4])))
  for i in countup(0, fracPadded.len - 1, 4):
    digits.add(int16(parseInt(fracPadded[i ..< i + 4])))
  let intGroups = intPadded.len div 4
  # Strip trailing zero groups, keeping enough for dscale
  let minDigits = intGroups + (if dscale > 0: (dscale.int + 3) div 4 else: 0)
  while digits.len > minDigits and digits.len > 0 and digits[^1] == 0:
    digits.setLen(digits.len - 1)
  # Strip leading zero groups from fractional part (pure fractions like 0.001)
  var leadingZeroGroups = 0
  if intGroups == 0:
    while leadingZeroGroups < digits.len and digits[leadingZeroGroups] == 0:
      inc leadingZeroGroups
    if leadingZeroGroups > 0:
      digits = digits[leadingZeroGroups .. ^1]
  # Compute weight (exponent of first digit group)
  let weight =
    if intGroups > 0:
      int16(intGroups - 1)
    elif digits.len > 0:
      int16(-leadingZeroGroups - 1)
    else:
      0'i16
  if digits.len == 0:
    return PgNumeric(weight: 0, sign: pgPositive, dscale: dscale, digits: @[])
  PgNumeric(weight: weight, sign: sign, dscale: dscale, digits: digits)

proc `$`*(v: PgNumeric): string =
  ## Convert PgNumeric to its decimal string representation.
  if v.sign == pgNaN:
    return "NaN"
  if v.digits.len == 0:
    if v.dscale > 0:
      result = "0."
      for _ in 0 ..< v.dscale.int:
        result.add('0')
      return
    return "0"
  result = ""
  if v.sign == pgNegative:
    result.add('-')
  let intGroups = v.weight + 1
  # Integer part
  var wroteInt = false
  for i in 0 ..< min(v.digits.len, intGroups.int):
    let d = int(v.digits[i])
    if not wroteInt:
      result.add($d)
      wroteInt = true
    else:
      let s = $d
      for _ in 0 ..< 4 - s.len:
        result.add('0')
      result.add(s)
  if intGroups > v.digits.len:
    for _ in 0 ..< (intGroups.int - v.digits.len) * 4:
      result.add('0')
    wroteInt = true
  if not wroteInt:
    result.add('0')
  if v.dscale > 0:
    result.add('.')
    let fracStart = result.len
    # Leading zero groups for pure fractions (intGroups < 0)
    if intGroups < 0:
      for _ in 0 ..< -intGroups.int * 4:
        result.add('0')
    # Fractional digit groups
    for i in max(intGroups.int, 0) ..< v.digits.len:
      let s = $int(v.digits[i])
      for _ in 0 ..< 4 - s.len:
        result.add('0')
      result.add(s)
    # Trim or pad to dscale
    let fracLen = result.len - fracStart
    if fracLen > v.dscale.int:
      result.setLen(fracStart + v.dscale.int)
    elif fracLen < v.dscale.int:
      for _ in 0 ..< v.dscale.int - fracLen:
        result.add('0')

proc cmpMagnitude(a, b: PgNumeric): int =
  ## Compare absolute values. Returns -1, 0, or 1.
  # Compare weights first
  let aWeight =
    if a.digits.len > 0:
      a.weight.int
    else:
      -int.high
  let bWeight =
    if b.digits.len > 0:
      b.weight.int
    else:
      -int.high
  if aWeight != bWeight:
    return if aWeight < bWeight: -1 else: 1
  # Same weight: compare digit-by-digit
  let maxLen = max(a.digits.len, b.digits.len)
  for i in 0 ..< maxLen:
    let ad =
      if i < a.digits.len:
        a.digits[i].int
      else:
        0
    let bd =
      if i < b.digits.len:
        b.digits[i].int
      else:
        0
    if ad != bd:
      return if ad < bd: -1 else: 1
  return 0

proc isZero*(v: PgNumeric): bool =
  ## Check if the numeric value is zero.
  v.sign != pgNaN and v.digits.len == 0

proc cmp*(a, b: PgNumeric): int =
  ## Compare two PgNumeric values. NaN sorts highest (PostgreSQL convention).
  # NaN handling
  if a.sign == pgNaN and b.sign == pgNaN:
    return 0
  if a.sign == pgNaN:
    return 1
  if b.sign == pgNaN:
    return -1
  # Zero handling
  let aZero = a.isZero
  let bZero = b.isZero
  if aZero and bZero:
    return 0
  if aZero:
    return (if b.sign == pgNegative: 1 else: -1)
  if bZero:
    return (if a.sign == pgNegative: -1 else: 1)
  # Sign comparison
  if a.sign != b.sign:
    return if a.sign == pgNegative: -1 else: 1
  # Same sign: compare magnitude
  let mc = cmpMagnitude(a, b)
  if a.sign == pgNegative:
    -mc
  else:
    mc

proc `==`*(a, b: PgNumeric): bool =
  ## Value-based equality. 1.0 == 1.00 is true.
  cmp(a, b) == 0

proc `<`*(a, b: PgNumeric): bool =
  cmp(a, b) < 0

proc `<=`*(a, b: PgNumeric): bool =
  cmp(a, b) <= 0

proc `>`*(a, b: PgNumeric): bool =
  cmp(a, b) > 0

proc `>=`*(a, b: PgNumeric): bool =
  cmp(a, b) >= 0

proc hash*(v: PgNumeric): Hash =
  ## Hash consistent with value-based ==.
  if v.sign == pgNaN:
    return !$(0 !& hash(pgNaN.ord))
  var lastNonZero = v.digits.len - 1
  while lastNonZero >= 0 and v.digits[lastNonZero] == 0:
    dec lastNonZero
  if lastNonZero < 0:
    return !$(0 !& hash(0) !& hash(0))
  var h: Hash = 0
  h = h !& hash(v.sign.ord)
  h = h !& hash(v.weight)
  for i in 0 .. lastNonZero:
    h = h !& hash(v.digits[i])
  !$h

proc `$`*(v: PgPoint): string =
  "(" & $v.x & "," & $v.y & ")"

proc `==`*(a, b: PgPoint): bool =
  a.x == b.x and a.y == b.y

proc `$`*(v: PgLine): string =
  "{" & $v.a & "," & $v.b & "," & $v.c & "}"

proc `==`*(a, b: PgLine): bool =
  a.a == b.a and a.b == b.b and a.c == b.c

proc `$`*(v: PgLseg): string =
  "[" & $v.p1 & "," & $v.p2 & "]"

proc `==`*(a, b: PgLseg): bool =
  a.p1 == b.p1 and a.p2 == b.p2

proc `$`*(v: PgBox): string =
  $v.high & "," & $v.low

proc `==`*(a, b: PgBox): bool =
  a.high == b.high and a.low == b.low

proc `$`*(v: PgPath): string =
  let inner = v.points
    .map(
      proc(p: PgPoint): string =
        $p
    )
    .join(",")
  if v.closed:
    "(" & inner & ")"
  else:
    "[" & inner & "]"

proc `==`*(a, b: PgPath): bool =
  a.closed == b.closed and a.points == b.points

proc `$`*(v: PgPolygon): string =
  "(" &
    v.points
    .map(
      proc(p: PgPoint): string =
        $p
    )
    .join(",") & ")"

proc `==`*(a, b: PgPolygon): bool =
  a.points == b.points

proc `$`*(v: PgCircle): string =
  "<" & $v.center & "," & $v.radius & ">"

proc `==`*(a, b: PgCircle): bool =
  a.center == b.center and a.radius == b.radius

proc `$`*(v: PgInet): string =
  $v.address & "/" & $v.mask

proc `==`*(a, b: PgInet): bool =
  a.address == b.address and a.mask == b.mask

proc `$`*(v: PgCidr): string =
  $v.address & "/" & $v.mask

proc `==`*(a, b: PgCidr): bool =
  a.address == b.address and a.mask == b.mask

proc `$`*(v: PgInterval): string =
  var parts: seq[string]
  if v.months != 0:
    let years = v.months div 12
    let mons = v.months mod 12
    if years != 0:
      parts.add($years & " year" & (if years != 1 and years != -1: "s" else: ""))
    if mons != 0:
      parts.add($mons & " mon" & (if mons != 1 and mons != -1: "s" else: ""))
  if v.days != 0:
    parts.add($v.days & " day" & (if v.days != 1 and v.days != -1: "s" else: ""))
  var us = v.microseconds
  let neg = us < 0
  if neg:
    if us == int64.low:
      us = int64.high # -int64.min overflows; clamp to int64.max
    else:
      us = -us
  let hours = us div 3_600_000_000
  us = us mod 3_600_000_000
  let mins = us div 60_000_000
  us = us mod 60_000_000
  let secs = us div 1_000_000
  let frac = us mod 1_000_000
  var timePart =
    (if neg: "-" else: "") & align($hours, 2, '0') & ":" & align($mins, 2, '0') & ":" &
    align($secs, 2, '0')
  if frac != 0:
    timePart.add("." & align($frac, 6, '0'))
  if parts.len == 0 and v.microseconds == 0:
    return "00:00:00"
  if v.microseconds != 0:
    parts.add(timePart)
  result = parts.join(" ")

proc `==`*(a, b: PgInterval): bool =
  a.months == b.months and a.days == b.days and a.microseconds == b.microseconds

proc `$`*(v: PgTime): string =
  result =
    align($v.hour, 2, '0') & ":" & align($v.minute, 2, '0') & ":" &
    align($v.second, 2, '0')
  if v.microsecond != 0:
    result.add("." & align($v.microsecond, 6, '0'))

proc `$`*(v: PgTimeTz): string =
  result =
    align($v.hour, 2, '0') & ":" & align($v.minute, 2, '0') & ":" &
    align($v.second, 2, '0')
  if v.microsecond != 0:
    result.add("." & align($v.microsecond, 6, '0'))
  let off = v.utcOffset
  if off >= 0:
    result.add("+")
  else:
    result.add("-")
  let absOff = abs(off)
  let offH = absOff div 3600
  let offM = (absOff mod 3600) div 60
  let offS = absOff mod 60
  result.add(align($offH, 2, '0') & ":" & align($offM, 2, '0'))
  if offS != 0:
    result.add(":" & align($offS, 2, '0'))

proc toBytes*(s: string): seq[byte] =
  ## Converts a string to a sequence of bytes.
  result = newSeq[byte](s.len)
  if s.len > 0:
    copyMem(addr result[0], addr s[0], s.len)

proc toString*(s: seq[byte]): string =
  ## Converts a sequence of bytes to a string.
  result = newString(s.len)
  if s.len > 0:
    copyMem(addr result[0], addr s[0], s.len)

proc toBE16*(v: int16): array[2, byte] =
  [byte((v shr 8) and 0xFF), byte(v and 0xFF)]

proc toBE32*(v: int32): array[4, byte] =
  [
    byte((v shr 24) and 0xFF),
    byte((v shr 16) and 0xFF),
    byte((v shr 8) and 0xFF),
    byte(v and 0xFF),
  ]

proc toBE64*(v: int64): array[8, byte] =
  [
    byte((v shr 56) and 0xFF),
    byte((v shr 48) and 0xFF),
    byte((v shr 40) and 0xFF),
    byte((v shr 32) and 0xFF),
    byte((v shr 24) and 0xFF),
    byte((v shr 16) and 0xFF),
    byte((v shr 8) and 0xFF),
    byte(v and 0xFF),
  ]

proc fromBE16*(data: openArray[byte]): int16 =
  ## Decode a 16-bit integer from big-endian bytes.
  int16(data[0]) shl 8 or int16(data[1])

proc fromBE32*(data: openArray[byte]): int32 =
  ## Decode a 32-bit integer from big-endian bytes.
  int32(data[0]) shl 24 or int32(data[1]) shl 16 or int32(data[2]) shl 8 or
    int32(data[3])

proc fromBE64*(data: openArray[byte]): int64 =
  ## Decode a 64-bit integer from big-endian bytes.
  int64(data[0]) shl 56 or int64(data[1]) shl 48 or int64(data[2]) shl 40 or
    int64(data[3]) shl 32 or int64(data[4]) shl 24 or int64(data[5]) shl 16 or
    int64(data[6]) shl 8 or int64(data[7])

proc decodeFloat64BE*(data: openArray[byte], offset: int = 0): float64 =
  ## Decode a big-endian 64-bit float from bytes at the given offset.
  let bits =
    (uint64(data[offset]) shl 56) or (uint64(data[offset + 1]) shl 48) or
    (uint64(data[offset + 2]) shl 40) or (uint64(data[offset + 3]) shl 32) or
    (uint64(data[offset + 4]) shl 24) or (uint64(data[offset + 5]) shl 16) or
    (uint64(data[offset + 6]) shl 8) or uint64(data[offset + 7])
  cast[float64](bits)
