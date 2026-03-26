import
  std/[
    hashes, json, macros, math, options, parseutils, sequtils, strutils, tables, times,
    net,
  ]

import pg_protocol

type
  PgError* = object of CatchableError
    ## General PostgreSQL error. Base type for all pg-specific errors.

  PgTypeError* = object of PgError
    ## Raised when a PostgreSQL value cannot be converted to the requested Nim type.

  PgUuid* = distinct string
    ## UUID value stored as its string representation (e.g. "550e8400-e29b-41d4-a716-446655440000").

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

  PgInet* = object ## PostgreSQL inet type: an IP address with a subnet mask.
    address*: IpAddress
    mask*: uint8

  PgCidr* = object ## PostgreSQL cidr type: a network address with a subnet mask.
    address*: IpAddress
    mask*: uint8

  PgMacAddr* = distinct string ## MAC address as "08:00:2b:01:02:03"

  PgMacAddr8* = distinct string ## EUI-64 MAC address as "08:00:2b:01:02:03:04:05"

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

  PgParam* = object
    ## A single query parameter in binary wire format, ready to send to PostgreSQL.
    oid*: int32
    format*: int16 # 0=text, 1=binary
    value*: Option[seq[byte]]

  ResultFormat* = enum
    ## How result columns should be encoded by the server.
    rfAuto ## Per-column binary-safe detection via statement cache (default).
    rfText ## All columns in text format.
    rfBinary ## All columns in binary format.

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
  OidNumeric* = 1700'i32
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

  # Multirange types (PostgreSQL 14+)
  OidInt4Multirange* = 4451'i32
  OidNumMultirange* = 4532'i32
  OidTsMultirange* = 4533'i32
  OidTsTzMultirange* = 4534'i32
  OidDateMultirange* = 4535'i32
  OidInt8Multirange* = 4536'i32

  rangeEmpty* = 0x01'u8 ## Range flag: range is empty.
  rangeHasLower* = 0x02'u8 ## Range flag: lower bound present.
  rangeHasUpper* = 0x04'u8 ## Range flag: upper bound present.
  rangeLowerInc* = 0x08'u8 ## Range flag: lower bound is inclusive.
  rangeUpperInc* = 0x10'u8 ## Range flag: upper bound is inclusive.

proc `$`*(v: PgUuid): string {.borrow.}
proc `==`*(a, b: PgUuid): bool {.borrow.}
proc hash*(v: PgUuid): Hash {.borrow.}

proc `$`*(v: PgMacAddr): string {.borrow.}
proc `==`*(a, b: PgMacAddr): bool {.borrow.}

proc `$`*(v: PgMacAddr8): string {.borrow.}
proc `==`*(a, b: PgMacAddr8): bool {.borrow.}

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

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  if s.len > 0:
    copyMem(addr result[0], unsafeAddr s[0], s.len)

proc toBE16(v: int16): array[2, byte] =
  [byte((v shr 8) and 0xFF), byte(v and 0xFF)]

proc toBE32(v: int32): array[4, byte] =
  [
    byte((v shr 24) and 0xFF),
    byte((v shr 16) and 0xFF),
    byte((v shr 8) and 0xFF),
    byte(v and 0xFF),
  ]

proc toBE64(v: int64): array[8, byte] =
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

proc toPgParam*(v: string): PgParam =
  ## Convert a Nim value to a PgParam for use as a query parameter.
  ## Uses text format for strings, binary for numeric types.
  PgParam(oid: OidText, format: 0, value: some(toBytes(v)))

proc toPgParam*(v: int16): PgParam =
  PgParam(oid: OidInt2, format: 1, value: some(@(toBE16(v))))

proc toPgParam*(v: int32): PgParam =
  PgParam(oid: OidInt4, format: 1, value: some(@(toBE32(v))))

proc toPgParam*(v: int64): PgParam =
  PgParam(oid: OidInt8, format: 1, value: some(@(toBE64(v))))

proc toPgParam*(v: int): PgParam =
  PgParam(oid: OidInt8, format: 1, value: some(@(toBE64(int64(v)))))

proc toPgParam*(v: float32): PgParam =
  PgParam(oid: OidFloat4, format: 1, value: some(@(toBE32(cast[int32](v)))))

proc toPgParam*(v: float64): PgParam =
  PgParam(oid: OidFloat8, format: 1, value: some(@(toBE64(cast[int64](v)))))

proc toPgParam*(v: bool): PgParam =
  PgParam(oid: OidBool, format: 1, value: some(@[if v: 1'u8 else: 0'u8]))

proc toPgParam*(v: seq[byte]): PgParam =
  PgParam(oid: OidBytea, format: 0, value: some(v))

proc toPgParam*(v: DateTime): PgParam =
  let s = v.format("yyyy-MM-dd HH:mm:ss'.'ffffff")
  PgParam(oid: OidTimestamp, format: 0, value: some(toBytes(s)))

proc toPgParam*(v: PgUuid): PgParam =
  PgParam(oid: OidUuid, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: PgNumeric): PgParam =
  PgParam(oid: OidNumeric, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgInterval): PgParam =
  PgParam(oid: OidInterval, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgInet): PgParam =
  PgParam(oid: OidInet, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgCidr): PgParam =
  PgParam(oid: OidCidr, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgMacAddr): PgParam =
  PgParam(oid: OidMacAddr, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: PgMacAddr8): PgParam =
  PgParam(oid: OidMacAddr8, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: PgPoint): PgParam =
  PgParam(oid: OidPoint, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgLine): PgParam =
  PgParam(oid: OidLine, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgLseg): PgParam =
  PgParam(oid: OidLseg, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgBox): PgParam =
  PgParam(oid: OidBox, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgPath): PgParam =
  PgParam(oid: OidPath, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgPolygon): PgParam =
  PgParam(oid: OidPolygon, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgCircle): PgParam =
  PgParam(oid: OidCircle, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: JsonNode): PgParam =
  PgParam(oid: OidJsonb, format: 0, value: some(toBytes($v)))

proc encodeBinaryArray*(elemOid: int32, elements: seq[seq[byte]]): seq[byte] =
  ## Encode a 1-dimensional PostgreSQL binary array.
  ## Header: ndim(4) + has_null(4) + elem_oid(4) + dim_len(4) + lower_bound(4) = 20 bytes
  ## Each element: len(4) + data
  if elements.len > int32.high.int:
    raise
      newException(PgError, "Array has too many elements for PostgreSQL binary format")
  let headerSize = 20
  var dataSize = 0
  for e in elements:
    if e.len > int32.high.int:
      raise
        newException(PgError, "Array element too large for PostgreSQL binary format")
    dataSize += 4 + e.len
  result = newSeq[byte](headerSize + dataSize)
  # ndim = 1
  let ndim = toBE32(1'i32)
  copyMem(addr result[0], unsafeAddr ndim[0], 4)
  # has_null = 0
  let hasNull = toBE32(0'i32)
  copyMem(addr result[4], unsafeAddr hasNull[0], 4)
  # elem_oid
  let oid = toBE32(elemOid)
  copyMem(addr result[8], unsafeAddr oid[0], 4)
  # dim_len
  let dimLen = toBE32(int32(elements.len))
  copyMem(addr result[12], unsafeAddr dimLen[0], 4)
  # lower_bound = 1
  let lb = toBE32(1'i32)
  copyMem(addr result[16], unsafeAddr lb[0], 4)
  var pos = headerSize
  for e in elements:
    let eLen = toBE32(int32(e.len))
    copyMem(addr result[pos], unsafeAddr eLen[0], 4)
    pos += 4
    if e.len > 0:
      copyMem(addr result[pos], unsafeAddr e[0], e.len)
      pos += e.len

proc encodeBinaryArrayEmpty*(elemOid: int32): seq[byte] =
  ## Encode an empty 1-dimensional PostgreSQL binary array.
  ## ndim=0, has_null=0, elem_oid
  result = newSeq[byte](12)
  # ndim = 0
  let ndim = toBE32(0'i32)
  copyMem(addr result[0], unsafeAddr ndim[0], 4)
  # has_null = 0
  let hasNull = toBE32(0'i32)
  copyMem(addr result[4], unsafeAddr hasNull[0], 4)
  # elem_oid
  let oid = toBE32(elemOid)
  copyMem(addr result[8], unsafeAddr oid[0], 4)

proc toPgParam*(v: seq[int16]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt2Array, format: 1, value: some(encodeBinaryArrayEmpty(OidInt2))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = @(toBE16(x))
  PgParam(
    oid: OidInt2Array, format: 1, value: some(encodeBinaryArray(OidInt2, elements))
  )

proc toPgParam*(v: seq[int32]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt4Array, format: 1, value: some(encodeBinaryArrayEmpty(OidInt4))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = @(toBE32(x))
  PgParam(
    oid: OidInt4Array, format: 1, value: some(encodeBinaryArray(OidInt4, elements))
  )

proc toPgParam*(v: seq[int64]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt8Array, format: 1, value: some(encodeBinaryArrayEmpty(OidInt8))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = @(toBE64(x))
  PgParam(
    oid: OidInt8Array, format: 1, value: some(encodeBinaryArray(OidInt8, elements))
  )

proc toPgParam*(v: seq[float32]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidFloat4Array, format: 1, value: some(encodeBinaryArrayEmpty(OidFloat4))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = @(toBE32(cast[int32](x)))
  PgParam(
    oid: OidFloat4Array, format: 1, value: some(encodeBinaryArray(OidFloat4, elements))
  )

proc toPgParam*(v: seq[float64]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidFloat8Array, format: 1, value: some(encodeBinaryArrayEmpty(OidFloat8))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = @(toBE64(cast[int64](x)))
  PgParam(
    oid: OidFloat8Array, format: 1, value: some(encodeBinaryArray(OidFloat8, elements))
  )

proc toPgParam*(v: seq[bool]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidBoolArray, format: 1, value: some(encodeBinaryArrayEmpty(OidBool))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = @[if x: 1'u8 else: 0'u8]
  PgParam(
    oid: OidBoolArray, format: 1, value: some(encodeBinaryArray(OidBool, elements))
  )

proc toPgParam*(v: seq[string]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidTextArray, format: 1, value: some(encodeBinaryArrayEmpty(OidText))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = toBytes(x)
  PgParam(
    oid: OidTextArray, format: 1, value: some(encodeBinaryArray(OidText, elements))
  )

proc toPgParam*(v: Option[JsonNode]): PgParam =
  if v.isSome:
    toPgParam(v.get)
  else:
    PgParam(oid: OidJsonb, format: 0, value: none(seq[byte]))

proc toPgParam*[T](v: Option[T]): PgParam =
  if v.isSome:
    result = toPgParam(v.get)
  else:
    let proto = toPgParam(default(T))
    result = PgParam(oid: proto.oid, format: proto.format, value: none(seq[byte]))

macro pgParams*(args: varargs[typed]): untyped =
  ## Convert multiple values to a ``seq[PgParam]`` in one call.
  ## At least one argument is required; for parameterless queries omit the
  ## parameter argument entirely instead of calling ``pgParams()``.
  ##
  ## .. code-block:: nim
  ##   await conn.query("SELECT * FROM users WHERE age > $1 AND name = $2",
  ##     pgParams(25'i32, "Alice"))
  result = newNimNode(nnkPrefix)
  result.add(ident"@")
  let bracket = newNimNode(nnkBracket)
  for arg in args:
    bracket.add(newCall(bindSym"toPgParam", arg))
  result.add(bracket)

proc toPgBinaryParam*(v: string): PgParam =
  ## Convert a Nim value to a PgParam using binary format.
  ## Prefer this over `toPgParam` when binary format is needed for all types.
  PgParam(oid: OidText, format: 1, value: some(toBytes(v)))

proc toPgBinaryParam*(v: int16): PgParam =
  PgParam(oid: OidInt2, format: 1, value: some(@(toBE16(v))))

proc toPgBinaryParam*(v: int32): PgParam =
  PgParam(oid: OidInt4, format: 1, value: some(@(toBE32(v))))

proc toPgBinaryParam*(v: int64): PgParam =
  PgParam(oid: OidInt8, format: 1, value: some(@(toBE64(v))))

proc toPgBinaryParam*(v: int): PgParam =
  PgParam(oid: OidInt8, format: 1, value: some(@(toBE64(int64(v)))))

proc toPgBinaryParam*(v: float32): PgParam =
  PgParam(oid: OidFloat4, format: 1, value: some(@(toBE32(cast[int32](v)))))

proc toPgBinaryParam*(v: float64): PgParam =
  PgParam(oid: OidFloat8, format: 1, value: some(@(toBE64(cast[int64](v)))))

proc toPgBinaryParam*(v: bool): PgParam =
  PgParam(oid: OidBool, format: 1, value: some(@[if v: 1'u8 else: 0'u8]))

proc toPgBinaryParam*(v: seq[byte]): PgParam =
  PgParam(oid: OidBytea, format: 1, value: some(v))

proc toPgBinaryParam*(v: DateTime): PgParam =
  let t = v.toTime()
  let unixUs = t.toUnix() * 1_000_000 + int64(t.nanosecond div 1000)
  let pgUs = unixUs - pgEpochUnix * 1_000_000
  PgParam(oid: OidTimestamp, format: 1, value: some(@(toBE64(pgUs))))

proc putBE16(buf: var seq[byte], off: int, v: int16) =
  let b = toBE16(v)
  buf[off] = b[0]
  buf[off + 1] = b[1]

proc encodeNumericBinary*(v: PgNumeric): seq[byte] =
  ## Encode PgNumeric as PostgreSQL binary numeric format.
  let ndigits = int16(v.digits.len)
  let signVal = cast[int16](v.sign.uint16)
  result = newSeq[byte](8 + ndigits.int * 2)
  result.putBE16(0, ndigits)
  result.putBE16(2, v.weight)
  result.putBE16(4, signVal)
  result.putBE16(6, v.dscale)
  for i in 0 ..< ndigits.int:
    result.putBE16(8 + i * 2, v.digits[i])

proc toPgBinaryParam*(v: PgNumeric): PgParam =
  PgParam(oid: OidNumeric, format: 1, value: some(encodeNumericBinary(v)))

proc toPgBinaryParam*(v: PgUuid): PgParam =
  let hex = string(v).replace("-", "")
  var bytes = newSeq[byte](16)
  for i in 0 ..< 16:
    bytes[i] = byte(parseHexInt(hex[i * 2 .. i * 2 + 1]))
  PgParam(oid: OidUuid, format: 1, value: some(bytes))

proc toPgBinaryParam*(v: PgInterval): PgParam =
  var data = newSeq[byte](16)
  let usBytes = toBE64(v.microseconds)
  copyMem(addr data[0], unsafeAddr usBytes[0], 8)
  let dayBytes = toBE32(v.days)
  copyMem(addr data[8], unsafeAddr dayBytes[0], 4)
  let monBytes = toBE32(v.months)
  copyMem(addr data[12], unsafeAddr monBytes[0], 4)
  PgParam(oid: OidInterval, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgInet): PgParam =
  ## Binary format: family(1) + bits(1) + is_cidr(1) + addrlen(1) + addr(4|16)
  if v.address.family == IpAddressFamily.IPv4:
    var data = newSeq[byte](8)
    data[0] = 2 # AF_INET
    data[1] = v.mask
    data[2] = 0 # is_cidr = false
    data[3] = 4 # addrlen
    for i in 0 ..< 4:
      data[4 + i] = v.address.address_v4[i]
    PgParam(oid: OidInet, format: 1, value: some(data))
  else:
    var data = newSeq[byte](20)
    data[0] = 3 # AF_INET6
    data[1] = v.mask
    data[2] = 0 # is_cidr = false
    data[3] = 16 # addrlen
    for i in 0 ..< 16:
      data[4 + i] = v.address.address_v6[i]
    PgParam(oid: OidInet, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgCidr): PgParam =
  ## Binary format: family(1) + bits(1) + is_cidr(1) + addrlen(1) + addr(4|16)
  if v.address.family == IpAddressFamily.IPv4:
    var data = newSeq[byte](8)
    data[0] = 2 # AF_INET
    data[1] = v.mask
    data[2] = 1 # is_cidr = true
    data[3] = 4 # addrlen
    for i in 0 ..< 4:
      data[4 + i] = v.address.address_v4[i]
    PgParam(oid: OidCidr, format: 1, value: some(data))
  else:
    var data = newSeq[byte](20)
    data[0] = 3 # AF_INET6
    data[1] = v.mask
    data[2] = 1 # is_cidr = true
    data[3] = 16 # addrlen
    for i in 0 ..< 16:
      data[4 + i] = v.address.address_v6[i]
    PgParam(oid: OidCidr, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgMacAddr): PgParam =
  ## Binary format: 6 raw bytes
  let s = string(v)
  let parts = s.split(':')
  var data = newSeq[byte](6)
  for i in 0 ..< 6:
    data[i] = byte(parseHexInt(parts[i]))
  PgParam(oid: OidMacAddr, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgMacAddr8): PgParam =
  ## Binary format: 8 raw bytes
  let s = string(v)
  let parts = s.split(':')
  var data = newSeq[byte](8)
  for i in 0 ..< 8:
    data[i] = byte(parseHexInt(parts[i]))
  PgParam(oid: OidMacAddr8, format: 1, value: some(data))

proc encodePointBinary(p: PgPoint): seq[byte] =
  ## Encode a point as 16 bytes (two float64 big-endian).
  result = newSeq[byte](16)
  let xBytes = toBE64(cast[int64](p.x))
  copyMem(addr result[0], unsafeAddr xBytes[0], 8)
  let yBytes = toBE64(cast[int64](p.y))
  copyMem(addr result[8], unsafeAddr yBytes[0], 8)

proc toPgBinaryParam*(v: PgPoint): PgParam =
  ## Binary format: 16 bytes (two float64 big-endian).
  PgParam(oid: OidPoint, format: 1, value: some(encodePointBinary(v)))

proc toPgBinaryParam*(v: PgLine): PgParam =
  ## Binary format: 24 bytes (three float64 big-endian: A, B, C).
  var data = newSeq[byte](24)
  let aBytes = toBE64(cast[int64](v.a))
  copyMem(addr data[0], unsafeAddr aBytes[0], 8)
  let bBytes = toBE64(cast[int64](v.b))
  copyMem(addr data[8], unsafeAddr bBytes[0], 8)
  let cBytes = toBE64(cast[int64](v.c))
  copyMem(addr data[16], unsafeAddr cBytes[0], 8)
  PgParam(oid: OidLine, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgLseg): PgParam =
  ## Binary format: 32 bytes (two points).
  var data = newSeq[byte](32)
  let p1 = encodePointBinary(v.p1)
  copyMem(addr data[0], unsafeAddr p1[0], 16)
  let p2 = encodePointBinary(v.p2)
  copyMem(addr data[16], unsafeAddr p2[0], 16)
  PgParam(oid: OidLseg, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgBox): PgParam =
  ## Binary format: 32 bytes (high point, low point).
  var data = newSeq[byte](32)
  let hi = encodePointBinary(v.high)
  copyMem(addr data[0], unsafeAddr hi[0], 16)
  let lo = encodePointBinary(v.low)
  copyMem(addr data[16], unsafeAddr lo[0], 16)
  PgParam(oid: OidBox, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgPath): PgParam =
  ## Binary format: closed(1) + npts(4) + points(npts \* 16).
  var data = newSeq[byte](1 + 4 + v.points.len * 16)
  data[0] = if v.closed: 1'u8 else: 0'u8
  let npts = toBE32(int32(v.points.len))
  copyMem(addr data[1], unsafeAddr npts[0], 4)
  for i, p in v.points:
    let pb = encodePointBinary(p)
    copyMem(addr data[5 + i * 16], unsafeAddr pb[0], 16)
  PgParam(oid: OidPath, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgPolygon): PgParam =
  ## Binary format: npts(4) + points(npts \* 16).
  var data = newSeq[byte](4 + v.points.len * 16)
  let npts = toBE32(int32(v.points.len))
  copyMem(addr data[0], unsafeAddr npts[0], 4)
  for i, p in v.points:
    let pb = encodePointBinary(p)
    copyMem(addr data[4 + i * 16], unsafeAddr pb[0], 16)
  PgParam(oid: OidPolygon, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgCircle): PgParam =
  ## Binary format: 24 bytes (center point + radius float64).
  var data = newSeq[byte](24)
  let cp = encodePointBinary(v.center)
  copyMem(addr data[0], unsafeAddr cp[0], 16)
  let rBytes = toBE64(cast[int64](v.radius))
  copyMem(addr data[16], unsafeAddr rBytes[0], 8)
  PgParam(oid: OidCircle, format: 1, value: some(data))

proc toPgBinaryParam*(v: JsonNode): PgParam =
  let jsonBytes = toBytes($v)
  var data = newSeq[byte](1 + jsonBytes.len)
  data[0] = 1 # jsonb version byte
  for i in 0 ..< jsonBytes.len:
    data[i + 1] = jsonBytes[i]
  PgParam(oid: OidJsonb, format: 1, value: some(data))

proc toPgBinaryParam*[T](v: seq[T]): PgParam =
  toPgParam(v)

proc toPgBinaryParam*(v: Option[JsonNode]): PgParam =
  if v.isSome:
    toPgBinaryParam(v.get)
  else:
    PgParam(oid: OidJsonb, format: 1, value: none(seq[byte]))

proc toPgBinaryParam*[T](v: Option[T]): PgParam =
  if v.isSome:
    result = toPgBinaryParam(v.get)
  else:
    let proto = toPgBinaryParam(default(T))
    result = PgParam(oid: proto.oid, format: proto.format, value: none(seq[byte]))

proc fromPgText*(data: seq[byte], oid: int32): string =
  ## Convert text-format bytes from PostgreSQL to a Nim string.
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

# Binary decoders needed by both basic and format-aware row accessors.
# Placed before the Row getters to avoid forward-declaration issues.

proc decodeNumericBinary(data: openArray[byte]): PgNumeric =
  ## Decode PostgreSQL binary numeric format into PgNumeric.
  if data.len < 8:
    raise newException(PgTypeError, "Numeric binary data too short: " & $data.len)
  let ndigits = int(fromBE16(data.toOpenArray(0, 1)))
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
  var digits = newSeq[int16](ndigits)
  for i in 0 ..< ndigits:
    digits[i] = fromBE16(data.toOpenArray(8 + i * 2, 9 + i * 2))
  PgNumeric(weight: weight, sign: sign, dscale: dscale, digits: digits)

proc decodeBinaryTimestamp(data: openArray[byte]): DateTime =
  let pgUs = fromBE64(data)
  let unixUs = pgUs + pgEpochUnix * 1_000_000
  var unixSec = unixUs div 1_000_000
  var fracUs = unixUs mod 1_000_000
  if fracUs < 0:
    unixSec -= 1
    fracUs += 1_000_000
  initTime(unixSec, int(fracUs * 1000)).utc()

proc decodeBinaryDate(data: openArray[byte]): DateTime =
  let pgDays = fromBE32(data)
  let unixSec = (int64(pgDays) + int64(pgEpochDaysOffset)) * 86400
  initTime(unixSec, 0).utc()

proc decodeInetBinary(data: openArray[byte]): tuple[address: IpAddress, mask: uint8] =
  ## Decode PostgreSQL binary inet/cidr format:
  ##   1 byte: family (2=IPv4, 3=IPv6)
  ##   1 byte: bits (netmask length)
  ##   1 byte: is_cidr (0 or 1)
  ##   1 byte: addrlen (4 or 16)
  ##   N bytes: address
  let family = data[0]
  let bits = data[1]
  # data[2] = is_cidr, ignored for decoding
  # data[3] = addrlen
  if family == 2:
    var ip = IpAddress(family: IpAddressFamily.IPv4)
    for i in 0 ..< 4:
      ip.address_v4[i] = data[4 + i]
    (ip, bits)
  else:
    var ip = IpAddress(family: IpAddressFamily.IPv6)
    for i in 0 ..< 16:
      ip.address_v6[i] = data[4 + i]
    (ip, bits)

proc decodePointBinary(data: openArray[byte], off: int): PgPoint =
  ## Decode a point from 16 bytes at offset.
  var xBits = uint64(
    (uint64(data[off]) shl 56) or (uint64(data[off + 1]) shl 48) or
      (uint64(data[off + 2]) shl 40) or (uint64(data[off + 3]) shl 32) or
      (uint64(data[off + 4]) shl 24) or (uint64(data[off + 5]) shl 16) or
      (uint64(data[off + 6]) shl 8) or uint64(data[off + 7])
  )
  var yBits = uint64(
    (uint64(data[off + 8]) shl 56) or (uint64(data[off + 9]) shl 48) or
      (uint64(data[off + 10]) shl 40) or (uint64(data[off + 11]) shl 32) or
      (uint64(data[off + 12]) shl 24) or (uint64(data[off + 13]) shl 16) or
      (uint64(data[off + 14]) shl 8) or uint64(data[off + 15])
  )
  copyMem(addr result.x, addr xBits, 8)
  copyMem(addr result.y, addr yBits, 8)

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
  # lower_bound at offset 16, ignored
  result.elements = newSeq[tuple[off: int, len: int]](dimLen)
  var pos = 20
  for i in 0 ..< dimLen:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary array truncated at element " & $i)
    let eLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if eLen == -1:
      result.elements[i] = (off: 0, len: -1)
    else:
      result.elements[i] = (off: pos, len: eLen)
      pos += eLen

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
    result.lowerOff = pos
    result.lowerLen = bLen
    pos += bLen
  if result.hasUpper:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary range truncated at upper bound length")
    let bLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    result.upperOff = pos
    result.upperLen = bLen

proc decodeInt4RangeBinary(data: openArray[byte]): PgRange[int32] =
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

proc decodeInt8RangeBinary(data: openArray[byte]): PgRange[int64] =
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

proc decodeDateRangeBinary(data: openArray[byte]): PgRange[DateTime] =
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

proc decodeMultirangeBinaryRaw(data: openArray[byte]): seq[tuple[off: int, len: int]] =
  if data.len < 4:
    raise newException(PgTypeError, "Binary multirange too short")
  let count = int(fromBE32(data.toOpenArray(0, 3)))
  result = newSeq[tuple[off: int, len: int]](count)
  var pos = 4
  for i in 0 ..< count:
    if pos + 4 > data.len:
      raise newException(PgTypeError, "Binary multirange truncated at range " & $i)
    let rLen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    result[i] = (off: pos, len: rLen)
    pos += rLen

proc decodeBinaryComposite*(
    data: openArray[byte]
): seq[tuple[oid: int32, off: int, len: int]] =
  ## Decode a PostgreSQL binary composite value.
  ## Returns (typeOid, offset, length) tuples. offset is relative to `data`.
  ## length of -1 indicates NULL.
  if data.len < 4:
    raise newException(PgTypeError, "Binary composite too short")
  let numFields = int(fromBE32(data.toOpenArray(0, 3)))
  result = newSeq[tuple[oid: int32, off: int, len: int]](numFields)
  var pos = 4
  for i in 0 ..< numFields:
    if pos + 8 > data.len:
      raise newException(PgTypeError, "Binary composite truncated at field " & $i)
    result[i].oid = fromBE32(data.toOpenArray(pos, pos + 3))
    pos += 4
    let flen = int(fromBE32(data.toOpenArray(pos, pos + 3)))
    pos += 4
    if flen == -1:
      result[i].off = 0
      result[i].len = -1
    else:
      result[i].off = pos
      result[i].len = flen
      pos += flen

# Row/RowData types are defined in pg_protocol and re-exported here.

proc cellInfo(row: Row, col: int): tuple[off: int, len: int] {.inline.} =
  let idx = (int(row.rowIdx) * int(row.data.numCols) + col) * 2
  result.off = int(row.data.cellIndex[idx])
  result.len = int(row.data.cellIndex[idx + 1])

template bufView(row: Row, off, clen: int): openArray[char] =
  ## Zero-copy char view into row.data.buf for parseutils.
  cast[ptr UncheckedArray[char]](unsafeAddr row.data.buf[off]).toOpenArray(0, clen - 1)

proc len*(row: Row): int {.inline.} =
  ## Return the number of columns in this row.
  int(row.data.numCols)

proc `[]`*(row: Row, col: int): Option[seq[byte]] =
  ## Backward-compatible cell access. Returns a copy of the cell data.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    none(seq[byte])
  elif clen == 0:
    some(newSeq[byte](0))
  else:
    some(@(row.data.buf.toOpenArray(off, off + clen - 1)))

converter toRow*(cells: seq[Option[seq[byte]]]): Row =
  ## Backward-compatible converter: build a Row from ``seq[Option[seq[byte]]]``.
  let rd = RowData(
    numCols: int16(cells.len), buf: @[], cellIndex: newSeq[int32](cells.len * 2)
  )
  for i, cell in cells:
    if cell.isNone:
      rd.cellIndex[i * 2] = 0'i32
      rd.cellIndex[i * 2 + 1] = -1'i32
    else:
      let data = cell.get
      rd.cellIndex[i * 2] = int32(rd.buf.len)
      rd.cellIndex[i * 2 + 1] = int32(data.len)
      rd.buf.add(data)
  Row(data: rd, rowIdx: 0)

proc parseAffectedRows*(tag: string): int64 =
  ## Extract row count from command tag (e.g. "UPDATE 3" -> 3, "INSERT 0 1" -> 1).
  let parts = tag.split(' ')
  if parts.len > 0:
    try:
      return parseBiggestInt(parts[^1])
    except ValueError:
      return 0
  return 0

proc initCommandResult*(tag: string): CommandResult {.inline.} =
  CommandResult(commandTag: tag)

proc affectedRows*(cr: CommandResult): int64 {.inline.} =
  ## Extract the number of affected rows from the command result.
  parseAffectedRows(cr.commandTag)

proc `$`*(cr: CommandResult): string {.inline.} =
  cr.commandTag

proc `==`*(cr: CommandResult, s: string): bool {.inline.} =
  cr.commandTag == s

proc contains*(cr: CommandResult, s: string): bool {.inline.} =
  ## Check if the command tag contains the given string.
  s in cr.commandTag

proc isNull*(row: Row, col: int): bool =
  ## Check if the column value is NULL.
  let idx = (int(row.rowIdx) * int(row.data.numCols) + col) * 2
  row.data.cellIndex[idx + 1] == -1'i32

proc isBinaryCol*(row: Row, col: int): bool {.inline.} =
  ## Check if column was received in binary format.
  row.data.colFormats.len > col and row.data.colFormats[col] == 1'i16

proc colTypeOid*(row: Row, col: int): int32 {.inline.} =
  ## Get the type OID for a column, or 0 if not available.
  if row.data.colTypeOids.len > col:
    row.data.colTypeOids[col]
  else:
    0'i32

proc getStr*(row: Row, col: int): string =
  ## Get a column value as a string. Handles binary-to-text conversion for
  ## common types (bool, int2/4/8, float4/8). Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    let oid = row.colTypeOid(col)
    let b = row.data.buf
    case oid
    of 16: # bool
      return if b[off] != 0: "t" else: "f"
    of 21: # int2
      if clen == 2:
        return $int16((uint16(b[off]) shl 8) or uint16(b[off + 1]))
    of 23: # int4
      if clen == 4:
        return $int32(
          (uint32(b[off]) shl 24) or (uint32(b[off + 1]) shl 16) or
            (uint32(b[off + 2]) shl 8) or uint32(b[off + 3])
        )
    of 20: # int8
      if clen == 8:
        return $int64(
          (uint64(b[off]) shl 56) or (uint64(b[off + 1]) shl 48) or
            (uint64(b[off + 2]) shl 40) or (uint64(b[off + 3]) shl 32) or
            (uint64(b[off + 4]) shl 24) or (uint64(b[off + 5]) shl 16) or
            (uint64(b[off + 6]) shl 8) or uint64(b[off + 7])
        )
    of 700: # float4
      if clen == 4:
        var bits = uint32(
          (uint32(b[off]) shl 24) or (uint32(b[off + 1]) shl 16) or
            (uint32(b[off + 2]) shl 8) or uint32(b[off + 3])
        )
        var f: float32
        copyMem(addr f, addr bits, 4)
        return $f
    of 701: # float8
      if clen == 8:
        var bits = uint64(
          (uint64(b[off]) shl 56) or (uint64(b[off + 1]) shl 48) or
            (uint64(b[off + 2]) shl 40) or (uint64(b[off + 3]) shl 32) or
            (uint64(b[off + 4]) shl 24) or (uint64(b[off + 5]) shl 16) or
            (uint64(b[off + 6]) shl 8) or uint64(b[off + 7])
        )
        var f: float64
        copyMem(addr f, addr bits, 8)
        return $f
    of OidNumeric:
      return $decodeNumericBinary(b.toOpenArray(off, off + clen - 1))
    else:
      discard # text, varchar, bytea: fall through to raw copy
  result = newString(clen)
  if clen > 0:
    copyMem(addr result[0], unsafeAddr row.data.buf[off], clen)

proc getInt*(row: Row, col: int): int32 =
  ## Get a column value as int32. Handles binary int2/int4 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 4:
      let b = row.data.buf
      return int32(
        (uint32(b[off]) shl 24) or (uint32(b[off + 1]) shl 16) or
          (uint32(b[off + 2]) shl 8) or uint32(b[off + 3])
      )
    elif clen == 2:
      let b = row.data.buf
      return int32(int16((uint16(b[off]) shl 8) or uint16(b[off + 1])))
  var v: int
  discard parseInt(row.bufView(off, clen), v)
  result = int32(v)

proc getInt64*(row: Row, col: int): int64 =
  ## Get a column value as int64. Handles binary int2/4/8 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 8:
      let b = row.data.buf
      return int64(
        (uint64(b[off]) shl 56) or (uint64(b[off + 1]) shl 48) or
          (uint64(b[off + 2]) shl 40) or (uint64(b[off + 3]) shl 32) or
          (uint64(b[off + 4]) shl 24) or (uint64(b[off + 5]) shl 16) or
          (uint64(b[off + 6]) shl 8) or uint64(b[off + 7])
      )
    elif clen == 4:
      let b = row.data.buf
      return int64(
        int32(
          (uint32(b[off]) shl 24) or (uint32(b[off + 1]) shl 16) or
            (uint32(b[off + 2]) shl 8) or uint32(b[off + 3])
        )
      )
    elif clen == 2:
      let b = row.data.buf
      return int64(int16((uint16(b[off]) shl 8) or uint16(b[off + 1])))
  var v: BiggestInt
  discard parseBiggestInt(row.bufView(off, clen), v)
  result = v

proc getFloat*(row: Row, col: int): float64 =
  ## Get a column value as float64. Handles binary float4/8 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 8:
      var bits: uint64
      let b = row.data.buf
      bits =
        (uint64(b[off]) shl 56) or (uint64(b[off + 1]) shl 48) or
        (uint64(b[off + 2]) shl 40) or (uint64(b[off + 3]) shl 32) or
        (uint64(b[off + 4]) shl 24) or (uint64(b[off + 5]) shl 16) or
        (uint64(b[off + 6]) shl 8) or uint64(b[off + 7])
      copyMem(addr result, addr bits, 8)
      return
    elif clen == 4:
      var bits: uint32
      let b = row.data.buf
      bits =
        (uint32(b[off]) shl 24) or (uint32(b[off + 1]) shl 16) or
        (uint32(b[off + 2]) shl 8) or uint32(b[off + 3])
      var f32: float32
      copyMem(addr f32, addr bits, 4)
      return float64(f32)
  discard parseFloat(row.bufView(off, clen), result)

proc getNumeric*(row: Row, col: int): PgNumeric =
  ## Get a column value as PgNumeric. Handles binary numeric format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen >= 8:
      return decodeNumericBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  parsePgNumeric(row.getStr(col))

proc getUuid*(row: Row, col: int): PgUuid =
  ## Get a column value as PgUuid. Handles binary format (16 bytes).
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen == 16:
      const hexChars = "0123456789abcdef"
      var s = newString(36)
      var pos = 0
      for i in 0 ..< 16:
        if i == 4 or i == 6 or i == 8 or i == 10:
          s[pos] = '-'
          inc pos
        let b = row.data.buf[off + i]
        s[pos] = hexChars[int(b shr 4)]
        s[pos + 1] = hexChars[int(b and 0x0F)]
        pos += 2
      return PgUuid(s)
  PgUuid(row.getStr(col))

proc getBool*(row: Row, col: int): bool =
  ## Get a column value as bool. Handles binary format directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    return row.data.buf[off] != 0
  let c = char(row.data.buf[off])
  case c
  of 't', '1':
    true
  of 'f', '0':
    false
  else:
    raise newException(PgTypeError, "Invalid boolean value: " & c)

proc getBytes*(row: Row, col: int): seq[byte] =
  ## Get a column value as raw bytes. Decodes hex-encoded bytea in text format.
  ## Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    # Binary format: raw bytes, no hex encoding
    result = newSeq[byte](clen)
    if clen > 0:
      copyMem(addr result[0], unsafeAddr row.data.buf[off], clen)
    return
  # Text format: bytea uses hex encoding \xDEADBEEF
  if clen >= 2 and row.data.buf[off] == byte('\\') and row.data.buf[off + 1] == byte(
    'x'
  ):
    let hexLen = clen - 2
    var hex = newString(hexLen)
    for i in 0 ..< hexLen:
      hex[i] = char(row.data.buf[off + 2 + i])
    result = newSeq[byte](hexLen div 2)
    for i in 0 ..< result.len:
      result[i] = byte(parseHexInt(hex[i * 2 .. i * 2 + 1]))
  else:
    result = newSeq[byte](clen)
    if clen > 0:
      copyMem(addr result[0], unsafeAddr row.data.buf[off], clen)

proc getTimestamp*(row: Row, col: int): DateTime =
  ## Get a column value as DateTime. Handles binary timestamp format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeBinaryTimestamp(row.data.buf.toOpenArray(off, off + 7))
  let s = row.getStr(col)
  const formats = [
    "yyyy-MM-dd HH:mm:ss'.'ffffffzzz", "yyyy-MM-dd HH:mm:ss'.'ffffffzz",
    "yyyy-MM-dd HH:mm:ss'.'ffffff", "yyyy-MM-dd HH:mm:sszzz", "yyyy-MM-dd HH:mm:sszz",
    "yyyy-MM-dd HH:mm:ss",
  ]
  for fmt in formats:
    try:
      return parse(s, fmt)
    except TimeParseError:
      discard
  raise newException(PgTypeError, "Invalid timestamp: " & s)

proc getDate*(row: Row, col: int): DateTime =
  ## Get a column value as DateTime. Handles binary date format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeBinaryDate(row.data.buf.toOpenArray(off, off + 3))
  let s = row.getStr(col)
  try:
    return parse(s, "yyyy-MM-dd")
  except TimeParseError:
    raise newException(PgTypeError, "Invalid date: " & s)

proc getJson*(row: Row, col: int): JsonNode =
  ## Get a column value as a parsed JsonNode. Handles binary json/jsonb format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    var jsonStr: string
    if row.colTypeOid(col) == OidJsonb and clen > 0 and row.data.buf[off] == 1:
      # jsonb binary: skip version byte
      jsonStr = newString(clen - 1)
      for i in 1 ..< clen:
        jsonStr[i - 1] = char(row.data.buf[off + i])
    else:
      jsonStr = newString(clen)
      for i in 0 ..< clen:
        jsonStr[i] = char(row.data.buf[off + i])
    try:
      return parseJson(jsonStr)
    except JsonParsingError:
      raise newException(PgTypeError, "Invalid JSON: " & jsonStr)
  let s = row.getStr(col)
  try:
    return parseJson(s)
  except JsonParsingError:
    raise newException(PgTypeError, "Invalid JSON: " & s)

proc parseIntervalText(s: string): PgInterval =
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

proc getInterval*(row: Row, col: int): PgInterval =
  ## Get a column value as PgInterval. Handles binary interval format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 16:
      raise newException(PgTypeError, "Invalid binary interval length: " & $clen)
    result.microseconds = fromBE64(row.data.buf.toOpenArray(off, off + 7))
    result.days = fromBE32(row.data.buf.toOpenArray(off + 8, off + 11))
    result.months = fromBE32(row.data.buf.toOpenArray(off + 12, off + 15))
    return
  let s = row.getStr(col)
  parseIntervalText(s)

proc parseInetText(s: string): tuple[address: IpAddress, mask: uint8] =
  let slashIdx = s.find('/')
  if slashIdx == -1:
    let ip = parseIpAddress(s)
    let defaultMask = if ip.family == IpAddressFamily.IPv4: 32'u8 else: 128'u8
    return (ip, defaultMask)
  let addrStr = s.substr(0, slashIdx - 1)
  let maskStr = s.substr(slashIdx + 1)
  result = (parseIpAddress(addrStr), uint8(parseInt(maskStr)))

proc getInet*(row: Row, col: int): PgInet =
  ## Get a column value as PgInet (IP address with mask). Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let (ip, mask) = decodeInetBinary(row.data.buf.toOpenArray(off, off + clen - 1))
    return PgInet(address: ip, mask: mask)
  let s = row.getStr(col)
  let (ip, mask) = parseInetText(s)
  PgInet(address: ip, mask: mask)

proc getCidr*(row: Row, col: int): PgCidr =
  ## Get a column value as PgCidr (CIDR network address). Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let (ip, mask) = decodeInetBinary(row.data.buf.toOpenArray(off, off + clen - 1))
    return PgCidr(address: ip, mask: mask)
  let s = row.getStr(col)
  let (ip, mask) = parseInetText(s)
  PgCidr(address: ip, mask: mask)

proc getMacAddr*(row: Row, col: int): PgMacAddr =
  ## Get a column value as PgMacAddr. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 6:
      raise newException(PgTypeError, "Invalid binary macaddr length: " & $clen)
    var parts = newSeq[string](6)
    for i in 0 ..< 6:
      parts[i] = toHex(row.data.buf[off + i], 2).toLowerAscii()
    return PgMacAddr(parts.join(":"))
  PgMacAddr(row.getStr(col))

proc getMacAddr8*(row: Row, col: int): PgMacAddr8 =
  ## Get a column value as PgMacAddr8 (EUI-64). Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 8:
      raise newException(PgTypeError, "Invalid binary macaddr8 length: " & $clen)
    var parts = newSeq[string](8)
    for i in 0 ..< 8:
      parts[i] = toHex(row.data.buf[off + i], 2).toLowerAscii()
    return PgMacAddr8(parts.join(":"))
  PgMacAddr8(row.getStr(col))

# Geometry text format parsers

proc parsePointText(s: string): PgPoint =
  ## Parse "(x,y)" text format.
  var inner = s.strip()
  if inner.len >= 2 and inner[0] == '(' and inner[^1] == ')':
    inner = inner[1 ..^ 2]
  let comma = inner.find(',')
  if comma < 0:
    raise newException(PgTypeError, "Invalid point: " & s)
  PgPoint(x: parseFloat(inner[0 ..< comma]), y: parseFloat(inner[comma + 1 ..^ 1]))

proc parsePointsText(s: string): seq[PgPoint] =
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

proc getPoint*(row: Row, col: int): PgPoint =
  ## Get a column value as PgPoint. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 16:
      raise newException(PgTypeError, "Invalid binary point length: " & $clen)
    return decodePointBinary(row.data.buf, off)
  parsePointText(row.getStr(col))

proc getLine*(row: Row, col: int): PgLine =
  ## Get a column value as PgLine. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 24:
      raise newException(PgTypeError, "Invalid binary line length: " & $clen)
    let b = row.data.buf
    var aBits, bBits, cBits: uint64
    aBits =
      (uint64(b[off]) shl 56) or (uint64(b[off + 1]) shl 48) or
      (uint64(b[off + 2]) shl 40) or (uint64(b[off + 3]) shl 32) or
      (uint64(b[off + 4]) shl 24) or (uint64(b[off + 5]) shl 16) or
      (uint64(b[off + 6]) shl 8) or uint64(b[off + 7])
    bBits =
      (uint64(b[off + 8]) shl 56) or (uint64(b[off + 9]) shl 48) or
      (uint64(b[off + 10]) shl 40) or (uint64(b[off + 11]) shl 32) or
      (uint64(b[off + 12]) shl 24) or (uint64(b[off + 13]) shl 16) or
      (uint64(b[off + 14]) shl 8) or uint64(b[off + 15])
    cBits =
      (uint64(b[off + 16]) shl 56) or (uint64(b[off + 17]) shl 48) or
      (uint64(b[off + 18]) shl 40) or (uint64(b[off + 19]) shl 32) or
      (uint64(b[off + 20]) shl 24) or (uint64(b[off + 21]) shl 16) or
      (uint64(b[off + 22]) shl 8) or uint64(b[off + 23])
    copyMem(addr result.a, addr aBits, 8)
    copyMem(addr result.b, addr bBits, 8)
    copyMem(addr result.c, addr cBits, 8)
    return
  let s = row.getStr(col)
  var inner = s.strip()
  if inner.len >= 2 and inner[0] == '{' and inner[^1] == '}':
    inner = inner[1 ..^ 2]
  else:
    raise newException(PgTypeError, "Invalid line: " & s)
  let parts = inner.split(',')
  if parts.len != 3:
    raise newException(PgTypeError, "Invalid line: " & s)
  PgLine(a: parseFloat(parts[0]), b: parseFloat(parts[1]), c: parseFloat(parts[2]))

proc getLseg*(row: Row, col: int): PgLseg =
  ## Get a column value as PgLseg. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 32:
      raise newException(PgTypeError, "Invalid binary lseg length: " & $clen)
    return PgLseg(
      p1: decodePointBinary(row.data.buf, off),
      p2: decodePointBinary(row.data.buf, off + 16),
    )
  let s = row.getStr(col).strip()
  var inner = s
  if inner.len >= 2 and inner[0] == '[' and inner[^1] == ']':
    inner = inner[1 ..^ 2]
  let points = parsePointsText(inner)
  if points.len != 2:
    raise newException(PgTypeError, "Invalid lseg: " & s)
  PgLseg(p1: points[0], p2: points[1])

proc getBox*(row: Row, col: int): PgBox =
  ## Get a column value as PgBox. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 32:
      raise newException(PgTypeError, "Invalid binary box length: " & $clen)
    return PgBox(
      high: decodePointBinary(row.data.buf, off),
      low: decodePointBinary(row.data.buf, off + 16),
    )
  let s = row.getStr(col).strip()
  let points = parsePointsText(s)
  if points.len != 2:
    raise newException(PgTypeError, "Invalid box: " & s)
  PgBox(high: points[0], low: points[1])

proc getPath*(row: Row, col: int): PgPath =
  ## Get a column value as PgPath. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let b = row.data.buf
    result.closed = b[off] != 0
    let npts = fromBE32(b.toOpenArray(off + 1, off + 4))
    result.points = newSeq[PgPoint](npts)
    for i in 0 ..< npts:
      result.points[i] = decodePointBinary(b, off + 5 + i * 16)
    return
  let s = row.getStr(col).strip()
  if s.len < 2:
    raise newException(PgTypeError, "Invalid path: " & s)
  let closed = s[0] == '('
  let inner = s[1 ..^ 2]
  let points = parsePointsText(inner)
  PgPath(closed: closed, points: points)

proc getPolygon*(row: Row, col: int): PgPolygon =
  ## Get a column value as PgPolygon. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let b = row.data.buf
    let npts = fromBE32(b.toOpenArray(off, off + 3))
    result.points = newSeq[PgPoint](npts)
    for i in 0 ..< npts:
      result.points[i] = decodePointBinary(b, off + 4 + i * 16)
    return
  let s = row.getStr(col).strip()
  if s.len < 2 or s[0] != '(' or s[^1] != ')':
    raise newException(PgTypeError, "Invalid polygon: " & s)
  let inner = s[1 ..^ 2]
  PgPolygon(points: parsePointsText(inner))

proc getCircle*(row: Row, col: int): PgCircle =
  ## Get a column value as PgCircle. Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 24:
      raise newException(PgTypeError, "Invalid binary circle length: " & $clen)
    result.center = decodePointBinary(row.data.buf, off)
    var rBits: uint64
    let b = row.data.buf
    rBits =
      (uint64(b[off + 16]) shl 56) or (uint64(b[off + 17]) shl 48) or
      (uint64(b[off + 18]) shl 40) or (uint64(b[off + 19]) shl 32) or
      (uint64(b[off + 20]) shl 24) or (uint64(b[off + 21]) shl 16) or
      (uint64(b[off + 22]) shl 8) or uint64(b[off + 23])
    copyMem(addr result.radius, addr rBits, 8)
    return
  let s = row.getStr(col).strip()
  if s.len < 2 or s[0] != '<' or s[^1] != '>':
    raise newException(PgTypeError, "Invalid circle: " & s)
  let inner = s[1 ..^ 2]
  # Find the last comma that's outside parens
  var depth = 0
  var lastComma = -1
  for i in 0 ..< inner.len:
    if inner[i] == '(':
      depth += 1
    elif inner[i] == ')':
      depth -= 1
    elif inner[i] == ',' and depth == 0:
      lastComma = i
  if lastComma < 0:
    raise newException(PgTypeError, "Invalid circle: " & s)
  let center = parsePointText(inner[0 ..< lastComma])
  let radius = parseFloat(inner[lastComma + 1 ..^ 1])
  PgCircle(center: center, radius: radius)

# NULL-safe Option accessors — return `none` for NULL instead of raising.
#
# Template helpers used throughout this module to eliminate boilerplate for
# delegation-only accessor overloads.  Each template generates a public proc
# whose body simply delegates to an existing overload with different parameter
# types.

template optAccessor(getProc, optProc: untyped, T: typedesc) =
  ## Generate ``optProc*(row, col): Option[T]`` that delegates to ``getProc``.
  proc optProc*(row: Row, col: int): Option[T] =
    if row.isNull(col):
      none(T)
    else:
      some(row.getProc(col))

template nameAccessor(getProc: untyped, T: typedesc) =
  ## Generate ``getProc*(row, name): T`` that delegates to the index-based overload.
  proc getProc*(row: Row, name: string): T =
    row.getProc(row.columnIndex(name))

optAccessor(getStr, getStrOpt, string)
optAccessor(getInt, getIntOpt, int32)
optAccessor(getInt64, getInt64Opt, int64)
optAccessor(getFloat, getFloatOpt, float64)
optAccessor(getNumeric, getNumericOpt, PgNumeric)
optAccessor(getUuid, getUuidOpt, PgUuid)
optAccessor(getBool, getBoolOpt, bool)
optAccessor(getJson, getJsonOpt, JsonNode)
optAccessor(getInterval, getIntervalOpt, PgInterval)
optAccessor(getInet, getInetOpt, PgInet)
optAccessor(getCidr, getCidrOpt, PgCidr)
optAccessor(getMacAddr, getMacAddrOpt, PgMacAddr)
optAccessor(getMacAddr8, getMacAddr8Opt, PgMacAddr8)
optAccessor(getPoint, getPointOpt, PgPoint)
optAccessor(getLine, getLineOpt, PgLine)
optAccessor(getLseg, getLsegOpt, PgLseg)
optAccessor(getBox, getBoxOpt, PgBox)
optAccessor(getPath, getPathOpt, PgPath)
optAccessor(getPolygon, getPolygonOpt, PgPolygon)
optAccessor(getCircle, getCircleOpt, PgCircle)

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

proc getIntArray*(row: Row, col: int): seq[int32] =
  ## Get a column value as a seq of int32. Handles binary array format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[int32](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in int array")
      result[i] =
        fromBE32(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in int array")
    result.add(int32(parseInt(e.get)))

proc getInt16Array*(row: Row, col: int): seq[int16] =
  ## Get a column value as a seq of int16. Handles binary array format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[int16](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in int16 array")
      result[i] =
        fromBE16(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in int16 array")
    result.add(int16(parseInt(e.get)))

proc getInt64Array*(row: Row, col: int): seq[int64] =
  ## Get a column value as a seq of int64. Handles binary array format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[int64](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in int64 array")
      result[i] =
        fromBE64(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in int64 array")
    result.add(parseBiggestInt(e.get))

proc getFloatArray*(row: Row, col: int): seq[float64] =
  ## Get a column value as a seq of float64. Handles binary array format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[float64](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in float array")
      if e.len == 4:
        result[i] = float64(
          cast[float32](cast[uint32](fromBE32(
            row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
          )))
        )
      else:
        result[i] = cast[float64](cast[uint64](fromBE64(
          row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
        )))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in float array")
    result.add(parseFloat(e.get))

proc getFloat32Array*(row: Row, col: int): seq[float32] =
  ## Get a column value as a seq of float32. Handles binary array format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[float32](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in float32 array")
      result[i] = cast[float32](cast[uint32](fromBE32(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in float32 array")
    result.add(float32(parseFloat(e.get)))

proc getBoolArray*(row: Row, col: int): seq[bool] =
  ## Get a column value as a seq of bool. Handles binary array format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[bool](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in bool array")
      result[i] = row.data.buf[off + e.off] == 1'u8
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in bool array")
    case e.get
    of "t", "true", "1":
      result.add(true)
    of "f", "false", "0":
      result.add(false)
    else:
      raise newException(PgTypeError, "Invalid boolean: " & e.get)

proc getStrArray*(row: Row, col: int): seq[string] =
  ## Get a column value as a seq of strings. Handles binary array format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[string](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in string array")
      result[i] = newString(e.len)
      if e.len > 0:
        copyMem(addr result[i][0], unsafeAddr row.data.buf[off + e.off], e.len)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in string array")
    result.add(e.get)

# Array Opt accessors (text format)

optAccessor(getIntArray, getIntArrayOpt, seq[int32])
optAccessor(getInt16Array, getInt16ArrayOpt, seq[int16])
optAccessor(getInt64Array, getInt64ArrayOpt, seq[int64])
optAccessor(getFloatArray, getFloatArrayOpt, seq[float64])
optAccessor(getFloat32Array, getFloat32ArrayOpt, seq[float32])
optAccessor(getBoolArray, getBoolArrayOpt, seq[bool])
optAccessor(getStrArray, getStrArrayOpt, seq[string])

# Coerce a binary PgParam to match the server-inferred type from a prepared
# statement.  This handles the common case where e.g. int32.toPgParam is
# passed but the server inferred int8 (LIMIT/OFFSET).  Only safe widening
# conversions are performed; incompatible types raise PgTypeError.

proc coerceBinaryParam*(param: PgParam, serverOid: int32): PgParam =
  ## Return a copy of `param` whose binary payload matches `serverOid`.
  ## Text-format parameters (format == 0) and matching OIDs are returned
  ## unchanged.  For binary-format parameters with a type mismatch, safe
  ## widening conversions are applied.
  if param.format == 0 or param.oid == serverOid or serverOid == 0:
    return param
  if param.value.isNone:
    # NULL – OID doesn't matter for the wire payload
    return PgParam(oid: serverOid, format: param.format, value: param.value)

  let data = param.value.get

  # int2 -> int4
  if param.oid == OidInt2 and serverOid == OidInt4 and data.len == 2:
    let v = int32(fromBE16(data))
    return PgParam(oid: OidInt4, format: 1, value: some(@(toBE32(v))))

  # int2 -> int8
  if param.oid == OidInt2 and serverOid == OidInt8 and data.len == 2:
    let v = int64(fromBE16(data))
    return PgParam(oid: OidInt8, format: 1, value: some(@(toBE64(v))))

  # int4 -> int8
  if param.oid == OidInt4 and serverOid == OidInt8 and data.len == 4:
    let v = int64(fromBE32(data))
    return PgParam(oid: OidInt8, format: 1, value: some(@(toBE64(v))))

  # float4 -> float8
  if param.oid == OidFloat4 and serverOid == OidFloat8 and data.len == 4:
    let f = cast[float32](fromBE32(data))
    let d = float64(f)
    return PgParam(oid: OidFloat8, format: 1, value: some(@(toBE64(cast[int64](d)))))

  raise newException(
    PgTypeError,
    "Prepared statement parameter type mismatch: client sent OID " & $param.oid &
      " (binary, " & $data.len & " bytes) but server expects OID " & $serverOid &
      ". Use an explicit SQL cast (e.g. $N::int4) or pass the correct type.",
  )

# PgParam-aware in-place encoding (avoids extractParams allocations)

proc addParse*(
    buf: var seq[byte], stmtName: string, sql: string, params: openArray[PgParam]
) =
  let msgStart = buf.len
  buf.add(byte('P'))
  buf.addInt32(0) # length placeholder
  buf.addCString(stmtName)
  buf.addCString(sql)
  buf.addInt16(int16(params.len))
  for p in params:
    buf.addInt32(p.oid)
  buf.patchMsgLen(msgStart)

proc addBind*(
    buf: var seq[byte],
    portalName: string,
    stmtName: string,
    params: openArray[PgParam],
    resultFormats: openArray[int16] = [],
) =
  let msgStart = buf.len
  buf.add(byte('B'))
  buf.addInt32(0) # length placeholder
  buf.addCString(portalName)
  buf.addCString(stmtName)
  # Parameter format codes
  buf.addInt16(int16(params.len))
  for p in params:
    buf.addInt16(p.format)
  # Parameter values
  buf.addInt16(int16(params.len))
  for p in params:
    if p.value.isNone:
      buf.addInt32(-1) # NULL
    else:
      let data = p.value.get
      buf.addInt32(int32(data.len))
      if data.len > 0:
        let oldLen = buf.len
        buf.setLen(oldLen + data.len)
        copyMem(addr buf[oldLen], unsafeAddr data[0], data.len)
  # Result format codes
  buf.addInt16(int16(resultFormats.len))
  for f in resultFormats:
    buf.addInt16(f)
  buf.patchMsgLen(msgStart)

# Zero-alloc parameter encoding — write directly to send buffer

proc writeParamFormat*(buf: var seq[byte], v: int16) =
  buf.addInt16(1'i16) # binary

proc writeParamFormat*(buf: var seq[byte], v: int32) =
  buf.addInt16(1'i16)

proc writeParamFormat*(buf: var seq[byte], v: int64) =
  buf.addInt16(1'i16)

proc writeParamFormat*(buf: var seq[byte], v: int) =
  buf.addInt16(1'i16)

proc writeParamFormat*(buf: var seq[byte], v: float32) =
  buf.addInt16(1'i16)

proc writeParamFormat*(buf: var seq[byte], v: float64) =
  buf.addInt16(1'i16)

proc writeParamFormat*(buf: var seq[byte], v: bool) =
  buf.addInt16(1'i16)

proc writeParamFormat*(buf: var seq[byte], v: string) =
  buf.addInt16(0'i16) # text

proc writeParamFormat*(buf: var seq[byte], v: seq[byte]) =
  buf.addInt16(0'i16)

proc writeParamFormat*(buf: var seq[byte], v: PgNumeric) =
  buf.addInt16(0'i16)

proc writeParamValue*(buf: var seq[byte], v: int16) =
  buf.addInt32(2'i32)
  buf.addInt16(v)

proc writeParamValue*(buf: var seq[byte], v: int32) =
  buf.addInt32(4'i32)
  let o = buf.len
  buf.setLen(o + 4)
  buf[o] = byte((v shr 24) and 0xFF)
  buf[o + 1] = byte((v shr 16) and 0xFF)
  buf[o + 2] = byte((v shr 8) and 0xFF)
  buf[o + 3] = byte(v and 0xFF)

proc writeParamValue*(buf: var seq[byte], v: int64) =
  buf.addInt32(8'i32)
  let o = buf.len
  buf.setLen(o + 8)
  buf[o] = byte((v shr 56) and 0xFF)
  buf[o + 1] = byte((v shr 48) and 0xFF)
  buf[o + 2] = byte((v shr 40) and 0xFF)
  buf[o + 3] = byte((v shr 32) and 0xFF)
  buf[o + 4] = byte((v shr 24) and 0xFF)
  buf[o + 5] = byte((v shr 16) and 0xFF)
  buf[o + 6] = byte((v shr 8) and 0xFF)
  buf[o + 7] = byte(v and 0xFF)

proc writeParamValue*(buf: var seq[byte], v: int) =
  writeParamValue(buf, int64(v))

proc writeParamValue*(buf: var seq[byte], v: float32) =
  let bits = cast[int32](v)
  buf.addInt32(4'i32)
  let o = buf.len
  buf.setLen(o + 4)
  buf[o] = byte((bits shr 24) and 0xFF)
  buf[o + 1] = byte((bits shr 16) and 0xFF)
  buf[o + 2] = byte((bits shr 8) and 0xFF)
  buf[o + 3] = byte(bits and 0xFF)

proc writeParamValue*(buf: var seq[byte], v: float64) =
  let bits = cast[int64](v)
  writeParamValue(buf, bits)

proc writeParamValue*(buf: var seq[byte], v: bool) =
  buf.addInt32(1'i32)
  buf.add(if v: 1'u8 else: 0'u8)

proc writeParamValue*(buf: var seq[byte], v: string) =
  buf.addInt32(int32(v.len))
  if v.len > 0:
    let o = buf.len
    buf.setLen(o + v.len)
    copyMem(addr buf[o], unsafeAddr v[0], v.len)

proc writeParamValue*(buf: var seq[byte], v: seq[byte]) =
  buf.addInt32(int32(v.len))
  if v.len > 0:
    let o = buf.len
    buf.setLen(o + v.len)
    copyMem(addr buf[o], unsafeAddr v[0], v.len)

proc writeParamValue*(buf: var seq[byte], v: PgNumeric) =
  writeParamValue(buf, $v)

proc writeParamOid*(buf: var seq[byte], v: int16) =
  buf.addInt32(OidInt2)

proc writeParamOid*(buf: var seq[byte], v: int32) =
  buf.addInt32(OidInt4)

proc writeParamOid*(buf: var seq[byte], v: int64) =
  buf.addInt32(OidInt8)

proc writeParamOid*(buf: var seq[byte], v: int) =
  buf.addInt32(OidInt8)

proc writeParamOid*(buf: var seq[byte], v: float32) =
  buf.addInt32(OidFloat4)

proc writeParamOid*(buf: var seq[byte], v: float64) =
  buf.addInt32(OidFloat8)

proc writeParamOid*(buf: var seq[byte], v: bool) =
  buf.addInt32(OidBool)

proc writeParamOid*(buf: var seq[byte], v: string) =
  buf.addInt32(OidText)

proc writeParamOid*(buf: var seq[byte], v: seq[byte]) =
  buf.addInt32(OidBytea)

proc writeParamOid*(buf: var seq[byte], v: PgNumeric) =
  buf.addInt32(OidNumeric)

macro addParseDirect*(
    buf: untyped, stmtName: string, sql: string, args: varargs[untyped]
): untyped =
  ## Compile-time macro: generates Parse message with OIDs from arg types.
  result = newStmtList()
  let msgStart = genSym(nskLet, "msgStart")
  let nParams = newLit(int16(args.len))
  result.add quote do:
    let `msgStart` = `buf`.len
    `buf`.add(byte('P'))
    `buf`.addInt32(0)
    `buf`.addCString(`stmtName`)
    `buf`.addCString(`sql`)
    `buf`.addInt16(`nParams`)
  for arg in args:
    result.add quote do:
      `buf`.writeParamOid(`arg`)
  result.add quote do:
    `buf`.patchMsgLen(`msgStart`)

macro addBindDirect*(
    buf: untyped,
    portalName: string,
    stmtName: string,
    resultFormats: untyped,
    args: varargs[untyped],
): untyped =
  ## Compile-time macro: generates Bind message writing params directly to buffer.
  ## Zero intermediate PgParam/``seq[byte]`` allocations.
  result = newStmtList()
  let msgStart = genSym(nskLet, "msgStart")
  let nParamsLit = newLit(int16(args.len))
  result.add quote do:
    let `msgStart` = `buf`.len
    `buf`.add(byte('B'))
    `buf`.addInt32(0)
    `buf`.addCString(`portalName`)
    `buf`.addCString(`stmtName`)
    # Parameter format codes
    `buf`.addInt16(`nParamsLit`)
  for arg in args:
    result.add quote do:
      `buf`.writeParamFormat(`arg`)
  result.add quote do:
    # Parameter values
    `buf`.addInt16(`nParamsLit`)
  for arg in args:
    result.add quote do:
      `buf`.writeParamValue(`arg`)
  # Result format codes — handle at compile time to avoid empty-bracket inference issues
  if resultFormats.kind == nnkBracket and resultFormats.len == 0:
    result.add quote do:
      `buf`.addInt16(0'i16)
      `buf`.patchMsgLen(`msgStart`)
  else:
    result.add quote do:
      `buf`.addInt16(int16(`resultFormats`.len))
      for f in `resultFormats`:
        `buf`.addInt16(f)
      `buf`.patchMsgLen(`msgStart`)

# User-defined enum type support
#
# PostgreSQL user-defined enums have dynamic OIDs assigned at creation time.
# Both text and binary wire formats transmit the enum label as a UTF-8 string.
#
# Usage:
#   type Mood = enum
#     happy = "happy"
#     sad = "sad"
#     ok = "ok"
#
#   pgEnum(Mood)                  # OID = 0; PostgreSQL infers the type
#   pgEnum(Mood, 12345'i32)      # explicit OID (e.g. from pg_type lookup)
#
# Reading rows:
#   let m = row.getEnum[Mood](0)
#   let m = row.getEnumOpt[Mood](0)

macro pgEnum*(T: untyped): untyped =
  ## Generate ``toPgParam`` for a Nim enum type.
  ## The parameter is sent as text with OID 0 (unspecified) so that
  ## PostgreSQL infers the enum type from context.
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: 0'i32, format: 0'i16, value: some(toBytes($v)))

macro pgEnum*(T: untyped, oid: untyped): untyped =
  ## Generate ``toPgParam`` for a Nim enum type with an explicit OID.
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: int32(`oid`), format: 0'i16, value: some(toBytes($v)))

proc getEnum*[T: enum](row: Row, col: int): T =
  ## Read a PostgreSQL enum column (text format) as a Nim enum.
  ## The column value must exactly match one of ``T``'s string representations.
  parseEnum[T](row.getStr(col))

proc getEnumOpt*[T: enum](row: Row, col: int): Option[T] =
  ## Read a PostgreSQL enum column as ``Option[T]``. Returns none if NULL.
  ## NULL-safe version of ``getEnum``.
  if row.isNull(col):
    none(T)
  else:
    some(getEnum[T](row, col))

# User-defined composite type support
#
# PostgreSQL composite types (row types / record types) have dynamic OIDs.
# Text format:  (val1,val2,...)  with quoting for special chars
# Binary format: numFields(4) + [oid(4) + len(4) + data]...
#
# Usage:
#   type Point = object
#     x: float64
#     y: float64
#
#   pgComposite(Point)                # OID = 0; PostgreSQL infers
#   pgComposite(Point, 12345'i32)     # explicit OID
#
# Reading rows:
#   let p = row.getComposite[Point](0)
#   let p = row.getCompositeOpt[Point](0)

proc parseCompositeText*(s: string): seq[Option[string]] =
  ## Parse PostgreSQL composite text format: (val1,val2,...)
  ## Returns fields as ``Option[string]`` (none for NULL).
  if s.len < 2 or s[0] != '(' or s[^1] != ')':
    raise newException(PgTypeError, "Invalid composite literal: " & s)
  let inner = s[1 ..^ 2]
  if inner.len == 0:
    return @[]
  var i = 0
  while i < inner.len:
    if inner[i] == ',':
      # Empty unquoted field at start or after comma = NULL
      result.add(none(string))
      i += 1
      if i == inner.len:
        result.add(none(string))
    elif inner[i] == '"':
      # Quoted field
      i += 1
      var elem = ""
      while i < inner.len:
        if inner[i] == '\\' and i + 1 < inner.len:
          i += 1
          elem.add(inner[i])
        elif inner[i] == '"':
          if i + 1 < inner.len and inner[i + 1] == '"':
            # Doubled quote
            elem.add('"')
            i += 1
          else:
            break
        else:
          elem.add(inner[i])
        i += 1
      i += 1 # skip closing quote
      result.add(some(elem))
      if i < inner.len and inner[i] == ',':
        i += 1
        if i == inner.len:
          result.add(none(string))
    else:
      # Unquoted field
      var elem = ""
      while i < inner.len and inner[i] != ',':
        elem.add(inner[i])
        i += 1
      result.add(some(elem))
      if i < inner.len and inner[i] == ',':
        i += 1
        if i == inner.len:
          result.add(none(string))

proc encodeBinaryComposite*(
    fields: seq[tuple[oid: int32, data: Option[seq[byte]]]]
): seq[byte] =
  ## Encode a PostgreSQL binary composite value.
  ## Format: ``numFields(4) + [oid(4) + len(4) + data]...``
  var size = 4
  for f in fields:
    size += 8 # oid + len
    if f.data.isSome:
      size += f.data.get.len
  result = newSeq[byte](size)
  let nf = toBE32(int32(fields.len))
  copyMem(addr result[0], unsafeAddr nf[0], 4)
  var pos = 4
  for f in fields:
    let oid = toBE32(f.oid)
    copyMem(addr result[pos], unsafeAddr oid[0], 4)
    pos += 4
    if f.data.isNone:
      let nl = toBE32(-1'i32)
      copyMem(addr result[pos], unsafeAddr nl[0], 4)
      pos += 4
    else:
      let data = f.data.get
      let dl = toBE32(int32(data.len))
      copyMem(addr result[pos], unsafeAddr dl[0], 4)
      pos += 4
      if data.len > 0:
        copyMem(addr result[pos], unsafeAddr data[0], data.len)
        pos += data.len

proc compositeFieldToText(val: string): string =
  ## Escape a composite field value for text format output.
  var needsQuote = val.len == 0
  for c in val:
    if c in {',', '(', ')', '"', '\\', ' '}:
      needsQuote = true
      break
  if not needsQuote:
    return val
  result = "\""
  for c in val:
    if c == '"':
      result.add("\"\"")
    elif c == '\\':
      result.add("\\\\")
    else:
      result.add(c)
  result.add('"')

proc encodeCompositeText*(fields: seq[Option[string]]): string =
  ## Encode fields as PostgreSQL composite text format: (val1,val2,...)
  result = "("
  for i, f in fields:
    if i > 0:
      result.add(',')
    if f.isSome:
      result.add(compositeFieldToText(f.get))
  result.add(')')

macro pgComposite*(T: typedesc, oid: int32 = 0'i32): untyped =
  ## Generate ``toPgParam`` for a Nim object as a PostgreSQL composite type.
  ## Each field is sent as text inside the composite text format.
  ## When OID is 0 (default), PostgreSQL infers the type from context.
  let tImpl = T.getType[1]
  let tSym = tImpl
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `tSym`): PgParam =
      var fields: seq[Option[string]]
      for _, val in v.fieldPairs:
        when typeof(val) is Option:
          if val.isSome:
            fields.add(some($val.get))
          else:
            fields.add(none(string))
        else:
          fields.add(some($val))
      PgParam(
        oid: `oid`, format: 0'i16, value: some(toBytes(encodeCompositeText(fields)))
      )

proc compositeFieldFromText[T](s: string): T =
  ## Parse a single composite text field to the target type.
  when T is string:
    s
  elif T is int32:
    int32(parseInt(s))
  elif T is int16:
    int16(parseInt(s))
  elif T is int64:
    parseBiggestInt(s)
  elif T is int:
    parseInt(s)
  elif T is float64:
    parseFloat(s)
  elif T is float32:
    float32(parseFloat(s))
  elif T is bool:
    case s
    of "t", "true", "1":
      true
    of "f", "false", "0":
      false
    else:
      raise newException(PgTypeError, "Invalid boolean in composite: " & s)
  elif T is PgNumeric:
    parsePgNumeric(s)
  else:
    raise newException(PgTypeError, "Unsupported composite field type")

template decodeBinaryField(val, buf: untyped, fOff, fEnd, fLen: int) =
  when typeof(val) is string:
    val = newString(fLen)
    if fLen > 0:
      copyMem(addr val[0], unsafeAddr buf[fOff], fLen)
  elif typeof(val) is int16:
    val = fromBE16(buf.toOpenArray(fOff, fEnd))
  elif typeof(val) is int32:
    val = fromBE32(buf.toOpenArray(fOff, fEnd))
  elif typeof(val) is (int64 or int):
    val = typeof(val)(fromBE64(buf.toOpenArray(fOff, fEnd)))
  elif typeof(val) is float64:
    val = cast[float64](cast[uint64](fromBE64(buf.toOpenArray(fOff, fEnd))))
  elif typeof(val) is float32:
    val = cast[float32](cast[uint32](fromBE32(buf.toOpenArray(fOff, fEnd))))
  elif typeof(val) is bool:
    val = buf[fOff] != 0
  else:
    var s = newString(fLen)
    if fLen > 0:
      copyMem(addr s[0], unsafeAddr buf[fOff], fLen)
    val = compositeFieldFromText[typeof(val)](s)

proc getComposite*[T: object](row: Row, col: int): T =
  ## Read a PostgreSQL composite column as a Nim object. Handles binary format.
  if row.isNull(col):
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    let decoded = decodeBinaryComposite(row.data.buf.toOpenArray(off, off + clen - 1))
    var idx = 0
    for _, val in result.fieldPairs:
      if idx >= decoded.len:
        raise newException(PgTypeError, "Binary composite has fewer fields than object")
      let f = decoded[idx]
      let fOff = off + f.off
      let fEnd = fOff + f.len - 1
      when typeof(val) is Option:
        if f.len == -1:
          val = none(typeof(val.get))
        else:
          var inner: typeof(val.get)
          decodeBinaryField(inner, row.data.buf, fOff, fEnd, f.len)
          val = some(inner)
      else:
        if f.len == -1:
          raise
            newException(PgTypeError, "NULL field in binary composite at index " & $idx)
        decodeBinaryField(val, row.data.buf, fOff, fEnd, f.len)
      idx += 1
    return
  let s = row.getStr(col)
  let parts = parseCompositeText(s)
  var idx = 0
  for _, val in result.fieldPairs:
    if idx >= parts.len:
      raise newException(PgTypeError, "Composite has fewer fields than object")
    when typeof(val) is Option:
      if parts[idx].isNone:
        val = none(typeof(val.get))
      else:
        val = some(compositeFieldFromText[typeof(val.get)](parts[idx].get))
    else:
      if parts[idx].isNone:
        raise newException(PgTypeError, "NULL field in composite at index " & $idx)
      val = compositeFieldFromText[typeof(val)](parts[idx].get)
    idx += 1

proc getCompositeOpt*[T: object](row: Row, col: int): Option[T] =
  ## NULL-safe version of ``getComposite``.
  if row.isNull(col):
    none(T)
  else:
    some(getComposite[T](row, col))

# Range type support
#
# PostgreSQL range types represent a range of values of some element type.
# Text format:  [lower,upper)  (lower,upper]  empty  [lower,)  (,upper]
# Binary format: flags(1) + [len(4) + lower] + [len(4) + upper]
#   flags: 0x01=empty, 0x02=has_lower, 0x04=has_upper, 0x08=lower_inc, 0x10=upper_inc
#
# Built-in range types: int4range, int8range, numrange, tsrange, tstzrange, daterange
#
# Usage:
#   let r = rangeOf(1'i32, 10'i32)           # [1,10)
#   let r = rangeOf(1'i32, 10'i32, upperInc=true)  # [1,10]
#   let r = emptyRange[int32]()               # empty
#   let r = rangeFrom(5'i64)                  # [5,)
#
# Reading rows:
#   let r = row.getInt4Range(0)
#   let r = row.getInt4RangeOpt(0)

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

proc parseRangeText*[T](s: string, parseElem: proc(s: string): T): PgRange[T] =
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
    let lb = toBE32(int32(r.lowerData.len))
    copyMem(addr result[pos], unsafeAddr lb[0], 4)
    pos += 4
    if r.lowerData.len > 0:
      copyMem(addr result[pos], unsafeAddr r.lowerData[0], r.lowerData.len)
      pos += r.lowerData.len
  if r.hasUpper:
    let ub = toBE32(int32(r.upperData.len))
    copyMem(addr result[pos], unsafeAddr ub[0], 4)
    pos += 4
    if r.upperData.len > 0:
      copyMem(addr result[pos], unsafeAddr r.upperData[0], r.upperData.len)

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
    proc(e: string): int32 =
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
    proc(e: string): int64 =
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
    proc(e: string): PgNumeric =
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
    proc(e: string): DateTime =
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
    proc(e: string): DateTime =
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
    proc(e: string): DateTime =
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
#
# PostgreSQL multirange types (PostgreSQL 14+) represent a set of non-overlapping ranges.
# Text format:  {[1,3),[5,8)}
# Binary format: count(4) + [len(4) + range_binary_data]...
#
# Usage:
#   let mr = toMultirange(rangeOf(1'i32, 3'i32), rangeOf(5'i32, 8'i32))
#   let mr = row.getInt4Multirange(0)

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
    s: string, parseElem: proc(s: string): T
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
  let cnt = toBE32(int32(rangeData.len))
  copyMem(addr result[0], unsafeAddr cnt[0], 4)
  var pos = 4
  for rd in rangeData:
    let rl = toBE32(int32(rd.len))
    copyMem(addr result[pos], unsafeAddr rl[0], 4)
    pos += 4
    if rd.len > 0:
      copyMem(addr result[pos], unsafeAddr rd[0], rd.len)
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
    proc(e: string): int32 =
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
    proc(e: string): int64 =
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
    proc(e: string): PgNumeric =
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
    proc(e: string): DateTime =
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
    proc(e: string): DateTime =
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
    proc(e: string): DateTime =
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

# Generic accessors — static dispatch by type, no OID branching.
# DateTime-based types (DateTime, PgRange[DateTime], PgMultirange[DateTime])
# are excluded because DateTime maps to multiple PG types (timestamp,
# timestamptz, date) making a single `get` overload ambiguous. Use the
# explicit getters (getTimestamp, getTimestampTz, getDate, etc.) instead.

proc get*(row: Row, col: int, T: typedesc[int32]): int32 =
  ## Generic typed accessor. Usage: ``row.get(0, int32)``
  row.getInt(col)

proc get*(row: Row, col: int, T: typedesc[int64]): int64 =
  row.getInt64(col)

proc get*(row: Row, col: int, T: typedesc[float64]): float64 =
  row.getFloat(col)

proc get*(row: Row, col: int, T: typedesc[bool]): bool =
  row.getBool(col)

proc get*(row: Row, col: int, T: typedesc[string]): string =
  row.getStr(col)

proc get*(row: Row, col: int, T: typedesc[seq[byte]]): seq[byte] =
  row.getBytes(col)

proc get*(row: Row, col: int, T: typedesc[PgNumeric]): PgNumeric =
  row.getNumeric(col)

proc get*(row: Row, col: int, T: typedesc[JsonNode]): JsonNode =
  row.getJson(col)

proc get*(row: Row, col: int, T: typedesc[PgInterval]): PgInterval =
  row.getInterval(col)

proc get*(row: Row, col: int, T: typedesc[PgInet]): PgInet =
  row.getInet(col)

proc get*(row: Row, col: int, T: typedesc[PgCidr]): PgCidr =
  row.getCidr(col)

proc get*(row: Row, col: int, T: typedesc[PgMacAddr]): PgMacAddr =
  row.getMacAddr(col)

proc get*(row: Row, col: int, T: typedesc[PgMacAddr8]): PgMacAddr8 =
  row.getMacAddr8(col)

proc get*(row: Row, col: int, T: typedesc[PgPoint]): PgPoint =
  row.getPoint(col)

proc get*(row: Row, col: int, T: typedesc[PgLine]): PgLine =
  row.getLine(col)

proc get*(row: Row, col: int, T: typedesc[PgLseg]): PgLseg =
  row.getLseg(col)

proc get*(row: Row, col: int, T: typedesc[PgBox]): PgBox =
  row.getBox(col)

proc get*(row: Row, col: int, T: typedesc[PgPath]): PgPath =
  row.getPath(col)

proc get*(row: Row, col: int, T: typedesc[PgPolygon]): PgPolygon =
  row.getPolygon(col)

proc get*(row: Row, col: int, T: typedesc[PgCircle]): PgCircle =
  row.getCircle(col)

# Array types

proc get*(row: Row, col: int, T: typedesc[seq[int16]]): seq[int16] =
  row.getInt16Array(col)

proc get*(row: Row, col: int, T: typedesc[seq[int32]]): seq[int32] =
  row.getIntArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[int64]]): seq[int64] =
  row.getInt64Array(col)

proc get*(row: Row, col: int, T: typedesc[seq[float32]]): seq[float32] =
  row.getFloat32Array(col)

proc get*(row: Row, col: int, T: typedesc[seq[float64]]): seq[float64] =
  row.getFloatArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[bool]]): seq[bool] =
  row.getBoolArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[string]]): seq[string] =
  row.getStrArray(col)

# Range types (DateTime-based ranges excluded — see note above)

proc get*(row: Row, col: int, T: typedesc[PgRange[int32]]): PgRange[int32] =
  row.getInt4Range(col)

proc get*(row: Row, col: int, T: typedesc[PgRange[int64]]): PgRange[int64] =
  row.getInt8Range(col)

proc get*(row: Row, col: int, T: typedesc[PgRange[PgNumeric]]): PgRange[PgNumeric] =
  row.getNumRange(col)

# Multirange types (DateTime-based multiranges excluded — see note above)

proc get*(row: Row, col: int, T: typedesc[PgMultirange[int32]]): PgMultirange[int32] =
  row.getInt4Multirange(col)

proc get*(row: Row, col: int, T: typedesc[PgMultirange[int64]]): PgMultirange[int64] =
  row.getInt8Multirange(col)

proc get*(
    row: Row, col: int, T: typedesc[PgMultirange[PgNumeric]]
): PgMultirange[PgNumeric] =
  row.getNumMultirange(col)

proc columnIndex*(fields: seq[FieldDescription], name: string): int =
  ## Find the index of a column by name. Raises PgTypeError if not found.
  for i, f in fields:
    if f.name == name:
      return i
  raise newException(PgTypeError, "Column not found: " & name)

proc columnMap*(fields: seq[FieldDescription]): Table[string, int] =
  ## Build a name-to-index mapping for all columns.
  for i, f in fields:
    result[f.name] = i

# Name-based column access

proc columnIndex*(row: Row, name: string): int =
  ## Find the index of a column by name using a cached name→index table on the
  ## row's underlying ``RowData``.  The table is built lazily on first access.
  ## Raises ``PgTypeError`` if the metadata is not available (e.g. the Row was
  ## constructed manually) or the column name is not found.
  if row.data == nil or row.data.fields.len == 0:
    raise newException(PgTypeError, "Column name lookup requires field metadata")
  if row.data.colMap.len == 0 and row.data.fields.len > 0:
    for i, f in row.data.fields:
      row.data.colMap[f.name] = i
  let idx = row.data.colMap.getOrDefault(name, -1)
  if idx < 0:
    raise newException(PgTypeError, "Column not found: " & name)
  idx

# Generic typed accessor by column name

proc get*[T](row: Row, name: string, _: typedesc[T]): T =
  ## Generic typed accessor by column name. Usage: ``row.get("id", int32)``
  row.get(row.columnIndex(name), T)

nameAccessor(isNull, bool)
nameAccessor(getStr, string)
nameAccessor(getInt, int32)
nameAccessor(getInt64, int64)
nameAccessor(getFloat, float64)
nameAccessor(getBool, bool)
nameAccessor(getBytes, seq[byte])
nameAccessor(getNumeric, PgNumeric)
nameAccessor(getUuid, PgUuid)
nameAccessor(getTimestamp, DateTime)
nameAccessor(getDate, DateTime)
nameAccessor(getJson, JsonNode)
nameAccessor(getInterval, PgInterval)
nameAccessor(getInet, PgInet)
nameAccessor(getCidr, PgCidr)
nameAccessor(getMacAddr, PgMacAddr)
nameAccessor(getMacAddr8, PgMacAddr8)
nameAccessor(getPoint, PgPoint)
nameAccessor(getLine, PgLine)
nameAccessor(getLseg, PgLseg)
nameAccessor(getBox, PgBox)
nameAccessor(getPath, PgPath)
nameAccessor(getPolygon, PgPolygon)
nameAccessor(getCircle, PgCircle)
nameAccessor(getStrOpt, Option[string])
nameAccessor(getIntOpt, Option[int32])
nameAccessor(getInt64Opt, Option[int64])
nameAccessor(getFloatOpt, Option[float64])
nameAccessor(getNumericOpt, Option[PgNumeric])
nameAccessor(getUuidOpt, Option[PgUuid])
nameAccessor(getBoolOpt, Option[bool])
nameAccessor(getJsonOpt, Option[JsonNode])
nameAccessor(getIntervalOpt, Option[PgInterval])
nameAccessor(getInetOpt, Option[PgInet])
nameAccessor(getCidrOpt, Option[PgCidr])
nameAccessor(getMacAddrOpt, Option[PgMacAddr])
nameAccessor(getMacAddr8Opt, Option[PgMacAddr8])
nameAccessor(getPointOpt, Option[PgPoint])
nameAccessor(getLineOpt, Option[PgLine])
nameAccessor(getLsegOpt, Option[PgLseg])
nameAccessor(getBoxOpt, Option[PgBox])
nameAccessor(getPathOpt, Option[PgPath])
nameAccessor(getPolygonOpt, Option[PgPolygon])
nameAccessor(getCircleOpt, Option[PgCircle])
nameAccessor(getIntArray, seq[int32])
nameAccessor(getInt16Array, seq[int16])
nameAccessor(getInt64Array, seq[int64])
nameAccessor(getFloatArray, seq[float64])
nameAccessor(getFloat32Array, seq[float32])
nameAccessor(getBoolArray, seq[bool])
nameAccessor(getStrArray, seq[string])
nameAccessor(getIntArrayOpt, Option[seq[int32]])
nameAccessor(getInt16ArrayOpt, Option[seq[int16]])
nameAccessor(getInt64ArrayOpt, Option[seq[int64]])
nameAccessor(getFloatArrayOpt, Option[seq[float64]])
nameAccessor(getFloat32ArrayOpt, Option[seq[float32]])
nameAccessor(getBoolArrayOpt, Option[seq[bool]])
nameAccessor(getStrArrayOpt, Option[seq[string]])
nameAccessor(getInt4Range, PgRange[int32])
nameAccessor(getInt8Range, PgRange[int64])
nameAccessor(getNumRange, PgRange[PgNumeric])
nameAccessor(getTsRange, PgRange[DateTime])
nameAccessor(getTsTzRange, PgRange[DateTime])
nameAccessor(getDateRange, PgRange[DateTime])
nameAccessor(getInt4RangeOpt, Option[PgRange[int32]])
nameAccessor(getInt8RangeOpt, Option[PgRange[int64]])
nameAccessor(getNumRangeOpt, Option[PgRange[PgNumeric]])
nameAccessor(getTsRangeOpt, Option[PgRange[DateTime]])
nameAccessor(getTsTzRangeOpt, Option[PgRange[DateTime]])
nameAccessor(getDateRangeOpt, Option[PgRange[DateTime]])
nameAccessor(getInt4Multirange, PgMultirange[int32])
nameAccessor(getInt8Multirange, PgMultirange[int64])
nameAccessor(getNumMultirange, PgMultirange[PgNumeric])
nameAccessor(getTsMultirange, PgMultirange[DateTime])
nameAccessor(getTsTzMultirange, PgMultirange[DateTime])
nameAccessor(getDateMultirange, PgMultirange[DateTime])
nameAccessor(getInt4MultirangeOpt, Option[PgMultirange[int32]])
nameAccessor(getInt8MultirangeOpt, Option[PgMultirange[int64]])
nameAccessor(getNumMultirangeOpt, Option[PgMultirange[PgNumeric]])
nameAccessor(getTsMultirangeOpt, Option[PgMultirange[DateTime]])
nameAccessor(getTsTzMultirangeOpt, Option[PgMultirange[DateTime]])
nameAccessor(getDateMultirangeOpt, Option[PgMultirange[DateTime]])
