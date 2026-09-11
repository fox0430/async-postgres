import std/[hashes, math, options, parseutils, sequtils, strutils, tables, net]

import ../pg_errors
export pg_errors

import ../pg_bytes
export pg_bytes

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
    ## PostgreSQL money value: the raw signed 64-bit amount in the locale's
    ## minor currency unit plus ``scale``, the number of fractional digits
    ## (``frac_digits`` from ``lc_monetary``). The wire format carries only the
    ## amount, so ``scale`` is a client-side tag defaulting to 2; a server on a
    ## non-default ``lc_monetary`` (``ja_JP`` → 0) must be told. `initPgMoney`
    ## is the only constructor. Comparison and ``hash`` use the decimal value,
    ## so ``1.00`` at scale 2 equals ``1.000`` at scale 3.
    amountRaw: int64
    scaleRaw: int8

  PgMoneyConventions* = object
    ## Rendering conventions of one ``lc_monetary`` locale, ``frac_digits``
    ## included, shared by ``formatPgMoney`` and ``parsePgMoney``.
    ## ``initPgMoneyConventions`` is the only constructor and rejects
    ## combinations that cannot be parsed back; a zero-initialized value is
    ## rejected on use rather than acting as ``fracDigits = 0``.
    symbolRaw: string
    decimalSepRaw: char
    thousandsSepRaw: string
    symbolBeforeRaw: bool
    sepBySpaceRaw: bool
    accountingParensRaw: bool
    fracDigitsRaw: int8
    groupDigitsRaw: int8

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
    hour*: int32 ## 0..24 (24 only as '24:00:00', PostgreSQL's end-of-day bound)
    minute*: int32 ## 0..59
    second*: int32 ## 0..59
    microsecond*: int32 ## 0..999999

  PgTimeTz* = object ## PostgreSQL time with time zone.
    hour*: int32 ## 0..24 (24 only as '24:00:00', PostgreSQL's end-of-day bound)
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
    nbitsRaw: int32
    dataRaw: seq[byte]
      ## Private coupled pair; build via `initPgBit` / `parseBitString`, read via `nbits` / `data`.

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
    rfAuto
      ## Default. Cache miss → text; cache hit → per-column binary for
      ## ``BinarySafeOids`` (see ``buildResultFormats``). Format is thus
      ## non-deterministic across calls; use typed accessors or force
      ## ``rfText`` when reading via ``getStr`` / typed ``string``.
    rfText ## All columns in text format.
    rfBinary ## All columns in binary format.

  CommandResult* = object
    ## Result of a command execution, wrapping the PostgreSQL command tag.
    commandTag*: string
      ## Raw command tag string (e.g. "INSERT 0 1", "UPDATE 3", "DELETE 5").

const
  PgBitMaxBits* = 1'i32 shl 30
    ## Defensive upper bound on ``PgBit.nbits`` (1 G bits = 128 MiB of packed
    ## data) used by the binary bit decoders to refuse pathologically large
    ## lengths from the wire before allocating the data buffer. PostgreSQL's
    ## own ``VARBITMAXLEN`` is effectively ``INT_MAX`` bits, but a 1G-bit
    ## ceiling suffices for any realistic ``bit varying`` column and bounds
    ## DoS risk from a malicious intermediary.

  # PostgreSQL type OIDs for scalar and array types.
  OidBool* = 16'i32
  OidInt2* = 21'i32
  OidInt4* = 23'i32
  OidInt8* = 20'i32
  OidFloat4* = 700'i32
  OidFloat8* = 701'i32
  OidText* = 25'i32
  OidVarchar* = 1043'i32
  OidChar* = 18'i32
  OidName* = 19'i32
  OidBpchar* = 1042'i32
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

  # Range flag bits, as PostgreSQL defines them in `utils/rangetypes.h`. Note
  # the sense of the infinity bits: an absent bound is spelled *positively* on
  # the wire (`LB_INF` set), so "has a lower bound" is the absence of
  # `rangeLbInf`, not the presence of a flag of its own.
  rangeEmpty* = 0x01'u8 ## ``RANGE_EMPTY``: the range is empty.
  rangeLbInc* = 0x02'u8 ## ``RANGE_LB_INC``: lower bound is inclusive.
  rangeUbInc* = 0x04'u8 ## ``RANGE_UB_INC``: upper bound is inclusive.
  rangeLbInf* = 0x08'u8 ## ``RANGE_LB_INF``: lower bound is infinite (absent).
  rangeUbInf* = 0x10'u8 ## ``RANGE_UB_INF``: upper bound is infinite (absent).
  rangeContainEmpty* = 0x80'u8
    ## ``RANGE_CONTAIN_EMPTY``: GiST-internal, never set on a stored value.

  PgInlineBufSize* = 16
    ## Maximum payload size that fits in `PgParamInline.inlineBuf` without a
    ## heap allocation. Values longer than this are stored in `overflow`.

# Safe text-value parsing helpers.
#
# `std/strutils`' throwing `parseInt`/`parseFloat`/`parseBiggestInt`/`parseHexInt`
# raise the standard `ValueError`, which escapes the library's `except PgError`
# contract (see ``pg_errors``). Text-format accessors must route their parsing
# through these wrappers so a malformed value surfaces as a catchable
# `PgTypeError` instead of a raw `ValueError`. This applies to the scalar
# accessors too: the non-throwing `parseInt(s, v)` overload returns 0 only for
# the "no digits" case but still raises a raw `ValueError` on overflow, so
# `getInt`/`getInt16`/`getInt64` wrap the parse in `pgTypeErrorOnValueError`
# (and `getFloat`/`getFloat32` route through `pgParseFloat`). These helpers
# carry the same guarantee to the array/range/bytea/geometric/composite paths.

template pgTypeErrorOnValueError*(context: string, body: untyped): untyped =
  ## Evaluate ``body`` and convert any standard ``ValueError`` it raises into a
  ## `PgTypeError` whose message is ``context`` plus the original detail. Use at
  ## call sites that parse a server text value but need a context-specific
  ## message the generic ``pgParse*`` helpers can't carry (function name,
  ## protocol field, enum type, …). Keeps the ``except PgError`` contract (see
  ## ``pg_errors``) without re-spelling the same try/except at every site.
  try:
    body
  except ValueError:
    raise newException(PgTypeError, context & " (" & getCurrentExceptionMsg() & ")")

proc pgParseInt*(s: string): int =
  ## Parse a text integer, converting `ValueError` (invalid or overflowing) to `PgTypeError`.
  pgTypeErrorOnValueError("invalid integer value"):
    parseInt(s)

proc pgParseInt32*(s: string): int32 {.gcsafe, raises: [CatchableError].} =
  ## Parse a text integer into int32, rejecting non-numeric and out-of-range values.
  ## Plain ``int32(parseInt)`` would silently truncate (wrap) in release builds.
  ## Effect signature lets ``parseRangeText``/``parseMultirangeText`` take it directly.
  let v = pgParseInt(s)
  if v < int(int32.low) or v > int(int32.high):
    raise newException(PgTypeError, "integer value out of int32 range: " & s)
  int32(v)

proc pgParseInt16*(s: string): int16 =
  ## Parse a text integer into int16, rejecting non-numeric and out-of-range values.
  ## Plain ``int16(parseInt)`` would silently truncate (wrap) in release builds.
  let v = pgParseInt(s)
  if v < int(int16.low) or v > int(int16.high):
    raise newException(PgTypeError, "integer value out of int16 range: " & s)
  int16(v)

proc pgParseBiggestInt*(s: string): int64 {.gcsafe, raises: [CatchableError].} =
  ## Parse a text integer into int64, converting `ValueError` to `PgTypeError`.
  ## Return type is spelled int64 (== BiggestInt) so procvar callers such as
  ## ``parseRangeText[int64]`` get an exact match without alias-widening.
  pgTypeErrorOnValueError("invalid integer value"):
    parseBiggestInt(s)

proc oaToString(s: openArray[char]): string =
  ## Materialise an ``openArray[char]`` view into a ``string``. Only used on the
  ## error path of the float parsers, which take a zero-copy view so the scalar
  ## ``getFloat``/``getFloat32`` accessors can parse straight out of the row
  ## buffer without an allocation.
  result = newString(s.len)
  for i in 0 ..< s.len:
    result[i] = s[i]

proc pgParseFloat*(s: openArray[char]): float =
  ## Parse a text float, raising `PgTypeError` on malformed input. Also accepts
  ## PostgreSQL's full-word ``Infinity``/``-Infinity`` spelling, which Nim's
  ## ``parseFloat`` rejects (it only takes the ``inf`` form); ``NaN`` is parsed
  ## natively. This keeps the text path symmetric with the binary path, where
  ## float infinities decode fine. Takes an ``openArray[char]`` so the scalar
  ## accessors can pass a zero-copy view of the row buffer (a ``string`` argument
  ## converts implicitly, so the array/geometric/composite callers are unaffected).
  if s == "Infinity":
    return Inf
  if s == "-Infinity":
    return NegInf
  # Mirror ``strutils.parseFloat``'s strictness (entire input must parse) but on a
  # view, without its throwing `string` overload's allocation/`ValueError`.
  let n = parseutils.parseFloat(s, result)
  if n == 0 or n != s.len:
    raise newException(PgTypeError, "invalid float value: " & oaToString(s))

proc pgParseFloat32*(s: openArray[char]): float32 =
  ## Parse a text float into float32, raising `PgTypeError` on malformed input and
  ## rejecting finite values that overflow float32's range (a plain
  ## ``float32(parseFloat)`` would silently collapse them to ``inf``). Genuine
  ## ``Infinity``/``-Infinity`` inputs (recognised by ``pgParseFloat``) are
  ## preserved.
  let v = pgParseFloat(s)
  result = float32(v)
  if result.classify in {fcInf, fcNegInf} and v.classify notin {fcInf, fcNegInf}:
    raise
      newException(PgTypeError, "float value out of float32 range: " & oaToString(s))

proc pgParseHexInt*(s: string): int =
  ## Parse a hex string, converting `ValueError` to `PgTypeError`.
  ## Used to decode hex-encoded bytea (``\xDEADBEEF``) text pairs.
  pgTypeErrorOnValueError("invalid hex value"):
    parseHexInt(s)

proc parsePgBoolText*(s: string): bool =
  ## Parse a PostgreSQL text-format bool value. PostgreSQL itself always emits
  ## ``t``/``f`` on the wire; the multi-char spellings (``true``/``false``,
  ## ``1``/``0``) are accepted defensively so composite / array / user-supplied
  ## text (e.g. an explicit ``'true'::text`` cast) parses uniformly across the
  ## scalar, array-element, and composite-field decoders. Any other input
  ## raises ``PgTypeError`` — including strings that merely start with an
  ## accepted character (``typo``, ``finalize``), which older byte-level
  ## implementations silently accepted.
  case s
  of "t", "true", "1":
    true
  of "f", "false", "0":
    false
  else:
    raise newException(PgTypeError, "Invalid boolean value: " & s)

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

const MaxMoneyScale* = 18
  ## Largest fractional-digit count a `PgMoney` can carry: `$` scales by
  ## ``10^scale`` in a ``uint64``, which overflows past 19 digits.

proc checkMoneyScale*(scale: int) {.raises: [PgTypeError].} =
  ## Raise unless ``scale`` is within ``0..MaxMoneyScale``.
  if scale < 0 or scale > MaxMoneyScale:
    raise newException(PgTypeError, "PgMoney scale out of range: " & $scale)

const Pow10u64 = block:
  var a: array[0 .. MaxMoneyScale, uint64]
  a[0] = 1'u64
  for i in 1 .. MaxMoneyScale:
    a[i] = a[i - 1] * 10'u64
  a

func pow10u64(n: int): uint64 {.inline.} =
  ## Table lookup, not a loop: ``==``/``<``/``hash`` rescale both operands on
  ## every comparison.
  Pow10u64[n]

proc initPgMoney*(amount: int64, scale: int = 2): PgMoney =
  ## Construct a PgMoney. ``amount`` is the raw integer in the minor currency
  ## unit; ``scale`` is the number of fractional digits (default 2).
  checkMoneyScale(scale)
  PgMoney(amountRaw: amount, scaleRaw: int8(scale))

func amount*(m: PgMoney): int64 {.inline.} =
  ## Raw amount in the minor currency unit. Set only through `initPgMoney`.
  m.amountRaw

func scale*(m: PgMoney): int8 {.inline.} =
  ## Fractional digits (``0..18`` via `initPgMoney`).
  m.scaleRaw

func pgMoneyParts(m: PgMoney): (int64, int64) {.inline.} =
  ## Decimal value as (floor of the whole part, fraction rescaled to
  ## ``MaxMoneyScale``), so different scales compare directly. Rescaling cannot
  ## overflow: the fraction is below ``10^scale``.
  let d = int64(pow10u64(int(m.scaleRaw)))
  var w = m.amountRaw div d
  var f = m.amountRaw mod d
  if f < 0:
    dec w
    f += d
  (w, f * int64(pow10u64(MaxMoneyScale - int(m.scaleRaw))))

proc `==`*(a, b: PgMoney): bool =
  ## Compare decimal values, not representations: ``1.00`` at scale 2 equals
  ## ``1.000`` at scale 3. Consistent with ``<`` and ``hash``.
  pgMoneyParts(a) == pgMoneyParts(b)

proc `<`*(a, b: PgMoney): bool =
  ## Order by decimal value across scales. See ``==``.
  pgMoneyParts(a) < pgMoneyParts(b)

proc `<=`*(a, b: PgMoney): bool =
  ## Order by decimal value across scales. See ``==``.
  pgMoneyParts(a) <= pgMoneyParts(b)

proc hash*(v: PgMoney): Hash =
  ## Hashes the decimal value, so equal values hash equally across scales.
  let (w, f) = pgMoneyParts(v)
  var h: Hash = 0
  h = h !& hash(w)
  h = h !& hash(f)
  !$h

proc `$`*(v: PgMoney): string =
  ## Format PgMoney as a plain decimal number with ``scale`` fractional digits
  ## and no symbol or separators: ``initPgMoney(123456, 2)`` -> ``"1234.56"``.
  ## Use ``formatPgMoney`` for currency symbols and thousand separators.
  let c = v.amountRaw
  let scale = int(v.scaleRaw)
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

func isPgMoneyDigit(ch: char): bool {.inline.} =
  ch >= '0' and ch <= '9'

func isPgMoneyDigitOrSign(ch: char): bool {.inline.} =
  isPgMoneyDigit(ch) or ch == '-' or ch == '+'

func pgMoneyGroupSepLen(s: string, i: int): int {.inline.} =
  ## Byte length of the group separator at ``s[i]``, or 0. Covers what glibc
  ## puts in ``mon_thousands_sep``: ASCII space and apostrophe, U+00A0,
  ## U+2007..U+2009, U+202F, U+2019.
  let ch = s[i]
  if ch == ' ' or ch == '\'':
    return 1
  if ch == '\xC2' and i + 1 < s.len and s[i + 1] == '\xA0':
    return 2 # U+00A0
  if ch == '\xE2' and i + 2 < s.len and s[i + 1] == '\x80':
    case s[i + 2]
    of '\x87', '\x88', '\x89', '\xAF', '\x99':
      # U+2007, U+2008, U+2009, U+202F, U+2019
      return 3
    else:
      discard
  0

func pgMoneySpaceLen(s: string, i: int): int {.inline.} =
  ## Byte length of one ``sep_by_space`` space at ``s[i]``, or 0 — the space
  ## between symbol and digits, not a group separator.
  if s[i] == ' ':
    1
  elif s[i] == '\xC2' and i + 1 < s.len and s[i + 1] == '\xA0':
    2
  else:
    0

func pgMoneyTrimTrailingSpace(s: string): (string, bool) =
  ## ``s`` without its trailing ``sep_by_space`` run, plus whether one was
  ## there.
  var e = s.len
  while e > 0:
    if s[e - 1] == ' ':
      dec e
    elif e >= 2 and s[e - 2] == '\xC2' and s[e - 1] == '\xA0':
      e -= 2
    else:
      break
  (s[0 ..< e], e != s.len)

func pgMoneyTrimLeadingSpace(s: string): (string, bool) =
  ## Leading counterpart of `pgMoneyTrimTrailingSpace`.
  var b = 0
  while b < s.len:
    let n = pgMoneySpaceLen(s, b)
    if n == 0:
      break
    b += n
  (s[b ..^ 1], b != 0)

proc initPgMoneyConventions*(
    symbol: string = "",
    decimalSep: char = '.',
    thousandsSep: string = "",
    symbolBefore: bool = true,
    accountingParens: bool = false,
    sepBySpace: bool = false,
    fracDigits: int = 2,
    groupDigits: int = 3,
): PgMoneyConventions {.raises: [PgTypeError].} =
  ## Validate and package the conventions of one ``lc_monetary`` locale,
  ## ``fracDigits`` (``frac_digits``) included. Separators and symbol may not
  ## contain digits, signs or parentheses and the two separators may not
  ## collide: such conventions render text that cannot be read back.
  ## Space between symbol and digits is ``sepBySpace``, never part of
  ## ``symbol``, which parsing's whitespace trimming would eat.
  ## ``groupDigits`` is the locale's ``mon_grouping`` (``cash_out`` groups by
  ## its first element, so ``cmn_TW`` writes ``NT$1,2345,6789.00``).
  checkMoneyScale(fracDigits)
  if groupDigits < 1 or groupDigits > 6:
    raise
      newException(PgTypeError, "Money group size out of range (1..6): " & $groupDigits)
  if decimalSep == '\0':
    # NUL is what a zero-initialized value carries; see `checkPgMoneyConventions`.
    raise newException(PgTypeError, "Money decimal separator may not be NUL")
  if isPgMoneyDigitOrSign(decimalSep) or decimalSep == '(' or decimalSep == ')' or
      decimalSep == ' ':
    raise newException(PgTypeError, "Invalid money decimal separator: " & $decimalSep)
  for ch in thousandsSep:
    if isPgMoneyDigitOrSign(ch) or ch == '(' or ch == ')':
      raise
        newException(PgTypeError, "Invalid money thousands separator: " & thousandsSep)
  if fracDigits > 0 and decimalSep in thousandsSep:
    raise newException(
      PgTypeError, "Money thousands separator contains the decimal separator"
    )
  for ch in symbol:
    if isPgMoneyDigitOrSign(ch) or ch == '(' or ch == ')':
      raise newException(PgTypeError, "Invalid money currency symbol: " & symbol)
  if symbol.len == 0:
    if sepBySpace:
      raise newException(
        PgTypeError, "Money sepBySpace needs a currency symbol to separate"
      )
  elif symbol[0] in Whitespace or symbol[^1] in Whitespace or
      pgMoneyTrimLeadingSpace(symbol)[1] or pgMoneyTrimTrailingSpace(symbol)[1]:
    # Parsing strips all whitespace, not just the `sep_by_space` class.
    raise newException(
      PgTypeError,
      "Money currency symbol has outer whitespace: " & symbol & " (use sepBySpace)",
    )
  PgMoneyConventions(
    symbolRaw: symbol,
    decimalSepRaw: decimalSep,
    thousandsSepRaw: thousandsSep,
    symbolBeforeRaw: symbolBefore,
    sepBySpaceRaw: sepBySpace,
    accountingParensRaw: accountingParens,
    fracDigitsRaw: int8(fracDigits),
    groupDigitsRaw: int8(groupDigits),
  )

func symbol*(c: PgMoneyConventions): string {.inline.} =
  ## Currency symbol without surrounding space (see ``sepBySpace``).
  c.symbolRaw

func decimalSep*(c: PgMoneyConventions): char {.inline.} =
  c.decimalSepRaw

func thousandsSep*(c: PgMoneyConventions): string {.inline.} =
  ## Group separator; ``""`` disables grouping. A string because real
  ## ``mon_thousands_sep`` values are multi-byte.
  c.thousandsSepRaw

func symbolBefore*(c: PgMoneyConventions): bool {.inline.} =
  c.symbolBeforeRaw

func sepBySpace*(c: PgMoneyConventions): bool {.inline.} =
  ## A space stands between the symbol and the digits.
  c.sepBySpaceRaw

func accountingParens*(c: PgMoneyConventions): bool {.inline.} =
  ## Negatives render and parse as ``(1.00)``.
  c.accountingParensRaw

func fracDigits*(c: PgMoneyConventions): int =
  ## ``frac_digits`` — the ``scale`` a `PgMoney` must carry under these
  ## conventions.
  int(c.fracDigitsRaw)

func groupDigits*(c: PgMoneyConventions): int =
  ## Digits per group between ``thousandsSep`` separators (``mon_grouping``).
  int(c.groupDigitsRaw)

func checkPgMoneyConventions*(c: PgMoneyConventions) {.raises: [PgTypeError].} =
  ## Reject a value that never went through `initPgMoneyConventions`: its
  ## ``fracDigits`` is 0, reading a two-decimal amount 100 times too large.
  if c.decimalSepRaw == '\0':
    raise newException(
      PgTypeError, "Uninitialized PgMoneyConventions (use initPgMoneyConventions)"
    )

proc formatPgMoney*(
    v: PgMoney, conv: PgMoneyConventions
): string {.raises: [PgTypeError].} =
  ## Render with explicit locale conventions; the inverse of
  ## ``parsePgMoney(s, conv)``. ``v.scale`` must equal ``conv.fracDigits``,
  ## since a mismatch would render a wrong amount in silence.
  checkPgMoneyConventions(conv)
  if int(v.scaleRaw) != int(conv.fracDigitsRaw):
    raise newException(
      PgTypeError,
      "PgMoney.scale=" & $v.scaleRaw & " does not match conventions fracDigits=" &
        $conv.fracDigitsRaw,
    )
  let c = v.amountRaw
  let scale = int(v.scaleRaw)
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
  let groupLen = int(conv.groupDigitsRaw)
  if conv.thousandsSepRaw.len > 0 and wholeStr.len > groupLen:
    var grouped = newStringOfCap(
      wholeStr.len + wholeStr.len div groupLen * conv.thousandsSepRaw.len
    )
    let firstLen = wholeStr.len mod groupLen
    var idx = 0
    if firstLen > 0:
      grouped.add(wholeStr[0 ..< firstLen])
      idx = firstLen
    while idx < wholeStr.len:
      if grouped.len > 0:
        grouped.add(conv.thousandsSepRaw)
      grouped.add(wholeStr[idx ..< idx + groupLen])
      idx += groupLen
    wholeStr = grouped
  result = newStringOfCap(conv.symbolRaw.len + wholeStr.len + scale + 5)
  if neg and conv.accountingParensRaw:
    result.add('(')
  elif neg:
    result.add('-')
  if conv.symbolBeforeRaw and conv.symbolRaw.len > 0:
    result.add(conv.symbolRaw)
    if conv.sepBySpaceRaw:
      result.add(' ')
  result.add(wholeStr)
  if scale > 0:
    result.add(conv.decimalSepRaw)
    let fracStr = $frac
    for _ in 0 ..< (scale - fracStr.len):
      result.add('0')
    result.add(fracStr)
  if not conv.symbolBeforeRaw and conv.symbolRaw.len > 0:
    if conv.sepBySpaceRaw:
      result.add(' ')
    result.add(conv.symbolRaw)
  if neg and conv.accountingParensRaw:
    result.add(')')

proc formatPgMoney*(
    v: PgMoney,
    symbol: string = "",
    decimalSep: char = '.',
    thousandsSep: string = "",
    symbolBefore: bool = true,
    accountingParens: bool = false,
    sepBySpace: bool = false,
    groupDigits: int = 3,
): string {.raises: [PgTypeError].} =
  ## Locale-aware money formatter taking the conventions apart instead of as a
  ## `PgMoneyConventions`; ``fracDigits`` is ``v``'s own ``scale``, so the two
  ## cannot disagree. ``thousandsSep`` of ``""`` disables grouping. Example:
  ## ``formatPgMoney(initPgMoney(123456), symbol = "€", decimalSep = ',',
  ## thousandsSep = ".", symbolBefore = false, sepBySpace = true)`` ->
  ## ``"1.234,56 €"``.
  ##
  ## A space folded into ``symbol`` (``" €"``) is unfolded into ``sepBySpace``
  ## here rather than rejected, that spelling being all the API offered before
  ## ``sepBySpace`` existed.
  var sym = symbol
  var sep = sepBySpace
  if sym.len > 0:
    let (facing, hadSpace) =
      if symbolBefore:
        pgMoneyTrimTrailingSpace(sym)
      else:
        pgMoneyTrimLeadingSpace(sym)
    sep = sep or hadSpace
    sym =
      if symbolBefore:
        pgMoneyTrimLeadingSpace(facing)[0].strip()
      else:
        pgMoneyTrimTrailingSpace(facing)[0].strip()
  formatPgMoney(
    v,
    initPgMoneyConventions(
      sym,
      decimalSep,
      thousandsSep,
      symbolBefore,
      accountingParens,
      sep,
      int(v.scale),
      groupDigits,
    ),
  )

proc pgMoneyFromDigits(
    whole, frac: string, neg: bool, scale: int, src: string
): PgMoney {.raises: [PgTypeError].} =
  ## Combine validated digit runs into the minor-unit amount. The magnitude is
  ## accumulated in ``uint64``: ``abs(int64.low)`` does not fit ``int64``.
  const magMax = uint64(high(int64))
  var mag: uint64 = 0
  for ch in whole & frac:
    let d = uint64(ord(ch) - ord('0'))
    if mag > (high(uint64) - d) div 10'u64:
      raise newException(PgTypeError, "Money value out of range: " & src)
    mag = mag * 10 + d
  if neg:
    if mag == magMax + 1'u64:
      return initPgMoney(low(int64), scale)
    if mag > magMax:
      raise newException(PgTypeError, "Money value out of range: " & src)
    return initPgMoney(-int64(mag), scale)
  if mag > magMax:
    raise newException(PgTypeError, "Money value out of range: " & src)
  initPgMoney(int64(mag), scale)

proc parsePgMoney*(s: string, conv: PgMoneyConventions): PgMoney =
  ## Parse against known conventions — the inverse of ``formatPgMoney``.
  ## Nothing is guessed: symbol, decimal separator, ``conv.fracDigits``
  ## fraction digits and ``conv.groupDigits``-wide groups (the first may be
  ## shorter) must all match, with nothing left over. A sign may sit before or
  ## after a prefix symbol; parenthesized negatives need ``accountingParens``.
  ## Raises ``PgTypeError`` on anything else.
  checkPgMoneyConventions(conv)
  var t = s.strip()
  if t.len == 0:
    raise newException(PgTypeError, "Empty money string")
  var neg = false
  if conv.accountingParensRaw and t.len >= 2 and t[0] == '(' and t[^1] == ')':
    neg = true
    t = t[1 ..^ 2].strip()
  var i = 0
  var signSeen = neg
  template takeSign() =
    if i < t.len and (t[i] == '-' or t[i] == '+'):
      if signSeen:
        raise newException(PgTypeError, "Invalid money format: " & s)
      signSeen = true
      if t[i] == '-':
        neg = true
      inc i

  template takeSpace() =
    let n =
      if i < t.len:
        pgMoneySpaceLen(t, i)
      else:
        0
    if n == 0:
      raise newException(PgTypeError, "Invalid money format: " & s)
    i += n

  takeSign()
  if conv.symbolRaw.len > 0 and conv.symbolBeforeRaw:
    if not t.continuesWith(conv.symbolRaw, i):
      raise newException(PgTypeError, "Invalid money format: " & s)
    i += conv.symbolRaw.len
    if conv.sepBySpaceRaw:
      takeSpace()
    # ``$-1.00``: some locales place the sign between symbol and digits.
    takeSign()
  # Integer part. The first group holds 1..groupSize digits and every later one
  # exactly groupSize. A separator counts only between digits, so one that is
  # also a space cannot swallow the space before a trailing symbol.
  let groupSize = int(conv.groupDigitsRaw)
  var digits = newStringOfCap(t.len - i)
  var groupLen = 0
  var seenSep = false
  while i < t.len:
    let ch = t[i]
    if isPgMoneyDigit(ch):
      digits.add(ch)
      inc groupLen
      inc i
    elif conv.thousandsSepRaw.len > 0 and t.continuesWith(conv.thousandsSepRaw, i) and
        i + conv.thousandsSepRaw.len < t.len and
        isPgMoneyDigit(t[i + conv.thousandsSepRaw.len]):
      if groupLen == 0 or groupLen > groupSize or (seenSep and groupLen != groupSize):
        raise newException(PgTypeError, "Invalid money format: " & s)
      seenSep = true
      groupLen = 0
      i += conv.thousandsSepRaw.len
    else:
      break
  if digits.len == 0 or (seenSep and groupLen != groupSize):
    raise newException(PgTypeError, "Invalid money format: " & s)
  var fracPart = ""
  let fracDigits = int(conv.fracDigitsRaw)
  if fracDigits > 0:
    if i >= t.len or t[i] != conv.decimalSepRaw:
      raise newException(PgTypeError, "Invalid money format: " & s)
    inc i
    for _ in 0 ..< fracDigits:
      if i >= t.len or not isPgMoneyDigit(t[i]):
        raise newException(PgTypeError, "Invalid money format: " & s)
      fracPart.add(t[i])
      inc i
  if conv.symbolRaw.len > 0 and not conv.symbolBeforeRaw:
    if conv.sepBySpaceRaw:
      takeSpace()
    if not t.continuesWith(conv.symbolRaw, i):
      raise newException(PgTypeError, "Invalid money format: " & s)
    i += conv.symbolRaw.len
  if i != t.len:
    raise newException(PgTypeError, "Invalid money format: " & s)
  pgMoneyFromDigits(digits, fracPart, neg, fracDigits, s)

proc inferPgMoneyConventions*(s: string, fracDigits: int = 2): PgMoneyConventions =
  ## Read the locale conventions back out of one formatted money string, for
  ## callers who do not know the server's ``lc_monetary``; feeding the result
  ## to ``parsePgMoney(s, conv)`` is what the inferring overload does.
  ##
  ## The currency symbol is whatever non-digit run sits on one side — real
  ## symbols contain letters and ``.`` (``kr.``, ``Bs.``, ``د.م.``), so
  ## ``"1.00 kr and more"`` yields the symbol ``"kr and more"``; runs on both
  ## sides are rejected. The decimal separator is the last ``.``/``,``, every
  ## other separator must be one and the same group separator, and the second
  ## group gives the group size. Anything else between the digits raises.
  ##
  ## Only the sign ``s`` carries is described: ``accountingParens``,
  ## ``symbolBefore`` and ``sepBySpace`` come from this one sample, while a
  ## locale may place the symbol and the negative sign differently
  ## (``n_sign_posn``, ``n_cs_precedes``, ``n_sep_by_space``). Conventions
  ## inferred from a positive sample therefore need not accept the negative
  ## values of the same locale: infer from a negative sample, or set the sign
  ## conventions yourself, before reusing the result across a result set.
  checkMoneyScale(fracDigits)
  var t = s.strip()
  if t.len == 0:
    raise newException(PgTypeError, "Empty money string")
  var parens = false
  if t.len >= 2 and t[0] == '(' and t[^1] == ')':
    parens = true
    t = t[1 ..^ 2].strip()
  var first = -1
  var last = -1
  for k in 0 ..< t.len:
    if isPgMoneyDigit(t[k]):
      if first < 0:
        first = k
      last = k
  if first < 0:
    raise newException(PgTypeError, "Invalid money format: " & s)
  # Prefix: at most one sign, anywhere among the symbol bytes.
  var pre = ""
  var signCount = 0
  for ch in t[0 ..< first]:
    if ch == '-' or ch == '+':
      inc signCount
    else:
      pre.add(ch)
  if signCount > 1 or (parens and signCount > 0):
    raise newException(PgTypeError, "Invalid money format: " & s)
  let suf = t[last + 1 ..^ 1]
  for ch in suf:
    if ch == '-' or ch == '+':
      raise newException(PgTypeError, "Invalid money format: " & s)
  # The space adjacent to the digits is ``sepBySpace``; the rest is the symbol,
  # which may sit on one side only.
  let (preInner, preSpace) = pgMoneyTrimTrailingSpace(pre)
  let (sufInner, sufSpace) = pgMoneyTrimLeadingSpace(suf)
  let preSym = pgMoneyTrimLeadingSpace(preInner)[0]
  let sufSym = pgMoneyTrimTrailingSpace(sufInner)[0]
  if preSym.len > 0 and sufSym.len > 0:
    raise newException(PgTypeError, "Invalid money format: " & s)
  var symbol = ""
  var symbolBefore = true
  var sepBySpace = false
  if sufSym.len > 0:
    symbol = sufSym
    symbolBefore = false
    sepBySpace = sufSpace
  elif preSym.len > 0:
    symbol = preSym
    sepBySpace = preSpace
  # Separators between the digits, in order, with the digit run before each.
  var seps: seq[string] = @[]
  var runs: seq[int] = @[]
  var run = 0
  var k = first
  while k <= last:
    if isPgMoneyDigit(t[k]):
      inc run
      inc k
      continue
    if t[k] == '.' or t[k] == ',':
      seps.add($t[k])
      runs.add(run)
      run = 0
      inc k
      continue
    let n = pgMoneyGroupSepLen(t, k)
    if n == 0 or k + n > last:
      raise newException(PgTypeError, "Invalid money format: " & s)
    seps.add(t[k ..< k + n])
    runs.add(run)
    run = 0
    k += n
  runs.add(run)
  var decimalSep = '.'
  var thousandsSep = ""
  # The second integer group gives ``mon_grouping``; the first is a remainder.
  # Unused when no separator was seen.
  var groupDigits = 3
  template inferGroupDigits(n: int, allowFour: bool) =
    # Real locales only carry 3 or 4; taking any size at face value would read
    # "1 23" as 123 instead of rejecting a short group. Without fraction digits
    # no locale carries 4, so accepting it there would silently read a 4-decimal
    # "1.2345" as 12345.
    if n != 3 and not (allowFour and n == 4):
      raise newException(PgTypeError, "Invalid money format: " & s)
    groupDigits = n

  if fracDigits > 0:
    var di = -1
    for idx in countdown(seps.high, 0):
      if seps[idx] == "." or seps[idx] == ",":
        di = idx
        break
    # The decimal separator must come last: "1.2 3" is not a number.
    if di < 0 or di != seps.high:
      raise newException(PgTypeError, "Invalid money format: " & s)
    decimalSep = seps[di][0]
    for idx in 0 ..< di:
      if thousandsSep.len == 0:
        thousandsSep = seps[idx]
      elif thousandsSep != seps[idx]:
        raise newException(PgTypeError, "Invalid money format: " & s)
    if di >= 1:
      inferGroupDigits(runs[1], allowFour = true)
  else:
    for sp in seps:
      if thousandsSep.len == 0:
        thousandsSep = sp
      elif thousandsSep != sp:
        raise newException(PgTypeError, "Invalid money format: " & s)
    if seps.len >= 1:
      inferGroupDigits(runs[1], allowFour = false)
    if thousandsSep == ".":
      decimalSep = ','
  try:
    initPgMoneyConventions(
      symbol = symbol,
      decimalSep = decimalSep,
      thousandsSep = thousandsSep,
      symbolBefore = symbolBefore,
      accountingParens = parens,
      sepBySpace = sepBySpace,
      fracDigits = fracDigits,
      groupDigits = groupDigits,
    )
  except PgTypeError:
    # Construction messages name conventions the caller never supplied.
    raise newException(PgTypeError, "Invalid money format: " & s)

proc parsePgMoney*(s: string, scale: int = 2): PgMoney =
  ## Parse a money string whose locale conventions are unknown: infer them
  ## with `inferPgMoneyConventions`, then parse against them.
  ##
  ## Inference cannot tell a currency symbol from trailing junk, so
  ## ``"1.00 kr and more"`` parses as ``1.00``; pass a `PgMoneyConventions` to
  ## reject that. ``frac_digits`` is not in the text either — pass ``scale``
  ## when the server runs a non-default ``lc_monetary``.
  ## Raises ``PgTypeError`` on malformed input.
  parsePgMoney(s, inferPgMoneyConventions(s, scale))

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

func nbits*(v: PgBit): int32 {.inline.} =
  ## Number of bits. Set only through `initPgBit` / `parseBitString`.
  v.nbitsRaw

func data*(v: PgBit): lent seq[byte] {.inline.} =
  ## Packed bit data (MSB first), ``ceil(nbits/8)`` bytes.
  v.dataRaw

proc initPgBit*(nbits: int32, data: sink seq[byte]): PgBit =
  ## Construct a PgBit. Raises ``PgTypeError`` unless ``data`` holds exactly
  ## ``ceil(nbits/8)`` bytes for an ``nbits`` in ``0..PgBitMaxBits`` — the bound
  ## the encoder applies, applied here so an invalid value cannot be built.
  if nbits < 0:
    raise newException(PgTypeError, "Invalid PgBit: negative nbits " & $nbits)
  if nbits > PgBitMaxBits:
    raise newException(
      PgTypeError,
      "Invalid PgBit: nbits " & $nbits & " exceeds limit (" & $PgBitMaxBits & ")",
    )
  if (int64(nbits) + 7) div 8 != int64(data.len):
    raise newException(
      PgTypeError,
      "Invalid PgBit: nbits=" & $nbits & " inconsistent with data.len=" & $data.len,
    )
  PgBit(nbitsRaw: nbits, dataRaw: data)

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
  # Bound before narrowing to int32, then hand the result to `initPgBit` so the
  # invariant has exactly one owner.
  if s.len > PgBitMaxBits:
    raise newException(
      PgTypeError,
      "Invalid PgBit: nbits " & $s.len & " exceeds limit (" & $PgBitMaxBits & ")",
    )
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
  initPgBit(nbits, data)

proc parsePgNumeric*(s: string): PgNumeric {.gcsafe, raises: [CatchableError].} =
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
  # ``intPadded``/``fracPadded`` contain only digits (validated above and zero-padded),
  # so each 4-char group is always in 0..9999. Plain ``parseInt`` cannot raise here —
  # there is no ``ValueError`` to convert — so skip ``pgParseInt``'s redundant guard.
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
  let us = v.microseconds
  let neg = us < 0
  # Avoid overflow on int64.low by working in uint64 for the magnitude.
  let mag =
    if neg:
      uint64(not us) + 1'u64
    else:
      uint64(us)
  let hours = mag div 3_600_000_000'u64
  var r = mag mod 3_600_000_000'u64
  let mins = r div 60_000_000'u64
  r = r mod 60_000_000'u64
  let secs = r div 1_000_000'u64
  let frac = r mod 1_000_000'u64
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

# Big-endian integer/float encode/decode helpers live in ``pg_bytes`` (the
# dependency-free module shared with ``pg_protocol``) and are re-exported above
# via ``export pg_bytes``: ``toBE16/32/64``, ``fromBE16/32/64``,
# ``decodeFloat32BE``/``decodeFloat64BE``.
