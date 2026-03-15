import std/[json, options, strutils, tables, times]

import pg_protocol

const
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

type
  PgUuid* = distinct string

  PgNumeric* = distinct string
    ## Arbitrary-precision numeric value stored as its string representation.
    ## Use this instead of float64 to avoid precision loss with PostgreSQL numeric/decimal.

  PgParam* = object
    oid*: int32
    format*: int16 # 0=text, 1=binary
    value*: Option[seq[byte]]

proc `$`*(v: PgNumeric): string {.borrow.}
proc `==`*(a, b: PgNumeric): bool {.borrow.}

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
  int16(data[0]) shl 8 or int16(data[1])

proc fromBE32*(data: openArray[byte]): int32 =
  int32(data[0]) shl 24 or int32(data[1]) shl 16 or int32(data[2]) shl 8 or
    int32(data[3])

proc fromBE64*(data: openArray[byte]): int64 =
  int64(data[0]) shl 56 or int64(data[1]) shl 48 or int64(data[2]) shl 40 or
    int64(data[3]) shl 32 or int64(data[4]) shl 24 or int64(data[5]) shl 16 or
    int64(data[6]) shl 8 or int64(data[7])

proc toPgParam*(v: string): PgParam =
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
  PgParam(oid: OidNumeric, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: JsonNode): PgParam =
  PgParam(oid: OidJsonb, format: 0, value: some(toBytes($v)))

proc encodeBinaryArray*(elemOid: int32, elements: seq[seq[byte]]): seq[byte] =
  ## Encode a 1-dimensional PostgreSQL binary array.
  ## Header: ndim(4) + has_null(4) + elem_oid(4) + dim_len(4) + lower_bound(4) = 20 bytes
  ## Each element: len(4) + data
  let headerSize = 20
  var dataSize = 0
  for e in elements:
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

proc toPgBinaryParam*(v: string): PgParam =
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

proc toPgBinaryParam*(v: PgNumeric): PgParam =
  ## Sends numeric as text format (binary numeric encoding is complex).
  PgParam(oid: OidNumeric, format: 0, value: some(toBytes(string(v))))

proc toPgBinaryParam*(v: PgUuid): PgParam =
  let hex = string(v).replace("-", "")
  var bytes = newSeq[byte](16)
  for i in 0 ..< 16:
    bytes[i] = byte(parseHexInt(hex[i * 2 .. i * 2 + 1]))
  PgParam(oid: OidUuid, format: 1, value: some(bytes))

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

type PgTypeError* = object of CatchableError

# Row/RowData types are defined in pg_protocol and re-exported here.

proc cellInfo(row: Row, col: int): tuple[off: int, len: int] {.inline.} =
  let idx = (int(row.rowIdx) * int(row.data.numCols) + col) * 2
  result.off = int(row.data.cellIndex[idx])
  result.len = int(row.data.cellIndex[idx + 1])

proc len*(row: Row): int {.inline.} =
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
  ## Backward-compatible converter: build a Row from seq[Option[seq[byte]]].
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

proc affectedRows*(tag: string): int64 =
  ## Extract row count from command tag (e.g. "UPDATE 3" -> 3, "INSERT 0 1" -> 1).
  let parts = tag.split(' ')
  if parts.len > 0:
    try:
      return parseBiggestInt(parts[^1])
    except ValueError:
      return 0
  return 0

proc isNull*(row: Row, col: int): bool =
  let idx = (int(row.rowIdx) * int(row.data.numCols) + col) * 2
  row.data.cellIndex[idx + 1] == -1'i32

proc getStr*(row: Row, col: int): string =
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  result = newString(clen)
  if clen > 0:
    copyMem(addr result[0], unsafeAddr row.data.buf[off], clen)

proc getInt*(row: Row, col: int): int32 =
  let s = row.getStr(col)
  result = int32(parseInt(s))

proc getInt64*(row: Row, col: int): int64 =
  let s = row.getStr(col)
  result = parseBiggestInt(s)

proc getFloat*(row: Row, col: int): float64 =
  let s = row.getStr(col)
  result = parseFloat(s)

proc getNumeric*(row: Row, col: int): PgNumeric =
  PgNumeric(row.getStr(col))

proc getBool*(row: Row, col: int): bool =
  let s = row.getStr(col)
  case s
  of "t", "true", "1":
    true
  of "f", "false", "0":
    false
  else:
    raise newException(PgTypeError, "Invalid boolean value: " & s)

proc getBytes*(row: Row, col: int): seq[byte] =
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  # PostgreSQL text-format bytea uses hex encoding: \xDEADBEEF
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
  let s = row.getStr(col)
  try:
    return parse(s, "yyyy-MM-dd")
  except TimeParseError:
    raise newException(PgTypeError, "Invalid date: " & s)

proc getJson*(row: Row, col: int): JsonNode =
  let s = row.getStr(col)
  try:
    return parseJson(s)
  except JsonParsingError:
    raise newException(PgTypeError, "Invalid JSON: " & s)

# NULL-safe Option accessors (text format)

proc getStrOpt*(row: Row, col: int): Option[string] =
  if row.isNull(col):
    none(string)
  else:
    some(row.getStr(col))

proc getIntOpt*(row: Row, col: int): Option[int32] =
  if row.isNull(col):
    none(int32)
  else:
    some(row.getInt(col))

proc getInt64Opt*(row: Row, col: int): Option[int64] =
  if row.isNull(col):
    none(int64)
  else:
    some(row.getInt64(col))

proc getFloatOpt*(row: Row, col: int): Option[float64] =
  if row.isNull(col):
    none(float64)
  else:
    some(row.getFloat(col))

proc getNumericOpt*(row: Row, col: int): Option[PgNumeric] =
  if row.isNull(col):
    none(PgNumeric)
  else:
    some(row.getNumeric(col))

proc getBoolOpt*(row: Row, col: int): Option[bool] =
  if row.isNull(col):
    none(bool)
  else:
    some(row.getBool(col))

proc getJsonOpt*(row: Row, col: int): Option[JsonNode] =
  if row.isNull(col):
    none(JsonNode)
  else:
    some(row.getJson(col))

# Array text format parser

proc parseTextArray*(s: string): seq[Option[string]] =
  ## Parse PostgreSQL text-format array literal: {elem1,elem2,...}
  ## Returns elements as Option[string] (none for NULL).
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
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in int array")
    result.add(int32(parseInt(e.get)))

proc getInt16Array*(row: Row, col: int): seq[int16] =
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in int16 array")
    result.add(int16(parseInt(e.get)))

proc getInt64Array*(row: Row, col: int): seq[int64] =
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in int64 array")
    result.add(parseBiggestInt(e.get))

proc getFloatArray*(row: Row, col: int): seq[float64] =
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in float array")
    result.add(parseFloat(e.get))

proc getFloat32Array*(row: Row, col: int): seq[float32] =
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in float32 array")
    result.add(float32(parseFloat(e.get)))

proc getBoolArray*(row: Row, col: int): seq[bool] =
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
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in string array")
    result.add(e.get)

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

proc getIntArray*(row: Row, col: int, fields: seq[FieldDescription]): seq[int32] =
  if fields[col].formatCode == 0:
    return row.getIntArray(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
  result = newSeq[int32](decoded.elements.len)
  for i, e in decoded.elements:
    if e.len == -1:
      raise newException(PgTypeError, "NULL element in int array")
    result[i] = fromBE32(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1))

proc getInt16Array*(row: Row, col: int, fields: seq[FieldDescription]): seq[int16] =
  if fields[col].formatCode == 0:
    return row.getInt16Array(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
  result = newSeq[int16](decoded.elements.len)
  for i, e in decoded.elements:
    if e.len == -1:
      raise newException(PgTypeError, "NULL element in int16 array")
    result[i] = fromBE16(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1))

proc getInt64Array*(row: Row, col: int, fields: seq[FieldDescription]): seq[int64] =
  if fields[col].formatCode == 0:
    return row.getInt64Array(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
  result = newSeq[int64](decoded.elements.len)
  for i, e in decoded.elements:
    if e.len == -1:
      raise newException(PgTypeError, "NULL element in int64 array")
    result[i] = fromBE64(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1))

proc getFloatArray*(row: Row, col: int, fields: seq[FieldDescription]): seq[float64] =
  if fields[col].formatCode == 0:
    return row.getFloatArray(col)
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

proc getFloat32Array*(row: Row, col: int, fields: seq[FieldDescription]): seq[float32] =
  if fields[col].formatCode == 0:
    return row.getFloat32Array(col)
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

proc getBoolArray*(row: Row, col: int, fields: seq[FieldDescription]): seq[bool] =
  if fields[col].formatCode == 0:
    return row.getBoolArray(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
  result = newSeq[bool](decoded.elements.len)
  for i, e in decoded.elements:
    if e.len == -1:
      raise newException(PgTypeError, "NULL element in bool array")
    result[i] = row.data.buf[off + e.off] == 1'u8

proc getStrArray*(row: Row, col: int, fields: seq[FieldDescription]): seq[string] =
  if fields[col].formatCode == 0:
    return row.getStrArray(col)
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

# Format-aware array Opt accessors

proc getIntArrayOpt*(
    row: Row, col: int, fields: seq[FieldDescription]
): Option[seq[int32]] =
  if row.isNull(col):
    none(seq[int32])
  else:
    some(row.getIntArray(col, fields))

proc getInt16ArrayOpt*(
    row: Row, col: int, fields: seq[FieldDescription]
): Option[seq[int16]] =
  if row.isNull(col):
    none(seq[int16])
  else:
    some(row.getInt16Array(col, fields))

proc getInt64ArrayOpt*(
    row: Row, col: int, fields: seq[FieldDescription]
): Option[seq[int64]] =
  if row.isNull(col):
    none(seq[int64])
  else:
    some(row.getInt64Array(col, fields))

proc getFloatArrayOpt*(
    row: Row, col: int, fields: seq[FieldDescription]
): Option[seq[float64]] =
  if row.isNull(col):
    none(seq[float64])
  else:
    some(row.getFloatArray(col, fields))

proc getFloat32ArrayOpt*(
    row: Row, col: int, fields: seq[FieldDescription]
): Option[seq[float32]] =
  if row.isNull(col):
    none(seq[float32])
  else:
    some(row.getFloat32Array(col, fields))

proc getBoolArrayOpt*(
    row: Row, col: int, fields: seq[FieldDescription]
): Option[seq[bool]] =
  if row.isNull(col):
    none(seq[bool])
  else:
    some(row.getBoolArray(col, fields))

proc getStrArrayOpt*(
    row: Row, col: int, fields: seq[FieldDescription]
): Option[seq[string]] =
  if row.isNull(col):
    none(seq[string])
  else:
    some(row.getStrArray(col, fields))

# Array Opt accessors (text format)

proc getIntArrayOpt*(row: Row, col: int): Option[seq[int32]] =
  if row.isNull(col):
    none(seq[int32])
  else:
    some(row.getIntArray(col))

proc getInt16ArrayOpt*(row: Row, col: int): Option[seq[int16]] =
  if row.isNull(col):
    none(seq[int16])
  else:
    some(row.getInt16Array(col))

proc getInt64ArrayOpt*(row: Row, col: int): Option[seq[int64]] =
  if row.isNull(col):
    none(seq[int64])
  else:
    some(row.getInt64Array(col))

proc getFloatArrayOpt*(row: Row, col: int): Option[seq[float64]] =
  if row.isNull(col):
    none(seq[float64])
  else:
    some(row.getFloatArray(col))

proc getFloat32ArrayOpt*(row: Row, col: int): Option[seq[float32]] =
  if row.isNull(col):
    none(seq[float32])
  else:
    some(row.getFloat32Array(col))

proc getBoolArrayOpt*(row: Row, col: int): Option[seq[bool]] =
  if row.isNull(col):
    none(seq[bool])
  else:
    some(row.getBoolArray(col))

proc getStrArrayOpt*(row: Row, col: int): Option[seq[string]] =
  if row.isNull(col):
    none(seq[string])
  else:
    some(row.getStrArray(col))

# Format-aware row accessors (binary support)

proc decodeNumericBinary(data: openArray[byte]): string =
  ## Decode PostgreSQL binary numeric format:
  ##   2 bytes: ndigits (number of base-10000 digit groups)
  ##   2 bytes: weight (weight of first digit)
  ##   2 bytes: sign (0x0000=positive, 0x4000=negative, 0xC000=NaN)
  ##   2 bytes: dscale (digits after decimal point)
  ##   ndigits * 2 bytes: digit groups (each 0-9999)
  let ndigits = int(fromBE16(data.toOpenArray(0, 1)))
  let weight = int(int16(fromBE16(data.toOpenArray(2, 3))))
  let sign = uint16(fromBE16(data.toOpenArray(4, 5)))
  let dscale = int(fromBE16(data.toOpenArray(6, 7)))
  if sign == 0xC000'u16:
    return "NaN"
  var digits = newSeq[int16](ndigits)
  for i in 0 ..< ndigits:
    digits[i] = fromBE16(data.toOpenArray(8 + i * 2, 9 + i * 2))
  if ndigits == 0:
    if dscale > 0:
      return "0." & repeat('0', dscale)
    return "0"
  var intPart = ""
  var fracPart = ""
  let intGroups = weight + 1
  for i in 0 ..< ndigits:
    let d = int(digits[i])
    if i < intGroups:
      if intPart.len == 0:
        intPart.add($d)
      else:
        let s = $d
        intPart.add(repeat('0', 4 - s.len))
        intPart.add(s)
    else:
      let s = $d
      fracPart.add(repeat('0', 4 - s.len))
      fracPart.add(s)
  if intGroups > ndigits:
    intPart.add(repeat('0', (intGroups - ndigits) * 4))
  if intPart.len == 0:
    intPart = "0"
    let leadingZeroGroups = -intGroups
    fracPart = repeat('0', leadingZeroGroups * 4) & fracPart
  if dscale > 0:
    if fracPart.len > dscale:
      fracPart = fracPart[0 ..< dscale]
    elif fracPart.len < dscale:
      fracPart.add(repeat('0', dscale - fracPart.len))
    result = (if sign == 0x4000'u16: "-" else: "") & intPart & "." & fracPart
  else:
    result = (if sign == 0x4000'u16: "-" else: "") & intPart

proc getStr*(row: Row, col: int, fields: seq[FieldDescription]): string =
  if fields[col].formatCode == 0:
    return row.getStr(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  case fields[col].typeOid
  of OidInt2:
    return $fromBE16(row.data.buf.toOpenArray(off, off + clen - 1))
  of OidInt4:
    return $fromBE32(row.data.buf.toOpenArray(off, off + clen - 1))
  of OidInt8:
    return $fromBE64(row.data.buf.toOpenArray(off, off + clen - 1))
  of OidFloat4:
    return $cast[float32](cast[uint32](fromBE32(
      row.data.buf.toOpenArray(off, off + clen - 1)
    )))
  of OidFloat8:
    return $cast[float64](cast[uint64](fromBE64(
      row.data.buf.toOpenArray(off, off + clen - 1)
    )))
  of OidNumeric:
    return decodeNumericBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  of OidBool:
    return if row.data.buf[off] == 1: "t" else: "f"
  else:
    result = newString(clen)
    for i in 0 ..< clen:
      result[i] = char(row.data.buf[off + i])

proc getInt*(row: Row, col: int, fields: seq[FieldDescription]): int32 =
  if fields[col].formatCode == 0:
    return row.getInt(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if clen == 2:
    int32(fromBE16(row.data.buf.toOpenArray(off, off + 1)))
  else:
    fromBE32(row.data.buf.toOpenArray(off, off + 3))

proc getInt64*(row: Row, col: int, fields: seq[FieldDescription]): int64 =
  if fields[col].formatCode == 0:
    return row.getInt64(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if clen == 2:
    int64(fromBE16(row.data.buf.toOpenArray(off, off + 1)))
  elif clen == 4:
    int64(fromBE32(row.data.buf.toOpenArray(off, off + 3)))
  else:
    fromBE64(row.data.buf.toOpenArray(off, off + 7))

proc getNumeric*(row: Row, col: int, fields: seq[FieldDescription]): PgNumeric =
  if fields[col].formatCode == 0:
    return row.getNumeric(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  PgNumeric(decodeNumericBinary(row.data.buf.toOpenArray(off, off + clen - 1)))

proc getFloat*(row: Row, col: int, fields: seq[FieldDescription]): float64 =
  if fields[col].formatCode == 0:
    return row.getFloat(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if clen == 4:
    cast[float32](cast[uint32](fromBE32(row.data.buf.toOpenArray(off, off + 3)))).float64
  else:
    cast[float64](cast[uint64](fromBE64(row.data.buf.toOpenArray(off, off + 7))))

proc getBool*(row: Row, col: int, fields: seq[FieldDescription]): bool =
  if fields[col].formatCode == 0:
    return row.getBool(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  row.data.buf[off] == 1'u8

proc getBytes*(row: Row, col: int, fields: seq[FieldDescription]): seq[byte] =
  if fields[col].formatCode == 0:
    return row.getBytes(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  result = newSeq[byte](clen)
  if clen > 0:
    copyMem(addr result[0], unsafeAddr row.data.buf[off], clen)

proc getTimestamp*(row: Row, col: int, fields: seq[FieldDescription]): DateTime =
  if fields[col].formatCode == 0:
    return row.getTimestamp(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let pgUs = fromBE64(row.data.buf.toOpenArray(off, off + 7))
  let unixUs = pgUs + pgEpochUnix * 1_000_000
  var unixSec = unixUs div 1_000_000
  var fracUs = unixUs mod 1_000_000
  if fracUs < 0:
    unixSec -= 1
    fracUs += 1_000_000
  initTime(unixSec, int(fracUs * 1000)).utc()

proc getDate*(row: Row, col: int, fields: seq[FieldDescription]): DateTime =
  if fields[col].formatCode == 0:
    return row.getDate(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let pgDays = fromBE32(row.data.buf.toOpenArray(off, off + 3))
  let unixSec = (int64(pgDays) + int64(pgEpochDaysOffset)) * 86400
  initTime(unixSec, 0).utc()

proc getJson*(row: Row, col: int, fields: seq[FieldDescription]): JsonNode =
  if fields[col].formatCode == 0:
    return row.getJson(col)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  var jsonStr: string
  if fields[col].typeOid == OidJsonb and clen > 0 and row.data.buf[off] == 1:
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
