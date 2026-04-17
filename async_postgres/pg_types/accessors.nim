import std/[options, json, macros, parseutils, strutils, tables, times, net]

import ../pg_protocol
import ./core
import ./decoding

proc cellInfo*(row: Row, col: int): tuple[off: int, len: int] {.inline.} =
  if col < 0 or col >= int(row.data.numCols):
    raise newException(
      IndexDefect, "column index " & $col & " out of range 0..<" & $row.data.numCols
    )
  let idx = (int(row.rowIdx) * int(row.data.numCols) + col) * 2
  result.off = int(row.data.cellIndex[idx])
  result.len = int(row.data.cellIndex[idx + 1])

template bufView*(row: Row, off, clen: int): openArray[char] =
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
  initRow(rd, 0)

proc parseAffectedRowsRaw*(tag: openArray[char]): int64 =
  ## Extract row count from the raw bytes of a command tag (e.g.
  ## "UPDATE 3" -> 3, "INSERT 0 1" -> 1). Unlike `parseAffectedRows(string)`
  ## this performs zero heap allocation — useful for pipelines that process
  ## many `CommandComplete` messages.
  ##
  ## Mirrors the legacy `split(' ')` semantics exactly: the last token (bytes
  ## after the final space) must parse as an integer; a trailing space or any
  ## non-numeric tail yields 0.
  if tag.len == 0:
    return 0
  var lo = tag.high
  while lo >= 0 and tag[lo] != ' ':
    dec lo
  inc lo
  if lo > tag.high:
    return 0
  var parsed: BiggestInt = 0
  try:
    let consumed = parseutils.parseBiggestInt(tag.toOpenArray(lo, tag.high), parsed)
    if consumed == 0 or consumed != tag.high - lo + 1:
      return 0
  except ValueError, OverflowDefect:
    return 0
  parsed

proc parseAffectedRows*(tag: string): int64 =
  ## Extract row count from command tag (e.g. "UPDATE 3" -> 3, "INSERT 0 1" -> 1).
  parseAffectedRowsRaw(tag.toOpenArray(0, tag.high))

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
  if col < 0 or col >= int(row.data.numCols):
    raise newException(
      IndexDefect, "column index " & $col & " out of range 0..<" & $row.data.numCols
    )
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
  if parseInt(row.bufView(off, clen), v) == 0:
    raise newException(PgTypeError, "Column " & $col & ": invalid integer value")
  result = int32(v)

proc getInt16*(row: Row, col: int): int16 =
  ## Get a column value as int16. Handles binary int2 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 2:
      let b = row.data.buf
      return int16((uint16(b[off]) shl 8) or uint16(b[off + 1]))
    raise newException(
      PgTypeError,
      "Column " & $col & ": unexpected binary length " & $clen & " for int16",
    )
  var v: int
  if parseInt(row.bufView(off, clen), v) == 0:
    raise newException(PgTypeError, "Column " & $col & ": invalid int16 value")
  result = int16(v)

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
  if parseBiggestInt(row.bufView(off, clen), v) == 0:
    raise newException(PgTypeError, "Column " & $col & ": invalid int64 value")
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

proc getFloat32*(row: Row, col: int): float32 =
  ## Get a column value as float32. Handles binary float4 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 4:
      var bits: uint32
      let b = row.data.buf
      bits =
        (uint32(b[off]) shl 24) or (uint32(b[off + 1]) shl 16) or
        (uint32(b[off + 2]) shl 8) or uint32(b[off + 3])
      copyMem(addr result, addr bits, 4)
      return
  var f: float64
  if parseFloat(row.bufView(off, clen), f) == 0:
    raise newException(PgTypeError, "Column " & $col & ": invalid float32 value")
  result = float32(f)

proc getNumeric*(row: Row, col: int): PgNumeric =
  ## Get a column value as PgNumeric. Handles binary numeric format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen >= 8:
      return decodeNumericBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  parsePgNumeric(row.getStr(col))

proc getMoney*(row: Row, col: int, scale: int = 2): PgMoney =
  ## Get a column value as PgMoney. Handles binary money (8-byte int64) and
  ## locale-formatted text (see ``parsePgMoney`` for accepted forms).
  ## ``scale`` is the server's ``frac_digits`` (default 2 for ``C`` /
  ## ``en_US``; pass 0 for ``ja_JP`` etc.). The wire protocol does not expose
  ## this, so callers must specify it when it differs from the default.
  ## Raises ``PgTypeError`` on NULL or when ``scale`` is outside ``0..18``.
  if scale < 0 or scale > 18:
    raise newException(PgTypeError, "PgMoney scale out of range: " & $scale)
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 8:
      return PgMoney(
        amount: fromBE64(row.data.buf.toOpenArray(off, off + 7)), scale: int8(scale)
      )
    raise newException(
      PgTypeError,
      "Column " & $col & ": unexpected binary length " & $clen & " for money",
    )
  parsePgMoney(row.getStr(col), scale)

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
  return parseTimestampText(s)

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

proc getTimestampTz*(row: Row, col: int): DateTime =
  ## Get a column value as DateTime from a timestamptz column.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeBinaryTimestamp(row.data.buf.toOpenArray(off, off + 7))
  let s = row.getStr(col)
  return parseTimestampText(s)

proc getTime*(row: Row, col: int): PgTime =
  ## Get a column value as PgTime. Handles binary time format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeBinaryTime(row.data.buf.toOpenArray(off, off + 7))
  let s = row.getStr(col)
  return parseTimeText(s)

proc getTimeTz*(row: Row, col: int): PgTimeTz =
  ## Get a column value as PgTimeTz. Handles binary timetz format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeBinaryTimeTz(row.data.buf.toOpenArray(off, off + 11))
  let s = row.getStr(col)
  return parseTimeTzText(s)

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

proc getBit*(row: Row, col: int): PgBit =
  ## Get a column value as PgBit. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen < 4:
      raise newException(PgTypeError, "Invalid binary bit data: too short")
    let nbits = fromBE32(row.data.buf[off .. off + 3])
    let dataLen = clen - 4
    var data = newSeq[byte](dataLen)
    for i in 0 ..< dataLen:
      data[i] = row.data.buf[off + 4 + i]
    return PgBit(nbits: nbits, data: data)
  parseBitString(row.getStr(col))

proc getTsVector*(row: Row, col: int): PgTsVector =
  ## Get a column value as PgTsVector. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return
      PgTsVector(decodeBinaryTsVector(row.data.buf.toOpenArray(off, off + clen - 1)))
  PgTsVector(row.getStr(col))

proc getTsQuery*(row: Row, col: int): PgTsQuery =
  ## Get a column value as PgTsQuery. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return PgTsQuery(decodeBinaryTsQuery(row.data.buf.toOpenArray(off, off + clen - 1)))
  PgTsQuery(row.getStr(col))

proc getXml*(row: Row, col: int): PgXml =
  ## Get a column value as PgXml. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    var s = newString(clen)
    for i in 0 ..< clen:
      s[i] = char(row.data.buf[off + i])
    return PgXml(s)
  PgXml(row.getStr(col))

proc getHstore*(row: Row, col: int): PgHstore =
  ## Get a column value as PgHstore. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeHstoreBinary(row.data.buf.toOpenArray(off, off + clen - 1))
  parseHstoreText(row.getStr(col))

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
    result.a = decodeFloat64BE(row.data.buf, off)
    result.b = decodeFloat64BE(row.data.buf, off + 8)
    result.c = decodeFloat64BE(row.data.buf, off + 16)
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
    result.radius = decodeFloat64BE(row.data.buf, off + 16)
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

template optAccessor*(getProc, optProc: untyped, T: typedesc) =
  ## Generate ``optProc*(row, col): Option[T]`` that delegates to ``getProc``.
  proc optProc*(row: Row, col: int): Option[T] =
    if row.isNull(col):
      none(T)
    else:
      some(row.getProc(col))

template nameAccessor*(getProc: untyped, T: typedesc) =
  ## Generate ``getProc*(row, name): T`` that delegates to the index-based overload.
  proc getProc*(row: Row, name: string): T =
    row.getProc(row.columnIndex(name))

optAccessor(getStr, getStrOpt, string)
optAccessor(getInt, getIntOpt, int32)
optAccessor(getInt16, getInt16Opt, int16)
optAccessor(getInt64, getInt64Opt, int64)
optAccessor(getFloat, getFloatOpt, float64)
optAccessor(getFloat32, getFloat32Opt, float32)
optAccessor(getNumeric, getNumericOpt, PgNumeric)
optAccessor(getMoney, getMoneyOpt, PgMoney)
optAccessor(getUuid, getUuidOpt, PgUuid)
optAccessor(getBool, getBoolOpt, bool)
optAccessor(getBytes, getBytesOpt, seq[byte])
optAccessor(getJson, getJsonOpt, JsonNode)
optAccessor(getTimestamp, getTimestampOpt, DateTime)
optAccessor(getDate, getDateOpt, DateTime)
optAccessor(getTime, getTimeOpt, PgTime)
optAccessor(getTimeTz, getTimeTzOpt, PgTimeTz)
optAccessor(getTimestampTz, getTimestampTzOpt, DateTime)
optAccessor(getInterval, getIntervalOpt, PgInterval)
optAccessor(getInet, getInetOpt, PgInet)
optAccessor(getCidr, getCidrOpt, PgCidr)
optAccessor(getMacAddr, getMacAddrOpt, PgMacAddr)
optAccessor(getMacAddr8, getMacAddr8Opt, PgMacAddr8)
optAccessor(getTsVector, getTsVectorOpt, PgTsVector)
optAccessor(getTsQuery, getTsQueryOpt, PgTsQuery)
optAccessor(getXml, getXmlOpt, PgXml)
optAccessor(getBit, getBitOpt, PgBit)
optAccessor(getHstore, getHstoreOpt, PgHstore)
optAccessor(getPoint, getPointOpt, PgPoint)
optAccessor(getLine, getLineOpt, PgLine)
optAccessor(getLseg, getLsegOpt, PgLseg)
optAccessor(getBox, getBoxOpt, PgBox)
optAccessor(getPath, getPathOpt, PgPath)
optAccessor(getPolygon, getPolygonOpt, PgPolygon)
optAccessor(getCircle, getCircleOpt, PgCircle)

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

proc getMoneyArray*(row: Row, col: int, scale: int = 2): seq[PgMoney] =
  ## Get a column value as a seq of PgMoney. Handles binary array format and
  ## locale-formatted text arrays (see ``parsePgMoney``). ``scale`` tags each
  ## element's ``frac_digits`` and is also used for text parsing.
  ## Raises ``PgTypeError`` when ``scale`` is outside ``0..18``.
  if scale < 0 or scale > 18:
    raise newException(PgTypeError, "PgMoney scale out of range: " & $scale)
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgMoney](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in money array")
      result[i] = PgMoney(
        amount: fromBE64(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)),
        scale: int8(scale),
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in money array")
    result.add(parsePgMoney(e.get, scale))

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

proc getBitArray*(row: Row, col: int): seq[PgBit] =
  ## Get a column value as a seq of PgBit. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgBit](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in bit array")
      if e.len < 4:
        raise newException(PgTypeError, "Invalid binary bit element: too short")
      let nbits = fromBE32(row.data.buf.toOpenArray(off + e.off, off + e.off + 3))
      let dataLen = e.len - 4
      var data = newSeq[byte](dataLen)
      for j in 0 ..< dataLen:
        data[j] = row.data.buf[off + e.off + 4 + j]
      result[i] = PgBit(nbits: nbits, data: data)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in bit array")
    result.add(parseBitString(e.get))

# Temporal array decoders

template genTimestampArrayDecoder(getProc: untyped, typeName: static string) =
  proc getProc*(row: Row, col: int): seq[DateTime] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      result = newSeq[DateTime](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in " & typeName & " array")
        result[i] =
          decodeBinaryTimestamp(row.data.buf.toOpenArray(off + e.off, off + e.off + 7))
      return
    let s = row.getStr(col)
    let elems = parseTextArray(s)
    for e in elems:
      if e.isNone:
        raise newException(PgTypeError, "NULL element in " & typeName & " array")
      result.add(parseTimestampText(e.get))

genTimestampArrayDecoder(getTimestampArray, "timestamp")
genTimestampArrayDecoder(getTimestampTzArray, "timestamptz")

proc getDateArray*(row: Row, col: int): seq[DateTime] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[DateTime](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in date array")
      result[i] =
        decodeBinaryDate(row.data.buf.toOpenArray(off + e.off, off + e.off + 3))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in date array")
    try:
      result.add(parse(e.get, "yyyy-MM-dd"))
    except TimeParseError:
      raise newException(PgTypeError, "Invalid date: " & e.get)

proc getTimeArray*(row: Row, col: int): seq[PgTime] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgTime](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in time array")
      result[i] =
        decodeBinaryTime(row.data.buf.toOpenArray(off + e.off, off + e.off + 7))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in time array")
    result.add(parseTimeText(e.get))

proc getTimeTzArray*(row: Row, col: int): seq[PgTimeTz] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgTimeTz](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in timetz array")
      result[i] =
        decodeBinaryTimeTz(row.data.buf.toOpenArray(off + e.off, off + e.off + 11))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in timetz array")
    result.add(parseTimeTzText(e.get))

proc getIntervalArray*(row: Row, col: int): seq[PgInterval] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgInterval](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in interval array")
      if e.len != 16:
        raise
          newException(PgTypeError, "Invalid binary interval element length: " & $e.len)
      result[i].microseconds =
        fromBE64(row.data.buf.toOpenArray(off + e.off, off + e.off + 7))
      result[i].days =
        fromBE32(row.data.buf.toOpenArray(off + e.off + 8, off + e.off + 11))
      result[i].months =
        fromBE32(row.data.buf.toOpenArray(off + e.off + 12, off + e.off + 15))
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in interval array")
    result.add(parseIntervalText(e.get))

# Identifier / network array decoders

proc getUuidArray*(row: Row, col: int): seq[PgUuid] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgUuid](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in uuid array")
      if e.len != 16:
        raise newException(PgTypeError, "Invalid binary uuid element length: " & $e.len)
      const hexChars = "0123456789abcdef"
      var s = newString(36)
      var pos = 0
      for j in 0 ..< 16:
        if j == 4 or j == 6 or j == 8 or j == 10:
          s[pos] = '-'
          inc pos
        let b = row.data.buf[off + e.off + j]
        s[pos] = hexChars[int(b shr 4)]
        s[pos + 1] = hexChars[int(b and 0x0F)]
        pos += 2
      result[i] = PgUuid(s)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in uuid array")
    result.add(PgUuid(e.get))

template genInetArrayDecoder(getProc: untyped, T: typedesc, typeName: static string) =
  proc getProc*(row: Row, col: int): seq[T] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      result = newSeq[T](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in " & typeName & " array")
        let (ip, mask) = decodeInetBinary(
          row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
        )
        result[i] = T(address: ip, mask: mask)
      return
    let s = row.getStr(col)
    let elems = parseTextArray(s)
    for e in elems:
      if e.isNone:
        raise newException(PgTypeError, "NULL element in " & typeName & " array")
      let (ip, mask) = parseInetText(e.get)
      result.add(T(address: ip, mask: mask))

genInetArrayDecoder(getInetArray, PgInet, "inet")
genInetArrayDecoder(getCidrArray, PgCidr, "cidr")

template genMacAddrArrayDecoder(
    getProc: untyped, T: typedesc, nBytes: static int, typeName: static string
) =
  proc getProc*(row: Row, col: int): seq[T] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      result = newSeq[T](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in " & typeName & " array")
        if e.len != nBytes:
          raise newException(
            PgTypeError, "Invalid binary " & typeName & " element length: " & $e.len
          )
        var parts = newSeq[string](nBytes)
        for j in 0 ..< nBytes:
          parts[j] = toHex(row.data.buf[off + e.off + j], 2).toLowerAscii()
        result[i] = T(parts.join(":"))
      return
    let s = row.getStr(col)
    let elems = parseTextArray(s)
    for e in elems:
      if e.isNone:
        raise newException(PgTypeError, "NULL element in " & typeName & " array")
      result.add(T(e.get))

genMacAddrArrayDecoder(getMacAddrArray, PgMacAddr, 6, "macaddr")
genMacAddrArrayDecoder(getMacAddr8Array, PgMacAddr8, 8, "macaddr8")

# Numeric / binary / JSON array decoders

proc getNumericArray*(row: Row, col: int): seq[PgNumeric] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgNumeric](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in numeric array")
      result[i] = decodeNumericBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in numeric array")
    result.add(parsePgNumeric(e.get))

proc getBytesArray*(row: Row, col: int): seq[seq[byte]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[seq[byte]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in bytea array")
      result[i] = newSeq[byte](e.len)
      if e.len > 0:
        copyMem(addr result[i][0], unsafeAddr row.data.buf[off + e.off], e.len)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in bytea array")
    let v = e.get
    if v.len >= 2 and v[0] == '\\' and v[1] == 'x':
      let hexStr = v[2 ..^ 1]
      var bytes = newSeq[byte](hexStr.len div 2)
      for j in 0 ..< bytes.len:
        bytes[j] = byte(parseHexInt(hexStr[j * 2 .. j * 2 + 1]))
      result.add(bytes)
    else:
      result.add(toBytes(v))

proc getJsonArray*(row: Row, col: int): seq[JsonNode] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[JsonNode](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in json array")
      var jsonStr: string
      if decoded.elemOid == OidJsonb and e.len > 0 and row.data.buf[off + e.off] == 1:
        jsonStr = newString(e.len - 1)
        for j in 1 ..< e.len:
          jsonStr[j - 1] = char(row.data.buf[off + e.off + j])
      else:
        jsonStr = newString(e.len)
        for j in 0 ..< e.len:
          jsonStr[j] = char(row.data.buf[off + e.off + j])
      try:
        result[i] = parseJson(jsonStr)
      except JsonParsingError:
        raise newException(PgTypeError, "Invalid JSON element: " & jsonStr)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in json array")
    try:
      result.add(parseJson(e.get))
    except JsonParsingError:
      raise newException(PgTypeError, "Invalid JSON element: " & e.get)

# Geometric array decoders

proc getPointArray*(row: Row, col: int): seq[PgPoint] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgPoint](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in point array")
      result[i] = decodePointBinary(row.data.buf, off + e.off)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in point array")
    result.add(parsePointText(e.get))

proc getLineArray*(row: Row, col: int): seq[PgLine] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgLine](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in line array")
      if e.len != 24:
        raise newException(PgTypeError, "Invalid binary line element length: " & $e.len)
      let o = off + e.off
      result[i].a = decodeFloat64BE(row.data.buf, o)
      result[i].b = decodeFloat64BE(row.data.buf, o + 8)
      result[i].c = decodeFloat64BE(row.data.buf, o + 16)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in line array")
    let v = e.get.strip()
    var inner = v
    if inner.len >= 2 and inner[0] == '{' and inner[^1] == '}':
      inner = inner[1 ..^ 2]
    else:
      raise newException(PgTypeError, "Invalid line: " & v)
    let parts = inner.split(',')
    if parts.len != 3:
      raise newException(PgTypeError, "Invalid line: " & v)
    result.add(
      PgLine(a: parseFloat(parts[0]), b: parseFloat(parts[1]), c: parseFloat(parts[2]))
    )

proc getLsegArray*(row: Row, col: int): seq[PgLseg] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgLseg](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in lseg array")
      result[i] = PgLseg(
        p1: decodePointBinary(row.data.buf, off + e.off),
        p2: decodePointBinary(row.data.buf, off + e.off + 16),
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in lseg array")
    let v = e.get.strip()
    var inner = v
    if inner.len >= 2 and inner[0] == '[' and inner[^1] == ']':
      inner = inner[1 ..^ 2]
    let points = parsePointsText(inner)
    if points.len != 2:
      raise newException(PgTypeError, "Invalid lseg: " & v)
    result.add(PgLseg(p1: points[0], p2: points[1]))

proc getBoxArray*(row: Row, col: int): seq[PgBox] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgBox](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in box array")
      result[i] = PgBox(
        high: decodePointBinary(row.data.buf, off + e.off),
        low: decodePointBinary(row.data.buf, off + e.off + 16),
      )
    return
  # PostgreSQL uses ';' as array element delimiter for box type
  let s = row.getStr(col)
  if s.len < 2 or s[0] != '{' or s[^1] != '}':
    raise newException(PgTypeError, "Invalid box array literal: " & s)
  let inner = s[1 ..^ 2]
  if inner.len == 0:
    return
  let parts = inner.split(';')
  for p in parts:
    let v = p.strip()
    if v == "NULL":
      raise newException(PgTypeError, "NULL element in box array")
    let points = parsePointsText(v)
    if points.len != 2:
      raise newException(PgTypeError, "Invalid box: " & v)
    result.add(PgBox(high: points[0], low: points[1]))

proc getPathArray*(row: Row, col: int): seq[PgPath] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgPath](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in path array")
      let b = row.data.buf
      let o = off + e.off
      result[i].closed = b[o] != 0
      let npts = fromBE32(b.toOpenArray(o + 1, o + 4))
      result[i].points = newSeq[PgPoint](npts)
      for j in 0 ..< npts:
        result[i].points[j] = decodePointBinary(b, o + 5 + j * 16)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in path array")
    let v = e.get.strip()
    if v.len < 2:
      raise newException(PgTypeError, "Invalid path: " & v)
    let closed = v[0] == '('
    let inner = v[1 ..^ 2]
    result.add(PgPath(closed: closed, points: parsePointsText(inner)))

proc getPolygonArray*(row: Row, col: int): seq[PgPolygon] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgPolygon](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in polygon array")
      let b = row.data.buf
      let o = off + e.off
      let npts = fromBE32(b.toOpenArray(o, o + 3))
      result[i].points = newSeq[PgPoint](npts)
      for j in 0 ..< npts:
        result[i].points[j] = decodePointBinary(b, o + 4 + j * 16)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in polygon array")
    let v = e.get.strip()
    if v.len < 2 or v[0] != '(' or v[^1] != ')':
      raise newException(PgTypeError, "Invalid polygon: " & v)
    result.add(PgPolygon(points: parsePointsText(v[1 ..^ 2])))

proc getCircleArray*(row: Row, col: int): seq[PgCircle] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgCircle](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in circle array")
      if e.len != 24:
        raise
          newException(PgTypeError, "Invalid binary circle element length: " & $e.len)
      result[i].center = decodePointBinary(row.data.buf, off + e.off)
      result[i].radius = decodeFloat64BE(row.data.buf, off + e.off + 16)
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in circle array")
    let v = e.get.strip()
    if v.len < 2 or v[0] != '<' or v[^1] != '>':
      raise newException(PgTypeError, "Invalid circle: " & v)
    let inner = v[1 ..^ 2]
    var depth = 0
    var lastComma = -1
    for j in 0 ..< inner.len:
      if inner[j] == '(':
        depth += 1
      elif inner[j] == ')':
        depth -= 1
      elif inner[j] == ',' and depth == 0:
        lastComma = j
    if lastComma < 0:
      raise newException(PgTypeError, "Invalid circle: " & v)
    result.add(
      PgCircle(
        center: parsePointText(inner[0 ..< lastComma]),
        radius: parseFloat(inner[lastComma + 1 ..^ 1]),
      )
    )

# Other array decoders

template genStringArrayDecoder(getProc: untyped, T: typedesc, typeName: static string) =
  proc getProc*(row: Row, col: int): seq[T] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      result = newSeq[T](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in " & typeName & " array")
        var s = newString(e.len)
        if e.len > 0:
          copyMem(addr s[0], unsafeAddr row.data.buf[off + e.off], e.len)
        result[i] = T(s)
      return
    let s = row.getStr(col)
    let elems = parseTextArray(s)
    for e in elems:
      if e.isNone:
        raise newException(PgTypeError, "NULL element in " & typeName & " array")
      result.add(T(e.get))

genStringArrayDecoder(getXmlArray, PgXml, "xml")
genStringArrayDecoder(getTsVectorArray, PgTsVector, "tsvector")
genStringArrayDecoder(getTsQueryArray, PgTsQuery, "tsquery")

proc getHstoreArray*(row: Row, col: int): seq[PgHstore] =
  ## Get a column value as ``seq[PgHstore]``. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[PgHstore](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in hstore array")
      result[i] = decodeHstoreBinary(
        row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
      )
    return
  let s = row.getStr(col)
  let elems = parseTextArray(s)
  for e in elems:
    if e.isNone:
      raise newException(PgTypeError, "NULL element in hstore array")
    result.add(parseHstoreText(e.get))

# Element-level NULL-safe array getters

proc getIntArrayElemOpt*(row: Row, col: int): seq[Option[int32]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[int32]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(int32)
      else:
        result[i] =
          some(fromBE32(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)))
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      result.add(none(int32))
    else:
      result.add(some(int32(parseInt(e.get))))

proc getInt16ArrayElemOpt*(row: Row, col: int): seq[Option[int16]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[int16]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(int16)
      else:
        result[i] =
          some(fromBE16(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)))
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      result.add(none(int16))
    else:
      result.add(some(int16(parseInt(e.get))))

proc getInt64ArrayElemOpt*(row: Row, col: int): seq[Option[int64]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[int64]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(int64)
      else:
        result[i] =
          some(fromBE64(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)))
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      result.add(none(int64))
    else:
      result.add(some(parseBiggestInt(e.get)))

proc getFloatArrayElemOpt*(row: Row, col: int): seq[Option[float64]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[float64]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(float64)
      elif e.len == 4:
        result[i] = some(
          float64(
            cast[float32](cast[uint32](fromBE32(
              row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
            )))
          )
        )
      else:
        result[i] = some(
          cast[float64](cast[uint64](fromBE64(
            row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
          )))
        )
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      result.add(none(float64))
    else:
      result.add(some(parseFloat(e.get)))

proc getFloat32ArrayElemOpt*(row: Row, col: int): seq[Option[float32]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[float32]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(float32)
      else:
        result[i] = some(
          cast[float32](cast[uint32](fromBE32(
            row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
          )))
        )
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      result.add(none(float32))
    else:
      result.add(some(float32(parseFloat(e.get))))

proc getBoolArrayElemOpt*(row: Row, col: int): seq[Option[bool]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[bool]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(bool)
      else:
        result[i] = some(row.data.buf[off + e.off] == 1'u8)
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      result.add(none(bool))
    else:
      case e.get
      of "t", "true", "1":
        result.add(some(true))
      of "f", "false", "0":
        result.add(some(false))
      else:
        raise newException(PgTypeError, "Invalid boolean: " & e.get)

proc getStrArrayElemOpt*(row: Row, col: int): seq[Option[string]] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[string]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(string)
      else:
        var s = newString(e.len)
        if e.len > 0:
          copyMem(addr s[0], unsafeAddr row.data.buf[off + e.off], e.len)
        result[i] = some(s)
    return
  let s = row.getStr(col)
  result = parseTextArray(s)

# Array Opt accessors (text format)

optAccessor(getIntArray, getIntArrayOpt, seq[int32])
optAccessor(getInt16Array, getInt16ArrayOpt, seq[int16])
optAccessor(getInt64Array, getInt64ArrayOpt, seq[int64])
optAccessor(getFloatArray, getFloatArrayOpt, seq[float64])
optAccessor(getFloat32Array, getFloat32ArrayOpt, seq[float32])
optAccessor(getBoolArray, getBoolArrayOpt, seq[bool])
optAccessor(getStrArray, getStrArrayOpt, seq[string])
optAccessor(getBitArray, getBitArrayOpt, seq[PgBit])
optAccessor(getTimestampArray, getTimestampArrayOpt, seq[DateTime])
optAccessor(getTimestampTzArray, getTimestampTzArrayOpt, seq[DateTime])
optAccessor(getDateArray, getDateArrayOpt, seq[DateTime])
optAccessor(getTimeArray, getTimeArrayOpt, seq[PgTime])
optAccessor(getTimeTzArray, getTimeTzArrayOpt, seq[PgTimeTz])
optAccessor(getIntervalArray, getIntervalArrayOpt, seq[PgInterval])
optAccessor(getUuidArray, getUuidArrayOpt, seq[PgUuid])
optAccessor(getInetArray, getInetArrayOpt, seq[PgInet])
optAccessor(getCidrArray, getCidrArrayOpt, seq[PgCidr])
optAccessor(getMacAddrArray, getMacAddrArrayOpt, seq[PgMacAddr])
optAccessor(getMacAddr8Array, getMacAddr8ArrayOpt, seq[PgMacAddr8])
optAccessor(getNumericArray, getNumericArrayOpt, seq[PgNumeric])
optAccessor(getMoneyArray, getMoneyArrayOpt, seq[PgMoney])
optAccessor(getBytesArray, getBytesArrayOpt, seq[seq[byte]])
optAccessor(getJsonArray, getJsonArrayOpt, seq[JsonNode])
optAccessor(getPointArray, getPointArrayOpt, seq[PgPoint])
optAccessor(getLineArray, getLineArrayOpt, seq[PgLine])
optAccessor(getLsegArray, getLsegArrayOpt, seq[PgLseg])
optAccessor(getBoxArray, getBoxArrayOpt, seq[PgBox])
optAccessor(getPathArray, getPathArrayOpt, seq[PgPath])
optAccessor(getPolygonArray, getPolygonArrayOpt, seq[PgPolygon])
optAccessor(getCircleArray, getCircleArrayOpt, seq[PgCircle])
optAccessor(getXmlArray, getXmlArrayOpt, seq[PgXml])
optAccessor(getTsVectorArray, getTsVectorArrayOpt, seq[PgTsVector])
optAccessor(getTsQueryArray, getTsQueryArrayOpt, seq[PgTsQuery])
optAccessor(getHstoreArray, getHstoreArrayOpt, seq[PgHstore])

# Column-level + element-level NULL-safe
optAccessor(getIntArrayElemOpt, getIntArrayElemOptOpt, seq[Option[int32]])
optAccessor(getInt16ArrayElemOpt, getInt16ArrayElemOptOpt, seq[Option[int16]])
optAccessor(getInt64ArrayElemOpt, getInt64ArrayElemOptOpt, seq[Option[int64]])
optAccessor(getFloatArrayElemOpt, getFloatArrayElemOptOpt, seq[Option[float64]])
optAccessor(getFloat32ArrayElemOpt, getFloat32ArrayElemOptOpt, seq[Option[float32]])
optAccessor(getBoolArrayElemOpt, getBoolArrayElemOptOpt, seq[Option[bool]])
optAccessor(getStrArrayElemOpt, getStrArrayElemOptOpt, seq[Option[string]])

# Generic accessors — static dispatch by type, no OID branching.

proc get*(row: Row, col: int, T: typedesc[int16]): int16 =
  ## Generic typed accessor. Usage: ``row.get(0, int16)``
  row.getInt16(col)

proc get*(row: Row, col: int, T: typedesc[int32]): int32 =
  ## Generic typed accessor. Usage: ``row.get(0, int32)``
  row.getInt(col)

proc get*(row: Row, col: int, T: typedesc[int64]): int64 =
  row.getInt64(col)

proc get*(row: Row, col: int, T: typedesc[float32]): float32 =
  row.getFloat32(col)

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

proc get*(row: Row, col: int, T: typedesc[PgMoney]): PgMoney =
  row.getMoney(col)

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

proc get*(row: Row, col: int, T: typedesc[PgTsVector]): PgTsVector =
  row.getTsVector(col)

proc get*(row: Row, col: int, T: typedesc[PgTsQuery]): PgTsQuery =
  row.getTsQuery(col)

proc get*(row: Row, col: int, T: typedesc[PgXml]): PgXml =
  row.getXml(col)

proc get*(row: Row, col: int, T: typedesc[PgBit]): PgBit =
  row.getBit(col)

proc get*(row: Row, col: int, T: typedesc[PgHstore]): PgHstore =
  row.getHstore(col)

proc get*(row: Row, col: int, T: typedesc[PgUuid]): PgUuid =
  row.getUuid(col)

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

proc get*(row: Row, col: int, T: typedesc[seq[PgBit]]): seq[PgBit] =
  row.getBitArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgTime]]): seq[PgTime] =
  row.getTimeArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgTimeTz]]): seq[PgTimeTz] =
  row.getTimeTzArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgInterval]]): seq[PgInterval] =
  row.getIntervalArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgUuid]]): seq[PgUuid] =
  row.getUuidArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgInet]]): seq[PgInet] =
  row.getInetArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgCidr]]): seq[PgCidr] =
  row.getCidrArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgMacAddr]]): seq[PgMacAddr] =
  row.getMacAddrArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgMacAddr8]]): seq[PgMacAddr8] =
  row.getMacAddr8Array(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgNumeric]]): seq[PgNumeric] =
  row.getNumericArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgMoney]]): seq[PgMoney] =
  row.getMoneyArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[JsonNode]]): seq[JsonNode] =
  row.getJsonArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgPoint]]): seq[PgPoint] =
  row.getPointArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgLine]]): seq[PgLine] =
  row.getLineArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgLseg]]): seq[PgLseg] =
  row.getLsegArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgBox]]): seq[PgBox] =
  row.getBoxArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgPath]]): seq[PgPath] =
  row.getPathArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgPolygon]]): seq[PgPolygon] =
  row.getPolygonArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgCircle]]): seq[PgCircle] =
  row.getCircleArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgXml]]): seq[PgXml] =
  row.getXmlArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgTsVector]]): seq[PgTsVector] =
  row.getTsVectorArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgTsQuery]]): seq[PgTsQuery] =
  row.getTsQueryArray(col)

proc get*(row: Row, col: int, T: typedesc[seq[PgHstore]]): seq[PgHstore] =
  row.getHstoreArray(col)

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
