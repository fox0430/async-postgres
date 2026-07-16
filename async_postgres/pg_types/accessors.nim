import std/[options, json, macros, parseutils, strutils, tables, times, net]

import ../pg_protocol
import core, decoding, encoding

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
  ## clen <= 0 skips `addr buf[off]`: a trailing empty cell has off == buf.len,
  ## which would otherwise raise an uncatchable IndexDefect.
  (
    if clen > 0:
      cast[ptr UncheckedArray[char]](addr row.data.buf[off]).toOpenArray(0, clen - 1)
    else:
      cast[ptr UncheckedArray[char]](nil).toOpenArray(0, -1)
  )

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

const NumericBinaryHeaderLen = 8
  ## Minimum byte length of a binary numeric value (4 x int16: ndigits, weight, sign, dscale).

template raiseIfBadNumericBinary(col, clen: int) =
  if clen < NumericBinaryHeaderLen:
    raise newException(
      PgTypeError,
      "Column " & $col & ": unexpected binary length " & $clen & " for numeric",
    )

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
      if clen != 1:
        raise newException(
          PgTypeError,
          "Column " & $col & ": unexpected binary length " & $clen & " for bool",
        )
      return if b[off] != 0: "t" else: "f"
    of 21: # int2
      if clen != 2:
        raise newException(
          PgTypeError,
          "Column " & $col & ": unexpected binary length " & $clen & " for int2",
        )
      return $fromBE16(b, off)
    of 23: # int4
      if clen != 4:
        raise newException(
          PgTypeError,
          "Column " & $col & ": unexpected binary length " & $clen & " for int4",
        )
      return $fromBE32(b, off)
    of 20: # int8
      if clen != 8:
        raise newException(
          PgTypeError,
          "Column " & $col & ": unexpected binary length " & $clen & " for int8",
        )
      return $fromBE64(b, off)
    of 700: # float4
      if clen != 4:
        raise newException(
          PgTypeError,
          "Column " & $col & ": unexpected binary length " & $clen & " for float4",
        )
      return $decodeFloat32BE(b, off)
    of 701: # float8
      if clen != 8:
        raise newException(
          PgTypeError,
          "Column " & $col & ": unexpected binary length " & $clen & " for float8",
        )
      return $decodeFloat64BE(b, off)
    of OidNumeric:
      raiseIfBadNumericBinary(col, clen)
      return $decodeNumericBinary(b.toOpenArray(off, off + clen - 1))
    else:
      discard # text, varchar, bytea: fall through to raw copy
  result = readString(row.data.buf, off, clen)

proc getInt*(row: Row, col: int): int32 =
  ## Get a column value as int32. Handles binary int2/int4 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 4:
      return fromBE32(row.data.buf, off)
    elif clen == 2:
      return int32(fromBE16(row.data.buf, off))
    else:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for int32",
      )
  var v: int
  var n: int
  # ``parseInt(s, v)`` returns 0 for "no digits" but raises a raw ``ValueError``
  # when the value overflows ``int``; route that through ``pgTypeErrorOnValueError``
  # so an oversized text value surfaces as a catchable ``PgTypeError`` instead of
  # escaping the ``except PgError`` contract.
  pgTypeErrorOnValueError("Column " & $col & ": integer value out of range"):
    n = parseInt(row.bufView(off, clen), v)
  if n == 0:
    raise newException(PgTypeError, "Column " & $col & ": invalid integer value")
  if v < int(int32.low) or v > int(int32.high):
    raise newException(
      PgTypeError, "Column " & $col & ": integer value out of int32 range: " & $v
    )
  result = int32(v)

proc getInt16*(row: Row, col: int): int16 =
  ## Get a column value as int16. Handles binary int2 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 2:
      return fromBE16(row.data.buf, off)
    else:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for int16",
      )
  var v: int
  var n: int
  # Convert ``parseInt``'s overflow ``ValueError`` to ``PgTypeError`` (see getInt).
  pgTypeErrorOnValueError("Column " & $col & ": integer value out of range"):
    n = parseInt(row.bufView(off, clen), v)
  if n == 0:
    raise newException(PgTypeError, "Column " & $col & ": invalid int16 value")
  if v < int(int16.low) or v > int(int16.high):
    raise newException(
      PgTypeError, "Column " & $col & ": integer value out of int16 range: " & $v
    )
  result = int16(v)

proc getInt64*(row: Row, col: int): int64 =
  ## Get a column value as int64. Handles binary int2/4/8 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 8:
      return fromBE64(row.data.buf, off)
    elif clen == 4:
      return int64(fromBE32(row.data.buf, off))
    elif clen == 2:
      return int64(fromBE16(row.data.buf, off))
    else:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for int64",
      )
  var v: BiggestInt
  var n: int
  # Convert ``parseBiggestInt``'s overflow ``ValueError`` to ``PgTypeError`` (see getInt).
  pgTypeErrorOnValueError("Column " & $col & ": integer value out of range"):
    n = parseBiggestInt(row.bufView(off, clen), v)
  if n == 0:
    raise newException(PgTypeError, "Column " & $col & ": invalid int64 value")
  result = v

proc getFloat*(row: Row, col: int): float64 =
  ## Get a column value as float64. Handles binary float4/8 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 8:
      return decodeFloat64BE(row.data.buf, off)
    elif clen == 4:
      return float64(decodeFloat32BE(row.data.buf, off))
    else:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for float64",
      )
  # Route through `pgParseFloat` so the text path accepts PostgreSQL's
  # ``Infinity``/``-Infinity`` spelling and surfaces failures as `PgTypeError`,
  # matching the array/range paths. Parse the zero-copy ``bufView`` of the already
  # known ``(off, clen)`` rather than re-running ``getStr``'s NULL/binary checks
  # or allocating an intermediate string.
  result = pgParseFloat(row.bufView(off, clen))

proc getFloat32*(row: Row, col: int): float32 =
  ## Get a column value as float32. Handles binary float4 directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen == 4:
      return decodeFloat32BE(row.data.buf, off)
    else:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for float32",
      )
  # `pgParseFloat32` adds `Infinity`/`-Infinity` support and rejects finite
  # values that overflow float32's range (the old `float32(parseFloat)` collapsed
  # them silently to `inf`), keeping the scalar path consistent with the arrays.
  # Parse the zero-copy ``bufView`` to skip ``getStr``'s redundant NULL/binary
  # re-checks and avoid an intermediate string allocation.
  result = pgParseFloat32(row.bufView(off, clen))

proc getNumeric*(row: Row, col: int): PgNumeric =
  ## Get a column value as PgNumeric. Handles binary numeric format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    raiseIfBadNumericBinary(col, clen)
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

# Binary decoders for types whose scalar accessors reuse the same body as the
# array-element decoders below. Defined here (above the scalars) so both call
# sites route through a single implementation. The rest of the
# `decodePgArrayElement*` overload set — plus text-only helpers — lives in the
# registry section further down.

proc decodePgArrayElement*(_: typedesc[PgUuid], buf: openArray[byte]): PgUuid =
  if buf.len != 16:
    raise newException(PgTypeError, "uuid: bad length " & $buf.len)
  const hexChars = "0123456789abcdef"
  var s = newString(36)
  var pos = 0
  for j in 0 ..< 16:
    if j == 4 or j == 6 or j == 8 or j == 10:
      s[pos] = '-'
      inc pos
    let b = buf[j]
    s[pos] = hexChars[int(b shr 4)]
    s[pos + 1] = hexChars[int(b and 0x0F)]
    pos += 2
  PgUuid(s)

proc decodePgArrayElement*(_: typedesc[PgInterval], buf: openArray[byte]): PgInterval =
  if buf.len != 16:
    raise newException(PgTypeError, "interval: bad length " & $buf.len)
  result.microseconds = fromBE64(buf.toOpenArray(0, 7))
  result.days = fromBE32(buf.toOpenArray(8, 11))
  result.months = fromBE32(buf.toOpenArray(12, 15))

proc decodePgArrayElement*(_: typedesc[PgMacAddr], buf: openArray[byte]): PgMacAddr =
  if buf.len != 6:
    raise newException(PgTypeError, "macaddr: bad length " & $buf.len)
  var parts = newSeq[string](6)
  for j in 0 ..< 6:
    parts[j] = toHex(buf[j], 2).toLowerAscii()
  PgMacAddr(parts.join(":"))

proc decodePgArrayElement*(_: typedesc[PgMacAddr8], buf: openArray[byte]): PgMacAddr8 =
  if buf.len != 8:
    raise newException(PgTypeError, "macaddr8: bad length " & $buf.len)
  var parts = newSeq[string](8)
  for j in 0 ..< 8:
    parts[j] = toHex(buf[j], 2).toLowerAscii()
  PgMacAddr8(parts.join(":"))

proc decodeJsonArrayElem*(buf: openArray[byte], elemOid: int32): JsonNode =
  # Strip the leading jsonb version byte only when elemOid says jsonb.
  let jsonStr =
    if elemOid == OidJsonb and buf.len > 0 and buf[0] == 1:
      readString(buf, 1, buf.len - 1)
    else:
      readString(buf, 0, buf.len)
  try:
    parseJson(jsonStr)
  except JsonParsingError:
    raise newException(PgTypeError, "Invalid JSON: " & jsonStr)

proc getUuid*(row: Row, col: int): PgUuid =
  ## Get a column value as PgUuid. Handles binary format (16 bytes).
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodePgArrayElement(PgUuid, row.data.buf.toOpenArray(off, off + clen - 1))
  PgUuid(row.getStr(col))

proc getBool*(row: Row, col: int): bool =
  ## Get a column value as bool. Handles binary format directly. Raises `PgTypeError` on NULL.
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    if clen != 1:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for bool",
      )
    return row.data.buf[off] != 0
  if clen == 0:
    raise newException(PgTypeError, "Column " & $col & ": empty boolean value")
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
    result = readBytes(row.data.buf, off, clen)
    return
  # Text format: bytea uses hex encoding \xDEADBEEF
  if clen >= 2 and row.data.buf[off] == byte('\\') and row.data.buf[off + 1] == byte(
    'x'
  ):
    let hexLen = clen - 2
    if hexLen mod 2 != 0:
      raise newException(
        PgTypeError, "Column " & $col & ": odd-length hex in bytea text value"
      )
    result = newSeq[byte](hexLen div 2)
    let hexOff = off + 2
    let errCtx = "Column " & $col
    for i in 0 ..< result.len:
      result[i] = decodeHexPair(row.data.buf, hexOff + i * 2, errCtx)
  else:
    result = readBytes(row.data.buf, off, clen)

proc getTimestamp*(row: Row, col: int): DateTime =
  ## Get a column value as DateTime. Handles binary timestamp format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 8:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for timestamp",
      )
    return decodeBinaryTimestamp(row.data.buf.toOpenArray(off, off + 7))
  let s = row.getStr(col)
  return parseTimestampText(s)

proc getDate*(row: Row, col: int): DateTime =
  ## Get a column value as DateTime. Handles binary date format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 4:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for date",
      )
    return decodeBinaryDate(row.data.buf.toOpenArray(off, off + 3))
  parseDateText(row.getStr(col))

proc getTimestampTz*(row: Row, col: int): DateTime =
  ## Get a column value as DateTime from a timestamptz column.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 8:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for timestamptz",
      )
    return decodeBinaryTimestamp(row.data.buf.toOpenArray(off, off + 7))
  let s = row.getStr(col)
  return parseTimestampText(s)

proc getTime*(row: Row, col: int): PgTime =
  ## Get a column value as PgTime. Handles binary time format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 8:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for time",
      )
    return decodeBinaryTime(row.data.buf.toOpenArray(off, off + 7))
  let s = row.getStr(col)
  return parseTimeText(s)

proc getTimeTz*(row: Row, col: int): PgTimeTz =
  ## Get a column value as PgTimeTz. Handles binary timetz format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen != 12:
      raise newException(
        PgTypeError,
        "Column " & $col & ": unexpected binary length " & $clen & " for timetz",
      )
    return decodeBinaryTimeTz(row.data.buf.toOpenArray(off, off + 11))
  let s = row.getStr(col)
  return parseTimeTzText(s)

proc getJson*(row: Row, col: int): JsonNode =
  ## Get a column value as a parsed JsonNode. Handles binary json/jsonb format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return decodeJsonArrayElem(
      row.data.buf.toOpenArray(off, off + clen - 1), row.colTypeOid(col)
    )
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
    return
      decodePgArrayElement(PgInterval, row.data.buf.toOpenArray(off, off + clen - 1))
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
    return
      decodePgArrayElement(PgMacAddr, row.data.buf.toOpenArray(off, off + clen - 1))
  PgMacAddr(row.getStr(col))

proc getMacAddr8*(row: Row, col: int): PgMacAddr8 =
  ## Get a column value as PgMacAddr8 (EUI-64). Handles binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    return
      decodePgArrayElement(PgMacAddr8, row.data.buf.toOpenArray(off, off + clen - 1))
  PgMacAddr8(row.getStr(col))

proc getBit*(row: Row, col: int): PgBit =
  ## Get a column value as PgBit. Handles both text and binary format.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    if clen < 4:
      raise newException(PgTypeError, "Invalid binary bit data: too short")
    let nbits = fromBE32(row.data.buf.toOpenArray(off, off + 3))
    if nbits < 0:
      raise
        newException(PgTypeError, "Invalid binary bit data: negative nbits " & $nbits)
    if nbits > PgBitMaxBits:
      raise newException(
        PgTypeError,
        "Invalid binary bit data: nbits " & $nbits & " exceeds limit (" & $PgBitMaxBits &
          ")",
      )
    let dataLen = clen - 4
    if (int64(nbits) + 7) div 8 != int64(dataLen):
      raise newException(
        PgTypeError,
        "Invalid binary bit data: nbits=" & $nbits & " inconsistent with dataLen=" &
          $dataLen,
      )
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
    let s = readString(row.data.buf, off, clen)
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
  PgLine(
    a: pgParseFloat(parts[0]), b: pgParseFloat(parts[1]), c: pgParseFloat(parts[2])
  )

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
    if clen < 5:
      raise newException(
        PgTypeError,
        "Column " & $col & ": binary path header too short (" & $clen & " bytes)",
      )
    let b = row.data.buf
    result.closed = b[off] != 0
    let npts = fromBE32(b.toOpenArray(off + 1, off + 4))
    if npts < 0:
      raise newException(
        PgTypeError,
        "Column " & $col & ": binary path has negative point count " & $npts,
      )
    if npts > (clen - 5) div 16:
      raise newException(
        PgTypeError,
        "Column " & $col & ": binary path " & $npts & " points exceed " & $clen &
          "-byte cell",
      )
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
    if clen < 4:
      raise newException(
        PgTypeError,
        "Column " & $col & ": binary polygon header too short (" & $clen & " bytes)",
      )
    let b = row.data.buf
    let npts = fromBE32(b.toOpenArray(off, off + 3))
    if npts < 0:
      raise newException(
        PgTypeError,
        "Column " & $col & ": binary polygon has negative point count " & $npts,
      )
    if npts > (clen - 4) div 16:
      raise newException(
        PgTypeError,
        "Column " & $col & ": binary polygon " & $npts & " points exceed " & $clen &
          "-byte cell",
      )
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
  let radius = pgParseFloat(inner[lastComma + 1 ..^ 1])
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
  ## Auto-detects a ``scale`` parameter on the index-based proc and forwards it —
  ## so a scaled accessor like ``getMoney`` cannot silently drop the kwarg when
  ## its index-based signature grows one.
  when compiles((var r: Row; discard r.getProc(0, scale = 2))):
    proc getProc*(row: Row, name: string, scale: int = 2): T =
      row.getProc(row.columnIndex(name), scale)

  else:
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

# Shared array element decoder registry — 1-D and N-D accessors route here.

proc decodePgArrayElement*(_: typedesc[int16], buf: openArray[byte]): int16 =
  if buf.len != 2:
    raise newException(PgTypeError, "int2 array element: bad length " & $buf.len)
  fromBE16(buf)

proc decodePgArrayElement*(_: typedesc[int32], buf: openArray[byte]): int32 =
  if buf.len != 4:
    raise newException(PgTypeError, "int4 array element: bad length " & $buf.len)
  fromBE32(buf)

proc decodePgArrayElement*(_: typedesc[int64], buf: openArray[byte]): int64 =
  if buf.len != 8:
    raise newException(PgTypeError, "int8 array element: bad length " & $buf.len)
  fromBE64(buf)

proc decodePgArrayElement*(_: typedesc[float32], buf: openArray[byte]): float32 =
  if buf.len != 4:
    raise newException(PgTypeError, "float4 array element: bad length " & $buf.len)
  decodeFloat32BE(buf)

proc decodePgArrayElement*(_: typedesc[float64], buf: openArray[byte]): float64 =
  if buf.len != 8:
    raise newException(PgTypeError, "float8 array element: bad length " & $buf.len)
  decodeFloat64BE(buf)

proc decodePgArrayElement*(_: typedesc[bool], buf: openArray[byte]): bool =
  if buf.len != 1:
    raise newException(PgTypeError, "bool array element: bad length " & $buf.len)
  buf[0] != 0'u8

proc decodePgArrayElement*(_: typedesc[string], buf: openArray[byte]): string =
  readString(buf, 0, buf.len)

proc decodePgArrayElement*(_: typedesc[seq[byte]], buf: openArray[byte]): seq[byte] =
  readBytes(buf, 0, buf.len)

proc decodePgArrayElement*(_: typedesc[PgNumeric], buf: openArray[byte]): PgNumeric =
  decodeNumericBinary(buf)

# No PgMoney overload: binary money lacks scale; callers must supply it.

proc decodePgArrayElement*(_: typedesc[PgBit], buf: openArray[byte]): PgBit =
  if buf.len < 4:
    raise newException(PgTypeError, "bit array element too short")
  let nbits = fromBE32(buf.toOpenArray(0, 3))
  if nbits < 0:
    raise newException(PgTypeError, "bit array element: negative nbits " & $nbits)
  if nbits > PgBitMaxBits:
    raise newException(
      PgTypeError,
      "bit array element: nbits " & $nbits & " exceeds limit (" & $PgBitMaxBits & ")",
    )
  let dataLen = buf.len - 4
  if (int64(nbits) + 7) div 8 != int64(dataLen):
    raise newException(
      PgTypeError,
      "bit array element: nbits=" & $nbits & " inconsistent with dataLen=" & $dataLen,
    )
  var data = newSeq[byte](dataLen)
  for j in 0 ..< dataLen:
    data[j] = buf[4 + j]
  PgBit(nbits: nbits, data: data)

proc decodePgArrayElement*(_: typedesc[PgTime], buf: openArray[byte]): PgTime =
  if buf.len != 8:
    raise newException(PgTypeError, "time array element: bad length " & $buf.len)
  decodeBinaryTime(buf)

proc decodePgArrayElement*(_: typedesc[PgTimeTz], buf: openArray[byte]): PgTimeTz =
  if buf.len != 12:
    raise newException(PgTypeError, "timetz array element: bad length " & $buf.len)
  decodeBinaryTimeTz(buf)

proc decodePgArrayElement*[T: PgInet | PgCidr](
    _: typedesc[T], buf: openArray[byte]
): T =
  let (ip, mask) = decodeInetBinary(buf)
  T(address: ip, mask: mask)

proc decodePgArrayElement*[T: PgXml | PgTsVector | PgTsQuery](
    _: typedesc[T], buf: openArray[byte]
): T =
  T(readString(buf, 0, buf.len))

proc decodePgArrayElement*(_: typedesc[PgHstore], buf: openArray[byte]): PgHstore =
  decodeHstoreBinary(buf)

proc decodePgArrayElement*(_: typedesc[PgPoint], buf: openArray[byte]): PgPoint =
  if buf.len != 16:
    raise newException(PgTypeError, "point array element: bad length " & $buf.len)
  decodePointBinary(buf, 0)

proc decodePgArrayElement*(_: typedesc[PgLine], buf: openArray[byte]): PgLine =
  if buf.len != 24:
    raise newException(PgTypeError, "line array element: bad length " & $buf.len)
  result.a = decodeFloat64BE(buf, 0)
  result.b = decodeFloat64BE(buf, 8)
  result.c = decodeFloat64BE(buf, 16)

proc decodePgArrayElement*(_: typedesc[PgLseg], buf: openArray[byte]): PgLseg =
  if buf.len != 32:
    raise newException(PgTypeError, "lseg array element: bad length " & $buf.len)
  PgLseg(p1: decodePointBinary(buf, 0), p2: decodePointBinary(buf, 16))

proc decodePgArrayElement*(_: typedesc[PgBox], buf: openArray[byte]): PgBox =
  if buf.len != 32:
    raise newException(PgTypeError, "box array element: bad length " & $buf.len)
  PgBox(high: decodePointBinary(buf, 0), low: decodePointBinary(buf, 16))

proc decodePgArrayElement*(_: typedesc[PgPath], buf: openArray[byte]): PgPath =
  if buf.len < 5:
    raise newException(PgTypeError, "path array element too short: " & $buf.len)
  result.closed = buf[0] != 0
  let npts = fromBE32(buf.toOpenArray(1, 4))
  if npts < 0 or int64(buf.len) != 5 + int64(npts) * 16:
    raise newException(
      PgTypeError,
      "path array element: bad npts " & $npts & " (buf.len " & $buf.len & ")",
    )
  result.points = newSeq[PgPoint](npts)
  for j in 0 ..< npts:
    result.points[j] = decodePointBinary(buf, 5 + j * 16)

proc decodePgArrayElement*(_: typedesc[PgPolygon], buf: openArray[byte]): PgPolygon =
  if buf.len < 4:
    raise newException(PgTypeError, "polygon array element too short: " & $buf.len)
  let npts = fromBE32(buf.toOpenArray(0, 3))
  if npts < 0 or int64(buf.len) != 4 + int64(npts) * 16:
    raise newException(
      PgTypeError,
      "polygon array element: bad npts " & $npts & " (buf.len " & $buf.len & ")",
    )
  result.points = newSeq[PgPoint](npts)
  for j in 0 ..< npts:
    result.points[j] = decodePointBinary(buf, 4 + j * 16)

proc decodePgArrayElement*(_: typedesc[PgCircle], buf: openArray[byte]): PgCircle =
  if buf.len != 24:
    raise newException(PgTypeError, "circle array element: bad length " & $buf.len)
  result.center = decodePointBinary(buf, 0)
  result.radius = decodeFloat64BE(buf, 16)

# Named helpers where typedesc dispatch can't distinguish: DateTime is shared
# by timestamp/timestamptz/date; JsonNode needs runtime elemOid.

proc decodeTimestampArrayElem*(
    buf: openArray[byte], typeName: static string
): DateTime =
  if buf.len != 8:
    raise newException(
      PgTypeError, "Invalid binary " & typeName & " element length: " & $buf.len
    )
  decodeBinaryTimestamp(buf)

proc decodeDateArrayElem*(buf: openArray[byte]): DateTime =
  if buf.len != 4:
    raise newException(PgTypeError, "Invalid binary date element length: " & $buf.len)
  decodeBinaryDate(buf)

# ``decodeJsonArrayElem`` is defined above (near the scalar accessors) so
# ``getJson`` can delegate to it without a forward declaration.

# Array decoder skeletons. ``genArrayDecoder`` hardcodes the binary body to
# ``decodePgArrayElement(T, slice)``; ``genArrayDecoderCustom`` takes an
# explicit ``binBody`` for types that need extra context (json/timestamps).
# In both, ``textBody`` decodes one text element with ``e: Option[string]``
# in scope; ``binBody`` has ``row``/``off``/``e``/``decoded`` in scope.
template genArrayDecoderCustom(
    getProc: untyped, T: typedesc, typeName: static string, binBody, textBody: untyped
) {.dirty.} =
  proc getProc*(row: Row, col: int): seq[T] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      rejectMultiDim(decoded)
      result = newSeq[T](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in " & typeName & " array")
        result[i] = binBody
      return
    for e in parseTextArray(row.getStr(col)):
      if e.isNone:
        raise newException(PgTypeError, "NULL element in " & typeName & " array")
      result.add(textBody)

template genArrayDecoder(
    getProc: untyped, T: typedesc, typeName: static string, textBody: untyped
) {.dirty.} =
  genArrayDecoderCustom(
    getProc,
    T,
    typeName,
    decodePgArrayElement(
      T, row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
    ),
    textBody,
  )

# Scalar array decoders.

genArrayDecoder(getIntArray, int32, "int", pgParseInt32(e.get))
genArrayDecoder(getInt16Array, int16, "int16", pgParseInt16(e.get))
genArrayDecoder(getInt64Array, int64, "int64", pgParseBiggestInt(e.get))

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
    rejectMultiDim(decoded)
    result = newSeq[PgMoney](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in money array")
      if e.len != 8:
        raise newException(
          PgTypeError, "Unexpected binary element length " & $e.len & " for money array"
        )
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

# ``getFloatArray`` decodes ``float8[]`` only; ``float4[]`` raises PgTypeError.
genArrayDecoder(getFloatArray, float64, "float", pgParseFloat(e.get))
genArrayDecoder(getFloat32Array, float32, "float32", pgParseFloat32(e.get))

proc boolElemFromText(s: string): bool =
  case s
  of "t", "true", "1":
    true
  of "f", "false", "0":
    false
  else:
    raise newException(PgTypeError, "Invalid boolean: " & s)

genArrayDecoder(getBoolArray, bool, "bool", boolElemFromText(e.get))
genArrayDecoder(getStrArray, string, "string", e.get)
genArrayDecoder(getBitArray, PgBit, "bit", parseBitString(e.get))

# Temporal array decoders

genArrayDecoderCustom(
  getTimestampArray,
  DateTime,
  "timestamp",
  decodeTimestampArrayElem(
    row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1), "timestamp"
  ),
  parseTimestampText(e.get),
)
genArrayDecoderCustom(
  getTimestampTzArray,
  DateTime,
  "timestamptz",
  decodeTimestampArrayElem(
    row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1), "timestamptz"
  ),
  parseTimestampText(e.get),
)

genArrayDecoderCustom(
  getDateArray,
  DateTime,
  "date",
  decodeDateArrayElem(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)),
  parseDateText(e.get),
)

genArrayDecoder(getTimeArray, PgTime, "time", parseTimeText(e.get))
genArrayDecoder(getTimeTzArray, PgTimeTz, "timetz", parseTimeTzText(e.get))
genArrayDecoder(getIntervalArray, PgInterval, "interval", parseIntervalText(e.get))

# Identifier / network array decoders

genArrayDecoder(getUuidArray, PgUuid, "uuid", PgUuid(e.get))

proc inetElemFromText[T](s: string): T =
  let (ip, mask) = parseInetText(s)
  result.address = ip
  result.mask = mask

genArrayDecoder(getInetArray, PgInet, "inet", inetElemFromText[PgInet](e.get))
genArrayDecoder(getCidrArray, PgCidr, "cidr", inetElemFromText[PgCidr](e.get))
genArrayDecoder(getMacAddrArray, PgMacAddr, "macaddr", PgMacAddr(e.get))
genArrayDecoder(getMacAddr8Array, PgMacAddr8, "macaddr8", PgMacAddr8(e.get))

# Numeric / binary / JSON array decoders

genArrayDecoder(getNumericArray, PgNumeric, "numeric", parsePgNumeric(e.get))

proc bytesElemFromText(s: string): seq[byte] =
  if s.len >= 2 and s[0] == '\\' and s[1] == 'x':
    let hexLen = s.len - 2
    if hexLen mod 2 != 0:
      raise newException(PgTypeError, "odd-length hex in bytea array element: " & s)
    const errCtx = "bytea array element"
    result = newSeq[byte](hexLen div 2)
    for j in 0 ..< result.len:
      result[j] = decodeHexPair(s, 2 + j * 2, errCtx)
  else:
    result = toBytes(s)

genArrayDecoder(getBytesArray, seq[byte], "bytea", bytesElemFromText(e.get))

proc jsonElemFromText(s: string): JsonNode =
  try:
    parseJson(s)
  except JsonParsingError:
    raise newException(PgTypeError, "Invalid JSON element: " & s)

genArrayDecoderCustom(
  getJsonArray,
  JsonNode,
  "json",
  decodeJsonArrayElem(
    row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1), decoded.elemOid
  ),
  jsonElemFromText(e.get),
)

# Geometric array decoders

genArrayDecoder(getPointArray, PgPoint, "point", parsePointText(e.get))

proc lineElemFromText(s: string): PgLine =
  let v = s.strip()
  var inner = v
  if inner.len >= 2 and inner[0] == '{' and inner[^1] == '}':
    inner = inner[1 ..^ 2]
  else:
    raise newException(PgTypeError, "Invalid line: " & v)
  let parts = inner.split(',')
  if parts.len != 3:
    raise newException(PgTypeError, "Invalid line: " & v)
  PgLine(
    a: pgParseFloat(parts[0]), b: pgParseFloat(parts[1]), c: pgParseFloat(parts[2])
  )

genArrayDecoder(getLineArray, PgLine, "line", lineElemFromText(e.get))

proc lsegElemFromText(s: string): PgLseg =
  let v = s.strip()
  var inner = v
  if inner.len >= 2 and inner[0] == '[' and inner[^1] == ']':
    inner = inner[1 ..^ 2]
  let points = parsePointsText(inner)
  if points.len != 2:
    raise newException(PgTypeError, "Invalid lseg: " & v)
  PgLseg(p1: points[0], p2: points[1])

genArrayDecoder(getLsegArray, PgLseg, "lseg", lsegElemFromText(e.get))

proc getBoxArray*(row: Row, col: int): seq[PgBox] =
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    rejectMultiDim(decoded)
    result = newSeq[PgBox](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in box array")
      result[i] = decodePgArrayElement(
        PgBox, row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
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

proc pathElemFromText(s: string): PgPath =
  let v = s.strip()
  if v.len < 2:
    raise newException(PgTypeError, "Invalid path: " & v)
  let closed = v[0] == '('
  let inner = v[1 ..^ 2]
  PgPath(closed: closed, points: parsePointsText(inner))

genArrayDecoder(getPathArray, PgPath, "path", pathElemFromText(e.get))

proc polygonElemFromText(s: string): PgPolygon =
  let v = s.strip()
  if v.len < 2 or v[0] != '(' or v[^1] != ')':
    raise newException(PgTypeError, "Invalid polygon: " & v)
  PgPolygon(points: parsePointsText(v[1 ..^ 2]))

genArrayDecoder(getPolygonArray, PgPolygon, "polygon", polygonElemFromText(e.get))

proc circleElemFromText(s: string): PgCircle =
  let v = s.strip()
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
  PgCircle(
    center: parsePointText(inner[0 ..< lastComma]),
    radius: pgParseFloat(inner[lastComma + 1 ..^ 1]),
  )

genArrayDecoder(getCircleArray, PgCircle, "circle", circleElemFromText(e.get))

# Other array decoders

genArrayDecoder(getXmlArray, PgXml, "xml", PgXml(e.get))
genArrayDecoder(getTsVectorArray, PgTsVector, "tsvector", PgTsVector(e.get))
genArrayDecoder(getTsQueryArray, PgTsQuery, "tsquery", PgTsQuery(e.get))
genArrayDecoder(getHstoreArray, PgHstore, "hstore", parseHstoreText(e.get))

# Element-level NULL-safe array getters

# Like ``genArrayDecoder`` but yields ``seq[Option[T]]``: a NULL element maps
# to ``none(T)`` instead of raising. Short form uses ``decodePgArrayElement``;
# ``…Custom`` takes an explicit ``binBody``.
template genArrayDecoderElemOptCustom(
    getProc: untyped, T: typedesc, binBody, textBody: untyped
) {.dirty.} =
  proc getProc*(row: Row, col: int): seq[Option[T]] =
    if row.isBinaryCol(col):
      let (off, clen) = cellInfo(row, col)
      if clen == -1:
        raise newException(PgTypeError, "Column " & $col & " is NULL")
      let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
      rejectMultiDim(decoded)
      result = newSeq[Option[T]](decoded.elements.len)
      for i, e in decoded.elements:
        if e.len == -1:
          result[i] = none(T)
        else:
          result[i] = some(binBody)
      return
    for e in parseTextArray(row.getStr(col)):
      if e.isNone:
        result.add(none(T))
      else:
        result.add(some(textBody))

template genArrayDecoderElemOpt(
    getProc: untyped, T: typedesc, textBody: untyped
) {.dirty.} =
  genArrayDecoderElemOptCustom(
    getProc,
    T,
    decodePgArrayElement(
      T, row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
    ),
    textBody,
  )

genArrayDecoderElemOpt(getIntArrayElemOpt, int32, pgParseInt32(e.get))
genArrayDecoderElemOpt(getInt16ArrayElemOpt, int16, pgParseInt16(e.get))
genArrayDecoderElemOpt(getInt64ArrayElemOpt, int64, pgParseBiggestInt(e.get))
genArrayDecoderElemOpt(getFloatArrayElemOpt, float64, pgParseFloat(e.get))
genArrayDecoderElemOpt(getFloat32ArrayElemOpt, float32, pgParseFloat32(e.get))
genArrayDecoderElemOpt(getBoolArrayElemOpt, bool, boolElemFromText(e.get))
genArrayDecoderElemOpt(getStrArrayElemOpt, string, e.get)

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

# N-dimensional array accessor. Element decoding routes through the shared
# decodePgArrayElement registry.

proc getArrayND*[T](row: Row, col: int): PgArray[T] =
  ## Read an N-dimensional PostgreSQL array column as a ``PgArray[T]``.
  ## Requires binary column format; text-format multi-dimensional arrays are
  ## not supported. Raises ``PgTypeError`` when the column is NULL, or when
  ## the wire ``elemOid`` does not match the registered OID for ``T``
  ## (``JsonNode`` accepts both ``json`` and ``jsonb``).
  ##
  ## Validation looks only at the wire ``elemOid`` carried in the array
  ## payload, not at the column's field OID from ``RowDescription``. A bind
  ## mismatch (e.g. reading a ``text[]`` column as ``getArrayND[int32]``) is
  ## caught by the elemOid check; reading e.g. ``int4[]`` via
  ## ``getArrayND[int32]`` against a column declared as ``int8[]`` succeeds
  ## as long as the wire bytes match the requested ``T``. For ``JsonNode``,
  ## the leading jsonb version byte is stripped only when ``elemOid == jsonb``;
  ## ``json`` payloads pass through unchanged, and any other wire ``elemOid``
  ## (e.g. ``int4``) is rejected by the OID check so calling
  ## ``getArrayND[JsonNode]`` on a non-JSON column raises rather than
  ## silently misinterpreting the bytes.
  when T is DateTime:
    {.
      error:
        "getArrayND[DateTime] is ambiguous (timestamp / timestamptz / date " &
        "have distinct OIDs). Use the dedicated 1-D accessors " &
        "(getTimestampArray / getTimestampTzArray / getDateArray) instead."
    .}
  elif T is seq[byte]:
    {.
      error:
        "getArrayND[seq[byte]] is not supported by the PgArray registry. " &
        "Use getByteaArray instead."
    .}
  elif T is int:
    {.
      error:
        "getArrayND[int] is not supported (platform-dependent width). " &
        "Use getArrayND[int32] or getArrayND[int64] explicitly."
    .}
  elif T is PgMoney:
    {.
      error:
        "getArrayND[PgMoney] would silently hardcode scale=2 and produce " &
        "wrong values on servers whose lc_monetary frac_digits differ from " &
        "2. Use getMoneyArrayND(row, col, scale = ...) instead " &
        "(getMoneyArrayNDOpt for the NULL-safe variant)."
    .}
  elif T is PgTsVector or T is PgTsQuery:
    {.
      error:
        "getArrayND[PgTsVector] / getArrayND[PgTsQuery] is not supported by " &
        "the PgArray registry: the binary wire format for tsvector / " &
        "tsquery is structured (not the text representation), so element " &
        "round-trip would not match what PostgreSQL sends. Use the 1-D " &
        "accessor (getTsVectorArray / getTsQueryArray) instead; " &
        "PgArray[T] currently has no multi-dim equivalent for these types."
    .}
  if not row.isBinaryCol(col):
    raise newException(
      PgTypeError, "getArrayND requires binary column format (col " & $col & ")"
    )
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
  when T is JsonNode:
    if decoded.elemOid != OidJson and decoded.elemOid != OidJsonb:
      raise newException(
        PgTypeError,
        "getArrayND[JsonNode]: wire elemOid=" & $decoded.elemOid &
          " is neither json nor jsonb",
      )
  else:
    if decoded.elemOid != pgArrayElemOid(T):
      raise newException(
        PgTypeError,
        "getArrayND[" & $T & "]: wire elemOid=" & $decoded.elemOid & " expected " &
          $pgArrayElemOid(T),
      )
  result.dims = decoded.dims
  result.lowerBounds = decoded.lowerBounds
  result.elements = newSeq[Option[T]](decoded.elements.len)
  for i, e in decoded.elements:
    if e.len == -1:
      result.elements[i] = none(T)
    else:
      when T is JsonNode:
        # Strip the leading jsonb version byte only when the wire elemOid
        # actually says jsonb. Plain ``json`` payloads are forwarded as-is.
        result.elements[i] = some(
          decodeJsonArrayElem(
            row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1),
            decoded.elemOid,
          )
        )
      else:
        result.elements[i] = some(
          decodePgArrayElement(
            T, row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)
          )
        )

proc getArrayNDOpt*[T](row: Row, col: int): Option[PgArray[T]] =
  ## NULL-safe column-level variant of ``getArrayND[T]``.
  if row.isNull(col):
    none(PgArray[T])
  else:
    some(getArrayND[T](row, col))

proc getMoneyArrayND*(row: Row, col: int, scale: int = 2): PgArray[PgMoney] =
  ## ``getArrayND``-style accessor for ``money[]`` (any dimensionality).
  ## ``getArrayND[PgMoney]`` is intentionally ``{.error.}``-gated because the
  ## binary ``money`` wire format does not carry the fractional-digit count,
  ## so the caller must supply ``scale`` (matching the server's
  ## ``lc_monetary`` ``frac_digits``) — this accessor is the only way to
  ## read a ``money[]`` column. Defaults to ``scale = 2`` for the common
  ## locale. Raises ``PgTypeError`` when ``scale`` is outside ``0..18``.
  if scale < 0 or scale > 18:
    raise newException(PgTypeError, "PgMoney scale out of range: " & $scale)
  if not row.isBinaryCol(col):
    raise newException(
      PgTypeError, "getMoneyArrayND requires binary column format (col " & $col & ")"
    )
  let (off, clen) = cellInfo(row, col)
  if clen == -1:
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
  if decoded.elemOid != pgArrayElemOid(PgMoney):
    raise newException(
      PgTypeError,
      "getMoneyArrayND: wire elemOid=" & $decoded.elemOid & " expected " &
        $pgArrayElemOid(PgMoney),
    )
  result.dims = decoded.dims
  result.lowerBounds = decoded.lowerBounds
  result.elements = newSeq[Option[PgMoney]](decoded.elements.len)
  for i, e in decoded.elements:
    if e.len == -1:
      result.elements[i] = none(PgMoney)
    else:
      if e.len != 8:
        raise newException(
          PgTypeError, "Unexpected binary element length " & $e.len & " for money array"
        )
      result.elements[i] = some(
        PgMoney(
          amount:
            fromBE64(row.data.buf.toOpenArray(off + e.off, off + e.off + e.len - 1)),
          scale: int8(scale),
        )
      )

proc getMoneyArrayNDOpt*(row: Row, col: int, scale: int = 2): Option[PgArray[PgMoney]] =
  ## NULL-safe column-level variant of ``getMoneyArrayND``.
  if row.isNull(col):
    none(PgArray[PgMoney])
  else:
    some(getMoneyArrayND(row, col, scale))

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

proc get*(row: Row, col: int, T: typedesc[PgTime]): PgTime =
  row.getTime(col)

proc get*(row: Row, col: int, T: typedesc[PgTimeTz]): PgTimeTz =
  row.getTimeTz(col)

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

proc get*(row: Row, col: int, T: typedesc[seq[seq[byte]]]): seq[seq[byte]] =
  row.getBytesArray(col)

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

# Per-element Option array types

proc get*(row: Row, col: int, T: typedesc[seq[Option[int16]]]): seq[Option[int16]] =
  row.getInt16ArrayElemOpt(col)

proc get*(row: Row, col: int, T: typedesc[seq[Option[int32]]]): seq[Option[int32]] =
  row.getIntArrayElemOpt(col)

proc get*(row: Row, col: int, T: typedesc[seq[Option[int64]]]): seq[Option[int64]] =
  row.getInt64ArrayElemOpt(col)

proc get*(row: Row, col: int, T: typedesc[seq[Option[float32]]]): seq[Option[float32]] =
  row.getFloat32ArrayElemOpt(col)

proc get*(row: Row, col: int, T: typedesc[seq[Option[float64]]]): seq[Option[float64]] =
  row.getFloatArrayElemOpt(col)

proc get*(row: Row, col: int, T: typedesc[seq[Option[bool]]]): seq[Option[bool]] =
  row.getBoolArrayElemOpt(col)

proc get*(row: Row, col: int, T: typedesc[seq[Option[string]]]): seq[Option[string]] =
  row.getStrArrayElemOpt(col)

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
