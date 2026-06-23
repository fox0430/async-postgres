import std/[options, json, macros, strutils, tables, times, net, math]

import ../[pg_bytes, pg_protocol]
import core, array

export pg_bytes, array

proc toPgParamInline*(v: int16): PgParamInline =
  result.oid = OidInt2
  result.format = 1
  result.len = 2
  result.inlineBuf.writeBE16(0, v)

proc toPgParamInline*(v: int32): PgParamInline =
  result.oid = OidInt4
  result.format = 1
  result.len = 4
  result.inlineBuf.writeBE32(0, v)

proc toPgParamInline*(v: int64): PgParamInline =
  result.oid = OidInt8
  result.format = 1
  result.len = 8
  result.inlineBuf.writeBE64(0, v)

proc toPgParamInline*(v: int): PgParamInline =
  toPgParamInline(int64(v))

proc toPgParamInline*(v: float32): PgParamInline =
  result.oid = OidFloat4
  result.format = 1
  result.len = 4
  result.inlineBuf.writeBE32(0, cast[int32](v))

proc toPgParamInline*(v: float64): PgParamInline =
  result.oid = OidFloat8
  result.format = 1
  result.len = 8
  result.inlineBuf.writeBE64(0, cast[int64](v))

proc toPgParamInline*(v: bool): PgParamInline =
  result.oid = OidBool
  result.format = 1
  result.len = 1
  result.inlineBuf[0] = if v: 1'u8 else: 0'u8

proc toPgParamInline*(v: string): PgParamInline =
  result.oid = OidText
  result.format = 0
  result.len = int32(v.len)
  if v.len == 0:
    discard
  elif v.len <= PgInlineBufSize:
    result.inlineBuf.writeBytesAt(0, v.toOpenArrayByte(0, v.high))
  else:
    result.overflow = newSeq[byte](v.len)
    result.overflow.writeBytesAt(0, v.toOpenArrayByte(0, v.high))

proc toPgParamInline*(v: seq[byte]): PgParamInline =
  result.oid = OidBytea
  result.format = 0
  result.len = int32(v.len)
  if v.len == 0:
    discard
  elif v.len <= PgInlineBufSize:
    result.inlineBuf.writeBytesAt(0, v)
  else:
    result.overflow = v

proc toPgParamInline*(v: PgUuid): PgParamInline =
  # Text format with OidUuid (matches toPgParam). UUID canonical string is
  # 36 bytes, so the payload always takes the overflow path.
  let s = string(v)
  result.oid = OidUuid
  result.format = 0
  result.len = int32(s.len)
  if s.len == 0:
    discard
  elif s.len <= PgInlineBufSize:
    result.inlineBuf.writeBytesAt(0, s.toOpenArrayByte(0, s.high))
  else:
    result.overflow = newSeq[byte](s.len)
    result.overflow.writeBytesAt(0, s.toOpenArrayByte(0, s.high))

proc toPgParamInline*(v: PgMoney): PgParamInline =
  result.oid = OidMoney
  result.format = 1
  result.len = 8
  result.inlineBuf.writeBE64(0, v.amount)

proc toPgParamInline*[T](v: Option[T]): PgParamInline =
  if v.isSome:
    toPgParamInline(v.get)
  else:
    let tmpl = toPgParamInline(default(T))
    PgParamInline(oid: tmpl.oid, format: tmpl.format, len: -1)

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

proc toPgDateParam*(v: DateTime): PgParam =
  ## Encode a DateTime as a date parameter (OID 1082).
  let s = v.format("yyyy-MM-dd")
  PgParam(oid: OidDate, format: 0, value: some(toBytes(s)))

proc toPgTimestampTzParam*(v: DateTime): PgParam =
  ## Encode a DateTime as a timestamptz parameter (OID 1184).
  let s = v.format("yyyy-MM-dd HH:mm:ss'.'ffffffzzz")
  PgParam(oid: OidTimestampTz, format: 0, value: some(toBytes(s)))

proc toPgParam*(v: PgTime): PgParam =
  PgParam(oid: OidTime, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgTimeTz): PgParam =
  PgParam(oid: OidTimeTz, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgUuid): PgParam =
  PgParam(oid: OidUuid, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: PgNumeric): PgParam =
  PgParam(oid: OidNumeric, format: 0, value: some(toBytes($v)))

proc toPgParam*(v: PgMoney): PgParam =
  ## Uses binary format: money's text representation is ``lc_monetary``-dependent,
  ## so a text round-trip is not reliable. Binary sends the raw int64 amount.
  PgParam(oid: OidMoney, format: 1, value: some(@(toBE64(v.amount))))

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

proc toPgParam*(v: PgTsVector): PgParam =
  PgParam(oid: OidTsVector, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: PgTsQuery): PgParam =
  PgParam(oid: OidTsQuery, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: PgXml): PgParam =
  PgParam(oid: OidXml, format: 0, value: some(toBytes(string(v))))

proc toPgParam*(v: PgBit): PgParam =
  PgParam(oid: OidVarbit, format: 0, value: some(toBytes($v)))

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

proc encodeHstoreText*(v: PgHstore): string =
  ## Encode hstore as PostgreSQL text format: ``"key1"=>"val1", "key2"=>NULL``.
  var parts: seq[string]
  for k, v in v.pairs:
    var keyEsc = newStringOfCap(k.len + 2)
    keyEsc.add('"')
    for c in k:
      if c == '"' or c == '\\':
        keyEsc.add('\\')
      keyEsc.add(c)
    keyEsc.add('"')
    if v.isSome:
      var valEsc = newStringOfCap(v.get.len + 2)
      valEsc.add('"')
      for c in v.get:
        if c == '"' or c == '\\':
          valEsc.add('\\')
        valEsc.add(c)
      valEsc.add('"')
      parts.add(keyEsc & "=>" & valEsc)
    else:
      parts.add(keyEsc & "=>NULL")
  parts.join(", ")

proc toPgParam*(v: PgHstore): PgParam =
  ## Send hstore as text format. PostgreSQL casts text to hstore implicitly.
  PgParam(oid: OidText, format: 0, value: some(toBytes(encodeHstoreText(v))))

proc encodeBinaryArray*(
    elemOid: int32,
    dims: openArray[int32],
    lowerBounds: openArray[int32],
    elements: openArray[Option[seq[byte]]],
): seq[byte] =
  ## Encode an N-dimensional PostgreSQL binary array.
  ##
  ## Header: ``ndim(4) + has_null(4) + elem_oid(4) + per-dim (dim_len(4) +
  ## lower_bound(4))``. Each element is encoded as ``len(4) + data`` in
  ## row-major order; a NULL element is encoded with ``len = -1`` and no
  ## payload. ``has_null`` is set to ``1`` iff any element is ``none``.
  ##
  ## When ``dims.len == 0`` an empty array is emitted (``ndim=0``, 12-byte
  ## header, no elements); ``elements`` must be empty in that case.
  ## Otherwise ``elements.len`` must equal ``product(dims)``. See
  ## ``validatePgArrayShape`` in ``array.nim`` for the validation rules.
  validatePgArrayShape(dims, lowerBounds, elements.len)
  let headerSize = 12 + 8 * dims.len
  var dataSize = 0
  var anyNull = false
  for e in elements:
    if e.isNone:
      anyNull = true
      dataSize += 4
    else:
      let ev = e.get
      if ev.len > int32.high.int:
        raise
          newException(PgError, "Array element too large for PostgreSQL binary format")
      dataSize += 4 + ev.len
    # Cap the cumulative payload at int32.high so a crafted input with many
    # large elements cannot make ``headerSize + dataSize`` wrap negative
    # before the ``newSeq`` allocation. Per-element ``ev.len`` is already
    # bounded above; this guards the sum.
    if dataSize > int32.high.int:
      raise
        newException(PgError, "Array payload too large for PostgreSQL binary format")
  result = newSeq[byte](headerSize + dataSize)
  result.writeBE32(0, int32(dims.len)) # ndim
  result.writeBE32(4, if anyNull: 1'i32 else: 0'i32) # has_null
  result.writeBE32(8, elemOid) # elem_oid
  var pos = 12
  for i in 0 ..< dims.len:
    result.writeBE32(pos, dims[i])
    result.writeBE32(pos + 4, lowerBounds[i])
    pos += 8
  for e in elements:
    if e.isNone:
      result.writeBE32(pos, -1'i32)
      pos += 4
    else:
      let ev = e.get
      result.writeBE32(pos, int32(ev.len))
      pos += 4
      result.writeBytesAt(pos, ev)
      pos += ev.len

proc encodeBinaryArray*(
    elemOid: int32, dims: openArray[int32], elements: openArray[Option[seq[byte]]]
): seq[byte] =
  ## Convenience overload: ``lowerBounds`` defaults to ``1`` for each
  ## dimension (PostgreSQL's default).
  var lb = newSeq[int32](dims.len)
  for i in 0 ..< dims.len:
    lb[i] = 1
  encodeBinaryArray(elemOid, dims, lb, elements)

proc dimsFor1D*(n: int): seq[int32] {.inline.} =
  ## Helper used by 1D ``toPgParam`` wrappers: returns ``@[]`` when ``n == 0``
  ## (so ``encodeBinaryArray`` emits the ``ndim=0`` wire form) and
  ## ``@[int32(n)]`` otherwise. Exported because external array encoders
  ## (``ranges.nim`` etc.) share the same single-dimension shortcut. Raises
  ## ``PgTypeError`` when ``n`` exceeds ``int32.high`` — without this guard
  ## the ``int32(n)`` cast on 64-bit platforms would silently wrap and the
  ## resulting ``dims[0]`` would disagree with ``elements.len``.
  if n < 0:
    raise newException(PgTypeError, "dimsFor1D: negative length " & $n)
  if n > int32.high.int:
    raise newException(
      PgTypeError, "Array has too many elements for PostgreSQL binary format: " & $n
    )
  if n == 0:
    newSeq[int32]()
  else:
    @[int32(n)]

proc toPgParam*(v: seq[int16]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(@(toBE16(x)))
  PgParam(
    oid: OidInt2Array,
    format: 1,
    value: some(encodeBinaryArray(OidInt2, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[int32]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(@(toBE32(x)))
  PgParam(
    oid: OidInt4Array,
    format: 1,
    value: some(encodeBinaryArray(OidInt4, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[int64]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(@(toBE64(x)))
  PgParam(
    oid: OidInt8Array,
    format: 1,
    value: some(encodeBinaryArray(OidInt8, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[float32]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(@(toBE32(cast[int32](x))))
  PgParam(
    oid: OidFloat4Array,
    format: 1,
    value: some(encodeBinaryArray(OidFloat4, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[float64]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(@(toBE64(cast[int64](x))))
  PgParam(
    oid: OidFloat8Array,
    format: 1,
    value: some(encodeBinaryArray(OidFloat8, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[bool]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(@[if x: 1'u8 else: 0'u8])
  PgParam(
    oid: OidBoolArray,
    format: 1,
    value: some(encodeBinaryArray(OidBool, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[string]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(toBytes(x))
  PgParam(
    oid: OidTextArray,
    format: 1,
    value: some(encodeBinaryArray(OidText, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[int16]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE16(x.get)))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt2Array,
    format: 1,
    value: some(encodeBinaryArray(OidInt2, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[int32]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE32(x.get)))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt4Array,
    format: 1,
    value: some(encodeBinaryArray(OidInt4, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[int64]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE64(x.get)))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt8Array,
    format: 1,
    value: some(encodeBinaryArray(OidInt8, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[int]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE64(int64(x.get))))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt8Array,
    format: 1,
    value: some(encodeBinaryArray(OidInt8, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[float32]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE32(cast[int32](x.get))))
      else:
        none(seq[byte])
  PgParam(
    oid: OidFloat4Array,
    format: 1,
    value: some(encodeBinaryArray(OidFloat4, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[float64]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE64(cast[int64](x.get))))
      else:
        none(seq[byte])
  PgParam(
    oid: OidFloat8Array,
    format: 1,
    value: some(encodeBinaryArray(OidFloat8, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[bool]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@[if x.get: 1'u8 else: 0'u8])
      else:
        none(seq[byte])
  PgParam(
    oid: OidBoolArray,
    format: 1,
    value: some(encodeBinaryArray(OidBool, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[Option[string]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(toBytes(x.get))
      else:
        none(seq[byte])
  PgParam(
    oid: OidTextArray,
    format: 1,
    value: some(encodeBinaryArray(OidText, dimsFor1D(v.len), elements)),
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

proc toPgBinaryDateParam*(v: DateTime): PgParam =
  ## Encode a DateTime as a binary date parameter (OID 1082).
  let t = v.toTime()
  let pgDays = int32(floorDiv(t.toUnix(), 86400'i64) - int64(pgEpochDaysOffset))
  PgParam(oid: OidDate, format: 1, value: some(@(toBE32(pgDays))))

proc toPgBinaryTimestampTzParam*(v: DateTime): PgParam =
  ## Encode a DateTime as a binary timestamptz parameter (OID 1184).
  let t = v.toTime()
  let unixUs = t.toUnix() * 1_000_000 + int64(t.nanosecond div 1000)
  let pgUs = unixUs - pgEpochUnix * 1_000_000
  PgParam(oid: OidTimestampTz, format: 1, value: some(@(toBE64(pgUs))))

proc toPgBinaryParam*(v: PgTime): PgParam =
  let us =
    int64(v.hour) * 3_600_000_000'i64 + int64(v.minute) * 60_000_000'i64 +
    int64(v.second) * 1_000_000'i64 + int64(v.microsecond)
  PgParam(oid: OidTime, format: 1, value: some(@(toBE64(us))))

proc toPgBinaryParam*(v: PgTimeTz): PgParam =
  let us =
    int64(v.hour) * 3_600_000_000'i64 + int64(v.minute) * 60_000_000'i64 +
    int64(v.second) * 1_000_000'i64 + int64(v.microsecond)
  let pgOffset = int32(-v.utcOffset) # PostgreSQL stores offset negated
  var data: seq[byte] = @(toBE64(us))
  data.add(@(toBE32(pgOffset)))
  PgParam(oid: OidTimeTz, format: 1, value: some(data))

proc encodeNumericBinary*(v: PgNumeric): seq[byte] =
  ## Encode PgNumeric as PostgreSQL binary numeric format.
  if v.digits.len > int(int16.high):
    raise newException(
      PgTypeError, "Numeric binary: too many digit groups: " & $v.digits.len
    )
  let ndigits = int16(v.digits.len)
  let signVal = cast[int16](v.sign.uint16)
  result = newSeq[byte](8 + ndigits.int * 2)
  result.writeBE16(0, ndigits)
  result.writeBE16(2, v.weight)
  result.writeBE16(4, signVal)
  result.writeBE16(6, v.dscale)
  for i in 0 ..< ndigits.int:
    let digit = v.digits[i]
    # Mirror decodeNumericBinary: each base-10000 digit must be in [0, 9999].
    # The server's numeric_recv would reject out-of-range values anyway, so
    # fail locally with a clear error instead of a server-side one.
    if digit < 0 or digit > 9999:
      raise newException(PgTypeError, "Numeric binary: invalid digit " & $digit)
    result.writeBE16(8 + i * 2, digit)

proc toPgBinaryParam*(v: PgNumeric): PgParam =
  PgParam(oid: OidNumeric, format: 1, value: some(encodeNumericBinary(v)))

proc toPgBinaryParam*(v: PgMoney): PgParam =
  PgParam(oid: OidMoney, format: 1, value: some(@(toBE64(v.amount))))

proc hexNibble(c: char): int =
  case c
  of '0' .. '9':
    ord(c) - ord('0')
  of 'a' .. 'f':
    ord(c) - ord('a') + 10
  of 'A' .. 'F':
    ord(c) - ord('A') + 10
  else:
    -1

proc decodeHexPair(s: string, i: int, errCtx: string): byte =
  let hi = hexNibble(s[i])
  let lo = hexNibble(s[i + 1])
  if hi < 0 or lo < 0:
    raise newException(
      PgTypeError, errCtx & ": non-hex character at position " & $i & " in " & s.escape
    )
  byte((hi shl 4) or lo)

proc toPgBinaryParam*(v: PgUuid): PgParam =
  # Dashes are stripped before validation, so dash positions are not enforced
  # (e.g. dashless and non-canonical placements are accepted). PostgreSQL itself
  # is stricter in text format, but binary form has no dash concept.
  let hex = string(v).replace("-", "")
  if hex.len != 32:
    raise newException(
      PgTypeError,
      "Invalid PgUuid: expected 32 hex digits (dashes optional), got " & $hex.len &
        " in " & string(v).escape,
    )
  var bytes = newSeq[byte](16)
  for i in 0 ..< 16:
    bytes[i] = decodeHexPair(hex, i * 2, "Invalid PgUuid")
  PgParam(oid: OidUuid, format: 1, value: some(bytes))

proc toPgBinaryParam*(v: PgInterval): PgParam =
  var data = newSeq[byte](16)
  data.writeBE64(0, v.microseconds)
  data.writeBE32(8, v.days)
  data.writeBE32(12, v.months)
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

proc encodeMacBinary(s: string, n: int, label: string): seq[byte] =
  let parts = s.split(':')
  let prefix = "Invalid " & label
  if parts.len != n:
    raise newException(
      PgTypeError,
      prefix & ": expected " & $n & " colon-separated octets, got " & $parts.len & " in " &
        s.escape,
    )
  result = newSeq[byte](n)
  for i in 0 ..< n:
    if parts[i].len != 2:
      raise newException(
        PgTypeError, prefix & ": octet " & $i & " is not 2 hex digits in " & s.escape
      )
    result[i] = decodeHexPair(parts[i], 0, prefix)

proc toPgBinaryParam*(v: PgMacAddr): PgParam =
  ## Binary format: 6 raw bytes
  PgParam(
    oid: OidMacAddr, format: 1, value: some(encodeMacBinary(string(v), 6, "PgMacAddr"))
  )

proc toPgBinaryParam*(v: PgMacAddr8): PgParam =
  ## Binary format: 8 raw bytes
  PgParam(
    oid: OidMacAddr8,
    format: 1,
    value: some(encodeMacBinary(string(v), 8, "PgMacAddr8")),
  )

proc toPgBinaryParam*(v: PgTsVector): PgParam =
  ## Send as text format — PostgreSQL handles the parsing.
  PgParam(oid: OidTsVector, format: 0, value: some(toBytes(string(v))))

proc toPgBinaryParam*(v: PgTsQuery): PgParam =
  ## Send as text format — PostgreSQL handles the parsing.
  PgParam(oid: OidTsQuery, format: 0, value: some(toBytes(string(v))))

proc toPgBinaryParam*(v: PgXml): PgParam =
  ## Binary wire format for xml is the text representation itself.
  PgParam(oid: OidXml, format: 1, value: some(toBytes(string(v))))

proc toPgBinaryParam*(v: PgBit): PgParam =
  ## Binary format: 4-byte bit count (big-endian) + packed bit data.
  var data = newSeq[byte](4 + v.data.len)
  data.writeBE32(0, v.nbits)
  for i in 0 ..< v.data.len:
    data[4 + i] = v.data[i]
  PgParam(oid: OidVarbit, format: 1, value: some(data))

proc toPgBinaryParam*(v: seq[PgBit]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(toPgBinaryParam(x).value.get)
  PgParam(
    oid: OidVarbitArray,
    format: 1,
    value: some(encodeBinaryArray(OidVarbit, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[PgBit]): PgParam =
  toPgBinaryParam(v)

# Temporal array encoders

proc toPgTimestampArrayParam*(v: seq[DateTime]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(toPgBinaryParam(x).value.get)
  PgParam(
    oid: OidTimestampArray,
    format: 1,
    value: some(encodeBinaryArray(OidTimestamp, dimsFor1D(v.len), elements)),
  )

proc toPgTimestampTzArrayParam*(v: seq[DateTime]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(toPgBinaryTimestampTzParam(x).value.get)
  PgParam(
    oid: OidTimestampTzArray,
    format: 1,
    value: some(encodeBinaryArray(OidTimestampTz, dimsFor1D(v.len), elements)),
  )

proc toPgDateArrayParam*(v: seq[DateTime]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(toPgBinaryDateParam(x).value.get)
  PgParam(
    oid: OidDateArray,
    format: 1,
    value: some(encodeBinaryArray(OidDate, dimsFor1D(v.len), elements)),
  )

template genArrayEncoder(T: typedesc, arrayOid, elemOid: int32) =
  proc toPgParam*(v: seq[T]): PgParam =
    var elements = newSeq[Option[seq[byte]]](v.len)
    for i, x in v:
      elements[i] = some(toPgBinaryParam(x).value.get)
    PgParam(
      oid: arrayOid,
      format: 1,
      value: some(encodeBinaryArray(elemOid, dimsFor1D(v.len), elements)),
    )

genArrayEncoder(PgTime, OidTimeArray, OidTime)
genArrayEncoder(PgTimeTz, OidTimeTzArray, OidTimeTz)
genArrayEncoder(PgInterval, OidIntervalArray, OidInterval)

# Identifier / network array encoders

genArrayEncoder(PgUuid, OidUuidArray, OidUuid)
genArrayEncoder(PgInet, OidInetArray, OidInet)
genArrayEncoder(PgCidr, OidCidrArray, OidCidr)
genArrayEncoder(PgMacAddr, OidMacAddrArray, OidMacAddr)
genArrayEncoder(PgMacAddr8, OidMacAddr8Array, OidMacAddr8)
genArrayEncoder(PgMoney, OidMoneyArray, OidMoney)

# Numeric / binary / JSON array encoders

genArrayEncoder(PgNumeric, OidNumericArray, OidNumeric)

proc toPgByteaArrayParam*(v: seq[seq[byte]]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(x)
  PgParam(
    oid: OidByteaArray,
    format: 1,
    value: some(encodeBinaryArray(OidBytea, dimsFor1D(v.len), elements)),
  )

proc toPgParam*(v: seq[JsonNode]): PgParam =
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    let jsonBytes = toBytes($x)
    var data = newSeq[byte](1 + jsonBytes.len)
    data[0] = 1 # jsonb version byte
    for j in 0 ..< jsonBytes.len:
      data[j + 1] = jsonBytes[j]
    elements[i] = some(data)
  PgParam(
    oid: OidJsonbArray,
    format: 1,
    value: some(encodeBinaryArray(OidJsonb, dimsFor1D(v.len), elements)),
  )

template writePointAt*(dst: var openArray[byte], pos: int, p: PgPoint) =
  ## Write a point as 16 bytes (two float64 big-endian) into dst at pos.
  dst.writeBE64(pos, cast[int64](p.x))
  dst.writeBE64(pos + 8, cast[int64](p.y))

proc encodePointBinary*(p: PgPoint): seq[byte] =
  ## Encode a point as 16 bytes (two float64 big-endian).
  result = newSeq[byte](16)
  result.writePointAt(0, p)

proc toPgBinaryParam*(v: PgPoint): PgParam =
  ## Binary format: 16 bytes (two float64 big-endian).
  PgParam(oid: OidPoint, format: 1, value: some(encodePointBinary(v)))

proc toPgBinaryParam*(v: PgLine): PgParam =
  ## Binary format: 24 bytes (three float64 big-endian: A, B, C).
  var data = newSeq[byte](24)
  data.writeBE64(0, cast[int64](v.a))
  data.writeBE64(8, cast[int64](v.b))
  data.writeBE64(16, cast[int64](v.c))
  PgParam(oid: OidLine, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgLseg): PgParam =
  ## Binary format: 32 bytes (two points).
  var data = newSeq[byte](32)
  data.writePointAt(0, v.p1)
  data.writePointAt(16, v.p2)
  PgParam(oid: OidLseg, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgBox): PgParam =
  ## Binary format: 32 bytes (high point, low point).
  var data = newSeq[byte](32)
  data.writePointAt(0, v.high)
  data.writePointAt(16, v.low)
  PgParam(oid: OidBox, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgPath): PgParam =
  ## Binary format: closed(1) + npts(4) + points(npts \* 16).
  var data = newSeq[byte](1 + 4 + v.points.len * 16)
  data[0] = if v.closed: 1'u8 else: 0'u8
  data.writeBE32(1, int32(v.points.len))
  for i, p in v.points:
    data.writePointAt(5 + i * 16, p)
  PgParam(oid: OidPath, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgPolygon): PgParam =
  ## Binary format: npts(4) + points(npts \* 16).
  var data = newSeq[byte](4 + v.points.len * 16)
  data.writeBE32(0, int32(v.points.len))
  for i, p in v.points:
    data.writePointAt(4 + i * 16, p)
  PgParam(oid: OidPolygon, format: 1, value: some(data))

proc toPgBinaryParam*(v: PgCircle): PgParam =
  ## Binary format: 24 bytes (center point + radius float64).
  var data = newSeq[byte](24)
  data.writePointAt(0, v.center)
  data.writeBE64(16, cast[int64](v.radius))
  PgParam(oid: OidCircle, format: 1, value: some(data))

proc toPgBinaryParam*(v: JsonNode): PgParam =
  let jsonBytes = toBytes($v)
  var data = newSeq[byte](1 + jsonBytes.len)
  data[0] = 1 # jsonb version byte
  for i in 0 ..< jsonBytes.len:
    data[i + 1] = jsonBytes[i]
  PgParam(oid: OidJsonb, format: 1, value: some(data))

# Geometric array encoders

genArrayEncoder(PgPoint, OidPointArray, OidPoint)
genArrayEncoder(PgLine, OidLineArray, OidLine)
genArrayEncoder(PgLseg, OidLsegArray, OidLseg)
genArrayEncoder(PgBox, OidBoxArray, OidBox)
genArrayEncoder(PgPath, OidPathArray, OidPath)
genArrayEncoder(PgPolygon, OidPolygonArray, OidPolygon)
genArrayEncoder(PgCircle, OidCircleArray, OidCircle)

# Other array encoders

template genStringArrayEncoder(T: typedesc, arrayOid, elemOid: int32) =
  proc toPgParam*(v: seq[T]): PgParam =
    var elements = newSeq[Option[seq[byte]]](v.len)
    for i, x in v:
      elements[i] = some(toBytes(string(x)))
    PgParam(
      oid: arrayOid,
      format: 1,
      value: some(encodeBinaryArray(elemOid, dimsFor1D(v.len), elements)),
    )

genStringArrayEncoder(PgXml, OidXmlArray, OidXml)
genStringArrayEncoder(PgTsVector, OidTsVectorArray, OidTsVector)
genStringArrayEncoder(PgTsQuery, OidTsQueryArray, OidTsQuery)

proc toPgBinaryParam*[T](v: seq[T]): PgParam =
  toPgParam(v)

proc toPgBinaryParam*(v: Option[JsonNode]): PgParam =
  if v.isSome:
    toPgBinaryParam(v.get)
  else:
    PgParam(oid: OidJsonb, format: 1, value: none(seq[byte]))

proc encodeHstoreBinary*(v: PgHstore): seq[byte] =
  ## Encode hstore as PostgreSQL binary format.
  ## Format: ``numPairs(int32) + [keyLen(int32) + keyData + valLen(int32) + valData]...``
  var size = 4
  for k, val in v.pairs:
    size += 4 + k.len + 4
    if val.isSome:
      size += val.get.len
  result = newSeq[byte](size)
  result.writeBE32(0, int32(v.len))
  var pos = 4
  for k, val in v.pairs:
    result.writeBE32(pos, int32(k.len))
    pos += 4
    if k.len > 0:
      result.writeBytesAt(pos, k.toOpenArrayByte(0, k.high))
      pos += k.len
    if val.isSome:
      let vs = val.get
      result.writeBE32(pos, int32(vs.len))
      pos += 4
      if vs.len > 0:
        result.writeBytesAt(pos, vs.toOpenArrayByte(0, vs.high))
        pos += vs.len
    else:
      result.writeBE32(pos, -1'i32)
      pos += 4

proc toPgBinaryParam*(v: PgHstore, oid: int32): PgParam =
  ## Encode hstore in binary format. Requires the dynamic hstore OID,
  ## obtained via ``lookupTypeOids(conn, @["hstore"])``.
  PgParam(oid: oid, format: 1, value: some(encodeHstoreBinary(v)))

proc toPgParam*(v: seq[PgHstore]): PgParam =
  ## Send ``hstore[]`` in text format using ``OidTextArray``. Requires an
  ## explicit ``::hstore[]`` cast in the SQL statement (e.g.
  ## ``SELECT $1::hstore[]``), since the parameter is typed as ``text[]``. No
  ## connection-specific OID is needed; prefer ``toPgBinaryParam`` when the
  ## hstore / hstore[] OIDs are available via ``lookupTypeOids`` (faster, no
  ## cast required).
  if v.len == 0:
    return PgParam(oid: OidTextArray, format: 0, value: some(toBytes("{}")))
  var s = "{"
  for i, h in v:
    if i > 0:
      s.add(',')
    s.add('"')
    for c in encodeHstoreText(h):
      if c == '"' or c == '\\':
        s.add('\\')
      s.add(c)
    s.add('"')
  s.add('}')
  PgParam(oid: OidTextArray, format: 0, value: some(toBytes(s)))

proc toPgBinaryParam*(v: seq[PgHstore], elemOid: int32, arrayOid: int32): PgParam =
  ## Encode ``hstore[]`` in binary format. Requires the dynamic hstore and
  ## ``hstore[]`` OIDs, obtained in a single call via
  ## ``lookupTypeOids(conn, @["hstore"])`` (the result entry exposes
  ## ``oid`` and ``arrayOid``).
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] = some(encodeHstoreBinary(x))
  PgParam(
    oid: arrayOid,
    format: 1,
    value: some(encodeBinaryArray(elemOid, dimsFor1D(v.len), elements)),
  )

proc toPgBinaryParam*[T](v: Option[T]): PgParam =
  if v.isSome:
    result = toPgBinaryParam(v.get)
  else:
    let proto = toPgBinaryParam(default(T))
    result = PgParam(oid: proto.oid, format: proto.format, value: none(seq[byte]))

# PgArray[T] element registry: per-type element and array OIDs plus
# element-to-bytes encoders, consumed by the generic ``toPgParam(PgArray[T])``.
# Decoders live in ``accessors.nim`` alongside ``getArrayND[T]``.

proc pgArrayElemOid*(_: typedesc[int16]): int32 =
  OidInt2

proc pgArrayArrayOid*(_: typedesc[int16]): int32 =
  OidInt2Array

proc encodePgArrayElement*(v: int16): seq[byte] =
  @(toBE16(v))

proc pgArrayElemOid*(_: typedesc[int32]): int32 =
  OidInt4

proc pgArrayArrayOid*(_: typedesc[int32]): int32 =
  OidInt4Array

proc encodePgArrayElement*(v: int32): seq[byte] =
  @(toBE32(v))

proc pgArrayElemOid*(_: typedesc[int64]): int32 =
  OidInt8

proc pgArrayArrayOid*(_: typedesc[int64]): int32 =
  OidInt8Array

proc encodePgArrayElement*(v: int64): seq[byte] =
  @(toBE64(v))

proc pgArrayElemOid*(_: typedesc[float32]): int32 =
  OidFloat4

proc pgArrayArrayOid*(_: typedesc[float32]): int32 =
  OidFloat4Array

proc encodePgArrayElement*(v: float32): seq[byte] =
  @(toBE32(cast[int32](v)))

proc pgArrayElemOid*(_: typedesc[float64]): int32 =
  OidFloat8

proc pgArrayArrayOid*(_: typedesc[float64]): int32 =
  OidFloat8Array

proc encodePgArrayElement*(v: float64): seq[byte] =
  @(toBE64(cast[int64](v)))

proc pgArrayElemOid*(_: typedesc[bool]): int32 =
  OidBool

proc pgArrayArrayOid*(_: typedesc[bool]): int32 =
  OidBoolArray

proc encodePgArrayElement*(v: bool): seq[byte] =
  @[if v: 1'u8 else: 0'u8]

proc pgArrayElemOid*(_: typedesc[string]): int32 =
  OidText

proc pgArrayArrayOid*(_: typedesc[string]): int32 =
  OidTextArray

proc encodePgArrayElement*(v: string): seq[byte] =
  toBytes(v)

proc pgArrayElemOid*(_: typedesc[PgUuid]): int32 =
  OidUuid

proc pgArrayArrayOid*(_: typedesc[PgUuid]): int32 =
  OidUuidArray

proc encodePgArrayElement*(v: PgUuid): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgNumeric]): int32 =
  OidNumeric

proc pgArrayArrayOid*(_: typedesc[PgNumeric]): int32 =
  OidNumericArray

proc encodePgArrayElement*(v: PgNumeric): seq[byte] =
  encodeNumericBinary(v)

proc pgArrayElemOid*(_: typedesc[PgMoney]): int32 =
  ## Used by ``getMoneyArrayND`` to verify the wire ``elemOid``.
  ## ``pgArrayArrayOid(PgMoney)`` and ``encodePgArrayElement(PgMoney)`` are
  ## intentionally not provided: ``money``'s binary wire format does not
  ## carry the scale, so callers must go through
  ## ``toPgMoneyArrayNDParam(v, scale = ...)`` (which validates every
  ## element's ``scale`` against the server's ``frac_digits``), and the
  ## generic ``toPgParam(PgArray[PgMoney])`` is ``{.error.}``-gated.
  OidMoney

proc pgArrayElemOid*(_: typedesc[PgBit]): int32 =
  OidVarbit

proc pgArrayArrayOid*(_: typedesc[PgBit]): int32 =
  OidVarbitArray

proc encodePgArrayElement*(v: PgBit): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgInterval]): int32 =
  OidInterval

proc pgArrayArrayOid*(_: typedesc[PgInterval]): int32 =
  OidIntervalArray

proc encodePgArrayElement*(v: PgInterval): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgTime]): int32 =
  OidTime

proc pgArrayArrayOid*(_: typedesc[PgTime]): int32 =
  OidTimeArray

proc encodePgArrayElement*(v: PgTime): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgTimeTz]): int32 =
  OidTimeTz

proc pgArrayArrayOid*(_: typedesc[PgTimeTz]): int32 =
  OidTimeTzArray

proc encodePgArrayElement*(v: PgTimeTz): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgInet]): int32 =
  OidInet

proc pgArrayArrayOid*(_: typedesc[PgInet]): int32 =
  OidInetArray

proc encodePgArrayElement*(v: PgInet): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgCidr]): int32 =
  OidCidr

proc pgArrayArrayOid*(_: typedesc[PgCidr]): int32 =
  OidCidrArray

proc encodePgArrayElement*(v: PgCidr): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgMacAddr]): int32 =
  OidMacAddr

proc pgArrayArrayOid*(_: typedesc[PgMacAddr]): int32 =
  OidMacAddrArray

proc encodePgArrayElement*(v: PgMacAddr): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgMacAddr8]): int32 =
  OidMacAddr8

proc pgArrayArrayOid*(_: typedesc[PgMacAddr8]): int32 =
  OidMacAddr8Array

proc encodePgArrayElement*(v: PgMacAddr8): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgXml]): int32 =
  OidXml

proc pgArrayArrayOid*(_: typedesc[PgXml]): int32 =
  OidXmlArray

proc encodePgArrayElement*(v: PgXml): seq[byte] =
  toBytes(string(v))

proc pgArrayElemOid*(_: typedesc[PgPoint]): int32 =
  OidPoint

proc pgArrayArrayOid*(_: typedesc[PgPoint]): int32 =
  OidPointArray

proc encodePgArrayElement*(v: PgPoint): seq[byte] =
  encodePointBinary(v)

proc pgArrayElemOid*(_: typedesc[PgLine]): int32 =
  OidLine

proc pgArrayArrayOid*(_: typedesc[PgLine]): int32 =
  OidLineArray

proc encodePgArrayElement*(v: PgLine): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgLseg]): int32 =
  OidLseg

proc pgArrayArrayOid*(_: typedesc[PgLseg]): int32 =
  OidLsegArray

proc encodePgArrayElement*(v: PgLseg): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgBox]): int32 =
  OidBox

proc pgArrayArrayOid*(_: typedesc[PgBox]): int32 =
  OidBoxArray

proc encodePgArrayElement*(v: PgBox): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgPath]): int32 =
  OidPath

proc pgArrayArrayOid*(_: typedesc[PgPath]): int32 =
  OidPathArray

proc encodePgArrayElement*(v: PgPath): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgPolygon]): int32 =
  OidPolygon

proc pgArrayArrayOid*(_: typedesc[PgPolygon]): int32 =
  OidPolygonArray

proc encodePgArrayElement*(v: PgPolygon): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[PgCircle]): int32 =
  OidCircle

proc pgArrayArrayOid*(_: typedesc[PgCircle]): int32 =
  OidCircleArray

proc encodePgArrayElement*(v: PgCircle): seq[byte] =
  toPgBinaryParam(v).value.get

proc pgArrayElemOid*(_: typedesc[JsonNode]): int32 =
  OidJsonb

proc pgArrayArrayOid*(_: typedesc[JsonNode]): int32 =
  OidJsonbArray

proc encodePgArrayElement*(v: JsonNode): seq[byte] =
  let jsonBytes = toBytes($v)
  result = newSeq[byte](1 + jsonBytes.len)
  result[0] = 1 # jsonb version byte
  for j in 0 ..< jsonBytes.len:
    result[j + 1] = jsonBytes[j]

proc toPgParam*[T](v: PgArray[T]): PgParam =
  ## Encode an N-dimensional ``PgArray[T]`` as a PostgreSQL binary array
  ## parameter. ``T`` must be a registered scalar type — see the
  ## ``pgArrayElemOid`` / ``encodePgArrayElement`` overloads above.
  ##
  ## For ``T == JsonNode`` the parameter is always tagged as ``jsonb[]`` (with
  ## the leading ``0x01`` version byte on every element). Binding such a value
  ## against a ``json[]`` column requires an explicit ``$1::json[]`` cast,
  ## otherwise PostgreSQL will reject the parameter.
  when T is DateTime:
    {.
      error:
        "PgArray[DateTime] is ambiguous (timestamp / timestamptz / date have " &
        "distinct OIDs). Use toPgTimestampArrayParam / " &
        "toPgTimestampTzArrayParam / toPgDateArrayParam with seq[DateTime] " &
        "(or seq[Option[DateTime]]) instead."
    .}
  elif T is seq[byte]:
    {.
      error:
        "PgArray[seq[byte]] is not supported by the PgArray registry. " &
        "Use toPgByteaArrayParam with seq[seq[byte]] instead."
    .}
  elif T is PgHstore:
    {.
      error:
        "PgArray[PgHstore] is not supported by the PgArray registry " &
        "(hstore uses a dynamic OID). Use toPgBinaryParam with seq[PgHstore] " &
        "and OIDs obtained via lookupTypeOids instead."
    .}
  elif T is int:
    {.
      error:
        "PgArray[int] is not supported (platform-dependent width). " &
        "Use PgArray[int32] or PgArray[int64] explicitly."
    .}
  elif T is PgTsVector or T is PgTsQuery:
    {.
      error:
        "PgArray[PgTsVector] / PgArray[PgTsQuery] is not supported by the " &
        "PgArray registry: PostgreSQL's binary wire format for tsvector / " &
        "tsquery is structured (not the text representation), so the array " &
        "element bytes would be rejected by the server. Send a " &
        "seq[PgTsVector] / seq[PgTsQuery] via the existing text-format " &
        "encoders instead, or build the parameter manually."
    .}
  elif T is PgMoney:
    {.
      error:
        "PgArray[PgMoney] would silently hardcode scale=2 and produce wrong " &
        "values on servers whose lc_monetary frac_digits differ from 2. " &
        "Use toPgMoneyArrayNDParam(v, scale = ...) instead."
    .}
  # Shape validation is delegated to encodeBinaryArray below; no need to
  # validate here as well — both call paths check the same invariants.
  let elemOid = pgArrayElemOid(T)
  let arrayOid = pgArrayArrayOid(T)
  var elements = newSeq[Option[seq[byte]]](v.elements.len)
  for i, oe in v.elements:
    if oe.isSome:
      elements[i] = some(encodePgArrayElement(oe.get))
    else:
      elements[i] = none(seq[byte])
  PgParam(
    oid: arrayOid,
    format: 1,
    value: some(encodeBinaryArray(elemOid, v.dims, v.lowerBounds, elements)),
  )

proc toPgMoneyArrayNDParam*(v: PgArray[PgMoney], scale: int = 2): PgParam =
  ## Encoder counterpart to ``getMoneyArrayND``. PostgreSQL's binary ``money``
  ## wire format only carries the raw amount; the fractional-digit count is
  ## determined by the server's ``lc_monetary``. ``scale`` declares the
  ## ``frac_digits`` the caller's ``PgMoney.amount`` values are scaled for,
  ## and every non-NULL element's ``scale`` field must match it — otherwise
  ## a silent value mismatch on the server is likely.
  ##
  ## Raises ``PgError`` when ``scale`` is outside ``0..18`` or when any
  ## element's ``scale`` differs from the parameter.
  if scale < 0 or scale > 18:
    raise newException(PgError, "PgMoney scale out of range: " & $scale)
  for i, oe in v.elements:
    if oe.isSome and int(oe.get.scale) != scale:
      raise newException(
        PgError,
        "PgMoney array element[" & $i & "].scale=" & $oe.get.scale &
          " does not match declared scale=" & $scale &
          " (server lc_monetary frac_digits)",
      )
  var elements = newSeq[Option[seq[byte]]](v.elements.len)
  for i, oe in v.elements:
    if oe.isSome:
      elements[i] = some(@(toBE64(oe.get.amount)))
    else:
      elements[i] = none(seq[byte])
  PgParam(
    oid: OidMoneyArray,
    format: 1,
    value: some(encodeBinaryArray(OidMoney, v.dims, v.lowerBounds, elements)),
  )

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
  buf.addCount16(params.len, "Parse parameter-type")
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
  buf.addCount16(params.len, "Bind parameter-format")
  for p in params:
    buf.addInt16(p.format)
  # Parameter values
  buf.addCount16(params.len, "Bind parameter")
  for p in params:
    if p.value.isNone:
      buf.addInt32(-1) # NULL
    else:
      let data = p.value.get
      buf.addLen32(data.len, "Bind parameter value")
      buf.appendBytes(data)
  # Result format codes
  buf.addCount16(resultFormats.len, "Bind result-format")
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
  buf.writeBE32(o, v)

proc writeParamValue*(buf: var seq[byte], v: int64) =
  buf.addInt32(8'i32)
  let o = buf.len
  buf.setLen(o + 8)
  buf.writeBE64(o, v)

proc writeParamValue*(buf: var seq[byte], v: int) =
  writeParamValue(buf, int64(v))

proc writeParamValue*(buf: var seq[byte], v: float32) =
  buf.addInt32(4'i32)
  let o = buf.len
  buf.setLen(o + 4)
  buf.writeBE32(o, cast[int32](v))

proc writeParamValue*(buf: var seq[byte], v: float64) =
  let bits = cast[int64](v)
  writeParamValue(buf, bits)

proc writeParamValue*(buf: var seq[byte], v: bool) =
  buf.addInt32(1'i32)
  buf.add(if v: 1'u8 else: 0'u8)

proc writeParamValue*(buf: var seq[byte], v: string) =
  buf.addLen32(v.len, "parameter value")
  if v.len > 0:
    buf.appendBytes(v.toOpenArrayByte(0, v.high))

proc writeParamValue*(buf: var seq[byte], v: seq[byte]) =
  buf.addLen32(v.len, "parameter value")
  buf.appendBytes(v)

proc writeParamValue*(buf: var seq[byte], v: PgNumeric) =
  writeParamValue(buf, $v)

# Compile-time OID lookup for parameter scalar types. Source of truth for
# the OID emitted by ``writeParamOid`` (Parse message) and consumed by
# ``queryDirect`` / ``execDirect``'s cache-hit OID validation. Adding a new
# scalar type means adding a ``paramOidOf`` overload here — ``writeParamOid``
# below picks it up automatically, so the two cannot drift.

func paramOidOf*(v: int16): int32 =
  OidInt2

func paramOidOf*(v: int32): int32 =
  OidInt4

func paramOidOf*(v: int64): int32 =
  OidInt8

func paramOidOf*(v: int): int32 =
  OidInt8

func paramOidOf*(v: float32): int32 =
  OidFloat4

func paramOidOf*(v: float64): int32 =
  OidFloat8

func paramOidOf*(v: bool): int32 =
  OidBool

func paramOidOf*(v: string): int32 =
  OidText

func paramOidOf*(v: seq[byte]): int32 =
  OidBytea

func paramOidOf*(v: PgNumeric): int32 =
  OidNumeric

proc writeParamOid*(buf: var seq[byte], v: int16) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: int32) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: int64) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: int) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: float32) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: float64) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: bool) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: string) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: seq[byte]) =
  buf.addInt32(paramOidOf(v))

proc writeParamOid*(buf: var seq[byte], v: PgNumeric) =
  buf.addInt32(paramOidOf(v))

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
