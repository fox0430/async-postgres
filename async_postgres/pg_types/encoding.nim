import std/[options, json, macros, strutils, tables, times, net]

import ../pg_protocol
import ./core

proc toPgParamInline*(v: int16): PgParamInline =
  result.oid = OidInt2
  result.format = 1
  result.len = 2
  let be = toBE16(v)
  result.inlineBuf[0] = be[0]
  result.inlineBuf[1] = be[1]

proc toPgParamInline*(v: int32): PgParamInline =
  result.oid = OidInt4
  result.format = 1
  result.len = 4
  let be = toBE32(v)
  result.inlineBuf[0] = be[0]
  result.inlineBuf[1] = be[1]
  result.inlineBuf[2] = be[2]
  result.inlineBuf[3] = be[3]

proc toPgParamInline*(v: int64): PgParamInline =
  result.oid = OidInt8
  result.format = 1
  result.len = 8
  let be = toBE64(v)
  for i in 0 ..< 8:
    result.inlineBuf[i] = be[i]

proc toPgParamInline*(v: int): PgParamInline =
  toPgParamInline(int64(v))

proc toPgParamInline*(v: float32): PgParamInline =
  result.oid = OidFloat4
  result.format = 1
  result.len = 4
  let be = toBE32(cast[int32](v))
  result.inlineBuf[0] = be[0]
  result.inlineBuf[1] = be[1]
  result.inlineBuf[2] = be[2]
  result.inlineBuf[3] = be[3]

proc toPgParamInline*(v: float64): PgParamInline =
  result.oid = OidFloat8
  result.format = 1
  result.len = 8
  let be = toBE64(cast[int64](v))
  for i in 0 ..< 8:
    result.inlineBuf[i] = be[i]

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
    copyMem(addr result.inlineBuf[0], unsafeAddr v[0], v.len)
  else:
    result.overflow = newSeq[byte](v.len)
    copyMem(addr result.overflow[0], unsafeAddr v[0], v.len)

proc toPgParamInline*(v: seq[byte]): PgParamInline =
  result.oid = OidBytea
  result.format = 0
  result.len = int32(v.len)
  if v.len == 0:
    discard
  elif v.len <= PgInlineBufSize:
    copyMem(addr result.inlineBuf[0], unsafeAddr v[0], v.len)
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
    copyMem(addr result.inlineBuf[0], unsafeAddr s[0], s.len)
  else:
    result.overflow = newSeq[byte](s.len)
    copyMem(addr result.overflow[0], unsafeAddr s[0], s.len)

proc toPgParamInline*(v: PgMoney): PgParamInline =
  result.oid = OidMoney
  result.format = 1
  result.len = 8
  let be = toBE64(v.amount)
  for i in 0 ..< 8:
    result.inlineBuf[i] = be[i]

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

proc encodeBinaryArray*(elemOid: int32, elements: seq[Option[seq[byte]]]): seq[byte] =
  ## Encode a 1-dimensional binary array that may contain NULL elements.
  ## NULL elements are written with length ``-1`` and no payload.
  ## ``has_null`` is set to 1 iff any element is ``none``.
  if elements.len > int32.high.int:
    raise
      newException(PgError, "Array has too many elements for PostgreSQL binary format")
  let headerSize = 20
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
  result = newSeq[byte](headerSize + dataSize)
  let ndim = toBE32(1'i32)
  copyMem(addr result[0], unsafeAddr ndim[0], 4)
  let hasNull = toBE32(if anyNull: 1'i32 else: 0'i32)
  copyMem(addr result[4], unsafeAddr hasNull[0], 4)
  let oid = toBE32(elemOid)
  copyMem(addr result[8], unsafeAddr oid[0], 4)
  let dimLen = toBE32(int32(elements.len))
  copyMem(addr result[12], unsafeAddr dimLen[0], 4)
  let lb = toBE32(1'i32)
  copyMem(addr result[16], unsafeAddr lb[0], 4)
  var pos = headerSize
  for e in elements:
    if e.isNone:
      let eLen = toBE32(-1'i32)
      copyMem(addr result[pos], unsafeAddr eLen[0], 4)
      pos += 4
    else:
      let ev = e.get
      let eLen = toBE32(int32(ev.len))
      copyMem(addr result[pos], unsafeAddr eLen[0], 4)
      pos += 4
      if ev.len > 0:
        copyMem(addr result[pos], unsafeAddr ev[0], ev.len)
        pos += ev.len

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

proc toPgParam*(v: seq[Option[int16]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt2Array, format: 1, value: some(encodeBinaryArrayEmpty(OidInt2))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE16(x.get)))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt2Array, format: 1, value: some(encodeBinaryArray(OidInt2, elements))
  )

proc toPgParam*(v: seq[Option[int32]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt4Array, format: 1, value: some(encodeBinaryArrayEmpty(OidInt4))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE32(x.get)))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt4Array, format: 1, value: some(encodeBinaryArray(OidInt4, elements))
  )

proc toPgParam*(v: seq[Option[int64]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt8Array, format: 1, value: some(encodeBinaryArrayEmpty(OidInt8))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE64(x.get)))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt8Array, format: 1, value: some(encodeBinaryArray(OidInt8, elements))
  )

proc toPgParam*(v: seq[Option[int]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidInt8Array, format: 1, value: some(encodeBinaryArrayEmpty(OidInt8))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE64(int64(x.get))))
      else:
        none(seq[byte])
  PgParam(
    oid: OidInt8Array, format: 1, value: some(encodeBinaryArray(OidInt8, elements))
  )

proc toPgParam*(v: seq[Option[float32]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidFloat4Array, format: 1, value: some(encodeBinaryArrayEmpty(OidFloat4))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE32(cast[int32](x.get))))
      else:
        none(seq[byte])
  PgParam(
    oid: OidFloat4Array, format: 1, value: some(encodeBinaryArray(OidFloat4, elements))
  )

proc toPgParam*(v: seq[Option[float64]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidFloat8Array, format: 1, value: some(encodeBinaryArrayEmpty(OidFloat8))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@(toBE64(cast[int64](x.get))))
      else:
        none(seq[byte])
  PgParam(
    oid: OidFloat8Array, format: 1, value: some(encodeBinaryArray(OidFloat8, elements))
  )

proc toPgParam*(v: seq[Option[bool]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidBoolArray, format: 1, value: some(encodeBinaryArrayEmpty(OidBool))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(@[if x.get: 1'u8 else: 0'u8])
      else:
        none(seq[byte])
  PgParam(
    oid: OidBoolArray, format: 1, value: some(encodeBinaryArray(OidBool, elements))
  )

proc toPgParam*(v: seq[Option[string]]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidTextArray, format: 1, value: some(encodeBinaryArrayEmpty(OidText))
    )
  var elements = newSeq[Option[seq[byte]]](v.len)
  for i, x in v:
    elements[i] =
      if x.isSome:
        some(toBytes(x.get))
      else:
        none(seq[byte])
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

proc toPgBinaryDateParam*(v: DateTime): PgParam =
  ## Encode a DateTime as a binary date parameter (OID 1082).
  let t = v.toTime()
  let pgDays = int32(t.toUnix() div 86400 - int64(pgEpochDaysOffset))
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

proc toPgBinaryParam*(v: PgMoney): PgParam =
  PgParam(oid: OidMoney, format: 1, value: some(@(toBE64(v.amount))))

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
  let beNbits = toBE32(v.nbits)
  for i in 0 ..< 4:
    data[i] = beNbits[i]
  for i in 0 ..< v.data.len:
    data[4 + i] = v.data[i]
  PgParam(oid: OidVarbit, format: 1, value: some(data))

proc toPgBinaryParam*(v: seq[PgBit]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidVarbitArray, format: 1, value: some(encodeBinaryArrayEmpty(OidVarbit))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = toPgBinaryParam(x).value.get
  PgParam(
    oid: OidVarbitArray, format: 1, value: some(encodeBinaryArray(OidVarbit, elements))
  )

proc toPgParam*(v: seq[PgBit]): PgParam =
  toPgBinaryParam(v)

# Temporal array encoders

proc toPgTimestampArrayParam*(v: seq[DateTime]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidTimestampArray,
      format: 1,
      value: some(encodeBinaryArrayEmpty(OidTimestamp)),
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = toPgBinaryParam(x).value.get
  PgParam(
    oid: OidTimestampArray,
    format: 1,
    value: some(encodeBinaryArray(OidTimestamp, elements)),
  )

proc toPgTimestampTzArrayParam*(v: seq[DateTime]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidTimestampTzArray,
      format: 1,
      value: some(encodeBinaryArrayEmpty(OidTimestampTz)),
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = toPgBinaryTimestampTzParam(x).value.get
  PgParam(
    oid: OidTimestampTzArray,
    format: 1,
    value: some(encodeBinaryArray(OidTimestampTz, elements)),
  )

proc toPgDateArrayParam*(v: seq[DateTime]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidDateArray, format: 1, value: some(encodeBinaryArrayEmpty(OidDate))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = toPgBinaryDateParam(x).value.get
  PgParam(
    oid: OidDateArray, format: 1, value: some(encodeBinaryArray(OidDate, elements))
  )

template genArrayEncoder(T: typedesc, arrayOid, elemOid: int32) =
  proc toPgParam*(v: seq[T]): PgParam =
    if v.len == 0:
      return
        PgParam(oid: arrayOid, format: 1, value: some(encodeBinaryArrayEmpty(elemOid)))
    var elements = newSeq[seq[byte]](v.len)
    for i, x in v:
      elements[i] = toPgBinaryParam(x).value.get
    PgParam(oid: arrayOid, format: 1, value: some(encodeBinaryArray(elemOid, elements)))

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
  if v.len == 0:
    return PgParam(
      oid: OidByteaArray, format: 1, value: some(encodeBinaryArrayEmpty(OidBytea))
    )
  PgParam(oid: OidByteaArray, format: 1, value: some(encodeBinaryArray(OidBytea, v)))

proc toPgParam*(v: seq[JsonNode]): PgParam =
  if v.len == 0:
    return PgParam(
      oid: OidJsonbArray, format: 1, value: some(encodeBinaryArrayEmpty(OidJsonb))
    )
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    let jsonBytes = toBytes($x)
    var data = newSeq[byte](1 + jsonBytes.len)
    data[0] = 1 # jsonb version byte
    for j in 0 ..< jsonBytes.len:
      data[j + 1] = jsonBytes[j]
    elements[i] = data
  PgParam(
    oid: OidJsonbArray, format: 1, value: some(encodeBinaryArray(OidJsonb, elements))
  )

proc encodePointBinary*(p: PgPoint): seq[byte] =
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
    if v.len == 0:
      return
        PgParam(oid: arrayOid, format: 1, value: some(encodeBinaryArrayEmpty(elemOid)))
    var elements = newSeq[seq[byte]](v.len)
    for i, x in v:
      elements[i] = toBytes(string(x))
    PgParam(oid: arrayOid, format: 1, value: some(encodeBinaryArray(elemOid, elements)))

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
  let np = toBE32(int32(v.len))
  copyMem(addr result[0], unsafeAddr np[0], 4)
  var pos = 4
  for k, val in v.pairs:
    let kLen = toBE32(int32(k.len))
    copyMem(addr result[pos], unsafeAddr kLen[0], 4)
    pos += 4
    if k.len > 0:
      copyMem(addr result[pos], unsafeAddr k[0], k.len)
      pos += k.len
    if val.isSome:
      let vLen = toBE32(int32(val.get.len))
      copyMem(addr result[pos], unsafeAddr vLen[0], 4)
      pos += 4
      if val.get.len > 0:
        copyMem(addr result[pos], unsafeAddr val.get[0], val.get.len)
        pos += val.get.len
    else:
      let nullLen = toBE32(-1'i32)
      copyMem(addr result[pos], unsafeAddr nullLen[0], 4)
      pos += 4

proc toPgBinaryParam*(v: PgHstore, oid: int32): PgParam =
  ## Encode hstore in binary format. Requires the dynamic hstore OID
  ## (available as ``conn.hstoreOid`` after connection).
  PgParam(oid: oid, format: 1, value: some(encodeHstoreBinary(v)))

proc toPgParam*(v: seq[PgHstore]): PgParam =
  ## Send ``hstore[]`` in text format using ``OidTextArray``. Requires an
  ## explicit ``::hstore[]`` cast in the SQL statement (e.g.
  ## ``SELECT $1::hstore[]``), since the parameter is typed as ``text[]``. No
  ## connection-specific OID is needed; prefer ``toPgBinaryParam`` when a
  ## ``PgConnection`` with the discovered hstore OIDs is available (faster, no
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
  ## Encode ``hstore[]`` in binary format. Requires both the dynamic hstore OID
  ## and ``hstore[]`` OID (available as ``conn.hstoreOid`` and
  ## ``conn.hstoreArrayOid`` after connection). See also the ``PgConnection``
  ## overload in ``pg_connection`` which reads these OIDs automatically.
  if v.len == 0:
    return
      PgParam(oid: arrayOid, format: 1, value: some(encodeBinaryArrayEmpty(elemOid)))
  var elements = newSeq[seq[byte]](v.len)
  for i, x in v:
    elements[i] = encodeHstoreBinary(x)
  PgParam(oid: arrayOid, format: 1, value: some(encodeBinaryArray(elemOid, elements)))

proc toPgBinaryParam*[T](v: Option[T]): PgParam =
  if v.isSome:
    result = toPgBinaryParam(v.get)
  else:
    let proto = toPgBinaryParam(default(T))
    result = PgParam(oid: proto.oid, format: proto.format, value: none(seq[byte]))

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
