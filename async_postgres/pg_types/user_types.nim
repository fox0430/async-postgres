import std/[options, macros, strutils, typetraits]

import ../pg_protocol
import ./core
import ./decoding
import ./accessors

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

proc encodeEnumTextArray*(labels: seq[Option[string]]): string =
  ## Encode enum labels as a PostgreSQL text-format array literal.
  ## ``none`` labels become unquoted ``NULL``.
  result = "{"
  for i, lbl in labels:
    if i > 0:
      result.add(',')
    if lbl.isSome:
      result.add('"')
      for c in lbl.get:
        if c == '"' or c == '\\':
          result.add('\\')
        result.add(c)
      result.add('"')
    else:
      result.add("NULL")
  result.add('}')

macro pgEnum*(T: untyped): untyped =
  ## Generate ``toPgParam`` overloads for a Nim enum type and its array forms.
  ## OIDs are 0 (unspecified) so PostgreSQL infers the type from context
  ## (use ``$1::mytype`` / ``$1::mytype[]`` in the SQL).
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: 0'i32, format: 0'i16, value: some(toBytes($v)))

    proc toPgParam*(v: seq[`T`]): PgParam =
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] = some($x)
      PgParam(
        oid: 0'i32, format: 0'i16, value: some(toBytes(encodeEnumTextArray(labels)))
      )

    proc toPgParam*(v: seq[Option[`T`]]): PgParam =
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] =
          if x.isSome:
            some($x.get)
          else:
            none(string)
      PgParam(
        oid: 0'i32, format: 0'i16, value: some(toBytes(encodeEnumTextArray(labels)))
      )

macro pgEnum*(T: untyped, oid: untyped): untyped =
  ## Generate ``toPgParam`` overloads for a Nim enum type with an explicit
  ## scalar OID. The array OID is unspecified (0); add a ``$1::mytype[]``
  ## cast in the SQL, or use the 3-argument form to set the array OID too.
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: int32(`oid`), format: 0'i16, value: some(toBytes($v)))

    proc toPgParam*(v: seq[`T`]): PgParam =
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] = some($x)
      PgParam(
        oid: 0'i32, format: 0'i16, value: some(toBytes(encodeEnumTextArray(labels)))
      )

    proc toPgParam*(v: seq[Option[`T`]]): PgParam =
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] =
          if x.isSome:
            some($x.get)
          else:
            none(string)
      PgParam(
        oid: 0'i32, format: 0'i16, value: some(toBytes(encodeEnumTextArray(labels)))
      )

macro pgEnum*(T: untyped, oid: untyped, arrayOid: untyped): untyped =
  ## Generate ``toPgParam`` overloads with explicit scalar and array OIDs.
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: int32(`oid`), format: 0'i16, value: some(toBytes($v)))

    proc toPgParam*(v: seq[`T`]): PgParam =
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] = some($x)
      PgParam(
        oid: int32(`arrayOid`),
        format: 0'i16,
        value: some(toBytes(encodeEnumTextArray(labels))),
      )

    proc toPgParam*(v: seq[Option[`T`]]): PgParam =
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] =
          if x.isSome:
            some($x.get)
          else:
            none(string)
      PgParam(
        oid: int32(`arrayOid`),
        format: 0'i16,
        value: some(toBytes(encodeEnumTextArray(labels))),
      )

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

proc getEnumArray*[T: enum](row: Row, col: int): seq[T] =
  ## Read a PostgreSQL enum[] column as ``seq[T]``.
  ## Raises ``PgTypeError`` on NULL column or NULL element.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[T](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in enum array")
      var s = newString(e.len)
      if e.len > 0:
        copyMem(addr s[0], addr row.data.buf[off + e.off], e.len)
      result[i] = parseEnum[T](s)
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      raise newException(PgTypeError, "NULL element in enum array")
    result.add(parseEnum[T](e.get))

proc getEnumArrayOpt*[T: enum](row: Row, col: int): Option[seq[T]] =
  ## NULL-safe column-level variant. Element NULL still raises.
  if row.isNull(col):
    none(seq[T])
  else:
    some(getEnumArray[T](row, col))

proc getEnumArrayElemOpt*[T: enum](row: Row, col: int): seq[Option[T]] =
  ## Element-level NULL-safe: each element is ``Option[T]``.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    result = newSeq[Option[T]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(T)
      else:
        var s = newString(e.len)
        if e.len > 0:
          copyMem(addr s[0], addr row.data.buf[off + e.off], e.len)
        result[i] = some(parseEnum[T](s))
    return
  let s = row.getStr(col)
  for e in parseTextArray(s):
    if e.isNone:
      result.add(none(T))
    else:
      result.add(some(parseEnum[T](e.get)))

# User-defined composite type support

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
  copyMem(addr result[0], addr nf[0], 4)
  var pos = 4
  for f in fields:
    let oid = toBE32(f.oid)
    copyMem(addr result[pos], addr oid[0], 4)
    pos += 4
    if f.data.isNone:
      let nl = toBE32(-1'i32)
      copyMem(addr result[pos], addr nl[0], 4)
      pos += 4
    else:
      let data = f.data.get
      let dl = toBE32(int32(data.len))
      copyMem(addr result[pos], addr dl[0], 4)
      pos += 4
      if data.len > 0:
        copyMem(addr result[pos], addr data[0], data.len)
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
      copyMem(addr val[0], addr buf[fOff], fLen)
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
      copyMem(addr s[0], addr buf[fOff], fLen)
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

# User-defined domain type support

macro pgDomain*(T: typedesc, Base: typedesc, oid: int32 = 0'i32): untyped =
  ## Generate ``toPgParam`` for a Nim distinct type as a PostgreSQL domain type.
  ## Encoding delegates to the base type's ``toPgParam``.
  ## When OID is 0 (default), the base type's OID is used.
  let tSym = T.getType[1]
  let bSym = Base.getType[1]
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `tSym`): PgParam =
      result = toPgParam(`bSym`(v))
      if `oid` != 0'i32:
        result.oid = `oid`

proc getDomain*[T: distinct](row: Row, col: int): T =
  ## Read a PostgreSQL domain column as a Nim distinct type.
  ## The base type determines which row accessor is used.
  when distinctBase(T) is string:
    T(row.getStr(col))
  elif distinctBase(T) is int16:
    T(int16(row.getInt(col)))
  elif distinctBase(T) is int32:
    T(row.getInt(col))
  elif distinctBase(T) is int64:
    T(row.getInt64(col))
  elif distinctBase(T) is float64:
    T(row.getFloat(col))
  elif distinctBase(T) is bool:
    T(row.getBool(col))
  else:
    {.
      error:
        "Unsupported domain base type: use string, int16, int32, int64, float64, or bool"
    .}

proc getDomainOpt*[T: distinct](row: Row, col: int): Option[T] =
  ## NULL-safe version of ``getDomain``.
  if row.isNull(col):
    none(T)
  else:
    some(getDomain[T](row, col))
