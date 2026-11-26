import std/[json, options, macros, strutils, typetraits, times]

import ../pg_protocol
import core, decoding, encoding, accessors, ranges

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

const LabelBearingOids = [OidText, OidVarchar, OidBpchar, OidName, OidChar]
  ## Character types whose binary payload is the label itself, so
  ## ``SELECT status::text`` reads back as an enum.

proc checkEnumOid(accessor: string, actual: int32, kind: string) =
  ## Wire-OID guard for enum reads. Accepts dynamic OIDs and character types;
  ## rejects explicit 0 and other built-ins. ``kind`` is ``"colOid"``/``"elemOid"``.
  if wireOidIsDynamic(actual) or actual in LabelBearingOids:
    return
  if actual == 0'i32:
    raise newException(
      PgTypeError,
      accessor & ": wire " & kind & "=0 is unknown (no type info from RowDescription);" &
        " not readable as an enum in binary (drop the OID metadata or use resultFormat = rfText)",
    )
  raise newException(
    PgTypeError,
    accessor & ": wire " & kind & "=" & $actual &
      " is not an enum (binary column type mismatch)",
  )

proc checkEnumArrayElemOid(accessor: string, actual: int32) =
  ## Binary ``enum[]`` element guard.
  checkEnumOid(accessor, actual, "elemOid")

proc checkEnumColOid(accessor: string, row: Row, col: int) =
  ## Scalar-enum column guard. Missing metadata skips the check; explicit OIDs judged.
  if col < 0 or row.data == nil or row.data.colTypeOids.len <= col:
    return
  checkEnumOid(accessor, row.data.colTypeOids[col], "colOid")

proc encodeEnumTextArray*(
    labels: seq[Option[string]]
): string {.raises: [PgTypeError].} =
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
    # Per element, so an oversized array fails before the literal is built whole.
    checkPgBinLen(result.len + 1, "enum array")
  result.add('}')

macro checkPgEnumType(T: typedesc): untyped =
  ## Reject a non-enum ``T``: ``pgEnum(int32)`` would otherwise shadow the
  ## built-in ``toPgParam(int32)`` with an OID-0 text overload.
  if T.getType[1].typeKind != ntyEnum:
    error("pgEnum requires an enum type, got " & repr(T), T)
  newEmptyNode()

macro pgEnum*(T: untyped): untyped =
  ## Generate ``toPgParam`` overloads for a Nim enum type and its array forms.
  ## OIDs are 0 (unspecified) so PostgreSQL infers the type from context
  ## (use ``$1::mytype`` / ``$1::mytype[]`` in the SQL).
  ## The generated overloads live in the calling module, where the ``sql``
  ## macro and ``pgParams`` resolve them alongside the built-in ones.
  ## ``queryDirect`` / ``execDirect`` take their parameters as
  ## separate arguments and cannot encode these types.
  result = newStmtList(newCall(bindSym"checkPgEnumType", T))
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: 0'i32, format: 0'i16, value: some(toBytes($v)))

    proc toPgParam*(v: seq[`T`]): PgParam {.raises: [PgTypeError].} =
      checkArrayLen(v.len)
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] = some($x)
      PgParam(
        oid: 0'i32, format: 0'i16, value: some(toBytes(encodeEnumTextArray(labels)))
      )

    proc toPgParam*(v: seq[Option[`T`]]): PgParam {.raises: [PgTypeError].} =
      checkArrayLen(v.len)
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
  result = newStmtList(newCall(bindSym"checkPgEnumType", T))
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: int32(`oid`), format: 0'i16, value: some(toBytes($v)))

    proc toPgParam*(v: seq[`T`]): PgParam {.raises: [PgTypeError].} =
      checkArrayLen(v.len)
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] = some($x)
      PgParam(
        oid: 0'i32, format: 0'i16, value: some(toBytes(encodeEnumTextArray(labels)))
      )

    proc toPgParam*(v: seq[Option[`T`]]): PgParam {.raises: [PgTypeError].} =
      checkArrayLen(v.len)
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
  result = newStmtList(newCall(bindSym"checkPgEnumType", T))
  result.add quote do:
    proc toPgParam*(v: `T`): PgParam =
      PgParam(oid: int32(`oid`), format: 0'i16, value: some(toBytes($v)))

    proc toPgParam*(v: seq[`T`]): PgParam {.raises: [PgTypeError].} =
      checkArrayLen(v.len)
      var labels = newSeq[Option[string]](v.len)
      for i, x in v:
        labels[i] = some($x)
      PgParam(
        oid: int32(`arrayOid`),
        format: 0'i16,
        value: some(toBytes(encodeEnumTextArray(labels))),
      )

    proc toPgParam*(v: seq[Option[`T`]]): PgParam {.raises: [PgTypeError].} =
      checkArrayLen(v.len)
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

proc pgParseEnum[T: enum](s: string): T =
  ## Parse an enum label by exact match, raising `PgTypeError` for unknown ones.
  ## Unlike ``parseEnum``, no case/underscore folding: server labels stay distinct.
  for v in T:
    if $v == s:
      return v
  raise newException(
    PgTypeError, "invalid enum value for " & name(T) & " (len=" & $s.len & ")"
  )

proc getEnum*[T: enum](row: Row, col: int): T =
  ## Read a PostgreSQL enum column (text format) as a Nim enum.
  ## The column value must exactly match one of ``T``'s string representations.
  ## In binary, built-in OIDs are rejected (stringified scalars would collide
  ## with labels).
  if row.isBinaryCol(col) and not row.isNull(col):
    checkEnumColOid("getEnum", row, col)
  let s = row.getStr(col)
  withColumnContext(col, pgParseEnum[T](s))

proc getEnumOpt*[T: enum](row: Row, col: int): Option[T] =
  ## Read a PostgreSQL enum column as ``Option[T]``. Returns none if NULL.
  ## NULL-safe version of ``getEnum``.
  if row.isNull(col):
    none(T)
  else:
    some(getEnum[T](row, col))

proc getEnumArray*[T: enum](row: Row, col: int): seq[T] =
  ## Read a PostgreSQL ``enum[]`` column as ``seq[T]``.
  ## Raises ``PgTypeError`` on NULL column or NULL element.
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    if clen == -1:
      raise newException(PgTypeError, "Column " & $col & " is NULL")
    let decoded = withColumnContext(
      col, decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    )
    withColumnContext(col, rejectMultiDim(decoded))
    withColumnContext(col, checkEnumArrayElemOid("getEnumArray", decoded.elemOid))
    result = newSeq[T](decoded.elements.len)
    withColumnContext(col):
      for i, e in decoded.elements:
        if e.len == -1:
          raise newException(PgTypeError, "NULL element in enum array")
        result[i] = pgParseEnum[T](readString(row.data.buf, off + e.off, e.len))
    return
  let s = row.getStr(col)
  withColumnContext(col):
    for e in parseTextArray(s):
      if e.isNone:
        raise newException(PgTypeError, "NULL element in enum array")
      result.add(pgParseEnum[T](e.get))

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
    let decoded = withColumnContext(
      col, decodeBinaryArray(row.data.buf.toOpenArray(off, off + clen - 1))
    )
    withColumnContext(col, rejectMultiDim(decoded))
    withColumnContext(
      col, checkEnumArrayElemOid("getEnumArrayElemOpt", decoded.elemOid)
    )
    result = newSeq[Option[T]](decoded.elements.len)
    withColumnContext(col):
      for i, e in decoded.elements:
        if e.len == -1:
          result[i] = none(T)
        else:
          result[i] = some(pgParseEnum[T](readString(row.data.buf, off + e.off, e.len)))
    return
  let s = row.getStr(col)
  withColumnContext(col):
    for e in parseTextArray(s):
      if e.isNone:
        result.add(none(T))
      else:
        result.add(some(pgParseEnum[T](e.get)))

# User-defined composite type support

proc parseCompositeText*(s: string): seq[Option[string]] =
  ## Parse PostgreSQL composite text format: (val1,val2,...)
  ## Returns fields as ``Option[string]`` (none for NULL).
  ##
  ## Expects canonical ``record_out`` output. Inside a double-quoted field both
  ## doubled bytes (``""`` / ``\\``) and backslash escapes (as accepted by the
  ## server's ``record_in``) are decoded; a quoted field must be followed by
  ## ``','`` or the end. Unquoted fields reject ``"`` / ``\\`` / ``(`` / ``)``
  ## with ``PgTypeError``: ``record_out`` always quotes such bytes, so accepting
  ## them would decode non-canonical input silently.
  if s.len < 2 or s[0] != '(' or s[^1] != ')':
    raise newException(PgTypeError, "Invalid composite literal (len=" & $s.len & ")")
  let inner = s[1 ..^ 2]
  if inner.len == 0:
    # PostgreSQL emits `()` for a 1-field composite whose sole field is NULL;
    # a 0-field composite is not representable, so treat empty as one NULL.
    return @[none(string)]
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
      var closed = false
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
            closed = true
            break
        else:
          elem.add(inner[i])
        i += 1
      if not closed:
        raise newException(PgTypeError, "composite: unterminated quoted field")
      i += 1 # skip closing quote
      # Quoted field must be followed by ',' or end of composite; anything else
      # (e.g. `("a"b)`) is malformed and would otherwise be silently split.
      if i < inner.len and inner[i] != ',':
        raise newException(PgTypeError, "composite: unexpected byte after quoted field")
      result.add(some(elem))
      if i < inner.len and inner[i] == ',':
        i += 1
        if i == inner.len:
          result.add(none(string))
    else:
      # Unquoted field
      var elem = ""
      while i < inner.len and inner[i] != ',':
        # Server-side output quotes fields containing these structural
        # bytes (see compositeFieldToText), so an unquoted occurrence is
        # malformed input that would otherwise decode silently.
        if inner[i] in {'"', '\\', '(', ')'}:
          raise
            newException(PgTypeError, "composite: unexpected byte in unquoted field")
        elem.add(inner[i])
        i += 1
      result.add(some(elem))
      if i < inner.len and inner[i] == ',':
        i += 1
        if i == inner.len:
          result.add(none(string))

proc encodeBinaryComposite*(
    fields: seq[tuple[oid: int32, data: Option[seq[byte]]]]
): seq[byte] {.raises: [PgTypeError, PgProtocolError].} =
  ## Encode a PostgreSQL binary composite value.
  ## Format: ``numFields(4) + [oid(4) + len(4) + data]...``
  checkPgBinLen(fields.len, "Composite field count")
  var size: int64 = 4
  for f in fields:
    size += 8 # oid + len
    if f.data.isSome:
      checkPgBinLen(f.data.get.len, "Composite field")
      size += f.data.get.len.int64
    checkPgBinPayload(size, "Composite")
  result = newSeq[byte](size.int)
  result.writeBE32(0, int32(fields.len))
  var pos = 4
  for f in fields:
    result.writeBE32(pos, f.oid)
    pos += 4
    if f.data.isNone:
      result.writeBE32(pos, -1'i32)
      pos += 4
    else:
      let data = f.data.get
      result.writeBE32(pos, int32(data.len))
      pos += 4
      result.writeBytesAt(pos, data)
      pos += data.len

proc compositeFieldToText(val: string): string =
  ## Escape a composite field value for text format output.
  ## An empty value must be quoted: record input reads an empty unquoted field
  ## as SQL NULL. ``NULL`` (any case) is quoted too, though unlike array input,
  ## record input keeps an unquoted ``NULL`` as text, so that quote is harmless.
  var needsQuote = val.len == 0 or cmpIgnoreCase(val, "NULL") == 0
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

proc encodeCompositeText*(fields: seq[Option[string]]): string {.raises: [].} =
  ## Encode fields as PostgreSQL composite text format: (val1,val2,...)
  result = "("
  for i, f in fields:
    if i > 0:
      result.add(',')
    if f.isSome:
      result.add(compositeFieldToText(f.get))
  result.add(')')

macro distinctBaseOnce*(T: typedesc): typedesc =
  ## Sibling-only: ``distinctBase(T, recursive = false)``. Before Nim 2.2.12
  ## that gives the bare generic for a ``distinct PgRange[DateTime]``, which is
  ## no longer a ``PgRange``.
  let impl = T.getTypeImpl[1].getTypeImpl
  expectKind impl, nnkDistinctTy
  impl[0]

proc isCompositeUtcField(T: typedesc): bool {.compileTime.} =
  ## Fields written as UTC instants: the attribute may be a timestamp,
  ## timestamptz or date (or a range of one), which the text form does not say.
  ## A ``distinct`` type is a domain over its base; ``$`` would write its local
  ## wall clock. ``compositeUtcFieldToText`` follows the same branches.
  when T is PgRange or T is PgMultirange:
    hasDateTimeBounds(T)
  # One level at a time: a multirange is itself distinct.
  elif T is distinct:
    # Via a type: the macro call as the argument crashes the VM (Nim 2.2.14).
    type Base = distinctBaseOnce(T)
    isCompositeUtcField(Base)
  else:
    T is DateTime

proc compositeDateTimeToText(dt: DateTime): string {.raises: [PgTypeError].} =
  ## Text form of a composite DateTime field. The UTC offset is mandatory:
  ## without it a timestamptz field is reinterpreted in the session TimeZone.
  pgUtcInstantText(dt, " in composite field")

proc compositeUtcFieldToText[F](v: F): string {.raises: [PgTypeError].} =
  ## A field for which ``isCompositeUtcField`` holds. A range's bounds are
  ## written as lone fields.
  when F is PgRange:
    formatDateTimeRangeText(v, compositeDateTimeToText)
  elif F is PgMultirange:
    formatDateTimeMultirangeText(v, compositeDateTimeToText)
  elif F is distinct:
    compositeUtcFieldToText(distinctBaseOnce(F)(v))
  else:
    compositeDateTimeToText(v)

# Composite field text form. The field type chooses the encoder/decoder, not
# the caller's `$` (`$seq` writes a Nim repr and a domain has no `$` at all).

proc compositeFieldText[T](v: T): string
proc compositeFieldFromText[T](s: string): T

template arrayElem(A: typedesc[array]): typedesc =
  typeof(default(A)[default(A).low])

proc compositeSeqToArray[A, E](s: seq[E], _: typedesc[A]): A =
  if s.len != default(A).len:
    raise newException(
      PgTypeError,
      "array field: " & $s.len & " elements for a Nim array of " & $default(A).len,
    )
  var i = 0
  for x in result.mitems:
    x = s[i]
    inc i

const CompositeHexChars = "0123456789abcdef"

proc appendCompositeArrayElement(result: var string, elem: Option[string]) =
  ## Append one array element, quoted. Quoting is valid for every element type,
  ## and NULL must stay unquoted.
  if elem.isNone:
    result.add("NULL")
    return
  result.add('"')
  for c in elem.get:
    if c == '"' or c == '\\':
      result.add('\\')
    result.add(c)
  result.add('"')

proc encodeCompositeByteaText(v: seq[byte]): string =
  checkPgBinPayload(int64(v.len) * 2 + 2, "bytea field")
  result = newString(v.len * 2 + 2)
  result[0] = '\\'
  result[1] = 'x'
  var pos = 2
  for b in v:
    result[pos] = CompositeHexChars[int(b) shr 4]
    result[pos + 1] = CompositeHexChars[int(b) and 0x0F]
    pos += 2

proc compositeArrayDelim[T](): char =
  ## `box` is the one built-in type whose array delimiter is ';'; a domain
  ## inherits its base type's.
  when T is Option:
    compositeArrayDelim[typeof(default(T).get)]()
  elif T is PgBox:
    ';'
  elif T is distinct:
    compositeArrayDelim[distinctBaseOnce(T)]()
  else:
    ','

macro isReprDollar(call, arg: typed): bool =
  ## Whether `call` (`$arg`) resolved to std's generic object/tuple `$`, whose
  ## `(field: value)` repr is no PostgreSQL literal. A template `$` of the
  ## type's own arrives expanded, so the std call must take `arg` itself.
  if call.kind notin nnkCallKinds or call.len != 2 or call[0].kind != nnkSym:
    return newLit(false)
  var a = call[1]
  while a.kind in {nnkHiddenDeref, nnkHiddenAddr, nnkHiddenStdConv, nnkHiddenSubConv}:
    a = a[^1]
  if a.kind != nnkSym or a != arg:
    return newLit(false)
  let file = call[0].getImpl.lineInfoObj.filename.replace('\\', '/')
  newLit(file.endsWith("/std/objectdollar.nim") or file.endsWith("/system/dollars.nim"))

template isByteArray(T: typedesc): bool =
  when T is array:
    arrayElem(T) is byte
  else:
    false

proc compositeSetElem[E](v: set[E]): E =
  ## Type helper: the member type of a set field.
  discard

proc compositeArrayElementText[T](v: T): string =
  when T is seq[byte] or isByteArray(T):
    # bytea is a legal element type (`bytea[]`).
    compositeFieldText(v)
  elif T is seq or T is array or T is PgArray or T is set:
    {.
      error:
        "pgComposite: an array field cannot hold another array; use PgArray[T] for more dimensions"
    .}
  else:
    compositeFieldText(v)

proc encodeCompositeSeqText[T](v: seq[T]): string =
  result = "{"
  for i, e in v:
    if i > 0:
      result.add(compositeArrayDelim[T]())
    when T is Option:
      if e.isSome:
        appendCompositeArrayElement(result, some(compositeArrayElementText(e.get)))
      else:
        appendCompositeArrayElement(result, none(string))
    else:
      appendCompositeArrayElement(result, some(compositeArrayElementText(e)))
  result.add('}')

proc appendCompositeArrayDim[T](
    elements: seq[Option[T]],
    dims: seq[int32],
    dim: int,
    pos: var int,
    result: var string,
) =
  result.add('{')
  for i in 0 ..< int(dims[dim]):
    if i > 0:
      result.add(compositeArrayDelim[T]())
    if dim + 1 == dims.len:
      let e = elements[pos]
      inc pos
      if e.isSome:
        appendCompositeArrayElement(result, some(compositeArrayElementText(e.get)))
      else:
        appendCompositeArrayElement(result, none(string))
    else:
      appendCompositeArrayDim(elements, dims, dim + 1, pos, result)
  result.add('}')

proc encodeCompositePgArrayText[T](v: PgArray[T]): string =
  v.validate()
  if v.dims.len == 0:
    return "{}"
  for d in 0 ..< v.dims.len:
    let lower = int64(v.lowerBounds[d])
    let upper = lower + int64(v.dims[d]) - 1
    if lower < int64(int32.low) or upper > int64(int32.high):
      raise newException(PgTypeError, "pgComposite: array field bounds exceed int32")
    result.add("[" & $lower & ":" & $upper & "]")
  result.add('=')
  var pos = 0
  appendCompositeArrayDim(v.elements, v.dims, 0, pos, result)
  if pos != v.elements.len:
    raise newException(PgTypeError, "pgComposite: array field shape mismatch")

proc compositeRangeText[E](v: PgRange[E]): string =
  ## Bounds go through ``compositeFieldText``, as the reader parses them, so a
  ## domain element is written by its base type.
  if v.isEmpty:
    return "empty"
  result = if v.hasLower and v.lower.inclusive: "[" else: "("
  if v.hasLower:
    result.add(quoteRangeElem(compositeFieldText(v.lower.value)))
  result.add(',')
  if v.hasUpper:
    result.add(quoteRangeElem(compositeFieldText(v.upper.value)))
  result.add(if v.hasUpper and v.upper.inclusive: "]" else: ")")

proc compositeMultirangeText[E](v: PgMultirange[E]): string =
  result = "{"
  for i, r in seq[PgRange[E]](v):
    if i > 0:
      result.add(',')
    result.add(compositeRangeText(r))
  result.add('}')

proc hasCompositeCodec(T: typedesc): bool {.compileTime.} =
  ## Whether ``compositeFieldText`` maps ``T`` to a PostgreSQL type itself.
  ## A domain's base does; ``Lsn`` (``distinct uint64``) is no domain.
  when T is PgMultirange or T is PgTsVector or T is PgTsQuery:
    true
  elif T is distinct:
    when compiles(pgArrayAcceptOids(T)):
      true
    else:
      type Base = distinctBaseOnce(T)
      hasCompositeCodec(Base)
  else:
    T is (
      string or int16 or int32 or int64 or int or float32 or float64 or bool or enum or
      seq or array or PgArray or object or tuple or ref or Option or set
    ) or compiles(pgArrayAcceptOids(T))

proc compositeFieldText[T](v: T): string =
  ## Text form of one composite field.
  when T is Option:
    {.
      error:
        "pgComposite: a nested Option is not supported; seq[Option[T]] and PgArray[T] already hold NULL elements"
    .}
  elif T is set:
    # A 1-D array of its members, ascending; array_in reads 0.5.0's `{1, 2}`
    # as the same value.
    var members: seq[typeof(compositeSetElem(v))]
    for e in v:
      members.add(e)
    encodeCompositeSeqText(members)
  elif isCompositeUtcField(T):
    compositeUtcFieldToText(v)
  elif T is PgHstore:
    encodeHstoreText(v)
  elif T is PgRange:
    compositeRangeText(v)
  elif T is PgMultirange:
    compositeMultirangeText(v)
  elif T is seq[byte]:
    encodeCompositeByteaText(v)
  elif T is seq:
    encodeCompositeSeqText(v)
  elif T is array:
    when arrayElem(T) is byte:
      encodeCompositeByteaText(@v)
    else:
      encodeCompositeSeqText(@v)
  elif T is PgArray:
    encodeCompositePgArrayText(v)
  elif T is enum:
    $v
  elif T is JsonNode:
    # A nil JsonNode is not JSON null: std/json's `$` dereferences nil. Use
    # none(JsonNode) for a SQL NULL.
    if v.isNil:
      raise newException(
        PgTypeError,
        "pgComposite: a nil JsonNode field is not JSON null; use newJNull() for" &
          " JSON null, or none(JsonNode) in an Option[JsonNode] field for a SQL NULL",
      )
    $v
  elif T is object or T is tuple:
    # A `$` of the type's own is written verbatim, as before;
    # std's generic `$` gives a `(field: value)` repr, no composite literal.
    when T is PgMoney or compiles(parseRegisteredScalar(T, "")):
      $v
    elif not isReprDollar($v, v):
      $v
    else:
      raise newException(
        PgTypeError,
        "pgComposite: unsupported field type " & name(T) &
          " (a nested composite or object field needs a `$` of its own)",
      )
  elif T is distinct:
    # Library distinct types are in the array element registry, or are
    # text-search types with their own `$`. A domain is neither, so its base
    # type decides; a custom `$` on a domain is not called. A base with no
    # PostgreSQL type (`Lsn` over uint64) makes no domain: its `$` is written.
    when compiles(pgArrayAcceptOids(T)) or T is PgTsVector or T is PgTsQuery:
      $v
    else:
      type Base = distinctBaseOnce(T)
      when not hasCompositeCodec(Base) and compiles($v):
        $v
      else:
        compositeFieldText(Base(v))
  else:
    when compiles($v):
      $v
    else:
      raise newException(
        PgTypeError,
        "pgComposite: unsupported field type " & name(T) &
          " (it has no text form this library can write)",
      )

proc parseCompositeByteaText(s: string): seq[byte] =
  if s.len >= 2 and s[0] == '\\' and s[1] == 'x':
    let hexLen = s.len - 2
    if hexLen mod 2 != 0:
      raise newException(PgTypeError, "bytea field: odd-length hex text")
    result = newSeq[byte](hexLen div 2)
    for i in 0 ..< result.len:
      result[i] = decodeHexPair(s, 2 + i * 2, "bytea field")
  else:
    result = decodeByteaEscape(s.toOpenArray(0, s.high), "bytea field")

proc compositeArrayElementFromText[T](s: string): T =
  when T is seq[byte] or isByteArray(T):
    compositeFieldFromText[T](s)
  elif T is seq or T is array or T is PgArray or T is set:
    {.
      error:
        "pgComposite: an array field cannot hold another array; use PgArray[T] for more dimensions"
    .}
  else:
    compositeFieldFromText[T](s)

proc compositeRangeElemFromText[E](s: string): E {.gcsafe.} =
  compositeFieldFromText[E](s)

proc compositeRangeFromText[E](s: string, _: PgRange[E]): PgRange[E] =
  parseRangeText[E](s, compositeRangeElemFromText[E])

proc compositeMultirangeFromText[E](s: string, _: PgMultirange[E]): PgMultirange[E] =
  parseMultirangeText[E](s, compositeRangeElemFromText[E])

proc parseCompositeArrayElement(s: string, i: var int, delim: char): Option[string] =
  ## One element of an N-dimensional array literal. Quoting and NULL follow
  ## the array text format.
  if s[i] == '"':
    inc i
    var value = ""
    while i < s.len:
      let c = s[i]
      if c == '\\':
        if i + 1 >= s.len:
          raise newException(PgTypeError, "array field: trailing backslash")
        value.add(s[i + 1])
        i += 2
      elif c == '"':
        inc i
        return some(value)
      else:
        value.add(c)
        inc i
    raise newException(PgTypeError, "array field: unterminated quoted element")
  var value = ""
  while i < s.len and s[i] != delim and s[i] != '}':
    value.add(s[i])
    inc i
  if value.len == 0:
    raise newException(PgTypeError, "array field: empty element")
  if cmpIgnoreCase(value, "NULL") == 0:
    none(string)
  else:
    some(value)

proc parseCompositeArrayLevel(
    s: string,
    i: var int,
    dims: var seq[int32],
    elements: var seq[Option[string]],
    depth: int,
    delim: char,
    leafDepth: var int,
) =
  ## Parse one `{...}` level, recording every dimension length in `dims`.
  ## `leafDepth` is the depth of the first scalar (-1 before one is seen);
  ## every scalar must sit at that depth.
  if depth >= int(PgArrayMaxDim):
    raise newException(PgTypeError, "array field: too many dimensions")
  inc i # '{'
  if i < s.len and s[i] == '}':
    inc i
    if depth != 0:
      raise newException(PgTypeError, "array field: empty subarray")
    return
  var count = 0
  var sawScalar = false
  var sawSubarray = false
  while true:
    if i >= s.len:
      raise newException(PgTypeError, "array field: unterminated literal")
    if s[i] == '{':
      parseCompositeArrayLevel(s, i, dims, elements, depth + 1, delim, leafDepth)
      sawSubarray = true
    else:
      if leafDepth == -1:
        leafDepth = depth
      elif leafDepth != depth:
        raise newException(PgTypeError, "array field: irregular dimensions")
      elements.add(parseCompositeArrayElement(s, i, delim))
      sawScalar = true
    inc count
    if count > int(int32.high):
      raise newException(PgTypeError, "array field: too many elements")
    if i >= s.len:
      raise newException(PgTypeError, "array field: unterminated literal")
    if s[i] == delim:
      inc i
    elif s[i] == '}':
      inc i
      break
    else:
      raise newException(PgTypeError, "array field: unexpected byte after element")
  if sawScalar and sawSubarray:
    raise newException(PgTypeError, "array field: irregular dimensions")
  while dims.len <= depth:
    dims.add(-1'i32)
  if dims[depth] == -1:
    dims[depth] = int32(count)
  elif dims[depth] != int32(count):
    raise newException(PgTypeError, "array field: irregular dimensions")

proc parseCompositeArrayBounds(
    s: string, i: var int, lowerBounds, prefixDims: var seq[int32]
) =
  ## Optional `[l:u]...=` prefix, which PostgreSQL prints when a lower bound
  ## is not 1. Leaves `i` at the opening '{'.
  while i < s.len and s[i] == '[':
    let close = s.find(']', i + 1)
    if close < 0:
      raise newException(PgTypeError, "array field: unterminated bounds")
    let spec = s[i + 1 ..< close]
    let colon = spec.find(':')
    if colon < 0:
      raise newException(PgTypeError, "array field: malformed bounds")
    let lower = pgParseInt32(spec[0 ..< colon])
    let upper = pgParseInt32(spec[colon + 1 .. ^1])
    if upper < lower:
      raise newException(PgTypeError, "array field: upper bound below lower")
    let dim = int64(upper) - int64(lower) + 1
    if dim > int64(int32.high):
      raise newException(PgTypeError, "array field: dimension too large")
    lowerBounds.add(lower)
    prefixDims.add(int32(dim))
    i = close + 1
  if lowerBounds.len > 0:
    if i >= s.len or s[i] != '=':
      raise newException(PgTypeError, "array field: bounds without '='")
    inc i
  if i >= s.len or s[i] != '{':
    raise newException(PgTypeError, "array field: expected '{'")

proc parseCompositeDelimSeqText(
    s: string, start: int, delim: char
): seq[Option[string]] =
  ## 1-D literal of an element type whose delimiter is not ','.
  var i = start
  var dims: seq[int32]
  var leafDepth = -1
  parseCompositeArrayLevel(s, i, dims, result, 0, delim, leafDepth)
  if i != s.len:
    raise newException(PgTypeError, "array field: trailing bytes")
  if dims.len > 1:
    raise newException(
      PgTypeError, "array field: a seq field reads one dimension; use PgArray[T]"
    )

proc parseCompositeSeqText[E](s: string, _: seq[E]): seq[E] =
  ## A seq has no lower bound, so a `[l:u]=` prefix is checked and dropped,
  ## as the binary read drops the header's bound.
  let delim = compositeArrayDelim[E]()
  var i = 0
  var lowerBounds, prefixDims: seq[int32]
  parseCompositeArrayBounds(s, i, lowerBounds, prefixDims)
  if prefixDims.len > 1:
    raise newException(
      PgTypeError, "array field: a seq field reads one dimension; use PgArray[T]"
    )
  let parts =
    if delim == ',':
      if i == 0:
        parseTextArray(s)
      else:
        parseTextArray(s[i .. ^1])
    else:
      parseCompositeDelimSeqText(s, i, delim)
  if prefixDims.len == 1 and parts.len != int(prefixDims[0]):
    raise newException(PgTypeError, "array field: bounds do not match dimensions")
  when E is Option:
    type Inner = typeof(default(E).get)
    result = newSeq[Option[Inner]](parts.len)
    for i, e in parts:
      if e.isSome:
        result[i] = some(compositeArrayElementFromText[Inner](e.get))
      else:
        result[i] = none(Inner)
  else:
    result = newSeq[E](parts.len)
    for i, e in parts:
      if e.isNone:
        raise newException(PgTypeError, "NULL element in a composite array field")
      result[i] = compositeArrayElementFromText[E](e.get)

proc parseCompositePgArrayText[E](s: string, _: PgArray[E]): PgArray[E] =
  var i = 0
  var lowerBounds: seq[int32]
  var prefixDims: seq[int32]
  parseCompositeArrayBounds(s, i, lowerBounds, prefixDims)
  var dims: seq[int32]
  var rawElements: seq[Option[string]]
  var leafDepth = -1
  parseCompositeArrayLevel(
    s, i, dims, rawElements, 0, compositeArrayDelim[E](), leafDepth
  )
  if i != s.len:
    raise newException(PgTypeError, "array field: trailing bytes")
  if lowerBounds.len > 0:
    if lowerBounds.len != dims.len:
      raise newException(PgTypeError, "array field: bounds do not match dimensions")
    for d in 0 ..< dims.len:
      if prefixDims[d] != dims[d]:
        raise newException(PgTypeError, "array field: bounds do not match dimensions")
  else:
    lowerBounds = newSeq[int32](dims.len)
    for d in 0 ..< dims.len:
      lowerBounds[d] = 1'i32
  var elements = newSeq[Option[E]](rawElements.len)
  for j, e in rawElements:
    if e.isSome:
      elements[j] = some(compositeArrayElementFromText[E](e.get))
  result = PgArray[E](dims: dims, lowerBounds: lowerBounds, elements: elements)
  result.validate()

proc compositeSetFromText[E](s: string, _: set[E]): set[E] =
  for e in parseCompositeSeqText(s, default(seq[E])):
    result.incl(e)

proc compositeFieldFromText[T](s: string): T =
  ## Parse a single composite text field to the target type.
  when T is Option:
    {.
      error:
        "pgComposite: a nested Option is not supported; seq[Option[T]] and PgArray[T] already hold NULL elements"
    .}
  elif T is set:
    compositeSetFromText(s, default(T))
  elif T is string:
    s
  elif T is int32:
    pgParseInt32(s)
  elif T is int16:
    pgParseInt16(s)
  elif T is int64:
    pgParseBiggestInt(s)
  elif T is int:
    pgParseInt(s)
  elif T is float64:
    pgParseFloat(s)
  elif T is float32:
    pgParseFloat32(s)
  elif T is bool:
    parsePgBoolText(s)
  elif T is PgNumeric:
    parsePgNumeric(s)
  elif T is DateTime:
    # The text form does not name the attribute type; under the pinned ISO
    # DateStyle only a timestamp has a clock. Infinity stays with the
    # timestamp parser so its error is unchanged.
    if ':' in s or s.endsWith("infinity"):
      parseTimestampText(s)
    else:
      parseDateText(s)
  elif T is PgMoney:
    parsePgMoney(s)
  elif T is PgHstore:
    parseHstoreText(s)
  elif T is seq[byte]:
    parseCompositeByteaText(s)
  elif T is PgTsVector:
    PgTsVector(s)
  elif T is PgTsQuery:
    PgTsQuery(s)
  elif T is enum:
    pgParseEnum[T](s)
  elif T is PgRange:
    compositeRangeFromText(s, default(T))
  elif T is PgMultirange:
    compositeMultirangeFromText(s, default(T))
  elif T is seq:
    parseCompositeSeqText(s, default(T))
  elif T is array:
    when arrayElem(T) is byte:
      compositeSeqToArray(parseCompositeByteaText(s), T)
    else:
      compositeSeqToArray(parseCompositeSeqText(s, default(seq[arrayElem(T)])), T)
  elif T is PgArray:
    parseCompositePgArrayText(s, default(T))
  elif T is distinct:
    when compiles(parseRegisteredScalar(T, s)):
      parseRegisteredScalar(T, s)
    else:
      type Base = distinctBaseOnce(T)
      T(compositeFieldFromText[Base](s))
  else:
    when compiles(parseRegisteredScalar(T, s)):
      parseRegisteredScalar(T, s)
    else:
      raise newException(
        PgTypeError,
        "pgComposite: unsupported field type " & name(T) &
          " (it has no text form this library can read; cast the attribute in SQL)",
      )

template checkFieldLen(actual, expected: int, typeName: string) =
  # Guard against DB-column / Nim-field width mismatch: without this,
  # a wider wire value would silently truncate and a narrower one IndexDefect.
  if actual != expected:
    raise newException(
      PgTypeError,
      "Composite binary field length " & $actual & " does not match " & typeName &
        " (expected " & $expected & ")",
    )

proc dynamicOidHint(actual: int32): string =
  ## Only a user-defined OID has another way to read it; a built-in mismatch
  ## fails in text format too.
  if wireOidIsDynamic(actual):
    " (a user-defined type: domain, enum, composite, range or extension; read" &
      " a domain into a `distinct` of its base type, an enum into a pgEnum" &
      " type, or use resultFormat = rfText)"
  else:
    ""

template checkFieldOid(
    actual: int32,
    allowed: openArray[int32],
    typeName: string,
    allowDynamic: bool = false,
) =
  # Same-width types (int4/float4, int8/float8/timestamp) share a length so
  # checkFieldLen alone lets them silently misdecode. actual == 0 → unknown/
  # polymorphic column; fall back to the length guard. A domain's OID is
  # dynamic and its payload follows the base type, so `allowDynamic` widens
  # the check on that path.
  if actual != 0'i32 and actual notin allowed and
      not (allowDynamic and wireOidIsDynamic(actual)):
    raise newException(
      PgTypeError,
      "Composite binary field OID " & $actual & " incompatible with Nim " & typeName &
        dynamicOidHint(actual),
    )

proc rejectCompositeOid(actual: int32, typeName: string, what: string) {.noreturn.} =
  raise newException(
    PgTypeError,
    "Composite binary " & what & " OID " & $actual & " incompatible with Nim " & typeName &
      dynamicOidHint(actual),
  )

const BuiltinArrayOids = [
  OidBoolArray, OidInt2Array, OidInt4Array, OidInt8Array, OidFloat4Array,
  OidFloat8Array, OidTextArray, OidVarcharArray, OidBpcharArray, OidNameArray,
  OidCharArray, OidInt4RangeArray, OidNumRangeArray, OidTsRangeArray, OidTsTzRangeArray,
  OidDateRangeArray, OidInt8RangeArray, OidInt4MultirangeArray, OidNumMultirangeArray,
  OidTsMultirangeArray, OidTsTzMultirangeArray, OidDateMultirangeArray,
  OidInt8MultirangeArray, OidByteaArray, OidTimestampArray, OidDateArray, OidTimeArray,
  OidTimestampTzArray, OidIntervalArray, OidNumericArray, OidMoneyArray, OidTimeTzArray,
  OidUuidArray, OidJsonbArray, OidJsonArray, OidInetArray, OidCidrArray,
  OidMacAddrArray, OidMacAddr8Array, OidPointArray, OidLsegArray, OidPathArray,
  OidBoxArray, OidPolygonArray, OidLineArray, OidCircleArray, OidXmlArray,
  OidTsVectorArray, OidTsQueryArray, OidBitArray, OidVarbitArray,
]

proc checkArrayFieldOid(actual: int32, typeName: string) =
  ## The payload is decoded as an array header, so a non-array attribute must
  ## not reach it. The element OID in the header is checked separately.
  if actual != 0'i32 and not wireOidIsDynamic(actual) and actual notin BuiltinArrayOids:
    rejectCompositeOid(actual, typeName, "field")

proc checkDynamicFieldOid(actual: int32, typeName: string) =
  ## Attributes that only a dynamic OID can name (extension types, enums).
  if actual != 0'i32 and not wireOidIsDynamic(actual):
    rejectCompositeOid(actual, typeName, "field")

proc compositeParseJson(s: string): JsonNode =
  try:
    parseJson(s)
  except ValueError, IOError, OSError:
    raise newException(
      PgTypeError, "Invalid JSON in a composite field (len=" & $s.len & ")"
    )

proc checkCompositeElemOid[T](actual: int32) =
  ## Array element OID guard for composite array fields: built-in types are
  ## strict, enums and domains may carry a dynamic OID. Unlike the column
  ## accessors, element OID 0 is accepted, as for composite fields.
  when T is Option:
    type Inner = typeof(default(T).get)
    checkCompositeElemOid[Inner](actual)
  elif T is DateTime:
    if actual != 0 and actual notin [OidTimestamp, OidTimestampTz, OidDate]:
      rejectCompositeOid(actual, name(T), "array element")
  elif T is enum:
    if actual != 0 and not wireOidIsDynamic(actual) and actual notin LabelBearingOids:
      rejectCompositeOid(actual, name(T), "array element")
  elif T is int:
    if actual != 0 and actual != OidInt8:
      rejectCompositeOid(actual, "int", "array element")
  elif T is seq[byte]:
    if actual != 0 and actual != OidBytea:
      rejectCompositeOid(actual, "bytea", "array element")
  elif T is JsonNode:
    if actual != 0 and actual notin [OidJson, OidJsonb]:
      rejectCompositeOid(actual, "JsonNode", "array element")
  elif T is PgTsVector:
    if actual != 0 and actual != OidTsVector:
      rejectCompositeOid(actual, name(T), "array element")
  elif T is PgTsQuery:
    if actual != 0 and actual != OidTsQuery:
      rejectCompositeOid(actual, name(T), "array element")
  elif T is PgMoney:
    raise newException(
      PgTypeError,
      "pgComposite: binary money array elements are not supported;" &
        " use resultFormat = rfText",
    )
  elif T is PgRange or T is PgMultirange:
    raise newException(
      PgTypeError,
      "pgComposite: binary " & name(T) & " array elements are not supported;" &
        " use resultFormat = rfText",
    )
  elif T is distinct:
    when compiles(pgArrayAcceptOids(T)):
      if actual != 0 and actual notin pgArrayAcceptOids(T):
        rejectCompositeOid(actual, name(T), "array element")
    else:
      # A domain array names its elements by the domain's dynamic OID or, for
      # an array of the base type, by the base's.
      if not wireOidIsDynamic(actual):
        checkCompositeElemOid[distinctBaseOnce(T)](actual)
  elif isByteArray(T):
    if actual != 0 and actual != OidBytea:
      rejectCompositeOid(actual, "bytea", "array element")
  elif T is seq or T is array or T is PgArray or T is set:
    discard # Rejected at compile time by compositeArrayElementFromBinary.
  else:
    when compiles(pgArrayAcceptOids(T)):
      if actual != 0 and actual notin pgArrayAcceptOids(T):
        rejectCompositeOid(actual, name(T), "array element")
    else:
      if actual != 0 and not wireOidIsDynamic(actual):
        rejectCompositeOid(actual, name(T), "array element")

proc compositeFieldFromBinary[T](
  buf: openArray[byte], off, len: int, oid: int32, allowDynamicOid: bool
): T

proc compositeArrayElementFromBinary[T](
    buf: openArray[byte], off, len: int, oid: int32
): T =
  when T is seq[byte]:
    if oid != 0 and oid != OidBytea:
      rejectCompositeOid(oid, "bytea", "array element")
    readBytes(buf, off, len)
  elif isByteArray(T):
    if oid != 0 and oid != OidBytea:
      rejectCompositeOid(oid, "bytea", "array element")
    compositeSeqToArray(readBytes(buf, off, len), T)
  elif T is seq or T is array or T is PgArray or T is set:
    {.
      error:
        "pgComposite: an array field cannot hold another array; use PgArray[T] for more dimensions"
    .}
  else:
    compositeFieldFromBinary[T](buf, off, len, oid, false)

proc compositeSeqFromBinary[E](buf: openArray[byte], off, len: int, _: seq[E]): seq[E] =
  let decoded = decodeBinaryArray(buf.toOpenArray(off, off + len - 1))
  rejectMultiDim(decoded)
  checkCompositeElemOid[E](decoded.elemOid)
  when E is Option:
    type Inner = typeof(default(E).get)
    result = newSeq[Option[Inner]](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        result[i] = none(Inner)
      else:
        result[i] = some(
          compositeArrayElementFromBinary[Inner](
            buf, off + e.off, e.len, decoded.elemOid
          )
        )
  else:
    result = newSeq[E](decoded.elements.len)
    for i, e in decoded.elements:
      if e.len == -1:
        raise newException(PgTypeError, "NULL element in a composite array field")
      result[i] =
        compositeArrayElementFromBinary[E](buf, off + e.off, e.len, decoded.elemOid)

proc compositePgArrayFromBinary[E](
    buf: openArray[byte], off, len: int, _: PgArray[E]
): PgArray[E] =
  let decoded = decodeBinaryArray(buf.toOpenArray(off, off + len - 1))
  checkCompositeElemOid[E](decoded.elemOid)
  result.dims = decoded.dims
  result.lowerBounds = decoded.lowerBounds
  result.elements = newSeq[Option[E]](decoded.elements.len)
  for i, e in decoded.elements:
    if e.len == -1:
      result.elements[i] = none(E)
    else:
      result.elements[i] = some(
        compositeArrayElementFromBinary[E](buf, off + e.off, e.len, decoded.elemOid)
      )
  result.validate()

proc compositeRegistered[T](
    buf: openArray[byte], off, len: int, oid: int32, allowDynamicOid: bool
): T =
  when compiles(pgArrayAcceptOids(T)):
    checkFieldOid(oid, pgArrayAcceptOids(T), name(T), allowDynamicOid)
  elif T is PgTsVector:
    checkFieldOid(oid, [OidTsVector], name(T), allowDynamicOid)
  elif T is PgTsQuery:
    checkFieldOid(oid, [OidTsQuery], name(T), allowDynamicOid)
  else:
    checkDynamicFieldOid(oid, name(T))
  decodeRegisteredScalar(T, buf.toOpenArray(off, off + len - 1))

proc compositeFieldFromBinary[T](
    buf: openArray[byte], off, len: int, oid: int32, allowDynamicOid: bool
): T =
  when T is Option:
    {.
      error:
        "pgComposite: a nested Option is not supported; seq[Option[T]] and PgArray[T] already hold NULL elements"
    .}
  elif T is set:
    checkArrayFieldOid(oid, name(T))
    type E = typeof(compositeSetElem(result))
    for e in compositeSeqFromBinary(buf, off, len, default(seq[E])):
      result.incl(e)
  elif T is DateTime:
    # OID 0 names no type, so the width picks the decoder.
    if oid == OidDate or
        ((oid == 0 or (allowDynamicOid and wireOidIsDynamic(oid))) and len == 4):
      checkFieldLen(len, 4, "date")
      result = decodeBinaryDate(buf.toOpenArray(off, off + len - 1))
    else:
      checkFieldOid(oid, [OidTimestamp, OidTimestampTz], "DateTime", allowDynamicOid)
      checkFieldLen(len, 8, "DateTime")
      result = decodeBinaryTimestamp(buf.toOpenArray(off, off + len - 1))
  elif T is int16:
    checkFieldOid(oid, [OidInt2], "int16", allowDynamicOid)
    checkFieldLen(len, 2, "int16")
    result = fromBE16(buf.toOpenArray(off, off + len - 1))
  elif T is int32:
    checkFieldOid(oid, [OidInt4], "int32", allowDynamicOid)
    checkFieldLen(len, 4, "int32")
    result = fromBE32(buf.toOpenArray(off, off + len - 1))
  elif T is (int64 or int):
    checkFieldOid(oid, [OidInt8], "int64", allowDynamicOid)
    checkFieldLen(len, 8, "int64")
    result = typeof(result)(fromBE64(buf.toOpenArray(off, off + len - 1)))
  elif T is float64:
    checkFieldOid(oid, [OidFloat8], "float64", allowDynamicOid)
    checkFieldLen(len, 8, "float64")
    result = decodeFloat64BE(buf.toOpenArray(off, off + len - 1))
  elif T is float32:
    checkFieldOid(oid, [OidFloat4], "float32", allowDynamicOid)
    checkFieldLen(len, 4, "float32")
    result = decodeFloat32BE(buf.toOpenArray(off, off + len - 1))
  elif T is bool:
    checkFieldOid(oid, [OidBool], "bool", allowDynamicOid)
    checkFieldLen(len, 1, "bool")
    result = buf[off] != 0
  elif T is string:
    checkFieldOid(oid, LabelBearingOids, "string", allowDynamicOid)
    result = readString(buf, off, len)
  elif T is PgNumeric:
    checkFieldOid(oid, [OidNumeric], "PgNumeric", allowDynamicOid)
    result = decodeNumericBinary(buf.toOpenArray(off, off + len - 1))
  elif T is seq[byte]:
    checkFieldOid(oid, [OidBytea], "bytea", allowDynamicOid)
    result = readBytes(buf, off, len)
  elif T is enum:
    if oid != 0 and not wireOidIsDynamic(oid) and oid notin LabelBearingOids:
      rejectCompositeOid(oid, name(T), "field")
    result = pgParseEnum[T](readString(buf, off, len))
  elif T is PgHstore:
    checkDynamicFieldOid(oid, "PgHstore")
    result = decodeHstoreBinary(buf.toOpenArray(off, off + len - 1))
  elif T is PgRange or T is PgMultirange:
    raise newException(
      PgTypeError,
      "pgComposite: binary " & name(T) & " fields are not supported" &
        " (use resultFormat = rfText)",
    )
  elif T is PgMoney:
    raise newException(
      PgTypeError,
      "pgComposite: a binary money field does not carry the fractional digits;" &
        " use resultFormat = rfText",
    )
  elif T is JsonNode:
    if oid == OidJsonb:
      if len < 1:
        raise newException(PgTypeError, "Composite jsonb field is empty")
      if buf[off] != 1'u8:
        raise newException(
          PgTypeError, "Composite jsonb field: unsupported version " & $buf[off]
        )
      result = compositeParseJson(readString(buf, off + 1, len - 1))
    elif oid == OidJson or oid == 0:
      result = compositeParseJson(readString(buf, off, len))
    elif allowDynamicOid and wireOidIsDynamic(oid):
      # A domain over json or jsonb: the payload follows the base type, which
      # the OID does not name. jsonb carries a version byte; json text cannot
      # start with 0x01, so the first byte tells them apart.
      if len >= 1 and buf[off] == 1'u8:
        result = compositeParseJson(readString(buf, off + 1, len - 1))
      else:
        result = compositeParseJson(readString(buf, off, len))
    else:
      rejectCompositeOid(oid, "JsonNode", "field")
  elif T is seq:
    checkArrayFieldOid(oid, name(T))
    result = compositeSeqFromBinary(buf, off, len, default(T))
  elif T is array:
    when arrayElem(T) is byte:
      result = compositeSeqToArray(
        compositeFieldFromBinary[seq[byte]](buf, off, len, oid, allowDynamicOid), T
      )
    else:
      checkArrayFieldOid(oid, name(T))
      result = compositeSeqToArray(
        compositeSeqFromBinary(buf, off, len, default(seq[arrayElem(T)])), T
      )
  elif T is PgArray:
    checkArrayFieldOid(oid, name(T))
    result = compositePgArrayFromBinary(buf, off, len, default(T))
  elif T is distinct:
    when compiles(decodeRegisteredScalar(T, buf.toOpenArray(off, off + len - 1))):
      result = compositeRegistered[T](buf, off, len, oid, allowDynamicOid)
    else:
      type Base = distinctBaseOnce(T)
      result = T(compositeFieldFromBinary[Base](buf, off, len, oid, true))
  elif T is object:
    when compiles(decodeRegisteredScalar(T, buf.toOpenArray(off, off + len - 1))):
      result = compositeRegistered[T](buf, off, len, oid, allowDynamicOid)
    else:
      raise newException(
        PgTypeError,
        "pgComposite: unsupported field type " & name(T) &
          " (it has no form this library can read; cast the attribute in SQL)",
      )
  else:
    when compiles(decodeRegisteredScalar(T, buf.toOpenArray(off, off + len - 1))):
      result = compositeRegistered[T](buf, off, len, oid, allowDynamicOid)
    else:
      raise newException(
        PgTypeError,
        "pgComposite: unsupported field type " & name(T) &
          " (it has no form this library can read; cast the attribute in SQL)",
      )

template decodeBinaryField(val, buf: untyped, fOid: int32, fOff, fLen: int) =
  val = compositeFieldFromBinary[typeof(val)](buf, fOff, fLen, fOid, false)

proc getComposite*[T: object](row: Row, col: int): T =
  ## Read a PostgreSQL composite column as a Nim object. Handles binary format.
  ## A ``DateTime`` field reads a ``timestamp``, ``timestamptz`` or ``date``
  ## attribute, the last as midnight UTC.
  if row.isNull(col):
    raise newException(PgTypeError, "Column " & $col & " is NULL")
  if row.isBinaryCol(col):
    let (off, clen) = cellInfo(row, col)
    let decoded = withColumnContext(
      col, decodeBinaryComposite(row.data.buf.toOpenArray(off, off + clen - 1))
    )
    var idx = 0
    withColumnContext(col):
      for _, val in result.fieldPairs:
        if idx >= decoded.len:
          raise
            newException(PgTypeError, "Binary composite has fewer fields than object")
        let f = decoded[idx]
        let fOff = off + f.off
        when typeof(val) is Option:
          if f.len == -1:
            val = none(typeof(val.get))
          else:
            var inner: typeof(val.get)
            decodeBinaryField(inner, row.data.buf, f.oid, fOff, f.len)
            val = some(inner)
        else:
          if f.len == -1:
            raise newException(
              PgTypeError, "NULL field in binary composite at index " & $idx
            )
          decodeBinaryField(val, row.data.buf, f.oid, fOff, f.len)
        idx += 1
      if idx != decoded.len:
        raise newException(
          PgTypeError,
          "Binary composite has " & $decoded.len & " fields but object has " & $idx,
        )
    return
  let s = row.getStr(col)
  let parts = withColumnContext(col, parseCompositeText(s))
  var idx = 0
  withColumnContext(col):
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
    if idx != parts.len:
      raise newException(
        PgTypeError, "Composite has " & $parts.len & " fields but object has " & $idx
      )

proc getCompositeOpt*[T: object](row: Row, col: int): Option[T] =
  ## NULL-safe version of ``getComposite``.
  if row.isNull(col):
    none(T)
  else:
    some(getComposite[T](row, col))

macro pgComposite*(T: typedesc, oid: int32 = 0'i32): untyped =
  ## Generate ``toPgParam`` and ``get`` for a Nim object as a PostgreSQL
  ## composite type. Each field is sent as text inside the composite text
  ## format; ``get`` reads with ``getComposite``, so ``row.get(col, T)``,
  ## ``queryValue`` and ``to`` read ``T``.
  ## A field is encoded by its Nim type rather than by the calling module's
  ## ``$``: numbers, ``bool``, ``string``, ``seq[byte]`` and
  ## ``array[N, byte]`` (bytea), ``DateTime``, ``PgNumeric``, ``PgUuid``,
  ## ``PgMoney``, ``PgInterval``, ``PgTime``/``PgTimeTz``, ``PgBit``,
  ## inet/cidr, MAC addresses, XML, text-search and geometric types,
  ## ``JsonNode``, ``PgHstore``, enums (``pgEnum``), domains (``pgDomain``),
  ## ranges and multiranges, ``seq[T]``/``seq[Option[T]]`` and
  ## ``array[N, T]`` (one dimension), ``set[T]`` (a one-dimensional array
  ## of its members) and ``PgArray[T]`` (any dimension).
  ## A one-dimensional field drops a lower bound other than 1 on read, in
  ## text as in binary. A nested ``Option`` does not compile. An array of
  ## arrays does not compile; use ``PgArray[T]``.
  ## Any other object field is written through its own ``$``, verbatim, and
  ## cannot be read back: ``times.Time``'s drops the fraction of a second and
  ## ``Duration``'s is no interval literal, so use ``DateTime`` or
  ## ``PgInterval``. An object or tuple with only std's generic ``$``, such
  ## as a nested composite or ``(int32, int32)``, raises ``PgTypeError``.
  ## A ``DateTime`` field, and each bound of a ``DateTime`` range or
  ## multirange field, is sent as its UTC instant, which a timestamp,
  ## timestamptz or date attribute reads right.
  ## A ``PgMoney`` field is read with the default scale of 2
  ## (`parsePgMoney`), so another fractional-digit count raises; use a typed
  ## money accessor for that. Writing one uses the value's scale, which the
  ## server rounds to its ``lc_monetary`` fractional digits; the text has '.'
  ## as its decimal point, which an ``lc_monetary`` with ',' misreads.
  ## A domain field is decoded by the base type of the Nim ``distinct`` type,
  ## so the Nim base must match the domain's base. A binary read cannot verify
  ## it (the domain OID is dynamic), so a mismatch decodes silently.
  ## A binary read (``resultFormat = rfBinary``) supports the same types
  ## except ranges/multiranges and ``PgMoney``, which need text format (a
  ## binary money value does not carry its scale).
  ## Fields map by position, in both directions: the object's field order must
  ## match the composite's attribute order. Neither wire format carries names,
  ## so a swap of same-typed fields is not detected.
  ## When OID is 0 (default), PostgreSQL infers the type from context.
  ## The generated overloads live in the calling module, where the ``sql``
  ## macro and ``pgParams`` resolve them alongside the built-in ones; ``to``
  ## reads ``T`` only where that module is imported.
  ## ``queryDirect`` / ``execDirect`` take their parameters as
  ## separate arguments and cannot encode these types.
  let tImpl = T.getType[1]
  let tSym = tImpl
  result = newStmtList()
  result.add quote do:
    proc toPgParam*(v: `tSym`): PgParam =
      var fields: seq[Option[string]]
      for _, val in v.fieldPairs:
        when typeof(val) is Option:
          if val.isSome:
            fields.add(some(compositeFieldText(val.get)))
          else:
            fields.add(none(string))
        else:
          fields.add(some(compositeFieldText(val)))
      PgParam(
        oid: `oid`, format: 0'i16, value: some(toBytes(encodeCompositeText(fields)))
      )

    proc get*(row: Row, col: int, _: typedesc[`tSym`]): `tSym` =
      getComposite[`tSym`](row, col)

# User-defined domain type support

macro checkPgDomainTypes(T, Base: typedesc): untyped =
  ## The generated overload converts ``T`` to ``Base`` and encodes that. A
  ## distinct ``Base`` must lie below ``T`` in its ``distinct`` chain, so each
  ## step moves down a finite chain: ``Base`` equal to ``T``, or two types
  ## naming each other, would otherwise recurse forever. A non-distinct ``T``
  ## would make the overload call itself.
  let tSym = T.getType[1]
  if tSym.typeKind != ntyDistinct:
    error("pgDomain requires a distinct type, got " & repr(T), T)
  let bSym = Base.getType[1]
  if bSym.typeKind == ntyDistinct:
    var t = tSym
    while true:
      let impl = t.getTypeImpl
      # getTypeImpl, unlike typeKind, sees through an alias of a distinct type.
      if impl.kind != nnkDistinctTy or impl[0].getTypeImpl.kind != nnkDistinctTy:
        error(
          "pgDomain base " & repr(Base) & " is distinct, so " & repr(T) &
            " must be declared from it (`" & repr(T) & " = distinct " & repr(Base) & "`)",
          Base,
        )
      t = impl[0]
      if sameType(t, bSym):
        break
  newEmptyNode()

macro pgDomain*(T: typedesc, Base: typedesc, oid: int32 = 0'i32): untyped =
  ## Generate ``toPgParam`` for a Nim distinct type as a PostgreSQL domain type.
  ## Encoding delegates to the base type's ``toPgParam``. A distinct ``Base``
  ## (a domain over another domain) must be one ``T`` is declared from:
  ## ``type T = distinct Base``.
  ## When OID is 0 (default), the base type's OID is used.
  ## The generated overload lives in the calling module, where the ``sql``
  ## macro and ``pgParams`` resolve it alongside the built-in ones.
  ## ``queryDirect`` / ``execDirect`` take their parameters as
  ## separate arguments and cannot encode these types.
  let tSym = T.getType[1]
  let bSym = Base.getType[1]
  result = newStmtList(newCall(bindSym"checkPgDomainTypes", T, Base))
  result.add quote do:
    proc toPgParam*(v: `tSym`): PgParam =
      result = toPgParam(`bSym`(v))
      if `oid` != 0'i32:
        result.oid = `oid`
