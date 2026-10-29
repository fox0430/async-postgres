## The field-to-column plan and the binary COPY encoders behind ``copyInRows``.
##
## Binary COPY carries no type, so a wrong payload is read as another value
## (an int8 as a float8): only the type pairs `copyConv` allows are written.
##
## Internal module: not part of the public API.

import std/[json, macros, math, options, times, typetraits]

import ../[pg_bytes, pg_protocol]
import core, array, encoding, ranges, row_mapping

type
  CopyConv* = enum
    ## How a field's value reaches its column's binary form, fixed per column
    ## before the COPY starts.
    ccPayload ## the value's own encoder, its OID checked on every value
    ccInt2 ## an integer, or each element of an integer array, as int2
    ccInt4
    ccInt8
    ccFloat4 ## a float rounded to float4, as PostgreSQL's float8 cast does
    ccFloat8
    ccNumeric ## an integer or a decimal string as numeric
    ccText ## a string's bytes, which the text types, json and xml read as is
    ccJsonb ## JSON text behind jsonb's version byte
    ccUuid ## a string parsed as a uuid
    ccTimestamp ## a DateTime, or each element of an array of them
    ccTimestampTz
    ccDate
    ccLabel ## an enum's label

  CopyRowsPlan* = object ## The columns a row type writes, in COPY order.
    columns*: seq[string] ## Names as the server described them.
    oids*: seq[int32]
    convs*: seq[CopyConv]
    writes*: seq[bool] ## Per field in declaration order: whether it is written.
    enumChecks*: seq[tuple[oid: int32, err: string]]
      ## User types that labels go to, with the error raised unless each is an
      ## enum: another type's receive function may misread a label.

  CopyKind = enum
    ## Which branch of `copyConv` / `encodeCopyValue` a field type takes.
    ckOption
    ckInt
    ckFloat
    ckBool
    ckString
    ckBytes
    ckDateTime
    ckJson
    ckEnum
    ckIntArray
    ckDateTimeArray
    ckBinary ## has a `toPgBinaryParam`
    ckDistinct ## a distinct type without one: written as its base (a domain)
    ckTextParam ## has only a `toPgParam`
    ckNone

  CopyInt = int | int16 | int32 | int64

const textOids = [OidText, OidVarchar, OidBpchar, OidName]

proc isIntSeq[F](_: typedesc[F]): bool {.compileTime.} =
  ## A seq of a ``CopyInt`` or of its ``Option``. ``seq[CopyInt]`` misses a
  ## range element such as ``Natural``, though ``Natural`` is a ``CopyInt``.
  when F is seq:
    type E = typeof(default(F)[0])
    when E is Option:
      typeof(default(E).get) is CopyInt
    else:
      E is CopyInt
  else:
    false

proc copyKind[F](_: typedesc[F]): CopyKind {.compileTime.} =
  mixin toPgBinaryParam, toPgParam
  when F is Option:
    ckOption
  elif F is CopyInt:
    ckInt
  elif F is float32 | float64:
    ckFloat
  elif F is bool:
    ckBool
  elif F is string:
    ckString
  elif F is seq[byte]:
    ckBytes
  elif F is DateTime:
    ckDateTime
  elif F is JsonNode:
    ckJson
  elif F is enum and not compiles(toPgBinaryParam(default(F))):
    ckEnum
  elif isIntSeq(F):
    ckIntArray
  elif F is seq[DateTime] or F is seq[Option[DateTime]]:
    ckDateTimeArray
  elif compiles(toPgBinaryParam(default(F))):
    ckBinary
  # A multirange is distinct too, but not a domain.
  elif F is distinct and F isnot PgMultirange:
    ckDistinct
  elif compiles(toPgParam(default(F))):
    ckTextParam
  else:
    ckNone

func isUserOid(oid: int32): bool =
  ## Past PostgreSQL's FirstNormalObjectId. OIDs are unsigned.
  cast[uint32](oid) >= 16384'u32

func oidText(oid: int32): string =
  ## ``oid`` as PostgreSQL prints it.
  $cast[uint32](oid)

func isTextLike(oid: int32): bool =
  ## Types whose binary form is the text's bytes.
  oid in textOids or oid == OidJson or oid == OidXml

func samePayload(a, b: int32): bool =
  ## Whether types ``a`` and ``b`` share one binary form.
  a == b or (a in textOids and b in textOids) or
    (a in [OidBit, OidVarbit] and b in [OidBit, OidVarbit])

func sharedArrayElem(oid: int32): int32 =
  ## The element type of an array type whose payload another array type of
  ## the same family reads once the element type is rewritten, else 0.
  case oid
  of OidTextArray: OidText
  of OidVarcharArray: OidVarchar
  of OidBpcharArray: OidBpchar
  of OidNameArray: OidName
  of OidBitArray: OidBit
  of OidVarbitArray: OidVarbit
  else: 0

func payloadFits(p: PgParam, oid: int32): bool =
  ## Whether ``p``'s payload is a column of type ``oid``'s binary form, as it
  ## is or with an array's element type rewritten.
  if p.oid in textOids:
    # A text type's text and binary forms are the same bytes.
    isTextLike(oid)
  elif p.format != 1:
    false
  else:
    let a = sharedArrayElem(p.oid)
    let b = sharedArrayElem(oid)
    samePayload(p.oid, oid) or (a != 0 and b != 0 and samePayload(a, b))

func noConvMsg(what: string, oid: int32): string =
  what & " cannot be written to a column of type OID " & oidText(oid)

proc noConv(what: string, oid: int32): ref PgTypeError =
  newException(PgTypeError, noConvMsg(what, oid))

proc checkProto[F](proto: PgParam, oid: int32) =
  ## Fail early when ``F``'s encoder gives ``oid`` no payload it can read.
  if not payloadFits(proto, oid):
    if proto.format != 1 and proto.oid notin textOids:
      let column = "a column of type OID " & oidText(oid)
      raise newException(
        PgTypeError,
        name(F) & " has only a text encoding, which " & column &
          " cannot read in binary COPY",
      )
    raise noConv(name(F), oid)

proc copyConv*[F](_: typedesc[F], oid: int32): CopyConv =
  ## The conversion writing an ``F`` to a column of type ``oid``. Raises
  ## ``PgTypeError`` for a pair none covers.
  mixin toPgBinaryParam, toPgParam
  const kind = copyKind(F)
  when kind == ckOption:
    copyConv(typeof(default(F).get), oid)
  elif kind == ckInt:
    case oid
    of OidInt2:
      ccInt2
    of OidInt4:
      ccInt4
    of OidInt8:
      ccInt8
    of OidNumeric:
      ccNumeric
    else:
      raise noConv(name(F), oid)
  elif kind == ckFloat:
    case oid
    of OidFloat4:
      ccFloat4
    of OidFloat8:
      ccFloat8
    else:
      raise noConv(name(F), oid)
  elif kind == ckBool:
    if oid != OidBool:
      raise noConv(name(F), oid)
    ccPayload
  elif kind == ckString:
    if isTextLike(oid):
      ccText
    else:
      case oid
      of OidJsonb:
        ccJsonb
      of OidUuid:
        ccUuid
      of OidNumeric:
        ccNumeric
      else:
        raise noConv(name(F), oid)
  elif kind == ckBytes:
    if oid != OidBytea:
      raise noConv(name(F), oid)
    ccPayload
  elif kind == ckDateTime:
    case oid
    of OidTimestamp:
      ccTimestamp
    of OidTimestampTz:
      ccTimestampTz
    of OidDate:
      ccDate
    else:
      raise noConv(name(F), oid)
  elif kind == ckJson:
    case oid
    of OidJson:
      ccText
    of OidJsonb:
      ccJsonb
    else:
      raise noConv(name(F), oid)
  elif kind == ckEnum:
    # A user type is checked to be an enum before the COPY: see `enumChecks`.
    if oid in textOids or isUserOid(oid):
      ccLabel
    else:
      raise noConv(name(F), oid)
  elif kind == ckIntArray:
    case oid
    of OidInt2Array:
      ccInt2
    of OidInt4Array:
      ccInt4
    of OidInt8Array:
      ccInt8
    of OidNumericArray:
      ccNumeric
    else:
      raise noConv(name(F), oid)
  elif kind == ckDateTimeArray:
    case oid
    of OidTimestampArray:
      ccTimestamp
    of OidTimestampTzArray:
      ccTimestampTz
    of OidDateArray:
      ccDate
    else:
      raise noConv(name(F), oid)
  elif kind == ckBinary or kind == ckTextParam:
    # The default value's encoding names the OID; failing that (an empty
    # PgUuid) the array element registry, else only each value is checked.
    when F isnot ref and F isnot ptr:
      var proto: PgParam
      var known = false
      try:
        proto =
          when kind == ckBinary:
            toPgBinaryParam(default(F))
          else:
            toPgParam(default(F))
        known = true
      except CatchableError:
        discard
      if known:
        checkProto[F](proto, oid)
      else:
        when compiles(pgArrayElemOid(F)):
          checkProto[F](PgParam(oid: pgArrayElemOid(F), format: 1), oid)
    ccPayload
  elif kind == ckDistinct:
    copyConv(distinctBase(F), oid)
  else:
    {.
      error:
        "copyInRows: no encoder for " & name(F) &
        "; declare a `toPgBinaryParam` for it in a module imported where " &
        "`copyInRows` is called"
    .}

proc beginField(buf: var seq[byte]): int =
  ## Reserve a field's length, to be filled by `endField`.
  result = buf.len
  buf.addInt32(0)

proc endField(buf: var seq[byte], at: int) =
  let n = buf.len - at - 4
  checkPgBinLen(n, "COPY field")
  buf.writeBE32(at, int32(n))

proc addInt(buf: var seq[byte], v: int64, conv: CopyConv) =
  case conv
  of ccInt2:
    if v < int64(int16.low) or v > int64(int16.high):
      raise newException(PgTypeError, "value out of range for smallint")
    buf.addCopyFieldInt16(int16(v))
  of ccInt4:
    if v < int64(int32.low) or v > int64(int32.high):
      raise newException(PgTypeError, "value out of range for integer")
    buf.addCopyFieldInt32(int32(v))
  of ccInt8:
    buf.addCopyFieldInt64(v)
  of ccNumeric:
    buf.addCopyFieldText(encodeNumericBinary(parsePgNumeric($v)))
  else:
    raiseAssert "integer conversion " & $conv

proc addFloat(buf: var seq[byte], v: float64, conv: CopyConv) =
  case conv
  of ccFloat4:
    let f = float32(v)
    # The checks of PostgreSQL's float8-to-float4 cast.
    const infs = {fcInf, fcNegInf}
    if f.classify in infs and v.classify notin infs:
      raise newException(PgTypeError, "value out of range for real: overflow")
    if f == 0 and v != 0:
      raise newException(PgTypeError, "value out of range for real: underflow")
    buf.addCopyFieldFloat32(f)
  of ccFloat8:
    buf.addCopyFieldFloat64(v)
  else:
    raiseAssert "float conversion " & $conv

proc addDateTime(buf: var seq[byte], v: DateTime, conv: CopyConv) =
  case conv
  of ccTimestamp, ccTimestampTz:
    # Absolute, so a timestamptz does not depend on the session TimeZone.
    buf.addCopyFieldInt64(pgTimestampMicros(v))
  of ccDate:
    buf.addCopyFieldInt32(pgDateDays(v))
  else:
    raiseAssert "DateTime conversion " & $conv

proc addArrayHeader(buf: var seq[byte], elemOid: int32, n: int, hasNull: bool) =
  ## A one-dimensional array's header; an empty array has no dimension.
  checkArrayLen(n)
  if n == 0:
    buf.addInt32(0)
    buf.addInt32(0)
    buf.addInt32(elemOid)
  else:
    buf.addInt32(1)
    buf.addInt32(int32(hasNull))
    buf.addInt32(elemOid)
    buf.addInt32(int32(n))
    buf.addInt32(1)

func arrayElemOid(conv: CopyConv): int32 =
  case conv
  of ccInt2: OidInt2
  of ccInt4: OidInt4
  of ccInt8: OidInt8
  of ccTimestamp: OidTimestamp
  of ccTimestampTz: OidTimestampTz
  of ccDate: OidDate
  of ccNumeric: OidNumeric
  else: 0

proc addArray[E](buf: var seq[byte], v: seq[E], conv: CopyConv) =
  ## An integer or DateTime array, each element converted by ``conv``.
  var hasNull = false
  when E is Option:
    for e in v:
      if e.isNone:
        hasNull = true
        break
  let at = buf.beginField()
  buf.addArrayHeader(arrayElemOid(conv), v.len, hasNull)
  for e in v:
    when E is Option:
      if e.isNone:
        buf.addCopyFieldNull()
        continue
      let x = e.get
    else:
      let x = e
    when x is DateTime:
      buf.addDateTime(x, conv)
    else:
      buf.addInt(int64(x), conv)
  buf.endField(at)

proc addParam(buf: var seq[byte], p: PgParam, oid: int32, what: string) =
  ## ``p``'s payload, which must fit ``oid`` (see `payloadFits`).
  if p.value.isNone:
    buf.addCopyFieldNull()
    return
  if not payloadFits(p, oid):
    raise noConv(what & " (encoded as OID " & oidText(p.oid) & ")", oid)
  let elem = sharedArrayElem(oid)
  let rewrite = elem != 0 and p.oid != oid
  if rewrite and p.value.get.len < 12:
    raise newException(
      PgTypeError,
      what & " (encoded as OID " & oidText(p.oid) & "): " & $p.value.get.len &
        " bytes, too short for an array header",
    )
  let at = buf.len
  buf.addCopyFieldText(p.value.get)
  if rewrite:
    # Past the length, ndim and flags: the element type `array_recv` checks.
    buf.writeBE32(at + 4 + 8, elem)

proc encodeCopyValue*[F](buf: var seq[byte], v: F, conv: CopyConv, oid: int32) =
  ## Append ``v`` as one binary COPY field (length and payload) of a column of
  ## type ``oid``, by the ``conv`` that `copyConv` gave for the pair.
  mixin toPgBinaryParam, toPgParam
  const kind = copyKind(F)
  when kind == ckOption:
    if v.isNone:
      buf.addCopyFieldNull()
    else:
      encodeCopyValue(buf, v.get, conv, oid)
  elif kind == ckInt:
    buf.addInt(int64(v), conv)
  elif kind == ckFloat:
    buf.addFloat(float64(v), conv)
  elif kind == ckBool:
    buf.addCopyFieldBool(v)
  elif kind == ckString:
    case conv
    of ccText:
      buf.addCopyFieldString(v)
    of ccJsonb:
      buf.addLen32(v.len + 1, "jsonb")
      buf.add(1'u8)
      buf.appendBytes(v.toOpenArrayByte(0, v.high))
    of ccUuid:
      buf.addCopyFieldText(toPgBinaryParam(PgUuid(v)).value.get)
    of ccNumeric:
      buf.addCopyFieldText(encodeNumericBinary(parsePgNumeric(v)))
    else:
      raiseAssert "string conversion " & $conv
  elif kind == ckBytes:
    buf.addCopyFieldText(v)
  elif kind == ckDateTime:
    buf.addDateTime(v, conv)
  elif kind == ckJson:
    if v.isNil:
      raise newException(PgTypeError, "nil JsonNode; use Option[JsonNode] for NULL")
    if conv == ccJsonb:
      buf.addCopyFieldText(encodeJsonbBinary(v))
    else:
      buf.addCopyFieldString($v)
  elif kind == ckEnum:
    buf.addCopyFieldString($v)
  elif kind == ckIntArray or kind == ckDateTimeArray:
    buf.addArray(v, conv)
  elif kind == ckBinary:
    buf.addParam(toPgBinaryParam(v), oid, name(F))
  elif kind == ckDistinct:
    encodeCopyValue(buf, distinctBase(F)(v), conv, oid)
  elif kind == ckTextParam:
    buf.addParam(toPgParam(v), oid, name(F))
  else:
    {.error: "copyInRows: no encoder for " & name(F).}

proc columnContext(owner, field, column: string): string =
  fieldContext(owner, field) & "column \"" & column & "\": "

proc labelName[F](_: typedesc[F]): string =
  ## ``F`` without its ``Option``, as `copyConv` names it.
  when F is Option:
    labelName(typeof(default(F).get))
  else:
    name(F)

proc planField[F](
    plan: var CopyRowsPlan, _: typedesc[F], f: FieldDescription, owner, field: string
) =
  let context = columnContext(owner, field, f.name)
  var conv: CopyConv
  try:
    conv = copyConv(F, f.typeOid)
  except PgTypeError as e:
    e.msg = context & e.msg
    raise e
  if conv == ccLabel and isUserOid(f.typeOid):
    plan.enumChecks.add(
      (
        f.typeOid,
        context & noConvMsg(labelName(F), f.typeOid) & ", which is not an enum",
      )
    )
  plan.convs.add(conv)
  plan.columns.add(f.name)
  plan.oids.add(f.typeOid)

proc planNamed[O](
    plan: var CopyRowsPlan,
    probe: var O,
    fields: seq[FieldDescription],
    owner: string,
    columnsGiven: bool,
) =
  ## Each field's column by `to`'s rules. With ``columnsGiven`` a field
  ## matching no column is skipped; without, it is an error.
  let data = RowData(numCols: int16(fields.len), fields: fields)
  var takenBy = newSeq[string](fields.len)
  for field, val in probe.fieldPairs:
    when val.hasCustomPragma(pgSkip):
      plan.writes.add(false)
    else:
      let col =
        when val.hasCustomPragma(pgColumn):
          findColumn(
            data,
            owner,
            field,
            val.getCustomPragmaVal(pgColumn),
            exact = true,
            optional = columnsGiven,
            fromQuery = false,
          )
        else:
          findColumn(
            data,
            owner,
            field,
            field,
            exact = false,
            optional = columnsGiven,
            fromQuery = false,
          )
      if col < 0:
        plan.writes.add(false)
      else:
        if takenBy[col].len > 0:
          raise newException(
            PgTypeError,
            fieldContext(owner, field) & "column \"" & fields[col].name &
              "\" is also written by " & owner & "." & takenBy[col],
          )
        takenBy[col] = field
        plan.planField(typeof(val), fields[col], owner, field)
        plan.writes.add(true)
  if columnsGiven:
    for i, f in fields:
      if takenBy[i].len == 0:
        raise newException(
          PgTypeError, owner & ": no field matches column \"" & f.name & "\""
        )
  if plan.columns.len == 0:
    raise newException(PgTypeError, owner & ": no field to write")

proc planCopyRows*[T](fields: seq[FieldDescription], columnsGiven: bool): CopyRowsPlan =
  ## The plan writing ``T`` rows to the columns ``fields`` describes, by
  ## `to`'s name rules or, for an anonymous tuple, by position.
  const owner = name(T)
  when isCaseObject(T):
    {.error: "copyInRows: " & owner & " has a case section".}
  elif T is tuple and not isNamedTuple(T):
    if fields.len != tupleLen(T):
      raise newException(
        PgTypeError,
        owner & ": " & $fields.len & " columns but the tuple has " & $tupleLen(T) &
          " fields",
      )
    var probe: T
    var i = 0
    for val in probe.fields:
      result.planField(typeof(val), fields[i], owner, "")
      result.writes.add(true)
      inc i
  elif T is ref:
    var probe = new(T)
    result.planNamed(probe[], fields, owner, columnsGiven)
  else:
    var probe: T
    result.planNamed(probe, fields, owner, columnsGiven)

proc checkEnumColumns*(plan: CopyRowsPlan, enums: openArray[int32]) =
  ## Raise ``PgTypeError`` unless each type of ``plan.enumChecks`` is among
  ## ``enums``.
  for c in plan.enumChecks:
    if c.oid notin enums:
      raise newException(PgTypeError, c.err)

proc encodeField[F](
    buf: var seq[byte], v: F, plan: CopyRowsPlan, col: int, owner, field: string
) =
  try:
    encodeCopyValue(buf, v, plan.convs[col], plan.oids[col])
  except CatchableError as e:
    e.msg = columnContext(owner, field, plan.columns[col]) & e.msg
    raise e

proc encodeNamed[O](buf: var seq[byte], obj: O, plan: CopyRowsPlan, owner: string) =
  var i, col = 0
  for field, val in obj.fieldPairs:
    when not val.hasCustomPragma(pgSkip):
      if plan.writes[i]:
        buf.encodeField(val, plan, col, owner, field)
        inc col
    inc i

proc encodeCopyRow*[T](buf: var seq[byte], row: T, plan: CopyRowsPlan) =
  ## Append ``row`` as one binary COPY tuple.
  const owner = name(T)
  buf.addCopyTupleStart(int16(plan.columns.len))
  when T is tuple and not isNamedTuple(T):
    var i = 0
    for val in row.fields:
      buf.encodeField(val, plan, i, owner, "")
      inc i
  elif T is ref:
    if row.isNil:
      raise newException(PgTypeError, owner & ": nil row")
    buf.encodeNamed(row[], plan, owner)
  else:
    buf.encodeNamed(row, plan, owner)

proc encodeCopyRows*[T](
    buf: var seq[byte],
    rows: openArray[T],
    next: var int,
    plan: CopyRowsPlan,
    limit: int,
    base = 0,
) =
  ## Append ``rows`` from ``next`` on until ``buf`` holds ``limit`` bytes,
  ## advancing ``next``. Errors name the row as ``base + next``.
  while next < rows.len and buf.len < limit:
    try:
      buf.encodeCopyRow(rows[next], plan)
    except CatchableError as e:
      e.msg = "row " & $(base + next) & ": " & e.msg
      raise e
    inc next
