import std/[macros, options, strutils, times, typetraits]

import ../pg_protocol
import core, array, accessors, ranges, user_types

# Row-to-object mapping
#
# `to(row, T)` fills an object, a `ref object` or a named tuple field by
# field, each from the column its name matches; an anonymous tuple takes the
# columns by position. A field is read with `row.get(col, F)` when such an
# overload exists, the caller's own included, else with the accessor its type
# implies (see `decodeValue`).

template pgColumn*(name: string) {.pragma.}
  ## Field pragma for ``to``: read the column ``name``, matched byte for byte,
  ## instead of the one the field name matches.

template pgSkip*() {.pragma.}
  ## Field pragma for ``to``: read no column and keep the field's default.

proc recordHasCase(n: NimNode): bool =
  if n.kind == nnkRecCase:
    return true
  for c in n:
    if recordHasCase(c):
      return true
  false

proc derefObjectTy(t: NimNode): NimNode =
  result = t.getTypeImpl
  if result.kind == nnkRefTy:
    result = result[0].getTypeImpl

macro isCaseObject*(T: typedesc): bool =
  ## Whether ``T`` or one of its bases has a ``case`` section.
  var t = derefObjectTy(T.getTypeImpl[1])
  while t.kind == nnkObjectTy:
    if recordHasCase(t[2]):
      return newLit(true)
    if t[1].kind != nnkOfInherit:
      break
    t = derefObjectTy(t[1][0])
  newLit(false)

proc decodeValue[F](row: Row, col: int, _: typedesc[F]): F =
  mixin get
  when F is Option:
    if row.isNull(col):
      none(typeof(default(F).get))
    else:
      some(decodeValue(row, col, typeof(default(F).get)))
  elif compiles(row.get(col, F)):
    row.get(col, F)
  elif F is int:
    let v = row.getInt64(col)
    when sizeof(int) < sizeof(int64):
      if v < int64(int.low) or v > int64(int.high):
        raise newException(
          PgTypeError, "Column " & $col & ": integer value out of int range"
        )
    int(v)
  # The accessors for timestamp, timestamptz and date differ in the column
  # type they accept, so the column's own type picks one.
  elif F is DateTime:
    case row.colTypeOid(col)
    of OidDate:
      row.getDate(col)
    of OidTimestampTz:
      row.getTimestampTz(col)
    else:
      row.getTimestamp(col)
  elif F is seq[DateTime]:
    case row.colTypeOid(col)
    of OidDateArray:
      row.getDateArray(col)
    of OidTimestampTzArray:
      row.getTimestampTzArray(col)
    else:
      row.getTimestampArray(col)
  elif F is PgRange[DateTime]:
    case row.colTypeOid(col)
    of OidDateRange:
      row.getDateRange(col)
    of OidTsTzRange:
      row.getTsTzRange(col)
    else:
      row.getTsRange(col)
  elif F is PgMultirange[DateTime]:
    case row.colTypeOid(col)
    of OidDateMultirange:
      row.getDateMultirange(col)
    of OidTsTzMultirange:
      row.getTsTzMultirange(col)
    else:
      row.getTsMultirange(col)
  elif F is enum:
    getEnum[F](row, col)
  elif F is seq[enum]:
    getEnumArray[typeof(default(F)[0])](row, col)
  elif F is seq[Option[enum]]:
    getEnumArrayElemOpt[typeof(default(F)[0].get)](row, col)
  elif F is PgArray:
    getArrayND[typeof(default(F).elements[0].get)](row, col)
  # A domain reads as its base, so one over `DateTime` follows the column type
  # as well. A multirange with no `get` here is not one.
  elif F is distinct and F isnot PgMultirange:
    F(decodeValue(row, col, distinctBase(F)))
  # Objects need a `get`, which pgComposite declares: a catch-all composite
  # read would also take `Time`, `Table` or a type whose `get` the caller has
  # not imported, and fail only at run time.
  else:
    {.
      error:
        "to: no reader for " & name(F) &
        "; declare a `get` overload for it (pgComposite does for a composite) " &
        "in a module imported where `to` is called"
    .}

proc fieldContext*(owner, field: string): string =
  ## Error prefix naming the field, or only its owner for a positional one.
  if field.len > 0:
    owner & "." & field & ": "
  else:
    owner & ": "

proc decodeField[F](row: Row, col: int, _: typedesc[F], owner, field: string): F =
  ## ``decodeValue`` with the field prefixed to a ``PgTypeError``.
  try:
    result = decodeValue(row, col, F)
  except PgTypeError as err:
    err.msg = fieldContext(owner, field) & err.msg
    raise err

proc findColumn*(
    data: RowData,
    owner, field, key: string,
    exact: bool,
    optional = false,
    fromQuery = true,
): int =
  ## Index of the column ``key`` names among ``data``'s fields. Unless
  ## ``exact``, case and ``_`` are ignored as in Nim identifiers, and among
  ## several such matches only a byte-exact one is taken. When ``optional``,
  ## no match is ``-1`` instead of an error. ``fromQuery`` is whether the
  ## columns come from the caller's query, which could alias them.
  if data == nil or data.fields.len == 0:
    # `SELECT FROM t` describes no columns, so it has no metadata to miss.
    let why =
      if data != nil and data.numCols == 0:
        "the row has no columns"
      else:
        "column name lookup requires field metadata"
    raise newException(PgTypeError, fieldContext(owner, field) & why)
  var exactAt, foldedAt = -1
  var exactCount, foldedCount = 0
  for i in 0 ..< data.fields.len:
    if data.fields[i].name == key:
      exactAt = i
      inc exactCount
    elif not exact and cmpIgnoreStyle(data.fields[i].name, key) == 0:
      foldedAt = i
      inc foldedCount
  if exactCount == 1:
    return exactAt
  if exactCount == 0 and foldedCount == 1:
    return foldedAt
  if exactCount == 0 and foldedCount == 0:
    if optional:
      return -1
    raise newException(
      PgTypeError,
      fieldContext(owner, field) & "no column " & (if exact: "named" else: "matches") &
        " \"" & key & "\"" & (if exact: "" else: " (case and \"_\" ignored)"),
    )
  var names = ""
  for f in data.fields:
    if f.name == key or (exactCount == 0 and cmpIgnoreStyle(f.name, key) == 0):
      if names.len > 0:
        names.add(", ")
      names.add('"' & f.name & '"')
  var fixes: seq[string]
  if fromQuery:
    fixes.add("alias them in the query")
  if exactCount == 0:
    fixes.add("name one with pgColumn")
  raise newException(
    PgTypeError,
    fieldContext(owner, field) & "more than one column matches (" & names & ")" &
      (if fixes.len > 0: "; " & fixes.join(" or ") else: ""),
  )

proc resolveColumns[O](obj: var O, data: RowData, owner: string, cols: var seq[int]) =
  ## Resolve ``obj``'s fields to column indices, ``-1`` for a ``pgSkip`` field.
  for field, val in obj.fieldPairs:
    when val.hasCustomPragma(pgSkip):
      cols.add(-1)
    else:
      when val.hasCustomPragma(pgColumn):
        cols.add(
          findColumn(data, owner, field, val.getCustomPragmaVal(pgColumn), exact = true)
        )
      else:
        cols.add(findColumn(data, owner, field, field, exact = false))

proc fillByName[O](obj: var O, row: Row, owner: string, cols: var seq[int]) =
  ## Fill ``obj``'s fields from ``row``, resolving their columns into ``cols``
  ## when it is empty and reusing them after.
  if cols.len == 0:
    resolveColumns(obj, row.data, owner, cols)
  var i = 0
  for field, val in obj.fieldPairs:
    when val.hasCustomPragma(pgSkip):
      discard
    else:
      val = decodeField(row, cols[i], typeof(val), owner, field)
    inc i

proc checkTupleLen(owner: string, numCols, fieldCount: int) =
  ## Raise unless an anonymous tuple has one field per column.
  if numCols != fieldCount:
    raise newException(
      PgTypeError,
      owner & ": row has " & $numCols & " columns but tuple has " & $fieldCount &
        " fields",
    )

proc resolveRowColumns*[T](data: RowData, _: typedesc[T], cols: var seq[int]) =
  ## ``mapRow``'s field-to-column resolution without a row, so a result with no
  ## rows still reports a field that has no column or matches several, or an
  ## anonymous tuple of the wrong length. ``cols`` stays empty for an anonymous
  ## tuple, which maps by position. Sibling-only.
  const owner = name(T)
  when isCaseObject(T):
    {.error: "to: " & owner & " has a case section; map it field by field".}
  elif T is tuple and not isNamedTuple(T):
    checkTupleLen(owner, (if data == nil: 0 else: int(data.numCols)), tupleLen(T))
  elif T is ref:
    var probe = new(T)
    resolveColumns(probe[], data, owner, cols)
  else:
    var probe: T
    resolveColumns(probe, data, owner, cols)

proc mapRow*[T: object | tuple | ref object](
    row: Row, _: typedesc[T], cols: var seq[int]
): T =
  ## ``to(row, T)`` with the field-to-column indices cached in ``cols``:
  ## resolved on the first row, or before a loop by ``resolveRowColumns``.
  ## Sibling-only.
  const owner = name(T)
  when isCaseObject(T):
    {.error: "to: " & owner & " has a case section; map it field by field".}
  elif T is tuple and not isNamedTuple(T):
    checkTupleLen(owner, row.len, tupleLen(T))
    var i = 0
    for val in result.fields:
      val = decodeField(row, i, typeof(val), owner, "")
      inc i
  elif T is ref:
    result = new(T)
    fillByName(result[], row, owner, cols)
  else:
    result = default(T)
    fillByName(result, row, owner, cols)

proc to*[T: object | tuple | ref object](row: Row, _: typedesc[T]): T =
  ## Read ``row`` into a ``T``: an object, a ``ref object`` or a named tuple
  ## field by field, each from the column its name matches, or an anonymous
  ## tuple from all the columns in order.
  ##
  ## A field name matches a column ignoring case and ``_``, as Nim identifiers
  ## do, so ``createdAt`` reads ``created_at``; among several matches only a
  ## byte-exact one is taken. ``{.pgColumn: "name".}`` names the column
  ## instead (matched byte for byte) and ``{.pgSkip.}`` leaves the field at its
  ## default. Columns no field matches are ignored.
  ##
  ## A field of type ``F`` is read with ``row.get(col, F)``, so a ``get``
  ## overload declared for your own type is used, as is the one
  ## ``pgComposite`` declares; either must be visible where ``to`` is called.
  ## Without one: ``Option[F]`` is ``none`` for NULL; ``int`` reads any
  ## integer column; ``DateTime``, ``seq[DateTime]`` and their ranges and
  ## multiranges follow the column's type (timestamp, timestamptz or date); an
  ## enum, ``seq`` of one or ``seq[Option]`` of one goes through ``getEnum`` /
  ## ``getEnumArray`` / ``getEnumArrayElemOpt``; ``PgArray[E]`` through
  ## ``getArrayND``; a distinct type (a domain) as its base type. Any other
  ## type, an object included, is a compile error.
  ##
  ## ``PgArray[E]`` needs the column in binary format: ``rfAuto`` sends text on
  ## a statement's first execution, and ``simpleQuery`` / ``rfText`` never send
  ## binary, so query with ``rfBinary``.
  ##
  ## Raises ``PgTypeError``, prefixed with ``T.field``, when a field has no
  ## column, matches more than one, or cannot be read (a NULL in a field that
  ## is not an ``Option``, for one). Name matching needs the row's field
  ## metadata, which query results carry.
  ##
  ## .. code-block:: nim
  ##   type User = object
  ##     id: int64
  ##     name: string
  ##     createdAt: DateTime
  ##     email: Option[string]
  ##
  ##   let row = await conn.queryRow(
  ##     sql"SELECT id, name, created_at, email FROM users WHERE id = {id}")
  ##   let user = row.to(User)
  var cols: seq[int]
  mapRow(row, T, cols)
