## Generic PostgreSQL type OID lookup.
##
## Provides a single-round-trip helper that resolves arbitrary PostgreSQL
## type names — including extension types like ``hstore``, ``citext``,
## ``vector`` — to their base and array OIDs via ``to_regtype()``. The
## library does not auto-discover extension OIDs at connect time; callers
## use this API on demand and pass the OIDs to OID-aware encoders such as
## ``toPgBinaryParam(v: PgHstore, oid: int32)``.
##
## Re-exported through ``pg_connection.nim``.

import std/[options, strutils, tables]

import ../[async_backend, pg_errors, pg_types]
import types, simple_query

type TypeOidInfo* = tuple[oid: int32, arrayOid: int32]
  ## Result of ``lookupTypeOids``. ``oid`` is the base type OID and
  ## ``arrayOid`` is the corresponding array type OID
  ## (``pg_type.typarray``); ``arrayOid`` is ``0`` for types that have no
  ## array companion.

proc isSafeTypeName(name: string): bool =
  if name.len == 0:
    return false
  for c in name:
    case c
    of 'A' .. 'Z', 'a' .. 'z', '0' .. '9', '_', '.', '"':
      discard
    else:
      return false
  true

proc lookupTypeOids*(
    conn: PgConnection, names: seq[string]
): Future[Table[string, TypeOidInfo]] {.async.} =
  ## Resolve PostgreSQL type names to ``(oid, arrayOid)`` tuples in a single
  ## round trip via ``to_regtype()``. Resolution honours the connection's
  ## *current* ``search_path``; types not found on the server are omitted
  ## from the result, so callers detect absence with ``hasKey`` /
  ## ``getOrDefault``.
  ##
  ## Requires the connection to be in ``csReady`` state and raises
  ## ``PgConnectionError`` otherwise. Rejects names containing characters
  ## outside ``[A-Za-z0-9_."]`` with ``PgTypeError``. Empty ``names``
  ## returns an empty table without contacting the server.
  ##
  ## Results are a snapshot — call again after ``SET search_path`` or
  ## ``CREATE``/``DROP EXTENSION``.
  ##
  ## Example:
  ## ```nim
  ## let oids = await conn.lookupTypeOids(@["hstore", "citext"])
  ## if oids.hasKey("hstore"):
  ##   let p = toPgBinaryParam(myHstore, oids["hstore"].oid)
  ## ```
  result = initTable[string, TypeOidInfo]()
  if names.len == 0:
    return
  for n in names:
    if not isSafeTypeName(n):
      raise newException(PgTypeError, "lookupTypeOids: unsafe type name: " & n)
  conn.checkReady()

  var sql =
    "SELECT n, t.oid::int4, COALESCE(t.typarray, 0)::int4 " & "FROM unnest(ARRAY["
  for i, n in names:
    if i > 0:
      sql.add(',')
    sql.add("$lt$")
    sql.add(n)
    sql.add("$lt$")
  sql.add("]::text[]) AS n LEFT JOIN pg_type t ON t.oid = to_regtype(n)")

  let qrs = await conn.simpleQuery(sql)
  if qrs.len == 0:
    return
  let qr = qrs[0]
  for row in qr.rows:
    let nameOpt = row[0]
    if nameOpt.isNone:
      continue
    let name = bytesToString(nameOpt.get)
    let oidOpt = row[1]
    if oidOpt.isNone:
      continue
    var oid: int32
    try:
      oid = int32(parseInt(bytesToString(oidOpt.get)))
    except ValueError:
      continue
    var arrOid: int32 = 0
    let arrOpt = row[2]
    if arrOpt.isSome:
      try:
        arrOid = int32(parseInt(bytesToString(arrOpt.get)))
      except ValueError:
        arrOid = 0
    result[name] = (oid: oid, arrayOid: arrOid)
