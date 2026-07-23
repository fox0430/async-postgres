import std/[json, macros, options, times]

import pg_protocol
import pg_types/[core, array, encoding, decoding, accessors, user_types, ranges]

export core, array, encoding, decoding, accessors, user_types, ranges

# Name-based accessors bound here so both accessors.nim (non-range getters)
# and ranges.nim (Range/Multirange getters) are visible when `nameAccessor`
# expands its delegation bodies.

# Family helpers: emit paired `nameAccessor` calls from a single (base, T) row.
# Names are derived by concatenating suffixes onto the base identifier, so the
# table below stays a flat data list rather than 4-8 near-duplicate lines per
# type.

macro accessorPair(bare, T: untyped): untyped =
  ## Emit `nameAccessor(bare, T)` and `nameAccessor(bareOpt, Option[T])`.
  let optName = ident($bare & "Opt")
  result = quote:
    nameAccessor(`bare`, `T`)
    nameAccessor(`optName`, Option[`T`])

macro arrayPair(base, T: untyped): untyped =
  ## Emit `nameAccessor(baseArray, seq[T])` and its Opt.
  let arrName = ident($base & "Array")
  let arrOptName = ident($base & "ArrayOpt")
  result = quote:
    nameAccessor(`arrName`, seq[`T`])
    nameAccessor(`arrOptName`, Option[seq[`T`]])

macro elemOptPair(base, T: untyped): untyped =
  ## Emit `nameAccessor(baseArrayElemOpt, seq[Option[T]])` and its Opt.
  let elemName = ident($base & "ArrayElemOpt")
  let elemOptName = ident($base & "ArrayElemOptOpt")
  result = quote:
    nameAccessor(`elemName`, seq[Option[`T`]])
    nameAccessor(`elemOptName`, Option[seq[Option[`T`]]])

macro rangeFamily(base, T: untyped): untyped =
  ## Emit the 8 Range/Multirange × {bare, Opt, Array, ArrayOpt} accessors.
  let r = ident($base & "Range")
  let rOpt = ident($base & "RangeOpt")
  let m = ident($base & "Multirange")
  let mOpt = ident($base & "MultirangeOpt")
  let rArr = ident($base & "RangeArray")
  let rArrOpt = ident($base & "RangeArrayOpt")
  let mArr = ident($base & "MultirangeArray")
  let mArrOpt = ident($base & "MultirangeArrayOpt")
  result = quote:
    nameAccessor(`r`, PgRange[`T`])
    nameAccessor(`rOpt`, Option[PgRange[`T`]])
    nameAccessor(`m`, PgMultirange[`T`])
    nameAccessor(`mOpt`, Option[PgMultirange[`T`]])
    nameAccessor(`rArr`, seq[PgRange[`T`]])
    nameAccessor(`rArrOpt`, Option[seq[PgRange[`T`]]])
    nameAccessor(`mArr`, seq[PgMultirange[`T`]])
    nameAccessor(`mArrOpt`, Option[seq[PgMultirange[`T`]]])

# Exceptions to the paired pattern: no matching Opt / non-seq array form.
nameAccessor(isNull, bool)
nameAccessor(getMoneyArrayND, PgArray[PgMoney])
nameAccessor(getMoneyArrayNDOpt, Option[PgArray[PgMoney]])

# Bare + Opt
accessorPair(getStr, string)
accessorPair(getInt, int32)
accessorPair(getInt16, int16)
accessorPair(getInt64, int64)
accessorPair(getFloat, float64)
accessorPair(getFloat32, float32)
accessorPair(getBool, bool)
accessorPair(getBytes, seq[byte])
accessorPair(getNumeric, PgNumeric)
accessorPair(getMoney, PgMoney)
accessorPair(getUuid, PgUuid)
accessorPair(getTimestamp, DateTime)
accessorPair(getTimestampTz, DateTime)
accessorPair(getDate, DateTime)
accessorPair(getTime, PgTime)
accessorPair(getTimeTz, PgTimeTz)
accessorPair(getJson, JsonNode)
accessorPair(getInterval, PgInterval)
accessorPair(getInet, PgInet)
accessorPair(getCidr, PgCidr)
accessorPair(getMacAddr, PgMacAddr)
accessorPair(getMacAddr8, PgMacAddr8)
accessorPair(getTsVector, PgTsVector)
accessorPair(getTsQuery, PgTsQuery)
accessorPair(getXml, PgXml)
accessorPair(getBit, PgBit)
accessorPair(getHstore, PgHstore)
accessorPair(getPoint, PgPoint)
accessorPair(getLine, PgLine)
accessorPair(getLseg, PgLseg)
accessorPair(getBox, PgBox)
accessorPair(getPath, PgPath)
accessorPair(getPolygon, PgPolygon)
accessorPair(getCircle, PgCircle)

# Array + ArrayOpt
arrayPair(getInt, int32)
arrayPair(getInt16, int16)
arrayPair(getInt64, int64)
arrayPair(getFloat, float64)
arrayPair(getFloat32, float32)
arrayPair(getBool, bool)
arrayPair(getStr, string)
arrayPair(getBit, PgBit)
arrayPair(getTimestamp, DateTime)
arrayPair(getTimestampTz, DateTime)
arrayPair(getDate, DateTime)
arrayPair(getTime, PgTime)
arrayPair(getTimeTz, PgTimeTz)
arrayPair(getInterval, PgInterval)
arrayPair(getUuid, PgUuid)
arrayPair(getInet, PgInet)
arrayPair(getCidr, PgCidr)
arrayPair(getMacAddr, PgMacAddr)
arrayPair(getMacAddr8, PgMacAddr8)
arrayPair(getNumeric, PgNumeric)
arrayPair(getMoney, PgMoney)
arrayPair(getBytes, seq[byte])
arrayPair(getJson, JsonNode)
arrayPair(getPoint, PgPoint)
arrayPair(getLine, PgLine)
arrayPair(getLseg, PgLseg)
arrayPair(getBox, PgBox)
arrayPair(getPath, PgPath)
arrayPair(getPolygon, PgPolygon)
arrayPair(getCircle, PgCircle)
arrayPair(getXml, PgXml)
arrayPair(getTsVector, PgTsVector)
arrayPair(getTsQuery, PgTsQuery)
arrayPair(getHstore, PgHstore)

# ArrayElemOpt + ArrayElemOptOpt
elemOptPair(getInt, int32)
elemOptPair(getInt16, int16)
elemOptPair(getInt64, int64)
elemOptPair(getFloat, float64)
elemOptPair(getFloat32, float32)
elemOptPair(getBool, bool)
elemOptPair(getStr, string)

# Range / Multirange × {bare, Opt, Array, ArrayOpt}
rangeFamily(getInt4, int32)
rangeFamily(getInt8, int64)
rangeFamily(getNum, PgNumeric)
rangeFamily(getTs, DateTime)
rangeFamily(getTsTz, DateTime)
rangeFamily(getDate, DateTime)

# Generic Option[T] dispatch: delegates to the bare `get(col, T)` plus a NULL
# check, so every bare type overload automatically gains an Option counterpart.
proc get*[T](row: Row, col: int, _: typedesc[Option[T]]): Option[T] =
  if row.isNull(col):
    none(T)
  else:
    some(row.get(col, T))
