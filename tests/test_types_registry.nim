## Cross-checks every ``PgArray[T]`` registration against the scalar codecs
## and both array codecs. The literal OIDs are PostgreSQL's ``pg_type.oid``.

import std/[algorithm, json, macrocache, macros, net, options, times, unittest]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}

import types_common

const fixtureTypes = CacheSeq"test_types_registry.fixtureTypes"

# Their wire length depends on the value; every other registered type has a
# fixed width.
const varWidthTypes = [
  "string", "PgXml", "PgBit", "JsonNode", "PgNumeric", "PgInet", "PgCidr", "PgPath",
  "PgPolygon",
]

# Pinned so a dropped or duplicated registration fails here: the registered
# names and the fixture names come from the same cache, so comparing them
# cannot see a removal from both sides.
const expectedRegisteredTypes = [
  "JsonNode", "PgBit", "PgBox", "PgCidr", "PgCircle", "PgInet", "PgInterval", "PgLine",
  "PgLseg", "PgMacAddr", "PgMacAddr8", "PgNumeric", "PgPath", "PgPoint", "PgPolygon",
  "PgTime", "PgTimeTz", "PgUuid", "PgXml", "bool", "float32", "float64", "int16",
  "int32", "int64", "string",
]

macro recordFixture(T: typed): untyped =
  fixtureTypes.add(T)
  result = newStmtList()

macro forEachRegistered(check: untyped): untyped =
  result = newStmtList()
  for T in pgArrayElemTypes:
    result.add newCall(check, T)

macro forEachRegisteredOptSeq(check: untyped): untyped =
  var optSeqNames: seq[string]
  for T in pgArrayOptSeqElemTypes:
    optSeqNames.add T.repr
  result = newStmtList()
  for T in pgArrayElemTypes:
    result.add newCall(check, T, newLit(T.repr in optSeqNames))

macro typeNames(registered: static bool): untyped =
  var names: seq[string]
  if registered:
    for T in pgArrayElemTypes:
      names.add T.repr
  else:
    for T in fixtureTypes:
      names.add T.repr
  newLit(names.sorted)

template fixture(T: typedesc, value: untyped, scalarGetter, arrayGetter: untyped) =
  ## A sample value and the scalar and 1-D getters for one registered type.
  ## ``forEachRegistered`` does not compile while a registered type lacks one.
  proc sample(_: typedesc[T]): T =
    value

  proc readScalar(_: typedesc[T], row: Row): T =
    scalarGetter(row, 0)

  proc read1D(_: typedesc[T], row: Row): seq[T] =
    arrayGetter(row, 0)

  recordFixture(T)

proc elemWire[T](v: T, elemOid: int32): seq[byte] =
  toPgBinaryParam(v).value.get

proc elemWire(v: JsonNode, elemOid: int32): seq[byte] =
  # ``json`` carries the bare text; only ``jsonb`` has the version byte.
  if elemOid == OidJson:
    toBytes($v)
  else:
    toPgBinaryParam(v).value.get

proc encodedElem[T](v: T): seq[byte] =
  ## The element bytes the array encoder sends: the fixed-width path writes
  ## into one buffer, the variable-width path builds a ``seq`` per element.
  when pgArrayElemFixedWidth(T) > 0:
    result = newSeq[byte](pgArrayElemFixedWidth(T))
    writePgArrayElementAt(result, 0, v)
  else:
    result = encodePgArrayElement(v)

template checkOptSeqPair(T: typedesc, expected: static bool) =
  block:
    checkpoint $T
    # ``optSeq`` registrations yield ``toPgParam(seq[Option[T]])`` while the
    # generic ``get`` overloads for ``seq[Option[T]]`` delegate to the
    # hand-written ElemOpt getters. One side without the other is a
    # half-exposed API, so both sides must match the registry flag.
    check compiles(toPgParam(default(seq[Option[T]]))) == expected
    check compiles(get(default(Row), 0, seq[Option[T]])) == expected

template checkRegistered(T: typedesc) =
  block:
    checkpoint $T
    let v = sample(T)
    let elemOid = pgArrayElemOid(T)
    let arrayOid = pgArrayArrayOid(T)
    # rfAuto requests these in binary on a statement-cache hit. Of the element
    # OIDs only the canonical one is; bpchar / name / "char" columns stay
    # text. "char"[] stays text too: its text form escapes bytes that binary
    # sends raw, so the value would change with the cache state.
    check isBinarySafeOid(elemOid)
    check elemOid in pgArrayAcceptOids(T)
    check arrayOid in pgArrayAcceptArrayOids(T)
    check pgArrayAcceptArrayOids(T).len == pgArrayAcceptOids(T).len
    for arrOid in pgArrayAcceptArrayOids(T):
      check isBinarySafeOid(arrOid) == (arrOid != OidCharArray)
    # The scalar binary encoder is the reference: its bytes are pinned
    # elsewhere, and both array encoders must send the same element bytes.
    let scalar = toPgBinaryParam(v)
    check scalar.oid == elemOid
    let elem = scalar.value.get
    check encodedElem(v) == elem
    let fixed = $T notin varWidthTypes
    check pgArrayElemFixedWidth(T) == (if fixed: elem.len else: 0)
    # Their length is checked for every element before any is copied.
    check pgArrayElemCheckFirst(T) == ($T in ["string", "PgXml"])
    # Only the variable-width path can raise ``PgProtocolError``, so this
    # checks the path the generated encoders take, not just the width.
    type FixedPathEncoder = proc(v: seq[T]): PgParam {.nimcall, raises: [PgTypeError].}
    check compiles((let p: FixedPathEncoder = toPgParam)) == fixed
    let oneDim = toPgParam(@[v])
    check oneDim.oid == arrayOid
    check oneDim.value.get == encodeBinaryArray(elemOid, @[elem])
    let nDim = toPgParam(pgArray(@[v]))
    check nDim.oid == arrayOid
    check nDim.value == oneDim.value
    # ``pgParams`` binds only the ``toPgParam`` overloads its module can see.
    check pgParams(@[v], pgArray(@[v])) == @[oneDim, nDim]
    # Which types have a ``seq[Option[T]]`` encoder is pinned in test_aggregate.
    when compiles(toPgParam(@[some(v)])):
      check pgParams(@[some(v)]) == @[toPgParam(@[some(v)])]
      check toPgParam(@[some(v), none(T)]) == toPgParam(pgArray(@[some(v), none(T)]))
    # Every element OID the decoders accept reads back as ``v`` through the
    # scalar, 1-D and N-D accessors alike.
    for oid in pgArrayAcceptOids(T):
      checkpoint "elemOid " & $oid
      let wire = elemWire(v, oid)
      check readScalar(T, mkRow(@[some(wire)], @[mkField(oid, 1)])) == v
      let arrRow =
        mkRow(@[some(encodeBinaryArray(oid, @[wire]))], @[mkField(arrayOid, 1)])
      check read1D(T, arrRow) == @[v]
      check getArrayND[T](arrRow, 0) == pgArray(@[v])

fixture(int16, 7'i16, getInt16, getInt16Array)
fixture(int32, 7'i32, getInt, getIntArray)
fixture(int64, 7'i64, getInt64, getInt64Array)
fixture(float32, 1.5'f32, getFloat32, getFloat32Array)
fixture(float64, 1.5'f64, getFloat, getFloatArray)
fixture(bool, true, getBool, getBoolArray)
fixture(string, "ab", getStr, getStrArray)
fixture(PgXml, PgXml("<a/>"), getXml, getXmlArray)
fixture(PgBit, parseBitString("101"), getBit, getBitArray)
fixture(JsonNode, %*{"a": 1}, getJson, getJsonArray)
fixture(PgUuid, PgUuid("550e8400-e29b-41d4-a716-446655440000"), getUuid, getUuidArray)
fixture(PgNumeric, parsePgNumeric("1.5"), getNumeric, getNumericArray)
fixture(
  PgInterval,
  PgInterval(months: 1, days: 2, microseconds: 3),
  getInterval,
  getIntervalArray,
)
fixture(
  PgTime, PgTime(hour: 1, minute: 2, second: 3, microsecond: 4), getTime, getTimeArray
)
fixture(
  PgTimeTz,
  PgTimeTz(hour: 1, minute: 2, second: 3, microsecond: 4, utcOffset: 3600),
  getTimeTz,
  getTimeTzArray,
)
fixture(
  PgInet, PgInet(address: parseIpAddress("10.0.0.1"), mask: 24), getInet, getInetArray
)
fixture(
  PgCidr, PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8), getCidr, getCidrArray
)
fixture(PgMacAddr, PgMacAddr("08:00:2b:01:02:03"), getMacAddr, getMacAddrArray)
fixture(
  PgMacAddr8, PgMacAddr8("08:00:2b:01:02:03:04:05"), getMacAddr8, getMacAddr8Array
)
fixture(PgPoint, PgPoint(x: 1, y: 2), getPoint, getPointArray)
fixture(PgLine, PgLine(a: 1, b: 2, c: 3), getLine, getLineArray)
fixture(
  PgLseg,
  PgLseg(p1: PgPoint(x: 1, y: 2), p2: PgPoint(x: 3, y: 4)),
  getLseg,
  getLsegArray,
)
fixture(
  PgBox, PgBox(high: PgPoint(x: 3, y: 4), low: PgPoint(x: 1, y: 2)), getBox, getBoxArray
)
fixture(
  PgPath,
  PgPath(closed: true, points: @[PgPoint(x: 1, y: 2), PgPoint(x: 3, y: 4)]),
  getPath,
  getPathArray,
)
fixture(
  PgPolygon,
  PgPolygon(points: @[PgPoint(x: 1, y: 2), PgPoint(x: 3, y: 4)]),
  getPolygon,
  getPolygonArray,
)
fixture(
  PgCircle,
  PgCircle(center: PgPoint(x: 1, y: 2), radius: 1.5),
  getCircle,
  getCircleArray,
)

suite "Array OID constants vs the PostgreSQL catalog":
  test "scalar OIDs":
    check OidBool == 16'i32
    check OidBytea == 17'i32
    check OidChar == 18'i32
    check OidName == 19'i32
    check OidInt8 == 20'i32
    check OidInt2 == 21'i32
    check OidInt4 == 23'i32
    check OidText == 25'i32
    check OidJson == 114'i32
    check OidXml == 142'i32
    check OidPoint == 600'i32
    check OidLseg == 601'i32
    check OidPath == 602'i32
    check OidBox == 603'i32
    check OidPolygon == 604'i32
    check OidLine == 628'i32
    check OidCidr == 650'i32
    check OidFloat4 == 700'i32
    check OidFloat8 == 701'i32
    check OidCircle == 718'i32
    check OidMacAddr8 == 774'i32
    check OidMoney == 790'i32
    check OidMacAddr == 829'i32
    check OidInet == 869'i32
    check OidBpchar == 1042'i32
    check OidVarchar == 1043'i32
    check OidDate == 1082'i32
    check OidTime == 1083'i32
    check OidTimestamp == 1114'i32
    check OidTimestampTz == 1184'i32
    check OidInterval == 1186'i32
    check OidTimeTz == 1266'i32
    check OidBit == 1560'i32
    check OidVarbit == 1562'i32
    check OidNumeric == 1700'i32
    check OidUuid == 2950'i32
    check OidTsVector == 3614'i32
    check OidTsQuery == 3615'i32
    check OidJsonb == 3802'i32

  test "range and multirange OIDs":
    check OidInt4Range == 3904'i32
    check OidNumRange == 3906'i32
    check OidTsRange == 3908'i32
    check OidTsTzRange == 3910'i32
    check OidDateRange == 3912'i32
    check OidInt8Range == 3926'i32
    check OidInt4Multirange == 4451'i32
    check OidNumMultirange == 4532'i32
    check OidTsMultirange == 4533'i32
    check OidTsTzMultirange == 4534'i32
    check OidDateMultirange == 4535'i32
    check OidInt8Multirange == 4536'i32

  test "array OIDs":
    check OidBoolArray == 1000'i32
    check OidByteaArray == 1001'i32
    check OidCharArray == 1002'i32
    check OidNameArray == 1003'i32
    check OidInt2Array == 1005'i32
    check OidInt4Array == 1007'i32
    check OidTextArray == 1009'i32
    check OidBpcharArray == 1014'i32
    check OidVarcharArray == 1015'i32
    check OidInt8Array == 1016'i32
    check OidPointArray == 1017'i32
    check OidLsegArray == 1018'i32
    check OidPathArray == 1019'i32
    check OidBoxArray == 1020'i32
    check OidFloat4Array == 1021'i32
    check OidFloat8Array == 1022'i32
    check OidPolygonArray == 1027'i32
    check OidMacAddrArray == 1040'i32
    check OidInetArray == 1041'i32
    check OidTimestampArray == 1115'i32
    check OidDateArray == 1182'i32
    check OidTimeArray == 1183'i32
    check OidTimestampTzArray == 1185'i32
    check OidIntervalArray == 1187'i32
    check OidNumericArray == 1231'i32
    check OidTimeTzArray == 1270'i32
    check OidBitArray == 1561'i32
    check OidVarbitArray == 1563'i32
    check OidUuidArray == 2951'i32
    check OidJsonArray == 199'i32
    check OidJsonbArray == 3807'i32
    check OidTsVectorArray == 3643'i32
    check OidTsQueryArray == 3645'i32
    check OidXmlArray == 143'i32
    check OidLineArray == 629'i32
    check OidCircleArray == 719'i32
    check OidCidrArray == 651'i32
    check OidMacAddr8Array == 775'i32
    check OidMoneyArray == 791'i32

  test "range and multirange array OIDs":
    check OidInt4RangeArray == 3905'i32
    check OidNumRangeArray == 3907'i32
    check OidTsRangeArray == 3909'i32
    check OidTsTzRangeArray == 3911'i32
    check OidDateRangeArray == 3913'i32
    check OidInt8RangeArray == 3927'i32
    check OidInt4MultirangeArray == 6150'i32
    check OidNumMultirangeArray == 6151'i32
    check OidTsMultirangeArray == 6152'i32
    check OidTsTzMultirangeArray == 6153'i32
    check OidDateMultirangeArray == 6155'i32
    check OidInt8MultirangeArray == 6157'i32

suite "Array registry vs the scalar codecs and both array codecs":
  test "every registered type has a fixture and nothing else does":
    check typeNames(registered = true) == expectedRegisteredTypes.sorted
    check typeNames(registered = false) == typeNames(registered = true)

  test "registered types":
    forEachRegistered(checkRegistered)

  test "optSeq encoders and ElemOpt decoders agree":
    forEachRegisteredOptSeq(checkOptSeqPair)

  test "types outside the registry stay outside":
    # They have scalar or 1-D codecs only; the generic codec rejects them at
    # compile time, mostly with an ``{.error.}`` naming the alternative.
    template outside(T: typedesc) =
      check not compiles(pgArrayAcceptOids(T))
      check not compiles(pgArrayAcceptArrayOids(T))
      check not compiles(toPgParam(default(PgArray[T])))
      check not compiles(getArrayND[T](default(Row), 0))

    outside(PgMoney)
    outside(PgTsVector)
    outside(PgTsQuery)
    outside(PgHstore)
    outside(seq[byte])
    outside(DateTime)
