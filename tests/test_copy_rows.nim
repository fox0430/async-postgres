## Unit tests for `pg_types/copy_rows`: the field-to-column plan and the binary
## COPY encoding behind `copyInRows`. PG-less.

import std/[unittest, options, strutils, json, times]

import ../async_postgres/[pg_protocol, pg_types]
import ../async_postgres/pg_types/copy_rows
from ../async_postgres/pg_types/encoding import encodeNumericBinary, encodeJsonbBinary

type
  Mood = enum
    happy = "happy"
    sad = "sad"

  Level = enum
    low
    high

  Moods = object
    a: Mood
    b: Option[Mood]
    c: Mood

  Email = distinct string

  Cents = distinct int64

  Pt = object
    x: int32
    y: int32

  User = object
    id {.pgSkip.}: int64
    name: string
    createdAt: DateTime
    email: Option[string]

  Renamed = object
    name {.pgColumn: "Full Name".}: string

  Twice = object
    userId: int64
    other {.pgColumn: "user_id".}: int64

  RefUser = ref object
    name: string

  Boxed = object
    pt: Pt

  Handle = object
    userId: string

  ShortArr = object

proc toPgBinaryParam(v: Cents): PgParam =
  PgParam(oid: OidInt8, format: 1, value: some(@(toBE64(int64(v)))))

proc toPgBinaryParam(v: Level): PgParam =
  PgParam(oid: OidInt4, format: 1, value: some(@(toBE32(int32(ord(v))))))

proc toPgBinaryParam(v: ShortArr): PgParam =
  ## Tagged text[], but shorter than an array header.
  PgParam(oid: OidTextArray, format: 1, value: some(@[0'u8, 0, 0, 0]))

pgComposite(Pt)

proc fd(name: string, oid: int32): FieldDescription =
  FieldDescription(name: name, typeOid: oid)

proc enc[F](v: F, oid: int32): seq[byte] =
  ## ``v`` as one COPY field of a column of type ``oid``.
  encodeCopyValue(result, v, copyConv(F, oid), oid)

proc field(payload: openArray[byte]): seq[byte] =
  ## A COPY field: its length, then ``payload``.
  result.addInt32(int32(payload.len))
  result.add(payload)

proc errMsg(body: proc()): string =
  try:
    body()
  except PgTypeError as e:
    return e.msg
  "no error"

suite "copy_rows: plan":
  test "fields match columns by to's rules, in field order":
    let plan = planCopyRows[User](
      @[
        fd("email", OidText),
        fd("id", OidInt8),
        fd("created_at", OidTimestampTz),
        fd("name", OidVarchar),
      ],
      columnsGiven = false,
    )
    check plan.columns == @["name", "created_at", "email"]
    check plan.oids == @[OidVarchar, OidTimestampTz, OidText]
    check plan.convs == @[ccText, ccTimestampTz, ccText]
    check plan.writes == @[false, true, true, true]

  test "pgColumn names the column exactly":
    let plan = planCopyRows[Renamed](@[fd("Full Name", OidText)], false)
    check plan.columns == @["Full Name"]
    let msg = errMsg(
      proc() =
        discard planCopyRows[Renamed](@[fd("full name", OidText)], false)
    )
    check "Renamed.name: no column named \"Full Name\"" in msg

  test "a field without a column fails unless columns were given":
    let fields = @[fd("name", OidText), fd("created_at", OidDate)]
    let msg = errMsg(
      proc() =
        discard planCopyRows[User](fields, false)
    )
    check "User.email: no column matches \"email\"" in msg
    let plan = planCopyRows[User](fields, true)
    check plan.columns == @["name", "created_at"]
    check plan.writes == @[false, true, true, false]

  test "a given column without a field fails":
    let msg = errMsg(
      proc() =
        discard planCopyRows[User](@[fd("name", OidText), fd("age", OidInt4)], true)
    )
    check msg == "User: no field matches column \"age\""

  test "two fields cannot write one column":
    # `userId` folds to `user_id`, which `other` names as well.
    let msg = errMsg(
      proc() =
        discard planCopyRows[Twice](@[fd("user_id", OidInt8), fd("x", OidInt8)], true)
    )
    check "is also written by Twice.userId" in msg

  test "an anonymous tuple goes by position":
    let plan = planCopyRows[(int, string)](@[fd("a", OidInt4), fd("b", OidText)], true)
    check plan.convs == @[ccInt4, ccText]
    let msg = errMsg(
      proc() =
        discard planCopyRows[(int, string)](@[fd("a", OidInt4)], true)
    )
    check "1 columns but the tuple has 2 fields" in msg

  test "a ref object plans like its object":
    let plan = planCopyRows[RefUser](@[fd("name", OidText)], false)
    check plan.columns == @["name"]

  test "an ambiguous column asks for pgColumn, not a query alias":
    let msg = errMsg(
      proc() =
        discard
          planCopyRows[Handle](@[fd("userid", OidText), fd("user_id", OidText)], false)
    )
    check msg ==
      "Handle.userId: more than one column matches (\"userid\", \"user_id\"); " &
      "name one with pgColumn"

  test "a mismatched type names the field, the column and the OID":
    let msg = errMsg(
      proc() =
        discard planCopyRows[User](
          @[fd("name", OidInt4), fd("created_at", OidDate), fd("email", OidText)], false
        )
    )
    check msg ==
      "User.name: column \"name\": string cannot be written to a column of type OID 23"

  test "a text-only encoder is refused for a binary column":
    let msg = errMsg(
      proc() =
        discard planCopyRows[Boxed](@[fd("pt", 16400'i32)], false)
    )
    check "Pt has only a text encoding" in msg

  test "an enum label to a user type awaits the check that it is an enum":
    # An OID past 2^31 is negative as an int32.
    let big = cast[int32](2_500_000_000'u32)
    let plan =
      planCopyRows[Moods](@[fd("a", 16400'i32), fd("b", big), fd("c", OidText)], false)
    check plan.convs == @[ccLabel, ccLabel, ccLabel]
    check plan.enumChecks.len == 2
    check plan.enumChecks[0].oid == 16400'i32
    check plan.enumChecks[1].oid == big
    plan.checkEnumColumns([big, 16400'i32])
    check errMsg(
      proc() =
        plan.checkEnumColumns([16400'i32])
    ) ==
      "Moods.b: column \"b\": Mood cannot be written to a column of type " &
      "OID 2500000000, which is not an enum"

  test "an enum label goes to a text type, not json or xml":
    for oid in [OidText, OidVarchar, OidBpchar, OidName]:
      check copyConv(Mood, oid) == ccLabel
    check errMsg(
      proc() =
        discard copyConv(Mood, OidJson)
    ) == "Mood cannot be written to a column of type OID 114"
    check errMsg(
      proc() =
        discard copyConv(Mood, OidXml)
    ) == "Mood cannot be written to a column of type OID 142"

suite "copy_rows: conversions":
  test "integers narrow and widen with a range check":
    check enc(7, OidInt2) == field(@(toBE16(7)))
    check enc(7'i16, OidInt4) == field(@(toBE32(7)))
    check enc(-7'i32, OidInt8) == field(@(toBE64(-7)))
    check enc(42, OidNumeric) == field(encodeNumericBinary(parsePgNumeric("42")))
    check errMsg(
      proc() =
        discard enc(int64(int32.high) + 1, OidInt4)
    ) == "value out of range for integer"
    check errMsg(
      proc() =
        discard enc(40000, OidInt2)
    ) == "value out of range for smallint"
    check errMsg(
      proc() =
        discard copyConv(int, OidFloat8)
    ) == "int cannot be written to a column of type OID 701"

  test "float64 rounds to real as PostgreSQL's cast does":
    check enc(0.5, OidFloat4) == field(@(toBE32(cast[int32](0.5'f32))))
    check enc(0.1'f32, OidFloat8) == field(@(toBE64(cast[int64](float64(0.1'f32)))))
    check enc(NaN, OidFloat4).len == 8
    check enc(Inf, OidFloat4) == field(@(toBE32(cast[int32](float32(Inf)))))
    check errMsg(
      proc() =
        discard enc(1e300, OidFloat4)
    ) == "value out of range for real: overflow"
    check errMsg(
      proc() =
        discard enc(1e-300, OidFloat4)
    ) == "value out of range for real: underflow"
    check enc(0.0, OidFloat4) == field(@(toBE32(0)))

  test "strings go to text types as is and are parsed for others":
    for oid in [OidText, OidVarchar, OidBpchar, OidName, OidJson, OidXml]:
      check enc("hi", oid) == field(@[byte('h'), byte('i')])
    check enc("{}", OidJsonb) == field(@[1'u8, byte('{'), byte('}')])
    check enc("00000000-0000-0000-0000-000000000001", OidUuid) ==
      field(@[0'u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
    check enc("1.5", OidNumeric) == field(encodeNumericBinary(parsePgNumeric("1.5")))
    check errMsg(
      proc() =
        discard copyConv(string, 16400'i32)
    ) == "string cannot be written to a column of type OID 16400"
    check errMsg(
      proc() =
        discard copyConv(string, OidChar)
    ) == "string cannot be written to a column of type OID 18"

  test "DateTime follows its column's type":
    let dt = dateTime(2000, mJan, 2, zone = utc())
    check enc(dt, OidDate) == field(@(toBE32(1)))
    check enc(dt, OidTimestamp) == field(@(toBE64(86_400_000_000)))
    check enc(dt, OidTimestampTz) == field(@(toBE64(86_400_000_000)))
    check errMsg(
      proc() =
        discard enc(default(DateTime), OidDate)
    ) == "Uninitialized DateTime"

  test "JSON, enums, bytes, bool and NULL":
    let node = %*{"a": 1}
    check enc(node, OidJson) == field(($node).toOpenArrayByte(0, ($node).high))
    check enc(node, OidJsonb) == field(encodeJsonbBinary(node))
    check errMsg(
      proc() =
        discard enc(JsonNode(nil), OidJson)
    ) == "nil JsonNode; use Option[JsonNode] for NULL"
    check enc(sad, 16400'i32) == field(@[byte('s'), byte('a'), byte('d')])
    check enc(happy, OidText).len == 9
    check errMsg(
      proc() =
        discard copyConv(Mood, OidInt4)
    ) == "Mood cannot be written to a column of type OID 23"
    check enc(@[1'u8, 2], OidBytea) == field(@[1'u8, 2])
    check enc(true, OidBool) == field(@[1'u8])
    check enc(none(int), OidInt4) == @(toBE32(-1))
    check enc(some(3), OidInt4) == field(@(toBE32(3)))

  test "integer and DateTime arrays are written per element":
    var want: seq[byte]
    want.addInt32(1) # ndim
    want.addInt32(1) # has NULL
    want.addInt32(OidInt4)
    want.addInt32(2) # length
    want.addInt32(1) # lower bound
    want.add(field(@(toBE32(5))))
    want.addInt32(-1)
    check enc(@[some(5), none(int)], OidInt4Array) == field(want)
    check enc(@[some(Positive(5)), none(Positive)], OidInt4Array) == field(want)
    var nat: seq[byte]
    nat.addInt32(1)
    nat.addInt32(0)
    nat.addInt32(OidInt8)
    nat.addInt32(1)
    nat.addInt32(1)
    nat.add(field(@(toBE64(5))))
    check enc(@[Natural(5)], OidInt8Array) == field(nat)
    var empty: seq[byte]
    empty.addInt32(0)
    empty.addInt32(0)
    empty.addInt32(OidInt2)
    check enc(newSeq[int](), OidInt2Array) == field(empty)
    check errMsg(
      proc() =
        discard enc(@[70000], OidInt2Array)
    ) == "value out of range for smallint"
    var nums: seq[byte]
    nums.addInt32(1)
    nums.addInt32(0)
    nums.addInt32(OidNumeric)
    nums.addInt32(1)
    nums.addInt32(1)
    nums.add(field(encodeNumericBinary(parsePgNumeric("12"))))
    check enc(@[12], OidNumericArray) == field(nums)
    let dates = enc(@[dateTime(2000, mJan, 2, zone = utc())], OidDateArray)
    check dates[4 + 8 ..< 4 + 12] == @(toBE32(OidDate))
    check dates[^4 ..^ 1] == @(toBE32(1))

  test "an encoder's payload must be the column's type":
    let uid = PgUuid("00000000-0000-0000-0000-000000000001")
    check enc(uid, OidUuid).len == 20
    check errMsg(
      proc() =
        discard copyConv(PgUuid, OidText)
    ) == "PgUuid cannot be written to a column of type OID 25"
    check enc(Cents(9), OidInt8) == field(@(toBE64(9)))
    check errMsg(
      proc() =
        discard copyConv(Cents, OidInt4)
    ) == "Cents cannot be written to a column of type OID 23"

  test "an enum with its own encoder goes by it, not its label":
    check enc(high, OidInt4) == field(@(toBE32(1)))
    check errMsg(
      proc() =
        discard copyConv(Level, 16400'i32)
    ) == "Level cannot be written to a column of type OID 16400"

  test "bit and varbit share their payload, scalar and array":
    let bits = initPgBit(3, @[0b1010_0000'u8])
    check enc(bits, OidBit) == enc(bits, OidVarbit)
    let arr = enc(@[bits], OidBitArray)
    check arr[4 + 8 ..< 4 + 12] == @(toBE32(OidBit))

  test "a text array may go to another text array type":
    let tags = enc(@["a"], OidVarcharArray)
    check tags[4 + 8 ..< 4 + 12] == @(toBE32(OidVarchar))
    check enc(@["a"], OidTextArray)[4 + 8 ..< 4 + 12] == @(toBE32(OidText))
    check errMsg(
      proc() =
        discard copyConv(seq[string], OidInt4Array)
    ) == "seq[string] cannot be written to a column of type OID 1007"

  test "a payload too short for an array header is refused, not patched":
    check errMsg(
      proc() =
        discard enc(ShortArr(), OidVarcharArray)
    ) == "ShortArr (encoded as OID 1009): 4 bytes, too short for an array header"
    check enc(ShortArr(), OidTextArray) == field(@[0'u8, 0, 0, 0])

  test "a distinct type without an encoder is written as its base":
    check enc(Email("a@b"), OidText) == field(@[byte('a'), byte('@'), byte('b')])

suite "copy_rows: rows":
  test "a row is its column count, then its fields in plan order":
    let plan = planCopyRows[User](
      @[fd("name", OidText), fd("created_at", OidDate), fd("email", OidText)], false
    )
    var buf: seq[byte]
    buf.encodeCopyRow(
      User(id: 9, name: "a", createdAt: dateTime(2000, mJan, 1, zone = utc())), plan
    )
    var want: seq[byte]
    want.addInt16(3)
    want.add(field(@[byte('a')]))
    want.add(field(@(toBE32(0))))
    want.addInt32(-1)
    check buf == want

  test "an error names the row, the field and the column":
    let plan = planCopyRows[(int, string)](@[fd("n", OidInt2), fd("s", OidText)], true)
    var buf: seq[byte]
    var next = 0
    let msg = errMsg(
      proc() =
        buf.encodeCopyRows([(1, "a"), (99999, "b")], next, plan, 1 shl 20)
    )
    check msg == "row 1: (int, string): column \"n\": value out of range for smallint"
    check next == 1

  test "rows stop once the buffer reaches the limit":
    let plan = planCopyRows[(int,)](@[fd("n", OidInt8)], true)
    let rows = @[(1,), (2,), (3,)]
    var buf: seq[byte]
    var next = 0
    buf.encodeCopyRows(rows, next, plan, 1)
    check next == 1
    buf.encodeCopyRows(rows, next, plan, 1 shl 20)
    check next == 3
    check buf.len == 3 * (2 + 4 + 8)

  test "a nil ref row fails":
    let plan = planCopyRows[RefUser](@[fd("name", OidText)], false)
    var buf: seq[byte]
    check errMsg(
      proc() =
        buf.encodeCopyRow(RefUser(nil), plan)
    ) == "RefUser: nil row"
