import std/[unittest, options, strutils, times]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types
import ../async_postgres/pg_connection
from row_mapping_fixture import RemoteBill, RemoteFee

type
  Col = tuple[name: string, oid: int32, fmt: int16]

  Mood = enum
    happy = "happy"
    sad = "sad"

  UserId = distinct int64

  Point2 = object
    x: int32
    label: string

  Cents = object
    value: int64

  User = object
    id: int32
    name: string
    email: Option[string]

  Account = object
    accountId: int64
    displayName: string
    createdAt: DateTime

  Renamed = object
    name {.pgColumn: "Full Name".}: string
    note {.pgSkip.}: string = "kept"

  Typed = object
    count: int
    mood: Mood
    moods: seq[Mood]
    maybeMood: Option[Mood]
    owner: UserId
    at: Point2
    price: Cents
    tags: seq[string]

  Stamps = object
    ts: DateTime
    tstz: DateTime
    day: DateTime

  RefUser = ref object
    id: int32
    name: string

  BaseRec = ref object of RootObj
    id: int32

  ChildRec = ref object of BaseRec
    name: string

  Variant = object
    case isNum: bool
    of true:
      num: int32
    of false:
      text: string

  Unreadable = object
    letters: seq[char]

proc `==`(a, b: UserId): bool {.borrow.}

pgDomain(UserId, int64)
pgComposite(Point2)

proc get(row: Row, col: int, T: typedesc[Cents]): Cents =
  Cents(value: int64(parseFloat(row.getStr(col)) * 100))

proc txt(s: string): Option[seq[byte]] =
  some(toBytes(s))

proc bin(p: PgParam): Option[seq[byte]] =
  p.value

proc mkData(cols: openArray[Col], rows: openArray[seq[Option[seq[byte]]]]): RowData =
  result = RowData(numCols: int16(cols.len))
  for c in cols:
    result.colFormats.add(c.fmt)
    result.colTypeOids.add(c.oid)
    result.fields.add(FieldDescription(name: c.name, typeOid: c.oid, formatCode: c.fmt))
  for cells in rows:
    for cell in cells:
      if cell.isNone:
        result.cellIndex.add([0'i32, -1'i32])
      else:
        result.cellIndex.add([int32(result.buf.len), int32(cell.get.len)])
        result.buf.add(cell.get)

proc mkRow(cols: openArray[Col], cells: seq[Option[seq[byte]]]): Row =
  initRow(mkData(cols, [cells]), 0)

suite "to(Row, T) by name":
  test "reads each field from the column of its name":
    let row = mkRow(
      [("email", OidText, 0'i16), ("id", OidInt4, 1'i16), ("name", OidText, 0'i16)],
      @[txt("a@example.com"), bin(toPgBinaryParam(7'i32)), txt("Alice")],
    )
    let u = row.to(User)
    check u.id == 7
    check u.name == "Alice"
    check u.email == some("a@example.com")

  test "Option field reads NULL as none":
    let row = mkRow(
      [("id", OidInt4, 0'i16), ("name", OidText, 0'i16), ("email", OidText, 0'i16)],
      @[txt("1"), txt("Bob"), none(seq[byte])],
    )
    check row.to(User).email.isNone

  test "matching ignores case and underscores":
    let row = mkRow(
      [
        ("account_id", OidInt8, 0'i16),
        ("DISPLAY_NAME", OidText, 0'i16),
        ("created_at", OidTimestamp, 0'i16),
      ],
      @[txt("42"), txt("Carol"), txt("2024-01-15 10:30:00")],
    )
    let a = row.to(Account)
    check a.accountId == 42
    check a.displayName == "Carol"
    check a.createdAt.year == 2024

  test "a byte-exact match wins over folded ones":
    type Nick = object
      username: string

    let row = mkRow(
      [("user_name", OidText, 0'i16), ("username", OidText, 0'i16)],
      @[txt("folded"), txt("exact")],
    )
    check row.to(Nick).username == "exact"

  test "several folded matches are ambiguous":
    type Nick = object
      userName: string

    let row = mkRow(
      [("user_name", OidText, 0'i16), ("username", OidText, 0'i16)],
      @[txt("a"), txt("b")],
    )
    try:
      discard row.to(Nick)
      fail()
    except PgTypeError as e:
      check "Nick.userName: more than one column matches" in e.msg
      check "\"user_name\", \"username\"" in e.msg
      check "pgColumn" in e.msg

  test "a duplicated column name is ambiguous":
    let row = mkRow(
      [("id", OidInt4, 0'i16), ("name", OidText, 0'i16), ("id", OidInt4, 0'i16)],
      @[txt("1"), txt("x"), txt("2")],
    )
    try:
      discard row.to(User)
      fail()
    except PgTypeError as e:
      check "User.id: more than one column matches (\"id\", \"id\")" in e.msg
      check "pgColumn" notin e.msg

  test "a field with no column raises":
    let row =
      mkRow([("id", OidInt4, 0'i16), ("name", OidText, 0'i16)], @[txt("1"), txt("x")])
    try:
      discard row.to(User)
      fail()
    except PgTypeError as e:
      check "User.email: no column matches \"email\"" in e.msg

  test "extra columns are ignored":
    let row = mkRow(
      [
        ("id", OidInt4, 0'i16),
        ("extra", OidText, 0'i16),
        ("name", OidText, 0'i16),
        ("email", OidText, 0'i16),
      ],
      @[txt("1"), txt("ignored"), txt("x"), none(seq[byte])],
    )
    check row.to(User).name == "x"

  test "NULL in a non-Option field names the field":
    let row = mkRow(
      [("id", OidInt4, 0'i16), ("name", OidText, 0'i16), ("email", OidText, 0'i16)],
      @[txt("1"), none(seq[byte]), none(seq[byte])],
    )
    try:
      discard row.to(User)
      fail()
    except PgTypeError as e:
      check e.msg.startsWith("User.name: ")
      check "NULL" in e.msg

  test "a decode failure names the field":
    let row = mkRow(
      [("id", OidInt4, 0'i16), ("name", OidText, 0'i16), ("email", OidText, 0'i16)],
      @[txt("not a number"), txt("x"), none(seq[byte])],
    )
    expect PgTypeError:
      try:
        discard row.to(User)
      except PgTypeError as e:
        check e.msg.startsWith("User.id: Column 0: ")
        raise e

  test "pgColumn names the column byte for byte":
    let row = mkRow([("Full Name", OidText, 0'i16)], @[txt("Dana")])
    let r = row.to(Renamed)
    check r.name == "Dana"
    check r.note == "kept"

  test "pgColumn does not fold case":
    let row = mkRow([("full name", OidText, 0'i16)], @[txt("Dana")])
    try:
      discard row.to(Renamed)
      fail()
    except PgTypeError as e:
      check "Renamed.name: no column named \"Full Name\"" in e.msg

  test "pgSkip reads no column even when one matches":
    let row = mkRow(
      [("Full Name", OidText, 0'i16), ("note", OidText, 0'i16)],
      @[txt("Dana"), txt("from row")],
    )
    check row.to(Renamed).note == "kept"

  test "a Row without field metadata cannot map by name":
    let row: Row = @[txt("1"), txt("x"), txt("y")]
    try:
      discard row.to(User)
      fail()
    except PgTypeError as e:
      check "User.id: column name lookup requires field metadata" in e.msg

  test "a row with no columns says so":
    let row = mkRow([], @[])
    try:
      discard row.to(User)
      fail()
    except PgTypeError as e:
      check "User.id: the row has no columns" in e.msg

suite "to(Row, T) field types":
  test "int, enum, enum array, domain, composite and a caller's get overload":
    let row = mkRow(
      [
        ("count", OidInt4, 1'i16),
        ("mood", 16500'i32, 0'i16),
        ("moods", 16501'i32, 0'i16),
        ("maybe_mood", 16500'i32, 0'i16),
        ("owner", OidInt8, 1'i16),
        ("at", 16502'i32, 0'i16),
        ("price", OidNumeric, 0'i16),
        ("tags", OidTextArray, 0'i16),
      ],
      @[
        bin(toPgBinaryParam(5'i32)),
        txt("sad"),
        txt("{happy,sad}"),
        none(seq[byte]),
        bin(toPgBinaryParam(9'i64)),
        txt("(3,\"a b\")"),
        txt("12.34"),
        txt("{x,y}"),
      ],
    )
    let t = row.to(Typed)
    check t.count == 5
    check t.mood == sad
    check t.moods == @[happy, sad]
    check t.maybeMood.isNone
    check t.owner == UserId(9)
    check t.at == Point2(x: 3, label: "a b")
    check t.price.value == 1234
    check t.tags == @["x", "y"]

  test "int reads an int8 column":
    type Big = object
      n: int

    let row = mkRow([("n", OidInt8, 1'i16)], @[bin(toPgBinaryParam(1'i64 shl 40))])
    check row.to(Big).n == 1 shl 40

  test "DateTime follows the column type":
    let t = dateTime(2024, mJan, 15, 10, 30, 0, zone = utc())
    let row = mkRow(
      [
        ("ts", OidTimestamp, 1'i16),
        ("tstz", OidTimestampTz, 1'i16),
        ("day", OidDate, 1'i16),
      ],
      @[
        bin(toPgBinaryParam(t)),
        bin(toPgBinaryTimestampTzParam(t)),
        bin(toPgBinaryDateParam(t)),
      ],
    )
    let s = row.to(Stamps)
    check s.ts == t
    check s.tstz == t
    check s.day == dateTime(2024, mJan, 15, zone = utc())

  test "a distinct DateTime follows the column type":
    type
      At = distinct DateTime
      Ats = object
        tstz: At
        day: At

    let t = dateTime(2024, mJan, 15, 10, 30, 0, zone = utc())
    let row = mkRow(
      [("tstz", OidTimestampTz, 1'i16), ("day", OidDate, 1'i16)],
      @[bin(toPgBinaryTimestampTzParam(t)), bin(toPgBinaryDateParam(t))],
    )
    let a = row.to(Ats)
    check DateTime(a.tstz) == t
    check DateTime(a.day) == dateTime(2024, mJan, 15, zone = utc())

  test "DateTime reads a text date by the field's type OID":
    type Day = object
      day: DateTime

    let rd = mkData([("day", OidDate, 0'i16)], [@[txt("2024-01-15")]])
    rd.colTypeOids.setLen(0)
    check initRow(rd, 0).to(Day).day == dateTime(2024, mJan, 15, zone = utc())

  test "seq[DateTime] follows the column type":
    type Days = object
      days: seq[DateTime]

    let row = mkRow([("days", OidDateArray, 0'i16)], @[txt("{2024-01-15}")])
    check row.to(Days).days == @[dateTime(2024, mJan, 15, zone = utc())]

  test "DateTime ranges follow the column type":
    type Times = object
      ts: PgRange[DateTime]
      tstz: PgRange[DateTime]
      day: PgRange[DateTime]

    let r = PgRange[DateTime](
      hasLower: true,
      lower: PgRangeBound[DateTime](
        value: dateTime(2024, mJan, 1, zone = utc()), inclusive: true
      ),
      hasUpper: true,
      upper: PgRangeBound[DateTime](
        value: dateTime(2024, mFeb, 1, zone = utc()), inclusive: false
      ),
    )
    let row = mkRow(
      [
        ("ts", OidTsRange, 1'i16),
        ("tstz", OidTsTzRange, 1'i16),
        ("day", OidDateRange, 1'i16),
      ],
      @[
        bin(toPgBinaryParam(r)),
        bin(toPgBinaryTsTzRangeParam(r)),
        bin(toPgBinaryDateRangeParam(r)),
      ],
    )
    let got = row.to(Times)
    check got.ts == r
    check got.tstz == r
    check got.day == r

  test "DateTime ranges and multiranges read text by the column type":
    type Spans = object
      ts: PgRange[DateTime]
      day: PgRange[DateTime]
      mts: PgMultirange[DateTime]
      mday: PgMultirange[DateTime]

    let row = mkRow(
      [
        ("ts", OidTsRange, 0'i16),
        ("day", OidDateRange, 0'i16),
        ("mts", OidTsMultirange, 0'i16),
        ("mday", OidDateMultirange, 0'i16),
      ],
      @[
        txt("[2024-01-01 00:00:00,2024-02-01 00:00:00)"),
        txt("[2024-01-01,2024-02-01)"),
        txt("{[2024-01-01 00:00:00,2024-02-01 00:00:00)}"),
        txt("{[2024-01-01,2024-02-01)}"),
      ],
    )
    let spans = row.to(Spans)
    check spans.ts.lower.value == dateTime(2024, mJan, 1, zone = utc())
    check spans.day.lower.value == dateTime(2024, mJan, 1, zone = utc())
    check spans.mts.len == 1
    check spans.mday.len == 1

  test "PgArray reads through getArrayND":
    type Grid = object
      grid: PgArray[int32]

    let cells = @[
      some(@(toBE32(1'i32))),
      some(@(toBE32(2'i32))),
      none(seq[byte]),
      some(@(toBE32(4'i32))),
    ]
    let row = mkRow(
      [("grid", OidInt4Array, 1'i16)],
      @[some(encodeBinaryArray(OidInt4, [2'i32, 2], cells))],
    )
    let g = row.to(Grid).grid
    check g.dims == @[2'i32, 2]
    check g.elements == @[some(1'i32), some(2'i32), none(int32), some(4'i32)]

  test "PgArray needs a binary column":
    type TextGrid = object
      grid: PgArray[int32]

    let row = mkRow([("grid", OidInt4Array, 0'i16)], @[txt("{1,2,3}")])
    try:
      discard row.to(TextGrid)
      fail()
    except PgTypeError as e:
      check e.msg.startsWith("TextGrid.grid: ")
      check "binary column format" in e.msg

  test "a type with no reader does not compile":
    let row = mkRow([("id", OidInt4, 0'i16)], @[txt("1")])
    check compiles(row.to(User))
    check not compiles(row.to(Unreadable))

  test "an object reads only through a get, not as a composite by default":
    type
      Plain = object
        x: int32
        label: string

      Registered = object
        at: Point2

      Unregistered = object
        at: Plain

      Stdlib = object
        at: Time
        span: Duration

    let row = mkRow([("at", 16502'i32, 0'i16)], @[txt("(3,\"a b\")")])
    check row.to(Registered).at == Point2(x: 3, label: "a b")
    check not compiles(row.to(Unregistered))
    check not compiles(row.to(Stdlib))

  test "a get the caller does not import does not compile":
    let row = mkRow([("fee", OidInt8, 0'i16)], @[txt("7")])
    check row_mapping_fixture.get(row, 0, RemoteFee).cents == 7
    check not compiles(row.to(RemoteBill))

  test "a case object does not compile":
    let row = mkRow([("isNum", OidBool, 0'i16)], @[txt("t")])
    check not compiles(row.to(Variant))

suite "to(Row, T) target kinds":
  test "ref object":
    let row =
      mkRow([("id", OidInt4, 0'i16), ("name", OidText, 0'i16)], @[txt("3"), txt("Eve")])
    let u = row.to(RefUser)
    check u != nil
    check u.id == 3
    check u.name == "Eve"

  test "inherited fields are mapped":
    let row =
      mkRow([("name", OidText, 0'i16), ("id", OidInt4, 0'i16)], @[txt("kid"), txt("4")])
    let c = row.to(ChildRec)
    check c.id == 4
    check c.name == "kid"

  test "named tuple maps by name":
    let row =
      mkRow([("name", OidText, 0'i16), ("id", OidInt4, 0'i16)], @[txt("Fay"), txt("5")])
    let t = row.to(tuple[id: int32, name: string])
    check t == (id: 5'i32, name: "Fay")

  test "anonymous tuple maps by position":
    let row = mkRow(
      [("a", OidInt4, 0'i16), ("b", OidText, 0'i16), ("c", OidText, 0'i16)],
      @[txt("6"), txt("Gus"), none(seq[byte])],
    )
    check row.to((int32, string, Option[string])) == (6'i32, "Gus", none(string))

  test "anonymous tuple works without field metadata":
    let row: Row = @[txt("6"), txt("Gus")]
    check row.to((int32, string)) == (6'i32, "Gus")

  test "anonymous tuple needs one field per column":
    let row =
      mkRow([("a", OidInt4, 0'i16), ("b", OidText, 0'i16)], @[txt("6"), txt("x")])
    try:
      discard row.to((int32,))
      fail()
    except PgTypeError as e:
      check "(int32,): row has 2 columns but tuple has 1 fields" in e.msg

  test "anonymous tuple decode failure names the tuple":
    let row = mkRow([("a", OidInt4, 0'i16)], @[none(seq[byte])])
    try:
      discard row.to((int32,))
      fail()
    except PgTypeError as e:
      check e.msg.startsWith("(int32,): ")

suite "to(QueryResult, seq[T])":
  test "maps every row":
    let cols =
      [("id", OidInt4, 0'i16), ("name", OidText, 0'i16), ("email", OidText, 0'i16)]
    let rd = mkData(
      cols, [@[txt("1"), txt("a"), none(seq[byte])], @[txt("2"), txt("b"), txt("b@x")]]
    )
    let qr = QueryResult(fields: rd.fields, data: rd, rowCount: 2)
    let users = qr.to(seq[User])
    check users.len == 2
    check users[0] == User(id: 1, name: "a", email: none(string))
    check users[1] == User(id: 2, name: "b", email: some("b@x"))

  test "uses the caller's get overload":
    type Priced = object
      price: Cents

    let rd = mkData([("price", OidNumeric, 0'i16)], [@[txt("1.25")], @[txt("2")]])
    let qr = QueryResult(fields: rd.fields, data: rd, rowCount: 2)
    check qr.to(seq[Priced]) ==
      @[Priced(price: Cents(value: 125)), Priced(price: Cents(value: 200))]

  test "a statement with no columns raises":
    try:
      discard QueryResult(commandTag: "UPDATE 1").to(seq[User])
      fail()
    except PgTypeError as e:
      check e.msg.startsWith("User: the statement returns no columns")

  test "a result with no rows still checks the columns":
    let rd = mkData([("id", OidInt4, 0'i16)], [])
    let qr = QueryResult(fields: rd.fields, data: rd, rowCount: 0)
    try:
      discard qr.to(seq[User])
      fail()
    except PgTypeError as e:
      check "User.name: no column matches" in e.msg

  test "a result with no rows still checks a tuple's length":
    let rd = mkData([("id", OidInt4, 0'i16)], [])
    let qr = QueryResult(fields: rd.fields, data: rd, rowCount: 0)
    try:
      discard qr.to(seq[(int32, string)])
      fail()
    except PgTypeError as e:
      check "(int32, string): row has 1 columns but tuple has 2 fields" in e.msg

  test "a result of zero columns raises as one with no columns":
    let empty: seq[Option[seq[byte]]] = @[]
    let rd = mkData([], [empty, empty])
    let qr = QueryResult(data: rd, rowCount: 2)
    try:
      discard qr.to(seq[User])
      fail()
    except PgTypeError as e:
      check e.msg.startsWith("User: the statement returns no columns")

  test "a result with no rows maps an empty seq":
    let rd = mkData([("id", OidInt4, 0'i16)], [])
    let qr = QueryResult(fields: rd.fields, data: rd, rowCount: 0)
    check qr.to(seq[tuple[id: int32]]).len == 0

  test "unreadable and case types do not compile through QueryResult":
    let rd = mkData([("id", OidInt4, 0'i16)], [@[txt("1")]])
    let qr = QueryResult(fields: rd.fields, data: rd, rowCount: 1)
    check not compiles(qr.to(seq[Unreadable]))
    check not compiles(qr.to(seq[Variant]))
