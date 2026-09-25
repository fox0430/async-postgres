import std/[unittest, options, strutils, times, math, net, typetraits]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}

import types_common

type
  UsPostalCode = distinct string
  SmallCount = distinct int16
  PositiveInt = distinct int32
  ProbabilityF = distinct float64
  BigCount = distinct int64
  IsActive = distinct bool
  RatioF32 = distinct float32
  EventAt = distinct DateTime

proc `==`(a, b: UsPostalCode): bool {.borrow.}
proc `==`(a, b: SmallCount): bool {.borrow.}
proc `==`(a, b: PositiveInt): bool {.borrow.}
proc `==`(a, b: BigCount): bool {.borrow.}
proc `==`(a, b: IsActive): bool {.borrow.}
proc `==`(a, b: RatioF32): bool {.borrow.}
proc `$`(v: UsPostalCode): string {.borrow.}
proc `$`(v: SmallCount): string {.borrow.}
proc `$`(v: PositiveInt): string {.borrow.}

pgDomain(UsPostalCode, string)
pgDomain(SmallCount, int16)
pgDomain(PositiveInt, int32)
pgDomain(ProbabilityF, float64, 90001)
pgDomain(BigCount, int64)
pgDomain(IsActive, bool)
pgDomain(RatioF32, float32)
pgDomain(EventAt, DateTime)

suite "User-defined enum":
  test "pgEnum generates toPgParam with OID 0":
    let p = toPgParam(happy)
    check p.oid == 0'i32
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "happy"

  test "pgEnum with custom string values":
    let p = toPgParam(sad)
    check toString(p.value.get) == "sad"

  test "pgEnum with explicit OID":
    let p = toPgParam(red)
    check p.oid == 99999'i32
    check p.format == 0'i16
    check toString(p.value.get) == "red"

  test "pgEnum explicit OID all values":
    check toString(toPgParam(green).value.get) == "green"
    check toString(toPgParam(blue).value.get) == "blue"

  test "getEnum text format":
    let row: Row = @[some(toBytes("happy"))]
    check getEnum[Mood](row, 0) == happy

  test "getEnum all values":
    check getEnum[Mood](Row @[some(toBytes("sad"))], 0) == sad
    check getEnum[Mood](Row @[some(toBytes("ok"))], 0) == ok

  test "getEnum raises on invalid value":
    let row: Row = @[some(toBytes("unknown"))]
    var raised = false
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getEnum raises on NULL":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getEnumOpt some":
    let row: Row = @[some(toBytes("happy"))]
    check getEnumOpt[Mood](row, 0) == some(happy)

  test "getEnumOpt none":
    let row: Row = @[none(seq[byte])]
    check getEnumOpt[Mood](row, 0) == none(Mood)

  test "getEnum binary format":
    let row: Row = @[some(toBytes("sad"))]
    # Use a non-standard OID to simulate a user-defined enum type
    check getEnum[Mood](row, 0) == sad

  test "getEnum binary format falls back to text":
    let row: Row = @[some(toBytes("ok"))]
    check getEnum[Mood](row, 0) == ok

  test "getEnumOpt binary NULL":
    let fields = @[mkField(99999'i32, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getEnumOpt[Mood](row, 0) == none(Mood)

  test "getEnum binary NULL raises":
    let fields = @[mkField(99999'i32, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getEnum binary with unknown OID reads the label":
    # Dynamic enum OIDs must keep passing the guard; only well-known
    # built-in OIDs are rejected.
    let row = mkRow(@[some(toBytes("sad"))], @[mkField(99999'i32, 1'i16)])
    check getEnum[Mood](row, 0) == sad

  test "getEnum binary rejects bool colOid (label collision)":
    # Binary true (0x01) stringifies to "t" via getStr; without the OID guard
    # it would silently return YesNo.t instead of raising.
    let row = mkRow(@[some(@[1'u8])], @[mkField(OidBool, 1)])
    expect PgTypeError:
      discard getEnum[YesNo](row, 0)

  test "getEnum matches a label exactly, not case- or underscore-folded":
    # parseEnum normalizes, which would fold these three distinct PostgreSQL
    # labels onto one Nim value.
    type Step = enum
      inProgress = "inProgress"

    for label in ["in_progress", "INPROGRESS", "inprogress"]:
      let row = mkRow(@[some(toBytes(label))], @[mkField(OidText, 0)])
      expect PgTypeError:
        discard getEnum[Step](row, 0)
    let ok = mkRow(@[some(toBytes("inProgress"))], @[mkField(OidText, 0)])
    check getEnum[Step](ok, 0) == inProgress

  test "getEnum binary reads a character colOid":
    # A binary character payload is byte-identical to an enum label and getStr
    # raw-copies it, so `SELECT status::text` reads back as the enum.
    for oid in [OidText, OidVarchar, OidBpchar]:
      let row = mkRow(@[some(toBytes("happy"))], @[mkField(oid, 1)])
      check getEnum[Mood](row, 0) == happy

  test "getEnum binary rejects explicit OID 0":
    let row = mkRow(@[some(toBytes("happy"))], @[mkField(0'i32, 1'i16)])
    var msg = ""
    try:
      discard getEnum[Mood](row, 0)
    except PgTypeError as e:
      msg = e.msg
    check "unknown" in msg
    check "colOid=0" in msg

  test "Option[Enum] toPgParam some":
    let p = toPgParam(some(happy))
    check p.oid == 0'i32
    check p.format == 0'i16
    check toString(p.value.get) == "happy"

  test "Option[Enum] toPgParam none":
    let p = toPgParam(none(Mood))
    check p.oid == 0'i32
    check p.value.isNone

  test "roundtrip text":
    let orig = ok
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    check getEnum[Mood](row, 0) == orig

  test "roundtrip binary":
    let orig = happy
    let p = toPgParam(orig)
    let fields = @[mkField(99999'i32, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getEnum[Mood](row, 0) == orig

suite "Composite text parser":
  test "parseCompositeText simple":
    let parts = parseCompositeText("(1,2,3)")
    check parts.len == 3
    check parts[0] == some("1")
    check parts[1] == some("2")
    check parts[2] == some("3")

  test "parseCompositeText with NULL":
    let parts = parseCompositeText("(hello,,world)")
    check parts.len == 3
    check parts[0] == some("hello")
    check parts[1] == none(string)
    check parts[2] == some("world")

  test "parseCompositeText quoted":
    let parts = parseCompositeText("(\"hello, world\",42)")
    check parts.len == 2
    check parts[0] == some("hello, world")
    check parts[1] == some("42")

  test "parseCompositeText quoted with escaped quote":
    let parts = parseCompositeText("(\"say \"\"hi\"\"\",done)")
    check parts.len == 2
    check parts[0] == some("say \"hi\"")
    check parts[1] == some("done")

  test "parseCompositeText quoted with backslash escapes":
    # Canonical record_out doubles ``"`` and ``\\``; record_in (and this
    # parser) also accept backslash-escaped bytes inside quotes.
    let doubled = parseCompositeText("(\"a\\\\b\",c)")
    check doubled.len == 2
    check doubled[0] == some("a\\b")
    check doubled[1] == some("c")
    let escaped = parseCompositeText("(\"a\\\"b\",c)")
    check escaped.len == 2
    check escaped[0] == some("a\"b")
    check escaped[1] == some("c")
    let trailing = parseCompositeText("(\"a\\\\\",c)")
    check trailing.len == 2
    check trailing[0] == some("a\\")
    check trailing[1] == some("c")

  test "parseCompositeText backslash escaping closing quote raises":
    # record_in fails with "Unexpected end of input" for the same input.
    expect PgTypeError:
      discard parseCompositeText("(\"a\\\")")

  test "parseCompositeText empty string quoted":
    let parts = parseCompositeText("(\"\",42)")
    check parts.len == 2
    check parts[0] == some("")
    check parts[1] == some("42")

  test "parseCompositeText () is one NULL field":
    # PostgreSQL emits `()` for a 1-field composite whose sole field is NULL.
    let parts = parseCompositeText("()")
    check parts.len == 1
    check parts[0] == none(string)

  test "parseCompositeText single NULL":
    let parts = parseCompositeText("(,)")
    check parts.len == 2
    check parts[0] == none(string)
    check parts[1] == none(string)

  test "parseCompositeText invalid raises":
    var raised = false
    try:
      discard parseCompositeText("not a composite")
    except PgTypeError:
      raised = true
    check raised

  test "parseCompositeText unterminated quoted field raises":
    expect PgTypeError:
      discard parseCompositeText("(\"abc)")

  test "parseCompositeText garbage after closing quote raises":
    expect PgTypeError:
      discard parseCompositeText("(\"a\"b)")

  test "parseCompositeText quote in unquoted field raises":
    expect PgTypeError:
      discard parseCompositeText("(a\"b,c)")

  test "parseCompositeText backslash in unquoted field raises":
    # record_in treats ``\x`` in an unquoted field as an escape, but record_out
    # never emits it (such bytes are quoted), so non-canonical input is
    # rejected instead of decoded.
    expect PgTypeError:
      discard parseCompositeText("(a\\b,c)")

  test "parseCompositeText paren in unquoted field raises":
    expect PgTypeError:
      discard parseCompositeText("(a(b,c)")
    expect PgTypeError:
      discard parseCompositeText("(a)b,c)")

  test "encodeCompositeText simple":
    let s = encodeCompositeText(@[some("1"), some("2")])
    check s == "(1,2)"

  test "encodeCompositeText with NULL":
    let s = encodeCompositeText(@[some("hello"), none(string), some("world")])
    check s == "(hello,,world)"

  test "encodeCompositeText quoting":
    let s = encodeCompositeText(@[some("hello, world"), some("42")])
    check s == "(\"hello, world\",42)"

  test "encodeCompositeText empty string quoted":
    let s = encodeCompositeText(@[some(""), some("42")])
    check s == "(\"\",42)"

  test "encodeCompositeText literal NULL string quoted":
    # An unquoted NULL token (any case) would be read back as a SQL NULL by
    # PostgreSQL's record input, so the literal string must be quoted.
    check encodeCompositeText(@[some("NULL"), some("42")]) == "(\"NULL\",42)"
    check encodeCompositeText(@[some("null")]) == "(\"null\")"
    check encodeCompositeText(@[some("Null")]) == "(\"Null\")"

  test "encodeCompositeText doubles backslash and quote like record_out":
    # Canonical record_out output doubles both bytes; the server's record_in
    # (and parseCompositeText) decode this form back to the original value.
    check encodeCompositeText(@[some("a\\b")]) == "(\"a\\\\b\")"
    check encodeCompositeText(@[some("q\"w")]) == "(\"q\"\"w\")"

  test "roundtrip text encode/parse with backslash and quote":
    let fields = @[some("a\\b"), some("q\"w"), some("a\\"), some("\\\"")]
    check parseCompositeText(encodeCompositeText(fields)) == fields

  test "roundtrip text encode/parse":
    let fields = @[some("hello world"), some("42"), none(string), some("with,comma")]
    let encoded = encodeCompositeText(fields)
    let decoded = parseCompositeText(encoded)
    check decoded.len == 4
    check decoded[0] == some("hello world")
    check decoded[1] == some("42")
    check decoded[2] == none(string)
    check decoded[3] == some("with,comma")

suite "Composite binary encoder/decoder":
  test "encodeBinaryComposite":
    let fields = @[
      (oid: OidInt4, data: some(@(toBE32(42'i32)))),
      (oid: OidText, data: some(toBytes("hello"))),
    ]
    let data = encodeBinaryComposite(fields)
    # numFields = 2
    check fromBE32(data[0 .. 3]) == 2'i32
    # field 0: oid=OidInt4, len=4, value=42
    check fromBE32(data[4 .. 7]) == OidInt4
    check fromBE32(data[8 .. 11]) == 4'i32
    check fromBE32(data[12 .. 15]) == 42'i32
    # field 1: oid=OidText, len=5, value="hello"
    check fromBE32(data[16 .. 19]) == OidText
    check fromBE32(data[20 .. 23]) == 5'i32
    check toString(data[24 .. 28]) == "hello"

  test "encodeBinaryComposite with NULL":
    let fields = @[
      (oid: OidInt4, data: some(@(toBE32(1'i32)))),
      (oid: OidText, data: none(seq[byte])),
    ]
    let data = encodeBinaryComposite(fields)
    check fromBE32(data[0 .. 3]) == 2'i32
    # field 1: NULL (len = -1)
    check fromBE32(data[16 .. 19]) == OidText
    check fromBE32(data[20 .. 23]) == -1'i32

  test "decodeBinaryComposite":
    let fields = @[
      (oid: OidInt4, data: some(@(toBE32(99'i32)))),
      (oid: OidText, data: some(toBytes("abc"))),
    ]
    let data = encodeBinaryComposite(fields)
    let decoded = decodeBinaryComposite(data)
    check decoded.len == 2
    check decoded[0].oid == OidInt4
    check decoded[0].len == 4
    let f0 = 0 + decoded[0].off
    check fromBE32(data[f0 .. f0 + 3]) == 99'i32
    check decoded[1].oid == OidText
    check decoded[1].len == 3

  test "decodeBinaryComposite with NULL":
    let fields = @[(oid: OidInt4, data: none(seq[byte]))]
    let data = encodeBinaryComposite(fields)
    let decoded = decodeBinaryComposite(data)
    check decoded.len == 1
    check decoded[0].oid == OidInt4
    check decoded[0].len == -1

  test "decodeBinaryComposite empty":
    let data = encodeBinaryComposite(@[])
    let decoded = decodeBinaryComposite(data)
    check decoded.len == 0

suite "User-defined composite":
  test "pgComposite generates toPgParam with OID 0":
    let p = toPgParam(PointRecord(x: 1.5, y: 2.5))
    check p.oid == 0'i32
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "(1.5,2.5)"

  test "pgComposite with explicit OID":
    let p = toPgParam(PersonRecord(name: "Alice", age: 30, score: 95.5))
    check p.oid == 50000'i32
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "(Alice,30,95.5)"

  test "getComposite text format":
    let row: Row = @[some(toBytes("(3.14,2.72)"))]
    let pt = getComposite[PointRecord](row, 0)
    check abs(pt.x - 3.14) < 1e-10
    check abs(pt.y - 2.72) < 1e-10

  test "getComposite text format with string":
    let row: Row = @[some(toBytes("(Bob,25,88.5)"))]
    let p = getComposite[PersonRecord](row, 0)
    check p.name == "Bob"
    check p.age == 25
    check abs(p.score - 88.5) < 1e-10

  test "getComposite text format quoted string":
    let row: Row = @[some(toBytes("(\"Alice, Jr.\",30,95.5)"))]
    let p = getComposite[PersonRecord](row, 0)
    check p.name == "Alice, Jr."
    check p.age == 30

  test "getComposite raises on NULL column":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard getComposite[PointRecord](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getCompositeOpt some":
    let row: Row = @[some(toBytes("(1.0,2.0)"))]
    let opt = getCompositeOpt[PointRecord](row, 0)
    check opt.isSome
    check abs(opt.get.x - 1.0) < 1e-10
    check abs(opt.get.y - 2.0) < 1e-10

  test "getCompositeOpt none":
    let row: Row = @[none(seq[byte])]
    check getCompositeOpt[PointRecord](row, 0) == none(PointRecord)

  test "getComposite binary format":
    # Build binary composite: 2 fields, both float8
    let fields_data = @[
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](3.14'f64))))),
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](2.72'f64))))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let pt = getComposite[PointRecord](row, 0)
    check abs(pt.x - 3.14) < 1e-10
    check abs(pt.y - 2.72) < 1e-10

  test "getComposite binary text fallback":
    let row: Row = @[some(toBytes("(5.0,6.0)"))]
    let pt = getComposite[PointRecord](row, 0)
    check abs(pt.x - 5.0) < 1e-10
    check abs(pt.y - 6.0) < 1e-10

  test "getCompositeOpt binary NULL":
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getCompositeOpt[PointRecord](row, 0) == none(PointRecord)

  test "roundtrip text":
    let orig = PointRecord(x: 1.5, y: -3.7)
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = getComposite[PointRecord](row, 0)
    check abs(decoded.x - orig.x) < 1e-10
    check abs(decoded.y - orig.y) < 1e-10

  test "roundtrip text PersonRecord":
    let orig = PersonRecord(name: "Charlie", age: 42, score: 99.9)
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = getComposite[PersonRecord](row, 0)
    check decoded.name == orig.name
    check decoded.age == orig.age
    check abs(decoded.score - orig.score) < 1e-10

  test "Option field toPgParam with values":
    let r = NullableRecord(name: "Alice", age: some(30'i32), note: some("hi"))
    let p = toPgParam(r)
    check toString(p.value.get) == "(Alice,30,hi)"

  test "Option field toPgParam with none":
    let r = NullableRecord(name: "Bob", age: none(int32), note: none(string))
    let p = toPgParam(r)
    check toString(p.value.get) == "(Bob,,)"

  test "getComposite text with Option fields some":
    let row: Row = @[some(toBytes("(Carol,25,hello)"))]
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Carol"
    check r.age == some(25'i32)
    check r.note == some("hello")

  test "getComposite text with Option fields none":
    let row: Row = @[some(toBytes("(Dave,,)"))]
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Dave"
    check r.age == none(int32)
    check r.note == none(string)

  test "getComposite binary with Option fields none":
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Eve"))),
      (oid: OidInt4, data: none(seq[byte])),
      (oid: OidText, data: none(seq[byte])),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(0'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Eve"
    check r.age == none(int32)
    check r.note == none(string)

  test "getComposite binary with Option fields some":
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Fay"))),
      (oid: OidInt4, data: some(@(toBE32(99'i32)))),
      (oid: OidText, data: some(toBytes("note"))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(0'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = getComposite[NullableRecord](row, 0)
    check r.name == "Fay"
    check r.age == some(99'i32)
    check r.note == some("note")

  test "Option field roundtrip":
    let orig = NullableRecord(name: "Test", age: some(42'i32), note: none(string))
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    let decoded = getComposite[NullableRecord](row, 0)
    check decoded.name == orig.name
    check decoded.age == orig.age
    check decoded.note == orig.note

  test "Option[Composite] toPgParam some":
    let p = toPgParam(some(PointRecord(x: 1.0, y: 2.0)))
    check p.oid == 0'i32
    check p.format == 0'i16
    check p.value.isSome

  test "Option[Composite] toPgParam none":
    let p = toPgParam(none(PointRecord))
    check p.oid == 0'i32
    check p.value.isNone

  test "getComposite binary wider wire int than Nim field raises":
    # Nim field is int32 but wire sends int8 (bigint, 8 bytes). Without a
    # length check, fromBE32 would silently keep only the top 4 bytes.
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Grace"))),
      (oid: OidInt8, data: some(@(toBE64(1'i64)))),
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](1.0'f64))))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[PersonRecord](row, 0)

  test "getComposite binary narrower wire int than Nim field raises":
    # Nim field is int64 but wire sends int4 (4 bytes). Without a length
    # check, fromBE64 would run past the field and IndexDefect.
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Heidi"))),
      (oid: OidInt4, data: some(@(toBE32(7'i32)))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(0'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[WideIntRecord](row, 0)

  test "getComposite binary wider wire float than Nim field raises":
    # Nim field is float64 but wire sends float4 (4 bytes) — narrower than
    # expected 8, so fromBE64 would IndexDefect.
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Ivan"))),
      (oid: OidInt4, data: some(@(toBE32(10'i32)))),
      (oid: OidFloat4, data: some(@(toBE32(cast[int32](1.5'f32))))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[PersonRecord](row, 0)

  test "getComposite binary Option field width mismatch raises":
    # Same guard applies inside the Option branch: age is Option[int32],
    # wire sends 8-byte int8 → must raise, not silently truncate.
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Judy"))),
      (oid: OidInt8, data: some(@(toBE64(99'i64)))),
      (oid: OidText, data: some(toBytes("n"))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(0'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[NullableRecord](row, 0)

  test "getComposite binary int32 field with float4 wire OID raises":
    # int4 and float4 share 4 bytes; only OID separates them. Without the
    # OID check the raw bit pattern of 1.5f would decode as int32 garbage.
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Kate"))),
      (oid: OidFloat4, data: some(@(toBE32(cast[int32](1.5'f32))))),
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](1.0'f64))))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[PersonRecord](row, 0)

  test "getComposite binary int64 field with timestamp wire OID raises":
    # int8 and timestamp share 8 bytes; only OID separates them.
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Leo"))),
      (oid: OidTimestamp, data: some(@(toBE64(1'i64)))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(0'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[WideIntRecord](row, 0)

  test "getComposite binary float64 field with int8 wire OID raises":
    let fields_data = @[
      (oid: OidText, data: some(toBytes("Mia"))),
      (oid: OidInt4, data: some(@(toBE32(1'i32)))),
      (oid: OidInt8, data: some(@(toBE64(42'i64)))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[PersonRecord](row, 0)

  test "getComposite binary extra wire fields raises":
    # Wire has 3 fields but PointRecord has 2 — previously silent-truncated.
    let fields_data = @[
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](1.0'f64))))),
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](2.0'f64))))),
      (oid: OidFloat8, data: some(@(toBE64(cast[int64](3.0'f64))))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    expect PgTypeError:
      discard getComposite[PointRecord](row, 0)

  test "getComposite text extra wire fields raises":
    let row: Row = @[some(toBytes("(1.0,2.0,3.0)"))]
    expect PgTypeError:
      discard getComposite[PointRecord](row, 0)

  test "getComposite binary OID 0 accepts width-matching payload":
    # OID 0 = server didn't disclose type; length guard still applies.
    let fields_data = @[
      (oid: 0'i32, data: some(@(toBE64(cast[int64](3.14'f64))))),
      (oid: 0'i32, data: some(@(toBE64(cast[int64](2.72'f64))))),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let pt = getComposite[PointRecord](row, 0)
    check abs(pt.x - 3.14) < 1e-10
    check abs(pt.y - 2.72) < 1e-10

  test "pgComposite DateTime text round-trip":
    let dt = dateTime(2024, mMar, 15, 10, 30, 0, 123456000, utc())
    let p = toPgParam(TimestampRecord(label: "evt", at: dt))
    check p.format == 0'i16
    let encoded = toString(p.value.get)
    # The offset must be kept: a zoneless literal would be reinterpreted in the
    # session TimeZone when the server-side field is timestamptz.
    check "2024-03-15 10:30:00.123456Z" in encoded
    let row: Row = @[some(p.value.get)]
    let got = getComposite[TimestampRecord](row, 0)
    check got.label == "evt"
    check got.at == dt

  test "pgComposite rejects an uninitialized DateTime field":
    expect PgTypeError:
      discard toPgParam(TimestampRecord(label: "evt"))

  test "getComposite DateTime binary format":
    let dt = dateTime(2024, mMar, 15, 10, 30, 0, 0, utc())
    let tsBytes = toPgBinaryParam(dt).value.get
    let fields_data = @[
      (oid: OidText, data: some(toBytes("evt"))),
      (oid: OidTimestamp, data: some(tsBytes)),
    ]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let got = getComposite[TimestampRecord](row, 0)
    check got.label == "evt"
    check got.at == dt

  test "getComposite binary name OID accepted for string field":
    let fields_data = @[(oid: OidName, data: some(toBytes("alice")))]
    let data = encodeBinaryComposite(fields_data)
    let fields = @[mkField(50000'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let got = getComposite[NameFieldRecord](row, 0)
    check got.name == "alice"

suite "User-defined domain":
  test "pgDomain generates toPgParam with base type OID":
    let p = toPgParam(UsPostalCode("12345"))
    check p.oid == OidText
    check p.format == 0'i16
    check p.value.isSome
    check toString(p.value.get) == "12345"

  test "pgDomain int16 base type":
    let p = toPgParam(SmallCount(7))
    check p.oid == OidInt2
    check p.format == 1'i16
    check p.value.isSome

  test "pgDomain int32 base type":
    let p = toPgParam(PositiveInt(42))
    check p.oid == OidInt4
    check p.format == 1'i16 # binary inherited from int32
    check p.value.isSome

  test "pgDomain with explicit OID":
    let p = toPgParam(ProbabilityF(0.95))
    check p.oid == 90001'i32
    check p.value.isSome

  test "getDomain text format string":
    let row: Row = @[some(toBytes("12345"))]
    check getDomain[UsPostalCode](row, 0) == UsPostalCode("12345")

  test "getDomain text format int16":
    let row: Row = @[some(toBytes("7"))]
    check getDomain[SmallCount](row, 0) == SmallCount(7)

  test "getDomain text format int16 out of range raises PgTypeError":
    # int32-legal but int16-overflowing text must raise a catchable PgTypeError,
    # not silently wrap (release) or escape as a RangeDefect (debug).
    let row: Row = @[some(toBytes("40000"))]
    expect PgTypeError:
      discard getDomain[SmallCount](row, 0)

  test "getDomain text format int32":
    let row: Row = @[some(toBytes("42"))]
    check getDomain[PositiveInt](row, 0) == PositiveInt(42)

  test "getDomain text format int64":
    let row: Row = @[some(toBytes("1000000000"))]
    check getDomain[BigCount](row, 0) == BigCount(1000000000'i64)

  test "getDomain text format float64":
    let row: Row = @[some(toBytes("0.95"))]
    let v = getDomain[ProbabilityF](row, 0)
    check abs(float64(v) - 0.95) < 1e-10

  test "getDomain text format bool":
    let rowT: Row = @[some(toBytes("t"))]
    check getDomain[IsActive](rowT, 0) == IsActive(true)
    let rowF: Row = @[some(toBytes("f"))]
    check getDomain[IsActive](rowF, 0) == IsActive(false)

  test "getDomain text format float32":
    let row: Row = @[some(toBytes("0.5"))]
    check getDomain[RatioF32](row, 0) == RatioF32(0.5'f32)

  test "getDomain text format DateTime":
    let row: Row = @[some(toBytes("2024-01-15 10:30:00.000000"))]
    let got = getDomain[EventAt](row, 0)
    check DateTime(got).year == 2024
    check DateTime(got).month == mJan
    check DateTime(got).monthday == 15

  test "getDomain binary format int16":
    let fields = @[mkField(OidInt2, 1'i16)]
    let row = mkRow(@[some(@(toBE16(3'i16)))], fields)
    check getDomain[SmallCount](row, 0) == SmallCount(3)

  test "getDomain binary format int32":
    let fields = @[mkField(OidInt4, 1'i16)]
    let row = mkRow(@[some(@(toBE32(99'i32)))], fields)
    check getDomain[PositiveInt](row, 0) == PositiveInt(99)

  test "getDomain binary format int64":
    let fields = @[mkField(OidInt8, 1'i16)]
    let row = mkRow(@[some(@(toBE64(1000000000'i64)))], fields)
    check getDomain[BigCount](row, 0) == BigCount(1000000000'i64)

  test "getDomain binary format float64":
    let fields = @[mkField(OidFloat8, 1'i16)]
    let row = mkRow(@[some(@(toBE64(cast[int64](3.14'f64))))], fields)
    let v = getDomain[ProbabilityF](row, 0)
    check abs(float64(v) - 3.14) < 1e-10

  test "getDomain binary format bool":
    let fields = @[mkField(OidBool, 1'i16)]
    let rowT = mkRow(@[some(@[1'u8])], fields)
    check getDomain[IsActive](rowT, 0) == IsActive(true)
    let rowF = mkRow(@[some(@[0'u8])], fields)
    check getDomain[IsActive](rowF, 0) == IsActive(false)

  test "getDomain raises on NULL":
    let row: Row = @[none(seq[byte])]
    var raised = false
    try:
      discard getDomain[UsPostalCode](row, 0)
    except PgTypeError:
      raised = true
    check raised

  test "getDomainOpt some":
    let row: Row = @[some(toBytes("12345"))]
    check getDomainOpt[UsPostalCode](row, 0) == some(UsPostalCode("12345"))

  test "getDomainOpt none":
    let row: Row = @[none(seq[byte])]
    check getDomainOpt[UsPostalCode](row, 0) == none(UsPostalCode)

  test "getDomainOpt binary NULL":
    let fields = @[mkField(OidInt4, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check getDomainOpt[PositiveInt](row, 0) == none(PositiveInt)

  test "Option[Domain] toPgParam some":
    let p = toPgParam(some(UsPostalCode("12345")))
    check p.oid == OidText
    check p.value.isSome
    check toString(p.value.get) == "12345"

  test "Option[Domain] toPgParam none":
    let p = toPgParam(none(UsPostalCode))
    check p.oid == OidText
    check p.value.isNone

  test "roundtrip text string":
    let orig = UsPostalCode("90210")
    let p = toPgParam(orig)
    let row: Row = @[p.value]
    check getDomain[UsPostalCode](row, 0) == orig

  test "roundtrip binary int16":
    let orig = SmallCount(5)
    let p = toPgParam(orig)
    let fields = @[mkField(OidInt2, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[SmallCount](row, 0) == orig

  test "roundtrip binary int32":
    let orig = PositiveInt(7)
    let p = toPgParam(orig)
    let fields = @[mkField(OidInt4, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[PositiveInt](row, 0) == orig

  test "pgDomain int64 base type":
    let p = toPgParam(BigCount(999999'i64))
    check p.oid == OidInt8
    check p.format == 1'i16
    check p.value.isSome

  test "pgDomain bool base type":
    let p = toPgParam(IsActive(true))
    check p.oid == OidBool
    check p.format == 1'i16
    check p.value.isSome

  test "roundtrip binary int64":
    let orig = BigCount(123456789'i64)
    let p = toPgParam(orig)
    let fields = @[mkField(OidInt8, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[BigCount](row, 0) == orig

  test "roundtrip binary bool":
    let orig = IsActive(true)
    let p = toPgParam(orig)
    let fields = @[mkField(OidBool, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check getDomain[IsActive](row, 0) == orig
