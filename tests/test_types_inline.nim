import std/[json, unittest, options, tables, math, net, typetraits, importutils]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}
import ../async_postgres/pg_client
import ../async_postgres/pg_client/core {.all.}
import ../async_postgres/pg_client/pipeline {.all.}

proc inlinePayload(p: PgParamInline): seq[byte] =
  ## Reconstruct the binary payload a PgParamInline would emit on the wire,
  ## picking between `inlineBuf` and `overflow` based on `len`.
  if p.len == -1:
    return @[]
  if p.len == 0:
    return @[]
  if p.len <= PgInlineBufSize:
    result = newSeq[byte](p.len)
    copyMem(addr result[0], addr p.inlineBuf[0], p.len)
  else:
    result = p.overflow

suite "toPgParamInline":
  test "int16 encodes as binary BE and fits inline":
    let p = toPgParamInline(42'i16)
    check p.oid == OidInt2
    check p.format == 1
    check p.len == 2
    check inlinePayload(p) == @(toBE16(42'i16))

  test "int16 negative":
    let p = toPgParamInline(-1'i16)
    check inlinePayload(p) == @(toBE16(-1'i16))

  test "int32 wire-format matches toPgParam":
    let old = toPgParam(123456'i32)
    let new = toPgParamInline(123456'i32)
    check new.oid == old.oid
    check new.format == old.format
    check inlinePayload(new) == old.value.get

  test "int32 boundaries":
    for v in [0'i32, 1, -1, int32.high, int32.low]:
      let p = toPgParamInline(v)
      check p.len == 4
      check inlinePayload(p) == @(toBE32(v))

  test "int64 fits inline at 8 bytes":
    let p = toPgParamInline(9_999_999_999'i64)
    check p.oid == OidInt8
    check p.len == 8
    check inlinePayload(p) == @(toBE64(9_999_999_999'i64))

  test "int widens to int64":
    let p = toPgParamInline(42.int)
    check p.oid == OidInt8
    check p.len == 8
    check inlinePayload(p) == @(toBE64(42'i64))

  test "float32 wire-format":
    let p = toPgParamInline(3.14'f32)
    check p.oid == OidFloat4
    check p.len == 4
    # Match toPgParam exactly (same cast-to-int32 + BE encoding).
    let old = toPgParam(3.14'f32)
    check inlinePayload(p) == old.value.get

  test "float64 wire-format":
    let p = toPgParamInline(3.14)
    check p.oid == OidFloat8
    check p.len == 8
    let bits = fromBE64(inlinePayload(p))
    check abs(cast[float64](bits) - 3.14) < 1e-10

  test "bool true / false":
    let pt = toPgParamInline(true)
    check pt.oid == OidBool
    check pt.len == 1
    check pt.inlineBuf[0] == 1'u8
    let pf = toPgParamInline(false)
    check pf.inlineBuf[0] == 0'u8

  test "string short fits inline":
    let p = toPgParamInline("hello")
    check p.oid == OidText
    check p.format == 0
    check p.len == 5
    check p.overflow.len == 0
    check inlinePayload(p) == @(toBytes("hello"))

  test "string empty":
    let p = toPgParamInline("")
    check p.len == 0
    check inlinePayload(p).len == 0

  test "string at inline boundary (16 bytes)":
    let s = "0123456789ABCDEF" # exactly 16 chars
    let p = toPgParamInline(s)
    check p.len == 16
    check p.overflow.len == 0
    check inlinePayload(p) == @(toBytes(s))

  test "string above boundary uses overflow":
    let s = "0123456789ABCDEFG" # 17 chars
    let p = toPgParamInline(s)
    check p.len == 17
    check p.overflow.len == 17
    check inlinePayload(p) == @(toBytes(s))

  test "seq[byte] short fits inline":
    let b = @[byte 1, 2, 3, 4]
    let p = toPgParamInline(b)
    check p.oid == OidBytea
    check p.len == 4
    check p.overflow.len == 0
    check inlinePayload(p) == b

  test "seq[byte] above boundary uses overflow":
    var b = newSeq[byte](32)
    for i in 0 ..< 32:
      b[i] = byte(i)
    let p = toPgParamInline(b)
    check p.len == 32
    check p.overflow.len == 32
    check inlinePayload(p) == b

  test "PgUuid matches toPgParam (OidUuid, text, overflow)":
    # UUID string is 36 chars, exceeds PgInlineBufSize.
    let u = PgUuid("550e8400-e29b-41d4-a716-446655440000")
    let p = toPgParamInline(u)
    let old = toPgParam(u)
    check p.oid == OidUuid
    check p.oid == old.oid
    check p.format == old.format
    check p.len == 36
    check p.overflow.len == 36
    check inlinePayload(p) == old.value.get
    check inlinePayload(p) == @(toBytes(string(u)))

  test "PgMoney encodes int64 amount only":
    let m = initPgMoney(12345'i64, 2'i8)
    let p = toPgParamInline(m)
    check p.oid == OidMoney
    check p.len == 8
    check inlinePayload(p) == @(toBE64(12345'i64))

  test "Option[int32] some delegates to int32":
    let p = toPgParamInline(some(42'i32))
    check p.oid == OidInt4
    check p.len == 4
    check inlinePayload(p) == @(toBE32(42'i32))

  test "Option[int32] none marks NULL":
    let p = toPgParamInline(none(int32))
    check p.oid == OidInt4
    check p.format == 1
    check p.len == -1

  test "Option[string] none marks NULL with text oid":
    let p = toPgParamInline(none(string))
    check p.oid == OidText
    check p.format == 0
    check p.len == -1

  test "string length guard raises PgTypeError":
    # PgTypeError is a PgError subtype, so `except PgError` covers this too.
    check int64(maxInt32Len) == int64(int32.high)
    when sizeof(int) >= 8:
      # maxInt32Len is high(int) on a 32-bit target, so `+ 1` would overflow.
      expect PgTypeError:
        checkPgBinLen(maxInt32Len + 1, "string")
      # Through the encoder: a helper-only check stays green if the guard is
      # dropped from toPgParamInline, leaving a RangeDefect on int32(v.len).
      expect PgTypeError:
        discard toPgParamInline(newString(int(int32.high) + 1))
    check toPgParamInline("a").len == 1

  test "seq[byte] length guard raises PgTypeError":
    when sizeof(int) >= 8:
      expect PgTypeError:
        checkPgBinLen(maxInt32Len + 1, "bytea")
      expect PgTypeError:
        discard toPgParamInline(newSeq[byte](int(int32.high) + 1))
    check toPgParamInline(@[1'u8, 2]).len == 2

  test "toPgParam string and seq[byte] share the inline length guard":
    # The guard fires before addBind's own PgTypeError, with the value's label.
    check toPgParam("a").value.get.len == 1
    check toPgParam(@[1'u8, 2]).value.get.len == 2

  test "unbounded text encoders reject oversized payloads with PgTypeError":
    # One boundary pair pins the helper, which branches on the length alone.
    # The shared funnels are then proven end to end with a single oversized
    # value each (`textParam` via `toPgParam`, direct `checkPgBinLen` via
    # `toPgBinaryParam`); every encoder below reaches one of those two funnels,
    # so removing either guard fails here. The small-value paths pin that each
    # encoder routes into its funnel.
    checkPgBinLen(maxInt32Len, "xml")
    when sizeof(int) >= 8:
      expect PgTypeError:
        checkPgBinLen(maxInt32Len + 1, "xml")
      let huge = newString(maxInt32Len + 1)
      expect PgTypeError:
        discard toPgParam(PgXml(huge))
      expect PgTypeError:
        discard toPgBinaryParam(PgXml(huge))
    check toPgParam(PgXml("<a/>")).value.get.len == 4
    check toPgParam(PgTsVector("a")).value.get.len == 1
    check toPgParam(PgTsQuery("a")).value.get.len == 1
    check toPgParam(%*{"a": 1}).value.get.len > 0
    check toPgBinaryParam("a").value.get.len == 1
    check toPgBinaryParam(PgXml("a")).value.get.len == 1
    check toPgBinaryParam(@[1'u8, 2'u8]).value.get.len == 2

  test "hstore/json/varbit/path/polygon text encoders stay PgTypeError via textParam":
    # Every encoder here reaches the shared `textParam` funnel, whose rejection
    # is proven end to end in the sibling test above; the success paths below
    # pin that each encoder routes into that funnel with its label.
    check textParam(OidText, "a", "hstore").value.get.len == 1
    check toPgParam(PgHstore(initTable[string, Option[string]]())).value.isSome
    check toPgParam(initPgBit(1, @[0b10000000'u8])).value.get.len > 0
    check toPgParam(PgPath(closed: false, points: @[PgPoint(x: 0, y: 0)])).value.get.len >
      0
    check toPgParam(PgPolygon(points: @[PgPoint(x: 0, y: 0)])).value.get.len > 0
    check toPgParam(PgNumeric(weight: 0, sign: pgPositive, dscale: 0, digits: @[1'i16])).value.get.len >
      0
    check encodeJsonbBinary(%*{"a": 1}).len == 1 + ($(%*{"a": 1})).len

  test "cumulative payload guard boundary":
    # checkPgBinPayload backs json, arrays, ranges, hstore and the geometric
    # encoders; int64 input keeps the boundary reachable without allocating.
    checkPgBinPayload(int64(int32.high), "json (with version byte)")
    expect PgTypeError:
      checkPgBinPayload(int64(int32.high) + 1, "json (with version byte)")

suite "addBindRaw wire-format parity":
  test "single int32 param matches addBind":
    let old = toPgParam(42'i32)
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("p", "s", [int16(1)], [old.value], [])
    var rawBuf: seq[byte] = @[]
    let data = old.value.get
    let ranges = @[(off: int32(0), len: int32(data.len))]
    rawBuf.addBindRaw("p", "s", [int16(1)], data, ranges, [])
    check legacyBuf == rawBuf

  test "three params matches addBind":
    let a = toPgParam(1'i32)
    let b = toPgParam(2'i32)
    let c = toPgParam(3'i32)
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("", "stmt", [int16(1), 1, 1], [a.value, b.value, c.value], [])
    var data: seq[byte] = @[]
    var ranges: seq[tuple[off: int32, len: int32]] = @[]
    for v in [a.value.get, b.value.get, c.value.get]:
      ranges.add (off: int32(data.len), len: int32(v.len))
      data.add v
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw("", "stmt", [int16(1), 1, 1], data, ranges, [])
    check legacyBuf == rawBuf

  test "NULL param matches addBind":
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("", "s", [int16(1)], [none(seq[byte])], [])
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw("", "s", [int16(1)], @[], @[(off: int32(0), len: int32(-1))], [])
    check legacyBuf == rawBuf

  test "mixed NULL and non-NULL matches addBind":
    let a = toPgParam(7'i32)
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("", "s", [int16(1), 1], [a.value, none(seq[byte])], [])
    var data: seq[byte] = a.value.get
    var ranges =
      @[(off: int32(0), len: int32(data.len)), (off: int32(0), len: int32(-1))]
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw("", "s", [int16(1), 1], data, ranges, [])
    check legacyBuf == rawBuf

  test "result formats are preserved":
    var legacyBuf: seq[byte] = @[]
    legacyBuf.addBind("p", "", [int16(0)], [none(seq[byte])], [int16(1), 0, 1])
    var rawBuf: seq[byte] = @[]
    rawBuf.addBindRaw(
      "p", "", [int16(0)], @[], @[(off: int32(0), len: int32(-1))], [int16(1), 0, 1]
    )
    check legacyBuf == rawBuf

suite "addBindRaw range validation":
  test "range len below -1 raises PgTypeError":
    var buf: seq[byte] = @[]
    expect PgTypeError:
      buf.addBindRaw("", "", [int16(1)], @[], @[(off: int32(0), len: int32(-2))], [])

  test "negative off with non-zero len raises PgTypeError":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    expect PgTypeError:
      buf.addBindRaw("", "", [int16(1)], data, @[(off: int32(-1), len: int32(4))], [])

  test "off + len past paramData.len raises PgTypeError":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    expect PgTypeError:
      # off + len == 5, data.len == 4 — reads past end
      buf.addBindRaw("", "", [int16(1)], data, @[(off: int32(1), len: int32(4))], [])

  test "range exactly at end of paramData is valid":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    # off + len == 4, data.len == 4 — boundary is inclusive on the data side
    buf.addBindRaw("", "", [int16(1)], data, @[(off: int32(0), len: int32(4))], [])
    check buf.len > 0

  test "len == 0 with off at end of data is valid (empty string case)":
    var buf: seq[byte] = @[]
    let data = @[byte 1, 2, 3, 4]
    # Mirrors flattenInline's encoding for empty strings: off = data.len, len = 0
    buf.addBindRaw("", "", [int16(0)], data, @[(off: int32(4), len: int32(0))], [])
    check buf.len > 0

  test "NULL range with arbitrary off is valid (off ignored when len == -1)":
    var buf: seq[byte] = @[]
    buf.addBindRaw("", "", [int16(1)], @[], @[(off: int32(999), len: int32(-1))], [])
    check buf.len > 0

suite "parseAffectedRowsRaw":
  test "UPDATE / DELETE returns trailing count":
    check parseAffectedRows("UPDATE 3") == 3
    check parseAffectedRows("DELETE 5") == 5
    check parseAffectedRows("SELECT 100") == 100

  test "INSERT 0 N returns N":
    check parseAffectedRows("INSERT 0 1") == 1
    check parseAffectedRows("INSERT 0 42") == 42

  test "trailing whitespace yields 0 (matches legacy split semantics)":
    check parseAffectedRows("UPDATE 3 ") == 0
    check parseAffectedRows("UPDATE 7   ") == 0

  test "empty / non-numeric returns 0":
    check parseAffectedRows("") == 0
    check parseAffectedRows("CREATE TABLE") == 0
    check parseAffectedRows("COMMIT") == 0
    check parseAffectedRows("   ") == 0

  test "single-token numeric tag":
    # Tag that is just a number — not a real PostgreSQL tag but robust for safety.
    check parseAffectedRows("123") == 123

  test "large values":
    check parseAffectedRows("INSERT 0 9999999999") == 9_999_999_999'i64

  test "raw overload on char openArray":
    let s = "UPDATE 42"
    check parseAffectedRowsRaw(s.toOpenArray(0, s.high)) == 42

suite "flattenInline SoA layout":
  test "empty input produces empty SoA":
    let params: seq[PgParamInline] = @[]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 0
    check ranges.len == 0
    check oids.len == 0
    check formats.len == 0

  test "an out-of-range result-format count is rejected with no params":
    # The empty-params early return must not skip the result-format check: the
    # count is a Bind field of its own.
    let params: seq[PgParamInline] = @[]
    expect PgTypeError:
      discard flattenInline(params, maxInt16Count + 1)
    expect PgTypeError:
      discard flattenInline(params, -1)

  test "single short int32 param":
    let params = @[toPgParamInline(42'i32)]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data == @(toBE32(42'i32))
    check ranges.len == 1
    check ranges[0].off == 0
    check ranges[0].len == 4
    check oids == @[OidInt4]
    check formats == @[1'i16]

  test "single NULL param (off=0, len=-1, no bytes written)":
    let params = @[none(int32).toPgParamInline]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 0
    check ranges.len == 1
    check ranges[0].len == -1
    check oids == @[OidInt4]
    check formats == @[1'i16]

  test "single empty string (len=0, off points to current data.len)":
    let params = @[toPgParamInline("")]
    let (data, ranges, _, _) = flattenInline(params)
    check data.len == 0
    check ranges.len == 1
    check ranges[0].len == 0
    check ranges[0].off == 0

  test "boundary string (16 bytes fits in inlineBuf path)":
    let params = @[toPgParamInline("0123456789ABCDEF")]
    let (data, ranges, _, _) = flattenInline(params)
    check data.len == 16
    check data == @(toBytes("0123456789ABCDEF"))
    check ranges.len == 1
    check ranges[0].off == 0
    check ranges[0].len == 16

  test "overflow string (17 bytes uses overflow path)":
    let long = "0123456789ABCDEFG" # 17 chars
    let params = @[toPgParamInline(long)]
    let (data, ranges, _, _) = flattenInline(params)
    check data.len == 17
    check data == @(toBytes(long))
    check ranges.len == 1
    check ranges[0].off == 0
    check ranges[0].len == 17

  test "mixed [short, NULL, overflow, empty, short] — offsets are consecutive":
    let long = "abcdefghijklmnopqrst" # 20 chars → overflow
    let params = @[
      toPgParamInline(1'i32), # 4 bytes
      none(int32).toPgParamInline, # NULL
      toPgParamInline(long), # 20 bytes
      toPgParamInline(""), # 0 bytes
      toPgParamInline(7'i32), # 4 bytes
    ]
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 4 + 20 + 4 # NULL and empty contribute nothing
    check ranges.len == 5
    # int32(1)
    check ranges[0].off == 0
    check ranges[0].len == 4
    check data[0 ..< 4] == @(toBE32(1'i32))
    # NULL
    check ranges[1].len == -1
    # long overflow string
    check ranges[2].off == 4
    check ranges[2].len == 20
    check data[4 ..< 24] == @(toBytes(long))
    # empty string — off points just past the overflow bytes, len 0
    check ranges[3].off == 24
    check ranges[3].len == 0
    # int32(7)
    check ranges[4].off == 24
    check ranges[4].len == 4
    check data[24 ..< 28] == @(toBE32(7'i32))
    check oids == @[OidInt4, OidInt4, OidText, OidText, OidInt4]
    # int32 uses binary(1), text uses text(0)
    check formats == @[1'i16, 1'i16, 0'i16, 0'i16, 1'i16]

  test "100 int32 params — data size, offsets, oids all consistent":
    var params = newSeqOfCap[PgParamInline](100)
    for i in 0 ..< 100:
      params.add toPgParamInline(int32(i * 3))
    let (data, ranges, oids, formats) = flattenInline(params)
    check data.len == 100 * 4
    check ranges.len == 100
    for i in 0 ..< 100:
      check ranges[i].off == int32(i * 4)
      check ranges[i].len == 4
      check data[i * 4 ..< (i + 1) * 4] == @(toBE32(int32(i * 3)))
    for o in oids:
      check o == OidInt4
    for f in formats:
      check f == 1'i16

  test "oversized inline param is rejected before the data buffer is sized":
    # `estBytes` is summed from caller-supplied lengths, so validating late
    # turns the reservation into an OutOfMemDefect instead of a PgError.
    let params = @[
      toPgParamInline(1'i32),
      PgParamInline(oid: OidText, format: 0, len: int32.high, overflow: @[]),
    ]
    expect PgTypeError:
      discard flattenInline(params)
    # Catchable through the public base type, like every other param error.
    expect PgError:
      discard flattenInline(params)

  test "an inline param count past the protocol maximum is rejected":
    # Without this the count only surfaced from `addCount16` mid-send.
    let params = newSeq[PgParamInline](maxInt16Count + 1)
    expect PgTypeError:
      discard flattenInline(params)

suite "Pipeline appendInline SoA layout":
  test "single op: inlineStart/Count correct, ranges point into p.inlineData":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("INSERT", [toPgParamInline(42'i32), toPgParamInline("abc")])
    check p.ops.len == 1
    check p.ops[0].hasInline
    check p.ops[0].inlineStart == 0
    check p.ops[0].inlineCount == 2
    check p.inlineRanges.len == 2
    check p.inlineOids == @[OidInt4, OidText]
    check p.inlineFormats == @[1'i16, 0'i16]
    # Byte layout: 4 bytes for int32 + 3 bytes for "abc"
    check p.inlineData.len == 7
    check p.inlineRanges[0].off == 0
    check p.inlineRanges[0].len == 4
    check p.inlineRanges[1].off == 4
    check p.inlineRanges[1].len == 3

  test "two ops: second op's inlineStart resumes after first":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("INSERT 1", [toPgParamInline(1'i32)])
    p.addExec("INSERT 2", [toPgParamInline(2'i32), toPgParamInline(3'i32)])
    check p.ops.len == 2
    check p.ops[0].inlineStart == 0
    check p.ops[0].inlineCount == 1
    check p.ops[1].inlineStart == 1
    check p.ops[1].inlineCount == 2
    check p.inlineRanges.len == 3
    check p.inlineData.len == 12
    check p.inlineRanges[0].off == 0 # op0 param0
    check p.inlineRanges[1].off == 4 # op1 param0
    check p.inlineRanges[2].off == 8 # op1 param1

  test "mixed NULL + overflow in one op, offsets correct":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    let long = "abcdefghijklmnopqrst" # 20 chars → overflow
    p.addExec(
      "X", [toPgParamInline(9'i32), none(int32).toPgParamInline, toPgParamInline(long)]
    )
    check p.ops[0].inlineCount == 3
    check p.inlineRanges.len == 3
    check p.inlineData.len == 4 + 20 # NULL contributes nothing
    check p.inlineRanges[0].off == 0
    check p.inlineRanges[0].len == 4
    check p.inlineRanges[1].len == -1 # NULL
    check p.inlineRanges[2].off == 4
    check p.inlineRanges[2].len == 20
    check p.inlineData[4 ..< 24] == @(toBytes(long))

  test "addQuery + addExec populate same SoA buffer":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addQuery("SELECT", [toPgParamInline(1'i32)])
    p.addExec("INSERT", [toPgParamInline(2'i32)])
    check p.ops.len == 2
    check p.ops[0].kind == pokQuery
    check p.ops[1].kind == pokExec
    check p.ops[0].hasInline
    check p.ops[1].hasInline
    check p.ops[0].inlineStart == 0
    check p.ops[1].inlineStart == 1
    check p.inlineData.len == 8
    check p.inlineData[0 ..< 4] == @(toBE32(1'i32))
    check p.inlineData[4 ..< 8] == @(toBE32(2'i32))

  test "addExec with empty inline params: hasInline=true, count=0":
    # Edge case: the inline overload is chosen but no params are provided.
    # executeImpl computes `endIdx = inlineStart + inlineCount - 1`, so for
    # count==0 we get an empty `toOpenArray(start, start-1)` view — make sure
    # that view is empty and the SoA buffers are untouched.
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    let empty: seq[PgParamInline] = @[]
    p.addExec("SELECT 1", empty)
    check p.ops.len == 1
    check p.ops[0].hasInline
    check p.ops[0].inlineStart == 0
    check p.ops[0].inlineCount == 0
    check p.inlineRanges.len == 0
    check p.inlineOids.len == 0
    check p.inlineFormats.len == 0
    check p.inlineData.len == 0

  test "addExec empty inline after a populated op: start resumes correctly":
    # A second op with zero inline params must record `inlineStart` at the
    # current tail of the SoA buffers, not 0.
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("A", [toPgParamInline(1'i32), toPgParamInline(2'i32)])
    let empty: seq[PgParamInline] = @[]
    p.addExec("B", empty)
    check p.ops.len == 2
    check p.ops[1].hasInline
    check p.ops[1].inlineStart == 2 # resumes after the two params of op A
    check p.ops[1].inlineCount == 0
    check p.inlineRanges.len == 2 # unchanged by op B

suite "appendInlineParam validation and SoA atomicity":
  ## The batch appenders validate up front and then call the unchecked form,
  ## so the combined validate-and-append shape exists only here.
  template appendInlineParam(
      data: var seq[byte],
      ranges: var seq[tuple[off: int32, len: int32]],
      oids: var seq[int32],
      formats: var seq[int16],
      p: PgParamInline,
  ) =
    validateInlineParam(p)
    appendInlineParamUnchecked(data, ranges, oids, formats, p)

  test "len < -1 raises PgTypeError and leaves SoA unchanged":
    var data: seq[byte] = @[1'u8, 2, 3]
    var ranges: seq[tuple[off: int32, len: int32]] = @[(0'i32, 3'i32)]
    var oids: seq[int32] = @[OidInt4]
    var formats: seq[int16] = @[1'i16]
    var bad = PgParamInline(oid: OidInt4, format: 1, len: -2)
    expect PgTypeError:
      appendInlineParam(data, ranges, oids, formats, bad)
    check data == @[1'u8, 2, 3]
    check ranges.len == 1
    check oids.len == 1
    check formats.len == 1
    check ranges[0] == (0'i32, 3'i32)

  test "overflow capacity mismatch raises PgTypeError and leaves SoA unchanged":
    var data: seq[byte] = @[]
    var ranges: seq[tuple[off: int32, len: int32]] = @[]
    var oids: seq[int32] = @[]
    var formats: seq[int16] = @[]
    var bad = PgParamInline(oid: OidText, format: 0, len: 20, overflow: @[1'u8, 2, 3])
    expect PgTypeError:
      appendInlineParam(data, ranges, oids, formats, bad)
    check data.len == 0
    check ranges.len == 0
    check oids.len == 0
    check formats.len == 0

  test "overflow exact capacity succeeds (len == overflow.len)":
    var data: seq[byte] = @[]
    var ranges: seq[tuple[off: int32, len: int32]] = @[]
    var oids: seq[int32] = @[]
    var formats: seq[int16] = @[]
    var ok = PgParamInline(oid: OidText, format: 0, len: 20, overflow: newSeq[byte](20))
    for i in 0 ..< 20:
      ok.overflow[i] = byte(i)
    appendInlineParam(data, ranges, oids, formats, ok)
    check ranges.len == 1
    check oids.len == 1
    check formats.len == 1
    check data.len == 20
    check data[0] == 0 and data[19] == 19

  test "cumulative SoA size past int32 raises PgTypeError, not RangeDefect":
    # The per-param offset is an int32, so the guard is on the running `data`
    # total. Probed unchecked to reach the bound without allocating 2 GiB.
    var data: seq[byte] = @[1'u8, 2, 3, 4]
    var ranges: seq[tuple[off: int32, len: int32]] = @[]
    var oids: seq[int32] = @[]
    var formats: seq[int16] = @[]
    let huge = PgParamInline(oid: OidText, format: 0, len: int32.high - 1)
    expect PgTypeError:
      appendInlineParamUnchecked(data, ranges, oids, formats, huge)
    check data.len == 4
    check ranges.len == 0 and oids.len == 0 and formats.len == 0
    # An empty param at the same offset takes the same guard.
    let empty = PgParamInline(oid: OidText, format: 0, len: 0)
    appendInlineParamUnchecked(data, ranges, oids, formats, empty)
    check ranges[0] == (4'i32, 0'i32)

  test "valid appends keep SoA lengths synced":
    var data: seq[byte] = @[]
    var ranges: seq[tuple[off: int32, len: int32]] = @[]
    var oids: seq[int32] = @[]
    var formats: seq[int16] = @[]
    var p1 = toPgParamInline(1'i32)
    appendInlineParam(data, ranges, oids, formats, p1)
    check oids.len == 1 and formats.len == 1 and ranges.len == 1
    var p2 = none(int32).toPgParamInline
    appendInlineParam(data, ranges, oids, formats, p2)
    check oids.len == 2 and formats.len == 2 and ranges.len == 2
    check ranges[1].len == -1
    var p3 = toPgParamInline("hello")
    appendInlineParam(data, ranges, oids, formats, p3)
    check oids.len == 3 and formats.len == 3 and ranges.len == 3
    check oids.len == formats.len and formats.len == ranges.len
    check data.len == 4 + 5

  test "Pipeline stays consistent and usable after PgTypeError":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("A", [toPgParamInline(1'i32)])
    check p.inlineRanges.len == 1
    check p.inlineOids.len == 1
    check p.inlineData.len == 4
    var bad = PgParamInline(oid: OidInt4, format: 1, len: -2)
    expect PgTypeError:
      appendInlineParam(
        p.inlineData, p.inlineRanges, p.inlineOids, p.inlineFormats, bad
      )
    check p.inlineRanges.len == 1
    check p.inlineOids.len == 1
    check p.inlineFormats.len == 1
    check p.inlineData.len == 4
    # Next successful append must resume at correct offset
    p.addExec("B", [toPgParamInline(2'i32)])
    check p.inlineRanges.len == 2
    check p.inlineRanges[1].off == 4
    check p.inlineRanges[1].len == 4
    check p.inlineData[4 ..< 8] == @(toBE32(2'i32))
    check p.ops.len == 2
    check p.ops[1].inlineStart == 1
    check p.ops[1].inlineCount == 1

  test "addExec rolls back the whole param batch when one param is bad":
    privateAccess(Pipeline)
    privateAccess(PipelineOp)
    let p = newPipeline(nil)
    p.addExec("A", [toPgParamInline(1'i32)])
    let bad = PgParamInline(oid: OidInt4, format: 1, len: -2)
    expect PgTypeError:
      p.addExec("B", [toPgParamInline(2'i32), toPgParamInline(3'i32), bad])
    # The two params appended before `bad` would be orphaned in the SoA
    # buffers forever: no op references them.
    check p.inlineRanges.len == 1
    check p.inlineOids.len == 1
    check p.inlineFormats.len == 1
    check p.inlineData.len == 4
    check p.ops.len == 1
    p.addExec("C", [toPgParamInline(4'i32)])
    check p.inlineRanges.len == 2
    check p.inlineRanges[1].off == 4
    check p.ops[1].inlineStart == 1

  test "typed params over the Int16 count limit are rejected at add time":
    # A send-phase raise would be attributed to the whole batch.
    privateAccess(Pipeline)
    let p = newPipeline(nil)
    p.addExec("A", @[toPgParam(1'i32)])
    var tooMany = newSeq[PgParam](maxInt16Count + 1)
    for i in 0 ..< tooMany.len:
      tooMany[i] = toPgParam(1'i32)
    expect PgTypeError:
      p.addExec("B", tooMany)
    expect PgTypeError:
      p.addQuery("C", tooMany)
    check p.ops.len == 1

  test "inline params over the Int16 count limit are rejected at add time":
    privateAccess(Pipeline)
    let p = newPipeline(nil)
    var tooMany = newSeq[PgParamInline](maxInt16Count + 1)
    for i in 0 ..< tooMany.len:
      tooMany[i] = toPgParamInline(1'i32)
    expect PgTypeError:
      p.addExec("A", tooMany)
    check p.ops.len == 0
    check p.inlineRanges.len == 0
    check p.inlineData.len == 0

  test "flattenInline propagates PgTypeError without desync":
    var bad = PgParamInline(oid: OidInt4, format: 1, len: -5)
    expect PgTypeError:
      discard flattenInline(@[toPgParamInline(1'i32), bad, toPgParamInline(2'i32)])
    # flattenInline uses temporaries, so only the raise itself is observable:
    # it must not be swallowed and must be PgTypeError, not ValueError.
    var ok = flattenInline(@[toPgParamInline(1'i32), toPgParamInline(2'i32)])
    check ok.ranges.len == 2
    check ok.oids.len == 2
