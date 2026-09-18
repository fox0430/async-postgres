## Dedicated unit tests for ``pg_bytes`` — big-endian helpers and bounded copies.
## Keeps the leaf encoding primitives covered without relying on protocol suites.

import std/unittest

import ../async_postgres/[pg_bytes, pg_errors]

suite "pg_bytes big-endian encode/decode":
  test "encodes known big-endian byte order":
    check toBE16(0x0102'i16) == [1'u8, 2'u8]
    check toBE16(-1'i16) == [0xFF'u8, 0xFF'u8]
    check toBE32(0x01020304'i32) == [1'u8, 2'u8, 3'u8, 4'u8]
    check toBE32(-1'i32) == [0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8]
    check toBE64(0x1122334455667788'i64) ==
      [0x11'u8, 0x22'u8, 0x33'u8, 0x44'u8, 0x55'u8, 0x66'u8, 0x77'u8, 0x88'u8]
    check fromBE16([1'u8, 2'u8]) == 0x0102'i16
    check fromBE32([1'u8, 2'u8, 3'u8, 4'u8]) == 0x01020304'i32
    check fromBE64(
      [0x11'u8, 0x22'u8, 0x33'u8, 0x44'u8, 0x55'u8, 0x66'u8, 0x77'u8, 0x88'u8]
    ) == 0x1122334455667788'i64

  test "toBE16/fromBE16 roundtrip including negatives":
    for v in [0'i16, 1'i16, -1'i16, high(int16), low(int16)]:
      let enc = toBE16(v)
      check fromBE16(enc) == v

  test "toBE32/fromBE32 roundtrip including negatives":
    for v in [0'i32, 1'i32, -1'i32, high(int32), low(int32)]:
      let enc = toBE32(v)
      check fromBE32(enc) == v

  test "toBE64/fromBE64 roundtrip including negatives":
    for v in [0'i64, 1'i64, -1'i64, high(int64), low(int64)]:
      let enc = toBE64(v)
      check fromBE64(enc) == v

  test "writeBE* writes big-endian bytes at an offset":
    var buf = newSeq[byte](16)
    writeBE16(buf, 2, 0x0102'i16)
    writeBE32(buf, 4, 0x01020304'i32)
    writeBE64(buf, 8, 0x1122334455667788'i64)
    check buf[2 ..< 4] == @[1'u8, 2'u8]
    check buf[4 ..< 8] == @[1'u8, 2'u8, 3'u8, 4'u8]
    check buf[8 ..< 16] ==
      @[0x11'u8, 0x22'u8, 0x33'u8, 0x44'u8, 0x55'u8, 0x66'u8, 0x77'u8, 0x88'u8]

  test "writeBE* matches toBE* at an offset":
    var buf = newSeq[byte](16)
    writeBE16(buf, 2, -42'i16)
    writeBE32(buf, 4, 0x01020304'i32)
    writeBE64(buf, 8, 0x1122334455667788'i64)
    check fromBE16(buf, 2) == -42'i16
    check fromBE32(buf, 4) == 0x01020304'i32
    check fromBE64(buf, 8) == 0x1122334455667788'i64

  test "decodeFloat32BE / decodeFloat64BE decode known byte order":
    check decodeFloat32BE([0x3F'u8, 0xC0'u8, 0x00'u8, 0x00'u8]) == 1.5'f32
    check decodeFloat64BE(
      [0xC0'u8, 0x02'u8, 0x00'u8, 0x00'u8, 0x00'u8, 0x00'u8, 0x00'u8, 0x00'u8]
    ) == -2.25'f64

  test "decodeFloat32BE / decodeFloat64BE match IEEE bit patterns":
    let f32 = 1.5'f32
    let f64 = -2.25'f64
    check decodeFloat32BE(@(toBE32(cast[int32](cast[uint32](f32))))) == f32
    check decodeFloat64BE(@(toBE64(cast[int64](cast[uint64](f64))))) == f64

suite "pg_bytes bounded copies":
  test "writeBytesAt copies and rejects out-of-range slices":
    var dst = newSeq[byte](4)
    let src = @[1'u8, 2]
    writeBytesAt(dst, 1, src)
    check dst == @[0'u8, 1, 2, 0]
    let empty: seq[byte] = @[]
    writeBytesAt(dst, 0, empty) # empty is a no-op
    check dst == @[0'u8, 1, 2, 0]
    let tooLong = @[9'u8, 9]
    expect PgProtocolError:
      writeBytesAt(dst, 3, tooLong)
    let one = @[1'u8]
    expect PgProtocolError:
      writeBytesAt(dst, -1, one)

  test "appendBytes grows the destination":
    var buf: seq[byte]
    let empty: seq[byte] = @[]
    appendBytes(buf, empty)
    check buf.len == 0
    let a = @[1'u8, 2, 3]
    let b = @[4'u8]
    appendBytes(buf, a)
    appendBytes(buf, b)
    check buf == @[1'u8, 2, 3, 4]

  test "readString / readBytes copy and reject bad ranges":
    let src = @[byte('a'), byte('b'), byte('c'), byte('d')]
    check readString(src, 1, 2) == "bc"
    check readBytes(src, 0, 4) == src
    check readString(src, 0, 0) == ""
    check readBytes(src, 2, 0) == newSeq[byte]()
    expect PgProtocolError:
      discard readString(src, 2, 3)
    expect PgProtocolError:
      discard readBytes(src, -1, 1)
    expect PgProtocolError:
      discard readString(src, 0, -1)
