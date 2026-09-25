import std/[json, unittest, options, tables, math, net, typetraits]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_types/core {.all.}
import ../async_postgres/pg_types/encoding {.all.}
import ../async_postgres/pg_types/accessors {.all.}
import ../async_postgres/pg_types/decoding {.all.}

import types_common

suite "PgInet":
  test "$ IPv4":
    let v = PgInet(address: parseIpAddress("192.168.1.1"), mask: 24)
    check $v == "192.168.1.1/24"

  test "$ IPv6":
    let v = PgInet(address: parseIpAddress("::1"), mask: 128)
    check $v == "::1/128"

  test "== equality":
    let a = PgInet(address: parseIpAddress("10.0.0.1"), mask: 32)
    let b = PgInet(address: parseIpAddress("10.0.0.1"), mask: 32)
    let c = PgInet(address: parseIpAddress("10.0.0.1"), mask: 24)
    check a == b
    check a != c

  test "toPgParam PgInet":
    let v = PgInet(address: parseIpAddress("192.168.1.1"), mask: 24)
    let p = toPgParam(v)
    check p.oid == OidInet
    check p.format == 0
    check toString(p.value.get) == "192.168.1.1/24"

  test "toPgBinaryParam PgInet IPv4":
    let v = PgInet(address: parseIpAddress("192.168.1.1"), mask: 24)
    let p = toPgBinaryParam(v)
    check p.oid == OidInet
    check p.format == 1
    let data = p.value.get
    check data.len == 8
    check data[0] == 2 # AF_INET
    check data[1] == 24 # mask
    check data[2] == 0 # is_cidr
    check data[3] == 4 # addrlen
    check data[4] == 192
    check data[5] == 168
    check data[6] == 1
    check data[7] == 1

  test "toPgBinaryParam PgInet IPv6":
    let v = PgInet(address: parseIpAddress("::1"), mask: 128)
    let p = toPgBinaryParam(v)
    check p.oid == OidInet
    check p.format == 1
    let data = p.value.get
    check data.len == 20
    check data[0] == 3 # AF_INET6
    check data[1] == 128 # mask
    check data[2] == 0 # is_cidr
    check data[3] == 16 # addrlen
    check data[19] == 1 # last byte of ::1

  test "getInet text format":
    let row: Row = @[some(toBytes("192.168.1.1/24"))]
    let v = row.getInet(0)
    check v.address == parseIpAddress("192.168.1.1")
    check v.mask == 24

  test "getInet text format no mask":
    let row: Row = @[some(toBytes("10.0.0.1"))]
    let v = row.getInet(0)
    check v.address == parseIpAddress("10.0.0.1")
    check v.mask == 32

  test "getInet binary format IPv4":
    let data = @[2'u8, 24, 0, 4, 192, 168, 1, 1]
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getInet(0)
    check v.address == parseIpAddress("192.168.1.1")
    check v.mask == 24

  test "getInet binary format IPv6":
    var data = newSeq[byte](20)
    data[0] = 3 # AF_INET6
    data[1] = 64 # mask
    data[2] = 0 # is_cidr
    data[3] = 16 # addrlen
    # fe80::1
    data[4] = 0xfe
    data[5] = 0x80
    data[19] = 1
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getInet(0)
    check v.address == parseIpAddress("fe80::1")
    check v.mask == 64

  test "getInet binary NULL raises":
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getInet(0)
    except PgTypeError:
      raised = true
    check raised

  test "getInetOpt text some":
    let row: Row = @[some(toBytes("10.0.0.1/32"))]
    let v = row.getInetOpt(0)
    check v.isSome
    check v.get.mask == 32

  test "getInetOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getInetOpt(0) == none(PgInet)

  test "getInetOpt binary some":
    let data = @[2'u8, 32, 0, 4, 10, 0, 0, 1]
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getInetOpt(0)
    check v.isSome
    check v.get.address == parseIpAddress("10.0.0.1")

  test "getInetOpt binary none":
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getInetOpt(0) == none(PgInet)

  test "toPgParam Option[PgInet] some":
    let p = toPgParam(some(PgInet(address: parseIpAddress("10.0.0.1"), mask: 32)))
    check p.oid == OidInet
    check p.value.isSome

  test "toPgParam Option[PgInet] none":
    let p = toPgParam(none(PgInet))
    check p.oid == OidInet
    check p.value.isNone

  test "roundtrip binary IPv4":
    let orig = PgInet(address: parseIpAddress("172.16.0.1"), mask: 16)
    let p = toPgBinaryParam(orig)
    let data = p.value.get
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let decoded = row.getInet(0)
    check decoded == orig

  test "roundtrip binary IPv6":
    let orig = PgInet(address: parseIpAddress("2001:db8::1"), mask: 48)
    let p = toPgBinaryParam(orig)
    let data = p.value.get
    let fields = @[mkField(OidInet, 1)]
    let row = mkRow(@[some(data)], fields)
    let decoded = row.getInet(0)
    check decoded == orig

suite "PgCidr":
  test "$ PgCidr":
    let v = PgCidr(address: parseIpAddress("192.168.1.0"), mask: 24)
    check $v == "192.168.1.0/24"

  test "== equality":
    let a = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    let b = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    check a == b

  test "toPgParam PgCidr":
    let v = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    let p = toPgParam(v)
    check p.oid == OidCidr
    check p.format == 0

  test "toPgBinaryParam PgCidr IPv4":
    let v = PgCidr(address: parseIpAddress("10.0.0.0"), mask: 8)
    let p = toPgBinaryParam(v)
    check p.oid == OidCidr
    let data = p.value.get
    check data[0] == 2 # AF_INET
    check data[1] == 8 # mask
    check data[2] == 1 # is_cidr
    check data[3] == 4

  test "getCidr text format":
    let row: Row = @[some(toBytes("10.0.0.0/8"))]
    let v = row.getCidr(0)
    check v.address == parseIpAddress("10.0.0.0")
    check v.mask == 8

  test "getCidr binary format":
    let data = @[2'u8, 8, 1, 4, 10, 0, 0, 0]
    let fields = @[mkField(OidCidr, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getCidr(0)
    check v.address == parseIpAddress("10.0.0.0")
    check v.mask == 8

  test "getCidrOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getCidrOpt(0) == none(PgCidr)

  test "roundtrip binary":
    let orig = PgCidr(address: parseIpAddress("192.168.0.0"), mask: 16)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidCidr, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let decoded = row.getCidr(0)
    check decoded == orig

suite "PgMacAddr":
  test "$ PgMacAddr":
    let v = PgMacAddr("08:00:2b:01:02:03")
    check $v == "08:00:2b:01:02:03"

  test "== equality":
    let a = PgMacAddr("08:00:2b:01:02:03")
    let b = PgMacAddr("08:00:2b:01:02:03")
    let c = PgMacAddr("08:00:2b:01:02:04")
    check a == b
    check a != c

  test "toPgParam PgMacAddr":
    let v = PgMacAddr("08:00:2b:01:02:03")
    let p = toPgParam(v)
    check p.oid == OidMacAddr
    check p.format == 0
    check toString(p.value.get) == "08:00:2b:01:02:03"

  test "toPgBinaryParam PgMacAddr":
    let v = PgMacAddr("08:00:2b:01:02:03")
    let p = toPgBinaryParam(v)
    check p.oid == OidMacAddr
    check p.format == 1
    let data = p.value.get
    check data.len == 6
    check data[0] == 0x08
    check data[1] == 0x00
    check data[2] == 0x2b
    check data[3] == 0x01
    check data[4] == 0x02
    check data[5] == 0x03

  test "toPgBinaryParam PgMacAddr wrong octet count raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr("08:00:2b:01:02"))
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr("08:00:2b:01:02:03:04"))

  test "toPgBinaryParam PgMacAddr non-hex digit raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr("08:00:2b:01:02:zz"))

  test "toPgBinaryParam PgMacAddr octet not 2 digits raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr("8:00:2b:01:02:03"))
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr("080:00:2b:01:02:03"))

  test "toPgBinaryParam PgMacAddr empty raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr(""))

  test "toPgParam PgMacAddr text passes through unvalidated":
    let p = toPgParam(PgMacAddr("not-a-mac"))
    check p.format == 0
    check toString(p.value.get) == "not-a-mac"

  test "getMacAddr text format":
    let row: Row = @[some(toBytes("08:00:2b:01:02:03"))]
    let v = row.getMacAddr(0)
    check v == PgMacAddr("08:00:2b:01:02:03")

  test "getMacAddr binary format":
    let data = @[0x08'u8, 0x00, 0x2b, 0x01, 0x02, 0x03]
    let fields = @[mkField(OidMacAddr, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getMacAddr(0)
    check v == PgMacAddr("08:00:2b:01:02:03")

  test "getMacAddr binary NULL raises":
    let fields = @[mkField(OidMacAddr, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getMacAddr(0)
    except PgTypeError:
      raised = true
    check raised

  test "getMacAddrOpt text some":
    let row: Row = @[some(toBytes("08:00:2b:01:02:03"))]
    let v = row.getMacAddrOpt(0)
    check v.isSome
    check v.get == PgMacAddr("08:00:2b:01:02:03")

  test "getMacAddrOpt text none":
    let row: Row = @[none(seq[byte])]
    check row.getMacAddrOpt(0) == none(PgMacAddr)

  test "toPgParam Option[PgMacAddr] none":
    let p = toPgParam(none(PgMacAddr))
    check p.oid == OidMacAddr
    check p.format == 0
    check p.value.isNone

  test "toPgBinaryParam Option[PgMacAddr] none":
    # Must not prototype via default(PgMacAddr) (empty string → PgTypeError).
    let p = toPgBinaryParam(none(PgMacAddr))
    check p.oid == OidMacAddr
    check p.format == 1
    check p.value.isNone

  test "toPgBinaryParam Option[PgMacAddr] some":
    let p = toPgBinaryParam(some(PgMacAddr("08:00:2b:01:02:03")))
    check p.oid == OidMacAddr
    check p.format == 1
    check p.value.get.len == 6

  test "roundtrip binary":
    let orig = PgMacAddr("aa:bb:cc:dd:ee:ff")
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidMacAddr, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let decoded = row.getMacAddr(0)
    check decoded == orig

suite "PgMacAddr8":
  test "$ PgMacAddr8":
    let v = PgMacAddr8("08:00:2b:01:02:03:04:05")
    check $v == "08:00:2b:01:02:03:04:05"

  test "== equality":
    let a = PgMacAddr8("08:00:2b:01:02:03:04:05")
    let b = PgMacAddr8("08:00:2b:01:02:03:04:05")
    check a == b

  test "toPgParam PgMacAddr8":
    let v = PgMacAddr8("08:00:2b:01:02:03:04:05")
    let p = toPgParam(v)
    check p.oid == OidMacAddr8
    check p.format == 0

  test "toPgBinaryParam PgMacAddr8":
    let v = PgMacAddr8("08:00:2b:01:02:03:04:05")
    let p = toPgBinaryParam(v)
    check p.oid == OidMacAddr8
    check p.format == 1
    let data = p.value.get
    check data.len == 8
    check data[0] == 0x08
    check data[7] == 0x05

  test "toPgBinaryParam PgMacAddr8 wrong octet count raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr8("08:00:2b:01:02:03"))

  test "toPgBinaryParam PgMacAddr8 non-hex digit raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr8("08:00:2b:01:02:03:04:gg"))

  test "toPgBinaryParam PgMacAddr8 octet not 2 digits raises":
    expect PgTypeError:
      discard toPgBinaryParam(PgMacAddr8("08:00:2b:01:02:03:04:5"))

  test "getMacAddr8 text format":
    let row: Row = @[some(toBytes("08:00:2b:01:02:03:04:05"))]
    let v = row.getMacAddr8(0)
    check v == PgMacAddr8("08:00:2b:01:02:03:04:05")

  test "getMacAddr8 binary format":
    let data = @[0x08'u8, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]
    let fields = @[mkField(OidMacAddr8, 1)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getMacAddr8(0)
    check v == PgMacAddr8("08:00:2b:01:02:03:04:05")

  test "getMacAddr8 binary NULL raises":
    let fields = @[mkField(OidMacAddr8, 1)]
    let row = mkRow(@[none(seq[byte])], fields)
    var raised = false
    try:
      discard row.getMacAddr8(0)
    except PgTypeError:
      raised = true
    check raised

  test "getMacAddr8Opt text none":
    let row: Row = @[none(seq[byte])]
    check row.getMacAddr8Opt(0) == none(PgMacAddr8)

  test "toPgParam Option[PgMacAddr8] none":
    let p = toPgParam(none(PgMacAddr8))
    check p.oid == OidMacAddr8
    check p.format == 0
    check p.value.isNone

  test "toPgBinaryParam Option[PgMacAddr8] none":
    # Must not prototype via default(PgMacAddr8) (empty string → PgTypeError).
    let p = toPgBinaryParam(none(PgMacAddr8))
    check p.oid == OidMacAddr8
    check p.format == 1
    check p.value.isNone

  test "toPgBinaryParam Option[PgMacAddr8] some":
    let p = toPgBinaryParam(some(PgMacAddr8("08:00:2b:01:02:03:04:05")))
    check p.oid == OidMacAddr8
    check p.format == 1
    check p.value.get.len == 8

  test "roundtrip binary":
    let orig = PgMacAddr8("aa:bb:cc:dd:ee:ff:00:11")
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidMacAddr8, 1)]
    let row = mkRow(@[some(p.value.get)], fields)
    let decoded = row.getMacAddr8(0)
    check decoded == orig

suite "Geometry types":
  test "OID constants":
    check OidPoint == 600'i32
    check OidLseg == 601'i32
    check OidPath == 602'i32
    check OidBox == 603'i32
    check OidPolygon == 604'i32
    check OidLine == 628'i32
    check OidCircle == 718'i32

  test "PgPoint $ and ==":
    let p = PgPoint(x: 1.5, y: 2.5)
    check $p == "(1.5,2.5)"
    check p == PgPoint(x: 1.5, y: 2.5)
    check p != PgPoint(x: 1.5, y: 3.0)

  test "PgLine $ and ==":
    let l = PgLine(a: 1.0, b: 2.0, c: 3.0)
    check $l == "{1.0,2.0,3.0}"
    check l == PgLine(a: 1.0, b: 2.0, c: 3.0)

  test "PgLseg $ and ==":
    let s = PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 1.0, y: 1.0))
    check $s == "[(0.0,0.0),(1.0,1.0)]"

  test "PgBox $ and ==":
    let b = PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0))
    check $b == "(3.0,4.0),(1.0,2.0)"

  test "PgPath closed $ and ==":
    let p = PgPath(
      closed: true,
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)],
    )
    check $p == "((0.0,0.0),(1.0,0.0),(0.0,1.0))"

  test "PgPath open":
    let p =
      PgPath(closed: false, points: @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 1.0)])
    check $p == "[(0.0,0.0),(1.0,1.0)]"

  test "PgPolygon $":
    let p = PgPolygon(
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)]
    )
    check $p == "((0.0,0.0),(1.0,0.0),(0.0,1.0))"

  test "PgCircle $ and ==":
    let c = PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0)
    check $c == "<(1.0,2.0),5.0>"

  # toPgParam tests
  test "toPgParam PgPoint":
    let p = toPgParam(PgPoint(x: 1.5, y: 2.5))
    check p.oid == OidPoint
    check p.format == 0

  test "toPgParam PgLine":
    let p = toPgParam(PgLine(a: 1.0, b: 2.0, c: 3.0))
    check p.oid == OidLine
    check p.format == 0

  test "toPgParam PgLseg":
    let p = toPgParam(PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 1.0, y: 1.0)))
    check p.oid == OidLseg
    check p.format == 0

  test "toPgParam PgBox":
    let p =
      toPgParam(PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0)))
    check p.oid == OidBox
    check p.format == 0

  test "toPgParam PgPath":
    let p = toPgParam(PgPath(closed: true, points: @[PgPoint(x: 0.0, y: 0.0)]))
    check p.oid == OidPath
    check p.format == 0

  test "toPgParam PgPolygon":
    let p = toPgParam(PgPolygon(points: @[PgPoint(x: 0.0, y: 0.0)]))
    check p.oid == OidPolygon
    check p.format == 0

  test "toPgParam PgCircle":
    let p = toPgParam(PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0))
    check p.oid == OidCircle
    check p.format == 0

  # toPgBinaryParam tests
  test "toPgBinaryParam PgPoint":
    let p = toPgBinaryParam(PgPoint(x: 1.5, y: 2.5))
    check p.oid == OidPoint
    check p.format == 1
    check p.value.get.len == 16

  test "toPgBinaryParam PgLine":
    let p = toPgBinaryParam(PgLine(a: 1.0, b: 2.0, c: 3.0))
    check p.oid == OidLine
    check p.format == 1
    check p.value.get.len == 24

  test "toPgBinaryParam PgLseg":
    let p =
      toPgBinaryParam(PgLseg(p1: PgPoint(x: 0.0, y: 0.0), p2: PgPoint(x: 1.0, y: 1.0)))
    check p.oid == OidLseg
    check p.format == 1
    check p.value.get.len == 32

  test "toPgBinaryParam PgBox":
    let p = toPgBinaryParam(
      PgBox(high: PgPoint(x: 3.0, y: 4.0), low: PgPoint(x: 1.0, y: 2.0))
    )
    check p.oid == OidBox
    check p.format == 1
    check p.value.get.len == 32

  test "toPgBinaryParam PgPath":
    let pts = @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 1.0)]
    let p = toPgBinaryParam(PgPath(closed: true, points: pts))
    check p.oid == OidPath
    check p.format == 1
    check p.value.get.len == 1 + 4 + 2 * 16

  test "toPgBinaryParam PgPolygon":
    let pts =
      @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)]
    let p = toPgBinaryParam(PgPolygon(points: pts))
    check p.oid == OidPolygon
    check p.format == 1
    check p.value.get.len == 4 + 3 * 16

  test "toPgBinaryParam PgCircle":
    let p = toPgBinaryParam(PgCircle(center: PgPoint(x: 1.0, y: 2.0), radius: 5.0))
    check p.oid == OidCircle
    check p.format == 1
    check p.value.get.len == 24

  # Text format decoding tests
  test "getPoint text format":
    let row: Row = @[some(toBytes("(1.5,2.5)"))]
    let v = row.getPoint(0)
    check v.x == 1.5
    check v.y == 2.5

  test "getLine text format":
    let row: Row = @[some(toBytes("{1.0,2.0,3.0}"))]
    let v = row.getLine(0)
    check v.a == 1.0
    check v.b == 2.0
    check v.c == 3.0

  test "getLseg text format":
    let row: Row = @[some(toBytes("[(0,0),(1,1)]"))]
    let v = row.getLseg(0)
    check v.p1 == PgPoint(x: 0.0, y: 0.0)
    check v.p2 == PgPoint(x: 1.0, y: 1.0)

  test "getBox text format":
    let row: Row = @[some(toBytes("(3,4),(1,2)"))]
    let v = row.getBox(0)
    check v.high == PgPoint(x: 3.0, y: 4.0)
    check v.low == PgPoint(x: 1.0, y: 2.0)

  test "getPath text format closed":
    let row: Row = @[some(toBytes("((0,0),(1,0),(0,1))"))]
    let v = row.getPath(0)
    check v.closed == true
    check v.points.len == 3

  test "getPath text format open":
    let row: Row = @[some(toBytes("[(0,0),(1,1)]"))]
    let v = row.getPath(0)
    check v.closed == false
    check v.points.len == 2

  test "getPolygon text format":
    let row: Row = @[some(toBytes("((0,0),(1,0),(0,1))"))]
    let v = row.getPolygon(0)
    check v.points.len == 3

  test "getCircle text format":
    let row: Row = @[some(toBytes("<(1,2),5>"))]
    let v = row.getCircle(0)
    check v.center == PgPoint(x: 1.0, y: 2.0)
    check v.radius == 5.0

  # Binary format roundtrip tests
  test "PgPoint binary roundtrip":
    let orig = PgPoint(x: -3.14, y: 2.718)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPoint, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPoint(0)
    check decoded == orig

  test "PgLine binary roundtrip":
    let orig = PgLine(a: 1.0, b: -2.0, c: 3.5)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidLine, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getLine(0)
    check decoded == orig

  test "PgLseg binary roundtrip":
    let orig = PgLseg(p1: PgPoint(x: -1.0, y: 2.0), p2: PgPoint(x: 3.0, y: -4.0))
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidLseg, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getLseg(0)
    check decoded == orig

  test "PgBox binary roundtrip":
    let orig = PgBox(high: PgPoint(x: 5.0, y: 6.0), low: PgPoint(x: 1.0, y: 2.0))
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidBox, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getBox(0)
    check decoded == orig

  test "PgPath binary roundtrip closed":
    let orig = PgPath(
      closed: true,
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 1.0, y: 0.0), PgPoint(x: 0.0, y: 1.0)],
    )
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPath, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPath(0)
    check decoded == orig

  test "PgPath binary roundtrip open":
    let orig =
      PgPath(closed: false, points: @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 5.0, y: 5.0)])
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPath, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPath(0)
    check decoded == orig

  test "PgPolygon binary roundtrip":
    let orig = PgPolygon(
      points:
        @[PgPoint(x: 0.0, y: 0.0), PgPoint(x: 4.0, y: 0.0), PgPoint(x: 2.0, y: 3.0)]
    )
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidPolygon, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getPolygon(0)
    check decoded == orig

  test "PgCircle binary roundtrip":
    let orig = PgCircle(center: PgPoint(x: -1.5, y: 2.5), radius: 10.0)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidCircle, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let decoded = row.getCircle(0)
    check decoded == orig

  # Opt accessor tests
  test "getPointOpt some":
    let row: Row = @[some(toBytes("(1,2)"))]
    check row.getPointOpt(0).isSome

  test "getPointOpt none":
    let row: Row = @[none(seq[byte])]
    check row.getPointOpt(0).isNone

  test "getCircleOpt binary some":
    let orig = PgCircle(center: PgPoint(x: 0.0, y: 0.0), radius: 1.0)
    let p = toPgBinaryParam(orig)
    let fields = @[mkField(OidCircle, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let r = row.getCircleOpt(0)
    check r.isSome
    check r.get == orig

  test "getCircleOpt binary none":
    let fields = @[mkField(OidCircle, 1'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getCircleOpt(0).isNone

suite "tsvector / tsquery":
  test "toPgParam PgTsVector":
    let v = PgTsVector("'cat':1A 'dog':3")
    let p = toPgParam(v)
    check p.oid == OidTsVector
    check p.format == 0
    check toString(p.value.get) == "'cat':1A 'dog':3"

  test "toPgParam PgTsQuery":
    let q = PgTsQuery("'fat' & 'rat'")
    let p = toPgParam(q)
    check p.oid == OidTsQuery
    check p.format == 0
    check toString(p.value.get) == "'fat' & 'rat'"

  test "toPgBinaryParam PgTsVector sends text format":
    let v = PgTsVector("'cat':1A 'dog':3")
    let p = toPgBinaryParam(v)
    check p.oid == OidTsVector
    check p.format == 0

  test "toPgBinaryParam PgTsQuery sends text format":
    let q = PgTsQuery("'fat' & 'rat'")
    let p = toPgBinaryParam(q)
    check p.oid == OidTsQuery
    check p.format == 0

  test "$ PgTsVector":
    let v = PgTsVector("'cat':1A 'dog':3")
    check $v == "'cat':1A 'dog':3"

  test "$ PgTsQuery":
    let q = PgTsQuery("'fat' & 'rat'")
    check $q == "'fat' & 'rat'"

  test "== PgTsVector":
    check PgTsVector("'a':1") == PgTsVector("'a':1")
    check PgTsVector("'a':1") != PgTsVector("'b':1")

  test "== PgTsQuery":
    check PgTsQuery("'a' & 'b'") == PgTsQuery("'a' & 'b'")
    check PgTsQuery("'a'") != PgTsQuery("'b'")

  test "getTsVector text format":
    let data = toBytes("'cat':1A,2B 'dog':3")
    let fields = @[mkField(OidTsVector, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getTsVector(0) == "'cat':1A,2B 'dog':3"

  test "getTsQuery text format":
    let data = toBytes("'fat' & 'rat'")
    let fields = @[mkField(OidTsQuery, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getTsQuery(0) == "'fat' & 'rat'"

  test "getTsVector binary format":
    # Binary tsvector for 'cat':1A (1 lexeme, 1 position with weight A)
    # Header: nlexemes = 1
    # Lexeme: "cat\0" + npos=1 + position=1 with weight A (3 << 14 | 1 = 0xC001)
    var data: seq[byte] = @[]
    # nlexemes = 1
    data.add(@(toBE32(1'i32)))
    # lexeme "cat" + null terminator
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    # npos = 1
    data.add(@(toBE16(1'i16)))
    # position 1 with weight A (weight=3, so 3 << 14 | 1 = 0xC001)
    data.add(@(toBE16(cast[int16](0xC001'u16))))
    let fields = @[mkField(OidTsVector, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getTsVector(0)
    check "'cat':1A" == $v

  test "getTsVector binary format multiple lexemes":
    # Binary tsvector for 'bar' 'foo':2B
    var data: seq[byte] = @[]
    # nlexemes = 2
    data.add(@(toBE32(2'i32)))
    # lexeme "bar" + null + npos=0
    for c in "bar":
      data.add(byte(c))
    data.add(0'u8)
    data.add(@(toBE16(0'i16)))
    # lexeme "foo" + null + npos=1 + position 2 with weight B (weight=2, 2 << 14 | 2 = 0x8002)
    for c in "foo":
      data.add(byte(c))
    data.add(0'u8)
    data.add(@(toBE16(1'i16)))
    data.add(@(toBE16(cast[int16](0x8002'u16))))
    let fields = @[mkField(OidTsVector, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let v = row.getTsVector(0)
    check "'bar' 'foo':2B" == $v

  test "getTsQuery binary format simple AND":
    # Binary tsquery for 'cat' & 'dog' (prefix: AND, cat, dog)
    var data: seq[byte] = @[]
    # ntokens = 3
    data.add(@(toBE32(3'i32)))
    # AND operator: type=2, op=2
    data.add(2'u8)
    data.add(2'u8)
    # operand "cat": type=1, weight=0, prefix=0, "cat\0"
    data.add(1'u8) # type
    data.add(0'u8) # weight
    data.add(0'u8) # prefix
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    # operand "dog": type=1, weight=0, prefix=0, "dog\0"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "dog":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "'cat' & 'dog'" == $q

  test "getTsQuery binary format NOT":
    # Binary tsquery for !'cat' (prefix: NOT, cat)
    var data: seq[byte] = @[]
    data.add(@(toBE32(2'i32)))
    # NOT operator: type=2, op=1
    data.add(2'u8)
    data.add(1'u8)
    # operand "cat"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "!'cat'" == $q

  test "getTsQuery binary format PHRASE":
    # Binary tsquery for 'cat' <-> 'dog' (prefix: PHRASE dist=1, cat, dog)
    var data: seq[byte] = @[]
    data.add(@(toBE32(3'i32)))
    # PHRASE operator: type=2, op=4, distance=1
    data.add(2'u8)
    data.add(4'u8)
    data.add(@(toBE16(1'i16)))
    # operand "cat"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    # operand "dog"
    data.add(1'u8)
    data.add(0'u8)
    data.add(0'u8)
    for c in "dog":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "'cat' <-> 'dog'" == $q

  test "getTsQuery binary NOT wraps compound child":
    # NOT(AND(a, b)) must round-trip as "!( 'a' & 'b' )", not "!'a' & 'b'"
    # (which reparses as AND(NOT a, b)).
    var data: seq[byte] = @[]
    data.add(@(toBE32(4'i32)))
    data.add(@[byte 2, 1]) # NOT
    data.add(@[byte 2, 2]) # AND
    data.add(@[byte 1, 0, 0, byte('a'), 0])
    data.add(@[byte 1, 0, 0, byte('b'), 0])
    check decodeBinaryTsQuery(data) == "!( 'a' & 'b' )"

  test "getTsQuery binary AND(NOT a, b) does not collide with NOT(AND a b)":
    # NOT binds tighter than AND, so this needs no wrap and stays "!'a' & 'b'".
    var data: seq[byte] = @[]
    data.add(@(toBE32(4'i32)))
    data.add(@[byte 2, 2]) # AND
    data.add(@[byte 2, 1]) # NOT
    data.add(@[byte 1, 0, 0, byte('a'), 0])
    data.add(@[byte 1, 0, 0, byte('b'), 0])
    check decodeBinaryTsQuery(data) == "!'a' & 'b'"

  test "getTsQuery binary PHRASE wraps AND child":
    # PHRASE binds tighter than AND, so AND under PHRASE needs parens.
    var data: seq[byte] = @[]
    data.add(@(toBE32(5'i32)))
    data.add(@[byte 2, 4, 0, 1]) # PHRASE dist=1
    data.add(@[byte 1, 0, 0, byte('a'), 0])
    data.add(@[byte 2, 2]) # AND
    data.add(@[byte 1, 0, 0, byte('b'), 0])
    data.add(@[byte 1, 0, 0, byte('c'), 0])
    check decodeBinaryTsQuery(data) == "'a' <-> ( 'b' & 'c' )"

  test "getTsQuery binary AND wraps OR child":
    # AND binds tighter than OR, so OR under AND needs parens.
    var data: seq[byte] = @[]
    data.add(@(toBE32(5'i32)))
    data.add(@[byte 2, 2]) # AND
    data.add(@[byte 2, 3]) # OR
    data.add(@[byte 1, 0, 0, byte('a'), 0])
    data.add(@[byte 1, 0, 0, byte('b'), 0])
    data.add(@[byte 1, 0, 0, byte('c'), 0])
    check decodeBinaryTsQuery(data) == "( 'a' | 'b' ) & 'c'"

  test "getTsQuery binary OR does not wrap AND child":
    # AND binds tighter than OR; no parens needed.
    var data: seq[byte] = @[]
    data.add(@(toBE32(5'i32)))
    data.add(@[byte 2, 3]) # OR
    data.add(@[byte 2, 2]) # AND
    data.add(@[byte 1, 0, 0, byte('a'), 0])
    data.add(@[byte 1, 0, 0, byte('b'), 0])
    data.add(@[byte 1, 0, 0, byte('c'), 0])
    check decodeBinaryTsQuery(data) == "'a' & 'b' | 'c'"

  test "getTsQuery binary format with weight and prefix":
    # Binary tsquery for 'cat':AB* (single operand with weights A+B and prefix)
    var data: seq[byte] = @[]
    data.add(@(toBE32(1'i32)))
    # operand "cat": type=1, weight=0x0C (A=0x08 + B=0x04), prefix=1
    data.add(1'u8)
    data.add(0x0C'u8) # A + B
    data.add(1'u8) # prefix
    for c in "cat":
      data.add(byte(c))
    data.add(0'u8)
    let fields = @[mkField(OidTsQuery, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let q = row.getTsQuery(0)
    check "'cat':AB*" == $q

  test "getTsVectorOpt text some":
    let data = toBytes("'cat':1A")
    let fields = @[mkField(OidTsVector, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getTsVectorOpt(0)
    check r.isSome
    check $r.get == "'cat':1A"

  test "getTsVectorOpt none":
    let fields = @[mkField(OidTsVector, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getTsVectorOpt(0).isNone

  test "getTsQueryOpt text some":
    let data = toBytes("'cat' & 'dog'")
    let fields = @[mkField(OidTsQuery, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getTsQueryOpt(0)
    check r.isSome
    check $r.get == "'cat' & 'dog'"

  test "getTsQueryOpt none":
    let fields = @[mkField(OidTsQuery, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getTsQueryOpt(0).isNone

suite "xml":
  test "OID constant":
    check OidXml == 142'i32

  test "toPgParam PgXml":
    let v = PgXml("<root><item>hello</item></root>")
    let p = toPgParam(v)
    check p.oid == OidXml
    check p.format == 0
    check toString(p.value.get) == "<root><item>hello</item></root>"

  test "toPgBinaryParam PgXml sends binary format":
    let v = PgXml("<root/>")
    let p = toPgBinaryParam(v)
    check p.oid == OidXml
    check p.format == 1

  test "$ PgXml":
    let v = PgXml("<root/>")
    check $v == "<root/>"

  test "== PgXml":
    check PgXml("<a/>") == PgXml("<a/>")
    check PgXml("<a/>") != PgXml("<b/>")

  test "getXml text format":
    let data = toBytes("<root><item>test</item></root>")
    let fields = @[mkField(OidXml, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getXml(0) == "<root><item>test</item></root>"

  test "getXml binary format":
    let data = toBytes("<root><item>test</item></root>")
    let fields = @[mkField(OidXml, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    check $row.getXml(0) == "<root><item>test</item></root>"

  test "getXmlOpt some":
    let data = toBytes("<root/>")
    let fields = @[mkField(OidXml, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getXmlOpt(0)
    check r.isSome
    check $r.get == "<root/>"

  test "getXmlOpt none":
    let fields = @[mkField(OidXml, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getXmlOpt(0).isNone

suite "hstore":
  test "encodeHstoreText empty":
    let h: PgHstore = initTable[string, Option[string]]()
    check encodeHstoreText(h) == ""

  test "encodeHstoreText single pair":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = some("val")
    check encodeHstoreText(h) == "\"key\"=>\"val\""

  test "encodeHstoreText NULL value":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = none(string)
    check encodeHstoreText(h) == "\"key\"=>NULL"

  test "encodeHstoreText escape":
    var h: PgHstore = initTable[string, Option[string]]()
    h["k\"ey"] = some("v\\al")
    check encodeHstoreText(h) == "\"k\\\"ey\"=>\"v\\\\al\""

  test "parseHstoreText empty":
    let h = parseHstoreText("")
    check h.len == 0

  test "parseHstoreText single pair":
    let h = parseHstoreText("\"key\"=>\"val\"")
    check h.len == 1
    check h["key"] == some("val")

  test "parseHstoreText NULL value":
    let h = parseHstoreText("\"key\"=>NULL")
    check h.len == 1
    check h["key"] == none(string)

  test "parseHstoreText multiple pairs":
    let h = parseHstoreText("\"a\"=>\"1\", \"b\"=>NULL, \"c\"=>\"3\"")
    check h.len == 3
    check h["a"] == some("1")
    check h["b"] == none(string)
    check h["c"] == some("3")

  test "parseHstoreText escaped":
    let h = parseHstoreText("\"k\\\"ey\"=>\"v\\\\al\"")
    check h.len == 1
    check h["k\"ey"] == some("v\\al")

  test "parseHstoreText roundtrip":
    var h: PgHstore = initTable[string, Option[string]]()
    h["hello"] = some("world")
    h["null_val"] = none(string)
    let encoded = encodeHstoreText(h)
    let decoded = parseHstoreText(encoded)
    check decoded == h

  test "encodeHstoreBinary empty":
    let h: PgHstore = initTable[string, Option[string]]()
    let data = encodeHstoreBinary(h)
    check data == @[byte 0, 0, 0, 0] # numPairs = 0

  test "decodeHstoreBinary empty":
    let data = @[byte 0, 0, 0, 0]
    let h = decodeHstoreBinary(data)
    check h.len == 0

  test "encodeHstoreBinary and decodeHstoreBinary roundtrip":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = some("val")
    h["nul"] = none(string)
    let data = encodeHstoreBinary(h)
    let decoded = decodeHstoreBinary(data)
    check decoded == h

  test "decodeHstoreBinary single pair with NULL":
    # numPairs=1, key="a" (len=1), val=NULL (len=-1)
    let data = @[
      byte 0,
      0,
      0,
      1, # numPairs = 1
      0,
      0,
      0,
      1, # keyLen = 1
      byte('a'), # key data
      0xFF,
      0xFF,
      0xFF,
      0xFF, # valLen = -1 (NULL)
    ]
    let h = decodeHstoreBinary(data)
    check h.len == 1
    check h["a"] == none(string)

  test "toPgParam hstore":
    var h: PgHstore = initTable[string, Option[string]]()
    h["k"] = some("v")
    let p = toPgParam(h)
    check p.oid == 0'i32
    check p.format == 0
    check p.value.isSome
    check toString(p.value.get) == "\"k\"=>\"v\""

  test "toPgBinaryParam hstore":
    var h: PgHstore = initTable[string, Option[string]]()
    h["k"] = some("v")
    let p = toPgBinaryParam(h, 16385'i32)
    check p.oid == 16385'i32
    check p.format == 1
    check p.value.isSome
    let decoded = decodeHstoreBinary(p.value.get)
    check decoded == h

  test "getHstore text format":
    let data = toBytes("\"a\"=>\"1\", \"b\"=>NULL")
    let fields = @[mkField(OidText, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let h = row.getHstore(0)
    check h.len == 2
    check h["a"] == some("1")
    check h["b"] == none(string)

  test "getHstore binary format":
    var h: PgHstore = initTable[string, Option[string]]()
    h["key"] = some("val")
    let data = encodeHstoreBinary(h)
    let fields = @[mkField(16385'i32, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    check row.getHstore(0) == h

  test "getHstoreOpt some":
    let data = toBytes("\"a\"=>\"1\"")
    let fields = @[mkField(OidText, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let r = row.getHstoreOpt(0)
    check r.isSome
    check r.get["a"] == some("1")

  test "getHstoreOpt none":
    let fields = @[mkField(OidText, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getHstoreOpt(0).isNone

  test "toPgParam seq[PgHstore] text roundtrip":
    var h1: PgHstore = initTable[string, Option[string]]()
    h1["a"] = some("1")
    var h2: PgHstore = initTable[string, Option[string]]()
    h2["b"] = none(string)
    let p = toPgParam(@[h1, h2])
    check p.oid == 0'i32
    check p.format == 0
    let fields = @[mkField(OidTextArray, 0'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getHstoreArray(0)
    check arr.len == 2
    check arr[0] == h1
    check arr[1] == h2

  test "toPgParam seq[PgHstore] empty":
    let p = toPgParam(newSeq[PgHstore]())
    check p.oid == 0'i32
    check p.format == 0
    check toString(p.value.get) == "{}"

  test "toPgBinaryParam seq[PgHstore] roundtrip":
    var h1: PgHstore = initTable[string, Option[string]]()
    h1["x"] = some("y")
    var h2: PgHstore = initTable[string, Option[string]]()
    h2["nul"] = none(string)
    let p = toPgBinaryParam(@[h1, h2], 16385'i32, 16386'i32)
    check p.oid == 16386'i32
    check p.format == 1
    let fields = @[mkField(16386'i32, 1'i16)]
    let row = mkRow(@[p.value], fields)
    let arr = row.getHstoreArray(0)
    check arr.len == 2
    check arr[0] == h1
    check arr[1] == h2

  test "toPgBinaryParam seq[PgHstore] empty":
    let p = toPgBinaryParam(newSeq[PgHstore](), 16385'i32, 16386'i32)
    check p.oid == 16386'i32
    check p.format == 1
    let fields = @[mkField(16386'i32, 1'i16)]
    let row = mkRow(@[p.value], fields)
    check row.getHstoreArray(0).len == 0

  test "getHstoreArray text format":
    let row: Row = @[some(toBytes("{\"\\\"a\\\"=>\\\"1\\\"\",\"\\\"b\\\"=>NULL\"}"))]
    let arr = row.getHstoreArray(0)
    check arr.len == 2
    check arr[0]["a"] == some("1")
    check arr[1]["b"] == none(string)

  test "getHstoreArrayOpt some":
    var h1: PgHstore = initTable[string, Option[string]]()
    h1["k"] = some("v")
    let p = toPgParam(@[h1])
    let fields = @[mkField(OidTextArray, 0'i16)]
    let row = mkRow(@[p.value], fields)
    let r = row.getHstoreArrayOpt(0)
    check r.isSome
    check r.get.len == 1
    check r.get[0] == h1

  test "getHstoreArrayOpt none":
    let fields = @[mkField(OidTextArray, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getHstoreArrayOpt(0).isNone

suite "PgBit":
  test "OID constants":
    check OidBit == 1560'i32
    check OidVarbit == 1562'i32
    check OidBitArray == 1561'i32
    check OidVarbitArray == 1563'i32

  test "parseBitString and $ roundtrip":
    let b = parseBitString("10110011")
    check b.nbits == 8
    check b.data == @[0b10110011'u8]
    check $b == "10110011"

  test "parseBitString non-byte-aligned":
    let b = parseBitString("101")
    check b.nbits == 3
    check $b == "101"
    # data should be 0b10100000
    check b.data == @[0b10100000'u8]

  test "parseBitString empty":
    let b = parseBitString("")
    check b.nbits == 0
    check b.data.len == 0
    check $b == ""

  test "== operator":
    check parseBitString("1010") == parseBitString("1010")
    check parseBitString("1010") != parseBitString("1011")
    check parseBitString("10") != parseBitString("1000")

  test "toPgParam PgBit":
    let b = parseBitString("10110011")
    let p = toPgParam(b)
    check p.oid == OidVarbit
    check p.format == 0
    check toString(p.value.get) == "10110011"

  test "toPgBinaryParam PgBit":
    let b = parseBitString("10110011")
    let p = toPgBinaryParam(b)
    check p.oid == OidVarbit
    check p.format == 1
    let data = p.value.get
    check data.len == 5 # 4 bytes for nbits + 1 byte for data
    # nbits = 8 in big-endian
    check data[0 .. 3] == @[0'u8, 0, 0, 8]
    check data[4] == 0b10110011'u8

  test "toPgBinaryParam PgBit non-byte-aligned":
    let b = parseBitString("101")
    let p = toPgBinaryParam(b)
    let data = p.value.get
    check data.len == 5
    # nbits = 3 in big-endian
    check data[0 .. 3] == @[0'u8, 0, 0, 3]
    check data[4] == 0b10100000'u8

  # The invariant holds by construction: `initPgBit` is the only way to build a
  # PgBit, so it is rejected before a value exists rather than at encode time.
  test "initPgBit rejects negative nbits":
    expect PgTypeError:
      discard initPgBit(-1, @[0'u8])

  test "initPgBit rejects nbits above limit":
    expect PgTypeError:
      discard initPgBit(PgBitMaxBits + 1, @[])

  test "initPgBit rejects nbits/data.len mismatch":
    # nbits=8 requires exactly 1 packed byte; supplying 2 must be rejected.
    expect PgTypeError:
      discard initPgBit(8, @[0'u8, 0'u8])
    # nbits=3 requires 1 byte; supplying 0 must also be rejected.
    expect PgTypeError:
      discard initPgBit(3, @[])

  test "getBit text format":
    let data = toBytes("10110011")
    let fields = @[mkField(OidVarbit, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBit(0)
    check b.nbits == 8
    check $b == "10110011"

  test "getBit binary format":
    # Binary: 4 bytes nbits (8) + 1 byte data
    var data: seq[byte] = @[]
    data.add(@(toBE32(8'i32)))
    data.add(0b10110011'u8)
    let fields = @[mkField(OidVarbit, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBit(0)
    check b.nbits == 8
    check $b == "10110011"

  test "getBit binary format non-byte-aligned":
    var data: seq[byte] = @[]
    data.add(@(toBE32(3'i32)))
    data.add(0b10100000'u8)
    let fields = @[mkField(OidBit, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBit(0)
    check b.nbits == 3
    check $b == "101"

  test "getBitOpt with value":
    let data = toBytes("10110011")
    let fields = @[mkField(OidVarbit, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let b = row.getBitOpt(0)
    check b.isSome
    check $b.get == "10110011"

  test "getBitOpt with NULL":
    let fields = @[mkField(OidVarbit, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getBitOpt(0).isNone

  test "toPgParam seq[PgBit]":
    let v = @[parseBitString("1010"), parseBitString("110")]
    let p = toPgParam(v)
    check p.oid == OidVarbitArray
    check p.format == 1

  test "toPgParam seq[PgBit] empty":
    let v: seq[PgBit] = @[]
    let p = toPgParam(v)
    check p.oid == OidVarbitArray
    check p.format == 1

  test "toPgBinaryParam seq[PgBit]":
    let v = @[parseBitString("1010"), parseBitString("110")]
    let p = toPgBinaryParam(v)
    check p.oid == OidVarbitArray
    check p.format == 1

  test "getBitArray text format":
    let data = toBytes("{1010,110,00001111}")
    let fields = @[mkField(OidVarbitArray, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let arr = row.getBitArray(0)
    check arr.len == 3
    check $arr[0] == "1010"
    check $arr[1] == "110"
    check $arr[2] == "00001111"

  test "getBitArray binary format":
    # Encode two elements using toPgBinaryParam
    let v = @[parseBitString("1010"), parseBitString("11001100")]
    let p = toPgBinaryParam(v)
    let data = p.value.get
    let fields = @[mkField(OidVarbitArray, 1'i16)]
    let row = mkRow(@[some(data)], fields)
    let arr = row.getBitArray(0)
    check arr.len == 2
    check $arr[0] == "1010"
    check $arr[1] == "11001100"

  test "getBitArrayOpt with value":
    let data = toBytes("{101}")
    let fields = @[mkField(OidVarbitArray, 0'i16)]
    let row = mkRow(@[some(data)], fields)
    let arr = row.getBitArrayOpt(0)
    check arr.isSome
    check arr.get.len == 1
    check $arr.get[0] == "101"

  test "getBitArrayOpt with NULL":
    let fields = @[mkField(OidVarbitArray, 0'i16)]
    let row = mkRow(@[none(seq[byte])], fields)
    check row.getBitArrayOpt(0).isNone

suite "PgBit construction validation":
  ## Was encoder-side: an invalid `PgBit` could be built and only failed at
  ## `toPgParam`. It cannot be built now.
  test "inconsistent nbits/data raises PgTypeError, not IndexDefect":
    expect PgTypeError:
      discard initPgBit(16, @[0xFF'u8])

  test "negative nbits raises PgTypeError instead of encoding an empty varbit":
    expect PgTypeError:
      discard initPgBit(-1, @[])

  test "nbits above the limit raises PgTypeError":
    expect PgTypeError:
      discard initPgBit(PgBitMaxBits + 1, @[])

  test "consistent PgBit still encodes":
    check toPgParam(initPgBit(4, @[0xA0'u8])).value.get == toBytes("1010")
