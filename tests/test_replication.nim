import std/unittest

import ../async_postgres/[pg_errors, pg_protocol, pg_replication]

proc buildBackendMsg(msgType: char, body: seq[byte]): seq[byte] =
  ## Helper to build a raw backend message from type char + body bytes
  result.add(byte(msgType))
  result.addInt32(int32(body.len + 4))
  result.add(body)

suite "LSN":
  test "parseLsn / $ roundtrip":
    let lsn = parseLsn("0/16B3740")
    check $lsn == "0/16B3740"
    check lsn.toUInt64 == 0x16B3740'u64

  test "parseLsn with high bits":
    let lsn = parseLsn("1/ABCD1234")
    check $lsn == "1/ABCD1234"
    check lsn.toUInt64 == 0x1_ABCD1234'u64

  test "parseLsn max value (32-bit safe)":
    let lsn = parseLsn("FFFFFFFF/FFFFFFFF")
    check $lsn == "FFFFFFFF/FFFFFFFF"
    check lsn.toUInt64 == 0xFFFFFFFF_FFFFFFFF'u64

  # IDENTIFY_SYSTEM / CREATE_REPLICATION_SLOT return the LSN as text. Like
  # parseTimelineId, parseLsn must convert a malformed value into PgTypeError
  # (not leak a raw ValueError, including the one fromHex throws on non-hex
  # halves) so callers stay under the single `except PgError` contract.
  test "parseLsn invalid format":
    expect(PgTypeError):
      discard parseLsn("invalid")

  test "parseLsn empty parts":
    expect(PgTypeError):
      discard parseLsn("12345678")

  test "parseLsn non-hex half":
    expect(PgTypeError):
      discard parseLsn("0/XYZ")

  test "parseLsn half wider than 32 bits":
    # A wider half must be rejected, not silently truncated to 32 bits.
    expect(PgTypeError):
      discard parseLsn("100000000/0")

  test "parseLsn half wider than 64 bits":
    # fromHex wraps modulo 2^64 past 16 hex digits instead of raising.
    expect(PgTypeError):
      discard parseLsn("10000000000000000/0")

  test "parseLsn zero-padded half longer than 16 characters still parses":
    # Leading zeros pad past 16 characters but the value is still in-range.
    let lsn = parseLsn("000000000000000001/1")
    check lsn.toUInt64 == 0x1_00000001'u64

  test "InvalidLsn":
    check $InvalidLsn == "0/0"
    check InvalidLsn.toUInt64 == 0'u64

  test "LSN comparison":
    let a = parseLsn("0/100")
    let b = parseLsn("0/200")
    let c = parseLsn("1/0")
    check a < b
    check b < c
    check a <= a
    check not (b < a)

  test "LSN comparison > and >=":
    let a = parseLsn("0/100")
    let b = parseLsn("0/200")
    check b > a
    check not (a > b)
    check a >= a
    check b >= a
    check not (a >= b)

  test "LSN toInt64":
    let lsn = parseLsn("0/FF")
    check lsn.toInt64 == 0xFF'i64

suite "Int64 encoding/decoding":
  test "addInt64 / decodeInt64 roundtrip":
    for val in [0'i64, 1, -1, int64.high, int64.low, 0x0102030405060708'i64]:
      var buf: seq[byte]
      buf.addInt64(val)
      check buf.len == 8
      let decoded = decodeInt64(buf, 0)
      check decoded == val

  test "addInt64 big-endian layout":
    var buf: seq[byte]
    buf.addInt64(0x0102030405060708'i64)
    check buf == @[1'u8, 2, 3, 4, 5, 6, 7, 8]

suite "CopyBothResponse parsing":
  test "parse CopyBothResponse text format":
    var body: seq[byte]
    body.add(0'u8) # text format
    body.addInt16(2'i16) # 2 columns
    body.addInt16(0'i16) # col 0: text
    body.addInt16(1'i16) # col 1: binary
    let raw = buildBackendMsg('W', body)
    var consumed: int
    let res = parseBackendMessage(raw, consumed)
    check res.state == psComplete
    check res.message.kind == bmkCopyBothResponse
    check res.message.copyFormat == cfText
    check res.message.copyColumnFormats == @[0'i16, 1'i16]

  test "parse CopyBothResponse binary format no columns":
    var body: seq[byte]
    body.add(1'u8) # binary format
    body.addInt16(0'i16) # 0 columns
    let raw = buildBackendMsg('W', body)
    var consumed: int
    let res = parseBackendMessage(raw, consumed)
    check res.state == psComplete
    check res.message.kind == bmkCopyBothResponse
    check res.message.copyFormat == cfBinary
    check res.message.copyColumnFormats.len == 0

suite "Standby Status Update encoding":
  test "encodeStandbyStatusUpdate layout":
    let msg = encodeStandbyStatusUpdate(
      receiveLsn = 0x0000000100000000'i64,
      flushLsn = 0x0000000100000000'i64,
      applyLsn = 0x0000000000000000'i64,
      sendTime = 0x0000000000000001'i64,
      reply = 1'u8,
    )
    # CopyData: 'd' + 4-byte length + 'r' + 8+8+8+8+1 = 39 bytes total
    check msg[0] == byte('d')
    check msg.len == 1 + 4 + 1 + 8 + 8 + 8 + 8 + 1 # 39
    # Inner type byte
    check msg[5] == byte('r')
    # Reply requested
    check msg[^1] == 1'u8

suite "Replication message parsing":
  test "parse XLogData":
    var payload: seq[byte]
    payload.add(byte('w'))
    payload.addInt64(0x100'i64) # startLsn
    payload.addInt64(0x200'i64) # walEnd
    payload.addInt64(12345'i64) # sendTime
    payload.add(@[1'u8, 2, 3]) # WAL data

    # Simulate receiving this as CopyData
    let replMsg = parseReplicationMessage(payload)
    check replMsg.kind == rmkXLogData
    check replMsg.xlogData.startLsn == Lsn(0x100'u64)
    check replMsg.xlogData.walEnd == Lsn(0x200'u64)
    check replMsg.xlogData.sendTime == 12345'i64
    check replMsg.xlogData.data == @[1'u8, 2, 3]
    # receivedEndLsn = startLsn + data.len, independent of walEnd
    check replMsg.xlogData.receivedEndLsn == Lsn(0x103'u64)

  test "receivedEndLsn raises instead of wrapping past uint64.high":
    # Unreachable from a real server's WAL position, but the addition must
    # not silently wrap into a smaller, wrong LSN.
    let xlog =
      XLogData(startLsn: Lsn(high(uint64) - 1), walEnd: Lsn(0'u64), data: @[1'u8, 2, 3])
    expect(PgProtocolError):
      discard xlog.receivedEndLsn

  test "parse PrimaryKeepalive":
    var payload: seq[byte]
    payload.add(byte('k'))
    payload.addInt64(0x300'i64) # walEnd
    payload.addInt64(67890'i64) # sendTime
    payload.add(1'u8) # replyRequested

    let replMsg = parseReplicationMessage(payload)
    check replMsg.kind == rmkPrimaryKeepalive
    check replMsg.keepalive.walEnd == Lsn(0x300'u64)
    check replMsg.keepalive.sendTime == 67890'i64
    check replMsg.keepalive.replyRequested == true

  test "parse PrimaryKeepalive no reply":
    var payload: seq[byte]
    payload.add(byte('k'))
    payload.addInt64(0x400'i64)
    payload.addInt64(0'i64)
    payload.add(0'u8)

    let replMsg = parseReplicationMessage(payload)
    check replMsg.keepalive.replyRequested == false

suite "pgoutput decoder":
  test "Begin message":
    var data: seq[byte]
    data.add(byte('B'))
    data.addInt64(0x500'i64) # finalLsn
    data.addInt64(99999'i64) # commitTime
    data.addInt32(42'i32) # xid

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkBegin
    check msg.begin.finalLsn == Lsn(0x500'u64)
    check msg.begin.commitTime == 99999'i64
    check msg.begin.xid == 42'i32

  test "Commit message":
    var data: seq[byte]
    data.add(byte('C'))
    data.add(0'u8) # flags
    data.addInt64(0x600'i64) # commitLsn
    data.addInt64(0x700'i64) # endLsn
    data.addInt64(11111'i64) # commitTime

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkCommit
    check msg.commit.commitLsn == Lsn(0x600'u64)
    check msg.commit.endLsn == Lsn(0x700'u64)

  test "Relation message":
    var data: seq[byte]
    data.add(byte('R'))
    data.addInt32(16384'i32) # relationId
    # namespace: "public\0"
    for c in "public":
      data.add(byte(c))
    data.add(0'u8)
    # name: "users\0"
    for c in "users":
      data.add(byte(c))
    data.add(0'u8)
    data.add(byte('d')) # replicaIdentity = default
    data.addInt16(2'i16) # 2 columns
    # Column 1: id
    data.add(1'u8) # flags (part of key)
    for c in "id":
      data.add(byte(c))
    data.add(0'u8)
    data.addInt32(23'i32) # typeOid = int4
    data.addInt32(-1'i32) # typeMod
    # Column 2: name
    data.add(0'u8) # flags
    for c in "name":
      data.add(byte(c))
    data.add(0'u8)
    data.addInt32(25'i32) # typeOid = text
    data.addInt32(-1'i32) # typeMod

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkRelation
    check msg.relation.relationId == 16384'i32
    check msg.relation.namespace == "public"
    check msg.relation.name == "users"
    check msg.relation.replicaIdentity == 'd'
    check msg.relation.columns.len == 2
    check msg.relation.columns[0].name == "id"
    check msg.relation.columns[0].flags == 1'u8
    check msg.relation.columns[0].typeOid == 23'i32
    check msg.relation.columns[1].name == "name"
    check msg.relation.columns[1].typeOid == 25'i32

  test "Insert message":
    var data: seq[byte]
    data.add(byte('I'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('N')) # new tuple marker
    data.addInt16(2'i16) # 2 columns
    # Column 1: text "42"
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('4'))
    data.add(byte('2'))
    # Column 2: null
    data.add(byte('n'))

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkInsert
    check msg.insert.relationId == 16384'i32
    check msg.insert.newTuple.len == 2
    check msg.insert.newTuple[0].kind == tdkText
    check msg.insert.newTuple[0].data == @[byte('4'), byte('2')]
    check msg.insert.newTuple[1].kind == tdkNull

  test "Insert message with invalid marker raises":
    var data: seq[byte]
    data.add(byte('I'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('X')) # invalid marker (should be 'N')
    data.addInt16(1'i16)
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('4'))
    data.add(byte('2'))

    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Delete message":
    var data: seq[byte]
    data.add(byte('D'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('K')) # key tuple marker
    data.addInt16(1'i16) # 1 column
    # Column 1: text "42"
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('4'))
    data.add(byte('2'))

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkDelete
    check msg.delete.relationId == 16384'i32
    check msg.delete.keyKind == 'K'
    check msg.delete.oldTuple.len == 1
    check msg.delete.oldTuple[0].kind == tdkText

  test "Delete message with full old tuple":
    var data: seq[byte]
    data.add(byte('D'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('O')) # old tuple marker (REPLICA IDENTITY FULL)
    data.addInt16(1'i16) # 1 column
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('4'))
    data.add(byte('2'))

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkDelete
    check msg.delete.keyKind == 'O'
    check msg.delete.oldTuple.len == 1

  test "Delete message with invalid marker raises":
    var data: seq[byte]
    data.add(byte('D'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('X')) # invalid marker
    data.addInt16(1'i16)
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('4'))
    data.add(byte('2'))

    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Update message with old tuple":
    var data: seq[byte]
    data.add(byte('U'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('O')) # old tuple marker
    data.addInt16(1'i16) # 1 column in old tuple
    data.add(byte('t'))
    data.addInt32(3'i32)
    data.add(byte('o'))
    data.add(byte('l'))
    data.add(byte('d'))
    data.add(byte('N')) # new tuple marker
    data.addInt16(1'i16) # 1 column in new tuple
    data.add(byte('t'))
    data.addInt32(3'i32)
    data.add(byte('n'))
    data.add(byte('e'))
    data.add(byte('w'))

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkUpdate
    check msg.update.hasOldTuple == true
    check msg.update.keyKind == 'O'
    check msg.update.oldTuple.len == 1
    check msg.update.oldTuple[0].data == @[byte('o'), byte('l'), byte('d')]
    check msg.update.newTuple.len == 1
    check msg.update.newTuple[0].data == @[byte('n'), byte('e'), byte('w')]

  test "Update message with old key only":
    var data: seq[byte]
    data.add(byte('U'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('K')) # old key marker
    data.addInt16(1'i16) # 1 column in old key
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('4'))
    data.add(byte('2'))
    data.add(byte('N')) # new tuple marker
    data.addInt16(1'i16) # 1 column in new tuple
    data.add(byte('t'))
    data.addInt32(3'i32)
    data.add(byte('n'))
    data.add(byte('e'))
    data.add(byte('w'))

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkUpdate
    check msg.update.hasOldTuple == true
    check msg.update.keyKind == 'K'
    check msg.update.oldTuple.len == 1
    check msg.update.newTuple.len == 1

  test "Update message without old tuple":
    var data: seq[byte]
    data.add(byte('U'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('N')) # new tuple marker directly
    data.addInt16(1'i16) # 1 column
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('h'))
    data.add(byte('i'))

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkUpdate
    check msg.update.hasOldTuple == false
    check msg.update.keyKind == '\0'
    check msg.update.oldTuple.len == 0
    check msg.update.newTuple.len == 1

  test "Update message with invalid marker raises":
    var data: seq[byte]
    data.add(byte('U'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('X')) # invalid marker
    data.addInt16(1'i16)
    data.add(byte('t'))
    data.addInt32(2'i32)
    data.add(byte('h'))
    data.add(byte('i'))

    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Update message with invalid new tuple marker raises":
    var data: seq[byte]
    data.add(byte('U'))
    data.addInt32(16384'i32) # relationId
    data.add(byte('O')) # old tuple marker
    data.addInt16(1'i16) # 1 column in old tuple
    data.add(byte('t'))
    data.addInt32(3'i32)
    data.add(byte('o'))
    data.add(byte('l'))
    data.add(byte('d'))
    data.add(byte('X')) # invalid new tuple marker (should be 'N')
    data.addInt16(1'i16)
    data.add(byte('t'))
    data.addInt32(3'i32)
    data.add(byte('n'))
    data.add(byte('e'))
    data.add(byte('w'))

    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Truncate message":
    var data: seq[byte]
    data.add(byte('T'))
    data.addInt32(2'i32) # 2 relations
    data.add(3'u8) # CASCADE | RESTART IDENTITY
    data.addInt32(16384'i32) # relation 1
    data.addInt32(16385'i32) # relation 2

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkTruncate
    check msg.truncate.options == 3'u8
    check msg.truncate.relationIds == @[16384'i32, 16385'i32]

  test "Logical message":
    var data: seq[byte]
    data.add(byte('M'))
    data.add(1'u8) # flags: transactional
    data.addInt64(0x800'i64) # lsn
    for c in "test_prefix":
      data.add(byte(c))
    data.add(0'u8) # null terminator
    data.addInt32(5'i32) # content length
    data.add(byte('h'))
    data.add(byte('e'))
    data.add(byte('l'))
    data.add(byte('l'))
    data.add(byte('o'))

    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkMessage
    check msg.message.flags == 1'u8
    check msg.message.lsn == Lsn(0x800'u64)
    check msg.message.prefix == "test_prefix"
    check msg.message.content == @[
      byte('h'), byte('e'), byte('l'), byte('l'), byte('o')
    ]

  test "empty data raises":
    expect(PgProtocolError):
      discard parsePgOutputMessage(newSeq[byte]())

  test "unknown message type raises":
    expect(PgProtocolError):
      discard parsePgOutputMessage(@[byte('Z')])

suite "pgoutput decoder bounds checking":
  # Each case feeds a truncated or forged message and asserts a catchable
  # PgProtocolError instead of an out-of-bounds read (which would be an
  # uncatchable IndexDefect, or undefined behaviour under -d:danger).

  test "Begin truncated raises PgProtocolError":
    # 'B' followed by only 4 of the 8 finalLsn bytes
    var data = @[byte('B'), 0, 0, 0, 0]
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Commit truncated raises PgProtocolError":
    # 'C' + flags only, the three LSN/time fields are missing
    var data = @[byte('C'), 0]
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Origin truncated raises PgProtocolError":
    var data = @[byte('O'), 0, 0, 0]
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Type truncated raises PgProtocolError":
    var data = @[byte('Y'), 0, 0]
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Insert with column count past end raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('I'))
    data.addInt32(16384'i32)
    data.add(byte('N'))
    data.addInt16(100'i16) # claims 100 columns, none follow
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Insert with negative column count raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('I'))
    data.addInt32(16384'i32)
    data.add(byte('N'))
    data.addInt16(-1'i16)
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Insert with column count over PG's 1600 column limit raises PgProtocolError":
    # 2000 well-formed 'n' (NULL) columns would otherwise decode successfully;
    # the explicit upper bound must reject it before the loop inspects the buffer.
    var data: seq[byte]
    data.add(byte('I'))
    data.addInt32(16384'i32)
    data.add(byte('N'))
    data.addInt16(2000'i16)
    for _ in 0 ..< 2000:
      data.add(byte('n'))
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Insert tuple data length past end raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('I'))
    data.addInt32(16384'i32)
    data.add(byte('N'))
    data.addInt16(1'i16) # 1 column
    data.add(byte('t'))
    data.addInt32(1000'i32) # claims 1000 bytes
    data.add(byte('x')) # but only 1 present
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Insert tuple with negative data length raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('I'))
    data.addInt32(16384'i32)
    data.add(byte('N'))
    data.addInt16(1'i16)
    data.add(byte('t'))
    data.addInt32(-5'i32)
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Relation truncated mid-column raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('R'))
    data.addInt32(16384'i32)
    for c in "public":
      data.add(byte(c))
    data.add(0'u8)
    for c in "t":
      data.add(byte(c))
    data.add(0'u8)
    data.add(byte('d')) # replicaIdentity
    data.addInt16(1'i16) # 1 column
    data.add(1'u8) # flags
    for c in "id":
      data.add(byte(c))
    data.add(0'u8)
    data.addInt32(23'i32) # typeOid
    # typeMod missing — truncated here
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Relation with column count over PG's 1600 column limit raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('R'))
    data.addInt32(16384'i32)
    for c in "public":
      data.add(byte(c))
    data.add(0'u8)
    for c in "t":
      data.add(byte(c))
    data.add(0'u8)
    data.add(byte('d')) # replicaIdentity
    data.addInt16(2000'i16) # over the limit; no column entries need to follow
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Truncate with oversized relation count raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('T'))
    data.addInt32(1_000_000'i32) # claims 1M relations
    data.add(0'u8) # options
    data.addInt32(16384'i32) # only one actually present
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Truncate with negative relation count raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('T'))
    data.addInt32(-1'i32)
    data.add(0'u8)
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Update with marker past end raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('U'))
    data.addInt32(16384'i32) # relationId, then nothing (no marker byte)
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Message content length past end raises PgProtocolError":
    var data: seq[byte]
    data.add(byte('M'))
    data.add(0'u8) # flags
    data.addInt64(0x800'i64) # lsn
    for c in "p":
      data.add(byte(c))
    data.add(0'u8) # prefix terminator
    data.addInt32(9999'i32) # claims 9999 bytes
    data.add(byte('x')) # but only 1 present
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "Message truncated header raises PgProtocolError":
    var data = @[byte('M'), 0, 0, 0]
    expect(PgProtocolError):
      discard parsePgOutputMessage(data)

  test "valid messages still decode after hardening":
    # Regression guard: a well-formed Begin must continue to parse cleanly.
    var data: seq[byte]
    data.add(byte('B'))
    data.addInt64(0x500'i64)
    data.addInt64(99999'i64)
    data.addInt32(42'i32)
    let msg = parsePgOutputMessage(data)
    check msg.kind == pomkBegin
    check msg.begin.finalLsn == Lsn(0x500'u64)
    check msg.begin.xid == 42'i32

suite "parseTimelineId":
  # IDENTIFY_SYSTEM's timeline column is text. parseTimelineId must convert both
  # the non-numeric ValueError and the out-of-int32-range case into PgTypeError
  # (not leak a raw ValueError or a RangeDefect) so callers stay under the single
  # `except PgError` contract.
  test "valid timeline":
    check parseTimelineId("1") == 1'i32
    check parseTimelineId("2147483647") == int32.high

  test "non-numeric raises PgTypeError":
    expect(PgTypeError):
      discard parseTimelineId("abc")

  test "out-of-int32-range raises PgTypeError (no RangeDefect)":
    expect(PgTypeError):
      discard parseTimelineId("2147483648")
