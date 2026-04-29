## Protocol decoder fuzz / negative-path tests.
##
## Goal: prove that `parseBackendMessage` and the binary type decoders never
## crash (no `Defect`) on arbitrary byte input. Acceptable outcomes for any
## input are:
##   * `psIncomplete` (need more bytes)
##   * `psComplete` / `psDataRow` (valid message recognised)
##   * raising `ProtocolError` (wire protocol violation)
##   * raising `PgTypeError` (unparseable type payload)
## Anything else — in particular `IndexDefect`, `RangeDefect`, `DivByZeroDefect`
## — is a bug in the decoder.

import std/[unittest, random]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types/[core, decoding]

# Helpers

proc wrap(msgType: char, body: openArray[byte]): seq[byte] =
  ## Wrap a body with a backend-message header. `msgLen` = 4 + body.len.
  result = newSeq[byte](5 + body.len)
  result[0] = byte(msgType)
  let msgLen = int32(4 + body.len)
  result[1] = byte((msgLen shr 24) and 0xFF)
  result[2] = byte((msgLen shr 16) and 0xFF)
  result[3] = byte((msgLen shr 8) and 0xFF)
  result[4] = byte(msgLen and 0xFF)
  for i, b in body:
    result[5 + i] = b

proc header(msgType: char, claimedLen: int32): seq[byte] =
  ## Header with an explicit `msgLen` — useful for testing length violations.
  result = newSeq[byte](5)
  result[0] = byte(msgType)
  result[1] = byte((claimedLen shr 24) and 0xFF)
  result[2] = byte((claimedLen shr 16) and 0xFF)
  result[3] = byte((claimedLen shr 8) and 0xFF)
  result[4] = byte(claimedLen and 0xFF)

template expectParseError(body: untyped) =
  ## Assert that `body` raises `ProtocolError` (not `Defect`).
  var raised = false
  try:
    body
  except ProtocolError:
    raised = true
  check raised

template expectTypeError(body: untyped) =
  var raised = false
  try:
    body
  except PgTypeError:
    raised = true
  check raised

proc tryParse(buf: openArray[byte]): tuple[state: ParseState, consumed: int] =
  ## Run `parseBackendMessage` and normalise the outcome. Raises `ProtocolError`
  ## on wire-protocol violations; all other exceptions escape (test failure).
  var consumed = 0
  let res = parseBackendMessage(buf, consumed)
  (res.state, consumed)

# Hand-written negative paths: framing

suite "parseBackendMessage: framing":
  test "empty buffer is incomplete":
    var consumed = 0
    let res = parseBackendMessage(@[], consumed)
    check res.state == psIncomplete
    check consumed == 0

  test "4-byte buffer is incomplete":
    var consumed = 0
    let res = parseBackendMessage(@[byte('C'), 0, 0, 0], consumed)
    check res.state == psIncomplete
    check consumed == 0

  test "msgLen == 0 raises ProtocolError":
    expectParseError:
      discard tryParse(header('C', 0))

  test "msgLen == 3 (below minimum 4) raises ProtocolError":
    expectParseError:
      discard tryParse(header('C', 3))

  test "negative msgLen raises ProtocolError":
    expectParseError:
      discard tryParse(header('C', -1))
    expectParseError:
      discard tryParse(header('C', int32.low))

  test "msgLen above default cap raises ProtocolError":
    # int32.high (~2 GiB) exceeds DefaultMaxBackendMessageLen (1 GiB) and
    # must be rejected before the recv loop can grow the buffer further.
    expectParseError:
      discard tryParse(header('C', int32.high))

  test "msgLen below cap with short buffer is incomplete":
    # Just under the default cap; only the header is supplied, so the
    # parser should ask for more bytes rather than fail.
    let (state, consumed) =
      tryParse(header('C', int32(DefaultMaxBackendMessageLen - 1)))
    check state == psIncomplete
    check consumed == 0

  test "unknown message type 'X' raises ProtocolError":
    expectParseError:
      discard tryParse(wrap('X', @[]))

  test "bytes unassigned on the backend side raise ProtocolError":
    # 'P', 'B', 'F', 'Q' are frontend message types; the rest are truly
    # unassigned. All should be rejected by the backend parser.
    for ch in ['x', 'Y', 'Q', 'P', 'B', 'F', '!', '@']:
      expectParseError:
        discard tryParse(wrap(ch, @[]))

# Hand-written negative paths: per-message-kind body truncation

suite "parseBackendMessage: per-kind malformed bodies":
  test "Authentication (R) with body too short":
    # Needs 4 bytes for the auth type tag.
    expectParseError:
      discard tryParse(wrap('R', @[byte 0]))
    expectParseError:
      discard tryParse(wrap('R', @[byte 0, 0, 0]))

  test "Authentication MD5 without 4-byte salt":
    # authType=5 present, but salt missing.
    expectParseError:
      discard tryParse(wrap('R', @[byte 0, 0, 0, 5]))
    expectParseError:
      discard tryParse(wrap('R', @[byte 0, 0, 0, 5, 0, 0]))

  test "Authentication unknown authType":
    expectParseError:
      discard tryParse(wrap('R', @[byte 0, 0, 0, 99]))

  test "BackendKeyData (K) body shorter than 8":
    expectParseError:
      discard tryParse(wrap('K', @[]))
    expectParseError:
      discard tryParse(wrap('K', newSeq[byte](7)))

  test "DataRow (D) body too short for column count":
    expectParseError:
      discard tryParse(wrap('D', @[]))
    expectParseError:
      discard tryParse(wrap('D', @[byte 0]))

  test "DataRow with more columns than data":
    # numCols=2 but only 4 bytes (enough for one colLen header, not two).
    let body = @[byte 0, 2, 0, 0, 0, 0]
    expectParseError:
      discard tryParse(wrap('D', body))

  test "DataRow with invalid (< -1) column length":
    # numCols=1, colLen=-2.
    let body = @[byte 0, 1, 0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFE'u8]
    expectParseError:
      discard tryParse(wrap('D', body))

  test "DataRow with colLen exceeding body":
    # numCols=1, colLen=100, no actual bytes.
    let body = @[byte 0, 1, 0'u8, 0'u8, 0'u8, 100'u8]
    expectParseError:
      discard tryParse(wrap('D', body))

  test "DataRow with negative column count raises ProtocolError":
    let body = @[byte 0xFF, 0xFF]
    expectParseError:
      discard tryParse(wrap('D', body))

  test "RowDescription (T) shorter than 2 bytes":
    expectParseError:
      discard tryParse(wrap('T', @[]))
    expectParseError:
      discard tryParse(wrap('T', @[byte 0]))

  test "RowDescription claims a field without enough metadata":
    # numFields=1, then a CString "a" followed by only a few bytes (need 18).
    let body = @[byte 0, 1, byte('a'), 0'u8, 0, 0, 0, 0]
    expectParseError:
      discard tryParse(wrap('T', body))

  test "RowDescription field name missing null terminator":
    # numFields=1, "abcd" with no trailing 0.
    let body = @[byte 0, 1, byte('a'), byte('b'), byte('c'), byte('d')]
    expectParseError:
      discard tryParse(wrap('T', body))

  test "CommandComplete (C) without null terminator":
    expectParseError:
      discard tryParse(wrap('C', @[byte('S'), byte('E'), byte('L')]))

  test "ErrorResponse (E) / NoticeResponse (N) with non-terminated field":
    for kind in ['E', 'N']:
      # fieldType byte present, value CString missing null.
      let body = @[byte('M'), byte('x'), byte('y')]
      expectParseError:
        discard tryParse(wrap(kind, body))

  test "ParameterStatus (S) missing value terminator":
    # "key\0" but value has no null.
    let body = @[byte('k'), byte('e'), byte('y'), 0'u8, byte('v'), byte('a')]
    expectParseError:
      discard tryParse(wrap('S', body))

  test "ReadyForQuery (Z) empty or bad status":
    expectParseError:
      discard tryParse(wrap('Z', @[]))
    expectParseError:
      discard tryParse(wrap('Z', @[byte('X')]))

  test "ParameterDescription (t) short header":
    expectParseError:
      discard tryParse(wrap('t', @[]))
    expectParseError:
      discard tryParse(wrap('t', @[byte 0]))

  test "ParameterDescription claims params without OID bytes":
    # numParams=2 but only 4 bytes of OID data (need 8).
    let body = @[byte 0, 2, 0, 0, 0, 0]
    expectParseError:
      discard tryParse(wrap('t', body))

  test "CopyInResponse (G) / CopyOutResponse (H) short":
    for kind in ['G', 'H']:
      expectParseError:
        discard tryParse(wrap(kind, @[]))
      # Format + numCols header present (3 bytes), but one column format missing.
      let body = @[byte 0, 0, 1]
      expectParseError:
        discard tryParse(wrap(kind, body))

  test "CopyBothResponse (W) short":
    expectParseError:
      discard tryParse(wrap('W', @[]))
    expectParseError:
      discard tryParse(wrap('W', @[byte 0, 0, 1]))

  test "NotificationResponse (A) short and malformed":
    expectParseError:
      discard tryParse(wrap('A', @[]))
    # pid present but channel missing null terminator.
    let body = @[byte 0, 0, 0, 1, byte('c'), byte('h')]
    expectParseError:
      discard tryParse(wrap('A', body))

  test "Zero-body messages round-trip successfully":
    for ch in ['1', '2', '3', 'I', 'n', 's', 'c']:
      let (state, consumed) = tryParse(wrap(ch, @[]))
      check state == psComplete
      check consumed == 5 # 1 type + 4 length

# Seeded random fuzz on parseBackendMessage

suite "parseBackendMessage: seeded random fuzz":
  const seeds = [1'i64, 42, 0xDEADBEEF, 0xCAFEBABE, 0xA5A5A5A5, 1234567890]
  const itersPerSeed = 2000

  test "random bytes never crash parseBackendMessage":
    # The contract: any input produces psComplete/psDataRow/psIncomplete or
    # raises ProtocolError. Any other exception escapes and fails the test.
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let n = r.rand(0 .. 512)
        var buf = newSeq[byte](n)
        for i in 0 ..< n:
          buf[i] = byte(r.rand(255))
        try:
          var consumed = 0
          let res = parseBackendMessage(buf, consumed)
          if res.state == psIncomplete:
            check consumed == 0
        except ProtocolError:
          discard

  test "random buffers whose first byte picks a valid message type":
    # Increase hit rate on per-kind decoders by forcing a legal leading byte.
    const types = [
      '1', '2', '3', 'A', 'C', 'D', 'E', 'G', 'H', 'I', 'K', 'N', 'R', 'S', 'T', 'W',
      'Z', 'c', 'd', 'n', 's', 't',
    ]
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let n = r.rand(5 .. 512)
        var buf = newSeq[byte](n)
        buf[0] = byte(types[r.rand(0 ..< types.len)])
        # Random length field, random body.
        for i in 1 ..< n:
          buf[i] = byte(r.rand(255))
        try:
          var consumed = 0
          discard parseBackendMessage(buf, consumed)
        except ProtocolError:
          discard

  test "random buffer fed into streaming RowData sink never crashes":
    # Exercise the `parseDataRowInto` branch with random data.
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed div 4:
        let n = r.rand(5 .. 512)
        var buf = newSeq[byte](n)
        buf[0] = byte('D')
        for i in 1 ..< n:
          buf[i] = byte(r.rand(255))
        let rd = newRowData(1'i16)
        try:
          var consumed = 0
          discard parseBackendMessage(buf, consumed, rd)
        except ProtocolError:
          discard

# Hand-written negative paths: binary type decoders

suite "Binary type decoders: malformed input":
  test "decodeBinaryTimestamp on short input":
    expectTypeError:
      discard decodeBinaryTimestamp(@[])
    expectTypeError:
      discard decodeBinaryTimestamp(newSeq[byte](7))

  test "decodeBinaryDate on short input":
    expectTypeError:
      discard decodeBinaryDate(@[])
    expectTypeError:
      discard decodeBinaryDate(newSeq[byte](3))

  test "decodeBinaryTime on short input":
    expectTypeError:
      discard decodeBinaryTime(@[])
    expectTypeError:
      discard decodeBinaryTime(newSeq[byte](7))

  test "decodeBinaryTimeTz on short input":
    expectTypeError:
      discard decodeBinaryTimeTz(newSeq[byte](11))

  test "decodeInetBinary on short input":
    expectTypeError:
      discard decodeInetBinary(@[])
    expectTypeError:
      discard decodeInetBinary(newSeq[byte](3))
    # family=2 (IPv4) needs 8 bytes total.
    expectTypeError:
      discard decodeInetBinary(@[byte 2, 32, 0, 4])
    # family=3 (IPv6) needs 20 bytes total.
    expectTypeError:
      discard decodeInetBinary(@[byte 3, 128, 0, 16])

  test "decodePointBinary bounds":
    expectTypeError:
      discard decodePointBinary(@[], 0)
    expectTypeError:
      discard decodePointBinary(newSeq[byte](16), 1) # off + 16 > len
    expectTypeError:
      discard decodePointBinary(newSeq[byte](16), -1)

  test "decodeNumericBinary short header":
    expectTypeError:
      discard decodeNumericBinary(@[])
    expectTypeError:
      discard decodeNumericBinary(newSeq[byte](7))

  test "decodeNumericBinary negative ndigits":
    let data = @[byte 0xFF, 0xFF, 0, 0, 0, 0, 0, 0]
    expectTypeError:
      discard decodeNumericBinary(data)

  test "decodeNumericBinary unknown sign":
    let data = @[byte 0, 0, 0, 0, 0x12, 0x34, 0, 0]
    expectTypeError:
      discard decodeNumericBinary(data)

  test "decodeNumericBinary claims more digits than data":
    # ndigits=100 — payload not present.
    let data = @[byte 0, 100, 0, 0, 0, 0, 0, 0]
    expectTypeError:
      discard decodeNumericBinary(data)

  test "decodeBinaryArray short":
    expectTypeError:
      discard decodeBinaryArray(@[])
    expectTypeError:
      discard decodeBinaryArray(newSeq[byte](11))

  test "decodeBinaryArray ndim != 0, 1 is rejected":
    # ndim=2 with 20 bytes of header.
    var data = newSeq[byte](20)
    data[3] = 2 # ndim low byte
    expectTypeError:
      discard decodeBinaryArray(data)

  test "decodeBinaryArray ndim=1 header truncated":
    # ndim=1 but only 12 bytes (need 20).
    var data = newSeq[byte](12)
    data[3] = 1
    expectTypeError:
      discard decodeBinaryArray(data)

  test "decodeBinaryArray bad dimLen":
    var data = newSeq[byte](20)
    data[3] = 1 # ndim=1
    data[12] = 0xFF # dimLen=-1 (after sign extension)
    data[13] = 0xFF
    data[14] = 0xFF
    data[15] = 0xFF
    expectTypeError:
      discard decodeBinaryArray(data)

  test "decodeBinaryArray element truncated":
    # ndim=1, dimLen=1, then claim eLen=100 with no payload.
    var data = newSeq[byte](24)
    data[3] = 1 # ndim=1
    data[15] = 1 # dimLen=1
    data[23] = 100 # eLen=100
    expectTypeError:
      discard decodeBinaryArray(data)

  test "decodeBinaryComposite short":
    expectTypeError:
      discard decodeBinaryComposite(@[])
    expectTypeError:
      discard decodeBinaryComposite(newSeq[byte](3))

  test "decodeBinaryComposite negative numFields":
    var data = newSeq[byte](4)
    data[0] = 0xFF
    data[1] = 0xFF
    data[2] = 0xFF
    data[3] = 0xFF
    expectTypeError:
      discard decodeBinaryComposite(data)

  test "decodeBinaryComposite field truncated":
    # numFields=1 but no field header.
    var data = newSeq[byte](4)
    data[3] = 1
    expectTypeError:
      discard decodeBinaryComposite(data)

  test "decodeHstoreBinary short":
    expectTypeError:
      discard decodeHstoreBinary(@[])
    expectTypeError:
      discard decodeHstoreBinary(newSeq[byte](3))

  test "decodeHstoreBinary truncated key":
    # numPairs=1, keyLen=100 with no data.
    var data = newSeq[byte](8)
    data[3] = 1
    data[7] = 100
    expectTypeError:
      discard decodeHstoreBinary(data)

  test "decodeBinaryTsVector short":
    expectTypeError:
      discard decodeBinaryTsVector(@[])
    expectTypeError:
      discard decodeBinaryTsVector(newSeq[byte](3))

  test "decodeBinaryTsVector negative nlexemes":
    var data = newSeq[byte](4)
    data[0] = 0xFF
    data[1] = 0xFF
    data[2] = 0xFF
    data[3] = 0xFF
    expectTypeError:
      discard decodeBinaryTsVector(data)

  test "decodeBinaryTsVector missing null terminator":
    # nlexemes=1, then bytes without a null terminator.
    let data = @[byte 0, 0, 0, 1, byte('a'), byte('b')]
    expectTypeError:
      discard decodeBinaryTsVector(data)

  test "decodeBinaryTsQuery short":
    expectTypeError:
      discard decodeBinaryTsQuery(@[])
    expectTypeError:
      discard decodeBinaryTsQuery(newSeq[byte](3))

  test "decodeBinaryTsQuery unknown token type":
    # ntokens=1, then token type byte = 99 (unknown).
    let data = @[byte 0, 0, 0, 1, 99'u8]
    expectTypeError:
      discard decodeBinaryTsQuery(data)

# Seeded random fuzz on binary type decoders

suite "Binary type decoders: seeded random fuzz":
  const seeds = [1'i64, 42, 0xBEEF, 0xC0FFEE, 0x13371337]
  const itersPerSeed = 500

  proc randomBuf(r: var Rand, maxLen: int): seq[byte] =
    let n = r.rand(0 .. maxLen)
    result = newSeq[byte](n)
    for i in 0 ..< n:
      result[i] = byte(r.rand(255))

  test "decodeNumericBinary never crashes":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 64)
        try:
          discard decodeNumericBinary(buf)
        except PgTypeError:
          discard

  test "decodeBinaryTimestamp / Date / Time / TimeTz never crash":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 32)
        try:
          discard decodeBinaryTimestamp(buf)
        except PgTypeError:
          discard
        try:
          discard decodeBinaryDate(buf)
        except PgTypeError:
          discard
        try:
          discard decodeBinaryTime(buf)
        except PgTypeError:
          discard
        try:
          discard decodeBinaryTimeTz(buf)
        except PgTypeError:
          discard

  test "decodeInetBinary never crashes":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 32)
        try:
          discard decodeInetBinary(buf)
        except PgTypeError:
          discard

  test "decodePointBinary never crashes":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 32)
        # Try a range of offsets, including negative and past the end.
        for off in [-1, 0, 1, 8, 16, buf.len, buf.len + 1]:
          try:
            discard decodePointBinary(buf, off)
          except PgTypeError:
            discard

  test "decodeBinaryArray never crashes":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 128)
        try:
          discard decodeBinaryArray(buf)
        except PgTypeError:
          discard

  test "decodeBinaryComposite never crashes":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 128)
        try:
          discard decodeBinaryComposite(buf)
        except PgTypeError:
          discard

  test "decodeHstoreBinary never crashes":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 128)
        try:
          discard decodeHstoreBinary(buf)
        except PgTypeError:
          discard

  test "decodeBinaryTsVector / TsQuery never crash":
    for seed in seeds:
      var r = initRand(seed)
      for _ in 0 ..< itersPerSeed:
        let buf = randomBuf(r, 128)
        try:
          discard decodeBinaryTsVector(buf)
        except PgTypeError:
          discard
        try:
          discard decodeBinaryTsQuery(buf)
        except PgTypeError:
          discard
