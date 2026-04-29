import std/[unittest, options, strutils, tables, importutils]

import ../async_postgres/async_backend
import ../async_postgres/[pg_protocol, pg_connection]

privateAccess(PgConnection)

proc parseBackendMessage(buf: var seq[byte]): ParseResult =
  ## Test-only wrapper that preserves the old var-buf interface.
  var consumed: int
  result = parseBackendMessage(buf, consumed)
  if consumed > 0:
    buf = buf[consumed .. ^1]

suite "Byte helpers":
  test "encodeInt16 / decodeInt16 roundtrip":
    for val in [0'i16, 1, 255, 256, -1, int16.high, int16.low]:
      let encoded = encodeInt16(val)
      let decoded = decodeInt16(encoded, 0)
      check decoded == val

  test "encodeInt32 / decodeInt32 roundtrip":
    for val in [0'i32, 1, 255, 65536, -1, int32.high, int32.low]:
      let encoded = encodeInt32(val)
      let decoded = decodeInt32(encoded, 0)
      check decoded == val

  test "addInt16 / addInt32":
    var buf: seq[byte] = @[]
    buf.addInt16(0x0102)
    check buf == @[1'u8, 2]
    buf.addInt32(0x03040506)
    check buf == @[1'u8, 2, 3, 4, 5, 6]

  test "addCString":
    var buf: seq[byte] = @[]
    buf.addCString("abc")
    check buf == @[byte('a'), byte('b'), byte('c'), 0'u8]

  test "decodeCString":
    let buf = @[byte('h'), byte('i'), 0'u8, byte('x')]
    let (s, consumed) = decodeCString(buf, 0)
    check s == "hi"
    check consumed == 3 # 'h', 'i', '\0'

  test "decodeCString empty":
    let buf = @[0'u8]
    let (s, consumed) = decodeCString(buf, 0)
    check s == ""
    check consumed == 1

  test "decodeCString at end of buffer":
    let buf = @[byte('a'), 0'u8]
    expect(ProtocolError):
      discard decodeCString(buf, 2)

  test "decodeCString offset past end of buffer":
    let buf = @[byte('a'), 0'u8]
    expect(ProtocolError):
      discard decodeCString(buf, 3)

  test "decodeCString offset past end of empty buffer":
    let buf: seq[byte] = @[]
    expect(ProtocolError):
      discard decodeCString(buf, 1)

  test "decodeCString missing null terminator":
    let buf = @[byte('h'), byte('i')]
    expect(ProtocolError):
      discard decodeCString(buf, 0)

suite "Frontend encoding":
  test "encodeStartup - no type byte, version 3.0":
    let msg = encodeStartup("testuser", "testdb")
    # No type byte - first 4 bytes are length
    let length = decodeInt32(msg, 0)
    check length == int32(msg.len)
    # Protocol version 3.0 = 196608
    let version = decodeInt32(msg, 4)
    check version == 196608
    # Contains user and database
    let s = cast[string](msg[8 .. ^1])
    check "user" in s
    check "testuser" in s
    check "database" in s
    check "testdb" in s
    # Ends with null terminator
    check msg[^1] == 0'u8

  test "encodeStartup - extra params":
    let msg = encodeStartup("u", "d", {"application_name": "test"})
    let s = cast[string](msg[8 .. ^1])
    check "application_name" in s
    check "test" in s

  test "encodeSSLRequest - 8 bytes fixed":
    let msg = encodeSSLRequest()
    check msg.len == 8
    check decodeInt32(msg, 0) == 8'i32
    check decodeInt32(msg, 4) == 80877103'i32

  test "encodePassword":
    let msg = encodePassword("secret")
    check msg[0] == byte('p')
    let length = decodeInt32(msg, 1)
    check length == int32(msg.len - 1)

  test "encodeSASLInitialResponse":
    let data = cast[seq[byte]]("n,,n=user,r=nonce")
    let msg = encodeSASLInitialResponse("SCRAM-SHA-256", data)
    check msg[0] == byte('p')
    let length = decodeInt32(msg, 1)
    check length == int32(msg.len - 1)

  test "encodeSASLResponse":
    let data = @[1'u8, 2, 3]
    let msg = encodeSASLResponse(data)
    check msg[0] == byte('p')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

  test "encodeQuery":
    let msg = encodeQuery("SELECT 1")
    check msg[0] == byte('Q')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

  test "encodeParse":
    let msg = encodeParse("stmt1", "SELECT $1::int", @[23'i32])
    check msg[0] == byte('P')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

  test "encodeBind":
    let paramValues = @[some(cast[seq[byte]]("42")), none(seq[byte])]
    let msg = encodeBind("", "stmt1", @[0'i16, 0'i16], paramValues)
    check msg[0] == byte('B')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

  test "encodeDescribe":
    let msg = encodeDescribe(dkStatement, "stmt1")
    check msg[0] == byte('D')
    check msg[5] == byte('S')

  test "encodeExecute":
    let msg = encodeExecute("", 0)
    check msg[0] == byte('E')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

  test "encodeClose":
    let msg = encodeClose(dkPortal, "p1")
    check msg[0] == byte('C')
    check msg[5] == byte('P')

  test "encodeSync - 5 bytes fixed":
    let msg = encodeSync()
    check msg.len == 5
    check msg[0] == byte('S')
    check decodeInt32(msg, 1) == 4'i32

  test "encodeFlush - 5 bytes fixed":
    let msg = encodeFlush()
    check msg.len == 5
    check msg[0] == byte('H')
    check decodeInt32(msg, 1) == 4'i32

  test "encodeTerminate - 5 bytes fixed":
    let msg = encodeTerminate()
    check msg.len == 5
    check msg[0] == byte('X')
    check decodeInt32(msg, 1) == 4'i32

  test "encodeCancelRequest - 16 bytes fixed":
    let msg = encodeCancelRequest(1234, 5678)
    check msg.len == 16
    check decodeInt32(msg, 0) == 16'i32
    check decodeInt32(msg, 4) == 80877102'i32
    check decodeInt32(msg, 8) == 1234'i32
    check decodeInt32(msg, 12) == 5678'i32

  test "encodeCopyData":
    var msg: seq[byte]
    encodeCopyData(msg, @[1'u8, 2, 3])
    check msg[0] == byte('d')
    check decodeInt32(msg, 1) == 7'i32 # 4 + 3

  test "encodeCopyDone":
    let msg = encodeCopyDone()
    check msg.len == 5
    check msg[0] == byte('c')

  test "encodeCopyFail":
    let msg = encodeCopyFail("error")
    check msg[0] == byte('f')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

suite "Backend decoding":
  # Helper to build a backend message byte buffer
  proc buildMsg(msgType: char, body: seq[byte]): seq[byte] =
    result = @[byte(msgType)]
    result.addInt32(int32(4 + body.len))
    result.add(body)

  test "AuthenticationOk":
    var buf = buildMsg('R', @[0'u8, 0, 0, 0])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkAuthenticationOk
    check buf.len == 0

  test "AuthenticationCleartextPassword":
    var buf = buildMsg('R', @[0'u8, 0, 0, 3])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkAuthenticationCleartextPassword

  test "AuthenticationMD5Password":
    var body: seq[byte] = @[]
    body.addInt32(5)
    body.add(@[0xDE'u8, 0xAD, 0xBE, 0xEF])
    var buf = buildMsg('R', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkAuthenticationMD5Password
    check res.message.md5Salt == [0xDE'u8, 0xAD, 0xBE, 0xEF]

  test "AuthenticationSASL":
    var body: seq[byte] = @[]
    body.addInt32(10)
    body.addCString("SCRAM-SHA-256")
    body.add(0'u8) # terminator
    var buf = buildMsg('R', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkAuthenticationSASL
    check res.message.saslMechanisms == @["SCRAM-SHA-256"]

  test "AuthenticationSASLContinue":
    var body: seq[byte] = @[]
    body.addInt32(11)
    body.add(cast[seq[byte]]("r=nonce,s=salt,i=4096"))
    var buf = buildMsg('R', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkAuthenticationSASLContinue
    check cast[string](res.message.saslData) == "r=nonce,s=salt,i=4096"

  test "AuthenticationSASLFinal":
    var body: seq[byte] = @[]
    body.addInt32(12)
    body.add(cast[seq[byte]]("v=signature"))
    var buf = buildMsg('R', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkAuthenticationSASLFinal

  test "BackendKeyData":
    var body: seq[byte] = @[]
    body.addInt32(42)
    body.addInt32(12345)
    var buf = buildMsg('K', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkBackendKeyData
    check res.message.backendPid == 42
    check res.message.backendSecretKey == 12345

  test "ReadyForQuery - idle":
    var buf = buildMsg('Z', @[byte('I')])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkReadyForQuery
    check res.message.txStatus == tsIdle

  test "ReadyForQuery - in transaction":
    var buf = buildMsg('Z', @[byte('T')])
    let res = parseBackendMessage(buf)
    check res.message.txStatus == tsInTransaction

  test "ReadyForQuery - failed transaction":
    var buf = buildMsg('Z', @[byte('E')])
    let res = parseBackendMessage(buf)
    check res.message.txStatus == tsInFailedTransaction

  test "ParameterStatus":
    var body: seq[byte] = @[]
    body.addCString("server_version")
    body.addCString("15.2")
    var buf = buildMsg('S', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkParameterStatus
    check res.message.paramName == "server_version"
    check res.message.paramValue == "15.2"

  test "RowDescription":
    var body: seq[byte] = @[]
    body.addInt16(1) # 1 field
    body.addCString("id")
    body.addInt32(0) # tableOid
    body.addInt16(0) # columnAttrNum
    body.addInt32(23) # typeOid (int4)
    body.addInt16(4) # typeSize
    body.addInt32(-1) # typeMod
    body.addInt16(0) # formatCode (text)
    var buf = buildMsg('T', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkRowDescription
    check res.message.fields.len == 1
    check res.message.fields[0].name == "id"
    check res.message.fields[0].typeOid == 23
    check res.message.fields[0].typeSize == 4

  test "DataRow":
    var body: seq[byte] = @[]
    body.addInt16(2) # 2 columns
    body.addInt32(3) # first column: 3 bytes
    body.add(cast[seq[byte]]("123"))
    body.addInt32(-1) # second column: NULL
    var buf = buildMsg('D', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkDataRow
    check res.message.columns.len == 2
    check res.message.columns[0].isSome
    check cast[string](res.message.columns[0].get) == "123"
    check res.message.columns[1].isNone

  test "ErrorResponse":
    var body: seq[byte] = @[]
    body.add(byte('S'))
    body.addCString("ERROR")
    body.add(byte('C'))
    body.addCString("42P01")
    body.add(byte('M'))
    body.addCString("relation does not exist")
    body.add(0'u8) # terminator
    var buf = buildMsg('E', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkErrorResponse
    check res.message.errorFields.len == 3
    check getErrorField(res.message.errorFields, 'S') == "ERROR"
    check getErrorField(res.message.errorFields, 'C') == "42P01"
    check getErrorField(res.message.errorFields, 'M') == "relation does not exist"

  test "CommandComplete":
    var body: seq[byte] = @[]
    body.addCString("INSERT 0 1")
    var buf = buildMsg('C', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkCommandComplete
    check res.message.commandTag == "INSERT 0 1"

  test "ParseComplete":
    var buf = buildMsg('1', @[])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkParseComplete

  test "BindComplete":
    var buf = buildMsg('2', @[])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkBindComplete

  test "CloseComplete":
    var buf = buildMsg('3', @[])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkCloseComplete

  test "EmptyQueryResponse":
    var buf = buildMsg('I', @[])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkEmptyQueryResponse

  test "NoData":
    var buf = buildMsg('n', @[])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkNoData

  test "PortalSuspended":
    var buf = buildMsg('s', @[])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkPortalSuspended

  test "NoticeResponse":
    var body: seq[byte] = @[]
    body.add(byte('S'))
    body.addCString("NOTICE")
    body.add(byte('M'))
    body.addCString("test notice")
    body.add(0'u8)
    var buf = buildMsg('N', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkNoticeResponse
    check res.message.noticeFields.len == 2

  test "NotificationResponse":
    var body: seq[byte] = @[]
    body.addInt32(100) # pid
    body.addCString("my_channel")
    body.addCString("payload data")
    var buf = buildMsg('A', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkNotificationResponse
    check res.message.notifPid == 100
    check res.message.notifChannel == "my_channel"
    check res.message.notifPayload == "payload data"

  test "ParameterDescription":
    var body: seq[byte] = @[]
    body.addInt16(2)
    body.addInt32(23) # int4
    body.addInt32(25) # text
    var buf = buildMsg('t', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkParameterDescription
    check res.message.paramTypeOids == @[23'i32, 25'i32]

  test "CopyInResponse":
    var body: seq[byte] = @[]
    body.add(0'u8) # text format
    body.addInt16(2)
    body.addInt16(0)
    body.addInt16(0)
    var buf = buildMsg('G', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkCopyInResponse
    check res.message.copyFormat == cfText
    check res.message.copyColumnFormats.len == 2

  test "CopyOutResponse":
    var body: seq[byte] = @[]
    body.add(1'u8) # binary format
    body.addInt16(1)
    body.addInt16(1)
    var buf = buildMsg('H', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkCopyOutResponse
    check res.message.copyFormat == cfBinary

  test "CopyData":
    var buf = buildMsg('d', @[1'u8, 2, 3])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkCopyData
    check res.message.copyData == @[1'u8, 2, 3]

  test "CopyDone":
    var buf = buildMsg('c', @[])
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkCopyDone

suite "Incomplete data handling":
  test "empty buffer returns psIncomplete":
    var buf: seq[byte] = @[]
    let res = parseBackendMessage(buf)
    check res.state == psIncomplete

  test "partial header returns psIncomplete":
    var buf = @[byte('R'), 0'u8, 0]
    let res = parseBackendMessage(buf)
    check res.state == psIncomplete

  test "header present but body incomplete returns psIncomplete":
    # Type + length says 9 bytes body, but only 2 provided
    var buf = @[byte('R'), 0'u8, 0, 0, 13, 0, 0]
    let res = parseBackendMessage(buf)
    check res.state == psIncomplete
    check buf.len == 7 # buffer unchanged

suite "Consecutive messages":
  proc buildMsg(msgType: char, body: seq[byte]): seq[byte] =
    result = @[byte(msgType)]
    result.addInt32(int32(4 + body.len))
    result.add(body)

  test "parse multiple messages from single buffer":
    var buf: seq[byte] = @[]
    # First: ParseComplete ('1')
    buf.add(buildMsg('1', @[]))
    # Second: BindComplete ('2')
    buf.add(buildMsg('2', @[]))
    # Third: ReadyForQuery
    buf.add(buildMsg('Z', @[byte('I')]))

    let r1 = parseBackendMessage(buf)
    check r1.state == psComplete
    check r1.message.kind == bmkParseComplete

    let r2 = parseBackendMessage(buf)
    check r2.state == psComplete
    check r2.message.kind == bmkBindComplete

    let r3 = parseBackendMessage(buf)
    check r3.state == psComplete
    check r3.message.kind == bmkReadyForQuery
    check r3.message.txStatus == tsIdle

    # Buffer should be empty now
    check buf.len == 0

    # Next parse should return incomplete
    let r4 = parseBackendMessage(buf)
    check r4.state == psIncomplete

suite "newPgQueryError":
  test "all fields populated":
    let fields = @[
      ErrorField(code: 'S', value: "ERROR"),
      ErrorField(code: 'C', value: "42P01"),
      ErrorField(code: 'M', value: "relation \"foo\" does not exist"),
      ErrorField(code: 'D', value: "some detail"),
      ErrorField(code: 'H', value: "create the table first"),
    ]
    let err = newPgQueryError(fields)
    check err.sqlState == "42P01"
    check err.severity == "ERROR"
    check err.detail == "some detail"
    check err.hint == "create the table first"
    check "42P01" in err.msg
    check "relation \"foo\" does not exist" in err.msg

  test "missing optional fields default to empty":
    let fields = @[
      ErrorField(code: 'S', value: "ERROR"),
      ErrorField(code: 'M', value: "something failed"),
    ]
    let err = newPgQueryError(fields)
    check err.sqlState == ""
    check err.severity == "ERROR"
    check err.detail == ""
    check err.hint == ""
    check "something failed" in err.msg

  test "inherits from PgError":
    let fields = @[
      ErrorField(code: 'S', value: "ERROR"),
      ErrorField(code: 'C', value: "23505"),
      ErrorField(code: 'M', value: "duplicate key"),
    ]
    let err = newPgQueryError(fields)
    # Can be caught as PgError
    var caughtAsPgError = false
    try:
      raise err
    except PgError:
      caughtAsPgError = true
    check caughtAsPgError

suite "Utility":
  test "formatError":
    let fields = @[
      ErrorField(code: 'S', value: "ERROR"),
      ErrorField(code: 'C', value: "42P01"),
      ErrorField(code: 'M', value: "table not found"),
      ErrorField(code: 'D', value: "some detail"),
    ]
    let s = formatError(fields)
    check "ERROR" in s
    check "table not found" in s
    check "42P01" in s
    check "some detail" in s

  test "getErrorField - missing returns empty":
    let fields = @[ErrorField(code: 'S', value: "ERROR")]
    check getErrorField(fields, 'M') == ""
    check getErrorField(fields, 'S') == "ERROR"

  test "formatError with hint":
    let fields = @[
      ErrorField(code: 'S', value: "ERROR"),
      ErrorField(code: 'M', value: "permission denied"),
      ErrorField(code: 'H', value: "Grant access first"),
    ]
    let s = formatError(fields)
    check "HINT: Grant access first" in s

  test "formatError minimal - only message":
    let fields = @[ErrorField(code: 'M', value: "something failed")]
    let s = formatError(fields)
    check "something failed" in s

  test "getErrorField - empty list":
    let fields: seq[ErrorField] = @[]
    check getErrorField(fields, 'S') == ""

suite "Backend decoding - edge cases":
  proc buildMsg(msgType: char, body: seq[byte]): seq[byte] =
    result = @[byte(msgType)]
    result.addInt32(int32(4 + body.len))
    result.add(body)

  test "RowDescription with 0 fields":
    var body: seq[byte] = @[]
    body.addInt16(0)
    var buf = buildMsg('T', body)
    let res = parseBackendMessage(buf)
    check res.state == psComplete
    check res.message.kind == bmkRowDescription
    check res.message.fields.len == 0

  test "RowDescription with multiple fields":
    var body: seq[byte] = @[]
    body.addInt16(2)
    # field 1
    body.addCString("col_a")
    body.addInt32(1000) # tableOid
    body.addInt16(1) # columnAttrNum
    body.addInt32(23) # typeOid int4
    body.addInt16(4) # typeSize
    body.addInt32(-1) # typeMod
    body.addInt16(0) # formatCode
    # field 2
    body.addCString("col_b")
    body.addInt32(1000)
    body.addInt16(2)
    body.addInt32(25) # typeOid text
    body.addInt16(-1) # typeSize (variable)
    body.addInt32(-1) # typeMod
    body.addInt16(0) # formatCode
    var buf = buildMsg('T', body)
    let res = parseBackendMessage(buf)
    check res.message.fields.len == 2
    check res.message.fields[0].name == "col_a"
    check res.message.fields[0].typeOid == 23
    check res.message.fields[0].columnAttrNum == 1
    check res.message.fields[1].name == "col_b"
    check res.message.fields[1].typeOid == 25
    check res.message.fields[1].typeSize == -1

  test "DataRow with 0 columns":
    var body: seq[byte] = @[]
    body.addInt16(0)
    var buf = buildMsg('D', body)
    let res = parseBackendMessage(buf)
    check res.message.kind == bmkDataRow
    check res.message.columns.len == 0

  test "DataRow with empty data (len=0 vs NULL)":
    var body: seq[byte] = @[]
    body.addInt16(2)
    body.addInt32(0) # empty string (NOT NULL)
    body.addInt32(-1) # NULL
    var buf = buildMsg('D', body)
    let res = parseBackendMessage(buf)
    check res.message.columns[0].isSome
    check res.message.columns[0].get.len == 0
    check res.message.columns[1].isNone

  test "DataRow with large column":
    var body: seq[byte] = @[]
    body.addInt16(1)
    let largeData = newSeq[byte](1000)
    body.addInt32(int32(largeData.len))
    body.add(largeData)
    var buf = buildMsg('D', body)
    let res = parseBackendMessage(buf)
    check res.message.columns[0].get.len == 1000

  test "DataRow with negative column length raises ProtocolError":
    var body: seq[byte] = @[]
    body.addInt16(1)
    body.addInt32(-2) # invalid: only -1 (NULL) is valid
    var buf = buildMsg('D', body)
    expect ProtocolError:
      discard parseBackendMessage(buf)

  test "unknown message type raises ProtocolError":
    var buf = buildMsg('?', @[0'u8])
    expect ProtocolError:
      discard parseBackendMessage(buf)

  test "unknown authentication type raises ProtocolError":
    var body: seq[byte] = @[]
    body.addInt32(99) # unknown auth type
    var buf = buildMsg('R', body)
    expect ProtocolError:
      discard parseBackendMessage(buf)

  test "invalid message length raises ProtocolError":
    # length < 4 is invalid
    var buf = @[byte('Z'), 0'u8, 0, 0, 3, byte('I')]
    expect ProtocolError:
      discard parseBackendMessage(buf)

  test "oversized message length raises ProtocolError before allocation":
    # Header advertises a near-int32-max body; without a cap this would
    # cause the recv loop to grow recvBuf until OOM. The parser must
    # reject such headers before any further read.
    var buf = @[byte('D'), 0x7F'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8] # msgLen = int32.high
    expect ProtocolError:
      discard parseBackendMessage(buf)

  test "configurable maxLen rejects header above caller's threshold":
    # 100-byte body declared, but caller permits only 32 bytes total.
    var buf = @[byte('C'), 0'u8, 0, 0, 100'u8]
    var consumed: int
    expect ProtocolError:
      discard parseBackendMessage(buf, consumed, nil, 32)

  test "maxLen permits message exactly at the limit":
    # Build a CommandComplete ("C") of total size = 1 + 4 + 4 = 9 bytes.
    var body: seq[byte] = @[]
    body.addCString("OK")
    var buf = buildMsg('C', body)
    var consumed: int
    let res = parseBackendMessage(buf, consumed, nil, buf.len)
    check res.state == psComplete
    check consumed == buf.len

  test "SASL with multiple mechanisms":
    var body: seq[byte] = @[]
    body.addInt32(10)
    body.addCString("SCRAM-SHA-256")
    body.addCString("SCRAM-SHA-512")
    body.add(0'u8)
    var buf = buildMsg('R', body)
    let res = parseBackendMessage(buf)
    check res.message.saslMechanisms == @["SCRAM-SHA-256", "SCRAM-SHA-512"]

  test "NotificationResponse with empty payload":
    var body: seq[byte] = @[]
    body.addInt32(55)
    body.addCString("channel")
    body.addCString("")
    var buf = buildMsg('A', body)
    let res = parseBackendMessage(buf)
    check res.message.notifChannel == "channel"
    check res.message.notifPayload == ""

  test "ErrorResponse with all standard fields":
    var body: seq[byte] = @[]
    body.add(byte('S'))
    body.addCString("ERROR")
    body.add(byte('V'))
    body.addCString("ERROR")
    body.add(byte('C'))
    body.addCString("23505")
    body.add(byte('M'))
    body.addCString("duplicate key")
    body.add(byte('D'))
    body.addCString("Key (id)=(1) already exists")
    body.add(byte('H'))
    body.addCString("Delete the existing row first")
    body.add(byte('W'))
    body.addCString("somewhere")
    body.add(byte('F'))
    body.addCString("nbtinsert.c")
    body.add(byte('L'))
    body.addCString("666")
    body.add(byte('R'))
    body.addCString("_bt_check_unique")
    body.add(0'u8)
    var buf = buildMsg('E', body)
    let res = parseBackendMessage(buf)
    check res.message.errorFields.len == 10
    check getErrorField(res.message.errorFields, 'C') == "23505"
    check getErrorField(res.message.errorFields, 'D') == "Key (id)=(1) already exists"
    check getErrorField(res.message.errorFields, 'F') == "nbtinsert.c"

  test "CommandComplete various tags":
    for tag in ["SELECT 100", "INSERT 0 5", "UPDATE 3", "DELETE 1", "CREATE TABLE"]:
      var body: seq[byte] = @[]
      body.addCString(tag)
      var buf = buildMsg('C', body)
      let res = parseBackendMessage(buf)
      check res.message.commandTag == tag

  test "CopyInResponse binary with multiple columns":
    var body: seq[byte] = @[]
    body.add(1'u8) # binary
    body.addInt16(3)
    body.addInt16(1)
    body.addInt16(0)
    body.addInt16(1)
    var buf = buildMsg('G', body)
    let res = parseBackendMessage(buf)
    check res.message.copyFormat == cfBinary
    check res.message.copyColumnFormats == @[1'i16, 0, 1]

  test "buffer preserved on incomplete parse":
    var buf = @[byte('Z'), 0'u8, 0, 0, 5]
    # Need 6 total bytes (1 type + 4 len + 1 body), only have 5
    let original = buf
    let res = parseBackendMessage(buf)
    check res.state == psIncomplete
    check buf == original

suite "Frontend encoding - edge cases":
  test "encodeBind with result formats":
    let msg = encodeBind("", "", @[0'i16], @[some(@[byte('1')])], @[1'i16])
    check msg[0] == byte('B')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

  test "encodeParse with no param types":
    let msg = encodeParse("", "SELECT 1")
    check msg[0] == byte('P')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

  test "encodeStartup with empty database":
    let msg = encodeStartup("user", "")
    let s = cast[string](msg[8 .. ^1])
    check "user" in s
    check "database" notin s

  test "encodeDescribe portal vs statement":
    let stmtMsg = encodeDescribe(dkStatement, "s1")
    let portalMsg = encodeDescribe(dkPortal, "p1")
    check stmtMsg[5] == byte('S')
    check portalMsg[5] == byte('P')

  test "encodeCopyFail with empty message":
    let msg = encodeCopyFail("")
    check msg[0] == byte('f')
    check decodeInt32(msg, 1) == int32(msg.len - 1)

suite "nextMessage recvBufStart update":
  proc buildMsg(msgType: char, body: seq[byte]): seq[byte] =
    result = @[byte(msgType)]
    result.addInt32(int32(4 + body.len))
    result.add(body)

  proc buildDataRowMsg(values: openArray[string]): seq[byte] =
    var body: seq[byte] = @[]
    body.addInt16(int16(values.len))
    for v in values:
      body.addInt32(int32(v.len))
      for c in v:
        body.add(byte(c))
    buildMsg('D', body)

  proc mockConn(): PgConnection =
    PgConnection(
      recvBuf: @[],
      recvBufStart: 0,
      state: csReady,
      txStatus: tsIdle,
      serverParams: initTable[string, string](),
      createdAt: Moment.now(),
    )

  test "recvBufStart advances past DataRows on exception":
    var conn = mockConn()
    let validRow = buildDataRowMsg(["hello"])
    # Build a malformed DataRow: claims 1 column but body is truncated
    var badBody: seq[byte] = @[]
    badBody.addInt16(1)
    badBody.addInt32(-2) # invalid column length
    let badRow = buildMsg('D', badBody)

    conn.recvBuf = validRow & badRow
    conn.recvBufStart = 0

    var rd = newRowData(1)
    var count: int32 = 0
    expect ProtocolError:
      discard conn.nextMessage(rd, addr count)

    # recvBufStart should have advanced past the valid DataRow
    check conn.recvBufStart == validRow.len

  test "recvBufStart advances past multiple DataRows before non-DataRow":
    var conn = mockConn()
    let row1 = buildDataRowMsg(["a"])
    let row2 = buildDataRowMsg(["bb"])
    # CommandComplete: 'C' with tag "SELECT 2\0"
    var ccBody: seq[byte] = @[]
    for c in "SELECT 2":
      ccBody.add(byte(c))
    ccBody.add(0'u8)
    let cc = buildMsg('C', ccBody)

    conn.recvBuf = row1 & row2 & cc
    conn.recvBufStart = 0

    var rd = newRowData(1)
    var count: int32 = 0
    let opt = conn.nextMessage(rd, addr count)

    check opt.isSome
    check opt.get.kind == bmkCommandComplete
    check count == 2
    check conn.recvBufStart == row1.len + row2.len + cc.len
