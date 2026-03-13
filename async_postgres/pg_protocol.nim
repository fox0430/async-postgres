import std/options

type
  ProtocolError* = object of CatchableError

  FrontendMessageKind* = enum
    fmkStartup
    fmkSSLRequest
    fmkPassword
    fmkSASLInitialResponse
    fmkSASLResponse
    fmkQuery
    fmkParse
    fmkBind
    fmkDescribe
    fmkExecute
    fmkClose
    fmkSync
    fmkFlush
    fmkTerminate
    fmkCopyData
    fmkCopyDone
    fmkCopyFail

  BackendMessageKind* = enum
    bmkAuthenticationOk
    bmkAuthenticationCleartextPassword
    bmkAuthenticationMD5Password
    bmkAuthenticationSASL
    bmkAuthenticationSASLContinue
    bmkAuthenticationSASLFinal
    bmkBackendKeyData
    bmkBindComplete
    bmkCloseComplete
    bmkCommandComplete
    bmkCopyInResponse
    bmkCopyOutResponse
    bmkCopyData
    bmkCopyDone
    bmkDataRow
    bmkEmptyQueryResponse
    bmkErrorResponse
    bmkNoData
    bmkNoticeResponse
    bmkNotificationResponse
    bmkParameterDescription
    bmkParameterStatus
    bmkParseComplete
    bmkPortalSuspended
    bmkReadyForQuery
    bmkRowDescription

  DescribeKind* = enum
    dkPortal = 'P'
    dkStatement = 'S'

  TransactionStatus* = enum
    tsInFailedTransaction = 'E'
    tsIdle = 'I'
    tsInTransaction = 'T'

  ErrorField* = object
    code*: char
    value*: string

  FieldDescription* = object
    name*: string
    tableOid*: int32
    columnAttrNum*: int16
    typeOid*: int32
    typeSize*: int16
    typeMod*: int32
    formatCode*: int16

  CopyFormat* = enum
    cfText = 0
    cfBinary = 1

  BackendMessage* = object
    case kind*: BackendMessageKind
    of bmkAuthenticationOk, bmkAuthenticationCleartextPassword:
      discard
    of bmkAuthenticationMD5Password:
      md5Salt*: array[4, byte]
    of bmkAuthenticationSASL:
      saslMechanisms*: seq[string]
    of bmkAuthenticationSASLContinue:
      saslData*: seq[byte]
    of bmkAuthenticationSASLFinal:
      saslFinalData*: seq[byte]
    of bmkBackendKeyData:
      backendPid*: int32
      backendSecretKey*: int32
    of bmkBindComplete, bmkCloseComplete, bmkNoData, bmkEmptyQueryResponse,
        bmkParseComplete, bmkPortalSuspended, bmkCopyDone:
      discard
    of bmkCommandComplete:
      commandTag*: string
    of bmkCopyInResponse, bmkCopyOutResponse:
      copyFormat*: CopyFormat
      copyColumnFormats*: seq[int16]
    of bmkCopyData:
      copyData*: seq[byte]
    of bmkDataRow:
      columns*: seq[Option[seq[byte]]]
    of bmkErrorResponse:
      errorFields*: seq[ErrorField]
    of bmkNoticeResponse:
      noticeFields*: seq[ErrorField]
    of bmkNotificationResponse:
      notifPid*: int32
      notifChannel*: string
      notifPayload*: string
    of bmkParameterDescription:
      paramTypeOids*: seq[int32]
    of bmkParameterStatus:
      paramName*: string
      paramValue*: string
    of bmkReadyForQuery:
      txStatus*: TransactionStatus
    of bmkRowDescription:
      fields*: seq[FieldDescription]

  ParseState* = enum
    psComplete
    psIncomplete

  ParseResult* = object
    state*: ParseState
    message*: BackendMessage

  RowData* = ref object
    buf*: seq[byte] ## All column data concatenated
    cellIndex*: seq[int32] ## [off, len, off, len, ...] per cell; len=-1 = NULL
    numCols*: int16

  Row* = object
    data*: RowData
    rowIdx*: int32

# Byte-level helpers

proc encodeInt16*(val: int16): array[2, byte] =
  result[0] = byte((val shr 8) and 0xFF)
  result[1] = byte(val and 0xFF)

proc encodeInt32*(val: int32): array[4, byte] =
  result[0] = byte((val shr 24) and 0xFF)
  result[1] = byte((val shr 16) and 0xFF)
  result[2] = byte((val shr 8) and 0xFF)
  result[3] = byte(val and 0xFF)

proc addInt16*(buf: var seq[byte], val: int16) =
  let encoded = encodeInt16(val)
  buf.add(encoded[0])
  buf.add(encoded[1])

proc addInt32*(buf: var seq[byte], val: int32) =
  let encoded = encodeInt32(val)
  buf.add(encoded[0])
  buf.add(encoded[1])
  buf.add(encoded[2])
  buf.add(encoded[3])

proc addCString*(buf: var seq[byte], s: string) =
  for c in s:
    buf.add(byte(c))
  buf.add(0'u8)

proc decodeInt16*(buf: openArray[byte], offset: int): int16 =
  result = int16(buf[offset]) shl 8 or int16(buf[offset + 1])

proc decodeInt32*(buf: openArray[byte], offset: int): int32 =
  result =
    int32(buf[offset]) shl 24 or int32(buf[offset + 1]) shl 16 or
    int32(buf[offset + 2]) shl 8 or int32(buf[offset + 3])

proc decodeCString*(buf: openArray[byte], offset: int): (string, int) =
  var s = ""
  var i = offset
  while i < buf.len and buf[i] != 0:
    s.add(char(buf[i]))
    inc i
  if i < buf.len:
    inc i # skip null terminator
  result = (s, i - offset)

# Frontend message encoding

proc encodeStartup*(
    user: string, database: string, extraParams: openArray[(string, string)] = []
): seq[byte] =
  result.addInt32(0) # length placeholder
  result.addInt32(196608) # protocol version 3.0
  result.addCString("user")
  result.addCString(user)
  if database.len > 0:
    result.addCString("database")
    result.addCString(database)
  for (k, v) in extraParams:
    result.addCString(k)
    result.addCString(v)
  result.add(0'u8) # terminator
  let length = int32(result.len)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i] = e

proc encodeSSLRequest*(): seq[byte] =
  result = newSeq[byte](8)
  let lenBytes = encodeInt32(8)
  let magicBytes = encodeInt32(80877103)
  for i, lb in lenBytes:
    result[i] = lb
  for i, mb in magicBytes:
    result[i + 4] = mb

proc encodePassword*(password: string): seq[byte] =
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.addCString(password)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeSASLInitialResponse*(mechanism: string, data: seq[byte]): seq[byte] =
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.addCString(mechanism)
  result.addInt32(int32(data.len))
  result.add(data)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeSASLResponse*(data: seq[byte]): seq[byte] =
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.add(data)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeQuery*(sql: string): seq[byte] =
  result.add(byte('Q'))
  result.addInt32(0) # length placeholder
  result.addCString(sql)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeParse*(
    stmtName: string, sql: string, paramTypeOids: openArray[int32] = []
): seq[byte] =
  result.add(byte('P'))
  result.addInt32(0) # length placeholder
  result.addCString(stmtName)
  result.addCString(sql)
  result.addInt16(int16(paramTypeOids.len))
  for oid in paramTypeOids:
    result.addInt32(oid)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeBind*(
    portalName: string,
    stmtName: string,
    paramFormats: openArray[int16],
    paramValues: openArray[Option[seq[byte]]],
    resultFormats: openArray[int16] = [],
): seq[byte] =
  result.add(byte('B'))
  result.addInt32(0) # length placeholder
  result.addCString(portalName)
  result.addCString(stmtName)
  # Parameter format codes
  result.addInt16(int16(paramFormats.len))
  for f in paramFormats:
    result.addInt16(f)
  # Parameter values
  result.addInt16(int16(paramValues.len))
  for v in paramValues:
    if v.isNone:
      result.addInt32(-1) # NULL
    else:
      let data = v.get
      result.addInt32(int32(data.len))
      result.add(data)
  # Result format codes
  result.addInt16(int16(resultFormats.len))
  for f in resultFormats:
    result.addInt16(f)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeDescribe*(kind: DescribeKind, name: string): seq[byte] =
  result.add(byte('D'))
  result.addInt32(0) # length placeholder
  result.add(byte(kind))
  result.addCString(name)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeExecute*(portalName: string, maxRows: int32 = 0): seq[byte] =
  result.add(byte('E'))
  result.addInt32(0) # length placeholder
  result.addCString(portalName)
  result.addInt32(maxRows)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeClose*(kind: DescribeKind, name: string): seq[byte] =
  result.add(byte('C'))
  result.addInt32(0) # length placeholder
  result.add(byte(kind))
  result.addCString(name)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

proc encodeSync*(): seq[byte] =
  result = @[byte('S'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeFlush*(): seq[byte] =
  result = @[byte('H'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeTerminate*(): seq[byte] =
  result = @[byte('X'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeCancelRequest*(pid: int32, secretKey: int32): seq[byte] =
  result = newSeq[byte](16)

  let lenBytes = encodeInt32(16)
  for i, lb in lenBytes:
    result[i] = lb

  let magicBytes = encodeInt32(80877102)
  for i, mb in magicBytes:
    result[i + 4] = mb

  let pidBytes = encodeInt32(pid)
  for i, pb in pidBytes:
    result[i + 8] = pb

  let keyBytes = encodeInt32(secretKey)
  for i, kb in keyBytes:
    result[i + 12] = kb

proc encodeCopyData*(buf: var seq[byte], data: seq[byte]) =
  ## Encode a CopyData message, appending to `buf`.
  buf.add(byte('d'))
  buf.addInt32(int32(4 + data.len))
  buf.add(data)

proc encodeCopyDone*(): seq[byte] =
  result = @[byte('c'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeCopyFail*(errorMsg: string): seq[byte] =
  result.add(byte('f'))
  result.addInt32(0) # length placeholder
  result.addCString(errorMsg)
  let length = int32(result.high)
  let encoded = encodeInt32(length)
  for i, e in encoded:
    result[i + 1] = e

# Backend message parsing (internal helpers)

proc parseAuthentication(body: openArray[byte]): BackendMessage =
  if body.len < 4:
    raise newException(ProtocolError, "Authentication message too short")

  let authType = decodeInt32(body, 0)
  case authType
  of 0:
    result = BackendMessage(kind: bmkAuthenticationOk)
  of 3:
    result = BackendMessage(kind: bmkAuthenticationCleartextPassword)
  of 5:
    if body.len < 8:
      raise newException(ProtocolError, "MD5 auth message too short")
    result = BackendMessage(kind: bmkAuthenticationMD5Password)
    for i, b in body[4 .. 7]:
      result.md5Salt[i] = b
  of 10:
    # SASL
    result = BackendMessage(kind: bmkAuthenticationSASL)
    result.saslMechanisms = @[]
    var offset = 4
    while offset < body.len:
      let (mechanism, consumed) = decodeCString(body, offset)
      offset += consumed
      if mechanism.len == 0:
        break
      result.saslMechanisms.add(mechanism)
  of 11:
    # SASLContinue
    result = BackendMessage(kind: bmkAuthenticationSASLContinue)
    result.saslData = @(body.toOpenArray(4, body.len - 1))
  of 12:
    # SASLFinal
    result = BackendMessage(kind: bmkAuthenticationSASLFinal)
    result.saslFinalData = @(body.toOpenArray(4, body.len - 1))
  else:
    raise newException(ProtocolError, "Unknown authentication type: " & $authType)

proc parseBackendKeyData(body: openArray[byte]): BackendMessage =
  if body.len < 8:
    raise newException(ProtocolError, "BackendKeyData message too short")
  result = BackendMessage(kind: bmkBackendKeyData)
  result.backendPid = decodeInt32(body, 0)
  result.backendSecretKey = decodeInt32(body, 4)

proc parseCommandComplete(body: openArray[byte]): BackendMessage =
  result = BackendMessage(kind: bmkCommandComplete)
  let (tag, _) = decodeCString(body, 0)
  result.commandTag = tag

proc parseDataRow(body: openArray[byte]): BackendMessage =
  if body.len < 2:
    raise newException(ProtocolError, "DataRow message too short")
  result = BackendMessage(kind: bmkDataRow)
  let numCols = decodeInt16(body, 0)
  result.columns = newSeq[Option[seq[byte]]](numCols)
  var offset = 2
  for i in 0 ..< numCols:
    if offset + 4 > body.len:
      raise newException(ProtocolError, "DataRow: unexpected end of data")
    let colLen = decodeInt32(body, offset)
    offset += 4
    if colLen == -1:
      result.columns[i] = none(seq[byte])
    else:
      if offset + colLen > body.len:
        raise newException(ProtocolError, "DataRow: column data truncated")
      result.columns[i] = some(@(body.toOpenArray(offset, offset + colLen - 1)))
      offset += colLen

proc parseErrorOrNotice(body: openArray[byte], isError: bool): BackendMessage =
  if isError:
    result = BackendMessage(kind: bmkErrorResponse)
    result.errorFields = @[]
  else:
    result = BackendMessage(kind: bmkNoticeResponse)
    result.noticeFields = @[]
  var offset = 0
  while offset < body.len:
    let fieldType = char(body[offset])
    inc offset
    if fieldType == '\0':
      break
    let (value, consumed) = decodeCString(body, offset)
    offset += consumed
    let field = ErrorField(code: fieldType, value: value)
    if isError:
      result.errorFields.add(field)
    else:
      result.noticeFields.add(field)

proc parseNotification(body: openArray[byte]): BackendMessage =
  if body.len < 4:
    raise newException(ProtocolError, "Notification message too short")
  result = BackendMessage(kind: bmkNotificationResponse)
  result.notifPid = decodeInt32(body, 0)
  var offset = 4
  let (channel, consumed1) = decodeCString(body, offset)
  result.notifChannel = channel
  offset += consumed1
  let (payload, _) = decodeCString(body, offset)
  result.notifPayload = payload

proc parseParameterStatus(body: openArray[byte]): BackendMessage =
  result = BackendMessage(kind: bmkParameterStatus)
  let (name, consumed) = decodeCString(body, 0)
  result.paramName = name
  let (value, _) = decodeCString(body, consumed)
  result.paramValue = value

proc parseRowDescription(body: openArray[byte]): BackendMessage =
  if body.len < 2:
    raise newException(ProtocolError, "RowDescription message too short")
  result = BackendMessage(kind: bmkRowDescription)
  let numFields = decodeInt16(body, 0)
  result.fields = newSeq[FieldDescription](numFields)
  var offset = 2
  for i in 0 ..< numFields:
    let (name, consumed) = decodeCString(body, offset)
    offset += consumed
    if offset + 18 > body.len:
      raise newException(ProtocolError, "RowDescription: unexpected end of data")
    result.fields[i] = FieldDescription(
      name: name,
      tableOid: decodeInt32(body, offset),
      columnAttrNum: decodeInt16(body, offset + 4),
      typeOid: decodeInt32(body, offset + 6),
      typeSize: decodeInt16(body, offset + 10),
      typeMod: decodeInt32(body, offset + 12),
      formatCode: decodeInt16(body, offset + 16),
    )
    offset += 18

proc parseReadyForQuery(body: openArray[byte]): BackendMessage =
  if body.len < 1:
    raise newException(ProtocolError, "ReadyForQuery message too short")
  result = BackendMessage(kind: bmkReadyForQuery)
  case char(body[0])
  of 'I':
    result.txStatus = tsIdle
  of 'T':
    result.txStatus = tsInTransaction
  of 'E':
    result.txStatus = tsInFailedTransaction
  else:
    raise newException(ProtocolError, "Unknown transaction status: " & $char(body[0]))

proc parseParameterDescription(body: openArray[byte]): BackendMessage =
  if body.len < 2:
    raise newException(ProtocolError, "ParameterDescription too short")
  result = BackendMessage(kind: bmkParameterDescription)
  let numParams = decodeInt16(body, 0)
  result.paramTypeOids = newSeq[int32](numParams)
  var offset = 2
  for i in 0 ..< numParams:
    if offset + 4 > body.len:
      raise newException(ProtocolError, "ParameterDescription truncated")
    result.paramTypeOids[i] = decodeInt32(body, offset)
    offset += 4

proc parseCopyResponse(body: openArray[byte], isIn: bool): BackendMessage =
  if body.len < 3:
    raise newException(ProtocolError, "CopyResponse message too short")
  if isIn:
    result = BackendMessage(kind: bmkCopyInResponse)
  else:
    result = BackendMessage(kind: bmkCopyOutResponse)
  result.copyFormat = if body[0] == 0: cfText else: cfBinary
  let numCols = decodeInt16(body, 1)
  result.copyColumnFormats = newSeq[int16](numCols)
  var offset = 3
  for i in 0 ..< numCols:
    if offset + 2 > body.len:
      raise newException(ProtocolError, "CopyResponse truncated")
    result.copyColumnFormats[i] = decodeInt16(body, offset)
    offset += 2

proc newRowData*(numCols: int16): RowData =
  RowData(buf: @[], cellIndex: @[], numCols: numCols)

proc parseDataRowInto*(body: openArray[byte], rd: RowData) =
  ## Parse a DataRow message body directly into a RowData flat buffer.
  ## Column data is appended to rd.buf and (offset, length) pairs to rd.cellIndex.
  if body.len < 2:
    raise newException(ProtocolError, "DataRow message too short")
  let numCols = decodeInt16(body, 0)
  # Pre-extend cellIndex for this row to avoid per-column dynamic growth
  let cellBase = rd.cellIndex.len
  rd.cellIndex.setLen(cellBase + int(numCols) * 2)
  var offset = 2
  for i in 0 ..< numCols:
    if offset + 4 > body.len:
      rd.cellIndex.setLen(cellBase) # rollback on error
      raise newException(ProtocolError, "DataRow: unexpected end of data")
    let colLen = decodeInt32(body, offset)
    offset += 4
    let ci = cellBase + int(i) * 2
    if colLen == -1:
      rd.cellIndex[ci] = 0'i32
      rd.cellIndex[ci + 1] = -1'i32
    else:
      if offset + colLen > body.len:
        rd.cellIndex.setLen(cellBase) # rollback on error
        raise newException(ProtocolError, "DataRow: column data truncated")
      let bufOff = int32(rd.buf.len)
      let oldLen = rd.buf.len
      rd.buf.setLen(oldLen + int(colLen))
      if colLen > 0:
        copyMem(addr rd.buf[oldLen], unsafeAddr body[offset], int(colLen))
      rd.cellIndex[ci] = bufOff
      rd.cellIndex[ci + 1] = colLen
      offset += colLen

# Streaming backend message parser

proc parseBackendMessage*(
    buf: openArray[byte], consumed: var int, rowData: RowData = nil
): ParseResult =
  ## Parse a single backend message from `buf`.
  ## On success, sets `consumed` to the number of bytes used.
  ## The caller is responsible for discarding those bytes from the buffer.
  consumed = 0

  # Need at least 5 bytes: 1 type + 4 length
  if buf.len < 5:
    return ParseResult(state: psIncomplete)

  let msgType = char(buf[0])
  let msgLen = decodeInt32(buf, 1) # includes self but not type byte

  if msgLen < 4:
    raise newException(ProtocolError, "Invalid message length: " & $msgLen)

  let totalLen = int(msgLen) + 1 # type byte + length field + body
  if buf.len < totalLen:
    return ParseResult(state: psIncomplete)

  # Body is the region after type byte and 4-byte length
  let bodyStart = 5
  let bodyEnd = bodyStart + int(msgLen) - 4 - 1
  template body(): untyped =
    buf.toOpenArray(bodyStart, bodyEnd)

  var msg: BackendMessage

  case msgType
  of 'R':
    msg = parseAuthentication(body)
  of 'K':
    msg = parseBackendKeyData(body)
  of 'C':
    msg = parseCommandComplete(body)
  of 'D':
    if rowData != nil:
      parseDataRowInto(body, rowData)
      msg = BackendMessage(kind: bmkDataRow)
    else:
      msg = parseDataRow(body)
  of 'E':
    msg = parseErrorOrNotice(body, isError = true)
  of 'N':
    msg = parseErrorOrNotice(body, isError = false)
  of 'A':
    msg = parseNotification(body)
  of 'S':
    msg = parseParameterStatus(body)
  of 'T':
    msg = parseRowDescription(body)
  of 'Z':
    msg = parseReadyForQuery(body)
  of 't':
    msg = parseParameterDescription(body)
  of '1':
    msg = BackendMessage(kind: bmkParseComplete)
  of '2':
    msg = BackendMessage(kind: bmkBindComplete)
  of '3':
    msg = BackendMessage(kind: bmkCloseComplete)
  of 'I':
    msg = BackendMessage(kind: bmkEmptyQueryResponse)
  of 'n':
    msg = BackendMessage(kind: bmkNoData)
  of 's':
    msg = BackendMessage(kind: bmkPortalSuspended)
  of 'G':
    msg = parseCopyResponse(body, isIn = true)
  of 'H':
    msg = parseCopyResponse(body, isIn = false)
  of 'd':
    msg = BackendMessage(kind: bmkCopyData)
    msg.copyData = @(body)
  of 'c':
    msg = BackendMessage(kind: bmkCopyDone)
  else:
    raise newException(ProtocolError, "Unknown backend message type: " & msgType)

  consumed = totalLen
  result = ParseResult(state: psComplete, message: msg)

# Utility

proc getErrorField*(fields: seq[ErrorField], code: char): string =
  for f in fields:
    if f.code == code:
      return f.value
  return ""

proc formatError*(fields: seq[ErrorField]): string =
  let severity = getErrorField(fields, 'S')
  let code = getErrorField(fields, 'C')
  let message = getErrorField(fields, 'M')
  let detail = getErrorField(fields, 'D')
  let hint = getErrorField(fields, 'H')
  result = severity & ": " & message
  if code.len > 0:
    result.add(" (SQLSTATE " & code & ")")
  if detail.len > 0:
    result.add("\nDETAIL: " & detail)
  if hint.len > 0:
    result.add("\nHINT: " & hint)
