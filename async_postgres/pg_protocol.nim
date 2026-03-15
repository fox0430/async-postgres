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
    psDataRow ## DataRow parsed in-place into RowData; no BackendMessage constructed

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

proc addInt16*(buf: var seq[byte], val: int16) {.inline.} =
  let oldLen = buf.len
  buf.setLen(oldLen + 2)
  buf[oldLen] = byte((val shr 8) and 0xFF)
  buf[oldLen + 1] = byte(val and 0xFF)

proc addInt32*(buf: var seq[byte], val: int32) {.inline.} =
  let oldLen = buf.len
  buf.setLen(oldLen + 4)
  buf[oldLen] = byte((val shr 24) and 0xFF)
  buf[oldLen + 1] = byte((val shr 16) and 0xFF)
  buf[oldLen + 2] = byte((val shr 8) and 0xFF)
  buf[oldLen + 3] = byte(val and 0xFF)

proc patchLen*(buf: var seq[byte], offset: int = 1) =
  ## Patch the length placeholder at `offset` with buf.len minus the tag byte.
  let length = int32(buf.high)
  buf[offset] = byte((length shr 24) and 0xFF)
  buf[offset + 1] = byte((length shr 16) and 0xFF)
  buf[offset + 2] = byte((length shr 8) and 0xFF)
  buf[offset + 3] = byte(length and 0xFF)

proc patchMsgLen*(buf: var seq[byte], msgStart: int) {.inline.} =
  ## Patch the length field of a message starting at `msgStart`.
  ## Length = total message size minus the type byte.
  let length = int32(buf.len - msgStart - 1)
  buf[msgStart + 1] = byte((length shr 24) and 0xFF)
  buf[msgStart + 2] = byte((length shr 16) and 0xFF)
  buf[msgStart + 3] = byte((length shr 8) and 0xFF)
  buf[msgStart + 4] = byte(length and 0xFF)

proc addCString*(buf: var seq[byte], s: string) =
  let oldLen = buf.len
  buf.setLen(oldLen + s.len + 1)
  if s.len > 0:
    copyMem(addr buf[oldLen], unsafeAddr s[0], s.len)
  buf[oldLen + s.len] = 0'u8

proc decodeInt16*(buf: openArray[byte], offset: int): int16 =
  result = int16(buf[offset]) shl 8 or int16(buf[offset + 1])

proc decodeInt32*(buf: openArray[byte], offset: int): int32 =
  result =
    int32(buf[offset]) shl 24 or int32(buf[offset + 1]) shl 16 or
    int32(buf[offset + 2]) shl 8 or int32(buf[offset + 3])

proc decodeCString*(buf: openArray[byte], offset: int): (string, int) =
  var i = offset
  while i < buf.len and buf[i] != 0:
    inc i
  let slen = i - offset
  var s = newString(slen)
  if slen > 0:
    copyMem(addr s[0], unsafeAddr buf[offset], slen)
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
  result[0] = encoded[0]
  result[1] = encoded[1]
  result[2] = encoded[2]
  result[3] = encoded[3]

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
  result.patchLen()

proc encodeSASLInitialResponse*(mechanism: string, data: seq[byte]): seq[byte] =
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.addCString(mechanism)
  result.addInt32(int32(data.len))
  result.add(data)
  result.patchLen()

proc encodeSASLResponse*(data: seq[byte]): seq[byte] =
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.add(data)
  result.patchLen()

proc encodeQuery*(sql: string): seq[byte] =
  result.add(byte('Q'))
  result.addInt32(0) # length placeholder
  result.addCString(sql)
  result.patchLen()

# In-place frontend message encoding (append directly to batch buffer)

const
  syncMsg* = [byte('S'), 0'u8, 0'u8, 0'u8, 4'u8]
  flushMsg* = [byte('H'), 0'u8, 0'u8, 0'u8, 4'u8]
  copyDoneMsg* = [byte('c'), 0'u8, 0'u8, 0'u8, 4'u8]

proc addFixedMsg(buf: var seq[byte], msg: array[5, byte]) {.inline.} =
  let oldLen = buf.len
  buf.setLen(oldLen + 5)
  copyMem(addr buf[oldLen], unsafeAddr msg[0], 5)

proc addParse*(
    buf: var seq[byte],
    stmtName: string,
    sql: string,
    paramTypeOids: openArray[int32] = [],
) =
  let msgStart = buf.len
  buf.add(byte('P'))
  buf.addInt32(0) # length placeholder
  buf.addCString(stmtName)
  buf.addCString(sql)
  buf.addInt16(int16(paramTypeOids.len))
  for oid in paramTypeOids:
    buf.addInt32(oid)
  buf.patchMsgLen(msgStart)

proc addBind*(
    buf: var seq[byte],
    portalName: string,
    stmtName: string,
    paramFormats: openArray[int16],
    paramValues: openArray[Option[seq[byte]]],
    resultFormats: openArray[int16] = [],
) =
  let msgStart = buf.len
  buf.add(byte('B'))
  buf.addInt32(0) # length placeholder
  buf.addCString(portalName)
  buf.addCString(stmtName)
  # Parameter format codes
  buf.addInt16(int16(paramFormats.len))
  for f in paramFormats:
    buf.addInt16(f)
  # Parameter values
  buf.addInt16(int16(paramValues.len))
  for v in paramValues:
    if v.isNone:
      buf.addInt32(-1) # NULL
    else:
      let data = v.get
      buf.addInt32(int32(data.len))
      if data.len > 0:
        let oldLen = buf.len
        buf.setLen(oldLen + data.len)
        copyMem(addr buf[oldLen], unsafeAddr data[0], data.len)
  # Result format codes
  buf.addInt16(int16(resultFormats.len))
  for f in resultFormats:
    buf.addInt16(f)
  buf.patchMsgLen(msgStart)

proc addDescribe*(buf: var seq[byte], kind: DescribeKind, name: string) =
  let msgStart = buf.len
  buf.add(byte('D'))
  buf.addInt32(0) # length placeholder
  buf.add(byte(kind))
  buf.addCString(name)
  buf.patchMsgLen(msgStart)

proc addExecute*(buf: var seq[byte], portalName: string, maxRows: int32 = 0) =
  let msgStart = buf.len
  buf.add(byte('E'))
  buf.addInt32(0) # length placeholder
  buf.addCString(portalName)
  buf.addInt32(maxRows)
  buf.patchMsgLen(msgStart)

proc addClose*(buf: var seq[byte], kind: DescribeKind, name: string) =
  let msgStart = buf.len
  buf.add(byte('C'))
  buf.addInt32(0) # length placeholder
  buf.add(byte(kind))
  buf.addCString(name)
  buf.patchMsgLen(msgStart)

proc addSync*(buf: var seq[byte]) {.inline.} =
  buf.addFixedMsg(syncMsg)

proc addFlush*(buf: var seq[byte]) {.inline.} =
  buf.addFixedMsg(flushMsg)

proc addCopyDone*(buf: var seq[byte]) {.inline.} =
  buf.addFixedMsg(copyDoneMsg)

# Wrapper functions that return seq[byte] (for non-batched sends)

proc encodeParse*(
    stmtName: string, sql: string, paramTypeOids: openArray[int32] = []
): seq[byte] =
  result.addParse(stmtName, sql, paramTypeOids)

proc encodeBind*(
    portalName: string,
    stmtName: string,
    paramFormats: openArray[int16],
    paramValues: openArray[Option[seq[byte]]],
    resultFormats: openArray[int16] = [],
): seq[byte] =
  result.addBind(portalName, stmtName, paramFormats, paramValues, resultFormats)

proc encodeDescribe*(kind: DescribeKind, name: string): seq[byte] =
  result.addDescribe(kind, name)

proc encodeExecute*(portalName: string, maxRows: int32 = 0): seq[byte] =
  result.addExecute(portalName, maxRows)

proc encodeClose*(kind: DescribeKind, name: string): seq[byte] =
  result.addClose(kind, name)

proc encodeSync*(): seq[byte] =
  result = @[byte('S'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeFlush*(): seq[byte] =
  result = @[byte('H'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeTerminate*(): seq[byte] =
  result = @[byte('X'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeCancelRequest*(pid: int32, secretKey: int32): seq[byte] =
  result = newSeqOfCap[byte](16)
  result.addInt32(16)
  result.addInt32(80877102)
  result.addInt32(pid)
  result.addInt32(secretKey)

proc encodeCopyData*(buf: var seq[byte], data: openArray[byte]) =
  ## Encode a CopyData message, appending to `buf`.
  ## Single setLen for header + payload to minimize bounds checks.
  let msgLen = int32(4 + data.len)
  let oldLen = buf.len
  buf.setLen(oldLen + 5 + data.len)
  buf[oldLen] = byte('d')
  buf[oldLen + 1] = byte((msgLen shr 24) and 0xFF)
  buf[oldLen + 2] = byte((msgLen shr 16) and 0xFF)
  buf[oldLen + 3] = byte((msgLen shr 8) and 0xFF)
  buf[oldLen + 4] = byte(msgLen and 0xFF)
  if data.len > 0:
    copyMem(addr buf[oldLen + 5], unsafeAddr data[0], data.len)

proc encodeCopyDone*(): seq[byte] =
  result = @[byte('c'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeCopyFail*(errorMsg: string): seq[byte] =
  result.add(byte('f'))
  result.addInt32(0) # length placeholder
  result.addCString(errorMsg)
  result.patchLen()

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
    if colLen < -1:
      raise newException(ProtocolError, "DataRow: invalid column length " & $colLen)
    elif colLen == -1:
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
  # Pre-extend buf with upper bound (remaining body bytes); trim after loop
  let bufBase = rd.buf.len
  let maxDataBytes = body.len - 2 # upper bound on column data
  rd.buf.setLen(bufBase + maxDataBytes)
  var offset = 2
  var bufOff = bufBase
  for i in 0 ..< numCols:
    if offset + 4 > body.len:
      rd.cellIndex.setLen(cellBase)
      rd.buf.setLen(bufBase)
      raise newException(ProtocolError, "DataRow: unexpected end of data")
    let colLen = decodeInt32(body, offset)
    offset += 4
    let ci = cellBase + int(i) * 2
    if colLen < -1:
      rd.cellIndex.setLen(cellBase)
      rd.buf.setLen(bufBase)
      raise newException(ProtocolError, "DataRow: invalid column length " & $colLen)
    elif colLen == -1:
      rd.cellIndex[ci] = 0'i32
      rd.cellIndex[ci + 1] = -1'i32
    else:
      if offset + colLen > body.len:
        rd.cellIndex.setLen(cellBase)
        rd.buf.setLen(bufBase)
        raise newException(ProtocolError, "DataRow: column data truncated")
      rd.cellIndex[ci] = int32(bufOff)
      rd.cellIndex[ci + 1] = colLen
      if colLen > 0:
        copyMem(addr rd.buf[bufOff], unsafeAddr body[offset], int(colLen))
      bufOff += int(colLen)
      offset += colLen
  # Trim buf to actual size used
  rd.buf.setLen(bufOff)

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
      consumed = totalLen
      return ParseResult(state: psDataRow)
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

# Binary COPY format helpers

const
  pgCopyBinaryHeader*: array[19, byte] = [
    byte('P'),
    byte('G'),
    byte('C'),
    byte('O'),
    byte('P'),
    byte('Y'),
    byte('\n'),
    0xFF'u8,
    byte('\r'),
    byte('\n'),
    0x00'u8,
    # flags (int32 = 0)
    0x00'u8,
    0x00'u8,
    0x00'u8,
    0x00'u8,
    # header extension area length (int32 = 0)
    0x00'u8,
    0x00'u8,
    0x00'u8,
    0x00'u8,
  ]
  pgCopyBinaryTrailer*: array[2, byte] = [0xFF'u8, 0xFF'u8] # int16(-1)

proc addCopyBinaryHeader*(buf: var seq[byte]) =
  let oldLen = buf.len
  buf.setLen(oldLen + pgCopyBinaryHeader.len)
  copyMem(addr buf[oldLen], unsafeAddr pgCopyBinaryHeader[0], pgCopyBinaryHeader.len)

proc addCopyBinaryTrailer*(buf: var seq[byte]) =
  let oldLen = buf.len
  buf.setLen(oldLen + 2)
  buf[oldLen] = 0xFF'u8
  buf[oldLen + 1] = 0xFF'u8

proc addCopyTupleStart*(buf: var seq[byte], numCols: int16) =
  buf.addInt16(numCols)

proc addCopyFieldNull*(buf: var seq[byte]) =
  buf.addInt32(-1'i32)

proc addCopyFieldInt16*(buf: var seq[byte], val: int16) =
  buf.addInt32(2'i32)
  buf.addInt16(val)

proc addCopyFieldInt32*(buf: var seq[byte], val: int32) =
  buf.addInt32(4'i32)
  buf.addInt32(val)

proc addCopyFieldInt64*(buf: var seq[byte], val: int64) =
  buf.addInt32(8'i32)
  let oldLen = buf.len
  buf.setLen(oldLen + 8)
  buf[oldLen] = byte((val shr 56) and 0xFF)
  buf[oldLen + 1] = byte((val shr 48) and 0xFF)
  buf[oldLen + 2] = byte((val shr 40) and 0xFF)
  buf[oldLen + 3] = byte((val shr 32) and 0xFF)
  buf[oldLen + 4] = byte((val shr 24) and 0xFF)
  buf[oldLen + 5] = byte((val shr 16) and 0xFF)
  buf[oldLen + 6] = byte((val shr 8) and 0xFF)
  buf[oldLen + 7] = byte(val and 0xFF)

proc addCopyFieldFloat64*(buf: var seq[byte], val: float64) =
  buf.addCopyFieldInt64(cast[int64](val))

proc addCopyFieldFloat32*(buf: var seq[byte], val: float32) =
  buf.addCopyFieldInt32(cast[int32](val))

proc addCopyFieldBool*(buf: var seq[byte], val: bool) =
  buf.addInt32(1'i32)
  buf.add(if val: 1'u8 else: 0'u8)

proc addCopyFieldText*(buf: var seq[byte], val: openArray[byte]) =
  buf.addInt32(int32(val.len))
  if val.len > 0:
    let oldLen = buf.len
    buf.setLen(oldLen + val.len)
    copyMem(addr buf[oldLen], unsafeAddr val[0], val.len)

proc addCopyFieldString*(buf: var seq[byte], val: string) =
  buf.addInt32(int32(val.len))
  if val.len > 0:
    let oldLen = buf.len
    buf.setLen(oldLen + val.len)
    copyMem(addr buf[oldLen], unsafeAddr val[0], val.len)
