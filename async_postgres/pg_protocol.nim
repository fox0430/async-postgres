import std/options

type
  ProtocolError* = object of CatchableError
    ## Raised on PostgreSQL wire protocol violations.

  FrontendMessageKind* = enum
    ## Message types sent from client to server.
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
    ## Message types received from server to client.
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
    ## Target of a Describe or Close message.
    dkPortal = 'P'
    dkStatement = 'S'

  TransactionStatus* = enum
    ## Server transaction state reported in ReadyForQuery.
    tsInFailedTransaction = 'E'
    tsIdle = 'I'
    tsInTransaction = 'T'

  ErrorField* = object
    ## A single field from an ErrorResponse or NoticeResponse message.
    code*: char
    value*: string

  FieldDescription* = object ## Column metadata from a RowDescription message.
    name*: string
    tableOid*: int32
    columnAttrNum*: int16
    typeOid*: int32
    typeSize*: int16
    typeMod*: int32
    formatCode*: int16

  CopyFormat* = enum
    ## Wire format for COPY operations.
    cfText = 0
    cfBinary = 1

  BackendMessage* = object
    ## Parsed message from the PostgreSQL backend. Variant type keyed by `kind`.
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

  ParseResult* = object ## Result of parsing bytes from the receive buffer.
    state*: ParseState
    message*: BackendMessage

  RowData* = ref object ## Flat buffer holding all row data for a query result.
    buf*: seq[byte] ## All column data concatenated
    cellIndex*: seq[int32] ## [off, len, off, len, ...] per cell; len=-1 = NULL
    numCols*: int16
    colFormats*: seq[int16] ## Per-column format codes (0=text, 1=binary)
    colTypeOids*: seq[int32] ## Per-column type OIDs for binary→text conversion
    fields*: seq[FieldDescription] ## Column metadata for name-based access

  Row* = object ## Lightweight view into a single row within a `RowData` buffer.
    data*: RowData
    rowIdx*: int32

const
  syncMsg* = [byte('S'), 0'u8, 0'u8, 0'u8, 4'u8] ## Pre-built Sync message bytes.
  flushMsg* = [byte('H'), 0'u8, 0'u8, 0'u8, 4'u8] ## Pre-built Flush message bytes.
  copyDoneMsg* = [byte('c'), 0'u8, 0'u8, 0'u8, 4'u8] ## Pre-built CopyDone message bytes.

  BinarySafeOids* = [ ## Type OIDs that the statement cache requests in binary format.
    16'i32, # bool
    17, # bytea
    20, # int8 / bigint
    21, # int2 / smallint
    23, # int4 / integer
    25, # text
    600, # point
    601, # lseg
    602, # path
    603, # box
    604, # polygon
    628, # line
    700, # float4
    701, # float8
    718, # circle
    1043, # varchar
    3904, # int4range
    3906, # numrange
    3908, # tsrange
    3910, # tstzrange
    3912, # daterange
    3926, # int8range
    4451, # int4multirange
    4532, # nummultirange
    4533, # tsmultirange
    4534, # tstzmultirange
    4535, # datemultirange
    4536, # int8multirange
  ]

  BinarySafeMaxOid = 4536

  pgCopyBinaryHeader*: array[19, byte] = [
    ## PGCOPY binary format header (signature + flags + extension length).
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
  pgCopyBinaryTrailer*: array[2, byte] = [0xFF'u8, 0xFF'u8]
    ## PGCOPY binary format trailer (int16(-1) sentinel).

func makeBinarySafeLookup(): array[BinarySafeMaxOid + 1, bool] {.compileTime.} =
  for oid in BinarySafeOids:
    result[oid] = true

const binarySafeLookup = makeBinarySafeLookup()

func isBinarySafeOid*(oid: int32): bool =
  ## Check if a type OID can be safely requested in binary format.
  oid >= 0 and oid <= BinarySafeMaxOid and binarySafeLookup[oid]

# Byte-level helpers

proc encodeInt16*(val: int16): array[2, byte] =
  ## Encode a 16-bit integer as big-endian bytes.
  result[0] = byte((val shr 8) and 0xFF)
  result[1] = byte(val and 0xFF)

proc encodeInt32*(val: int32): array[4, byte] =
  ## Encode a 32-bit integer as big-endian bytes.
  result[0] = byte((val shr 24) and 0xFF)
  result[1] = byte((val shr 16) and 0xFF)
  result[2] = byte((val shr 8) and 0xFF)
  result[3] = byte(val and 0xFF)

proc addInt16*(buf: var seq[byte], val: int16) {.inline.} =
  ## Append a 16-bit integer in big-endian format to the buffer.
  let oldLen = buf.len
  buf.setLen(oldLen + 2)
  buf[oldLen] = byte((val shr 8) and 0xFF)
  buf[oldLen + 1] = byte(val and 0xFF)

proc addInt32*(buf: var seq[byte], val: int32) {.inline.} =
  ## Append a 32-bit integer in big-endian format to the buffer.
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
  ## Append a null-terminated C string to the buffer.
  let oldLen = buf.len
  buf.setLen(oldLen + s.len + 1)
  if s.len > 0:
    copyMem(addr buf[oldLen], unsafeAddr s[0], s.len)
  buf[oldLen + s.len] = 0'u8

proc decodeInt16*(buf: openArray[byte], offset: int): int16 =
  ## Decode a 16-bit integer from big-endian bytes at the given offset.
  result = int16(buf[offset]) shl 8 or int16(buf[offset + 1])

proc decodeInt32*(buf: openArray[byte], offset: int): int32 =
  ## Decode a 32-bit integer from big-endian bytes at the given offset.
  result =
    int32(buf[offset]) shl 24 or int32(buf[offset + 1]) shl 16 or
    int32(buf[offset + 2]) shl 8 or int32(buf[offset + 3])

proc decodeCString*(buf: openArray[byte], offset: int): (string, int) =
  ## Decode a null-terminated string at the given offset. Returns (string, bytes consumed).
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
  ## Encode a StartupMessage (protocol v3.0) with user, database, and extra parameters.
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
  ## Encode an SSLRequest message (magic number 80877103).
  result = newSeq[byte](8)
  let lenBytes = encodeInt32(8)
  let magicBytes = encodeInt32(80877103)
  for i, lb in lenBytes:
    result[i] = lb
  for i, mb in magicBytes:
    result[i + 4] = mb

proc encodePassword*(password: string): seq[byte] =
  ## Encode a PasswordMessage for cleartext or MD5 authentication.
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.addCString(password)
  result.patchLen()

proc encodeSASLInitialResponse*(mechanism: string, data: seq[byte]): seq[byte] =
  ## Encode a SASLInitialResponse message with the chosen mechanism and client-first data.
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.addCString(mechanism)
  result.addInt32(int32(data.len))
  result.add(data)
  result.patchLen()

proc encodeSASLResponse*(data: seq[byte]): seq[byte] =
  ## Encode a SASLResponse message with client-final data.
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.add(data)
  result.patchLen()

proc encodeQuery*(sql: string): seq[byte] =
  ## Encode a simple Query message.
  result.add(byte('Q'))
  result.addInt32(0) # length placeholder
  result.addCString(sql)
  result.patchLen()

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
  ## Append a Parse message to the buffer (extended query protocol).
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
  ## Append a Bind message to the buffer (extended query protocol).
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
  ## Append a Describe message to the buffer (portal or statement).
  let msgStart = buf.len
  buf.add(byte('D'))
  buf.addInt32(0) # length placeholder
  buf.add(byte(kind))
  buf.addCString(name)
  buf.patchMsgLen(msgStart)

proc addExecute*(buf: var seq[byte], portalName: string, maxRows: int32 = 0) =
  ## Append an Execute message to the buffer. `maxRows` of 0 means unlimited.
  let msgStart = buf.len
  buf.add(byte('E'))
  buf.addInt32(0) # length placeholder
  buf.addCString(portalName)
  buf.addInt32(maxRows)
  buf.patchMsgLen(msgStart)

proc addClose*(buf: var seq[byte], kind: DescribeKind, name: string) =
  ## Append a Close message to the buffer (portal or statement).
  let msgStart = buf.len
  buf.add(byte('C'))
  buf.addInt32(0) # length placeholder
  buf.add(byte(kind))
  buf.addCString(name)
  buf.patchMsgLen(msgStart)

proc addSync*(buf: var seq[byte]) {.inline.} =
  ## Append a Sync message to the buffer.
  buf.addFixedMsg(syncMsg)

proc addFlush*(buf: var seq[byte]) {.inline.} =
  ## Append a Flush message to the buffer.
  buf.addFixedMsg(flushMsg)

proc addCopyDone*(buf: var seq[byte]) {.inline.} =
  ## Append a CopyDone message to the buffer.
  buf.addFixedMsg(copyDoneMsg)

# Wrapper functions that return seq[byte] (for non-batched sends)

proc encodeParse*(
    stmtName: string, sql: string, paramTypeOids: openArray[int32] = []
): seq[byte] =
  ## Encode a standalone Parse message.
  result.addParse(stmtName, sql, paramTypeOids)

proc encodeBind*(
    portalName: string,
    stmtName: string,
    paramFormats: openArray[int16],
    paramValues: openArray[Option[seq[byte]]],
    resultFormats: openArray[int16] = [],
): seq[byte] =
  ## Encode a standalone Bind message.
  result.addBind(portalName, stmtName, paramFormats, paramValues, resultFormats)

proc encodeDescribe*(kind: DescribeKind, name: string): seq[byte] =
  ## Encode a standalone Describe message.
  result.addDescribe(kind, name)

proc encodeExecute*(portalName: string, maxRows: int32 = 0): seq[byte] =
  ## Encode a standalone Execute message.
  result.addExecute(portalName, maxRows)

proc encodeClose*(kind: DescribeKind, name: string): seq[byte] =
  ## Encode a standalone Close message.
  result.addClose(kind, name)

proc encodeSync*(): seq[byte] =
  ## Encode a standalone Sync message.
  result = @[byte('S'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeFlush*(): seq[byte] =
  ## Encode a standalone Flush message.
  result = @[byte('H'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeTerminate*(): seq[byte] =
  ## Encode a Terminate message to close the connection.
  result = @[byte('X'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeCancelRequest*(pid: int32, secretKey: int32): seq[byte] =
  ## Encode a CancelRequest message to abort a running query.
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
  ## Encode a standalone CopyDone message.
  result = @[byte('c'), 0'u8, 0'u8, 0'u8, 4'u8]

proc encodeCopyFail*(errorMsg: string): seq[byte] =
  ## Encode a CopyFail message to abort a COPY operation with an error.
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

proc newRowData*(
    numCols: int16, colFormats: seq[int16] = @[], colTypeOids: seq[int32] = @[]
): RowData =
  ## Create a new RowData flat buffer for accumulating DataRow messages.
  RowData(
    buf: @[],
    cellIndex: @[],
    numCols: numCols,
    colFormats: colFormats,
    colTypeOids: colTypeOids,
  )

proc reuseRowData*(
    rd: RowData,
    numCols: int16,
    colFormats: sink seq[int16],
    colTypeOids: sink seq[int32],
): RowData =
  ## Create a new RowData that takes over the old buffer's capacity via move.
  ## The old RowData (and any QueryResult still referencing it) is left intact.
  result = RowData(
    buf: move rd.buf,
    cellIndex: move rd.cellIndex,
    numCols: numCols,
    colFormats: colFormats,
    colTypeOids: colTypeOids,
  )
  result.buf.setLen(0)
  result.cellIndex.setLen(0)

proc reuseRowData*(rd: RowData, numCols: int16): RowData =
  ## Create a new RowData that takes over the old buffer's capacity via move,
  ## without format metadata.
  result = RowData(
    buf: move rd.buf,
    cellIndex: move rd.cellIndex,
    numCols: numCols,
    colFormats: move rd.colFormats,
    colTypeOids: move rd.colTypeOids,
  )
  result.buf.setLen(0)
  result.cellIndex.setLen(0)
  result.colFormats.setLen(0)
  result.colTypeOids.setLen(0)

proc buildResultFormats*(fields: openArray[FieldDescription]): seq[int16] =
  ## Build per-column binary format codes: 1 for known safe types, 0 for others.
  result = newSeq[int16](fields.len)
  for i, f in fields:
    result[i] = if isBinarySafeOid(f.typeOid): 1'i16 else: 0'i16

proc parseDataRowInto*(body: openArray[byte], rd: RowData) =
  ## Parse a DataRow message body directly into a RowData flat buffer.
  ## Column data is appended to rd.buf and (offset, length) pairs to rd.cellIndex.
  ## Uses a single bulk copyMem for the entire row payload, then walks the
  ## copied buffer to build cellIndex entries.
  if body.len < 2:
    raise newException(ProtocolError, "DataRow message too short")
  let numCols = decodeInt16(body, 0)
  # Pre-extend cellIndex for this row
  let cellBase = rd.cellIndex.len
  rd.cellIndex.setLen(cellBase + int(numCols) * 2)
  # Bulk-copy everything after the 2-byte numCols into rd.buf
  let bufBase = rd.buf.len
  let dataLen = body.len - 2
  rd.buf.setLen(bufBase + dataLen)
  if dataLen > 0:
    copyMem(addr rd.buf[bufBase], unsafeAddr body[2], dataLen)
  # Walk the copied buffer to build cellIndex
  var pos = bufBase # current position in rd.buf
  let bufEnd = bufBase + dataLen
  for i in 0 ..< numCols:
    if pos + 4 > bufEnd:
      rd.cellIndex.setLen(cellBase)
      rd.buf.setLen(bufBase)
      raise newException(ProtocolError, "DataRow: unexpected end of data")
    # Decode column length from copied buffer (big-endian int32)
    let colLen =
      int32(rd.buf[pos]) shl 24 or int32(rd.buf[pos + 1]) shl 16 or
      int32(rd.buf[pos + 2]) shl 8 or int32(rd.buf[pos + 3])
    pos += 4
    let ci = cellBase + int(i) * 2
    if colLen < -1:
      rd.cellIndex.setLen(cellBase)
      rd.buf.setLen(bufBase)
      raise newException(ProtocolError, "DataRow: invalid column length " & $colLen)
    elif colLen == -1:
      rd.cellIndex[ci] = 0'i32
      rd.cellIndex[ci + 1] = -1'i32
    else:
      if pos + colLen > bufEnd:
        rd.cellIndex.setLen(cellBase)
        rd.buf.setLen(bufBase)
        raise newException(ProtocolError, "DataRow: column data truncated")
      rd.cellIndex[ci] = int32(pos)
      rd.cellIndex[ci + 1] = colLen
      pos += int(colLen)

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
  ## Get the value of an error field by its single-char code (e.g. 'M' for message).
  for f in fields:
    if f.code == code:
      return f.value
  return ""

proc formatError*(fields: seq[ErrorField]): string =
  ## Format error fields into a human-readable error message with severity, SQLSTATE, detail, and hint.
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

proc addCopyBinaryHeader*(buf: var seq[byte]) =
  ## Append the PostgreSQL binary COPY header (signature + flags + extension area).
  let oldLen = buf.len
  buf.setLen(oldLen + pgCopyBinaryHeader.len)
  copyMem(addr buf[oldLen], unsafeAddr pgCopyBinaryHeader[0], pgCopyBinaryHeader.len)

proc addCopyBinaryTrailer*(buf: var seq[byte]) =
  ## Append the binary COPY trailer (int16 = -1).
  let oldLen = buf.len
  buf.setLen(oldLen + 2)
  buf[oldLen] = 0xFF'u8
  buf[oldLen + 1] = 0xFF'u8

proc addCopyTupleStart*(buf: var seq[byte], numCols: int16) =
  ## Start a new tuple in binary COPY format with the given column count.
  buf.addInt16(numCols)

proc addCopyFieldNull*(buf: var seq[byte]) =
  ## Append a NULL field in binary COPY format.
  buf.addInt32(-1'i32)

proc addCopyFieldInt16*(buf: var seq[byte], val: int16) =
  ## Append an int16 field in binary COPY format.
  buf.addInt32(2'i32)
  buf.addInt16(val)

proc addCopyFieldInt32*(buf: var seq[byte], val: int32) =
  ## Append an int32 field in binary COPY format.
  buf.addInt32(4'i32)
  buf.addInt32(val)

proc addCopyFieldInt64*(buf: var seq[byte], val: int64) =
  ## Append an int64 field in binary COPY format.
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
  ## Append a float64 field in binary COPY format.
  buf.addCopyFieldInt64(cast[int64](val))

proc addCopyFieldFloat32*(buf: var seq[byte], val: float32) =
  ## Append a float32 field in binary COPY format.
  buf.addCopyFieldInt32(cast[int32](val))

proc addCopyFieldBool*(buf: var seq[byte], val: bool) =
  ## Append a boolean field in binary COPY format.
  buf.addInt32(1'i32)
  buf.add(if val: 1'u8 else: 0'u8)

proc addCopyFieldText*(buf: var seq[byte], val: openArray[byte]) =
  ## Append a raw byte field in binary COPY format.
  buf.addInt32(int32(val.len))
  if val.len > 0:
    let oldLen = buf.len
    buf.setLen(oldLen + val.len)
    copyMem(addr buf[oldLen], unsafeAddr val[0], val.len)

proc addCopyFieldString*(buf: var seq[byte], val: string) =
  ## Append a string field in binary COPY format.
  buf.addInt32(int32(val.len))
  if val.len > 0:
    let oldLen = buf.len
    buf.setLen(oldLen + val.len)
    copyMem(addr buf[oldLen], unsafeAddr val[0], val.len)
