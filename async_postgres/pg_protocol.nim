import std/[options, tables]

import pg_bytes, pg_errors
export pg_errors

type
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
    bmkCopyBothResponse
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
    of bmkCopyInResponse, bmkCopyOutResponse, bmkCopyBothResponse:
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
    colMap*: Table[string, int] ## Cached name→index mapping (lazily built)

  Row* = object ## Lightweight view into a single row within a `RowData` buffer.
    data: RowData
    rowIdx: int32

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
    114, # json
    142, # xml
    143, # xml[]
    600, # point
    601, # lseg
    602, # path
    603, # box
    604, # polygon
    628, # line
    629, # line[]
    650, # cidr
    651, # cidr[]
    700, # float4
    701, # float8
    718, # circle
    719, # circle[]
    774, # macaddr8
    775, # macaddr8[]
    790, # money
    791, # money[]
    829, # macaddr
    869, # inet
    1000, # bool[]
    1001, # bytea[]
    1005, # int2[]
    1007, # int4[]
    1009, # text[]
    1015, # varchar[]
    1016, # int8[]
    1017, # point[]
    1018, # lseg[]
    1019, # path[]
    1020, # box[]
    1021, # float4[]
    1022, # float8[]
    1027, # polygon[]
    1040, # macaddr[]
    1041, # inet[]
    1043, # varchar
    1082, # date
    1083, # time
    1114, # timestamp
    1115, # timestamp[]
    1182, # date[]
    1183, # time[]
    1184, # timestamptz
    1185, # timestamptz[]
    1186, # interval
    1187, # interval[]
    1231, # numeric[]
    1266, # timetz
    1270, # timetz[]
    1560, # bit
    1561, # bit[]
    1562, # varbit
    1563, # varbit[]
    1700, # numeric
    2950, # uuid
    2951, # uuid[]
    3614, # tsvector
    3615, # tsquery
    3643, # tsvector[]
    3645, # tsquery[]
    3802, # jsonb
    3807, # jsonb[]
    3904, # int4range
    3905, # int4range[]
    3906, # numrange
    3907, # numrange[]
    3908, # tsrange
    3909, # tsrange[]
    3910, # tstzrange
    3911, # tstzrange[]
    3912, # daterange
    3913, # daterange[]
    3926, # int8range
    3927, # int8range[]
    4451, # int4multirange
    4532, # nummultirange
    4533, # tsmultirange
    4534, # tstzmultirange
    4535, # datemultirange
    4536, # int8multirange
    6150, # int4multirange[]
    6151, # nummultirange[]
    6152, # tsmultirange[]
    6153, # tstzmultirange[]
    6155, # datemultirange[]
    6157, # int8multirange[]
  ]

  BinarySafeMaxOid = 6157

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

  maxInt16Count = int(high(int16))
    ## Maximum element count encodable in a wire Int16 count field (32767).
    ## Parameter-type, format-code, and parameter-value counts in Parse/Bind
    ## messages are all Int16, so they cannot represent more elements than this.

  maxInt32Len* = int(high(int32))
    ## Maximum byte length encodable in a wire Int32 length field (2147483647).
    ## Parameter values, SASL data, binary COPY fields, and every message length
    ## are Int32. PostgreSQL further caps a single value at `MaxAllocSize`
    ## (~1 GiB - 1), so legitimate payloads stay well below this.

  DefaultMaxBackendMessageLen* = 1024 * 1024 * 1024
    ## Default upper bound on a single backend message (header + body), 1 GiB.
    ## Prevents a malicious or broken server from causing unbounded recv-buffer
    ## growth (and OOM) by advertising an int32-max length. PostgreSQL itself
    ## bounds individual values by `MaxAllocSize` (~1 GiB - 1), so legitimate
    ## traffic stays well below this cap.

func makeBinarySafeLookup(): array[BinarySafeMaxOid + 1, bool] {.compileTime.} =
  for oid in BinarySafeOids:
    result[oid] = true

const binarySafeLookup = makeBinarySafeLookup()

func initRow*(data: RowData, rowIdx: int32): Row =
  ## Create a Row view into the given RowData at the specified row index.
  Row(data: data, rowIdx: rowIdx)

func data*(row: Row): RowData = ## The underlying RowData buffer.
  row.data

func rowIdx*(row: Row): int32 = ## The row index within the RowData buffer.
  row.rowIdx

func isBinarySafeOid*(oid: int32): bool =
  ## Check if a type OID can be safely requested in binary format.
  oid >= 0 and oid <= BinarySafeMaxOid and binarySafeLookup[oid]

# Byte-level helpers

proc encodeInt16*(val: int16): array[2, byte] {.inline.} =
  ## Encode a 16-bit integer as big-endian bytes.
  toBE16(val)

proc encodeInt32*(val: int32): array[4, byte] {.inline.} =
  ## Encode a 32-bit integer as big-endian bytes.
  toBE32(val)

proc addInt16*(buf: var seq[byte], val: int16) =
  ## Append a 16-bit integer in big-endian format to the buffer.
  let oldLen = buf.len
  buf.setLen(oldLen + 2)
  buf.writeBE16(oldLen, val)

proc addCount16*(buf: var seq[byte], n: int, what: string) =
  ## Append an Int16 count field, rejecting counts that overflow the wire's
  ## signed 16-bit range. Without this guard the `int16(n)` conversion raises an
  ## uncatchable `RangeDefect` on default builds, or silently wraps to a bogus
  ## (often negative) count that desyncs the protocol stream under `-d:danger`.
  ## The check is always active so callers get a catchable `ValueError` instead.
  ##
  ## `ValueError` is used here (and in `addLen32`) because the count is supplied
  ## directly by the caller; message-level length overflows detected after the
  ## message has been assembled raise `PgProtocolError` instead.
  if n < 0:
    raise newException(ValueError, what & " count " & $n & " is negative")
  if n > maxInt16Count:
    raise newException(
      ValueError,
      what & " count " & $n & " exceeds protocol maximum of " & $maxInt16Count,
    )
  buf.addInt16(int16(n))

proc addInt32*(buf: var seq[byte], val: int32) =
  ## Append a 32-bit integer in big-endian format to the buffer.
  let oldLen = buf.len
  buf.setLen(oldLen + 4)
  buf.writeBE32(oldLen, val)

proc addLen32*(buf: var seq[byte], n: int, what: string) =
  ## Append an Int32 length field, rejecting payloads that overflow the wire's
  ## signed 32-bit length. Like `addCount16`, this turns the otherwise
  ## uncatchable `RangeDefect` (or a wrapped, often-negative length that desyncs
  ## the stream under `-d:danger`) into a catchable `ValueError` raised before
  ## the oversized payload is appended.
  ##
  ## `ValueError` is used here (and in `addCount16`) because the length is
  ## supplied directly by the caller; message-level length overflows detected
  ## after the message has been assembled raise `PgProtocolError` instead.
  if n < 0:
    raise newException(ValueError, what & " length " & $n & " is negative")
  if n > maxInt32Len:
    raise newException(
      ValueError,
      what & " length " & $n & " exceeds protocol maximum of " & $maxInt32Len,
    )
  buf.addInt32(int32(n))

proc addInt64*(buf: var seq[byte], val: int64) =
  ## Append a 64-bit integer in big-endian format to the buffer.
  let oldLen = buf.len
  buf.setLen(oldLen + 8)
  buf.writeBE64(oldLen, val)

proc patchLen*(buf: var seq[byte], offset: int = 1) =
  ## Patch the length placeholder at `offset` with buf.len minus the tag byte.
  ## Raises `PgProtocolError` when the assembled message exceeds the Int32
  ## maximum; this is a protocol-level failure for an internally built message,
  ## distinct from the `ValueError` raised by `addLen32`/`addCount16` for
  ## caller-supplied field values.
  if offset < 0 or offset + 3 >= buf.len:
    raise newException(
      PgProtocolError,
      "patchLen: offset " & $offset & " out of range for buf.len " & $buf.len,
    )
  if buf.high > maxInt32Len:
    raise newException(
      PgProtocolError,
      "patchLen: message length " & $buf.high & " exceeds Int32 maximum of " &
        $maxInt32Len,
    )
  let length = int32(buf.high)
  buf.writeBE32(offset, length)

proc patchMsgLen*(buf: var seq[byte], msgStart: int) =
  ## Patch the length field of a message starting at `msgStart`.
  ## Length = total message size minus the type byte.
  ## Raises `PgProtocolError` when the assembled message exceeds the Int32
  ## maximum; this is a protocol-level failure for an internally built message,
  ## distinct from the `ValueError` raised by `addLen32`/`addCount16` for
  ## caller-supplied field values.
  if msgStart < 0 or msgStart + 4 >= buf.len:
    raise newException(
      PgProtocolError,
      "patchMsgLen: msgStart " & $msgStart & " out of range for buf.len " & $buf.len,
    )
  if buf.len - msgStart - 1 > maxInt32Len:
    raise newException(
      PgProtocolError,
      "patchMsgLen: message length " & $(buf.len - msgStart - 1) &
        " exceeds Int32 maximum of " & $maxInt32Len,
    )
  let length = int32(buf.len - msgStart - 1)
  buf.writeBE32(msgStart + 1, length)

proc addCString*(buf: var seq[byte], s: string) =
  ## Append a null-terminated C string to the buffer.
  ##
  ## Raises ``ValueError`` if ``s`` contains an embedded NUL byte. PostgreSQL
  ## protocol fields are NUL-terminated, so an embedded ``\0`` would split the
  ## value into two fields on the server side — at best causing a desync
  ## (`invalid message format`), at worst allowing startup-parameter injection
  ## through the StartupMessage K/V stream.
  for c in s:
    if c == '\0':
      raise newException(ValueError, "addCString: embedded NUL byte in protocol string")
  let oldLen = buf.len
  buf.setLen(oldLen + s.len + 1)
  if s.len > 0:
    buf.writeBytesAt(oldLen, s.toOpenArrayByte(0, s.high))
  buf[oldLen + s.len] = 0'u8

proc decodeInt16*(buf: openArray[byte], offset: int): int16 {.inline.} =
  ## Decode a 16-bit integer from big-endian bytes at the given offset.
  fromBE16(buf, offset)

proc decodeInt32*(buf: openArray[byte], offset: int): int32 {.inline.} =
  ## Decode a 32-bit integer from big-endian bytes at the given offset.
  fromBE32(buf, offset)

proc decodeInt64*(buf: openArray[byte], offset: int): int64 {.inline.} =
  ## Decode a 64-bit integer from big-endian bytes at the given offset.
  fromBE64(buf, offset)

proc decodeCString*(buf: openArray[byte], offset: int): (string, int) =
  ## Decode a null-terminated string at the given offset. Returns (string, bytes consumed).
  if offset >= buf.len:
    raise newException(PgProtocolError, "decodeCString: offset past end of buffer")
  var i = offset
  while i < buf.len and buf[i] != 0:
    inc i
  if i >= buf.len:
    raise newException(PgProtocolError, "decodeCString: missing null terminator")
  let slen = i - offset
  let s = readString(buf, offset, slen)
  inc i # skip null terminator
  result = (s, i - offset)

# Frontend message encoding

proc encodeStartup*(
    user: string, database: string, extraParams: openArray[(string, string)] = []
): seq[byte] =
  ## Encode a StartupMessage (protocol v3.0) with user, database, and extra parameters.
  ## Raises `ValueError` for invalid caller-supplied values (e.g. embedded NUL
  ## bytes) and `PgProtocolError` if the assembled message exceeds the Int32
  ## maximum length.
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
  if result.len > maxInt32Len:
    raise newException(
      PgProtocolError,
      "encodeStartup: message length " & $result.len & " exceeds Int32 maximum of " &
        $maxInt32Len,
    )
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
  ## Pre-allocates the buffer so internal `add` calls do not realloc and
  ## leave residual copies of the password in freed heap memory.
  result = newSeqOfCap[byte](1 + 4 + password.len + 1)
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.addCString(password)
  result.patchLen()

proc encodeSASLInitialResponse*(mechanism: string, data: seq[byte]): seq[byte] =
  ## Encode a SASLInitialResponse message with the chosen mechanism and client-first data.
  result.add(byte('p'))
  result.addInt32(0) # length placeholder
  result.addCString(mechanism)
  result.addLen32(data.len, "SASLInitialResponse data")
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

proc addFixedMsg(buf: var seq[byte], msg: array[5, byte]) =
  let oldLen = buf.len
  buf.setLen(oldLen + 5)
  buf.writeBytesAt(oldLen, msg)

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
  buf.addCount16(paramTypeOids.len, "Parse parameter-type")
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
  buf.addCount16(paramFormats.len, "Bind parameter-format")
  for f in paramFormats:
    buf.addInt16(f)
  # Parameter values
  buf.addCount16(paramValues.len, "Bind parameter")
  for v in paramValues:
    if v.isNone:
      buf.addInt32(-1) # NULL
    else:
      let data = v.get
      buf.addLen32(data.len, "Bind parameter value")
      buf.appendBytes(data)
  # Result format codes
  buf.addCount16(resultFormats.len, "Bind result-format")
  for f in resultFormats:
    buf.addInt16(f)
  buf.patchMsgLen(msgStart)

proc addBindRaw*(
    buf: var seq[byte],
    portalName: string,
    stmtName: string,
    paramFormats: openArray[int16],
    paramData: openArray[byte],
    paramRanges: openArray[tuple[off: int32, len: int32]],
    resultFormats: openArray[int16] = [],
) =
  ## Append a Bind message built from a raw byte buffer and offset/length
  ## ranges. Each parameter is described by `(off, len)`: `len == -1` encodes
  ## NULL; any other `len` reads `paramData[off ..< off + len]`. Lets callers
  ## write payloads straight into a single owned buffer without constructing
  ## `Option[seq[byte]]` per parameter.
  ##
  ## Each range must satisfy one of: `len == -1` (NULL), or
  ## `len >= 0` with `0 <= off` and `off + len <= paramData.len`.
  ## Invalid ranges raise `ValueError` — the check is always active so callers
  ## cannot silently trigger an out-of-bounds `copyMem` in release builds.
  let msgStart = buf.len
  buf.add(byte('B'))
  buf.addInt32(0) # length placeholder
  buf.addCString(portalName)
  buf.addCString(stmtName)
  buf.addCount16(paramFormats.len, "Bind parameter-format")
  for f in paramFormats:
    buf.addInt16(f)
  buf.addCount16(paramRanges.len, "Bind parameter")
  for r in paramRanges:
    if r.len < -1:
      raise newException(ValueError, "addBindRaw: invalid range len " & $r.len)
    if r.len == -1:
      buf.addInt32(-1)
    else:
      buf.addInt32(r.len)
      if r.len > 0:
        if r.off < 0:
          raise newException(ValueError, "addBindRaw: negative range off " & $r.off)
        if r.off.int64 + r.len.int64 > paramData.len.int64:
          raise newException(
            ValueError,
            "addBindRaw: range out of bounds (off=" & $r.off & ", len=" & $r.len &
              ", data.len=" & $paramData.len & ")",
          )
        let oldLen = buf.len
        buf.setLen(oldLen + r.len)
        buf.writeBytesAt(oldLen, paramData.toOpenArray(r.off, r.off + r.len - 1))
  buf.addCount16(resultFormats.len, "Bind result-format")
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

proc addSync*(buf: var seq[byte]) =
  ## Append a Sync message to the buffer.
  buf.addFixedMsg(syncMsg)

proc addFlush*(buf: var seq[byte]) =
  ## Append a Flush message to the buffer.
  buf.addFixedMsg(flushMsg)

proc addCopyDone*(buf: var seq[byte]) =
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
  ## The Int32 length field covers itself plus the payload, so reject payloads
  ## that would overflow it (a wrapped length desyncs the stream) before any
  ## allocation, matching the `addLen32` guard used by the other encoders.
  ## Like `addLen32`, the payload length comes from the caller, so an overflow
  ## raises `ValueError` rather than `PgProtocolError`.
  if data.len > maxInt32Len - 4:
    raise newException(
      ValueError,
      "CopyData payload length " & $data.len & " exceeds protocol maximum of " &
        $(maxInt32Len - 4),
    )
  let msgLen = int32(4 + data.len)
  let oldLen = buf.len
  buf.setLen(oldLen + 5 + data.len)
  buf[oldLen] = byte('d')
  buf.writeBE32(oldLen + 1, msgLen)
  buf.writeBytesAt(oldLen + 5, data)

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
    raise newException(PgProtocolError, "Authentication message too short")

  let authType = decodeInt32(body, 0)
  case authType
  of 0:
    result = BackendMessage(kind: bmkAuthenticationOk)
  of 3:
    result = BackendMessage(kind: bmkAuthenticationCleartextPassword)
  of 5:
    if body.len < 8:
      raise newException(PgProtocolError, "MD5 auth message too short")
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
    raise newException(PgProtocolError, "Unknown authentication type: " & $authType)

proc parseBackendKeyData(body: openArray[byte]): BackendMessage =
  if body.len < 8:
    raise newException(PgProtocolError, "BackendKeyData message too short")
  result = BackendMessage(kind: bmkBackendKeyData)
  result.backendPid = decodeInt32(body, 0)
  result.backendSecretKey = decodeInt32(body, 4)

proc parseCommandComplete(body: openArray[byte]): BackendMessage =
  result = BackendMessage(kind: bmkCommandComplete)
  let (tag, _) = decodeCString(body, 0)
  result.commandTag = tag

proc parseDataRow(body: openArray[byte]): BackendMessage =
  if body.len < 2:
    raise newException(PgProtocolError, "DataRow message too short")
  result = BackendMessage(kind: bmkDataRow)
  let numCols = decodeInt16(body, 0)
  if numCols < 0:
    raise newException(PgProtocolError, "DataRow: invalid column count " & $numCols)
  result.columns = newSeq[Option[seq[byte]]](numCols)
  var offset = 2
  for i in 0 ..< numCols:
    if offset + 4 > body.len:
      raise newException(PgProtocolError, "DataRow: unexpected end of data")
    let colLen = decodeInt32(body, offset)
    offset += 4
    if colLen < -1:
      raise newException(PgProtocolError, "DataRow: invalid column length " & $colLen)
    elif colLen == -1:
      result.columns[i] = none(seq[byte])
    else:
      if offset + colLen > body.len:
        raise newException(PgProtocolError, "DataRow: column data truncated")
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
    raise newException(PgProtocolError, "Notification message too short")
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
    raise newException(PgProtocolError, "RowDescription message too short")
  result = BackendMessage(kind: bmkRowDescription)
  let numFields = decodeInt16(body, 0)
  if numFields < 0:
    raise
      newException(PgProtocolError, "RowDescription: invalid field count " & $numFields)
  result.fields = newSeq[FieldDescription](numFields)
  var offset = 2
  for i in 0 ..< numFields:
    let (name, consumed) = decodeCString(body, offset)
    offset += consumed
    if offset + 18 > body.len:
      raise newException(PgProtocolError, "RowDescription: unexpected end of data")
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
    raise newException(PgProtocolError, "ReadyForQuery message too short")
  result = BackendMessage(kind: bmkReadyForQuery)
  case char(body[0])
  of 'I':
    result.txStatus = tsIdle
  of 'T':
    result.txStatus = tsInTransaction
  of 'E':
    result.txStatus = tsInFailedTransaction
  else:
    raise newException(PgProtocolError, "Unknown transaction status: " & $char(body[0]))

proc parseParameterDescription(body: openArray[byte]): BackendMessage =
  if body.len < 2:
    raise newException(PgProtocolError, "ParameterDescription too short")
  result = BackendMessage(kind: bmkParameterDescription)
  let numParams = decodeInt16(body, 0)
  if numParams < 0:
    raise newException(
      PgProtocolError, "ParameterDescription: invalid param count " & $numParams
    )
  result.paramTypeOids = newSeq[int32](numParams)
  var offset = 2
  for i in 0 ..< numParams:
    if offset + 4 > body.len:
      raise newException(PgProtocolError, "ParameterDescription truncated")
    result.paramTypeOids[i] = decodeInt32(body, offset)
    offset += 4

proc parseCopyResponse(
    body: openArray[byte], kind: BackendMessageKind
): BackendMessage =
  let label = if kind == bmkCopyBothResponse: "CopyBothResponse" else: "CopyResponse"
  if body.len < 3:
    raise newException(PgProtocolError, label & " message too short")
  result = BackendMessage(kind: kind)
  result.copyFormat = if body[0] == 0: cfText else: cfBinary
  let numCols = decodeInt16(body, 1)
  if numCols < 0:
    raise newException(PgProtocolError, label & ": invalid column count " & $numCols)
  result.copyColumnFormats = newSeq[int16](numCols)
  var offset = 3
  for i in 0 ..< numCols:
    if offset + 2 > body.len:
      raise newException(PgProtocolError, label & " truncated")
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

proc clone*(row: Row): Row =
  ## Return a deep copy of `row` with an independent `RowData` backing buffer
  ## containing only this single row. Use this to retain rows from a
  ## `queryEach` callback beyond the callback's lifetime — the original
  ## buffer is reused for subsequent rows and would otherwise be overwritten.
  if row.data == nil:
    return Row(data: nil, rowIdx: 0)
  let src = row.data
  let numCols = src.numCols
  let cellBase = int(row.rowIdx) * int(numCols) * 2
  var total = 0
  for i in 0 ..< int(numCols):
    let clen = src.cellIndex[cellBase + i * 2 + 1]
    if clen > 0:
      total += int(clen)
  let rd = RowData(
    numCols: numCols,
    colFormats: src.colFormats,
    colTypeOids: src.colTypeOids,
    fields: src.fields,
    colMap: src.colMap,
    cellIndex: newSeq[int32](int(numCols) * 2),
    buf: newSeq[byte](total),
  )
  var pos = 0
  for i in 0 ..< int(numCols):
    let srcOff = int(src.cellIndex[cellBase + i * 2])
    let clen = src.cellIndex[cellBase + i * 2 + 1]
    if clen == -1:
      rd.cellIndex[i * 2] = 0'i32
      rd.cellIndex[i * 2 + 1] = -1'i32
    elif clen == 0:
      rd.cellIndex[i * 2] = 0'i32
      rd.cellIndex[i * 2 + 1] = 0'i32
    else:
      rd.buf.writeBytesAt(pos, src.buf.toOpenArray(srcOff, srcOff + int(clen) - 1))
      rd.cellIndex[i * 2] = int32(pos)
      rd.cellIndex[i * 2 + 1] = clen
      pos += int(clen)
  Row(data: rd, rowIdx: 0)

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
    raise newException(PgProtocolError, "DataRow message too short")

  let numCols = decodeInt16(body, 0)
  if numCols < 0:
    raise newException(PgProtocolError, "DataRow: invalid column count " & $numCols)
  # cellInfo strides cellIndex by rd.numCols; a mismatched row would misalign
  # every subsequent row (wrong-cell reads or IndexDefect).
  if numCols != rd.numCols:
    raise newException(
      PgProtocolError,
      "DataRow: column count " & $numCols & " does not match RowDescription " &
        $rd.numCols,
    )

  # Check cumulative buffer size before any mutation
  let bufBase = rd.buf.len
  let dataLen = body.len - 2
  if bufBase + dataLen > int32.high:
    raise newException(
      PgProtocolError, "DataRow: result set exceeds maximum addressable size (2 GiB)"
    )

  # Pre-extend cellIndex for this row
  let cellBase = rd.cellIndex.len
  rd.cellIndex.setLen(cellBase + int(numCols) * 2)

  # Bulk-copy everything after the 2-byte numCols into rd.buf
  rd.buf.setLen(bufBase + dataLen)
  if dataLen > 0:
    rd.buf.writeBytesAt(bufBase, body.toOpenArray(2, 2 + dataLen - 1))

  # Walk the copied buffer to build cellIndex
  var pos = bufBase # current position in rd.buf
  let bufEnd = bufBase + dataLen
  for i in 0 ..< numCols:
    if pos + 4 > bufEnd:
      rd.cellIndex.setLen(cellBase)
      rd.buf.setLen(bufBase)
      raise newException(PgProtocolError, "DataRow: unexpected end of data")
    # Decode column length from copied buffer (big-endian int32)
    let colLen = fromBE32(rd.buf, pos)
    pos += 4
    let ci = cellBase + int(i) * 2
    if colLen < -1:
      rd.cellIndex.setLen(cellBase)
      rd.buf.setLen(bufBase)
      raise newException(PgProtocolError, "DataRow: invalid column length " & $colLen)
    elif colLen == -1:
      rd.cellIndex[ci] = 0'i32
      rd.cellIndex[ci + 1] = -1'i32
    else:
      if pos + colLen > bufEnd:
        rd.cellIndex.setLen(cellBase)
        rd.buf.setLen(bufBase)
        raise newException(PgProtocolError, "DataRow: column data truncated")
      rd.cellIndex[ci] = int32(pos)
      rd.cellIndex[ci + 1] = colLen
      pos += int(colLen)

# Streaming backend message parser

proc parseBackendMessage*(
    buf: openArray[byte],
    consumed: var int,
    rowData: RowData = nil,
    maxLen: int = DefaultMaxBackendMessageLen,
    skipDataRow: bool = false,
): ParseResult {.raises: [PgProtocolError].} =
  ## Parse a single backend message from `buf`.
  ## On success, sets `consumed` to the number of bytes used.
  ## The caller is responsible for discarding those bytes from the buffer.
  ## A message whose declared length exceeds `maxLen` is rejected with
  ## `PgProtocolError` before any allocation, capping recv-buffer growth.
  ## ``maxLen <= 0`` disables the cap (intended for tests); production
  ## callers should resolve the default via ``ConnConfig.maxMessageSize``
  ## and ``effectiveMaxMessageSize``.
  ## When ``rowData == nil`` and ``skipDataRow`` is true, a ``DataRow`` message
  ## is only framed and consumed — the columns are NOT decoded, avoiding the
  ## per-row ``seq[Option[seq[byte]]]`` + per-cell ``seq`` allocation that the
  ## caller would immediately discard. Result state is ``psDataRow`` in that
  ## case, with ``message`` left default-initialised.
  consumed = 0

  # Need at least 5 bytes: 1 type + 4 length
  if buf.len < 5:
    return ParseResult(state: psIncomplete)

  let msgType = char(buf[0])
  let msgLen = decodeInt32(buf, 1) # includes self but not type byte

  if msgLen < 4:
    raise newException(PgProtocolError, "Invalid message length: " & $msgLen)
  # Compare in int64 to avoid overflow on 32-bit platforms where `int` is
  # 32-bit and `msgLen + 1` would wrap when msgLen approaches int32.high.
  # Total message size is `msgLen + 1` (type byte), so reject when that
  # would exceed maxLen, i.e. when `msgLen >= maxLen`.
  if maxLen > 0 and int64(msgLen) >= int64(maxLen):
    raise newException(
      PgProtocolError,
      "Backend message too large: " & $msgLen & " bytes (max " & $maxLen & ")",
    )

  let totalLen = int(msgLen) + 1 # type byte + length field + body
  if buf.len < totalLen:
    return ParseResult(state: psIncomplete)

  # Body is the region after type byte and 4-byte length
  let bodyStart = 5
  let bodyLen = int(msgLen) - 4 # msgLen includes the 4-byte length field itself
  template body(): untyped =
    buf.toOpenArray(bodyStart, bodyStart + bodyLen - 1)

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
    elif skipDataRow:
      # Caller does not want the row — skip decoding entirely to avoid the
      # per-row seq[Option[seq[byte]]] + per-cell seq allocation.
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
    msg = parseCopyResponse(body, bmkCopyInResponse)
  of 'H':
    msg = parseCopyResponse(body, bmkCopyOutResponse)
  of 'W':
    msg = parseCopyResponse(body, bmkCopyBothResponse)
  of 'd':
    msg = BackendMessage(kind: bmkCopyData)
    msg.copyData = @(body)
  of 'c':
    msg = BackendMessage(kind: bmkCopyDone)
  else:
    raise newException(PgProtocolError, "Unknown backend message type: " & msgType)

  consumed = totalLen
  result = ParseResult(state: psComplete, message: msg)

# Utility

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
  buf.appendBytes(pgCopyBinaryHeader)

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
  buf.addInt64(val)

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
  buf.addLen32(val.len, "COPY field")
  buf.appendBytes(val)

proc addCopyFieldString*(buf: var seq[byte], val: string) =
  ## Append a string field in binary COPY format.
  buf.addLen32(val.len, "COPY field")
  if val.len > 0:
    buf.appendBytes(val.toOpenArrayByte(0, val.high))

# Replication protocol helpers

proc encodeStandbyStatusUpdate*(
    receiveLsn, flushLsn, applyLsn, sendTime: int64, reply: byte
): seq[byte] =
  ## Encode a Standby Status Update as a CopyData message.
  ## The inner format is: byte 'r' + receiveLsn(8) + flushLsn(8) + applyLsn(8) + sendTime(8) + reply(1) = 34 bytes payload.
  result.add(byte('d')) # CopyData message type
  result.addInt32(int32(4 + 1 + 8 + 8 + 8 + 8 + 1)) # length: self(4) + payload(34)
  result.add(byte('r'))
  result.addInt64(receiveLsn)
  result.addInt64(flushLsn)
  result.addInt64(applyLsn)
  result.addInt64(sendTime)
  result.add(reply)
