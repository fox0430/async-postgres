## Internal building blocks shared by every `pg_client/` submodule.
##
## Contains types/constants for transaction options, the inline-parameter
## encoder, and the receive-loop templates that the extended-query path
## (`exec`, `query`, `queryEach`, `queryDirect`, …) reuses. Re-exported through
## `pg_client.nim`; submodules import this module directly via `./core`.

import std/[options, tables]

import ../[async_backend, pg_protocol, pg_connection, pg_types]

const copyBatchSize* = 262144 ## 256KB batch threshold for COPY IN buffering

func toFormatCodes*(rf: ResultFormat): seq[int16] =
  ## Convert a high-level ResultFormat to wire-protocol format codes.
  case rf
  of rfAuto:
    @[]
  of rfText:
    @[0'i16]
  of rfBinary:
    @[1'i16]

type
  IsolationLevel* = enum
    ## PostgreSQL transaction isolation level.
    ilDefault
    ilReadCommitted
    ilRepeatableRead
    ilSerializable
    ilReadUncommitted

  AccessMode* = enum
    ## PostgreSQL transaction access mode (read-write or read-only).
    amDefault
    amReadWrite
    amReadOnly

  DeferrableMode* = enum
    ## PostgreSQL transaction deferrable mode (for serializable read-only transactions).
    dmDefault
    dmDeferrable
    dmNotDeferrable

  TransactionOptions* = object
    ## Options for BEGIN: isolation level, access mode, and deferrable mode.
    isolation*: IsolationLevel
    access*: AccessMode
    deferrable*: DeferrableMode

proc buildBeginSql*(opts: TransactionOptions): string =
  ## Build a BEGIN SQL statement with the specified transaction options
  ## (isolation level, access mode, deferrable mode).
  result = "BEGIN"
  case opts.isolation
  of ilDefault:
    discard
  of ilReadCommitted:
    result.add " ISOLATION LEVEL READ COMMITTED"
  of ilRepeatableRead:
    result.add " ISOLATION LEVEL REPEATABLE READ"
  of ilSerializable:
    result.add " ISOLATION LEVEL SERIALIZABLE"
  of ilReadUncommitted:
    result.add " ISOLATION LEVEL READ UNCOMMITTED"
  case opts.access
  of amDefault:
    discard
  of amReadWrite:
    result.add " READ WRITE"
  of amReadOnly:
    result.add " READ ONLY"
  case opts.deferrable
  of dmDefault:
    discard
  of dmDeferrable:
    result.add " DEFERRABLE"
  of dmNotDeferrable:
    result.add " NOT DEFERRABLE"

proc extractParams*(
    params: openArray[PgParam]
): tuple[oids: seq[int32], formats: seq[int16], values: seq[Option[seq[byte]]]] =
  result.oids = newSeq[int32](params.len)
  result.formats = newSeq[int16](params.len)
  result.values = newSeq[Option[seq[byte]]](params.len)
  for i, p in params:
    result.oids[i] = p.oid
    result.formats[i] = p.format
    result.values[i] = p.value

template appendInlineParam*(
    data: var seq[byte],
    ranges: var seq[tuple[off: int32, len: int32]],
    oids: var seq[int32],
    formats: var seq[int16],
    p: PgParamInline,
) =
  ## Shared encoder for a single `PgParamInline` into SoA buffers. Used by
  ## both `flattenInline` (per-call temporaries) and `Pipeline.appendInline`
  ## (pipeline-wide SoA). Keeping the NULL / empty / inline / overflow
  ## branching in one place means wire-format semantics cannot drift between
  ## the two code paths.
  oids.add p.oid
  formats.add p.format
  if p.len == -1:
    ranges.add((int32(0), int32(-1)))
  elif p.len == 0:
    ranges.add((int32(data.len), int32(0)))
  else:
    let dataOff = int32(data.len)
    let oldLen = data.len
    data.setLen(oldLen + int(p.len))
    if p.len <= PgInlineBufSize:
      data.writeBytesAt(oldLen, p.inlineBuf.toOpenArray(0, int(p.len) - 1))
    else:
      data.writeBytesAt(oldLen, p.overflow.toOpenArray(0, int(p.len) - 1))
    ranges.add((dataOff, p.len))

proc flattenInline*(
    params: openArray[PgParamInline]
): tuple[
  data: seq[byte],
  ranges: seq[tuple[off: int32, len: int32]],
  oids: seq[int32],
  formats: seq[int16],
] =
  if params.len == 0:
    return
  result.oids = newSeqOfCap[int32](params.len)
  result.formats = newSeqOfCap[int16](params.len)
  result.ranges = newSeqOfCap[tuple[off: int32, len: int32]](params.len)
  var estBytes = 0
  for p in params:
    if p.len > 0:
      estBytes += int(p.len)
  result.data = newSeqOfCap[byte](estBytes)
  for p in params:
    appendInlineParam(result.data, result.ranges, result.oids, result.formats, p)

template queryRecvLoop*(
    conn: PgConnection,
    sql: string,
    resultFormats: openArray[int16],
    cacheHit, cacheMiss: bool,
    stmtName: string,
    cachedFields: var seq[FieldDescription],
    cachedColFmts: seq[int16],
    cachedColOids: seq[int32],
    qr: var QueryResult,
    timeout: Duration = ZeroDuration,
) =
  var queryError: ref PgQueryError

  if cacheHit:
    swap(qr.fields, cachedFields)
    if resultFormats.len > 0 and cachedColFmts.len > 0:
      for i in 0 ..< qr.fields.len:
        qr.fields[i].formatCode = cachedColFmts[i]
    if qr.fields.len > 0:
      qr.data = newRowData(int16(qr.fields.len), cachedColFmts, cachedColOids)
      qr.data.fields = qr.fields

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(qr.data, addr qr.rowCount); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          discard
        of bmkRowDescription:
          var cf: seq[int16]
          var co: seq[int32]
          if cacheMiss:
            cachedFields = msg.fields
            qr.fields = cachedFields
            if resultFormats.len > 0:
              cf = newSeq[int16](qr.fields.len)
              co = newSeq[int32](qr.fields.len)
              for i in 0 ..< qr.fields.len:
                co[i] = qr.fields[i].typeOid
                if resultFormats.len == 1:
                  qr.fields[i].formatCode = resultFormats[0]
                  cf[i] = resultFormats[0]
                elif i < resultFormats.len:
                  qr.fields[i].formatCode = resultFormats[i]
                  cf[i] = resultFormats[i]
          else:
            qr.fields = msg.fields
          qr.data = newRowData(int16(qr.fields.len), cf, co)
          qr.data.fields = qr.fields
        of bmkNoData:
          discard
        of bmkCommandComplete:
          qr.commandTag = msg.commandTag
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            if cacheHit and queryError.sqlState == "26000":
              conn.removeStmtCache(sql)
            raise queryError
          if cacheMiss:
            conn.addStmtCache(sql, CachedStmt(name: stmtName, fields: cachedFields))
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

template queryEachRecvLoop*(
    conn: PgConnection,
    sql: string,
    resultFormats: openArray[int16],
    cacheHit, cacheMiss: bool,
    stmtName: string,
    cachedFields: var seq[FieldDescription],
    cachedColFmts: seq[int16],
    cachedColOids: seq[int32],
    callback: RowCallback,
    rowCount: var int64,
    timeout: Duration = ZeroDuration,
) =
  var queryError: ref PgQueryError
  var rd: RowData
  var callbackError: ref CatchableError = nil

  if cacheHit:
    if cachedColFmts.len > 0 or cachedColOids.len > 0:
      rd = newRowData(int16(cachedFields.len), cachedColFmts, cachedColOids)
    else:
      rd = newRowData(int16(cachedFields.len))
    rd.fields = cachedFields
    if resultFormats.len > 0 and cachedColFmts.len > 0:
      for i in 0 ..< cachedFields.len:
        rd.colFormats[i] = cachedColFmts[i]

  let maxLen = conn.effectiveMaxMessageSize()
  block recvLoop:
    while true:
      # Parse messages directly from recvBuf using parseBackendMessage
      var pos = conn.recvBufStart
      while true:
        var consumed: int
        let res =
          try:
            parseBackendMessage(
              conn.recvBuf.toOpenArray(pos, conn.recvBuf.len - 1), consumed, rd, maxLen
            )
          except ProtocolError as e:
            conn.state = csClosed
            raise e
        if res.state == psIncomplete:
          break # need more data
        pos += consumed
        conn.recvBufStart = pos
        if res.state == psDataRow:
          # DataRow was parsed into rd — invoke callback, then reset for next row
          if callbackError == nil:
            try:
              callback(initRow(rd, 0))
              rowCount += 1
            except CatchableError as e:
              callbackError = e
          # Reset buffers but keep capacity
          rd.buf.setLen(0)
          rd.cellIndex.setLen(0)
          continue
        let msg = res.message
        case msg.kind
        of bmkNotificationResponse:
          conn.dispatchNotification(msg)
          continue
        of bmkNoticeResponse:
          conn.dispatchNotice(msg)
          continue
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          discard
        of bmkRowDescription:
          var cf: seq[int16]
          var co: seq[int32]
          if cacheMiss:
            cachedFields = msg.fields
            if resultFormats.len > 0:
              cf = newSeq[int16](cachedFields.len)
              co = newSeq[int32](cachedFields.len)
              for i in 0 ..< cachedFields.len:
                co[i] = cachedFields[i].typeOid
                if resultFormats.len == 1:
                  cachedFields[i].formatCode = resultFormats[0]
                  cf[i] = resultFormats[0]
                elif i < resultFormats.len:
                  cachedFields[i].formatCode = resultFormats[i]
                  cf[i] = resultFormats[i]
          else:
            cachedFields = msg.fields
          rd = newRowData(int16(cachedFields.len), cf, co)
          rd.fields = cachedFields
        of bmkNoData:
          discard
        of bmkCommandComplete:
          discard
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.recvBufStart = pos
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if callbackError != nil:
            raise callbackError
          if queryError != nil:
            if cacheHit and queryError.sqlState == "26000":
              conn.removeStmtCache(sql)
            raise queryError
          if cacheMiss:
            conn.addStmtCache(sql, CachedStmt(name: stmtName, fields: cachedFields))
          break recvLoop
        else:
          discard
        conn.recvBufStart = pos
      await conn.fillRecvBuf(timeout)
