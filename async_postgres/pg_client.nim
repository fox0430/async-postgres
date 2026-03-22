import std/[options, tables, macros]

import async_backend, pg_protocol, pg_connection, pg_types

const copyBatchSize = 262144 ## 256KB batch threshold for COPY IN buffering

func toFormatCodes(rf: ResultFormat): seq[int16] =
  ## Convert a high-level ResultFormat to wire-protocol format codes.
  case rf
  of rfAuto:
    @[]
  of rfText:
    @[0'i16]
  of rfBinary:
    @[1'i16]

type
  PreparedStatement* = object ## A server-side prepared statement returned by `prepare`.
    conn*: PgConnection
    name*: string
    fields*: seq[FieldDescription]
    paramOids*: seq[int32]

  Cursor* = ref object
    ## A server-side portal for incremental row fetching via `declareCursor`/`fetch`.
    conn*: PgConnection
    portalName: string
    chunkSize: int32
    timeout: Duration
    fields*: seq[FieldDescription]
    exhausted*: bool
    bufferedData: RowData
    bufferedCount: int32

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

  PipelineOpKind = enum
    pokExec
    pokQuery

  PipelineOp = object
    kind: PipelineOpKind
    sql: string
    params: seq[Option[seq[byte]]]
    paramOids: seq[int32]
    paramFormats: seq[int16]
    resultFormats: seq[int16]
    # Set during send phase
    cacheHit: bool
    cacheMiss: bool
    stmtName: string

  PipelineResultKind* = enum
    ## Discriminator for pipeline result variants.
    prkExec
    prkQuery

  PipelineResult* = object ## Result of a single operation within a pipeline.
    case kind*: PipelineResultKind
    of prkExec:
      commandResult*: CommandResult
    of prkQuery:
      queryResult*: QueryResult

  Pipeline* = ref object
    ## Batch of queries/execs sent through the PostgreSQL pipeline protocol.
    conn: PgConnection
    ops: seq[PipelineOp]

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

proc columnIndex*(stmt: PreparedStatement, name: string): int =
  ## Find the index of a column by name in a prepared statement.
  stmt.fields.columnIndex(name)

proc columnIndex*(cursor: Cursor, name: string): int =
  ## Find the index of a column by name in a cursor.
  cursor.fields.columnIndex(name)

proc extractParams(
    params: openArray[PgParam]
): tuple[oids: seq[int32], formats: seq[int16], values: seq[Option[seq[byte]]]] =
  result.oids = newSeq[int32](params.len)
  result.formats = newSeq[int16](params.len)
  result.values = newSeq[Option[seq[byte]]](params.len)
  for i, p in params:
    result.oids[i] = p.oid
    result.formats[i] = p.format
    result.values[i] = p.value

proc execImpl(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  var cacheMiss = false
  var stmtName = ""

  conn.sendBuf.setLen(0)
  if cacheHit:
    stmtName = cached.name
    conn.sendBuf.addBind("", stmtName, formats, params)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, paramOids)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, formats, params)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    conn.sendBuf.addParse("", sql, paramOids)
    conn.sendBuf.addBind("", "", formats, params)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var commandTag = ""
  var queryError: ref PgQueryError
  var cachedFields: seq[FieldDescription]

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          discard
        of bmkRowDescription:
          if cacheMiss:
            cachedFields = msg.fields
        of bmkNoData:
          discard
        of bmkDataRow:
          discard # exec ignores rows
        of bmkCommandComplete:
          commandTag = msg.commandTag
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

  return commandTag

proc execImpl(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  var cacheMiss = false
  var stmtName = ""

  conn.sendBuf.setLen(0)
  if cacheHit:
    stmtName = cached.name
    conn.sendBuf.addBind("", stmtName, params)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, params)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, params)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    conn.sendBuf.addParse("", sql, params)
    conn.sendBuf.addBind("", "", params)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var commandTag = ""
  var queryError: ref PgQueryError
  var cachedFields: seq[FieldDescription]

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          discard
        of bmkRowDescription:
          if cacheMiss:
            cachedFields = msg.fields
        of bmkNoData:
          discard
        of bmkDataRow:
          discard # exec ignores rows
        of bmkCommandComplete:
          commandTag = msg.commandTag
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

  return commandTag

proc exec(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a single SQL statement via extended query protocol, returning the command tag.
  ## Only one statement is allowed; use `simpleQuery` for multiple statements.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await execImpl(conn, sql, params, paramOids, paramFormats, timeout).wait(
        timeout
      )
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "Exec timed out")
  else:
    return await execImpl(conn, sql, params, paramOids, paramFormats)

proc exec*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with typed parameters.
  var tag: string
  if timeout > ZeroDuration:
    try:
      tag = await execImpl(conn, sql, params, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "Exec timed out")
  else:
    tag = await execImpl(conn, sql, params)
  return initCommandResult(tag)

template queryRecvLoop(
    conn: PgConnection,
    sql: string,
    resultFormats: openArray[int16],
    cacheHit, cacheMiss: bool,
    stmtName: string,
    cachedFields: var seq[FieldDescription],
    cachedColFmts: seq[int16],
    cachedColOids: seq[int32],
    qr: var QueryResult,
    timeout: Duration,
) =
  var queryError: ref PgQueryError

  if cacheHit:
    swap(qr.fields, cachedFields)
    if resultFormats.len > 0 and cachedColFmts.len > 0:
      for i in 0 ..< qr.fields.len:
        qr.fields[i].formatCode = cachedColFmts[i]
    if qr.fields.len > 0:
      if conn.rowDataBuf != nil:
        conn.rowDataBuf = conn.rowDataBuf.reuseRowData(
          int16(qr.fields.len), cachedColFmts, cachedColOids
        )
      else:
        conn.rowDataBuf = newRowData(int16(qr.fields.len), cachedColFmts, cachedColOids)
      conn.rowDataBuf.fields = qr.fields
      qr.data = conn.rowDataBuf

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
          if conn.rowDataBuf != nil:
            conn.rowDataBuf = conn.rowDataBuf.reuseRowData(int16(qr.fields.len), cf, co)
          else:
            conn.rowDataBuf = newRowData(int16(qr.fields.len), cf, co)
          conn.rowDataBuf.fields = qr.fields
          qr.data = conn.rowDataBuf
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

proc queryImpl(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  conn.sendBuf.setLen(0)
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    conn.sendBuf.addBind("", stmtName, formats, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, paramOids)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, formats, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    effectiveResultFormats = resultFormats
    conn.sendBuf.addParse("", sql, paramOids)
    conn.sendBuf.addBind("", "", formats, params, effectiveResultFormats)
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var qr = QueryResult()
  queryRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, qr, timeout,
  )
  return qr

proc queryImpl(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  conn.sendBuf.setLen(0)
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, params)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    effectiveResultFormats = resultFormats
    conn.sendBuf.addParse("", sql, params)
    conn.sendBuf.addBind("", "", params, effectiveResultFormats)
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var qr = QueryResult()
  queryRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, qr, timeout,
  )
  return qr

template queryEachRecvLoop(
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
    timeout: Duration,
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

  block recvLoop:
    while true:
      # Parse messages directly from recvBuf using parseBackendMessage
      var pos = conn.recvBufStart
      while true:
        var consumed: int
        let res = parseBackendMessage(
          conn.recvBuf.toOpenArray(pos, conn.recvBuf.len - 1), consumed, rd
        )
        if res.state == psIncomplete:
          conn.recvBufStart = pos
          break # need more data
        pos += consumed
        if res.state == psDataRow:
          # DataRow was parsed into rd — invoke callback, then reset for next row
          if callbackError == nil:
            try:
              callback(Row(data: rd, rowIdx: 0))
            except CatchableError as e:
              callbackError = e
          rowCount += 1
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

proc queryEachImpl(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    callback: RowCallback,
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  conn.sendBuf.setLen(0)
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, params)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    effectiveResultFormats = resultFormats
    conn.sendBuf.addParse("", sql, params)
    conn.sendBuf.addBind("", "", params, effectiveResultFormats)
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var rowCount: int64 = 0
  queryEachRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, callback, rowCount, timeout,
  )
  return rowCount

proc queryEach*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a query with typed parameters, invoking `callback` once per row.
  ## Returns the number of rows processed.
  let resultFormats = resultFormat.toFormatCodes()
  if timeout > ZeroDuration:
    try:
      return await queryEachImpl(conn, sql, params, callback, resultFormats, timeout)
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "queryEach timed out")
  else:
    return await queryEachImpl(conn, sql, params, callback, resultFormats)

proc query(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a single SQL query via extended query protocol, returning rows.
  ## Only one statement is allowed; use `simpleQuery` for multiple statements.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await queryImpl(
        conn, sql, params, paramOids, paramFormats, resultFormats, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "Query timed out")
  else:
    return await queryImpl(conn, sql, params, paramOids, paramFormats, resultFormats)

proc query*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with typed parameters.
  let resultFormats = resultFormat.toFormatCodes()
  if timeout > ZeroDuration:
    try:
      return await queryImpl(conn, sql, params, resultFormats, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "Query timed out")
  else:
    return await queryImpl(conn, sql, params, resultFormats)

proc queryOne*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query and return the first row, or `none` if no rows.
  let qr = await conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  if qr.rowCount > 0:
    if qr.fields.len > 0 and qr.data.fields.len == 0:
      qr.data.fields = qr.fields
    return some(Row(data: qr.data, rowIdx: 0))
  else:
    return none(Row)

proc queryValue*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Raises PgError if no rows are returned or the value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgError, "Query returned no rows")
  let row = Row(data: qr.data, rowIdx: 0)
  if row.isNull(0):
    raise newException(PgError, "Query returned NULL")
  return row.getStr(0)

proc queryValue*[T](
    conn: PgConnection,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Raises PgError if no rows are returned or the value is NULL.
  ## Supported types: int32, int64, float64, bool, string.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgError, "Query returned no rows")
  let row = Row(data: qr.data, rowIdx: 0)
  if row.isNull(0):
    raise newException(PgError, "Query returned NULL")
  return row.get(0, T)

proc queryValueOpt*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[string]] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `none` if no rows are returned or the value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return none(string)
  let row = Row(data: qr.data, rowIdx: 0)
  if row.isNull(0):
    return none(string)
  return some(row.getStr(0))

proc queryValueOpt*[T](
    conn: PgConnection,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[T]] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Returns `none` if no rows are returned or the value is NULL.
  ## Supported types: int32, int64, float64, bool, string.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return none(T)
  let row = Row(data: qr.data, rowIdx: 0)
  if row.isNull(0):
    return none(T)
  return some(row.get(0, T))

proc queryValueOrDefault*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `default` if no rows or the value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return default
  let row = Row(data: qr.data, rowIdx: 0)
  if row.isNull(0):
    return default
  return row.getStr(0)

proc queryValueOrDefault*[T](
    conn: PgConnection,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Returns `default` if no rows or the value is NULL.
  ## Supported types: int32, int64, float64, bool, string.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return default
  let row = Row(data: qr.data, rowIdx: 0)
  if row.isNull(0):
    return default
  return row.get(0, T)

proc queryExists*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a query and return whether any rows exist.
  let qr = await conn.query(sql, params, timeout = timeout)
  return qr.rowCount > 0

proc queryColumn*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  ## Execute a query and return the first column of all rows as strings.
  ## Raises PgTypeError if any value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  for i in 0 ..< qr.rowCount:
    let row = Row(data: qr.data, rowIdx: i)
    if row.isNull(0):
      raise newException(PgTypeError, "NULL value in column")
    result.add(row.getStr(0))

proc prepareImpl(
    conn: PgConnection, name: string, sql: string, timeout: Duration = ZeroDuration
): Future[PreparedStatement] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  var batch = newSeqOfCap[byte](sql.len + name.len + 32)
  batch.addParse(name, sql)
  batch.addDescribe(dkStatement, name)
  batch.addSync()
  await conn.sendMsg(batch)

  var stmt = PreparedStatement(conn: conn, name: name)
  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete:
          discard
        of bmkParameterDescription:
          stmt.paramOids = msg.paramTypeOids
        of bmkRowDescription:
          stmt.fields = msg.fields
        of bmkNoData:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return stmt

proc prepare*(
    conn: PgConnection, name: string, sql: string, timeout: Duration = ZeroDuration
): Future[PreparedStatement] {.async.} =
  ## Prepare a named statement, returning metadata.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await prepareImpl(conn, name, sql, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "Prepare timed out")
  else:
    return await prepareImpl(conn, name, sql)

proc executeImpl(
    stmt: PreparedStatement,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  let conn = stmt.conn

  conn.checkReady()
  conn.state = csBusy

  # Coerce binary parameters to match server-inferred types from prepare().
  var coerced: seq[PgParam]
  var needsCoercion = false
  if stmt.paramOids.len == params.len:
    for i in 0 ..< params.len:
      if params[i].format == 1 and params[i].oid != stmt.paramOids[i] and
          stmt.paramOids[i] != 0:
        if not needsCoercion:
          coerced = params
          needsCoercion = true
        coerced[i] = coerceBinaryParam(params[i], stmt.paramOids[i])

  conn.sendBuf.setLen(0)
  conn.sendBuf.addBind(
    "", stmt.name, if needsCoercion: coerced else: params, resultFormats
  )
  conn.sendBuf.addExecute("", 0)
  conn.sendBuf.addSync()
  await conn.sendBufMsg()

  var qr = QueryResult(fields: stmt.fields)
  if resultFormats.len > 0:
    for i in 0 ..< qr.fields.len:
      if resultFormats.len == 1:
        qr.fields[i].formatCode = resultFormats[0]
      elif i < resultFormats.len:
        qr.fields[i].formatCode = resultFormats[i]
  if qr.fields.len > 0:
    var colFmts = newSeq[int16](qr.fields.len)
    var colOids = newSeq[int32](qr.fields.len)
    for i in 0 ..< qr.fields.len:
      colFmts[i] = qr.fields[i].formatCode
      colOids[i] = qr.fields[i].typeOid
    if conn.rowDataBuf != nil:
      conn.rowDataBuf =
        conn.rowDataBuf.reuseRowData(int16(qr.fields.len), colFmts, colOids)
    else:
      conn.rowDataBuf = newRowData(int16(qr.fields.len), colFmts, colOids)
    conn.rowDataBuf.fields = qr.fields
    qr.data = conn.rowDataBuf
  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(qr.data, addr qr.rowCount); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkBindComplete:
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
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return qr

proc execute*(
    stmt: PreparedStatement,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a prepared statement with typed parameters.
  let resultFormats = resultFormat.toFormatCodes()
  if timeout > ZeroDuration:
    try:
      return await executeImpl(stmt, params, resultFormats, timeout).wait(timeout)
    except AsyncTimeoutError:
      stmt.conn.state = csClosed
      raise newException(PgTimeoutError, "Execute timed out")
  else:
    return await executeImpl(stmt, params, resultFormats)

proc closeImpl(
    stmt: PreparedStatement, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  let conn = stmt.conn

  conn.checkReady()
  conn.state = csBusy

  var batch = newSeqOfCap[byte](stmt.name.len + 16)
  batch.addClose(dkStatement, stmt.name)
  batch.addSync()
  await conn.sendMsg(batch)

  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCloseComplete:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

proc close*(
    stmt: PreparedStatement, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Close a prepared statement.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      await closeImpl(stmt, timeout).wait(timeout)
    except AsyncTimeoutError:
      stmt.conn.state = csClosed
      raise newException(PgTimeoutError, "Statement close timed out")
  else:
    await closeImpl(stmt)

proc notify*(
    conn: PgConnection,
    channel: string,
    payload: string = "",
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Send a NOTIFY on `channel` with optional `payload`.
  ## Uses NOTIFY for empty payloads, pg_notify() otherwise.
  let quoted = quoteIdentifier(channel)
  if payload.len == 0:
    discard await conn.exec("NOTIFY " & quoted, timeout = timeout)
  else:
    discard await conn.exec(
      "SELECT pg_notify($1, $2)",
      @[channel.toPgParam, payload.toPgParam],
      timeout = timeout,
    )

proc copyInRawImpl(
    conn: PgConnection, sql: string, data: seq[byte], timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var commandTag = ""
  var queryError: ref PgQueryError

  # Wait for CopyInResponse (or error)
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyInResponse:
          break recvLoop
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          return commandTag
        else:
          discard
      await conn.fillRecvBuf(timeout)

  # Send CopyData in batches, slicing from the input buffer
  const maxPayload = copyBatchSize - 5 # leave room for CopyData header
  conn.sendBuf.setLen(0)
  var offset = 0
  while offset < data.len:
    let endIdx = min(offset + maxPayload - 1, data.len - 1)
    encodeCopyData(conn.sendBuf, data.toOpenArray(offset, endIdx))
    offset = endIdx + 1
    if conn.sendBuf.len >= copyBatchSize:
      await conn.sendBufMsg()
      conn.sendBuf.setLen(0)
  # Flush remaining data + CopyDone in one send
  conn.sendBuf.addCopyDone()
  await conn.sendBufMsg()
  conn.sendBuf.setLen(0)

  # Wait for CommandComplete + ReadyForQuery
  block recvLoop2:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCommandComplete:
          commandTag = msg.commandTag
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop2
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return commandTag

proc copyIn*(
    conn: PgConnection, sql: string, data: seq[byte], timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute COPY ... FROM STDIN with a single contiguous ``seq[byte]``.
  ## Avoids the copy that the ``openArray[byte]`` overload performs.
  var tag: string
  if timeout > ZeroDuration:
    try:
      tag = await copyInRawImpl(conn, sql, data, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "COPY IN timed out")
  else:
    tag = await copyInRawImpl(conn, sql, data)
  return initCommandResult(tag)

proc copyIn*(
    conn: PgConnection,
    sql: string,
    data: openArray[byte],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] =
  ## Execute COPY ... FROM STDIN with a single contiguous buffer.
  ## Slices `data` into CopyData messages internally.
  ## Returns the command result (e.g. "COPY 5").
  let dataCopy = @data # copy openArray to seq before async boundary
  copyIn(conn, sql, dataCopy, timeout)

proc copyIn*(
    conn: PgConnection, sql: string, data: string, timeout: Duration = ZeroDuration
): Future[CommandResult] =
  ## Execute COPY ... FROM STDIN with text data as a string.
  ## Converts to bytes internally; avoids manual toOpenArrayByte.
  var bytes = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr bytes[0], unsafeAddr data[0], data.len)
  copyIn(conn, sql, bytes, timeout)

proc copyIn*(
    conn: PgConnection,
    sql: string,
    data: seq[seq[byte]],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] =
  ## Execute COPY ... FROM STDIN via simple query protocol.
  ## Concatenates chunks and delegates to the ``seq[byte]`` overload.
  ## Returns the command result (e.g. "COPY 5").
  var totalLen = 0
  for chunk in data:
    totalLen += chunk.len
  var combined = newSeq[byte](totalLen)
  var offset = 0
  for chunk in data:
    if chunk.len > 0:
      copyMem(addr combined[offset], unsafeAddr chunk[0], chunk.len)
      offset += chunk.len
  copyIn(conn, sql, combined, timeout)

proc copyInStreamImpl(
    conn: PgConnection,
    sql: string,
    callback: CopyInCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyInInfo] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var info = CopyInInfo()
  var queryError: ref PgQueryError

  # Wait for CopyInResponse (or error)
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyInResponse:
          info.format = msg.copyFormat
          info.columnFormats = msg.copyColumnFormats
          break recvLoop
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          return info
        else:
          discard
      await conn.fillRecvBuf(timeout)

  # Pull data from callback and send as CopyData in batches
  const batchThreshold = copyBatchSize
  var callbackError: ref CatchableError = nil
  conn.sendBuf.setLen(0)
  try:
    while true:
      let chunk = await callback()
      if chunk.len == 0:
        break
      encodeCopyData(conn.sendBuf, chunk)
      if conn.sendBuf.len >= batchThreshold:
        await conn.sendBufMsg()
        conn.sendBuf.setLen(0)
  except CatchableError as e:
    callbackError = e

  if callbackError != nil:
    # Callback raised: flush pending data is pointless, send CopyFail
    conn.sendBuf.setLen(0)
    await conn.sendMsg(encodeCopyFail(callbackError.msg))
    block drainLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            break drainLoop
          else:
            discard
        await conn.fillRecvBuf(timeout)
    raise callbackError
  else:
    # Normal completion: flush remaining data + CopyDone in one send
    conn.sendBuf.addCopyDone()
    await conn.sendBufMsg()
    conn.sendBuf.setLen(0)

  # Wait for CommandComplete + ReadyForQuery
  block recvLoop2:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCommandComplete:
          info.commandTag = msg.commandTag
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop2
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return info

proc copyInStream*(
    conn: PgConnection,
    sql: string,
    callback: CopyInCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyInInfo] {.async.} =
  ## Execute COPY ... FROM STDIN via simple query protocol, streaming data
  ## from ``callback``. The callback is called repeatedly; returning an empty
  ## ``seq[byte]`` signals EOF. If the callback raises, CopyFail is sent and
  ## the connection returns to csReady.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await copyInStreamImpl(conn, sql, callback, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "COPY IN stream timed out")
  else:
    return await copyInStreamImpl(conn, sql, callback)

proc copyOutImpl(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CopyResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var cr = CopyResult()
  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyOutResponse:
          cr.format = msg.copyFormat
          cr.columnFormats = msg.copyColumnFormats
        of bmkCopyData:
          cr.data.add(msg.copyData)
        of bmkCopyDone:
          discard
        of bmkCommandComplete:
          cr.commandTag = msg.commandTag
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return cr

proc copyOut*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CopyResult] {.async.} =
  ## Execute COPY ... TO STDOUT via simple query protocol.
  ## Collects all CopyData messages and returns them in a CopyResult.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await copyOutImpl(conn, sql, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "COPY OUT timed out")
  else:
    return await copyOutImpl(conn, sql)

proc copyOutStreamImpl(
    conn: PgConnection,
    sql: string,
    callback: CopyOutCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyOutInfo] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var info = CopyOutInfo()
  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyOutResponse:
          info.format = msg.copyFormat
          info.columnFormats = msg.copyColumnFormats
        of bmkCopyData:
          try:
            await callback(msg.copyData)
          except CatchableError as e:
            conn.state = csClosed
            raise e
        of bmkCopyDone:
          discard
        of bmkCommandComplete:
          info.commandTag = msg.commandTag
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return info

proc copyOutStream*(
    conn: PgConnection,
    sql: string,
    callback: CopyOutCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyOutInfo] {.async.} =
  ## Execute COPY ... TO STDOUT via simple query protocol, streaming each
  ## CopyData chunk through `callback`. The callback is awaited, providing
  ## natural TCP backpressure. If the callback raises, the connection is
  ## marked csClosed (protocol cannot be resynchronized).
  if timeout > ZeroDuration:
    try:
      return await copyOutStreamImpl(conn, sql, callback, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "COPY OUT stream timed out")
  else:
    return await copyOutStreamImpl(conn, sql, callback)

template withTransaction*(
    conn: PgConnection, body: untyped, txTimeout: Duration = ZeroDuration
) =
  ## Execute `body` inside a BEGIN/COMMIT transaction.
  ## On exception, ROLLBACK is issued automatically.
  ## **Note:** Do not use `return` inside the body.
  discard await conn.simpleExec("BEGIN", timeout = txTimeout)
  try:
    body
    discard await conn.simpleExec("COMMIT", timeout = txTimeout)
  except CatchableError as e:
    try:
      discard await conn.simpleExec("ROLLBACK", timeout = txTimeout)
    except CatchableError:
      discard
    raise e

template withTransaction*(
    conn: PgConnection,
    opts: TransactionOptions,
    body: untyped,
    txTimeout: Duration = ZeroDuration,
) =
  ## Execute `body` inside a transaction with custom options.
  ## On exception, ROLLBACK is issued automatically.
  let beginSql = buildBeginSql(opts)
  discard await conn.simpleExec(beginSql, timeout = txTimeout)
  try:
    body
    discard await conn.simpleExec("COMMIT", timeout = txTimeout)
  except CatchableError as e:
    try:
      discard await conn.simpleExec("ROLLBACK", timeout = txTimeout)
    except CatchableError:
      discard
    raise e

template withSavepoint*(
    conn: PgConnection, body: untyped, spTimeout: Duration = ZeroDuration
) =
  ## Execute `body` inside a SAVEPOINT with an auto-generated name.
  ## On exception, ROLLBACK TO SAVEPOINT is issued automatically.
  inc conn.portalCounter
  let spName = "_sp_" & $conn.portalCounter
  discard await conn.simpleExec("SAVEPOINT " & spName, timeout = spTimeout)
  try:
    body
    discard await conn.simpleExec("RELEASE SAVEPOINT " & spName, timeout = spTimeout)
  except CatchableError as e:
    try:
      discard
        await conn.simpleExec("ROLLBACK TO SAVEPOINT " & spName, timeout = spTimeout)
    except CatchableError:
      discard
    raise e

template withSavepoint*(
    conn: PgConnection, name: string, body: untyped, spTimeout: Duration = ZeroDuration
) =
  ## Execute `body` inside a SAVEPOINT with the given `name`.
  ## On exception, ROLLBACK TO SAVEPOINT is issued automatically.
  discard await conn.simpleExec("SAVEPOINT " & name, timeout = spTimeout)
  try:
    body
    discard await conn.simpleExec("RELEASE SAVEPOINT " & name, timeout = spTimeout)
  except CatchableError as e:
    try:
      discard
        await conn.simpleExec("ROLLBACK TO SAVEPOINT " & name, timeout = spTimeout)
    except CatchableError:
      discard
    raise e

proc execInTransactionImpl(
    conn: PgConnection,
    beginSql: string,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    timeout: Duration,
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)

  # Pipeline: Parse+Bind+Execute for BEGIN, user SQL, COMMIT + single Sync
  conn.sendBuf.setLen(0)
  # BEGIN
  conn.sendBuf.addParse("", beginSql)
  conn.sendBuf.addBind("", "", @[], @[])
  conn.sendBuf.addExecute("", 0)
  # User SQL
  conn.sendBuf.addParse("", sql, paramOids)
  conn.sendBuf.addBind("", "", formats, params)
  conn.sendBuf.addExecute("", 0)
  # COMMIT
  conn.sendBuf.addParse("", "COMMIT")
  conn.sendBuf.addBind("", "", @[], @[])
  conn.sendBuf.addExecute("", 0)
  # Single Sync
  conn.sendBuf.addSync()
  await conn.sendBufMsg()

  # Parse response: 3 phases (BEGIN=0, user=1, COMMIT=2)
  var phase = 0
  var userCommandTag = ""
  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete:
          discard
        of bmkRowDescription, bmkDataRow, bmkEmptyQueryResponse:
          discard
        of bmkCommandComplete:
          if phase == 1:
            userCommandTag = msg.commandTag
          inc phase
        of bmkErrorResponse:
          if queryError == nil:
            queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            # Error occurred: if we're in a failed transaction, send ROLLBACK
            if msg.txStatus == tsInFailedTransaction:
              discard await conn.simpleExec("ROLLBACK")
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return userCommandTag

proc execInTransaction(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a statement inside a pipelined BEGIN/COMMIT transaction (1 round trip).
  ## On error, ROLLBACK is issued automatically.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await execInTransactionImpl(
        conn, "BEGIN", sql, params, paramOids, paramFormats, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "execInTransaction timed out")
  else:
    return await execInTransactionImpl(
      conn, "BEGIN", sql, params, paramOids, paramFormats, timeout
    )

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined transaction with typed parameters.
  let (oids, formats, values) = extractParams(params)
  let tag = await conn.execInTransaction(sql, values, oids, formats, timeout)
  return initCommandResult(tag)

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    opts: TransactionOptions,
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined transaction with options.
  let (oids, formats, values) = extractParams(params)
  let beginSql = buildBeginSql(opts)
  var tag: string
  if timeout > ZeroDuration:
    try:
      tag = await execInTransactionImpl(
        conn, beginSql, sql, values, oids, formats, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "execInTransaction timed out")
  else:
    tag =
      await execInTransactionImpl(conn, beginSql, sql, values, oids, formats, timeout)
  return initCommandResult(tag)

proc queryInTransactionImpl(
    conn: PgConnection,
    beginSql: string,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16],
    timeout: Duration,
): Future[QueryResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)

  # Pipeline: Parse+Bind+Execute for BEGIN, user SQL (with Describe), COMMIT + Sync
  conn.sendBuf.setLen(0)
  # BEGIN
  conn.sendBuf.addParse("", beginSql)
  conn.sendBuf.addBind("", "", @[], @[])
  conn.sendBuf.addExecute("", 0)
  # User SQL
  conn.sendBuf.addParse("", sql, paramOids)
  conn.sendBuf.addBind("", "", formats, params, resultFormats)
  conn.sendBuf.addDescribe(dkPortal, "")
  conn.sendBuf.addExecute("", 0)
  # COMMIT
  conn.sendBuf.addParse("", "COMMIT")
  conn.sendBuf.addBind("", "", @[], @[])
  conn.sendBuf.addExecute("", 0)
  # Single Sync
  conn.sendBuf.addSync()
  await conn.sendBufMsg()

  var qr = QueryResult()
  var phase = 0
  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(qr.data, addr qr.rowCount); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete:
          discard
        of bmkRowDescription:
          qr.fields = msg.fields
          qr.data = newRowData(int16(msg.fields.len))
        of bmkNoData:
          discard
        of bmkEmptyQueryResponse:
          discard
        of bmkCommandComplete:
          if phase == 1:
            qr.commandTag = msg.commandTag
          inc phase
        of bmkErrorResponse:
          if queryError == nil:
            queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            if msg.txStatus == tsInFailedTransaction:
              discard await conn.simpleExec("ROLLBACK")
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return qr

proc queryInTransaction(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined BEGIN/COMMIT transaction (1 round trip).
  ## Returns rows. On error, ROLLBACK is issued automatically.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await queryInTransactionImpl(
        conn, "BEGIN", sql, params, paramOids, paramFormats, resultFormats, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "queryInTransaction timed out")
  else:
    return await queryInTransactionImpl(
      conn, "BEGIN", sql, params, paramOids, paramFormats, resultFormats, timeout
    )

proc queryInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with typed parameters.
  let (oids, formats, values) = extractParams(params)
  let resultFormats = resultFormat.toFormatCodes()
  return
    await conn.queryInTransaction(sql, values, oids, formats, resultFormats, timeout)

proc queryInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    opts: TransactionOptions,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with options.
  let (oids, formats, values) = extractParams(params)
  let resultFormats = resultFormat.toFormatCodes()
  let beginSql = buildBeginSql(opts)
  if timeout > ZeroDuration:
    try:
      return await queryInTransactionImpl(
        conn, beginSql, sql, values, oids, formats, resultFormats, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "queryInTransaction timed out")
  else:
    return await queryInTransactionImpl(
      conn, beginSql, sql, values, oids, formats, resultFormats, timeout
    )

proc newPipeline*(conn: PgConnection): Pipeline =
  ## Create a new pipeline for batching multiple operations into a single round trip.
  Pipeline(conn: conn, ops: @[])

proc addExec*(p: Pipeline, sql: string, params: seq[PgParam] = @[]) =
  ## Add an exec operation to the pipeline with typed parameters.
  var op = PipelineOp(kind: pokExec, sql: sql)
  if params.len > 0:
    op.paramOids = newSeqOfCap[int32](params.len)
    op.paramFormats = newSeqOfCap[int16](params.len)
    op.params = newSeqOfCap[Option[seq[byte]]](params.len)
    for param in params:
      op.paramOids.add param.oid
      op.paramFormats.add param.format
      op.params.add param.value
  p.ops.add move(op)

proc addQuery*(
    p: Pipeline,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
) =
  ## Add a query operation to the pipeline with typed parameters.
  let (oids, formats, values) = extractParams(params)
  p.ops.add PipelineOp(
    kind: pokQuery,
    sql: sql,
    params: values,
    paramOids: oids,
    paramFormats: formats,
    resultFormats: resultFormat.toFormatCodes(),
  )

proc executeImpl(
    p: Pipeline, timeout: Duration
): Future[seq[PipelineResult]] {.async.} =
  let conn = p.conn
  conn.checkReady()
  conn.state = csBusy

  # Send Phase — also collect CachedStmt data needed by receive phase
  conn.sendBuf.setLen(0)
  var cachedStmts: seq[CachedStmt] # lazy-init, only populated for pokQuery cache-hit ops
  var hasCachedStmts = false
  var pendingCacheAdds = 0 # track pending additions for LRU eviction in pipeline
  var defaultFormats: seq[int16] # reused across ops when paramFormats is empty

  for i in 0 ..< p.ops.len:
    let formats =
      if p.ops[i].paramFormats.len > 0:
        p.ops[i].paramFormats
      else:
        let needed = p.ops[i].params.len
        if defaultFormats.len != needed:
          defaultFormats = newSeq[int16](needed)
        defaultFormats

    let cached = conn.lookupStmtCache(p.ops[i].sql)
    p.ops[i].cacheHit = cached != nil
    p.ops[i].cacheMiss = false

    if cached != nil:
      p.ops[i].stmtName = cached.name
      if p.ops[i].kind == pokQuery:
        if not hasCachedStmts:
          cachedStmts = newSeq[CachedStmt](p.ops.len)
          hasCachedStmts = true
        cachedStmts[i] = cached[]
      var effectiveResultFormats: seq[int16]
      if p.ops[i].kind == pokQuery:
        effectiveResultFormats =
          if p.ops[i].resultFormats.len == 0:
            cached.resultFormats
          else:
            p.ops[i].resultFormats
        p.ops[i].resultFormats = effectiveResultFormats
      conn.sendBuf.addBind(
        "", cached.name, formats, p.ops[i].params, effectiveResultFormats
      )
      conn.sendBuf.addExecute("", 0)
    elif conn.stmtCacheCapacity > 0:
      p.ops[i].cacheMiss = true
      p.ops[i].stmtName = conn.nextStmtName()
      if conn.stmtCache.len + pendingCacheAdds >= conn.stmtCacheCapacity and
          conn.stmtCache.len > 0:
        let evicted = conn.evictStmtCache()
        conn.sendBuf.addClose(dkStatement, evicted.name)
      inc pendingCacheAdds
      conn.sendBuf.addParse(p.ops[i].stmtName, p.ops[i].sql, p.ops[i].paramOids)
      conn.sendBuf.addDescribe(dkStatement, p.ops[i].stmtName)
      conn.sendBuf.addBind(
        "", p.ops[i].stmtName, formats, p.ops[i].params, p.ops[i].resultFormats
      )
      conn.sendBuf.addExecute("", 0)
    else:
      conn.sendBuf.addParse("", p.ops[i].sql, p.ops[i].paramOids)
      conn.sendBuf.addBind("", "", formats, p.ops[i].params, p.ops[i].resultFormats)
      if p.ops[i].kind == pokQuery:
        conn.sendBuf.addDescribe(dkPortal, "")
      conn.sendBuf.addExecute("", 0)

  conn.sendBuf.addSync()
  await conn.sendBufMsg()

  # Receive Phase
  var results = newSeq[PipelineResult](p.ops.len)
  var activeOpIdx = 0
  var queryError: ref PgQueryError
  var cachedFieldsPerOp: seq[seq[FieldDescription]] # lazy-init for cache misses

  # Initialize query results
  for i in 0 ..< p.ops.len:
    if p.ops[i].kind == pokQuery:
      results[i] = PipelineResult(kind: prkQuery)
      if p.ops[i].cacheHit:
        let c = cachedStmts[i]
        results[i].queryResult.fields = c.fields
        if p.ops[i].resultFormats.len > 0 and c.colFmts.len > 0:
          for j in 0 ..< results[i].queryResult.fields.len:
            results[i].queryResult.fields[j].formatCode = c.colFmts[j]
        if results[i].queryResult.fields.len > 0:
          results[i].queryResult.data =
            newRowData(int16(results[i].queryResult.fields.len), c.colFmts, c.colOids)
    else:
      results[i] = PipelineResult(kind: prkExec)

  block recvLoop:
    while true:
      var rowData: RowData = nil
      var rowCount: ptr int32 = nil
      if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
        rowData = results[activeOpIdx].queryResult.data
        rowCount = addr results[activeOpIdx].queryResult.rowCount

      while (let opt = conn.nextMessage(rowData, rowCount); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          discard
        of bmkRowDescription:
          if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
            var cf: seq[int16]
            var co: seq[int32]
            if p.ops[activeOpIdx].cacheMiss:
              if cachedFieldsPerOp.len == 0:
                cachedFieldsPerOp = newSeq[seq[FieldDescription]](p.ops.len)
              cachedFieldsPerOp[activeOpIdx] = msg.fields
              results[activeOpIdx].queryResult.fields = msg.fields
              if p.ops[activeOpIdx].resultFormats.len > 0:
                cf = newSeq[int16](msg.fields.len)
                co = newSeq[int32](msg.fields.len)
                for j in 0 ..< msg.fields.len:
                  co[j] = msg.fields[j].typeOid
                  if p.ops[activeOpIdx].resultFormats.len == 1:
                    results[activeOpIdx].queryResult.fields[j].formatCode =
                      p.ops[activeOpIdx].resultFormats[0]
                    cf[j] = p.ops[activeOpIdx].resultFormats[0]
                  elif j < p.ops[activeOpIdx].resultFormats.len:
                    results[activeOpIdx].queryResult.fields[j].formatCode =
                      p.ops[activeOpIdx].resultFormats[j]
                    cf[j] = p.ops[activeOpIdx].resultFormats[j]
            else:
              results[activeOpIdx].queryResult.fields = msg.fields
            results[activeOpIdx].queryResult.data =
              newRowData(int16(msg.fields.len), cf, co)
            # Update pointers for nextMessage
            rowData = results[activeOpIdx].queryResult.data
            rowCount = addr results[activeOpIdx].queryResult.rowCount
        of bmkNoData:
          discard
        of bmkCommandComplete:
          if activeOpIdx < p.ops.len:
            if p.ops[activeOpIdx].kind == pokExec:
              results[activeOpIdx].commandResult = initCommandResult(msg.commandTag)
            else:
              results[activeOpIdx].queryResult.commandTag = msg.commandTag
            inc activeOpIdx
            # Update rowData/rowCount for next op
            if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
              rowData = results[activeOpIdx].queryResult.data
              rowCount = addr results[activeOpIdx].queryResult.rowCount
            else:
              rowData = nil
              rowCount = nil
        of bmkEmptyQueryResponse:
          if activeOpIdx < p.ops.len:
            inc activeOpIdx
            if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
              rowData = results[activeOpIdx].queryResult.data
              rowCount = addr results[activeOpIdx].queryResult.rowCount
            else:
              rowData = nil
              rowCount = nil
        of bmkErrorResponse:
          if queryError == nil:
            queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            # Invalidate cache for 26000 (prepared statement does not exist)
            if queryError.sqlState == "26000":
              for i in 0 ..< p.ops.len:
                if p.ops[i].cacheHit:
                  conn.removeStmtCache(p.ops[i].sql)
            raise queryError
          # Cache misses: add to cache
          for i in 0 ..< p.ops.len:
            if p.ops[i].cacheMiss:
              let fields =
                if cachedFieldsPerOp.len > 0:
                  cachedFieldsPerOp[i]
                else:
                  @[]
              conn.addStmtCache(
                p.ops[i].sql, CachedStmt(name: p.ops[i].stmtName, fields: fields)
              )
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return results

proc execute*(
    p: Pipeline, timeout: Duration = ZeroDuration
): Future[seq[PipelineResult]] {.async.} =
  ## Execute all queued pipeline operations in a single round trip.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if p.ops.len == 0:
    return @[]
  if timeout > ZeroDuration:
    try:
      return await executeImpl(p, timeout).wait(timeout)
    except AsyncTimeoutError:
      p.conn.state = csClosed
      raise newException(PgTimeoutError, "Pipeline execute timed out")
  else:
    return await executeImpl(p, timeout)

proc openCursorImpl(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16],
    chunkSize: int32,
    timeout: Duration = ZeroDuration,
): Future[Cursor] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  inc conn.portalCounter
  let portalName = "_cursor_" & $conn.portalCounter

  var batch = newSeqOfCap[byte](sql.len + 128)
  batch.addParse("", sql, paramOids)
  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)
  batch.addBind(portalName, "", formats, params, resultFormats)
  batch.addDescribe(dkPortal, portalName)
  batch.addExecute(portalName, chunkSize)
  batch.addFlush()
  await conn.sendMsg(batch)

  var cursor =
    Cursor(conn: conn, portalName: portalName, chunkSize: chunkSize, exhausted: false)
  var queryError: ref PgQueryError

  block recvLoop:
    while true:
      while (
        let opt = conn.nextMessage(cursor.bufferedData, addr cursor.bufferedCount)
        opt.isSome
      )
      :
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete:
          discard
        of bmkRowDescription:
          cursor.fields = msg.fields
          cursor.bufferedData = newRowData(int16(msg.fields.len))
        of bmkNoData:
          discard
        of bmkPortalSuspended:
          break recvLoop
        of bmkCommandComplete:
          cursor.exhausted = true
          # Need to Sync to get ReadyForQuery
          await conn.sendMsg(encodeSync())
          block drainLoop:
            while true:
              while (let ropt = conn.nextMessage(); ropt.isSome):
                let rmsg = ropt.get
                case rmsg.kind
                of bmkReadyForQuery:
                  conn.txStatus = rmsg.txStatus
                  conn.state = csReady
                  break drainLoop
                else:
                  discard
              await conn.fillRecvBuf(timeout)
          break recvLoop
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
          # Drain until ReadyForQuery
          await conn.sendMsg(encodeSync())
          block errDrain:
            while true:
              while (let ropt = conn.nextMessage(); ropt.isSome):
                if ropt.get.kind == bmkReadyForQuery:
                  conn.txStatus = ropt.get.txStatus
                  conn.state = csReady
                  break errDrain
              await conn.fillRecvBuf(timeout)
          raise queryError
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return cursor

proc openCursor(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]] = @[],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    resultFormats: seq[int16] = @[],
    chunkSize: int32 = 100,
    timeout: Duration = ZeroDuration,
): Future[Cursor] {.async.} =
  ## Open a server-side cursor for streaming rows in chunks.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      result = await openCursorImpl(
        conn, sql, params, paramOids, paramFormats, resultFormats, chunkSize, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgTimeoutError, "Cursor open timed out")
  else:
    result = await openCursorImpl(
      conn, sql, params, paramOids, paramFormats, resultFormats, chunkSize
    )
  result.timeout = timeout

proc fetchNextImpl(
    cursor: Cursor, timeout: Duration = ZeroDuration
): Future[seq[Row]] {.async.} =
  let conn = cursor.conn
  let rd = newRowData(int16(cursor.fields.len))
  var rowCount: int32 = 0

  conn.sendBuf.setLen(0)
  conn.sendBuf.addExecute(cursor.portalName, cursor.chunkSize)
  conn.sendBuf.addFlush()
  await conn.sendBufMsg()

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(rd, addr rowCount); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkPortalSuspended:
          break recvLoop
        of bmkCommandComplete:
          cursor.exhausted = true
          # Close portal and sync
          var closeBatch: seq[byte]
          closeBatch.addClose(dkPortal, cursor.portalName)
          closeBatch.addSync()
          await conn.sendMsg(closeBatch)
          block drainLoop:
            while true:
              while (let ropt = conn.nextMessage(); ropt.isSome):
                let rmsg = ropt.get
                case rmsg.kind
                of bmkCloseComplete:
                  discard
                of bmkReadyForQuery:
                  conn.txStatus = rmsg.txStatus
                  conn.state = csReady
                  break drainLoop
                else:
                  discard
              await conn.fillRecvBuf(timeout)
          break recvLoop
        of bmkErrorResponse:
          let queryError = newPgQueryError(msg.errorFields)
          await conn.sendMsg(encodeSync())
          block errDrain:
            while true:
              while (let ropt = conn.nextMessage(); ropt.isSome):
                if ropt.get.kind == bmkReadyForQuery:
                  conn.txStatus = ropt.get.txStatus
                  conn.state = csReady
                  break errDrain
              await conn.fillRecvBuf(timeout)
          raise queryError
        else:
          discard
      await conn.fillRecvBuf(timeout)

  if rd != nil and cursor.fields.len > 0 and rd.fields.len == 0:
    rd.fields = cursor.fields
  result = newSeq[Row](rowCount)
  for i in 0 ..< rowCount:
    result[i] = Row(data: rd, rowIdx: i)

proc fetchNext*(cursor: Cursor): Future[seq[Row]] {.async.} =
  ## Fetch the next chunk of rows from the cursor.
  ## Returns an empty seq when the cursor is exhausted.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if cursor.bufferedCount > 0:
    if cursor.fields.len > 0 and cursor.bufferedData.fields.len == 0:
      cursor.bufferedData.fields = cursor.fields
    result = newSeq[Row](cursor.bufferedCount)
    for i in 0 ..< cursor.bufferedCount:
      result[i] = Row(data: cursor.bufferedData, rowIdx: i)
    cursor.bufferedData = nil
    cursor.bufferedCount = 0
    return result

  if cursor.exhausted:
    return @[]

  if cursor.timeout > ZeroDuration:
    try:
      return await fetchNextImpl(cursor, cursor.timeout).wait(cursor.timeout)
    except AsyncTimeoutError:
      cursor.conn.state = csClosed
      raise newException(PgTimeoutError, "Cursor fetch timed out")
  else:
    return await fetchNextImpl(cursor)

proc closeCursorImpl(
    cursor: Cursor, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  if cursor.exhausted:
    return

  let conn = cursor.conn
  var batch = newSeqOfCap[byte](cursor.portalName.len + 16)
  batch.addClose(dkPortal, cursor.portalName)
  batch.addSync()
  await conn.sendMsg(batch)

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCloseComplete:
          discard
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  cursor.exhausted = true

proc closeCursor*(cursor: Cursor): Future[void] {.async.} =
  ## Close the cursor and return the connection to ready state.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if cursor.timeout > ZeroDuration:
    try:
      await closeCursorImpl(cursor, cursor.timeout).wait(cursor.timeout)
    except AsyncTimeoutError:
      cursor.conn.state = csClosed
      raise newException(PgTimeoutError, "Cursor close timed out")
  else:
    await closeCursorImpl(cursor)

template withCursor*(
    conn: PgConnection,
    sql: string,
    chunks: int32,
    cursorName, body: untyped,
    cursorTimeout: Duration = ZeroDuration,
) =
  ## Open a cursor, execute `body`, then close the cursor automatically.
  ## The cursor is available as `cursorName` inside the body.
  let cursorName =
    await conn.openCursor(sql, chunkSize = chunks, timeout = cursorTimeout)
  try:
    body
  finally:
    await cursorName.closeCursor()

proc openCursor*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    resultFormat: ResultFormat = rfAuto,
    chunkSize: int32 = 100,
    timeout: Duration = ZeroDuration,
): Future[Cursor] {.async.} =
  ## Open a cursor with typed parameters.
  let (oids, formats, values) = extractParams(params)
  let resultFormats = resultFormat.toFormatCodes()
  return
    await conn.openCursor(sql, values, oids, formats, resultFormats, chunkSize, timeout)

# Zero-alloc query/exec via compile-time macros

proc queryDirectImpl(
    conn: PgConnection,
    sql: string,
    resultFormats: seq[int16],
    colFmts: seq[int16],
    colOids: seq[int32],
    cacheHit: bool,
    cacheMiss: bool,
    stmtName: string,
    cachedFields: seq[FieldDescription],
    timeout: Duration,
): Future[QueryResult] {.async.} =
  ## Shared receive path for queryDirect macros.
  await conn.sendBufMsg()
  var qr = QueryResult()
  var cf = cachedFields
  queryRecvLoop(
    conn, sql, resultFormats, cacheHit, cacheMiss, stmtName, cf, colFmts, colOids, qr,
    timeout,
  )
  return qr

macro queryDirect*(conn: PgConnection, sql: string, args: varargs[untyped]): untyped =
  ## Zero-allocation query: encodes parameters directly into the send buffer
  ## at compile time, avoiding ``seq[PgParam]`` and intermediate ``seq[byte]`` allocs.
  ##
  ## Usage: let qr = await conn.queryDirect("SELECT ... WHERE id = $1", myId)
  result = newStmtList()

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
  let cachedPtrSym = genSym(nskLet, "cachedPtr")
  let cacheHitSym = genSym(nskVar, "cacheHit")
  let cacheMissSym = genSym(nskVar, "cacheMiss")
  let stmtNameSym = genSym(nskVar, "stmtName")
  let cachedFieldsSym = genSym(nskVar, "cachedFields")
  let effectiveRfSym = genSym(nskVar, "effectiveRf")
  let colFmtsSym = genSym(nskVar, "colFmts")
  let colOidsSym = genSym(nskVar, "colOids")

  result.add quote do:
    let `connSym` = `conn`
    let `sqlSym` = `sql`
    `connSym`.checkReady()
    `connSym`.state = csBusy

    let `cachedPtrSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedPtrSym` != nil
    var `cacheMissSym` = false
    var `stmtNameSym` = ""
    var `cachedFieldsSym`: seq[FieldDescription]
    var `effectiveRfSym`: seq[int16]
    var `colFmtsSym`: seq[int16]
    var `colOidsSym`: seq[int32]

  # Helper to build addBindDirect call with args
  proc makeBindDirect(buf, portal, stmt, rf: NimNode, argList: NimNode): NimNode =
    result = newCall(bindSym"addBindDirect", buf, portal, stmt, rf)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  proc makeParseDirect(buf, stmt, sql: NimNode, argList: NimNode): NimNode =
    result = newCall(bindSym"addParseDirect", buf, stmt, sql)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  let argList = newNimNode(nnkBracket)
  for arg in args:
    argList.add(arg)

  # Cache hit path
  let hitBlock = newStmtList()
  hitBlock.add quote do:
    `stmtNameSym` = `cachedPtrSym`.name
    `cachedFieldsSym` = `cachedPtrSym`.fields
    `colFmtsSym` = `cachedPtrSym`.colFmts
    `colOidsSym` = `cachedPtrSym`.colOids
    `effectiveRfSym` = `cachedPtrSym`.resultFormats
    `connSym`.sendBuf.setLen(0)
  let sendBufNode = newDotExpr(connSym, ident"sendBuf")
  hitBlock.add(
    makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, effectiveRfSym, argList)
  )
  hitBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Cache miss path
  let missBlock = newStmtList()
  missBlock.add quote do:
    `cacheMissSym` = true
    `stmtNameSym` = `connSym`.nextStmtName()
    `effectiveRfSym` = @[]
    `connSym`.sendBuf.setLen(0)
    if `connSym`.stmtCache.len >= `connSym`.stmtCacheCapacity:
      let evicted = `connSym`.evictStmtCache()
      `connSym`.sendBuf.addClose(dkStatement, evicted.name)
  missBlock.add(makeParseDirect(sendBufNode, stmtNameSym, sqlSym, argList))
  missBlock.add quote do:
    `connSym`.sendBuf.addDescribe(dkStatement, `stmtNameSym`)
  missBlock.add(
    makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, effectiveRfSym, argList)
  )
  missBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # No-cache path
  let elseBlock = newStmtList()
  elseBlock.add quote do:
    `effectiveRfSym` = @[]
    `connSym`.sendBuf.setLen(0)
  elseBlock.add(makeParseDirect(sendBufNode, newStrLitNode(""), sqlSym, argList))
  elseBlock.add(
    makeBindDirect(
      sendBufNode, newStrLitNode(""), newStrLitNode(""), effectiveRfSym, argList
    )
  )
  elseBlock.add quote do:
    `connSym`.sendBuf.addDescribe(dkPortal, "")
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Build if/elif/else
  let ifNode = newNimNode(nnkIfStmt)
  ifNode.add(
    newNimNode(nnkElifBranch).add(
      quote do:
        `cacheHitSym`,
      hitBlock,
    )
  )

  let missCondition = quote:
    `connSym`.stmtCacheCapacity > 0
  ifNode.add(newNimNode(nnkElifBranch).add(missCondition, missBlock))
  ifNode.add(newNimNode(nnkElse).add(elseBlock))
  result.add(ifNode)

  result.add quote do:
    queryDirectImpl(
      `connSym`, `sqlSym`, `effectiveRfSym`, `colFmtsSym`, `colOidsSym`, `cacheHitSym`,
      `cacheMissSym`, `stmtNameSym`, `cachedFieldsSym`, ZeroDuration,
    )

proc execDirectImpl(
    conn: PgConnection,
    sql: string,
    cacheHit: bool,
    cacheMiss: bool,
    stmtName: string,
    timeout: Duration,
): Future[CommandResult] {.async.} =
  ## Shared receive path for execDirect macro.
  await conn.sendBufMsg()
  var commandTag = ""
  var queryError: ref PgQueryError
  var cachedFields: seq[FieldDescription]

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          discard
        of bmkRowDescription:
          if cacheMiss:
            cachedFields = msg.fields
        of bmkNoData:
          discard
        of bmkDataRow:
          discard
        of bmkCommandComplete:
          commandTag = msg.commandTag
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

  return initCommandResult(commandTag)

macro execDirect*(conn: PgConnection, sql: string, args: varargs[untyped]): untyped =
  ## Zero-allocation exec: encodes parameters directly into the send buffer
  ## at compile time, avoiding ``seq[PgParam]`` and intermediate ``seq[byte]`` allocs.
  ##
  ## Usage: discard await conn.execDirect("UPDATE ... WHERE id = $1", myId)
  result = newStmtList()

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
  let cachedPtrSym = genSym(nskLet, "cachedPtr")
  let cacheHitSym = genSym(nskVar, "cacheHit")
  let cacheMissSym = genSym(nskVar, "cacheMiss")
  let stmtNameSym = genSym(nskVar, "stmtName")

  result.add quote do:
    let `connSym` = `conn`
    let `sqlSym` = `sql`
    `connSym`.checkReady()
    `connSym`.state = csBusy

    let `cachedPtrSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedPtrSym` != nil
    var `cacheMissSym` = false
    var `stmtNameSym` = ""

  proc makeBindDirect(buf, portal, stmt: NimNode, argList: NimNode): NimNode =
    let emptyRf = newNimNode(nnkBracket) # no result formats for exec
    result = newCall(bindSym"addBindDirect", buf, portal, stmt, emptyRf)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  proc makeParseDirect(buf, stmt, sql: NimNode, argList: NimNode): NimNode =
    result = newCall(bindSym"addParseDirect", buf, stmt, sql)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  let argList = newNimNode(nnkBracket)
  for arg in args:
    argList.add(arg)

  let sendBufNode = newDotExpr(connSym, ident"sendBuf")

  # Cache hit path
  let hitBlock = newStmtList()
  hitBlock.add quote do:
    `stmtNameSym` = `cachedPtrSym`.name
    `connSym`.sendBuf.setLen(0)
  hitBlock.add(makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, argList))
  hitBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Cache miss path
  let missBlock = newStmtList()
  missBlock.add quote do:
    `cacheMissSym` = true
    `stmtNameSym` = `connSym`.nextStmtName()
    `connSym`.sendBuf.setLen(0)
    if `connSym`.stmtCache.len >= `connSym`.stmtCacheCapacity:
      let evicted = `connSym`.evictStmtCache()
      `connSym`.sendBuf.addClose(dkStatement, evicted.name)
  missBlock.add(makeParseDirect(sendBufNode, stmtNameSym, sqlSym, argList))
  missBlock.add quote do:
    `connSym`.sendBuf.addDescribe(dkStatement, `stmtNameSym`)
  missBlock.add(makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, argList))
  missBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # No-cache path
  let elseBlock = newStmtList()
  elseBlock.add quote do:
    `connSym`.sendBuf.setLen(0)
  elseBlock.add(makeParseDirect(sendBufNode, newStrLitNode(""), sqlSym, argList))
  elseBlock.add(
    makeBindDirect(sendBufNode, newStrLitNode(""), newStrLitNode(""), argList)
  )
  elseBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Build if/elif/else
  let ifNode = newNimNode(nnkIfStmt)
  ifNode.add(
    newNimNode(nnkElifBranch).add(
      quote do:
        `cacheHitSym`,
      hitBlock,
    )
  )

  let missCondition = quote:
    `connSym`.stmtCacheCapacity > 0
  ifNode.add(newNimNode(nnkElifBranch).add(missCondition, missBlock))
  ifNode.add(newNimNode(nnkElse).add(elseBlock))
  result.add(ifNode)

  result.add quote do:
    execDirectImpl(
      `connSym`, `sqlSym`, `cacheHitSym`, `cacheMissSym`, `stmtNameSym`, ZeroDuration
    )
