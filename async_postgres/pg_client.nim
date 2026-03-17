import std/[options, tables, macros]

import async_backend, pg_protocol, pg_connection, pg_types

const binaryFormat*: seq[int16] =
  @[1'i16] ## Result format list requesting binary encoding for all columns.
const copyBatchSize = 262144 ## 256KB batch threshold for COPY IN buffering

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
      commandTag*: string
    of prkQuery:
      queryResult*: QueryResult

  Pipeline* = object
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

  let cachedOpt = conn.lookupStmtCache(sql)
  var cacheHit = cachedOpt.isSome
  var cacheMiss = false
  var stmtName = ""

  if cacheHit:
    let c = cachedOpt.get
    stmtName = c.name
    var batch = newSeqOfCap[byte](params.len * 16 + 128)
    batch.addBind("", stmtName, formats, params)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    var batch = newSeqOfCap[byte](sql.len + 128)
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      batch.addClose(dkStatement, evicted.name)
    batch.addParse(stmtName, sql, paramOids)
    batch.addDescribe(dkStatement, stmtName)
    batch.addBind("", stmtName, formats, params)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  else:
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.addParse("", sql, paramOids)
    batch.addBind("", "", formats, params)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)

  var commandTag = ""
  var errorMsg = ""
  var errorCode = ""
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
          errorMsg = formatError(msg.errorFields)
          for f in msg.errorFields:
            if f.code == 'C':
              errorCode = f.value
              break
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            if cacheHit and errorCode == "26000":
              conn.removeStmtCache(sql)
            raise newException(PgError, errorMsg)
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

  let cachedOpt = conn.lookupStmtCache(sql)
  var cacheHit = cachedOpt.isSome
  var cacheMiss = false
  var stmtName = ""

  if cacheHit:
    let c = cachedOpt.get
    stmtName = c.name
    var batch = newSeqOfCap[byte](params.len * 16 + 128)
    batch.addBind("", stmtName, params)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    var batch = newSeqOfCap[byte](sql.len + 128)
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      batch.addClose(dkStatement, evicted.name)
    batch.addParse(stmtName, sql, params)
    batch.addDescribe(dkStatement, stmtName)
    batch.addBind("", stmtName, params)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  else:
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.addParse("", sql, params)
    batch.addBind("", "", params)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)

  var commandTag = ""
  var errorMsg = ""
  var errorCode = ""
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
          errorMsg = formatError(msg.errorFields)
          for f in msg.errorFields:
            if f.code == 'C':
              errorCode = f.value
              break
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            if cacheHit and errorCode == "26000":
              conn.removeStmtCache(sql)
            raise newException(PgError, errorMsg)
          if cacheMiss:
            conn.addStmtCache(sql, CachedStmt(name: stmtName, fields: cachedFields))
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return commandTag

proc exec*(
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
      raise newException(PgError, "Exec timed out")
  else:
    return await execImpl(conn, sql, params, paramOids, paramFormats)

proc exec*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a statement with typed parameters.
  if timeout > ZeroDuration:
    try:
      return await execImpl(conn, sql, params, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgError, "Exec timed out")
  else:
    return await execImpl(conn, sql, params)

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
  var errorMsg = ""
  var errorCode = ""

  if cacheHit:
    qr.fields = cachedFields
    if resultFormats.len > 0 and cachedColFmts.len > 0:
      for i in 0 ..< qr.fields.len:
        qr.fields[i].formatCode = cachedColFmts[i]
    if qr.fields.len > 0:
      qr.data = newRowData(int16(qr.fields.len), cachedColFmts, cachedColOids)

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
        of bmkNoData:
          discard
        of bmkCommandComplete:
          qr.commandTag = msg.commandTag
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          errorMsg = formatError(msg.errorFields)
          for f in msg.errorFields:
            if f.code == 'C':
              errorCode = f.value
              break
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            if cacheHit and errorCode == "26000":
              conn.removeStmtCache(sql)
            raise newException(PgError, errorMsg)
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

  let cachedOpt = conn.lookupStmtCache(sql)
  var cacheHit = cachedOpt.isSome
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  if cacheHit:
    let c = cachedOpt.get
    stmtName = c.name
    cachedFields = c.fields
    cachedColFmts = c.colFmts
    cachedColOids = c.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: c.resultFormats else: resultFormats
    var batch = newSeqOfCap[byte](params.len * 16 + 128)
    batch.addBind("", stmtName, formats, params, effectiveResultFormats)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    var batch = newSeqOfCap[byte](sql.len + 128)
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      batch.addClose(dkStatement, evicted.name)
    batch.addParse(stmtName, sql, paramOids)
    batch.addDescribe(dkStatement, stmtName)
    batch.addBind("", stmtName, formats, params, effectiveResultFormats)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  else:
    effectiveResultFormats = resultFormats
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.addParse("", sql, paramOids)
    batch.addBind("", "", formats, params, effectiveResultFormats)
    batch.addDescribe(dkPortal, "")
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)

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

  let cachedOpt = conn.lookupStmtCache(sql)
  var cacheHit = cachedOpt.isSome
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  if cacheHit:
    let c = cachedOpt.get
    stmtName = c.name
    cachedFields = c.fields
    cachedColFmts = c.colFmts
    cachedColOids = c.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: c.resultFormats else: resultFormats
    var batch = newSeqOfCap[byte](params.len * 16 + 128)
    batch.addBind("", stmtName, params, effectiveResultFormats)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    var batch = newSeqOfCap[byte](sql.len + 128)
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      batch.addClose(dkStatement, evicted.name)
    batch.addParse(stmtName, sql, params)
    batch.addDescribe(dkStatement, stmtName)
    batch.addBind("", stmtName, params, effectiveResultFormats)
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)
  else:
    effectiveResultFormats = resultFormats
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.addParse("", sql, params)
    batch.addBind("", "", params, effectiveResultFormats)
    batch.addDescribe(dkPortal, "")
    batch.addExecute("", 0)
    batch.addSync()
    await conn.sendMsg(batch)

  var qr = QueryResult()
  queryRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, qr, timeout,
  )
  return qr

proc query*(
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
      raise newException(PgError, "Query timed out")
  else:
    return await queryImpl(conn, sql, params, paramOids, paramFormats, resultFormats)

proc query*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with typed parameters.
  if timeout > ZeroDuration:
    try:
      return await queryImpl(conn, sql, params, resultFormats, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgError, "Query timed out")
  else:
    return await queryImpl(conn, sql, params, resultFormats)

proc queryOne*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query and return the first row, or `none` if no rows.
  let qr =
    await conn.query(sql, params, resultFormats = resultFormats, timeout = timeout)
  if qr.rowCount > 0:
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

proc queryExists*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a query and return whether any rows exist.
  let qr = await conn.query(sql, params, timeout = timeout)
  return qr.rowCount > 0

proc execAffected*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a statement and return the number of affected rows.
  let tag = await conn.exec(sql, params, timeout)
  return affectedRows(tag)

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
  var errorMsg = ""

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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
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
      raise newException(PgError, "Prepare timed out")
  else:
    return await prepareImpl(conn, name, sql)

proc executeImpl(
    stmt: PreparedStatement,
    params: seq[Option[seq[byte]]],
    paramFormats: seq[int16],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  let conn = stmt.conn

  conn.checkReady()
  conn.state = csBusy

  var batch = newSeqOfCap[byte](params.len * 16 + 128)
  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)
  batch.addBind("", stmt.name, formats, params, resultFormats)
  batch.addExecute("", 0)
  batch.addSync()
  await conn.sendMsg(batch)

  var qr = QueryResult(fields: stmt.fields)
  if resultFormats.len > 0:
    for i in 0 ..< qr.fields.len:
      if resultFormats.len == 1:
        qr.fields[i].formatCode = resultFormats[0]
      elif i < resultFormats.len:
        qr.fields[i].formatCode = resultFormats[i]
  if qr.fields.len > 0:
    qr.data = newRowData(int16(qr.fields.len))
  var errorMsg = ""

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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return qr

proc execute*(
    stmt: PreparedStatement,
    params: seq[Option[seq[byte]]] = @[],
    paramFormats: seq[int16] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a prepared statement with parameters.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await executeImpl(stmt, params, paramFormats, resultFormats, timeout).wait(
        timeout
      )
    except AsyncTimeoutError:
      stmt.conn.state = csClosed
      raise newException(PgError, "Execute timed out")
  else:
    return await executeImpl(stmt, params, paramFormats, resultFormats)

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

  var batch = newSeqOfCap[byte](params.len * 16 + 128)
  batch.addBind("", stmt.name, if needsCoercion: coerced else: params, resultFormats)
  batch.addExecute("", 0)
  batch.addSync()
  await conn.sendMsg(batch)

  var qr = QueryResult(fields: stmt.fields)
  if resultFormats.len > 0:
    for i in 0 ..< qr.fields.len:
      if resultFormats.len == 1:
        qr.fields[i].formatCode = resultFormats[0]
      elif i < resultFormats.len:
        qr.fields[i].formatCode = resultFormats[i]
  if qr.fields.len > 0:
    qr.data = newRowData(int16(qr.fields.len))
  var errorMsg = ""

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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return qr

proc execute*(
    stmt: PreparedStatement,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a prepared statement with typed parameters.
  if timeout > ZeroDuration:
    try:
      return await executeImpl(stmt, params, resultFormats, timeout).wait(timeout)
    except AsyncTimeoutError:
      stmt.conn.state = csClosed
      raise newException(PgError, "Execute timed out")
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

  var errorMsg = ""

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCloseComplete:
          discard
        of bmkErrorResponse:
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
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
      raise newException(PgError, "Statement close timed out")
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
  var errorMsg = ""

  # Wait for CopyInResponse (or error)
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyInResponse:
          break recvLoop
        of bmkErrorResponse:
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
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
      await conn.sendMsg(conn.sendBuf)
      conn.sendBuf.setLen(0)
  # Flush remaining data + CopyDone in one send
  conn.sendBuf.addCopyDone()
  await conn.sendMsg(conn.sendBuf)
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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
          break recvLoop2
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return commandTag

proc copyIn*(
    conn: PgConnection, sql: string, data: seq[byte], timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  ## Execute COPY ... FROM STDIN with a single contiguous ``seq[byte]``.
  ## Avoids the copy that the ``openArray[byte]`` overload performs.
  if timeout > ZeroDuration:
    try:
      return await copyInRawImpl(conn, sql, data, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgError, "COPY IN timed out")
  else:
    return await copyInRawImpl(conn, sql, data)

proc copyIn*(
    conn: PgConnection,
    sql: string,
    data: openArray[byte],
    timeout: Duration = ZeroDuration,
): Future[string] =
  ## Execute COPY ... FROM STDIN with a single contiguous buffer.
  ## Slices `data` into CopyData messages internally.
  ## Returns the command tag (e.g. "COPY 5").
  let dataCopy = @data # copy openArray to seq before async boundary
  if timeout > ZeroDuration:
    proc inner(): Future[string] {.async.} =
      try:
        return await copyInRawImpl(conn, sql, dataCopy, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.state = csClosed
        raise newException(PgError, "COPY IN timed out")

    return inner()
  else:
    return copyInRawImpl(conn, sql, dataCopy)

proc copyIn*(
    conn: PgConnection, sql: string, data: string, timeout: Duration = ZeroDuration
): Future[string] =
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
): Future[string] =
  ## Execute COPY ... FROM STDIN via simple query protocol.
  ## Concatenates chunks and delegates to the ``seq[byte]`` overload.
  ## Returns the command tag (e.g. "COPY 5").
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
  var errorMsg = ""

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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
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
        await conn.sendMsg(conn.sendBuf)
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
    await conn.sendMsg(conn.sendBuf)
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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
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
      raise newException(PgError, "COPY IN stream timed out")
  else:
    return await copyInStreamImpl(conn, sql, callback)

proc copyOutImpl(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CopyResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var cr = CopyResult()
  var errorMsg = ""

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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
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
      raise newException(PgError, "COPY OUT timed out")
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
  var errorMsg = ""

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
          errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            raise newException(PgError, errorMsg)
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
      raise newException(PgError, "COPY OUT stream timed out")
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
  var batch = newSeqOfCap[byte](beginSql.len + sql.len + 256)
  # BEGIN
  batch.addParse("", beginSql)
  batch.addBind("", "", @[], @[])
  batch.addExecute("", 0)
  # User SQL
  batch.addParse("", sql, paramOids)
  batch.addBind("", "", formats, params)
  batch.addExecute("", 0)
  # COMMIT
  batch.addParse("", "COMMIT")
  batch.addBind("", "", @[], @[])
  batch.addExecute("", 0)
  # Single Sync
  batch.addSync()
  await conn.sendMsg(batch)

  # Parse response: 3 phases (BEGIN=0, user=1, COMMIT=2)
  var phase = 0
  var userCommandTag = ""
  var errorMsg = ""

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
          if errorMsg.len == 0:
            errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            # Error occurred: if we're in a failed transaction, send ROLLBACK
            if msg.txStatus == tsInFailedTransaction:
              discard await conn.simpleExec("ROLLBACK")
            raise newException(PgError, errorMsg)
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return userCommandTag

proc execInTransaction*(
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
      raise newException(PgError, "execInTransaction timed out")
  else:
    return await execInTransactionImpl(
      conn, "BEGIN", sql, params, paramOids, paramFormats, timeout
    )

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a statement inside a pipelined transaction with typed parameters.
  let (oids, formats, values) = extractParams(params)
  return await conn.execInTransaction(sql, values, oids, formats, timeout)

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    opts: TransactionOptions,
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a statement inside a pipelined transaction with options.
  let (oids, formats, values) = extractParams(params)
  let beginSql = buildBeginSql(opts)
  if timeout > ZeroDuration:
    try:
      return await execInTransactionImpl(
        conn, beginSql, sql, values, oids, formats, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgError, "execInTransaction timed out")
  else:
    return
      await execInTransactionImpl(conn, beginSql, sql, values, oids, formats, timeout)

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
  var batch = newSeqOfCap[byte](beginSql.len + sql.len + 256)
  # BEGIN
  batch.addParse("", beginSql)
  batch.addBind("", "", @[], @[])
  batch.addExecute("", 0)
  # User SQL
  batch.addParse("", sql, paramOids)
  batch.addBind("", "", formats, params, resultFormats)
  batch.addDescribe(dkPortal, "")
  batch.addExecute("", 0)
  # COMMIT
  batch.addParse("", "COMMIT")
  batch.addBind("", "", @[], @[])
  batch.addExecute("", 0)
  # Single Sync
  batch.addSync()
  await conn.sendMsg(batch)

  var qr = QueryResult()
  var phase = 0
  var errorMsg = ""

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
          if errorMsg.len == 0:
            errorMsg = formatError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            if msg.txStatus == tsInFailedTransaction:
              discard await conn.simpleExec("ROLLBACK")
            raise newException(PgError, errorMsg)
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return qr

proc queryInTransaction*(
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
      raise newException(PgError, "queryInTransaction timed out")
  else:
    return await queryInTransactionImpl(
      conn, "BEGIN", sql, params, paramOids, paramFormats, resultFormats, timeout
    )

proc queryInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with typed parameters.
  let (oids, formats, values) = extractParams(params)
  return
    await conn.queryInTransaction(sql, values, oids, formats, resultFormats, timeout)

proc queryInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam],
    opts: TransactionOptions,
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with options.
  let (oids, formats, values) = extractParams(params)
  let beginSql = buildBeginSql(opts)
  if timeout > ZeroDuration:
    try:
      return await queryInTransactionImpl(
        conn, beginSql, sql, values, oids, formats, resultFormats, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgError, "queryInTransaction timed out")
  else:
    return await queryInTransactionImpl(
      conn, beginSql, sql, values, oids, formats, resultFormats, timeout
    )

proc newPipeline*(conn: PgConnection): Pipeline =
  ## Create a new pipeline for batching multiple operations into a single round trip.
  Pipeline(conn: conn, ops: @[])

proc addExec*(p: var Pipeline, sql: string, params: seq[PgParam] = @[]) =
  ## Add an exec operation to the pipeline with typed parameters.
  let (oids, formats, values) = extractParams(params)
  p.ops.add PipelineOp(
    kind: pokExec, sql: sql, params: values, paramOids: oids, paramFormats: formats
  )

proc addExec*(
    p: var Pipeline,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
) =
  ## Add an exec operation to the pipeline with raw binary parameters.
  p.ops.add PipelineOp(
    kind: pokExec,
    sql: sql,
    params: params,
    paramOids: paramOids,
    paramFormats: paramFormats,
  )

proc addQuery*(
    p: var Pipeline,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormats: seq[int16] = @[],
) =
  ## Add a query operation to the pipeline with typed parameters.
  let (oids, formats, values) = extractParams(params)
  p.ops.add PipelineOp(
    kind: pokQuery,
    sql: sql,
    params: values,
    paramOids: oids,
    paramFormats: formats,
    resultFormats: resultFormats,
  )

proc addQuery*(
    p: var Pipeline,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32] = @[],
    paramFormats: seq[int16] = @[],
    resultFormats: seq[int16] = @[],
) =
  ## Add a query operation to the pipeline with raw binary parameters.
  p.ops.add PipelineOp(
    kind: pokQuery,
    sql: sql,
    params: params,
    paramOids: paramOids,
    paramFormats: paramFormats,
    resultFormats: resultFormats,
  )

proc executeImpl(
    pIn: Pipeline, timeout: Duration
): Future[seq[PipelineResult]] {.async.} =
  var p = pIn
  let conn = p.conn
  conn.checkReady()
  conn.state = csBusy

  # Send Phase — also collect CachedStmt data needed by receive phase
  var batch = newSeqOfCap[byte](p.ops.len * 256)
  var cachedStmts = newSeq[CachedStmt](p.ops.len) # populated for cache-hit ops
  var pendingCacheAdds = 0 # track pending additions for LRU eviction in pipeline

  for i in 0 ..< p.ops.len:
    let formats =
      if p.ops[i].paramFormats.len > 0:
        p.ops[i].paramFormats
      else:
        newSeq[int16](p.ops[i].params.len)

    let cachedOpt = conn.lookupStmtCache(p.ops[i].sql)
    p.ops[i].cacheHit = cachedOpt.isSome
    p.ops[i].cacheMiss = false

    if cachedOpt.isSome:
      let c = cachedOpt.get
      p.ops[i].stmtName = c.name
      cachedStmts[i] = c
      var effectiveResultFormats: seq[int16]
      if p.ops[i].kind == pokQuery:
        effectiveResultFormats =
          if p.ops[i].resultFormats.len == 0:
            c.resultFormats
          else:
            p.ops[i].resultFormats
        p.ops[i].resultFormats = effectiveResultFormats
      batch.addBind("", c.name, formats, p.ops[i].params, effectiveResultFormats)
      batch.addExecute("", 0)
    elif conn.stmtCacheCapacity > 0:
      p.ops[i].cacheMiss = true
      p.ops[i].stmtName = conn.nextStmtName()
      if conn.stmtCache.len + pendingCacheAdds >= conn.stmtCacheCapacity and
          conn.stmtCache.len > 0:
        let evicted = conn.evictStmtCache()
        batch.addClose(dkStatement, evicted.name)
      inc pendingCacheAdds
      batch.addParse(p.ops[i].stmtName, p.ops[i].sql, p.ops[i].paramOids)
      batch.addDescribe(dkStatement, p.ops[i].stmtName)
      batch.addBind(
        "", p.ops[i].stmtName, formats, p.ops[i].params, p.ops[i].resultFormats
      )
      batch.addExecute("", 0)
    else:
      batch.addParse("", p.ops[i].sql, p.ops[i].paramOids)
      batch.addBind("", "", formats, p.ops[i].params, p.ops[i].resultFormats)
      if p.ops[i].kind == pokQuery:
        batch.addDescribe(dkPortal, "")
      batch.addExecute("", 0)

  batch.addSync()
  await conn.sendMsg(batch)

  # Receive Phase
  var results = newSeq[PipelineResult](p.ops.len)
  var activeOpIdx = 0
  var errorMsg = ""
  var errorCode = ""
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
              results[activeOpIdx].commandTag = msg.commandTag
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
          if errorMsg.len == 0:
            errorMsg = formatError(msg.errorFields)
            for f in msg.errorFields:
              if f.code == 'C':
                errorCode = f.value
                break
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if errorMsg.len > 0:
            # Invalidate cache for 26000 (prepared statement does not exist)
            if errorCode == "26000":
              for i in 0 ..< p.ops.len:
                if p.ops[i].cacheHit:
                  conn.removeStmtCache(p.ops[i].sql)
            raise newException(PgError, errorMsg)
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
      raise newException(PgError, "Pipeline execute timed out")
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
  var errorMsg = ""

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
          errorMsg = formatError(msg.errorFields)
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
          raise newException(PgError, errorMsg)
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return cursor

proc openCursor*(
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
      raise newException(PgError, "Cursor open timed out")
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

  var batch = newSeqOfCap[byte](cursor.portalName.len + 24)
  batch.addExecute(cursor.portalName, cursor.chunkSize)
  batch.addFlush()
  await conn.sendMsg(batch)

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
          let errorMsg = formatError(msg.errorFields)
          await conn.sendMsg(encodeSync())
          block errDrain:
            while true:
              while (let ropt = conn.nextMessage(); ropt.isSome):
                if ropt.get.kind == bmkReadyForQuery:
                  conn.txStatus = ropt.get.txStatus
                  conn.state = csReady
                  break errDrain
              await conn.fillRecvBuf(timeout)
          raise newException(PgError, errorMsg)
        else:
          discard
      await conn.fillRecvBuf(timeout)

  result = newSeq[Row](rowCount)
  for i in 0 ..< rowCount:
    result[i] = Row(data: rd, rowIdx: i)

proc fetchNext*(cursor: Cursor): Future[seq[Row]] {.async.} =
  ## Fetch the next chunk of rows from the cursor.
  ## Returns an empty seq when the cursor is exhausted.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if cursor.bufferedCount > 0:
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
      raise newException(PgError, "Cursor fetch timed out")
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
      raise newException(PgError, "Cursor close timed out")
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
    resultFormats: seq[int16] = @[],
    chunkSize: int32 = 100,
    timeout: Duration = ZeroDuration,
): Future[Cursor] {.async.} =
  ## Open a cursor with typed parameters.
  let (oids, formats, values) = extractParams(params)
  return
    await conn.openCursor(sql, values, oids, formats, resultFormats, chunkSize, timeout)

# Zero-alloc query/exec via compile-time macros

proc queryDirectImpl(
    conn: PgConnection,
    sql: string,
    batch: seq[byte],
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
  await conn.sendMsg(batch)
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
  let cachedOptSym = genSym(nskLet, "cachedOpt")
  let cacheHitSym = genSym(nskVar, "cacheHit")
  let cacheMissSym = genSym(nskVar, "cacheMiss")
  let stmtNameSym = genSym(nskVar, "stmtName")
  let cachedFieldsSym = genSym(nskVar, "cachedFields")
  let effectiveRfSym = genSym(nskVar, "effectiveRf")
  let batchSym = genSym(nskVar, "batch")
  let colFmtsSym = genSym(nskVar, "colFmts")
  let colOidsSym = genSym(nskVar, "colOids")

  result.add quote do:
    let `connSym` = `conn`
    let `sqlSym` = `sql`
    `connSym`.checkReady()
    `connSym`.state = csBusy

    let `cachedOptSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedOptSym`.isSome
    var `cacheMissSym` = false
    var `stmtNameSym` = ""
    var `cachedFieldsSym`: seq[FieldDescription]
    var `effectiveRfSym`: seq[int16]
    var `batchSym`: seq[byte]
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
    let c = `cachedOptSym`.get
    `stmtNameSym` = c.name
    `cachedFieldsSym` = c.fields
    `colFmtsSym` = c.colFmts
    `colOidsSym` = c.colOids
    `effectiveRfSym` = c.resultFormats
    `batchSym` = newSeqOfCap[byte](128)
  hitBlock.add(
    makeBindDirect(batchSym, newStrLitNode(""), stmtNameSym, effectiveRfSym, argList)
  )
  hitBlock.add quote do:
    `batchSym`.addExecute("", 0)
    `batchSym`.addSync()

  # Cache miss path
  let missBlock = newStmtList()
  missBlock.add quote do:
    `cacheMissSym` = true
    `stmtNameSym` = `connSym`.nextStmtName()
    `effectiveRfSym` = @[]
    `batchSym` = newSeqOfCap[byte](`sqlSym`.len + 128)
    if `connSym`.stmtCache.len >= `connSym`.stmtCacheCapacity:
      let evicted = `connSym`.evictStmtCache()
      `batchSym`.addClose(dkStatement, evicted.name)
  missBlock.add(makeParseDirect(batchSym, stmtNameSym, sqlSym, argList))
  missBlock.add quote do:
    `batchSym`.addDescribe(dkStatement, `stmtNameSym`)
  missBlock.add(
    makeBindDirect(batchSym, newStrLitNode(""), stmtNameSym, effectiveRfSym, argList)
  )
  missBlock.add quote do:
    `batchSym`.addExecute("", 0)
    `batchSym`.addSync()

  # No-cache path
  let elseBlock = newStmtList()
  elseBlock.add quote do:
    `effectiveRfSym` = @[]
    `batchSym` = newSeqOfCap[byte](`sqlSym`.len + 128)
  elseBlock.add(makeParseDirect(batchSym, newStrLitNode(""), sqlSym, argList))
  elseBlock.add(
    makeBindDirect(
      batchSym, newStrLitNode(""), newStrLitNode(""), effectiveRfSym, argList
    )
  )
  elseBlock.add quote do:
    `batchSym`.addDescribe(dkPortal, "")
    `batchSym`.addExecute("", 0)
    `batchSym`.addSync()

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
      `connSym`, `sqlSym`, `batchSym`, `effectiveRfSym`, `colFmtsSym`, `colOidsSym`,
      `cacheHitSym`, `cacheMissSym`, `stmtNameSym`, `cachedFieldsSym`, ZeroDuration,
    )
