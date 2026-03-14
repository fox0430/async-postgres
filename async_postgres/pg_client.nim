import std/[options, tables]

import async_backend, pg_protocol, pg_connection, pg_types

const binaryFormat*: seq[int16] = @[1'i16]
const copyBatchSize = 65536 ## 64KB batch threshold for COPY IN buffering

type
  PreparedStatement* = object
    conn*: PgConnection
    name*: string
    fields*: seq[FieldDescription]
    paramOids*: seq[int32]

  Cursor* = ref object
    conn*: PgConnection
    portalName: string
    chunkSize: int32
    timeout: Duration
    fields*: seq[FieldDescription]
    exhausted*: bool
    bufferedData: RowData
    bufferedCount: int32

  IsolationLevel* = enum
    ilDefault
    ilReadCommitted
    ilRepeatableRead
    ilSerializable
    ilReadUncommitted

  AccessMode* = enum
    amDefault
    amReadWrite
    amReadOnly

  DeferrableMode* = enum
    dmDefault
    dmDeferrable
    dmNotDeferrable

  TransactionOptions* = object
    isolation*: IsolationLevel
    access*: AccessMode
    deferrable*: DeferrableMode

proc buildBeginSql*(opts: TransactionOptions): string =
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
    # Cache hit: Bind(cached.name) + Execute + Sync
    let c = cachedOpt.get
    stmtName = c.name
    var batch = newSeqOfCap[byte](64)
    batch.add(encodeBind("", stmtName, formats, params))
    batch.add(encodeExecute("", 0))
    batch.add(encodeSync())
    await conn.sendMsg(batch)
  elif conn.stmtCacheCapacity > 0 and conn.stmtCache.len < conn.stmtCacheCapacity:
    # Cache miss with room: Parse(name) + Describe(Stmt) + Bind(name) + Execute + Sync
    cacheMiss = true
    stmtName = conn.nextStmtName()
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.add(encodeParse(stmtName, sql, paramOids))
    batch.add(encodeDescribe(dkStatement, stmtName))
    batch.add(encodeBind("", stmtName, formats, params))
    batch.add(encodeExecute("", 0))
    batch.add(encodeSync())
    await conn.sendMsg(batch)
  else:
    # Cache disabled/full: unnamed statement (original behavior)
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.add(encodeParse("", sql, paramOids))
    batch.add(encodeBind("", "", formats, params))
    batch.add(encodeExecute("", 0))
    batch.add(encodeSync())
    await conn.sendMsg(batch)

  var commandTag = ""
  var errorMsg = ""
  var errorCode = ""
  var cachedFields: seq[FieldDescription]

  while true:
    let msg = await conn.recvMessage(timeout)
    case msg.kind
    of bmkParseComplete, bmkBindComplete:
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
      break
    else:
      discard

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
  let (oids, formats, values) = extractParams(params)
  return await conn.exec(sql, values, oids, formats, timeout)

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
  var cachedFields: seq[FieldDescription] # raw fields (formatCode=0) for cache storage

  if cacheHit:
    # Cache hit: Bind(cached.name) + Execute + Sync
    let c = cachedOpt.get
    stmtName = c.name
    cachedFields = c.fields
    var batch = newSeqOfCap[byte](64)
    batch.add(encodeBind("", stmtName, formats, params, resultFormats))
    batch.add(encodeExecute("", 0))
    batch.add(encodeSync())
    await conn.sendMsg(batch)
  elif conn.stmtCacheCapacity > 0 and conn.stmtCache.len < conn.stmtCacheCapacity:
    # Cache miss with room: Parse(name) + Describe(Stmt) + Bind(name) + Execute + Sync
    cacheMiss = true
    stmtName = conn.nextStmtName()
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.add(encodeParse(stmtName, sql, paramOids))
    batch.add(encodeDescribe(dkStatement, stmtName))
    batch.add(encodeBind("", stmtName, formats, params, resultFormats))
    batch.add(encodeExecute("", 0))
    batch.add(encodeSync())
    await conn.sendMsg(batch)
  else:
    # Cache disabled/full: unnamed statement (original behavior)
    var batch = newSeqOfCap[byte](sql.len + 128)
    batch.add(encodeParse("", sql, paramOids))
    batch.add(encodeBind("", "", formats, params, resultFormats))
    batch.add(encodeDescribe(dkPortal, ""))
    batch.add(encodeExecute("", 0))
    batch.add(encodeSync())
    await conn.sendMsg(batch)

  var qr = QueryResult()
  var errorMsg = ""
  var errorCode = ""

  # For cache hit, set up fields from cache (with formatCode adjusted for resultFormats)
  if cacheHit:
    qr.fields = cachedFields
    if resultFormats.len > 0:
      for i in 0 ..< qr.fields.len:
        if resultFormats.len == 1:
          qr.fields[i].formatCode = resultFormats[0]
        elif i < resultFormats.len:
          qr.fields[i].formatCode = resultFormats[i]
    if qr.fields.len > 0:
      qr.data = newRowData(int16(qr.fields.len))

  while true:
    let msg =
      await conn.recvMessage(timeout, rowData = qr.data, rowCount = addr qr.rowCount)
    case msg.kind
    of bmkParseComplete, bmkBindComplete:
      discard
    of bmkParameterDescription:
      discard
    of bmkRowDescription:
      if cacheMiss:
        # From Describe(Statement): save raw fields (formatCode=0) for cache
        cachedFields = msg.fields
        # Apply resultFormats for this execution
        qr.fields = cachedFields
        if resultFormats.len > 0:
          for i in 0 ..< qr.fields.len:
            if resultFormats.len == 1:
              qr.fields[i].formatCode = resultFormats[0]
            elif i < resultFormats.len:
              qr.fields[i].formatCode = resultFormats[i]
      else:
        # Uncached path: RowDescription from Describe(Portal) already has correct formatCode
        qr.fields = msg.fields
      qr.data = newRowData(int16(qr.fields.len))
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
      break
    else:
      discard

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
  let (oids, formats, values) = extractParams(params)
  return await conn.query(sql, values, oids, formats, resultFormats, timeout)

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
  batch.add(encodeParse(name, sql))
  batch.add(encodeDescribe(dkStatement, name))
  batch.add(encodeSync())
  await conn.sendMsg(batch)

  var stmt = PreparedStatement(conn: conn, name: name)
  var errorMsg = ""

  while true:
    let msg = await conn.recvMessage(timeout)
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
      break
    else:
      discard

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

  var batch = newSeqOfCap[byte](64)
  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)
  batch.add(encodeBind("", stmt.name, formats, params, resultFormats))
  batch.add(encodeExecute("", 0))
  batch.add(encodeSync())
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

  while true:
    let msg =
      await conn.recvMessage(timeout, rowData = qr.data, rowCount = addr qr.rowCount)
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
      break
    else:
      discard

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

proc execute*(
    stmt: PreparedStatement,
    params: seq[PgParam],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a prepared statement with typed parameters.
  let (_, formats, values) = extractParams(params)
  return await stmt.execute(values, formats, resultFormats, timeout)

proc closeImpl(
    stmt: PreparedStatement, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  let conn = stmt.conn

  conn.checkReady()
  conn.state = csBusy

  var batch = newSeqOfCap[byte](stmt.name.len + 16)
  batch.add(encodeClose(dkStatement, stmt.name))
  batch.add(encodeSync())
  await conn.sendMsg(batch)

  var errorMsg = ""

  while true:
    let msg = await conn.recvMessage(timeout)
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
      break
    else:
      discard

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
  let quoted = quoteIdentifier(channel)
  if payload.len == 0:
    discard await conn.exec("NOTIFY " & quoted, timeout = timeout)
  else:
    discard await conn.exec(
      "SELECT pg_notify($1, $2)",
      @[channel.toPgParam, payload.toPgParam],
      timeout = timeout,
    )

proc copyInImpl(
    conn: PgConnection,
    sql: string,
    data: seq[seq[byte]],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var commandTag = ""
  var errorMsg = ""

  # Wait for CopyInResponse (or error)
  while true:
    let msg = await conn.recvMessage(timeout)
    case msg.kind
    of bmkCopyInResponse:
      break
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

  # Send CopyData in batches to minimize syscalls and await overhead
  const batchThreshold = copyBatchSize
  var buf = newSeqOfCap[byte](batchThreshold)
  for chunk in data:
    encodeCopyData(buf, chunk)
    if buf.len >= batchThreshold:
      await conn.sendMsg(buf)
      buf.setLen(0)
  # Flush remaining data + CopyDone in one send
  buf.add(encodeCopyDone())
  await conn.sendMsg(buf)

  # Wait for CommandComplete + ReadyForQuery
  while true:
    let msg = await conn.recvMessage(timeout)
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
      break
    else:
      discard

  return commandTag

proc copyIn*(
    conn: PgConnection,
    sql: string,
    data: seq[seq[byte]],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute COPY ... FROM STDIN via simple query protocol.
  ## Sends each element of `data` as a CopyData message, then CopyDone.
  ## Returns the command tag (e.g. "COPY 5").
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      return await copyInImpl(conn, sql, data, timeout).wait(timeout)
    except AsyncTimeoutError:
      conn.state = csClosed
      raise newException(PgError, "COPY IN timed out")
  else:
    return await copyInImpl(conn, sql, data)

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
  while true:
    let msg = await conn.recvMessage(timeout)
    case msg.kind
    of bmkCopyInResponse:
      info.format = msg.copyFormat
      info.columnFormats = msg.copyColumnFormats
      break
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

  # Pull data from callback and send as CopyData in batches
  const batchThreshold = copyBatchSize
  var callbackError: ref CatchableError = nil
  var buf = newSeqOfCap[byte](batchThreshold)
  try:
    while true:
      let chunk = await callback()
      if chunk.len == 0:
        break
      encodeCopyData(buf, chunk)
      if buf.len >= batchThreshold:
        await conn.sendMsg(buf)
        buf.setLen(0)
  except CatchableError as e:
    callbackError = e

  if callbackError != nil:
    # Callback raised: flush pending data is pointless, send CopyFail
    await conn.sendMsg(encodeCopyFail(callbackError.msg))
    while true:
      let msg = await conn.recvMessage(timeout)
      case msg.kind
      of bmkReadyForQuery:
        conn.txStatus = msg.txStatus
        conn.state = csReady
        break
      else:
        discard
    raise callbackError
  else:
    # Normal completion: flush remaining data + CopyDone in one send
    buf.add(encodeCopyDone())
    await conn.sendMsg(buf)

  # Wait for CommandComplete + ReadyForQuery
  while true:
    let msg = await conn.recvMessage(timeout)
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
      break
    else:
      discard

  return info

proc copyInStream*(
    conn: PgConnection,
    sql: string,
    callback: CopyInCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyInInfo] {.async.} =
  ## Execute COPY ... FROM STDIN via simple query protocol, streaming data
  ## from `callback`. The callback is called repeatedly; returning an empty
  ## seq[byte] signals EOF. If the callback raises, CopyFail is sent and
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

  while true:
    let msg = await conn.recvMessage(timeout)
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
      break
    else:
      discard

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

  while true:
    let msg = await conn.recvMessage(timeout)
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
      break
    else:
      discard

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
  batch.add(encodeParse("", beginSql))
  batch.add(encodeBind("", "", @[], @[]))
  batch.add(encodeExecute("", 0))
  # User SQL
  batch.add(encodeParse("", sql, paramOids))
  batch.add(encodeBind("", "", formats, params))
  batch.add(encodeExecute("", 0))
  # COMMIT
  batch.add(encodeParse("", "COMMIT"))
  batch.add(encodeBind("", "", @[], @[]))
  batch.add(encodeExecute("", 0))
  # Single Sync
  batch.add(encodeSync())
  await conn.sendMsg(batch)

  # Parse response: 3 phases (BEGIN=0, user=1, COMMIT=2)
  var phase = 0
  var userCommandTag = ""
  var errorMsg = ""

  while true:
    let msg = await conn.recvMessage(timeout)
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
      break
    else:
      discard

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
  batch.add(encodeParse("", beginSql))
  batch.add(encodeBind("", "", @[], @[]))
  batch.add(encodeExecute("", 0))
  # User SQL
  batch.add(encodeParse("", sql, paramOids))
  batch.add(encodeBind("", "", formats, params, resultFormats))
  batch.add(encodeDescribe(dkPortal, ""))
  batch.add(encodeExecute("", 0))
  # COMMIT
  batch.add(encodeParse("", "COMMIT"))
  batch.add(encodeBind("", "", @[], @[]))
  batch.add(encodeExecute("", 0))
  # Single Sync
  batch.add(encodeSync())
  await conn.sendMsg(batch)

  var qr = QueryResult()
  var phase = 0
  var errorMsg = ""

  while true:
    let msg =
      await conn.recvMessage(timeout, rowData = qr.data, rowCount = addr qr.rowCount)
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
      break
    else:
      discard

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
  batch.add(encodeParse("", sql, paramOids))
  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)
  batch.add(encodeBind(portalName, "", formats, params, resultFormats))
  batch.add(encodeDescribe(dkPortal, portalName))
  batch.add(encodeExecute(portalName, chunkSize))
  batch.add(encodeFlush())
  await conn.sendMsg(batch)

  var cursor =
    Cursor(conn: conn, portalName: portalName, chunkSize: chunkSize, exhausted: false)
  var errorMsg = ""

  while true:
    let msg = await conn.recvMessage(
      timeout, rowData = cursor.bufferedData, rowCount = addr cursor.bufferedCount
    )
    case msg.kind
    of bmkParseComplete, bmkBindComplete:
      discard
    of bmkRowDescription:
      cursor.fields = msg.fields
      cursor.bufferedData = newRowData(int16(msg.fields.len))
    of bmkNoData:
      discard
    of bmkPortalSuspended:
      break
    of bmkCommandComplete:
      cursor.exhausted = true
      # Need to Sync to get ReadyForQuery
      await conn.sendMsg(encodeSync())
      while true:
        let rmsg = await conn.recvMessage(timeout)
        case rmsg.kind
        of bmkReadyForQuery:
          conn.txStatus = rmsg.txStatus
          conn.state = csReady
          break
        else:
          discard
      break
    of bmkErrorResponse:
      errorMsg = formatError(msg.errorFields)
      # Drain until ReadyForQuery
      await conn.sendMsg(encodeSync())
      while true:
        let rmsg = await conn.recvMessage(timeout)
        if rmsg.kind == bmkReadyForQuery:
          conn.txStatus = rmsg.txStatus
          conn.state = csReady
          break
      raise newException(PgError, errorMsg)
    else:
      discard

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
  batch.add(encodeExecute(cursor.portalName, cursor.chunkSize))
  batch.add(encodeFlush())
  await conn.sendMsg(batch)

  while true:
    let msg = await conn.recvMessage(timeout, rowData = rd, rowCount = addr rowCount)
    case msg.kind
    of bmkPortalSuspended:
      break
    of bmkCommandComplete:
      cursor.exhausted = true
      # Close portal and sync
      var closeBatch: seq[byte]
      closeBatch.add(encodeClose(dkPortal, cursor.portalName))
      closeBatch.add(encodeSync())
      await conn.sendMsg(closeBatch)
      while true:
        let rmsg = await conn.recvMessage(timeout)
        case rmsg.kind
        of bmkCloseComplete:
          discard
        of bmkReadyForQuery:
          conn.txStatus = rmsg.txStatus
          conn.state = csReady
          break
        else:
          discard
      break
    of bmkErrorResponse:
      let errorMsg = formatError(msg.errorFields)
      await conn.sendMsg(encodeSync())
      while true:
        let rmsg = await conn.recvMessage(timeout)
        if rmsg.kind == bmkReadyForQuery:
          conn.txStatus = rmsg.txStatus
          conn.state = csReady
          break
      raise newException(PgError, errorMsg)
    else:
      discard

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
  batch.add(encodeClose(dkPortal, cursor.portalName))
  batch.add(encodeSync())
  await conn.sendMsg(batch)

  while true:
    let msg = await conn.recvMessage(timeout)
    case msg.kind
    of bmkCloseComplete:
      discard
    of bmkReadyForQuery:
      conn.txStatus = msg.txStatus
      conn.state = csReady
      break
    else:
      discard

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
