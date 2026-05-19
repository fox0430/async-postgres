## `exec` overloads: extended-query single-statement execution that ignores
## result rows and returns just the command tag (`CommandResult`).

import std/[options, tables]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

proc execImpl*(
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
  conn.flushPendingStmtCloses()
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

proc execImpl*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  var cacheMiss = false
  var stmtName = ""

  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
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

proc exec*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with typed parameters via the extended query protocol.
  ##
  ## Single statement only; the plan is cached per-connection. Use
  ## ``simpleExec`` for parameter-less session commands (``BEGIN``, ``SET``,
  ## ``VACUUM``, ``LISTEN`` …) or ``simpleQuery`` when you need multi-statement
  ## execution in one round trip.
  ##
  ## On timeout the connection is marked closed (protocol desync) and cannot be
  ## reused; pooled connections are discarded automatically.
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    if timeout > ZeroDuration:
      try:
        tag = await execImpl(conn, sql, params, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Exec timed out")
    else:
      tag = await execImpl(conn, sql, params)
  return initCommandResult(tag)

proc execInlineImpl*(
    conn: PgConnection,
    sql: string,
    data: seq[byte],
    ranges: seq[tuple[off: int32, len: int32]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  var cacheMiss = false
  var stmtName = ""

  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  if cacheHit:
    stmtName = cached.name
    conn.sendBuf.addBindRaw("", stmtName, paramFormats, data, ranges)
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
    conn.sendBuf.addBindRaw("", stmtName, paramFormats, data, ranges)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    conn.sendBuf.addParse("", sql, paramOids)
    conn.sendBuf.addBindRaw("", "", paramFormats, data, ranges)
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

  return commandTag

proc exec*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParamInline],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with heap-alloc-free inline parameters.
  ## Prefer this overload for scalar-heavy workloads (e.g. bulk INSERT of
  ## numeric columns) where `seq[PgParam]` would heap-allocate per parameter.
  let (data, ranges, oids, formats) = flattenInline(params)
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, paramsInline: params, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    if timeout > ZeroDuration:
      try:
        tag = await execInlineImpl(conn, sql, data, ranges, oids, formats, timeout).wait(
          timeout
        )
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Exec timed out")
    else:
      tag = await execInlineImpl(conn, sql, data, ranges, oids, formats)
  return initCommandResult(tag)

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
