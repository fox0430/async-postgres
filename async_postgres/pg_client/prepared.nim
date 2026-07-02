## Named server-side prepared statements: `prepare`, `execute`, and `close`.

import std/[options]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

type PreparedStatement* = object
  ## A server-side prepared statement returned by `prepare`.
  ##
  ## Valid only within the server session that prepared it. A session reset
  ## (``DISCARD ALL`` / ``DEALLOCATE``, or a pooled backend being recycled)
  ## drops the statement; a later `execute` then raises ``PgQueryError`` with
  ## SQLSTATE ``26000`` (invalid_sql_statement_name) and the caller must
  ## `prepare` it again. There is no transparent re-prepare here — that is
  ## reserved for the auto-prepare statement cache (see the cache path's
  ## ``StmtCacheInvalidatingStates`` handling in `pg_client/core`).
  conn*: PgConnection
  name*: string
  sql*: string
  fields*: seq[FieldDescription]
  paramOids*: seq[int32]

proc columnIndex*(stmt: PreparedStatement, name: string): int =
  ## Find the index of a column by name in a prepared statement.
  stmt.fields.columnIndex(name)

proc prepareImpl*(
    conn: PgConnection, name: string, sql: string
): Future[PreparedStatement] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  var batch = newSeqOfCap[byte](sql.len + name.len + 32)
  batch.addParse(name, sql)
  batch.addDescribe(dkStatement, name)
  batch.addSync()
  await conn.sendMsg(batch)

  var stmt = PreparedStatement(conn: conn, name: name, sql: sql)
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
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf()

  return stmt

proc prepare*(
    conn: PgConnection, name: string, sql: string, timeout: Duration = ZeroDuration
): Future[PreparedStatement] {.async.} =
  ## Prepare a named statement, returning metadata.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var stmt: PreparedStatement
  withConnTracing(
    conn,
    onPrepareStart,
    onPrepareEnd,
    TracePrepareStartData(name: name, sql: sql),
    TracePrepareEndData,
    TracePrepareEndData(),
  ):
    if timeout > ZeroDuration:
      try:
        stmt = await prepareImpl(conn, name, sql).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Prepare timed out")
    else:
      stmt = await prepareImpl(conn, name, sql)
  return stmt

proc executeImpl*(
    stmt: PreparedStatement, params: seq[PgParam] = @[], resultFormats: seq[int16] = @[]
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
  conn.flushPendingStmtCloses()
  conn.sendBuf.addBind(
    "", stmt.name, if needsCoercion: coerced else: params, resultFormats
  )
  conn.sendBuf.addExecute("", 0)
  conn.sendBuf.addSync()
  await conn.sendBufMsg()

  var qr = QueryResult(fields: stmt.fields)
  if resultFormats.len > 0:
    let colFmts = deriveColFmts(resultFormats, qr.fields.len)
    for i in 0 ..< qr.fields.len:
      qr.fields[i].formatCode = colFmts[i]
  if qr.fields.len > 0:
    var colFmts = newSeq[int16](qr.fields.len)
    var colOids = newSeq[int32](qr.fields.len)
    for i in 0 ..< qr.fields.len:
      colFmts[i] = qr.fields[i].formatCode
      colOids[i] = qr.fields[i].typeOid
    qr.data = newRowData(int16(qr.fields.len), colFmts, colOids)
    qr.data.fields = qr.fields
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
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf()

  return qr

proc execute*(
    stmt: PreparedStatement,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a prepared statement with typed parameters.
  ##
  ## If the server session has lost the statement (``DISCARD ALL`` /
  ## ``DEALLOCATE``, or a pooled backend reset), this raises ``PgQueryError``
  ## with SQLSTATE ``26000``; recover by calling `prepare` again. The error is
  ## propagated, not retried — unlike the auto-prepare cache, an explicit
  ## `PreparedStatement` is never re-prepared transparently.
  var qr: QueryResult
  withConnTracing(
    stmt.conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: stmt.sql, params: params, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: qr.commandTag, rowCount: qr.rowCount),
  ):
    let resultFormats = resultFormat.toFormatCodes()
    if timeout > ZeroDuration:
      try:
        qr = await executeImpl(stmt, params, resultFormats).wait(timeout)
      except AsyncTimeoutError:
        stmt.conn.invalidateOnTimeout("Execute timed out")
    else:
      qr = await executeImpl(stmt, params, resultFormats)
  return qr

proc closeImpl*(stmt: PreparedStatement): Future[void] {.async.} =
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
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf()

proc close*(
    stmt: PreparedStatement, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Close a prepared statement.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeout > ZeroDuration:
    try:
      await closeImpl(stmt).wait(timeout)
    except AsyncTimeoutError:
      stmt.conn.invalidateOnTimeout("Statement close timed out")
  else:
    await closeImpl(stmt)
