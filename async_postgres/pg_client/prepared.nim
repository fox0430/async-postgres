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

  var batch = newSeqOfCap[byte](sql.len + name.len + 32)
  batch.addParse(name, sql)
  batch.addDescribe(dkStatement, name)
  batch.addSync()
  conn.state = csBusy
  await conn.sendMsg(batch)

  var stmt = PreparedStatement(conn: conn, name: name, sql: sql)

  conn.pumpUntilReady:
    case pumpMsg.kind
    of bmkParseComplete:
      discard
    of bmkParameterDescription:
      stmt.paramOids = pumpMsg.paramTypeOids
    of bmkRowDescription:
      stmt.fields = pumpMsg.fields
    of bmkNoData:
      discard
    else:
      discard
  do:
    discard

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
    awaitOrInvalidate(
      conn, stmt, prepareImpl(conn, name, sql), timeout, "Prepare timed out"
    )
  return stmt

proc executeImpl*(
    stmt: PreparedStatement, params: seq[PgParam] = @[], resultFormats: seq[int16] = @[]
): Future[QueryResult] {.async.} =
  let conn = stmt.conn

  conn.checkReady()

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
  conn.state = csBusy
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
  conn.pumpUntilReady(qr.data, addr qr.rowCount):
    case pumpMsg.kind
    of bmkBindComplete:
      discard
    of bmkCommandComplete:
      qr.commandTag = pumpMsg.commandTag
    of bmkEmptyQueryResponse:
      discard
    else:
      discard
  do:
    discard

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
    awaitOrInvalidate(
      stmt.conn,
      qr,
      executeImpl(stmt, params, resultFormats),
      timeout,
      "Execute timed out",
    )
  return qr

proc closeImpl*(stmt: PreparedStatement): Future[void] {.async.} =
  let conn = stmt.conn

  conn.checkReady()

  var batch = newSeqOfCap[byte](stmt.name.len + 16)
  batch.addClose(dkStatement, stmt.name)
  batch.addSync()
  conn.state = csBusy
  await conn.sendMsg(batch)

  conn.pumpUntilReady:
    case pumpMsg.kind
    of bmkCloseComplete: discard
    else: discard
  do:
    discard

proc close*(
    stmt: PreparedStatement, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Close a prepared statement.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  awaitVoidOrInvalidate(
    stmt.conn, closeImpl(stmt), timeout, "Statement close timed out"
  )
