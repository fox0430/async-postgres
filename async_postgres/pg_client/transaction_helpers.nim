## Pipelined single-statement transactions: `execInTransaction` and
## `queryInTransaction` issue BEGIN, the user SQL, and COMMIT with a single
## Sync round trip.

import std/[options]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

proc queryInTransactionImpl(
    conn: PgConnection,
    beginSql: string,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16],
): Future[QueryResult] {.async.} =
  conn.checkReady()
  conn.checkTxIdle()
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

  conn.pumpUntilReady(qr.data, addr qr.rowCount):
    case pumpMsg.kind
    of bmkParseComplete, bmkBindComplete:
      discard
    of bmkRowDescription:
      var fields = pumpMsg.fields
      var cf: seq[int16]
      var co: seq[int32]
      if resultFormats.len > 0:
        cf = deriveColFmts(resultFormats, fields.len)
        co = newSeq[int32](fields.len)
        for i in 0 ..< fields.len:
          co[i] = fields[i].typeOid
          fields[i].formatCode = cf[i]
      qr.fields = fields
      qr.data = newRowData(int16(qr.fields.len), cf, co)
      qr.data.fields = qr.fields
    of bmkNoData:
      discard
    of bmkEmptyQueryResponse:
      # Empty/comment-only user SQL yields EmptyQueryResponse instead of
      # CommandComplete; advance the phase anyway so the trailing COMMIT's
      # CommandComplete isn't captured as the user statement's tag.
      inc phase
    of bmkCommandComplete:
      if phase == 1:
        qr.commandTag = pumpMsg.commandTag
      inc phase
    else:
      discard
  do:
    if queryError != nil:
      # ROLLBACK a failed transaction, swallowing any failure so it cannot
      # mask the query error; the outer wait(timeout) bounds the cleanup.
      if conn.txStatus == tsInFailedTransaction:
        try:
          discard await conn.simpleExec("ROLLBACK")
        except CancelledError as e:
          # Don't swallow cancellation (e.g. the outer wait(timeout)
          # cancelling this future under chronos) — propagate it.
          raise e
        except CatchableError:
          discard

  return qr

proc execInTransactionImpl(
    conn: PgConnection,
    beginSql: string,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
): Future[string] {.async.} =
  let qr = await queryInTransactionImpl(
    conn, beginSql, sql, params, paramOids, paramFormats, @[]
  )
  return qr.commandTag

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined BEGIN/COMMIT transaction (1 round trip).
  ## On error, ROLLBACK is issued automatically.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    let (oids, formats, values) = extractParams(params)
    if timeout > ZeroDuration:
      try:
        tag = await execInTransactionImpl(conn, "BEGIN", sql, values, oids, formats)
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("execInTransaction timed out")
    else:
      tag = await execInTransactionImpl(conn, "BEGIN", sql, values, oids, formats)
  return initCommandResult(tag)

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    opts: TransactionOptions,
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined transaction with options.
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    let (oids, formats, values) = extractParams(params)
    let beginSql = buildBeginSql(opts)
    if timeout > ZeroDuration:
      try:
        tag = await execInTransactionImpl(conn, beginSql, sql, values, oids, formats)
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("execInTransaction timed out")
    else:
      tag = await execInTransactionImpl(conn, beginSql, sql, values, oids, formats)
  return initCommandResult(tag)

proc queryInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined BEGIN/COMMIT transaction (1 round trip).
  ## Returns rows. On error, ROLLBACK is issued automatically.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var qr: QueryResult
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: qr.commandTag, rowCount: qr.rowCount),
  ):
    let (oids, formats, values) = extractParams(params)
    let resultFormats = resultFormat.toFormatCodes()
    if timeout > ZeroDuration:
      try:
        qr = await queryInTransactionImpl(
          conn, "BEGIN", sql, values, oids, formats, resultFormats
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("queryInTransaction timed out")
    else:
      qr = await queryInTransactionImpl(
        conn, "BEGIN", sql, values, oids, formats, resultFormats
      )
  return qr

proc queryInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    opts: TransactionOptions,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query inside a pipelined transaction with options.
  var qr: QueryResult
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: qr.commandTag, rowCount: qr.rowCount),
  ):
    let (oids, formats, values) = extractParams(params)
    let resultFormats = resultFormat.toFormatCodes()
    let beginSql = buildBeginSql(opts)
    if timeout > ZeroDuration:
      try:
        qr = await queryInTransactionImpl(
          conn, beginSql, sql, values, oids, formats, resultFormats
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("queryInTransaction timed out")
    else:
      qr = await queryInTransactionImpl(
        conn, beginSql, sql, values, oids, formats, resultFormats
      )
  return qr
