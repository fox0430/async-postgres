## Pipelined single-statement transactions: `execInTransaction` and
## `queryInTransaction` issue BEGIN, the user SQL, and COMMIT with a single
## Sync round trip.
##
## Internal module: not part of the public API. Import the `pg_client` hub instead.

import std/options

import ../[async_backend, pg_protocol, pg_types]
import ../pg_connection/[types, buffer_io, simple_query]
import core

proc stageInTransaction(
    conn: PgConnection,
    beginSql: string,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16],
    describe: bool,
) =
  conn.checkReady()
  conn.checkTxIdle()
  validateExtendedQuery(sql, params.len, paramOids.len, stmtNameLen = 0)

  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)
  validateEncodedParams(params, formats.len, resultFormats.len, stmtNameLen = 0)

  # Pipeline: Parse+Bind+Execute for BEGIN, user SQL, COMMIT + Sync
  conn.beginSendBuf()
  # BEGIN
  conn.addParse("", beginSql)
  conn.addBind("", "", @[], @[])
  conn.addExecute("", 0)
  # User SQL
  conn.addParse("", sql, paramOids)
  conn.addBind("", "", formats, params, resultFormats)
  if describe:
    conn.addDescribe(dkPortal, "")
  conn.addExecute("", 0)
  # COMMIT
  conn.addParse("", "COMMIT")
  conn.addBind("", "", @[], @[])
  conn.addExecute("", 0)
  # Single Sync
  conn.addSync()
  conn.markBusy()

func noteCompletion(phase: var int, tag: var string, msg: BackendMessage) =
  ## Count one statement's completion; only the user statement's tag is kept.
  # Empty/comment-only user SQL yields EmptyQueryResponse instead of
  # CommandComplete; advance the phase anyway so the trailing COMMIT's
  # CommandComplete isn't captured as the user statement's tag.
  if msg.kind notin {bmkEmptyQueryResponse, bmkCommandComplete}:
    return
  if msg.kind == bmkCommandComplete and phase == 1:
    tag = msg.commandTag
  inc phase

template rollbackFailedTx(conn: PgConnection, queryError: ref PgQueryError) =
  # ROLLBACK without masking the query error; report failure via onCleanupSkipped.
  if queryError != nil and conn.txStatus == tsInFailedTransaction:
    try:
      discard await conn.simpleExec("ROLLBACK")
    except CancelledError as e:
      # Don't swallow cancellation (e.g. the outer wait(timeout)
      # cancelling this future under chronos) — propagate it.
      raise e
    except CatchableError as rollbackErr:
      conn.fireCleanupSkipped(ckTxRollback, csrCleanupFailed, rollbackErr)

proc queryInTransactionImpl(
    conn: PgConnection,
    beginSql: string,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16],
): Future[QueryResult] {.async.} =
  conn.stageInTransaction(
    beginSql, sql, params, paramOids, paramFormats, resultFormats, describe = true
  )
  await conn.sendStagedBufMsg()

  var qr = QueryResult()
  var phase = 0

  conn.pumpUntilReady(qr.data, addr qr.rowCount):
    case pumpMsg.kind
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
    of bmkEmptyQueryResponse, bmkCommandComplete:
      noteCompletion(phase, qr.commandTag, pumpMsg)
    else:
      discard
  do:
    conn.rollbackFailedTx(queryError)

  return qr

proc execInTransactionImpl(
    conn: PgConnection,
    beginSql: string,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
): Future[string] {.async.} =
  # No Describe, and the bare pump frames rows (e.g. RETURNING) without decoding.
  conn.stageInTransaction(
    beginSql, sql, params, paramOids, paramFormats, @[], describe = false
  )
  await conn.sendStagedBufMsg()

  var tag = ""
  var phase = 0

  conn.pumpUntilReady:
    noteCompletion(phase, tag, pumpMsg)
  do:
    conn.rollbackFailedTx(queryError)

  return tag

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined BEGIN/COMMIT transaction (1 round trip).
  ## Returned rows (e.g. ``RETURNING``) are discarded undecoded; read them with
  ## ``queryInTransaction``. On error, ROLLBACK is issued automatically.
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
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
    awaitOrInvalidate(
      conn,
      tag,
      execInTransactionImpl(conn, "BEGIN", sql, values, oids, formats),
      timeout,
      "execInTransaction timed out",
    )
  return initCommandResult(tag)

proc execInTransaction*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    opts: TransactionOptions,
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement inside a pipelined transaction with options.
  ## Returned rows (e.g. ``RETURNING``) are discarded undecoded; read them with
  ## ``queryInTransaction``.
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
    awaitOrInvalidate(
      conn,
      tag,
      execInTransactionImpl(conn, beginSql, sql, values, oids, formats),
      timeout,
      "execInTransaction timed out",
    )
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
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
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
    awaitOrInvalidate(
      conn,
      qr,
      queryInTransactionImpl(conn, "BEGIN", sql, values, oids, formats, resultFormats),
      timeout,
      "queryInTransaction timed out",
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
    awaitOrInvalidate(
      conn,
      qr,
      queryInTransactionImpl(conn, beginSql, sql, values, oids, formats, resultFormats),
      timeout,
      "queryInTransaction timed out",
    )
  return qr
