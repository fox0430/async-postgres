## Pipelined single-statement transactions: `execInTransaction` and
## `queryInTransaction` issue BEGIN, the user SQL, and COMMIT with a single
## Sync round trip.

import std/[options]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

proc execInTransactionImpl(
    conn: PgConnection,
    beginSql: string,
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
        tag = await execInTransactionImpl(
          conn, "BEGIN", sql, values, oids, formats, timeout
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("execInTransaction timed out")
    else:
      tag =
        await execInTransactionImpl(conn, "BEGIN", sql, values, oids, formats, timeout)
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
        tag = await execInTransactionImpl(
          conn, beginSql, sql, values, oids, formats, timeout
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("execInTransaction timed out")
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
    timeout: Duration = ZeroDuration,
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
          qr.data.fields = qr.fields
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
          conn, "BEGIN", sql, values, oids, formats, resultFormats, timeout
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("queryInTransaction timed out")
    else:
      qr = await queryInTransactionImpl(
        conn, "BEGIN", sql, values, oids, formats, resultFormats, timeout
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
          conn, beginSql, sql, values, oids, formats, resultFormats, timeout
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("queryInTransaction timed out")
    else:
      qr = await queryInTransactionImpl(
        conn, beginSql, sql, values, oids, formats, resultFormats, timeout
      )
  return qr
