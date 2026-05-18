## Server-side portal-based cursors: `openCursor`, `fetchNext`, `close`, and
## the scoped `withCursor` template.

import std/[options]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

type Cursor* = ref object
  ## A server-side portal for incremental row fetching via `declareCursor`/`fetch`.
  conn*: PgConnection
  portalName: string
  chunkSize: int32
  timeout: Duration
  fields*: seq[FieldDescription]
  exhausted*: bool
  bufferedData: RowData
  bufferedCount: int32

proc columnIndex*(cursor: Cursor, name: string): int =
  ## Find the index of a column by name in a cursor.
  cursor.fields.columnIndex(name)

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
          cursor.bufferedData.fields = cursor.fields
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

proc fetchNextImpl(
    cursor: Cursor, timeout: Duration = ZeroDuration
): Future[seq[Row]] {.async.} =
  let conn = cursor.conn
  let rd = newRowData(int16(cursor.fields.len))
  rd.fields = cursor.fields
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

  result = newSeq[Row](rowCount)
  for i in 0 ..< rowCount:
    result[i] = initRow(rd, i)

proc fetchNext*(cursor: Cursor): Future[seq[Row]] {.async.} =
  ## Fetch the next chunk of rows from the cursor.
  ## Returns an empty seq when the cursor is exhausted.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if cursor.bufferedCount > 0:
    result = newSeq[Row](cursor.bufferedCount)
    for i in 0 ..< cursor.bufferedCount:
      result[i] = initRow(cursor.bufferedData, i)
    cursor.bufferedData = nil
    cursor.bufferedCount = 0
    return result

  if cursor.exhausted:
    return @[]

  if cursor.timeout > ZeroDuration:
    try:
      return await fetchNextImpl(cursor, cursor.timeout).wait(cursor.timeout)
    except AsyncTimeoutError:
      cursor.conn.invalidateOnTimeout("Cursor fetch timed out")
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

  cursor.exhausted = true

proc close*(cursor: Cursor): Future[void] {.async.} =
  ## Close the cursor and return the connection to ready state.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if cursor.timeout > ZeroDuration:
    try:
      await closeCursorImpl(cursor, cursor.timeout).wait(cursor.timeout)
    except AsyncTimeoutError:
      cursor.conn.invalidateOnTimeout("Cursor close timed out")
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
    await cursorName.close()

proc openCursor*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    chunkSize: int32 = 100,
    timeout: Duration = ZeroDuration,
): Future[Cursor] {.async.} =
  ## Open a server-side cursor for streaming rows in chunks.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  let (oids, formats, values) = extractParams(params)
  let resultFormats = resultFormat.toFormatCodes()
  if timeout > ZeroDuration:
    try:
      result = await openCursorImpl(
        conn, sql, values, oids, formats, resultFormats, chunkSize, timeout
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.invalidateOnTimeout("Cursor open timed out")
  else:
    result =
      await openCursorImpl(conn, sql, values, oids, formats, resultFormats, chunkSize)
  result.timeout = timeout
