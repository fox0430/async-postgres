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
  colFormats: seq[int16]
  colTypeOids: seq[int32]
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
): Future[Cursor] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  inc conn.portalCounter
  let portalName = "_cursor_" & $conn.portalCounter

  var batch = newSeqOfCap[byte](sql.len + 128)
  conn.flushPendingStmtCloses(batch)
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
          # Describe(Portal) runs after Bind, so the server reports the bound
          # result formats. Mirror the query path: derive per-column format
          # codes and type OIDs so binary DataRows are decoded as binary
          # rather than misread as text.
          if resultFormats.len > 0:
            cursor.colFormats = newSeq[int16](cursor.fields.len)
            cursor.colTypeOids = newSeq[int32](cursor.fields.len)
            for i in 0 ..< cursor.fields.len:
              cursor.colTypeOids[i] = cursor.fields[i].typeOid
              if resultFormats.len == 1:
                cursor.fields[i].formatCode = resultFormats[0]
                cursor.colFormats[i] = resultFormats[0]
              elif i < resultFormats.len:
                cursor.fields[i].formatCode = resultFormats[i]
                cursor.colFormats[i] = resultFormats[i]
          cursor.bufferedData =
            newRowData(int16(msg.fields.len), cursor.colFormats, cursor.colTypeOids)
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
              await conn.fillRecvBuf()
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
              await conn.fillRecvBuf()
          raise queryError
        else:
          discard
      await conn.fillRecvBuf()

  return cursor

proc fetchNextImpl(cursor: Cursor): Future[seq[Row]] {.async.} =
  let conn = cursor.conn
  let rd = newRowData(int16(cursor.fields.len), cursor.colFormats, cursor.colTypeOids)
  rd.fields = cursor.fields
  var rowCount: int32 = 0

  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
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
              await conn.fillRecvBuf()
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
              await conn.fillRecvBuf()
          raise queryError
        else:
          discard
      await conn.fillRecvBuf()

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
      return await fetchNextImpl(cursor).wait(cursor.timeout)
    except AsyncTimeoutError:
      cursor.conn.invalidateOnTimeout("Cursor fetch timed out")
  else:
    return await fetchNextImpl(cursor)

proc closeCursorImpl(cursor: Cursor): Future[void] {.async.} =
  if cursor.exhausted:
    return

  let conn = cursor.conn
  # A connection retired by `invalidateOnTimeout` (e.g. a `fetchNext` timeout that
  # left the protocol out of sync) is `csClosed`. Sending Close/Sync on it would
  # write to a corrupted stream, and promoting it back to `csReady` on
  # ReadyForQuery below would let `releaseCore` hand the broken socket to the next
  # borrower. Leave the connection retired. (`csClosed` only: a `fetchNext` query
  # error drains cleanly to `csReady`, where the portal still needs closing.)
  # Still mark the cursor exhausted so a stray `fetchNext` after `close` short-
  # circuits to `@[]` instead of writing to the corrupted socket.
  if conn.state == csClosed:
    cursor.exhausted = true
    return

  var batch = newSeqOfCap[byte](cursor.portalName.len + 16)
  conn.flushPendingStmtCloses(batch)
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
      await conn.fillRecvBuf()

  cursor.exhausted = true

proc close*(cursor: Cursor): Future[void] {.async.} =
  ## Close the cursor and return the connection to ready state.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if cursor.timeout > ZeroDuration:
    try:
      await closeCursorImpl(cursor).wait(cursor.timeout)
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
  ##
  ## A failure in the automatic `close` never masks an exception raised by
  ## `body`: the body error is captured and re-raised after the close attempt.
  ## (A `finally` block cannot be used here because an `await` inside `finally`
  ## clobbers the in-flight exception under asyncdispatch, silently discarding
  ## the body's error.) If `body` succeeds, a close failure propagates to the
  ## caller.
  let cursorName =
    await conn.openCursor(sql, chunkSize = chunks, timeout = cursorTimeout)
  var bodyErr: ref CatchableError = nil
  try:
    body
  except CatchableError as e:
    bodyErr = e

  if bodyErr != nil:
    # Body failed: still close the cursor, but never let a close failure mask
    # the original error.
    try:
      await cursorName.close()
    except CatchableError:
      discard
    raise bodyErr
  else:
    # Body succeeded: surface any close failure to the caller.
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
        conn, sql, values, oids, formats, resultFormats, chunkSize
      )
        .wait(timeout)
    except AsyncTimeoutError:
      conn.invalidateOnTimeout("Cursor open timed out")
  else:
    result =
      await openCursorImpl(conn, sql, values, oids, formats, resultFormats, chunkSize)
  result.timeout = timeout
