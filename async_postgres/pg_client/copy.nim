## COPY IN / COPY OUT via the simple-query protocol, including the streaming
## `copyInStream` / `copyOutStream` variants that move data through callbacks.

import std/[options]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

proc pollCopyInError(
    conn: PgConnection, watch: RecvWatch
): Future[ref PgQueryError] {.async.} =
  ## Non-blocking check, between COPY IN batches, for an unsolicited server
  ## ErrorResponse aborting the COPY (constraint violation, disk full, …).
  ##
  ## Returns the error — in which case `watch` has been consumed and the caller
  ## must drain to ReadyForQuery and raise — or `nil`, leaving `watch` armed to
  ## keep watching. A transport failure on the background read is re-raised.
  ## Only ever parses after the in-flight read has settled, so it never reads
  ## the read's uninitialised buffer tail.
  if not watch.ready:
    return nil
  await watch.take()
  while (let opt = conn.nextMessage(); opt.isSome):
    if opt.get.kind == bmkErrorResponse:
      return newPgQueryError(opt.get.errorFields)
  # Only a partial / non-error message buffered so far — resume watching.
  watch.rearm(conn)
  return nil

proc copyInRawImpl*(
    conn: PgConnection, sql: string, data: seq[byte], timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var commandTag = ""
  var queryError: ref PgQueryError

  # Wait for CopyInResponse (or error)
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyInResponse:
          break recvLoop
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          return commandTag
        else:
          discard
      await conn.fillRecvBuf(timeout)

  # Send CopyData in batches, slicing from the input buffer, while watching for
  # an early server ErrorResponse so a doomed COPY stops streaming instead of
  # blindly sending the whole input and only learning of the failure after
  # CopyDone.
  const maxPayload = copyBatchSize - 5 # leave room for CopyData header
  conn.sendBuf.setLen(0)
  let watch = conn.startRecvWatch()
  var offset = 0
  try:
    while offset < data.len:
      let endIdx = min(offset + maxPayload - 1, data.len - 1)
      encodeCopyData(conn.sendBuf, data.toOpenArray(offset, endIdx))
      offset = endIdx + 1
      if conn.sendBuf.len >= copyBatchSize:
        await conn.sendBufMsg()
        conn.sendBuf.setLen(0)
        queryError = await conn.pollCopyInError(watch)
        if queryError != nil:
          conn.sendBuf.setLen(0)
          break
    if queryError == nil:
      # Flush remaining data + CopyDone in one send
      conn.sendBuf.addCopyDone()
      await conn.sendBufMsg()
      conn.sendBuf.setLen(0)
  except CatchableError:
    watch.cancel()
    raise

  # Settle the in-flight watch read before parsing: on normal completion it
  # carries the CommandComplete/ReadyForQuery response; on an early error it was
  # already consumed (`watch.pending` is false) and the remaining bytes drain
  # via fillRecvBuf.
  if watch.pending:
    await watch.take()

  # Wait for CommandComplete + ReadyForQuery
  block recvLoop2:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCommandComplete:
          commandTag = msg.commandTag
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop2
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return commandTag

proc copyIn*(
    conn: PgConnection, sql: string, data: seq[byte], timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute COPY ... FROM STDIN with a single contiguous ``seq[byte]``.
  ## Avoids the copy that the ``openArray[byte]`` overload performs.
  ##
  ## A server-side abort (constraint violation, disk full, …) is detected
  ## between batches, so a doomed COPY stops streaming early and raises the
  ## server's ``PgQueryError`` instead of sending the whole input first.
  var tag: string
  withConnTracing(
    conn,
    onCopyStart,
    onCopyEnd,
    TraceCopyStartData(sql: sql, direction: tcdIn),
    TraceCopyEndData,
    TraceCopyEndData(commandTag: tag),
  ):
    if timeout > ZeroDuration:
      try:
        tag = await copyInRawImpl(conn, sql, data, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("COPY IN timed out")
    else:
      tag = await copyInRawImpl(conn, sql, data)
  return initCommandResult(tag)

proc copyIn*(
    conn: PgConnection,
    sql: string,
    data: openArray[byte],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] =
  ## Execute COPY ... FROM STDIN with a single contiguous buffer.
  ## Slices `data` into CopyData messages internally.
  ## Returns the command result (e.g. "COPY 5").
  let dataCopy = @data # copy openArray to seq before async boundary
  copyIn(conn, sql, dataCopy, timeout)

proc copyIn*(
    conn: PgConnection, sql: string, data: string, timeout: Duration = ZeroDuration
): Future[CommandResult] =
  ## Execute COPY ... FROM STDIN with text data as a string.
  ## Converts to bytes internally; avoids manual toOpenArrayByte.
  var bytes = newSeq[byte](data.len)
  if data.len > 0:
    bytes.writeBytesAt(0, data.toOpenArrayByte(0, data.high))
  copyIn(conn, sql, bytes, timeout)

proc copyIn*(
    conn: PgConnection,
    sql: string,
    data: seq[seq[byte]],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] =
  ## Execute COPY ... FROM STDIN via simple query protocol.
  ## Concatenates chunks and delegates to the ``seq[byte]`` overload.
  ## Returns the command result (e.g. "COPY 5").
  var totalLen = 0
  for chunk in data:
    totalLen += chunk.len
  var combined = newSeq[byte](totalLen)
  var offset = 0
  for chunk in data:
    combined.writeBytesAt(offset, chunk)
    offset += chunk.len
  copyIn(conn, sql, combined, timeout)

proc copyInStreamImpl*(
    conn: PgConnection,
    sql: string,
    callback: CopyInCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyInInfo] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var info = CopyInInfo()
  var queryError: ref PgQueryError

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
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          return info
        else:
          discard
      await conn.fillRecvBuf(timeout)

  # Pull data from the callback and send as CopyData in batches, watching for an
  # early server ErrorResponse so a doomed COPY stops pulling/streaming instead
  # of draining the whole callback and only failing after CopyDone.
  const batchThreshold = copyBatchSize
  var callbackError: ref CatchableError = nil
  conn.sendBuf.setLen(0)
  let watch = conn.startRecvWatch()
  try:
    while true:
      let chunk = await callback()
      if chunk.len == 0:
        break
      encodeCopyData(conn.sendBuf, chunk)
      if conn.sendBuf.len >= batchThreshold:
        await conn.sendBufMsg()
        conn.sendBuf.setLen(0)
        queryError = await conn.pollCopyInError(watch)
        if queryError != nil:
          break
  except CatchableError as e:
    callbackError = e

  if queryError != nil:
    # Server aborted the COPY mid-stream (already detected; watch consumed). In
    # the simple-query protocol the backend has left copy-in mode and will emit
    # ReadyForQuery, so neither CopyDone nor CopyFail is needed — drain and raise.
    conn.sendBuf.setLen(0)
    block drainErr:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            break drainErr
          else:
            discard
        await conn.fillRecvBuf(timeout)
    raise queryError
  elif callbackError != nil:
    # Callback raised: flush pending data is pointless, send CopyFail
    conn.sendBuf.setLen(0)
    try:
      await conn.sendMsg(encodeCopyFail(callbackError.msg))
    except CatchableError:
      # Transport is gone; abandon the watch read and surface the failure.
      watch.cancel()
      raise
    # Fold the in-flight watch read to receive the CopyFail response, then drain.
    if watch.pending:
      await watch.take()
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
    await conn.sendBufMsg()
    conn.sendBuf.setLen(0)

  # Settle the in-flight watch read (normal completion) before parsing.
  if watch.pending:
    await watch.take()

  # Wait for CommandComplete + ReadyForQuery
  block recvLoop2:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCommandComplete:
          info.commandTag = msg.commandTag
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
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
  ##
  ## If the server aborts the COPY (constraint violation, disk full, …) while
  ## the client is still streaming, the error is detected between batches and
  ## the COPY stops pulling from ``callback`` instead of draining it in full;
  ## the server's ``PgQueryError`` is then raised. Detection is best-effort —
  ## an error arriving mid-batch only surfaces after the current batch — so a
  ## doomed COPY is bounded by one batch of extra streaming, not the whole input.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var info: CopyInInfo
  withConnTracing(
    conn,
    onCopyStart,
    onCopyEnd,
    TraceCopyStartData(sql: sql, direction: tcdIn),
    TraceCopyEndData,
    TraceCopyEndData(commandTag: info.commandTag),
  ):
    if timeout > ZeroDuration:
      try:
        info = await copyInStreamImpl(conn, sql, callback, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("COPY IN stream timed out")
    else:
      info = await copyInStreamImpl(conn, sql, callback)
  return info

proc copyOutImpl*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CopyResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var cr = CopyResult()
  var queryError: ref PgQueryError

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

  return cr

proc copyOut*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CopyResult] {.async.} =
  ## Execute COPY ... TO STDOUT via simple query protocol.
  ## Collects all CopyData messages and returns them in a CopyResult.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var cr: CopyResult
  withConnTracing(
    conn,
    onCopyStart,
    onCopyEnd,
    TraceCopyStartData(sql: sql, direction: tcdOut),
    TraceCopyEndData,
    TraceCopyEndData(commandTag: cr.commandTag),
  ):
    if timeout > ZeroDuration:
      try:
        cr = await copyOutImpl(conn, sql, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("COPY OUT timed out")
    else:
      cr = await copyOutImpl(conn, sql)
  return cr

proc copyOutStreamImpl*(
    conn: PgConnection,
    sql: string,
    callback: CopyOutCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyOutInfo] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var info = CopyOutInfo()
  var queryError: ref PgQueryError

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
            # Drain remaining messages until ReadyForQuery to keep protocol in sync
            block drainLoop:
              while true:
                while (let opt2 = conn.nextMessage(); opt2.isSome):
                  let msg2 = opt2.get
                  case msg2.kind
                  of bmkReadyForQuery:
                    conn.txStatus = msg2.txStatus
                    conn.state = csReady
                    break drainLoop
                  else:
                    discard
                await conn.fillRecvBuf(timeout)
            raise e
        of bmkCopyDone:
          discard
        of bmkCommandComplete:
          info.commandTag = msg.commandTag
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

  return info

proc copyOutStream*(
    conn: PgConnection,
    sql: string,
    callback: CopyOutCallback,
    timeout: Duration = ZeroDuration,
): Future[CopyOutInfo] {.async.} =
  ## Execute COPY ... TO STDOUT via simple query protocol, streaming each
  ## CopyData chunk through `callback`. The callback is awaited, providing
  ## natural TCP backpressure.
  ##
  ## If the callback raises, COPY OUT has no client->server abort (CopyFail is
  ## COPY IN only), so the remaining CopyData is drained up to ReadyForQuery to
  ## keep the protocol in sync; the connection returns to csReady and stays
  ## usable, and the callback's error is then re-raised. Because the server
  ## still streams the whole result before ReadyForQuery, a callback failure on
  ## an early chunk of a large COPY can mean draining a substantial amount of
  ## data before the error surfaces — cancel the query out-of-band (`cancel`)
  ## if you need to abort a large COPY OUT promptly. On timeout the connection
  ## is instead marked csClosed (protocol out of sync).
  var info: CopyOutInfo
  withConnTracing(
    conn,
    onCopyStart,
    onCopyEnd,
    TraceCopyStartData(sql: sql, direction: tcdOut),
    TraceCopyEndData,
    TraceCopyEndData(commandTag: info.commandTag),
  ):
    if timeout > ZeroDuration:
      try:
        info = await copyOutStreamImpl(conn, sql, callback, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("COPY OUT stream timed out")
    else:
      info = await copyOutStreamImpl(conn, sql, callback)
  return info
