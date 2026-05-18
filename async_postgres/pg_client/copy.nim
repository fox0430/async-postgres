## COPY IN / COPY OUT via the simple-query protocol, including the streaming
## `copyInStream` / `copyOutStream` variants that move data through callbacks.

import std/[options]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

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

  # Send CopyData in batches, slicing from the input buffer
  const maxPayload = copyBatchSize - 5 # leave room for CopyData header
  conn.sendBuf.setLen(0)
  var offset = 0
  while offset < data.len:
    let endIdx = min(offset + maxPayload - 1, data.len - 1)
    encodeCopyData(conn.sendBuf, data.toOpenArray(offset, endIdx))
    offset = endIdx + 1
    if conn.sendBuf.len >= copyBatchSize:
      await conn.sendBufMsg()
      conn.sendBuf.setLen(0)
  # Flush remaining data + CopyDone in one send
  conn.sendBuf.addCopyDone()
  await conn.sendBufMsg()
  conn.sendBuf.setLen(0)

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

  # Pull data from callback and send as CopyData in batches
  const batchThreshold = copyBatchSize
  var callbackError: ref CatchableError = nil
  conn.sendBuf.setLen(0)
  try:
    while true:
      let chunk = await callback()
      if chunk.len == 0:
        break
      encodeCopyData(conn.sendBuf, chunk)
      if conn.sendBuf.len >= batchThreshold:
        await conn.sendBufMsg()
        conn.sendBuf.setLen(0)
  except CatchableError as e:
    callbackError = e

  if callbackError != nil:
    # Callback raised: flush pending data is pointless, send CopyFail
    conn.sendBuf.setLen(0)
    await conn.sendMsg(encodeCopyFail(callbackError.msg))
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
  ## natural TCP backpressure. If the callback raises, the connection is
  ## marked csClosed (protocol cannot be resynchronized).
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
