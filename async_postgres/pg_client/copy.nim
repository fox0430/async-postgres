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

proc drainToReady(conn: PgConnection) {.async.} =
  ## Discard buffered/incoming backend messages until ReadyForQuery, commit the
  ## transaction status it carries, and return the connection to csReady. Used to
  ## resynchronise once a COPY has otherwise concluded (server abort, callback
  ## CopyFail response, callback-side COPY OUT error). A transport failure here is
  ## re-raised after `fillRecvBuf` has already marked the connection csClosed;
  ## callers that want to surface a previously captured error instead use
  ## `drainToReadyBestEffort`, which swallows that secondary failure.
  block drainLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        if msg.kind == bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          if conn.state != csClosed:
            conn.state = csReady
          break drainLoop
      await conn.fillRecvBuf()

proc drainToReadyBestEffort(conn: PgConnection) {.async.} =
  ## `drainToReady`, but swallow a secondary transport failure so a previously
  ## captured error (a server abort or a callback failure) stays the one that is
  ## surfaced. A `CancelledError` still propagates — cancellation must never be
  ## swallowed.
  try:
    await conn.drainToReady()
  except CancelledError as e:
    raise e
  except CatchableError:
    discard

proc abortCopyWatch(conn: PgConnection, watch: RecvWatch) =
  ## An unrecoverable transport failure (or a teardown) mid-COPY: abandon the
  ## in-flight watch read and mark the connection csClosed so the now
  ## protocol-desynced connection is never reused (the next caller must reconnect
  ## rather than see a misleading busy state). Call immediately before re-raising
  ## — on chronos `cancel` schedules the read's cancellation, so no further
  ## await / recvBuf access may run before unwinding. `cancel` is a no-op when the
  ## watch read has already been consumed.
  watch.cancel()
  conn.state = csClosed

proc copyInRawImpl*(
    conn: PgConnection, sql: string, data: seq[byte]
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
        of bmkCopyOutResponse:
          # Wrong direction; keep draining to ReadyForQuery, then raise there.
          if queryError == nil:
            queryError = newException(
              PgQueryError, "COPY IN got a COPY ... TO STDOUT statement; use copyOut"
            )
        of bmkRowDescription, bmkCommandComplete, bmkEmptyQueryResponse:
          # Not a COPY statement; keep draining to ReadyForQuery, then raise there.
          if queryError == nil:
            queryError = newException(
              PgQueryError, "COPY IN requires a COPY ... FROM STDIN statement"
            )
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          return commandTag
        else:
          discard
      await conn.fillRecvBuf()

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
  except CatchableError as e:
    # Transport failure mid-stream: the protocol is out of sync, so invalidate
    # the connection and surface the original error.
    conn.abortCopyWatch(watch)
    raise e

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
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop2
        else:
          discard
      await conn.fillRecvBuf()

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
  ##
  ## A statement that is not ``COPY ... FROM STDIN`` (e.g. ``COPY ... TO
  ## STDOUT`` or a plain query) raises ``PgQueryError`` instead of silently
  ## succeeding without sending any data.
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
        tag = await copyInRawImpl(conn, sql, data).wait(timeout)
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
    conn: PgConnection, sql: string, callback: CopyInCallback
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
        of bmkCopyOutResponse:
          # Wrong direction; keep draining to ReadyForQuery, then raise there.
          if queryError == nil:
            queryError = newException(
              PgQueryError, "COPY IN got a COPY ... TO STDOUT statement; use copyOutStream"
            )
        of bmkRowDescription, bmkCommandComplete, bmkEmptyQueryResponse:
          # Not a COPY statement; keep draining to ReadyForQuery, then raise there.
          if queryError == nil:
            queryError = newException(
              PgQueryError, "COPY IN requires a COPY ... FROM STDIN statement"
            )
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          return info
        else:
          discard
      await conn.fillRecvBuf()

  # Pull data from the callback and send as CopyData in batches, watching for an
  # early server ErrorResponse so a doomed COPY stops pulling/streaming instead
  # of draining the whole callback and only failing after CopyDone.
  const batchThreshold = copyBatchSize
  var callbackError: ref CatchableError = nil
  conn.sendBuf.setLen(0)
  let watch = conn.startRecvWatch()
  try:
    while true:
      var chunk: seq[byte]
      try:
        chunk = await callback()
        if chunk.len == 0:
          break
        encodeCopyData(conn.sendBuf, chunk)
      except CancelledError as e:
        # Cancellation (e.g. a `wait()`-driven timeout tearing down the stream)
        # is not a recoverable callback failure: let it reach the outer handler,
        # which abandons the watch read and invalidates the connection rather
        # than attempting CopyFail on a future that is being unwound.
        raise e
      except CatchableError as e:
        # A callback failure — or a local error encoding its chunk (e.g. an
        # oversized chunk that overflows the CopyData length field) — is
        # recoverable: the transport is still healthy, so CopyFail can abort the
        # COPY cleanly (handled below). Only these errors take this path —
        # transport failures must not be sent CopyFail.
        callbackError = e
        break
      if conn.sendBuf.len >= batchThreshold:
        await conn.sendBufMsg()
        conn.sendBuf.setLen(0)
        queryError = await conn.pollCopyInError(watch)
        if queryError != nil:
          break
  except CatchableError as e:
    # Transport failure on the send or on the background watch read
    # (`pollCopyInError` re-raises those). The recv side is already gone, so
    # attempting CopyFail would only raise a secondary send error and mask the
    # real failure — abandon the watch read, invalidate the connection and
    # surface the original error.
    conn.abortCopyWatch(watch)
    raise e

  if queryError != nil:
    # Server aborted the COPY mid-stream (already detected; watch consumed). In
    # the simple-query protocol the backend has left copy-in mode and will emit
    # ReadyForQuery, so neither CopyDone nor CopyFail is needed — drain and raise.
    # If the transport dies before the drain reaches ReadyForQuery the connection
    # is already csClosed; surface the server's error, not that secondary failure.
    conn.sendBuf.setLen(0)
    await conn.drainToReadyBestEffort()
    raise queryError
  elif callbackError != nil:
    # The callback raised. Flushing pending data is pointless, but before
    # sending CopyFail check whether the server already aborted the COPY in the
    # window between the last `pollCopyInError` rearm and the raise: if it did,
    # the backend has left copy-in mode and a CopyFail would be a stray message
    # that desyncs the stream, so drain the abort and surface the server's error
    # instead of the callback's.
    conn.sendBuf.setLen(0)
    try:
      queryError = await conn.pollCopyInError(watch)
    except CatchableError as e:
      # `pollCopyInError` re-raises a transport failure on the watch read. The
      # recv side is gone, so (like the outer handler and copyInRawImpl) abandon
      # the watch read, invalidate the connection and surface the transport
      # failure rather than attempting CopyFail.
      conn.abortCopyWatch(watch)
      raise e
    if queryError != nil:
      # Backend already left copy-in mode; drain its abort and surface the
      # server's error, preferring it over a secondary transport failure here.
      await conn.drainToReadyBestEffort()
      raise queryError
    # Transport healthy and the backend still in copy-in mode: abort cleanly.
    try:
      await conn.sendMsg(encodeCopyFail(callbackError.msg))
    except CatchableError as e:
      # Transport is gone before CopyFail was delivered; invalidate the
      # connection and surface the transport failure.
      conn.abortCopyWatch(watch)
      raise e
    # CopyFail was sent; fold the in-flight watch read to receive its response
    # and drain. `take` consumes the watch read here, so on failure there is
    # nothing left to abandon — just invalidate the connection. If the transport
    # dies the abort was already delivered, so surface the original callback error
    # rather than the secondary transport failure.
    try:
      if watch.pending:
        await watch.take()
      await conn.drainToReady()
    except CancelledError as e:
      # Cancellation tears the operation down: invalidate and propagate it rather
      # than masking it with the callback error.
      conn.state = csClosed
      raise e
    except CatchableError:
      conn.state = csClosed
      raise callbackError
    raise callbackError
  else:
    # Normal completion: flush remaining data + CopyDone in one send
    conn.sendBuf.addCopyDone()
    try:
      await conn.sendBufMsg()
    except CatchableError as e:
      # Transport failure on the final CopyDone flush: the protocol is out of
      # sync, so abandon the watch read, invalidate the connection and surface
      # the original error. Mirrors copyInRawImpl and the outer handler.
      conn.abortCopyWatch(watch)
      raise e
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
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop2
        else:
          discard
      await conn.fillRecvBuf()

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
  ##
  ## A statement that is not ``COPY ... FROM STDIN`` raises ``PgQueryError``
  ## instead of silently succeeding without pulling from ``callback``.
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
        info = await copyInStreamImpl(conn, sql, callback).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("COPY IN stream timed out")
    else:
      info = await copyInStreamImpl(conn, sql, callback)
  return info

proc copyOutImpl*(conn: PgConnection, sql: string): Future[CopyResult] {.async.} =
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
          if conn.state != csClosed:
            conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf()

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
        cr = await copyOutImpl(conn, sql).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("COPY OUT timed out")
    else:
      cr = await copyOutImpl(conn, sql)
  return cr

proc copyOutStreamImpl*(
    conn: PgConnection, sql: string, callback: CopyOutCallback
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
          except CancelledError as e:
            # Cancellation tears the operation down; do not run the recovery
            # drain (more I/O on a future being unwound). The COPY OUT is left
            # mid-stream so the protocol is out of sync — invalidate the
            # connection (the next caller must reconnect rather than see a
            # misleading busy state) and propagate the cancellation as-is.
            conn.state = csClosed
            raise e
          except CatchableError as e:
            # The callback raised. Drain remaining messages until ReadyForQuery
            # to keep the protocol in sync, then re-raise the callback error. If
            # the transport dies during the drain, `fillRecvBuf` has already
            # invalidated the connection (csClosed); surface the original
            # callback error rather than that secondary transport failure.
            await conn.drainToReadyBestEffort()
            raise e
        of bmkCopyDone:
          discard
        of bmkCommandComplete:
          info.commandTag = msg.commandTag
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
        info = await copyOutStreamImpl(conn, sql, callback).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("COPY OUT stream timed out")
    else:
      info = await copyOutStreamImpl(conn, sql, callback)
  return info
