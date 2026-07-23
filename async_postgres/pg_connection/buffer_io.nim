## Transport-layer buffering and message I/O.
##
## - recvBuf/sendBuf management (compact, fill, send)
## - Synchronous backend-message parsing (`nextMessage`) and the async wrapper
##   `recvMessage`
## - Notification/Notice dispatch (called from `nextMessage`)
## - Transport teardown (`closeTransport`)
## - TCP keepalive / TCP_NODELAY socket options
## - Host helpers (`isUnixSocket`, `unixSocketPath`, `getHosts`)
## - `makeCopyOutCallback` / `makeCopyInCallback` cross-backend templates
##
## Re-exported through `pg_connection.nim`; depends only on `types.nim` and
## the protocol/error/backend abstraction modules.

import std/[deques, options, tables]
when defined(posix):
  import std/posix

import ../[async_backend, pg_errors, pg_protocol]
import types

when hasChronos:
  import chronos/streams/tlsstream
elif hasAsyncDispatch:
  import std/asyncnet

when defined(posix):
  # POSIX socket option constants (used by liveness probes and TCP keepalive)
  var TCP_NODELAY* {.importc, header: "<netinet/tcp.h>".}: cint
  var MSG_DONTWAIT* {.importc, header: "<sys/socket.h>".}: cint

type
  RecvWatch* = ref object
    ## A single in-flight background socket read used to watch for an unsolicited
    ## backend message while the client is busy sending.
    ##
    ## A `ref` so it can be passed into and mutated by `async` helpers (a `var` of
    ## a value type cannot be captured across an `await`).
    ##
    ## Contract: at most one background read per connection is in flight at a
    ## time. The read carries no per-read timeout — bound the whole operation with
    ## an outer `wait`. Before reusing the normal recv path (`fillRecvBuf` /
    ## `nextMessage` on freshly read bytes) the watch must be settled: either
    ## consume it via `take` + `await`, or drop it with `cancel` immediately
    ## before raising.
    fut: Future[void]

  SocketPeek = enum
    ## Outcome of a single non-blocking `MSG_PEEK` byte probe of a socket.
    spData ## bytes are readable in the kernel buffer (`recv` > 0)
    spClosed ## peer has closed: FIN/RST observed (`recv` == 0)
    spIdle ## socket alive with no data ready (`EAGAIN`/`EWOULDBLOCK`)
    spTransient ## transient kernel resource exhaustion (`ENOMEM`/`ENOBUFS`)
    spError ## any other `recv` error
    spUnavailable ## no transport handle, or probe unsupported (non-POSIX)

# Host / address helpers

proc isUnixSocket*(host: string): bool {.inline.} =
  ## True if `host` represents a Unix socket directory (starts with '/').
  ## Compatible with libpq behavior.
  host.len > 0 and host[0] == '/'

proc unixSocketPath*(host: string, port: int): string =
  ## Build the libpq-compatible Unix socket file path: ``{dir}/.s.PGSQL.{port}``.
  host & "/.s.PGSQL." & $port

proc getHosts*(config: ConnConfig): seq[HostEntry] =
  ## Return the list of hosts to try. If `hosts` is populated, return it;
  ## otherwise synthesize a single entry from `host`/`port`.
  if config.hosts.len > 0:
    config.hosts
  else:
    @[
      HostEntry(
        host: config.host,
        hostaddr: config.hostaddr,
        port: if config.port == 0: 5432 else: config.port,
      )
    ]

# COPY callback factories (cross-backend)

template makeCopyOutCallback*(body: untyped): CopyOutCallback =
  ## Create a ``CopyOutCallback`` that works with both asyncdispatch and chronos.
  ## Inside ``body``, the current chunk is available as ``data: sink seq[byte]``.
  ##
  ## .. code-block:: nim
  ##   var chunks: seq[seq[byte]]
  ##   let cb = makeCopyOutCallback:
  ##     chunks.add(data)
  makeAsyncSinkByteCallback(CopyOutCallback, body)

template makeCopyInCallback*(body: untyped): CopyInCallback =
  ## Create a ``CopyInCallback`` that works with both asyncdispatch and chronos.
  ## ``body`` must evaluate to ``seq[byte]``. Return an empty seq to signal completion.
  ##
  ## .. code-block:: nim
  ##   var idx = 0
  ##   let rows = @["1\tAlice\n".toBytes(), "2\tBob\n".toBytes()]
  ##   let cb = makeCopyInCallback:
  ##     if idx < rows.len:
  ##       let chunk = rows[idx]
  ##       inc idx
  ##       chunk
  ##     else:
  ##       newSeq[byte]()
  makeAsyncSeqByteCallback(CopyInCallback, body)

# Notification / notice dispatch

proc dispatchNotification*(conn: PgConnection, msg: BackendMessage) {.raises: [].} =
  let notif = Notification(
    pid: msg.notifPid, channel: msg.notifChannel, payload: msg.notifPayload
  )
  # A positive `notifyMaxQueue` caps the pull-API queue and drops the oldest
  # notifications on overflow. `<= 0` means an unbounded queue (libpq/psycopg
  # convention, mirroring Python's `queue.Queue(maxsize<=0)`): never drop, just
  # accumulate until `waitNotification` drains it. The queue is enqueued and the
  # pull-API waiter completed unconditionally, so `waitNotification` works for
  # every `notifyMaxQueue`; only the overflow bookkeeping is gated on a cap.
  var droppedNow = 0
  if conn.notifyMaxQueue > 0:
    while conn.notifyQueue.len >= conn.notifyMaxQueue:
      discard conn.notifyQueue.popFirst()
      if conn.notifyDropped < high(int):
        conn.notifyDropped.inc
      droppedNow.inc
  conn.notifyQueue.addLast(notif)
  if droppedNow > 0 and conn.notifyOverflowCallback != nil:
    conn.notifyOverflowCallback(droppedNow)
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    # asyncdispatch's `Future.complete` has inferred effect `Exception`
    # via the callback chain; swallow it to keep this proc `raises: []`.
    try:
      conn.notifyWaiter.complete()
    except Exception:
      discard
  if conn.notifyCallback != nil:
    conn.notifyCallback(notif)

proc dispatchNotice*(conn: PgConnection, msg: BackendMessage) {.raises: [].} =
  if conn.noticeCallback != nil:
    conn.noticeCallback(Notice(fields: msg.noticeFields))

# Raw send helpers (asyncdispatch only)

when hasAsyncDispatch:
  proc sendRawData*(socket: AsyncSocket, p: pointer, len: int): Future[void] =
    ## Send raw bytes via asyncdispatch socket. Copies data into a string once.
    if len == 0:
      var fut = newFuture[void]("sendRawData")
      fut.complete()
      return fut
    var s = newString(len)
    copyMem(addr s[0], p, len)
    socket.send(move s)

  proc sendRawBytes*(socket: AsyncSocket, data: seq[byte]): Future[void] =
    ## Send ``seq[byte]`` via asyncdispatch socket.
    if data.len == 0:
      var fut = newFuture[void]("sendRawBytes")
      fut.complete()
      return fut
    sendRawData(socket, addr data[0], data.len)

# Receive buffer management

proc compactRecvBuf*(conn: PgConnection) {.inline.} =
  ## Shift unconsumed data to the front of recvBuf, reclaiming space consumed
  ## by the read pointer.  Called only before reading new data from the socket.
  let start = conn.recvBufStart
  if start == 0:
    return
  let remaining = conn.recvBuf.len - start
  if remaining == 0:
    conn.recvBuf.setLen(0)
  else:
    moveMem(addr conn.recvBuf[0], addr conn.recvBuf[start], remaining)
    conn.recvBuf.setLen(remaining)
  conn.recvBufStart = 0

proc fillRecvBuf*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Read data from socket into buffer. The only await point for message reception.
  ##
  ## On `AsyncTimeoutError` the caller (typically `invalidateOnTimeout`) is
  ## responsible for the state transition. On any other `CatchableError`
  ## (transport failure, cancellation, etc.) the connection is marked
  ## `csClosed` before re-raising, since the read may have consumed an
  ## indeterminate number of bytes from the socket and the stream is no
  ## longer parseable.
  conn.compactRecvBuf()
  when hasChronos:
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + RecvBufSize)
    var n: int
    try:
      n =
        if timeout == ZeroDuration:
          await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize)
        else:
          await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize).wait(
            timeout
          )
    except AsyncTimeoutError as e:
      conn.recvBuf.setLen(oldLen)
      raise e
    except CatchableError as e:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      raise e
    if n == 0:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      raise newException(PgConnectionError, "Connection closed by server")
    conn.recvBuf.setLen(oldLen + n)
  elif hasAsyncDispatch:
    # On timeout, `wait()` cannot cancel `recvInto` — the orphan may still write
    # into `recvBuf[oldLen..]` after we truncate. Safe because `invalidateOnTimeout`
    # marks csClosed (no further extender) and seq shrink keeps capacity.
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + RecvBufSize)
    var n: int
    try:
      n =
        if timeout == ZeroDuration:
          await conn.socket.recvInto(addr conn.recvBuf[oldLen], RecvBufSize)
        else:
          await conn.socket.recvInto(addr conn.recvBuf[oldLen], RecvBufSize).wait(
            timeout
          )
    except AsyncTimeoutError as e:
      conn.recvBuf.setLen(oldLen)
      raise e
    except CatchableError as e:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      raise e
    if n == 0:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      raise newException(PgConnectionError, "Connection closed by server")
    conn.recvBuf.setLen(oldLen + n)

when hasChronos:
  proc fillRecvBufDetached*(conn: PgConnection): Future[void] {.async.} =
    ## Read one chunk into a private scratch buffer and append it to ``recvBuf``
    ## only once the read settles, leaving ``recvBuf`` parseable while the read is
    ## still in flight.
    ##
    ## ``fillRecvBuf`` grows ``recvBuf`` by ``RecvBufSize`` up front to hand the
    ## chronos ``readOnce`` a destination pointer, so a caller that parses
    ## ``recvBuf`` before the read completes would see uninitialised tail bytes.
    ## The replication status-interval path keeps a single read pending across
    ## timer wakes and parses between them, so it reads through here instead (see
    ## ``replFillRecvBuf``). On any read failure the connection is marked
    ## ``csClosed`` before re-raising, matching ``fillRecvBuf``.
    if conn.replReadScratch.len < RecvBufSize:
      conn.replReadScratch.setLen(RecvBufSize)
    let n =
      try:
        await conn.reader.readOnce(addr conn.replReadScratch[0], RecvBufSize)
      except CatchableError as e:
        conn.state = csClosed
        raise e
    if n == 0:
      conn.state = csClosed
      raise newException(PgConnectionError, "Connection closed by server")
    conn.compactRecvBuf()
    let oldLen = conn.recvBuf.len
    conn.recvBuf.setLen(oldLen + n)
    copyMem(addr conn.recvBuf[oldLen], addr conn.replReadScratch[0], n)

proc nextMessage*(
    conn: PgConnection,
    rowData: RowData = nil,
    rowCount: ptr int32 = nil,
    onRow: RowCallback = nil,
    onRowError: ptr ref CatchableError = nil,
    skipDataRow: bool = false,
): Option[BackendMessage] {.raises: [PgProtocolError].} =
  ## Synchronously parse the next message from the receive buffer.
  ## Returns none if the buffer doesn't contain a complete message.
  ## Notification/Notice messages are dispatched internally.
  ## ParameterStatus messages are recorded into `conn.serverParams` and
  ## consumed, so callers never see them.
  ## DataRow messages are consumed: when `onRow` is nil, they are counted
  ## (if `rowCount != nil`) and left decoded in `rowData` for the caller;
  ## when `onRow` is set, it is invoked once per row and `rowData.buf` /
  ## `rowData.cellIndex` are reset before the next row, giving streaming
  ## callers (e.g. ``queryEach``) constant memory. When `onRow` raises,
  ## the error is captured into ``onRowError[]`` (required to be non-nil
  ## when `onRow` is set) and subsequent rows are drained without
  ## re-invoking the callback.
  ## When ``rowData == nil`` and ``onRow == nil`` and ``skipDataRow`` is true,
  ## DataRow messages are also skipped without decoding their columns — used
  ## by discard-only consumers (exec paths, simple-protocol exec) to avoid a
  ## per-row ``seq[Option[seq[byte]]]`` + per-cell ``seq`` allocation.
  ##
  ## On `PgProtocolError` the protocol stream is desynchronised — the connection
  ## is transitioned to `csClosed` before re-raising so that it is never
  ## reused (in particular, by the connection pool).
  var pos = conn.recvBufStart
  let maxLen = conn.effectiveMaxMessageSize()
  while true:
    var consumed: int
    let res =
      try:
        parseBackendMessage(
          conn.recvBuf.toOpenArray(pos, conn.recvBuf.len - 1),
          consumed,
          rowData,
          maxLen,
          skipDataRow = skipDataRow and rowData == nil and onRow == nil,
        )
      except PgProtocolError as e:
        conn.state = csClosed
        raise e
    if res.state == psIncomplete:
      return none(BackendMessage)
    pos += consumed
    conn.recvBufStart = pos
    if res.state == psDataRow:
      if onRow != nil:
        if onRowError[] == nil:
          try:
            onRow(initRow(rowData, 0))
            if rowCount != nil:
              rowCount[] += 1
          except CatchableError as e:
            onRowError[] = e
        rowData.buf.setLen(0)
        rowData.cellIndex.setLen(0)
      elif rowCount != nil:
        rowCount[] += 1
      continue
    if res.message.kind == bmkNotificationResponse:
      conn.dispatchNotification(res.message)
      continue
    if res.message.kind == bmkNoticeResponse:
      conn.dispatchNotice(res.message)
      continue
    if res.message.kind == bmkParameterStatus:
      # Keep serverParams current for the whole session (e.g. in_hot_standby
      # after a standby promotion), like libpq's pqSaveParameterStatus.
      let m = res.message
      conn.serverParams[m.paramName] = m.paramValue
      continue
    if res.message.kind == bmkNegotiateProtocolVersion:
      # Informational per libpq; record and drop so callers never see it.
      let m = res.message
      conn.negotiatedMinorVersion = m.newestMinorVersion
      conn.unrecognizedStartupOptions = m.unrecognizedOptions
      continue
    if res.message.kind == bmkDataRow and rowCount != nil:
      rowCount[] += 1
      continue
    return some(res.message)

proc recvMessage*(
    conn: PgConnection,
    timeout = ZeroDuration,
    rowData: RowData = nil,
    rowCount: ptr int32 = nil,
): Future[BackendMessage] {.async.} =
  ## Receive a single backend message from the connection.
  ## Thin wrapper around nextMessage + fillRecvBuf for backward compatibility.
  while true:
    let opt = conn.nextMessage(rowData, rowCount)
    if opt.isSome:
      return opt.get
    await conn.fillRecvBuf(timeout)

template pumpUntilReady*(
    conn: PgConnection,
    resultData: untyped,
    rowCountPtr: untyped,
    body: untyped,
    readyBody: untyped,
) {.dirty.} =
  ## Generic protocol pump loop.  `pumpMsg` and `queryError` are accessible
  ## in `body` and `readyBody`.  `DataRow` messages are decoded in-place into
  ## `resultData` and counted through `rowCountPtr` by `nextMessage`, so they
  ## never surface in `body`.
  ##
  ## The loop is spelled out in both overloads rather than one forwarding to
  ## the other: `{.dirty.}` injection only reaches `body`/`readyBody` across a
  ## single template boundary, so forwarding would leave their references to
  ## `pumpMsg`/`queryError` undeclared.  A single template with defaulted
  ## `resultData`/`rowCountPtr` fails the same way — typed params ahead of the
  ## untyped bodies suppress the injection (Nim 2.2.x).
  block pumpLoop:
    # Declared inside the block so two pumps in one proc scope (e.g. copy.nim's
    # main loop plus its recvLoop2) don't collide on these dirty-injected names.
    var queryError: ref PgQueryError
    var pumpMsg: BackendMessage
    while true:
      while (let opt = conn.nextMessage(resultData, rowCountPtr); opt.isSome):
        pumpMsg = opt.get
        if pumpMsg.kind == bmkErrorResponse:
          if queryError == nil:
            queryError = newPgQueryError(pumpMsg.errorFields)
        elif pumpMsg.kind == bmkReadyForQuery:
          conn.txStatus = pumpMsg.txStatus
          if conn.state != csClosed:
            conn.state = csReady
          readyBody
          if queryError != nil:
            raise queryError
          break pumpLoop
        else:
          body
      await conn.fillRecvBuf()

template pumpUntilReady*(
    conn: PgConnection,
    resultData: untyped,
    onRow: untyped,
    onRowErr: untyped,
    body: untyped,
    readyBody: untyped,
) {.dirty.} =
  ## Streaming overload: `resultData` is a `RowData` for in-place DataRow
  ## decoding, `onRow` a `RowCallback` invoked per row, `onRowErr` a
  ## `ptr ref CatchableError` capturing the first callback failure so
  ## remaining rows drain without re-invoking `onRow`.
  block pumpLoop:
    var queryError: ref PgQueryError
    var pumpMsg: BackendMessage
    while true:
      while (let opt = conn.nextMessage(resultData, nil, onRow, onRowErr); opt.isSome):
        pumpMsg = opt.get
        if pumpMsg.kind == bmkErrorResponse:
          if queryError == nil:
            queryError = newPgQueryError(pumpMsg.errorFields)
        elif pumpMsg.kind == bmkReadyForQuery:
          conn.txStatus = pumpMsg.txStatus
          if conn.state != csClosed:
            conn.state = csReady
          readyBody
          if queryError != nil:
            raise queryError
          break pumpLoop
        else:
          body
      await conn.fillRecvBuf()

template pumpUntilReady*(
    conn: PgConnection, body: untyped, readyBody: untyped
) {.dirty.} =
  ## Bare overload for callers that do not accumulate into a `RowData`.
  ## Body mirrors the data overload above with `nextMessage()` in place of
  ## the accumulating call.
  ##
  ## `DataRow` is skipped in the parser here — no current caller inspects
  ## rows inside `body`: exec paths (extended-query, simple-protocol)
  ## discard them, and the remaining callers (prepare, close, cursor
  ## close, COPY setup, ping) never receive `DataRow` in a well-formed
  ## reply. If a future caller needs the row bytes, drop
  ## `skipDataRow = true` here.
  block pumpLoop:
    var queryError: ref PgQueryError
    var pumpMsg: BackendMessage
    while true:
      while (let opt = conn.nextMessage(skipDataRow = true); opt.isSome):
        pumpMsg = opt.get
        if pumpMsg.kind == bmkErrorResponse:
          if queryError == nil:
            queryError = newPgQueryError(pumpMsg.errorFields)
        elif pumpMsg.kind == bmkReadyForQuery:
          conn.txStatus = pumpMsg.txStatus
          if conn.state != csClosed:
            conn.state = csReady
          readyBody
          if queryError != nil:
            raise queryError
          break pumpLoop
        else:
          body
      await conn.fillRecvBuf()

# Non-blocking receive watch
#
# During an otherwise send-only phase (COPY IN) the client must keep streaming
# while still noticing an unsolicited backend message — typically an
# ErrorResponse aborting the COPY (constraint violation, disk full, …). The
# blocking-only read path (`fillRecvBuf`) cannot be polled, and there is no
# portable non-blocking socket read that also works over TLS and the two async
# backends. `RecvWatch` provides one: it keeps a single background read in
# flight whose completion is observed cheaply with `Future.finished`, adding no
# latency to the common (no early error) path.

proc startRecvWatch*(conn: PgConnection): RecvWatch =
  ## Begin watching for an unsolicited backend message. The bytes are committed
  ## to `recvBuf` when the read completes; poll with `ready`, then `take` +
  ## `await` (immediate once ready) and parse with `nextMessage`.
  RecvWatch(fut: conn.fillRecvBuf(ZeroDuration))

proc pending*(w: RecvWatch): bool =
  ## Whether a background read is currently in flight.
  w.fut != nil

proc ready*(w: RecvWatch): bool =
  ## Whether the in-flight read has settled, so `take` + `await` will not block.
  ## A read that failed also reports ready; awaiting it then re-raises.
  w.fut != nil and w.fut.finished

proc take*(w: RecvWatch): Future[void] =
  ## Surrender the in-flight read for the caller to `await` (immediate when
  ## `ready`). Clears the watch; the caller owns the returned Future.
  result = w.fut
  w.fut = nil

proc rearm*(w: RecvWatch, conn: PgConnection) =
  ## Resume watching with a fresh background read. Only call once the previous
  ## read has been consumed (`take` + `await`), never while one is still in
  ## flight.
  w.fut = conn.fillRecvBuf(ZeroDuration)

proc cancel*(w: RecvWatch) =
  ## Abandon any in-flight read. Must be followed immediately by raising/exit:
  ## on chronos the read is cancelled asynchronously (`cancelSoon`), so starting
  ## a new read before unwinding would race the cancellation against the shared
  ## `recvBuf`. On asyncdispatch (no cancellation) the read keeps running; its
  ## eventual result is swallowed so it never surfaces as an unhandled future
  ## error.
  if w.fut != nil and not w.fut.finished:
    when hasChronos:
      w.fut.cancelSoon()
    elif hasAsyncDispatch:
      w.fut.addCallback(
        proc(f: Future[void]) {.gcsafe.} =
          try:
            f.read()
          except CatchableError:
            discard
      )
  w.fut = nil

# Send helpers

proc sendMsg*(conn: PgConnection, data: seq[byte]): Future[void] {.async.} =
  ## Send raw bytes to the PostgreSQL server over the connection.
  ## On failure the connection is marked ``csClosed`` (the stream may be
  ## partially written), symmetric with ``fillRecvBuf``.
  when hasChronos:
    try:
      await conn.writer.write(data)
    except CatchableError as e:
      conn.state = csClosed
      raise e
  elif hasAsyncDispatch:
    if data.len > 0:
      try:
        await conn.socket.sendRawBytes(data)
      except CatchableError as e:
        conn.state = csClosed
        raise e

proc sendBufMsg*(conn: PgConnection): Future[void] {.async.} =
  ## Send conn.sendBuf to the server.
  ## The transport receives its own copy of the buffer, so conn.sendBuf is safe
  ## to mutate while the returned Future is still pending.
  ## On failure the connection is marked ``csClosed`` (the stream may be
  ## partially written), symmetric with ``sendMsg``.
  when hasChronos:
    if conn.sendBuf.len > 0:
      try:
        await conn.writer.write(conn.sendBuf)
      except CatchableError as e:
        conn.state = csClosed
        raise e
  elif hasAsyncDispatch:
    if conn.sendBuf.len > 0:
      try:
        await conn.socket.sendRawBytes(conn.sendBuf)
      except CatchableError as e:
        conn.state = csClosed
        raise e

# Transport teardown

proc closeTransport*(conn: PgConnection) {.async.} =
  ## Close transport resources without sending Terminate.
  when hasChronos:
    if conn.tlsStream != nil:
      try:
        await conn.tlsStream.reader.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsTlsReader, e)
      try:
        await conn.tlsStream.writer.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsTlsWriter, e)
      conn.tlsStream = nil
    if conn.baseReader != nil:
      try:
        await conn.baseReader.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsBaseReader, e)
      try:
        await conn.baseWriter.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsBaseWriter, e)
      conn.baseReader = nil
      conn.baseWriter = nil
    if conn.transport != nil:
      try:
        await conn.transport.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsTransport, e)
      conn.transport = nil
    # Drop the cached reader/writer aliases so isConnected() reports false.
    conn.reader = nil
    conn.writer = nil
  elif hasAsyncDispatch:
    if not conn.socket.isNil:
      conn.socket.close()
      conn.socket = nil

# Liveness probes

proc peekSocket(conn: PgConnection): SocketPeek =
  ## Single `recv(MSG_PEEK | MSG_DONTWAIT)` byte probe shared by the liveness
  ## and pre-TLS-injection checks. Classifies the kernel's view of the socket
  ## without consuming data or blocking; retries on `EINTR`. Callers decide
  ## what each outcome means (see `socketHasFin` / `socketHasPendingData`).
  when defined(posix):
    when hasChronos:
      if conn.transport.isNil:
        return spUnavailable
      let fd = posix.SocketHandle(conn.transport.fd)
    elif hasAsyncDispatch:
      if conn.socket.isNil:
        return spUnavailable
      let fd = posix.SocketHandle(conn.socket.getFd())
    var buf: byte
    let flags = posix.MSG_PEEK or MSG_DONTWAIT
    while true:
      let n = posix.recv(fd, addr buf, 1, flags)
      if n > 0:
        return spData
      if n == 0:
        return spClosed
      let err = errno
      if err == EINTR:
        continue
      if err == EAGAIN or err == EWOULDBLOCK:
        return spIdle
      if err == ENOMEM or err == ENOBUFS:
        return spTransient
      return spError
  else:
    spUnavailable

proc socketHasFin*(conn: PgConnection): bool =
  ## Non-blocking OS-level half-open probe (POSIX only).
  ##
  ## Returns `true` when the kernel has already observed a peer-side FIN/RST
  ## on this connection's underlying socket. Returns `false` when the socket
  ## is alive and idle, when there is pending data (which the next operation
  ## will handle), when the probe hits transient kernel resource exhaustion
  ## (`ENOMEM`/`ENOBUFS`, which says nothing about peer state), or when there
  ## is no transport handle to probe (e.g. mock connections, or after `close`).
  ##
  ## A single `recv(MSG_PEEK | MSG_DONTWAIT)` syscall — no round trip. For
  ## TLS connections this still detects TCP-level FIN/RST, but not TLS-layer
  ## errors that haven't been read yet; use `ping` for that.
  ##
  ## On non-POSIX platforms this always returns `false` (no probe available).
  case conn.peekSocket()
  of spClosed, spError:
    # FIN/RST observed, or an unclassified error we conservatively read as a
    # peer-side close.
    true
  of spData, spIdle, spTransient, spUnavailable:
    # Data pending (alive), idle, transient resource shortage (says nothing
    # about peer state, so keep the live socket rather than force a reconnect),
    # or no probe available.
    false

proc socketHasPendingData*(conn: PgConnection): bool =
  ## Non-blocking OS-level check: does the kernel currently hold readable
  ## bytes on this connection's socket? (POSIX only.)
  ##
  ## Used by SSL negotiation to detect pre-TLS plaintext injection
  ## (CVE-2021-23214 / CVE-2021-23222 family): after a server answers the
  ## SSLRequest with `'S'` it must stay silent until the client sends the TLS
  ## ClientHello, so any byte already readable was injected by a
  ## man-in-the-middle to be smuggled ahead of the encrypted stream.
  ##
  ## A single `recv(MSG_PEEK | MSG_DONTWAIT)` syscall — no round trip. Only a
  ## positive read of buffered bytes yields `true`. Returns `false` when the
  ## socket is idle (`EAGAIN`), when the peer has closed (`FIN`: nothing was
  ## injected), on `EINTR`/other transient errors, and where the probe is
  ## unavailable (non-POSIX, or no transport handle).
  ##
  ## Note: this sees only bytes still in the *kernel* buffer. Data the
  ## higher-level transport has already drained into its own buffer (the
  ## chronos `StreamTransport` may do this) is invisible here and must be
  ## detected by the caller reading more than the single response byte.
  ##
  ## Fail open: any non-data outcome (idle, FIN, transient or other error)
  ## yields `false` so a probe error never rejects a legitimate connection.
  conn.peekSocket() == spData

proc isConnected*(conn: PgConnection): bool =
  ## Whether the underlying transport is present and the OS has not yet
  ## observed a peer-side close.
  ##
  ## Cheap, non-blocking (no round trip): checks that the connection object
  ## holds a transport handle, and on POSIX also issues a single
  ## `recv(MSG_PEEK | MSG_DONTWAIT)` via `socketHasFin` to catch FIN/RST
  ## already sitting in the kernel buffer (half-open detection). On
  ## non-POSIX platforms the check falls back to handle presence only.
  ##
  ## Pair with `state == csReady` to decide whether a connection is usable
  ## before issuing a query. Use `ping` for a full server round trip when
  ## the OS-level probe is insufficient (e.g. TLS-layer state, application
  ## liveness rather than transport liveness).
  when hasChronos:
    if conn.writer.isNil:
      return false
  elif hasAsyncDispatch:
    if conn.socket.isNil:
      return false
  not conn.socketHasFin()

# TCP socket options

when defined(posix):
  proc setSockOptInt(
      fd: posix.SocketHandle, level, optname: cint, value: cint, name: string
  ) =
    var optval = value
    if setsockopt(fd, level, optname, addr optval, sizeof(optval).SockLen) < 0:
      raise newException(
        PgConnectionError, "Failed to set " & name & ": " & $strerror(errno)
      )

  proc configureTcpNoDelay*(fd: posix.SocketHandle) =
    ## Disable Nagle's algorithm for low-latency sends.
    var optval: cint = 1
    discard setsockopt(
      fd, cint(posix.IPPROTO_TCP), TCP_NODELAY, addr optval, sizeof(optval).SockLen
    )

  proc configureKeepalive*(fd: posix.SocketHandle, config: ConnConfig) =
    ## Set TCP keepalive options on the socket.
    if not config.keepAlive:
      return
    setSockOptInt(fd, SOL_SOCKET, SO_KEEPALIVE, 1, "SO_KEEPALIVE")
    when defined(linux) or defined(macosx):
      let ipproto = cint(posix.IPPROTO_TCP)
      if config.keepAliveIdle > 0:
        when defined(linux):
          setSockOptInt(
            fd, ipproto, TCP_KEEPIDLE, cint(config.keepAliveIdle), "TCP_KEEPIDLE"
          )
        else:
          setSockOptInt(
            fd, ipproto, TCP_KEEPALIVE, cint(config.keepAliveIdle), "TCP_KEEPALIVE"
          )
      if config.keepAliveInterval > 0:
        setSockOptInt(
          fd, ipproto, TCP_KEEPINTVL, cint(config.keepAliveInterval), "TCP_KEEPINTVL"
        )
      if config.keepAliveCount > 0:
        setSockOptInt(
          fd, ipproto, TCP_KEEPCNT, cint(config.keepAliveCount), "TCP_KEEPCNT"
        )
    else:
      if config.keepAliveIdle > 0 or config.keepAliveInterval > 0 or
          config.keepAliveCount > 0:
        {.
          warning:
            "TCP keepalive timing options (idle/interval/count) are not supported on this platform and will be ignored"
        .}
