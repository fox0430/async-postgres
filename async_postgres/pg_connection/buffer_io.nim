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
## The host helpers and `makeCopy*` templates are re-exported through
## `pg_connection.nim`; the transport buffering machinery stays here for
## sibling modules and tests. Depends only on `types.nim` and the
## protocol/error/backend abstraction modules.

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
  var TCP_NODELAY {.importc, header: "<netinet/tcp.h>".}: cint
  var MSG_DONTWAIT {.importc, header: "<sys/socket.h>".}: cint

type
  RecvWatch* = ref object
    ## Background read watch for unsolicited messages during send (at most one).
    ##
    ## Settle it (``take`` + ``await``, or ``cancel``) before reusing the normal
    ## recv path: an unsettled read shares ``recvBuf`` with whatever runs next.
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

proc enqueueNotification*(conn: PgConnection, notif: Notification) {.raises: [].} =
  ## Enqueue under ``notifyMaxQueue`` (<=0 = unbounded); drop oldest on overflow.
  # The cap counts queued notifications only: an outstanding handoff belongs to a
  # waiter about to consume it, and charging it here would shrink the depth by one.
  var droppedNow = 0
  if conn.notifyMaxQueue > 0:
    while conn.notifyQueue.len >= conn.notifyMaxQueue:
      discard conn.notifyQueue.popFirst()
      if conn.notifyDropped < high(int): # saturating lifetime counter
        conn.notifyDropped.inc
      droppedNow.inc
  conn.notifyQueue.addLast(notif)
  if droppedNow > 0 and conn.notifyOverflowCallback != nil:
    conn.notifyOverflowCallback(droppedNow)

proc requeueHandoff*(conn: PgConnection, notif: Notification) {.raises: [].} =
  ## Requeue an unconsumed handoff at the front.
  # Trims nothing, keeping the drop policy in one place: the queue may sit one
  # over the cap until the next arrival's drop-oldest reaches this entry.
  conn.notifyQueue.addFirst(notif)

proc reclaimHandoff*(conn: PgConnection) {.raises: [].} =
  ## Requeue a handoff whose waiter will never claim it, so an abandoned frame
  ## cannot make the notification unreachable.
  if conn.hasNotifyHandoff:
    conn.hasNotifyHandoff = false
    conn.requeueHandoff(move conn.notifyHandoff)

proc dispatchNotification*(conn: PgConnection, msg: BackendMessage) {.raises: [].} =
  let notif = Notification(
    pid: msg.notifPid, channel: msg.notifChannel, payload: msg.notifPayload
  )
  # Handed directly to an unresumed waiter: parking it in the shared queue
  # instead would make it the first thing the overflow drop discards.
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    conn.notifyHandoff = notif
    conn.hasNotifyHandoff = true
    # asyncdispatch's `Future.complete` has inferred effect `Exception`
    # via the callback chain; swallow it to keep this proc `raises: []`.
    try:
      conn.notifyWaiter.complete()
    except Exception:
      # The waiter will never resume, so nothing would ever move the handoff
      # back: queue it here instead of losing it.
      conn.hasNotifyHandoff = false
      conn.notifyHandoff = Notification()
      conn.enqueueNotification(notif)
  else:
    conn.enqueueNotification(notif)
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
  ## Compact recvBuf (caller checks ``csClosed``). Only safe before reading new
  ## data from the socket: it moves bytes an in-flight read still points at.
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
  ## Read into recvBuf. ``AsyncTimeoutError``: caller handles state; other errors → ``csClosed`` + ``raiseTransportFailure``.
  # An orphaned pump can revive here after `invalidateOnTimeout` set csClosed;
  # refuse a socket read on a connection we've given up on.
  if conn.state == csClosed:
    conn.raiseClosedConnection("fillRecvBuf: connection is closed (csClosed)")
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
    except CancelledError as e:
      # csClosed as for any other failure: the read may have consumed bytes, so
      # the stream is no longer parseable. Only the exception type is preserved.
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      raise e
    except CatchableError as e:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      conn.raiseTransportFailure("fillRecvBuf", e)
    if n == 0:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      conn.raiseClosedConnection("Connection closed by server")
    # An orphan read settling after csClosed must not re-extend the buffer.
    if conn.state == csClosed:
      conn.recvBuf.setLen(oldLen)
      conn.raiseClosedConnection("fillRecvBuf: connection was closed during readOnce")
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
    except CancelledError as e:
      # csClosed as for any other failure: the read may have consumed bytes, so
      # the stream is no longer parseable. Only the exception type is preserved.
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      raise e
    except CatchableError as e:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      conn.raiseTransportFailure("fillRecvBuf", e)
    if n == 0:
      conn.recvBuf.setLen(oldLen)
      conn.state = csClosed
      conn.raiseClosedConnection("Connection closed by server")
    # An orphan `recvInto` settling after csClosed must not re-extend the buffer.
    if conn.state == csClosed:
      conn.recvBuf.setLen(oldLen)
      conn.raiseClosedConnection("fillRecvBuf: connection was closed during recvInto")
    conn.recvBuf.setLen(oldLen + n)

when hasChronos:
  proc fillRecvBufDetached*(conn: PgConnection): Future[void] {.async.} =
    ## Read into scratch then append to ``recvBuf`` (keeps ``recvBuf`` parseable while pending); errors → ``csClosed``.
    # Entrance guard, as in ``fillRecvBuf``: no fresh read on csClosed.
    if conn.state == csClosed:
      conn.raiseClosedConnection("fillRecvBufDetached: connection is closed (csClosed)")
    if conn.replReadScratch.len < RecvBufSize:
      conn.replReadScratch.setLen(RecvBufSize)
    let n =
      try:
        await conn.reader.readOnce(addr conn.replReadScratch[0], RecvBufSize)
      except CancelledError as e:
        conn.state = csClosed
        raise e
      except CatchableError as e:
        conn.state = csClosed
        conn.raiseTransportFailure("fillRecvBufDetached", e)
    if n == 0:
      conn.state = csClosed
      conn.raiseClosedConnection("Connection closed by server")
    # Exit guard: a read settling after the caller flipped csClosed must not
    # re-extend recvBuf.
    if conn.state == csClosed:
      conn.raiseClosedConnection(
        "fillRecvBufDetached: connection was closed during readOnce"
      )
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
  ## Parse next message from recvBuf (none = incomplete). Dispatches notify/notice,
  ## consumes ParameterStatus/DataRow (streaming via ``onRow``); ``skipDataRow`` avoids decode. Error → ``csClosed``.
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
  ## Receive one message (``nextMessage`` + ``fillRecvBuf``); timeout → ``csClosed``.
  while true:
    let opt = conn.nextMessage(rowData, rowCount)
    if opt.isSome:
      return opt.get
    try:
      await conn.fillRecvBuf(timeout)
    except AsyncTimeoutError as e:
      conn.state = csClosed
      raise e

template pumpUntilReady*(
    conn: PgConnection,
    resultData: untyped,
    rowCountPtr: untyped,
    body: untyped,
    readyBody: untyped,
) {.dirty.} =
  ## Pump until ``ReadyForQuery``; ``pumpMsg``/``queryError`` injected into ``body``/``readyBody``.
  # Spelled out per overload, not forwarded: ``{.dirty.}`` injection crosses only
  # one template boundary, and typed params ahead of the untyped bodies suppress
  # it, so neither forwarding nor defaulted params declare the names (Nim 2.2.x).
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
  ## Streaming pump (``onRow`` per row; first error in ``onRowErr``).
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
  ## Bare pump (``skipDataRow=true``; for callers that discard rows).
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

# Background read watch for COPY IN early-error detection.

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
  ## Abandon in-flight read (must raise/exit immediately after).
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
  ## Send raw bytes; failure → ``csClosed``.
  when hasChronos:
    try:
      await conn.writer.write(data)
    except CancelledError as e:
      conn.state = csClosed
      raise e
    except CatchableError as e:
      conn.state = csClosed
      conn.raiseTransportFailure("sendMsg", e)
  elif hasAsyncDispatch:
    if data.len > 0:
      try:
        await conn.socket.sendRawBytes(data)
      except CancelledError as e:
        conn.state = csClosed
        raise e
      except CatchableError as e:
        conn.state = csClosed
        conn.raiseTransportFailure("sendMsg", e)

proc sendBufMsg*(conn: PgConnection): Future[void] {.async.} =
  ## Send ``sendBuf`` (copied; safe to mutate after call); failure → ``csClosed``.
  when hasChronos:
    if conn.sendBuf.len > 0:
      try:
        await conn.writer.write(conn.sendBuf)
      except CancelledError as e:
        conn.state = csClosed
        raise e
      except CatchableError as e:
        conn.state = csClosed
        conn.raiseTransportFailure("sendBufMsg", e)
  elif hasAsyncDispatch:
    if conn.sendBuf.len > 0:
      try:
        await conn.socket.sendRawBytes(conn.sendBuf)
      except CancelledError as e:
        conn.state = csClosed
        raise e
      except CatchableError as e:
        conn.state = csClosed
        conn.raiseTransportFailure("sendBufMsg", e)

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
  ## POSIX half-open probe (``MSG_PEEK``): true if FIN/RST observed; false otherwise or unavailable.
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
  ## True if kernel has readable bytes (pre-TLS injection check; kernel buffer only).
  conn.peekSocket() == spData

proc isConnected*(conn: PgConnection): bool =
  ## Transport present and no kernel FIN/RST observed (cheap, non-blocking; use ``ping`` for full check).
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
