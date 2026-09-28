## Transport-layer message I/O.
##
## - sendBuf management and raw sends
## - Synchronous backend-message parsing (`nextMessage`) and the async wrapper
##   `recvMessage`
## - Background read watch for COPY IN early-error detection (`RecvWatch`)
## - Transport teardown (`closeTransport`)
## - TCP keepalive / TCP_NODELAY socket options
## - Host helpers (`isUnixSocket`, `unixSocketPath`, `getHosts`) and dialing
##   (`resolveTargets`, `dialTargets`, `dialServer`, `socketError`, `oneLine`)
## - `makeCopyOutCallback` / `makeCopyInCallback` cross-backend templates
##
## The receive buffer and its fills live in `types` with the private
## `recvBuf` / `recvBufStart` pair they move.
##
## The host helpers and `makeCopy*` templates are re-exported through
## `pg_connection.nim`; the message-parsing and send machinery stays here for
## sibling modules and tests. Depends only on `types.nim` and the
## protocol/error/backend abstraction modules.
##
## Internal module: not part of the public API. Import the `pg_connection` hub instead.

import std/[options, strutils]
when defined(posix):
  import std/posix

import ../[async_backend, pg_errors, pg_protocol]
import types

when hasChronos:
  import chronos/streams/tlsstream
elif hasAsyncDispatch:
  import std/asyncnet
  from std/nativesockets import
    Domain, SockType, Protocol, `==`, getAddrInfo, freeAddrInfo, toKnownDomain,
    getAddrString, getSockOptInt
  when defined(posix):
    from std/oserrors import OSErrorCode, newOSError, osLastError

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

# Dialing

const AsyncTracebackMarker = "\nAsync traceback:"
  ## Header asyncdispatch prepends to its injected traceback.

proc oneLine*(msg: string): string =
  ## Collapse `msg` to one line, dropping the asyncdispatch traceback.
  ## Cuts at the traceback marker so server DETAIL/HINT lines survive,
  ## joined with " | ".
  let cut = msg.find(AsyncTracebackMarker)
  let body =
    if cut >= 0:
      msg[0 ..< cut]
    else:
      msg
  var parts: seq[string]
  for line in body.splitLines():
    let stripped = line.strip()
    if stripped.len > 0:
      parts.add(stripped)
  parts.join(" | ")

type DialFailure = tuple[target: string, err: ref CatchableError]

when defined(posix):
  proc isTransientErrno(code: int32): bool {.raises: [].} =
    ## Whether an OS error may clear (``ENOENT``: a Unix socket not created yet).
    # Qualified: chronos exports same-named OSErrorCode constants. A proc, not
    # a func: macOS's posix errno constants are importc vars, not consts.
    code in [
      posix.ECONNREFUSED, posix.ECONNRESET, posix.ECONNABORTED, posix.ETIMEDOUT,
      posix.EHOSTUNREACH, posix.ENETUNREACH, posix.ENETDOWN, posix.EADDRNOTAVAIL,
      posix.EPIPE, posix.EAGAIN, posix.ENOENT, posix.EMFILE, posix.ENFILE,
      posix.ENOBUFS, posix.ENOMEM,
    ]

proc isTransientDial(e: ref CatchableError): bool {.raises: [].} =
  ## Whether a failed connect or send may succeed later.
  var code: int32
  when hasChronos:
    if e of TransportTooManyError: # EMFILE and the like, its code not kept
      return true
    if not (e of TransportOsError):
      return false
    code = int32((ref TransportOsError)(e).code)
  else:
    if not (e of OSError):
      return false
    code = (ref OSError)(e).errorCode
  when defined(posix):
    isTransientErrno(code)
  else:
    discard code # Windows' connect error codes are not mapped.
    true

proc dialError(failures: openArray[DialFailure]): ref PgConnectionError {.raises: [].} =
  ## One error for every address that failed, the last as ``parent``: a
  ## ``PgUnavailableError`` when any of them may clear.
  var msg = ""
  var transient = false
  for i, f in failures:
    transient = transient or isTransientDial(f.err)
    if failures.len == 1:
      msg = oneLine(f.err.msg)
    else:
      if i > 0:
        msg.add("; ")
      msg.add(f.target & ": " & oneLine(f.err.msg))
  if transient:
    (ref PgUnavailableError)(msg: msg, parent: failures[^1].err)
  else:
    (ref PgConnectionError)(msg: msg, parent: failures[^1].err)

proc unresolved(host: string): ref PgConnectionError =
  newException(PgUnavailableError, "Could not resolve host: " & host)

when defined(posix):
  proc lookup(host: string, port: int): ptr posix.AddrInfo =
    ## ``host``'s TCP addresses, to free with ``freeAddrInfo``. An unknown name
    ## is transient: it may be a container not registered yet.
    var hints: posix.AddrInfo
    hints.ai_family = posix.AF_UNSPEC
    hints.ai_socktype = posix.SOCK_STREAM
    hints.ai_protocol = posix.IPPROTO_TCP
    let rc = posix.getaddrinfo(cstring(host), cstring($port), addr hints, result)
    if rc != 0:
      let sysErr = posix.errno
      let reason =
        if rc == posix.EAI_SYSTEM:
          $posix.strerror(sysErr)
        else:
          $posix.gai_strerror(rc)
      let msg = "Could not resolve host " & host & ": " & reason
      if rc in [
        posix.EAI_FAIL, posix.EAI_FAMILY, posix.EAI_SOCKTYPE, posix.EAI_SERVICE,
        posix.EAI_BADFLAGS,
      ] or (rc == posix.EAI_SYSTEM and not isTransientErrno(sysErr)):
        raise newException(PgConnectionError, msg)
      raise newException(PgUnavailableError, msg)

when hasChronos:
  type
    DialStream* = StreamTransport
    Dialed* = tuple[stream: DialStream, target: DialTarget]
      ## A connected stream and the address it reached.

  func shown*(t: DialTarget): string =
    ## ``t`` as error messages name it.
    if t.family == AddressFamily.Unix:
      $t
    else:
      t.host

  proc resolveTargets*(host: string, port: int): seq[DialTarget] =
    ## What ``host`` names: its Unix socket, or each address it resolves to in
    ## the resolver's order.
    if isUnixSocket(host):
      when defined(posix):
        try:
          return @[initTAddress(unixSocketPath(host, port))]
        except TransportAddressError as e:
          raise (ref PgConnectionError)(msg: e.msg, parent: e)
      else:
        raise newException(
          PgConnectionError, "Unix sockets are not supported on this platform"
        )
    when defined(posix):
      let aiList = lookup(host, port)
      try:
        var it = aiList
        while it != nil:
          var ta: TransportAddress
          fromSAddr(cast[ptr Sockaddr_storage](it.ai_addr), SockLen(it.ai_addrlen), ta)
          if ta.family in {AddressFamily.IPv4, AddressFamily.IPv6} and ta notin result:
            result.add(ta)
          it = it.ai_next
      finally:
        posix.freeAddrInfo(aiList)
    else:
      try:
        result = resolveTAddress(host, Port(port))
      except TransportAddressError as e:
        # Its resolver code is lost here: judged as a name not known yet.
        raise (ref PgUnavailableError)(msg: e.msg, parent: e)
    if result.len == 0:
      raise unresolved(host)

  proc dialTargets*(targets: seq[DialTarget]): Future[Dialed] {.async.} =
    ## Connect to the first of ``targets`` that accepts, as libpq does.
    if targets.len == 0:
      raise newException(ValueError, "dialTargets: no address to dial")
    var failures: seq[DialFailure]
    for t in targets:
      try:
        return (stream: await connect(t), target: t)
      except TransportError as e:
        failures.add((t.shown, (ref CatchableError)(e)))
    raise dialError(failures)

elif hasAsyncDispatch:
  type
    DialStream* = AsyncSocket
    Dialed* = tuple[stream: DialStream, target: DialTarget]
      ## A connected stream and the address it reached.

  func shown*(t: DialTarget): string =
    ## ``t`` as error messages name it.
    if t.domain == Domain.AF_INET6:
      "[" & t.address & "]"
    else:
      t.address

  func `==`(a, b: DialTarget): bool =
    # Not `sa`: the text names the address, zone included.
    a.domain == b.domain and a.address == b.address and a.port == b.port

  proc resolveTargets*(host: string, port: int): seq[DialTarget] =
    ## What ``host`` names: its Unix socket, or each address it resolves to in
    ## the resolver's order.
    if isUnixSocket(host):
      when defined(posix):
        var t: DialTarget
        t.domain = Domain.AF_UNIX
        t.address = unixSocketPath(host, port)
        t.port = Port(port)
        return @[t]
      else:
        raise newException(
          PgConnectionError, "Unix sockets are not supported on this platform"
        )
    when defined(posix):
      let aiList = lookup(host, port)
      try:
        var it = aiList
        while it != nil:
          var t: DialTarget
          t.port = Port(port)
          if it.ai_family == posix.AF_INET:
            t.domain = Domain.AF_INET
            copyMem(addr t.sa, it.ai_addr, it.ai_addrlen)
            t.saLen = it.ai_addrlen
          elif it.ai_family == posix.AF_INET6:
            let sa6 = cast[ptr Sockaddr_in6](it.ai_addr)
            if posix.IN6_IS_ADDR_V4MAPPED(addr sa6.sin6_addr) != 0:
              # Dial the IPv4 it names: an AF_INET6 socket may be v6-only.
              var sa4: Sockaddr_in
              sa4.sin_family = typeof(sa4.sin_family)(posix.AF_INET)
              sa4.sin_port = sa6.sin6_port
              copyMem(addr sa4.sin_addr, addr sa6.sin6_addr.s6_addr[12], 4)
              t.domain = Domain.AF_INET
              copyMem(addr t.sa, addr sa4, sizeof(sa4))
              t.saLen = SockLen(sizeof(sa4))
            else:
              t.domain = Domain.AF_INET6
              copyMem(addr t.sa, it.ai_addr, it.ai_addrlen)
              t.saLen = it.ai_addrlen
          else:
            it = it.ai_next
            continue
          # nim doc builds nativesockets on winlean: name its types.
          t.address = getAddrString(cast[ptr nativesockets.SockAddr](addr t.sa))
          if t.domain == Domain.AF_INET6:
            # The text form drops a link-local address's zone; keep it.
            let scope = cast[ptr Sockaddr_in6](addr t.sa).sin6_scope_id
            if scope != 0:
              t.address.add("%" & $scope)
          if t notin result:
            result.add(t)
          it = it.ai_next
      finally:
        posix.freeAddrInfo(aiList)
    else:
      # Resolved here rather than by std `dial`: its lookup failure carries a
      # stale errno, so only this split tells it from a refused connect.
      let aiList =
        try:
          getAddrInfo(host, Port(port), Domain.AF_UNSPEC)
        except OSError as e:
          const WSANO_RECOVERY = 11003 # as EAI_FAIL
          if e.errorCode == WSANO_RECOVERY:
            raise (ref PgConnectionError)(msg: e.msg, parent: e)
          raise (ref PgUnavailableError)(msg: e.msg, parent: e)
      try:
        var it = aiList
        while it != nil:
          let known = toKnownDomain(it.ai_family)
          if known.isSome and known.get in {Domain.AF_INET, Domain.AF_INET6}:
            var domain = known.get
            let ip = getAddrString(it.ai_addr)
            if domain == Domain.AF_INET6 and ':' notin ip:
              # getAddrString unmaps ::ffff:a.b.c.d; dial the IPv4 it names.
              domain = Domain.AF_INET
            let t: DialTarget = (domain: domain, address: ip, port: Port(port))
            if t notin result:
              result.add(t)
          it = it.ai_next
      finally:
        freeAddrInfo(aiList)
    if result.len == 0:
      raise unresolved(host)

  when defined(posix):
    proc connectResolved(sock: AsyncSocket, t: DialTarget): Future[void] =
      ## Connect ``sock`` to ``t``'s resolved address, with no second lookup.
      let fut = newFuture[void]("connectResolved")
      result = fut

      proc onWritable(fd: AsyncFD): bool =
        let err =
          nativesockets.SocketHandle(fd).getSockOptInt(cint(SOL_SOCKET), cint(SO_ERROR))
        if err == 0:
          fut.complete()
        elif err == EINTR:
          return false
        else:
          fut.fail(newOSError(OSErrorCode(err)))
        true

      var sa = t.sa
      if posix.connect(
        posix.SocketHandle(sock.getFd), cast[ptr SockAddr](addr sa), t.saLen
      ) == 0:
        fut.complete()
      else:
        let err = osLastError()
        if err.int32 in [EINTR, EINPROGRESS]:
          addWrite(AsyncFD(sock.getFd), onWritable)
        else:
          fut.fail(newOSError(err))

  proc dialTargets*(targets: seq[DialTarget]): Future[Dialed] {.async.} =
    ## Connect to the first of ``targets`` that accepts, as libpq does.
    if targets.len == 0:
      raise newException(ValueError, "dialTargets: no address to dial")
    var failures: seq[DialFailure]
    for t in targets:
      var sock: AsyncSocket
      try:
        if t.domain == Domain.AF_UNIX:
          when defined(posix):
            sock = newAsyncSocket(
              Domain.AF_UNIX,
              SockType.SOCK_STREAM,
              Protocol.IPPROTO_IP,
              buffered = false,
            )
            await sock.connectUnix(t.address)
          else:
            raiseAssert "resolveTargets yields AF_UNIX on POSIX only"
        else:
          sock = newAsyncSocket(
            t.domain, SockType.SOCK_STREAM, Protocol.IPPROTO_TCP, buffered = false
          )
          when defined(posix):
            await sock.connectResolved(t)
          else:
            await sock.connect(t.address, t.port)
        return (stream: sock, target: t)
      except CatchableError as e:
        if sock != nil:
          sock.close()
        if not (e of OSError):
          raise e
        failures.add((t.shown, e))
    raise dialError(failures)

proc dialServer*(host: string, port: int): Future[Dialed] =
  ## Connect to the first address ``host`` resolves to that accepts.
  # No future of its own: each extra layer delays a pump's cancellation by a
  # tick, which chronos (4.4) may run only at the next I/O or timer event.
  dialTargets(resolveTargets(host, port))

proc socketError*(e: ref CatchableError): ref PgConnectionError =
  ## A failed connect or send, classified as a dial is.
  dialError([("", e)])

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

# Notification / notice dispatch lives in ``types`` with the queue fields.

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
#
# ``compactRecvBuf`` / ``fillRecvBuf`` / ``fillRecvBufDetached`` live in
# ``types`` with the private fields they move as a pair.

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
  ##
  ## ``onRow`` requires ``onRowError``: callback failures are deferred into that
  ## slot so the pump can drain to ReadyForQuery. Without a slot the first
  ## DataRow would dereference nil, so the missing slot is rejected up front
  ## with ``PgProtocolError`` (the connection is left open — unlike wire
  ## corruption, this is a caller bug, not a broken peer).
  if onRow != nil and onRowError == nil:
    raise newException(PgProtocolError, "nextMessage: onRow requires onRowError")
  let maxLen = conn.effectiveMaxMessageSize()
  while true:
    var consumed: int
    let res =
      try:
        parseBackendMessage(
          conn.recvBuf.toOpenArray(conn.recvBufStart, conn.recvBuf.len - 1),
          consumed,
          rowData,
          maxLen,
          skipDataRow = skipDataRow and rowData == nil and onRow == nil,
        )
      except PgProtocolError as e:
        conn.markClosed()
        raise e
    if res.state == psIncomplete:
      return none(BackendMessage)
    conn.consumeRecv(consumed)
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
      # Distinct-key and total-byte caps reject hostile flooding (fail-closed).
      let m = res.message
      conn.recordParameterStatus(m.paramName, m.paramValue)
      continue
    if res.message.kind == bmkNegotiateProtocolVersion:
      # Informational per libpq; record and drop so callers never see it.
      let m = res.message
      conn.noteNegotiatedProtocol(m.newestMinorVersion, m.unrecognizedOptions)
      continue
    if res.message.kind == bmkDataRow and rowCount != nil:
      rowCount[] += 1
      continue
    if res.message.kind == bmkErrorResponse and
        isSessionFatal(errorSeverity(res.message.errorFields)):
      conn.fatalServerError = newPgQueryError(res.message.errorFields)
    if res.message.kind == bmkReadyForQuery:
      # The session answered after all, so that FATAL did not end it (a
      # proxy's; the server closes after its own).
      conn.fatalServerError = nil
      # `unsyncedWrite` is untouched — this reply belongs to a sync point that
      # preceded those writes, so only a later one (in `noteWrite`) can end them.
      conn.settlePendingSync()
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
      # Load-bearing: on asyncdispatch the timed-out read stays orphaned in
      # `recvBuf`, and csClosed is what stops a later read re-extending it.
      conn.markClosed()
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
            conn.markReady()
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
            conn.markReady()
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
            conn.markReady()
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
  ## Send raw bytes; failure → ``csClosed``. Books what they leave owed first;
  ## see ``noteWrite``.
  conn.noteWrite(data)
  when hasChronos:
    try:
      await conn.writer.write(data)
    except CancelledError as e:
      conn.markClosed()
      raise e
    except CatchableError as e:
      conn.markClosed()
      conn.raiseTransportFailure("sendMsg", e)
  elif hasAsyncDispatch:
    if data.len > 0:
      try:
        await conn.socket.sendRawBytes(data)
      except CancelledError as e:
        conn.markClosed()
        raise e
      except CatchableError as e:
        conn.markClosed()
        conn.raiseTransportFailure("sendMsg", e)

proc sendBufMsg*(conn: PgConnection): Future[void] {.async.} =
  ## Send ``sendBuf`` (copied; safe to mutate after call); failure → ``csClosed``.
  ## Books what the buffer leaves owed; see ``sendMsg``.
  conn.noteWrite(conn.sendBuf)
  when hasChronos:
    if conn.sendBuf.len > 0:
      try:
        await conn.writer.write(conn.sendBuf)
      except CancelledError as e:
        conn.markClosed()
        raise e
      except CatchableError as e:
        conn.markClosed()
        conn.raiseTransportFailure("sendBufMsg", e)
  elif hasAsyncDispatch:
    if conn.sendBuf.len > 0:
      try:
        await conn.socket.sendRawBytes(conn.sendBuf)
      except CancelledError as e:
        conn.markClosed()
        raise e
      except CatchableError as e:
        conn.markClosed()
        conn.raiseTransportFailure("sendBufMsg", e)

proc sendStagedBufMsg*(conn: PgConnection) {.async.} =
  ## `sendBufMsg` paired with `stagePendingStmtCloses`: drop the staged
  ## statement Closes only once the buffer is on the wire.
  await conn.sendBufMsg()
  conn.dropStagedStmtCloses()

proc sendStagedMsg*(conn: PgConnection, data: seq[byte]) {.async.} =
  ## `sendMsg` counterpart, for builds that assemble their own buffer.
  await conn.sendMsg(data)
  conn.dropStagedStmtCloses()

# Transport teardown

proc closeTransportImpl(conn: PgConnection) {.async.} =
  ## One teardown pass. Entered once per connection at a time; see
  ## `closeTransport`.
  when hasChronos:
    # Every handle is detached before the first suspension: layer-by-layer
    # detaching let a racing teardown close the base transport under this
    # frame's still-running TLS close. `reader`/`writer` go with them, or
    # `isConnected()` reports healthy while `peekSocket` sees no transport.
    let detached = conn.detachTransport()
    let tls = detached.tls
    let baseReader = detached.baseReader
    let baseWriter = detached.baseWriter
    let transport = detached.transport
    if tls != nil:
      try:
        await tls.reader.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsTlsReader, e)
      try:
        await tls.writer.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsTlsWriter, e)
    if baseReader != nil:
      try:
        await baseReader.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsBaseReader, e)
    if baseWriter != nil:
      try:
        await baseWriter.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsBaseWriter, e)
    if transport != nil:
      try:
        await transport.closeWait()
      except CatchableError as e:
        conn.fireTransportCloseError(tcsTransport, e)
  elif hasAsyncDispatch:
    let socket = conn.detachTransport()
    if not socket.isNil:
      socket.close()

proc closeTransport*(conn: PgConnection) {.async.} =
  ## Close transport resources without sending Terminate.
  ##
  ## Re-entrant: a racing second teardown awaits the first rather than
  ## returning early on the already-detached handles, so a resolved `close()`
  ## still means the fd and the backend session are released.
  let inFlight = conn.transportCloseFut
  if inFlight != nil and not inFlight.finished:
    # `noCancel` and swallowed: this caller neither started nor cancelled the
    # teardown, so it must not inherit its outcome.
    try:
      when hasChronos:
        await noCancel inFlight
      else:
        await inFlight
    except CatchableError:
      discard
    return
  let fut = conn.closeTransportImpl()
  conn.transportCloseFut = fut
  # `noCancel` on the owner too: the handles are detached before the first
  # suspension, so a cancelled teardown would strand the only references to
  # them and leak the fd for the process lifetime.
  when hasChronos:
    await noCancel fut
  else:
    await fut
  # Released once finished: the guard above only cares about a running
  # teardown, and a connection that reconnects in place must not carry the
  # finished frame of the transport before last.
  if conn.transportCloseFut == fut:
    conn.transportCloseFut = nil

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
