import std/[tables, sets, strutils, uri, deques]

import async_backend

when hasChronos:
  import chronos/streams/tlsstream
  import bearssl/[x509, rsa, ec]
elif hasAsyncDispatch:
  import std/asyncnet
  when defined(ssl):
    import std/[net, tempfiles, os]

import pg_protocol, pg_auth, pg_types

type
  PgError* = object of CatchableError

  PgConnState* = enum
    csConnecting
    csAuthentication
    csReady
    csBusy
    csListening
    csClosed

  SslMode* = enum
    sslDisable ## Disable SSL (default)
    sslPrefer ## Try SSL; fall back to plaintext if refused
    sslRequire ## Require SSL (no certificate verification)
    sslVerifyCa ## Require SSL + verify CA chain (no hostname verification)
    sslVerifyFull ## Require SSL + verify CA chain and hostname

  ConnConfig* = object
    host*: string
    port*: int # default 5432
    user*: string
    password*: string
    database*: string
    sslMode*: SslMode
    sslRootCert*: string ## PEM-encoded CA certificate(s) for sslVerifyCa/sslVerifyFull
    applicationName*: string
    connectTimeout*: Duration ## TCP connect timeout (default: no timeout)
    extraParams*: seq[(string, string)] ## Additional startup parameters

  Notification* = object
    pid*: int32
    channel*: string
    payload*: string

  NotifyCallback* = proc(notification: Notification) {.gcsafe, raises: [].}

  Notice* = object
    fields*: seq[ErrorField]

  NoticeCallback* = proc(notice: Notice) {.gcsafe, raises: [].}

  PgConnection* = ref object
    when hasChronos:
      transport*: StreamTransport
      baseReader: AsyncStreamReader
      baseWriter: AsyncStreamWriter
      reader: AsyncStreamReader
      writer: AsyncStreamWriter
      tlsStream: TLSAsyncStream
      trustAnchorBufs: seq[seq[byte]] ## Backing memory for custom trust anchor pointers
    elif hasAsyncDispatch:
      socket*: AsyncSocket
    sslEnabled*: bool
    recvBuf*: seq[byte]
    state*: PgConnState
    pid*: int32
    secretKey*: int32
    serverParams*: Table[string, string]
    txStatus*: TransactionStatus
    notifyCallback*: NotifyCallback
    noticeCallback*: NoticeCallback
    listenChannels*: HashSet[string]
    listenTask*: Future[void]
    host: string
    port: int
    createdAt*: Moment
    portalCounter*: int
    config*: ConnConfig
    notifyQueue*: Deque[Notification]
    notifyMaxQueue*: int
    notifyWaiter: Future[void]
    reconnectCallback*: proc() {.gcsafe, raises: [].}

  QueryResult* = object
    fields*: seq[FieldDescription]
    rows*: seq[Row]
    commandTag*: string

  CopyResult* = object
    format*: CopyFormat
    columnFormats*: seq[int16]
    data*: seq[seq[byte]]
    commandTag*: string

  CopyOutInfo* = object
    format*: CopyFormat
    columnFormats*: seq[int16]
    commandTag*: string

  CopyInInfo* = object
    format*: CopyFormat
    columnFormats*: seq[int16]
    commandTag*: string

when hasChronos:
  type CopyOutCallback* =
    proc(data: seq[byte]): Future[void] {.async: (raises: [CatchableError]), gcsafe.}

  type CopyInCallback* =
    proc(): Future[seq[byte]] {.async: (raises: [CatchableError]), gcsafe.}

  type TrustAnchorResult = object
    store: TrustAnchorStore
    backing: seq[seq[byte]] ## Owns memory pointed to by trust anchor fields

else:
  type CopyOutCallback* = proc(data: seq[byte]): Future[void] {.gcsafe.}

  type CopyInCallback* = proc(): Future[seq[byte]] {.gcsafe.}

const RecvBufSize = 32768 ## Size of the temporary read buffer for recv operations

proc dispatchNotification(conn: PgConnection, msg: BackendMessage) =
  let notif = Notification(
    pid: msg.notifPid, channel: msg.notifChannel, payload: msg.notifPayload
  )
  if conn.notifyMaxQueue > 0:
    while conn.notifyQueue.len >= conn.notifyMaxQueue:
      discard conn.notifyQueue.popFirst()
    conn.notifyQueue.addLast(notif)
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    conn.notifyWaiter.complete()
  if conn.notifyCallback != nil:
    conn.notifyCallback(notif)

proc dispatchNotice(conn: PgConnection, msg: BackendMessage) =
  if conn.noticeCallback != nil:
    conn.noticeCallback(Notice(fields: msg.noticeFields))

when hasAsyncDispatch:
  template bytesToString(data: seq[byte]): string =
    let d = data
    var s = newString(d.len)
    if d.len > 0:
      copyMem(addr s[0], unsafeAddr d[0], d.len)
    s

  proc sendRawBytes(socket: AsyncSocket, data: seq[byte]): Future[void] =
    ## Send seq[byte] via asyncdispatch socket.
    if data.len == 0:
      var fut = newFuture[void]("sendRawBytes")
      fut.complete()
      return fut
    socket.send(bytesToString(data))

proc compactRecvBuf(conn: PgConnection, consumed: int) {.inline.} =
  ## Remove consumed bytes from the front of recvBuf efficiently.
  ## Shifts remaining data to the front instead of allocating a new seq.
  if consumed == 0:
    return
  let remaining = conn.recvBuf.len - consumed
  if remaining == 0:
    conn.recvBuf.setLen(0)
  else:
    moveMem(addr conn.recvBuf[0], addr conn.recvBuf[consumed], remaining)
    conn.recvBuf.setLen(remaining)

proc recvMessage*(
    conn: PgConnection, timeout = ZeroDuration
): Future[BackendMessage] {.async.} =
  ## Receive a single backend message from the connection.
  ## If `timeout` is non-zero, each read operation is bounded by the timeout,
  ## raising AsyncTimeoutError if no data arrives within the specified duration.
  var totalConsumed = 0
  while true:
    var consumed: int
    let res = parseBackendMessage(
      conn.recvBuf.toOpenArray(totalConsumed, conn.recvBuf.len - 1), consumed
    )
    if res.state == psComplete:
      totalConsumed += consumed
      if res.message.kind == bmkNotificationResponse:
        conn.dispatchNotification(res.message)
        continue
      if res.message.kind == bmkNoticeResponse:
        conn.dispatchNotice(res.message)
        continue
      conn.compactRecvBuf(totalConsumed)
      return res.message
    # Compact before reading more data to maximize contiguous space
    conn.compactRecvBuf(totalConsumed)
    totalConsumed = 0
    when hasChronos:
      let oldLen = conn.recvBuf.len
      conn.recvBuf.setLen(oldLen + RecvBufSize)
      try:
        let n =
          if timeout == ZeroDuration:
            await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize)
          else:
            await conn.reader.readOnce(addr conn.recvBuf[oldLen], RecvBufSize).wait(
              timeout
            )
        if n == 0:
          conn.state = csClosed
          raise newException(PgError, "Connection closed by server")
        conn.recvBuf.setLen(oldLen + n)
      except AsyncTimeoutError as e:
        conn.recvBuf.setLen(oldLen)
        raise e
    elif hasAsyncDispatch:
      let data =
        if timeout == ZeroDuration:
          await conn.socket.recv(RecvBufSize)
        else:
          await conn.socket.recv(RecvBufSize).wait(timeout)
      if data.len == 0:
        conn.state = csClosed
        raise newException(PgError, "Connection closed by server")
      let oldLen = conn.recvBuf.len
      conn.recvBuf.setLen(oldLen + data.len)
      copyMem(addr conn.recvBuf[oldLen], unsafeAddr data[0], data.len)

proc sendMsg*(conn: PgConnection, data: seq[byte]): Future[void] {.async.} =
  when hasChronos:
    await conn.writer.write(data)
  elif hasAsyncDispatch:
    if data.len > 0:
      await conn.socket.sendRawBytes(data)

when hasChronos:
  proc appendDnCallback(
      ctx: pointer, buf: pointer, len: uint
  ) {.exportc: "pg_append_dn_nim", cdecl, gcsafe, noSideEffect, raises: [].} =
    let s = cast[ptr seq[byte]](ctx)
    let p = cast[ptr UncheckedArray[byte]](buf)
    for i in 0 ..< int(len):
      s[].add(p[i])

  # C shim with const void* to satisfy BearSSL's br_x509_decoder_init signature
  {.
    emit: """
  static void pg_append_dn_shim(void *ctx, const void *buf, size_t len) {
    pg_append_dn_nim(ctx, (void*)buf, len);
  }
  """
  .}

  proc initX509Decoder(ctx: var X509DecoderContext, appendDnCtx: pointer) =
    {.
      emit: ["br_x509_decoder_init(&", ctx, ", pg_append_dn_shim, ", appendDnCtx, ");"]
    .}

  proc parseTrustAnchors(pemData: string): TrustAnchorResult =
    ## Parse PEM-encoded CA certificates into a TrustAnchorStore.
    ## Returns both the store and the backing memory that anchor pointers reference.
    ##
    ## IMPORTANT: X509TrustAnchor contains raw `ptr byte` fields (dn.data,
    ## pkey.key.rsa.n/e, pkey.key.ec.q). TrustAnchorStore.new() only shallow-copies
    ## these structs, and BearSSL only stores a pointer to the anchor array.
    ## The caller MUST keep `result.backing` alive for the lifetime of the TLS session.
    let items = pemDecode(pemData)
    var anchors: seq[X509TrustAnchor]
    var backing: seq[seq[byte]]

    for item in items:
      if item.name != "CERTIFICATE":
        continue

      var dnBuf: seq[byte]
      var decoder: X509DecoderContext
      initX509Decoder(decoder, addr dnBuf)
      x509DecoderPush(decoder, unsafeAddr item.data[0], uint(item.data.len))

      if x509DecoderLastError(decoder) != 0:
        continue

      let pkey = x509DecoderGetPkey(decoder)
      if pkey.isNil:
        continue

      # Deep-copy DN
      backing.add(dnBuf)
      let dnData = addr backing[^1][0]

      # Deep-copy public key and build anchor
      var anchor: X509TrustAnchor
      anchor.dn = X500Name(data: dnData, len: uint(dnBuf.len))
      anchor.flags =
        if x509DecoderIsCA(decoder) != 0:
          cuint(X509_TA_CA)
        else:
          0
      anchor.pkey.keyType = pkey.keyType

      if pkey.keyType == byte(KEYTYPE_RSA):
        var nBuf = newSeq[byte](pkey.key.rsa.nlen)
        copyMem(addr nBuf[0], pkey.key.rsa.n, nBuf.len)
        backing.add(nBuf)
        var eBuf = newSeq[byte](pkey.key.rsa.elen)
        copyMem(addr eBuf[0], pkey.key.rsa.e, eBuf.len)
        backing.add(eBuf)
        anchor.pkey.key.rsa = RsaPublicKey(
          n: addr backing[^2][0],
          nlen: uint(nBuf.len),
          e: addr backing[^1][0],
          elen: uint(eBuf.len),
        )
      elif pkey.keyType == byte(KEYTYPE_EC):
        var qBuf = newSeq[byte](pkey.key.ec.qlen)
        copyMem(addr qBuf[0], pkey.key.ec.q, qBuf.len)
        backing.add(qBuf)
        anchor.pkey.key.ec = EcPublicKey(
          curve: pkey.key.ec.curve, q: addr backing[^1][0], qlen: uint(qBuf.len)
        )
      else:
        continue

      anchors.add(anchor)

    if anchors.len == 0:
      raise newException(PgError, "No valid CA certificates found in PEM data")

    result = TrustAnchorResult(store: TrustAnchorStore.new(anchors), backing: backing)

proc negotiateSSL(conn: PgConnection, config: ConnConfig) {.async.} =
  ## Send SSLRequest and negotiate TLS if server accepts.
  let sslReq = encodeSSLRequest()
  var respChar: char

  when hasChronos:
    discard await conn.transport.write(sslReq)
    var response: array[1, byte]
    let n = await conn.transport.readOnce(addr response[0], 1)
    if n == 0:
      raise newException(PgError, "Connection closed during SSL negotiation")
    respChar = char(response[0])
  elif hasAsyncDispatch:
    await conn.socket.sendRawBytes(sslReq)
    let respStr = await conn.socket.recv(1)
    if respStr.len == 0:
      raise newException(PgError, "Connection closed during SSL negotiation")
    respChar = respStr[0]

  case respChar
  of 'S':
    when hasChronos:
      conn.baseReader = newAsyncStreamReader(conn.transport)
      conn.baseWriter = newAsyncStreamWriter(conn.transport)

      let flags =
        case config.sslMode
        of sslVerifyFull:
          {}
        of sslVerifyCa:
          {TLSFlags.NoVerifyServerName}
        else:
          {TLSFlags.NoVerifyHost, TLSFlags.NoVerifyServerName}

      let serverName = if config.sslMode == sslVerifyFull: config.host else: ""

      if config.sslRootCert.len > 0:
        let parsed = parseTrustAnchors(config.sslRootCert)
        conn.trustAnchorBufs = parsed.backing
          # Must outlive TLS session (see parseTrustAnchors doc)
        conn.tlsStream = newTLSClientAsyncStream(
          conn.baseReader,
          conn.baseWriter,
          serverName,
          flags = flags,
          minVersion = TLSVersion.TLS12,
          maxVersion = TLSVersion.TLS12,
          trustAnchors = parsed.store,
        )
      else:
        conn.tlsStream = newTLSClientAsyncStream(
          conn.baseReader,
          conn.baseWriter,
          serverName,
          flags = flags,
          minVersion = TLSVersion.TLS12,
          maxVersion = TLSVersion.TLS12,
        )
      await conn.tlsStream.handshake()
      conn.reader = conn.tlsStream.reader
      conn.writer = conn.tlsStream.writer
      conn.sslEnabled = true
    elif hasAsyncDispatch:
      when defined(ssl):
        let verifyMode =
          case config.sslMode
          of sslVerifyCa, sslVerifyFull: SslCVerifyMode.CVerifyPeer
          else: SslCVerifyMode.CVerifyNone

        var ctx: SslContext
        if config.sslRootCert.len > 0:
          let (tmpFile, tmpPath) = createTempFile("pg_ca_", ".pem")
          try:
            tmpFile.write(config.sslRootCert)
            tmpFile.close()
            ctx = newContext(verifyMode = verifyMode, caFile = tmpPath)
          finally:
            removeFile(tmpPath)
        else:
          ctx = newContext(verifyMode = verifyMode)

        let hostname = if config.sslMode == sslVerifyFull: config.host else: ""
        wrapConnectedSocket(ctx, conn.socket, handshakeAsClient, hostname)
        conn.sslEnabled = true
      else:
        raise newException(PgError, "SSL support requires compiling with -d:ssl")
  of 'N':
    if config.sslMode in {sslRequire, sslVerifyCa, sslVerifyFull}:
      raise newException(PgError, "Server does not support SSL")
    # sslPrefer: server refused SSL – connection will proceed unencrypted.
    # WARNING: This is vulnerable to MITM downgrade attacks. A network
    # attacker can intercept the SSLRequest and reply 'N' to force
    # plaintext. Use sslRequire or stronger if security is needed.
    stderr.writeLine "pg_connection: SSL refused by server, falling back to plaintext (sslmode=prefer)"
  else:
    raise newException(PgError, "Unexpected SSL response: " & $respChar)

proc connect*(config: ConnConfig): Future[PgConnection] =
  proc perform(): Future[PgConnection] {.async.} =
    var conn: PgConnection

    when hasChronos:
      let addresses = resolveTAddress(config.host, Port(config.port))
      if addresses.len == 0:
        raise newException(PgError, "Could not resolve host: " & config.host)
      let transport = await connect(addresses[0])
      conn = PgConnection(
        transport: transport,
        recvBuf: @[],
        state: csConnecting,
        serverParams: initTable[string, string](),
        host: config.host,
        port: config.port,
        config: config,
        notifyMaxQueue: 1024,
      )
    elif hasAsyncDispatch:
      let sock = newAsyncSocket(buffered = false)
      try:
        await sock.connect(config.host, Port(config.port))
      except CatchableError:
        sock.close()
        raise
      conn = PgConnection(
        socket: sock,
        recvBuf: @[],
        state: csConnecting,
        serverParams: initTable[string, string](),
        host: config.host,
        port: config.port,
        config: config,
        notifyMaxQueue: 1024,
      )

    try:
      # SSL negotiation (before StartupMessage)
      if config.sslMode != sslDisable:
        await negotiateSSL(conn, config)

      when hasChronos:
        # If SSL was not established, create plain streams
        if conn.reader.isNil:
          conn.baseReader = newAsyncStreamReader(conn.transport)
          conn.baseWriter = newAsyncStreamWriter(conn.transport)
          conn.reader = conn.baseReader
          conn.writer = conn.baseWriter

      # Send StartupMessage
      var startupParams = config.extraParams
      if config.applicationName.len > 0:
        startupParams.add(("application_name", config.applicationName))
      await conn.sendMsg(encodeStartup(config.user, config.database, startupParams))
      conn.state = csAuthentication

      # Authentication loop
      var scramState: ScramState
      while true:
        let msg = await conn.recvMessage()
        case msg.kind
        of bmkAuthenticationOk:
          break
        of bmkAuthenticationCleartextPassword:
          await conn.sendMsg(encodePassword(config.password))
        of bmkAuthenticationMD5Password:
          let hash = md5AuthHash(config.user, config.password, msg.md5Salt)
          await conn.sendMsg(encodePassword(hash))
        of bmkAuthenticationSASL:
          if "SCRAM-SHA-256" notin msg.saslMechanisms:
            raise newException(PgError, "Server doesn't support SCRAM-SHA-256")
          let clientFirst = scramClientFirstMessage(config.user, scramState)
          await conn.sendMsg(encodeSASLInitialResponse("SCRAM-SHA-256", clientFirst))
        of bmkAuthenticationSASLContinue:
          let clientFinal =
            scramClientFinalMessage(config.password, msg.saslData, scramState)
          await conn.sendMsg(encodeSASLResponse(clientFinal))
        of bmkAuthenticationSASLFinal:
          if not scramVerifyServerFinal(msg.saslFinalData, scramState):
            raise newException(PgError, "SCRAM server signature verification failed")
        of bmkErrorResponse:
          raise newException(PgError, formatError(msg.errorFields))
        else:
          discard

      # Collect ParameterStatus, BackendKeyData until ReadyForQuery
      while true:
        let msg = await conn.recvMessage()
        case msg.kind
        of bmkParameterStatus:
          conn.serverParams[msg.paramName] = msg.paramValue
        of bmkBackendKeyData:
          conn.pid = msg.backendPid
          conn.secretKey = msg.backendSecretKey
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          break
        of bmkErrorResponse:
          raise newException(PgError, formatError(msg.errorFields))
        else:
          discard

      conn.createdAt = Moment.now()
      return conn
    except CatchableError as e:
      when hasChronos:
        if conn.tlsStream != nil:
          try:
            await conn.tlsStream.reader.closeWait()
          except CatchableError:
            discard
          try:
            await conn.tlsStream.writer.closeWait()
          except CatchableError:
            discard
        if conn.baseReader != nil:
          try:
            await conn.baseReader.closeWait()
          except CatchableError:
            discard
          try:
            await conn.baseWriter.closeWait()
          except CatchableError:
            discard
        if conn.transport != nil:
          try:
            await conn.transport.closeWait()
          except CatchableError:
            discard
      elif hasAsyncDispatch:
        if not conn.socket.isNil:
          conn.socket.close()
      raise e

  if config.connectTimeout != default(Duration):
    perform().wait(config.connectTimeout)
  else:
    perform()

proc checkReady*(conn: PgConnection) =
  if conn.state != csReady:
    raise newException(PgError, "Connection is not ready (state: " & $conn.state & ")")

proc quoteIdentifier*(s: string): string =
  "\"" & s.replace("\"", "\"\"") & "\""

proc simpleQuery*(conn: PgConnection, sql: string): Future[seq[QueryResult]] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))

  var results: seq[QueryResult]
  var current = QueryResult()
  var errorMsg = ""

  while true:
    let msg = await conn.recvMessage()
    case msg.kind
    of bmkRowDescription:
      current = QueryResult(fields: msg.fields)
    of bmkDataRow:
      current.rows.add(msg.columns)
    of bmkCommandComplete:
      current.commandTag = msg.commandTag
      results.add(current)
      current = QueryResult()
    of bmkEmptyQueryResponse:
      results.add(QueryResult())
    of bmkErrorResponse:
      errorMsg = formatError(msg.errorFields)
    of bmkReadyForQuery:
      conn.txStatus = msg.txStatus
      conn.state = csReady
      if errorMsg.len > 0:
        raise newException(PgError, errorMsg)
      break
    else:
      discard

  return results

proc isConnected(conn: PgConnection): bool =
  when hasChronos:
    not conn.writer.isNil
  elif hasAsyncDispatch:
    not conn.socket.isNil

proc ping*(conn: PgConnection, timeout = ZeroDuration): Future[void] =
  ## Lightweight health check using an empty simple query.
  ## Sends Query("") -> expects EmptyQueryResponse + ReadyForQuery.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  proc perform(): Future[void] {.async.} =
    conn.checkReady()
    if not conn.isConnected():
      conn.state = csClosed
      raise newException(PgError, "Connection is not established")
    conn.state = csBusy
    await conn.sendMsg(encodeQuery(""))

    var errorMsg = ""
    while true:
      let msg = await conn.recvMessage()
      case msg.kind
      of bmkEmptyQueryResponse:
        discard
      of bmkErrorResponse:
        errorMsg = formatError(msg.errorFields)
      of bmkReadyForQuery:
        conn.txStatus = msg.txStatus
        conn.state = csReady
        if errorMsg.len > 0:
          raise newException(PgError, errorMsg)
        break
      else:
        discard

  if timeout > ZeroDuration:
    proc withTimeout(): Future[void] {.async.} =
      try:
        await perform().wait(timeout)
      except AsyncTimeoutError:
        conn.state = csClosed
        raise newException(PgError, "Ping timed out")

    withTimeout()
  else:
    perform()

proc cancel*(conn: PgConnection): Future[void] {.async.} =
  ## Send a CancelRequest over a separate TCP connection to abort the running query.
  when hasChronos:
    let addresses = resolveTAddress(conn.host, Port(conn.port))
    if addresses.len == 0:
      raise newException(PgError, "Could not resolve host: " & conn.host)
    let transport = await connect(addresses[0])
    try:
      let msg = encodeCancelRequest(conn.pid, conn.secretKey)
      discard await transport.write(msg)
    finally:
      await transport.closeWait()
  elif hasAsyncDispatch:
    let sock = newAsyncSocket(buffered = false)
    try:
      await sock.connect(conn.host, Port(conn.port))
      let msg = encodeCancelRequest(conn.pid, conn.secretKey)
      await sock.sendRawBytes(msg)
    finally:
      sock.close()

proc closeTransport(conn: PgConnection) {.async.} =
  ## Close transport resources without sending Terminate.
  when hasChronos:
    if conn.tlsStream != nil:
      try:
        await conn.tlsStream.reader.closeWait()
      except CatchableError:
        discard
      try:
        await conn.tlsStream.writer.closeWait()
      except CatchableError:
        discard
      conn.tlsStream = nil
    if conn.baseReader != nil:
      try:
        await conn.baseReader.closeWait()
      except CatchableError:
        discard
      try:
        await conn.baseWriter.closeWait()
      except CatchableError:
        discard
      conn.baseReader = nil
      conn.baseWriter = nil
    if conn.transport != nil:
      try:
        await conn.transport.closeWait()
      except CatchableError:
        discard
      conn.transport = nil
  elif hasAsyncDispatch:
    if not conn.socket.isNil:
      conn.socket.close()
      conn.socket = nil

proc close*(conn: PgConnection): Future[void] {.async.} =
  ## Close the connection. Idempotent: safe to call multiple times.
  # Stop background listen pump if running
  if conn.listenTask != nil and not conn.listenTask.finished:
    await cancelAndWait(conn.listenTask)
  conn.listenTask = nil
  # Only send Terminate if we haven't already detected the connection is dead
  if conn.state != csClosed and conn.isConnected():
    try:
      await conn.sendMsg(encodeTerminate())
    except CatchableError:
      discard
  conn.state = csClosed
  # Fail any pending notification waiter
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    conn.notifyWaiter.fail(newException(PgError, "Connection closed"))
  await conn.closeTransport()

proc onNotify*(conn: PgConnection, callback: NotifyCallback) =
  conn.notifyCallback = callback

proc reconnectInPlace(conn: PgConnection) {.async.} =
  ## Reconnect using stored config, re-LISTENing on all channels.
  await conn.closeTransport()
  conn.recvBuf.setLen(0)
  conn.state = csConnecting
  let newConn = await connect(conn.config)
  when hasChronos:
    conn.transport = newConn.transport
    conn.baseReader = newConn.baseReader
    conn.baseWriter = newConn.baseWriter
    conn.reader = newConn.reader
    conn.writer = newConn.writer
    conn.tlsStream = newConn.tlsStream
    conn.trustAnchorBufs = newConn.trustAnchorBufs
  elif hasAsyncDispatch:
    conn.socket = newConn.socket
  conn.sslEnabled = newConn.sslEnabled
  conn.recvBuf = newConn.recvBuf
  conn.pid = newConn.pid
  conn.secretKey = newConn.secretKey
  conn.serverParams = newConn.serverParams
  conn.txStatus = newConn.txStatus
  conn.state = csReady
  conn.createdAt = newConn.createdAt
  for ch in conn.listenChannels:
    discard await conn.simpleQuery("LISTEN " & quoteIdentifier(ch))

proc listenPump(conn: PgConnection) {.async.} =
  ## Background loop: repeatedly receives messages, dispatching notifications.
  ## Non-notification messages are discarded (recvMessage handles dispatch).
  ## On connection failure, attempts automatic reconnection with exponential
  ## backoff (up to 10 attempts) and re-subscribes to all channels.
  ## Exits cleanly when state changes from csListening (via stopListening
  ## sending an empty query), then drains until ReadyForQuery.
  while true:
    try:
      while conn.state == csListening:
        discard await conn.recvMessage()
      # State changed -- drain the stop-signal query response until ReadyForQuery
      while true:
        let msg = await conn.recvMessage()
        if msg.kind == bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          break
      return # Clean exit via stopListening
    except CancelledError:
      return # Cancelled from close()
    except CatchableError:
      if conn.listenChannels.len == 0:
        conn.state = csClosed
        if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
          conn.notifyWaiter.fail(newException(PgError, "Connection closed"))
        return
      # Auto-reconnect with exponential backoff
      var reconnected = false
      var backoff = 1
      for attempt in 0 ..< 10:
        try:
          await sleepAsync(seconds(backoff))
          await conn.reconnectInPlace()
          conn.state = csListening
          reconnected = true
          if conn.reconnectCallback != nil:
            conn.reconnectCallback()
          break
        except CancelledError:
          return
        except CatchableError:
          backoff = min(backoff * 2, 30)
      if not reconnected:
        conn.state = csClosed
        if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
          conn.notifyWaiter.fail(newException(PgError, "Reconnection failed"))
        return

proc startListening(conn: PgConnection) =
  conn.state = csListening
  conn.listenTask = conn.listenPump()

proc stopListening*(conn: PgConnection) {.async.} =
  if conn.listenTask == nil or conn.listenTask.finished:
    conn.listenTask = nil
    if conn.state == csListening:
      conn.state = csReady
    return
  # Signal pump to exit by changing state, then send empty query to unblock read
  conn.state = csBusy
  try:
    await conn.sendMsg(encodeQuery(""))
    # Wait for pump to drain and exit naturally
    await conn.listenTask
  except CancelledError as e:
    raise e
  except CatchableError:
    # Send or pump failed -- connection is dead
    if conn.listenTask != nil and not conn.listenTask.finished:
      await cancelAndWait(conn.listenTask)
    conn.listenTask = nil
    conn.state = csClosed
    return
  conn.listenTask = nil
  # Preserve csClosed if pump detected a connection error
  if conn.state != csClosed:
    conn.state = csReady

proc listen*(conn: PgConnection, channel: string): Future[void] {.async.} =
  if conn.state == csListening:
    await conn.stopListening()
  conn.checkReady()
  discard await conn.simpleQuery("LISTEN " & quoteIdentifier(channel))
  conn.listenChannels.incl(channel)
  conn.startListening()

proc unlisten*(conn: PgConnection, channel: string): Future[void] {.async.} =
  if conn.state == csListening:
    await conn.stopListening()
  conn.checkReady()
  discard await conn.simpleQuery("UNLISTEN " & quoteIdentifier(channel))
  conn.listenChannels.excl(channel)
  if conn.listenChannels.len > 0:
    conn.startListening()

proc waitNotification*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[Notification] {.async.} =
  ## Wait for the next notification from the buffer.
  ## If the buffer is empty, blocks until a notification arrives or timeout expires.
  if conn.notifyQueue.len > 0:
    return conn.notifyQueue.popFirst()
  conn.notifyWaiter = newFuture[void]("waitNotification")
  try:
    if timeout > ZeroDuration:
      try:
        await conn.notifyWaiter.wait(timeout)
      except AsyncTimeoutError:
        raise newException(PgError, "Wait for notification timed out")
    else:
      await conn.notifyWaiter
  finally:
    conn.notifyWaiter = nil
  if conn.notifyQueue.len > 0:
    return conn.notifyQueue.popFirst()
  raise newException(PgError, "No notification available")

proc parseSslMode(s: string): SslMode =
  case s
  of "disable":
    sslDisable
  of "prefer":
    sslPrefer
  of "require":
    sslRequire
  of "verify-ca":
    sslVerifyCa
  of "verify-full":
    sslVerifyFull
  else:
    raise newException(PgError, "Invalid sslmode: " & s)

proc parsePort(s: string): int =
  try:
    result = parseInt(s)
  except ValueError:
    raise newException(PgError, "Invalid port in DSN: " & s)
  if result < 1 or result > 65535:
    raise newException(PgError, "Port out of range (1-65535): " & s)

proc parseDsn*(dsn: string): ConnConfig =
  ## Parse a PostgreSQL DSN/URL connection string into a ConnConfig.
  ##
  ## Format: ``postgresql://[user[:password]@][host[:port]][/database][?param=value&...]``
  ##
  ## Both ``postgresql://`` and ``postgres://`` schemes are accepted.
  let scheme =
    if dsn.startsWith("postgresql://"):
      "postgresql"
    elif dsn.startsWith("postgres://"):
      "postgres"
    else:
      raise newException(
        PgError, "Invalid DSN scheme: expected postgresql:// or postgres://"
      )

  # Strip scheme prefix
  let rest = dsn[scheme.len + 3 .. ^1] # skip "scheme://"

  # Split query string
  var body: string
  var queryStr: string
  let qpos = rest.find('?')
  if qpos >= 0:
    body = rest[0 ..< qpos]
    queryStr = rest[qpos + 1 .. ^1]
  else:
    body = rest

  # Split userinfo and hostpath by '@'
  var userinfo, hostpath: string
  let apos = body.rfind('@')
  if apos >= 0:
    userinfo = body[0 ..< apos]
    hostpath = body[apos + 1 .. ^1]
  else:
    hostpath = body

  # Parse user:password
  if userinfo.len > 0:
    let cpos = userinfo.find(':')
    if cpos >= 0:
      result.user = decodeUrl(userinfo[0 ..< cpos])
      result.password = decodeUrl(userinfo[cpos + 1 .. ^1])
    else:
      result.user = decodeUrl(userinfo)

  # Parse host:port/database
  var hostport, dbpath: string
  let spos = hostpath.find('/')
  if spos >= 0:
    hostport = hostpath[0 ..< spos]
    dbpath = hostpath[spos + 1 .. ^1]
  else:
    hostport = hostpath

  if dbpath.len > 0:
    result.database = decodeUrl(dbpath)

  # Parse host and port
  if hostport.len > 0:
    # Handle IPv6: [::1]:5432
    if hostport.startsWith("["):
      let bracket = hostport.find(']')
      if bracket < 0:
        raise newException(PgError, "Invalid IPv6 address in DSN")
      result.host = hostport[1 ..< bracket]
      let afterBracket = hostport[bracket + 1 .. ^1]
      if afterBracket.startsWith(":"):
        result.port = parsePort(afterBracket[1 .. ^1])
      else:
        result.port = 5432
    else:
      let cpos = hostport.rfind(':')
      if cpos >= 0:
        result.host = hostport[0 ..< cpos]
        result.port = parsePort(hostport[cpos + 1 .. ^1])
      else:
        result.host = hostport
        result.port = 5432
  else:
    result.host = "127.0.0.1"
    result.port = 5432

  if result.host.len == 0:
    result.host = "127.0.0.1"

  # Parse query parameters
  if queryStr.len > 0:
    for pair in queryStr.split('&'):
      let epos = pair.find('=')
      if epos < 0:
        continue
      let key = decodeUrl(pair[0 ..< epos])
      let val = decodeUrl(pair[epos + 1 .. ^1])
      case key
      of "sslmode":
        result.sslMode = parseSslMode(val)
      of "application_name":
        result.applicationName = val
      of "connect_timeout":
        try:
          result.connectTimeout = seconds(parseInt(val))
        except ValueError:
          raise newException(PgError, "Invalid connect_timeout: " & val)
      of "sslrootcert":
        try:
          result.sslRootCert = readFile(val)
        except IOError:
          raise newException(PgError, "Cannot read sslrootcert file: " & val)
      else:
        result.extraParams.add((key, val))
