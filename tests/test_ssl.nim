import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_protocol, pg_connection]

when hasAsyncDispatch:
  import std/asyncnet

proc buildBackendMsg(msgType: char, body: seq[byte]): seq[byte] =
  result = @[byte(msgType)]
  result.addInt32(int32(4 + body.len))
  result.add(body)

when hasChronos:
  type
    MockServer = object
      server: StreamServer
      port: int

    MockClient = StreamTransport

  proc startMockServer(): MockServer =
    let server = createStreamServer(initTAddress("127.0.0.1", 0))
    MockServer(server: server, port: int(server.localAddress().port))

  proc accept(ms: MockServer): Future[MockClient] =
    ms.server.accept()

  proc closeServer(ms: MockServer) {.async.} =
    await ms.server.closeWait()

  proc closeClient(client: MockClient) {.async.} =
    await client.closeWait()

  proc readN(client: MockClient, n: int): Future[seq[byte]] {.async.} =
    result = newSeq[byte](n)
    var offset = 0
    while offset < n:
      let bytesRead = await client.readOnce(addr result[offset], n - offset)
      if bytesRead == 0:
        raise newException(CatchableError, "Connection closed prematurely")
      offset += bytesRead

  proc sendBytes(client: MockClient, data: seq[byte]) {.async.} =
    discard await client.write(data)

elif hasAsyncDispatch:
  type
    MockServer = object
      socket: AsyncSocket
      port: int

    MockClient = AsyncSocket

  proc startMockServer(): MockServer =
    let sock = newAsyncSocket(buffered = false)
    sock.setSockOpt(OptReuseAddr, true)
    sock.bindAddr(Port(0))
    let port = int(sock.getLocalAddr()[1])
    sock.listen()
    MockServer(socket: sock, port: port)

  proc accept(ms: MockServer): Future[MockClient] =
    ms.socket.accept()

  proc closeServer(ms: MockServer) {.async.} =
    ms.socket.close()

  proc closeClient(client: MockClient) {.async.} =
    client.close()

  proc readN(client: MockClient, n: int): Future[seq[byte]] {.async.} =
    result = newSeq[byte](n)
    var offset = 0
    while offset < n:
      let data = await client.recv(n - offset)
      if data.len == 0:
        raise newException(CatchableError, "Connection closed prematurely")
      copyMem(addr result[offset], unsafeAddr data[0], data.len)
      offset += data.len

  proc sendBytes(client: MockClient, data: seq[byte]) {.async.} =
    if data.len > 0:
      var str = newString(data.len)
      copyMem(addr str[0], unsafeAddr data[0], data.len)
      await client.send(str)

proc drainStartupMessage(client: MockClient): Future[void] {.async.} =
  let lenBuf = await readN(client, 4)
  let msgLen = decodeInt32(lenBuf, 0)
  if msgLen > 4:
    discard await readN(client, msgLen - 4)

proc sendAuthOkAndReady(client: MockClient): Future[void] {.async.} =
  var resp: seq[byte]
  resp.add(buildBackendMsg('R', @[0'u8, 0, 0, 0]))
  resp.add(buildBackendMsg('Z', @[byte('I')]))
  await sendBytes(client, resp)

proc sendEmptyQueryResult(client: MockClient): Future[void] {.async.} =
  ## Respond to the hstore OID discovery query with an empty result set.
  var resp: seq[byte]
  # CommandComplete: "SELECT 0"
  var ccBody: seq[byte] = @[]
  for c in "SELECT 0":
    ccBody.add(byte(c))
  ccBody.add(0'u8)
  resp.add(buildBackendMsg('C', ccBody))
  # ReadyForQuery
  resp.add(buildBackendMsg('Z', @[byte('I')]))
  await sendBytes(client, resp)

proc drainFrontendMessage(client: MockClient): Future[void] {.async.} =
  ## Read a frontend message (type byte + int32 length + body).
  discard await readN(client, 1) # message type
  let lenBuf = await readN(client, 4)
  let msgLen = decodeInt32(lenBuf, 0)
  if msgLen > 4:
    discard await readN(client, msgLen - 4)

proc drainUntilClose(client: MockClient): Future[void] {.async.} =
  try:
    # Drain the hstore discovery query ('Q' message)
    await drainFrontendMessage(client)
    # Send back an empty result + ReadyForQuery
    await sendEmptyQueryResult(client)
    # Drain the Terminate message
    await drainFrontendMessage(client)
  except CatchableError:
    discard

suite "SslMode and ConnConfig defaults":
  test "ConnConfig zero init has sslDisable":
    let config = ConnConfig()
    check config.sslMode == sslDisable

  test "sslDisable is ordinal 0":
    check ord(sslDisable) == 0

suite "SSL negotiation - server rejects SSL":
  test "sslRequire raises PgError when server responds N":
    var raised = false
    var sslReqLength: int32 = 0
    var sslReqMagic: int32 = 0

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          let sslReq = await readN(st, 8)
          sslReqLength = decodeInt32(sslReq, 0)
          sslReqMagic = decodeInt32(sslReq, 4)
          await sendBytes(st, @[byte('N')])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslRequire,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        raised = true

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check sslReqLength == 8'i32
    check sslReqMagic == 80877103'i32

  test "sslVerifyFull raises PgError when server responds N":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
          await sendBytes(st, @[byte('N')])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslVerifyFull,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        raised = true

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "sslPrefer falls through to plain text when server responds N":
    var connState: PgConnState
    var connSslEnabled: bool

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
          await sendBytes(st, @[byte('N')])
          await drainStartupMessage(st)
          await sendAuthOkAndReady(st)
          await drainUntilClose(st)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslPrefer,
      )

      let conn = await connect(config)
      connState = conn.state
      connSslEnabled = conn.sslEnabled
      await conn.close()

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check connState == csReady
    check connSslEnabled == false

suite "SSL negotiation - error handling":
  test "connection closed during SSL negotiation raises PgError":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslPrefer,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        raised = true

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "unexpected SSL response byte raises PgError":
    var raised = false
    var msgHasUnexpected = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
          await sendBytes(st, @[byte('X')])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslRequire,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        msgHasUnexpected = "Unexpected" in e.msg

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgHasUnexpected

suite "SSL negotiation - sslVerifyCa":
  test "sslVerifyCa raises PgError when server responds N":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
          await sendBytes(st, @[byte('N')])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslVerifyCa,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        raised = true

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "sslAllow ordinal is between sslDisable and sslPrefer":
    check ord(sslAllow) > ord(sslDisable)
    check ord(sslAllow) < ord(sslPrefer)

  test "sslVerifyCa ordinal is between sslRequire and sslVerifyFull":
    check ord(sslVerifyCa) > ord(sslRequire)
    check ord(sslVerifyCa) < ord(sslVerifyFull)

  test "ConnConfig sslRootCert defaults to empty":
    let config = ConnConfig()
    check config.sslRootCert == ""

suite "SSL negotiation - sslAllow":
  test "sslAllow connects without SSL when server accepts plaintext":
    var connState: PgConnState
    var connSslEnabled: bool

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendAuthOkAndReady(st)
          await drainUntilClose(st)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslAllow,
      )

      let conn = await connect(config)
      connState = conn.state
      connSslEnabled = conn.sslEnabled
      await conn.close()

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check connState == csReady
    check connSslEnabled == false

  test "sslAllow retries with SSL when plaintext is rejected":
    var connState: PgConnState
    var connSslEnabled: bool
    var attemptCount: int = 0

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        # First connection: reject plaintext with FATAL error
        block:
          let st = await ms.accept()
          attemptCount.inc
          try:
            discard await readN(st, 8) # read StartupMessage header
            # Send FATAL error response (server requires SSL)
            var body: seq[byte] = @[]
            body.add(byte('S'))
            for c in "FATAL":
              body.add(byte(c))
            body.add(0)
            body.add(byte('M'))
            for c in "no pg_hba.conf entry":
              body.add(byte(c))
            body.add(0)
            body.add(0) # terminator
            var msg: seq[byte] = @[byte('E')]
            msg.addInt32(int32(4 + body.len))
            msg.add(body)
            await sendBytes(st, msg)
          except CatchableError:
            discard
          await closeClient(st)

        # Second connection: accept SSL and complete handshake
        block:
          let st = await ms.accept()
          attemptCount.inc
          try:
            # Read SSLRequest
            discard await readN(st, 8)
            # Refuse SSL (sslPrefer will fall back to plaintext)
            await sendBytes(st, @[byte('N')])
            await drainStartupMessage(st)
            await sendAuthOkAndReady(st)
            await drainUntilClose(st)
          except CatchableError:
            discard
          await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslAllow,
      )

      let conn = await connect(config)
      connState = conn.state
      connSslEnabled = conn.sslEnabled
      await conn.close()

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check attemptCount == 2
    check connState == csReady
    check connSslEnabled == false

suite "SSL negotiation - sslDisable":
  test "sslDisable sends StartupMessage directly without SSLRequest":
    var firstMsgVersion: int32 = 0
    var connState: PgConnState
    var connSslEnabled: bool

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          let header = await readN(st, 8)
          firstMsgVersion = decodeInt32(header, 4)

          let msgLen = decodeInt32(header, 0)
          if msgLen > 8:
            discard await readN(st, msgLen - 8)

          await sendAuthOkAndReady(st)
          await drainUntilClose(st)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslDisable,
      )

      let conn = await connect(config)
      connState = conn.state
      connSslEnabled = conn.sslEnabled
      await conn.close()

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check firstMsgVersion == 196608'i32
    check connState == csReady
    check connSslEnabled == false
