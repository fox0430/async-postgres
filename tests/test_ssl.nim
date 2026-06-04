import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_protocol]

import ../async_postgres/pg_connection {.all.}

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
      copyMem(addr result[offset], addr data[0], data.len)
      offset += data.len

  proc sendBytes(client: MockClient, data: seq[byte]) {.async.} =
    if data.len > 0:
      var str = newString(data.len)
      copyMem(addr str[0], addr data[0], data.len)
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

proc drainFrontendMessage(client: MockClient): Future[void] {.async.} =
  ## Read a frontend message (type byte + int32 length + body).
  discard await readN(client, 1) # message type
  let lenBuf = await readN(client, 4)
  let msgLen = decodeInt32(lenBuf, 0)
  if msgLen > 4:
    discard await readN(client, msgLen - 4)

proc drainUntilClose(client: MockClient): Future[void] {.async.} =
  ## Drain the Terminate message sent by the client on close.
  try:
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

  test "sslAllow attempts SSL after plaintext failure and reports both errors":
    var attemptCount: int = 0
    var raised = false
    var msgHasSslMode = false
    var msgHasPlaintext = false
    var msgHasPgHba = false
    var msgHasSslFallback = false
    var msgHasNoSslSupport = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        # First connection: reject plaintext with a FATAL error
        block:
          let st = await ms.accept()
          attemptCount.inc
          try:
            discard await readN(st, 8) # read StartupMessage header
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

        # Second connection: SSL fallback. Refuse SSL so sslRequire fails —
        # this verifies libpq-compatible semantics (no further plaintext retry).
        block:
          let st = await ms.accept()
          attemptCount.inc
          try:
            discard await readN(st, 8) # read SSLRequest
            await sendBytes(st, @[byte('N')]) # refuse SSL
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

      try:
        let conn = await connect(config)
        await conn.close()
      except PgConnectionError as e:
        raised = true
        msgHasSslMode = "sslmode=allow" in e.msg
        msgHasPlaintext = "plaintext attempt failed" in e.msg
        msgHasPgHba = "no pg_hba.conf entry" in e.msg
        msgHasSslFallback = "SSL fallback failed" in e.msg
        msgHasNoSslSupport = "Server does not support SSL" in e.msg

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check attemptCount == 2
    check raised
    # Both failure reasons must be preserved in the final error message.
    check msgHasSslMode
    check msgHasPlaintext
    check msgHasPgHba
    check msgHasSslFallback
    check msgHasNoSslSupport

  test "sslAllow connects via SSL fallback when SSL handshake refused fails cleanly":
    # Verifies that sslAllow does NOT fall back to plaintext a second time
    # if the server refuses SSL — i.e. fallback uses sslRequire semantics,
    # not sslPrefer.
    var attemptCount: int = 0
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        # First connection: close immediately to fail plaintext startup.
        block:
          let st = await ms.accept()
          attemptCount.inc
          await closeClient(st)

        # Second connection: refuse SSL. Must NOT result in plaintext retry.
        block:
          let st = await ms.accept()
          attemptCount.inc
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
        sslMode: sslAllow,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgConnectionError:
        raised = true

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    # Exactly two attempts: one plaintext, one SSL — no third plaintext retry.
    check attemptCount == 2
    check raised

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

proc sendAuthSasl(client: MockClient, mechanisms: seq[string]): Future[void] {.async.} =
  var body: seq[byte] = @[]
  body.addInt32(10) # AuthenticationSASL
  for m in mechanisms:
    body.addCString(m)
  body.add(0'u8) # terminator
  await sendBytes(client, buildBackendMsg('R', body))

proc readSaslInitialResponseMechanism(client: MockClient): Future[string] {.async.} =
  ## Read a frontend 'p' message (SASLInitialResponse) and return the mechanism name.
  discard await readN(client, 1) # 'p' message type
  let lenBuf = await readN(client, 4)
  let msgLen = decodeInt32(lenBuf, 0)
  let body = await readN(client, msgLen - 4)
  result = ""
  var i = 0
  while i < body.len and body[i] != 0:
    result.add(char(body[i]))
    inc i

suite "SCRAM channel binding enforcement":
  test "cbRequire without SSL raises PgError":
    var raised = false
    var msgMatches = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendAuthSasl(st, @["SCRAM-SHA-256"])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        password: "test",
        database: "test",
        sslMode: sslDisable,
        channelBinding: cbRequire,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        msgMatches = "SSL is not in use" in e.msg

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgMatches

  test "cbRequire errors when server offers only SCRAM-SHA-256":
    var raised = false
    var msgMatches = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendAuthSasl(st, @["SCRAM-SHA-256"])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        password: "test",
        database: "test",
        sslMode: sslDisable,
        channelBinding: cbRequire,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        msgMatches = "channel binding" in e.msg

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgMatches

  test "cbDisable picks SCRAM-SHA-256 even when PLUS is offered":
    var pickedNonPlus = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendAuthSasl(st, @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"])
          let mech = await readSaslInitialResponseMechanism(st)
          pickedNonPlus = mech == "SCRAM-SHA-256"
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        password: "test",
        database: "test",
        sslMode: sslDisable,
        channelBinding: cbDisable,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        discard

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check pickedNonPlus

  test "cbDisable errors when server offers only PLUS":
    var raised = false
    var msgMatches = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendAuthSasl(st, @["SCRAM-SHA-256-PLUS"])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        password: "test",
        database: "test",
        sslMode: sslDisable,
        channelBinding: cbDisable,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        msgMatches = "channel binding" in e.msg

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgMatches

  test "cbPrefer without SSL accepts SCRAM-SHA-256":
    var pickedScram = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendAuthSasl(st, @["SCRAM-SHA-256"])
          let mech = await readSaslInitialResponseMechanism(st)
          pickedScram = mech == "SCRAM-SHA-256"
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        password: "test",
        database: "test",
        sslMode: sslDisable,
        channelBinding: cbPrefer,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        discard

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check pickedScram

suite "selectScramMechanism":
  const fakeCert = @[byte 0x30, 0x82, 0x01, 0x22] # dummy DER prefix
  const bothMechs = @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]

  test "cbDisable rejects PLUS when SSL is available":
    let choice = selectScramMechanism(
      sslEnabled = true,
      serverCertDer = fakeCert,
      saslMechanisms = bothMechs,
      mode = cbDisable,
    )
    check choice.mechanism == "SCRAM-SHA-256"
    check choice.cbType == ""
    check choice.cbData.len == 0
    # cbDisable always sends "n,,", never the downgrade-detection "y,,".
    check choice.cbSupportedButUnused == false

  test "cbPrefer signals y,, over SSL when server omits PLUS (downgrade detect)":
    let choice = selectScramMechanism(
      sslEnabled = true,
      serverCertDer = fakeCert,
      saslMechanisms = @["SCRAM-SHA-256"],
      mode = cbPrefer,
    )
    check choice.mechanism == "SCRAM-SHA-256"
    check choice.cbType == ""
    check choice.cbSupportedButUnused == true

  test "cbPrefer sends n,, without SSL even when CB unavailable":
    let choice = selectScramMechanism(
      sslEnabled = false,
      serverCertDer = @[],
      saslMechanisms = @["SCRAM-SHA-256"],
      mode = cbPrefer,
    )
    check choice.mechanism == "SCRAM-SHA-256"
    check choice.cbSupportedButUnused == false

  test "cbPrefer picks PLUS when SSL + cert + server support all present":
    let choice = selectScramMechanism(
      sslEnabled = true,
      serverCertDer = fakeCert,
      saslMechanisms = bothMechs,
      mode = cbPrefer,
    )
    check choice.mechanism == "SCRAM-SHA-256-PLUS"
    check choice.cbType == "tls-server-end-point"
    check choice.cbData.len > 0
    # Channel binding is in use ("p=,,"), so no downgrade signal needed.
    check choice.cbSupportedButUnused == false

  test "cbPrefer falls back to SCRAM-SHA-256 when cert is missing":
    let choice = selectScramMechanism(
      sslEnabled = true,
      serverCertDer = @[],
      saslMechanisms = bothMechs,
      mode = cbPrefer,
    )
    check choice.mechanism == "SCRAM-SHA-256"
    # The server *did* offer SCRAM-SHA-256-PLUS, so this is not a downgrade — the
    # cert is simply unavailable. Sending "y,," here would make the server abort
    # with a channel binding negotiation error, so send "n,," instead.
    check choice.cbSupportedButUnused == false

  test "cbRequire succeeds when SSL + cert + PLUS all available":
    let choice = selectScramMechanism(
      sslEnabled = true,
      serverCertDer = fakeCert,
      saslMechanisms = bothMechs,
      mode = cbRequire,
    )
    check choice.mechanism == "SCRAM-SHA-256-PLUS"
    check choice.cbType == "tls-server-end-point"
    check choice.cbData.len > 0

  test "cbRequire raises when cert is missing even with SSL":
    expect PgConnectionError:
      discard selectScramMechanism(
        sslEnabled = true,
        serverCertDer = @[],
        saslMechanisms = bothMechs,
        mode = cbRequire,
      )

  test "cbPrefer raises when no SCRAM mechanism is offered":
    expect PgConnectionError:
      discard selectScramMechanism(
        sslEnabled = false,
        serverCertDer = @[],
        saslMechanisms = @["SOMETHING-ELSE"],
        mode = cbPrefer,
      )
