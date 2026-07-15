import std/[unittest, strutils, os]

import ../async_postgres/[async_backend, pg_protocol]

import ../async_postgres/pg_connection {.all.}

proc testCaCert(): string =
  readFile(currentSourcePath().parentDir / "certs" / "ca.crt")

when hasAsyncDispatch:
  import std/asyncnet
  when defined(ssl):
    import std/[dynlib, net, openssl, base64]
    import ../async_postgres/pg_connection/ssl {.all.}

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

suite "sniName":
  test "returns host for DNS name when sslSni is true":
    check sniName("db.example.com", true) == "db.example.com"

  test "empty when sslSni is false":
    check sniName("db.example.com", false) == ""

  test "empty for empty host (hostaddr-only)":
    check sniName("", true) == ""

  test "empty for IPv4 literal (RFC 6066)":
    check sniName("127.0.0.1", true) == ""
    check sniName("10.0.0.1", true) == ""

  test "empty for IPv6 literal (RFC 6066)":
    check sniName("::1", true) == ""
    check sniName("2001:db8::1", true) == ""

  test "returns hostname that only looks numeric":
    check sniName("db1.example.com", true) == "db1.example.com"

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
        sslRootCert: testCaCert(),
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

suite "SSL negotiation - pre-TLS byte injection":
  test "residual bytes after 'S' response are rejected (CVE-2021-23214 family)":
    # A man-in-the-middle appends plaintext to the server's 'S' reply to smuggle
    # it ahead of the encrypted stream. A compliant server sends only 'S' and
    # then waits for the client's ClientHello, so any byte already readable here
    # is injected and the connection must be refused before the TLS handshake.
    var raised = false
    var msgMatches = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8) # SSLRequest
          # 'S' plus injected plaintext, sent as a single segment.
          await sendBytes(st, @[byte('S'), byte('X'), byte('Y'), byte('Z')])
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
        msgMatches = "unencrypted data" in e.msg

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgMatches

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
        sslRootCert: testCaCert(),
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

  test "sslVerifyCa without sslRootCert fails closed before I/O":
    # Web PKI fallback would let any publicly-issued cert MITM (no hostname check).
    var raised = false
    var msgMatches = false
    var sslRequestSeen = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        try:
          let st = await ms.accept()
          try:
            discard await readN(st, 8)
            sslRequestSeen = true
          except CatchableError:
            discard
          await closeClient(st)
        except CatchableError:
          discard

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
      except PgError as e:
        raised = true
        msgMatches = "sslrootcert" in e.msg

      await closeServer(ms)
      try:
        await serverFut
      except CatchableError:
        discard

    waitFor testBody()
    check raised
    check msgMatches
    check not sslRequestSeen

  test "sslVerifyFull without sslRootCert fails closed before I/O":
    var raised = false
    var msgMatches = false
    var sslRequestSeen = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        try:
          let st = await ms.accept()
          try:
            discard await readN(st, 8)
            sslRequestSeen = true
          except CatchableError:
            discard
          await closeClient(st)
        except CatchableError:
          discard

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
      except PgError as e:
        raised = true
        msgMatches = "sslrootcert" in e.msg

      await closeServer(ms)
      try:
        await serverFut
      except CatchableError:
        discard

    waitFor testBody()
    check raised
    check msgMatches
    check not sslRequestSeen

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

when hasAsyncDispatch and defined(ssl):
  # Self-signed test certificates (DER, base64). Regenerate with:
  #   openssl req -x509 -newkey rsa:2048 -nodes -keyout k.pem -out c.pem \
  #     -days 3650 -subj "/CN=pgtest" -addext "subjectAltName=IP:127.0.0.1"
  #   openssl x509 -in c.pem -outform DER | base64 -w0
  const ipSanCertDerB64 =
    "MIIDFDCCAfygAwIBAgIUBC/ksOrlAHotFl9NjclhJw3G2O4wDQYJKoZIhvcNAQELBQAwETEPMA0G" &
    "A1UEAwwGcGd0ZXN0MB4XDTI2MDYwNDEwMDkxOVoXDTM2MDYwMTEwMDkxOVowETEPMA0GA1UEAwwG" &
    "cGd0ZXN0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp2J791gza2g6udeU/GqjOPo3" &
    "R3+qQjmnMMoseROShwYvy3jx3104MQjET3+CKSq+WE5s7/klCdth8OSXIx5v8pYXYWpv0pttoLY9u" &
    "EELVuDtakKDn6/JUGnqSLICaXROgwt+BFRvcowZDnQAN2NK2JLyjdniUb9/v2rJBei7dduPeXXGYX" &
    "TxFiD3CZARH3v0vHX7q8BLJcvvQyF7e4OjciPn6TElWwCQ8dhl+EbhZToqiB/Y2e/bFRc4akxVCsF" &
    "QkzPoL8ZjjtRZ+TdI2yA309ijQZjtl5NEyvZrepRDwcdi9YJ0jNrSwVvCc/kvwbIFX+ZPoaqjmWQ" &
    "MBl6eUJRy5QIDAQABo2QwYjAdBgNVHQ4EFgQUbk2YkIqLrUFiGt60lUd18n3TFU8wHwYDVR0jBBgw" &
    "FoAUbk2YkIqLrUFiGt60lUd18n3TFU8wDwYDVR0TAQH/BAUwAwEB/zAPBgNVHREECDAGhwR/AAABM" &
    "A0GCSqGSIb3DQEBCwUAA4IBAQCKxKeRBH89U/v3F/CCtc7yETsZTycPitHT+hhEZ27Q5lLsk6Ij6U" &
    "KXWpe9Jeols9IpchYGN6pa2BAj299504dLaEYe49qelytB9nS1PqZ5zPbulOu21cxLnlf2ZJWyn3N" &
    "WmcTyxI5RyG7I0jC1eXzlDOHgu9v9ZR2hlAzeXn01L73uU41AqJHO8lFOWdwfBxpCmWwznDjjHDUk" &
    "jFbr+SOpxKSCqQx7lDF+9Cvov2S4+h4X2VJIVPHunhafu50/8DSSzofhe4koJVi9SzY79P8ftBq+K" &
    "wgmje3FeNkhuoU5UNfDpUjxYvCj6qf5czhNYFUuj7MuHomym/pfH0vARKEA"

  # Same identity but SAN is DNS:example.com — no iPAddress entry at all.
  const dnsOnlyCertDerB64 =
    "MIIDGzCCAgOgAwIBAgIUTdXfQzY0TPGM5QtLNrtMKFZhSiwwDQYJKoZIhvcNAQELBQAwETEPMA0G" &
    "A1UEAwwGcGd0ZXN0MB4XDTI2MDYwNDEwMDkxOVoXDTM2MDYwMTEwMDkxOVowETEPMA0GA1UEAwwG" &
    "cGd0ZXN0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvpF69ndENQ0GoYmc1W66TykY3" &
    "GXR5xjj9WYRu2CQoSbpsEkjUy1aLeVd21FCP0EnHeld19ubu13R4G79uxm/WHWDVWb6vkUT7SjG0t" &
    "tulwwo3HB1BEhBam6gM26O5XeTMc/QYfUC/qvCkgYr4rNk8PMscygOlKrMTGef7wxp/Xq3r1x/K+S" &
    "4j3mhSiXJLz9bs3akYMnUuXrHhLwpLMGEu3iLwyByU69jqafm7mtsL0dzUVT/tfUPYAK/XB04wZlm" &
    "Lj1ELStyE/YE3Xz4hDp+w6HLo1o8LJS/uSqVKV2ewkvV3VlGhsJ8w7lXqaFX3E8HccLUlq227h6p7" &
    "Yt5oqd8YQIDAQABo2swaTAdBgNVHQ4EFgQUGczIcS+cbD+81sWdz9RUezuv/owwHwYDVR0jBBgwFo" &
    "AUGczIcS+cbD+81sWdz9RUezuv/owwDwYDVR0TAQH/BAUwAwEB/zAWBgNVHREEDzANggtleGFtcGxl" &
    "LmNvbTANBgkqhkiG9w0BAQsFAAOCAQEAPkrlJ66nQyFo70GOZ86HnKP2QXZnX1jV3JAUsIwyuP7Dx" &
    "IIohD05OO9aYg83H82ws407S+irdMiVoQ2Gfc1pPynZs05UZ8z1BTyhQxy0yANkBSz5+tKch3XbOR" &
    "rnQ7nuOgtIoWzaBYYoURN6q7kSk0nazgmBjHGfiE2SdP54PbUGFMENmySVSINJHvARYb3wC4w6E0o" &
    "LJlsnOUOJCVD64UVolb7UB2Ar/oLf5eqkNFjxBIgDu+Hh6drh58gzhFYE7EiA6LDLorjJssa9Vo+d" &
    "Fq+ENKzk1ucMPe+uxWNR+L3idun/UEXb7cyUDisqv9xsZH0m/c/vi1q2ZPTnYsto0A=="

  template withCert(b64: string, certVar, body: untyped) =
    let der = base64.decode(b64)
    let certVar {.inject.} = d2i_X509(der)
    try:
      body
    finally:
      X509_free(certVar)

  # Lazy-load these too: Apple's system libcrypto omits some LibreSSL exports
  # and an eager `{.dynlib.}` binding would abort the test binary at startup.
  type
    X509CheckIpAscFn = proc(cert: PX509, ipasc: cstring, flags: cuint): cint {.
      cdecl, gcsafe, raises: []
    .}
    X509GetHostFn =
      proc(param: pointer, idx: cint): cstring {.cdecl, gcsafe, raises: [].}

  var
    x509CheckIpAscFn: X509CheckIpAscFn
    x509GetHostFn: X509GetHostFn
    x509TestSymsResolved: bool

  proc resolveX509TestSyms() =
    if x509TestSymsResolved:
      return
    let lib = loadLibPattern(DLLUtilName)
    if lib != nil:
      x509CheckIpAscFn = cast[X509CheckIpAscFn](symAddr(lib, "X509_check_ip_asc"))
      x509GetHostFn = cast[X509GetHostFn](symAddr(lib, "X509_VERIFY_PARAM_get0_host"))
    x509TestSymsResolved = true

  proc dnsMatches(cert: PX509, name: string): bool =
    X509_check_host(cert, name.cstring, name.len.cint, 0.cuint, nil) == 1

  proc ipMatches(cert: PX509, ip: string): bool =
    resolveX509TestSyms()
    doAssert x509CheckIpAscFn != nil, "X509_check_ip_asc unavailable"
    x509CheckIpAscFn(cert, ip.cstring, 0.cuint) == 1

  suite "SSL verify-full - certificate identity contract (OpenSSL backend)":
    test "IP-SAN cert matches its IP and rejects others":
      resolveX509TestSyms()
      if x509CheckIpAscFn == nil:
        skip()
      else:
        withCert(ipSanCertDerB64, cert):
          check ipMatches(cert, "127.0.0.1")
          check not ipMatches(cert, "10.0.0.1")
          check not dnsMatches(cert, "example.com")

    test "DNS-SAN cert matches its hostname and rejects others":
      resolveX509TestSyms()
      if x509CheckIpAscFn == nil:
        skip()
      else:
        withCert(dnsOnlyCertDerB64, cert):
          check dnsMatches(cert, "example.com")
          check not dnsMatches(cert, "evil.example.com")
          check not dnsMatches(cert, "pgtest")
          check not ipMatches(cert, "127.0.0.1")

  suite "SSL verify-full - enforceVerifyFullIdentity (OpenSSL backend)":
    test "installs the DNS host on the SSL handle":
      resolveX509TestSyms()
      let getParam = sslGet0Param()
      if x509GetHostFn == nil or getParam == nil or sslSet1Host() == nil:
        skip()
      else:
        let ctx = newContext(verifyMode = CVerifyNone)
        let ssl = SSL_new(ctx.context)
        doAssert ssl != nil
        try:
          enforceVerifyFullIdentity(ssl, "db.example.com")
          check $x509GetHostFn(getParam(ssl), 0.cint) == "db.example.com"
        finally:
          SSL_free(ssl)

    test "accepts IP literals without error":
      if x509VerifyParamSet1IpAsc() == nil:
        skip()
      else:
        let ctx = newContext(verifyMode = CVerifyNone)
        let ssl = SSL_new(ctx.context)
        doAssert ssl != nil
        try:
          enforceVerifyFullIdentity(ssl, "127.0.0.1")
          enforceVerifyFullIdentity(ssl, "::1")
        finally:
          SSL_free(ssl)
