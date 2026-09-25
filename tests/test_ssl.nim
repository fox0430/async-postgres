import std/[unittest, strutils, os]

import cert_fixtures
from mock_pg_server import buildPreV3Error
import ../async_postgres/[async_backend, pg_bytes, pg_protocol]

import ../async_postgres/pg_connection/types
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/ssl {.all.}
import ../async_postgres/pg_connection/lifecycle {.all.}

import std/importutils
privateAccess(PgConnection)

when hasAsyncDispatch and not defined(ssl):
  # `tests/config.nims` defines `ssl` for the asyncdispatch backend. Without it
  # the OpenSSL implementation is not compiled and this file's OpenSSL suites
  # vanish from the run silently, leaving a green build over untested code.
  {.
    error:
      "the asyncdispatch test build must define `ssl` (see tests/config.nims); without it the OpenSSL path is never compiled"
  .}

when hasChronos:
  import ../async_postgres/pg_bearssl {.all.}
  import chronos/streams/tlsstream
  import bearssl/abi/bearssl_ssl as bssl

proc testCaCert(): string =
  doAssert ensureTestCerts(),
    "test certificates missing; install openssl and run `bash tests/gen_certs.sh`"
  readFile(currentSourcePath().parentDir / "certs" / "ca.crt")

when hasAsyncDispatch:
  import std/asyncnet
  when defined(ssl):
    import std/[dynlib, net, openssl, base64]

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

  test "empty for bracketed IPv6 and zone-scoped literals":
    check sniName("[::1]", true) == ""
    check sniName("[2001:db8::1]", true) == ""
    check sniName("fe80::1%eth0", true) == ""

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

  when hasChronos:
    test "sslVerifyFull with an IP-literal host is rejected up front (BearSSL)":
      # Regression: BearSSL matches only dNSName SANs, so verify-full with an
      # IP-literal host must fail fast with a clear PgConnectionError instead of
      # attempting a handshake that could never verify the peer identity.
      var raised = false

      proc testBody() {.async.} =
        let ms = startMockServer()

        proc serverHandler() {.async.} =
          let st = await ms.accept()
          try:
            discard await readN(st, 8)
            await sendBytes(st, @[byte('S')])
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
        except PgConnectionError as e:
          raised = true
          doAssert "not supported on the chronos/BearSSL backend" in e.msg,
            "expected the up-front BearSSL rejection, got: " & e.msg

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

  test "an unexpected SSL response byte is a protocol violation":
    var raised = false
    var msgHasUnexpected = false
    var msgHasRawByte = false
    var protocolViolation = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
          await sendBytes(st, @[0x1B'u8]) # ESC: must not reach the message raw
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
        msgHasUnexpected = "Unexpected" in e.msg and "\\x1B" in e.msg
        msgHasRawByte = '\x1B' in e.msg
        protocolViolation = e.parent of PgProtocolError

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgHasUnexpected
    check not msgHasRawByte
    check protocolViolation

  test "a fork failure in reply to the SSLRequest is reported without its text":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
          await sendBytes(
            st,
            buildPreV3Error(
              "could not fork new process for connection: out of memory\n"
            ),
          )
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
      except PgConnectionError as e:
        result = e

      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    require err != nil
    check "error response during SSL exchange" in err.msg
    check "could not fork" notin err.msg
    # Not a protocol violation: the server just could not serve the request.
    check err.parent != nil
    check not (err.parent of PgProtocolError)

suite "SSL negotiation - pre-TLS byte injection":
  test "residual bytes after 'S' response are rejected (CVE-2021-23214 family)":
    # A MITM appends plaintext to 'S' to smuggle it ahead of the encrypted
    # stream; it must be refused before the TLS handshake.
    var raised = false
    var msgMatches = false
    var securityRefusal = false

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
        securityRefusal = e.parent of PgSecurityError

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgMatches
    check securityRefusal

  test "split-write injection after 'S' response is rejected (CVE-2021-23214 family)":
    # Two writes: caught by `socketHasPendingData` or, if they coalesce into
    # chronos's read, by the `n > 1` path.
    var raised = false
    var msgMatches = false
    var securityRefusal = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8) # SSLRequest
          await sendBytes(st, @[byte('S')])
          await sendBytes(st, @[byte('X'), byte('Y'), byte('Z')])
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
        securityRefusal = e.parent of PgSecurityError

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check msgMatches
    check securityRefusal

  test "data trailing an 'N' reply is rejected in every sslmode":
    # One segment so chronos's `readOnce` pulls the extra bytes in.
    proc testBody(mode: SslMode): Future[ref PgError] {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8) # SSLRequest
          await sendBytes(st, @[byte('N'), byte('X'), byte('Y'), byte('Z')])
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: mode,
        sslRootCert:
          if mode in {sslVerifyCa, sslVerifyFull}:
            testCaCert()
          else:
            "",
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        result = e

      await serverFut
      await closeServer(ms)

    for mode in [sslPrefer, sslRequire, sslVerifyCa, sslVerifyFull]:
      checkpoint $mode
      let err = waitFor testBody(mode)
      require err != nil
      check "after SSL refusal" in err.msg
      if mode == sslPrefer:
        check err.parent of PgProtocolError
        check not (err of PgSecurityError)
      else:
        check err.parent of PgSecurityError
        check err of PgSecurityError

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

  test "ConnConfig sslCert and sslKey default to empty":
    let config = ConnConfig()
    check config.sslCert == ""
    check config.sslKey == ""

suite "initConnConfig client certificate validation":
  test "sslCert without sslKey is rejected":
    expect PgError:
      discard initConnConfig(sslMode = sslRequire, sslCert = "dummy")

  test "sslKey without sslCert is rejected":
    expect PgError:
      discard initConnConfig(sslMode = sslRequire, sslKey = "dummy")

  test "sslCert/sslKey with sslDisable is rejected":
    expect PgError:
      discard initConnConfig(sslMode = sslDisable, sslCert = "cert", sslKey = "key")

  test "sslCert/sslKey with sslAllow is rejected":
    expect PgError:
      discard initConnConfig(sslMode = sslAllow, sslCert = "cert", sslKey = "key")

  test "sslCert/sslKey with sslPrefer is accepted":
    let cfg = initConnConfig(sslMode = sslPrefer, sslCert = "cert", sslKey = "key")
    check cfg.sslCert == "cert"
    check cfg.sslKey == "key"

  test "sslCert/sslKey with sslRequire is accepted":
    let cfg = initConnConfig(sslMode = sslRequire, sslCert = "cert", sslKey = "key")
    check cfg.sslCert == "cert"
    check cfg.sslKey == "key"

suite "initConnConfig numeric and hostaddr validation":
  # Mirrors DSN guards so initConnConfig cannot bypass them.
  test "port 0 is rejected":
    expect PgError:
      discard initConnConfig(port = 0)

  test "port above 65535 is rejected":
    expect PgError:
      discard initConnConfig(port = 65536)

  test "hosts entry port out of range is rejected":
    expect PgError:
      discard initConnConfig(
        hosts = @[HostEntry(host: "a", port: 0), HostEntry(host: "b", port: 5432)]
      )

  test "slash hostaddr is rejected":
    expect PgError:
      discard initConnConfig(hostaddr = "/tmp")

  test "slash hostaddr on hosts entry is rejected":
    expect PgError:
      discard initConnConfig(
        hosts = @[HostEntry(host: "db", hostaddr: "/var/run/postgresql", port: 5432)]
      )

  test "negative keepAliveIdle is rejected":
    expect PgError:
      discard initConnConfig(keepAliveIdle = -1)

  test "keepAliveIdle exceeding cint is rejected":
    when sizeof(cint) < sizeof(int):
      expect PgError:
        discard initConnConfig(keepAliveIdle = int(high(cint)) + 1)

  test "negative keepAliveInterval is rejected":
    expect PgError:
      discard initConnConfig(keepAliveInterval = -1)

  test "keepAliveInterval exceeding cint is rejected":
    when sizeof(cint) < sizeof(int):
      expect PgError:
        discard initConnConfig(keepAliveInterval = int(high(cint)) + 1)

  test "negative keepAliveCount is rejected":
    expect PgError:
      discard initConnConfig(keepAliveCount = -1)

  test "keepAliveCount exceeding cint is rejected":
    when sizeof(cint) < sizeof(int):
      expect PgError:
        discard initConnConfig(keepAliveCount = int(high(cint)) + 1)

  test "negative maxMessageSize is rejected":
    expect PgError:
      discard initConnConfig(maxMessageSize = -1)

  test "negative maxScramIterations is rejected":
    expect PgError:
      discard initConnConfig(maxScramIterations = -1)

  test "negative connectTimeout normalizes to ZeroDuration":
    let cfg = initConnConfig(connectTimeout = seconds(-5))
    check cfg.connectTimeout == ZeroDuration

  test "valid boundary port 1 and 65535 are accepted":
    check initConnConfig(port = 1).port == 1
    check initConnConfig(port = 65535).port == 65535

  test "hosts syncs scalar host/port from hosts[0]":
    let cfg = initConnConfig(
      host = "scalar-ignored",
      port = 1111,
      hosts = @[HostEntry(host: "hosts-b", port: 2222)],
    )
    check cfg.host == "hosts-b"
    check cfg.port == 2222
    check cfg.hosts.len == 1
    check cfg.hosts[0].host == "hosts-b"
    check getHosts(cfg)[0].host == "hosts-b"
    check getHosts(cfg)[0].port == 2222

  test "explicitly empty host is rejected, not defaulted to localhost":
    # An unset template variable must fail loudly instead of sending the
    # credentials to whatever listens on localhost.
    expect PgConfigError:
      discard initConnConfig(host = "")

  test "empty hosts entry host is rejected":
    expect PgConfigError:
      discard initConnConfig(hosts = @[HostEntry(host: "", hostaddr: "", port: 5432)])

  test "one empty entry in a multi-host list is rejected":
    expect PgConfigError:
      discard initConnConfig(
        hosts =
          @[HostEntry(host: "primary", port: 5432), HostEntry(host: "", port: 5432)]
      )

  test "empty host paired with a hostaddr stays valid":
    let cfg = initConnConfig(host = "", hostaddr = "10.0.0.1")
    check cfg.hosts.len == 0
    check getHosts(cfg)[0].dialAddr == "10.0.0.1"

suite "Client certificate config validation":
  # `connect()` now validates cert/key pairing before dialing, so these tests
  # no longer need a mock server — the failure fires client-side.
  test "providing only sslCert is a config fault, not a connection failure":
    var raised = false
    var msgMatches = false
    var configFault = false

    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "127.0.0.1",
        port: 1,
        user: "test",
        database: "test",
        sslMode: sslRequire,
        sslCert: "dummy",
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        msgMatches = "sslcert and sslkey must be provided together" in e.msg
        # A pairing mistake no reconnect can fix must stay out of the
        # `PgConnectionError` family a retry loop watches.
        configFault = e of PgConfigError

    waitFor testBody()
    check raised
    check msgMatches
    check configFault

  test "providing only sslKey is a config fault, not a connection failure":
    var raised = false
    var configFault = false

    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "127.0.0.1",
        port: 1,
        user: "test",
        database: "test",
        sslMode: sslRequire,
        sslKey: "dummy",
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        configFault = e of PgConfigError

    waitFor testBody()
    check raised
    check configFault

suite "connect hand-built ConnConfig numeric validation":
  # #631 gap: `validateConnConfig` used to run only in `initConnConfig`.
  # Hand-built `ConnConfig` must hit the same guards at the `connect` chokepoint
  # (no mock server — failure is client-side before dial).
  test "out-of-range port is a config fault, not a connection failure":
    var raised = false
    var configFault = false

    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "127.0.0.1",
        port: 99999,
        user: "test",
        database: "test",
        sslMode: sslDisable,
      )
      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        configFault = e of PgConfigError

    waitFor testBody()
    check raised
    check configFault

  test "port 0 is a config fault":
    var raised = false
    var configFault = false

    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "127.0.0.1", port: 0, user: "test", database: "test", sslMode: sslDisable
      )
      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        configFault = e of PgConfigError

    waitFor testBody()
    check raised
    check configFault

  test "slash hostaddr is a config fault":
    var raised = false
    var configFault = false

    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "db",
        hostaddr: "/tmp",
        port: 5432,
        user: "test",
        database: "test",
        sslMode: sslDisable,
      )
      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        configFault = e of PgConfigError

    waitFor testBody()
    check raised
    check configFault

  test "negative keepAliveIdle is a config fault":
    var raised = false
    var configFault = false

    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "127.0.0.1",
        port: 1,
        user: "test",
        database: "test",
        sslMode: sslDisable,
        keepAliveIdle: -1,
      )
      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        configFault = e of PgConfigError

    waitFor testBody()
    check raised
    check configFault

  test "keepAliveIdle exceeding cint is a config fault":
    when sizeof(cint) < sizeof(int):
      var raised = false
      var configFault = false

      proc testBody() {.async.} =
        let config = ConnConfig(
          host: "127.0.0.1",
          port: 1,
          user: "test",
          database: "test",
          sslMode: sslDisable,
          keepAliveIdle: int(high(cint)) + 1,
        )
        try:
          let conn = await connect(config)
          await conn.close()
        except PgError as e:
          raised = true
          configFault = e of PgConfigError

      waitFor testBody()
      check raised
      check configFault

  test "negative connectTimeout normalizes and does not raise PgConfigError":
    # Normalization must happen before dial; use an immediately-refused port so
    # the attempt finishes without hanging (ZeroDuration = no timeout).
    var configFault = false
    var connected = false

    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "127.0.0.1",
        port: 1,
        user: "test",
        database: "test",
        sslMode: sslDisable,
        connectTimeout: seconds(-5),
      )
      try:
        let conn = await connect(config)
        connected = true
        await conn.close()
      except PgConfigError:
        configFault = true
      except PgError:
        discard

    waitFor testBody()
    check not configFault
    check not connected

suite "connect error aggregation":
  # `oneLine` decides what survives into the combined sslAllow error. It has to
  # flatten each failure onto one line — the next async traceback injection
  # truncates the combined message at the first embedded one — but without
  # throwing away the DETAIL/HINT lines `formatError` appends to every
  # server-sourced error.

  test "oneLine keeps DETAIL and HINT":
    let msg =
      "FATAL: password authentication failed for user \"x\" (SQLSTATE 28P01)\n" &
      "DETAIL: Connection matched pg_hba.conf line 100\nHINT: check sslcert"
    check oneLine(msg) ==
      "FATAL: password authentication failed for user \"x\" (SQLSTATE 28P01) | " &
      "DETAIL: Connection matched pg_hba.conf line 100 | HINT: check sslcert"

  test "oneLine drops the async traceback asyncdispatch injects":
    # Shape of `asyncfutures.injectStacktrace`: the original message, the
    # header, the frame list, then an "Exception message:" echo of the same
    # text. Everything from the header on has to go — including that echo,
    # which would otherwise duplicate the message into the summary.
    let original = "boom\nDETAIL: why"
    let injected =
      original & "\nAsync traceback:\n  lifecycle.nim(1) connect\n" &
      "Exception message: " & original & "\nException type:"
    check oneLine(injected) == "boom | DETAIL: why"

  test "oneLine collapses blank lines and trailing whitespace":
    check oneLine("a\n\n  b  \n") == "a | b"

  test "oneLine leaves a single-line message alone":
    check oneLine("connection refused") == "connection refused"

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
    var msgHasDetail = false
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
            # DETAIL lands on its own line in `formatError`; the summary below
            # must not truncate the failure at the first newline.
            body.add(byte('D'))
            for c in "Connection matched pg_hba.conf line 100":
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
        msgHasDetail = "Connection matched pg_hba.conf line 100" in e.msg
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
    check msgHasDetail
    check msgHasSslFallback
    check msgHasNoSslSupport

  test "sslAllow connects via SSL fallback when SSL handshake refused fails cleanly":
    # Verifies that sslAllow does NOT fall back to plaintext a second time
    # if the server refuses SSL — i.e. its TLS attempt fails on 'N' instead of
    # falling back like sslPrefer.
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

  proc allowNeedingTls(
      channelBinding: ChannelBindingMode, requireAuth: set[AuthMethod]
  ): tuple[firstIsSslRequest: bool, err: ref PgConnectionError] =
    ## `connect` under sslmode=allow against a server that answers 'N'.
    var firstIsSslRequest = false

    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          let req = await readN(st, 8)
          firstIsSslRequest = decodeInt32(req, 4) == 80877103'i32
          await sendBytes(st, @[byte('N')])
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
        sslMode: sslAllow,
        channelBinding: channelBinding,
        requireAuth: requireAuth,
        # A plaintext leg would leave a second dial with no one to answer it.
        connectTimeout: milliseconds(5000),
      )

      var err: ref PgConnectionError
      try:
        let conn = await connect(config)
        await conn.close()
      except PgConnectionError as e:
        err = e

      await serverFut
      await closeServer(ms)
      err

    let err = waitFor testBody()
    (firstIsSslRequest, err)

  test "sslAllow skips the plaintext leg when authentication needs TLS":
    # The one attempt opens with SSLRequest; its 'N' is a security refusal.
    let cases = [
      (cbRequire, set[AuthMethod]({}), "channel binding is required"),
      (cbPrefer, {amScramSha256Plus}, "require_auth allows only SCRAM-SHA-256-PLUS"),
    ]
    for (channelBinding, requireAuth, needle) in cases:
      checkpoint $channelBinding & " " & $requireAuth
      let r = allowNeedingTls(channelBinding, requireAuth)
      check r.firstIsSslRequest
      require r.err != nil
      check r.err of PgSecurityError
      check needle in r.err.msg
      check "plaintext attempt" notin r.err.msg

suite "SSL negotiation - sslDisable":
  test "negotiateSSL with sslDisable raises PgConfigError":
    var raised = false

    proc t() {.async.} =
      var conn = PgConnection()
      conn.state = csReady
      let config = ConnConfig(
        host: "127.0.0.1", port: 1, user: "test", database: "test", sslMode: sslDisable
      )
      try:
        await negotiateSSL(conn, config, "localhost")
      except PgConfigError:
        raised = true

    waitFor t()
    check raised

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

suite "Direct SSL negotiation":
  test "sslnegotiation=direct rejects weak sslmode before any bytes are sent":
    var raised = false
    var errMentionsDirect = false
    var bytesFromClient = 0

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        var st: MockClient
        try:
          st = await ms.accept()
        except CatchableError:
          return
        try:
          let data = await readN(st, 1)
          bytesFromClient = data.len
        except CatchableError:
          discard
        try:
          await closeClient(st)
        except CatchableError:
          discard

      let serverFut = serverHandler()

      let config = ConnConfig(
        host: "127.0.0.1",
        port: ms.port,
        user: "test",
        database: "test",
        sslMode: sslPrefer,
        sslNegotiation: sslnDirect,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        errMentionsDirect = "sslnegotiation=direct" in e.msg

      await closeServer(ms)
      await serverFut

    waitFor testBody()
    check raised
    check errMentionsDirect
    check bytesFromClient == 0

  test "sslnegotiation=direct starts TLS immediately without an SSLRequest":
    var raised = false
    var firstByte: int = -1
    var alpnAdvertised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        # 5-byte TLS record header: type(1) + version(2) + length(2). Reading
        # the full record avoids racing packet boundaries under load/MTU splits.
        try:
          let header = await readN(st, 5)
          firstByte = int(header[0])
          let recordLen = int(fromBE16(header, 3))
          if recordLen > 0:
            let body = await readN(st, recordLen)
            # "postgresql" cannot straddle into the fixed 5-byte record header,
            # so searching the body alone is equivalent to searching the whole
            # record (the ALPN extension always lives inside the ClientHello).
            alpnAdvertised = "postgresql" in readString(body, 0, body.len)
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
        sslNegotiation: sslnDirect,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        # The dumb mock cannot complete the TLS handshake, so connect fails after
        # the ClientHello is observed — exactly what this test inspects.
        raised = true

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check firstByte == 0x16
    check alpnAdvertised
    check raised

  test "sslnegotiation=postgres advertises 'postgresql' ALPN in ClientHello (libpq 17 parity)":
    var raised = false
    var alpnAdvertised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8) # SSLRequest
          await sendBytes(st, @[byte('S')])
          # TLS record: type(1) + version(2) + length(2)
          let header = await readN(st, 5)
          let recordLen = int(fromBE16(header, 3))
          if recordLen > 0:
            let body = await readN(st, recordLen)
            alpnAdvertised = "postgresql" in readString(body, 0, body.len)
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
        sslNegotiation: sslnPostgres,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError:
        # Mock cannot complete TLS handshake; connect fails after ClientHello.
        raised = true

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check alpnAdvertised
    check raised

  test "sslnegotiation=direct with weak sslmode fails once across a multi-host list":
    var raised = false
    var errMentionsDirect = false
    var noAggregateMsg = false

    proc testBody() {.async.} =
      # Port 1: every dial is refused, so only the pre-loop validate can
      # produce the error.
      let config = ConnConfig(
        host: "127.0.0.1,127.0.0.1,127.0.0.1",
        port: 1,
        user: "test",
        database: "test",
        sslMode: sslPrefer,
        sslNegotiation: sslnDirect,
      )
      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        raised = true
        errMentionsDirect = "sslnegotiation=direct requires" in e.msg
        noAggregateMsg = "Could not connect to any host" notin e.msg

    waitFor testBody()
    check raised
    check errMentionsDirect
    check noAggregateMsg

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
  proc connectPort1(
      sslMode: SslMode,
      channelBinding: ChannelBindingMode,
      requireAuth: set[AuthMethod],
      direct: bool,
  ): ref PgError =
    ## The error `connect` (or `connectToHost` when `direct`) raises for a host
    ## that refuses every connection: only a pre-dial check yields a
    ## `PgConfigError`.
    var err: ref PgError
    proc testBody() {.async.} =
      let config = ConnConfig(
        host: "127.0.0.1",
        port: 1,
        user: "test",
        password: "test",
        database: "test",
        sslMode: sslMode,
        channelBinding: channelBinding,
        requireAuth: requireAuth,
      )
      try:
        let conn =
          if direct:
            await connectToHost(config, HostEntry(host: "127.0.0.1", port: 1))
          else:
            await connect(config)
        await conn.close()
      except PgError as e:
        err = e

    waitFor testBody()
    err

  test "security settings contradicting each other are rejected before any dial":
    let cases = [
      (sslDisable, cbRequire, set[AuthMethod]({}), "sslmode=disable"),
      (sslPrefer, cbRequire, {amMd5}, "require_auth"),
      (sslPrefer, cbDisable, {amScramSha256Plus}, "channel_binding=disable"),
      (sslDisable, cbPrefer, {amScramSha256Plus}, "sslmode=disable"),
    ]
    for (sslMode, channelBinding, requireAuth, needle) in cases:
      for direct in [false, true]:
        checkpoint $sslMode & " " & $channelBinding & " " & $requireAuth & " direct=" &
          $direct
        let err = connectPort1(sslMode, channelBinding, requireAuth, direct)
        require err != nil
        check err of PgConfigError
        check needle in err.msg

  test "channel_binding=require with SCRAM-SHA-256-PLUS allowed reaches the dial":
    let err = connectPort1(sslPrefer, cbRequire, {amScramSha256Plus}, false)
    require err != nil
    check err of PgConnectionError

  proc saslRefusal(
      mode: ChannelBindingMode, offer: seq[string]
  ): tuple[err: ref PgError, clientReplied: bool] =
    ## `connect` without TLS against a server offering `offer`.
    var r: tuple[err: ref PgError, clientReplied: bool]

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendAuthSasl(st, offer)
          discard await readN(st, 1)
          r.clientReplied = true
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
        channelBinding: mode,
      )
      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        r.err = e
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    r

  test "SCRAM-SHA-256-PLUS offered without TLS is refused before answering":
    # A server offers -PLUS only over TLS, so TLS was stripped on the way.
    for mode in [cbPrefer, cbDisable]:
      for offer in [@["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"], @["SCRAM-SHA-256-PLUS"]]:
        checkpoint $mode & " " & $offer
        let r = saslRefusal(mode, offer)
        require r.err != nil
        check r.err of PgSecurityError
        check "over a non-SSL connection" in r.err.msg
        check not r.clientReplied

  proc cbRequireRefusal(
      authReq: seq[byte]
  ): tuple[err: ref PgError, clientReplied: bool] =
    ## `connect` under channel_binding=require against a server that declines
    ## TLS, then answers the startup with `authReq`.
    var r: tuple[err: ref PgError, clientReplied: bool]

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st) # SSLRequest
          await sendBytes(st, @[byte('N')])
          await drainStartupMessage(st)
          await sendBytes(st, authReq)
          discard await readN(st, 1)
          r.clientReplied = true
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
        sslMode: sslPrefer,
        channelBinding: cbRequire,
      )
      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        r.err = e
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    r

  test "cbRequire refuses a password request before sending the password":
    let requests = [
      ("cleartext", buildBackendMsg('R', @[byte 0, 0, 0, 3])),
      ("md5", buildBackendMsg('R', @[byte 0, 0, 0, 5, 1, 2, 3, 4])),
    ]
    for (name, req) in requests:
      checkpoint name
      let r = cbRequireRefusal(req)
      require r.err != nil
      check r.err of PgSecurityError
      check "channel binding is required, but server requested" in r.err.msg
      check not r.clientReplied

  test "cbRequire refuses AuthenticationOk without channel binding":
    # ReadyForQuery follows so a missing check connects instead of hanging.
    let r = cbRequireRefusal(
      buildBackendMsg('R', @[byte 0, 0, 0, 0]) & buildBackendMsg('Z', @[byte('I')])
    )
    require r.err != nil
    check r.err of PgSecurityError
    check "without channel binding" in r.err.msg

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

  test "cbRequire raises when the server offers only SCRAM-SHA-256":
    # The mock server cannot speak TLS, so the offered-mechanism check is only
    # reachable here as a unit test.
    var msg = ""
    try:
      discard selectScramMechanism(
        sslEnabled = true,
        serverCertDer = fakeCert,
        saslMechanisms = @["SCRAM-SHA-256"],
        mode = cbRequire,
      )
    except PgConnectionError as e:
      msg = e.msg
    check "did not offer SCRAM-SHA-256-PLUS" in msg

  test "cbRequire raises when the server declined TLS":
    # Reached under sslmode=prefer once the server answers 'N': a per-host
    # outcome, so it stays a `PgConnectionError` the failover loop folds.
    var msg = ""
    try:
      discard selectScramMechanism(
        sslEnabled = false,
        serverCertDer = @[],
        saslMechanisms = bothMechs,
        mode = cbRequire,
      )
    except PgConnectionError as e:
      msg = e.msg
    check "SSL is not in use" in msg

  test "no SCRAM mechanism offered is no PgSecurityError":
    # An unsupported mechanism, not a refusal of anything the config requires.
    for mode in [cbPrefer, cbDisable]:
      checkpoint $mode
      var err: ref PgConnectionError
      try:
        discard selectScramMechanism(
          sslEnabled = false,
          serverCertDer = @[],
          saslMechanisms = @["OAUTHBEARER"],
          mode = mode,
        )
      except PgConnectionError as e:
        err = e
      require err != nil
      check not (err of PgSecurityError)

  test "a lone SCRAM-SHA-256-PLUS the server offered is no PgSecurityError":
    # Unusable (binding off, or no certificate), but nothing refused.
    for (mode, cert) in [(cbDisable, fakeCert), (cbPrefer, newSeq[byte]())]:
      checkpoint $mode
      var err: ref PgConnectionError
      try:
        discard selectScramMechanism(
          sslEnabled = true,
          serverCertDer = cert,
          saslMechanisms = @["SCRAM-SHA-256-PLUS"],
          mode = mode,
        )
      except PgConnectionError as e:
        err = e
      require err != nil
      check not (err of PgSecurityError)
      check "server only offered SCRAM-SHA-256-PLUS" in err.msg

  test "require_auth leaving only SCRAM-SHA-256-PLUS it cannot bind is refused":
    for (mode, cert) in [(cbDisable, fakeCert), (cbPrefer, newSeq[byte]())]:
      checkpoint $mode
      var err: ref PgConnectionError
      try:
        discard selectScramMechanism(
          sslEnabled = true,
          serverCertDer = cert,
          saslMechanisms = bothMechs,
          mode = mode,
          allowed = {amScramSha256Plus},
        )
      except PgConnectionError as e:
        err = e
      require err != nil
      check err of PgSecurityError
      check "require_auth allows only SCRAM-SHA-256-PLUS" in err.msg

  test "require_auth dropping SCRAM-SHA-256-PLUS over TLS sends n,,":
    # The server knows it offered -PLUS, so "y,," would make it abort.
    let choice = selectScramMechanism(
      sslEnabled = true,
      serverCertDer = fakeCert,
      saslMechanisms = bothMechs,
      mode = cbPrefer,
      allowed = {amScramSha256},
    )
    check choice.mechanism == "SCRAM-SHA-256"
    check choice.cbSupportedButUnused == false

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
      x509Free(certVar)

  # Lazy-load these too: Apple's system libcrypto omits some LibreSSL exports
  # and an eager `{.dynlib.}` binding would abort the test binary at startup.
  type
    X509CheckIpAscFn = proc(cert: PX509, ipasc: cstring, flags: cuint): cint {.
      cdecl, gcsafe, raises: []
    .}
    X509CheckHostFn = proc(
      cert: PX509, name: cstring, nameLen: cint, flags: cuint, peerName: ptr cstring
    ): cint {.cdecl, gcsafe, raises: [].}
    X509GetHostFn =
      proc(param: pointer, idx: cint): cstring {.cdecl, gcsafe, raises: [].}

  var
    x509CheckIpAscFn: X509CheckIpAscFn
    x509CheckHostFn: X509CheckHostFn
    x509GetHostFn: X509GetHostFn
    x509TestSymsResolved: bool

  proc resolveX509TestSyms() =
    if x509TestSymsResolved:
      return
    let lib = loadLibPattern(DLLUtilName)
    if lib != nil:
      x509CheckIpAscFn = cast[X509CheckIpAscFn](symAddr(lib, "X509_check_ip_asc"))
      x509CheckHostFn = cast[X509CheckHostFn](symAddr(lib, "X509_check_host"))
      x509GetHostFn = cast[X509GetHostFn](symAddr(lib, "X509_VERIFY_PARAM_get0_host"))
    x509TestSymsResolved = true

  proc dnsMatches(cert: PX509, name: string): bool =
    doAssert x509CheckHostFn != nil, "X509_check_host unavailable"
    x509CheckHostFn(cert, name.cstring, name.len.cint, 0.cuint, nil) == 1

  proc ipMatches(cert: PX509, ip: string): bool =
    resolveX509TestSyms()
    doAssert x509CheckIpAscFn != nil, "X509_check_ip_asc unavailable"
    x509CheckIpAscFn(cert, ip.cstring, 0.cuint) == 1

  suite "SSL verify-full - certificate identity contract (OpenSSL backend)":
    test "IP-SAN cert matches its IP and rejects others":
      resolveX509TestSyms()
      if x509CheckIpAscFn == nil or x509CheckHostFn == nil or x509Free == nil:
        skip()
      else:
        withCert(ipSanCertDerB64, cert):
          check ipMatches(cert, "127.0.0.1")
          check not ipMatches(cert, "10.0.0.1")
          check not dnsMatches(cert, "example.com")

    test "DNS-SAN cert matches its hostname and rejects others":
      resolveX509TestSyms()
      if x509CheckIpAscFn == nil or x509CheckHostFn == nil or x509Free == nil:
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
      if x509GetHostFn == nil or sslGet0Param == nil or sslSet1Host == nil:
        skip()
      else:
        let ctx = newContext(verifyMode = CVerifyNone)
        let ssl = SSL_new(ctx.context)
        doAssert ssl != nil
        try:
          enforceVerifyFullIdentity(ssl, "db.example.com")
          check $x509GetHostFn(sslGet0Param(ssl), 0.cint) == "db.example.com"
        finally:
          SSL_free(ssl)

    test "accepts IP literals without error":
      if x509VerifyParamSet1IpAsc == nil:
        skip()
      else:
        let ctx = newContext(verifyMode = CVerifyNone)
        let ssl = SSL_new(ctx.context)
        doAssert ssl != nil
        try:
          enforceVerifyFullIdentity(ssl, "127.0.0.1")
          enforceVerifyFullIdentity(ssl, "::1")
          # Normalized literals: brackets and zone suffixes are stripped before
          # set1_ip_asc, which accepts only bare IPs.
          enforceVerifyFullIdentity(ssl, "[::1]")
          enforceVerifyFullIdentity(ssl, "fe80::1%eth0")
        finally:
          SSL_free(ssl)

  suite "SSL TLS 1.2 minimum version (OpenSSL backend)":
    test "constant values match OpenSSL semantics":
      # std/openssl misdefines SSL_OP_NO_TLSv1_1 as bit 27 (TLSv1_2); the
      # local constant must be bit 28. SSL_OP_NO_SSLv2 is bit 24 on 1.0.x,
      # the only version where the fallback mask can still run.
      check sslOpNoSslv2 == 0x01000000'i64
      check sslOpNoTlsv11 == 0x10000000'i64
      check sslTls12Version == 0x0303

    test "SET_MIN_PROTO_VERSION path is acknowledged and read back":
      # Exercises `enforceTls12Minimum` (the helper `establishTls` calls) on
      # the installed libssl and pins the exported constants.
      const sslCtrlGetMinProtoVersion = 130 # SSL_CTRL_GET_MIN_PROTO_VERSION (1.1.0+)
      let ctx = newContext(verifyMode = CVerifyNone)
      defer:
        destroyContext(ctx)
      let usedMinVersionControl = enforceTls12Minimum(ctx.context)
      if usedMinVersionControl:
        # OpenSSL 1.1.0+: the minimum must be readable back as TLS 1.2.
        check SSL_CTX_ctrl(ctx.context, sslCtrlGetMinProtoVersion, 0, nil) ==
          sslTls12Version
      else:
        # OpenSSL < 1.1.0 / LibreSSL: the NO_* fallback mask must land in the
        # context options, including the corrected NO_TLSv1_1 and NO_SSLv2 bits.
        let opts = SSL_CTX_ctrl(ctx.context, SSL_CTRL_OPTIONS, 0, nil)
        check (opts and sslOpNoTlsv11) != 0
        check (opts and sslOpNoSslv2) != 0

  suite "SSL driveTlsHandshake (OpenSSL backend)":
    # `wrapConnectedSocket` defers the TLS handshake until the first
    # send/recv on the AsyncSocket. `driveTlsHandshake` completes it up
    # front so `SSL_get_peer_certificate` returns the leaf cert for SCRAM
    # channel binding — without it, cbRequire always fails on asyncdispatch.

    test "sslGetRbio / sslGetWbio resolve on this OpenSSL":
      # Universally exported on OpenSSL and BoringSSL; a nil resolver would
      # break the memory-BIO shuttle the handshake driver relies on.
      check sslGetRbio != nil
      check sslGetWbio != nil

    test "wrapConnectedSocket installs both memory BIOs on the SSL handle":
      if sslGetRbio == nil or sslGetWbio == nil:
        skip()
      else:
        let sock = newAsyncSocket(buffered = false)
        let ctx = newContext(verifyMode = CVerifyNone)
        try:
          wrapConnectedSocket(ctx, sock, handshakeAsClient)
          check sock.sslHandle != nil
          check sslGetRbio(sock.sslHandle) != nil
          check sslGetWbio(sock.sslHandle) != nil
        finally:
          sock.close()

    test "peer certificate is available on client after handshake":
      if not ensureTestCerts() or sslGetPeerCertificate == nil or x509Free == nil:
        skip()
      else:
        let certDir = currentSourcePath().parentDir / "certs"
        var peerCertOk = false
        var serverGotAppByte = false

        proc testBody() {.async.} =
          let listener = newAsyncSocket(buffered = false)
          listener.setSockOpt(OptReuseAddr, true)
          listener.bindAddr(Port(0))
          let port = listener.getLocalAddr()[1]
          listener.listen()

          proc serverSide() {.async.} =
            let s = await listener.accept()
            try:
              let serverCtx = newContext(
                verifyMode = CVerifyNone,
                certFile = certDir / "server.crt",
                keyFile = certDir / "server.key",
              )
              wrapConnectedSocket(serverCtx, s, handshakeAsServer)
              # asyncnet's sslLoop drives the server-side handshake inside recv,
              # then delivers the one application byte the client sends below.
              let data = await s.recv(1)
              serverGotAppByte = data.len == 1
            finally:
              s.close()

          let serverFut = serverSide()

          let c = newAsyncSocket(buffered = false)
          await c.connect("127.0.0.1", port)
          try:
            let clientCtx = newContext(verifyMode = CVerifyNone)
            wrapConnectedSocket(clientCtx, c, handshakeAsClient)
            await driveTlsHandshake(c, verifyingPeer = false)
            let peer = sslGetPeerCertificate(c.sslHandle)
            peerCertOk = peer != nil
            if peer != nil:
              x509Free(peer)
            # Unblock the server's `recv(1)` so its future completes.
            await c.send(" ")
          finally:
            c.close()

          await serverFut
          listener.close()

        waitFor testBody()
        check peerCertOk
        check serverGotAppByte

    type TestServer = enum
      tsTls ## completes the handshake
      tsTls12NeedsClientCert ## fails it with an alert: we send no certificate
      tsGarbage ## answers the ClientHello with no TLS at all

    proc handshakeError(
        caFile: string, server: TestServer
    ): Future[ref CatchableError] {.async.} =
      ## What `driveTlsHandshake` raises, verifying the server against `caFile`.
      const
        SslVerifyFailIfNoPeerCert = 0x02
        SslCtrlSetMaxProtoVersion = 124
        Tls12Version = 0x0303
      let certDir = currentSourcePath().parentDir / "certs"
      let listener = newAsyncSocket(buffered = false)
      listener.setSockOpt(OptReuseAddr, true)
      listener.bindAddr(Port(0))
      let port = listener.getLocalAddr()[1]
      listener.listen()

      proc serverSide() {.async.} =
        let s = await listener.accept()
        try:
          if server == tsGarbage:
            discard await s.recv(1)
            await s.send("not a TLS record")
          else:
            let serverCtx = newContext(
              verifyMode = CVerifyNone,
              certFile = certDir / "server.crt",
              keyFile = certDir / "server.key",
            )
            if server == tsTls12NeedsClientCert:
              SSL_CTX_set_verify(
                serverCtx.context, SSL_VERIFY_PEER or SslVerifyFailIfNoPeerCert, nil
              )
              discard SSL_CTX_ctrl(
                serverCtx.context, SslCtrlSetMaxProtoVersion, Tls12Version, nil
              )
            wrapConnectedSocket(serverCtx, s, handshakeAsServer)
            discard await s.recv(1)
        except CatchableError:
          discard
        finally:
          s.close()

      let serverFut = serverSide()
      let clientCtx = newContext(verifyMode = CVerifyNone)
      SSL_CTX_set_verify(clientCtx.context, SSL_VERIFY_PEER, nil)
      doAssert SSL_CTX_load_verify_locations(
        clientCtx.context, cstring(certDir / caFile), nil
      ) == 1
      let c = newAsyncSocket(buffered = false)
      await c.connect("127.0.0.1", port)
      try:
        wrapConnectedSocket(clientCtx, c, handshakeAsClient)
        await driveTlsHandshake(c, verifyingPeer = true)
      except CatchableError as e:
        result = e
      finally:
        c.close()
      await serverFut
      listener.close()

    test "a server certificate our check rejects is a PgSecurityError":
      if not ensureTestCerts():
        skip()
      else:
        let err = waitFor handshakeError("wrong_ca.crt", tsTls)
        check err of PgSecurityError

    test "the server's alert stays a plain PgConnectionError":
      # Our check passed; the server then refuses us for lacking a certificate.
      if not ensureTestCerts():
        skip()
      else:
        let err = waitFor handshakeError("ca.crt", tsTls12NeedsClientCert)
        check err of PgConnectionError
        check not (err of PgSecurityError)

    test "a handshake failing before the certificate is no PgSecurityError":
      if not ensureTestCerts():
        skip()
      else:
        let err = waitFor handshakeError("ca.crt", tsGarbage)
        check err of PgConnectionError
        check not (err of PgSecurityError)

when hasChronos:
  suite "reconnectInPlace X509 capture rebind":
    test "field-copy alone leaves x509Capture pointers targeting newConn":
      # X509CertCaptureContext holds raw pointers into the connection that
      # installed it. reconnectInPlace value-copies the struct from a transient
      # newConn to conn: pre-fix, both certDer and the engine's x509 slot still
      # reference newConn's memory. Once newConn is GC'd the next TLS I/O
      # follows dangling pointers.
      var conn = PgConnection(serverCertDer: newSeq[byte](0))
      var newConn = PgConnection(serverCertDer: @[byte 0xAA, 0xBB])
      var eng: bssl.SslEngineContext

      installX509Capture(newConn.x509Capture, eng, addr newConn.serverCertDer)
      let newVtableAddr = cast[uint](addr newConn.x509Capture.vtable)

      conn.x509Capture = newConn.x509Capture
      conn.serverCertDer = newConn.serverCertDer

      check conn.x509Capture.certDer == addr newConn.serverCertDer
      check conn.x509Capture.certDer != addr conn.serverCertDer
      check cast[uint](eng.x509ctx) == newVtableAddr
      check cast[uint](eng.x509ctx) != cast[uint](addr conn.x509Capture.vtable)

    test "rebindX509Capture repoints certDer and engine slot at conn":
      var conn = PgConnection(serverCertDer: newSeq[byte](0))
      var newConn = PgConnection(serverCertDer: @[byte 0xAA, 0xBB])
      var eng: bssl.SslEngineContext

      installX509Capture(newConn.x509Capture, eng, addr newConn.serverCertDer)
      conn.x509Capture = newConn.x509Capture
      conn.serverCertDer = newConn.serverCertDer

      rebindX509Capture(conn.x509Capture, eng, addr conn.serverCertDer)

      check conn.x509Capture.certDer == addr conn.serverCertDer
      check cast[uint](eng.x509ctx) == cast[uint](addr conn.x509Capture.vtable)
      # `inner` still routes into the shared default validator (untouched by
      # rebind), so cert-chain delegation keeps working.
      check conn.x509Capture.inner == newConn.x509Capture.inner

when hasChronos:
  proc readCertFile(name: string): string =
    doAssert ensureTestCerts(),
      "test certificates missing; install openssl and run `bash tests/gen_certs.sh`"
    readFile(currentSourcePath().parentDir / "certs" / name)

  proc legacyX509Cert(pem = testCaCert()): string =
    ## `pem` under OpenSSL's legacy "X509 CERTIFICATE" banner.
    pem.replace("BEGIN CERTIFICATE", "BEGIN X509 CERTIFICATE").replace(
      "END CERTIFICATE", "END X509 CERTIFICATE"
    )

  proc connectWithIdentity(cert, key: string): tuple[raised, configFault: bool] =
    ## A load failure is PgConfigError; a loaded identity instead fails as a
    ## connection error when the mock server hangs up.
    var r: tuple[raised, configFault: bool]

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await readN(st, 8)
          await sendBytes(st, @[byte('S')])
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
        sslCert: cert,
        sslKey: key,
      )

      try:
        let conn = await connect(config)
        await conn.close()
      except PgError as e:
        r.raised = true
        r.configFault = e of PgConfigError

      await serverFut
      await closeServer(ms)

    waitFor testBody()
    r

  suite "parseTrustAnchors - malformed PEM input":
    test "empty CERTIFICATE block alone raises PgError, not IndexDefect":
      const pem = "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"
      expect PgError:
        discard parseTrustAnchors(pem)

    test "empty CERTIFICATE block mixed with a valid CA yields the valid anchor":
      const emptyBlock = "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"
      let mixed = emptyBlock & testCaCert()
      let parsed = parseTrustAnchors(mixed)
      check parsed.backing.len > 0

    test "valid CA followed by empty CERTIFICATE block yields the valid anchor":
      const emptyBlock = "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"
      let mixed = testCaCert() & emptyBlock
      let parsed = parseTrustAnchors(mixed)
      check parsed.backing.len > 0

    test "legacy X509 CERTIFICATE label is parsed as an anchor":
      let legacy = legacyX509Cert()
      let parsed = parseTrustAnchors(legacy)
      check parsed.backing.len > 0

    test "loadCertificate accepts the legacy X509 CERTIFICATE label":
      let legacy = legacyX509Cert()
      check loadCertificate(legacy) != nil
      check loadCertificate(testCaCert()) != nil

    test "loadCertificate rejects a PEM with no certificate block":
      expect TLSStreamProtocolError:
        discard loadCertificate(readCertFile("wrong_ca.key"))

    test "legacy-labelled sslcert passes client identity loading in connect":
      let legacy = legacyX509Cert(readCertFile("wrong_ca.crt"))
      let r = connectWithIdentity(legacy, readCertFile("wrong_ca.key"))
      check r.raised
      check not r.configFault

    test "PKCS#1 sslkey passes client identity loading in connect":
      let r = connectWithIdentity(
        readCertFile("wrong_ca.crt"), readCertFile("wrong_ca.rsa.key")
      )
      check r.raised
      check not r.configFault

    test "TRUSTED CERTIFICATE is skipped as an anchor, not trusted without its settings":
      let trusted = readCertFile("ca.trusted.crt")
      var msg = ""
      try:
        discard parseTrustAnchors(trusted)
      except PgConfigError as e:
        msg = e.msg
      check "TRUSTED CERTIFICATE" in msg
      # Plain anchors next to it still load.
      privateAccess(TrustAnchorStore)
      let mixed = parseTrustAnchors(testCaCert() & trusted)
      check mixed.store.anchors.len == 1

    test "TRUSTED CERTIFICATE sslcert is loaded without its trust settings":
      let trusted = readCertFile("ca.trusted.crt")
      privateAccess(TLSCertificate)
      let cert = loadCertificate(trusted)
      let plain = loadCertificate(testCaCert())
      check cert.certs.len == 1
      check cert.certs[0].dataLen == plain.certs[0].dataLen

    test "TRUSTED CERTIFICATE sslcert with a truncated DER body is rejected":
      const pem =
        "-----BEGIN TRUSTED CERTIFICATE-----\n" & "MIIE\n" &
        "-----END TRUSTED CERTIFICATE-----\n"
      expect TLSStreamProtocolError:
        discard loadCertificate(pem)

    test "loadPrivateKey accepts PKCS#8, PKCS#1 and SEC1 banners":
      check loadPrivateKey(readCertFile("wrong_ca.key")) != nil
      check loadPrivateKey(readCertFile("wrong_ca.rsa.key")) != nil
      check loadPrivateKey(readCertFile("ec.key")) != nil

    test "loadPrivateKey skips a leading EC PARAMETERS block":
      const params =
        "-----BEGIN EC PARAMETERS-----\nBggqhkjOPQMBBw==\n-----END EC PARAMETERS-----\n"
      check loadPrivateKey(params & readCertFile("ec.key")) != nil

    test "loadCertificate re-encodes legacy blocks mixed with canonical ones":
      privateAccess(TLSCertificate)
      let ca = loadCertificate(testCaCert())
      let wrongCa = loadCertificate(readCertFile("wrong_ca.crt"))
      let mixed =
        loadCertificate(testCaCert() & legacyX509Cert(readCertFile("wrong_ca.crt")))
      check mixed.certs.len == 2
      check mixed.certs[0].dataLen == ca.certs[0].dataLen
      check mixed.certs[1].dataLen == wrongCa.certs[0].dataLen

    test "loadPrivateKey skips an empty key block":
      const empty = "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n"
      check loadPrivateKey(empty & readCertFile("wrong_ca.rsa.key")) != nil

    const legacyEncryptedKey =
      "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\n" &
      "DEK-Info: AES-256-CBC,00\n\nAAAA\n-----END RSA PRIVATE KEY-----\n"
    const pkcs8EncryptedKey =
      "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n"

    proc loadKeyError(pem: string): string =
      try:
        discard loadPrivateKey(pem)
      except TLSStreamProtocolError as e:
        return e.msg

    test "loadPrivateKey reports passphrase-protected keys":
      let plain = readCertFile("wrong_ca.rsa.key")
      check EncryptedKeyMsg == loadKeyError(readCertFile("encrypted.key"))
      check EncryptedKeyMsg == loadKeyError(pkcs8EncryptedKey)
      check EncryptedKeyMsg == loadKeyError(legacyEncryptedKey)
      # An encrypted block before any usable key aborts the load.
      check EncryptedKeyMsg == loadKeyError(legacyEncryptedKey & plain)
      check EncryptedKeyMsg == loadKeyError(pkcs8EncryptedKey & plain)
      # Blank lines around the RFC 1421 headers are not body text.
      check EncryptedKeyMsg ==
        loadKeyError(
          legacyEncryptedKey.replace("-----\nProc-Type", "-----\n\nProc-Type")
        )

    test "loadPrivateKey loads a plain key before encrypted blocks":
      let plain = readCertFile("wrong_ca.rsa.key")
      check loadPrivateKey(plain & legacyEncryptedKey) != nil
      check loadPrivateKey(plain & pkcs8EncryptedKey) != nil

    test "key blocks BearSSL cannot read are skipped unless encrypted":
      let plain = readCertFile("wrong_ca.rsa.key")
      const brokenDsa = "-----BEGIN DSA PRIVATE KEY-----\nAAAA\n"
      const dsa =
        "-----BEGIN DSA PRIVATE KEY-----\nAAAA\n-----END DSA PRIVATE KEY-----\n"
      const openssh =
        "-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----\n"
      check loadPrivateKey(dsa & plain) != nil
      check loadPrivateKey(openssh & plain) != nil
      check loadPrivateKey(brokenDsa & plain) != nil
      check EncryptedKeyMsg ==
        loadKeyError(legacyEncryptedKey.replace("RSA PRIVATE", "DSA PRIVATE") & plain)

    test "truncated key blocks are reported as malformed":
      for key in [pkcs8EncryptedKey, legacyEncryptedKey]:
        let truncated = key[0 ..< key.find("-----END")]
        check "Invalid PEM encoding" in loadKeyError(truncated)

    test "a malformed key block is reported as such, not as encrypted":
      const corrupt = "-----BEGIN PRIVATE KEY-----\nAA*A\n-----END PRIVATE KEY-----\n"
      let msg = loadKeyError(corrupt & legacyEncryptedKey)
      check "Invalid PEM encoding" in msg

    test "structurally broken blocks are reported, not skipped":
      let cert = testCaCert()
      let truncated = cert[0 ..< cert.find("-----END")]
      let mismatched =
        cert.replace("-----END CERTIFICATE-----", "-----END PRIVATE KEY-----")
      let lines = cert.splitLines
      let colonInBody = (lines[0 .. 2] & @["AAAA:AAAA"] & lines[3 .. ^1]).join("\n")
      for pem in [cert & truncated, mismatched, colonInBody]:
        expect TLSStreamProtocolError:
          discard loadCertificate(pem)
        expect PgConfigError:
          discard parseTrustAnchors(pem)

    test "base64 that BearSSL would reject is not decoded leniently":
      let lines = testCaCert().splitLines
      var mangled = lines
      mangled[1] = mangled[1][0 ..< ^2] & "-_"
      var truncated = lines
      truncated[1] = truncated[1][0 ..< ^1]
      for pem in [mangled.join("\n"), truncated.join("\n")]:
        expect PgConfigError:
          discard parseTrustAnchors(pem)

    test "folded RFC 1421 headers are not body text":
      check EncryptedKeyMsg ==
        loadKeyError(
          legacyEncryptedKey.replace("DEK-Info", "Comment: a\n  b\nDEK-Info")
        )

    test "banners are matched case-sensitively, as by OpenSSL":
      expect TLSStreamProtocolError:
        discard loadCertificate(testCaCert().replace("CERTIFICATE", "certificate"))

    test "text around PEM blocks is ignored":
      let key = readCertFile("wrong_ca.rsa.key")
      check loadPrivateKey(key & "\n") != nil
      check loadPrivateKey("# comment\n" & key & "# trailing note\n") != nil
      check loadCertificate(testCaCert() & "\n# note\n") != nil
      check parseTrustAnchors(testCaCert() & "\n# note\n").backing.len > 0

    test "loadPrivateKey rejects a PEM with no key block":
      expect TLSStreamProtocolError:
        discard loadPrivateKey(testCaCert())

    test "a PEM with no anchor is a config fault, not a connection failure":
      # `PgConnectionError` is the reconnect-worthy family; a PEM that can never
      # parse must not land an application in a retry loop.
      const pem = "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"
      var raised: ref PgError
      try:
        discard parseTrustAnchors(pem)
      except PgError as e:
        raised = e
      check raised != nil
      check raised of PgConfigError

    test "PEM that does not decode at all is a config fault too":
      # Garbage yields no block at all, the anchorless case.
      var raised: ref PgError
      try:
        discard parseTrustAnchors("not a PEM certificate")
      except PgError as e:
        raised = e
      check raised != nil
      check raised of PgConfigError

    test "PEM with only non-CERTIFICATE blocks raises PgError":
      const pem =
        "-----BEGIN PRIVATE KEY-----\n" &
        "MC4CAQAwBQYDK2VwBCIEIN+NLDLNCHmoBpZm5oR0MpsHtL9tS1kYtL9x9wxTx9ZM\n" &
        "-----END PRIVATE KEY-----\n"
      expect PgError:
        discard parseTrustAnchors(pem)

    test "CERTIFICATE block with garbage body is skipped, not crashed":
      # Garbage DER hits the decoder-error path (safe pre-fix); regression guard.
      const pem =
        "-----BEGIN CERTIFICATE-----\n" & "QUFBQQ==\n" & "-----END CERTIFICATE-----\n"
      expect PgError:
        discard parseTrustAnchors(pem)

  suite "cdecl callback len guards":
    test "appendDnCallback appends normal-size chunks":
      var buf: seq[byte]
      let src = @[byte 0xAA, 0xBB, 0xCC]
      appendDnCallback(
        cast[pointer](addr buf), cast[pointer](unsafeAddr src[0]), csize_t(src.len)
      )
      check buf == src

    test "appendDnCallback ignores len > high(int) instead of RangeDefect":
      var buf: seq[byte]
      let src = @[byte 0x11]
      let hugeLen = csize_t(high(int)) + csize_t(1)
      appendDnCallback(
        cast[pointer](addr buf), cast[pointer](unsafeAddr src[0]), hugeLen
      )
      check buf.len == 0

    test "appendDnCallback handles csize_t.high":
      var buf: seq[byte]
      let src = @[byte 0x22]
      appendDnCallback(
        cast[pointer](addr buf), cast[pointer](unsafeAddr src[0]), csize_t.high
      )
      check buf.len == 0

when hasChronos:
  import chronos/streams/asyncstream

  suite "chronos TLS handshake classification":
    # Self-signed RSA for localhost, valid only 2020-01-01..02. Regenerate:
    #   openssl req -x509 -newkey rsa:2048 -nodes \
    #     -keyout k.pem -out c.pem -subj "/CN=localhost" \
    #     -addext "subjectAltName=DNS:localhost" \
    #     -not_before 20200101000000Z -not_after 20200102000000Z
    const expiredCert = """-----BEGIN CERTIFICATE-----
MIIDHzCCAgegAwIBAgIUKXkWe/HrCbKSDlCqZoFrqXwtN+UwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTIwMDEwMTAwMDAwMFoXDTIwMDEw
MjAwMDAwMFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA9w24nF7eFappWaptKD9SVQ66cNZJtl2iry8TzpIEdc6x
JSl6U2wnLnmCSPMz1airphykgvX1xfgejtbdy0X0ikbVbhlMxMFDRYR3laaSvMGx
KuPAedY4GOjri+M5CZ0k0PZ0JTTGWkmoUNkxtaRkgbHDLdPUzmag0sBbcFyynmkI
bWSbjqptDMGls+gwwGixks8L0B4s8rNNJYGPHvurk+wYkxrKOLX18INBlJ5qfyLs
rH875vVerr6//judsnKHEtaKCnKRhG2pt2mk/NHuhqnZRGWqVnLGOQa+LrUcVI5+
9XtLloNMu98Mn/cqROxN68FEMIv7ZZcyIpAHO+MNPwIDAQABo2kwZzAdBgNVHQ4E
FgQUoiI06AcVVlkxlpNrmqa4UPbfrREwHwYDVR0jBBgwFoAUoiI06AcVVlkxlpNr
mqa4UPbfrREwDwYDVR0TAQH/BAUwAwEB/zAUBgNVHREEDTALgglsb2NhbGhvc3Qw
DQYJKoZIhvcNAQELBQADggEBAOlcov2NXiabaZf+WhLggDfs6k1HhS2wSiMMgqJs
HiRIUZqqh5KrlhTMEeFOlVkadYs2JLNnXmiJ6fvO04eB9fuZidrrGgD3a2MBtJ7p
kMMENuhdRx9aN5bl37TLxZOKW6CZPDMlsHe7kn6D5Ag90LAeEVSrzV4xROQJAvxG
eM0r6Aweo7myN3bPBMzkD5oOLBoGo/Q6mn5PmuY5qGEX/R1Nfj6WfVwOKJwHOx58
rdyah4R2cjwIASd81W6i4vxUZH6sBFYVrPCDpUmHimxabTw6uXS8tpKKC3vZfcZS
Ifzbx3fAolwkb+N86Rs1+O3E18zk69+7krfxx0EkXPc1kX4=
-----END CERTIFICATE-----
"""
    const expiredKey = """-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQD3DbicXt4VqmlZ
qm0oP1JVDrpw1km2XaKvLxPOkgR1zrElKXpTbCcueYJI8zPVqKumHKSC9fXF+B6O
1t3LRfSKRtVuGUzEwUNFhHeVppK8wbEq48B51jgY6OuL4zkJnSTQ9nQlNMZaSahQ
2TG1pGSBscMt09TOZqDSwFtwXLKeaQhtZJuOqm0MwaWz6DDAaLGSzwvQHizys00l
gY8e+6uT7BiTGso4tfXwg0GUnmp/Iuysfzvm9V6uvr/+O52ycocS1ooKcpGEbam3
aaT80e6GqdlEZapWcsY5Br4utRxUjn71e0uWg0y73wyf9ypE7E3rwUQwi/tllzIi
kAc74w0/AgMBAAECggEAOpr4KKv+feA7dohNtabzxnakdqD2wnqK3YjK541O3o1m
C11ABesZjlZHuDttF+mXsmOICQMExu4ZfaPt4Esbe/ParG/2/JOl/Cc04PyvQXSn
LPfzEFPnYc1bFweTX7r14VYdbjgLN57SfT9Qofi52ORM2yGtkTraOrRj3I841ge4
IpKbJjkcxDVHFtQedTYSso3XLYBEnlj7xi/4LSttKLv5LzGlCzEh9YL81iianXYt
62ZySTUZ50Z2o8yTtpv/CO+w4XFC6TmrpnZg1bQuesJM8wKLUyqhTdNLBHeQJirA
mtv3xXYAS8jnEsO8u28KW0j+IZmHlm2xbySUM054wQKBgQD/G2oZ+35AxTMQu2jE
gNU/dMaK6FJgtZlCljjrBtkBKAClJlzdafGAXN4TIY06mPEUrQxCdEQM2AXUpvBl
vqWaGOCSa8dSJl+CyaFJv2TDRq8Q3EwEdTwAcOxmQLry8hWKHRi9vgv3/go9p+FX
qmzQblWdTlNif0QHfQu05e3ffwKBgQD36xcnlCw23MLiMqm81MF64j3tD8DlFccJ
6A1H2oYBCsneSoTD3aieqsYNMUiVvJH4YNw6ByqxsZE+fl/Am04J95ozgdEfmFaC
SLg9Iy2e7f7SbgUkMUrar9iHTu6VaS0SwVat7qJUZBnOcHwfTX6+kb4xV7d+uf3n
dO58NmmyQQKBgQD68Smuw1BPQGxaEjAd1Clw0VsYey3Fif1nncQBlWvTklkIG7OP
7c4tGa0uHnwBXz8Ouqbrm9jw1XLu2wRw4VefPMdz4Odh7PNZASRSGh5xZM+DA2EX
pYbPXEV+1D/SCcacZMDYrOCzIsdKHSEyjieZ5F79bXXi1xPBVgU0/lS+2wKBgQDI
lzxa17aWhTRhlKBlmrcZWCjGwHJQaLhsuYbVVmgKO9Jtu1mEqLof9wjb775M+RAa
KTTG9rmCoKtmJxYOXxpbUi0/849iwv1r2K7JOMdWyjXdyQr7564rFxBZGnJMDZdc
j3Y0sNpC8eM3dyfWo/si8gUzI0fij1ZyidfURKpsgQKBgQDP0gF3ONn+OGwCgUEj
Q4+esbU1u048dEtivYu0yMEdaRXOYYooXT/B5vR91LaSy1B1EbeSAXAYaXHQ8qFu
LQ0pnbWDcbpdJRYVH24gp9f/RbLT2q7ETXH3bMtqfWToXyvMIFOCl5kEu7YRdWBQ
JeOmWtVZvOCrgXRtH9DmA+/cbA==
-----END PRIVATE KEY-----
"""

    proc expiredCertError(mode: SslMode): ref PgConnectionError =
      ## `connect`'s error against a server presenting `expiredCert`.
      var err: ref PgConnectionError

      proc testBody(key: TLSPrivateKey, cert: TLSCertificate) {.async.} =
        let ms = startMockServer()

        # The server stream keeps no reference to `key` and `cert`, so they
        # arrive as parameters that outlive the handshake.
        proc serverHandler(key: TLSPrivateKey, cert: TLSCertificate) {.async.} =
          let st = await ms.accept()
          let reader = newAsyncStreamReader(st)
          let writer = newAsyncStreamWriter(st)
          var tls: TLSAsyncStream
          try:
            discard await readN(st, 8) # SSLRequest
            await sendBytes(st, @[byte('S')])
            tls = newTLSServerAsyncStream(
              reader, writer, key, cert, minVersion = TLSVersion.TLS12
            )
            await tls.handshake()
          except CatchableError:
            discard
          if tls != nil:
            await tls.reader.closeWait()
            await tls.writer.closeWait()
          await reader.closeWait()
          await writer.closeWait()
          await closeClient(st)

        let serverFut = serverHandler(key, cert)
        let config = ConnConfig(
          host: "127.0.0.1",
          port: ms.port,
          user: "test",
          database: "test",
          sslMode: mode,
          sslRootCert: expiredCert,
        )
        try:
          let conn = await connect(config)
          await conn.close()
        except PgConnectionError as e:
          err = e
        await serverFut
        await closeServer(ms)

      waitFor testBody(TLSPrivateKey.init(expiredKey), TLSCertificate.init(expiredCert))
      err

    test "an expired certificate sslmode asked to verify is a PgSecurityError":
      let err = expiredCertError(sslVerifyCa)
      require err != nil
      check err of PgSecurityError
      check "TLS handshake failed" in err.msg

    test "BearSSL rejecting a certificate nothing asked to verify is no PgSecurityError":
      let err = expiredCertError(sslRequire)
      require err != nil
      check "TLS handshake failed" in err.msg
      check not (err of PgSecurityError)
