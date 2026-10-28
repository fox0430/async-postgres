## Mock-server tests for `ConnConfig.credentialsProvider`: what each
## authentication request sends, when the provider runs, and how its failures
## classify for a retry loop.

import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_auth, pg_errors, pg_protocol]
import ../async_postgres/pg_connection

import ./mock_pg_server

# Built outside the `{.async.}` test bodies: there a `return` in a provider's
# body would be taken as the outer proc's.

type Calls = ref object
  requests: seq[CredentialsRequest]

proc recording(calls: Calls, creds: ConnCredentials): CredentialsProvider =
  ## Record each request and return `creds`.
  makeCredentialsProvider:
    calls.requests.add(req)
    return creds

proc failingFor(calls: Calls, port: int): CredentialsProvider =
  ## Record each request; raise for the host on `port`.
  makeCredentialsProvider:
    calls.requests.add(req)
    if req.host.port == port:
      raise newException(ValueError, "no token for this host")
    return ConnCredentials(password: "tok")

proc sleeping(ms: int): CredentialsProvider =
  makeCredentialsProvider:
    await sleepAsync(milliseconds(ms))
    return ConnCredentials(password: "tok")

proc gated(gate: Future[void]): CredentialsProvider =
  makeCredentialsProvider:
    await gate
    return ConnCredentials(password: "tok")

proc raising(): CredentialsProvider =
  makeCredentialsProvider:
    raise newException(PgConfigError, "no region configured")

proc providerConfig(port: int, provider: CredentialsProvider): ConnConfig =
  ConnConfig(
    host: "127.0.0.1",
    port: port,
    user: "test",
    database: "test",
    sslMode: sslDisable,
    credentialsProvider: provider,
  )

proc closedPort(): Future[int] {.async.} =
  ## A port nothing listens on: a dial to it is refused at once.
  let ms = startMockServer()
  result = ms.port
  await closeServer(ms)

proc startupUser(client: MockClient): Future[string] {.async.} =
  ## The `user` of the client's StartupMessage.
  let lenBuf = await readN(client, 4)
  let body = await readN(client, int(decodeInt32(lenBuf, 0)) - 4)
  # NUL-terminated key/value pairs after the protocol version.
  let fields = cast[string](body[4 .. ^1]).split('\0')
  for i in countup(0, fields.len - 2, 2):
    if fields[i] == "user":
      return fields[i + 1]

proc connectError(cfg: ConnConfig): Future[ref PgConnectionError] {.async.} =
  try:
    let conn = await connect(cfg)
    await conn.close()
  except PgConnectionError as e:
    return e

suite "makeCredentialsProvider":
  test "inside an async proc, the body ends with the credentials":
    proc testBody(): Future[ConnCredentials] {.async.} =
      let provider = makeCredentialsProvider:
        await sleepAsync(milliseconds(1))
        ConnCredentials(user: req.user, password: "tok")
      return await provider(CredentialsRequest(user: "u"))

    check (waitFor testBody()) == ConnCredentials(user: "u", password: "tok")

suite "credentialsProvider: what is sent":
  test "a cleartext request gets the provider's password":
    proc testBody(): Future[tuple[sent: string, port: int, calls: Calls]] {.async.} =
      let calls = Calls()
      let provider = recording(calls, ConnCredentials(password: "tok"))
      let ms = startMockServer()
      var st: MockClient
      var sent: string
      proc server() {.async.} =
        st = await ms.accept()
        await drainStartupMessage(st)
        await sendBytes(st, buildAuthCleartextPassword())
        sent = cast[string]((await drainFrontendMessage(st)).body)
        await sendFullHandshake(st)

      let serverFut = server()
      let conn = await connect(providerConfig(ms.port, provider))
      await serverFut
      doAssert conn.state == csReady
      await conn.close()
      await closeClient(st)
      await closeServer(ms)
      return (sent, ms.port, calls)

    let (sent, port, calls) = waitFor testBody()
    check sent == "tok\0"
    check calls.requests.len == 1
    check calls.requests[0].host.host == "127.0.0.1"
    check calls.requests[0].host.port == port
    check calls.requests[0].user == "test"

  test "the provider's user replaces the config's, in the startup and the MD5 hash":
    const salt = [9'u8, 8, 7, 6]
    proc testBody(): Future[tuple[user, sent: string]] {.async.} =
      let provider =
        recording(Calls(), ConnCredentials(user: "rotated", password: "tok"))
      let ms = startMockServer()
      var st: MockClient
      var got: tuple[user, sent: string]
      proc server() {.async.} =
        st = await ms.accept()
        got.user = await startupUser(st)
        await sendBytes(st, buildAuthMD5Password(salt))
        got.sent = cast[string]((await drainFrontendMessage(st)).body)
        await sendFullHandshake(st)

      let serverFut = server()
      let conn = await connect(providerConfig(ms.port, provider))
      await serverFut
      await conn.close()
      await closeClient(st)
      await closeServer(ms)
      return got

    let (user, sent) = waitFor testBody()
    check user == "rotated"
    check sent == md5AuthHash("rotated", "tok", salt) & "\0"

  test "connectToHost asks the provider too":
    proc testBody(): Future[string] {.async.} =
      let provider = recording(Calls(), ConnCredentials(password: "tok"))
      let ms = startMockServer()
      var st: MockClient
      var sent: string
      proc server() {.async.} =
        st = await ms.accept()
        await drainStartupMessage(st)
        await sendBytes(st, buildAuthCleartextPassword())
        sent = cast[string]((await drainFrontendMessage(st)).body)
        await sendFullHandshake(st)

      let serverFut = server()
      let conn = await connectToHost(
        providerConfig(ms.port, provider), HostEntry(host: "127.0.0.1", port: ms.port)
      )
      await serverFut
      await conn.close()
      await closeClient(st)
      await closeServer(ms)
      return sent

    check (waitFor testBody()) == "tok\0"

suite "credentialsProvider: when it runs":
  test "each host gets a call, and one that fails moves on to the next host":
    proc testBody(): Future[tuple[port, failing: int, calls: Calls]] {.async.} =
      let failing = await closedPort()
      let calls = Calls()
      let provider = failingFor(calls, failing)
      let ms = startMockServer()
      let accepted = ms.acceptAndReady()
      var cfg = providerConfig(0, provider)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: failing),
        HostEntry(host: "127.0.0.1", port: ms.port),
      ]
      let conn = await connect(cfg)
      let client = await accepted
      doAssert conn.state == csReady
      await conn.close()
      await closeClient(client)
      await closeServer(ms)
      return (ms.port, failing, calls)

    let (port, failing, calls) = waitFor testBody()
    check calls.requests.len == 2
    check calls.requests[0].host.port == failing
    check calls.requests[1].host.port == port

  test "preferStandby's two passes share each host's one call, a failed one too":
    proc testBody(): Future[tuple[port, failing: int, calls: Calls]] {.async.} =
      let failing = await closedPort()
      let calls = Calls()
      let provider = failingFor(calls, failing)
      let ms = startMockServer()
      proc server() {.async.} =
        # A primary: the first pass turns it away, the second takes it.
        for _ in 1 .. 2:
          let st = await ms.acceptAndReady(params = @[("in_hot_standby", "off")]).wait(
            seconds(5)
          )
          try:
            discard await drainFrontendMessage(st) # Terminate
          except CatchableError:
            discard
          await closeClient(st)

      var cfg = providerConfig(0, provider)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: failing),
        HostEntry(host: "127.0.0.1", port: ms.port),
      ]
      cfg.targetSessionAttrs = tsaPreferStandby
      let serverFut = server()
      let conn = await connect(cfg)
      doAssert conn.state == csReady
      await conn.close()
      await serverFut
      await closeServer(ms)
      return (ms.port, failing, calls)

    let (port, failing, calls) = waitFor testBody()
    check calls.requests.len == 2
    check calls.requests[0].host.port == failing
    check calls.requests[1].host.port == port

  test "sslmode=allow's two legs share one call":
    proc testBody(): Future[tuple[err: ref PgConnectionError, calls: Calls]] {.async.} =
      let calls = Calls()
      let provider = recording(calls, ConnCredentials(password: "tok"))
      let ms = startMockServer()
      proc server() {.async.} =
        # The plaintext leg is turned away, the TLS leg's SSLRequest refused.
        for reply in [
          buildErrorResponse("28000", "no pg_hba.conf entry", "FATAL"), @[byte('N')]
        ]:
          let st = await ms.accept()
          try:
            await drainStartupMessage(st)
            await sendBytes(st, reply)
            discard await readN(st, 1)
          except CatchableError:
            discard
          await closeClient(st)

      var cfg = providerConfig(ms.port, provider)
      cfg.sslMode = sslAllow
      let serverFut = server()
      let err = await connectError(cfg)
      await serverFut
      await closeServer(ms)
      return (err, calls)

    let (err, calls) = waitFor testBody()
    check err != nil
    check (ref PgConnectionError)(err.attempts[0]).attempts.len == 2
    check calls.requests.len == 1

  test "a password alongside a provider is a PgConfigError, before any call":
    let calls = Calls()
    var cfg = providerConfig(
      waitFor closedPort(), recording(calls, ConnCredentials(password: "tok"))
    )
    cfg.password = "pencil"
    expect PgConfigError:
      discard waitFor connect(cfg)
    check calls.requests.len == 0

  test "the call has a connectTimeout of its own":
    proc testBody(): Future[PgConnState] {.async.} =
      let provider = sleeping(300)
      let ms = startMockServer()
      var st: MockClient
      proc server() {.async.} =
        st = await ms.accept()
        await drainStartupMessage(st)
        await sleepAsync(milliseconds(300))
        await sendFullHandshake(st)

      var cfg = providerConfig(ms.port, provider)
      # Each step fits the budget; both together would not.
      cfg.connectTimeout = milliseconds(450)
      let serverFut = server()
      let conn = await connect(cfg)
      await serverFut
      result = conn.state
      await conn.close()
      await closeClient(st)
      await closeServer(ms)

    check (waitFor testBody()) == csReady

  test "a call that outlives connectTimeout times out like a dial":
    let gate = newFuture[void]("gate")
    var cfg = providerConfig(waitFor closedPort(), gated(gate))
    cfg.connectTimeout = milliseconds(100)
    var err: ref CatchableError
    try:
      discard waitFor connect(cfg)
    except CatchableError as e:
      err = e
    # Lets asyncdispatch's call, which no timeout can stop, finish.
    if not gate.finished:
      gate.complete()
    check err of AsyncTimeoutError
    check "credentialsProvider timed out" in err.msg
    check retryAdvice(err) == raRetry

suite "credentialsProvider: failures":
  test "a provider that raises fails the host, never as a config fault":
    let err = waitFor connectError(providerConfig(waitFor closedPort(), raising()))
    check err != nil
    check "credentialsProvider failed: no region configured" in err.msg
    check err.attempts[0].parent of PgConfigError
    check retryAdvice(err) == raUnclear
    check not isLastingRefusal(err)

  test "an empty password or a zero byte fails before the dial":
    for (creds, reason) in [
      (ConnCredentials(), "empty password"),
      (ConnCredentials(password: "t\0k"), "zero byte"),
      (ConnCredentials(user: "u\0", password: "tok"), "zero byte"),
    ]:
      let cfg = providerConfig(waitFor closedPort(), recording(Calls(), creds))
      let err = waitFor connectError(cfg)
      check err != nil
      check reason in err.msg
      check retryAdvice(err) == raUnclear

  proc startupAnswer(
      cfg: ConnConfig, sqlState: string
  ): Future[ref PgConnectionError] {.async.} =
    ## `connect`'s error when the server answers the password with `sqlState`.
    let ms = startMockServer()
    proc server() {.async.} =
      let st = await ms.accept()
      try:
        await drainStartupMessage(st)
        await sendBytes(st, buildAuthCleartextPassword())
        discard await drainFrontendMessage(st)
        await sendBytes(st, buildErrorResponse(sqlState, "refused", "FATAL"))
        discard await readN(st, 1)
      except CatchableError:
        discard
      await closeClient(st)

    var cfg = cfg
    cfg.port = ms.port
    let serverFut = server()
    result = await connectError(cfg)
    await serverFut
    await closeServer(ms)

  test "a 28P01 answering the provider's password is no refusal":
    let provided =
      providerConfig(0, recording(Calls(), ConnCredentials(password: "expired")))
    let err = waitFor startupAnswer(provided, "28P01")
    check err != nil
    check err.serverErrors.len == 1
    check err.serverErrors[0].sqlState == "28P01"
    check not isLastingRefusal(err)
    check retryAdvice(err) == raUnclear
    # Not the provider's password to blame: still a refusal.
    check isLastingRefusal(waitFor startupAnswer(provided, "3D000"))
    var fixed = providerConfig(0, nil)
    fixed.password = "pencil"
    check isLastingRefusal(waitFor startupAnswer(fixed, "28P01"))
