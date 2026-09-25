## Network-failure tests using an in-process mock PostgreSQL server.
##
## These exercise code paths that a real server will not reproduce on demand:
## mid-handshake disconnects, mid-query disconnects, malformed backend
## messages, and unknown message type bytes. Verifies that the client raises
## the expected exception type and leaves the connection in a state that
## prevents accidental reuse (`csClosed`).

import std/[unittest, strutils, base64]

import pkg/nimcrypto
import pkg/nimcrypto/pbkdf2

import ../async_postgres/[async_backend, pg_protocol]
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/buffer_io
from ../async_postgres/pg_connection/lifecycle {.all.} import attemptHostTimed
when hasAsyncDispatch:
  from std/nativesockets import Domain

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1",
    port: port,
    user: "test",
    database: "test",
    password: "pencil",
    sslMode: sslDisable,
  )

proc hasIpv6Loopback(): bool =
  ## Whether ::1 can be bound: not in a container or runner with IPv6 off.
  try:
    waitFor closeServer(startMockServer("::1"))
    true
  except CatchableError:
    false

suite "Dial":
  test "an IPv6 host is reachable, and so is its cancel":
    proc testBody(): Future[seq[byte]] {.async.} =
      let ms = startMockServer("::1")
      let accepted = ms.acceptAndReady()
      var cfg = mockConfig(ms.port)
      cfg.host = "::1"
      let conn = await connect(cfg)
      let client = await accepted
      let cancelSide = ms.accept()
      await conn.cancel()
      let cancelClient = await cancelSide
      result = await readN(cancelClient, 8)
      await closeClient(cancelClient)
      await conn.close()
      await closeClient(client)
      await closeServer(ms)

    if not hasIpv6Loopback():
      skip()
    else:
      let request = waitFor testBody()
      check decodeInt32(request, 0) == 16
      check decodeInt32(request, 4) == 80877102 # CancelRequest code

  when defined(linux):
    # All of 127/8 is loopback on Linux: 127.0.0.2 and 127.0.0.3 refuse where
    # only 127.0.0.1 listens.
    test "each address is tried until one accepts":
      proc testBody(): Future[string] {.async.} =
        let ms = startMockServer("127.0.0.1")
        let accepted = ms.accept()
        let targets =
          resolveTargets("127.0.0.2", ms.port) & resolveTargets("127.0.0.1", ms.port)
        let dialed = await dialTargets(targets)
        let client = await accepted
        result = dialed.target.shown
        await closeClient(dialed.stream)
        await closeClient(client)
        await closeServer(ms)

      check (waitFor testBody()) == "127.0.0.1"

    test "every refused address is reported, on one line each":
      proc testBody(): Future[ref CatchableError] {.async.} =
        let ms = startMockServer("127.0.0.1")
        let port = ms.port
        await closeServer(ms)
        let targets =
          resolveTargets("127.0.0.2", port) & resolveTargets("127.0.0.3", port)
        try:
          discard await dialTargets(targets)
        except CatchableError as e:
          result = e

      let err = waitFor testBody()
      check err of PgUnavailableError
      check isTransientError(err)
      # asyncdispatch appends its traceback at the await above.
      let summary = err.msg.split("\nAsync traceback:")[0]
      check summary.startsWith("127.0.0.2: ")
      check "; 127.0.0.3: " in summary
      check '\n' notin summary

  test "prefer-standby looks each host up once for both passes":
    proc testBody(): Future[ref CatchableError] {.async.} =
      let gone = startMockServer()
      let refusedPort = gone.port
      await closeServer(gone)
      var cfg = mockConfig(refusedPort)
      cfg.hosts = @[
        HostEntry(host: "nonexistent.invalid", port: 5432),
        HostEntry(host: "127.0.0.1", port: refusedPort),
      ]
      cfg.targetSessionAttrs = tsaPreferStandby
      try:
        discard await connect(cfg)
      except CatchableError as e:
        result = e

    let err = waitFor testBody()
    require err of PgConnectionError
    check (ref PgConnectionError)(err).attempts.len == 2
    # The dial is tried in both passes, the failed lookup only in the first.
    let summary = err.msg.split("\nAsync traceback:")[0]
    check summary.count("Could not resolve host") == 1
    check summary.count("127.0.0.1:") == 2

  test "a name the resolver does not know is transient":
    # A container or pod may not be registered yet; a typo retries to the cap.
    var err: ref CatchableError
    try:
      discard resolveTargets("nonexistent.invalid", 5432)
    except CatchableError as e:
      err = e
    check err of PgUnavailableError
    check "nonexistent.invalid" in err.msg
    check isTransientError(err)

  when hasAsyncDispatch and defined(linux):
    test "a mapped IPv4 is dialed over IPv4, with no second lookup":
      proc testBody(): Future[string] {.async.} =
        let ms = startMockServer("127.0.0.1")
        let accepted = ms.accept()
        let dialed = await dialTargets(resolveTargets("::ffff:127.0.0.1", ms.port))
        let client = await accepted
        result = dialed.target.shown
        await closeClient(dialed.stream)
        await closeClient(client)
        await closeServer(ms)

      check (waitFor testBody()) == "127.0.0.1"

    test "a resolved address keeps its zone and unmaps a mapped IPv4":
      let linkLocal = resolveTargets("fe80::1%1", 5432)
      check linkLocal.len == 1
      check linkLocal[0].domain == Domain.AF_INET6
      check linkLocal[0].address == "fe80::1%1"
      let mapped = resolveTargets("::ffff:127.0.0.1", 5432)
      check mapped.len == 1
      check mapped[0].domain == Domain.AF_INET
      check mapped[0].address == "127.0.0.1"

  test "an empty target list is refused, not indexed":
    proc testBody(): Future[ref CatchableError] {.async.} =
      try:
        discard await dialTargets(@[])
      except CatchableError as e:
        result = e

    check (waitFor testBody()) of ValueError

  test "a config fault is not hidden behind a failed lookup":
    # verify-full without sslrootcert is refused per TCP host, before its lookup.
    proc testBody(): Future[ref CatchableError] {.async.} =
      var cfg = mockConfig(5432)
      cfg.host = "nonexistent.invalid"
      cfg.sslMode = sslVerifyFull
      try:
        discard await connect(cfg)
      except CatchableError as e:
        result = e

    check (waitFor testBody()) of PgConfigError

suite "Per-address connectTimeout":
  # Two ports on 127.0.0.1 stand in for two addresses of one host.
  proc timedConfig(port: int): ConnConfig =
    result = mockConfig(port)
    result.connectTimeout = milliseconds(300)

  test "an address that never answers leaves the next its own budget":
    proc testBody(): Future[PgConnState] {.async.} =
      let silent = startMockServer()
      let ms = startMockServer()
      let stalled = silent.accept()
      let accepted = ms.acceptAndReady()
      let targets =
        resolveTargets("127.0.0.1", silent.port) & resolveTargets("127.0.0.1", ms.port)
      let conn = await attemptHostTimed(
        timedConfig(ms.port),
        HostEntry(host: "127.0.0.1", port: ms.port),
        tsaAny,
        targets,
      )
      let client = await accepted
      result = conn.state
      await conn.close()
      await closeClient(client)
      await closeClient(await stalled)
      await closeServer(silent)
      await closeServer(ms)

    check (waitFor testBody()) == csReady

  test "a host whose every address times out times out":
    proc testBody(): Future[ref CatchableError] {.async.} =
      let a = startMockServer()
      let b = startMockServer()
      let stalledA = a.accept()
      let stalledB = b.accept()
      let targets =
        resolveTargets("127.0.0.1", a.port) & resolveTargets("127.0.0.1", b.port)
      try:
        discard await attemptHostTimed(
          timedConfig(a.port),
          HostEntry(host: "127.0.0.1", port: a.port),
          tsaAny,
          targets,
        )
      except CatchableError as e:
        result = e
      await closeClient(await stalledA)
      await closeClient(await stalledB)
      await closeServer(a)
      await closeServer(b)

    let err = waitFor testBody()
    check err of AsyncTimeoutError
    check isTransientError(err)

  test "a host that timed out at one address and was refused at another times out":
    proc testBody(): Future[ref CatchableError] {.async.} =
      let silent = startMockServer()
      let gone = startMockServer()
      let refusedPort = gone.port
      await closeServer(gone)
      let stalled = silent.accept()
      let targets =
        resolveTargets("127.0.0.1", silent.port) &
        resolveTargets("127.0.0.1", refusedPort)
      try:
        discard await attemptHostTimed(
          timedConfig(silent.port),
          HostEntry(host: "127.0.0.1", port: silent.port),
          tsaAny,
          targets,
        )
      except CatchableError as e:
        result = e
      await closeClient(await stalled)
      await closeServer(silent)

    let err = waitFor testBody()
    # Raw, as a single host's connectTimeout is, yet naming every address.
    check err of AsyncTimeoutError
    let summary = err.msg.split("\nAsync traceback:")[0]
    check summary.count("127.0.0.1: ") == 2
    check err.parent of PgUnavailableError
    check isTransientError(err)

  test "a server's refusal skips the host's other addresses":
    # They lead to the same server: the password is not sent again, nor the
    # refusal summed up with a refused dial into a transient failure.
    proc testBody(timeout: Duration): Future[ref CatchableError] {.async.} =
      let refusing = startMockServer()
      let gone = startMockServer()
      let refusedPort = gone.port
      await closeServer(gone)
      proc refuse() {.async.} =
        let st = await refusing.accept()
        await drainStartupMessage(st)
        await sendBytes(
          st, buildErrorResponse("28P01", "password authentication failed", "FATAL")
        )
        await closeClient(st)

      let serverFut = refuse()
      var cfg = mockConfig(refusing.port)
      cfg.connectTimeout = timeout
      let targets =
        resolveTargets("127.0.0.1", refusing.port) &
        resolveTargets("127.0.0.1", refusedPort)
      try:
        discard await attemptHostTimed(
          cfg, HostEntry(host: "127.0.0.1", port: refusing.port), tsaAny, targets
        )
      except CatchableError as e:
        result = e
      await serverFut
      await closeServer(refusing)

    for timeout in [default(Duration), milliseconds(300)]:
      let err = waitFor testBody(timeout)
      check err of PgConnectionError
      let ce = (ref PgConnectionError)(err)
      check ce.attempts.len == 0
      check ce.serverError != nil and ce.serverError.sqlState == "28P01"
      check not isTransientError(err)

  test "cancel reaches the address the session is on":
    # The host's first address refused the session, so it runs on the second;
    # a cancel dialing the host afresh would hit the first.
    proc testBody(): Future[seq[byte]] {.async.} =
      let gone = startMockServer()
      let refusedPort = gone.port
      await closeServer(gone)
      let ms = startMockServer()
      let accepted = ms.acceptAndReady()
      let targets =
        resolveTargets("127.0.0.1", refusedPort) & resolveTargets("127.0.0.1", ms.port)
      let conn = await attemptHostTimed(
        mockConfig(refusedPort),
        HostEntry(host: "127.0.0.1", port: refusedPort),
        tsaAny,
        targets,
      )
      let client = await accepted
      let cancelSide = ms.accept()
      await conn.cancel()
      let cancelClient = await cancelSide
      result = await readN(cancelClient, 8)
      await closeClient(cancelClient)
      await conn.close()
      await closeClient(client)
      await closeServer(ms)

    let request = waitFor testBody()
    check decodeInt32(request, 4) == 80877102 # CancelRequest code

# Handshake failures

suite "Network failure: handshake":
  test "server closes immediately after accept":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except CatchableError:
        raised = true
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "server reads startup then disconnects":
    proc testBody(): Future[ref CatchableError] {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except CatchableError as e:
        result = e
      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    require err of PgConnectionError
    # Also how a proxy with nothing behind it yet answers.
    check (ref PgConnectionError)(err).attempts[0] of PgUnavailableError
    check isTransientError(err)

when hasAsyncDispatch:
  suite "Network failure: connect timeout orphan":
    test "timed-out connect closes the late-arriving connection":
      ## asyncdispatch's wait() cannot cancel a timed-out connect; the
      ## attempt keeps running in the background. If it eventually succeeds,
      ## the orphaned connection must be closed, not leaked.
      var timedOut = false
      var serverSawClose = false

      proc testBody() {.async.} =
        let ms = startMockServer()

        proc serverHandler() {.async.} =
          let st = await ms.accept()
          await drainStartupMessage(st)
          # Complete the handshake well after the client's connectTimeout.
          await sleepMsAsync(250)
          await sendFullHandshake(st)
          # The orphan close path sends Terminate and drops the socket:
          # keep reading until EOF. The 5s guard turns a leaked connection
          # into a test failure instead of a hang.
          try:
            while true:
              discard await readN(st, 1).wait(seconds(5))
          except AsyncTimeoutError:
            serverSawClose = false
          except CatchableError:
            serverSawClose = true
          await closeClient(st)

        let serverFut = serverHandler()
        var cfg = mockConfig(ms.port)
        cfg.connectTimeout = milliseconds(50)
        try:
          discard await connect(cfg)
        except AsyncTimeoutError:
          timedOut = true
        await serverFut
        await closeServer(ms)

      waitFor testBody()
      check timedOut
      check serverSawClose

# Per-host connect timeout (multi-host failover)

suite "Network failure: per-host connect timeout":
  test "a hung first host fails over to a healthy second host":
    ## `connectTimeout` is applied per host (libpq semantics): a host that
    ## accepts TCP but never answers the startup message consumes only its own
    ## timeout budget, after which failover reaches the next, healthy host.
    ## A summed budget would let the first host exhaust it and the second host
    ## would never be tried.
    var connected = false

    proc testBody() {.async.} =
      let slow = startMockServer() # accepts TCP but stalls the handshake
      let fast = startMockServer() # completes the handshake promptly

      proc slowHandler() {.async.} =
        let st = await slow.accept()
        try:
          await drainStartupMessage(st)
          # Answer only well after the per-host timeout has fired and failover
          # has moved on. Under asyncdispatch the abandoned attempt then
          # completes and its orphan-close path tears this connection down;
          # under chronos the attempt was already cancelled.
          await sleepMsAsync(300)
          await sendFullHandshake(st)
          while true:
            discard await readN(st, 1).wait(seconds(5))
        except CatchableError:
          discard
        await closeClient(st)

      proc fastHandler() {.async.} =
        let st = await acceptAndReady(fast)
        try:
          discard await drainFrontendMessage(st) # Terminate from close()
        except CatchableError:
          discard
        await closeClient(st)

      let slowFut = slowHandler()
      let fastFut = fastHandler()

      let cfg = ConnConfig(
        hosts: @[
          HostEntry(host: "127.0.0.1", port: slow.port),
          HostEntry(host: "127.0.0.1", port: fast.port),
        ],
        user: "test",
        database: "test",
        sslMode: sslDisable,
        connectTimeout: milliseconds(100),
      )
      let conn = await connect(cfg)
      connected = conn.isConnected()
      await conn.close()

      await closeServer(slow)
      await closeServer(fast)
      await fastFut
      await slowFut

    waitFor testBody()
    check connected

# Malformed / truncated backend messages

suite "Network failure: malformed server messages":
  test "unknown backend message type 'X' raises PgProtocolError":
    var raised = false
    var finalState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st) # SELECT 1
          # Reply with an unknown message type wrapped in a valid frame.
          await sendBytes(st, buildBackendMsg('X', @[byte 0]))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgProtocolError:
        raised = true
      except CatchableError:
        raised = true
      finalState = conn.state
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check finalState == csClosed

  test "truncated RowDescription before full body arrives":
    # Server sends a valid header claiming a larger body than it will deliver,
    # then closes. The read loop should return an incomplete-parse state until
    # the close signals EOF, which raises an error.
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # Claim a 100-byte message but only send 10 bytes then close.
          var truncated: seq[byte]
          truncated.add(byte('T'))
          truncated.addInt32(100'i32)
          for _ in 0 ..< 5:
            truncated.add(0'u8)
          await sendBytes(st, truncated)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "claimed msgLen below minimum (3) raises PgProtocolError":
    var raised = false
    var gotProtocolError = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # 5-byte frame with msgLen=3 — parser must reject.
          var buf = newSeq[byte](5)
          buf[0] = byte('C')
          buf[1] = 0
          buf[2] = 0
          buf[3] = 0
          buf[4] = 3
          await sendBytes(st, buf)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgProtocolError:
        raised = true
        gotProtocolError = true
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check gotProtocolError

  test "malformed CommandComplete without null terminator raises PgProtocolError":
    var raised = false
    var gotProtocolError = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # CommandComplete body with no NUL terminator.
          var buf: seq[byte]
          buf.add(byte('C'))
          buf.addInt32(int32(4 + 3))
          buf.add(byte('S'))
          buf.add(byte('E'))
          buf.add(byte('L')) # no trailing 0
          await sendBytes(st, buf)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgProtocolError:
        raised = true
        gotProtocolError = true
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check gotProtocolError

# Mid-query disconnects

suite "Network failure: mid-query disconnects":
  test "server closes after sending partial DataRow":
    # Note: csClosed here is reached via the EOF / socket-close path, not via
    # the `PgProtocolError -> csClosed` transition in `nextMessage`. If future
    # changes decouple socket close from state transition, this assertion may
    # start failing silently (the state would no longer be csClosed).
    var raised = false
    var finalState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # Send a valid RowDescription with one int4 column (oid=23).
          var rd: seq[byte]
          rd.addInt16(1) # 1 field
          for c in "n":
            rd.add(byte(c))
          rd.add(0'u8) # name terminator
          rd.addInt32(0'i32) # tableOid
          rd.addInt16(0'i16) # columnAttrNum
          rd.addInt32(23'i32) # typeOid (int4)
          rd.addInt16(4'i16) # typeSize
          rd.addInt32(-1'i32) # typeMod
          rd.addInt16(0'i16) # formatCode
          await sendBytes(st, buildBackendMsg('T', rd))
          # Start a DataRow but truncate it mid-column.
          var dr: seq[byte]
          dr.addInt16(1) # 1 column
          dr.addInt32(100'i32) # claim 100 bytes
          dr.add(byte(0)) # partial
          # Claim the whole msgLen is correct for our buffer so parser reads it.
          let body = dr
          var frame: seq[byte]
          frame.add(byte('D'))
          frame.addInt32(int32(4 + body.len))
          frame.add(body)
          await sendBytes(st, frame)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except CatchableError:
        raised = true
      finalState = conn.state
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check finalState == csClosed

  test "server closes after sending ErrorResponse without final field terminator":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # ErrorResponse body with field 'M' value missing its NUL terminator.
          var body: seq[byte]
          body.add(byte('M'))
          body.add(byte('o')) # start value, no NUL
          await sendBytes(st, buildBackendMsg('E', body))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

# SCRAM mutual-authentication enforcement (CR-1 regression)
#
# A malicious server / MITM must not be able to skip AuthenticationSASLFinal
# (which carries the server signature proving it knows the password) and have
# the client accept a bare AuthenticationOk. The client must verify the server
# signature before treating SCRAM as successful.

const scramSalt = @[
  byte 0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE, 0x01, 0x23, 0x45, 0x67, 0x89,
  0xAB, 0xCD, 0xEF,
]
const scramIterations = 4096

proc driveScramUntilClientFinal(
    st: MockClient
): Future[tuple[clientFirstBare, serverFirst, clientFinal: string]] {.async.} =
  ## Run the server side of a SCRAM-SHA-256 exchange up to (and including)
  ## reading the client's client-final message. Returns the strings needed to
  ## compute the server signature.
  await drainStartupMessage(st)
  await sendBytes(st, buildAuthSASL(@["SCRAM-SHA-256"]))
  let (_, initBody) = await drainFrontendMessage(st)
  let clientFirst = parseSaslInitialClientFirst(initBody)
  # Non-PLUS gs2 header is "n,," (3 bytes); the rest is the client-first-bare.
  let clientFirstBare = clientFirst[3 .. ^1]
  let rpos = clientFirstBare.rfind("r=")
  let clientNonce = clientFirstBare[rpos + 2 .. ^1]
  let serverFirst =
    "r=" & clientNonce & "serverNoncePart,s=" & base64.encode(scramSalt) & ",i=" &
    $scramIterations
  await sendBytes(st, buildAuthSASLContinue(serverFirst))
  let (_, finalBody) = await drainFrontendMessage(st)
  let clientFinal = cast[string](finalBody)
  return (clientFirstBare, serverFirst, clientFinal)

proc serverSignatureFor(
    password, clientFirstBare, serverFirst, clientFinal: string
): string =
  ## Compute the correct ``v=...`` SASLFinal payload for the given exchange.
  let cfwp = clientFinal[0 ..< clientFinal.find(",p=")]
  let authMessage = clientFirstBare & "," & serverFirst & "," & cfwp
  let saltedPassword = sha256.pbkdf2(password, scramSalt, scramIterations, 32)
  let serverKey = sha256.hmac(saltedPassword, "Server Key").data
  let serverSig = sha256.hmac(serverKey, authMessage).data
  "v=" & base64.encode(serverSig)

suite "SCRAM mutual-auth enforcement":
  test "rejects AuthenticationOk sent before SASLFinal":
    var raised = false
    var sawScramMsg = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          discard await driveScramUntilClientFinal(st)
          # Malicious: skip AuthenticationSASLFinal entirely and jump to AuthOk.
          await sendBytes(st, buildAuthOk())
          await sendBytes(st, buildBackendKeyData(1, 2))
          await sendBytes(st, buildReadyForQuery('I'))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except PgConnectionError as e:
        raised = true
        sawScramMsg = e.msg.contains("SCRAM")
      except CatchableError:
        discard
      try:
        await serverFut
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check raised
    check sawScramMsg

  test "accepts a valid SASLFinal server signature":
    var connected = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          let (cfb, sf, cf) = await driveScramUntilClientFinal(st)
          # mockConfig sets password "pencil"; both sides must use the same.
          await sendBytes(
            st, buildAuthSASLFinal(serverSignatureFor("pencil", cfb, sf, cf))
          )
          await sendBytes(st, buildAuthOk())
          await sendBytes(st, buildBackendKeyData(1, 2))
          await sendBytes(st, buildReadyForQuery('I'))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        connected = conn.state == csReady
        await conn.close()
      except CatchableError:
        discard
      try:
        await serverFut
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check connected

  test "rejects SASLContinue without a preceding AuthenticationSASL":
    # A malicious server / MITM that skips AuthenticationSASL leaves scramState
    # default-initialized (clientNonce == ""), which would otherwise make the
    # nonce-binding check pass vacuously. The client must reject the message.
    var raised = false
    var sawScramMsg = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          # Forge a server-first message without ever sending AuthenticationSASL.
          let serverFirst =
            "r=forgedNonce,s=" & base64.encode(scramSalt) & ",i=" & $scramIterations
          await sendBytes(st, buildAuthSASLContinue(serverFirst))
          await sendBytes(st, buildAuthOk())
          await sendBytes(st, buildBackendKeyData(1, 2))
          await sendBytes(st, buildReadyForQuery('I'))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except PgConnectionError as e:
        raised = true
        sawScramMsg = e.msg.contains("AuthenticationSASL")
      except CatchableError:
        discard
      try:
        await serverFut
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check raised
    check sawScramMsg

  test "rejects SASLFinal without a preceding AuthenticationSASL":
    # A malicious server / MITM that skips AuthenticationSASL leaves scramState
    # default-initialized (serverSignature zeroed) and would otherwise bypass
    # the SCRAM server-signature verification path. The client must reject it.
    var raised = false
    var sawScramMsg = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          # Forge a server-final message without ever sending AuthenticationSASL.
          await sendBytes(st, buildAuthSASLFinal("v=forgedSignature"))
          await sendBytes(st, buildAuthOk())
          await sendBytes(st, buildBackendKeyData(1, 2))
          await sendBytes(st, buildReadyForQuery('I'))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except PgConnectionError as e:
        raised = true
        sawScramMsg = e.msg.contains("AuthenticationSASL")
      except CatchableError:
        discard
      try:
        await serverFut
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check raised
    check sawScramMsg
