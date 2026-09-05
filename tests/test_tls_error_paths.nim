import std/[net, os, osproc, strutils, unittest]

import ../async_postgres/[async_backend, pg_connection, pg_errors]
import mock_pg_server

const CertDir = currentSourcePath().parentDir / "certs"

type ProbeResult = tuple[raised: bool, msg: string]

proc testConfig(port: int, mode: SslMode, sslNegotiation = sslnPostgres): ConnConfig =
  ConnConfig(
    host: "127.0.0.1",
    port: port,
    user: "test",
    database: "test",
    sslMode: mode,
    sslNegotiation: sslNegotiation,
    connectTimeout: milliseconds(5000),
  )

proc readCert(name: string): string =
  readFile(CertDir / name)

proc startSslProbe(ms: MockServer, closeAfterReply: bool) =
  ## Accept one connection, answer the SSLRequest probe with 'S' (TLS
  ## requested), then either hold the connection or close it. All reads are
  ## bounded so a client that fails before closing cannot hang the suite.
  proc handler() {.async.} =
    try:
      let st = await ms.accept()
      discard await readN(st, 8)
      await sendBytes(st, @[byte('S')])
      if closeAfterReply:
        await closeClient(st)
      else:
        try:
          discard await (readN(st, 8)).wait(milliseconds(3000))
        except CatchableError:
          discard
        await closeClient(st)
    except CatchableError:
      discard

  discard handler()

# The client-side certificate/key/CA load failures below happen inside
# `establishTls` *after* the SSLRequest probe succeeds and *before* any TLS
# bytes are exchanged, so a bare 'S' responder is sufficient — no real TLS
# server and no PostgreSQL are needed.

suite "TLS error paths: client cert/key/CA loading":
  test "garbage CA content fails verify-ca":
    proc runTest(): Future[ProbeResult] {.async.} =
      let ms = startMockServer()
      startSslProbe(ms, closeAfterReply = false)
      var cfg = testConfig(ms.port, sslVerifyCa)
      cfg.sslRootCert = "not a PEM certificate"
      var raised = false
      var msg = ""
      try:
        let conn = await connect(cfg)
        await conn.close()
      except PgError as e:
        raised = true
        msg = e.msg
      await closeServer(ms)
      (raised, msg)

    let (raised, msg) = waitFor runTest()
    check raised
    check msg.len > 0

  test "garbage client certificate content fails":
    proc runTest(): Future[ProbeResult] {.async.} =
      let ms = startMockServer()
      startSslProbe(ms, closeAfterReply = false)
      var cfg = testConfig(ms.port, sslRequire)
      cfg.sslCert = "not a PEM certificate"
      cfg.sslKey = readCert("server.key")
      var raised = false
      var msg = ""
      try:
        let conn = await connect(cfg)
        await conn.close()
      except PgError as e:
        raised = true
        msg = e.msg
      await closeServer(ms)
      (raised, msg)

    let (raised, msg) = waitFor runTest()
    check raised
    when hasAsyncDispatch:
      check "Failed to load client certificate" in msg
    elif hasChronos:
      # BearSSL reports the same PEM-decode error for cert and key loading,
      # so only the load-stage failure is observable, not which of the two
      # inputs was malformed.
      check "Invalid PEM encoding" in msg

  test "garbage client key content fails":
    proc runTest(): Future[ProbeResult] {.async.} =
      let ms = startMockServer()
      startSslProbe(ms, closeAfterReply = false)
      var cfg = testConfig(ms.port, sslRequire)
      cfg.sslCert = readCert("server.crt")
      cfg.sslKey = "not a PEM private key"
      var raised = false
      var msg = ""
      try:
        let conn = await connect(cfg)
        await conn.close()
      except PgError as e:
        raised = true
        msg = e.msg
      await closeServer(ms)
      (raised, msg)

    let (raised, msg) = waitFor runTest()
    check raised
    when hasAsyncDispatch:
      check "Failed to load client private key" in msg
    elif hasChronos:
      # Same BearSSL PEM-decode error as the certificate case above.
      check "Invalid PEM encoding" in msg

  test "passphrase-protected client key is rejected":
    proc runTest(): Future[ProbeResult] {.async.} =
      let ms = startMockServer()
      startSslProbe(ms, closeAfterReply = false)
      var cfg = testConfig(ms.port, sslRequire)
      cfg.sslCert = readCert("server.crt")
      cfg.sslKey = readCert("encrypted.key")
      var raised = false
      var msg = ""
      try:
        let conn = await connect(cfg)
        await conn.close()
      except PgError as e:
        raised = true
        msg = e.msg
      await closeServer(ms)
      (raised, msg)

    let (raised, msg) = waitFor runTest()
    check raised
    when hasAsyncDispatch:
      # Rejection surfaces either as the repo's passphrase-protected message
      # or as an OpenSSL PEM-lib load failure (when the passwd callback path
      # is taken) — both carry "client private key", which the load-error
      # wrapper prefix does not.
      check "client private key" in msg
    elif hasChronos:
      check "Could not find private key" in msg

  when hasAsyncDispatch:
    test "mismatched client cert/key pair is rejected (key values mismatch)":
      # chronos/BearSSL never pairs the client cert with the key client-side;
      # the mismatch only surfaces if the server requests client auth, so this
      # fail-closed check exists on the asyncdispatch backend only. OpenSSL
      # rejects the pair while loading the key, so the error text carries the
      # "key values mismatch" reason.
      proc runTest(): Future[ProbeResult] {.async.} =
        let ms = startMockServer()
        startSslProbe(ms, closeAfterReply = false)
        var cfg = testConfig(ms.port, sslRequire)
        cfg.sslCert = readCert("server.crt")
        cfg.sslKey = readCert("wrong_ca.key")
        var raised = false
        var msg = ""
        try:
          let conn = await connect(cfg)
          await conn.close()
        except PgError as e:
          raised = true
          msg = e.msg
        await closeServer(ms)
        (raised, msg)

      let (raised, msg) = waitFor runTest()
      check raised
      check "mismatch" in msg

suite "TLS handshake failure path":
  test "peer closes the connection during the handshake":
    proc runTest(): Future[string] {.async.} =
      let ms = startMockServer()
      startSslProbe(ms, closeAfterReply = true)
      var msg = ""
      # Catch broadly and assert the type: a raw AsyncStreamError must fail the
      # assertion, not skip closeServer and leak the mock server.
      try:
        try:
          let conn = await connect(testConfig(ms.port, sslRequire))
          await conn.close()
        except CatchableError as e:
          doAssert e of PgConnectionError
          msg = e.msg
      finally:
        await closeServer(ms)
      msg

    let msg = waitFor runTest()
    check msg.len > 0
    # `connect` folds every per-host failure into PgConnectionError, so the type
    # says nothing here; only the wording checked below rules out a leak.
    when hasAsyncDispatch:
      check "closed by peer" in msg
    elif hasChronos:
      check "TLS handshake failed" in msg

suite "direct SSL: ALPN enforcement":
  proc ephemeralPort(): int =
    ## Grab an OS-assigned free port and release it for the s_server
    ## subprocess to bind. The PID-derived fixed range used before collided
    ## with any other process on the same host.
    var sock = newSocket()
    defer:
      sock.close()
    sock.bindAddr(Port(0))
    int(sock.getLocalAddr()[1])

  proc runAlpnProbe(opensslPath: string): string =
    ## Start `openssl s_server` without ALPN and try a direct-TLS connection.
    ## Returns the client's error message, or a descriptive message when the
    ## server exits early (e.g. bind failure) so the caller's check fails
    ## loudly instead of after the full retry window.
    let port = ephemeralPort()
    let p = startProcess(
      opensslPath,
      args = [
        "s_server",
        "-accept",
        $port,
        "-cert",
        CertDir / "server.crt",
        "-key",
        CertDir / "server.key",
        "-quiet",
      ],
      options = {poUsePath, poStdErrToStdOut},
    )
    defer:
      try:
        p.terminate()
      except OSError:
        discard
      p.close()
    proc runTest(): Future[string] {.async.} =
      var msg = ""
      for attempt in 0 ..< 30:
        let exitCode = p.peekExitCode()
        if exitCode != -1:
          msg =
            "openssl s_server exited before accepting connections (port " & $port &
            ", exit code " & $exitCode & ")"
          break
        try:
          let conn = await connect(testConfig(port, sslRequire, sslnDirect))
          await conn.close()
        except PgError as e:
          msg = e.msg
          if "ALPN" in msg:
            break
        await sleepMsAsync(200)
      msg

    # chronos waitFor returns `lent string` (a view into the future's frame);
    # materialize it into the owned result before the future is released.
    result = waitFor runTest()

  when hasAsyncDispatch:
    test "peer without the postgresql ALPN protocol is rejected":
      # `openssl s_server` without `-alpn` completes the handshake with no
      # ALPN selection (the PostgreSQL < 17 scenario), so the client reaches
      # assertAlpnPostgres and rejects the connection. Marked skipped (never
      # silently passed) when openssl is unavailable.
      let opensslPath = findExe("openssl")
      if opensslPath.len == 0:
        skip()
      else:
        let msg = runAlpnProbe(opensslPath)
        check "ALPN" in msg
        check "without ALPN" in msg

  when hasChronos:
    test "peer without the postgresql ALPN protocol is rejected (OpenSSL server)":
      # Same scenario as the asyncdispatch variant: the BearSSL client talks
      # to an OpenSSL server without ALPN support — the exact peer mix of a
      # PostgreSQL < 17 server. Marked skipped (never silently passed) when
      # openssl is unavailable.
      let opensslPath = findExe("openssl")
      if opensslPath.len == 0:
        skip()
      else:
        let msg = runAlpnProbe(opensslPath)
        check "ALPN" in msg
        check "without ALPN" in msg
