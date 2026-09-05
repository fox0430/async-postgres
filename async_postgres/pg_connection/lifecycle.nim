## Connection lifecycle: auth, single/multi-host connect, and close.

import std/[options, random, strutils, sysrand, tables]

import ../[async_backend, pg_errors, pg_protocol, pg_auth]
import pkg/nimcrypto/utils as ncutils
import types, buffer_io, ssl, simple_query, dsn

when defined(posix):
  import std/posix

when hasAsyncDispatch:
  import std/asyncnet
  from std/nativesockets import Domain, SockType, Protocol

# Authentication policy helpers

proc enforceAuthAllowed(
    authMethod: AuthMethod, allowed: set[AuthMethod], offered: string = ""
) {.raises: [PgConnectionError].} =
  if allowed.len > 0 and authMethod notin allowed:
    var msg =
      "server requested auth method '" & $authMethod &
      "' which is not in require_auth allowlist " & $allowed
    if offered.len > 0:
      msg.add(" (server offered: ")
      msg.add(offered)
      msg.add(")")
    raise newException(PgConnectionError, msg)

proc filterSaslByRequireAuth*(
    mechs: seq[string], allowed: set[AuthMethod]
): seq[string] =
  ## Filter a server-offered SASL mechanism list by the client's
  ## `requireAuth` policy. An empty `allowed` set performs no filtering
  ## (matching libpq semantics when `require_auth` is unset).
  if allowed.len == 0:
    return mechs
  for m in mechs:
    if m == "SCRAM-SHA-256-PLUS" and amScramSha256Plus in allowed:
      result.add(m)
    elif m == "SCRAM-SHA-256" and amScramSha256 in allowed:
      result.add(m)

proc selectScramMechanism*(
    sslEnabled: bool,
    serverCertDer: openArray[byte],
    saslMechanisms: seq[string],
    mode: ChannelBindingMode,
): tuple[
  mechanism: string, cbType: string, cbData: seq[byte], cbSupportedButUnused: bool
] =
  ## Pick SCRAM mechanism/binding (raises if ``mode`` unsatisfied; ``cbSupportedButUnused`` → ``y,,`` else ``n,,``).
  let serverHasPlus = "SCRAM-SHA-256-PLUS" in saslMechanisms
  let serverHasScram = "SCRAM-SHA-256" in saslMechanisms
  let canUsePlus = sslEnabled and serverCertDer.len > 0 and serverHasPlus
  case mode
  of cbRequire:
    if not sslEnabled:
      raise newException(
        PgConnectionError, "channel binding is required, but SSL is not in use"
      )
    if not serverHasPlus:
      raise newException(
        PgConnectionError,
        "channel binding is required, but server did not offer SCRAM-SHA-256-PLUS",
      )
    if serverCertDer.len == 0:
      raise newException(
        PgConnectionError,
        "channel binding is required, but server certificate is unavailable",
      )
    result.mechanism = "SCRAM-SHA-256-PLUS"
    result.cbType = "tls-server-end-point"
    result.cbData = computeTlsServerEndpoint(serverCertDer)
  of cbPrefer:
    if canUsePlus:
      result.mechanism = "SCRAM-SHA-256-PLUS"
      result.cbType = "tls-server-end-point"
      result.cbData = computeTlsServerEndpoint(serverCertDer)
    elif serverHasScram:
      result.mechanism = "SCRAM-SHA-256"
      # TLS is in use but plain SCRAM-SHA-256 was selected. Only signal "y,,"
      # when the server did not offer SCRAM-SHA-256-PLUS: that is the genuine
      # downgrade case (a MITM may have stripped -PLUS from the offered list),
      # and the real server detects it because it knows it offered -PLUS. If the
      # server *did* offer -PLUS but we couldn't use it (e.g. the certificate was
      # unavailable), sending "y,," would make the server abort with a channel
      # binding negotiation error, so fall back to "n,," instead.
      result.cbSupportedButUnused = sslEnabled and not serverHasPlus
    else:
      raise newException(
        PgConnectionError, "server doesn't support SCRAM-SHA-256 or SCRAM-SHA-256-PLUS"
      )
  of cbDisable:
    if serverHasScram:
      result.mechanism = "SCRAM-SHA-256"
    else:
      raise newException(
        PgConnectionError,
        "channel binding is disabled, but server only offered SCRAM-SHA-256-PLUS",
      )

# Single-host bootstrap

proc connectToHost*(
    config: ConnConfig, entry: HostEntry
): Future[PgConnection] {.async.} =
  ## Connect to single host (dial ``hostaddr`` else ``host``; verify via ``host``).

  # Validate before the sslAllow branch rewrites sslMode to sslDisable, which
  # would mask an sslnDirect conflict.
  validateDirectSslCompatible(config)

  if config.sslMode == sslAllow:
    # sslAllow: try plaintext first, then fall back to SSL (libpq semantics).
    # WARNING: This is vulnerable to MITM downgrade attacks. A network
    # attacker can force the first attempt to fail and then intercept
    # the SSL connection. Use sslRequire or stronger if security is needed.
    var plainConfig = config
    plainConfig.sslMode = sslDisable
    var plainErrMsg = ""
    try:
      return await connectToHost(plainConfig, entry)
    except CancelledError as e:
      raise e
    except CatchableError as e:
      # Keep only the first line: asyncdispatch appends an async traceback and
      # "Exception message:" prefix to e.msg, which would make the combined
      # error below unreadable. Assumes PgConnectionError messages are single-line.
      plainErrMsg = e.msg.split('\n')[0]

    var sslConfig = config
    sslConfig.sslMode = sslRequire
    try:
      return await connectToHost(sslConfig, entry)
    except CancelledError as e:
      raise e
    except CatchableError as e:
      let sslErrMsg = e.msg.split('\n')[0]
      raise newException(
        PgConnectionError,
        "sslmode=allow: plaintext attempt failed (" & plainErrMsg &
          ") and SSL fallback failed (" & sslErrMsg & ")",
        e,
      )

  let hostAddr = entry.dialAddr
  let hostPort = entry.port
  let isUnix = isUnixSocket(hostAddr)

  var conn: PgConnection

  when hasChronos:
    let transport =
      if isUnix:
        when defined(posix):
          await connect(initTAddress(unixSocketPath(hostAddr, hostPort)))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        let addresses = resolveTAddress(hostAddr, Port(hostPort))
        if addresses.len == 0:
          raise newException(PgConnectionError, "Could not resolve host: " & hostAddr)
        await connect(addresses[0])
    when defined(posix):
      if not isUnix:
        try:
          configureTcpNoDelay(posix.SocketHandle(transport.fd))
          configureKeepalive(posix.SocketHandle(transport.fd), config)
        except CatchableError as e:
          try:
            await noCancel transport.closeWait()
          except CatchableError:
            discard
          raise newException(PgConnectionError, e.msg, e)
    conn = PgConnection(
      transport: transport,
      recvBuf: @[],
      state: csConnecting,
      serverParams: initTable[string, string](),
      host: hostAddr,
      port: hostPort,
      config: config,
      notifyMaxQueue: 1024,
      stmtCacheCapacity: 256,
      listenReconnectMaxAttempts: 10,
      listenReconnectMaxBackoff: 30,
    )
  elif hasAsyncDispatch:
    let sock =
      if isUnix:
        when defined(posix):
          newAsyncSocket(
            Domain.AF_UNIX, SockType.SOCK_STREAM, Protocol.IPPROTO_IP, buffered = false
          )
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        newAsyncSocket(buffered = false)
    try:
      if isUnix:
        when defined(posix):
          await sock.connectUnix(unixSocketPath(hostAddr, hostPort))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        await sock.connect(hostAddr, Port(hostPort))
        when defined(posix):
          when defined(nimdoc):
            # nim doc resolves nativesockets.SocketHandle to winlean on some
            # setups, so cast explicitly to satisfy the doc-time type check.
            configureTcpNoDelay(posix.SocketHandle(sock.getFd()))
            configureKeepalive(posix.SocketHandle(sock.getFd()), config)
          else:
            configureTcpNoDelay(sock.getFd())
            configureKeepalive(sock.getFd(), config)
    except CatchableError:
      sock.close()
      raise
    conn = PgConnection(
      socket: sock,
      recvBuf: @[],
      state: csConnecting,
      serverParams: initTable[string, string](),
      host: hostAddr,
      port: hostPort,
      config: config,
      notifyMaxQueue: 1024,
      stmtCacheCapacity: 256,
      listenReconnectMaxAttempts: 10,
      listenReconnectMaxBackoff: 30,
    )

  try:
    # SSL negotiation (before StartupMessage). Unix sockets skip it (libpq 17
    # parity: sslnegotiation is ignored for AF_UNIX). Certificate verification
    # must use the host *name*, never the dialed hostaddr, and must be per-entry:
    # with multi-host failover config.host only reflects the first entry.
    if isUnix and config.sslCert.len > 0:
      # Unix sockets skip TLS regardless of sslmode, so a configured client
      # cert is silently dropped — warn like the sslPrefer 'N' fallback path.
      warnStderr "pg_connection: client certificate will NOT be sent over Unix-socket connection (TLS is skipped for AF_UNIX)"
    if config.sslMode != sslDisable and not isUnix:
      await negotiateSSL(conn, config, entry.host)

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
    var
      scramState: ScramState
      sawAuthRequest = false

    var
      # SCRAM mutual authentication: once a SASL exchange has begun the client
      # MUST verify the server's signature (AuthenticationSASLFinal) before
      # accepting AuthenticationOk. Otherwise a malicious server / MITM could
      # skip SASLFinal and be accepted without proving it knows the password,
      # defeating SCRAM's mutual-auth guarantee.
      scramStarted = false
      scramFinalVerified = false

    block authLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkAuthenticationOk:
            if not sawAuthRequest:
              enforceAuthAllowed(amNone, config.requireAuth)
            if scramStarted and not scramFinalVerified:
              raise newException(
                PgConnectionError,
                "server sent AuthenticationOk before completing SCRAM server " &
                  "signature verification (possible downgrade or MITM)",
              )
            break authLoop
          of bmkAuthenticationCleartextPassword:
            sawAuthRequest = true
            if not conn.sslEnabled:
              fireInsecureAuth(conn, amPassword)
            enforceAuthAllowed(amPassword, config.requireAuth)
            var pwMsg = encodePassword(config.password)
            try:
              await conn.sendMsg(pwMsg)
            finally:
              ncutils.burnMem(pwMsg)
          of bmkAuthenticationMD5Password:
            sawAuthRequest = true
            fireDeprecatedAuth(conn, amMd5)
            enforceAuthAllowed(amMd5, config.requireAuth)
            var hash = md5AuthHash(config.user, config.password, msg.md5Salt)
            var hashMsg = encodePassword(hash)
            burnStr(hash)
            try:
              await conn.sendMsg(hashMsg)
            finally:
              ncutils.burnMem(hashMsg)
          of bmkAuthenticationSASL:
            sawAuthRequest = true
            scramStarted = true
            let filtered =
              filterSaslByRequireAuth(msg.saslMechanisms, config.requireAuth)
            if config.requireAuth.len > 0 and filtered.len == 0:
              raise newException(
                PgConnectionError,
                "server offered SASL mechanisms " & $msg.saslMechanisms &
                  " but none match require_auth allowlist " & $config.requireAuth,
              )
            let choice = selectScramMechanism(
              conn.sslEnabled, conn.serverCertDer, filtered, config.channelBinding
            )
            let chosen =
              if choice.mechanism == "SCRAM-SHA-256-PLUS":
                amScramSha256Plus
              else:
                amScramSha256
            # Defensive: filterSaslByRequireAuth above already dropped
            # disallowed mechanisms, so `chosen` is guaranteed allowed. This
            # re-check guards against future changes to selectScramMechanism
            # introducing a bypass (e.g. a fallback that reaches past the
            # filtered list).
            enforceAuthAllowed(chosen, config.requireAuth, $msg.saslMechanisms)
            let clientFirst = scramClientFirstMessage(
              config.user, scramState, choice.cbType, choice.cbData,
              choice.cbSupportedButUnused,
            )
            await conn.sendMsg(encodeSASLInitialResponse(choice.mechanism, clientFirst))
          of bmkAuthenticationSASLContinue:
            if not scramStarted:
              # No AuthenticationSASL preceded this. scramState is still
              # default-initialized (clientNonce == ""), which would make the
              # nonce-binding check in scramClientFinalMessage pass vacuously
              # (combinedNonce.startsWith("") is always true). Reject instead so
              # a malicious server / MITM cannot inject a forged SCRAM exchange.
              raise newException(
                PgConnectionError,
                "server sent AuthenticationSASLContinue without a preceding " &
                  "AuthenticationSASL (possible protocol violation or MITM)",
              )
            var clientFinal = scramClientFinalMessage(
              config.password, msg.saslData, scramState,
              config.effectiveMaxScramIterations,
            )
            var saslMsg = encodeSASLResponse(clientFinal)
            ncutils.burnMem(clientFinal)
            try:
              await conn.sendMsg(saslMsg)
            finally:
              ncutils.burnMem(saslMsg)
          of bmkAuthenticationSASLFinal:
            if not scramStarted:
              # As with SASLContinue: without a preceding AuthenticationSASL the
              # serverSignature in scramState is zeroed, so reject rather than
              # let a forged exchange reach scramVerifyServerFinal.
              raise newException(
                PgConnectionError,
                "server sent AuthenticationSASLFinal without a preceding " &
                  "AuthenticationSASL (possible protocol violation or MITM)",
              )
            let ok = scramVerifyServerFinal(msg.saslFinalData, scramState)
            ncutils.burnMem(scramState.serverSignature)
            if not ok:
              raise newException(
                PgConnectionError, "SCRAM server signature verification failed"
              )
            scramFinalVerified = true
          of bmkErrorResponse:
            raise newException(PgConnectionError, formatError(msg.errorFields))
          else:
            discard
        await conn.fillRecvBuf()

    # Collect ParameterStatus, BackendKeyData until ReadyForQuery
    block readyLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          # ParameterStatus is recorded into conn.serverParams centrally by
          # nextMessage, so it is never returned here.
          case msg.kind
          of bmkBackendKeyData:
            conn.pid = msg.backendPid
            conn.secretKey = msg.backendSecretKey
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            break readyLoop
          of bmkErrorResponse:
            raise newException(PgConnectionError, formatError(msg.errorFields))
          else:
            discard
        await conn.fillRecvBuf()

    conn.createdAt = Moment.now()
    return conn
  except CatchableError as e:
    await conn.closeTransport()
    raise e

# Close

proc closeImpl*(conn: PgConnection, byUser: bool): Future[void] {.async.} =
  ## Close with ownership flag: ``byUser=false`` keeps ``PgConnectionError`` for pool evictions.
  # Set ``closedByUser`` before first suspension so racing ``waitNotification`` sees it.
  if byUser:
    conn.closedByUser = true
  # Stop background listen pump if running
  if conn.listenTask != nil and not conn.listenTask.finished:
    when hasAsyncDispatch:
      # cancelAndWait is a no-op here: signal stop so the reconnect loop bails
      # instead of re-LISTENing into an orphan socket, close the transport to
      # break its recv, then await the pump. A pump inside connect() cannot be
      # cancelled, so the wait is bounded and it is orphaned on timeout; the
      # stop flag stays set to disarm reconnectInPlace's graft.
      conn.listenStopRequested = true
      let pump = conn.listenTask
      await conn.closeTransport()
      var pumpStopped = false
      try:
        await pump.wait(milliseconds(listenReconnectStopWaitMs))
        pumpStopped = true
      except AsyncTimeoutError:
        discard
      except CatchableError:
        pumpStopped = true
      if pumpStopped:
        conn.listenStopRequested = false
    else:
      await cancelAndWait(conn.listenTask)
  conn.listenTask = nil
  # Only send Terminate if we haven't already detected the connection is dead
  if conn.state != csClosed and conn.isConnected():
    try:
      await conn.sendMsg(encodeTerminate())
    except CatchableError:
      discard
  conn.state = csClosed
  conn.heldSessionLocks = 0
  conn.sessionLockDirty = false
  conn.failNotifyWaiter() # `closedByUser` maps it to PgStateError
  await conn.closeTransport()

proc close*(conn: PgConnection): Future[void] =
  ## Idempotent close; asyncdispatch may orphan blocking pump until it unwinds. Waiter → ``PgStateError``.
  conn.closeImpl(byUser = true)

# Multi-host connect

proc matchesOrClose(
    conn: PgConnection, attrs: TargetSessionAttrs
): Future[bool] {.async.} =
  ## Probe against ``attrs``; non-match/failure closes ``conn`` (no leak).
  # `byUser = false` throughout: the application never saw a handle for this
  # probe, so discarding it must not stamp `closedByUser`.
  try:
    if await conn.checkSessionAttrs(attrs):
      return true
  except CatchableError as e:
    # A cancelled connection's bare awaits re-raise immediately, so force the
    # teardown to run under chronos; close() swallows its own errors.
    when hasChronos:
      await noCancel conn.closeImpl(byUser = false)
    else:
      await conn.closeImpl(byUser = false)
    # Re-raise the captured exception rather than a bare `raise`: if close()
    # suspended, the resumed coroutine has no "current exception" and a bare
    # raise dies with ReraiseDefect ("no exception to reraise").
    raise e
  await conn.closeImpl(byUser = false)
  return false

proc attemptHost(
    config: ConnConfig, entry: HostEntry, attrs: TargetSessionAttrs
): Future[PgConnection] {.async.} =
  ## Dial host and verify ``attrs``; nil = wrong role (already closed).
  let conn = await connectToHost(config, entry)
  if attrs == tsaAny or await conn.matchesOrClose(attrs):
    return conn
  return nil

proc attemptHostTimed(
    config: ConnConfig, entry: HostEntry, attrs: TargetSessionAttrs
): Future[PgConnection] {.async.} =
  ## ``attemptHost`` with per-host ``connectTimeout`` (libpq semantics).
  if config.connectTimeout == default(Duration):
    return await attemptHost(config, entry, attrs)
  when hasAsyncDispatch:
    # asyncdispatch's wait() cannot cancel the attempt: on timeout it keeps
    # running in the background. If it later produces a live connection nobody
    # is waiting for it, so close the orphan instead of leaking a socket and a
    # server slot. onOrphan on wait() registers the cleanup so the caller
    # doesn't need a separate addCallback. (chronos's wait() cancels the
    # attempt, and connectToHost / matchesOrClose tear down their transports
    # on the way out.)
    let attempt = attemptHost(config, entry, attrs)
    return await attempt.wait(
      config.connectTimeout,
      onOrphan = proc(fut: Future[PgConnection]) =
        if fut.completed():
          asyncSpawn (
            proc() {.async.} =
              try:
                let orphan = fut.read()
                if orphan != nil:
                  # Nobody ever held this one: the library dialled it and the
                  # library discards it (see `matchesOrClose`).
                  await orphan.closeImpl(byUser = false)
              except CatchableError:
                discard
          )()
      ,
    )
  else:
    return await attemptHost(config, entry, attrs).wait(config.connectTimeout)

proc orderedHosts*(config: ConnConfig): seq[HostEntry] =
  ## Hosts per ``loadBalanceHosts`` (``lbhRandom`` shuffles via ``urandom``; no global state).
  result = config.getHosts()
  if config.loadBalanceHosts == lbhRandom and result.len > 1:
    let bytes =
      try:
        urandom(8)
      except OSError as e:
        raise newException(
          PgConnectionError,
          "Failed to read entropy for load_balance_hosts=random: " & e.msg,
        )
    var seed: uint64
    for b in bytes:
      seed = (seed shl 8) or b
    # initRand expects a signed seed; use a bit-preserving cast so any
    # 64-bit random value is valid, not just values <= int64.high.
    var rng = initRand(cast[int64](seed))
    rng.shuffle(result)

proc connect*(config: ConnConfig): Future[PgConnection] =
  ## Connect with multi-host failover, ``targetSessionAttrs``, per-host ``connectTimeout``.
  proc perform(hosts: seq[HostEntry]): Future[PgConnection] {.async.} =
    # `hosts` is already ordered by the caller (shuffled under lbhRandom), so
    # both the preferStandby two-pass loop and the single-pass loop below share
    # one order.
    # Reject sslnDirect + weak sslmode once — a per-host check would repeat the
    # identical error across the aggregate. Other host-independent SSL errors
    # (missing sslrootcert, verify-full without a host name) are still raised
    # from negotiateSSL and folded per host.
    validateDirectSslCompatible(config)
    var errors: seq[string]
    # With a single host there is no failover. Preserve the contract that its
    # `connectTimeout` surfaces as a raw `AsyncTimeoutError` (callers and the
    # pool branch on the type) instead of being folded into the aggregate
    # `PgConnectionError` below — which only makes sense across multiple hosts.
    var lastFailure: ref CatchableError

    if config.targetSessionAttrs == tsaPreferStandby:
      # First pass: look for a standby
      for entry in hosts:
        try:
          let conn = await attemptHostTimed(config, entry, tsaStandby)
          if conn != nil:
            return conn
        except CancelledError as e:
          raise e
        except CatchableError as e:
          lastFailure = e
          errors.add(entry.displayHost & ":" & $entry.port & ": " & e.msg)
      # Second pass: accept any server
      for entry in hosts:
        try:
          return await attemptHostTimed(config, entry, tsaAny)
        except CancelledError as e:
          raise e
        except CatchableError as e:
          lastFailure = e
          errors.add(entry.displayHost & ":" & $entry.port & ": " & e.msg)
    else:
      for entry in hosts:
        try:
          let conn = await attemptHostTimed(config, entry, config.targetSessionAttrs)
          if conn != nil:
            return conn
          errors.add(
            entry.displayHost & ":" & $entry.port &
              ": server does not match target_session_attrs " &
              $config.targetSessionAttrs
          )
        except CancelledError as e:
          raise e
        except CatchableError as e:
          lastFailure = e
          errors.add(entry.displayHost & ":" & $entry.port & ": " & e.msg)

    if hosts.len == 1 and lastFailure != nil and lastFailure of AsyncTimeoutError:
      raise lastFailure
    raise newException(
      PgConnectionError, "Could not connect to any host: " & errors.join("; ")
    )

  proc wrapped(): Future[PgConnection] {.async.} =
    # ConnConfig may be built or mutated without passing through the parsers'
    # validation — re-check here so every connect path rejects bad cert config.
    try:
      validateClientCertConfig(config)
    except PgError as e:
      raise newException(PgConnectionError, e.msg, e)
    # Compute the ordered host list once so the trace and the actual connection
    # attempts see the same order under lbhRandom.
    let hosts = config.orderedHosts()
    var conn: PgConnection
    withTracing(
      config.tracer,
      onConnectStart,
      onConnectEnd,
      TraceConnectStartData(hosts: hosts),
      TraceConnectEndData,
      TraceConnectEndData(conn: conn),
    ):
      # `connectTimeout` is enforced per host inside `attemptHostTimed`, so
      # `perform()` is awaited directly here — no outer total-timeout wrapper.
      conn = await perform(hosts)
      conn.tracer = config.tracer
    return conn

  wrapped()

proc connect*(dsn: string): Future[PgConnection] =
  ## Shorthand for ``connect(parseDsn(dsn))``.
  connect(parseDsn(dsn))
