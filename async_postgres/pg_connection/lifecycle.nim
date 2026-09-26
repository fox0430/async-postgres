## Connection lifecycle: auth, single/multi-host connect, and close.
##
## Internal module: not part of the public API. Import the `pg_connection` hub instead.

import std/[options, random, sequtils, strutils, sysrand]

import ../[async_backend, pg_errors, pg_protocol, pg_auth]
from ../pg_bytes import readString
import pkg/nimcrypto/utils as ncutils
import types, buffer_io, ssl, simple_query, dsn

when defined(posix):
  import std/posix

when hasAsyncDispatch:
  import std/asyncnet

type AuthProgress = object ## What the authentication exchange has established so far.
  sawRequest: bool ## the server asked for credentials
  scramStarted: bool ## an AuthenticationSASL began a SCRAM exchange
  scramFinalVerified: bool ## the server proved it knows the password
  channelBound: bool ## SCRAM-SHA-256-PLUS was chosen

# Error message helpers

proc startupError(fields: seq[ErrorField]): ref PgConnectionError =
  ## The server refused the session; keep its fields so callers can tell a
  ## bad password from a server that is still starting up.
  (ref PgConnectionError)(
    msg: formatError(fields), serverError: newPgQueryError(fields)
  )

proc foldFailures(
    msg: string, attempts: seq[ref CatchableError], perHost = false
): ref PgConnectionError =
  ## One error summing up ``attempts`` (the last one as ``parent``): a
  ## ``PgSecurityError`` when every attempt was. ``perHost``: they are
  ## ``connect``'s hosts.
  let parent =
    if attempts.len > 0:
      attempts[^1]
    else:
      nil
  if attempts.len > 0 and attempts.allIt(it of PgSecurityError):
    result = (ref PgSecurityError)(msg: msg, parent: parent, attempts: attempts)
  else:
    result = (ref PgConnectionError)(msg: msg, parent: parent, attempts: attempts)
  result.setPerHost(perHost)

const PreV3MaxErrLen = 30000 # libpq's MAX_ERRLEN

proc readPreV3Error(conn: PgConnection): Future[string] {.async.} =
  ## The pre-3.0 error text, read up to its NUL (or the server's close), at
  ## most ``PreV3MaxErrLen`` bytes.
  # Offsets are relative to the text start: fillRecvBuf compacts the buffer.
  template start(): int =
    conn.recvBufStart + 1

  var scanned = 0 # text bytes already searched for the NUL
  while true:
    let avail = min(conn.recvBuf.len - start, PreV3MaxErrLen)
    let i = conn.recvBuf.toOpenArray(start + scanned, start + avail - 1).find(0'u8)
    if i >= 0:
      return readString(conn.recvBuf, start, scanned + i)
    scanned = avail
    if avail == PreV3MaxErrLen:
      break
    try:
      await conn.fillRecvBuf()
    except PgConnectionError:
      break # the postmaster closes right after the text
  return readString(conn.recvBuf, start, scanned)

proc checkPreV3Error(conn: PgConnection) {.async.} =
  ## Raise the first reply if it is a pre-3.0 error ('E' + NUL-terminated
  ## text): as in libpq, a length below 8 or above MAX_ERRLEN marks the old
  ## format.
  # Only the first reply: past it, a long v3 ErrorResponse keeps its fields.
  # Every v3 message has these 5 bytes: an 'E' the server closes short of them
  # can only be a pre-3.0 text.
  while conn.recvBuf.len - conn.recvBufStart < 5:
    try:
      await conn.fillRecvBuf()
    except PgConnectionError as e:
      if conn.recvBuf.len == conn.recvBufStart or
          conn.recvBuf[conn.recvBufStart] != byte('E'):
        raise e
      break
  let start = conn.recvBufStart
  if conn.recvBuf[start] != byte('E') or (
    conn.recvBuf.len - start >= 5 and
    decodeInt32(conn.recvBuf, start + 1) in 8 .. PreV3MaxErrLen
  ):
    return
  var text = await conn.readPreV3Error()
  text.stripLineEnd() # the postmaster ends it with '\n'
  # A failed fork clears with load; any other text is a pre-3.0 or non-PG peer.
  let forkFailed = text.startsWith(ForkFailureText)
  if text.len == 0:
    text = "server rejected the connection during startup"
  if forkFailed:
    raise newException(PgUnavailableError, text)
  raise newException(PgConnectionError, text)

# Authentication policy helpers

func saslAuthMethod(mech: string): Option[AuthMethod] =
  ## The require_auth method a SASL mechanism name maps to.
  case mech
  of "SCRAM-SHA-256":
    some(amScramSha256)
  of "SCRAM-SHA-256-PLUS":
    some(amScramSha256Plus)
  else:
    none(AuthMethod)

func permits(allowed: set[AuthMethod], authMethod: AuthMethod): bool =
  ## Whether require_auth ``allowed`` admits ``authMethod``: libpq's
  ## scram-sha-256 covers SCRAM-SHA-256-PLUS too.
  allowed.len == 0 or authMethod in allowed or
    (authMethod == amScramSha256Plus and amScramSha256 in allowed)

proc enforceAuthAllowed(
    authMethod: AuthMethod, allowed: set[AuthMethod], offered: string = ""
) {.raises: [PgSecurityError].} =
  if not allowed.permits(authMethod):
    var msg =
      "server requested auth method '" & $authMethod &
      "' which is not in require_auth allowlist " & $allowed
    if offered.len > 0:
      msg.add(" (server offered: ")
      msg.add(offered)
      msg.add(")")
    raise newException(PgSecurityError, msg)

proc checkAuthRequest(
    kind: BackendMessageKind, auth: AuthProgress, config: ConnConfig
) {.raises: [PgSecurityError].} =
  ## Refuse an authentication message ``config`` does not allow at this point
  ## of the exchange, as libpq's ``check_expected_areq`` does.
  case kind
  of bmkAuthenticationOk:
    if not auth.sawRequest:
      enforceAuthAllowed(amNone, config.requireAuth)
    # SCRAM authenticates both ways: skipping SASLFinal would accept a server
    # that never proved it knows the password.
    if auth.scramStarted and not auth.scramFinalVerified:
      raise newException(
        PgSecurityError,
        "server sent AuthenticationOk before completing SCRAM server " &
          "signature verification (possible downgrade or MITM)",
      )
    if config.channelBinding == cbRequire and not auth.channelBound:
      raise newException(
        PgSecurityError,
        "channel binding is required, but server authenticated client " &
          "without channel binding",
      )
  of bmkAuthenticationCleartextPassword, bmkAuthenticationMD5Password:
    let authMethod = if kind == bmkAuthenticationMD5Password: amMd5 else: amPassword
    enforceAuthAllowed(authMethod, config.requireAuth)
    # Refused before the password leaves: only SCRAM-SHA-256-PLUS binds.
    if config.channelBinding == cbRequire:
      raise newException(
        PgSecurityError,
        "channel binding is required, but server requested auth method '" & $authMethod &
          "'",
      )
  of bmkAuthenticationSASLContinue, bmkAuthenticationSASLFinal:
    if not auth.scramStarted:
      # A default scramState (empty nonce, zeroed signature) would let a forged
      # exchange through.
      let name =
        if kind == bmkAuthenticationSASLContinue:
          "AuthenticationSASLContinue"
        else:
          "AuthenticationSASLFinal"
      raise newException(
        PgSecurityError,
        "server sent " & name & " without a preceding " &
          "AuthenticationSASL (possible protocol violation or MITM)",
      )
  else:
    discard

proc filterSaslByRequireAuth*(
    mechs: seq[string], allowed: set[AuthMethod]
): seq[string] =
  ## Filter a server-offered SASL mechanism list by the client's
  ## `requireAuth` policy. An empty `allowed` set performs no filtering
  ## (matching libpq semantics when `require_auth` is unset).
  if allowed.len == 0:
    return mechs
  for m in mechs:
    let authMethod = saslAuthMethod(m)
    if authMethod.isSome and allowed.permits(authMethod.get):
      result.add(m)

proc plusOnlyError(offered: seq[string], why: string): ref PgConnectionError =
  ## SCRAM-SHA-256-PLUS is all that is left but channel binding is ``why``: a
  ## refusal when require_auth dropped SCRAM-SHA-256 from ``offered``.
  if "SCRAM-SHA-256" in offered:
    result = newException(
      PgSecurityError,
      "require_auth allows only SCRAM-SHA-256-PLUS, but channel binding is " & why,
    )
  else:
    result = newException(
      PgConnectionError,
      "channel binding is " & why & ", but server only offered SCRAM-SHA-256-PLUS",
    )

proc selectScramMechanism*(
    sslEnabled: bool,
    serverCertDer: openArray[byte],
    saslMechanisms: seq[string],
    mode: ChannelBindingMode,
    allowed: set[AuthMethod] = {},
): tuple[
  mechanism: string, cbType: string, cbData: seq[byte], cbSupportedButUnused: bool
] =
  ## Pick SCRAM mechanism/binding from the server's offer narrowed to
  ## ``allowed`` (raises if ``mode`` or ``allowed`` unsatisfied;
  ## ``cbSupportedButUnused`` → ``y,,`` else ``n,,``).
  if mode == cbRequire and not sslEnabled:
    raise newException(
      PgSecurityError, "channel binding is required, but SSL is not in use"
    )
  let offeredPlus = "SCRAM-SHA-256-PLUS" in saslMechanisms
  if offeredPlus and not sslEnabled:
    # A server offers -PLUS only over TLS, so TLS was stripped on the way
    # (libpq refuses this too).
    raise newException(
      PgSecurityError,
      "server offered SCRAM-SHA-256-PLUS authentication over a non-SSL connection",
    )
  let mechs = filterSaslByRequireAuth(saslMechanisms, allowed)
  if allowed.len > 0 and mechs.len == 0:
    raise newException(
      PgSecurityError,
      "server offered SASL mechanisms " & $saslMechanisms &
        " but none match require_auth allowlist " & $allowed,
    )
  let hasPlus = "SCRAM-SHA-256-PLUS" in mechs
  let hasScram = "SCRAM-SHA-256" in mechs
  case mode
  of cbRequire:
    if not hasPlus:
      raise newException(
        PgSecurityError,
        "channel binding is required, but server did not offer SCRAM-SHA-256-PLUS",
      )
    if serverCertDer.len == 0:
      raise newException(
        PgSecurityError,
        "channel binding is required, but server certificate is unavailable",
      )
    result.mechanism = "SCRAM-SHA-256-PLUS"
    result.cbType = "tls-server-end-point"
    result.cbData = computeTlsServerEndpoint(serverCertDer)
  of cbPrefer:
    if hasPlus and sslEnabled and serverCertDer.len > 0:
      result.mechanism = "SCRAM-SHA-256-PLUS"
      result.cbType = "tls-server-end-point"
      result.cbData = computeTlsServerEndpoint(serverCertDer)
    elif hasScram:
      result.mechanism = "SCRAM-SHA-256"
      # "y,," lets the server detect a MITM that stripped -PLUS; if it did
      # offer -PLUS and we just couldn't use it, "y,," would make it abort.
      result.cbSupportedButUnused = sslEnabled and not offeredPlus
    elif hasPlus:
      raise plusOnlyError(saslMechanisms, "unavailable")
    else:
      raise newException(
        PgConnectionError, "server doesn't support SCRAM-SHA-256 or SCRAM-SHA-256-PLUS"
      )
  of cbDisable:
    if hasScram:
      result.mechanism = "SCRAM-SHA-256"
    elif hasPlus:
      raise plusOnlyError(saslMechanisms, "disabled")
    else:
      raise newException(PgConnectionError, "server doesn't support SCRAM-SHA-256")

proc validateSecurityConfig(
    config: ConnConfig, overTcp: bool
) {.raises: [PgConfigError].} =
  ## Reject security settings no server can satisfy, before any dial. A
  ## requirement only the server's answer can fail stays a per-host
  ## ``PgSecurityError``. ``overTcp`` as in ``validateTlsConfig``.
  validateTlsConfig(config, overTcp)
  if config.sslMode == sslDisable and config.requireAuth == {amScramSha256Plus}:
    raise newException(
      PgConfigError,
      "require_auth allows only SCRAM-SHA-256-PLUS, which needs TLS, but " &
        "sslmode=disable never negotiates it",
    )
  case config.channelBinding
  of cbRequire:
    if config.sslMode == sslDisable:
      raise newException(
        PgConfigError,
        "channel_binding=require needs TLS, but sslmode=disable never negotiates it",
      )
    if not config.requireAuth.permits(amScramSha256Plus):
      raise newException(
        PgConfigError,
        "channel_binding=require needs SCRAM-SHA-256-PLUS, but require_auth " &
          $config.requireAuth & " does not allow it",
      )
  of cbDisable:
    if config.requireAuth == {amScramSha256Plus}:
      raise newException(
        PgConfigError,
        "require_auth allows only SCRAM-SHA-256-PLUS, but channel_binding=disable " &
          "never uses it",
      )
  of cbPrefer:
    discard

# Single-host bootstrap

proc hostConfig(config: ConnConfig, entry: HostEntry, validated = false): ConnConfig =
  ## ``config`` for dialing ``entry``, checked before any lookup or dial.
  ## ``validated``: ``connect`` already checked what every host shares.
  # Local mutable copy: ``validateConnConfig`` may normalize ``connectTimeout``.
  result = config

  # Validation below checks the scalars, but the caller dials ``entry``; a bare
  # config plus an explicit entry would otherwise trip the empty-host guard.
  # With a ``hosts`` list the scalars are re-derived there instead.
  if result.hosts.len == 0:
    result.host = entry.host
    result.hostaddr = entry.hostaddr
    result.port = entry.port

  # Re-check numeric / hostaddr / mTLS pairing here as well: `connect` validates
  # them in `wrapped`, but `connectToHost` is public and a direct caller would
  # otherwise bypass the parsers (port wrap, keepalive ``cint`` RangeDefect,
  # negative timeout footgun) or have certs silently dropped by a successful
  # sslAllow plaintext attempt.
  if not validated:
    validateConnConfig(result)
    validateClientCertConfig(result)
    # Validate before the sslAllow branch rewrites sslMode to sslDisable, which
    # would mask an sslnDirect conflict.
    validateDirectSslCompatible(result)
  validateSecurityConfig(result, overTcp = not isUnixSocket(entry.dialAddr))

  if entry.hostaddr.len > 0 and entry.hostaddr[0] == '/':
    # `hostaddr` is a numeric IP (libpq forces TCP/IP whenever it is
    # non-empty). A '/' value would otherwise select AF_UNIX via `dialAddr`
    # and skip TLS entirely. Unix sockets stay available via `host`.
    # Checked here (not just in `buildHosts`) so a directly constructed
    # `HostEntry`/`ConnConfig` cannot bypass the DSN parsers.
    raise newException(
      PgConfigError,
      "Invalid hostaddr: must be a numeric IP address, not a Unix socket path (use host for Unix sockets): " &
        entry.hostaddr,
    )

proc connectToHostImpl(
    config: ConnConfig,
    entry: HostEntry,
    allowTlsLeg: bool,
    targets: seq[DialTarget],
    reached: ref bool = nil,
    checked = false,
): Future[PgConnection] {.async.} =
  ## ``connectToHost``; ``allowTlsLeg`` marks sslmode=allow's TLS attempt,
  ## ``targets`` what to dial (empty: what ``entry`` resolves to). Sets
  ## ``reached`` once a dial succeeds. ``checked``: ``config`` is already
  ## ``hostConfig``'s.
  let config =
    if checked:
      config
    else:
      hostConfig(config, entry)

  # Without TLS there is no second leg: `negotiateSSL` leaves allow plaintext.
  if hasTls and config.sslMode == sslAllow and not allowTlsLeg:
    if config.channelBinding == cbRequire or config.requireAuth == {amScramSha256Plus}:
      # Channel binding and SCRAM-SHA-256-PLUS need TLS, so the plaintext leg
      # could only fail.
      return await connectToHostImpl(config, entry, true, targets, reached, true)
    # sslAllow: try plaintext first, then fall back to SSL (libpq semantics).
    # WARNING: This is vulnerable to MITM downgrade attacks. A network
    # attacker can force the first attempt to fail and then intercept
    # the SSL connection. Use sslRequire or stronger if security is needed.
    var plainConfig = config
    plainConfig.sslMode = sslDisable
    var plainErr: ref CatchableError
    try:
      return await connectToHostImpl(plainConfig, entry, false, targets, reached, true)
    except CancelledError as e:
      raise e
    except CatchableError as e:
      plainErr = e
    let plainErrMsg = oneLine(plainErr.msg)

    # Still allow, not require: an 'N' here is no refusal of required TLS.
    try:
      return await connectToHostImpl(config, entry, true, targets, reached, true)
    except CancelledError as e:
      raise e
    except PgConfigError as e:
      # A cert or key that will not load is the shared config's fault, never
      # hidden behind the plaintext leg's outcome.
      raise e
    except CatchableError as e:
      # A server still starting up refuses plaintext, then 'N' fails the SSL
      # leg: either leg failing transiently keeps the pair retryable.
      raise foldFailures(
        "sslmode=allow: plaintext attempt failed (" & plainErrMsg &
          ") and SSL fallback failed (" & oneLine(e.msg) & ")",
        @[plainErr, e],
      )

  let hostAddr = entry.dialAddr
  let hostPort = entry.port
  let isUnix = isUnixSocket(hostAddr)

  var conn: PgConnection

  let dialing =
    if targets.len > 0:
      dialTargets(targets)
    else:
      dialServer(hostAddr, hostPort)

  when hasChronos:
    let dialed = await dialing
    if reached != nil:
      reached[] = true
    let transport = dialed.stream
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
    conn = newPgConnection(hostAddr, hostPort, config)
    conn.transport = transport
    conn.cancelTarget = @[dialed.target]
  elif hasAsyncDispatch:
    let dialed = await dialing
    if reached != nil:
      reached[] = true
    let sock = dialed.stream
    when defined(posix):
      if not isUnix:
        try:
          when defined(nimdoc):
            # nim doc resolves nativesockets.SocketHandle to winlean on some
            # setups, so cast explicitly to satisfy the doc-time type check.
            configureTcpNoDelay(posix.SocketHandle(sock.getFd()))
            configureKeepalive(posix.SocketHandle(sock.getFd()), config)
          else:
            configureTcpNoDelay(sock.getFd())
            configureKeepalive(sock.getFd(), config)
        except CatchableError as e:
          sock.close()
          raise e
    conn = newPgConnection(hostAddr, hostPort, config)
    conn.socket = sock
    conn.cancelTarget = @[dialed.target]

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
    conn.markState(csAuthentication)
    await conn.checkPreV3Error()

    # Authentication loop
    var
      scramState: ScramState
      auth: AuthProgress

    block authLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkAuthenticationOk:
            checkAuthRequest(msg.kind, auth, config)
            break authLoop
          of bmkAuthenticationCleartextPassword:
            auth.sawRequest = true
            if not conn.sslEnabled:
              fireInsecureAuth(conn, amPassword)
            checkAuthRequest(msg.kind, auth, config)
            var pwMsg = encodePassword(config.password)
            try:
              await conn.sendMsg(pwMsg)
            finally:
              ncutils.burnMem(pwMsg)
          of bmkAuthenticationMD5Password:
            auth.sawRequest = true
            fireDeprecatedAuth(conn, amMd5)
            checkAuthRequest(msg.kind, auth, config)
            var hash = md5AuthHash(config.user, config.password, msg.md5Salt)
            var hashMsg = encodePassword(hash)
            burnStr(hash)
            try:
              await conn.sendMsg(hashMsg)
            finally:
              ncutils.burnMem(hashMsg)
          of bmkAuthenticationSASL:
            auth.sawRequest = true
            auth.scramStarted = true
            let choice = selectScramMechanism(
              conn.sslEnabled, conn.serverCertDer, msg.saslMechanisms,
              config.channelBinding, config.requireAuth,
            )
            let chosen = saslAuthMethod(choice.mechanism).get
            auth.channelBound = chosen == amScramSha256Plus
            # Defensive: selectScramMechanism only picks from the require_auth
            # filtered offer; this guards against a future fallback past it.
            enforceAuthAllowed(chosen, config.requireAuth, $msg.saslMechanisms)
            let clientFirst = scramClientFirstMessage(
              config.user, scramState, choice.cbType, choice.cbData,
              choice.cbSupportedButUnused,
            )
            await conn.sendMsg(encodeSASLInitialResponse(choice.mechanism, clientFirst))
          of bmkAuthenticationSASLContinue:
            checkAuthRequest(msg.kind, auth, config)
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
            checkAuthRequest(msg.kind, auth, config)
            let ok = scramVerifyServerFinal(msg.saslFinalData, scramState)
            ncutils.burnMem(scramState.serverSignature)
            if not ok:
              raise newException(
                PgSecurityError, "SCRAM server signature verification failed"
              )
            auth.scramFinalVerified = true
          of bmkErrorResponse:
            raise startupError(msg.errorFields)
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
            conn.markReady()
            break readyLoop
          of bmkErrorResponse:
            raise startupError(msg.errorFields)
          else:
            discard
        await conn.fillRecvBuf()

    conn.createdAt = Moment.now()
    return conn
  except CatchableError as e:
    await conn.closeTransport()
    raise e

proc connectToHost*(config: ConnConfig, entry: HostEntry): Future[PgConnection] =
  ## Connect to a single host (dial ``hostaddr`` else ``host``; verify via ``host``).
  ##
  ## Low-level dial primitive. Unlike ``connect`` it does **not** apply
  ## ``targetSessionAttrs``, ``connectTimeout`` or connect tracing, and it
  ## moves on to the host's next address only when a dial fails.
  ##
  ## On Unix sockets TLS is skipped (libpq parity); if ``sslCert`` is set a
  ## stderr warning is emitted because the client certificate is not sent.
  connectToHostImpl(config, entry, false, @[])

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
  conn.markClosed()
  conn.resetWireState()
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
    config: ConnConfig,
    entry: HostEntry,
    attrs: TargetSessionAttrs,
    target: DialTarget,
    reached: ref bool,
): Future[PgConnection] {.async.} =
  ## Dial ``target`` and verify ``attrs``; nil = wrong role (already closed).
  let conn = await connectToHostImpl(config, entry, false, @[target], reached, true)
  if attrs == tsaAny or await conn.matchesOrClose(attrs):
    return conn
  return nil

proc attemptHostTimed(
    config: ConnConfig,
    entry: HostEntry,
    attrs: TargetSessionAttrs,
    targets: seq[DialTarget],
): Future[PgConnection] {.async.} =
  ## ``attemptHost`` at each of ``targets`` in turn, each within its own
  ## ``connectTimeout`` (libpq semantics). Only a failed dial or a timeout
  ## moves on: past the dial, the host's one server has answered.
  # The loop is inlined, not a proc per address: an extra future layer would
  # delay chronos cancellation (see `dialServer`).
  var failures: seq[ref CatchableError]
  var errors: seq[string]
  var timedOut = false
  for target in targets:
    let reached = new(bool)
    try:
      if config.connectTimeout == default(Duration):
        return await attemptHost(config, entry, attrs, target, reached)
      when hasAsyncDispatch:
        # asyncdispatch's wait() cannot cancel the attempt: close a connection
        # it produces after the timeout instead of leaking it.
        let attempt = attemptHost(config, entry, attrs, target, reached)
        return await attempt.wait(
          config.connectTimeout,
          onOrphan = proc(fut: Future[PgConnection]) =
            if fut.completed():
              asyncSpawn (
                proc() {.async.} =
                  try:
                    let orphan = fut.read()
                    if orphan != nil:
                      # Nobody ever held this one: the library dialled it and
                      # the library discards it (see `matchesOrClose`).
                      await orphan.closeImpl(byUser = false)
                  except CatchableError:
                    discard
              )()
          ,
        )
      else:
        return await attemptHost(config, entry, attrs, target, reached).wait(
          config.connectTimeout
        )
    except CancelledError as e:
      raise e
    except PgConfigError as e:
      raise e
    except CatchableError as e:
      # A server's verdict stands alone: summed with a refused dial before it,
      # a bad password would look transient.
      if targets.len == 1 or (reached[] and not (e of AsyncTimeoutError)):
        raise e
      timedOut = timedOut or e of AsyncTimeoutError
      failures.add(e)
      errors.add(target.shown & ": " & oneLine(e.msg))
  if timedOut:
    # Raw, as `connect` promises for a single host's timeout.
    raise newException(AsyncTimeoutError, errors.join("; "), failures[^1])
  raise foldFailures(errors.join("; "), failures)

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
  ## Connect with multi-host failover, ``targetSessionAttrs``, ``connectTimeout``
  ## per address a host resolves to.
  ## Per-host failures fold into one ``PgConnectionError``; a ``PgConfigError`` escapes the fold.
  ## Unlike libpq, a failed authentication or security check moves on to the next
  ## host; a host's next address is tried only after a failed dial or a timeout.
  ## Its ``attempts`` hold each host's latest failure, a mismatch included
  ## (``serverErrors`` collects their refusals), its ``parent`` the last one; a
  ## ``PgSecurityError`` when every host was.
  ## A single host's timeout raises ``AsyncTimeoutError`` (not folded).
  # Local mutable copy: ``validateConnConfig`` may normalize ``connectTimeout``.
  var config = config
  proc perform(hosts: seq[HostEntry]): Future[PgConnection] {.async.} =
    # `hosts` is already ordered by the caller (shuffled under lbhRandom), so
    # both the preferStandby two-pass loop and the single-pass loop below share
    # one order.
    # Reject sslnDirect + weak sslmode once — a per-host check would repeat the
    # identical error across the aggregate.
    validateDirectSslCompatible(config)
    # A shared-config fault raised per host (e.g. a PEM that will not load)
    # would repeat on every entry and, folded, look retryable: only
    # `PgConfigError` escapes; per-host outcomes still fail over.
    template reraiseConfigFault(err: ref CatchableError) =
      if err of PgConfigError:
        raise err

    # One line each: asyncdispatch cuts a message at the first traceback in it.
    var errors: seq[string]
    # Each host's latest failure (a preferStandby second pass overwrites the
    # first), so every host that failed is judged by its last attempt.
    var failures = newSeq[ref CatchableError](hosts.len)
    # Each host is checked and resolved once, on first use: the preferStandby
    # passes share the lookup, a blocking call.
    var prepared =
      newSeq[tuple[config: ConnConfig, targets: seq[DialTarget]]](hosts.len)

    template prepare(i: int) =
      prepared[i].config = hostConfig(config, hosts[i], validated = true)
      # After the check: a config fault must not hide behind a failed lookup.
      prepared[i].targets = resolveTargets(hosts[i].dialAddr, hosts[i].port)

    template attempt(i: int, attrs: TargetSessionAttrs): untyped =
      attemptHostTimed(prepared[i].config, hosts[i], attrs, prepared[i].targets)

    if config.targetSessionAttrs == tsaPreferStandby:
      # First pass: look for a standby
      for i, entry in hosts:
        try:
          prepare(i)
          let conn = await attempt(i, tsaStandby)
          if conn != nil:
            return conn
        except CancelledError as e:
          raise e
        except CatchableError as e:
          reraiseConfigFault(e)
          failures[i] = e
          errors.add(entry.displayHost & ":" & $entry.port & ": " & oneLine(e.msg))
      # Second pass: accept any server
      for i, entry in hosts:
        if prepared[i].targets.len == 0:
          continue # its check or lookup failed in the first pass
        try:
          return await attempt(i, tsaAny)
        except CancelledError as e:
          raise e
        except CatchableError as e:
          reraiseConfigFault(e)
          failures[i] = e
          errors.add(entry.displayHost & ":" & $entry.port & ": " & oneLine(e.msg))
    else:
      for i, entry in hosts:
        try:
          prepare(i)
          let conn = await attempt(i, config.targetSessionAttrs)
          if conn != nil:
            return conn
          # A failover may promote a standby or demote a primary, so the
          # mismatch retries like a lost connection.
          let mismatch = newException(
            PgUnavailableError,
            "server does not match target_session_attrs " & $config.targetSessionAttrs,
          )
          failures[i] = mismatch
          errors.add(entry.displayHost & ":" & $entry.port & ": " & mismatch.msg)
        except CancelledError as e:
          raise e
        except CatchableError as e:
          reraiseConfigFault(e)
          failures[i] = e
          errors.add(entry.displayHost & ":" & $entry.port & ": " & oneLine(e.msg))

    # With a single host there is no failover. Preserve the contract that its
    # `connectTimeout` surfaces as a raw `AsyncTimeoutError` (callers and the
    # pool branch on the type) instead of being folded into the aggregate
    # `PgConnectionError` below — which only makes sense across multiple hosts.
    if hosts.len == 1 and failures[0] of AsyncTimeoutError:
      raise failures[0]
    var attempts: seq[ref CatchableError]
    for f in failures:
      if f != nil:
        attempts.add(f)
    raise foldFailures(
      "Could not connect to any host: " & errors.join("; "), attempts, perHost = true
    )

  proc wrapped(): Future[PgConnection] {.async.} =
    # ConnConfig may be built or mutated without passing through the parsers'
    # validation — re-check here so every connect path rejects bad numeric /
    # hostaddr / cert config (``initConnConfig`` alone is not enough).
    validateConnConfig(config)
    validateClientCertConfig(config)
    # sslrootcert is left to each host's attempt: a Unix socket needs none.
    validateSecurityConfig(config, overTcp = false)
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
      # `connectTimeout` is enforced per address inside `attemptHostTimed`, so
      # `perform()` is awaited directly here — no outer total-timeout wrapper.
      conn = await perform(hosts)
      conn.tracer = config.tracer
    return conn

  wrapped()

proc connect*(dsn: string): Future[PgConnection] =
  ## Shorthand for ``connect(parseDsn(dsn))``.
  connect(parseDsn(dsn))
