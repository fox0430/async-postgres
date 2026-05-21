## Connection lifecycle: open, authenticate, fail over across hosts, close.
##
## Contains:
## - Authentication helpers (`enforceAuthAllowed`, `filterSaslByRequireAuth`,
##   `selectScramMechanism`) that the auth loop in `connectToHost` consumes.
## - `connectToHost` — the single-host bootstrap: socket → SSL → startup →
##   auth loop → ParameterStatus/BackendKeyData → extension OID discovery.
## - `connect` — the public entry: multi-host failover, `targetSessionAttrs`
##   handling, optional `connectTimeout`, top-level connect tracing.
## - `close` — idempotent teardown: stop background listen pump, send
##   `Terminate`, drop transport handles.
##
## Imports `simple_query` for `checkSessionAttrs` (failover probe).
## Re-exported through `pg_connection.nim`.

import std/[options, strutils, tables]

import ../[async_backend, pg_errors, pg_protocol, pg_auth]
import pkg/nimcrypto/utils as ncutils
import types, buffer_io, ssl, simple_query, dsn

when defined(posix):
  import std/posix

when hasAsyncDispatch:
  import std/asyncnet
  from std/nativesockets import Domain, SockType, Protocol

# Authentication policy helpers

proc enforceAuthAllowed*(
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
): tuple[mechanism: string, cbType: string, cbData: seq[byte]] =
  ## Pick the SCRAM mechanism and channel-binding material for a SASL
  ## authentication attempt. Raises `PgConnectionError` when the server-offered
  ## mechanisms cannot satisfy `mode`.
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

# Internal helpers

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

# Single-host bootstrap

proc connectToHost*(
    config: ConnConfig, hostAddr: string, hostPort: int
): Future[PgConnection] {.async.} =
  ## Connect to a single PostgreSQL host. Internal helper for multi-host connect.

  if config.sslMode == sslAllow:
    # sslAllow: try plaintext first, then fall back to SSL (libpq semantics).
    # WARNING: This is vulnerable to MITM downgrade attacks. A network
    # attacker can force the first attempt to fail and then intercept
    # the SSL connection. Use sslRequire or stronger if security is needed.
    var plainConfig = config
    plainConfig.sslMode = sslDisable
    var plainErrMsg = ""
    try:
      return await connectToHost(plainConfig, hostAddr, hostPort)
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
      return await connectToHost(sslConfig, hostAddr, hostPort)
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

  var conn: PgConnection

  let isUnix = isUnixSocket(hostAddr)

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
    # SSL negotiation (before StartupMessage) — skip for Unix sockets
    if config.sslMode != sslDisable and not isUnix:
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
    var sawAuthRequest = false
    block authLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkAuthenticationOk:
            if not sawAuthRequest:
              enforceAuthAllowed(amNone, config.requireAuth)
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
              config.user, scramState, choice.cbType, choice.cbData
            )
            await conn.sendMsg(encodeSASLInitialResponse(choice.mechanism, clientFirst))
          of bmkAuthenticationSASLContinue:
            var clientFinal =
              scramClientFinalMessage(config.password, msg.saslData, scramState)
            var saslMsg = encodeSASLResponse(clientFinal)
            ncutils.burnMem(clientFinal)
            try:
              await conn.sendMsg(saslMsg)
            finally:
              ncutils.burnMem(saslMsg)
          of bmkAuthenticationSASLFinal:
            let ok = scramVerifyServerFinal(msg.saslFinalData, scramState)
            ncutils.burnMem(scramState.serverSignature)
            if not ok:
              raise newException(
                PgConnectionError, "SCRAM server signature verification failed"
              )
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
          case msg.kind
          of bmkParameterStatus:
            conn.serverParams[msg.paramName] = msg.paramValue
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

    # Discover extension type OIDs (hstore, etc.)
    conn.state = csBusy
    await conn.sendMsg(
      encodeQuery("SELECT oid, typarray FROM pg_type WHERE oid = to_regtype('hstore')")
    )
    block discoverLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkRowDescription:
            discard
          of bmkDataRow:
            if msg.columns.len > 0 and msg.columns[0].isSome:
              try:
                conn.hstoreOid = int32(parseInt(bytesToString(msg.columns[0].get)))
              except ValueError:
                discard
            if msg.columns.len > 1 and msg.columns[1].isSome:
              try:
                conn.hstoreArrayOid = int32(parseInt(bytesToString(msg.columns[1].get)))
              except ValueError:
                discard
          of bmkCommandComplete, bmkEmptyQueryResponse:
            discard
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            break discoverLoop
          of bmkErrorResponse:
            discard
          else:
            discard
        await conn.fillRecvBuf()

    conn.createdAt = Moment.now()
    return conn
  except CatchableError as e:
    await conn.closeTransport()
    raise e

# Close

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
  conn.heldSessionLocks = 0
  # Fail any pending notification waiter
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    conn.notifyWaiter.fail(newException(PgError, "Connection closed"))
  await conn.closeTransport()

# Multi-host connect

proc connect*(config: ConnConfig): Future[PgConnection] =
  ## Establish a new connection to a PostgreSQL server.
  ## Supports multi-host failover: tries each host in order.
  ## Respects `targetSessionAttrs` to select the appropriate server type.
  ## The `connectTimeout` wraps the entire multi-host connection attempt.
  proc perform(): Future[PgConnection] {.async.} =
    let hosts = config.getHosts()
    var errors: seq[string]

    if config.targetSessionAttrs == tsaPreferStandby:
      # First pass: look for a standby
      for entry in hosts:
        try:
          let conn = await connectToHost(config, entry.host, entry.port)
          if await conn.checkSessionAttrs(tsaStandby):
            return conn
          await conn.close()
        except CancelledError as e:
          raise e
        except CatchableError as e:
          errors.add(entry.host & ":" & $entry.port & ": " & e.msg)
      # Second pass: accept any server
      for entry in hosts:
        try:
          let conn = await connectToHost(config, entry.host, entry.port)
          return conn
        except CancelledError as e:
          raise e
        except CatchableError as e:
          errors.add(entry.host & ":" & $entry.port & ": " & e.msg)
    else:
      for entry in hosts:
        try:
          let conn = await connectToHost(config, entry.host, entry.port)
          if config.targetSessionAttrs == tsaAny or
              await conn.checkSessionAttrs(config.targetSessionAttrs):
            return conn
          await conn.close()
          errors.add(
            entry.host & ":" & $entry.port &
              ": server does not match target_session_attrs " &
              $config.targetSessionAttrs
          )
        except CancelledError as e:
          raise e
        except CatchableError as e:
          errors.add(entry.host & ":" & $entry.port & ": " & e.msg)

    raise newException(
      PgConnectionError, "Could not connect to any host: " & errors.join("; ")
    )

  proc wrapped(): Future[PgConnection] {.async.} =
    let hosts = config.getHosts()
    var conn: PgConnection
    withTracing(
      config.tracer,
      onConnectStart,
      onConnectEnd,
      TraceConnectStartData(hosts: hosts),
      TraceConnectEndData,
      TraceConnectEndData(conn: conn),
    ):
      conn =
        if config.connectTimeout != default(Duration):
          await perform().wait(config.connectTimeout)
        else:
          await perform()
      conn.tracer = config.tracer
    return conn

  wrapped()

proc connect*(dsn: string): Future[PgConnection] =
  ## Shorthand for ``connect(parseDsn(dsn))``.
  connect(parseDsn(dsn))
