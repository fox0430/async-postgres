## TLS/SSL negotiation for PostgreSQL connections.
##
## Implements both libpq negotiation styles: the traditional SSLRequest
## handshake (`sslnegotiation=postgres`) and Direct SSL, which starts TLS
## immediately and requires the "postgresql" ALPN protocol
## (`sslnegotiation=direct`, PostgreSQL 17+). The TLS handshake itself is shared
## by `establishTls` under both async backends:
##
## - **chronos**: BearSSL-based TLS via `chronos/streams/tlsstream`, with
##   custom trust anchor parsing (`parseTrustAnchors`) and X.509 capture for
##   SCRAM-SHA-256-PLUS channel binding (`installX509Capture`).
## - **asyncdispatch**: OpenSSL via `std/net.wrapConnectedSocket`, with PEM
##   trust anchors written to a temp file and `SSL_get_peer_certificate` used
##   for channel binding.
##
## Re-exported through `pg_connection.nim`.

import ../[async_backend, pg_errors, pg_protocol]
import types, buffer_io

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet
  when defined(ssl):
    import std/[net, openssl, tempfiles, os]

const PgAlpnProtocol = "postgresql"
  ## ALPN protocol name a Direct SSL connection must negotiate (PostgreSQL 17+).
  ## libpq sends and requires the same name for `sslnegotiation=direct`.

when hasAsyncDispatch and defined(ssl):
  # std/net's `wrapConnectedSocket` gates its certificate-name check behind
  # `not isIpAddress(hostname)` (see lib/pure/net.nim), so when the connect
  # host is an IP literal it performs *no* SAN/CN matching at all — only chain
  # verification. sslVerifyFull would then accept any CA-trusted certificate
  # regardless of which host/IP it was issued for, defeating MITM protection.
  # (The chronos/BearSSL backend still matches the IP against the cert and
  # fails closed, so this gap is asymmetric and OpenSSL-only.) OpenSSL exposes
  # `X509_check_ip_asc` to match an IP literal against the certificate's
  # iPAddress SANs; std/openssl doesn't bind it, so declare it and enforce it
  # ourselves for verify-full.
  proc X509_check_ip_asc(
    cert: PX509, ipasc: cstring, flags: cuint
  ): cint {.cdecl, dynlib: DLLSSLName, importc.}

  template needsManualIpVerification(sslMode: SslMode, host: string): bool =
    ## verify-full to an IP literal needs an explicit IP-SAN check because
    ## `wrapConnectedSocket` skips it. Other modes and DNS hostnames are already
    ## verified by `wrapConnectedSocket`.
    sslMode == sslVerifyFull and isIpAddress(host)

  template certMatchesIp(cert: PX509, host: string): bool =
    ## True if `host` (an IP literal) matches an iPAddress SAN in `cert`.
    X509_check_ip_asc(cert, host.cstring, 0) == 1

  proc verifyPeerIpSan(socket: AsyncSocket, host: string) =
    ## Raise `PgConnectionError` unless the peer certificate covers `host`
    ## (an IP literal) via an iPAddress SAN. Closes the verify-full gap that
    ## `wrapConnectedSocket` leaves open for IP connections.
    let cert = SSL_get_peer_certificate(socket.sslHandle)
    if cert == nil:
      raise newException(
        PgConnectionError,
        "sslmode=verify-full: server presented no certificate for IP host " & host,
      )
    try:
      if not certMatchesIp(cert, host):
        raise newException(
          PgConnectionError,
          "sslmode=verify-full: server certificate has no matching IP SAN for " & host,
        )
    finally:
      X509_free(cert)

proc establishTls(
    conn: PgConnection, config: ConnConfig, sslHost: string, direct: bool
) {.async.} =
  ## Run the TLS handshake over `conn`'s transport and wire up the encrypted
  ## reader/writer. Shared by traditional (post-`SSLRequest`) and Direct SSL
  ## negotiation. When `direct` is true the client offers the "postgresql" ALPN
  ## protocol and requires the server to select it, matching libpq's
  ## `sslnegotiation=direct` behaviour (PostgreSQL 17+).
  when hasChronos:
    conn.baseReader = newAsyncStreamReader(conn.transport)
    conn.baseWriter = newAsyncStreamWriter(conn.transport)

    let flags =
      case config.sslMode
      of sslVerifyFull:
        {}
      of sslVerifyCa:
        {TLSFlags.NoVerifyServerName}
      else:
        {TLSFlags.NoVerifyHost, TLSFlags.NoVerifyServerName}

    let serverName = if config.sslMode == sslVerifyFull: sslHost else: ""
    let alpn =
      if direct:
        @[PgAlpnProtocol]
      else:
        @[]

    if config.sslRootCert.len > 0:
      let parsed = parseTrustAnchors(config.sslRootCert)
      conn.trustAnchorBufs = parsed.backing
        # Must outlive TLS session (see parseTrustAnchors doc)
      conn.tlsStream = newTLSClientAsyncStream(
        conn.baseReader,
        conn.baseWriter,
        serverName,
        flags = flags,
        minVersion = TLSVersion.TLS12,
        maxVersion = TLSVersion.TLS12,
        trustAnchors = parsed.store,
        alpnProtocols = alpn,
      )
    else:
      conn.tlsStream = newTLSClientAsyncStream(
        conn.baseReader,
        conn.baseWriter,
        serverName,
        flags = flags,
        minVersion = TLSVersion.TLS12,
        maxVersion = TLSVersion.TLS12,
        alpnProtocols = alpn,
      )
    installX509Capture(
      conn.x509Capture, conn.tlsStream.ccontext.eng, addr conn.serverCertDer
    )
    await conn.tlsStream.handshake()
    if direct and conn.tlsStream.getSelectedAlpnProtocol() != PgAlpnProtocol:
      raise newException(
        PgConnectionError,
        "direct SSL connection established without ALPN: the server does not " &
          "support sslnegotiation=direct (requires PostgreSQL 17+)",
      )
    conn.reader = conn.tlsStream.reader
    conn.writer = conn.tlsStream.writer
    conn.sslEnabled = true
  elif hasAsyncDispatch:
    when defined(ssl):
      let verifyMode =
        case config.sslMode
        of sslVerifyCa, sslVerifyFull: SslCVerifyMode.CVerifyPeer
        else: SslCVerifyMode.CVerifyNone

      var ctx: SslContext
      var tmpPath: string
      if config.sslRootCert.len > 0:
        let (tmpFile, tp) = createTempFile("pg_ca_", ".pem")
        tmpPath = tp
        try:
          tmpFile.write(config.sslRootCert)
          tmpFile.close()
          ctx = newContext(verifyMode = verifyMode, caFile = tmpPath)
        except:
          removeFile(tmpPath)
          raise
      else:
        ctx = newContext(verifyMode = verifyMode)

      if direct:
        # Direct SSL requires offering the "postgresql" ALPN protocol; the wire
        # form is a 1-byte length prefix followed by the protocol name. Setting
        # it on the context makes `wrapConnectedSocket`'s ClientHello advertise
        # it, which is what a PostgreSQL 17+ direct-SSL server keys off.
        #
        # Unlike the chronos backend, we cannot *verify* the negotiated ALPN
        # here: `wrapConnectedSocket` on an AsyncSocket only sets connect state
        # and defers the TLS handshake to the first send (the StartupMessage in
        # `connectToHost`), so the selected protocol — like the peer certificate
        # captured below — is not yet available. A server that fails to negotiate
        # the handshake still surfaces as a connection error on that first send.
        const alpnProto = "\x0a" & PgAlpnProtocol
        discard
          SSL_CTX_set_alpn_protos(ctx.context, alpnProto.cstring, cuint(alpnProto.len))

      try:
        let hostname = if config.sslMode == sslVerifyFull: sslHost else: ""
        wrapConnectedSocket(ctx, conn.socket, handshakeAsClient, hostname)
        # wrapConnectedSocket skips name verification for IP hostnames; for
        # verify-full we must match the IP against the cert's SANs ourselves.
        if needsManualIpVerification(config.sslMode, sslHost):
          verifyPeerIpSan(conn.socket, sslHost)
        conn.sslEnabled = true
        # Extract server certificate DER for SCRAM-SHA-256-PLUS channel binding.
        # If unavailable, cbPrefer will silently fall back to SCRAM-SHA-256 —
        # warn the operator so the loss of channel binding is observable.
        # (cbRequire is enforced in selectScramMechanism.)
        let peerCert = SSL_get_peer_certificate(conn.socket.sslHandle)
        if peerCert != nil:
          try:
            let derStr = i2d_X509(peerCert)
            if derStr.len > 0:
              conn.serverCertDer = newSeq[byte](derStr.len)
              for i in 0 ..< derStr.len:
                conn.serverCertDer[i] = byte(derStr[i])
            else:
              stderr.writeLine "pg_connection: server certificate DER encoding is empty; SCRAM-SHA-256-PLUS channel binding unavailable"
          finally:
            X509_free(peerCert)
        else:
          stderr.writeLine "pg_connection: server certificate unavailable; SCRAM-SHA-256-PLUS channel binding unavailable"
      finally:
        if tmpPath.len > 0:
          removeFile(tmpPath)
    else:
      raise
        newException(PgConnectionError, "SSL support requires compiling with -d:ssl")

proc negotiateSSL*(conn: PgConnection, config: ConnConfig, sslHost: string) {.async.} =
  ## Negotiate TLS for the connection. With `sslnegotiation=postgres` (default)
  ## this sends an `SSLRequest` and starts TLS only if the server accepts;
  ## with `sslnegotiation=direct` it starts TLS immediately without the
  ## round-trip (PostgreSQL 17+). `sslHost` is the host *name* the server
  ## certificate is verified against (the entry's `host`, never its `hostaddr` —
  ## libpq semantics).
  if config.sslMode == sslVerifyFull and sslHost.len == 0:
    # hostaddr without host: there is no name to match the certificate
    # against (libpq raises the same way).
    raise newException(
      PgConnectionError, "A host name must be specified for a verified SSL connection"
    )

  if config.sslNegotiation == sslnDirect:
    # Direct SSL skips the SSLRequest probe, so there is no plaintext path to
    # fall back to. libpq rejects weak sslmodes here for the same reason; SSL
    # must actually be required.
    if config.sslMode notin {sslRequire, sslVerifyCa, sslVerifyFull}:
      raise newException(
        PgConnectionError,
        "sslnegotiation=direct requires sslmode=require, verify-ca, or verify-full",
      )
    await establishTls(conn, config, sslHost, direct = true)
    return

  let sslReq = encodeSSLRequest()
  var respChar: char
  var extraBytesBuffered = false
    ## True when the SSLRequest-reply read pulled in more than the single
    ## response byte, i.e. the transport had already buffered bytes the server
    ## should not have sent before the TLS handshake (pre-TLS injection).

  when hasChronos:
    discard await conn.transport.write(sslReq)
    # Read up to two bytes so a man-in-the-middle who appended plaintext to the
    # 'S' reply (CVE-2021-23214 family) is caught even when chronos drains the
    # whole TCP segment into its own transport buffer (where a kernel-level
    # MSG_PEEK can no longer see it). A compliant server sends exactly one byte
    # and then waits for our ClientHello, and `readOnce` returns as soon as any
    # data is available, so this never blocks on a second byte that will not come.
    var response: array[2, byte]
    let n = await conn.transport.readOnce(addr response[0], 2)
    if n == 0:
      raise newException(PgConnectionError, "Connection closed during SSL negotiation")
    respChar = char(response[0])
    extraBytesBuffered = n > 1
  elif hasAsyncDispatch:
    await conn.socket.sendRawBytes(sslReq)
    # The socket is unbuffered (`newAsyncSocket(buffered = false)`), so `recv(1)`
    # issues a single recv syscall for at most one byte; any injected bytes stay
    # in the kernel buffer and are caught by `socketHasPendingData` below.
    let respStr = await conn.socket.recv(1)
    if respStr.len == 0:
      raise newException(PgConnectionError, "Connection closed during SSL negotiation")
    respChar = respStr[0]

  case respChar
  of 'S':
    # Reject pre-TLS byte injection before starting the handshake. A server
    # that accepts SSL must not send anything between the 'S' reply and the TLS
    # ClientHello, so bytes already readable here were injected by a
    # man-in-the-middle to be smuggled ahead of (and possibly mistaken for part
    # of) the encrypted stream. libpq performs the same check. `extraBytesBuffered`
    # catches bytes the transport already drained; `socketHasPendingData` catches
    # bytes still sitting in the kernel buffer.
    if extraBytesBuffered or conn.socketHasPendingData():
      raise newException(
        PgConnectionError,
        "Received unencrypted data after SSL response (possible man-in-the-middle)",
      )
    await establishTls(conn, config, sslHost, direct = false)
  of 'N':
    if config.sslMode in {sslRequire, sslVerifyCa, sslVerifyFull}:
      raise newException(PgConnectionError, "Server does not support SSL")
    # sslPrefer: server refused SSL – connection will proceed unencrypted.
    # WARNING: This is vulnerable to MITM downgrade attacks. A network
    # attacker can intercept the SSLRequest and reply 'N' to force
    # plaintext. Use sslRequire or stronger if security is needed.
    stderr.writeLine "pg_connection: SSL refused by server, falling back to plaintext (sslmode=prefer)"
  else:
    raise newException(PgConnectionError, "Unexpected SSL response: " & $respChar)
