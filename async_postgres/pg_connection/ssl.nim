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
## - **asyncdispatch**: OpenSSL via `std/asyncnet.wrapConnectedSocket`, with PEM
##   trust anchors written to a temp file and `SSL_get_peer_certificate` used
##   for channel binding.
##
## Re-exported through `pg_connection.nim`.

import std/[net, strutils]
import ../[async_backend, pg_errors, pg_protocol, pg_types]
import types, buffer_io

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet
  when defined(ssl):
    import std/[dynlib, openssl, tempfiles, os]

when hasTls:
  const PgAlpnProtocol = "postgresql"
    ## ALPN protocol name required for `sslnegotiation=direct` (PostgreSQL 17+).

when hasAsyncDispatch and defined(ssl):
  const PgAlpnWire = char(PgAlpnProtocol.len) & PgAlpnProtocol
    ## Length-prefixed wire form for `SSL_CTX_set_alpn_protos` (RFC 7301 §3.1).

  # On asyncdispatch `conn.socket` is an `AsyncSocket`, so `wrapConnectedSocket`
  # resolves to `std/asyncnet`'s overload, which only sets SNI — it never matches
  # the certificate against the host (unlike `std/net`'s) and defers the
  # handshake, leaving the peer cert unavailable on return. With `newContext`
  # requesting only `SSL_VERIFY_PEER` (chain verification), sslVerifyFull would
  # accept any CA-trusted cert for any host. So tell OpenSSL the expected identity
  # *before* the handshake; it then matches the cert during its own verification
  # and fails closed. std/openssl doesn't bind these symbols, so resolve them
  # ourselves.
  type
    SslSet1HostFn =
      proc(ssl: SslPtr, hostname: cstring): cint {.cdecl, gcsafe, raises: [].}
    SslGet0ParamFn = proc(ssl: SslPtr): pointer {.cdecl, gcsafe, raises: [].}
    X509SetIpAscFn =
      proc(param: pointer, ipasc: cstring): cint {.cdecl, gcsafe, raises: [].}
    SslGetBioFn = proc(ssl: SslPtr): BIO {.cdecl, gcsafe, raises: [].}
    SslGet0AlpnSelectedFn =
      proc(ssl: SslPtr, data: ptr pointer, len: ptr cuint) {.cdecl, gcsafe, raises: [].}
    SslCtxSetAlpnProtosFn = proc(ctx: SslCtx, protos: cstring, protos_len: cuint): cint {.
      cdecl, gcsafe, raises: []
    .}
    SslCtxSetDefaultPasswdCbFn =
      proc(ctx: SslCtx, cb: pem_password_cb) {.cdecl, gcsafe, raises: [].}

  # Apple's system libssl/libcrypto omit some of these symbols; an eager
  # `{.dynlib.}` binding would abort the process at startup. Resolve via
  # `symAddr` (nil when missing) and let callers handle nil. Resolution runs at
  # module init, before user threads can exist, so no synchronization is needed.
  proc resolveSym(lib: LibHandle, symbol: string): pointer =
    if lib == nil:
      nil
    else:
      symAddr(lib, symbol)

  let
    sslDynlib = loadLibPattern(DLLSSLName)
    utilDynlib = loadLibPattern(DLLUtilName)
    sslSet1Host* = cast[SslSet1HostFn](resolveSym(sslDynlib, "SSL_set1_host"))
    sslGet0Param* = cast[SslGet0ParamFn](resolveSym(sslDynlib, "SSL_get0_param"))
    x509VerifyParamSet1IpAsc* =
      cast[X509SetIpAscFn](resolveSym(utilDynlib, "X509_VERIFY_PARAM_set1_ip_asc"))
    sslGetRbio* = cast[SslGetBioFn](resolveSym(sslDynlib, "SSL_get_rbio"))
    sslGetWbio* = cast[SslGetBioFn](resolveSym(sslDynlib, "SSL_get_wbio"))
    sslGet0AlpnSelected* =
      cast[SslGet0AlpnSelectedFn](resolveSym(sslDynlib, "SSL_get0_alpn_selected"))
    sslCtxSetAlpnProtos* =
      cast[SslCtxSetAlpnProtosFn](resolveSym(sslDynlib, "SSL_CTX_set_alpn_protos"))
    sslCtxSetDefaultPasswdCb* = cast[SslCtxSetDefaultPasswdCbFn](resolveSym(
      sslDynlib, "SSL_CTX_set_default_passwd_cb"
    ))

  proc failPemPassphrase(
      buf: cstring, size, rwflag: cint, userdata: pointer
  ): cint {.cdecl.} =
    ## Passphrase callback that always fails. OpenSSL's default callback prompts
    ## on the controlling TTY, which would block the async event loop when an
    ## encrypted client key is loaded; failing turns that into a load error.
    -1

  proc formatSslError(prefix: string): string =
    result = prefix
    let code = ERR_peek_last_error()
    if code != 0:
      result &= ": " & $ERR_error_string(code, nil)

  proc driveTlsHandshake(socket: AsyncSocket) {.async.} =
    ## Drive `wrapConnectedSocket`'s deferred client handshake to completion,
    ## shuttling bytes between OpenSSL's memory BIOs and the raw AsyncFD.
    ## Required so `SSL_get_peer_certificate` returns the leaf cert for SCRAM
    ## channel binding (asyncnet only performs the handshake on the first
    ## application send/recv, and its `sslLoop`/BIO plumbing is not exported).
    const HandshakeBufSize = 4096
    let ssl = socket.sslHandle
    if ssl == nil:
      raise
        newException(PgConnectionError, "TLS handshake: SSL handle is not initialised")
    if sslGetRbio == nil or sslGetWbio == nil:
      raise newException(
        PgConnectionError,
        "TLS handshake: libssl does not export SSL_get_rbio / SSL_get_wbio",
      )
    let rbio = sslGetRbio(ssl)
    let wbio = sslGetWbio(ssl)
    if rbio == nil or wbio == nil:
      raise newException(PgConnectionError, "TLS handshake: memory BIOs unavailable")
    let fd = socket.getFd.AsyncFD
    while true:
      ErrClearError()
      let ret = sslDoHandshake(ssl)
      # Flush anything OpenSSL wrote to the outgoing memory BIO (ClientHello,
      # key exchange, Finished, …) regardless of `ret`, so a WANT_READ still
      # sends its handshake record before we block on the peer's reply.
      let pending = bioCtrlPending(wbio)
      if pending > 0:
        var outBuf = newString(pending)
        let read = bioRead(wbio, cast[cstring](addr outBuf[0]), pending)
        if read <= 0:
          raise newException(
            PgConnectionError, formatSslError("TLS handshake: BIO_read failed")
          )
        outBuf.setLen(read)
        # Qualified to force asyncdispatch's raw AsyncFD overload — asyncnet's
        # `send` would recurse into the very handshake loop we drive.
        await asyncdispatch.send(fd, outBuf, flags = {})
      if ret == 1:
        return
      let err = SSL_get_error(ssl, ret)
      case err
      of SSL_ERROR_WANT_READ:
        let data = await asyncdispatch.recv(fd, HandshakeBufSize, flags = {})
        if data.len == 0:
          raise
            newException(PgConnectionError, "TLS handshake: connection closed by peer")
        let wrote = bioWrite(rbio, cast[cstring](unsafeAddr data[0]), data.len.cint)
        if wrote <= 0:
          raise newException(
            PgConnectionError, formatSslError("TLS handshake: BIO_write failed")
          )
      of SSL_ERROR_WANT_WRITE:
        # Nothing to do: pending output was flushed above. Loop and retry.
        discard
      else:
        raise newException(
          PgConnectionError,
          formatSslError("TLS handshake failed (SSL_get_error=" & $err & ")"),
        )

  proc getSelectedAlpnOpenssl(ssl: SslPtr): string =
    ## Return the peer-selected ALPN protocol, or "" if none was selected.
    if sslGet0AlpnSelected == nil:
      raise newException(
        PgConnectionError,
        "sslnegotiation=direct: libssl does not export SSL_get0_alpn_selected",
      )
    var protoPtr: pointer
    var protoLen: cuint
    sslGet0AlpnSelected(ssl, addr protoPtr, addr protoLen)
    if protoPtr.isNil or protoLen == 0:
      return ""
    result = newString(protoLen.int)
    copyMem(addr result[0], protoPtr, protoLen.int)

  proc enforceVerifyFullIdentity(sslHandle: SslPtr, host: string) =
    ## Make OpenSSL match the peer certificate against `host` during the deferred
    ## handshake (iPAddress SANs for an IP literal, DNS name otherwise), so
    ## verify-full fails closed on a mismatched but CA-trusted cert. Raises if the
    ## identity cannot be installed, so it never silently degrades to chain-only.
    let ok =
      if isIpAddress(host):
        if x509VerifyParamSet1IpAsc == nil:
          raise newException(
            PgConnectionError,
            "sslmode=verify-full: libcrypto does not export " &
              "X509_VERIFY_PARAM_set1_ip_asc; cannot verify " & host,
          )
        if sslGet0Param == nil:
          raise newException(
            PgConnectionError,
            "sslmode=verify-full: libssl does not export SSL_get0_param; " &
              "cannot verify " & host,
          )
        x509VerifyParamSet1IpAsc(sslGet0Param(sslHandle), host.cstring)
      else:
        if sslSet1Host == nil:
          raise newException(
            PgConnectionError,
            "sslmode=verify-full: libssl does not export SSL_set1_host; " &
              "cannot verify " & host,
          )
        sslSet1Host(sslHandle, host.cstring)
    if ok != 1:
      raise newException(
        PgConnectionError,
        "sslmode=verify-full: failed to set certificate verification identity for " &
          host,
      )

proc validateDirectSslCompatible*(config: ConnConfig) {.raises: [PgConnectionError].} =
  ## Reject `sslnegotiation=direct` under a weak `sslmode`. Direct SSL skips
  ## the SSLRequest probe so it has no plaintext fall-back path (libpq parity).
  ## Idempotent — safe to call from any layer.
  if config.sslNegotiation == sslnDirect and
      config.sslMode notin {sslRequire, sslVerifyCa, sslVerifyFull}:
    raise newException(
      PgConnectionError,
      "sslnegotiation=direct requires sslmode=require, verify-ca, or verify-full",
    )

when hasTls:
  proc assertAlpnPostgres(selected: string) {.raises: [PgConnectionError].} =
    if selected.len == 0:
      raise newException(
        PgConnectionError,
        "direct SSL connection established without ALPN: the server does not " &
          "support sslnegotiation=direct (requires PostgreSQL 17+)",
      )
    if selected != PgAlpnProtocol:
      # Peer-controlled value: escape non-printable bytes so an embedded NUL
      # can't truncate a C-string logger and hide the actual selection.
      raise newException(
        PgConnectionError,
        "direct SSL connection negotiated unexpected ALPN protocol '" &
          selected.escape("", "") & "' (expected '" & PgAlpnProtocol & "')",
      )

proc sniName*(sslHost: string, sslSni: bool): string =
  ## Value for the TLS SNI extension. Empty means "do not send SNI".
  ## Matches libpq: SNI is on by default and suppressed for IP literals
  ## (RFC 6066 §3 forbids IPs in server_name) and when the host name is
  ## unknown (hostaddr-only).
  if not sslSni:
    return ""
  if sslHost.len == 0:
    return ""
  if isIpAddress(sslHost):
    return ""
  sslHost

proc establishTls(conn: PgConnection, config: ConnConfig, sslHost: string) {.async.} =
  ## Run the TLS handshake and wire up the encrypted reader/writer. Under
  ## `sslnegotiation=direct` (PG17+) the "postgresql" ALPN selection is
  ## enforced. `config` is the source of truth for every TLS parameter — do
  ## not read `conn.config` here, so callers can rewrite it (e.g. sslAllow)
  ## without silently downgrading the handshake.

  when hasChronos:
    let direct = config.sslNegotiation == sslnDirect
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

    # BearSSL's serverName doubles as SNI wire value and X509 name check input.
    # Under verify-full it must be sslHost for BearSSL to verify; other modes
    # honor sslSni and RFC 6066 IP-literal suppression.
    let serverName =
      if config.sslMode == sslVerifyFull:
        sslHost
      else:
        sniName(sslHost, config.sslSni)
    # newTLSClientAsyncStream stores these on TLSAsyncStream
    # (clientCertificate/clientPrivateKey) so BearSSL keeps a valid reference
    # for the lifetime of conn.tlsStream — no extra retention on PgConnection
    # is needed (unlike trustAnchorBufs above).
    var clientCert: TLSCertificate
    var clientKey: TLSPrivateKey
    if config.sslCert.len > 0 and config.sslKey.len > 0:
      try:
        clientCert = TLSCertificate.init(config.sslCert)
        clientKey = TLSPrivateKey.init(config.sslKey)
      except TLSStreamProtocolError as e:
        raise newException(
          PgConnectionError, "Failed to load client certificate/key: " & e.msg
        )

    # Advertise ALPN on every TLS connection (libpq 17 parity: SSL_set_alpn_protos
    # is called unconditionally); enforcement stays direct-only below.
    try:
      if config.sslMode in {sslVerifyCa, sslVerifyFull}:
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
          alpnProtocols = [PgAlpnProtocol],
          certificate = clientCert,
          privateKey = clientKey,
        )
      else:
        # NoVerifyHost is set, so trust anchors are ignored regardless.
        conn.tlsStream = newTLSClientAsyncStream(
          conn.baseReader,
          conn.baseWriter,
          serverName,
          flags = flags,
          minVersion = TLSVersion.TLS12,
          maxVersion = TLSVersion.TLS12,
          alpnProtocols = [PgAlpnProtocol],
          certificate = clientCert,
          privateKey = clientKey,
        )
    except TLSStreamInitError as e:
      # Covers cert/key decode failures newTLSClientAsyncStream performs itself
      # (e.g. getSignerAlgo), which the TLSCertificate.init wrapping above misses.
      raise
        newException(PgConnectionError, "Failed to initialise TLS stream: " & e.msg, e)
    installX509Capture(
      conn.x509Capture, conn.tlsStream.ccontext.eng, addr conn.serverCertDer
    )
    await conn.tlsStream.handshake()
    if direct:
      assertAlpnPostgres(conn.tlsStream.getSelectedAlpnProtocol())
    conn.reader = conn.tlsStream.reader
    conn.writer = conn.tlsStream.writer
    conn.sslEnabled = true
  elif hasAsyncDispatch:
    when defined(ssl):
      let direct = config.sslNegotiation == sslnDirect
      var ctx: SslContext
      var tmpPaths: seq[string]

      proc removeTempPem(p: string) =
        try:
          removeFile(p)
        except OSError as e:
          warnStderr "pg_connection: failed to remove temp SSL file " & p & ": " & e.msg

      # std/net's newContext accepts only file paths, so each PEM is staged as
      # a private temp file (0600, O_EXCL; /dev/shm tmpfs when available) and
      # removed as soon as newContext has loaded it.
      proc writeTempPem(content, prefix: string): string =
        if content.len == 0:
          return ""
        var f: File
        var p: string
        when defined(linux):
          const shm = "/dev/shm"
          if dirExists(shm):
            try:
              (f, p) = createTempFile(prefix, ".pem", dir = shm)
            except CatchableError:
              (f, p) = createTempFile(prefix, ".pem")
          else:
            (f, p) = createTempFile(prefix, ".pem")
        else:
          (f, p) = createTempFile(prefix, ".pem")
        tmpPaths.add(p)
        try:
          f.write(content)
        finally:
          f.close()
        p

      try:
        let caPath =
          if config.sslMode in {sslVerifyCa, sslVerifyFull}:
            writeTempPem(config.sslRootCert, "pg_ca_")
          else:
            ""
        let certPath = writeTempPem(config.sslCert, "pg_cert_")
        let keyPath = writeTempPem(config.sslKey, "pg_key_")

        try:
          # Always CVerifyNone: with CVerifyPeer, newContext preloads the OS CA
          # bundle (and fails outright when none exists), and the later
          # SSL_CTX_load_verify_locations *appends* sslrootcert instead of
          # replacing that store — any Web-PKI-issued cert would then pass
          # verify-ca/verify-full. Peer verification is enabled manually below
          # so only the pinned CA is trusted.
          ctx = newContext(verifyMode = CVerifyNone)
        except CatchableError as e:
          raise
            newException(PgConnectionError, "Failed to create SSL context: " & e.msg, e)
        if config.sslMode in {sslVerifyCa, sslVerifyFull}:
          SSL_CTX_set_verify(ctx.context, SSL_VERIFY_PEER, nil)

        # Load CA / cert / key ourselves rather than letting `newContext` do it
        # via its `caFile`/`certFile`/`keyFile` parameters: std/net leaks the
        # SSL_CTX it just allocated when a file load fails (raises before
        # returning the SslContext, so no handle to `destroyContext`). Doing it
        # here keeps `ctx` non-nil so the outer `finally` cleans up.
        try:
          if caPath.len > 0:
            if SSL_CTX_load_verify_locations(ctx.context, caPath.cstring, nil) != 1:
              raise newException(IOError, "Failed to load CA certificate: " & caPath)
          if certPath.len > 0:
            if SSL_CTX_use_certificate_chain_file(ctx.context, certPath.cstring) != 1:
              raise
                newException(IOError, "Failed to load client certificate: " & certPath)
          if keyPath.len > 0:
            if sslCtxSetDefaultPasswdCb != nil:
              sslCtxSetDefaultPasswdCb(ctx.context, failPemPassphrase)
            elif "ENCRYPTED" in config.sslKey:
              # PKCS#8 "BEGIN ENCRYPTED PRIVATE KEY" / PKCS#1 "Proc-Type:
              # 4,ENCRYPTED". Without the callback an encrypted key would
              # freeze the event loop on a TTY prompt (see failPemPassphrase).
              raise newException(
                IOError,
                "client private key is passphrase-protected; " &
                  "only unencrypted keys are supported",
              )
            if SSL_CTX_use_PrivateKey_file(
              ctx.context, keyPath.cstring, SSL_FILETYPE_PEM
            ) != 1:
              raise newException(
                IOError, formatSslError("Failed to load client private key")
              )
            if SSL_CTX_check_private_key(ctx.context) != 1:
              raise
                newException(IOError, "Client certificate and private key do not match")
        except CatchableError as e:
          raise newException(
            PgConnectionError,
            "Failed to load client certificate/key or CA: " & e.msg,
            e,
          )

        # newContext has now copied the PEM bytes into the SslContext (OpenSSL
        # decodes and stores them in memory). Drop the on-disk copies before
        # the TLS handshake / connection attempt so a crash during negotiation
        # cannot leave the client private key behind for another process on
        # the same uid. The outer finally still runs for the newContext-failure
        # path (tmpPaths retains entries only until this loop clears it).
        for p in tmpPaths:
          removeTempPem(p)
        tmpPaths.setLen(0)

        # Advertise ALPN unconditionally (libpq 17 parity). The missing-symbol
        # path is only fatal for direct mode; traditional mode degrades to no-ALPN.
        if sslCtxSetAlpnProtos != nil:
          let rc =
            sslCtxSetAlpnProtos(ctx.context, PgAlpnWire.cstring, cuint(PgAlpnWire.len))
          if rc != 0:
            raise newException(
              PgConnectionError,
              "failed to configure ALPN (SSL_CTX_set_alpn_protos returned " & $rc & ")",
            )
        elif direct:
          raise newException(
            PgConnectionError,
            "sslnegotiation=direct: libssl does not export SSL_CTX_set_alpn_protos",
          )

        let hostname = sniName(sslHost, config.sslSni)
        wrapConnectedSocket(ctx, conn.socket, handshakeAsClient, hostname)
        # asyncnet skips name matching; make OpenSSL enforce it during handshake.
        if config.sslMode == sslVerifyFull:
          enforceVerifyFullIdentity(conn.socket.sslHandle, sslHost)
        # Drive the handshake now so the peer cert is available before SCRAM
        # decides channel binding (asyncnet defers it to the first send/recv).
        await driveTlsHandshake(conn.socket)
        if direct:
          assertAlpnPostgres(getSelectedAlpnOpenssl(conn.socket.sslHandle))
        conn.sslEnabled = true
        # Extract server certificate DER for SCRAM-SHA-256-PLUS channel binding.
        # If unavailable, cbPrefer silently falls back to SCRAM-SHA-256 — warn
        # so the loss of channel binding is observable. (cbRequire is enforced
        # in selectScramMechanism.)
        let peerCert = SSL_get_peer_certificate(conn.socket.sslHandle)
        if peerCert != nil:
          try:
            let derStr = i2d_X509(peerCert)
            if derStr.len > 0:
              conn.serverCertDer = toBytes(derStr)
            else:
              warnStderr "pg_connection: server certificate DER encoding is empty; SCRAM-SHA-256-PLUS channel binding unavailable"
          finally:
            X509_free(peerCert)
        else:
          warnStderr "pg_connection: server certificate unavailable; SCRAM-SHA-256-PLUS channel binding unavailable"
      finally:
        # asyncnet doesn't free the SslContext (no =destroy on std/net's type).
        # SSL_new inside wrapConnectedSocket takes its own ref, so destroying
        # here on both success and failure balances newContext without freeing
        # the SSL's copy — the socket's SSL_free drops that one on close.
        if ctx != nil:
          ctx.destroyContext()
        # Fallback cleanup for the newContext-failure path — on success we
        # already removed the files above and cleared tmpPaths. Failures are
        # surfaced (not silently dropped) because tmpPaths may contain the
        # client private key PEM — leaving it around would be a footgun.
        for p in tmpPaths:
          removeTempPem(p)
    else:
      raise
        newException(PgConnectionError, "SSL support requires compiling with -d:ssl")

proc negotiateSSL*(conn: PgConnection, config: ConnConfig, sslHost: string) {.async.} =
  ## Negotiate TLS. `sslnegotiation=postgres` (default) sends an SSLRequest first;
  ## `sslnegotiation=direct` starts TLS immediately (PostgreSQL 17+). `sslHost`
  ## is the name matched against the server certificate (libpq semantics: the
  ## entry's `host`, never `hostaddr`).
  # Defensive: connectToHost / perform already validate, but this proc is
  # exported and may be called directly; the checks are idempotent.
  # `validateClientCertConfig` also runs at the connect-time chokepoint in
  # `wrapped()` (lifecycle.nim), but is re-invoked here so direct callers of
  # `negotiateSSL` cannot bypass the mTLS pairing check — otherwise chronos
  # would silently drop a lone `sslCert` while asyncdispatch errors out.
  try:
    validateClientCertConfig(config)
  except PgError as e:
    raise newException(PgConnectionError, e.msg, e)
  validateDirectSslCompatible(config)
  if config.sslMode in {sslVerifyCa, sslVerifyFull} and config.sslRootCert.len == 0:
    # Both backends silently fall back to a Web PKI store (chronos:
    # MozillaTrustAnchors, std/net: OS CA bundle) — for verify-ca that also
    # skips hostname checks, so any publicly-issued cert MITMs. Fail closed.
    raise newException(
      PgConnectionError, "sslmode=verify-ca/verify-full requires sslrootcert to be set"
    )
  if config.sslMode == sslVerifyFull and sslHost.len == 0:
    # hostaddr without host: there is no name to match the certificate
    # against (libpq raises the same way).
    raise newException(
      PgConnectionError, "A host name must be specified for a verified SSL connection"
    )
  # Pairing/sslmode compatibility is validated by `wrapped()` (connect-time
  # chokepoint) and defensively again at the top of this proc.

  if config.sslNegotiation == sslnDirect:
    await establishTls(conn, config, sslHost)
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
    await establishTls(conn, config, sslHost)
  of 'N':
    if config.sslMode in {sslRequire, sslVerifyCa, sslVerifyFull}:
      raise newException(PgConnectionError, "Server does not support SSL")
    # sslPrefer: server refused SSL – connection will proceed unencrypted.
    # WARNING: This is vulnerable to MITM downgrade attacks. A network
    # attacker can intercept the SSLRequest and reply 'N' to force
    # plaintext. Use sslRequire or stronger if security is needed.
    warnStderr "pg_connection: SSL refused by server, falling back to plaintext (sslmode=prefer)"
    if config.sslCert.len > 0:
      # Make the silent mTLS drop observable on the plaintext fallback.
      warnStderr "pg_connection: client certificate will NOT be sent over the plaintext fallback connection"
  else:
    raise newException(PgConnectionError, "Unexpected SSL response: " & $respChar)
