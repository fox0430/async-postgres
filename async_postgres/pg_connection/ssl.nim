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
import ../[async_backend, pg_errors, pg_protocol]
import types, buffer_io

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet
  when defined(ssl):
    import std/[dynlib, openssl, tempfiles, os]

when hasChronos or (hasAsyncDispatch and defined(ssl)):
  const PgAlpnProtocol = "postgresql"
    ## ALPN protocol name required for `sslnegotiation=direct` (PostgreSQL 17+).

when hasAsyncDispatch and defined(ssl):
  const PgAlpnWire = char(PgAlpnProtocol.len) & PgAlpnProtocol
    ## Length-prefixed wire form for `SSL_CTX_set_alpn_protos` (RFC 7301 §3.1).

when hasAsyncDispatch and defined(ssl):
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

  # Apple's system libssl/libcrypto omit some of these symbols; an eager
  # `{.dynlib.}` binding would abort the process at startup. Resolve lazily and
  # let callers handle nil.
  template defineLazySym(
      procName: untyped, FnType: typedesc, libPattern: string, symbol: string
  ) =
    var
      cachedFn {.global.}: FnType
      resolved {.global.}: bool
    proc procName*(): FnType =
      if not resolved:
        let lib = loadLibPattern(libPattern)
        if lib != nil:
          cachedFn = cast[FnType](symAddr(lib, symbol))
        resolved = true
      cachedFn

  defineLazySym(sslSet1Host, SslSet1HostFn, DLLSSLName, "SSL_set1_host")
  defineLazySym(sslGet0Param, SslGet0ParamFn, DLLSSLName, "SSL_get0_param")
  defineLazySym(
    x509VerifyParamSet1IpAsc, X509SetIpAscFn, DLLUtilName,
    "X509_VERIFY_PARAM_set1_ip_asc",
  )
  defineLazySym(sslGetRbio, SslGetBioFn, DLLSSLName, "SSL_get_rbio")
  defineLazySym(sslGetWbio, SslGetBioFn, DLLSSLName, "SSL_get_wbio")
  defineLazySym(
    sslGet0AlpnSelected, SslGet0AlpnSelectedFn, DLLSSLName, "SSL_get0_alpn_selected"
  )
  defineLazySym(
    sslCtxSetAlpnProtos, SslCtxSetAlpnProtosFn, DLLSSLName, "SSL_CTX_set_alpn_protos"
  )

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
    let getRbio = sslGetRbio()
    let getWbio = sslGetWbio()
    if getRbio == nil or getWbio == nil:
      raise newException(
        PgConnectionError,
        "TLS handshake: libssl does not export SSL_get_rbio / SSL_get_wbio",
      )
    let rbio = getRbio(ssl)
    let wbio = getWbio(ssl)
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
    let getAlpn = sslGet0AlpnSelected()
    if getAlpn == nil:
      raise newException(
        PgConnectionError,
        "sslnegotiation=direct: libssl does not export SSL_get0_alpn_selected",
      )
    var protoPtr: pointer
    var protoLen: cuint
    getAlpn(ssl, addr protoPtr, addr protoLen)
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
        let fn = x509VerifyParamSet1IpAsc()
        if fn == nil:
          raise newException(
            PgConnectionError,
            "sslmode=verify-full: libcrypto does not export " &
              "X509_VERIFY_PARAM_set1_ip_asc; cannot verify " & host,
          )
        let getParam = sslGet0Param()
        if getParam == nil:
          raise newException(
            PgConnectionError,
            "sslmode=verify-full: libssl does not export SSL_get0_param; " &
              "cannot verify " & host,
          )
        fn(getParam(sslHandle), host.cstring)
      else:
        let fn = sslSet1Host()
        if fn == nil:
          raise newException(
            PgConnectionError,
            "sslmode=verify-full: libssl does not export SSL_set1_host; " &
              "cannot verify " & host,
          )
        fn(sslHandle, host.cstring)
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

when hasChronos or (hasAsyncDispatch and defined(ssl)):
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

proc establishTls(conn: PgConnection, sslHost: string, direct: bool) {.async.} =
  ## Run the TLS handshake and wire up the encrypted reader/writer. `direct=true`
  ## enforces the "postgresql" ALPN selection (libpq `sslnegotiation=direct`,
  ## PostgreSQL 17+). Reads TLS parameters from `conn.config`.
  # Alias, not a copy: an async closure would deep-copy a `let config = ...`.
  template config(): ConnConfig =
    conn.config

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

    # BearSSL's serverName doubles as SNI wire value and X509 name check input.
    # Under verify-full it must be sslHost for BearSSL to verify; other modes
    # honor sslSni and RFC 6066 IP-literal suppression.
    let serverName =
      if config.sslMode == sslVerifyFull:
        sslHost
      else:
        sniName(sslHost, config.sslSni)
    # Advertise ALPN on every TLS connection (libpq 17 parity: SSL_set_alpn_protos
    # is called unconditionally); enforcement stays direct-only below.
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
      )
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
      var ctx: SslContext
      var tmpPath: string
      try:
        if config.sslMode in {sslVerifyCa, sslVerifyFull}:
          let (tmpFile, tp) = createTempFile("pg_ca_", ".pem")
          tmpPath = tp
          tmpFile.write(config.sslRootCert)
          tmpFile.close()
          ctx = newContext(verifyMode = CVerifyPeer, caFile = tmpPath)
        else:
          ctx = newContext(verifyMode = CVerifyNone)

        # Advertise ALPN unconditionally (libpq 17 parity). The missing-symbol
        # path is only fatal for direct mode; traditional mode degrades to no-ALPN.
        let setAlpn = sslCtxSetAlpnProtos()
        if setAlpn != nil:
          let rc = setAlpn(ctx.context, PgAlpnWire.cstring, cuint(PgAlpnWire.len))
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
        # asyncnet doesn't free the SslContext (no =destroy on std/net's type).
        # SSL_new inside wrapConnectedSocket takes its own ref, so destroying
        # here on both success and failure balances newContext without freeing
        # the SSL's copy — the socket's SSL_free drops that one on close.
        if ctx != nil:
          ctx.destroyContext()
        if tmpPath.len > 0:
          removeFile(tmpPath)
    else:
      raise
        newException(PgConnectionError, "SSL support requires compiling with -d:ssl")

proc negotiateSSL*(conn: PgConnection, config: ConnConfig, sslHost: string) {.async.} =
  ## Negotiate TLS. `sslnegotiation=postgres` (default) sends an SSLRequest first;
  ## `sslnegotiation=direct` starts TLS immediately (PostgreSQL 17+). `sslHost`
  ## is the name matched against the server certificate (libpq semantics: the
  ## entry's `host`, never `hostaddr`).
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

  if config.sslNegotiation == sslnDirect:
    await establishTls(conn, sslHost, direct = true)
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
    await establishTls(conn, sslHost, direct = false)
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
