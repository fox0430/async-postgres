## TLS/SSL negotiation for PostgreSQL connections.
##
## Implements the libpq-compatible SSLRequest handshake and the subsequent
## TLS handshake under both async backends:
##
## - **chronos**: BearSSL-based TLS via `chronos/streams/tlsstream`, with
##   custom trust anchor parsing (`parseTrustAnchors`) and X.509 capture for
##   SCRAM-SHA-256-PLUS channel binding (`installX509Capture`).
## - **asyncdispatch**: OpenSSL via `std/asyncnet.wrapConnectedSocket`, with PEM
##   trust anchors written to a temp file and `SSL_get_peer_certificate` used
##   for channel binding.
##
## Re-exported through `pg_connection.nim`.

import std/net
import ../[async_backend, pg_errors, pg_protocol]
import types, buffer_io

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet
  when defined(ssl):
    import std/[dynlib, openssl, tempfiles, os]

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

  # Apple's system libssl/libcrypto omit these symbols; an eager `{.dynlib.}`
  # binding would abort the process at startup. Resolve lazily and let callers
  # handle nil.
  var
    sslSet1HostFn: SslSet1HostFn
    sslSet1HostResolved: bool
    sslGet0ParamFn: SslGet0ParamFn
    sslGet0ParamResolved: bool
    x509SetIpAscFn: X509SetIpAscFn
    x509SetIpAscResolved: bool
    sslGetRbioFn: SslGetBioFn
    sslGetRbioResolved: bool
    sslGetWbioFn: SslGetBioFn
    sslGetWbioResolved: bool

  proc sslSet1Host*(): SslSet1HostFn =
    if not sslSet1HostResolved:
      let lib = loadLibPattern(DLLSSLName)
      if lib != nil:
        sslSet1HostFn = cast[SslSet1HostFn](symAddr(lib, "SSL_set1_host"))
      sslSet1HostResolved = true
    sslSet1HostFn

  proc sslGet0Param*(): SslGet0ParamFn =
    if not sslGet0ParamResolved:
      let lib = loadLibPattern(DLLSSLName)
      if lib != nil:
        sslGet0ParamFn = cast[SslGet0ParamFn](symAddr(lib, "SSL_get0_param"))
      sslGet0ParamResolved = true
    sslGet0ParamFn

  proc x509VerifyParamSet1IpAsc*(): X509SetIpAscFn =
    if not x509SetIpAscResolved:
      let lib = loadLibPattern(DLLUtilName)
      if lib != nil:
        x509SetIpAscFn =
          cast[X509SetIpAscFn](symAddr(lib, "X509_VERIFY_PARAM_set1_ip_asc"))
      x509SetIpAscResolved = true
    x509SetIpAscFn

  proc sslGetRbio*(): SslGetBioFn =
    if not sslGetRbioResolved:
      let lib = loadLibPattern(DLLSSLName)
      if lib != nil:
        sslGetRbioFn = cast[SslGetBioFn](symAddr(lib, "SSL_get_rbio"))
      sslGetRbioResolved = true
    sslGetRbioFn

  proc sslGetWbio*(): SslGetBioFn =
    if not sslGetWbioResolved:
      let lib = loadLibPattern(DLLSSLName)
      if lib != nil:
        sslGetWbioFn = cast[SslGetBioFn](symAddr(lib, "SSL_get_wbio"))
      sslGetWbioResolved = true
    sslGetWbioFn

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

proc negotiateSSL*(conn: PgConnection, config: ConnConfig, sslHost: string) {.async.} =
  ## Send SSLRequest and negotiate TLS if server accepts.
  ## `sslHost` is the host *name* the server certificate is verified against
  ## (the entry's `host`, never its `hostaddr` — libpq semantics).
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

      # BearSSL's `serverName` doubles as SNI wire value and X509 name check
      # input. Under verify-full it must be `sslHost` for BearSSL to verify;
      # other modes honor sslSni and RFC 6066 IP-literal suppression.
      let serverName =
        if config.sslMode == sslVerifyFull:
          sslHost
        else:
          sniName(sslHost, config.sslSni)

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
        )
      installX509Capture(
        conn.x509Capture, conn.tlsStream.ccontext.eng, addr conn.serverCertDer
      )
      await conn.tlsStream.handshake()
      conn.reader = conn.tlsStream.reader
      conn.writer = conn.tlsStream.writer
      conn.sslEnabled = true
    elif hasAsyncDispatch:
      when defined(ssl):
        var ctx: SslContext
        var tmpPath: string
        if config.sslMode in {sslVerifyCa, sslVerifyFull}:
          let (tmpFile, tp) = createTempFile("pg_ca_", ".pem")
          tmpPath = tp
          try:
            tmpFile.write(config.sslRootCert)
            tmpFile.close()
            ctx = newContext(verifyMode = CVerifyPeer, caFile = tmpPath)
          except:
            removeFile(tmpPath)
            raise
        else:
          ctx = newContext(verifyMode = CVerifyNone)

        try:
          let hostname = sniName(sslHost, config.sslSni)
          wrapConnectedSocket(ctx, conn.socket, handshakeAsClient, hostname)
          # asyncnet does no name matching and defers the handshake, so have
          # OpenSSL enforce the hostname/IP match itself during that handshake.
          if config.sslMode == sslVerifyFull:
            enforceVerifyFullIdentity(conn.socket.sslHandle, sslHost)
          # Drive the handshake here rather than letting the first application
          # send/recv trigger it: the peer cert is required *before* SCRAM to
          # decide channel binding, and OpenSSL only populates it once the
          # handshake completes.
          await driveTlsHandshake(conn.socket)
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
