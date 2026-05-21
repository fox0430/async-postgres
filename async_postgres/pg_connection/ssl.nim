## TLS/SSL negotiation for PostgreSQL connections.
##
## Implements the libpq-compatible SSLRequest handshake and the subsequent
## TLS handshake under both async backends:
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
import types

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl
elif hasAsyncDispatch:
  import std/asyncnet
  import buffer_io
  when defined(ssl):
    import std/[net, openssl, tempfiles, os]

proc negotiateSSL*(conn: PgConnection, config: ConnConfig) {.async.} =
  ## Send SSLRequest and negotiate TLS if server accepts.
  let sslReq = encodeSSLRequest()
  var respChar: char

  when hasChronos:
    discard await conn.transport.write(sslReq)
    var response: array[1, byte]
    let n = await conn.transport.readOnce(addr response[0], 1)
    if n == 0:
      raise newException(PgConnectionError, "Connection closed during SSL negotiation")
    respChar = char(response[0])
  elif hasAsyncDispatch:
    await conn.socket.sendRawBytes(sslReq)
    let respStr = await conn.socket.recv(1)
    if respStr.len == 0:
      raise newException(PgConnectionError, "Connection closed during SSL negotiation")
    respChar = respStr[0]

  case respChar
  of 'S':
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

      let serverName = if config.sslMode == sslVerifyFull: config.host else: ""

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
        )
      else:
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

        try:
          let hostname = if config.sslMode == sslVerifyFull: config.host else: ""
          wrapConnectedSocket(ctx, conn.socket, handshakeAsClient, hostname)
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
