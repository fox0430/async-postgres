## BearSSL X509 certificate handling for SCRAM-SHA-256-PLUS channel binding.
## Wraps BearSSL callbacks to capture the leaf certificate DER bytes during
## TLS handshake, and provides trust anchor parsing from PEM data.

import async_backend

when hasChronos:
  import std/[base64, strutils]
  import chronos/streams/tlsstream
  import bearssl/[x509, rsa, ec, ssl]
  import pg_types

  type
    X509CertCaptureContext* = object
      ## X509 callback wrapper that captures the leaf certificate DER bytes
      ## during TLS handshake for SCRAM-SHA-256-PLUS channel binding.
      vtable*: ptr X509Class
      inner*: X509ClassPointerConst ## Original X509 engine to delegate to
      certDer*: ptr seq[byte] ## Points to PgConnection.serverCertDer
      depth*: int ## Certificate depth in chain (0 = leaf)
      capturing*: bool ## True while capturing leaf cert bytes

    TrustAnchorResult* = object
      store*: TrustAnchorStore
      backing*: seq[seq[byte]] ## Owns memory pointed to by trust anchor fields

  proc appendDnCallback(
      ctx: pointer, buf: pointer, len: csize_t
  ) {.cdecl, gcsafe, noSideEffect, raises: [].} =
    ## DN accumulation callback
    let s = cast[ptr seq[byte]](ctx)
    let p = cast[ptr UncheckedArray[byte]](buf)
    # int(len) traps RangeDefect > high(int); Defect leaks past raises: [] into C (UB).
    if len > csize_t(high(int)):
      return
    for i in 0 ..< int(len):
      s[].add(p[i])

  # X509 certificate capture callbacks
  # Intercepts BearSSL X509 callbacks to capture the leaf certificate DER bytes,
  # then delegates to the original X509 engine for actual validation.

  proc x509CaptureStartChain(
      ctx: X509ClassPointerConst, serverName: ConstCstring
  ) {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    self.depth = 0
    self.capturing = false
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].startChain(inner, serverName)

  proc x509CaptureStartCert(ctx: X509ClassPointerConst, length: uint32) {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    if self.depth == 0:
      self.capturing = true
      self.certDer[].setLen(0)
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].startCert(inner, length)

  proc x509CaptureAppend(
      ctx: X509ClassPointerConst, buf: ConstPtrByte, len: csize_t
  ) {.cdecl, raises: [].} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    # int(len) trap: Defect out of a cdecl frame into C is UB. Inner still gets raw csize_t.
    if self.capturing and len <= csize_t(high(int)):
      let n = int(len)
      let oldLen = self.certDer[].len
      self.certDer[].setLen(oldLen + n)
      copyMem(addr self.certDer[][oldLen], buf, n)
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].append(inner, buf, len)

  proc x509CaptureEndCert(ctx: X509ClassPointerConst) {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    if self.capturing:
      self.capturing = false
    self.depth += 1
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].endCert(inner)

  proc x509CaptureEndChain(ctx: X509ClassPointerConst): cuint {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    let inner = cast[ptr ptr X509Class](self.inner)
    result = inner[].endChain(inner)

  proc x509CaptureGetPkey(
      ctx: X509ClassPointerConstConst, usages: ptr cuint
  ): ConstPtrX509Pkey {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    let inner = cast[ptr ptr X509Class](self.inner)
    result = inner[].getPkey(inner, usages)

  var x509CertCaptureVtable {.global.} = X509Class(
    contextSize: uint(sizeof(X509CertCaptureContext)),
    startChain: x509CaptureStartChain,
    startCert: x509CaptureStartCert,
    append: x509CaptureAppend,
    endCert: x509CaptureEndCert,
    endChain: x509CaptureEndChain,
    getPkey: x509CaptureGetPkey,
  )

  # Public API

  proc installX509Capture*(
      captureCtx: var X509CertCaptureContext,
      eng: var SslEngineContext,
      serverCertDer: ptr seq[byte],
  ) =
    ## Install X509 capture wrapper to intercept server certificate DER bytes.
    captureCtx.inner = eng.x509ctx
    captureCtx.certDer = serverCertDer
    captureCtx.vtable = addr x509CertCaptureVtable
    sslEngineSetX509(eng, X509ClassPointerConst(addr captureCtx.vtable))

  proc rebindX509Capture*(
      captureCtx: var X509CertCaptureContext,
      eng: var SslEngineContext,
      serverCertDer: ptr seq[byte],
  ) =
    ## Rebind `captureCtx` and the engine's x509 slot to the caller's storage
    ## after a struct-level copy (e.g. reconnectInPlace) so callback pointers
    ## no longer target the transient source's fields. Preserves `inner`.
    captureCtx.certDer = serverCertDer
    sslEngineSetX509(eng, X509ClassPointerConst(addr captureCtx.vtable))

  const CertPemLabels = ["CERTIFICATE", "X509 CERTIFICATE"]
    ## "X509 CERTIFICATE" is OpenSSL's legacy label for the same DER.

  const KeyPemLabels = ["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"]
    ## PKCS#8, PKCS#1 and SEC1 banners, all of which BearSSL's skey decoder reads.

  const TrustedCertLabel = "TRUSTED CERTIFICATE"
    ## OpenSSL `-trustout`: DER plus X509_CERT_AUX, which BearSSL cannot honour.

  const EncryptedKeyMsg* =
    "client private key is passphrase-protected; only unencrypted keys are supported"
    ## asyncdispatch's no-passwd-callback guard in `ssl.nim` uses the same text.

  type PemBlock = object
    label: string
    body: string ## Base64 text without headers or line breaks
    encrypted: bool ## Legacy "Proc-Type: 4,ENCRYPTED" header
    malformed: bool ## Unterminated, mismatched END, or a header inside the body

  proc pemBlocks(pem: string): seq[PemBlock] =
    ## Split `pem` into undecoded blocks. Unlike `pemDecode`, tolerates text
    ## around blocks and RFC 1421 headers, as OpenSSL does.
    var open, inHeader = false
    var cur: PemBlock
    for rawLine in pem.splitLines:
      let line = rawLine.strip
      if inHeader and line.len > 0 and rawLine[0] in {' ', '\t'}:
        continue # RFC 1421 folded header continuation
      inHeader = false
      if line.startsWith("-----BEGIN ") and line.endsWith("-----"):
        if open:
          cur.malformed = true
          result.add(cur)
        open = true
        cur = PemBlock(label: line["-----BEGIN ".len ..< line.len - "-----".len])
      elif not open:
        discard
      elif line.startsWith("-----END "):
        open = false
        if line != "-----END " & cur.label & "-----":
          cur.malformed = true
        result.add(cur)
      elif ':' in line:
        # RFC 1421 headers precede the body; base64 never contains ':'.
        if cur.body.len > 0:
          cur.malformed = true
        else:
          inHeader = true
          if line.startsWith("Proc-Type:") and "ENCRYPTED" in line:
            cur.encrypted = true
      else:
        cur.body.add(line)
    if open:
      cur.malformed = true
      result.add(cur)

  proc decode(b: PemBlock): seq[byte] {.raises: [TLSStreamProtocolError].} =
    ## DER of an unencrypted block; raises when it is malformed.
    # BearSSL's decoder, unlike std/base64, rejects URL-safe or truncated text.
    if not (b.malformed or b.encrypted):
      try:
        let items = pemDecode("-----BEGIN X-----\n" & b.body & "\n-----END X-----\n")
        if items.len == 1:
          return items[0].data
      except TLSStreamProtocolError:
        discard
    raise newException(
      TLSStreamProtocolError, "Invalid PEM encoding in " & b.label & " block"
    )

  proc firstDerElement(
      data: openArray[byte]
  ): seq[byte] {.raises: [TLSStreamProtocolError].} =
    ## The leading DER SEQUENCE of `data`, i.e. the certificate of a
    ## TRUSTED CERTIFICATE block without its trailing X509_CERT_AUX.
    template malformed() =
      raise
        newException(TLSStreamProtocolError, "Malformed TRUSTED CERTIFICATE PEM block")

    if data.len < 2 or data[0] != 0x30'u8:
      malformed()
    var bodyLen = 0
    var hdrLen = 2
    if data[1] < 0x80'u8:
      bodyLen = int(data[1])
    else:
      let n = int(data[1] and 0x7F'u8)
      if n == 0 or n > 4 or data.len < 2 + n:
        malformed()
      for i in 0 ..< n:
        bodyLen = (bodyLen shl 8) or int(data[2 + i])
      hdrLen = 2 + n
    if bodyLen > data.len - hdrLen:
      malformed()
    @(data.toOpenArray(0, hdrLen + bodyLen - 1))

  iterator certificateDers(
      blocks: openArray[PemBlock], trusted: bool
  ): seq[byte] {.raises: [TLSStreamProtocolError].} =
    ## Non-empty certificate DER of every `CertPemLabels` block, plus
    ## TRUSTED CERTIFICATE blocks without their trust settings when `trusted`.
    for b in blocks:
      let isTrusted = b.label == TrustedCertLabel
      if b.label notin CertPemLabels and not (trusted and isTrusted):
        continue
      let data = decode(b)
      if data.len > 0:
        yield (if isTrusted: firstDerElement(data) else: data)

  proc loadCertificate*(
      pem: string
  ): TLSCertificate {.raises: [TLSStreamProtocolError].} =
    ## `TLSCertificate.init` that also accepts legacy banners, re-encoding
    ## each block as "CERTIFICATE" since chronos matches only that.
    var canonical = ""
    # Trust settings are irrelevant to the chain we present.
    for der in certificateDers(pemBlocks(pem), trusted = true):
      # BearSSL's PEM decoder has no line-length limit.
      canonical.add("-----BEGIN CERTIFICATE-----\n")
      canonical.add(base64.encode(der))
      canonical.add("\n-----END CERTIFICATE-----\n")
    if canonical.len == 0:
      raise newException(TLSStreamProtocolError, "Could not find any certificates")
    TLSCertificate.init(canonical)

  proc loadPrivateKey*(
      pem: string
  ): TLSPrivateKey {.raises: [TLSStreamProtocolError].} =
    ## `TLSPrivateKey.init` that also accepts every `KeyPemLabels` banner via
    ## chronos's DER overload. Loads the first non-empty such block; an
    ## encrypted key block before it raises `EncryptedKeyMsg`.
    for b in pemBlocks(pem):
      if not b.label.endsWith("PRIVATE KEY"):
        continue
      if b.label == "ENCRYPTED PRIVATE KEY" or b.encrypted:
        raise newException(
          TLSStreamProtocolError,
          if b.malformed:
            "Invalid PEM encoding in " & b.label & " block"
          else:
            EncryptedKeyMsg,
        )
      if b.label in KeyPemLabels:
        let data = decode(b)
        if data.len > 0:
          return TLSPrivateKey.init(data)
    raise newException(TLSStreamProtocolError, "Could not find private key")

  proc parseTrustAnchors*(pemData: string): TrustAnchorResult =
    ## Parse PEM-encoded CA certificates into a TrustAnchorStore.
    ## Returns both the store and the backing memory that anchor pointers reference.
    ##
    ## IMPORTANT: X509TrustAnchor contains raw `ptr byte` fields (dn.data,
    ## pkey.key.rsa.n/e, pkey.key.ec.q). TrustAnchorStore.new() only shallow-copies
    ## these structs, and BearSSL only stores a pointer to the anchor array.
    ## The caller MUST keep `result.backing` alive for the lifetime of the TLS session.
    let blocks = pemBlocks(pemData)
    var anchors: seq[X509TrustAnchor]
    var backing: seq[seq[byte]]

    try:
      for der in certificateDers(blocks, trusted = false):
        var dnBuf: seq[byte]
        var decoder: X509DecoderContext
        x509DecoderInit(decoder, appendDnCallback, addr dnBuf)
        x509DecoderPush(decoder, addr der[0], uint(der.len))

        if x509DecoderLastError(decoder) != 0:
          continue

        let pkey = x509DecoderGetPkey(decoder)
        if pkey.isNil:
          continue

        # Stage all buffers first: `addr buf[0]` needs non-empty guards, and a
        # mid-loop skip must not orphan a committed dnBuf in `backing`.
        if dnBuf.len == 0:
          continue

        var nBuf, eBuf, qBuf: seq[byte]
        if pkey.keyType == byte(KEYTYPE_RSA):
          if pkey.key.rsa.nlen == 0 or pkey.key.rsa.elen == 0:
            continue
          nBuf = newSeq[byte](pkey.key.rsa.nlen)
          copyMem(addr nBuf[0], pkey.key.rsa.n, nBuf.len)
          eBuf = newSeq[byte](pkey.key.rsa.elen)
          copyMem(addr eBuf[0], pkey.key.rsa.e, eBuf.len)
        elif pkey.keyType == byte(KEYTYPE_EC):
          if pkey.key.ec.qlen == 0:
            continue
          qBuf = newSeq[byte](pkey.key.ec.qlen)
          copyMem(addr qBuf[0], pkey.key.ec.q, qBuf.len)
        else:
          continue

        backing.add(dnBuf)
        let dnData = addr backing[^1][0]

        var anchor: X509TrustAnchor
        anchor.dn = X500Name(data: dnData, len: uint(dnBuf.len))
        anchor.flags =
          if x509DecoderIsCA(decoder) != 0:
            cuint(X509_TA_CA)
          else:
            0
        anchor.pkey.keyType = pkey.keyType

        if pkey.keyType == byte(KEYTYPE_RSA):
          backing.add(nBuf)
          backing.add(eBuf)
          anchor.pkey.key.rsa = RsaPublicKey(
            n: addr backing[^2][0],
            nlen: uint(nBuf.len),
            e: addr backing[^1][0],
            elen: uint(eBuf.len),
          )
        else: # KEYTYPE_EC
          backing.add(qBuf)
          anchor.pkey.key.ec = EcPublicKey(
            curve: pkey.key.ec.curve, q: addr backing[^1][0], qlen: uint(qBuf.len)
          )

        anchors.add(anchor)
    except TLSStreamProtocolError as e:
      # Same config fault as the anchorless case below.
      raise newException(PgConfigError, e.msg & " of CA data")

    if anchors.len == 0:
      # A config fault a reconnect loop can only spin on (see `pg_errors`).
      # TRUSTED CERTIFICATE is skipped: ignoring its trust settings widens trust.
      var msg = "No valid CA certificates found in PEM data"
      for b in blocks:
        if b.label == TrustedCertLabel:
          msg.add(
            "; TRUSTED CERTIFICATE blocks are ignored on the chronos backend, " &
              "re-export them as plain CERTIFICATE"
          )
          break
      raise newException(PgConfigError, msg)

    result = TrustAnchorResult(store: TrustAnchorStore.new(anchors), backing: backing)
