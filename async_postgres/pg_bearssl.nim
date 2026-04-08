## BearSSL X509 certificate handling for SCRAM-SHA-256-PLUS channel binding.
## Wraps BearSSL callbacks to capture the leaf certificate DER bytes during
## TLS handshake, and provides trust anchor parsing from PEM data.

import async_backend, pg_types

when hasChronos:
  import chronos/streams/tlsstream
  import bearssl/[x509, rsa, ec, ssl]

  type
    X509CertCaptureContext* = object
      ## X509 callback wrapper that captures the leaf certificate DER bytes
      ## during TLS handshake for SCRAM-SHA-256-PLUS channel binding.
      vtable: ptr X509Class
      inner: X509ClassPointerConst ## Original X509 engine to delegate to
      certDer: ptr seq[byte] ## Points to PgConnection.serverCertDer
      depth: int ## Certificate depth in chain (0 = leaf)
      capturing: bool ## True while capturing leaf cert bytes

    TrustAnchorResult* = object
      store*: TrustAnchorStore
      backing*: seq[seq[byte]] ## Owns memory pointed to by trust anchor fields

  proc appendDnCallback(
      ctx: pointer, buf: pointer, len: uint
  ) {.exportc: "pg_append_dn_nim", cdecl, gcsafe, noSideEffect, raises: [].} =
    ## DN accumulation callback
    let s = cast[ptr seq[byte]](ctx)
    let p = cast[ptr UncheckedArray[byte]](buf)
    for i in 0 ..< int(len):
      s[].add(p[i])

  # C shim with const void* to satisfy BearSSL's br_x509_decoder_init signature
  {.
    emit: """
  static void pg_append_dn_shim(void *ctx, const void *buf, size_t len) {
    pg_append_dn_nim(ctx, (void*)buf, len);
  }
  """
  .}

  proc initX509Decoder(ctx: var X509DecoderContext, appendDnCtx: pointer) =
    {.
      emit: ["br_x509_decoder_init(&", ctx, ", pg_append_dn_shim, ", appendDnCtx, ");"]
    .}

  # X509 certificate capture callbacks
  # Intercepts BearSSL X509 callbacks to capture the leaf certificate DER bytes,
  # then delegates to the original X509 engine for actual validation.
  #
  # BearSSL X509 callbacks expect `const br_x509_class**` but the Nim binding
  # maps them to `ptr ptr X509Class` (non-const). Suppress the resulting
  # incompatible-pointer-types error from GCC for this module.
  {.localPassC: "-Wno-incompatible-pointer-types".}

  proc x509CaptureStartChain(ctx: ptr ptr X509Class, serverName: cstring) {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    self.depth = 0
    self.capturing = false
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].startChain(inner, serverName)

  proc x509CaptureStartCert(ctx: ptr ptr X509Class, length: uint32) {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    if self.depth == 0:
      self.capturing = true
      self.certDer[].setLen(0)
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].startCert(inner, length)

  proc x509CaptureAppend(
      ctx: ptr ptr X509Class, buf: ptr byte, len: csize_t
  ) {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    if self.capturing:
      let oldLen = self.certDer[].len
      self.certDer[].setLen(oldLen + int(len))
      copyMem(addr self.certDer[][oldLen], buf, int(len))
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].append(inner, buf, len)

  proc x509CaptureEndCert(ctx: ptr ptr X509Class) {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    if self.capturing:
      self.capturing = false
    self.depth += 1
    let inner = cast[ptr ptr X509Class](self.inner)
    inner[].endCert(inner)

  proc x509CaptureEndChain(ctx: ptr ptr X509Class): cuint {.cdecl.} =
    let self = cast[ptr X509CertCaptureContext](ctx)
    let inner = cast[ptr ptr X509Class](self.inner)
    result = inner[].endChain(inner)

  proc x509CaptureGetPkey(
      ctx: ptr ptr X509Class, usages: ptr cuint
  ): ptr X509Pkey {.cdecl.} =
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

  proc parseTrustAnchors*(pemData: string): TrustAnchorResult =
    ## Parse PEM-encoded CA certificates into a TrustAnchorStore.
    ## Returns both the store and the backing memory that anchor pointers reference.
    ##
    ## IMPORTANT: X509TrustAnchor contains raw `ptr byte` fields (dn.data,
    ## pkey.key.rsa.n/e, pkey.key.ec.q). TrustAnchorStore.new() only shallow-copies
    ## these structs, and BearSSL only stores a pointer to the anchor array.
    ## The caller MUST keep `result.backing` alive for the lifetime of the TLS session.
    let items = pemDecode(pemData)
    var anchors: seq[X509TrustAnchor]
    var backing: seq[seq[byte]]

    for item in items:
      if item.name != "CERTIFICATE":
        continue

      var dnBuf: seq[byte]
      var decoder: X509DecoderContext
      initX509Decoder(decoder, addr dnBuf)
      x509DecoderPush(decoder, unsafeAddr item.data[0], uint(item.data.len))

      if x509DecoderLastError(decoder) != 0:
        continue

      let pkey = x509DecoderGetPkey(decoder)
      if pkey.isNil:
        continue

      # Deep-copy DN
      backing.add(dnBuf)
      let dnData = addr backing[^1][0]

      # Deep-copy public key and build anchor
      var anchor: X509TrustAnchor
      anchor.dn = X500Name(data: dnData, len: uint(dnBuf.len))
      anchor.flags =
        if x509DecoderIsCA(decoder) != 0:
          cuint(X509_TA_CA)
        else:
          0
      anchor.pkey.keyType = pkey.keyType

      if pkey.keyType == byte(KEYTYPE_RSA):
        var nBuf = newSeq[byte](pkey.key.rsa.nlen)
        copyMem(addr nBuf[0], pkey.key.rsa.n, nBuf.len)
        backing.add(nBuf)
        var eBuf = newSeq[byte](pkey.key.rsa.elen)
        copyMem(addr eBuf[0], pkey.key.rsa.e, eBuf.len)
        backing.add(eBuf)
        anchor.pkey.key.rsa = RsaPublicKey(
          n: addr backing[^2][0],
          nlen: uint(nBuf.len),
          e: addr backing[^1][0],
          elen: uint(eBuf.len),
        )
      elif pkey.keyType == byte(KEYTYPE_EC):
        var qBuf = newSeq[byte](pkey.key.ec.qlen)
        copyMem(addr qBuf[0], pkey.key.ec.q, qBuf.len)
        backing.add(qBuf)
        anchor.pkey.key.ec = EcPublicKey(
          curve: pkey.key.ec.curve, q: addr backing[^1][0], qlen: uint(qBuf.len)
        )
      else:
        continue

      anchors.add(anchor)

    if anchors.len == 0:
      raise
        newException(PgError, "No valid CA certificates found in PEM data")

    result = TrustAnchorResult(store: TrustAnchorStore.new(anchors), backing: backing)
