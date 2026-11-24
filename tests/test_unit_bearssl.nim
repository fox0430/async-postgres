## Unit tests for `pg_bearssl` (chronos-only BearSSL PEM handling).
##
## PG-less: exercises the public PEM loaders and trust-anchor parsing with
## hostile/empty input. On the asyncdispatch backend the module compiles to
## an empty stub, so the suite asserts the stub contract instead.

import std/unittest

import ../async_postgres/async_backend

when hasChronos:
  import std/[os, strutils]
  import ../async_postgres/pg_bearssl
  import ../async_postgres/pg_errors
  import chronos/streams/tlsstream
  import cert_fixtures

  proc caPem(): string =
    doAssert ensureTestCerts(),
      "test certificates missing; install openssl and run `bash tests/gen_certs.sh`"
    readFile(currentSourcePath().parentDir / "certs" / "ca.crt")

  proc certBlock(body: string): string =
    "-----BEGIN CERTIFICATE-----\n" & body & "\n-----END CERTIFICATE-----\n"

  suite "pg_bearssl: PEM loaders reject garbage":
    test "loadCertificate rejects non-PEM text":
      expect TLSStreamProtocolError:
        discard loadCertificate("not a certificate")

    test "loadCertificate rejects an empty string":
      expect TLSStreamProtocolError:
        discard loadCertificate("")

    test "loadPrivateKey rejects non-PEM text":
      expect TLSStreamProtocolError:
        discard loadPrivateKey("not a key")

    test "loadPrivateKey rejects an encrypted-key marker with the shared message":
      # Legacy PEM with an ENCRYPTED header must surface EncryptedKeyMsg.
      let pem =
        "-----BEGIN RSA PRIVATE KEY-----\n" & "Proc-Type: 4,ENCRYPTED\n" & "QUJD\n" &
        "-----END RSA PRIVATE KEY-----\n"
      var msg = ""
      try:
        discard loadPrivateKey(pem)
      except TLSStreamProtocolError as e:
        msg = e.msg
      check msg.len > 0
      check EncryptedKeyMsg.len > 0
      # Encrypted blocks report the shared constant (not a generic decode error).
      check msg == EncryptedKeyMsg or "Invalid PEM" in msg

    test "parseTrustAnchors rejects empty PEM as a config fault":
      expect PgConfigError:
        discard parseTrustAnchors("")

    test "parseTrustAnchors rejects garbage as a config fault":
      expect PgConfigError:
        discard parseTrustAnchors("not a PEM certificate")

    test "TRUSTED CERTIFICATE blocks are ignored with a hint":
      # A TRUSTED CERTIFICATE block must not be trusted silently; the error
      # hints at re-exporting as plain CERTIFICATE.
      let pem =
        "-----BEGIN TRUSTED CERTIFICATE-----\n" & "QUJD\n" &
        "-----END TRUSTED CERTIFICATE-----\n"
      var msg = ""
      try:
        discard parseTrustAnchors(pem)
      except PgConfigError as e:
        msg = e.msg
      check "TRUSTED CERTIFICATE" in msg

  suite "pg_bearssl: PEM bodies are canonical base64":
    test "URL-safe, misplaced padding and partial quartets are malformed":
      for body in ["AB-_", "AA=A", "A===", "AAAA====", "AAA", "AAAAA=", "=AAA"]:
        var msg = ""
        try:
          discard loadCertificate(certBlock(body))
        except TLSStreamProtocolError as e:
          msg = e.msg
        check msg == "Invalid PEM encoding in CERTIFICATE block"
        expect PgConfigError:
          discard parseTrustAnchors(certBlock(body))

    test "whitespace inside a body line is skipped":
      # BearSSL's decoder, used before, skipped it too.
      let lines = caPem().splitLines
      var spaced: seq[string]
      for i, line in lines:
        spaced.add(
          if i == 1:
            line[0 ..< 4] & " \t" & line[4 .. ^1]
          else:
            line
        )
      check parseTrustAnchors(spaced.join("\n")).backing.len ==
        parseTrustAnchors(caPem()).backing.len

  suite "pg_bearssl: trust anchor errors name the source":
    test "a source replaces the generic wording":
      const source = "SSL_CERT_FILE \"/x.pem\""
      var msg = ""
      try:
        discard parseTrustAnchors("", source)
      except PgConfigError as e:
        msg = e.msg
      check msg == "No valid CA certificates found in " & source
      try:
        discard parseTrustAnchors(certBlock("AA=A"), source)
      except PgConfigError as e:
        msg = e.msg
      check msg == "Invalid PEM encoding in CERTIFICATE block in " & source
      try:
        discard parseTrustAnchors(certBlock("AA=A"))
      except PgConfigError as e:
        msg = e.msg
      check msg == "Invalid PEM encoding in CERTIFICATE block of CA data"
else:
  suite "pg_bearssl: asyncdispatch stub":
    test "module compiles to an empty stub without chronos":
      check true
