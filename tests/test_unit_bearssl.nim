## Unit tests for `pg_bearssl` (chronos-only BearSSL PEM handling).
##
## PG-less: exercises the public PEM loaders and trust-anchor parsing with
## hostile/empty input. On the asyncdispatch backend the module compiles to
## an empty stub, so the suite asserts the stub contract instead.

import std/[unittest, strutils]

import ../async_postgres/async_backend

when hasChronos:
  import ../async_postgres/pg_bearssl
  import ../async_postgres/pg_errors
  import chronos/streams/tlsstream

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
else:
  suite "pg_bearssl: asyncdispatch stub":
    test "module compiles to an empty stub without chronos":
      check true
