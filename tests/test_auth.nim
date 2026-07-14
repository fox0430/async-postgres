import std/[unittest, strutils, base64]

import pkg/nimcrypto
import pkg/nimcrypto/pbkdf2

import ../async_postgres/pg_auth

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc toString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

suite "MD5 authentication":
  test "md5AuthHash format":
    let hash = md5AuthHash("testuser", "testpass", [0x01'u8, 0x02, 0x03, 0x04])
    check hash.startsWith("md5")
    check hash.len == 35 # "md5" + 32 hex chars

  test "md5AuthHash deterministic":
    let salt = [0xDE'u8, 0xAD, 0xBE, 0xEF]
    let h1 = md5AuthHash("user", "pass", salt)
    let h2 = md5AuthHash("user", "pass", salt)
    check h1 == h2

  test "md5AuthHash different inputs":
    let salt = [0x01'u8, 0x02, 0x03, 0x04]
    let h1 = md5AuthHash("user1", "pass", salt)
    let h2 = md5AuthHash("user2", "pass", salt)
    check h1 != h2

  test "md5AuthHash different salts":
    let h1 = md5AuthHash("user", "pass", [0x01'u8, 0x02, 0x03, 0x04])
    let h2 = md5AuthHash("user", "pass", [0x05'u8, 0x06, 0x07, 0x08])
    check h1 != h2

suite "SCRAM username escaping":
  test "scramEscapeUsername with no special chars":
    check scramEscapeUsername("user") == "user"

  test "scramEscapeUsername escapes '='":
    check scramEscapeUsername("user=1") == "user=3D1"

  test "scramEscapeUsername escapes ','":
    check scramEscapeUsername("user,name") == "user=2Cname"

  test "scramEscapeUsername escapes both '=' and ','":
    check scramEscapeUsername("a=b,c") == "a=3Db=2Cc"

  test "scramEscapeUsername escapes '=' before ','":
    # '=' must be escaped first so that '=2C' introduced by comma escaping
    # is not double-escaped.
    check scramEscapeUsername("=,") == "=3D=2C"

suite "SCRAM-SHA-256":
  test "clientFirstMessage with fixed nonce":
    var state: ScramState
    let msg = scramClientFirstMessage("user", "rOprNGfwEbeRWgbNEkqO", state)
    check toString(msg) == "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
    check state.clientNonce == "rOprNGfwEbeRWgbNEkqO"
    check state.clientFirstBare == "n=user,r=rOprNGfwEbeRWgbNEkqO"

  test "clientFirstMessage escapes username":
    var state: ScramState
    let msg = scramClientFirstMessage("u=ser,1", "testNonce", state)
    check toString(msg) == "n,,n=u=3Dser=2C1,r=testNonce"
    check state.clientFirstBare == "n=u=3Dser=2C1,r=testNonce"

  test "clientFirstMessage with random nonce":
    var state: ScramState
    let msg = scramClientFirstMessage("user", state)
    let msgStr = toString(msg)
    check msgStr.startsWith("n,,n=user,r=")
    check state.clientNonce.len > 0
    check state.clientFirstBare.startsWith("n=user,r=")

  test "full SCRAM-SHA-256 exchange (RFC 7677 test vectors)":
    var state: ScramState

    # Step 1: Client first message
    let clientFirst = scramClientFirstMessage("user", "rOprNGfwEbeRWgbNEkqO", state)
    check toString(clientFirst) == "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"

    # Step 2: Simulated server first message
    let serverFirst =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"

    # Step 3: Client final message
    let clientFinal = scramClientFinalMessage("pencil", toBytes(serverFirst), state)
    let expected =
      "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    check toString(clientFinal) == expected

    # Step 4: Verify server final message
    let serverFinal = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
    check scramVerifyServerFinal(toBytes(serverFinal), state) == true

    # Step 5: Wrong signature fails
    let wrongFinal = "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    check scramVerifyServerFinal(toBytes(wrongFinal), state) == false

  test "scramVerifyServerFinal rejects invalid format":
    var state: ScramState
    check scramVerifyServerFinal(toBytes("invalid"), state) == false
    check scramVerifyServerFinal(toBytes("v=short"), state) == false

  test "scramClientFinalMessage rejects mismatched nonce":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=differentNonce,s=c2FsdA==,i=4096"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage rejects missing salt":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,i=4096"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage rejects missing iteration count":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=c2FsdA=="
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage rejects invalid iteration count":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=c2FsdA==,i=abc"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage rejects iteration count below 4096":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=c2FsdA==,i=4095"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage accepts minimum iteration count 4096":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage rejects excessive iteration count":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=c2FsdA==,i=10000001"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage accepts iteration count above 600000":
    # PG16+ scram_iterations may legitimately exceed the OWASP-recommended 600k
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=1000000"
    discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage enforces custom maxIterations":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=5001"
    expect CatchableError:
      discard scramClientFinalMessage(
        "password", toBytes(serverFirst), state, maxIterations = 5000
      )
    discard scramClientFirstMessage("user", "myNonce", state)
    let atLimit = "r=myNonceServerPart,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=5000"
    discard
      scramClientFinalMessage("password", toBytes(atLimit), state, maxIterations = 5000)

  test "scramClientFinalMessage rejects invalid base64 salt":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=!!!invalid!!!,i=4096"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramVerifyServerFinal rejects invalid base64 signature":
    var state: ScramState
    check scramVerifyServerFinal(toBytes("v=!!!invalid!!!"), state) == false

suite "SCRAM-SHA-256-PLUS channel binding":
  test "clientFirstMessage with channel binding":
    var state: ScramState
    let cbData = @[0x01'u8, 0x02, 0x03]
    let msg = scramClientFirstMessage(
      "user", "testNonce", state, cbType = "tls-server-end-point", cbData = cbData
    )
    check toString(msg) == "p=tls-server-end-point,,n=user,r=testNonce"
    check state.gs2Header == "p=tls-server-end-point,,"
    check state.channelBindingData == cbData
    check state.clientFirstBare == "n=user,r=testNonce"

  test "clientFirstMessage without channel binding preserves gs2Header":
    var state: ScramState
    let msg = scramClientFirstMessage("user", "testNonce", state)
    check toString(msg) == "n,,n=user,r=testNonce"
    check state.gs2Header == "n,,"
    check state.channelBindingData.len == 0

  test "clientFirstMessage emits y,, when CB supported but unused (downgrade)":
    var state: ScramState
    let msg =
      scramClientFirstMessage("user", "testNonce", state, cbSupportedButUnused = true)
    check toString(msg) == "y,,n=user,r=testNonce"
    check state.gs2Header == "y,,"
    check state.channelBindingData.len == 0

  test "clientFirstMessage prefers p=,, over y,, when channel binding is in use":
    var state: ScramState
    let cbData = @[0x01'u8, 0x02, 0x03]
    discard scramClientFirstMessage(
      "user",
      "testNonce",
      state,
      cbType = "tls-server-end-point",
      cbData = cbData,
      cbSupportedButUnused = true,
    )
    check state.gs2Header == "p=tls-server-end-point,,"

  test "clientFinalMessage with channel binding encodes cbind-input":
    var state: ScramState
    let cbData = @[0xAA'u8, 0xBB, 0xCC]
    discard scramClientFirstMessage(
      "user",
      "rOprNGfwEbeRWgbNEkqO",
      state,
      cbType = "tls-server-end-point",
      cbData = cbData,
    )
    let serverFirst =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    let clientFinal = scramClientFinalMessage("pencil", toBytes(serverFirst), state)
    let clientFinalStr = toString(clientFinal)
    # c= should be base64(gs2Header + cbData), NOT "biws"
    let expectedCbind = base64.encode("p=tls-server-end-point,," & "\xAA\xBB\xCC")
    check clientFinalStr.startsWith("c=" & expectedCbind & ",r=")

  test "clientFinalMessage without channel binding produces c=biws":
    var state: ScramState
    discard scramClientFirstMessage("user", "rOprNGfwEbeRWgbNEkqO", state)
    let serverFirst =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    let clientFinal = scramClientFinalMessage("pencil", toBytes(serverFirst), state)
    let clientFinalStr = toString(clientFinal)
    check clientFinalStr.startsWith("c=biws,r=")

  test "RFC 7677 test vectors still pass with default params":
    # Backward compatibility: existing test vectors work unchanged
    var state: ScramState
    let clientFirst = scramClientFirstMessage("user", "rOprNGfwEbeRWgbNEkqO", state)
    check toString(clientFirst) == "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
    let serverFirst =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    let clientFinal = scramClientFinalMessage("pencil", toBytes(serverFirst), state)
    let expected =
      "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    check toString(clientFinal) == expected
    let serverFinal = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
    check scramVerifyServerFinal(toBytes(serverFinal), state) == true

  test "full SCRAM-SHA-256-PLUS exchange with server signature verification":
    # Simulate a full exchange with channel binding and verify the server
    # signature is correctly computed (round-trip: the authMessage changes
    # when channel binding is used, so serverSignature must reflect that).
    var state: ScramState
    let cbData = @[0xDE'u8, 0xAD, 0xBE, 0xEF]
    let clientFirst = scramClientFirstMessage(
      "user",
      "rOprNGfwEbeRWgbNEkqO",
      state,
      cbType = "tls-server-end-point",
      cbData = cbData,
    )
    check toString(clientFirst) ==
      "p=tls-server-end-point,,n=user,r=rOprNGfwEbeRWgbNEkqO"

    let serverFirst =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    let clientFinal = scramClientFinalMessage("pencil", toBytes(serverFirst), state)
    let clientFinalStr = toString(clientFinal)

    # c= value must NOT be "biws" (that's the non-PLUS value)
    check not clientFinalStr.startsWith("c=biws,")

    # Verify server signature: manually compute expected value
    let salt = base64.decode("W22ZaJ0SNY7soEsUEjb6gQ==")
    let saltedPassword = sha256.pbkdf2("pencil", salt, 4096, 32)
    let serverKey = sha256.hmac(saltedPassword, "Server Key").data
    var cbindInput = toBytes("p=tls-server-end-point,,")
    cbindInput.add(cbData)
    let clientFinalWithoutProof =
      "c=" & base64.encode(cbindInput) &
      ",r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
    let authMessage =
      "n=user,r=rOprNGfwEbeRWgbNEkqO" & "," & serverFirst & "," & clientFinalWithoutProof
    let expectedSig = sha256.hmac(serverKey, authMessage).data

    # Build server-final message and verify
    let serverFinal = "v=" & base64.encode(expectedSig)
    check scramVerifyServerFinal(toBytes(serverFinal), state) == true

    # Wrong signature must fail
    check scramVerifyServerFinal(
      toBytes("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="), state
    ) == false

  test "channel binding changes server signature vs non-PLUS":
    # The server signature for the same credentials must differ between
    # SCRAM-SHA-256 and SCRAM-SHA-256-PLUS because authMessage changes.
    var stateNormal: ScramState
    discard scramClientFirstMessage("user", "rOprNGfwEbeRWgbNEkqO", stateNormal)
    let serverFirst =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    discard scramClientFinalMessage("pencil", toBytes(serverFirst), stateNormal)

    var statePlus: ScramState
    discard scramClientFirstMessage(
      "user",
      "rOprNGfwEbeRWgbNEkqO",
      statePlus,
      cbType = "tls-server-end-point",
      cbData = @[0x01'u8],
    )
    discard scramClientFinalMessage("pencil", toBytes(serverFirst), statePlus)

    check stateNormal.serverSignature != statePlus.serverSignature

  test "clientFirstMessage with random nonce and channel binding":
    var state: ScramState
    let cbData = @[0xFF'u8, 0xFE]
    let msg = scramClientFirstMessage(
      "user", state, cbType = "tls-server-end-point", cbData = cbData
    )
    let msgStr = toString(msg)
    check msgStr.startsWith("p=tls-server-end-point,,n=user,r=")
    check state.gs2Header == "p=tls-server-end-point,,"
    check state.channelBindingData == cbData
    check state.clientNonce.len > 0

  test "clientFirstMessage with channel binding and special username":
    var state: ScramState
    let cbData = @[0x01'u8, 0x02]
    let msg = scramClientFirstMessage(
      "u=ser,1", "testNonce", state, cbType = "tls-server-end-point", cbData = cbData
    )
    check toString(msg) == "p=tls-server-end-point,,n=u=3Dser=2C1,r=testNonce"
    check state.clientFirstBare == "n=u=3Dser=2C1,r=testNonce"
    check state.gs2Header == "p=tls-server-end-point,,"

  test "clientFirstMessage with channel binding type but empty cbData":
    var state: ScramState
    let msg = scramClientFirstMessage(
      "user", "testNonce", state, cbType = "tls-server-end-point", cbData = @[]
    )
    check toString(msg) == "p=tls-server-end-point,,n=user,r=testNonce"
    check state.gs2Header == "p=tls-server-end-point,,"
    check state.channelBindingData.len == 0
    # c= should be base64("p=tls-server-end-point,,") with no trailing binding data
    let serverFirst =
      "r=testNonce%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," & "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    let clientFinal = scramClientFinalMessage("pencil", toBytes(serverFirst), state)
    let clientFinalStr = toString(clientFinal)
    let expectedCbind = base64.encode("p=tls-server-end-point,,")
    check clientFinalStr.startsWith("c=" & expectedCbind & ",r=")

  # Minimal X.509 shell: outer SEQ + empty tbs + AlgorithmIdentifier(oid).
  proc syntheticCert(oid: openArray[byte]): seq[byte] =
    let oidBlock = @[byte 0x06, byte(oid.len)] & @oid
    let sigAlg = @[byte 0x30, byte(oidBlock.len)] & oidBlock
    let tbs = @[byte 0x30, 0x00]
    let inner = tbs & sigAlg
    result = @[byte 0x30, byte(inner.len)] & inner

  test "computeTlsServerEndpoint falls back to SHA-256 on parse failure":
    let empty = computeTlsServerEndpoint(@[])
    check empty.len == 32
    check empty == @(sha256.digest(@(newSeq[byte](0))).data)
    let garbage = @[byte 0x30, 0x82, 0x01, 0x00]
    let g = computeTlsServerEndpoint(garbage)
    check g.len == 32
    check g == @(sha256.digest(garbage).data)

  test "computeTlsServerEndpoint uses SHA-256 for sha256WithRSAEncryption":
    let cert =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0B])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 32
    check binding == @(sha256.digest(cert).data)

  test "computeTlsServerEndpoint uses SHA-384 for sha384WithRSAEncryption":
    let cert =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0C])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 48
    check binding == @(sha384.digest(cert).data)

  test "computeTlsServerEndpoint uses SHA-512 for sha512WithRSAEncryption":
    let cert =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0D])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 64
    check binding == @(sha512.digest(cert).data)

  test "computeTlsServerEndpoint uses SHA-384 for ecdsa-with-SHA384":
    let cert = syntheticCert([byte 0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x03])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 48
    check binding == @(sha384.digest(cert).data)

  test "computeTlsServerEndpoint uses SHA-512 for ecdsa-with-SHA512":
    let cert = syntheticCert([byte 0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x04])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 64
    check binding == @(sha512.digest(cert).data)

  test "computeTlsServerEndpoint uses SHA-384 for dsa-with-SHA384":
    let cert =
      syntheticCert([byte 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x03, 0x03])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 48
    check binding == @(sha384.digest(cert).data)

  test "computeTlsServerEndpoint promotes SHA-1 to SHA-256 per RFC 5929":
    let cert =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x05])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 32
    check binding == @(sha256.digest(cert).data)

  test "computeTlsServerEndpoint promotes MD5 to SHA-256 per RFC 5929":
    let cert =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x04])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 32
    check binding == @(sha256.digest(cert).data)

  test "computeTlsServerEndpoint falls back to SHA-256 for unknown OID":
    let cert = syntheticCert([byte 0x2A, 0x03, 0x04, 0x05])
    let binding = computeTlsServerEndpoint(cert)
    check binding.len == 32
    check binding == @(sha256.digest(cert).data)

  test "computeTlsServerEndpoint is deterministic":
    let cert =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0C])
    check computeTlsServerEndpoint(cert) == computeTlsServerEndpoint(cert)

  test "computeTlsServerEndpoint differs for different certs":
    let cert1 =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0B])
    let cert2 =
      syntheticCert([byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0C])
    check computeTlsServerEndpoint(cert1) != computeTlsServerEndpoint(cert2)
