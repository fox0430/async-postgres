import std/[unittest, strutils]

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

  test "scramClientFinalMessage rejects zero iteration count":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=c2FsdA==,i=0"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage rejects excessive iteration count":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=c2FsdA==,i=600001"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramClientFinalMessage rejects invalid base64 salt":
    var state: ScramState
    discard scramClientFirstMessage("user", "myNonce", state)
    let serverFirst = "r=myNonceServerPart,s=!!!invalid!!!,i=4096"
    expect CatchableError:
      discard scramClientFinalMessage("password", toBytes(serverFirst), state)

  test "scramVerifyServerFinal rejects invalid base64 signature":
    var state: ScramState
    check scramVerifyServerFinal(toBytes("v=!!!invalid!!!"), state) == false
