import std/[strutils, base64]

import pkg/checksums/md5
import pkg/nimcrypto
import pkg/nimcrypto/pbkdf2

type ScramState* = object
  ## Intermediate state for SCRAM-SHA-256 authentication handshake.
  clientNonce*: string
  clientFirstBare*: string
  serverSignature*: array[32, byte]

proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc toString(data: openArray[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

proc md5AuthHash*(user, password: string, salt: array[4, byte]): string =
  ## Compute MD5 authentication hash for PostgreSQL.
  ## Returns "md5" followed by hex of MD5(MD5(password+user) + salt).
  let inner = getMD5(password & user)
  var saltedInput = inner
  for b in salt:
    saltedInput.add(char(b))
  result = "md5" & getMD5(saltedInput)

proc scramClientFirstMessage*(user: string, state: var ScramState): seq[byte] =
  ## Generate the SCRAM-SHA-256 client-first message with a random nonce.
  var nonceBuf: array[24, byte]
  let n = randomBytes(nonceBuf)
  doAssert n == 24
  state.clientNonce = base64.encode(nonceBuf)
  state.clientFirstBare = "n=" & user & ",r=" & state.clientNonce
  result = toBytes("n,," & state.clientFirstBare)

proc scramClientFirstMessage*(
    user: string, nonce: string, state: var ScramState
): seq[byte] =
  ## Overload with explicit nonce for testing.
  state.clientNonce = nonce
  state.clientFirstBare = "n=" & user & ",r=" & nonce
  result = toBytes("n,," & state.clientFirstBare)

proc scramClientFinalMessage*(
    password: string, serverFirstData: openArray[byte], state: var ScramState
): seq[byte] =
  ## Generate the SCRAM-SHA-256 client-final message from the server's first response.
  ## Computes the client proof and stores the expected server signature in `state`.
  let serverFirstMsg = toString(serverFirstData)
  var combinedNonce, saltB64: string
  var iterations: int
  var hasNonce, hasSalt, hasIterations: bool
  for part in serverFirstMsg.split(','):
    if part.startsWith("r="):
      combinedNonce = part[2 .. ^1]
      hasNonce = true
    elif part.startsWith("s="):
      saltB64 = part[2 .. ^1]
      hasSalt = true
    elif part.startsWith("i="):
      try:
        iterations = parseInt(part[2 .. ^1])
      except ValueError:
        raise newException(CatchableError, "SCRAM: invalid iteration count")
      hasIterations = true

  if not hasNonce:
    raise newException(CatchableError, "SCRAM: server response missing nonce (r=)")
  if not hasSalt:
    raise newException(CatchableError, "SCRAM: server response missing salt (s=)")
  if not hasIterations:
    raise newException(
      CatchableError, "SCRAM: server response missing iteration count (i=)"
    )
  if iterations <= 0:
    raise newException(CatchableError, "SCRAM: iteration count must be positive")

  if not combinedNonce.startsWith(state.clientNonce):
    raise newException(
      CatchableError, "SCRAM: server nonce doesn't start with client nonce"
    )

  let salt = base64.decode(saltB64)
  let saltedPassword = sha256.pbkdf2(password, salt, iterations, 32)
  let clientKey = sha256.hmac(saltedPassword, "Client Key").data
  let storedKey = sha256.digest(clientKey).data
  let clientFinalWithoutProof = "c=biws,r=" & combinedNonce
  let authMessage =
    state.clientFirstBare & "," & serverFirstMsg & "," & clientFinalWithoutProof
  let clientSignature = sha256.hmac(storedKey, authMessage).data

  var clientProof: array[32, byte]
  for i in 0 ..< 32:
    clientProof[i] = clientKey[i] xor clientSignature[i]

  let serverKey = sha256.hmac(saltedPassword, "Server Key").data
  state.serverSignature = sha256.hmac(serverKey, authMessage).data

  result = toBytes(clientFinalWithoutProof & ",p=" & base64.encode(clientProof))

proc scramVerifyServerFinal*(
    serverFinalData: openArray[byte], state: ScramState
): bool =
  ## Verify the server's final SCRAM-SHA-256 signature matches the expected value.
  let serverFinalMsg = toString(serverFinalData)
  if not serverFinalMsg.startsWith("v="):
    return false
  let sig = base64.decode(serverFinalMsg[2 .. ^1])
  if sig.len != 32:
    return false
  var diff: byte = 0
  for i in 0 ..< 32:
    diff = diff or (byte(sig[i]) xor state.serverSignature[i])
  return diff == 0
