import std/[strutils, base64]

import pkg/checksums/md5
import pkg/nimcrypto
import pkg/nimcrypto/pbkdf2
import pkg/nimcrypto/utils as ncutils

template burnStr*(s: var string) =
  ## Wipe a string's heap buffer. Compiler is prevented from eliding the
  ## write because nimcrypto's `burnMem` uses a volatile memset.
  if s.len > 0:
    ncutils.burnMem(addr s[0], s.len)
    s.setLen(0)

type ScramState* = object
  ## Intermediate state for SCRAM-SHA-256 authentication handshake.
  clientNonce*: string
  clientFirstBare*: string
  serverSignature*: array[32, byte]
  gs2Header*: string ## GS2 header: "n,," (no binding) or "p=tls-server-end-point,,"
  channelBindingData*: seq[byte] ## Channel binding data (empty for non-PLUS)

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
  var combined = newStringOfCap(password.len + user.len)
  combined.add(password)
  combined.add(user)
  var inner = getMD5(combined)
  burnStr(combined)
  var saltedInput = newStringOfCap(inner.len + salt.len)
  saltedInput.add(inner)
  burnStr(inner)
  for b in salt:
    saltedInput.add(char(b))
  result = "md5" & getMD5(saltedInput)
  burnStr(saltedInput)

proc scramEscapeUsername*(user: string): string =
  ## Escape username for SCRAM per RFC 5802 Section 5.1.
  ## '=' is encoded as '=3D' and ',' is encoded as '=2C'.
  result = user.replace("=", "=3D").replace(",", "=2C")

proc scramClientFirstMessage*(
    user: string, state: var ScramState, cbType: string = "", cbData: seq[byte] = @[]
): seq[byte] =
  ## Generate the SCRAM-SHA-256 client-first message with a random nonce.
  ## When `cbType` is non-empty, use channel binding (SCRAM-SHA-256-PLUS).
  var nonceBuf: array[24, byte]
  let n = randomBytes(nonceBuf)
  if n != 24:
    raise newException(CatchableError, "SCRAM: failed to generate random nonce")
  state.clientNonce = base64.encode(nonceBuf)
  state.clientFirstBare = "n=" & scramEscapeUsername(user) & ",r=" & state.clientNonce
  state.gs2Header =
    if cbType.len > 0:
      "p=" & cbType & ",,"
    else:
      "n,,"
  state.channelBindingData = cbData
  result = toBytes(state.gs2Header & state.clientFirstBare)

proc scramClientFirstMessage*(
    user: string,
    nonce: string,
    state: var ScramState,
    cbType: string = "",
    cbData: seq[byte] = @[],
): seq[byte] =
  ## Overload with explicit nonce for testing.
  state.clientNonce = nonce
  state.clientFirstBare = "n=" & scramEscapeUsername(user) & ",r=" & nonce
  state.gs2Header =
    if cbType.len > 0:
      "p=" & cbType & ",,"
    else:
      "n,,"
  state.channelBindingData = cbData
  result = toBytes(state.gs2Header & state.clientFirstBare)

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
  if iterations > 600_000:
    raise
      newException(CatchableError, "SCRAM: iteration count too large: " & $iterations)

  if not combinedNonce.startsWith(state.clientNonce):
    raise newException(
      CatchableError, "SCRAM: server nonce doesn't start with client nonce"
    )

  let salt =
    try:
      base64.decode(saltB64)
    except ValueError:
      raise newException(CatchableError, "SCRAM: invalid base64 in salt")

  var saltedPassword: seq[byte]
  var clientKey, storedKey, clientSignature, serverKey: array[32, byte]
  var cbindInput: seq[byte]
  var authMessage: string
  var clientProof: array[32, byte]
  try:
    saltedPassword = sha256.pbkdf2(password, salt, iterations, 32)
    clientKey = sha256.hmac(saltedPassword, "Client Key").data
    storedKey = sha256.digest(clientKey).data
    cbindInput = toBytes(state.gs2Header)
    cbindInput.add(state.channelBindingData)
    let clientFinalWithoutProof =
      "c=" & base64.encode(cbindInput) & ",r=" & combinedNonce
    authMessage =
      state.clientFirstBare & "," & serverFirstMsg & "," & clientFinalWithoutProof
    clientSignature = sha256.hmac(storedKey, authMessage).data

    for i in 0 ..< 32:
      clientProof[i] = clientKey[i] xor clientSignature[i]

    serverKey = sha256.hmac(saltedPassword, "Server Key").data
    state.serverSignature = sha256.hmac(serverKey, authMessage).data

    result = toBytes(clientFinalWithoutProof & ",p=" & base64.encode(clientProof))
  finally:
    ncutils.burnMem(saltedPassword)
    ncutils.burnMem(clientKey)
    ncutils.burnMem(storedKey)
    ncutils.burnMem(cbindInput)
    ncutils.burnMem(clientSignature)
    ncutils.burnMem(serverKey)
    ncutils.burnMem(clientProof)
    burnStr(authMessage)

proc computeTlsServerEndpoint*(certDer: openArray[byte]): seq[byte] =
  ## Compute tls-server-end-point channel binding data per RFC 5929.
  ## Always uses SHA-256, matching PostgreSQL (libpq) behavior.
  let hash = sha256.digest(certDer)
  result = @(hash.data)

proc scramVerifyServerFinal*(
    serverFinalData: openArray[byte], state: ScramState
): bool =
  ## Verify the server's final SCRAM-SHA-256 signature matches the expected value.
  ## The caller is expected to wipe `state.serverSignature` after verification
  ## since it is no longer needed and would aid an attacker impersonating the server.
  let serverFinalMsg = toString(serverFinalData)
  if not serverFinalMsg.startsWith("v="):
    return false
  let sig =
    try:
      base64.decode(serverFinalMsg[2 .. ^1])
    except ValueError:
      return false
  if sig.len != 32:
    return false
  var diff: byte = 0
  for i in 0 ..< 32:
    diff = diff or (byte(sig[i]) xor state.serverSignature[i])
  return diff == 0
