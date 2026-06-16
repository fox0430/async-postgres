## In-process PostgreSQL wire-protocol mock server.
##
## Starts a TCP listener on 127.0.0.1 with an ephemeral port and lets test code
## script arbitrary byte sequences back to a real `PgConnection`. Used to
## exercise code paths that a real PostgreSQL server would never reproduce on
## demand: mid-message disconnects, malformed responses, truncated frames,
## stalled senders, etc.
##
## Works with both `chronos` and `asyncdispatch` via the same unified API.
## The chronos and asyncdispatch branches expose identical `MockServer` /
## `MockClient` types and procs so tests can be backend-agnostic.

import ../async_postgres/[async_backend, pg_protocol]

when hasAsyncDispatch:
  import std/asyncnet

# Types and low-level transport

type AutoKeepaliveResult* =
  tuple[msgType: char, receive: int64, flush: int64, apply: int64]

when hasChronos:
  type
    MockServer* = object
      server: StreamServer
      port*: int

    MockClient* = StreamTransport

  proc startMockServer*(): MockServer =
    let server = createStreamServer(initTAddress("127.0.0.1", 0))
    MockServer(server: server, port: int(server.localAddress().port))

  proc accept*(ms: MockServer): Future[MockClient] =
    ms.server.accept()

  proc closeServer*(ms: MockServer) {.async.} =
    await ms.server.closeWait()

  proc closeClient*(client: MockClient) {.async.} =
    await client.closeWait()

  proc readN*(client: MockClient, n: int): Future[seq[byte]] {.async.} =
    result = newSeq[byte](n)
    var offset = 0
    while offset < n:
      let bytesRead = await client.readOnce(addr result[offset], n - offset)
      if bytesRead == 0:
        raise newException(CatchableError, "Connection closed prematurely")
      offset += bytesRead

  proc sendBytes*(client: MockClient, data: seq[byte]) {.async.} =
    if data.len > 0:
      discard await client.write(data)

elif hasAsyncDispatch:
  type
    MockServer* = object
      socket: AsyncSocket
      port*: int

    MockClient* = AsyncSocket

  proc startMockServer*(): MockServer =
    let sock = newAsyncSocket(buffered = false)
    sock.setSockOpt(OptReuseAddr, true)
    sock.bindAddr(Port(0))
    let port = int(sock.getLocalAddr()[1])
    sock.listen()
    MockServer(socket: sock, port: port)

  proc accept*(ms: MockServer): Future[MockClient] =
    ms.socket.accept()

  proc closeServer*(ms: MockServer) {.async.} =
    ms.socket.close()

  proc closeClient*(client: MockClient) {.async.} =
    client.close()

  proc readN*(client: MockClient, n: int): Future[seq[byte]] {.async.} =
    result = newSeq[byte](n)
    var offset = 0
    while offset < n:
      let data = await client.recv(n - offset)
      if data.len == 0:
        raise newException(CatchableError, "Connection closed prematurely")
      copyMem(addr result[offset], addr data[0], data.len)
      offset += data.len

  proc sendBytes*(client: MockClient, data: seq[byte]) {.async.} =
    if data.len > 0:
      await client.send(cast[string](data))

# Message-building helpers

proc buildBackendMsg*(msgType: char, body: openArray[byte]): seq[byte] =
  ## Wrap `body` with a backend message header (1 type byte + 4 length bytes).
  result = @[byte(msgType)]
  result.addInt32(int32(4 + body.len))
  result.add(@body)

proc buildAuthOk*(): seq[byte] =
  buildBackendMsg('R', @[byte 0, 0, 0, 0])

proc buildAuthSASL*(mechanisms: seq[string] = @["SCRAM-SHA-256"]): seq[byte] =
  ## AuthenticationSASL (R, subtype 10): advertise SASL mechanism names as a
  ## null-terminated list followed by a final null terminator.
  var body: seq[byte]
  body.addInt32(10)
  for m in mechanisms:
    for c in m:
      body.add(byte(c))
    body.add(0'u8)
  body.add(0'u8) # mechanism list terminator
  buildBackendMsg('R', body)

proc buildAuthSASLContinue*(data: string): seq[byte] =
  ## AuthenticationSASLContinue (R, subtype 11): SCRAM server-first data.
  var body: seq[byte]
  body.addInt32(11)
  for c in data:
    body.add(byte(c))
  buildBackendMsg('R', body)

proc buildAuthSASLFinal*(data: string): seq[byte] =
  ## AuthenticationSASLFinal (R, subtype 12): SCRAM server-final data (``v=...``).
  var body: seq[byte]
  body.addInt32(12)
  for c in data:
    body.add(byte(c))
  buildBackendMsg('R', body)

proc parseSaslInitialClientFirst*(body: seq[byte]): string =
  ## Given a SASLInitialResponse body (mechanism cstring + int32 length +
  ## client-first bytes), return the client-first message as a string.
  var i = 0
  while i < body.len and body[i] != 0'u8: # skip mechanism cstring
    inc i
  inc i # skip the null terminator
  i += 4 # skip the int32 length prefix
  result = newString(body.len - i)
  for j in 0 ..< result.len:
    result[j] = char(body[i + j])

proc buildParameterStatus*(name, value: string): seq[byte] =
  var body: seq[byte]
  for c in name:
    body.add(byte(c))
  body.add(0'u8)
  for c in value:
    body.add(byte(c))
  body.add(0'u8)
  buildBackendMsg('S', body)

proc buildBackendKeyData*(pid, secretKey: int32): seq[byte] =
  var body: seq[byte]
  body.addInt32(pid)
  body.addInt32(secretKey)
  buildBackendMsg('K', body)

proc buildReadyForQuery*(status: char = 'I'): seq[byte] =
  buildBackendMsg('Z', @[byte(status)])

proc buildRowDescriptionFields*(
    cols: openArray[tuple[name: string, typeOid: int32, typeSize: int16]]
): seq[byte] =
  ## RowDescription ('T') with the given text-format columns.
  var body: seq[byte]
  body.addInt16(int16(cols.len)) # field count
  for c in cols:
    body.addCString(c.name)
    body.addInt32(0) # table OID
    body.addInt16(0) # column attribute number
    body.addInt32(c.typeOid) # type OID
    body.addInt16(c.typeSize) # type size
    body.addInt32(-1) # type modifier
    body.addInt16(0) # format code (text)
  buildBackendMsg('T', body)

proc buildRowDescription*(colName: string): seq[byte] =
  ## RowDescription with a single text-format `text` column.
  buildRowDescriptionFields(@[(colName, 25'i32, -1'i16)])

proc buildDataRowText*(cols: openArray[string]): seq[byte] =
  ## DataRow ('D') with text-format columns. Does not emit SQL NULL.
  var body: seq[byte]
  body.addInt16(int16(cols.len)) # column count
  for c in cols:
    body.addInt32(int32(c.len))
    for ch in c:
      body.add(byte(ch))
  buildBackendMsg('D', body)

proc buildDataRow*(value: string): seq[byte] =
  ## DataRow with a single text-format column.
  buildDataRowText([value])

proc queryText*(body: seq[byte]): string =
  ## Extract the SQL string from a Query ('Q') message body (a single cstring).
  decodeCString(body, 0)[0]

proc buildCommandComplete*(tag: string): seq[byte] =
  var body: seq[byte]
  for c in tag:
    body.add(byte(c))
  body.add(0'u8)
  buildBackendMsg('C', body)

proc buildErrorResponse*(sqlState, message: string): seq[byte] =
  ## Minimal ErrorResponse with severity 'S', sqlstate 'C', message 'M'.
  var body: seq[byte]
  body.add(byte('S'))
  for c in "ERROR":
    body.add(byte(c))
  body.add(0'u8)
  body.add(byte('C'))
  for c in sqlState:
    body.add(byte(c))
  body.add(0'u8)
  body.add(byte('M'))
  for c in message:
    body.add(byte(c))
  body.add(0'u8)
  body.add(0'u8) # field list terminator
  buildBackendMsg('E', body)

# Replication (CopyBothResponse / CopyData) builders and decoders, shared by the
# replication test suites.

proc buildCopyBothResponse*(): seq[byte] =
  ## CopyBothResponse: format(1=binary) + numCols(0).
  var body: seq[byte]
  body.add(1'u8)
  body.addInt16(0'i16)
  buildBackendMsg('W', body)

proc buildCopyData*(payload: openArray[byte]): seq[byte] =
  buildBackendMsg('d', payload)

proc buildXLogData*(startLsn, walEnd, sendTime: int64, walData: seq[byte]): seq[byte] =
  ## CopyData('w' + startLsn + walEnd + sendTime + walData).
  var payload: seq[byte]
  payload.add(byte('w'))
  payload.addInt64(startLsn)
  payload.addInt64(walEnd)
  payload.addInt64(sendTime)
  payload.add(walData)
  buildCopyData(payload)

proc buildKeepalive*(walEnd, sendTime: int64, replyRequested: bool): seq[byte] =
  ## CopyData('k' + walEnd + sendTime + replyRequested).
  var payload: seq[byte]
  payload.add(byte('k'))
  payload.addInt64(walEnd)
  payload.addInt64(sendTime)
  payload.add(if replyRequested: 1'u8 else: 0'u8)
  buildCopyData(payload)

proc buildCopyDone*(): seq[byte] =
  @[byte('c'), 0'u8, 0'u8, 0'u8, 4'u8]

proc decodeStandbyStatus*(body: seq[byte]): tuple[receive, flush, apply: int64] =
  ## Frontend CopyData body for a Standby Status Update:
  ## 'r' + receive(8) + flush(8) + apply(8) + clock(8) + replyRequested(1).
  doAssert body.len == 1 + 8 + 8 + 8 + 8 + 1, "unexpected standby status size"
  doAssert body[0] == byte('r'), "expected standby status type byte 'r'"
  (decodeInt64(body, 1), decodeInt64(body, 9), decodeInt64(body, 17))

# Frontend readers

proc drainStartupMessage*(client: MockClient) {.async.} =
  ## Consume the initial StartupMessage sent by the client (no type byte,
  ## 4-byte length prefix).
  let lenBuf = await readN(client, 4)
  let msgLen = decodeInt32(lenBuf, 0)
  if msgLen > 4:
    discard await readN(client, msgLen - 4)

proc drainFrontendMessage*(
    client: MockClient
): Future[tuple[msgType: char, body: seq[byte]]] {.async.} =
  ## Read a single post-startup frontend message: 1 type byte, int32 length, body.
  let head = await readN(client, 1)
  result.msgType = char(head[0])
  let lenBuf = await readN(client, 4)
  let msgLen = decodeInt32(lenBuf, 0)
  if msgLen > 4:
    result.body = await readN(client, msgLen - 4)

proc runAutoKeepaliveServer*(
    client: MockClient,
    startLsn, walEnd, keepaliveWalEnd: int64,
    walData: seq[byte],
    endStream: bool = true,
): Future[AutoKeepaliveResult] {.async.} =
  ## Server-side helper for auto-keepalive reply tests. Drains the
  ## START_REPLICATION query, sends CopyBothResponse + XLogData +
  ## PrimaryKeepalive(replyRequested=true), captures the client's Standby Status
  ## Update reply, and optionally ends the stream with CopyDone + ReadyForQuery
  ## (consuming the client's CopyDone reply). Returns the decoded reply fields.
  discard await drainFrontendMessage(client) # START_REPLICATION
  var burst: seq[byte]
  burst.add(buildCopyBothResponse())
  burst.add(buildXLogData(startLsn, walEnd, 0, walData))
  burst.add(buildKeepalive(keepaliveWalEnd, 0, replyRequested = true))
  await sendBytes(client, burst)
  let reply = await drainFrontendMessage(client)
  var observed: AutoKeepaliveResult
  observed.msgType = reply.msgType
  if reply.msgType == 'd':
    let ssu = decodeStandbyStatus(reply.body)
    observed.receive = ssu.receive
    observed.flush = ssu.flush
    observed.apply = ssu.apply
  if endStream:
    var tail: seq[byte]
    tail.add(buildCopyDone())
    tail.add(buildReadyForQuery('I'))
    await sendBytes(client, tail)
    discard await drainFrontendMessage(client) # client's CopyDone
  return observed

# Full handshake shortcut

proc sendFullHandshake*(
    client: MockClient,
    pid: int32 = 1234,
    secretKey: int32 = 5678,
    params: seq[(string, string)] = @[],
) {.async.} =
  ## Send AuthOk + ParameterStatus* + BackendKeyData + ReadyForQuery in one
  ## round-trip. Use after `drainStartupMessage` and before the client's first
  ## real query.
  var resp: seq[byte]
  resp.add(buildAuthOk())
  for (k, v) in params:
    resp.add(buildParameterStatus(k, v))
  resp.add(buildBackendKeyData(pid, secretKey))
  resp.add(buildReadyForQuery('I'))
  await sendBytes(client, resp)

proc acceptAndReady*(
    ms: MockServer,
    pid: int32 = 1234,
    secretKey: int32 = 5678,
    params: seq[(string, string)] = @[],
): Future[MockClient] {.async.} =
  ## End-to-end helper: accept a client and complete startup + handshake,
  ## reporting `params` via ParameterStatus during the handshake. On return
  ## the client is positioned at `csReady` with no outstanding requests.
  let client = await ms.accept()
  await drainStartupMessage(client)
  await sendFullHandshake(client, pid, secretKey, params)
  client
