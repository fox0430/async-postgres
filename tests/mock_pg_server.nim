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

proc sendEmptyHstoreDiscovery*(client: MockClient) {.async.} =
  ## Respond to the post-handshake hstore OID discovery query with an empty
  ## result set (CommandComplete "SELECT 0" + ReadyForQuery). Leaves the
  ## client in `csReady` state.
  var resp: seq[byte]
  resp.add(buildCommandComplete("SELECT 0"))
  resp.add(buildReadyForQuery('I'))
  await sendBytes(client, resp)

proc acceptAndReady*(
    ms: MockServer, pid: int32 = 1234, secretKey: int32 = 5678
): Future[MockClient] {.async.} =
  ## End-to-end helper: accept a client, complete startup + handshake, and
  ## answer the hstore OID discovery query with an empty result. On return
  ## the client is positioned at `csReady` with no outstanding requests.
  let client = await ms.accept()
  await drainStartupMessage(client)
  await sendFullHandshake(client, pid, secretKey)
  # The connect() path issues the hstore discovery query next.
  discard await drainFrontendMessage(client)
  await sendEmptyHstoreDiscovery(client)
  client
