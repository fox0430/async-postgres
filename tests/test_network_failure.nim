## Network-failure tests using an in-process mock PostgreSQL server.
##
## These exercise code paths that a real server will not reproduce on demand:
## mid-handshake disconnects, mid-query disconnects, malformed backend
## messages, and unknown message type bytes. Verifies that the client raises
## the expected exception type and leaves the connection in a state that
## prevents accidental reuse (`csClosed`).

import std/[unittest]

import ../async_postgres/[async_backend, pg_protocol]
import ../async_postgres/pg_connection {.all.}

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

# Handshake failures

suite "Network failure: handshake":
  test "server closes immediately after accept":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except CatchableError:
        raised = true
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "server reads startup then disconnects":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except CatchableError:
        raised = true
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "handshake succeeds but hstore discovery is dropped mid-response":
    # Server completes auth + ready, but closes the socket after the client's
    # hstore discovery query, before sending any response.
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendFullHandshake(st)
          discard await drainFrontendMessage(st) # the hstore query
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port))
        await conn.close()
      except CatchableError:
        raised = true
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

# Malformed / truncated backend messages

suite "Network failure: malformed server messages":
  test "unknown backend message type 'X' raises ProtocolError":
    var raised = false
    var finalState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st) # SELECT 1
          # Reply with an unknown message type wrapped in a valid frame.
          await sendBytes(st, buildBackendMsg('X', @[byte 0]))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except ProtocolError:
        raised = true
      except CatchableError:
        raised = true
      finalState = conn.state
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check finalState == csClosed

  test "truncated RowDescription before full body arrives":
    # Server sends a valid header claiming a larger body than it will deliver,
    # then closes. The read loop should return an incomplete-parse state until
    # the close signals EOF, which raises an error.
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # Claim a 100-byte message but only send 10 bytes then close.
          var truncated: seq[byte]
          truncated.add(byte('T'))
          truncated.addInt32(100'i32)
          for _ in 0 ..< 5:
            truncated.add(0'u8)
          await sendBytes(st, truncated)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "claimed msgLen below minimum (3) raises ProtocolError":
    var raised = false
    var gotProtocolError = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # 5-byte frame with msgLen=3 — parser must reject.
          var buf = newSeq[byte](5)
          buf[0] = byte('C')
          buf[1] = 0
          buf[2] = 0
          buf[3] = 0
          buf[4] = 3
          await sendBytes(st, buf)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except ProtocolError:
        raised = true
        gotProtocolError = true
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check gotProtocolError

  test "malformed CommandComplete without null terminator raises ProtocolError":
    var raised = false
    var gotProtocolError = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # CommandComplete body with no NUL terminator.
          var buf: seq[byte]
          buf.add(byte('C'))
          buf.addInt32(int32(4 + 3))
          buf.add(byte('S'))
          buf.add(byte('E'))
          buf.add(byte('L')) # no trailing 0
          await sendBytes(st, buf)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except ProtocolError:
        raised = true
        gotProtocolError = true
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check gotProtocolError

# Mid-query disconnects

suite "Network failure: mid-query disconnects":
  test "server closes after sending partial DataRow":
    # Note: csClosed here is reached via the EOF / socket-close path, not via
    # the `ProtocolError -> csClosed` transition in `nextMessage`. If future
    # changes decouple socket close from state transition, this assertion may
    # start failing silently (the state would no longer be csClosed).
    var raised = false
    var finalState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # Send a valid RowDescription with one int4 column (oid=23).
          var rd: seq[byte]
          rd.addInt16(1) # 1 field
          for c in "n":
            rd.add(byte(c))
          rd.add(0'u8) # name terminator
          rd.addInt32(0'i32) # tableOid
          rd.addInt16(0'i16) # columnAttrNum
          rd.addInt32(23'i32) # typeOid (int4)
          rd.addInt16(4'i16) # typeSize
          rd.addInt32(-1'i32) # typeMod
          rd.addInt16(0'i16) # formatCode
          await sendBytes(st, buildBackendMsg('T', rd))
          # Start a DataRow but truncate it mid-column.
          var dr: seq[byte]
          dr.addInt16(1) # 1 column
          dr.addInt32(100'i32) # claim 100 bytes
          dr.add(byte(0)) # partial
          # Claim the whole msgLen is correct for our buffer so parser reads it.
          let body = dr
          var frame: seq[byte]
          frame.add(byte('D'))
          frame.addInt32(int32(4 + body.len))
          frame.add(body)
          await sendBytes(st, frame)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except CatchableError:
        raised = true
      finalState = conn.state
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check finalState == csClosed

  test "server closes after sending ErrorResponse without final field terminator":
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          # ErrorResponse body with field 'M' value missing its NUL terminator.
          var body: seq[byte]
          body.add(byte('M'))
          body.add(byte('o')) # start value, no NUL
          await sendBytes(st, buildBackendMsg('E', body))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except CatchableError:
        raised = true
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
