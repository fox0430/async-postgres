## Unit tests for `pg_client/copy` via the mock server.
##
## PG-less: COPY IN (text payload) and COPY OUT (data collection) over the
## simple-query protocol, plus a wrong-direction error.

import std/unittest

import ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol]
import ../async_postgres/pg_connection/buffer_io

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc copyInResponse(): seq[byte] =
  buildBackendMsg('G', @[0'u8, 0, 0])

proc copyOutResponse(): seq[byte] =
  buildBackendMsg('H', @[0'u8, 0, 0])

suite "copy: mock round trip":
  test "copyIn sends rows and returns the tag":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client) # COPY ... FROM STDIN Query
        await client.sendBytes(copyInResponse())
        # Drain CopyData/CopyDone frames through the frontend CopyDone.
        while true:
          let m = await drainFrontendMessage(client)
          if m.msgType == 'c':
            break
        await client.sendBytes(buildCommandComplete("COPY 2") & buildReadyForQuery('I'))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.copyIn("COPY t FROM STDIN", "a\nb\n")
      doAssert res.commandTag == "COPY 2"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyOut collects CopyData":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client) # COPY ... TO STDOUT Query
        var reply = copyOutResponse()
        reply.add(buildCopyData(@[byte('h'), byte('i')]))
        reply.add(buildCopyDone())
        reply.add(buildCommandComplete("COPY 1"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.copyOut("COPY t TO STDOUT")
      doAssert res.data.len == 1
      doAssert res.commandTag == "COPY 1"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyOutStream streams chunks through the callback":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        var reply = copyOutResponse()
        reply.add(buildCopyData(@[byte('x')]))
        reply.add(buildCopyData(@[byte('y')]))
        reply.add(buildCopyDone())
        reply.add(buildCommandComplete("COPY 2"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var chunks: seq[seq[byte]]
      let cb = makeCopyOutCallback:
        chunks.add(data)
      let info = await conn.copyOutStream("COPY t TO STDOUT", cb)
      doAssert chunks.len == 2
      doAssert info.commandTag == "COPY 2"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyOut of a non-COPY statement raises PgQueryError":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        await client.sendBytes(
          buildRowDescription("v") & buildDataRow("1") & buildCommandComplete(
            "SELECT 1"
          ) & buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      try:
        discard await conn.copyOut("SELECT 1")
      except PgQueryError:
        raised = true
      doAssert raised
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await ms.closeServer()

    waitFor t()
