## Wire-level tests for the pipelined transaction helpers against the mock
## server: what `execInTransaction` sends, and that it drops returned rows
## without decoding them.

import std/unittest

import ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol]

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc readPipeline(client: MockClient): Future[string] {.async.} =
  ## Read frontend messages through Sync; returns their type bytes in order.
  while true:
    let m = await drainFrontendMessage(client)
    result.add(m.msgType)
    if m.msgType == 'S':
      break

proc undecodableRow(): seq[byte] =
  ## A DataRow the decoder rejects (cell length -2), so it only passes framed.
  var body: seq[byte]
  body.addInt16(1)
  body.addInt32(-2)
  buildBackendMsg('D', body)

proc stepReply(tag: string): seq[byte] =
  buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) & buildCommandComplete(tag)

suite "execInTransaction on the wire":
  test "rows are framed, not decoded, and only the statement's tag is kept":
    proc t() {.async.} =
      let ms = startMockServer()
      var sent: string
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        sent = await readPipeline(client)
        var reply = stepReply("BEGIN")
        reply.add(buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]))
        reply.add(undecodableRow() & undecodableRow())
        reply.add(buildCommandComplete("INSERT 0 2"))
        reply.add(stepReply("COMMIT") & buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let tag =
        await conn.execInTransaction("INSERT INTO t VALUES (1), (2) RETURNING id")
      doAssert tag.commandTag == "INSERT 0 2", tag.commandTag
      doAssert sent == "PBEPBEPBES", "no Describe for exec: " & sent
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "a failed statement is rolled back and its error surfaces":
    proc t() {.async.} =
      let ms = startMockServer()
      var rollback: tuple[msgType: char, body: seq[byte]]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await readPipeline(client)
        var reply = stepReply("BEGIN")
        reply.add(buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]))
        reply.add(buildErrorResponse("23505", "duplicate key"))
        reply.add(buildReadyForQuery('E'))
        await client.sendBytes(reply)
        rollback = await drainFrontendMessage(client)
        await client.sendBytes(
          buildCommandComplete("ROLLBACK") & buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var sqlState = ""
      try:
        discard await conn.execInTransaction("INSERT INTO t VALUES (1)")
      except PgQueryError as e:
        sqlState = e.sqlState
      doAssert sqlState == "23505", sqlState
      doAssert rollback.msgType == 'Q'
      doAssert queryText(rollback.body) == "ROLLBACK"
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
