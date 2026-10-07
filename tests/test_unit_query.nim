## Unit tests for `pg_client/query` via the mock server.
##
## PG-less: extended-protocol `query` family against scripted replies.

import std/[unittest, options]

import
  ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol, pg_types]

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc drainOneExtended(client: MockClient) {.async.} =
  while true:
    let (t, _) = await drainFrontendMessage(client)
    if t == 'S':
      break

proc scalarReply(value, tag: string): seq[byte] =
  buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
    buildRowDescriptionFields(@[("v", 25'i32, -1'i16)]) & buildDataRowText([value]) &
    buildCommandComplete(tag) & buildReadyForQuery('I')

suite "query: mock round trip":
  test "query returns rows and the command tag":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(scalarReply("hello", "SELECT 1"))
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let qr = await conn.query("SELECT $1", @[toPgParam("x")])
      doAssert qr.rowCount == 1
      doAssert qr.commandTag == "SELECT 1"
      doAssert qr.rows.len == 1
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "queryRow/queryValue/queryExists/queryColumn shapes":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        for v in ["a", "b"]:
          await drainOneExtended(client)
          await client.sendBytes(scalarReply(v, "SELECT 1"))
        # queryExists: one row.
        await drainOneExtended(client)
        await client.sendBytes(scalarReply("1", "SELECT 1"))
        # queryColumn: two rows need two DataRows in one reply.
        await drainOneExtended(client)
        var reply = buildBackendMsg('1', @[]) & buildBackendMsg('2', @[])
        reply.add(buildRowDescriptionFields(@[("v", 25'i32, -1'i16)]))
        reply.add(buildDataRowText(["x"]))
        reply.add(buildDataRowText(["y"]))
        reply.add(buildCommandComplete("SELECT 2"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let row = await conn.queryRow("SELECT $1", @[toPgParam(1'i32)])
      doAssert not row.isNull(0)
      let val = await conn.queryValue("SELECT 2")
      doAssert val == "b"
      doAssert await conn.queryExists("SELECT 3")
      let col = await conn.queryColumn("SELECT 4")
      doAssert col == @["x", "y"]
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "queryEach streams rows via callback":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        var reply = buildBackendMsg('1', @[]) & buildBackendMsg('2', @[])
        reply.add(buildRowDescriptionFields(@[("v", 25'i32, -1'i16)]))
        reply.add(buildDataRowText(["r1"]))
        reply.add(buildDataRowText(["r2"]))
        reply.add(buildCommandComplete("SELECT 2"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var seen: seq[string]
      let n = await conn.queryEach(
        "SELECT 1",
        @[],
        proc(r: Row) {.gcsafe, raises: [CatchableError].} =
          seen.add(r.getStr(0)),
      )
      doAssert n == 2
      doAssert seen == @["r1", "r2"]
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "server error surfaces as PgQueryError with SQLSTATE":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildErrorResponse("23505", "duplicate key") & buildReadyForQuery('E')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var state = ""
      try:
        discard await conn.query("INSERT INTO t VALUES (1)")
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "23505"
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await ms.closeServer()

    waitFor t()
