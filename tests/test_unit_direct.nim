## Unit tests for `pg_client/direct` via the mock server.
##
## PG-less: `queryDirect` / `execDirect` macros against scripted replies.

import std/unittest

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

suite "direct: mock round trip":
  test "queryDirect returns a decoded row":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildRowDescriptionFields(@[("v", 23'i32, 4'i16)]) & buildDataRowText(["7"]) &
            buildCommandComplete("SELECT 1") & buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let got = await conn.queryDirect("SELECT $1", 7'i32)
      doAssert got.rowCount == 1
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "execDirect returns the command tag":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildCommandComplete("INSERT 0 1") & buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let tag = await conn.execDirect("INSERT INTO t VALUES ($1)", 1'i32)
      doAssert tag.commandTag == "INSERT 0 1"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
