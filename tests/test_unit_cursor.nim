## Unit tests for `pg_client/cursor` via the mock server.
##
## PG-less: validation plus an open/fetch/close round trip where the portal
## completes immediately (CommandComplete -> Sync -> ReadyForQuery).

import std/unittest

import ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol]

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc drainUntilFlush(client: MockClient) {.async.} =
  while true:
    let (t, _) = await drainFrontendMessage(client)
    if t == 'H':
      break

proc drainUntilSync(client: MockClient) {.async.} =
  while true:
    let (t, _) = await drainFrontendMessage(client)
    if t == 'S':
      break

suite "cursor: validation (no rows)":
  test "non-positive chunkSize is rejected":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      try:
        discard await conn.openCursor("SELECT 1", chunkSize = 0'i32)
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

suite "cursor: mock round trip":
  test "open/fetch/close on an immediately-exhausted portal":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        # openCursor: Parse/Bind/Describe(portal)/Execute/Flush.
        await drainUntilFlush(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildCommandComplete("SELECT 0")
            # No ReadyForQuery: client sends Sync after CommandComplete.
        )
        await drainUntilSync(client)
        await client.sendBytes(buildReadyForQuery('I'))
        # close() on an exhausted cursor sends nothing; just Terminate.
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let cur = await conn.openCursor("SELECT 1", chunkSize = 10'i32)
      doAssert cur.conn == conn
      doAssert cur.exhausted
      let rows = await cur.fetchNext()
      doAssert rows.len == 0
      await cur.close()
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "withCursor runs the body and closes":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainUntilFlush(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildCommandComplete("SELECT 0")
        )
        await drainUntilSync(client)
        await client.sendBytes(buildReadyForQuery('I'))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var ran = false
      conn.withCursor("SELECT 1", 10'i32, cur):
        ran = true
        doAssert cur.exhausted
      doAssert ran
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
