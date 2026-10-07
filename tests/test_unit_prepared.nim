## Unit tests for `pg_client/prepared` via the mock server.
##
## PG-less: `prepare` / `execute` / `close` plus name-validation errors.

import std/unittest

import ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol]

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc drainUntilSync(client: MockClient) {.async.} =
  while true:
    let (t, _) = await drainFrontendMessage(client)
    if t == 'S':
      break

suite "prepared: validation (no server)":
  test "empty name is rejected":
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
        discard await conn.prepare("", "SELECT 1")
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "cache-reserved _sc_ prefix is rejected":
    proc t() {.async.} =
      let ms = startMockServer()
      let serverFut = (
        proc() {.async.} =
          let client = await acceptAndReady(ms)
          discard await drainFrontendMessage(client) # Terminate
          await closeClient(client)
      )()
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      try:
        discard await conn.prepare("_sc_1", "SELECT 1")
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

suite "prepared: mock round trip":
  test "prepare/execute/close succeed":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        # prepare: Parse + Describe(statement) + Sync.
        await drainUntilSync(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & # ParseComplete
          buildRowDescriptionFields(@[("v", 25'i32, -1'i16)]) & buildReadyForQuery('I')
        )
        # execute: Bind + Execute + Sync -> DataRow path is skipped for
        # prepared execute; reply CommandComplete + ReadyForQuery is enough
        # when the statement reported NoData... here RowDescription was
        # reported, so include a DataRow-free CommandComplete.
        await drainUntilSync(client)
        await client.sendBytes(
          buildBackendMsg('2', @[]) & # BindComplete
          buildCommandComplete("SELECT 1") & buildReadyForQuery('I')
        )
        # close: Close + Sync.
        await drainUntilSync(client)
        await client.sendBytes(buildBackendMsg('3', @[]) & buildReadyForQuery('I'))
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let stmt = await conn.prepare("my_stmt", "SELECT 1")
      doAssert stmt.name == "my_stmt"
      doAssert stmt.sql == "SELECT 1"
      doAssert stmt.conn == conn
      let qr = await stmt.execute()
      doAssert qr.commandTag == "SELECT 1"
      await stmt.close()
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "prepare error surfaces SQLSTATE":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainUntilSync(client)
        await client.sendBytes(
          buildErrorResponse("42601", "syntax error") & buildReadyForQuery('E')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var state = ""
      try:
        discard await conn.prepare("bad_stmt", "BAD SQL")
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "42601"
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await ms.closeServer()

    waitFor t()
