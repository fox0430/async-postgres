## Unit tests for `pg_client/cursor` via the mock server.
##
## PG-less: validation plus an open/fetch/close round trip where the portal
## completes immediately (CommandComplete or EmptyQueryResponse -> Close + Sync
## -> ReadyForQuery), and a commit failure reported after that Sync.

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

proc drainOpenCursor(client: MockClient): Future[string] {.async.} =
  ## Drain openCursor's batch through Flush; return the portal name from Bind.
  while true:
    let (t, body) = await drainFrontendMessage(client)
    if t == 'B':
      result = decodeCString(body, 0)[0]
    elif t == 'H':
      break

proc expectClosePortal(client: MockClient, portal: string) {.async.} =
  ## Expect Close(portal) + Sync, skipping queued statement Closes staged ahead.
  while true:
    let (closeType, closeBody) = await drainFrontendMessage(client)
    doAssert closeType == 'C'
    if char(closeBody[0]) == 'S':
      continue
    doAssert char(closeBody[0]) == 'P'
    doAssert decodeCString(closeBody, 1)[0] == portal
    break
  let (syncType, _) = await drainFrontendMessage(client)
  doAssert syncType == 'S'

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
        let portal = await drainOpenCursor(client)
        doAssert portal.len > 0
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildCommandComplete("SELECT 0")
            # No ReadyForQuery: client sends Close + Sync after CommandComplete.
        )
        # The portal is closed here, since close() skips an exhausted cursor.
        await expectClosePortal(client, portal)
        await client.sendBytes(buildBackendMsg('3', @[]) & buildReadyForQuery('I'))
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
        let portal = await drainOpenCursor(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildCommandComplete("SELECT 0")
        )
        await expectClosePortal(client, portal)
        await client.sendBytes(buildBackendMsg('3', @[]) & buildReadyForQuery('I'))
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

  test "empty SQL completes with EmptyQueryResponse and closes the portal":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        let portal = await drainOpenCursor(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildBackendMsg('n', @[]) & buildEmptyQueryResponse()
        )
        await expectClosePortal(client, portal)
        await client.sendBytes(buildBackendMsg('3', @[]) & buildReadyForQuery('I'))
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let cur = await conn.openCursor("", chunkSize = 10'i32)
      doAssert cur.exhausted
      doAssert cur.fields.len == 0
      doAssert (await cur.fetchNext()).len == 0
      doAssert conn.state == csReady
      await cur.close()
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

suite "cursor: commit failure at the closing Sync":
  # Outside a transaction block the closing Sync commits the implicit
  # transaction; a deferred constraint can fail it after CloseComplete.

  test "openCursor raises it":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        let portal = await drainOpenCursor(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildRowDescription("id") & buildDataRow("1") &
            buildCommandComplete("INSERT 0 1")
        )
        await expectClosePortal(client, portal)
        await client.sendBytes(
          buildBackendMsg('3', @[]) & buildErrorResponse("23503", "deferred fk") &
            buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var state = ""
      try:
        discard await conn.openCursor("INSERT ... RETURNING id", chunkSize = 10'i32)
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "23503"
      doAssert conn.state == csReady
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "fetchNext raises it":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        let portal = await drainOpenCursor(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildRowDescription("id") & buildDataRow("1") & buildBackendMsg('s', @[])
        )
        # fetchNext: Execute/Flush.
        await drainUntilFlush(client)
        await client.sendBytes(buildDataRow("2") & buildCommandComplete("INSERT 0 2"))
        await expectClosePortal(client, portal)
        await client.sendBytes(
          buildBackendMsg('3', @[]) & buildErrorResponse("23503", "deferred fk") &
            buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let cur = await conn.openCursor("INSERT ... RETURNING id", chunkSize = 1'i32)
      let buffered = await cur.fetchNext()
      doAssert buffered.len == 1
      var state = ""
      try:
        discard await cur.fetchNext()
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "23503"
      doAssert cur.exhausted
      doAssert conn.state == csReady
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "close raises it and leaves the cursor exhausted":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        let portal = await drainOpenCursor(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildRowDescription("id") & buildDataRow("1") & buildBackendMsg('s', @[])
        )
        await expectClosePortal(client, portal)
        await client.sendBytes(
          buildBackendMsg('3', @[]) & buildErrorResponse("23503", "deferred fk") &
            buildReadyForQuery('I')
        )
        # The later fetchNext and close send nothing.
        let (t, _) = await drainFrontendMessage(client)
        doAssert t == 'X'
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let cur = await conn.openCursor("INSERT ... RETURNING id", chunkSize = 1'i32)
      var state = ""
      try:
        await cur.close()
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "23503"
      doAssert cur.exhausted
      doAssert conn.state == csReady
      doAssert (await cur.fetchNext()).len == 0
      await cur.close()
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
