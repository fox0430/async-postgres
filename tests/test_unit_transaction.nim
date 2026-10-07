## Unit tests for `pg_client/transaction` via the mock server.
##
## PG-less: `withTransaction` commit/rollback and the body-escape
## compile-time guards.

import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol]

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc readSimpleQuery(client: MockClient): Future[string] {.async.} =
  let m = await drainFrontendMessage(client)
  doAssert m.msgType == 'Q'
  queryText(m.body)

suite "transaction: body-escape guards (compile time)":
  test "withTransaction rejects return at compile time":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let ms = startMockServer()
          let conn = await connect(mockConfig(ms.port))
          conn.withTransaction:
            discard

    )
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let ms = startMockServer()
          let conn = await connect(mockConfig(ms.port))
          conn.withTransaction:
            return

    )

  test "withSavepoint rejects break at compile time":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let ms = startMockServer()
          let conn = await connect(mockConfig(ms.port))
          conn.withTransaction:
            conn.withSavepoint("sp"):
              discard

    )
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let ms = startMockServer()
          let conn = await connect(mockConfig(ms.port))
          conn.withTransaction:
            conn.withSavepoint("sp"):
              break

    )

suite "transaction: mock round trip":
  test "withTransaction commits":
    proc t() {.async.} =
      let ms = startMockServer()
      var queries: seq[string]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        for _ in 0 ..< 3: # BEGIN, SELECT 1, COMMIT
          queries.add(await readSimpleQuery(client))
          await client.sendBytes(
            buildCommandComplete(queries[^1]) & buildReadyForQuery('I')
          )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      conn.withTransaction:
        discard await conn.simpleExec("SELECT 1")
      doAssert queries == @["BEGIN", "SELECT 1", "COMMIT"]
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "withTransaction rolls back on body error":
    proc t() {.async.} =
      let ms = startMockServer()
      var queries: seq[string]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        for _ in 0 ..< 2: # BEGIN, ROLLBACK (body raises before COMMIT)
          queries.add(await readSimpleQuery(client))
          let tx = if queries.len == 1: 'T' else: 'I'
          await client.sendBytes(
            buildCommandComplete(queries[^1]) & buildReadyForQuery(tx)
          )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var caught = false
      try:
        conn.withTransaction:
          raise newException(CatchableError, "body boom")
      except CatchableError:
        caught = true
      doAssert caught
      doAssert queries == @["BEGIN", "ROLLBACK"]
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "withSavepoint releases on success":
    proc t() {.async.} =
      let ms = startMockServer()
      var queries: seq[string]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        for _ in 0 ..< 5: # BEGIN, SAVEPOINT, SELECT, RELEASE, COMMIT
          queries.add(await readSimpleQuery(client))
          let tx = if queries[^1] == "COMMIT": 'I' else: 'T'
          await client.sendBytes(
            buildCommandComplete(queries[^1]) & buildReadyForQuery(tx)
          )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      conn.withTransaction:
        conn.withSavepoint("sp1"):
          discard await conn.simpleExec("SELECT 1")
      doAssert queries[1].startsWith("SAVEPOINT")
      doAssert queries[3].startsWith("RELEASE")
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
