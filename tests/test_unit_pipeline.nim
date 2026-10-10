## Unit tests for `pg_client/pipeline` via the mock server.
##
## PG-less: builder behavior (`newPipeline`/`reset`) plus `execute` and
## `executeIsolated` round trips.

import std/unittest

import ../async_postgres/[async_backend, pg_client, pg_connection]

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc drainOnePipeline(client: MockClient, ops: int) {.async.} =
  # A 2-op pipeline sends Parse/Bind/Execute per op plus one Sync.
  # Drain through the single trailing Sync.
  while true:
    let (t, _) = await drainFrontendMessage(client)
    if t == 'S':
      break

suite "pipeline: builder":
  test "newPipeline starts empty and reset clears":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let p = newPipeline(conn)
      p.addExec("SELECT 1")
      p.addQuery("SELECT 2")
      p.reset()
      # After reset the pipeline is empty: execute returns no results but
      # still performs a (possibly empty) round trip.
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

suite "pipeline: mock round trip":
  test "execute runs exec + query ops":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOnePipeline(client, 2)
        var reply: seq[byte]
        # op1 (exec): ParseComplete + BindComplete + CommandComplete.
        reply.add(buildBackendMsg('1', @[]))
        reply.add(buildBackendMsg('2', @[]))
        reply.add(buildCommandComplete("SELECT 1"))
        # op2 (query): ParseComplete + BindComplete + RowDescription + DataRow + CommandComplete.
        reply.add(buildBackendMsg('1', @[]))
        reply.add(buildBackendMsg('2', @[]))
        reply.add(buildRowDescriptionFields(@[("v", 25'i32, -1'i16)]))
        reply.add(buildDataRowText(["hi"]))
        reply.add(buildCommandComplete("SELECT 1"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let p = newPipeline(conn)
      p.addExec("SELECT 1")
      p.addQuery("SELECT 2")
      let res = await p.execute()
      doAssert res.len == 2
      doAssert res[0].kind == prkExec
      doAssert res[1].kind == prkQuery
      doAssert res[1].queryResult.rowCount == 1
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "executeIsolated isolates a failing op":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        # executeIsolated uses per-op Sync.
        for _ in 0 ..< 2:
          await drainOnePipeline(client, 1)
        var reply: seq[byte]
        reply.add(buildBackendMsg('1', @[]))
        reply.add(buildBackendMsg('2', @[]))
        reply.add(buildErrorResponse("42P01", "no such table"))
        reply.add(buildReadyForQuery('E'))
        reply.add(buildBackendMsg('1', @[]))
        reply.add(buildBackendMsg('2', @[]))
        reply.add(buildRowDescriptionFields(@[("v", 25'i32, -1'i16)]))
        reply.add(buildDataRowText(["ok"]))
        reply.add(buildCommandComplete("SELECT 1"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let p = newPipeline(conn)
      p.addExec("SELECT * FROM missing")
      p.addQuery("SELECT 1")
      let iso = await p.executeIsolated()
      doAssert iso.results.len == 2
      doAssert iso.errors.len == 2
      doAssert iso.errors[0] != nil
      doAssert iso.errors[1] == nil
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
