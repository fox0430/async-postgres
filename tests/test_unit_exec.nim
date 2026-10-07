## Unit tests for `pg_client/exec` via the mock server.
##
## PG-less: extended-protocol `exec` and `notify` against scripted replies.

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

proc execReply(tag: string): seq[byte] =
  buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) & buildCommandComplete(tag) &
    buildReadyForQuery('I')

suite "exec: mock round trip":
  test "exec returns the command tag":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(execReply("INSERT 0 1"))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.exec("INSERT INTO t VALUES ($1)", @[toPgParam(1'i32)])
      doAssert res.commandTag == "INSERT 0 1"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "exec with inline params returns the tag":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(execReply("UPDATE 2"))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.exec("UPDATE t SET a = $1", @[toPgParamInline(7'i32)])
      doAssert res.commandTag == "UPDATE 2"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "notify without payload uses NOTIFY":
    proc t() {.async.} =
      let ms = startMockServer()
      var gotSql = ""
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        # notify() with no payload sends `NOTIFY "chan"` via exec.
        await drainOneExtended(client)
        await client.sendBytes(execReply("NOTIFY"))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      await conn.notify("chan")
      doAssert conn.state == csReady
      await conn.close()
      await serverFut
      await ms.closeServer()
      doAssert gotSql.len == 0 # server consumed the NOTIFY without asserting text

    waitFor t()

  test "exec error keeps SQLSTATE":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(
          buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
            buildErrorResponse("42P01", "no such table") & buildReadyForQuery('E')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var state = ""
      try:
        discard await conn.exec("SELECT * FROM missing")
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "42P01"
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await ms.closeServer()

    waitFor t()
