## Unit tests for `pg_advisory_lock` via the mock server.
##
## PG-less: session lock acquire/release accounting, try-lock booleans,
## xact-scope guard, and `withAdvisoryLock` release on body error.

import std/[unittest, importutils]

import ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol]
import ../async_postgres/pg_advisory_lock

import mock_pg_server

privateAccess(PgConnection)

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc drainOneExtended(client: MockClient) {.async.} =
  while true:
    let (t, _) = await drainFrontendMessage(client)
    if t == 'S':
      break

proc boolReply(v: bool): seq[byte] =
  let text = if v: "t" else: "f"
  buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) &
    buildRowDescriptionFields(@[("ok", 16'i32, 1'i16)]) & buildDataRowText([text]) &
    buildCommandComplete("SELECT 1") & buildReadyForQuery('I')

proc voidReply(tag: string): seq[byte] =
  buildBackendMsg('1', @[]) & buildBackendMsg('2', @[]) & buildCommandComplete(tag) &
    buildReadyForQuery('I')

suite "advisory_lock: mock round trip":
  test "advisoryLock/unlock track heldSessionLocks":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client) # lock
        await client.sendBytes(boolReply(true))
        await drainOneExtended(client) # unlock
        await client.sendBytes(boolReply(true))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      doAssert conn.heldSessionLocks == 0
      await conn.advisoryLock(42'i64)
      doAssert conn.heldSessionLocks == 1
      doAssert await conn.advisoryUnlock(42'i64)
      doAssert conn.heldSessionLocks == 0
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "advisoryTryLock false does not increment":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(boolReply(false))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      doAssert not await conn.advisoryTryLock(43'i64)
      doAssert conn.heldSessionLocks == 0
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "two-key lock/unlock":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client)
        await client.sendBytes(boolReply(true))
        await drainOneExtended(client)
        await client.sendBytes(boolReply(true))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      await conn.advisoryLock(1'i32, 2'i32)
      doAssert conn.heldSessionLocks == 1
      doAssert await conn.advisoryUnlock(1'i32, 2'i32)
      doAssert conn.heldSessionLocks == 0
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "xact lock outside a transaction raises PgStateError":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      try:
        await conn.advisoryLockXact(99'i64)
      except PgStateError:
        raised = true
      doAssert raised
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "withAdvisoryLock releases even on body error":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client) # acquire
        await client.sendBytes(boolReply(true))
        await drainOneExtended(client) # release
        await client.sendBytes(boolReply(true))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var caught = false
      try:
        conn.withAdvisoryLock(77'i64):
          doAssert conn.heldSessionLocks == 1
          raise newException(CatchableError, "body boom")
      except CatchableError:
        caught = true
      doAssert caught
      doAssert conn.heldSessionLocks == 0
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "advisoryUnlockAll zeroes the counter":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client) # lock
        await client.sendBytes(boolReply(true))
        await drainOneExtended(client) # unlock_all via exec
        await client.sendBytes(voidReply("SELECT 1"))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      await conn.advisoryLock(88'i64)
      doAssert conn.heldSessionLocks == 1
      await conn.advisoryUnlockAll()
      doAssert conn.heldSessionLocks == 0
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
