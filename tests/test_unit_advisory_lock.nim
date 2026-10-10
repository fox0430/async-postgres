## Unit tests for `pg_advisory_lock` via the mock server.
##
## PG-less: session lock acquire/release accounting, try-lock booleans,
## xact-scope guard, `withAdvisoryLock` release on body error, and the
## `onAdvisoryUnlockFailed` report of a failed release.

import std/[unittest, importutils, strutils]

import ../async_postgres/[async_backend, pg_connection, pg_protocol]
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

type UnlockFailLog = ref object
  calls: seq[TraceAdvisoryUnlockFailedData]

proc tracedConfig(port: int, log: UnlockFailLog): ConnConfig =
  result = mockConfig(port)
  result.tracer = PgTracer(
    onAdvisoryUnlockFailed: proc(
        data: TraceAdvisoryUnlockFailedData
    ) {.gcsafe, raises: [].} =
      log.calls.add(data)
  )

suite "advisory_lock: unlock failure hook":
  test "withAdvisoryLock reports an unlock that returns false with a nil err":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client) # acquire
        await client.sendBytes(boolReply(true))
        await drainOneExtended(client) # release: lock was not held
        await client.sendBytes(boolReply(false))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let log = UnlockFailLog()
      let conn = await connect(tracedConfig(ms.port, log)).wait(seconds(5))
      var ran = false
      conn.withAdvisoryLock(77'i64):
        ran = true
      doAssert ran
      doAssert log.calls.len == 1, $log.calls.len
      let data = log.calls[0]
      doAssert data.err == nil
      doAssert data.conn == conn
      doAssert data.key == 77'i64
      doAssert not data.shared
      doAssert not data.twoKey
      await conn.close()
      await serverFut.wait(seconds(5))
      await ms.closeServer()

    waitFor t()

  test "withAdvisoryLock reports a raised unlock failure without masking the body's error":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client) # acquire
        await client.sendBytes(boolReply(true))
        await drainOneExtended(client) # release fails
        await client.sendBytes(
          buildErrorResponse("XX000", "unlock boom") & buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let log = UnlockFailLog()
      let conn = await connect(tracedConfig(ms.port, log)).wait(seconds(5))
      var bodyMsg = ""
      try:
        conn.withAdvisoryLock(78'i64):
          raise newException(ValueError, "body boom")
      except ValueError as e:
        bodyMsg = e.msg
      doAssert "body boom" in bodyMsg, bodyMsg
      doAssert log.calls.len == 1, $log.calls.len
      let data = log.calls[0]
      doAssert data.err != nil
      doAssert data.err of PgQueryError
      doAssert "unlock boom" in data.err.msg, data.err.msg
      doAssert data.key == 78'i64
      await conn.close()
      await serverFut.wait(seconds(5))
      await ms.closeServer()

    waitFor t()

  test "withAdvisoryLockShared reports the shared two-key lock it failed to release":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await drainOneExtended(client) # acquire
        await client.sendBytes(boolReply(true))
        await drainOneExtended(client) # release: lock was not held
        await client.sendBytes(boolReply(false))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let log = UnlockFailLog()
      let conn = await connect(tracedConfig(ms.port, log)).wait(seconds(5))
      conn.withAdvisoryLockShared(3'i32, 4'i32):
        discard
      doAssert log.calls.len == 1, $log.calls.len
      let data = log.calls[0]
      doAssert data.err == nil
      doAssert data.shared
      doAssert data.twoKey
      doAssert data.key1 == 3'i32
      doAssert data.key2 == 4'i32
      await conn.close()
      await serverFut.wait(seconds(5))
      await ms.closeServer()

    waitFor t()
