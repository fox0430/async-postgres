## Unit tests for `pg_client/transaction` via the mock server.
##
## PG-less: `withTransaction` commit/rollback, the body-escape compile-time
## guards, and a body failing on its deadline's tick.

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
          if true:
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

# `Moment.now()` plus the longest `Duration` overflowed with a Defect before
# the body ran. Capped instead, each statement left a timer queued for good.
suite "transaction: a full-range deadline":
  proc serveOk(ms: MockServer, n: int, queries: ref seq[string]) {.async.} =
    ## Answer `n` simple queries, in a transaction until COMMIT.
    let client = await acceptAndReady(ms)
    for _ in 0 ..< n:
      queries[].add(await readSimpleQuery(client))
      let tx = if queries[^1] == "COMMIT": 'I' else: 'T'
      await client.sendBytes(buildCommandComplete(queries[^1]) & buildReadyForQuery(tx))
    discard await drainFrontendMessage(client)
    await closeClient(client)

  let longest = nanoseconds(int64.high)

  test "withTransactionDeadline commits":
    proc t() {.async.} =
      let ms = startMockServer()
      let queries = new seq[string]
      let serverFut = serveOk(ms, 3, queries)
      let conn = await connect(mockConfig(ms.port))
      let held = holdTimer()
      let timers = timerCount()
      conn.withTransactionDeadline(longest):
        discard await conn.simpleExec("SELECT 1")
      doAssert timerCount() == timers
      cancelTimer(held)
      doAssert queries[] == @["BEGIN", "SELECT 1", "COMMIT"], $queries[]
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "withTransactionRetryDeadline commits":
    proc t() {.async.} =
      let ms = startMockServer()
      let queries = new seq[string]
      let serverFut = serveOk(ms, 3, queries)
      let conn = await connect(mockConfig(ms.port))
      let held = holdTimer()
      let timers = timerCount()
      conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), longest):
        discard await conn.simpleExec("SELECT 1")
      doAssert timerCount() == timers
      cancelTimer(held)
      doAssert queries[] == @["BEGIN", "SELECT 1", "COMMIT"], $queries[]
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "withSavepointDeadline releases":
    proc t() {.async.} =
      let ms = startMockServer()
      let queries = new seq[string]
      let serverFut = serveOk(ms, 5, queries)
      let conn = await connect(mockConfig(ms.port))
      let held = holdTimer()
      let timers = timerCount()
      conn.withTransaction:
        conn.withSavepointDeadline("sp1", longest):
          discard await conn.simpleExec("SELECT 1")
      doAssert timerCount() == timers
      cancelTimer(held)
      doAssert queries[1].startsWith("SAVEPOINT"), $queries[]
      doAssert queries[3].startsWith("RELEASE"), $queries[]
      doAssert queries[4] == "COMMIT", $queries[]
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

when hasAsyncDispatch:
  from std/os import sleep

  # A body that fails after `wait` saw the deadline but before it resumed: the
  # default orphan hook cleared that failure, so the timeout handler took the
  # body for completed and the macro returned as if it had committed.
  suite "transaction: a body failing on its deadline's tick":
    proc blockPast(deadline: Moment) =
      ## Block the event loop until `deadline` has passed, so every timer due by
      ## then fires in the next poll, in deadline order.
      sleep(max(0, toMilliseconds(deadline - Moment.now())) + 50)

    proc serveBeginRollback(
        ms: MockServer, queries: ref seq[string]
    ): Future[void] {.async.} =
      let client = await acceptAndReady(ms)
      for tx in ['T', 'I']: # BEGIN, then the timeout path's ROLLBACK
        queries[].add(await readSimpleQuery(client))
        await client.sendBytes(
          buildCommandComplete(queries[^1]) & buildReadyForQuery(tx)
        )
      await closeClient(client)

    test "withTransactionDeadline rolls back and raises PgTimeoutError":
      proc t() {.async.} =
        let ms = startMockServer()
        let queries = new seq[string]
        let serverFut = serveBeginRollback(ms, queries)
        let conn = await connect(mockConfig(ms.port))
        let deadline = milliseconds(300)
        let start = Moment.now()
        var caught: ref CatchableError
        try:
          conn.withTransactionDeadline(deadline):
            blockPast(start + deadline)
            await sleepMsAsync(0) # due after the deadline's timer
            if true:
              raise newException(ValueError, "boom")
        except CatchableError as e:
          caught = e
        doAssert caught of PgTimeoutError,
          (if caught == nil: "no error" else: caught.msg)
        await serverFut.wait(seconds(5))
        doAssert queries[] == @["BEGIN", "ROLLBACK"], $queries[]
        await conn.close()
        await ms.closeServer()

      waitFor t()

    test "withTransactionRetryDeadline rolls back and raises PgTimeoutError":
      proc t() {.async.} =
        let ms = startMockServer()
        let queries = new seq[string]
        let serverFut = serveBeginRollback(ms, queries)
        let conn = await connect(mockConfig(ms.port))
        let deadline = milliseconds(300)
        let start = Moment.now()
        var attempts = 0
        var caught: ref CatchableError
        try:
          conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), deadline):
            inc attempts
            blockPast(start + deadline)
            await sleepMsAsync(0) # due after the deadline's timer
            if true:
              raise newException(ValueError, "boom")
        except CatchableError as e:
          caught = e
        doAssert caught of PgTimeoutError,
          (if caught == nil: "no error" else: caught.msg)
        doAssert attempts == 1
        await serverFut.wait(seconds(5))
        doAssert queries[] == @["BEGIN", "ROLLBACK"], $queries[]
        await conn.close()
        await ms.closeServer()

      waitFor t()
