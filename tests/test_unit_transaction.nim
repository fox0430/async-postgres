## Unit tests for `pg_client/transaction` via the mock server.
##
## PG-less: `withTransaction` commit/rollback, the body-escape compile-time
## guards, a body that ends its own transaction, and a body failing on its
## deadline's tick.

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

proc answerSimpleQueries(
    ms: MockServer, queries: ref seq[string], terminated: ref bool = nil
) {.async.} =
  ## Accept one client and answer each simple query until it hangs up. COMMIT
  ## and ROLLBACK end the transaction; anything else leaves one open. A
  ## Terminate sets `terminated` when given.
  let client = await acceptAndReady(ms)
  try:
    while true:
      let m = await drainFrontendMessage(client)
      if m.msgType == 'X' and terminated != nil:
        terminated[] = true
      if m.msgType != 'Q':
        continue
      let q = queryText(m.body)
      queries[].add(q)
      let tx = if q in ["COMMIT", "ROLLBACK"]: 'I' else: 'T'
      await client.sendBytes(buildCommandComplete(q) & buildReadyForQuery(tx))
  except CatchableError:
    discard
  await closeClient(client)

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

when hasChronos:
  template keepsConnOnExpiredDeadline(name: string, scope: untyped) {.dirty.} =
    ## `scope` runs on `conn` and must raise `PgTimeoutError` with the
    ## transaction rolled back and the connection kept.
    test "an expired " & name & " rolls back past the scopes nested in it":
      proc t() {.async.} =
        let ms = startMockServer()
        let queries = new seq[string]
        let serverFut = answerSimpleQueries(ms, queries)
        let conn = await connect(mockConfig(ms.port))
        var timedOut = false
        try:
          scope
        except PgTimeoutError:
          timedOut = true
        doAssert timedOut
        doAssert conn.state == csReady and conn.txStatus == tsIdle,
          "got " & $conn.state & "/" & $conn.txStatus & " after " & $queries[]
        doAssert queries[][^1] == "ROLLBACK", "got " & $queries[]
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

suite "transaction: mock round trip":
  test "withTransaction commits":
    proc t() {.async.} =
      let ms = startMockServer()
      var queries: seq[string]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        for _ in 0 ..< 3: # BEGIN, SELECT 1, COMMIT
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

  # A body raising CancelledError takes the path a chronos cancel between
  # statements does; asyncdispatch reaches it no other way.
  test "withTransaction retires the connection when the body is cancelled":
    proc t() {.async.} =
      let ms = startMockServer()
      let queries = new seq[string]
      let terminated = new bool
      let serverFut = answerSimpleQueries(ms, queries, terminated)
      let conn = await connect(mockConfig(ms.port))
      var caught = false
      try:
        conn.withTransaction:
          if true:
            raise newException(CancelledError, "body cancelled")
      except CancelledError:
        caught = true
      doAssert caught
      doAssert conn.state == csClosed, "got " & $conn.state
      await conn.close()
      await serverFut
      doAssert queries[] == @["BEGIN"],
        "no ROLLBACK may follow a cancel; got " & $queries[]
      # The wire is settled, so close still ends the session cleanly.
      doAssert terminated[], "close sent no Terminate"
      await ms.closeServer()

    waitFor t()

  test "a cancelled withSavepoint keeps the outer transaction from committing":
    # The caller goes on after the cancel, as after a `wait` timeout: the outer
    # COMMIT must not keep the savepoint's writes.
    proc t() {.async.} =
      let ms = startMockServer()
      let queries = new seq[string]
      let serverFut = answerSimpleQueries(ms, queries)
      let conn = await connect(mockConfig(ms.port))
      var outerErr = false
      try:
        conn.withTransaction:
          try:
            conn.withSavepoint("sp1"):
              discard await conn.simpleExec("INSERT")
              if true:
                raise newException(CancelledError, "body cancelled")
          except CancelledError:
            discard
      except CancelledError:
        doAssert false, "the savepoint's cancel was caught in the body"
      except CatchableError:
        outerErr = true
      doAssert outerErr, "the outer transaction must fail, not commit"
      doAssert conn.state == csClosed, "got " & $conn.state
      await conn.close()
      await serverFut
      doAssert queries[].len == 3 and queries[][1].startsWith("SAVEPOINT") and
        queries[][2] == "INSERT", "nothing may follow the cancel; got " & $queries[]
      await ms.closeServer()

    waitFor t()

  when hasChronos:
    test "a cancel inside a deadline's body still retires the connection":
      # The body catches the cancel and goes on, so the INSERT after it must
      # fail rather than run in the abandoned savepoint.
      proc t() {.async.} =
        let ms = startMockServer()
        let queries = new seq[string]
        let serverFut = answerSimpleQueries(ms, queries)
        let conn = await connect(mockConfig(ms.port))
        proc savepointBody() {.async.} =
          conn.withSavepoint("sp1"):
            await sleepAsync(seconds(10))

        var outerErr = false
        try:
          conn.withTransactionDeadline(seconds(60)):
            try:
              await savepointBody().wait(milliseconds(100))
            except AsyncTimeoutError:
              discard
            discard await conn.simpleExec("INSERT")
        except CatchableError:
          outerErr = true
        doAssert outerErr, "the outer transaction must fail, not commit"
        doAssert conn.state == csClosed, "got " & $conn.state
        await conn.close()
        await serverFut
        doAssert queries[].len == 2, "nothing may follow the cancel; got " & $queries[]
        await ms.closeServer()

      waitFor t()

    # The deadline cancels its body while it sleeps in nested scopes. They
    # retire the connection; the deadline takes it back for its ROLLBACK.
    keepsConnOnExpiredDeadline("withTransactionDeadline"):
      conn.withTransactionDeadline(milliseconds(200)):
        conn.withSavepointDeadline("sp1", seconds(60)):
          conn.withSavepoint("sp2"):
            await sleepAsync(seconds(10))

    keepsConnOnExpiredDeadline("withTransactionRetryDeadline"):
      conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), milliseconds(200)):
        conn.withSavepointDeadline("sp1", seconds(60)):
          conn.withSavepoint("sp2"):
            await sleepAsync(seconds(10))

    keepsConnOnExpiredDeadline("withSavepointDeadline"):
      conn.withTransaction:
        conn.withSavepointDeadline("sp1", milliseconds(200)):
          conn.withSavepoint("sp2"):
            await sleepAsync(seconds(10))

    test "a body swallowing its deadline's cancel commits nothing":
      # The savepoint is neither released nor rolled back, so a COMMIT would
      # keep its writes.
      proc t() {.async.} =
        let ms = startMockServer()
        let queries = new seq[string]
        let serverFut = answerSimpleQueries(ms, queries)
        let conn = await connect(mockConfig(ms.port))
        var failed = false
        try:
          conn.withTransactionDeadline(milliseconds(200)):
            try:
              conn.withSavepoint("sp"):
                discard await conn.simpleExec("INSERT")
                await sleepAsync(seconds(10))
            except CatchableError:
              discard
        except CatchableError:
          failed = true
        doAssert failed
        doAssert conn.state == csClosed, "got " & $conn.state
        doAssert "COMMIT" notin queries[], "got " & $queries[]
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

template raisesForEndedTx(name: string, scope: untyped) {.dirty.} =
  ## `scope` runs on `conn`, and its body sends a COMMIT of its own.
  test name & " raises PgStateError in place of its COMMIT":
    proc t() {.async.} =
      let ms = startMockServer()
      let queries = new seq[string]
      let serverFut = answerSimpleQueries(ms, queries)
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      try:
        scope
      except PgStateError:
        raised = true
      doAssert raised
      doAssert queries[] == @["BEGIN", "COMMIT"], "got " & $queries[]
      doAssert conn.state == csReady and conn.txStatus == tsIdle,
        "got " & $conn.state & "/" & $conn.txStatus
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

suite "transaction: a body that ends its transaction":
  raisesForEndedTx("withTransaction"):
    conn.withTransaction:
      discard await conn.simpleExec("COMMIT")

  raisesForEndedTx("withTransactionRetry"):
    conn.withTransactionRetry(RetryOptions(maxAttempts: 3)):
      discard await conn.simpleExec("COMMIT")

  raisesForEndedTx("withTransactionDeadline"):
    conn.withTransactionDeadline(seconds(5)):
      discard await conn.simpleExec("COMMIT")

  raisesForEndedTx("withTransactionRetryDeadline"):
    conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), seconds(5)):
      discard await conn.simpleExec("COMMIT")

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
      let timers = farTimerCount()
      conn.withTransactionDeadline(longest):
        discard await conn.simpleExec("SELECT 1")
      doAssert farTimerCount() == timers
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
      let timers = farTimerCount()
      conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), longest):
        discard await conn.simpleExec("SELECT 1")
      doAssert farTimerCount() == timers
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
      let timers = farTimerCount()
      conn.withTransaction:
        conn.withSavepointDeadline("sp1", longest):
          discard await conn.simpleExec("SELECT 1")
      doAssert farTimerCount() == timers
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
