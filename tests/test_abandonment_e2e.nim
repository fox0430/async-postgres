## Abandonment / lifecycle-edge-case E2E tests.
##
## Complements existing coverage (test_e2e.nim has pipeline error isolation and
## basic cursor happy-path). These tests focus on what happens when cursors or
## COPY OUT operations are not cleanly torn down: orphaned cursors, cursor
## usage in an aborted transaction, and COPY OUT stalled on a short timeout.
## Ensures the connection state machine recovers to `csReady` or is
## transitioned to `csClosed` as appropriate, so pools never recycle a
## broken connection.

import std/[unittest, importutils]

import ../async_postgres/[async_backend, pg_client, pg_types]
import ../async_postgres/pg_connection {.all.}
import ./e2e_common

privateAccess(PgConnection)

# Cursor abandonment

suite "E2E: Cursor lifecycle invariants":
  test "cursor keeps connection csBusy until close() or exhaustion":
    # Pinning the documented invariant: after openCursor returns with rows
    # suspended server-side, the connection is csBusy and any attempt to
    # issue a direct conn.query MUST fail. Concurrent use of one connection is
    # a programming error, so it surfaces as PgStateError — NOT
    # PgConnectionError, which would wrongly drive a reconnect-on-failure
    # recovery loop. This refusal is what prevents a pool from handing out the
    # connection while a cursor is live.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let cursor =
        await conn.openCursor("SELECT i FROM generate_series(1, 100) i", chunkSize = 10)
      doAssert conn.state == csBusy, $conn.state
      var blocked = false
      var notConnError = false
      try:
        discard await conn.query("SELECT 1")
      except PgConnectionError:
        blocked = true
      except PgStateError:
        blocked = true
        notConnError = true
      doAssert blocked, "conn.query must refuse while cursor is active"
      doAssert notConnError,
        "concurrent use must raise PgStateError, not PgConnectionError"
      await cursor.close()
      doAssert conn.state == csReady
      let qr = await conn.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn.close()

    waitFor t()

  test "cursor.close() without any fetch leaves connection ready":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let cursor =
        await conn.openCursor("SELECT i FROM generate_series(1, 100) i", chunkSize = 10)
      await cursor.close()
      doAssert conn.state == csReady
      let qr = await conn.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn.close()

    waitFor t()

  test "openCursor with invalid SQL raises and leaves connection ready":
    # Recovery path: ErrorResponse during openCursor drains Sync back to
    # ReadyForQuery so the connection is reusable immediately.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.openCursor("SELECT * FROM nonexistent_table_xyz_12345")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady, $conn.state
      let qr = await conn.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn.close()

    waitFor t()

  test "fetchNext error marks cursor exhausted so close() skips a wasted round-trip":
    # An ErrorResponse mid-fetch drains a Sync that aborts the implicit
    # transaction holding the portal, so the server has already dropped it.
    # The cursor must be marked exhausted so a subsequent close() short-circuits
    # instead of issuing Close/Sync against a portal that no longer exists.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Errors with division by zero once the series reaches i == 5; chunkSize 3
      # so the first (buffered) chunk succeeds and a later fetchNext hits it.
      let cursor = await conn.openCursor(
        "SELECT 1 / (i - 5) FROM generate_series(1, 20) i", chunkSize = 3
      )
      discard await cursor.fetchNext() # buffered chunk: i = 1, 2, 3
      var raised = false
      try:
        while true:
          let rows = await cursor.fetchNext()
          if rows.len == 0:
            break
      except PgQueryError:
        raised = true
      doAssert raised, "fetchNext must surface the server error"
      doAssert cursor.exhausted, "error path must mark the cursor exhausted"
      doAssert conn.state == csReady, $conn.state
      await cursor.close() # short-circuits on exhausted; no extra Close/Sync
      doAssert conn.state == csReady, $conn.state
      let qr = await conn.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn.close()

    waitFor t()

# Pipeline error recovery beyond existing coverage

suite "E2E: Pipeline error recovery":
  test "executeIsolated: first op errors, later ops complete independently":
    # Existing test_e2e.nim covers the "middle op fails" case; here we pin
    # down the "first op fails" variant to ensure per-op SYNC returns the
    # connection to csReady before op 1 is dispatched.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = conn.newPipeline()
      p.addQuery("SELECT * FROM definitely_not_a_table_98765")
      p.addQuery("SELECT 'survived'::text AS s")
      p.addQuery("SELECT 42 AS n")
      let ir = await p.executeIsolated()
      doAssert ir.errors.len == 3
      doAssert ir.errors[0] != nil, "first op should fail"
      doAssert ir.errors[1] == nil, "second op should succeed"
      doAssert ir.errors[2] == nil, "third op should succeed"
      doAssert ir.results[1].kind == prkQuery
      doAssert ir.results[1].queryResult.rows[0].getStr(0) == "survived"
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "execute: error aborts the remaining batch but leaves connection ready":
    # Complements test_e2e.nim's "error aborts remaining ops" test — we also
    # verify that the batch abort recovers conn.state before the connection is
    # released back to a pool. The key invariant is that connection is NEVER
    # csBusy after a pipeline error propagates to the caller.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = conn.newPipeline()
      p.addQuery("SELECT 1")
      p.addQuery("SELECT * FROM definitely_not_a_table_54321")
      p.addQuery("SELECT 2")
      var gotError = false
      try:
        discard await p.execute()
      except PgError:
        gotError = true
      doAssert gotError
      doAssert conn.state == csReady, $conn.state
      let qr = await conn.query("SELECT 'post-batch'::text")
      doAssert qr.rowCount == 1
      await conn.close()

    waitFor t()

# COPY OUT timeout

suite "E2E: COPY OUT timeout / stall":
  test "slow COPY OUT with a short timeout raises PgTimeoutError and closes the connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Generate rows one at a time with pg_sleep(5) each; a 200ms timeout will
      # fire long before the first row is produced (200ms ≪ 5s per row), so this
      # is robust even on slow CI runners.
      let sql =
        "COPY (SELECT i, pg_sleep(5)::text FROM generate_series(1, 10) i) TO STDOUT"
      var timedOut = false
      try:
        discard await conn.copyOut(sql, timeout = milliseconds(200))
      except PgConnectionError as e:
        # A timeout poisons the connection (csClosed), so it must surface to an
        # `except PgConnectionError` reconnect loop while still being a
        # PgTimeoutError for callers that distinguish the cause.
        timedOut = e of ref PgTimeoutError
      doAssert timedOut, "copyOut timeout must be catchable as a PgConnectionError"
      doAssert conn.state == csClosed, $conn.state
      # A fresh connection works — the closed one is isolated.
      let conn2 = await connect(plainConfig())
      let qr = await conn2.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn2.close()

    waitFor t()

# simpleQuery timeout

suite "E2E: simpleQuery timeout / stall":
  test "slow simpleQuery with a short timeout raises PgTimeoutError and closes the connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # pg_sleep(5) blocks the result for 5s; a 200ms timeout fires long before
      # the row is produced, so this is robust even on slow CI runners.
      var timedOut = false
      try:
        discard
          await conn.simpleQuery("SELECT pg_sleep(5)", timeout = milliseconds(200))
      except PgConnectionError as e:
        # A timeout poisons the connection (csClosed), so it must surface to an
        # `except PgConnectionError` reconnect loop while still being a
        # PgTimeoutError for callers that distinguish the cause.
        timedOut = e of ref PgTimeoutError
      doAssert timedOut, "simpleQuery timeout must be catchable as a PgConnectionError"
      doAssert conn.state == csClosed, $conn.state
      # A fresh connection works — the closed one is isolated.
      let conn2 = await connect(plainConfig())
      let qr = await conn2.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn2.close()

    waitFor t()

  test "simpleQuery within its timeout completes normally and stays ready":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("SELECT 42 AS answer", timeout = seconds(5))
      doAssert results.len == 1
      doAssert results[0].rowCount == 1
      doAssert conn.state == csReady, $conn.state
      # The connection is reusable after a successful timed query.
      let qr = await conn.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn.close()

    waitFor t()
