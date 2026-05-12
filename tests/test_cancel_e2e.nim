## Cancellation E2E coverage beyond the simple-query case.
##
## Existing `test_e2e.nim` only tests cancel on `simpleQuery("pg_sleep(...)")`.
## These tests extend coverage to the extended query, pipeline, cursor, and
## COPY OUT paths, plus invalid-secret-key and post-completion race cases.
## All tests require a live PostgreSQL at 127.0.0.1:15432 (docker-compose.yml).

import std/[unittest, importutils]

import ../async_postgres/[async_backend, pg_client, pg_types]
import ../async_postgres/pg_connection {.all.}
import ./e2e_common

privateAccess(PgConnection)

template isCanceled(e: ref PgQueryError): bool =
  e.sqlState == "57014"

# extended query

suite "E2E: Cancel extended query":
  test "cancel aborts a long pg_sleep issued via extended protocol":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let fut = conn.query("SELECT pg_sleep(30)")
      await sleepAsync(milliseconds(100))
      await conn.cancel()
      var raised = false
      try:
        discard await fut
      except PgQueryError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.sqlState
      doAssert raised, "query should have raised"
      doAssert conn.state == csReady, $conn.state
      # Connection is still usable afterwards.
      let res = await conn.query("SELECT 1")
      doAssert res.rowCount == 1
      await conn.close()

    waitFor t()

# pipeline

suite "E2E: Cancel pipeline execute":
  test "cancel aborts a shared-SYNC pipeline mid-batch":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = conn.newPipeline()
      p.addQuery("SELECT pg_sleep(30)")
      p.addQuery("SELECT 1")
      p.addQuery("SELECT 2")
      let fut = p.execute()
      await sleepAsync(milliseconds(100))
      await conn.cancel()
      var raised = false
      try:
        discard await fut
      except PgQueryError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.sqlState
      doAssert raised, "pipeline should have raised"
      doAssert conn.state == csReady, $conn.state
      let res = await conn.query("SELECT 42")
      doAssert res.rowCount == 1
      await conn.close()

    waitFor t()

  test "cancel propagates to at least one op in executeIsolated":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = conn.newPipeline()
      p.addQuery("SELECT pg_sleep(30)")
      p.addQuery("SELECT 7")
      let fut = p.executeIsolated()
      await sleepAsync(milliseconds(100))
      await conn.cancel()
      let ir = await fut
      # First op is expected to error with 57014. The second op usually
      # completes (per-op SYNC), but under timing race it may also be caught
      # by the cancel — both outcomes are acceptable as long as at least one
      # op reports 57014 and the errors length matches ops length.
      doAssert ir.errors.len == 2
      var cancelSeen = false
      for err in ir.errors:
        if err != nil and err of PgQueryError:
          if (ref PgQueryError)(err).sqlState == "57014":
            cancelSeen = true
      doAssert cancelSeen, "expected at least one 57014 in errors"
      doAssert conn.state == csReady, $conn.state
      let res = await conn.query("SELECT 'post-pipeline'::text AS s")
      doAssert res.rowCount == 1
      await conn.close()

    waitFor t()

# cursor

suite "E2E: Cancel cursor openCursor":
  test "cancel during a slow cursor open aborts with 57014":
    # openCursor performs the Parse + Bind + initial Execute internally, so the
    # first chunk of rows is already blocking on pg_sleep by the time cancel
    # arrives. fetchNext is not the right target because openCursor buffers the
    # first chunk before returning.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let fut = conn.openCursor(
        "SELECT i, pg_sleep(5) FROM generate_series(1, 5) i", chunkSize = 5
      )
      await sleepAsync(milliseconds(100))
      await conn.cancel()
      var raised = false
      try:
        discard await fut
      except PgQueryError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.sqlState
      doAssert raised, "openCursor should have raised"
      doAssert conn.state == csReady, $conn.state
      # A new query on the same connection must succeed.
      let res = await conn.query("SELECT 1")
      doAssert res.rowCount == 1
      await conn.close()

    waitFor t()

# copy out

suite "E2E: Cancel COPY OUT":
  test "cancel during a long COPY OUT aborts with 57014":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # A server-side-slow COPY OUT: generate_series with pg_sleep per row.
      let sql =
        "COPY (SELECT i, pg_sleep(5)::text FROM generate_series(1, 10) i) TO STDOUT"
      let fut = conn.copyOut(sql)
      await sleepAsync(milliseconds(100))
      await conn.cancel()
      var raised = false
      try:
        discard await fut
      except PgQueryError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.sqlState
      doAssert raised, "copyOut should have raised"
      doAssert conn.state == csReady, $conn.state
      let res = await conn.query("SELECT 1")
      doAssert res.rowCount == 1
      await conn.close()

    waitFor t()

# invalid secret key

suite "E2E: Cancel with invalid secret key":
  test "server ignores cancel with a wrong secret; original query continues":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Tamper the secret key so the CancelRequest is rejected by the server.
      let origSecret = conn.secretKey
      conn.secretKey = origSecret xor 0x5A5A5A5A'i32

      let fut = conn.query("SELECT pg_sleep(1)")
      await sleepAsync(milliseconds(100))
      await conn.cancel() # silently ignored by the server
      # Query completes normally (no 57014).
      let res = await fut
      doAssert res.rowCount == 1
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

# cancel race

suite "E2E: Cancel races":
  test "cancel after query completion recovers on a subsequent query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Run a quick query and wait for it to fully complete.
      let qr1 = await conn.query("SELECT 100")
      doAssert qr1.rowCount == 1
      # Now issue a cancel — CancelRequest is best-effort: the postmaster
      # sends SIGINT to the backend asynchronously. The signal may arrive:
      #   (a) while the backend is idle → ignored
      #   (b) while a subsequent query is executing → that query raises 57014
      # A single CancelRequest delivers at most one SIGINT, so at most one
      # subsequent query is cancelled. Under CI load the signal can be
      # delayed long enough to hit a query later than the immediate next
      # one — test only for recovery, not a specific target.
      await conn.cancel()
      # Loop until a query succeeds. At most one 57014 is expected; the
      # connection must recover to csReady after any cancellation.
      var recovered = false
      var cancelCount = 0
      for i in 1 .. 5:
        try:
          let qr = await conn.query("SELECT " & $(100 + i))
          doAssert qr.rowCount == 1
          recovered = true
          break
        except PgQueryError as e:
          doAssert e.isCanceled, "unexpected error on probe " & $i & ": " & e.sqlState
          doAssert conn.state == csReady
          cancelCount.inc
          doAssert cancelCount == 1, "SIGINT should be delivered at most once"
      doAssert recovered, "connection did not recover after cancel"
      await conn.close()

    waitFor t()
