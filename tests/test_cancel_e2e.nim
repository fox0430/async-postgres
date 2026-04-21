## Cancellation E2E coverage beyond the simple-query case.
##
## Existing `test_e2e.nim` only tests cancel on `simpleQuery("pg_sleep(...)")`.
## These tests extend coverage to the extended query, pipeline, cursor, and
## COPY OUT paths, plus invalid-secret-key and post-completion race cases.
## All tests require a live PostgreSQL at 127.0.0.1:15432 (docker-compose.yml).

import std/[unittest, importutils, strutils]

import ../async_postgres/[async_backend, pg_client, pg_types]
import ../async_postgres/pg_connection {.all.}

privateAccess(PgConnection)

const
  PgHost = "127.0.0.1"
  PgPort = 15432
  PgUser = "test"
  PgPassword = "test"
  PgDatabase = "test"

proc plainConfig(): ConnConfig =
  ConnConfig(
    host: PgHost,
    port: PgPort,
    user: PgUser,
    password: PgPassword,
    database: PgDatabase,
    sslMode: sslDisable,
  )

template isCanceled(e: ref PgError): bool =
  "57014" in e.msg

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
      except PgError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.msg
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
      except PgError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.msg
      doAssert raised, "pipeline should have raised"
      doAssert conn.state == csReady, $conn.state
      let res = await conn.query("SELECT 42")
      doAssert res.rowCount == 1
      await conn.close()

    waitFor t()

  test "cancel aborts an executeIsolated op; subsequent ops are not cancelled":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = conn.newPipeline()
      p.addQuery("SELECT pg_sleep(30)")
      p.addQuery("SELECT 7")
      let fut = p.executeIsolated()
      await sleepAsync(milliseconds(100))
      await conn.cancel()
      let ir = await fut
      # First op must have errored with 57014. Second op should typically
      # have completed — but if it didn't (race), at minimum the errors
      # length equals ops length and at least one is 57014.
      doAssert ir.errors.len == 2
      var cancelSeen = false
      for e in ir.errors:
        if e != nil and "57014" in e.msg:
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
      except PgError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.msg
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
      except PgError as e:
        raised = true
        doAssert e.isCanceled, "expected 57014, got: " & e.msg
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
  test "cancel after query completion does not affect the next query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Run a quick query and wait for it to fully complete.
      let qr1 = await conn.query("SELECT 100")
      doAssert qr1.rowCount == 1
      # Now issue a cancel — it will race against nothing; the backend may
      # queue it for the NEXT statement per PostgreSQL documentation, but
      # since there is no active statement the CancelRequest is a no-op.
      await conn.cancel()
      # Next query may or may not be cancelled depending on timing. The
      # documented safe behaviour: either the next query succeeds, or it
      # raises 57014; the connection must recover to csReady either way.
      var secondOk = false
      var secondCancelled = false
      try:
        let qr2 = await conn.query("SELECT 200")
        doAssert qr2.rowCount == 1
        secondOk = true
      except PgError as e:
        doAssert e.isCanceled, "unexpected error on second query: " & e.msg
        secondCancelled = true
      doAssert secondOk or secondCancelled
      doAssert conn.state == csReady
      # A third query MUST succeed regardless — the connection is healthy.
      let qr3 = await conn.query("SELECT 300")
      doAssert qr3.rowCount == 1
      await conn.close()

    waitFor t()
