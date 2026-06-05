## target_session_attrs probe tests using the in-process mock server.
##
## Verifies the libpq-compatible server-role checks in `checkSessionAttrs`:
## `tsaPrimary`/`tsaStandby` are judged on the recovery state (`in_hot_standby`
## ParameterStatus on PostgreSQL 14+, otherwise a
## `SELECT pg_catalog.pg_is_in_recovery()` probe), while `tsaReadWrite`/
## `tsaReadOnly` are judged on the read-only state. In particular a primary
## running with `default_transaction_read_only=on` must still match
## `tsaPrimary`, and an indeterminate probe result must skip the host.

import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_connection]

import mock_pg_server

proc mockConfig(port: int, attrs: TargetSessionAttrs): ConnConfig =
  ConnConfig(
    host: "127.0.0.1",
    port: port,
    user: "test",
    database: "test",
    sslMode: sslDisable,
    targetSessionAttrs: attrs,
  )

proc buildSingleRowResult(colName, value, tag: string): seq[byte] =
  ## RowDescription + DataRow + CommandComplete + ReadyForQuery in one shot.
  result.add(buildRowDescription(colName))
  result.add(buildDataRow(value))
  result.add(buildCommandComplete(tag))
  result.add(buildReadyForQuery('I'))

proc buildEmptyResult(colName, tag: string): seq[byte] =
  ## RowDescription + CommandComplete + ReadyForQuery — a probe response with
  ## zero rows (no DataRow), which `checkSessionAttrs` treats as indeterminate.
  result.add(buildRowDescription(colName))
  result.add(buildCommandComplete(tag))
  result.add(buildReadyForQuery('I'))

suite "target_session_attrs: recovery-state checks":
  test "tsaPrimary accepts a read-only-by-default primary without a probe query":
    # The M-6 regression: a primary with default_transaction_read_only=on
    # must match tsaPrimary (libpq judges primary/standby on recovery state,
    # not on the read-only state). With in_hot_standby reported (PG 14+),
    # no probe query may be sent at all.
    var firstMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(
          ms,
          params = @[("in_hot_standby", "off"), ("default_transaction_read_only", "on")],
        )
        try:
          # The next frontend message must be Terminate, not a probe Query.
          let (msgType, _) = await drainFrontendMessage(st)
          firstMsgType = msgType
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaPrimary))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check firstMsgType == 'X'

  test "tsaStandby rejects a server reporting in_hot_standby=off":
    # The host must be rejected for a role mismatch specifically, not merely
    # fail to connect for some unrelated reason; checking the error message
    # keeps the test from passing on an incidental connect failure.
    var rejectedForMismatch = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms, params = @[("in_hot_standby", "off")])
        try:
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port, tsaStandby))
        await conn.close()
      except PgConnectionError as e:
        rejectedForMismatch = e.msg.contains("does not match target_session_attrs")
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check rejectedForMismatch

  test "tsaStandby accepts a server reporting in_hot_standby=on":
    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms, params = @[("in_hot_standby", "on")])
        try:
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaStandby))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()

  test "tsaPrimary falls back to SELECT pg_is_in_recovery() when in_hot_standby is not reported":
    var probeOk = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        # Pre-14 server: no in_hot_standby ParameterStatus.
        let st = await acceptAndReady(ms)
        try:
          let (msgType, body) = await drainFrontendMessage(st)
          probeOk =
            msgType == 'Q' and queryText(body) == "SELECT pg_catalog.pg_is_in_recovery()"
          await sendBytes(
            st, buildSingleRowResult("pg_is_in_recovery", "f", "SELECT 1")
          )
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaPrimary))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check probeOk

  test "a pre-14 physical replication connection probes SHOW, not SELECT":
    # A walsender rejects arbitrary SQL, so the recovery probe must fall back
    # to SHOW transaction_read_only. The alternate boolean spelling
    # `replication=on` (reaching extraParams verbatim, e.g. via a DSN) must
    # also be recognised as physical replication.
    var probeOk = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        # Pre-14 server: no in_hot_standby ParameterStatus.
        let st = await acceptAndReady(ms)
        try:
          let (msgType, body) = await drainFrontendMessage(st)
          probeOk = msgType == 'Q' and queryText(body) == "SHOW transaction_read_only"
          await sendBytes(
            st, buildSingleRowResult("transaction_read_only", "off", "SHOW")
          )
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      var cfg = mockConfig(ms.port, tsaPrimary)
      cfg.extraParams = @[("replication", "on")]
      let conn = await connect(cfg)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check probeOk

  test "an indeterminate recovery probe skips the host (fail-closed)":
    # A probe that returns zero rows is indeterminate; libpq advances to the
    # next host rather than accepting an unknown server. connect() must raise.
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st) # the probe Query
          await sendBytes(st, buildEmptyResult("pg_is_in_recovery", "SELECT 0"))
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port, tsaPrimary))
        await conn.close()
      except PgConnectionError:
        raised = true
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised

  test "tsaPreferStandby falls back to any server when no standby is found":
    # Distinct BackendKeyData pids let us prove the connection came from the
    # second (accept-any) pass; bounding the second accept makes a pass-1
    # over-accept regression fail instead of hanging.
    const pass1Pid = 111'i32
    const pass2Pid = 222'i32
    var connPid: int32 = 0

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        # First pass: standby probe fails against a primary.
        let st1 = await acceptAndReady(
          ms, pid = pass1Pid, params = @[("in_hot_standby", "off")]
        )
        try:
          discard await drainFrontendMessage(st1) # Terminate
        except CatchableError:
          discard
        await closeClient(st1)
        # Second pass: any server is accepted.
        let st2 = await acceptAndReady(
          ms, pid = pass2Pid, params = @[("in_hot_standby", "off")]
        )
          .wait(seconds(5))
        try:
          discard await drainFrontendMessage(st2) # Terminate
        except CatchableError:
          discard
        await closeClient(st2)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaPreferStandby))
      connPid = conn.pid
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check connPid == pass2Pid

suite "target_session_attrs: read-only-state checks":
  test "tsaReadWrite still probes SHOW transaction_read_only":
    var probeOk = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms, params = @[("in_hot_standby", "off")])
        try:
          let (msgType, body) = await drainFrontendMessage(st)
          probeOk = msgType == 'Q' and queryText(body) == "SHOW transaction_read_only"
          await sendBytes(
            st, buildSingleRowResult("transaction_read_only", "off", "SHOW")
          )
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaReadWrite))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check probeOk

  test "tsaReadOnly accepts a read-only server via SHOW transaction_read_only":
    var probeOk = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        # No in_hot_standby / default_transaction_read_only reported, so the
        # read-only state must be probed with SHOW transaction_read_only.
        let st = await acceptAndReady(ms)
        try:
          let (msgType, body) = await drainFrontendMessage(st)
          probeOk = msgType == 'Q' and queryText(body) == "SHOW transaction_read_only"
          await sendBytes(
            st, buildSingleRowResult("transaction_read_only", "on", "SHOW")
          )
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaReadOnly))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check probeOk

  test "tsaReadWrite answers from the reported GUCs without a probe query":
    # PG 14+ reports both default_transaction_read_only and in_hot_standby,
    # so the read-only state needs no round-trip: the next frontend message
    # must be Terminate, not a probe Query.
    var firstMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(
          ms,
          params =
            @[("default_transaction_read_only", "off"), ("in_hot_standby", "off")],
        )
        try:
          let (msgType, _) = await drainFrontendMessage(st)
          firstMsgType = msgType
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaReadWrite))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check firstMsgType == 'X'

  test "tsaReadOnly accepts a read-only-by-default primary without a probe query":
    # default_transaction_read_only=on alone makes the session read-only,
    # even on a primary (in_hot_standby=off) — answered from the reported
    # GUCs with no probe query.
    var firstMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(
          ms,
          params = @[("default_transaction_read_only", "on"), ("in_hot_standby", "off")],
        )
        try:
          let (msgType, _) = await drainFrontendMessage(st)
          firstMsgType = msgType
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaReadOnly))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check firstMsgType == 'X'

  test "tsaAny accepts any server without a probe query":
    var firstMsgType = '\0'

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms, params = @[("in_hot_standby", "off")])
        try:
          let (msgType, _) = await drainFrontendMessage(st)
          firstMsgType = msgType
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port, tsaAny))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check firstMsgType == 'X'
