## target_session_attrs probe tests using the in-process mock server.
##
## Verifies the libpq-compatible server-role checks in `checkSessionAttrs`:
## `tsaPrimary`/`tsaStandby` are judged on the recovery state (`in_hot_standby`
## ParameterStatus on PostgreSQL 14+, otherwise a
## `SELECT pg_catalog.pg_is_in_recovery()` probe), while `tsaReadWrite`/
## `tsaReadOnly` are judged on the read-only state. In particular a primary
## running with `default_transaction_read_only=on` must still match
## `tsaPrimary`, and an indeterminate probe result must skip the host.
## Also covers the `DateStyle` and `TimeZone` `connect` sends at startup.

import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_connection]
from ../async_postgres/pg_protocol import decodeInt32
from ../async_postgres/pg_connection/types import isUtcZoneName

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

proc readStartupParams(client: MockClient): Future[string] {.async.} =
  ## The StartupMessage's key/value cstrings (after the protocol version).
  let lenBuf = await readN(client, 4)
  let body = await readN(client, int(decodeInt32(lenBuf, 0)) - 4)
  for b in body[4 .. ^1]:
    result.add(char(b))

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
    # to SHOW transaction_read_only. Every spelling the server's parse_bool
    # reads as true (reaching extraParams verbatim, e.g. via a DSN) must be
    # recognised as physical replication.
    for spelling in ["on", "TRUE", "t", "Yes", "1"]:
      checkpoint spelling
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
        cfg.extraParams = @[("replication", spelling)]
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

  test "a probe that errors on one host fails over to the next":
    # An ErrorResponse to the probe is a per-host outcome (`PgQueryError`), not
    # a config fault: it must fold into the aggregate and let the next host be
    # tried, not escape `connect`.
    const errPid = 111'i32
    const okPid = 222'i32
    var connPid: int32 = 0

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        # Host 1: no in_hot_standby, so the recovery probe runs — and fails.
        let st1 = await acceptAndReady(ms, pid = errPid)
        try:
          discard await drainFrontendMessage(st1) # the probe Query
          await sendBytes(
            st1,
            buildErrorResponse("57P01", "terminating connection") &
              buildReadyForQuery('I'),
          )
          discard await drainFrontendMessage(st1) # Terminate
        except CatchableError:
          discard
        await closeClient(st1)
        # Host 2: reports its role, so no probe is needed.
        let st2 = await acceptAndReady(
          ms, pid = okPid, params = @[("in_hot_standby", "off")]
        )
          .wait(seconds(5))
        try:
          discard await drainFrontendMessage(st2) # Terminate
        except CatchableError:
          discard
        await closeClient(st2)

      let serverFut = serverHandler()
      var cfg = mockConfig(ms.port, tsaPrimary)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: ms.port),
        HostEntry(host: "127.0.0.1", port: ms.port),
      ]
      let conn = await connect(cfg)
      connPid = conn.pid
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check connPid == okPid

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

suite "connect: DateStyle and TimeZone":
  proc connectZoneOutcome(
      extra: seq[(string, string)], reported: string
  ): Future[(seq[string], string)] {.async.} =
    ## Queries after startup, then the session zone, or the connect error.
    ## An empty `reported` reports no zone.
    let ms = startMockServer()
    var queries: seq[string]
    var lastMsgType = '\0'
    proc serverHandler() {.async.} =
      let params =
        if reported.len > 0:
          @[("TimeZone", reported)]
        else:
          @[]
      let st = await acceptAndReady(ms, params = params)
      try:
        while true:
          let (msgType, body) = await drainFrontendMessage(st)
          if msgType != 'Q':
            lastMsgType = msgType
            break
          queries.add(queryText(body))
      except CatchableError:
        discard
      await closeClient(st)

    let serverFut = serverHandler()
    var cfg = mockConfig(ms.port, tsaAny)
    cfg.extraParams = extra
    var outcome: string
    try:
      let conn = await connect(cfg)
      outcome = conn.serverParam("TimeZone")
      await conn.close()
    except PgConnectionError as e:
      outcome = e.msg
    await serverFut
    await closeServer(ms)
    # A refused session ends with Terminate too, not a bare disconnect.
    doAssert lastMsgType == 'X'
    return (queries, outcome)

  proc startupAndFirstMsg(
      cfg: ConnConfig, ms: MockServer, reported: string
  ): Future[(string, char)] {.async.} =
    ## The StartupMessage parameters, and the first message after ReadyForQuery.
    var startup = ""
    var firstMsgType = '\0'
    proc serverHandler() {.async.} =
      let st = await ms.accept()
      startup = await readStartupParams(st)
      await sendFullHandshake(st, params = @[("DateStyle", reported)])
      try:
        let (msgType, _) = await drainFrontendMessage(st)
        firstMsgType = msgType
      except CatchableError:
        discard
      await closeClient(st)

    let serverFut = serverHandler()
    let conn = await connect(cfg)
    await conn.close()
    await serverFut
    await closeServer(ms)
    return (startup, firstMsgType)

  test "ISO is sent at startup, with no SET after it":
    # A startup value, unlike a SET, is what RESET and DISCARD ALL restore.
    proc testBody(): Future[(string, char)] {.async.} =
      let ms = startMockServer()
      return await startupAndFirstMsg(mockConfig(ms.port, tsaAny), ms, "ISO, MDY")

    let (startup, firstMsgType) = waitFor testBody()
    check "DateStyle\0ISO\0" in startup
    check firstMsgType == 'X'

  test "a caller's field order joins the startup ISO":
    proc testBody(): Future[(string, char)] {.async.} =
      let ms = startMockServer()
      var cfg = mockConfig(ms.port, tsaAny)
      cfg.extraParams = @[("datestyle", "DMY")]
      return await startupAndFirstMsg(cfg, ms, "ISO, DMY")

    let (startup, _) = waitFor testBody()
    check "DateStyle\0ISO, DMY\0" in startup
    check "datestyle" notin startup

  test "TimeZone is UTC at startup unless the caller sets one":
    proc testBody(extra: seq[(string, string)]): Future[(string, char)] {.async.} =
      let ms = startMockServer()
      var cfg = mockConfig(ms.port, tsaAny)
      cfg.extraParams = extra
      return await startupAndFirstMsg(cfg, ms, "ISO, MDY")

    let (byDefault, firstMsgType) = waitFor testBody(@[])
    check "TimeZone\0UTC\0" in byDefault
    check firstMsgType == 'X'
    let (direct, _) = waitFor testBody(@[("timezone", "Asia/Tokyo")])
    check "TimeZone\0Asia/Tokyo\0" in direct
    check "timezone" notin direct
    # A startup TimeZone would override the -c switch.
    let (viaOptions, _) = waitFor testBody(@[("options", "-c TimeZone=Asia/Tokyo")])
    check "options\0-c TimeZone=Asia/Tokyo\0" in viaOptions
    check "TimeZone\0UTC\0" notin viaOptions
    # The server applies startup values after the -c switches.
    let (both, _) = waitFor testBody(
      @[("TimeZone", "Asia/Tokyo"), ("options", "-c TimeZone=America/New_York")]
    )
    check "TimeZone\0Asia/Tokyo\0" in both
    # DEFAULT keeps the server's zone; sent as a value it would fail startup.
    let (inherit, _) = waitFor testBody(@[("TimeZone", "default")])
    check "TimeZone\0" notin inherit
    check "default" notin inherit

  test "a startup UTC the server did not apply fails connect":
    # A proxy that dropped the startup value; an explicit UTC is checked too,
    # directly or via a -c switch in options.
    # No SET stands in: transaction pooling would not keep it.
    for extra in [
      newSeq[(string, string)](),
      @[("TimeZone", "utc")],
      @[("TimeZone", "UTC0")],
      @[("TimeZone", "+00")],
      @[("options", "-c TimeZone=UTC")],
      @[("options", "-c TimeZone=GMT+00:00")],
      @[("options", "--timezone=Etc/UTC")],
      # DEFAULT sends none, so the -c switch is what asks for UTC.
      @[("TimeZone", "DEFAULT"), ("options", "-c TimeZone=0")],
    ]:
      let (queries, msg) = waitFor connectZoneOutcome(extra, "Asia/Tokyo")
      check queries.len == 0
      check "TimeZone is Asia/Tokyo, not UTC" in msg
    let none = newSeq[string]()
    check waitFor(connectZoneOutcome(@[], "UTC")) == (none, "UTC")
    check waitFor(connectZoneOutcome(@[], "Etc/UTC")) == (none, "Etc/UTC")
    check waitFor(connectZoneOutcome(@[], "")) == (none, "")
    # A -c switch naming another zone is honored, not overridden by a UTC pin.
    check waitFor(
      connectZoneOutcome(@[("options", "-c TimeZone=Asia/Tokyo")], "Asia/Tokyo")
    ) == (none, "Asia/Tokyo")
    # The last -c switch wins, as on the server.
    check waitFor(
      connectZoneOutcome(
        @[("options", "-c TimeZone=UTC -c TimeZone=Asia/Tokyo")], "Asia/Tokyo"
      )
    ) == (none, "Asia/Tokyo")
    check waitFor(connectZoneOutcome(@[("options", "-c TimeZone=UTC")], "UTC")) ==
      (none, "UTC")
    # A zero offset under a POSIX spelling is UTC, requested or reported.
    check waitFor(connectZoneOutcome(@[("TimeZone", "UTC0")], "UTC0")) == (none, "UTC0")
    check waitFor(connectZoneOutcome(@[("options", "-c TimeZone=+00")], "<+00>-00")) ==
      (none, "<+00>-00")
    # DEFAULT defers to a -c switch naming UTC: the switch is what is checked.
    check waitFor(
      connectZoneOutcome(
        @[("TimeZone", "DEFAULT"), ("options", "-c TimeZone=UTC")], "UTC"
      )
    ) == (none, "UTC")
    # Other zones go unchecked: the server reports "+09" as "<+09>-09".
    check waitFor(connectZoneOutcome(@[("TimeZone", "+09")], "<+09>-09")) ==
      (none, "<+09>-09")

  test "a UTC+0 spelling the server reports is not a dropped startup value":
    # A proxy that dropped the startup value still leaves a UTC session when
    # the server's own zone is UTC+0 under another spelling: the check must
    # not read that as the pin having been lost.
    for reported in [
      "UTC", "utc", "Etc/UTC", "GMT", "GMT0", "GMT+0", "GMT-0", "UTC+0", "UTC-0",
      "UTC+00", "GMT+00", "GMT-000", "UCT+0", "ZULU+0", "Etc/GMT+0", "ETC/GMT+00",
      "UTC0", "ETC/UTC0", "GMT+00:00", "UTC+00:00", "+00", "-0", "0", "0.0", "00",
      "00:00", "<+00>-00", "<UTC>0",
    ]:
      for extra in [
        newSeq[(string, string)](),
        @[("TimeZone", "GMT")],
        @[("options", "-c TimeZone=UTC")],
      ]:
        let none = newSeq[string]()
        check waitFor(connectZoneOutcome(extra, reported)) == (none, reported)

  test "isUtcZoneName reads a numeric tail, so zero-offset spellings pass":
    for zone in [
      "UTC",
      "utc",
      "Etc/UTC",
      "etc/utc",
      "UCT",
      "Etc/UCT",
      "Universal",
      "Etc/Universal",
      "Zulu",
      "Etc/Zulu",
      "GMT",
      "Etc/GMT",
      "GMT0",
      "Etc/GMT0",
      "GMT+0",
      "GMT-0",
      "Etc/GMT+0",
      "Etc/GMT-0",
      "Greenwich",
      "Etc/Greenwich",
      "UTC+0",
      "UTC-0",
      "UTC+00",
      "UTC-00",
      "GMT+00",
      "GMT-00",
      "GMT-000",
      "UCT+0",
      "ZULU+0",
      "ETC/GMT+00",
      # POSIX spellings of the links and bare zero offsets.
      "UTC0",
      "UTC00",
      "UTC0.0",
      "ETC/UTC0",
      "Etc/UTC0",
      "GMT+00:00",
      "GMT+0:0",
      "UTC+00:00",
      "UTC+00:00:00",
      "0",
      "00",
      "+00",
      "-0",
      "0.0",
      "-0.0",
      ".0",
      "0:0",
      "00:00",
      "0:0:0",
      # The server reports a numeric zone under a bracketed name.
      "<+00>-00",
      "<UTC>0",
      "<+00>+0",
    ]:
      check isUtcZoneName(zone)
    for zone in [
      "",
      "Asia/Tokyo",
      "Europe/Berlin",
      "America/New_York",
      "<+09>-09",
      "+09",
      "GMT+1",
      "GMT-1",
      "Etc/GMT+1",
      "Etc/GMT-8",
      "UTC+1",
      "UTC-9",
      "GMT+",
      "GMT+0x",
      "GMT0+1",
      "utc+",
      # A non-zero offset stays non-zero under every spelling.
      "0.5",
      "00:30",
      "UTC0:30",
      "GMT+00:01",
      "<+05:30>-05:30",
      "0:0:1",
    ]:
      check not isUtcZoneName(zone)

  test "a non-ISO DateStyle reported at startup fails connect":
    # A server or proxy that ignored the startup value.
    var msg = ""

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms, params = @[("DateStyle", "SQL, DMY")])
        await closeClient(st)

      let serverFut = serverHandler()
      try:
        let conn = await connect(mockConfig(ms.port, tsaAny))
        await conn.close()
      except PgConnectionError as e:
        {.cast(gcsafe).}:
          msg = e.msg
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check "DateStyle changed to SQL, DMY" in msg
