## `PgConnectionError.serverError` and `attempts`, using the in-process mock
## server: the server's ErrorResponse must survive a refused startup and a
## connection the server closes after a FATAL, so a caller can tell a bad
## password from a server shutdown.

import std/[sequtils, sets, strutils, unittest]

import ../async_postgres/[async_backend, pg_replication]
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/types
import ../async_postgres/pg_connection/notify {.all.}

import std/importutils
privateAccess(PgConnection)

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc refuseStartup(ms: MockServer, sqlState: string) {.async.} =
  let st = await ms.accept()
  try:
    await drainStartupMessage(st)
    await sendBytes(st, buildErrorResponse(sqlState, "refused", "FATAL"))
  except CatchableError:
    discard
  await closeClient(st)

type Redial = enum
  rdGone ## the server is gone
  rdPasswordRotated ## back, refusing the password
  rdListenRefused ## back, refusing the re-LISTEN
  rdListenFatal ## back, ending the new session during the re-LISTEN

proc fatalError(sqlState: string): ref PgQueryError =
  (ref PgQueryError)(msg: sqlState, sqlState: sqlState, severity: "FATAL")

proc connectError(cfg: ConnConfig): Future[ref PgConnectionError] {.async.} =
  try:
    let conn = await connect(cfg)
    await conn.close()
  except PgConnectionError as e:
    return e

proc replyToStartup(ms: MockServer, chunks: seq[seq[byte]]) {.async.} =
  ## Answer the StartupMessage with `chunks`, 50 ms apart, then close.
  let st = await ms.accept()
  try:
    await drainStartupMessage(st)
    for i, chunk in chunks:
      if i > 0:
        await sleepAsync(milliseconds(50))
      await sendBytes(st, chunk)
  except CatchableError:
    discard
  await closeClient(st)

proc connectErrorOn(chunks: seq[seq[byte]]): Future[ref PgConnectionError] {.async.} =
  ## The error `connect` raises when the server answers the startup with `chunks`.
  let ms = startMockServer()
  let serverFut = replyToStartup(ms, chunks)
  result = await connectError(mockConfig(ms.port))
  await serverFut
  await closeServer(ms)

proc attempt(e: ref PgConnectionError, i: int): ref PgConnectionError =
  (ref PgConnectionError)(e.attempts[i])

proc sqlStates(e: ref Exception): seq[string] =
  e.serverErrors.mapIt(it.sqlState)

proc refusal(e: ref PgConnectionError, i = 0): string =
  ## SQLSTATE of the refusal behind the aggregate's `i`th host, "" if none.
  let se = e.attempt(i).serverError
  if se == nil: "" else: se.sqlState

suite "serverError on startup":
  test "a refused startup keeps the SQLSTATE":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()
      let serverFut = refuseStartup(ms, "28P01")
      result = await connectError(mockConfig(ms.port))
      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    check err != nil
    # The aggregate carries none of its own: the host's failure holds it.
    check err.serverError == nil
    check err.attempts.len == 1
    check err.refusal == "28P01"
    check err.attempt(0).serverError.severity == "FATAL"
    check err.sqlStates == @["28P01"]

  test "the aggregate keeps every host's failure":
    proc testBody(): Future[(ref PgConnectionError, ref PgConnectionError)] {.async.} =
      var allRefused, oneUnreachable: ref PgConnectionError
      let ms1 = startMockServer()
      let ms2 = startMockServer()
      var cfg = mockConfig(ms1.port)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: ms1.port),
        HostEntry(host: "127.0.0.1", port: ms2.port),
      ]
      var serverFut = refuseStartup(ms1, "57P03")
      let serverFut2 = refuseStartup(ms2, "28P01")
      allRefused = await connectError(cfg)
      await serverFut
      await serverFut2

      # The second host now refuses the TCP connection outright.
      await closeServer(ms2)
      serverFut = refuseStartup(ms1, "28P01")
      oneUnreachable = await connectError(cfg)
      await serverFut
      await closeServer(ms1)
      return (allRefused, oneUnreachable)

    let (allRefused, oneUnreachable) = waitFor testBody()
    check allRefused != nil
    check allRefused.serverError == nil
    check allRefused.attempts.len == 2
    check allRefused.refusal(0) == "57P03"
    check allRefused.refusal(1) == "28P01"
    check allRefused.parent == allRefused.attempts[1]
    check allRefused.sqlStates == @["57P03", "28P01"]
    check oneUnreachable != nil
    check oneUnreachable.attempts.len == 2
    check oneUnreachable.refusal(0) == "28P01"
    check oneUnreachable.sqlStates == @["28P01"]

  test "a failed session-attrs probe is not taken for a refusal":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st) # the probe
          await sendBytes(
            st,
            buildErrorResponse("42501", "permission denied") & buildReadyForQuery('I'),
          )
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      var cfg = mockConfig(ms.port)
      cfg.targetSessionAttrs = tsaPrimary
      result = await connectError(cfg)
      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    check err != nil
    check err.serverError == nil
    check err.attempts.len == 1
    check err.attempts[0] of PgQueryError
    check err.sqlStates.len == 0

  test "a host that answers but does not match is kept among the attempts":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st) # the probe
          await sendBytes(
            st,
            buildRowDescription("pg_is_in_recovery") & buildDataRow("t") &
              buildCommandComplete("SELECT 1") & buildReadyForQuery('I'),
          )
          discard await drainFrontendMessage(st) # Terminate
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      var cfg = mockConfig(ms.port)
      cfg.targetSessionAttrs = tsaPrimary
      result = await connectError(cfg)
      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    check err != nil
    check err.attempts.len == 1
    check "target_session_attrs" in err.attempt(0).msg

  test "a fork failure in the pre-3.0 format keeps its text":
    let err = waitFor connectErrorOn(
      @[buildPreV3Error("could not fork new process for connection: out of memory\n")]
    )
    check err != nil
    # asyncdispatch appends an async traceback to the message.
    let text = err.msg.split("\nAsync traceback:")[0]
    check text.endsWith("could not fork new process for connection: out of memory")
    check not (err.parent of PgProtocolError)

  test "a pre-3.0 error shorter than a v3 header keeps its text":
    # 'E' + "no" + NUL: the server closes short of the 5 bytes of a v3 header.
    let err = waitFor connectErrorOn(@[buildPreV3Error("no")])
    check err != nil
    let text = err.msg.split("\nAsync traceback:")[0]
    check text.endsWith(": no")

  test "an empty pre-3.0 error text gets a fallback message":
    let err = waitFor connectErrorOn(@[buildPreV3Error("")])
    check err != nil
    check "rejected the connection" in err.msg

  test "a localized fork failure split across reads is read to its end":
    # A UTF-8 first byte decodes as a negative length; the text arrives in
    # pieces and the postmaster closes right after it.
    const text = "\xE6\x96\xB0 could not fork new process: out of memory"
    let reply = buildPreV3Error(text)
    let err = waitFor connectErrorOn(@[reply[0 ..< 6], reply[6 .. ^1]])
    check err != nil
    check text in err.msg

  test "a pre-3.0 error text is capped at MAX_ERRLEN":
    let err = waitFor connectErrorOn(@[buildPreV3Error('x'.repeat(40000))])
    check err != nil
    let text = err.msg.split("\nAsync traceback:")[0]
    check text.endsWith('x'.repeat(30000))
    check not text.endsWith('x'.repeat(30001))

  test "a long ErrorResponse after the first reply is not taken for pre-3.0":
    # Only the first reply can be pre-3.0: past it, a v3 error above
    # MAX_ERRLEN keeps its fields.
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendBytes(st, buildAuthSASL())
          discard await drainFrontendMessage(st)
          await sendBytes(st, buildErrorResponse("28P01", 'x'.repeat(40000), "FATAL"))
        except CatchableError:
          discard
        await closeClient(st)

      var cfg = mockConfig(ms.port)
      cfg.password = "pw"
      let serverFut = serverHandler()
      result = await connectError(cfg)
      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    check err != nil
    check err.refusal == "28P01"

  test "a message consumed internally still counts as the first reply":
    # nextMessage never returns ParameterStatus, yet it ends the window for
    # a pre-3.0 reply as any other message does.
    let err = waitFor connectErrorOn(
      @[
        buildParameterStatus("server_version", "18.0"),
        buildErrorResponse("28P01", 'x'.repeat(40000), "FATAL"),
      ]
    )
    check err != nil
    check err.refusal == "28P01"

  test "prefer-standby keeps each host's second attempt":
    # Both hosts are starting up during the standby pass and reject the
    # password by the second: the stale 57P03 is not what they failed with.
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms1 = startMockServer()
      let ms2 = startMockServer()
      var cfg = mockConfig(ms1.port)
      cfg.targetSessionAttrs = tsaPreferStandby
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: ms1.port),
        HostEntry(host: "127.0.0.1", port: ms2.port),
      ]
      proc serve(ms: MockServer) {.async.} =
        await refuseStartup(ms, "57P03")
        await refuseStartup(ms, "28P01")

      let f1 = serve(ms1)
      let f2 = serve(ms2)
      result = await connectError(cfg)
      await f1
      await f2
      await closeServer(ms1)
      await closeServer(ms2)

    let err = waitFor testBody()
    check err != nil
    check err.refusal(0) == "28P01"
    check err.refusal(1) == "28P01"

suite "serverError when the server closes the connection":
  test "a FATAL during a query":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      var err: ref PgConnectionError
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          await sendBytes(st, buildErrorResponse("57P01", "terminating", "FATAL"))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgConnectionError as e:
        err = e
      await conn.close()
      await serverFut
      await closeServer(ms)
      return err

    let err = waitFor testBody()
    check err != nil
    check err.serverError != nil
    check err.serverError.sqlState == "57P01"

  test "a statement ERROR before the connection drops is not kept":
    # The ERROR belongs to the statement; the failure is the lost connection.
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      var err: ref PgConnectionError
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          await sendBytes(st, buildErrorResponse("23505", "duplicate key"))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("INSERT INTO t VALUES (1)")
      except PgConnectionError as e:
        err = e
      await conn.close()
      await serverFut
      await closeServer(ms)
      return err

    let err = waitFor testBody()
    check err != nil
    check err.serverError == nil

  test "a FATAL the session answers after is not kept":
    # Only a proxy does this; the server closes after its own FATAL.
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      var err: ref PgConnectionError
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st)
          await sendBytes(
            st,
            buildErrorResponse("57P01", "terminating", "FATAL") & buildReadyForQuery(
              'I'
            ),
          )
          discard await drainFrontendMessage(st)
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgQueryError:
        discard
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgConnectionError as e:
        err = e
      await conn.close()
      await serverFut
      await closeServer(ms)
      return err

    let err = waitFor testBody()
    check err != nil
    check err.serverError == nil

  test "an in-place reconnect forgets the old session's FATAL":
    proc testBody(): Future[ref PgQueryError] {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        sc2 = await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      conn.fatalServerError = (ref PgQueryError)(msg: "gone", sqlState: "57P01")
      await conn.reconnectInPlace()
      result = conn.newClosedError("lost").serverError
      await serverFut
      await conn.close()
      await closeClient(sc1)
      await closeClient(sc2)
      await closeServer(ms)

    check waitFor(testBody()) == nil

  proc fatalAfterFailedReListen(reply: seq[byte]): Future[string] {.async.} =
    ## The old session ended with FATAL 57P01 and the redial's re-LISTEN gets
    ## `reply`. Returns the SQLSTATE `conn` is left dead of, "" if none.
    let ms = startMockServer()
    var sc1, sc2: MockClient
    proc serverHandler() {.async.} =
      sc1 = await acceptAndReady(ms)
      sc2 = await acceptAndReady(ms)
      try:
        discard await drainFrontendMessage(sc2) # re-LISTEN
        await sendBytes(sc2, reply)
      except CatchableError:
        discard
      await closeClient(sc2)

    let serverFut = serverHandler()
    let conn = await connect(mockConfig(ms.port))
    conn.listenChannels.incl("x")
    conn.fatalServerError = fatalError("57P01")
    try:
      await conn.reconnectInPlace()
    except CatchableError:
      discard
    let se = conn.newClosedError("lost").serverError
    result = if se == nil: "" else: se.sqlState
    await serverFut
    await conn.close()
    await closeClient(sc1)
    await closeServer(ms)

  test "a failed re-LISTEN leaves the old session's FATAL":
    let reply =
      buildErrorResponse("42501", "permission denied") & buildReadyForQuery('I')
    check waitFor(fatalAfterFailedReListen(reply)) == "57P01"

  test "a redial that ends with a FATAL of its own keeps it":
    let reply = buildErrorResponse("57P02", "crash", "FATAL")
    check waitFor(fatalAfterFailedReListen(reply)) == "57P02"

  test "each error gets its own copy of the FATAL":
    let conn = PgConnection()
    conn.fatalServerError = fatalError("57P01")
    conn.markClosed()
    let a = conn.newClosedError("a").serverError
    let b = conn.newClosedError("b").serverError
    check a.sqlState == "57P01"
    check a != b
    check a != conn.fatalServerError
    conn.notifyListenDeath("Listen connection lost", false)
    let stored = conn.listenError.serverError
    check stored != conn.fatalServerError
    var raised: ref PgListenError
    try:
      conn.checkListenAlive()
    except PgListenError as e:
      raised = e
    check raised.serverError.sqlState == "57P01"
    check raised.serverError != stored

  test "a listen death carries the FATAL that ended the session":
    let conn = PgConnection()
    conn.fatalServerError = (ref PgQueryError)(msg: "bye", sqlState: "57P01")
    conn.notifyListenDeath("Listen connection lost", false)
    check conn.listenError.serverError != nil
    check conn.listenError.serverError.sqlState == "57P01"

  test "a late FATAL explains a listen death latched on a live transport":
    let conn = PgConnection()
    conn.listenError = (ref PgListenError)(msg: "stopped", transportAlive: true)
    conn.fatalServerError = (ref PgQueryError)(msg: "bye", sqlState: "57P01")
    conn.markClosed()
    var raised: ref PgListenError
    try:
      conn.checkListenAlive()
    except PgListenError as e:
      raised = e
    check raised != nil
    check raised.serverError != nil
    check raised.serverError.sqlState == "57P01"

  test "a LISTEN that loses the connection keeps the cause":
    let conn = PgConnection()
    conn.fatalServerError = fatalError("57P04")
    conn.markClosed()
    let cause = conn.newClosedError("lost")
    conn.restartPumpOrFailWaiter(restarted = true, cause = cause)
    check conn.listenError != nil
    check conn.listenError.parent == cause
    check conn.listenError.serverError.sqlState == "57P04"

  test "a cancelled LISTEN is not the listen death's cause":
    let conn = PgConnection()
    conn.markClosed()
    conn.restartPumpOrFailWaiter(
      restarted = true, cause = newException(CancelledError, "cancelled")
    )
    check conn.listenError != nil
    check conn.listenError.parent == nil

  test "a LISTEN that leaves the connection busy keeps the cause":
    let conn = PgConnection()
    conn.markBusy()
    let cause = newException(PgStateError, "busy")
    conn.restartPumpOrFailWaiter(restarted = true, cause = cause)
    check conn.listenError != nil
    check conn.listenError.transportAlive
    check conn.listenError.parent == cause

  proc listenDeathAfterRedial(redial: Redial): Future[ref PgListenError] {.async.} =
    ## The session ends with FATAL 57P01, then the one redial fails as `redial`
    ## says. Returns what `onListenError` got, nil if nothing.
    var death: ref PgListenError
    let ms = startMockServer()
    proc serverHandler() {.async.} =
      let st = await acceptAndReady(ms)
      try:
        discard await drainFrontendMessage(st) # LISTEN
        await sendBytes(st, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        await sleepAsync(milliseconds(50))
        await sendBytes(st, buildErrorResponse("57P01", "shutting down", "FATAL"))
      except CatchableError:
        discard
      await closeClient(st)
      case redial
      of rdGone:
        await closeServer(ms)
      of rdPasswordRotated:
        await refuseStartup(ms, "28P01")
      of rdListenRefused:
        let st2 = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st2) # re-LISTEN
          await sendBytes(
            st2,
            buildErrorResponse("42501", "permission denied") & buildReadyForQuery('I'),
          )
        except CatchableError:
          discard
        await closeClient(st2)
      of rdListenFatal:
        let st2 = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st2) # re-LISTEN
          await sendBytes(st2, buildErrorResponse("57P02", "crash", "FATAL"))
        except CatchableError:
          discard
        await closeClient(st2)

    let serverFut = serverHandler()
    let conn = await connect(mockConfig(ms.port))
    conn.listenReconnectMaxAttempts = 1
    conn.listenReconnectMaxBackoff = 1
    conn.onListenError(
      proc(err: ref PgListenError) {.gcsafe, raises: [].} =
        death = err
    )
    await conn.listen("x")
    await serverFut.wait(seconds(5))
    var spins = 0
    while death == nil and spins < 2500:
      inc spins
      await sleepAsync(milliseconds(2))
    if redial != rdGone:
      await closeServer(ms)
    try:
      await conn.close()
    except CatchableError:
      discard
    return death

  test "a listen death after an unreachable redial keeps the session's FATAL":
    check waitFor(listenDeathAfterRedial(rdGone)).sqlStates == @["57P01"]

  test "a listen death keeps its last redial beside the session's FATAL":
    # The server came back with a rotated password.
    let err = waitFor listenDeathAfterRedial(rdPasswordRotated)
    check err.sqlStates == @["57P01", "28P01"]

  test "a listen death keeps the session's FATAL past a redial that got in":
    # The redial connects but its re-LISTEN is refused.
    let err = waitFor listenDeathAfterRedial(rdListenRefused)
    require err != nil
    # The refused re-LISTEN is a statement's ERROR, kept only as the attempt.
    check err.sqlStates == @["57P01"]
    check err.attempts.len == 1
    check err.attempts[0] of PgQueryError
    check (ref PgQueryError)(err.attempts[0]).sqlState == "42501"

  test "a listen death keeps the session's FATAL apart from its redial's":
    # The redial's session ends with a FATAL of its own during the re-LISTEN.
    let err = waitFor listenDeathAfterRedial(rdListenFatal)
    check err.sqlStates == @["57P01", "57P02"]

  test "a FATAL ending a replication stream":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      var err: ref PgConnectionError
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        try:
          discard await drainFrontendMessage(st) # START_REPLICATION
          await sendBytes(st, buildCopyBothResponse())
          await sendBytes(st, buildErrorResponse("57P01", "terminating", "FATAL"))
        except CatchableError:
          discard
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        discard msg
      try:
        await conn.startReplication("test_slot", callback = cb)
      except PgConnectionError as e:
        err = e
      await conn.close()
      await serverFut
      await closeServer(ms)
      return err

    let err = waitFor testBody()
    check err != nil
    check err.serverError != nil
    check err.serverError.sqlState == "57P01"

suite "SSL request":
  test "sslmode=allow keeps both attempts":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st1 = await ms.accept()
        try:
          await drainStartupMessage(st1)
          await sendBytes(st1, buildErrorResponse("28P01", "bad password", "FATAL"))
        except CatchableError:
          discard
        await closeClient(st1)
        let st2 = await ms.accept()
        try:
          await drainStartupMessage(st2)
          await sendBytes(st2, @[byte('N')])
          discard await readN(st2, 1)
        except CatchableError:
          discard
        await closeClient(st2)

      let serverFut = serverHandler()
      var cfg = mockConfig(ms.port)
      cfg.sslMode = sslAllow
      result = await connectError(cfg)
      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    check err != nil
    let allow = err.attempt(0)
    check allow.serverError == nil
    check allow.attempts.len == 2
    check allow.refusal(0) == "28P01"
    check "does not support SSL" in allow.attempts[1].msg
    check allow.parent == allow.attempts[1]
    check err.sqlStates == @["28P01"]

suite "serverErrors":
  proc refused(sqlState: string): ref CatchableError =
    (ref PgConnectionError)(msg: sqlState, serverError: fatalError(sqlState))

  proc failed(
      msg: string, attempts: varargs[ref CatchableError]
  ): ref PgConnectionError =
    (ref PgConnectionError)(msg: msg, attempts: @attempts)

  test "an error's own first, then its attempts depth-first":
    let unreachable = (ref CatchableError)(newException(OSError, "refused"))
    let err = failed(
      "any host", refused("57P03"), failed("allow", refused("28P01"), unreachable)
    )
    err.serverError = fatalError("57P01")
    check err.sqlStates == @["57P01", "57P03", "28P01"]

  test "a statement's ERROR among the attempts is left out":
    let probe =
      (ref CatchableError)(newPgQueryError(@[ErrorField(code: 'C', value: "42501")]))
    check failed("any host", probe).sqlStates.len == 0

  test "a connection error's parent is not walked":
    # A listen death's cause carries the same FATAL.
    let err = (ref PgConnectionError)(
      msg: "lost", serverError: fatalError("57P01"), parent: refused("57P01")
    )
    check err.sqlStates == @["57P01"]

  test "another error's parent is":
    let err = newPoolError(
      pekConnectFailed, "Pool connect failed", failed("any host", refused("28P01"))
    )
    check err.sqlStates == @["28P01"]

  test "a FATAL raised as a query error counts":
    let fatal = newPgQueryError(
      @[ErrorField(code: 'V', value: "FATAL"), ErrorField(code: 'C', value: "57P01")]
    )
    check fatal.sqlStates == @["57P01"]

  test "nil has none":
    check serverErrors(nil).len == 0

suite "severity":
  test "severity prefers the non-localized field":
    let err = newPgQueryError(
      @[
        ErrorField(code: 'S', value: "FATAL-localized"),
        ErrorField(code: 'V', value: "FATAL"),
        ErrorField(code: 'C', value: "57P01"),
      ]
    )
    check err.severity == "FATAL"

  test "only a FATAL or PANIC ends the session":
    check isSessionFatal("FATAL")
    check isSessionFatal("PANIC")
    check not isSessionFatal("ERROR")
