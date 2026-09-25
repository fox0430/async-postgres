## `PgConnectionError.serverError`, `attempts` and `isTransientError`, using the
## in-process mock server: the server's ErrorResponse must survive a refused
## startup and a connection the server closes after a FATAL, and each failure is
## classified from what it records, so a reconnect loop can tell a bad password
## from a server shutdown.

import std/[os, sequtils, sets, strutils, unittest]
when defined(posix):
  import std/posix

import ../async_postgres/[async_backend, pg_replication]
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/types
import ../async_postgres/pg_connection/notify {.all.}
from ../async_postgres/pg_connection/buffer_io {.all.} import isTransientDial
from ../async_postgres/pg_errors {.all.} import isTransientServerError

import std/importutils
privateAccess(PgConnection)
privateAccess(PgConnectionError)

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
    check not isTransientError(err.attempt(0))
    check not isTransientError(err)

  test "a server still starting up is transient":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()
      let serverFut = refuseStartup(ms, "57P03")
      result = await connectError(mockConfig(ms.port))
      await serverFut
      await closeServer(ms)

    let err = waitFor testBody()
    check err != nil
    check err.refusal == "57P03"
    check isTransientError(err.attempt(0))
    check isTransientError(err)

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
    # The first host is only starting up, but the second's bad password awaits
    # it too once it is up.
    check not isTransientError(allRefused)
    check oneUnreachable != nil
    check oneUnreachable.attempts.len == 2
    check oneUnreachable.refusal(0) == "28P01"
    check oneUnreachable.sqlStates == @["28P01"]
    # The refused dial is raised as unavailable, the backend's error kept as cause.
    check oneUnreachable.attempts[1] of PgUnavailableError
    check oneUnreachable.attempts[1].parent != nil
    check not (oneUnreachable.attempts[1].parent of PgError)
    check isTransientError(oneUnreachable.attempts[1])
    # The refused dial does not hide the bad password.
    check not isTransientError(oneUnreachable)

  test "a host starting up and one refusing the dial keep the aggregate retryable":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms1 = startMockServer()
      let gone = startMockServer()
      let refusedPort = gone.port
      await closeServer(gone)
      var cfg = mockConfig(ms1.port)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: ms1.port),
        HostEntry(host: "127.0.0.1", port: refusedPort),
      ]
      let serverFut = refuseStartup(ms1, "57P03")
      result = await connectError(cfg)
      await serverFut
      await closeServer(ms1)

    let err = waitFor testBody()
    check err != nil
    check err.attempts.len == 2
    check isTransientError(err)

  test "the aggregate is permanent when every host refused permanently":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms1 = startMockServer()
      let ms2 = startMockServer()
      var cfg = mockConfig(ms1.port)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: ms1.port),
        HostEntry(host: "127.0.0.1", port: ms2.port),
      ]
      let serverFut = refuseStartup(ms1, "28P01")
      let serverFut2 = refuseStartup(ms2, "3D000")
      result = await connectError(cfg)
      await serverFut
      await serverFut2
      await closeServer(ms1)
      await closeServer(ms2)

    let err = waitFor testBody()
    check err != nil
    check err.refusal(0) == "28P01"
    check err.refusal(1) == "3D000"
    check not isTransientError(err)

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
    check not isTransientError(err)

  test "a host that answers but does not match keeps the aggregate retryable":
    # A failover may promote it.
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
    check isTransientError(err)

  test "a fork failure in the pre-3.0 format is retried":
    let err = waitFor connectErrorOn(
      @[buildPreV3Error("could not fork new process for connection: out of memory\n")]
    )
    check err != nil
    # asyncdispatch appends an async traceback to the message.
    let text = err.msg.split("\nAsync traceback:")[0]
    check text.endsWith("could not fork new process for connection: out of memory")
    check not (err.parent of PgProtocolError)
    check isTransientError(err)

  test "a pre-3.0 error shorter than a v3 header keeps its text":
    # 'E' + "no" + NUL: the server closes short of the 5 bytes of a v3 header.
    let err = waitFor connectErrorOn(@[buildPreV3Error("no")])
    check err != nil
    let text = err.msg.split("\nAsync traceback:")[0]
    check text.endsWith(": no")
    # Not a failed fork: a server before protocol 3.0, or not PostgreSQL.
    check not isTransientError(err)

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
    # Only the untranslated text is known for a failed fork.
    check not isTransientError(err)

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

  test "prefer-standby judges each host by its second attempt":
    # Both hosts are starting up during the standby pass and reject the
    # password by the second: that stale 57P03 must not keep the loop going.
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
    check not isTransientError(err)

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
    check isTransientError(err)

  test "a statement ERROR before the connection drops is not kept":
    # The ERROR belongs to the statement; the failure is the lost connection,
    # which a reconnect loop must still retry.
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
    check isTransientError(err)

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

  test "a closed connection is transient unless its FATAL recurs":
    let conn = PgConnection()
    check isTransientError(conn.newClosedError("lost"))
    conn.fatalServerError = fatalError("57P01")
    check isTransientError(conn.newClosedError("lost"))
    conn.fatalServerError = fatalError("57P04")
    check not isTransientError(conn.newClosedError("lost"))

  test "a listen death carries the FATAL that ended the session":
    let conn = PgConnection()
    conn.fatalServerError = (ref PgQueryError)(msg: "bye", sqlState: "57P01")
    conn.notifyListenDeath("Listen connection lost", false)
    check conn.listenError.serverError != nil
    check conn.listenError.serverError.sqlState == "57P01"
    check isTransientError(conn.listenError)

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
    check isTransientError(raised)

  test "a LISTEN that loses the connection keeps the cause":
    let conn = PgConnection()
    conn.fatalServerError = fatalError("57P04")
    conn.markClosed()
    let cause = conn.newClosedError("lost")
    conn.restartPumpOrFailWaiter(restarted = true, cause = cause)
    check conn.listenError != nil
    check conn.listenError.parent == cause
    check conn.listenError.serverError != nil
    check conn.listenError.serverError.sqlState == "57P04"
    # The database is gone: a redial cannot get past that.
    check not isTransientError(conn.listenError)

  test "a cancelled LISTEN is not the listen death's cause":
    let conn = PgConnection()
    conn.markClosed()
    conn.restartPumpOrFailWaiter(
      restarted = true, cause = newException(CancelledError, "cancelled")
    )
    check conn.listenError != nil
    check conn.listenError.parent of PgUnavailableError
    # The connection it retired is lost all the same: a reconnect loop redials.
    check isTransientError(conn.listenError)

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
    let err = waitFor listenDeathAfterRedial(rdGone)
    check err.sqlStates == @["57P01"]
    check isTransientError(err)

  test "a listen death is judged by its last redial, not the session's FATAL":
    # The server came back with a rotated password: 57P01 alone would retry.
    let err = waitFor listenDeathAfterRedial(rdPasswordRotated)
    check err.sqlStates == @["57P01", "28P01"]
    check not isTransientError(err)

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
    check isTransientError(err)

suite "client-side refusals":
  proc refusal(
      cfg: ConnConfig, reply: seq[byte], thenRefusedDial = false
  ): Future[ref PgConnectionError] {.async.} =
    ## `connect`'s error when the server answers the startup with `reply`;
    ## `thenRefusedDial` adds a second host whose dial is refused.
    let ms = startMockServer()
    proc serverHandler() {.async.} =
      let st = await ms.accept()
      try:
        await drainStartupMessage(st)
        await sendBytes(st, reply)
        discard await readN(st, 1)
      except CatchableError:
        discard
      await closeClient(st)

    var cfg = cfg
    cfg.port = ms.port
    if thenRefusedDial:
      let gone = startMockServer()
      let refusedPort = gone.port
      await closeServer(gone)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: ms.port),
        HostEntry(host: "127.0.0.1", port: refusedPort),
      ]
    let serverFut = serverHandler()
    result = await connectError(cfg)
    await serverFut
    await closeServer(ms)

  test "an auth method outside require_auth is not retried":
    var cfg = mockConfig(0)
    cfg.requireAuth = {amScramSha256}
    let cleartext = buildBackendMsg('R', @[byte 0, 0, 0, 3])
    let err = waitFor refusal(cfg, cleartext)
    check err != nil
    check err.serverError == nil
    check (ref Exception)(err) of PgSecurityError
    check not isTransientError(err)

  test "a refusal is not hidden behind another host's refused dial":
    var cfg = mockConfig(0)
    cfg.requireAuth = {amScramSha256}
    let cleartext = buildBackendMsg('R', @[byte 0, 0, 0, 3])
    let err = waitFor refusal(cfg, cleartext, thenRefusedDial = true)
    check err != nil
    check err.attempts.len == 2
    check err.attempts[0] of PgSecurityError
    check err.attempt(0).serverError == nil
    check isTransientError(err.attempts[1])
    check not isTransientError(err)

  test "sslmode=allow stays retryable while plaintext is only starting up":
    proc testBody(): Future[ref PgConnectionError] {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        # Plaintext leg: the server is still starting up.
        let st1 = await ms.accept()
        try:
          await drainStartupMessage(st1)
          await sendBytes(st1, buildErrorResponse("57P03", "starting up", "FATAL"))
        except CatchableError:
          discard
        await closeClient(st1)
        # SSL leg: the server has no SSL.
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
    check err.attempt(0).attempts.len == 2
    check isTransientError(err)

  proc allowError(
      cfg: ConnConfig, plainReply, sslReply: seq[byte]
  ): Future[ref PgConnectionError] {.async.} =
    ## `connect`'s error under sslmode=allow: the plaintext attempt's startup
    ## answered with `plainReply`, the TLS attempt's SSLRequest with `sslReply`.
    let ms = startMockServer()
    proc serverHandler() {.async.} =
      for reply in [plainReply, sslReply]:
        let st = await ms.accept()
        try:
          await drainStartupMessage(st)
          await sendBytes(st, reply)
          discard await readN(st, 1)
        except CatchableError:
          discard
        await closeClient(st)

    var cfg = cfg
    cfg.port = ms.port
    cfg.sslMode = sslAllow
    let serverFut = serverHandler()
    result = await connectError(cfg)
    await serverFut
    await closeServer(ms)

  test "sslmode=allow keeps both attempts":
    let err = waitFor allowError(
      mockConfig(0), buildErrorResponse("28P01", "bad password", "FATAL"), @[byte('N')]
    )
    check err != nil
    let allow = err.attempt(0)
    check allow.serverError == nil
    check allow.attempts.len == 2
    check allow.refusal(0) == "28P01"
    check "does not support SSL" in allow.attempts[1].msg
    # allow never required TLS, so an 'N' is no security refusal.
    check not (allow.attempts[1] of PgSecurityError)
    check allow.parent == allow.attempts[1]
    check err.sqlStates == @["28P01"]
    # The server answers 'N' again as surely as it refuses the password again.
    check not isTransientError(err)

  test "sslmode=allow reports data trailing 'N' as a protocol violation":
    # One segment so chronos's `readOnce` pulls the extra byte in.
    let err = waitFor allowError(
      mockConfig(0),
      buildErrorResponse("28P01", "bad password", "FATAL"),
      @[byte('N'), byte('X')],
    )
    check err != nil
    let tlsAttempt = err.attempt(0).attempts[1]
    check "after SSL refusal" in tlsAttempt.msg
    check tlsAttempt of PgProtocolError
    check not (tlsAttempt of PgSecurityError)

  test "sslmode=allow refused on security grounds both ways stays a PgSecurityError":
    var cfg = mockConfig(0)
    cfg.requireAuth = {amScramSha256}
    # Plaintext: cleartext auth, outside require_auth. TLS: bytes injected
    # after 'S', in one segment for chronos's `readOnce`.
    let err = waitFor allowError(
      cfg, buildBackendMsg('R', @[byte 0, 0, 0, 3]), @[byte('S'), byte('X')]
    )
    check err != nil
    check (ref Exception)(err) of PgSecurityError
    check not isTransientError(err)

  test "sslmode=allow with only one security refusal is no PgSecurityError":
    var cfg = mockConfig(0)
    cfg.requireAuth = {amScramSha256}
    let err =
      waitFor allowError(cfg, buildBackendMsg('R', @[byte 0, 0, 0, 3]), @[byte('N')])
    check err != nil
    check err.attempt(0).attempts[0] of PgSecurityError
    check not ((ref Exception)(err) of PgSecurityError)

  test "sslmode=require against a server without SSL is not retried":
    var cfg = mockConfig(0)
    cfg.sslMode = sslRequire
    let err = waitFor refusal(cfg, @[byte('N')])
    check err != nil
    check (ref Exception)(err) of PgSecurityError
    check not isTransientError(err)

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

suite "isTransientError":
  proc queryError(sqlState: string, severity = "ERROR"): ref PgQueryError =
    (ref PgQueryError)(msg: sqlState, sqlState: sqlState, severity: severity)

  proc summing(attempts: varargs[ref CatchableError]): ref PgConnectionError =
    (ref PgConnectionError)(msg: "sum", attempts: @attempts)

  proc hosts(attempts: varargs[ref CatchableError]): ref PgConnectionError =
    (ref PgConnectionError)(msg: "hosts", attempts: @attempts, perHost: true)

  test "a connection error is judged by what it records":
    check isTransientError((ref PgUnavailableError)(msg: "lost"))
    # A failure nobody classified is not retried.
    check not isTransientError((ref PgConnectionError)(msg: "unsupported"))
    check isTransientError(
      (ref PgConnectionError)(msg: "gone", serverError: queryError("57P01", "FATAL"))
    )
    check not isTransientError(
      (ref PgConnectionError)(msg: "dropped", serverError: queryError("3D000", "FATAL"))
    )
    # The FATAL behind a lost connection outranks its type.
    check not isTransientError(
      (ref PgUnavailableError)(
        msg: "dropped", serverError: queryError("57P04", "FATAL")
      )
    )
    check not isTransientError((ref PgSecurityError)(msg: "refused"))
    check not isTransientError((ref PgProtocolError)(msg: "garbled"))
    check not isTransientError(
      (ref PgListenError)(msg: "stopped", transportAlive: true)
    )

  test "an error summing up attempts is transient when any of them is":
    let lost = (ref PgUnavailableError)(msg: "lost")
    let refused = (ref PgSecurityError)(msg: "refused")
    check isTransientError(summing(refused, lost))
    check not isTransientError(summing(refused, refused))
    # The attempts, not a stale FATAL of the session they tried to replace.
    let redialed = summing(refused)
    redialed.serverError = queryError("57P01", "FATAL")
    check not isTransientError(redialed)

  test "a server's lasting refusal in any attempt outranks the rest":
    # The same config meets it again once the other hosts are back.
    let lost = (ref PgUnavailableError)(msg: "lost")
    let badPassword =
      (ref PgConnectionError)(msg: "refused", serverError: queryError("28P01", "FATAL"))
    let startingUp = (ref PgConnectionError)(
      msg: "starting", serverError: queryError("57P03", "FATAL")
    )
    let noTls = (ref PgConnectionError)(msg: "Server does not support SSL")
    check not isTransientError(summing(lost, badPassword))
    # Nested, as sslmode=allow's pair sits in connect's aggregate.
    check not isTransientError(summing(lost, summing(badPassword, lost)))
    # A refusal that clears hides nothing; a failure without one is no verdict.
    check isTransientError(summing(startingUp, noTls))
    check isTransientError(summing(lost, noTls))

  test "connect's aggregate is transient only when every host's failure is":
    let lost = (ref PgUnavailableError)(msg: "lost")
    let tlsAlert = (ref PgConnectionError)(msg: "certificate required")
    let refused = (ref PgSecurityError)(msg: "refused")
    let startingUp = (ref PgConnectionError)(
      msg: "starting", serverError: queryError("57P03", "FATAL")
    )
    let noTls = (ref PgConnectionError)(msg: "Server does not support SSL")
    check isTransientError(hosts(lost, startingUp))
    # The same config meets them again once the other host is back.
    check not isTransientError(hosts(tlsAlert, lost))
    check not isTransientError(hosts(lost, refused))
    # A host's sslmode=allow legs still clear when either does.
    check isTransientError(hosts(summing(startingUp, noTls), lost))

  test "timeouts are transient, other raw errors unclassified":
    check isTransientError((ref PgTimeoutError)(msg: "timeout"))
    # `connect` surfaces a host's connectTimeout as is.
    check isTransientError((ref AsyncTimeoutError)(msg: "timeout"))
    # A lost transport is raised as PgUnavailableError, never raw.
    check not isTransientError((ref OSError)(msg: "refused"))
    when hasChronos:
      check not isTransientError((ref TransportOsError)(msg: "refused"))

  test "a cancel whose dial is refused is transient":
    proc testBody(): Future[ref CatchableError] {.async.} =
      let ms = startMockServer()
      let port = ms.port
      await closeServer(ms)
      let conn = PgConnection(host: "127.0.0.1", port: port)
      try:
        await conn.cancel()
      except CatchableError as e:
        result = e

    let err = waitFor testBody()
    check err of PgUnavailableError
    check err.parent != nil
    check not (err.parent of PgError)
    check isTransientError(err)

  when defined(posix):
    proc cancelVia(dir: string): ref CatchableError =
      proc testBody(): Future[ref CatchableError] {.async.} =
        try:
          await PgConnection(host: dir, port: 5432).cancel()
        except CatchableError as e:
          result = e

      waitFor testBody()

    test "a Unix socket not created yet is transient, a forbidden one is not":
      let dir = getTempDir() / "async_postgres_dial_" & $getCurrentProcessId()
      createDir(dir)
      defer:
        setFilePermissions(dir, {fpUserRead, fpUserWrite, fpUserExec})
        removeDir(dir)
      # No server started: the socket file is not there (ENOENT).
      let missing = cancelVia(dir)
      check missing of PgUnavailableError
      check isTransientError(missing)
      # Root bypasses the permission check.
      if posix.geteuid() != 0:
        setFilePermissions(dir, {})
        let forbidden = cancelVia(dir) # EACCES
        check forbidden of PgConnectionError
        check not (forbidden of PgUnavailableError)
        check not isTransientError(forbidden)

    test "a dial out of descriptors, buffers or memory is transient":
      for code in [posix.EMFILE, posix.ENFILE, posix.ENOBUFS, posix.ENOMEM]:
        when hasChronos:
          check isTransientDial((ref TransportOsError)(code: OSErrorCode(code)))
        else:
          check isTransientDial((ref OSError)(errorCode: code))
      when hasChronos:
        # chronos raises EMFILE on socket creation without its code.
        check isTransientDial((ref TransportTooManyError)(msg: "too many"))
        check not isTransientDial(
          (ref TransportOsError)(code: OSErrorCode(posix.EACCES))
        )
        check not isTransientDial((ref TransportAddressError)(msg: "bad"))
      else:
        check not isTransientDial((ref OSError)(errorCode: posix.EACCES))
      check not isTransientDial((ref ValueError)(msg: "bad"))

  test "a connection error is judged by its type, not the cause it wraps":
    proc wrapping(cause: ref Exception): ref PgConnectionError =
      (ref PgConnectionError)(msg: "wrapped", parent: cause)

    check not isTransientError(wrapping((ref OSError)(msg: "reset")))
    check not isTransientError(wrapping((ref PgUnavailableError)(msg: "lost")))
    check isTransientError(
      (ref PgUnavailableError)(msg: "lost", parent: (ref ValueError)(msg: "bad"))
    )

  test "a listen death whose transport is gone follows what ended the pump":
    proc death(cause: ref Exception): ref PgListenError =
      (ref PgListenError)(msg: "died", parent: cause)

    check isTransientError(death((ref PgUnavailableError)(msg: "lost")))
    check not isTransientError(death((ref PgProtocolError)(msg: "garbled")))
    check not isTransientError(death(nil))

  test "server errors are classified by SQLSTATE":
    for s in [
      "53100", "53200", "53300", "25P03", "25P04", "40001", "40P01", "57P01", "57P02",
      "57P03", "57P05", "55006", "55P03",
    ]:
      check isTransientError(queryError(s))
      check isTransientError(queryError(s, "FATAL"))
    for s in ["28P01", "28000", "3D000", "42501", "42704", "08P01", "53400"]:
      check not isTransientError(queryError(s))
      check not isTransientError(queryError(s, "FATAL"))
    # A FATAL 08xxx ends this session; a statement's is a dblink or
    # postgres_fdw link, which may fail for good (e.g. on a wrong password).
    for s in ["08000", "08001", "08003", "08006"]:
      check isTransientError(queryError(s, "FATAL"))
      check not isTransientError(queryError(s))
    # A statement's 57014 is a cancel; FATAL, it is the server's
    # authentication_timeout, which a reconnect clears.
    check not isTransientError(queryError("57014"))
    check isTransientError(queryError("57014", "FATAL"))
    # A PANIC means crash recovery, whatever its SQLSTATE.
    check isTransientError(queryError("XX000", "PANIC"))
    check not isTransientError(queryError("XX000", "FATAL"))
    # A missing ErrorResponse is not transient.
    check not isTransientServerError(nil)

  test "config, state and cancellation are not transient":
    check not isTransientError(nil)
    check not isTransientError((ref PgConfigError)(msg: "cfg"))
    check not isTransientError((ref PgStateError)(msg: "busy"))
    # The pull API's live-transport listen death: re-`listen`, do not re-dial.
    check not isTransientError((ref PgListenStoppedError)(msg: "stopped"))
    check not isTransientError((ref CancelledError)(msg: "cancelled"))
    check not isTransientError((ref ValueError)(msg: "bad"))

  test "pool errors follow their kind and the failure they wrap":
    let lost = (ref PgUnavailableError)(msg: "lost")
    let refused = (ref PgSecurityError)(msg: "refused")
    check isTransientError(newPoolError(pekAcquireTimeout, "t"))
    # Capacity may free up: the parent (a connect failure that used up the
    # deadline, a cluster's replica failure) only explains the wait.
    check isTransientError(newPoolError(pekAcquireTimeout, "t", refused))
    check isTransientError(newPoolError(pekQueueFull, "q"))
    check not isTransientError(newPoolError(pekClosed, "c"))
    check not isTransientError(newPoolError(pekConfigFault, "f"))
    check isTransientError(newPoolError(pekConnectFailed, "c", lost))
    # A per-host `connectTimeout` a spawned connect surfaced is retried.
    check isTransientError(
      newPoolError(pekConnectFailed, "c", (ref AsyncTimeoutError)(msg: "timeout"))
    )
    check not isTransientError(newPoolError(pekConnectFailed, "c", refused))
    check isTransientError(
      newPoolError(pekBatchFailed, "b", newPoolError(pekAcquireTimeout, "t"))
    )
    check not isTransientError(newPoolError(pekBatchFailed, "b", refused))
