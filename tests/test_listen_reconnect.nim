## Regression test for the LISTEN/NOTIFY in-place reconnect path.
##
## Contract under test: after `reconnectInPlace` swaps in a fresh transport, it
## must copy `recvBuf` *and* `recvBufStart` from the new connection as a pair.
## A freshly-connected `PgConnection` has consumed the entire startup/auth
## handshake (AuthOk + ParameterStatus* + BackendKeyData + ReadyForQuery); when
## that handshake arrives in a single packet the bytes stay in `recvBuf` with
## `recvBufStart` pointing past them (no compaction runs after the final
## ReadyForQuery). So a clean post-connect connection satisfies
## `recvBufStart == recvBuf.len` with `recvBuf.len > 0`.
##
## If `reconnectInPlace` copies only `recvBuf` and leaves `recvBufStart` at 0
## (its pre-connect reset value), the connection is left pointing at the front
## of a buffer full of already-consumed auth bytes. The next `nextMessage` — the
## re-`LISTEN` round trip — re-parses them and desyncs, so the headline
## auto-reconnect feature dies on its first re-subscribe.
##
## Regression target (CR-2): `reconnectInPlace` copied `newConn.recvBuf` but not
## `newConn.recvBufStart`. We reconnect with no subscribed channels so the
## re-LISTEN loop is a no-op, isolating the buffer copy, and assert the pairing
## invariant directly — deterministic, no timing dependence on either backend.

import std/[monotimes, unittest, sets, strutils]

import ../async_postgres/async_backend
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/buffer_io
import ../async_postgres/pg_connection/[simple_query, notify]
import ../async_postgres/pg_connection/types {.all.}

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

suite "reconnectInPlace buffer pairing":
  test "recvBufStart stays paired with recvBuf across reconnect":
    var preLen = -1
    var preStart = -1
    var finalState: PgConnState
    var finalLen = -1
    var finalStart = -1

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      proc serverHandler() {.async.} =
        # Connection 1: the original transport the test connects with.
        sc1 = await acceptAndReady(ms)
        # Connection 2: reconnectInPlace closes sc1 and dials again. The whole
        # handshake goes out in one write, so the fresh connection ends with
        # recvBufStart pointing past the consumed auth bytes (the CR-2 setup).
        sc2 = await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      # A freshly connected conn holds the consumed handshake: non-empty buffer,
      # read pointer at the end.
      preLen = conn.recvBuf.len
      preStart = conn.recvBufStart

      # No subscribed channels => the re-LISTEN loop does nothing, so only the
      # buffer/pointer copy is exercised.
      await conn.reconnectInPlace()
      finalState = conn.state
      finalLen = conn.recvBuf.len
      finalStart = conn.recvBufStart

      await serverFut
      try:
        await conn.close()
      except CatchableError:
        discard
      await closeClient(sc1)
      await closeClient(sc2)
      await closeServer(ms)

    waitFor testBody()
    # Sanity: the scenario is only meaningful if the handshake actually left
    # consumed bytes behind (otherwise recvBufStart == 0 == len trivially).
    check preLen > 0
    check preStart == preLen
    check finalState == csReady
    # The fix: recvBufStart copied alongside recvBuf. Pre-fix this is 0 while
    # finalLen > 0, so the next read would re-parse stale auth bytes and desync.
    check finalLen > 0
    check finalStart == finalLen

## Regression target (M-7): when multi-host failover reconnects to a *different*
## host, `reconnectInPlace` must copy `newConn.host`/`newConn.port` too — not just
## `pid`/`secretKey`. `cancel()` dials `conn.host`/`conn.port` with the (updated)
## backend secretKey; leaving host/port stale sends the CancelRequest to the old,
## now-dead host where the new key is unknown, so the cancel silently no-ops.
##
## We use a two-host config [A, B]. The initial connect lands on A; we then take
## A's listener down and `reconnectInPlace`, which dials A (refused) and fails
## over to B. B completes the handshake with a *distinct* pid/secretKey, so the
## connection's identity provably moved. The assertion is that host/port follow
## the transport to B. Pre-fix, port stays at A's while the transport talks to B.

proc failoverConfig(portA, portB: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1",
    port: portA,
    user: "test",
    database: "test",
    sslMode: sslDisable,
    hosts: @[
      HostEntry(host: "127.0.0.1", port: portA),
      HostEntry(host: "127.0.0.1", port: portB),
    ],
  )

suite "reconnectInPlace host/port failover":
  test "host/port follow the transport when failover picks a new host":
    const
      aPid = 1111'i32
      aSecret = 2222'i32
      bPid = 3333'i32
      bSecret = 4444'i32
    var portA, portB = -1
    var initialPort = -1
    var finalState: PgConnState
    var finalHostIsLoopback = false
    var finalPort, finalPid, finalSecret = -1

    proc testBody() {.async.} =
      let msA = startMockServer()
      let msB = startMockServer()
      portA = msA.port
      portB = msB.port
      var scA, scB: MockClient
      proc serverHandler() {.async.} =
        # Connection 1: original transport, served by host A.
        scA = await acceptAndReady(msA, pid = aPid, secretKey = aSecret)
        # Connection 2: the failover target. reconnectInPlace dials A (refused,
        # listener closed below) then B, which answers with a distinct key.
        scB = await acceptAndReady(msB, pid = bPid, secretKey = bSecret)

      let serverFut = serverHandler()
      let conn = await connect(failoverConfig(msA.port, msB.port))
      # Landed on the first host A.
      initialPort = conn.port

      # Take A down so the reconnect's dial to A is refused and failover moves
      # to B. Closing the listener leaves the already-accepted scA untouched.
      await closeServer(msA)
      await conn.reconnectInPlace()
      finalState = conn.state
      finalHostIsLoopback = conn.host == "127.0.0.1"
      finalPort = conn.port
      finalPid = conn.pid
      finalSecret = conn.secretKey

      await serverFut
      try:
        await conn.close()
      except CatchableError:
        discard
      try:
        await closeClient(scA)
      except CatchableError:
        discard
      await closeClient(scB)
      await closeServer(msB)

    waitFor testBody()
    # Sanity: the initial connect really used host A, and the two endpoints
    # differ — otherwise "failover changed the host" proves nothing.
    check initialPort == portA
    check portA != portB
    check finalState == csReady
    # pid/secretKey track the new backend (true even pre-fix); asserting them
    # pins down that the transport genuinely moved to B.
    check finalPid == bPid
    check finalSecret == bSecret
    # The fix: host/port copied from newConn, so they point at B — the host
    # cancel() will dial. Pre-fix, finalPort stays at portA (the dead host).
    check finalHostIsLoopback
    check finalPort == portB

suite "reconnectInPlace TLS field pairing":
  test "serverCertDer is copied from the fresh connection":
    ## Regression: `reconnectInPlace` must overwrite `serverCertDer` from the fresh
    ## connection. Pre-fix the stale DER survived the swap, so any post-reconnect
    ## reader (SCRAM channel binding — since #470 the tls-server-end-point hash is
    ## picked from the cert's own signatureAlgorithm) saw a cert that no longer
    ## matched the live transport.
    var preCertLen = -1
    var finalCertLen = -1

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        sc2 = await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      # Non-TLS mock leaves serverCertDer empty; seeding it distinguishes copy
      # from no-op.
      conn.serverCertDer = @[byte 0xDE, 0xAD, 0xBE, 0xEF]
      preCertLen = conn.serverCertDer.len

      await conn.reconnectInPlace()
      finalCertLen = conn.serverCertDer.len

      await serverFut
      try:
        await conn.close()
      except CatchableError:
        discard
      await closeClient(sc1)
      await closeClient(sc2)
      await closeServer(ms)

    waitFor testBody()
    check preCertLen == 4
    check finalCertLen == 0

suite "reconnectInPlace session state reset":
  ## Regression: reconnectInPlace must clear heldSessionLocks / sessionLockDirty
  ## (fresh backend never had them — else pool release fakes a lock leak) and
  ## sendBuf (symmetry with recvBuf; stale bytes must not survive the swap).
  test "heldSessionLocks / sessionLockDirty / sendBuf cleared across reconnect":
    var preHeld = -1
    var preDirty = false
    var preSendLen = -1
    var finalHeld = -1
    var finalDirty = true
    var finalSendLen = -1

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        sc2 = await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      # Seed the shape a typed advisoryLock + a partial send would leave.
      conn.heldSessionLocks = 3
      conn.sessionLockDirty = true
      conn.sendBuf = @[byte 0xAA, 0xBB, 0xCC]
      preHeld = conn.heldSessionLocks
      preDirty = conn.sessionLockDirty
      preSendLen = conn.sendBuf.len

      await conn.reconnectInPlace()
      finalHeld = conn.heldSessionLocks
      finalDirty = conn.sessionLockDirty
      finalSendLen = conn.sendBuf.len

      await serverFut
      try:
        await conn.close()
      except CatchableError:
        discard
      await closeClient(sc1)
      await closeClient(sc2)
      await closeServer(ms)

    waitFor testBody()
    # Sanity: seeding took effect (otherwise "reset to 0" proves nothing).
    check preHeld == 3
    check preDirty
    check preSendLen == 3
    check finalHeld == 0
    check not finalDirty
    check finalSendLen == 0

## Regression: `stopListening` must not hang when it races a successful in-place
## reconnect by the listen pump.
##
## When the pump loses its transport it enters the auto-reconnect loop, and the
## moment `reconnectInPlace` swaps in a live transport it used to unconditionally
## restore `csListening`. A `stopListening` issued during that window set its stop
## signal (`csBusy`) only to have it overwritten by the reconnect's csListening,
## so the pump looped straight back into the recv loop and `await listenTask`
## never returned — a permanent hang.
##
## The fix routes the stop through `listenStopRequested`, which the pump checks
## right after `reconnectInPlace` succeeds (before restoring csListening) and at
## every other reconnect-loop yield point. We reproduce the race deterministically
## with the mock server: it withholds the re-LISTEN response until stopListening
## has suspended on the pump, then answers it — so the pump observes the stop just
## as the reconnect completes. The assertion is simply that stopListening returns
## (bounded by a `wait`) and leaves the freshly reconnected connection `csReady`.

suite "stopListening during reconnect":
  test "stopListening returns and leaves csReady when it races a reconnect":
    var finalState: PgConnState
    var finalChannels = -1
    var stopReturned = false
    var finalTaskNil = false
    var finalStopRequested = true
    var finalReconnecting = true

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")
      let reListenSeen = newFuture[void]("reListenSeen")
      let stopIssued = newFuture[void]("stopIssued")

      proc serverHandler() {.async.} =
        # Connection 1: initial connect + first LISTEN.
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1) # LISTEN "x"
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        # Wait until the pump is parked reading sc1, then kill the transport so
        # the pump enters its auto-reconnect loop.
        await pumpStarted
        await closeClient(sc1)
        # Connection 2: the reconnect target. acceptAndReady completes the
        # handshake; reconnectInPlace then re-LISTENs and parks awaiting our
        # response — that read is what `drainFrontendMessage` below observes.
        sc2 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc2) # re-LISTEN "x"
        # Pump is now suspended mid-reconnect (listenReconnecting = true).
        reListenSeen.complete()
        # Hold the LISTEN response until stopListening has suspended on the pump,
        # so the stop and the reconnect's completion genuinely race.
        await stopIssued
        await sendBytes(sc2, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await conn.listen("x")
      pumpStarted.complete()

      try:
        # Wait until the pump is mid-reconnect, then stop. Pre-fix this hangs;
        # every `wait` here turns a regression into a test failure instead of a
        # stuck CI rather than relying on an unbounded await.
        await reListenSeen.wait(seconds(10))
        let stopFut = conn.stopListening()
        stopIssued.complete()
        await stopFut.wait(seconds(10))
        stopReturned = true
        finalState = conn.state
        finalChannels = conn.listenChannels.len
        # The pump must have fully exited and left no stale stop/reconnect state.
        finalTaskNil = conn.listenTask.isNil
        finalStopRequested = conn.listenStopRequested
        finalReconnecting = conn.listenReconnecting
      finally:
        # Always tear down — even if a `wait` above timed out — so a leaked mock
        # server socket can't break later tests in the suite. Unblock the server
        # handler first in case we never reached the stop.
        if not stopIssued.finished:
          stopIssued.complete()
        try:
          await serverFut.wait(seconds(10))
        except CatchableError:
          discard
        try:
          await conn.close()
        except CatchableError:
          discard
        if not sc2.isNil:
          try:
            await closeClient(sc2)
          except CatchableError:
            discard
        await closeServer(ms)

    waitFor testBody()
    # The fix: stopListening returns promptly instead of hanging, and the pump
    # exited without resuming the recv loop, leaving the reconnected connection
    # ready for reuse. Pre-fix `stopReturned` never becomes true (the await never
    # completes); the `wait` would raise first.
    check stopReturned
    check finalState == csReady
    # stopListening stops the pump but does not UNLISTEN, matching its contract.
    check finalChannels == 1
    # The pump handle is released and no stop/reconnect flag leaked — a fix that
    # left either set (e.g. a missed reset on the stop-wins path) would pass the
    # state check above but fail here.
    check finalTaskNil
    check not finalStopRequested
    check not finalReconnecting

  test "stopListening returns csClosed when it races a reconnect that cannot complete":
    # Covers the *other* stop-during-reconnect exit: the pump is mid-reconnect
    # but no live transport is ever restored, so the stop must drive it to
    # csClosed via the post-loop check — not the csReady reconnect-won path the
    # test above exercises. We kill the listener so every dial is refused, wait
    # until the pump is in its reconnect loop, then stop.
    var finalState: PgConnState
    var stopReturned = false
    var finalTaskNil = false
    var finalStopRequested = true
    var finalReconnecting = true

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")

      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1) # LISTEN "x"
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        # Wait until the pump is parked reading sc1, then kill the transport so
        # the pump enters its auto-reconnect loop.
        await pumpStarted
        await closeClient(sc1)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await conn.listen("x")
      pumpStarted.complete()
      await serverFut.wait(seconds(10))

      try:
        # Take the listener down so the pump's reconnect dials are refused
        # outright and it can never restore a live transport.
        await closeServer(ms)
        # Wait until the pump is inside its reconnect loop (it sleeps one backoff
        # interval before the first dial), so stopListening takes the
        # `listenReconnecting` branch. There is no server-side event to wait on
        # here — the reconnect dials are refused — so poll the flag, bounded so a
        # stuck pump fails rather than hangs.
        var spins = 0
        while not conn.listenReconnecting and spins < 10000:
          inc spins
          await sleepAsync(milliseconds(1))
        # Fail loudly if the window was missed: otherwise stopListening would take
        # the normal path, also land on csClosed via the dead transport, and pass
        # the assertions below without ever exercising the reconnecting branch.
        # `doAssert` (not `check`) because this runs inside an async proc, whose
        # effect signature forbids the `Exception` that `check` can raise.
        doAssert conn.listenReconnecting, "pump never entered its reconnect loop"
        let stopFut = conn.stopListening()
        await stopFut.wait(seconds(10))
        stopReturned = true
        finalState = conn.state
        finalTaskNil = conn.listenTask.isNil
        finalStopRequested = conn.listenStopRequested
        finalReconnecting = conn.listenReconnecting
      finally:
        try:
          await serverFut.wait(seconds(10))
        except CatchableError:
          discard
        try:
          await conn.close()
        except CatchableError:
          discard

    waitFor testBody()
    # The stop won the race before any reconnect completed: the pump exited via
    # the post-loop csClosed path, releasing its handle and clearing both flags.
    check stopReturned
    check finalState == csClosed
    check finalTaskNil
    check not finalStopRequested
    check not finalReconnecting

suite "listenPump async ErrorResponse":
  ## Regression: an asynchronous ErrorResponse delivered to the idle listen pump
  ## must not be silently discarded. PostgreSQL sends a FATAL ErrorResponse (e.g.
  ## 57P01 "terminating connection due to administrator command" on
  ## `pg_terminate_backend`) just before tearing down a backend. The pump's recv
  ## loop used to `discard` every message it got back, so that diagnostic was
  ## dropped on the floor; the loss was only noticed one recv later as the generic
  ## "Connection closed by server", and the server's actual reason never reached
  ## the caller.
  ##
  ## The fix raises the ErrorResponse from the recv loop so its reason drives the
  ## failure path. With channels subscribed the pump reconnects; when reconnection
  ## is then refused, the permanent-death notification must carry the original
  ## FATAL reason, not just a retry count. We reproduce it deterministically: the
  ## mock server completes the LISTEN, sends the FATAL, and closes; we then take
  ## the listener down so the single reconnect attempt is refused and assert the
  ## surfaced `PgListenError.msg` contains the server's reason.

  test "FATAL ErrorResponse reason surfaces in listen death instead of being discarded":
    var errored = false
    var reconnectionAttempted = false
    var finalState: PgConnState

    proc testBody() {.async.} =
      # Locals (not the test-scope vars) so the gcsafe `onListenError` callback
      # closes over stack memory. The captured string never escapes to a global —
      # under the chronos backend assigning a GC'd value to a module-level var
      # makes this async proc non-gcsafe — so its content is asserted in-place
      # below; only the non-GC'd bool/enum results flow out for the final checks.
      var cbErrored = false
      var cbErrMsg = ""
      var cbReconnectionAttempted = false
      let ms = startMockServer()
      var sc1: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")

      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1) # LISTEN "x"
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        # Pump is now parked in its recv loop. Deliver the async FATAL the way
        # `pg_terminate_backend` does — an ErrorResponse, then a close — and let
        # the pump observe it before the socket goes away.
        await pumpStarted
        await sendBytes(
          sc1,
          buildErrorResponse(
            "57P01", "terminating connection due to administrator command"
          ),
        )
        await closeClient(sc1)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      # One refused reconnect attempt, minimum backoff: the pump dies permanently
      # so the death notification (carrying the FATAL reason) is observable.
      conn.listenReconnectMaxAttempts = 1
      conn.listenReconnectMaxBackoff = 1
      conn.onListenError(
        proc(err: ref PgListenError) {.gcsafe, raises: [].} =
          cbErrored = true
          cbErrMsg = err.msg
          cbReconnectionAttempted = err.reconnectionAttempted
      )
      await conn.listen("x")
      pumpStarted.complete()

      # Let the server hand over the FATAL and close, then take the listener down
      # so the pump's single reconnect dial is refused → permanent death. Closing
      # well within the 1s backoff guarantees the dial finds no listener (a live
      # listener with no acceptor would instead hang the reconnect handshake).
      await serverFut.wait(seconds(10))
      await closeServer(ms)

      # Poll for the death callback (1s backoff + a refused dial), bounded so a
      # regression that never surfaces the error fails instead of hanging.
      var spins = 0
      while not cbErrored and spins < 5000:
        inc spins
        await sleepAsync(milliseconds(2))

      finalState = conn.state
      errored = cbErrored
      reconnectionAttempted = cbReconnectionAttempted

      # The fix: the previously-discarded ErrorResponse's reason now reaches the
      # caller. Pre-fix the pump swallowed it and reported only a generic
      # transport loss, so this substring was absent. `doAssert` (not `check`)
      # because this runs inside an async proc whose effect signature forbids the
      # Exception `check` can raise.
      doAssert cbErrored, "listen pump never reported permanent death"
      doAssert "terminating connection due to administrator command" in cbErrMsg,
        "FATAL reason was discarded; death message was: " & cbErrMsg
      # Layered under the reconnect-failure context: it took the reconnect path
      # (channels present) and still preserved the original cause.
      doAssert "reconnection failed" in cbErrMsg

      try:
        await conn.close()
      except CatchableError:
        discard

    waitFor testBody()
    check errored
    check reconnectionAttempted
    check finalState == csClosed

## Regression: with the listen pump parked inside a blocking `connect()`,
## asyncdispatch `close()` must return bounded instead of hanging on `await
## pump`, and `reconnectInPlace` must discard the freshly built transport rather
## than grafting it onto an already-closed `conn`.

suite "close during reconnect":
  test "close returns bounded and reconnectInPlace discards newConn on stop":
    var closeReturned = false
    var closedInBounded = false
    var stateAfterClose: PgConnState
    var taskNilAfterClose = false
    var pumpFinishedEventually = false
    var stateAfterSettle: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")
      let sc2Accepted = newFuture[void]("sc2Accepted")
      let closeCompleted = newFuture[void]("closeCompleted")

      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1) # LISTEN "x"
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        await pumpStarted
        await closeClient(sc1)
        # Accept the reconnect dial (TCP-level) but withhold the startup response
        # so the pump is parked inside connect().
        sc2 = await ms.accept()
        sc2Accepted.complete()
        # Release the handshake only after close() has finished, so the pump's
        # connect() returns into reconnectInPlace's post-connect stop-check.
        await closeCompleted
        try:
          await drainStartupMessage(sc2)
          await sendFullHandshake(sc2)
          # Answer the re-LISTEN too: if newConn were grafted, the pump reaches
          # csListening instead of hanging, so the assertion below fails loudly.
          discard await drainFrontendMessage(sc2) # re-LISTEN "x"
          await sendBytes(sc2, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        except CatchableError:
          discard

      let saved = listenReconnectStopWaitMs
      listenReconnectStopWaitMs = 300
      defer:
        listenReconnectStopWaitMs = saved

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      conn.listenReconnectMaxAttempts = 0
      conn.listenReconnectMaxBackoff = 1
      await conn.listen("x")
      pumpStarted.complete()

      await sc2Accepted.wait(seconds(10))

      # Capture the pump future before close() nils listenTask; its `finished`
      # is the direct signal of pump exit after the handshake is released.
      let pumpFut = conn.listenTask

      let t0 = getMonoTime()
      let closeFut = conn.close()
      try:
        await closeFut.wait(seconds(5))
        closeReturned = true
      except CatchableError:
        discard
      let elapsedNs = getMonoTime().ticks - t0.ticks
      closedInBounded = elapsedNs < 2_000_000_000
      stateAfterClose = conn.state
      taskNilAfterClose = conn.listenTask.isNil
      closeCompleted.complete()

      # Let the mock finish the handshake, then poll pumpFut.finished directly.
      var spins = 0
      while not pumpFut.finished and spins < 5000:
        inc spins
        await sleepAsync(milliseconds(2))
      pumpFinishedEventually = pumpFut.finished

      # Re-sample after settle: grafting newConn would move csClosed → csReady →
      # csListening; the post-connect stop-check must leave it csClosed.
      stateAfterSettle = conn.state

      try:
        await serverFut.wait(seconds(5))
      except CatchableError:
        discard
      try:
        await closeClient(sc2)
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check closeReturned
    check closedInBounded
    check stateAfterClose == csClosed
    check taskNilAfterClose
    check pumpFinishedEventually
    # Core assertion: newConn was discarded, not grafted (else csListening).
    check stateAfterSettle == csClosed

  test "stopListening timeout raises PgTimeoutError, marks csClosed, preserves flag":
    # Same mock shape as the close test above, but stopping via stopListening():
    # the bounded wait must raise PgTimeoutError, mark csClosed, nil listenTask
    # (so a follow-up close() does not re-wait the orphan), and keep
    # listenStopRequested set so the orphan's post-connect check stays armed.
    var raisedPgTimeout = false
    # Only asyncdispatch has the bounded wait; chronos exits via cancellation.
    when hasAsyncDispatch:
      var msgHasStopListening = false
      var flagPreserved = false
      var taskNilAfterRaise = false
      var stateAfterRaise: PgConnState

    proc testBody() {.async.} =
      var stopMsg = ""
      let ms = startMockServer()
      var sc1, sc2: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")
      let sc2Accepted = newFuture[void]("sc2Accepted")
      let stopReturned = newFuture[void]("stopReturned")

      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1)
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        await pumpStarted
        await closeClient(sc1)
        sc2 = await ms.accept()
        sc2Accepted.complete()
        # Release the handshake only after stopListening returned, so the pump's
        # connect() unwinds into the stop-check with the orphan already detached.
        await stopReturned
        try:
          await drainStartupMessage(sc2)
          await sendFullHandshake(sc2)
          discard await drainFrontendMessage(sc2)
          await sendBytes(sc2, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        except CatchableError:
          discard

      let saved = listenReconnectStopWaitMs
      listenReconnectStopWaitMs = 300
      defer:
        listenReconnectStopWaitMs = saved

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      conn.listenReconnectMaxAttempts = 0
      conn.listenReconnectMaxBackoff = 1
      await conn.listen("x")
      pumpStarted.complete()

      await sc2Accepted.wait(seconds(10))

      let pumpFut = conn.listenTask
      try:
        await conn.stopListening().wait(seconds(5))
      except PgTimeoutError as e:
        raisedPgTimeout = true
        stopMsg = e.msg
      except CatchableError:
        discard
      when hasAsyncDispatch:
        flagPreserved = conn.listenStopRequested
        taskNilAfterRaise = conn.listenTask.isNil
        stateAfterRaise = conn.state
        msgHasStopListening = "stopListening" in stopMsg
      stopReturned.complete()

      # Drain the orphan so tear-down doesn't leave a live future dangling.
      var spins = 0
      while not pumpFut.finished and spins < 5000:
        inc spins
        await sleepAsync(milliseconds(2))

      try:
        await serverFut.wait(seconds(5))
      except CatchableError:
        discard
      try:
        await conn.close()
      except CatchableError:
        discard
      try:
        await closeClient(sc2)
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    when hasAsyncDispatch:
      check raisedPgTimeout # PgTimeoutError is a PgConnectionError subtype
      check msgHasStopListening
      check flagPreserved
      check taskNilAfterRaise
      check stateAfterRaise == csClosed

  test "stopListening orphan-raise releases pending waitNotification and refuses new ones":
    # The orphan pump dispatches no notifications, so an in-flight waiter must be
    # failed rather than left to deadlock, and a new one refused via csClosed.
    var waiterFailed = false
    var newWaiterRejected = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")
      let sc2Accepted = newFuture[void]("sc2Accepted")
      let stopDone = newFuture[void]("stopDone")

      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1)
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        await pumpStarted
        await closeClient(sc1)
        sc2 = await ms.accept()
        sc2Accepted.complete()
        await stopDone
        try:
          await drainStartupMessage(sc2)
          await sendFullHandshake(sc2)
          discard await drainFrontendMessage(sc2)
          await sendBytes(sc2, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        except CatchableError:
          discard

      let saved = listenReconnectStopWaitMs
      listenReconnectStopWaitMs = 200
      defer:
        listenReconnectStopWaitMs = saved

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      conn.listenReconnectMaxAttempts = 0
      conn.listenReconnectMaxBackoff = 1
      await conn.listen("x")
      pumpStarted.complete()
      await sc2Accepted.wait(seconds(10))

      # Start the waiter before stopListening runs. It parks on notifyWaiter and
      # must be failed the instant orphan-raise fires (state was not csClosed
      # then — checkListenAlive gate passed already).
      let waitFut = conn.waitNotification()

      try:
        await conn.stopListening().wait(seconds(5))
      except CatchableError:
        discard

      try:
        discard await waitFut.wait(seconds(2))
      except PgError:
        waiterFailed = true
      except CatchableError:
        discard

      # A new waitNotification must be refused immediately (state=csClosed).
      try:
        discard await conn.waitNotification().wait(seconds(1))
      except PgConnectionError:
        newWaiterRejected = true
      except CatchableError:
        discard

      stopDone.complete()
      try:
        await serverFut.wait(seconds(5))
      except CatchableError:
        discard
      try:
        await conn.close()
      except CatchableError:
        discard
      try:
        await closeClient(sc2)
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    when hasAsyncDispatch:
      check waiterFailed
      check newWaiterRejected

  test "stopListening after close() orphan does not clear the flag or resurrect":
    # After close() orphans the pump, stopListening() takes the early-return path
    # (listenTask already nil'd). Clearing listenStopRequested there would rearm
    # the orphan's post-connect graft and resurrect the closed connection.
    var stateAfterStopListening: PgConnState
    var flagAfterStopListening = false
    var stateAfterSettle: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")
      let sc2Accepted = newFuture[void]("sc2Accepted")
      let handshakeReleased = newFuture[void]("handshakeReleased")

      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1)
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        await pumpStarted
        await closeClient(sc1)
        sc2 = await ms.accept()
        sc2Accepted.complete()
        await handshakeReleased
        try:
          await drainStartupMessage(sc2)
          await sendFullHandshake(sc2)
          discard await drainFrontendMessage(sc2)
          await sendBytes(sc2, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        except CatchableError:
          discard

      let saved = listenReconnectStopWaitMs
      listenReconnectStopWaitMs = 200
      defer:
        listenReconnectStopWaitMs = saved

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      conn.listenReconnectMaxAttempts = 0
      conn.listenReconnectMaxBackoff = 1
      await conn.listen("x")
      pumpStarted.complete()
      await sc2Accepted.wait(seconds(10))

      let pumpFut = conn.listenTask
      # close() orphans the pump (asyncdispatch) — flag stays set, listenTask nil'd.
      try:
        await conn.close().wait(seconds(5))
      except CatchableError:
        discard

      # stopListening on the closed connection: the early return must leave the
      # flag alone, or the unwinding connect() grafts and resurrects conn.
      try:
        await conn.stopListening().wait(seconds(5))
      except CatchableError:
        discard
      stateAfterStopListening = conn.state
      flagAfterStopListening = conn.listenStopRequested

      handshakeReleased.complete()
      var spins = 0
      while not pumpFut.finished and spins < 5000:
        inc spins
        await sleepAsync(milliseconds(2))
      stateAfterSettle = conn.state

      try:
        await serverFut.wait(seconds(5))
      except CatchableError:
        discard
      try:
        await closeClient(sc2)
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    when hasAsyncDispatch:
      check stateAfterStopListening == csClosed
      # Core assertion: the flag survives while an orphan pump is still live.
      check flagAfterStopListening
      check stateAfterSettle == csClosed

  test "stopListening racing a reconnect discards newConn on both backends":
    # Backend-independent guard for reconnectInPlace's post-connect stop-check.
    # The close()-based test above only reaches it on asyncdispatch — chronos
    # cancels the pump inside connect() instead. Here connect() completes
    # *normally* with the stop flag already set, so the check must discard
    # newConn; without it the graft would leave the connection csReady.
    var stateAfterStop: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      var sc1, sc2: MockClient
      let pumpStarted = newFuture[void]("pumpStarted")
      let sc2Accepted = newFuture[void]("sc2Accepted")
      let releaseHandshake = newFuture[void]("releaseHandshake")

      proc serverHandler() {.async.} =
        sc1 = await acceptAndReady(ms)
        discard await drainFrontendMessage(sc1) # LISTEN "x"
        await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        await pumpStarted
        await closeClient(sc1)
        # Accept the reconnect dial (TCP-level) but withhold the startup response
        # so the pump parks inside connect() until releaseHandshake fires.
        sc2 = await ms.accept()
        sc2Accepted.complete()
        await releaseHandshake
        try:
          await drainStartupMessage(sc2)
          await sendFullHandshake(sc2)
          # Answer a re-LISTEN too: if newConn were grafted the pump progresses
          # and the assertion below fails loudly. With the stop-check intact
          # newConn is already closed, so this read falls into the except.
          discard await drainFrontendMessage(sc2) # re-LISTEN "x"
          await sendBytes(sc2, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
        except CatchableError:
          discard

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      conn.listenReconnectMaxAttempts = 0
      conn.listenReconnectMaxBackoff = 1
      await conn.listen("x")
      pumpStarted.complete()

      # Pump is now parked inside connect() (TCP accepted, handshake withheld),
      # listenReconnecting = true.
      await sc2Accepted.wait(seconds(10))

      # stopListening() sets the flag synchronously before its first suspension,
      # so by the time this future is returned the flag is guaranteed set — the
      # handshake release below cannot beat it.
      let stopFut = conn.stopListening()
      releaseHandshake.complete()
      try:
        await stopFut.wait(seconds(5))
      except CatchableError:
        discard
      stateAfterStop = conn.state

      try:
        await serverFut.wait(seconds(5))
      except CatchableError:
        discard
      try:
        await closeClient(sc2)
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    # Unguarded on purpose: both backends end csClosed with the stop-check, and
    # csReady without it — chronos included, unlike the close()-based test.
    check stateAfterStop == csClosed

when hasAsyncDispatch:
  import std/asyncnet

  suite "stopListening send failure":
    ## Regression: `stopListening` whose stop-signal query fails must not leave the
    ## pump running behind a nil'd `listenTask`.
    ##
    ## On asyncdispatch `abortListenTask`'s `cancelAndWait` is a no-op, and the
    ## failed `sendMsg` only marks csClosed — the socket stays open, so the pump sits
    ## in a recv nothing will break. `stopListening` then returned csClosed with
    ## `listenTask = nil` and `listenStopRequested` cleared, leaving an untrackable
    ## pump that reconnects the moment its recv finally fails and resurrects the
    ## "closed" connection into csListening.
    ##
    ## The fix mirrors close(): close the transport to break the parked recv, wait
    ## bounded, and keep the stop flag set if the pump did not stop.
    test "failed stop query does not leave a resurrecting pump":
      var stateAfterStop: PgConnState
      var taskNilAfterStop = false
      var pumpStoppedAfterStop = false
      var flagAfterStop = false
      var stateAfterSettle: PgConnState
      var pumpFinishedAfterSettle = false
      var secondDialSeen = false

      proc testBody() {.async.} =
        let ms = startMockServer()
        var sc1, sc2: MockClient
        let stopReturned = newFuture[void]("stopReturned")

        proc serverHandler() {.async.} =
          sc1 = await acceptAndReady(ms)
          discard await drainFrontendMessage(sc1) # LISTEN "x"
          await sendBytes(sc1, buildCommandComplete("LISTEN") & buildReadyForQuery('I'))
          # Stay silent so the pump parks in a healthy recv.
          await stopReturned
          await closeClient(sc1)
          # A dial here means the pump outlived stopListening. Answer it fully so
          # a surviving pump reaches csListening and the assertions fail loudly.
          try:
            sc2 = await ms.accept()
            secondDialSeen = true
            await drainStartupMessage(sc2)
            await sendFullHandshake(sc2)
            discard await drainFrontendMessage(sc2) # re-LISTEN "x"
            await sendBytes(
              sc2, buildCommandComplete("LISTEN") & buildReadyForQuery('I')
            )
          except CatchableError:
            discard

        let saved = listenReconnectStopWaitMs
        listenReconnectStopWaitMs = 300
        defer:
          listenReconnectStopWaitMs = saved

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        conn.listenReconnectMaxAttempts = 0
        conn.listenReconnectMaxBackoff = 1
        await conn.listen("x")
        let pumpFut = conn.listenTask
        let realSock = conn.socket

        # Fail only the stop-signal query: a closed AsyncSocket rejects the write
        # while the pump stays parked in its live recv on the real socket. That
        # is the abort path — a send failure that does not stop the pump.
        let deadSock = newAsyncSocket(buffered = false)
        deadSock.close()
        conn.socket = deadSock

        try:
          await conn.stopListening().wait(seconds(5))
        except CatchableError:
          discard
        stateAfterStop = conn.state
        taskNilAfterStop = conn.listenTask.isNil
        pumpStoppedAfterStop = pumpFut.finished
        flagAfterStop = conn.listenStopRequested
        conn.socket = realSock
        stopReturned.complete()

        # Long enough for a surviving pump to fail its recv, serve one backoff
        # tick (1 s) and dial again.
        await sleepAsync(milliseconds(2500))
        stateAfterSettle = conn.state
        pumpFinishedAfterSettle = pumpFut.finished

        try:
          await serverFut.wait(seconds(2))
        except CatchableError:
          discard
        try:
          await conn.close()
        except CatchableError:
          discard
        try:
          await closeClient(sc1)
        except CatchableError:
          discard
        try:
          if sc2 != nil:
            await closeClient(sc2)
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor testBody()
      check stateAfterStop == csClosed
      check taskNilAfterStop
      # Either the pump stopped, or the flag stays armed to disarm the orphan.
      check (pumpStoppedAfterStop or flagAfterStop)
      # Core assertions: no reconnect dial, no resurrection.
      check not secondDialSeen
      check stateAfterSettle == csClosed
      check pumpFinishedAfterSettle
