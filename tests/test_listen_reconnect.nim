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

import std/[unittest]

import ../async_postgres/async_backend
import ../async_postgres/pg_connection {.all.}

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
