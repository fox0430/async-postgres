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
