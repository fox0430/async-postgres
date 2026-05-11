## Direct tests for `fillRecvBuf` error-path invariants.
##
## The contract under test: when `fillRecvBuf` raises an exception other than
## `AsyncTimeoutError`, the receive buffer length must be restored (no
## RecvBufSize-sized zero tail left over from the pre-extend), and
## `conn.state` must be `csClosed`. This protects the protocol stream from
## being parsed against garbage tail bytes and ensures the pool will retire
## the connection.
##
## Regression target: prior to the fix, the chronos path of `fillRecvBuf`
## pre-extended `recvBuf` by `RecvBufSize` bytes before `readOnce`, and only
## rewound on `AsyncTimeoutError`. Other failures (EOF, cancellation,
## transport errors) left the buffer inflated and the state untouched.
##
## Buffer-length invariant: `fillRecvBuf` calls `compactRecvBuf` first, which
## drops already-consumed bytes (`recvBufStart..<recvBuf.len`) to the front
## and zeros `recvBufStart`. On a failure path the post-condition is therefore
## `recvBuf.len == preCallLen - preCallStart` and `recvBufStart == 0` —
## strictly tighter than `recvBuf.len <= preCallLen`.

import std/[unittest]

import ../async_postgres/async_backend
import ../async_postgres/pg_connection {.all.}

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

suite "fillRecvBuf invariants":
  test "EOF rewinds recvBuf and sets csClosed":
    var raised = false
    var finalState: PgConnState
    var finalBufLen = -1
    var finalBufStart = -1
    var preCallLen = -1
    var preCallStart = -1

    proc testBody() {.async.} =
      let ms = startMockServer()
      var serverClient: MockClient
      proc serverHandler() {.async.} =
        serverClient = await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await serverFut
      # Server closes its socket; client's next fillRecvBuf will see EOF.
      await closeClient(serverClient)
      preCallLen = conn.recvBuf.len
      preCallStart = conn.recvBufStart
      try:
        await conn.fillRecvBuf()
      except CatchableError:
        raised = true
      finalState = conn.state
      finalBufLen = conn.recvBuf.len
      finalBufStart = conn.recvBufStart
      try:
        await conn.close()
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check raised
    check finalState == csClosed
    # Pre-fix invariant violation (chronos): finalBufLen would be
    # (preCallLen - preCallStart) + RecvBufSize. Post-fix: exactly equal to
    # the compacted length.
    check finalBufLen == preCallLen - preCallStart
    check finalBufStart == 0

  when hasChronos:
    test "cancellation rewinds recvBuf and sets csClosed (chronos)":
      var finalState: PgConnState
      var finalBufLen = -1
      var finalBufStart = -1
      var preCallLen = -1
      var preCallStart = -1
      var cancelled = false

      proc testBody() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)
          # Stay silent; client's fillRecvBuf will hang until cancelled.

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        await serverFut
        preCallLen = conn.recvBuf.len
        preCallStart = conn.recvBufStart

        let fillFut = conn.fillRecvBuf()
        await fillFut.cancelAndWait()
        cancelled = fillFut.cancelled()
        finalState = conn.state
        finalBufLen = conn.recvBuf.len
        finalBufStart = conn.recvBufStart

        await closeClient(serverClient)
        try:
          await conn.close()
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor testBody()
      check cancelled
      check finalState == csClosed
      check finalBufLen == preCallLen - preCallStart
      check finalBufStart == 0

  test "AsyncTimeoutError leaves state untouched (caller invalidates)":
    ## Contract: on timeout, `fillRecvBuf` rewinds the buffer but does NOT
    ## set `csClosed` — the caller (typically `invalidateOnTimeout`) owns
    ## that transition. This guards the `if not (e of AsyncTimeoutError)`
    ## branch in the asyncdispatch path (and the dedicated `except
    ## AsyncTimeoutError` arm in the chronos path) against accidental
    ## regression. Server is intentionally silent so `wait(timeout)` fires.
    var raised = false
    var stateBefore: PgConnState
    var stateAfter: PgConnState
    var finalBufLen = -1
    var finalBufStart = -1
    var preCallLen = -1
    var preCallStart = -1

    proc testBody() {.async.} =
      let ms = startMockServer()
      var serverClient: MockClient
      proc serverHandler() {.async.} =
        serverClient = await acceptAndReady(ms)
        # Stay silent — fillRecvBuf will hit its timeout.

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await serverFut
      preCallLen = conn.recvBuf.len
      preCallStart = conn.recvBufStart
      stateBefore = conn.state
      try:
        await conn.fillRecvBuf(milliseconds(50))
      except AsyncTimeoutError:
        raised = true
      stateAfter = conn.state
      finalBufLen = conn.recvBuf.len
      finalBufStart = conn.recvBufStart

      await closeClient(serverClient)
      try:
        await conn.close()
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check raised
    check stateAfter == stateBefore # specifically: not forced to csClosed
    check stateAfter != csClosed
    check finalBufLen == preCallLen - preCallStart
    check finalBufStart == 0
