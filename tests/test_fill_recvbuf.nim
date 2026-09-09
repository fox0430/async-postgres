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
import ../async_postgres/pg_connection/buffer_io
import ../async_postgres/pg_connection/simple_query
import ../async_postgres/pg_connection/types

import mock_pg_server

import std/importutils
privateAccess(PgConnection)

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

  test "csClosed at entry refuses to read":
    ## Contract: an asyncdispatch pump orphaned by external `wait(timeout)`
    ## can revive and call `fillRecvBuf` again after `invalidateOnTimeout`
    ## has already set csClosed. Fresh reads on a closed connection are
    ## refused with `PgConnectionError` so no orphan traffic is emitted.
    var raised = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      var serverClient: MockClient
      proc serverHandler() {.async.} =
        serverClient = await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await serverFut
      # Simulate invalidateOnTimeout having already fired.
      conn.state = csClosed
      try:
        await conn.fillRecvBuf()
      except PgConnectionError:
        raised = true

      await closeClient(serverClient)
      try:
        await conn.close()
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check raised

  when hasAsyncDispatch:
    test "exit guard discards recvInto that settles after csClosed":
      ## An orphan `recvInto` settling after csClosed was flipped must be
      ## discarded (no buffer re-extend, no pump resume). Start a no-timeout
      ## fillRecvBuf, flip csClosed while it blocks in recvInto, then deliver
      ## bytes so the read settles with n > 0.
      var raised = false
      var finalState: PgConnState
      var finalBufLen = -1
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
        preCallLen = conn.recvBuf.len
        preCallStart = conn.recvBufStart
        let fut = conn.fillRecvBuf()
        # Wait for the pre-extend to prove the read is in flight before the
        # flip; poll with a bound instead of a fixed sleep.
        var extended = false
        for _ in 0 ..< 100:
          if conn.recvBuf.len == preCallLen - preCallStart + RecvBufSize:
            extended = true
            break
          await sleepAsync(milliseconds(5))
        doAssert extended, "fillRecvBuf must pre-extend recvBuf before reading"
        conn.state = csClosed
        await sendBytes(serverClient, @[byte 0x00])
        try:
          await fut
        except PgConnectionError:
          raised = true
        finalState = conn.state
        finalBufLen = conn.recvBuf.len

        await closeClient(serverClient)
        try:
          await conn.close()
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor testBody()
      check raised
      check finalState == csClosed
      check finalBufLen == preCallLen - preCallStart

  test "recvMessage transitions to csClosed on AsyncTimeoutError":
    ## Contract: `recvMessage(timeout=...)` marks csClosed on
    ## `AsyncTimeoutError`, matching `invalidateOnTimeout`'s discipline
    ## everywhere else. Otherwise a partial-read stream would be reparsed
    ## on reuse and desync.
    var raised = false
    var stateAfter: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      var serverClient: MockClient
      proc serverHandler() {.async.} =
        serverClient = await acceptAndReady(ms)
        # Stay silent — recvMessage will hit its timeout.

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await serverFut
      try:
        discard await conn.recvMessage(timeout = milliseconds(50))
      except AsyncTimeoutError:
        raised = true
      stateAfter = conn.state

      await closeClient(serverClient)
      try:
        await conn.close()
      except CatchableError:
        discard
      await closeServer(ms)

    waitFor testBody()
    check raised
    check stateAfter == csClosed

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

  when hasChronos:
    test "fillRecvBufDetached refuses csClosed at entry (chronos)":
      ## Regression: an entrance guard must reject a fresh detached read on a
      ## csClosed connection, matching ``fillRecvBuf``. Without it a caller
      ## that starts a detached read after send failure flipped csClosed would
      ## trip ``compactRecvBuf``'s doAssert on completion.
      var raised = false

      proc testBody() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        await serverFut
        conn.state = csClosed
        try:
          await conn.fillRecvBufDetached()
        except PgConnectionError:
          raised = true

        await closeClient(serverClient)
        try:
          await conn.close()
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor testBody()
      check raised

    test "fillRecvBufDetached exit guard refuses csClosed after readOnce (chronos)":
      ## Regression: if csClosed is flipped externally (send failure) while a
      ## detached read is in flight, its completion must NOT call
      ## ``compactRecvBuf`` (which asserts on csClosed). Simulate by starting
      ## the read, flipping csClosed, then delivering bytes.
      var raised = false
      var finalState: PgConnState

      proc testBody() {.async.} =
        let ms = startMockServer()
        var serverClient: MockClient
        proc serverHandler() {.async.} =
          serverClient = await acceptAndReady(ms)

        let serverFut = serverHandler()
        let conn = await connect(mockConfig(ms.port))
        await serverFut
        let fut = conn.fillRecvBufDetached()
        # Flip csClosed before the pending readOnce settles, simulating a
        # send failure in the replication main loop after a timer wake.
        conn.state = csClosed
        # Deliver bytes so readOnce completes with n > 0.
        await sendBytes(serverClient, @[byte 0x00])
        try:
          await fut
        except PgConnectionError:
          raised = true
        finalState = conn.state

        await closeClient(serverClient)
        try:
          await conn.close()
        except CatchableError:
          discard
        await closeServer(ms)

      waitFor testBody()
      check raised
      check finalState == csClosed

suite "closed-connection contract on the transport paths":
  test "a read parked across close() is not a connection failure":
    ## Regression: an operation already suspended when `close()` ran used to
    ## surface the transport failure verbatim, so an `except PgConnectionError`
    ## reconnect loop resurrected the connection the application just shut down.
    var outcome = "not run"

    proc testBody() {.async.} =
      let ms = startMockServer()
      var serverClient: MockClient
      proc serverHandler() {.async.} =
        serverClient = await acceptAndReady(ms) # then stay silent

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await serverFut
      let fut = conn.fillRecvBuf()
      await conn.close()
      {.cast(gcsafe).}:
        try:
          await fut
          outcome = "completed"
        except CancelledError:
          outcome = "cancelled"
        except PgStateError:
          outcome = "PgStateError"
        except CatchableError as e:
          outcome = "leaked " & $e.name & ": " & e.msg

      await closeClient(serverClient)
      await closeServer(ms)

    waitFor testBody()
    # Named positively: "not PgConnectionError" would also pass for a raw
    # backend type escaping — the regression this test exists for.
    check outcome == "PgStateError"

  test "a read refused after close() reports PgStateError":
    var isStateError = false

    proc testBody() {.async.} =
      let ms = startMockServer()
      var serverClient: MockClient
      proc serverHandler() {.async.} =
        serverClient = await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      await serverFut
      await conn.close()
      try:
        await conn.fillRecvBuf()
      except PgStateError:
        isStateError = true
      except CatchableError:
        discard

      await closeClient(serverClient)
      await closeServer(ms)

    waitFor testBody()
    check isStateError
