## Regression tests for the COPY IN final-phase race: the server aborts on the
## final CopyData while our CopyDone is already in flight, then (as a
## non-standard server would) replies to the stray CopyDone with a
## protocol-violation ErrorResponse + ReadyForQuery trailing the abort response.
##
## PostgreSQL itself ignores stray CopyData/CopyDone/CopyFail in idle state
## ("accept but ignore these messages, per protocol spec; we probably got here
## because a COPY failed, and the frontend is still sending data"), so a real
## server never desyncs. The mock exercises the client-side defensive drain
## that keeps the connection usable against servers that do reply, plus the
## pre-CopyDone final poll that skips CopyDone when an abort is already
## buffered, and the CopyFail path's drain of a stray CopyFail violation reply.

import std/[unittest, options, strutils]

import ../async_postgres/[async_backend, pg_protocol, pg_types]
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_client
import ../async_postgres/pg_client/core

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1",
    port: port,
    user: "test",
    database: "test",
    password: "pencil",
    sslMode: sslDisable,
  )

proc buildCopyInResponse*(): seq[byte] =
  ## CopyInResponse: format(0=text) + numCols(0).
  var body: seq[byte]
  body.add(0'u8)
  body.addInt16(0'i16)
  buildBackendMsg('G', body)

suite "COPY IN final-phase race (stray CopyDone after server abort)":
  test "copyIn drains the protocol-violation reply so the connection stays usable":
    var raised = false
    var connState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        await drainStartupMessage(st)
        await sendFullHandshake(st)
        # COPY query -> CopyInResponse
        let q = await drainFrontendMessage(st)
        doAssert q.msgType == 'Q'
        doAssert "COPY" in queryText(q.body)
        await sendBytes(st, buildCopyInResponse())
        # Final CopyData + stray CopyDone arrive together (sub-batch COPY).
        let d = await drainFrontendMessage(st)
        doAssert d.msgType == 'd'
        let c = await drainFrontendMessage(st)
        doAssert c.msgType == 'c'
        # The server aborted on the CopyData (real error), and — unlike
        # PostgreSQL, which ignores the stray CopyDone — replies to it with a
        # protocol-violation ErrorResponse + ReadyForQuery trailing the abort
        # response, coalesced as a real server would send it.
        var tail: seq[byte]
        tail.add(buildErrorResponse("22P02", "invalid input syntax"))
        tail.add(buildReadyForQuery('I'))
        tail.add(
          buildErrorResponse(
            "08P01", "unexpected message type 0x63 during COPY from stdin"
          )
        )
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        # The client must have drained the violation pair: its next query
        # arrives with no leftover errors to confuse the parser.
        let q2 = await drainFrontendMessage(st)
        doAssert q2.msgType == 'Q'
        doAssert "SELECT 42" in queryText(q2.body)
        var resp: seq[byte]
        resp.add(buildRowDescription("?column?"))
        resp.add(buildDataRowText(["42"]))
        resp.add(buildCommandComplete("SELECT 1"))
        resp.add(buildReadyForQuery('I'))
        await sendBytes(st, resp)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.copyIn("COPY t FROM STDIN", @["abc\n".toBytes()])
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "22P02" # the real COPY error, not the violation
      connState = conn.state
      # The leftover protocol-violation response must not surface here: without
      # the drain this query would raise 08P01 "unexpected message type".
      let res = await conn.simpleQuery("SELECT 42")
      doAssert res[0].rows[0][0].get().toString() == "42"
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check connState == csReady

  test "copyInStream final poll detects the abort and skips CopyDone":
    var raised = false
    var connState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        await drainStartupMessage(st)
        await sendFullHandshake(st)
        let q = await drainFrontendMessage(st)
        doAssert q.msgType == 'Q'
        doAssert "COPY" in queryText(q.body)
        await sendBytes(st, buildCopyInResponse())
        # Abort on the first CopyData batch, but only after the client has run
        # its inter-batch poll: the abort must land on the final pre-CopyDone
        # poll instead, which is the path this test exercises.
        let d = await drainFrontendMessage(st)
        doAssert d.msgType == 'd'
        await sleepAsync(milliseconds(200))
        var tail: seq[byte]
        tail.add(buildErrorResponse("23505", "duplicate key value"))
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        # CopyDone must never arrive: the abort landed on the client's final
        # pre-CopyDone poll. Discard any in-flight 'd', fail on 'c'.
        while true:
          let m = await drainFrontendMessage(st)
          if m.msgType == 'Q':
            doAssert "SELECT 42" in queryText(m.body)
            break
          doAssert m.msgType == 'd'
        var resp: seq[byte]
        resp.add(buildRowDescription("?column?"))
        resp.add(buildDataRowText(["42"]))
        resp.add(buildCommandComplete("SELECT 1"))
        resp.add(buildReadyForQuery('I'))
        await sendBytes(st, resp)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      var callCount = 0
      let bigChunk = newSeq[byte](copyBatchSize)
      let cb = makeCopyInCallback:
        inc callCount
        if callCount == 1:
          bigChunk
        elif callCount == 2:
          # Give the server's delayed abort time to land on the background read
          # before signalling EOF, so the final poll sees it and skips CopyDone.
          await sleepAsync(milliseconds(500))
          newSeq[byte]()
        else:
          newSeq[byte]()
      try:
        discard await conn.copyInStream("COPY t FROM STDIN", cb)
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "23505" # the real COPY error
      connState = conn.state
      let res = await conn.simpleQuery("SELECT 42")
      doAssert res[0].rows[0][0].get().toString() == "42"
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check connState == csReady

  test "copyInStream drains the violation reply to a stray CopyFail":
    var raised = false
    var connState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        await drainStartupMessage(st)
        await sendFullHandshake(st)
        let q = await drainFrontendMessage(st)
        doAssert q.msgType == 'Q'
        doAssert "COPY" in queryText(q.body)
        await sendBytes(st, buildCopyInResponse())
        # The callback fails, so the client sends CopyFail. The server had
        # already left copy-in mode, so the CopyFail is a stray message: a
        # non-standard backend replies with a protocol-violation ErrorResponse
        # + ReadyForQuery trailing the abort response the client drains first.
        let f = await drainFrontendMessage(st)
        doAssert f.msgType == 'f'
        var tail: seq[byte]
        tail.add(buildErrorResponse("23505", "duplicate key value"))
        tail.add(buildReadyForQuery('I'))
        tail.add(
          buildErrorResponse(
            "08P01", "unexpected message type 0x66 during COPY from stdin"
          )
        )
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        # The client must have drained the violation pair: its next query
        # arrives with no leftover errors to confuse the parser.
        let q2 = await drainFrontendMessage(st)
        doAssert q2.msgType == 'Q'
        doAssert "SELECT 42" in queryText(q2.body)
        var resp: seq[byte]
        resp.add(buildRowDescription("?column?"))
        resp.add(buildDataRowText(["42"]))
        resp.add(buildCommandComplete("SELECT 1"))
        resp.add(buildReadyForQuery('I'))
        await sendBytes(st, resp)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      var callCount = 0
      let cb = makeCopyInCallback:
        inc callCount
        if callCount == 1:
          "abc\n".toBytes()
        else:
          raise newException(CatchableError, "callback failed")
      try:
        discard await conn.copyInStream("COPY t FROM STDIN", cb)
      except CatchableError as e:
        raised = true
        doAssert "callback failed" in e.msg # the callback error, not the violation
      connState = conn.state
      # The leftover protocol-violation response must not surface here.
      let res = await conn.simpleQuery("SELECT 42")
      doAssert res[0].rows[0][0].get().toString() == "42"
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check connState == csReady

  test "copyInStream drains the protocol-violation reply so the connection stays usable":
    var raised = false
    var connState: PgConnState

    proc testBody() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await ms.accept()
        await drainStartupMessage(st)
        await sendFullHandshake(st)
        let q = await drainFrontendMessage(st)
        doAssert q.msgType == 'Q'
        doAssert "COPY" in queryText(q.body)
        await sendBytes(st, buildCopyInResponse())
        let d = await drainFrontendMessage(st)
        doAssert d.msgType == 'd'
        let c = await drainFrontendMessage(st)
        doAssert c.msgType == 'c'
        var tail: seq[byte]
        tail.add(buildErrorResponse("23505", "duplicate key value"))
        tail.add(buildReadyForQuery('I'))
        tail.add(
          buildErrorResponse(
            "08P01", "unexpected message type 0x63 during COPY from stdin"
          )
        )
        tail.add(buildReadyForQuery('I'))
        await sendBytes(st, tail)
        let q2 = await drainFrontendMessage(st)
        doAssert q2.msgType == 'Q'
        var resp: seq[byte]
        resp.add(buildRowDescription("?column?"))
        resp.add(buildDataRowText(["42"]))
        resp.add(buildCommandComplete("SELECT 1"))
        resp.add(buildReadyForQuery('I'))
        await sendBytes(st, resp)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      var callCount = 0
      let cb = makeCopyInCallback:
        inc callCount
        if callCount == 1:
          "abc\n".toBytes()
        else:
          newSeq[byte]()
      try:
        discard await conn.copyInStream("COPY t FROM STDIN", cb)
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "23505"
      connState = conn.state
      let res = await conn.simpleQuery("SELECT 42")
      doAssert res[0].rows[0][0].get().toString() == "42"
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check raised
    check connState == csReady
