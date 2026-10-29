## Unit tests for `pg_client/copy` via the mock server.
##
## PG-less: COPY IN (text payload) and COPY OUT (data collection) over the
## simple-query protocol, a wrong-direction error, and transport failures and
## teardowns mid-COPY, which must retire the connection.

import std/[unittest, strutils]

import
  ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol, pg_types]
import ../async_postgres/pg_connection/buffer_io
from ../async_postgres/pg_client/core import copyBatchSize
from ../async_postgres/pg_connection/types import recvBufLen
when defined(posix):
  import std/posix
  when hasChronos:
    from ../async_postgres/pg_connection/types import transport
  else:
    from std/asyncnet import getFd
    from ../async_postgres/pg_connection/types import socket

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc errName(err: ref Exception): string =
  if err == nil:
    "no error"
  else:
    $err.name

proc copyInResponse(): seq[byte] =
  buildBackendMsg('G', @[0'u8, 0, 0])

proc copyOutResponse(): seq[byte] =
  buildBackendMsg('H', @[0'u8, 0, 0])

proc noticeResponse(): seq[byte] =
  result = buildErrorResponse("00000", "copy notice", "NOTICE")
  result[0] = byte('N')

when defined(posix):
  proc shutWrite(conn: PgConnection) =
    ## Make the client's next send fail (EPIPE) while its reads stay open.
    when hasChronos:
      discard posix.shutdown(SocketHandle(conn.transport.fd), SHUT_WR)
    else:
      discard posix.shutdown(conn.socket.getFd(), SHUT_WR)

proc waitClosed(conn: PgConnection) {.async.} =
  ## Until the background COPY read has seen the server hang up.
  for _ in 0 ..< 400:
    if conn.state == csClosed:
      return
    await sleepAsync(milliseconds(5))
  doAssert false, "the server's hang-up was never read"

suite "copy: mock round trip":
  test "copyIn sends rows and returns the tag":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client) # COPY ... FROM STDIN Query
        await client.sendBytes(copyInResponse())
        # Drain CopyData/CopyDone frames through the frontend CopyDone.
        while true:
          let m = await drainFrontendMessage(client)
          if m.msgType == 'c':
            break
        await client.sendBytes(buildCommandComplete("COPY 2") & buildReadyForQuery('I'))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.copyIn("COPY t FROM STDIN", "a\nb\n")
      doAssert res.commandTag == "COPY 2"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyOut collects CopyData":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client) # COPY ... TO STDOUT Query
        var reply = copyOutResponse()
        reply.add(buildCopyData(@[byte('h'), byte('i')]))
        reply.add(buildCopyDone())
        reply.add(buildCommandComplete("COPY 1"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.copyOut("COPY t TO STDOUT")
      doAssert res.data.len == 1
      doAssert res.commandTag == "COPY 1"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyOutStream streams chunks through the callback":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        var reply = copyOutResponse()
        reply.add(buildCopyData(@[byte('x')]))
        reply.add(buildCopyData(@[byte('y')]))
        reply.add(buildCopyDone())
        reply.add(buildCommandComplete("COPY 2"))
        reply.add(buildReadyForQuery('I'))
        await client.sendBytes(reply)
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var chunks: seq[seq[byte]]
      let cb = makeCopyOutCallback:
        chunks.add(data)
      let info = await conn.copyOutStream("COPY t TO STDOUT", cb)
      doAssert chunks.len == 2
      doAssert info.commandTag == "COPY 2"
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyOut of a non-COPY statement raises PgQueryError":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        await client.sendBytes(
          buildRowDescription("v") & buildDataRow("1") & buildCommandComplete(
            "SELECT 1"
          ) & buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      try:
        discard await conn.copyOut("SELECT 1")
      except PgQueryError:
        raised = true
      doAssert raised
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "a notice mid-COPY IN is consumed and the background read re-armed":
    proc t() {.async.} =
      let ms = startMockServer()
      let notice = noticeResponse()
      var noticeSent = false
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        await client.sendBytes(copyInResponse())
        while true:
          let m = await drainFrontendMessage(client)
          if m.msgType == 'c':
            break
          if not noticeSent:
            await client.sendBytes(notice)
            noticeSent = true
        await client.sendBytes(buildCommandComplete("COPY 2") & buildReadyForQuery('I'))
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var notices = 0
      conn.onNotice(
        proc(n: Notice) {.gcsafe, raises: [].} =
          inc notices
      )
      var calls = 0
      let cb = makeCopyInCallback:
        inc calls
        case calls
        of 1:
          newSeq[byte](copyBatchSize)
        of 2:
          # The background read has settled once the buffer holds exactly the
          # notice, so the next poll parses it.
          while conn.recvBufLen() != notice.len:
            await sleepAsync(milliseconds(5))
          newSeq[byte](copyBatchSize)
        else:
          # Only an inter-batch poll parses mid-COPY, so the notice went
          # through it, which found no error and resumed watching.
          doAssert notices == 1
          newSeq[byte]()
      let info = await conn.copyInStream("COPY t FROM STDIN", cb)
      doAssert info.commandTag == "COPY 2"
      doAssert conn.state == csReady
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

suite "copy: transport failure mid-COPY retires the connection":
  proc hangUpAfterCopyIn(ms: MockServer) {.async.} =
    let client = await acceptAndReady(ms)
    discard await drainFrontendMessage(client)
    await client.sendBytes(copyInResponse())
    await closeClient(client)

  test "copyInStream callback error after the server hung up":
    proc t() {.async.} =
      let ms = startMockServer()
      let serverFut = hangUpAfterCopyIn(ms)
      let conn = await connect(mockConfig(ms.port))
      let cb = makeCopyInCallback:
        await conn.waitClosed()
        raise newException(ValueError, "callback failed")
      var err: ref CatchableError
      try:
        discard await conn.copyInStream("COPY t FROM STDIN", cb)
      except CatchableError as e:
        err = e
      # The dead transport wins over the callback error: no CopyFail is tried.
      doAssert err of PgUnavailableError, errName(err)
      doAssert conn.state == csClosed
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyInStream EOF after the server hung up":
    proc t() {.async.} =
      let ms = startMockServer()
      let serverFut = hangUpAfterCopyIn(ms)
      let conn = await connect(mockConfig(ms.port))
      let cb = makeCopyInCallback:
        await conn.waitClosed()
        newSeq[byte]()
      var err: ref CatchableError
      try:
        discard await conn.copyInStream("COPY t FROM STDIN", cb)
      except CatchableError as e:
        err = e
      doAssert err of PgUnavailableError, errName(err)
      doAssert conn.state == csClosed
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyInStream server hangs up instead of answering CopyFail":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        await client.sendBytes(copyInResponse())
        let f = await drainFrontendMessage(client)
        doAssert f.msgType == 'f'
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeCopyInCallback:
        raise newException(ValueError, "callback failed")
      var err: ref CatchableError
      try:
        discard await conn.copyInStream("COPY t FROM STDIN", cb)
      except CatchableError as e:
        err = e
      # CopyFail was delivered, so the callback's error is the one surfaced.
      doAssert err of ValueError, errName(err)
      doAssert "callback failed" in err.msg
      doAssert conn.state == csClosed
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  when defined(posix):
    proc holdUntilClientCloses(ms: MockServer, reply: seq[byte]) {.async.} =
      let client = await acceptAndReady(ms)
      discard await drainFrontendMessage(client)
      await client.sendBytes(reply)
      doAssert await closedByPeer(client)
      await closeClient(client)

    test "copyIn send failure abandons the in-flight background read":
      proc t() {.async.} =
        let ms = startMockServer()
        # The notice is parsed ahead of CopyInResponse, so its callback cuts
        # the write side before any CopyData goes out.
        let serverFut = holdUntilClientCloses(ms, noticeResponse() & copyInResponse())
        let conn = await connect(mockConfig(ms.port))
        conn.onNotice(
          proc(n: Notice) {.gcsafe, raises: [].} =
            conn.shutWrite()
        )
        var err: ref CatchableError
        try:
          discard await conn.copyIn("COPY t FROM STDIN", "a\n")
        except CatchableError as e:
          err = e
        doAssert err of PgUnavailableError, errName(err)
        doAssert conn.state == csClosed
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

    test "copyInStream send failure abandons the in-flight background read":
      # A full batch fails inside the send loop; a short one on the final
      # CopyData + CopyDone flush.
      for size in [copyBatchSize, 1]:
        proc t() {.async.} =
          let ms = startMockServer()
          let serverFut = holdUntilClientCloses(ms, copyInResponse())
          let conn = await connect(mockConfig(ms.port))
          var calls = 0
          let cb = makeCopyInCallback:
            inc calls
            if calls == 1:
              conn.shutWrite()
              newSeq[byte](size)
            else:
              newSeq[byte]()
          var err: ref CatchableError
          try:
            discard await conn.copyInStream("COPY t FROM STDIN", cb)
          except CatchableError as e:
            err = e
          doAssert err of PgUnavailableError, errName(err)
          doAssert conn.state == csClosed
          await conn.close()
          await serverFut
          await ms.closeServer()

        waitFor t()

    test "copyInStream callback error with CopyFail undeliverable":
      proc t() {.async.} =
        let ms = startMockServer()
        let serverFut = holdUntilClientCloses(ms, copyInResponse())
        let conn = await connect(mockConfig(ms.port))
        let cb = makeCopyInCallback:
          conn.shutWrite()
          raise newException(ValueError, "callback failed")
        var err: ref CatchableError
        try:
          discard await conn.copyInStream("COPY t FROM STDIN", cb)
        except CatchableError as e:
          err = e
        doAssert err of PgUnavailableError, errName(err)
        doAssert conn.state == csClosed
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

    test "copyOut and copyOutStream of COPY FROM STDIN with CopyFail undeliverable":
      for streaming in [false, true]:
        proc t() {.async.} =
          let ms = startMockServer()
          let serverFut = holdUntilClientCloses(ms, noticeResponse() & copyInResponse())
          let conn = await connect(mockConfig(ms.port))
          conn.onNotice(
            proc(n: Notice) {.gcsafe, raises: [].} =
              conn.shutWrite()
          )
          let cb = makeCopyOutCallback:
            discard data
          var err: ref CatchableError
          try:
            if streaming:
              discard await conn.copyOutStream("COPY t FROM STDIN", cb)
            else:
              discard await conn.copyOut("COPY t FROM STDIN")
          except CatchableError as e:
            err = e
          doAssert err of PgUnavailableError, errName(err)
          doAssert conn.state == csClosed
          await conn.close()
          await serverFut
          await ms.closeServer()

        waitFor t()

when hasChronos:
  # asyncdispatch's `wait` cannot cancel the COPY it gave up on, so only
  # chronos unwinds these frames with a CancelledError.
  suite "copy: a timeout cancelling a COPY retires the connection":
    proc copyServer(ms: MockServer, reply: seq[byte], awaitCopyFail = false) {.async.} =
      let client = await acceptAndReady(ms)
      discard await drainFrontendMessage(client)
      await client.sendBytes(reply)
      if awaitCopyFail:
        let f = await drainFrontendMessage(client)
        doAssert f.msgType == 'f'
      # Never answer: the client's timeout ends the COPY.
      discard await closedByPeer(client, seconds(5))
      await closeClient(client)

    template expectTimedOutClosed(conn: PgConnection, body: untyped) =
      var err: ref CatchableError
      try:
        body
      except CatchableError as e:
        err = e
      doAssert err of PgTimeoutError, errName(err)
      doAssert conn.state == csClosed

    test "copyInStream callback still running":
      proc t() {.async.} =
        let ms = startMockServer()
        let serverFut = copyServer(ms, copyInResponse())
        let conn = await connect(mockConfig(ms.port))
        let cb = makeCopyInCallback:
          await sleepAsync(seconds(10))
          newSeq[byte]()
        expectTimedOutClosed(conn):
          discard await conn.copyInStream(
            "COPY t FROM STDIN", cb, timeout = milliseconds(100)
          )
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

    test "copyInStream awaiting the reply to CopyFail":
      proc t() {.async.} =
        let ms = startMockServer()
        let serverFut = copyServer(ms, copyInResponse(), awaitCopyFail = true)
        let conn = await connect(mockConfig(ms.port))
        let cb = makeCopyInCallback:
          raise newException(ValueError, "callback failed")
        expectTimedOutClosed(conn):
          discard await conn.copyInStream(
            "COPY t FROM STDIN", cb, timeout = milliseconds(100)
          )
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

    test "copyOutStream callback still running":
      proc t() {.async.} =
        let ms = startMockServer()
        let serverFut = copyServer(ms, copyOutResponse() & buildCopyData(@[byte('x')]))
        let conn = await connect(mockConfig(ms.port))
        let cb = makeCopyOutCallback:
          await sleepAsync(seconds(10))
        expectTimedOutClosed(conn):
          discard await conn.copyOutStream(
            "COPY t TO STDOUT", cb, timeout = milliseconds(100)
          )
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

    test "copyOutStream draining after a callback error":
      proc t() {.async.} =
        let ms = startMockServer()
        let serverFut = copyServer(ms, copyOutResponse() & buildCopyData(@[byte('x')]))
        let conn = await connect(mockConfig(ms.port))
        let cb = makeCopyOutCallback:
          raise newException(ValueError, "callback failed")
        expectTimedOutClosed(conn):
          discard await conn.copyOutStream(
            "COPY t TO STDOUT", cb, timeout = milliseconds(100)
          )
        await conn.close()
        await serverFut
        await ms.closeServer()

      waitFor t()

proc describeReply(
    cols: openArray[tuple[name: string, typeOid: int32, typeSize: int16]]
): seq[byte] =
  ## ParseComplete, an empty ParameterDescription, ``cols`` and ReadyForQuery.
  result = buildBackendMsg('1', @[])
  result.add(buildBackendMsg('t', @[0'u8, 0]))
  result.add(buildRowDescriptionFields(cols))
  result.add(buildReadyForQuery('T'))

proc drainDescribe(client: MockClient): Future[string] {.async.} =
  ## Read Parse, Describe and Sync, returning the Parse's SQL.
  let parse = await drainFrontendMessage(client)
  doAssert parse.msgType == 'P'
  doAssert (await drainFrontendMessage(client)).msgType == 'D'
  doAssert (await drainFrontendMessage(client)).msgType == 'S'
  return decodeCString(parse.body, decodeCString(parse.body, 0)[1])[0]

proc answerTx(client: MockClient, sql: string, status: char) {.async.} =
  ## Read the simple query ``sql`` and complete it, leaving ``status``.
  let q = await drainFrontendMessage(client)
  doAssert q.msgType == 'Q'
  doAssert queryText(q.body) == sql, queryText(q.body)
  await client.sendBytes(buildCommandComplete(sql) & buildReadyForQuery(status))

suite "copy: copyInRows":
  test "describes the target, then streams the binary rows":
    proc t() {.async.} =
      let ms = startMockServer()
      var describeSql, copySql: string
      var stream: seq[byte]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        describeSql = await drainDescribe(client)
        await client.sendBytes(
          describeReply(@[("name", 25'i32, -1'i16), ("n", 23'i32, 4'i16)])
        )
        copySql = queryText((await drainFrontendMessage(client)).body)
        await client.sendBytes(copyInResponse())
        while true:
          let m = await drainFrontendMessage(client)
          if m.msgType == 'c':
            break
          doAssert m.msgType == 'd'
          stream.add(m.body)
        await client.sendBytes(buildCommandComplete("COPY 1") & buildReadyForQuery('T'))
        await client.answerTx("COMMIT", 'I')
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.copyInRows("t", @[(n: 7, name: "a")], schema = "s")
      doAssert res.affectedRows == 1
      doAssert describeSql == "SELECT * FROM \"s\".\"t\""
      doAssert copySql == "COPY \"s\".\"t\" (\"n\", \"name\") FROM STDIN (FORMAT binary)"
      var want: seq[byte]
      want.addCopyBinaryHeader()
      want.addCopyTupleStart(2)
      want.addCopyFieldInt32(7)
      want.addCopyFieldString("a")
      want.addCopyBinaryTrailer()
      doAssert stream == want
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "a field its column cannot take fails before the COPY":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        doAssert (await drainDescribe(client)) == "SELECT \"n\" FROM \"t\""
        await client.sendBytes(describeReply(@[("n", 25'i32, -1'i16)]))
        # A ROLLBACK, not a COPY.
        await client.answerTx("ROLLBACK", 'I')
        doAssert (await drainFrontendMessage(client)).msgType == 'X'
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var err: ref PgTypeError
      try:
        discard await conn.copyInRows("t", @[(1,)], columns = ["n"])
      except PgTypeError as e:
        err = e
      doAssert err != nil
      doAssert "int cannot be written to a column of type OID 25" in err.msg, err.msg
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "rows without field names need columns, checked before any message":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        doAssert (await drainFrontendMessage(client)).msgType == 'X'
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var err: ref PgTypeError
      try:
        discard await conn.copyInRows("t", @[(1, "a")])
      except PgTypeError as e:
        err = e
      doAssert err != nil
      doAssert "rows without field names need the columns named" in err.msg
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "a value that cannot be written sends CopyFail and keeps the connection":
    proc t() {.async.} =
      let ms = startMockServer()
      var failMsg = ""
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        discard await drainDescribe(client)
        await client.sendBytes(describeReply(@[("n", 21'i32, 2'i16)]))
        discard await drainFrontendMessage(client) # COPY ... FROM STDIN
        await client.sendBytes(copyInResponse())
        while true:
          let m = await drainFrontendMessage(client)
          if m.msgType == 'f':
            failMsg = decodeCString(m.body, 0)[0]
            break
          doAssert m.msgType == 'd'
        await client.sendBytes(
          buildErrorResponse("57014", "COPY from stdin failed") & buildReadyForQuery(
            'E'
          )
        )
        await client.answerTx("ROLLBACK", 'I')
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var err: ref PgTypeError
      try:
        discard await conn.copyInRows("t", @[(n: 1), (n: 40_000)])
      except PgTypeError as e:
        err = e
      doAssert err != nil
      const want =
        "row 1: tuple[n: int].n: column \"n\": value out of range for smallint"
      doAssert want in err.msg, err.msg
      doAssert failMsg.startsWith(want), failMsg
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "inside the caller's transaction, no BEGIN or COMMIT of its own":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        discard await drainDescribe(client)
        await client.sendBytes(describeReply(@[("n", 23'i32, 4'i16)]))
        doAssert (await drainFrontendMessage(client)).msgType == 'Q'
        await client.sendBytes(copyInResponse())
        while (await drainFrontendMessage(client)).msgType != 'c':
          discard
        await client.sendBytes(buildCommandComplete("COPY 1") & buildReadyForQuery('T'))
        # The next message is the client's Terminate, not a COMMIT.
        doAssert (await drainFrontendMessage(client)).msgType == 'X'
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      discard await conn.simpleExec("BEGIN")
      let res = await conn.copyInRows("t", @[(n: 1)])
      doAssert res.affectedRows == 1
      doAssert conn.txStatus == tsInTransaction
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "copyInRowsStream pulls batches only once the COPY has started":
    proc t() {.async.} =
      let ms = startMockServer()
      var calls = 0
      var callsAtDescribe = -1
      var stream: seq[byte]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        discard await drainDescribe(client)
        callsAtDescribe = calls
        await client.sendBytes(describeReply(@[("n", 20'i32, 8'i16)]))
        discard await drainFrontendMessage(client) # COPY ... FROM STDIN
        await client.sendBytes(copyInResponse())
        while true:
          let m = await drainFrontendMessage(client)
          if m.msgType == 'c':
            break
          stream.add(m.body)
        await client.sendBytes(buildCommandComplete("COPY 3") & buildReadyForQuery('T'))
        await client.answerTx("COMMIT", 'I')
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeCopyInRowsCallback(tuple[n: int64]):
        inc calls
        case calls
        of 1:
          @[(n: 1'i64), (n: 2'i64)]
        of 2:
          @[(n: 3'i64)]
        else:
          newSeq[tuple[n: int64]]()
      let res = await conn.copyInRowsStream("t", cb)
      doAssert res.affectedRows == 3
      doAssert callsAtDescribe == 0
      doAssert calls == 3
      var want: seq[byte]
      want.addCopyBinaryHeader()
      for n in 1'i64 .. 3'i64:
        want.addCopyTupleStart(1)
        want.addCopyFieldInt64(n)
      want.addCopyBinaryTrailer()
      doAssert stream == want
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

when hasChronos:
  # Each stall lands the timeout inside the transaction `copyInRows` opened, so
  # a connection handed back would still be in it.
  suite "copy: a timeout inside copyInRows' own transaction retires the connection":
    proc stallingServer(ms: MockServer, stage: string) {.async.} =
      ## Answer each request up to ``stage``'s, then never again.
      let client = await acceptAndReady(ms)
      block answered:
        await client.answerTx("BEGIN", 'T')
        discard await drainDescribe(client)
        if stage == "describe":
          break answered
        await client.sendBytes(describeReply(@[("n", 23'i32, 4'i16)]))
        doAssert (await drainFrontendMessage(client)).msgType == 'Q'
        await client.sendBytes(copyInResponse())
        while (await drainFrontendMessage(client)).msgType != 'c':
          discard
        if stage == "copy":
          break answered
        await client.sendBytes(buildCommandComplete("COPY 1") & buildReadyForQuery('T'))
        doAssert queryText((await drainFrontendMessage(client)).body) == "COMMIT"
      discard await closedByPeer(client, seconds(5))
      await closeClient(client)

    proc expectRetired(stage: string) {.async.} =
      let ms = startMockServer()
      let serverFut = stallingServer(ms, stage)
      let conn = await connect(mockConfig(ms.port))
      var err: ref CatchableError
      try:
        discard await conn.copyInRows("t", @[(n: 1)], timeout = milliseconds(100))
      except CatchableError as e:
        err = e
      doAssert err of PgTimeoutError, errName(err)
      doAssert conn.state == csClosed
      await conn.close()
      await serverFut
      await ms.closeServer()

    test "awaiting the Describe":
      waitFor expectRetired("describe")

    test "awaiting the COPY's completion":
      waitFor expectRetired("copy")

    test "awaiting the COMMIT":
      waitFor expectRetired("commit")

type CopyLevel = enum
  clLow = "low"
  clHigh = "high"

proc enumReply(oids: openArray[string]): seq[byte] =
  ## The pg_type lookup's reply: the OIDs that are enums.
  result = buildRowDescription("oid")
  for oid in oids:
    result.add(buildDataRow(oid))
  result.add(buildCommandComplete("SELECT " & $oids.len))
  result.add(buildReadyForQuery('T'))

suite "copy: copyInRows enum check":
  test "an enum label to a user type is copied once pg_type says it is an enum":
    proc t() {.async.} =
      let ms = startMockServer()
      var lookupSql, copySql: string
      var stream: seq[byte]
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        discard await drainDescribe(client)
        await client.sendBytes(describeReply(@[("level", 16400'i32, 4'i16)]))
        lookupSql = queryText((await drainFrontendMessage(client)).body)
        await client.sendBytes(enumReply(["16400"]))
        copySql = queryText((await drainFrontendMessage(client)).body)
        await client.sendBytes(copyInResponse())
        while true:
          let m = await drainFrontendMessage(client)
          if m.msgType == 'c':
            break
          stream.add(m.body)
        await client.sendBytes(buildCommandComplete("COPY 1") & buildReadyForQuery('T'))
        await client.answerTx("COMMIT", 'I')
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      var events: seq[string]
      let tracer = PgTracer()
      tracer.onPrepareStart = proc(
          conn: PgConnection, data: TracePrepareStartData
      ): TraceContext {.gcsafe, raises: [].} =
        events.add("prepare " & data.name & ": " & data.sql)
      tracer.onQueryStart = proc(
          conn: PgConnection, data: TraceQueryStartData
      ): TraceContext {.gcsafe, raises: [].} =
        events.add("query " & data.sql.split(' ')[0])
      tracer.onCopyStart = proc(
          conn: PgConnection, data: TraceCopyStartData
      ): TraceContext {.gcsafe, raises: [].} =
        events.add("copy")
      var cfg = mockConfig(ms.port)
      cfg.tracer = tracer
      let conn = await connect(cfg)
      let res = await conn.copyInRows("t", @[(level: clHigh)])
      doAssert res.affectedRows == 1
      doAssert "typtype = 'e'" in lookupSql and "'{16400}'" in lookupSql, lookupSql
      doAssert copySql == "COPY \"t\" (\"level\") FROM STDIN (FORMAT binary)"
      doAssert events ==
        @[
          "query BEGIN", "prepare : SELECT * FROM \"t\"", "query SELECT", "copy",
          "query COMMIT",
        ], $events
      var want: seq[byte]
      want.addCopyBinaryHeader()
      want.addCopyTupleStart(1)
      want.addCopyFieldString("high")
      want.addCopyBinaryTrailer()
      doAssert stream == want
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "an enum OID past 2^31 is matched through its int4 form":
    proc t() {.async.} =
      let ms = startMockServer()
      let big = cast[int32](2_500_000_000'u32)
      var lookupSql: string
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        discard await drainDescribe(client)
        await client.sendBytes(describeReply(@[("level", big, 4'i16)]))
        lookupSql = queryText((await drainFrontendMessage(client)).body)
        await client.sendBytes(enumReply([$big]))
        doAssert (await drainFrontendMessage(client)).msgType == 'Q'
        await client.sendBytes(copyInResponse())
        while (await drainFrontendMessage(client)).msgType != 'c':
          discard
        await client.sendBytes(buildCommandComplete("COPY 1") & buildReadyForQuery('T'))
        await client.answerTx("COMMIT", 'I')
        discard await drainFrontendMessage(client)
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let res = await conn.copyInRows("t", @[(level: clHigh)])
      doAssert res.affectedRows == 1
      doAssert "oid::pg_catalog.int4" in lookupSql and "'{2500000000}'" in lookupSql,
        lookupSql
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "a user type that is not an enum fails before the COPY":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        await client.answerTx("BEGIN", 'T')
        discard await drainDescribe(client)
        await client.sendBytes(describeReply(@[("level", 16400'i32, -1'i16)]))
        doAssert (await drainFrontendMessage(client)).msgType == 'Q'
        await client.sendBytes(enumReply([]))
        # A ROLLBACK, not a COPY.
        await client.answerTx("ROLLBACK", 'I')
        doAssert (await drainFrontendMessage(client)).msgType == 'X'
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var err: ref PgTypeError
      try:
        discard await conn.copyInRows("t", @[(level: clLow)])
      except PgTypeError as e:
        err = e
      doAssert err != nil
      doAssert "column \"level\": CopyLevel cannot be written to a column of type " &
        "OID 16400, which is not an enum" in err.msg, err.msg
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "a column named twice fails before any message":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        doAssert (await drainFrontendMessage(client)).msgType == 'X'
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var err: ref PgTypeError
      try:
        discard await conn.copyInRows("t", @[(a: 1, b: 2)], columns = ["a", "b", "a"])
      except PgTypeError as e:
        err = e
      doAssert err != nil
      doAssert "copyInRows: column \"a\" is named twice" in err.msg, err.msg
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()
