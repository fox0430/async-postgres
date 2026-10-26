## Unit tests for `pg_client/copy` via the mock server.
##
## PG-less: COPY IN (text payload) and COPY OUT (data collection) over the
## simple-query protocol, a wrong-direction error, and transport failures and
## teardowns mid-COPY, which must retire the connection.

import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_client, pg_connection, pg_protocol]
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
