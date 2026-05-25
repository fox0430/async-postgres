## Physical replication unit tests using the in-process mock server.
##
## Verifies the COPY BOTH protocol surface needed for physical replication:
## the SQL emitted by `startPhysicalReplication`, the streaming + drain
## behaviour (including the optional CommandComplete that the server sends
## on a timeline switch), the `sendCopyData` public API, the
## `timelineHistory` command, and that `connectReplication(..., rmPhysical)`
## carries `replication=true` in StartupMessage.

import std/[unittest, strutils]

import ../async_postgres/[async_backend, pg_protocol, pg_replication]
import ../async_postgres/pg_connection {.all.}

import ./mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc buildCopyBothResponse(): seq[byte] =
  var body: seq[byte]
  body.add(1'u8)
  body.addInt16(0'i16)
  buildBackendMsg('W', body)

proc buildCopyData(payload: openArray[byte]): seq[byte] =
  buildBackendMsg('d', payload)

proc buildXLogData(startLsn, walEnd, sendTime: int64, walData: seq[byte]): seq[byte] =
  var payload: seq[byte]
  payload.add(byte('w'))
  payload.addInt64(startLsn)
  payload.addInt64(walEnd)
  payload.addInt64(sendTime)
  payload.add(walData)
  buildCopyData(payload)

proc buildCopyDone(): seq[byte] =
  @[byte('c'), 0'u8, 0'u8, 0'u8, 4'u8]

proc buildRowDescriptionPair(
    n1: string, oid1: int32, sz1: int32, n2: string, oid2: int32, sz2: int32
): seq[byte] =
  ## RowDescription with two fields.
  var body: seq[byte]
  body.addInt16(2'i16)
  body.addCString(n1)
  body.addInt32(0'i32)
  body.addInt16(0'i16)
  body.addInt32(oid1)
  body.addInt16(int16(sz1))
  body.addInt32(-1'i32)
  body.addInt16(0'i16)
  body.addCString(n2)
  body.addInt32(0'i32)
  body.addInt16(0'i16)
  body.addInt32(oid2)
  body.addInt16(int16(sz2))
  body.addInt32(-1'i32)
  body.addInt16(0'i16)
  buildBackendMsg('T', body)

proc buildDataRowText(cols: openArray[string]): seq[byte] =
  ## DataRow with text-format columns. NULL is signalled by an empty string
  ## here only via length; this helper does not emit NULL.
  var body: seq[byte]
  body.addInt16(int16(cols.len))
  for c in cols:
    body.addInt32(int32(c.len))
    for ch in c:
      body.add(byte(ch))
  buildBackendMsg('D', body)

proc readStartupMessage(client: MockClient): Future[seq[byte]] {.async.} =
  ## Same as drainStartupMessage but returns the body so tests can inspect
  ## the startup parameters.
  let lenBuf = await readN(client, 4)
  let msgLen = decodeInt32(lenBuf, 0)
  if msgLen > 4:
    result = await readN(client, msgLen - 4)

proc startupContainsParam(body: seq[byte], key, value: string): bool =
  ## StartupMessage payload after the 4-byte length prefix is:
  ## int32 protocol + (cstring key + cstring value)* + terminating NUL.
  ## We just look for the key\0value\0 byte sequence after the 4-byte
  ## protocol version.
  let needle = key & "\0" & value & "\0"
  if body.len < 4:
    return false
  let hay = cast[string](body[4 ..< body.len])
  return hay.contains(needle)

# Module-scope capture vars. Writes from inside async procs go through a
# `{.cast(gcsafe).}` block (same pattern as `test_replication_keepalive.nim`)
# because chronos's strict GC-safety check otherwise rejects the closure.
var capturedSql: string = ""
var capturedReceivedKinds: seq[ReplicationMessageKind]
var capturedStartupBody: seq[byte]
var capturedRaised: bool = false
var capturedFinalState: PgConnState = csClosed
var capturedFilename: string = ""
var capturedContent: seq[byte]

suite "Physical replication: SQL building":
  proc runOneSqlTest(
      startLsn: Lsn, slotName: string, timeline: int32, expectedSql: string
  ) =
    capturedSql = ""

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        # Capture the START_REPLICATION query.
        let q = await drainFrontendMessage(st)
        if q.msgType == 'Q':
          # Query body is a cstring; strip the trailing NUL.
          var s = newString(q.body.len)
          for i, b in q.body:
            s[i] = char(b)
          if s.len > 0 and s[^1] == '\0':
            s.setLen(s.len - 1)
          {.cast(gcsafe).}:
            capturedSql = s
        var burst: seq[byte]
        burst.add(buildCopyBothResponse())
        burst.add(buildCopyDone())
        burst.add(buildReadyForQuery('I'))
        await sendBytes(st, burst)
        discard await drainFrontendMessage(st) # client's CopyDone reply
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          discard msg

      await conn.startPhysicalReplication(
        startLsn = startLsn, slotName = slotName, timeline = timeline, callback = cb
      )
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check capturedSql == expectedSql

  test "no slot + no timeline":
    let lsn = Lsn(0x1000'u64)
    runOneSqlTest(lsn, "", 0, "START_REPLICATION PHYSICAL " & $lsn)

  test "with slot, no timeline":
    let lsn = Lsn(0x1000'u64)
    runOneSqlTest(
      lsn, "test_phys", 0, "START_REPLICATION SLOT \"test_phys\" PHYSICAL " & $lsn
    )

  test "with slot + timeline":
    let lsn = Lsn(0x1000'u64)
    runOneSqlTest(
      lsn,
      "test_phys",
      7'i32,
      "START_REPLICATION SLOT \"test_phys\" PHYSICAL " & $lsn & " TIMELINE 7",
    )

  test "slot name with double-quote is escaped":
    let lsn = Lsn(0x1000'u64)
    runOneSqlTest(lsn, "a\"b", 0, "START_REPLICATION SLOT \"a\"\"b\" PHYSICAL " & $lsn)

suite "Physical replication: streaming and drain":
  test "receives XLogData and drains CopyDone -> CommandComplete -> ReadyForQuery":
    capturedReceivedKinds.setLen(0)
    capturedFinalState = csClosed

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION query
        var burst: seq[byte]
        burst.add(buildCopyBothResponse())
        burst.add(buildXLogData(0x1000'i64, 0x5000'i64, 0, @[1'u8, 2, 3]))
        burst.add(buildCopyDone())
        # Physical replication on timeline switch: CommandComplete BEFORE
        # ReadyForQuery. Verify the drain loop accepts this sequence.
        burst.add(buildCommandComplete("START_STREAMING"))
        burst.add(buildReadyForQuery('I'))
        await sendBytes(st, burst)
        discard await drainFrontendMessage(st) # client's CopyDone reply
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          capturedReceivedKinds.add(msg.kind)

      await conn.startPhysicalReplication(startLsn = Lsn(0x1000'u64), callback = cb)
      {.cast(gcsafe).}:
        capturedFinalState = conn.state
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check capturedFinalState == csReady # drain returned us to csReady
    check capturedReceivedKinds == @[rmkXLogData]

  test "errors when START_REPLICATION returns ErrorResponse":
    capturedFinalState = csClosed
    capturedRaised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst: seq[byte]
        burst.add(buildErrorResponse("XX000", "boom"))
        burst.add(buildReadyForQuery('I'))
        await sendBytes(st, burst)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          discard msg

      try:
        await conn.startPhysicalReplication(startLsn = Lsn(0x1000'u64), callback = cb)
      except PgQueryError:
        {.cast(gcsafe).}:
          capturedRaised = true
      {.cast(gcsafe).}:
        capturedFinalState = conn.state
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check capturedRaised
    check capturedFinalState == csReady

suite "sendCopyData public API":
  test "errors when connection is not in csReplicating state":
    capturedRaised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        discard await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        await conn.sendCopyData(@[byte('x')])
      except PgConnectionError:
        {.cast(gcsafe).}:
          capturedRaised = true
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check capturedRaised

suite "connectReplication mode":
  test "rmPhysical sends replication=true in StartupMessage":
    capturedStartupBody.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let client = await ms.accept()
        let body = await readStartupMessage(client)
        {.cast(gcsafe).}:
          capturedStartupBody = body
        await sendFullHandshake(client)
        # The post-handshake hstore discovery only runs for rmDatabase
        # connections (replication=database). For rmPhysical the library
        # skips it because SQL on user databases is not permitted.
        await closeClient(client)

      let serverFut = serverHandler()
      let conn = await connectReplication(mockConfig(ms.port), rmPhysical)
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check startupContainsParam(capturedStartupBody, "replication", "true")

  test "rmDatabase sends replication=database in StartupMessage":
    capturedStartupBody.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let client = await ms.accept()
        let body = await readStartupMessage(client)
        {.cast(gcsafe).}:
          capturedStartupBody = body
        await sendFullHandshake(client)
        # Drain (and ignore) any post-handshake discovery query.
        try:
          discard await wait(drainFrontendMessage(client), milliseconds(200))
          await sendBytes(
            client, buildCommandComplete("SELECT 0") & buildReadyForQuery('I')
          )
        except AsyncTimeoutError:
          discard
        await closeClient(client)

      let serverFut = serverHandler()
      let conn = await connectReplication(mockConfig(ms.port))
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check startupContainsParam(capturedStartupBody, "replication", "database")

suite "timelineHistory":
  test "parses (filename, content) result":
    capturedFilename = ""
    capturedContent.setLen(0)

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # TIMELINE_HISTORY 2
        var burst: seq[byte]
        # RowDescription: filename text(oid=25), content text(oid=25)
        burst.add(
          buildRowDescriptionPair("filename", 25'i32, -1'i32, "content", 25'i32, -1'i32)
        )
        burst.add(buildDataRowText(["00000002.history", "first line\n"]))
        burst.add(buildCommandComplete("TIMELINE_HISTORY"))
        burst.add(buildReadyForQuery('I'))
        await sendBytes(st, burst)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let info = await conn.timelineHistory(2'i32)
      {.cast(gcsafe).}:
        capturedFilename = info.filename
        capturedContent = info.content
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check capturedFilename == "00000002.history"
    check cast[string](capturedContent) == "first line\n"

  test "rejects non-positive timeline":
    capturedRaised = false

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        discard await acceptAndReady(ms)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      try:
        discard await conn.timelineHistory(0'i32)
      except ValueError:
        {.cast(gcsafe).}:
          capturedRaised = true
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check capturedRaised
