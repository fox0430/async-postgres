## Replication restart tests using the in-process mock server.
##
## PostgreSQL's logical walsender does not reset its streaming flags across
## ``START_REPLICATION`` queries (BUG #18754): a connection that already
## streamed ends its next logical start at once, with ReadyForQuery and no
## CopyDone. The library must retire the connection and raise
## ``PgUnavailableError`` instead of reporting a clean stop.

import std/[strutils, unittest]

import ../async_postgres/[async_backend, pg_replication]
import ../async_postgres/pg_connection {.all.}

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

# Captured by the closure-typed `ReplicationCallback`. Kept at module scope so
# the callback body can mutate state without forcing a non-gcsafe seq capture.
var callbackRan: bool
var restartRaised: bool
var restartTransient: bool
var restartMessage: string
var restartFinalState: PgConnState

proc sendEndWithoutCopyDone(client: MockClient) {.async.} =
  ## The tail a reused logical walsender produces: COPY ends with no CopyData
  ## or CopyDone. Then wait for the client to tear down.
  var burst: seq[byte]
  burst.add(buildCopyBothResponse())
  burst.add(buildCommandComplete("COPY 0"))
  burst.add(buildCommandComplete("START_REPLICATION"))
  burst.add(buildReadyForQuery('I'))
  await sendBytes(client, burst)
  while true:
    let m =
      try:
        await drainFrontendMessage(client)
      except CatchableError:
        break
    if m.msgType == 'X':
      break
  await closeClient(client)

suite "Replication: end without CopyDone":
  test "logical: ReadyForQuery without CopyDone retires the connection":
    callbackRan = false
    restartRaised = false
    restartTransient = false
    restartMessage = ""
    restartFinalState = csReady

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendEndWithoutCopyDone(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          callbackRan = true

      try:
        await conn.startReplication("test_slot", callback = cb)
      except PgUnavailableError as e:
        {.cast(gcsafe).}:
          restartRaised = true
          restartTransient = isTransientError(e)
          restartMessage = e.msg
      {.cast(gcsafe).}:
        restartFinalState = conn.state
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check restartRaised
    check restartTransient
    check restartFinalState == csClosed
    check not callbackRan
    check restartMessage.startsWith("replication: ")
    check "without CopyDone" in restartMessage

  test "server CopyDone then a close stays the connection-loss path":
    # A walsender the server stops (shutdown, pg_terminate_backend, slot drop)
    # sends CopyDone and exits without ReadyForQuery: that stays the
    # connection-loss path, not the end-without-CopyDone one.
    callbackRan = false
    restartRaised = false
    restartTransient = false
    restartMessage = ""
    restartFinalState = csReady

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        var burst: seq[byte]
        burst.add(buildCopyBothResponse())
        burst.add(buildCopyDone())
        burst.add(buildCommandComplete("START_STREAMING"))
        await sendBytes(st, burst)
        # The client mirrors the CopyDone behind its final status; read both and
        # drop the connection without ReadyForQuery.
        discard await drainFrontendMessage(st) # standby status
        discard await drainFrontendMessage(st) # client's CopyDone
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          callbackRan = true

      try:
        await conn.startReplication("test_slot", callback = cb)
      except PgUnavailableError as e:
        {.cast(gcsafe).}:
          restartRaised = true
          restartTransient = isTransientError(e)
          restartMessage = e.msg
      {.cast(gcsafe).}:
        restartFinalState = conn.state
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check restartRaised
    check restartTransient
    check restartFinalState == csClosed
    check not callbackRan
    check "without CopyDone" notin restartMessage

  test "physical: the same end retires the connection too":
    callbackRan = false
    restartRaised = false
    restartTransient = false
    restartMessage = ""
    restartFinalState = csReady

    proc testBody() {.async.} =
      let ms = startMockServer()

      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        discard await drainFrontendMessage(st) # START_REPLICATION
        await sendEndWithoutCopyDone(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))
      let cb = makeReplicationCallback:
        {.cast(gcsafe).}:
          callbackRan = true

      try:
        await conn.startPhysicalReplication(startLsn = Lsn(0x1000'u64), callback = cb)
      except PgUnavailableError as e:
        {.cast(gcsafe).}:
          restartRaised = true
          restartTransient = isTransientError(e)
          restartMessage = e.msg
      {.cast(gcsafe).}:
        restartFinalState = conn.state
      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor testBody()
    check restartRaised
    check restartTransient
    check restartFinalState == csClosed
    check not callbackRan
    check restartMessage.startsWith("physical replication: ")
    check "without CopyDone" in restartMessage
    check "logical" notin restartMessage
