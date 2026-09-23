## Logical Replication example.
##
## Demonstrates PostgreSQL logical replication using the pgoutput plugin,
## including a **reconnect-and-resume** loop: `startReplication` poisons its
## connection on any error, so the consumer reconnects and resumes from the
## slot's confirmed position.
##
## Requires PostgreSQL configured with wal_level=logical (the repository's
## docker-compose.yml already sets it) and a user with REPLICATION privilege.
##
## Setup:
##   ALTER SYSTEM SET wal_level = logical;  -- then restart PostgreSQL
##   CREATE TABLE test_repl (id serial PRIMARY KEY, name text);
##   CREATE PUBLICATION test_pub FOR TABLE test_repl;
##
## Usage:
##   nim c -r examples/replication.nim

import std/[options, tables]

import pkg/async_postgres

const
  dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"
  slotName = "example_slot"
  stopAfter = 10 # committed messages to consume before stopping
  # Total backoff (~65 s) outlasts the default wal_sender_timeout (60 s), during
  # which a stale walsender may still hold the slot.
  maxRetries = 10
  maxBackoffMs = 10_000
  # Streaming this long counts as recovered and resets the retry budget.
  stableSecs = 60

proc closeQuietly(conn: PgConnection) {.async.} =
  ## Close without letting a close failure replace the error that broke `conn`.
  try:
    await conn.close()
  except CancelledError as e:
    raise e
  except CatchableError:
    discard

template withReplConn(conn, body: untyped) =
  ## Run `body` on a fresh replication connection, closing it afterwards
  ## without masking an error `body` raised.
  block:
    let conn = await connectReplication(dsn)
    var bodyErr: ref CatchableError = nil
    try:
      body
    except CatchableError as e:
      bodyErr = e
    await conn.closeQuietly()
    if bodyErr != nil:
      raise bodyErr

proc main() {.async.} =
  # A *permanent* slot (the default) survives reconnects; a temporary one
  # would vanish with its connection.
  withReplConn(conn):
    let sysInfo = await conn.identifySystem()
    echo "System ID: ", sysInfo.systemId, " timeline ", sysInfo.timeline
    try:
      let slot = await conn.createReplicationSlot(slotName, "pgoutput")
      echo "Created slot ", slotName, " at ", slot.consistentPoint
    except PgQueryError as e:
      if not e.isDuplicateObject:
        raise e
      echo "Reusing slot ", slotName

  var relations: RelationCache
  var lastCommit = InvalidLsn # for progress reports only; the slot tracks resume
  var msgCount = 0 # messages of committed transactions only
  var failures = 0 # consecutive short-lived attempts
  var done = false # set once stopReplication was issued
  var loopErr: ref CatchableError = nil

  # autoConfirm confirms each Commit once the callback returns (and keepalive
  # walEnd outside a transaction, so the slot also advances while pgoutput
  # skips unpublished ones); InvalidLsn resumes from the slot.
  while true:
    var err: ref CatchableError = nil
    var txnMsgs = 0 # uncommitted messages are re-streamed on resume
    # Set once connected, so a hanging connect is not "stable".
    var streamStart = none(Moment)
    try:
      withReplConn(replConn):
        streamStart = some(Moment.now())
        let cb = makeReplicationCallback:
          # After stopping, progress can no longer reach the server; skip what
          # is still buffered. The slot is dropped at the end of the run, so
          # these messages are never redelivered.
          if not done and msg.kind == rmkXLogData:
            let pgMsg = decodePgOutput(msg.xlogData)
            case pgMsg.kind
            of pomkRelation:
              relations[pgMsg.relation.relationId] = pgMsg.relation
              echo "Relation: ", pgMsg.relation.namespace, ".", pgMsg.relation.name
            of pomkBegin:
              echo "BEGIN xid=", pgMsg.begin.xid
            of pomkCommit:
              echo "COMMIT"
              # A redelivered transaction repeats a position already counted;
              # the uncommitted messages before it are discarded either way.
              if pgMsg.commit.endLsn > lastCommit:
                msgCount += txnMsgs + 1
              txnMsgs = 0
              lastCommit = max(lastCommit, pgMsg.commit.endLsn)
              if msgCount >= stopAfter:
                # Still reports this transaction: the final status waits for
                # this callback to return.
                await replConn.stopReplication()
                done = true
            of pomkInsert:
              let rel = relations[pgMsg.insert.relationId]
              echo "INSERT into ", rel.name, ":"
              for i, field in pgMsg.insert.newTuple:
                let colName =
                  if i < rel.columns.len:
                    rel.columns[i].name
                  else:
                    $i
                case field.kind
                of tdkNull:
                  echo "  ", colName, " = NULL"
                of tdkText:
                  echo "  ", colName, " = ", field.toString()
                else:
                  echo "  ", colName, " = <", field.kind, ">"
            of pomkUpdate:
              echo "UPDATE on ", relations[pgMsg.update.relationId].name
            of pomkDelete:
              echo "DELETE on ", relations[pgMsg.delete.relationId].name
            else:
              echo "Other: ", pgMsg.kind
            if pgMsg.kind != pomkCommit:
              inc txnMsgs

        echo "Starting replication from slot ", slotName
        echo "Insert rows into test_repl from another session to see changes..."
        await replConn.startReplication(
          slotName,
          InvalidLsn,
          options = @{"proto_version": "1", "publication_names": "test_pub"},
          callback = cb,
          autoConfirm = true,
        )
    except CatchableError as e:
      err = e

    if err of CancelledError:
      raise err

    if done:
      # The slot is dropped next, so a failed wind-down needs no retry.
      if err != nil:
        echo "Stream ended with an error after stopping: ", err.msg
      break
    if err == nil:
      # No stop was requested: retry it like a lost connection.
      err = newException(PgUnavailableError, "server ended the replication stream")

    if streamStart.isSome and Moment.now() - streamStart.get >= seconds(stableSecs):
      failures = 0
    inc failures
    # Retries connection loss, connect timeouts, shutdown/crash/startup
    # (57P01-57P03) and a stale walsender still holding the slot (55006), but
    # not a rejected login.
    if not isTransientError(err) or failures > maxRetries:
      loopErr = err
      break
    let backoffMs = min(maxBackoffMs, 500 shl (failures - 1))
    echo "Replication interrupted (",
      err.msg, "); retry ", failures, "/", maxRetries, " in ", backoffMs, " ms"
    await sleepMsAsync(backoffMs)

  if loopErr != nil:
    # Keep the slot so a restart resumes instead of skipping changes; drop it
    # with pg_drop_replication_slot() to abandon the consumer.
    echo "Giving up; slot ", slotName, " kept (last commit seen at ", lastCommit, ")"
    raise loopErr
  echo "Replication ended after ", msgCount, " messages."

  # A leftover slot retains WAL indefinitely, so a drop failure propagates.
  withReplConn(conn):
    await conn.dropReplicationSlot(slotName, wait = true)

waitFor main()
