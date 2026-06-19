## Logical Replication example.
##
## Demonstrates PostgreSQL logical replication using the pgoutput plugin,
## including a **reconnect-and-resume** loop: `startReplication` poisons its
## connection on any mid-stream error (a callback exception, a dropped TCP
## connection, ...), so a long-running consumer must reconnect on a fresh
## connection and resume from the last LSN it confirmed durable.
##
## Requires PostgreSQL configured with wal_level=logical and a user with
## REPLICATION privilege.
##
## Setup:
##   ALTER SYSTEM SET wal_level = logical;
##   -- Restart PostgreSQL
##   CREATE TABLE test_repl (id serial PRIMARY KEY, name text);
##   CREATE PUBLICATION test_pub FOR TABLE test_repl;
##
## Usage:
##   nim c -r examples/replication.nim

import std/tables

import pkg/async_postgres

const
  dsn = "postgresql://myuser:mypass@127.0.0.1:5432/mydb"
  slotName = "example_slot"

proc main() {.async.} =
  # Use a *permanent* slot (the default) so it survives reconnects — a temporary
  # slot would vanish the moment its connection drops, defeating resume. Create
  # it once with a short-lived connection (or reuse it if the example was
  # restarted) and report the consistent point.
  block createSlot:
    let conn = await connectReplication(dsn)
    defer:
      await conn.close()
    let sysInfo = await conn.identifySystem()
    echo "System ID: ", sysInfo.systemId, " timeline ", sysInfo.timeline
    let slot =
      try:
        await conn.createReplicationSlot(slotName, "pgoutput")
      except PgQueryError as e:
        # 42710 = duplicate_object: the slot already exists from a prior run.
        if e.sqlState == "42710":
          echo "Slot already exists, reusing it"
          await conn.readReplicationSlot(slotName)
        else:
          raise e
    echo "Using slot: ", slot.slotName, " at ", slot.consistentPoint

  # The resume point. confirmFlushed / confirmedFlushLsn reset once a stream
  # ends, so we cannot read the restart position back off a poisoned connection
  # — we track the last durably-processed LSN ourselves. InvalidLsn (0/0) tells
  # the server to start from the slot's confirmed_flush_lsn on the first attempt.
  var resumeLsn = InvalidLsn
  var relations: RelationCache
  var msgCount = 0
  var done = false # set when we have consumed enough and stopped cleanly

  # Reconnect-and-resume loop. Each attempt uses a fresh connection (the previous
  # one is poisoned on any error) and resumes from `resumeLsn`.
  while not done:
    let replConn = await connectReplication(dsn)

    # Build the callback for *this* connection. It records progress both locally
    # (`resumeLsn`, used to restart) and on the connection (`confirmFlushed`,
    # which the automatic keepalive reply carries to the server to advance the
    # slot's confirmed_flush_lsn and let it recycle WAL).
    let cb = makeReplicationCallback:
      case msg.kind
      of rmkXLogData:
        let pgMsg = decodePgOutput(msg.xlogData)
        case pgMsg.kind
        of pomkRelation:
          relations[pgMsg.relation.relationId] = pgMsg.relation
          echo "Relation: ", pgMsg.relation.namespace, ".", pgMsg.relation.name
        of pomkBegin:
          echo "BEGIN xid=", pgMsg.begin.xid
        of pomkCommit:
          echo "COMMIT"
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

        # Mark the received changes durable. Use receivedEndLsn (startLsn +
        # data.len), not walEnd: walEnd is the server's current WAL position and
        # may point past what this message carries. Record it locally as the
        # resume point *and* confirm it on the connection so the auto-reply
        # advances the slot. Do this only *after* the change is durably
        # processed (here, echoed above) — until then a crash re-streams it.
        resumeLsn = msg.xlogData.receivedEndLsn
        discard replConn.confirmFlushed(msg.xlogData.receivedEndLsn)

        inc msgCount
        if msgCount >= 10:
          # Enough consumed: stop the stream cleanly so startReplication returns
          # normally (CopyDone -> ReadyForQuery) instead of being poisoned.
          done = true
          await replConn.stopReplication()
      of rmkPrimaryKeepalive:
        # autoKeepaliveReply (default) answers reply-requested keepalives for us,
        # carrying the confirmFlushed position above. No manual reply needed.
        discard

    echo "Starting replication from ", resumeLsn
    echo "Insert rows into test_repl from another session to see changes..."
    try:
      await replConn.startReplication(
        slotName,
        resumeLsn,
        options = @{"proto_version": "'1'", "publication_names": "'test_pub'"},
        callback = cb,
      )
    except CatchableError as e:
      # Mid-stream failure (callback exception, dropped connection, server
      # error, ...). The connection is already poisoned; close it and loop to
      # resume from resumeLsn on a fresh connection. A real consumer would back
      # off / cap retries here.
      echo "Replication interrupted (", e.msg, "); resuming from ", resumeLsn
    finally:
      await replConn.close()

  echo "Replication ended after ", msgCount, " messages."

  # Clean up the permanent slot (wait for it to go inactive after the stream).
  block dropSlot:
    let conn = await connectReplication(dsn)
    defer:
      await conn.close()
    await conn.dropReplicationSlot(slotName, wait = true)

waitFor main()
