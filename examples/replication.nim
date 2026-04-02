## Logical Replication example.
##
## Demonstrates PostgreSQL logical replication using the pgoutput plugin.
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

proc main() {.async.} =
  let dsn = "postgresql://myuser:mypass@127.0.0.1:5432/mydb"

  # Connect with replication mode
  let replConn = await connectReplication(dsn)
  defer:
    await replConn.close()

  # Identify the system
  let sysInfo = await replConn.identifySystem()
  echo "System ID: ", sysInfo.systemId
  echo "Timeline: ", sysInfo.timeline
  echo "WAL position: ", sysInfo.xLogPos

  # Create a temporary replication slot
  let slot =
    await replConn.createReplicationSlot("example_slot", "pgoutput", temporary = true)
  echo "Created slot: ", slot.slotName, " at ", slot.consistentPoint

  # Use a separate connection to make changes
  let writerConn = await connect(dsn)
  defer:
    await writerConn.close()

  # Track relation metadata
  var relations: RelationCache

  # Start replication with pgoutput plugin
  var msgCount = 0
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

      # Acknowledge progress
      await replConn.sendStandbyStatus(msg.xlogData.endLsn)

      inc msgCount
      if msgCount >= 10:
        await replConn.stopReplication()
    of rmkPrimaryKeepalive:
      if msg.keepalive.replyRequested:
        await replConn.sendStandbyStatus(msg.keepalive.walEnd)

  echo "Starting replication from ", slot.consistentPoint
  echo "Insert rows into test_repl from another session to see changes..."

  await replConn.startReplication(
    "example_slot",
    slot.consistentPoint,
    options = @{"proto_version": "'1'", "publication_names": "'test_pub'"},
    callback = cb,
  )

  echo "Replication ended."

waitFor main()
