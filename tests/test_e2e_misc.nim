import std/[unittest, options, tables, math, importutils, net]

import
  ../async_postgres/
    [async_backend, pg_protocol, pg_types, pg_replication, pg_client, pg_connection]

import e2e_common

privateAccess(PgConnection)

suite "E2E: Error type granularity":
  test "invalid SQL via exec raises PgQueryError with SQLSTATE":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sqlState = ""
      var detail = ""
      try:
        discard await conn.exec("INSERT INTO nonexistent_tbl VALUES (1)")
      except PgQueryError as e:
        sqlState = e.sqlState
        detail = e.detail
      doAssert sqlState == "42P01" # undefined_table
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "syntax error via query raises PgQueryError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sqlState = ""
      try:
        discard await conn.query("SELECTT 1")
      except PgQueryError as e:
        sqlState = e.sqlState
      doAssert sqlState == "42601" # syntax_error
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "PgQueryError is catchable as PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var caught = false
      try:
        discard await conn.exec("INSERT INTO nonexistent_tbl VALUES (1)")
      except PgError:
        caught = true
      doAssert caught
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "wrong password raises PgConnectionError":
    proc t() {.async.} =
      let badConfig = ConnConfig(
        host: PgHost,
        port: PgPort,
        user: PgUser,
        password: "wrong_password",
        database: PgDatabase,
        sslMode: sslDisable,
      )
      var caught = false
      try:
        let conn = await connect(badConfig)
        await conn.close()
      except PgConnectionError:
        caught = true
      except PgError:
        discard
      doAssert caught

    waitFor t()

  test "connection to bad host raises PgConnectionError":
    proc t() {.async.} =
      let badConfig = ConnConfig(
        host: "127.0.0.1",
        port: 1, # unlikely to have a PG server
        user: "test",
        password: "test",
        database: "test",
        sslMode: sslDisable,
      )
      var caught = false
      try:
        let conn = await connect(badConfig)
        await conn.close()
      except PgConnectionError:
        caught = true
      except CatchableError:
        discard
      doAssert caught

    waitFor t()

  test "PgConnectionError is catchable as PgError":
    proc t() {.async.} =
      let badConfig = ConnConfig(
        host: PgHost,
        port: PgPort,
        user: PgUser,
        password: "wrong_password",
        database: PgDatabase,
        sslMode: sslDisable,
      )
      var caught = false
      try:
        let conn = await connect(badConfig)
        await conn.close()
      except PgError:
        caught = true
      doAssert caught

    waitFor t()

  test "exec timeout raises PgTimeoutError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var caught = false
      try:
        discard await conn.exec("SELECT pg_sleep(10)", timeout = milliseconds(50))
      except PgTimeoutError:
        caught = true
      except PgError:
        discard
      doAssert caught
      # Connection should be closed after timeout
      doAssert conn.state == csClosed

    waitFor t()

  test "PgTimeoutError is catchable as PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var caught = false
      try:
        discard await conn.exec("SELECT pg_sleep(10)", timeout = milliseconds(50))
      except PgError:
        caught = true
      doAssert caught

    waitFor t()

  test "PgQueryError fields populated for constraint violation":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_err_types")
      discard await conn.exec(
        "CREATE TABLE test_err_types (id int PRIMARY KEY, val text NOT NULL)"
      )
      discard await conn.exec(
        "INSERT INTO test_err_types VALUES ($1, $2)", pgParams(1'i32, "hello")
      )

      var sqlState = ""
      var detail = ""
      try:
        discard await conn.exec(
          "INSERT INTO test_err_types VALUES ($1, $2)", pgParams(1'i32, "duplicate")
        )
      except PgQueryError as e:
        sqlState = e.sqlState
        detail = e.detail

      doAssert sqlState == "23505" # unique_violation
      doAssert detail.len > 0

      discard await conn.exec("DROP TABLE test_err_types")
      await conn.close()

    waitFor t()

suite "E2E: quoteIdentifier":
  test "simple identifier":
    doAssert quoteIdentifier("foo") == "\"foo\""

  test "identifier with double quotes":
    doAssert quoteIdentifier("foo\"bar") == "\"foo\"\"bar\""

  test "empty string":
    doAssert quoteIdentifier("") == "\"\""

  test "identifier with spaces":
    doAssert quoteIdentifier("my table") == "\"my table\""

suite "E2E: Logical Replication":
  test "identifySystem returns valid info":
    proc t() {.async.} =
      let conn = await connectReplication(plainConfig())
      let info = await conn.identifySystem()
      doAssert info.systemId.len > 0
      doAssert info.timeline >= 1
      doAssert info.xLogPos != InvalidLsn
      doAssert info.dbName == PgDatabase
      await conn.close()

    waitFor t()

  test "create and drop temporary replication slot":
    proc t() {.async.} =
      let conn = await connectReplication(plainConfig())
      let slot =
        await conn.createReplicationSlot("test_temp_slot", "pgoutput", temporary = true)
      doAssert slot.slotName == "test_temp_slot"
      doAssert slot.consistentPoint != InvalidLsn
      doAssert slot.outputPlugin == "pgoutput"

      await conn.dropReplicationSlot("test_temp_slot")
      await conn.close()

    waitFor t()

  test "stream replication and receive insert":
    proc t() {.async.} =
      let writer = await connect(plainConfig())

      # Set up test table and publication
      discard await writer.simpleQuery("DROP PUBLICATION IF EXISTS test_repl_pub")
      discard await writer.simpleQuery("DROP TABLE IF EXISTS test_repl_tbl")
      discard await writer.simpleQuery(
        "CREATE TABLE test_repl_tbl (id serial PRIMARY KEY, val text)"
      )
      discard await writer.simpleQuery(
        "CREATE PUBLICATION test_repl_pub FOR TABLE test_repl_tbl"
      )

      # Replication connection
      let replConn = await connectReplication(plainConfig())
      let slot = await replConn.createReplicationSlot(
        "test_stream_slot", "pgoutput", temporary = true
      )

      var relations: RelationCache
      var gotInsert = false
      var insertRelName = ""
      var insertVal = ""

      let cb = makeReplicationCallback:
        case msg.kind
        of rmkXLogData:
          let pgMsg = decodePgOutput(msg.xlogData)
          case pgMsg.kind
          of pomkRelation:
            relations[pgMsg.relation.relationId] = pgMsg.relation
          of pomkInsert:
            gotInsert = true
            if pgMsg.insert.relationId in relations:
              insertRelName = relations[pgMsg.insert.relationId].name
            if pgMsg.insert.newTuple.len >= 2 and
                pgMsg.insert.newTuple[1].kind == tdkText:
              insertVal = pgMsg.insert.newTuple[1].toString()
            await replConn.sendStandbyStatus(msg.xlogData.receivedEndLsn)
            await replConn.stopReplication()
          of pomkCommit:
            discard
          else:
            discard
        of rmkPrimaryKeepalive:
          # autoKeepaliveReply (default) already replied: receivedEndLsn in the
          # receive field (resets wal_sender_timeout); flush/apply track the
          # confirmFlushed position. No manual reply needed.
          discard

      # Insert a row from the writer connection after a short delay
      proc insertRow() {.async.} =
        await sleepAsync(milliseconds(200))
        discard await writer.simpleQuery(
          "INSERT INTO test_repl_tbl (val) VALUES ('hello_repl')"
        )

      let insertFut = insertRow()

      await replConn.startReplication(
        "test_stream_slot",
        slot.consistentPoint,
        options = @{"proto_version": "'1'", "publication_names": "'test_repl_pub'"},
        callback = cb,
      )

      await insertFut

      doAssert gotInsert, "Should have received an INSERT message"
      doAssert insertRelName == "test_repl_tbl"
      doAssert insertVal == "hello_repl"
      doAssert replConn.state == csReady

      await replConn.close()

      # Clean up
      discard await writer.simpleQuery("DROP PUBLICATION test_repl_pub")
      discard await writer.simpleQuery("DROP TABLE test_repl_tbl")
      await writer.close()

    waitFor t()

  test "connection state is csReady after replication ends":
    proc t() {.async.} =
      let writer = await connect(plainConfig())
      discard await writer.simpleQuery("DROP PUBLICATION IF EXISTS test_state_pub")
      discard await writer.simpleQuery("CREATE PUBLICATION test_state_pub")

      let replConn = await connectReplication(plainConfig())
      let slot = await replConn.createReplicationSlot(
        "test_state_slot", "pgoutput", temporary = true
      )

      let cb = makeReplicationCallback:
        case msg.kind
        of rmkXLogData:
          await replConn.sendStandbyStatus(msg.xlogData.receivedEndLsn)
        of rmkPrimaryKeepalive:
          # Stop immediately on first keepalive. autoKeepaliveReply (default)
          # already replied (receive = receivedEndLsn); flush/apply track the
          # confirmFlushed position.
          await replConn.stopReplication()

      await replConn.startReplication(
        "test_state_slot",
        slot.consistentPoint,
        options = @{"proto_version": "'1'", "publication_names": "'test_state_pub'"},
        callback = cb,
      )

      doAssert replConn.state == csReady

      # Connection should be reusable
      let info = await replConn.identifySystem()
      doAssert info.systemId.len > 0

      await replConn.close()

      discard await writer.simpleQuery("DROP PUBLICATION test_state_pub")
      await writer.close()

    waitFor t()

  test "unsupported pgoutput proto_version raises ValueError":
    proc t() {.async.} =
      let replConn = await connectReplication(plainConfig())

      let cb = makeReplicationCallback:
        discard

      var raised = false
      try:
        await replConn.startReplication(
          "no_such_slot", InvalidLsn, options = @{"proto_version": "'2'"}, callback = cb
        )
      except ValueError:
        raised = true
      doAssert raised, "proto_version other than 1 should raise ValueError"

      # Validation runs before checkReady / state change / wire I/O, so the
      # connection stays usable and the nonexistent slot is never referenced.
      doAssert replConn.state == csReady
      let info = await replConn.identifySystem()
      doAssert info.systemId.len > 0

      await replConn.close()

    waitFor t()

suite "E2E: Physical Replication":
  test "connectReplication(rmPhysical) + identifySystem":
    proc t() {.async.} =
      let conn = await connectReplication(plainConfig(), rmPhysical)
      let info = await conn.identifySystem()
      doAssert info.systemId.len > 0
      doAssert info.timeline >= 1
      doAssert info.xLogPos != InvalidLsn
      # Physical connections are not bound to a database; dbName should be empty.
      doAssert info.dbName == ""
      await conn.close()

    waitFor t()

  test "physical streaming receives WAL and returns to csReady":
    proc t() {.async.} =
      # Writer connection (regular SQL) to generate WAL traffic.
      let writer = await connect(plainConfig())
      discard await writer.simpleQuery("DROP TABLE IF EXISTS test_phys_repl")
      discard await writer.simpleQuery(
        "CREATE TABLE test_phys_repl (id serial PRIMARY KEY, val text)"
      )

      let replConn = await connectReplication(plainConfig(), rmPhysical)
      let info = await replConn.identifySystem()
      let startLsn = info.xLogPos

      var gotWal = false
      var byteCount = 0

      let cb = makeReplicationCallback:
        case msg.kind
        of rmkXLogData:
          if msg.xlogData.data.len > 0:
            gotWal = true
            byteCount += msg.xlogData.data.len
            await replConn.stopReplication()
        of rmkPrimaryKeepalive:
          # autoKeepaliveReply (default) handles the ACK.
          discard

      proc insertRows() {.async.} =
        await sleepAsync(milliseconds(200))
        for i in 0 ..< 5:
          discard await writer.simpleQuery(
            "INSERT INTO test_phys_repl (val) VALUES ('phys" & $i & "')"
          )

      let insertFut = insertRows()

      await replConn.startPhysicalReplication(startLsn = startLsn, callback = cb)

      await insertFut

      doAssert gotWal, "Expected at least one XLogData with WAL bytes"
      doAssert byteCount > 0
      doAssert replConn.state == csReady

      await replConn.close()

      discard await writer.simpleQuery("DROP TABLE test_phys_repl")
      await writer.close()

    waitFor t()

  test "sendCopyData requires csReplicating state":
    proc t() {.async.} =
      let conn = await connectReplication(plainConfig(), rmPhysical)
      var raised = false
      try:
        await conn.sendCopyData(@[byte('x')])
      except PgConnectionError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "physical replication slot create -> stream -> drop":
    proc t() {.async.} =
      let writer = await connect(plainConfig())
      # Ensure the slot does not exist from a prior failed run.
      discard await writer.simpleQuery(
        "SELECT pg_drop_replication_slot('test_phys_e2e') " &
          "FROM pg_replication_slots WHERE slot_name = 'test_phys_e2e'"
      )
      discard await writer.simpleQuery(
        "SELECT pg_create_physical_replication_slot('test_phys_e2e')"
      )

      let replConn = await connectReplication(plainConfig(), rmPhysical)
      let info = await replConn.identifySystem()
      let startLsn = info.xLogPos

      let cb = makeReplicationCallback:
        # Stop on the very first message of any kind to keep the test snappy.
        case msg.kind
        of rmkXLogData, rmkPrimaryKeepalive:
          await replConn.stopReplication()

      # Generate a bit of WAL so the server has something to send.
      discard await writer.simpleQuery("CHECKPOINT")
      discard await writer.simpleQuery("SELECT pg_switch_wal()")

      await replConn.startPhysicalReplication(
        startLsn = startLsn, slotName = "test_phys_e2e", callback = cb
      )
      doAssert replConn.state == csReady
      await replConn.close()

      discard
        await writer.simpleQuery("SELECT pg_drop_replication_slot('test_phys_e2e')")
      await writer.close()

    waitFor t()

# User-defined type definitions for e2e tests (macros must be at top level)
type
  TestPoint = object
    x: float64
    y: float64

  TestPerson = object
    name: string
    age: int32
    score: float64

  TestNullable = object
    name: string
    age: Option[int32]
    note: Option[string]

pgComposite(TestPoint)
pgComposite(TestPerson)
pgComposite(TestNullable)

type TestMood = enum
  tmHappy = "happy"
  tmSad = "sad"
  tmOk = "ok"

pgEnum(TestMood)

type TestPosInt = distinct int32

pgDomain(TestPosInt, int32)
