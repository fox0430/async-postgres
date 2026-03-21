import std/[unittest, options, strutils, tables, os, math, deques, sets, importutils]
from std/times import
  DateTime, dateTime, mMar, mJun, mJan, utc, year, month, monthday, hour, minute,
  second, toTime, toUnix, nanosecond

import ../async_postgres/[async_backend, pg_protocol, pg_types]

import ../async_postgres/pg_client {.all.}
import ../async_postgres/pg_pool {.all.}
import ../async_postgres/pg_connection {.all.}

privateAccess(PgConnection)

const
  PgHost = "127.0.0.1"
  PgPort = 15432
  PgUser = "test"
  PgPassword = "test"
  PgDatabase = "test"

proc plainConfig(): ConnConfig =
  ConnConfig(
    host: PgHost,
    port: PgPort,
    user: PgUser,
    password: PgPassword,
    database: PgDatabase,
    sslMode: sslDisable,
  )

proc sslConfig(mode: SslMode = sslRequire): ConnConfig =
  ConnConfig(
    host: PgHost,
    port: PgPort,
    user: PgUser,
    password: PgPassword,
    database: PgDatabase,
    sslMode: mode,
  )

proc loadCaCert(): string =
  let certsDir = currentSourcePath().parentDir / "certs"
  readFile(certsDir / "ca.crt")

proc loadWrongCaCert(): string =
  let certsDir = currentSourcePath().parentDir / "certs"
  readFile(certsDir / "wrong_ca.crt")

proc toBytes(s: string): seq[byte] =
  @(s.toOpenArrayByte(0, s.high))

proc toString(b: seq[byte]): string =
  result = newString(b.len)
  if b.len > 0:
    copyMem(addr result[0], unsafeAddr b[0], b.len)

suite "E2E: Basic Connection":
  test "plain connection and close":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == false
      await conn.close()
      doAssert conn.state == csClosed

    waitFor t()

  test "connect with DSN string":
    proc t() {.async.} =
      let dsn =
        "postgresql://" & PgUser & ":" & PgPassword & "@" & PgHost & ":" & $PgPort & "/" &
        PgDatabase & "?sslmode=disable"
      let conn = await connect(dsn)
      doAssert conn.state == csReady
      let res = await conn.simpleQuery("SELECT 1")
      doAssert res[0].rows[0][0].get().toString() == "1"
      await conn.close()
      doAssert conn.state == csClosed

    waitFor t()

  test "connect with keyword=value DSN string":
    proc t() {.async.} =
      let dsn =
        "host=" & PgHost & " port=" & $PgPort & " user=" & PgUser & " password=" &
        PgPassword & " dbname=" & PgDatabase & " sslmode=disable"
      let conn = await connect(dsn)
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "server parameters available":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.serverParams.hasKey("server_version")
      doAssert conn.serverParams.hasKey("server_encoding")
      await conn.close()

    waitFor t()

suite "E2E: ConnConfig Options":
  test "applicationName is sent to server":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.applicationName = "chronos-pg-test"
      let conn = await connect(cfg)
      let res = await conn.simpleQuery("SHOW application_name")
      doAssert res[0].rows[0][0].get().toString() == "chronos-pg-test"
      await conn.close()

    waitFor t()

  test "extraParams are sent to server":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.extraParams = @[("application_name", "from-extra")]
      let conn = await connect(cfg)
      let res = await conn.simpleQuery("SHOW application_name")
      doAssert res[0].rows[0][0].get().toString() == "from-extra"
      await conn.close()

    waitFor t()

  test "connectTimeout raises on unreachable host":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.host = "192.0.2.1" # TEST-NET, non-routable
      cfg.connectTimeout = milliseconds(200)
      var raised = false
      try:
        let conn = await connect(cfg)
        await conn.close()
      except AsyncTimeoutError:
        raised = true
      doAssert raised

    waitFor t()

  test "connectTimeout does not interfere with normal connection":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.connectTimeout = seconds(10)
      let conn = await connect(cfg)
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

suite "E2E: Simple Query Protocol":
  test "SELECT 1":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("SELECT 1 AS num")
      doAssert results.len == 1
      doAssert results[0].fields.len == 1
      doAssert results[0].fields[0].name == "num"
      doAssert results[0].rows.len == 1
      doAssert results[0].rows[0][0].get().toString() == "1"
      await conn.close()

    waitFor t()

  test "multiple rows with generate_series":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("SELECT generate_series(1,3)")
      doAssert results.len == 1
      doAssert results[0].rows.len == 3
      doAssert results[0].rows[0][0].get().toString() == "1"
      doAssert results[0].rows[1][0].get().toString() == "2"
      doAssert results[0].rows[2][0].get().toString() == "3"
      await conn.close()

    waitFor t()

  test "empty query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("")
      doAssert results.len == 1
      doAssert results[0].fields.len == 0
      doAssert results[0].rows.len == 0
      doAssert results[0].commandTag == ""
      await conn.close()

    waitFor t()

  test "invalid SQL raises PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.simpleQuery("INVALID SQL STATEMENT")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "invalid SQL raises PgQueryError with SQLSTATE":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sqlState = ""
      var severity = ""
      try:
        discard await conn.simpleQuery("SELECT FROM nonexistent_table_xyz")
      except PgQueryError as e:
        sqlState = e.sqlState
        severity = e.severity
      doAssert sqlState.len == 5, "expected 5-char SQLSTATE, got: " & sqlState
      doAssert severity == "ERROR"
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

suite "E2E: Extended Query Protocol":
  test "exec CREATE TABLE and INSERT":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_e2e")
      let createTag = await conn.exec(
        "CREATE TABLE test_e2e (id serial PRIMARY KEY, name text NOT NULL)"
      )
      doAssert "CREATE TABLE" in createTag
      let insertTag = await conn.exec(
        "INSERT INTO test_e2e (name) VALUES ($1)", @[some("alice".toBytes())]
      )
      doAssert "INSERT" in insertTag
      discard await conn.exec("DROP TABLE test_e2e")
      await conn.close()

    waitFor t()

  test "query with parameters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_e2e_q")
      discard
        await conn.exec("CREATE TABLE test_e2e_q (id serial PRIMARY KEY, name text)")
      discard await conn.exec(
        "INSERT INTO test_e2e_q (name) VALUES ($1), ($2)",
        @[some("alice".toBytes()), some("bob".toBytes())],
      )
      let res = await conn.query(
        "SELECT name FROM test_e2e_q WHERE name = $1", @[some("bob".toBytes())]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0][0].get().toString() == "bob"
      discard await conn.exec("DROP TABLE test_e2e_q")
      await conn.close()

    waitFor t()

  test "prepare, execute, and close statement":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_e2e_ps")
      discard
        await conn.exec("CREATE TABLE test_e2e_ps (id serial PRIMARY KEY, val text)")
      discard await conn.exec(
        "INSERT INTO test_e2e_ps (val) VALUES ($1), ($2)",
        @[some("x".toBytes()), some("y".toBytes())],
      )

      let stmt =
        await conn.prepare("my_stmt", "SELECT val FROM test_e2e_ps WHERE val = $1")
      doAssert stmt.name == "my_stmt"
      doAssert stmt.fields.len == 1
      doAssert stmt.paramOids.len == 1

      let res = await stmt.execute(@[some("x".toBytes())])
      doAssert res.rows.len == 1
      doAssert res.rows[0][0].get().toString() == "x"

      await stmt.close()

      # Connection still usable after statement close
      let r2 = await conn.query("SELECT 1 AS check_col")
      doAssert r2.rows.len == 1

      discard await conn.exec("DROP TABLE test_e2e_ps")
      await conn.close()

    waitFor t()

suite "E2E: SSL Connection":
  test "sslRequire connects with SSL":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslRequire))
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslPrefer connects with SSL when server supports it":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslPrefer))
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslDisable connects without SSL":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslDisable))
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == false
      await conn.close()

    waitFor t()

  test "query over SSL connection":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslRequire))
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

suite "E2E: SSL Verification":
  test "sslVerifyCa connects with CA verification":
    proc t() {.async.} =
      let conn = await connect(
        ConnConfig(
          host: PgHost,
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyCa,
          sslRootCert: loadCaCert(),
        )
      )
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslVerifyFull connects with full verification":
    proc t() {.async.} =
      # Use localhost (matches SAN DNS:localhost in server cert)
      let conn = await connect(
        ConnConfig(
          host: "localhost",
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyFull,
          sslRootCert: loadCaCert(),
        )
      )
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslVerifyCa fails with wrong CA":
    proc t() {.async.} =
      var raised = false
      try:
        let conn = await connect(
          ConnConfig(
            host: PgHost,
            port: PgPort,
            user: PgUser,
            password: PgPassword,
            database: PgDatabase,
            sslMode: sslVerifyCa,
            sslRootCert: loadWrongCaCert(),
          )
        )
        await conn.close()
      except CatchableError:
        raised = true
      doAssert raised

    waitFor t()

  test "sslVerifyFull fails with wrong CA":
    proc t() {.async.} =
      var raised = false
      try:
        let conn = await connect(
          ConnConfig(
            host: "localhost",
            port: PgPort,
            user: PgUser,
            password: PgPassword,
            database: PgDatabase,
            sslMode: sslVerifyFull,
            sslRootCert: loadWrongCaCert(),
          )
        )
        await conn.close()
      except CatchableError:
        raised = true
      doAssert raised

    waitFor t()

  test "query over sslVerifyCa connection":
    proc t() {.async.} =
      let conn = await connect(
        ConnConfig(
          host: PgHost,
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyCa,
          sslRootCert: loadCaCert(),
        )
      )
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

  test "query over sslVerifyFull connection":
    proc t() {.async.} =
      let conn = await connect(
        ConnConfig(
          host: "localhost",
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyFull,
          sslRootCert: loadCaCert(),
        )
      )
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

suite "E2E: Authentication":
  test "valid credentials succeed":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "wrong password raises PgError":
    proc t() {.async.} =
      let badConfig = ConnConfig(
        host: PgHost,
        port: PgPort,
        user: PgUser,
        password: "wrong_password",
        database: PgDatabase,
        sslMode: sslDisable,
      )
      var raised = false
      try:
        let conn = await connect(badConfig)
        await conn.close()
      except PgError:
        raised = true
      doAssert raised

    waitFor t()

suite "E2E: Connection Pool":
  test "basic acquire and release":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let conn = await pool.acquire()
      doAssert conn.state == csReady
      pool.release(conn)
      await pool.close()

    waitFor t()

  test "withConnection template":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      pool.withConnection(conn):
        let results = await conn.simpleQuery("SELECT 1")
        doAssert results.len == 1
      await pool.close()

    waitFor t()

  test "multiple concurrent acquires":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let c1 = await pool.acquire()
      let c2 = await pool.acquire()
      let c3 = await pool.acquire()
      doAssert c1.state == csReady
      doAssert c2.state == csReady
      doAssert c3.state == csReady
      pool.release(c1)
      pool.release(c2)
      pool.release(c3)
      await pool.close()

    waitFor t()

  test "maxLifetime recycles connections":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 1,
          maxSize: 3,
          maxLifetime: milliseconds(500),
          maintenanceInterval: milliseconds(100),
        )
      )

      # Get the initial connection and remember its pid
      let conn1 = await pool.acquire()
      let pid1 = conn1.pid
      doAssert conn1.state == csReady
      pool.release(conn1)

      # Wait for maxLifetime to expire
      await sleepAsync(milliseconds(600))

      # Maintenance should have closed the expired connection
      # Next acquire should create a new one
      let conn2 = await pool.acquire()
      doAssert conn2.state == csReady
      # The new connection should be different (different pid from server)
      doAssert conn2.pid != pid1
      pool.release(conn2)

      await pool.close()

    waitFor t()

  test "idleTimeout removes unused connections":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 0,
          maxSize: 3,
          idleTimeout: milliseconds(200),
          maintenanceInterval: milliseconds(100),
        )
      )

      # Create and release a connection so it sits idle
      let conn = await pool.acquire()
      doAssert conn.state == csReady
      pool.release(conn)

      # Wait for idleTimeout + maintenance cycle
      await sleepAsync(milliseconds(500))

      # Pool should have cleaned up idle connections
      doAssert pool.idleCount == 0

      await pool.close()

    waitFor t()

  test "idleTimeout respects minSize":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 2,
          maxSize: 3,
          idleTimeout: milliseconds(200),
          maintenanceInterval: milliseconds(100),
        )
      )

      # Create 3 connections and release them all
      let c1 = await pool.acquire()
      let c2 = await pool.acquire()
      let c3 = await pool.acquire()
      pool.release(c1)
      pool.release(c2)
      pool.release(c3)
      doAssert pool.idleCount == 3

      # Wait for idleTimeout + maintenance cycles
      await sleepAsync(milliseconds(500))

      # Should shrink to minSize, not below
      doAssert pool.idleCount == 2

      await pool.close()

    waitFor t()

  test "acquire skips expired connections without waiting for maintenance":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 1,
          maxSize: 3,
          maxLifetime: milliseconds(300),
          maintenanceInterval: seconds(60), # long interval so maintenance won't run
        )
      )

      let conn1 = await pool.acquire()
      let pid1 = conn1.pid
      pool.release(conn1)

      # Wait for maxLifetime to expire
      await sleepAsync(milliseconds(400))

      # Maintenance hasn't run (60s interval), but acquire should skip expired
      let conn2 = await pool.acquire()
      doAssert conn2.state == csReady
      doAssert conn2.pid != pid1
      pool.release(conn2)

      await pool.close()

    waitFor t()

  test "recycled connection is fully functional":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 1,
          maxSize: 3,
          maxLifetime: milliseconds(300),
          maintenanceInterval: milliseconds(100),
        )
      )

      let conn1 = await pool.acquire()
      pool.release(conn1)

      # Wait for maxLifetime to expire and maintenance to clean up
      await sleepAsync(milliseconds(500))

      # New connection after recycle should execute queries correctly
      pool.withConnection(conn):
        let res = await conn.simpleQuery("SELECT 42 AS answer")
        doAssert res.len == 1
        doAssert res[0].rows[0][0].get().toString() == "42"

      await pool.close()

    waitFor t()

  test "released connection is reused":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 0,
          maxSize: 3,
          idleTimeout: minutes(10),
          maintenanceInterval: seconds(30),
        )
      )

      let conn1 = await pool.acquire()
      let pid1 = conn1.pid
      pool.release(conn1)

      # Immediate re-acquire should return the same connection
      let conn2 = await pool.acquire()
      doAssert conn2.pid == pid1
      pool.release(conn2)

      await pool.close()

    waitFor t()

suite "E2E: Pool Extended Query":
  test "pool.exec creates table":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_pool_eq")
      let tag = await pool.exec(
        "CREATE TABLE test_pool_eq (id serial PRIMARY KEY, name text NOT NULL)"
      )
      doAssert "CREATE TABLE" in tag
      discard await pool.exec("DROP TABLE test_pool_eq")
      await pool.close()

    waitFor t()

  test "pool.query returns rows":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let res = await pool.query("SELECT 1 AS num, 'hello' AS msg")
      doAssert res.rows.len == 1
      doAssert res.fields.len == 2
      doAssert res.fields[0].name == "num"
      doAssert res.fields[1].name == "msg"
      doAssert res.rows[0][0].get().toString() == "1"
      doAssert res.rows[0][1].get().toString() == "hello"
      await pool.close()

    waitFor t()

  test "pool.query with params":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_pool_qp")
      discard
        await pool.exec("CREATE TABLE test_pool_qp (id serial PRIMARY KEY, name text)")
      discard await pool.exec(
        "INSERT INTO test_pool_qp (name) VALUES ($1), ($2)",
        @[some("alice".toBytes()), some("bob".toBytes())],
      )
      let res = await pool.query(
        "SELECT name FROM test_pool_qp WHERE name = $1", @[some("bob".toBytes())]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0][0].get().toString() == "bob"
      discard await pool.exec("DROP TABLE test_pool_qp")
      await pool.close()

    waitFor t()

  test "pool.simpleQuery":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let results = await pool.simpleQuery("SELECT 1 AS a; SELECT 2 AS b")
      doAssert results.len == 2
      doAssert results[0].rows[0][0].get().toString() == "1"
      doAssert results[1].rows[0][0].get().toString() == "2"
      await pool.close()

    waitFor t()

  test "pool.notify sends notification":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))

      var received: seq[Notification]
      pool.withConnection(listener):
        listener.onNotify(
          proc(n: Notification) {.gcsafe, raises: [].} =
            received.add(n)
        )
        await listener.listen("pool_notify_chan")

        await pool.notify("pool_notify_chan", "from_pool")

        # Pump receives notification in background
        await sleepAsync(milliseconds(200))

        doAssert received.len == 1
        doAssert received[0].channel == "pool_notify_chan"
        doAssert received[0].payload == "from_pool"

        await listener.unlisten("pool_notify_chan")

      await pool.close()

    waitFor t()

suite "E2E: Transaction":
  test "withTransaction commits on success":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx")
      discard await conn.exec("CREATE TABLE test_tx (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        discard await conn.exec(
          "INSERT INTO test_tx (val) VALUES ($1)", @[some("committed".toBytes())]
        )

      let res = await conn.query("SELECT val FROM test_tx")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "committed"

      discard await conn.exec("DROP TABLE test_tx")
      await conn.close()

    waitFor t()

  test "withTransaction rolls back on exception":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_rb")
      discard
        await conn.exec("CREATE TABLE test_tx_rb (id serial PRIMARY KEY, val text)")

      var raised = false
      try:
        conn.withTransaction:
          discard await conn.exec(
            "INSERT INTO test_tx_rb (val) VALUES ($1)", @[some("rollback_me".toBytes())]
          )
          raise newException(ValueError, "intentional error")
      except ValueError:
        raised = true

      doAssert raised
      let res = await conn.query("SELECT val FROM test_tx_rb")
      doAssert res.rows.len == 0

      discard await conn.exec("DROP TABLE test_tx_rb")
      await conn.close()

    waitFor t()

  test "pool.withTransaction commits on success":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptx")
      discard await pool.exec("CREATE TABLE test_ptx (id serial PRIMARY KEY, val text)")

      pool.withTransaction(conn):
        discard await conn.exec(
          "INSERT INTO test_ptx (val) VALUES ($1)", @[some("pool_commit".toBytes())]
        )

      let res = await pool.query("SELECT val FROM test_ptx")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pool_commit"

      discard await pool.exec("DROP TABLE test_ptx")
      await pool.close()

    waitFor t()

  test "pool.withTransaction rolls back on exception":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptx_rb")
      discard
        await pool.exec("CREATE TABLE test_ptx_rb (id serial PRIMARY KEY, val text)")

      var raised = false
      try:
        pool.withTransaction(conn):
          discard await conn.exec(
            "INSERT INTO test_ptx_rb (val) VALUES ($1)",
            @[some("pool_rollback".toBytes())],
          )
          raise newException(ValueError, "intentional error")
      except ValueError:
        raised = true

      doAssert raised
      let res = await pool.query("SELECT val FROM test_ptx_rb")
      doAssert res.rows.len == 0

      discard await pool.exec("DROP TABLE test_ptx_rb")
      await pool.close()

    waitFor t()

  test "withTransaction propagates original exception when ROLLBACK fails":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let killer = await connect(plainConfig())

      var raised = false
      try:
        conn.withTransaction:
          let pidRes = await conn.query("SELECT pg_backend_pid()")
          let pid = pidRes.rows[0].getStr(0)
          # Kill the connection from another session
          discard await killer.query(
            "SELECT pg_terminate_backend($1)", @[some(pid.toBytes())]
          )
          # Give the server a moment to terminate the backend
          await sleepAsync(milliseconds(100))
          raise newException(ValueError, "original error")
      except ValueError as e:
        raised = true
        doAssert e.msg == "original error"

      doAssert raised
      await killer.close()

    waitFor t()

  test "pool.withTransaction propagates original exception when ROLLBACK fails":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let killer = await connect(plainConfig())

      var raised = false
      try:
        pool.withTransaction(conn):
          let pidRes = await conn.query("SELECT pg_backend_pid()")
          let pid = pidRes.rows[0].getStr(0)
          discard await killer.query(
            "SELECT pg_terminate_backend($1)", @[some(pid.toBytes())]
          )
          await sleepAsync(milliseconds(100))
          raise newException(ValueError, "original error")
      except ValueError as e:
        raised = true
        doAssert e.msg == "original error"

      doAssert raised
      await killer.close()
      await pool.close()

    waitFor t()

  test "withTransaction with isolation level commits":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_iso")
      discard
        await conn.exec("CREATE TABLE test_tx_iso (id serial PRIMARY KEY, val text)")

      conn.withTransaction(TransactionOptions(isolation: ilSerializable)):
        discard await conn.exec(
          "INSERT INTO test_tx_iso (val) VALUES ($1)", @[some("serializable".toBytes())]
        )

      let res = await conn.query("SELECT val FROM test_tx_iso")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "serializable"

      discard await conn.exec("DROP TABLE test_tx_iso")
      await conn.close()

    waitFor t()

  test "withTransaction with READ ONLY rejects writes":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_ro")
      discard
        await conn.exec("CREATE TABLE test_tx_ro (id serial PRIMARY KEY, val text)")

      var raised = false
      try:
        conn.withTransaction(TransactionOptions(access: amReadOnly)):
          discard await conn.exec(
            "INSERT INTO test_tx_ro (val) VALUES ($1)", @[some("nope".toBytes())]
          )
      except PgError:
        raised = true

      doAssert raised

      discard await conn.exec("DROP TABLE test_tx_ro")
      await conn.close()

    waitFor t()

  test "withTransaction with all options":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      conn.withTransaction(
        TransactionOptions(
          isolation: ilSerializable, access: amReadOnly, deferrable: dmDeferrable
        )
      ):
        let res = await conn.query("SELECT 1")
        doAssert res.rows.len == 1

      await conn.close()

    waitFor t()

  test "pool.withTransaction with options":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptx_opts")
      discard
        await pool.exec("CREATE TABLE test_ptx_opts (id serial PRIMARY KEY, val text)")

      pool.withTransaction(conn, TransactionOptions(isolation: ilRepeatableRead)):
        discard await conn.exec(
          "INSERT INTO test_ptx_opts (val) VALUES ($1)", @[some("pool_opts".toBytes())]
        )

      let res = await pool.query("SELECT val FROM test_ptx_opts")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pool_opts"

      discard await pool.exec("DROP TABLE test_ptx_opts")
      await pool.close()

    waitFor t()

  test "withSavepoint releases on success":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_sp")
      discard await conn.exec("CREATE TABLE test_sp (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        conn.withSavepoint:
          discard await conn.exec(
            "INSERT INTO test_sp (val) VALUES ($1)", @[some("saved".toBytes())]
          )

      let res = await conn.query("SELECT val FROM test_sp")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "saved"

      discard await conn.exec("DROP TABLE test_sp")
      await conn.close()

    waitFor t()

  test "withSavepoint rolls back on exception":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_sp_rb")
      discard
        await conn.exec("CREATE TABLE test_sp_rb (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        discard await conn.exec(
          "INSERT INTO test_sp_rb (val) VALUES ($1)", @[some("before".toBytes())]
        )
        try:
          conn.withSavepoint:
            discard await conn.exec(
              "INSERT INTO test_sp_rb (val) VALUES ($1)", @[some("inner".toBytes())]
            )
            raise newException(ValueError, "savepoint error")
        except ValueError:
          discard

      let res = await conn.query("SELECT val FROM test_sp_rb ORDER BY id")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "before"

      discard await conn.exec("DROP TABLE test_sp_rb")
      await conn.close()

    waitFor t()

  test "nested withSavepoint":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_sp_nest")
      discard
        await conn.exec("CREATE TABLE test_sp_nest (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        conn.withSavepoint:
          discard await conn.exec(
            "INSERT INTO test_sp_nest (val) VALUES ($1)", @[some("outer".toBytes())]
          )
          try:
            conn.withSavepoint:
              discard await conn.exec(
                "INSERT INTO test_sp_nest (val) VALUES ($1)", @[some("inner".toBytes())]
              )
              raise newException(ValueError, "inner error")
          except ValueError:
            discard

      let res = await conn.query("SELECT val FROM test_sp_nest ORDER BY id")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "outer"

      discard await conn.exec("DROP TABLE test_sp_nest")
      await conn.close()

    waitFor t()

  test "withSavepoint with named savepoint":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_sp_named")
      discard
        await conn.exec("CREATE TABLE test_sp_named (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        conn.withSavepoint("my_sp"):
          discard await conn.exec(
            "INSERT INTO test_sp_named (val) VALUES ($1)", @[some("named".toBytes())]
          )

      let res = await conn.query("SELECT val FROM test_sp_named")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "named"

      discard await conn.exec("DROP TABLE test_sp_named")
      await conn.close()

    waitFor t()

suite "E2E: Type Roundtrip":
  test "integer types roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::int4, $2::int8",
        @[some("42".toBytes()), some("9999999999".toBytes())],
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 42'i32
      doAssert res.rows[0].getInt64(1) == 9999999999'i64
      await conn.close()

    waitFor t()

  test "float roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::float8", @[some("3.14".toBytes())])
      doAssert res.rows.len == 1
      doAssert abs(res.rows[0].getFloat(0) - 3.14) < 1e-10
      await conn.close()

    waitFor t()

  test "bool roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::bool, $2::bool", @[some("t".toBytes()), some("f".toBytes())]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBool(0) == true
      doAssert res.rows[0].getBool(1) == false
      await conn.close()

    waitFor t()

  test "text roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::text", @[some("hello world".toBytes())])
      doAssert res.rows[0].getStr(0) == "hello world"
      await conn.close()

    waitFor t()

  test "NULL handling":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::text, $1::text", @[some("ok".toBytes())])
      doAssert res.rows[0].isNull(0)
      doAssert not res.rows[0].isNull(1)
      doAssert res.rows[0].getStr(1) == "ok"
      await conn.close()

    waitFor t()

  test "NULL parameter with Option[T]":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::text IS NULL, $2::int4",
        @[toPgParam(none(string)).value, some("7".toBytes())],
      )
      doAssert res.rows[0].getStr(0) == "t"
      doAssert res.rows[0].getInt(1) == 7'i32
      await conn.close()

    waitFor t()

suite "E2E: PgParam Typed Parameters":
  test "exec and query with toPgParam (no explicit casts)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_pgparam")
      discard
        await conn.exec("CREATE TABLE test_pgparam (id int, name text, active bool)")

      # Insert using PgParam — OIDs let PostgreSQL infer types without $1::type casts
      discard await conn.exec(
        "INSERT INTO test_pgparam (id, name, active) VALUES ($1, $2, $3)",
        @[toPgParam(42'i32), toPgParam("alice"), toPgParam(true)],
      )

      let res = await conn.query(
        "SELECT id, name, active FROM test_pgparam WHERE id = $1", @[toPgParam(42'i32)]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 42'i32
      doAssert res.rows[0].getStr(1) == "alice"
      doAssert res.rows[0].getBool(2) == true

      discard await conn.exec("DROP TABLE test_pgparam")
      await conn.close()

    waitFor t()

  test "query with int (platform int) parameter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1 + 1", @[toPgParam(99)])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt64(0) == 100'i64
      await conn.close()

    waitFor t()

  test "query with NULL via Option[T]":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1 IS NULL, $2", @[toPgParam(none(string)), toPgParam("ok")]
      )
      doAssert res.rows[0].getStr(0) == "t"
      doAssert res.rows[0].getStr(1) == "ok"
      await conn.close()

    waitFor t()

  test "exec with int64 and float64 params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT $1, $2", @[toPgParam(9999999999'i64), toPgParam(3.14)])
      doAssert res.rows[0].getInt64(0) == 9999999999'i64
      doAssert abs(res.rows[0].getFloat(1) - 3.14) < 1e-10
      await conn.close()

    waitFor t()

  test "pool exec and query with PgParam":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_pgparam_pool")
      discard await pool.exec("CREATE TABLE test_pgparam_pool (id int, val text)")
      discard await pool.exec(
        "INSERT INTO test_pgparam_pool (id, val) VALUES ($1, $2)",
        @[toPgParam(1'i32), toPgParam("pooled")],
      )
      let res = await pool.query(
        "SELECT val FROM test_pgparam_pool WHERE id = $1", @[toPgParam(1'i32)]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pooled"
      discard await pool.exec("DROP TABLE test_pgparam_pool")
      await pool.close()

    waitFor t()

  test "execute prepared statement with PgParam":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt = await conn.prepare("pgparam_stmt", "SELECT $1::int4 + $2::int4")
      let res = await stmt.execute(@[toPgParam(10'i32), toPgParam(20'i32)])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 30'i32
      await stmt.close()
      await conn.close()

    waitFor t()

suite "E2E: COPY Protocol":
  test "copyIn inserts rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_in")
      discard await conn.exec("CREATE TABLE test_copy_in (id int, name text)")

      # Prepare tab-delimited rows (PostgreSQL text format default)
      let rows =
        @["1\tAlice\n".toBytes(), "2\tBob\n".toBytes(), "3\tCharlie\n".toBytes()]
      let tag = await conn.copyIn("COPY test_copy_in FROM STDIN", rows)
      doAssert "COPY 3" in tag

      # Verify the data was inserted
      let res = await conn.query("SELECT id, name FROM test_copy_in ORDER BY id")
      doAssert res.rows.len == 3
      doAssert res.rows[0].getStr(1) == "Alice"
      doAssert res.rows[1].getStr(1) == "Bob"
      doAssert res.rows[2].getStr(1) == "Charlie"

      discard await conn.exec("DROP TABLE test_copy_in")
      await conn.close()

    waitFor t()

  test "copyOut retrieves rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_out")
      discard await conn.exec("CREATE TABLE test_copy_out (id int, name text)")
      discard
        await conn.exec("INSERT INTO test_copy_out VALUES (1, 'Alice'), (2, 'Bob')")

      let result = await conn.copyOut("COPY test_copy_out TO STDOUT")
      doAssert result.format == cfText
      doAssert "COPY 2" in result.commandTag
      doAssert result.data.len == 2

      # Each row is a tab-delimited line with trailing newline
      doAssert result.data[0].toString() == "1\tAlice\n"
      doAssert result.data[1].toString() == "2\tBob\n"

      discard await conn.exec("DROP TABLE test_copy_out")
      await conn.close()

    waitFor t()

  test "copyIn with invalid SQL raises PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.copyIn(
          "COPY nonexistent_table FROM STDIN", @["1\ttest\n".toBytes()]
        )
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "copyOut with invalid SQL raises PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.copyOut("COPY nonexistent_table TO STDOUT")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "copyIn empty data":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_empty")
      discard await conn.exec("CREATE TABLE test_copy_empty (id int, name text)")

      let tag = await conn.copyIn("COPY test_copy_empty FROM STDIN", @[])
      doAssert "COPY 0" in tag

      let res = await conn.query("SELECT count(*) FROM test_copy_empty")
      doAssert res.rows[0].getStr(0) == "0"

      discard await conn.exec("DROP TABLE test_copy_empty")
      await conn.close()

    waitFor t()

  test "copyOut from empty table":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_out_empty")
      discard await conn.exec("CREATE TABLE test_copy_out_empty (id int, name text)")

      let result = await conn.copyOut("COPY test_copy_out_empty TO STDOUT")
      doAssert result.data.len == 0
      doAssert "COPY 0" in result.commandTag
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_copy_out_empty")
      await conn.close()

    waitFor t()

  test "copyIn large data (10000 rows)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_large")
      discard await conn.exec("CREATE TABLE test_copy_large (id int, val text)")

      var rows: seq[seq[byte]]
      for i in 1 .. 10000:
        rows.add(($i & "\trow_" & $i & "\n").toBytes())
      let tag = await conn.copyIn("COPY test_copy_large FROM STDIN", rows)
      doAssert "COPY 10000" in tag

      let res = await conn.query("SELECT count(*) FROM test_copy_large")
      doAssert res.rows[0].getStr(0) == "10000"

      discard await conn.exec("DROP TABLE test_copy_large")
      await conn.close()

    waitFor t()

  test "copyOut large data (10000 rows)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_out_large")
      discard await conn.exec("CREATE TABLE test_copy_out_large (id int)")
      discard await conn.exec(
        "INSERT INTO test_copy_out_large SELECT g FROM generate_series(1, 10000) AS g"
      )

      let result = await conn.copyOut("COPY test_copy_out_large TO STDOUT")
      doAssert result.data.len == 10000
      doAssert "COPY 10000" in result.commandTag

      discard await conn.exec("DROP TABLE test_copy_out_large")
      await conn.close()

    waitFor t()

  test "copyIn with NULL values":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_null")
      discard await conn.exec("CREATE TABLE test_copy_null (id int, name text)")

      # \N is the PostgreSQL text-format representation of NULL
      let rows = @["1\tAlice\n".toBytes(), "2\t\\N\n".toBytes(), "\\N\tBob\n".toBytes()]
      let tag = await conn.copyIn("COPY test_copy_null FROM STDIN", rows)
      doAssert "COPY 3" in tag

      let res = await conn.query(
        "SELECT id, name FROM test_copy_null ORDER BY COALESCE(id, 999)"
      )
      doAssert res.rows[0].getStr(0) == "1"
      doAssert res.rows[0].getStr(1) == "Alice"
      doAssert res.rows[1].getStr(0) == "2"
      doAssert res.rows[1].isNull(1)
      doAssert res.rows[2].isNull(0)
      doAssert res.rows[2].getStr(1) == "Bob"

      discard await conn.exec("DROP TABLE test_copy_null")
      await conn.close()

    waitFor t()

  test "copyOut with NULL values":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_out_null")
      discard await conn.exec("CREATE TABLE test_copy_out_null (id int, name text)")
      discard await conn.exec(
        "INSERT INTO test_copy_out_null VALUES (1, NULL), (NULL, 'Bob')"
      )

      let result = await conn.copyOut("COPY test_copy_out_null TO STDOUT")
      doAssert result.data.len == 2
      # NULL is represented as \N in text format
      doAssert result.data[0].toString() == "1\t\\N\n"
      doAssert result.data[1].toString() == "\\N\tBob\n"

      discard await conn.exec("DROP TABLE test_copy_out_null")
      await conn.close()

    waitFor t()

  test "copyIn with special characters (tab, backslash, newline in data)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_special")
      discard await conn.exec("CREATE TABLE test_copy_special (id int, val text)")

      # In PostgreSQL text COPY format:
      # \t = literal tab, \n = literal newline, \\ = literal backslash
      let rows = @[
        "1\thas\\\\backslash\n".toBytes(),
        "2\thas\\nnewline\n".toBytes(),
        "3\thas\\ttab\n".toBytes(),
      ]
      let tag = await conn.copyIn("COPY test_copy_special FROM STDIN", rows)
      doAssert "COPY 3" in tag

      let res = await conn.query("SELECT id, val FROM test_copy_special ORDER BY id")
      doAssert res.rows.len == 3
      doAssert res.rows[0].getStr(1) == "has\\backslash"
      doAssert res.rows[1].getStr(1) == "has\nnewline"
      doAssert res.rows[2].getStr(1) == "has\ttab"

      discard await conn.exec("DROP TABLE test_copy_special")
      await conn.close()

    waitFor t()

  test "copyIn with CSV format":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_csv")
      discard await conn.exec("CREATE TABLE test_copy_csv (id int, name text)")

      let rows = @[
        "1,Alice\n".toBytes(),
        "2,\"Bob, Jr.\"\n".toBytes(),
        "3,\"Has \"\"quotes\"\"\"\n".toBytes(),
      ]
      let tag =
        await conn.copyIn("COPY test_copy_csv FROM STDIN WITH (FORMAT csv)", rows)
      doAssert "COPY 3" in tag

      let res = await conn.query("SELECT id, name FROM test_copy_csv ORDER BY id")
      doAssert res.rows[0].getStr(1) == "Alice"
      doAssert res.rows[1].getStr(1) == "Bob, Jr."
      doAssert res.rows[2].getStr(1) == "Has \"quotes\""

      discard await conn.exec("DROP TABLE test_copy_csv")
      await conn.close()

    waitFor t()

  test "copyOut with CSV format":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_csv_out")
      discard await conn.exec("CREATE TABLE test_copy_csv_out (id int, name text)")
      discard await conn.exec(
        "INSERT INTO test_copy_csv_out VALUES (1, 'Alice'), (2, 'Bob, Jr.')"
      )

      let result =
        await conn.copyOut("COPY test_copy_csv_out TO STDOUT WITH (FORMAT csv)")
      doAssert result.data.len == 2
      doAssert result.data[0].toString() == "1,Alice\n"
      doAssert result.data[1].toString() == "2,\"Bob, Jr.\"\n"

      discard await conn.exec("DROP TABLE test_copy_csv_out")
      await conn.close()

    waitFor t()

  test "copyOutStream basic streaming":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_stream")
      discard await conn.exec("CREATE TABLE test_copy_stream (id int, name text)")
      discard
        await conn.exec("INSERT INTO test_copy_stream VALUES (1, 'Alice'), (2, 'Bob')")

      var chunks: seq[seq[byte]]
      let cb = makeCopyOutCallback:
        chunks.add(data)
      let info = await conn.copyOutStream("COPY test_copy_stream TO STDOUT", cb)
      doAssert info.format == cfText
      doAssert "COPY 2" in info.commandTag
      doAssert chunks.len == 2
      doAssert chunks[0].toString() == "1\tAlice\n"
      doAssert chunks[1].toString() == "2\tBob\n"
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_copy_stream")
      await conn.close()

    waitFor t()

  test "copyOutStream empty table":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_stream_empty")
      discard await conn.exec("CREATE TABLE test_copy_stream_empty (id int, name text)")

      var callCount = 0
      let cb = makeCopyOutCallback:
        inc callCount
      let info = await conn.copyOutStream("COPY test_copy_stream_empty TO STDOUT", cb)
      doAssert callCount == 0
      doAssert "COPY 0" in info.commandTag
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_copy_stream_empty")
      await conn.close()

    waitFor t()

  test "consecutive copyIn operations":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_consecutive")
      discard await conn.exec("CREATE TABLE test_copy_consecutive (id int, batch int)")

      let rows1 = @["1\t1\n".toBytes(), "2\t1\n".toBytes()]
      let tag1 = await conn.copyIn("COPY test_copy_consecutive FROM STDIN", rows1)
      doAssert "COPY 2" in tag1

      let rows2 = @["3\t2\n".toBytes(), "4\t2\n".toBytes(), "5\t2\n".toBytes()]
      let tag2 = await conn.copyIn("COPY test_copy_consecutive FROM STDIN", rows2)
      doAssert "COPY 3" in tag2

      let res = await conn.query("SELECT count(*) FROM test_copy_consecutive")
      doAssert res.rows[0].getStr(0) == "5"

      discard await conn.exec("DROP TABLE test_copy_consecutive")
      await conn.close()

    waitFor t()

  test "copyIn single row with large payload":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_big_row")
      discard await conn.exec("CREATE TABLE test_copy_big_row (id int, data text)")

      let bigText = 'x'.repeat(100_000)
      let rows = @[("1\t" & bigText & "\n").toBytes()]
      let tag = await conn.copyIn("COPY test_copy_big_row FROM STDIN", rows)
      doAssert "COPY 1" in tag

      let res = await conn.query("SELECT length(data) FROM test_copy_big_row")
      doAssert res.rows[0].getStr(0) == "100000"

      discard await conn.exec("DROP TABLE test_copy_big_row")
      await conn.close()

    waitFor t()

suite "E2E: Cancel Request":
  test "cancel aborts pg_sleep":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Start a long-running query
      let sleepFut = conn.simpleQuery("SELECT pg_sleep(30)")

      # Give the server time to start executing
      await sleepAsync(milliseconds(100))

      # Cancel the query via a separate TCP connection
      await conn.cancel()

      # The original query should fail with query_canceled (57014)
      var raised = false
      try:
        discard await sleepFut
      except PgError as e:
        raised = true
        doAssert "57014" in e.msg
      doAssert raised

      # Connection should still be usable after cancel
      doAssert conn.state == csReady
      let res = await conn.simpleQuery("SELECT 1 AS check_col")
      doAssert res[0].rows[0][0].get().toString() == "1"

      await conn.close()

    waitFor t()

suite "E2E: Notice Callback":
  test "RAISE NOTICE triggers noticeCallback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var received: seq[Notice]
      conn.noticeCallback = proc(n: Notice) {.gcsafe, raises: [].} =
        received.add(n)

      discard await conn.exec("DO $$ BEGIN RAISE NOTICE 'hello from notice'; END $$")

      doAssert received.len == 1
      # Check that the message field ('M') contains our text
      var foundMsg = false
      for f in received[0].fields:
        if f.code == 'M':
          doAssert f.value == "hello from notice"
          foundMsg = true
      doAssert foundMsg

      await conn.close()

    waitFor t()

  test "notice callback not set does not interfere":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # No noticeCallback set — should not hang or error
      discard await conn.exec("DO $$ BEGIN RAISE NOTICE 'ignored'; END $$")
      let res = await conn.query("SELECT 1 AS check_col")
      doAssert res.rows.len == 1
      await conn.close()

    waitFor t()

suite "E2E: LISTEN/NOTIFY":
  test "basic notify and receive":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("test_chan")

      await sender.notify("test_chan", "hello")

      # Pump receives notification in background
      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "test_chan"
      doAssert received[0].payload == "hello"

      await listener.unlisten("test_chan")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notify without payload":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("test_chan2")

      await sender.notify("test_chan2")

      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "test_chan2"
      doAssert received[0].payload == ""

      await listener.unlisten("test_chan2")
      await listener.close()
      await sender.close()

    waitFor t()

  test "multiple channels":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )

      await listener.listen("chan_a")
      await listener.listen("chan_b")

      await sender.notify("chan_a", "msg_a")
      await sender.notify("chan_b", "msg_b")

      await sleepAsync(milliseconds(200))

      doAssert received.len == 2
      doAssert received[0].channel == "chan_a"
      doAssert received[0].payload == "msg_a"
      doAssert received[1].channel == "chan_b"
      doAssert received[1].payload == "msg_b"

      await listener.unlisten("chan_a")
      await listener.unlisten("chan_b")
      await listener.close()
      await sender.close()

    waitFor t()

  test "unlisten stops notifications":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("test_unsub")

      await sender.notify("test_unsub", "before")
      # Pump is running in background, wait for delivery
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("test_unsub")

      await sender.notify("test_unsub", "after")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.close()
      await sender.close()

    waitFor t()

suite "E2E: Background LISTEN Pump":
  test "notification arrives without explicit query":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("bg_chan")

      # Send notification — pump should receive it without SELECT 1
      await sender.notify("bg_chan", "background")
      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "bg_chan"
      doAssert received[0].payload == "background"

      await listener.unlisten("bg_chan")
      await listener.close()
      await sender.close()

    waitFor t()

  test "multiple channels received in background":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )

      await listener.listen("bg_a")
      await listener.listen("bg_b")

      await sender.notify("bg_a", "msg_a")
      await sender.notify("bg_b", "msg_b")
      await sleepAsync(milliseconds(200))

      doAssert received.len == 2
      doAssert received[0].channel == "bg_a"
      doAssert received[1].channel == "bg_b"

      await listener.unlisten("bg_a")
      await listener.unlisten("bg_b")
      await listener.close()
      await sender.close()

    waitFor t()

  test "unlisten stops pump when no channels remain":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("bg_stop")

      await sender.notify("bg_stop", "before")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("bg_stop")
      doAssert listener.state == csReady

      # Connection should be usable for queries after pump stops
      let res = await listener.simpleQuery("SELECT 1")
      doAssert res[0].rows[0][0].get().toString() == "1"

      await listener.close()
      await sender.close()

    waitFor t()

  test "close does not hang with active pump":
    proc t() {.async.} =
      let listener = await connect(plainConfig())

      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          discard
      )
      await listener.listen("bg_close")

      doAssert listener.state == csListening

      # close should cancel pump and not hang
      await listener.close()
      doAssert listener.state == csClosed

    waitFor t()

  test "unlisten partial channels keeps pump running":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )

      await listener.listen("partial_a")
      await listener.listen("partial_b")

      # Unlisten only one channel — pump should still be running for the other
      await listener.unlisten("partial_a")
      doAssert listener.state == csListening

      await sender.notify("partial_b", "still_alive")
      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "partial_b"
      doAssert received[0].payload == "still_alive"

      # Notification on unlistened channel should not arrive
      await sender.notify("partial_a", "should_not_arrive")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("partial_b")
      await listener.close()
      await sender.close()

    waitFor t()

  test "listen after unlisten restarts pump":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("bg_restart")

      await sender.notify("bg_restart", "first")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("bg_restart")
      doAssert listener.state == csReady

      # Re-listen should restart pump (callback is preserved)
      await listener.listen("bg_restart2")
      doAssert listener.state == csListening

      await sender.notify("bg_restart2", "second")
      await sleepAsync(milliseconds(200))
      doAssert received.len == 2
      doAssert received[1].channel == "bg_restart2"
      doAssert received[1].payload == "second"

      await listener.unlisten("bg_restart2")
      await listener.close()
      await sender.close()

    waitFor t()

suite "E2E: Cursor/Streaming":
  test "cursor fetches all rows in chunks":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor")
      discard await conn.exec("CREATE TABLE test_cursor (id int)")
      for i in 1 .. 100:
        discard await conn.exec(
          "INSERT INTO test_cursor (id) VALUES ($1)", @[some(($i).toBytes())]
        )

      let cursor =
        await conn.openCursor("SELECT id FROM test_cursor ORDER BY id", chunkSize = 10)
      doAssert cursor.fields.len == 1

      var allRows: seq[Row]
      while true:
        let chunk = await cursor.fetchNext()
        if chunk.len == 0:
          break
        allRows.add(chunk)

      doAssert allRows.len == 100
      doAssert allRows[0].getStr(0) == "1"
      doAssert allRows[99].getStr(0) == "100"
      doAssert cursor.exhausted
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor")
      await conn.close()

    waitFor t()

  test "mid-stream closeCursor returns conn to ready":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_close")
      discard await conn.exec("CREATE TABLE test_cursor_close (id int)")
      for i in 1 .. 50:
        discard await conn.exec(
          "INSERT INTO test_cursor_close (id) VALUES ($1)", @[some(($i).toBytes())]
        )

      let cursor = await conn.openCursor(
        "SELECT id FROM test_cursor_close ORDER BY id", chunkSize = 10
      )
      let chunk1 = await cursor.fetchNext()
      doAssert chunk1.len == 10

      await cursor.closeCursor()
      doAssert conn.state == csReady

      # Connection is usable after closing cursor mid-stream
      let res = await conn.query("SELECT 1 AS check_col")
      doAssert res.rows.len == 1

      discard await conn.exec("DROP TABLE test_cursor_close")
      await conn.close()

    waitFor t()

  test "withCursor template":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_with")
      discard await conn.exec("CREATE TABLE test_cursor_with (id int)")
      for i in 1 .. 25:
        discard await conn.exec(
          "INSERT INTO test_cursor_with (id) VALUES ($1)", @[some(($i).toBytes())]
        )

      var total = 0
      conn.withCursor("SELECT id FROM test_cursor_with ORDER BY id", 10'i32, cur):
        while true:
          let chunk = await cur.fetchNext()
          if chunk.len == 0:
            break
          total += chunk.len

      doAssert total == 25
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor_with")
      await conn.close()

    waitFor t()

  test "cursor with zero rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_empty")
      discard await conn.exec("CREATE TABLE test_cursor_empty (id int)")

      let cursor =
        await conn.openCursor("SELECT id FROM test_cursor_empty", chunkSize = 10)
      doAssert cursor.exhausted
      let chunk = await cursor.fetchNext()
      doAssert chunk.len == 0
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor_empty")
      await conn.close()

    waitFor t()

  test "fetchNext on exhausted cursor returns empty":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_exhaust")
      discard await conn.exec("CREATE TABLE test_cursor_exhaust (id int)")
      discard
        await conn.exec("INSERT INTO test_cursor_exhaust (id) VALUES (1), (2), (3)")

      let cursor = await conn.openCursor(
        "SELECT id FROM test_cursor_exhaust ORDER BY id", chunkSize = 100
      )
      # First fetch gets all rows + marks exhausted
      let chunk1 = await cursor.fetchNext()
      doAssert chunk1.len == 3
      doAssert cursor.exhausted

      let chunk2 = await cursor.fetchNext()
      doAssert chunk2.len == 0

      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor_exhaust")
      await conn.close()

    waitFor t()

  test "withCursor cleans up on exception":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_exc")
      discard await conn.exec("CREATE TABLE test_cursor_exc (id int)")
      for i in 1 .. 20:
        discard await conn.exec(
          "INSERT INTO test_cursor_exc (id) VALUES ($1)", @[some(($i).toBytes())]
        )

      var raised = false
      try:
        conn.withCursor("SELECT id FROM test_cursor_exc ORDER BY id", 5'i32, cur):
          let chunk = await cur.fetchNext()
          doAssert chunk.len == 5
          raise newException(ValueError, "intentional error")
      except ValueError:
        raised = true

      doAssert raised
      doAssert conn.state == csReady

      # Connection is usable after exception in withCursor
      let res = await conn.query("SELECT 1 AS check_col")
      doAssert res.rows.len == 1

      discard await conn.exec("DROP TABLE test_cursor_exc")
      await conn.close()

    waitFor t()

  test "openCursor with invalid SQL raises PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var raised = false
      try:
        discard
          await conn.openCursor("SELECT * FROM nonexistent_table_xyz", chunkSize = 10)
      except PgError:
        raised = true

      doAssert raised
      doAssert conn.state == csReady

      # Connection is usable after cursor error
      let res = await conn.query("SELECT 1 AS check_col")
      doAssert res.rows.len == 1

      await conn.close()

    waitFor t()

  test "cursor with PgParam parameters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_params")
      discard await conn.exec("CREATE TABLE test_cursor_params (id int, name text)")
      discard await conn.exec(
        "INSERT INTO test_cursor_params VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')"
      )

      let cursor = await conn.openCursor(
        "SELECT id FROM test_cursor_params WHERE name = $1 ORDER BY id",
        @[toPgParam("alice")],
        chunkSize = 10,
      )
      let chunk = await cursor.fetchNext()
      doAssert chunk.len == 2
      doAssert chunk[0].getStr(0) == "1"
      doAssert chunk[1].getStr(0) == "3"

      let empty = await cursor.fetchNext()
      doAssert empty.len == 0
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor_params")
      await conn.close()

    waitFor t()

  test "cursor with timeout succeeds when fast enough":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_timeout")
      discard await conn.exec("CREATE TABLE test_cursor_timeout (id int)")
      for i in 1 .. 10:
        discard await conn.exec(
          "INSERT INTO test_cursor_timeout (id) VALUES ($1)", @[some(($i).toBytes())]
        )

      let cursor = await conn.openCursor(
        "SELECT id FROM test_cursor_timeout ORDER BY id",
        chunkSize = 5,
        timeout = seconds(5),
      )
      doAssert cursor.fields.len == 1

      var allRows: seq[Row]
      while true:
        let chunk = await cursor.fetchNext()
        if chunk.len == 0:
          break
        allRows.add(chunk)

      doAssert allRows.len == 10
      doAssert cursor.exhausted
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor_timeout")
      await conn.close()

    waitFor t()

  test "openCursor times out on slow query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var raised = false
      try:
        # pg_sleep(10) will delay the first Execute response
        discard await conn.openCursor(
          "SELECT pg_sleep(10)", chunkSize = 1, timeout = milliseconds(100)
        )
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg

      doAssert raised
      doAssert conn.state == csClosed

    waitFor t()

  test "fetchNext times out on slow query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Row v=1 returns instantly, v>=2 sleeps 2s — exceeds the 500ms timeout
      let cursor = await conn.openCursor(
        "SELECT v, CASE WHEN v = 1 THEN pg_sleep(0) ELSE pg_sleep(2) END " &
          "FROM generate_series(1, 5) AS v",
        chunkSize = 1,
        timeout = milliseconds(500),
      )
      # openCursor fetched v=1 (instant) into buffer
      let chunk1 = await cursor.fetchNext() # returns buffered data, no I/O
      doAssert chunk1.len == 1

      var raised = false
      try:
        discard await cursor.fetchNext() # v=2 sleeps 2s → timeout
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg

      doAssert raised
      doAssert conn.state == csClosed

    waitFor t()

  test "cursor with chunkSize 1 fetches one row at a time":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let cursor =
        await conn.openCursor("SELECT g FROM generate_series(1, 5) AS g", chunkSize = 1)

      for i in 1 .. 5:
        let chunk = await cursor.fetchNext()
        doAssert chunk.len == 1
        doAssert chunk[0].getStr(0) == $i

      let empty = await cursor.fetchNext()
      doAssert empty.len == 0
      doAssert cursor.exhausted
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "cursor rows exactly equal to chunkSize (boundary)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # 20 rows with chunkSize=10 → exactly 2 full chunks, no partial
      let cursor = await conn.openCursor(
        "SELECT g FROM generate_series(1, 20) AS g", chunkSize = 10
      )

      let chunk1 = await cursor.fetchNext()
      doAssert chunk1.len == 10

      let chunk2 = await cursor.fetchNext()
      doAssert chunk2.len == 10

      # Next fetch should discover exhaustion
      let chunk3 = await cursor.fetchNext()
      doAssert chunk3.len == 0
      doAssert cursor.exhausted
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "multiple sequential cursors on same connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # First cursor
      let cursor1 = await conn.openCursor(
        "SELECT g FROM generate_series(1, 5) AS g", chunkSize = 10
      )
      let rows1 = await cursor1.fetchNext()
      doAssert rows1.len == 5
      doAssert cursor1.exhausted

      # Second cursor on same connection
      let cursor2 = await conn.openCursor(
        "SELECT g FROM generate_series(10, 15) AS g", chunkSize = 10
      )
      let rows2 = await cursor2.fetchNext()
      doAssert rows2.len == 6
      doAssert rows2[0].getStr(0) == "10"
      doAssert cursor2.exhausted

      # Third cursor to confirm no state leakage
      let cursor3 = await conn.openCursor("SELECT 42 AS answer", chunkSize = 10)
      let rows3 = await cursor3.fetchNext()
      doAssert rows3.len == 1
      doAssert rows3[0].getStr(0) == "42"

      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "cursor with NULL values in result":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_nulls")
      discard await conn.exec("CREATE TABLE test_cursor_nulls (id int, val text)")
      discard await conn.exec(
        "INSERT INTO test_cursor_nulls VALUES (1, 'a'), (2, NULL), (3, 'c'), (NULL, NULL)"
      )

      let cursor = await conn.openCursor(
        "SELECT id, val FROM test_cursor_nulls ORDER BY COALESCE(id, 999)",
        chunkSize = 2,
      )

      let chunk1 = await cursor.fetchNext()
      doAssert chunk1.len == 2
      doAssert chunk1[0].getStr(0) == "1"
      doAssert chunk1[0].getStr(1) == "a"
      doAssert chunk1[1].getStr(0) == "2"
      doAssert chunk1[1].isNull(1)

      let chunk2 = await cursor.fetchNext()
      doAssert chunk2.len == 2
      doAssert chunk2[0].getStr(0) == "3"
      doAssert chunk2[0].getStr(1) == "c"
      doAssert chunk2[1].isNull(0)
      doAssert chunk2[1].isNull(1)

      let empty = await cursor.fetchNext()
      doAssert empty.len == 0
      doAssert cursor.exhausted

      discard await conn.exec("DROP TABLE test_cursor_nulls")
      await conn.close()

    waitFor t()

  test "closeCursor on already exhausted cursor":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let cursor = await conn.openCursor("SELECT 1 AS x", chunkSize = 10)
      let chunk = await cursor.fetchNext()
      doAssert chunk.len == 1
      doAssert cursor.exhausted

      # closeCursor on exhausted cursor should be safe no-op
      await cursor.closeCursor()
      doAssert conn.state == csReady

      let res = await conn.query("SELECT 2 AS y")
      doAssert res.rows[0].getStr(0) == "2"
      await conn.close()

    waitFor t()

suite "E2E: Operation Timeouts":
  test "exec with timeout succeeds when fast enough":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let tag = await conn.exec("SELECT 1", timeout = seconds(5))
      doAssert tag == "SELECT 1"
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "exec times out on slow query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.exec("SELECT pg_sleep(10)", timeout = milliseconds(100))
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg
      doAssert raised
      doAssert conn.state == csClosed

    waitFor t()

  test "query with timeout succeeds when fast enough":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT 42 AS v", timeout = seconds(5))
      doAssert qr.rows.len == 1
      doAssert qr.rows[0][0].get().toString() == "42"
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "query times out on slow query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.query("SELECT pg_sleep(10)", timeout = milliseconds(100))
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg
      doAssert raised
      doAssert conn.state == csClosed

    waitFor t()

  test "prepare and execute with timeout succeed when fast enough":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt =
        await conn.prepare("test_timeout_stmt", "SELECT $1::int", timeout = seconds(5))
      let qr = await stmt.execute(@[some(("7").toBytes())], timeout = seconds(5))
      doAssert qr.rows.len == 1
      doAssert qr.rows[0][0].get().toString() == "7"
      await stmt.close(timeout = seconds(5))
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "execute times out on slow query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt = await conn.prepare("test_timeout_exec", "SELECT pg_sleep($1::float)")
      var raised = false
      try:
        discard
          await stmt.execute(@[some(("10").toBytes())], timeout = milliseconds(100))
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg
      doAssert raised
      doAssert conn.state == csClosed

    waitFor t()

  test "pool exec with timeout succeeds":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 2))
      let tag = await pool.exec("SELECT 1", timeout = seconds(5))
      doAssert tag == "SELECT 1"
      await pool.close()

    waitFor t()

  test "pool query times out on slow query":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 2))
      var raised = false
      try:
        discard await pool.query("SELECT pg_sleep(10)", timeout = milliseconds(100))
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg
      doAssert raised
      await pool.close()

    waitFor t()

suite "E2E: Pool minSize Replenishment":
  test "replenishes to minSize after maxLifetime expiry":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 2,
          maxSize: 5,
          maxLifetime: milliseconds(200),
          maintenanceInterval: milliseconds(100),
        )
      )

      doAssert pool.idleCount == 2

      # Wait for maxLifetime to expire + maintenance to clean + replenish
      await sleepAsync(milliseconds(600))

      doAssert pool.idleCount == 2

      # Verify replenished connections are functional
      let conn = await pool.acquire()
      doAssert conn.state == csReady
      let res = await conn.simpleQuery("SELECT 1")
      doAssert res[0].rows[0][0].get().toString() == "1"
      pool.release(conn)

      await pool.close()

    waitFor t()

suite "E2E: Extended Type Roundtrip":
  test "bytea roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE test_bytea (data bytea)")
      # Insert via SQL hex literal (toPgParam for bytea uses text format, so
      # raw non-UTF8 bytes are rejected by PostgreSQL — use hex literals instead)
      discard
        await conn.exec("INSERT INTO test_bytea (data) VALUES ('\\x00DEADBEEFFF')")
      let res = await conn.query("SELECT data FROM test_bytea")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBytes(0) == @[0x00'u8, 0xDE, 0xAD, 0xBE, 0xEF, 0xFF]
      # Also test toPgParam with ASCII-safe bytes
      let safeInput = @[0x41'u8, 0x42, 0x43] # "ABC"
      discard await conn.exec(
        "INSERT INTO test_bytea (data) VALUES ($1)", @[toPgParam(safeInput)]
      )
      let res2 = await conn.query(
        "SELECT data FROM test_bytea WHERE data = $1", @[toPgParam(safeInput)]
      )
      doAssert res2.rows.len == 1
      doAssert res2.rows[0].getBytes(0) == safeInput
      await conn.close()

    waitFor t()

  test "timestamp roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2025, mMar, 15, 10, 30, 45, zone = utc())
      let res = await conn.query("SELECT $1::timestamp", @[toPgParam(dt)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTimestamp(0)
      doAssert got.year == 2025
      doAssert got.month == mMar
      doAssert got.monthday == 15
      doAssert got.hour == 10
      doAssert got.minute == 30
      doAssert got.second == 45
      await conn.close()

    waitFor t()

  test "date roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT '2025-06-15'::date")
      doAssert res.rows.len == 1
      let got = res.rows[0].getDate(0)
      doAssert got.year == 2025
      doAssert got.month == mJun
      doAssert got.monthday == 15
      await conn.close()

    waitFor t()

  test "UUID roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let uuid = PgUuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
      let res = await conn.query("SELECT $1::uuid", @[toPgParam(uuid)])
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getUuid(0) == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
      await conn.close()

    waitFor t()

  test "int16 and float32 roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::int2, $2::float4", @[toPgParam(42'i16), toPgParam(3.14'f32)]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 42'i32
      doAssert abs(res.rows[0].getFloat(1) - 3.14) < 0.01
      await conn.close()

    waitFor t()

  test "empty string vs NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE test_empty_null (val text)")
      discard await conn.exec(
        "INSERT INTO test_empty_null (val) VALUES ($1)", @[toPgParam("")]
      )
      discard await conn.exec(
        "INSERT INTO test_empty_null (val) VALUES ($1)", @[toPgParam(none(string))]
      )
      let res =
        await conn.query("SELECT val FROM test_empty_null ORDER BY val NULLS LAST")
      doAssert res.rows.len == 2
      doAssert not res.rows[0].isNull(0)
      doAssert res.rows[0].getStr(0) == ""
      doAssert res.rows[1].isNull(0)
      await conn.close()

    waitFor t()

  test "special characters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let values = @["こんにちは世界", "it's a test", "back\\slash", "NULL"]
      for v in values:
        let res = await conn.query("SELECT $1::text", @[toPgParam(v)])
        doAssert res.rows[0].getStr(0) == v
      await conn.close()

    waitFor t()

suite "E2E: Pool Stress":
  test "concurrent pool operations":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 1,
          maxSize: 3,
          acquireTimeout: seconds(30),
          maintenanceInterval: seconds(30),
        )
      )

      var futures: seq[Future[QueryResult]]
      for i in 0 ..< 20:
        futures.add(pool.query("SELECT pg_backend_pid()"))

      await allFutures(futures)
      var successCount = 0
      for f in futures:
        if f.completed():
          let qr = f.read()
          doAssert qr.rows.len == 1
          successCount.inc
      doAssert successCount == 20

      await pool.close()

    waitFor t()

  test "pool acquire timeout under contention":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 1,
          maxSize: 1,
          acquireTimeout: milliseconds(200),
          maintenanceInterval: seconds(30),
        )
      )

      # Hold the only connection
      let conn = await pool.acquire()

      # Second acquire should timeout
      var raised = false
      try:
        let conn2 = await pool.acquire()
        pool.release(conn2)
      except PgError as e:
        raised = true
        doAssert "timeout" in e.msg.toLowerAscii()

      doAssert raised

      # Release and verify pool still works
      pool.release(conn)
      let res = await pool.query("SELECT 1")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "1"

      await pool.close()

    waitFor t()

  test "pool handles broken connection":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 1,
          maxSize: 3,
          acquireTimeout: seconds(10),
          maintenanceInterval: seconds(30),
          # Health check with very short idle threshold so acquire pings the connection
          healthCheckTimeout: milliseconds(1),
          pingTimeout: seconds(5),
        )
      )

      # Get a connection and kill its backend
      let conn = await pool.acquire()
      let pidRes = await conn.query("SELECT pg_backend_pid()")
      let pid = pidRes.rows[0].getInt(0)
      pool.release(conn)

      # Kill the backend via a separate connection
      let killer = await connect(plainConfig())
      discard await killer.exec("SELECT pg_terminate_backend($1)", @[toPgParam(pid)])
      await killer.close()

      # Give the server time to terminate the backend
      await sleepAsync(milliseconds(200))

      # Pool should detect broken connection via health check, discard it,
      # create a new connection and work fine
      let res = await pool.query("SELECT 42 AS val")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "42"

      await pool.close()

    waitFor t()

suite "E2E: Multi-Statement and Large Results":
  test "simpleQuery multiple statements":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let results = await conn.simpleQuery("SELECT 1 AS a; SELECT 2 AS b, 3 AS c")
      doAssert results.len == 2
      doAssert results[0].fields.len == 1
      doAssert results[0].rows.len == 1
      doAssert results[0].rows[0][0].get().toString() == "1"
      doAssert results[1].fields.len == 2
      doAssert results[1].rows.len == 1
      doAssert results[1].rows[0][0].get().toString() == "2"
      doAssert results[1].rows[0][1].get().toString() == "3"
      await conn.close()

    waitFor t()

  test "query 10000 rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT g FROM generate_series(1, 10000) AS g")
      doAssert res.rows.len == 10000
      doAssert res.rows[0].getStr(0) == "1"
      doAssert res.rows[9999].getStr(0) == "10000"
      await conn.close()

    waitFor t()

  test "cursor over 10000 rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let cursor = await conn.openCursor(
        "SELECT g FROM generate_series(1, 10000) AS g", chunkSize = 100
      )

      var totalRows = 0
      while true:
        let chunk = await cursor.fetchNext()
        if chunk.len == 0:
          break
        doAssert chunk.len <= 100
        totalRows += chunk.len

      doAssert totalRows == 10000
      doAssert cursor.exhausted
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

suite "E2E: Prepared Statement Edge Cases":
  test "20 parameters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var placeholders: seq[string]
      var params: seq[PgParam]
      for i in 1 .. 20:
        placeholders.add("$" & $i & "::int")
        params.add(toPgParam(int32(i * 10)))
      let sql = "SELECT " & placeholders.join(", ")
      let stmt = await conn.prepare("stmt_20_params", sql)
      let res = await stmt.execute(params)
      doAssert res.rows.len == 1
      for i in 0 ..< 20:
        doAssert res.rows[0].getInt(i) == int32((i + 1) * 10)
      await stmt.close()
      await conn.close()

    waitFor t()

  test "duplicate name raises error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt1 = await conn.prepare("dup_stmt", "SELECT 1")
      var raised = false
      try:
        let stmt2 = await conn.prepare("dup_stmt", "SELECT 2")
        discard stmt2
      except PgError:
        raised = true
      doAssert raised
      # Connection should still be usable
      doAssert conn.state == csReady
      let res = await conn.query("SELECT 42")
      doAssert res.rows[0].getStr(0) == "42"
      await stmt1.close()
      await conn.close()

    waitFor t()

suite "E2E: DSN Connection":
  test "connect via parseDsn":
    proc t() {.async.} =
      let config =
        parseDsn("postgresql://test:test@127.0.0.1:15432/test?sslmode=disable")
      let conn = await connect(config)
      doAssert conn.state == csReady
      let res = await conn.query("SELECT 1 AS val")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "1"
      await conn.close()

    waitFor t()

suite "E2E: JSON and Numeric":
  test "JSON/JSONB as text":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("""SELECT '{"k":"v"}'::json, '{"n":42}'::jsonb""")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "{\"k\":\"v\"}"
      doAssert res.rows[0].getStr(1) == "{\"n\": 42}"
      await conn.close()

    waitFor t()

  test "numeric precision":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT 12345.6789::numeric, 0.00001::numeric, 99999999999999999.12345678901234567890::numeric"
      )
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "12345.6789"
      doAssert $res.rows[0].getNumeric(1) == "0.00001"
      # Precision preserved - float64 would lose digits here
      doAssert $res.rows[0].getNumeric(2) == "99999999999999999.12345678901234567890"
      await conn.close()

    waitFor t()

  test "numeric negative and NaN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT -123.456::numeric, 'NaN'::numeric")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "-123.456"
      doAssert $res.rows[0].getNumeric(1) == "NaN"
      await conn.close()

    waitFor t()

  test "numeric fixed precision":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 1.5::numeric(10,4), 0::numeric(8,2)")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "1.5000"
      doAssert $res.rows[0].getNumeric(1) == "0.00"
      await conn.close()

    waitFor t()

  test "numeric NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::numeric")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getNumericOpt(0) == none(PgNumeric)
      await conn.close()

    waitFor t()

  test "numeric as parameter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_numeric_param")
      discard await conn.exec("CREATE TABLE test_numeric_param (val numeric(20,8))")
      discard await conn.exec(
        "INSERT INTO test_numeric_param VALUES ($1)",
        @[toPgParam(parsePgNumeric("123456789012.56789012"))],
      )
      let res = await conn.query("SELECT val FROM test_numeric_param")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "123456789012.56789012"
      discard await conn.exec("DROP TABLE test_numeric_param")
      await conn.close()

    waitFor t()

  test "numeric large integer":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 99999999999999999999999999999::numeric")
      doAssert res.rows.len == 1
      doAssert $res.rows[0].getNumeric(0) == "99999999999999999999999999999"
      await conn.close()

    waitFor t()

suite "E2E: Connection Edge Cases":
  test "double close is safe":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.state == csReady
      await conn.close()
      doAssert conn.state == csClosed
      await conn.close()
      doAssert conn.state == csClosed

    waitFor t()

  test "operations on closed connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      await conn.close()

      var raised1 = false
      try:
        discard await conn.exec("SELECT 1")
      except PgError:
        raised1 = true
      doAssert raised1

      var raised2 = false
      try:
        discard await conn.query("SELECT 1")
      except PgError:
        raised2 = true
      doAssert raised2

      var raised3 = false
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgError:
        raised3 = true
      doAssert raised3

    waitFor t()

suite "E2E: Binary Format":
  test "binary results for int types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT 42::int2, 123456::int4, 9999999999::int8", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      let row = qr.rows[0]
      doAssert row.getInt(0) == 42'i32 # int2 promoted via getInt
      doAssert row.getInt(1) == 123456'i32
      doAssert row.getInt64(2) == 9999999999'i64
      # getInt64 should also work on int2/int4 columns (promotion)
      doAssert row.getInt64(0) == 42'i64
      doAssert row.getInt64(1) == 123456'i64
      await conn.close()

    waitFor t()

  test "binary results for float types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr =
        await conn.query("SELECT 3.14::float8, 1.5::float4", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let row = qr.rows[0]
      doAssert abs(row.getFloat(0) - 3.14) < 1e-10
      doAssert abs(row.getFloat(1) - 1.5) < 1e-5
      await conn.close()

    waitFor t()

  test "binary results for bool":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT true, false", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBool(0) == true
      doAssert qr.rows[0].getBool(1) == false
      await conn.close()

    waitFor t()

  test "binary results for text":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT 'hello'::text, 'world'::varchar", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getStr(0) == "hello"
      doAssert qr.rows[0].getStr(1) == "world"
      await conn.close()

    waitFor t()

  test "binary results for bytea":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT '\\xDEADBEEF'::bytea", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBytes(0) == @[0xDE'u8, 0xAD, 0xBE, 0xEF]
      await conn.close()

    waitFor t()

  test "binary results for timestamp":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT '2024-01-15 10:30:00'::timestamp", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      let dt = qr.rows[0].getTimestamp(0)
      doAssert dt.year == 2024
      doAssert dt.month == mJan
      doAssert dt.monthday == 15
      doAssert dt.hour == 10
      doAssert dt.minute == 30
      await conn.close()

    waitFor t()

  test "binary results for date":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT '2024-01-15'::date", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let dt = qr.rows[0].getDate(0)
      doAssert dt.year == 2024
      doAssert dt.month == mJan
      doAssert dt.monthday == 15
      await conn.close()

    waitFor t()

  test "binary results for uuid":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid", resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      let data = qr.rows[0].getBytes(0)
      doAssert data.len == 16
      doAssert data[0] == 0x55'u8
      doAssert data[1] == 0x0e'u8
      await conn.close()

    waitFor t()

  test "binary params and binary results roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[
        toPgBinaryParam(42'i32), toPgBinaryParam(9999999999'i64), toPgBinaryParam(true)
      ]
      let qr = await conn.query(
        "SELECT $1::int4, $2::int8, $3::bool", params, resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      doAssert qr.rows[0].getInt64(1) == 9999999999'i64
      doAssert qr.rows[0].getBool(2) == true
      await conn.close()

    waitFor t()

  test "binary params with text results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[toPgBinaryParam(42'i32)]
      let qr = await conn.query("SELECT $1::int4", params)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      await conn.close()

    waitFor t()

  test "text params with binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[toPgParam(42'i32)]
      let qr = await conn.query("SELECT $1::int4", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      await conn.close()

    waitFor t()

  test "NULL handling in binary mode":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr =
        await conn.query("SELECT NULL::int4, NULL::text", resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].isNull(0)
      doAssert qr.rows[0].isNull(1)
      await conn.close()

    waitFor t()

  test "prepared statement with binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt = await conn.prepare("bin_stmt", "SELECT $1::int4 + 10")
      let qr = await stmt.execute(@[toPgBinaryParam(32'i32)], resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0) == 42'i32
      await stmt.close()
      await conn.close()

    waitFor t()

  test "binary timestamp param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
      let params = @[toPgBinaryParam(dt)]
      let qr = await conn.query("SELECT $1::timestamp", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      let result = qr.rows[0].getTimestamp(0)
      doAssert result.year == 2024
      doAssert result.month == mJan
      doAssert result.monthday == 15
      doAssert result.hour == 10
      doAssert result.minute == 30
      await conn.close()

    waitFor t()

  test "binary float roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let params = @[toPgBinaryParam(3.14159265358979)]
      let qr = await conn.query("SELECT $1::float8", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert abs(qr.rows[0].getFloat(0) - 3.14159265358979) < 1e-14
      await conn.close()

    waitFor t()

  test "binary bytea param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let data = @[0xDE'u8, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]
      let params = @[toPgBinaryParam(data)]
      let qr = await conn.query("SELECT $1::bytea", params, resultFormat = rfBinary)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBytes(0) == data
      await conn.close()

    waitFor t()

suite "E2E: recvMessage Timeout":
  test "recvMessage with timeout succeeds on immediate response":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Send a simple query and receive with timeout
      await conn.sendMsg(encodeQuery("SELECT 1"))
      var gotRowData = false
      var gotComplete = false
      while true:
        let msg = await conn.recvMessage(timeout = seconds(5))
        case msg.kind
        of bmkRowDescription:
          discard
        of bmkDataRow:
          gotRowData = true
        of bmkCommandComplete:
          gotComplete = true
        of bmkReadyForQuery:
          break
        else:
          discard
      doAssert gotRowData
      doAssert gotComplete
      await conn.close()

    waitFor t()

  test "recvMessage with timeout raises AsyncTimeoutError when no data":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Don't send anything — recvMessage should timeout waiting for data
      var raised = false
      try:
        discard await conn.recvMessage(timeout = milliseconds(100))
      except AsyncTimeoutError:
        raised = true
      doAssert raised

    waitFor t()

  test "recvMessage buffer restored after timeout":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Logical unconsumed size (compactRecvBuf may shrink the raw buffer)
      let unconsumedBefore = conn.recvBuf.len - conn.recvBufStart
      # Trigger a timeout with no pending server data
      try:
        discard await conn.recvMessage(timeout = milliseconds(100))
      except AsyncTimeoutError:
        discard
      # recvBuf must not grow from the failed read
      let unconsumedAfter = conn.recvBuf.len - conn.recvBufStart
      doAssert unconsumedAfter == unconsumedBefore
      await conn.close()

    waitFor t()

  test "recvMessage timeout on large result set":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Start a query that produces a large result, then timeout mid-stream
      await conn.sendMsg(encodeQuery("SELECT generate_series(1, 100000)"))
      var raised = false
      try:
        # Very short timeout — unlikely to receive all messages in time
        while true:
          discard await conn.recvMessage(timeout = milliseconds(1))
      except AsyncTimeoutError:
        raised = true
      doAssert raised

    waitFor t()

  test "recvMessage without timeout (default) does not raise on normal message":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      await conn.sendMsg(encodeQuery("SELECT 42"))
      var value = ""
      while true:
        let msg = await conn.recvMessage() # default: no timeout
        case msg.kind
        of bmkDataRow:
          if msg.columns.len > 0 and msg.columns[0].isSome:
            value = cast[string](msg.columns[0].get())
        of bmkReadyForQuery:
          break
        else:
          discard
      doAssert value == "42"
      await conn.close()

    waitFor t()

suite "E2E: Notification Buffering":
  test "notifications buffered without callback":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      # No callback set — notifications should still be buffered
      await listener.listen("buf_nocb")

      await sender.notify("buf_nocb", "msg1")
      await sender.notify("buf_nocb", "msg2")
      await sleepAsync(milliseconds(200))

      doAssert listener.notifyQueue.len == 2
      let n1 = listener.notifyQueue.popFirst()
      doAssert n1.channel == "buf_nocb"
      doAssert n1.payload == "msg1"
      let n2 = listener.notifyQueue.popFirst()
      doAssert n2.channel == "buf_nocb"
      doAssert n2.payload == "msg2"

      await listener.unlisten("buf_nocb")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notifications buffered alongside callback":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var cbReceived: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          cbReceived.add(n)
      )
      await listener.listen("buf_both")

      await sender.notify("buf_both", "hello")
      await sleepAsync(milliseconds(200))

      # Both callback and queue should have the notification
      doAssert cbReceived.len == 1
      doAssert listener.notifyQueue.len == 1
      doAssert listener.notifyQueue.peekFirst().payload == "hello"

      await listener.unlisten("buf_both")
      await listener.close()
      await sender.close()

    waitFor t()

  test "buffer respects max queue size and tracks drops":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      listener.notifyMaxQueue = 3
      await listener.listen("buf_max")

      for i in 1 .. 5:
        await sender.notify("buf_max", $i)
      await sleepAsync(milliseconds(300))

      # Only last 3 should remain (oldest dropped)
      doAssert listener.notifyQueue.len == 3
      doAssert listener.notifyDropped == 2

      # waitNotification should raise PgNotifyOverflowError
      var caught = false
      try:
        discard await listener.waitNotification()
      except PgNotifyOverflowError as e:
        caught = true
        doAssert e.dropped == 2
      doAssert caught

      # After overflow is cleared, normal access works
      doAssert listener.notifyDropped == 0
      let n = await listener.waitNotification()
      doAssert n.payload == "3"
      doAssert listener.notifyQueue.popFirst().payload == "4"
      doAssert listener.notifyQueue.popFirst().payload == "5"

      await listener.unlisten("buf_max")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notifyOverflowCallback fires on drop":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      listener.notifyMaxQueue = 2
      var cbDropped = 0
      listener.notifyOverflowCallback = proc(dropped: int) {.gcsafe, raises: [].} =
        cbDropped += dropped

      await listener.listen("buf_cb")

      for i in 1 .. 4:
        await sender.notify("buf_cb", $i)
      await sleepAsync(milliseconds(300))

      # 4 notifications into queue of 2: 2 dropped
      doAssert cbDropped == 2
      doAssert listener.notifyDropped == 2

      await listener.unlisten("buf_cb")
      await listener.close()
      await sender.close()

    waitFor t()

suite "E2E: waitNotification":
  test "waitNotification returns buffered notification immediately":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      await listener.listen("wait_imm")

      await sender.notify("wait_imm", "instant")
      await sleepAsync(milliseconds(200))

      let notif = await listener.waitNotification()
      doAssert notif.channel == "wait_imm"
      doAssert notif.payload == "instant"
      doAssert listener.notifyQueue.len == 0

      await listener.unlisten("wait_imm")
      await listener.close()
      await sender.close()

    waitFor t()

  test "waitNotification blocks until notification arrives":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      await listener.listen("wait_block")

      # Start waiting before notification is sent
      let waitFut = listener.waitNotification()

      await sleepAsync(milliseconds(50))
      doAssert not waitFut.finished

      await sender.notify("wait_block", "delayed")
      await sleepAsync(milliseconds(200))

      doAssert waitFut.finished
      let notif = await waitFut
      doAssert notif.channel == "wait_block"
      doAssert notif.payload == "delayed"

      await listener.unlisten("wait_block")
      await listener.close()
      await sender.close()

    waitFor t()

  test "waitNotification with timeout raises on expiry":
    proc t() {.async.} =
      let listener = await connect(plainConfig())

      await listener.listen("wait_timeout")

      var raised = false
      try:
        discard await listener.waitNotification(timeout = milliseconds(100))
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg
      doAssert raised

      await listener.unlisten("wait_timeout")
      await listener.close()

    waitFor t()

  test "waitNotification drains queue in order":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      await listener.listen("wait_order")

      await sender.notify("wait_order", "first")
      await sender.notify("wait_order", "second")
      await sender.notify("wait_order", "third")
      await sleepAsync(milliseconds(200))

      let n1 = await listener.waitNotification()
      let n2 = await listener.waitNotification()
      let n3 = await listener.waitNotification()
      doAssert n1.payload == "first"
      doAssert n2.payload == "second"
      doAssert n3.payload == "third"

      await listener.unlisten("wait_order")
      await listener.close()
      await sender.close()

    waitFor t()

when hasChronos:
  suite "E2E: LISTEN/NOTIFY Auto-Reconnect":
    test "reconnectInPlace restores connection and re-subscribes":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        let sender = await connect(plainConfig())

        var received: seq[Notification]
        listener.onNotify(
          proc(n: Notification) {.gcsafe, raises: [].} =
            received.add(n)
        )
        await listener.listen("reconn_manual")

        # Send before reconnect
        await sender.notify("reconn_manual", "before")
        await sleepAsync(milliseconds(200))
        doAssert received.len == 1

        # Force reconnect
        await listener.stopListening()
        await listener.reconnectInPlace()
        doAssert listener.state == csReady
        doAssert sets.contains(listener.listenChannels, "reconn_manual")

        # Channel was re-LISTENed by reconnectInPlace, start pump again
        listener.state = csListening
        listener.listenTask = listener.listenPump()

        await sender.notify("reconn_manual", "after")
        await sleepAsync(milliseconds(200))
        doAssert received.len == 2
        doAssert received[1].payload == "after"

        await listener.close()
        await sender.close()

      waitFor t()

    test "reconnectCallback is invoked on auto-reconnect":
      proc t() {.async.} =
        let listener = await connect(plainConfig())

        var reconnected = false
        listener.reconnectCallback = proc() {.gcsafe, raises: [].} =
          reconnected = true

        await listener.listen("reconn_cb")

        # Kill the connection from the server side
        let killer = await connect(plainConfig())
        let pidStr = $listener.pid
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[some(pidStr.toBytes())]
          )
        except PgError:
          discard
        await killer.close()

        # Wait for reconnect (backoff starts at 1s)
        await sleepAsync(milliseconds(3000))

        doAssert reconnected
        doAssert listener.state == csListening

        # Verify notifications still work after reconnect
        let sender = await connect(plainConfig())
        var received: seq[Notification]
        listener.onNotify(
          proc(n: Notification) {.gcsafe, raises: [].} =
            received.add(n)
        )

        await sender.notify("reconn_cb", "after_reconnect")
        await sleepAsync(milliseconds(200))
        doAssert received.len == 1
        doAssert received[0].payload == "after_reconnect"

        await listener.close()
        await sender.close()

      waitFor t()

    test "close during reconnect does not hang":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        await listener.listen("reconn_close")

        # Kill the connection
        let killer = await connect(plainConfig())
        let pidStr = $listener.pid
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[some(pidStr.toBytes())]
          )
        except PgError:
          discard
        await killer.close()

        # Give pump time to detect the failure and start reconnecting
        await sleepAsync(milliseconds(500))

        # Close should not hang even if reconnect is in progress
        await listener.close().wait(seconds(5))
        doAssert listener.state == csClosed

      waitFor t()

    test "waitNotification fails on close":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        await listener.listen("wait_close")

        let waitFut = listener.waitNotification()
        await sleepAsync(milliseconds(50))
        doAssert not waitFut.finished

        await listener.close()

        var raised = false
        try:
          discard await waitFut
        except PgError:
          raised = true
        doAssert raised

      waitFor t()

suite "E2E: Array Types":
  test "int4 array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::int4[]", @[toPgParam(@[1'i32, 2, 3])])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArray(0) == @[1'i32, 2, 3]
      await conn.close()

    waitFor t()

  test "int8 array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT $1::int8[]", @[toPgParam(@[9999999999'i64, -1'i64])])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt64Array(0) == @[9999999999'i64, -1'i64]
      await conn.close()

    waitFor t()

  test "bool array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT $1::bool[]", @[toPgParam(@[true, false, true])])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBoolArray(0) == @[true, false, true]
      await conn.close()

    waitFor t()

  test "text array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::text[]", @[toPgParam(@["hello", "world", "foo bar"])]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStrArray(0) == @["hello", "world", "foo bar"]
      await conn.close()

    waitFor t()

  test "text array with special characters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT $1::text[]", @[toPgParam(@["a\"b", "c\\d", "e,f", ""])]
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStrArray(0) == @["a\"b", "c\\d", "e,f", ""]
      await conn.close()

    waitFor t()

  test "empty array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::int4[]", @[toPgParam(newSeq[int32]())])
      doAssert res.rows.len == 1
      doAssert res.rows[0].getIntArray(0).len == 0
      await conn.close()

    waitFor t()

  test "float8 array roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::float8[]", @[toPgParam(@[3.14, 2.72])])
      doAssert res.rows.len == 1
      let arr = res.rows[0].getFloatArray(0)
      doAssert abs(arr[0] - 3.14) < 1e-10
      doAssert abs(arr[1] - 2.72) < 1e-10
      await conn.close()

    waitFor t()

  test "NULL array column":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT $1::int4[]", @[toPgParam(none(seq[int32]))])
      doAssert res.rows.len == 1
      doAssert res.rows[0].isNull(0)
      doAssert res.rows[0].getIntArrayOpt(0).isNone
      await conn.close()

    waitFor t()

suite "E2E: COPY IN Stream":
  test "copyInStream basic streaming":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyin_stream")
      discard await conn.exec("CREATE TABLE test_copyin_stream (id int, name text)")

      var idx = 0
      let rows =
        @["1\tAlice\n".toBytes(), "2\tBob\n".toBytes(), "3\tCharlie\n".toBytes()]
      let cb = makeCopyInCallback:
        if idx < rows.len:
          let chunk = rows[idx]
          inc idx
          chunk
        else:
          newSeq[byte]()

      let info = await conn.copyInStream("COPY test_copyin_stream FROM STDIN", cb)
      doAssert "COPY 3" in info.commandTag
      doAssert info.format == cfText
      doAssert conn.state == csReady

      let res = await conn.query("SELECT id, name FROM test_copyin_stream ORDER BY id")
      doAssert res.rows.len == 3
      doAssert res.rows[0].getStr(1) == "Alice"
      doAssert res.rows[2].getStr(1) == "Charlie"

      discard await conn.exec("DROP TABLE test_copyin_stream")
      await conn.close()

    waitFor t()

  test "copyInStream empty data (immediate EOF)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyin_empty")
      discard await conn.exec("CREATE TABLE test_copyin_empty (id int)")

      let cb = makeCopyInCallback:
        newSeq[byte]()

      let info = await conn.copyInStream("COPY test_copyin_empty FROM STDIN", cb)
      doAssert "COPY 0" in info.commandTag
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_copyin_empty")
      await conn.close()

    waitFor t()

  test "copyInStream callback error sends CopyFail":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyin_fail")
      discard await conn.exec("CREATE TABLE test_copyin_fail (id int)")

      var callCount = 0
      let cb = makeCopyInCallback:
        inc callCount
        if callCount == 1:
          "1\n".toBytes()
        else:
          raise newException(CatchableError, "callback failed")
          newSeq[byte]()

      var raised = false
      try:
        discard await conn.copyInStream("COPY test_copyin_fail FROM STDIN", cb)
      except CatchableError as e:
        raised = true
        doAssert "callback failed" in e.msg
      doAssert raised
      doAssert conn.state == csReady

      # Connection should still be usable
      let res = await conn.query("SELECT count(*) FROM test_copyin_fail")
      doAssert res.rows[0].getStr(0) == "0" # CopyFail aborted the COPY

      discard await conn.exec("DROP TABLE test_copyin_fail")
      await conn.close()

    waitFor t()

  test "copyInStream invalid SQL raises PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let cb = makeCopyInCallback:
        newSeq[byte]()

      var raised = false
      try:
        discard await conn.copyInStream("COPY nonexistent_table FROM STDIN", cb)
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady

      # Connection should still be usable
      let res = await conn.simpleQuery("SELECT 1")
      doAssert res[0].rows[0][0].get().toString() == "1"

      await conn.close()

    waitFor t()

  test "copyInStream large data (10000 rows)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyin_large")
      discard await conn.exec("CREATE TABLE test_copyin_large (id int, val text)")

      var idx = 0
      let cb = makeCopyInCallback:
        if idx < 10000:
          let row = ($idx & "\trow_" & $idx & "\n").toBytes()
          inc idx
          row
        else:
          newSeq[byte]()

      let info = await conn.copyInStream("COPY test_copyin_large FROM STDIN", cb)
      doAssert "COPY 10000" in info.commandTag
      doAssert conn.state == csReady

      let res = await conn.query("SELECT count(*) FROM test_copyin_large")
      doAssert res.rows[0].getStr(0) == "10000"

      discard await conn.exec("DROP TABLE test_copyin_large")
      await conn.close()

    waitFor t()

  test "copyInStream format info in CopyInInfo":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyin_info")
      discard await conn.exec("CREATE TABLE test_copyin_info (id int, name text)")

      let cb = makeCopyInCallback:
        newSeq[byte]()

      let info = await conn.copyInStream("COPY test_copyin_info FROM STDIN", cb)
      doAssert info.format == cfText
      doAssert info.columnFormats.len == 2
      doAssert info.columnFormats[0] == 0'i16 # text format
      doAssert info.columnFormats[1] == 0'i16

      discard await conn.exec("DROP TABLE test_copyin_info")
      await conn.close()

    waitFor t()

suite "E2E: COPY IN openArray[byte]":
  test "copyIn with openArray[byte] inserts rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_raw")
      discard await conn.exec("CREATE TABLE test_copy_raw (id int, name text)")

      let data = "1\tAlice\n2\tBob\n3\tCharlie\n"
      let tag = await conn.copyIn(
        "COPY test_copy_raw FROM STDIN", data.toOpenArrayByte(0, data.high)
      )
      doAssert "COPY 3" in tag

      let res = await conn.query("SELECT id, name FROM test_copy_raw ORDER BY id")
      doAssert res.rows.len == 3
      doAssert res.rows[0].getStr(1) == "Alice"
      doAssert res.rows[2].getStr(1) == "Charlie"

      discard await conn.exec("DROP TABLE test_copy_raw")
      await conn.close()

    waitFor t()

  test "copyIn with openArray[byte] empty data":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_raw_empty")
      discard await conn.exec("CREATE TABLE test_copy_raw_empty (id int, name text)")

      let empty: seq[byte] = @[]
      let tag = await conn.copyIn("COPY test_copy_raw_empty FROM STDIN", empty)
      doAssert "COPY 0" in tag

      discard await conn.exec("DROP TABLE test_copy_raw_empty")
      await conn.close()

    waitFor t()

  test "copyIn with openArray[byte] large data":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_raw_large")
      discard await conn.exec("CREATE TABLE test_copy_raw_large (id int, val text)")

      var data = ""
      for i in 0 ..< 10000:
        data.add($i & "\trow" & $i & "\n")
      let tag = await conn.copyIn(
        "COPY test_copy_raw_large FROM STDIN", data.toOpenArrayByte(0, data.high)
      )
      doAssert "COPY 10000" in tag

      let res = await conn.query("SELECT count(*) FROM test_copy_raw_large")
      doAssert res.rows[0].getStr(0) == "10000"

      discard await conn.exec("DROP TABLE test_copy_raw_large")
      await conn.close()

    waitFor t()

suite "E2E: Binary COPY IN":
  test "binary copyIn with int, float, text, bool, NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_bin")
      discard await conn.exec(
        """
        CREATE TABLE test_copy_bin (
          id int,
          val double precision,
          name text,
          flag boolean
        )
      """
      )

      var buf: seq[byte]
      buf.addCopyBinaryHeader()
      # Row 1: all values
      buf.addCopyTupleStart(4)
      buf.addCopyFieldInt32(1'i32)
      buf.addCopyFieldFloat64(3.14)
      buf.addCopyFieldString("hello")
      buf.addCopyFieldBool(true)
      # Row 2: with NULL
      buf.addCopyTupleStart(4)
      buf.addCopyFieldInt32(2'i32)
      buf.addCopyFieldNull()
      buf.addCopyFieldString("world")
      buf.addCopyFieldBool(false)
      # Row 3
      buf.addCopyTupleStart(4)
      buf.addCopyFieldInt32(3'i32)
      buf.addCopyFieldFloat64(-1.5)
      buf.addCopyFieldText("bytes".toBytes())
      buf.addCopyFieldBool(true)
      buf.addCopyBinaryTrailer()

      let tag =
        await conn.copyIn("COPY test_copy_bin FROM STDIN WITH (FORMAT binary)", buf)
      doAssert "COPY 3" in tag

      let res =
        await conn.query("SELECT id, val, name, flag FROM test_copy_bin ORDER BY id")
      doAssert res.rows.len == 3
      doAssert res.rows[0].getStr(0) == "1"
      doAssert res.rows[0].getStr(2) == "hello"
      doAssert res.rows[0].getStr(3) == "t"
      # Row 2: NULL val
      doAssert res.rows[1].getStr(0) == "2"
      doAssert res.rows[1].isNull(1) == true
      doAssert res.rows[1].getStr(2) == "world"
      doAssert res.rows[1].getStr(3) == "f"
      # Row 3
      doAssert res.rows[2].getStr(0) == "3"
      doAssert res.rows[2].getStr(2) == "bytes"

      discard await conn.exec("DROP TABLE test_copy_bin")
      await conn.close()

    waitFor t()

  test "binary copyIn with int16 and int64 fields":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_bin_ints")
      discard await conn.exec(
        """
        CREATE TABLE test_copy_bin_ints (
          a smallint,
          b bigint
        )
      """
      )

      var buf: seq[byte]
      buf.addCopyBinaryHeader()
      buf.addCopyTupleStart(2)
      buf.addCopyFieldInt16(42'i16)
      buf.addCopyFieldInt64(9_000_000_000'i64)
      buf.addCopyTupleStart(2)
      buf.addCopyFieldInt16(-1'i16)
      buf.addCopyFieldInt64(0'i64)
      buf.addCopyBinaryTrailer()

      let tag = await conn.copyIn(
        "COPY test_copy_bin_ints FROM STDIN WITH (FORMAT binary)", buf
      )
      doAssert "COPY 2" in tag

      let res = await conn.query("SELECT a, b FROM test_copy_bin_ints ORDER BY a")
      doAssert res.rows.len == 2
      doAssert res.rows[0].getStr(0) == "-1"
      doAssert res.rows[0].getStr(1) == "0"
      doAssert res.rows[1].getStr(0) == "42"
      doAssert res.rows[1].getStr(1) == "9000000000"

      discard await conn.exec("DROP TABLE test_copy_bin_ints")
      await conn.close()

    waitFor t()

  test "binary copyIn with float32":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_bin_f32")
      discard await conn.exec("CREATE TABLE test_copy_bin_f32 (val real)")

      var buf: seq[byte]
      buf.addCopyBinaryHeader()
      buf.addCopyTupleStart(1)
      buf.addCopyFieldFloat32(1.5'f32)
      buf.addCopyTupleStart(1)
      buf.addCopyFieldFloat32(-0.25'f32)
      buf.addCopyBinaryTrailer()

      let tag =
        await conn.copyIn("COPY test_copy_bin_f32 FROM STDIN WITH (FORMAT binary)", buf)
      doAssert "COPY 2" in tag

      let res = await conn.query("SELECT val FROM test_copy_bin_f32 ORDER BY val")
      doAssert res.rows.len == 2
      doAssert res.rows[0].getStr(0) == "-0.25"
      doAssert res.rows[1].getStr(0) == "1.5"

      discard await conn.exec("DROP TABLE test_copy_bin_f32")
      await conn.close()

    waitFor t()

suite "E2E: Column Name Access":
  test "columnIndex on QueryResult":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 42::int4 AS id, 'alice'::text AS name")
      doAssert res.columnIndex("id") == 0
      doAssert res.columnIndex("name") == 1
      doAssert res.rows[0].getInt(res.columnIndex("id")) == 42'i32
      doAssert res.rows[0].getStr(res.columnIndex("name")) == "alice"
      await conn.close()

    waitFor t()

  test "columnMap for repeated access":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT 1::int4 AS a, 'x'::text AS b UNION ALL SELECT 2, 'y'")
      let cols = res.fields.columnMap()
      doAssert res.rows[0].getInt(cols["a"]) == 1'i32
      doAssert res.rows[0].getStr(cols["b"]) == "x"
      doAssert res.rows[1].getInt(cols["a"]) == 2'i32
      doAssert res.rows[1].getStr(cols["b"]) == "y"
      await conn.close()

    waitFor t()

  test "columnIndex on PreparedStatement":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt =
        await conn.prepare("col_idx_stmt", "SELECT $1::int4 AS val, $2::text AS label")
      doAssert stmt.columnIndex("val") == 0
      doAssert stmt.columnIndex("label") == 1
      let res = await stmt.execute(@[toPgParam(99'i32), toPgParam("test")])
      doAssert res.rows[0].getInt(stmt.columnIndex("val")) == 99'i32
      doAssert res.rows[0].getStr(stmt.columnIndex("label")) == "test"
      await stmt.close()
      await conn.close()

    waitFor t()

  test "columnIndex raises for missing column":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 1 AS x")
      var raised = false
      try:
        discard res.columnIndex("nonexistent")
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "name-based row accessors via rows()":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT 42::int4 AS id, 'alice'::text AS name, true::bool AS active, 3.14::float8 AS score"
      )
      doAssert res.rows.len == 1
      let row = res.rows[0]
      doAssert row.getInt("id") == 42'i32
      doAssert row.getStr("name") == "alice"
      doAssert row.getBool("active") == true
      doAssert abs(row.getFloat("score") - 3.14) < 0.001
      doAssert row.isNull("name") == false
      await conn.close()

    waitFor t()

  test "name-based row accessors via items iterator":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT 1::int4 AS v UNION ALL SELECT 2 UNION ALL SELECT 3")
      var vals: seq[int32]
      for row in res:
        vals.add(row.getInt("v"))
      doAssert vals == @[1'i32, 2'i32, 3'i32]
      await conn.close()

    waitFor t()

  test "name-based Opt accessors":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 10::int4 AS a, NULL::text AS b")
      let row = res.rows[0]
      doAssert row.getIntOpt("a") == some(10'i32)
      doAssert row.getStrOpt("b").isNone
      await conn.close()

    waitFor t()

  test "name-based queryOne accessors":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let rowOpt = await conn.queryOne("SELECT 99::int8 AS big, 'hello'::text AS msg")
      doAssert rowOpt.isSome
      let row = rowOpt.get
      doAssert row.getInt64("big") == 99'i64
      doAssert row.getStr("msg") == "hello"
      await conn.close()

    waitFor t()

  test "name-based accessor raises on missing column":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 1::int4 AS x")
      let row = res.rows[0]
      var raised = false
      try:
        discard row.getStr("nonexistent")
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "name-based accessor raises without field metadata":
    proc t() {.async.} =
      # Row constructed manually without fields
      let row: Row = @[some(@[byte(49)])]
      var raised = false
      try:
        discard row.getStr("x")
      except PgTypeError:
        raised = true
      doAssert raised

    waitFor t()

suite "E2E: Convenience Query Methods":
  test "queryOne returns first row":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row = await conn.queryOne("SELECT 1 AS a, 'hello' AS b")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "1"
      doAssert row.get.getStr(1) == "hello"
      await conn.close()

    waitFor t()

  test "queryOne returns none for empty result":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row = await conn.queryOne("SELECT 1 WHERE false")
      doAssert row.isNone
      await conn.close()

    waitFor t()

  test "queryValue returns scalar":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValue("SELECT 42")
      doAssert val == "42"
      await conn.close()

    waitFor t()

  test "queryValue raises on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue("SELECT 1 WHERE false")
      except PgError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValue raises on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue("SELECT NULL::text")
      except PgError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValueOrDefault returns value":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOrDefault("SELECT 'yes'")
      doAssert val == "yes"
      await conn.close()

    waitFor t()

  test "queryValueOrDefault returns default on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOrDefault("SELECT 1 WHERE false", default = "nope")
      doAssert val == "nope"
      await conn.close()

    waitFor t()

  test "queryValueOrDefault returns default on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val =
        await conn.queryValueOrDefault("SELECT NULL::text", default = "fallback")
      doAssert val == "fallback"
      await conn.close()

    waitFor t()

  test "queryValue with typedesc returns typed value":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValue(int64, "SELECT 42")
      doAssert val == 42'i64
      let fval = await conn.queryValue(float64, "SELECT 3.14::float8")
      doAssert abs(fval - 3.14) < 0.001
      let bval = await conn.queryValue(bool, "SELECT true")
      doAssert bval == true
      let sval = await conn.queryValue(string, "SELECT 'hello'")
      doAssert sval == "hello"
      await conn.close()

    waitFor t()

  test "queryValue with typedesc raises on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue(int32, "SELECT 1 WHERE false")
      except PgError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValue with typedesc raises on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryValue(int64, "SELECT NULL::int8")
      except PgError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryValueOrDefault with typedesc":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val =
        await conn.queryValueOrDefault(int64, "SELECT 1 WHERE false", default = -1'i64)
      doAssert val == -1'i64
      let val2 = await conn.queryValueOrDefault(int64, "SELECT 99", default = 0'i64)
      doAssert val2 == 99'i64
      await conn.close()

    waitFor t()

  test "queryValueOpt returns some on value":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt("SELECT 'hello'")
      doAssert val == some("hello")
      await conn.close()

    waitFor t()

  test "queryValueOpt returns none on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt("SELECT 1 WHERE false")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryValueOpt returns none on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt("SELECT NULL::text")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryValueOpt with typedesc returns some":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt(int64, "SELECT 42")
      doAssert val == some(42'i64)
      await conn.close()

    waitFor t()

  test "queryValueOpt with typedesc returns none on no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt(int32, "SELECT 1 WHERE false")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryValueOpt with typedesc returns none on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let val = await conn.queryValueOpt(int64, "SELECT NULL::int8")
      doAssert val.isNone
      await conn.close()

    waitFor t()

  test "queryExists returns true when rows exist":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let exists = await conn.queryExists("SELECT 1")
      doAssert exists
      await conn.close()

    waitFor t()

  test "queryExists returns false when no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let exists = await conn.queryExists("SELECT 1 WHERE false")
      doAssert not exists
      await conn.close()

    waitFor t()

  test "execAffected returns affected row count":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE ea_test (id int)")
      discard await conn.exec("INSERT INTO ea_test VALUES (1), (2), (3)")
      let n = await conn.execAffected("DELETE FROM ea_test WHERE id > 1")
      doAssert n == 2
      await conn.close()

    waitFor t()

  test "queryColumn returns column values":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let vals = await conn.queryColumn("SELECT generate_series(1,3)::text")
      doAssert vals == @["1", "2", "3"]
      await conn.close()

    waitFor t()

  test "queryColumn raises on NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.queryColumn("SELECT NULL::text")
      except PgTypeError:
        raised = true
      doAssert raised
      await conn.close()

    waitFor t()

  test "queryOne returns only first row from multiple":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row = await conn.queryOne("SELECT generate_series(10,12)::text AS v")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "10"
      await conn.close()

    waitFor t()

  test "queryColumn returns empty seq for no rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let vals = await conn.queryColumn("SELECT 1::text WHERE false")
      doAssert vals.len == 0
      await conn.close()

    waitFor t()

  test "execAffected returns 0 when no rows affected":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE ea_zero (id int)")
      let n = await conn.execAffected("DELETE FROM ea_zero WHERE id = 999")
      doAssert n == 0
      await conn.close()

    waitFor t()

  test "queryOne with params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let row =
        await conn.queryOne("SELECT $1::int + $2::int", @[3.toPgParam, 4.toPgParam])
      doAssert row.isSome
      doAssert row.get.getStr(0) == "7"
      await conn.close()

    waitFor t()

  test "pool queryOne":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let row = await pool.queryOne("SELECT 'pooled'")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "pooled"
      await pool.close()

    waitFor t()

  test "pool queryValue":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValue("SELECT 99")
      doAssert val == "99"
      await pool.close()

    waitFor t()

  test "pool queryExists":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      doAssert (await pool.queryExists("SELECT 1"))
      doAssert not (await pool.queryExists("SELECT 1 WHERE false"))
      await pool.close()

    waitFor t()

  test "pool execAffected":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("CREATE TEMP TABLE pool_ea2 (id int)")
      discard await conn.exec("INSERT INTO pool_ea2 VALUES (1), (2)")
      let n = await conn.execAffected("DELETE FROM pool_ea2")
      doAssert n == 2
      await conn.close()

    waitFor t()

  test "pool queryColumn":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let vals = await pool.queryColumn("SELECT generate_series(10,12)::text")
      doAssert vals == @["10", "11", "12"]
      await pool.close()

    waitFor t()

  test "pool queryValueOrDefault":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValueOrDefault("SELECT 1 WHERE false", default = "x")
      doAssert val == "x"
      let val2 = await pool.queryValueOrDefault("SELECT 'ok'")
      doAssert val2 == "ok"
      await pool.close()

    waitFor t()

  test "pool queryValue with typedesc":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValue(int64, "SELECT 123")
      doAssert val == 123'i64
      await pool.close()

    waitFor t()

  test "pool queryValueOrDefault with typedesc":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val =
        await pool.queryValueOrDefault(int32, "SELECT 1 WHERE false", default = -1'i32)
      doAssert val == -1'i32
      let val2 = await pool.queryValueOrDefault(int32, "SELECT 7", default = 0'i32)
      doAssert val2 == 7'i32
      await pool.close()

    waitFor t()

  test "pool queryValueOpt":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValueOpt("SELECT 'ok'")
      doAssert val == some("ok")
      let none_val = await pool.queryValueOpt("SELECT 1 WHERE false")
      doAssert none_val.isNone
      await pool.close()

    waitFor t()

  test "pool queryValueOpt with typedesc":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      let val = await pool.queryValueOpt(int64, "SELECT 123")
      doAssert val == some(123'i64)
      let none_val = await pool.queryValueOpt(int32, "SELECT 1 WHERE false")
      doAssert none_val.isNone
      await pool.close()

    waitFor t()

  test "stmt cache: repeated query uses cache":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.stmtCacheCapacity == 256
      doAssert conn.stmtCache.len == 0

      # First call: cache miss -> populates cache
      let r1 = await conn.query("SELECT 1 AS v")
      doAssert r1.rows[0].getStr(0) == "1"
      doAssert conn.stmtCache.len == 1

      # Second call: cache hit
      let r2 = await conn.query("SELECT 1 AS v")
      doAssert r2.rows[0].getStr(0) == "1"
      doAssert conn.stmtCache.len == 1 # no new entry

      await conn.close()

    waitFor t()

  test "stmt cache: repeated exec uses cache":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      discard await conn.exec("SELECT 1")
      doAssert conn.stmtCache.len == 1

      discard await conn.exec("SELECT 1")
      doAssert conn.stmtCache.len == 1

      # Different SQL gets its own entry
      discard await conn.exec("SELECT 2")
      doAssert conn.stmtCache.len == 2

      await conn.close()

    waitFor t()

  test "stmt cache: query with params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query(
        "SELECT $1::int + $2::int AS sum", @[some(@[byte('1')]), some(@[byte('2')])]
      )
      doAssert r1.rows[0].getStr(0) == "3"
      doAssert conn.stmtCache.len == 1

      # Same SQL, different params: cache hit
      let r2 = await conn.query(
        "SELECT $1::int + $2::int AS sum", @[some(@[byte('3')]), some(@[byte('4')])]
      )
      doAssert r2.rows[0].getStr(0) == "7"
      doAssert conn.stmtCache.len == 1

      await conn.close()

    waitFor t()

  test "stmt cache: binary result format works with cache":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let r1 = await conn.query("SELECT 42::int4", resultFormat = rfBinary)
      doAssert r1.rows[0].getInt(0) == 42
      doAssert conn.stmtCache.len == 1

      # Cache hit with binary format
      let r2 = await conn.query("SELECT 42::int4", resultFormat = rfBinary)
      doAssert r2.rows[0].getInt(0) == 42

      await conn.close()

    waitFor t()

  test "stmt cache: clearStmtCache works":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      discard await conn.query("SELECT 1")
      discard await conn.query("SELECT 2")
      doAssert conn.stmtCache.len == 2

      conn.clearStmtCache()
      doAssert conn.stmtCache.len == 0

      # After clear, queries still work (cache miss path)
      let r = await conn.query("SELECT 3")
      doAssert r.rows[0].getStr(0) == "3"
      doAssert conn.stmtCache.len == 1

      await conn.close()

    waitFor t()

  test "stmt cache: disabled when capacity=0":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 0

      discard await conn.query("SELECT 1")
      doAssert conn.stmtCache.len == 0

      discard await conn.exec("SELECT 1")
      doAssert conn.stmtCache.len == 0

      await conn.close()

    waitFor t()

  test "stmt cache: full cache evicts LRU entry":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      discard await conn.query("SELECT 1")
      discard await conn.query("SELECT 2")
      doAssert conn.stmtCache.len == 2

      # Cache full, LRU entry ("SELECT 1") is evicted
      let r = await conn.query("SELECT 3")
      doAssert r.rows[0].getStr(0) == "3"
      doAssert conn.stmtCache.len == 2
      doAssert not conn.stmtCache.hasKey("SELECT 1") # evicted
      doAssert conn.stmtCache.hasKey("SELECT 2")
      doAssert conn.stmtCache.hasKey("SELECT 3") # newly cached

      # Access "SELECT 2" to make it most recent, then add new
      discard await conn.query("SELECT 2")
      discard await conn.query("SELECT 4")
      doAssert conn.stmtCache.len == 2
      doAssert not conn.stmtCache.hasKey("SELECT 3") # evicted (was LRU)
      doAssert conn.stmtCache.hasKey("SELECT 2") # kept (was accessed)
      doAssert conn.stmtCache.hasKey("SELECT 4") # newly cached

      await conn.close()

    waitFor t()

  test "stmt cache: works with pool":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 1))

      # First query populates cache on the pooled connection
      let r1 = await pool.query("SELECT 'cached'")
      doAssert r1.rows[0].getStr(0) == "cached"

      # Second query should hit cache
      let r2 = await pool.query("SELECT 'cached'")
      doAssert r2.rows[0].getStr(0) == "cached"

      await pool.close()

    waitFor t()

suite "E2E: simpleExec":
  test "simpleExec returns command tag":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_simpleexec")
      discard await conn.exec(
        "CREATE TABLE test_simpleexec (id serial PRIMARY KEY, val text)"
      )

      let tag = await conn.simpleExec("INSERT INTO test_simpleexec (val) VALUES ('a')")
      doAssert tag == "INSERT 0 1"

      let tag2 =
        await conn.simpleExec("INSERT INTO test_simpleexec (val) VALUES ('b'), ('c')")
      doAssert tag2 == "INSERT 0 2"

      discard await conn.exec("DROP TABLE test_simpleexec")
      await conn.close()

    waitFor t()

  test "simpleExec raises on error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        discard await conn.simpleExec("SELECT * FROM nonexistent_table_xyz")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "pool.simpleExec":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let tag = await pool.simpleExec("SELECT 1")
      doAssert tag == "SELECT 1"
      await pool.close()

    waitFor t()

suite "E2E: execInTransaction / queryInTransaction":
  test "execInTransaction commits successfully":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_eit")
      discard await conn.exec("CREATE TABLE test_eit (id serial PRIMARY KEY, val text)")

      let tag = await conn.execInTransaction(
        "INSERT INTO test_eit (val) VALUES ($1)", @[some("pipelined".toBytes())]
      )
      doAssert tag == "INSERT 0 1"

      # Verify committed
      let res = await conn.query("SELECT val FROM test_eit")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pipelined"

      discard await conn.exec("DROP TABLE test_eit")
      await conn.close()

    waitFor t()

  test "execInTransaction rolls back on error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_eit_err")
      discard await conn.exec(
        "CREATE TABLE test_eit_err (id serial PRIMARY KEY, val text UNIQUE)"
      )
      discard await conn.exec("INSERT INTO test_eit_err (val) VALUES ('existing')")

      var raised = false
      try:
        discard await conn.execInTransaction(
          "INSERT INTO test_eit_err (val) VALUES ($1)", @[some("existing".toBytes())]
        )
      except PgError:
        raised = true

      doAssert raised
      doAssert conn.state == csReady

      # Verify no extra row was committed
      let res = await conn.query("SELECT count(*) FROM test_eit_err")
      doAssert res.rows[0].getStr(0) == "1"

      discard await conn.exec("DROP TABLE test_eit_err")
      await conn.close()

    waitFor t()

  test "execInTransaction with PgParam":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_eit_pg")
      discard await conn.exec("CREATE TABLE test_eit_pg (id int, name text)")

      let tag = await conn.execInTransaction(
        "INSERT INTO test_eit_pg (id, name) VALUES ($1, $2)",
        @[toPgParam(42'i32), toPgParam("typed")],
      )
      doAssert tag == "INSERT 0 1"

      let res = await conn.query("SELECT name FROM test_eit_pg WHERE id = 42")
      doAssert res.rows[0].getStr(0) == "typed"

      discard await conn.exec("DROP TABLE test_eit_pg")
      await conn.close()

    waitFor t()

  test "execInTransaction with TransactionOptions":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_eit_opts")
      discard
        await conn.exec("CREATE TABLE test_eit_opts (id serial PRIMARY KEY, val text)")

      let tag = await conn.execInTransaction(
        "INSERT INTO test_eit_opts (val) VALUES ($1)",
        @[toPgParam("serializable")],
        TransactionOptions(isolation: ilSerializable),
      )
      doAssert tag == "INSERT 0 1"

      let res = await conn.query("SELECT val FROM test_eit_opts")
      doAssert res.rows[0].getStr(0) == "serializable"

      discard await conn.exec("DROP TABLE test_eit_opts")
      await conn.close()

    waitFor t()

  test "queryInTransaction returns rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_qit")
      discard await conn.exec("CREATE TABLE test_qit (id serial PRIMARY KEY, val text)")
      discard await conn.exec("INSERT INTO test_qit (val) VALUES ('a'), ('b'), ('c')")

      let qr = await conn.queryInTransaction("SELECT val FROM test_qit ORDER BY id")
      doAssert qr.rows.len == 3
      doAssert qr.rows[0].getStr(0) == "a"
      doAssert qr.rows[1].getStr(0) == "b"
      doAssert qr.rows[2].getStr(0) == "c"

      discard await conn.exec("DROP TABLE test_qit")
      await conn.close()

    waitFor t()

  test "queryInTransaction with PgParam":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_qit_pg")
      discard await conn.exec("CREATE TABLE test_qit_pg (id int, val text)")
      discard
        await conn.exec("INSERT INTO test_qit_pg (id, val) VALUES (1, 'x'), (2, 'y')")

      let qr = await conn.queryInTransaction(
        "SELECT val FROM test_qit_pg WHERE id = $1", @[toPgParam(1'i32)]
      )
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getStr(0) == "x"

      discard await conn.exec("DROP TABLE test_qit_pg")
      await conn.close()

    waitFor t()

  test "queryInTransaction with TransactionOptions":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let qr = await conn.queryInTransaction(
        "SELECT 1",
        @[],
        TransactionOptions(isolation: ilSerializable, access: amReadOnly),
      )
      doAssert qr.rows.len == 1

      await conn.close()

    waitFor t()

  test "pool.execInTransaction":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_peit")
      discard
        await pool.exec("CREATE TABLE test_peit (id serial PRIMARY KEY, val text)")

      let tag = await pool.execInTransaction(
        "INSERT INTO test_peit (val) VALUES ($1)", @[toPgParam("pool_tx")]
      )
      doAssert tag == "INSERT 0 1"

      let res = await pool.query("SELECT val FROM test_peit")
      doAssert res.rows[0].getStr(0) == "pool_tx"

      discard await pool.exec("DROP TABLE test_peit")
      await pool.close()

    waitFor t()

  test "pool.queryInTransaction":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))

      let qr = await pool.queryInTransaction("SELECT 42::int4")
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getStr(0) == "42"

      await pool.close()

    waitFor t()

  test "pipeline: multiple exec":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_pipe_exec")
      discard
        await conn.exec("CREATE TABLE test_pipe_exec (id serial PRIMARY KEY, val text)")

      let p = newPipeline(conn)
      p.addExec("INSERT INTO test_pipe_exec (val) VALUES ($1)", @[toPgParam("a")])
      p.addExec("INSERT INTO test_pipe_exec (val) VALUES ($1)", @[toPgParam("b")])
      p.addExec("INSERT INTO test_pipe_exec (val) VALUES ($1)", @[toPgParam("c")])
      let results = await p.execute()
      doAssert results.len == 3
      for r in results:
        doAssert r.kind == prkExec
        doAssert r.commandTag == "INSERT 0 1"

      let qr = await conn.query("SELECT val FROM test_pipe_exec ORDER BY id")
      doAssert qr.rowCount == 3
      doAssert qr.rows[0].getStr(0) == "a"
      doAssert qr.rows[1].getStr(0) == "b"
      doAssert qr.rows[2].getStr(0) == "c"

      discard await conn.exec("DROP TABLE test_pipe_exec")
      await conn.close()

    waitFor t()

  test "pipeline: multiple query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addQuery("SELECT 1::int4 AS a")
      p.addQuery("SELECT 2::int4 AS b, 3::int4 AS c")
      p.addQuery("SELECT 'hello'::text AS greeting")
      let results = await p.execute()
      doAssert results.len == 3

      doAssert results[0].kind == prkQuery
      doAssert results[0].queryResult.rowCount == 1
      doAssert results[0].queryResult.rows[0].getStr(0) == "1"

      doAssert results[1].kind == prkQuery
      doAssert results[1].queryResult.rowCount == 1
      doAssert results[1].queryResult.rows[0].getStr(0) == "2"
      doAssert results[1].queryResult.rows[0].getStr(1) == "3"

      doAssert results[2].kind == prkQuery
      doAssert results[2].queryResult.rowCount == 1
      doAssert results[2].queryResult.rows[0].getStr(0) == "hello"

      await conn.close()

    waitFor t()

  test "pipeline: mixed exec and query":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_pipe_mixed")
      discard await conn.exec(
        "CREATE TABLE test_pipe_mixed (id serial PRIMARY KEY, val text)"
      )

      let p = newPipeline(conn)
      p.addExec("INSERT INTO test_pipe_mixed (val) VALUES ($1)", @[toPgParam("x")])
      p.addQuery("SELECT val FROM test_pipe_mixed ORDER BY id")
      p.addExec("INSERT INTO test_pipe_mixed (val) VALUES ($1)", @[toPgParam("y")])
      p.addQuery("SELECT count(*)::int4 FROM test_pipe_mixed")
      let results = await p.execute()
      doAssert results.len == 4

      doAssert results[0].kind == prkExec
      doAssert results[0].commandTag == "INSERT 0 1"

      doAssert results[1].kind == prkQuery
      doAssert results[1].queryResult.rowCount == 1
      doAssert results[1].queryResult.rows[0].getStr(0) == "x"

      doAssert results[2].kind == prkExec
      doAssert results[2].commandTag == "INSERT 0 1"

      doAssert results[3].kind == prkQuery
      doAssert results[3].queryResult.rowCount == 1
      doAssert results[3].queryResult.rows[0].getStr(0) == "2"

      discard await conn.exec("DROP TABLE test_pipe_mixed")
      await conn.close()

    waitFor t()

  test "pipeline: statement cache hit/miss":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # First execution: cache miss
      var p1 = newPipeline(conn)
      p1.addQuery("SELECT $1::text", @[toPgParam("first")])
      p1.addQuery("SELECT $1::int4", @[toPgParam(42'i32)])
      let r1 = await p1.execute()
      doAssert r1[0].queryResult.rows[0].getStr(0) == "first"
      doAssert r1[1].queryResult.rows[0].getStr(0) == "42"

      # Second execution: cache hit (same SQL)
      var p2 = newPipeline(conn)
      p2.addQuery("SELECT $1::text", @[toPgParam("second")])
      p2.addQuery("SELECT $1::int4", @[toPgParam(99'i32)])
      let r2 = await p2.execute()
      doAssert r2[0].queryResult.rows[0].getStr(0) == "second"
      doAssert r2[1].queryResult.rows[0].getStr(0) == "99"

      await conn.close()

    waitFor t()

  test "pipeline: error aborts remaining ops":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addExec("SELECT 1")
      p.addExec("INVALID SQL THAT WILL FAIL")
      p.addExec("SELECT 2") # Should be skipped by server
      var gotError = false
      try:
        discard await p.execute()
      except PgError:
        gotError = true
      doAssert gotError

      # Connection should still be usable
      doAssert conn.state == csReady
      let qr = await conn.query("SELECT 1::int4")
      doAssert qr.rows[0].getStr(0) == "1"

      await conn.close()

    waitFor t()

  test "pipeline: PgParam raw overload":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addExec("SELECT $1::text", @[some(@(toOpenArrayByte("hello", 0, 4)))])
      p.addQuery("SELECT $1::text", @[some(@(toOpenArrayByte("world", 0, 4)))])
      let results = await p.execute()
      doAssert results[0].kind == prkExec
      doAssert results[1].kind == prkQuery
      doAssert results[1].queryResult.rows[0].getStr(0) == "world"

      await conn.close()

    waitFor t()

  test "pipeline: empty pipeline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      let results = await p.execute()
      doAssert results.len == 0

      await conn.close()

    waitFor t()

  test "pipeline: pool withPipeline":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_pipe_pool")
      discard
        await pool.exec("CREATE TABLE test_pipe_pool (id serial PRIMARY KEY, val text)")

      pool.withPipeline(p):
        p.addExec(
          "INSERT INTO test_pipe_pool (val) VALUES ($1)", @[toPgParam("pooled")]
        )
        p.addQuery("SELECT val FROM test_pipe_pool")
        let results = await p.execute()
        doAssert results.len == 2
        doAssert results[0].commandTag == "INSERT 0 1"
        doAssert results[1].queryResult.rows[0].getStr(0) == "pooled"

      discard await pool.exec("DROP TABLE test_pipe_pool")
      await pool.close()

    waitFor t()

  test "pipeline: query with multiple rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addQuery("SELECT generate_series(1, 5)::int4 AS n")
      p.addQuery("SELECT generate_series(10, 12)::int4 AS m")
      let results = await p.execute()
      doAssert results.len == 2

      doAssert results[0].queryResult.rowCount == 5
      for i in 0 ..< 5:
        doAssert results[0].queryResult.rows[i].getStr(0) == $(i + 1)

      doAssert results[1].queryResult.rowCount == 3
      doAssert results[1].queryResult.rows[0].getStr(0) == "10"
      doAssert results[1].queryResult.rows[2].getStr(0) == "12"

      await conn.close()

    waitFor t()

  test "pipeline: query returning zero rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addQuery("SELECT 1::int4 WHERE false")
      p.addQuery("SELECT 42::int4")
      let results = await p.execute()
      doAssert results.len == 2

      doAssert results[0].queryResult.rowCount == 0
      doAssert results[1].queryResult.rowCount == 1
      doAssert results[1].queryResult.rows[0].getStr(0) == "42"

      await conn.close()

    waitFor t()

  test "pipeline: single op":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var p1 = newPipeline(conn)
      p1.addQuery("SELECT 'only'::text")
      let r1 = await p1.execute()
      doAssert r1.len == 1
      doAssert r1[0].queryResult.rows[0].getStr(0) == "only"

      var p2 = newPipeline(conn)
      p2.addExec("SELECT 1")
      let r2 = await p2.execute()
      doAssert r2.len == 1
      doAssert r2[0].kind == prkExec

      await conn.close()

    waitFor t()

  test "pipeline: cache full evicts LRU entries":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      # Fill the cache
      discard await conn.query("SELECT 1")
      discard await conn.query("SELECT 2")
      doAssert conn.stmtCache.len == 2

      # Pipeline with new SQL: evicts LRU entries
      let p = newPipeline(conn)
      p.addQuery("SELECT 100::int4")
      p.addQuery("SELECT 300::int4")
      let results = await p.execute()
      doAssert results.len == 2
      doAssert conn.stmtCache.len == 2
      doAssert conn.stmtCache.hasKey("SELECT 100::int4")
      doAssert conn.stmtCache.hasKey("SELECT 300::int4")

      doAssert results[0].queryResult.rows[0].getStr(0) == "100"
      doAssert results[1].queryResult.rows[0].getStr(0) == "300"

      await conn.close()

    waitFor t()

  test "pipeline: cache misses exceed capacity":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      # 3 cache misses with capacity 2: should not crash
      let p = newPipeline(conn)
      p.addQuery("SELECT 10::int4")
      p.addQuery("SELECT 20::int4")
      p.addQuery("SELECT 30::int4")
      let results = await p.execute()
      doAssert results.len == 3
      doAssert results[0].queryResult.rows[0].getStr(0) == "10"
      doAssert results[1].queryResult.rows[0].getStr(0) == "20"
      doAssert results[2].queryResult.rows[0].getStr(0) == "30"
      # Only first 2 are cached (3rd exceeds capacity)
      doAssert conn.stmtCache.len == 2

      await conn.close()

    waitFor t()

  test "pipeline: same SQL query repeated in single pipeline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addQuery("SELECT $1::text", @[toPgParam("aaa")])
      p.addQuery("SELECT $1::text", @[toPgParam("bbb")])
      p.addQuery("SELECT $1::text", @[toPgParam("ccc")])
      let results = await p.execute()
      doAssert results.len == 3
      doAssert results[0].queryResult.rows[0].getStr(0) == "aaa"
      doAssert results[1].queryResult.rows[0].getStr(0) == "bbb"
      doAssert results[2].queryResult.rows[0].getStr(0) == "ccc"

      # Second pipeline: same SQL should hit cache
      var p2 = newPipeline(conn)
      p2.addQuery("SELECT $1::text", @[toPgParam("cached")])
      let r2 = await p2.execute()
      doAssert r2[0].queryResult.rows[0].getStr(0) == "cached"

      await conn.close()

    waitFor t()

suite "E2E: queryDirect / execDirect":
  test "queryDirect with int32 param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::int4 + 10", 5'i32)
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 15
      await conn.close()

    waitFor t()

  test "queryDirect with string param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::text || ' world'", "hello")
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getStr(0) == "hello world"
      await conn.close()

    waitFor t()

  test "queryDirect with multiple params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect(
        "SELECT $1::int4 + $2::int4, $3::text", 10'i32, 20'i32, "abc"
      )
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 30
      doAssert qr.rows[0].getStr(1) == "abc"
      await conn.close()

    waitFor t()

  test "queryDirect cache hit on repeated call":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      for i in 0 ..< 5:
        let qr = await conn.queryDirect("SELECT $1::int4 * 2", int32(i))
        doAssert qr.rows[0].getInt(0) == int32(i * 2)
      await conn.close()

    waitFor t()

  test "queryDirect no params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT 42 AS answer")
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 42
      await conn.close()

    waitFor t()

  test "execDirect INSERT and UPDATE":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_exec_direct")
      discard await conn.exec(
        "CREATE TABLE test_exec_direct (id serial PRIMARY KEY, val int NOT NULL)"
      )

      let tag1 =
        await conn.execDirect("INSERT INTO test_exec_direct (val) VALUES ($1)", 100'i32)
      doAssert "INSERT" in tag1

      let tag2 = await conn.execDirect(
        "UPDATE test_exec_direct SET val = $1 WHERE val = $2", 200'i32, 100'i32
      )
      doAssert "UPDATE 1" in tag2

      let qr = await conn.queryDirect(
        "SELECT val FROM test_exec_direct WHERE val = $1", 200'i32
      )
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 200

      discard await conn.exec("DROP TABLE test_exec_direct")
      await conn.close()

    waitFor t()

  test "execDirect cache hit on repeated call":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_exec_direct2")
      discard await conn.exec(
        "CREATE TABLE test_exec_direct2 (id serial PRIMARY KEY, val int NOT NULL)"
      )

      for i in 0 ..< 5:
        discard await conn.execDirect(
          "INSERT INTO test_exec_direct2 (val) VALUES ($1)", int32(i)
        )

      let qr = await conn.query("SELECT count(*) FROM test_exec_direct2")
      doAssert qr.rows[0].getInt64(0) == 5

      discard await conn.exec("DROP TABLE test_exec_direct2")
      await conn.close()

    waitFor t()

  test "execDirect with bool param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::bool", true)
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getBool(0) == true
      await conn.close()

    waitFor t()

  test "execDirect with int64 param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::int8 + 1", 9223372036854775806'i64)
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt64(0) == 9223372036854775807'i64
      await conn.close()

    waitFor t()

  test "execDirect with float64 param":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.queryDirect("SELECT $1::float8 + 0.5", 1.25'f64)
      doAssert qr.rowCount == 1
      doAssert abs(qr.rows[0].getFloat(0) - 1.75) < 1e-10
      await conn.close()

    waitFor t()

suite "E2E: queryEach":
  test "basic - all rows passed to callback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var count = 0
      let rowCount = await conn.queryEach(
        "SELECT generate_series(1, 5)",
        callback = proc(row: Row) =
          count += 1,
      )
      doAssert count == 5
      doAssert rowCount == 5
      await conn.close()

    waitFor t()

  test "value access in callback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var values: seq[string]
      discard await conn.queryEach(
        "SELECT 'hello'::text, 42::int4, true::bool",
        callback = proc(row: Row) =
          values.add(row.getStr(0))
          values.add($row.getInt(1))
          values.add($row.getBool(2)),
      )
      doAssert values == @["hello", "42", "true"]
      await conn.close()

    waitFor t()

  test "with PgParam parameters":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sum = 0
      let rowCount = await conn.queryEach(
        "SELECT generate_series(1, $1::int4)",
        @[10'i32.toPgParam],
        callback = proc(row: Row) =
          sum += row.getInt(0),
      )
      doAssert rowCount == 10
      doAssert sum == 55
      await conn.close()

    waitFor t()

  test "zero rows - callback not called":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var called = false
      let rowCount = await conn.queryEach(
        "SELECT 1 WHERE false",
        callback = proc(row: Row) =
          called = true,
      )
      doAssert not called
      doAssert rowCount == 0
      await conn.close()

    waitFor t()

  test "10000 rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var count = 0
      let rowCount = await conn.queryEach(
        "SELECT generate_series(1, 10000)",
        callback = proc(row: Row) =
          count += 1,
      )
      doAssert count == 10000
      doAssert rowCount == 10000
      await conn.close()

    waitFor t()

  test "binary format with cache hit":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # First call: cache miss, populates stmt cache
      var firstVal = 0
      discard await conn.queryEach(
        "SELECT 42::int4",
        callback = proc(row: Row) =
          firstVal = row.getInt(0),
      )
      doAssert firstVal == 42
      # Second call: cache hit, should use binary format automatically
      var secondVal = 0
      discard await conn.queryEach(
        "SELECT 42::int4",
        callback = proc(row: Row) =
          secondVal = row.getInt(0),
      )
      doAssert secondVal == 42
      await conn.close()

    waitFor t()

  test "callback exception is propagated":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var count = 0
      var gotError = false
      try:
        discard await conn.queryEach(
          "SELECT generate_series(1, 5)",
          callback = proc(row: Row) =
            count += 1
            if count == 3:
              raise newException(ValueError, "test error")
          ,
        )
      except CatchableError:
        gotError = true
      doAssert gotError
      # Connection should be in ready state after exception
      doAssert conn.state == csReady
      # Connection should still be usable
      let qr = await conn.query("SELECT 1")
      doAssert qr.rowCount == 1
      await conn.close()

    waitFor t()

  test "NULL value handling":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var gotNull = false
      var gotValue = false
      discard await conn.queryEach(
        "SELECT NULL::text, 'hello'::text",
        callback = proc(row: Row) =
          gotNull = row.isNull(0)
          gotValue = row.getStr(1) == "hello",
      )
      doAssert gotNull
      doAssert gotValue
      await conn.close()

    waitFor t()

  test "multiple rows have correct values":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var values: seq[int]
      discard await conn.queryEach(
        "SELECT x FROM generate_series(1, 5) AS x ORDER BY x",
        callback = proc(row: Row) =
          values.add(row.getInt(0)),
      )
      doAssert values == @[1, 2, 3, 4, 5]
      await conn.close()

    waitFor t()

  test "consecutive queryEach on same connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sum1 = 0
      discard await conn.queryEach(
        "SELECT generate_series(1, 3)",
        callback = proc(row: Row) =
          sum1 += row.getInt(0),
      )
      var sum2 = 0
      discard await conn.queryEach(
        "SELECT generate_series(10, 12)",
        callback = proc(row: Row) =
          sum2 += row.getInt(0),
      )
      doAssert sum1 == 6
      doAssert sum2 == 33
      await conn.close()

    waitFor t()

  test "queryEach then query on same connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var eachCount = 0
      discard await conn.queryEach(
        "SELECT generate_series(1, 5)",
        callback = proc(row: Row) =
          eachCount += 1,
      )
      doAssert eachCount == 5
      let qr = await conn.query("SELECT 42::int4")
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getInt(0) == 42
      await conn.close()

    waitFor t()

  test "invalid SQL raises PgError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var gotError = false
      try:
        discard await conn.queryEach(
          "SELECT FROM nonexistent_table_xyz",
          callback = proc(row: Row) =
            discard,
        )
      except PgError:
        gotError = true
      doAssert gotError
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "without stmt cache":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 0
      var values: seq[int]
      let rowCount = await conn.queryEach(
        "SELECT generate_series(1, 3)",
        callback = proc(row: Row) =
          values.add(row.getInt(0)),
      )
      doAssert rowCount == 3
      doAssert values == @[1, 2, 3]
      await conn.close()

    waitFor t()

  test "Option[seq[byte]] params overload":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sum = 0
      let rowCount = await conn.queryEach(
        "SELECT generate_series(1, $1::int4)",
        @[some(@[byte('5')])],
        callback = proc(row: Row) =
          sum += row.getInt(0),
      )
      doAssert rowCount == 5
      doAssert sum == 15
      await conn.close()

    waitFor t()

  test "pool queryEach":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      var count = 0
      let rowCount = await pool.queryEach(
        "SELECT generate_series(1, 5)",
        callback = proc(row: Row) =
          count += 1,
      )
      doAssert count == 5
      doAssert rowCount == 5
      await pool.close()

    waitFor t()

  test "pool queryEach with PgParam":
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 2))
      var sum = 0
      let rowCount = await pool.queryEach(
        "SELECT generate_series(1, $1::int4)",
        @[5'i32.toPgParam],
        callback = proc(row: Row) =
          sum += row.getInt(0),
      )
      doAssert rowCount == 5
      doAssert sum == 15
      await pool.close()

    waitFor t()

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
