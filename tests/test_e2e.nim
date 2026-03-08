import std/[unittest, options, strutils, tables, os, math, deques, sets]
from std/times import
  DateTime, dateTime, mMar, mJun, mJan, utc, year, month, monthday, hour, minute,
  second, toTime, toUnix, nanosecond

import
  ../async_postgres/
    [async_backend, pg_protocol, pg_client, pg_pool, pg_types]

import ../async_postgres/pg_connection {.all.}

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

template makeCopyOutCallback(body: untyped): CopyOutCallback =
  block:
    when hasChronos:
      let r: CopyOutCallback = proc(
          data {.inject.}: seq[byte]
      ) {.async: (raises: [CatchableError]).} =
        body
      r
    else:
      let r: CopyOutCallback = proc(data {.inject.}: seq[byte]) {.async.} =
        body
      r

suite "E2E: Basic Connection":
  test "plain connection and close":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == false
      await conn.close()
      doAssert conn.state == csClosed

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
      doAssert pool.idle.len == 0

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
      doAssert pool.idle.len == 3

      # Wait for idleTimeout + maintenance cycles
      await sleepAsync(milliseconds(500))

      # Should shrink to minSize, not below
      doAssert pool.idle.len == 2

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

      var allRows: seq[seq[Option[seq[byte]]]]
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

      var allRows: seq[seq[Option[seq[byte]]]]
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

      doAssert pool.idle.len == 2

      # Wait for maxLifetime to expire + maintenance to clean + replenish
      await sleepAsync(milliseconds(600))

      doAssert pool.idle.len == 2

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
      doAssert res.rows[0].getStr(0) == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
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
        @[toPgParam(PgNumeric("123456789012.56789012"))],
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
        "SELECT 42::int2, 123456::int4, 9999999999::int8", resultFormats = binaryFormat
      )
      doAssert qr.rows.len == 1
      let row = qr.rows[0]
      doAssert row.getInt(0, qr.fields) == 42'i32 # int2 promoted via getInt
      doAssert row.getInt(1, qr.fields) == 123456'i32
      doAssert row.getInt64(2, qr.fields) == 9999999999'i64
      # getInt64 should also work on int2/int4 columns (promotion)
      doAssert row.getInt64(0, qr.fields) == 42'i64
      doAssert row.getInt64(1, qr.fields) == 123456'i64
      await conn.close()

    waitFor t()

  test "binary results for float types":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT 3.14::float8, 1.5::float4", resultFormats = binaryFormat
      )
      doAssert qr.rows.len == 1
      let row = qr.rows[0]
      doAssert abs(row.getFloat(0, qr.fields) - 3.14) < 1e-10
      doAssert abs(row.getFloat(1, qr.fields) - 1.5) < 1e-5
      await conn.close()

    waitFor t()

  test "binary results for bool":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query("SELECT true, false", resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBool(0, qr.fields) == true
      doAssert qr.rows[0].getBool(1, qr.fields) == false
      await conn.close()

    waitFor t()

  test "binary results for text":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT 'hello'::text, 'world'::varchar", resultFormats = binaryFormat
      )
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getStr(0, qr.fields) == "hello"
      doAssert qr.rows[0].getStr(1, qr.fields) == "world"
      await conn.close()

    waitFor t()

  test "binary results for bytea":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr =
        await conn.query("SELECT '\\xDEADBEEF'::bytea", resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBytes(0, qr.fields) == @[0xDE'u8, 0xAD, 0xBE, 0xEF]
      await conn.close()

    waitFor t()

  test "binary results for timestamp":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT '2024-01-15 10:30:00'::timestamp", resultFormats = binaryFormat
      )
      doAssert qr.rows.len == 1
      let dt = qr.rows[0].getTimestamp(0, qr.fields)
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
      let qr =
        await conn.query("SELECT '2024-01-15'::date", resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      let dt = qr.rows[0].getDate(0, qr.fields)
      doAssert dt.year == 2024
      doAssert dt.month == mJan
      doAssert dt.monthday == 15
      await conn.close()

    waitFor t()

  test "binary results for uuid":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr = await conn.query(
        "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid",
        resultFormats = binaryFormat,
      )
      doAssert qr.rows.len == 1
      let data = qr.rows[0].getBytes(0, qr.fields)
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
        "SELECT $1::int4, $2::int8, $3::bool", params, resultFormats = binaryFormat
      )
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0, qr.fields) == 42'i32
      doAssert qr.rows[0].getInt64(1, qr.fields) == 9999999999'i64
      doAssert qr.rows[0].getBool(2, qr.fields) == true
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
      let qr = await conn.query("SELECT $1::int4", params, resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0, qr.fields) == 42'i32
      await conn.close()

    waitFor t()

  test "NULL handling in binary mode":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let qr =
        await conn.query("SELECT NULL::int4, NULL::text", resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].isNull(0)
      doAssert qr.rows[0].isNull(1)
      await conn.close()

    waitFor t()

  test "prepared statement with binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let stmt = await conn.prepare("bin_stmt", "SELECT $1::int4 + 10")
      let qr =
        await stmt.execute(@[toPgBinaryParam(32'i32)], resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getInt(0, qr.fields) == 42'i32
      await stmt.close()
      await conn.close()

    waitFor t()

  test "binary timestamp param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let dt = dateTime(2024, mJan, 15, 10, 30, 0, 0, utc())
      let params = @[toPgBinaryParam(dt)]
      let qr =
        await conn.query("SELECT $1::timestamp", params, resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      let result = qr.rows[0].getTimestamp(0, qr.fields)
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
      let qr =
        await conn.query("SELECT $1::float8", params, resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      doAssert abs(qr.rows[0].getFloat(0, qr.fields) - 3.14159265358979) < 1e-14
      await conn.close()

    waitFor t()

  test "binary bytea param roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let data = @[0xDE'u8, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]
      let params = @[toPgBinaryParam(data)]
      let qr =
        await conn.query("SELECT $1::bytea", params, resultFormats = binaryFormat)
      doAssert qr.rows.len == 1
      doAssert qr.rows[0].getBytes(0, qr.fields) == data
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
      let bufLenBefore = conn.recvBuf.len
      # Trigger a timeout with no pending server data
      try:
        discard await conn.recvMessage(timeout = milliseconds(100))
      except AsyncTimeoutError:
        discard
      # recvBuf must not grow from the failed read
      doAssert conn.recvBuf.len == bufLenBefore
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

  test "buffer respects max queue size":
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
      doAssert listener.notifyQueue.popFirst().payload == "3"
      doAssert listener.notifyQueue.popFirst().payload == "4"
      doAssert listener.notifyQueue.popFirst().payload == "5"

      await listener.unlisten("buf_max")
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
