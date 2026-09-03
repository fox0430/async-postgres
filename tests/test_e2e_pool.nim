import std/[unittest, options, math, importutils, net, deques]

import
  ../async_postgres/[
    async_backend, pg_protocol, pg_types, pg_client, pg_pool, pg_connection,
    pg_advisory_lock,
  ]

import e2e_common

privateAccess(PgPool)
privateAccess(PgConnection)

suite "E2E: Connection Pool":
  test "basic acquire and release":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      let conn = await pool.acquire()
      doAssert conn.state == csReady
      conn.release()
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
      c1.release()
      c2.release()
      c3.release()
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
      conn1.release()

      # Wait for maxLifetime to expire
      await sleepAsync(milliseconds(600))

      # Maintenance should have closed the expired connection
      # Next acquire should create a new one
      let conn2 = await pool.acquire()
      doAssert conn2.state == csReady
      # The new connection should be different (different pid from server)
      doAssert conn2.pid != pid1
      conn2.release()

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
      conn.release()

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
      c1.release()
      c2.release()
      c3.release()
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
      conn1.release()

      # Wait for maxLifetime to expire
      await sleepAsync(milliseconds(400))

      # Maintenance hasn't run (60s interval), but acquire should skip expired
      let conn2 = await pool.acquire()
      doAssert conn2.state == csReady
      doAssert conn2.pid != pid1
      conn2.release()

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
      conn1.release()

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
      conn1.release()

      # Immediate re-acquire should return the same connection
      let conn2 = await pool.acquire()
      doAssert conn2.pid == pid1
      conn2.release()

      await pool.close()

    waitFor t()

suite "E2E: resetQuery timeout":
  test "hung resetQuery is bounded by resetQueryTimeout and connection is discarded":
    # Simulate a server-side hang during the release-path reset by pointing
    # resetQuery at pg_sleep. Without a timeout, the release blocks forever
    # and starves the pool; with resetQueryTimeout set, the reset raises,
    # the connection is closed, and the release completes.
    proc t() {.async.} =
      let cfg = initPoolConfig(
        plainConfig(),
        minSize = 0,
        maxSize = 1,
        resetQuery = "SELECT pg_sleep(30)",
        resetQueryTimeout = milliseconds(200),
      )
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      let closeBefore = pool.metrics.closeCount
      pool.withConnection(conn):
        discard await conn.simpleQuery("SELECT 1")
      # resetSession hit the timeout, poisoned the connection, and tracedClose
      # discarded it — nothing returned to idle.
      doAssert pool.idleCount == 0
      doAssert pool.metrics.closeCount - closeBefore == 1

      # A follow-up acquire opens a fresh conn instead of blocking on the
      # (now-drained) idle queue behind a stuck reset.
      pool.withConnection(conn2):
        let r = await conn2.simpleQuery("SELECT 2")
        doAssert r.len == 1

    waitFor t()

  test "advisory unlock during reset is bounded by resetQueryTimeout":
    # sessionLockDirty forces pg_advisory_unlock_all() during resetSession.
    # With a lock_timeout of 0 the unlock itself cannot hang, so we use a
    # short resetQueryTimeout only to prove it plumbs through; a real hang
    # would surface identically.
    proc t() {.async.} =
      let cfg = initPoolConfig(
        plainConfig(), minSize = 0, maxSize = 1, resetQueryTimeout = milliseconds(500)
      )
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      pool.withConnection(conn):
        await conn.advisoryLock(73101'i64)
      doAssert pool.idleCount == 1
      # Re-acquire the same conn (minSize=0, maxSize=1) and verify unlock ran.
      pool.withConnection(conn2):
        doAssert conn2.heldSessionLocks == 0

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
        @[toPgParam("alice"), toPgParam("bob")],
      )
      let res = await pool.query(
        "SELECT name FROM test_pool_qp WHERE name = $1", @[toPgParam("bob")]
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
      conn.release()

      await pool.close()

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
        conn2.release()
      except PgError as e:
        raised = true
        doAssert e of PgPoolError
        doAssert (ref PgPoolError)(e).kind == pekAcquireTimeout

      doAssert raised

      # Release and verify pool still works
      conn.release()
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
      conn.release()

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

suite "E2E: Pipelined Pool":
  test "pipelined pool: basic exec and query":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      discard await pool.exec("DROP TABLE IF EXISTS test_pp")
      discard await pool.exec("CREATE TABLE test_pp (id serial PRIMARY KEY, val text)")

      discard
        await pool.exec("INSERT INTO test_pp (val) VALUES ($1)", @[toPgParam("hi")])
      let qr = await pool.query("SELECT val FROM test_pp")
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getStr(0) == "hi"

      discard await pool.exec("DROP TABLE test_pp")
      await pool.close()

    waitFor t()

  test "pool: PgParamInline overload (non-pipelined)":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_pool_inline")
      discard
        await pool.exec("CREATE TABLE test_pool_inline (id serial PRIMARY KEY, n int4)")

      discard await pool.exec(
        "INSERT INTO test_pool_inline (n) VALUES ($1)", @[toPgParamInline(42'i32)]
      )
      let qr = await pool.query(
        "SELECT n FROM test_pool_inline WHERE n = $1", @[toPgParamInline(42'i32)]
      )
      doAssert qr.rowCount == 1
      doAssert qr.rows[0].getStr(0) == "42"

      discard await pool.exec("DROP TABLE test_pool_inline")
      await pool.close()

    waitFor t()

  test "pipelined pool: PgParamInline overload batched through pipeline":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      discard await pool.exec("DROP TABLE IF EXISTS test_pp_inline")
      discard
        await pool.exec("CREATE TABLE test_pp_inline (id serial PRIMARY KEY, n int4)")

      # Fire in the same tick so they land in one pipelined batch, exercising
      # the PendingPoolOp inline path.
      let f1 = pool.exec(
        "INSERT INTO test_pp_inline (n) VALUES ($1)", @[toPgParamInline(1'i32)]
      )
      let f2 = pool.exec(
        "INSERT INTO test_pp_inline (n) VALUES ($1)", @[toPgParamInline(2'i32)]
      )
      let f3 = pool.query(
        "SELECT n FROM test_pp_inline WHERE n = $1", @[toPgParamInline(1'i32)]
      )
      discard await f1
      discard await f2
      let r3 = await f3
      doAssert r3.rowCount == 1
      doAssert r3.rows[0].getStr(0) == "1"

      discard await pool.exec("DROP TABLE test_pp_inline")
      await pool.close()

    waitFor t()

  test "pipelined pool: concurrent ops are batched":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )

      # Fire multiple queries concurrently within the same tick
      let f1 = pool.query("SELECT 1::int4")
      let f2 = pool.query("SELECT 2::int4")
      let f3 = pool.query("SELECT 3::int4")
      let r1 = await f1
      let r2 = await f2
      let r3 = await f3
      doAssert r1.rows[0].getStr(0) == "1"
      doAssert r2.rows[0].getStr(0) == "2"
      doAssert r3.rows[0].getStr(0) == "3"

      await pool.close()

    waitFor t()

  test "pipelined pool: error isolation between ops":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )

      let f1 = pool.query("SELECT 1::int4")
      let f2 = pool.query("INVALID SQL HERE")
      let f3 = pool.query("SELECT 3::int4")

      let r1 = await f1
      doAssert r1.rows[0].getStr(0) == "1"

      var gotError = false
      try:
        discard await f2
      except PgError:
        gotError = true
      doAssert gotError

      let r3 = await f3
      doAssert r3.rows[0].getStr(0) == "3"

      await pool.close()

    waitFor t()

  test "pipelined pool: exec error isolation":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      discard await pool.exec("DROP TABLE IF EXISTS test_pp_exec_err")
      discard await pool.exec("CREATE TABLE test_pp_exec_err (id int PRIMARY KEY)")
      discard await pool.exec("INSERT INTO test_pp_exec_err VALUES (1)")

      # Fire concurrent ops: one exec will violate PK, others should succeed
      let f1 = pool.query("SELECT 1::int4")
      let f2 = pool.exec("INSERT INTO test_pp_exec_err VALUES (1)") # duplicate PK
      let f3 = pool.query("SELECT 3::int4")

      let r1 = await f1
      doAssert r1.rows[0].getStr(0) == "1"

      var gotError = false
      try:
        discard await f2
      except PgError:
        gotError = true
      doAssert gotError

      let r3 = await f3
      doAssert r3.rows[0].getStr(0) == "3"

      discard await pool.exec("DROP TABLE test_pp_exec_err")
      await pool.close()

    waitFor t()

  test "pipelined pool: queryRowOpt works":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      let row = await pool.queryRowOpt("SELECT 42::int4 AS answer")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "42"

      let empty = await pool.queryRowOpt("SELECT 1 WHERE false")
      doAssert empty.isNone

      await pool.close()

    waitFor t()

  test "pipelined pool: queryValue works":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      let v = await pool.queryValue("SELECT 'hello'::text")
      doAssert v == "hello"

      await pool.close()

    waitFor t()

  test "pipelined pool: queryValueOpt works":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      let opt = await pool.queryValueOpt("SELECT 'found'::text")
      doAssert opt.isSome
      doAssert opt.get == "found"

      let optNone = await pool.queryValueOpt("SELECT 1 WHERE false")
      doAssert optNone.isNone

      await pool.close()

    waitFor t()

  test "pipelined pool: queryValueOrDefault works":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      let v = await pool.queryValueOrDefault("SELECT 'val'::text", default = "fb")
      doAssert v == "val"

      let def =
        await pool.queryValueOrDefault("SELECT 1 WHERE false", default = "fallback")
      doAssert def == "fallback"

      await pool.close()

    waitFor t()

  test "pipelined pool: queryExists works":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      let exists = await pool.queryExists("SELECT 1")
      doAssert exists

      let notExists = await pool.queryExists("SELECT 1 WHERE false")
      doAssert not notExists

      await pool.close()

    waitFor t()

  test "pipelined pool: queryColumn works":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      let col = await pool.queryColumn(
        "SELECT v::text FROM (VALUES ('a'), ('b'), ('c')) AS t(v)"
      )
      doAssert col.len == 3
      doAssert col == @["a", "b", "c"]

      await pool.close()

    waitFor t()

  test "pipelined pool: close fails pending ops":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3, pipelined: true)
      )
      # Queue an op but close immediately
      let fut = pool.query("SELECT 1::int4")
      await pool.close()

      var gotError = false
      try:
        discard await fut
      except PgError:
        gotError = true
      doAssert gotError

    waitFor t()

  test "pipelined pool: maxPipelineSize limits batch":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(
          connConfig: plainConfig(),
          minSize: 1,
          maxSize: 3,
          pipelined: true,
          maxPipelineSize: 2,
        )
      )

      # Fire 4 queries concurrently; maxPipelineSize=2 means batches of 2
      let f1 = pool.query("SELECT 10::int4")
      let f2 = pool.query("SELECT 20::int4")
      let f3 = pool.query("SELECT 30::int4")
      let f4 = pool.query("SELECT 40::int4")
      doAssert (await f1).rows[0].getStr(0) == "10"
      doAssert (await f2).rows[0].getStr(0) == "20"
      doAssert (await f3).rows[0].getStr(0) == "30"
      doAssert (await f4).rows[0].getStr(0) == "40"

      await pool.close()

    waitFor t()

  test "pipelined pool: high concurrency distributes across connections":
    proc t() {.async.} =
      let pool = await newPool(
        PoolConfig(connConfig: plainConfig(), minSize: 2, maxSize: 4, pipelined: true)
      )

      # Fire 6 concurrent queries -- more than maxSize
      let f1 = pool.query("SELECT 1::int4")
      let f2 = pool.query("SELECT 2::int4")
      let f3 = pool.query("SELECT 3::int4")
      let f4 = pool.query("SELECT 4::int4")
      let f5 = pool.query("SELECT 5::int4")
      let f6 = pool.query("SELECT 6::int4")
      doAssert (await f1).rows[0].getStr(0) == "1"
      doAssert (await f2).rows[0].getStr(0) == "2"
      doAssert (await f3).rows[0].getStr(0) == "3"
      doAssert (await f4).rows[0].getStr(0) == "4"
      doAssert (await f5).rows[0].getStr(0) == "5"
      doAssert (await f6).rows[0].getStr(0) == "6"

      await pool.close()

    waitFor t()

suite "E2E: queryRowOpt via pool":
  test "pool queryRowOpt returns first row and none on empty":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 2))
      let row = await pool.queryRowOpt("SELECT 7 AS v")
      doAssert row.isSome
      doAssert row.get.getStr(0) == "7"
      let empty = await pool.queryRowOpt("SELECT 1 WHERE false")
      doAssert empty.isNone
      await pool.close()

    waitFor t()

suite "E2E: withConnection body-exception release":
  test "withConnection releases the connection when the body raises a Defect":
    # Regression: a Defect raised in the body must still release the
    # connection (captured, released, re-raised wrapped in `PgPoolError`
    # with the Defect as `parent`), not leak a pool slot.
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 0, maxSize = 3))
      defer:
        await pool.close()

      for i in 0 ..< 2:
        var caught: ref PgPoolError = nil
        try:
          pool.withConnection(conn):
            raise newException(AssertionDefect, "boom")
        except PgPoolError as e:
          caught = e
        doAssert caught != nil, "body Defect must surface as PgPoolError"
        doAssert caught.parent of Defect,
          "the original Defect must be preserved as parent"
        doAssert pool.activeCount == 0

    waitFor t()

  test "withConnection re-raises a body error after a failed release":
    # Regression: a body exception must survive a failing release (hung
    # resetQuery bounded by resetQueryTimeout closes the connection instead),
    # and the pool must not be left with the slot stuck active.
    proc t() {.async.} =
      let cfg = initPoolConfig(
        plainConfig(),
        minSize = 0,
        maxSize = 1,
        resetQuery = "SELECT pg_sleep(30)",
        resetQueryTimeout = milliseconds(200),
      )
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      var raised = false
      try:
        pool.withConnection(conn):
          discard await conn.simpleQuery("SELECT 1")
          raise newException(ValueError, "body error")
      except ValueError:
        raised = true

      doAssert raised, "body error must propagate despite release failure"
      doAssert pool.activeCount == 0

    waitFor t()

  test "withConnection re-raises a body Defect after a failed release":
    # Regression: a body Defect must survive a failing release (hung resetQuery
    # bounded by resetQueryTimeout closes the connection instead) and surface as
    # `PgPoolError` (parent = Defect) — the release error must not shadow it.
    proc t() {.async.} =
      let cfg = initPoolConfig(
        plainConfig(),
        minSize = 0,
        maxSize = 1,
        resetQuery = "SELECT pg_sleep(30)",
        resetQueryTimeout = milliseconds(200),
      )
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      var caught: ref PgPoolError = nil
      try:
        pool.withConnection(conn):
          discard await conn.simpleQuery("SELECT 1")
          raise newException(AssertionDefect, "body defect")
      except PgPoolError as e:
        caught = e

      doAssert caught != nil, "body Defect must propagate despite release failure"
      doAssert caught.parent of Defect,
        "the original Defect must be preserved as parent"
      doAssert pool.activeCount == 0

    waitFor t()

suite "E2E: withPipeline macro hygiene":
  test "withPipeline conn binding is scoped to the macro":
    # Regression: the macro-local `conn` (exposed to the body) must not collide
    # with a caller-side `conn`, and must not leak past the macro expansion.
    doAssert compiles(
      block:
        proc t() {.async.} =
          let pool =
            await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 1))
          let conn = "caller-side conn"
          pool.withPipeline(pipe):
            discard await conn.exec("SELECT 1")

    )
    doAssert compiles(
      block:
        proc t() {.async.} =
          let pool =
            await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 1))
          pool.withPipeline(pipe):
            discard conn

    )
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let pool =
            await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 1))
          pool.withPipeline(pipe):
            discard await conn.exec("SELECT 1")
          # A PgConnection-specific access compiles only if the macro-local
          # `conn` leaked into the caller's scope — `echo conn` would not,
          # because PgConnection has no `$` even when the leak exists.
          discard conn.state

    )

suite "E2E: pipelined dispatch inline-param failure":
  proc badInlineParam(): seq[PgParamInline] =
    ## A `PgParamInline` with `len` beyond its `overflow` buffer — the inline
    ## encoder raises a `PgTypeError` mid-dispatch. The Defect arms of the same
    ## dispatch paths are covered by `test_tx_cleanup_defect`.
    @[PgParamInline(oid: 0, format: 1, len: int32(PgInlineBufSize + 1), overflow: @[])]

  test "single pipelined op encode failure fails the op future, not the dispatch task":
    # An encode failure in the single-op dispatch path must fail the op's
    # future and release the connection, not hang the op / kill the task.
    proc t() {.async.} =
      let pool = await newPool(
        initPoolConfig(plainConfig(), minSize = 0, maxSize = 1, pipelined = true)
      )
      defer:
        await pool.close()

      var raised = false
      try:
        discard await pool.exec("SELECT $1", badInlineParam())
      except PgTypeError:
        raised = true
      doAssert raised, "op future must fail with PgTypeError, not hang"
      doAssert pool.activeCount == 0, "connection must be released after the raise"

      let res = await pool.query("SELECT 1")
      doAssert res.rows.len == 1, "pool must remain usable after the raise"

    waitFor t()

  test "non-pipelined inline-param failure releases the connection":
    # Regression: a raise from the body's synchronous prelude (inline-param
    # encoding) must release the connection and reach the caller verbatim.
    proc t() {.async.} =
      let pool = await newPool(
        initPoolConfig(
          plainConfig(), minSize = 0, maxSize = 1, healthCheckTimeout = ZeroDuration
        )
      )
      defer:
        await pool.close()

      var raised = false
      try:
        discard await pool.exec("SELECT $1", badInlineParam())
      except PgTypeError:
        raised = true
      doAssert raised, "inline-param failure must propagate to the caller"
      doAssert pool.activeCount == 0, "connection must be released after the raise"

      let res = await pool.query("SELECT 1")
      doAssert res.rows.len == 1, "pool must remain usable after the raise"

    waitFor t()

  test "batch op encode failure fails only the offending op":
    # An encode failure while building a pipeline batch must fail just that op:
    # its batch-mates never issued the bad statement.
    proc t() {.async.} =
      let pool = await newPool(
        initPoolConfig(plainConfig(), minSize = 0, maxSize = 1, pipelined = true)
      )
      defer:
        await pool.close()

      # Enqueue both ops synchronously: dispatch runs on the next loop tick, so
      # both are queued before it drains, pinning the batch (executeBatch) path.
      # Only that path exercises the per-op isolation, so the pendingOps asserts
      # are meant to fail loudly if dispatch ever becomes synchronous.
      let f1 = pool.exec("SELECT $1", badInlineParam())
      doAssert pool.pendingOps.len == 1
      let f2 = pool.exec("SELECT $1", @[toPgParamInline(1'i32)])
      doAssert pool.pendingOps.len == 2,
        "both ops must be queued before dispatch drains them as a batch"
      var raised = false
      try:
        discard await f1
      except PgTypeError:
        raised = true
      doAssert raised, "the offending op must fail with PgTypeError"
      discard await f2
      doAssert pool.activeCount == 0, "connection must be released after the raise"

    waitFor t()

suite "E2E: pool withTransaction body-Defect handling":
  test "body Defect rolls back, releases, and re-raises":
    # Regression: a body Defect must still ROLLBACK server-side, release the
    # pool slot, and surface as `PgPoolError` (parent = Defect).
    proc t() {.async.} =
      let pool = await newPool(initPoolConfig(plainConfig(), minSize = 0, maxSize = 2))
      defer:
        await pool.close()

      discard await pool.simpleExec("DROP TABLE IF EXISTS test_pool_tx_defect")
      discard await pool.simpleExec(
        "CREATE TABLE test_pool_tx_defect (id serial PRIMARY KEY, val text)"
      )

      var caught: ref PgPoolError = nil
      try:
        pool.withTransaction(conn):
          discard await conn.exec(
            "INSERT INTO test_pool_tx_defect (val) VALUES ($1)", @[toPgParam("leak")]
          )
          raise newException(AssertionDefect, "boom")
      except PgPoolError as e:
        caught = e
      doAssert caught != nil, "body Defect must surface as PgPoolError"
      doAssert caught.parent of Defect,
        "the original Defect must be preserved as parent"
      doAssert pool.activeCount == 0, "connection must be released after Defect"

      # ROLLBACK must have run: the insert must not survive the Defect.
      let leaked =
        await pool.simpleQuery("SELECT count(*) AS n FROM test_pool_tx_defect")
      doAssert leaked.len == 1
      doAssert leaked[0].rows.len == 1
      doAssert leaked[0].rows[0].getStr(0) == "0", "ROLLBACK must have run"

      # Connection is reusable after the ROLLBACK.
      let res = await pool.query("SELECT 1")
      doAssert res.rows.len == 1, "pool must remain usable after Defect"

      discard await pool.simpleExec("DROP TABLE test_pool_tx_defect")

    waitFor t()
