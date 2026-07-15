import std/[unittest, options, strutils, tables, math, importutils, net]

import
  ../async_postgres/[
    async_backend, pg_protocol, pg_types, pg_client, pg_pool, pg_pool_cluster,
    pg_connection,
  ]

import e2e_common

privateAccess(PgConnection)

suite "E2E: Transaction":
  test "withTransaction commits on success":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx")
      discard await conn.exec("CREATE TABLE test_tx (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        discard await conn.exec(
          "INSERT INTO test_tx (val) VALUES ($1)", @[toPgParam("committed")]
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
      var shouldRaise = true
      try:
        conn.withTransaction:
          discard await conn.exec(
            "INSERT INTO test_tx_rb (val) VALUES ($1)", @[toPgParam("rollback_me")]
          )
          if shouldRaise:
            raise newException(ValueError, "intentional error")
      except ValueError:
        raised = true

      doAssert raised
      let res = await conn.query("SELECT val FROM test_tx_rb")
      doAssert res.rows.len == 0

      discard await conn.exec("DROP TABLE test_tx_rb")
      await conn.close()

    waitFor t()

  test "withTransactionRetry retries on serialization failure then commits":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_retry")
      discard
        await conn.exec("CREATE TABLE test_tx_retry (id serial PRIMARY KEY, val text)")

      var attempts = 0
      conn.withTransactionRetry(
        RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false)
      ):
        inc attempts
        discard await conn.exec(
          "INSERT INTO test_tx_retry (val) VALUES ($1)", @[toPgParam("retried")]
        )
        if attempts < 3:
          raise (ref PgQueryError)(
            msg: "synthetic serialization failure", sqlState: "40001"
          )

      doAssert attempts == 3
      # Only the committed (3rd) attempt's INSERT must survive; the first two
      # were rolled back.
      let res = await conn.query("SELECT val FROM test_tx_retry")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "retried"

      discard await conn.exec("DROP TABLE test_tx_retry")
      await conn.close()

    waitFor t()

  test "withTransactionRetry does not retry a non-retryable error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var attempts = 0
      var raised = false
      try:
        {.push warning[UnreachableCode]: off.} # body always raises
        conn.withTransactionRetry(RetryOptions(maxAttempts: 5, baseDelayMs: 1)):
          inc attempts
          raise (ref PgQueryError)(msg: "unique violation", sqlState: "23505")
        {.pop.}
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "23505"

      doAssert raised
      doAssert attempts == 1

      await conn.close()

    waitFor t()

  test "withTransactionRetry exhausts maxAttempts and raises the last error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var attempts = 0
      var raised = false
      try:
        {.push warning[UnreachableCode]: off.} # body always raises
        conn.withTransactionRetry(
          RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false)
        ):
          inc attempts
          raise (ref PgQueryError)(msg: "deadlock", sqlState: "40P01")
        {.pop.}
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "40P01"

      doAssert raised
      doAssert attempts == 3

      await conn.close()

    waitFor t()

  test "withTransactionRetry rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransactionRetry(RetryOptions()):
            return

    )

  test "isRetryableTxError / backoffDelayMs unit behavior":
    let opts = RetryOptions()
    # default retryable states
    doAssert isRetryableTxError(
      (ref PgQueryError)(sqlState: "40001"), opts.retryableStates
    )
    doAssert isRetryableTxError(
      (ref PgQueryError)(sqlState: "40P01"), opts.retryableStates
    )
    doAssert not isRetryableTxError(
      (ref PgQueryError)(sqlState: "23505"), opts.retryableStates
    )
    # non-PgQueryError is never retryable
    doAssert not isRetryableTxError((ref ValueError)(msg: "x"), opts.retryableStates)
    # exponential growth, capped, jitter off => deterministic
    let g =
      RetryOptions(baseDelayMs: 10, maxDelayMs: 100, multiplier: 2.0, jitter: false)
    doAssert backoffDelayMs(g, 1) == 10
    doAssert backoffDelayMs(g, 2) == 20
    doAssert backoffDelayMs(g, 3) == 40
    doAssert backoffDelayMs(g, 10) == 100 # capped at maxDelayMs

  test "pool.withTransactionRetry retries on serialization failure then commits":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 2))
      discard await pool.exec("DROP TABLE IF EXISTS test_pool_retry")
      discard await pool.exec(
        "CREATE TABLE test_pool_retry (id serial PRIMARY KEY, val text)"
      )

      var attempts = 0
      pool.withTransactionRetry(
        RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false), conn
      ):
        inc attempts
        discard await conn.exec(
          "INSERT INTO test_pool_retry (val) VALUES ($1)", @[toPgParam("ok")]
        )
        if attempts < 3:
          raise (ref PgQueryError)(msg: "synthetic", sqlState: "40001")

      doAssert attempts == 3
      let res = await pool.query("SELECT val FROM test_pool_retry")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "ok"

      discard await pool.exec("DROP TABLE test_pool_retry")
      await pool.close()

    waitFor t()

  test "pool.withTransactionRetry exhausts maxAttempts and raises":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 2))
      var attempts = 0
      var raised = false
      try:
        {.push warning[UnreachableCode]: off.} # body always raises
        pool.withTransactionRetry(
          RetryOptions(maxAttempts: 2, baseDelayMs: 1, jitter: false), conn
        ):
          inc attempts
          raise (ref PgQueryError)(msg: "deadlock", sqlState: "40P01")
        {.pop.}
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "40P01"

      doAssert raised
      doAssert attempts == 2
      await pool.close()

    waitFor t()

  test "pool.withTransactionRetry rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let pool =
            await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 1))
          pool.withTransactionRetry(RetryOptions(), conn):
            return

    )

  test "pool.withTransactionRetry does not leak a connection when retryOpts raises":
    proc raisingRetryOpts(): RetryOptions =
      raise newException(ValueError, "compute retryOpts failed")

    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 0, maxSize: 1))

      var raised = false
      try:
        pool.withTransactionRetry(raisingRetryOpts(), conn):
          discard await conn.exec("SELECT 1")
      except ValueError:
        raised = true

      doAssert raised
      doAssert pool.activeCount == 0
      # A leaked slot would starve this acquire on a maxSize=1 pool.
      let c = await pool.acquire()
      c.release()
      await pool.close()

    waitFor t()

  test "cluster.withTransactionRetry retries on serialization failure then commits":
    proc t() {.async.} =
      # Point both primary and replica at the local server. Set tsaReadWrite
      # explicitly so newPoolCluster does not override the replica with
      # tsaPreferStandby (which the single standalone server would reject).
      var cfg = plainConfig()
      cfg.targetSessionAttrs = tsaReadWrite
      let cluster = await newPoolCluster(
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2),
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2),
      )
      discard await cluster.primaryPool.exec("DROP TABLE IF EXISTS test_cluster_retry")
      discard await cluster.primaryPool.exec(
        "CREATE TABLE test_cluster_retry (id serial PRIMARY KEY, val text)"
      )

      var attempts = 0
      cluster.withTransactionRetry(
        RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false), conn
      ):
        inc attempts
        discard await conn.exec(
          "INSERT INTO test_cluster_retry (val) VALUES ($1)", @[toPgParam("ok")]
        )
        if attempts < 3:
          raise (ref PgQueryError)(msg: "synthetic", sqlState: "40001")

      doAssert attempts == 3
      let res = await cluster.primaryPool.query("SELECT val FROM test_cluster_retry")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "ok"

      discard await cluster.primaryPool.exec("DROP TABLE test_cluster_retry")
      await cluster.close()

    waitFor t()

  test "cluster.withTransactionRetry rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          var cfg = plainConfig()
          cfg.targetSessionAttrs = tsaReadWrite
          let cluster = await newPoolCluster(
            PoolConfig(connConfig: cfg, minSize: 1, maxSize: 1),
            PoolConfig(connConfig: cfg, minSize: 1, maxSize: 1),
          )
          cluster.withTransactionRetry(RetryOptions(), conn):
            return

    )

  test "cluster.withTransactionRetry does not leak a connection when retryOpts raises":
    proc raisingRetryOpts(): RetryOptions =
      raise newException(ValueError, "compute retryOpts failed")

    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.targetSessionAttrs = tsaReadWrite
      let cluster = await newPoolCluster(
        PoolConfig(connConfig: cfg, minSize: 0, maxSize: 1),
        PoolConfig(connConfig: cfg, minSize: 0, maxSize: 1),
      )

      var raised = false
      try:
        cluster.withTransactionRetry(raisingRetryOpts(), conn):
          discard await conn.exec("SELECT 1")
      except ValueError:
        raised = true

      doAssert raised
      doAssert cluster.primaryPool.activeCount == 0
      let c = await cluster.primaryPool.acquire()
      c.release()
      await cluster.close()

    waitFor t()

  test "pool.withTransaction commits on success":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptx")
      discard await pool.exec("CREATE TABLE test_ptx (id serial PRIMARY KEY, val text)")

      pool.withTransaction(conn):
        discard await conn.exec(
          "INSERT INTO test_ptx (val) VALUES ($1)", @[toPgParam("pool_commit")]
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
      var shouldRaise = true
      try:
        pool.withTransaction(conn):
          discard await conn.exec(
            "INSERT INTO test_ptx_rb (val) VALUES ($1)", @[toPgParam("pool_rollback")]
          )
          if shouldRaise:
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
      var shouldRaise = true
      try:
        conn.withTransaction:
          let pidRes = await conn.query("SELECT pg_backend_pid()")
          let pid = pidRes.rows[0].getStr(0)
          # Kill the connection from another session
          discard await killer.query(
            "SELECT pg_terminate_backend($1)", @[toPgParam(parseInt(pid).int32)]
          )
          # Give the server a moment to terminate the backend
          await sleepAsync(milliseconds(100))
          if shouldRaise:
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
      var shouldRaise = true
      try:
        pool.withTransaction(conn):
          let pidRes = await conn.query("SELECT pg_backend_pid()")
          let pid = pidRes.rows[0].getStr(0)
          discard await killer.query(
            "SELECT pg_terminate_backend($1)", @[toPgParam(parseInt(pid).int32)]
          )
          await sleepAsync(milliseconds(100))
          if shouldRaise:
            raise newException(ValueError, "original error")
      except ValueError as e:
        raised = true
        doAssert e.msg == "original error"

      doAssert raised
      await killer.close()
      await pool.close()

    waitFor t()

  test "withTransaction with timeout only":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_timeout")
      discard await conn.exec(
        "CREATE TABLE test_tx_timeout (id serial PRIMARY KEY, val text)"
      )

      conn.withTransaction(seconds(5)):
        discard await conn.exec(
          "INSERT INTO test_tx_timeout (val) VALUES ($1)", @[toPgParam("timeout_only")]
        )

      let res = await conn.query("SELECT val FROM test_tx_timeout")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "timeout_only"

      discard await conn.exec("DROP TABLE test_tx_timeout")
      await conn.close()

    waitFor t()

  test "withTransaction with isolation level commits":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_iso")
      discard
        await conn.exec("CREATE TABLE test_tx_iso (id serial PRIMARY KEY, val text)")

      conn.withTransaction(TransactionOptions(isolation: ilSerializable)):
        discard await conn.exec(
          "INSERT INTO test_tx_iso (val) VALUES ($1)", @[toPgParam("serializable")]
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
            "INSERT INTO test_tx_ro (val) VALUES ($1)", @[toPgParam("nope")]
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
          "INSERT INTO test_ptx_opts (val) VALUES ($1)", @[toPgParam("pool_opts")]
        )

      let res = await pool.query("SELECT val FROM test_ptx_opts")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pool_opts"

      discard await pool.exec("DROP TABLE test_ptx_opts")
      await pool.close()

    waitFor t()

  test "pool.withTransaction with timeout only":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptx_timeout")
      discard await pool.exec(
        "CREATE TABLE test_ptx_timeout (id serial PRIMARY KEY, val text)"
      )

      pool.withTransaction(conn, seconds(5)):
        discard await conn.exec(
          "INSERT INTO test_ptx_timeout (val) VALUES ($1)", @[toPgParam("pool_timeout")]
        )

      let res = await pool.query("SELECT val FROM test_ptx_timeout")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pool_timeout"

      discard await pool.exec("DROP TABLE test_ptx_timeout")
      await pool.close()

    waitFor t()

  test "withTransaction with TransactionOptions variable":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_opts_var")
      discard await conn.exec(
        "CREATE TABLE test_tx_opts_var (id serial PRIMARY KEY, val text)"
      )

      let opts = TransactionOptions(isolation: ilSerializable)
      conn.withTransaction(opts):
        discard await conn.exec(
          "INSERT INTO test_tx_opts_var (val) VALUES ($1)", @[toPgParam("opts_var")]
        )

      let res = await conn.query("SELECT val FROM test_tx_opts_var")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "opts_var"

      discard await conn.exec("DROP TABLE test_tx_opts_var")
      await conn.close()

    waitFor t()

  test "withTransaction with Duration variable":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_dur_var")
      discard await conn.exec(
        "CREATE TABLE test_tx_dur_var (id serial PRIMARY KEY, val text)"
      )

      let timeout = seconds(5)
      conn.withTransaction(timeout):
        discard await conn.exec(
          "INSERT INTO test_tx_dur_var (val) VALUES ($1)", @[toPgParam("dur_var")]
        )

      let res = await conn.query("SELECT val FROM test_tx_dur_var")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "dur_var"

      discard await conn.exec("DROP TABLE test_tx_dur_var")
      await conn.close()

    waitFor t()

  test "pool.withTransaction with TransactionOptions variable":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptx_opts_var")
      discard await pool.exec(
        "CREATE TABLE test_ptx_opts_var (id serial PRIMARY KEY, val text)"
      )

      let opts = TransactionOptions(isolation: ilRepeatableRead)
      pool.withTransaction(conn, opts):
        discard await conn.exec(
          "INSERT INTO test_ptx_opts_var (val) VALUES ($1)",
          @[toPgParam("pool_opts_var")],
        )

      let res = await pool.query("SELECT val FROM test_ptx_opts_var")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pool_opts_var"

      discard await pool.exec("DROP TABLE test_ptx_opts_var")
      await pool.close()

    waitFor t()

  test "pool.withTransaction with Duration variable":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptx_dur_var")
      discard await pool.exec(
        "CREATE TABLE test_ptx_dur_var (id serial PRIMARY KEY, val text)"
      )

      let timeout = seconds(5)
      pool.withTransaction(conn, timeout):
        discard await conn.exec(
          "INSERT INTO test_ptx_dur_var (val) VALUES ($1)", @[toPgParam("pool_dur_var")]
        )

      let res = await pool.query("SELECT val FROM test_ptx_dur_var")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pool_dur_var"

      discard await pool.exec("DROP TABLE test_ptx_dur_var")
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
            "INSERT INTO test_sp (val) VALUES ($1)", @[toPgParam("saved")]
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

      var shouldRaise = true
      conn.withTransaction:
        discard await conn.exec(
          "INSERT INTO test_sp_rb (val) VALUES ($1)", @[toPgParam("before")]
        )
        try:
          conn.withSavepoint:
            discard await conn.exec(
              "INSERT INTO test_sp_rb (val) VALUES ($1)", @[toPgParam("inner")]
            )
            if shouldRaise:
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

      var shouldRaise = true
      conn.withTransaction:
        conn.withSavepoint:
          discard await conn.exec(
            "INSERT INTO test_sp_nest (val) VALUES ($1)", @[toPgParam("outer")]
          )
          try:
            conn.withSavepoint:
              discard await conn.exec(
                "INSERT INTO test_sp_nest (val) VALUES ($1)", @[toPgParam("inner")]
              )
              if shouldRaise:
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
            "INSERT INTO test_sp_named (val) VALUES ($1)", @[toPgParam("named")]
          )

      let res = await conn.query("SELECT val FROM test_sp_named")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "named"

      discard await conn.exec("DROP TABLE test_sp_named")
      await conn.close()

    waitFor t()

  test "withSavepoint with timeout and name":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_sp_timeout_name")
      discard await conn.exec(
        "CREATE TABLE test_sp_timeout_name (id serial PRIMARY KEY, val text)"
      )

      conn.withTransaction:
        conn.withSavepoint("my_sp_tn", seconds(5)):
          discard await conn.exec(
            "INSERT INTO test_sp_timeout_name (val) VALUES ($1)",
            @[toPgParam("timeout_name")],
          )

      let res = await conn.query("SELECT val FROM test_sp_timeout_name")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "timeout_name"

      discard await conn.exec("DROP TABLE test_sp_timeout_name")
      await conn.close()

    waitFor t()

  test "withTransaction rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransaction:
            return

    )

  test "withSavepoint rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withSavepoint:
            return

    )

  test "withSavepoint (named) rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withSavepoint("sp"):
            return

    )

  test "pool.withTransaction rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let pool =
            await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 1))
          pool.withTransaction(conn):
            return

    )

  test "return inside nested proc is allowed in withTransaction":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransaction:
            let inner = proc() =
              return
            inner()

    )

  test "withTransaction rejects break escaping to an enclosing loop":
    # Without the guard the `break` binds to the caller's `for` and skips COMMIT.
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          for i in 0 ..< 3:
            conn.withTransaction:
              break

    )

  test "withTransaction rejects continue escaping to an enclosing loop":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          for i in 0 ..< 3:
            conn.withTransaction:
              continue

    )

  test "pool.withTransaction rejects break escaping to an enclosing loop":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let pool =
            await newPool(initPoolConfig(plainConfig(), minSize = 1, maxSize = 1))
          for i in 0 ..< 3:
            pool.withTransaction(conn):
              break

    )

  test "withTransactionRetry rejects break at compile time":
    # The retry loop is a `while true` the macro owns; a body `break` would bind
    # to it and skip COMMIT even without a caller loop.
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransactionRetry(RetryOptions()):
            break

    )

  test "withTransactionRetry rejects continue at compile time":
    # A body `continue` would re-run BEGIN on the existing transaction.
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransactionRetry(RetryOptions()):
            continue

    )

  test "withSavepoint rejects break escaping to an enclosing loop":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          for i in 0 ..< 3:
            conn.withSavepoint:
              break

    )

  test "break/continue inside a nested loop is allowed in withTransaction":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransaction:
            for i in 0 ..< 3:
              if i == 1:
                continue
              if i == 2:
                break
              discard i

    )

  test "break inside a nested block is allowed in withTransaction":
    doAssert compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransaction:
            block inner:
              break inner

    )

  test "withTransaction skips ROLLBACK when COMMIT fails":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_commit_fail")
      discard await conn.exec(
        """
        CREATE TABLE test_tx_commit_fail (
          id int PRIMARY KEY,
          ref_id int REFERENCES test_tx_commit_fail(id)
            DEFERRABLE INITIALLY DEFERRED
        )
        """
      )

      var queries = newSeq[string]()
      let tracer = PgTracer()
      tracer.onQueryStart = proc(
          c: PgConnection, data: TraceQueryStartData
      ): TraceContext {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          queries.add(data.sql)
        return nil
      conn.tracer = tracer

      var raised = false
      try:
        conn.withTransaction:
          discard await conn.exec("INSERT INTO test_tx_commit_fail VALUES (1, 999)")
      except PgQueryError:
        raised = true

      doAssert raised
      doAssert "COMMIT" in queries
      doAssert "ROLLBACK" notin queries
      doAssert conn.txStatus == tsIdle

      conn.tracer = nil
      discard await conn.exec("DROP TABLE test_tx_commit_fail")
      await conn.close()

    waitFor t()

  test "withSavepoint skips ROLLBACK TO SAVEPOINT when outer transaction has ended":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var queries = newSeq[string]()
      let tracer = PgTracer()
      tracer.onQueryStart = proc(
          c: PgConnection, data: TraceQueryStartData
      ): TraceContext {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          queries.add(data.sql)
        return nil
      conn.tracer = tracer

      discard await conn.exec("BEGIN")
      var raised = false
      try:
        {.push warning[UnreachableCode]: off.} # body always raises
        conn.withSavepoint("sp_outer_done"):
          # End the outer transaction from inside the savepoint, so the
          # savepoint no longer exists by the time we raise.
          discard await conn.exec("ROLLBACK")
          raise newException(ValueError, "boom")
        {.pop.}
      except ValueError:
        raised = true

      doAssert raised
      doAssert "SAVEPOINT \"sp_outer_done\"" in queries
      var hasRollbackToSp = false
      for q in queries:
        if q.startsWith("ROLLBACK TO SAVEPOINT"):
          hasRollbackToSp = true
          break
      doAssert not hasRollbackToSp
      doAssert conn.txStatus == tsIdle

      conn.tracer = nil
      await conn.close()

    waitFor t()

  test "withSavepoint quotes savepoint name (SQL injection guard)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS sp_injection_probe2")
      discard
        await conn.exec("CREATE TABLE sp_injection_probe2 (id serial PRIMARY KEY)")

      var queries = newSeq[string]()
      let tracer = PgTracer()
      tracer.onQueryStart = proc(
          c: PgConnection, data: TraceQueryStartData
      ): TraceContext {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          queries.add(data.sql)
        return nil
      conn.tracer = tracer

      # withSavepoint requires a string-literal name; still worth verifying it
      # is emitted quoted so identifier-legal characters (e.g. dashes) round-trip.
      conn.withTransaction:
        conn.withSavepoint("odd-name"):
          discard await conn.exec("SELECT 1")

      var sawSavepoint = false
      var sawRelease = false
      for q in queries:
        if q == "SAVEPOINT \"odd-name\"":
          sawSavepoint = true
        elif q == "RELEASE SAVEPOINT \"odd-name\"":
          sawRelease = true
      doAssert sawSavepoint
      doAssert sawRelease

      conn.tracer = nil
      discard await conn.exec("DROP TABLE sp_injection_probe2")
      await conn.close()

    waitFor t()

suite "E2E: Deadline-bounded Transaction":
  test "withTransactionDeadline commits on success":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_txd")
      discard await conn.exec("CREATE TABLE test_txd (id serial PRIMARY KEY, val text)")

      conn.withTransactionDeadline(seconds(5)):
        discard await conn.exec(
          "INSERT INTO test_txd (val) VALUES ($1)", @[toPgParam("deadline_commit")]
        )

      let res = await conn.query("SELECT val FROM test_txd")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "deadline_commit"
      doAssert conn.txStatus == tsIdle

      discard await conn.exec("DROP TABLE test_txd")
      await conn.close()

    waitFor t()

  test "withTransactionDeadline rolls back on body exception":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_txd_rb")
      discard
        await conn.exec("CREATE TABLE test_txd_rb (id serial PRIMARY KEY, val text)")

      var raised = false
      var shouldRaise = true
      try:
        conn.withTransactionDeadline(seconds(5)):
          discard await conn.exec(
            "INSERT INTO test_txd_rb (val) VALUES ($1)", @[toPgParam("nope")]
          )
          if shouldRaise:
            raise newException(ValueError, "intentional")
      except ValueError:
        raised = true

      doAssert raised
      let res = await conn.query("SELECT val FROM test_txd_rb")
      doAssert res.rows.len == 0
      doAssert conn.txStatus == tsIdle

      discard await conn.exec("DROP TABLE test_txd_rb")
      await conn.close()

    waitFor t()

  test "withTransactionDeadline raises PgTimeoutError when body exceeds deadline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var raised = false
      try:
        conn.withTransactionDeadline(milliseconds(300)):
          # pg_sleep blocks server-side for 2s — exceeds the 300ms deadline.
          discard await conn.query("SELECT pg_sleep(2)")
      except PgTimeoutError:
        raised = true

      doAssert raised
      # Connection is invalidated; not safe to reuse.
      doAssert conn.state == csClosed

      await conn.close()

    waitFor t()

  test "withTransactionDeadline with TransactionOptions commits":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      conn.withTransactionDeadline(
        TransactionOptions(isolation: ilSerializable), seconds(5)
      ):
        let res = await conn.query("SELECT 1")
        doAssert res.rows.len == 1

      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "withSavepointDeadline releases on success":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_spd")
      discard await conn.exec("CREATE TABLE test_spd (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        conn.withSavepointDeadline(seconds(5)):
          discard await conn.exec(
            "INSERT INTO test_spd (val) VALUES ($1)", @[toPgParam("sp_deadline")]
          )

      let res = await conn.query("SELECT val FROM test_spd")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "sp_deadline"

      discard await conn.exec("DROP TABLE test_spd")
      await conn.close()

    waitFor t()

  test "withSavepointDeadline rolls back to savepoint on body exception":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_spd_rb")
      discard
        await conn.exec("CREATE TABLE test_spd_rb (id serial PRIMARY KEY, val text)")

      var shouldRaise = true
      conn.withTransaction:
        discard await conn.exec(
          "INSERT INTO test_spd_rb (val) VALUES ($1)", @[toPgParam("outer")]
        )
        try:
          conn.withSavepointDeadline("sp1", seconds(5)):
            discard await conn.exec(
              "INSERT INTO test_spd_rb (val) VALUES ($1)", @[toPgParam("inner")]
            )
            if shouldRaise:
              raise newException(ValueError, "sp error")
        except ValueError:
          discard

      let res = await conn.query("SELECT val FROM test_spd_rb ORDER BY id")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "outer"

      discard await conn.exec("DROP TABLE test_spd_rb")
      await conn.close()

    waitFor t()

  test "pool.withTransactionDeadline commits on success":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptxd")
      discard
        await pool.exec("CREATE TABLE test_ptxd (id serial PRIMARY KEY, val text)")

      pool.withTransactionDeadline(conn, seconds(5)):
        discard await conn.exec(
          "INSERT INTO test_ptxd (val) VALUES ($1)", @[toPgParam("pool_deadline")]
        )

      let res = await pool.query("SELECT val FROM test_ptxd")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "pool_deadline"

      discard await pool.exec("DROP TABLE test_ptxd")
      await pool.close()

    waitFor t()

  test "withTransactionRetryDeadline retries within budget then commits":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_txrd")
      discard
        await conn.exec("CREATE TABLE test_txrd (id serial PRIMARY KEY, val text)")

      var attempts = 0
      conn.withTransactionRetryDeadline(
        RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false), seconds(10)
      ):
        inc attempts
        discard
          await conn.exec("INSERT INTO test_txrd (val) VALUES ($1)", @[toPgParam("rd")])
        if attempts < 3:
          raise (ref PgQueryError)(msg: "synthetic", sqlState: "40001")

      doAssert attempts == 3
      let res = await conn.query("SELECT val FROM test_txrd")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "rd"

      discard await conn.exec("DROP TABLE test_txrd")
      await conn.close()

    waitFor t()

  test "withTransactionRetryDeadline raises PgTimeoutError when body exceeds deadline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      try:
        conn.withTransactionRetryDeadline(
          RetryOptions(maxAttempts: 5), milliseconds(300)
        ):
          discard await conn.exec("SELECT pg_sleep(2)")
      except PgTimeoutError:
        raised = true
      doAssert raised
      # Connection invalidated on timeout; close it.
      await conn.close()

    waitFor t()

  test "withTransactionRetryDeadline exhausts retries and raises the last error":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var attempts = 0
      var raised = false
      try:
        # Long deadline so the loop ends by exhausting maxAttempts (a retryable
        # error every time), not by running out of budget.
        {.push warning[UnreachableCode]: off.} # body always raises
        conn.withTransactionRetryDeadline(
          RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false), seconds(10)
        ):
          inc attempts
          raise (ref PgQueryError)(msg: "deadlock", sqlState: "40P01")
        {.pop.}
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "40P01"

      doAssert raised
      doAssert attempts == 3

      await conn.close()

    waitFor t()

  test "withTransactionRetryDeadline rejects return at compile time":
    doAssert not compiles(
      block:
        proc t() {.async.} =
          let conn = await connect(plainConfig())
          conn.withTransactionRetryDeadline(RetryOptions(), seconds(5)):
            return

    )

  test "pool.withTransactionRetryDeadline retries within budget then commits":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))
      discard await pool.exec("DROP TABLE IF EXISTS test_ptxrd")
      discard
        await pool.exec("CREATE TABLE test_ptxrd (id serial PRIMARY KEY, val text)")

      var attempts = 0
      pool.withTransactionRetryDeadline(
        RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false), conn, seconds(10)
      ):
        inc attempts
        discard await conn.exec(
          "INSERT INTO test_ptxrd (val) VALUES ($1)", @[toPgParam("prd")]
        )
        if attempts < 3:
          raise (ref PgQueryError)(msg: "synthetic", sqlState: "40001")

      doAssert attempts == 3
      let res = await pool.query("SELECT val FROM test_ptxrd")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "prd"

      discard await pool.exec("DROP TABLE test_ptxrd")
      await pool.close()

    waitFor t()

  test "pool.withTransactionRetryDeadline exhausts retries and raises the last error":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))

      var attempts = 0
      var raised = false
      try:
        {.push warning[UnreachableCode]: off.} # body always raises
        pool.withTransactionRetryDeadline(
          RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false), conn, seconds(10)
        ):
          inc attempts
          raise (ref PgQueryError)(msg: "deadlock", sqlState: "40P01")
        {.pop.}
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "40P01"

      doAssert raised
      doAssert attempts == 3

      # Pool still usable after exhausting retries (each attempt released its
      # connection cleanly via the per-attempt finally).
      let res = await pool.query("SELECT 1::int")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 1'i32

      await pool.close()

    waitFor t()

  test "pool.withTransactionRetryDeadline drops invalidated connection on deadline":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))

      var raised = false
      try:
        # The body overruns the deadline: the in-flight connection is
        # invalidated (timeoutElse) and PgTimeoutError raised — never retried.
        pool.withTransactionRetryDeadline(
          RetryOptions(maxAttempts: 5), conn, milliseconds(300)
        ):
          discard await conn.query("SELECT pg_sleep(2)")
      except PgTimeoutError:
        raised = true

      doAssert raised
      # The bad connection is dropped, not stuck — subsequent pool ops work.
      let res = await pool.query("SELECT 1::int")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 1'i32

      await pool.close()

    waitFor t()

  when hasChronos:
    # Same handoff-poisoning regression guard as the withTransactionDeadline
    # version above; the retry variant's timeoutElse branches identically on
    # `releasedSym`, so a clean unwind must hand a healthy conn to the waiter.
    test "pool.withTransactionRetryDeadline hands off healthy conn when body unwinds cleanly on deadline":
      proc t() {.async.} =
        let pool =
          await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 1))
        let blocker = await pool.acquire()

        var raised = false
        proc macroTask() {.async.} =
          try:
            pool.withTransactionRetryDeadline(
              RetryOptions(maxAttempts: 5), conn, milliseconds(200)
            ):
              # BEGIN completes well within the deadline, leaving the conn
              # csReady. The cancellation arrives at the sleepAsync await with
              # the conn idle, so bodyFn's `except CancelledError` leaves it
              # untouched and finally's resetSession + release hands a healthy
              # conn to the queued waiter.
              await sleepAsync(seconds(1))
          except PgTimeoutError:
            raised = true

        let macroFut = macroTask()
        # Yield so macroTask queues its acquire before the waiter.
        await sleepAsync(milliseconds(5))

        var waiterOk = false
        proc waiterTask() {.async.} =
          let c = await pool.acquire()
          # A poisoned (csClosed) conn handed via FIFO would fail here.
          let res = await c.query("SELECT 1::int")
          waiterOk = res.rows.len == 1 and res.rows[0].getInt(0) == 1'i32
          c.release()

        let waiterFut = waiterTask()

        # Release blocker: conn goes to macroTask (FIFO head), waiter queued.
        blocker.release()

        await macroFut
        doAssert raised, "deadline should have raised PgTimeoutError"

        # macroTask released a healthy conn; waiter should complete with a
        # usable connection rather than a poisoned one.
        await waiterFut
        doAssert waiterOk, "waiter should have received a usable connection"

        await pool.close()

      waitFor t()

  test "pool.withTransactionDeadline drops invalidated connection on deadline":
    proc t() {.async.} =
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 3))

      var raised = false
      try:
        pool.withTransactionDeadline(conn, milliseconds(300)):
          discard await conn.query("SELECT pg_sleep(2)")
      except PgTimeoutError:
        raised = true

      doAssert raised
      # Subsequent pool ops still work — the bad connection is dropped, not stuck.
      let res = await pool.query("SELECT 1::int")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 1'i32

      await pool.close()

    waitFor t()

  when hasChronos:
    # Under chronos, wait() cancels bodyFn and runs its finally (release)
    # before the timeout handler runs. When the body unwinds cleanly (conn
    # csReady), release() FIFO-hands a healthy connection to a queued waiter,
    # skipping the health check. The handler must NOT then invalidate that
    # already-handed-off connection — a regression that unconditionally calls
    # invalidateOnTimeout would mark it csClosed *after* the handoff, leaving
    # the waiter with a poisoned connection.
    test "pool.withTransactionDeadline hands off healthy conn when body unwinds cleanly on deadline":
      proc t() {.async.} =
        let pool =
          await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 1))
        let blocker = await pool.acquire()

        var raised = false
        proc macroTask() {.async.} =
          try:
            pool.withTransactionDeadline(conn, milliseconds(200)):
              # BEGIN completes well within the deadline, leaving the conn
              # csReady. The cancellation arrives at the sleepAsync await with
              # the conn idle, so bodyFn's `except CancelledError` leaves it
              # untouched and finally's resetSession + release hands a healthy
              # conn to the queued waiter.
              await sleepAsync(seconds(1))
          except PgTimeoutError:
            raised = true

        let macroFut = macroTask()
        # Yield so macroTask queues its acquire before the waiter.
        await sleepAsync(milliseconds(5))

        var waiterOk = false
        proc waiterTask() {.async.} =
          let c = await pool.acquire()
          # A poisoned (csClosed) conn handed via FIFO would fail here.
          let res = await c.query("SELECT 1::int")
          waiterOk = res.rows.len == 1 and res.rows[0].getInt(0) == 1'i32
          c.release()

        let waiterFut = waiterTask()

        # Release blocker: conn goes to macroTask (FIFO head), waiter queued.
        blocker.release()

        await macroFut
        doAssert raised, "deadline should have raised PgTimeoutError"

        # macroTask released a healthy conn; waiter should complete with a
        # usable connection rather than a poisoned one.
        await waiterFut
        doAssert waiterOk, "waiter should have received a usable connection"

        await pool.close()

      waitFor t()

  test "pool.withTransactionDeadline raises PgTimeoutError when acquire times out":
    proc t() {.async.} =
      # maxSize=1: a single held connection forces the next acquire to queue.
      let pool =
        await newPool(PoolConfig(connConfig: plainConfig(), minSize: 1, maxSize: 1))
      let blocker = await pool.acquire()

      var bodyRan = false
      var raised = false
      try:
        pool.withTransactionDeadline(conn, milliseconds(300)):
          # Should never run — acquire must time out before BEGIN.
          bodyRan = true
          discard await conn.exec("SELECT 1")
      except PgTimeoutError:
        raised = true

      doAssert raised
      doAssert not bodyRan

      # Release blocker so any background-queued acquire (asyncdispatch) can
      # settle, then poll until its bodyFn finishes BEGIN/COMMIT and releases
      # the connection (activeCount returns to 0) before pool.close() runs.
      # Bounded by a generous wall-clock cap to avoid hangs on regressions.
      blocker.release()
      let drainDeadline = Moment.now() + seconds(5)
      while pool.activeCount > 0 and Moment.now() < drainDeadline:
        await sleepAsync(milliseconds(10))
      doAssert pool.activeCount == 0,
        "background acquire did not drain within 5s (activeCount=" & $pool.activeCount &
          ")"

      # Pool should still be usable after the timeout.
      let res = await pool.query("SELECT 1::int")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getInt(0) == 1'i32

      await pool.close()

    waitFor t()

  test "withSavepointDeadline raises PgTimeoutError when body exceeds deadline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var raised = false
      try:
        # Interaction note: the inner deadline fires and invalidates the
        # connection (csClosed) before raising PgTimeoutError. The outer
        # withTransaction's `except CatchableError` then tries ROLLBACK on
        # the closed connection — that simpleExec fails immediately, but
        # withTransaction's inner try/except swallows the cleanup error and
        # re-raises the original PgTimeoutError, which is what we catch here.
        conn.withTransaction:
          conn.withSavepointDeadline(milliseconds(300)):
            # pg_sleep blocks server-side for 2s — exceeds the 300ms deadline.
            discard await conn.query("SELECT pg_sleep(2)")
      except PgTimeoutError:
        raised = true

      doAssert raised
      # Connection is invalidated; not safe to reuse.
      doAssert conn.state == csClosed

      await conn.close()

    waitFor t()

  test "withSavepointDeadline with auto-generated name round-trips":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_spd_auto")
      discard
        await conn.exec("CREATE TABLE test_spd_auto (id serial PRIMARY KEY, val text)")

      conn.withTransaction:
        # No name argument — macro generates "_sp_<portalCounter>".
        conn.withSavepointDeadline(seconds(5)):
          discard await conn.exec(
            "INSERT INTO test_spd_auto (val) VALUES ($1)", @[toPgParam("auto1")]
          )
        # Second auto-named savepoint in the same tx must get a distinct name.
        conn.withSavepointDeadline(seconds(5)):
          discard await conn.exec(
            "INSERT INTO test_spd_auto (val) VALUES ($1)", @[toPgParam("auto2")]
          )

      let res = await conn.query("SELECT val FROM test_spd_auto ORDER BY id")
      doAssert res.rows.len == 2
      doAssert res.rows[0].getStr(0) == "auto1"
      doAssert res.rows[1].getStr(0) == "auto2"

      discard await conn.exec("DROP TABLE test_spd_auto")
      await conn.close()

    waitFor t()

  test "withSavepointDeadline quotes savepoint name (SQL injection guard)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS sp_injection_probe")
      discard await conn.exec("CREATE TABLE sp_injection_probe (id serial PRIMARY KEY)")

      # Name containing ';' and embedded '"' — if concatenated unquoted into
      # simpleExec it would execute the trailing DROP TABLE via the simple
      # query protocol's multi-statement support.
      let hostileName = "sp\"; DROP TABLE sp_injection_probe; --"

      var queries = newSeq[string]()
      let tracer = PgTracer()
      tracer.onQueryStart = proc(
          c: PgConnection, data: TraceQueryStartData
      ): TraceContext {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          queries.add(data.sql)
        return nil
      conn.tracer = tracer

      conn.withTransaction:
        conn.withSavepointDeadline(hostileName, seconds(5)):
          discard await conn.exec("SELECT 1")

      # Probe table must survive — injection payload never executed.
      let res = await conn.query(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'sp_injection_probe'"
      )
      doAssert res.rows.len == 1

      # Quoted-identifier form: embedded '"' doubled per SQL rules.
      let quoted = "\"sp\"\"; DROP TABLE sp_injection_probe; --\""
      var sawSavepoint = false
      var sawRelease = false
      for q in queries:
        if q == "SAVEPOINT " & quoted:
          sawSavepoint = true
        elif q == "RELEASE SAVEPOINT " & quoted:
          sawRelease = true
      doAssert sawSavepoint
      doAssert sawRelease

      conn.tracer = nil
      discard await conn.exec("DROP TABLE sp_injection_probe")
      await conn.close()

    waitFor t()

suite "E2E: execInTransaction / queryInTransaction":
  test "execInTransaction commits successfully":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_eit")
      discard await conn.exec("CREATE TABLE test_eit (id serial PRIMARY KEY, val text)")

      let tag = await conn.execInTransaction(
        "INSERT INTO test_eit (val) VALUES ($1)", @[toPgParam("pipelined")]
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
          "INSERT INTO test_eit_err (val) VALUES ($1)", @[toPgParam("existing")]
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

  test "execInTransaction with comment-only SQL does not capture COMMIT tag":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Comment-only (or empty) user SQL yields EmptyQueryResponse instead of
      # CommandComplete. The returned tag must reflect the empty user statement,
      # never the trailing COMMIT.
      let tag = await conn.execInTransaction("-- nothing here")
      doAssert tag == ""
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle

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

  test "queryInTransaction with rfBinary decodes binary rows":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # rfBinary makes the server send binary DataRows; the recv loop must
      # record the requested column formats so accessors take the binary
      # decode path instead of reading raw big-endian bytes as text.
      let qr = await conn.queryInTransaction(
        "SELECT 42::int4, 'hello'::text", @[], resultFormat = rfBinary
      )
      doAssert qr.rows.len == 1
      doAssert qr.fields[0].formatCode == 1
      doAssert qr.fields[1].formatCode == 1
      doAssert qr.rows[0].getInt(0) == 42
      doAssert qr.rows[0].getStr(1) == "hello"

      await conn.close()

    waitFor t()

  test "queryInTransaction with comment-only SQL does not capture COMMIT tag":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Comment-only (or empty) user SQL yields EmptyQueryResponse instead of
      # CommandComplete. commandTag must stay empty, never the trailing COMMIT.
      let qr = await conn.queryInTransaction("/* empty */")
      doAssert qr.rows.len == 0
      doAssert qr.commandTag == ""
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle

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

      let qr = await pool.queryInTransaction("SELECT 42::int4", @[])
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
        doAssert r.commandResult == "INSERT 0 1"

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
      doAssert results[0].commandResult == "INSERT 0 1"

      doAssert results[1].kind == prkQuery
      doAssert results[1].queryResult.rowCount == 1
      doAssert results[1].queryResult.rows[0].getStr(0) == "x"

      doAssert results[2].kind == prkExec
      doAssert results[2].commandResult == "INSERT 0 1"

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

  test "pipeline: execute recovers pre-error cache-miss statements (M-9)":
    # A mid-batch error must not orphan the server-side prepared statements of
    # cache-miss ops that completed before the failure. They are recovered into
    # the cache and reused, instead of being re-parsed under a fresh name.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.stmtCache.len == 0

      let p = newPipeline(conn)
      p.addQuery("SELECT $1::int4", @[toPgParam(7'i32)]) # miss, succeeds
      p.addQuery("SELECT 1 / $1::int4", @[toPgParam(0'i32)]) # miss, Execute fails
      var state = ""
      try:
        discard await p.execute()
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "22012" # division_by_zero

      # Succeeded-before-error op is cached; the failing op is not.
      doAssert conn.stmtCache.hasKey("SELECT $1::int4")
      doAssert not conn.stmtCache.hasKey("SELECT 1 / $1::int4")

      # Reuse: a fresh pipeline with the same SQL is a cache hit and adds no new
      # entry (with the bug it was a miss -> a new entry and a leaked _sc_N).
      let cacheLen = conn.stmtCache.len
      let p2 = newPipeline(conn)
      p2.addQuery("SELECT $1::int4", @[toPgParam(9'i32)])
      let r2 = await p2.execute()
      doAssert r2[0].queryResult.rows[0].getStr(0) == "9"
      doAssert conn.stmtCache.len == cacheLen

      await conn.close()

    waitFor t()

  test "pipeline: execute evicts only the failing op's cached statement (M-9)":
    # On a cache-invalidating error (0A000), only the op that actually failed
    # is evicted — an unrelated cache hit in the same batch is kept — and the
    # evicted (still-live) statement gets a Close queued so it is not orphaned.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.query("DROP TABLE IF EXISTS t_m9_evict")
      discard await conn.query("CREATE TABLE t_m9_evict(a int4)")
      discard await conn.query("INSERT INTO t_m9_evict VALUES (1)")

      # Prime the cache: one unrelated stmt, one whose plan DDL will invalidate.
      discard await conn.query("SELECT 12345::int4")
      discard await conn.query("SELECT * FROM t_m9_evict")
      doAssert conn.stmtCache.hasKey("SELECT 12345::int4")
      doAssert conn.stmtCache.hasKey("SELECT * FROM t_m9_evict")

      discard await conn.query("ALTER TABLE t_m9_evict ADD COLUMN b int4")

      let p = newPipeline(conn)
      p.addQuery("SELECT 12345::int4") # cache hit, succeeds (op 0)
      p.addQuery("SELECT * FROM t_m9_evict") # cache hit, fails 0A000 (op 1)
      var state = ""
      try:
        discard await p.execute()
      except PgQueryError as e:
        state = e.sqlState
      doAssert state == "0A000" # cached plan must not change result type

      doAssert conn.stmtCache.hasKey("SELECT 12345::int4") # unrelated, kept
      doAssert not conn.stmtCache.hasKey("SELECT * FROM t_m9_evict") # evicted
      doAssert conn.pendingStmtCloses.len >= 1 # Close queued for the evicted stmt

      discard await conn.query("DROP TABLE IF EXISTS t_m9_evict")
      await conn.close()

    waitFor t()

  test "pipeline: PgParam raw overload":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addExec("SELECT $1::text", @[toPgParam("hello")])
      p.addQuery("SELECT $1::text", @[toPgParam("world")])
      let results = await p.execute()
      doAssert results[0].kind == prkExec
      doAssert results[1].kind == prkQuery
      doAssert results[1].queryResult.rows[0].getStr(0) == "world"

      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline overload roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_inline_pipe")
      discard
        await conn.exec("CREATE TABLE test_inline_pipe (id serial PRIMARY KEY, v int)")

      let p = newPipeline(conn)
      for i in 0 ..< 5:
        p.addExec(
          "INSERT INTO test_inline_pipe (v) VALUES ($1)", [i.int32.toPgParamInline]
        )
      p.addQuery("SELECT v FROM test_inline_pipe ORDER BY id")
      let results = await p.execute()
      doAssert results.len == 6
      for i in 0 ..< 5:
        doAssert results[i].commandResult == "INSERT 0 1"
      let qr = results[5].queryResult
      doAssert qr.rows.len == 5
      for i in 0 ..< 5:
        doAssert qr.rows[i].getStr(0) == $i

      discard await conn.exec("DROP TABLE test_inline_pipe")
      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline and PgParam overloads can be mixed in one pipeline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_mixed_pipe")
      discard await conn.exec(
        "CREATE TABLE test_mixed_pipe (id serial PRIMARY KEY, v int, s text)"
      )

      let p = newPipeline(conn)
      # inline path
      p.addExec(
        "INSERT INTO test_mixed_pipe (v, s) VALUES ($1, $2)",
        [1.int32.toPgParamInline, "from-inline".toPgParamInline],
      )
      # legacy path
      p.addExec(
        "INSERT INTO test_mixed_pipe (v, s) VALUES ($1, $2)",
        @[toPgParam(2'i32), toPgParam("from-legacy")],
      )
      p.addQuery("SELECT v, s FROM test_mixed_pipe ORDER BY id")
      let results = await p.execute()
      doAssert results.len == 3
      let rows = results[2].queryResult.rows
      doAssert rows.len == 2
      doAssert rows[0].getStr(0) == "1"
      doAssert rows[0].getStr(1) == "from-inline"
      doAssert rows[1].getStr(0) == "2"
      doAssert rows[1].getStr(1) == "from-legacy"

      discard await conn.exec("DROP TABLE test_mixed_pipe")
      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline NULL via Option":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_inline_null")
      discard
        await conn.exec("CREATE TABLE test_inline_null (id serial PRIMARY KEY, v int)")

      let p = newPipeline(conn)
      p.addExec(
        "INSERT INTO test_inline_null (v) VALUES ($1)", [none(int32).toPgParamInline]
      )
      p.addQuery("SELECT v FROM test_inline_null ORDER BY id")
      let results = await p.execute()
      doAssert results[1].queryResult.rows[0].isNull(0)

      discard await conn.exec("DROP TABLE test_inline_null")
      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline overflow path (long string)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_inline_overflow")
      discard await conn.exec(
        "CREATE TABLE test_inline_overflow (id serial PRIMARY KEY, s text)"
      )

      let long = "abcdefghijklmnopqrstuvwxyz0123456789" # 36 chars → overflow
      let p = newPipeline(conn)
      p.addExec(
        "INSERT INTO test_inline_overflow (s) VALUES ($1)", [long.toPgParamInline]
      )
      p.addQuery("SELECT s FROM test_inline_overflow")
      let results = await p.execute()
      doAssert results[1].queryResult.rows[0].getStr(0) == long

      discard await conn.exec("DROP TABLE test_inline_overflow")
      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline large overflow (16KB single value)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_inline_large")
      discard await conn.exec(
        "CREATE TABLE test_inline_large (id serial PRIMARY KEY, s text)"
      )

      # 16 KB payload — well beyond PgInlineBufSize, exercises a single large
      # copy into the SoA inlineData buffer.
      var big = newStringOfCap(16 * 1024)
      for i in 0 ..< 16 * 1024:
        big.add char(ord('a') + (i mod 26))
      let p = newPipeline(conn)
      p.addExec("INSERT INTO test_inline_large (s) VALUES ($1)", [big.toPgParamInline])
      p.addQuery("SELECT s FROM test_inline_large")
      let results = await p.execute()
      doAssert results[0].commandResult == "INSERT 0 1"
      doAssert results[1].queryResult.rows[0].getStr(0) == big

      discard await conn.exec("DROP TABLE test_inline_large")
      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline many large overflows stress SoA reallocation":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_inline_stress")
      discard await conn.exec(
        "CREATE TABLE test_inline_stress (id serial PRIMARY KEY, s text)"
      )

      # 50 × ~2 KB values across 50 ops in one pipeline. Each value is unique
      # so a bug in SoA offset accounting (e.g. slice aliasing, reuse of stale
      # offsets after inlineData grows) surfaces as a mismatched readback.
      let numOps = 50
      let valueSize = 2 * 1024
      var expected = newSeq[string](numOps)
      let p = newPipeline(conn)
      for i in 0 ..< numOps:
        var s = newStringOfCap(valueSize)
        # Prefix with the index so every value is distinct and order-sensitive.
        s.add "op" & $i & ":"
        while s.len < valueSize:
          s.add char(ord('a') + ((i + s.len) mod 26))
        expected[i] = s
        p.addExec("INSERT INTO test_inline_stress (s) VALUES ($1)", [s.toPgParamInline])
      p.addQuery("SELECT s FROM test_inline_stress ORDER BY id")
      let results = await p.execute()
      doAssert results.len == numOps + 1
      for i in 0 ..< numOps:
        doAssert results[i].commandResult == "INSERT 0 1"
      let rows = results[numOps].queryResult.rows
      doAssert rows.len == numOps
      for i in 0 ..< numOps:
        doAssert rows[i].getStr(0) == expected[i]

      discard await conn.exec("DROP TABLE test_inline_stress")
      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline empty params on param-less SQL":
    # Edge case: caller reaches the inline overload with zero params (e.g. a
    # SQL that takes no bound parameters). executeImpl builds an empty
    # openArray via `toOpenArray(start, start-1)`; regression test that the
    # Bind message still goes out cleanly and the query round-trips.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = newPipeline(conn)
      let empty: seq[PgParamInline] = @[]
      p.addQuery("SELECT 42", empty)
      p.addExec("SELECT 1", empty)
      let results = await p.execute()
      doAssert results.len == 2
      doAssert results[0].queryResult.rows[0].getStr(0) == "42"
      doAssert results[1].commandResult == "SELECT 1"
      await conn.close()

    waitFor t()

  test "exec: PgParamInline overload roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_exec_inline")
      discard
        await conn.exec("CREATE TABLE test_exec_inline (id serial PRIMARY KEY, v int)")

      # Multi-value INSERT (the benchmark workload): 100 params in one exec.
      var sql = "INSERT INTO test_exec_inline (v) VALUES "
      var params = newSeqOfCap[PgParamInline](100)
      for j in 0 ..< 100:
        if j > 0:
          sql.add ","
        sql.add "($" & $(j + 1) & ")"
        params.add j.int32.toPgParamInline
      let cr = await conn.exec(sql, params)
      doAssert cr.affectedRows == 100

      let qr =
        await conn.query("SELECT count(*)::int4, max(v)::int4 FROM test_exec_inline")
      doAssert qr.rows[0].getStr(0) == "100"
      doAssert qr.rows[0].getStr(1) == "99"

      discard await conn.exec("DROP TABLE test_exec_inline")
      await conn.close()

    waitFor t()

  test "pipeline: executeIsolated with PgParamInline roundtrip":
    # Covers the inline path through executeIsolatedImpl (per-op SYNC) and
    # its concurrent send/recv scheduling under chronos. Mixes inline and
    # legacy params across ops so both code paths in the op loop execute.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_iso_inline")
      discard await conn.exec(
        "CREATE TABLE test_iso_inline (id serial PRIMARY KEY, v int, s text)"
      )

      let p = newPipeline(conn)
      p.addExec(
        "INSERT INTO test_iso_inline (v, s) VALUES ($1, $2)",
        [10.int32.toPgParamInline, "inline".toPgParamInline],
      )
      p.addExec(
        "INSERT INTO test_iso_inline (v, s) VALUES ($1, $2)",
        @[toPgParam(20'i32), toPgParam("legacy")],
      )
      p.addQuery(
        "SELECT v, s FROM test_iso_inline WHERE v > $1 ORDER BY id",
        [5.int32.toPgParamInline],
      )
      let ir = await p.executeIsolated()
      doAssert ir.results.len == 3
      for e in ir.errors:
        doAssert e == nil
      doAssert ir.results[0].commandResult == "INSERT 0 1"
      doAssert ir.results[1].commandResult == "INSERT 0 1"
      let rows = ir.results[2].queryResult.rows
      doAssert rows.len == 2
      doAssert rows[0].getStr(0) == "10"
      doAssert rows[0].getStr(1) == "inline"
      doAssert rows[1].getStr(0) == "20"
      doAssert rows[1].getStr(1) == "legacy"

      discard await conn.exec("DROP TABLE test_iso_inline")
      await conn.close()

    waitFor t()

  test "pipeline: executeIsolated with PgParamInline isolates per-op errors":
    # Confirms that per-op SYNC still provides error isolation when ops use
    # the inline overload: a failing inline op must not abort subsequent
    # inline ops, and the connection must remain usable afterwards.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_iso_inline_err")
      discard await conn.exec(
        "CREATE TABLE test_iso_inline_err (id serial PRIMARY KEY, v int NOT NULL)"
      )

      let p = newPipeline(conn)
      p.addExec(
        "INSERT INTO test_iso_inline_err (v) VALUES ($1)", [1.int32.toPgParamInline]
      )
      # NULL inline param violates NOT NULL — isolated error.
      p.addExec(
        "INSERT INTO test_iso_inline_err (v) VALUES ($1)", [none(int32).toPgParamInline]
      )
      p.addExec(
        "INSERT INTO test_iso_inline_err (v) VALUES ($1)", [3.int32.toPgParamInline]
      )
      let ir = await p.executeIsolated()
      doAssert ir.results.len == 3
      doAssert ir.errors[0] == nil
      doAssert ir.errors[1] != nil
      doAssert ir.errors[2] == nil # not aborted

      doAssert conn.state == csReady
      let qr = await conn.query("SELECT v FROM test_iso_inline_err ORDER BY id")
      doAssert qr.rowCount == 2
      doAssert qr.rows[0].getStr(0) == "1"
      doAssert qr.rows[1].getStr(0) == "3"

      discard await conn.exec("DROP TABLE test_iso_inline_err")
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

  test "pipeline: reset clears queued ops and allows reuse":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.addQuery("SELECT $1::int4", @[toPgParam(1'i32)])
      p.addQuery("SELECT $1::int4", @[toPgParam(2'i32)])
      let r1 = await p.execute()
      doAssert r1.len == 2

      # Without reset, queued ops would be executed again.
      p.reset()
      let r0 = await p.execute()
      doAssert r0.len == 0

      # Reused instance is healthy for a brand-new batch.
      p.addQuery("SELECT $1::text", @[toPgParam("reused")])
      let r2 = await p.execute()
      doAssert r2.len == 1
      doAssert r2[0].queryResult.rows[0].getStr(0) == "reused"

      await conn.close()

    waitFor t()

  test "pipeline: reset on empty pipeline is a no-op":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn)
      p.reset()
      p.reset()
      let results = await p.execute()
      doAssert results.len == 0

      await conn.close()

    waitFor t()

  test "pipeline: autoReset clears state after execute":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn, autoReset = true)
      p.addQuery("SELECT $1::int4", @[toPgParam(10'i32)])
      p.addQuery("SELECT $1::int4", @[toPgParam(20'i32)])
      let r1 = await p.execute()
      doAssert r1.len == 2

      # After the first execute(), autoReset has cleared queued ops, so
      # calling execute() again produces an empty result without replaying.
      let r0 = await p.execute()
      doAssert r0.len == 0

      # Same instance is safe to reuse for a new batch.
      p.addQuery("SELECT $1::text", @[toPgParam("auto")])
      let r2 = await p.execute()
      doAssert r2.len == 1
      doAssert r2[0].queryResult.rows[0].getStr(0) == "auto"

      await conn.close()

    waitFor t()

  test "pipeline: autoReset clears state after executeIsolated (incl. on error)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn, autoReset = true)
      p.addQuery("SELECT 1::int4")
      p.addQuery("SELECT * FROM __definitely_missing_table__")
      p.addQuery("SELECT 2::int4")
      let ir = await p.executeIsolated()
      doAssert ir.results.len == 3
      doAssert ir.errors.len == 3
      doAssert ir.errors[0] == nil
      doAssert ir.errors[1] != nil
      doAssert ir.errors[2] == nil

      # After executeIsolated, autoReset should have cleared queued ops.
      let ir0 = await p.executeIsolated()
      doAssert ir0.results.len == 0
      doAssert ir0.errors.len == 0

      await conn.close()

    waitFor t()

  test "pipeline: autoReset clears state when execute raises":
    # execute() uses a single SYNC, so a failing op aborts the batch and the
    # await re-raises. Confirms the finally-path reset runs on raise.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let p = newPipeline(conn, autoReset = true)
      p.addExec("SELECT 1")
      p.addExec("INVALID SQL THAT WILL FAIL")
      p.addExec("SELECT 2")
      var gotError = false
      try:
        discard await p.execute()
      except PgError:
        gotError = true
      doAssert gotError

      # Connection should still be usable after the error.
      doAssert conn.state == csReady

      # autoReset must have cleared queued ops even though execute() raised.
      let r0 = await p.execute()
      doAssert r0.len == 0

      # Reused instance is healthy for a brand-new batch.
      p.addQuery("SELECT $1::int4", @[toPgParam(42'i32)])
      let r1 = await p.execute()
      doAssert r1.len == 1
      doAssert r1[0].queryResult.rows[0].getStr(0) == "42"

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
        doAssert results[0].commandResult == "INSERT 0 1"
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

  test "pipeline: executeIsolated basic":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_pipe_iso")
      discard
        await conn.exec("CREATE TABLE test_pipe_iso (id serial PRIMARY KEY, val text)")

      let p = newPipeline(conn)
      p.addExec("INSERT INTO test_pipe_iso (val) VALUES ($1)", @[toPgParam("a")])
      p.addQuery("SELECT 42::int4")
      p.addExec("INSERT INTO test_pipe_iso (val) VALUES ($1)", @[toPgParam("b")])
      let ir = await p.executeIsolated()
      doAssert ir.results.len == 3
      doAssert ir.errors.len == 3
      for i in 0 ..< 3:
        doAssert ir.errors[i] == nil
      doAssert ir.results[0].kind == prkExec
      doAssert ir.results[0].commandResult == "INSERT 0 1"
      doAssert ir.results[1].kind == prkQuery
      doAssert ir.results[1].queryResult.rows[0].getStr(0) == "42"
      doAssert ir.results[2].kind == prkExec

      let qr = await conn.query("SELECT val FROM test_pipe_iso ORDER BY id")
      doAssert qr.rowCount == 2
      doAssert qr.rows[0].getStr(0) == "a"
      doAssert qr.rows[1].getStr(0) == "b"

      discard await conn.exec("DROP TABLE test_pipe_iso")
      await conn.close()

    waitFor t()

  test "pipeline: executeIsolated error does not abort subsequent ops":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_pipe_iso2")
      discard await conn.exec(
        "CREATE TABLE test_pipe_iso2 (id serial PRIMARY KEY, val text NOT NULL)"
      )

      let p = newPipeline(conn)
      p.addExec("INSERT INTO test_pipe_iso2 (val) VALUES ($1)", @[toPgParam("ok")])
      # This will fail: NULL violates NOT NULL constraint
      p.addExec("INSERT INTO test_pipe_iso2 (val) VALUES (NULL)")
      p.addExec("INSERT INTO test_pipe_iso2 (val) VALUES ($1)", @[toPgParam("also_ok")])
      let ir = await p.executeIsolated()
      doAssert ir.results.len == 3
      doAssert ir.errors[0] == nil
      doAssert ir.errors[1] != nil
      doAssert ir.errors[2] == nil # NOT aborted, unlike execute()

      # Connection should still be usable
      doAssert conn.state == csReady
      let qr = await conn.query("SELECT val FROM test_pipe_iso2 ORDER BY id")
      doAssert qr.rowCount == 2
      doAssert qr.rows[0].getStr(0) == "ok"
      doAssert qr.rows[1].getStr(0) == "also_ok"

      discard await conn.exec("DROP TABLE test_pipe_iso2")
      await conn.close()

    waitFor t()

  test "pipeline: executeIsolated empty":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = newPipeline(conn)
      let ir = await p.executeIsolated()
      doAssert ir.results.len == 0
      doAssert ir.errors.len == 0
      await conn.close()

    waitFor t()

  test "pipeline: executeIsolated with cache hit":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Warm the cache
      discard await conn.query("SELECT $1::text", @[toPgParam("warm")])

      let p = newPipeline(conn)
      p.addQuery("SELECT $1::text", @[toPgParam("cached")])
      p.addQuery("SELECT $1::int4", @[toPgParam(99'i32)])
      let ir = await p.executeIsolated()
      doAssert ir.errors[0] == nil
      doAssert ir.errors[1] == nil
      doAssert ir.results[0].queryResult.rows[0].getStr(0) == "cached"
      doAssert ir.results[1].queryResult.rows[0].getStr(0) == "99"

      await conn.close()

    waitFor t()

  test "pipeline: executeIsolated timeout":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let p = newPipeline(conn)
      p.addQuery("SELECT pg_sleep(10)")
      var caught = false
      try:
        discard await p.executeIsolated(timeout = milliseconds(100))
      except PgTimeoutError:
        caught = true
      doAssert caught
      doAssert conn.state == csClosed

    waitFor t()

  test "pipeline: executeIsolated cache eviction":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 2

      # Warm cache with 2 entries
      discard await conn.query("SELECT 1")
      discard await conn.query("SELECT 2")
      doAssert conn.stmtCache.len == 2

      # Pipeline 2 new queries via executeIsolated => evicts both old entries
      let p = newPipeline(conn)
      p.addQuery("SELECT 100::int4")
      p.addQuery("SELECT 200::int4")
      let ir = await p.executeIsolated()
      doAssert ir.errors[0] == nil
      doAssert ir.errors[1] == nil
      doAssert ir.results[0].queryResult.rows[0].getStr(0) == "100"
      doAssert ir.results[1].queryResult.rows[0].getStr(0) == "200"

      doAssert conn.stmtCache.len == 2
      doAssert conn.stmtCache.hasKey("SELECT 100::int4")
      doAssert conn.stmtCache.hasKey("SELECT 200::int4")

      await conn.close()

    waitFor t()

  test "pipeline: saves paramOids and invalidates on OID mismatch (execute)":
    # Pipeline must capture ParameterDescription on cache-miss and validate
    # cached paramOids on cache-hit — the same protection commit a7b656bd
    # added for query/exec. Without it, re-binding the same SQL with a
    # different parameter type would silently reinterpret bytes under the
    # original parse-time OID.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1 AS v"
      var p1 = newPipeline(conn)
      p1.addQuery(sql, @[toPgParam(123'i64)])
      let r1 = await p1.execute()
      doAssert r1[0].queryResult.rows[0].getInt64(0) == 123
      doAssert conn.stmtCache[sql].paramOids == @[OidInt8]
      let firstName = conn.stmtCache[sql].name

      # Same SQL, different OID (int4 vs int8) → cache eviction + re-parse.
      var p2 = newPipeline(conn)
      p2.addQuery(sql, @[toPgParam(7'i32)])
      let r2 = await p2.execute()
      doAssert r2[0].queryResult.rows[0].getInt(0) == 7
      doAssert conn.stmtCache.len == 1
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidInt4]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "pipeline: invalidates on OID mismatch (executeIsolated)":
    # Same fix, executeIsolated path (per-op SYNC). Each op has its own
    # ReadyForQuery, but the cache-miss paramOids capture and cache-hit OID
    # validation must both still trigger.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1 AS v"
      var p1 = newPipeline(conn)
      p1.addQuery(sql, @[toPgParam(123'i64)])
      let ir1 = await p1.executeIsolated()
      doAssert ir1.errors[0] == nil
      doAssert ir1.results[0].queryResult.rows[0].getInt64(0) == 123
      doAssert conn.stmtCache[sql].paramOids == @[OidInt8]
      let firstName = conn.stmtCache[sql].name

      var p2 = newPipeline(conn)
      p2.addQuery(sql, @[toPgParam(7'i32)])
      let ir2 = await p2.executeIsolated()
      doAssert ir2.errors[0] == nil
      doAssert ir2.results[0].queryResult.rows[0].getInt(0) == 7
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidInt4]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "pipeline: PgParamInline saves paramOids and invalidates on mismatch":
    # Inline param path uses a separate SoA storage for OIDs; pin that the
    # cache-hit check threads through ``p.inlineOids`` and the cache-miss
    # path still stores OIDs even though Parse came from the inline buffer.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1 AS v"
      var p1 = newPipeline(conn)
      p1.addQuery(sql, [toPgParamInline(1'i32)])
      let r1 = await p1.execute()
      doAssert r1[0].queryResult.rows[0].getInt(0) == 1
      doAssert conn.stmtCache[sql].paramOids == @[OidInt4]
      let firstName = conn.stmtCache[sql].name

      var p2 = newPipeline(conn)
      p2.addQuery(sql, [toPgParamInline("hello")])
      let r2 = await p2.execute()
      doAssert r2[0].queryResult.rows[0].getStr(0) == "hello"
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidText]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "pipeline: matching OIDs reuse cached statement":
    # The OID check must not regress the happy path: two pipeline calls with
    # the same SQL and same OIDs should share one server-side statement.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1::int4 AS v"
      var p1 = newPipeline(conn)
      p1.addQuery(sql, @[toPgParam(1'i32)])
      discard await p1.execute()
      let firstName = conn.stmtCache[sql].name

      var p2 = newPipeline(conn)
      p2.addQuery(sql, @[toPgParam(2'i32)])
      let r2 = await p2.execute()
      doAssert r2[0].queryResult.rows[0].getInt(0) == 2
      doAssert conn.stmtCache.len == 1
      doAssert conn.stmtCache[sql].name == firstName

      await conn.close()

    waitFor t()

  test "pipeline: cross-path OID mismatch with prior conn.query":
    # ``query`` populates the cache with paramOids; a follow-up pipeline call
    # that finds the entry as a cache-hit must run the OID check and
    # invalidate when types differ. Pins interop between the two cache
    # populators.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1 AS v"
      let r0 = await conn.query(sql, @[toPgParam(123'i64)])
      doAssert r0.rows[0].getInt64(0) == 123
      doAssert conn.stmtCache[sql].paramOids == @[OidInt8]
      let firstName = conn.stmtCache[sql].name

      var p = newPipeline(conn)
      p.addQuery(sql, @[toPgParam(7'i32)])
      let r1 = await p.execute()
      doAssert r1[0].queryResult.rows[0].getInt(0) == 7
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidInt4]
      doAssert entry.name != firstName

      await conn.close()

    waitFor t()

  test "pipeline: addExec invalidates on OID mismatch":
    # ``addExec`` shares ``buildSendPhase`` with ``addQuery`` but lacks
    # RowDescription on cache-miss. Pin that the paramOids capture from
    # ParameterDescription still happens, and that cache-hit OID validation
    # kicks in for non-query ops.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_pipe_exec_oid")
      discard await conn.exec(
        "CREATE TABLE test_pipe_exec_oid (id serial PRIMARY KEY, v int)"
      )

      let sql = "INSERT INTO test_pipe_exec_oid (v) VALUES ($1)"
      var p1 = newPipeline(conn)
      p1.addExec(sql, @[toPgParam(1'i64)])
      let r1 = await p1.execute()
      doAssert r1[0].commandResult == "INSERT 0 1"
      doAssert conn.stmtCache[sql].paramOids == @[OidInt8]
      let firstName = conn.stmtCache[sql].name

      var p2 = newPipeline(conn)
      p2.addExec(sql, @[toPgParam(2'i32)])
      let r2 = await p2.execute()
      doAssert r2[0].commandResult == "INSERT 0 1"
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidInt4]
      doAssert entry.name != firstName

      let qr = await conn.query("SELECT v FROM test_pipe_exec_oid ORDER BY id")
      doAssert qr.rowCount == 2
      doAssert qr.rows[0].getInt(0) == 1
      doAssert qr.rows[1].getInt(0) == 2

      discard await conn.exec("DROP TABLE test_pipe_exec_oid")
      await conn.close()

    waitFor t()

  test "pipeline: same SQL repeated in one batch Parses once":
    # Three ops with identical SQL and identical OIDs must collapse to one
    # server-side prepared statement. Before in-flight dedup, each op got a
    # fresh stmtName via nextStmtName(); only the last addStmtCache survived,
    # leaving N-1 orphaned prepared statements on the session.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1::int8 AS v"
      var p = newPipeline(conn)
      p.addQuery(sql, @[toPgParam(1'i64)])
      p.addQuery(sql, @[toPgParam(2'i64)])
      p.addQuery(sql, @[toPgParam(3'i64)])
      let rs = await p.execute()
      doAssert rs[0].queryResult.rows[0].getInt64(0) == 1
      doAssert rs[1].queryResult.rows[0].getInt64(0) == 2
      doAssert rs[2].queryResult.rows[0].getInt64(0) == 3

      doAssert conn.stmtCache.len == 1
      let cachedName = conn.stmtCache[sql].name

      # Verify the server only has the one statement we tracked — no orphans.
      let pq = await conn.simpleQuery("SELECT name FROM pg_prepared_statements")
      doAssert pq[0].rowCount == 1
      doAssert pq[0].rows[0].getStr(0) == cachedName

      await conn.close()

    waitFor t()

  test "pipeline: same SQL repeated (executeIsolated) Parses once":
    # per-op SYNC path: each op gets its own ReadyForQuery, but the in-flight
    # entry must persist across those Sync boundaries because prepared
    # statements are not torn down on Sync.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1::int8 AS v"
      var p = newPipeline(conn)
      p.addQuery(sql, @[toPgParam(1'i64)])
      p.addQuery(sql, @[toPgParam(2'i64)])
      p.addQuery(sql, @[toPgParam(3'i64)])
      let ir = await p.executeIsolated()
      doAssert ir.errors[0] == nil
      doAssert ir.errors[1] == nil
      doAssert ir.errors[2] == nil
      doAssert ir.results[0].queryResult.rows[0].getInt64(0) == 1
      doAssert ir.results[1].queryResult.rows[0].getInt64(0) == 2
      doAssert ir.results[2].queryResult.rows[0].getInt64(0) == 3

      doAssert conn.stmtCache.len == 1
      let cachedName = conn.stmtCache[sql].name
      let pq = await conn.simpleQuery("SELECT name FROM pg_prepared_statements")
      doAssert pq[0].rowCount == 1
      doAssert pq[0].rows[0].getStr(0) == cachedName

      await conn.close()

    waitFor t()

  test "pipeline: same SQL with inline params Parses once":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1::int4 AS v"
      var p = newPipeline(conn)
      p.addQuery(sql, [toPgParamInline(10'i32)])
      p.addQuery(sql, [toPgParamInline(20'i32)])
      let rs = await p.execute()
      doAssert rs[0].queryResult.rows[0].getInt(0) == 10
      doAssert rs[1].queryResult.rows[0].getInt(0) == 20

      doAssert conn.stmtCache.len == 1
      let pq = await conn.simpleQuery("SELECT name FROM pg_prepared_statements")
      doAssert pq[0].rowCount == 1
      doAssert pq[0].rows[0].getStr(0) == conn.stmtCache[sql].name

      await conn.close()

    waitFor t()

  test "pipeline: same SQL with mismatched OIDs in one batch":
    # Mid-batch OID change: op 0 Parses with int8, op 1 wants int4. The
    # in-flight entry's OIDs don't match, so the in-flight stmt must be
    # explicitly Closed and a fresh Parse emitted. The cache must end with
    # only the latest (type-correct) stmt; the server must not retain the
    # int8 stmt either.
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1 AS v"
      var p = newPipeline(conn)
      p.addQuery(sql, @[toPgParam(100'i64)])
      p.addQuery(sql, @[toPgParam(7'i32)])
      let rs = await p.execute()
      doAssert rs[0].queryResult.rows[0].getInt64(0) == 100
      doAssert rs[1].queryResult.rows[0].getInt(0) == 7

      doAssert conn.stmtCache.len == 1
      let entry = conn.stmtCache[sql]
      doAssert entry.paramOids == @[OidInt4]

      let pq = await conn.simpleQuery("SELECT name FROM pg_prepared_statements")
      doAssert pq[0].rowCount == 1
      doAssert pq[0].rows[0].getStr(0) == entry.name

      await conn.close()

    waitFor t()

  test "pipeline: same SQL three ops, OID changes after a share":
    # The hardest interleave: op 0 cacheMiss (int8) — op 1 cacheShare (int8)
    # — op 2 OID mismatch (int4). The mid-batch Close must target op 0's
    # stmt, op 0 must be marked superseded (so it isn't added to cache), and
    # op 1's share is still valid (its Bind/Execute completes before the
    # Close hits the wire).
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1 AS v"
      var p = newPipeline(conn)
      p.addQuery(sql, @[toPgParam(1'i64)])
      p.addQuery(sql, @[toPgParam(2'i64)])
      p.addQuery(sql, @[toPgParam(3'i32)])
      let rs = await p.execute()
      doAssert rs[0].queryResult.rows[0].getInt64(0) == 1
      doAssert rs[1].queryResult.rows[0].getInt64(0) == 2
      doAssert rs[2].queryResult.rows[0].getInt(0) == 3

      doAssert conn.stmtCache.len == 1
      doAssert conn.stmtCache[sql].paramOids == @[OidInt4]
      let pq = await conn.simpleQuery("SELECT name FROM pg_prepared_statements")
      doAssert pq[0].rowCount == 1
      doAssert pq[0].rows[0].getStr(0) == conn.stmtCache[sql].name

      await conn.close()

    waitFor t()

  test "pipeline: same SQL repeated under capacity=1 stays leak-free":
    # If N same-SQL ops were each Parsed separately, eviction would close the
    # previously-tracked stmt but the in-pipeline duplicates would still
    # orphan server-side. With dedup, one stmt suffices regardless of cache
    # capacity.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      conn.stmtCacheCapacity = 1

      let sql = "SELECT $1::int4 AS v"
      var p = newPipeline(conn)
      for i in 0 ..< 5:
        p.addQuery(sql, @[toPgParam(int32(i))])
      let rs = await p.execute()
      for i in 0 ..< 5:
        doAssert rs[i].queryResult.rows[0].getInt(0) == i

      doAssert conn.stmtCache.len == 1
      let pq = await conn.simpleQuery("SELECT name FROM pg_prepared_statements")
      doAssert pq[0].rowCount == 1
      doAssert pq[0].rows[0].getStr(0) == conn.stmtCache[sql].name

      await conn.close()

    waitFor t()

  test "pipeline: rfBinary same SQL — cacheShare op decodes binary correctly":
    # Regression for cacheShare's RowData metadata: cacheShare ops skip
    # Describe(Statement) but still emit Describe(Portal), so they get a
    # RowDescription with server-confirmed binary formatCodes. Without
    # mirroring those into RowData.colFormats / colTypeOids, isBinaryCol
    # would report false and binary bytes would be decoded as text, giving
    # garbage (or raising on non-printable bytes).
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1::int8 AS v"
      var p = newPipeline(conn)
      p.addQuery(sql, @[toPgParam(11'i64)], rfBinary)
      p.addQuery(sql, @[toPgParam(22'i64)], rfBinary)
      p.addQuery(sql, @[toPgParam(33'i64)], rfBinary)
      let rs = await p.execute()
      doAssert rs[0].queryResult.rows[0].getInt64(0) == 11
      doAssert rs[1].queryResult.rows[0].getInt64(0) == 22
      doAssert rs[2].queryResult.rows[0].getInt64(0) == 33
      # All three ops must see binary metadata on column 0. Op 0 is cacheMiss
      # (Describe(Statement) path), ops 1/2 are cacheShare (Describe(Portal)
      # path) — both must yield isBinaryCol == true.
      doAssert rs[0].queryResult.rows[0].isBinaryCol(0)
      doAssert rs[1].queryResult.rows[0].isBinaryCol(0)
      doAssert rs[2].queryResult.rows[0].isBinaryCol(0)

      doAssert conn.stmtCache.len == 1
      await conn.close()

    waitFor t()

  test "pipeline: rfBinary same SQL via executeIsolated — cacheShare decodes":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      let sql = "SELECT $1::int8 AS v"
      var p = newPipeline(conn)
      p.addQuery(sql, @[toPgParam(100'i64)], rfBinary)
      p.addQuery(sql, @[toPgParam(200'i64)], rfBinary)
      let ir = await p.executeIsolated()
      doAssert ir.errors[0] == nil
      doAssert ir.errors[1] == nil
      doAssert ir.results[0].queryResult.rows[0].getInt64(0) == 100
      doAssert ir.results[1].queryResult.rows[0].getInt64(0) == 200
      doAssert ir.results[0].queryResult.rows[0].isBinaryCol(0)
      doAssert ir.results[1].queryResult.rows[0].isBinaryCol(0)

      doAssert conn.stmtCache.len == 1
      await conn.close()

    waitFor t()

suite "E2E: Nested BEGIN rejection":
  # Nested BEGIN is a server-side no-op; the inner COMMIT would silently end the
  # outer transaction and confirm its uncommitted work. Every top-level
  # BEGIN/COMMIT scope must reject entry when a transaction is already active.

  test "withTransaction rejects nested BEGIN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      conn.withTransaction:
        try:
          conn.withTransaction:
            discard await conn.exec("SELECT 1")
        except PgStateError:
          raised = true
      doAssert raised
      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "withTransactionRetry rejects nested BEGIN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      conn.withTransaction:
        try:
          conn.withTransactionRetry(RetryOptions(maxAttempts: 2)):
            discard await conn.exec("SELECT 1")
        except PgStateError:
          raised = true
      doAssert raised
      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "withTransactionDeadline rejects nested BEGIN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      conn.withTransaction:
        try:
          conn.withTransactionDeadline(seconds(5)):
            discard await conn.exec("SELECT 1")
        except PgStateError:
          raised = true
      doAssert raised
      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "withTransactionRetryDeadline rejects nested BEGIN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      conn.withTransaction:
        try:
          conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 2), seconds(5)):
            discard await conn.exec("SELECT 1")
        except PgStateError:
          raised = true
      doAssert raised
      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "execInTransaction rejects nested BEGIN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      conn.withTransaction:
        try:
          discard await conn.execInTransaction("SELECT 1")
        except PgStateError:
          raised = true
      doAssert raised
      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "queryInTransaction rejects nested BEGIN":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var raised = false
      conn.withTransaction:
        try:
          discard await conn.queryInTransaction("SELECT 1")
        except PgStateError:
          raised = true
      doAssert raised
      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "outer withTransaction ROLLBACKs on inner rejection, not COMMITs":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_tx_nest_reject")
      discard await conn.exec(
        "CREATE TABLE test_tx_nest_reject (id serial PRIMARY KEY, val text)"
      )

      var raised = false
      try:
        conn.withTransaction:
          discard await conn.exec(
            "INSERT INTO test_tx_nest_reject (val) VALUES ($1)", @[toPgParam("outer")]
          )
          conn.withTransaction:
            discard await conn.exec("SELECT 1")
      except PgStateError:
        raised = true

      doAssert raised
      doAssert conn.txStatus == tsIdle
      let res = await conn.query("SELECT count(*) FROM test_tx_nest_reject")
      doAssert res.rows[0].getInt64(0) == 0,
        "outer INSERT must have been rolled back, not committed by inner COMMIT"

      discard await conn.exec("DROP TABLE test_tx_nest_reject")
      await conn.close()

    waitFor t()
