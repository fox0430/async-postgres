import std/[unittest, options, strutils, math, importutils, net]

import
  ../async_postgres/
    [async_backend, pg_protocol, pg_types, pg_client, pg_pool, pg_connection]

import e2e_common

privateAccess(PgConnection)

suite "E2E: Cursor/Streaming":
  test "cursor fetches all rows in chunks":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor")
      discard await conn.exec("CREATE TABLE test_cursor (id int)")
      for i in 1 .. 100:
        discard await conn.exec(
          "INSERT INTO test_cursor (id) VALUES ($1)", @[toPgParam(i.int32)]
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

  test "mid-stream close returns conn to ready":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_close")
      discard await conn.exec("CREATE TABLE test_cursor_close (id int)")
      for i in 1 .. 50:
        discard await conn.exec(
          "INSERT INTO test_cursor_close (id) VALUES ($1)", @[toPgParam(i.int32)]
        )

      let cursor = await conn.openCursor(
        "SELECT id FROM test_cursor_close ORDER BY id", chunkSize = 10
      )
      let chunk1 = await cursor.fetchNext()
      doAssert chunk1.len == 10

      await cursor.close()
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
          "INSERT INTO test_cursor_with (id) VALUES ($1)", @[toPgParam(i.int32)]
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

  test "fetchNext after close returns empty even when open buffered rows":
    # Single-chunk results are buffered during openCursor. If close() leaves that
    # buffer intact, a later fetchNext returns stale rows instead of the
    # documented empty seq.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_closed_buf")
      discard await conn.exec("CREATE TABLE test_cursor_closed_buf (id int)")
      discard
        await conn.exec("INSERT INTO test_cursor_closed_buf (id) VALUES (1), (2), (3)")

      let cursor = await conn.openCursor(
        "SELECT id FROM test_cursor_closed_buf ORDER BY id", chunkSize = 100
      )
      await cursor.close()
      let chunk = await cursor.fetchNext()
      doAssert chunk.len == 0
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor_closed_buf")
      await conn.close()

    waitFor t()

  test "withCursor cleans up on exception":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_exc")
      discard await conn.exec("CREATE TABLE test_cursor_exc (id int)")
      for i in 1 .. 20:
        discard await conn.exec(
          "INSERT INTO test_cursor_exc (id) VALUES ($1)", @[toPgParam(i.int32)]
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

  test "withCursor body error survives a failing close":
    # When `body` raises and the automatic close() also fails, the original
    # body exception must propagate — the close failure must not mask it.
    # The backend is terminated mid-body so close()'s Close/Sync hits a dead
    # socket and raises PgConnectionError.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_mask")
      discard await conn.exec("CREATE TABLE test_cursor_mask (id int)")
      for i in 1 .. 10:
        discard await conn.exec(
          "INSERT INTO test_cursor_mask (id) VALUES ($1)", @[toPgParam(i.int32)]
        )

      let pidRes = await conn.query("SELECT pg_backend_pid()")
      let pid = pidRes.rows[0].getInt(0)

      let killer = await connect(plainConfig())

      var raised = false
      try:
        conn.withCursor("SELECT id FROM test_cursor_mask ORDER BY id", 5'i32, cur):
          let chunk = await cur.fetchNext()
          doAssert chunk.len == 5
          # Kill this cursor's own backend so the automatic close() fails.
          discard
            await killer.exec("SELECT pg_terminate_backend($1)", @[toPgParam(pid)])
          await sleepAsync(milliseconds(200))
          raise newException(ValueError, "intentional body error")
      except ValueError as e:
        raised = true
        doAssert e.msg == "intentional body error"

      doAssert raised

      await killer.close()
      try:
        await conn.close()
      except CatchableError:
        discard

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
          "INSERT INTO test_cursor_timeout (id) VALUES ($1)", @[toPgParam(i.int32)]
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

  test "close after fetchNext timeout keeps connection retired":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Same setup as "fetchNext times out": v=1 returns instantly, v>=2 sleeps
      # 2s — the second fetch overruns the 500ms timeout.
      let cursor = await conn.openCursor(
        "SELECT v, CASE WHEN v = 1 THEN pg_sleep(0) ELSE pg_sleep(2) END " &
          "FROM generate_series(1, 5) AS v",
        chunkSize = 1,
        timeout = milliseconds(500),
      )
      let chunk1 = await cursor.fetchNext() # buffered v=1, no I/O
      doAssert chunk1.len == 1

      var raised = false
      try:
        discard await cursor.fetchNext() # v=2 sleeps 2s → timeout
      except PgTimeoutError:
        raised = true
      doAssert raised
      doAssert conn.state == csClosed

      # The timeout left the protocol out of sync and retired the connection.
      # close() (as withCursor's finally calls it) must NOT revive it to csReady
      # on ReadyForQuery — a pool would otherwise hand the corrupted socket to
      # the next borrower.
      await cursor.close()
      doAssert conn.state == csClosed
      # close() must still mark the cursor exhausted so a stray fetchNext
      # short-circuits instead of writing to the corrupted socket.
      doAssert cursor.exhausted

      await conn.close()

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

  test "close on already exhausted cursor":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let cursor = await conn.openCursor("SELECT 1 AS x", chunkSize = 10)
      let chunk = await cursor.fetchNext()
      doAssert chunk.len == 1
      doAssert cursor.exhausted

      # close on exhausted cursor should be safe no-op
      await cursor.close()
      doAssert conn.state == csReady

      let res = await conn.query("SELECT 2 AS y")
      doAssert res.rows[0].getStr(0) == "2"
      await conn.close()

    waitFor t()

  test "binary result format decodes across chunks":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_cursor_bin")
      discard await conn.exec("CREATE TABLE test_cursor_bin (id int4, big int8)")
      for i in 1 .. 25:
        discard await conn.exec(
          "INSERT INTO test_cursor_bin (id, big) VALUES ($1, $2)",
          @[toPgParam(i.int32), toPgParam((i * 1000000000'i64))],
        )

      # chunkSize < row count so both the openCursor-buffered first chunk and
      # subsequent fetchNext (fetchNextImpl) chunks are exercised under binary.
      let cursor = await conn.openCursor(
        "SELECT id, big FROM test_cursor_bin ORDER BY id",
        resultFormat = rfBinary,
        chunkSize = 10,
      )
      doAssert cursor.fields.len == 2

      var allRows: seq[Row]
      while true:
        let chunk = await cursor.fetchNext()
        if chunk.len == 0:
          break
        allRows.add(chunk)

      doAssert allRows.len == 25
      for i in 0 ..< 25:
        doAssert allRows[i].getInt(0) == int32(i + 1)
        doAssert allRows[i].getInt64(1) == (i + 1).int64 * 1000000000'i64
      doAssert cursor.exhausted
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_cursor_bin")
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
      let qr = await stmt.execute(@[toPgParam("7")], timeout = seconds(5))
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
        discard await stmt.execute(@[toPgParam("10")], timeout = milliseconds(100))
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

  test "row.clone() retains row beyond callback lifetime":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var saved: seq[Row] = @[]
      discard await conn.queryEach(
        "SELECT * FROM (VALUES ('a', 1), ('b', 2), ('c', 3)) AS t(s, n)",
        callback = proc(row: Row) =
          saved.add(row.clone()),
      )
      doAssert saved.len == 3
      doAssert saved[0].getStr(0) == "a"
      doAssert saved[0].getInt(1) == 1
      doAssert saved[1].getStr(0) == "b"
      doAssert saved[1].getInt(1) == 2
      doAssert saved[2].getStr(0) == "c"
      doAssert saved[2].getInt(1) == 3
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

  test "queryEach with params":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var sum = 0
      let rowCount = await conn.queryEach(
        "SELECT generate_series(1, $1::int4)",
        @[toPgParam("5")],
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
