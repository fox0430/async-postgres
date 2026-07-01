import std/[unittest, options, strutils, math, importutils, net]

import
  ../async_postgres/[async_backend, pg_protocol, pg_types, pg_client, pg_connection]

import e2e_common

privateAccess(PgConnection)

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

      let r = await conn.copyOut("COPY test_copy_out TO STDOUT")
      doAssert r.format == cfText
      doAssert "COPY 2" in r.commandTag
      doAssert r.data.len == 2

      # Each row is a tab-delimited line with trailing newline
      doAssert r.data[0].toString() == "1\tAlice\n"
      doAssert r.data[1].toString() == "2\tBob\n"

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

      let r = await conn.copyOut("COPY test_copy_out_empty TO STDOUT")
      doAssert r.data.len == 0
      doAssert "COPY 0" in r.commandTag
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

      let r = await conn.copyOut("COPY test_copy_out_large TO STDOUT")
      doAssert r.data.len == 10000
      doAssert "COPY 10000" in r.commandTag

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

      let r = await conn.copyOut("COPY test_copy_out_null TO STDOUT")
      doAssert r.data.len == 2
      # NULL is represented as \N in text format
      doAssert r.data[0].toString() == "1\t\\N\n"
      doAssert r.data[1].toString() == "\\N\tBob\n"

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

      let r = await conn.copyOut("COPY test_copy_csv_out TO STDOUT WITH (FORMAT csv)")
      doAssert r.data.len == 2
      doAssert r.data[0].toString() == "1,Alice\n"
      doAssert r.data[1].toString() == "2,\"Bob, Jr.\"\n"

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

suite "E2E: COPY Failure Recovery":
  test "copyIn bad data inside txn: rollback recovers and next query works":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_fail_txn")
      discard await conn.exec("CREATE TABLE test_copy_fail_txn (id int, name text)")

      discard await conn.exec("BEGIN")
      doAssert conn.txStatus == tsInTransaction

      var raised = false
      try:
        # "abc" is not a valid int4 -> server raises invalid_text_representation
        discard await conn.copyIn(
          "COPY test_copy_fail_txn FROM STDIN", @["abc\tAlice\n".toBytes()]
        )
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsInFailedTransaction

      # Any query in a failed txn errors until ROLLBACK
      var stillFailed = false
      try:
        discard await conn.query("SELECT 1")
      except PgError:
        stillFailed = true
      doAssert stillFailed
      doAssert conn.txStatus == tsInFailedTransaction

      discard await conn.exec("ROLLBACK")
      doAssert conn.txStatus == tsIdle

      let res = await conn.query("SELECT 1")
      doAssert res.rows[0].getStr(0) == "1"

      let cnt = await conn.query("SELECT count(*) FROM test_copy_fail_txn")
      doAssert cnt.rows[0].getStr(0) == "0"

      discard await conn.exec("DROP TABLE test_copy_fail_txn")
      await conn.close()

    waitFor t()

  test "copyIn invalid SQL inside txn: rollback recovers":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("BEGIN")
      doAssert conn.txStatus == tsInTransaction

      var raised = false
      try:
        discard await conn.copyIn(
          "COPY nonexistent_table_xyz FROM STDIN", @["1\ttest\n".toBytes()]
        )
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsInFailedTransaction

      discard await conn.exec("ROLLBACK")
      doAssert conn.txStatus == tsIdle

      let res = await conn.query("SELECT 42")
      doAssert res.rows[0].getStr(0) == "42"

      await conn.close()

    waitFor t()

  test "copyInStream callback error inside txn: rollback recovers":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_fail_stream_txn")
      discard
        await conn.exec("CREATE TABLE test_copy_fail_stream_txn (id int, name text)")

      discard await conn.exec("BEGIN")
      doAssert conn.txStatus == tsInTransaction
      discard
        await conn.exec("INSERT INTO test_copy_fail_stream_txn VALUES (1, 'pre-copy')")

      var callCount = 0
      let cb = makeCopyInCallback:
        inc callCount
        if callCount == 1:
          "1\tfirst\n".toBytes()
        else:
          raise newException(CatchableError, "stream aborted")

      var raised = false
      try:
        discard await conn.copyInStream("COPY test_copy_fail_stream_txn FROM STDIN", cb)
      except CatchableError as e:
        raised = true
        doAssert "stream aborted" in e.msg
      doAssert raised
      doAssert conn.state == csReady
      # CopyFail inside txn aborts the transaction
      doAssert conn.txStatus == tsInFailedTransaction

      discard await conn.exec("ROLLBACK")
      doAssert conn.txStatus == tsIdle

      # Table was created + pre-copy row inserted, both rolled back
      let cnt = await conn.query("SELECT count(*) FROM test_copy_fail_stream_txn")
      doAssert cnt.rows[0].getStr(0) == "0"

      discard await conn.exec("DROP TABLE test_copy_fail_stream_txn")
      await conn.close()

    waitFor t()

  test "copyOut invalid SQL inside txn: rollback recovers":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("BEGIN")
      doAssert conn.txStatus == tsInTransaction

      var raised = false
      try:
        discard await conn.copyOut("COPY nonexistent_table_xyz TO STDOUT")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsInFailedTransaction

      discard await conn.exec("ROLLBACK")
      doAssert conn.txStatus == tsIdle

      let res = await conn.query("SELECT 7")
      doAssert res.rows[0].getStr(0) == "7"

      await conn.close()

    waitFor t()

  test "copyOutStream callback error inside txn: rollback recovers":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_out_fail_txn")
      discard await conn.exec("CREATE TABLE test_copy_out_fail_txn (id int)")
      discard await conn.exec(
        "INSERT INTO test_copy_out_fail_txn SELECT g FROM generate_series(1, 200) AS g"
      )

      discard await conn.exec("BEGIN")
      doAssert conn.txStatus == tsInTransaction

      var chunkCount = 0
      let failingCb = makeCopyOutCallback:
        inc chunkCount
        raise newException(CatchableError, "out callback failed")

      var raised = false
      try:
        discard
          await conn.copyOutStream("COPY test_copy_out_fail_txn TO STDOUT", failingCb)
      except CatchableError as e:
        raised = true
        doAssert "out callback failed" in e.msg
      doAssert raised
      doAssert chunkCount >= 1
      doAssert conn.state == csReady
      # COPY OUT has no client->server abort; server completes normally so tx
      # remains in-transaction even though the client callback failed.
      doAssert conn.txStatus == tsInTransaction

      discard await conn.exec("ROLLBACK")
      doAssert conn.txStatus == tsIdle

      let res = await conn.query("SELECT 1")
      doAssert res.rows[0].getStr(0) == "1"

      discard await conn.exec("DROP TABLE test_copy_out_fail_txn")
      await conn.close()

    waitFor t()

  test "cursor works after copyIn failure (portal state clean)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_portal")
      discard await conn.exec("CREATE TABLE test_copy_portal (id int)")
      discard await conn.exec(
        "INSERT INTO test_copy_portal SELECT g FROM generate_series(1, 30) AS g"
      )

      var raised = false
      try:
        discard await conn.copyIn(
          "COPY test_copy_portal FROM STDIN", @["not_an_int\n".toBytes()]
        )
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle

      # Subsequent portal-based cursor op must succeed
      let cursor = await conn.openCursor(
        "SELECT id FROM test_copy_portal ORDER BY id", chunkSize = 10
      )
      var total = 0
      while true:
        let chunk = await cursor.fetchNext()
        if chunk.len == 0:
          break
        total += chunk.len
      doAssert total == 30
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_copy_portal")
      await conn.close()

    waitFor t()

  test "cursor works after copyOut failure (portal state clean)":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_out_portal")
      discard await conn.exec("CREATE TABLE test_copy_out_portal (id int)")
      discard await conn.exec(
        "INSERT INTO test_copy_out_portal SELECT g FROM generate_series(1, 15) AS g"
      )

      var raised = false
      try:
        discard await conn.copyOut("COPY nonexistent_portal_tbl TO STDOUT")
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle

      let cursor = await conn.openCursor(
        "SELECT id FROM test_copy_out_portal ORDER BY id", chunkSize = 5
      )
      var total = 0
      while true:
        let chunk = await cursor.fetchNext()
        if chunk.len == 0:
          break
        total += chunk.len
      doAssert total == 15

      discard await conn.exec("DROP TABLE test_copy_out_portal")
      await conn.close()

    waitFor t()

suite "E2E: COPY IN Early Error Detection":
  # These exercise the multi-batch send path (data exceeds the 256KB COPY batch
  # threshold), where the client streams several batches and polls for an
  # unsolicited server ErrorResponse between them via the RecvWatch primitive.

  test "copyIn multi-batch valid data: no false early abort":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_mb_ok")
      discard await conn.exec("CREATE TABLE test_copy_mb_ok (id int, val text)")

      # ~2.1MB of rows -> spans many 256KB batches.
      let pad = 'x'.repeat(100)
      var data = ""
      for i in 0 ..< 20000:
        data.add($i & "\t" & pad & "\n")
      doAssert data.len > 5 * 262144

      let tag = await conn.copyIn("COPY test_copy_mb_ok FROM STDIN", data)
      doAssert "COPY 20000" in tag
      doAssert conn.state == csReady

      let res = await conn.query("SELECT count(*) FROM test_copy_mb_ok")
      doAssert res.rows[0].getStr(0) == "20000"

      discard await conn.exec("DROP TABLE test_copy_mb_ok")
      await conn.close()

    waitFor t()

  test "copyIn multi-batch early server error surfaces and recovers":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_mb_err")
      discard await conn.exec("CREATE TABLE test_copy_mb_err (id int, val text)")

      # The second row carries a non-integer id: the server aborts the COPY very
      # early, while the client is still streaming the remaining ~2MB.
      let pad = 'x'.repeat(100)
      var data = "0\t" & pad & "\n"
      data.add("not_an_int\t" & pad & "\n")
      for i in 2 ..< 20000:
        data.add($i & "\t" & pad & "\n")
      doAssert data.len > 5 * 262144

      var raised = false
      try:
        discard await conn.copyIn("COPY test_copy_mb_err FROM STDIN", data)
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle

      # Connection is still usable and the failed COPY inserted nothing.
      let res = await conn.query("SELECT count(*) FROM test_copy_mb_err")
      doAssert res.rows[0].getStr(0) == "0"

      discard await conn.exec("DROP TABLE test_copy_mb_err")
      await conn.close()

    waitFor t()

  test "copyInStream multi-batch early server error surfaces and recovers":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyin_mb_err")
      discard await conn.exec("CREATE TABLE test_copyin_mb_err (id int, val text)")

      let pad = 'x'.repeat(100)
      var idx = 0
      let cb = makeCopyInCallback:
        if idx < 20000:
          let row =
            if idx == 1:
              ("not_an_int\t" & pad & "\n").toBytes()
            else:
              ($idx & "\t" & pad & "\n").toBytes()
          inc idx
          row
        else:
          newSeq[byte]()

      var raised = false
      try:
        discard await conn.copyInStream("COPY test_copyin_mb_err FROM STDIN", cb)
      except PgError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle

      let res = await conn.query("SELECT count(*) FROM test_copyin_mb_err")
      doAssert res.rows[0].getStr(0) == "0"

      discard await conn.exec("DROP TABLE test_copyin_mb_err")
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
