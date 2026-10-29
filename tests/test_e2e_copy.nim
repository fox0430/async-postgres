import std/[unittest, options, strutils, json]
from std/times import DateTime, Month, dateTime, utc, toTime, initDuration, `+`, `==`

import
  ../async_postgres/[async_backend, pg_protocol, pg_types, pg_client, pg_connection]

import e2e_common

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

  test "copyIn with COPY TO STDOUT raises PgQueryError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_wrongdir")
      discard await conn.exec("CREATE TABLE test_copy_wrongdir (id int)")
      discard await conn.exec("INSERT INTO test_copy_wrongdir VALUES (1), (2)")

      var raised = false
      try:
        discard
          await conn.copyIn("COPY test_copy_wrongdir TO STDOUT", @["3\n".toBytes()])
      except PgQueryError as e:
        raised = true
        doAssert "copyOut" in e.msg
      doAssert raised
      doAssert conn.state == csReady

      # Connection stays usable and no data was inserted
      let res = await conn.query("SELECT count(*) FROM test_copy_wrongdir")
      doAssert res.rows[0].getStr(0) == "2"

      discard await conn.exec("DROP TABLE test_copy_wrongdir")
      await conn.close()

    waitFor t()

  test "copyIn with non-COPY statement raises PgQueryError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Row-returning statement (RowDescription)
      var raised = false
      try:
        discard await conn.copyIn("SELECT 1", @["1\n".toBytes()])
      except PgQueryError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady

      # Non-row statement (CommandComplete only)
      raised = false
      try:
        discard await conn.copyIn("SET search_path TO public", @["1\n".toBytes()])
      except PgQueryError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady

      # Connection stays usable
      let res = await conn.query("SELECT 1")
      doAssert res.rows[0].getStr(0) == "1"

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

  test "copyOut with COPY FROM STDIN raises PgQueryError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyout_wrongdir")
      discard await conn.exec("CREATE TABLE test_copyout_wrongdir (id int)")

      var raised = false
      try:
        # Would deadlock without the CopyFail abort; timeout is a regression guard
        discard await conn.copyOut(
          "COPY test_copyout_wrongdir FROM STDIN", timeout = seconds(5)
        )
      except PgQueryError as e:
        raised = true
        doAssert "copyIn" in e.msg
      doAssert raised
      doAssert conn.state == csReady

      # Connection stays usable and no rows were inserted
      let res = await conn.query("SELECT count(*) FROM test_copyout_wrongdir")
      doAssert res.rows[0].getStr(0) == "0"

      discard await conn.exec("DROP TABLE test_copyout_wrongdir")
      await conn.close()

    waitFor t()

  test "copyOut with non-COPY statement raises PgQueryError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Row-returning statement (RowDescription)
      var raised = false
      try:
        discard await conn.copyOut("SELECT 1")
      except PgQueryError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady

      # Non-row statement (CommandComplete only)
      raised = false
      try:
        discard await conn.copyOut("SET search_path TO public")
      except PgQueryError:
        raised = true
      doAssert raised
      doAssert conn.state == csReady

      # Connection stays usable
      let res = await conn.query("SELECT 1")
      doAssert res.rows[0].getStr(0) == "1"

      await conn.close()

    waitFor t()

  test "copyOutStream with COPY FROM STDIN raises without invoking callback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyout_stream_wrongdir")
      discard await conn.exec("CREATE TABLE test_copyout_stream_wrongdir (id int)")

      var callCount = 0
      let cb = makeCopyOutCallback:
        inc callCount
      var raised = false
      try:
        discard await conn.copyOutStream(
          "COPY test_copyout_stream_wrongdir FROM STDIN", cb, timeout = seconds(5)
        )
      except PgQueryError as e:
        raised = true
        doAssert "copyInStream" in e.msg
      doAssert raised
      doAssert callCount == 0
      doAssert conn.state == csReady

      discard await conn.exec("DROP TABLE test_copyout_stream_wrongdir")
      await conn.close()

    waitFor t()

  test "copyOutStream with non-COPY statement raises without invoking callback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var callCount = 0
      let cb = makeCopyOutCallback:
        inc callCount
      var raised = false
      try:
        discard await conn.copyOutStream("SELECT 1", cb)
      except PgQueryError:
        raised = true
      doAssert raised
      doAssert callCount == 0
      doAssert conn.state == csReady

      # Connection stays usable
      let res = await conn.query("SELECT 1")
      doAssert res.rows[0].getStr(0) == "1"

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

  test "copyInStream non-COPY statement raises without pulling callback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var callCount = 0
      let cb = makeCopyInCallback:
        inc callCount
        newSeq[byte]()

      var raised = false
      try:
        discard await conn.copyInStream("SELECT 1", cb)
      except PgQueryError:
        raised = true
      doAssert raised
      doAssert callCount == 0
      doAssert conn.state == csReady

      # Connection stays usable
      let res = await conn.simpleQuery("SELECT 1")
      doAssert res[0].rows[0][0].get().toString() == "1"

      await conn.close()

    waitFor t()

  test "copyInStream with COPY TO STDOUT raises without pulling callback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copyin_stream_wrongdir")
      discard await conn.exec("CREATE TABLE test_copyin_stream_wrongdir (id int)")
      discard await conn.exec("INSERT INTO test_copyin_stream_wrongdir VALUES (1), (2)")

      var callCount = 0
      let cb = makeCopyInCallback:
        inc callCount
        "3\n".toBytes()

      var msg = ""
      try:
        discard
          await conn.copyInStream("COPY test_copyin_stream_wrongdir TO STDOUT", cb)
      except PgQueryError as e:
        msg = e.msg
      doAssert "copyOutStream" in msg, msg
      doAssert callCount == 0
      doAssert conn.state == csReady

      # The streamed-out rows were drained, so the next query reads its own reply.
      let res = await conn.query("SELECT count(*) FROM test_copyin_stream_wrongdir")
      doAssert res.rows[0].getStr(0) == "2"

      discard await conn.exec("DROP TABLE test_copyin_stream_wrongdir")
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

type
  CopyMood = enum
    cmHappy = "happy"
    cmSad = "sad"

  CopyFlag = enum
    cfA = "a"
    cfX = "x"

  CopyEmail = distinct string

  CopyAll = object
    n4: int
    n2: int16
    n8: int64
    numInt: int
    numStr: string
    r: float64
    d: float64
    b: bool
    t: string
    vc: string
    ch: string
    doc: JsonNode
    js: string
    uidStr: string
    uid: PgUuid
    raw: seq[byte]
    tstz: DateTime
    ts: DateTime
    day: DateTime
    mood: CopyMood
    ints: seq[int32]
    nums: seq[int64]
    tags: seq[string]
    email: CopyEmail
    note: Option[string]

  CopyUser = object
    id {.pgSkip.}: int64
    userName: string
    createdAt: DateTime

proc get(row: Row, col: int, T: typedesc[CopyEmail]): CopyEmail =
  CopyEmail(row.getStr(col))

proc `==`(a, b: CopyEmail): bool {.borrow.}

proc copyAllRow(i: int): CopyAll =
  CopyAll(
    n4: i,
    n2: int16(-i),
    n8: int64(i) * 10_000_000_000,
    numInt: i * 3,
    numStr: $i & ".25",
    r: 0.5 + float64(i),
    d: 0.1 * float64(i),
    b: i mod 2 == 0,
    t: "t" & $i,
    vc: "v" & $i,
    ch: "c" & $i & "x",
    doc: %*{"i": i},
    js: "[" & $i & "]",
    uidStr: "00000000-0000-0000-0000-00000000000" & $i,
    uid: PgUuid("10000000-0000-0000-0000-00000000000" & $i),
    raw: @[byte(i), 0, 255],
    tstz: dateTime(2024, mMar, 1, 12, 30, 0, zone = utc()) + initDuration(days = i),
    ts: dateTime(2024, mMar, 1, 8, 0, 0, zone = utc()),
    day: dateTime(2024, mMar, 1 + i, zone = utc()),
    mood: if i mod 2 == 0: cmHappy else: cmSad,
    ints: @[int32(i), int32(-i)],
    nums: @[int64(i) * 1_000_000_000_000, -int64(i)],
    tags: @["x" & $i, "y"],
    email: CopyEmail("u" & $i & "@example.com"),
    note:
      if i == 1:
        some("first")
      else:
        none(string),
  )

suite "E2E: copyInRows":
  test "every supported pair round-trips through to":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_all")
      discard await conn.exec("DROP TYPE IF EXISTS test_copy_mood")
      discard await conn.exec("DROP DOMAIN IF EXISTS test_copy_email")
      discard await conn.exec("CREATE TYPE test_copy_mood AS ENUM ('happy', 'sad')")
      discard await conn.exec("CREATE DOMAIN test_copy_email AS varchar(64)")
      discard await conn.exec(
        """CREATE TABLE test_copy_rows_all (
          n4 int, n2 smallint, n8 bigint, num_int numeric, num_str numeric(10, 2),
          r real, d double precision, b boolean, t text, vc varchar(10), ch char(3),
          doc jsonb, js json, uid_str uuid, uid uuid, raw bytea, tstz timestamptz,
          ts timestamp, day date, mood test_copy_mood, ints int[], nums numeric[],
          tags varchar[], email test_copy_email, note text)"""
      )
      let rows = @[copyAllRow(1), copyAllRow(2)]
      let res = await conn.copyInRows("test_copy_rows_all", rows)
      doAssert res.affectedRows == 2

      # Binary results read numeric, json and uuid as no string or int: cast.
      let back = (
        await conn.query(
          "SELECT n4, n2, n8, num_int::bigint AS num_int, num_str::text AS num_str, " &
            "r, d, b, t, vc, ch, doc, js::text AS js, uid_str::text AS uid_str, uid, " &
            "raw, tstz, ts, day, mood, ints, nums::bigint[] AS nums, tags, email, note " &
            "FROM test_copy_rows_all ORDER BY n4",
          resultFormat = rfBinary,
        )
      ).to(seq[CopyAll])
      doAssert back.len == 2
      for i in 0 .. 1:
        let (a, b) = (rows[i], back[i])
        doAssert a.n4 == b.n4 and a.n2 == b.n2 and a.n8 == b.n8
        doAssert a.numInt == b.numInt
        doAssert b.numStr == a.numStr
        doAssert a.r == b.r and a.d == b.d and a.b == b.b
        doAssert a.t == b.t and a.vc == b.vc and a.ch == b.ch
        doAssert a.doc == b.doc and a.js == b.js
        doAssert a.uidStr == b.uidStr and a.uid == b.uid
        doAssert a.raw == b.raw
        doAssert a.tstz.toTime == b.tstz.toTime
        doAssert a.ts.toTime == b.ts.toTime
        doAssert a.day.toTime == b.day.toTime
        doAssert a.mood == b.mood and a.ints == b.ints and a.nums == b.nums
        doAssert a.tags == b.tags
        doAssert a.email == b.email and a.note == b.note

      discard await conn.exec("DROP TABLE test_copy_rows_all")
      discard await conn.exec("DROP TYPE test_copy_mood")
      discard await conn.exec("DROP DOMAIN test_copy_email")
      await conn.close()

    waitFor t()

  test "pgSkip leaves a serial column to its default":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_users")
      discard await conn.exec(
        "CREATE TABLE test_copy_rows_users " &
          "(id bigserial PRIMARY KEY, user_name text, created_at timestamptz)"
      )
      let now = dateTime(2024, mJan, 1, zone = utc())
      let res = await conn.copyInRows(
        "test_copy_rows_users",
        @[
          CopyUser(id: 99, userName: "a", createdAt: now),
          CopyUser(userName: "b", createdAt: now),
        ],
      )
      doAssert res.affectedRows == 2
      let back = await conn.query(
        "SELECT id, user_name FROM test_copy_rows_users ORDER BY id",
        resultFormat = rfText,
      )
      doAssert back.rows[0].getStr(0) == "1" and back.rows[0].getStr(1) == "a"
      doAssert back.rows[1].getStr(0) == "2" and back.rows[1].getStr(1) == "b"
      discard await conn.exec("DROP TABLE test_copy_rows_users")
      await conn.close()

    waitFor t()

  test "columns pick the fields to write; a tuple writes them by position":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_cols")
      discard await conn.exec(
        "CREATE TABLE test_copy_rows_cols (a int, b text DEFAULT 'dflt', c int)"
      )
      discard await conn.copyInRows(
        "test_copy_rows_cols", @[(a: 1, b: "x", c: 3)], columns = ["c", "a"]
      )
      discard
        await conn.copyInRows("test_copy_rows_cols", @[(4, "y")], columns = ["c", "b"])
      let res = await conn.query(
        "SELECT a, b, c FROM test_copy_rows_cols ORDER BY c", resultFormat = rfText
      )
      doAssert res.rows.len == 2
      doAssert res.rows[0].getStr(0) == "1" and res.rows[0].getStr(1) == "dflt"
      doAssert res.rows[0].getStr(2) == "3"
      doAssert res.rows[1].isNull(0) and res.rows[1].getStr(1) == "y"
      doAssert res.rows[1].getStr(2) == "4"
      discard await conn.exec("DROP TABLE test_copy_rows_cols")
      await conn.close()

    waitFor t()

  test "schema is quoted apart from the table":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP SCHEMA IF EXISTS \"Copy Rows\" CASCADE")
      discard await conn.exec("CREATE SCHEMA \"Copy Rows\"")
      discard await conn.exec("CREATE TABLE \"Copy Rows\".\"Items\" (n int)")
      let res = await conn.copyInRows("Items", @[(n: 1), (n: 2)], schema = "Copy Rows")
      doAssert res.affectedRows == 2
      discard await conn.exec("DROP SCHEMA \"Copy Rows\" CASCADE")
      await conn.close()

    waitFor t()

  test "zero rows still check the target":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_zero")
      discard await conn.exec("CREATE TABLE test_copy_rows_zero (n int)")
      let res = await conn.copyInRows("test_copy_rows_zero", newSeq[tuple[n: int]]())
      doAssert res.commandTag == "COPY 0"
      var code = ""
      try:
        discard await conn.copyInRows("test_copy_rows_missing", newSeq[tuple[n: int]]())
      except PgQueryError as e:
        code = e.sqlState
      doAssert code == "42P01"
      doAssert conn.state == csReady
      discard await conn.exec("DROP TABLE test_copy_rows_zero")
      await conn.close()

    waitFor t()

  test "a field type its column cannot take fails before the COPY":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_plan")
      discard await conn.exec("CREATE TABLE test_copy_rows_plan (n float8)")
      var msg = ""
      try:
        discard await conn.copyInRows("test_copy_rows_plan", @[(n: 1)])
      except PgTypeError as e:
        msg = e.msg
      doAssert "tuple[n: int].n: column \"n\": int cannot be written to a column of type OID 701" in
        msg, msg
      doAssert conn.state == csReady
      doAssert (
        await conn.queryValue(int64, "SELECT count(*) FROM test_copy_rows_plan")
      ) == 0
      discard await conn.exec("DROP TABLE test_copy_rows_plan")
      await conn.close()

    waitFor t()

  test "an enum label to a user type that is not an enum fails before the COPY":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_rng")
      discard await conn.exec("DROP TYPE IF EXISTS test_copy_int_rng")
      # `range_recv` would read the label 'a' as its flags byte: 'empty'.
      discard await conn.exec("CREATE TYPE test_copy_int_rng AS RANGE (subtype = int4)")
      discard await conn.exec("CREATE TABLE test_copy_rows_rng (r test_copy_int_rng)")
      var msg = ""
      try:
        discard await conn.copyInRows("test_copy_rows_rng", @[(r: cfA)])
      except PgTypeError as e:
        msg = e.msg
      doAssert "column \"r\": CopyFlag cannot be written to a column of type OID " in msg and
        "which is not an enum" in msg, msg
      doAssert conn.state == csReady
      doAssert (await conn.queryValue(int64, "SELECT count(*) FROM test_copy_rows_rng")) ==
        0
      discard await conn.exec("DROP TABLE test_copy_rows_rng")
      discard await conn.exec("DROP TYPE test_copy_int_rng")
      await conn.close()

    waitFor t()

  test "a value out of range aborts the whole COPY and keeps the connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_range")
      discard await conn.exec("CREATE TABLE test_copy_rows_range (n smallint)")
      var rows: seq[tuple[n: int]]
      for i in 0 ..< 50_000:
        rows.add((n: i mod 1000))
      rows.add((n: 70_000))
      var msg = ""
      try:
        discard await conn.copyInRows("test_copy_rows_range", rows)
      except PgTypeError as e:
        msg = e.msg
      doAssert "row 50000: tuple[n: int].n: column \"n\": value out of range for smallint" in
        msg, msg
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      doAssert (
        await conn.queryValue(int64, "SELECT count(*) FROM test_copy_rows_range")
      ) == 0
      discard await conn.exec("DROP TABLE test_copy_rows_range")
      await conn.close()

    waitFor t()

  test "many rows span several batches":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_many")
      discard await conn.exec("CREATE TABLE test_copy_rows_many (n bigint, s text)")
      var rows: seq[tuple[n: int64, s: string]]
      for i in 0 ..< 100_000:
        rows.add((n: int64(i), s: "row " & $i))
      let res = await conn.copyInRows("test_copy_rows_many", rows)
      doAssert res.affectedRows == 100_000
      doAssert (await conn.queryValue(int64, "SELECT sum(n) FROM test_copy_rows_many")) ==
        int64(99_999) * 100_000 div 2
      discard await conn.exec("DROP TABLE test_copy_rows_many")
      await conn.close()

    waitFor t()

  test "the server's own errors surface: a generated column, NOT NULL":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_srv")
      discard await conn.exec(
        "CREATE TABLE test_copy_rows_srv " &
          "(n int NOT NULL, twice int GENERATED ALWAYS AS (n * 2) STORED)"
      )
      var code = ""
      try:
        discard await conn.copyInRows("test_copy_rows_srv", @[(n: 1, twice: 2)])
      except PgQueryError as e:
        code = e.sqlState
      doAssert code == "42P10", code
      code = ""
      try:
        discard await conn.copyInRows(
          "test_copy_rows_srv", @[(n: none(int))], columns = ["n"]
        )
      except PgQueryError as e:
        code = e.sqlState
      doAssert code == "23502", code
      doAssert conn.state == csReady
      discard await conn.exec("DROP TABLE test_copy_rows_srv")
      await conn.close()

    waitFor t()

suite "E2E: copyInRowsStream":
  test "batches stream until an empty one":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_stream")
      discard await conn.exec("CREATE TABLE test_copy_rows_stream (n int, s text)")
      var calls = 0
      var n = 0
      let cb = makeCopyInRowsCallback(tuple[n: int, s: string]):
        inc calls
        var batch: seq[tuple[n: int, s: string]]
        # Sizes 0..9999: some batches fill a send on their own, some do not.
        if calls <= 8:
          for _ in 0 ..< (calls * 1777) mod 10_000:
            batch.add((n: n, s: "r" & $n))
            inc n
        batch
      let res = await conn.copyInRowsStream("test_copy_rows_stream", cb)
      doAssert calls == 9, $calls
      doAssert res.affectedRows == int64(n), $res.affectedRows
      doAssert (
        await conn.queryValue(
          int64, "SELECT count(DISTINCT n) FROM test_copy_rows_stream"
        )
      ) == int64(n)
      discard await conn.exec("DROP TABLE test_copy_rows_stream")
      await conn.close()

    waitFor t()

  test "a callback error aborts the COPY and surfaces as is":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_stream_err")
      discard await conn.exec("CREATE TABLE test_copy_rows_stream_err (n int)")
      var calls = 0
      let cb = makeCopyInRowsCallback(tuple[n: int]):
        inc calls
        if calls == 3:
          raise newException(ValueError, "source went away")
        @[(n: calls)]
      var msg = ""
      try:
        discard await conn.copyInRowsStream("test_copy_rows_stream_err", cb)
      except ValueError as e:
        msg = e.msg
      doAssert "source went away" in msg, msg
      doAssert conn.state == csReady
      doAssert (
        await conn.queryValue(int64, "SELECT count(*) FROM test_copy_rows_stream_err")
      ) == 0
      discard await conn.exec("DROP TABLE test_copy_rows_stream_err")
      await conn.close()

    waitFor t()

  test "a bad value counts rows across batches":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_stream_bad")
      discard await conn.exec("CREATE TABLE test_copy_rows_stream_bad (n smallint)")
      var calls = 0
      let cb = makeCopyInRowsCallback(tuple[n: int]):
        inc calls
        case calls
        of 1:
          @[(n: 1), (n: 2), (n: 3)]
        of 2:
          @[(n: 4), (n: 70_000)]
        else:
          newSeq[tuple[n: int]]()
      var msg = ""
      try:
        discard await conn.copyInRowsStream("test_copy_rows_stream_bad", cb)
      except PgTypeError as e:
        msg = e.msg
      doAssert "row 4: tuple[n: int].n: column \"n\": value out of range for smallint" in
        msg, msg
      doAssert conn.state == csReady
      discard await conn.exec("DROP TABLE test_copy_rows_stream_bad")
      await conn.close()

    waitFor t()

  test "a failed target check leaves the callback uncalled":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      var calls = 0
      let cb = makeCopyInRowsCallback(tuple[n: int]):
        inc calls
        newSeq[tuple[n: int]]()
      var code = ""
      try:
        discard await conn.copyInRowsStream("test_copy_rows_stream_missing", cb)
      except PgQueryError as e:
        code = e.sqlState
      doAssert code == "42P01", code
      doAssert calls == 0
      doAssert conn.state == csReady
      doAssert conn.txStatus == tsIdle
      await conn.close()

    waitFor t()

  test "outside a transaction, the Describe's lock holds through the COPY":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let other = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_lock")
      discard await conn.exec("CREATE TABLE test_copy_rows_lock (n int)")
      var held = -1'i64
      var sent = false
      let cb = makeCopyInRowsCallback(tuple[n: int]):
        if sent:
          newSeq[tuple[n: int]]()
        else:
          sent = true
          # The COPY itself takes RowExclusiveLock, not AccessShareLock.
          held = await other.queryValue(
            int64,
            "SELECT count(*) FROM pg_locks WHERE pid = $1 AND " &
              "relation = 'test_copy_rows_lock'::regclass AND " &
              "mode = 'AccessShareLock' AND granted",
            @[conn.pid.toPgParam],
          )
          @[(n: 1)]
      let res = await conn.copyInRowsStream("test_copy_rows_lock", cb)
      doAssert held == 1, $held
      doAssert res.affectedRows == 1
      doAssert conn.txStatus == tsIdle
      doAssert (
        await other.queryValue(int64, "SELECT count(*) FROM test_copy_rows_lock")
      ) == 1
      discard await conn.exec("DROP TABLE test_copy_rows_lock")
      await other.close()
      await conn.close()

    waitFor t()

  test "inside a transaction, the rows commit or roll back with it":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      discard await conn.exec("DROP TABLE IF EXISTS test_copy_rows_tx")
      discard await conn.exec("CREATE TABLE test_copy_rows_tx (n int)")
      discard await conn.exec("BEGIN")
      let res = await conn.copyInRows("test_copy_rows_tx", @[(n: 1), (n: 2)])
      doAssert res.affectedRows == 2
      doAssert conn.txStatus == tsInTransaction
      discard await conn.exec("ROLLBACK")
      doAssert (await conn.queryValue(int64, "SELECT count(*) FROM test_copy_rows_tx")) ==
        0
      discard await conn.exec("DROP TABLE test_copy_rows_tx")
      await conn.close()

    waitFor t()
