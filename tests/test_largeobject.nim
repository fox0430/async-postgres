import std/[unittest, importutils]

import ../async_postgres/[async_backend, pg_client, pg_largeobject]
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

proc toBytes(s: string): seq[byte] =
  @(s.toOpenArrayByte(0, s.high))

proc toString(b: seq[byte]): string =
  result = newString(b.len)
  if b.len > 0:
    copyMem(addr result[0], addr b[0], b.len)

suite "Large Object: create and unlink":
  test "loCreate and loUnlink":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        doAssert oid != 0.Oid
        await conn.loUnlink(oid)

    waitFor t()

  test "loCreate with requested OID >= 2^31 does not raise RangeDefect":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        # OID counter wraparound on long-lived clusters can produce OIDs whose
        # high bit is set. ``int32(oid)`` would raise an uncatchable RangeDefect
        # here; the implementation must preserve the bit pattern instead.
        let requestedOid = 0x80000000'u32
        let oid = await conn.loCreate(requestedOid)
        doAssert oid == requestedOid

        let lo = await conn.loOpen(oid, INV_READWRITE)
        let testData = toBytes("high-bit OID round-trip")
        let written = await lo.loWrite(testData)
        doAssert written == int32(testData.len)

        discard await lo.loSeek(0, SEEK_SET)
        let readBack = await lo.loRead(int32(testData.len))
        doAssert readBack == testData

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

suite "Large Object: basic read/write":
  test "write and read round-trip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)
        let testData = toBytes("Hello, Large Object!")
        let written = await lo.loWrite(testData)
        doAssert written == int32(testData.len)

        # Seek back to start and read
        discard await lo.loSeek(0, SEEK_SET)
        let readBack = await lo.loRead(int32(testData.len))
        doAssert readBack == testData

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  test "write and read binary data with nulls":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        # Binary data including null bytes
        let testData: seq[byte] = @[0'u8, 1, 2, 255, 0, 128, 64, 0, 32]
        let written = await lo.loWrite(testData)
        doAssert written == int32(testData.len)

        discard await lo.loSeek(0, SEEK_SET)
        let readBack = await lo.loRead(int32(testData.len))
        doAssert readBack == testData

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

suite "Large Object: seek and tell":
  test "loSeek and loTell":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        let testData = toBytes("ABCDEFGHIJ")
        discard await lo.loWrite(testData)

        # Tell should be at end after write
        let posAfterWrite = await lo.loTell()
        doAssert posAfterWrite == int64(testData.len)

        # Seek to start
        let newPos = await lo.loSeek(0, SEEK_SET)
        doAssert newPos == 0'i64

        # Seek from current
        discard await lo.loSeek(5, SEEK_CUR)
        let midPos = await lo.loTell()
        doAssert midPos == 5'i64

        # Read from middle
        let partial = await lo.loRead(5)
        doAssert toString(partial) == "FGHIJ"

        # Seek from end
        discard await lo.loSeek(-3, SEEK_END)
        let endPos = await lo.loTell()
        doAssert endPos == int64(testData.len - 3)

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  test "loSeek/loTell/loSize handle 64-bit offsets beyond 2GB (sparse)":
    # Exercises the lo_lseek64/lo_tell64 path with an offset that would be
    # truncated (and go negative) if cast to int32. Stays cheap because a
    # sparse write only stores the single page actually touched, not the
    # multi-GB hole preceding it.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        # Beyond 2^32 so a 32-bit truncation (signed or unsigned) is caught.
        const bigOffset = 5_000_000_000'i64

        let seeked = await lo.loSeek(bigOffset, SEEK_SET)
        doAssert seeked == bigOffset

        let marker = toBytes("64bit-marker")
        let written = await lo.loWrite(marker)
        doAssert written == int32(marker.len)

        # Position advanced past the write, still in 64-bit territory.
        let pos = await lo.loTell()
        doAssert pos == bigOffset + int64(marker.len)

        # Size reflects the sparse extent (offset + bytes written).
        let size = await lo.loSize()
        doAssert size == bigOffset + int64(marker.len)
        # loSize must restore the position it found.
        doAssert (await lo.loTell()) == bigOffset + int64(marker.len)

        # SEEK_END from a >2GB end position must report the 64-bit position.
        let fromEnd = await lo.loSeek(-int64(marker.len), SEEK_END)
        doAssert fromEnd == bigOffset

        # The marker round-trips when read back at the sparse offset.
        discard await lo.loSeek(bigOffset, SEEK_SET)
        let readBack = await lo.loRead(int32(marker.len))
        doAssert readBack == marker

        # The leading hole reads back as zeros.
        discard await lo.loSeek(0, SEEK_SET)
        let hole = await lo.loRead(16)
        doAssert hole == newSeq[byte](16)

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

suite "Large Object: truncate":
  test "loTruncate":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        discard await lo.loWrite(toBytes("Hello, World!"))
        await lo.loTruncate(5)

        discard await lo.loSeek(0, SEEK_SET)
        let data = await lo.loReadAll()
        doAssert toString(data) == "Hello"

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

suite "Large Object: convenience API":
  test "loReadAll and loWriteAll":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        # Write a larger payload using loWriteAll with small chunk size
        var bigData = newSeq[byte](1000)
        for i in 0 ..< bigData.len:
          bigData[i] = byte(i mod 256)

        await lo.loWriteAll(bigData, chunkSize = 100)

        discard await lo.loSeek(0, SEEK_SET)
        let readBack = await lo.loReadAll(chunkSize = 150)
        doAssert readBack == bigData

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  test "loSize":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        let testData = toBytes("Size test data")
        discard await lo.loWrite(testData)

        let size = await lo.loSize()
        doAssert size == int64(testData.len)

        # Position should be restored after loSize
        let pos = await lo.loTell()
        doAssert pos == int64(testData.len)

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  test "loSizeDeadline returns size within single wall-clock budget":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        let testData = toBytes("Deadline size test")
        discard await lo.loWrite(testData)

        # 3 internal lo_tell/lo_seek must all fit within seconds(5) total.
        let size = await lo.loSizeDeadline(seconds(5))
        doAssert size == int64(testData.len)

        let pos = await lo.loTell()
        doAssert pos == int64(testData.len)

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  test "loReadAllDeadline and loWriteAllDeadline round-trip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        var bigData = newSeq[byte](1000)
        for i in 0 ..< bigData.len:
          bigData[i] = byte(i mod 256)

        await lo.loWriteAllDeadline(bigData, seconds(5), chunkSize = 100)

        discard await lo.loSeek(0, SEEK_SET)
        let readBack = await lo.loReadAllDeadline(seconds(5), chunkSize = 150)
        doAssert readBack == bigData

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  # asyncdispatch only: the deadline fires reliably because asyncdispatch's
  # `withTimeout` honors ms-level per-call timeouts even when the inner future
  # completes synchronously. On chronos, fast successive `loRead` round-trips
  # complete synchronously inside `wait(fut, dur)` (asyncfutures.nim
  # wait/waitImpl), starving the timer; sub-deadline cancellation is therefore
  # best-effort and not exercised here.
  #
  # Note: chronos coverage of the deadline-fire path is provided by the
  # `withTransactionDeadline raises PgTimeoutError when body exceeds deadline`
  # test in `test_e2e.nim`, which uses `SELECT pg_sleep(2)` to force the
  # server-side response off the synchronous-completion path. The Large Object
  # protocol exposes no equivalent server-side blocking primitive, so the
  # cumulative-deadline behavior of `lo*Deadline` cannot be force-armed under
  # chronos without an artificial setup. Functional success cases above still
  # run on both backends.
  # `loSizeDeadline`-specific deadline-fire test omitted: loSize is only 3
  # round-trips against local Postgres (each ~0.1ms), so the per-call 1ms
  # floor of `remainingDeadlineDuration` does not reliably exceed the per-call
  # wait — the loop returns `size` before any timer arms. The deadline-fire
  # path is the same `simpleExec(timeout=1ms)` -> `invalidateOnTimeout` chain
  # exercised by `loReadAllDeadline raises PgTimeoutError when deadline expires
  # mid-read` below (1MB / 1KB chunkSize ≈ 1000 round-trips makes a fire
  # certain within 1ms). The SEEK_END-residue caveat in `loSizeDeadline`'s
  # docstring is therefore documentation-only.

  when not hasChronos:
    test "loReadAllDeadline raises PgTimeoutError when deadline expires mid-read":
      proc t() {.async.} =
        # Setup on a dedicated connection: the deadline test below invalidates
        # its own connection, so create/write must happen elsewhere.
        let setupConn = await connect(plainConfig())
        var oid: Oid
        setupConn.withTransaction:
          oid = await setupConn.loCreate()
          let lo = await setupConn.loOpen(oid, INV_READWRITE)
          var bigData = newSeq[byte](1_000_000)
          for i in 0 ..< bigData.len:
            bigData[i] = byte(i mod 256)
          discard await lo.loWrite(bigData)
          await lo.loClose()
        await setupConn.close()

        # Exercise on a separate connection. Avoid `withTransaction`: when the
        # connection is invalidated on timeout, the macro's COMMIT/ROLLBACK
        # cleanup would raise PgConnectionError on csClosed. Issue BEGIN by
        # hand and let the server abandon the tx when the connection is dropped.
        let conn = await connect(plainConfig())
        discard await conn.simpleExec("BEGIN")
        let lo = await conn.loOpen(oid, INV_READWRITE)
        discard await lo.loSeek(0, SEEK_SET)

        var raised = false
        try:
          # 1MB / 1KB chunkSize ≈ 1000 server round-trips. On asyncdispatch the
          # first per-call wait past the 1ms deadline (which uses a 1ms floor)
          # fires reliably, invalidating the connection.
          discard await lo.loReadAllDeadline(milliseconds(1), chunkSize = 1024)
        except PgTimeoutError:
          raised = true

        doAssert raised
        doAssert conn.state == csClosed
        await conn.close()

        # Cleanup via a fresh connection.
        let cleanupConn = await connect(plainConfig())
        await cleanupConn.loUnlink(oid)
        await cleanupConn.close()

      waitFor t()

  test "loReadStreamDeadline streams chunks within deadline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        var bigData = newSeq[byte](500)
        for i in 0 ..< bigData.len:
          bigData[i] = byte(i mod 256)
        discard await lo.loWrite(bigData)
        discard await lo.loSeek(0, SEEK_SET)

        var collected: seq[byte] = @[]
        var callCount = 0
        let cb = makeLoReadCallback:
          collected.add(data)
          callCount.inc

        await lo.loReadStreamDeadline(cb, seconds(5), chunkSize = 100)
        doAssert collected == bigData
        doAssert callCount == 5

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  test "loWriteStreamDeadline writes all callback chunks within deadline":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        let chunks = @[toBytes("alpha"), toBytes("beta"), toBytes("gamma")]
        var idx = 0
        let cb = makeLoWriteCallback:
          if idx < chunks.len:
            let c = chunks[idx]
            idx.inc
            c
          else:
            @[]

        await lo.loWriteStreamDeadline(cb, seconds(5))

        discard await lo.loSeek(0, SEEK_SET)
        let all = await lo.loReadAll()
        doAssert toString(all) == "alphabetagamma"

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

suite "Large Object: withLargeObject template":
  test "withLargeObject opens and closes":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        conn.withLargeObject(lo, oid, INV_READWRITE):
          discard await lo.loWrite(toBytes("template test"))
          discard await lo.loSeek(0, SEEK_SET)
          let data = await lo.loReadAll()
          doAssert toString(data) == "template test"
        await conn.loUnlink(oid)

    waitFor t()

  test "withLargeObject preserves the original error when the tx is aborted":
    # Regression: when `body` poisons the transaction, the cleanup `loClose`
    # itself raises "current transaction is aborted". That cleanup failure
    # must not mask the real error that `body` raised.
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()

      # Create the object in its own committed transaction so it survives the
      # rollback triggered by the aborted transaction below.
      var oid: Oid
      conn.withTransaction:
        oid = await conn.loCreate()

      var caught = ""
      try:
        {.push warning[UnreachableCode]: off.} # body always raises
        conn.withTransaction:
          conn.withLargeObject(lo, oid, INV_READWRITE):
            # Poison the transaction: every later statement (including the
            # cleanup `loClose`) now fails with "current transaction is aborted".
            try:
              discard await conn.queryValue("SELECT 1 / 0")
            except CatchableError:
              discard
            raise newException(ValueError, "sentinel body error")
        {.pop.}
      except ValueError as e:
        caught = e.msg
      except CatchableError as e:
        caught = "masked by cleanup: " & e.msg

      doAssert caught == "sentinel body error",
        "withLargeObject did not preserve the original error: " & caught

      conn.withTransaction:
        await conn.loUnlink(oid)

    waitFor t()

suite "Large Object: streaming API":
  test "loReadStream":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        var bigData = newSeq[byte](500)
        for i in 0 ..< bigData.len:
          bigData[i] = byte(i mod 256)
        discard await lo.loWrite(bigData)

        discard await lo.loSeek(0, SEEK_SET)

        var collected: seq[byte] = @[]
        var callCount = 0
        let cb = makeLoReadCallback:
          collected.add(data)
          callCount.inc

        await lo.loReadStream(cb, chunkSize = 100)
        doAssert collected == bigData
        doAssert callCount == 5

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

  test "loWriteStream":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let oid = await conn.loCreate()
        let lo = await conn.loOpen(oid, INV_READWRITE)

        let chunks = @[toBytes("chunk1"), toBytes("chunk2"), toBytes("chunk3")]
        var idx = 0
        let cb = makeLoWriteCallback:
          if idx < chunks.len:
            let c = chunks[idx]
            idx.inc
            c
          else:
            @[]

        await lo.loWriteStream(cb)

        discard await lo.loSeek(0, SEEK_SET)
        let all = await lo.loReadAll()
        doAssert toString(all) == "chunk1chunk2chunk3"

        await lo.loClose()
        await conn.loUnlink(oid)

    waitFor t()

suite "Large Object: transaction requirement":
  test "fd invalid outside transaction due to autocommit":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      let oid = await conn.loCreate()
      # lo_open succeeds in autocommit (implicit single-statement tx),
      # but the fd is invalid for subsequent operations.
      let lo = await conn.loOpen(oid, INV_READ)
      var raised = false
      try:
        discard await lo.loRead(10)
      except CatchableError:
        raised = true
      doAssert raised, "loRead on fd from autocommit should raise"
      # Clean up inside a transaction
      conn.withTransaction:
        await conn.loUnlink(oid)

    waitFor t()
