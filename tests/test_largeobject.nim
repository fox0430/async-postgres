import std/[unittest, strutils, importutils]

import ../async_postgres/[async_backend, pg_client, pg_types, pg_largeobject]
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
    copyMem(addr result[0], unsafeAddr b[0], b.len)

template makeLoReadCallback(body: untyped): LoReadCallback =
  block:
    when hasChronos:
      let r: LoReadCallback = proc(
          data {.inject.}: seq[byte]
      ) {.async: (raises: [CatchableError]).} =
        body
      r
    else:
      let r: LoReadCallback = proc(data {.inject.}: seq[byte]) {.async.} =
        body
      r

template makeLoWriteCallback(body: untyped): LoWriteCallback =
  block:
    when hasChronos:
      let r: LoWriteCallback = proc(): Future[seq[byte]] {.
          async: (raises: [CatchableError])
      .} =
        body
      r
    else:
      let r: LoWriteCallback = proc(): Future[seq[byte]] {.gcsafe.} =
        let fut = newFuture[seq[byte]]("loWriteCallback")
        try:
          let res: seq[byte] = body
          fut.complete(res)
        except CatchableError as e:
          fut.fail(e)
        return fut
      r

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
