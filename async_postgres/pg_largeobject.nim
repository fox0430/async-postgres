## PostgreSQL Large Object API
##
## Provides an async interface to PostgreSQL's Large Object facility for
## storing and streaming binary data larger than what fits comfortably in
## a ``bytea`` column.
##
## All Large Object operations **must** be performed inside a transaction
## (use ``withTransaction``).
##
## Example
## =======
##
## .. code-block:: nim
##   conn.withTransaction:
##     let oid = await conn.loCreate()
##     conn.withLargeObject(lo, oid, INV_READWRITE):
##       let written = await lo.loWrite(data)
##       await lo.loSeek(0, SEEK_SET)
##       let readBack = await lo.loReadAll()

import std/[strutils, options]

import async_backend, pg_types, pg_protocol, pg_connection, pg_client

const
  INV_READ* = 0x00040000'i32
  INV_WRITE* = 0x00020000'i32
  INV_READWRITE* = INV_READ or INV_WRITE
  SEEK_SET* = 0'i32
  SEEK_CUR* = 1'i32
  SEEK_END* = 2'i32
  loDefaultChunkSize* = 262144 ## 256KB

type
  Oid* = uint32

  LargeObject* = object
    conn*: PgConnection
    fd*: int32
    oid*: Oid

# Callback types for streaming, matching CopyOutCallback/CopyInCallback pattern
when hasChronos:
  type LoReadCallback* =
    proc(data: seq[byte]): Future[void] {.async: (raises: [CatchableError]), gcsafe.}

  type LoWriteCallback* =
    proc(): Future[seq[byte]] {.async: (raises: [CatchableError]), gcsafe.}

else:
  type LoReadCallback* = proc(data: seq[byte]): Future[void] {.gcsafe.}
  type LoWriteCallback* = proc(): Future[seq[byte]] {.gcsafe.}

template makeLoReadCallback*(body: untyped): LoReadCallback =
  ## Create a ``LoReadCallback`` that works with both asyncdispatch and chronos.
  ## Inside ``body``, the current chunk is available as ``data: seq[byte]``.
  ##
  ## .. code-block:: nim
  ##   var chunks: seq[seq[byte]]
  ##   let cb = makeLoReadCallback:
  ##     chunks.add(data)
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

template makeLoWriteCallback*(body: untyped): LoWriteCallback =
  ## Create a ``LoWriteCallback`` that works with both asyncdispatch and chronos.
  ## ``body`` must evaluate to ``seq[byte]``. Return an empty seq to signal completion.
  ##
  ## With asyncdispatch, anonymous async procs cannot return non-void types,
  ## so this template wraps the body in manual ``Future`` construction.
  ##
  ## .. code-block:: nim
  ##   var idx = 0
  ##   let chunks = @[data1, data2]
  ##   let cb = makeLoWriteCallback:
  ##     if idx < chunks.len:
  ##       let chunk = chunks[idx]
  ##       inc idx
  ##       chunk
  ##     else:
  ##       newSeq[byte]()
  block:
    when hasChronos:
      let r: LoWriteCallback = proc(): Future[seq[byte]] {.
          async: (raises: [CatchableError])
      .} =
        body
      r
    else:
      let r: LoWriteCallback = proc(): Future[seq[byte]] {.gcsafe.} =
        let fut = newFuture[seq[byte]]("makeLoWriteCallback")
        try:
          let res: seq[byte] = body
          fut.complete(res)
        except CatchableError as e:
          fut.fail(e)
        return fut
      r

# Core API

proc loCreate*(
    conn: PgConnection, requestedOid: Oid = 0, timeout: Duration = ZeroDuration
): Future[Oid] {.async.} =
  ## Create a new Large Object, returning its OID.
  ## Pass ``requestedOid = 0`` to let the server assign an OID.
  let s = await conn.queryValue(
    "SELECT lo_create($1)", @[toPgParam(int32(requestedOid))], timeout = timeout
  )
  return Oid(parseUInt(s))

proc loUnlink*(
    conn: PgConnection, oid: Oid, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Delete a Large Object.
  discard await conn.queryValue(
    "SELECT lo_unlink($1)", @[toPgParam(int32(oid))], timeout = timeout
  )

proc loOpen*(
    conn: PgConnection,
    oid: Oid,
    mode: int32 = INV_READWRITE,
    timeout: Duration = ZeroDuration,
): Future[LargeObject] {.async.} =
  ## Open a Large Object for reading/writing. Returns a ``LargeObject`` handle.
  let s = await conn.queryValue(
    "SELECT lo_open($1, $2)",
    @[toPgParam(int32(oid)), toPgParam(mode)],
    timeout = timeout,
  )
  return LargeObject(conn: conn, fd: int32(parseInt(s)), oid: oid)

proc loClose*(
    lo: LargeObject, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Close an open Large Object handle.
  discard await lo.conn.queryValue(
    "SELECT lo_close($1)", @[toPgParam(lo.fd)], timeout = timeout
  )

proc loRead*(
    lo: LargeObject, length: int32, timeout: Duration = ZeroDuration
): Future[seq[byte]] {.async.} =
  ## Read up to ``length`` bytes from the current position.
  ## Returns the bytes read (may be fewer than ``length`` at EOF).
  let qr = await lo.conn.query(
    "SELECT loread($1, $2)",
    @[toPgParam(lo.fd), toPgParam(length)],
    resultFormat = rfBinary,
    timeout = timeout,
  )
  if qr.rowCount == 0:
    return @[]
  let row = Row(data: qr.data, rowIdx: 0)
  if row.isNull(0):
    return @[]
  return row.getBytes(0)

proc loWrite*(
    lo: LargeObject, data: seq[byte], timeout: Duration = ZeroDuration
): Future[int32] {.async.} =
  ## Write ``data`` at the current position. Returns the number of bytes written.
  let s = await lo.conn.queryValue(
    "SELECT lowrite($1, $2)",
    @[toPgParam(lo.fd), PgParam(oid: OidBytea, format: 1, value: some(data))],
    timeout = timeout,
  )
  return int32(parseInt(s))

proc loSeek*(
    lo: LargeObject,
    offset: int64,
    whence: int32 = SEEK_SET,
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Seek to a position. Returns the new absolute position.
  let s = await lo.conn.queryValue(
    "SELECT lo_lseek64($1, $2, $3)",
    @[toPgParam(lo.fd), toPgParam(offset), toPgParam(whence)],
    timeout = timeout,
  )
  return parseBiggestInt(s)

proc loTell*(
    lo: LargeObject, timeout: Duration = ZeroDuration
): Future[int64] {.async.} =
  ## Return the current read/write position.
  let s = await lo.conn.queryValue(
    "SELECT lo_tell64($1)", @[toPgParam(lo.fd)], timeout = timeout
  )
  return parseBiggestInt(s)

proc loTruncate*(
    lo: LargeObject, length: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Truncate the Large Object to ``length`` bytes.
  discard await lo.conn.queryValue(
    "SELECT lo_truncate64($1, $2)",
    @[toPgParam(lo.fd), toPgParam(length)],
    timeout = timeout,
  )

proc loImport*(
    conn: PgConnection, filename: string, timeout: Duration = ZeroDuration
): Future[Oid] {.async.} =
  ## Import a server-side file into a new Large Object, returning its OID.
  let s = await conn.queryValue(
    "SELECT lo_import($1)", @[toPgParam(filename)], timeout = timeout
  )
  return Oid(parseUInt(s))

proc loExport*(
    conn: PgConnection, oid: Oid, filename: string, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Export a Large Object to a server-side file.
  discard await conn.queryValue(
    "SELECT lo_export($1, $2)",
    @[toPgParam(int32(oid)), toPgParam(filename)],
    timeout = timeout,
  )

# Convenience API

proc loReadAll*(
    lo: LargeObject,
    chunkSize: int32 = loDefaultChunkSize,
    timeout: Duration = ZeroDuration,
): Future[seq[byte]] {.async.} =
  ## Read the entire Large Object from the current position to EOF.
  result = @[]
  while true:
    let chunk = await lo.loRead(chunkSize, timeout)
    if chunk.len == 0:
      break
    result.add(chunk)

proc loWriteAll*(
    lo: LargeObject,
    data: seq[byte],
    chunkSize: int = loDefaultChunkSize,
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Write all of ``data``, splitting into chunks.
  var offset = 0
  while offset < data.len:
    let endIdx = min(offset + chunkSize, data.len)
    let chunk = data[offset ..< endIdx]
    let written = await lo.loWrite(chunk, timeout)
    if written != int32(chunk.len):
      raise newException(
        CatchableError,
        "loWriteAll: partial write (" & $written & "/" & $chunk.len & " bytes)",
      )
    offset = endIdx

proc loSize*(
    lo: LargeObject, timeout: Duration = ZeroDuration
): Future[int64] {.async.} =
  ## Return the total size of the Large Object in bytes.
  let savedPos = await lo.loTell(timeout)
  discard await lo.loSeek(0, SEEK_END, timeout)
  result = await lo.loTell(timeout)
  discard await lo.loSeek(savedPos, SEEK_SET, timeout)

# Template

template withLargeObject*(
    conn: PgConnection, lo: untyped, oidVal: Oid, mode: int32, body: untyped
) =
  ## Open a Large Object, execute ``body``, then close it.
  ## Must be used inside ``withTransaction``.
  let lo = await conn.loOpen(oidVal, mode)
  try:
    body
  finally:
    await lo.loClose()

# Streaming API

proc loReadStream*(
    lo: LargeObject,
    callback: LoReadCallback,
    chunkSize: int32 = loDefaultChunkSize,
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Read the Large Object in chunks, calling ``callback`` for each chunk.
  while true:
    let chunk = await lo.loRead(chunkSize, timeout)
    if chunk.len == 0:
      break
    await callback(chunk)

proc loWriteStream*(
    lo: LargeObject,
    callback: LoWriteCallback,
    chunkSize: int = loDefaultChunkSize,
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Write to the Large Object by repeatedly calling ``callback`` until it
  ## returns an empty ``seq[byte]``.
  while true:
    let data = await callback()
    if data.len == 0:
      break
    await lo.loWriteAll(data, chunkSize, timeout)
