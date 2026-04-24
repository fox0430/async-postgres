## Large Object example.
##
## Demonstrates storing and retrieving binary data using PostgreSQL's
## Large Object API. All operations must be within a transaction.
##
## Usage:
##   nim c -r examples/large_object.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  var oid: Oid

  # Create and write a Large Object (must be inside a transaction)
  conn.withTransaction:
    oid = await conn.loCreate()
    echo "Created Large Object: oid=", oid

    conn.withLargeObject(lo, oid, INV_READWRITE):
      let data = "Hello, Large Object!".toBytes()
      let written = await lo.loWrite(data)
      echo "Wrote ", written, " bytes"

  # Read it back
  conn.withTransaction:
    conn.withLargeObject(lo, oid, INV_READ):
      let content = await lo.loReadAll()
      echo "Read back: ", content.toString()

      # Check size
      let size = await lo.loSize()
      echo "Size: ", size, " bytes"

  # Streaming read, then clean up in the same transaction
  conn.withTransaction:
    conn.withLargeObject(lo, oid, INV_READ):
      echo "\nStreaming read:"
      let cb = makeLoReadCallback:
        echo "  chunk (", data.len, " bytes): ", data.toString()
      await lo.loReadStream(cb, chunkSize = 10)

    await conn.loUnlink(oid)
    echo "\nDeleted Large Object: oid=", oid

waitFor main()
