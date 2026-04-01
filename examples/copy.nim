## COPY protocol example.
##
## Demonstrates bulk data import/export using PostgreSQL's COPY protocol,
## including both buffered and streaming variants.
##
## Usage:
##   nim c -r examples/copy.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  discard await conn.exec(
    """
    CREATE TEMP TABLE items (
      id int NOT NULL,
      name text NOT NULL
    )
  """
  )

  # copyIn: bulk insert with string data
  let tag = await conn.copyIn("COPY items FROM STDIN", "1\tAlice\n2\tBob\n3\tCharlie\n")
  echo "copyIn result: ", tag.commandTag

  # copyOut: bulk export
  let result = await conn.copyOut("COPY items TO STDOUT")
  echo "\ncopyOut rows:"
  for chunk in result.data:
    echo "  ", chunk.toString()

  # copyInStream: streaming bulk insert
  discard await conn.exec("TRUNCATE items")

  var idx = 0
  let rows = @["10\tDave\n".toBytes(), "20\tEve\n".toBytes()]
  let inCb = makeCopyInCallback:
    if idx < rows.len:
      let chunk = rows[idx]
      inc idx
      chunk
    else:
      newSeq[byte]()

  let inInfo = await conn.copyInStream("COPY items FROM STDIN", inCb)
  echo "\ncopyInStream result: ", inInfo.commandTag

  # copyOutStream: streaming bulk export
  var chunks: seq[seq[byte]]
  let outCb = makeCopyOutCallback:
    chunks.add(data)

  let outInfo = await conn.copyOutStream("COPY items TO STDOUT", outCb)
  echo "\ncopyOutStream rows (", outInfo.commandTag, "):"
  for chunk in chunks:
    echo "  ", chunk.toString()

waitFor main()
