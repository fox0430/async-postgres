import std/unittest

import ../async_postgres/pg_protocol

proc buildDataRowBody(values: openArray[string]): seq[byte] =
  ## Helper: build a DataRow message body (without the 'D' header) for `numCols`
  ## columns from string values. Use "" for empty, and "\xFF" sentinel for NULL.
  result.addInt16(int16(values.len))
  for v in values:
    if v == "\xFF":
      result.addInt32(-1) # NULL
    else:
      result.addInt32(int32(v.len))
      for c in v:
        result.add(byte(c))

proc getCell(rd: RowData, rowIdx, col: int): string =
  ## Read a text cell from the flat buffer.
  let idx = (rowIdx * int(rd.numCols) + col) * 2
  let off = int(rd.cellIndex[idx])
  let clen = int(rd.cellIndex[idx + 1])
  if clen <= 0:
    return ""
  result = newString(clen)
  copyMem(addr result[0], unsafeAddr rd.buf[off], clen)

proc isCellNull(rd: RowData, rowIdx, col: int): bool =
  let idx = (rowIdx * int(rd.numCols) + col) * 2
  rd.cellIndex[idx + 1] == -1'i32

suite "resetRowData":
  test "resets buf and cellIndex to empty":
    let rd = newRowData(2)
    let body = buildDataRowBody(["hello", "world"])
    parseDataRowInto(body, rd)
    check rd.buf.len > 0
    check rd.cellIndex.len > 0

    rd.resetRowData(3)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0
    check rd.numCols == 3

  test "preserves seq capacity after reset":
    let rd = newRowData(2)
    for i in 0 ..< 100:
      let body = buildDataRowBody(["row" & $i, "data" & $i])
      parseDataRowInto(body, rd)

    let bufCap = rd.buf.capacity
    let cellCap = rd.cellIndex.capacity
    check bufCap > 0
    check cellCap > 0

    rd.resetRowData(2)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0
    # Capacity is preserved — no reallocation needed on next fill
    check rd.buf.capacity == bufCap
    check rd.cellIndex.capacity == cellCap

  test "updates colFormats and colTypeOids":
    let rd = newRowData(2, @[0'i16, 0'i16], @[23'i32, 25'i32])
    check rd.colFormats == @[0'i16, 0'i16]
    check rd.colTypeOids == @[23'i32, 25'i32]

    rd.resetRowData(3, @[1'i16, 0'i16, 1'i16], @[20'i32, 25'i32, 16'i32])
    check rd.numCols == 3
    check rd.colFormats == @[1'i16, 0'i16, 1'i16]
    check rd.colTypeOids == @[20'i32, 25'i32, 16'i32]

  test "usable after reset — data accumulates correctly":
    let rd = newRowData(2)
    let body1 = buildDataRowBody(["aaa", "bbb"])
    parseDataRowInto(body1, rd)
    check getCell(rd, 0, 0) == "aaa"
    check getCell(rd, 0, 1) == "bbb"

    # Reset and reuse
    rd.resetRowData(2)
    let body2 = buildDataRowBody(["xxx", "yyy"])
    let body3 = buildDataRowBody(["111", "222"])
    parseDataRowInto(body2, rd)
    parseDataRowInto(body3, rd)

    check getCell(rd, 0, 0) == "xxx"
    check getCell(rd, 0, 1) == "yyy"
    check getCell(rd, 1, 0) == "111"
    check getCell(rd, 1, 1) == "222"

  test "reset with different column count":
    let rd = newRowData(2)
    parseDataRowInto(buildDataRowBody(["a", "b"]), rd)
    check rd.cellIndex.len == 4 # 2 cols * 2

    rd.resetRowData(3)
    parseDataRowInto(buildDataRowBody(["x", "y", "z"]), rd)
    check rd.numCols == 3
    check rd.cellIndex.len == 6 # 3 cols * 2
    check getCell(rd, 0, 0) == "x"
    check getCell(rd, 0, 1) == "y"
    check getCell(rd, 0, 2) == "z"

  test "reset handles NULL cells":
    let rd = newRowData(3)
    parseDataRowInto(buildDataRowBody(["val", "\xFF", "end"]), rd)
    check getCell(rd, 0, 0) == "val"
    check isCellNull(rd, 0, 1)
    check getCell(rd, 0, 2) == "end"

    rd.resetRowData(3)
    parseDataRowInto(buildDataRowBody(["\xFF", "mid", "\xFF"]), rd)
    check isCellNull(rd, 0, 0)
    check getCell(rd, 0, 1) == "mid"
    check isCellNull(rd, 0, 2)

  test "multiple reset cycles accumulate rows correctly":
    let rd = newRowData(1)
    for cycle in 0 ..< 5:
      rd.resetRowData(1)
      for row in 0 ..< 10:
        parseDataRowInto(buildDataRowBody(["c" & $cycle & "r" & $row]), rd)
      # Verify last cycle's data
      for row in 0 ..< 10:
        check getCell(rd, row, 0) == "c" & $cycle & "r" & $row

  test "numCols-only overload clears colFormats/colTypeOids preserving capacity":
    let rd = newRowData(2, @[1'i16, 0'i16], @[23'i32, 25'i32])
    # Grow internal format seqs so they have nonzero capacity
    check rd.colFormats.len == 2
    check rd.colTypeOids.len == 2

    rd.resetRowData(2)
    check rd.colFormats.len == 0
    check rd.colTypeOids.len == 0
    check rd.colFormats.capacity >= 2
    check rd.colTypeOids.capacity >= 2

  test "reset immediately after creation (no data parsed)":
    let rd = newRowData(2)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0

    rd.resetRowData(3)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0
    check rd.numCols == 3

    # Still usable
    parseDataRowInto(buildDataRowBody(["a", "b", "c"]), rd)
    check getCell(rd, 0, 0) == "a"
    check getCell(rd, 0, 2) == "c"

  test "fewer rows after reset do not expose stale data":
    let rd = newRowData(1)
    for i in 0 ..< 10:
      parseDataRowInto(buildDataRowBody(["old" & $i]), rd)
    check rd.cellIndex.len == 20 # 10 rows * 1 col * 2

    rd.resetRowData(1)
    parseDataRowInto(buildDataRowBody(["only"]), rd)
    check rd.cellIndex.len == 2 # 1 row * 1 col * 2
    check rd.buf.len == 4 # "only"
    check getCell(rd, 0, 0) == "only"

  test "empty string vs NULL distinction preserved after reset":
    let rd = newRowData(2)
    parseDataRowInto(buildDataRowBody(["x", "y"]), rd)

    rd.resetRowData(2)
    # col 0: empty string (len=0), col 1: NULL (len=-1)
    var body: seq[byte]
    body.addInt16(2)
    body.addInt32(0) # empty string
    body.addInt32(-1) # NULL
    parseDataRowInto(body, rd)

    check not isCellNull(rd, 0, 0)
    check getCell(rd, 0, 0) == ""
    check isCellNull(rd, 0, 1)
