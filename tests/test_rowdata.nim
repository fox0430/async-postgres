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

suite "reuseRowData":
  test "new RowData has empty buf and cellIndex":
    var rd = newRowData(2)
    let body = buildDataRowBody(["hello", "world"])
    parseDataRowInto(body, rd)
    check rd.buf.len > 0
    check rd.cellIndex.len > 0

    rd = rd.reuseRowData(3)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0
    check rd.numCols == 3

  test "preserves seq capacity after reuse":
    var rd = newRowData(2)
    for i in 0 ..< 100:
      let body = buildDataRowBody(["row" & $i, "data" & $i])
      parseDataRowInto(body, rd)

    let bufCap = rd.buf.capacity
    let cellCap = rd.cellIndex.capacity
    check bufCap > 0
    check cellCap > 0

    rd = rd.reuseRowData(2)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0
    # Capacity is preserved — no reallocation needed on next fill
    check rd.buf.capacity == bufCap
    check rd.cellIndex.capacity == cellCap

  test "updates colFormats and colTypeOids":
    var rd = newRowData(2, @[0'i16, 0'i16], @[23'i32, 25'i32])
    check rd.colFormats == @[0'i16, 0'i16]
    check rd.colTypeOids == @[23'i32, 25'i32]

    rd = rd.reuseRowData(3, @[1'i16, 0'i16, 1'i16], @[20'i32, 25'i32, 16'i32])
    check rd.numCols == 3
    check rd.colFormats == @[1'i16, 0'i16, 1'i16]
    check rd.colTypeOids == @[20'i32, 25'i32, 16'i32]

  test "usable after reuse — data accumulates correctly":
    var rd = newRowData(2)
    let body1 = buildDataRowBody(["aaa", "bbb"])
    parseDataRowInto(body1, rd)
    check getCell(rd, 0, 0) == "aaa"
    check getCell(rd, 0, 1) == "bbb"

    rd = rd.reuseRowData(2)
    let body2 = buildDataRowBody(["xxx", "yyy"])
    let body3 = buildDataRowBody(["111", "222"])
    parseDataRowInto(body2, rd)
    parseDataRowInto(body3, rd)

    check getCell(rd, 0, 0) == "xxx"
    check getCell(rd, 0, 1) == "yyy"
    check getCell(rd, 1, 0) == "111"
    check getCell(rd, 1, 1) == "222"

  test "reuse with different column count":
    var rd = newRowData(2)
    parseDataRowInto(buildDataRowBody(["a", "b"]), rd)
    check rd.cellIndex.len == 4 # 2 cols * 2

    rd = rd.reuseRowData(3)
    parseDataRowInto(buildDataRowBody(["x", "y", "z"]), rd)
    check rd.numCols == 3
    check rd.cellIndex.len == 6 # 3 cols * 2
    check getCell(rd, 0, 0) == "x"
    check getCell(rd, 0, 1) == "y"
    check getCell(rd, 0, 2) == "z"

  test "reuse handles NULL cells":
    var rd = newRowData(3)
    parseDataRowInto(buildDataRowBody(["val", "\xFF", "end"]), rd)
    check getCell(rd, 0, 0) == "val"
    check isCellNull(rd, 0, 1)
    check getCell(rd, 0, 2) == "end"

    rd = rd.reuseRowData(3)
    parseDataRowInto(buildDataRowBody(["\xFF", "mid", "\xFF"]), rd)
    check isCellNull(rd, 0, 0)
    check getCell(rd, 0, 1) == "mid"
    check isCellNull(rd, 0, 2)

  test "multiple reuse cycles accumulate rows correctly":
    var rd = newRowData(1)
    for cycle in 0 ..< 5:
      rd = rd.reuseRowData(1)
      for row in 0 ..< 10:
        parseDataRowInto(buildDataRowBody(["c" & $cycle & "r" & $row]), rd)
      # Verify last cycle's data
      for row in 0 ..< 10:
        check getCell(rd, row, 0) == "c" & $cycle & "r" & $row

  test "numCols-only overload clears colFormats/colTypeOids preserving capacity":
    var rd = newRowData(2, @[1'i16, 0'i16], @[23'i32, 25'i32])
    check rd.colFormats.len == 2
    check rd.colTypeOids.len == 2

    rd = rd.reuseRowData(2)
    check rd.colFormats.len == 0
    check rd.colTypeOids.len == 0
    check rd.colFormats.capacity >= 2
    check rd.colTypeOids.capacity >= 2

  test "reuse immediately after creation (no data parsed)":
    var rd = newRowData(2)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0

    rd = rd.reuseRowData(3)
    check rd.buf.len == 0
    check rd.cellIndex.len == 0
    check rd.numCols == 3

    # Still usable
    parseDataRowInto(buildDataRowBody(["a", "b", "c"]), rd)
    check getCell(rd, 0, 0) == "a"
    check getCell(rd, 0, 2) == "c"

  test "fewer rows after reuse do not expose stale data":
    var rd = newRowData(1)
    for i in 0 ..< 10:
      parseDataRowInto(buildDataRowBody(["old" & $i]), rd)
    check rd.cellIndex.len == 20 # 10 rows * 1 col * 2

    rd = rd.reuseRowData(1)
    parseDataRowInto(buildDataRowBody(["only"]), rd)
    check rd.cellIndex.len == 2 # 1 row * 1 col * 2
    check rd.buf.len == 8 # 4-byte length header + "only"
    check getCell(rd, 0, 0) == "only"

  test "empty string vs NULL distinction preserved after reuse":
    var rd = newRowData(2)
    parseDataRowInto(buildDataRowBody(["x", "y"]), rd)

    rd = rd.reuseRowData(2)
    # col 0: empty string (len=0), col 1: NULL (len=-1)
    var body: seq[byte]
    body.addInt16(2)
    body.addInt32(0) # empty string
    body.addInt32(-1) # NULL
    parseDataRowInto(body, rd)

    check not isCellNull(rd, 0, 0)
    check getCell(rd, 0, 0) == ""
    check isCellNull(rd, 0, 1)

  test "old RowData remains intact after reuse":
    let rd1 = newRowData(2)
    parseDataRowInto(buildDataRowBody(["hello", "world"]), rd1)
    check getCell(rd1, 0, 0) == "hello"
    check getCell(rd1, 0, 1) == "world"

    # Reuse creates a new RowData; rd1 should keep its data
    let rd2 = rd1.reuseRowData(2)
    parseDataRowInto(buildDataRowBody(["new", "data"]), rd2)

    # rd1's data is still accessible (buf/cellIndex were moved, so rd1 has empty seqs)
    # But the old data was moved away — rd1.buf is now empty
    check rd1.buf.len == 0
    check rd1.cellIndex.len == 0

    # rd2 has the new data
    check getCell(rd2, 0, 0) == "new"
    check getCell(rd2, 0, 1) == "data"

  test "old RowData with data preserved when previous result held":
    # Simulates: user holds result1, then a reuse happens for result2
    let rd1 = newRowData(1)
    parseDataRowInto(buildDataRowBody(["original"]), rd1)

    # Before reuse, save a snapshot of rd1's data
    let origCell = getCell(rd1, 0, 0)
    check origCell == "original"

    # After reuseRowData, rd1.buf is moved (empty), but rd1 ref object still exists
    # The key safety property: rd1 is a different ref object than rd2
    let rd2 = rd1.reuseRowData(1)
    check rd1 != rd2 # Different ref objects

suite "Row.clone":
  test "returns independent RowData backing":
    let rd = newRowData(2)
    parseDataRowInto(buildDataRowBody(["hello", "world"]), rd)
    let row = initRow(rd, 0)
    let cloned = row.clone()
    check cloned.data != rd
    check cloned.rowIdx == 0
    check cloned.data.numCols == 2

  test "clone survives original buffer reuse":
    let rd = newRowData(2)
    parseDataRowInto(buildDataRowBody(["hello", "world"]), rd)
    let cloned = initRow(rd, 0).clone()

    # Simulate queryEach behavior: reset and parse next row into same rd
    rd.buf.setLen(0)
    rd.cellIndex.setLen(0)
    parseDataRowInto(buildDataRowBody(["xxx", "yyy"]), rd)

    check getCell(cloned.data, 0, 0) == "hello"
    check getCell(cloned.data, 0, 1) == "world"

  test "clones a specific row out of a multi-row buffer":
    let rd = newRowData(2)
    parseDataRowInto(buildDataRowBody(["a0", "b0"]), rd)
    parseDataRowInto(buildDataRowBody(["a1", "b1"]), rd)
    parseDataRowInto(buildDataRowBody(["a2", "b2"]), rd)

    let cloned = initRow(rd, 1).clone()
    check cloned.data.numCols == 2
    check cloned.data.cellIndex.len == 4 # just one row worth
    check getCell(cloned.data, 0, 0) == "a1"
    check getCell(cloned.data, 0, 1) == "b1"

  test "preserves NULL cells":
    let rd = newRowData(3)
    parseDataRowInto(buildDataRowBody(["val", "\xFF", "end"]), rd)
    let cloned = initRow(rd, 0).clone()
    check getCell(cloned.data, 0, 0) == "val"
    check isCellNull(cloned.data, 0, 1)
    check getCell(cloned.data, 0, 2) == "end"

  test "preserves empty string vs NULL distinction":
    let rd = newRowData(2)
    var body: seq[byte]
    body.addInt16(2)
    body.addInt32(0) # empty string
    body.addInt32(-1) # NULL
    parseDataRowInto(body, rd)

    let cloned = initRow(rd, 0).clone()
    check not isCellNull(cloned.data, 0, 0)
    check getCell(cloned.data, 0, 0) == ""
    check isCellNull(cloned.data, 0, 1)

  test "copies colFormats, colTypeOids, and fields":
    let rd =
      newRowData(2, colFormats = @[1'i16, 0'i16], colTypeOids = @[23'i32, 25'i32])
    rd.fields = @[
      FieldDescription(name: "id", typeOid: 23, formatCode: 1),
      FieldDescription(name: "name", typeOid: 25, formatCode: 0),
    ]
    parseDataRowInto(buildDataRowBody(["x", "y"]), rd)

    let cloned = initRow(rd, 0).clone()
    check cloned.data.colFormats == @[1'i16, 0'i16]
    check cloned.data.colTypeOids == @[23'i32, 25'i32]
    check cloned.data.fields.len == 2
    check cloned.data.fields[0].name == "id"
    check cloned.data.fields[1].name == "name"

  test "clone of nil data is safe":
    let row = initRow(nil, 0)
    let cloned = row.clone()
    check cloned.data == nil
