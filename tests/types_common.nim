## Shared fixtures for the ``test_types_*`` unit tests.

import std/[options, times]

import ../async_postgres/pg_protocol
import ../async_postgres/pg_types
from ../async_postgres/pg_types/encoding import dimsFor1D

# Test-local shims for the legacy 1-D ``encodeBinaryArray`` and
# ``encodeBinaryArrayEmpty`` shapes that were removed when the encoder was
# generalized to N dimensions. They delegate to the new N-D entry point so
# the existing fixture-style tests keep working unchanged.
proc encodeBinaryArrayEmpty*(elemOid: int32): seq[byte] =
  encodeBinaryArray(elemOid, newSeq[int32](), newSeq[Option[seq[byte]]]())

proc encodeBinaryArray*(elemOid: int32, elements: seq[seq[byte]]): seq[byte] =
  var opts = newSeq[Option[seq[byte]]](elements.len)
  for i, e in elements:
    opts[i] = some(e)
  encodeBinaryArray(elemOid, dimsFor1D(elements.len), opts)

proc encodeBinaryArray*(elemOid: int32, elements: seq[Option[seq[byte]]]): seq[byte] =
  encodeBinaryArray(elemOid, dimsFor1D(elements.len), elements)

proc mkField*(typeOid: int32, formatCode: int16): FieldDescription =
  FieldDescription(
    name: "test",
    tableOid: 0,
    columnAttrNum: 0,
    typeOid: typeOid,
    typeSize: 0,
    typeMod: 0,
    formatCode: formatCode,
  )

proc mkRow*(cells: seq[Option[seq[byte]]], fields: seq[FieldDescription]): Row =
  ## Build a Row from cell data with format metadata from fields.
  let rd = RowData(
    numCols: int16(cells.len),
    buf: @[],
    cellIndex: newSeq[int32](cells.len * 2),
    colFormats: newSeq[int16](fields.len),
    colTypeOids: newSeq[int32](fields.len),
    fields: fields,
  )
  for i in 0 ..< fields.len:
    rd.colFormats[i] = fields[i].formatCode
    rd.colTypeOids[i] = fields[i].typeOid
  for i, cell in cells:
    if cell.isNone:
      rd.cellIndex[i * 2] = 0'i32
      rd.cellIndex[i * 2 + 1] = -1'i32
    else:
      let data = cell.get
      rd.cellIndex[i * 2] = int32(rd.buf.len)
      rd.cellIndex[i * 2 + 1] = int32(data.len)
      rd.buf.add(data)
  initRow(rd, 0)

# User-defined enum fixtures

type
  Mood* = enum
    happy = "happy"
    sad = "sad"
    ok = "ok"

  Color* = enum
    red
    green
    blue

  YesNo* = enum
    t = "t"
    f = "f"

pgEnum(Mood)
pgEnum(Color, 99999)

# Composite type fixtures

type
  PointRecord* = object
    x*: float64
    y*: float64

  PersonRecord* = object
    name*: string
    age*: int32
    score*: float64

  NullableRecord* = object
    name*: string
    age*: Option[int32]
    note*: Option[string]

  WideIntRecord* = object
    name*: string
    n*: int64

  TimestampRecord* = object
    label*: string
    at*: DateTime

  NameFieldRecord* = object
    name*: string

pgComposite(PointRecord)
pgComposite(PersonRecord, 50000'i32)
pgComposite(NullableRecord)
pgComposite(WideIntRecord)
pgComposite(TimestampRecord)
pgComposite(NameFieldRecord)
