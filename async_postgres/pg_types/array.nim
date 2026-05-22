import std/options

import core

type PgArray*[T] = object
  ## N-dimensional PostgreSQL array value.
  ##
  ## ``dims.len`` is the number of dimensions. ``dims.len == 0`` represents
  ## an empty array (PostgreSQL ``ndim=0``); otherwise ``elements`` holds
  ## the values in row-major (lexicographic) order with
  ## ``elements.len == product(dims)``. ``lowerBounds.len`` always equals
  ## ``dims.len``; the default lower bound for each dimension is ``1``,
  ## matching PostgreSQL's default. NULL elements are represented as
  ## ``none(T)``.
  dims*: seq[int32]
  lowerBounds*: seq[int32]
  elements*: seq[Option[T]]

const PgArrayMaxDim* = 6'i32
  ## Maximum number of array dimensions supported, matching PostgreSQL's
  ## ``MAXDIM`` (``src/include/c.h``). Both the encoder and the decoder
  ## reject arrays whose ``ndim`` exceeds this value.

proc expectedElemCount*(dims: openArray[int32]): int =
  ## Compute the expected number of elements for an array with the given
  ## dimensions. By convention ``dims.len == 0`` returns ``0`` (empty array
  ## marker); otherwise returns ``product(dims)``. Raises ``PgTypeError`` on
  ## any per-dim length ``<= 0`` (PostgreSQL represents empty arrays with
  ## ``ndim == 0`` rather than a zero-sized dimension), or when the product
  ## would exceed ``int32.high`` (PostgreSQL's wire format uses ``int32`` for
  ## element counts). ``PgTypeError`` derives from ``PgError`` so existing
  ## ``except PgError`` callers keep working.
  if dims.len == 0:
    return 0
  var p: int64 = 1
  for d in dims:
    if d <= 0:
      raise newException(
        PgTypeError, "Array dimension length must be > 0 when ndim > 0 (got " & $d & ")"
      )
    p *= int64(d)
    if p > int64(int32.high):
      raise newException(
        PgTypeError, "Array element count overflows int32 (product of dims)"
      )
  return int(p)

proc validatePgArrayShape*(dims, lowerBounds: openArray[int32], elementsLen: int) =
  ## Validate ``dims``/``lowerBounds``/``elements`` shape consistency for an
  ## N-dimensional array. Raises ``PgTypeError`` on mismatch, exceeded
  ## ``PgArrayMaxDim``, element-count overflow, or any per-dim length ``<= 0``
  ## when ``dims.len > 0`` (PostgreSQL represents empty arrays with
  ## ``ndim == 0`` rather than a zero-sized dimension). ``PgTypeError`` derives
  ## from ``PgError`` so existing ``except PgError`` callers keep working.
  if dims.len != lowerBounds.len:
    raise newException(
      PgTypeError,
      "Array dims.len (" & $dims.len & ") != lowerBounds.len (" & $lowerBounds.len & ")",
    )
  if dims.len > int(PgArrayMaxDim):
    raise newException(
      PgTypeError,
      "Too many array dimensions (max " & $PgArrayMaxDim & "): got " & $dims.len,
    )
  for d in dims:
    if d <= 0:
      raise newException(
        PgTypeError,
        "Array dimension length must be > 0 when ndim > 0 (got " & $d &
          "); use an empty PgArray (dims = @[]) for an empty array",
      )
  let expected = expectedElemCount(dims)
  if elementsLen != expected:
    if elementsLen == 0 and dims.len > 0:
      raise newException(
        PgTypeError,
        "Array elements.len (0) != product(dims) (" & $expected &
          "); use an empty PgArray (dims = @[]) for an empty array",
      )
    raise newException(
      PgTypeError,
      "Array elements.len (" & $elementsLen & ") != product(dims) (" & $expected & ")",
    )

proc validate*[T](v: PgArray[T]) =
  ## Validate that ``v`` is internally consistent.
  validatePgArrayShape(v.dims, v.lowerBounds, v.elements.len)

proc defaultLowerBounds(n: int): seq[int32] =
  result = newSeq[int32](n)
  for i in 0 ..< n:
    result[i] = 1

proc pgArray*[T](elements: openArray[T]): PgArray[T] =
  ## Construct a 1-dimensional ``PgArray`` from non-NULL elements. An empty
  ## ``elements`` produces an empty array (``dims = @[]``). Raises
  ## ``PgTypeError`` when ``elements.len > int32.high`` (PostgreSQL's wire
  ## format uses ``int32`` for element counts).
  if elements.len == 0:
    result.dims = @[]
    result.lowerBounds = @[]
    result.elements = @[]
    return
  if elements.len > int32.high.int:
    raise newException(
      PgTypeError, "Array has too many elements for PostgreSQL binary format"
    )
  result.dims = @[int32(elements.len)]
  result.lowerBounds = @[1'i32]
  result.elements = newSeq[Option[T]](elements.len)
  for i, e in elements:
    result.elements[i] = some(e)
  validate(result)

proc pgArray*[T](elements: openArray[Option[T]]): PgArray[T] =
  ## Construct a 1-dimensional ``PgArray`` from elements that may be NULL.
  ## An empty ``elements`` produces an empty array (``dims = @[]``). Raises
  ## ``PgTypeError`` when ``elements.len > int32.high``.
  if elements.len == 0:
    result.dims = @[]
    result.lowerBounds = @[]
    result.elements = @[]
    return
  if elements.len > int32.high.int:
    raise newException(
      PgTypeError, "Array has too many elements for PostgreSQL binary format"
    )
  result.dims = @[int32(elements.len)]
  result.lowerBounds = @[1'i32]
  result.elements = @elements
  validate(result)

proc pgArray*[T](dims: openArray[int32], elements: openArray[T]): PgArray[T] =
  ## Construct an N-dimensional ``PgArray`` from non-NULL elements. ``elements``
  ## must be in row-major order with ``elements.len == product(dims)``.
  ## ``lowerBounds`` defaults to ``1`` for each dimension.
  result.dims = @dims
  result.lowerBounds = defaultLowerBounds(dims.len)
  result.elements = newSeq[Option[T]](elements.len)
  for i, e in elements:
    result.elements[i] = some(e)
  validate(result)

proc pgArray*[T](dims: openArray[int32], elements: openArray[Option[T]]): PgArray[T] =
  ## Construct an N-dimensional ``PgArray`` from elements that may be NULL.
  ## ``elements`` must be in row-major order with
  ## ``elements.len == product(dims)``. ``lowerBounds`` defaults to ``1`` for
  ## each dimension.
  result.dims = @dims
  result.lowerBounds = defaultLowerBounds(dims.len)
  result.elements = @elements
  validate(result)

proc pgArray*[T](
    dims, lowerBounds: openArray[int32], elements: openArray[Option[T]]
): PgArray[T] =
  ## Construct an N-dimensional ``PgArray`` with explicit ``lowerBounds`` (one
  ## per dimension). ``elements`` must be in row-major order with
  ## ``elements.len == product(dims)``.
  result.dims = @dims
  result.lowerBounds = @lowerBounds
  result.elements = @elements
  validate(result)

proc isEmpty*[T](v: PgArray[T]): bool {.inline.} =
  ## Whether ``v`` is the empty array (``dims.len == 0``).
  v.dims.len == 0

proc ndim*[T](v: PgArray[T]): int {.inline.} =
  ## Number of dimensions. ``0`` for an empty array.
  v.dims.len

proc `==`*[T](a, b: PgArray[T]): bool =
  a.dims == b.dims and a.lowerBounds == b.lowerBounds and a.elements == b.elements

proc `$`*[T](v: PgArray[T]): string =
  result = "PgArray(dims="
  result.add($v.dims)
  result.add(", lowerBounds=")
  result.add($v.lowerBounds)
  result.add(", elements=")
  result.add($v.elements)
  result.add(")")
