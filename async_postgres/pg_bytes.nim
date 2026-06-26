## Low-level byte buffer helpers for big-endian encoding / bulk copy.
##
## This module is dependency-free (stdlib only) and intended to be imported
## from both ``pg_protocol`` and ``pg_types/*`` without introducing circular
## dependencies. Prefer these helpers over hand-written ``copyMem`` calls
## for readability and to keep ``addr`` use localized.

template writeBE16*(buf: var openArray[byte], pos: int, v: int16) =
  buf[pos] = byte((v shr 8) and 0xFF)
  buf[pos + 1] = byte(v and 0xFF)

template writeBE32*(buf: var openArray[byte], pos: int, v: int32) =
  buf[pos] = byte((v shr 24) and 0xFF)
  buf[pos + 1] = byte((v shr 16) and 0xFF)
  buf[pos + 2] = byte((v shr 8) and 0xFF)
  buf[pos + 3] = byte(v and 0xFF)

template writeBE64*(buf: var openArray[byte], pos: int, v: int64) =
  buf[pos] = byte((v shr 56) and 0xFF)
  buf[pos + 1] = byte((v shr 48) and 0xFF)
  buf[pos + 2] = byte((v shr 40) and 0xFF)
  buf[pos + 3] = byte((v shr 32) and 0xFF)
  buf[pos + 4] = byte((v shr 24) and 0xFF)
  buf[pos + 5] = byte((v shr 16) and 0xFF)
  buf[pos + 6] = byte((v shr 8) and 0xFF)
  buf[pos + 7] = byte(v and 0xFF)

func toBE16*(v: int16): array[2, byte] {.inline.} =
  ## Encode a 16-bit integer as big-endian bytes.
  [byte((v shr 8) and 0xFF), byte(v and 0xFF)]

func toBE32*(v: int32): array[4, byte] {.inline.} =
  ## Encode a 32-bit integer as big-endian bytes.
  [
    byte((v shr 24) and 0xFF),
    byte((v shr 16) and 0xFF),
    byte((v shr 8) and 0xFF),
    byte(v and 0xFF),
  ]

func toBE64*(v: int64): array[8, byte] {.inline.} =
  ## Encode a 64-bit integer as big-endian bytes.
  [
    byte((v shr 56) and 0xFF),
    byte((v shr 48) and 0xFF),
    byte((v shr 40) and 0xFF),
    byte((v shr 32) and 0xFF),
    byte((v shr 24) and 0xFF),
    byte((v shr 16) and 0xFF),
    byte((v shr 8) and 0xFF),
    byte(v and 0xFF),
  ]

func fromBE16*(data: openArray[byte], offset = 0): int16 {.inline.} =
  ## Decode a big-endian 16-bit integer from `data` at `offset`.
  int16(data[offset]) shl 8 or int16(data[offset + 1])

func fromBE32*(data: openArray[byte], offset = 0): int32 {.inline.} =
  ## Decode a big-endian 32-bit integer from `data` at `offset`.
  int32(data[offset]) shl 24 or int32(data[offset + 1]) shl 16 or
    int32(data[offset + 2]) shl 8 or int32(data[offset + 3])

func fromBE64*(data: openArray[byte], offset = 0): int64 {.inline.} =
  ## Decode a big-endian 64-bit integer from `data` at `offset`.
  int64(data[offset]) shl 56 or int64(data[offset + 1]) shl 48 or
    int64(data[offset + 2]) shl 40 or int64(data[offset + 3]) shl 32 or
    int64(data[offset + 4]) shl 24 or int64(data[offset + 5]) shl 16 or
    int64(data[offset + 6]) shl 8 or int64(data[offset + 7])

func decodeFloat32BE*(data: openArray[byte], offset = 0): float32 {.inline.} =
  ## Decode a big-endian IEEE-754 32-bit float from `data` at `offset`.
  cast[float32](cast[uint32](fromBE32(data, offset)))

func decodeFloat64BE*(data: openArray[byte], offset = 0): float64 {.inline.} =
  ## Decode a big-endian IEEE-754 64-bit float from `data` at `offset`.
  cast[float64](cast[uint64](fromBE64(data, offset)))

template writeBytesAt*(dst: var openArray[byte], pos: int, src: openArray[byte]) =
  ## Copy src bytes into dst starting at pos. No-op when src is empty.
  if src.len > 0:
    copyMem(addr dst[pos], addr src[0], src.len)

template appendBytes*(buf: var seq[byte], src: openArray[byte]) =
  ## Append src bytes to the end of buf. No-op when src is empty.
  if src.len > 0:
    buf.add(src)

proc readString*(src: openArray[byte], off, len: int): string =
  ## Copy `len` bytes from src starting at off into a new string.
  result = newString(len)
  if len > 0:
    copyMem(addr result[0], addr src[off], len)

proc readBytes*(src: openArray[byte], off, len: int): seq[byte] =
  ## Copy `len` bytes from src starting at off into a new `seq[byte]`.
  result = newSeq[byte](len)
  if len > 0:
    copyMem(addr result[0], addr src[off], len)
