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
  ## Copy `len` bytes from src starting at off into a new seq[byte].
  result = newSeq[byte](len)
  if len > 0:
    copyMem(addr result[0], addr src[off], len)
