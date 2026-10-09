## Minimal DER reading for the few X.509 / PKCS#8 fields the library inspects.
## Dependency-free and not re-exported by ``async_postgres``.

const oidRsaPss* = [byte 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0A]
  ## id-RSASSA-PSS content bytes, shared by the signature and key checks.

proc derReadLen(data: openArray[byte], pos: var int): int =
  ## DER definite-form length; -1 on malformed input.
  if pos >= data.len:
    return -1
  let first = data[pos]
  inc pos
  if first < 0x80:
    return int(first)
  let n = int(first and 0x7F)
  if n == 0 or n > 4 or pos + n > data.len:
    return -1
  # Accumulate as uint32 to avoid signed-shift UB and to represent full
  # 4-byte DER lengths on 32-bit int platforms.
  var v: uint32 = 0
  for i in 0 ..< n:
    v = (v shl 8) or uint32(data[pos + i])
  pos += n
  if v > uint32(high(int)):
    return -1
  return int(v)

proc derElement*(data: openArray[byte], pos: var int, tag: byte, limit: int): int =
  ## Body length of the `tag` element at `pos`, advancing `pos` past its
  ## header; -1 when the tag differs or the element overruns `limit` or `data`.
  let limit = min(limit, data.len)
  if pos >= limit or data[pos] != tag:
    return -1
  inc pos
  let len = derReadLen(data, pos)
  if len < 0 or len > limit - pos:
    return -1
  len

proc oidValid*(oid: openArray[byte]): bool =
  ## Whether OID content bytes are well formed: non-empty, the last
  ## subidentifier terminated, and every arc within `uint64`.
  if oid.len == 0 or (oid[^1] and 0x80) != 0:
    return false
  var arc = 0'u64
  for b in oid:
    if arc > (high(uint64) shr 7):
      return false
    arc =
      if (b and 0x80) == 0:
        0'u64
      else:
        (arc shl 7) or uint64(b and 0x7F)
  true

proc oidText*(oid: openArray[byte]): string =
  ## Dotted form of OID content bytes, for messages; "" when malformed.
  if not oidValid(oid):
    return ""
  var arcs: seq[uint64]
  var arc = 0'u64
  for b in oid:
    arc = (arc shl 7) or uint64(b and 0x7F)
    if (b and 0x80) == 0:
      arcs.add(arc)
      arc = 0
  # The first subidentifier packs the top two arcs.
  let top = min(arcs[0] div 40, 2)
  result = $top & "." & $(arcs[0] - top * 40)
  for a in arcs[1 ..^ 1]:
    result.add("." & $a)
