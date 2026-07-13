## RFC 4013 SASLprep (stringprep profile for user names and passwords).
## Applied to passwords before SCRAM-SHA-256 PBKDF2 so that PostgreSQL,
## which stores pg_saslprep-normalized verifiers, accepts passwords whose
## Unicode form differs from the raw input (e.g. U+00A0 -> U+0020,
## U+FB01 "fi" ligature -> "fi", NFKC-decomposed forms).
##
## Unassigned code points (RFC 3454 Table A.1, Unicode 3.2) are not
## prohibited here. This deviates from PostgreSQL's pg_saslprep, which
## rejects them; the A.1 table is large and the practical impact is low
## (such a password could not have been set via `CREATE ROLE` either).

import std/[strutils, unicode]

import pkg/nimcrypto/utils as ncutils
import pkg/normalize
import pkg/unicodedb/properties

import pg_errors

template burnString(s: var string) =
  if s.len > 0:
    ncutils.burnMem(addr s[0], s.len)
    s.setLen(0)

proc isMapToNothing(cp: int32): bool =
  ## RFC 3454 Table B.1.
  case cp
  of 0x00AD, 0x034F, 0x1806, 0x180B, 0x180C, 0x180D, 0x200B, 0x200C, 0x200D, 0x2060,
      0xFE00, 0xFE01, 0xFE02, 0xFE03, 0xFE04, 0xFE05, 0xFE06, 0xFE07, 0xFE08, 0xFE09,
      0xFE0A, 0xFE0B, 0xFE0C, 0xFE0D, 0xFE0E, 0xFE0F, 0xFEFF:
    true
  else:
    false

proc isNonAsciiSpace(cp: int32): bool =
  ## RFC 3454 Table C.1.2. Mapped to U+0020 by SASLprep.
  case cp
  of 0x00A0, 0x1680, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007,
      0x2008, 0x2009, 0x200A, 0x200B, 0x202F, 0x205F, 0x3000:
    true
  else:
    false

proc isProhibited(cp: int32): bool =
  ## Aggregate of RFC 3454 Tables C.1.2, C.2.1, C.2.2, C.3, C.4, C.5, C.6,
  ## C.7, C.8, C.9 as required by RFC 4013 Section 2.3.
  if cp <= 0x001F or cp == 0x007F:
    return true # C.2.1
  if isNonAsciiSpace(cp):
    return true # C.1.2
  if cp >= 0x0080 and cp <= 0x009F:
    return true # C.2.2 range
  case cp
  of 0x06DD, 0x070F, 0x180E, 0x200C, 0x200D, 0x2028, 0x2029, 0x2060, 0x2061, 0x2062,
      0x2063, 0x206A, 0x206B, 0x206C, 0x206D, 0x206E, 0x206F, 0xFEFF:
    return true # C.2.2 discrete
  else:
    discard
  if cp >= 0x1D173 and cp <= 0x1D17A:
    return true # C.2.2 musical control
  if cp >= 0xE000 and cp <= 0xF8FF:
    return true # C.3 BMP private use
  if cp >= 0xF0000 and cp <= 0xFFFFD:
    return true # C.3 SPUA-A
  if cp >= 0x100000 and cp <= 0x10FFFD:
    return true # C.3 SPUA-B
  if cp >= 0xFDD0 and cp <= 0xFDEF:
    return true # C.4 non-character block
  if (cp and 0xFFFF) == 0xFFFE or (cp and 0xFFFF) == 0xFFFF:
    return true # C.4 plane-final non-characters
  if cp >= 0xD800 and cp <= 0xDFFF:
    return true # C.5 surrogate
  if cp >= 0xFFF9 and cp <= 0xFFFD:
    return true # C.6
  if cp >= 0x2FF0 and cp <= 0x2FFB:
    return true # C.7
  case cp
  of 0x0340, 0x0341, 0x200E, 0x200F, 0x202A, 0x202B, 0x202C, 0x202D, 0x202E:
    return true # C.8 (0x206A..0x206F already covered by C.2.2)
  else:
    discard
  if cp == 0xE0001:
    return true # C.9
  if cp >= 0xE0020 and cp <= 0xE007F:
    return true # C.9
  return false

proc saslprep*(input: string): string =
  ## Apply RFC 4013 SASLprep. Raises `PgConnectionError` for prohibited
  ## code points, bidi rule violations, or empty passwords.
  # Fast path: SASLprep is the identity on printable ASCII (0x20..0x7E).
  var asciiFast = true
  for c in input:
    let b = c.uint8
    if b < 0x20u8 or b > 0x7Eu8:
      asciiFast = false
      break
  if asciiFast:
    if input.len == 0:
      raise newException(PgConnectionError, "SASLprep: empty password")
    # Return a heap-allocated copy (not sharing the caller's buffer) so the
    # caller can safely wipe the result.
    result = newString(input.len)
    copyMem(addr result[0], unsafeAddr input[0], input.len)
    return

  # Step 1: map (C.1.2 -> U+0020 takes precedence over B.1 -> delete).
  # U+200B is the only code point in both tables; PostgreSQL maps it to
  # space, so non-ASCII space is checked first.
  var mapped = newStringOfCap(input.len)
  try:
    for r in runes(input):
      let cp = int32(r)
      if isNonAsciiSpace(cp):
        mapped.add(char(0x20))
      elif isMapToNothing(cp):
        discard
      else:
        mapped.add(r.toUTF8)

    if mapped.len == 0:
      raise newException(PgConnectionError, "SASLprep: empty password")

    # Step 3 + 4: prohibit + bidi (RFC 3454 Section 6), run on the
    # post-mapping string (matching PostgreSQL's pg_saslprep).
    var hasRandAL = false
    var hasL = false
    var firstBidi = ""
    var lastBidi = ""
    var first = true
    for r in runes(mapped):
      let cp = int32(r)
      if isProhibited(cp):
        raise newException(
          PgConnectionError, "SASLprep: prohibited code point U+" & toHex(cp.int64, 4)
        )
      let bidi = bidirectional(r)
      if bidi == "R" or bidi == "AL":
        hasRandAL = true
      elif bidi == "L":
        hasL = true
      if first:
        firstBidi = bidi
        first = false
      lastBidi = bidi

    if hasRandAL and hasL:
      raise newException(
        PgConnectionError, "SASLprep: bidi violation (RandALCat and LCat both present)"
      )
    if hasRandAL and (
      (firstBidi != "R" and firstBidi != "AL") or (lastBidi != "R" and lastBidi != "AL")
    ):
      raise newException(
        PgConnectionError,
        "SASLprep: bidi violation (RandALCat string must start and end with RandALCat)",
      )

    # Step 2: NFKC normalize.
    result = toNFKC(mapped)
  finally:
    burnString(mapped)
