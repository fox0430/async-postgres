import std/[unittest, strutils, sequtils]
from std/unicode import Rune, add

import ../async_postgres/pg_saslprep {.all.}

suite "SASLprep - ASCII fast path":
  test "empty string returns empty":
    check saslprep("") == ""

  test "printable ASCII is identity":
    check saslprep("pencil") == "pencil"
    check saslprep("Password123!") == "Password123!"
    check saslprep("a b c") == "a b c"

  test "ASCII control characters fall back to raw (fast path bypassed)":
    # Fast path checks 0x20..0x7E; anything else falls through to the
    # prohibit stage, which now falls back to raw instead of raising.
    check saslprep("bad\x07pass") == "bad\x07pass" # BEL
    check saslprep("tab\tpass") == "tab\tpass" # TAB is 0x09
    check saslprep("del\x7Fpass") == "del\x7Fpass" # DEL

suite "SASLprep - RFC 3454 B.1 (map to nothing)":
  test "soft hyphen U+00AD is deleted":
    check saslprep("I\xC2\xADX") == "IX"

  test "ZWNJ U+200C is deleted (B.1, not in C.1.2)":
    # 0x200C is in B.1 (delete) and C.2.2 (prohibited) but not C.1.2.
    # B.1 mapping runs first so the character is removed before the
    # prohibit check.
    check saslprep("a\xE2\x80\x8Cb") == "ab"

  test "ZWSP U+200B is mapped to space (C.1.2 wins over B.1)":
    # 0x200B is in both C.1.2 (map to space) and B.1 (delete).
    # PostgreSQL checks C.1.2 first, so it becomes U+0020.
    check saslprep("a\xE2\x80\x8Bb") == "a b"

  test "variation selectors U+FE00..U+FE0F are deleted":
    check saslprep("a\xEF\xB8\x80b") == "ab" # U+FE00
    check saslprep("a\xEF\xB8\x8Fb") == "ab" # U+FE0F

suite "SASLprep - RFC 3454 C.1.2 (non-ASCII space -> U+0020)":
  test "no-break space U+00A0 becomes ASCII space":
    check saslprep("a\xC2\xA0b") == "a b"

  test "ideographic space U+3000 becomes ASCII space":
    check saslprep("a\xE3\x80\x80b") == "a b"

  test "narrow no-break space U+202F becomes ASCII space":
    check saslprep("a\xE2\x80\xAFb") == "a b"

  test "Ogham space mark U+1680 becomes ASCII space":
    check saslprep("a\xE1\x9A\x80b") == "a b"

suite "SASLprep - RFC 3454 range tables":
  test "every table is sorted, disjoint inclusive pairs":
    for table in [
      @prohibitedRanges,
      @unassignedRanges,
      @randALCatRanges,
      @lCatRanges,
      rejectedRanges,
    ]:
      check table.len mod 2 == 0
      for k in 0 ..< table.len div 2:
        check table[2 * k] <= table[2 * k + 1]
        if k > 0:
          check table[2 * k - 1] < table[2 * k]

  test "merged reject table is the union of prohibited and unassigned":
    var bad = -1'i32
    for cp in 0'i32 .. 0x10FFFF:
      if inRanges(rejectedRanges, cp) !=
          (inRanges(prohibitedRanges, cp) or inRanges(unassignedRanges, cp)):
        bad = cp
        break
    check bad == -1

suite "SASLprep - NFKC normalization":
  test "compat decomposition of feminine ordinal U+00AA -> a":
    check saslprep("\xC2\xAA") == "a"

  test "compat decomposition of Roman numeral nine U+2168 -> IX":
    check saslprep("\xE2\x85\xA8") == "IX"

  test "ligature fi U+FB01 -> fi":
    check saslprep("of\xEF\xAC\x81ce") == "office"

  test "full-width Latin A U+FF21 -> A":
    check saslprep("\xEF\xBC\xA1BC") == "ABC"

suite "SASLprep - prohibited fallback (matches PostgreSQL)":
  # PG server and libpq fall back to raw bytes when SASLprep rejects the
  # post-mapping string; the raw password is then hashed as-is. Match this
  # so a role created with `CREATE ROLE ... PASSWORD '<contains-prohibited>'`
  # can authenticate.

  test "ASCII NUL (C.2.1) falls back to raw":
    let raw = "x\x00y"
    check saslprep(raw) == raw

  test "non-ASCII control U+0085 NEL (C.2.2) falls back to raw":
    let raw = "x\xC2\x85y"
    check saslprep(raw) == raw

  test "line separator U+2028 (C.2.2) falls back to raw":
    let raw = "x\xE2\x80\xA8y"
    check saslprep(raw) == raw

  test "BOM U+FEFF is deleted by B.1 (not rejected)":
    # U+FEFF is in B.1 too; B.1 wins so it is deleted rather than rejected.
    check saslprep("a\xEF\xBB\xBFb") == "ab"

  test "private use U+E000 (C.3) falls back to raw":
    let raw = "x\xEE\x80\x80y"
    check saslprep(raw) == raw

  test "non-character U+FFFE (C.4) falls back to raw":
    let raw = "x\xEF\xBF\xBEy"
    check saslprep(raw) == raw

  test "non-character U+FDD0 (C.4) falls back to raw":
    let raw = "x\xEF\xB7\x90y"
    check saslprep(raw) == raw

  test "inappropriate for plain text U+FFFD (C.6) falls back to raw":
    let raw = "x\xEF\xBF\xBDy"
    check saslprep(raw) == raw

  test "left-to-right mark U+200E (C.8) falls back to raw":
    let raw = "x\xE2\x80\x8Ey"
    check saslprep(raw) == raw

  test "fallback preserves original bytes, not post-mapping bytes":
    # U+00A0 would map to 0x20 under normal SASLprep; the prohibited
    # NEL (U+0085) triggers fallback, so the mapping must be discarded
    # and the raw input returned verbatim.
    let raw = "a\xC2\xA0b\xC2\x85c"
    check saslprep(raw) == raw

suite "SASLprep - unassigned in Unicode 3.2 (A.1) falls back to raw":
  # U+00A0 maps to a space on success, so a raw result proves the fallback
  # for code points that SASLprep would otherwise leave unchanged.

  test "emoji U+1F600 next to U+00A0":
    let raw = "pass\xC2\xA0word\xF0\x9F\x98\x80"
    check saslprep(raw) == raw

  test "U+1F100, whose NFKC form is \"0.\", is not normalized":
    let raw = "\xF0\x9F\x84\x80"
    check saslprep(raw) == raw

  test "range edges U+0221, U+0234, U+024F, U+E0080, U+EFFFD":
    for cp in [
      "\xC8\xA1", "\xC8\xB4", "\xC9\x8F", "\xF3\xA0\x82\x80", "\xF3\xAF\xBF\xBD"
    ]:
      let raw = "a\xC2\xA0" & cp
      check saslprep(raw) == raw

  test "assigned neighbours U+0233 and U+0250 still map":
    check saslprep("a\xC2\xA0\xC8\xB3") == "a \xC8\xB3"
    check saslprep("a\xC2\xA0\xC9\x90") == "a \xC9\x90"

suite "SASLprep - bidirectional (RFC 3454 Section 6)":
  test "ARABIC ALEF alone is accepted":
    let alef = "\xD8\xA7" # U+0627
    check saslprep(alef) == alef

  test "RandALCat mixed with LCat falls back to raw":
    let raw = "\xD8\xA7A" # U+0627 (AL) + Latin 'A' (L)
    check saslprep(raw) == raw

  test "RandALCat must start and end with RandALCat, else raw":
    let raw = "\xD8\xA71" # U+0627 (AL) + '1' (EN) → ends non-RandALCat
    check saslprep(raw) == raw

  test "LCat-only string is accepted":
    check saslprep("abc") == "abc"

  test "RandALCat string bracketed by RandALCat is accepted":
    # U+0627 U+0628 (both AL). First and last are AL.
    let s = "\xD8\xA7\xD8\xA8"
    check saslprep(s) == s

  test "U+2801 is not LCat in Unicode 3.2 (D.2)":
    # Braille became L after 3.2, so a newer Unicode table would fall back.
    check saslprep("\xD7\x90\xC2\xA0\xE2\xA0\x81\xD7\x90") ==
      "\xD7\x90 \xE2\xA0\x81\xD7\x90"

  test "U+1D6C1 and U+17B4 are LCat in Unicode 3.2 (D.2), so raw":
    # Both are L in D.2 but not in current Unicode; U+1D6C1 would NFKC to U+2207.
    for cp in ["\xF0\x9D\x9B\x81", "\xE1\x9E\xB4"]:
      let raw = "\xD7\x90\xC2\xA0" & cp & "\xD7\x90"
      check saslprep(raw) == raw

suite "SASLprep - invalid UTF-8 fallback (matches PostgreSQL)":
  # PG server and libpq both use the raw bytes when input is not valid
  # UTF-8; returning input unchanged keeps our client verifier in sync.

  test "bare Latin-1 byte is returned as-is":
    let raw = "p\xE9ss" # 0xE9 = Latin-1 'e-acute'
    check saslprep(raw) == raw

  test "bare continuation byte is returned as-is":
    let raw = "x\x80y"
    check saslprep(raw) == raw

  test "truncated 2-byte sequence is returned as-is":
    let raw = "x\xC2y"
    check saslprep(raw) == raw

  test "overlong ASCII encoding is returned as-is":
    let raw = "\xC0\xAF" # would 2-byte encode '/'
    check saslprep(raw) == raw

  test "ASCII fast path still wins over the fallback":
    check saslprep("Password123!") == "Password123!"

  test "code points above U+10FFFF are returned as-is":
    # validateUtf8 accepts these, and toNFKC asserts on them.
    for s in ["\xF4\x90\x80\x80", "\xF5\x80\x80\x80", "\xF7\xBF\xBF\xBF"]:
      check saslprep("abc" & s) == "abc" & s
      check saslprep("a\xC2\xA0" & s) == "a\xC2\xA0" & s

  test "overlong 3- and 4-byte encodings are returned as-is":
    # E0 82 A0 decodes to U+00A0, which would otherwise map to a space.
    for s in ["a\xE0\x82\xA0b", "\xE0\x80\xAF", "\xF0\x80\x80\xAF", "\xF0\x8F\xBF\xBF"]:
      check saslprep(s) == s

  test "shortest forms after the narrowed lead bytes E0, ED, F0 still map":
    check saslprep("a\xC2\xA0\xE0\xA4\x85") == "a \xE0\xA4\x85" # U+0905
    check saslprep("a\xC2\xA0\xED\x9E\xA3") == "a \xED\x9E\xA3" # U+D7A3
    check saslprep("a\xC2\xA0\xF0\xA0\x80\x80") == "a \xF0\xA0\x80\x80" # U+20000

suite "SASLprep - UTF-8 legality (PostgreSQL's pg_utf8_islegal)":
  test "every scalar value's encoding is legal, a surrogate's is not":
    var bad = -1'i32
    var buf = newStringOfCap(4)
    for cp in 0'i32 .. 0x10FFFF:
      buf.setLen(0)
      buf.add Rune(cp)
      if isLegalUtf8(buf) == (cp in 0xD800'i32 .. 0xDFFF'i32):
        bad = cp
        break
    check bad == -1

  test "sequences PostgreSQL rejects are not legal":
    for s in [
      "\x80", "\xBF", "\xC0\x80", "\xC1\xBF", "\xC2", "\xC2\x41", "\xE0\x9F\xBF",
      "\xE1\x80", "\xE1\x80\xC0", "\xED\xA0\x80", "\xED\xBF\xBF", "\xF0\x8F\xBF\xBF",
      "\xF1\x80\x80", "\xF4\x90\x80\x80", "\xF5\x80\x80\x80", "\xF8\x88\x80\x80\x80",
      "\xFF",
    ]:
      check not isLegalUtf8(s)

suite "SASLprep - empty post-mapping fallback":
  test "B.1-only input falls back to raw (not empty)":
    # Soft hyphen maps to nothing; the mapped result is empty, so we
    # fall back to the raw input rather than raising or returning "".
    let raw = "\xC2\xAD"
    check saslprep(raw) == raw

  test "ZWSP-only input maps to a single space":
    check saslprep("\xE2\x80\x8B") == " "

suite "SASLprep - dependencies":
  test "the package keeps a floor on the unicodedb that toNFKC reads":
    # Nothing imports unicodedb directly, and normalize allows >= 0.7.
    const manifest = staticRead("../async_postgres.nimble")
    var floor: seq[int]
    for line in manifest.splitLines:
      if line.strip.startsWith("requires"):
        for spec in line.split('"'):
          if spec.startsWith("unicodedb") and ">=" in spec:
            floor =
              spec.split(">=")[1].splitWhitespace[0].split('.').mapIt(parseInt(it))
    floor.setLen(3)
    check (floor[0], floor[1], floor[2]) >= (0, 13, 2)
