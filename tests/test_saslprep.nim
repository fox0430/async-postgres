import std/unittest

import ../async_postgres/[pg_saslprep, pg_errors]

suite "SASLprep - ASCII fast path":
  test "empty string is rejected":
    expect PgConnectionError:
      discard saslprep("")

  test "printable ASCII is identity":
    check saslprep("pencil") == "pencil"
    check saslprep("Password123!") == "Password123!"
    check saslprep("a b c") == "a b c"

  test "ASCII control characters are prohibited (fast path bypassed)":
    # Fast path checks 0x20..0x7E; anything else falls through to the
    # prohibit stage, which rejects C.2.1.
    expect PgConnectionError:
      discard saslprep("bad\x07pass") # BEL
    expect PgConnectionError:
      discard saslprep("tab\tpass") # TAB is 0x09
    expect PgConnectionError:
      discard saslprep("del\x7Fpass") # DEL

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

suite "SASLprep - NFKC normalization":
  test "compat decomposition of feminine ordinal U+00AA -> a":
    check saslprep("\xC2\xAA") == "a"

  test "compat decomposition of Roman numeral nine U+2168 -> IX":
    check saslprep("\xE2\x85\xA8") == "IX"

  test "ligature fi U+FB01 -> fi":
    check saslprep("of\xEF\xAC\x81ce") == "office"

  test "full-width Latin A U+FF21 -> A":
    check saslprep("\xEF\xBC\xA1BC") == "ABC"

suite "SASLprep - prohibited outputs":
  test "ASCII control (C.2.1)":
    expect PgConnectionError:
      discard saslprep("x\x00y")

  test "non-ASCII control range 0x80..0x9F (C.2.2)":
    expect PgConnectionError:
      discard saslprep("x\xC2\x85y") # U+0085 NEL

  test "line separator U+2028 (C.2.2)":
    expect PgConnectionError:
      discard saslprep("x\xE2\x80\xA8y")

  test "BOM U+FEFF (C.2.2)":
    # U+FEFF is in B.1 too; B.1 wins so it is deleted rather than rejected.
    check saslprep("a\xEF\xBB\xBFb") == "ab"

  test "private use U+E000 (C.3)":
    expect PgConnectionError:
      discard saslprep("x\xEE\x80\x80y")

  test "non-character U+FFFE (C.4)":
    expect PgConnectionError:
      discard saslprep("x\xEF\xBF\xBEy")

  test "non-character U+FDD0 (C.4)":
    expect PgConnectionError:
      discard saslprep("x\xEF\xB7\x90y")

  test "inappropriate for plain text U+FFFD (C.6)":
    expect PgConnectionError:
      discard saslprep("x\xEF\xBF\xBDy")

  test "left-to-right mark U+200E (C.8)":
    expect PgConnectionError:
      discard saslprep("x\xE2\x80\x8Ey")

suite "SASLprep - bidirectional (RFC 3454 Section 6)":
  test "ARABIC ALEF alone is accepted":
    let alef = "\xD8\xA7" # U+0627
    check saslprep(alef) == alef

  test "RandALCat mixed with LCat is rejected":
    # U+0627 (AL) + Latin 'A' (L) → violation.
    expect PgConnectionError:
      discard saslprep("\xD8\xA7A")

  test "RandALCat must start and end with RandALCat":
    # U+0627 (AL) + '1' (EN, neither AL nor L). Ends with EN → violation.
    expect PgConnectionError:
      discard saslprep("\xD8\xA71")

  test "LCat-only string is accepted":
    check saslprep("abc") == "abc"

  test "RandALCat string bracketed by RandALCat is accepted":
    # U+0627 U+0628 (both AL). First and last are AL.
    let s = "\xD8\xA7\xD8\xA8"
    check saslprep(s) == s

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

suite "SASLprep - empty password handling":
  test "password of only B.1 characters is rejected":
    # Soft hyphen maps to nothing; the result is empty, which PostgreSQL
    # rejects.
    expect PgConnectionError:
      discard saslprep("\xC2\xAD")

  test "password of only ZWSP maps to a single space and is accepted":
    check saslprep("\xE2\x80\x8B") == " "
