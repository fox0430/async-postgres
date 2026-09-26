## Dedicated unit tests for the detection half of
## ``tools/private_access_guard``. A clean tree prints ``ok`` whether the scan
## works or is blind, so only these tests can tell the two apart. They pin both
## directions: a real use has to be reported, and a comment, a literal or a
## neighbouring identifier has to stay quiet. ``test_source_scan`` covers the
## scanner the guard shares with ``parse_guard``; this file covers what the
## guard asks of its output.

import std/unittest

import ../tools/private_access_guard
import ../tools/source_scan

proc detects(lines: varargs[string]): bool =
  ## True when the guard's scan of ``lines`` reports a banned identifier.
  var sc: LineScanner
  for line in lines:
    if usesBanned(stripLiteralsAndComment(line, sc)):
      return true
  false

suite "private_access_guard":
  test "sameIdent is Nim's rule: first letter case-sensitive, rest is not":
    check sameIdent("privateAccess", "privateAccess")
    check sameIdent("privateAccess", "private_access")
    check sameIdent("privateAccess", "privateACCESS")
    check sameIdent("importutils", "importUtils")
    check sameIdent("importutils", "import_utils")
    check not sameIdent("privateAccess", "PrivateAccess")
    check not sameIdent("privateAccess", "PRIVATEACCESS")
    check not sameIdent("importutils", "Importutils")
    check not sameIdent("importutils", "importutils2")

  test "a real use is reported":
    check detects("import std/importutils")
    check detects("import std/[os, importutils]")
    check detects("privateAccess(PgConnection)")
    check detects("  {.push privateAccess.}")

  test "a neighbouring identifier is not a use":
    check not detects("let x = myPrivateAccess")
    check not detects("proc privateAccessor(): int = 1")
    check not detects("let import_utils2 = 1")

  test "comments and literals are not uses":
    check not detects("## privateAccess is for tests only")
    check not detects("let msg = \"privateAccess\"")
    check not detects("let s = r\"privateAccess\"")
    check not detects("let c = '#'")
    check not detects("#[", "privateAccess(PgConnection)", "]#")
    check not detects("##[", "privateAccess", "]##")
    check not detects("let s = \"\"\"", "privateAccess", "\"\"\"")

  test "a line comment does not blind the line after it":
    check detects("# importutils is banned", "privateAccess(PgConnection)")
