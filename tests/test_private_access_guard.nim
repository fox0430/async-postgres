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

proc allPragmaHits(lines: varargs[string]): seq[int] =
  ## Line indices the guard's `{.all.}` scan reports for ``lines``.
  var
    sc: LineScanner
    code: seq[string]
  for line in lines:
    code.add stripLiteralsAndComment(line, sc)
  allPragmaLines(code)

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

  test "an {.all.} import is reported":
    check allPragmaHits("import accessors {.all.}") == @[0]
    check allPragmaHits("from ../pg_errors {.all.} import foldFailures") == @[0]
    check allPragmaHits("import decoding {. all .}") == @[0]
    check allPragmaHits("import decoding {.a_ll.}") == @[0]
    check allPragmaHits("import decoding {.`all`.}") == @[0]
    check allPragmaHits("import core", "import decoding {.all.}") == @[1]

  test "an {.all.} pragma split across lines is reported":
    check allPragmaHits("import decoding {.", "  all", ".}") == @[1]

  test "all outside a pragma item name is not a use":
    check allPragmaHits("import accessors").len == 0
    check allPragmaHits("if all(xs, p): discard").len == 0
    check allPragmaHits("import decoding {.All.}").len == 0
    check allPragmaHits("proc f() {.raises: [], allWarnings.} = discard").len == 0
    check allPragmaHits("proc f() {.tags: [all].} = discard").len == 0
    check allPragmaHits("proc f() {.deprecated: all.} = discard").len == 0
    check allPragmaHits("proc f() {.raises: [].} = all(xs, p)").len == 0

  test "an {.all.} in a comment or literal is not a use":
    check allPragmaHits("# import accessors {.all.}").len == 0
    check allPragmaHits("## `import x {.all.}` is for tests only").len == 0
    check allPragmaHits("let s = \"import x {.all.}\"").len == 0
    check allPragmaHits("#[", "import accessors {.all.}", "]#").len == 0
