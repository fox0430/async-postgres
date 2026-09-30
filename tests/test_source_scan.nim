## Dedicated unit tests for ``tools/source_scan`` — the line scanner shared by
## ``parse_guard`` and ``private_access_guard``. Covers the literal / comment
## state that spans lines, so a guard's "ok" cannot silently mean "blinded".

import std/[strutils, unittest]

import ../tools/source_scan

proc scanLines(lines: varargs[string]): string =
  ## Strip each line in order, sharing one scanner (as the guards do per file).
  var sc: LineScanner
  for line in lines:
    result = stripLiteralsAndComment(line, sc)

suite "source_scan":
  test "drops a line comment and blanks string literals":
    var sc: LineScanner
    check stripLiteralsAndComment("let x = 1 # importutils", sc).find("importutils") < 0
    check stripLiteralsAndComment("let s = \"privateAccess\"", sc).find("privateAccess") <
      0
    check stripLiteralsAndComment("let x = 1", sc).find("let x") >= 0

  test "keeps a hash inside a char literal from starting a comment":
    let code = scanLines("let c = '#'; let p = parseInt(t)")
    check code.find("parseInt") >= 0

  test "does not treat a numeric type suffix as a char literal":
    check scanLines("let m = 0xFF'u8; let p = parseInt(t)").find("parseInt") >= 0
    check scanLines("let n = 1'i32; let p = parseInt(t)").find("parseInt") >= 0

  test "block comment spans lines and nests":
    var sc: LineScanner
    discard stripLiteralsAndComment("#[ outer #[ inner", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") < 0
    discard stripLiteralsAndComment("]# back to depth one", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") < 0
    discard stripLiteralsAndComment("]#", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") >= 0

  test "doc block comment spans lines and nests on ##[":
    var sc: LineScanner
    discard stripLiteralsAndComment("##[ outer", sc)
    discard stripLiteralsAndComment("##[ inner", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") < 0
    discard stripLiteralsAndComment("]## still doc", sc)
    # `#[` does not close a doc block.
    discard stripLiteralsAndComment("#[ still doc", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") < 0
    discard stripLiteralsAndComment("]##", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") >= 0

  test "a quote in a doc block body does not blind the file":
    check scanLines("##[ doc", "\" quote ]##", "importutils").find("importutils") >= 0

  test "triple quoted string spans lines":
    var sc: LineScanner
    discard stripLiteralsAndComment("let s = \"\"\"", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") < 0
    check stripLiteralsAndComment("importutils \"\"\"", sc).find("importutils") < 0
    check stripLiteralsAndComment("importutils", sc).find("importutils") >= 0

  test "raw string prefix keeps a backslash from escaping the closing quote":
    var sc: LineScanner
    check stripLiteralsAndComment("let s = r\"a\\\" # importutils", sc).find(
      "importutils"
    ) < 0
    check stripLiteralsAndComment("importutils", sc).find("importutils") >= 0

  test "a trailing backslash cannot blind the next line":
    var sc: LineScanner
    discard stripLiteralsAndComment("let s = \"abc\\", sc)
    check stripLiteralsAndComment("importutils", sc).find("importutils") >= 0
