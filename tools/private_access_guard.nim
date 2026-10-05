## Keep private-symbol access (`std/importutils`, `import x {.all.}`) out of
## the library.
##
## `privateAccess` opens every private field of a type to the calling module,
## and `import x {.all.}` opens every private symbol of `x`. Library modules
## reach `PgConnection` internals through the accessors in
## `pg_connection/types.nim` and sibling helpers through a `*` export that the
## hub keeps out of its own `export`; both escape hatches are reserved for
## tests. This tool fails the build on an `importutils` / `privateAccess`
## identifier, or on an `all` item of a `{. .}` pragma (Nim identifier
## equality: the first character is case-sensitive, the rest ignores case and
## underscores), outside comments and literals.
##
## The scan reports "ok" on a clean tree, so nothing but a test can tell a
## working guard from a blind one. `usesBanned` / `allPragmaLines` are
## therefore exported and covered, with `source_scan.sameIdent`, by
## `tests/test_private_access_guard.nim`.
##
## Usage:
##   private_access_guard   scan the package sources, exit 1 on any use

import std/[os, strutils]
import source_scan

const
  srcDir = "async_postgres"
  packageRoot = "async_postgres.nim"
  banned = ["importutils", "privateAccess"]

proc usesBanned*(code: string): bool =
  var i = 0
  while i < code.len:
    if code[i] notin identChars:
      inc i
      continue
    let start = i
    while i < code.len and code[i] in identChars:
      inc i
    let ident = code[start ..< i]
    for b in banned:
      if sameIdent(ident, b):
        return true
  false

proc allPragmaLines*(code: openArray[string]): seq[int] =
  ## Indices of the lines in ``code`` holding an `all` pragma item
  ## (``import x {.all.}``, ``from x {. all .} import y``). ``code`` is a whole
  ## file after `stripLiteralsAndComment`, since a pragma may span lines.
  var
    inPragma = false
    depth = 0 ## `(` / `[` / `{` nesting inside the pragma.
    atItem = false ## The next identifier names a pragma item.
  for lineNo, line in code:
    var i = 0
    while i < line.len:
      let c = line[i]
      if not inPragma:
        if c == '{' and i + 1 < line.len and line[i + 1] == '.':
          inPragma = true
          depth = 0
          atItem = true
          i += 2
        else:
          inc i
        continue
      if depth == 0 and c == '.' and i + 1 < line.len and line[i + 1] == '}':
        inPragma = false
        i += 2
        continue
      case c
      of identChars:
        let start = i
        while i < line.len and line[i] in identChars:
          inc i
        if atItem and depth == 0 and sameIdent(line[start ..< i], "all"):
          if result.len == 0 or result[^1] != lineNo:
            result.add lineNo
        atItem = false
        continue
      of '(', '[', '{':
        inc depth
        atItem = false
      of ')', ']', '}':
        dec depth
      of ',':
        atItem = depth == 0
      of ' ', '\t', '`':
        # Whitespace and a stropped name (`{.`all`.}`) keep the item open.
        discard
      else:
        atItem = false
      inc i

proc main() {.used.} =
  var files = @[packageRoot]
  for path in walkDirRec(srcDir):
    if path.endsWith(".nim"):
      files.add path.replace('\\', '/')
  var violations: seq[string]
  for path in files:
    let lines = readFile(path).splitLines()
    var
      sc: LineScanner
      code = newSeq[string](lines.len)
      hit = newSeq[bool](lines.len)
    for lineNo, line in lines:
      code[lineNo] = stripLiteralsAndComment(line, sc)
      hit[lineNo] = usesBanned(code[lineNo])
    for lineNo in allPragmaLines(code):
      hit[lineNo] = true
    for lineNo, line in lines:
      if hit[lineNo]:
        violations.add(path & ":" & $(lineNo + 1) & ": " & line.strip())
  if violations.len > 0:
    stderr.writeLine "privateAccess / {.all.} import in library sources:"
    for v in violations:
      stderr.writeLine "  " & v
    stderr.writeLine ""
    stderr.writeLine(
      "Add an internal accessor in pg_connection/types.nim, or export the " &
        "helper with `*` and keep it out of the hub's `export`; privateAccess " &
        "and {.all.} imports are for tests only."
    )
    quit(1)
  echo "private_access_guard: ok"

when isMainModule:
  main()
