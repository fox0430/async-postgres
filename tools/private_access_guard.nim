## Keep `std/importutils` out of the library.
##
## `privateAccess` opens every private field of a type to the calling module.
## Library modules reach `PgConnection` internals through the accessors in
## `pg_connection/types.nim` instead; `privateAccess` is reserved for tests.
## This tool fails the build on an `importutils` / `privateAccess` identifier
## (Nim identifier equality: the first character is case-sensitive, the rest
## ignores case and underscores) outside comments and literals.
##
## The scan reports "ok" on a clean tree, so nothing but a test can tell a
## working guard from a blind one. `sameIdent` / `usesBanned` are therefore
## exported and covered by `tests/test_private_access_guard.nim`.
##
## Usage:
##   private_access_guard   scan the package sources, exit 1 on any use

import std/[os, strutils]
import source_scan

const
  srcDir = "async_postgres"
  packageRoot = "async_postgres.nim"
  banned = ["importutils", "privateAccess"]

proc sameIdent*(a, b: string): bool =
  ## Nim identifier equality: the first character keeps its case, the rest
  ## ignores case and underscores.
  if a.len == 0 or b.len == 0:
    return a.len == b.len
  a[0] == b[0] and cmpIgnoreStyle(a[1 ..^ 1], b[1 ..^ 1]) == 0

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

proc main() =
  var files = @[packageRoot]
  for path in walkDirRec(srcDir):
    if path.endsWith(".nim"):
      files.add path.replace('\\', '/')
  var violations: seq[string]
  for path in files:
    var sc: LineScanner
    for lineNo, line in pairs(readFile(path).splitLines()):
      if usesBanned(stripLiteralsAndComment(line, sc)):
        violations.add(path & ":" & $(lineNo + 1) & ": " & line.strip())
  if violations.len > 0:
    stderr.writeLine "privateAccess in library sources:"
    for v in violations:
      stderr.writeLine "  " & v
    stderr.writeLine ""
    stderr.writeLine(
      "Add an internal accessor in pg_connection/types.nim instead; " &
        "privateAccess is for tests only."
    )
    quit(1)
  echo "private_access_guard: ok"

when isMainModule:
  main()
