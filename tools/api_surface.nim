## Snapshot of every exported identifier under ``async_postgres/``.
##
## Nim spells "public" per module: a `*` is needed for any cross-module use,
## including use by a sibling implementation module. So the exported set of a
## submodule is wider than the API the package promises, and the hub modules
## (`pg_connection.nim`, `pg_client.nim`, ...) re-export a hand-picked subset.
## That split means a new `*` silently widens what a user can reach by
## importing the submodule directly, and a new `export` line widens the API the
## package itself promises. This tool freezes both into
## `tests/api_surface.golden` so widening either takes a reviewed golden update.
##
## Overloads share a name, so repeats of one name in a file are numbered
## (`name#2`, ...) — otherwise privatizing one overload of two would leave the
## snapshot unchanged.
##
## Usage:
##   api_surface              print the surface
##   api_surface check FILE   diff against FILE, exit 1 on drift
##   api_surface write FILE   regenerate FILE

import std/[algorithm, os, sequtils, sets, strformat, strutils, tables]

const
  declKeywords = [
    "proc", "func", "template", "macro", "iterator", "converter", "method", "type",
    "const", "let", "var",
  ] ## Leading words a declaration may open with, stripped before reading the name.
  srcDir = "async_postgres"

proc exportedName(line: string): string =
  ## The identifier a line exports, or "" when it exports nothing. Covers
  ## routines, and the `Name*` entries of `type` / `const` / `var` / `let`
  ## blocks and of object field lists — a privatized field is exactly the kind
  ## of change this snapshot exists to catch.
  let stripped = line.strip()
  if stripped.len == 0 or stripped.startsWith('#'):
    return ""

  var rest = stripped
  for kw in declKeywords:
    if stripped.startsWith(kw & " "):
      rest = stripped[kw.len + 1 ..^ 1].strip()
      break

  var i = 0
  var name = ""
  if i < rest.len and rest[i] == '`':
    # Backtick-quoted operator or setter, e.g. `` `notifyMaxQueue=` ``.
    let close = rest.find('`', i + 1)
    if close < 0:
      return ""
    name = rest[i .. close]
    i = close + 1
  else:
    while i < rest.len and (rest[i].isAlphaNumeric or rest[i] == '_'):
      inc i
    name = rest[0 ..< i]
  if name.len == 0 or i >= rest.len or rest[i] != '*':
    return ""

  # `a*b` is multiplication, not an export marker; an export marker is followed
  # by the declaration's own punctuation (or a pragma).
  let after = rest[i + 1 ..^ 1].strip()
  if after.len == 0:
    return ""
  if after[0] notin {'(', '[', ':', '=', '{', ','}:
    return ""
  name

proc reexportedNames(line: string): seq[string] =
  ## The symbols an `export` statement re-exports, as `export <sym>` entries.
  ## The hub modules promise the package's public API through these lines, and
  ## no `*` appears on them, so `exportedName` cannot see them at all.
  ##
  ## Only the one-line list form is parsed. A continuation line (`export a,` /
  ## `  b`) would hide the rest of the list from a line parser, so it aborts
  ## the run rather than snapshot half the list; `export a except b` is kept
  ## as one entry so a change to either side drifts the golden.
  var stripped = line.strip()
  if not stripped.startsWith("export "):
    return
  let hash = stripped.find('#')
  if hash >= 0:
    stripped = stripped[0 ..< hash].strip()
  let body = stripped["export ".len ..^ 1].strip()
  if body.len == 0 or body.endsWith(','):
    quit(&"api_surface: unsupported multi-line export statement: '{stripped}'", 1)
  if " except " in body:
    result.add "export " & body.splitWhitespace().join(" ")
    return
  for part in body.split(','):
    let sym = part.strip()
    if sym.len == 0 or ' ' in sym:
      quit(&"api_surface: unsupported export statement: '{stripped}'", 1)
    result.add "export " & sym

proc collect(): seq[string] =
  var files: seq[string]
  for path in walkDirRec(srcDir):
    if path.endsWith(".nim"):
      files.add path
  sort files
  for path in files:
    let rel = path.replace('\\', '/')
    var occurrences: CountTable[string]
    for line in lines(path):
      var names = reexportedNames(line)
      let name = exportedName(line)
      if name.len > 0:
        names.add name
      for n in names:
        occurrences.inc n
        let k = occurrences[n]
        let label =
          if k == 1:
            n
          else:
            &"{n}#{k}"
        result.add &"{rel}\t{label}"
  sort result

when isMainModule:
  let surface = collect()
  let mode =
    if paramCount() >= 1:
      paramStr(1)
    else:
      "print"
  case mode
  of "print":
    for entry in surface:
      echo entry
  of "write":
    writeFile(paramStr(2), surface.join("\n") & "\n")
    echo &"wrote {surface.len} entries to {paramStr(2)}"
  of "check":
    let goldenPath = paramStr(2)
    if not fileExists(goldenPath):
      quit(&"missing golden file {goldenPath}; run `nimble apiSurfaceWrite`", 1)
    let golden = readFile(goldenPath).strip().splitLines()
    let current = surface.toHashSet()
    let expected = golden.toHashSet()
    let added = sorted(toSeq(current - expected))
    let removed = sorted(toSeq(expected - current))
    if added.len == 0 and removed.len == 0:
      echo &"API surface unchanged ({surface.len} exported identifiers)"
    else:
      for entry in removed:
        echo "- " & entry
      for entry in added:
        echo "+ " & entry
      quit(
        "\nThe exported surface drifted from " & goldenPath &
          ".\nIf the change is intended, review it and run `nimble apiSurfaceWrite`.",
        1,
      )
  else:
    quit("usage: api_surface [print|check FILE|write FILE]", 2)
