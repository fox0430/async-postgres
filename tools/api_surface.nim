## Snapshot of every exported identifier under ``async_postgres/``, split into
## the surface the package promises and the wider sibling-export set.
##
## Nim needs `*` for any cross-module use, so a submodule's exports are wider
## than the promised API. Hubs (`async_postgres.nim`, `pg_connection.nim`,
## `pg_client.nim`, `pg_types.nim`) re-export a subset (or a module minus
## `except`). A new hub `export` widens the promised API; a new `*` widens
## what a direct submodule import can reach (unsupported).
##
## Frozen in:
## - `tests/api_surface.public.golden` — hub `export`s, hub-file `*`, and `*`
##   reached by walking those exports (wholesale or `module.name`, minus `except`)
## - `tests/api_surface.internal.golden` — every other `*` / non-hub `export`
##
## Overloads share a name, so repeats in one file are numbered (`name#2`).
## Enum values are listed as `Enum.value` (`except Enum` hides the type, not
## the values). Object fields are `Type.field` so a named hub export of the
## type carries `*` fields into the promised surface.
##
## Blind spot: this reads source lines, not the compiler table, so
## template/macro expansions are invisible. Live public cases in neither
## golden: name-based row accessors (`row.getInt("col")`) and index-based
## array accessors (`row.getIntArray(0)`). `tests/test_aggregate.nim` probes
## both.
##
## `template` / `macro` bodies are skipped: a `proc X*` there is codegen in
## the caller, not this module's export. Wrapped signatures keep the skip
## until the depth-0 `=`.
##
## `Type.field` reachability is approximated: the classifier accepts either
## name, which over-reports only when an object type is named in `except`
## (no current instance).
##
## Usage:
##   api_surface                    print both surfaces
##   api_surface check PUB INT      diff against the two golden files, exit 1 on drift
##   api_surface write PUB INT      regenerate the two golden files

import std/[algorithm, os, sequtils, sets, strformat, strutils, tables]

const
  declKeywords = [
    "proc", "func", "template", "macro", "iterator", "converter", "method", "type",
    "const", "let", "var",
  ] ## Leading words a declaration may open with, stripped before reading the name.
  srcDir = "async_postgres"
  packageRoot = "async_postgres.nim"
  hubFiles = [
    "async_postgres.nim", "async_postgres/pg_connection.nim",
    "async_postgres/pg_client.nim", "async_postgres/pg_types.nim",
  ]

type
  SurfaceKind = enum
    skPublic
    skInternal

  SurfaceEntry = object
    path, label: string
    kind: SurfaceKind

  ExportClause = object
    module: string ## Imported identifier (`types`, `core`, `pg_auth`).
    names: seq[string] ## Empty = wholesale; otherwise `export module.name`.
    exceptNames: HashSet[string]

proc canon(path: string): string =
  result = normalizedPath(path).replace('\\', '/')
  if result.startsWith("./"):
    result = result[2 ..^ 1]

proc exportedName(line: string): string =
  ## Exported identifier on this line, or "". Object-field `*` is included so
  ## privatizing a field drifts the snapshot.
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

proc stripLineComment(line: string): string =
  let hash = line.find('#')
  if hash >= 0:
    line[0 ..< hash]
  else:
    line

proc needsContinuation(text: string, depth: int): bool =
  depth > 0 or text.endsWith(',') or text.endsWith(" except") or
    text in ["import", "export", "from"]

proc collectKeywordStatements(path, keyword: string): seq[string] =
  ## Join wrapped `import` / `export` / `from` into one clause (trailing comma,
  ## unmatched `[`, or `except` with the list on the next lines).
  var buf = ""
  var depth = 0
  for raw in lines(path):
    let stripped = stripLineComment(raw).strip()
    if buf.len == 0:
      if stripped == keyword or stripped.startsWith(keyword & " ") or
          stripped.startsWith(keyword & "[") or stripped.startsWith(keyword & "\t"):
        buf = stripped
        depth = stripped.count('[') - stripped.count(']')
        if not needsContinuation(buf, depth):
          result.add buf
          buf = ""
      continue
    if stripped.len == 0:
      continue
    buf.add " " & stripped
    depth += stripped.count('[') - stripped.count(']')
    if not needsContinuation(buf, depth):
      result.add buf
      buf = ""
  if buf.len > 0:
    quit(&"api_surface: unterminated {keyword} statement in {path}: '{buf}'", 1)

proc parseBracketList(body: string): seq[string] =
  let inner = body.strip()
  if inner.len >= 2 and inner[0] == '[' and inner[^1] == ']':
    for part in inner[1 ..^ 2].split(','):
      let item = part.strip()
      if item.len > 0:
        result.add item
  else:
    result.add inner

proc parseImportMaps(path: string): Table[string, string] =
  ## Last path component of each in-tree import → resolved `.nim` path.
  ## Unresolved in-tree specs are omitted so a later `export` of that name fails.
  let fromDir = parentDir(path)
  var mapped: Table[string, string]
  proc tryResolve(spec: string): string =
    var s = spec.strip()
    if s.len == 0 or s.startsWith("std/") or s == "std" or s.startsWith("pkg/"):
      return ""
    var candidates: seq[string]
    if s.startsWith("./") or s.startsWith("../"):
      candidates.add canon(fromDir / s & ".nim")
    else:
      candidates.add canon(fromDir / s & ".nim")
      candidates.add canon(srcDir / s & ".nim")
    for c in candidates:
      if fileExists(c):
        return c
    ""

  proc addSpec(spec: string) =
    let resolved = tryResolve(spec)
    if resolved.len == 0:
      return
    let ident = spec.split('/')[^1]
    if ident in mapped and mapped[ident] != resolved:
      quit(
        &"api_surface: import identifier '{ident}' is ambiguous in {path} ({mapped[ident]} vs {resolved})",
        1,
      )
    mapped[ident] = resolved

  for stmt in collectKeywordStatements(path, "import"):
    var body = stmt["import".len ..^ 1].strip()
    # `import foo except Bar` is unused here; keep the module side.
    let exceptAt = body.find(" except ")
    if exceptAt >= 0:
      body = body[0 ..< exceptAt].strip()
    if '[' in body:
      let bracket = body.find('[')
      let prefix = body[0 ..< bracket].strip()
      for item in parseBracketList(body[bracket ..^ 1]):
        if prefix.len == 0:
          addSpec(item)
        else:
          addSpec(prefix & item)
    else:
      for part in body.split(','):
        addSpec(part.strip())

  for stmt in collectKeywordStatements(path, "from"):
    # `from pg_types/core import isPgUIntText, ...`
    let rest = stmt["from".len ..^ 1].strip()
    let imp = rest.find(" import ")
    if imp < 0:
      continue
    addSpec(rest[0 ..< imp].strip())
  result = mapped

proc parseExportClauses(path: string): seq[ExportClause] =
  for stmt in collectKeywordStatements(path, "export"):
    var body = stmt["export".len ..^ 1].strip()
    if body.len == 0:
      quit(&"api_surface: empty export statement in {path}", 1)
    if " except " in body:
      let parts = body.split(" except ", 1)
      let moduleName = parts[0].strip()
      if '.' in moduleName or ',' in moduleName:
        quit(&"api_surface: unsupported export-except form in {path}: '{stmt}'", 1)
      var exceptNames: HashSet[string]
      for part in parts[1].split(','):
        let n = part.strip()
        if n.len > 0:
          exceptNames.incl n
      result.add ExportClause(module: moduleName, exceptNames: exceptNames)
      continue
    for part in body.split(','):
      let sym = part.strip()
      if sym.len == 0:
        quit(&"api_surface: empty name in export statement in {path}: '{stmt}'", 1)
      if '.' in sym:
        let bits = sym.split('.', 1)
        result.add ExportClause(module: bits[0], names: @[bits[1]])
      else:
        result.add ExportClause(module: sym)

proc exportSnapshotLabels(path: string): seq[string] =
  ## One-line `export` labels. Repeats are numbered so set comparison cannot
  ## hide a deletion.
  var occurrences: CountTable[string]
  template emit(label: string) =
    let l = label
    occurrences.inc l
    let k = occurrences[l]
    result.add(
      if k == 1:
        l
      else:
        l & "#" & $k
    )

  for stmt in collectKeywordStatements(path, "export"):
    var body = stmt["export".len ..^ 1].strip()
    body = body.splitWhitespace().join(" ")
    if " except " in body:
      emit "export " & body
      continue
    for part in body.split(','):
      let sym = part.strip()
      if sym.len == 0:
        quit(&"api_surface: empty name in export statement in {path}", 1)
      emit "export " & sym

proc sourceFiles(): seq[string] =
  result.add packageRoot
  for path in walkDirRec(srcDir):
    if path.endsWith(".nim"):
      result.add canon(path)
  sort result

proc enumMembers(all: seq[string], declIdx: int): seq[string] =
  ## Enum members in the indented block below `all[declIdx]`.
  let declIndent = all[declIdx].indentation
  for i in declIdx + 1 ..< all.len:
    let body = stripLineComment(all[i])
    if body.strip().len == 0:
      continue
    if body.indentation <= declIndent:
      break
    var name = body.strip()
    for sep in ["=", " "]:
      let at = name.find(sep)
      if at >= 0:
        name = name[0 ..< at].strip()
    if name.len > 0:
      result.add name

proc fieldExportName(line: string): string =
  ## The `*` field on this line, including `case kind*: T` discriminators.
  result = exportedName(line)
  if result.len > 0:
    return
  let stripped = line.strip()
  const casePrefix = "case "
  if stripped.startsWith(casePrefix):
    result = exportedName(stripped[casePrefix.len ..^ 1])

proc opensFieldBlock(body: string): bool =
  ## True when `body` is a type line whose fields occupy the indented block
  ## below (`object` / `ref object` / `tuple`, including `of` parents).
  let eq = body.find('=')
  if eq < 0:
    return false
  var rhs = body[eq + 1 ..^ 1].strip()
  if rhs.startsWith("ref ") or rhs.startsWith("ptr "):
    rhs = rhs[4 ..^ 1].strip()
  if rhs.startsWith("object"):
    let rest = rhs["object".len ..^ 1].strip()
    return rest.len == 0 or rest.startsWith("of ") or rest.startsWith("{.")
  rhs == "tuple"

proc headerHasBodyEquals(text: string): bool =
  ## True when `text` contains a parenthesis-depth-0 `=` that starts a
  ## template/macro body, not a default-value `=` inside the parameter list.
  var depth = 0
  var i = 0
  while i < text.len:
    let c = text[i]
    case c
    of '(':
      inc depth
    of ')':
      if depth > 0:
        dec depth
    of '=':
      if depth == 0:
        if i + 1 < text.len and text[i + 1] in {'=', '>'}:
          inc i
        else:
          return true
    else:
      discard
    inc i
  false

proc collectStarNames(path: string): seq[string] =
  ## Exported `*` names in `path`. Skips `template` / `macro` bodies.
  var occurrences: CountTable[string]
  let all = toSeq(lines(path))
  template emit(label: string) =
    let l = label
    occurrences.inc l
    let k = occurrences[l]
    result.add(
      if k == 1:
        l
      else:
        l & "#" & $k
    )

  var objectType = ""
  var objectIndent = -1
  var codegenIndent = -1
  var codegenAwaitEquals = false
  for i, line in all:
    let body = stripLineComment(line)
    if body.strip().len == 0:
      continue
    if codegenIndent >= 0:
      if line.indentation > codegenIndent:
        continue
      if codegenAwaitEquals:
        if headerHasBodyEquals(body):
          codegenAwaitEquals = false
        if line.indentation < codegenIndent:
          codegenIndent = -1
          codegenAwaitEquals = false
        else:
          continue
      else:
        codegenIndent = -1
    if objectType.len > 0 and line.indentation <= objectIndent:
      objectType = ""
      objectIndent = -1
    if objectType.len > 0:
      let field = fieldExportName(line)
      if field.len > 0:
        emit objectType & "." & field
      continue
    let stripped = body.strip()
    if stripped.startsWith("template ") or stripped.startsWith("macro "):
      let name = exportedName(line)
      if name.len > 0:
        emit name
      codegenIndent = line.indentation
      codegenAwaitEquals = not headerHasBodyEquals(body)
      continue
    let name = exportedName(line)
    if name.len == 0:
      continue
    emit name
    let enumAt = stripped.find("= enum")
    if enumAt >= 0:
      let rest = stripped[enumAt + "= enum".len ..^ 1]
      if rest.len == 0 or rest[0] in {' ', '\t'}:
        if rest.strip().len > 0:
          quit(
            &"api_surface: inline enum body is unsupported in {path}: '{stripped}'", 1
          )
        for member in enumMembers(all, i):
          emit name & "." & member
      continue
    if opensFieldBlock(stripped):
      objectType = name
      objectIndent = line.indentation

proc resolveExportModule(
    path: string,
    imports: Table[string, Table[string, string]],
    files: seq[string],
    moduleName: string,
): string =
  ## In-tree import of `moduleName` from `path`, or "" for a std/pkg re-export
  ## (`export chronos`). An in-tree basename that is not imported is a broken
  ## hub line and aborts.
  if moduleName in imports.getOrDefault(path):
    return imports[path][moduleName]
  var matches: seq[string]
  for f in files:
    if splitFile(f).name == moduleName:
      matches.add f
  if matches.len > 0:
    quit(
      "api_surface: cannot resolve export module '" & moduleName & "' in " & path &
        " (in-tree " & matches.join(", ") & " is not imported here)",
      1,
    )
  ""

proc classify(files: seq[string]): seq[SurfaceEntry] =
  var imports: Table[string, Table[string, string]]
  var stars: Table[string, seq[string]]
  var exportLabels: Table[string, seq[string]]
  var exportClauses: Table[string, seq[ExportClause]]
  for path in files:
    imports[path] = parseImportMaps(path)
    stars[path] = collectStarNames(path)
    exportLabels[path] = exportSnapshotLabels(path)
    exportClauses[path] = parseExportClauses(path)

  var wholesale: Table[string, HashSet[string]]
  var namedPublic: HashSet[(string, string)]
  var seenHub: HashSet[string]

  proc walk(path: string) =
    for clause in exportClauses[path]:
      let resolved = resolveExportModule(path, imports, files, clause.module)
      if resolved.len == 0:
        continue
      if clause.names.len > 0:
        for n in clause.names:
          namedPublic.incl (resolved, n)
        continue
      if resolved in wholesale:
        if wholesale[resolved] != clause.exceptNames:
          quit(&"api_surface: conflicting except lists for {resolved} (via {path})", 1)
        continue
      wholesale[resolved] = clause.exceptNames
      walk(resolved)

  for hub in hubFiles:
    if hub notin seenHub:
      seenHub.incl hub
      walk(hub)

  proc starKind(path, labeled: string): SurfaceKind =
    var base = labeled
    let hash = labeled.find('#')
    if hash >= 0:
      base = labeled[0 ..< hash]
    if path in hubFiles:
      return skPublic
    # `Enum.member` / `Type.field`: public if either side is promised.
    var names = @[base]
    let dot = base.find('.')
    if dot >= 0:
      names = @[base[0 ..< dot], base[dot + 1 ..^ 1]]
    for n in names:
      if path in wholesale and n notin wholesale[path]:
        return skPublic
      if (path, n) in namedPublic:
        return skPublic
    skInternal

  proc exportKind(path: string): SurfaceKind =
    ## Promised on a hub file, or on a file a hub re-exports wholesale.
    if path in hubFiles or path in wholesale: skPublic else: skInternal

  for path in files:
    for name in stars[path]:
      result.add SurfaceEntry(path: path, label: name, kind: starKind(path, name))
    for label in exportLabels[path]:
      result.add SurfaceEntry(path: path, label: label, kind: exportKind(path))

  result.sort do(a, b: SurfaceEntry) -> int:
    result = cmp(a.path, b.path)
    if result == 0:
      result = cmp(a.label, b.label)

proc formatEntry(e: SurfaceEntry): string =
  &"{e.path}\t{e.label}"

proc entriesOf(surface: seq[SurfaceEntry], kind: SurfaceKind): seq[string] =
  for e in surface:
    if e.kind == kind:
      result.add formatEntry(e)

proc writeGolden(path: string, entries: seq[string]) =
  writeFile(path, entries.join("\n") & "\n")
  echo &"wrote {entries.len} entries to {path}"

proc checkGolden(path: string, current: seq[string], title: string): bool =
  ## True when `path` matches `current`; prints a unified-style diff on drift.
  if not fileExists(path):
    echo &"missing golden file {path}; run `nimble apiSurfaceWrite`"
    return false
  let expected = readFile(path).strip().splitLines().toHashSet()
  let have = current.toHashSet()
  let added = sorted(toSeq(have - expected))
  let removed = sorted(toSeq(expected - have))
  if added.len == 0 and removed.len == 0:
    echo &"{title} unchanged ({current.len} identifiers)"
    return true
  echo &"{title} drifted from {path}"
  for entry in removed:
    echo "- " & entry
  for entry in added:
    echo "+ " & entry
  echo ""
  false

when isMainModule:
  let surface = classify(sourceFiles())
  let publicEntries = entriesOf(surface, skPublic)
  let internalEntries = entriesOf(surface, skInternal)
  let mode =
    if paramCount() >= 1:
      paramStr(1)
    else:
      "print"
  case mode
  of "print":
    echo "# public"
    for entry in publicEntries:
      echo entry
    echo "# internal"
    for entry in internalEntries:
      echo entry
  of "write":
    if paramCount() < 3:
      quit("usage: api_surface write PUBLIC.golden INTERNAL.golden", 2)
    writeGolden(paramStr(2), publicEntries)
    writeGolden(paramStr(3), internalEntries)
  of "check":
    if paramCount() < 3:
      quit("usage: api_surface check PUBLIC.golden INTERNAL.golden", 2)
    let pubOk = checkGolden(paramStr(2), publicEntries, "public API surface")
    let intOk = checkGolden(paramStr(3), internalEntries, "internal exported surface")
    if not (pubOk and intOk):
      quit(
        "The exported surface drifted.\n" &
          "Public drift is a semver event; internal drift is sibling-export review.\n" &
          "If the change is intended, review it and run `nimble apiSurfaceWrite`.",
        1,
      )
  else:
    quit("usage: api_surface [print|check PUB INT|write PUB INT]", 2)
