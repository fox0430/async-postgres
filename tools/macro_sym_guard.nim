## Keep spellable names out of the locals that library macros and templates
## declare in the caller's proc.
##
## Nim names a lifted closure-env field `name & $fieldIndex`, so a library
## local `conn` at index 14 and the caller's `conn1` at index 4 both become the
## C member `conn14` (see `pg_gensym.macroSym`). This tool fails the build on:
##
## - a `genSym` call outside `pg_gensym.nim`; library macros use `macroSym`.
##   A `{.gensym.}` pragma is the same identifier but is not flagged.
## - a `let`/`var`, `except ... as`, or `for` local named by `quote` or template
##   code that expands into an `{.async.}` routine, unless the template is
##   `{.macroSymLocals.}` and the local is not `{.inject.}` (see
##   `plainLocalLines`). A template's own local is ``x`gensymN``, and two of
##   those can meet the same way.
## - tuple unpacking (``let (a, b) = x``, ``(a, b) = x``) in such code. The
##   compiler's temporary for it is a plain `tmpTuple`.
##
## The scan reports "ok" on a clean tree, so nothing but a test can tell a
## working guard from a blind one. `genSymLines` / `tupleUnpackLines` /
## `plainLocalLines` are therefore exported and covered by
## `tests/test_macro_sym_guard.nim`.
##
## Usage:
##   macro_sym_guard   scan the package sources, exit 1 on any violation

import std/[os, strutils, tables]
import source_scan

const
  srcDir = "async_postgres"
  packageRoot = "async_postgres.nim"
  genSymHome = "async_postgres/pg_gensym.nim"

proc addLine(acc: var seq[int], lineNo: int) =
  if acc.len == 0 or acc[^1] != lineNo:
    acc.add lineNo

proc genSymLines*(code: openArray[string]): seq[int] =
  ## Indices of the lines in ``code`` naming `genSym` outside a `{. .}` pragma.
  ## ``code`` is a whole file after `stripLiteralsAndComment`, since a pragma
  ## may span lines.
  var inPragma = false
  for lineNo, line in code:
    var i = 0
    while i < line.len:
      let c = line[i]
      if c == '{' and not inPragma and i + 1 < line.len and line[i + 1] == '.':
        inPragma = true
        i += 2
      elif c == '.' and inPragma and i + 1 < line.len and line[i + 1] == '}':
        inPragma = false
        i += 2
      elif c in identChars:
        let start = i
        while i < line.len and line[i] in identChars:
          inc i
        if not inPragma and sameIdent(line[start ..< i], "genSym"):
          result.addLine lineNo
      else:
        inc i

proc indentOf(line: string): int =
  result = 0
  while result < line.len and line[result] == ' ':
    inc result

proc leadingIdent(s: string, at: int): string =
  var i = at
  while i < s.len and s[i] in identChars:
    inc i
  s[at ..< i]

type HeaderEnd = enum
  heBody ## ends in `=`: the body follows on the next lines
  heInline ## `= expr` on the same line: a one-line template
  heMore ## the header continues on the next line

proc headerEnd(s: string, depth: var int, at = 0): HeaderEnd =
  ## Where a routine header line leaves off, reading from ``at``. ``depth``
  ## carries the bracket nesting across the header's lines: only an `=`
  ## outside every bracket and backtick name starts the body.
  var i = at
  while i < s.len:
    case s[i]
    of '`':
      let close = s.find('`', i + 1)
      if close < 0:
        break
      i = close
    of '(', '[', '{':
      inc depth
    of ')', ']', '}':
      dec depth
    of '=':
      if depth == 0:
        return if s[i + 1 ..^ 1].strip().len == 0: heBody else: heInline
    else:
      discard
    inc i
  heMore

proc opensQuote(code: string): bool =
  ## Whether ``code`` starts a `quote:` / `quote do:` / ``quote("@") do:``.
  let s = code.strip()
  var at = s.find("quote")
  while at >= 0:
    if isIdent(s, at, "quote".len):
      var rest = s[at + "quote".len ..^ 1].strip()
      if rest.startsWith("("):
        let close = rest.find(')')
        if close < 0:
          return false
        rest = rest[close + 1 ..^ 1].strip()
      if rest.startsWith("do") and rest.leadingIdent(0) == "do":
        rest = rest[2 ..^ 1].strip()
      if rest == ":":
        return true
    at = s.find("quote", at + 1)
  false

proc unpacksTuple(code: string): bool =
  ## Whether ``code`` is a ``let/var/const (a, b) = x`` header, a
  ## ``(a, b) = x`` line of a section, or a tuple assignment.
  var s = code.strip()
  let kw = s.leadingIdent(0)
  if kw in ["let", "var", "const"]:
    s = s[kw.len ..^ 1].strip()
    return s.startsWith("(")
  if not s.startsWith("("):
    return false
  var depth = 0
  for i, c in s:
    case c
    of '(', '[', '{':
      inc depth
    of ')', ']', '}':
      dec depth
      if depth == 0:
        let rest = s[i + 1 ..^ 1].strip()
        return rest.startsWith("=") and not rest.startsWith("==")
    else:
      discard
  false

type
  SourceFile* = object
    path*: string
    raw*: seq[string] ## As written: `bindSym"name"` sits in a literal.
    code*: seq[string] ## After `stripLiteralsAndComment`.

  ScopeKind = enum
    skRoutine ## proc, func, iterator, method or converter, named or not
    skTemplate
    skMacro
    skQuote

  Scope = object
    kind: ScopeKind
    indent: int
    parent: int ## -1 at module level
    file, line: int
    name: string ## A declaration's name; empty for `quote` and `proc` values.
    exported: bool
    header: seq[string] ## The header's identifiers: params, types, pragmas.

  PlainLocal* = tuple[file, line: int, name: string]

  Expansions* = object
    scopes: seq[Scope]
    lineScopes: seq[seq[int]] ## Per file, the innermost scope of each line.
    isAsync: seq[bool]

const routineWords = ["proc", "func", "iterator", "method", "converter"]

proc identsFrom(s: string, at = 0): seq[string] =
  var i = at
  while i < s.len:
    if s[i] in identChars:
      let start = i
      while i < s.len and s[i] in identChars:
        inc i
      if s[start] notin Digits:
        result.add s[start ..< i]
    else:
      inc i

proc identAt(s, word: string, start = 0): int =
  ## Position of the whole identifier ``word`` in ``s`` from ``start``, or -1.
  var at = s.find(word, start)
  while at >= 0:
    if isIdent(s, at, word.len):
      return at
    at = s.find(word, at + 1)
  -1

proc nameToken(s: string): string =
  ## The leading declared name of ``s``: an identifier, or a backtick-quoted
  ## one kept with its backticks.
  let t = s.strip()
  if t.startsWith("`"):
    let close = t.find('`', 1)
    return
      if close < 0:
        t
      else:
        t[0 .. close]
  t.leadingIdent(0)

proc declNames(rest: string): seq[string] =
  ## Names declared by ``rest``, the text after `let` / `var` or of a
  ## section entry: ``a, b: T = x``, ``a {.used.} = x`` or ``(a, b) = x``.
  let t = rest.strip()
  if t.startsWith("("):
    let close = t.find(')')
    for part in t[1 ..< (if close < 0: t.len else: close)].split(','):
      result.add nameToken(part)
    return
  var stop = t.len
  for c in [':', '=', '{']:
    let at = t.find(c)
    if at >= 0 and at < stop:
      stop = at
  for part in t[0 ..< stop].split(','):
    result.add nameToken(part)

proc scanScopes(f: SourceFile, fileIdx: int, scopes: var seq[Scope]): seq[int] =
  ## The innermost scope around each line of ``f``, -1 at module level. A
  ## header's lines belong to the scope around it.
  result = newSeq[int](f.code.len)
  var
    stack: seq[int]
    pending = -1 # a header that continues past its first line
    depth = 0
  for lineNo, line in f.code:
    let s = line.strip()
    var id, at: int
    if pending >= 0:
      result[lineNo] = scopes[pending].parent
      id = pending
      at = 0
    else:
      let indent = indentOf(line)
      if s.len > 0:
        while stack.len > 0 and indent <= scopes[stack[^1]].indent:
          discard stack.pop()
      let outer =
        if stack.len > 0:
          stack[^1]
        else:
          -1
      result[lineNo] = outer
      if s.len == 0:
        continue
      var scope = Scope(indent: indent, parent: outer, file: fileIdx, line: lineNo)
      let kw = s.leadingIdent(0)
      at = -1
      if kw in routineWords or kw in ["template", "macro"]:
        scope.kind =
          case kw
          of "template": skTemplate
          of "macro": skMacro
          else: skRoutine
        let rest = s[kw.len ..^ 1].strip()
        let name = nameToken(rest)
        scope.name = name.strip(chars = {'`'})
        scope.exported = rest[name.len ..^ 1].strip().startsWith("*")
        at = 0
      else:
        for word in routineWords:
          at = identAt(s, word)
          if at >= 0:
            scope.kind = skRoutine
            break
      if at < 0:
        if opensQuote(line):
          scope.kind = skQuote
          scopes.add scope
          stack.add scopes.high
        continue
      scopes.add scope
      id = scopes.high
      depth = 0
    scopes[id].header.add identsFrom(s, at)
    case headerEnd(s, depth, at)
    of heBody:
      stack.add id
      pending = -1
    of heInline:
      pending = -1
    of heMore:
      # Brackets closed and no `=`: a forward declaration or a proc type.
      pending = if depth > 0: id else: -1

proc bindSymNames(raw: string): seq[string] =
  var at = raw.find("bindSym\"")
  while at >= 0:
    let start = at + "bindSym\"".len
    let close = raw.find('"', start)
    if close < 0:
      break
    result.add raw[start ..< close]
    at = raw.find("bindSym\"", close)

proc lineDecls(s: string, section: bool): seq[string] =
  ## Names a statement line declares: `let`/`var` (also inside parentheses),
  ## `except ... as x`, a `for` loop's variables, or an entry of a `let`/`var`
  ## section when ``section``.
  if section:
    return declNames(s)
  let kw = s.leadingIdent(0)
  if kw in ["let", "var"]:
    result.add declNames(s[kw.len ..^ 1])
  elif kw == "except":
    let at = identAt(s, "as")
    if at >= 0:
      result.add nameToken(s[at + 2 ..^ 1])
  elif kw == "for":
    let at = identAt(s, "in", 3)
    if at >= 0:
      result.add declNames(s[3 ..< at])
  var p = s.find('(')
  while p >= 0:
    let inner = s[p + 1 ..^ 1].strip(trailing = false)
    let w = inner.leadingIdent(0)
    # `compiles((var r: Row; ...))` only type-checks; nothing is declared.
    if w in ["let", "var"] and inner.len > w.len and inner[w.len] == ' ' and
        not s[0 ..< p].strip().endsWith("compiles("):
      result.add declNames(inner[w.len ..^ 1])
    p = s.find('(', p + 1)

proc injected(s, name: string): bool =
  ## Whether ``s`` declares ``name`` under an `{.inject.}` pragma.
  var at = identAt(s, name)
  while at >= 0:
    let rest = s[at + name.len ..^ 1].strip(trailing = false)
    if rest.startsWith("{."):
      let close = rest.find(".}")
      let pragma =
        if close < 0:
          rest
        else:
          rest[0 ..< close]
      if identAt(pragma, "inject") >= 0:
        return true
    at = identAt(s, name, at + 1)
  false

proc within(scopes: seq[Scope], site, outer: int): bool =
  ## Whether scope ``site`` is ``outer`` or nested in it.
  var up = site
  while up >= 0:
    if up == outer:
      return true
    up = scopes[up].parent
  false

proc findExpansions*(files: openArray[SourceFile]): Expansions =
  ## The scopes of ``files`` and which of them expand into an `{.async.}`
  ## routine, where every local is a closure env field.
  ##
  ## A `quote` may land in any routine. A template expands where it is called:
  ## it counts as async when a call sits in an async routine, a `quote` or
  ## such a template, when a macro names it in `bindSym"..."`, when its own
  ## body awaits, and when no call is found (it is someone else's to call).
  ## Code in a routine that the template or `quote` declares belongs to that
  ## routine, async only if its header names `async` or `closure`. A call is
  ## a matching identifier: the same file's templates, or any file's exported
  ## ones. The name in a declaration's header and a call inside the
  ## template's own body name another overload, so neither counts, nor does
  ## an `import` / `export`.
  var
    scopes: seq[Scope]
    lineScopes = newSeq[seq[int]](files.len)
  for i, f in files:
    lineScopes[i] = scanScopes(f, i, scopes)
  var
    byName: Table[string, seq[int]]
    declaredAt: Table[(int, int), string]
  for id, sc in scopes:
    if sc.kind == skTemplate:
      byName.mgetOrPut(nimIdentNormalize(sc.name), @[]).add id
    if sc.name.len > 0:
      declaredAt[(sc.file, sc.line)] = nimIdentNormalize(sc.name)
  var
    isAsync = newSeq[bool](scopes.len)
    called = newSeq[bool](scopes.len)
    edges: seq[tuple[callee, site: int]]
  for id, sc in scopes:
    isAsync[id] =
      sc.kind == skQuote or
      (sc.kind == skRoutine and ("async" in sc.header or "closure" in sc.header))
  for fi, f in files:
    var importIndent = -1
    for lineNo, line in f.code:
      let s = line.strip()
      if s.len == 0:
        continue
      let indent = indentOf(line)
      if importIndent >= 0:
        if indent > importIndent:
          continue
        importIndent = -1
      if s.leadingIdent(0) in ["import", "export", "from", "include"]:
        importIndent = indent
        continue
      let site = lineScopes[fi][lineNo]
      let declared = declaredAt.getOrDefault((fi, lineNo))
      var targets: seq[int]
      for word in identsFrom(line):
        if word == "await" and site >= 0 and scopes[site].kind == skTemplate:
          isAsync[site] = true
        let key = nimIdentNormalize(word)
        if key == declared:
          continue
        for t in byName.getOrDefault(key):
          let sc = scopes[t]
          if (sc.file == fi or sc.exported) and not scopes.within(site, t):
            targets.add t
      for t in targets:
        called[t] = true
        edges.add (t, site)
      for name in bindSymNames(f.raw[lineNo]):
        for t in byName.getOrDefault(nimIdentNormalize(name)):
          if scopes[t].file == fi or scopes[t].exported:
            called[t] = true
            isAsync[t] = true
  for id, sc in scopes:
    if sc.kind == skTemplate and not called[id]:
      isAsync[id] = true
  var changed = true
  while changed:
    changed = false
    for (callee, site) in edges:
      if not isAsync[callee] and site >= 0 and isAsync[site]:
        isAsync[callee] = true
        changed = true
  Expansions(scopes: scopes, lineScopes: lineScopes, isAsync: isAsync)

proc expandsIntoAsync(ex: Expansions, site: int, params: var seq[string]): bool =
  ## Whether code in scope ``site`` is template or `quote` code that expands
  ## into an async routine; a hand-written routine names its own locals.
  ## Adds the enclosing templates' header identifiers to ``params``.
  if site < 0 or not ex.isAsync[site]:
    return false
  var up = site
  while up >= 0:
    case ex.scopes[up].kind
    of skTemplate:
      result = true
      params.add ex.scopes[up].header
    of skQuote:
      result = true
    else:
      discard
    up = ex.scopes[up].parent

proc renamesLocals(ex: Expansions, site: int): bool =
  ## Whether ``site`` is a `{.macroSymLocals.}` template's own code, whose
  ## locals the pragma names; a routine or `quote` in it names its own.
  if site < 0 or ex.scopes[site].kind != skTemplate:
    return false
  for word in ex.scopes[site].header:
    if sameIdent(word, "macroSymLocals"):
      return true

proc tupleUnpackLines*(
    files: openArray[SourceFile], ex: Expansions
): seq[tuple[file, line: int]] =
  ## Lines that unpack a tuple in template or `quote` code that expands into
  ## an `{.async.}` routine (see `findExpansions`).
  for fi, f in files:
    for lineNo, line in f.code:
      var params: seq[string]
      if unpacksTuple(line) and ex.expandsIntoAsync(ex.lineScopes[fi][lineNo], params):
        result.add (fi, lineNo)

proc plainLocalLines*(files: openArray[SourceFile], ex: Expansions): seq[PlainLocal] =
  ## Locals declared under a name of their own (not a template parameter, not
  ## spliced into a `quote`) in template or `quote` code that expands into an
  ## `{.async.}` routine (see `findExpansions`). A `{.macroSymLocals.}`
  ## template's own locals are renamed, except `{.inject.}` ones.
  for fi, f in files:
    var
      sectionIndent = -1
      entryIndent = -1
    for lineNo, line in f.code:
      let s = line.strip()
      if s.len == 0:
        continue
      let indent = indentOf(line)
      var inSection = false
      if sectionIndent >= 0:
        if indent <= sectionIndent:
          sectionIndent = -1
        elif entryIndent < 0 or indent == entryIndent:
          entryIndent = indent
          inSection = true
        else:
          continue
      if not inSection and s in ["let", "var"]:
        sectionIndent = indent
        entryIndent = -1
        continue
      let site = ex.lineScopes[fi][lineNo]
      var params: seq[string]
      if not ex.expandsIntoAsync(site, params):
        continue
      let renamed = ex.renamesLocals(site)
      for name in lineDecls(s, inSection):
        if name.len == 0 or name == "_" or name.startsWith("`") or
            (renamed and not injected(s, name)):
          continue
        var isParam = false
        for p in params:
          if sameIdent(p, name):
            isParam = true
            break
        if not isParam:
          result.add (fi, lineNo, name)

proc main() {.used.} =
  var files = @[packageRoot]
  for path in walkDirRec(srcDir):
    if path.endsWith(".nim"):
      files.add path.replace('\\', '/')
  var
    genSyms, tuples, locals: seq[string]
    sources: seq[SourceFile]
  for path in files:
    let lines = readFile(path).splitLines()
    var
      sc: LineScanner
      code = newSeq[string](lines.len)
    for lineNo, line in lines:
      code[lineNo] = stripLiteralsAndComment(line, sc)
    if path != genSymHome:
      for lineNo in genSymLines(code):
        genSyms.add(path & ":" & $(lineNo + 1) & ": " & lines[lineNo].strip())
    sources.add SourceFile(path: path, raw: lines, code: code)
  let ex = findExpansions(sources)
  for (fi, lineNo) in tupleUnpackLines(sources, ex):
    let f = sources[fi]
    tuples.add(f.path & ":" & $(lineNo + 1) & ": " & f.raw[lineNo].strip())
  for (fi, lineNo, name) in plainLocalLines(sources, ex):
    let f = sources[fi]
    locals.add(
      f.path & ":" & $(lineNo + 1) & ": `" & name & "`: " & f.raw[lineNo].strip()
    )
  if genSyms.len > 0:
    stderr.writeLine "genSym in library sources:"
    for v in genSyms:
      stderr.writeLine "  " & v
    stderr.writeLine(
      "Use pg_gensym.macroSym: a plain genSym name can meet a caller's local " &
        "in the closure env."
    )
  if tuples.len > 0:
    if genSyms.len > 0:
      stderr.writeLine ""
    stderr.writeLine "Tuple unpacking in template or quote code that expands into an async routine:"
    for v in tuples:
      stderr.writeLine "  " & v
    stderr.writeLine(
      "Bind the tuple to a macroSym let and read its fields: the compiler " &
        "names the unpacking temporary a plain `tmpTuple`."
    )
  if locals.len > 0:
    if genSyms.len > 0 or tuples.len > 0:
      stderr.writeLine ""
    stderr.writeLine "A local named by template or quote code that expands into an async routine:"
    for v in locals:
      stderr.writeLine "  " & v
    stderr.writeLine(
      "Mark the template {.macroSymLocals.} and drop the local's {.inject.}, or " &
        "splice a macroSym into the quote."
    )
  if genSyms.len > 0 or tuples.len > 0 or locals.len > 0:
    quit(1)
  echo "macro_sym_guard: ok"

when isMainModule:
  main()
