## `genSym` for library macros, whose symbols can land in the caller's
## closure environment. This keeps the library's names from meeting the
## caller's; the caller's own locals can still meet each other.

import std/macros

proc macroSym*(kind: NimSymKind, ident: string): NimNode =
  ## `genSym` with a name no identifier can spell. Nim names a lifted local
  ## `name & $fieldIndex`, so a plain `conn` at index 14 and the caller's
  ## `conn1` at index 4 are both emitted as the C member `conn14`. C codegen
  ## escapes the backtick and appends `_`, which no Nim identifier ends with;
  ## the `pg` suffix leaves the index as the only trailing number.
  ## A routine is never an env field and its name shows in async tracebacks,
  ## so it keeps the plain name. An `{.async.}` proc's params sit in its own
  ## env, apart from its body's locals, so callback params can stay plain.
  ## Templates that declare locals in the caller use `macroSymLocals`.
  if kind in
      {nskProc, nskFunc, nskIterator, nskConverter, nskMethod, nskTemplate, nskMacro}:
    genSym(kind, ident)
  else:
    genSym(kind, ident & "`pg")

type TemplateLocal = tuple[kind: NimSymKind, name: string, info: LineInfo]

proc localSym(kind: NimSymKind, ident: string, info: LineInfo): NimNode =
  ## `macroSym` at the local's declaration, so errors point at the template
  ## rather than at this module.
  result = macroSym(kind, ident)
  result.setLineInfo(info)

func kindName(kind: NimSymKind): string =
  case kind
  of nskVar: "var"
  of nskForVar: "for variable"
  else: "let"

proc addLocal(
    acc: var seq[TemplateLocal], params: openArray[string], kind: NimSymKind, n: NimNode
) =
  var n = n
  if n.kind == nnkPragmaExpr:
    for p in n[1]:
      if p.kind == nnkIdent and p.eqIdent"inject":
        return
    n = n[0]
  if n.kind != nnkIdent:
    error("macroSymLocals: unsupported local declaration", n)
  for p in params:
    if n.eqIdent(p):
      return
  for l in acc:
    if n.eqIdent(l.name):
      # One parameter carries one symbol, and a symbol has one kind.
      if l.kind != kind:
        error(
          "macroSymLocals: '" & l.name & "' is declared as both a " & kindName(l.kind) &
            " and a " & kindName(kind) & "; rename one",
          n,
        )
      return
  acc.add (kind, $n, n.lineInfoObj)

proc routineNames(n: NimNode, acc: var seq[NimNode]) =
  ## The params and locals a nested routine declares, at any depth.
  case n.kind
  of nnkIdentDefs, nnkVarTuple, nnkForStmt:
    for i in 0 ..< n.len - 2:
      let d =
        if n[i].kind == nnkPragmaExpr:
          n[i][0]
        else:
          n[i]
      if d.kind == nnkIdent:
        acc.add d
  of nnkExceptBranch:
    for i in 0 ..< n.len - 1:
      if n[i].kind == nnkInfix:
        acc.add n[i][2]
  else:
    discard
  for child in n:
    routineNames(child, acc)

proc templateLocals(
    n: NimNode,
    params: openArray[string],
    acc: var seq[TemplateLocal],
    nested: var seq[NimNode],
) =
  ## The locals ``n`` declares outside nested routines, whose locals are their
  ## own env's. ``params`` are the caller's names. ``nested`` gets the names
  ## nested routines declare.
  case n.kind
  of RoutineNodes:
    routineNames(n, nested)
    return
  of nnkVarSection, nnkLetSection:
    let kind = if n.kind == nnkVarSection: nskVar else: nskLet
    for d in n:
      for i in 0 ..< d.len - 2:
        acc.addLocal(params, kind, d[i])
  of nnkExceptBranch:
    for i in 0 ..< n.len - 1:
      if n[i].kind == nnkInfix:
        acc.addLocal(params, nskLet, n[i][2])
  of nnkForStmt:
    for i in 0 ..< n.len - 2:
      acc.addLocal(params, nskForVar, n[i])
  else:
    discard
  for child in n:
    templateLocals(child, params, acc, nested)

macro macroSymLocals*(def: untyped): untyped =
  ## Template pragma: give the locals the template declares in its caller
  ## `macroSym` names. The body moves to a private ``<name>Core`` template that
  ## takes those names as parameters, and ``<name>`` becomes a macro with the
  ## same signature and doc that passes them. Parameters (the caller's names),
  ## `{.inject.}` locals and nested routines' own names are left alone.
  ##
  ## A local's name replaces every identifier spelled the same in the body, so
  ## a field (`a.f`, `T(f: x)`), named argument or outer symbol (`var n = n`)
  ## spelled like one fails to compile. Other pragmas, `varargs` params, a
  ## name declared with two kinds, and a nested routine's param or local
  ## sharing a name are rejected here.
  def.expectKind nnkTemplateDef
  let coreName = $def.name & "Core"
  for p in def.pragma:
    error(
      "macroSymLocals: {." & p.repr & ".} would apply to " & coreName &
        ", not to the macro",
      p,
    )
  var
    params: seq[string]
    args: seq[NimNode]
  for i in 1 ..< def.params.len:
    let d = def.params[i]
    let t = d[^2]
    if t.kind == nnkBracketExpr and t[0].eqIdent"varargs":
      error(
        "macroSymLocals: a varargs param would reach " & coreName & " as one argument",
        t,
      )
    let isStatic =
      t.kind == nnkStaticTy or
      (t.kind in {nnkCommand, nnkBracketExpr, nnkCall} and t[0].eqIdent"static")
    for j in 0 ..< d.len - 2:
      params.add $d[j]
      # A macro sees a `static` argument as a value.
      args.add(
        if isStatic:
          newCall(bindSym"newLit", ident($d[j]))
        else:
          ident($d[j])
      )
  var
    locals: seq[TemplateLocal]
    nested: seq[NimNode]
  templateLocals(def.body, params, locals, nested)
  # A nested local of the same kind would share the symbol, pass sem and then
  # crash lambda lifting ("environment misses").
  for d in nested:
    for l in locals:
      if d.eqIdent(l.name):
        error(
          "macroSymLocals: a nested routine reuses the local name '" & l.name &
            "'; rename one",
          d,
        )

  let doc = newStmtList()
  let body = newStmtList()
  for s in def.body:
    if s.kind == nnkCommentStmt and body.len == 0:
      doc.add s
    else:
      body.add s
  let core = def.copyNimTree
  core[0] = ident(coreName)
  core[6] = body
  if locals.len > 0:
    let localParams = newNimNode(nnkIdentDefs)
    for l in locals:
      localParams.add ident(l.name)
    localParams.add ident"untyped", newEmptyNode()
    core.params.add localParams

  let call = newCall(bindSym"newCall", newCall(bindSym"bindSym", newLit(coreName)))
  call.add args
  for l in locals:
    call.add newCall(bindSym"localSym", newLit(l.kind), newLit(l.name), newLit(l.info))
  doc.add call
  let macroParams = def.params.copyNimTree
  macroParams[0] = ident"untyped"
  newStmtList(
    core,
    nnkMacroDef.newTree(
      def[0].copyNimTree,
      newEmptyNode(),
      def[2].copyNimTree,
      macroParams,
      newEmptyNode(),
      newEmptyNode(),
      doc,
    ),
  )
