## Dedicated unit tests for the detection half of ``tools/macro_sym_guard``. A
## clean tree prints ``ok`` whether the scan works or is blind, so only these
## tests can tell the two apart. They pin both directions: a real use has to be
## reported, and a pragma, a comment or code outside an expansion has to stay
## quiet.

import std/unittest

import ../tools/macro_sym_guard
import ../tools/source_scan

proc scanned(lines: openArray[string]): seq[string] =
  var sc: LineScanner
  for line in lines:
    result.add stripLiteralsAndComment(line, sc)

proc source(path: string, lines: varargs[string]): SourceFile =
  result = SourceFile(path: path, raw: @lines, code: scanned(lines))

proc genSymHits(lines: varargs[string]): seq[int] =
  genSymLines(scanned(lines))

proc tupleHits(lines: varargs[string]): seq[int] =
  let files = [source("m.nim", lines)]
  for (_, line) in tupleUnpackLines(files, findExpansions(files)):
    result.add line

suite "macro_sym_guard":
  test "a genSym call is reported":
    check genSymHits("let s = genSym(nskLet, \"conn\")") == @[0]
    check genSymHits("let s = macros.genSym(nskVar)") == @[0]
    check genSymHits("let s = gen_sym(nskLet, \"x\")") == @[0]
    check genSymHits("let a = 1", "let s = genSym nskLet") == @[1]

  test "a gensym pragma, a comment or a literal is not a call":
    check genSymHits("proc p() {.gensym.} = discard").len == 0
    check genSymHits("proc p() {.", "  gensym, used.} = discard").len == 0
    check genSymHits("## a genSym'd label").len == 0
    check genSymHits("let msg = \"genSym\"").len == 0
    check genSymHits("let s = macroSym(nskLet, \"conn\")").len == 0
    check genSymHits("let GenSym = 1").len == 0

  test "tuple unpacking inside a quote block is reported":
    check tupleHits(
      "result = quote:", "  block:", "    let (`a`, `b`) = await f()", "    discard"
    ) == @[2]
    check tupleHits("let n = quote do:", "  var (x, y) = f()") == @[1]
    check tupleHits("let n = quote(\"@\") do:", "  (x, y) = f()") == @[1]
    check tupleHits("result = quote:", "  let", "    a = 1", "    (b, c) = f()") == @[3]

  test "tuple unpacking inside a template body is reported":
    check tupleHits("template t*(body: untyped) =", "  let (a, b) = f()", "  body") ==
      @[1]
    check tupleHits(
      "template t*(", "    body: untyped", ") =", "  let (a, b) = f()", "  body"
    ) == @[3]
    check tupleHits("template t(x = 1) =", "  let (a, b) = f()") == @[1]
    check tupleHits("template `[]=`*(x: var X, v: int) =", "  let (a, b) = f()") == @[1]
    check tupleHits("template one(): int = 1", "result = quote:", "  let (a, b) = f()") ==
      @[2]
    check tupleHits(
      "template t() =", "  let (a, b) = f()", "proc p() {.async.} =", "  t()"
    ) == @[1]

  test "tuple unpacking that expands only into sync code is not reported":
    check tupleHits(
      "template gen(name: untyped) {.dirty.} =", "  proc name(): int =",
      "    let (a, b) = f()", "    a",
    ).len == 0
    check tupleHits("template t() =", "  let (a, b) = f()", "proc p() =", "  t()").len ==
      0

  test "code outside an expansion is not reported":
    check tupleHits("proc p() =", "  let (a, b) = f()").len == 0
    check tupleHits("result = quote:", "  discard", "let (a, b) = f()").len == 0
    check tupleHits(
      "template t() =", "  discard", "", "proc p() =", "  let (a, b) = f()"
    ).len == 0
    check tupleHits(
      "template t(", "    x: int", ") =", "  discard", "proc p() =",
      "  let (a, b) = f()",
    ).len == 0
    check tupleHits("template one(): int = 1", "proc p() =", "  let (a, b) = f()").len ==
      0
    check tupleHits(
      "template t(", "    x: int", "): int = x", "proc p() =", "  let (a, b) = f()"
    ).len == 0

  test "a non-unpacking line inside an expansion is not reported":
    check tupleHits("result = quote:", "  let x = (a, b)").len == 0
    check tupleHits("result = quote:", "  (a + b) == c").len == 0
    check tupleHits("result = quote:", "  (f)(x)").len == 0
    check tupleHits("result = quote:", "  ## let (a, b) = f()").len == 0
    check tupleHits("let quoted = quote", "  let (a, b) = f()").len == 0

proc localHits(files: varargs[SourceFile]): seq[string] =
  ## ``path:line:name`` per reported local, lines counted from 0.
  for (fi, line, name) in plainLocalLines(files, findExpansions(files)):
    result.add files[fi].path & ":" & $line & ":" & name

suite "macro_sym_guard: locals of async expansions":
  test "a quote block's own locals are reported":
    let m = source(
      "m.nim", "macro m(body: untyped): untyped =", "  result = quote:", "    try:",
      "      `body`", "    except CatchableError as e:", "      let msg = e.msg",
      "      for i in 0 ..< 2:", "        discard", "    var", "      a = 1",
      "      b = (1,", "        2)",
    )
    check localHits(m) ==
      @["m.nim:4:e", "m.nim:5:msg", "m.nim:6:i", "m.nim:9:a", "m.nim:10:b"]

  test "names spliced into a quote, and the macro's own code, are not reported":
    let m = source(
      "m.nim", "macro m(body: untyped): untyped =", "  let e = macroSym(nskLet, \"e\")",
      "  result = quote:", "    try:", "      `body`",
      "    except CatchableError as `e`:", "      let `x` {.used.} = 1",
    )
    check localHits(m).len == 0

  test "a template is checked when a call sits in an async routine":
    check localHits(
      source(
        "m.nim", "template t() =", "  let local = 1", "proc p() {.async.} =", "  t()"
      )
    ) == @["m.nim:1:local"]
    check localHits(
      source(
        "m.nim", "template t() =", "  let local = 1", "proc p(", "    a: int",
        "): Future[void] {.async.} =", "  t()",
      )
    ) == @["m.nim:1:local"]

  test "a template called only from sync code is not reported":
    check localHits(
      source("m.nim", "template t() =", "  let local = 1", "proc p() =", "  t()")
    ).len == 0
    check localHits(
      source(
        "m.nim", "proc p() =", "  template step() =", "    let local = 1", "  step()"
      )
    ).len == 0

  test "async reaches a template through the templates that call it":
    let chain = [
      "template t() =", "  let local = 1", "template u() =", "  t()",
      "proc p() {.async.} =", "  u()",
    ]
    check localHits(source("m.nim", chain)) == @["m.nim:1:local"]
    check localHits(source("m.nim", chain[0 .. 3] & @["proc p() =", "  u()"])).len == 0

  test "a template that awaits, has no call, or is bound by a macro is checked":
    check localHits(
      source("m.nim", "template t() =", "  let x = await f()", "proc p() =", "  t()")
    ) == @["m.nim:1:x"]
    check localHits(source("m.nim", "template t*(e: untyped) =", "  let local = e")) ==
      @["m.nim:1:local"]
    check localHits(
      source(
        "m.nim", "template core(x: untyped) =", "  let local = x", "proc p() =",
        "  core(1)", "macro m(): untyped =", "  newCall(bindSym\"core\", newLit(1))",
      )
    ) == @["m.nim:1:local"]

  test "template parameters name the caller's locals":
    check localHits(
      source(
        "m.nim", "template t(conn: int, e, retried: untyped) =",
        "  var retried = false", "  try:", "    discard",
        "  except CatchableError as e:", "    raise e",
      )
    ).len == 0
    check localHits(
      source(
        "m.nim", "template t(", "    conn: int,", "    e: untyped,", ") =", "  try:",
        "    discard", "  except CatchableError as e:", "    raise e",
        "  for i in 0 ..< conn:", "    discard",
      )
    ) == @["m.nim:8:i"]

  test "a macroSymLocals template's own locals are not reported":
    check localHits(
      source(
        "m.nim", "template t*(e: untyped) {.macroSymLocals.} =", "  let local = e",
        "  for i in 0 ..< 2:", "    discard",
      )
    ).len == 0
    check localHits(
      source(
        "m.nim", "template t*(", "    e: untyped", ") {.used, macroSymLocals.} =",
        "  let local = e", "  let cb = proc() {.async.} =", "    let x = 1",
      )
    ) == @["m.nim:5:x"]

  test "a macroSymLocals template's inject locals are reported":
    check localHits(
      source(
        "m.nim", "template t*(e: untyped) {.macroSymLocals.} =",
        "  var ctx {.inject.}: int", "  let a {.used.} = 1",
        "  let b {.inject, used.} = e", "  var", "    c {.inject.} = 1", "    d = 2",
      )
    ) == @["m.nim:1:ctx", "m.nim:3:b", "m.nim:5:c"]

  test "an async routine inside a template is checked, a sync one is not":
    check localHits(
      source(
        "m.nim", "template cb(body: untyped): untyped =", "  T(",
        "    proc() {.async.} =", "      let x = 1", "      body", "  )", "proc p() =",
        "  discard cb(discard)",
      )
    ) == @["m.nim:3:x"]
    check localHits(
      source(
        "m.nim", "template gen(name: untyped) =", "  proc name(): int =",
        "    let x = 1", "    x", "  let onRow: Cb = proc(row: Row) =",
        "    let y = row", "  discard onRow",
      )
    ) == @["m.nim:4:onRow"]

  test "a declaration header or the template's own body names another overload":
    check localHits(
      source(
        "m.nim", "template w*(p: P, body: untyped) =", "  body",
        "template w*(c: C, body: untyped) =", "  let local = 1", "  c.p.w:", "    body",
      )
    ) == @["m.nim:3:local"]
    check localHits(
      source("a.nim", "macro w*(p: P, body: untyped): untyped =", "  body"),
      source(
        "b.nim", "template w*(c: C, body: untyped) =", "  let local = 1", "  c.p.w:",
        "    body",
      ),
    ) == @["b.nim:1:local"]

  test "an import or an export is not a call":
    check localHits(
      source("a.nim", "template t*() =", "  let local = 1"),
      source("b.nim", "import a", "export a.t", "export a except", "  t"),
    ) == @["a.nim:1:local"]

  test "an exported template links to other files, a private one does not":
    let lib =
      source("a.nim", "template t*() =", "  let local = 1", "proc q() =", "  t()")
    let user = source("b.nim", "proc p() {.async.} =", "  t()")
    check localHits(lib, user) == @["a.nim:1:local"]
    let private =
      source("a.nim", "template t() =", "  let local = 1", "proc q() =", "  t()")
    check localHits(private, user).len == 0

  test "declarations in parentheses count, a compiles check does not":
    check localHits(
      source(
        "m.nim", "template t() =", "  while (let opt = next(); opt.isSome):",
        "    discard", "  when compiles((var r: Row; r.get(0))):", "    discard",
      )
    ) == @["m.nim:1:opt"]
