## Locals that library macros and templates declare in the caller's proc must
## have `macroSym` names: a plain name can meet the caller's local in the
## closure env (see `pg_gensym`). Which names meet depends on the env layout,
## so every declaration is checked rather than one colliding layout.

import std/[macros, os, strutils, unittest]

import ../async_postgres
import ../async_postgres/pg_gensym
import ../async_postgres/pg_connection/[buffer_io, simple_query, types]
import ../async_postgres/pg_client/core

const
  repoDir = currentSourcePath().parentDir.parentDir.replace('\\', '/') & "/"
  testDir = currentSourcePath().parentDir.replace('\\', '/') & "/"
  libDir =
    (currentSourcePath().parentDir.parentDir / "async_postgres").replace('\\', '/') & "/"

proc envSafeName(name: string): bool =
  ## The backtick keeps identifiers out; the final letter leaves the appended
  ## index as the only trailing number.
  '`' in name and name[^1] notin Digits

proc declaredName(n: NimNode): NimNode =
  if n.kind == nnkPragmaExpr:
    n[0]
  else:
    n

proc collectDeclared(n: NimNode, acc: var seq[NimNode]) =
  ## Routine names are skipped: a routine is never an env field. So are the
  ## params of routines declared in the body: they sit in that routine's own
  ## env, and an `{.async.}` body's locals sit in its iterator's env.
  case n.kind
  of nnkFormalParams:
    return
  of nnkIdentDefs, nnkForStmt:
    for i in 0 ..< n.len - 2:
      acc.add declaredName(n[i])
  of nnkVarTuple:
    for i in 0 ..< n.len - 1:
      acc.add declaredName(n[i])
  of nnkExceptBranch:
    for i in 0 ..< n.len - 1:
      if n[i].kind == nnkInfix:
        acc.add n[i][2]
  else:
    discard
  for child in n:
    collectDeclared(child, acc)

macro unsafeLocalsFrom(
    dir: static string, userNames: static openArray[string], body: typed
): untyped =
  ## Names declared in `body` by code under `dir` that can meet another env
  ## field's name, other than `userNames` (names the caller passed in or the
  ## macro injects), and how many names code under `dir` declared at all, so a
  ## path that never matches cannot pass as "nothing unsafe". `body` is only
  ## type-checked, not run.
  var declared: seq[NimNode]
  collectDeclared(body, declared)
  var
    seen = 0
    unsafe: seq[string]
  for s in declared:
    if s.kind != nnkSym or not s.lineInfoObj.filename.replace('\\', '/').startsWith(dir):
      continue
    inc seen
    if not envSafeName($s) and $s notin userNames:
      unsafe.add $s
  newLit((seen: seen, unsafe: unsafe))

proc collectFiles(n: NimNode, name: string, acc: var seq[string]) =
  if n.kind == nnkSym and $n == name:
    acc.add n.lineInfoObj.filename.replace('\\', '/')
  for child in n:
    collectFiles(child, name, acc)

macro symFiles(name: static string, body: typed): untyped =
  ## The files the `name` nodes in `body` point at, where their errors go.
  var files: seq[string]
  collectFiles(body, name, files)
  newLit(files)

macro plainGenSymLet(body: untyped): untyped =
  let s = genSym(nskLet, "leaked")
  result = quote:
    let `s` = 1
    `body`
    discard `s`

template injectedVar(body: untyped) =
  var injected {.inject.} = 1
  body
  discard injected

template templateVar(body: untyped) =
  var counted = 1
  body
  discard counted

macro macroSymLet(body: untyped): untyped =
  let s = macroSym(nskLet, "kept")
  result = quote:
    let `s` = 1
    `body`
    discard `s`

template symLocals(shadow, body: untyped) {.macroSymLocals.} =
  var v = 1
  let w = 2
  var shadow = 3
  var injected {.inject.} = 4
  for i in 0 ..< 1:
    discard i
  try:
    body
  except CatchableError as e:
    discard e
  discard (v, w, shadow, injected)

template genericLocals[T](r: var seq[T], x: T) {.macroSymLocals.} =
  let y = x
  r.add y

# A local becomes a template parameter, so a nested routine's same-spelled
# local would share its symbol and crash lambda lifting after sem.
const nestedReuseCompiles = compiles(
  block:
    template t(r: var seq[int]) {.macroSymLocals.} =
      let e = 1
      let f = proc(): int =
        let e = 2
        e
      r.add e
      r.add f()

    var r: seq[int]
    t(r)
)

const twoKindsCompiles = compiles(
  block:
    template t(r: var seq[int], c: bool) {.macroSymLocals.} =
      if c:
        let e = 1
        r.add e
      else:
        var e = 2
        r.add e

    var r: seq[int]
    t(r, true)
)

const pragmaCompiles = compiles(
  block:
    template t(r: var seq[int]) {.used, macroSymLocals.} =
      let e = 1
      r.add e

)

const varargsCompiles = compiles(
  block:
    template t(r: var seq[int], xs: varargs[int]) {.macroSymLocals.} =
      let e = 1
      r.add e
      for x in xs:
        r.add x

    var r: seq[int]
    t(r, 2, 3)
)

proc expandScopedMacros(): Future[tuple[seen: int, unsafe: seq[string]]] {.async.} =
  let connection: PgConnection = nil
  let pool: PgPool = nil
  let cluster: PgPoolCluster = nil
  let tracer: PgTracer = nil
  let scoped = unsafeLocalsFrom(libDir, ["userConn", "userLo", "userCur", "userPipe"]):
    connection.withTransaction:
      discard
    connection.withTransaction(seconds(5)):
      discard
    connection.withTransactionRetry(RetryOptions()):
      discard
    connection.withSavepoint:
      discard
    connection.withSavepoint("sp", seconds(5)):
      discard
    connection.withTransactionDeadline(seconds(5)):
      discard
    connection.withTransactionRetryDeadline(RetryOptions(), seconds(5)):
      discard
    connection.withSavepointDeadline(seconds(5)):
      discard
    connection.withAdvisoryLock(1'i64):
      discard
    connection.withAdvisoryLockShared(1'i32, 2'i32, seconds(5)):
      discard
    connection.withAdvisoryLockXact(1'i64):
      discard
    connection.withAdvisoryLockXactShared(1'i64):
      discard
    connection.withLargeObject(userLo, 0.Oid, INV_READWRITE):
      discard userLo
    connection.withCursor("SELECT 1", 10'i32, userCur):
      discard userCur
    # Each in its own scope: a second `traceCtx` in one scope is an error.
    block:
      withConnTracing(
        connection,
        onPrepareStart,
        onPrepareEnd,
        TracePrepareStartData(),
        TracePrepareEndData,
        TracePrepareEndData(),
      ):
        discard traceCtx
    block:
      withTracing(
        tracer,
        onPoolAcquireStart,
        onPoolAcquireEnd,
        TracePoolAcquireStartData(),
        TracePoolAcquireEndData,
        TracePoolAcquireEndData(),
      ):
        discard traceCtx
    let loRead = makeLoReadCallback:
      discard data
    let loWrite = makeLoWriteCallback:
      newSeq[byte]()
    let copyOut = makeCopyOutCallback:
      discard data
    let copyIn = makeCopyInCallback:
      newSeq[byte]()
    let repl = makeReplicationCallback:
      discard msg
    discard (loRead, loWrite, copyOut, copyIn, repl)
    discard await connection.queryDirect("SELECT $1, $2", 1'i32, "a")
    discard await connection.execDirect("SELECT $1", 1'i32)
    block:
      pool.withConnection(userConn):
        discard userConn
    block:
      pool.withTransaction(userConn):
        discard userConn
    block:
      pool.withTransactionRetry(RetryOptions(), userConn):
        discard userConn
    block:
      pool.withTransactionDeadline(userConn, seconds(5)):
        discard userConn
    block:
      pool.withTransactionRetryDeadline(RetryOptions(), userConn, seconds(5)):
        discard userConn
    block:
      cluster.withReadConnection(userConn):
        discard userConn
    block:
      cluster.withWriteConnection(userConn):
        discard userConn
    block:
      cluster.withTransaction(userConn):
        discard userConn
    block:
      cluster.withTransactionRetry(RetryOptions(), userConn):
        discard userConn
    block:
      cluster.withTransactionDeadline(userConn, seconds(5)):
        discard userConn
    block:
      cluster.withTransactionRetryDeadline(RetryOptions(), userConn, seconds(5)):
        discard userConn
    # No `conn` is in scope here, so these resolve only to the macro's alias.
    pool.withPipeline(userPipe):
      let pipeConn: PgConnection = conn
      discard (userPipe, pipeConn)
    cluster.withPipeline(userPipe):
      let pipeConn: PgConnection = conn
      discard (userPipe, pipeConn)
  return scoped

proc expandInternalMacros(): Future[tuple[seen: int, unsafe: seq[string]]] {.async.} =
  ## The library's own expansions into its async procs.
  let connection: PgConnection = nil
  var
    qr: QueryResult
    rowCount: int64
    eachFacts: OpFacts
    rowErr: ref CatchableError
  let internal = unsafeLocalsFrom(libDir, ["cacheHit", "facts"]):
    awaitOrInvalidate(connection, qr, connection.query("SELECT 1"), seconds(5), "t")
    awaitVoidOrInvalidate(connection, sleepAsync(milliseconds(1)), seconds(5), "t")
    connection.pumpUntilReady:
      discard pumpMsg
    do:
      discard queryError
    connection.pumpUntilReady(qr.data, addr qr.rowCount):
      discard pumpMsg
    do:
      discard queryError
    connection.pumpUntilReady(qr.data, nil, addr rowErr):
      discard pumpMsg
    do:
      discard queryError
    retryStmtCacheInvalidation(connection, cacheHit, facts):
      discard (cacheHit, facts)
    queryRecvLoop(
      connection, "", newSeq[int16](), false, false, "", RowData(nil), qr, eachFacts
    )
    queryEachRecvLoop(
      connection,
      "",
      newSeq[int16](),
      false,
      false,
      "",
      RowData(nil),
      RowCallback(nil),
      rowCount,
      eachFacts,
    )
  return internal

suite "Macro env names":
  test "a plain genSym local is reported":
    let r = unsafeLocalsFrom(testDir, []):
      plainGenSymLet:
        discard
    check r.unsafe == @["leaked"]

  test "an injected local is reported":
    let r = unsafeLocalsFrom(testDir, []):
      injectedVar:
        discard
    check r.unsafe == @["injected"]

  test "a template local is reported":
    let r = unsafeLocalsFrom(testDir, []):
      templateVar:
        discard
    check r.unsafe.len == 1
    check r.unsafe[0].startsWith("counted`gensym")

  test "a macroSym local passes":
    # A `macroSym` symbol carries the line info of pg_gensym.nim.
    let r = unsafeLocalsFrom(libDir, []):
      macroSymLet:
        discard
    check r.seen == 1
    check r.unsafe.len == 0

  test "macroSymLocals gives a template's locals macroSym names":
    # A parameter is the caller's name; an `{.inject.}` local keeps its own.
    # Which file a declaration points at varies by kind, so both are scanned.
    let r = unsafeLocalsFrom(repoDir, ["userShadow"]):
      symLocals(userShadow):
        discard
    check r.seen == 6
    check r.unsafe == @["injected"]

  test "macroSymLocals reports a local's errors at the template":
    # The declaration keeps the symbol's own info; its uses carry the local's.
    let files = symFiles("v`pg"):
      symLocals(userShadow):
        discard
    check (testDir & "test_macro_env_names.nim") in files

  test "macroSymLocals keeps a template's generic params":
    var ints: seq[int]
    var strs: seq[string]
    let r = unsafeLocalsFrom(libDir, []):
      genericLocals(ints, 1)
    genericLocals(strs, "a")
    check r.seen == 1
    check r.unsafe.len == 0
    check strs == @["a"]

  test "macroSymLocals rejects a name it cannot give one symbol":
    check not nestedReuseCompiles
    check not twoKindsCompiles

  test "macroSymLocals rejects what its macro cannot forward":
    check not pragmaCompiles
    check not varargsCompiles

  test "scoped macros declare only macroSym locals":
    let (seen, unsafe) = waitFor expandScopedMacros()
    checkpoint "unsafe: " & $unsafe
    check seen > 0
    check unsafe.len == 0

  test "internal macros declare only macroSym locals":
    let (seen, unsafe) = waitFor expandInternalMacros()
    checkpoint "unsafe: " & $unsafe
    check seen > 0
    check unsafe.len == 0
