## Zero-allocation `queryDirect` / `execDirect` compile-time macros that
## encode parameters directly into the connection send buffer.

import std/[algorithm, macros, options, sets, tables]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

proc queryDirectRunImpl*(
    conn: PgConnection,
    sql: string,
    resultFormats: seq[int16],
    colFmts: seq[int16],
    colOids: seq[int32],
    cacheHit: bool,
    cacheMiss: bool,
    stmtName: string,
    cachedFields: seq[FieldDescription],
): Future[QueryResult] {.async.} =
  ## Inner send + receive loop for queryDirect. Pulled out so the outer Impl
  ## can wrap the returned Future with ``.wait(timeout)`` without paying for
  ## a closure allocation (mirrors the ``queryImpl`` / ``query*`` split in
  ## ``query.nim``).
  result = QueryResult()
  await conn.sendBufMsg()
  var cf = cachedFields
  queryRecvLoop(
    conn, sql, resultFormats, cacheHit, cacheMiss, stmtName, cf, colFmts, colOids,
    result,
  )

proc queryDirectImpl*(
    conn: PgConnection,
    sql: string,
    resultFormats: seq[int16],
    colFmts: seq[int16],
    colOids: seq[int32],
    cacheHit: bool,
    cacheMiss: bool,
    stmtName: string,
    cachedFields: seq[FieldDescription],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Trace + timeout wrapper for the queryDirect macro. The synchronous
  ## parameter-encoding step already filled ``conn.sendBuf`` before this proc
  ## was called, so tracing fires around the network round-trip only — which
  ## matches the behavior tracers see for ``query`` / ``exec``.
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: result.commandTag, rowCount: result.rowCount),
  ):
    awaitOrInvalidate(
      conn,
      result,
      queryDirectRunImpl(
        conn, sql, resultFormats, colFmts, colOids, cacheHit, cacheMiss, stmtName,
        cachedFields,
      ),
      timeout,
      "queryDirect timed out",
    )

proc buildInvalidateOnOidMismatchStmt(
    connSym, sqlSym, cachedSym, cacheHitSym: NimNode, positional: seq[NimNode]
): NimNode =
  ## AST builder for the cache-hit OID-validation call shared by both direct
  ## macros. Emits a single ``invalidateIfOidMismatch`` call with a
  ## ``[paramOidOf(arg0), paramOidOf(arg1), …]`` array literal — kept as an
  ## ``array`` (not ``seq``) so the OID list can live on the stack and
  ## preserve the macro's zero-allocation contract.
  ##
  ## When the macro has no positional parameters there is nothing to compare
  ## (the cache key — SQL text — already pins a fixed parameter count, so
  ## same-SQL cache hits cannot differ in arity here), and an empty
  ## ``[]`` literal has no inferable element type. Emit an empty statement
  ## list instead.
  if positional.len == 0:
    return newStmtList()
  let bracket = newNimNode(nnkBracket)
  for arg in positional:
    bracket.add(newCall(ident"paramOidOf", arg))
  result = quote:
    invalidateIfOidMismatch(`connSym`, `sqlSym`, `cachedSym`, `bracket`, `cacheHitSym`)

proc scanPlaceholders(sql: string): tuple[seen: HashSet[int], badZero: bool] =
  ## Compile-time SQL scanner: walks ``sql`` and collects every ``$N``
  ## placeholder index appearing in normal code positions, skipping single-
  ## quoted strings, ``E'...'`` C-style strings, double-quoted identifiers,
  ## dollar-quoted blocks (``$$...$$`` and ``$tag$...$tag$``), ``--`` line
  ## comments and ``/* ... */`` block comments (nestable, per PostgreSQL).
  ## Sets ``badZero`` when ``$0`` is encountered (PostgreSQL placeholders are
  ## 1-based).
  type ScanState = enum
    sNormal
    sSingleQuote
    sEString
    sDoubleQuote
    sDollarQuote
    sLineComment
    sBlockComment

  var state = sNormal
  var dollarTag = ""
  var blockDepth = 0
  var i = 0
  let n = sql.len

  while i < n:
    let c = sql[i]
    case state
    of sNormal:
      case c
      of '\'':
        let isE =
          i > 0 and sql[i - 1] in {'E', 'e'} and
          (i < 2 or sql[i - 2] notin {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'})
        state = if isE: sEString else: sSingleQuote
        inc i
      of '"':
        state = sDoubleQuote
        inc i
      of '-':
        if i + 1 < n and sql[i + 1] == '-':
          state = sLineComment
          i += 2
        else:
          inc i
      of '/':
        if i + 1 < n and sql[i + 1] == '*':
          state = sBlockComment
          blockDepth = 1
          i += 2
        else:
          inc i
      of '$':
        var j = i + 1
        if j < n and sql[j] == '$':
          dollarTag = "$$"
          state = sDollarQuote
          i = j + 1
        elif j < n and sql[j] in {'a' .. 'z', 'A' .. 'Z', '_'}:
          while j < n and sql[j] in {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'}:
            inc j
          if j < n and sql[j] == '$':
            dollarTag = sql[i .. j]
            state = sDollarQuote
            i = j + 1
          else:
            i = j
        elif j < n and sql[j] in {'0' .. '9'}:
          var num = 0
          while j < n and sql[j] in {'0' .. '9'}:
            num = num * 10 + (sql[j].ord - '0'.ord)
            inc j
          if num == 0:
            result.badZero = true
          else:
            result.seen.incl(num)
          i = j
        else:
          inc i
      else:
        inc i
    of sSingleQuote:
      if c == '\'':
        if i + 1 < n and sql[i + 1] == '\'':
          i += 2
        else:
          state = sNormal
          inc i
      else:
        inc i
    of sEString:
      if c == '\\':
        i += (if i + 1 < n: 2 else: 1)
      elif c == '\'':
        if i + 1 < n and sql[i + 1] == '\'':
          i += 2
        else:
          state = sNormal
          inc i
      else:
        inc i
    of sDoubleQuote:
      if c == '"':
        if i + 1 < n and sql[i + 1] == '"':
          i += 2
        else:
          state = sNormal
          inc i
      else:
        inc i
    of sDollarQuote:
      if c == '$' and i + dollarTag.len <= n and
          sql[i ..< i + dollarTag.len] == dollarTag:
        i += dollarTag.len
        state = sNormal
      else:
        inc i
    of sLineComment:
      if c == '\n':
        state = sNormal
      inc i
    of sBlockComment:
      if c == '*' and i + 1 < n and sql[i + 1] == '/':
        dec blockDepth
        i += 2
        if blockDepth == 0:
          state = sNormal
      elif c == '/' and i + 1 < n and sql[i + 1] == '*':
        inc blockDepth
        i += 2
      else:
        inc i

proc formatPlaceholders(xs: HashSet[int]): string =
  ## Render a HashSet of placeholder indices as a sorted ``$1, $2, ...`` string
  ## for diagnostic messages.
  var sorted: seq[int]
  for k in xs:
    sorted.add(k)
  sorted.sort()
  for idx, k in sorted:
    if idx > 0:
      result.add(", ")
    result.add('$')
    result.add($k)

proc resolveSqlLiteral(sqlNode: NimNode): NimNode =
  ## Return the underlying string-literal node for ``sqlNode`` when possible:
  ## direct string literals pass through, and ``const`` symbols bound to a
  ## string literal are resolved via ``getImpl``. Returns ``nil`` for anything
  ## else (let/var bindings, concatenations, proc results, …) so callers can
  ## skip compile-time checks for non-literal SQL.
  if sqlNode.kind in {nnkStrLit, nnkTripleStrLit, nnkRStrLit}:
    return sqlNode
  if sqlNode.kind == nnkSym:
    let impl = sqlNode.getImpl
    if impl.kind == nnkConstDef and impl.len >= 3:
      let val = impl[2]
      if val.kind in {nnkStrLit, nnkTripleStrLit, nnkRStrLit}:
        return val
  return nil

proc validatePlaceholderArity(
    sqlNode: NimNode, positionalCount: int, macroName: string
) =
  ## Compile-time arity check: when ``sqlNode`` resolves to a string literal
  ## (directly or via a ``const`` symbol), require that the set of ``$N``
  ## placeholder indices in normal SQL positions equals
  ## ``{1, 2, ..., positionalCount}`` exactly. Non-literal SQL (let/var
  ## bindings, concatenation results, runtime-built strings, etc.) is skipped
  ## silently — the runtime path still surfaces mismatches via the PostgreSQL
  ## ``08P01`` error response.
  let lit = resolveSqlLiteral(sqlNode)
  if lit == nil:
    return
  let (seen, badZero) = scanPlaceholders(lit.strVal)
  if badZero:
    error(macroName & ": '$0' is not a valid placeholder ($N is 1-based)", sqlNode)
  var expected: HashSet[int]
  for k in 1 .. positionalCount:
    expected.incl(k)
  if seen == expected:
    return
  let extra = seen - expected
  let missing = expected - seen
  if extra.len > 0 and missing.len == 0:
    error(
      macroName & ": SQL references " & formatPlaceholders(extra) & " but only " &
        $positionalCount & " arg(s) were passed",
      sqlNode,
    )
  elif missing.len > 0 and extra.len == 0:
    # missing-only happens in two shapes:
    #   (a) gap in numbering, e.g. $1, $3 with 3 args → missing={2}
    #   (b) too many args, e.g. SQL uses only $1 but caller passed 2 args
    # Distinguish by whether seen covers a contiguous prefix below
    # positionalCount: case (b) is friendlier framed as "too many args".
    var maxSeen = 0
    for k in seen:
      if k > maxSeen:
        maxSeen = k
    if maxSeen < positionalCount:
      let referenced =
        if seen.len == 0:
          "no placeholders"
        elif maxSeen == 1:
          "only $1"
        else:
          "only $1..$" & $maxSeen
      error(
        macroName & ": SQL references " & referenced & " but " & $positionalCount &
          " arg(s) were passed",
        sqlNode,
      )
    else:
      error(
        macroName & ": SQL is missing placeholder(s) " & formatPlaceholders(missing) &
          " (expected $1..$" & $positionalCount & ")",
        sqlNode,
      )
  else:
    error(
      macroName & ": SQL placeholders {" & formatPlaceholders(seen) &
        "} do not match expected $1..$" & $positionalCount,
      sqlNode,
    )

proc bindPositionalOnce(
    positional: seq[NimNode]
): tuple[bindings: NimNode, syms: seq[NimNode]] =
  ## Emit ``let tmp = <arg>`` for each positional argument so downstream
  ## fan-out (``paramOidOf`` in the invalidate call plus ``writeParamOid`` /
  ## ``writeParamFormat`` / ``writeParamValue`` in the Bind/Parse macros)
  ## substitutes the temporary, not the source expression. Without this a
  ## side-effecting argument such as ``getNextId()`` would fire 3–4 times per
  ## direct call.
  result.bindings = newStmtList()
  result.syms = newSeq[NimNode](positional.len)
  for i, arg in positional:
    let tmp = genSym(nskLet, "directArg" & $i)
    result.syms[i] = tmp
    result.bindings.add(newLetStmt(tmp, arg))

proc extractTimeoutArg(
    args: NimNode
): tuple[positional: seq[NimNode], timeout: NimNode] =
  ## Walk a varargs[untyped] array, peeling off a trailing
  ## ``timeout = <expr>`` keyword argument if present. Returns the remaining
  ## positional args plus the timeout expression (defaulting to
  ## ``ZeroDuration`` when absent).
  result.positional = newSeq[NimNode]()
  result.timeout = nil
  for arg in args:
    if arg.kind == nnkExprEqExpr and arg[0].kind == nnkIdent and
        arg[0].eqIdent("timeout"):
      result.timeout = arg[1]
    else:
      result.positional.add(arg)
  if result.timeout == nil:
    result.timeout = bindSym"ZeroDuration"

macro queryDirect*(conn: PgConnection, sql: string, args: varargs[untyped]): untyped =
  ## Zero-allocation query: encodes parameters directly into the send buffer
  ## at compile time, avoiding ``seq[PgParam]`` and intermediate ``seq[byte]`` allocs.
  ##
  ## Usage:
  ##   let qr = await conn.queryDirect("SELECT ... WHERE id = $1", myId)
  ##   let qr = await conn.queryDirect(
  ##     "SELECT ... WHERE id = $1", myId, timeout = 1.seconds)
  ##
  ## Tracing fires the ``onQueryStart`` / ``onQueryEnd`` hooks with only
  ## ``sql`` populated — ``params`` is left empty to preserve the zero-alloc
  ## guarantee.
  result = newStmtList()

  let (positional, timeoutExpr) = extractTimeoutArg(args)
  validatePlaceholderArity(sql, positional.len, "queryDirect")

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
  let timeoutSym = genSym(nskLet, "timeout")
  let cachedSym = genSym(nskLet, "cached")
  let cacheHitSym = genSym(nskVar, "cacheHit")
  let cacheMissSym = genSym(nskVar, "cacheMiss")
  let stmtNameSym = genSym(nskVar, "stmtName")
  let cachedFieldsSym = genSym(nskVar, "cachedFields")
  let effectiveRfSym = genSym(nskVar, "effectiveRf")
  let colFmtsSym = genSym(nskVar, "colFmts")
  let colOidsSym = genSym(nskVar, "colOids")

  result.add quote do:
    let `connSym` = `conn`
    let `sqlSym` = `sql`
    let `timeoutSym`: Duration = `timeoutExpr`
    `connSym`.checkReady()

    let `cachedSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedSym` != nil
    var `cacheMissSym` = false
    var `stmtNameSym` = ""
    var `cachedFieldsSym`: seq[FieldDescription]
    var `effectiveRfSym`: seq[int16]
    var `colFmtsSym`: seq[int16]
    var `colOidsSym`: seq[int32]

  let (argBindings, argSyms) = bindPositionalOnce(positional)
  result.add argBindings

  result.add buildInvalidateOnOidMismatchStmt(
    connSym, sqlSym, cachedSym, cacheHitSym, argSyms
  )

  # Helper to build addBindDirect call with args
  proc makeBindDirect(buf, portal, stmt, rf: NimNode, argList: NimNode): NimNode =
    result = newCall(bindSym"addBindDirect", buf, portal, stmt, rf)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  proc makeParseDirect(buf, stmt, sql: NimNode, argList: NimNode): NimNode =
    result = newCall(bindSym"addParseDirect", buf, stmt, sql)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  let argList = newNimNode(nnkBracket)
  for sym in argSyms:
    argList.add(sym)

  # Cache hit path
  let hitBlock = newStmtList()
  hitBlock.add quote do:
    `stmtNameSym` = `cachedSym`.name
    `cachedFieldsSym` = `cachedSym`.fields
    `colFmtsSym` = `cachedSym`.colFmts
    `colOidsSym` = `cachedSym`.colOids
    `effectiveRfSym` = `cachedSym`.resultFormats
    `connSym`.sendBuf.setLen(0)
    `connSym`.flushPendingStmtCloses()
  let sendBufNode = newDotExpr(connSym, ident"sendBuf")
  hitBlock.add(
    makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, effectiveRfSym, argList)
  )
  hitBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Cache miss path
  let missBlock = newStmtList()
  missBlock.add quote do:
    `cacheMissSym` = true
    `stmtNameSym` = `connSym`.nextStmtName()
    `effectiveRfSym` = @[]
    `connSym`.sendBuf.setLen(0)
    `connSym`.flushPendingStmtCloses()
    if `connSym`.stmtCache.len >= `connSym`.stmtCacheCapacity:
      let evicted = `connSym`.evictStmtCache()
      `connSym`.sendBuf.addClose(dkStatement, evicted.name)
  missBlock.add(makeParseDirect(sendBufNode, stmtNameSym, sqlSym, argList))
  missBlock.add quote do:
    `connSym`.sendBuf.addDescribe(dkStatement, `stmtNameSym`)
  missBlock.add(
    makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, effectiveRfSym, argList)
  )
  missBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # No-cache path
  let elseBlock = newStmtList()
  elseBlock.add quote do:
    `effectiveRfSym` = @[]
    `connSym`.sendBuf.setLen(0)
    `connSym`.flushPendingStmtCloses()
  elseBlock.add(makeParseDirect(sendBufNode, newStrLitNode(""), sqlSym, argList))
  elseBlock.add(
    makeBindDirect(
      sendBufNode, newStrLitNode(""), newStrLitNode(""), effectiveRfSym, argList
    )
  )
  elseBlock.add quote do:
    `connSym`.sendBuf.addDescribe(dkPortal, "")
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Build if/elif/else
  let ifNode = newNimNode(nnkIfStmt)
  ifNode.add(
    newNimNode(nnkElifBranch).add(
      quote do:
        `cacheHitSym`,
      hitBlock,
    )
  )

  let missCondition = quote:
    `connSym`.stmtCacheCapacity > 0
  ifNode.add(newNimNode(nnkElifBranch).add(missCondition, missBlock))
  ifNode.add(newNimNode(nnkElse).add(elseBlock))
  result.add(ifNode)

  result.add quote do:
    `connSym`.state = csBusy
    queryDirectImpl(
      `connSym`, `sqlSym`, `effectiveRfSym`, `colFmtsSym`, `colOidsSym`, `cacheHitSym`,
      `cacheMissSym`, `stmtNameSym`, `cachedFieldsSym`, `timeoutSym`,
    )

proc execDirectRunImpl*(
    conn: PgConnection, sql: string, cacheHit: bool, cacheMiss: bool, stmtName: string
): Future[string] {.async.} =
  ## Inner send + receive loop for execDirect. Returns the command tag and
  ## handles error reporting / cache bookkeeping. Split out so the outer
  ## Impl can apply ``.wait(timeout)`` without an extra closure alloc.
  await conn.sendBufMsg()
  var commandTag = ""
  execRecvLoop(conn, sql, cacheHit, cacheMiss, stmtName, commandTag)
  return commandTag

proc execDirectImpl*(
    conn: PgConnection,
    sql: string,
    cacheHit: bool,
    cacheMiss: bool,
    stmtName: string,
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Trace + timeout wrapper for the execDirect macro. Parameter encoding
  ## has already completed synchronously in the macro expansion before this
  ## proc is invoked.
  var tag = ""
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    awaitOrInvalidate(
      conn,
      tag,
      execDirectRunImpl(conn, sql, cacheHit, cacheMiss, stmtName),
      timeout,
      "execDirect timed out",
    )
  return initCommandResult(tag)

macro execDirect*(conn: PgConnection, sql: string, args: varargs[untyped]): untyped =
  ## Zero-allocation exec: encodes parameters directly into the send buffer
  ## at compile time, avoiding ``seq[PgParam]`` and intermediate ``seq[byte]`` allocs.
  ##
  ## Usage:
  ##   discard await conn.execDirect("UPDATE ... WHERE id = $1", myId)
  ##   discard await conn.execDirect(
  ##     "UPDATE ... WHERE id = $1", myId, timeout = 1.seconds)
  ##
  ## Tracing fires the ``onQueryStart`` / ``onQueryEnd`` hooks with only
  ## ``sql`` populated — ``params`` is left empty to preserve the zero-alloc
  ## guarantee.
  result = newStmtList()

  let (positional, timeoutExpr) = extractTimeoutArg(args)
  validatePlaceholderArity(sql, positional.len, "execDirect")

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
  let timeoutSym = genSym(nskLet, "timeout")
  let cachedSym = genSym(nskLet, "cached")
  let cacheHitSym = genSym(nskVar, "cacheHit")
  let cacheMissSym = genSym(nskVar, "cacheMiss")
  let stmtNameSym = genSym(nskVar, "stmtName")

  result.add quote do:
    let `connSym` = `conn`
    let `sqlSym` = `sql`
    let `timeoutSym`: Duration = `timeoutExpr`
    `connSym`.checkReady()

    let `cachedSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedSym` != nil
    var `cacheMissSym` = false
    var `stmtNameSym` = ""

  let (argBindings, argSyms) = bindPositionalOnce(positional)
  result.add argBindings

  result.add buildInvalidateOnOidMismatchStmt(
    connSym, sqlSym, cachedSym, cacheHitSym, argSyms
  )

  proc makeBindDirect(buf, portal, stmt: NimNode, argList: NimNode): NimNode =
    let emptyRf = newNimNode(nnkBracket) # no result formats for exec
    result = newCall(bindSym"addBindDirect", buf, portal, stmt, emptyRf)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  proc makeParseDirect(buf, stmt, sql: NimNode, argList: NimNode): NimNode =
    result = newCall(bindSym"addParseDirect", buf, stmt, sql)
    for i in 0 ..< argList.len:
      result.add(argList[i])

  let argList = newNimNode(nnkBracket)
  for sym in argSyms:
    argList.add(sym)

  let sendBufNode = newDotExpr(connSym, ident"sendBuf")

  # Cache hit path
  let hitBlock = newStmtList()
  hitBlock.add quote do:
    `stmtNameSym` = `cachedSym`.name
    `connSym`.sendBuf.setLen(0)
    `connSym`.flushPendingStmtCloses()
  hitBlock.add(makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, argList))
  hitBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Cache miss path
  let missBlock = newStmtList()
  missBlock.add quote do:
    `cacheMissSym` = true
    `stmtNameSym` = `connSym`.nextStmtName()
    `connSym`.sendBuf.setLen(0)
    `connSym`.flushPendingStmtCloses()
    if `connSym`.stmtCache.len >= `connSym`.stmtCacheCapacity:
      let evicted = `connSym`.evictStmtCache()
      `connSym`.sendBuf.addClose(dkStatement, evicted.name)
  missBlock.add(makeParseDirect(sendBufNode, stmtNameSym, sqlSym, argList))
  missBlock.add quote do:
    `connSym`.sendBuf.addDescribe(dkStatement, `stmtNameSym`)
  missBlock.add(makeBindDirect(sendBufNode, newStrLitNode(""), stmtNameSym, argList))
  missBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # No-cache path
  let elseBlock = newStmtList()
  elseBlock.add quote do:
    `connSym`.sendBuf.setLen(0)
    `connSym`.flushPendingStmtCloses()
  elseBlock.add(makeParseDirect(sendBufNode, newStrLitNode(""), sqlSym, argList))
  elseBlock.add(
    makeBindDirect(sendBufNode, newStrLitNode(""), newStrLitNode(""), argList)
  )
  elseBlock.add quote do:
    `connSym`.sendBuf.addExecute("", 0)
    `connSym`.sendBuf.addSync()

  # Build if/elif/else
  let ifNode = newNimNode(nnkIfStmt)
  ifNode.add(
    newNimNode(nnkElifBranch).add(
      quote do:
        `cacheHitSym`,
      hitBlock,
    )
  )

  let missCondition = quote:
    `connSym`.stmtCacheCapacity > 0
  ifNode.add(newNimNode(nnkElifBranch).add(missCondition, missBlock))
  ifNode.add(newNimNode(nnkElse).add(elseBlock))
  result.add(ifNode)

  result.add quote do:
    `connSym`.state = csBusy
    execDirectImpl(
      `connSym`, `sqlSym`, `cacheHitSym`, `cacheMissSym`, `stmtNameSym`, `timeoutSym`
    )
