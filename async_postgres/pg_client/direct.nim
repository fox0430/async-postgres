## Zero-allocation `queryDirect` / `execDirect` compile-time macros that
## encode parameters directly into the connection send buffer.

import std/[macros, options, tables]

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
    timeout: Duration,
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
    result, timeout,
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
    if timeout > ZeroDuration:
      try:
        result = await queryDirectRunImpl(
          conn, sql, resultFormats, colFmts, colOids, cacheHit, cacheMiss, stmtName,
          cachedFields, timeout,
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("queryDirect timed out")
    else:
      result = await queryDirectRunImpl(
        conn, sql, resultFormats, colFmts, colOids, cacheHit, cacheMiss, stmtName,
        cachedFields, ZeroDuration,
      )

proc buildInvalidateOnOidMismatchStmt(
    connSym, sqlSym, cachedPtrSym, cacheHitSym: NimNode, positional: seq[NimNode]
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
    invalidateIfOidMismatch(
      `connSym`, `sqlSym`, `cachedPtrSym`, `bracket`, `cacheHitSym`
    )

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

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
  let timeoutSym = genSym(nskLet, "timeout")
  let cachedPtrSym = genSym(nskLet, "cachedPtr")
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
    `connSym`.state = csBusy

    let `cachedPtrSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedPtrSym` != nil
    var `cacheMissSym` = false
    var `stmtNameSym` = ""
    var `cachedFieldsSym`: seq[FieldDescription]
    var `effectiveRfSym`: seq[int16]
    var `colFmtsSym`: seq[int16]
    var `colOidsSym`: seq[int32]

  result.add buildInvalidateOnOidMismatchStmt(
    connSym, sqlSym, cachedPtrSym, cacheHitSym, positional
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
  for arg in positional:
    argList.add(arg)

  # Cache hit path
  let hitBlock = newStmtList()
  hitBlock.add quote do:
    `stmtNameSym` = `cachedPtrSym`.name
    `cachedFieldsSym` = `cachedPtrSym`.fields
    `colFmtsSym` = `cachedPtrSym`.colFmts
    `colOidsSym` = `cachedPtrSym`.colOids
    `effectiveRfSym` = `cachedPtrSym`.resultFormats
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
    queryDirectImpl(
      `connSym`, `sqlSym`, `effectiveRfSym`, `colFmtsSym`, `colOidsSym`, `cacheHitSym`,
      `cacheMissSym`, `stmtNameSym`, `cachedFieldsSym`, `timeoutSym`,
    )

proc execDirectRunImpl*(
    conn: PgConnection,
    sql: string,
    cacheHit: bool,
    cacheMiss: bool,
    stmtName: string,
    timeout: Duration,
): Future[string] {.async.} =
  ## Inner send + receive loop for execDirect. Returns the command tag and
  ## handles error reporting / cache bookkeeping. Split out so the outer
  ## Impl can apply ``.wait(timeout)`` without an extra closure alloc.
  await conn.sendBufMsg()
  var commandTag = ""
  var queryError: ref PgQueryError
  var cachedFields: seq[FieldDescription]
  var cachedParamOids: seq[int32]

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          if cacheMiss:
            cachedParamOids = msg.paramTypeOids
        of bmkRowDescription:
          if cacheMiss:
            cachedFields = msg.fields
        of bmkNoData:
          discard
        of bmkDataRow:
          discard
        of bmkCommandComplete:
          commandTag = msg.commandTag
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            if cacheHit and queryError.sqlState == "26000":
              conn.removeStmtCache(sql)
            raise queryError
          if cacheMiss:
            conn.addStmtCache(
              sql,
              CachedStmt(
                name: stmtName, fields: cachedFields, paramOids: cachedParamOids
              ),
            )
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

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
    if timeout > ZeroDuration:
      try:
        tag = await execDirectRunImpl(conn, sql, cacheHit, cacheMiss, stmtName, timeout)
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("execDirect timed out")
    else:
      tag =
        await execDirectRunImpl(conn, sql, cacheHit, cacheMiss, stmtName, ZeroDuration)
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

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
  let timeoutSym = genSym(nskLet, "timeout")
  let cachedPtrSym = genSym(nskLet, "cachedPtr")
  let cacheHitSym = genSym(nskVar, "cacheHit")
  let cacheMissSym = genSym(nskVar, "cacheMiss")
  let stmtNameSym = genSym(nskVar, "stmtName")

  result.add quote do:
    let `connSym` = `conn`
    let `sqlSym` = `sql`
    let `timeoutSym`: Duration = `timeoutExpr`
    `connSym`.checkReady()
    `connSym`.state = csBusy

    let `cachedPtrSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedPtrSym` != nil
    var `cacheMissSym` = false
    var `stmtNameSym` = ""

  result.add buildInvalidateOnOidMismatchStmt(
    connSym, sqlSym, cachedPtrSym, cacheHitSym, positional
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
  for arg in positional:
    argList.add(arg)

  let sendBufNode = newDotExpr(connSym, ident"sendBuf")

  # Cache hit path
  let hitBlock = newStmtList()
  hitBlock.add quote do:
    `stmtNameSym` = `cachedPtrSym`.name
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
    execDirectImpl(
      `connSym`, `sqlSym`, `cacheHitSym`, `cacheMissSym`, `stmtNameSym`, `timeoutSym`
    )
