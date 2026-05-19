## Zero-allocation `queryDirect` / `execDirect` compile-time macros that
## encode parameters directly into the connection send buffer.

import std/[macros, options, tables]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

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
  ## Shared receive path for queryDirect macros.
  await conn.sendBufMsg()
  var qr = QueryResult()
  var cf = cachedFields
  queryRecvLoop(
    conn, sql, resultFormats, cacheHit, cacheMiss, stmtName, cf, colFmts, colOids, qr,
    timeout,
  )
  return qr

macro queryDirect*(conn: PgConnection, sql: string, args: varargs[untyped]): untyped =
  ## Zero-allocation query: encodes parameters directly into the send buffer
  ## at compile time, avoiding ``seq[PgParam]`` and intermediate ``seq[byte]`` allocs.
  ##
  ## Usage: let qr = await conn.queryDirect("SELECT ... WHERE id = $1", myId)
  result = newStmtList()

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
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
  for arg in args:
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
      `cacheMissSym`, `stmtNameSym`, `cachedFieldsSym`, ZeroDuration,
    )

proc execDirectImpl*(
    conn: PgConnection,
    sql: string,
    cacheHit: bool,
    cacheMiss: bool,
    stmtName: string,
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Shared receive path for execDirect macro.
  await conn.sendBufMsg()
  var commandTag = ""
  var queryError: ref PgQueryError
  var cachedFields: seq[FieldDescription]

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          discard
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
            conn.addStmtCache(sql, CachedStmt(name: stmtName, fields: cachedFields))
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)

  return initCommandResult(commandTag)

macro execDirect*(conn: PgConnection, sql: string, args: varargs[untyped]): untyped =
  ## Zero-allocation exec: encodes parameters directly into the send buffer
  ## at compile time, avoiding ``seq[PgParam]`` and intermediate ``seq[byte]`` allocs.
  ##
  ## Usage: discard await conn.execDirect("UPDATE ... WHERE id = $1", myId)
  result = newStmtList()

  let connSym = genSym(nskLet, "conn")
  let sqlSym = genSym(nskLet, "sql")
  let cachedPtrSym = genSym(nskLet, "cachedPtr")
  let cacheHitSym = genSym(nskVar, "cacheHit")
  let cacheMissSym = genSym(nskVar, "cacheMiss")
  let stmtNameSym = genSym(nskVar, "stmtName")

  result.add quote do:
    let `connSym` = `conn`
    let `sqlSym` = `sql`
    `connSym`.checkReady()
    `connSym`.state = csBusy

    let `cachedPtrSym` = `connSym`.lookupStmtCache(`sqlSym`)
    var `cacheHitSym` = `cachedPtrSym` != nil
    var `cacheMissSym` = false
    var `stmtNameSym` = ""

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
  for arg in args:
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
      `connSym`, `sqlSym`, `cacheHitSym`, `cacheMissSym`, `stmtNameSym`, ZeroDuration
    )
