import std/[options, macros, strutils]

import async_backend, pg_protocol, pg_connection, pg_types, pg_pool, pg_client

type
  ReplicaFallback* = enum
    fallbackNone ## Error when replica is unavailable
    fallbackPrimary ## Fall back to primary when replica is unavailable

  PgPoolCluster* = ref object
    ## Connection pool cluster with explicit read/write routing.
    ##
    ## - `read*` methods route to the replica pool (read-only queries).
    ## - `write*` methods route to the primary pool (writes, `SELECT FOR UPDATE`, etc.).
    ##
    ## For transactions, use `cluster.primaryPool.withTransaction` directly.
    primary: PgPool
    replica: PgPool
    fallback: ReplicaFallback
    fallbackTimeout: Duration
      ## Max time to wait for a fallback acquire (ZeroDuration = use pool's own acquireTimeout)
    closed: bool

proc primaryPool*(cluster: PgPoolCluster): PgPool =
  ## The primary (read-write) pool.
  cluster.primary

proc replicaPool*(cluster: PgPoolCluster): PgPool =
  ## The replica (read-only) pool.
  cluster.replica

proc replicaFallback*(cluster: PgPoolCluster): ReplicaFallback =
  ## The configured replica fallback behavior.
  cluster.fallback

proc isClosed*(cluster: PgPoolCluster): bool =
  ## Whether the cluster has been closed.
  cluster.closed

proc fallbackTimeout*(cluster: PgPoolCluster): Duration =
  ## The configured fallback timeout for read operations.
  ## When `fallbackPrimary` is set and the replica acquire fails, this limits
  ## how long the fallback primary acquire may wait. `ZeroDuration` means the
  ## primary pool's own `acquireTimeout` is used as-is.
  cluster.fallbackTimeout

proc newPoolCluster*(
    primaryConfig: PoolConfig,
    replicaConfig: PoolConfig,
    fallback = fallbackNone,
    fallbackTimeout = ZeroDuration,
): Future[PgPoolCluster] {.async.} =
  ## Create a new pool cluster with separate primary and replica pools.
  ## If `connConfig.targetSessionAttrs` is `tsaAny` (the default), it is
  ## automatically set to `tsaReadWrite` for primary and `tsaPreferStandby`
  ## for replica.
  var pCfg = primaryConfig
  if pCfg.connConfig.targetSessionAttrs == tsaAny:
    pCfg.connConfig.targetSessionAttrs = tsaReadWrite

  var rCfg = replicaConfig
  if rCfg.connConfig.targetSessionAttrs == tsaAny:
    rCfg.connConfig.targetSessionAttrs = tsaPreferStandby

  let pPool = await newPool(pCfg)
  var rPool: PgPool
  try:
    rPool = await newPool(rCfg)
  except CatchableError as e:
    await pPool.close()
    raise e

  return PgPoolCluster(
    primary: pPool,
    replica: rPool,
    fallback: fallback,
    fallbackTimeout: fallbackTimeout,
    closed: false,
  )

proc acquireRead(
    cluster: PgPoolCluster
): Future[tuple[conn: PgConnection, pool: PgPool]] {.async.} =
  ## Acquire a connection for read operations. Tries replica first;
  ## falls back to primary if configured.
  try:
    let conn = await cluster.replica.acquire()
    return (conn, cluster.replica)
  except CatchableError as e:
    if cluster.fallback == fallbackPrimary:
      if cluster.fallbackTimeout > ZeroDuration:
        try:
          let conn = await cluster.primary.acquire().wait(cluster.fallbackTimeout)
          return (conn, cluster.primary)
        except AsyncTimeoutError:
          raise newException(
            PgPoolError,
            "Pool cluster fallback acquire timeout (replica error: " & e.msg & ")",
            e,
          )
      else:
        let conn = await cluster.primary.acquire()
        return (conn, cluster.primary)
    raise e

template withReadConnection*(cluster: PgPoolCluster, conn, body: untyped) =
  ## Acquire a read connection (from replica, with optional primary fallback),
  ## execute `body`, then release.
  block:
    let (conn, connPool) = await acquireRead(cluster)
    try:
      body
    finally:
      await connPool.resetSession(conn)
      connPool.release(conn)

template withWriteConnection*(cluster: PgPoolCluster, conn, body: untyped) =
  ## Acquire a write connection from the primary pool, execute `body`, then release.
  block:
    let conn = await cluster.primary.acquire()
    try:
      body
    finally:
      await cluster.primary.resetSession(conn)
      cluster.primary.release(conn)

# Macro to generate cluster forwarding procs from compact declarations.
# Each entry is a bodiless `proc` whose name starts with "read" or "write".
# The prefix is stripped to derive the connection-level method name.
# `_: typedesc[T]` parameters are forwarded as `T`.

macro clusterForwards(mode: static[string], body: untyped): untyped =
  result = newStmtList()
  for child in body:
    child.expectKind(nnkProcDef)
    let name = child[0]
    let genericParams = child[2]
    let formalParams = child[3]

    var procIdent =
      if name.kind == nnkPostfix:
        name[1]
      else:
        name
    let nameStr = $procIdent
    var innerStr = nameStr[mode.len .. ^1]
    innerStr[0] = toLowerAscii(innerStr[0])

    # Build inner call: conn.innerMethod(params...)
    var call = newCall(newDotExpr(ident"conn", ident(innerStr)))
    for i in 2 ..< formalParams.len:
      let param = formalParams[i]
      if param[1].kind == nnkBracketExpr and param[1][0].eqIdent("typedesc"):
        call.add(param[1][1])
      else:
        call.add(param[0])

    # Check if return type is Future[void]
    let retType = formalParams[0]
    let isVoid =
      retType.kind == nnkBracketExpr and retType.len > 1 and retType[1].eqIdent("void")

    # Build: await call OR return await call
    let awaitCall = newNimNode(nnkCommand).add(ident"await", call)
    var tryStmt =
      if isVoid:
        newStmtList(awaitCall)
      else:
        newStmtList(newNimNode(nnkReturnStmt).add(awaitCall))

    # Build acquire and finally based on mode
    var acquireStmt, finallyBody: NimNode

    if mode == "read":
      let acquireCall = newNimNode(nnkCommand).add(
          ident"await", newCall(ident"acquireRead", ident"cluster")
        )
      acquireStmt = newNimNode(nnkLetSection).add(
          newNimNode(nnkVarTuple).add(
            ident"conn", ident"pool", newEmptyNode(), acquireCall
          )
        )
      finallyBody = newStmtList(
        newNimNode(nnkCommand).add(
          ident"await",
          newCall(newDotExpr(ident"pool", ident"resetSession"), ident"conn"),
        ),
        newCall(newDotExpr(ident"pool", ident"release"), ident"conn"),
      )
    else:
      let primary = newDotExpr(ident"cluster", ident"primary")
      let acquireCall = newNimNode(nnkCommand).add(
          ident"await", newCall(newDotExpr(primary, ident"acquire"))
        )
      acquireStmt = newNimNode(nnkLetSection).add(
          newIdentDefs(ident"conn", newEmptyNode(), acquireCall)
        )
      finallyBody = newStmtList(
        newNimNode(nnkCommand).add(
          ident"await",
          newCall(newDotExpr(primary.copyNimTree(), ident"resetSession"), ident"conn"),
        ),
        newCall(newDotExpr(primary.copyNimTree(), ident"release"), ident"conn"),
      )

    let tryFinally =
      newNimNode(nnkTryStmt).add(tryStmt, newNimNode(nnkFinally).add(finallyBody))

    var procBody = newStmtList(acquireStmt, tryFinally)

    var newProc = newNimNode(nnkProcDef)
    newProc.add(name.copyNimTree())
    newProc.add(newEmptyNode())
    newProc.add(genericParams.copyNimTree())
    newProc.add(formalParams.copyNimTree())
    newProc.add(newNimNode(nnkPragma).add(ident"async"))
    newProc.add(newEmptyNode())
    newProc.add(procBody)
    result.add(newProc)

# Read operations → replica

clusterForwards("read"):
  proc readQuery*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[QueryResult]

  proc readQueryOne*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[Option[Row]]

  proc readQueryRow*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[Row]

  proc readQueryValue*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[string]

  proc readQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[T]

  proc readQueryValueOpt*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[Option[string]]

  proc readQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[Option[T]]

  proc readQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
  ): Future[string]

  proc readQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
  ): Future[T]

  proc readQueryExists*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[bool]

  proc readQueryColumn*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[seq[string]]

  proc readQueryEach*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[int64]

  proc readSimpleQuery*(cluster: PgPoolCluster, sql: string): Future[seq[QueryResult]]

# Write operations → primary

clusterForwards("write"):
  proc writeExec*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[CommandResult]

  proc writeQuery*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[QueryResult]

  proc writeQueryOne*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[Option[Row]]

  proc writeQueryRow*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[Row]

  proc writeQueryValue*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[string]

  proc writeQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[T]

  proc writeQueryValueOpt*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[Option[string]]

  proc writeQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[Option[T]]

  proc writeQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
  ): Future[string]

  proc writeQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
  ): Future[T]

  proc writeQueryExists*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[bool]

  proc writeQueryColumn*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[seq[string]]

  proc writeQueryEach*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[int64]

  proc writeExecInTransaction*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
  ): Future[CommandResult]

  proc writeQueryInTransaction*(
    cluster: PgPoolCluster,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): Future[QueryResult]

  proc writeSimpleQuery*(cluster: PgPoolCluster, sql: string): Future[seq[QueryResult]]

  proc writeSimpleExec*(
    cluster: PgPoolCluster, sql: string, timeout: Duration = ZeroDuration
  ): Future[CommandResult]

  proc writeNotify*(
    cluster: PgPoolCluster,
    channel: string,
    payload: string = "",
    timeout: Duration = ZeroDuration,
  ): Future[void]

template withPipeline*(cluster: PgPoolCluster, pipeline, body: untyped) =
  ## Create a pipeline on the primary pool.
  cluster.primary.withPipeline(pipeline):
    body

proc close*(cluster: PgPoolCluster, timeout = ZeroDuration): Future[void] {.async.} =
  ## Close both primary and replica pools.
  cluster.closed = true
  var firstErr: ref CatchableError
  try:
    await cluster.primary.close(timeout)
  except CatchableError as e:
    firstErr = e
  try:
    await cluster.replica.close(timeout)
  except CatchableError as e:
    if firstErr == nil:
      firstErr = e
  if firstErr != nil:
    raise firstErr
