## `exec` overloads: extended-query single-statement execution that ignores
## result rows and returns just the command tag (`CommandResult`).

import std/[options, tables]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

proc execImpl*(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let formats =
    if paramFormats.len > 0:
      paramFormats
    else:
      newSeq[int16](params.len)

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  conn.invalidateIfOidMismatch(sql, cached, paramOids, cacheHit)
  var cacheMiss = false
  var stmtName = ""

  sendExtendedExec(
    conn = conn,
    cached = cached,
    cacheHit = cacheHit,
    cacheMiss = cacheMiss,
    stmtName = stmtName,
    parseStep = conn.sendBuf.addParse(stmtName, sql, paramOids),
    bindStep = conn.sendBuf.addBind("", stmtName, formats, params),
  )
  await conn.sendBufMsg()

  var commandTag = ""
  execRecvLoop(conn, sql, cacheHit, cacheMiss, stmtName, commandTag)
  return commandTag

proc execImpl*(
    conn: PgConnection, sql: string, params: seq[PgParam] = @[]
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  conn.invalidateIfOidMismatch(sql, cached, params, cacheHit)
  var cacheMiss = false
  var stmtName = ""

  sendExtendedExec(
    conn = conn,
    cached = cached,
    cacheHit = cacheHit,
    cacheMiss = cacheMiss,
    stmtName = stmtName,
    parseStep = conn.sendBuf.addParse(stmtName, sql, params),
    bindStep = conn.sendBuf.addBind("", stmtName, params),
  )
  await conn.sendBufMsg()

  var commandTag = ""
  execRecvLoop(conn, sql, cacheHit, cacheMiss, stmtName, commandTag)
  return commandTag

proc exec*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with typed parameters via the extended query protocol.
  ##
  ## Single statement only; the plan is cached per-connection. Use
  ## ``simpleExec`` for parameter-less session commands (``BEGIN``, ``SET``,
  ## ``VACUUM``, ``LISTEN`` …) or ``simpleQuery`` when you need multi-statement
  ## execution in one round trip.
  ##
  ## On timeout the connection is marked closed (protocol desync) and cannot be
  ## reused; pooled connections are discarded automatically.
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    if timeout > ZeroDuration:
      try:
        tag = await execImpl(conn, sql, params).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Exec timed out")
    else:
      tag = await execImpl(conn, sql, params)
  return initCommandResult(tag)

proc execInlineImpl*(
    conn: PgConnection,
    sql: string,
    data: seq[byte],
    ranges: seq[tuple[off: int32, len: int32]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  conn.invalidateIfOidMismatch(sql, cached, paramOids, cacheHit)
  var cacheMiss = false
  var stmtName = ""

  sendExtendedExec(
    conn = conn,
    cached = cached,
    cacheHit = cacheHit,
    cacheMiss = cacheMiss,
    stmtName = stmtName,
    parseStep = conn.sendBuf.addParse(stmtName, sql, paramOids),
    bindStep = conn.sendBuf.addBindRaw("", stmtName, paramFormats, data, ranges),
  )
  await conn.sendBufMsg()

  var commandTag = ""
  execRecvLoop(conn, sql, cacheHit, cacheMiss, stmtName, commandTag)
  return commandTag

proc exec*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParamInline],
    timeout: Duration = ZeroDuration,
): Future[CommandResult] {.async.} =
  ## Execute a statement with heap-alloc-free inline parameters.
  ## Prefer this overload for scalar-heavy workloads (e.g. bulk INSERT of
  ## numeric columns) where `seq[PgParam]` would heap-allocate per parameter.
  let (data, ranges, oids, formats) = flattenInline(params)
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, paramsInline: params, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    if timeout > ZeroDuration:
      try:
        tag = await execInlineImpl(conn, sql, data, ranges, oids, formats).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Exec timed out")
    else:
      tag = await execInlineImpl(conn, sql, data, ranges, oids, formats)
  return initCommandResult(tag)

proc notify*(
    conn: PgConnection,
    channel: string,
    payload: string = "",
    timeout: Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Send a NOTIFY on `channel` with optional `payload`.
  ## Uses NOTIFY for empty payloads, pg_notify() otherwise.
  let quoted = quoteIdentifier(channel)
  if payload.len == 0:
    discard await conn.exec("NOTIFY " & quoted, timeout = timeout)
  else:
    discard await conn.exec(
      "SELECT pg_notify($1, $2)",
      @[channel.toPgParam, payload.toPgParam],
      timeout = timeout,
    )
