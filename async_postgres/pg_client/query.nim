## `query` overloads and result-shape convenience wrappers (`queryRow`,
## `queryValue`, `queryExists`, `queryColumn`) on top of the extended-query
## protocol. Also hosts the row-streaming `queryEach` entry point.

import std/[options, tables]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ./core

proc queryImpl*(
    conn: PgConnection,
    sql: string,
    params: seq[Option[seq[byte]]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
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
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    conn.sendBuf.addBind("", stmtName, formats, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, paramOids)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, formats, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    effectiveResultFormats = resultFormats
    conn.sendBuf.addParse("", sql, paramOids)
    conn.sendBuf.addBind("", "", formats, params, effectiveResultFormats)
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var qr = QueryResult()
  queryRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, qr, timeout,
  )
  return qr

proc queryImpl*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  conn.invalidateIfOidMismatch(sql, cached, params, cacheHit)
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, params)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    effectiveResultFormats = resultFormats
    conn.sendBuf.addParse("", sql, params)
    conn.sendBuf.addBind("", "", params, effectiveResultFormats)
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var qr = QueryResult()
  queryRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, qr, timeout,
  )
  return qr

proc queryEachImpl*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  conn.invalidateIfOidMismatch(sql, cached, params, cacheHit)
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]

  var effectiveResultFormats: seq[int16]

  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, params)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBind("", stmtName, params, effectiveResultFormats)
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    effectiveResultFormats = resultFormats
    conn.sendBuf.addParse("", sql, params)
    conn.sendBuf.addBind("", "", params, effectiveResultFormats)
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var rowCount: int64 = 0
  queryEachRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, callback, rowCount, timeout,
  )
  return rowCount

proc queryEach*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[int64] {.async.} =
  ## Execute a query with typed parameters, invoking `callback` once per row.
  ## Returns the number of rows processed.
  ##
  ## The `Row` passed to `callback` is only valid for the duration of that
  ## single invocation: its backing buffer is reused for the next row as soon
  ## as the callback returns. To retain a row beyond the callback, call
  ## `row.clone()` to get a detached copy, or extract the column values you
  ## need into your own types before returning.
  var count: int64
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(rowCount: count),
  ):
    let resultFormats = resultFormat.toFormatCodes()
    if timeout > ZeroDuration:
      try:
        count = await queryEachImpl(conn, sql, params, callback, resultFormats, timeout)
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("queryEach timed out")
    else:
      count = await queryEachImpl(conn, sql, params, callback, resultFormats)
  return count

proc query*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with typed parameters via the extended query protocol.
  ##
  ## Single statement only; the plan is cached per-connection. Use
  ## ``simpleQuery`` when you need multiple ``;``-separated statements to run
  ## in one round trip (no parameters, text-only rows).
  ##
  ## On timeout the connection is marked closed (protocol desync) and cannot be
  ## reused; pooled connections are discarded automatically.
  var qr: QueryResult
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, params: params, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: qr.commandTag, rowCount: qr.rowCount),
  ):
    let resultFormats = resultFormat.toFormatCodes()
    if timeout > ZeroDuration:
      try:
        qr = await queryImpl(conn, sql, params, resultFormats, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Query timed out")
    else:
      qr = await queryImpl(conn, sql, params, resultFormats)
  return qr

proc queryInlineImpl*(
    conn: PgConnection,
    sql: string,
    data: seq[byte],
    ranges: seq[tuple[off: int32, len: int32]],
    paramOids: seq[int32],
    paramFormats: seq[int16],
    resultFormats: seq[int16] = @[],
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  conn.checkReady()
  conn.state = csBusy

  let cached = conn.lookupStmtCache(sql)
  var cacheHit = cached != nil
  conn.invalidateIfOidMismatch(sql, cached, paramOids, cacheHit)
  var cacheMiss = false
  var stmtName = ""
  var cachedFields: seq[FieldDescription]
  var cachedColFmts: seq[int16]
  var cachedColOids: seq[int32]
  var effectiveResultFormats: seq[int16]

  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    conn.sendBuf.addBindRaw(
      "", stmtName, paramFormats, data, ranges, effectiveResultFormats
    )
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    conn.sendBuf.addParse(stmtName, sql, paramOids)
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    conn.sendBuf.addBindRaw(
      "", stmtName, paramFormats, data, ranges, effectiveResultFormats
    )
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()
  else:
    effectiveResultFormats = resultFormats
    conn.sendBuf.addParse("", sql, paramOids)
    conn.sendBuf.addBindRaw("", "", paramFormats, data, ranges, effectiveResultFormats)
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
    await conn.sendBufMsg()

  var qr = QueryResult()
  queryRecvLoop(
    conn, sql, effectiveResultFormats, cacheHit, cacheMiss, stmtName, cachedFields,
    cachedColFmts, cachedColOids, qr, timeout,
  )
  return qr

proc query*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParamInline],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a query with heap-alloc-free inline parameters.
  ## Prefer this overload for scalar-heavy workloads where `seq[PgParam]`
  ## would heap-allocate per parameter.
  let (data, ranges, oids, formats) = flattenInline(params)
  var qr: QueryResult
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, paramsInline: params, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: qr.commandTag, rowCount: qr.rowCount),
  ):
    let resultFormats = resultFormat.toFormatCodes()
    if timeout > ZeroDuration:
      try:
        qr = await queryInlineImpl(
          conn, sql, data, ranges, oids, formats, resultFormats, timeout
        )
          .wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Query timed out")
    else:
      qr = await queryInlineImpl(conn, sql, data, ranges, oids, formats, resultFormats)
  return qr

proc queryRowOpt*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Option[Row]] {.async.} =
  ## Execute a query and return the first row, or `none` if no rows.
  let qr = await conn.query(sql, params, resultFormat = resultFormat, timeout = timeout)
  if qr.rowCount > 0:
    return some(initRow(qr.data, 0))
  else:
    return none(Row)

proc queryRow*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[Row] {.async.} =
  ## Execute a query and return the first row.
  ## Raises `PgNoRowsError` if no rows are returned.
  let row =
    await conn.queryRowOpt(sql, params, resultFormat = resultFormat, timeout = timeout)
  if row.isNone:
    raise newException(PgNoRowsError, "Query returned no rows")
  return row.get

proc queryValue*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Raises `PgNoRowsError` if no rows are returned, or `PgNullError` if the value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgNoRowsError, "Query returned no rows")
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    raise newException(PgNullError, "Query returned NULL")
  return row.getStr(0)

proc queryValue*[T](
    conn: PgConnection,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Raises `PgNoRowsError` if no rows are returned, or `PgNullError` if the value is NULL.
  ## Supported types: int32, int64, float64, bool, string.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    raise newException(PgNoRowsError, "Query returned no rows")
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    raise newException(PgNullError, "Query returned NULL")
  return row.get(0, T)

proc queryValueOpt*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[string]] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `none` if no rows are returned or the value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return none(string)
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return none(string)
  return some(row.getStr(0))

proc queryValueOpt*[T](
    conn: PgConnection,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[Option[T]] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Returns `none` if no rows are returned or the value is NULL.
  ## Supported types: int32, int64, float64, bool, string.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return none(T)
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return none(T)
  return some(row.get(0, T))

proc queryValueOrDefault*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    default: string = "",
    timeout: Duration = ZeroDuration,
): Future[string] {.async.} =
  ## Execute a query and return the first column of the first row as a string.
  ## Returns `default` if no rows or the value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return default
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return default
  return row.getStr(0)

proc queryValueOrDefault*[T](
    conn: PgConnection,
    _: typedesc[T],
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`.
  ## Returns `default` if no rows or the value is NULL.
  ## Supported types: int32, int64, float64, bool, string.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return default
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return default
  return row.get(0, T)

proc queryValueOrDefault*[T](
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    default: T,
    timeout: Duration = ZeroDuration,
): Future[T] {.async.} =
  ## Execute a query and return the first column of the first row as `T`,
  ## inferring `T` from `default`.
  ## Returns `default` if no rows or the value is NULL.
  ## Supported types: int32, int64, float64, bool, string.
  let qr = await conn.query(sql, params, timeout = timeout)
  if qr.rowCount == 0:
    return default
  let row = initRow(qr.data, 0)
  if row.isNull(0):
    return default
  return row.get(0, T)

proc queryExists*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[bool] {.async.} =
  ## Execute a query and return whether any rows exist.
  let qr = await conn.query(sql, params, timeout = timeout)
  return qr.rowCount > 0

proc queryColumn*(
    conn: PgConnection,
    sql: string,
    params: seq[PgParam] = @[],
    timeout: Duration = ZeroDuration,
): Future[seq[string]] {.async.} =
  ## Execute a query and return the first column of all rows as strings.
  ## Raises `PgNullError` if any value is NULL.
  let qr = await conn.query(sql, params, timeout = timeout)
  for i in 0 ..< qr.rowCount:
    let row = initRow(qr.data, i)
    if row.isNull(0):
      raise newException(PgNullError, "NULL value in column")
    result.add(row.getStr(0))
