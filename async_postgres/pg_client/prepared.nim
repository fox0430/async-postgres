## Named server-side prepared statements: `prepare`, `execute`, and `close`.
##
## Internal module: not part of the public API. Import the `pg_client` hub instead.

import std/[options, strutils]

import ../[async_backend, pg_protocol, pg_types]
import ../pg_connection/[types, buffer_io, simple_query]
import ../pg_types/encoding
import core

type PreparedStatement* = ref object
  ## A server-side prepared statement returned by `prepare`.
  ##
  ## A `ref` handle: copies refer to the same server-side statement.
  ##
  ## Valid only within the server session that prepared it. A session reset
  ## (``DISCARD ALL`` / ``DEALLOCATE``, or a pooled backend being recycled)
  ## drops the statement; a later `execute` then raises ``PgQueryError`` with
  ## SQLSTATE ``26000`` (invalid_sql_statement_name) and the caller must
  ## `prepare` it again. There is no transparent re-prepare here — that is
  ## reserved for the auto-prepare statement cache (see the cache path's
  ## ``StmtCacheInvalidatingStates`` handling in `pg_client/core`).
  ##
  ## Fields are private; use the `conn` / `name` / `sql` / `fields` /
  ## `paramOids` accessors for read-only access.
  conn: PgConnection
  name: string
  sql: string
  fields: seq[FieldDescription]
  noData: bool ## Describe answered ``NoData``: no rows, unlike zero columns.
  paramOids: seq[int32]

func conn*(stmt: PreparedStatement): PgConnection {.inline.} =
  ## The connection (server session) this statement was prepared on.
  stmt.conn

func name*(stmt: PreparedStatement): string {.inline.} =
  ## The server-side statement name given to `prepare`.
  stmt.name

func sql*(stmt: PreparedStatement): string {.inline.} =
  ## The SQL text this statement was prepared from.
  stmt.sql

func fields*(stmt: PreparedStatement): seq[FieldDescription] {.inline.} =
  ## Column descriptions of the statement's result rows.
  stmt.fields

func paramOids*(stmt: PreparedStatement): seq[int32] {.inline.} =
  ## Parameter type OIDs reported by the server at prepare time.
  stmt.paramOids

proc columnIndex*(stmt: PreparedStatement, name: string): int =
  ## Find the index of a column by name in a prepared statement.
  stmt.fields.columnIndex(name)

proc checkPreparedName(name: string) =
  ## Reject names another Parse on this session can replace: the library's own
  ## queries reuse the unnamed statement, and the statement cache owns
  ## ``stmtNamePrefix``.
  if name.len == 0:
    raise newException(
      PgTypeError,
      "prepared statement name must not be empty: " &
        "the unnamed statement is replaced by the next unnamed Parse",
    )
  if name.startsWith(stmtNamePrefix):
    raise newException(
      PgTypeError,
      "prepared statement name '" & name & "' starts with '" & stmtNamePrefix &
        "', which is reserved for the statement cache",
    )

proc prepareImpl*(
    conn: PgConnection, name: string, sql: string
): Future[PreparedStatement] {.async.} =
  conn.checkReady()
  # The name is the application's, not a generated `nextStmtName()`, so it is
  # checked for NUL and reserved names, and charged against the Parse envelope.
  checkNoNul(name, "prepared statement name")
  checkPreparedName(name)
  validateParseMsg(sql, nParams = 0, stmtNameLen = name.len)

  var batch = newSeqOfCap[byte](sql.len + name.len + 32)
  batch.addParse(name, sql)
  batch.addDescribe(dkStatement, name)
  batch.addSync()
  conn.markBusy()
  await conn.sendMsg(batch)

  var stmt = PreparedStatement(conn: conn, name: name, sql: sql)

  conn.pumpUntilReady:
    case pumpMsg.kind
    of bmkParseComplete:
      discard
    of bmkParameterDescription:
      stmt.paramOids = pumpMsg.paramTypeOids
    of bmkRowDescription:
      stmt.fields = pumpMsg.fields
    of bmkNoData:
      stmt.noData = true
    else:
      discard
  do:
    discard

  return stmt

proc prepare*(
    conn: PgConnection, name: string, sql: string, timeout: Duration = ZeroDuration
): Future[PreparedStatement] {.async.} =
  ## Prepare a named statement, returning metadata.
  ##
  ## Raises ``PgTypeError`` for an empty ``name`` or one starting with ``_sc_``
  ## (the statement cache's names): another Parse on the session could replace
  ## either.
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
  var stmt: PreparedStatement
  withConnTracing(
    conn,
    onPrepareStart,
    onPrepareEnd,
    TracePrepareStartData(name: name, sql: sql),
    TracePrepareEndData,
    TracePrepareEndData(),
  ):
    awaitOrInvalidate(
      conn, stmt, prepareImpl(conn, name, sql), timeout, "Prepare timed out"
    )
  return stmt

proc executeImpl*(
    stmt: PreparedStatement, params: seq[PgParam] = @[], resultFormats: seq[int16] = @[]
): Future[QueryResult] {.async.} =
  let conn = stmt.conn

  conn.checkReady()

  # Coerce binary parameters to match server-inferred types from prepare().
  var coerced: seq[PgParam]
  var needsCoercion = false
  if stmt.paramOids.len == params.len:
    for i in 0 ..< params.len:
      if params[i].format == 1 and params[i].oid != stmt.paramOids[i] and
          stmt.paramOids[i] != 0:
        if not needsCoercion:
          coerced = params
          needsCoercion = true
        coerced[i] = coerceBinaryParam(params[i], stmt.paramOids[i])

  # After coercion, which can change a value's encoded length. A cursor, so the
  # common path binds `params` rather than copying every parameter's payload:
  # both operands outlive it, and its readers take `openArray`.
  let effective {.cursor.} = if needsCoercion: coerced else: params
  validateTypedParams(effective, resultFormats.len, stmt.name.len)

  conn.beginSendBuf()
  conn.addBind("", stmt.name, effective, resultFormats)
  conn.addExecute("", 0)
  conn.addSync()
  conn.markBusy()
  await conn.sendStagedBufMsg()

  # Like a cache hit, Execute gets no RowDescription. `prepare`'s statement
  # Describe reported text for every column.
  var qr = QueryResult()
  if not stmt.noData:
    qr.initBoundResult(
      boundRowData(stmt.fields, deriveColFmts(resultFormats, stmt.fields.len))
    )
  conn.pumpUntilReady(qr.data, addr qr.rowCount):
    case pumpMsg.kind
    of bmkBindComplete:
      discard
    of bmkCommandComplete:
      qr.commandTag = pumpMsg.commandTag
    of bmkEmptyQueryResponse:
      discard
    else:
      discard
  do:
    discard

  return qr

proc execute*(
    stmt: PreparedStatement,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): Future[QueryResult] {.async.} =
  ## Execute a prepared statement with typed parameters.
  ##
  ## If the server session has lost the statement (``DISCARD ALL`` /
  ## ``DEALLOCATE``, or a pooled backend reset), this raises ``PgQueryError``
  ## with SQLSTATE ``26000``; recover by calling `prepare` again. The error is
  ## propagated, not retried — unlike the auto-prepare cache, an explicit
  ## `PreparedStatement` is never re-prepared transparently.
  var qr: QueryResult
  withConnTracing(
    stmt.conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: stmt.sql, params: params, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: qr.commandTag, rowCount: qr.rowCount),
  ):
    let resultFormats = resultFormat.toFormatCodes()
    awaitOrInvalidate(
      stmt.conn,
      qr,
      executeImpl(stmt, params, resultFormats),
      timeout,
      "Execute timed out",
    )
  return qr

proc closeImpl*(stmt: PreparedStatement): Future[void] {.async.} =
  let conn = stmt.conn

  conn.checkReady()

  var batch = newSeqOfCap[byte](stmt.name.len + 16)
  batch.addClose(dkStatement, stmt.name)
  batch.addSync()
  conn.markBusy()
  await conn.sendMsg(batch)

  conn.pumpUntilReady:
    case pumpMsg.kind
    of bmkCloseComplete: discard
    else: discard
  do:
    discard

proc close*(
    stmt: PreparedStatement, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Close a prepared statement.
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
  awaitVoidOrInvalidate(
    stmt.conn, closeImpl(stmt), timeout, "Statement close timed out"
  )
