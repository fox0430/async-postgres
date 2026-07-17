## Simple Query Protocol entry points and the cancellation helpers built
## on top of them.
##
## Contains:
## - `checkReady` — assertion used by every operation that requires `csReady`.
## - `simpleQuery` / `simpleExec` / `ping` — text-mode multi-statement and
##   single-statement query/exec via the simple query protocol.
## - `cancel` / `cancelNoWait` / `invalidateOnTimeout` — out-of-band cancel
##   request over a separate socket plus the standard "the wait timed out,
##   poison this connection" recovery path used by every timeout wrapper.
## - `checkSessionAttrs` — server-role probe (`SHOW transaction_read_only` /
##   `in_hot_standby` / `SELECT pg_catalog.pg_is_in_recovery()`) used by the
##   multi-host failover logic in `lifecycle.connect`.
## - `quoteIdentifier` — SQL identifier escaping used by `LISTEN`/`UNLISTEN`
##   and other identifier-bearing simple-query call sites.
## - `QueryResult` helpers (`len`, `columnIndex`, `rows`, `items`).
##
## Sits between `buffer_io`/`cache` (which it consumes) and `lifecycle`
## (which depends on `checkSessionAttrs` and the cancel helpers), so it
## avoids circular imports.
##
## Re-exported through `pg_connection.nim`.

import std/[options, strutils, tables]

import ../[async_backend, pg_errors, pg_protocol, pg_types]
import types, buffer_io

when hasAsyncDispatch:
  import std/asyncnet
  from std/nativesockets import Domain, SockType, Protocol

# QueryResult helpers

proc len*(qr: QueryResult): int {.inline.} =
  ## Return the number of rows in the query result.
  int(qr.rowCount)

proc columnIndex*(qr: QueryResult, name: string): int =
  ## Find the index of a column by name in a query result.
  qr.fields.columnIndex(name)

proc rows*(qr: QueryResult): seq[Row] =
  ## Return all rows as lightweight Row views into the flat buffer.
  if qr.data == nil:
    return @[]
  if qr.fields.len > 0 and qr.data.fields.len == 0:
    qr.data.fields = qr.fields
  result = newSeq[Row](qr.rowCount)
  for i in 0 ..< qr.rowCount:
    result[i] = initRow(qr.data, i)

iterator items*(qr: QueryResult): Row =
  ## Iterate over all rows in the query result.
  if qr.data != nil:
    if qr.fields.len > 0 and qr.data.fields.len == 0:
      qr.data.fields = qr.fields
    for i in 0 ..< qr.rowCount:
      yield initRow(qr.data, i)

# State assertion

proc checkReady*(conn: PgConnection) =
  ## Assert that the connection is in `csReady` before starting an operation.
  ##
  ## A `csClosed` connection is genuinely gone, so this raises
  ## `PgConnectionError` — reconnecting is the correct recovery. Any other
  ## non-ready state (`csBusy`, `csReplicating`, …) means the connection is
  ## alive but already in use, almost always a single connection driven
  ## concurrently; that raises `PgStateError` (a programming error), which is
  ## *not* a `PgConnectionError` and so never feeds a reconnect-on-failure loop.
  if conn.state == csReady:
    return
  if conn.state == csClosed:
    raise newException(PgConnectionError, "Connection is closed")
  raise newException(
    PgStateError,
    "Connection is not ready (state: " & $conn.state &
      "); a single connection cannot be used concurrently",
  )

proc checkTxIdle*(conn: PgConnection) =
  ## Reject entry to a top-level BEGIN/COMMIT scope when a transaction is
  ## already active: nested BEGIN is a server-side no-op, so the inner COMMIT
  ## would confirm the outer transaction's work. Use `withSavepoint` to nest.
  if conn.txStatus == tsIdle:
    return
  raise newException(
    PgStateError,
    "Connection already has an active transaction (txStatus: " & $conn.txStatus &
      "); use withSavepoint for nested scopes",
  )

# Identifier escaping

proc quoteIdentifier*(s: string): string =
  ## Quote a SQL identifier (e.g. table/channel name) with double quotes, escaping embedded quotes.
  "\"" & s.replace("\"", "\"\"") & "\""

# Simple Query Protocol entry points

proc simpleQueryImpl*(
    conn: PgConnection, sql: string
): Future[seq[QueryResult]] {.async.} =
  conn.checkReady()
  let msg = encodeQuery(sql)
  conn.state = csBusy
  await conn.sendMsg(msg)

  var results: seq[QueryResult]
  var current = QueryResult()

  conn.pumpUntilReady(current.data, addr current.rowCount):
    case pumpMsg.kind
    of bmkRowDescription:
      current =
        QueryResult(fields: pumpMsg.fields, data: newRowData(int16(pumpMsg.fields.len)))
    of bmkCommandComplete:
      current.commandTag = pumpMsg.commandTag
      results.add(current)
      current = QueryResult()
    of bmkEmptyQueryResponse:
      results.add(QueryResult())
    else:
      discard
  do:
    discard

  return results

proc simpleExecImpl*(conn: PgConnection, sql: string): Future[string] {.async.} =
  conn.checkReady()
  let msg = encodeQuery(sql)
  conn.state = csBusy
  await conn.sendMsg(msg)
  var commandTag = ""
  conn.pumpUntilReady:
    case pumpMsg.kind
    of bmkCommandComplete:
      commandTag = pumpMsg.commandTag
    of bmkRowDescription, bmkEmptyQueryResponse:
      discard
    else:
      discard
  do:
    discard
  return commandTag

# Cancellation (out-of-band CancelRequest over a separate socket)

proc cancel*(conn: PgConnection): Future[void] {.async.} =
  ## Send a CancelRequest over a separate connection to abort the running query.
  let isUnix = isUnixSocket(conn.host)
  when hasChronos:
    let transport =
      if isUnix:
        when defined(posix):
          await connect(initTAddress(unixSocketPath(conn.host, conn.port)))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        let addresses = resolveTAddress(conn.host, Port(conn.port))
        if addresses.len == 0:
          raise newException(PgConnectionError, "Could not resolve host: " & conn.host)
        await connect(addresses[0])
    try:
      let msg = encodeCancelRequest(conn.pid, conn.secretKey)
      discard await transport.write(msg)
    finally:
      await transport.closeWait()
  elif hasAsyncDispatch:
    let sock =
      if isUnix:
        when defined(posix):
          newAsyncSocket(
            Domain.AF_UNIX, SockType.SOCK_STREAM, Protocol.IPPROTO_IP, buffered = false
          )
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        newAsyncSocket(buffered = false)
    try:
      if isUnix:
        when defined(posix):
          await sock.connectUnix(unixSocketPath(conn.host, conn.port))
        else:
          raise newException(
            PgConnectionError, "Unix sockets are not supported on this platform"
          )
      else:
        await sock.connect(conn.host, Port(conn.port))
      let msg = encodeCancelRequest(conn.pid, conn.secretKey)
      await sock.sendRawBytes(msg)
    finally:
      sock.close()

proc cancelNoWait*(conn: PgConnection) =
  ## Schedule a best-effort CancelRequest without waiting. For use in timeout handlers.
  proc doCancel() {.async.} =
    try:
      await conn.cancel()
    except CatchableError:
      discard

  asyncSpawn doCancel()

proc invalidateOnTimeout*(conn: PgConnection, reason: string) =
  ## Timeout recovery for a connection whose last request may have left the
  ## protocol out of sync. Schedules a best-effort CancelRequest via
  ## `cancelNoWait`, marks the connection `csClosed` so it cannot be reused,
  ## and raises `PgTimeoutError` with `reason`.
  ##
  ## Under asyncdispatch the inner future keeps running in the background after
  ## ``wait()`` times out (see ``wait()``'s ``onOrphan`` hook in
  ## ``async_backend``). Reusing the connection would interleave its stale
  ## write with a new request and corrupt the protocol stream, so we mark
  ## ``csClosed`` unconditionally — the server may have processed the request
  ## partially and the cached session state (prepared statements, portals,
  ## transaction status) is no longer reliable.
  conn.cancelNoWait()
  conn.state = csClosed
  raise newException(PgTimeoutError, reason)

template awaitOrInvalidate*(
    connExpr: PgConnection,
    dest: untyped,
    fut: untyped,
    timeout: Duration,
    reason: static string,
) =
  ## Await `fut` with optional `timeout`, assigning the result to `dest`.
  ## On timeout, invalidates `connExpr` via `invalidateOnTimeout` (marks
  ## csClosed, dispatches CancelRequest, raises PgTimeoutError).
  ##
  ## Consolidates the repeated `if timeout > ZeroDuration / try / except
  ## AsyncTimeoutError / else` pattern so a single-site omission cannot leave
  ## the connection poisoned for the next borrower — see the
  ## `invalidateOnTimeout` doc-comment for why in-flight timeouts must always
  ## retire the connection under asyncdispatch.
  if timeout > ZeroDuration:
    try:
      dest = await fut.wait(timeout)
    except AsyncTimeoutError:
      connExpr.invalidateOnTimeout(reason)
  else:
    dest = await fut

template awaitVoidOrInvalidate*(
    connExpr: PgConnection, fut: untyped, timeout: Duration, reason: static string
) =
  ## Void-returning variant of `awaitOrInvalidate` for `Future[void]` sites
  ## (e.g. `close` on a prepared statement or cursor).
  if timeout > ZeroDuration:
    try:
      await fut.wait(timeout)
    except AsyncTimeoutError:
      connExpr.invalidateOnTimeout(reason)
  else:
    await fut

proc simpleExec*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Execute a side-effect SQL command via the **simple query protocol**,
  ## returning the final command tag.
  ##
  ## Lighter than ``exec`` for parameter-less commands — one ``Query`` message,
  ## no Parse/Bind/Describe round trip and no plan cache entry. Intended for
  ## session-level commands such as ``BEGIN``, ``SET``, ``VACUUM``,
  ## ``LISTEN``, ``NOTIFY``.
  ##
  ## The SQL string is sent verbatim (no parameters) — only use trusted input,
  ## or quote interpolated identifiers yourself via ``quoteIdentifier``.
  ##
  ## Multiple ``;``-separated statements are accepted, but only the **last**
  ## command tag is returned; use ``simpleQuery`` if you need per-statement
  ## results. For parameterised writes, prefer ``exec``.
  ##
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var tag: string
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, isExec: true),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: tag),
  ):
    awaitOrInvalidate(
      conn, tag, simpleExecImpl(conn, sql), timeout, "simpleExec timed out"
    )
  return initCommandResult(tag)

proc simpleQuery*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[seq[QueryResult]] {.async.} =
  ## Execute one or more SQL statements via the **simple query protocol**.
  ##
  ## Returns one ``QueryResult`` per statement; supports multiple statements
  ## separated by ``;`` in a single round trip — this is the main reason to
  ## choose ``simpleQuery`` over ``query``.
  ##
  ## No parameters are supported (the SQL string is sent verbatim — only use
  ## trusted input) and rows are always in the text wire format. No
  ## server-side plan cache entry is created.
  ##
  ## For single-statement parameterised reads, prefer ``query``; for
  ## parameter-less commands without rows, prefer ``simpleExec``.
  ##
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var results: seq[QueryResult]
  var totalRows: int32
  var lastTag: string
  # For multi-statement queries (e.g. "SELECT 1; SELECT 2"), the trace end hook
  # receives the aggregated row count and only the last command tag.
  withConnTracing(
    conn,
    onQueryStart,
    onQueryEnd,
    TraceQueryStartData(sql: sql, isExec: false),
    TraceQueryEndData,
    TraceQueryEndData(commandTag: lastTag, rowCount: totalRows),
  ):
    awaitOrInvalidate(
      conn, results, simpleQueryImpl(conn, sql), timeout, "simpleQuery timed out"
    )

    for r in results:
      totalRows += r.rowCount
      if r.commandTag.len > 0:
        lastTag = r.commandTag

  return results

# Liveness check

proc ping*(conn: PgConnection, timeout = ZeroDuration): Future[void] =
  ## Lightweight health check using an empty simple query.
  ## Sends Query("") -> expects EmptyQueryResponse + ReadyForQuery.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  proc perform(): Future[void] {.async.} =
    conn.checkReady()
    if not conn.isConnected():
      conn.state = csClosed
      raise newException(PgConnectionError, "Connection is not established")
    conn.state = csBusy
    await conn.sendMsg(encodeQuery(""))

    conn.pumpUntilReady:
      case pumpMsg.kind
      of bmkEmptyQueryResponse: discard
      else: discard
    do:
      discard

  if timeout > ZeroDuration:
    proc withTimeout(): Future[void] {.async.} =
      try:
        await perform().wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("Ping timed out")

    withTimeout()
  else:
    perform()

# Multi-host failover probe

proc bytesToString*(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

proc probeBool(conn: PgConnection, sql, trueLiteral: string): Future[bool] {.async.} =
  ## Run a single-row, single-column probe and compare its text value against
  ## `trueLiteral`. Raise if the result carries no usable value (empty / zero
  ## rows / NULL) so an indeterminate probe fails the host rather than silently
  ## defaulting to a match — matching libpq, which advances to the next host.
  let results = await conn.simpleQuery(sql)
  if results.len > 0 and results[0].rowCount > 0:
    let val = results[0].rows[0][0]
    if val.isSome:
      return bytesToString(val.get) == trueLiteral
  raise
    newException(PgConnectionError, "probe \"" & sql & "\" returned no usable result")

proc isPhysicalReplicationConn(conn: PgConnection): bool =
  ## Physical replication connections cannot execute arbitrary SQL, so a
  ## recovery probe must avoid `SELECT`. `connectReplication` always sets
  ## `replication=true`, but the server also accepts the other boolean
  ## spellings, which can reach `extraParams` verbatim via a user DSN
  ## (`replication=database` is logical replication and *can* run SQL).
  for (k, v) in conn.config.extraParams:
    if k == "replication" and v in ["true", "on", "yes", "1"]:
      return true
  false

proc inRecovery(conn: PgConnection): Future[bool] {.async.} =
  ## Recovery state. PostgreSQL 14+ reports `in_hot_standby`, so no query is
  ## needed; older servers are probed with `SELECT pg_catalog.pg_is_in_recovery()`.
  ## Physical replication connections reject `SELECT`, so they fall back to the
  ## walsender-accepted `SHOW transaction_read_only`. Note: on a pre-14 physical
  ## replication connection that is a recovery-state approximation, not the
  ## exact recovery state — unavoidable when `in_hot_standby` is absent.
  let ihs = conn.serverParams.getOrDefault("in_hot_standby", "")
  if ihs.len > 0:
    return ihs == "on"
  if conn.isPhysicalReplicationConn:
    return await conn.probeBool("SHOW transaction_read_only", "on")
  return await conn.probeBool("SELECT pg_catalog.pg_is_in_recovery()", "t")

proc isReadOnly(conn: PgConnection): Future[bool] {.async.} =
  ## Read-only state. PostgreSQL 14+ reports both `default_transaction_read_only`
  ## and `in_hot_standby`, so the answer needs no round-trip (libpq parity);
  ## otherwise fall back to `SHOW transaction_read_only`.
  let dtro = conn.serverParams.getOrDefault("default_transaction_read_only", "")
  let ihs = conn.serverParams.getOrDefault("in_hot_standby", "")
  if dtro.len > 0 and ihs.len > 0:
    return dtro == "on" or ihs == "on"
  return await conn.probeBool("SHOW transaction_read_only", "on")

proc checkSessionAttrs*(
    conn: PgConnection, attrs: TargetSessionAttrs
): Future[bool] {.async.} =
  ## Check whether a connection matches the desired target_session_attrs.
  ## Follows libpq: `tsaReadWrite`/`tsaReadOnly` are judged on the session's
  ## read-only state, while `tsaPrimary`/`tsaStandby` are judged on the recovery
  ## state — the `in_hot_standby` ParameterStatus reported by PostgreSQL 14+,
  ## with a `SELECT pg_catalog.pg_is_in_recovery()` probe as fallback for older
  ## servers. `tsaPreferStandby` is permissive: as a standalone predicate any
  ## server matches (the multi-host failover in `lifecycle.connect` handles the
  ## standby preference with a two-pass scan). Raises on an indeterminate probe.
  case attrs
  of tsaAny, tsaPreferStandby:
    return true
  of tsaReadWrite:
    return not await conn.isReadOnly()
  of tsaReadOnly:
    return await conn.isReadOnly()
  of tsaPrimary:
    return not await conn.inRecovery()
  of tsaStandby:
    return await conn.inRecovery()
