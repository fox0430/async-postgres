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
## - `checkSessionAttrs` — `SHOW transaction_read_only` probe used by the
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

import std/[options, strutils]

import ../[async_backend, pg_errors, pg_protocol, pg_types]
import types, buffer_io

if hasAsyncDispatch:
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
  ## Assert that the connection is in `csReady` state. Raises `PgConnectionError` otherwise.
  if conn.state != csReady:
    raise newException(
      PgConnectionError, "Connection is not ready (state: " & $conn.state & ")"
    )

# Identifier escaping

proc quoteIdentifier*(s: string): string =
  ## Quote a SQL identifier (e.g. table/channel name) with double quotes, escaping embedded quotes.
  "\"" & s.replace("\"", "\"\"") & "\""

# Simple Query Protocol entry points

proc simpleQuery*(conn: PgConnection, sql: string): Future[seq[QueryResult]] {.async.} =
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
  conn.checkReady()

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
    conn.state = csBusy
    await conn.sendMsg(encodeQuery(sql))

    var current = QueryResult()
    var queryError: ref PgQueryError

    block recvLoop:
      while true:
        while (
          let opt = conn.nextMessage(current.data, addr current.rowCount)
          opt.isSome
        )
        :
          let msg = opt.get
          case msg.kind
          of bmkRowDescription:
            current =
              QueryResult(fields: msg.fields, data: newRowData(int16(msg.fields.len)))
          of bmkCommandComplete:
            current.commandTag = msg.commandTag
            results.add(current)
            current = QueryResult()
          of bmkEmptyQueryResponse:
            results.add(QueryResult())
          of bmkErrorResponse:
            queryError = newPgQueryError(msg.errorFields)
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            if queryError != nil:
              raise queryError
            break recvLoop
          else:
            discard
        await conn.fillRecvBuf()

    for r in results:
      totalRows += r.rowCount
      if r.commandTag.len > 0:
        lastTag = r.commandTag

  return results

proc simpleExecImpl*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[string] {.async.} =
  conn.checkReady()
  conn.state = csBusy
  await conn.sendMsg(encodeQuery(sql))
  var commandTag = ""
  var queryError: ref PgQueryError
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCommandComplete:
          commandTag = msg.commandTag
        of bmkRowDescription, bmkDataRow, bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break recvLoop
        else:
          discard
      await conn.fillRecvBuf(timeout)
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
  ## Under asyncdispatch this is the **only** safe recovery path: the inner
  ## future keeps running in the background after `wait()` fires, and may
  ## still write to the socket. Reusing the connection would interleave its
  ## stale write with a new request and corrupt the protocol stream. chronos
  ## cancels the inner future properly, but we still invalidate unconditionally
  ## — the server may have processed the request partially and the cached
  ## session state (prepared statements, portals, transaction status) is no
  ## longer reliable.
  conn.cancelNoWait()
  conn.state = csClosed
  raise newException(PgTimeoutError, reason)

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
    if timeout > ZeroDuration:
      try:
        tag = await simpleExecImpl(conn, sql, timeout).wait(timeout)
      except AsyncTimeoutError:
        conn.invalidateOnTimeout("simpleExec timed out")
    else:
      tag = await simpleExecImpl(conn, sql)
  return initCommandResult(tag)

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

    var queryError: ref PgQueryError
    block recvLoop:
      while true:
        while (let opt = conn.nextMessage(); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkEmptyQueryResponse:
            discard
          of bmkErrorResponse:
            queryError = newPgQueryError(msg.errorFields)
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            conn.state = csReady
            if queryError != nil:
              raise queryError
            break recvLoop
          else:
            discard
        await conn.fillRecvBuf(timeout)

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

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i in 0 ..< data.len:
    result[i] = char(data[i])

proc checkSessionAttrs*(
    conn: PgConnection, attrs: TargetSessionAttrs
): Future[bool] {.async.} =
  ## Check whether a connection matches the desired target_session_attrs.
  ## Uses `SHOW transaction_read_only` to determine server role.
  if attrs == tsaAny:
    return true
  let results = await conn.simpleQuery("SHOW transaction_read_only")
  var readOnly = false
  if results.len > 0 and results[0].rowCount > 0:
    let val = results[0].rows[0][0]
    if val.isSome:
      readOnly = bytesToString(val.get) == "on"
  case attrs
  of tsaAny:
    true # unreachable, handled above
  of tsaReadWrite, tsaPrimary:
    not readOnly
  of tsaReadOnly, tsaStandby, tsaPreferStandby:
    readOnly
