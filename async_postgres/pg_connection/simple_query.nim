## Simple Query Protocol: ``simpleQuery``/``simpleExec``/``ping``, ``checkReady``,
## cancel helpers (``cancel``/``invalidateOnTimeout``), ``checkSessionAttrs``,
## and ``quoteIdentifier``. Layer between ``buffer_io`` and ``lifecycle``.
##
## Internal module: not part of the public API. Import the `pg_connection` hub
## instead; what it re-exports is the supported surface (see
## `tests/api_surface.golden`).

import std/[options, strutils, tables]

import ../[async_backend, pg_errors, pg_protocol, pg_types]
import types, buffer_io

when hasAsyncDispatch:
  import std/asyncnet
  from std/nativesockets import Domain, SockType, Protocol

import std/importutils
privateAccess(PgConnection)

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
  ## Closed: `PgStateError` for a deliberate `close()`, `PgConnectionError` for
  ## a lost connection — only the second is worth reconnecting. Any other
  ## non-ready state (`csBusy`, …) is a live connection already in use, almost
  ## always driven concurrently, and raises `PgStateError`.
  # Check ``closedByUser`` first: ``close()`` sets it while still ``csReady``.
  conn.checkNotClosed()
  if conn.state == csReady:
    when defined(pgStateChecks):
      conn.checkBorrowable()
    return
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

proc simpleQueryImpl(
    conn: PgConnection, sql: string
): Future[seq[QueryResult]] {.async.} =
  conn.checkReady()
  let msg = encodeQuery(sql)
  conn.markBusy()
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

proc simpleExecImpl(conn: PgConnection, sql: string): Future[string] {.async.} =
  conn.checkReady()
  let msg = encodeQuery(sql)
  conn.markBusy()
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

proc closeTransportNoWait(conn: PgConnection) =
  ## Best-effort transport teardown without waiting. Not fire-and-forget:
  ## ``closeTransport`` publishes the in-flight teardown on the connection, so
  ## a later ``close`` joins this one instead of racing it, and a test can
  ## await the same future rather than sleeping.
  proc doClose() {.async.} =
    try:
      await conn.closeTransport()
    except CatchableError:
      discard

  asyncSpawn doClose()

proc invalidateWire(conn: PgConnection, releaseTransport: bool, reuseIfSettled = true) =
  ## Sole owner of "a round trip died mid-flight, so this connection can no
  ## longer be reused". Every timeout and cancellation handler comes here, so
  ## the policy lives once and must be idempotent: one expired deadline reaches
  ## it once per nested frame the cancellation passes through.
  ##
  ## ``wireSettled`` decides both effects, since both answer one question: is
  ## the backend owed a reply this frame will never read? If so the server may
  ## still be working (abort it) and the next read would land mid-reply (retire
  ## it). Only a settled wire is provably on a message boundary, and only then
  ## may a ``csBusy`` connection be handed back. The ambiguous case retires on
  ## purpose: a needless reconnect is cheaper than silently corrupting an
  ## unrelated borrower's results.
  ##
  ## ``reuseIfSettled = false`` says the wire is not the whole story — the
  ## frame that timed out is still live on the socket, so a settled wire proves
  ## nothing about what it does next.
  if conn.wireSettled:
    if reuseIfSettled:
      if conn.state == csBusy:
        # Taken busy and drained since: every op books its writes before its
        # first await, so a settled wire means the last `ReadyForQuery` was
        # read and the frame died before the op reset the state. Hand a healthy
        # connection back rather than retiring it.
        conn.markReady()
      elif conn.state == csClosed and releaseTransport:
        # The read path can flip `csClosed` while leaving the socket up.
        conn.closeTransportNoWait()
      return
    # Retiring a settled wire: the backend is parked on a message boundary, so
    # there is nothing for a CancelRequest to abort and no counters to clear.
  else:
    # Only the first frame to claim the outstanding replies may dial, or every
    # frame the cancellation passes through opens its own socket for one query.
    conn.pendingSyncs = 0
    conn.unsyncedWrite = false
    conn.cancelNoWait()
  conn.markClosed()
  if releaseTransport:
    # `closeTransport` awaits an in-flight teardown, so a repeat from an outer
    # frame joins that one rather than starting a second.
    conn.closeTransportNoWait()

proc invalidateOnTimeout*(conn: PgConnection, reason: string) =
  ## Invalidate a timed-out round trip and raise ``PgTimeoutError``.
  ##
  ## For the frame that was driving the wire, so what the wire owes decides
  ## what it leaves behind. The transport is left alone: the caller is still in
  ## scope, so the pool's ``release`` or the user's ``close`` tears it down.
  ##
  ## On asyncdispatch ``wait`` cannot cancel the future it gave on, so the
  ## timed-out operation stays live on the socket, still reading into the
  ## shared receive buffer. A settled wire says nothing about that orphan: the
  ## connection is retired regardless, and the read path's ``csClosed`` check
  ## ends it.
  conn.invalidateWire(releaseTransport = false, reuseIfSettled = not hasAsyncDispatch)
  raise newException(PgTimeoutError, reason)

proc retireOnTimeout*(conn: PgConnection, reason: string) =
  ## Invalidate a timed-out scope whose body is still running, and raise
  ## ``PgTimeoutError``.
  ##
  ## Retirement is owed to ownership, not to the wire: a `wait` that cannot
  ## cancel what it gave on (asyncdispatch) leaves the body holding the
  ## connection, free to ``COMMIT`` a transaction the caller was told had timed
  ## out. So this one never hands the connection back.
  conn.invalidateWire(releaseTransport = false, reuseIfSettled = false)
  raise newException(PgTimeoutError, reason)

proc invalidateOnCancel*(conn: PgConnection, releaseTransport = true) =
  ## Invalidate a cancelled round trip.
  ##
  ## The request went out and its ``ReadyForQuery`` was never drained, so the
  ## stream is desynchronised and no later operation recovers it. Leaving
  ## ``csBusy`` instead made every subsequent call fail ``checkReady`` with a
  ## ``PgStateError`` no reconnect loop acts on.
  ##
  ## ``releaseTransport`` says whether this frame is the last one that knows
  ## about the connection: true for the operation wrappers, which cancellation
  ## unwinds past, false for the transaction and savepoint macros, whose own
  ## scope still hands the connection back.
  ##
  ## Whoever runs first decides, not the nesting: a cancellation inside an
  ## awaited operation is claimed by that operation's wrapper, so an enclosing
  ## macro's call finds the counters zeroed and is a no-op — and its ROLLBACK
  ## cleanup is then skipped as ``csrConnInvalidated``. The macro's ``false``
  ## only covers a cancellation landing between operations.
  conn.invalidateWire(releaseTransport)

template awaitOrInvalidate*(
    connExpr: PgConnection,
    dest: untyped,
    fut: untyped,
    timeout: Duration,
    reason: static string,
) =
  ## Await ``fut`` with optional timeout. ``AsyncTimeoutError`` invalidates via
  ## ``invalidateOnTimeout``; a cancellation via ``invalidateOnCancel`` and is
  ## re-raised.
  if timeout > ZeroDuration:
    try:
      dest = await fut.wait(timeout)
    except AsyncTimeoutError:
      connExpr.invalidateOnTimeout(reason)
    except CancelledError as e:
      connExpr.invalidateOnCancel()
      raise e
  else:
    try:
      dest = await fut
    except CancelledError as e:
      connExpr.invalidateOnCancel()
      raise e

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
    except CancelledError as e:
      connExpr.invalidateOnCancel()
      raise e
  else:
    try:
      await fut
    except CancelledError as e:
      connExpr.invalidateOnCancel()
      raise e

proc simpleExec*(
    conn: PgConnection, sql: string, timeout: Duration = ZeroDuration
): Future[CommandResult] {.async.} =
  ## Simple-query exec (one ``Query`` msg, no Parse/Bind). Parameter-less only;
  ## verbatim SQL — quote via ``quoteIdentifier``. Returns last tag.
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
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
  ## Simple-query multi-statement exec. One ``QueryResult`` per ``;``-separated stmt,
  ## text rows, no params/cache. Verbatim SQL — only trusted input; quote via
  ## ``quoteIdentifier``.
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
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
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
  proc perform(): Future[void] {.async.} =
    conn.checkReady()
    if not conn.isConnected():
      conn.markClosed()
      raise newException(PgConnectionError, "Connection is not established")
    conn.markBusy()
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
  ## Probe single value against ``trueLiteral``; raise if empty/NULL (fail host, don't default).
  let results = await conn.simpleQuery(sql)
  if results.len > 0 and results[0].rowCount > 0:
    let val = results[0].rows[0][0]
    if val.isSome:
      return bytesToString(val.get) == trueLiteral
  raise
    newException(PgConnectionError, "probe \"" & sql & "\" returned no usable result")

proc isPhysicalReplicationConn(conn: PgConnection): bool =
  ## True for physical replication (``replication`` in {true,on,yes,1}); cannot run ``SELECT``.
  for (k, v) in conn.config.extraParams:
    if k == "replication" and v in ["true", "on", "yes", "1"]:
      return true
  false

proc inRecovery(conn: PgConnection): Future[bool] {.async.} =
  ## Recovery state: ``in_hot_standby`` (PG14+) else ``pg_is_in_recovery()``;
  ## physical replication falls back to ``SHOW transaction_read_only`` (approximation).
  let ihs = conn.serverParams.getOrDefault("in_hot_standby", "")
  if ihs.len > 0:
    return ihs == "on"
  if conn.isPhysicalReplicationConn:
    return await conn.probeBool("SHOW transaction_read_only", "on")
  return await conn.probeBool("SELECT pg_catalog.pg_is_in_recovery()", "t")

proc isReadOnly(conn: PgConnection): Future[bool] {.async.} =
  ## Read-only state: ``default_transaction_read_only``/``in_hot_standby`` (PG14+, no query) else ``SHOW``.
  let dtro = conn.serverParams.getOrDefault("default_transaction_read_only", "")
  let ihs = conn.serverParams.getOrDefault("in_hot_standby", "")
  if dtro.len > 0 and ihs.len > 0:
    return dtro == "on" or ihs == "on"
  return await conn.probeBool("SHOW transaction_read_only", "on")

proc checkSessionAttrs*(
    conn: PgConnection, attrs: TargetSessionAttrs
): Future[bool] {.async.} =
  ## Check ``target_session_attrs`` (libpq semantics). ``tsaPreferStandby`` always
  ## matches standalone; failover handles preference. Raises on indeterminate probe.
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
