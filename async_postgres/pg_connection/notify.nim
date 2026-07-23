## LISTEN / NOTIFY plumbing.
##
## - `onNotify` / `onListenError` / `listen` / `unlisten` — channel
##   subscription API.
## - `startListening` / `stopListening` / `listenPump` — background pump
##   that converts incoming `NotificationResponse` messages into queue/
##   callback dispatch, with auto-reconnect on transport failure.
## - `reconnectInPlace` — replace the dead transport on the existing
##   `PgConnection` object and re-`LISTEN` every subscribed channel so
##   external references survive the reconnect.
## - `waitNotification` — async pull entry point with timeout and overflow
##   detection.
##
## Imports `lifecycle.connect` for `reconnectInPlace` and `simple_query`
## for the `LISTEN`/`UNLISTEN` round trips. Re-exported through
## `pg_connection.nim`.

import std/[deques, options, sets]

import ../[async_backend, pg_errors, pg_protocol]
import types, buffer_io, cache, simple_query, lifecycle

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl

const listenBackoffTickMs = 50
  ## Granularity of the listen pump's interruptible reconnect backoff: the pump
  ## sleeps in ticks this size and re-checks `listenStopRequested` between them,
  ## bounding how long a `stopListening` issued mid-backoff waits to be observed.

# Callback registration

proc onNotify*(conn: PgConnection, callback: NotifyCallback) =
  ## Set a callback invoked for each incoming NOTIFY message.
  conn.notifyCallback = callback

proc onListenError*(
    conn: PgConnection, callback: proc(err: ref PgListenError) {.gcsafe, raises: [].}
) =
  ## Set a callback invoked when the listen pump dies permanently (reconnection
  ## failed, or the connection was lost with no channels left to re-subscribe).
  ## Push API (`onNotify`) users have no other way to learn the pump is gone;
  ## pull API users see the same failure raised from `waitNotification`.
  conn.listenErrorCallback = callback

# In-place reconnect (preserves PgConnection identity for listeners)

proc reconnectInPlace*(conn: PgConnection) {.async.} =
  ## Reconnect using stored config, re-LISTENing on all channels. A re-LISTEN
  ## failure closes the freshly opened transport so the reconnect never leaks it.
  await conn.closeTransport()

  conn.recvBuf.setLen(0)
  conn.recvBufStart = 0
  conn.sendBuf.setLen(0)
  # Fresh backend holds none of the old session-level advisory locks; stale
  # state would fake an onLeakedSessionLocks on pool release.
  conn.heldSessionLocks = 0
  conn.sessionLockDirty = false
  conn.clearStmtCache()
  conn.state = csConnecting

  var newConn: PgConnection
  try:
    newConn = await connect(conn.config)
  except CatchableError as e:
    conn.state = csClosed
    raise e
  when hasChronos:
    conn.transport = newConn.transport
    conn.baseReader = newConn.baseReader
    conn.baseWriter = newConn.baseWriter
    conn.reader = newConn.reader
    conn.writer = newConn.writer
    conn.tlsStream = newConn.tlsStream
    conn.trustAnchorBufs = newConn.trustAnchorBufs
    conn.x509Capture = newConn.x509Capture
  elif hasAsyncDispatch:
    conn.socket = newConn.socket

  conn.sslEnabled = newConn.sslEnabled
  conn.serverCertDer = newConn.serverCertDer
  conn.recvBuf = newConn.recvBuf
  conn.recvBufStart = newConn.recvBufStart
  conn.host = newConn.host
  conn.port = newConn.port
  conn.pid = newConn.pid
  conn.secretKey = newConn.secretKey
  conn.serverParams = newConn.serverParams
  conn.txStatus = newConn.txStatus
  conn.state = csReady
  conn.createdAt = newConn.createdAt

  when hasChronos:
    # Value-copied x509Capture still holds pointers into newConn (soon freed).
    # Repoint certDer and the shared engine's x509 slot at conn's own fields.
    if conn.tlsStream != nil:
      rebindX509Capture(
        conn.x509Capture, conn.tlsStream.ccontext.eng, addr conn.serverCertDer
      )

  try:
    for ch in conn.listenChannels:
      discard await conn.simpleQuery("LISTEN " & quoteIdentifier(ch))
  except CancelledError as e:
    # Pump teardown: let close()'s own closeTransport reclaim the transport.
    raise e
  except CatchableError as e:
    # connect() succeeded but re-LISTEN failed: close the fresh transport so the
    # failed reconnect never leaks it (notifyListenDeath only sets csClosed).
    await conn.closeTransport()
    conn.state = csClosed
    raise e

# Background pump and start/stop

proc newListenError(
    msg: string, reconnectionAttempted: bool
): ref PgListenError {.raises: [].} =
  (ref PgListenError)(msg: msg, reconnectionAttempted: reconnectionAttempted)

proc notifyListenDeath(
    conn: PgConnection, msg: string, reconnectionAttempted: bool
) {.raises: [].} =
  ## Mark the listen pump as permanently dead and notify both APIs: the pull
  ## API via `notifyWaiter.fail` and the push API via `listenErrorCallback`.
  conn.listenError = newListenError(msg, reconnectionAttempted)
  conn.state = csClosed
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    # Fail the pull-API waiter with a *fresh* exception, never the stored
    # `conn.listenError`: that object is re-raised by `checkListenAlive` on every
    # later call, so sharing one ref would let its stack trace accumulate.
    #
    # asyncdispatch types `Future.fail`'s callback chain as raising the base
    # `Exception`, so catching `Exception` (not `CatchableError`) is what keeps
    # this proc `raises: []` — same idiom as `dispatchNotification`. `fail` runs
    # no user callbacks synchronously here, so nothing real is masked, and
    # swallowing it guarantees the push-API callback below still fires.
    try:
      conn.notifyWaiter.fail(newListenError(msg, reconnectionAttempted))
    except Exception:
      discard
  if conn.listenErrorCallback != nil:
    conn.listenErrorCallback(conn.listenError)

proc listenPump*(conn: PgConnection) {.async.} =
  ## Background loop: repeatedly receives messages, dispatching notifications.
  ## NotificationResponse/NoticeResponse are dispatched inside `recvMessage`; an
  ## asynchronous ErrorResponse (the server terminating this backend) is raised so
  ## its diagnostic drives the failure path rather than being silently dropped.
  ## Any other non-notification message is discarded.
  ## On connection failure, attempts automatic reconnection with exponential
  ## backoff (up to `listenReconnectMaxAttempts` attempts; 0 or negative =
  ## unlimited) and re-subscribes to all channels.
  ## Exits cleanly when state changes from csListening (via stopListening
  ## sending an empty query), then drains until ReadyForQuery. A stop requested
  ## while reconnecting (`listenStopRequested`) is honored at every yield point
  ## of the reconnect loop, so stopListening never strands on a pump that would
  ## otherwise loop back into csListening after a successful reconnect. The
  ## inter-attempt backoff is slept in short ticks that re-check the stop flag,
  ## so a stop mid-backoff is observed within a tick instead of after the full
  ## interval.
  while true:
    try:
      while conn.state == csListening:
        let msg = await conn.recvMessage()
        if msg.kind == bmkErrorResponse:
          # An asynchronous ErrorResponse on an idle LISTEN connection is the
          # server tearing down this backend (FATAL: administrator command,
          # recovery conflict, idle-session timeout, …) — `recvMessage` already
          # dispatched NotificationResponse/NoticeResponse internally, so this is
          # the one server-initiated message left to handle. Don't discard it and
          # fall through to the generic "Connection closed by server" the next
          # recv would raise once the socket closes: raise the server's own
          # diagnostic so the reconnect-failure death below reports the real
          # reason instead of swallowing it.
          raise newPgQueryError(msg.errorFields)
      # State changed: drain the stop-signal query response until ReadyForQuery
      block drainLoop:
        while true:
          while (let opt = conn.nextMessage(); opt.isSome):
            let msg = opt.get
            if msg.kind == bmkReadyForQuery:
              conn.txStatus = msg.txStatus
              break drainLoop
          await conn.fillRecvBuf()
      return # Clean exit via stopListening
    except CancelledError:
      return # Cancelled from close()
    except CatchableError as e:
      if conn.listenChannels.len == 0:
        conn.notifyListenDeath("Listen connection lost: " & e.msg, false)
        return
      # Auto-reconnect with exponential backoff. `listenReconnecting` marks this
      # window for a concurrent `stopListening`: while the transport is being
      # rebuilt the empty-query unblock it normally uses would interleave with
      # the reconnect's own LISTEN round trips and desync the stream, so it
      # signals a stop via `listenStopRequested` instead — checked at every
      # yield point below so the request is never lost.
      conn.listenReconnecting = true
      # Cleared once for every exit from the reconnect window by the `finally`
      # below, so no individual exit path (stop-wins, cancellation, post-loop)
      # can leak it true and mislead the next `stopListening`.
      try:
        let maxAttempts = conn.listenReconnectMaxAttempts
        # Cap so `backoff * 1000` and `backoff * 2` below cannot overflow int.
        let maxBackoff = clamp(conn.listenReconnectMaxBackoff, 1, high(int) div 1000)
        let unlimited = maxAttempts <= 0
        var reconnected = false
        var backoff = 1
        var attempt = 0
        while (unlimited or attempt < maxAttempts) and not conn.listenStopRequested:
          try:
            # Interruptible backoff: sleep in short ticks and re-check the stop
            # flag each tick, so a concurrent `stopListening` is observed within
            # a tick instead of after the full interval — a bare
            # `sleepAsync(seconds(backoff))` would strand the stop for up to
            # `listenReconnectMaxBackoff` seconds.
            var remainingMs = backoff * 1000
            while remainingMs > 0 and not conn.listenStopRequested:
              let tickMs = min(remainingMs, listenBackoffTickMs)
              await sleepAsync(milliseconds(tickMs))
              remainingMs -= tickMs
            if conn.listenStopRequested:
              break
            await conn.reconnectInPlace()
            if conn.listenStopRequested:
              # Stop won the race with a successful reconnect. The new transport
              # is live and already `csReady` (reconnectInPlace set it); do *not*
              # restore csListening — that overwrite is exactly what used to
              # strand the awaiting stopListening. Exit so it sees a finished pump
              # and a reusable connection. `reconnectCallback` is intentionally
              # skipped: the caller asked to stop, so the connection is handed
              # back csReady and *not* listening — firing a "reconnected, still
              # listening" notification would misrepresent that. The fresh backend
              # identity (pid/secretKey) is already on `conn` regardless.
              return
            conn.state = csListening
            reconnected = true
            if conn.reconnectCallback != nil:
              conn.reconnectCallback()
            break
          except CancelledError:
            return
          except CatchableError:
            backoff = min(backoff * 2, maxBackoff)
          inc attempt
        if conn.listenStopRequested:
          # Asked to stop before any live transport was restored: the old one is
          # already gone, so the connection is unusable. Mark it closed; the
          # awaiting stopListening surfaces that state.
          conn.state = csClosed
          return
        if not reconnected:
          # Carry the original loss reason (`e` — e.g. the FATAL ErrorResponse the
          # recv loop surfaced) into the death message; it is the actual cause the
          # caller wants, not just the count of failed retries.
          conn.notifyListenDeath(
            "Listen connection lost (" & e.msg & "): reconnection failed after " &
              $maxAttempts & " attempts",
            true,
          )
          return
      finally:
        conn.listenReconnecting = false

proc startListening*(conn: PgConnection) =
  conn.listenStopRequested = false
  conn.listenReconnecting = false
  conn.state = csListening
  conn.listenTask = conn.listenPump()

proc abortListenTask(conn: PgConnection) {.async.} =
  ## Shared `stopListening` failure cleanup: the pump's transport is dead, so
  ## cancel the task if it is still running and mark the connection closed. The
  ## caller nils `listenTask`; `stopListening`'s `finally` clears
  ## `listenStopRequested`.
  if conn.listenTask != nil and not conn.listenTask.finished:
    await cancelAndWait(conn.listenTask)
  conn.state = csClosed

proc failNotifyWaiter(conn: PgConnection) {.raises: [].} =
  ## Release the pull-API waiter when the pump has stopped so `waitNotification`
  ## does not hang: no future dispatch can complete it.
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    try:
      conn.notifyWaiter.fail(newException(PgError, "Listener stopped"))
    except Exception:
      discard

proc stopListening*(conn: PgConnection) {.async.} =
  ## Stop the background listen pump and return the connection to `csReady`
  ## (or leave it `csClosed` if the transport died with no live reconnect).
  ## `listenStopRequested` is cleared on *every* exit — including when the caller
  ## cancels this future mid-stop — so a later reconnect never trips over a stale
  ## stop request and silently closes the pump.
  if conn.listenTask == nil or conn.listenTask.finished:
    conn.listenTask = nil
    conn.listenStopRequested = false
    if conn.state == csListening:
      conn.state = csReady
    conn.failNotifyWaiter()
    return
  # Request the stop up front, before choosing how to deliver it: this also
  # covers the pump tripping into its reconnect loop *after* we pick the normal
  # path below (a recv that fails the instant we signal) — it still observes the
  # request there and exits instead of looping back into csListening.
  conn.listenStopRequested = true
  try:
    if conn.listenReconnecting:
      # The pump is rebuilding a dead transport. The empty-query unblock the
      # normal path uses would race the reconnect's LISTEN round trips and
      # desync the stream, so just wait for the pump to observe
      # `listenStopRequested` and exit. It leaves the connection `csReady` (a
      # reconnect completed before the stop) or `csClosed` (none did) — either
      # way a finished pump, never a hang. A pump parked in its backoff observes
      # the stop within one short tick (see the reconnect loop); otherwise we
      # wait only for the in-flight reconnect round trips to finish.
      try:
        await conn.listenTask
      except CancelledError as e:
        # The pump is still running and owns the live future; leave the handle
        # intact (the `finally` only clears the stop flag) so close() can cancel
        # it, then propagate the cancellation.
        raise e
      except CatchableError:
        await conn.abortListenTask()
      conn.listenTask = nil
      conn.failNotifyWaiter()
      return
    # Normal path: pump parked in the recv loop. Signal exit by changing state,
    # then send an empty query to unblock the read.
    conn.state = csBusy
    try:
      await conn.sendMsg(encodeQuery(""))
      # Wait for pump to drain and exit naturally
      await conn.listenTask
    except CancelledError as e:
      raise e
    except CatchableError:
      # Send or pump failed: connection is dead
      await conn.abortListenTask()
    conn.listenTask = nil
    # Preserve csClosed if pump detected a connection error
    if conn.state != csClosed:
      conn.state = csReady
    conn.failNotifyWaiter()
  finally:
    # Runs on the normal, failed, *and* cancelled paths: a stop request left set
    # would later abort a legitimate reconnect (the pump's reconnect loop reads
    # it at every yield point), so it must never outlive this call.
    conn.listenStopRequested = false

# LISTEN / UNLISTEN entry points

proc listen*(conn: PgConnection, channel: string): Future[void] {.async.} =
  ## Subscribe to a LISTEN channel and start the background notification pump.
  if conn.state == csListening:
    await conn.stopListening()
  conn.checkReady()
  discard await conn.simpleQuery("LISTEN " & quoteIdentifier(channel))
  conn.listenChannels.incl(channel)
  conn.startListening()

proc unlisten*(conn: PgConnection, channel: string): Future[void] {.async.} =
  ## Unsubscribe from a LISTEN channel. Stops the pump if no channels remain.
  if conn.state == csListening:
    await conn.stopListening()
  conn.checkReady()
  discard await conn.simpleQuery("UNLISTEN " & quoteIdentifier(channel))
  conn.listenChannels.excl(channel)
  if conn.listenChannels.len > 0:
    conn.startListening()

# Wait API

proc checkNotifyOverflow(conn: PgConnection) =
  ## Raise PgNotifyOverflowError if notifications were dropped since last check.
  if conn.notifyDropped > 0:
    let dropped = conn.notifyDropped
    conn.notifyDropped = 0
    let err = (ref PgNotifyOverflowError)(
      msg: "Dropped " & $dropped & " notifications due to queue overflow",
      dropped: dropped,
    )
    raise err

proc checkListenAlive(conn: PgConnection) =
  ## Raise if the listen pump has died permanently.
  if conn.listenError != nil:
    # Raise a fresh copy, not the stored object: re-raising one shared ref
    # across repeated calls would let its stack trace grow unbounded.
    raise newListenError(conn.listenError.msg, conn.listenError.reconnectionAttempted)
  if conn.state == csClosed:
    raise newException(PgConnectionError, "Connection is closed")

proc waitNotification*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[Notification] {.async.} =
  ## Wait for the next notification from the buffer.
  ## If the buffer is empty, blocks until a notification arrives or timeout expires.
  ## Raises PgNotifyOverflowError if notifications were dropped due to queue overflow.
  ## Raises PgListenError if the listen pump has died (e.g. reconnection failed).
  conn.checkNotifyOverflow()
  conn.checkListenAlive()
  if conn.notifyQueue.len > 0:
    return conn.notifyQueue.popFirst()
  # No pump means nothing will ever complete the waiter, so refuse instead of
  # blocking forever. During reconnect the task is still running.
  if conn.listenTask == nil or conn.listenTask.finished:
    raise newException(PgError, "Listener stopped")
  if conn.notifyWaiter != nil and not conn.notifyWaiter.finished:
    raise newException(PgError, "Another waitNotification is already active")
  conn.notifyWaiter = newFuture[void]("waitNotification")
  try:
    if timeout > ZeroDuration:
      try:
        await conn.notifyWaiter.wait(timeout)
      except AsyncTimeoutError:
        raise newException(PgTimeoutError, "Wait for notification timed out")
    else:
      await conn.notifyWaiter
  finally:
    conn.notifyWaiter = nil
  conn.checkNotifyOverflow()
  if conn.notifyQueue.len > 0:
    return conn.notifyQueue.popFirst()
  raise newException(PgError, "No notification available")
