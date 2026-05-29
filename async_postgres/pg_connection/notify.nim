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
  ## Reconnect using stored config, re-LISTENing on all channels.
  await conn.closeTransport()
  conn.recvBuf.setLen(0)
  conn.recvBufStart = 0
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
  elif hasAsyncDispatch:
    conn.socket = newConn.socket
  conn.sslEnabled = newConn.sslEnabled
  conn.recvBuf = newConn.recvBuf
  conn.pid = newConn.pid
  conn.secretKey = newConn.secretKey
  conn.serverParams = newConn.serverParams
  conn.txStatus = newConn.txStatus
  conn.state = csReady
  conn.createdAt = newConn.createdAt
  for ch in conn.listenChannels:
    discard await conn.simpleQuery("LISTEN " & quoteIdentifier(ch))

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
  ## Non-notification messages are discarded (recvMessage handles dispatch).
  ## On connection failure, attempts automatic reconnection with exponential
  ## backoff (up to `listenReconnectMaxAttempts` attempts; 0 or negative =
  ## unlimited) and re-subscribes to all channels.
  ## Exits cleanly when state changes from csListening (via stopListening
  ## sending an empty query), then drains until ReadyForQuery.
  while true:
    try:
      while conn.state == csListening:
        discard await conn.recvMessage()
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
    except CatchableError:
      if conn.listenChannels.len == 0:
        conn.notifyListenDeath("Listen connection lost", false)
        return
      # Auto-reconnect with exponential backoff
      let maxAttempts = conn.listenReconnectMaxAttempts
      let maxBackoff = max(1, conn.listenReconnectMaxBackoff)
      let unlimited = maxAttempts <= 0
      var reconnected = false
      var backoff = 1
      var attempt = 0
      while unlimited or attempt < maxAttempts:
        try:
          await sleepAsync(seconds(backoff))
          await conn.reconnectInPlace()
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
      if not reconnected:
        conn.notifyListenDeath(
          "Listen connection lost: reconnection failed after " & $maxAttempts &
            " attempts",
          true,
        )
        return

proc startListening*(conn: PgConnection) =
  conn.state = csListening
  conn.listenTask = conn.listenPump()

proc stopListening*(conn: PgConnection) {.async.} =
  ## Stop the background listen pump and return the connection to `csReady`.
  if conn.listenTask == nil or conn.listenTask.finished:
    conn.listenTask = nil
    if conn.state == csListening:
      conn.state = csReady
    return
  # Signal pump to exit by changing state, then send empty query to unblock read
  conn.state = csBusy
  try:
    await conn.sendMsg(encodeQuery(""))
    # Wait for pump to drain and exit naturally
    await conn.listenTask
  except CancelledError as e:
    raise e
  except CatchableError:
    # Send or pump failed: connection is dead
    if conn.listenTask != nil and not conn.listenTask.finished:
      await cancelAndWait(conn.listenTask)
    conn.listenTask = nil
    conn.state = csClosed
    return
  conn.listenTask = nil
  # Preserve csClosed if pump detected a connection error
  if conn.state != csClosed:
    conn.state = csReady

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
