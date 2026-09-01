## LISTEN/NOTIFY: subscription API, pump with auto-reconnect, and pull API.

import std/[deques, options, sets]

import ../[async_backend, pg_errors, pg_protocol]
import types, buffer_io, cache, simple_query, lifecycle

when hasChronos:
  import chronos/streams/tlsstream
  import ../pg_bearssl

const listenBackoffTickMs = 50 ## Backoff tick ms (stop check granularity).

# listenReconnectStopWaitMs lives in types.nim to avoid a circular import.

# Callback registration

proc onNotify*(conn: PgConnection, callback: NotifyCallback) =
  ## Set a callback invoked for each incoming NOTIFY message.
  conn.notifyCallback = callback

proc onListenError*(
    conn: PgConnection, callback: proc(err: ref PgListenError) {.gcsafe, raises: [].}
) =
  ## Callback for permanent pump failure. ``err.transportAlive`` marks a death
  ## the pull API reports as ``PgListenStoppedError``.
  conn.listenErrorCallback = callback

# In-place reconnect (preserves PgConnection identity for listeners)

proc reconnectInPlace*(conn: PgConnection) {.async.} =
  ## Reconnect and re-LISTEN all channels. Cleans up new transport on failure.
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
  if conn.listenStopRequested:
    # Stop landed while connect() blocked: discard newConn, the pump's own
    # stop-check then sees csClosed and exits.
    try:
      # The reconnect dialled `newConn` and the stop discards it, so this is the
      # library's own teardown — not the application's `close()`.
      await newConn.closeImpl(byUser = false)
    except CatchableError:
      discard
    conn.state = csClosed
    return
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
    msg: string, reconnectionAttempted: bool, transportAlive: bool = false
): ref PgListenError {.raises: [].} =
  (ref PgListenError)(
    msg: msg,
    reconnectionAttempted: reconnectionAttempted,
    transportAlive: transportAlive,
  )

proc newListenDeathError(
    err: ref PgListenError, transportAlive: bool
): ref PgError {.raises: [].} =
  ## Pull-API view of a pump death recorded in ``err``.
  # The type, not a field, carries the recovery, so a caller who never reads
  # `transportAlive` still won't re-dial. Re-supplied because `err` latched it.
  if transportAlive:
    (ref PgListenStoppedError)(
      msg: err.msg,
      reconnectionAttempted: err.reconnectionAttempted,
      transportAlive: true,
    )
  else:
    newListenError(err.msg, err.reconnectionAttempted, transportAlive)

proc notifyListenDeath(
    conn: PgConnection,
    msg: string,
    reconnectionAttempted: bool,
    markClosed: bool = true,
) {.raises: [].} =
  ## Pump died permanently; notify pull/push APIs. ``markClosed=false`` = listen side only.
  # `markClosed = false` is exactly the case where the transport survived, so
  # it is what tells a reconnect loop this failure is not its to act on.
  let transportAlive = not markClosed
  conn.listenError = newListenError(msg, reconnectionAttempted, transportAlive)
  if markClosed:
    conn.state = csClosed
  # Built fresh, never the stored ref: `checkListenAlive` re-raises that object
  # on every later call, so sharing it would accumulate stack traces.
  conn.failNotifyWaiter(newListenDeathError(conn.listenError, transportAlive))
  if conn.listenErrorCallback != nil:
    conn.listenErrorCallback(conn.listenError)

proc listenPump*(conn: PgConnection) {.async.} =
  ## Background loop: dispatch notifications, auto-reconnect on failure.
  while true:
    try:
      while conn.state == csListening:
        let msg = await conn.recvMessage()
        if msg.kind == bmkErrorResponse:
          # Server FATAL on idle LISTEN: surface diagnostic instead of generic close.
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
      # Auto-reconnect with exponential backoff. Flag guards concurrent stop.
      conn.listenReconnecting = true
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
            # Interruptible backoff: tick-based stop check.
            var remainingMs = backoff * 1000
            while remainingMs > 0 and not conn.listenStopRequested:
              let tickMs = min(remainingMs, listenBackoffTickMs)
              await sleepAsync(milliseconds(tickMs))
              remainingMs -= tickMs
            if conn.listenStopRequested:
              break
            await conn.reconnectInPlace()
            if conn.listenStopRequested:
              # Stop won race: keep csReady, skip reconnect callback.
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
          conn.state = csClosed
          return
        if not reconnected:
          conn.notifyListenDeath(
            "Listen connection lost (" & e.msg & "): reconnection failed after " &
              $maxAttempts & " attempts",
            true,
          )
          return
      finally:
        conn.listenReconnecting = false

proc startListening*(conn: PgConnection) =
  ## Start the notification pump. No-op if one is already running.
  # `restartPumpOrFailWaiter` reaches here from an exception handler; overwriting
  # a live task would put a second, unreachable pump on the same socket.
  if conn.listenTask != nil and not conn.listenTask.finished:
    return
  # Clear stale ``listenError`` (e.g. ``markClosed=false`` death) now that pump is live.
  conn.listenError = nil
  conn.listenStopRequested = false
  conn.listenReconnecting = false
  conn.state = csListening
  conn.listenTask = conn.listenPump()

proc abortListenTask(conn: PgConnection): Future[bool] {.async.} =
  ## Stop failed pump; false = orphan on asyncdispatch (keep ``listenStopRequested``).
  var stopped = true
  if conn.listenTask != nil and not conn.listenTask.finished:
    when hasAsyncDispatch:
      # Close transport to break parked recv, then bounded wait.
      conn.listenStopRequested = true
      let pump = conn.listenTask
      await conn.closeTransport()
      stopped = false
      try:
        await pump.wait(milliseconds(listenReconnectStopWaitMs))
        stopped = true
      except AsyncTimeoutError:
        discard
      except CatchableError:
        stopped = true
    else:
      await cancelAndWait(conn.listenTask)
  conn.state = csClosed
  return stopped

template releaseNotifyWaiter(conn: PgConnection, keepWaiter: bool) =
  ## Fail waiter unless kept for pump restart on live connection.
  if not keepWaiter or conn.state == csClosed:
    conn.failNotifyWaiter()

template abortAndFailWaiter(
    conn: PgConnection, keepWaiter: bool, preserveStopFlag: var bool
) =
  ## Teardown shared by `stopListening`'s two cancellation paths.
  # Abort rather than drop the reference: the pump still holds the transport and
  # nilling `listenTask` disarms `close()`'s own teardown. `noCancel` because the
  # cancellation in flight would abort a bare `await`.
  when hasChronos:
    if not (await noCancel conn.abortListenTask()):
      preserveStopFlag = true
  else:
    if not (await conn.abortListenTask()):
      preserveStopFlag = true
  conn.state = csClosed
  conn.listenTask = nil
  conn.releaseNotifyWaiter(keepWaiter)

proc stopListeningImpl(conn: PgConnection, keepWaiter: bool): Future[void] {.async.} =
  ## Stop pump → ``csReady``/``csClosed``; may raise ``PgTimeoutError``.
  ## ``keepWaiter`` preserves the parked waiter for a restart: internal only,
  ## for `listen` / `unlisten`, which stop a live pump on the way in.
  if conn.listenTask == nil or conn.listenTask.finished:
    conn.listenTask = nil
    if conn.state == csListening:
      conn.state = csReady
    # csClosed + flag set = orphan pump inside reconnectInPlace; keep the flag.
    if conn.state != csClosed:
      conn.listenStopRequested = false
    conn.releaseNotifyWaiter(keepWaiter)
    return
  # Request the stop up front, before choosing how to deliver it: this also
  # covers the pump tripping into its reconnect loop *after* we pick the normal
  # path below (a recv that fails the instant we signal) — it still observes the
  # request there and exits instead of looping back into csListening.
  conn.listenStopRequested = true
  # Set on every path that leaves a still-running orphan pump, which needs the
  # flag; the `finally` clears it once the pump is known to have stopped.
  var preserveStopFlag = false
  try:
    if conn.listenReconnecting:
      # Pump is rebuilding a dead transport. The empty-query unblock would race
      # the reconnect's LISTEN round trips and desync, so just wait for the pump
      # to observe the stop and exit. On asyncdispatch a pump inside connect()
      # can only be bounded by listenReconnectStopWaitMs, not cancelled.
      try:
        when hasAsyncDispatch:
          await conn.listenTask.wait(milliseconds(listenReconnectStopWaitMs))
        else:
          await conn.listenTask
      except CancelledError as e:
        # Unwinding without this leaves the pump running while the `finally`
        # clears `listenStopRequested`, so `listen` starts a second one.
        conn.abortAndFailWaiter(keepWaiter, preserveStopFlag)
        raise e
      except AsyncTimeoutError:
        # Orphan the pump; reconnectInPlace's stop-check discards newConn once
        # connect() unwinds. csClosed and the failed waiter make operations on
        # the orphaned connection fail fast instead of hanging, and nilling the
        # task keeps a follow-up close()/stopListening() from waiting another
        # full window on the detached orphan.
        preserveStopFlag = true
        conn.state = csClosed
        conn.listenTask = nil
        conn.releaseNotifyWaiter(keepWaiter)
        raise newException(
          PgTimeoutError,
          "stopListening: listen pump did not stop within " & $listenReconnectStopWaitMs &
            " ms while reconnecting",
        )
      except CatchableError:
        if not (await conn.abortListenTask()):
          preserveStopFlag = true
      conn.listenTask = nil
      conn.releaseNotifyWaiter(keepWaiter)
      return
    # Normal path: pump parked in the recv loop. Signal exit by changing state,
    # then send an empty query to unblock the read.
    conn.state = csBusy
    try:
      await conn.sendMsg(encodeQuery(""))
      await conn.listenTask
    except CancelledError as e:
      # The stop query went out but its `ReadyForQuery` was never drained, so the
      # wire is desynchronised; without this the connection stays `csBusy`.
      conn.abortAndFailWaiter(keepWaiter, preserveStopFlag)
      raise e
    except CatchableError:
      # Send or pump failed: connection is dead
      if not (await conn.abortListenTask()):
        preserveStopFlag = true
    conn.listenTask = nil
    # Preserve csClosed if pump detected a connection error
    if conn.state != csClosed:
      conn.state = csReady
    conn.releaseNotifyWaiter(keepWaiter)
  finally:
    if not preserveStopFlag:
      conn.listenStopRequested = false

proc stopListening*(conn: PgConnection): Future[void] {.async.} =
  ## Stop the notification pump, returning the connection to ``csReady``
  ## (``csClosed`` if the pump died); may raise ``PgTimeoutError``.
  ##
  ## The channels stay subscribed server-side, so notifications queue there
  ## and a later `listen` resumes without losing them — unlike `unlisten`,
  ## which drops the subscription. Use this to run a query on a listening
  ## connection, which `csListening` otherwise rejects.
  await conn.stopListeningImpl(keepWaiter = false)

# LISTEN / UNLISTEN entry points

proc restartPumpOrFailWaiter(conn: PgConnection, restarted: bool) =
  ## Recover from a failed LISTEN/UNLISTEN: restart the pump we stopped, or
  ## report the death to the waiter and the push API when it cannot come back.
  ## ``restarted`` = we stopped a live pump on the way in. A cancelled round trip
  ## needs no special case: `awaitOrInvalidate` already marked it `csClosed`.
  if conn.closedReason != crOpen:
    if restarted and conn.closedReason != crClosedByUser:
      # Permanent pump death with channels still subscribed: releasing only the
      # waiter would leave an `onNotify` subscriber silently deaf.
      conn.notifyListenDeath(
        "Listen pump stopped: connection lost during LISTEN/UNLISTEN",
        reconnectionAttempted = false,
      )
    elif restarted:
      # A deliberate `close()`: a `PgListenError` here would make a reconnecting
      # `onListenError` re-dial. The waiter still needs releasing.
      conn.failNotifyWaiter()
    return
  if not restarted:
    # Nothing of ours was torn down, so there is nothing to put back.
    return
  if conn.state == csReady:
    conn.startListening()
  else:
    # `csBusy` with no round trip of ours in flight: the state is a concurrent
    # operation's, so forcing `csClosed` would kill its live work. `markClosed =
    # false` also marks the error `transportAlive` (see `newListenDeathError`).
    conn.notifyListenDeath(
      "Listen pump stopped: LISTEN/UNLISTEN left the connection busy",
      reconnectionAttempted = false,
      markClosed = false,
    )

proc listen*(conn: PgConnection, channel: string): Future[void] {.async.} =
  ## Subscribe to channel and start pump. Keeps parked ``waitNotification`` across restart; may raise ``PgTimeoutError``.
  # Reconnecting pump is in ``csReady`` but still owns ``listenTask``; stop it first to keep waiter.
  let restarted = conn.state == csListening or conn.listenReconnecting
  try:
    if restarted:
      # Inside the `try`: a `CancelledError` escaping `stopListening` would
      # otherwise strand the kept waiter with no pump left to complete it.
      await conn.stopListeningImpl(keepWaiter = true)
    conn.checkReady()
    discard await conn.simpleQuery("LISTEN " & quoteIdentifier(channel))
  except CatchableError as e:
    # `CancelledError` included: cancelling *this* call does not unsubscribe the
    # channels the pump carried, and leaving them pumpless is a silent deafness.
    # `raise e`, not bare: the restarted pump can suspend in its own `except` arm
    # and leave `getCurrentException` pointing at its error.
    conn.restartPumpOrFailWaiter(restarted)
    raise e
  conn.listenChannels.incl(channel)
  conn.startListening()

proc unlisten*(conn: PgConnection, channel: string): Future[void] {.async.} =
  ## Unsubscribe; stops pump if no channels remain (keeps waiter across restart except last).
  let restarted = conn.state == csListening or conn.listenReconnecting
  try:
    if restarted:
      await conn.stopListeningImpl(keepWaiter = true) # see `listen` for why it is here
    conn.checkReady()
    discard await conn.simpleQuery("UNLISTEN " & quoteIdentifier(channel))
  except CatchableError as e: # `CancelledError` included, see `listen`
    conn.restartPumpOrFailWaiter(restarted)
    raise e
  conn.listenChannels.excl(channel)
  if conn.listenChannels.len > 0:
    conn.startListening()
  elif restarted:
    # We stopped the pump and no channel is left to restart it, so the waiter we
    # kept across the stop would never be woken.
    conn.failNotifyWaiter()

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

proc reclaimStaleWaiter(conn: PgConnection) =
  ## Drop a finished waiter's registration and put back its unclaimed handoff.
  # A waiter that never resumes would otherwise strand the notification in
  # ``notifyHandoff`` and block every later waiter.
  if conn.notifyWaiter != nil and conn.notifyWaiter.finished:
    conn.notifyWaiter = nil
    conn.reclaimHandoff()

proc checkListenAlive(conn: PgConnection) =
  ## Raise if pump died or closed (``closedByUser`` > ``listenError`` >
  ## ``csClosed``); see ``newListenDeathError`` for a death on a live connection.
  let reason = conn.closedReason
  if reason == crClosedByUser:
    raise newException(PgStateError, closedByUserMsg)
  if conn.listenError != nil:
    # `and reason == crOpen`: the flag was latched at pump death, and a
    # connection that has died since is the reconnect loop's business after all.
    raise newListenDeathError(
      conn.listenError, conn.listenError.transportAlive and reason == crOpen
    )
  if reason == crClosed:
    raise newException(PgConnectionError, "Connection is closed")

proc waitNotification*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[Notification] {.async.} =
  ## Wait for next notification. Raises ``PgNotifyOverflowError``,
  ## ``PgConnectionError`` (closed), ``PgListenError`` (pump died with the
  ## transport), ``PgListenStoppedError`` (pump died, connection still up),
  ## ``PgStateError`` (no pump / concurrent wait / closed by `close()`),
  ## ``PgTimeoutError`` (timeout).
  conn.checkNotifyOverflow()
  conn.checkListenAlive()
  # Ahead of the queue: the reclaimed handoff is older than anything queued, so
  # serving the queue first would invert NOTIFY delivery order.
  conn.reclaimStaleWaiter()
  if conn.notifyWaiter != nil:
    raise newException(PgStateError, "Another waitNotification is already active")
  if conn.notifyQueue.len > 0:
    return conn.notifyQueue.popFirst()
  if conn.listenTask == nil or conn.listenTask.finished:
    raise newException(PgStateError, "Listener stopped")
  let myWaiter = newFuture[void]("waitNotification")
  conn.notifyWaiter = myWaiter
  var handoff: Notification
  var hasHandoff = false
  try:
    try:
      if timeout > ZeroDuration:
        try:
          await myWaiter.wait(timeout)
        except AsyncTimeoutError:
          raise newException(PgTimeoutError, "Wait for notification timed out")
      else:
        await myWaiter
    finally:
      # Only the registered waiter may claim the handoff: one failed by a pump
      # restart can resume late and would steal a newer waiter's notification.
      let mine = conn.notifyWaiter == myWaiter
      if mine:
        conn.notifyWaiter = nil
        # Claim on every path so no other caller observes it half-delivered; it
        # is returned directly and never re-enters the capped queue.
        if conn.hasNotifyHandoff:
          conn.hasNotifyHandoff = false
          handoff = move conn.notifyHandoff
          hasHandoff = true
    conn.checkNotifyOverflow()
    if hasHandoff:
      hasHandoff = false
      return handoff
    if conn.notifyQueue.len > 0:
      return conn.notifyQueue.popFirst()
    raise newException(PgStateError, "No notification available")
  finally:
    # Unwound without returning it (timeout racing the complete, cancellation,
    # a raised overflow): leave it queued for the next caller.
    if hasHandoff:
      conn.requeueHandoff(handoff)
