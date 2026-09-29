## PostgreSQL Advisory Lock API
##
## Provides an async interface to PostgreSQL's advisory locking facility.
## Advisory locks are application-enforced locks that do not lock any actual
## table rows — they simply act on application-defined lock identifiers.
##
## Two flavours exist:
##
## - **Session-level** locks (default) — held until explicitly released or
##   the session ends.
## - **Transaction-level** locks — released automatically at the end of the
##   current transaction; no explicit unlock is needed.
##
## Locks can be **exclusive** (default) or **shared** (multiple sessions may
## hold the same shared lock concurrently).
##
## **Stacking:** Session-level advisory locks are stackable — if the same
## session acquires the same lock multiple times, it must be released the
## same number of times before it is truly released. The ``withAdvisoryLock``
## templates handle acquire/release as a pair, but be careful not to nest
## them with the same key unintentionally. Transaction-level locks are not
## stackable and are always released at transaction end.
##
## **Key space:** PostgreSQL uses a single advisory-lock name space per
## database that both session-level and transaction-level locks share — a
## session-level ``advisoryLock(42)`` blocks a concurrent
## ``advisoryLockXact(42)`` on another connection and vice versa. The
## single-``int64`` and ``(int32, int32)`` overloads live in **separate**
## sub-spaces (PostgreSQL treats the two signatures as distinct lock types),
## so ``advisoryLock(1'i64)`` never blocks ``advisoryLock(0'i32, 1'i32)``
## even though the numeric identifiers overlap. Shared and exclusive locks
## on the same key follow standard reader/writer semantics — a shared lock
## blocks concurrent exclusive acquires but not other shared acquires.
##
## **Pool integration:** Typed acquires set a sticky ``sessionLockDirty``
## flag; the pool runs ``pg_advisory_unlock_all`` on return based on the
## flag (not the counter), so a typed ``advisoryUnlock`` of a raw-acquired
## key cannot forge a lock-free state. Raw-SQL acquires still bypass
## tracking — callers must release them explicitly or invoke
## ``advisoryUnlockAll`` before returning the connection.
##
## **Mixed raw + typed usage:** ``heldSessionLocks`` counts only typed
## acquires, but ``advisoryUnlock*`` decrements it whenever the server
## reports the lock was released — including when the released lock was
## acquired via raw SQL. Under mixed usage the counter therefore
## under-reports still-held tracked locks. The ``onLeakedSessionLocks``
## hook fires on ``sessionLockDirty`` rather than the counter, so leak
## detection stays accurate even when the counter has been driven to zero;
## the delivered ``count`` field is an unreliable lower bound in that
## regime. Prefer sticking to a single API (typed or raw) per connection
## checkout.
##
## Example
## =======
##
## .. code-block:: nim
##   # Session-level exclusive lock (blocking)
##   await conn.advisoryLock(42'i64)
##   defer: await conn.advisoryUnlock(42'i64)
##
##   # Non-blocking try
##   if await conn.advisoryTryLock(42'i64):
##     defer: await conn.advisoryUnlock(42'i64)
##     echo "acquired"
##
##   # Transaction-level lock (auto-released on COMMIT/ROLLBACK)
##   conn.withTransaction:
##     await conn.advisoryLockXact(42'i64)
##     echo "locked for this transaction"
##
##   # Two-key variants
##   await conn.advisoryLock(1'i32, 2'i32)
##   await conn.advisoryUnlock(1'i32, 2'i32)
##
##   # RAII-style convenience
##   conn.withAdvisoryLock(42'i64):
##     echo "lock held here"

import std/macros

import async_backend, pg_protocol, pg_types, pg_client
import pg_connection/types
import pg_client/transaction

type AdvisoryLockArgs = object
  ## The key(s) and timeout of a session-level ``withAdvisoryLock*`` call.
  twoKey: bool
  key: int64
  key1, key2: int32
  timeout: Duration

# Internal body templates
#
# The acquire/try/unlock procedures below differ only in the SQL function they
# call and how they touch ``heldSessionLocks``. These templates hold the shared
# body so each public proc collapses to a single documented call. They expand
# inside ``{.async.}`` procs, so the ``await`` runs in the calling proc.

template acquireSessionLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
) =
  discard await conn.queryValue(sql, params, timeout = t)
  conn.noteSessionLockAcquired()

template trySessionLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
): bool =
  let acquired = await conn.queryValue(bool, sql, params, timeout = t)
  if acquired:
    conn.noteSessionLockAcquired()
  acquired

template unlockSessionLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
): bool =
  let released = await conn.queryValue(bool, sql, params, timeout = t)
  conn.noteSessionLockReleased(released)
  released

proc ensureXactScope(conn: PgConnection) {.inline.} =
  # In tsIdle the acquire's implicit tx would commit and drop the lock before
  # body runs, silently losing mutual exclusion.
  if conn.txStatus != tsInTransaction:
    raise newException(
      PgStateError,
      "transaction-level advisory lock requires an active transaction " & "(txStatus: " &
        $conn.txStatus & "); wrap the call in withTransaction",
    )

template acquireXactLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
) =
  ensureXactScope(conn)
  discard await conn.queryValue(sql, params, timeout = t)

template tryXactLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
): bool =
  ensureXactScope(conn)
  await conn.queryValue(bool, sql, params, timeout = t)

# Session-level exclusive locks

proc advisoryLock*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level exclusive advisory lock, blocking until available.
  acquireSessionLock(conn, "SELECT pg_advisory_lock($1)", @[toPgParam(key)], timeout)

proc advisoryTryLock*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level exclusive advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  return
    trySessionLock(conn, "SELECT pg_try_advisory_lock($1)", @[toPgParam(key)], timeout)

proc advisoryUnlock*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level exclusive advisory lock.
  ## Returns ``true`` if the lock was held and successfully released.
  return
    unlockSessionLock(conn, "SELECT pg_advisory_unlock($1)", @[toPgParam(key)], timeout)

# Session-level shared locks

proc advisoryLockShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level shared advisory lock, blocking until available.
  acquireSessionLock(
    conn, "SELECT pg_advisory_lock_shared($1)", @[toPgParam(key)], timeout
  )

proc advisoryTryLockShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level shared advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  return trySessionLock(
    conn, "SELECT pg_try_advisory_lock_shared($1)", @[toPgParam(key)], timeout
  )

proc advisoryUnlockShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level shared advisory lock.
  ## Returns ``true`` if the lock was held and successfully released.
  return unlockSessionLock(
    conn, "SELECT pg_advisory_unlock_shared($1)", @[toPgParam(key)], timeout
  )

proc advisoryUnlockAll*(
    conn: PgConnection, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Release all session-level advisory locks held by the current session.
  discard await conn.exec("SELECT pg_advisory_unlock_all()", timeout = timeout)
  conn.clearSessionLocks()

# Transaction-level exclusive locks

proc advisoryLockXact*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a transaction-level exclusive advisory lock, blocking until available.
  ## Automatically released at end of the current transaction.
  acquireXactLock(conn, "SELECT pg_advisory_xact_lock($1)", @[toPgParam(key)], timeout)

proc advisoryTryLockXact*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level exclusive advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  return tryXactLock(
    conn, "SELECT pg_try_advisory_xact_lock($1)", @[toPgParam(key)], timeout
  )

# Transaction-level shared locks

proc advisoryLockXactShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a transaction-level shared advisory lock, blocking until available.
  ## Automatically released at end of the current transaction.
  acquireXactLock(
    conn, "SELECT pg_advisory_xact_lock_shared($1)", @[toPgParam(key)], timeout
  )

proc advisoryTryLockXactShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level shared advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  return tryXactLock(
    conn, "SELECT pg_try_advisory_xact_lock_shared($1)", @[toPgParam(key)], timeout
  )

# Two-key (int32, int32) variants — Session-level exclusive

proc advisoryLock*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level exclusive advisory lock using two int32 keys.
  acquireSessionLock(
    conn,
    "SELECT pg_advisory_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

proc advisoryTryLock*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level exclusive advisory lock (two int32 keys).
  return trySessionLock(
    conn,
    "SELECT pg_try_advisory_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

proc advisoryUnlock*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level exclusive advisory lock (two int32 keys).
  return unlockSessionLock(
    conn,
    "SELECT pg_advisory_unlock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

# Two-key (int32, int32) variants — Session-level shared

proc advisoryLockShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level shared advisory lock using two int32 keys.
  acquireSessionLock(
    conn,
    "SELECT pg_advisory_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

proc advisoryTryLockShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level shared advisory lock (two int32 keys).
  return trySessionLock(
    conn,
    "SELECT pg_try_advisory_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

proc advisoryUnlockShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level shared advisory lock (two int32 keys).
  return unlockSessionLock(
    conn,
    "SELECT pg_advisory_unlock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

# Two-key (int32, int32) variants — Transaction-level exclusive

proc advisoryLockXact*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a transaction-level exclusive advisory lock (two int32 keys).
  acquireXactLock(
    conn,
    "SELECT pg_advisory_xact_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

proc advisoryTryLockXact*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level exclusive advisory lock (two int32 keys).
  return tryXactLock(
    conn,
    "SELECT pg_try_advisory_xact_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

# Two-key (int32, int32) variants — Transaction-level shared

proc advisoryLockXactShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a transaction-level shared advisory lock (two int32 keys).
  acquireXactLock(
    conn,
    "SELECT pg_advisory_xact_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

proc advisoryTryLockXactShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level shared advisory lock (two int32 keys).
  return tryXactLock(
    conn,
    "SELECT pg_try_advisory_xact_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout,
  )

# Convenience macros — session-level
#
# These are macros (not templates) so that ``conn``, ``key`` etc. are
# evaluated exactly once via ``genSym``-bound ``let`` bindings.
#
# ``advisoryUnlock*`` failures are swallowed so they cannot mask the original
# exception raised by ``body``. The body exception (including ``Defect``) is
# captured and re-raised after the unlock attempt, so the session lock is
# released even on a programming error in the body. A ``finally`` block is
# unusable here: on asyncdispatch a failing ``await`` in a ``finally``
# replaces the in-flight exception, discarding the body's error. Unlock
# failures are reported via the connection's tracer
# (``onAdvisoryUnlockFailed``); if the connection is lost the server releases
# the session lock anyway.
#
# Body ``return`` / ``break`` / ``continue`` that would escape the body are
# rejected at compile time: they would skip the unlock and hold the session
# lock until the connection closes.

proc advisoryLockArgs(key: int64, timeout: Duration = ZeroDuration): AdvisoryLockArgs =
  AdvisoryLockArgs(key: key, timeout: timeout)

proc advisoryLockArgs(
    key1, key2: int32, timeout: Duration = ZeroDuration
): AdvisoryLockArgs =
  AdvisoryLockArgs(twoKey: true, key1: key1, key2: key2, timeout: timeout)

template withAdvisoryLockCore(
    c: PgConnection,
    lockProc, unlockProc: untyped,
    a: AdvisoryLockArgs,
    shared: static bool,
    body: untyped,
) =
  ## Internal helper implementing the acquire/try/finally pattern for all
  ## session-level ``withAdvisoryLock*`` macros. ``c`` and ``a`` must already
  ## be bound to ``let`` symbols by the caller macro.
  const macroName = when shared: "withAdvisoryLockShared" else: "withAdvisoryLock"
  if a.twoKey:
    await c.lockProc(a.key1, a.key2, timeout = a.timeout)
  else:
    await c.lockProc(a.key, timeout = a.timeout)

  var bodyErr: ref CatchableError = nil
  var bodyDefect: ref Defect = nil
  try:
    checkTemplateBodyEscape(body, macroName, "the advisory unlock")
  except CatchableError as e:
    bodyErr = e
  except Defect as d:
    # A ``Defect`` is not a ``CatchableError``: it would skip the unlock and
    # leak the session lock, so capture and re-raise it raw.
    bodyDefect = d

  try:
    var released: bool
    if a.twoKey:
      released = await c.unlockProc(a.key1, a.key2, timeout = a.timeout)
    else:
      released = await c.unlockProc(a.key, timeout = a.timeout)
    if not released:
      # The unlock query succeeded but the server reports the lock was not
      # held (``pg_advisory_unlock*`` returned ``false``). Report it with a
      # nil ``err`` so observers can distinguish it from a raised failure.
      fireAdvisoryUnlockFailed(c, a.key, a.key1, a.key2, shared, a.twoKey, nil)
  except CatchableError as e:
    fireAdvisoryUnlockFailed(c, a.key, a.key1, a.key2, shared, a.twoKey, e)
  except Defect as d:
    # Same-frame Defect from the unlock: surface it only when it can't
    # replace a body error.
    if bodyErr == nil and bodyDefect == nil:
      raise d
    fireAdvisoryUnlockFailed(
      c, a.key, a.key1, a.key2, shared, a.twoKey, newException(PgError, d.msg, d)
    )

  if bodyErr != nil:
    # Re-raise the original body exception now that the lock has been released,
    # so a swallowed unlock failure can never mask it.
    raise bodyErr
  if bodyDefect != nil:
    raise bodyDefect

proc splitAdvisoryLockBody(
    macroName: string, args: NimNode
): tuple[lockArgs: seq[NimNode], body: NimNode] =
  ## Split the lock arguments from the body. Overloads per argument list would
  ## type-check the body as a longer overload's ``timeout``/``key2`` first,
  ## rejecting any type declared in it when it is spliced again.
  if args.len == 0 or args[^1].kind != nnkStmtList:
    error(
      macroName & " expects (conn, key), (conn, key, timeout), (conn, key1, key2) or " &
        "(conn, key1, key2, timeout) followed by the body",
      if args.len == 0:
        args
      else:
        args[^1],
    )
  for i in 0 ..< args.len - 1:
    result.lockArgs.add(args[i])
  result.body = args[^1]

proc buildSessionAdvisoryLock(conn, args: NimNode, shared: bool): NimNode =
  ## Expand a session-level ``withAdvisoryLock`` / ``withAdvisoryLockShared``
  ## call. ``conn``, then the keys and the timeout, are each evaluated once
  ## into ``let`` bindings.
  let macroName = if shared: "withAdvisoryLockShared" else: "withAdvisoryLock"
  let (lockArgs, body) = splitAdvisoryLockBody(macroName, args)
  let (lockProc, unlockProc) =
    if shared:
      (bindSym"advisoryLockShared", bindSym"advisoryUnlockShared")
    else:
      (bindSym"advisoryLock", bindSym"advisoryUnlock")
  let c = genSym(nskLet, "conn")
  let a = genSym(nskLet, "lockArgs")
  let argsCall = newCall(bindSym"advisoryLockArgs", lockArgs)
  # Report a mismatch at the arguments rather than at the body.
  argsCall.copyLineInfo(
    if lockArgs.len > 0:
      lockArgs[0]
    else:
      conn
  )
  newStmtList(
    newLetStmt(c, conn),
    newLetStmt(a, argsCall),
    newCall(
      bindSym"withAdvisoryLockCore", c, lockProc, unlockProc, a, newLit(shared), body
    ),
  )

proc buildXactAdvisoryLock(conn, args: NimNode, shared: bool): NimNode =
  ## Expand a ``withAdvisoryLockXact`` / ``withAdvisoryLockXactShared`` call:
  ## the lock arguments go to the ``advisoryLockXact*`` overloads.
  let macroName = if shared: "withAdvisoryLockXactShared" else: "withAdvisoryLockXact"
  let (lockArgs, body) = splitAdvisoryLockBody(macroName, args)
  let lock = newCall(
    if shared:
      bindSym"advisoryLockXactShared"
    else:
      bindSym"advisoryLockXact",
    conn,
  )
  for arg in lockArgs:
    lock.add(arg)
  lock.copyLineInfo(
    if lockArgs.len > 0:
      lockArgs[0]
    else:
      conn
  )
  quote:
    await `lock`
    `body`

macro withAdvisoryLock*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Acquire a session-level exclusive advisory lock, execute ``body``,
  ## then release the lock (even on exception). Accepts
  ## ``(conn, key: int64)``, ``(conn, key: int64, timeout: Duration)``,
  ## ``(conn, key1, key2: int32)`` or
  ## ``(conn, key1, key2: int32, timeout: Duration)``, followed by the body.
  ## ``timeout`` bounds the lock and unlock queries, not ``body``.
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  buildSessionAdvisoryLock(conn, args, shared = false)

macro withAdvisoryLockShared*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Acquire a session-level shared advisory lock, execute ``body``,
  ## then release the lock (even on exception). Accepts the same argument
  ## lists as ``withAdvisoryLock``.
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  buildSessionAdvisoryLock(conn, args, shared = true)

# Transaction-level convenience macros

macro withAdvisoryLockXact*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Acquire a transaction-level exclusive advisory lock inside a transaction,
  ## execute ``body``. The lock is automatically released at transaction end.
  ## Must be called within ``withTransaction``. Accepts the same argument
  ## lists as ``withAdvisoryLock``.
  buildXactAdvisoryLock(conn, args, shared = false)

macro withAdvisoryLockXactShared*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Acquire a transaction-level shared advisory lock inside a transaction,
  ## execute ``body``. The lock is automatically released at transaction end.
  ## Must be called within ``withTransaction``. Accepts the same argument
  ## lists as ``withAdvisoryLock``.
  buildXactAdvisoryLock(conn, args, shared = true)
