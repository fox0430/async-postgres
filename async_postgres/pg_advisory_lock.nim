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
## **Pool integration:** Session-level lock acquires through this typed API
## bump a per-connection counter, and the pool releases or discards the
## connection on return so that locks never leak to subsequent borrowers.
## Raw-SQL acquires (e.g. ``conn.exec("SELECT pg_advisory_lock(1)")``) bypass
## this tracking — callers must release them explicitly or invoke
## ``advisoryUnlockAll`` before returning the connection to the pool.
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

import std/[macros, importutils]

import async_backend, pg_types, pg_connection, pg_client

privateAccess(PgConnection)

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
  inc conn.heldSessionLocks

template trySessionLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
): bool =
  let acquired = await conn.queryValue(bool, sql, params, timeout = t)
  if acquired:
    inc conn.heldSessionLocks
  acquired

template unlockSessionLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
): bool =
  let released = await conn.queryValue(bool, sql, params, timeout = t)
  if released and conn.heldSessionLocks > 0:
    dec conn.heldSessionLocks
  released

template acquireXactLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
) =
  discard await conn.queryValue(sql, params, timeout = t)

template tryXactLock(
    conn: PgConnection, sql: string, params: seq[PgParam], t: Duration
): bool =
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
  conn.heldSessionLocks = 0

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
# exception raised by ``body``: the body exception is captured and re-raised
# after the unlock attempt. (A ``finally`` block cannot be used here because an
# ``await`` inside ``finally`` clobbers the in-flight exception under
# asyncdispatch, silently discarding the body's error.) The unlock failure is
# reported through the connection's tracer (``onAdvisoryUnlockFailed``). If the
# connection is lost the session lock is released server-side anyway.

template withAdvisoryLockCore(
    c: PgConnection,
    lockProc, unlockProc: untyped,
    k: int64,
    k1, k2: int32,
    shared, twoKey, hasTimeout: static bool,
    t: Duration,
    body: untyped,
) =
  ## Internal helper implementing the acquire/try/finally pattern for all
  ## session-level ``withAdvisoryLock*`` macros. ``c``, ``k``/``k1``/``k2``
  ## must already be bound to ``let`` symbols by the caller macro.
  when hasTimeout:
    when twoKey:
      await c.lockProc(k1, k2, timeout = t)
    else:
      await c.lockProc(k, timeout = t)
  else:
    when twoKey:
      await c.lockProc(k1, k2)
    else:
      await c.lockProc(k)

  var bodyErr: ref CatchableError = nil
  try:
    body
  except CatchableError as e:
    bodyErr = e

  try:
    var released: bool
    when twoKey:
      released = await c.unlockProc(k1, k2)
    else:
      released = await c.unlockProc(k)
    if not released:
      # The unlock query succeeded but the server reports the lock was not
      # held (``pg_advisory_unlock*`` returned ``false``). Report it with a
      # nil ``err`` so observers can distinguish it from a raised failure.
      fireAdvisoryUnlockFailed(c, k, k1, k2, shared, twoKey, nil)
  except CatchableError as e:
    fireAdvisoryUnlockFailed(c, k, k1, k2, shared, twoKey, e)

  if bodyErr != nil:
    # Re-raise the original body exception (if any) now that the lock has been
    # released, so a swallowed unlock failure can never mask it.
    raise bodyErr

macro withAdvisoryLock*(conn: PgConnection, key: int64, body: untyped): untyped =
  ## Acquire a session-level exclusive advisory lock, execute ``body``,
  ## then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k = genSym(nskLet, "key")
  let connExpr = conn
  let keyExpr = key
  result = quote:
    let `c` = `connExpr`
    let `k` = `keyExpr`
    withAdvisoryLockCore(
      `c`, advisoryLock, advisoryUnlock, `k`, 0'i32, 0'i32, false, false, false,
      ZeroDuration,
    ):
      `body`

macro withAdvisoryLock*(
    conn: PgConnection, key: int64, timeout: Duration, body: untyped
): untyped =
  ## Acquire a session-level exclusive advisory lock with a timeout,
  ## execute ``body``, then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k = genSym(nskLet, "key")
  let t = genSym(nskLet, "timeout")
  let connExpr = conn
  let keyExpr = key
  let timeoutExpr = timeout
  result = quote:
    let `c` = `connExpr`
    let `k` = `keyExpr`
    let `t` = `timeoutExpr`
    withAdvisoryLockCore(
      `c`, advisoryLock, advisoryUnlock, `k`, 0'i32, 0'i32, false, false, true, `t`
    ):
      `body`

macro withAdvisoryLock*(conn: PgConnection, key1, key2: int32, body: untyped): untyped =
  ## Acquire a session-level exclusive advisory lock (two int32 keys),
  ## execute ``body``, then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k1 = genSym(nskLet, "key1")
  let k2 = genSym(nskLet, "key2")
  let connExpr = conn
  let key1Expr = key1
  let key2Expr = key2
  result = quote:
    let `c` = `connExpr`
    let `k1` = `key1Expr`
    let `k2` = `key2Expr`
    withAdvisoryLockCore(
      `c`, advisoryLock, advisoryUnlock, 0'i64, `k1`, `k2`, false, true, false,
      ZeroDuration,
    ):
      `body`

macro withAdvisoryLock*(
    conn: PgConnection, key1, key2: int32, timeout: Duration, body: untyped
): untyped =
  ## Acquire a session-level exclusive advisory lock (two int32 keys)
  ## with a timeout, execute ``body``, then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k1 = genSym(nskLet, "key1")
  let k2 = genSym(nskLet, "key2")
  let t = genSym(nskLet, "timeout")
  let connExpr = conn
  let key1Expr = key1
  let key2Expr = key2
  let timeoutExpr = timeout
  result = quote:
    let `c` = `connExpr`
    let `k1` = `key1Expr`
    let `k2` = `key2Expr`
    let `t` = `timeoutExpr`
    withAdvisoryLockCore(
      `c`, advisoryLock, advisoryUnlock, 0'i64, `k1`, `k2`, false, true, true, `t`
    ):
      `body`

macro withAdvisoryLockShared*(conn: PgConnection, key: int64, body: untyped): untyped =
  ## Acquire a session-level shared advisory lock, execute ``body``,
  ## then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k = genSym(nskLet, "key")
  let connExpr = conn
  let keyExpr = key
  result = quote:
    let `c` = `connExpr`
    let `k` = `keyExpr`
    withAdvisoryLockCore(
      `c`, advisoryLockShared, advisoryUnlockShared, `k`, 0'i32, 0'i32, true, false,
      false, ZeroDuration,
    ):
      `body`

macro withAdvisoryLockShared*(
    conn: PgConnection, key: int64, timeout: Duration, body: untyped
): untyped =
  ## Acquire a session-level shared advisory lock with a timeout,
  ## execute ``body``, then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k = genSym(nskLet, "key")
  let t = genSym(nskLet, "timeout")
  let connExpr = conn
  let keyExpr = key
  let timeoutExpr = timeout
  result = quote:
    let `c` = `connExpr`
    let `k` = `keyExpr`
    let `t` = `timeoutExpr`
    withAdvisoryLockCore(
      `c`, advisoryLockShared, advisoryUnlockShared, `k`, 0'i32, 0'i32, true, false,
      true, `t`,
    ):
      `body`

macro withAdvisoryLockShared*(
    conn: PgConnection, key1, key2: int32, body: untyped
): untyped =
  ## Acquire a session-level shared advisory lock (two int32 keys),
  ## execute ``body``, then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k1 = genSym(nskLet, "key1")
  let k2 = genSym(nskLet, "key2")
  let connExpr = conn
  let key1Expr = key1
  let key2Expr = key2
  result = quote:
    let `c` = `connExpr`
    let `k1` = `key1Expr`
    let `k2` = `key2Expr`
    withAdvisoryLockCore(
      `c`, advisoryLockShared, advisoryUnlockShared, 0'i64, `k1`, `k2`, true, true,
      false, ZeroDuration,
    ):
      `body`

macro withAdvisoryLockShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration, body: untyped
): untyped =
  ## Acquire a session-level shared advisory lock (two int32 keys)
  ## with a timeout, execute ``body``, then release the lock (even on exception).
  ##
  ## If unlocking fails (for example because the connection was lost), the
  ## failure is reported through the connection's tracer
  ## (``onAdvisoryUnlockFailed``) so the original exception from ``body`` is
  ## not masked.
  let c = genSym(nskLet, "conn")
  let k1 = genSym(nskLet, "key1")
  let k2 = genSym(nskLet, "key2")
  let t = genSym(nskLet, "timeout")
  let connExpr = conn
  let key1Expr = key1
  let key2Expr = key2
  let timeoutExpr = timeout
  result = quote:
    let `c` = `connExpr`
    let `k1` = `key1Expr`
    let `k2` = `key2Expr`
    let `t` = `timeoutExpr`
    withAdvisoryLockCore(
      `c`, advisoryLockShared, advisoryUnlockShared, 0'i64, `k1`, `k2`, true, true,
      true, `t`,
    ):
      `body`

# Transaction-level convenience templates

template withAdvisoryLockXact*(conn: PgConnection, key: int64, body: untyped) =
  ## Acquire a transaction-level exclusive advisory lock inside a transaction,
  ## execute ``body``. The lock is automatically released at transaction end.
  ## Must be called within ``withTransaction``.
  await conn.advisoryLockXact(key)
  body

template withAdvisoryLockXact*(
    conn: PgConnection, key: int64, timeout: Duration, body: untyped
) =
  ## Acquire a transaction-level exclusive advisory lock with a timeout
  ## inside a transaction, execute ``body``. The lock is automatically
  ## released at transaction end. Must be called within ``withTransaction``.
  await conn.advisoryLockXact(key, timeout = timeout)
  body

template withAdvisoryLockXact*(conn: PgConnection, key1, key2: int32, body: untyped) =
  ## Acquire a transaction-level exclusive advisory lock (two int32 keys)
  ## inside a transaction, execute ``body``. The lock is automatically
  ## released at transaction end. Must be called within ``withTransaction``.
  await conn.advisoryLockXact(key1, key2)
  body

template withAdvisoryLockXact*(
    conn: PgConnection, key1, key2: int32, timeout: Duration, body: untyped
) =
  ## Acquire a transaction-level exclusive advisory lock (two int32 keys)
  ## with a timeout inside a transaction, execute ``body``. The lock is
  ## automatically released at transaction end.
  ## Must be called within ``withTransaction``.
  await conn.advisoryLockXact(key1, key2, timeout = timeout)
  body

template withAdvisoryLockXactShared*(conn: PgConnection, key: int64, body: untyped) =
  ## Acquire a transaction-level shared advisory lock inside a transaction,
  ## execute ``body``. The lock is automatically released at transaction end.
  ## Must be called within ``withTransaction``.
  await conn.advisoryLockXactShared(key)
  body

template withAdvisoryLockXactShared*(
    conn: PgConnection, key: int64, timeout: Duration, body: untyped
) =
  ## Acquire a transaction-level shared advisory lock with a timeout
  ## inside a transaction, execute ``body``. The lock is automatically
  ## released at transaction end. Must be called within ``withTransaction``.
  await conn.advisoryLockXactShared(key, timeout = timeout)
  body

template withAdvisoryLockXactShared*(
    conn: PgConnection, key1, key2: int32, body: untyped
) =
  ## Acquire a transaction-level shared advisory lock (two int32 keys)
  ## inside a transaction, execute ``body``. The lock is automatically
  ## released at transaction end. Must be called within ``withTransaction``.
  await conn.advisoryLockXactShared(key1, key2)
  body

template withAdvisoryLockXactShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration, body: untyped
) =
  ## Acquire a transaction-level shared advisory lock (two int32 keys)
  ## with a timeout inside a transaction, execute ``body``. The lock is
  ## automatically released at transaction end.
  ## Must be called within ``withTransaction``.
  await conn.advisoryLockXactShared(key1, key2, timeout = timeout)
  body
