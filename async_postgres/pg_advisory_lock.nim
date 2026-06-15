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

# Session-level exclusive locks

proc advisoryLock*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level exclusive advisory lock, blocking until available.
  discard await conn.queryValue(
    "SELECT pg_advisory_lock($1)", @[toPgParam(key)], timeout = timeout
  )
  inc conn.heldSessionLocks

proc advisoryTryLock*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level exclusive advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  let acquired = await conn.queryValue(
    bool, "SELECT pg_try_advisory_lock($1)", @[toPgParam(key)], timeout = timeout
  )
  if acquired:
    inc conn.heldSessionLocks
  return acquired

proc advisoryUnlock*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level exclusive advisory lock.
  ## Returns ``true`` if the lock was held and successfully released.
  let released = await conn.queryValue(
    bool, "SELECT pg_advisory_unlock($1)", @[toPgParam(key)], timeout = timeout
  )
  if released and conn.heldSessionLocks > 0:
    dec conn.heldSessionLocks
  return released

# Session-level shared locks

proc advisoryLockShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level shared advisory lock, blocking until available.
  discard await conn.queryValue(
    "SELECT pg_advisory_lock_shared($1)", @[toPgParam(key)], timeout = timeout
  )
  inc conn.heldSessionLocks

proc advisoryTryLockShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level shared advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  let acquired = await conn.queryValue(
    bool, "SELECT pg_try_advisory_lock_shared($1)", @[toPgParam(key)], timeout = timeout
  )
  if acquired:
    inc conn.heldSessionLocks
  return acquired

proc advisoryUnlockShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level shared advisory lock.
  ## Returns ``true`` if the lock was held and successfully released.
  let released = await conn.queryValue(
    bool, "SELECT pg_advisory_unlock_shared($1)", @[toPgParam(key)], timeout = timeout
  )
  if released and conn.heldSessionLocks > 0:
    dec conn.heldSessionLocks
  return released

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
  discard await conn.queryValue(
    "SELECT pg_advisory_xact_lock($1)", @[toPgParam(key)], timeout = timeout
  )

proc advisoryTryLockXact*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level exclusive advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  return await conn.queryValue(
    bool, "SELECT pg_try_advisory_xact_lock($1)", @[toPgParam(key)], timeout = timeout
  )

# Transaction-level shared locks

proc advisoryLockXactShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a transaction-level shared advisory lock, blocking until available.
  ## Automatically released at end of the current transaction.
  discard await conn.queryValue(
    "SELECT pg_advisory_xact_lock_shared($1)", @[toPgParam(key)], timeout = timeout
  )

proc advisoryTryLockXactShared*(
    conn: PgConnection, key: int64, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level shared advisory lock without blocking.
  ## Returns ``true`` if the lock was acquired.
  return await conn.queryValue(
    bool,
    "SELECT pg_try_advisory_xact_lock_shared($1)",
    @[toPgParam(key)],
    timeout = timeout,
  )

# Two-key (int32, int32) variants — Session-level exclusive

proc advisoryLock*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level exclusive advisory lock using two int32 keys.
  discard await conn.queryValue(
    "SELECT pg_advisory_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )
  inc conn.heldSessionLocks

proc advisoryTryLock*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level exclusive advisory lock (two int32 keys).
  let acquired = await conn.queryValue(
    bool,
    "SELECT pg_try_advisory_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )
  if acquired:
    inc conn.heldSessionLocks
  return acquired

proc advisoryUnlock*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level exclusive advisory lock (two int32 keys).
  let released = await conn.queryValue(
    bool,
    "SELECT pg_advisory_unlock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )
  if released and conn.heldSessionLocks > 0:
    dec conn.heldSessionLocks
  return released

# Two-key (int32, int32) variants — Session-level shared

proc advisoryLockShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a session-level shared advisory lock using two int32 keys.
  discard await conn.queryValue(
    "SELECT pg_advisory_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )
  inc conn.heldSessionLocks

proc advisoryTryLockShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a session-level shared advisory lock (two int32 keys).
  let acquired = await conn.queryValue(
    bool,
    "SELECT pg_try_advisory_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )
  if acquired:
    inc conn.heldSessionLocks
  return acquired

proc advisoryUnlockShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Release a session-level shared advisory lock (two int32 keys).
  let released = await conn.queryValue(
    bool,
    "SELECT pg_advisory_unlock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )
  if released and conn.heldSessionLocks > 0:
    dec conn.heldSessionLocks
  return released

# Two-key (int32, int32) variants — Transaction-level exclusive

proc advisoryLockXact*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a transaction-level exclusive advisory lock (two int32 keys).
  discard await conn.queryValue(
    "SELECT pg_advisory_xact_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )

proc advisoryTryLockXact*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level exclusive advisory lock (two int32 keys).
  return await conn.queryValue(
    bool,
    "SELECT pg_try_advisory_xact_lock($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )

# Two-key (int32, int32) variants — Transaction-level shared

proc advisoryLockXactShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[void] {.async.} =
  ## Acquire a transaction-level shared advisory lock (two int32 keys).
  discard await conn.queryValue(
    "SELECT pg_advisory_xact_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )

proc advisoryTryLockXactShared*(
    conn: PgConnection, key1, key2: int32, timeout: Duration = ZeroDuration
): Future[bool] {.async.} =
  ## Try to acquire a transaction-level shared advisory lock (two int32 keys).
  return await conn.queryValue(
    bool,
    "SELECT pg_try_advisory_xact_lock_shared($1, $2)",
    @[toPgParam(key1), toPgParam(key2)],
    timeout = timeout,
  )

# Convenience macros — session-level
#
# These are macros (not templates) so that ``conn``, ``key`` etc. are
# evaluated exactly once via ``genSym``-bound ``let`` bindings.
#
# In the ``finally`` blocks, ``advisoryUnlock*`` failures are swallowed so
# they cannot mask the original exception raised by ``body``. The failure is
# reported through the connection's tracer (``onAdvisoryUnlockFailed``). If
# the connection is lost the session lock is released server-side anyway.

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

  try:
    body
  finally:
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
