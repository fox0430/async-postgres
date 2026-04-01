## Advisory lock example.
##
## Demonstrates PostgreSQL advisory locks for application-level
## distributed locking, including try-lock and scoped locking.
##
## Usage:
##   nim c -r examples/advisory_lock.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  let conn = await connect(Dsn)
  defer:
    await conn.close()

  # Acquire and release an exclusive advisory lock (session-level)
  let lockKey = 42'i64
  await conn.advisoryLock(lockKey)
  echo "Acquired advisory lock: key=", lockKey

  discard await conn.advisoryUnlock(lockKey)
  echo "Released advisory lock: key=", lockKey

  # Try-lock: non-blocking acquisition
  let acquired = await conn.advisoryTryLock(100'i64)
  echo "\nTry-lock key=100: acquired=", acquired
  if acquired:
    discard await conn.advisoryUnlock(100'i64)

  # Two-key variant (int32, int32)
  await conn.advisoryLock(1'i32, 2'i32)
  echo "\nAcquired two-key lock: (1, 2)"
  discard await conn.advisoryUnlock(1'i32, 2'i32)
  echo "Released two-key lock: (1, 2)"

  # Scoped locking with withAdvisoryLock macro
  conn.withAdvisoryLock(200'i64):
    echo "\nInside withAdvisoryLock (key=200)"
    # Lock is automatically released when the block exits
  echo "Lock released after withAdvisoryLock"

waitFor main()
