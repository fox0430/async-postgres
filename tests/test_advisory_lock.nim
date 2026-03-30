import std/unittest

import ../async_postgres/[async_backend, pg_connection, pg_client, pg_advisory_lock]

const
  PgHost = "127.0.0.1"
  PgPort = 15432
  PgUser = "test"
  PgPassword = "test"
  PgDatabase = "test"

proc plainConfig(): ConnConfig =
  ConnConfig(
    host: PgHost,
    port: PgPort,
    user: PgUser,
    password: PgPassword,
    database: PgDatabase,
    sslMode: sslDisable,
  )

suite "Advisory Lock: session-level exclusive (int64)":
  test "lock and unlock":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      await conn.advisoryLock(12345'i64)
      let released = await conn.advisoryUnlock(12345'i64)
      doAssert released

    waitFor t()

  test "tryLock succeeds when not held":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      let acquired = await conn.advisoryTryLock(12346'i64)
      doAssert acquired
      discard await conn.advisoryUnlock(12346'i64)

    waitFor t()

  test "tryLock fails when held by another session":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      await conn1.advisoryLock(12347'i64)
      let acquired = await conn2.advisoryTryLock(12347'i64)
      doAssert not acquired
      discard await conn1.advisoryUnlock(12347'i64)

    waitFor t()

  test "unlock returns false when not held":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      let released = await conn.advisoryUnlock(99999'i64)
      doAssert not released

    waitFor t()

suite "Advisory Lock: session-level shared (int64)":
  test "multiple sessions can hold shared lock":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      await conn1.advisoryLockShared(20001'i64)
      let acquired = await conn2.advisoryTryLockShared(20001'i64)
      doAssert acquired
      discard await conn1.advisoryUnlockShared(20001'i64)
      discard await conn2.advisoryUnlockShared(20001'i64)

    waitFor t()

  test "exclusive blocked by shared":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      await conn1.advisoryLockShared(20002'i64)
      let acquired = await conn2.advisoryTryLock(20002'i64)
      doAssert not acquired
      discard await conn1.advisoryUnlockShared(20002'i64)

    waitFor t()

suite "Advisory Lock: unlockAll":
  test "releases all session locks":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      await conn1.advisoryLock(30001'i64)
      await conn1.advisoryLock(30002'i64)
      await conn1.advisoryUnlockAll()
      let a1 = await conn2.advisoryTryLock(30001'i64)
      let a2 = await conn2.advisoryTryLock(30002'i64)
      doAssert a1
      doAssert a2
      discard await conn2.advisoryUnlock(30001'i64)
      discard await conn2.advisoryUnlock(30002'i64)

    waitFor t()

suite "Advisory Lock: transaction-level (int64)":
  test "xact lock released on commit":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withTransaction:
        await conn1.advisoryLockXact(40001'i64)
        let held = await conn2.advisoryTryLock(40001'i64)
        doAssert not held
      # After transaction, lock is released
      let acquired = await conn2.advisoryTryLock(40001'i64)
      doAssert acquired
      discard await conn2.advisoryUnlock(40001'i64)

    waitFor t()

  test "xact tryLock":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        let acquired = await conn.advisoryTryLockXact(40002'i64)
        doAssert acquired

    waitFor t()

  test "xact shared lock":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withTransaction:
        await conn1.advisoryLockXactShared(40003'i64)
        let acquired = await conn2.advisoryTryLockShared(40003'i64)
        doAssert acquired
        discard await conn2.advisoryUnlockShared(40003'i64)

    waitFor t()

suite "Advisory Lock: two-key (int32, int32)":
  test "lock and unlock":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      await conn.advisoryLock(1'i32, 2'i32)
      let released = await conn.advisoryUnlock(1'i32, 2'i32)
      doAssert released

    waitFor t()

  test "tryLock blocked by another session":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      await conn1.advisoryLock(3'i32, 4'i32)
      let acquired = await conn2.advisoryTryLock(3'i32, 4'i32)
      doAssert not acquired
      discard await conn1.advisoryUnlock(3'i32, 4'i32)

    waitFor t()

  test "shared two-key":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      await conn1.advisoryLockShared(5'i32, 6'i32)
      let acquired = await conn2.advisoryTryLockShared(5'i32, 6'i32)
      doAssert acquired
      discard await conn1.advisoryUnlockShared(5'i32, 6'i32)
      discard await conn2.advisoryUnlockShared(5'i32, 6'i32)

    waitFor t()

  test "xact two-key":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        await conn.advisoryLockXact(7'i32, 8'i32)
        let acquired = await conn.advisoryTryLockXact(7'i32, 8'i32)
        doAssert acquired

    waitFor t()

  test "xact shared two-key":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withTransaction:
        await conn1.advisoryLockXactShared(9'i32, 10'i32)
        let acquired = await conn2.advisoryTryLockXactShared(9'i32, 10'i32)
        doAssert acquired

    waitFor t()

suite "Advisory Lock: withAdvisoryLock template":
  test "withAdvisoryLock int64":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withAdvisoryLock(50001'i64):
        let held = await conn2.advisoryTryLock(50001'i64)
        doAssert not held
      # After block, lock is released
      let released = await conn2.advisoryTryLock(50001'i64)
      doAssert released
      discard await conn2.advisoryUnlock(50001'i64)

    waitFor t()

  test "withAdvisoryLock two-key":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withAdvisoryLock(11'i32, 12'i32):
        let held = await conn2.advisoryTryLock(11'i32, 12'i32)
        doAssert not held
      let released = await conn2.advisoryTryLock(11'i32, 12'i32)
      doAssert released
      discard await conn2.advisoryUnlock(11'i32, 12'i32)

    waitFor t()

  test "withAdvisoryLockShared int64":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withAdvisoryLockShared(50002'i64):
        let acquired = await conn2.advisoryTryLockShared(50002'i64)
        doAssert acquired
        discard await conn2.advisoryUnlockShared(50002'i64)

    waitFor t()

  test "withAdvisoryLockShared two-key":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withAdvisoryLockShared(13'i32, 14'i32):
        let acquired = await conn2.advisoryTryLockShared(13'i32, 14'i32)
        doAssert acquired
        discard await conn2.advisoryUnlockShared(13'i32, 14'i32)

    waitFor t()

  test "withAdvisoryLock releases on exception":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      try:
        conn1.withAdvisoryLock(50003'i64):
          raise newException(CatchableError, "test error")
      except CatchableError:
        discard
      # Lock should be released despite exception
      let acquired = await conn2.advisoryTryLock(50003'i64)
      doAssert acquired
      discard await conn2.advisoryUnlock(50003'i64)

    waitFor t()

suite "Advisory Lock: withAdvisoryLockXact template":
  test "withAdvisoryLockXact int64":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withTransaction:
        conn1.withAdvisoryLockXact(60001'i64):
          let held = await conn2.advisoryTryLock(60001'i64)
          doAssert not held
      # After transaction, lock is released
      let released = await conn2.advisoryTryLock(60001'i64)
      doAssert released
      discard await conn2.advisoryUnlock(60001'i64)

    waitFor t()

  test "withAdvisoryLockXact two-key":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withTransaction:
        conn1.withAdvisoryLockXact(15'i32, 16'i32):
          let held = await conn2.advisoryTryLock(15'i32, 16'i32)
          doAssert not held
      let released = await conn2.advisoryTryLock(15'i32, 16'i32)
      doAssert released
      discard await conn2.advisoryUnlock(15'i32, 16'i32)

    waitFor t()

  test "withAdvisoryLockXactShared int64":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withTransaction:
        conn1.withAdvisoryLockXactShared(60002'i64):
          let acquired = await conn2.advisoryTryLockShared(60002'i64)
          doAssert acquired
          discard await conn2.advisoryUnlockShared(60002'i64)

    waitFor t()

  test "withAdvisoryLockXactShared two-key":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      conn1.withTransaction:
        conn1.withAdvisoryLockXactShared(17'i32, 18'i32):
          let acquired = await conn2.advisoryTryLockXactShared(17'i32, 18'i32)
          doAssert acquired

    waitFor t()
