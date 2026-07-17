import std/[unittest, importutils, deques]

import ../async_postgres/pg_pool {.all.}
import ../async_postgres/[async_backend, pg_connection, pg_client, pg_advisory_lock]

privateAccess(PgPool)
privateAccess(PgConnection)
privateAccess(PooledConn)

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

  test "xact lock outside transaction raises PgStateError":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      doAssertRaises(PgStateError):
        waitFor conn.advisoryLockXact(40010'i64)
      doAssertRaises(PgStateError):
        waitFor conn.advisoryLockXactShared(40011'i64)
      doAssertRaises(PgStateError):
        discard waitFor conn.advisoryTryLockXact(40012'i64)
      doAssertRaises(PgStateError):
        discard waitFor conn.advisoryTryLockXactShared(40013'i64)
      doAssertRaises(PgStateError):
        waitFor conn.advisoryLockXact(1'i32, 2'i32)
      doAssertRaises(PgStateError):
        waitFor conn.advisoryLockXactShared(3'i32, 4'i32)
      doAssertRaises(PgStateError):
        discard waitFor conn.advisoryTryLockXact(5'i32, 6'i32)
      doAssertRaises(PgStateError):
        discard waitFor conn.advisoryTryLockXactShared(7'i32, 8'i32)

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
        let acquired = await conn2.advisoryTryLockShared(9'i32, 10'i32)
        doAssert acquired
        discard await conn2.advisoryUnlockShared(9'i32, 10'i32)

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

  test "withAdvisoryLock preserves body exception when unlock fails":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLock(50004'i64):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      # The unlock attempt fails because the connection is closed, but the
      # original body exception must still propagate.
      doAssert bodyMsg == "original body error"

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
          let acquired = await conn2.advisoryTryLockShared(17'i32, 18'i32)
          doAssert acquired
          discard await conn2.advisoryUnlockShared(17'i32, 18'i32)

    waitFor t()

suite "Advisory Lock: counter accounting":
  test "advisoryLock increments counter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      doAssert conn.heldSessionLocks == 0
      await conn.advisoryLock(70001'i64)
      doAssert conn.heldSessionLocks == 1
      discard await conn.advisoryUnlock(70001'i64)
      doAssert conn.heldSessionLocks == 0

    waitFor t()

  test "advisoryTryLock increments only on success":
    proc t() {.async.} =
      let conn1 = await connect(plainConfig())
      let conn2 = await connect(plainConfig())
      defer:
        await conn1.close()
        await conn2.close()
      await conn1.advisoryLock(70002'i64)
      doAssert conn1.heldSessionLocks == 1
      # conn2 cannot acquire; counter stays at 0.
      let got = await conn2.advisoryTryLock(70002'i64)
      doAssert not got
      doAssert conn2.heldSessionLocks == 0
      discard await conn1.advisoryUnlock(70002'i64)

    waitFor t()

  test "recursive acquire stacks the counter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      await conn.advisoryLock(70003'i64)
      await conn.advisoryLock(70003'i64)
      doAssert conn.heldSessionLocks == 2
      discard await conn.advisoryUnlock(70003'i64)
      doAssert conn.heldSessionLocks == 1
      discard await conn.advisoryUnlock(70003'i64)
      doAssert conn.heldSessionLocks == 0

    waitFor t()

  test "advisoryUnlock returning false does not decrement":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      await conn.advisoryLock(70004'i64)
      doAssert conn.heldSessionLocks == 1
      # Unlock a key that is not held — server returns false.
      let released = await conn.advisoryUnlock(70099'i64)
      doAssert not released
      doAssert conn.heldSessionLocks == 1
      discard await conn.advisoryUnlock(70004'i64)
      doAssert conn.heldSessionLocks == 0

    waitFor t()

  test "advisoryUnlockAll zeroes the counter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      await conn.advisoryLock(70005'i64)
      await conn.advisoryLockShared(70006'i64)
      doAssert conn.heldSessionLocks == 2
      await conn.advisoryUnlockAll()
      doAssert conn.heldSessionLocks == 0

    waitFor t()

  test "two-key int32 variants track the counter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      await conn.advisoryLock(701'i32, 702'i32)
      doAssert conn.heldSessionLocks == 1
      discard await conn.advisoryUnlock(701'i32, 702'i32)
      doAssert conn.heldSessionLocks == 0

    waitFor t()

  test "xact-level lock does not affect counter":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      defer:
        await conn.close()
      conn.withTransaction:
        await conn.advisoryLockXact(70007'i64)
        doAssert conn.heldSessionLocks == 0
      doAssert conn.heldSessionLocks == 0

    waitFor t()

suite "Advisory Lock: pool integration":
  test "pool discards connection that still holds a session lock":
    proc t() {.async.} =
      let cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      let conn = await pool.acquire()
      await conn.advisoryLock(71001'i64)
      let closeBefore = pool.metrics.closeCount
      conn.release()
      doAssert pool.idle.len == 0
      doAssert pool.metrics.closeCount - closeBefore == 1

      # A different session can immediately take the same key.
      let probe = await connect(plainConfig())
      defer:
        await probe.close()
      let acquired = await probe.advisoryTryLock(71001'i64)
      doAssert acquired
      discard await probe.advisoryUnlock(71001'i64)

    waitFor t()

  test "explicit unlock alone does not spare direct release from discarding":
    # advisoryUnlock decrements the counter but not the sticky dirty flag,
    # so a direct release() (bypassing resetSession) still discards the
    # connection. Use advisoryUnlockAll or withConnection to keep it.
    proc t() {.async.} =
      let cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      let conn = await pool.acquire()
      await conn.advisoryLock(71002'i64)
      discard await conn.advisoryUnlock(71002'i64)
      doAssert conn.heldSessionLocks == 0
      doAssert conn.sessionLockDirty
      let closeBefore = pool.metrics.closeCount
      conn.release()
      doAssert pool.idle.len == 0
      doAssert pool.metrics.closeCount - closeBefore == 1

    waitFor t()

  test "advisoryUnlockAll lets the pool reuse the connection":
    proc t() {.async.} =
      let cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      let conn = await pool.acquire()
      await conn.advisoryLock(71003'i64)
      await conn.advisoryLockShared(71004'i64)
      await conn.advisoryUnlockAll()
      doAssert conn.heldSessionLocks == 0
      let closeBefore = pool.metrics.closeCount
      conn.release()
      doAssert pool.idle.len == 1
      doAssert pool.metrics.closeCount == closeBefore

    waitFor t()

  test "withAdvisoryLock through pool acquires releases on exit":
    proc t() {.async.} =
      let cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      pool.withConnection(conn):
        conn.withAdvisoryLock(71005'i64):
          doAssert conn.heldSessionLocks == 1
        doAssert conn.heldSessionLocks == 0

      doAssert pool.idle.len == 1

    waitFor t()

  test "resetQuery path unlocks via pg_advisory_unlock_all then runs reset":
    proc t() {.async.} =
      let cfg = initPoolConfig(
        plainConfig(), minSize = 0, maxSize = 1, resetQuery = "DISCARD ALL"
      )
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      pool.withConnection(conn):
        await conn.advisoryLock(71006'i64)
      # resetSession ran (via withConnection finally) and cleared the counter.
      doAssert pool.idle.len == 1
      let pooled = pool.idle[0]
      doAssert pooled.conn.heldSessionLocks == 0

      # Different session can now grab the key.
      let probe = await connect(plainConfig())
      defer:
        await probe.close()
      let acquired = await probe.advisoryTryLock(71006'i64)
      doAssert acquired
      discard await probe.advisoryUnlock(71006'i64)

    waitFor t()

  test "typed unlock of raw-acquired key does not leak the tracked lock":
    # Mixed-usage regression: raw pg_advisory_lock followed by typed
    # advisoryUnlock decrements the counter but must not clear the sticky
    # dirty flag, so the still-held tracked lock forces a pool-side cleanup
    # rather than being handed to the next borrower.
    proc t() {.async.} =
      let cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      let conn = await pool.acquire()
      await conn.advisoryLock(71007'i64) # tracked K1
      discard await conn.exec("SELECT pg_advisory_lock(71008)") # raw K2
      let released = await conn.advisoryUnlock(71008'i64) # typed unlock of raw K2
      doAssert released
      doAssert conn.heldSessionLocks == 0 # counter stolen, but…
      doAssert conn.sessionLockDirty # …dirty flag survives.

      let closeBefore = pool.metrics.closeCount
      conn.release()
      doAssert pool.idle.len == 0
      doAssert pool.metrics.closeCount - closeBefore == 1

      # K1 is released server-side by the close, so a fresh session can take it.
      let probe = await connect(plainConfig())
      defer:
        await probe.close()
      doAssert await probe.advisoryTryLock(71007'i64)
      discard await probe.advisoryUnlock(71007'i64)

    waitFor t()

  test "typed unlock of raw-acquired key: resetSession path releases via unlock_all":
    proc t() {.async.} =
      let cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      pool.withConnection(conn):
        await conn.advisoryLock(71009'i64) # tracked K1
        discard await conn.exec("SELECT pg_advisory_lock(71010)") # raw K2
        discard await conn.advisoryUnlock(71010'i64) # steals counter to 0
      # resetSession keyed off dirty flag → unlock_all cleared K1 too.
      doAssert pool.idle.len == 1
      doAssert not pool.idle[0].conn.sessionLockDirty
      doAssert pool.idle[0].conn.heldSessionLocks == 0

      let probe = await connect(plainConfig())
      defer:
        await probe.close()
      doAssert await probe.advisoryTryLock(71009'i64)
      discard await probe.advisoryUnlock(71009'i64)

    waitFor t()

suite "Advisory Lock: onLeakedSessionLocks tracer hook":
  type LeakLog = ref object
    counts: seq[int]

  proc buildLeakTracer(log: LeakLog): PgTracer =
    let tracer = PgTracer()
    tracer.onLeakedSessionLocks = proc(
        data: TraceLeakedSessionLocksData
    ) {.gcsafe, raises: [].} =
      log.counts.add(data.count)
    tracer

  test "resetSession path fires hook with leaked lock count":
    proc t() {.async.} =
      let log = LeakLog()
      var cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      cfg.tracer = buildLeakTracer(log)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      pool.withConnection(conn):
        await conn.advisoryLock(72001'i64)
        await conn.advisoryLockShared(72002'i64)
      # withConnection finally → resetSession → onLeakedSessionLocks fires once,
      # pg_advisory_unlock_all clears the counter, and the connection returns
      # to the idle queue (no resetQuery configured).
      doAssert log.counts == @[2]
      doAssert pool.idle.len == 1
      doAssert pool.idle[0].conn.heldSessionLocks == 0

    waitFor t()

  test "manual release path fires hook and discards connection":
    proc t() {.async.} =
      let log = LeakLog()
      var cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      cfg.tracer = buildLeakTracer(log)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      let conn = await pool.acquire()
      await conn.advisoryLock(72003'i64)
      conn.release()
      doAssert log.counts == @[1]
      doAssert pool.idle.len == 0

    waitFor t()

  test "no leak leaves hook silent":
    proc t() {.async.} =
      let log = LeakLog()
      var cfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      cfg.tracer = buildLeakTracer(log)
      let pool = await newPool(cfg)
      defer:
        await pool.close()

      pool.withConnection(conn):
        await conn.advisoryLock(72004'i64)
        discard await conn.advisoryUnlock(72004'i64)

      doAssert log.counts.len == 0
      doAssert pool.idle.len == 1

    waitFor t()

suite "Advisory Lock: onAdvisoryUnlockFailed tracer hook":
  type UnlockFailLog = ref object
    data: seq[TraceAdvisoryUnlockFailedData]

  proc buildUnlockFailTracer(log: UnlockFailLog): PgTracer =
    let tracer = PgTracer()
    tracer.onAdvisoryUnlockFailed = proc(
        data: TraceAdvisoryUnlockFailedData
    ) {.gcsafe, raises: [].} =
      log.data.add(data)
    tracer

  test "withAdvisoryLock int64 reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLock(50005'i64):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key == 50005'i64
      doAssert log.data[0].shared == false
      doAssert log.data[0].twoKey == false
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLockShared int64 reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLockShared(50006'i64):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key == 50006'i64
      doAssert log.data[0].shared == true
      doAssert log.data[0].twoKey == false
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLock two-key reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLock(31'i32, 32'i32):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key1 == 31'i32
      doAssert log.data[0].key2 == 32'i32
      doAssert log.data[0].shared == false
      doAssert log.data[0].twoKey == true
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLockShared two-key reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLockShared(33'i32, 34'i32):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key1 == 33'i32
      doAssert log.data[0].key2 == 34'i32
      doAssert log.data[0].shared == true
      doAssert log.data[0].twoKey == true
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLock int64 timeout reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLock(50007'i64, 5.seconds):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key == 50007'i64
      doAssert log.data[0].shared == false
      doAssert log.data[0].twoKey == false
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLockShared int64 timeout reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLockShared(50008'i64, 5.seconds):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key == 50008'i64
      doAssert log.data[0].shared == true
      doAssert log.data[0].twoKey == false
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLock two-key timeout reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLock(35'i32, 36'i32, 5.seconds):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key1 == 35'i32
      doAssert log.data[0].key2 == 36'i32
      doAssert log.data[0].shared == false
      doAssert log.data[0].twoKey == true
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLockShared two-key timeout reports unlock failure":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      var bodyMsg = ""
      try:
        conn.withAdvisoryLockShared(37'i32, 38'i32, 5.seconds):
          await conn.close()
          raise newException(CatchableError, "original body error")
      except CatchableError as e:
        bodyMsg = e.msg
      doAssert bodyMsg == "original body error"
      doAssert log.data.len == 1
      doAssert log.data[0].key1 == 37'i32
      doAssert log.data[0].key2 == 38'i32
      doAssert log.data[0].shared == true
      doAssert log.data[0].twoKey == true
      doAssert log.data[0].err != nil

    waitFor t()

  test "withAdvisoryLock int64 reports unlock returning false":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      conn.withAdvisoryLock(50009'i64):
        # Release the lock out-of-band so the macro's own unlock returns false.
        discard await conn.advisoryUnlock(50009'i64)
      doAssert log.data.len == 1
      doAssert log.data[0].key == 50009'i64
      doAssert log.data[0].shared == false
      doAssert log.data[0].twoKey == false
      # A false return is reported with a nil err to distinguish it from a raise.
      doAssert log.data[0].err == nil

    waitFor t()

  test "withAdvisoryLockShared two-key reports unlock returning false":
    proc t() {.async.} =
      let log = UnlockFailLog()
      var cfg = plainConfig()
      cfg.tracer = buildUnlockFailTracer(log)
      let conn = await connect(cfg)
      defer:
        await conn.close()
      conn.withAdvisoryLockShared(39'i32, 40'i32):
        # Release the lock out-of-band so the macro's own unlock returns false.
        discard await conn.advisoryUnlockShared(39'i32, 40'i32)
      doAssert log.data.len == 1
      doAssert log.data[0].key1 == 39'i32
      doAssert log.data[0].key2 == 40'i32
      doAssert log.data[0].shared == true
      doAssert log.data[0].twoKey == true
      doAssert log.data[0].err == nil

    waitFor t()
