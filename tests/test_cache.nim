## Dedicated unit tests for the statement cache in ``pg_connection/types`` — LRU order, capacity-0
## disable, defensive eviction into ``pendingStmtCloses``, and Close staging.

import std/[unittest, tables, strutils, importutils, lists]

import ../async_postgres/[async_backend, pg_errors, pg_protocol, pg_types]
import ../async_postgres/pg_connection/types {.all.}
import ../async_postgres/pg_client/core

privateAccess(PgConnection)

proc mockConn(capacity: int = 2): PgConnection =
  PgConnection(
    recvBuf: @[],
    state: csReady,
    txStatus: tsIdle,
    serverParams: initTable[string, string](),
    createdAt: Moment.now(),
    stmtCacheCapacity: capacity,
  )

proc cached(name: string, fields: seq[FieldDescription] = @[]): CachedStmt =
  CachedStmt(name: name, fields: fields)

proc lruLen(conn: PgConnection): int =
  for _ in conn.stmtCacheLru:
    inc result

suite "stmt cache LRU":
  test "capacity 0 disables lookup and add":
    let conn = mockConn(0)
    conn.addStmtCache("SELECT 1", cached("_sc_1"))
    check conn.stmtCache.len == 0
    check conn.lookupStmtCache("SELECT 1").isNil
    check not conn.stmtCachingEnabled
    # Parsed while caching was still on: the cache cannot keep it, so it closes it.
    check conn.pendingStmtCloses == @["_sc_1"]

  test "re-adding a key replaces the entry and queues the old Close":
    # Room to spare, so the defensive eviction cannot mask a stale node.
    let conn = mockConn(3)
    conn.addStmtCache("a", cached("_sc_1"))
    conn.addStmtCache("b", cached("_sc_2"))
    conn.addStmtCache("a", cached("_sc_3"))
    check conn.stmtCache.len == 2
    check conn.lruLen() == 2
    check conn.pendingStmtCloses == @["_sc_1"]
    # The replaced entry left no node behind: "b" is now the LRU victim.
    conn.addStmtCache("c", cached("_sc_4"))
    conn.addStmtCache("d", cached("_sc_5"))
    check conn.lruLen() == 3
    check conn.pendingStmtCloses == @["_sc_1", "_sc_2"]
    check conn.lookupStmtCache("a").name == "_sc_3"
    check conn.lookupStmtCache("d").name == "_sc_5"

  test "re-adding the same statement queues no Close":
    let conn = mockConn(2)
    conn.addStmtCache("a", cached("_sc_1"))
    conn.addStmtCache("a", cached("_sc_1"))
    check conn.stmtCache.len == 1
    check conn.lruLen() == 1
    check conn.pendingStmtCloses.len == 0

  # privateAccess turns `conn.stmtCacheCapacity = n` into a raw field write,
  # so the tests below call the setter by name.

  test "shrinking capacity evicts the LRU excess and queues their Closes":
    let conn = mockConn(3)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.addStmtCache("b", cached("_sc_b"))
    conn.addStmtCache("c", cached("_sc_c"))
    `stmtCacheCapacity=`(conn, 1)
    check conn.stmtCache.len == 1
    check conn.lruLen() == 1
    check conn.lookupStmtCache("c").name == "_sc_c"
    check conn.pendingStmtCloses == @["_sc_a", "_sc_b"]

  test "the capacity setter raises nothing":
    proc resize(conn: PgConnection, capacity: int) {.raises: [].} =
      `stmtCacheCapacity=`(conn, capacity)

    let conn = mockConn(2)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.addStmtCache("b", cached("_sc_b"))
    resize(conn, 1)
    check conn.pendingStmtCloses == @["_sc_a"]

  test "an LRU list out of step with the table loses no Close":
    let conn = mockConn(3)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.addStmtCache("b", cached("_sc_b"))
    conn.addStmtCache("c", cached("_sc_c"))
    conn.stmtCache.del("a") # node with no entry
    conn.stmtCacheLru.remove(conn.stmtCache["c"].lruNode) # entry with no node
    `stmtCacheCapacity=`(conn, 0)
    check conn.pendingStmtCloses == @["_sc_b"]
    conn.invalidateAllStmtCache("c", "_sc_c")
    check conn.pendingStmtCloses == @["_sc_b", "_sc_c"]
    check conn.stmtCache.len == 0

  test "disabling the cache evicts every entry and queues its Close":
    for disabled in [0, -1]:
      let conn = mockConn(2)
      conn.addStmtCache("a", cached("_sc_a"))
      conn.addStmtCache("b", cached("_sc_b"))
      `stmtCacheCapacity=`(conn, disabled)
      check conn.stmtCache.len == 0
      check conn.lruLen() == 0
      check conn.pendingStmtCloses == @["_sc_a", "_sc_b"]
      # Re-enabling finds nothing stale to hit.
      `stmtCacheCapacity=`(conn, 2)
      check conn.lookupStmtCache("a").isNil
      check conn.pendingStmtCloses == @["_sc_a", "_sc_b"]

  test "hit moves entry to MRU and miss returns nil":
    let conn = mockConn(2)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.addStmtCache("b", cached("_sc_b"))
    check conn.lookupStmtCache("a").name == "_sc_a"
    # After touching "a", "b" is LRU and is the next eviction victim.
    conn.addStmtCache("c", cached("_sc_c"))
    check conn.lookupStmtCache("b").isNil
    check conn.lookupStmtCache("a").name == "_sc_a"
    check conn.lookupStmtCache("c").name == "_sc_c"
    check conn.pendingStmtCloses == @["_sc_b"]

  test "defensive add eviction queues Close names":
    let conn = mockConn(1)
    conn.addStmtCache("old", cached("_sc_old"))
    conn.addStmtCache("new", cached("_sc_new"))
    check conn.stmtCache.len == 1
    check conn.lookupStmtCache("new").name == "_sc_new"
    check conn.pendingStmtCloses == @["_sc_old"]

  test "clearStmtCache drops LRU, pending, and staged Closes":
    let conn = mockConn(2)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.pendingStmtCloses = @["_sc_x"]
    conn.stagedStmtCloses = @["_sc_y"]
    conn.clearStmtCache()
    check conn.stmtCache.len == 0
    check conn.lookupStmtCache("a").isNil
    check conn.pendingStmtCloses.len == 0
    check conn.stagedStmtCloses.len == 0

  test "removeStmtCache drops one entry without touching others":
    let conn = mockConn(2)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.addStmtCache("b", cached("_sc_b"))
    conn.removeStmtCache("a")
    check conn.lookupStmtCache("a").isNil
    check conn.lookupStmtCache("b").name == "_sc_b"

  test "invalidateStmtCache drops the entry and queues its Close":
    let conn = mockConn(2)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.addStmtCache("b", cached("_sc_b"))
    conn.invalidateStmtCache("a", "_sc_a")
    check conn.stmtCache.len == 1
    check conn.lookupStmtCache("a").isNil
    check conn.lookupStmtCache("b").name == "_sc_b"
    check conn.pendingStmtCloses == @["_sc_a"]

  test "invalidateStmtCache leaves a name it no longer holds alone":
    # Shrinking the cache under an in-flight hit queues its Close first; the
    # hit's later 0A000 must not queue it again.
    let conn = mockConn(2)
    conn.addStmtCache("a", cached("_sc_a"))
    `stmtCacheCapacity=`(conn, 0)
    conn.invalidateStmtCache("a", "_sc_a")
    check conn.pendingStmtCloses == @["_sc_a"]
    # Nor may a stale name drop the entry that replaced it.
    `stmtCacheCapacity=`(conn, 2)
    conn.addStmtCache("a", cached("_sc_a2"))
    conn.invalidateStmtCache("a", "_sc_a")
    check conn.lookupStmtCache("a").name == "_sc_a2"
    check conn.pendingStmtCloses == @["_sc_a"]

  test "queueStmtClose queues a name the cache never held":
    let conn = mockConn(2)
    conn.queueStmtClose("_sc_orphan")
    check conn.pendingStmtCloses == @["_sc_orphan"]

  test "addStmtCache fills resultFormats from field OIDs":
    let conn = mockConn(2)
    let fields = @[
      FieldDescription(name: "i", typeOid: OidInt4, formatCode: 0),
      FieldDescription(name: "t", typeOid: OidText, formatCode: 0),
    ]
    conn.addStmtCache("SELECT 1", cached("_sc_1", fields))
    let entry = conn.lookupStmtCache("SELECT 1")
    check entry.resultFormats == @[1'i16, 1'i16]
    check entry.colOids == @[OidInt4, OidText]
    check entry.colFmts == @[1'i16, 1'i16]

  test "nextStmtName is unique and prefixed":
    let conn = mockConn()
    let a = conn.nextStmtName()
    let b = conn.nextStmtName()
    check a.startsWith(stmtNamePrefix)
    check b.startsWith(stmtNamePrefix)
    check a != b

  test "stagePendingStmtCloses empties the queue into staged + buffer":
    let conn = mockConn(1)
    conn.pendingStmtCloses = @["_sc_1", "_sc_2"]
    var buf: seq[byte]
    conn.stagePendingStmtCloses(buf)
    check conn.pendingStmtCloses.len == 0
    check conn.stagedStmtCloses == @["_sc_1", "_sc_2"]
    # Two Close('S') frames: type + int32 len + 'S' + cstring + NUL each.
    check buf.len > 0
    check buf[0] == byte('C')

  test "beginSendBuf clears then stages owed Closes":
    let conn = mockConn(1)
    conn.addSync() # stale bytes from an aborted build
    conn.pendingStmtCloses = @["_sc_owed"]
    conn.beginSendBuf()
    check conn.sendBuf.len > 0
    check conn.sendBuf[0] == byte('C')
    check conn.stagedStmtCloses == @["_sc_owed"]
    check conn.pendingStmtCloses.len == 0

suite "conn-level staging equals the two-argument form":
  ## The single-argument staging helpers must write the same bytes and move
  ## the same names as the two-argument forms they delegate to. Each test runs
  ## both forms from identical state and compares bytes and bookkeeping.

  test "stagePendingStmtCloses writes the same Closes and moves the same names":
    var oneArg = mockConn(1)
    oneArg.pendingStmtCloses = @["_sc_1", "_sc_2"]
    oneArg.stagePendingStmtCloses()

    var twoArg = mockConn(1)
    twoArg.pendingStmtCloses = @["_sc_1", "_sc_2"]
    var buf: seq[byte]
    twoArg.stagePendingStmtCloses(buf)

    var expected: seq[byte]
    expected.addClose(dkStatement, "_sc_1")
    expected.addClose(dkStatement, "_sc_2")
    check twoArg.sendBuf.len == 0
    check buf == expected
    check oneArg.sendBuf == buf
    check oneArg.stagedStmtCloses == twoArg.stagedStmtCloses
    check oneArg.stagedStmtCloses == @["_sc_1", "_sc_2"]
    check oneArg.pendingStmtCloses == twoArg.pendingStmtCloses
    check oneArg.pendingStmtCloses.len == 0

  test "evictForInsert evicts the same entry and stages the same Close":
    var oneArg = mockConn(1)
    oneArg.addStmtCache("a", cached("_sc_a"))
    oneArg.beginSendBuf()
    oneArg.evictForInsert()

    var twoArg = mockConn(1)
    twoArg.addStmtCache("a", cached("_sc_a"))
    twoArg.beginSendBuf()
    var buf: seq[byte]
    twoArg.evictForInsert(buf)

    var expected: seq[byte]
    expected.addClose(dkStatement, "_sc_a")
    check buf == expected
    check oneArg.sendBuf == buf
    check oneArg.stagedStmtCloses == twoArg.stagedStmtCloses
    check oneArg.stagedStmtCloses == @["_sc_a"]
    check oneArg.lookupStmtCache("a").isNil
    check twoArg.lookupStmtCache("a").isNil

suite "settleStmtCache acts on what the server confirmed":
  proc facts(parsed, described: bool): OpFacts =
    OpFacts(parsed: parsed, described: described, paramOids: @[23'i32])

  proc settle(
      conn: PgConnection, cache: StmtCacheStatus, f: OpFacts, err: ref PgQueryError
  ) =
    var f = f
    conn.settleStmtCache("q", "_sc_1", cache, f, err)

  proc failure(state: string): ref PgQueryError =
    (ref PgQueryError)(sqlState: state, msg: state)

  test "a miss the server Described is cached whether or not it failed later":
    # Even on 0A000: a plan gone stale fails its next hit, which invalidates it.
    for err in [nil, failure("22012"), failure("0A000")]:
      let conn = mockConn(2)
      conn.settle(scsMiss, facts(true, true), err)
      check conn.lookupStmtCache("q").name == "_sc_1"
      check conn.lookupStmtCache("q").paramOids == @[23'i32]
      check conn.pendingStmtCloses.len == 0

  test "a miss Parsed but not Described is Closed":
    let conn = mockConn(2)
    conn.settle(scsMiss, facts(true, false), failure("25P02"))
    check conn.lookupStmtCache("q").isNil
    check conn.pendingStmtCloses == @["_sc_1"]

  test "a miss never Parsed is left alone":
    # 42P05: the name belongs to someone else, whose statement a Close would drop.
    let conn = mockConn(2)
    conn.settle(scsMiss, facts(false, false), failure("42P05"))
    check conn.lookupStmtCache("q").isNil
    check conn.pendingStmtCloses.len == 0

  test "a hit is invalidated only by an invalidating state":
    let conn = mockConn(2)
    conn.addStmtCache("q", cached("_sc_1"))
    conn.settle(scsHit, OpFacts(), failure("22012"))
    check conn.lookupStmtCache("q").name == "_sc_1"
    conn.settle(scsHit, OpFacts(), failure("26000"))
    check conn.lookupStmtCache("q").isNil
    check conn.pendingStmtCloses == @["_sc_1"]

  test "a hit's 26000 drops every entry":
    # Gone with no reset tag seen: the other entries went with it.
    let conn = mockConn(3)
    conn.addStmtCache("a", cached("_sc_a"))
    conn.addStmtCache("q", cached("_sc_1"))
    conn.addStmtCache("b", cached("_sc_b"))
    conn.settle(scsHit, OpFacts(), failure("26000"))
    check conn.stmtCache.len == 0
    check lruLen(conn) == 0
    check conn.pendingStmtCloses == @["_sc_a", "_sc_1", "_sc_b"]

  test "a hit's 0A000 and a share's 26000 drop only their entry":
    # A share's 26000 comes from the earlier op's failed Parse.
    for (cache, state) in [(scsHit, "0A000"), (scsShare, "26000")]:
      let conn = mockConn(2)
      conn.addStmtCache("a", cached("_sc_a"))
      conn.addStmtCache("q", cached("_sc_1"))
      conn.settle(cache, OpFacts(), failure(state))
      check conn.lookupStmtCache("q").isNil
      check conn.lookupStmtCache("a").name == "_sc_a"
      check conn.pendingStmtCloses == @["_sc_1"]

  test "a hit or share whose Bind completed keeps the cache":
    # The statement bound, so the 26000 / 0A000 is the statement's own.
    for cache in [scsHit, scsShare]:
      for state in ["26000", "0A000"]:
        let conn = mockConn(2)
        conn.addStmtCache("a", cached("_sc_a"))
        conn.addStmtCache("q", cached("_sc_1"))
        conn.settle(cache, OpFacts(bound: true), failure(state))
        check conn.lookupStmtCache("q").name == "_sc_1"
        check conn.lookupStmtCache("a").name == "_sc_a"
        check conn.pendingStmtCloses.len == 0

  test "a hit's 26000 after a reset already seen leaves the cache alone":
    # A pipeline hit queued behind a DEALLOCATE ALL of the same batch.
    let conn = mockConn(2)
    conn.addStmtCache("q", cached("_sc_1"))
    conn.noteCommandTag("DEALLOCATE ALL")
    conn.addStmtCache("a", cached("_sc_2"))
    conn.settle(scsHit, OpFacts(), failure("26000"))
    check conn.lookupStmtCache("a").name == "_sc_2"
    check conn.pendingStmtCloses.len == 0

  test "a miss settled after caching was turned off is Closed":
    let conn = mockConn(0)
    conn.settle(scsMiss, facts(true, true), nil)
    check conn.pendingStmtCloses == @["_sc_1"]

  test "a miss Parsed before a reset is neither cached nor Closed":
    for described in [true, false]:
      let conn = mockConn(2)
      var f = facts(true, described)
      f.parsedGen = conn.stmtCacheResetGen
      conn.noteCommandTag("DEALLOCATE ALL")
      conn.settle(scsMiss, f, nil)
      check conn.lookupStmtCache("q").isNil
      check conn.pendingStmtCloses.len == 0

  test "a miss Parsed after a reset is cached":
    let conn = mockConn(2)
    conn.noteCommandTag("DISCARD ALL")
    var f: OpFacts
    f.observe(conn, BackendMessage(kind: bmkParseComplete), miss = true)
    f.observe(conn, BackendMessage(kind: bmkNoData), miss = true)
    conn.settle(scsMiss, f, nil)
    check conn.lookupStmtCache("q").name == "_sc_1"

  test "observe records any op's Bind but only a miss's Parse and Describe":
    let conn = mockConn(2)
    for miss in [false, true]:
      var f: OpFacts
      for kind in [bmkParseComplete, bmkNoData, bmkBindComplete]:
        f.observe(conn, BackendMessage(kind: kind), miss)
      check f.bound
      check f.parsed == miss
      check f.described == miss

suite "shouldRetryStmtCacheInvalidation retries only a Bind-phase 26000":
  test "a cache-hit Bind refused with 26000 on an idle connection retries":
    let conn = mockConn(2)
    check conn.shouldRetryStmtCacheInvalidation(true, "26000", false)

  test "an Execute-phase 26000 never retries":
    let conn = mockConn(2)
    check not conn.shouldRetryStmtCacheInvalidation(true, "26000", true)

  test "a miss, another state, or an open transaction never retries":
    let conn = mockConn(2)
    check not conn.shouldRetryStmtCacheInvalidation(false, "26000", false)
    check not conn.shouldRetryStmtCacheInvalidation(true, "0A000", false)
    check not conn.shouldRetryStmtCacheInvalidation(true, "22012", false)
    conn.txStatus = tsInTransaction
    check not conn.shouldRetryStmtCacheInvalidation(true, "26000", false)

suite "statements dropped by the session":
  test "DISCARD ALL and DEALLOCATE ALL empty the cache and the owed Closes":
    for tag in ["DISCARD ALL", "DEALLOCATE ALL"]:
      let conn = mockConn(2)
      conn.addStmtCache("a", cached("_sc_a"))
      conn.pendingStmtCloses = @["_sc_x"]
      conn.stagedStmtCloses = @["_sc_y"]
      let gen = conn.stmtCacheResetGen
      conn.noteCommandTag(tag)
      check conn.lookupStmtCache("a").isNil
      check lruLen(conn) == 0
      check conn.pendingStmtCloses.len == 0
      check conn.stagedStmtCloses.len == 0
      check conn.stmtCacheResetGen == gen + 1

  test "other tags leave the cache alone":
    # DEALLOCATE of one name reports no name, so its hits find out by 26000.
    for tag in ["DISCARD PLANS", "DISCARD TEMP", "DEALLOCATE", "RESET", "SELECT 1"]:
      let conn = mockConn(2)
      conn.addStmtCache("a", cached("_sc_a"))
      conn.pendingStmtCloses = @["_sc_x"]
      let gen = conn.stmtCacheResetGen
      conn.noteCommandTag(tag)
      check conn.lookupStmtCache("a").name == "_sc_a"
      check conn.pendingStmtCloses == @["_sc_x"]
      check conn.stmtCacheResetGen == gen

when defined(pgStateChecks):
  suite "an eviction Close only ahead of the build's first fallible message":
    test "staging behind a Parse is rejected":
      let conn = mockConn(1)
      conn.beginSendBuf()
      conn.addParse("_sc_1", "SELECT 1", newSeq[int32]())
      expect AssertionDefect:
        conn.stageEvictedClose(conn.sendBuf, "_sc_evict")
