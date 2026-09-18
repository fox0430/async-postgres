## Dedicated unit tests for ``pg_connection/cache`` — LRU order, capacity-0
## disable, defensive eviction into ``pendingStmtCloses``, and Close staging.

import std/[unittest, tables, strutils, importutils]

import ../async_postgres/[async_backend, pg_protocol, pg_types]
import ../async_postgres/pg_connection/types {.all.}
import ../async_postgres/pg_connection/cache {.all.}

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

suite "stmt cache LRU":
  test "capacity 0 disables lookup and add":
    let conn = mockConn(0)
    conn.addStmtCache("SELECT 1", cached("_sc_1"))
    check conn.stmtCache.len == 0
    check conn.lookupStmtCache("SELECT 1").isNil
    check not conn.stmtCachingEnabled

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
    conn.sendBuf = @[1'u8, 2, 3]
    conn.pendingStmtCloses = @["_sc_owed"]
    conn.beginSendBuf()
    check conn.sendBuf.len > 0
    check conn.sendBuf[0] == byte('C')
    check conn.stagedStmtCloses == @["_sc_owed"]
    check conn.pendingStmtCloses.len == 0
