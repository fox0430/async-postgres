## Client-side LRU cache for server-prepared statements.
##
## Holds the server statement name, parameter OIDs, field descriptions and
## pre-computed result formats for each cached SQL. The Extended Query
## send paths look up cached entries with `lookupStmtCache`, evict before
## adding via `addStmtCache`, and use `pendingStmtCloses` to bundle Close
## messages with the next operation's Sync.
##
## Internal: not re-exported through `pg_connection.nim`; import this module
## directly.

import std/[tables, lists]

import ../[async_backend, pg_protocol]
import types, buffer_io

const stmtNamePrefix* = "_sc_"

proc nextStmtName*(conn: PgConnection): string =
  ## Generate the next unique prepared statement name for the statement cache.
  inc conn.stmtCounter
  stmtNamePrefix & $conn.stmtCounter

proc clearStmtCache*(conn: PgConnection) =
  ## Clear the client-side statement cache. Does not close server-side
  ## statements, including any ``Close`` messages queued in
  ## ``pendingStmtCloses`` from defensive eviction — the queue is dropped on
  ## the assumption the caller will reset the session externally (e.g. via
  ## ``DISCARD ALL`` or by closing the connection).
  conn.stmtCache.clear()
  conn.stmtCacheLru = initDoublyLinkedList[string]()
  conn.pendingStmtCloses.setLen(0)
  conn.stagedStmtCloses.setLen(0)

proc lookupStmtCache*(conn: PgConnection, sql: string): CachedStmt =
  ## Look up a cached prepared statement by SQL text, updating LRU order on hit.
  ## Returns ``nil`` on miss. Because ``CachedStmt`` is a ``ref``, the returned
  ## value remains valid even if the cache is mutated afterwards — the entry's
  ## lifetime is extended by the reference.
  if conn.stmtCacheCapacity <= 0:
    return nil
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
    conn.stmtCacheLru.append(entry.lruNode)
    return entry[]
  return nil

proc evictStmtCache*(conn: PgConnection): CachedStmt =
  ## Evict the least recently used entry from the cache. Returns the evicted entry.
  let node = conn.stmtCacheLru.head
  let oldSql = node.value
  conn.stmtCacheLru.remove(node)
  result = conn.stmtCache[oldSql]
  conn.stmtCache.del(oldSql)

proc addStmtCache*(conn: PgConnection, sql: string, cached: CachedStmt) =
  ## Add a prepared statement to the cache with auto-computed result formats.
  ## Callers are expected to evict and send a server-side ``Close`` for the
  ## evicted statement before sending ``Parse``, so the loop below normally
  ## does not fire. It is a defensive guard: if a caller ever skips the
  ## pre-eviction step (or if ``stmtCacheCapacity`` was shrunk below the
  ## current size), we evict here instead of silently dropping the new entry
  ## and queue the evicted names in ``pendingStmtCloses`` so the next
  ## Extended Query operation can send their server-side ``Close``.
  if conn.stmtCacheCapacity <= 0:
    return
  while conn.stmtCache.len >= conn.stmtCacheCapacity:
    let evicted = conn.evictStmtCache()
    conn.pendingStmtCloses.add(evicted.name)
  if cached.resultFormats.len == 0 and cached.fields.len > 0:
    cached.resultFormats = buildResultFormats(cached.fields)
    cached.colFmts = newSeq[int16](cached.fields.len)
    cached.colOids = newSeq[int32](cached.fields.len)
    for i in 0 ..< cached.fields.len:
      cached.colOids[i] = cached.fields[i].typeOid
      cached.colFmts[i] = cached.resultFormats[i]
  let node = newDoublyLinkedNode(sql)
  cached.lruNode = node
  conn.stmtCache[sql] = cached
  conn.stmtCacheLru.append(node)

proc removeStmtCache*(conn: PgConnection, sql: string) =
  ## Remove a statement from the cache by its SQL text.
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
  conn.stmtCache.del(sql)

proc stagePendingStmtCloses*(conn: PgConnection, buf: var seq[byte]) =
  ## Append a ``Close`` for every owed statement name to ``buf`` so they ride
  ## along with this operation's ``Sync``, moving them from the queue to
  ## ``stagedStmtCloses``.
  ##
  ## Only `sendStagedBufMsg` / `sendStagedMsg` drop them, once the bytes are on
  ## the wire: an aborted build leaves them staged, the next one takes them back
  ## here, and a re-sent ``Close`` is a backend no-op.
  ##
  ## ``buf`` must already be emptied, or the build truncates the Closes away.
  if conn.stagedStmtCloses.len > 0:
    # A previous build staged these and never sent them. Owed again, ahead of
    # anything queued since.
    conn.pendingStmtCloses = conn.stagedStmtCloses & conn.pendingStmtCloses
    conn.stagedStmtCloses.setLen(0)
  for name in conn.pendingStmtCloses:
    buf.addClose(dkStatement, name)
  conn.stagedStmtCloses = move(conn.pendingStmtCloses)
  conn.markStaged()

proc stageEvictedClose*(conn: PgConnection, buf: var seq[byte], name: string) =
  ## Stage the ``Close`` for a statement the build itself evicted. Staged, not
  ## queued: the cache no longer remembers the name, and an aborted build
  ## leaves staged names owed just as the queue would.
  conn.requireStaged("staging an eviction Close")
  conn.stagedStmtCloses.add name
  buf.addClose(dkStatement, name)

proc stmtCachingEnabled*(conn: PgConnection): bool {.inline.} =
  ## Whether prepared statements are cached on this connection.
  conn.stmtCacheCapacity > 0

proc evictForInsert*(conn: PgConnection) =
  ## Make room for one more cache entry, staging the ``Close`` of whatever was
  ## evicted. Keeping the capacity comparison here also lets the `queryDirect` /
  ## `execDirect` writers reach it without unlocking `PgConnection` in the
  ## caller's scope.
  if conn.stmtCacheCapacity <= 0 or conn.stmtCache.len < conn.stmtCacheCapacity:
    return
  let evicted = conn.evictStmtCache()
  conn.stageEvictedClose(conn.sendBuf, evicted.name)

proc beginSendBuf*(conn: PgConnection) =
  ## Start a new operation's send buffer: empty it, then stage the queued
  ## ``Close`` messages into it.
  ##
  ## One call because the order is an invariant: staging first and emptying
  ## after would truncate the Closes back out. Emptying loses nothing —
  ## whatever the previous operation left was either sent or still staged, and
  ## the staging below takes it back.
  conn.sendBuf.setLen(0)
  conn.stagePendingStmtCloses(conn.sendBuf)

proc dropStagedStmtCloses(conn: PgConnection) =
  ## Forget the names whose ``Close`` is now on the wire. Names queued since the
  ## staging are in ``pendingStmtCloses`` and untouched by this.
  conn.requireStaged("dropping the staged statement Closes")
  conn.clearStaged()
  conn.stagedStmtCloses.setLen(0)

proc sendStagedBufMsg*(conn: PgConnection) {.async.} =
  ## `sendBufMsg` paired with `stagePendingStmtCloses`: drop the staged
  ## statement Closes only once the buffer is on the wire.
  await conn.sendBufMsg()
  conn.dropStagedStmtCloses()

proc sendStagedMsg*(conn: PgConnection, data: seq[byte]) {.async.} =
  ## `sendMsg` counterpart, for builds that assemble their own buffer.
  await conn.sendMsg(data)
  conn.dropStagedStmtCloses()
