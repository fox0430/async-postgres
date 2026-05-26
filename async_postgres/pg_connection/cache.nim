## Client-side LRU cache for server-prepared statements.
##
## Holds the server statement name, parameter OIDs, field descriptions and
## pre-computed result formats for each cached SQL. The Extended Query
## send paths look up cached entries with `lookupStmtCache`, evict before
## adding via `addStmtCache`, and use `pendingStmtCloses` to bundle Close
## messages with the next operation's Sync.
##
## Re-exported through `pg_connection.nim`.

import std/[tables, lists]

import ../pg_protocol
import types

proc nextStmtName*(conn: PgConnection): string =
  ## Generate the next unique prepared statement name for the statement cache.
  inc conn.stmtCounter
  "_sc_" & $conn.stmtCounter

proc clearStmtCache*(conn: PgConnection) =
  ## Clear the client-side statement cache. Does not close server-side
  ## statements, including any ``Close`` messages queued in
  ## ``pendingStmtCloses`` from defensive eviction — the queue is dropped on
  ## the assumption the caller will reset the session externally (e.g. via
  ## ``DISCARD ALL`` or by closing the connection).
  conn.stmtCache.clear()
  conn.stmtCacheLru = initDoublyLinkedList[string]()
  conn.pendingStmtCloses.setLen(0)

proc lookupStmtCache*(conn: PgConnection, sql: string): ptr CachedStmt =
  ## Look up a cached prepared statement by SQL text, updating LRU order on hit.
  ## Returns nil on miss. The returned pointer is valid until the next cache mutation.
  if conn.stmtCacheCapacity <= 0:
    return nil
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
    conn.stmtCacheLru.append(entry.lruNode)
    return addr entry[]
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
  var entry = cached
  if entry.resultFormats.len == 0 and entry.fields.len > 0:
    entry.resultFormats = buildResultFormats(entry.fields)
    entry.colFmts = newSeq[int16](entry.fields.len)
    entry.colOids = newSeq[int32](entry.fields.len)
    for i in 0 ..< entry.fields.len:
      entry.colOids[i] = entry.fields[i].typeOid
      entry.colFmts[i] = entry.resultFormats[i]
  let node = newDoublyLinkedNode(sql)
  entry.lruNode = node
  conn.stmtCache[sql] = entry
  conn.stmtCacheLru.append(node)

proc removeStmtCache*(conn: PgConnection, sql: string) =
  ## Remove a statement from the cache by its SQL text.
  conn.stmtCache.withValue(sql, entry):
    conn.stmtCacheLru.remove(entry.lruNode)
  conn.stmtCache.del(sql)

proc flushPendingStmtCloses*(conn: PgConnection, buf: var seq[byte]) =
  ## Append ``Close`` messages for any prepared statement names queued by the
  ## defensive eviction path in ``addStmtCache`` to ``buf`` and clear the
  ## queue. Called by Extended Query send paths after the outgoing buffer is
  ## emptied (or freshly allocated) so the closes ride along with the next
  ## operation's ``Sync``. The corresponding ``CloseComplete`` replies are
  ## absorbed by the receive loops (every Extended Query recv loop handles
  ## ``bmkCloseComplete`` or falls through ``else: discard``).
  if conn.pendingStmtCloses.len == 0:
    return
  for name in conn.pendingStmtCloses:
    buf.addClose(dkStatement, name)
  conn.pendingStmtCloses.setLen(0)

proc flushPendingStmtCloses*(conn: PgConnection) =
  ## Convenience overload that writes to ``conn.sendBuf``.
  conn.flushPendingStmtCloses(conn.sendBuf)
