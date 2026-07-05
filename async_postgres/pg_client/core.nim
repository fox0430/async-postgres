## Internal building blocks shared by every `pg_client/` submodule.
##
## Contains types/constants for transaction options, the inline-parameter
## encoder, and the receive-loop templates that the extended-query path
## (`exec`, `query`, `queryEach`, `queryDirect`, …) reuses. Re-exported through
## `pg_client.nim`; submodules import this module directly via `./core`.

import std/[options, tables, math, random]

import ../[async_backend, pg_protocol, pg_connection, pg_types]

type
  IsolationLevel* = enum
    ## PostgreSQL transaction isolation level.
    ilDefault
    ilReadCommitted
    ilRepeatableRead
    ilSerializable
    ilReadUncommitted

  AccessMode* = enum
    ## PostgreSQL transaction access mode (read-write or read-only).
    amDefault
    amReadWrite
    amReadOnly

  DeferrableMode* = enum
    ## PostgreSQL transaction deferrable mode (for serializable read-only transactions).
    dmDefault
    dmDeferrable
    dmNotDeferrable

  TransactionOptions* = object
    ## Options for BEGIN: isolation level, access mode, and deferrable mode.
    isolation*: IsolationLevel
    access*: AccessMode
    deferrable*: DeferrableMode

  RetryOptions* = object
    ## Controls how `withTransactionRetry` re-runs a transaction after a
    ## retryable failure. Relies on Nim object field defaults, so partial
    ## construction (e.g. ``RetryOptions(maxAttempts: 5)``) leaves the unset
    ## fields at their defaults below.
    maxAttempts*: int = 3
      ## Total attempts including the first. Values ``<= 1`` run the body exactly
      ## once with no retry (the body always runs at least once).
    baseDelayMs*: int = 20 ## Backoff before the first retry, in milliseconds.
    maxDelayMs*: int = 1000 ## Upper bound on the backoff delay, in milliseconds.
    multiplier*: float = 2.0 ## Exponential growth factor between attempts.
    jitter*: bool = true
      ## Full jitter: pick a random delay in ``0 .. computed``. Uses the
      ## `std/random` global RNG; to de-correlate retries *across processes*
      ## the application must call ``randomize()`` once at startup — otherwise
      ## every process replays the same (default-seeded) jitter sequence.
    retryableStates*: seq[string] = @[
      SqlStateSerializationFailure, SqlStateDeadlockDetected
    ]
      ## SQLSTATE codes that trigger a retry. Defaults to serialization_failure
      ## (40001) and deadlock_detected (40P01) — the transaction-level conflicts
      ## PostgreSQL recommends resolving by re-running the whole transaction.

const copyBatchSize* = 262144 ## 256KB batch threshold for COPY IN buffering

func toFormatCodes*(rf: ResultFormat): seq[int16] =
  ## Convert a high-level ResultFormat to wire-protocol format codes.
  case rf
  of rfAuto:
    @[]
  of rfText:
    @[0'i16]
  of rfBinary:
    @[1'i16]

func deriveColFmts*(resultFormats: openArray[int16], numCols: int): seq[int16] =
  ## Expand wire-level Bind result-format codes to one code per column.
  ## A single code broadcasts to every column (Bind's "apply to all" form);
  ## a per-column array is applied positionally; any column past the end of a
  ## multi-element array defaults to text (0). Shared by every Extended Query
  ## path that has to decode rows under the formats this Bind actually
  ## requested (cache hit and cache miss alike).
  result = newSeq[int16](numCols)
  for i in 0 ..< numCols:
    result[i] =
      if resultFormats.len == 1:
        resultFormats[0]
      elif i < resultFormats.len:
        resultFormats[i]
      else:
        0'i16

func cacheHitColFmts*(
    resultFormats: openArray[int16], cachedColFmts: seq[int16], numCols: int
): seq[int16] =
  ## Per-column decode formats for a statement-cache HIT. Use the formats this
  ## Bind actually requested (`deriveColFmts` of `resultFormats`, which on a
  ## cache hit is `effectiveResultFormats`), not the formats negotiated when the
  ## statement was first cached: the same SQL can be re-issued with a different
  ## `resultFormat` (e.g. cached as rfAuto/rfBinary, now rfText) and the server
  ## returns rows in the format this Bind asked for; reusing the stale cached
  ## format would reinterpret the bytes and silently corrupt values (text "42"
  ## decoded as a big-endian int, etc.). The `cachedColFmts` fallback covers the
  ## caller-didn't-override / zero-column cases, where `resultFormats` is empty.
  ## Shared by all four cache-hit Extended Query paths so they cannot drift.
  if resultFormats.len > 0 and numCols > 0:
    deriveColFmts(resultFormats, numCols)
  else:
    cachedColFmts

proc buildBeginSql*(opts: TransactionOptions): string =
  ## Build a BEGIN SQL statement with the specified transaction options
  ## (isolation level, access mode, deferrable mode).
  result = "BEGIN"
  case opts.isolation
  of ilDefault:
    discard
  of ilReadCommitted:
    result.add " ISOLATION LEVEL READ COMMITTED"
  of ilRepeatableRead:
    result.add " ISOLATION LEVEL REPEATABLE READ"
  of ilSerializable:
    result.add " ISOLATION LEVEL SERIALIZABLE"
  of ilReadUncommitted:
    result.add " ISOLATION LEVEL READ UNCOMMITTED"
  case opts.access
  of amDefault:
    discard
  of amReadWrite:
    result.add " READ WRITE"
  of amReadOnly:
    result.add " READ ONLY"
  case opts.deferrable
  of dmDefault:
    discard
  of dmDeferrable:
    result.add " DEFERRABLE"
  of dmNotDeferrable:
    result.add " NOT DEFERRABLE"

proc isRetryableTxError*(e: ref CatchableError, states: openArray[string]): bool =
  ## Whether `e` is a `PgQueryError` whose SQLSTATE is in `states`.
  ## Non-`PgQueryError` failures (connection drops, timeouts) are never
  ## retryable here: they leave the connection unusable for a fresh attempt.
  if e of PgQueryError:
    (ref PgQueryError)(e).sqlState in states
  else:
    false

const StmtCacheInvalidatingStates* = ["26000", "0A000"]
  ## SQLSTATEs that mean a cache-*hit* prepared statement can no longer be
  ## reused as cached and must be evicted so the next call re-parses it:
  ##
  ## * ``26000`` invalid_sql_statement_name — the server no longer has the
  ##   prepared statement (e.g. ``DISCARD ALL`` / ``DEALLOCATE`` ran on the
  ##   session, or a pooled backend was reset). Re-parse recreates it.
  ## * ``0A000`` feature_not_supported — chiefly "cached plan must not change
  ##   result type": DDL altered the statement's result columns, so the server
  ##   rejects the cached (fixed-result) plan on Execute. Because the cache-hit
  ##   path skips Describe, only a re-parse picks up the new schema; without
  ##   eviction every subsequent hit would re-raise ``0A000`` forever. Other
  ##   ``0A000`` causes simply re-parse once more (the error still propagates).
  ##
  ## ``42P18`` (indeterminate_datatype) is intentionally absent: it is a
  ## Parse-phase error and cannot arise on a cache hit (no Parse is sent), and
  ## a cache *miss* that fails to Parse never reaches ``addStmtCache``.

proc backoffDelayMs*(opts: RetryOptions, attempt: int): int =
  ## Backoff (milliseconds) to wait after the `attempt`-th failure (1-based).
  ## Exponential ``baseDelayMs * multiplier^(attempt-1)`` capped at `maxDelayMs`;
  ## with `jitter` the result is randomized within ``0 .. computed`` to spread
  ## out retries from many contending clients. Jitter draws from the
  ## `std/random` global RNG — see `RetryOptions.jitter` on calling
  ## ``randomize()`` for cross-process de-correlation.
  let raw = opts.baseDelayMs.float * pow(opts.multiplier, float(attempt - 1))
  var ms = int(min(raw, opts.maxDelayMs.float))
  if ms < 0:
    ms = 0
  if opts.jitter and ms > 0:
    ms = rand(ms)
  ms

proc paramOidsMatch*(cachedOids, currentOids: openArray[int32]): bool =
  ## Whether a cached prepared statement's parse-time parameter OIDs are
  ## compatible with the OIDs a caller wants to bind now.
  ##
  ## OID ``0`` (unknown) on either side is treated as a wildcard: the server
  ## inferred or will infer the type, so we cannot pre-judge a mismatch.
  ## A length mismatch is treated as incompatible.
  ##
  ## Empty-vs-empty (parameter-less SQL) matches trivially: the loop body does
  ## not execute and the length check passes.
  ##
  ## Callers use a ``false`` result to invalidate the cache entry and re-parse
  ## the statement with the new OIDs, preventing the server from interpreting
  ## bind payloads under the statement's original (and now wrong) parse-time
  ## type assumptions.
  if cachedOids.len != currentOids.len:
    return false
  for i in 0 ..< cachedOids.len:
    let c = cachedOids[i]
    let n = currentOids[i]
    if c == n or c == 0 or n == 0:
      continue
    return false
  return true

proc paramOidsMatch*(cachedOids: openArray[int32], params: openArray[PgParam]): bool =
  ## ``PgParam`` overload that reads each parameter's ``oid`` field directly,
  ## avoiding a temporary ``seq[int32]`` projection on the ``query``/``exec``
  ## cache-hit path. Semantics match the ``openArray[int32]`` overload.
  if cachedOids.len != params.len:
    return false
  for i in 0 ..< cachedOids.len:
    let c = cachedOids[i]
    let n = params[i].oid
    if c == n or c == 0 or n == 0:
      continue
    return false
  return true

proc invalidateIfOidMismatch*(
    conn: PgConnection,
    sql: string,
    cached: CachedStmt,
    currentOids: openArray[int32],
    cacheHit: var bool,
) =
  ## If the caller is about to bind ``currentOids`` to a cached prepared
  ## statement whose parse-time OIDs do not match, evict the cache entry
  ## (queue the server-side ``Close`` via ``pendingStmtCloses``, remove the
  ## local entry) and set ``cacheHit`` to ``false`` so the caller's
  ## cache-miss path runs and re-parses under the new OIDs.
  ##
  ## No-op when ``cacheHit`` is already ``false`` — ``cached`` is only
  ## dereferenced under the ``cacheHit`` guard, so passing ``nil`` is safe
  ## as long as ``cacheHit`` is ``false``.
  if not cacheHit:
    return
  if paramOidsMatch(cached.paramOids, currentOids):
    return
  conn.pendingStmtCloses.add(cached.name)
  conn.removeStmtCache(sql)
  cacheHit = false

proc invalidateIfOidMismatch*(
    conn: PgConnection,
    sql: string,
    cached: CachedStmt,
    params: openArray[PgParam],
    cacheHit: var bool,
) =
  ## ``PgParam`` overload for the ``query``/``exec`` call paths. Avoids the
  ## ``seq[int32]`` allocation a separate OID-projection step would require —
  ## the per-parameter ``.oid`` reads happen inside ``paramOidsMatch``.
  if not cacheHit:
    return
  if paramOidsMatch(cached.paramOids, params):
    return
  conn.pendingStmtCloses.add(cached.name)
  conn.removeStmtCache(sql)
  cacheHit = false

proc extractParams*(
    params: openArray[PgParam]
): tuple[oids: seq[int32], formats: seq[int16], values: seq[Option[seq[byte]]]] =
  result.oids = newSeq[int32](params.len)
  result.formats = newSeq[int16](params.len)
  result.values = newSeq[Option[seq[byte]]](params.len)
  for i, p in params:
    result.oids[i] = p.oid
    result.formats[i] = p.format
    result.values[i] = p.value

template appendInlineParam*(
    data: var seq[byte],
    ranges: var seq[tuple[off: int32, len: int32]],
    oids: var seq[int32],
    formats: var seq[int16],
    p: PgParamInline,
) =
  ## Shared encoder for a single `PgParamInline` into SoA buffers. Used by
  ## both `flattenInline` (per-call temporaries) and `Pipeline.appendInline`
  ## (pipeline-wide SoA). Keeping the NULL / empty / inline / overflow
  ## branching in one place means wire-format semantics cannot drift between
  ## the two code paths.
  oids.add p.oid
  formats.add p.format
  if p.len == -1:
    ranges.add((int32(0), int32(-1)))
  elif p.len == 0:
    ranges.add((int32(data.len), int32(0)))
  else:
    let dataOff = int32(data.len)
    let oldLen = data.len
    data.setLen(oldLen + int(p.len))
    if p.len <= PgInlineBufSize:
      data.writeBytesAt(oldLen, p.inlineBuf.toOpenArray(0, int(p.len) - 1))
    else:
      data.writeBytesAt(oldLen, p.overflow.toOpenArray(0, int(p.len) - 1))
    ranges.add((dataOff, p.len))

proc flattenInline*(
    params: openArray[PgParamInline]
): tuple[
  data: seq[byte],
  ranges: seq[tuple[off: int32, len: int32]],
  oids: seq[int32],
  formats: seq[int16],
] =
  if params.len == 0:
    return
  result.oids = newSeqOfCap[int32](params.len)
  result.formats = newSeqOfCap[int16](params.len)
  result.ranges = newSeqOfCap[tuple[off: int32, len: int32]](params.len)
  var estBytes = 0
  for p in params:
    if p.len > 0:
      estBytes += int(p.len)
  result.data = newSeqOfCap[byte](estBytes)
  for p in params:
    appendInlineParam(result.data, result.ranges, result.oids, result.formats, p)

template sendExtendedQuery*(
    conn: PgConnection,
    resultFormats: seq[int16],
    cached: CachedStmt,
    cacheHit, cacheMiss: var bool,
    stmtName: var string,
    cachedFields: var seq[FieldDescription],
    cachedColFmts: var seq[int16],
    cachedColOids: var seq[int32],
    effectiveResultFormats: var seq[int16],
    parseStep, bindStep: untyped,
) =
  ## Emit the extended-query wire sequence (Parse/Bind/Describe/Execute/Sync)
  ## for a `query`-shaped round-trip into `conn.sendBuf`, branching on the
  ## prepared-statement cache state:
  ##
  ## * cache hit  → Bind, Execute, Sync. Pulls `fields`/`colFmts`/`colOids`/
  ##   `resultFormats` out of `cached` so the recv loop can reuse them.
  ## * cache miss → optional Close (eviction), Parse, Describe(Statement),
  ##   Bind, Execute, Sync. `cachedFields`/`cachedColFmts`/`cachedColOids` are
  ##   left for the recv loop to populate on RowDescription.
  ## * cache disabled (`stmtCacheCapacity == 0`) → Parse, Bind,
  ##   Describe(Portal), Execute, Sync. Describe(Portal) is required so the
  ##   recv loop sees a RowDescription for `QueryResult.fields`.
  ##
  ## `parseStep` and `bindStep` are untyped blocks expanded inline so each
  ## caller can pick the right `addParse` / `addBind` / `addBindRaw` overload
  ## (PgParam, raw `Option[seq[byte]]` + OIDs, or inline raw data + ranges)
  ## without going through a proc-pointer indirection. Both blocks reference
  ## `stmtName` from the outer scope; the template sets it before expansion
  ## ("" on the cache-disabled path, the cache-named or freshly-generated
  ## name otherwise).
  ##
  ## Precondition: `cached` may be nil iff `cacheHit == false`; the cache-miss
  ## and cache-disabled branches never read `cached`.
  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  if cacheHit:
    stmtName = cached.name
    cachedFields = cached.fields
    cachedColFmts = cached.colFmts
    cachedColOids = cached.colOids
    # The `cached.resultFormats` fallback is cache-hit-only: cache-miss and
    # cache-disabled both re-issue Describe, so the server returns fresh
    # column formats and the caller-supplied `resultFormats` (possibly empty)
    # is used directly. On a cache hit we skip Describe, so the previously
    # negotiated formats must be replayed when the caller didn't override.
    effectiveResultFormats =
      if resultFormats.len == 0: cached.resultFormats else: resultFormats
    bindStep
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    parseStep
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    bindStep
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
  else:
    stmtName = ""
    effectiveResultFormats = resultFormats
    parseStep
    bindStep
    conn.sendBuf.addDescribe(dkPortal, "")
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()

template sendExtendedExec*(
    conn: PgConnection,
    cached: CachedStmt,
    cacheHit, cacheMiss: var bool,
    stmtName: var string,
    parseStep, bindStep: untyped,
) =
  ## `exec`-shaped counterpart of `sendExtendedQuery`: same 3-branch send
  ## sequence, but no Describe(Portal) on the cache-disabled path and no
  ## result-format / cached-column bookkeeping (exec callers discard rows).
  ## The cache-miss path still issues Describe(Statement) so the recv loop
  ## can stash parameter OIDs and field info for future cache hits.
  ##
  ## Precondition: `cached` may be nil iff `cacheHit == false`.
  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  if cacheHit:
    stmtName = cached.name
    bindStep
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
  elif conn.stmtCacheCapacity > 0:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    if conn.stmtCache.len >= conn.stmtCacheCapacity:
      let evicted = conn.evictStmtCache()
      conn.sendBuf.addClose(dkStatement, evicted.name)
    parseStep
    conn.sendBuf.addDescribe(dkStatement, stmtName)
    bindStep
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()
  else:
    stmtName = ""
    parseStep
    bindStep
    conn.sendBuf.addExecute("", 0)
    conn.sendBuf.addSync()

template queryRecvLoop*(
    conn: PgConnection,
    sql: string,
    resultFormats: openArray[int16],
    cacheHit, cacheMiss: bool,
    stmtName: string,
    cachedFields: var seq[FieldDescription],
    cachedColFmts: seq[int16],
    cachedColOids: seq[int32],
    qr: var QueryResult,
) =
  var cachedParamOids: seq[int32]

  if cacheHit:
    # Take the cached field descriptions (already a private copy of the cache
    # entry) so we can update formatCode without mutating the statement cache.
    qr.fields = cachedFields
    if qr.fields.len > 0:
      # Decode with the column formats this Bind actually requested, not the
      # stale cached formats (see `cacheHitColFmts`), then reflect them back
      # into the returned metadata so QueryResult.fields.formatCode stays
      # consistent with the formats used for decoding.
      let colFmts = cacheHitColFmts(resultFormats, cachedColFmts, qr.fields.len)
      for i in 0 ..< qr.fields.len:
        qr.fields[i].formatCode = colFmts[i]
      qr.data = newRowData(int16(qr.fields.len), colFmts, cachedColOids)
      qr.data.fields = qr.fields

  conn.pumpUntilReady(qr.data, addr qr.rowCount):
    case pumpMsg.kind
    of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
      discard
    of bmkParameterDescription:
      if cacheMiss:
        cachedParamOids = pumpMsg.paramTypeOids
    of bmkRowDescription:
      var fields = pumpMsg.fields
      var cf: seq[int16]
      var co: seq[int32]
      if cacheMiss:
        cachedFields = pumpMsg.fields
        if resultFormats.len > 0:
          cf = deriveColFmts(resultFormats, fields.len)
          co = newSeq[int32](fields.len)
          for i in 0 ..< fields.len:
            co[i] = fields[i].typeOid
            fields[i].formatCode = cf[i]
      qr.fields = fields
      qr.data = newRowData(int16(qr.fields.len), cf, co)
      qr.data.fields = qr.fields
    of bmkNoData:
      discard
    of bmkCommandComplete:
      qr.commandTag = pumpMsg.commandTag
    of bmkEmptyQueryResponse:
      discard
    else:
      discard
  do:
    if queryError != nil:
      if cacheHit and queryError.sqlState in StmtCacheInvalidatingStates:
        conn.removeStmtCache(sql)
    elif cacheMiss:
      conn.addStmtCache(
        sql,
        CachedStmt(name: stmtName, fields: cachedFields, paramOids: cachedParamOids),
      )

template queryEachRecvLoop*(
    conn: PgConnection,
    sql: string,
    resultFormats: openArray[int16],
    cacheHit, cacheMiss: bool,
    stmtName: string,
    cachedFields: var seq[FieldDescription],
    cachedColFmts: seq[int16],
    cachedColOids: seq[int32],
    callback: RowCallback,
    rowCount: var int64,
) =
  var queryError: ref PgQueryError
  var rd: RowData
  var callbackError: ref CatchableError = nil
  var cachedParamOids: seq[int32]

  if cacheHit:
    # Decode with the formats this Bind requested (`resultFormats`), not the
    # cached first-Parse formats — see `queryRecvLoop` for the silent corruption
    # this avoids when the same SQL is re-issued with a different `resultFormat`.
    # Take the cached fields (a private copy) so the statement cache is not mutated.
    var fields = cachedFields
    let colFmts = cacheHitColFmts(resultFormats, cachedColFmts, fields.len)
    for i in 0 ..< fields.len:
      fields[i].formatCode = colFmts[i]
    if colFmts.len > 0 or cachedColOids.len > 0:
      rd = newRowData(int16(fields.len), colFmts, cachedColOids)
    else:
      rd = newRowData(int16(fields.len))
    rd.fields = fields

  let maxLen = conn.effectiveMaxMessageSize()
  block recvLoop:
    while true:
      # Parse messages directly from recvBuf using parseBackendMessage
      var pos = conn.recvBufStart
      while true:
        var consumed: int
        let res =
          try:
            parseBackendMessage(
              conn.recvBuf.toOpenArray(pos, conn.recvBuf.len - 1), consumed, rd, maxLen
            )
          except PgProtocolError as e:
            conn.state = csClosed
            raise e
        if res.state == psIncomplete:
          break # need more data
        pos += consumed
        conn.recvBufStart = pos
        if res.state == psDataRow:
          # DataRow was parsed into rd — invoke callback, then reset for next row
          if callbackError == nil:
            try:
              callback(initRow(rd, 0))
              rowCount += 1
            except CatchableError as e:
              callbackError = e
          # Reset buffers but keep capacity
          rd.buf.setLen(0)
          rd.cellIndex.setLen(0)
          continue
        let pumpMsg = res.message
        case pumpMsg.kind
        of bmkNotificationResponse:
          conn.dispatchNotification(pumpMsg)
          continue
        of bmkNoticeResponse:
          conn.dispatchNotice(pumpMsg)
          continue
        of bmkParameterStatus:
          # Parsed directly from recvBuf, so not recorded by `nextMessage`;
          # mirror buffer_io to keep serverParams current (e.g. SET, promotion).
          conn.serverParams[pumpMsg.paramName] = pumpMsg.paramValue
          continue
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          if cacheMiss:
            cachedParamOids = pumpMsg.paramTypeOids
        of bmkRowDescription:
          var fields = pumpMsg.fields
          var cf: seq[int16]
          var co: seq[int32]
          if cacheMiss:
            cachedFields = pumpMsg.fields
            if resultFormats.len > 0:
              cf = deriveColFmts(resultFormats, fields.len)
              co = newSeq[int32](fields.len)
              for i in 0 ..< fields.len:
                co[i] = fields[i].typeOid
                fields[i].formatCode = cf[i]
          rd = newRowData(int16(fields.len), cf, co)
          rd.fields = fields
        of bmkNoData:
          discard
        of bmkCommandComplete:
          discard
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(pumpMsg.errorFields)
        of bmkReadyForQuery:
          conn.recvBufStart = pos
          conn.txStatus = pumpMsg.txStatus
          if conn.state != csClosed:
            conn.state = csReady
          if callbackError != nil:
            raise callbackError
          if queryError != nil:
            if cacheHit and queryError.sqlState in StmtCacheInvalidatingStates:
              conn.removeStmtCache(sql)
            raise queryError
          if cacheMiss:
            conn.addStmtCache(
              sql,
              CachedStmt(
                name: stmtName, fields: cachedFields, paramOids: cachedParamOids
              ),
            )
          break recvLoop
        else:
          discard
        conn.recvBufStart = pos
      await conn.fillRecvBuf()

template execRecvLoop*(
    conn: PgConnection,
    sql: string,
    cacheHit, cacheMiss: bool,
    stmtName: string,
    commandTag: var string,
) =
  ## Receive-loop counterpart of `queryRecvLoop` for the extended-query exec
  ## path: discards `DataRow`s (exec callers don't need rows) and exposes only
  ## the `CommandComplete` tag via the `commandTag` out-parameter. Shared by
  ## `execImpl` (both overloads), `execInlineImpl`, and `execDirectRunImpl`.
  var cachedFields: seq[FieldDescription]
  var cachedParamOids: seq[int32]

  conn.pumpUntilReady:
    case pumpMsg.kind
    of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
      discard
    of bmkParameterDescription:
      if cacheMiss:
        cachedParamOids = pumpMsg.paramTypeOids
    of bmkRowDescription:
      if cacheMiss:
        cachedFields = pumpMsg.fields
    of bmkNoData:
      discard
    of bmkDataRow:
      discard
    of bmkCommandComplete:
      commandTag = pumpMsg.commandTag
    of bmkEmptyQueryResponse:
      discard
    else:
      discard
  do:
    if queryError != nil:
      if cacheHit and queryError.sqlState in StmtCacheInvalidatingStates:
        conn.removeStmtCache(sql)
    elif cacheMiss:
      conn.addStmtCache(
        sql,
        CachedStmt(name: stmtName, fields: cachedFields, paramOids: cachedParamOids),
      )
