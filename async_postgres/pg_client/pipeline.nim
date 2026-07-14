## Pipelined batch execution of `addExec`/`addQuery` operations against the
## PostgreSQL extended-query protocol. Includes both the single-Sync `execute`
## variant and the per-op Sync `executeIsolated` (error-isolated) variant.

import std/[options, tables]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import core

type
  PipelineOpKind* = enum
    pokExec
    pokQuery

  PipelineOp* = object
    kind: PipelineOpKind
    sql: string
    # Legacy path — populated by the `seq[PgParam]` overloads. These seqs own
    # the per-parameter byte payloads directly, avoiding an extra copy into
    # Pipeline-level storage for bulk-string workloads.
    params: seq[Option[seq[byte]]]
    paramOids: seq[int32]
    paramFormats: seq[int16]
    resultFormats: seq[int16]
    # Inline path — populated by the `openArray[PgParamInline]` overloads.
    # Points at slices of the Pipeline-level SoA buffers
    # (`inlineRanges`/`inlineOids`/`inlineFormats`/`inlineData`). `hasInline`
    # true means the send phase should use these slices instead of the legacy
    # fields above.
    hasInline: bool
    inlineStart: int32
    inlineCount: int32
    # Set during send phase
    cacheHit: bool
    cacheMiss: bool
    cacheShare: bool
      ## Reuses an in-flight prepared statement Parsed by an earlier op in
      ## the same pipeline (same SQL, compatible OIDs). Skips Parse/Describe,
      ## emits Bind + Describe(Portal) + Execute instead.
    cacheSuperseded: bool
      ## A cacheMiss op whose freshly-Parsed prepared statement was Closed
      ## mid-pipeline by a later same-SQL op with mismatched OIDs. The op's
      ## results are still returned, but it is not added to the persistent
      ## stmt cache (only the latest, type-correct stmt is).
    stmtName: string

  PipelineResultKind* = enum
    ## Discriminator for pipeline result variants.
    prkExec
    prkQuery

  PipelineResult* = object ## Result of a single operation within a pipeline.
    case kind*: PipelineResultKind
    of prkExec:
      commandResult*: CommandResult
    of prkQuery:
      queryResult*: QueryResult

  Pipeline* = ref object
    ## Batch of queries/execs sent through the PostgreSQL pipeline protocol.
    conn: PgConnection
    ops: seq[PipelineOp]
    # SoA storage shared by all ops added via the `PgParamInline` path.
    # Index ranges in each op point into these sequences, eliminating per-op
    # parameter allocations.
    inlineData: seq[byte]
    inlineRanges: seq[tuple[off: int32, len: int32]]
    inlineOids: seq[int32]
    inlineFormats: seq[int16]
    autoReset*: bool
      ## When true, `execute`/`executeIsolated` call `reset()` in a `finally`
      ## block so the Pipeline can be safely reused without leaking state from
      ## the previous run. Default: false (backward-compatible).

  IsolatedPipelineResults* = object
    ## Results from `executeIsolated`: per-op error isolation via per-query SYNC.
    results*: seq[PipelineResult]
    errors*: seq[ref CatchableError] ## errors[i] is nil if ops[i] succeeded

proc newPipeline*(conn: PgConnection, autoReset: bool = false): Pipeline =
  ## Create a new pipeline for batching multiple operations into a single round trip.
  ## When `autoReset` is true, the pipeline's queued ops and inline buffers are
  ## cleared automatically after each `execute`/`executeIsolated` call, making
  ## it safe to reuse the same Pipeline instance.
  Pipeline(conn: conn, ops: @[], autoReset: autoReset)

proc reset*(p: Pipeline) =
  ## Clear all queued ops and inline SoA buffers. Safe to call at any time,
  ## including while the pipeline is empty. Does not affect the underlying
  ## connection or its statement cache. When `p.autoReset` is true,
  ## `execute`/`executeIsolated` call this automatically (including on raise),
  ## so manual calls are only needed when `autoReset` is false.
  p.ops.setLen(0)
  p.inlineData.setLen(0)
  p.inlineRanges.setLen(0)
  p.inlineOids.setLen(0)
  p.inlineFormats.setLen(0)

proc appendInline(
    p: Pipeline, params: openArray[PgParamInline]
): tuple[start, count: int32] =
  ## Append inline params to the Pipeline-level SoA buffers. Returns
  ## `(start, count)` identifying the appended slice.
  result.start = int32(p.inlineRanges.len)
  result.count = int32(params.len)
  for pi in params:
    appendInlineParam(p.inlineData, p.inlineRanges, p.inlineOids, p.inlineFormats, pi)

proc addExec*(p: Pipeline, sql: string, params: seq[PgParam] = @[]) =
  ## Add an exec operation to the pipeline with typed parameters.
  var op = PipelineOp(kind: pokExec, sql: sql)
  if params.len > 0:
    op.paramOids = newSeqOfCap[int32](params.len)
    op.paramFormats = newSeqOfCap[int16](params.len)
    op.params = newSeqOfCap[Option[seq[byte]]](params.len)
    for param in params:
      op.paramOids.add param.oid
      op.paramFormats.add param.format
      op.params.add param.value
  p.ops.add move(op)

proc addQuery*(
    p: Pipeline,
    sql: string,
    params: seq[PgParam] = @[],
    resultFormat: ResultFormat = rfAuto,
) =
  ## Add a query operation to the pipeline with typed parameters.
  let (oids, formats, values) = extractParams(params)
  p.ops.add PipelineOp(
    kind: pokQuery,
    sql: sql,
    params: values,
    paramOids: oids,
    paramFormats: formats,
    resultFormats: resultFormat.toFormatCodes(),
  )

proc addExec*(p: Pipeline, sql: string, params: openArray[PgParamInline]) =
  ## Add an exec operation using the heap-alloc-free `PgParamInline` path.
  let (start, count) = p.appendInline(params)
  p.ops.add PipelineOp(
    kind: pokExec, sql: sql, hasInline: true, inlineStart: start, inlineCount: count
  )

proc addQuery*(
    p: Pipeline,
    sql: string,
    params: openArray[PgParamInline],
    resultFormat: ResultFormat = rfAuto,
) =
  ## Add a query operation using the heap-alloc-free `PgParamInline` path.
  let (start, count) = p.appendInline(params)
  p.ops.add PipelineOp(
    kind: pokQuery,
    sql: sql,
    hasInline: true,
    inlineStart: start,
    inlineCount: count,
    resultFormats: resultFormat.toFormatCodes(),
  )

proc buildSendPhase(p: Pipeline, perOpSync: bool): seq[CachedStmt] =
  ## Encode all queued ops into `p.conn.sendBuf` and return the per-op
  ## `CachedStmt` snapshots needed by the receive phase for cache-hit queries
  ## (lazy: empty unless at least one pokQuery cache-hit was seen). When
  ## `perOpSync` is true a Sync is appended after each op (executeIsolated);
  ## otherwise a single trailing Sync is appended (execute).
  let conn = p.conn
  conn.sendBuf.setLen(0)
  conn.flushPendingStmtCloses()
  var hasCachedStmts = false
  var pendingCacheAdds = 0 # track pending additions for LRU eviction in pipeline
  var defaultFormats: seq[int16] # reused across ops when paramFormats is empty
  # Statements Parsed earlier in this same pipeline batch. Lets subsequent
  # same-SQL ops reuse the just-allocated stmtName instead of re-Parsing —
  # without this, N same-SQL ops on a cold cache would orphan N-1 server-side
  # prepared statements (only the last addStmtCache would survive, the rest
  # would leak until session end).
  var inFlight:
    Table[string, tuple[stmtName: string, paramOids: seq[int32], opIdx: int]]

  for i in 0 ..< p.ops.len:
    let hasInline = p.ops[i].hasInline
    let startIdx = int(p.ops[i].inlineStart)
    let endIdx = startIdx + int(p.ops[i].inlineCount) - 1
    if not hasInline and p.ops[i].paramFormats.len == 0:
      let needed = p.ops[i].params.len
      if defaultFormats.len != needed:
        defaultFormats = newSeq[int16](needed)

    template currentFormats(): openArray[int16] =
      if hasInline:
        p.inlineFormats.toOpenArray(startIdx, endIdx)
      elif p.ops[i].paramFormats.len > 0:
        p.ops[i].paramFormats.toOpenArray(0, p.ops[i].paramFormats.high)
      else:
        defaultFormats.toOpenArray(0, defaultFormats.high)

    template emitBind(stmt: string, resultFmts: openArray[int16]) =
      if hasInline:
        conn.sendBuf.addBindRaw(
          "",
          stmt,
          currentFormats(),
          p.inlineData,
          p.inlineRanges.toOpenArray(startIdx, endIdx),
          resultFmts,
        )
      else:
        conn.sendBuf.addBind("", stmt, currentFormats(), p.ops[i].params, resultFmts)

    template emitParse(stmt: string) =
      if hasInline:
        conn.sendBuf.addParse(
          stmt, p.ops[i].sql, p.inlineOids.toOpenArray(startIdx, endIdx)
        )
      else:
        conn.sendBuf.addParse(stmt, p.ops[i].sql, p.ops[i].paramOids)

    template currentOidsMatch(cachedOids: seq[int32]): bool =
      if hasInline:
        paramOidsMatch(cachedOids, p.inlineOids.toOpenArray(startIdx, endIdx))
      else:
        paramOidsMatch(cachedOids, p.ops[i].paramOids)

    let cached = conn.lookupStmtCache(p.ops[i].sql)
    var cacheHit = cached != nil
    if cacheHit:
      # Reject the cache entry if its parse-time OIDs no longer match what
      # this op wants to bind — otherwise the server would interpret bind
      # bytes under stale parse-time types. Pipeline can't reuse
      # ``invalidateIfOidMismatch`` because that routes Close through
      # ``pendingStmtCloses``, which was already flushed at the top of
      # ``buildSendPhase`` — so emit the Close directly into sendBuf.
      if not currentOidsMatch(cached.paramOids):
        conn.sendBuf.addClose(dkStatement, cached.name)
        conn.removeStmtCache(p.ops[i].sql)
        cacheHit = false
    p.ops[i].cacheHit = cacheHit
    p.ops[i].cacheMiss = false
    p.ops[i].cacheShare = false
    p.ops[i].cacheSuperseded = false

    if cacheHit:
      p.ops[i].stmtName = cached.name
      if p.ops[i].kind == pokQuery:
        if not hasCachedStmts:
          result = newSeq[CachedStmt](p.ops.len)
          hasCachedStmts = true
        result[i] = cached
      var effectiveResultFormats: seq[int16]
      if p.ops[i].kind == pokQuery:
        # Replay the cached result formats when the caller didn't override, so a
        # no-override cache hit re-Binds the format negotiated at first Parse.
        # Leave `p.ops[i].resultFormats` untouched (don't freeze an rfAuto op
        # into resolved formats across re-executes): the receive phase derives
        # the same decode formats via `cacheHitColFmts(p.ops[i].resultFormats,
        # c.colFmts, ...)`, whose empty branch returns `c.colFmts` — equal to
        # these `cached.resultFormats` (both are `buildResultFormats` output).
        effectiveResultFormats =
          if p.ops[i].resultFormats.len == 0:
            cached.resultFormats
          else:
            p.ops[i].resultFormats
      emitBind(cached.name, effectiveResultFormats)
      conn.sendBuf.addExecute("", 0)
    elif conn.stmtCacheCapacity > 0:
      var shared = false
      if inFlight.hasKey(p.ops[i].sql):
        let entry = inFlight[p.ops[i].sql]
        if currentOidsMatch(entry.paramOids):
          # Same SQL, compatible OIDs — reuse the earlier op's stmt. No new
          # Parse/Describe(Statement); we still send Describe(Portal) for
          # queries so the recv loop gets a RowDescription for this op.
          shared = true
          p.ops[i].cacheShare = true
          p.ops[i].stmtName = entry.stmtName
          emitBind(entry.stmtName, p.ops[i].resultFormats)
          if p.ops[i].kind == pokQuery:
            conn.sendBuf.addDescribe(dkPortal, "")
          conn.sendBuf.addExecute("", 0)
        else:
          # Same SQL, different OIDs — close the in-flight stmt and demote
          # its creator so it isn't added to the persistent cache. The
          # fall-through emits a fresh Parse with the new OIDs.
          conn.sendBuf.addClose(dkStatement, entry.stmtName)
          p.ops[entry.opIdx].cacheSuperseded = true
          dec pendingCacheAdds
          inFlight.del(p.ops[i].sql)
      if not shared:
        p.ops[i].cacheMiss = true
        p.ops[i].stmtName = conn.nextStmtName()
        if conn.stmtCache.len + pendingCacheAdds >= conn.stmtCacheCapacity and
            conn.stmtCache.len > 0:
          let evicted = conn.evictStmtCache()
          conn.sendBuf.addClose(dkStatement, evicted.name)
        inc pendingCacheAdds
        emitParse(p.ops[i].stmtName)
        conn.sendBuf.addDescribe(dkStatement, p.ops[i].stmtName)
        emitBind(p.ops[i].stmtName, p.ops[i].resultFormats)
        conn.sendBuf.addExecute("", 0)
        # Deep-copy the OIDs so the inFlight entry is independent of the op's
        # storage. Symmetric across inline (slice of pipeline-level SoA) and
        # legacy (op-owned seq) paths — no shared aliasing surprises.
        let recordedOids =
          if hasInline:
            @(p.inlineOids.toOpenArray(startIdx, endIdx))
          else:
            @(p.ops[i].paramOids)
        inFlight[p.ops[i].sql] =
          (stmtName: p.ops[i].stmtName, paramOids: recordedOids, opIdx: i)
    else:
      emitParse("")
      emitBind("", p.ops[i].resultFormats)
      if p.ops[i].kind == pokQuery:
        conn.sendBuf.addDescribe(dkPortal, "")
      conn.sendBuf.addExecute("", 0)

    if perOpSync:
      conn.sendBuf.addSync()

  if not perOpSync:
    conn.sendBuf.addSync()

template initPipelineResults(
    results: var seq[PipelineResult], p: Pipeline, cachedStmts: seq[CachedStmt]
) =
  ## Initialize prkQuery results from cache-hit CachedStmts; prkExec results
  ## get a default PipelineResult. Shared by executeImpl and executeIsolatedImpl.
  for i in 0 ..< p.ops.len:
    if p.ops[i].kind == pokQuery:
      results[i] = PipelineResult(kind: prkQuery)
      if p.ops[i].cacheHit:
        let c = cachedStmts[i]
        results[i].queryResult.fields = c.fields
        if results[i].queryResult.fields.len > 0:
          let colFmts = cacheHitColFmts(
            p.ops[i].resultFormats, c.colFmts, results[i].queryResult.fields.len
          )
          for j in 0 ..< results[i].queryResult.fields.len:
            results[i].queryResult.fields[j].formatCode = colFmts[j]
          results[i].queryResult.data =
            newRowData(int16(results[i].queryResult.fields.len), colFmts, c.colOids)
          results[i].queryResult.data.fields = results[i].queryResult.fields
    else:
      results[i] = PipelineResult(kind: prkExec)

template settleSendFut(sendFut: untyped) =
  ## Cancel or drain sendFut so the Future never leaks on abnormal exit.
  ## Shared by executeImpl and executeIsolatedImpl.
  when hasChronos:
    if not sendFut.finished:
      try:
        await cancelAndWait(sendFut)
      except CatchableError:
        discard
    else:
      try:
        await sendFut
      except CatchableError:
        discard

proc executeImpl(p: Pipeline): Future[seq[PipelineResult]] {.async.} =
  let conn = p.conn
  conn.checkReady()

  let cachedStmts = buildSendPhase(p, perOpSync = false)
  conn.state = csBusy
  when hasChronos:
    # chronos drains the send Future in the background while we descend into
    # the receive loop. The outer try/except below owns sendFut's lifetime:
    # it drains sendFut on the normal path (propagating any stored write
    # error) and cancels it on any abnormal exit so the Future never leaks.
    var sendFut = conn.sendBufMsg()
  else:
    await conn.sendBufMsg()

  # Receive Phase
  var results = newSeq[PipelineResult](p.ops.len)
  var activeOpIdx = 0
  var queryError: ref PgQueryError
  var cachedFieldsPerOp: seq[seq[FieldDescription]] # lazy-init for cache misses
  var cachedParamOidsPerOp: seq[seq[int32]] # lazy-init, parallel to ops

  initPipelineResults(results, p, cachedStmts)

  template addCacheMissOp(i: int) =
    ## Add op `i`'s freshly-Parsed prepared statement to the cache. Shared by the
    ## success path (all ops) and the error path (only ops before the failure),
    ## so a mid-batch error cannot orphan the statements of ops that completed
    ## before it. Skips ops superseded by a later same-SQL op (already Closed in
    ## buildSendPhase).
    if p.ops[i].cacheMiss and not p.ops[i].cacheSuperseded:
      let fields =
        if cachedFieldsPerOp.len > 0:
          cachedFieldsPerOp[i]
        else:
          @[]
      let paramOids =
        if cachedParamOidsPerOp.len > 0:
          cachedParamOidsPerOp[i]
        else:
          @[]
      conn.addStmtCache(
        p.ops[i].sql,
        CachedStmt(name: p.ops[i].stmtName, fields: fields, paramOids: paramOids),
      )

  try:
    block recvLoop:
      while true:
        var rowData: RowData = nil
        var rowCount: ptr int32 = nil
        if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
          rowData = results[activeOpIdx].queryResult.data
          rowCount = addr results[activeOpIdx].queryResult.rowCount

        while (let opt = conn.nextMessage(rowData, rowCount); opt.isSome):
          let msg = opt.get
          case msg.kind
          of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
            discard
          of bmkParameterDescription:
            # Skip cacheSuperseded ops: their stmt was Closed mid-pipeline and
            # will not be added to stmtCache, so the paramOids would be unused.
            if activeOpIdx < p.ops.len and p.ops[activeOpIdx].cacheMiss and
                not p.ops[activeOpIdx].cacheSuperseded:
              if cachedParamOidsPerOp.len == 0:
                cachedParamOidsPerOp = newSeq[seq[int32]](p.ops.len)
              cachedParamOidsPerOp[activeOpIdx] = msg.paramTypeOids
          of bmkRowDescription:
            if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
              var cf: seq[int16]
              var co: seq[int32]
              if p.ops[activeOpIdx].cacheMiss:
                if not p.ops[activeOpIdx].cacheSuperseded:
                  if cachedFieldsPerOp.len == 0:
                    cachedFieldsPerOp = newSeq[seq[FieldDescription]](p.ops.len)
                  cachedFieldsPerOp[activeOpIdx] = msg.fields
                results[activeOpIdx].queryResult.fields = msg.fields
                if p.ops[activeOpIdx].resultFormats.len > 0:
                  cf = deriveColFmts(p.ops[activeOpIdx].resultFormats, msg.fields.len)
                  co = newSeq[int32](msg.fields.len)
                  for j in 0 ..< msg.fields.len:
                    co[j] = msg.fields[j].typeOid
                    results[activeOpIdx].queryResult.fields[j].formatCode = cf[j]
              else:
                results[activeOpIdx].queryResult.fields = msg.fields
                # cacheShare: this op skipped Describe(Statement) but still
                # emitted Describe(Portal), so msg.fields' formatCode/typeOid
                # reflect this op's Bind. Mirror into RowData so binary
                # decoders (isBinaryCol/colTypeOid) see the right metadata —
                # otherwise binary results would be misread as text.
                if p.ops[activeOpIdx].cacheShare:
                  cf = newSeq[int16](msg.fields.len)
                  co = newSeq[int32](msg.fields.len)
                  for j in 0 ..< msg.fields.len:
                    cf[j] = msg.fields[j].formatCode
                    co[j] = msg.fields[j].typeOid
              results[activeOpIdx].queryResult.data =
                newRowData(int16(msg.fields.len), cf, co)
              results[activeOpIdx].queryResult.data.fields =
                results[activeOpIdx].queryResult.fields
              # Update pointers for nextMessage
              rowData = results[activeOpIdx].queryResult.data
              rowCount = addr results[activeOpIdx].queryResult.rowCount
          of bmkNoData:
            discard
          of bmkCommandComplete:
            if activeOpIdx < p.ops.len:
              if p.ops[activeOpIdx].kind == pokExec:
                results[activeOpIdx].commandResult = initCommandResult(msg.commandTag)
              else:
                results[activeOpIdx].queryResult.commandTag = msg.commandTag
              inc activeOpIdx
              # Update rowData/rowCount for next op
              if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
                rowData = results[activeOpIdx].queryResult.data
                rowCount = addr results[activeOpIdx].queryResult.rowCount
              else:
                rowData = nil
                rowCount = nil
          of bmkEmptyQueryResponse:
            if activeOpIdx < p.ops.len:
              inc activeOpIdx
              if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
                rowData = results[activeOpIdx].queryResult.data
                rowCount = addr results[activeOpIdx].queryResult.rowCount
              else:
                rowData = nil
                rowCount = nil
          of bmkErrorResponse:
            if queryError == nil:
              queryError = newPgQueryError(msg.errorFields)
          of bmkReadyForQuery:
            conn.txStatus = msg.txStatus
            if conn.state != csClosed:
              conn.state = csReady
            if queryError != nil:
              # The batch ran as one implicit transaction that aborted, but
              # prepared statements created by Parse survive the rollback (they
              # are session state, not transactional data). Every op before the
              # failing one (activeOpIdx) was fully Parse/Describe/Execute'd, so
              # its server statement still exists — recover each cache-miss into
              # the cache instead of orphaning it. The failing op and everything
              # after it were skipped by the server until Sync, so leave them
              # alone (the failing op's stmt may never have been Parsed).
              for i in 0 ..< activeOpIdx:
                addCacheMissOp(i)
              # Invalidate only the *failing* op's cache entry for 26000
              # (statement gone) / 0A000 (cached plan result type changed after
              # DDL) — not every cache hit in the batch. Ride a server-side
              # Close along on the next operation so a still-live statement
              # (0A000) is reclaimed instead of leaked; Close of an already-gone
              # statement (26000) is a harmless no-op. See
              # StmtCacheInvalidatingStates.
              if queryError.sqlState in StmtCacheInvalidatingStates and
                  activeOpIdx < p.ops.len and p.ops[activeOpIdx].cacheHit:
                conn.pendingStmtCloses.add(p.ops[activeOpIdx].stmtName)
                conn.removeStmtCache(p.ops[activeOpIdx].sql)
              raise queryError
            # Cache misses: add to cache (skip ops superseded by a later
            # same-SQL op in this same pipeline — those stmts were already
            # Closed in buildSendPhase).
            for i in 0 ..< p.ops.len:
              addCacheMissOp(i)
            break recvLoop
          else:
            discard
        await conn.fillRecvBuf()

    when hasChronos:
      await sendFut
  except CatchableError as e:
    settleSendFut(sendFut)
    raise e

  return results

proc execute*(
    p: Pipeline, timeout: Duration = ZeroDuration
): Future[seq[PipelineResult]] {.async.} =
  ## Execute all queued pipeline operations in a single round trip.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  ## When `p.autoReset` is true, the pipeline is reset on exit (including on
  ## raise) so it can be safely reused.
  var results: seq[PipelineResult]
  try:
    if p.ops.len == 0:
      return @[]
    withConnTracing(
      p.conn,
      onPipelineStart,
      onPipelineEnd,
      TracePipelineStartData(opCount: p.ops.len),
      TracePipelineEndData,
      TracePipelineEndData(),
    ):
      if timeout > ZeroDuration:
        try:
          results = await executeImpl(p).wait(timeout)
        except AsyncTimeoutError:
          p.conn.invalidateOnTimeout("Pipeline execute timed out")
      else:
        results = await executeImpl(p)
  finally:
    if p.autoReset:
      p.reset()
  return results

proc executeIsolatedImpl(p: Pipeline): Future[IsolatedPipelineResults] {.async.} =
  ## Execute pipeline ops with per-query SYNC for error isolation.
  ## Each op gets its own ReadyForQuery; a failed op does not abort others.
  let conn = p.conn
  conn.checkReady()

  let cachedStmts = buildSendPhase(p, perOpSync = true)
  conn.state = csBusy
  when hasChronos:
    # Same concurrent-send pattern as executeImpl: the write drains while the
    # recv loop consumes per-op ReadyForQuery messages. Per-op SYNC still
    # provides error isolation; only the IO scheduling differs.
    var sendFut = conn.sendBufMsg()
  else:
    await conn.sendBufMsg()

  # Receive Phase (per-op ReadyForQuery)
  var results = newSeq[PipelineResult](p.ops.len)
  var errors = newSeq[ref CatchableError](p.ops.len)

  initPipelineResults(results, p, cachedStmts)

  try:
    for opIdx in 0 ..< p.ops.len:
      var opError: ref PgQueryError
      var cachedFields: seq[FieldDescription]
      var cachedParamOids: seq[int32]

      block opRecv:
        while true:
          var rowData: RowData = nil
          var rowCount: ptr int32 = nil
          if p.ops[opIdx].kind == pokQuery:
            rowData = results[opIdx].queryResult.data
            rowCount = addr results[opIdx].queryResult.rowCount

          while (let opt = conn.nextMessage(rowData, rowCount); opt.isSome):
            let msg = opt.get
            case msg.kind
            of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
              discard
            of bmkParameterDescription:
              # Skip cacheSuperseded ops — see executeImpl for rationale.
              if p.ops[opIdx].cacheMiss and not p.ops[opIdx].cacheSuperseded:
                cachedParamOids = msg.paramTypeOids
            of bmkRowDescription:
              if p.ops[opIdx].kind == pokQuery:
                if p.ops[opIdx].cacheMiss:
                  if not p.ops[opIdx].cacheSuperseded:
                    cachedFields = msg.fields
                  results[opIdx].queryResult.fields = msg.fields
                  var cf: seq[int16]
                  var co: seq[int32]
                  if p.ops[opIdx].resultFormats.len > 0:
                    cf = deriveColFmts(p.ops[opIdx].resultFormats, msg.fields.len)
                    co = newSeq[int32](msg.fields.len)
                    for j in 0 ..< msg.fields.len:
                      co[j] = msg.fields[j].typeOid
                      results[opIdx].queryResult.fields[j].formatCode = cf[j]
                  results[opIdx].queryResult.data =
                    newRowData(int16(msg.fields.len), cf, co)
                  results[opIdx].queryResult.data.fields =
                    results[opIdx].queryResult.fields
                  rowData = results[opIdx].queryResult.data
                  rowCount = addr results[opIdx].queryResult.rowCount
                else:
                  results[opIdx].queryResult.fields = msg.fields
                  # cacheShare: mirror Describe(Portal)'s formatCode/typeOid
                  # into RowData so binary decoders see the right metadata.
                  var cf: seq[int16]
                  var co: seq[int32]
                  if p.ops[opIdx].cacheShare:
                    cf = newSeq[int16](msg.fields.len)
                    co = newSeq[int32](msg.fields.len)
                    for j in 0 ..< msg.fields.len:
                      cf[j] = msg.fields[j].formatCode
                      co[j] = msg.fields[j].typeOid
                  results[opIdx].queryResult.data =
                    newRowData(int16(msg.fields.len), cf, co)
                  results[opIdx].queryResult.data.fields =
                    results[opIdx].queryResult.fields
                  rowData = results[opIdx].queryResult.data
                  rowCount = addr results[opIdx].queryResult.rowCount
            of bmkNoData:
              discard
            of bmkCommandComplete:
              if p.ops[opIdx].kind == pokExec:
                results[opIdx].commandResult = initCommandResult(msg.commandTag)
              else:
                results[opIdx].queryResult.commandTag = msg.commandTag
            of bmkEmptyQueryResponse:
              discard
            of bmkErrorResponse:
              if opError == nil:
                opError = newPgQueryError(msg.errorFields)
            of bmkReadyForQuery:
              conn.txStatus = msg.txStatus
              if opError != nil:
                if opError.sqlState in StmtCacheInvalidatingStates and
                    p.ops[opIdx].cacheHit:
                  conn.removeStmtCache(p.ops[opIdx].sql)
                errors[opIdx] = opError
              elif p.ops[opIdx].cacheMiss and not p.ops[opIdx].cacheSuperseded:
                conn.addStmtCache(
                  p.ops[opIdx].sql,
                  CachedStmt(
                    name: p.ops[opIdx].stmtName,
                    fields: cachedFields,
                    paramOids: cachedParamOids,
                  ),
                )
              break opRecv
            else:
              discard
          await conn.fillRecvBuf()

    when hasChronos:
      await sendFut
  except CatchableError as e:
    settleSendFut(sendFut)
    raise e

  if conn.state != csClosed:
    conn.state = csReady
  return IsolatedPipelineResults(results: results, errors: errors)

proc executeIsolated*(
    p: Pipeline, timeout: Duration = ZeroDuration
): Future[IsolatedPipelineResults] {.async.} =
  ## Execute all queued pipeline operations with per-query error isolation.
  ## Each operation gets its own SYNC message, so a failed operation does not
  ## abort subsequent ones. Returns results and per-op errors.
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  ## When `p.autoReset` is true, the pipeline is reset on exit (including on
  ## raise) so it can be safely reused.
  var ir: IsolatedPipelineResults
  try:
    if p.ops.len == 0:
      return IsolatedPipelineResults(results: @[], errors: @[])
    withConnTracing(
      p.conn,
      onPipelineStart,
      onPipelineEnd,
      TracePipelineStartData(opCount: p.ops.len),
      TracePipelineEndData,
      TracePipelineEndData(),
    ):
      if timeout > ZeroDuration:
        try:
          ir = await executeIsolatedImpl(p).wait(timeout)
        except AsyncTimeoutError:
          p.conn.invalidateOnTimeout("Pipeline executeIsolated timed out")
      else:
        ir = await executeIsolatedImpl(p)
  finally:
    if p.autoReset:
      p.reset()
  return ir
