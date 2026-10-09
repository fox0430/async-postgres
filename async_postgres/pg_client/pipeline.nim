## Pipelined batch execution of `addExec`/`addQuery` operations against the
## PostgreSQL extended-query protocol. Includes both the single-Sync `execute`
## variant and the per-op Sync `executeIsolated` (error-isolated) variant.
##
## Internal module: not part of the public API. Import the `pg_client` hub instead.

import std/[options, tables]

import ../[async_backend, pg_protocol, pg_types]
import ../pg_connection/[types, buffer_io, simple_query]
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
    cache: StmtCacheStatus
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
      ##
      ## **Warning:** when false, a second `execute`/`executeIsolated` without
      ## an intervening `reset()` re-sends the same queued ops. Non-idempotent
      ## commands (INSERT, UPDATE, …) then run twice. Prefer `true` for
      ## reusable pipelines, or call `reset()` before building a new batch.

  IsolatedPipelineResults* = object
    ## Results from `executeIsolated`: per-op error isolation via per-query SYNC.
    results*: seq[PipelineResult]
    errors*: seq[ref CatchableError] ## errors[i] is nil if ops[i] succeeded

proc newPipeline*(conn: PgConnection, autoReset: bool = false): Pipeline =
  ## Create a new pipeline for batching multiple operations into a single round trip.
  ## When `autoReset` is true, the pipeline's queued ops and inline buffers are
  ## cleared automatically after each `execute`/`executeIsolated` call, making
  ## it safe to reuse the same Pipeline instance.
  ##
  ## When `autoReset` is false (the default), queued ops remain after
  ## `execute`/`executeIsolated`. Calling either again without `reset()`
  ## re-sends those ops and can duplicate non-idempotent side effects.
  ## Prefer `autoReset = true` when the same Pipeline will be reused.
  Pipeline(conn: conn, ops: @[], autoReset: autoReset)

proc reset*(p: Pipeline) =
  ## Clear all queued ops and inline SoA buffers. Safe to call at any time,
  ## including while the pipeline is empty. Does not affect the underlying
  ## connection or its statement cache. When `p.autoReset` is true,
  ## `execute`/`executeIsolated` call this automatically (including on raise),
  ## so manual calls are only needed when `autoReset` is false.
  ##
  ## Without `autoReset`, call this before building a new batch: a second
  ## `execute`/`executeIsolated` on uncleared ops re-sends them and can
  ## duplicate non-idempotent side effects.
  p.ops.setLen(0)
  p.inlineData.setLen(0)
  p.inlineRanges.setLen(0)
  p.inlineOids.setLen(0)
  p.inlineFormats.setLen(0)

proc appendInline(
    p: Pipeline, params: openArray[PgParamInline], resultFormatsLen: int = 0
): tuple[start, count: int32] =
  ## Append inline params. All-or-nothing: fully validated before any append.
  # Ahead of the `int32` conversions below, whose `RangeDefect` is uncatchable.
  # `inlineRanges.len` accumulates over every add since `reset`, so no per-add
  # bound covers it.
  if resultFormatsLen < 0 or resultFormatsLen > maxInt16Count:
    raise newException(
      PgTypeError,
      "Bind result-format count " & $resultFormatsLen & " exceeds protocol maximum of " &
        $maxInt16Count,
    )
  validateParamCount(params.len, "Bind parameter")
  if p.inlineRanges.len > maxInt32Len - params.len:
    raise newException(
      PgTypeError,
      "pipeline inline parameter count (" &
        $(int64(p.inlineRanges.len) + int64(params.len)) &
        ") exceeds protocol maximum of " & $maxInt32Len,
    )
  result.start = int32(p.inlineRanges.len)
  result.count = int32(params.len)
  # Run up front: raising partway through the loop would orphan bytes in
  # `inlineData` that no `PipelineOp` references. Bounds the shared buffer, not
  # the Bind message: `inlineData.len` becomes an `int32` range offset.
  var bufTotal: int64 = int64(p.inlineData.len)
  var opPayload: int64 = 0
  for pi in params:
    validateInlineParam(pi)
    if pi.len > 0:
      let plen = int64(pi.len)
      # Not `checkMsgLenBound64`: this is the shared buffer, not one assembled
      # message, so it must not surface as `PgMessageTooLargeError`.
      if bufTotal + plen > int64(maxInt32Len):
        raise newException(
          PgTypeError,
          "pipeline inline parameter data (" & $(bufTotal + plen) &
            " bytes) exceeds protocol maximum of " & $maxInt32Len,
        )
      bufTotal += plen
      addBindPayload(opPayload, int(pi.len))
  checkMsgLenBound64(
    calcBindMessageLength(
      0, generatedStmtNameLen, params.len, params.len, opPayload, resultFormatsLen
    ),
    "Bind message",
  )
  for pi in params:
    appendInlineParamUnchecked(
      p.inlineData, p.inlineRanges, p.inlineOids, p.inlineFormats, pi
    )

proc addExec*(p: Pipeline, sql: string, params: seq[PgParam] = @[]) =
  ## Add an exec operation to the pipeline with typed parameters.
  validateExtendedQuery(sql, params.len)
  validateTypedParams(params)
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
  ## The Bind pre-flight under-charges a cache hit's replayed result formats:
  ## the cache is only consulted in `buildSendPhase`, long after this add.
  validateExtendedQuery(sql, params.len)
  let rf = resultFormat.toFormatCodes()
  validateTypedParams(params, rf.len)
  let (oids, formats, values) = extractParams(params)
  p.ops.add PipelineOp(
    kind: pokQuery,
    sql: sql,
    params: values,
    paramOids: oids,
    paramFormats: formats,
    resultFormats: rf,
  )

proc addExec*(p: Pipeline, sql: string, params: openArray[PgParamInline]) =
  ## Add an exec operation using the heap-alloc-free `PgParamInline` path.
  validateExtendedQuery(sql, params.len)
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
  ## Under-charges a cache hit's result formats as the typed overload does.
  validateExtendedQuery(sql, params.len)
  let rf = resultFormat.toFormatCodes()
  let (start, count) = p.appendInline(params, rf.len)
  p.ops.add PipelineOp(
    kind: pokQuery,
    sql: sql,
    hasInline: true,
    inlineStart: start,
    inlineCount: count,
    resultFormats: rf,
  )

proc buildSendPhase(p: Pipeline, perOpSync: bool): seq[CachedStmt] =
  ## Encode all queued ops into `p.conn.sendBuf` and return the per-op
  ## `CachedStmt` snapshots needed by the receive phase for cache-hit queries
  ## (lazy: empty unless at least one pokQuery cache-hit was seen). When
  ## `perOpSync` is true a Sync is appended after each op (executeIsolated);
  ## otherwise a single trailing Sync is appended (execute).
  ##
  ## Writes no statement ``Close`` after the queued ones at the head: behind a
  ## failing op the backend would skip it up to ``Sync``. Statements this
  ## build drops from the cache are queued for the next operation instead, and
  ## it never pre-evicts (`addStmtCache` evicts at settle time).
  let conn = p.conn
  conn.beginSendBuf()
  var hasCachedStmts = false
  var defaultFormats: seq[int16] # reused across ops when paramFormats is empty
  # Statements Parsed earlier in this same pipeline batch. Lets subsequent
  # same-SQL ops reuse the just-allocated stmtName instead of Parsing (and
  # then Closing) one statement per op.
  var inFlight: Table[string, tuple[stmtName: string, paramOids: seq[int32]]]

  # Names the op an encode failure came from. The batch still fails whole (the
  # ops are positional on the wire), but the message says who caused it.
  var encodingOp = -1
  try:
    for i in 0 ..< p.ops.len:
      encodingOp = i
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
          conn.addBindRaw(
            "",
            stmt,
            currentFormats(),
            p.inlineData,
            p.inlineRanges.toOpenArray(startIdx, endIdx),
            resultFmts,
          )
        else:
          conn.addBind("", stmt, currentFormats(), p.ops[i].params, resultFmts)

      template emitParse(stmt: string) =
        if hasInline:
          conn.addParse(stmt, p.ops[i].sql, p.inlineOids.toOpenArray(startIdx, endIdx))
        else:
          conn.addParse(stmt, p.ops[i].sql, p.ops[i].paramOids)

      template currentOidsMatch(cachedOids: seq[int32]): bool =
        if hasInline:
          paramOidsMatch(cachedOids, p.inlineOids.toOpenArray(startIdx, endIdx))
        else:
          paramOidsMatch(cachedOids, p.ops[i].paramOids)

      let cached = conn.lookupStmtCache(p.ops[i].sql)
      var cacheHit = cached != nil
      if cacheHit:
        # Stale parse-time OIDs would have the server read the bind bytes
        # under the wrong types. The Close waits for the next operation: an
        # earlier op of this batch may still Bind the statement.
        if not currentOidsMatch(cached.paramOids):
          conn.invalidateStmtCache(p.ops[i].sql, cached.name)
          cacheHit = false
      p.ops[i].cache = scsUncached

      if cacheHit:
        p.ops[i].cache = scsHit
        p.ops[i].stmtName = cached.name
        if p.ops[i].kind == pokQuery:
          if not hasCachedStmts:
            result = newSeq[CachedStmt](p.ops.len)
            hasCachedStmts = true
          result[i] = cached
        var effectiveResultFormats: seq[int16]
        if p.ops[i].kind == pokQuery:
          # Replay the format negotiated at first Parse when the caller didn't
          # override, without freezing an rfAuto op across re-executes.
          effectiveResultFormats =
            if p.ops[i].resultFormats.len == 0:
              cached.resultFormats
            else:
              p.ops[i].resultFormats
        emitBind(cached.name, effectiveResultFormats)
        conn.addExecute("", 0)
      elif conn.stmtCacheCapacity > 0:
        var shared = false
        if inFlight.hasKey(p.ops[i].sql):
          let entry = inFlight[p.ops[i].sql]
          if currentOidsMatch(entry.paramOids):
            # Same SQL, compatible OIDs — reuse the earlier op's stmt. Queries
            # still need Describe(Portal) for their own RowDescription.
            shared = true
            p.ops[i].cache = scsShare
            p.ops[i].stmtName = entry.stmtName
            emitBind(entry.stmtName, p.ops[i].resultFormats)
            if p.ops[i].kind == pokQuery:
              conn.addDescribe(dkPortal, "")
            conn.addExecute("", 0)
          else:
            # Same SQL, different OIDs: Parse again. Settled in op order, the
            # newer statement replaces the older one in the cache, which
            # queues the older one's Close.
            inFlight.del(p.ops[i].sql)
        if not shared:
          p.ops[i].cache = scsMiss
          p.ops[i].stmtName = conn.nextStmtName()
          emitParse(p.ops[i].stmtName)
          conn.addDescribe(dkStatement, p.ops[i].stmtName)
          emitBind(p.ops[i].stmtName, p.ops[i].resultFormats)
          conn.addExecute("", 0)
          # Deep-copy so the inFlight entry does not alias the op's storage,
          # which is a slice of the pipeline-level SoA on the inline path.
          let recordedOids =
            if hasInline:
              @(p.inlineOids.toOpenArray(startIdx, endIdx))
            else:
              @(p.ops[i].paramOids)
          inFlight[p.ops[i].sql] =
            (stmtName: p.ops[i].stmtName, paramOids: recordedOids)
      else:
        emitParse("")
        emitBind("", p.ops[i].resultFormats)
        if p.ops[i].kind == pokQuery:
          conn.addDescribe(dkPortal, "")
        conn.addExecute("", 0)

      if perOpSync:
        conn.addSync()

    # The trailing Sync belongs to the batch, not to the last op.
    encodingOp = -1
    if not perOpSync:
      conn.addSync()
  except PgError as e:
    # Only the encoders raise these two, so the interleaved cache bookkeeping
    # is not blamed on the in-flight op.
    if encodingOp >= 0 and (e of PgTypeError or e of PgProtocolError):
      e.msg =
        "pipeline op #" & $encodingOp & " (" & p.ops[encodingOp].sql & "): " & e.msg
    raise e

proc initPipelineResults(
    results: var seq[PipelineResult], p: Pipeline, cachedStmts: seq[CachedStmt]
) =
  ## Initialize prkQuery results from cache-hit CachedStmts; prkExec results
  ## get a default PipelineResult. Shared by executeImpl and executeIsolatedImpl.
  for i in 0 ..< p.ops.len:
    if p.ops[i].kind == pokQuery:
      results[i] = PipelineResult(kind: prkQuery)
      if p.ops[i].cache == scsHit:
        results[i].queryResult.initBoundResult(
          cacheHitRowData(p.ops[i].resultFormats, cachedStmts[i])
        )
    else:
      results[i] = PipelineResult(kind: prkExec)

proc applyRowDescriptionToQr(
    qr: var QueryResult,
    fields: sink seq[FieldDescription],
    cache: StmtCacheStatus,
    resultFormats: seq[int16],
) =
  ## Populate qr.fields / qr.data from a RowDescription. scsMiss Describes the
  ## statement; scsShare / scsUncached Describe the portal (see
  ## `describedRowData`). Shared by both pipeline receive paths (executeImpl
  ## and executeIsolatedImpl) so the two cannot drift on cache handling.
  ## `fields` is sunk so the RowDescription seq is moved into qr.fields
  ## without an intermediate copy.
  qr.fields = fields
  qr.data = describedRowData(
    qr.fields, portal = cache in {scsShare, scsUncached}, resultFormats
  )

template settleSendFut(sendFut: untyped) =
  ## Cancel or drain sendFut so the Future never leaks on abnormal exit.
  ## Shared by executeImpl and executeIsolatedImpl.
  when hasChronos:
    if not sendFut.finished:
      try:
        await cancelAndWaitPumped(sendFut)
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
  conn.markBusy()
  when hasChronos:
    # chronos drains the send Future in the background while we descend into
    # the receive loop. The outer try/except below owns sendFut's lifetime:
    # it drains sendFut on the normal path (propagating any stored write
    # error) and cancels it on any abnormal exit so the Future never leaks.
    var sendFut = conn.sendStagedBufMsg()
  else:
    await conn.sendStagedBufMsg()

  # Receive Phase
  var results = newSeq[PipelineResult](p.ops.len)
  var activeOpIdx = 0
  var queryError: ref PgQueryError
  var facts = newSeq[OpFacts](p.ops.len)

  initPipelineResults(results, p, cachedStmts)

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
          if activeOpIdx < p.ops.len:
            facts[activeOpIdx].observe(conn, msg, p.ops[activeOpIdx].cache == scsMiss)
          case msg.kind
          of bmkRowDescription:
            if activeOpIdx < p.ops.len and p.ops[activeOpIdx].kind == pokQuery:
              applyRowDescriptionToQr(
                results[activeOpIdx].queryResult,
                msg.fields,
                p.ops[activeOpIdx].cache,
                p.ops[activeOpIdx].resultFormats,
              )
              # Update pointers for nextMessage
              rowData = results[activeOpIdx].queryResult.data
              rowCount = addr results[activeOpIdx].queryResult.rowCount
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
              conn.markReady()
            # Prepared statements survive the batch's implicit rollback, so
            # every op settles on its own facts. In op order, so a later
            # same-SQL miss replaces an earlier one. Only the failing op
            # (activeOpIdx) sees the error; the backend skipped the ops after
            # it, whose facts are empty.
            for i in 0 ..< p.ops.len:
              conn.settleStmtCache(
                p.ops[i].sql,
                p.ops[i].stmtName,
                p.ops[i].cache,
                facts[i],
                if i == activeOpIdx: queryError else: nil,
              )
            if queryError != nil:
              raise queryError
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
  ## The batch shares one Sync, so it runs as one implicit transaction: when
  ## an op fails, the backend skips the ops after it, rolls back the ones
  ## before it, and the first `PgQueryError` is raised. Inside an explicit
  ## transaction (an earlier `BEGIN`), that transaction is aborted instead
  ## and needs a `ROLLBACK`. Use `executeIsolated` for per-op outcomes.
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
  ## When `p.autoReset` is true, the pipeline is reset on exit (including on
  ## raise) so it can be safely reused.
  ##
  ## When `p.autoReset` is false (the default), queued ops are left in place.
  ## A second call without `reset()` re-sends them and can duplicate
  ## non-idempotent side effects — prefer `autoReset = true` for reuse.
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
      awaitOrInvalidate(
        p.conn, results, executeImpl(p), timeout, "Pipeline execute timed out"
      )
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
  conn.markBusy()
  when hasChronos:
    # Same concurrent-send pattern as executeImpl: the write drains while the
    # recv loop consumes per-op ReadyForQuery messages. Per-op SYNC still
    # provides error isolation; only the IO scheduling differs.
    var sendFut = conn.sendStagedBufMsg()
  else:
    await conn.sendStagedBufMsg()

  # Receive Phase (per-op ReadyForQuery)
  var results = newSeq[PipelineResult](p.ops.len)
  var errors = newSeq[ref CatchableError](p.ops.len)

  initPipelineResults(results, p, cachedStmts)

  try:
    for opIdx in 0 ..< p.ops.len:
      var opError: ref PgQueryError
      var facts: OpFacts

      block opRecv:
        while true:
          var rowData: RowData = nil
          var rowCount: ptr int32 = nil
          if p.ops[opIdx].kind == pokQuery:
            rowData = results[opIdx].queryResult.data
            rowCount = addr results[opIdx].queryResult.rowCount

          while (let opt = conn.nextMessage(rowData, rowCount); opt.isSome):
            let msg = opt.get
            facts.observe(conn, msg, p.ops[opIdx].cache == scsMiss)
            case msg.kind
            of bmkRowDescription:
              if p.ops[opIdx].kind == pokQuery:
                applyRowDescriptionToQr(
                  results[opIdx].queryResult,
                  msg.fields,
                  p.ops[opIdx].cache,
                  p.ops[opIdx].resultFormats,
                )
                rowData = results[opIdx].queryResult.data
                rowCount = addr results[opIdx].queryResult.rowCount
            of bmkCommandComplete:
              if p.ops[opIdx].kind == pokExec:
                results[opIdx].commandResult = initCommandResult(msg.commandTag)
              else:
                results[opIdx].queryResult.commandTag = msg.commandTag
            of bmkErrorResponse:
              if opError == nil:
                opError = newPgQueryError(msg.errorFields)
            of bmkReadyForQuery:
              conn.txStatus = msg.txStatus
              conn.settleStmtCache(
                p.ops[opIdx].sql,
                p.ops[opIdx].stmtName,
                p.ops[opIdx].cache,
                facts,
                opError,
              )
              if opError != nil:
                errors[opIdx] = opError
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
    conn.markReady()
  return IsolatedPipelineResults(results: results, errors: errors)

proc executeIsolated*(
    p: Pipeline, timeout: Duration = ZeroDuration
): Future[IsolatedPipelineResults] {.async.} =
  ## Execute all queued pipeline operations with per-query error isolation.
  ## Each operation gets its own SYNC message, so a failed operation does not
  ## abort subsequent ones. Returns results and per-op errors.
  ## `errors[i]` only ever holds the server's `PgQueryError` for op `i`;
  ## a transport failure, timeout or cancellation raises instead, and the
  ## results are lost. A failed op's `results[i]` holds no result: an exec's
  ## stays default, but a query's may already carry the column descriptions
  ## and any rows received before the error. Inside an explicit
  ## transaction, the first failure aborts it, so every later op fails too
  ## (`25P02`).
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
  ## When `p.autoReset` is true, the pipeline is reset on exit (including on
  ## raise) so it can be safely reused.
  ##
  ## When `p.autoReset` is false (the default), queued ops are left in place.
  ## A second call without `reset()` re-sends them and can duplicate
  ## non-idempotent side effects — prefer `autoReset = true` for reuse.
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
      awaitOrInvalidate(
        p.conn,
        ir,
        executeIsolatedImpl(p),
        timeout,
        "Pipeline executeIsolated timed out",
      )
  finally:
    if p.autoReset:
      p.reset()
  return ir
