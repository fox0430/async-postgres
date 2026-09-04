## Shared building blocks for ``pg_client`` submodules (transaction opts, inline params, recv loops).

import std/[options, tables, math, random]

import ../[async_backend, pg_protocol, pg_connection, pg_types]
import ../pg_connection/[types, buffer_io, cache, simple_query]
import ../pg_types/encoding

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
    ## Retry config for ``withTransactionRetry``; unset fields keep the
    ## defaults below.
    maxAttempts*: int = 3 ## Total attempts (``<=1`` = no retry).
    baseDelayMs*: int = 20 ## Initial backoff ms.
    maxDelayMs*: int = 1000 ## Max backoff ms.
    multiplier*: float = 2.0 ## Backoff multiplier.
    jitter*: bool = true
      ## Full jitter via ``std/random``; call ``randomize()`` for cross-process de-correlation.
    retryableStates*: seq[string] = @[
      SqlStateSerializationFailure, SqlStateDeadlockDetected
    ] ## SQLSTATEs that trigger retry.

const copyBatchSize* = 262144 ## 256KB batch threshold for COPY IN buffering

const
  generatedStmtNameLen* = "_sc_".len + len($int.high)
    ## Longest name `nextStmtName` can produce. Size pre-flights charge it
    ## because the real name is only picked at send time.

  generatedPortalNameLen* = "_cursor_".len + len($int.high)
    ## Same, for the portal name `openCursorImpl` generates.

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
  ## Expand Bind result-format codes to per-column: one code broadcasts,
  ## an array applies positionally, columns past its end default to text (0).
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
  ## Per-column formats for a cache hit. Prefers the formats this Bind
  ## requested: the same SQL may be re-issued with a different ``resultFormat``,
  ## and the stale cached one would reinterpret the bytes.
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
  ## SQLSTATEs that invalidate cached prepared statements (requires re-parse).
  ## ``42P18`` is absent on purpose: it is Parse-phase, so no cached statement
  ## can hit it.

proc backoffDelayMs*(opts: RetryOptions, attempt: int): int =
  ## Backoff ms for attempt (1-based). Exponential with jitter.
  let raw = opts.baseDelayMs.float * pow(opts.multiplier, float(attempt - 1))
  var ms = int(min(raw, opts.maxDelayMs.float))
  if ms < 0:
    ms = 0
  if opts.jitter and ms > 0:
    ms = rand(ms)
  ms

proc paramOidsMatch*(cachedOids, currentOids: openArray[int32]): bool =
  ## Whether cached param OIDs match current (0 = wildcard).
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
  ## ``PgParam`` overload (avoids ``seq[int32]`` alloc).
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
  ## Evict cached statement if OIDs mismatch; sets ``cacheHit=false``.
  ## ``cached`` may be nil iff ``cacheHit == false`` (only deref'd under it).
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
  ## ``PgParam`` overload (no ``seq[int32]`` alloc). Same nil precondition on
  ## ``cached``.
  if not cacheHit:
    return
  if paramOidsMatch(cached.paramOids, params):
    return
  conn.pendingStmtCloses.add(cached.name)
  conn.removeStmtCache(sql)
  cacheHit = false

proc preflightResultFormatsLen*(
    cached: CachedStmt, cacheHit: bool, resultFormatsLen: int = 0
): int =
  ## Result-format count the send path will really emit: a cache hit replays
  ## ``cached.resultFormats`` when the caller passed none.
  ## Call after ``invalidateIfOidMismatch``; ``cached`` may be nil iff not ``cacheHit``.
  if cacheHit and resultFormatsLen == 0: cached.resultFormats.len else: resultFormatsLen

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

proc validateParamCount*(n: int, what: string) =
  ## Reject a count the encoder would reject later: a send-phase failure
  ## would take the whole pipelined batch down.
  if n > maxInt16Count:
    raise newException(
      PgTypeError,
      what & " count " & $n & " exceeds protocol maximum of " & $maxInt16Count,
    )

proc validateParseMsg*(sql: string, nParams: int, stmtNameLen = generatedStmtNameLen) =
  ## Reject a Parse the encoder would reject later, so a pipelined op fails on
  ## its own instead of in `buildSendPhase`. Best-effort; `patchMsgLen` stays
  ## the authority on message size.
  checkNoNul(sql, "SQL statement")
  checkMsgLenBound64(
    calcParseMessageLength(stmtNameLen, sql.len, nParams), "Parse message"
  )

proc validateTypedParams*(
    params: openArray[PgParam],
    resultFormatsLen: int = 0,
    stmtNameLen: int = generatedStmtNameLen,
) =
  ## `validateInlineParam`'s counterpart for the `seq[PgParam]` path: checks the
  ## Int16 count, the payload total and the Bind envelope at add time.
  ## `resultFormatsLen` 0 means unknown, making the check a lower bound.
  if resultFormatsLen < 0 or resultFormatsLen > maxInt16Count:
    raise newException(
      PgTypeError,
      "Bind result-format count " & $resultFormatsLen & " exceeds protocol maximum of " &
        $maxInt16Count,
    )
  validateParamCount(params.len, "Bind parameter")
  var payload: int64 = 0
  for p in params:
    if p.value.isSome:
      addBindPayload(payload, p.value.get.len)
  checkMsgLenBound64(
    calcBindMessageLength(
      0, stmtNameLen, params.len, params.len, payload, resultFormatsLen
    ),
    "Bind message",
  )

proc validateEncodedParams*(
    params: openArray[Option[seq[byte]]],
    paramFormatsLen: int,
    resultFormatsLen: int = 0,
    stmtNameLen: int = generatedStmtNameLen,
    portalLen: int = 0,
) =
  ## `validateTypedParams` for the already-encoded `seq[Option[seq[byte]]]` path.
  ## Runs before the send templates drain `pendingStmtCloses` into `sendBuf`,
  ## which a later rejection would discard.
  if paramFormatsLen < 0 or paramFormatsLen > maxInt16Count:
    raise newException(
      PgTypeError,
      "Bind parameter-format count " & $paramFormatsLen & " exceeds protocol maximum of " &
        $maxInt16Count,
    )
  if resultFormatsLen < 0 or resultFormatsLen > maxInt16Count:
    raise newException(
      PgTypeError,
      "Bind result-format count " & $resultFormatsLen & " exceeds protocol maximum of " &
        $maxInt16Count,
    )
  validateParamCount(params.len, "Bind parameter")
  var payload: int64 = 0
  for p in params:
    if p.isSome:
      addBindPayload(payload, p.get.len)
  checkMsgLenBound64(
    calcBindMessageLength(
      portalLen, stmtNameLen, paramFormatsLen, params.len, payload, resultFormatsLen
    ),
    "Bind message",
  )

proc validateRawBind*(
    data: openArray[byte],
    ranges: openArray[tuple[off: int32, len: int32]],
    paramFormats: openArray[int16],
    resultFormatsLen: int = 0,
    stmtNameLen: int = generatedStmtNameLen,
) =
  ## `validateEncodedParams` for the raw buffer/ranges path (`addBindRaw`).
  ## The `*Impl` procs are public, so `data`/`ranges` may never have gone
  ## through `flattenInline`.
  preflightBindCounts("", "", paramFormats.len, ranges.len, resultFormatsLen)
  var payload: int64 = 0
  for r in ranges:
    if r.len < -1:
      raise newException(PgTypeError, "Bind range len " & $r.len & " is invalid")
    if r.len > 0:
      if r.off < 0:
        raise newException(PgTypeError, "Bind range off " & $r.off & " is negative")
      if r.off.int64 + r.len.int64 > data.len.int64:
        raise newException(
          PgTypeError,
          "Bind range out of bounds (off=" & $r.off & ", len=" & $r.len & ", data.len=" &
            $data.len & ")",
        )
      addBindPayload(payload, r.len)
  checkMsgLenBound64(
    calcBindMessageLength(
      0, stmtNameLen, paramFormats.len, ranges.len, payload, resultFormatsLen
    ),
    "Bind message",
  )

proc validateExtendedQuery*(
    sql: string,
    nParams: int,
    nParamOids: int = nParams,
    stmtNameLen: int = generatedStmtNameLen,
) =
  ## Add-time validation shared by every extended-query entry point.
  ## `nParamOids` sizes the Parse, which the `*Impl` procs take separately from
  ## the values. Pass `stmtNameLen` 0 for an unnamed statement.
  validateParamCount(nParams, "Bind parameter")
  validateParamCount(nParamOids, "Parse parameter-type")
  validateParseMsg(sql, nParamOids, stmtNameLen)

template validateInlineParam*(p: PgParamInline) =
  ## Reject a bad ``PgParamInline`` with ``PgTypeError``, so a hand-built one
  ## stays catchable under ``PgError`` instead of a fatal ``RangeDefect``.
  if p.len < -1:
    # Only -1 encodes NULL; any other negative would shrink `data` below.
    raise newException(
      PgTypeError, "PgParamInline.len (" & $p.len & ") is negative but not NULL (-1)"
    )
  if p.len > PgInlineBufSize and p.overflow.len < int(p.len):
    raise newException(
      PgTypeError,
      "PgParamInline.len (" & $p.len & ") exceeds overflow capacity (" & $p.overflow.len &
        ")",
    )

template appendInlineParamUnchecked*(
    data: var seq[byte],
    ranges: var seq[tuple[off: int32, len: int32]],
    oids: var seq[int32],
    formats: var seq[int16],
    p: PgParamInline,
) =
  ## Encode one ``PgParamInline`` into SoA buffers. Raises ``PgTypeError``.
  ## The caller must have run ``validateInlineParam`` on ``p``: its bounds keep
  ## the ``overflow`` read in range.
  var rng: typeof(ranges[0])
  if p.len == -1:
    rng = (int32(0), int32(-1))
  else:
    checkMsgLenBound64(int64(data.len) + int64(p.len), "inline parameter data")
    let dataOff = int32(data.len)
    if p.len == 0:
      rng = (dataOff, int32(0))
    else:
      let oldLen = data.len
      data.setLen(oldLen + int(p.len))
      if p.len <= PgInlineBufSize:
        data.writeBytesAt(oldLen, p.inlineBuf.toOpenArray(0, int(p.len) - 1))
      else:
        data.writeBytesAt(oldLen, p.overflow.toOpenArray(0, int(p.len) - 1))
      rng = (dataOff, p.len)
  oids.add p.oid
  formats.add p.format
  ranges.add rng

proc flattenInline*(
    params: openArray[PgParamInline], resultFormatsLen: int = 0
): tuple[
  data: seq[byte],
  ranges: seq[tuple[off: int32, len: int32]],
  oids: seq[int32],
  formats: seq[int16],
] =
  # Ahead of the empty-params early return: the result-format count is a Bind
  # field of its own, so it must be rejected even with no parameters.
  if resultFormatsLen < 0 or resultFormatsLen > maxInt16Count:
    raise newException(
      PgTypeError,
      "Bind result-format count " & $resultFormatsLen & " exceeds protocol maximum of " &
        $maxInt16Count,
    )
  if params.len == 0:
    return
  validateParamCount(params.len, "Bind parameter")
  # Bounds the buffer, not a message: `data.len` becomes an `int32` range
  # offset. Summed first so the reservation below stays sane.
  var estBytes: int64 = 0
  for p in params:
    validateInlineParam(p)
    if p.len > 0:
      let plen = int64(p.len)
      checkMsgLenBound64(estBytes + plen, "inline parameter data")
      estBytes += plen
  checkMsgLenBound64(
    calcBindMessageLength(
      0, generatedStmtNameLen, params.len, params.len, estBytes, resultFormatsLen
    ),
    "Bind message",
  )
  result.oids = newSeqOfCap[int32](params.len)
  result.formats = newSeqOfCap[int16](params.len)
  result.ranges = newSeqOfCap[tuple[off: int32, len: int32]](params.len)
  result.data = newSeqOfCap[byte](int(estBytes))
  for p in params:
    appendInlineParamUnchecked(
      result.data, result.ranges, result.oids, result.formats, p
    )

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
  ## Emit Parse/Bind/Describe/Execute/Sync sequence (cache hit/miss/disabled).
  ## Precondition: ``cached`` may be nil iff ``cacheHit == false``.
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
  ## ``exec`` variant of ``sendExtendedQuery`` (no per-column format tracking).
  ## Precondition: ``cached`` may be nil iff ``cacheHit == false``.
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
        conn.pendingStmtCloses.add(stmtName)
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
  var rd: RowData
  var cachedParamOids: seq[int32]
  var callbackError: ref CatchableError = nil

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

  # Wrap the user callback so we can bump the int64 rowCount on success
  # (nextMessage counts through a ptr int32 which is too narrow for queryEach).
  let onRow: RowCallback = proc(row: Row) {.gcsafe, raises: [CatchableError].} =
    callback(row)
    rowCount += 1

  conn.pumpUntilReady(rd, onRow, addr callbackError):
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
      rd = newRowData(int16(fields.len), cf, co)
      rd.fields = fields
    of bmkNoData, bmkCommandComplete, bmkEmptyQueryResponse:
      discard
    else:
      discard
  do:
    # Callback errors take precedence over server errors: match the previous
    # inline order and skip cache updates when the caller's callback failed.
    if callbackError != nil:
      raise callbackError
    if queryError != nil:
      if cacheHit and queryError.sqlState in StmtCacheInvalidatingStates:
        conn.pendingStmtCloses.add(stmtName)
        conn.removeStmtCache(sql)
    elif cacheMiss:
      conn.addStmtCache(
        sql,
        CachedStmt(name: stmtName, fields: cachedFields, paramOids: cachedParamOids),
      )

template execRecvLoop*(
    conn: PgConnection,
    sql: string,
    cacheHit, cacheMiss: bool,
    stmtName: string,
    commandTag: var string,
) =
  ## Receive-loop counterpart of `queryRecvLoop` for the extended-query exec
  ## path: `DataRow`s are dropped by the parser (bare `pumpUntilReady` uses
  ## `skipDataRow = true`); this loop only exposes the `CommandComplete` tag
  ## via the `commandTag` out-parameter. Shared by `execImpl` (both
  ## overloads), `execInlineImpl`, and `execDirectRunImpl`.
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
    of bmkCommandComplete:
      commandTag = pumpMsg.commandTag
    of bmkEmptyQueryResponse:
      discard
    else:
      discard
  do:
    if queryError != nil:
      if cacheHit and queryError.sqlState in StmtCacheInvalidatingStates:
        conn.pendingStmtCloses.add(stmtName)
        conn.removeStmtCache(sql)
    elif cacheMiss:
      conn.addStmtCache(
        sql,
        CachedStmt(name: stmtName, fields: cachedFields, paramOids: cachedParamOids),
      )
