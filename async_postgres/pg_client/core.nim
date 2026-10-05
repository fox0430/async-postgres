## Shared building blocks for ``pg_client`` submodules (transaction opts, inline params, recv loops).
##
## Internal module: not part of the public API. Import the `pg_client` hub instead.

import std/[options, math, random]

import ../[async_backend, pg_protocol, pg_types]
import ../pg_connection/[types, buffer_io, simple_query]
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

proc describedRowData*(
    fields: var seq[FieldDescription], portal: bool, resultFormats: openArray[int16]
): RowData =
  ## RowData for a RowDescription. A portal Describe reports the formats Bind
  ## negotiated; a statement Describe reports text for every column, so the
  ## requested formats are stamped over ``fields``.
  var cf: seq[int16]
  var co: seq[int32]
  if portal:
    cf = newSeq[int16](fields.len)
    co = newSeq[int32](fields.len)
    for i in 0 ..< fields.len:
      cf[i] = fields[i].formatCode
      co[i] = fields[i].typeOid
  elif resultFormats.len > 0:
    cf = deriveColFmts(resultFormats, fields.len)
    co = newSeq[int32](fields.len)
    for i in 0 ..< fields.len:
      co[i] = fields[i].typeOid
      fields[i].formatCode = cf[i]
  result = newRowData(int16(fields.len), cf, co)
  result.fields = fields

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
  ## Whether `e` is a `PgQueryError` whose SQLSTATE is in `states`. The `25P02`
  ## a transaction macro raises for a COMMIT answered with ROLLBACK also
  ## qualifies when its `parent`, the error that aborted the transaction, does.
  ## Non-`PgQueryError` failures (connection drops, timeouts) are never
  ## retryable here: they leave the connection unusable for a fresh attempt.
  if e of PgQueryError:
    let qe = (ref PgQueryError)(e)
    qe.sqlState in states or (
      qe.sqlState == SqlStateInFailedSqlTransaction and qe.parent != nil and
      qe.parent of PgQueryError and (ref PgQueryError)(qe.parent).sqlState in states
    )
  else:
    false

const StmtCacheInvalidatingStates* = ["26000", "0A000"]
  ## SQLSTATEs that invalidate cached prepared statements (requires re-parse).
  ## ``42P18`` is absent on purpose: it is Parse-phase, so no cached statement
  ## can hit it.

type
  StmtCacheStatus* = enum
    ## How an operation uses the statement cache, decided in the send phase.
    scsUncached ## Caching disabled: unnamed Parse.
    scsHit ## Binds a cache entry's statement (same SQL, matching OIDs).
    scsShare
      ## Binds a statement an earlier op of the same pipeline Parses (same SQL,
      ## compatible OIDs). Skips Parse/Describe(Statement) and Describes the
      ## portal instead.
    scsMiss ## Parses a fresh named statement; `settleStmtCache` decides its fate.

  OpFacts* = object
    ## What the server confirmed about an op's Bind and, for a cache miss, its
    ## Parse and Describe(Statement). The cache acts on these alone: whether
    ## the operation failed says nothing about how far the server got.
    bound*: bool ## ``BindComplete`` arrived: a later error is the statement's own.
    parsed*: bool ## ``ParseComplete`` arrived: the statement exists.
    parsedGen*: int ## ``stmtCacheResetGen`` when it arrived.
    described*: bool
      ## ``RowDescription`` or ``NoData`` arrived: ``fields`` and ``paramOids``
      ## are complete.
    fields*: seq[FieldDescription]
    paramOids*: seq[int32]

func stmtCacheStatus*(cacheHit, cacheMiss: bool): StmtCacheStatus {.inline.} =
  if cacheHit:
    scsHit
  elif cacheMiss:
    scsMiss
  else:
    scsUncached

proc observe*(facts: var OpFacts, conn: PgConnection, msg: BackendMessage, miss: bool) =
  ## Record a reply to the op's Bind, or to a miss's Parse or
  ## Describe(Statement). Only a miss settles on the latter.
  if msg.kind == bmkBindComplete:
    facts.bound = true
  elif miss:
    case msg.kind
    of bmkParseComplete:
      facts.parsed = true
      facts.parsedGen = conn.stmtCacheResetGen
    of bmkParameterDescription:
      facts.paramOids = msg.paramTypeOids
    of bmkRowDescription:
      facts.fields = msg.fields
      facts.described = true
    of bmkNoData:
      facts.described = true
    else:
      discard

proc settleStmtCache*(
    conn: PgConnection,
    sql, stmtName: string,
    cache: StmtCacheStatus,
    facts: var OpFacts,
    queryError: ref PgQueryError,
) =
  ## Statement-cache bookkeeping once an operation's ``ReadyForQuery``
  ## arrives. A miss the server Described is cached even when Bind or Execute
  ## then failed (a plan gone stale meanwhile fails its next hit, which
  ## invalidates it); one it Parsed but did not Describe is Closed; one it
  ## never Parsed is left alone, since the name may be someone else's (42P05).
  ## A miss Parsed before a later ``DISCARD ALL`` / ``DEALLOCATE ALL``
  ## (``facts.parsedGen`` behind ``stmtCacheResetGen``) is neither cached nor
  ## Closed: the server already dropped the statement. A hit or share that
  ## bound is kept: the error is the statement's own.
  case cache
  of scsMiss:
    if facts.parsed and facts.parsedGen != conn.stmtCacheResetGen:
      discard
    elif facts.described:
      conn.addStmtCache(
        sql,
        CachedStmt(
          name: stmtName, fields: move facts.fields, paramOids: move facts.paramOids
        ),
      )
    elif facts.parsed:
      conn.queueStmtClose(stmtName)
  of scsHit, scsShare:
    if queryError == nil or facts.bound or
        queryError.sqlState notin StmtCacheInvalidatingStates:
      discard
    elif cache == scsHit and queryError.sqlState == "26000":
      # Gone without a reset tag (a DEALLOCATE ALL inside a function): one
      # failure instead of one per entry. Not for a share, whose 26000 is the
      # earlier op's failed Parse.
      conn.invalidateAllStmtCache(sql, stmtName)
    else:
      conn.invalidateStmtCache(sql, stmtName)
  of scsUncached:
    discard

func shouldRetryStmtCacheInvalidation*(
    conn: PgConnection, cacheHit: bool, sqlState: string, bound: bool
): bool =
  ## Whether a failed op may be re-issued once from Parse: a cache-hit Bind
  ## refused with ``26000`` (statements dropped without a reset tag) outside a
  ## transaction. Bind precedes Execute, so nothing ran; ``bound`` means the
  ## error came later, from the statement itself.
  cacheHit and not bound and sqlState == "26000" and conn.txStatus == tsIdle

template retryStmtCacheInvalidation*(
    conn: PgConnection, cacheHit, facts, body: untyped
) =
  ## Run ``body``, one extended-query op that sets ``cacheHit`` in its send
  ## phase, fills ``facts`` in its receive loop and ``return``s on success,
  ## and re-issue it once when ``shouldRetryStmtCacheInvalidation`` allows. The
  ## re-issue passes ``checkReady`` first, so a ``close()`` during the failed
  ## attempt stops it.
  var retried = false
  while true:
    var cacheHit = false
    var facts: OpFacts
    try:
      body
    except PgQueryError as e:
      if retried or
          not conn.shouldRetryStmtCacheInvalidation(cacheHit, e.sqlState, facts.bound):
        raise e
      retried = true
      conn.checkReady()

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
  conn.invalidateStmtCache(sql, cached.name)
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
  conn.invalidateStmtCache(sql, cached.name)
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
  ## Runs before the send templates start filling `sendBuf`, so a call this
  ## rejects fails without having touched the connection.
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
  ## Precondition: ``cached`` may be nil iff ``cacheHit == false``; the
  ## cache-miss and cache-disabled branches never read it.
  conn.beginSendBuf()
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
    conn.addExecute("", 0)
    conn.addSync()
  elif conn.stmtCachingEnabled:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    effectiveResultFormats = resultFormats
    conn.evictForInsert()
    parseStep
    conn.addDescribe(dkStatement, stmtName)
    bindStep
    conn.addExecute("", 0)
    conn.addSync()
  else:
    stmtName = ""
    effectiveResultFormats = resultFormats
    parseStep
    bindStep
    conn.addDescribe(dkPortal, "")
    conn.addExecute("", 0)
    conn.addSync()

template sendExtendedExec*(
    conn: PgConnection,
    cached: CachedStmt,
    cacheHit, cacheMiss: var bool,
    stmtName: var string,
    parseStep, bindStep: untyped,
) =
  ## ``exec`` variant of ``sendExtendedQuery`` (no per-column format tracking).
  ## Precondition: ``cached`` may be nil iff ``cacheHit == false``.
  conn.beginSendBuf()
  if cacheHit:
    stmtName = cached.name
    bindStep
    conn.addExecute("", 0)
    conn.addSync()
  elif conn.stmtCachingEnabled:
    cacheMiss = true
    stmtName = conn.nextStmtName()
    conn.evictForInsert()
    parseStep
    conn.addDescribe(dkStatement, stmtName)
    bindStep
    conn.addExecute("", 0)
    conn.addSync()
  else:
    stmtName = ""
    parseStep
    bindStep
    conn.addExecute("", 0)
    conn.addSync()

template queryRecvLoop*(
    conn: PgConnection,
    sql: string,
    resultFormats: openArray[int16],
    cacheHit, cacheMiss: bool,
    stmtName: string,
    cachedFields: seq[FieldDescription],
    cachedColFmts: seq[int16],
    cachedColOids: seq[int32],
    qr: var QueryResult,
    facts: var OpFacts,
) =
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
    facts.observe(conn, pumpMsg, cacheMiss)
    case pumpMsg.kind
    of bmkRowDescription:
      # A cache hit sends no Describe; only the cache-disabled path Describes
      # the portal.
      qr.fields = pumpMsg.fields
      qr.data = describedRowData(qr.fields, portal = not cacheMiss, resultFormats)
    of bmkCommandComplete:
      qr.commandTag = pumpMsg.commandTag
    else:
      discard
  do:
    conn.settleStmtCache(
      sql, stmtName, stmtCacheStatus(cacheHit, cacheMiss), facts, queryError
    )

template queryEachRecvLoop*(
    conn: PgConnection,
    sql: string,
    resultFormats: openArray[int16],
    cacheHit, cacheMiss: bool,
    stmtName: string,
    cachedFields: seq[FieldDescription],
    cachedColFmts: seq[int16],
    cachedColOids: seq[int32],
    callback: RowCallback,
    rowCount: var int64,
    facts: var OpFacts,
) =
  var rd: RowData
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
    facts.observe(conn, pumpMsg, cacheMiss)
    if pumpMsg.kind == bmkRowDescription:
      var fields = pumpMsg.fields
      rd = describedRowData(fields, portal = not cacheMiss, resultFormats)
  do:
    # The statement's fate is the server's outcome, whatever the callback did.
    conn.settleStmtCache(
      sql, stmtName, stmtCacheStatus(cacheHit, cacheMiss), facts, queryError
    )
    # Callback errors take precedence over server errors.
    if callbackError != nil:
      raise callbackError

template execRecvLoop*(
    conn: PgConnection,
    sql: string,
    cacheHit, cacheMiss: bool,
    stmtName: string,
    commandTag: var string,
    facts: var OpFacts,
) =
  ## Receive-loop counterpart of `queryRecvLoop` for the extended-query exec
  ## path: `DataRow`s are dropped by the parser (bare `pumpUntilReady` uses
  ## `skipDataRow = true`); this loop only exposes the `CommandComplete` tag
  ## via the `commandTag` out-parameter. Shared by `execImpl` (both
  ## overloads), `execInlineImpl`, and `execDirectRunImpl`.
  conn.pumpUntilReady:
    facts.observe(conn, pumpMsg, cacheMiss)
    if pumpMsg.kind == bmkCommandComplete:
      commandTag = pumpMsg.commandTag
  do:
    conn.settleStmtCache(
      sql, stmtName, stmtCacheStatus(cacheHit, cacheMiss), facts, queryError
    )
