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
    retryableStates*: seq[string] = @["40001", "40P01"]
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
    timeout: Duration = ZeroDuration,
) =
  var queryError: ref PgQueryError
  var cachedParamOids: seq[int32]

  if cacheHit:
    swap(qr.fields, cachedFields)
    if resultFormats.len > 0 and cachedColFmts.len > 0:
      for i in 0 ..< qr.fields.len:
        qr.fields[i].formatCode = cachedColFmts[i]
    if qr.fields.len > 0:
      qr.data = newRowData(int16(qr.fields.len), cachedColFmts, cachedColOids)
      qr.data.fields = qr.fields

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(qr.data, addr qr.rowCount); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          if cacheMiss:
            cachedParamOids = msg.paramTypeOids
        of bmkRowDescription:
          var cf: seq[int16]
          var co: seq[int32]
          if cacheMiss:
            cachedFields = msg.fields
            qr.fields = cachedFields
            if resultFormats.len > 0:
              cf = newSeq[int16](qr.fields.len)
              co = newSeq[int32](qr.fields.len)
              for i in 0 ..< qr.fields.len:
                co[i] = qr.fields[i].typeOid
                if resultFormats.len == 1:
                  qr.fields[i].formatCode = resultFormats[0]
                  cf[i] = resultFormats[0]
                elif i < resultFormats.len:
                  qr.fields[i].formatCode = resultFormats[i]
                  cf[i] = resultFormats[i]
          else:
            qr.fields = msg.fields
          qr.data = newRowData(int16(qr.fields.len), cf, co)
          qr.data.fields = qr.fields
        of bmkNoData:
          discard
        of bmkCommandComplete:
          qr.commandTag = msg.commandTag
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            if cacheHit and queryError.sqlState == "26000":
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
      await conn.fillRecvBuf(timeout)

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
    timeout: Duration = ZeroDuration,
) =
  var queryError: ref PgQueryError
  var rd: RowData
  var callbackError: ref CatchableError = nil
  var cachedParamOids: seq[int32]

  if cacheHit:
    if cachedColFmts.len > 0 or cachedColOids.len > 0:
      rd = newRowData(int16(cachedFields.len), cachedColFmts, cachedColOids)
    else:
      rd = newRowData(int16(cachedFields.len))
    rd.fields = cachedFields
    if resultFormats.len > 0 and cachedColFmts.len > 0:
      for i in 0 ..< cachedFields.len:
        rd.colFormats[i] = cachedColFmts[i]

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
          except ProtocolError as e:
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
        let msg = res.message
        case msg.kind
        of bmkNotificationResponse:
          conn.dispatchNotification(msg)
          continue
        of bmkNoticeResponse:
          conn.dispatchNotice(msg)
          continue
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          if cacheMiss:
            cachedParamOids = msg.paramTypeOids
        of bmkRowDescription:
          var cf: seq[int16]
          var co: seq[int32]
          if cacheMiss:
            cachedFields = msg.fields
            if resultFormats.len > 0:
              cf = newSeq[int16](cachedFields.len)
              co = newSeq[int32](cachedFields.len)
              for i in 0 ..< cachedFields.len:
                co[i] = cachedFields[i].typeOid
                if resultFormats.len == 1:
                  cachedFields[i].formatCode = resultFormats[0]
                  cf[i] = resultFormats[0]
                elif i < resultFormats.len:
                  cachedFields[i].formatCode = resultFormats[i]
                  cf[i] = resultFormats[i]
          else:
            cachedFields = msg.fields
          rd = newRowData(int16(cachedFields.len), cf, co)
          rd.fields = cachedFields
        of bmkNoData:
          discard
        of bmkCommandComplete:
          discard
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.recvBufStart = pos
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if callbackError != nil:
            raise callbackError
          if queryError != nil:
            if cacheHit and queryError.sqlState == "26000":
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
      await conn.fillRecvBuf(timeout)

template execRecvLoop*(
    conn: PgConnection,
    sql: string,
    cacheHit, cacheMiss: bool,
    stmtName: string,
    commandTag: var string,
    timeout: Duration = ZeroDuration,
) =
  ## Receive-loop counterpart of `queryRecvLoop` for the extended-query exec
  ## path: discards `DataRow`s (exec callers don't need rows) and exposes only
  ## the `CommandComplete` tag via the `commandTag` out-parameter. Shared by
  ## `execImpl` (both overloads), `execInlineImpl`, and `execDirectRunImpl`.
  var queryError: ref PgQueryError
  var cachedFields: seq[FieldDescription]
  var cachedParamOids: seq[int32]

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkParseComplete, bmkBindComplete, bmkCloseComplete:
          discard
        of bmkParameterDescription:
          if cacheMiss:
            cachedParamOids = msg.paramTypeOids
        of bmkRowDescription:
          if cacheMiss:
            cachedFields = msg.fields
        of bmkNoData:
          discard
        of bmkDataRow:
          discard
        of bmkCommandComplete:
          commandTag = msg.commandTag
        of bmkEmptyQueryResponse:
          discard
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            if cacheHit and queryError.sqlState == "26000":
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
      await conn.fillRecvBuf(timeout)
