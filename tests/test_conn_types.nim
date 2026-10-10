## Dedicated unit tests for pure helpers in ``pg_connection/types``.
## Covers default resolution, portal naming, dial/display host, raw
## replication LSN bookkeeping, tracer routing, notice dispatch and the
## replication write queue — all without a live server or mock transport.

import std/[unittest, tables, deques, strutils, importutils]

import ../async_postgres/[async_backend, pg_protocol, pg_auth, pg_errors]
import ../async_postgres/pg_connection/types {.all.}

privateAccess(PgConnection)

proc bareConn(config = ConnConfig()): PgConnection =
  PgConnection(
    recvBuf: @[],
    state: csReady,
    txStatus: tsIdle,
    serverParams: initTable[string, string](),
    createdAt: Moment.now(),
    config: config,
  )

suite "conn-types effective defaults":
  test "effectiveMaxMessageSize resolves 0 to the protocol default":
    let conn = bareConn()
    check conn.config.maxMessageSize == 0
    check conn.effectiveMaxMessageSize == DefaultMaxBackendMessageLen
    conn.config.maxMessageSize = 4096
    check conn.effectiveMaxMessageSize == 4096

  test "effectiveMaxScramIterations resolves 0 to the auth default":
    var cfg = ConnConfig()
    check cfg.maxScramIterations == 0
    check effectiveMaxScramIterations(cfg) == DefaultMaxScramIterations
    cfg.maxScramIterations = 1000
    check effectiveMaxScramIterations(cfg) == 1000

suite "conn-types portal and host helpers":
  test "nextPortalName is monotonic under a shared prefix":
    let conn = bareConn()
    let a = conn.nextPortalName("p")
    let b = conn.nextPortalName("p")
    check a == "p1"
    check b == "p2"

  test "dialAddr prefers hostaddr; displayHost prefers host":
    let both = HostEntry(host: "db.example", hostaddr: "10.0.0.1", port: 5432)
    check dialAddr(both) == "10.0.0.1"
    check displayHost(both) == "db.example"
    let onlyHost = HostEntry(host: "db.example", port: 5432)
    check dialAddr(onlyHost) == "db.example"
    check displayHost(onlyHost) == "db.example"
    let onlyAddr = HostEntry(hostaddr: "10.0.0.1", port: 5432)
    check dialAddr(onlyAddr) == "10.0.0.1"
    check displayHost(onlyAddr) == "10.0.0.1"

suite "conn-types replication LSN helpers":
  test "init / update / confirm clamp and advance monotonically":
    let conn = bareConn()
    conn.initReplLsnTracking(100)
    check conn.replConfirmedFlushLsn == 100
    check conn.replMaxReceivedLsn == 100

    check not conn.updateReplMaxReceivedLsn(50)
    check conn.replMaxReceivedLsn == 100
    check conn.updateReplMaxReceivedLsn(150)
    check conn.replMaxReceivedLsn == 150

    # Above max-received clamps to max and advances flush.
    check conn.confirmReplFlushed(200)
    check conn.replConfirmedFlushLsn == 150
    # Below confirmed is a no-op; between confirmed and max advances.
    check not conn.confirmReplFlushed(110)
    check conn.replConfirmedFlushLsn == 150
    conn.initReplLsnTracking(100)
    discard conn.updateReplMaxReceivedLsn(150)
    check conn.confirmReplFlushed(120)
    check conn.replConfirmedFlushLsn == 120
    check not conn.confirmReplFlushed(110)
    check conn.replConfirmedFlushLsn == 120

type
  TestSpan = ref object of RootObj

  TracedCtx = tuple[started, replaced, seen, ended: TraceContext]

# Procs, not test bodies: the gcsafe hooks capture these locals, and a test
# body's locals are globals.
proc connTracingCtx(): TracedCtx =
  var r: TracedCtx = (TestSpan(), TestSpan(), nil, nil)
  let conn = bareConn()
  conn.tracer = PgTracer(
    onPrepareStart: proc(
        c: PgConnection, d: TracePrepareStartData
    ): TraceContext {.gcsafe, raises: [].} =
      r.started,
    onPrepareEnd: proc(
        ctx: TraceContext, c: PgConnection, d: TracePrepareEndData
    ) {.gcsafe, raises: [].} =
      r.ended = ctx,
  )
  withConnTracing(
    conn,
    onPrepareStart,
    onPrepareEnd,
    TracePrepareStartData(),
    TracePrepareEndData,
    TracePrepareEndData(),
  ):
    r.seen = traceCtx
    traceCtx = r.replaced
  r

proc tracingCtx(): TracedCtx =
  var r: TracedCtx = (TestSpan(), TestSpan(), nil, nil)
  let tracer = PgTracer(
    onPoolAcquireStart: proc(
        d: TracePoolAcquireStartData
    ): TraceContext {.gcsafe, raises: [].} =
      r.started,
    onPoolAcquireEnd: proc(
        ctx: TraceContext, d: TracePoolAcquireEndData
    ) {.gcsafe, raises: [].} =
      r.ended = ctx,
  )
  withTracing(
    tracer,
    onPoolAcquireStart,
    onPoolAcquireEnd,
    TracePoolAcquireStartData(),
    TracePoolAcquireEndData,
    TracePoolAcquireEndData(),
  ):
    r.seen = traceCtx
    traceCtx = r.replaced
  r

suite "conn-types tracing helpers":
  test "withConnTracing: body reads and replaces traceCtx":
    let r = connTracingCtx()
    check r.seen == r.started
    check r.ended == r.replaced

  test "withTracing: body reads and replaces traceCtx":
    let r = tracingCtx()
    check r.seen == r.started
    check r.ended == r.replaced

  test "withConnTracing: traceCtx cannot take over a name in its scope":
    # As with the variable it replaced: a second call, or a scope that already
    # has a `traceCtx`, is a redefinition.
    check not compiles(
      block:
        let conn = bareConn()
        withConnTracing(
          conn,
          onPrepareStart,
          onPrepareEnd,
          TracePrepareStartData(),
          TracePrepareEndData,
          TracePrepareEndData(),
        ):
          discard
        withConnTracing(
          conn,
          onPrepareStart,
          onPrepareEnd,
          TracePrepareStartData(),
          TracePrepareEndData,
          TracePrepareEndData(),
        ):
          discard
    )
    check not compiles(
      block:
        let conn = bareConn()
        template traceCtx(): int =
          0

        withConnTracing(
          conn,
          onPrepareStart,
          onPrepareEnd,
          TracePrepareStartData(),
          TracePrepareEndData,
          TracePrepareEndData(),
        ):
          discard
    )
    check compiles(
      block:
        let conn = bareConn()
        block:
          withConnTracing(
            conn,
            onPrepareStart,
            onPrepareEnd,
            TracePrepareStartData(),
            TracePrepareEndData,
            TracePrepareEndData(),
          ):
            discard
        withConnTracing(
          conn,
          onPrepareStart,
          onPrepareEnd,
          TracePrepareStartData(),
          TracePrepareEndData,
          TracePrepareEndData(),
        ):
          discard
    )

type FailedSpan = object
  started, replaced, ended: TraceContext
  endErr, caught: ref CatchableError
  endCount: int

# The body always raises, which makes the tracing template's success-path End
# unreachable; the warning is about the helper, not a defect worth a branch.
{.push warning[UnreachableCode]: off.}

proc connTracingFailure(boom: ref CatchableError): FailedSpan =
  var r = FailedSpan(started: TestSpan(), replaced: TestSpan())
  let conn = bareConn()
  conn.tracer = PgTracer(
    onPrepareStart: proc(
        c: PgConnection, d: TracePrepareStartData
    ): TraceContext {.gcsafe, raises: [].} =
      r.started,
    onPrepareEnd: proc(
        ctx: TraceContext, c: PgConnection, d: TracePrepareEndData
    ) {.gcsafe, raises: [].} =
      inc r.endCount
      r.ended = ctx
      r.endErr = d.err,
  )
  try:
    withConnTracing(
      conn,
      onPrepareStart,
      onPrepareEnd,
      TracePrepareStartData(),
      TracePrepareEndData,
      TracePrepareEndData(),
    ):
      traceCtx = r.replaced
      raise boom
  except CatchableError as caughtErr:
    r.caught = caughtErr
  r

proc tracingFailure(boom: ref CatchableError): FailedSpan =
  var r = FailedSpan(started: TestSpan(), replaced: TestSpan())
  let tracer = PgTracer(
    onPoolAcquireStart: proc(
        d: TracePoolAcquireStartData
    ): TraceContext {.gcsafe, raises: [].} =
      r.started,
    onPoolAcquireEnd: proc(
        ctx: TraceContext, d: TracePoolAcquireEndData
    ) {.gcsafe, raises: [].} =
      inc r.endCount
      r.ended = ctx
      r.endErr = d.err,
  )
  try:
    withTracing(
      tracer,
      onPoolAcquireStart,
      onPoolAcquireEnd,
      TracePoolAcquireStartData(),
      TracePoolAcquireEndData,
      TracePoolAcquireEndData(),
    ):
      traceCtx = r.replaced
      raise boom
  except CatchableError as caughtErr:
    r.caught = caughtErr
  r

{.pop.}

suite "conn-types tracing helpers on a raising body":
  test "withConnTracing: End gets the body's error, which is re-raised":
    let boom = newException(ValueError, "boom")
    let r = connTracingFailure(boom)
    check r.endCount == 1
    check r.ended == r.replaced
    check r.endErr == boom
    check r.caught == boom

  test "withTracing: End gets the body's error, which is re-raised":
    let boom = newException(ValueError, "boom")
    let r = tracingFailure(boom)
    check r.endCount == 1
    check r.ended == r.replaced
    check r.endErr == boom
    check r.caught == boom

type AuthAdvice = ref object
  insecure: seq[TraceInsecureAuthData]
  deprecated: seq[TraceDeprecatedAuthData]

proc authAdviceTracer(log: AuthAdvice): PgTracer =
  PgTracer(
    onInsecureAuth: proc(d: TraceInsecureAuthData) {.gcsafe, raises: [].} =
      log.insecure.add(d),
    onDeprecatedAuth: proc(d: TraceDeprecatedAuthData) {.gcsafe, raises: [].} =
      log.deprecated.add(d),
  )

suite "conn-types auth advisories":
  # Only `config.tracer` is set: the runtime `conn.tracer` alias stays nil, so
  # a helper reading it would fire nothing.
  test "fireInsecureAuth reports the method and transport state":
    let log = AuthAdvice()
    let conn = bareConn(ConnConfig(tracer: authAdviceTracer(log)))
    conn.fireInsecureAuth(amPassword)
    conn.sslEnabled = true
    conn.fireInsecureAuth(amPassword)
    check log.insecure.len == 2
    check log.insecure[0].conn == conn
    check log.insecure[0].authMethod == amPassword
    check not log.insecure[0].sslEnabled
    check log.insecure[1].sslEnabled
    check log.deprecated.len == 0

  test "fireDeprecatedAuth reports the method":
    let log = AuthAdvice()
    let conn = bareConn(ConnConfig(tracer: authAdviceTracer(log)))
    conn.fireDeprecatedAuth(amMd5)
    check log.deprecated.len == 1
    check log.deprecated[0].conn == conn
    check log.deprecated[0].authMethod == amMd5
    check log.insecure.len == 0

  test "auth advisories are no-ops without a tracer or a hook":
    let untraced = bareConn()
    untraced.fireInsecureAuth(amPassword)
    untraced.fireDeprecatedAuth(amMd5)
    let hookless = bareConn(ConnConfig(tracer: PgTracer()))
    hookless.fireInsecureAuth(amPassword)
    hookless.fireDeprecatedAuth(amMd5)

proc noticeSink(seen: ref seq[Notice]): NoticeCallback =
  result = proc(n: Notice) {.gcsafe, raises: [].} =
    seen[].add n

proc notifySink(calls: ref int): NotifyCallback =
  result = proc(n: Notification) {.gcsafe, raises: [].} =
    inc calls[]

suite "conn-types notice dispatch":
  test "dispatchNotice hands the notice fields to the callback":
    let seen = new seq[Notice]
    let conn = bareConn()
    conn.noticeCallback = noticeSink(seen)
    conn.dispatchNotice(
      BackendMessage(
        kind: bmkNoticeResponse,
        noticeFields: @[
          ErrorField(code: 'S', value: "WARNING"),
          ErrorField(code: 'M', value: "careful"),
        ],
      )
    )
    check seen[].len == 1
    check seen[][0].fields.len == 2
    check seen[][0].fields[0].code == 'S'
    check seen[][0].fields[0].value == "WARNING"
    check seen[][0].fields[1].code == 'M'
    check seen[][0].fields[1].value == "careful"

  test "dispatchNotice without a notice callback is a no-op":
    let conn = bareConn()
    let notified = new int
    conn.notifyCallback = notifySink(notified)
    conn.dispatchNotice(
      BackendMessage(
        kind: bmkNoticeResponse,
        noticeFields: @[ErrorField(code: 'M', value: "careful")],
      )
    )
    # A notice is not a LISTEN notification: neither path carries it.
    check notified[] == 0
    check not conn.hasNotification

proc streamConn(): PgConnection =
  result = bareConn()
  result.replResetStream(autoConfirm = false, serverFlush = 0)

proc startNextReplWrite(conn: PgConnection): ReplWrite =
  ## One drain step: take the next write and mark it started.
  result = conn.nextReplWrite()
  if result != nil:
    result.state = rwWriting

proc failedAsEnded(fut: Future[void]): bool =
  fut.failed and fut.error of PgStateError and "ended before this write" in fut.error.msg

suite "conn-types replication write queue":
  test "caller writes drain in order, then the final status, then CopyDone":
    let conn = streamConn()
    let first = ReplWrite(frame: @[1'u8])
    let second = ReplWrite(frame: @[2'u8])
    conn.replQueueWrite(first)
    conn.replQueueWrite(second)
    let copyDone = conn.replQueueStop()
    check copyDone.frame == @copyDoneMsg
    let final = conn.replFinalStatus
    check final != nil
    check final.frame.len == 0
    # Queued after the stop, still ahead of its final status.
    let third = ReplWrite(frame: @[3'u8])
    conn.replQueueWrite(third)
    for want in [first, second, third, final, copyDone]:
      let w = conn.startNextReplWrite()
      check w == want
      conn.settleReplWrite(w, ok = true)
    check conn.nextReplWrite() == nil

  test "the library's status is shared at the tail, not behind a caller write":
    let conn = streamConn()
    let s1 = conn.queueReplStatus()
    check conn.queueReplStatus() == s1
    check conn.replWrites.len == 1
    let caller = ReplWrite(frame: @[1'u8])
    conn.replQueueWrite(caller)
    check conn.tailPendingReplStatus() == nil
    let s2 = conn.queueReplStatus()
    check s2 != s1
    check conn.replWrites.len == 3
    check conn.startNextReplWrite() == s1
    check conn.startNextReplWrite() == caller
    check conn.tailPendingReplStatus() == s2
    # Taken off the queue, it is being encoded: the next status is a new entry.
    check conn.startNextReplWrite() == s2
    check conn.tailPendingReplStatus() == nil
    check conn.replPendingStatus == nil
    let s3 = conn.queueReplStatus()
    check s3 != s2
    check conn.replWrites.len == 1

  test "a stale status mark with an empty queue reads as nil":
    let conn = streamConn()
    # A mark whose entry is gone must not read the tail of an empty queue.
    conn.replPendingStatus = ReplWrite()
    check conn.tailPendingReplStatus() == nil
    # Queueing after it starts a fresh entry and takes over the mark.
    let status = conn.queueReplStatus()
    check conn.replWrites.len == 1
    check conn.replPendingStatus == status
    check conn.tailPendingReplStatus() == status

  test "a stop turns the status at the tail into its final status":
    let conn = streamConn()
    let status = conn.queueReplStatus()
    discard conn.replQueueStop()
    check conn.replFinalStatus == status
    check conn.replWrites.len == 0
    check conn.tailPendingReplStatus() == nil

    let behind = streamConn()
    let early = behind.queueReplStatus()
    let caller = ReplWrite(frame: @[1'u8])
    behind.replQueueWrite(caller)
    discard behind.replQueueStop()
    check behind.replFinalStatus != early
    check behind.replWrites.len == 2

  test "the final status and CopyDone wait for a running callback":
    let conn = streamConn()
    let caller = ReplWrite(frame: @[1'u8])
    conn.replQueueWrite(caller)
    let copyDone = conn.replQueueStop()
    conn.setReplInCallback(true)
    # Caller writes still drain; only the final status is held.
    check conn.startNextReplWrite() == caller
    check conn.nextReplWrite() == nil
    conn.setReplInCallback(false)
    let final = conn.startNextReplWrite()
    check final == conn.replFinalStatus
    conn.settleReplWrite(final, ok = true)
    check conn.startNextReplWrite() == copyDone

  test "failing the queue spares the write being sent":
    let conn = streamConn()
    let sending = ReplWrite(frame: @[1'u8])
    let queued = ReplWrite(frame: @[2'u8])
    conn.replQueueWrite(sending)
    conn.replQueueWrite(queued)
    let status = conn.queueReplStatus()
    let sendingFut = sending.waitReplWrite()
    let queuedFut = queued.waitReplWrite()
    let statusFut = status.waitReplWrite()
    let copyDone = conn.replQueueStop()
    let copyDoneFut = copyDone.waitReplWrite()
    check conn.replFinalStatus == status
    check conn.startNextReplWrite() == sending

    # A status queued after the stop stays pending: failing the queue must clear
    # the mark rather than leave it pointing at a write it dropped.
    let pending = conn.queueReplStatus()
    let pendingFut = pending.waitReplWrite()
    check pending != status

    conn.failQueuedReplWrites()
    check not sendingFut.finished
    check sending.state == rwWriting
    check queuedFut.failedAsEnded
    check queued.state == rwFailed
    check statusFut.failedAsEnded
    check pendingFut.failedAsEnded
    check conn.replPendingStatus == nil
    check conn.tailPendingReplStatus() == nil
    check copyDoneFut.failedAsEnded
    check conn.replWrites.len == 0
    check conn.nextReplWrite() == nil

  test "failing the queue spares a final status being sent":
    let conn = streamConn()
    let copyDone = conn.replQueueStop()
    let final = conn.replFinalStatus
    let finalFut = final.waitReplWrite()
    let copyDoneFut = copyDone.waitReplWrite()
    check conn.startNextReplWrite() == final

    conn.failQueuedReplWrites()
    check not finalFut.finished
    check final.state == rwWriting
    check copyDoneFut.failedAsEnded
    check copyDone.state == rwFailed
