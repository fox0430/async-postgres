## Dedicated unit tests for pure helpers in ``pg_connection/types``.
## Covers default resolution, portal naming, dial/display host, and raw
## replication LSN bookkeeping — all without a live server or mock transport.

import std/[unittest, tables, importutils]

import ../async_postgres/[async_backend, pg_protocol, pg_auth]
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
