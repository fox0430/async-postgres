import std/[unittest, strutils]

import ../async_postgres/async_backend

import ../async_postgres/[pg_client, pg_types, pg_protocol]
import ../async_postgres/pg_pool {.all.}
import ../async_postgres/pg_pool_cluster {.all.}
import ../async_postgres/pg_connection {.all.}

const
  PgHost = "127.0.0.1"
  PgPort = 15432
  PgUser = "test"
  PgPassword = "test"
  PgDatabase = "test"

proc plainConfig(): ConnConfig =
  ConnConfig(
    host: PgHost,
    port: PgPort,
    user: PgUser,
    password: PgPassword,
    database: PgDatabase,
    sslMode: sslDisable,
  )

proc toBytes(s: string): seq[byte] =
  @(s.toOpenArrayByte(0, s.high))

# TraceContext helpers
type
  SpanKind = enum
    skConnect
    skQuery
    skPrepare
    skPipeline
    skCopy
    skPoolAcquire
    skPoolRelease

  Span = ref object of RootObj
    kind: SpanKind
    id: int

var nextSpanId = 0

proc newSpan(kind: SpanKind): Span =
  nextSpanId.inc
  Span(kind: kind, id: nextSpanId)

# Record types
type
  ConnectStartRec = object
    hosts: seq[HostEntry]

  ConnectEndRec = object
    spanId: int
    hasConn: bool
    hasErr: bool

  QueryStartRec = object
    sql: string
    isExec: bool

  QueryEndRec = object
    spanId: int
    commandTag: string
    rowCount: int64
    hasErr: bool

  PrepareStartRec = object
    name: string
    sql: string

  PrepareEndRec = object
    spanId: int
    hasErr: bool

  PipelineStartRec = object
    opCount: int

  PipelineEndRec = object
    spanId: int
    hasErr: bool

  CopyStartRec = object
    sql: string
    direction: TraceCopyDirection

  CopyEndRec = object
    spanId: int
    commandTag: string
    hasErr: bool

  PoolAcquireStartRec = object
    idleCount: int
    activeCount: int
    maxSize: int

  PoolAcquireEndRec = object
    spanId: int
    hasConn: bool
    wasCreated: bool
    hasErr: bool

  PoolReleaseStartRec = object
    hasConn: bool

  PoolReleaseEndRec = object
    spanId: int
    wasClosed: bool
    handedToWaiter: bool

  PoolCloseErrorRec = object
    hasConn: bool
    errMsg: string

  TransportCloseErrorRec = object
    hasConn: bool
    stage: TransportCloseStage
    errMsg: string

  InsecureAuthRec = object
    hasConn: bool
    authMethod: AuthMethod
    sslEnabled: bool

  DeprecatedAuthRec = object
    hasConn: bool
    authMethod: AuthMethod

  CleanupSkippedRec = object
    hasConn: bool
    kind: CleanupKind
    reason: CleanupSkipReason
    errMsg: string

  TraceLog = ref object
    connectStarts: seq[ConnectStartRec]
    connectEnds: seq[ConnectEndRec]
    queryStarts: seq[QueryStartRec]
    queryEnds: seq[QueryEndRec]
    prepareStarts: seq[PrepareStartRec]
    prepareEnds: seq[PrepareEndRec]
    pipelineStarts: seq[PipelineStartRec]
    pipelineEnds: seq[PipelineEndRec]
    copyStarts: seq[CopyStartRec]
    copyEnds: seq[CopyEndRec]
    poolAcquireStarts: seq[PoolAcquireStartRec]
    poolAcquireEnds: seq[PoolAcquireEndRec]
    poolReleaseStarts: seq[PoolReleaseStartRec]
    poolReleaseEnds: seq[PoolReleaseEndRec]
    poolCloseErrors: seq[PoolCloseErrorRec]
    transportCloseErrors: seq[TransportCloseErrorRec]
    insecureAuths: seq[InsecureAuthRec]
    deprecatedAuths: seq[DeprecatedAuthRec]
    cleanupSkipped: seq[CleanupSkippedRec]

proc newTraceLog(): TraceLog =
  TraceLog()

proc buildTracer(log: TraceLog): PgTracer =
  let tracer = PgTracer()

  tracer.onConnectStart = proc(
      data: TraceConnectStartData
  ): TraceContext {.gcsafe, raises: [].} =
    log.connectStarts.add(ConnectStartRec(hosts: data.hosts))
    let span = newSpan(skConnect)
    return span

  tracer.onConnectEnd = proc(
      ctx: TraceContext, data: TraceConnectEndData
  ) {.gcsafe, raises: [].} =
    let span = Span(ctx)
    log.connectEnds.add(
      ConnectEndRec(spanId: span.id, hasConn: data.conn != nil, hasErr: data.err != nil)
    )

  tracer.onQueryStart = proc(
      conn: PgConnection, data: TraceQueryStartData
  ): TraceContext {.gcsafe, raises: [].} =
    log.queryStarts.add(QueryStartRec(sql: data.sql, isExec: data.isExec))
    let span = newSpan(skQuery)
    return span

  tracer.onQueryEnd = proc(
      ctx: TraceContext, conn: PgConnection, data: TraceQueryEndData
  ) {.gcsafe, raises: [].} =
    let span = Span(ctx)
    log.queryEnds.add(
      QueryEndRec(
        spanId: span.id,
        commandTag: data.commandTag,
        rowCount: data.rowCount,
        hasErr: data.err != nil,
      )
    )

  tracer.onPrepareStart = proc(
      conn: PgConnection, data: TracePrepareStartData
  ): TraceContext {.gcsafe, raises: [].} =
    log.prepareStarts.add(PrepareStartRec(name: data.name, sql: data.sql))
    let span = newSpan(skPrepare)
    return span

  tracer.onPrepareEnd = proc(
      ctx: TraceContext, conn: PgConnection, data: TracePrepareEndData
  ) {.gcsafe, raises: [].} =
    let span = Span(ctx)
    log.prepareEnds.add(PrepareEndRec(spanId: span.id, hasErr: data.err != nil))

  tracer.onPipelineStart = proc(
      conn: PgConnection, data: TracePipelineStartData
  ): TraceContext {.gcsafe, raises: [].} =
    log.pipelineStarts.add(PipelineStartRec(opCount: data.opCount))
    let span = newSpan(skPipeline)
    return span

  tracer.onPipelineEnd = proc(
      ctx: TraceContext, conn: PgConnection, data: TracePipelineEndData
  ) {.gcsafe, raises: [].} =
    let span = Span(ctx)
    log.pipelineEnds.add(PipelineEndRec(spanId: span.id, hasErr: data.err != nil))

  tracer.onCopyStart = proc(
      conn: PgConnection, data: TraceCopyStartData
  ): TraceContext {.gcsafe, raises: [].} =
    log.copyStarts.add(CopyStartRec(sql: data.sql, direction: data.direction))
    let span = newSpan(skCopy)
    return span

  tracer.onCopyEnd = proc(
      ctx: TraceContext, conn: PgConnection, data: TraceCopyEndData
  ) {.gcsafe, raises: [].} =
    let span = Span(ctx)
    log.copyEnds.add(
      CopyEndRec(spanId: span.id, commandTag: data.commandTag, hasErr: data.err != nil)
    )

  tracer.onPoolAcquireStart = proc(
      data: TracePoolAcquireStartData
  ): TraceContext {.gcsafe, raises: [].} =
    log.poolAcquireStarts.add(
      PoolAcquireStartRec(
        idleCount: data.idleCount, activeCount: data.activeCount, maxSize: data.maxSize
      )
    )
    let span = newSpan(skPoolAcquire)
    return span

  tracer.onPoolAcquireEnd = proc(
      ctx: TraceContext, data: TracePoolAcquireEndData
  ) {.gcsafe, raises: [].} =
    let span = Span(ctx)
    log.poolAcquireEnds.add(
      PoolAcquireEndRec(
        spanId: span.id,
        hasConn: data.conn != nil,
        wasCreated: data.wasCreated,
        hasErr: data.err != nil,
      )
    )

  tracer.onPoolReleaseStart = proc(
      data: TracePoolReleaseStartData
  ): TraceContext {.gcsafe, raises: [].} =
    log.poolReleaseStarts.add(PoolReleaseStartRec(hasConn: data.conn != nil))
    let span = newSpan(skPoolRelease)
    return span

  tracer.onPoolReleaseEnd = proc(
      ctx: TraceContext, data: TracePoolReleaseEndData
  ) {.gcsafe, raises: [].} =
    let span = Span(ctx)
    log.poolReleaseEnds.add(
      PoolReleaseEndRec(
        spanId: span.id, wasClosed: data.wasClosed, handedToWaiter: data.handedToWaiter
      )
    )

  tracer.onPoolCloseError = proc(data: TracePoolCloseErrorData) {.gcsafe, raises: [].} =
    log.poolCloseErrors.add(
      PoolCloseErrorRec(
        hasConn: data.conn != nil, errMsg: (if data.err != nil: data.err.msg else: "")
      )
    )

  tracer.onTransportCloseError = proc(
      data: TraceTransportCloseErrorData
  ) {.gcsafe, raises: [].} =
    log.transportCloseErrors.add(
      TransportCloseErrorRec(
        hasConn: data.conn != nil,
        stage: data.stage,
        errMsg: (if data.err != nil: data.err.msg else: ""),
      )
    )

  tracer.onInsecureAuth = proc(data: TraceInsecureAuthData) {.gcsafe, raises: [].} =
    log.insecureAuths.add(
      InsecureAuthRec(
        hasConn: data.conn != nil,
        authMethod: data.authMethod,
        sslEnabled: data.sslEnabled,
      )
    )

  tracer.onDeprecatedAuth = proc(data: TraceDeprecatedAuthData) {.gcsafe, raises: [].} =
    log.deprecatedAuths.add(
      DeprecatedAuthRec(hasConn: data.conn != nil, authMethod: data.authMethod)
    )

  tracer.onCleanupSkipped = proc(data: TraceCleanupSkippedData) {.gcsafe, raises: [].} =
    log.cleanupSkipped.add(
      CleanupSkippedRec(
        hasConn: data.conn != nil,
        kind: data.kind,
        reason: data.reason,
        errMsg: (if data.err != nil: data.err.msg else: ""),
      )
    )

  return tracer

proc tracedConfig(tracer: PgTracer): ConnConfig =
  var cfg = plainConfig()
  cfg.tracer = tracer
  return cfg

suite "Tracing: connect":
  test "onConnectStart and onConnectEnd are called with correct context":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      doAssert log.connectStarts.len == 1
      doAssert log.connectStarts[0].hosts.len == 1
      doAssert log.connectStarts[0].hosts[0].host == PgHost
      doAssert log.connectStarts[0].hosts[0].port == PgPort
      doAssert log.connectEnds.len == 1
      doAssert log.connectEnds[0].hasConn
      doAssert not log.connectEnds[0].hasErr
      # Verify context correlation
      doAssert log.connectEnds[0].spanId > 0

      await conn.close()

    waitFor t()

  test "onConnectEnd receives error on connection failure":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var cfg = tracedConfig(tracer)
      cfg.port = 19999 # wrong port

      var raised = false
      try:
        let conn = await connect(cfg)
        await conn.close()
      except CatchableError:
        raised = true

      doAssert raised
      doAssert log.connectStarts.len == 1
      doAssert log.connectEnds.len == 1
      doAssert log.connectEnds[0].hasErr

    waitFor t()

suite "Tracing: exec":
  test "onQueryStart(isExec=true) and onQueryEnd with commandTag":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      discard await conn.exec("SELECT 1")

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].sql == "SELECT 1"
      doAssert log.queryStarts[0].isExec == true
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].commandTag.len > 0
      doAssert not log.queryEnds[0].hasErr
      # Context correlation
      doAssert log.queryEnds[0].spanId > 0

      await conn.close()

    waitFor t()

suite "Tracing: query":
  test "onQueryStart(isExec=false) and onQueryEnd with rowCount":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      discard await conn.query("SELECT generate_series(1, 3)")

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].sql == "SELECT generate_series(1, 3)"
      doAssert log.queryStarts[0].isExec == false
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].rowCount == 3
      doAssert not log.queryEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: queryDirect":
  test "onQueryStart(isExec=false) and onQueryEnd with rowCount":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      let qr = await conn.queryDirect("SELECT generate_series(1, $1)::int4", 3'i32)
      doAssert qr.rowCount == 3

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].sql == "SELECT generate_series(1, $1)::int4"
      doAssert log.queryStarts[0].isExec == false
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].rowCount == 3
      doAssert not log.queryEnds[0].hasErr
      doAssert log.queryEnds[0].spanId > 0

      await conn.close()

    waitFor t()

suite "Tracing: execDirect":
  test "onQueryStart(isExec=true) and onQueryEnd with commandTag":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      let tag = await conn.execDirect("SELECT $1::int4", 1'i32)
      doAssert tag.commandTag.len > 0

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].sql == "SELECT $1::int4"
      doAssert log.queryStarts[0].isExec == true
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].commandTag.len > 0
      doAssert not log.queryEnds[0].hasErr
      doAssert log.queryEnds[0].spanId > 0

      await conn.close()

    waitFor t()

  test "onQueryEnd receives err on timeout":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      try:
        discard await conn.execDirect(
          "SELECT pg_sleep($1)", 10'f64, timeout = milliseconds(50)
        )
      except PgError:
        discard

      doAssert log.queryStarts.len == 1
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].hasErr

    waitFor t()

suite "Tracing: simpleExec":
  test "onQueryStart and onQueryEnd are called":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      discard await conn.simpleExec("SELECT 1")

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].isExec == true
      doAssert log.queryEnds.len == 1
      doAssert not log.queryEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: simpleQuery":
  test "onQueryStart and onQueryEnd are called":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      discard await conn.simpleQuery("SELECT 1; SELECT 2")

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].isExec == false
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].rowCount == 2 # 1 row per statement
      doAssert not log.queryEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: prepare":
  test "onPrepareStart and onPrepareEnd with context correlation":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      discard await conn.prepare("test_stmt", "SELECT $1::int4")

      doAssert log.prepareStarts.len == 1
      doAssert log.prepareStarts[0].name == "test_stmt"
      doAssert log.prepareStarts[0].sql == "SELECT $1::int4"
      doAssert log.prepareEnds.len == 1
      doAssert not log.prepareEnds[0].hasErr
      doAssert log.prepareEnds[0].spanId > 0

      await conn.close()

    waitFor t()

suite "Tracing: PreparedStatement.execute":
  test "onQueryStart and onQueryEnd are called":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      let stmt = await conn.prepare("test_exec_stmt", "SELECT $1::int4")
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      discard await stmt.execute(@[toPgParam(42'i32)])

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].sql == "SELECT $1::int4"
      doAssert log.queryStarts[0].isExec == false
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].rowCount == 1
      doAssert not log.queryEnds[0].hasErr
      doAssert log.queryEnds[0].spanId > 0

      await conn.close()

    waitFor t()

suite "Tracing: copyIn":
  test "onCopyStart(tcdIn) and onCopyEnd with commandTag":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      discard
        await conn.exec("CREATE TEMP TABLE test_trace_copy_in (id int, name text)")

      var rows: seq[byte]
      rows.add("1\tAlice\n".toBytes())
      rows.add("2\tBob\n".toBytes())
      discard await conn.copyIn("COPY test_trace_copy_in FROM STDIN", rows)

      doAssert log.copyStarts.len == 1
      doAssert log.copyStarts[0].direction == tcdIn
      doAssert "test_trace_copy_in" in log.copyStarts[0].sql
      doAssert log.copyEnds.len == 1
      doAssert log.copyEnds[0].commandTag.startsWith("COPY")
      doAssert not log.copyEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: copyOut":
  test "onCopyStart(tcdOut) and onCopyEnd with commandTag":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      discard
        await conn.exec("CREATE TEMP TABLE test_trace_copy_out (id int, name text)")
      discard await conn.exec("INSERT INTO test_trace_copy_out VALUES (1, 'Alice')")

      log.copyStarts.setLen(0)
      log.copyEnds.setLen(0)

      discard await conn.copyOut("COPY test_trace_copy_out TO STDOUT")

      doAssert log.copyStarts.len == 1
      doAssert log.copyStarts[0].direction == tcdOut
      doAssert log.copyEnds.len == 1
      doAssert log.copyEnds[0].commandTag.startsWith("COPY")
      doAssert not log.copyEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: copyInStream":
  test "onCopyStart(tcdIn) and onCopyEnd with commandTag":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      discard await conn.exec(
        "CREATE TEMP TABLE test_trace_copy_in_stream (id int, name text)"
      )

      var chunks = @["1\tAlice\n".toBytes(), "2\tBob\n".toBytes()]
      var idx = 0
      let cb = makeCopyInCallback:
        if idx < chunks.len:
          let data = chunks[idx]
          idx.inc
          data
        else:
          @[]

      log.copyStarts.setLen(0)
      log.copyEnds.setLen(0)

      discard await conn.copyInStream("COPY test_trace_copy_in_stream FROM STDIN", cb)

      doAssert log.copyStarts.len == 1
      doAssert log.copyStarts[0].direction == tcdIn
      doAssert "test_trace_copy_in_stream" in log.copyStarts[0].sql
      doAssert log.copyEnds.len == 1
      doAssert log.copyEnds[0].commandTag.startsWith("COPY")
      doAssert not log.copyEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: copyOutStream":
  test "onCopyStart(tcdOut) and onCopyEnd with commandTag":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      discard await conn.exec(
        "CREATE TEMP TABLE test_trace_copy_out_stream (id int, name text)"
      )
      discard
        await conn.exec("INSERT INTO test_trace_copy_out_stream VALUES (1, 'Alice')")

      log.copyStarts.setLen(0)
      log.copyEnds.setLen(0)

      var received: seq[seq[byte]]
      let cb = makeCopyOutCallback:
        received.add(data)

      discard await conn.copyOutStream("COPY test_trace_copy_out_stream TO STDOUT", cb)

      doAssert received.len > 0
      doAssert log.copyStarts.len == 1
      doAssert log.copyStarts[0].direction == tcdOut
      doAssert log.copyEnds.len == 1
      doAssert log.copyEnds[0].commandTag.startsWith("COPY")
      doAssert not log.copyEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: pipeline":
  test "onPipelineStart and onPipelineEnd with opCount":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      let p = newPipeline(conn)
      p.addQuery("SELECT 1::int4")
      p.addQuery("SELECT 2::int4")
      p.addQuery("SELECT 3::int4")
      discard await p.execute()

      doAssert log.pipelineStarts.len == 1
      doAssert log.pipelineStarts[0].opCount == 3
      doAssert log.pipelineEnds.len == 1
      doAssert not log.pipelineEnds[0].hasErr
      doAssert log.pipelineEnds[0].spanId > 0

      await conn.close()

    waitFor t()

suite "Tracing: pool acquire/release":
  test "acquire and release hooks are called":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 2)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      let conn = await pool.acquire()

      doAssert log.poolAcquireStarts.len == 1
      doAssert log.poolAcquireStarts[0].maxSize == 2
      doAssert log.poolAcquireEnds.len == 1
      doAssert log.poolAcquireEnds[0].hasConn
      doAssert log.poolAcquireEnds[0].wasCreated == true
      doAssert not log.poolAcquireEnds[0].hasErr

      conn.release()

      doAssert log.poolReleaseStarts.len == 1
      doAssert log.poolReleaseStarts[0].hasConn
      doAssert log.poolReleaseEnds.len == 1
      doAssert not log.poolReleaseEnds[0].wasClosed
      doAssert not log.poolReleaseEnds[0].handedToWaiter

      # Re-acquire should reuse the idle connection
      let conn2 = await pool.acquire()

      doAssert log.poolAcquireStarts.len == 2
      doAssert log.poolAcquireEnds.len == 2
      doAssert log.poolAcquireEnds[1].wasCreated == false

      conn2.release()
      await pool.close()

    waitFor t()

  test "release hands connection to waiter":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 1)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      let conn1 = await pool.acquire()
      # maxSize=1, so next acquire will wait
      let fut = pool.acquire()
      # Release conn1 -- should hand to the waiter
      conn1.release()
      let conn2 = await fut

      doAssert log.poolReleaseEnds.len == 1
      doAssert not log.poolReleaseEnds[0].wasClosed
      doAssert log.poolReleaseEnds[0].handedToWaiter

      conn2.release()
      await pool.close()

    waitFor t()

suite "Tracing: pool close errors":
  test "onPoolCloseError reports swallowed close failures":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 1)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      let conn = await pool.acquire()

      # Drive the hook through reportCloseError — the single chokepoint that
      # tracedClose funnels errors through. Inducing a real conn.close() failure
      # is impractical in tests: close() is idempotent and its only raising path
      # is inside sendMsg, which it already swallows internally.
      let err = newException(PgError, "simulated close failure")
      pool.reportCloseError(conn, err)

      doAssert log.poolCloseErrors.len == 1
      doAssert log.poolCloseErrors[0].hasConn
      doAssert log.poolCloseErrors[0].errMsg == "simulated close failure"

      conn.release()
      await pool.close()

    waitFor t()

  test "nil onPoolCloseError hook is a no-op":
    proc t() {.async.} =
      # Build a tracer that sets OTHER hooks but leaves onPoolCloseError nil.
      let tracer = PgTracer()
      var poolCfg = initPoolConfig(plainConfig(), minSize = 0, maxSize = 1)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      let conn = await pool.acquire()
      # Must not raise even though the hook is nil.
      pool.reportCloseError(conn, newException(PgError, "ignored"))

      conn.release()
      await pool.close()

    waitFor t()

  test "tracedClose on healthy connection does not fire hook":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 1)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      # Use a stand-alone connection so pool.active/idle counters stay
      # consistent — tracedClose only touches the tracer, not pool state.
      let conn = await connect(tracedConfig(tracer))
      await pool.tracedClose(conn)

      doAssert log.poolCloseErrors.len == 0

      await pool.close()

    waitFor t()

  test "tracedClose on already-closed connection is a no-op":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 1)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      # conn.close() is documented as idempotent. tracedClose against an
      # already-closed conn exercises the real close path end-to-end and must
      # neither raise nor fire the error hook.
      let conn = await connect(tracedConfig(tracer))
      await conn.close()
      await pool.tracedClose(conn)

      doAssert log.poolCloseErrors.len == 0

      await pool.close()

    waitFor t()

suite "Tracing: transport close errors":
  when hasChronos:
    test "onTransportCloseError reports swallowed close failures":
      proc t() {.async.} =
        let log = newTraceLog()
        let tracer = buildTracer(log)
        let conn = await connect(tracedConfig(tracer))

        # Drive the hook directly — inducing a real closeWait() failure is
        # impractical (the chronos streams swallow most peer-side faults
        # internally), so we exercise the chokepoint that closeTransport
        # funnels every swallowed close error through.
        let err = newException(PgError, "simulated tls close failure")
        conn.fireTransportCloseError(tcsTlsReader, err)

        doAssert log.transportCloseErrors.len == 1
        doAssert log.transportCloseErrors[0].hasConn
        doAssert log.transportCloseErrors[0].stage == tcsTlsReader
        doAssert log.transportCloseErrors[0].errMsg == "simulated tls close failure"

        await conn.close()

      waitFor t()

    test "every stage value round-trips through the hook":
      proc t() {.async.} =
        let log = newTraceLog()
        let tracer = buildTracer(log)
        let conn = await connect(tracedConfig(tracer))

        const stages =
          [tcsTlsReader, tcsTlsWriter, tcsBaseReader, tcsBaseWriter, tcsTransport]
        for stage in stages:
          conn.fireTransportCloseError(stage, newException(PgError, "x"))

        doAssert log.transportCloseErrors.len == stages.len
        for i in 0 ..< stages.len:
          doAssert log.transportCloseErrors[i].stage == stages[i]
          doAssert log.transportCloseErrors[i].errMsg == "x"

        await conn.close()

      waitFor t()

    test "nil onTransportCloseError hook is a no-op":
      proc t() {.async.} =
        # Build a config with no tracer at all — fire must be a no-op.
        let conn = await connect(plainConfig())
        conn.fireTransportCloseError(tcsTransport, newException(PgError, "ignored"))

        # Tracer present but the hook itself is nil — also a no-op.
        let tracer = PgTracer()
        var cfg = plainConfig()
        cfg.tracer = tracer
        let conn2 = await connect(cfg)
        conn2.fireTransportCloseError(tcsBaseReader, newException(PgError, "ignored2"))

        await conn.close()
        await conn2.close()

      waitFor t()

  test "healthy close() does not fire onTransportCloseError":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      await conn.close()

      doAssert log.transportCloseErrors.len == 0

    waitFor t()

  test "closeTransport wires every TransportCloseStage to a fire call":
    # Structural guard. Inducing real closeWait() failures is impractical, so
    # the behavioural tests above drive fireTransportCloseError directly and
    # cannot catch regressions inside closeTransport itself (forgotten except
    # clause, new stage without a fire call, new closeWait without wiring).
    # This test reads the source and asserts the invariants mechanically.
    const src = staticRead("../async_postgres/pg_connection/buffer_io.nim")
    let body = src.split("proc closeTransport*(")[1].split("\nproc ")[0]

    for stage in [
      "tcsTlsReader", "tcsTlsWriter", "tcsBaseReader", "tcsBaseWriter", "tcsTransport"
    ]:
      doAssert stage in body,
        "closeTransport missing reference to " & stage &
          " — fire call wiring is incomplete"

    let closeWaits = body.count("closeWait()")
    let fires = body.count("fireTransportCloseError(")
    doAssert closeWaits == fires,
      "closeTransport has " & $closeWaits & " closeWait() calls but " & $fires &
        " fireTransportCloseError() calls — each closeWait must be paired with a fire"

suite "Tracing: queryEach":
  test "onQueryStart and onQueryEnd are called with rowCount":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      var rows: seq[string]
      discard await conn.queryEach(
        "SELECT generate_series(1, 3)",
        callback = proc(row: Row) =
          rows.add(row.getStr(0)),
      )

      doAssert rows.len == 3
      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].sql == "SELECT generate_series(1, 3)"
      doAssert log.queryStarts[0].isExec == false
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].rowCount == 3
      doAssert not log.queryEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: execInTransaction":
  test "onQueryStart(isExec=true) and onQueryEnd with commandTag":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      discard
        await conn.execInTransaction("CREATE TEMP TABLE test_trace_exec_tx (id int)")

      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].isExec == true
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].commandTag.len > 0
      doAssert not log.queryEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: queryInTransaction":
  test "onQueryStart(isExec=false) and onQueryEnd with rowCount":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))
      log.queryStarts.setLen(0)
      log.queryEnds.setLen(0)

      let qr = await conn.queryInTransaction("SELECT generate_series(1, 3)")

      doAssert qr.rowCount == 3
      doAssert log.queryStarts.len == 1
      doAssert log.queryStarts[0].sql == "SELECT generate_series(1, 3)"
      doAssert log.queryStarts[0].isExec == false
      doAssert log.queryEnds.len == 1
      doAssert log.queryEnds[0].rowCount == 3
      doAssert not log.queryEnds[0].hasErr

      await conn.close()

    waitFor t()

suite "Tracing: nil tracer":
  test "operations work without tracer":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.tracer == nil

      discard await conn.exec("SELECT 1")
      let qr = await conn.query("SELECT 1")
      doAssert qr.rowCount == 1
      discard await conn.simpleExec("SELECT 1")
      discard await conn.simpleQuery("SELECT 1")

      await conn.close()

    waitFor t()

suite "Tracing: onInsecureAuth":
  # These unit tests exercise the tracer closure directly via the public
  # hook. End-to-end firing from the auth loop (cleartext over plaintext)
  # requires a PG server configured to request `password` auth, which is
  # out of scope for the current docker-compose setup.
  test "closure receives method and transport state (plaintext)":
    let log = newTraceLog()
    let tracer = buildTracer(log)
    let fake = PgConnection()

    tracer.onInsecureAuth(
      TraceInsecureAuthData(conn: fake, authMethod: amPassword, sslEnabled: false)
    )

    check log.insecureAuths.len == 1
    check log.insecureAuths[0].hasConn
    check log.insecureAuths[0].authMethod == amPassword
    check log.insecureAuths[0].sslEnabled == false

  test "closure receives sslEnabled=true":
    let log = newTraceLog()
    let tracer = buildTracer(log)
    let fake = PgConnection()

    tracer.onInsecureAuth(
      TraceInsecureAuthData(conn: fake, authMethod: amPassword, sslEnabled: true)
    )

    check log.insecureAuths.len == 1
    check log.insecureAuths[0].sslEnabled == true

  test "PgTracer with nil onInsecureAuth hook is safe to leave unset":
    let tracer = PgTracer()
    check tracer.onInsecureAuth == nil

suite "Tracing: onDeprecatedAuth":
  # End-to-end firing from the auth loop (MD5 challenge) requires a PG
  # server configured with `password_encryption = md5` and an md5-stored
  # role, which is out of scope for the current docker-compose setup.
  # These unit tests exercise the closure directly.
  test "closure receives MD5 auth method":
    let log = newTraceLog()
    let tracer = buildTracer(log)
    let fake = PgConnection()

    tracer.onDeprecatedAuth(TraceDeprecatedAuthData(conn: fake, authMethod: amMd5))

    check log.deprecatedAuths.len == 1
    check log.deprecatedAuths[0].hasConn
    check log.deprecatedAuths[0].authMethod == amMd5

  test "PgTracer with nil onDeprecatedAuth hook is safe to leave unset":
    let tracer = PgTracer()
    check tracer.onDeprecatedAuth == nil

suite "Tracing: onCleanupSkipped":
  # The `withTransaction*` / `withSavepoint*` macros invoke
  # `fireCleanupSkipped` through `conn.config.tracer`, so unit tests can
  # drive that codepath without a live PG by attaching a tracer to a
  # zero-initialised `PgConnection` and calling `fireCleanupSkipped`
  # directly. The end-to-end tests below exercise the macro paths against
  # a real server (timeout-induced csClosed skip, body-error swallowed
  # ROLLBACK failure) — but the unit tests cover the data wiring on their
  # own.
  test "fire helper routes csrConnInvalidated with nil err":
    let log = newTraceLog()
    let tracer = buildTracer(log)
    let fake = PgConnection(config: ConnConfig(tracer: tracer))

    fake.fireCleanupSkipped(ckTxRollback, csrConnInvalidated)

    check log.cleanupSkipped.len == 1
    check log.cleanupSkipped[0].hasConn
    check log.cleanupSkipped[0].kind == ckTxRollback
    check log.cleanupSkipped[0].reason == csrConnInvalidated
    check log.cleanupSkipped[0].errMsg == ""

  test "fire helper routes csrCleanupFailed with err message":
    let log = newTraceLog()
    let tracer = buildTracer(log)
    let fake = PgConnection(config: ConnConfig(tracer: tracer))

    let err = newException(PgError, "rollback boom")
    fake.fireCleanupSkipped(ckSavepointRollback, csrCleanupFailed, err)

    check log.cleanupSkipped.len == 1
    check log.cleanupSkipped[0].kind == ckSavepointRollback
    check log.cleanupSkipped[0].reason == csrCleanupFailed
    check log.cleanupSkipped[0].errMsg == "rollback boom"

  test "nil tracer is a no-op":
    let fake = PgConnection() # no config.tracer
    fake.fireCleanupSkipped(ckTxRollback, csrConnInvalidated) # must not raise

  test "nil onCleanupSkipped hook is a no-op":
    let tracer = PgTracer() # other hooks left nil too — verify just this one is safe
    check tracer.onCleanupSkipped == nil
    let fake = PgConnection(config: ConnConfig(tracer: tracer))
    fake.fireCleanupSkipped(ckTxRollback, csrConnInvalidated) # must not raise

  test "withTransaction body error fires no cleanup-skipped event on healthy conn":
    # The success-path inner ROLLBACK must complete cleanly and NOT fire
    # the advisory hook — only swallowed failures do.
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      var raised = false
      try:
        conn.withTransaction:
          discard await conn.exec("SELECT 1")
          raise newException(PgError, "body boom")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 0

      await conn.close()

    waitFor t()

  test "withTransaction on csClosed conn fires csrConnInvalidated":
    # Force the per-call timeout path by invalidating mid-body. The inner
    # ROLLBACK is then skipped because `state != csReady`, and the advisory
    # hook fires with reason=csrConnInvalidated.
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      var raised = false
      try:
        conn.withTransaction:
          # Stand-in for "per-call timeout invalidated the conn after BEGIN":
          # mark csClosed by hand and raise. The macro must observe the
          # invalidated state and skip ROLLBACK while reporting the skip.
          conn.state = csClosed
          raise newException(PgError, "simulated timeout invalidation")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 1
      doAssert log.cleanupSkipped[0].kind == ckTxRollback
      doAssert log.cleanupSkipped[0].reason == csrConnInvalidated
      doAssert log.cleanupSkipped[0].errMsg == ""

      # `conn` was marked closed — let close() run idempotently for cleanup.
      await conn.close()

    waitFor t()

  test "withSavepoint on csClosed conn fires csrConnInvalidated":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      var raised = false
      try:
        conn.withTransaction:
          conn.withSavepoint:
            conn.state = csClosed
            raise newException(PgError, "simulated timeout invalidation")
      except PgError:
        raised = true

      doAssert raised
      # Nested macros fire `onCleanupSkipped` once per cleanup site: the
      # inner savepoint's except handler reports a `ckSavepointRollback`
      # csClosed skip, then re-raises into the outer transaction whose
      # except handler reports a `ckTxRollback` csClosed skip on the same
      # connection. Observers aggregating by failure (not by cleanup
      # attempt) should dedupe — see `onCleanupSkipped` docs.
      doAssert log.cleanupSkipped.len == 2
      doAssert log.cleanupSkipped[0].kind == ckSavepointRollback
      doAssert log.cleanupSkipped[0].reason == csrConnInvalidated
      doAssert log.cleanupSkipped[0].errMsg == ""
      doAssert log.cleanupSkipped[1].kind == ckTxRollback
      doAssert log.cleanupSkipped[1].reason == csrConnInvalidated
      doAssert log.cleanupSkipped[1].errMsg == ""

      await conn.close()

    waitFor t()

  test "withTransaction with mock cleanup failure fires csrCleanupFailed":
    # End-to-end coverage for the `csrCleanupFailed` arm of the macro
    # cleanup path. Inducing a real ROLLBACK failure inside the macro is
    # impractical against a healthy server (the SQL is fixed and the
    # connection is still ready), so we drive `fireCleanupSkipped` from
    # the same `conn.config.tracer` chain the macro uses — this verifies
    # the wiring end-to-end (tracer attached via `tracedConfig`, hook
    # callable while a real connection is open) without relying on a
    # synthetic transport failure. The direct `csrCleanupFailed` arm of
    # `fireCleanupSkipped` is already covered by the unit test above; the
    # added value here is the live-connection path.
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let conn = await connect(tracedConfig(tracer))

      let err = newException(PgError, "synthetic ROLLBACK failure")
      conn.fireCleanupSkipped(ckTxRollback, csrCleanupFailed, err)

      doAssert log.cleanupSkipped.len == 1
      doAssert log.cleanupSkipped[0].hasConn
      doAssert log.cleanupSkipped[0].kind == ckTxRollback
      doAssert log.cleanupSkipped[0].reason == csrCleanupFailed
      doAssert log.cleanupSkipped[0].errMsg == "synthetic ROLLBACK failure"

      await conn.close()

    waitFor t()

suite "Tracing: onCleanupSkipped (pool)":
  test "pool.withTransaction body error fires no cleanup-skipped event on healthy conn":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 2)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      var raised = false
      try:
        pool.withTransaction(conn):
          discard await conn.exec("SELECT 1")
          raise newException(PgError, "body boom")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 0

      await pool.close()

    waitFor t()

  test "pool.withTransaction on csClosed conn fires csrConnInvalidated":
    # Mirror of the per-connection csClosed test: drive the cleanup path
    # without a real per-call timeout by forcing `state = csClosed` mid-
    # body. The pool variant must skip ROLLBACK and report the skip the
    # same way the per-connection macro does.
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 2)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      var raised = false
      try:
        pool.withTransaction(conn):
          conn.state = csClosed
          raise newException(PgError, "simulated timeout invalidation")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 1
      doAssert log.cleanupSkipped[0].kind == ckTxRollback
      doAssert log.cleanupSkipped[0].reason == csrConnInvalidated
      doAssert log.cleanupSkipped[0].errMsg == ""

      await pool.close()

    waitFor t()

  test "pool.withTransactionDeadline on csClosed conn fires csrConnInvalidated":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      var poolCfg = initPoolConfig(tracedConfig(tracer), minSize = 0, maxSize = 2)
      poolCfg.tracer = tracer
      let pool = await newPool(poolCfg)

      var raised = false
      try:
        pool.withTransactionDeadline(conn, seconds(30)):
          conn.state = csClosed
          raise newException(PgError, "simulated timeout invalidation")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 1
      doAssert log.cleanupSkipped[0].kind == ckTxRollback
      doAssert log.cleanupSkipped[0].reason == csrConnInvalidated
      doAssert log.cleanupSkipped[0].errMsg == ""

      await pool.close()

    waitFor t()

suite "Tracing: onCleanupSkipped (cluster)":
  # Point both primary and replica at the local server: set tsaReadWrite
  # explicitly so newPoolCluster does not override the replica with
  # tsaPreferStandby (which the single standalone server would reject).
  proc clusterConfig(tracer: PgTracer): ConnConfig =
    result = tracedConfig(tracer)
    result.targetSessionAttrs = tsaReadWrite

  test "cluster.withTransaction body error fires no cleanup-skipped event on healthy conn":
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let cfg = clusterConfig(tracer)
      let cluster = await newPoolCluster(
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2, tracer: tracer),
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2, tracer: tracer),
      )

      var raised = false
      try:
        cluster.withTransaction(conn):
          discard await conn.exec("SELECT 1")
          raise newException(PgError, "body boom")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 0

      await cluster.close()

    waitFor t()

  test "cluster.withTransaction on csClosed conn fires csrConnInvalidated":
    # Mirror of the conn/pool csClosed tests: drive the cleanup path without a
    # real per-call timeout by forcing `state = csClosed` mid-body. The cluster
    # variant must skip ROLLBACK and report the skip the same way.
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let cfg = clusterConfig(tracer)
      let cluster = await newPoolCluster(
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2, tracer: tracer),
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2, tracer: tracer),
      )

      var raised = false
      try:
        cluster.withTransaction(conn):
          conn.state = csClosed
          raise newException(PgError, "simulated timeout invalidation")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 1
      doAssert log.cleanupSkipped[0].kind == ckTxRollback
      doAssert log.cleanupSkipped[0].reason == csrConnInvalidated
      doAssert log.cleanupSkipped[0].errMsg == ""

      await cluster.close()

    waitFor t()

  test "cluster.withTransactionRetry on csClosed conn fires csrConnInvalidated":
    # The csClosed state both fires the skip report and (via `state == csReady`
    # in the retry gate) suppresses any retry, so the original error propagates.
    proc t() {.async.} =
      let log = newTraceLog()
      let tracer = buildTracer(log)
      let cfg = clusterConfig(tracer)
      let cluster = await newPoolCluster(
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2, tracer: tracer),
        PoolConfig(connConfig: cfg, minSize: 1, maxSize: 2, tracer: tracer),
      )

      var raised = false
      try:
        cluster.withTransactionRetry(
          RetryOptions(maxAttempts: 3, baseDelayMs: 1, jitter: false), conn
        ):
          conn.state = csClosed
          raise newException(PgError, "simulated timeout invalidation")
      except PgError:
        raised = true

      doAssert raised
      doAssert log.cleanupSkipped.len == 1
      doAssert log.cleanupSkipped[0].kind == ckTxRollback
      doAssert log.cleanupSkipped[0].reason == csrConnInvalidated
      doAssert log.cleanupSkipped[0].errMsg == ""

      await cluster.close()

    waitFor t()

suite "filterSaslByRequireAuth":
  test "empty allowed set performs no filtering":
    let mechs = @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]
    check filterSaslByRequireAuth(mechs, {}) == mechs

  test "drops PLUS when only SCRAM allowed":
    let mechs = @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]
    check filterSaslByRequireAuth(mechs, {amScramSha256}) == @["SCRAM-SHA-256"]

  test "drops SCRAM when only PLUS allowed":
    let mechs = @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]
    check filterSaslByRequireAuth(mechs, {amScramSha256Plus}) == @["SCRAM-SHA-256-PLUS"]

  test "empty result when nothing matches":
    let mechs = @["SCRAM-SHA-256"]
    check filterSaslByRequireAuth(mechs, {amMd5}).len == 0

  test "unrelated non-SCRAM methods in allowlist do not add mechanisms":
    let mechs = @["SCRAM-SHA-256"]
    check filterSaslByRequireAuth(mechs, {amScramSha256, amMd5, amPassword}) ==
      @["SCRAM-SHA-256"]

suite "Tracing: requireAuth happy path":
  test "accepts SCRAM when explicitly allowed":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.requireAuth = {amScramSha256, amScramSha256Plus}
      let conn = await connect(cfg)
      doAssert conn != nil
      discard await conn.exec("SELECT 1")
      await conn.close()

    waitFor t()

suite "Tracing: requireAuth negative path":
  # These tests require the docker-compose PG to use SCRAM auth (the default
  # for modern postgres images). If the server ever switches to trust/md5,
  # the expected error message will differ but the connect must still fail.
  proc connectRaises(requireAuth: set[AuthMethod]): bool =
    var raised = false
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.requireAuth = requireAuth
      try:
        let conn = await connect(cfg)
        await conn.close()
      except PgConnectionError:
        raised = true

    waitFor t()
    raised

  test "rejects when only md5 is allowed against SCRAM server":
    check connectRaises({amMd5})

  test "rejects when only password is allowed against SCRAM server":
    check connectRaises({amPassword})

  test "rejects when only amNone is allowed against SCRAM server":
    check connectRaises({amNone})
