import std/[unittest, strutils]

import ../async_postgres/async_backend

import ../async_postgres/[pg_client, pg_types, pg_protocol]
import ../async_postgres/pg_pool {.all.}
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

      pool.release(conn)

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

      pool.release(conn2)
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
      pool.release(conn1)
      let conn2 = await fut

      doAssert log.poolReleaseEnds.len == 1
      doAssert not log.poolReleaseEnds[0].wasClosed
      doAssert log.poolReleaseEnds[0].handedToWaiter

      pool.release(conn2)
      await pool.close()

    waitFor t()

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
