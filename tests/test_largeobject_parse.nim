## Unit tests for the Large Object result parsers.
##
## The parsers take text returned by server functions; a hostile or buggy
## server can send values outside the column's nominal range. Those must
## surface as `PgTypeError` (catchable via `except PgError`), never as an
## uncatchable `RangeDefect`.
##
## The wire-level suite below drives the public API (`loOpen`/`loWrite`/
## `loCreate`) against a scripted mock server, so the call sites that consume
## these parsers are covered too — not just the private helpers.

import std/[unittest, strutils, sequtils, importutils]

import ../async_postgres/[async_backend, pg_connection]
import ../async_postgres/pg_errors
import ../async_postgres/pg_largeobject {.all.}

import mock_pg_server

privateAccess(LargeObject)

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc drainUntilSync(client: MockClient) {.async.} =
  ## Consume one extended-query round trip (Parse/Describe/Bind/Execute/Sync).
  while true:
    let (msgType, _) = await drainFrontendMessage(client)
    if msgType == 'S':
      break

proc buildScalarResult(value, tag: string): seq[byte] =
  ## Extended-protocol reply carrying a single text-format int4 row.
  result.add(buildBackendMsg('1', @[])) # ParseComplete
  result.add(buildBackendMsg('2', @[])) # BindComplete
  result.add(buildRowDescriptionFields(@[("result", 23'i32, 4'i16)]))
  result.add(buildDataRowText([value]))
  result.add(buildCommandComplete(tag))
  result.add(buildReadyForQuery('I'))

proc serveReplies(ms: MockServer, replies: seq[seq[byte]]) {.async.} =
  ## Answer each extended query with the corresponding raw reply.
  let client = await acceptAndReady(ms)
  for reply in replies:
    await drainUntilSync(client)
    await sendBytes(client, reply)
  await closeClient(client)

proc buildLoReadReply(
    row: seq[byte], fields = @[("loread", 17'i32, -1'i16)]
): seq[byte] =
  ## Extended-protocol reply to `loread` with `row` as its DataRows (none if empty).
  result.add(buildBackendMsg('1', @[])) # ParseComplete
  result.add(buildBackendMsg('2', @[])) # BindComplete
  result.add(buildRowDescriptionFields(fields))
  result.add(row)
  result.add(buildCommandComplete(if row.len == 0: "SELECT 0" else: "SELECT 1"))
  result.add(buildReadyForQuery('I'))

proc serveScalarResults(ms: MockServer, values: seq[string]): Future[void] =
  ## Answer each extended query with the corresponding scalar text.
  serveReplies(ms, values.mapIt(buildScalarResult(it, "SELECT 1")))

suite "Large Object result parsers":
  test "parseLoInt32 accepts the full int32 range":
    check parseLoInt32("0", "lo_open") == 0'i32
    check parseLoInt32("2147483647", "lo_open") == int32.high
    check parseLoInt32("-2147483648", "lowrite") == int32.low

  test "parseLoInt32 rejects out-of-range text as PgTypeError":
    expect PgTypeError:
      discard parseLoInt32("2147483648", "lo_open")
    expect PgTypeError:
      discard parseLoInt32("-2147483649", "lowrite")
    expect PgTypeError:
      discard parseLoInt32("99999999999999999999999999", "lo_open")

  test "parseLoOid accepts the full uint32 range":
    check parseLoOid("0", "lo_create") == Oid(0)
    check parseLoOid("4294967295", "lo_create") == high(Oid)

  test "parseLoOid rejects out-of-range text as PgTypeError":
    expect PgTypeError:
      discard parseLoOid("4294967296", "lo_create")
    expect PgTypeError:
      discard parseLoOid("99999999999999999999", "lo_create")
    expect PgTypeError:
      discard parseLoOid("-1", "lo_create")
    expect PgTypeError:
      discard parseLoOid("abc", "lo_create")

  test "parseLoOid separates a range error from a syntax error":
    # Also the observable half of parsing into int64: on a 32-bit build an int
    # would fold every OID above 2^31 into the syntax-error branch.
    var msg = ""
    try:
      discard parseLoOid("99999999999999999999", "lo_create")
    except PgTypeError as e:
      msg = e.msg
    check "outside uint32 range" in msg
    try:
      discard parseLoOid("abc", "lo_create")
    except PgTypeError as e:
      msg = e.msg
    check "non-numeric OID" in msg

  test "hostile values never escape as RangeDefect":
    var defect: ref Defect = nil
    try:
      discard parseLoOid("4294967296", "lo_create")
    except PgTypeError:
      discard
    except Defect as e:
      defect = e
    check defect == nil

    try:
      discard parseLoInt32("2147483648", "lo_open")
    except PgTypeError:
      discard
    except Defect as e:
      defect = e
    check defect == nil

suite "Large Object parsers: hostile server text via the public API":
  test "loOpen surfaces an out-of-range fd text as PgTypeError":
    proc t() {.async.} =
      let ms = startMockServer()
      let serverFut = serveScalarResults(ms, @["2147483648"])
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      var defect: ref Defect = nil
      try:
        discard await conn.loOpen(Oid(1), INV_READ)
      except PgTypeError:
        raised = true
      except Defect as e:
        defect = e
      doAssert defect == nil, "hostile lo_open fd text escaped as " & $defect.name
      doAssert raised, "lo_open must reject the out-of-range fd text"
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor t()

  test "loWrite surfaces an out-of-range byte count as PgTypeError":
    proc t() {.async.} =
      let ms = startMockServer()
      let serverFut = serveScalarResults(ms, @["3", "2147483648"])
      let conn = await connect(mockConfig(ms.port))
      let lo = await conn.loOpen(Oid(1), INV_READ)
      doAssert lo.fd == 3, "loOpen must still parse a valid fd"
      var raised = false
      var defect: ref Defect = nil
      try:
        discard await lo.loWrite(@[byte 0x68, byte 0x69])
      except PgTypeError:
        raised = true
      except Defect as e:
        defect = e
      doAssert defect == nil, "hostile lowrite text escaped as " & $defect.name
      doAssert raised, "lo_write must reject the out-of-range byte count"
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor t()

  test "loCreate surfaces an out-of-range OID text as PgTypeError":
    proc t() {.async.} =
      let ms = startMockServer()
      let serverFut = serveScalarResults(ms, @["4294967296"])
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      var defect: ref Defect = nil
      try:
        discard await conn.loCreate()
      except PgTypeError:
        raised = true
      except Defect as e:
        defect = e
      doAssert defect == nil, "hostile lo_create OID text escaped as " & $defect.name
      doAssert raised, "lo_create must reject the out-of-range OID text"
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await closeServer(ms)

    waitFor t()

suite "Large Object loRead: an answer without a value is not EOF":
  proc loReadError(reply: seq[byte]): Future[ref PgError] {.async.} =
    let ms = startMockServer()
    let serverFut = serveReplies(ms, @[reply])
    let conn = await connect(mockConfig(ms.port))
    let lo = LargeObject(conn: conn, fd: 0, oid: Oid(1))
    try:
      discard await lo.loRead(16)
    except PgError as e:
      result = e
    try:
      await conn.close()
    except CatchableError:
      discard
    await serverFut
    await closeServer(ms)

  test "no rows raises PgNoRowsError":
    let err = waitFor loReadError(buildLoReadReply(@[]))
    check err != nil
    check err of PgNoRowsError

  test "no columns raises PgTypeError":
    let err = waitFor loReadError(buildLoReadReply(buildDataRowText([]), @[]))
    check err != nil
    check err of PgTypeError
    check "no columns" in err.msg

  test "a NULL value raises PgNullError":
    let err = waitFor loReadError(buildLoReadReply(buildDataRowOpt([""], [true])))
    check err != nil
    check err of PgNullError

suite "Large Object precondition errors (no server)":
  test "loRead rejects negative length with ValueError":
    let lo = LargeObject(conn: nil, fd: 1, oid: Oid(1))
    expect ValueError:
      discard waitFor lo.loRead(-1)

  test "loSeek rejects invalid whence with ValueError":
    let lo = LargeObject(conn: nil, fd: 1, oid: Oid(1))
    expect ValueError:
      discard waitFor lo.loSeek(0, whence = 99'i32)
