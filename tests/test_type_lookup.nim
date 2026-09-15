## Dedicated tests for `lookupTypeOids`, focused on hostile catalog rows.
##
## The helper runs a simple-protocol query and parses the OID columns itself.
## A hostile or buggy server can send text outside int32 range there; such
## rows must be skipped as malformed (the documented behavior for bad rows)
## instead of raising an uncatchable RangeDefect.

import std/[unittest, tables]

import ../async_postgres/[async_backend, pg_connection]
import ../async_postgres/pg_connection/type_lookup

import mock_pg_server

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

proc oidRowDescription(): seq[byte] =
  buildRowDescriptionFields(
    @[("n", 25'i32, -1'i16), ("oid", 23'i32, 4'i16), ("typarray", 23'i32, 4'i16)]
  )

suite "lookupTypeOids: hostile OID text":
  test "out-of-range and non-numeric rows are skipped, not fatal":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serverHandler() {.async.} =
        let st = await acceptAndReady(ms)
        # lookupTypeOids uses the simple protocol; answer its query with a
        # scripted catalog result mixing valid and malformed rows.
        let (msgType, _) = await drainFrontendMessage(st)
        doAssert msgType == 'Q', "lookupTypeOids must use the simple protocol"
        var resp: seq[byte]
        resp.add(oidRowDescription())
        resp.add(buildDataRowText(["int4", "23", "1007"]))
        resp.add(buildDataRowText(["big_oid", "9999999999", "0"]))
        resp.add(buildDataRowText(["big_array", "42", "9999999999"]))
        resp.add(buildDataRowText(["huge_oid", "999999999999999999999999", "0"]))
        resp.add(buildDataRowText(["non_numeric", "abc", "0"]))
        # A negative int4 is the valid textual form of a large wrapped OID.
        resp.add(buildDataRowText(["negative_oid", "-1", "0"]))
        resp.add(buildCommandComplete("SELECT 6"))
        resp.add(buildReadyForQuery('I'))
        await sendBytes(st, resp)
        await closeClient(st)

      let serverFut = serverHandler()
      let conn = await connect(mockConfig(ms.port))

      var defect: ref Defect = nil
      var oids: Table[string, TypeOidInfo]
      try:
        oids = await conn.lookupTypeOids(
          @["int4", "big_oid", "big_array", "huge_oid", "non_numeric", "negative_oid"]
        )
      except Defect as e:
        defect = e

      doAssert defect == nil, "hostile OID text escaped as " & $defect.name
      if defect == nil:
        # int4, the hostile-typarray row (valid OID kept, arrayOid falls back
        # to 0), and the valid negative int4 row are kept; the out-of-range and
        # non-numeric OID rows are skipped.
        doAssert oids.len == 3, "expected 3 rows, got " & $oids.len
        doAssert oids["int4"].oid == 23'i32
        doAssert oids["int4"].arrayOid == 1007'i32
        doAssert oids["big_array"].oid == 42'i32
        doAssert oids["big_array"].arrayOid == 0'i32
        doAssert oids["negative_oid"].oid == -1'i32
        doAssert oids["negative_oid"].arrayOid == 0'i32
        doAssert not oids.hasKey("big_oid")
        doAssert not oids.hasKey("huge_oid")
        doAssert not oids.hasKey("non_numeric")

      await conn.close()
      await serverFut
      await closeServer(ms)

    waitFor t()
