## Unit tests for `pg_connection/simple_query`.
##
## PG-less pure part plus a mock-server round trip for
## `simpleExec` / `simpleQuery` / `ping`.

import std/[unittest, strutils, importutils]

import ../async_postgres/[async_backend, pg_connection, pg_protocol]
import ../async_postgres/pg_connection/simple_query {.all.}
import ../async_postgres/pg_connection/types {.all.}

import mock_pg_server

privateAccess(PgConnection)

proc mockConfig(port: int): ConnConfig =
  ConnConfig(
    host: "127.0.0.1", port: port, user: "test", database: "test", sslMode: sslDisable
  )

suite "simple_query: quoting":
  test "quoteIdentifier doubles embedded quotes":
    check quoteIdentifier("abc") == "\"abc\""
    check quoteIdentifier("a\"b") == "\"a\"\"b\""

  test "quoteIdentifier rejects NUL":
    expect ValueError:
      discard quoteIdentifier("a\0b")

  test "quoteLiteral handles quotes and backslashes":
    check quoteLiteral("o'clock") == "'o''clock'"
    check quoteLiteral("a\\b").startsWith(" E'")
    check quoteLiteral("plain") == "'plain'"

  test "quoteLiteral rejects NUL":
    expect ValueError:
      discard quoteLiteral("a\0b")

suite "simple_query: QueryResult helpers":
  test "len/columnIndex on an empty result":
    let qr = QueryResult()
    check qr.len == 0
    expect PgTypeError:
      discard qr.columnIndex("missing")

  test "bytesToString round-trips":
    check bytesToString(@[byte('a'), byte('b')]) == "ab"
    check bytesToString(@[]) == ""

suite "simple_query: state guards":
  test "checkReady on a ready connection passes":
    let conn = newPgConnection("127.0.0.1", 5432, mockConfig(5432))
    conn.state = csReady
    conn.checkReady()

  test "checkTxIdle rejects an open transaction":
    let conn = newPgConnection("127.0.0.1", 5432, mockConfig(5432))
    conn.txStatus = tsInTransaction
    expect PgStateError:
      conn.checkTxIdle()
    conn.txStatus = tsIdle
    conn.checkTxIdle()

suite "simple_query: mock round trip":
  test "simpleExec and simpleQuery and ping succeed":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        # simpleExec: one Query -> CommandComplete + ReadyForQuery.
        discard await drainFrontendMessage(client)
        await client.sendBytes(
          buildCommandComplete("SELECT 1") & buildReadyForQuery('I')
        )
        # simpleQuery: one Query -> RowDescription + DataRow + CommandComplete + ReadyForQuery.
        discard await drainFrontendMessage(client)
        await client.sendBytes(
          buildRowDescription("v") & buildDataRow("42") &
            buildCommandComplete("SELECT 1") & buildReadyForQuery('I')
        )
        # ping: SELECT 1 via simpleQuery.
        discard await drainFrontendMessage(client)
        await client.sendBytes(
          buildRowDescription("?column?") & buildDataRow("1") &
            buildCommandComplete("SELECT 1") & buildReadyForQuery('I')
        )
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      let tag = await conn.simpleExec("SELECT 1")
      doAssert tag.commandTag == "SELECT 1"
      let qrs = await conn.simpleQuery("SELECT 42")
      doAssert qrs.len == 1
      doAssert qrs[0].len == 1
      await conn.ping()
      doAssert conn.state == csReady
      await conn.close()
      await serverFut
      await ms.closeServer()

    waitFor t()

  test "simpleExec surfaces a server error":
    proc t() {.async.} =
      let ms = startMockServer()
      proc serve() {.async.} =
        let client = await acceptAndReady(ms)
        discard await drainFrontendMessage(client)
        await client.sendBytes(
          buildErrorResponse("42601", "syntax error") & buildReadyForQuery('E')
        )
        discard await drainFrontendMessage(client) # Terminate
        await closeClient(client)

      let serverFut = serve()
      let conn = await connect(mockConfig(ms.port))
      var raised = false
      try:
        discard await conn.simpleExec("BAD SQL")
      except PgQueryError as e:
        raised = true
        doAssert e.sqlState == "42601"
      doAssert raised
      try:
        await conn.close()
      except CatchableError:
        discard
      await serverFut
      await ms.closeServer()

    waitFor t()
