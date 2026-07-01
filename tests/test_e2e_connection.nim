import std/[unittest, options, tables, math, importutils, net]

import
  ../async_postgres/[async_backend, pg_protocol, pg_types, pg_client, pg_connection]

when hasAsyncDispatch:
  import std/strutils

import e2e_common

privateAccess(PgConnection)

suite "E2E: Basic Connection":
  test "plain connection and close":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == false
      await conn.close()
      doAssert conn.state == csClosed

    waitFor t()

  test "connect with DSN string":
    proc t() {.async.} =
      let dsn =
        "postgresql://" & PgUser & ":" & PgPassword & "@" & PgHost & ":" & $PgPort & "/" &
        PgDatabase & "?sslmode=disable"
      let conn = await connect(dsn)
      doAssert conn.state == csReady
      let res = await conn.simpleQuery("SELECT 1")
      doAssert res[0].rows[0][0].get().toString() == "1"
      await conn.close()
      doAssert conn.state == csClosed

    waitFor t()

  test "connect with keyword=value DSN string":
    proc t() {.async.} =
      let dsn =
        "host=" & PgHost & " port=" & $PgPort & " user=" & PgUser & " password=" &
        PgPassword & " dbname=" & PgDatabase & " sslmode=disable"
      let conn = await connect(dsn)
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "server parameters available":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.serverParams.hasKey("server_version")
      doAssert conn.serverParams.hasKey("server_encoding")
      await conn.close()

    waitFor t()

suite "E2E: ConnConfig Options":
  test "applicationName is sent to server":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.applicationName = "chronos-pg-test"
      let conn = await connect(cfg)
      let res = await conn.simpleQuery("SHOW application_name")
      doAssert res[0].rows[0][0].get().toString() == "chronos-pg-test"
      await conn.close()

    waitFor t()

  test "extraParams are sent to server":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.extraParams = @[("application_name", "from-extra")]
      let conn = await connect(cfg)
      let res = await conn.simpleQuery("SHOW application_name")
      doAssert res[0].rows[0][0].get().toString() == "from-extra"
      await conn.close()

    waitFor t()

  test "connectTimeout raises on unreachable host":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.host = "192.0.2.1" # TEST-NET, non-routable
      cfg.connectTimeout = milliseconds(200)
      var raised = false
      try:
        let conn = await connect(cfg)
        await conn.close()
      except AsyncTimeoutError:
        raised = true
      doAssert raised

    waitFor t()

  test "connectTimeout does not interfere with normal connection":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.connectTimeout = seconds(10)
      let conn = await connect(cfg)
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

suite "E2E: SSL Connection":
  test "sslRequire connects with SSL":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslRequire))
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslPrefer connects with SSL when server supports it":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslPrefer))
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslDisable connects without SSL":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslDisable))
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == false
      await conn.close()

    waitFor t()

  test "sslAllow connects without SSL when server accepts plaintext":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslAllow))
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == false
      await conn.close()

    waitFor t()

  test "query over SSL connection":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslRequire))
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

suite "E2E: SSL Verification":
  test "sslVerifyCa connects with CA verification":
    proc t() {.async.} =
      let conn = await connect(
        ConnConfig(
          host: PgHost,
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyCa,
          sslRootCert: loadCaCert(),
        )
      )
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslVerifyFull connects with full verification":
    proc t() {.async.} =
      # Use localhost (matches SAN DNS:localhost in server cert)
      let conn = await connect(
        ConnConfig(
          host: "localhost",
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyFull,
          sslRootCert: loadCaCert(),
        )
      )
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      await conn.close()

    waitFor t()

  test "sslVerifyCa fails with wrong CA":
    proc t() {.async.} =
      var raised = false
      try:
        let conn = await connect(
          ConnConfig(
            host: PgHost,
            port: PgPort,
            user: PgUser,
            password: PgPassword,
            database: PgDatabase,
            sslMode: sslVerifyCa,
            sslRootCert: loadWrongCaCert(),
          )
        )
        await conn.close()
      except CatchableError:
        raised = true
      doAssert raised

    waitFor t()

  test "sslVerifyFull fails with wrong CA":
    proc t() {.async.} =
      var raised = false
      try:
        let conn = await connect(
          ConnConfig(
            host: "localhost",
            port: PgPort,
            user: PgUser,
            password: PgPassword,
            database: PgDatabase,
            sslMode: sslVerifyFull,
            sslRootCert: loadWrongCaCert(),
          )
        )
        await conn.close()
      except CatchableError:
        raised = true
      doAssert raised

    waitFor t()

  test "query over sslVerifyCa connection":
    proc t() {.async.} =
      let conn = await connect(
        ConnConfig(
          host: PgHost,
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyCa,
          sslRootCert: loadCaCert(),
        )
      )
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

  test "query over sslVerifyFull connection":
    proc t() {.async.} =
      let conn = await connect(
        ConnConfig(
          host: "localhost",
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyFull,
          sslRootCert: loadCaCert(),
        )
      )
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

suite "E2E: Authentication":
  test "valid credentials succeed":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.state == csReady
      await conn.close()

    waitFor t()

  test "wrong password raises PgError":
    proc t() {.async.} =
      let badConfig = ConnConfig(
        host: PgHost,
        port: PgPort,
        user: PgUser,
        password: "wrong_password",
        database: PgDatabase,
        sslMode: sslDisable,
      )
      var raised = false
      try:
        let conn = await connect(badConfig)
        await conn.close()
      except PgError:
        raised = true
      doAssert raised

    waitFor t()

suite "E2E: DSN Connection":
  test "connect via parseDsn":
    proc t() {.async.} =
      let config =
        parseDsn("postgresql://test:test@127.0.0.1:15432/test?sslmode=disable")
      let conn = await connect(config)
      doAssert conn.state == csReady
      let res = await conn.query("SELECT 1 AS val")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getStr(0) == "1"
      await conn.close()

    waitFor t()

suite "E2E: Connection Edge Cases":
  test "double close is safe":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      doAssert conn.state == csReady
      await conn.close()
      doAssert conn.state == csClosed
      await conn.close()
      doAssert conn.state == csClosed

    waitFor t()

  test "operations on closed connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      await conn.close()

      var raised1 = false
      try:
        discard await conn.exec("SELECT 1")
      except PgError:
        raised1 = true
      doAssert raised1

      var raised2 = false
      try:
        discard await conn.query("SELECT 1")
      except PgError:
        raised2 = true
      doAssert raised2

      var raised3 = false
      try:
        discard await conn.simpleQuery("SELECT 1")
      except PgError:
        raised3 = true
      doAssert raised3

    waitFor t()
