import std/[unittest, options, tables]
from std/times import dateTime, mJan, mFeb, utc, `==`
from std/strutils import startsWith

import ../async_postgres/[async_backend, pg_types, pg_client, pg_connection]

import e2e_common

template withProbeRole(role, rolePassword, setting: string, body: untyped) =
  ## Runs ``body`` with ``cfg`` logging in as ``role`` with ``rolePassword``,
  ## whose session default is ``setting`` unless empty. Drops the role after
  ## ``body``'s own deferred closes.
  let admin = await connect(plainConfig())
  defer:
    await admin.close()
  discard await admin.simpleQuery("DROP ROLE IF EXISTS " & role)
  var ddl = "CREATE ROLE " & role & " LOGIN PASSWORD " & quoteLiteral(rolePassword)
  if setting.len > 0:
    ddl.add "; ALTER ROLE " & role & " SET " & setting
  discard await admin.simpleQuery(ddl)
  defer:
    discard await admin.simpleQuery("DROP ROLE " & role)
  var cfg {.inject.} = plainConfig()
  cfg.user = role
  cfg.password = rolePassword
  body

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

  test "client_encoding is pinned to UTF8":
    proc t() {.async.} =
      var cfg = plainConfig()
      cfg.extraParams = @[("client_encoding", "utf-8")]
      let conn = await connect(cfg)
      doAssert conn.serverParam("client_encoding") == "UTF8"
      var raised = false
      try:
        discard await conn.simpleQuery("SET client_encoding TO 'SJIS'")
      except PgProtocolError:
        raised = true
      doAssert raised
      doAssert conn.state == csClosed
      await conn.close()

    waitFor t()

  test "DateStyle is ISO from startup, so RESET and DISCARD ALL keep it":
    proc t() {.async.} =
      withProbeRole("async_pg_datestyle_probe", "p", "DateStyle = 'SQL, YMD'"):
        # The startup value overrides the role's, field order included.
        let conn = await connect(cfg)
        defer:
          await conn.close()
        let style = conn.serverParam("DateStyle")
        doAssert style.startsWith("ISO, ") and style != "ISO, YMD", style
        for sql in [
          "RESET DateStyle", "RESET ALL", "DISCARD ALL", "SET DateStyle TO DEFAULT"
        ]:
          discard await conn.simpleQuery(sql)
          doAssert conn.state == csReady, sql
          doAssert conn.serverParam("DateStyle") == style, sql
        let res = await conn.query(
          "SELECT '2024-01-15 10:00:00'::timestamp", resultFormat = rfText
        )
        doAssert res.rows[0].getTimestamp(0) ==
          dateTime(2024, mJan, 15, 10, 0, 0, 0, utc())
        # A field order from the caller joins the ISO style, and RESET keeps it.
        cfg.extraParams = @[("DateStyle", "DMY")]
        let explicit = await connect(cfg)
        defer:
          await explicit.close()
        doAssert explicit.serverParam("DateStyle") == "ISO, DMY"
        discard await explicit.simpleQuery("RESET ALL")
        doAssert explicit.serverParam("DateStyle") == "ISO, DMY"
        let dmy =
          await explicit.query("SELECT '01/02/2026'::date", resultFormat = rfText)
        doAssert dmy.rows[0].getDate(0) == dateTime(2026, mFeb, 1, 0, 0, 0, 0, utc())

    waitFor t()

  test "a later non-ISO DateStyle closes the connection":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # A field order change keeps the ISO output.
      discard await conn.simpleQuery("SET DateStyle TO 'ISO, DMY'")
      doAssert conn.state == csReady
      let res = await conn.query("SELECT '2024-01-15'::date", resultFormat = rfText)
      doAssert res.rows[0].getDate(0) == dateTime(2024, mJan, 15, 0, 0, 0, 0, utc())
      var raised = false
      try:
        discard await conn.simpleQuery("SET DateStyle TO 'SQL, DMY'")
      except PgProtocolError:
        raised = true
      doAssert raised
      doAssert conn.state == csClosed
      await conn.close()

    waitFor t()

  test "TimeZone is UTC from startup, so a DateTime binds to timestamptz as its instant":
    const sameInstant = "SELECT $1::timestamptz = '2024-01-01 01:00:00Z'"

    proc bindsInstant(cfg: ConnConfig, zone: string): Future[bool] {.async.} =
      ## Whether a session of ``cfg`` reports ``zone`` and reads a UTC
      ## ``DateTime`` bound to ``timestamptz`` as its instant. Not a loop body:
      ## Nim 2.2.4 rejects an awaiting ``defer`` in a ``for`` over an array.
      let conn = await connect(cfg)
      defer:
        await conn.close()
      doAssert conn.serverParam("TimeZone") == zone, zone
      let res = await conn.query(
        sameInstant, @[toPgParam(dateTime(2024, mJan, 1, 1, 0, 0, 0, utc()))]
      )
      return res.rows[0].getBool(0)

    proc t() {.async.} =
      withProbeRole("async_pg_timezone_probe", "p", "TimeZone = 'Asia/Tokyo'"):
        let dt = dateTime(2024, mJan, 1, 1, 0, 0, 0, utc())
        # The startup value overrides the role's.
        let conn = await connect(cfg)
        defer:
          await conn.close()
        doAssert conn.serverParam("TimeZone") == "UTC"
        for sql in ["RESET ALL", "DISCARD ALL"]:
          discard await conn.simpleQuery(sql)
          doAssert conn.serverParam("TimeZone") == "UTC", sql
        for p in [toPgParam(dt), toPgBinaryParam(dt)]:
          let res = await conn.query(sameInstant, @[p])
          doAssert res.rows[0].getBool(0)
        # A caller's zone replaces UTC, directly or via options; DEFAULT keeps
        # the role's.
        for (p, zone) in [
          (("TimeZone", "America/New_York"), "America/New_York"),
          (("options", "-c TimeZone=America/New_York"), "America/New_York"),
          (("TimeZone", "DEFAULT"), "Asia/Tokyo"),
        ]:
          cfg.extraParams = @[p]
          let same = await bindsInstant(cfg, zone)
          doAssert not same, p[1]
        # A -c switch asking for UTC binds the instant like the startup pin, and
        # so does a zero offset under a POSIX spelling; the server reports a
        # numeric zone under a bracketed name.
        for (p, zone) in [
          (("options", "-c TimeZone=UTC"), "UTC"),
          (("TimeZone", "UTC0"), "UTC0"),
          (("TimeZone", "+00"), "<+00>-00"),
        ]:
          cfg.extraParams = @[p]
          let same = await bindsInstant(cfg, zone)
          doAssert same, p[1]

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

  test "require_auth=scram-sha-256 connects over SSL, channel binding or not":
    # libpq's scram-sha-256 covers SCRAM-SHA-256-PLUS, which the server offers.
    # Only cbRequire proves that here; test_ssl pins each mode's choice.
    proc t(cb: ChannelBindingMode) {.async.} =
      var cfg = sslConfig(sslRequire)
      cfg.requireAuth = {amScramSha256}
      cfg.channelBinding = cb
      let conn = await connect(cfg)
      doAssert conn.sslEnabled == true
      await conn.close()

    for cb in [cbPrefer, cbRequire, cbDisable]:
      checkpoint "channelBinding=" & $cb
      waitFor t(cb)

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
      # The plaintext leg keeps sslmode=allow, so a reconnect still tries TLS
      # (libpq `PQreset` parity).
      doAssert conn.config.sslMode == sslAllow
      await conn.close()

    waitFor t()

  test "query over SSL connection":
    proc t() {.async.} =
      let conn = await connect(sslConfig(sslRequire))
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

  test "sslnegotiation=direct connects, negotiates ALPN, and queries (PG17+)":
    # Exercises the direct-SSL success path against a real server: no
    # SSLRequest probe is sent, the ClientHello advertises the "postgresql"
    # ALPN, and the server must both accept the immediate TLS start and
    # select that ALPN — otherwise assertAlpnPostgres raises.
    proc t() {.async.} =
      var cfg = sslConfig(sslRequire)
      cfg.sslNegotiation = sslnDirect
      let conn = await connect(cfg)
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
      let results = await conn.simpleQuery("SELECT 42 AS answer")
      doAssert results[0].rows[0][0].get().toString() == "42"
      await conn.close()

    waitFor t()

  test "sslnegotiation=direct with sslVerifyFull succeeds against configured CA":
    # Combines direct SSL with verify-full: SNI/host identity must be
    # verified on the same TLS session that starts without an SSLRequest.
    proc t() {.async.} =
      let conn = await connect(
        ConnConfig(
          host: "localhost",
          port: PgPort,
          user: PgUser,
          password: PgPassword,
          database: PgDatabase,
          sslMode: sslVerifyFull,
          sslNegotiation: sslnDirect,
          sslRootCert: loadCaCert(),
        )
      )
      doAssert conn.state == csReady
      doAssert conn.sslEnabled == true
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
      except PgSecurityError:
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
      except PgSecurityError:
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

  proc scramLogin(password: string) {.async.} =
    withProbeRole("async_pg_saslprep_probe", password, ""):
      # Without this, a trust pg_hba entry would pass any password.
      cfg.requireAuth = {amScramSha256}
      let conn = await connect(cfg)
      defer:
        await conn.close()
      doAssert conn.state == csReady

  test "SCRAM password with a code point unassigned in Unicode 3.2":
    # The server hashes such a password raw, U+00A0 included.
    waitFor scramLogin("pass\xC2\xA0word\xF0\x9F\x98\x80")

  test "SCRAM password bidi check uses the Unicode 3.2 tables":
    # U+2801 is not L in 3.2, so the server maps U+00A0.
    waitFor scramLogin("\xD7\x90\xC2\xA0\xE2\xA0\x81\xD7\x90")
    # U+1D6C1 is L in 3.2, so the server hashes the password raw.
    waitFor scramLogin("\xD7\x90\xC2\xA0\xF0\x9D\x9B\x81\xD7\x90")

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
