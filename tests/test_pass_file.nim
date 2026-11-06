## The password file (`.pgpass`): libpq's format and matching rules, how a
## missing or unsafe file is skipped, and what `connect` sends from it.

import std/[unittest, os, tempfiles, strutils]

import ../async_postgres/[async_backend, pg_errors]
import ../async_postgres/pg_connection
import ../async_postgres/pg_connection/conf_files {.all.}
from ../async_postgres/pg_connection/lifecycle {.all.} import passFileHost

import ./mock_pg_server

proc find(content: string, host = "h", port = "5432", db = "db", user = "u"): string =
  ## The password `content` gives the keys, or "<none>".
  let got = findPassword(content, host, port, db, user)
  if got.found: got.password else: "<none>"

proc dsnQuote(s: string): string =
  ## `s` as a quoted DSN value: a Windows path's backslashes would otherwise
  ## be read as escapes.
  "'" & s.replace("\\", "\\\\").replace("'", "\\'") & "'"

proc writePassFile(path, content: string) =
  writeFile(path, content)
  setFilePermissions(path, {fpUserRead, fpUserWrite})

template withTempDir(body: untyped) =
  let dir {.inject.} = createTempDir("pg_pass_file_", "")
  try:
    body
  finally:
    removeDir(dir)

suite "password file format":
  test "the first line matching all four keys wins":
    check find("h:5432:db:u:one\nh:5432:db:u:two\n") == "one"
    check find("x:5432:db:u:no\nh:5432:db:u:yes\n") == "yes"
    check find("h:5433:db:u:no\n") == "<none>"
    check find("H:5432:db:u:no\n") == "<none>" # exact bytes

  test "a whole-field * matches anything":
    check find("*:*:*:*:pw") == "pw"
    check find("h:*:db:*:pw") == "pw"
    check find("*x:5432:db:u:pw") == "<none>"
    check find("*x:5432:db:u:pw", host = "*x") == "pw"

  test "a backslash takes the next character literally, in the password too":
    check find("\\*:5432:db:u:pw") == "<none>"
    check find("\\*:5432:db:u:pw", host = "*") == "pw"
    check find("h\\:1:5432:db:u:p\\:w\\\\d", host = "h:1") == "p:w\\d"
    check find("\\h:5432:db:u:pw") == "pw"
    check find("h:5432:db:u:pw\\") == "pw\\" # a trailing lone one stays

  test "the password ends at an unescaped colon":
    check find("h:5432:db:u:pw:more:fields") == "pw"

  test "comments, blank lines and line ends":
    check find("#h:5432:db:u:no\n\n\nh:5432:db:u:yes") == "yes"
    check find(" #h:5432:db:u:pw", host = " #h") == "pw" # only column 0 comments
    check find(" h:5432:db:u:pw") == "<none>" # leading space is part of the host
    check find("h:5432:db:u:pw\r\n") == "pw"
    check find("h:5432:db:u:pw  \n") == "pw  "

  test "a line short of five fields does not match; an empty password does":
    check find("h:5432:db:u") == "<none>"
    check find("h:5432:db") == "<none>"
    check find("h:5432:db:u:") == ""

  test "a line holding a zero byte is skipped":
    check find("h:5432:db:u:p\0w\nh:5432:db:u:ok") == "ok"
    check find("h\0:5432:db:u:pw", host = "h") == "<none>"

suite "configuration file reading":
  test "a size hint short of the content still reads all of it":
    # A streamed file (st_size == 0) or one growing mid-read.
    withTempDir:
      let path = dir / "big"
      let content = "x".repeat(10_000) & "end"
      writeFile(path, content)
      var f = open(path)
      try:
        check readCapped(f, 0) == content
      finally:
        f.close()

suite "password file lookup":
  test "a found password says where it came from":
    withTempDir:
      let path = dir / "pgpass"
      writePassFile(path, "h:5432:db:u:pw\n")
      let got = lookupPassFile(path, "h", "5432", "db", "u")
      check got.found
      check got.password == "pw"
      check got.note == "password retrieved from file \"" & path & "\""

  test "a missing file, or no user or database, is silently nothing":
    withTempDir:
      check lookupPassFile(dir / "nope", "h", "5432", "db", "u") == PassFileLookup()
      let path = dir / "pgpass"
      writePassFile(path, "*:*:*:*:pw\n")
      check not lookupPassFile(path, "h", "5432", "", "u").found
      check not lookupPassFile(path, "h", "5432", "db", "").found
      check not lookupPassFile("", "h", "5432", "db", "u").found
      check not lookupPassFile(path & "\0x", "h", "5432", "db", "u").found

  test "an unmatched file says nothing":
    withTempDir:
      let path = dir / "pgpass"
      writePassFile(path, "other:5432:db:u:pw\n")
      check lookupPassFile(path, "h", "5432", "db", "u") == PassFileLookup()

  test "a non-regular file is skipped with a note":
    # Windows `fopen` refuses a directory before the handle check, so use a
    # character device there.
    withTempDir:
      let path = when defined(windows): "NUL" else: dir
      let got = lookupPassFile(path, "h", "5432", "db", "u")
      check not got.found
      check "is not a plain file" in got.note

  when defined(posix):
    test "group or world access skips the file with a note":
      withTempDir:
        let path = dir / "pgpass"
        writeFile(path, "*:*:*:*:pw\n")
        setFilePermissions(path, {fpUserRead, fpUserWrite, fpGroupRead})
        let got = lookupPassFile(path, "h", "5432", "db", "u")
        check not got.found
        check "has group or world access" in got.note

suite "password file key":
  test "a Unix socket directory matches as localhost":
    let cfg = parseDsn("host=/var/run/postgresql user=u dbname=d", environment = false)
    check passFileHost(cfg, cfg.getHosts()[0]) == "localhost"

  test "a hostaddr-only target matches as its address":
    let cfg = parseDsn("hostaddr=10.0.0.1 user=u dbname=d", environment = false)
    check passFileHost(cfg, cfg.getHosts()[0]) == "10.0.0.1"

  test "initConnConfig carries passFile; its default host matches as written":
    let cfg = initConnConfig(passFile = "/x")
    check cfg.passFile == "/x"
    check passFileHost(cfg, cfg.getHosts()[0]) == "127.0.0.1"

proc closedPort(): Future[int] {.async.} =
  ## A port nothing listens on: a dial to it is refused at once.
  let ms = startMockServer()
  result = ms.port
  await closeServer(ms)

proc sentPassword(cfg: ConnConfig, ms: MockServer): Future[string] {.async.} =
  ## The password `connect(cfg)` answers `ms`'s cleartext request with.
  var st: MockClient
  var sent: string
  proc server() {.async.} =
    st = await ms.accept()
    await drainStartupMessage(st)
    await sendBytes(st, buildAuthCleartextPassword())
    sent = cast[string]((await drainFrontendMessage(st)).body)
    await sendFullHandshake(st)

  let serverFut = server()
  let conn = await connect(cfg)
  await serverFut
  doAssert conn.state == csReady
  await conn.close()
  await closeClient(st)
  return sent

proc refusedMessage(
    cfg: ConnConfig, ms: MockServer, sqlState: string
): Future[string] {.async.} =
  ## `connect(cfg)`'s error when `ms` answers its password with `sqlState`.
  var st: MockClient
  proc server() {.async.} =
    st = await ms.accept()
    await drainStartupMessage(st)
    await sendBytes(st, buildAuthCleartextPassword())
    discard await drainFrontendMessage(st)
    await sendBytes(
      st, buildErrorResponse(sqlState, "password authentication failed", "FATAL")
    )

  let serverFut = server()
  try:
    let conn = await connect(cfg)
    await conn.close()
  except PgConnectionError as e:
    result = e.msg
  await serverFut
  await closeClient(st)

proc fileConfig(port: int, passFile: string): ConnConfig =
  ConnConfig(
    host: "127.0.0.1",
    port: port,
    user: "test",
    database: "test",
    sslMode: sslDisable,
    passFile: passFile,
  )

suite "connect: password file":
  test "an empty password is taken from the file, by each host's port":
    proc testBody(): Future[string] {.async.} =
      let failing = await closedPort()
      let ms = startMockServer()
      let dir = createTempDir("pg_pass_file_", "")
      let path = dir / "pgpass"
      writePassFile(
        path,
        "127.0.0.1:" & $failing & ":test:test:first\n" & "127.0.0.1:" & $ms.port &
          ":test:test:second\n",
      )
      var cfg = fileConfig(0, path)
      cfg.hosts = @[
        HostEntry(host: "127.0.0.1", port: failing),
        HostEntry(host: "127.0.0.1", port: ms.port),
      ]
      result = await sentPassword(cfg, ms)
      await closeServer(ms)
      removeDir(dir)

    check (waitFor testBody()) == "second\0"

  test "a set password, or a credentialsProvider, keeps the file unread":
    proc testBody(): Future[seq[string]] {.async.} =
      let ms = startMockServer()
      let dir = createTempDir("pg_pass_file_", "")
      let path = dir / "pgpass"
      writePassFile(path, "*:*:*:*:fromfile\n")
      var withPassword = fileConfig(ms.port, path)
      withPassword.password = "given"
      result.add await sentPassword(withPassword, ms)
      var withProvider = fileConfig(ms.port, path)
      withProvider.credentialsProvider = makeCredentialsProvider:
        ConnCredentials(password: "tok")
      result.add await sentPassword(withProvider, ms)
      await closeServer(ms)
      removeDir(dir)

    check (waitFor testBody()) == @["given\0", "tok\0"]

  test "an empty database is matched as the user":
    proc testBody(): Future[string] {.async.} =
      let ms = startMockServer()
      let dir = createTempDir("pg_pass_file_", "")
      let path = dir / "pgpass"
      writePassFile(path, "127.0.0.1:" & $ms.port & ":test:test:pw\n")
      var cfg = fileConfig(ms.port, path)
      cfg.database = ""
      result = await sentPassword(cfg, ms)
      await closeServer(ms)
      removeDir(dir)

    check (waitFor testBody()) == "pw\0"

  test "physical replication matches as replication whatever its database":
    proc testBody(): Future[seq[string]] {.async.} =
      let ms = startMockServer()
      let dir = createTempDir("pg_pass_file_", "")
      let path = dir / "pgpass"
      let prefix = "127.0.0.1:" & $ms.port & ":"
      writePassFile(
        path,
        prefix & "replication:test:physical\n" & prefix & "app:test:byname\n" & prefix &
          "test:test:byuser\n",
      )
      for (db, mode) in [
        ("", "true"), ("app", "true"), ("", "database"), ("app", "database")
      ]:
        var cfg = fileConfig(ms.port, path)
        cfg.database = db
        cfg.extraParams = @[("replication", mode)]
        result.add await sentPassword(cfg, ms)
      await closeServer(ms)
      removeDir(dir)

    check (waitFor testBody()) == @["physical\0", "physical\0", "byuser\0", "byname\0"]

  test "the implicit default host matches as localhost, an explicit one as written":
    proc testBody(): Future[seq[string]] {.async.} =
      let ms = startMockServer()
      let dir = createTempDir("pg_pass_file_", "")
      let path = dir / "pgpass"
      writePassFile(
        path,
        "127.0.0.1:" & $ms.port & ":test:test:byaddress\n" & "localhost:" & $ms.port &
          ":test:test:byname\n",
      )
      let tail =
        " port=" & $ms.port & " user=test dbname=test sslmode=disable passfile=" &
        dsnQuote(path)
      result.add await sentPassword(parseDsn(tail, environment = false), ms)
      result.add await sentPassword(
        parseDsn("host=127.0.0.1" & tail, environment = false), ms
      )
      await closeServer(ms)
      removeDir(dir)

    check (waitFor testBody()) == @["byname\0", "byaddress\0"]

  test "an edit to the file takes effect on the next connect":
    proc testBody(): Future[seq[string]] {.async.} =
      let ms = startMockServer()
      let dir = createTempDir("pg_pass_file_", "")
      let path = dir / "pgpass"
      let cfg = fileConfig(ms.port, path)
      writePassFile(path, "*:*:*:*:old\n")
      result.add await sentPassword(cfg, ms)
      writePassFile(path, "*:*:*:*:new\n")
      result.add await sentPassword(cfg, ms)
      await closeServer(ms)
      removeDir(dir)

    check (waitFor testBody()) == @["old\0", "new\0"]

  test "a refused password from the file names the file":
    proc testBody(): Future[seq[string]] {.async.} =
      let ms = startMockServer()
      let dir = createTempDir("pg_pass_file_", "")
      let path = dir / "pgpass"
      writePassFile(path, "*:*:*:*:stale\n")
      let cfg = fileConfig(ms.port, path)
      result.add await refusedMessage(cfg, ms, "28P01")
      result.add await refusedMessage(cfg, ms, "28000")
      await closeServer(ms)
      removeDir(dir)
      result.add path

    let got = waitFor testBody()
    # `connect` folds each host's message onto one line.
    check ("(SQLSTATE 28P01) | password retrieved from file \"" & got[2] & "\"") in
      got[0]
    check "password retrieved" notin got[1]

  when defined(posix):
    test "a refusal after an unsafe file was skipped says why":
      proc testBody(): Future[string] {.async.} =
        let ms = startMockServer()
        let dir = createTempDir("pg_pass_file_", "")
        let path = dir / "pgpass"
        writeFile(path, "*:*:*:*:pw\n")
        setFilePermissions(path, {fpUserRead, fpUserWrite, fpOthersRead})
        result = await refusedMessage(fileConfig(ms.port, path), ms, "28P01")
        await closeServer(ms)
        removeDir(dir)

      check "has group or world access" in (waitFor testBody())
