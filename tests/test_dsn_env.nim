## `parseDsn`'s libpq defaults: environment variables, the connection service
## file and the OS user, read through a fake `DsnEnv` so the process
## environment never matters.

import std/[unittest, os, tempfiles, strutils, tables, importutils]

import ../async_postgres/[async_backend, pg_connection]
import ../async_postgres/pg_connection/dsn {.all.}
import ../async_postgres/pg_connection/types
when defined(posix):
  from std/posix import geteuid

privateAccess(DsnEnv)

proc fakeEnv(
    vars: openArray[(string, string)] = [], user = "alice", home = ""
): DsnEnv =
  let table = vars.toTable
  DsnEnv(
    enabled: true,
    get: proc(name: string): string {.gcsafe, raises: [].} =
      table.getOrDefault(name),
    user: proc(): tuple[name, home: string] {.gcsafe, raises: [].} =
      (user, home),
  )

const passName = when defined(windows): "pgpass.conf" else: ".pgpass"

proc userDirVar(dir: string): (string, string) =
  ## The variable `libpqUserDir` reads to find `dir`.
  when defined(windows):
    ("APPDATA", dir)
  else:
    ("HOME", dir)

proc userDir(dir: string): string =
  ## Where `libpqUserDir` looks under `userDirVar(dir)`.
  when defined(windows):
    dir & "\\postgresql"
  else:
    dir

proc countingEnv(sysDir: string, calls: ref int): DsnEnv =
  ## No variable but ``PGSYSCONFDIR`` (so no ``HOME``), counting user lookups.
  DsnEnv(
    enabled: true,
    get: proc(name: string): string {.gcsafe, raises: [].} =
      (if name == "PGSYSCONFDIR": sysDir else: ""),
    user: proc(): tuple[name, home: string] {.gcsafe, raises: [].} =
      inc calls[]
      ("alice", sysDir / "nohome"),
  )

proc dsnQuote(s: string): string =
  ## `s` as a quoted DSN value: a Windows path's backslashes would otherwise
  ## be read as escapes.
  "'" & s.replace("\\", "\\\\").replace("'", "\\'") & "'"

proc parseError(dsn: string, env: DsnEnv): string =
  try:
    discard parseDsnWith(dsn, env)
  except PgConfigError as e:
    return e.msg

template withTempDir(body: untyped) =
  let dir {.inject.} = createTempDir("pg_dsn_env_", "")
  try:
    body
  finally:
    removeDir(dir)

suite "parseDsn: environment variables":
  test "fill what the string leaves out":
    let cfg = parseDsnWith(
      "dbname=fromdsn",
      fakeEnv(
        {
          "PGHOST": "envhost",
          "PGPORT": "5433",
          "PGDATABASE": "fromenv",
          "PGUSER": "envuser",
          "PGPASSWORD": "envpw",
          "PGAPPNAME": "app",
          "PGSSLMODE": "require",
          "PGCONNECT_TIMEOUT": "7",
          "PGTARGETSESSIONATTRS": "read-write",
        }
      ),
    )
    check cfg.host == "envhost"
    check cfg.port == 5433
    check cfg.database == "fromdsn"
    check cfg.user == "envuser"
    check cfg.password == "envpw"
    check cfg.applicationName == "app"
    check cfg.sslMode == sslRequire
    check cfg.connectTimeout == seconds(7)
    check cfg.targetSessionAttrs == tsaReadWrite

  test "an empty variable counts as unset":
    let cfg = parseDsnWith(
      "", fakeEnv({"PGHOST": "", "PGPORT": "", "PGSSLMODE": "", "PGUSER": ""})
    )
    check cfg.host == "127.0.0.1"
    check cfg.port == 5432
    check cfg.sslMode == sslPrefer
    check cfg.user == "alice"

  test "a key given empty in the string still blocks the variable":
    let cfg = parseDsnWith("host=h password=''", fakeEnv({"PGPASSWORD": "envpw"}))
    check cfg.password == ""

  test "each key is filled on its own: a DSN hostaddr pairs with PGHOST":
    let cfg = parseDsnWith("hostaddr=10.0.0.1", fakeEnv({"PGHOST": "db.example"}))
    check cfg.hosts == @[
      HostEntry(host: "db.example", hostaddr: "10.0.0.1", port: 5432)
    ]

  test "a PGHOST list with a DSN port":
    let cfg = parseDsnWith("port=6000", fakeEnv({"PGHOST": "a,b"}))
    check cfg.hosts ==
      @[HostEntry(host: "a", port: 6000), HostEntry(host: "b", port: 6000)]

  test "a URI's empty user, password, authority and path are unset":
    let env = fakeEnv(
      {
        "PGHOST": "envhost",
        "PGPORT": "6001",
        "PGUSER": "envuser",
        "PGPASSWORD": "envpw",
        "PGDATABASE": "envdb",
      }
    )
    let cfg = parseDsnWith("postgresql://:@/", env)
    check cfg.host == "envhost"
    check cfg.port == 6001
    check cfg.user == "envuser"
    check cfg.password == "envpw"
    check cfg.database == "envdb"
    let given = parseDsnWith("postgresql://u:p@h:6002/d", env)
    check given.host == "h"
    check given.port == 6002
    check given.user == "u"
    check given.password == "p"
    check given.database == "d"

  test "a URI authority with only a port takes PGHOST, else stays rejected":
    let cfg = parseDsnWith("postgresql://:6004/db", fakeEnv({"PGHOST": "a,b"}))
    check cfg.hosts ==
      @[HostEntry(host: "a", port: 6004), HostEntry(host: "b", port: 6004)]
    check not cfg.hostDefaulted
    check parseError("postgresql://:6004/db", fakeEnv()).startsWith("Empty host")
    let paired = parseDsnWith("postgresql://:6004/db?hostaddr=10.0.0.1", fakeEnv())
    check paired.hosts == @[HostEntry(host: "", hostaddr: "10.0.0.1", port: 6004)]
    let both = parseDsnWith(
      "postgresql://:6004/db?hostaddr=10.0.0.1", fakeEnv({"PGHOST": "envhost"})
    )
    check both.hosts == @[HostEntry(host: "envhost", hostaddr: "10.0.0.1", port: 6004)]

  test "a URI host list without ports takes PGPORT":
    let cfg = parseDsnWith("postgresql://a,b/db", fakeEnv({"PGPORT": "6003"}))
    check cfg.hosts ==
      @[HostEntry(host: "a", port: 6003), HostEntry(host: "b", port: 6003)]

  test "PGREQUIRESSL=1 means require only when no sslmode is set":
    check parseDsnWith("", fakeEnv({"PGREQUIRESSL": "1"})).sslMode == sslRequire
    check parseDsnWith("", fakeEnv({"PGREQUIRESSL": "0"})).sslMode == sslPrefer
    check parseDsnWith("", fakeEnv({"PGREQUIRESSL": "1", "PGSSLMODE": "disable"})).sslMode ==
      sslDisable
    check parseDsnWith("sslmode=allow", fakeEnv({"PGREQUIRESSL": "1"})).sslMode ==
      sslAllow

  test "PGDATESTYLE and PGGEQO become startup parameters":
    let cfg = parseDsnWith("", fakeEnv({"PGDATESTYLE": "ISO, DMY", "PGGEQO": "off"}))
    check ("DateStyle", "DMY") in cfg.extraParams
    check ("geqo", "off") in cfg.extraParams

  test "PGTZ is not read, so the session TimeZone stays UTC":
    # A DateTime parameter is sent as a zoneless timestamp, which a timestamptz
    # target reads in the session zone: another zone would shift the instant.
    for val in ["UTC", "Asia/Tokyo", "default"]:
      let cfg = parseDsnWith("", fakeEnv({"PGTZ": val}))
      for (k, _) in cfg.extraParams:
        check not isGucName(k, "TimeZone")

  test "PGDATESTYLE=default sends nothing":
    let cfg = parseDsnWith("", fakeEnv({"PGDATESTYLE": "Default"}))
    for (k, _) in cfg.extraParams:
      check not isGucName(k, "DateStyle")

  test "the string's TimeZone is kept and PGTZ adds none":
    for dsn in ["timezone=UTC", "options='-c TimeZone=UTC'"]:
      let cfg = parseDsnWith(dsn, fakeEnv({"PGTZ": "Asia/Tokyo"}))
      check ("TimeZone", "Asia/Tokyo") notin cfg.extraParams

  test "a service's -c switch outranks PGDATESTYLE, PGOPTIONS's does not":
    # libpq sends both; the startup parameter, applied later, wins.
    let env = {"PGOPTIONS": "-c DateStyle=DMY", "PGDATESTYLE": "ISO, MDY"}
    check ("DateStyle", "MDY") in parseDsnWith("", fakeEnv(env)).extraParams
    withTempDir:
      let file = dir / "s.conf"
      writeFile(file, "[a]\noptions=-c DateStyle=DMY\n")
      let cfg = parseDsnWith(
        "service=a", fakeEnv({"PGSERVICEFILE": file, "PGDATESTYLE": "ISO, MDY"})
      )
      check ("DateStyle", "MDY") notin cfg.extraParams

  test "a variable's bad value names the variable, not the value":
    let msg = parseError("", fakeEnv({"PGPORT": "notaport"}))
    check msg.startsWith("PGPORT: ")
    check "notaport" notin msg
    check parseError("", fakeEnv({"PGSSLMODE": "sometimes"})).startsWith("PGSSLMODE: ")
    check parseError("", fakeEnv({"PGCONNECT_TIMEOUT": "soon"})).startsWith(
      "PGCONNECT_TIMEOUT: "
    )

  test "a libpq keyword asking for an unapplied check is rejected":
    for (name, val) in [
      ("PGSSLCRL", "/crl.pem"),
      ("PGSSLCRLDIR", "/crl"),
      ("PGREQUIREPEER", "postgres"),
      ("PGSSLCERTMODE", "require"),
      ("PGSSLMINPROTOCOLVERSION", "TLSv1.3"),
      ("PGSSLMAXPROTOCOLVERSION", "TLSv1.2"),
      ("PGGSSENCMODE", "require"),
      ("PGMINPROTOCOLVERSION", "3.2"),
    ]:
      let msg = parseError("", fakeEnv({name: val}))
      check msg.startsWith(name & ": ")
      check "is not supported" in msg

  test "a libpq keyword whose value already holds is ignored":
    for (name, val) in [
      ("PGSSLCERTMODE", "allow"),
      ("PGSSLCERTMODE", "disable"),
      ("PGSSLMINPROTOCOLVERSION", "TLSv1.2"),
      ("PGSSLMAXPROTOCOLVERSION", "TLSv1.3"),
      ("PGGSSENCMODE", "disable"),
      ("PGGSSENCMODE", "prefer"),
      ("PGMINPROTOCOLVERSION", "3.0"),
      ("PGMAXPROTOCOLVERSION", "3.2"),
      ("PGKRBSRVNAME", "postgres"),
      ("PGGSSLIB", "gssapi"),
      ("PGGSSDELEGATION", "1"),
      ("PGSSLCOMPRESSION", "1"),
    ]:
      let cfg = parseDsnWith("", fakeEnv({name: val}))
      check cfg.extraParams.len == 0

  test "a missing user is the OS user":
    check parseDsnWith("", fakeEnv(user = "osuser")).user == "osuser"
    check parseDsnWith("user=given", fakeEnv(user = "osuser")).user == "given"
    check parseDsnWith("", fakeEnv({"PGUSER": "envuser"}, user = "osuser")).user ==
      "envuser"

  test "the password file defaults under the user's directory":
    let cfg = parseDsnWith("", fakeEnv([userDirVar("/u")]))
    check cfg.passFile == confPath(userDir("/u"), passName)
    check parseDsnWith("passfile=/a", fakeEnv([userDirVar("/u")])).passFile == "/a"
    check parseDsnWith("", fakeEnv([userDirVar("/u"), ("PGPASSFILE", "/b")])).passFile ==
      "/b"
    check parseDsnWith("passfile=/a", fakeEnv({"PGPASSFILE": "/b"})).passFile == "/a"

  when not defined(windows):
    test "without HOME, the user database's home directory":
      check parseDsnWith("", fakeEnv(home = "/pw/home")).passFile == "/pw/home/.pgpass"

    test "with no home directory at all, no password file":
      check parseDsnWith("", fakeEnv()).passFile == ""

  test "the OS user is looked up once per parse":
    # Without HOME, the service search, the user and the password file would
    # each query the user database.
    withTempDir:
      writeFile(dir / "pg_service.conf", "[a]\nhost=h\n")
      let calls = new int
      check parseDsnWith("service=a", countingEnv(dir, calls)).user == "alice"
      check calls[] == 1

  test "environment = false reads nothing":
    let cfg = parseDsn("", environment = false)
    check cfg.user == ""
    check cfg.passFile == ""
    check cfg.host == "127.0.0.1"
    check parseDsnWith("", DsnEnv()).user == ""

  test "the implicit default host is marked, an explicit one is not":
    check parseDsnWith("", fakeEnv()).hostDefaulted
    check parseDsnWith("port=5433", fakeEnv()).hostDefaulted
    check not parseDsnWith("host=127.0.0.1", fakeEnv()).hostDefaulted
    check not parseDsnWith("", fakeEnv({"PGHOST": "127.0.0.1"})).hostDefaulted
    check not parseDsnWith("hostaddr=127.0.0.1", fakeEnv()).hostDefaulted

  test "PGPASSWORD with a credentialsProvider points at the variable and service file":
    var cfg = parseDsnWith("host=h", fakeEnv({"PGPASSWORD": "envpw"}))
    cfg.credentialsProvider = makeCredentialsProvider:
      ConnCredentials(password: "tok")
    try:
      validateConnConfig(cfg)
      check false
    except PgConfigError as e:
      check "PGPASSWORD" in e.msg
      check "service file" in e.msg

suite "parseDsn: what psql's environment asks for that this client does not":
  test "a variable's TLS file is read only when the sslmode uses it":
    withTempDir:
      let cert = dir / "c.pem"
      let key = dir / "k.pem"
      writeFile(cert, "cert")
      writeFile(key, "key")
      setFilePermissions(key, {fpUserRead, fpUserWrite})
      let env = fakeEnv(
        {"PGSSLCERT": cert, "PGSSLKEY": key, "PGSSLROOTCERT": dir / "missing.crt"}
      )
      for mode in ["disable", "allow"]:
        let cfg = parseDsnWith("host=h sslmode=" & mode, env)
        check cfg.sslCert == ""
        check cfg.sslKey == ""
      let tls = parseDsnWith("host=h sslmode=require", env)
      check tls.sslCert == "cert"
      check tls.sslKey == "key"
      check tls.sslRootCert == "" # verifies nothing, so the missing CA is unread
      check parseError("host=h sslmode=verify-full", env).startsWith(
        "PGSSLROOTCERT: Cannot read sslrootcert file"
      )

  test "a variable's missing certificate means none, and a key needs one":
    withTempDir:
      let cert = dir / "c.pem"
      let key = dir / "k.pem"
      writeFile(cert, "cert")
      writeFile(key, "key")
      setFilePermissions(key, {fpUserRead, fpUserWrite})
      let missing = parseDsnWith(
        "host=h", fakeEnv({"PGSSLCERT": dir / "missing.crt", "PGSSLKEY": key})
      )
      check missing.sslCert == ""
      check missing.sslKey == ""
      check parseDsnWith("host=h", fakeEnv({"PGSSLKEY": key})).sslKey == ""
      # One that is there but unusable is still an error, as in libpq.
      check parseError("host=h", fakeEnv({"PGSSLCERT": dir})).startsWith("PGSSLCERT: ")
      check parseError("host=h", fakeEnv({"PGSSLCERT": cert})) ==
        "PGSSLCERT: " & ClientCertPairingErrorMsg
      let paired =
        parseDsnWith("host=h sslkey=" & dsnQuote(key), fakeEnv({"PGSSLCERT": cert}))
      check paired.sslCert == "cert"
      check paired.sslKey == "key"

  test "sslrootcert=system from a variable or a service file is rejected":
    # libpq would verify-full against the system store; this client has none.
    for mode in ["", " sslmode=disable", " sslmode=verify-full"]:
      check parseError("host=h" & mode, fakeEnv({"PGSSLROOTCERT": "system"})) ==
        "PGSSLROOTCERT: sslrootcert=system is not supported by this client"
    withTempDir:
      let file = dir / "s.conf"
      writeFile(file, "[a]\nsslrootcert=system\n")
      check parseError("service=a host=h", fakeEnv({"PGSERVICEFILE": file})) ==
        "service file \"" & file &
        "\", line 2: sslrootcert=system is not supported by this client"

  test "a CA that exists is rejected under require, from the string too":
    # libpq verifies against it under require; prefer and allow may fall back
    # to plaintext there anyway, and a missing CA verifies nothing.
    withTempDir:
      let ca = dir / "ca.pem"
      writeFile(ca, "ca")
      const msg =
        "sslrootcert with sslmode=require is not supported by this client; use verify-ca"
      check parseError("host=h sslmode=require", fakeEnv({"PGSSLROOTCERT": ca})) ==
        "PGSSLROOTCERT: " & msg
      check parseError("host=h", fakeEnv({"PGSSLMODE": "require", "PGSSLROOTCERT": ca})) ==
        "PGSSLROOTCERT: " & msg
      let file = dir / "s.conf"
      writeFile(file, "[a]\nsslmode=require\nsslrootcert=" & ca & "\n")
      check parseError("service=a host=h", fakeEnv({"PGSERVICEFILE": file})) ==
        "service file \"" & file & "\", line 3: " & msg
      # The string's own CA is refused the same way, with no variable to blame.
      check parseError("host=h sslmode=require sslrootcert=" & dsnQuote(ca), fakeEnv()) ==
        msg
      check parseDsnWith(
        "host=h sslmode=verify-ca sslrootcert=" & dsnQuote(ca), fakeEnv()
      ).sslRootCert == "ca"
      for mode in ["", " sslmode=allow", " sslmode=disable"]:
        check parseDsnWith("host=h" & mode, fakeEnv({"PGSSLROOTCERT": ca})).sslRootCert ==
          ""
      check parseDsnWith("host=h sslmode=verify-ca", fakeEnv({"PGSSLROOTCERT": ca})).sslRootCert ==
        "ca"

  test "a string's certificate under a variable's weak sslmode names the variable":
    withTempDir:
      let cert = dir / "c.pem"
      let key = dir / "k.pem"
      writeFile(cert, "cert")
      writeFile(key, "key")
      setFilePermissions(key, {fpUserRead, fpUserWrite})
      let dsn = "host=h sslcert=" & dsnQuote(cert) & " sslkey=" & dsnQuote(key)
      let env = fakeEnv({"PGSSLMODE": "disable"})
      check parseError(dsn, env).startsWith("PGSSLMODE: sslcert/sslkey require sslmode")
      check parseError(dsn & " sslmode=allow", env).startsWith(
        "sslcert/sslkey require sslmode"
      )
      # A pairing fault lies in the string's files, not the sslmode.
      check parseError("host=h sslcert=" & dsnQuote(cert), env) ==
        ClientCertPairingErrorMsg

  test "a string's TLS file is read whatever the sslmode, as before":
    check "Cannot read sslrootcert file" in
      parseError("host=h sslrootcert=/nonexistent/ca.crt", DsnEnv())
    check "Cannot read sslcert file" in
      parseError("host=h sslcert=/nonexistent/c.crt", DsnEnv())

  test "client_encoding from a variable or a service file is ignored":
    check parseDsnWith("", fakeEnv({"PGCLIENTENCODING": "LATIN1"})).extraParams.len == 0
    withTempDir:
      writeFile(dir / "s.conf", "[a]\nclient_encoding=auto\n")
      check parseDsnWith("service=a", fakeEnv({"PGSERVICEFILE": dir / "s.conf"})).extraParams.len ==
        0
    check "client_encoding must be UTF8" in
      parseError("client_encoding=LATIN1", DsnEnv())

  test "PGDATESTYLE keeps only its field order":
    check ("DateStyle", "DMY") in
      parseDsnWith("", fakeEnv({"PGDATESTYLE": "SQL, DMY"})).extraParams
    check parseDsnWith("", fakeEnv({"PGDATESTYLE": "Postgres"})).extraParams.len == 0

  test "PGDATESTYLE=German keeps the DMY order it implies":
    for (style, order) in [
      ("German", "DMY"),
      ("\"german\"", "DMY"),
      ("German, MDY", "MDY"),
      ("German, US", "US"),
    ]:
      check ("DateStyle", order) in
        parseDsnWith("", fakeEnv({"PGDATESTYLE": style})).extraParams

  test "a -c switch in the string outranks PGDATESTYLE and PGGEQO":
    let cfg = parseDsnWith(
      "options='-c DateStyle=ISO,DMY -c geqo=on'",
      fakeEnv({"PGDATESTYLE": "ISO, MDY", "PGGEQO": "off"}),
    )
    check cfg.extraParams.len == 1 # the options themselves

  test "sslcertmode=disable with a client certificate is rejected":
    withTempDir:
      let cert = dir / "c.pem"
      let key = dir / "k.pem"
      writeFile(cert, "cert")
      writeFile(key, "key")
      setFilePermissions(key, {fpUserRead, fpUserWrite})
      check parseError(
        "host=h sslmode=require sslcert=" & dsnQuote(cert) & " sslkey=" & dsnQuote(key),
        fakeEnv({"PGSSLCERTMODE": "disable"}),
      ) ==
        "PGSSLCERTMODE: sslcertmode=disable is not supported by this client with a client certificate"
      # A variable's certificate is then left unread, as libpq sends none.
      let env =
        fakeEnv({"PGSSLCERTMODE": "disable", "PGSSLCERT": cert, "PGSSLKEY": key})
      let cfg = parseDsnWith("host=h sslmode=require", env)
      check cfg.sslCert == ""
      check cfg.sslKey == ""
      let file = dir / "s.conf"
      writeFile(file, "[a]\nsslcertmode=disable\n")
      check parseDsnWith(
        "service=a host=h",
        fakeEnv({"PGSERVICEFILE": file, "PGSSLCERT": cert, "PGSSLKEY": key}),
      ).sslCert == ""

  test "a fault in the string is not blamed on a variable that set another list":
    # The empty host is the string's; the port came from the environment and
    # has nothing to do with it.
    check parseError("host=", fakeEnv({"PGPORT": "6000"})) == "Empty host in DSN"
    check parseError("host=,h2", fakeEnv({"PGPORT": "6000"})) ==
      "Empty host in DSN host list (element #0)"
    check parseError("hostaddr=/tmp/sock", fakeEnv({"PGPORT": "6000"})) ==
      "Invalid hostaddr: must be a numeric IP address, not a Unix socket path (use host for Unix sockets) (len=9)"
    # The bad port is the string's; PGHOST is not.
    check parseError("port=nope", fakeEnv({"PGHOST": "envhost"})) ==
      "Invalid port in DSN (len=4)"

  test "a fault in a variable's list is still named":
    check parseError("", fakeEnv({"PGHOST": "a,"})) ==
      "PGHOST: Empty host in DSN host list (element #1)"
    check parseError("", fakeEnv({"PGPORT": "nope"})) ==
      "PGPORT: Invalid port in DSN (len=4)"

suite "parseDsn: connection service file":
  test "the service fills what the string leaves out, before the environment":
    withTempDir:
      let file = dir / "services.conf"
      writeFile(
        file,
        """
# comment
[other]
host=wrong

[prod]
host = svchost
port=6100
dbname=svcdb
user=svcuser
search_path=app
""",
      )
      let cfg = parseDsnWith(
        "service=prod dbname=dsndb",
        fakeEnv({"PGSERVICEFILE": file, "PGHOST": "envhost", "PGUSER": "envuser"}),
      )
      check cfg.host == "svchost"
      check cfg.port == 6100
      check cfg.database == "dsndb"
      check cfg.user == "svcuser"
      check ("search_path", "app") in cfg.extraParams

  test "PGSERVICE names the service when the string does not":
    withTempDir:
      let file = dir / "s.conf"
      writeFile(file, "[a]\nhost=ha\n[b]\nhost=hb\n")
      let env = fakeEnv({"PGSERVICEFILE": file, "PGSERVICE": "b"})
      check parseDsnWith("", env).host == "hb"
      check parseDsnWith("service=a", env).host == "ha"

  test "an empty service in the string still outranks PGSERVICE":
    withTempDir:
      let file = dir / "s.conf"
      writeFile(file, "[prod]\nuser=svc\n")
      let env = fakeEnv({"PGSERVICEFILE": file, "PGSERVICE": "prod"})
      for dsn in ["service='' host=h", "postgresql://h?service="]:
        check "definition of service not found (len=0)" in parseError(dsn, env)

  test "the user's file wins, and the system file is read only if it lacks the service":
    withTempDir:
      createDir(userDir(dir))
      createDir(dir / "sys")
      writeFile(confPath(userDir(dir), ".pg_service.conf"), "[a]\nhost=user\n")
      writeFile(
        dir / "sys" / "pg_service.conf", "[a]\nhost=sys\nport=6200\n[b]\nhost=sysb\n"
      )
      let env = fakeEnv([userDirVar(dir), ("PGSYSCONFDIR", dir / "sys")])
      let a = parseDsnWith("service=a", env)
      check a.host == "user"
      check a.port == 5432 # files are not merged
      check parseDsnWith("service=b", env).host == "sysb"

  test "PGSERVICEFILE without the service falls through to the system file":
    withTempDir:
      writeFile(dir / "mine.conf", "[a]\nhost=mine\n")
      writeFile(dir / "pg_service.conf", "[b]\nhost=sys\n")
      let env = fakeEnv({"PGSERVICEFILE": dir / "mine.conf", "PGSYSCONFDIR": dir})
      check parseDsnWith("service=b", env).host == "sys"

  test "a missing PGSERVICEFILE is an error, a missing user file is not":
    withTempDir:
      check "not found" in
        parseError("service=a", fakeEnv({"PGSERVICEFILE": dir / "nope.conf"}))
      writeFile(dir / "pg_service.conf", "[a]\nhost=sys\n")
      check parseDsnWith(
        "service=a", fakeEnv([userDirVar(dir / "nohome"), ("PGSYSCONFDIR", dir)])
      ).host == "sys"

  when defined(posix):
    test "an unreadable file is an error, one that cannot be stat'ed is skipped":
      # As libpq: falling through would take the next file's same-named service.
      if geteuid() == 0:
        skip() # root reads mode 000
      else:
        withTempDir:
          writeFile(dir / "pg_service.conf", "[a]\nhost=sys\n")
          let user = dir / ".pg_service.conf"
          writeFile(user, "[a]\nhost=user\n")
          setFilePermissions(user, {})
          check parseError("service=a", fakeEnv({"HOME": dir, "PGSYSCONFDIR": dir})) ==
            "Cannot read service file: " & user
          check parseError("service=a", fakeEnv({"PGSERVICEFILE": user})) ==
            "Cannot read service file: " & user
          let locked = dir / "locked"
          createDir(locked)
          writeFile(locked / ".pg_service.conf", "[a]\nhost=user\n")
          setFilePermissions(locked, {})
          try:
            check parseDsnWith(
              "service=a", fakeEnv({"HOME": locked, "PGSYSCONFDIR": dir})
            ).host == "sys"
          finally:
            setFilePermissions(locked, {fpUserRead, fpUserWrite, fpUserExec})

  when defined(posix):
    test "a directory PGSERVICEFILE is rejected, not skipped":
      # Stricter than libpq, which reads nothing from a directory and falls
      # through to the system file.
      withTempDir:
        check parseError("service=a", fakeEnv({"PGSERVICEFILE": dir})) ==
          "service file is not a regular file, refusing to use: " & dir

  test "an unknown service names no value":
    withTempDir:
      writeFile(dir / "s.conf", "[a]\nhost=x\n")
      let msg = parseError("service=secret", fakeEnv({"PGSERVICEFILE": dir / "s.conf"}))
      check "definition of service not found (len=6)" in msg
      check "secret" notin msg
      check parseError("service=a", DsnEnv()).endsWith(": no service file to search")

  test "the first section of a name, and within it the first value, wins":
    withTempDir:
      writeFile(dir / "s.conf", "[a]\nhost=one\nhost=two\n[a]\nport=6300\n")
      let cfg = parseDsnWith("service=a", fakeEnv({"PGSERVICEFILE": dir / "s.conf"}))
      check cfg.host == "one"
      check cfg.port == 5432

  test "errors name the file and line":
    withTempDir:
      let file = dir / "s.conf"
      for (body, want) in [
        ("[a]\nhost\n", "syntax error in service file \"" & file & "\", line 2"),
        ("[a]\n\nservice=b\n", "nested service specifications not supported"),
        ("[a]\nldap://h/x?y\n", "LDAP lookup is not supported"),
        ("[a]\nport=nope\n", "service file \"" & file & "\", line 2: "),
        ("[a]\nsslcrl=/c\n", "sslcrl is not supported"),
      ]:
        writeFile(file, body)
        check want in parseError("service=a", fakeEnv({"PGSERVICEFILE": file}))

  test "lines are trimmed, CRLF too, and a section name matches whole":
    withTempDir:
      writeFile(
        dir / "s.conf",
        "[ab]\r\nhost=wrong\r\n  [a]  \r\n\thost =  spaced \t\r\n  # c\r\nport= 6400\r\n",
      )
      let cfg = parseDsnWith("service=a", fakeEnv({"PGSERVICEFILE": dir / "s.conf"}))
      check cfg.host == "spaced"
      check cfg.port == 6400

  test "a zero byte in the section is rejected, elsewhere ignored":
    withTempDir:
      let file = dir / "s.conf"
      writeFile(file, "[x]\nhost=a\0b\n[a]\nhost=ok\n")
      check parseDsnWith("service=a", fakeEnv({"PGSERVICEFILE": file})).host == "ok"
      writeFile(file, "[a]\nhost=a\0b\n")
      check "Forbidden zero byte in service file \"" & file & "\", line 2" in
        parseError("service=a", fakeEnv({"PGSERVICEFILE": file}))

  test "lines outside the section are not checked":
    withTempDir:
      writeFile(
        dir / "s.conf", "junk before any section\n[x]\nnot a pair\n[a]\nhost=ok\n"
      )
      check parseDsnWith("service=a", fakeEnv({"PGSERVICEFILE": dir / "s.conf"})).host ==
        "ok"

  test "a fault in the string is not blamed on the service's other list":
    withTempDir:
      let file = dir / "s.conf"
      writeFile(file, "[a]\nhost=svchost\nport=6543\n")
      let env = fakeEnv({"PGSERVICEFILE": file})
      # The empty host is the string's, though the service supplied the port.
      check parseError("service=a host=", env) == "Empty host in DSN"
      # The bad port is the string's, though the service supplied the host.
      check parseError("service=a port=nope", env) == "Invalid port in DSN (len=4)"
      # A fault in the service's own list is still named.
      writeFile(file, "[a]\nhost=a,\n")
      check parseError("service=a", fakeEnv({"PGSERVICEFILE": file})) ==
        "service file \"" & file & "\", line 2: Empty host in DSN host list (element #1)"
