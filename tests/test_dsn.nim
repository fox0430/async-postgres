import std/unittest

import ../async_postgres/[async_backend, pg_connection]

suite "parseDsn":
  test "full DSN":
    let cfg = parseDsn("postgresql://myuser:mypass@dbhost:5433/mydb")
    check cfg.user == "myuser"
    check cfg.password == "mypass"
    check cfg.host == "dbhost"
    check cfg.port == 5433
    check cfg.database == "mydb"

  test "postgres:// scheme":
    let cfg = parseDsn("postgres://u:p@h:1234/d")
    check cfg.user == "u"
    check cfg.password == "p"
    check cfg.host == "h"
    check cfg.port == 1234
    check cfg.database == "d"

  test "minimal DSN - scheme only":
    let cfg = parseDsn("postgresql://")
    check cfg.host == "127.0.0.1"
    check cfg.port == 5432
    check cfg.user == ""
    check cfg.database == ""

  test "host only":
    let cfg = parseDsn("postgresql://localhost")
    check cfg.host == "localhost"
    check cfg.port == 5432
    check cfg.user == ""

  test "host and port":
    let cfg = parseDsn("postgresql://localhost:9999")
    check cfg.host == "localhost"
    check cfg.port == 9999

  test "host and database":
    let cfg = parseDsn("postgresql://localhost/testdb")
    check cfg.host == "localhost"
    check cfg.port == 5432
    check cfg.database == "testdb"

  test "user only":
    let cfg = parseDsn("postgresql://admin@localhost")
    check cfg.user == "admin"
    check cfg.password == ""
    check cfg.host == "localhost"

  test "user and password":
    let cfg = parseDsn("postgresql://admin:secret@localhost")
    check cfg.user == "admin"
    check cfg.password == "secret"

  test "database with no host":
    let cfg = parseDsn("postgresql:///mydb")
    check cfg.host == "127.0.0.1"
    check cfg.port == 5432
    check cfg.database == "mydb"

  test "URL-encoded password with special chars":
    let cfg = parseDsn("postgresql://user:p%40ss%3Aw%2Fd@host/db")
    check cfg.user == "user"
    check cfg.password == "p@ss:w/d"
    check cfg.host == "host"
    check cfg.database == "db"

  test "URL-encoded user":
    let cfg = parseDsn("postgresql://my%40user@host/db")
    check cfg.user == "my@user"

  test "URL-encoded database":
    let cfg = parseDsn("postgresql://host/my%2Fdb")
    check cfg.database == "my/db"

  test "query param sslmode":
    for mode in ["disable", "prefer", "require", "verify-ca", "verify-full"]:
      let cfg = parseDsn("postgresql://host/db?sslmode=" & mode)
      case mode
      of "disable":
        check cfg.sslMode == sslDisable
      of "prefer":
        check cfg.sslMode == sslPrefer
      of "require":
        check cfg.sslMode == sslRequire
      of "verify-ca":
        check cfg.sslMode == sslVerifyCa
      of "verify-full":
        check cfg.sslMode == sslVerifyFull
      else:
        discard

  test "query param application_name":
    let cfg = parseDsn("postgresql://host/db?application_name=myapp")
    check cfg.applicationName == "myapp"

  test "query param connect_timeout":
    let cfg = parseDsn("postgresql://host/db?connect_timeout=30")
    check cfg.connectTimeout == seconds(30)

  test "unknown query params go to extraParams":
    let cfg = parseDsn(
      "postgresql://host/db?search_path=public&options=-c%20log_statement%3Dall"
    )
    check cfg.extraParams.len == 2
    check cfg.extraParams[0] == ("search_path", "public")
    check cfg.extraParams[1] == ("options", "-c log_statement=all")

  test "multiple query params":
    let cfg = parseDsn(
      "postgresql://u:p@h:5433/d?sslmode=require&application_name=test&connect_timeout=10"
    )
    check cfg.user == "u"
    check cfg.password == "p"
    check cfg.host == "h"
    check cfg.port == 5433
    check cfg.database == "d"
    check cfg.sslMode == sslRequire
    check cfg.applicationName == "test"
    check cfg.connectTimeout == seconds(10)

  test "IPv6 host":
    let cfg = parseDsn("postgresql://user:pass@[::1]:5433/db")
    check cfg.host == "::1"
    check cfg.port == 5433
    check cfg.user == "user"
    check cfg.database == "db"

  test "IPv6 host default port":
    let cfg = parseDsn("postgresql://[::1]/db")
    check cfg.host == "::1"
    check cfg.port == 5432
    check cfg.database == "db"

  test "error: invalid scheme":
    expect PgError:
      discard parseDsn("mysql://host/db")

  test "error: invalid port":
    expect PgError:
      discard parseDsn("postgresql://host:notaport/db")

  test "error: invalid sslmode":
    expect PgError:
      discard parseDsn("postgresql://host/db?sslmode=bogus")

  test "error: invalid connect_timeout":
    expect PgError:
      discard parseDsn("postgresql://host/db?connect_timeout=abc")

  test "password with @ sign":
    let cfg = parseDsn("postgresql://user:p%40ssword@host/db")
    check cfg.password == "p@ssword"

  test "empty password":
    let cfg = parseDsn("postgresql://user:@host/db")
    check cfg.user == "user"
    check cfg.password == ""

  test "user and password with no host":
    let cfg = parseDsn("postgresql://user:pass@/db")
    check cfg.user == "user"
    check cfg.password == "pass"
    check cfg.host == "127.0.0.1"
    check cfg.port == 5432
    check cfg.database == "db"

  test "error: port out of range":
    expect PgError:
      discard parseDsn("postgresql://host:0/db")
    expect PgError:
      discard parseDsn("postgresql://host:65536/db")
    expect PgError:
      discard parseDsn("postgresql://host:-1/db")

  test "keepalive default enabled via parseDsn":
    let cfg = parseDsn("postgresql://host/db")
    check cfg.keepAlive == true

  test "keepalives=0 disables keepalive":
    let cfg = parseDsn("postgresql://host/db?keepalives=0")
    check cfg.keepAlive == false

  test "keepalives=1 enables keepalive":
    let cfg = parseDsn("postgresql://host/db?keepalives=1")
    check cfg.keepAlive == true

  test "keepalive timing parameters":
    let cfg = parseDsn(
      "postgresql://host/db?keepalives_idle=60&keepalives_interval=10&keepalives_count=5"
    )
    check cfg.keepAliveIdle == 60
    check cfg.keepAliveInterval == 10
    check cfg.keepAliveCount == 5

  test "keepalives not in DSN leaves keepAlive false":
    var cfg = ConnConfig()
    check cfg.keepAlive == false

  test "keepalives with other params":
    let cfg = parseDsn(
      "postgresql://host/db?keepalives=1&keepalives_idle=30&application_name=test"
    )
    check cfg.keepAlive == true
    check cfg.keepAliveIdle == 30
    check cfg.applicationName == "test"

  test "keepalive defaults are zero":
    let cfg = parseDsn("postgresql://host/db")
    check cfg.keepAliveIdle == 0
    check cfg.keepAliveInterval == 0
    check cfg.keepAliveCount == 0

  test "keepalives nonzero values are truthy (libpq compat)":
    check parseDsn("postgresql://host/db?keepalives=2").keepAlive == true
    check parseDsn("postgresql://host/db?keepalives=-1").keepAlive == true
    check parseDsn("postgresql://host/db?keepalives=999").keepAlive == true

  test "keepalives=0 with timing params still disables":
    let cfg = parseDsn(
      "postgresql://host/db?keepalives_idle=60&keepalives=0&keepalives_interval=10"
    )
    check cfg.keepAlive == false
    check cfg.keepAliveIdle == 60
    check cfg.keepAliveInterval == 10

  test "partial keepalive timing (idle only)":
    let cfg = parseDsn("postgresql://host/db?keepalives_idle=120")
    check cfg.keepAlive == true
    check cfg.keepAliveIdle == 120
    check cfg.keepAliveInterval == 0
    check cfg.keepAliveCount == 0

  test "error: invalid keepalives value":
    expect PgError:
      discard parseDsn("postgresql://host/db?keepalives=abc")

  test "error: invalid keepalives_idle value":
    expect PgError:
      discard parseDsn("postgresql://host/db?keepalives_idle=abc")

  test "error: invalid keepalives_interval value":
    expect PgError:
      discard parseDsn("postgresql://host/db?keepalives_interval=abc")

  test "error: invalid keepalives_count value":
    expect PgError:
      discard parseDsn("postgresql://host/db?keepalives_count=abc")

  test "error: negative keepalives_idle":
    expect PgError:
      discard parseDsn("postgresql://host/db?keepalives_idle=-1")

  test "error: negative keepalives_interval":
    expect PgError:
      discard parseDsn("postgresql://host/db?keepalives_interval=-1")

  test "error: negative keepalives_count":
    expect PgError:
      discard parseDsn("postgresql://host/db?keepalives_count=-1")

  test "error: sslrootcert file not found":
    expect PgError:
      discard parseDsn("postgresql://host/db?sslrootcert=/nonexistent/file.pem")

  test "multi-host DSN":
    let cfg = parseDsn("postgresql://h1:5432,h2:5433/db")
    check cfg.hosts.len == 2
    check cfg.hosts[0].host == "h1"
    check cfg.hosts[0].port == 5432
    check cfg.hosts[1].host == "h2"
    check cfg.hosts[1].port == 5433
    check cfg.host == "h1"
    check cfg.port == 5432
    check cfg.database == "db"

  test "multi-host with IPv6":
    let cfg = parseDsn("postgresql://[::1]:5432,h2/db")
    check cfg.hosts.len == 2
    check cfg.hosts[0].host == "::1"
    check cfg.hosts[0].port == 5432
    check cfg.hosts[1].host == "h2"
    check cfg.hosts[1].port == 5432
    check cfg.database == "db"

  test "multi-host with userinfo":
    let cfg = parseDsn("postgresql://user:pass@h1,h2/db")
    check cfg.user == "user"
    check cfg.password == "pass"
    check cfg.hosts.len == 2
    check cfg.hosts[0].host == "h1"
    check cfg.hosts[1].host == "h2"
    check cfg.database == "db"

  test "multi-host mixed ports":
    let cfg = parseDsn("postgresql://h1,h2:5433/db")
    check cfg.hosts.len == 2
    check cfg.hosts[0].host == "h1"
    check cfg.hosts[0].port == 5432
    check cfg.hosts[1].host == "h2"
    check cfg.hosts[1].port == 5433

  test "multi-host skips empty entries":
    let cfg = parseDsn("postgresql://h1,,h3/db")
    check cfg.hosts.len == 2
    check cfg.hosts[0].host == "h1"
    check cfg.hosts[1].host == "h3"

  test "target_session_attrs all values":
    check parseDsn("postgresql://h/db?target_session_attrs=any").targetSessionAttrs ==
      tsaAny
    check parseDsn("postgresql://h/db?target_session_attrs=read-write").targetSessionAttrs ==
      tsaReadWrite
    check parseDsn("postgresql://h/db?target_session_attrs=read-only").targetSessionAttrs ==
      tsaReadOnly
    check parseDsn("postgresql://h/db?target_session_attrs=primary").targetSessionAttrs ==
      tsaPrimary
    check parseDsn("postgresql://h/db?target_session_attrs=standby").targetSessionAttrs ==
      tsaStandby
    check parseDsn("postgresql://h/db?target_session_attrs=prefer-standby").targetSessionAttrs ==
      tsaPreferStandby

  test "error: invalid target_session_attrs":
    expect PgError:
      discard parseDsn("postgresql://h/db?target_session_attrs=bogus")

  test "single host backward compat - hosts has one entry":
    let cfg = parseDsn("postgresql://myhost:5433/db")
    check cfg.hosts.len == 1
    check cfg.hosts[0].host == "myhost"
    check cfg.hosts[0].port == 5433
    check cfg.host == "myhost"
    check cfg.port == 5433

  test "getHosts with populated hosts":
    let cfg = parseDsn("postgresql://h1,h2:5433/db")
    let hosts = cfg.getHosts()
    check hosts.len == 2
    check hosts[0].host == "h1"
    check hosts[1].host == "h2"
    check hosts[1].port == 5433

  test "getHosts with empty hosts falls back to host/port":
    var cfg = ConnConfig(host: "myhost", port: 9999)
    let hosts = cfg.getHosts()
    check hosts.len == 1
    check hosts[0].host == "myhost"
    check hosts[0].port == 9999

  test "getHosts with zero port defaults to 5432":
    var cfg = ConnConfig(host: "myhost", port: 0)
    let hosts = cfg.getHosts()
    check hosts[0].port == 5432

  test "multi-host with target_session_attrs":
    let cfg = parseDsn("postgresql://h1,h2,h3/db?target_session_attrs=read-write")
    check cfg.hosts.len == 3
    check cfg.targetSessionAttrs == tsaReadWrite

  test "three hosts":
    let cfg = parseDsn("postgresql://h1:5432,h2:5433,h3/db")
    check cfg.hosts.len == 3
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5432)
    check cfg.hosts[1] == HostEntry(host: "h2", port: 5433)
    check cfg.hosts[2] == HostEntry(host: "h3", port: 5432)

suite "parseDsn keyword=value":
  test "full connection string":
    let cfg = parseDsn("host=dbhost port=5433 dbname=mydb user=myuser password=mypass")
    check cfg.host == "dbhost"
    check cfg.port == 5433
    check cfg.database == "mydb"
    check cfg.user == "myuser"
    check cfg.password == "mypass"

  test "minimal - host only":
    let cfg = parseDsn("host=localhost")
    check cfg.host == "localhost"
    check cfg.port == 5432
    check cfg.user == ""
    check cfg.database == ""

  test "empty string defaults":
    let cfg = parseDsn("")
    check cfg.host == "127.0.0.1"
    check cfg.port == 5432

  test "single-quoted value with spaces":
    let cfg = parseDsn("application_name='my app'")
    check cfg.applicationName == "my app"

  test "single-quoted value with escaped quote":
    let cfg = parseDsn(r"password='it\'s secret'")
    check cfg.password == "it's secret"

  test "single-quoted value with escaped backslash":
    let cfg = parseDsn(r"password='back\\slash'")
    check cfg.password == "back\\slash"

  test "hostaddr keyword":
    let cfg = parseDsn("hostaddr=192.168.1.1 dbname=test")
    check cfg.host == "192.168.1.1"
    check cfg.database == "test"

  test "sslmode parameter":
    let cfg = parseDsn("host=h dbname=d sslmode=require")
    check cfg.sslMode == sslRequire

  test "connect_timeout parameter":
    let cfg = parseDsn("host=h connect_timeout=30")
    check cfg.connectTimeout == seconds(30)

  test "keepalive parameters":
    let cfg = parseDsn(
      "host=h keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5"
    )
    check cfg.keepAlive == true
    check cfg.keepAliveIdle == 60
    check cfg.keepAliveInterval == 10
    check cfg.keepAliveCount == 5

  test "keepalives=0 disables keepalive":
    let cfg = parseDsn("host=h keepalives=0")
    check cfg.keepAlive == false

  test "unknown params go to extraParams":
    let cfg = parseDsn("host=h search_path=public options='-c log'")
    check cfg.extraParams.len == 2
    check cfg.extraParams[0] == ("search_path", "public")
    check cfg.extraParams[1] == ("options", "-c log")

  test "spaces around equals sign":
    let cfg = parseDsn("host = localhost port = 5433 dbname = test")
    check cfg.host == "localhost"
    check cfg.port == 5433
    check cfg.database == "test"

  test "extra whitespace between pairs":
    let cfg = parseDsn("  host=localhost   port=5432   dbname=test  ")
    check cfg.host == "localhost"
    check cfg.port == 5432
    check cfg.database == "test"

  test "error: invalid port":
    expect PgError:
      discard parseDsn("host=h port=notaport")

  test "error: port out of range":
    expect PgError:
      discard parseDsn("host=h port=0")
    expect PgError:
      discard parseDsn("host=h port=65536")

  test "error: invalid sslmode":
    expect PgError:
      discard parseDsn("host=h sslmode=bogus")

  test "error: invalid connect_timeout":
    expect PgError:
      discard parseDsn("host=h connect_timeout=abc")

  test "error: invalid keepalives value":
    expect PgError:
      discard parseDsn("host=h keepalives=abc")

  test "error: unterminated quoted value":
    expect PgError:
      discard parseDsn("host=h password='unterminated")

  test "error: empty key":
    expect PgError:
      discard parseDsn("=value")

  test "URI strings still work":
    let cfg = parseDsn("postgresql://myuser:mypass@dbhost:5433/mydb")
    check cfg.user == "myuser"
    check cfg.host == "dbhost"
    check cfg.port == 5433

suite "Unix socket":
  test "isUnixSocket":
    check isUnixSocket("/var/run/postgresql") == true
    check isUnixSocket("/tmp") == true
    check isUnixSocket("localhost") == false
    check isUnixSocket("127.0.0.1") == false
    check isUnixSocket("") == false

  test "unixSocketPath":
    check unixSocketPath("/var/run/postgresql", 5432) ==
      "/var/run/postgresql/.s.PGSQL.5432"
    check unixSocketPath("/tmp", 5433) == "/tmp/.s.PGSQL.5433"

  test "key-value DSN with unix socket host":
    let cfg = parseDsn("host=/var/run/postgresql port=5432 dbname=test user=myuser")
    check cfg.host == "/var/run/postgresql"
    check cfg.port == 5432
    check cfg.database == "test"
    check cfg.user == "myuser"

  test "key-value DSN with unix socket host default port":
    let cfg = parseDsn("host=/tmp dbname=test")
    check cfg.host == "/tmp"
    check cfg.port == 5432

  test "URI DSN with unix socket via query param":
    let cfg = parseDsn("postgresql:///mydb?host=/var/run/postgresql")
    check cfg.host == "/var/run/postgresql"
    check cfg.database == "mydb"
    check cfg.port == 5432
