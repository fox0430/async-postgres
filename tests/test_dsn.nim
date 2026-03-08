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

  test "error: sslrootcert file not found":
    expect PgError:
      discard parseDsn("postgresql://host/db?sslrootcert=/nonexistent/file.pem")
