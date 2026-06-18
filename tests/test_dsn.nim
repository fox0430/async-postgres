import std/[random, unittest]

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

  test "sslmode defaults to prefer when unspecified (libpq parity)":
    check parseDsn("postgresql://host/db").sslMode == sslPrefer

  test "query param sslmode":
    for mode in ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]:
      let cfg = parseDsn("postgresql://host/db?sslmode=" & mode)
      case mode
      of "disable":
        check cfg.sslMode == sslDisable
      of "allow":
        check cfg.sslMode == sslAllow
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

  test "query param channel_binding":
    for mode in ["disable", "prefer", "require"]:
      let cfg = parseDsn("postgresql://host/db?channel_binding=" & mode)
      case mode
      of "disable":
        check cfg.channelBinding == cbDisable
      of "prefer":
        check cfg.channelBinding == cbPrefer
      of "require":
        check cfg.channelBinding == cbRequire
      else:
        discard

  test "channel_binding default is prefer":
    let cfg = parseDsn("postgresql://host/db")
    check cfg.channelBinding == cbPrefer

  test "ConnConfig zero init has cbPrefer":
    let cfg = ConnConfig()
    check cfg.channelBinding == cbPrefer

  test "error: invalid channel_binding":
    expect PgError:
      discard parseDsn("postgresql://host/db?channel_binding=bogus")

  test "require_auth default is empty set":
    let cfg = parseDsn("postgresql://host/db")
    check cfg.requireAuth == {}

  test "ConnConfig zero init has empty requireAuth":
    let cfg = ConnConfig()
    check cfg.requireAuth == {}

  test "query param require_auth single value":
    let cases = {
      "none": amNone,
      "password": amPassword,
      "md5": amMd5,
      "scram-sha-256": amScramSha256,
      "scram-sha-256-plus": amScramSha256Plus,
    }
    for (name, expected) in cases:
      let cfg = parseDsn("postgresql://host/db?require_auth=" & name)
      check cfg.requireAuth == {expected}

  test "query param require_auth comma list":
    let cfg =
      parseDsn("postgresql://host/db?require_auth=scram-sha-256,scram-sha-256-plus")
    check cfg.requireAuth == {amScramSha256, amScramSha256Plus}

  test "query param require_auth tolerates whitespace":
    let cfg = parseDsn("postgresql://host/db?require_auth=scram-sha-256,%20md5")
    check cfg.requireAuth == {amScramSha256, amMd5}

  test "keyword=value form require_auth":
    let cfg = parseDsn("host=127.0.0.1 require_auth=md5,password")
    check cfg.requireAuth == {amMd5, amPassword}

  test "error: unknown require_auth method":
    expect PgError:
      discard parseDsn("postgresql://host/db?require_auth=sha1")

  test "error: empty entry in require_auth list":
    expect PgError:
      discard parseDsn("postgresql://host/db?require_auth=md5,,password")

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

  test "max_message_size from URI DSN":
    let cfg = parseDsn("postgresql://host/db?max_message_size=1048576")
    check cfg.maxMessageSize == 1048576

  test "max_message_size from keyword DSN":
    let cfg = parseDsn("host=h dbname=d max_message_size=2097152")
    check cfg.maxMessageSize == 2097152

  test "max_message_size default is zero (use library default)":
    let cfg = parseDsn("postgresql://host/db")
    check cfg.maxMessageSize == 0

  test "error: invalid max_message_size value":
    expect PgError:
      discard parseDsn("postgresql://host/db?max_message_size=abc")

  test "error: negative max_message_size":
    expect PgError:
      discard parseDsn("postgresql://host/db?max_message_size=-1")

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

  test "multi-host empty entry selects default (libpq parity)":
    let cfg = parseDsn("postgresql://h1,,h3/db")
    check cfg.hosts.len == 3
    check cfg.hosts[0].host == "h1"
    check cfg.hosts[1].host == "127.0.0.1"
    check cfg.hosts[1].port == 5432
    check cfg.hosts[2].host == "h3"

  test "host and port as query parameters (libpq documented form)":
    let cfg = parseDsn("postgresql:///mydb?host=localhost&port=5433")
    check cfg.hosts.len == 1
    check cfg.hosts[0].host == "localhost"
    check cfg.hosts[0].port == 5433
    check cfg.host == "localhost"
    check cfg.port == 5433
    check cfg.database == "mydb"

  test "multi-host via query parameters":
    let cfg = parseDsn("postgresql:///db?host=h1,h2&port=5433,5434")
    check cfg.hosts.len == 2
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5433)
    check cfg.hosts[1] == HostEntry(host: "h2", port: 5434)

  test "query parameter host overrides authority host (last wins)":
    let cfg = parseDsn("postgresql://ignored:9999/db?host=real&port=5433")
    check cfg.hosts.len == 1
    check cfg.hosts[0] == HostEntry(host: "real", port: 5433)

  test "query parameter port applies to all authority hosts":
    let cfg = parseDsn("postgresql://h1,h2/db?port=5433")
    check cfg.hosts.len == 2
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5433)
    check cfg.hosts[1] == HostEntry(host: "h2", port: 5433)

  test "error: URI port count mismatch via query parameters":
    expect PgError:
      discard parseDsn("postgresql:///db?host=h1,h2,h3&port=5433,5434")

  test "hostaddr query parameter kept separate from authority host":
    let cfg = parseDsn("postgresql://db.example.com/db?hostaddr=10.0.0.1")
    check cfg.hosts.len == 1
    check cfg.hosts[0].host == "db.example.com"
    check cfg.hosts[0].hostaddr == "10.0.0.1"
    check cfg.hosts[0].dialAddr == "10.0.0.1"
    check cfg.host == "db.example.com"
    check cfg.hostaddr == "10.0.0.1"

  test "error: host names and hostaddr values count mismatch (URI)":
    expect PgError:
      discard parseDsn("postgresql://h1,h2/db?hostaddr=10.0.0.1")

  test "error: unexpected character after IPv6 bracket":
    expect PgError:
      discard parseDsn("postgresql://[::1]junk:5433/db")

  test "error: unbracketed IPv6 literal in DSN":
    # `::1` must be bracketed; without brackets rfind(':') would otherwise
    # silently mis-split it into host=":" port="1".
    expect PgError:
      discard parseDsn("postgresql://user:pass@::1/db")

  test "error: unbracketed IPv6 literal with trailing port in DSN":
    expect PgError:
      discard parseDsn("postgresql://user:pass@2001:db8::1:5432/db")

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

  test "load_balance_hosts all values":
    check parseDsn("postgresql://h/db?load_balance_hosts=disable").loadBalanceHosts ==
      lbhDisable
    check parseDsn("postgresql://h/db?load_balance_hosts=random").loadBalanceHosts ==
      lbhRandom

  test "load_balance_hosts defaults to disable":
    check parseDsn("postgresql://h1,h2/db").loadBalanceHosts == lbhDisable

  test "load_balance_hosts keyword=value form":
    check parseDsn("host=h1,h2 load_balance_hosts=random").loadBalanceHosts == lbhRandom

  test "error: invalid load_balance_hosts":
    expect PgError:
      discard parseDsn("postgresql://h/db?load_balance_hosts=bogus")

  test "orderedHosts: lbhDisable preserves configured order":
    let cfg = parseDsn("postgresql://h1,h2,h3/db?load_balance_hosts=disable")
    check cfg.orderedHosts() == cfg.getHosts()

  test "orderedHosts: lbhRandom is a permutation and reorders":
    let cfg = parseDsn("postgresql://h1,h2,h3,h4,h5/db?load_balance_hosts=random")
    let base = cfg.getHosts()
    var sawReorder = false
    for _ in 0 ..< 100:
      let o = cfg.orderedHosts()
      check o.len == base.len
      for h in base: # same multiset of hosts, just reordered
        check h in o
      if o != base:
        sawReorder = true
    check sawReorder

  test "orderedHosts: single host is never shuffled under lbhRandom":
    let cfg = parseDsn("postgresql://only/db?load_balance_hosts=random")
    check cfg.orderedHosts() == cfg.getHosts()

  test "orderedHosts: exactly two hosts are eligible for shuffling":
    # Boundary of the `result.len > 1` guard: two hosts (the smallest pool that
    # can be balanced) must be reorderable, so a `> 2` off-by-one regression
    # that silently disabled load balancing for 2-host pools would be caught.
    let cfg = parseDsn("postgresql://h1,h2/db?load_balance_hosts=random")
    let base = cfg.getHosts()
    var sawReorder = false
    for _ in 0 ..< 100:
      let o = cfg.orderedHosts()
      check o.len == 2
      for h in base:
        check h in o
      if o != base:
        sawReorder = true
    check sawReorder

  test "orderedHosts: lbhRandom is independent of the global RNG seed":
    # The shuffle must come from a local RNG seeded by the OS entropy source,
    # not std/random's global RNG. Pinning the global RNG to the same seed
    # before every call must NOT make the order reproducible: a buggy
    # implementation that shuffled via the global RNG would return an identical
    # order each time the global seed was reset to the same value.
    let cfg = parseDsn("postgresql://h1,h2,h3,h4,h5/db?load_balance_hosts=random")
    let base = cfg.getHosts()
    randomize(123456789)
    let first = cfg.orderedHosts()
    var sawDifferent = false
    for _ in 0 ..< 50:
      randomize(123456789) # reset the global RNG to an identical state each time
      let o = cfg.orderedHosts()
      check o.len == base.len
      for h in base:
        check h in o
      if o != first:
        sawDifferent = true
    # With a global-RNG implementation every `o` would equal `first`;
    # independence means we still observe variation despite the fixed seed.
    check sawDifferent
    randomize() # restore a non-deterministic global RNG (avoid cross-test leak)

  # The invariant that connect() shuffles once and reuses that single order for
  # both the connect-start trace and every host attempt (including both
  # prefer-standby passes) is covered end-to-end in tests/test_tracing.nim.

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
    # No host name given: entry host stays empty (no name to verify SSL
    # against), the scalar falls back to hostaddr like libpq's PQhost().
    check cfg.hosts.len == 1
    check cfg.hosts[0].host == ""
    check cfg.hosts[0].hostaddr == "192.168.1.1"
    check cfg.hosts[0].dialAddr == "192.168.1.1"
    check cfg.hosts[0].displayHost == "192.168.1.1"
    check cfg.host == "192.168.1.1"
    check cfg.hostaddr == "192.168.1.1"
    check cfg.database == "test"

  test "host and hostaddr kept separate":
    let cfg = parseDsn("host=db.example.com hostaddr=10.0.0.1")
    check cfg.hosts.len == 1
    check cfg.hosts[0].host == "db.example.com"
    check cfg.hosts[0].hostaddr == "10.0.0.1"
    check cfg.hosts[0].dialAddr == "10.0.0.1"
    check cfg.host == "db.example.com"
    check cfg.hostaddr == "10.0.0.1"

  test "single host populates hosts":
    let cfg = parseDsn("host=myhost port=5433")
    check cfg.hosts.len == 1
    check cfg.hosts[0] == HostEntry(host: "myhost", port: 5433)

  test "multi-host comma-separated":
    let cfg = parseDsn("host=h1,h2 dbname=test")
    check cfg.hosts.len == 2
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5432)
    check cfg.hosts[1] == HostEntry(host: "h2", port: 5432)
    check cfg.host == "h1"
    check cfg.port == 5432

  test "multi-host with single port applies to all":
    let cfg = parseDsn("host=h1,h2 port=5433")
    check cfg.hosts.len == 2
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5433)
    check cfg.hosts[1] == HostEntry(host: "h2", port: 5433)

  test "multi-host with matching port list":
    let cfg = parseDsn("host=h1,h2 port=5433,5434")
    check cfg.hosts.len == 2
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5433)
    check cfg.hosts[1] == HostEntry(host: "h2", port: 5434)

  test "multi-host port order independent":
    let cfg = parseDsn("port=5433,5434 host=h1,h2")
    check cfg.hosts.len == 2
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5433)
    check cfg.hosts[1] == HostEntry(host: "h2", port: 5434)

  test "multi-host empty entry selects default":
    let cfg = parseDsn("host=h1,,h3 port=5433,,5435")
    check cfg.hosts.len == 3
    check cfg.hosts[0] == HostEntry(host: "h1", port: 5433)
    check cfg.hosts[1] == HostEntry(host: "127.0.0.1", port: 5432)
    check cfg.hosts[2] == HostEntry(host: "h3", port: 5435)

  test "multi-host hostaddr comma-separated":
    let cfg = parseDsn("hostaddr=10.0.0.1,10.0.0.2")
    check cfg.hosts.len == 2
    check cfg.hosts[0].hostaddr == "10.0.0.1"
    check cfg.hosts[1].hostaddr == "10.0.0.2"
    check cfg.hosts[0].dialAddr == "10.0.0.1"
    check cfg.hosts[1].dialAddr == "10.0.0.2"

  test "multi-host with matching hostaddr list":
    let cfg = parseDsn("host=h1,h2 hostaddr=10.0.0.1,10.0.0.2 port=5433")
    check cfg.hosts.len == 2
    check cfg.hosts[0] == HostEntry(host: "h1", hostaddr: "10.0.0.1", port: 5433)
    check cfg.hosts[1] == HostEntry(host: "h2", hostaddr: "10.0.0.2", port: 5433)

  test "error: host names and hostaddr values count mismatch":
    expect PgError:
      discard parseDsn("host=h1,h2 hostaddr=10.0.0.1")

  test "error: port count mismatch with hosts":
    expect PgError:
      discard parseDsn("host=h1,h2,h3 port=5433,5434")

  test "error: multiple ports with single host":
    expect PgError:
      discard parseDsn("host=h1 port=5433,5434")

  test "error: multiple ports without host":
    expect PgError:
      discard parseDsn("port=5433,5434")

  test "error: invalid port inside port list":
    expect PgError:
      discard parseDsn("host=h1,h2 port=5433,bogus")

  test "sslmode defaults to prefer when unspecified (libpq parity)":
    let cfg = parseDsn("host=h dbname=d")
    check cfg.sslMode == sslPrefer

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

  test "channel_binding parameter":
    let cfg = parseDsn("host=h channel_binding=require")
    check cfg.channelBinding == cbRequire

  test "error: invalid channel_binding":
    expect PgError:
      discard parseDsn("host=h channel_binding=bogus")

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
