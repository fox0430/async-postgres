## DSN parsing for PostgreSQL connection strings.
##
## Supports both libpq formats:
## - keyword=value:  ``host=localhost port=5432 dbname=test``
## - URI:            ``postgresql://user:pass@host:port/db?param=value``
##
## Re-exported through `pg_connection.nim`; depends only on `types.nim`
## (in particular, does not touch `PgConnection`).

import std/strutils

import ../[async_backend, pg_errors]
import types

proc parseSslMode*(s: string): SslMode =
  case s
  of "disable":
    sslDisable
  of "allow":
    sslAllow
  of "prefer":
    sslPrefer
  of "require":
    sslRequire
  of "verify-ca":
    sslVerifyCa
  of "verify-full":
    sslVerifyFull
  else:
    raise newException(PgError, "Invalid sslmode: " & s)

proc parseChannelBindingMode*(s: string): ChannelBindingMode =
  case s
  of "disable":
    cbDisable
  of "prefer":
    cbPrefer
  of "require":
    cbRequire
  else:
    raise newException(PgError, "Invalid channel_binding: " & s)

proc parseAuthMethod*(s: string): AuthMethod =
  case s
  of "none":
    amNone
  of "password":
    amPassword
  of "md5":
    amMd5
  of "scram-sha-256":
    amScramSha256
  of "scram-sha-256-plus":
    amScramSha256Plus
  else:
    raise newException(PgError, "Invalid require_auth method: " & s)

proc parseRequireAuth*(s: string): set[AuthMethod] =
  ## Parse a comma-separated list of auth method names into a set
  ## (libpq `require_auth` syntax; negation prefix `!` is not yet supported).
  ## Empty input returns the empty set (allow any).
  if s.len == 0:
    return {}
  for raw in s.split(','):
    let tok = raw.strip()
    if tok.len == 0:
      raise newException(PgError, "Empty entry in require_auth list: " & s)
    result.incl(parseAuthMethod(tok))

proc parseTargetSessionAttrs*(s: string): TargetSessionAttrs =
  case s
  of "any":
    tsaAny
  of "read-write":
    tsaReadWrite
  of "read-only":
    tsaReadOnly
  of "primary":
    tsaPrimary
  of "standby":
    tsaStandby
  of "prefer-standby":
    tsaPreferStandby
  else:
    raise newException(PgError, "Invalid target_session_attrs: " & s)

proc parseLoadBalanceHosts*(s: string): LoadBalanceHosts =
  case s
  of "disable":
    lbhDisable
  of "random":
    lbhRandom
  else:
    raise newException(PgError, "Invalid load_balance_hosts: " & s)

proc parsePort*(s: string): int =
  ## Follows libpq's strtol-based rules: surrounding whitespace and a leading
  ## sign are accepted (libpq takes `+5432` too), digit-group underscores are
  ## not, and the final value must be in 1–65535.
  let t = s.strip()
  if t.find('_') >= 0:
    raise newException(PgError, "Invalid port in DSN: " & s)
  try:
    result = parseInt(t)
  except ValueError:
    raise newException(PgError, "Invalid port in DSN: " & s)
  if result < 1 or result > 65535:
    raise newException(PgError, "Port out of range (1-65535): " & s)

proc splitList(s: string): seq[string] =
  ## Comma-split a multi-host parameter; an empty string means "not provided".
  if s.len == 0:
    @[]
  else:
    s.split(',')

proc buildHosts(hostList, addrList, portList: seq[string]): seq[HostEntry] =
  ## Expand host/hostaddr/port lists into HostEntry values following libpq
  ## multi-host rules: the number of entries is set by `hostaddr` when given,
  ## else by `host` (an empty seq means the parameter was not provided; when
  ## both are given their lengths must match exactly). The port list must
  ## contain either a single entry (applied to every host) or exactly one
  ## entry per host. An empty list entry selects the default (127.0.0.1 /
  ## 5432); an empty host with a hostaddr stays empty, so SSL verification
  ## can require an explicit name.
  if hostList.len > 0 and addrList.len > 0 and hostList.len != addrList.len:
    raise newException(
      PgError,
      "Could not match " & $hostList.len & " host names to " & $addrList.len &
        " hostaddr values",
    )
  let count =
    if addrList.len > 0:
      addrList.len
    elif hostList.len > 0:
      hostList.len
    else:
      1
  let ports =
    if portList.len == 0:
      @[""]
    else:
      portList
  if ports.len != 1 and ports.len != count:
    raise newException(
      PgError, "Could not match " & $ports.len & " port numbers to " & $count & " hosts"
    )
  for i in 0 ..< count:
    let h =
      if hostList.len > 0:
        hostList[i]
      else:
        ""
    let a =
      if addrList.len > 0:
        addrList[i]
      else:
        ""
    let p =
      if ports.len == 1:
        ports[0]
      else:
        ports[i]
    result.add HostEntry(
      host: if h.len == 0 and a.len == 0: "127.0.0.1" else: h,
      hostaddr: a,
      port:
        if p.len == 0:
          5432
        else:
          parsePort(p),
    )

proc rawHost(host, hostaddr: string): string =
  ## Inverse of `buildHosts`' defaulting: 127.0.0.1 with no hostaddr can only
  ## come from an entry where neither was provided, so fold it back to "".
  ## An explicit `host=127.0.0.1` is structurally indistinguishable from the
  ## implicit default, but re-defaults to the same value on rebuild —
  ## functionally correct for the round-trip.
  if host == "127.0.0.1" and hostaddr.len == 0: "" else: host

proc rawHostLists(c: ConnConfig): tuple[hosts, addrs, ports: seq[string]] =
  ## Re-derive raw multi-host lists from a config so `applyParam` can rebuild
  ## `hosts` with one list replaced. Best-effort inverse of `buildHosts`:
  ## all-empty lists collapse to "not provided" and an all-equal port list to
  ## a single entry.
  ##
  ## The port collapsing is required for correctness: if the replacement list
  ## in `applyParam` has a different length than the current hosts, a
  ## collapsed single-entry port list "applies to all" via `buildHosts`,
  ## whereas a length-specific list would be rejected as mismatched.
  if c.hosts.len == 0:
    result.hosts = splitList(rawHost(c.host, c.hostaddr))
    result.addrs = splitList(c.hostaddr)
    if c.port > 0:
      result.ports = @[$c.port]
    return
  var anyHost, anyAddr = false
  var samePorts = true
  for e in c.hosts:
    let h = rawHost(e.host, e.hostaddr)
    result.hosts.add h
    result.addrs.add e.hostaddr
    result.ports.add $e.port
    anyHost = anyHost or h.len > 0
    anyAddr = anyAddr or e.hostaddr.len > 0
    samePorts = samePorts and e.port == c.hosts[0].port
  if not anyHost:
    result.hosts = @[]
  if not anyAddr:
    result.addrs = @[]
  if samePorts:
    result.ports = @[result.ports[0]]

proc applyParam*(result: var ConnConfig, key, val: string) =
  ## Apply a single connection parameter to a ConnConfig.
  ##
  ## `host`/`hostaddr`/`port` accept comma-separated multi-host lists and
  ## rebuild `hosts` on each call. The DSN parsers still intercept these
  ## keys: there all values are collected first and expanded once, so their
  ## correlation is order-independent, while repeated `applyParam` calls
  ## correlate each list against the already-expanded state.
  case key
  of "host", "hostaddr", "port":
    # Rebuild `hosts` so it stays consistent with the scalar view
    # (`getHosts` prefers `hosts` when non-empty); the two untouched
    # lists are re-derived from the current config.
    var (hostList, addrList, portList) = result.rawHostLists()
    case key
    of "host":
      hostList = splitList(val)
    of "hostaddr":
      addrList = splitList(val)
    else:
      portList = splitList(val)
    result.hosts = buildHosts(hostList, addrList, portList)
    result.host = result.hosts[0].displayHost
    result.hostaddr = result.hosts[0].hostaddr
    result.port = result.hosts[0].port
  of "dbname":
    result.database = val
  of "user":
    result.user = val
  of "password":
    result.password = val
  of "sslmode":
    result.sslMode = parseSslMode(val)
  of "channel_binding":
    result.channelBinding = parseChannelBindingMode(val)
  of "require_auth":
    result.requireAuth = parseRequireAuth(val)
  of "application_name":
    result.applicationName = val
  of "connect_timeout":
    var secs: int
    try:
      secs = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid connect_timeout: " & val)
    # libpq treats a zero or negative connect_timeout as "wait indefinitely";
    # ZeroDuration is this codebase's "no timeout" sentinel. Mapping <= 0 here
    # avoids building a negative Duration, which would make `wait` time out
    # every host immediately — the exact opposite of libpq's behavior.
    result.connectTimeout =
      if secs <= 0:
        ZeroDuration
      else:
        seconds(secs)
  of "sslrootcert":
    try:
      result.sslRootCert = readFile(val)
    except IOError:
      raise newException(PgError, "Cannot read sslrootcert file: " & val)
  of "sslsni":
    try:
      result.sslSni = parseInt(val) != 0
    except ValueError:
      raise newException(PgError, "Invalid sslsni: " & val)
  of "keepalives":
    try:
      result.keepAlive = parseInt(val) != 0
    except ValueError:
      raise newException(PgError, "Invalid keepalives: " & val)
  of "keepalives_idle":
    try:
      result.keepAliveIdle = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid keepalives_idle: " & val)
    if result.keepAliveIdle < 0:
      raise newException(PgError, "keepalives_idle must be non-negative: " & val)
  of "keepalives_interval":
    try:
      result.keepAliveInterval = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid keepalives_interval: " & val)
    if result.keepAliveInterval < 0:
      raise newException(PgError, "keepalives_interval must be non-negative: " & val)
  of "keepalives_count":
    try:
      result.keepAliveCount = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid keepalives_count: " & val)
    if result.keepAliveCount < 0:
      raise newException(PgError, "keepalives_count must be non-negative: " & val)
  of "target_session_attrs":
    result.targetSessionAttrs = parseTargetSessionAttrs(val)
  of "load_balance_hosts":
    result.loadBalanceHosts = parseLoadBalanceHosts(val)
  of "max_message_size":
    try:
      result.maxMessageSize = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid max_message_size: " & val)
    if result.maxMessageSize < 0:
      raise newException(PgError, "max_message_size must be non-negative: " & val)
  of "max_scram_iterations":
    try:
      result.maxScramIterations = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid max_scram_iterations: " & val)
    if result.maxScramIterations < 0:
      raise newException(PgError, "max_scram_iterations must be non-negative: " & val)
  else:
    result.extraParams.add((key, val))

proc parseKeyValueDsn*(dsn: string): ConnConfig =
  ## Parse a libpq keyword=value connection string into a ConnConfig.
  ##
  ## Format: ``host=localhost port=5432 dbname=test user=myuser``
  ##
  ## Values may be single-quoted: ``password='has spaces'``
  ## As in libpq, a backslash escapes the following character both inside
  ## and outside quotes (``\'``, ``\\``, ``host=foo\ bar``).
  ##
  ## `host`, `hostaddr`, and `port` accept comma-separated lists for
  ## multi-host failover (libpq compatible): ``host=h1,h2 port=5433,5434``.
  result.keepAlive = true
  result.sslMode = sslPrefer # libpq default; overridden by an explicit sslmode
  result.sslSni = true # libpq default

  # Tokenize into (key, value) pairs
  var pairs: seq[(string, string)]
  var i = 0
  while i < dsn.len:
    # Skip whitespace
    while i < dsn.len and dsn[i] in {' ', '\t', '\n', '\r'}:
      inc i
    if i >= dsn.len:
      break

    # Read key
    var key = ""
    while i < dsn.len and dsn[i] notin {'=', ' ', '\t', '\n', '\r'}:
      key.add dsn[i]
      inc i
    if key.len == 0:
      raise newException(PgError, "Empty key in connection string")

    # Skip whitespace around '='
    while i < dsn.len and dsn[i] in {' ', '\t'}:
      inc i
    if i >= dsn.len or dsn[i] != '=':
      raise newException(
        PgError, "Expected '=' after key '" & key & "' in connection string"
      )
    inc i # skip '='
    while i < dsn.len and dsn[i] in {' ', '\t'}:
      inc i

    # Read value
    var val = ""
    if i < dsn.len and dsn[i] == '\'':
      # Quoted value
      inc i # skip opening quote
      var closed = false
      while i < dsn.len:
        if dsn[i] == '\\' and i + 1 < dsn.len:
          # Escape sequence
          val.add dsn[i + 1]
          i += 2
        elif dsn[i] == '\'':
          inc i # skip closing quote
          closed = true
          break
        else:
          val.add dsn[i]
          inc i
      if not closed:
        raise newException(PgError, "Unterminated quoted value for key '" & key & "'")
    else:
      # Unquoted value; backslash escapes the next character, even
      # whitespace (libpq drops a trailing lone backslash).
      while i < dsn.len and dsn[i] notin {' ', '\t', '\n', '\r'}:
        if dsn[i] == '\\':
          inc i
          if i < dsn.len:
            val.add dsn[i]
            inc i
        else:
          val.add dsn[i]
          inc i

    pairs.add((key, val))

  # host/hostaddr/port accept comma-separated multi-host lists, which can
  # only be correlated once all parameters are seen — collect them raw and
  # expand at the end (last occurrence wins, as in libpq).
  var hostStr, hostaddrStr, portStr: string
  for (key, val) in pairs:
    case key
    of "host":
      hostStr = val
    of "hostaddr":
      hostaddrStr = val
    of "port":
      portStr = val
    else:
      result.applyParam(key, val)

  result.hosts =
    buildHosts(splitList(hostStr), splitList(hostaddrStr), splitList(portStr))
  # Back-compat: set scalar host/hostaddr/port from the first entry;
  # `host` falls back to `hostaddr` like libpq's PQhost().
  result.host = result.hosts[0].displayHost
  result.hostaddr = result.hosts[0].hostaddr
  result.port = result.hosts[0].port

proc pctDecode(s: string): string =
  ## Percent-decode a URI component following libpq rules: strict ``%XX``
  ## (a malformed sequence or an encoded zero byte is an error) and no
  ## ``+``-to-space translation.
  result = newStringOfCap(s.len)
  var i = 0
  while i < s.len:
    if s[i] == '%':
      if i + 2 >= s.len or s[i + 1] notin HexDigits or s[i + 2] notin HexDigits:
        raise newException(PgError, "Invalid percent-encoded token in DSN: " & s)
      let c = chr(parseHexInt(s[i + 1 .. i + 2]))
      if c == '\0':
        raise newException(PgError, "Forbidden zero byte in DSN: " & s)
      result.add c
      i += 3
    else:
      result.add s[i]
      inc i

proc parseUriDsn*(dsn: string): ConnConfig =
  ## Parse a PostgreSQL URI connection string into a ConnConfig.
  result.keepAlive = true
  result.sslMode = sslPrefer # libpq default; overridden by an explicit sslmode
  result.sslSni = true # libpq default
  let scheme =
    if dsn.startsWith("postgresql://"):
      "postgresql"
    elif dsn.startsWith("postgres://"):
      "postgres"
    else:
      raise newException(
        PgError, "Invalid DSN scheme: expected postgresql:// or postgres://"
      )

  # Strip scheme prefix
  let rest = dsn[scheme.len + 3 .. ^1] # skip "scheme://"

  # Split query string
  var body: string
  var queryStr: string
  let qpos = rest.find('?')
  if qpos >= 0:
    body = rest[0 ..< qpos]
    queryStr = rest[qpos + 1 .. ^1]
  else:
    body = rest

  # Split userinfo and hostpath by '@'
  var userinfo, hostpath: string
  let apos = body.rfind('@')
  if apos >= 0:
    userinfo = body[0 ..< apos]
    hostpath = body[apos + 1 .. ^1]
  else:
    hostpath = body

  # Parse user:password
  if userinfo.len > 0:
    let cpos = userinfo.find(':')
    if cpos >= 0:
      result.user = pctDecode(userinfo[0 ..< cpos])
      result.password = pctDecode(userinfo[cpos + 1 .. ^1])
    else:
      result.user = pctDecode(userinfo)

  # Parse host:port/database
  var hostport, dbpath: string
  let spos = hostpath.find('/')
  if spos >= 0:
    hostport = hostpath[0 ..< spos]
    dbpath = hostpath[spos + 1 .. ^1]
  else:
    hostport = hostpath

  if dbpath.len > 0:
    result.database = pctDecode(dbpath)

  # Parse host(s) and port(s) — supports comma-separated multi-host syntax.
  # libpq order: the authority splits on commas *before* percent-decoding
  # (an encoded comma stays inside one host), query parameters decode
  # *before* splitting. Structural separators are matched on the raw text,
  # so encoded ones never split.
  var hostList, addrList, portList: seq[string]
  if hostport.len > 0:
    for part in hostport.split(','):
      if part.startsWith("["):
        # IPv6: [::1]:5432; a zone id is encoded as %25: [fe80::1%25eth0]
        let bracket = part.find(']')
        if bracket < 0:
          raise newException(PgError, "Invalid IPv6 address in DSN")
        hostList.add pctDecode(part[1 ..< bracket])
        let afterBracket = part[bracket + 1 .. ^1]
        if afterBracket.len == 0:
          portList.add ""
        elif afterBracket.startsWith(":"):
          portList.add pctDecode(afterBracket[1 .. ^1])
        else:
          raise newException(
            PgError, "Unexpected character after IPv6 address in DSN: " & part
          )
      else:
        let cpos = part.rfind(':')
        if cpos >= 0:
          # A bare host:port has exactly one ':'. More than one means an
          # unbracketed IPv6 literal (e.g. ::1), which is ambiguous in a URI
          # authority — RFC 3986 requires IPv6 literals to be bracketed.
          # Reject it instead of silently splitting host=":" port="1".
          if part.find(':') != cpos:
            raise newException(
              PgError, "IPv6 address in DSN must be bracketed, e.g. [::1]:5432: " & part
            )
          hostList.add pctDecode(part[0 ..< cpos])
          portList.add pctDecode(part[cpos + 1 .. ^1])
        else:
          hostList.add pctDecode(part)
          portList.add ""

  # Parse query parameters
  if queryStr.len > 0:
    for pair in queryStr.split('&'):
      let epos = pair.find('=')
      if epos < 0:
        continue
      let key = pctDecode(pair[0 ..< epos])
      let val = pctDecode(pair[epos + 1 .. ^1])
      case key
      of "host":
        hostList = splitList(val)
      of "hostaddr":
        addrList = splitList(val)
      of "port":
        portList = splitList(val)
      else:
        result.applyParam(key, val)

  result.hosts = buildHosts(hostList, addrList, portList)
  # Back-compat: set scalar host/hostaddr/port from the first entry;
  # `host` falls back to `hostaddr` like libpq's PQhost().
  result.host = result.hosts[0].displayHost
  result.hostaddr = result.hosts[0].hostaddr
  result.port = result.hosts[0].port

proc initConnConfig*(
    host = "127.0.0.1",
    port = 5432,
    hostaddr = "",
    user = "",
    password = "",
    database = "",
    sslMode = sslPrefer,
    sslRootCert = "",
    sslSni = true,
    channelBinding = cbPrefer,
    applicationName = "",
    connectTimeout = ZeroDuration,
    keepAlive = true,
    keepAliveIdle = 0,
    keepAliveInterval = 0,
    keepAliveCount = 0,
    hosts: seq[HostEntry] = @[],
    targetSessionAttrs = tsaAny,
    loadBalanceHosts = lbhDisable,
    requireAuth: set[AuthMethod] = {},
    extraParams: seq[(string, string)] = @[],
    maxMessageSize = 0,
    maxScramIterations = 0,
): ConnConfig =
  ## Create a connection configuration with sensible defaults.
  ## For DSN-based configuration, use `parseDsn` instead.
  ConnConfig(
    host: host,
    port: port,
    hostaddr: hostaddr,
    user: user,
    password: password,
    database: database,
    sslMode: sslMode,
    sslRootCert: sslRootCert,
    sslSni: sslSni,
    channelBinding: channelBinding,
    applicationName: applicationName,
    connectTimeout: connectTimeout,
    keepAlive: keepAlive,
    keepAliveIdle: keepAliveIdle,
    keepAliveInterval: keepAliveInterval,
    keepAliveCount: keepAliveCount,
    hosts: hosts,
    targetSessionAttrs: targetSessionAttrs,
    loadBalanceHosts: loadBalanceHosts,
    requireAuth: requireAuth,
    extraParams: extraParams,
    maxMessageSize: maxMessageSize,
    maxScramIterations: maxScramIterations,
  )

proc parseDsn*(dsn: string): ConnConfig =
  ## Parse a PostgreSQL connection string into a ConnConfig.
  ##
  ## Supports two formats:
  ## - URI: ``postgresql://[user[:password]@][host[:port]][/database][?param=value&...]``
  ## - keyword=value: ``host=localhost port=5432 dbname=test`` (libpq compatible)
  ##
  ## Both ``postgresql://`` and ``postgres://`` schemes are accepted for URI format.
  if dsn.startsWith("postgresql://") or dsn.startsWith("postgres://"):
    parseUriDsn(dsn)
  else:
    parseKeyValueDsn(dsn)
