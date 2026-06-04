## DSN parsing for PostgreSQL connection strings.
##
## Supports both libpq formats:
## - keyword=value:  ``host=localhost port=5432 dbname=test``
## - URI:            ``postgresql://user:pass@host:port/db?param=value``
##
## Re-exported through `pg_connection.nim`; depends only on `types.nim`
## (in particular, does not touch `PgConnection`).

import std/[strutils, uri]

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

proc parsePort*(s: string): int =
  try:
    result = parseInt(s)
  except ValueError:
    raise newException(PgError, "Invalid port in DSN: " & s)
  if result < 1 or result > 65535:
    raise newException(PgError, "Port out of range (1-65535): " & s)

proc applyParam*(result: var ConnConfig, key, val: string) =
  ## Apply a single connection parameter to a ConnConfig.
  case key
  of "host", "hostaddr":
    result.host = val
  of "port":
    result.port = parsePort(val)
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
    try:
      result.connectTimeout = seconds(parseInt(val))
    except ValueError:
      raise newException(PgError, "Invalid connect_timeout: " & val)
  of "sslrootcert":
    try:
      result.sslRootCert = readFile(val)
    except IOError:
      raise newException(PgError, "Cannot read sslrootcert file: " & val)
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
  of "max_message_size":
    try:
      result.maxMessageSize = parseInt(val)
    except ValueError:
      raise newException(PgError, "Invalid max_message_size: " & val)
    if result.maxMessageSize < 0:
      raise newException(PgError, "max_message_size must be non-negative: " & val)
  else:
    result.extraParams.add((key, val))

proc parseKeyValueDsn*(dsn: string): ConnConfig =
  ## Parse a libpq keyword=value connection string into a ConnConfig.
  ##
  ## Format: ``host=localhost port=5432 dbname=test user=myuser``
  ##
  ## Values may be single-quoted: ``password='has spaces'``
  ## Within quoted values, ``\'`` and ``\\`` are escape sequences.
  result.keepAlive = true
  result.host = "127.0.0.1"
  result.port = 5432
  result.sslMode = sslPrefer # libpq default; overridden by an explicit sslmode

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
      # Unquoted value
      while i < dsn.len and dsn[i] notin {' ', '\t', '\n', '\r'}:
        val.add dsn[i]
        inc i

    pairs.add((key, val))

  for (key, val) in pairs:
    result.applyParam(key, val)

  if result.host.len == 0:
    result.host = "127.0.0.1"

proc parseUriDsn*(dsn: string): ConnConfig =
  ## Parse a PostgreSQL URI connection string into a ConnConfig.
  result.keepAlive = true
  result.sslMode = sslPrefer # libpq default; overridden by an explicit sslmode
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
      result.user = decodeUrl(userinfo[0 ..< cpos])
      result.password = decodeUrl(userinfo[cpos + 1 .. ^1])
    else:
      result.user = decodeUrl(userinfo)

  # Parse host:port/database
  var hostport, dbpath: string
  let spos = hostpath.find('/')
  if spos >= 0:
    hostport = hostpath[0 ..< spos]
    dbpath = hostpath[spos + 1 .. ^1]
  else:
    hostport = hostpath

  if dbpath.len > 0:
    result.database = decodeUrl(dbpath)

  # Parse host(s) and port(s) — supports comma-separated multi-host syntax
  proc parseHostEntry(entry: string): HostEntry =
    if entry.startsWith("["):
      # IPv6: [::1]:5432
      let bracket = entry.find(']')
      if bracket < 0:
        raise newException(PgError, "Invalid IPv6 address in DSN")
      result.host = entry[1 ..< bracket]
      let afterBracket = entry[bracket + 1 .. ^1]
      if afterBracket.startsWith(":"):
        result.port = parsePort(afterBracket[1 .. ^1])
      else:
        result.port = 5432
    else:
      let cpos = entry.rfind(':')
      if cpos >= 0:
        result.host = entry[0 ..< cpos]
        result.port = parsePort(entry[cpos + 1 .. ^1])
      else:
        result.host = entry
        result.port = 5432

  if hostport.len > 0:
    let parts = hostport.split(',')
    for part in parts:
      if part.len > 0:
        result.hosts.add(parseHostEntry(part))
    # Back-compat: set host/port from first entry
    result.host = result.hosts[0].host
    result.port = result.hosts[0].port
  else:
    result.host = "127.0.0.1"
    result.port = 5432
    result.hosts = @[HostEntry(host: "127.0.0.1", port: 5432)]

  if result.host.len == 0:
    result.host = "127.0.0.1"

  # Parse query parameters
  if queryStr.len > 0:
    for pair in queryStr.split('&'):
      let epos = pair.find('=')
      if epos < 0:
        continue
      let key = decodeUrl(pair[0 ..< epos])
      let val = decodeUrl(pair[epos + 1 .. ^1])
      result.applyParam(key, val)

proc initConnConfig*(
    host = "127.0.0.1",
    port = 5432,
    user = "",
    password = "",
    database = "",
    sslMode = sslPrefer,
    sslRootCert = "",
    channelBinding = cbPrefer,
    applicationName = "",
    connectTimeout = ZeroDuration,
    keepAlive = true,
    keepAliveIdle = 0,
    keepAliveInterval = 0,
    keepAliveCount = 0,
    hosts: seq[HostEntry] = @[],
    targetSessionAttrs = tsaAny,
    requireAuth: set[AuthMethod] = {},
    extraParams: seq[(string, string)] = @[],
    maxMessageSize = 0,
): ConnConfig =
  ## Create a connection configuration with sensible defaults.
  ## For DSN-based configuration, use `parseDsn` instead.
  ConnConfig(
    host: host,
    port: port,
    user: user,
    password: password,
    database: database,
    sslMode: sslMode,
    sslRootCert: sslRootCert,
    channelBinding: channelBinding,
    applicationName: applicationName,
    connectTimeout: connectTimeout,
    keepAlive: keepAlive,
    keepAliveIdle: keepAliveIdle,
    keepAliveInterval: keepAliveInterval,
    keepAliveCount: keepAliveCount,
    hosts: hosts,
    targetSessionAttrs: targetSessionAttrs,
    requireAuth: requireAuth,
    extraParams: extraParams,
    maxMessageSize: maxMessageSize,
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
