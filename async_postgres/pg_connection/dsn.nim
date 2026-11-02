## DSN parsing for PostgreSQL connection strings.
##
## Supports both libpq formats:
## - keyword=value:  ``host=localhost port=5432 dbname=test``
## - URI:            ``postgresql://user:pass@host:port/db?param=value``
##
## Treat the DSN as trusted operator config, not end-user input: unknown keys
## become StartupMessage parameters and values such as ``options`` are
## forwarded verbatim. Sanitize before ``parseDsn`` if it is not under your
## control.
##
## Only `initConnConfig` / `parseDsn` / `validateConnConfig` are re-exported
## through `pg_connection.nim`; the intermediate parsers stay here. Depends
## only on `types.nim` and `conf_files.nim` (does not touch `PgConnection`).
##
## Internal module: not part of the public API. Import the `pg_connection` hub instead.

import std/[strutils, options, sequtils]
from std/os import getEnv

import ../[async_backend, pg_errors]
from ../pg_types/encoding import hexNibble
import types, conf_files

const RootCertUnderRequireErrorMsg =
  "sslrootcert with sslmode=require is not supported by this client; use verify-ca"
  ## A CA under ``require`` cannot be honored: libpq verifies the server against
  ## it there, this client would accept any certificate.

proc parseSslMode(s: string): SslMode =
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
    raise newException(PgConfigError, "Invalid sslmode (len=" & $s.len & ")")

proc parseChannelBindingMode(s: string): ChannelBindingMode =
  case s
  of "disable":
    cbDisable
  of "prefer":
    cbPrefer
  of "require":
    cbRequire
  else:
    raise newException(PgConfigError, "Invalid channel_binding (len=" & $s.len & ")")

proc parseSslNegotiation(s: string): SslNegotiation =
  case s
  of "postgres":
    sslnPostgres
  of "direct":
    sslnDirect
  else:
    raise newException(PgConfigError, "Invalid sslnegotiation (len=" & $s.len & ")")

proc parseAuthMethod(s: string): AuthMethod =
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
    raise
      newException(PgConfigError, "Invalid require_auth method (len=" & $s.len & ")")

proc parseRequireAuth(s: string): set[AuthMethod] =
  ## Parse a comma-separated list of auth method names into a set
  ## (libpq `require_auth` syntax; negation prefix `!` is not yet supported).
  ## Empty input returns the empty set (allow any).
  if s.len == 0:
    return {}
  for raw in s.split(','):
    let tok = raw.strip()
    if tok.len == 0:
      raise newException(
        PgConfigError, "Empty entry in require_auth list (len=" & $s.len & ")"
      )
    result.incl(parseAuthMethod(tok))

proc parseTargetSessionAttrs(s: string): TargetSessionAttrs =
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
    raise
      newException(PgConfigError, "Invalid target_session_attrs (len=" & $s.len & ")")

proc parseLoadBalanceHosts(s: string): LoadBalanceHosts =
  case s
  of "disable":
    lbhDisable
  of "random":
    lbhRandom
  else:
    raise newException(PgConfigError, "Invalid load_balance_hosts (len=" & $s.len & ")")

proc parseLibpqInt(s: string, cInt = true): Option[int] =
  ## libpq's strtol-based integer: surrounding whitespace and a leading sign
  ## are accepted, digit-group underscores (which Nim's parseInt takes) are
  ## not, and with `cInt` the value must fit a C int.
  let t = s.strip()
  if t.len == 0 or t.find('_') >= 0:
    return none(int)
  try:
    let n = parseInt(t)
    if not cInt or (n >= int(low(int32)) and n <= int(high(int32))):
      result = some(n)
  except ValueError:
    discard

proc parseDsnInt(val, label: string, cInt = true): int =
  ## A DSN integer by `parseLibpqInt`'s rules. `cInt = false` is for this
  ## client's own keywords, which libpq does not bound.
  let n = parseLibpqInt(val, cInt)
  if n.isNone:
    raise newException(PgConfigError, "Invalid " & label & " (len=" & $val.len & ")")
  n.get

proc parsePort(s: string): int =
  ## libpq's integer rules (`+5432` is taken too), in 1–65535.
  let n = parseLibpqInt(s, cInt = false)
  if n.isNone:
    raise newException(PgConfigError, "Invalid port in DSN (len=" & $s.len & ")")
  result = n.get
  if result < 1 or result > 65535:
    raise
      newException(PgConfigError, "Port out of range (1-65535) (len=" & $s.len & ")")

proc splitList(s: string): seq[string] =
  ## Comma-split a multi-host parameter; an empty string means "not provided".
  if s.len == 0:
    @[]
  else:
    s.split(',')

proc withOrigin(e: ref PgConfigError, origin: string): ref PgConfigError =
  ## `e` prefixed with where its value came from, if anywhere but the string.
  if origin.len == 0:
    e
  else:
    newException(PgConfigError, origin & ": " & e.msg, e)

proc joinOrigins(origins: openArray[string]): string =
  ## The distinct origins that apply, for an error about more than one list.
  origins.filterIt(it.len > 0).deduplicate().join(", ")

type HostOrigins = tuple[host, address, port: string]
  ## Where each of a connection string's three lists came from: "" for the
  ## string itself, else a variable name or a service file line.

proc buildHosts(
    hostList, addrList, portList: seq[string],
    hostGiven = false,
    origins: HostOrigins = (host: "", address: "", port: ""),
): seq[HostEntry] =
  ## Expand host/hostaddr/port lists into HostEntry values following libpq
  ## multi-host rules: the number of entries is set by `hostaddr` when given,
  ## else by `host` (an empty seq means the parameter was not provided; when
  ## both are given their lengths must match exactly). The port list must
  ## contain either a single entry (applied to every host) or exactly one
  ## entry per host. An empty list entry selects the default (127.0.0.1 /
  ## 5432); an empty host with a hostaddr stays empty, so SSL verification
  ## can require an explicit name.
  ##
  ## `hostGiven` (and any given `addrList`) suppresses the 127.0.0.1 default:
  ## a spelled-out empty host must not silently become a localhost target.
  ## `applyParam` cannot yet know whether a later `hostaddr` will pair with
  ## it, so the fault is carried to `validateConnConfig` instead.
  ##
  ## A fault is blamed on the list it is about, via `origins`: a port that
  ## will not parse comes from the port list, not from a host list the
  ## environment happened to supply.
  if hostList.len > 0 and addrList.len > 0 and hostList.len != addrList.len:
    raise newException(
        PgConfigError,
        "Could not match " & $hostList.len & " host names to " & $addrList.len &
          " hostaddr values",
      )
      .withOrigin(joinOrigins([origins.host, origins.address]))
  let explicit = hostGiven or addrList.len > 0
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
        PgConfigError,
        "Could not match " & $ports.len & " port numbers to " & $count & " hosts",
      )
      .withOrigin(origins.port)
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
    if a.len > 0 and a[0] == '/':
      # `hostaddr` is a numeric IP (libpq forces TCP/IP whenever it is
      # non-empty). A '/' value would otherwise select AF_UNIX via `dialAddr`
      # and skip TLS entirely. Unix sockets stay available via `host`.
      raise newException(
          PgConfigError,
          "Invalid hostaddr: must be a numeric IP address, not a Unix socket path (use host for Unix sockets) (len=" &
            $a.len & ")",
        )
        .withOrigin(origins.address)
    let p =
      if ports.len == 1:
        ports[0]
      else:
        ports[i]
    var port = 5432
    if p.len > 0:
      try:
        port = parsePort(p)
      except PgConfigError as e:
        raise e.withOrigin(origins.port)
    result.add HostEntry(
      host: if h.len == 0 and a.len == 0 and not explicit: DefaultHost else: h,
      hostaddr: a,
      port: port,
    )

proc checkExplicitHosts(
    hostList, addrList: seq[string], hostGiven: bool, addrGiven = false
) =
  ## Reject a spelled-out target with neither name nor address: a
  ## mis-templated DSN would otherwise default to 127.0.0.1. An omitted
  ## `host`/`hostaddr` still defaults; an empty element paired with the other
  ## stays valid. `hostaddr=` alone collapses to an empty list rather than an
  ## empty element, hence `addrGiven` alongside `addrList.len`.
  if not hostGiven and not addrGiven and addrList.len == 0:
    return
  if hostList.len > 0 and addrList.len > 0 and hostList.len != addrList.len:
    # Let `buildHosts` report the mismatch; its message names both counts.
    return
  let count =
    if addrList.len > 0:
      addrList.len
    elif hostList.len > 0:
      hostList.len
    else:
      0
  if count == 0:
    raise newException(PgConfigError, "Empty host in DSN")
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
    if h.len == 0 and a.len == 0:
      raise
        newException(PgConfigError, "Empty host in DSN host list (element #" & $i & ")")

proc notRegularFileError(label, path: string): ref PgConfigError =
  newException(
    PgConfigError, label & " file is not a regular file, refusing to use: " & path
  )

proc rejectRawNulField(field, value: string) =
  ## Raise ``PgConfigError`` if ``value`` holds a raw NUL. The message names the
  ## field and offset but never the value, which may be the password.
  let at = value.find('\0')
  if at >= 0:
    raise newException(
      PgConfigError,
      "Forbidden zero byte in " & field & " at offset=" & $at & " (len=" & $value.len &
        ")",
    )

proc readPemFileParam(path, label: string, checkKeyPerms = false): string =
  ## Read a PEM parameter file (sslrootcert/sslcert/sslkey) into a
  ## `PgConfigError` naming the parameter on any failure. An empty file is
  ## rejected: it would silently fall through the callers' `.len > 0` guards.
  ## With `checkKeyPerms` (sslkey), any group or world permission bit is
  ## rejected — intentionally stricter than libpq, which permits `0o640` for
  ## root-owned keys. No permission check off-POSIX (libpq also skips it on
  ## Windows).
  # Before the C `open`, which would cut the path at a NUL and read another file.
  rejectRawNulField(label, path)
  case readConfFile(path, result, checkKeyPerms)
  of cffNone:
    discard
  of cffOpen, cffRead:
    raise newException(PgConfigError, "Cannot read " & label & " file: " & path)
  of cffStat:
    raise newException(PgConfigError, "Cannot stat " & label & " file: " & path)
  of cffNotRegular:
    raise notRegularFileError(label, path)
  of cffLoosePerms:
    raise newException(
      PgConfigError,
      label & " file has group or world accessible permissions, refusing to use: " & path,
    )
  of cffTooLarge:
    raise newException(
      PgConfigError,
      label & " file exceeds size limit (" & $MaxConfFileBytes & " bytes): " & path,
    )
  if result.len == 0:
    raise newException(PgConfigError, label & " file is empty: " & path)

proc rawHost(host, hostaddr: string): string =
  ## Inverse of `buildHosts`' defaulting: 127.0.0.1 with no hostaddr can only
  ## come from an entry where neither was provided, so fold it back to "".
  ## An explicit `host=127.0.0.1` is structurally indistinguishable from the
  ## implicit default, but re-defaults to the same value on rebuild —
  ## functionally correct for the round-trip.
  if host == DefaultHost and hostaddr.len == 0: "" else: host

proc rawHostLists(
    c: ConnConfig
): tuple[hosts, addrs, ports: seq[string], hostGiven: bool] =
  ## Re-derive raw multi-host lists from a config so `applyParam` can rebuild
  ## `hosts` with one list replaced. Best-effort inverse of `buildHosts`:
  ## all-empty lists collapse to "not provided" and an all-equal port list to
  ## a single entry.
  ##
  ## The port collapsing is required for correctness: if the replacement list
  ## in `applyParam` has a different length than the current hosts, a
  ## collapsed single-entry port list "applies to all" via `buildHosts`,
  ## whereas a length-specific list would be rejected as mismatched.
  ##
  ## `hostGiven` flags a literally empty host in `hosts`, which `buildHosts`
  ## only produces for an explicitly empty one, so rebuilding must not default
  ## it away. The scalar branch cannot tell that apart from the zero value, so
  ## it leaves `hostGiven` false.
  if c.hosts.len == 0:
    result.hosts = splitList(rawHost(c.host, c.hostaddr))
    result.addrs = splitList(c.hostaddr)
    if c.port > 0:
      result.ports = @[$c.port]
    return
  # `buildHosts` only defaults to 127.0.0.1 for a single target; folding it
  # away in a multi-entry list would erase an explicit `host=127.0.0.1`.
  let foldDefault = c.hosts.len == 1
  var anyHost, anyAddr = false
  var samePorts = true
  for e in c.hosts:
    let h =
      if foldDefault:
        rawHost(e.host, e.hostaddr)
      else:
        e.host
    result.hosts.add h
    result.addrs.add e.hostaddr
    result.ports.add $e.port
    anyHost = anyHost or h.len > 0
    anyAddr = anyAddr or e.hostaddr.len > 0
    samePorts = samePorts and e.port == c.hosts[0].port
    if e.host.len == 0:
      result.hostGiven = true
  if not anyHost:
    result.hosts = @[]
  if not anyAddr:
    result.addrs = @[]
  if samePorts:
    result.ports = @[result.ports[0]]

when defined(linux):
  # The kernel's MAX_TCP_KEEPIDLE / MAX_TCP_KEEPINTVL / MAX_TCP_KEEPCNT. Past
  # them `setsockopt` fails at connect, which reads as a connection failure.
  const
    maxKeepAliveIdle = 32767'i64
    maxKeepAliveInterval = 32767'i64
    maxKeepAliveCount = 127'i64
    maxTcpUserTimeout = int64(high(cint))
else:
  const
    maxSockOptInt = int64(high(cint))
      ## Keepalive timings reach `setsockopt` as `cint`; a larger value would
      ## turn into an uncatchable RangeDefect at connect time instead of a
      ## PgConfigError here.
    maxKeepAliveIdle = maxSockOptInt
    maxKeepAliveInterval = maxSockOptInt
    maxKeepAliveCount = maxSockOptInt
    maxTcpUserTimeout = maxSockOptInt

proc checkClientEncoding(val: string) =
  ## The client pins ``client_encoding`` to UTF8.
  if not isUtf8EncodingName(val):
    raise
      newException(PgConfigError, "client_encoding must be UTF8 (len=" & $val.len & ")")

proc checkDateStyle(val: string) =
  ## The client pins the ``DateStyle`` output style to ISO; only the field
  ## order (``DMY``, ...) is the caller's.
  if namesNonIsoDateStyle(val):
    raise newException(
      PgConfigError, "DateStyle must use the ISO output style (len=" & $val.len & ")"
    )

proc splitStartupOptions(options: string): seq[string] =
  ## Split like the server's ``pg_split_opts``: whitespace separates, ``\``
  ## escapes the next byte.
  var cur = ""
  var inArg = false
  var i = 0
  while i < options.len:
    let c = options[i]
    if c in Whitespace:
      if inArg:
        result.add(cur)
        cur = ""
        inArg = false
    else:
      if c == '\\':
        # A trailing escape is dropped.
        if i + 1 < options.len:
          inc i
          cur.add(options[i])
      else:
        cur.add(c)
      inArg = true
    inc i
  if inArg:
    result.add(cur)

iterator optionsSettings(options: string): tuple[name, value: string] =
  ## The ``name=value`` settings of the ``-c`` and ``--`` switches in
  ## ``options``, read like the server's getopt.
  const argOpts =
    {'B', 'C', 'c', 'D', 'd', 'f', 'h', 'k', 'N', 'p', 'r', 'S', 't', 'v', 'W', '-'}
    # postgres's getopt string
  let args = splitStartupOptions(options)
  var i = 0
  while i < args.len:
    let a = args[i]
    inc i
    if a.len < 2 or a[0] != '-':
      continue
    for j in 1 ..< a.len:
      if a[j] notin argOpts:
        continue
      var optArg: string
      if j + 1 < a.len:
        optArg = a[j + 1 .. ^1]
      elif i < args.len:
        optArg = args[i]
        inc i
      if a[j] in {'c', '-'}:
        let eq = optArg.find('=')
        if eq >= 0:
          # ParseLongOption maps '-' to '_' in the name.
          yield (optArg[0 ..< eq].replace('-', '_'), optArg[eq + 1 .. ^1])
      break

proc checkOptionsPinnedParams(options: string) =
  ## Our startup ``client_encoding`` and ``DateStyle`` output style would
  ## silently override a ``-c`` switch, so reject a conflicting one as if it
  ## were given directly.
  for (name, value) in optionsSettings(options):
    if isGucName(name, "client_encoding"):
      checkClientEncoding(value)
    elif isGucName(name, "DateStyle"):
      checkDateStyle(value)

proc optionsTimeZone*(options: string): Option[string] =
  ## The ``TimeZone`` value a ``-c`` switch in ``options`` sets, if any. The
  ## last switch wins, as on the server.
  for (name, value) in optionsSettings(options):
    if isGucName(name, "TimeZone"):
      result = some(value)

proc applyParam(result: var ConnConfig, key, val: string) =
  ## Apply a single connection parameter to a ConnConfig.
  ##
  ## `host`/`hostaddr`/`port` accept comma-separated multi-host lists and
  ## rebuild `hosts` on each call. The DSN parsers still intercept these
  ## keys: there all values are collected first and expanded once, so their
  ## correlation is order-independent, while repeated `applyParam` calls
  ## correlate each list against the already-expanded state.
  ##
  ## An explicitly empty host is kept empty so `host` and `hostaddr` may be
  ## applied in either order; still unpaired ones are rejected by
  ## `validateConnConfig`.
  case key
  of "host", "hostaddr", "port":
    # Rebuild `hosts` so it stays consistent with the scalar view
    # (`getHosts` prefers `hosts` when non-empty); the two untouched
    # lists are re-derived from the current config.
    var (hostList, addrList, portList, hostGiven) = result.rawHostLists()
    case key
    of "host":
      hostList = splitList(val)
      hostGiven = true
    of "hostaddr":
      addrList = splitList(val)
    else:
      portList = splitList(val)
    result.hosts = buildHosts(hostList, addrList, portList, hostGiven)
    result.host = result.hosts[0].displayHost
    result.hostaddr = result.hosts[0].hostaddr
    result.port = result.hosts[0].port
    # A port leaves the target as it was; `rawHostLists` folds an explicit
    # 127.0.0.1 away, so the rebuild alone cannot tell.
    if key == "host" or addrList.len > 0:
      result.setHostDefaulted(false)
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
  of "sslnegotiation":
    result.sslNegotiation = parseSslNegotiation(val)
  of "require_auth":
    result.requireAuth = parseRequireAuth(val)
  of "application_name":
    result.applicationName = val
  of "connect_timeout":
    let secs = parseDsnInt(val, "connect_timeout")
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
    result.sslRootCert = readPemFileParam(val, "sslrootcert")
  of "sslcert", "sslkey":
    # The sslmode-compatibility check is deferred to `validateClientCertConfig`:
    # a field-by-field `applyParam` caller may set sslcert before sslmode.
    if key == "sslcert":
      result.sslCert = readPemFileParam(val, "sslcert")
    else:
      result.sslKey = readPemFileParam(val, "sslkey", checkKeyPerms = true)
  of "sslsni":
    # libpq only checks for a leading '1'; garbage is still refused here.
    discard parseDsnInt(val, "sslsni", cInt = false)
    result.sslSni = val.len > 0 and val[0] == '1'
  of "keepalives":
    result.keepAlive = parseDsnInt(val, "keepalives") != 0
  of "keepalives_idle":
    result.keepAliveIdle = parseDsnInt(val, "keepalives_idle")
    if result.keepAliveIdle < 0:
      raise newException(
        PgConfigError, "keepalives_idle must be non-negative (len=" & $val.len & ")"
      )
    if int64(result.keepAliveIdle) > maxKeepAliveIdle:
      raise newException(
        PgConfigError,
        "keepalives_idle out of range (0-" & $maxKeepAliveIdle & ") (len=" & $val.len &
          ")",
      )
  of "keepalives_interval":
    result.keepAliveInterval = parseDsnInt(val, "keepalives_interval")
    if result.keepAliveInterval < 0:
      raise newException(
        PgConfigError, "keepalives_interval must be non-negative (len=" & $val.len & ")"
      )
    if int64(result.keepAliveInterval) > maxKeepAliveInterval:
      raise newException(
        PgConfigError,
        "keepalives_interval out of range (0-" & $maxKeepAliveInterval & ") (len=" &
          $val.len & ")",
      )
  of "keepalives_count":
    result.keepAliveCount = parseDsnInt(val, "keepalives_count")
    if result.keepAliveCount < 0:
      raise newException(
        PgConfigError, "keepalives_count must be non-negative (len=" & $val.len & ")"
      )
    if int64(result.keepAliveCount) > maxKeepAliveCount:
      raise newException(
        PgConfigError,
        "keepalives_count out of range (0-" & $maxKeepAliveCount & ") (len=" & $val.len &
          ")",
      )
  of "target_session_attrs":
    result.targetSessionAttrs = parseTargetSessionAttrs(val)
  of "load_balance_hosts":
    result.loadBalanceHosts = parseLoadBalanceHosts(val)
  of "max_message_size":
    result.maxMessageSize = parseDsnInt(val, "max_message_size", cInt = false)
    if result.maxMessageSize < 0:
      raise newException(
        PgConfigError, "max_message_size must be non-negative (len=" & $val.len & ")"
      )
  of "max_scram_iterations":
    result.maxScramIterations = parseDsnInt(val, "max_scram_iterations", cInt = false)
    if result.maxScramIterations < 0:
      raise newException(
        PgConfigError,
        "max_scram_iterations must be non-negative (len=" & $val.len & ")",
      )
  else:
    if isGucName(key, "client_encoding"):
      # Not kept: the startup message always sends UTF8.
      checkClientEncoding(val)
    else:
      if isGucName(key, "DateStyle"):
        checkDateStyle(val)
      elif key == "options":
        checkOptionsPinnedParams(val)
      result.extraParams.add((key, val))

type
  DsnItem = object
    key, val: string
    origin: string
      ## Where a value the connection string left out came from (an
      ## environment variable, a service file line); "" for the string itself.

  DsnFields = object
    ## A connection string's settings before they are applied, so that the
    ## service file and the environment can fill in only what it leaves out.
    items: seq[DsnItem] ## Every key but host, hostaddr and port, in order.
    hostList, addrList, portList: seq[string]
    hostGiven, addrGiven, portGiven: bool
    hostBlank: bool
      ## A URI authority with no host (``:5433``): libpq stores none, so the
      ## environment or a service may fill it; unfilled, it is rejected.
    hostOrigin, addrOrigin, portOrigin: string

const DsnKeywords = [
  # libpq's (but client_encoding, a GUC this client checks as one) and this
  # client's own. libpq matches them case-sensitively.
  "service", "user", "password", "passfile", "channel_binding", "connect_timeout",
  "dbname", "host", "hostaddr", "port", "options", "application_name",
  "fallback_application_name", "keepalives", "keepalives_idle", "keepalives_interval",
  "keepalives_count", "tcp_user_timeout", "sslmode", "sslnegotiation", "sslcompression",
  "sslcert", "sslkey", "sslcertmode", "sslpassword", "sslrootcert", "sslcrl",
  "sslcrldir", "sslsni", "requirepeer", "require_auth", "min_protocol_version",
  "max_protocol_version", "ssl_min_protocol_version", "ssl_max_protocol_version",
  "gssencmode", "krbsrvname", "gsslib", "gssdelegation", "replication",
  "target_session_attrs", "load_balance_hosts", "scram_client_key", "scram_server_key",
  "oauth_issuer", "oauth_client_id", "oauth_client_secret", "oauth_scope",
  "oauth_ca_file", "sslkeylogfile", "max_message_size", "max_scram_iterations",
]

proc sameKey(a, b: string): bool =
  ## A keyword only matches itself; `SSLMODE` is a startup parameter, not
  ## `sslmode`. Other names are GUCs, which the server matches without case.
  if a == b:
    true
  elif a in DsnKeywords or b in DsnKeywords:
    false
  else:
    isGucName(a, b)

func canonicalKey(key: string): string =
  ## The server's application_name in any case is kept as the keyword, so an
  ## environment value, a service line or a fallback cannot override it.
  if isGucName(key, "application_name"): "application_name" else: key

proc add(f: var DsnFields, key, val, origin: string) =
  ## Record a setting. `host`, `hostaddr` and `port` are comma-separated lists
  ## correlated only once all are seen. A later setting of a key replaces an
  ## earlier one, as libpq keeps only the last.
  case key
  of "host":
    f.hostList = splitList(val)
    f.hostGiven = true
    f.hostBlank = false
    f.hostOrigin = origin
  of "hostaddr":
    f.addrList = splitList(val)
    f.addrGiven = true
    f.addrOrigin = origin
  of "port":
    f.portList = splitList(val)
    f.portGiven = true
    f.portOrigin = origin
  else:
    let key = canonicalKey(key)
    for it in f.items.mitems:
      if sameKey(it.key, key):
        it = DsnItem(key: key, val: val, origin: origin)
        return
    f.items.add DsnItem(key: key, val: val, origin: origin)

proc has(f: DsnFields, key: string): bool =
  ## Whether `key` is set, even to "" (see `sameKey`).
  case key
  of "host":
    f.hostGiven and not f.hostBlank
  of "hostaddr":
    f.addrGiven
  of "port":
    f.portGiven
  else:
    let key = canonicalKey(key)
    for it in f.items:
      if sameKey(it.key, key):
        return true
    false

proc translateLibpqAlias(key, val: var string, uri: bool) =
  ## libpq's aliases for ``sslmode`` in a connection string: the deprecated
  ## ``requiressl`` (a value starting with ``1`` means require, anything else
  ## prefer) and, in a URI only, JDBC's ``ssl=true``.
  if key == "requiressl":
    key = "sslmode"
    val = if val.len > 0 and val[0] == '1': "require" else: "prefer"
  elif uri and key == "ssl" and val == "true":
    key = "sslmode"
    val = "require"

proc collectKeyValueDsn(dsn: string): DsnFields =
  ## The settings of a libpq keyword=value connection string.
  ##
  ## Format: ``host=localhost port=5432 dbname=test user=myuser``
  ##
  ## Values may be single-quoted: ``password='has spaces'``
  ## As in libpq, a backslash escapes the following character both inside
  ## and outside quotes (``\'``, ``\\``, ``host=foo\ bar``).
  ##
  ## `host`, `hostaddr`, and `port` accept comma-separated lists for
  ## multi-host failover (libpq compatible): ``host=h1,h2 port=5433,5434``.
  # Tokenize into (key, value) pairs
  var i = 0
  while i < dsn.len:
    # Skip whitespace
    while i < dsn.len and dsn[i] in {' ', '\t', '\n', '\r'}:
      inc i
    if i >= dsn.len:
      break

    # Read key
    let keyStart = i
    var key = ""
    while i < dsn.len and dsn[i] notin {'=', ' ', '\t', '\n', '\r'}:
      key.add dsn[i]
      inc i
    if key.len == 0:
      raise newException(PgConfigError, "Empty key in connection string")

    # Skip whitespace around '='
    while i < dsn.len and dsn[i] in {' ', '\t'}:
      inc i
    if i >= dsn.len or dsn[i] != '=':
      # The stray token may hold secret material (unquoted whitespace), so
      # report the location only.
      raise newException(
        PgConfigError,
        "Expected '=' after key at offset=" & $keyStart & " (len=" & $key.len &
          ") in connection string",
      )
    inc i # skip '='
    while i < dsn.len and dsn[i] in {' ', '\t'}:
      inc i

    # Read value
    let valStart = i
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
        # Unknown keys are collected as-is, so the key may hold a secret;
        # report the location only.
        raise newException(
          PgConfigError,
          "Unterminated quoted value for key at offset=" & $keyStart & " (len=" &
            $key.len & ", value offset=" & $valStart & ") in connection string",
        )
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

    translateLibpqAlias(key, val, uri = false)
    result.add(key, val, "")

proc pctDecode(s: string, field: string): string =
  ## Percent-decode a URI component following libpq rules: strict ``%XX``
  ## (a malformed sequence or an encoded zero byte is an error) and no
  ## ``+``-to-space translation.
  ##
  ## Failures report the component name, byte offset and length only, never
  ## the input (userinfo may hold the password). Offsets are relative to the
  ## component slice, not the whole DSN.
  result = newStringOfCap(s.len)
  var i = 0
  while i < s.len:
    if s[i] == '%':
      if i + 2 >= s.len or s[i + 1] notin HexDigits or s[i + 2] notin HexDigits:
        raise newException(
          PgConfigError,
          "Invalid percent-encoded token in " & field & " of DSN at offset=" & $i &
            " (len=" & $s.len & ")",
        )
      # Not parseHexInt: its unreachable ValueError would join parseDsn's raises.
      let c = chr(hexNibble(s[i + 1]) * 16 + hexNibble(s[i + 2]))
      if c == '\0':
        raise newException(
          PgConfigError,
          "Forbidden zero byte in " & field & " of DSN at offset=" & $i & " (len=" &
            $s.len & ")",
        )
      result.add c
      i += 3
    else:
      result.add s[i]
      inc i

proc collectUriDsn(dsn: string): DsnFields =
  ## The settings of a PostgreSQL URI connection string. As in libpq, an empty
  ## user, password, authority or database is left unset, and so is the host
  ## of an authority with only a port.
  let scheme =
    if dsn.startsWith("postgresql://"):
      "postgresql"
    elif dsn.startsWith("postgres://"):
      "postgres"
    else:
      raise newException(
        PgConfigError, "Invalid DSN scheme: expected postgresql:// or postgres://"
      )

  # Strip scheme prefix
  let rest = dsn[scheme.len + 3 .. ^1] # skip "scheme://"

  # Userinfo ends at the last '@' before the first '/' or '?' (WHATWG, not
  # libpq's first '@'), so a raw '@' in user or password parses.
  let authEnd = block:
    let p = rest.find({'/', '?'})
    if p < 0: rest.len else: p
  let upos =
    if authEnd > 0:
      rest.rfind('@', last = authEnd - 1)
    else:
      -1
  let userinfo =
    if upos >= 0:
      rest[0 ..< upos]
    else:
      ""
  let hostport = rest[upos + 1 ..< authEnd]
  let hasPath = authEnd < rest.len and rest[authEnd] == '/'
  let qpos = rest.find('?', start = authEnd)
  var dbpath, queryStr: string
  if hasPath:
    dbpath = rest[authEnd + 1 ..< (if qpos >= 0: qpos else: rest.len)]
  if qpos >= 0:
    queryStr = rest[qpos + 1 .. ^1]

  # A raw '/' or '?' in the userinfo moves its '@'-bearing tail right after the
  # authority ("svc/admin@REALM@h" -> host "svc"); reject before host errors.
  if '@' in dbpath:
    raise newException(
      PgConfigError,
      "Ambiguous DSN: raw '@' in the database; percent-encode '/' in the " &
        "user name or password (%2F) or '@' in the database (%40)",
    )
  if not hasPath and '@' in queryStr:
    raise newException(
      PgConfigError,
      "Ambiguous DSN: raw '@' in a query with no path; percent-encode '?' " &
        "in the user name or password (%3F) or '@' in the query (%40), or " &
        "add '/' before '?'",
    )

  # Parse user:password
  if userinfo.len > 0:
    let cpos = userinfo.find(':')
    let (user, password) =
      if cpos >= 0:
        (
          pctDecode(userinfo[0 ..< cpos], "userinfo"),
          pctDecode(userinfo[cpos + 1 .. ^1], "userinfo"),
        )
      else:
        (pctDecode(userinfo, "userinfo"), "")
    if user.len > 0:
      result.add("user", user, "")
    if password.len > 0:
      result.add("password", password, "")

  # Parse host(s) and port(s) — supports comma-separated multi-host syntax.
  # libpq order: the authority splits on commas *before* percent-decoding
  # (an encoded comma stays inside one host), query parameters decode
  # *before* splitting. Structural separators are matched on the raw text,
  # so encoded ones never split.
  var hostList, portList: seq[string]
  template decodeHostElem(s, kind: string, elementIdx: int): string =
    ## Like ``pctDecode``, locating failures by 0-based authority element.
    try:
      pctDecode(s, kind)
    except PgConfigError as e:
      raise newException(
        PgConfigError, "URI host/port (element #" & $elementIdx & "): " & e.msg
      )

  if hostport.len > 0:
    let hostParts = hostport.split(',')
    for hidx in 0 ..< hostParts.len:
      let part = hostParts[hidx]
      if part.startsWith("["):
        # IPv6: [::1]:5432; a zone id is encoded as %25: [fe80::1%25eth0]
        let bracket = part.find(']')
        if bracket < 0:
          raise newException(PgConfigError, "Invalid IPv6 address in DSN")
        let inside = part[1 ..< bracket]
        # Rejected here rather than by ``checkExplicitHosts`` so the message
        # names the malformed IPv6 literal.
        if inside.len == 0:
          raise newException(PgConfigError, "Empty IPv6 address in DSN")
        hostList.add decodeHostElem(inside, "host", hidx)
        let afterBracket = part[bracket + 1 .. ^1]
        if afterBracket.len == 0:
          portList.add ""
        elif afterBracket.startsWith(":"):
          portList.add decodeHostElem(afterBracket[1 .. ^1], "port", hidx)
        else:
          raise newException(
            PgConfigError,
            "Unexpected character after IPv6 address in DSN (len=" & $part.len & ")",
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
              PgConfigError,
              "IPv6 address in DSN must be bracketed, e.g. [::1]:5432 (len=" & $part.len &
                ")",
            )
          hostList.add decodeHostElem(part[0 ..< cpos], "host", hidx)
          portList.add decodeHostElem(part[cpos + 1 .. ^1], "port", hidx)
        else:
          hostList.add decodeHostElem(part, "host", hidx)
          portList.add ""
    result.hostList = hostList
    result.hostGiven = true
    result.hostBlank = hostList == @[""]
    result.portList = portList
    result.portGiven = portList.anyIt(it.len > 0)

  if dbpath.len > 0:
    result.add("dbname", pctDecode(dbpath, "database"), "")

  if queryStr.len > 0:
    let items = queryStr.split('&')
    for idx in 0 ..< items.len:
      let pair = items[idx]
      if pair.len == 0:
        # A trailing '&' leaves an empty item carrying no value; skip it.
        continue
      let epos = pair.find('=')
      if epos < 0:
        # A nameless item cannot set anything. Ignoring it would silently
        # drop a security-relevant parameter (e.g. `?sslmode` falling back
        # to the default), so reject it instead. The index locates the item;
        # its text may hold secrets (e.g. `password=...`) and is not echoed.
        raise newException(
          PgConfigError,
          "Missing key/value separator '=' in URI query parameter (item #" & $idx &
            ", len=" & $pair.len & ")",
        )
      var key, val: string
      try:
        key = pctDecode(pair[0 ..< epos], "query key")
        val = pctDecode(pair[epos + 1 .. ^1], "query value")
      except PgConfigError as e:
        # The item index locates the fault, mirroring the missing-'=' error
        # above; pctDecode keeps the item content out of the message.
        raise newException(
          PgConfigError, "URI query parameter (item #" & $idx & "): " & e.msg
        )
      if key.len == 0:
        # A `=value` item has a separator but no name. Like a missing '='
        # it cannot set anything, so reject it instead of storing an
        # empty-named extra parameter. This mirrors `collectKeyValueDsn`,
        # which rejects empty keys as well.
        raise newException(
          PgConfigError,
          "Empty key in URI query parameter (item #" & $idx & ", len=" & $pair.len & ")",
        )
      translateLibpqAlias(key, val, uri = true)
      result.add(key, val, "")

proc rejectRawNul(config: ConnConfig) =
  ## Reject a raw NUL in every value that leaves the process as a C string: a
  ## StartupMessage parameter, the dial target, or a certificate value. A NUL
  ## reaches here untouched in either DSN form (``pctDecode`` only rejects
  ## ``%00``), and the C boundary would silently cut the value.
  ##
  ## ``password`` is exempt: it is not a startup item, SCRAM only hashes it, and
  ## the cleartext/MD5 messages are checked when they are built.
  rejectRawNulField("user", config.user)
  rejectRawNulField("database", config.database)
  rejectRawNulField("application_name", config.applicationName)
  var i = 0
  for pair in config.extraParams:
    rejectRawNulField("extra parameter name #" & $i, pair[0])
    rejectRawNulField("extra parameter value #" & $i, pair[1])
    inc i
  rejectRawNulField("host", config.host)
  rejectRawNulField("hostaddr", config.hostaddr)
  var h = 0
  for entry in config.hosts:
    rejectRawNulField("host #" & $h, entry.host)
    rejectRawNulField("hostaddr #" & $h, entry.hostaddr)
    inc h
  # A DSN path was checked before its file read; this catches a hand-built config.
  rejectRawNulField("sslrootcert", config.sslRootCert)
  rejectRawNulField("sslcert", config.sslCert)
  rejectRawNulField("sslkey", config.sslKey)
  rejectRawNulField("passfile", config.passFile)

proc validateConnConfig*(config: var ConnConfig) =
  ## Mirror DSN guards for ``initConnConfig`` and the ``connect`` chokepoint
  ## (DSN parsers validate inline; hand-built ``ConnConfig`` is re-checked at
  ## connect time so numeric / hostaddr faults become ``PgConfigError``).
  ##
  ## An empty ``host`` with no ``hostaddr`` is rejected rather than defaulted
  ## (matching the DSN parsers), so an unset template variable cannot hand the
  ## credentials to whatever listens on localhost. The scalar
  ## ``host``/``hostaddr``/``port`` are mirrored from ``hosts[0]``, which is
  ## what ``getHosts`` dials. Negative ``connectTimeout`` becomes
  ## ``ZeroDuration``. A ``credentialsProvider`` alongside a non-empty
  ## ``password`` is rejected: which one is sent would not show.
  rejectRawNul(config)
  if config.credentialsProvider != nil and config.password.len > 0:
    raise newException(
      PgConfigError,
      "password and credentialsProvider are both set; clear password to use the provider" &
        " (parseDsn also sets it from PGPASSWORD or a service file)",
    )
  if config.connectTimeout < ZeroDuration:
    config.connectTimeout = ZeroDuration

  template checkPort(port: int) =
    if port < 1 or port > 65535:
      raise newException(PgConfigError, "Port out of range (1-65535)")

  template checkHostaddr(a: string) =
    if a.len > 0 and a[0] == '/':
      raise newException(
        PgConfigError,
        "Invalid hostaddr: must be a numeric IP address, not a Unix socket path (use host for Unix sockets)",
      )

  template checkEmptyHost(h, a: string, where: string) =
    if h.len == 0 and a.len == 0:
      raise newException(PgConfigError, "Empty host in config" & where)

  if config.hosts.len > 0:
    for i in 0 ..< config.hosts.len:
      checkEmptyHost(
        config.hosts[i].host,
        config.hosts[i].hostaddr,
        " host list (element #" & $i & ")",
      )
      checkPort(config.hosts[i].port)
      checkHostaddr(config.hosts[i].hostaddr)
    # Keep scalar back-compat fields aligned with what ``getHosts`` will dial.
    config.host = config.hosts[0].displayHost
    config.hostaddr = config.hosts[0].hostaddr
    config.port = config.hosts[0].port
  else:
    checkEmptyHost(config.host, config.hostaddr, "")
    checkPort(config.port)
    checkHostaddr(config.hostaddr)

  if config.keepAliveIdle < 0:
    raise newException(PgConfigError, "keepalives_idle must be non-negative")
  if int64(config.keepAliveIdle) > maxKeepAliveIdle:
    raise newException(
      PgConfigError, "keepalives_idle out of range (0-" & $maxKeepAliveIdle & ")"
    )
  if config.keepAliveInterval < 0:
    raise newException(PgConfigError, "keepalives_interval must be non-negative")
  if int64(config.keepAliveInterval) > maxKeepAliveInterval:
    raise newException(
      PgConfigError,
      "keepalives_interval out of range (0-" & $maxKeepAliveInterval & ")",
    )
  if config.keepAliveCount < 0:
    raise newException(PgConfigError, "keepalives_count must be non-negative")
  if int64(config.keepAliveCount) > maxKeepAliveCount:
    raise newException(
      PgConfigError, "keepalives_count out of range (0-" & $maxKeepAliveCount & ")"
    )

  if config.tcpUserTimeout < 0:
    raise newException(PgConfigError, "tcp_user_timeout must be non-negative")
  if int64(config.tcpUserTimeout) > maxTcpUserTimeout:
    raise newException(
      PgConfigError, "tcp_user_timeout out of range (0-" & $maxTcpUserTimeout & ")"
    )

  if config.maxMessageSize < 0:
    raise newException(PgConfigError, "max_message_size must be non-negative")
  if config.maxScramIterations < 0:
    raise newException(PgConfigError, "max_scram_iterations must be non-negative")
  for (k, v) in config.extraParams:
    if isGucName(k, "client_encoding"):
      checkClientEncoding(v)
    elif isGucName(k, "DateStyle"):
      checkDateStyle(v)
    elif k == "options": # the server matches this key case-sensitively
      checkOptionsPinnedParams(v)

proc initConnConfig*(
    host = DefaultHost,
    port = 5432,
    hostaddr = "",
    user = "",
    password = "",
    database = "",
    sslMode = sslPrefer,
    sslRootCert = "",
    sslCert = "",
    sslKey = "",
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
    sslNegotiation = sslnPostgres,
    passFile = "",
    tcpUserTimeout = 0,
): ConnConfig =
  ## Create a connection configuration with sensible defaults.
  ## For DSN-based configuration, use `parseDsn` instead. Unlike `parseDsn`,
  ## nothing comes from the environment: no password file unless
  ## ``passFile`` names one, and no OS user for an empty ``user``.
  ##
  ## Validates like DSN parsing; negative ``connectTimeout`` becomes
  ## ``ZeroDuration`` (no timeout). When ``hosts`` is non-empty, scalar
  ## ``host``/``hostaddr``/``port`` are overwritten from ``hosts[0]`` (same as
  ## DSN parsers). An empty ``host`` with no ``hostaddr`` is rejected, like an
  ## explicitly empty ``host`` in a DSN.
  result = ConnConfig(
    host: host,
    port: port,
    hostaddr: hostaddr,
    user: user,
    password: password,
    database: database,
    sslMode: sslMode,
    sslNegotiation: sslNegotiation,
    sslRootCert: sslRootCert,
    sslCert: sslCert,
    sslKey: sslKey,
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
    passFile: passFile,
    tcpUserTimeout: tcpUserTimeout,
  )
  validateConnConfig(result)
  validateClientCertConfig(result)

type
  DsnEnv = object ## What ``parseDsn`` reads besides the string. Tests swap it out.
    enabled: bool ## Off: the string alone, no variable, file or OS user.
    get: proc(name: string): string {.gcsafe, raises: [].}
      ## An environment variable; "" when unset or empty.
    user: proc(): tuple[name, home: string] {.gcsafe, raises: [].}
      ## The OS user (`osUserInfo`).

  LibpqOnly = enum
    loNone ## Not a libpq-only keyword: applied as usual.
    loIgnore
    loReject
    loNoClientCert ## Holds unless a client certificate is set.
    loInvalid ## A value libpq refuses too.

const
  EnvKeywords = [
    ("PGHOST", "host"),
    ("PGHOSTADDR", "hostaddr"),
    ("PGPORT", "port"),
    ("PGDATABASE", "dbname"),
    ("PGUSER", "user"),
    ("PGPASSWORD", "password"),
    ("PGPASSFILE", "passfile"),
    ("PGOPTIONS", "options"),
    ("PGAPPNAME", "application_name"),
    ("PGCONNECT_TIMEOUT", "connect_timeout"),
    ("PGCLIENTENCODING", "client_encoding"),
    ("PGREQUIREAUTH", "require_auth"),
    ("PGCHANNELBINDING", "channel_binding"),
    ("PGSSLMODE", "sslmode"),
    ("PGSSLNEGOTIATION", "sslnegotiation"),
    ("PGSSLCOMPRESSION", "sslcompression"),
    ("PGSSLCERT", "sslcert"),
    ("PGSSLKEY", "sslkey"),
    ("PGSSLCERTMODE", "sslcertmode"),
    ("PGSSLROOTCERT", "sslrootcert"),
    ("PGSSLCRL", "sslcrl"),
    ("PGSSLCRLDIR", "sslcrldir"),
    ("PGSSLSNI", "sslsni"),
    ("PGREQUIREPEER", "requirepeer"),
    ("PGSSLMINPROTOCOLVERSION", "ssl_min_protocol_version"),
    ("PGSSLMAXPROTOCOLVERSION", "ssl_max_protocol_version"),
    ("PGGSSENCMODE", "gssencmode"),
    ("PGKRBSRVNAME", "krbsrvname"),
    ("PGGSSLIB", "gsslib"),
    ("PGGSSDELEGATION", "gssdelegation"),
    ("PGTARGETSESSIONATTRS", "target_session_attrs"),
    ("PGLOADBALANCEHOSTS", "load_balance_hosts"),
    ("PGMINPROTOCOLVERSION", "min_protocol_version"),
    ("PGMAXPROTOCOLVERSION", "max_protocol_version"),
    ("PGOAUTHCAFILE", "oauth_ca_file"),
  ]
    ## libpq's environment variables for connection keywords, libpq-only
    ## ones included: `libpqOnly` decides those.
  EnvStartupParams = [("PGDATESTYLE", "DateStyle"), ("PGGEQO", "geqo")]
    ## libpq sends these as startup parameters unless set to ``default``.
    ## ``PGTZ`` is not read: it would move the session ``TimeZone``, and a
    ## ``DateTime`` parameter bound to ``timestamptz`` is sent as a zoneless
    ## timestamp, which the server then reads in that zone. A caller that wants
    ## another zone sets ``TimeZone`` in the string, ``options`` or
    ## ``extraParams``.

proc libpqOnly(key, val: string): LibpqOnly =
  ## libpq keywords this client does not implement. Ignoring one that asks
  ## for a check or a limit would quietly weaken the connection, so those are
  ## rejected unless the value asks for what already holds (TLS 1.2 or later,
  ## protocol 3.0, no GSS encryption, a client certificate only if set, or
  ## none when none is set). A value libpq itself refuses is `loInvalid`.
  ##
  ## An empty value (never a variable's, which counts as unset) is stored by
  ## libpq, which refuses it for an enum option.
  case key
  of "sslcrl", "sslcrldir", "requirepeer":
    if val.len == 0: loIgnore else: loReject
  of "sslcertmode":
    case val
    of "allow": loIgnore
    of "disable": loNoClientCert
    of "require": loReject
    else: loInvalid
  of "ssl_min_protocol_version", "ssl_max_protocol_version":
    let v = val.toLowerAscii
    if v.len == 0:
      loIgnore # libpq too
    elif v notin ["tlsv1", "tlsv1.1", "tlsv1.2", "tlsv1.3"]:
      loInvalid
    elif key == "ssl_min_protocol_version":
      if v == "tlsv1.3": loReject else: loIgnore
    else:
      # asyncdispatch may negotiate TLS 1.3, so a lower cap would not hold
      # there; chronos (BearSSL) speaks TLS 1.2 only.
      when hasChronos:
        if v in ["tlsv1.2", "tlsv1.3"]: loIgnore else: loReject
      else:
        if v == "tlsv1.3": loIgnore else: loReject
  of "gssencmode":
    case val
    of "disable", "prefer": loIgnore
    of "require": loReject
    else: loInvalid
  of "min_protocol_version":
    case val
    of "3.0": loIgnore
    of "3.2", "latest": loReject
    else: loInvalid
  of "max_protocol_version":
    # The client speaks protocol 3.0, so every value libpq reads is a cap it
    # never exceeds.
    case val
    of "3.0", "3.2", "latest": loIgnore
    else: loInvalid
  of "krbsrvname", "gsslib", "gssdelegation", "sslcompression", "sslpassword",
      "sslkeylogfile", "oauth_issuer", "oauth_client_id", "oauth_client_secret",
      "oauth_scope", "oauth_ca_file", "scram_client_key", "scram_server_key":
    loIgnore
  else:
    loNone

proc processEnv(): DsnEnv =
  DsnEnv(
    enabled: true,
    get: proc(name: string): string {.gcsafe, raises: [].} =
      getEnv(name),
    user: osUserInfo,
  )

proc libpqUserDir(env: DsnEnv): string =
  ## Where libpq looks for the user's password and service files: the home
  ## directory (``$HOME``, else the user database), on Windows
  ## ``%APPDATA%\postgresql``. "" when unknown, which skips both.
  when defined(windows):
    let appData = env.get("APPDATA")
    if appData.len > 0:
      appData & "\\postgresql"
    else:
      ""
  else:
    let home = env.get("HOME")
    if home.len > 0:
      home
    else:
      env.user().home

proc confPath(dir, name: string): string =
  when defined(windows):
    dir & "\\" & name
  else:
    dir & "/" & name

proc serviceEntries(
    path, name: string, required: bool, found: var bool
): seq[ServiceEntry] =
  ## Section `name` of the service file at `path`. A file that cannot be
  ## stat'ed (or is not a regular file) is skipped unless `required`; one that
  ## can but cannot be opened is an error, as in libpq.
  found = false
  rejectRawNulField("service file path", path)
  if not confFileStats(path):
    if required:
      raise newException(PgConfigError, "service file \"" & path & "\" not found")
    return
  var content: string
  case readConfFile(path, content)
  of cffNone, cffLoosePerms:
    discard
  of cffNotRegular:
    if required:
      raise notRegularFileError("service", path)
    return
  of cffOpen, cffStat, cffRead:
    # Not skipped: the next file may define the same name for another server.
    raise newException(PgConfigError, "Cannot read service file: " & path)
  of cffTooLarge:
    raise newException(
      PgConfigError,
      "service file exceeds size limit (" & $MaxConfFileBytes & " bytes): " & path,
    )
  try:
    parseServiceSection(content, path, name, found)
  finally:
    wipe(content) # other services' passwords

proc mergeService(f: var DsnFields, env: DsnEnv) =
  ## Fill what `f` leaves out from its service (``service``, else
  ## ``PGSERVICE``), searched as libpq does: ``PGSERVICEFILE`` or the user's
  ## file, then ``$PGSYSCONFDIR/pg_service.conf``. Files are not merged.
  var name = ""
  var given = false
  for it in f.items:
    if it.key == "service":
      name = it.val
      given = true # even "": libpq looks that up, not PGSERVICE
  if not given and env.enabled:
    name = env.get("PGSERVICE")
    given = name.len > 0
  if not given:
    return
  var found = false
  var entries: seq[ServiceEntry]
  var path, searched = ""
  template search(p: string, required: bool) =
    if not found and p.len > 0:
      entries = serviceEntries(p, name, required, found)
      path = p
      searched.add((if searched.len > 0: ", \"" else: "\"") & p & "\"")

  if env.enabled:
    let explicit = env.get("PGSERVICEFILE")
    if explicit.len > 0:
      search(explicit, true)
    else:
      let dir = libpqUserDir(env)
      if dir.len > 0:
        search(confPath(dir, ".pg_service.conf"), false)
    let sysDir = env.get("PGSYSCONFDIR")
    if sysDir.len > 0:
      search(confPath(sysDir, "pg_service.conf"), false)
  if not found:
    # The name is a value, which errors never echo.
    raise newException(
      PgConfigError,
      "definition of service not found (len=" & $name.len & ")" &
        (if searched.len > 0: " in " & searched
        else: ": no service file to search"),
    )
  for e in entries:
    let origin = "service file \"" & path & "\", line " & $e.line
    if e.key == "requiressl":
      # libpq rejects it here. Read as sslmode, it would outrank PGSSLMODE
      # and could lower a verify-full asked for there.
      raise newException(PgConfigError, "requiressl is not supported in a service file")
        .withOrigin(origin)
    if not f.has(e.key):
      f.add(e.key, e.value, origin)

proc mergeEnvironment(f: var DsnFields, env: DsnEnv) =
  ## Fill what `f` still leaves out from libpq's environment variables. An
  ## empty one counts as unset.
  for (name, key) in EnvKeywords:
    if not f.has(key):
      let val = env.get(name)
      if val.len > 0:
        f.add(key, val, name)
  if not f.has("sslmode"):
    # Deprecated; libpq still reads one starting with "1" as require.
    let val = env.get("PGREQUIRESSL")
    if val.len > 0 and val[0] == '1':
      f.add("sslmode", "require", "PGREQUIRESSL")
  # What -c switches in the string's or service's options set. PGOPTIONS's
  # do not outrank the variables below, which libpq sends after them.
  var switched: seq[string]
  for it in f.items:
    if it.key == "options" and it.origin != "PGOPTIONS":
      switched.setLen(0)
      for (name, _) in optionsSettings(it.val):
        switched.add name
  for (name, key) in EnvStartupParams:
    var val = env.get(name)
    # A -c switch in the string or service outranks the environment, though
    # the server would apply a startup parameter after it.
    if val.len == 0 or f.has(key) or switched.anyIt(isGucName(it, key)):
      continue
    if cmpIgnoreCase(val, "default") == 0:
      continue
    if key == "DateStyle":
      # The output style is pinned to ISO; keep the field order asked for.
      let tokens = val.split(',').mapIt(it.strip)
      val = tokens
        .filterIt(
          it.len > 0 and not namesNonIsoDateStyle(it) and cmpIgnoreCase(it, "ISO") != 0
        )
        .join(", ")
      if val.len == 0 and
          tokens.anyIt(cmpIgnoreCase(it.strip(chars = {'"'}), "German") == 0):
        val = "DMY" # the server's order for German when none is given
      if val.len == 0:
        continue
    f.add(key, val, name)

proc applyFields(f: DsnFields): ConnConfig =
  result.keepAlive = true
  result.sslMode = sslPrefer # libpq default; overridden by an explicit sslmode
  result.sslSni = true # libpq default
  var tlsFiles: seq[DsnItem] # from the environment or a service file
  var ownCertFiles: seq[DsnItem] # the string's sslcert/sslkey, once sslcertmode is known
  var noClientCert = none(string) # where an sslcertmode=disable came from
  var modeOrigin = ""
  var tcpUserTimeout = none(DsnItem)
  var fallbackAppName = ""
  for it in f.items:
    case it.key
    of "service":
      continue
    of "sslmode":
      modeOrigin = it.origin
    of "passfile":
      result.passFile = it.val
      continue
    of "fallback_application_name":
      fallbackAppName = it.val
      continue
    of "client_encoding":
      # psql's display encoding; this client always speaks UTF8.
      if it.origin.len > 0:
        continue
    of "tcp_user_timeout":
      # Read once the hosts and keepalives are known, as libpq reads it.
      tcpUserTimeout = some(it)
      continue
    of "sslcert", "sslkey", "sslrootcert":
      if it.origin.len > 0:
        tlsFiles.add it
        continue
      if it.key != "sslrootcert":
        ownCertFiles.add it
        continue
    else:
      discard
    case libpqOnly(it.key, it.val)
    of loIgnore:
      continue
    of loReject:
      raise newException(PgConfigError, it.key & " is not supported by this client")
        .withOrigin(it.origin)
    of loInvalid:
      raise newException(
          PgConfigError, "invalid " & it.key & " value (len=" & $it.val.len & ")"
        )
        .withOrigin(it.origin)
    of loNoClientCert:
      noClientCert = some(it.origin)
      continue
    of loNone:
      discard
    try:
      result.applyParam(it.key, it.val)
    except PgConfigError as e:
      raise e.withOrigin(it.origin)
  # The string's client certificate waits for sslcertmode. With disable,
  # libpq reads neither file (a key goes only with a certificate); one that
  # sslmode would send is refused rather than quietly left out.
  if noClientCert.isNone:
    for it in ownCertFiles:
      result.applyParam(it.key, it.val)
  elif ownCertFiles.anyIt(it.key == "sslcert" and it.val.len > 0) and
      result.sslMode notin {sslDisable, sslAllow}:
    raise newException(
        PgConfigError,
        "sslcertmode=disable is not supported by this client with a client certificate",
      )
      .withOrigin(noClientCert.get)
  # Read only if this sslmode uses it, as libpq does: an mTLS shell must not
  # break a sslmode=disable DSN, nor a missing CA one that verifies nothing,
  # nor a certificate one with sslcertmode=disable that sends none.
  # Also as libpq: a missing certificate file means none, and the key is read
  # only along with a certificate, so the key goes last.
  let sends = result.sslMode notin {sslDisable, sslAllow}
  var certOrigin = "" # of a client cert or key taken from tlsFiles
  for keyPass in [false, true]:
    for it in tlsFiles:
      if (it.key == "sslkey") != keyPass:
        continue
      if it.key == "sslrootcert" and it.val == "system":
        # libpq reads this as the system trust store and verify-full; left
        # unread it would keep sslmode at prefer.
        raise newException(
            PgConfigError, "sslrootcert=system is not supported by this client"
          )
          .withOrigin(it.origin)
      if it.key == "sslrootcert" and result.sslMode == sslRequire and
          confFileStats(it.val):
        # libpq verifies the server against a CA that is there even under
        # require; left unread it would accept any certificate.
        raise newException(PgConfigError, RootCertUnderRequireErrorMsg).withOrigin(
            it.origin
          )
      let used =
        case it.key
        of "sslrootcert":
          result.sslMode in {sslVerifyCa, sslVerifyFull}
        of "sslcert":
          sends and noClientCert.isNone and not confFileMissing(it.val)
        else:
          sends and result.sslCert.len > 0
      if used:
        try:
          result.applyParam(it.key, it.val)
        except PgConfigError as e:
          raise e.withOrigin(it.origin)
        if it.key != "sslrootcert" and certOrigin.len == 0:
          certOrigin = it.origin
  # A CA the string itself names is read whatever the sslmode, so one met by
  # require would go unread like a variable's; refuse it the same way.
  if result.sslMode == sslRequire and result.sslRootCert.len > 0:
    raise
      newException(PgConfigError, RootCertUnderRequireErrorMsg).withOrigin(modeOrigin)
  # A spelled-out empty target is a host or address fault, never a port's.
  try:
    checkExplicitHosts(f.hostList, f.addrList, f.hostGiven, f.addrGiven)
  except PgConfigError as e:
    raise e.withOrigin(joinOrigins([f.hostOrigin, f.addrOrigin]))
  result.hosts = buildHosts(
    f.hostList,
    f.addrList,
    f.portList,
    f.hostGiven,
    (host: f.hostOrigin, address: f.addrOrigin, port: f.portOrigin),
  )
  result.setHostDefaulted(not f.hostGiven and not f.addrGiven)
  if result.applicationName.len == 0:
    result.applicationName = fallbackAppName
  if tcpUserTimeout.isSome and result.keepAlive and
      not result.hosts.allIt(isUnixSocket(it.dialAddr)):
    # libpq parses it (pqParseIntParam) only for a TCP socket with keepalives
    # on; a Unix socket listed first does not excuse a later TCP host. A
    # negative value is clamped to 0, its default.
    let it = tcpUserTimeout.get
    let n = parseLibpqInt(it.val)
    if n.isNone:
      raise newException(
          PgConfigError, "invalid tcp_user_timeout value (len=" & $it.val.len & ")"
        )
        .withOrigin(it.origin)
    result.tcpUserTimeout = max(n.get, 0)
  # Back-compat: set scalar host/hostaddr/port from the first entry;
  # `host` falls back to `hostaddr` like libpq's PQhost().
  result.host = result.hosts[0].displayHost
  result.hostaddr = result.hosts[0].hostaddr
  result.port = result.hosts[0].port
  # A variable's file goes unread under disable/allow, so an sslmode fault
  # is the string's file meeting that sslmode; a pairing fault is the files'.
  let paired = (result.sslCert.len > 0) == (result.sslKey.len > 0)
  try:
    validateClientCertConfig(result)
  except PgConfigError as e:
    raise e.withOrigin(if paired: modeOrigin else: certOrigin)

proc parseDsnWith(dsn: string, env: DsnEnv): ConnConfig =
  var env = env
  if env.enabled:
    # Up to three callers below; without HOME each is a user-database lookup.
    let lookup = env.user
    var looked = false
    var info: tuple[name, home: string]
    env.user = proc(): tuple[name, home: string] {.gcsafe, raises: [].} =
      if not looked:
        info = lookup()
        looked = true
      info
  var f =
    if dsn.startsWith("postgresql://") or dsn.startsWith("postgres://"):
      collectUriDsn(dsn)
    else:
      collectKeyValueDsn(dsn)
  mergeService(f, env)
  if env.enabled:
    mergeEnvironment(f, env)
  result = applyFields(f)
  if env.enabled:
    if result.user.len == 0:
      result.user = env.user().name
    if result.passFile.len == 0:
      let dir = libpqUserDir(env)
      if dir.len > 0:
        result.passFile =
          confPath(dir, (when defined(windows): "pgpass.conf" else: ".pgpass"))
  rejectRawNul(result)

proc parseDsn*(dsn: string, environment = true): ConnConfig =
  ## Parse a PostgreSQL connection string into a ConnConfig.
  ##
  ## Supports two formats:
  ## - URI: ``postgresql://[user[:password]@][host[:port]][/database][?param=value&...]``
  ## - keyword=value: ``host=localhost port=5432 dbname=test`` (libpq compatible)
  ##
  ## Both ``postgresql://`` and ``postgres://`` schemes are accepted for URI format.
  ## A raw ``@`` in user or password parses; a raw ``/`` or ``?`` there must be
  ## percent-encoded, and a raw ``@`` in the database, or in a query with no
  ## path, raises ``PgConfigError`` (``h/db?user=a@b`` is fine).
  ##
  ## As libpq does, a setting the string leaves out (or, in a URI, leaves
  ## empty) is taken from its ``service`` (``PGSERVICE``) in the connection
  ## service file, else from libpq's environment variable (``PGHOST``,
  ## ``PGUSER``, ``PGPASSWORD``, ``PGSSLMODE``, ...; an empty one counts as
  ## unset). The service file is ``PGSERVICEFILE`` or ``~/.pg_service.conf``
  ## (``%APPDATA%\postgresql\.pg_service.conf`` on Windows), then
  ## ``$PGSYSCONFDIR/pg_service.conf``. An empty ``service`` in the string
  ## still outranks ``PGSERVICE`` and is not found, as in libpq. A missing
  ## ``user`` is the OS user, and ``ConnConfig.passFile`` defaults to
  ## ``~/.pgpass``.
  ##
  ## From a variable or the service file, ``sslcert`` and ``sslkey`` are read
  ## only for an sslmode that sends them (prefer or stronger, and not with
  ## ``sslcertmode=disable``) and
  ## ``sslrootcert`` only for verify-ca or verify-full; under require, an
  ## ``sslrootcert`` file that exists is a ``PgConfigError`` here or in the
  ## string, since libpq verifies the server against it there and this client
  ## would accept any certificate. As in libpq, a missing ``sslcert``
  ## file there means no client certificate, and ``sslkey`` is read only along
  ## with one (libpq's ``~/.postgresql`` defaults are not read), and
  ## ``sslrootcert=system`` (libpq's system trust store, which there implies
  ## verify-full) is a ``PgConfigError``. ``client_encoding`` is
  ## ignored there and ``PGDATESTYLE`` keeps only its field order (``DMY``,
  ## ...): the session is UTF8 and ISO regardless.
  ##
  ## A libpq keyword this client does not implement (``gssencmode=disable``,
  ## ``krbsrvname``, ...) is ignored wherever it is set, unless it asks for a
  ## check or a limit this client would not apply (``sslcrl``,
  ## ``requirepeer``, ``ssl_min_protocol_version=TLSv1.3``, ...), which is a
  ## ``PgConfigError``. So is a value libpq refuses too (an empty
  ## ``sslcertmode=`` in the string or the service file, ...).
  ## ``tcp_user_timeout`` is read only as libpq reads it, for a TCP host with
  ## keepalives on (a malformed value there is a ``PgConfigError``), into
  ## ``ConnConfig.tcpUserTimeout``; it is applied to the socket on Linux, as
  ## libpq applies it, and never sent to the server.
  ## ``fallback_application_name`` is the ``application_name`` when none is
  ## set. ``Application_Name`` in any case is read as ``application_name``,
  ## the server setting it names. The deprecated ``requiressl`` is read as ``sslmode`` in the
  ## string and a URI (a value starting with ``1`` is ``require``, anything
  ## else ``prefer``), and a URI's ``ssl=true`` as ``sslmode=require``; in the
  ## service file ``requiressl`` is a ``PgConfigError``, as in libpq. A key set
  ## twice in the string keeps the last value; a keyword matches only in its
  ## own case (``SSLMODE`` is a startup parameter, as libpq would refuse it).
  ## Integers follow libpq's strtol rules: surrounding spaces and a sign are
  ## taken, and a libpq keyword's value must fit a C int.
  ## ``sslsni`` is on only for a value starting with ``1``, as in libpq; a
  ## value that is not an integer is a ``PgConfigError``.
  ## ``sslcertmode=disable`` is honored: no client certificate is sent, and
  ## one in the string with it is a ``PgConfigError`` unless ``sslmode`` is
  ## ``disable`` or ``allow``, where the string's certificate and key are
  ## dropped unread (so is a key given without a certificate, in any mode).
  ## With ``environment = false`` only the string counts: no variable, no
  ## default file and no OS user; a ``service`` there then finds no file.
  ##
  ## ``client_encoding`` is always UTF8: another value, directly or via ``-c``
  ## in ``options``, raises ``PgConfigError``. Likewise ``DateStyle`` always
  ## uses the ISO output style: only its field order (``DMY``, ...) may be set.
  ## ``TimeZone`` is UTC unless set here; ``PGTZ`` is not read, since a session
  ## zone other than UTC shifts a ``DateTime`` parameter bound to
  ## ``timestamptz``. Set ``TimeZone`` in the string, ``options`` or
  ## ``extraParams`` to change it (see ``ConnConfig.extraParams``).
  ##
  ## Security: the DSN is trusted configuration — unknown keys are forwarded
  ## as StartupMessage parameters, so a typo in a security-sensitive key (e.g.
  ## ``sslmde``) is not detected. Do not pass end-user-controlled strings
  ## without sanitizing: a ``service`` with a password in its file sends it to
  ## whatever ``host`` the string names. Parse and validation failures report
  ## lengths and offsets only, never parameter values (a malformed DSN may
  ## place the password in any field), and name the variable or service file
  ## line a value came from; file I/O errors name the path for diagnosis. See
  ## ``ConnConfig`` for plaintext password handling.
  parseDsnWith(
    dsn,
    if environment:
      processEnv()
    else:
      DsnEnv(),
  )
