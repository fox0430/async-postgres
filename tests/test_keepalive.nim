import std/[unittest, posix]

import ../async_postgres/pg_connection {.all.}

suite "configureKeepalive":
  proc getIntSockOpt(fd: SocketHandle, level: cint, optname: cint): cint =
    var optval: cint
    var optlen: SockLen = sizeof(optval).SockLen
    let rc = getsockopt(fd, level, optname, addr optval, addr optlen)
    doAssert rc == 0, "getsockopt failed"
    optval

  proc makeSocket(): SocketHandle =
    let fd = socket(AF_INET, SOCK_STREAM, 0)
    doAssert fd != SocketHandle(-1), "socket() failed"
    fd

  test "keepAlive=false does not set SO_KEEPALIVE":
    let fd = makeSocket()
    defer:
      discard close(fd)
    var config = ConnConfig()
    config.keepAlive = false
    configureKeepalive(fd, config)
    doAssert getIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE) == 0

  test "keepAlive=true sets SO_KEEPALIVE":
    let fd = makeSocket()
    defer:
      discard close(fd)
    var config = ConnConfig()
    config.keepAlive = true
    configureKeepalive(fd, config)
    doAssert getIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE) == 1

  test "keepAlive with idle/interval/count":
    let fd = makeSocket()
    defer:
      discard close(fd)
    var config = ConnConfig()
    config.keepAlive = true
    config.keepAliveIdle = 42
    config.keepAliveInterval = 7
    config.keepAliveCount = 3
    configureKeepalive(fd, config)
    doAssert getIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE) == 1
    when defined(linux):
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPIDLE) == 42
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPINTVL) == 7
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPCNT) == 3
    elif defined(macosx):
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPALIVE) == 42
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPINTVL) == 7
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPCNT) == 3

  test "zero values use OS defaults (only SO_KEEPALIVE set)":
    let fd = makeSocket()
    defer:
      discard close(fd)
    var config = ConnConfig()
    config.keepAlive = true
    config.keepAliveIdle = 0
    config.keepAliveInterval = 0
    config.keepAliveCount = 0
    configureKeepalive(fd, config)
    doAssert getIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE) == 1

  test "keepAlive=false with timing params does not set SO_KEEPALIVE":
    let fd = makeSocket()
    defer:
      discard close(fd)
    var config = ConnConfig()
    config.keepAlive = false
    config.keepAliveIdle = 60
    config.keepAliveInterval = 10
    config.keepAliveCount = 3
    configureKeepalive(fd, config)
    doAssert getIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE) == 0

  test "partial timing (idle only)":
    let fd = makeSocket()
    defer:
      discard close(fd)
    var config = ConnConfig()
    config.keepAlive = true
    config.keepAliveIdle = 99
    configureKeepalive(fd, config)
    doAssert getIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE) == 1
    when defined(linux):
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPIDLE) == 99
    elif defined(macosx):
      doAssert getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPALIVE) == 99

  test "configureKeepalive raises on invalid fd":
    var config = ConnConfig()
    config.keepAlive = true
    expect PgError:
      configureKeepalive(SocketHandle(-1), config)
