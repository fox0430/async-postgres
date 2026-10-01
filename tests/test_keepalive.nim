import std/unittest
from std/nativesockets import SocketHandle, getSockOptInt
when defined(posix):
  import std/posix
elif defined(windows):
  from std/winlean import nil

import ../async_postgres/async_backend
import ../async_postgres/pg_connection {.all.}
import ../async_postgres/pg_connection/buffer_io
import ../async_postgres/pg_connection/types {.all.}
import ./mock_pg_server

when hasAsyncDispatch:
  from std/asyncnet import getFd

suite "TCP_NODELAY":
  test "connect disables Nagle on the TCP socket":
    var noDelay = false

    proc t() {.async.} =
      let ms = startMockServer()
      let serverFut = acceptAndReady(ms)
      let conn = await connect(
        ConnConfig(
          host: "127.0.0.1",
          port: ms.port,
          user: "test",
          database: "test",
          sslMode: sslDisable,
        )
      )
      when hasChronos:
        let fd = conn.transport.fd
      elif hasAsyncDispatch:
        let fd = conn.socket.getFd()
      # Windows may write back a single byte; getSockOptInt starts from zero.
      when defined(windows):
        const IpprotoTcp = 6 # winsock2.h; not in winlean
        noDelay = getSockOptInt(SocketHandle(fd), IpprotoTcp, winlean.TCP_NODELAY) != 0
      else:
        noDelay = getSockOptInt(SocketHandle(fd), IPPROTO_TCP, TCP_NODELAY) != 0
      await conn.close()
      await closeClient(await serverFut)
      await closeServer(ms)

    waitFor t()
    check noDelay

# `configureKeepalive` is posix-only: Windows has no keepalive path at all, so
# `keepAlive` and its timing options are ignored there (see
# buffer_io.configureKeepalive).
when defined(posix):
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

    proc keepaliveEnabled(fd: SocketHandle): bool =
      # macOS/BSD report the flag bit (8) instead of 1.
      getIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE) != 0

    test "keepAlive=false does not set SO_KEEPALIVE":
      let fd = makeSocket()
      defer:
        discard close(fd)
      var config = ConnConfig()
      config.keepAlive = false
      configureKeepalive(fd, config)
      check not keepaliveEnabled(fd)

    test "keepAlive=true sets SO_KEEPALIVE":
      let fd = makeSocket()
      defer:
        discard close(fd)
      var config = ConnConfig()
      config.keepAlive = true
      configureKeepalive(fd, config)
      check keepaliveEnabled(fd)

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
      check keepaliveEnabled(fd)
      when defined(linux):
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPIDLE) == 42
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPINTVL) == 7
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPCNT) == 3
      elif defined(macosx):
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPALIVE) == 42
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPINTVL) == 7
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPCNT) == 3

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
      check keepaliveEnabled(fd)

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
      check not keepaliveEnabled(fd)

    test "partial timing (idle only)":
      let fd = makeSocket()
      defer:
        discard close(fd)
      var config = ConnConfig()
      config.keepAlive = true
      config.keepAliveIdle = 99
      configureKeepalive(fd, config)
      check keepaliveEnabled(fd)
      when defined(linux):
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPIDLE) == 99
      elif defined(macosx):
        check getIntSockOpt(fd, cint(posix.IPPROTO_TCP), TCP_KEEPALIVE) == 99

    test "configureKeepalive raises on invalid fd":
      var config = ConnConfig()
      config.keepAlive = true
      expect PgError:
        configureKeepalive(SocketHandle(-1), config)
