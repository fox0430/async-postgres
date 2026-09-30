## Compile-time seal for the send-buffer encapsulation in
## ``pg_connection/types``: outside that module the send buffer admits no
## writable alias, so bytes can only enter through the builders behind the
## staged-Close bookkeeping.
##
## This module deliberately avoids ``privateAccess(PgConnection)``: the
## backdoor makes the private field writable on purpose, which would mask the
## very widening this file pins. The connection is never built here —
## ``compiles`` only type-checks, so a nil reference is enough.

import std/[unittest]

import ../async_postgres/pg_connection/types
import ../async_postgres/pg_protocol

suite "sendBuf admits no writable alias outside types":
  test "no mutable alias reaches the send buffer":
    ## Each `not compiles` below fails the build if the view widens again: a
    ## re-added `var` accessor would admit the builder call and the `setLen`,
    ## an added setter would admit the assignment, and an exported
    ## `sendBufVar` would admit the direct call.
    var conn: PgConnection
    check not compiles(conn.sendBuf.addSync())
    check not compiles(conn.sendBuf.setLen(0))
    check not compiles(conn.sendBuf = @[byte 1])
    check not compiles(conn.sendBufVar())

  test "the buffer stays readable through the lent view":
    ## The seal is one-directional: reads keep working, so the test above
    ## cannot pass vacuously by the symbol going missing.
    static:
      doAssert compiles((var c: PgConnection; len(c.sendBuf) >= 0)),
        "the read-only sendBuf view must stay visible"
    # Real use of the protocol builder: without this import the `addSync`
    # probe above would pass vacuously on an undeclared name.
    var buf: seq[byte]
    buf.addSync()
    check buf == @syncMsg
