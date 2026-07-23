import std/unittest

import ../async_postgres/async_backend

# Regression tests for makeAsyncSeqByteCallback. The asyncdispatch branch used
# to construct the Future manually, so an early `return <expr>` inside `body`
# skipped the fut.complete call — chronos's {.async.} rewrote `return` but
# asyncdispatch's manual path did not, leaving behavior asymmetric.

declareAsyncCallback(TestCb, proc(): Future[seq[byte]])

suite "makeAsyncSeqByteCallback":
  test "final-expression body yields value":
    let cb = makeAsyncSeqByteCallback(TestCb):
      @[byte(1), byte(2), byte(3)]
    let r = waitFor cb()
    check r == @[byte(1), byte(2), byte(3)]

  test "early return <expr> completes with returned value":
    var called = 0
    let cb = makeAsyncSeqByteCallback(TestCb):
      inc called
      if called mod 2 == 0:
        return newSeq[byte]()
      @[byte(called)]
    check (waitFor cb()) == @[byte(1)]
    check (waitFor cb()) == newSeq[byte]()
    check (waitFor cb()) == @[byte(3)]

  test "raised exception fails the future":
    let cb = makeAsyncSeqByteCallback(TestCb):
      if true:
        raise newException(ValueError, "boom")
      @[byte(0)]
    expect ValueError:
      discard waitFor cb()
