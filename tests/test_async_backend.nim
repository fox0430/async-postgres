import std/unittest

when defined(posix):
  from std/posix import pipe, write, close

import ../async_postgres/async_backend

# Regression tests for makeAsyncSeqByteCallback. The asyncdispatch branch used
# to construct the Future manually, so an early `return <expr>` inside `body`
# skipped the fut.complete call — chronos's {.async.} rewrote `return` but
# asyncdispatch's manual path did not, leaving behavior asymmetric.

declareAsyncCallback(TestCb, proc(): Future[seq[byte]])
declareAsyncCallback(SinkCb, proc(data: sink seq[byte]): Future[void])

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

suite "makeAsyncSinkByteCallback":
  test "delivers data to the body":
    proc run(): Future[seq[byte]] {.async.} =
      var got: seq[byte]
      let cb = makeAsyncSinkByteCallback(SinkCb):
        got = data
      await cb(@[byte(1), byte(2)])
      got

    check (waitFor run()) == @[byte(1), byte(2)]

suite "Duration":
  test "conversion helpers":
    check milliseconds(1) == nanoseconds(1_000_000)
    check seconds(1) == milliseconds(1000)
    check minutes(1) == seconds(60)
    check hours(1) == minutes(60)

  test "comparisons and arithmetic":
    check milliseconds(1) < milliseconds(2)
    check milliseconds(2) <= milliseconds(2)
    check milliseconds(5) > milliseconds(1)
    check milliseconds(1) + milliseconds(2) == milliseconds(3)
    check milliseconds(3) - milliseconds(1) == milliseconds(2)
    check ZeroDuration < milliseconds(1)

  when hasAsyncDispatch:
    test "toMilliseconds truncates sub-millisecond parts":
      check toMilliseconds(milliseconds(1500)) == 1500
      check toMilliseconds(milliseconds(1) + nanoseconds(500_000)) == 1
      check toMilliseconds(nanoseconds(999_999)) == 0

    test "$ rendering":
      check $ZeroDuration == "0ns"
      check $nanoseconds(7) == "7ns"
      check $milliseconds(5) == "5ms"
      check $seconds(2) == "2s"

suite "Moment":
  test "arithmetic and comparison":
    let m0 = Moment.now()
    check m0 <= m0
    check not (m0 < m0)
    let m1 = m0 + milliseconds(5)
    check m1 > m0
    check m1 - m0 == milliseconds(5)
    when hasAsyncDispatch:
      check m0 - m1 < ZeroDuration
    else:
      check m0 - m1 == ZeroDuration

suite "wait":
  test "returns the value on success":
    proc delayed(): Future[int] {.async.} =
      await sleepMsAsync(1)
      return 42

    check (waitFor wait(delayed(), milliseconds(1000), nil)) == 42

  test "completes for void futures":
    waitFor sleepAsync(milliseconds(1)).wait(milliseconds(1000))

  test "raises AsyncTimeoutError on timeout":
    expect AsyncTimeoutError:
      waitFor sleepAsync(milliseconds(30)).wait(milliseconds(1))

  when hasAsyncDispatch:
    test "onOrphan fires when the orphan completes after the timeout":
      var orphanCalled = false
      let fut = sleepAsync(milliseconds(30))
      expect AsyncTimeoutError:
        waitFor wait(
          fut,
          milliseconds(1),
          proc(f: Future[void]) {.gcsafe.} =
            orphanCalled = true,
        )
      waitFor fut
      check orphanCalled

    test "onOrphan is not called when wait succeeds":
      var orphanCalled = false
      waitFor wait(
        sleepAsync(milliseconds(1)),
        milliseconds(500),
        proc(f: Future[void]) {.gcsafe.} =
          orphanCalled = true,
      )
      check not orphanCalled

    test "default onOrphan drains a failing orphan":
      # The default drain reads and clears the orphan's late error so it
      # cannot resurface. This test fails without the drain.
      proc boom(): Future[void] {.async.} =
        await sleepMsAsync(20)
        raise newException(ValueError, "boom")

      let fut = boom()
      expect AsyncTimeoutError:
        waitFor wait(fut, milliseconds(1))
      waitFor sleepMsAsync(50)
      check fut.finished
      check not fut.failed
      check fut.error == nil

    test "default onOrphan drains a completing orphan with a value":
      # Exercises the non-void branch of the default drain.
      proc delayed(): Future[int] {.async.} =
        await sleepMsAsync(20)
        return 42

      let fut = delayed()
      expect AsyncTimeoutError:
        discard waitFor wait(fut, milliseconds(1))
      waitFor sleepMsAsync(50)
      check fut.finished
      check not fut.failed

  when hasChronos:
    test "the inner future is cancelled on timeout":
      let fut = sleepAsync(milliseconds(30))
      expect AsyncTimeoutError:
        waitFor wait(fut, milliseconds(1))
      waitFor sleepMsAsync(1)
      check fut.cancelled

suite "cancelAndWait":
  test "no-op under asyncdispatch, real cancel under chronos":
    let fut = sleepAsync(milliseconds(30))
    waitFor cancelAndWait(fut)
    when hasChronos:
      waitFor sleepMsAsync(1)
      check fut.cancelled
    else:
      waitFor fut

suite "asyncSpawn":
  test "runs the future to completion":
    var ran = false
    proc setFlag() {.async.} =
      ran = true

    asyncSpawn(setFlag())
    waitFor sleepMsAsync(10)
    check ran

suite "allFutures":
  test "empty list completes immediately":
    waitFor allFutures(newSeq[Future[int]]())

  test "completes when every future settles, including failures":
    proc okOne(): Future[int] {.async.} =
      await sleepMsAsync(1)
      1

    proc badOne(): Future[int] {.async.} =
      raise newException(ValueError, "boom")

    let ok = okOne()
    let bad = badOne()
    var futs = @[ok, bad]
    waitFor allFutures(futs)
    check (waitFor ok) == 1
    expect ValueError:
      discard waitFor bad

suite "completed":
  test "reflects success, failure and pending state":
    proc okOne(): Future[int] {.async.} =
      7

    proc badOne(): Future[int] {.async.} =
      raise newException(ValueError, "boom")

    let ok = okOne()
    discard waitFor ok
    check ok.completed
    let bad = badOne()
    check not bad.completed
    expect ValueError:
      discard waitFor bad

suite "cancelTimer":
  test "cancels under chronos, no-op under asyncdispatch":
    let fut = sleepAsync(milliseconds(50))
    cancelTimer(fut)
    when hasChronos:
      waitFor sleepMsAsync(1)
      check fut.cancelled
    else:
      waitFor fut

suite "scheduleSoon":
  test "runs on the next event loop tick":
    var ran = false
    scheduleSoon(
      proc() {.gcsafe.} =
        ran = true
    )
    waitFor sleepMsAsync(1)
    check ran

suite "registerFdReader":
  when defined(posix):
    test "fires when the fd becomes readable":
      var fds: array[2, cint]
      check posix.pipe(fds) == 0
      defer:
        unregisterFdReader(fds[0])
        discard posix.close(fds[0])
        discard posix.close(fds[1])
      var fired = false
      registerFdReader(
        fds[0],
        proc() {.gcsafe.} =
          fired = true,
      )
      var one: cchar = cchar('\1')
      discard posix.write(fds[1], addr one, 1)
      waitFor sleepMsAsync(20)
      check fired

suite "remainingDeadlineDuration":
  test "floors to 1ms when the deadline has passed":
    let past = Moment.now() - milliseconds(10)
    check remainingDeadlineDuration(past) == milliseconds(1)

  test "returns the remaining time for a future deadline":
    let deadline = Moment.now() + milliseconds(50)
    let rem = remainingDeadlineDuration(deadline)
    check rem > ZeroDuration
    check rem <= milliseconds(50)
