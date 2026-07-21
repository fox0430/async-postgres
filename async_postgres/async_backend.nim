## Async backend configuration module.
##
## Provides unified async framework abstraction for both asyncdispatch and chronos.
## Select the backend at compile time with `-d:asyncBackend=asyncdispatch|chronos`.
## Default backend is asyncdispatch.

import std/[macros, strutils]

const asyncBackend {.strdefine.} = "asyncdispatch"

const hasAsyncDispatch* = asyncBackend == "asyncdispatch"
  ## `true` when the asyncdispatch backend is selected.
const hasChronos* = asyncBackend == "chronos"
  ## `true` when the chronos backend is selected.

const hasTls* = hasChronos or (hasAsyncDispatch and defined(ssl))
  ## `true` when the selected backend can perform TLS. Gates every symbol
  ## whose definition depends on backend TLS APIs; keeping this in one place
  ## prevents drift between call sites and their `when` guards.

when hasChronos:
  import chronos
  export chronos

  proc wait*[T](
      fut: Future[T], timeout: Duration, onOrphan: proc(fut: Future[T]) {.gcsafe.}
  ): Future[T] {.async.} =
    ## Wait for a future with a timeout. Raises AsyncTimeoutError on timeout.
    ##
    ## ``onOrphan`` is never called under chronos — the inner future is properly
    ## cancelled on timeout, so no orphan can exist. Accepted for API parity with
    ## the asyncdispatch backend, where the hook handles futures that keep running
    ## after ``wait()`` fires.
    let _ = onOrphan
    return await chronos.wait(fut, timeout)

  proc sleepMsAsync*(ms: int): Future[void] =
    ## Sleep for `ms` milliseconds. Wrapper around chronos Duration-based API.
    sleepAsync(milliseconds(ms))

  proc cancelTimer*(fut: Future[void]) =
    ## Cancel a pending timer future to prevent future tracking warnings.
    if not fut.finished():
      fut.cancelSoon()

  proc registerFdReader*(fd: cint, cb: proc() {.gcsafe, raises: [].}) =
    ## Register a file descriptor for read-readiness notifications on the event loop.
    ## `cb` is called whenever the fd becomes readable.
    let afd = AsyncFD(fd)
    register2(afd).tryGet()

    try:
      addReader2(
        afd,
        proc(udata: pointer) {.raises: [].} =
          cb(),
        nil,
      )
        .tryGet()
    except CatchableError as e:
      discard unregister2(afd)
      raise e

  proc unregisterFdReader*(fd: cint) =
    ## Remove a previously registered read-readiness watcher from the event loop.
    let afd = AsyncFD(fd)
    discard removeReader2(afd)
    discard unregister2(afd)

  proc scheduleSoon*(cb: proc() {.gcsafe, raises: [].}) =
    ## Schedule `cb` to run on the next event loop tick.
    callSoon(
      proc(udata: pointer) {.raises: [].} =
        cb(),
      nil,
    )

elif hasAsyncDispatch:
  {.warning: "asyncdispatch backend does not support cancellation".}
  import std/[asyncdispatch, monotimes]
  export asyncdispatch

  type CancelledError* = object of CatchableError
    ## Raised when an async operation is cancelled.

  type AsyncTimeoutError* = object of CatchableError
    ## Raised when an async operation times out (chronos-compatible name).

  type Duration* = distinct int64
    ## Time interval in nanoseconds. API-compatible with chronos.Duration.

  proc nanoseconds*(ns: int64): Duration =
    ## Create a Duration from nanoseconds.
    Duration(ns)

  proc nanoseconds*(ns: int): Duration =
    ## Create a Duration from nanoseconds.
    Duration(int64(ns))

  proc milliseconds*(ms: int): Duration =
    ## Create a Duration from milliseconds.
    Duration(int64(ms) * 1_000_000)

  proc seconds*(s: int): Duration =
    ## Create a Duration from seconds.
    Duration(int64(s) * 1_000_000_000)

  proc minutes*(m: int): Duration =
    ## Create a Duration from minutes.
    Duration(int64(m) * 60_000_000_000)

  proc hours*(h: int): Duration =
    ## Create a Duration from hours.
    Duration(int64(h) * 3_600_000_000_000)

  const ZeroDuration* = Duration(0) ## A zero-length duration.

  proc `==`*(a, b: Duration): bool {.borrow.}
  proc `<`*(a, b: Duration): bool {.borrow.}
  proc `<=`*(a, b: Duration): bool {.borrow.}

  proc `-`*(a, b: Duration): Duration {.borrow.}
  proc `+`*(a, b: Duration): Duration {.borrow.}

  proc `$`*(d: Duration): string =
    let ns = int64(d)
    if ns == 0:
      "0ns"
    elif ns mod 1_000_000_000 == 0:
      $(ns div 1_000_000_000) & "s"
    elif ns mod 1_000_000 == 0:
      $(ns div 1_000_000) & "ms"
    else:
      $ns & "ns"

  proc toMilliseconds*(d: Duration): int =
    ## Convert Duration to milliseconds (for asyncdispatch APIs).
    int(int64(d) div 1_000_000)

  type Moment* = object ## Monotonic timestamp. API-compatible with chronos.Moment.
    ticks: int64 ## nanoseconds

  proc now*(T: typedesc[Moment]): Moment =
    ## Get the current monotonic timestamp.
    Moment(ticks: getMonoTime().ticks)

  proc `-`*(a, b: Moment): Duration =
    Duration(a.ticks - b.ticks)

  proc `-`*(a: Moment, b: Duration): Moment =
    Moment(ticks: a.ticks - int64(b))

  proc `+`*(a: Moment, b: Duration): Moment =
    Moment(ticks: a.ticks + int64(b))

  proc `<=`*(a, b: Moment): bool =
    a.ticks <= b.ticks

  proc `<`*(a, b: Moment): bool =
    a.ticks < b.ticks

  proc wait*[T](
      fut: Future[T], timeout: Duration, onOrphan: proc(fut: Future[T]) {.gcsafe.} = nil
  ): Future[T] {.async.} =
    ## Wait for a future with a timeout. Raises AsyncTimeoutError on timeout.
    ## API-compatible with chronos Future.wait().
    ##
    ## ``onOrphan`` cleanup hook
    ## asyncdispatch has **no cancellation**. When a timeout fires, the inner
    ## future keeps running in the background until its I/O completes. This
    ## ``onOrphan`` callback is registered on the inner future and called once
    ## that orphan eventually completes. Use it to close a live connection or
    ## release other resources the orphan holds. Without it the orphan produces
    ## an unhandled ``FutureCompleted`` warning at best and a leaked socket /
    ## server slot at worst.
    ##
    ## Under chronos the ``onOrphan`` argument is accepted but never called —
    ## futures are properly cancelled on timeout and no orphan remains.
    let ms = toMilliseconds(timeout)
    let completed = await withTimeout(fut, ms)
    if not completed:
      fut.addCallback(
        proc() =
          if not onOrphan.isNil:
            onOrphan(fut)
      )
      raise newException(AsyncTimeoutError, "Timeout")
    when T is void:
      fut.read()
    else:
      return fut.read()

  proc cancelAndWait*(fut: Future[void]): Future[void] {.async.} =
    ## Cancel a future and wait for completion.
    ##
    ## .. warning::
    ##   On asyncdispatch this is a **no-op** — the future is neither cancelled
    ##   nor awaited. asyncdispatch has no cancellation primitive. Callers must
    ##   not assume the future has stopped: any buffer it holds via
    ##   `addr` remains live, and any socket write it scheduled will
    ##   still complete. Do not reuse the affected resource (socket, buffer)
    ##   after calling this under asyncdispatch. chronos cancels the future
    ##   properly.
    discard

  proc asyncSpawn*(fut: Future[void]) =
    ## Fire-and-forget a future. If the future fails with an unhandled exception,
    ## a Defect is raised (matching chronos behaviour).
    ## The caller is responsible for error handling inside the async proc.
    fut.addCallback(
      proc() =
        if fut.failed:
          let msg = "Async procedure failed: " & fut.error.msg
          raiseAssert msg
    )

  proc completed*[T](fut: Future[T]): bool =
    ## Check if a future completed successfully (not failed).
    ## Chronos-compatible name for `finished and not failed`.
    fut.finished() and not fut.failed()

  proc allFutures*[T](futures: seq[Future[T]]): Future[void] =
    ## Wait for all futures to complete (success or failure).
    let retFuture = newFuture[void]("allFutures")
    if futures.len == 0:
      retFuture.complete()
      return retFuture
    var remaining = futures.len
    for f in futures:
      f.addCallback(
        proc() =
          dec remaining
          if remaining == 0:
            retFuture.complete()
      )
    return retFuture

  proc sleepAsync*(d: Duration): Future[void] =
    ## Sleep for the given Duration.
    asyncdispatch.sleepAsync(toMilliseconds(d))

  proc sleepMsAsync*(ms: int): Future[void] =
    ## Sleep for `ms` milliseconds.
    asyncdispatch.sleepAsync(ms)

  proc cancelTimer*(fut: Future[void]) =
    ## No-op under asyncdispatch: timers cannot be cancelled, but they complete
    ## harmlessly and are garbage-collected. Provided for API parity with
    ## chronos, which does cancel the timer via `cancelSoon`.
    discard

  proc registerFdReader*(fd: cint, cb: proc() {.gcsafe, raises: [].}) =
    ## Register a file descriptor for read-readiness notifications on the event loop.
    ## `cb` is called whenever the fd becomes readable.
    let afd = AsyncFD(fd)
    register(afd)
    try:
      addRead(
        afd,
        proc(fd: AsyncFD): bool =
          cb()
          return false # keep watching; unregister via unregisterFdReader
        ,
      )
    except CatchableError as e:
      unregister(afd)
      raise e

  proc unregisterFdReader*(fd: cint) =
    ## Remove a previously registered read-readiness watcher from the event loop.
    unregister(AsyncFD(fd))

  proc scheduleSoon*(cb: proc() {.gcsafe, raises: [].}) =
    ## Schedule `cb` to run on the next event loop tick.
    callSoon(
      proc() =
        cb()
    )

else:
  {.fatal: "Unknown asyncBackend. Use -d:asyncBackend=asyncdispatch|chronos".}

macro declareAsyncCallback*(
    name: untyped, procType: untyped, doc: static string = ""
): untyped =
  ## Declare `name*` as an async callback proc-type, injecting
  ## backend-appropriate pragmas so callers do not repeat
  ## `when hasChronos: ... else: ...` blocks.
  ##
  ## chronos gets `{.async: (raises: [CatchableError]), gcsafe.}`; asyncdispatch
  ## gets `{.gcsafe.}`. `procType` must be a bare proc-type expression with no
  ## pragmas attached. Optional `doc` becomes the emitted type's docstring.
  when hasChronos:
    const pragmas = "{.async: (raises: [CatchableError]), gcsafe.}"
  else:
    const pragmas = "{.gcsafe.}"
  var src = "type " & $name & "* = " & procType.repr & " " & pragmas
  if doc.len > 0:
    src.add("\n  ## ")
    src.add(doc.replace("\n", "\n  ## "))
  result = parseStmt(src)

template makeAsyncSinkByteCallback*(cbType: typedesc, body: untyped): untyped =
  ## Build a `cbType` async callback receiving `data: sink seq[byte]` and
  ## returning `Future[void]`. Split from a generic void-shape maker because
  ## splicing the parameter type through an `untyped` template param confuses
  ## asyncdispatch's `{.async.}` macro; keeping the whole parameter literal
  ## sidesteps that. `data` is injected into `body`'s scope.
  block:
    when hasChronos:
      let r: cbType = proc(
          data {.inject.}: sink seq[byte]
      ) {.async: (raises: [CatchableError]).} =
        body
      r
    else:
      let r: cbType = proc(data {.inject.}: sink seq[byte]) {.async.} =
        body
      r

template makeAsyncSeqByteCallback*(
    cbType: typedesc, futName: static string, body: untyped
): untyped =
  ## Build a `cbType` producer callback returning `Future[seq[byte]]`.
  ## Under asyncdispatch `body` must be synchronous: `async` cannot annotate a
  ## non-void anonymous proc, so the Future is constructed manually.
  block:
    when hasChronos:
      let r: cbType = proc(): Future[seq[byte]] {.async: (raises: [CatchableError]).} =
        body
      r
    else:
      let r: cbType = proc(): Future[seq[byte]] {.gcsafe.} =
        let fut = newFuture[seq[byte]](futName)
        try:
          let res: seq[byte] = body
          fut.complete(res)
        except CatchableError as e:
          fut.fail(e)
        return fut
      r

proc remainingDeadlineDuration*(deadline: Moment): Duration =
  ## Compute the remaining Duration until `deadline`. When the deadline has
  ## passed, returns 1 millisecond so the next `wait()` / `simpleExec(timeout)`
  ## fires reliably. A `nanoseconds(1)` floor would be ignored by chronos's
  ## `wait` when the awaited future completes synchronously (e.g. socket recv
  ## with bytes already buffered), letting the loop run indefinitely; 1ms is
  ## above chronos's timer resolution so the timer fires deterministically.
  ## Returning `ZeroDuration` would mean "no timeout" in this codebase — the
  ## opposite of what a missed deadline should do.
  ##
  ## **Practical minimum:** because of the 1ms floor and per-call event-loop
  ## scheduling overhead, deadlines smaller than a few milliseconds are not
  ## meaningfully enforced — the floor will kick in on the first call and
  ## subsequent ms-level waits still need an event-loop tick to fire. Useful
  ## deadlines for the `*Deadline` helpers start in the tens of milliseconds.
  let now = Moment.now()
  if deadline <= now:
    milliseconds(1)
  else:
    deadline - now
