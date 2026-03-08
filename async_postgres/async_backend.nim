## Async backend configuration module.
##
## Provides unified async framework abstraction for both asyncdispatch and chronos.
## Select the backend at compile time with `-d:asyncBackend=asyncdispatch|chronos`.
## Default backend is asyncdispatch.

const asyncBackend {.strdefine.} = "asyncdispatch"

const hasAsyncDispatch* = asyncBackend == "asyncdispatch"
  ## `true` when the asyncdispatch backend is selected.
const hasChronos* = asyncBackend == "chronos"
  ## `true` when the chronos backend is selected.

when hasChronos:
  import chronos
  export chronos

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
  import std/[asyncdispatch, monotimes]
  export asyncdispatch

  type CancelledError* = object of CatchableError
    ## Raised when an async operation is cancelled.

  type AsyncTimeoutError* = object of CatchableError
    ## Raised when an async operation times out (chronos-compatible name).

  type Duration* = distinct int64
    ## Time interval in nanoseconds. API-compatible with chronos.Duration.

  proc nanoseconds*(ns: int64): Duration =
    Duration(ns)

  proc nanoseconds*(ns: int): Duration =
    Duration(int64(ns))

  proc milliseconds*(ms: int): Duration =
    Duration(int64(ms) * 1_000_000)

  proc seconds*(s: int): Duration =
    Duration(int64(s) * 1_000_000_000)

  proc minutes*(m: int): Duration =
    Duration(int64(m) * 60_000_000_000)

  proc hours*(h: int): Duration =
    Duration(int64(h) * 3_600_000_000_000)

  const ZeroDuration* = Duration(0)

  proc `==`*(a, b: Duration): bool {.borrow.}
  proc `<`*(a, b: Duration): bool {.borrow.}
  proc `<=`*(a, b: Duration): bool {.borrow.}
  proc `>`*(a, b: Duration): bool =
    int64(a) > int64(b)

  proc `>=`*(a, b: Duration): bool =
    int64(a) >= int64(b)

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
    Moment(ticks: getMonoTime().ticks)

  proc `-`*(a, b: Moment): Duration =
    Duration(a.ticks - b.ticks)

  proc `-`*(a: Moment, b: Duration): Moment =
    Moment(ticks: a.ticks - int64(b))

  proc `+`*(a: Moment, b: Duration): Moment =
    Moment(ticks: a.ticks + int64(b))

  proc `>=`*(a, b: Moment): bool =
    a.ticks >= b.ticks

  proc `<=`*(a, b: Moment): bool =
    a.ticks <= b.ticks

  proc `<`*(a, b: Moment): bool =
    a.ticks < b.ticks

  proc `>`*(a, b: Moment): bool =
    a.ticks > b.ticks

  proc wait*[T](fut: Future[T], timeout: Duration): Future[T] {.async.} =
    ## Wait for a future with a timeout. Raises AsyncTimeoutError on timeout.
    ## API-compatible with chronos Future.wait().
    ## Note: asyncdispatch has no cancellation, so the inner future keeps running
    ## after timeout. We add a callback to suppress unhandled exception warnings.
    let ms = toMilliseconds(timeout)
    let completed = await withTimeout(fut, ms)
    if not completed:
      # Don't call fut.fail() — the inner future is still running and would
      # trigger "Future completed more than once" when it eventually finishes.
      # Instead, add a callback to suppress unhandled exception warnings.
      fut.addCallback(
        proc() =
          discard
      )
      raise newException(AsyncTimeoutError, "Timeout")
    when T is void:
      fut.read()
    else:
      return fut.read()

  proc cancelAndWait*(fut: Future[void]): Future[void] {.async.} =
    ## Cancel a future and wait for completion.
    ## asyncdispatch has no real cancellation; this is a best-effort no-op.
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
    ## No-op: asyncdispatch timers complete harmlessly and are GC'd.
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
