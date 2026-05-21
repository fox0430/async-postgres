## Transaction- and savepoint-scoping macros: `withTransaction`,
## `withSavepoint`, and their deadline-bounded variants.

import std/[macros, options]

import ../[async_backend, pg_protocol, pg_connection]
import ./core

proc hasReturnStmt*(n: NimNode): bool =
  ## Check whether an AST contains a `return` statement (excluding nested
  ## proc/func/method/iterator definitions where `return` is valid).
  if n.kind == nnkReturnStmt:
    return true
  if n.kind in {
    nnkProcDef, nnkFuncDef, nnkMethodDef, nnkIteratorDef, nnkLambda, nnkDo,
    nnkConverterDef, nnkTemplateDef, nnkMacroDef,
  }:
    return false
  for child in n:
    if hasReturnStmt(child):
      return true
  return false

proc bindCleanupSkippedSyms(): tuple[fire, invalidated, failed: NimNode] {.compileTime.} =
  ## Common `bindSym` set for the `onCleanupSkipped` wiring shared by
  ## `withTransaction*` / `withSavepoint*`. Returned as a tuple so each
  ## macro can destructure in a single line instead of three.
  (bindSym"fireCleanupSkipped", bindSym"csrConnInvalidated", bindSym"csrCleanupFailed")

proc buildTxBeginAndTimeout*(arg: NimNode): tuple[beginSql, txTimeout: NimNode] =
  ## Shared helper for `withTransaction` macros.
  ## Uses `when ... is` to dispatch on the argument type at compile time.
  let buildBeginSqlSym = bindSym"buildBeginSql"
  let zeroDurSym = bindSym"ZeroDuration"
  let txOptsSym = bindSym"TransactionOptions"
  let durSym = bindSym"Duration"
  let beginSql = quote:
    when `arg` is `txOptsSym`:
      `buildBeginSqlSym`(`arg`)
    elif `arg` is `durSym`:
      "BEGIN"
    else:
      {.error: "withTransaction expects TransactionOptions or Duration".}
  let txTimeout = quote:
    when `arg` is `txOptsSym`:
      `zeroDurSym`
    elif `arg` is `durSym`:
      `arg`
    else:
      {.error: "withTransaction expects TransactionOptions or Duration".}
  (beginSql, txTimeout)

macro withTransaction*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction.
  ## On exception, ROLLBACK is issued automatically.
  ## Using `return` inside the body is a compile-time error.
  ##
  ## Usage:
  ##   conn.withTransaction:
  ##     await conn.exec(...)
  ##   conn.withTransaction(seconds(5)):
  ##     await conn.exec(...)
  ##   conn.withTransaction(TransactionOptions(isolation: ilSerializable)):
  ##     await conn.exec(...)
  ##   conn.withTransaction(TransactionOptions(...), seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **Timeout semantics:** The `timeout` argument applies *per-call* to
  ## BEGIN, COMMIT, and ROLLBACK only — it does **not** bound `body` operations.
  ## Worst-case wall-clock = BEGIN(≤timeout) + body(unbounded) +
  ## COMMIT(≤timeout) \[+ ROLLBACK(≤timeout) on failure\]. Use
  ## `withTransactionDeadline` for a single wall-clock deadline covering
  ## BEGIN, body, and COMMIT together.
  ##
  ## **On per-call timeout** (BEGIN/COMMIT/in-body): `simpleExec` invalidates
  ## the connection via `invalidateOnTimeout` (marked `csClosed`, server-side
  ## CancelRequest dispatched) and raises `PgTimeoutError`. ROLLBACK is *not*
  ## attempted on an already-closed connection — `txStatus` may still read
  ## `tsInTransaction` (stale, because no `ReadyForQuery` was received), but
  ## the `csReady` guard prevents a futile cleanup call. Standalone callers
  ## must `await conn.close()` after this error; pooled connections are
  ## dropped on release.
  var body: NimNode
  var beginSql: NimNode
  var txTimeout: NimNode
  case args.len
  of 1:
    body = args[0]
    beginSql = newStrLitNode("BEGIN")
    txTimeout = bindSym"ZeroDuration"
  of 2:
    body = args[1]
    (beginSql, txTimeout) = buildTxBeginAndTimeout(args[0])
  of 3:
    let opts = args[0]
    txTimeout = args[1]
    body = args[2]
    beginSql = newCall(bindSym"buildBeginSql", opts)
  else:
    error(
      "withTransaction expects (body), (timeout, body), (opts, body), or (opts, timeout, body)",
      args[0],
    )

  if hasReturnStmt(body):
    error(
      "'return' inside withTransaction is not allowed: COMMIT/ROLLBACK would be skipped",
      body,
    )

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let eSym = genSym(nskLet, "e")
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let csReadySym = bindSym"csReady"
  let (fireCleanupSkippedSym, csrConnInvalidatedSym, csrCleanupFailedSym) =
    bindCleanupSkippedSyms()
  let ckTxRollbackSym = bindSym"ckTxRollback"
  result = quote:
    let `connSym` = `connExpr`
    try:
      discard await `connSym`.simpleExec(`beginSql`, timeout = `txTimeout`)
      `body`
      discard await `connSym`.simpleExec("COMMIT", timeout = `txTimeout`)
    except CatchableError as `eSym`:
      # Only ROLLBACK if the server is still in a transaction AND the connection
      # is usable. After a failed COMMIT, PostgreSQL has already ended the
      # transaction (txStatus = tsIdle), so an extra ROLLBACK would just emit
      # "no transaction in progress". On per-call timeout the connection is
      # csClosed but txStatus is stale (no RFQ received) — the state guard
      # avoids dispatching a ROLLBACK that would fail at checkReady(). Both
      # the csClosed skip and any swallowed inner-ROLLBACK failure are
      # reported via `onCleanupSkipped` so the timeout path is not silent.
      if `connSym`.state != `csReadySym`:
        `fireCleanupSkippedSym`(`connSym`, `ckTxRollbackSym`, `csrConnInvalidatedSym`)
      elif `connSym`.txStatus in {`tsInTxSym`, `tsInFailedSym`}:
        try:
          discard await `connSym`.simpleExec("ROLLBACK", timeout = `txTimeout`)
        except CatchableError as `cleanupErrSym`:
          `fireCleanupSkippedSym`(
            `connSym`, `ckTxRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
          )
      raise `eSym`

proc savepointNameExpr(connSym, spName: NimNode): NimNode {.compileTime.} =
  ## Build the NimNode that produces the savepoint name at runtime.
  ## When `spName` is non-nil (caller passed an explicit name) it is used as-is.
  ## Otherwise emits `block: inc conn.portalCounter; "_sp_" & $conn.portalCounter`,
  ## which guarantees distinct names for unnamed savepoints on the same connection.
  if spName != nil:
    spName
  else:
    let portalCounterSym = ident"portalCounter"
    quote:
      block:
        inc `connSym`.`portalCounterSym`
        "_sp_" & $`connSym`.`portalCounterSym`

macro withSavepoint*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Execute `body` inside a SAVEPOINT.
  ## On exception, ROLLBACK TO SAVEPOINT is issued automatically.
  ## Using `return` inside the body is a compile-time error.
  ##
  ## Usage:
  ##   conn.withSavepoint:
  ##     await conn.exec(...)
  ##   conn.withSavepoint("my_sp"):
  ##     await conn.exec(...)
  ##   conn.withSavepoint(seconds(5)):
  ##     await conn.exec(...)
  ##   conn.withSavepoint("my_sp", seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **Note:** The savepoint name must be a string literal, not a variable
  ## (the macro uses AST node kind to distinguish name from timeout).
  ##
  ## **Timeout semantics:** The `timeout` argument applies *per-call* to
  ## SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT only — it does
  ## **not** bound `body` operations. Use `withSavepointDeadline` for a single
  ## wall-clock deadline covering all three plus the body.
  var body: NimNode
  var spName: NimNode = nil
  var spTimeout: NimNode

  case args.len
  of 1:
    # conn.withSavepoint: body
    body = args[0]
    spTimeout = bindSym"ZeroDuration"
  of 2:
    if args[0].kind == nnkStrLit:
      # conn.withSavepoint("name"): body
      spName = args[0]
      body = args[1]
      spTimeout = bindSym"ZeroDuration"
    else:
      # conn.withSavepoint(timeout): body
      spTimeout = args[0]
      body = args[1]
  of 3:
    # conn.withSavepoint("name", timeout): body
    spName = args[0]
    spTimeout = args[1]
    body = args[2]
  else:
    error(
      "withSavepoint expects (body), (name, body), (timeout, body), or (name, timeout, body)",
      args[0],
    )

  if hasReturnStmt(body):
    error(
      "'return' inside withSavepoint is not allowed: RELEASE/ROLLBACK would be skipped",
      body,
    )

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let eSym = genSym(nskLet, "e")
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let spNameSym = genSym(nskLet, "spName")
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let csReadySym = bindSym"csReady"
  let (fireCleanupSkippedSym, csrConnInvalidatedSym, csrCleanupFailedSym) =
    bindCleanupSkippedSyms()
  let ckSpRollbackSym = bindSym"ckSavepointRollback"

  let nameExpr = savepointNameExpr(connSym, spName)

  result = quote:
    let `connSym` = `connExpr`
    let `spNameSym` = `nameExpr`
    try:
      discard
        await `connSym`.simpleExec("SAVEPOINT " & `spNameSym`, timeout = `spTimeout`)
      `body`
      discard await `connSym`.simpleExec(
        "RELEASE SAVEPOINT " & `spNameSym`, timeout = `spTimeout`
      )
    except CatchableError as `eSym`:
      # Only ROLLBACK TO SAVEPOINT if the outer transaction is still alive AND
      # the connection is usable. If txStatus is tsIdle the surrounding
      # transaction has already ended, so the savepoint no longer exists.
      # On per-call timeout the connection is csClosed but txStatus is stale
      # (no RFQ received) — the state guard avoids a cleanup call that would
      # fail at checkReady(). Both the csClosed skip and any swallowed
      # inner-ROLLBACK failure are reported via `onCleanupSkipped`.
      if `connSym`.state != `csReadySym`:
        `fireCleanupSkippedSym`(`connSym`, `ckSpRollbackSym`, `csrConnInvalidatedSym`)
      elif `connSym`.txStatus in {`tsInTxSym`, `tsInFailedSym`}:
        try:
          discard await `connSym`.simpleExec(
            "ROLLBACK TO SAVEPOINT " & `spNameSym`, timeout = `spTimeout`
          )
        except CatchableError as `cleanupErrSym`:
          `fireCleanupSkippedSym`(
            `connSym`, `ckSpRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
          )
      raise `eSym`

const rollbackGraceMs* {.intdefine: "asyncPgRollbackGraceMs".}: int = 5000
  ## Compile-time override (milliseconds) for the per-call ROLLBACK / RELEASE
  ## cleanup timeout used by `*Deadline` macros after the main deadline has
  ## expired. Set via `-d:asyncPgRollbackGraceMs=<ms>` (default 5000).
  ## Must be > 0; values <= 0 fall back to the default.

const rollbackGrace* =
  if rollbackGraceMs > 0:
    milliseconds(rollbackGraceMs)
  else:
    seconds(5)
  ## Per-call timeout for ROLLBACK / RELEASE cleanup in `*Deadline` macros
  ## when the main deadline has expired. Bounds how long a failed-body
  ## cleanup can hold a connection. Derived from `rollbackGraceMs`.
  ## Exported (with `*`) because `pg_pool`'s `withTransactionDeadline` macro
  ## binds it via `bindSym` — treat as an internal knob, not user API.

macro withTransactionDeadline*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction bounded by a single
  ## wall-clock deadline that covers BEGIN, the body, and COMMIT together.
  ## Unlike `withTransaction`, the timeout does not reset between calls.
  ##
  ## Usage:
  ##   conn.withTransactionDeadline(seconds(5)):
  ##     await conn.exec(...)
  ##   conn.withTransactionDeadline(TransactionOptions(...), seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **On deadline exceeded** (`AsyncTimeoutError` from the outer `wait`):
  ## the connection is invalidated via `invalidateOnTimeout` (marked `csClosed`
  ## and a server-side CancelRequest is dispatched), then `PgTimeoutError` is
  ## raised. ROLLBACK is *not* attempted — the in-flight body operation may
  ## still own the socket under asyncdispatch, so reusing it would corrupt the
  ## protocol stream. The closed connection is dropped by the pool on release.
  ##
  ## **Standalone connections (not pooled):** callers using `PgConnection`
  ## directly must `await conn.close()` after this error. Otherwise the
  ## server-side transaction lingers until the TCP connection drop is detected,
  ## holding locks and bloating tx state. The pool variant handles this
  ## automatically when the connection is released.
  ##
  ## **On other exceptions** from the body: ROLLBACK is issued with
  ## `rollbackGrace` (5s) as a per-call timeout so cleanup runs even
  ## past the main deadline. A failed ROLLBACK is swallowed.
  ##
  ## Using `return` inside the body is a compile-time error.
  var body: NimNode
  var beginSql: NimNode
  var deadline: NimNode
  case args.len
  of 2:
    deadline = args[0]
    body = args[1]
    beginSql = newStrLitNode("BEGIN")
  of 3:
    beginSql = newCall(bindSym"buildBeginSql", args[0])
    deadline = args[1]
    body = args[2]
  else:
    error(
      "withTransactionDeadline expects (deadline, body) or (opts, deadline, body)",
      args[0],
    )

  if hasReturnStmt(body):
    error(
      "'return' inside withTransactionDeadline is not allowed: COMMIT/ROLLBACK would be skipped",
      body,
    )

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let eSym = genSym(nskLet, "e")
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "txBodyDeadline")
  let bodyFutSym = genSym(nskLet, "bodyFut")
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let csReadySym = bindSym"csReady"
  let timeoutErrSym = bindSym"AsyncTimeoutError"
  let waitSym = bindSym"wait"
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let (fireCleanupSkippedSym, csrConnInvalidatedSym, csrCleanupFailedSym) =
    bindCleanupSkippedSyms()
  let ckTxRollbackSym = bindSym"ckTxRollback"
  result = quote:
    let `connSym` = `connExpr`
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    proc `bodyFnSym`(): Future[void] {.async.} =
      discard await `connSym`.simpleExec(
        `beginSql`, timeout = `remainingSym`(`deadlineMomentSym`)
      )
      `body`
      discard await `connSym`.simpleExec(
        "COMMIT", timeout = `remainingSym`(`deadlineMomentSym`)
      )

    let `bodyFutSym` = `bodyFnSym`()
    try:
      await `waitSym`(`bodyFutSym`, `totalDurSym`)
    except `timeoutErrSym`:
      # Suppress the timeout report if COMMIT happened to complete on the same
      # tick the timer fired. `completed()` (= finished and *not* failed) is
      # required: under chronos, `wait` cancels the inner future before raising
      # `AsyncTimeoutError`, leaving it in finished+failed (CancelledError)
      # state — `finished()` would treat that as "done" and skip the
      # invalidate-and-raise path. See the matching note in pg_pool's
      # withTransactionDeadline.
      if `bodyFutSym`.completed():
        discard
      else:
        # invalidateOnTimeout marks the connection csClosed and raises
        # PgTimeoutError — control does not return from this call.
        `connSym`.invalidateOnTimeout("withTransactionDeadline exceeded")
    except CatchableError as `eSym`:
      # Skip ROLLBACK when the connection is already csClosed (e.g. a per-call
      # timeout inside body invalidated it) — checkReady would just raise and
      # the failure would be swallowed by the inner except. Both the
      # csClosed skip and any swallowed inner-ROLLBACK failure are reported
      # via `onCleanupSkipped`.
      if `connSym`.state != `csReadySym`:
        `fireCleanupSkippedSym`(`connSym`, `ckTxRollbackSym`, `csrConnInvalidatedSym`)
      elif `connSym`.txStatus in {`tsInTxSym`, `tsInFailedSym`}:
        try:
          discard await `connSym`.simpleExec("ROLLBACK", timeout = `graceSym`)
        except CatchableError as `cleanupErrSym`:
          `fireCleanupSkippedSym`(
            `connSym`, `ckTxRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
          )
      raise `eSym`

macro withSavepointDeadline*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Execute `body` inside a SAVEPOINT bounded by a single wall-clock deadline
  ## covering SAVEPOINT, the body, and RELEASE SAVEPOINT together.
  ##
  ## Usage:
  ##   conn.withSavepointDeadline(seconds(5)):
  ##     await conn.exec(...)
  ##   conn.withSavepointDeadline("my_sp", seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **On deadline exceeded:** the connection is invalidated; ROLLBACK TO
  ## SAVEPOINT is *not* attempted (see `withTransactionDeadline` rationale).
  ## Because the connection itself becomes `csClosed`, the *outer* transaction
  ## is voided as well — this macro is not a fine-grained "roll back only the
  ## savepoint on timeout" primitive. If you need the outer transaction to
  ## survive a savepoint timeout, use `withSavepoint(timeout = ...)` (per-call
  ## timeout) instead of this deadline-bounded variant.
  ##
  ## **On other body exceptions:** ROLLBACK TO SAVEPOINT runs with
  ## `rollbackGrace` per-call timeout.
  ##
  ## **Note:** Unlike `withSavepoint`, the savepoint name is positional and
  ## may be any `string` expression (literal or variable) — disambiguation by
  ## AST kind is not needed because `(name, deadline, body)` and
  ## `(deadline, body)` differ in arity.
  ## Using `return` inside the body is a compile-time error.
  var body: NimNode
  var spName: NimNode = nil
  var deadline: NimNode
  case args.len
  of 2:
    # (deadline, body)
    deadline = args[0]
    body = args[1]
  of 3:
    # (name, deadline, body)
    spName = args[0]
    deadline = args[1]
    body = args[2]
  else:
    error(
      "withSavepointDeadline expects (deadline, body) or (name, deadline, body)",
      args[0],
    )

  if hasReturnStmt(body):
    error(
      "'return' inside withSavepointDeadline is not allowed: RELEASE/ROLLBACK would be skipped",
      body,
    )

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let eSym = genSym(nskLet, "e")
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let spNameSym = genSym(nskLet, "spName")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "spBodyDeadline")
  let bodyFutSym = genSym(nskLet, "bodyFut")
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let csReadySym = bindSym"csReady"
  let timeoutErrSym = bindSym"AsyncTimeoutError"
  let waitSym = bindSym"wait"
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let (fireCleanupSkippedSym, csrConnInvalidatedSym, csrCleanupFailedSym) =
    bindCleanupSkippedSyms()
  let ckSpRollbackSym = bindSym"ckSavepointRollback"

  let nameExpr = savepointNameExpr(connSym, spName)

  result = quote:
    let `connSym` = `connExpr`
    let `spNameSym` = `nameExpr`
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    proc `bodyFnSym`(): Future[void] {.async.} =
      discard await `connSym`.simpleExec(
        "SAVEPOINT " & `spNameSym`, timeout = `remainingSym`(`deadlineMomentSym`)
      )
      `body`
      discard await `connSym`.simpleExec(
        "RELEASE SAVEPOINT " & `spNameSym`,
        timeout = `remainingSym`(`deadlineMomentSym`),
      )

    let `bodyFutSym` = `bodyFnSym`()
    try:
      await `waitSym`(`bodyFutSym`, `totalDurSym`)
    except `timeoutErrSym`:
      # See the matching `completed()` rationale in withTransactionDeadline.
      if `bodyFutSym`.completed():
        discard
      else:
        # invalidateOnTimeout marks the connection csClosed and raises
        # PgTimeoutError — control does not return from this call.
        `connSym`.invalidateOnTimeout("withSavepointDeadline exceeded")
    except CatchableError as `eSym`:
      # Skip ROLLBACK TO SAVEPOINT when the connection is already csClosed.
      # Both the csClosed skip and any swallowed inner-ROLLBACK failure are
      # reported via `onCleanupSkipped`.
      if `connSym`.state != `csReadySym`:
        `fireCleanupSkippedSym`(`connSym`, `ckSpRollbackSym`, `csrConnInvalidatedSym`)
      elif `connSym`.txStatus in {`tsInTxSym`, `tsInFailedSym`}:
        try:
          discard await `connSym`.simpleExec(
            "ROLLBACK TO SAVEPOINT " & `spNameSym`, timeout = `graceSym`
          )
        except CatchableError as `cleanupErrSym`:
          `fireCleanupSkippedSym`(
            `connSym`, `ckSpRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
          )
      raise `eSym`
