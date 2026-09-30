## Transaction- and savepoint-scoping macros: `withTransaction`,
## `withSavepoint`, and their deadline-bounded variants.
##
## Internal module: not part of the public API. Import the `pg_client` hub instead.

import std/[macros, sequtils]

import ../[async_backend, pg_protocol]
import ../pg_connection/[types, simple_query]
import core

const routineDefKinds = {
  nnkProcDef, nnkFuncDef, nnkMethodDef, nnkIteratorDef, nnkLambda, nnkDo,
  nnkConverterDef, nnkTemplateDef, nnkMacroDef,
} ## Nested definitions whose `return` / `break` / `continue` can't leave the body.

proc sameLabel(a, b: NimNode): bool =
  ## Whether two `break`/`block` labels name the same block. Typed labels are
  ## compared as symbols, so a template's gensym'd label never matches a
  ## caller's label of the same name; untyped ones can only be compared by name.
  if a.kind == nnkSym and b.kind == nnkSym:
    a == b
  else:
    eqIdent(a, b)

proc escapingStmts(body: NimNode, typed = false): seq[NimNode] =
  ## Every `return` / `break` / `continue` that may leave `body`, skipping nested
  ## routines. Unexpanded, a body-local template's `return` counts (asyncdispatch's
  ## `async` rewrites it first); its `break`/`continue` are left to the typed walk.
  proc walk(
      n: NimNode,
      breakCaptured, continueCaptured, inTemplate: bool,
      labels: var seq[NimNode],
      found: var seq[NimNode],
  ) =
    case n.kind
    of nnkTemplateDef:
      if not typed:
        for child in n:
          walk(child, breakCaptured, continueCaptured, true, labels, found)
    of routineDefKinds - {nnkTemplateDef}:
      discard
    of nnkReturnStmt:
      found.add(n)
    of nnkContinueStmt:
      # Nim rejects labeled `continue`; unlabeled ones bind to the innermost loop.
      if not (inTemplate or continueCaptured):
        found.add(n)
    of nnkBreakStmt:
      if not inTemplate:
        let escapes =
          if n.len > 0 and n[0].kind != nnkEmpty:
            not labels.anyIt(sameLabel(it, n[0]))
          else:
            not breakCaptured
        if escapes:
          found.add(n)
    of nnkWhileStmt:
      # A `break` in the condition leaves this loop; a `continue` there doesn't.
      walk(n[0], true, continueCaptured, inTemplate, labels, found)
      walk(n[1], true, true, inTemplate, labels, found)
    of nnkForStmt:
      for i in 0 ..< n.len - 1:
        walk(n[i], breakCaptured, continueCaptured, inTemplate, labels, found)
      walk(n[^1], true, true, inTemplate, labels, found)
    of nnkBlockStmt, nnkBlockExpr:
      let hasLabel = n[0].kind != nnkEmpty
      if hasLabel:
        labels.add(n[0])
      for child in n:
        walk(child, true, continueCaptured, inTemplate, labels, found)
      if hasLabel:
        labels.setLen(labels.len - 1)
    else:
      for child in n:
        walk(child, breakCaptured, continueCaptured, inTemplate, labels, found)

  var labels: seq[NimNode]
  walk(body, false, false, false, labels, result)

proc rejectEscape(stmt: NimNode, macroName, cleanup: string, hidden: bool) =
  ## Raise the compile error for an escaping `stmt`; `hidden` marks one that
  ## comes from a template's expansion rather than the body itself.
  let what =
    if stmt.kind == nnkReturnStmt: "'return' inside" else: "'break'/'continue' escaping"
  let hint = if hidden: " (hidden inside a template)" else: ""
  error(
    what & " " & macroName & hint & " is not allowed: " & cleanup & " would be skipped",
    stmt,
  )

macro checkNoBodyEscapePost*(
    copy: typed, macroName, cleanup: static string, body: untyped
): untyped =
  ## Reject control flow that template expansions hid in the typed `copy`, then
  ## expand to the untyped `body`: re-splicing typed type sections breaks them.
  ## The `while false:` wrapper lets a stray `break`/`continue` type-check.
  if copy.kind != nnkWhileStmt or copy.len != 2:
    error(
      "internal: " & macroName & " body escape check expects the body copy " &
        "wrapped in `while false:`, got " & $copy.kind,
      copy,
    )
  for stmt in escapingStmts(copy[1], typed = true):
    rejectEscape(stmt, macroName, cleanup, hidden = true)
  result = body

proc checkNoBodyEscape*(body: NimNode, macroName, cleanup: string): NimNode =
  ## Reject control flow inside a scoped macro `body` that would bypass the
  ## trailing `cleanup` (`"COMMIT/ROLLBACK"`, `"RELEASE/ROLLBACK"`, a release
  ## or close): a `return`, or a `break`/`continue` that escapes the body to an
  ## enclosing loop. Either would skip the cleanup the macro appends after
  ## `body`, silently discarding the transaction's work or leaking the
  ## resource. Shared by every scoped construct (conn / pool / cluster).
  ##
  ## Returns the body to splice; its typed copy is type-checked too, so a body
  ## nested n constructs deep is type-checked 2^n times.
  for stmt in escapingStmts(body):
    rejectEscape(stmt, macroName, cleanup, hidden = false)
  newCall(
    bindSym"checkNoBodyEscapePost",
    nnkWhileStmt.newTree(newLit(false), body.copyNimTree),
    newLit(macroName),
    newLit(cleanup),
    body,
  )

macro checkTemplateBodyEscape*(
    body: untyped, macroName, cleanup: static string
): untyped =
  ## `checkNoBodyEscape` for template-based scoped constructs, which can't call
  ## a compile-time proc directly on their untyped `body`. Use it as the body's
  ## only occurrence: it expands to the checked body.
  checkNoBodyEscape(body, macroName, cleanup)

proc bindCleanupSkippedSyms(): tuple[fire, invalidated, failed: NimNode] {.compileTime.} =
  ## Common `bindSym` set for the `onCleanupSkipped` wiring shared by
  ## `withTransaction*` / `withSavepoint*`. Returned as a tuple so each
  ## macro can destructure in a single line instead of three.
  (bindSym"fireCleanupSkipped", bindSym"csrConnInvalidated", bindSym"csrCleanupFailed")

proc buildTxBeginAndTimeout*(
    arg: NimNode, macroName = "withTransaction"
): tuple[beginSql, txTimeout: NimNode] =
  ## Shared helper for `withTransaction` macros.
  ## Uses `when ... is` to dispatch on the argument type at compile time.
  ## `macroName` is interpolated into the type-mismatch error so callers like
  ## `withTransactionRetry` report their own name rather than `withTransaction`.
  let buildBeginSqlSym = bindSym"buildBeginSql"
  let zeroDurSym = bindSym"ZeroDuration"
  let txOptsSym = bindSym"TransactionOptions"
  let durSym = bindSym"Duration"
  let errMsg = newLit(macroName & " expects TransactionOptions or Duration")
  let beginSql = quote:
    when `arg` is `txOptsSym`:
      `buildBeginSqlSym`(`arg`)
    elif `arg` is `durSym`:
      "BEGIN"
    else:
      {.error: `errMsg`.}
  let txTimeout = quote:
    when `arg` is `txOptsSym`:
      `zeroDurSym`
    elif `arg` is `durSym`:
      `arg`
    else:
      {.error: `errMsg`.}
  (beginSql, txTimeout)

proc checkCommitTag(conn: PgConnection, tag: string) =
  ## Raise when COMMIT came back as ROLLBACK: the server ends an aborted
  ## transaction that way, without an ErrorResponse, so one is synthesized.
  ## Same SQLSTATE as the server's own answer to RELEASE SAVEPOINT in that state.
  ## The error that aborted the transaction, when known, becomes its `parent`.
  if tag == "ROLLBACK":
    let causeFields = conn.txAbortFields
    conn.txAbortFields.setLen(0)
    let causeMsg = getErrorField(causeFields, 'M')
    let causeState = getErrorField(causeFields, 'C')
    var fields = @[
      ErrorField(code: 'S', value: "ERROR"),
      ErrorField(code: 'V', value: "ERROR"),
      ErrorField(code: 'C', value: SqlStateInFailedSqlTransaction),
      ErrorField(
        code: 'M', value: "COMMIT rolled back a transaction aborted by an earlier error"
      ),
    ]
    if causeMsg.len > 0 and causeState.len > 0:
      fields.add ErrorField(
        code: 'D', value: "Aborted by: " & causeMsg & " (SQLSTATE " & causeState & ")"
      )
    fields.add ErrorField(
      code: 'H',
      value:
        "An earlier statement failed and its error did not propagate: it was " &
        "caught without re-raising (use withSavepoint to recover from an error " &
        "and continue), returned in executeIsolated's per-op errors, or " &
        "reported only to a tracer hook, as withAdvisoryLock does with an " &
        "unlock failure (onAdvisoryUnlockFailed).",
    )
    let err = newPgQueryError(fields)
    if causeFields.len > 0:
      err.parent = newPgQueryError(causeFields)
    raise err

proc commitTx*(conn: PgConnection, timeout: Duration): Future[void] {.async.} =
  ## COMMIT for the conn/pool/cluster transaction macros. `checkCommitTag` runs
  ## inside the trace, so `onQueryEnd` reports the rollback the caller sees.
  var tag: string
  tracedSimpleExec(conn, "COMMIT", timeout, "COMMIT timed out", tag):
    conn.checkCommitTag(tag)

proc buildRollbackCleanup*(connSym, rollbackTimeout: NimNode): NimNode =
  ## Build the shared `onCleanupSkipped`-wired ROLLBACK cleanup used on a failed
  ## attempt by the conn/pool/cluster transaction macros: skip ROLLBACK on an
  ## invalidated connection (reported as `csrConnInvalidated` via
  ## `onCleanupSkipped`) or a server-ended transaction (`tsIdle`, silent),
  ## otherwise ROLLBACK with `rollbackTimeout` and report a swallowed failure.
  ##
  ## Cancelled and plain-failure ROLLBACKs are both reported and swallowed so
  ## the enclosing `except` can re-raise the original body error.
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let cleanupCancelSym = genSym(nskLet, "cleanupCancel")
  let cleanupDefectSym = genSym(nskLet, "cleanupDefect")
  let csReadySym = bindSym"csReady"
  let stateSym = bindSym"state"
  let txStatusSym = bindSym"txStatus"
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let (fireCleanupSkippedSym, csrConnInvalidatedSym, csrCleanupFailedSym) =
    bindCleanupSkippedSyms()
  let ckTxRollbackSym = bindSym"ckTxRollback"
  quote:
    if `stateSym`(`connSym`) != `csReadySym`:
      `fireCleanupSkippedSym`(`connSym`, `ckTxRollbackSym`, `csrConnInvalidatedSym`)
    elif `txStatusSym`(`connSym`) in {`tsInTxSym`, `tsInFailedSym`}:
      try:
        discard await `connSym`.simpleExec("ROLLBACK", timeout = `rollbackTimeout`)
      except CancelledError as `cleanupCancelSym`:
        `fireCleanupSkippedSym`(
          `connSym`, `ckTxRollbackSym`, `csrCleanupFailedSym`, `cleanupCancelSym`
        )
      except CatchableError as `cleanupErrSym`:
        `fireCleanupSkippedSym`(
          `connSym`, `ckTxRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
        )
      except Defect as `cleanupDefectSym`:
        # Same-frame Defect from the ROLLBACK: report and swallow like any
        # cleanup failure, so it can't replace the body error being re-raised.
        `fireCleanupSkippedSym`(
          `connSym`,
          `ckTxRollbackSym`,
          `csrCleanupFailedSym`,
          newException(PgError, `cleanupDefectSym`.msg, `cleanupDefectSym`),
        )

proc buildSavepointRollbackCleanup(
    connSym, spNameSym, rollbackTimeout: NimNode
): NimNode =
  ## Build the shared `onCleanupSkipped`-wired ROLLBACK TO SAVEPOINT cleanup used
  ## on a failed body by `withSavepoint` / `withSavepointDeadline`: skip on an
  ## invalidated connection (reported as `csrConnInvalidated` via
  ## `onCleanupSkipped`) or an ended surrounding transaction (`tsIdle`, silent),
  ## otherwise ROLLBACK TO SAVEPOINT with `rollbackTimeout` and report a
  ## swallowed failure. The caller binds `spNameSym` (already quoted via
  ## `quoteIdentifier`) in the surrounding scope.
  ##
  ## Cancelled cleanup is swallowed as in `buildRollbackCleanup`.
  let cleanupErrSym = genSym(nskLet, "cleanupErr")
  let cleanupCancelSym = genSym(nskLet, "cleanupCancel")
  let cleanupDefectSym = genSym(nskLet, "cleanupDefect")
  let csReadySym = bindSym"csReady"
  let stateSym = bindSym"state"
  let txStatusSym = bindSym"txStatus"
  let tsInTxSym = bindSym"tsInTransaction"
  let tsInFailedSym = bindSym"tsInFailedTransaction"
  let (fireCleanupSkippedSym, csrConnInvalidatedSym, csrCleanupFailedSym) =
    bindCleanupSkippedSyms()
  let ckSpRollbackSym = bindSym"ckSavepointRollback"
  quote:
    if `stateSym`(`connSym`) != `csReadySym`:
      `fireCleanupSkippedSym`(`connSym`, `ckSpRollbackSym`, `csrConnInvalidatedSym`)
    elif `txStatusSym`(`connSym`) in {`tsInTxSym`, `tsInFailedSym`}:
      try:
        discard await `connSym`.simpleExec(
          "ROLLBACK TO SAVEPOINT " & `spNameSym`, timeout = `rollbackTimeout`
        )
      except CancelledError as `cleanupCancelSym`:
        `fireCleanupSkippedSym`(
          `connSym`, `ckSpRollbackSym`, `csrCleanupFailedSym`, `cleanupCancelSym`
        )
      except CatchableError as `cleanupErrSym`:
        `fireCleanupSkippedSym`(
          `connSym`, `ckSpRollbackSym`, `csrCleanupFailedSym`, `cleanupErrSym`
        )
      except Defect as `cleanupDefectSym`:
        # Same-frame Defect from the ROLLBACK TO SAVEPOINT: report and swallow
        # like any cleanup failure, so it can't replace the body error being
        # re-raised.
        `fireCleanupSkippedSym`(
          `connSym`,
          `ckSpRollbackSym`,
          `csrCleanupFailedSym`,
          newException(PgError, `cleanupDefectSym`.msg, `cleanupDefectSym`),
        )

proc buildDeadlineAwaitAndTimeout(
    connSym, bodyFnSym, totalDurSym: NimNode, reason: string, catchableCleanup: NimNode
): NimNode =
  ## Build the single-attempt deadline-bounded await + timeout handler shared
  ## by `withTransactionDeadline` and `withSavepointDeadline`. Kicks off
  ## `bodyFnSym()` under `wait(totalDur)`; on `AsyncTimeoutError`, suppresses
  ## the report if the body completed on the same tick the timer fired.
  ## On any other error, runs `catchableCleanup` then rethrows.
  ##
  ## An expired deadline splits on whether the body is still running, because
  ## the cleanup is owed to whoever holds the connection. A body that could not
  ## be cancelled (asyncdispatch) still owns it, so the connection is retired
  ## and the orphan cannot commit a timed-out transaction. A body that unwound
  ## hands its obligation back to the scope, so `catchableCleanup` runs before
  ## the timeout is reported.
  ##
  ## `completed()` (finished and *not* failed) is required in the timeout
  ## branch: under chronos, `wait` cancels the inner future before raising
  ## `AsyncTimeoutError`, leaving it in finished+failed (CancelledError)
  ## state — `finished()` would treat that as "done" and skip the
  ## invalidate-and-raise path. See the matching note in pg_pool's
  ## withTransactionDeadline.
  ##
  ## Cancel skips cleanup and invalidates (idempotent; chronos can run both
  ## timeout arms for one deadline).
  let bodyFutSym = genSym(nskLet, "bodyFut")
  let eSym = genSym(nskLet, "e")
  let cancelSym = genSym(nskLet, "cancel")
  let timeoutErrSym = bindSym"AsyncTimeoutError"
  let waitSym = bindSym"wait"
  let invalidateCancelSym = bindSym"invalidateOnCancel"
  let retireSym = bindSym"retireOnTimeout"
  let timeoutCleanup = catchableCleanup.copyNimTree()
  let reasonLit = newStrLitNode(reason)
  quote:
    let `bodyFutSym` = `bodyFnSym`()
    try:
      await `waitSym`(`bodyFutSym`, `totalDurSym`)
    except `timeoutErrSym`:
      if `bodyFutSym`.completed():
        discard
      elif not `bodyFutSym`.finished():
        `retireSym`(`connSym`, `reasonLit`)
      else:
        `timeoutCleanup`
        `connSym`.invalidateOnTimeout(`reasonLit`)
    except CancelledError as `cancelSym`:
      `invalidateCancelSym`(`connSym`, releaseTransport = false)
      raise `cancelSym`
    except CatchableError as `eSym`:
      `catchableCleanup`
      raise `eSym`

proc buildRetryTxLoop*(
    connSym, retryOptsSym, beginSql, txTimeout, body: NimNode
): NimNode =
  ## Build the shared BEGIN/body/COMMIT/ROLLBACK retry loop reused by every
  ## `withTransactionRetry` variant (conn / pool / cluster). The caller binds
  ## `connSym` (a standalone connection, or a pooled/primary handle it acquired)
  ## and `retryOptsSym` (a `RetryOptions`) in the surrounding scope, then splices
  ## the returned loop in.
  ##
  ## On a failed attempt the connection is cleaned up with the shared
  ## `onCleanupSkipped`-wired ROLLBACK (`buildRollbackCleanup`): ROLLBACK is
  ## skipped on an invalidated connection or when the server already ended the
  ## transaction, and both the skip and any swallowed ROLLBACK failure are
  ## surfaced through `onCleanupSkipped`.
  ##
  ## A retry is taken only when attempts remain, the error is retryable, and the
  ## connection is back to a clean reusable state (`csReady` + `tsIdle`) — so a
  ## `csClosed` (timeout) connection or a failed ROLLBACK ends the loop. Between
  ## attempts it sleeps for `backoffDelayMs`.
  let attemptSym = genSym(nskVar, "attempt")
  let eSym = genSym(nskLet, "e")
  let dSym = genSym(nskLet, "d")
  let cancelSym = genSym(nskLet, "cancel")
  let csReadySym = bindSym"csReady"
  let stateSym = bindSym"state"
  let txStatusSym = bindSym"txStatus"
  let tsIdleSym = bindSym"tsIdle"
  let isRetryableSym = bindSym"isRetryableTxError"
  let backoffSym = bindSym"backoffDelayMs"
  let sleepSym = bindSym"sleepMsAsync"
  let invalidateCancelSym = bindSym"invalidateOnCancel"

  let cleanup = buildRollbackCleanup(connSym, txTimeout)
  let commitSym = bindSym"commitTx"

  quote:
    var `attemptSym` = 0
    while true:
      inc `attemptSym`
      try:
        discard await `connSym`.simpleExec(`beginSql`, timeout = `txTimeout`)
        `body`
        await `commitSym`(`connSym`, `txTimeout`)
        break
      except CancelledError as `cancelSym`:
        # Never retry cancel; invalidate and let outer release tear down transport.
        `invalidateCancelSym`(`connSym`, releaseTransport = false)
        raise `cancelSym`
      except CatchableError as `eSym`:
        `cleanup`
        if `attemptSym` < `retryOptsSym`.maxAttempts and
            `isRetryableSym`(`eSym`, `retryOptsSym`.retryableStates) and
            `stateSym`(`connSym`) == `csReadySym` and
            `txStatusSym`(`connSym`) == `tsIdleSym`:
          await `sleepSym`(`backoffSym`(`retryOptsSym`, `attemptSym`))
          continue
        raise `eSym`
      except Defect as `dSym`:
        `cleanup`
        # Re-raise; deadline variants wrap.
        raise `dSym`

proc buildRetryDeadlineLoop*(
    bodyFnSym, retryOptsSym, deadlineMomentSym, connForStateCheck: NimNode,
    timeoutStillRunning, timeoutUnwound, catchableCleanup: NimNode,
): NimNode =
  ## Build the shared retry loop for the deadline-bounded retry macros
  ## (`withTransactionRetryDeadline`, conn and pool). The caller defines
  ## `bodyFnSym` (an `async proc(): Future[void]` that runs BEGIN/body/COMMIT for
  ## one attempt) and binds `retryOptsSym` / `deadlineMomentSym` in scope.
  ##
  ## Per-variant hooks:
  ## * `timeoutStillRunning` / `timeoutUnwound`: statements run when `wait` times
  ##   out and the body future did *not* complete, split the way
  ##   `buildDeadlineAwaitAndTimeout` splits it — a body that could not be
  ##   cancelled still owns the connection, while one that unwound leaves the
  ##   scope its own ROLLBACK. Never retried either way: a timeout exhausts the
  ##   shared budget.
  ## * `catchableCleanup`: statements run on a non-timeout error before the retry
  ##   decision (conn rolls back here; pool already did so inside `bodyFn`, so it
  ##   passes an empty list).
  ## * `connForStateCheck`: when non-nil, the retry is additionally gated on the
  ##   connection being back to `csReady` + `tsIdle` (conn reuses its single
  ##   connection; the pool variant acquires a fresh one per attempt and omits it).
  ##
  ## A retry (including its backoff sleep) is taken only when it still fits before
  ## the deadline, so all attempts together finish within `deadline`.
  let attemptSym = genSym(nskVar, "attempt")
  let bodyFutSym = genSym(nskLet, "bodyFut")
  let eSym = genSym(nskLet, "e")
  let cancelSym = genSym(nskLet, "cancel")
  let backoffMsSym = genSym(nskLet, "backoffMs")
  let csReadySym = bindSym"csReady"
  let stateSym = bindSym"state"
  let txStatusSym = bindSym"txStatus"
  let tsIdleSym = bindSym"tsIdle"
  let timeoutErrSym = bindSym"AsyncTimeoutError"
  let waitSym = bindSym"wait"
  let remainingSym = bindSym"remainingDeadlineDuration"
  let msSym = bindSym"milliseconds"
  let isRetryableSym = bindSym"isRetryableTxError"
  let backoffSym = bindSym"backoffDelayMs"
  let sleepSym = bindSym"sleepMsAsync"
  let invalidateCancelSym = bindSym"invalidateOnCancel"
  let stateCheck =
    if connForStateCheck == nil:
      newLit(true)
    else:
      infix(
        infix(newCall(stateSym, connForStateCheck), "==", csReadySym),
        "and",
        infix(newCall(txStatusSym, connForStateCheck), "==", tsIdleSym),
      )
  # Conn variant owns `connForStateCheck` and must abort server-side on cancel;
  # pool variant handles cancel inside `bodyFn` (which owns the acquired conn),
  # so the outer loop only re-raises.
  let cancelHandler =
    if connForStateCheck == nil:
      newStmtList()
    else:
      quote:
        `invalidateCancelSym`(`connForStateCheck`, releaseTransport = false)
  quote:
    var `attemptSym` = 0
    while true:
      inc `attemptSym`
      let `bodyFutSym` = `bodyFnSym`()
      try:
        # Each attempt is bounded by the *remaining* shared budget, not the full
        # deadline — so all attempts together finish within `deadline`.
        await `waitSym`(`bodyFutSym`, `remainingSym`(`deadlineMomentSym`))
        break
      except `timeoutErrSym`:
        # See withTransactionDeadline for the `completed()` and `finished()`
        # rationale. A timeout means the shared budget is exhausted: invalidate
        # and raise, never retry.
        if `bodyFutSym`.completed():
          break
        elif not `bodyFutSym`.finished():
          `timeoutStillRunning`
        else:
          `timeoutUnwound`
      except CancelledError as `cancelSym`:
        # Never retry cancellation; skip the catchable cleanup (would re-cancel).
        `cancelHandler`
        raise `cancelSym`
      except CatchableError as `eSym`:
        `catchableCleanup`
        # Retry only if the error is retryable, the connection is reusable (conn
        # variant), and the backoff sleep still fits inside the remaining budget.
        let `backoffMsSym` = `backoffSym`(`retryOptsSym`, `attemptSym`)
        if `attemptSym` < `retryOptsSym`.maxAttempts and
            `isRetryableSym`(`eSym`, `retryOptsSym`.retryableStates) and `stateCheck` and
            (Moment.now() + `msSym`(`backoffMsSym`)) < `deadlineMomentSym`:
          await `sleepSym`(`backoffMsSym`)
          continue
        raise `eSym`

macro withTransaction*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction.
  ## On exception, ROLLBACK is issued automatically.
  ## Using `return` inside the body is a compile-time error.
  ##
  ## A body that catches a query error and carries on leaves the transaction
  ## aborted: the server answers COMMIT with ROLLBACK, raised here as
  ## `PgQueryError` with SQLSTATE `25P02` (`SqlStateInFailedSqlTransaction`)
  ## and, when known, the error that aborted the transaction as `parent`. Use
  ## `withSavepoint` to recover from an error inside the body. An error
  ## `executeIsolated` returns in its per-op `errors`, or a `withAdvisoryLock`
  ## unlock failure reported only to `onAdvisoryUnlockFailed`, aborts the
  ## transaction the same way.
  ##
  ## Do not issue transaction-control SQL (`COMMIT`, `ROLLBACK`, `END`,
  ## `ABORT`, `PREPARE TRANSACTION`) inside the body: ending the transaction
  ## yourself is not detected — later statements run in autocommit and the
  ## macro still returns success.
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
  ## the connection via `invalidateOnTimeout` and raises `PgTimeoutError`.
  ## Normally that means `csClosed` plus a server-side CancelRequest, since a
  ## round trip cut short still owes a reply; only a deadline firing after the
  ## reply was read leaves the wire settled and the connection reusable.
  ## ROLLBACK is *not* attempted on a retired connection — `txStatus` may still
  ## read `tsInTransaction` (stale, no `ReadyForQuery` was received), but the
  ## `csReady` guard prevents a futile cleanup call. Standalone callers must
  ## `await conn.close()` after this error; pooled connections are dropped on
  ## release.
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

  body = checkNoBodyEscape(body, "withTransaction", "COMMIT/ROLLBACK")

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let eSym = genSym(nskLet, "e")
  let dSym = genSym(nskLet, "d")
  let cancelSym = genSym(nskLet, "cancel")
  let invalidateCancelSym = bindSym"invalidateOnCancel"
  let bodyCleanup = buildRollbackCleanup(connSym, txTimeout)
  let commitSym = bindSym"commitTx"
  result = quote:
    let `connSym` = `connExpr`
    `connSym`.checkTxIdle()
    try:
      discard await `connSym`.simpleExec(`beginSql`, timeout = `txTimeout`)
      `body`
      await `commitSym`(`connSym`, `txTimeout`)
    except CancelledError as `cancelSym`:
      # Skip ROLLBACK on cancel; invalidate instead.
      `invalidateCancelSym`(`connSym`, releaseTransport = false)
      raise `cancelSym`
    except CatchableError as `eSym`:
      `bodyCleanup`
      raise `eSym`
    except Defect as `dSym`:
      `bodyCleanup`
      # Re-raise; deadline variants wrap.
      raise `dSym`

macro withTransactionRetry*(
    conn: PgConnection, retryOpts: RetryOptions, args: varargs[untyped]
): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction, re-running the whole
  ## transaction when it fails with a retryable error (by default the
  ## serialization_failure / deadlock_detected SQLSTATEs — see `RetryOptions`).
  ## On a non-retryable error, or once `maxAttempts` is exhausted, the last
  ## exception propagates. The body always runs at least once, so
  ## ``maxAttempts <= 1`` means "no retry". Using `return` inside the body is a
  ## compile-time error.
  ##
  ## Usage:
  ##   conn.withTransactionRetry(RetryOptions(maxAttempts: 3)):
  ##     await conn.exec(...)
  ##   conn.withTransactionRetry(RetryOptions(...), seconds(5)):
  ##     await conn.exec(...)
  ##   conn.withTransactionRetry(RetryOptions(...), TransactionOptions(isolation: ilSerializable)):
  ##     await conn.exec(...)
  ##   conn.withTransactionRetry(RetryOptions(...), opts, seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **Idempotency:** `body` is executed once per attempt, so it must be safe to
  ## re-run. Side effects *outside* the database (sending email, mutating
  ## local state, enqueuing jobs) are repeated on every retry — keep them out
  ## of the body or make them idempotent.
  ##
  ## **Timeout semantics:** identical to `withTransaction` — the optional
  ## `timeout` argument is per-call to BEGIN/COMMIT/ROLLBACK only and does not
  ## bound `body`. A per-call timeout invalidates the connection (`csClosed`),
  ## which suppresses any further retry (the connection is no longer reusable).
  ##
  ## **Retry condition:** a retry happens only when the caught error is
  ## retryable *and* the connection is back to a clean, reusable state
  ## (`csReady` + `tsIdle`) after cleanup. This holds both when the body raised
  ## (ROLLBACK restores `tsIdle`) and when COMMIT itself raised a serialization
  ## failure (PostgreSQL has already ended the transaction). Between attempts
  ## the macro sleeps for `backoffDelayMs`. A COMMIT the server rolled back
  ## (`25P02`, see `withTransaction`) is retried when the error that aborted the
  ## transaction, its `parent`, is retryable (see `isRetryableTxError`).
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
    (beginSql, txTimeout) = buildTxBeginAndTimeout(args[0], "withTransactionRetry")
  of 3:
    let opts = args[0]
    txTimeout = args[1]
    body = args[2]
    beginSql = newCall(bindSym"buildBeginSql", opts)
  else:
    error(
      "withTransactionRetry expects (retryOpts, body), (retryOpts, timeout, body), (retryOpts, opts, body), or (retryOpts, opts, timeout, body)",
      retryOpts,
    )

  body = checkNoBodyEscape(body, "withTransactionRetry", "COMMIT/ROLLBACK")

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let retryOptsSym = genSym(nskLet, "retryOpts")
  let loop = buildRetryTxLoop(connSym, retryOptsSym, beginSql, txTimeout, body)
  result = quote:
    let `connSym` = `connExpr`
    `connSym`.checkTxIdle()
    let `retryOptsSym` = `retryOpts`
    `loop`

proc savepointNameExpr(connSym, spName: NimNode): NimNode {.compileTime.} =
  ## Savepoint name expr: explicit name as-is, else `nextPortalName` for uniqueness.
  if spName != nil:
    spName
  else:
    let nextPortalNameSym = bindSym"nextPortalName"
    quote:
      `nextPortalNameSym`(`connSym`, "_sp_")

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
    if args[0].kind in {nnkStrLit, nnkTripleStrLit, nnkRStrLit}:
      # conn.withSavepoint("name"): body
      # (also raw and triple-quoted string literals)
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

  body = checkNoBodyEscape(body, "withSavepoint", "RELEASE/ROLLBACK")

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let eSym = genSym(nskLet, "e")
  let dSym = genSym(nskLet, "d")
  let cancelSym = genSym(nskLet, "cancel")
  let spNameSym = genSym(nskLet, "spName")
  let quoteIdentSym = bindSym"quoteIdentifier"
  let invalidateCancelSym = bindSym"invalidateOnCancel"

  let nameExpr = savepointNameExpr(connSym, spName)
  # Skip ROLLBACK TO SAVEPOINT when the outer transaction has already ended or
  # the connection is csClosed (stale txStatus after per-call timeout) — the
  # state guard avoids a cleanup call that would fail at checkReady().
  let spCleanup = buildSavepointRollbackCleanup(connSym, spNameSym, spTimeout)

  result = quote:
    let `connSym` = `connExpr`
    let `spNameSym` = `quoteIdentSym`(`nameExpr`)
    try:
      discard
        await `connSym`.simpleExec("SAVEPOINT " & `spNameSym`, timeout = `spTimeout`)
      `body`
      discard await `connSym`.simpleExec(
        "RELEASE SAVEPOINT " & `spNameSym`, timeout = `spTimeout`
      )
    except CancelledError as `cancelSym`:
      # As withTransaction: skip cleanup, invalidate instead.
      `invalidateCancelSym`(`connSym`, releaseTransport = false)
      raise `cancelSym`
    except CatchableError as `eSym`:
      `spCleanup`
      raise `eSym`
    except Defect as `dSym`:
      `spCleanup`
      # Re-raise; deadline sibling wraps.
      raise `dSym`

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
  ## Exported for `pg_pool`'s `bindSym` from a direct import — not user API.

macro withTransactionDeadline*(conn: PgConnection, args: varargs[untyped]): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction bounded by a single
  ## wall-clock deadline that covers BEGIN, the body, and COMMIT together.
  ## Unlike `withTransaction`, the timeout does not reset between calls.
  ## A COMMIT the server rolled back raises `PgQueryError` (`25P02`), as in
  ## `withTransaction`.
  ##
  ## Usage:
  ##   conn.withTransactionDeadline(seconds(5)):
  ##     await conn.exec(...)
  ##   conn.withTransactionDeadline(TransactionOptions(...), seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **On deadline exceeded** (`AsyncTimeoutError` from the outer `wait`):
  ## `PgTimeoutError` is raised and the connection is invalidated. A body that
  ## unwound (chronos cancellation) is offered a ROLLBACK first, then
  ## `invalidateOnTimeout`. That ROLLBACK only goes out when the deadline
  ## expired *between* statements: a cancellation landing inside one is caught
  ## by that statement, which invalidates the connection itself, so the cleanup
  ## skips ROLLBACK on a no-longer-`csReady` connection (reported as
  ## `csrConnInvalidated`). A body that could not be cancelled (asyncdispatch)
  ## still owns the socket, so `retireOnTimeout` marks `csClosed` with no
  ## ROLLBACK attempt. Whenever ROLLBACK is skipped the server-side transaction
  ## lives until the connection closes; the pool drops it on release.
  ##
  ## **Standalone connections (not pooled):** callers using `PgConnection`
  ## directly must `await conn.close()` after this error. Otherwise the
  ## server-side transaction lingers until the TCP connection drop is detected,
  ## holding locks and bloating tx state. The pool variant handles this
  ## automatically when the connection is released.
  ##
  ## **On other exceptions** from the body: ROLLBACK is issued with
  ## `rollbackGrace` (5s) as a per-call timeout so cleanup runs even
  ## past the main deadline. A failed ROLLBACK is swallowed. A COMMIT the
  ## server rolled back raises as in `withTransaction`. A `Defect`
  ## raised by the body is re-raised wrapped in `PgError` (the Defect is
  ## `parent`): the body runs in a separate async frame, where chronos
  ## re-raises raw Defects eagerly — only a same-frame Defect is captured.
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

  body = checkNoBodyEscape(body, "withTransactionDeadline", "COMMIT/ROLLBACK")

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "txBodyDeadline")
  let dSym = genSym(nskLet, "d")
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let bodyCleanup = buildRollbackCleanup(connSym, graceSym)
  let commitSym = bindSym"commitTx"
  let awaitAndTimeout = buildDeadlineAwaitAndTimeout(
    connSym, bodyFnSym, totalDurSym, "withTransactionDeadline exceeded", bodyCleanup
  )
  result = quote:
    let `connSym` = `connExpr`
    `connSym`.checkTxIdle()
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    proc `bodyFnSym`(): Future[void] {.async.} =
      try:
        discard await `connSym`.simpleExec(
          `beginSql`, timeout = `remainingSym`(`deadlineMomentSym`)
        )
        `body`
        await `commitSym`(`connSym`, `remainingSym`(`deadlineMomentSym`))
      except Defect as `dSym`:
        # Wrap in `PgError` (parent = Defect) so chronos doesn't re-raise the
        # raw Defect eagerly and the ROLLBACK cleanup runs exactly once.
        raise newException(PgError, `dSym`.msg, `dSym`)

    `awaitAndTimeout`

macro withTransactionRetryDeadline*(
    conn: PgConnection, retryOpts: RetryOptions, args: varargs[untyped]
): untyped =
  ## Execute `body` inside a BEGIN/COMMIT transaction bounded by a single
  ## wall-clock deadline that is **shared across all retry attempts**, re-running
  ## the whole transaction on a retryable error (by default serialization_failure
  ## / deadlock_detected — see `RetryOptions`) while budget remains.
  ##
  ## Usage:
  ##   conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), seconds(5)):
  ##     await conn.exec(...)
  ##   conn.withTransactionRetryDeadline(RetryOptions(...), TransactionOptions(...), seconds(5)):
  ##     await conn.exec(...)
  ##
  ## **Deadline budget:** the `deadline` covers BEGIN + body + COMMIT of *every*
  ## attempt together. Each attempt is bounded by the *remaining* budget, and a
  ## retry (including its backoff sleep) is only taken when it fits before the
  ## deadline. Worst-case wall-clock is therefore `deadline`, not
  ## `maxAttempts * deadline`.
  ##
  ## **On deadline exceeded** (`AsyncTimeoutError`): `PgTimeoutError` is raised
  ## and never retried — the attempt that expired owns the shared budget. As in
  ## `withTransactionDeadline`, a body that unwound gets its ROLLBACK before the
  ## timeout is reported, and one that could not be cancelled retires the
  ## connection instead. Standalone callers must `await conn.close()` afterwards.
  ##
  ## **On a retryable body/COMMIT error:** ROLLBACK runs with `rollbackGrace`,
  ## and the transaction is retried if the connection is back to `csReady`/`tsIdle`
  ## and budget remains. A COMMIT the server rolled back (`25P02`) is retried as
  ## in `withTransactionRetry`. **Idempotency:** `body` runs once per attempt;
  ## non-database side effects repeat. Using `return` inside the body is a
  ## compile-time error.
  ##
  ## **On a `Defect` raised by the body:** re-raised wrapped in `PgError`
  ## (`parent` = Defect), never retried; see `withTransactionDeadline`.
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
      "withTransactionRetryDeadline expects (retryOpts, deadline, body) or (retryOpts, opts, deadline, body)",
      args[0],
    )

  body = checkNoBodyEscape(body, "withTransactionRetryDeadline", "COMMIT/ROLLBACK")

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let retryOptsSym = genSym(nskLet, "retryOpts")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "txBodyRetryDeadline")
  let dSym = genSym(nskLet, "d")
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let bodyCleanup = buildRollbackCleanup(connSym, graceSym)
  let commitSym = bindSym"commitTx"
  let retireSym = bindSym"retireOnTimeout"
  let timeoutCleanup = bodyCleanup.copyNimTree()
  # A body that could not be cancelled still holds the connection, so retire it
  # whatever the wire looks like; one that unwound leaves this scope a ROLLBACK
  # to run first. Without the split, `invalidateOnTimeout` on a settled wire
  # hands back a `csReady` connection with the server transaction still open.
  let timeoutStillRunning = quote:
    `retireSym`(`connSym`, "withTransactionRetryDeadline exceeded")
  let timeoutUnwound = quote:
    `timeoutCleanup`
    `connSym`.invalidateOnTimeout("withTransactionRetryDeadline exceeded")
  let loop = buildRetryDeadlineLoop(
    bodyFnSym,
    retryOptsSym,
    deadlineMomentSym,
    connForStateCheck = connSym,
    timeoutStillRunning = timeoutStillRunning,
    timeoutUnwound = timeoutUnwound,
    catchableCleanup = bodyCleanup,
  )
  result = quote:
    let `connSym` = `connExpr`
    `connSym`.checkTxIdle()
    let `retryOptsSym` = `retryOpts`
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    proc `bodyFnSym`(): Future[void] {.async.} =
      try:
        discard await `connSym`.simpleExec(
          `beginSql`, timeout = `remainingSym`(`deadlineMomentSym`)
        )
        `body`
        await `commitSym`(`connSym`, `remainingSym`(`deadlineMomentSym`))
      except Defect as `dSym`:
        # See withTransactionDeadline: wrap the Defect so the ROLLBACK cleanup
        # runs exactly once.
        raise newException(PgError, `dSym`.msg, `dSym`)

    `loop`

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
  ## `rollbackGrace` per-call timeout. A `Defect` raised by the body is
  ## re-raised wrapped in `PgError` (`parent` = Defect); see
  ## `withTransactionDeadline`.
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

  body = checkNoBodyEscape(body, "withSavepointDeadline", "RELEASE/ROLLBACK")

  let connExpr = conn
  let connSym = genSym(nskLet, "conn")
  let spNameSym = genSym(nskLet, "spName")
  let totalDurSym = genSym(nskLet, "totalDur")
  let deadlineMomentSym = genSym(nskLet, "deadlineMoment")
  let bodyFnSym = genSym(nskProc, "spBodyDeadline")
  let dSym = genSym(nskLet, "d")
  let remainingSym = bindSym"remainingDeadlineDuration"
  let graceSym = bindSym"rollbackGrace"
  let quoteIdentSym = bindSym"quoteIdentifier"

  let nameExpr = savepointNameExpr(connSym, spName)
  let spCleanup = buildSavepointRollbackCleanup(connSym, spNameSym, graceSym)
  let awaitAndTimeout = buildDeadlineAwaitAndTimeout(
    connSym, bodyFnSym, totalDurSym, "withSavepointDeadline exceeded", spCleanup
  )

  result = quote:
    let `connSym` = `connExpr`
    let `spNameSym` = `quoteIdentSym`(`nameExpr`)
    let `totalDurSym` = `deadline`
    let `deadlineMomentSym` = Moment.now() + `totalDurSym`
    proc `bodyFnSym`(): Future[void] {.async.} =
      try:
        discard await `connSym`.simpleExec(
          "SAVEPOINT " & `spNameSym`, timeout = `remainingSym`(`deadlineMomentSym`)
        )
        `body`
        discard await `connSym`.simpleExec(
          "RELEASE SAVEPOINT " & `spNameSym`,
          timeout = `remainingSym`(`deadlineMomentSym`),
        )
      except Defect as `dSym`:
        # See withTransactionDeadline: wrap the Defect so the ROLLBACK TO
        # SAVEPOINT cleanup runs exactly once.
        raise newException(PgError, `dSym`.msg, `dSym`)

    `awaitAndTimeout`
