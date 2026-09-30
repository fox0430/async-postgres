## Fixture for `test_body_escape_diagnostics`: never compiled on its own, only
## `nim check`ed. Each rejected statement is preceded by an `# expect:` line
## with the start of its error message; every other line must stay error-free.

import ../async_postgres

template twice(body: untyped) =
  body
  body

template eachN(n: int, body: untyped) =
  var i = 0
  while i < n:
    inc i
    body

template labeledBlock(body: untyped) =
  block outer:
    body

template later(body: untyped) =
  let fn = proc() {.async.} =
    body
  asyncSpawn fn()

template bailOut() =
  return

proc writtenBreak(conn: PgConnection) {.async.} =
  for i in 0 ..< 3:
    conn.withTransaction:
      # expect: 'break'/'continue' escaping withTransaction is not allowed
      break

proc writtenBreakInCall(conn: PgConnection) {.async.} =
  for i in 0 ..< 3:
    conn.withTransaction:
      twice:
        # expect: 'break'/'continue' escaping withTransaction is not allowed
        break

proc hiddenReturn(conn: PgConnection) {.async.} =
  conn.withTransaction:
    # expect: 'return' inside withTransaction (hidden inside a template) is not allowed
    bailOut()

proc writtenReturnInCall(conn: PgConnection) {.async.} =
  conn.withTransaction:
    twice:
      # expect: 'return' inside withTransaction is not allowed
      return

proc bodyLocalTemplate(conn: PgConnection) {.async.} =
  for i in 0 ..< 3:
    conn.withTransaction:
      template bail() =
        break

      # expect: 'break'/'continue' escaping withTransaction (hidden inside a template) is not allowed
      bail()

proc retryLoopContinue(conn: PgConnection) {.async.} =
  # No caller loop: the real body's `continue` binds to the retry loop.
  conn.withTransactionRetry(RetryOptions()):
    twice:
      # expect: 'break'/'continue' escaping withTransactionRetry is not allowed
      continue

proc poolDeadlineBreak(pool: PgPool) {.async.} =
  # The real body runs in a closure, where the `break` can't reach the loop.
  for i in 0 ..< 3:
    pool.withTransactionDeadline(conn, seconds(5)):
      twice:
        # expect: 'break'/'continue' escaping withTransactionDeadline is not allowed
        break

proc labeledThroughGensym(conn: PgConnection) {.async.} =
  block outer:
    conn.withTransaction:
      labeledBlock:
        # expect: 'break'/'continue' escaping withTransaction is not allowed
        break outer

proc cursorBreakInCall(conn: PgConnection) {.async.} =
  for i in 0 ..< 3:
    conn.withCursor("SELECT 1", 5'i32, cur):
      twice:
        # expect: 'break'/'continue' escaping withCursor is not allowed
        break

proc advisoryBreakInCall(conn: PgConnection) {.async.} =
  for i in 0 ..< 3:
    conn.withAdvisoryLock(1'i64):
      twice:
        # expect: 'break'/'continue' escaping withAdvisoryLock is not allowed
        break

proc nestedWrittenBreak(conn: PgConnection) {.async.} =
  # The transaction expands first and sees the `break` leaving both bodies.
  for i in 0 ..< 3:
    conn.withTransaction:
      conn.withSavepoint:
        # expect: 'break'/'continue' escaping withTransaction is not allowed
        break

proc nestedHiddenReturn(conn: PgConnection) {.async.} =
  conn.withTransaction:
    conn.withSavepoint:
      # expect: 'return' inside withSavepoint (hidden inside a template) is not allowed
      bailOut()

proc nestedInBodyTemplate(conn: PgConnection) {.async.} =
  for i in 0 ..< 3:
    conn.withTransaction:
      template inSavepoint() =
        conn.withSavepoint:
          # expect: 'break'/'continue' escaping withSavepoint is not allowed
          break

      inSavepoint()

proc closureReturn(conn: PgConnection) {.async.} =
  # asyncdispatch's `async` rewrites the `return` before `later` expands, so
  # it would complete this proc's future from inside the spawned closure.
  conn.withTransaction:
    later:
      # expect: 'return' inside withTransaction is not allowed
      return

proc bodyTemplateClosureReturn(conn: PgConnection) {.async.} =
  # asyncdispatch's `async` also rewrites a `return` in a template defined in
  # the body, before `later` wraps it in the spawned closure.
  conn.withTransaction:
    template bail() =
      later:
        # expect: 'return' inside withTransaction is not allowed
        return

    bail()

proc forIterableBreak(conn: PgConnection) {.async.} =
  # The iterable is evaluated before the loop is entered, so the `break`
  # binds to the caller's loop.
  for j in 0 ..< 2:
    conn.withTransaction:
      for x in (
        # expect: 'break'/'continue' escaping withTransaction is not allowed
        if j == 0: break
        @[1]
      ):
        discard x

proc loopTemplateContinue(conn: PgConnection) {.async.} =
  # The loop `eachN` wraps around its argument is invisible before expansion,
  # so the check rejects the `continue` although that loop would capture it.
  conn.withTransactionRetry(RetryOptions()):
    eachN(3):
      # expect: 'break'/'continue' escaping withTransactionRetry is not allowed
      continue

proc conditionContinue(conn: PgConnection) {.async.} =
  # A `continue` in a `while` condition binds to an enclosing loop: here the
  # retry loop, which would re-run the transaction without a COMMIT.
  conn.withTransactionRetry(RetryOptions()):
    # expect: 'break'/'continue' escaping withTransactionRetry is not allowed
    while (if conn.pid == 0: continue ; false):
      discard

proc acceptedBodyLocalTypes(conn: PgConnection) {.async.} =
  # The body must be type-checked only where it is spliced: a type-checked
  # body spliced back in rejects variables of a body-local type.
  conn.withTransaction:
    type TxLocal = object
      a: int

    let v = TxLocal(a: 1)
    conn.withSavepoint:
      type SpLocal = enum
        spA
        spB

      var e = spA
      discard (v, e)
  conn.withCursor("SELECT 1", 5'i32, cur):
    type CursorLocal = object
      a: int

    let v = CursorLocal(a: 1)
    discard v

proc acceptedConditionBreak(conn: PgConnection) {.async.} =
  # Nim evaluates the condition inside the loop, so an unlabeled `break`
  # there leaves only the `while`.
  for j in 0 ..< 2:
    conn.withTransaction:
      while (if j == 0: break ; false):
        discard

proc acceptedConditionBlockBreak(conn: PgConnection) {.async.} =
  # A `block:` inside the condition captures its own `break`.
  for j in 0 ..< 2:
    conn.withTransaction:
      while (
        block:
          (if j == 0: break )
        false
      )
      :
        discard
