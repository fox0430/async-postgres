## Dedicated unit tests for ``pg_errors`` — hierarchy, SQLSTATE helpers, and
## overflow-safe position parsing. Pins recovery-oriented exception contracts
## without needing a live server.

import std/unittest

import ../async_postgres/pg_errors

suite "pg_errors hierarchy":
  # Widen through `ref Exception` so `of` is a runtime check (not CondTrue/CondFalse).
  template asExc(e: ref CatchableError): ref Exception =
    e

  test "protocol and timeout errors are connection errors":
    check asExc((ref PgProtocolError)()) of PgConnectionError
    check asExc((ref PgTimeoutError)()) of PgConnectionError
    check asExc((ref PgListenError)()) of PgConnectionError

  test "state and config errors are siblings, not connection errors":
    check asExc((ref PgStateError)()) of PgError
    check asExc((ref PgConfigError)()) of PgError
    check not (asExc((ref PgStateError)()) of PgConnectionError)
    check not (asExc((ref PgConfigError)()) of PgConnectionError)
    check asExc((ref PgListenStoppedError)()) of PgStateError
    check not (asExc((ref PgListenStoppedError)()) of PgConnectionError)

  test "type / message-size / query / pool sit under PgError":
    check asExc((ref PgTypeError)()) of PgError
    check asExc((ref PgMessageTooLargeError)()) of PgTypeError
    check asExc((ref PgQueryError)()) of PgError
    check asExc((ref PgPoolError)()) of PgError
    check asExc((ref PgNoRowsError)()) of PgError
    check asExc((ref PgNullError)()) of PgError
    check asExc((ref PgNotifyOverflowError)()) of PgError

  test "newPoolError records kind and optional parent":
    let parent = (ref ValueError)(msg: "boom")
    let err = newPoolError(pekQueueFull, "full", parent)
    check err.kind == pekQueueFull
    check err.msg == "full"
    check err.parent == parent

suite "pg_errors field accessors":
  test "getErrorField returns first matching code":
    let fields = @[
      ErrorField(code: 'M', value: "msg"),
      ErrorField(code: 'C', value: "23505"),
      ErrorField(code: 'M', value: "later"),
    ]
    check getErrorField(fields, 'M') == "msg"
    check getErrorField(fields, 'C') == "23505"
    check getErrorField(fields, 'H') == ""

  test "PgQueryError named accessors and SQLSTATE predicates":
    let err = (ref PgQueryError)(
      msg: "dup",
      sqlState: SqlStateUniqueViolation,
      severity: "ERROR",
      detail: "d",
      hint: "h",
      fields: @[
        ErrorField(code: 's', value: "public"),
        ErrorField(code: 't', value: "users"),
        ErrorField(code: 'c', value: "email"),
        ErrorField(code: 'd', value: "text"),
        ErrorField(code: 'n', value: "users_email_key"),
        ErrorField(code: 'W', value: "PL/pgSQL"),
        ErrorField(code: 'q', value: "SELECT 1"),
        ErrorField(code: 'P', value: "12"),
        ErrorField(code: 'p', value: "3"),
      ],
    )
    check err.schemaName == "public"
    check err.tableName == "users"
    check err.columnName == "email"
    check err.dataTypeName == "text"
    check err.constraintName == "users_email_key"
    check err.where == "PL/pgSQL"
    check err.internalQuery == "SELECT 1"
    check err.position == 12
    check err.internalPosition == 3
    check err.isUniqueViolation
    check err.isIntegrityConstraintViolation
    check not err.isSerializationFailure

  test "position parsing rejects non-digits and overflow without Defect":
    let bad = (ref PgQueryError)(fields: @[ErrorField(code: 'P', value: "12x")])
    check bad.position == 0
    # Digits that would overflow int must clamp to "not present" (0), never
    # raise an uncatchable OverflowDefect for a caller that only reads position.
    var huge = "9"
    for i in 0 ..< 80:
      huge.add('9')
    let overflow = (ref PgQueryError)(fields: @[ErrorField(code: 'P', value: huge)])
    check overflow.position == 0

  test "integrity class predicate covers known 23xxx codes":
    for state in [
      SqlStateNotNullViolation, SqlStateForeignKeyViolation, SqlStateUniqueViolation,
      SqlStateCheckViolation, SqlStateExclusionViolation,
    ]:
      let err = (ref PgQueryError)(sqlState: state)
      check err.isIntegrityConstraintViolation
    check not (ref PgQueryError)(sqlState: SqlStateSyntaxError).isIntegrityConstraintViolation
    check not (ref PgQueryError)(sqlState: "23").isIntegrityConstraintViolation
