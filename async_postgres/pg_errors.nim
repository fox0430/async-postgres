## Exception hierarchy.
##
## All library-raised exceptions derive from ``PgError`` so callers can catch
## every pg-specific failure with a single ``except PgError`` clause. ``PgProtocolError``
## is a subtype of ``PgConnectionError`` because a protocol-level violation
## desynchronises the wire stream — the only viable recovery is to tear down
## and re-establish the connection.
##
## ``PgStateError`` is deliberately a *sibling* of ``PgConnectionError`` (both
## under ``PgError``) rather than a subtype: it signals a programming error
## (e.g. a single connection used concurrently), for which reconnecting is
## pointless, so it must stay out of ``except PgConnectionError`` reconnect
## loops.
##
## ``PgTimeoutError`` is a subtype of ``PgConnectionError`` for the opposite
## reason. A timed-out query/exec/copy/prepare/transaction dispatches a
## best-effort CancelRequest and marks the connection ``csClosed`` (the wire may
## be mid-exchange and is no longer trustworthy), so reconnecting *is* the
## correct recovery and the error must be visible to ``except PgConnectionError``
## loops. The two timeouts that leave the connection usable —
## ``waitNotification`` and an acquire timeout inside
## ``withTransactionDeadline`` / ``withTransactionRetryDeadline`` — still raise
## ``PgTimeoutError`` and are therefore also caught by ``except PgConnectionError``;
## a caller that needs to tell a timeout apart from a hard connection failure
## catches ``PgTimeoutError`` in a clause placed *before* the
## ``PgConnectionError`` one.

type
  ErrorField* = object
    ## A single field from an ErrorResponse or NoticeResponse message.
    code*: char
    value*: string

  PgError* = object of CatchableError
    ## General PostgreSQL error. Base type for all pg-specific errors.

  PgTypeError* = object of PgError
    ## Raised when a PostgreSQL value cannot be converted to the requested Nim type.

  PgNoRowsError* = object of PgError
    ## Raised by single-row/single-value queries when the result set is empty.

  PgNullError* = object of PgError
    ## Raised by single-value queries when the value is SQL NULL and the
    ## caller requested a non-nullable result.

  PgConnectionError* = object of PgError
    ## Connection failures, disconnections, SSL/auth errors.

  PgProtocolError* = object of PgConnectionError
    ## Raised on PostgreSQL wire protocol violations. The connection stream is
    ## desynchronised after this error and must be torn down.

  ProtocolError* {.deprecated: "use PgProtocolError".} = PgProtocolError
    ## Deprecated alias for `PgProtocolError`, kept for backwards compatibility.

  PgStateError* = object of PgError
    ## Raised when an operation is attempted on a connection that is alive but
    ## in the wrong state for that operation — most commonly a single connection
    ## used concurrently: a second query started while the first is still in
    ## flight finds the connection ``csBusy``.
    ##
    ## This is a programming error, not a connection failure. Reconnecting does
    ## not fix it, so ``PgStateError`` is intentionally **not** a subtype of
    ## ``PgConnectionError`` — code that recovers via ``except PgConnectionError``
    ## will not spin on it. The fix is to give each concurrent caller its own
    ## connection (e.g. via a ``PgPool``).

  PgQueryError* = object of PgError
    ## SQL execution errors from the server (ErrorResponse).
    ##
    ## The most common fields are stored directly; everything else the server
    ## sent (schema/table/column/constraint name, error position, …) is kept
    ## verbatim in ``fields`` and exposed through accessors such as
    ## ``constraintName`` and ``position``.
    sqlState*: string ## 5-char SQLSTATE code (e.g. "42P01"), empty if unavailable.
    severity*: string ## e.g. "ERROR", "FATAL"
    detail*: string ## DETAIL field, empty if not present.
    hint*: string ## HINT field, empty if not present.
    fields*: seq[ErrorField]
      ## All raw ErrorResponse fields as sent by the server, including any
      ## not covered by the named accessors below.

  PgTimeoutError* = object of PgConnectionError
    ## Raised when an operation times out.
    ##
    ## A timeout on a query/exec/copy/prepare/transaction invalidates the
    ## connection: a best-effort CancelRequest is dispatched and the connection
    ## is marked ``csClosed`` because the protocol may be mid-exchange. The only
    ## viable recovery is to reconnect, so ``PgTimeoutError`` is a subtype of
    ## ``PgConnectionError`` — an ``except PgConnectionError`` reconnect loop
    ## catches it instead of silently dropping the timeout-poisoned connection.
    ##
    ## ``waitNotification`` and an acquire timeout inside
    ## ``withTransactionDeadline`` / ``withTransactionRetryDeadline`` also raise
    ## ``PgTimeoutError`` but do **not** close the connection; catch
    ## ``PgTimeoutError`` before any ``PgConnectionError`` clause if you need to
    ## distinguish those.

  PgPoolError* = object of PgError
    ## Pool-level acquire failure: acquire timeout, pool closed, waiter queue
    ## full, or a failed connect attempt during acquire (the underlying error,
    ## e.g. ``PgConnectionError``, is preserved as ``parent``).

  PgNotifyOverflowError* = object of PgError
    dropped*: int ## Number of notifications dropped due to queue overflow

  PgListenError* = object of PgConnectionError
    ## Listen pump died permanently (reconnection failed or connection lost
    ## with no channels left to re-subscribe).
    reconnectionAttempted*: bool
      ## True if the pump attempted reconnection before giving up.

const
  # Commonly dispatched-on SQLSTATE codes
  SqlStateNotNullViolation* = "23502"
  SqlStateForeignKeyViolation* = "23503"
  SqlStateUniqueViolation* = "23505"
  SqlStateCheckViolation* = "23514"
  SqlStateExclusionViolation* = "23P01"
  SqlStateSerializationFailure* = "40001"
  SqlStateDeadlockDetected* = "40P01"
  SqlStateSyntaxError* = "42601"
  SqlStateUndefinedTable* = "42P01"
  SqlStateQueryCanceled* = "57014"

func getErrorField*(fields: seq[ErrorField], code: char): string =
  ## Get the value of an error field by its single-char code (e.g. 'M' for message).
  for f in fields:
    if f.code == code:
      return f.value

# PgQueryError field accessors. Field codes are defined by the wire protocol
# All return "" (or 0 for positions) when the server did not send the field.

func errorField*(e: ref PgQueryError, code: char): string =
  ## Raw ErrorResponse field by single-char code, "" if not present.
  getErrorField(e.fields, code)

func schemaName*(e: ref PgQueryError): string =
  ## Schema containing the object the error refers to.
  getErrorField(e.fields, 's')

func tableName*(e: ref PgQueryError): string =
  ## Table the error refers to.
  getErrorField(e.fields, 't')

func columnName*(e: ref PgQueryError): string =
  ## Column the error refers to.
  getErrorField(e.fields, 'c')

func dataTypeName*(e: ref PgQueryError): string =
  ## Data type the error refers to.
  getErrorField(e.fields, 'd')

func constraintName*(e: ref PgQueryError): string =
  ## Constraint the error refers to (e.g. the violated unique index).
  getErrorField(e.fields, 'n')

func where*(e: ref PgQueryError): string =
  ## Context call stack (PL/pgSQL traceback etc.).
  getErrorField(e.fields, 'W')

func internalQuery*(e: ref PgQueryError): string =
  ## Text of the internally-generated query that failed (e.g. inside a function).
  getErrorField(e.fields, 'q')

func parsePosition(v: string): int =
  # Server sends a 1-based decimal character index; 0 means "not present".
  for c in v:
    if c < '0' or c > '9':
      return 0
    result = result * 10 + (ord(c) - ord('0'))

func position*(e: ref PgQueryError): int =
  ## 1-based character index into the original query where the error occurred,
  ## 0 if the server did not report a position.
  parsePosition(getErrorField(e.fields, 'P'))

func internalPosition*(e: ref PgQueryError): int =
  ## Like ``position`` but for ``internalQuery``, 0 if not reported.
  parsePosition(getErrorField(e.fields, 'p'))

# SQLSTATE predicates

func isUniqueViolation*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateUniqueViolation

func isForeignKeyViolation*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateForeignKeyViolation

func isNotNullViolation*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateNotNullViolation

func isCheckViolation*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateCheckViolation

func isExclusionViolation*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateExclusionViolation

func isIntegrityConstraintViolation*(e: ref PgQueryError): bool =
  ## Any SQLSTATE in class 23 (integrity constraint violation).
  e.sqlState.len == 5 and e.sqlState[0] == '2' and e.sqlState[1] == '3'

func isSerializationFailure*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateSerializationFailure

func isDeadlockDetected*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateDeadlockDetected

func isQueryCanceled*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateQueryCanceled
