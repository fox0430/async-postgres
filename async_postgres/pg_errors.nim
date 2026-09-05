## Exception hierarchy. Every library-raised exception derives from ``PgError``.
##
## The hierarchy encodes *recovery*: ``PgProtocolError`` and ``PgTimeoutError``
## are ``PgConnectionError`` because both leave the wire unusable, so reconnect
## loops must see them; ``PgStateError`` is deliberately a sibling, being a
## programming error that reconnecting cannot fix.
##
## ``PgTypeError`` = caller data the wire format cannot carry; ``PgQueryError`` =
## an error the server reported; ``ValueError`` = a precondition, and the one kind
## not under ``PgError`` (except DSN parsing).

type
  ErrorField* = object
    ## A single field from an ErrorResponse or NoticeResponse message.
    code*: char
    value*: string

  PgError* = object of CatchableError
    ## General PostgreSQL error. Base type for all pg-specific errors.

  PgTypeError* = object of PgError
    ## Raised when the caller's data cannot be carried by the wire format: a
    ## value that will not convert to or from the requested Nim type, or one the
    ## protocol cannot encode (count past Int16, length past Int32, embedded NUL).

  PgMessageTooLargeError* = object of PgTypeError
    ## An assembled protocol message exceeds the wire format's Int32 length.
    ## Caller data decides the size, so this is input, not connection health:
    ## nothing was sent and a reconnect loop must not fire.

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
    ## An operation attempted on a connection that is alive but in the wrong
    ## state for it — most commonly a single connection used concurrently: a
    ## second query started while the first is still in flight finds the
    ## connection ``csBusy``.
    ##
    ## A programming error, not a connection failure: deliberately **not** a
    ## ``PgConnectionError``, so reconnect loops will not spin on it. Give each
    ## concurrent caller its own connection (e.g. via a ``PgPool``).

  PgQueryError* = object of PgError
    ## SQL execution error reported by the server (ErrorResponse).
    ##
    ## The most common fields are stored directly; everything else the server
    ## sent (schema/table/column/constraint name, error position, …) is kept
    ## verbatim in ``fields`` and exposed through the accessors below, such as
    ## ``constraintName`` and ``position``.
    sqlState*: string ## 5-char SQLSTATE code (e.g. "42P01"), empty if unavailable.
    severity*: string ## e.g. "ERROR", "FATAL"
    detail*: string ## DETAIL field, empty if not present.
    hint*: string ## HINT field, empty if not present.
    fields*: seq[ErrorField]
      ## All raw ErrorResponse fields as sent by the server, including any not
      ## covered by the named accessors below.

  PgTimeoutError* = object of PgConnectionError
    ## Raised when an operation times out.
    ##
    ## A timeout on a query/exec/copy/prepare/transaction marks the connection
    ## ``csClosed`` (the wire may be mid-exchange), hence ``PgConnectionError``.
    ## ``waitNotification`` and an acquire timeout inside
    ## ``withTransactionDeadline`` / ``withTransactionRetryDeadline`` leave the
    ## connection usable; catch ``PgTimeoutError`` before ``PgConnectionError``
    ## to tell them apart.

  PoolErrorKind* = enum
    ## Machine-readable category of a `PgPoolError`.
    pekUnknown
      ## Default: a `PgPoolError` built without an explicit `kind`; do not
      ## treat as `pekClosed`.
    pekClosed ## The pool is permanently closed; retrying cannot succeed.
    pekAcquireTimeout
      ## An acquire deadline elapsed (`acquireTimeout` or cluster fallback);
      ## retrying later may succeed.
    pekQueueFull
      ## The waiter queue is full (`maxWaiters` bound); retrying later may succeed.
    pekConnectFailed
      ## A connect attempt failed during acquire (underlying error in `parent`);
      ## retrying may succeed.
    pekBatchFailed ## A pipelined batch was unservable; no connection was acquired.
    pekDefectWrapped
      ## A user-code `Defect` (body/release block or session reset) wrapped to
      ## cross an async boundary; preserved as `parent`.

  PgPoolError* = object of PgError
    ## Pool-level acquire/operation failure (closed, acquire timeout, queue
    ## full, connect failed, unservable batch, or a wrapped user-code
    ## ``Defect``; the underlying error is preserved as ``parent``).
    ##
    ## ``kind`` classifies the failure programmatically; the message string is
    ## informational only. Errors built without ``newPoolError`` have
    ## ``kind == pekUnknown``.
    kind*: PoolErrorKind ## Failure category (see `PoolErrorKind`).

  PgNotifyOverflowError* = object of PgError
    dropped*: int ## Number of notifications dropped due to queue overflow

  PgListenError* = object of PgConnectionError
    ## Listen pump died permanently (reconnection failed or connection lost
    ## with no channels left to re-subscribe).
    reconnectionAttempted*: bool
      ## True if the pump attempted reconnection before giving up.
    transportAlive*: bool
      ## True when the pump died but the transport is still up. The pull API
      ## raises ``PgListenStoppedError`` for that case instead.

  PgListenStoppedError* = object of PgStateError
    ## Listen pump gone from a connection that is still usable: call ``listen``
    ## again to recover. Deliberately **not** a ``PgConnectionError``, so a
    ## reconnect loop will not re-dial a live connection. Raised by
    ## ``waitNotification``; the push API reports the same death as a
    ## ``PgListenError`` with ``transportAlive``.
    reconnectionAttempted*: bool
      ## True if the pump attempted reconnection before giving up.
    transportAlive*: bool
      ## Always true here — the type is only raised for a live transport. Kept
      ## so a caller reading the field need not special-case which error it got.

template newPoolError*(
    errKind: PoolErrorKind, message: string, parentErr: ref Exception = nil
): untyped =
  ## Create a `PgPoolError` with the given `errKind` (see `PoolErrorKind`).
  (ref PgPoolError)(kind: errKind, msg: message, parent: parentErr)

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
  # Guard the accumulator against OverflowDefect (uncatchable) so a buggy or
  # hostile server cannot crash a caller that simply reads `err.position`.
  for c in v:
    if c < '0' or c > '9':
      return 0
    let d = ord(c) - ord('0')
    if result > (high(int) - d) div 10:
      return 0
    result = result * 10 + d

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
