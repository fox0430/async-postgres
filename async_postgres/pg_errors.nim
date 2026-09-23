## Exception hierarchy. Every library-raised exception derives from ``PgError``.
##
## The hierarchy encodes *scope*, not retryability (ask ``isTransientError``):
## a ``PgConnectionError`` (``PgProtocolError``, ``PgTimeoutError``,
## ``PgSecurityError`` and ``PgUnavailableError`` included) ends one connection,
## so ``connect`` fails over past it and reconnect loops must see it;
## ``PgStateError`` and ``PgConfigError`` are deliberately siblings, being a
## programming error and a configuration fault that no reconnect fixes.
##
## ``PgTypeError`` = caller data the wire format cannot carry; ``PgQueryError`` =
## an error the server reported; ``ValueError`` = a precondition, and the one kind
## not under ``PgError`` (except DSN parsing).

from async_backend import AsyncTimeoutError

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
    ##
    ## Decode-failure messages report input lengths/offsets only, never the
    ## content (cells may hold PII or secrets).

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
    ## Connection failures, disconnections, TLS handshake and auth errors. A TLS
    ## fault that stems from the config itself (a cert, key or CA that will not
    ## load) is a ``PgConfigError`` instead.
    serverError*: ref PgQueryError
      ## The ErrorResponse behind the failure, nil if the server sent none: the
      ## one that rejected the session during startup, or the FATAL that ended
      ## it before the connection closed (e.g. ``57P01`` on shutdown). A
      ## statement's ERROR is never kept here, nor on ``connect``'s aggregate
      ## or ``sslmode=allow``'s pair: ``serverErrors`` collects their attempts'.
    attempts*: seq[ref CatchableError]
      ## The failed attempts this error sums up, in order: each host's latest
      ## failure for ``connect`` (a host that answered but did not match
      ## ``target_session_attrs`` included), the plaintext and SSL legs of
      ## ``sslmode=allow``, or a listen pump's last redial. Empty when it sums
      ## up nothing; ``connect``'s aggregate has one even for a single host.
    perHost: bool
      ## ``attempts`` are ``connect``'s hosts (all must clear), not one host's
      ## alternatives.

  PgProtocolError* = object of PgConnectionError
    ## Raised on PostgreSQL wire protocol violations. The connection stream is
    ## desynchronised after this error and must be torn down.

  PgSecurityError* = object of PgConnectionError
    ## The client refused the connection to uphold a config requirement
    ## (``sslmode=require`` or stronger, failing closed where the backend cannot
    ## verify; ``channel_binding=require``; ``require_auth``; direct-SSL ALPN) or
    ## a tampering check a genuine server always passes (SCRAM signature, nonce,
    ## message order and iteration floor; data injected after 'S';
    ## SCRAM-SHA-256-PLUS offered without TLS).
    ##
    ## It names the requirement that failed, not whether a misconfiguration or
    ## a MITM caused it. The server's own refusals (a TLS alert, a wrong
    ## password) stay plain ``PgConnectionError``. Per host: ``connect`` fails
    ## over past it, where libpq stops.

  PgUnavailableError* = object of PgConnectionError
    ## The server could not take the session now or went away (a lost
    ## connection, a failed lookup, a refused dial, ...). Transient unless its
    ## ``serverError`` says otherwise.

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

  PgConfigError* = object of PgError
    ## A ``ConnConfig`` fault no retry can fix: a cert/key/CA that will not
    ## load, a cert without its key, an sslmode or channel_binding that
    ## contradicts another option.
    ## Every host shares one config, so ``connect`` raises it in place of the
    ## per-host ``PgConnectionError`` aggregate. A fault of a single host entry
    ## (a verify-full entry without a host name) is per host, not config-wide,
    ## and stays a ``PgConnectionError``.
    ##
    ## Deliberately **not** a ``PgConnectionError``, so reconnect loops will not
    ## spin on it; a ``PgPool`` that sees one stops dialing (``pekConfigFault``).

  PgQueryError* = object of PgError
    ## SQL execution error reported by the server (ErrorResponse).
    ##
    ## The most common fields are stored directly; everything else the server
    ## sent (schema/table/column/constraint name, error position, …) is kept
    ## verbatim in ``fields`` and exposed through the accessors below, such as
    ## ``constraintName`` and ``position``.
    sqlState*: string ## 5-char SQLSTATE code (e.g. "42P01"), empty if unavailable.
    severity*: string
      ## e.g. "ERROR", "FATAL"; the non-localized 'V' field when the server
      ## sends it (PG 9.6+), else the possibly localized 'S'.
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
      ## The acquire waiter queue — or, when `pipelined`, the `pendingOps`
      ## queue — is full (`maxWaiters` bound); retrying later may succeed.
    pekConnectFailed
      ## A connect attempt failed during acquire (underlying error in `parent`);
      ## retrying may succeed.
    pekConfigFault
      ## The pool's `connConfig` can never connect: a connect raised
      ## `PgConfigError` (preserved as `parent`). Retrying cannot succeed, so
      ## the pool stops opening connections and every later acquire that needs
      ## one fails with this kind without dialing.
    pekBatchFailed ## A pipelined batch was unservable; no connection was acquired.
    pekDefectWrapped
      ## A user-code `Defect` (body/release block or session reset) wrapped to
      ## cross an async boundary; preserved as `parent`.

  PgPoolError* = object of PgError
    ## Pool-level acquire/operation failure (closed, acquire timeout, queue
    ## full, connect failed, config fault, unservable batch, or a wrapped
    ## user-code ``Defect``; the underlying error is preserved as ``parent``).
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

proc setPerHost*(e: ref PgConnectionError, value: bool) {.inline.} =
  ## Set ``perHost``. Sibling-only: the hub does not re-export it.
  e.perHost = value

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
  SqlStateDuplicateObject* = "42710"
  SqlStateQueryCanceled* = "57014"

func getErrorField*(fields: seq[ErrorField], code: char): string =
  ## Get the value of an error field by its single-char code (e.g. 'M' for message).
  for f in fields:
    if f.code == code:
      return f.value

func isSessionFatal*(severity: string): bool =
  ## Whether an ErrorResponse of ``severity`` ends the session: a FATAL or
  ## PANIC, never a statement's ERROR.
  severity in ["FATAL", "PANIC"]

func serverErrors*(e: ref Exception): seq[ref PgQueryError] =
  ## Every ErrorResponse behind ``e``, depth-first: a ``PgConnectionError``'s
  ## ``serverError``, then its ``attempts``'; a ``PgQueryError`` that is a FATAL
  ## or PANIC; any other error's ``parent``'s (e.g. a ``PgPoolError`` wrapping
  ## ``connect``'s failure). A statement's ERROR is left out, as
  ## ``serverError`` leaves it.
  if e == nil:
    return
  if e of PgConnectionError:
    # Not its `parent`: that repeats the last attempt, or a listen death's cause.
    let ce = (ref PgConnectionError)(e)
    if ce.serverError != nil:
      result.add(ce.serverError)
    for a in ce.attempts:
      result.add(serverErrors(a))
  elif e of PgQueryError:
    let qe = (ref PgQueryError)(e)
    if isSessionFatal(qe.severity):
      result.add(qe)
  else:
    result = serverErrors(e.parent)

# Retry classification

func isTransientServerError(se: ref PgQueryError): bool =
  ## Whether a later attempt may get past ``se``; false for nil.
  if se == nil:
    return false
  # A PANIC is crash recovery. Excluded: 08P01 (a malformed startup packet), a
  # statement's 08xxx (a dblink/postgres_fdw link), 53400 (a configured limit)
  # and an ERROR 57014 (a cancel; FATAL is authentication_timeout).
  let s = se.sqlState
  template inClass(c: string): bool =
    s.len == 5 and s[0] == c[0] and s[1] == c[1]

  const listed = [
    "25P03", "25P04", SqlStateSerializationFailure, SqlStateDeadlockDetected, "55006",
    "55P03", "57P01", "57P02", "57P03", "57P05",
  ]
  let fatal = se.severity == "FATAL"
  se.severity == "PANIC" or (fatal and inClass("08") and s != "08P01") or
    (inClass("53") and s != "53400") or s in listed or
    (fatal and s == SqlStateQueryCanceled)

func catchableParent(e: ref Exception): ref CatchableError =
  if e.parent of CatchableError:
    (ref CatchableError)(e.parent)
  else:
    nil

func hasLastingRefusal(e: ref CatchableError): bool =
  ## Whether a server behind ``e`` refused the session for a cause that recurs.
  if e of PgConnectionError:
    let ce = (ref PgConnectionError)(e)
    if ce.attempts.len > 0:
      for a in ce.attempts:
        if hasLastingRefusal(a):
          return true
      false
    else:
      ce.serverError != nil and not isTransientServerError(ce.serverError)
  else:
    false

func isTransientError*(e: ref CatchableError): bool {.raises: [], gcsafe.} =
  ## Whether retrying what failed with `e` may succeed later with the same
  ## config: a ``PgUnavailableError`` or ``PgTimeoutError``, a single host's
  ## ``connectTimeout``, a pool's full queue or acquire timeout, or a server
  ## error whose SQLSTATE may clear (``40001``, ``57P03``, a FATAL ``08xxx``,
  ## ...). Errors with ``attempts`` are judged by them (for ``connect``, every
  ## host must clear); anything unclassified is not transient.
  ##
  ## True does not mean the connection is gone (check ``conn.state``) or that
  ## replaying is safe (a lost ``COMMIT`` may have committed). Cap retries.
  if e == nil:
    false
  elif e of PgPoolError:
    case (ref PgPoolError)(e).kind
    of pekQueueFull, pekAcquireTimeout:
      # An acquire timeout's `parent` only explains the wait.
      true
    of pekConnectFailed, pekBatchFailed:
      isTransientError(e.catchableParent)
    else:
      false
  elif e of PgQueryError:
    isTransientServerError((ref PgQueryError)(e))
  elif e of PgConnectionError:
    let ce = (ref PgConnectionError)(e)
    if ce.attempts.len > 0 and ce.perHost:
      # A host failing for good most likely fails on a config every host shares.
      var every = true
      for a in ce.attempts:
        every = every and isTransientError(a)
      every
    elif ce.attempts.len > 0:
      var any = false
      for a in ce.attempts:
        any = any or isTransientError(a)
      any and not hasLastingRefusal(e)
    elif e of PgSecurityError or e of PgProtocolError:
      false
    elif e of PgListenError and (ref PgListenError)(e).transportAlive:
      # Not the reconnect loop's to act on: the connection is still up.
      false
    elif ce.serverError != nil:
      isTransientServerError(ce.serverError)
    elif e of PgListenError:
      # `parent` is the failure that ended the pump.
      isTransientError(e.catchableParent)
    else:
      e of PgUnavailableError or e of PgTimeoutError
  else:
    # `connect` surfaces a single host's `connectTimeout` as is.
    e of AsyncTimeoutError

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

func isDuplicateObject*(e: ref PgQueryError): bool =
  e.sqlState == SqlStateDuplicateObject
