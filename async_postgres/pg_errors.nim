## Exception hierarchy.
##
## All library-raised exceptions derive from ``PgError`` so callers can catch
## every pg-specific failure with a single ``except PgError`` clause. ``ProtocolError``
## is a subtype of ``PgConnectionError`` because a protocol-level violation
## desynchronises the wire stream — the only viable recovery is to tear down
## and re-establish the connection.

type
  PgError* = object of CatchableError
    ## General PostgreSQL error. Base type for all pg-specific errors.

  PgTypeError* = object of PgError
    ## Raised when a PostgreSQL value cannot be converted to the requested Nim type.

  PgConnectionError* = object of PgError
    ## Connection failures, disconnections, SSL/auth errors.

  ProtocolError* = object of PgConnectionError
    ## Raised on PostgreSQL wire protocol violations. The connection stream is
    ## desynchronised after this error and must be torn down.

  PgQueryError* = object of PgError
    ## SQL execution errors from the server (ErrorResponse).
    sqlState*: string ## 5-char SQLSTATE code (e.g. "42P01"), empty if unavailable.
    severity*: string ## e.g. "ERROR", "FATAL"
    detail*: string ## DETAIL field, empty if not present.
    hint*: string ## HINT field, empty if not present.

  PgTimeoutError* = object of PgError ## Operation timed out.

  PgPoolError* = object of PgError ## Pool exhaustion, pool closed, or acquire timeout.

  PgNotifyOverflowError* = object of PgError
    dropped*: int ## Number of notifications dropped due to queue overflow
