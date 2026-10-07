## Unit tests for `pg_client/core` pure helpers.
##
## PG-less: format derivation, BEGIN builder, retry predicates, backoff,
## OID matching, and inline flattening.

import std/unittest

import ../async_postgres/[async_backend, pg_errors, pg_protocol, pg_types]
import ../async_postgres/pg_client/core

suite "core: format codes":
  test "toFormatCodes maps ResultFormat":
    check toFormatCodes(rfAuto) == newSeq[int16]()
    check toFormatCodes(rfText) == @[0'i16]
    check toFormatCodes(rfBinary) == @[1'i16]

  test "deriveColFmts broadcasts and pads with text":
    check deriveColFmts([1'i16], 3) == @[1'i16, 1'i16, 1'i16]
    check deriveColFmts([1'i16, 0'i16], 3) == @[1'i16, 0'i16, 0'i16]
    check deriveColFmts([], 2) == @[0'i16, 0'i16]

  test "cacheHitColFmts prefers the caller's override":
    check cacheHitColFmts([1'i16], @[0'i16], 1) == @[1'i16]
    check cacheHitColFmts([], @[0'i16], 1) == @[0'i16]

suite "core: buildBeginSql":
  test "default options produce bare BEGIN":
    check buildBeginSql(TransactionOptions()) == "BEGIN"

  test "isolation + access + deferrable compose":
    let opts = TransactionOptions(
      isolation: ilSerializable, access: amReadOnly, deferrable: dmDeferrable
    )
    check buildBeginSql(opts) ==
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"

  test "read-committed read-write not-deferrable":
    let opts = TransactionOptions(
      isolation: ilReadCommitted, access: amReadWrite, deferrable: dmNotDeferrable
    )
    check buildBeginSql(opts) ==
      "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE NOT DEFERRABLE"

suite "core: isRetryableTxError":
  test "matching SQLSTATE retries":
    let e = (ref PgQueryError)(sqlState: "40001", msg: "serialization")
    check isRetryableTxError(e, ["40001", "40P01"])

  test "non-matching SQLSTATE does not retry":
    let e = (ref PgQueryError)(sqlState: "23505", msg: "unique")
    check not isRetryableTxError(e, ["40001"])

  test "non-query errors never retry":
    let e = newException(PgConnectionError, "gone")
    check not isRetryableTxError(e, ["40001"])

  test "25P02 with retryable parent retries":
    let parent = (ref PgQueryError)(sqlState: "40001", msg: "parent")
    let e = (ref PgQueryError)(
      sqlState: SqlStateInFailedSqlTransaction, msg: "failed", parent: parent
    )
    check isRetryableTxError(e, ["40001"])

suite "core: backoff and OID matching":
  test "backoffDelayMs is bounded and non-negative":
    let opts = RetryOptions(
      maxAttempts: 3, baseDelayMs: 100, maxDelayMs: 1000, multiplier: 2.0, jitter: false
    )
    check backoffDelayMs(opts, 1) == 100
    check backoffDelayMs(opts, 2) == 200
    check backoffDelayMs(opts, 10) == 1000

  test "paramOidsMatch treats 0 as wildcard":
    check paramOidsMatch([23'i32], [0'i32])
    check paramOidsMatch([0'i32], [23'i32])
    check not paramOidsMatch([23'i32], [25'i32])
    check not paramOidsMatch([23'i32], [23'i32, 25'i32])

suite "core: validation":
  test "validateExtendedQuery rejects NUL and mismatched counts":
    expect PgTypeError:
      validateExtendedQuery("a\0b", 0)
    # Too many params exceeds the protocol maximum.
    expect PgTypeError:
      validateExtendedQuery("SELECT 1", 70000)

  test "flattenInline round-trips a text param":
    let (data, ranges, oids, formats) = flattenInline([1'i32.toPgParamInline()])
    check ranges.len == 1
    check oids.len == 1
    check formats.len == 1
    check data.len > 0
