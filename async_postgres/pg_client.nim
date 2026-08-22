## Query execution API.
##
## Choosing between extended- and simple-protocol entry points
## ===========================================================
##
## ``exec`` / ``query`` use the **extended query protocol** (Parse / Bind /
## Describe / Execute). They are the default choice for application queries:
##
## - Exactly one statement per call.
## - Typed parameters via ``seq[PgParam]`` or ``openArray[PgParamInline]`` —
##   values are bound out-of-band, so no string escaping is required.
## - Reuses server-side prepared statements across calls with identical SQL
##   text (bounded by ``stmtCacheCapacity``); the statement is parsed once
##   and rebound on subsequent calls.
## - Result rows may use the binary wire format when ``resultFormat =
##   rfBinary`` is passed. The default ``rfAuto`` sends text on the first
##   execution, then binary on every statement-cache hit for binary-safe
##   OIDs (see ``ResultFormat.rfAuto``). Non-string typed accessors decode
##   either wire format; the string helpers (untyped ``queryValue`` etc.
##   and ``queryValue[string]``) force ``rfText`` internally.
##
## ``simpleExec`` / ``simpleQuery`` use the **simple query protocol** (a single
## ``Query`` message, text-only rows). Prefer them only when the extended
## protocol cannot express what you need:
##
## - **No parameters.** The SQL string is sent verbatim — only use with
##   trusted input, or quote identifiers/literals yourself (e.g. via
##   ``quoteIdentifier``).
## - **No prepared statement reuse.** Each call re-parses on the server;
##   appropriate for one-off session commands (``BEGIN``, ``SET``,
##   ``VACUUM`` …) where a cached statement would be wasted. For
##   ``LISTEN`` / ``UNLISTEN`` / ``NOTIFY`` prefer the dedicated ``listen``,
##   ``unlisten``, and ``notify`` helpers — they quote the channel name for
##   you.
## - ``simpleQuery`` accepts multiple ``;``-separated statements and returns
##   one ``QueryResult`` per statement — the one case the extended protocol
##   cannot cover in a single round trip.
## - ``simpleExec`` expects a side-effect command; the returned tag is the
##   **last** ``CommandComplete`` seen, so multi-statement input is accepted
##   but per-statement results are not surfaced — use ``simpleQuery`` when
##   you need them.
##
## Quick reference
## ---------------
##
## ===========================  =========  ============  ===========  ==============
## API                           Protocol   Multi-stmt   Parameters   Plan cache
## ===========================  =========  ============  ===========  ==============
## ``query`` / ``exec``          extended   no           yes          yes
## ``simpleQuery``               simple     yes          no           no
## ``simpleExec``                simple     last-wins    no           no
## ===========================  =========  ============  ===========  ==============
##
## Timeout behaviour is shared by all four: when a ``timeout`` is exceeded the
## connection is marked ``csClosed`` (the protocol may be mid-exchange) and a
## pooled connection is discarded on release.
##
## This module is a thin re-export hub for the implementation submodules under
## `pg_client/`. From `core` only the stable public surface
## (TransactionOptions and friends, `buildBeginSql`) is re-exported; the
## internal helpers (`queryRecvLoop`, `flattenInline`, `extractParams`,
## `toFormatCodes`, `copyBatchSize` …) stay reachable to submodules via
## `pg_client/core` but are not part of the public API.

import
  pg_client/[
    core, exec, query, prepared, copy, transaction, transaction_helpers, pipeline,
    cursor, direct,
  ]

export core.IsolationLevel
export core.AccessMode
export core.DeferrableMode
export core.TransactionOptions
export core.buildBeginSql
export core.RetryOptions
export core.isRetryableTxError
export core.backoffDelayMs

# `exec` — exec / notify entry points (the `*Impl` helpers stay internal).
export exec.exec
export exec.notify

# `query` — query entry points and result accessors (`*Impl` stay internal).
export query.query
export query.queryEach
export query.queryRow
export query.queryRowOpt
export query.queryValue
export query.queryValueOpt
export query.queryValueOrDefault
export query.queryExists
export query.queryColumn

# `prepared` — prepared statements.
export prepared.PreparedStatement
export prepared.columnIndex
export prepared.prepare
export prepared.execute
export prepared.close

# `copy` — COPY IN / COPY OUT entry points (`*Impl` stay internal).
export copy.copyIn
export copy.copyInStream
export copy.copyOut
export copy.copyOutStream

# `transaction` — transaction/savepoint scoping macros. `rollbackGrace` is
# re-exported because pg_pool's deadline macros resolve it via `bindSym`.
export transaction.withTransaction
export transaction.withTransactionRetry
export transaction.withSavepoint
export transaction.withTransactionDeadline
export transaction.withTransactionRetryDeadline
export transaction.withSavepointDeadline
export transaction.rollbackGrace

# `transaction_helpers` — the two in-transaction convenience helpers.
export transaction_helpers.execInTransaction
export transaction_helpers.queryInTransaction

# `pipeline` — batching.
export pipeline.Pipeline
export pipeline.PipelineResult
export pipeline.PipelineResultKind
export pipeline.IsolatedPipelineResults
export pipeline.newPipeline
export pipeline.reset
export pipeline.addExec
export pipeline.addQuery
export pipeline.execute
export pipeline.executeIsolated

# `cursor` — result cursors.
export cursor.Cursor
export cursor.columnIndex
export cursor.fetchNext
export cursor.close
export cursor.withCursor
export cursor.openCursor

# `direct` — zero-allocation macros.
export direct.queryDirect
export direct.execDirect
