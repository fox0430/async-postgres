## SQL query helpers: ``?``-placeholder conversion and ``sql`` string-literal macro.
##
## ``sqlParams`` converts ``?``-style placeholders (compatible with Nim's
## ``db_connector``) to PostgreSQL's ``$1, $2, ...`` positional format.
## This lets you combine ``std/strformat`` with parameterised queries:
##
## .. code-block:: nim
##   import std/strformat
##   await conn.query(fmt"SELECT * FROM {tbl} WHERE age > ? LIMIT {n}".sqlParams,
##     pgParams(minAge))
##
## The ``sql`` string-literal macro extracts ``{expr}`` placeholders at compile
## time, producing a ``SqlQuery`` that bundles the rewritten SQL and its
## parameters:
##
## .. code-block:: nim
##   await conn.query(sql"SELECT * FROM users WHERE age > {minAge} AND name = {name}")

import std/macros

import async_backend, pg_types, pg_connection, pg_client, pg_pool, pg_pool_cluster

type SqlQuery* = object ## A parameterised SQL query with its bound parameters.
  query*: string
  params*: seq[PgParam]

func sqlParams*(sql: string): string =
  ## Convert ``?``-style placeholders to PostgreSQL ``$1, $2, …`` positional
  ## placeholders.
  ##
  ## - ``??`` is an escape for a literal ``?``
  ## - ``?|`` and ``?&`` (PostgreSQL JSONB operators) are preserved
  ## - ``?`` inside single-quoted SQL strings is preserved
  ## - ``?`` inside ``E'…'`` C-style escape strings is preserved
  ## - ``?`` inside double-quoted identifiers is preserved
  ## - ``?`` inside dollar-quoted strings (``$$…$$``, ``$tag$…$tag$``) is preserved
  type State = enum
    sNormal
    sSingleQuote
    sEString
    sDoubleQuote
    sDollarQuote

  result = newStringOfCap(sql.len + 16)
  var i = 0
  var paramIdx = 0
  var state = sNormal
  var dollarTag = ""

  while i < sql.len:
    let c = sql[i]
    case state
    of sNormal:
      case c
      of '\'':
        let isE =
          i > 0 and sql[i - 1] in {'E', 'e'} and
          (i < 2 or sql[i - 2] notin {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'})
        state = if isE: sEString else: sSingleQuote
        result.add(c)
        inc i
      of '"':
        state = sDoubleQuote
        result.add(c)
        inc i
      of '$':
        var j = i + 1
        var matched = false
        if j < sql.len and sql[j] == '$':
          dollarTag = "$$"
          state = sDollarQuote
          result.add("$$")
          i = j + 1
          matched = true
        elif j < sql.len and sql[j] in {'a' .. 'z', 'A' .. 'Z', '_'}:
          while j < sql.len and sql[j] in {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'}:
            inc j
          if j < sql.len and sql[j] == '$':
            dollarTag = sql[i .. j]
            state = sDollarQuote
            result.add(dollarTag)
            i = j + 1
            matched = true
        if not matched:
          result.add(c)
          inc i
      of '?':
        if i + 1 < sql.len and sql[i + 1] == '?':
          result.add('?')
          i += 2
        elif i + 1 < sql.len and sql[i + 1] in {'|', '&'}:
          result.add(c)
          result.add(sql[i + 1])
          i += 2
        else:
          inc paramIdx
          result.add('$')
          result.addInt(paramIdx)
          inc i
      else:
        result.add(c)
        inc i
    of sSingleQuote:
      result.add(c)
      if c == '\'':
        if i + 1 < sql.len and sql[i + 1] == '\'':
          result.add('\'')
          inc i
        else:
          state = sNormal
      inc i
    of sEString:
      result.add(c)
      if c == '\\':
        if i + 1 < sql.len:
          inc i
          result.add(sql[i])
      elif c == '\'':
        if i + 1 < sql.len and sql[i + 1] == '\'':
          result.add('\'')
          inc i
        else:
          state = sNormal
      inc i
    of sDoubleQuote:
      result.add(c)
      if c == '"':
        if i + 1 < sql.len and sql[i + 1] == '"':
          result.add('"')
          inc i
        else:
          state = sNormal
      inc i
    of sDollarQuote:
      if c == '$' and i + dollarTag.len <= sql.len and
          sql[i ..< i + dollarTag.len] == dollarTag:
        result.add(dollarTag)
        i += dollarTag.len
        state = sNormal
      else:
        result.add(c)
        inc i

macro sql*(queryStr: static[string]): untyped =
  ## Compile-time macro that parses ``{expr}`` placeholders in a SQL string
  ## literal, replacing them with ``$1, $2, …`` and collecting the expressions
  ## as ``PgParam`` values.
  ##
  ## Use ``{{`` and ``}}`` to produce literal braces.  Placeholders inside
  ## single-quoted SQL strings, ``E'…'`` strings, double-quoted identifiers,
  ## and dollar-quoted strings are left as-is.
  type State = enum
    sNormal
    sSingleQuote
    sEString
    sDoubleQuote
    sDollarQuote

  var resultSql = newStringOfCap(queryStr.len)
  var paramNodes = newNimNode(nnkBracket)
  var paramIdx = 0
  var i = 0
  var state = sNormal
  var dollarTag = ""

  while i < queryStr.len:
    let c = queryStr[i]
    case state
    of sNormal:
      case c
      of '\'':
        let isE =
          i > 0 and queryStr[i - 1] in {'E', 'e'} and
          (i < 2 or queryStr[i - 2] notin {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'})
        state = if isE: sEString else: sSingleQuote
        resultSql.add(c)
        inc i
      of '"':
        state = sDoubleQuote
        resultSql.add(c)
        inc i
      of '$':
        var j = i + 1
        var matched = false
        if j < queryStr.len and queryStr[j] == '$':
          dollarTag = "$$"
          state = sDollarQuote
          resultSql.add("$$")
          i = j + 1
          matched = true
        elif j < queryStr.len and queryStr[j] in {'a' .. 'z', 'A' .. 'Z', '_'}:
          while j < queryStr.len and
              queryStr[j] in {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'}:
            inc j
          if j < queryStr.len and queryStr[j] == '$':
            dollarTag = queryStr[i .. j]
            state = sDollarQuote
            resultSql.add(dollarTag)
            i = j + 1
            matched = true
        if not matched:
          resultSql.add(c)
          inc i
      of '{':
        if i + 1 < queryStr.len and queryStr[i + 1] == '{':
          resultSql.add('{')
          i += 2
        else:
          let start = i + 1
          var depth = 1
          var j = start
          while j < queryStr.len and depth > 0:
            if queryStr[j] == '{':
              inc depth
            elif queryStr[j] == '}':
              dec depth
            if depth > 0:
              inc j
          if depth != 0:
            error("Unmatched '{' in sql string at position " & $i)
          let exprStr = queryStr[start ..< j]
          let exprNode = parseExpr(exprStr)
          inc paramIdx
          resultSql.add('$')
          resultSql.addInt(paramIdx)
          paramNodes.add(newCall(bindSym"toPgParam", exprNode))
          i = j + 1
      of '}':
        if i + 1 < queryStr.len and queryStr[i + 1] == '}':
          resultSql.add('}')
          i += 2
        else:
          error("Unmatched '}' in sql string at position " & $i)
      else:
        resultSql.add(c)
        inc i
    of sSingleQuote:
      resultSql.add(c)
      if c == '\'':
        if i + 1 < queryStr.len and queryStr[i + 1] == '\'':
          resultSql.add('\'')
          inc i
        else:
          state = sNormal
      inc i
    of sEString:
      resultSql.add(c)
      if c == '\\':
        if i + 1 < queryStr.len:
          inc i
          resultSql.add(queryStr[i])
      elif c == '\'':
        if i + 1 < queryStr.len and queryStr[i + 1] == '\'':
          resultSql.add('\'')
          inc i
        else:
          state = sNormal
      inc i
    of sDoubleQuote:
      resultSql.add(c)
      if c == '"':
        if i + 1 < queryStr.len and queryStr[i + 1] == '"':
          resultSql.add('"')
          inc i
        else:
          state = sNormal
      inc i
    of sDollarQuote:
      if c == '$' and i + dollarTag.len <= queryStr.len and
          queryStr[i ..< i + dollarTag.len] == dollarTag:
        resultSql.add(dollarTag)
        i += dollarTag.len
        state = sNormal
      else:
        resultSql.add(c)
        inc i

  let sqlLit = newStrLitNode(resultSql)
  let paramsSeq = newTree(nnkPrefix, ident"@", paramNodes)

  result = quote:
    SqlQuery(query: `sqlLit`, params: `paramsSeq`)

# SqlQuery forwarding templates – PgConnection

template exec*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  exec(conn, sq.query, sq.params, timeout = timeout)

template query*(
    conn: PgConnection,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  query(conn, sq.query, sq.params, resultFormat = resultFormat, timeout = timeout)

template queryOne*(
    conn: PgConnection,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  queryOne(conn, sq.query, sq.params, resultFormat, timeout)

template queryEach*(
    conn: PgConnection,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  queryEach(conn, sq.query, sq.params, callback, resultFormat, timeout)

template queryValue*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValue(conn, sq.query, sq.params, timeout = timeout)

template queryValue*[T](
    conn: PgConnection, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValue(conn, T, sq.query, sq.params, timeout = timeout)

template queryValueOpt*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValueOpt(conn, sq.query, sq.params, timeout = timeout)

template queryValueOpt*[T](
    conn: PgConnection, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValueOpt(conn, T, sq.query, sq.params, timeout = timeout)

template queryValueOrDefault*(
    conn: PgConnection,
    sq: SqlQuery,
    default: string = "",
    timeout: Duration = ZeroDuration,
): untyped =
  queryValueOrDefault(conn, sq.query, sq.params, default, timeout)

template queryValueOrDefault*[T](
    conn: PgConnection,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
): untyped =
  queryValueOrDefault(conn, T, sq.query, sq.params, default, timeout)

template queryExists*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryExists(conn, sq.query, sq.params, timeout = timeout)

template queryColumn*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryColumn(conn, sq.query, sq.params, timeout = timeout)

template execInTransaction*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  execInTransaction(conn, sq.query, sq.params, timeout = timeout)

template execInTransaction*(
    conn: PgConnection,
    sq: SqlQuery,
    opts: TransactionOptions,
    timeout: Duration = ZeroDuration,
): untyped =
  execInTransaction(conn, sq.query, sq.params, opts, timeout = timeout)

template queryInTransaction*(
    conn: PgConnection,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  queryInTransaction(conn, sq.query, sq.params, resultFormat, timeout = timeout)

template queryInTransaction*(
    conn: PgConnection,
    sq: SqlQuery,
    opts: TransactionOptions,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  queryInTransaction(conn, sq.query, sq.params, opts, resultFormat, timeout = timeout)

template addExec*(p: Pipeline, sq: SqlQuery): untyped =
  addExec(p, sq.query, sq.params)

template addQuery*(
    p: Pipeline, sq: SqlQuery, resultFormat: ResultFormat = rfAuto
): untyped =
  addQuery(p, sq.query, sq.params, resultFormat = resultFormat)

# SqlQuery forwarding templates – PgPool

template exec*(pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration): untyped =
  exec(pool, sq.query, sq.params, timeout = timeout)

template query*(
    pool: PgPool,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  query(pool, sq.query, sq.params, resultFormat = resultFormat, timeout = timeout)

template queryOne*(
    pool: PgPool,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  queryOne(pool, sq.query, sq.params, resultFormat, timeout)

template queryEach*(
    pool: PgPool,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  queryEach(pool, sq.query, sq.params, callback, resultFormat, timeout)

template queryValue*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValue(pool, sq.query, sq.params, timeout = timeout)

template queryValue*[T](
    pool: PgPool, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValue(pool, T, sq.query, sq.params, timeout = timeout)

template queryValueOpt*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValueOpt(pool, sq.query, sq.params, timeout = timeout)

template queryValueOpt*[T](
    pool: PgPool, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryValueOpt(pool, T, sq.query, sq.params, timeout = timeout)

template queryValueOrDefault*(
    pool: PgPool, sq: SqlQuery, default: string = "", timeout: Duration = ZeroDuration
): untyped =
  queryValueOrDefault(pool, sq.query, sq.params, default, timeout)

template queryValueOrDefault*[T](
    pool: PgPool,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
): untyped =
  queryValueOrDefault(pool, T, sq.query, sq.params, default, timeout)

template queryExists*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryExists(pool, sq.query, sq.params, timeout = timeout)

template queryColumn*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  queryColumn(pool, sq.query, sq.params, timeout = timeout)

template execInTransaction*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  execInTransaction(pool, sq.query, sq.params, timeout = timeout)

template queryInTransaction*(
    pool: PgPool,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  queryInTransaction(pool, sq.query, sq.params, resultFormat, timeout = timeout)

# SqlQuery forwarding templates – PgPoolCluster (read)

template readQuery*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  readQuery(cluster, sq.query, sq.params, resultFormat, timeout)

template readQueryOne*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  readQueryOne(cluster, sq.query, sq.params, resultFormat, timeout)

template readQueryValue*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  readQueryValue(cluster, sq.query, sq.params, timeout = timeout)

template readQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
): untyped =
  readQueryValue(cluster, T, sq.query, sq.params, timeout = timeout)

template readQueryValueOpt*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  readQueryValueOpt(cluster, sq.query, sq.params, timeout = timeout)

template readQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
): untyped =
  readQueryValueOpt(cluster, T, sq.query, sq.params, timeout = timeout)

template readQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    default: string = "",
    timeout: Duration = ZeroDuration,
): untyped =
  readQueryValueOrDefault(cluster, sq.query, sq.params, default, timeout)

template readQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
): untyped =
  readQueryValueOrDefault(cluster, T, sq.query, sq.params, default, timeout)

template readQueryExists*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  readQueryExists(cluster, sq.query, sq.params, timeout = timeout)

template readQueryColumn*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  readQueryColumn(cluster, sq.query, sq.params, timeout = timeout)

template readQueryEach*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  readQueryEach(cluster, sq.query, sq.params, callback, resultFormat, timeout)

# SqlQuery forwarding templates – PgPoolCluster (write)

template writeExec*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  writeExec(cluster, sq.query, sq.params, timeout = timeout)

template writeQuery*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  writeQuery(cluster, sq.query, sq.params, resultFormat, timeout)

template writeQueryOne*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  writeQueryOne(cluster, sq.query, sq.params, resultFormat, timeout)

template writeQueryValue*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  writeQueryValue(cluster, sq.query, sq.params, timeout = timeout)

template writeQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
): untyped =
  writeQueryValue(cluster, T, sq.query, sq.params, timeout = timeout)

template writeQueryValueOpt*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  writeQueryValueOpt(cluster, sq.query, sq.params, timeout = timeout)

template writeQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
): untyped =
  writeQueryValueOpt(cluster, T, sq.query, sq.params, timeout = timeout)

template writeQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    default: string = "",
    timeout: Duration = ZeroDuration,
): untyped =
  writeQueryValueOrDefault(cluster, sq.query, sq.params, default, timeout)

template writeQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
): untyped =
  writeQueryValueOrDefault(cluster, T, sq.query, sq.params, default, timeout)

template writeQueryExists*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  writeQueryExists(cluster, sq.query, sq.params, timeout = timeout)

template writeQueryColumn*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  writeQueryColumn(cluster, sq.query, sq.params, timeout = timeout)

template writeQueryEach*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  writeQueryEach(cluster, sq.query, sq.params, callback, resultFormat, timeout)

template writeExecInTransaction*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
): untyped =
  writeExecInTransaction(cluster, sq.query, sq.params, timeout = timeout)

template writeQueryInTransaction*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
): untyped =
  writeQueryInTransaction(cluster, sq.query, sq.params, resultFormat, timeout = timeout)
