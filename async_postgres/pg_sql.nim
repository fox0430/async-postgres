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

type
  SqlQuery* = object ## A parameterised SQL query with its bound parameters.
    query*: string
    params*: seq[PgParam]

  SqlParseState = enum
    sNormal
    sSingleQuote
    sEString
    sDoubleQuote
    sDollarQuote

template sqlParseLoop(
    sql: string,
    output: var string,
    idx: var int,
    state: var SqlParseState,
    dollarTag: var string,
    normalBody: untyped,
) =
  ## Shared SQL parsing loop that handles quote-state transitions.
  ## ``normalBody`` is executed for characters in ``sNormal`` that are not
  ## quote transitions (``'``, ``"``, ``$``-quote). Inside ``normalBody``,
  ## ``c`` is the current character at ``sql[idx]``.
  while idx < sql.len:
    let c {.inject.} = sql[idx]
    case state
    of sNormal:
      case c
      of '\'':
        let isE =
          idx > 0 and sql[idx - 1] in {'E', 'e'} and
          (idx < 2 or sql[idx - 2] notin {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'})
        state = if isE: sEString else: sSingleQuote
        output.add(c)
        inc idx
      of '"':
        state = sDoubleQuote
        output.add(c)
        inc idx
      of '$':
        var j = idx + 1
        var matched = false
        if j < sql.len and sql[j] == '$':
          dollarTag = "$$"
          state = sDollarQuote
          output.add("$$")
          idx = j + 1
          matched = true
        elif j < sql.len and sql[j] in {'a' .. 'z', 'A' .. 'Z', '_'}:
          while j < sql.len and sql[j] in {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'}:
            inc j
          if j < sql.len and sql[j] == '$':
            dollarTag = sql[idx .. j]
            state = sDollarQuote
            output.add(dollarTag)
            idx = j + 1
            matched = true
        if not matched:
          output.add(c)
          inc idx
      else:
        normalBody
    of sSingleQuote:
      output.add(c)
      if c == '\'':
        if idx + 1 < sql.len and sql[idx + 1] == '\'':
          output.add('\'')
          inc idx
        else:
          state = sNormal
      inc idx
    of sEString:
      output.add(c)
      if c == '\\':
        if idx + 1 < sql.len:
          inc idx
          output.add(sql[idx])
      elif c == '\'':
        if idx + 1 < sql.len and sql[idx + 1] == '\'':
          output.add('\'')
          inc idx
        else:
          state = sNormal
      inc idx
    of sDoubleQuote:
      output.add(c)
      if c == '"':
        if idx + 1 < sql.len and sql[idx + 1] == '"':
          output.add('"')
          inc idx
        else:
          state = sNormal
      inc idx
    of sDollarQuote:
      if c == '$' and idx + dollarTag.len <= sql.len and
          sql[idx ..< idx + dollarTag.len] == dollarTag:
        output.add(dollarTag)
        idx += dollarTag.len
        state = sNormal
      else:
        output.add(c)
        inc idx

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
  result = newStringOfCap(sql.len + 16)
  var i = 0
  var paramIdx = 0
  var state = sNormal
  var dollarTag = ""

  sqlParseLoop(sql, result, i, state, dollarTag):
    case c
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

macro sql*(queryStr: static[string]): untyped =
  ## Compile-time macro that parses ``{expr}`` placeholders in a SQL string
  ## literal, replacing them with ``$1, $2, …`` and collecting the expressions
  ## as ``PgParam`` values.
  ##
  ## Use ``{{`` and ``}}`` to produce literal braces.  Placeholders inside
  ## single-quoted SQL strings, ``E'…'`` strings, double-quoted identifiers,
  ## and dollar-quoted strings are left as-is.
  var resultSql = newStringOfCap(queryStr.len)
  var paramNodes = newNimNode(nnkBracket)
  var paramIdx = 0
  var i = 0
  var state = sNormal
  var dollarTag = ""

  sqlParseLoop(queryStr, resultSql, i, state, dollarTag):
    case c
    of '{':
      if i + 1 < queryStr.len and queryStr[i + 1] == '{':
        resultSql.add('{')
        i += 2
      else:
        let start = i + 1
        var depth = 1
        var j = start
        while j < queryStr.len and depth > 0:
          let ch = queryStr[j]
          if ch == '\'':
            # Nim char literal: skip 'x' or '\x'
            inc j
            if j < queryStr.len and queryStr[j] == '\\':
              j += min(2, queryStr.len - j)
            elif j < queryStr.len:
              inc j
            if j < queryStr.len and queryStr[j] == '\'':
              inc j
          elif ch == '"':
            # Nim string literal: skip until closing " (handle \" escape)
            inc j
            while j < queryStr.len:
              if queryStr[j] == '\\':
                j += min(2, queryStr.len - j)
              elif queryStr[j] == '"':
                inc j
                break
              else:
                inc j
          elif ch == '{':
            inc depth
            inc j
          elif ch == '}':
            dec depth
            if depth > 0:
              inc j
          else:
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

  let sqlLit = newStrLitNode(resultSql)
  let paramsSeq = newTree(nnkPrefix, ident"@", paramNodes)

  result = quote:
    SqlQuery(query: `sqlLit`, params: `paramsSeq`)

macro sqlQueryForwards(body: untyped): untyped =
  ## Generate ``SqlQuery`` forwarding templates from compact proc declarations.
  ##
  ## Each entry is a bodiless ``proc`` whose ``sq: SqlQuery`` parameter is
  ## expanded to ``sq.query, sq.params`` in the generated template body.
  ## ``_: typedesc[T]`` parameters are forwarded as ``T``.
  result = newStmtList()
  for child in body:
    child.expectKind(nnkProcDef)
    let name = child[0]
    let genericParams = child[2]
    let formalParams = child[3]
    var procIdent =
      if name.kind == nnkPostfix:
        name[1]
      else:
        name

    var sqIdx = -1
    for i in 1 ..< formalParams.len:
      if formalParams[i][0].eqIdent("sq"):
        sqIdx = i
        break
    doAssert sqIdx >= 0, "missing sq: SqlQuery in " & repr(procIdent)

    var call = newCall(procIdent)
    for i in 1 ..< formalParams.len:
      let param = formalParams[i]
      if i == sqIdx:
        call.add(newDotExpr(ident"sq", ident"query"))
        call.add(newDotExpr(ident"sq", ident"params"))
      elif param[1].kind == nnkBracketExpr and param[1][0].eqIdent("typedesc"):
        call.add(param[1][1])
      else:
        call.add(param[0])

    var tmpl = newNimNode(nnkTemplateDef)
    tmpl.add(name.copyNimTree())
    tmpl.add(newEmptyNode())
    tmpl.add(genericParams.copyNimTree())
    tmpl.add(formalParams.copyNimTree())
    tmpl.add(newEmptyNode())
    tmpl.add(newEmptyNode())
    tmpl.add(call)
    result.add(tmpl)

# SqlQuery forwarding templates – PgConnection

sqlQueryForwards:
  proc exec*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc query*(
    conn: PgConnection,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryOne*(
    conn: PgConnection,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryRow*(
    conn: PgConnection,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryEach*(
    conn: PgConnection,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryValue*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValue*[T](
    conn: PgConnection, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValueOpt*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValueOpt*[T](
    conn: PgConnection, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValueOrDefault*(
    conn: PgConnection,
    sq: SqlQuery,
    default: string = "",
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryValueOrDefault*[T](
    conn: PgConnection,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryExists*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryColumn*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc execInTransaction*(
    conn: PgConnection, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc execInTransaction*(
    conn: PgConnection,
    sq: SqlQuery,
    opts: TransactionOptions,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryInTransaction*(
    conn: PgConnection,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryInTransaction*(
    conn: PgConnection,
    sq: SqlQuery,
    opts: TransactionOptions,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc addExec*(p: Pipeline, sq: SqlQuery): untyped
  proc addQuery*(
    p: Pipeline, sq: SqlQuery, resultFormat: ResultFormat = rfAuto
  ): untyped

# SqlQuery forwarding templates – PgPool

sqlQueryForwards:
  proc exec*(pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration): untyped
  proc query*(
    pool: PgPool,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryOne*(
    pool: PgPool,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryRow*(
    pool: PgPool,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryEach*(
    pool: PgPool,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryValue*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValue*[T](
    pool: PgPool, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValueOpt*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValueOpt*[T](
    pool: PgPool, _: typedesc[T], sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryValueOrDefault*(
    pool: PgPool, sq: SqlQuery, default: string = "", timeout: Duration = ZeroDuration
  ): untyped

  proc queryValueOrDefault*[T](
    pool: PgPool,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc queryExists*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryColumn*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc execInTransaction*(
    pool: PgPool, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc queryInTransaction*(
    pool: PgPool,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

# SqlQuery forwarding templates – PgPoolCluster (read)

sqlQueryForwards:
  proc readQuery*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc readQueryOne*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc readQueryRow*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc readQueryValue*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc readQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc readQueryValueOpt*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc readQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc readQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    default: string = "",
    timeout: Duration = ZeroDuration,
  ): untyped

  proc readQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc readQueryExists*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc readQueryColumn*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc readQueryEach*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

# SqlQuery forwarding templates – PgPoolCluster (write)

sqlQueryForwards:
  proc writeExec*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc writeQuery*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeQueryOne*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeQueryRow*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeQueryValue*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc writeQueryValue*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeQueryValueOpt*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc writeQueryValueOpt*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeQueryValueOrDefault*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    default: string = "",
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeQueryValueOrDefault*[T](
    cluster: PgPoolCluster,
    _: typedesc[T],
    sq: SqlQuery,
    default: T,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeQueryExists*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc writeQueryColumn*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc writeQueryEach*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    callback: RowCallback,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped

  proc writeExecInTransaction*(
    cluster: PgPoolCluster, sq: SqlQuery, timeout: Duration = ZeroDuration
  ): untyped

  proc writeQueryInTransaction*(
    cluster: PgPoolCluster,
    sq: SqlQuery,
    resultFormat: ResultFormat = rfAuto,
    timeout: Duration = ZeroDuration,
  ): untyped
