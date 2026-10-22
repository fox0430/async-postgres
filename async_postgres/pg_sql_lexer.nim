## SQL lexing shared by ``sqlParams``, the ``sql`` macro and the compile-time
## ``$N`` check of ``queryDirect`` / ``execDirect``, so all three agree on what
## is quoted or commented out.

type SqlParseState = enum
  sNormal
  sSingleQuote
  sEString
  sDoubleQuote
  sDollarQuote
  sLineComment
  sBlockComment

const
  # PG's ident_start/ident_cont and dolq_start/dolq_cont (scan.l).
  sqlIdentStartChars = {'a' .. 'z', 'A' .. 'Z', '_', '\x80' .. '\xff'}
  sqlIdentContChars* = sqlIdentStartChars + {'0' .. '9', '$'}
  sqlDollarTagContChars = sqlIdentStartChars + {'0' .. '9'}

func dollarTagLen(sql: string, idx: int): int =
  ## Length of the ``$$`` or ``$tag$`` starting at ``idx``, or 0.
  var j = idx + 1
  if j < sql.len and sql[j] in sqlIdentStartChars:
    while j < sql.len and sql[j] in sqlDollarTagContChars:
      inc j
  if j < sql.len and sql[j] == '$':
    j + 1 - idx
  else:
    0

template sqlParseLoop*(
    sql: string, output: var string, idx: var int, normalBody: untyped
) =
  ## Copies ``sql`` from ``idx`` to ``output`` while tracking quotes and
  ## comments. ``normalBody`` runs for each character in normal code that does
  ## not open a quote or comment, except a ``$`` inside an identifier; the ``$``
  ## of ``$N`` reaches it. It advances ``idx`` and adds to ``output`` only what
  ## it keeps. Inside ``normalBody``, ``c`` is ``sql[idx]``.
  var state = sNormal
  var dollarTag = ""
  var blockDepth = 0
  # The identifier token just read, if any: ``$`` continues it instead of
  # opening a dollar quote, and only a lone ``E`` prefixes an E-string.
  var identStart, identEnd = -1
  while idx < sql.len:
    let c {.inject.} = sql[idx]
    case state
    of sNormal:
      let tagLen =
        if c == '$' and identEnd != idx:
          dollarTagLen(sql, idx)
        else:
          0
      if c == '\'':
        let isE =
          identEnd == idx and identStart == idx - 1 and sql[idx - 1] in {'E', 'e'}
        state = if isE: sEString else: sSingleQuote
        output.add(c)
        inc idx
      elif c == '"':
        state = sDoubleQuote
        output.add(c)
        inc idx
      elif c == '$' and identEnd == idx:
        # Part of the identifier, as in ``price$usd``.
        output.add(c)
        inc idx
        identEnd = idx
      elif tagLen > 0:
        dollarTag = sql[idx ..< idx + tagLen]
        state = sDollarQuote
        output.add(dollarTag)
        idx += tagLen
      elif c == '-' and idx + 1 < sql.len and sql[idx + 1] == '-':
        state = sLineComment
        output.add("--")
        idx += 2
      elif c == '/' and idx + 1 < sql.len and sql[idx + 1] == '*':
        state = sBlockComment
        blockDepth = 1
        output.add("/*")
        idx += 2
      else:
        if identEnd == idx and c in sqlIdentContChars:
          identEnd = idx + 1
        elif c in sqlIdentStartChars:
          identStart = idx
          identEnd = idx + 1
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
    of sLineComment:
      output.add(c)
      # PG's non_newline excludes CR too, so a lone CR ends the comment.
      if c in {'\n', '\r'}:
        state = sNormal
      inc idx
    of sBlockComment:
      # PostgreSQL block comments nest — track depth.
      if c == '/' and idx + 1 < sql.len and sql[idx + 1] == '*':
        output.add("/*")
        inc blockDepth
        idx += 2
      elif c == '*' and idx + 1 < sql.len and sql[idx + 1] == '/':
        output.add("*/")
        dec blockDepth
        idx += 2
        if blockDepth == 0:
          state = sNormal
      else:
        output.add(c)
        inc idx
