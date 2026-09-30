## Line-level helpers shared by the source guards (`parse_guard`,
## `private_access_guard`).

const identChars* = {'A' .. 'Z', 'a' .. 'z', '0' .. '9', '_'}

type LineScanner* = object
  ## State that outlives a single line. ``#[ ... ]#`` / ``##[ ... ]##``
  ## comments and ``"""`` strings span lines, so a per-call reset would scan
  ## their bodies as code -- and would latch the rest of the file into
  ## "comment" or "string". Make one per file: an unterminated literal must
  ## not leak into the next file. An ordinary `"` string cannot span lines in
  ## Nim, so `inStr` is latched only as a guard against malformed input.
  blockDepth: int ## Nesting depth of ``#[ ... ]#`` comments.
  docDepth: int ## Nesting depth of ``##[ ... ]##`` comments.
  inRaw: bool ## Inside a ``"""`` string.
  inStr: bool ## Inside a `"` string (see the note above).
  noEscapes: bool
    ## The open `"` carried an ``r`` prefix, so a backslash is an
    ## ordinary character. Without this a `"` that only looks
    ## escaped (``r"a\"``) would latch ``inStr`` and blank the rest
    ## of the file.

proc isIdent*(line: string, at, width: int): bool =
  ## Whether the match at ``at`` is a whole identifier (catches ``s.parseInt``
  ## and bare values, not just ``name(``).
  (at == 0 or line[at - 1] notin identChars) and
    (at + width >= line.len or line[at + width] notin identChars)

proc hasRawPrefix(blanked: string): bool =
  ## Whether the quote just consumed was written as ``r"..."``. ``blanked`` is
  ## the blanked prefix up to (not including) that quote, so its last char is
  ## the one before it, and that char must not itself be an identifier char
  ## (``foo"..."`` is not a prefixed string). ``c"..."`` is gone from Nim, so
  ## ``c`` gets no special handling.
  if blanked.len == 0 or blanked[^1] notin {'r', 'R'}:
    return false
  blanked.len < 2 or blanked[^2] notin identChars

proc consumeQuoteRun(line: string, i: var int, dst: var string): int =
  ## Blank the run of ``"`` starting at ``i``, step past it, and report its
  ## length. Three or more delimit a ``"""`` string.
  result = 0
  while i < line.len and line[i] == '"':
    dst.add ' '
    inc i
    inc result

proc stripLiteralsAndComment*(line: string, sc: var LineScanner): string =
  ## Blank string/char literals and ``#[ ... ]#`` / ``##[ ... ]##`` bodies,
  ## then drop the trailing comment. Prevents a ``'#'`` literal, a quoted
  ## identifier, or a multi-line comment from confusing the scan. ``sc``
  ## carries the state that spans lines.
  result = newStringOfCap(line.len)
  var
    i = 0
    # A char literal cannot span lines, so this one stays per line: a stray
    # quote must not blank the rest of the file.
    inChar = false
  while i < line.len:
    let c = line[i]
    if sc.blockDepth > 0:
      # `#[ ... ]#` is nestable, so an inner open counts while we are inside.
      if c == '#' and i + 1 < line.len and line[i + 1] == '[':
        inc sc.blockDepth
        result.add "  "
        i += 2
      elif c == ']' and i + 1 < line.len and line[i + 1] == '#':
        dec sc.blockDepth
        result.add "  "
        i += 2
      else:
        result.add ' '
        inc i
      continue
    if sc.docDepth > 0:
      # `##[ ... ]##` doc comments: only `##[` nests and only `]##` closes.
      if c == '#' and i + 2 < line.len and line[i + 1] == '#' and line[i + 2] == '[':
        inc sc.docDepth
        result.add "   "
        i += 3
      elif c == ']' and i + 2 < line.len and line[i + 1] == '#' and line[i + 2] == '#':
        dec sc.docDepth
        result.add "   "
        i += 3
      else:
        result.add ' '
        inc i
      continue
    if sc.inRaw:
      # No escapes in a raw string; a run of three closes it.
      let n = consumeQuoteRun(line, i, result)
      if n == 0:
        result.add ' '
        inc i
      elif n >= 3:
        sc.inRaw = false
      continue
    if sc.inStr or inChar:
      if c == '\\' and not sc.noEscapes:
        if i + 1 < line.len:
          result.add "  "
          i += 2
        else:
          # Nim cannot continue a string across lines, so a trailing `\` ends
          # the (malformed) literal here instead of latching the file.
          sc.inStr = false
          sc.noEscapes = false
          result.add ' '
          inc i
        continue
      if (sc.inStr and c == '"') or (inChar and c == '\''):
        sc.inStr = false
        sc.noEscapes = false
        inChar = false
        result.add(c)
      else:
        result.add(' ')
      inc i
    elif c == '"':
      # Read the prefix before consumeQuoteRun blanks the quote itself.
      let prefixed = hasRawPrefix(result)
      let n = consumeQuoteRun(line, i, result)
      if n >= 3:
        # `"""` opens; a 4th+ quote is body and a 6th+ closes again.
        sc.inRaw = n < 6
      elif n == 1:
        sc.inStr = true
        sc.noEscapes = prefixed
    elif c == '\'':
      # `'x'` / `'\n'` opens a char literal; `1'i32` / `0xFF'u8` is a type
      # suffix with no closing quote. Tell them apart by lookahead.
      if (i + 1 < line.len and line[i + 1] == '\\') or
          (i + 2 < line.len and line[i + 2] == '\''):
        inChar = true
        result.add(c)
        inc i
      else:
        result.add ' '
        inc i
        while i < line.len and line[i] in identChars:
          result.add ' '
          inc i
    elif c == '#':
      if i + 2 < line.len and line[i + 1] == '#' and line[i + 2] == '[':
        inc sc.docDepth
        result.add "   "
        i += 3
        continue
      if i + 1 < line.len and line[i + 1] == '[':
        inc sc.blockDepth
        result.add "  "
        i += 2
        continue
      break
    else:
      result.add(c)
      inc i
