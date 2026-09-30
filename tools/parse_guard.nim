## Restrict direct stdlib text-parser calls to the grammar layer.
##
## `strutils`/`parseutils` accept more than PostgreSQL emits (leading `+`,
## underscores, `0x` prefixes, silent overflow wrap); decoding those silently
## misreads wire data. `pg_types/core.nim` wraps them with strict checks;
## decoders must use those wrappers. This tool fails the build on direct
## calls outside the allowlist.
##
## Usage:
##   parse_guard   scan async_postgres/, exit 1 on a call outside the allowlist

import std/[os, strutils]
import source_scan

const
  srcDir = "async_postgres"
  parsers = [
    "parseInt", "parseBiggestInt", "parseUInt", "parseBiggestUInt", "parseFloat",
    "parseBiggestFloat", "parseHexInt", "parseOctInt", "parseBinInt", "parseBool",
    "parseEnum", "parseSaturatedNatural", "fromHex", "fromOct", "fromBin", "parseHex",
    "parseOct", "parseBin", "parseHexStr",
  ]
  allowed = [
    # Grammar wrappers live here.
    "async_postgres/pg_types/core.nim",
    # DSN follows libpq's strtol grammar (accepts `+5432`) with own guards.
    "async_postgres/pg_connection/dsn.nim",
  ]

proc main() =
  var violations: seq[string]
  for path in walkDirRec(srcDir):
    if not path.endsWith(".nim"):
      continue
    let rel = path.replace('\\', '/')
    if rel in allowed:
      continue
    var sc: LineScanner
    for lineNo, line in pairs(readFile(path).splitLines()):
      let code = stripLiteralsAndComment(line, sc)
      for p in parsers:
        var at = code.find(p)
        while at >= 0:
          if isIdent(code, at, p.len):
            violations.add(rel & ":" & $(lineNo + 1) & ": " & line.strip())
            break
          at = code.find(p, at + 1)
  if violations.len > 0:
    stderr.writeLine "Direct stdlib parser calls outside the grammar layer:"
    for v in violations:
      stderr.writeLine "  " & v
    stderr.writeLine ""
    stderr.writeLine(
      "Route wire text through pg_types/core's pgParse* / pgParseUIntField / " &
        "pgParseHexUInt32, or gate it with isPgIntText / isPgUIntText / isPgHexText."
    )
    quit(1)
  echo "parse_guard: ok"

main()
