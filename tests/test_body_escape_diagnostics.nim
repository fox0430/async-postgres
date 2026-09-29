## Compile-time diagnostics of the scoped-body escape check. `compiles()` only
## tells accepted from rejected, so this runs `nim check` on
## `body_escape_fixture.nim` and pins which message each rejected
## statement gets and where it is reported.
##
## Not part of `all_tests`: it runs the compiler, so `nimble escapeDiagnostics`
## runs it once per backend instead. The fixture and the compiler are looked
## up at run time, so the binary does not depend on where it was built.

import std/[os, osproc, strutils, tables, unittest]

const asyncBackend {.strdefine.} = "asyncdispatch"

let fixture = getAppDir() / "body_escape_fixture.nim"

proc nimExe(): string =
  result = findExe("nim")
  if result.len == 0:
    result = getCurrentCompilerExe()

proc expectedErrors(): Table[int, string] =
  ## Line number -> expected message prefix, from the fixture's markers (each
  ## applies to the line after it).
  var line = 0
  for text in fixture.lines:
    inc line
    let marker = text.strip
    if marker.startsWith("# expect: "):
      result[line + 1] = marker["# expect: ".len .. ^1]

proc firstErrors(output: string, elsewhere: var seq[string]): Table[int, string] =
  ## Line number -> first error message the compiler reports on that line of
  ## the fixture. Later ones on the same line are follow-ups of `nim check`.
  ## Errors reported in any other file are collected in `elsewhere`.
  let prefix = fixture.extractFilename & "("
  for text in output.splitLines:
    let errAt = text.find(") Error: ")
    if errAt < 0:
      continue
    let at = text.find(prefix)
    if at < 0 or at > errAt:
      elsewhere.add(text)
      continue
    let pos = text[at + prefix.len ..< errAt]
    let line = parseInt(pos.split(',')[0].strip)
    if line notin result:
      result[line] = text[errAt + ") Error: ".len .. ^1]

suite "Body escape diagnostics":
  test "each escape is reported at its statement with the right message":
    let (output, exitCode) = execCmdEx(
      quoteShellCommand(
        [nimExe(), "check", "--hints:off", "-d:asyncBackend=" & asyncBackend, fixture]
      )
    )
    let expected = expectedErrors()
    var elsewhere: seq[string]
    let actual = firstErrors(output, elsewhere)
    check expected.len > 0
    checkpoint output
    # The fixture has errors, so a zero exit means the check did not run.
    check exitCode != 0
    # An accepted case failing inside the library is reported there, not in
    # the fixture.
    check elsewhere.len == 0
    for line, msg in expected:
      checkpoint "line " & $line & ": " & output
      check line in actual
      if line in actual:
        check actual[line].startsWith(msg)
    for line, msg in actual:
      checkpoint "unexpected error at line " & $line & ": " & msg
      check line in expected
