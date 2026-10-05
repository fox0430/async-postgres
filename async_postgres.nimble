# Package

version = "0.4.0"
author = "fox0430"
description = "Async PostgreSQL client"
license = "MIT"

# Dependencies

requires "nim >= 2.2.4"
requires "nimcrypto >= 0.7.3"
requires "checksums >= 0.2.2"
requires "unicodedb >= 0.13.2"
requires "normalize >= 0.9.0"

import std/os

task apiSurface,
  "check promised vs sibling-export surfaces against the two golden files":
  exec "nim c -r --hints:off tools/api_surface.nim check tests/api_surface.public.golden tests/api_surface.internal.golden"

task apiSurfaceWrite,
  "regenerate tests/api_surface.public.golden and tests/api_surface.internal.golden":
  exec "nim c -r --hints:off tools/api_surface.nim write tests/api_surface.public.golden tests/api_surface.internal.golden"

task parseGuard, "check that stdlib text parsers are only called from the grammar layer":
  exec "nim c -r --hints:off tools/parse_guard.nim"

task privateAccessGuard,
  "check that privateAccess and {.all.} imports are only used from tests":
  exec "nim c -r --hints:off tools/private_access_guard.nim"

task macroSymGuard,
  "check that library macros declare no locals a caller's name can collide with":
  exec "nim c -r --hints:off tools/macro_sym_guard.nim"

task escapeDiagnostics,
  "check the compile errors of the scoped-body escape check with `nim check`":
  exec "nim c -r --hints:off -d:asyncBackend=asyncdispatch tests/test_body_escape_diagnostics.nim"
  exec "nim c -r --hints:off -d:asyncBackend=chronos tests/test_body_escape_diagnostics.nim"

proc runSuite(file: string) =
  ## Build and run `file` once per async backend.
  # A bare `bash` gets System32\bash.exe (the WSL launcher) on Windows, because
  # CreateProcess searches there before PATH. findExe walks PATH like a shell.
  exec quoteShell(findExe("bash")) & " tests/gen_certs.sh"
  exec "nim c -d:asyncBackend=asyncdispatch -r " & file
  exec "nim c -d:asyncBackend=chronos -r " & file

task test, "run the full suite (requires a live PostgreSQL on 127.0.0.1:15432)":
  apiSurfaceTask()
  parseGuardTask()
  privateAccessGuardTask()
  macroSymGuardTask()
  escapeDiagnosticsTask()
  runSuite "tests/all_tests.nim"

task testUnit, "run unit and mock-server tests only (no PostgreSQL required)":
  runSuite "tests/all_tests_unit.nim"
