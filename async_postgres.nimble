# Package

version = "0.3.0"
author = "fox0430"
description = "Async PostgreSQL client"
license = "MIT"

# Dependencies

requires "nim >= 2.2.4"
requires "nimcrypto >= 0.7.3"
requires "checksums >= 0.2.2"

task test, "run the full suite (requires a live PostgreSQL on 127.0.0.1:15432)":
  exec "nim c -d:asyncBackend=asyncdispatch -r tests/all_tests.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests.nim"

task test_unit, "run unit and mock-server tests only (no PostgreSQL required)":
  exec "nim c -d:asyncBackend=asyncdispatch -r tests/all_tests_unit.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests_unit.nim"
