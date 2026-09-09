# Package

version = "0.3.0"
author = "fox0430"
description = "Async PostgreSQL client"
license = "MIT"

# Dependencies

requires "nim >= 2.2.4"
requires "nimcrypto >= 0.7.3"
requires "checksums >= 0.2.2"
requires "unicodedb >= 0.13.2"
requires "normalize >= 0.9.0"

task apiSurface, "check the exported surface against tests/api_surface.golden":
  exec "nim c -r --hints:off tools/api_surface.nim check tests/api_surface.golden"

task apiSurfaceWrite, "regenerate tests/api_surface.golden after a reviewed change":
  exec "nim c -r --hints:off tools/api_surface.nim write tests/api_surface.golden"

task test, "run the full suite (requires a live PostgreSQL on 127.0.0.1:15432)":
  apiSurfaceTask()
  exec "nim c -d:asyncBackend=asyncdispatch -r tests/all_tests.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests.nim"

task test_unit, "run unit and mock-server tests only (no PostgreSQL required)":
  exec "nim c -d:asyncBackend=asyncdispatch -r tests/all_tests_unit.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests_unit.nim"
