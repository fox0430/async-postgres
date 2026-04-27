# Package

version = "0.2.0"
author = "fox0430"
description = "Async PostgreSQL client"
license = "MIT"

# Dependencies

requires "nim >= 2.2.4"
requires "nimcrypto >= 0.6.0"
requires "checksums >= 0.2.0"

task test, "test":
  exec "nim c -d:asyncBackend=asyncdispatch -d:ssl -r tests/all_tests.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests.nim"
