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

task test, "test":
  exec "nim c -d:asyncBackend=asyncdispatch -r tests/all_tests.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests.nim"
