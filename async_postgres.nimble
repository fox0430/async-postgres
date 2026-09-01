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

task apiSurface, "check the public symbol surface against its baseline":
  # `async_postgres.nim` re-exports a reviewed whitelist, but a direct module
  # import reaches everything marked `*`. Diffing keeps that a decision.
  # `mktemp` rather than a fixed path: concurrent runs on one host (a shared CI
  # runner, two developers) must not clobber each other's scratch file.
  exec """
set -e
now=$(mktemp /tmp/apiSurface.XXXXXX)
trap 'rm -f "$now"' EXIT
python3 tools/public_surface.py > "$now"
if ! diff -u tools/public_surface.txt "$now"; then
  echo "" >&2
  echo "public API surface changed. If intended, regenerate the baseline:" >&2
  echo "  python3 tools/public_surface.py > tools/public_surface.txt" >&2
  exit 1
fi
"""

task test, "test":
  exec "nim c -d:asyncBackend=asyncdispatch -r tests/all_tests.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests.nim"
