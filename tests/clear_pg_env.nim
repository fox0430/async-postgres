## Drops libpq's ``PG*`` variables from the test process, so a developer's
## psql settings (``PGPORT``, ``PGPASSWORD``, ``PGSSLMODE``, ...) never reach
## a ``parseDsn`` that reads the environment, and points the user's directory
## (``HOME``, ``APPDATA`` on Windows) at a missing one so their ``~/.pgpass``,
## ``~/.pg_service.conf`` and ``~/.postgresql`` TLS files are not read either.

import std/[os, strutils]

const noUserDir = "async_postgres_no_user_dir" # under the temp dir, never created

proc clearPgEnv*() =
  var names: seq[string]
  for name, _ in envPairs():
    if name.startsWith("PG"):
      names.add name
  for name in names:
    delEnv(name)
  # An empty value would count as unset.
  putEnv((when defined(windows): "APPDATA" else: "HOME"), getTempDir() / noUserDir)
  putEnv("PGPASSFILE", getTempDir() / noUserDir / "pgpass")
