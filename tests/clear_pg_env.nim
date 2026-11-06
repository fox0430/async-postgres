## Drops libpq's ``PG*`` variables from the test process, so a developer's
## psql settings (``PGPORT``, ``PGPASSWORD``, ``PGSSLMODE``, ...) never reach
## a ``parseDsn`` that reads the environment, and points ``PGPASSFILE`` at a
## missing file so their ``~/.pgpass`` is not read either.

import std/[os, strutils]

proc clearPgEnv*() =
  var names: seq[string]
  for name, _ in envPairs():
    if name.startsWith("PG"):
      names.add name
  for name in names:
    delEnv(name)
  # Under a directory never created; an empty value would count as unset.
  putEnv("PGPASSFILE", getTempDir() / "async_postgres_no_pgpass" / "pgpass")
