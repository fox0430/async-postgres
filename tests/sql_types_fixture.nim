## Fixture for `test_sql`: `toPgParam` overloads generated in a module other
## than the `pgParams` / `sql` caller, which reaches them only by importing it.

import ../async_postgres/pg_types

type
  CallerMood* = enum
    cmHappy
    cmSad

  CallerEmail* = distinct string
  CallerPerson* = object
    name*: string
    age*: int32

  CallerId* = distinct int32

pgEnum(CallerMood)
pgDomain(CallerEmail, string)
pgComposite(CallerPerson)

proc toPgParam*(v: CallerId): PgParam =
  toPgParam(int32(v))
