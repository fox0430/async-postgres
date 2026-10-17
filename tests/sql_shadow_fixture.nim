## Fixture for `test_sql`: a `toPgParam` with a built-in signature, declared
## where `pgParams` / `sql` are called. Keep generic forms such as
## `Option[string]` out of here: their instances are shared across modules.

import ../async_postgres/[pg_types, pg_sql]

const ShadowOid* = 99'i32

proc toPgParam*(v: string): PgParam =
  PgParam(oid: ShadowOid)

proc shadowedPgParams*(): seq[PgParam] =
  pgParams("x")

proc shadowedSqlParams*(): seq[PgParam] =
  let s = "x"
  sql"SELECT {s}".params
