## Fixture for `test_sql`: another module's `toPgParam(string)` in scope
## alongside the built-in one.

import ../async_postgres/[pg_types, pg_sql]
import sql_shadow_fixture

const
  stringParamCompiles* = compiles(pgParams("x")) or compiles(sql"SELECT {""x""}")
  # Controls: the rejection above is the ambiguity, not a missing overload.
  int32ParamCompiles* = compiles(pgParams(1'i32)) and compiles(sql"SELECT {1'i32}")
  qualifiedCallCompiles* = compiles(sql_shadow_fixture.toPgParam("x"))
