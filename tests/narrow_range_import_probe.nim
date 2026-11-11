## Compile-time probe, imported first by `test_types_range`: a module that
## imports only the names it uses must still read and write multiranges over a
## `distinct DateTime`, so the generic helpers it instantiates cannot lean on
## the caller importing the multirange `[]` or `items`. The types are local so
## no other module's instance of those helpers can stand in for this one's.

import std/times

from ../async_postgres/pg_protocol import Row
from ../async_postgres/pg_types import pgComposite, toPgParam, to, PgMultirange, PgParam

type
  ProbeAt = distinct DateTime

  ProbeSpans = object
    spans: PgMultirange[DateTime]
    domainSpans: PgMultirange[ProbeAt]

# Its toPgParam is not generic, so a failure stops the build at this line.
pgComposite(ProbeSpans)

when not compiles(default(Row).to(tuple[spans: PgMultirange[ProbeAt]])):
  {.
    error: "`to` does not read `PgMultirange[distinct DateTime]` under a narrow import"
  .}
