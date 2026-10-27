## Fixture for `test_row_mapping`: a `get` overload declared in a module the
## `to` caller takes only the types from (`from ... import`), so the caller
## cannot see it.

import ../async_postgres/[pg_protocol, pg_types]

type
  RemoteFee* = object
    cents*: int64

  RemoteBill* = object
    fee*: RemoteFee

proc get*(row: Row, col: int, T: typedesc[RemoteFee]): RemoteFee =
  RemoteFee(cents: row.getInt64(col))
