## Shared constants and helpers for E2E tests that require a live
## PostgreSQL at 127.0.0.1:15432 (docker-compose.yml).

import ../async_postgres/[pg_client, pg_connection, pg_types]

const
  PgHost* = "127.0.0.1"
  PgPort* = 15432
  PgUser* = "test"
  PgPassword* = "test"
  PgDatabase* = "test"

proc plainConfig*(): ConnConfig =
  ConnConfig(
    host: PgHost,
    port: PgPort,
    user: PgUser,
    password: PgPassword,
    database: PgDatabase,
    sslMode: sslDisable,
  )
