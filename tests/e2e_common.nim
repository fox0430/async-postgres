## Shared constants and helpers for E2E tests that require a live
## PostgreSQL at 127.0.0.1:15432 (docker-compose.yml).

import std/os

import ../async_postgres/pg_connection

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

proc sslConfig*(mode: SslMode = sslRequire): ConnConfig =
  ConnConfig(
    host: PgHost,
    port: PgPort,
    user: PgUser,
    password: PgPassword,
    database: PgDatabase,
    sslMode: mode,
  )

proc loadCaCert*(): string =
  let certsDir = currentSourcePath().parentDir / "certs"
  readFile(certsDir / "ca.crt")

proc loadWrongCaCert*(): string =
  let certsDir = currentSourcePath().parentDir / "certs"
  readFile(certsDir / "wrong_ca.crt")
