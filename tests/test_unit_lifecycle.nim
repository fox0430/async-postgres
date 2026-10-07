## Unit tests for `pg_connection/lifecycle` pure helpers.
##
## PG-less: `filterSaslByRequireAuth`, `selectScramMechanism`, and
## `orderedHosts` are pure and need no server.

import std/unittest

import ../async_postgres/[async_backend, pg_connection]
import ../async_postgres/pg_connection/lifecycle {.all.}

suite "lifecycle: filterSaslByRequireAuth":
  test "empty allowlist performs no filtering":
    let offered = @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]
    check filterSaslByRequireAuth(offered, {}) == offered

  test "allowlist keeps only permitted mechanisms":
    let offered = @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]
    # libpq: scram-sha-256 covers PLUS too.
    check filterSaslByRequireAuth(offered, {amScramSha256}) == offered
    check filterSaslByRequireAuth(offered, {amScramSha256Plus}) ==
      @["SCRAM-SHA-256-PLUS"]

  test "unknown mechanisms are dropped when filtering":
    let offered = @["SCRAM-SHA-256", "FUTURE-MECH"]
    check filterSaslByRequireAuth(offered, {amScramSha256}) == @["SCRAM-SHA-256"]
    check filterSaslByRequireAuth(offered, {amScramSha256Plus}).len == 0

suite "lifecycle: selectScramMechanism":
  test "cbDisable picks SCRAM-SHA-256":
    let r = selectScramMechanism(false, [], @["SCRAM-SHA-256"], cbDisable)
    check r.mechanism == "SCRAM-SHA-256"

  test "cbDisable with only PLUS raises":
    expect PgConnectionError:
      discard selectScramMechanism(false, [], @["SCRAM-SHA-256-PLUS"], cbDisable)

  test "cbRequire without SSL raises PgSecurityError":
    expect PgSecurityError:
      discard selectScramMechanism(false, [], @["SCRAM-SHA-256-PLUS"], cbRequire)

  test "cbRequire with PLUS but no cert raises":
    expect PgSecurityError:
      discard selectScramMechanism(true, [], @["SCRAM-SHA-256-PLUS"], cbRequire)

  test "cbPrefer falls back to plain SCRAM without channel binding":
    # sslEnabled but no cert: PLUS is unusable, plain SCRAM is picked.
    let r =
      selectScramMechanism(true, [], @["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"], cbPrefer)
    check r.mechanism == "SCRAM-SHA-256"

  test "cbPrefer rejects a PLUS-only offer without channel binding":
    expect PgConnectionError:
      discard selectScramMechanism(false, [], @["SCRAM-SHA-256-PLUS"], cbPrefer)

suite "lifecycle: orderedHosts":
  test "single host round-trips":
    let cfg = ConnConfig(
      host: "127.0.0.1", port: 5432, user: "u", database: "d", sslMode: sslDisable
    )
    let hosts = orderedHosts(cfg)
    check hosts.len == 1
    check hosts[0].host == "127.0.0.1"

  test "explicit hosts list is preserved in order":
    let cfg = ConnConfig(
      hosts: @[
        HostEntry(host: "127.0.0.1", port: 5432),
        HostEntry(host: "127.0.0.2", port: 5433),
      ],
      user: "u",
      database: "d",
      sslMode: sslDisable,
    )
    let hosts = orderedHosts(cfg)
    check hosts.len == 2
    check hosts[0].port == 5432
    check hosts[1].port == 5433
