#!/usr/bin/env bash
# Append a permissive replication line to pg_hba.conf at init time.
#
# The default docker-entrypoint pg_hba.conf only allows replication from
# 127.0.0.1 and ::1. Physical-replication integration tests connect from
# the docker bridge address (e.g. 172.x.x.x), so without this entry the
# server returns SQLSTATE 28000 ("no pg_hba.conf entry for replication
# connection").
#
# Runs once during the first-boot init phase of the official postgres image
# (files in /docker-entrypoint-initdb.d/). The container is initialised
# fresh on every `docker compose up` because the data directory is an
# anonymous volume, so this script runs every time without check.
set -euo pipefail

HBA="${PGDATA:?PGDATA must be set}/pg_hba.conf"

if grep -qE '^[^#]*host[[:space:]]+replication[[:space:]]+all[[:space:]]+all' "$HBA"; then
  exit 0
fi

{
  echo
  echo "# Added by async-postgres tests/pg_init for physical replication tests"
  echo "host    replication     all             all             scram-sha-256"
} >> "$HBA"
