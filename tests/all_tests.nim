{.push warning[UnusedImport]: off.}
# Note: test_cancel_e2e and test_abandonment_e2e are intentionally excluded.
# They rely on pg_sleep and short timeouts to exercise long-running cancel /
# abandonment paths, which adds tens of seconds to `nimble test`. Run them on
# demand with `nim c -r tests/test_cancel_e2e.nim` etc. when touching the
# cancel / cursor / COPY OUT / pipeline lifecycle code.
import
  test_advisory_lock, test_auth, test_dsn, test_e2e, test_keepalive, test_largeobject,
  test_network_failure, test_pool, test_protocol, test_protocol_fuzz, test_rowdata,
  test_sql, test_ssl, test_tracing, test_types, test_pool_cluster
{.pop.}
