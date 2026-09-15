## End-to-end tests that require a live PostgreSQL at 127.0.0.1:15432
## (started via docker-compose.yml). Run these only where Docker/PostgreSQL
## is available; the unit/mock suite lives in all_tests_unit.nim.
{.push warning[UnusedImport]: off.}
import
  test_abandonment_e2e, test_advisory_lock, test_cancel_e2e, test_e2e_arrays,
  test_e2e_connection, test_e2e_convenience, test_e2e_copy, test_e2e_cursor,
  test_e2e_listen, test_e2e_misc, test_e2e_pool, test_e2e_query, test_e2e_transaction,
  test_e2e_types, test_largeobject, test_tracing
{.pop.}
