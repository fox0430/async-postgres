## End-to-end tests that require a live PostgreSQL at 127.0.0.1:15432
## (started via docker-compose.yml). Run these only where Docker/PostgreSQL
## is available; the unit/mock suite lives in all_tests_unit.nim.
{.push warning[UnusedImport]: off.}
import
  test_abandonment_e2e, test_advisory_lock, test_cancel_e2e, test_e2e, test_largeobject,
  test_tracing
{.pop.}
