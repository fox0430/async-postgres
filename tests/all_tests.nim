## Full test suite: the unit/mock tests plus the live-PostgreSQL integration
## tests. Requires a running PostgreSQL (see docker-compose.yml). For a run
## that needs no database, use all_tests_unit.nim instead.
{.push warning[UnusedImport]: off.}
import all_tests_unit, all_tests_integration
{.pop.}
