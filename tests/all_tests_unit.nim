## Unit and in-process mock-server tests. None of these require a live
## PostgreSQL: every test either exercises pure logic or connects to an
## in-process mock server on an ephemeral port. They therefore run anywhere,
## including CI hosts without Docker (e.g. macOS runners).
{.push warning[UnusedImport]: off.}
import
  test_auth, test_dsn, test_fill_recvbuf, test_keepalive, test_listen_reconnect,
  test_network_failure, test_physical_replication, test_pool, test_pool_cluster,
  test_protocol, test_protocol_fuzz, test_replication, test_replication_keepalive,
  test_rowdata, test_session_attrs, test_sql, test_ssl, test_types
{.pop.}
