## Tests that need no PostgreSQL (pure logic or in-process mock servers).
{.push warning[UnusedImport]: off.}
import
  test_aggregate, test_async_backend, test_auth, test_bytes, test_cache,
  test_conn_types, test_copy_race, test_dsn, test_errors, test_fill_recvbuf,
  test_keepalive, test_largeobject_parse, test_listen_reconnect, test_network_failure,
  test_physical_replication, test_pool, test_pool_cluster, test_private_access_guard,
  test_protocol, test_protocol_fuzz, test_replication, test_replication_auto_confirm,
  test_replication_keepalive, test_replication_restart, test_rowdata, test_saslprep,
  test_sendbuf_seal, test_server_error, test_session_attrs, test_source_scan, test_sql,
  test_ssl, test_tls_error_paths, test_transaction_cancel, test_tx_cleanup_defect,
  test_type_lookup, test_types_array, test_types_inline, test_types_misc,
  test_types_numeric, test_types_range, test_types_scalar, test_types_temporal,
  test_types_user_defined, test_types_validation
{.pop.}
