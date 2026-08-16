# テスト分布と規約マップ (async_postgres)

調査種別: 読み取り専用監査。行数は `wc -l`。CI は `nimble test` で
`-d:asyncBackend=asyncdispatch` と `-d:asyncBackend=chronos` の両方を実行
(async_postgres.nimble:16-18)。集約エントリは `tests/all_tests.nim`。

---

## 1. テストファイル一覧

凡例: 分類 = e2e(実PG必要) / unit-mock(`mock_pg_server.nim`) / unit-ownmock(独自モック) / unit-pure(サーバ不要)。
`all_tests` = `tests/all_tests.nim` の import リストに含まれるか。

**結論: 37 個の `test_*.nim` は全て `all_tests.nim` に import されている。
ファイルは存在するが CI で走らない孤立テストは無い。**
(`all_tests.nim` は 37 モジュールを import。ヘルパー `e2e_common.nim` /
`mock_pg_server.nim` / 集約自体の `all_tests.nim` はテスト本体ではない。)

### 1a. e2e テスト（実 PostgreSQL 127.0.0.1:15432 必要、docker-compose.yml）

`e2e_common.nim` 経由（13ファイル）:

| ファイル | 行数 | テスト対象モジュール | 分類 | all_tests |
|---|---|---|---|---|
| test_e2e_query.nim | 338 | pg_client/query | e2e | yes |
| test_e2e_types.nim | 828 | pg_types/core, user_types, type_lookup | e2e | yes |
| test_e2e_pool.nim | 832 | pg_pool | e2e | yes |
| test_e2e_connection.nim | 394 | pg_connection/lifecycle (connect/failover) | e2e | yes |
| test_e2e_arrays.nim | 1522 | pg_types/array | e2e | yes |
| test_e2e_listen.nim | 1293 | pg_connection/notify (LISTEN/NOTIFY) | e2e | yes |
| test_abandonment_e2e.nim | 250 | pg_pool (acquire放棄) | e2e | yes |
| test_e2e_misc.nim | 512 | pg_client その他 API | e2e | yes |
| test_e2e_cursor.nim | 973 | pg_client/cursor | e2e | yes |
| test_cancel_e2e.nim | 211 | cancel (simple_query), PgQueryError 57014 | e2e | yes |
| test_e2e_convenience.nim | 2282 | pg_client query 簡易API群 / pipeline | e2e | yes |
| test_e2e_copy.nim | 1243 | pg_client/copy | e2e | yes |
| test_e2e_transaction.nim | 3537 | pg_client/transaction + transaction_helpers | e2e | yes |

`e2e_common.nim` を使わず独自に実 PG 接続を定義（3ファイル、規約上の不整合）:

| ファイル | 行数 | テスト対象モジュール | 分類 | all_tests |
|---|---|---|---|---|
| test_advisory_lock.nim | 1069 | pg_advisory_lock + pg_pool | e2e(独自config) | yes |
| test_largeobject.nim | 767 | pg_largeobject | e2e(独自config) | yes |
| test_tracing.nim | 1614 | PgTracer フック全般 (pool/client) | e2e(独自config) | yes |

> 上記3ファイルは `e2e_common.nim` の `plainConfig()` を使わず、同一内容の
> `PgHost/PgPort=15432/plainConfig()` をローカル再定義している
> (test_advisory_lock.nim:10-25, test_largeobject.nim:8-23, test_tracing.nim:10-25)。
> 機能的には e2e（実PG依存）だが共有ヘルパー未使用。

### 1b. ユニットテスト（モック `mock_pg_server.nim` 使用、9ファイル）

| ファイル | 行数 | テスト対象モジュール | 分類 | all_tests |
|---|---|---|---|---|
| test_pool.nim | 3639 | pg_pool (最大テスト) | unit-mock | yes |
| test_pool_cluster.nim | 738 | pg_pool_cluster | unit-mock | yes |
| test_network_failure.nim | 648 | pg_connection/lifecycle + buffer_io 障害経路 | unit-mock | yes |
| test_physical_replication.nim | 579 | pg_replication (物理) | unit-mock | yes |
| test_listen_reconnect.nim | 567 | pg_connection/notify 再接続 | unit-mock | yes |
| test_fill_recvbuf.nim | 167 | pg_connection/buffer_io (fillRecvBuf) | unit-mock | yes |
| test_transaction_cancel.nim | 189 | transaction cancel/timeout | unit-mock | yes |
| test_session_attrs.nim | 390 | pg_connection/simple_query (checkSessionAttrs) | unit-mock | yes |
| test_replication_keepalive.nim | 731 | pg_replication keepalive | unit-mock | yes |

> test_pool.nim の `15432` 出現は DSN 解析テストのフィクスチャ文字列
> (test_pool.nim:81,91) であり、実PG接続ではない。接続テストはモック使用。

### 1c. ユニットテスト（独自モック、1ファイル）

| ファイル | 行数 | テスト対象モジュール | 分類 | all_tests |
|---|---|---|---|---|
| test_ssl.nim | 1706 | pg_connection/ssl + pg_bearssl | unit-ownmock | yes |

> test_ssl.nim は `mock_pg_server.nim` を使わず、TLS ハンドシェイク用の
> `startMockServer` をファイル内で独自定義 (test_ssl.nim:33,66)。
> chronos(BearSSL) / asyncdispatch(OpenSSL, `-d:ssl`) の両分岐を持つ。

### 1d. ユニットテスト（サーバ不要・純粋ロジック、11ファイル）

| ファイル | 行数 | テスト対象モジュール | 分類 | all_tests |
|---|---|---|---|---|
| test_types.nim | 9107 | pg_types (encoding/decoding/ranges/accessors/core) | unit-pure | yes |
| test_protocol.nim | 1400 | pg_protocol + pg_bytes + pg_errors アクセサ | unit-pure | yes |
| test_dsn.nim | 1144 | pg_connection/dsn | unit-pure | yes |
| test_replication.nim | 783 | pg_replication (LSN, pgoutput デコード) | unit-pure | yes |
| test_protocol_fuzz.nim | 752 | pg_protocol (malformed/fuzz) | unit-pure | yes |
| test_auth.nim | 547 | pg_auth (MD5/SCRAM 暗号) | unit-pure | yes |
| test_sql.nim | 356 | pg_sql (sql マクロ/プレースホルダ) | unit-pure | yes |
| test_rowdata.nim | 339 | pg_types/decoding (行データ) | unit-pure | yes |
| test_saslprep.nim | 167 | pg_saslprep | unit-pure | yes |
| test_keepalive.nim | 98 | pg_connection (configureKeepalive sockopt) | unit-pure | yes |
| test_async_backend.nim | 36 | async_backend (makeAsyncSeqByteCallback) | unit-pure | yes |

### 1e. ヘルパー（テスト本体ではない）

| ファイル | 行数 | 役割 |
|---|---|---|
| all_tests.nim | 12 | 集約エントリ。37 テストを import |
| e2e_common.nim | 41 | e2e 用共有定数・config (PgHost=127.0.0.1, PgPort=15432) |
| mock_pg_server.nim | 356 | インプロセス・ワイヤプロトコルモック。バックエンド非依存 |

テスト合計: 42,157 行（ヘルパー・集約含む、`wc -l tests/*.nim`）。

---

## 2. テストが無い・薄い主要モジュール

ソース合計: 26,377 行（`async_postgres/` 配下、`wc -l`。ルート `async_postgres.nim` は別途）。

### 2a. タスクで名指しされたモジュール → 実測では全て厚くカバー済み

| ソースモジュール | ソース行数 | 対応テスト | 評価 |
|---|---|---|---|
| pg_pool.nim | 2310 | test_pool(3639) + test_e2e_pool(832) + test_abandonment_e2e(250) + test_advisory_lock の pool 部 | 厚（mock+e2e、エラーパス密集） |
| pg_replication.nim | 1387 | test_replication(783) + test_physical_replication(579) + test_replication_keepalive(731) = 2093 | 厚（unit+mock） |
| pg_connection/ssl.nim | 612 | test_ssl(1706、独自TLSモック) | 厚（except 52、エラーパス密集） |
| pg_auth.nim | 359 | test_auth(547、暗号ユニット) | カバー済 |

### 2b. 実際にテストが無い/薄いモジュール

| ソースモジュール | ソース行数 | 状況 |
|---|---|---|
| async_backend.nim | 354 | 専用テストは test_async_backend.nim のみ（36行、`makeAsyncSeqByteCallback` の回帰に限定）。`wait`/`sleepMsAsync`/`cancelTimer`/`registerFdReader`/`scheduleSoon` と chronos/asyncdispatch 分岐そのものの専用ユニットテストは無し（全 async テストが間接行使）。**薄い** |
| pg_errors.nim | 217 | 専用テストファイル無し。アクセサ/述語（sqlState, constraintName, isUniqueViolation, isQueryCanceled）は test_protocol.nim:821-935 と e2e（test_cancel_e2e, test_e2e_transaction, test_e2e_misc, test_e2e_query）で行使。`parsePosition` の_overflow ガード (pg_errors.nim:168-178)、`where`/`internalQuery`/`internalPosition`、`isIntegrityConstraintViolation` 等は部分カバー。**専用テスト無し** |
| pg_bearssl.nim | 209 | test_ssl.nim の chronos 分岐のみが行使。asyncdispatch CI レグでは未実行（バックエンド依存）。**部分/依存** |
| pg_connection/type_lookup.nim | 107 | 専用テスト無し。e2e のユーザー定義型（test_e2e_types）経由の間接行使のみ |
| pg_connection/cache.nim | 100 | 専用テスト無し。e2e の prepared statement 経由の間接行使のみ |
| pg_bytes.nim | 120 | 専用ファイル無し。ただしバイトヘルパーは test_protocol.nim でテスト済（encodeInt16/32 等） |
| pg_client.nim / pg_connection.nim / pg_types.nim | 79 / 54 / 160 | 再エクスポートハブ（ロジック無し、テスト対象外は妥当） |

---

## 3. エラーパステストの傾向

計測: `expect(` / `except ` / `try:` の出現数 + エラー系キーワード
(malformed/truncat/invalid/timeout/cancel/disconnect/overflow/dropped/premature/stall/corrupt/out of range) のヒット数。

### エラーパス重視（malformed input・例外・キャンセル・タイムアウトを積極テスト）
- test_pool.nim — except 65 / キーワード 317。acquire タイムアウト、プールクローズ、二重 release、接続切断など網羅。
- test_network_failure.nim — except 44。障害経路専用（切断・malformed・ストール）。
- test_ssl.nim — except 52 / try 58。ハンドシェイク失敗・証明書検証失敗。
- test_protocol.nim — expect 41。malformed メッセージ解析。
- test_protocol_fuzz.nim — except 17。ファジングで malformed input 耐性。
- test_replication.nim — expect 33。不正 LSN / pgoutput デコード例外。
- test_types.nim — expect 31 / except 54。型デコードの不正入力。
- test_e2e_transaction.nim — except 44。デッドロック(40P01)/unique違反(23505)/cancel。
- test_session_attrs.nim — except 15。test_e2e_listen.nim(19)/test_e2e_copy.nim(23)/test_e2e_convenience.nim(19) も多め。
- test_dsn.nim — `expect` は 0 だがキーワード 49。不正 DSN を `check`/`expect` で多数検証（invalid input 重視）。

### happy path 中心
- test_async_backend.nim — 36行、回帰特化（expect ValueError 1 のみ）。
- test_sql.nim — except 0 / キーワード 0。SQL 生成の正常系。
- test_e2e_types.nim — except 0 / キーワード 0。型ラウンドトリップ正常系。
- test_rowdata.nim / test_saslprep.nim / test_keepalive.nim / test_e2e_arrays.nim / test_e2e_query.nim — 正常系中心。
- test_auth.nim — except 0 だがキーワード 14（暗号の境界・異入力テストは `check` ベース）。

総傾向: **プロトコル/型/プール/ネットワーク/SSL の各レイヤはエラーパス（malformed・例外・キャンセル・タイムアウト）を厚くテスト**。
一方 **SQL 生成・型ラウンドトリップ・sockopt・async バックエンド抽象は happy path 中心**。
e2e 系はテストにより差が大きく、transaction/listen/copy/convenience はエラーパス厚め、types/arrays/query/connection は正常系中心。

---

## 4. 規約

### 4a. 明文規約の有無
- **AGENTS.md: 無し**（リポジトリ内に存在しない。`**/AGENTS.md` glob 0件）。
- **CLAUDE.md: 無し**（`**/CLAUDE.md` glob 0件）。
- `.claude/` の中身は `settings.local.json`（Bash/Read の権限許可リストのみ）と `worktrees/`。規約文書は無し。
- 利用者向け契約は README.md に記載（再接続ポリシー README.md:109-113、async バックエンド README.md:115-、エラー契約は pg_errors.nim モジュール doc）。
- `reviews/` に 15 ファイル（個別レビュー 14 件 + `RELEASE_0.4.0_TODO.md`）:
  buffer_io, pg_advisory_lock, pg_copy, pg_pool, pg_protocol, pg_replication,
  rest, review_decoding, review_dsn, review_encoding, REVIEW_largeobject,
  review_pg_sql, REVIEW_ranges, review-transaction, RELEASE_0.4.0_TODO。

### 4b. lint / format 設定（nph）
- `.github/workflows/nph.yml`: `arnetheduck/nph-action@v1`。PR の `async_postgres.nim*` /
  `async_postgres/**` / `tests/**` 変更時に起動。`version: latest`, `options: "./"`,
  `fail: true`, `suggest: true`。→ **nph = Nim formatter。CI でフォーマットを強制**。
- **nph の設定ファイルはリポジトリに無い**（`.nph`/`nph.json`/`nim.cfg`/`.editorconfig` 全て無し、glob/find 0件）。→ **nph デフォルトスタイル**を適用。
- 補足: `.github/workflows/test.yml` は Nim 2.2.4/stable/devel マトリクス、docker-compose で実 PG 起動、`tests/gen_certs.sh` で証明書生成、`nimble test`（両バックエンド）、examples を両バックエンドでコンパイル、`nim doc` 生成。

### 4c. 帰納した暗黙の慣習（観測パスつき）

**エラー型の使い方（例外階層）** — `async_postgres/pg_errors.nim`
- 全例外は `PgError`(CatchableError 派生) の単一階層 (pg_errors.nim:34)。呼び出し側は
  `except PgError` 一節で全 pg 固有失敗を捕捉可能。
- `PgProtocolError` は `PgConnectionError` のサブタイプ (pg_errors.nim:50) — プロトコル違反は
  接続を道連れに teardown するため。
- `PgTimeoutError` も `PgConnectionError` のサブタイプ (pg_errors.nim:84) — タイムアウトは
  CancelRequest を送り接続を csClosed にするため、再接続ループに捕捉させる意図。
- `PgStateError` は意図的に `PgConnectionError` の**兄弟**（サブタイプではない, pg_errors.nim:57）—
  並行使用などのプログラミングエラーで、再接続が無意味なため reconnect ループから除外。
- 非推奨エイリアスは `{.deprecated.}` で維持 (pg_errors.nim:54 `ProtocolError`)。
- SQLSTATE 定数 `SqlState*` (pg_errors.nim:116-125) と述語 `is*Violation`/`isQueryCanceled`
  (pg_errors.nim:191-217)、`PgQueryError` フィールドアクセサ (pg_errors.nim:136-187)。
- 設計意図をモジュール先頭 doc で詳述 (pg_errors.nim:1-26)。

**async パターン**
- `{.async.}` proc を使用。バックエンド抽象は `async_backend.nim` に集約。
- `hasChronos` / `hasAsyncDispatch` / `hasTls` の const ガード (async_backend.nim:11-19) で
  `when hasChronos: ... elif hasAsyncDispatch: ...` 分岐（async_backend.nim:21,78）。
  両バックエンドで同一 API 表面（`wait`/`sleepMsAsync`/`cancelTimer`/`registerFdReader`/`scheduleSoon`）を提供。
- コールバック生成は `declareAsyncCallback` マクロ（tests/test_async_backend.nim:10）。
- モック/テストもバックエンド非依存を維持（mock_pg_server.nim:9-11, test_ssl.nim の両分岐）。

**doc コメントのスタイル**
- `##` RST 形式。モジュール先頭に設計意図をまとめる（pg_errors.nim:1-26, pg_connection.nim:1-46,
  async_backend.nim:1-5, mock_pg_server.nim:1-11, e2e_common.nim:1-2）。
- 型・proc・フィールドに `##` doc。コード参照は二重バッククォート `` ``PgError`` ``。
- フィールド inline doc（`tracer*: PgTracer ## Optional tracer ...` pg_pool.nim:49, types.nim:182）。

**命名規則**
- ファイル: snake_case + `pg_` プレフィックス（pg_pool.nim, pg_connection/ssl.nim）。
- 型: PascalCase（PgConnection, ConnConfig, TraceContext, PgTracer）。
- proc/フィールド: camelCase（advisoryLock, fillRecvBuf, onQueryStart）。
- enum 値: 種別プレフィックス付き camelCase（sslDisable, csReady, tcdIn, skQuery, ckTxRollback, tcsTlsReader）。
- 定数: PascalCase（SqlStateUniqueViolation pg_errors.nim:118）。

**モジュール構成**
- 薄く再エクスポートするハブ（pg_connection.nim:48-54, pg_client.nim, pg_types.nim）+
  実装はサブディレクトリ（pg_connection/, pg_client/, pg_types/）に分割。

**tracing フックの使い方** — `async_postgres/pg_connection/types.nim`
- `PgTracer` は任意コールバックの `ref object` (types.nim:608)。nil コールバックはゼロオーバーヘッドでスキップ。
- Start フックは `TraceContext`(=RootRef, types.nim:396) を返し、対応する End フックへ相関のために渡す。
- 全フックは `{.gcsafe, raises: [].}`（types.nim:615-679）。
- 支援テンプレート `withConnTracing`(types.nim:923) / `withTracing`(types.nim:944) が body を
  try/except で包み、例外時は End フックに `err` を渡して再送出。
- 飲み込まれるエラーの可視化用 advisory フック（onPoolCloseError, onTransportCloseError,
  onLeakedSessionLocks, onCleanupSkipped, onPoolDoubleRelease）— pg_pool.nim:255-260,717-741。
- tracer は `ConnConfig.tracer`(types.nim:182) / `PoolConfig.tracer`(pg_pool.nim:49) で注入。

**テストの慣習**
- `std/unittest` の suite/test。async テストは `proc t() {.async.} = ...; waitFor t()` でラップ。
- 私有ヘルパー到達には `import ... {.all.}` + `privateAccess`（test_advisory_lock.nim:3-8, test_replication.nim:4-7）。
- e2e 共有ヘルパー `e2e_common.nim`、モック `mock_pg_server.nim`。
  ただし test_advisory_lock / test_largeobject / test_tracing は e2e_common を使わず config をローカル再定義（不整合、§1a 参照）。
- test_ssl / test_replication はモック/メッセージビルダーをファイル内で独自定義（test_ssl.nim:20-23, test_replication.nim:9-13）。

---

## 5. 未確認事項
- 各テストの「テスト対象モジュール」は import 行・ヘルパー使用・冒頭コードからの推定。
  全テスト本文を精読した網羅的マッピングではない（大規模ファイル test_types 9107行、
  test_e2e_transaction 3537行、test_pool 3639行 は冒頭と計測中心）。
- pg_bearssl.nim の asyncdispatch レグ未実行は CI 設定からの推定。実 CI ログは未確認。
- examples/（14サンプル）の内容精査は未実施（test.yml で両バックエンドコンパイルされる事実は確認）。
- `reviews/` 各レビューの中身（個別指摘）は本タスク範囲外として未精読（存在と一覧のみ確認）。
- nph の実際のフォーマット結果（差分）は未検証。設定ファイル不在＝デフォルト、という事実のみ。
