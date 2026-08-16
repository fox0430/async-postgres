# 地図（統合索引）

詳細は以下を参照:
- モジュール分割・依存グラフ: `.audit/map_modules.md`
- エントリポイント・データフロー: `.audit/map_dataflow.md`
- テスト分布・規約: `.audit/map_tests_conventions.md`
- 外部依存・ビルド/CI: `.audit/map_deps_build.md`

## A. モジュール分割（要点）
ソース41ファイル + ハブ、約26,516行。グループ構成:
- **L0 基底**: pg_errors (217), pg_bytes (120)
- **L1 プロトコル**: pg_protocol (1374) — ワイヤプロトコル encode/decode
- **L2 型**: pg_types/{core(1096), encoding(1940), decoding(935), accessors(1860), ranges(1223), array(183), user_types(514)} + pg_types.nim ハブ
- **L3 接続**: pg_connection/{types(963), dsn(719), buffer_io(780), ssl(612), cache(100), simple_query(447), lifecycle(680), notify(429), type_lookup(107)} + async_backend(354), pg_auth(359), pg_saslprep(156), pg_bearssl(209)
- **L4 クライアント**: pg_client/{core(622), exec(172), query(459), prepared(192), copy(632), transaction(860), transaction_helpers(224), pipeline(726), cursor(322), direct(646)}
- **L5 高レベル**: pg_pool(2310), pg_pool_cluster(395), pg_largeobject(485), pg_advisory_lock(689), pg_sql(586), pg_replication(1387)
- ハブ（re-export専用、実装なし）: async_postgres.nim, pg_client.nim, pg_connection.nim, pg_types.nim

## B. 依存グラフ（要点）
- **循環依存: なし（DAG）／レイヤ違反: なし**（良好な点）
- fan-in 上位: pg_protocol **28**, async_backend **26**, pg_typesハブ **21**, pg_connectionハブ **17**, pg_errors **13**, pg_client/core **10**, pg_connection/types **9**, pg_types/core **7**
- 特記: pg_bearssl→pg_typesフルハブ結合、pg_connection/types(963行)が pg_auth/pg_types/pg_protocol を一括依存、pg_sql→pg_pool(L5) 片方向依存

## C. エントリポイントと制御フロー（要点）
- 公開API: connect (lifecycle:678), query/exec (query:177/exec:69), queryDirect/execDirect (direct:463/580), simpleQuery/simpleExec (simple_query:298/265), sql"" (pg_sql:213), withTransaction系 (transaction:362〜), COPY (copy:169〜), LISTEN/NOTIFY (notify:357/398), pool acquire/release (pg_pool:609/1077), replication (pg_replication:541/1141/1325)
- **受信単一チェーン**: socket → fillRecvBuf (buffer_io:183) → nextMessage (:276) → parseBackendMessage (pg_protocol:1166) → 型別パーサ群。受信駆動は `pumpUntilReady` テンプレート3種 (buffer_io:376/417/450) が共通。

## D. データフロー（要点）
- 送信: 利用者値 → toPgParam/toPgBinaryParam (encoding) → addParse/addBind/addBindRaw (pg_protocol:562/579/612) → conn.sendBuf → sendMsg (buffer_io:544)
- 受信: socket → recvBuf → parse → RowData フラットバッファ → QueryResult → Row ビュー → **アクセサ呼び出し時に遅延デコード** (decoding)
- FS接触（クライアント側）: readPemFileParam (dsn:214, sslcert/sslkey/sslrootcert 読込・sslkey権限検査), writeTempPem (ssl:361, asyncdispatch一時PEM)
- loImport/loExport (pg_largeobject:209/218) は**サーバ側FS**操作

## E. テスト分布（要点）
- 全37 test_*.nim が all_tests.nim に含まれ CI で走る（孤立テストなし）。総量42,157行。
- e2e16 / モック9 / TLSモック1 / 純粋unit11。
- 厚い: protocol, types, pool, ssl, auth, network-failure, fuzz。
- **薄い/無い**: async_backend(354行に対し専用36行), pg_errors(専用なし), pg_bearssl(chronosレグのみ), type_lookup(107), cache(100)。
- エラーパス: protocol/type/pool/network/ssl は厚い。SQL生成・型roundtrip・sockopt・async抽象は happy path 中心。

## F. 外部依存（要点）
- nimble宣言（全て `>=` 下限のみ・上限なし）: nim 2.2.4, nimcrypto 0.7.3 (SCRAM/MD5/burnMem), checksums 0.2.2 (MD5), unicodedb 0.13.2 + normalize 0.9.0 (SASLprep)
- **README言及あるが nimble 未宣言**: chronos >= 4.4.0, nim-bearssl >= 0.2.11。CI は `nimble install chronos -y` で無固定。
- MD5 は nimcrypto にもあるのに checksums を使う軽微な重複。

## G. 規約（要点）
- **AGENTS.md / CLAUDE.md なし**。nph フォーマッタを CI で強制（設定ファイルなし・デフォルトスタイル）。
- 暗黙の慣習: 単一例外階層 PgError (pg_errors)、async_backend の hasChronos/hasAsyncDispatch/hasTls 分岐、`##` RST doc、pg_ snake_case ファイル/PascalCase 型/camelCase proc、re-export ハブ構成、PgTracer nil スキップ型フック。

## 構造的所見（地図由来、候補）
1. 依存の上限未固定 + chronos/bearssl 未宣言（再現性・互換性リスク）
2. CHANGELOG なし、v0.3.0 から555コミット先行（semver 運用の不透明さ）
3. async_backend の被参照26に対しテスト36行（ブラスト半径とカバレッジの不均衡）
4. fan-in 集中（pg_protocol 28, async_backend 26）— 基盤変更の波及大
