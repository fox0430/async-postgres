# モジュール分割と依存グラフ — async_postgres

対象: `async_postgres.nim` (ルート) + `async_postgres/` 配下。計 **43 ソースモジュール / 26,516 行**。
`tests/`, `examples/` は依存グラフの集計から除外（これらはライブラリの消費者であり、モジュールグラフの一部ではない）。

調査方法:
- 行数: `wc -l`（プロファイル済み数値と一致を確認）。
- import: `rg '^\s*(import|from|include)\s'` で単一行importを全抽出。複数行import（`import` 単独行）は
  `async_postgres.nim:131`, `pg_connection.nim:49`, `pg_client.nim:62` の3件（全てre-exportハブ）のみで、全文を読んで捕捉。
  `include` 文はソースに存在しない（コメント内の1一致 `pg_types/array.nim:21` のみ）。
- 被参照数(fan-in): 「そのモジュールをimportしている**別ソースファイル数**」で集計。ハブとサブモジュールは別ノードとして扱う。
  `pg_bearssl` のimportは chronos バックエンド時のみ有効（条件付き）だが、import文の存在をもって1件と数える。

---

## 1. モジュール一覧表

### L0 — 基盤（内部依存なし / std・外部pkgのみ）

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres/async_backend.nim` | 非同期バックエンド抽象（asyncdispatch / chronos の切換え）。`Future`/`async`/`await` 等の統一エイリアス | 354 | macro `declareAsyncCallback*`; template `makeAsyncSinkByteCallback*`, `makeAsyncSeqByteCallback*`; proc `remainingDeadlineDuration*` |
| `async_postgres/pg_errors.nim` | 例外階層。全例外は `PgError` 派生（`PgConnectionError`/`PgStateError`/`PgTimeoutError`/`PgQueryError`/`PgProtocolError` 等） | 217 | type `ErrorField*`, `PgError*` 以下の例外ref型群（import無し・純粋な型定義リーフ） |
| `async_postgres/pg_saslprep.nim` | RFC 4013 SASLprep（SCRAM用パスワード正規化）。pkg/normalize, unicodedb を使用 | 156 | proc `saslprep*` |

### L1 — バイト列・ワイヤプロトコル・認証

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres/pg_bytes.nim` | ビッグエンディアンの低レベルバイト操作（encode/decode, bulk copy用）。`pg_errors` のみに依存し循環を断つ | 120 | template `writeBE16/32/64*`, `writeBytesAt*`, `appendBytes*`; func `toBe16/32/64*`, `fromBE16/32/64*`, `decodeFloat32/64BE*`; proc `readString*`, `readBytes*` |
| `async_postgres/pg_protocol.nim` | ワイヤプロトコルv3のメッセージ定義・エンコード/デコード。Frontend/Backendメッセージ、RowData/Row、DataRow解析、COPY/レプリケーション補助 | 1374 | type `FrontendMessageKind*`, `BackendMessageKind*`, `TransactionStatus*`, `FieldDescription*`, `BackendMessage*`, `RowData*`, `Row*`; proc `encodeStartup*`, `encodeQuery*`, `addParse/Bind/Describe/Execute/Sync*`, `parseBackendMessage*`, `parseDataRowInto*`, `buildResultFormats*`, `formatError*`, `encodeStandbyStatusUpdate*` 他多数 |
| `async_postgres/pg_auth.nim` | 認証（MD5, SCRAM-SHA-256/-PLUS）。channel binding (tls-server-end-point) | 359 | template `burnStr*`; proc `md5AuthHash*`, `scramClientFirstMessage*`, `scramClientFinalMessage*`, `scramVerifyServerFinal*`, `computeTlsServerEndpoint*` |

### L2 — 型システム（pg_types/）

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres/pg_types/core.nim` | 中核PG型（PgNumeric/PgMoney/PgUuid/PgInterval/PgTime/PgInet/幾何型 等）と `PgParam`/`CommandResult`、テキストparse補助 | 1096 | type `PgUuid*`, `PgMoney*`, `PgNumeric*`, `PgInterval*`, `PgTime*`, `PgInet*`, `PgPoint*`…`PgCircle*`, `PgParam*`, `PgParamInline*`, `ResultFormat*`, `CommandResult*`; proc `parsePgNumeric*`, `parsePgMoney*`, `pgParseInt32*` 他（`export pg_errors`） |
| `async_postgres/pg_types/array.nim` | N次元配列 `PgArray[T]` の形状検証・構築 | 183 | type `PgArray*[T]`; proc `pgArray*`(多重定義), `validatePgArrayShape*`, `expectedElemCount*`, `validate*`, `isEmpty*`, `ndim*` |
| `async_postgres/pg_types/decoding.nim` | サーバ由来データのデコード（binary/text）。numeric, timestamp, inet, hstore, array, composite, tsvector/tsquery | 935 | proc `decodeNumericBinary*`, `decodeBinaryTimestamp*`, `decodeBinaryArray*`, `decodeBinaryComposite*`, `parseTimestampText*`, `parseIntervalText*`, `parseHstoreText*`, `decodeInetBinary*` 他（`export pg_bytes, array`） |
| `async_postgres/pg_types/encoding.nim` | パラメータエンコード。`toPgParam`/`toPgBinaryParam`/`toPgParamInline` の大量多重定義、配列/几何/JSON、ゼロアロケーション `addParseDirect`/`addBindDirect` | 1940 | proc `toPgParam*`(多数), `toPgBinaryParam*`(多数), `toPgParamInline*`, `encodeBinaryArray*`, `encodeNumericBinary*`, `coerceBinaryParam*`; macro `pgParams*`, `addParseDirect*`, `addBindDirect*`（`export pg_bytes, array`） |
| `async_postgres/pg_types/accessors.nim` | 行アクセサ。`Row.get*`/`getXxx`（int/float/text/uuid/几何/JSON…）、配列・ND配列取得、`decodePgArrayElement*`、nameAccessor機構 | 1860 | proc `getStr*`, `getInt*`, `getInt64*`, `getFloat*`, `getBool*`, `getJson*`, `getArrayND*`, `get*`(typedesc多重定義多数), `columnIndex*`; template `nameAccessor*`, `optAccessor*`; converter `toRow*` |
| `async_postgres/pg_types/user_types.nim` | ユーザ定義型（enum/composite/domain）のmacro生成と取得 | 514 | macro `pgEnum*`, `pgComposite*`, `pgDomain*`; proc `getEnum*`, `getComposite*`, `getDomain*`, `parseCompositeText*`, `encodeBinaryComposite*` |
| `async_postgres/pg_types/ranges.nim` | range / multirange 型。binary/textデコード、`toPgParam`/`toPgBinaryParam`、`get*` | 1223 | proc `rangeOf*`, `rangeFrom*`, `unboundedRange*`, `parseRangeText*`, `decodeInt4RangeBinary*`, `toMultirange*`, `toPgParam*`(PgRange/PgMultirange多数), `get*`(range系) |
| `async_postgres/pg_types.nim` | **re-exportハブ**。全pg_typesサブモジュールを `export` し、name-basedアクセサ（`accessorPair`/`arrayPair`/`rangeFamily` 等macro）を一括生成 | 160 | macro `accessorPair`, `arrayPair`, `elemOptPair`, `rangeFamily`（私有）; proc `get*[T](Option[T])`; `export core, array, encoding, decoding, accessors, user_types, ranges` |

### L2.5 — SSL補助

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres/pg_bearssl.nim` | BearSSL X509処理（SCRAM-SHA-256-PLUS channel binding用）。葉証明書DER捕捉、信頼アンカparse。chronos時のみ使用 | 209 | type `X509CertCaptureContext*`, `TrustAnchorResult*`（chronos分岐内） |

### L3 — 接続（pg_connection/）

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres/pg_connection/types.nim` | 接続層の共有土台。`PgConnection`/`ConnConfig`/状態enum、トレーシングデータ型・`PgTracer`、tracing template | 963 | type `PgConnState*`, `SslMode*`, `AuthMethod*`, `ConnConfig*`, `PgConnection*`, `QueryResult*`, `CopyResult*`, `CachedStmt*`, `PgTracer*`, `Notification*`; template `withConnTracing*`, `withTracing*`; proc `newPgQueryError*` 他 |
| `async_postgres/pg_connection/dsn.nim` | DSN解析（URI + keyword=value）。`initConnConfig`/`parseDsn`、各パラメータparse | 719 | proc `parseDsn*`, `initConnConfig*`, `parseUriDsn*`, `parseKeyValueDsn*`, `applyParam*`, `parseSslMode*`, `parseTargetSessionAttrs*` 他 |
| `async_postgres/pg_connection/buffer_io.nim` | トランスポートのバッファリングとメッセージI/O。recv/send、`nextMessage`/`recvMessage`、keepalive、通知dispatch、`closeTransport` | 780 | type `RecvWatch*`; proc `fillRecvBuf*`, `nextMessage*`, `recvMessage*`, `sendMsg*`, `closeTransport*`, `isConnected*`, `socketHasFin*`, `dispatchNotification*`, `getHosts*`; template `pumpUntilReady*` |
| `async_postgres/pg_connection/ssl.nim` | TLS/SSLネゴシエーション（postgres SSLRequest + direct SSL/ALPN）。chronos+BearSSL と asyncdispatch+OpenSSL | 612 | proc `negotiateSSL*`, `validateDirectSslCompatible*`, `sniName*` |
| `async_postgres/pg_connection/cache.nim` | サーバprepared statementのクライアント側LRUキャッシュ | 100 | proc `nextStmtName*`, `lookupStmtCache*`, `addStmtCache*`, `evictStmtCache*`, `removeStmtCache*`, `flushPendingStmtCloses*`, `clearStmtCache*` |
| `async_postgres/pg_connection/simple_query.nim` | Simple Queryプロトコル。`simpleQuery`/`simpleExec`/`ping`、`cancel`、`quoteIdentifier`、`checkSessionAttrs` | 447 | proc `simpleQuery*`, `simpleExec*`, `ping*`, `cancel*`, `invalidateOnTimeout*`, `quoteIdentifier*`, `checkSessionAttrs*`; type `QueryResult` のヘルパ群; template `awaitOrInvalidate*` |
| `async_postgres/pg_connection/lifecycle.nim` | 接続ライフサイクル。`connect`/`connectToHost`/`close`、ホストフェイルオーバ、SCRAM/require_auth補助 | 680 | proc `connectToHost*`, `enforceAuthAllowed*`, `filterSaslByRequireAuth*`, `selectScramMechanism*`（+ `connect`/`close` 等） |
| `async_postgres/pg_connection/notify.nim` | LISTEN/NOTIFY。`listen`/`unlisten`/`waitNotification`、バックグラウンドpump、`reconnectInPlace` | 429 | proc `listen*`, `unlisten*`, `waitNotification*`, `onNotify*`, `onListenError*`, `startListening*`, `stopListening*`, `listenPump*`, `reconnectInPlace*` |
| `async_postgres/pg_connection/type_lookup.nim` | 拡張型OID解決（`to_regtype`）。hstore/citext/vector 等のOIDを1往復で取得 | 107 | proc `lookupTypeOids*` |
| `async_postgres/pg_connection.nim` | **re-exportハブ**。全pg_connectionサブモジュール + `pg_errors` を `export` | 54 | `export pg_errors, types, dsn, buffer_io, ssl, cache, simple_query, lifecycle, notify, type_lookup` |

### L4 — クライアント / クエリ実行（pg_client/）

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres/pg_client/core.nim` | client層の共有土台。トランザクションオプション、inline-paramエンコーダ、extended-queryのrecv-loop template | 622 | type `IsolationLevel*`, `AccessMode*`, `DeferrableMode*`, `TransactionOptions*`, `RetryOptions*`; proc `buildBeginSql*`, `isRetryableTxError*`, `backoffDelayMs*`, `extractParams*`, `flattenInline*`; template `sendExtendedQuery*`, `queryRecvLoop*`, `execRecvLoop*` |
| `async_postgres/pg_client/exec.nim` | `exec`（extended query・結果行無視・コマンドタグ返却） | 172 | proc `exec*`(seq[PgParam]/openArray[PgParamInline]), `notify*` |
| `async_postgres/pg_client/query.nim` | `query` と結果形状ヘルパ（`queryRow`/`queryValue`/`queryExists`/`queryColumn`）、行ストリーム `queryEach` | 459 | proc `query*`, `queryEach*`, `queryRow*`, `queryRowOpt*`, `queryValue*`, `queryValueOpt*`, `queryValueOrDefault*`, `queryExists*`, `queryColumn*` |
| `async_postgres/pg_client/prepared.nim` | 名前付きprepared statement（`prepare`/`execute`/`close`） | 192 | proc `prepare*`, `execute*`, `close*`; type `PreparedStatement` ヘルパ |
| `async_postgres/pg_client/copy.nim` | COPY IN/OUT（simple query）、ストリーム版 `copyInStream`/`copyOutStream` | 632 | proc `copyIn*`(多重定義), `copyInStream*`, `copyOut*`, `copyOutStream*` |
| `async_postgres/pg_client/transaction.nim` | トランザクション/セーブポイントmacro。`withTransaction` 系・deadline/retry変種 | 860 | macro `withTransaction*`, `withTransactionRetry*`, `withSavepoint*`, `withTransactionDeadline*`, `withTransactionRetryDeadline*`, `withSavepointDeadline*` |
| `async_postgres/pg_client/transaction_helpers.nim` | パイプライン単一文トランザクション（BEGIN+SQL+COMMITを1 Syncで） | 224 | proc `execInTransaction*`, `queryInTransaction*` |
| `async_postgres/pg_client/pipeline.nim` | パイプラインバッチ実行（`addExec`/`addQuery`、単一Sync `execute` とエラー分離 `executeIsolated`） | 726 | type `PipelineOp*`, `PipelineResult*`, `Pipeline*`, `IsolatedPipelineResults*`; proc `newPipeline*`, `addExec*`, `addQuery*`, `execute*`, `executeIsolated*`, `reset*` |
| `async_postgres/pg_client/cursor.nim` | サーバ側portalカーソル（`openCursor`/`fetchNext`/`close`/`withCursor`） | 322 | proc `openCursor*`, `fetchNext*`, `close*`; template `withCursor*`; type `Cursor` |
| `async_postgres/pg_client/direct.nim` | ゼロアロケーション `queryDirect`/`execDirect` コンパイル時macro | 646 | macro `queryDirect*`, `execDirect*` |
| `async_postgres/pg_client.nim` | **re-exportハブ**。全pg_clientサブモジュールを `export`（coreは安定公開面のみ選別re-export） | 79 | `export core.{IsolationLevel, AccessMode, …, backoffDelayMs}`; `export exec, query, prepared, copy, transaction, transaction_helpers, pipeline, cursor, direct` |

### L5 — 高レベル機能

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres/pg_pool.nim` | コネクションプール。ヘルスチェック、メンテ、acquire/release、プール経由query/exec、`withTransaction` 系macro | 2310 | type `PoolConfig*`, `PoolMetrics*`, `PooledConnHandle*`, `PgPool*`; proc `newPool*`, `acquire*`, `acquireHandle*`, `release*`, `exec*`, `query*`, `queryValue*`, `close*`; template `withConnection*`, `withPipeline*`; macro `withTransaction*`, `withTransactionRetry*`, `withTransactionDeadline*` |
| `async_postgres/pg_pool_cluster.nim` | 読み取りレプリカプールクラスタ。クエリルーティング、フォールバック | 395 | type `PgPoolCluster*`, `ReplicaFallback*`; proc `newPoolCluster*`, `readConnection*`, `writeConnection*`, `close*`; template `withReadConnection*`, `withWriteConnection*`; macro `withTransaction*` 系 |
| `async_postgres/pg_replication.nim` | 論理/物理レプリケーション。ストリーミング、pgoutputデコーダ、WAL/LSN | 1387 | type `Lsn*`, `ReplicationMessage*`, `XLogData*`, `PgOutputMessage*`, `RelationInfo*`, `InsertMessage*`…; proc `connectReplication*`, `startReplication*`, `stopReplication*`, `startPhysicalReplication*`, `parsePgOutputMessage*`, `identifySystem*`, `createReplicationSlot*`, `sendStandbyStatus*` |
| `async_postgres/pg_largeobject.nim` | Large Object API。ストリーミング読書、deadline変種 | 485 | proc `loSeek*`, `loTell*`, `loTruncate*`, `loImport*`, `loExport*`, `loReadAll*`, `loWriteAll*`, `loSize*`, `loReadStream*`, `loWriteStream*`; template `withLargeObject*` |
| `async_postgres/pg_advisory_lock.nim` | アドバイザリロック（session/transaction, exclusive/shared）。`withAdvisoryLock` 系macro | 689 | proc `advisoryLock*`, `advisoryTryLock*`, `advisoryUnlock*`, `advisoryLockShared*`, `advisoryLockXact*` 他; macro/template `withAdvisoryLock*`, `withAdvisoryLockShared*`, `withAdvisoryLockXact*` |
| `async_postgres/pg_sql.nim` | SQLヘルパ。`?`プレースホルダ変換、`sql"..."` リテラルmacro | 586 | type `SqlQuery*`; func `sqlParams*`; macro `sql*` |

### L6 — エントリ

| パス | 責務 | 行数 | 主要公開要素 |
|---|---|---|---|
| `async_postgres.nim` | **公開エントリ / 顶层re-exportハブ**。L0–L5の主要12モジュールを `export` | 139 | `export async_backend, pg_protocol, pg_auth, pg_types, pg_connection, pg_client, pg_pool, pg_pool_cluster, pg_largeobject, pg_advisory_lock, pg_sql, pg_replication` |

---

## 2. 依存グラフ概要（グループ分け）

依存は全て「上位 → 下位」の単方向。層を下るほどfan-inが高い。

```
L6  async_postgres.nim
      └─> L5 {pg_pool, pg_pool_cluster, pg_replication, pg_largeobject, pg_advisory_lock, pg_sql}
          + L1..L4 のハブ (pg_protocol, pg_auth, pg_types, pg_connection, pg_client)

L5  高レベル機能
      pg_pool          -> async_backend, pg_protocol, pg_connection, pg_types, pg_client
      pg_pool_cluster  -> async_backend, pg_protocol, pg_connection, pg_types, pg_pool, pg_client
      pg_replication   -> async_backend, pg_protocol, pg_connection, pg_types
      pg_largeobject   -> async_backend, pg_types, pg_protocol, pg_connection, pg_client
      pg_advisory_lock -> async_backend, pg_protocol, pg_types, pg_connection, pg_client
      pg_sql           -> async_backend, pg_types, pg_connection, pg_client, pg_pool

L4  pg_client/*  (全て core を基底)
      core             -> async_backend, pg_protocol, pg_connection, pg_types
      exec/query/prepared/copy/transaction_helpers/pipeline/cursor/direct
                       -> ../[async_backend, pg_protocol, pg_connection, pg_types] + ./core
      transaction      -> ../[async_backend, pg_protocol, pg_connection] + ./core   (pg_types は非import)
      pg_client(ハブ)  -> 全サブモジュールを re-export

L3  pg_connection/*  (全て types を基底)
      types            -> async_backend, pg_auth, pg_errors, pg_protocol, pg_types, pg_bearssl(chronos)
      dsn              -> async_backend, pg_errors, types
      buffer_io        -> async_backend, pg_errors, pg_protocol, types
      cache            -> pg_protocol, types
      ssl              -> async_backend, pg_errors, pg_protocol, pg_types, types, buffer_io, pg_bearssl(chronos)
      simple_query     -> async_backend, pg_errors, pg_protocol, pg_types, types, buffer_io
      lifecycle        -> async_backend, pg_errors, pg_protocol, pg_auth, types, buffer_io, ssl, simple_query, dsn
      notify           -> async_backend, pg_errors, pg_protocol, types, buffer_io, cache, simple_query, lifecycle, pg_bearssl(chronos)
      type_lookup      -> async_backend, pg_errors, pg_types, types, simple_query
      pg_connection(ハブ) -> pg_errors + 全サブモジュールを re-export

L2.5 pg_bearssl       -> async_backend, pg_types   (chronos時のみ接続層から参照される)

L2  pg_types/*  (全て core を基底、相互はDAG)
      core             -> pg_errors, pg_bytes                 (export pg_errors)
      array            -> core
      decoding         -> pg_bytes, core, array               (export pg_bytes, array)
      encoding         -> pg_bytes, pg_protocol, core, array  (export pg_bytes, array)
      accessors        -> pg_protocol, core, decoding, encoding
      user_types       -> pg_protocol, core, decoding, encoding, accessors
      ranges           -> pg_protocol, core, encoding, decoding, accessors
      pg_types(ハブ)   -> pg_protocol + 全サブモジュールを re-export

L1  pg_protocol       -> pg_bytes, pg_errors
      pg_auth          -> pg_errors, pg_saslprep   (+ pkg/checksums, pkg/nimcrypto)
      pg_bytes         -> pg_errors

L0  pg_errors / async_backend / pg_saslprep   (内部依存なし)
```

---

## 3. 循環依存・レイヤ違反の所見

### 3.1 循環依存: **検出されず（グラフはDAG）**

- 全43モジュールのimportを抽出し、各グループの基底モジュール（`pg_errors`, `pg_bytes`, `pg_protocol`,
  `pg_types/core`, `pg_connection/types`, `pg_client/core`）が**同グループの他サブモジュールや上位層を
  importしていない**ことを確認した。これにより各グループ内部は木状、グループ間も下位→上位の一方通行。
- グループ横断の閉路も無し。特に:
  - `pg_connection/types -> pg_types(ハブ) -> pg_types/* -> pg_protocol -> pg_bytes -> pg_errors` は一方向。
  - `pg_connection/* -> pg_bearssl -> pg_types` も一方向（`pg_types` 側は `pg_bearssl`/`pg_connection` をimportしない）。
  - `pg_sql -> pg_pool` があるが、`pg_pool` は `pg_sql` をimportしないため閉路にならない。
- 検証の前提: 複数行importは3ハブのみで全文確認済み、`include` 文は無し。よって見落としの経路は無いと判断。

### 3.2 レイヤ違反: **検出されず**

指定された典型例は共に発生していない:
- `pg_protocol` が `pg_client` をimport: **無し**。`pg_protocol.nim:3` は `import pg_bytes, pg_errors` のみ（下位のみ）。
- `pg_types` が `pg_connection` をimport: **無し**。`pg_types/*` のimportは `pg_errors`, `pg_bytes`, `pg_protocol` と
  同グループ内のみ。`pg_types.nim:3-4` は `import pg_protocol` + `import pg_types/[…]`。
- 下位層（L0/L1/L2）が上位層（L3接続/L4クライアント/L5高レベル）をimportする経路は確認されなかった。

### 3.3 特筆すべき結合（違反ではないが結合度が高い点）

- `async_postgres/pg_bearssl.nim:5` — 低レベルSSL補助である `pg_bearssl` が **`pg_types`（フルハブ）をimport**。
  実際には channel binding 用の型参照が主目的とみられるが、L2.5のモジュールがL2ハブ全体に依存するのは
  結合としてやや重い（chronosビルド時のみ有効）。
- `async_postgres/pg_connection/types.nim:13` — 接続層の土台 `types` が `pg_auth`, `pg_types`, `pg_protocol` を
  一括import（963行の肥大基底モジュール）。同 `:17` で `pg_bearssl`（chronos条件付き）。
- `async_postgres/pg_sql.nim:31` — SQLヘルパ `pg_sql` が `pg_pool`（L5）に依存。同層内の結合だが、
  「SQLリテラルmacro」が「プール」を知る構造は責務の観点で要注目（循環は無し）。
- `pg_client/transaction.nim:6` は `pg_client/*` で唯一 `pg_types` をimportしない（macroのみで完結）。

---

## 4. 被参照数（fan-in）上位10モジュール

集計単位: そのモジュールをimportする**別ソースファイル数**（43ソース + ルートの範囲。tests/examples除外）。

| 順位 | モジュール | 被参照数 | 主な参照元 |
|---|---|---|---|
| 1 | `pg_protocol` | **28** | ほぼ全層（L1除く全client/conn/typesサブモジュール + 高レベル + ハブ） |
| 2 | `async_backend` | **26** | 全client/connサブモジュール + 高レベル + bearssl（cache, pg_types/*, pg_errors等は非参照） |
| 3 | `pg_types` (ハブ) | **21** | 全高レベル + 全client(core除く各impl) + conn/{types,ssl,simple_query,type_lookup} + bearssl |
| 4 | `pg_connection` (ハブ) | **17** | 全高レベル + 全clientサブモジュール + ルート |
| 5 | `pg_errors` | **13** | pg_protocol, pg_auth, pg_bytes, connハブ + conn/{types,dsn,buffer_io,ssl,simple_query,lifecycle,notify,type_lookup}, pg_types/core |
| 6 | `pg_client/core` | **10** | pg_clientハブ + exec/query/prepared/copy/transaction/transaction_helpers/pipeline/cursor/direct |
| 7 | `pg_connection/types` | **9** | pg_connectionハブ + dsn/buffer_io/ssl/cache/simple_query/lifecycle/notify/type_lookup |
| 8 | `pg_types/core` | **7** | pg_typesハブ + array/encoding/decoding/accessors/user_types/ranges |
| 9 | `pg_client` (ハブ) | **6** | ルート + pg_pool/pg_pool_cluster/pg_largeobject/pg_advisory_lock/pg_sql |
| 10 | `pg_connection/buffer_io` | **5** | pg_connectionハブ + ssl/simple_query/lifecycle/notify |

（参考）次点（被参照数4）: `pg_bytes`, `pg_types/encoding`, `pg_types/decoding`, `pg_connection/simple_query`。

### 結合の要点
- **`pg_protocol` と `async_backend` が事実上の基盤**（fan-in 28/26）。これら2つの変更は全モジュールに波及する。
- ハブ（`pg_types`/`pg_connection`/`pg_client`）は re-export 専用で実装を持たないため、ハブ経由の依存は
  実体のあるサブモジュール（`pg_types/core`, `pg_connection/types`, `pg_client/core`）へ集約される。
  各グループの `core`/`types` が**真の結合ハブ**（fan-in 6–10）。
