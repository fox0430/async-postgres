# エントリポイント / 制御フロー / データフロー マップ

対象: `async_postgres` (Nim 製非同期 PostgreSQL クライアントライブラリ v0.3.0)
調査種別: 読み取り専用監査。行番号は調査時 (HEAD) のもの。
パスはすべて `/home/fox/git/async-postgres/` 起点の相対で記載。

ライブラリであるため「エントリポイント」とは:
1. 利用者が呼ぶ公開 API (proc/macro/template)
2. PostgreSQL サーバからバイト列が入る経路 (受信パースチェーン)

---

## 1. 公開 API エントリポイント一覧

公開シンボルの集約ハブ: `async_postgres.nim:131-139` (import/export)。
`pg_connection.nim` と `pg_client.nim` はサブモジュールの再エクスポートハブ。

### 1.1 接続ライフサイクル

| proc | 定義ファイル:行 | 用途 |
|---|---|---|
| `connect*(dsn: string)` | async_postgres/pg_connection/lifecycle.nim:678 | DSN 文字列から接続。`connect(parseDsn(dsn))` の短縮形 |
| `connect*(config: ConnConfig)` | async_postgres/pg_connection/lifecycle.nim:584 | マルチホストフェイルオーバ付き接続。`orderedHosts` → `attemptHostTimed` → `connectToHost` |
| `connectToHost*` | async_postgres/pg_connection/lifecycle.nim:128 | 単一ホストブートストラップ: socket → SSL → Startup → 認証ループ → ReadyForQuery |
| `close*(conn)` | async_postgres/pg_connection/lifecycle.nim:446 | 冪等クローズ。listen ポンプ停止、Terminate 送信、トランスポート破棄 |
| `orderedHosts*` | async_postgres/pg_connection/lifecycle.nim:554 | ホスト順決定 (`load_balance_hosts=random` でシャッフル) |
| `parseDsn*` | async_postgres/pg_connection/dsn.nim:708 | URI / keyword=value DSN を `ConnConfig` へ |
| `parseUriDsn*` / `parseKeyValueDsn*` | async_postgres/pg_connection/dsn.nim:525 / 403 | DSN 各形式パーサ |
| `applyParam*` | async_postgres/pg_connection/dsn.nim:286 | 単一接続パラメータ適用 (sslrootcert 等でファイル読込を伴う) |
| `initConnConfig*` | async_postgres/pg_connection/dsn.nim:648 | プログラムによる ConnConfig 構築 |
| `ping*` | async_postgres/pg_connection/simple_query.nim:341 | 空 Query によるヘルスチェック |
| `cancel*` / `cancelNoWait*` | async_postgres/pg_connection/simple_query.nim:151 / 201 | 別ソケットで CancelRequest 送信 (帯域外クエリ取消) |
| `checkSessionAttrs*` | async_postgres/pg_connection/simple_query.nim:426 | target_session_attrs プローブ (primary/standby 判定) |
| `isConnected*` / `socketHasFin*` | async_postgres/pg_connection/buffer_io.nim:709 / 658 | 非ブロッキング生存プローブ (MSG_PEEK) |
| `quoteIdentifier*` | async_postgres/pg_connection/simple_query.nim:96 | 識別子クォート (LISTEN/UNLISTEN 等で使用) |

### 1.2 クエリ実行 (拡張問い合わせプロトコル)

| proc | 定義ファイル:行 | 用途 |
|---|---|---|
| `query*(conn, sql, params: seq[PgParam], ...)` | async_postgres/pg_client/query.nim:177 | 型付きパラメータ付き SELECT 系。`QueryResult` 返却 |
| `query*(conn, sql, params: seq[PgParamInline], ...)` | async_postgres/pg_client/query.nim:254 | ヒープ非割当インラインパラメータ版 |
| `queryEach*` | async_postgres/pg_client/query.nim:142 | 行ストリーミング (コールバック per 行、定数メモリ) |
| `queryRowOpt*` / `queryRow*` | async_postgres/pg_client/query.nim:284 / 298 | 先頭 1 行 |
| `queryValue*` (string / T) | async_postgres/pg_client/query.nim:313 / 329 | 先頭行先頭列 |
| `queryValueOpt*` | async_postgres/pg_client/query.nim:347 / 363 | NULL/空許容版 |
| `queryValueOrDefault*` | async_postgres/pg_client/query.nim:381 / 398 / 417 | デフォルト値版 |
| `queryExists*` | async_postgres/pg_client/query.nim:436 | 行存在 bool |
| `queryColumn*` | async_postgres/pg_client/query.nim:446 | 先頭列を seq[string] で |
| `exec*(conn, sql, params: seq[PgParam], ...)` | async_postgres/pg_client/exec.nim:69 | 行を捨てる実行。`CommandResult` (command tag) 返却 |
| `exec*(conn, sql, params: seq[PgParamInline], ...)` | async_postgres/pg_client/exec.nim:128 | インラインパラメータ版 |
| `notify*(conn, channel, payload)` | async_postgres/pg_client/exec.nim:156 | NOTIFY 送信 (payload 空なら `NOTIFY`、あれば `pg_notify`) |

内部 impl (公開だが内部向け): `queryImpl` query.nim:10/57、`queryInlineImpl` 207、
`queryEachImpl` 99、`execImpl` exec.nim:9/42、`execInlineImpl` 96。

### 1.3 ゼロアロケーション マクロ / simple プロトコル

| proc/macro | 定義ファイル:行 | 用途 |
|---|---|---|
| `queryDirect*` (macro) | async_postgres/pg_client/direct.nim:463 | パラメータを送信バッファへ直接エンコード |
| `execDirect*` (macro) | async_postgres/pg_client/direct.nim:580 | 同上 exec 版 |
| `simpleQuery*` | async_postgres/pg_connection/simple_query.nim:298 | simple protocol。複数文可、テキスト行のみ |
| `simpleExec*` | async_postgres/pg_connection/simple_query.nim:265 | simple protocol の副作用コマンド (BEGIN/SET/VACUUM 等) |
| `sql*` (macro) | async_postgres/pg_sql.nim:213 | `sql"..."` リテラルの `{expr}` → `$n` 展開 + `seq[PgParam]` 生成 |
| `sqlParams*` | async_postgres/pg_sql.nim:176 | `?` プレースホルダ → `$n` 変換 |
| `pgParams*` (macro) | async_postgres/pg_types/encoding.nim:599 | 複数値から `seq[PgParam]` 一括生成 |

### 1.4 Prepared Statement / Cursor / Pipeline

| proc | 定義ファイル:行 | 用途 |
|---|---|---|
| `prepare*` | async_postgres/pg_client/prepared.nim:59 | 明示的 prepare |
| `execute*` (stmt) | async_postgres/pg_client/prepared.nim:135 | PreparedStatement 実行 |
| `close*` (stmt) | async_postgres/pg_client/prepared.nim:185 | stmt クローズ |
| `openCursor*` | async_postgres/pg_client/cursor.nim:303 | カーソル OPEN |
| `fetchNext*` | async_postgres/pg_client/cursor.nim:202 | FETCH NEXT |
| `close*` (cursor) | async_postgres/pg_client/cursor.nim:260 | カーソル CLOSE |
| `withCursor*` (template) | async_postgres/pg_client/cursor.nim:267 | カーソル scoped ヘルパ |
| `newPipeline*` | async_postgres/pg_client/pipeline.nim:89 | Pipeline 生成 |
| `addExec*` / `addQuery*` | async_postgres/pg_client/pipeline.nim:118/148/155 | 文の積込 |
| `execute*` / `executeIsolated*` | async_postgres/pg_client/pipeline.nim:563 / 695 | パイプライン一括実行 |

### 1.5 トランザクション

| macro/proc | 定義ファイル:行 | 用途 |
|---|---|---|
| `withTransaction*` | async_postgres/pg_client/transaction.nim:362 | BEGIN/COMMIT/ROLLBACK scoped |
| `withTransactionRetry*` | async_postgres/pg_client/transaction.nim:435 | リトライ付き |
| `withSavepoint*` | async_postgres/pg_client/transaction.nim:520 | SAVEPOINT 嵌套 |
| `withTransactionDeadline*` | async_postgres/pg_client/transaction.nim:622 | deadline 付き |
| `withTransactionRetryDeadline*` | async_postgres/pg_client/transaction.nim:698 | retry + deadline |
| `withSavepointDeadline*` | async_postgres/pg_client/transaction.nim:782 | savepoint + deadline |
| `buildBeginSql*` | async_postgres/pg_client/core.nim:109 | BEGIN SQL 生成 |
| `execInTransaction*` / `queryInTransaction*` | async_postgres/pg_client/transaction_helpers.nim:110/138 / 166/196 | コールバック形式トランザクションヘルパ |

### 1.6 COPY

| proc/template | 定義ファイル:行 | 用途 |
|---|---|---|
| `copyIn*` (seq[byte] / openArray / string / seq[seq[byte]]) | async_postgres/pg_client/copy.nim:169 / 196 / 208 / 218 | COPY ... FROM STDIN (一括) |
| `copyInStream*` | async_postgres/pg_client/copy.nim:408 | COPY IN ストリーミング (CopyInCallback) |
| `copyOut*` | async_postgres/pg_client/copy.nim:498 | COPY ... TO STDOUT (一括 CopyResult) |
| `copyOutStream*` | async_postgres/pg_client/copy.nim:593 | COPY OUT ストリーミング (CopyOutCallback) |
| `makeCopyInCallback*` / `makeCopyOutCallback*` | async_postgres/pg_connection/buffer_io.nim:94 / 84 | バックエンド横断コールバック生成 |

### 1.7 LISTEN / NOTIFY

| proc | 定義ファイル:行 | 用途 |
|---|---|---|
| `listen*` | async_postgres/pg_connection/notify.nim:357 | チャネル購読 + バックグラウンドポンプ開始 |
| `unlisten*` | async_postgres/pg_connection/notify.nim:366 | 購読解除 |
| `waitNotification*` | async_postgres/pg_connection/notify.nim:398 | プル型通知待機 (timeout/overflow 検出) |
| `onNotify*` | async_postgres/pg_connection/notify.nim:34 | プッシュ型コールバック登録 |
| `onListenError*` | async_postgres/pg_connection/notify.nim:38 | ポンプ永久死通知 |
| `startListening*` / `stopListening*` | async_postgres/pg_connection/notify.nim:268 / 292 | ポンプ手動制御 |
| `reconnectInPlace*` | async_postgres/pg_connection/notify.nim:49 | 同一オブジェクトで再接続 + 再 LISTEN |

### 1.8 プール / クラスタ

| proc/template/macro | 定義ファイル:行 | 用途 |
|---|---|---|
| `newPool*` | async_postgres/pg_pool.nim:609 | プール生成 |
| `initPoolConfig*` | async_postgres/pg_pool.nim:156 | PoolConfig 構築 |
| `acquire*` / `acquireHandle*` | async_postgres/pg_pool.nim:1077 / 1102 | 接続取得 |
| `release*` (conn / handle) | async_postgres/pg_pool.nim:759 / 779 | 接続返却 |
| `withConnection*` (template) | async_postgres/pg_pool.nim:1114 | scoped 接続利用 |
| `resetSession*` / `resetSessionAndRelease*` | async_postgres/pg_pool.nim:307 / 793 | セッション状態リセット |
| `exec*` / `query*` / `queryEach*` / `queryRow*` / `queryValue*` 等 | async_postgres/pg_pool.nim:1378-1772 | プール経由クエリ群 |
| `withTransaction*` 系 (pool) | async_postgres/pg_pool.nim:1785 / 1860 / 1927 / 2078 | プールトランザクションマクロ |
| `withPipeline*` (pool) | async_postgres/pg_pool.nim:2210 | プールパイプライン |
| `close*` (pool) | async_postgres/pg_pool.nim:2220 | プールクローズ |
| `metrics*` / `idleCount*` / `activeCount*` 等 | async_postgres/pg_pool.nim:250 / 230 / 234 | 監視アクセサ |
| `newPoolCluster*` | async_postgres/pg_pool_cluster.nim:87 | 読み取りレプリカクラスタ生成 |
| `readConnection*` / `writeConnection*` | async_postgres/pg_pool_cluster.nim:241 / 259 | 振分接続取得 |
| `withReadConnection*` / `withWriteConnection*` | async_postgres/pg_pool_cluster.nim:274 / 284 | scoped 振分 |
| `withTransaction*` 系 (cluster) | async_postgres/pg_pool_cluster.nim:293 / 311 / 332 / 350 | クラスタトランザクション |
| `close*` (cluster) | async_postgres/pg_pool_cluster.nim:376 | クラスタクローズ |

### 1.9 Advisory Lock

| proc/macro/template | 定義ファイル:行 | 用途 |
|---|---|---|
| `advisoryLock*` / `advisoryTryLock*` / `advisoryUnlock*` | async_postgres/pg_advisory_lock.nim:140 / 146 / 154 (int64)、238 / 249 / 260 (int32×2) | セッション排他ロック |
| `advisoryLockShared*` / `advisoryTryLockShared*` / `advisoryUnlockShared*` | async_postgres/pg_advisory_lock.nim:164 / 172 / 181、273 / 284 / 295 | 共有ロック |
| `advisoryUnlockAll*` | async_postgres/pg_advisory_lock.nim:190 | 全ロック解除 |
| `advisoryLockXact*` / `advisoryTryLockXact*` (+Shared) | async_postgres/pg_advisory_lock.nim:200 / 207 / 218 / 227、308 / 319 / 332 / 343 | トランザクションスコープ |
| `withAdvisoryLock*` (macro 4 種) | async_postgres/pg_advisory_lock.nim:421 / 442 / 467 / 491 | scoped ロック |
| `withAdvisoryLockShared*` (macro 4 種) | async_postgres/pg_advisory_lock.nim:519 / 540 / 566 / 592 | scoped 共有 |
| `withAdvisoryLockXact*` / `withAdvisoryLockXactShared*` (template) | async_postgres/pg_advisory_lock.nim:623-653 / 656-686 | トランザクション scoped |

### 1.10 Large Object

| proc/template | 定義ファイル:行 | 用途 |
|---|---|---|
| `loCreate*` / `loUnlink*` | async_postgres/pg_largeobject.nim:107 / 117 | 生成 / 削除 |
| `loOpen*` / `loClose*` | async_postgres/pg_largeobject.nim:125 / 139 | ハンドル開閉 |
| `loRead*` / `loWrite*` | async_postgres/pg_largeobject.nim:147 / 165 | 読書 |
| `loSeek*` / `loTell*` / `loTruncate*` | async_postgres/pg_largeobject.nim:176 / 190 / 199 | 位置操作 |
| `loImport*` / `loExport*` | async_postgres/pg_largeobject.nim:209 / 218 | **サーバ側**ファイルとの入出力 |
| `loReadAll*` / `loWriteAll*` / `loSize*` | async_postgres/pg_largeobject.nim:230 / 249 / 275 | 一括操作 |
| `loReadStream*` / `loWriteStream*` | async_postgres/pg_largeobject.nim:323 / 342 | ストリーム操作 |
| `lo*Deadline*` 系 | async_postgres/pg_largeobject.nim:390 / 405 / 430 / 447 / 465 | deadline 版 |
| `withLargeObject*` (template) | async_postgres/pg_largeobject.nim:301 | scoped ハンドル |
| `makeLoReadCallback*` / `makeLoWriteCallback*` | async_postgres/pg_largeobject.nim:46 / 70 | コールバック生成 |

### 1.11 Replication

| proc/template | 定義ファイル:行 | 用途 |
|---|---|---|
| `connectReplication*` (config / dsn) | async_postgres/pg_replication.nim:541 / 554 | replication パラメータ付き接続 |
| `identifySystem*` | async_postgres/pg_replication.nim:579 | IDENTIFY_SYSTEM |
| `createReplicationSlot*` / `dropReplicationSlot*` / `readReplicationSlot*` | async_postgres/pg_replication.nim:609 / 629 / 643 | スロット管理 |
| `timelineHistory*` | async_postgres/pg_replication.nim:664 | TIMELINE_HISTORY |
| `startReplication*` | async_postgres/pg_replication.nim:1141 | 論理レプリケーションストリーム開始 |
| `startPhysicalReplication*` | async_postgres/pg_replication.nim:1325 | 物理レプリケーション開始 |
| `stopReplication*` | async_postgres/pg_replication.nim:1294 | ストリーム停止 |
| `sendStandbyStatus*` / `sendCopyData*` | async_postgres/pg_replication.nim:761 / 723 | Standby Status Update / 生 CopyData 送信 |
| `confirmFlushed*` / `confirmedFlushLsn*` | async_postgres/pg_replication.nim:801 / 788 | flush 位置確認 (at-least-once) |
| `parseReplicationMessage*` | async_postgres/pg_replication.nim:688 | CopyData → XLogData/PrimaryKeepalive |
| `parsePgOutputMessage*` / `decodePgOutput*` | async_postgres/pg_replication.nim:354 / 504 | pgoutput 論理デコード |
| `parseLsn*` / `makeReplicationCallback*` | async_postgres/pg_replication.nim:240 / 515 | LSN パース / コールバック生成 |

### 1.12 型変換・行アクセス (利用者向けデコード API)

| proc/macro | 定義ファイル:行 | 用途 |
|---|---|---|
| `toPgParam*` (多数のオーバーロード) | async_postgres/pg_types/encoding.nim:108 以降 (string:108, int32:116, bool:131, DateTime:137, JsonNode:219, seq 系:552 以降...) | Nim 値 → テキスト形式 PgParam |
| `toPgBinaryParam*` | async_postgres/pg_types/encoding.nim:614 以降、ranges.nim:488 以降 | バイナリ形式 PgParam |
| `toPgParamInline*` | async_postgres/pg_types/encoding.nim:8 以降 | インライン (ゼロアロ) パラメータ |
| `initRow*` / `clone*` | async_postgres/pg_protocol.nim:310 / 1052 | Row 生成 / 深コピー |
| `isNull*` | async_postgres/pg_types/accessors.nim:103 | NULL 判定 |
| `getStr*` / `getInt*` / `getInt16*` / `getInt64*` | async_postgres/pg_types/accessors.nim:133 / 192 / 223 / 249 | スカラーアクセス |
| `getFloat*` / `getFloat32*` / `getBool*` / `getBytes*` | async_postgres/pg_types/accessors.nim:275 / 297 / 416 / 433 | 同上 |
| `getNumeric*` / `getMoney*` / `getUuid*` | async_postgres/pg_types/accessors.nim:317 / 327 / 407 | 同上 |
| `getTimestamp*` / `getDate*` / `getTimestampTz*` / `getTime*` / `getTimeTz*` | async_postgres/pg_types/accessors.nim:457 / 472 / 486 / 501 / 516 | 時刻系 |
| `getJson*` / `getInterval*` / `getInet*` / `getCidr*` / `getMacAddr*` / `getMacAddr8*` | async_postgres/pg_types/accessors.nim:531 / 546 / 557 / 569 / 581 / 591 | 複合系 |
| `getBit*` / `getTsVector*` / `getTsQuery*` / `getXml*` / `getHstore*` | async_postgres/pg_types/accessors.nim:601 / 632 / 642 / 651 / 661 | 同上 |
| 幾何系 `getPoint*`〜`getCircle*` | async_postgres/pg_types/accessors.nim:670-845 | point/line/lseg/box/path/polygon/circle |
| 配列系 `get*Array*` / N 次元 `getArrayND*` | async_postgres/pg_types/accessors.nim:1124 以降 / 1455 | 配列デコード |
| `get*(row, col, typedesc[T])` 総称群 | async_postgres/pg_types/accessors.nim:1614 以降、pg_types.nim:156 (Option[T]) | 型ディスパッチ |
| 名前ベースアクセス群 (`nameAccessor` 展開) | async_postgres/pg_types.nim:61-160 | `row.getStr("name")` 等 |
| `len*` / `columnIndex*` / `rows*` / `items*` (QueryResult) | async_postgres/pg_connection/simple_query.nim:35 / 39 / 43 / 53 | 結果反復 |
| `initCommandResult*` / `affectedRows*` | async_postgres/pg_types/accessors.nim:86 / 89 | CommandResult |

### 1.13 認証 (公開だが主に内部使用)

| proc | 定義ファイル:行 | 用途 |
|---|---|---|
| `md5AuthHash*` | async_postgres/pg_auth.nim:43 | MD5 認証ハッシュ |
| `scramClientFirstMessage*` | async_postgres/pg_auth.nim:64 / 93 | SCRAM client-first |
| `scramClientFinalMessage*` | async_postgres/pg_auth.nim:114 | SCRAM client-final (PBKDF2 同期実行) |
| `scramVerifyServerFinal*` | async_postgres/pg_auth.nim:340 | サーバ署名検証 (相互認証) |
| `computeTlsServerEndpoint*` | async_postgres/pg_auth.nim:322 | channel binding データ (RFC 5929) |
| `selectScramMechanism*` / `enforceAuthAllowed*` / `filterSaslByRequireAuth*` | async_postgres/pg_connection/lifecycle.nim:60 / 33 / 46 | 機構選択・require_auth ポリシー |

---

## 2. サーバ入力のパース経路 (制御フロー)

### 2.1 呼び出しチェーン (ファイル:行)

```
socket (kernel)
  └─ fillRecvBuf            buffer_io.nim:183   ← 唯一の受信 await 点
       ├─ chronos:        conn.reader.readOnce (buffer_io.nim:202/204)
       └─ asyncdispatch:  conn.socket.recvInto (buffer_io.nim:229/231)
       (失敗時 conn.state = csClosed に遷移: 212/239)
       (chronos レプリケーション向け: fillRecvBufDetached buffer_io.nim:248)
  └─ nextMessage            buffer_io.nim:276   ← 同期パース。recvBuf を走査
       └─ parseBackendMessage  pg_protocol.nim:1166
            ├─ フレーミング: msgType=buf[0], msgLen=decodeInt32(buf,1) (1192-1193)
            ├─ maxLen 超過拒否 (1201-1205) / 不完全なら psIncomplete (1189/1208)
            └─ msgType 別ディスパッチ (1219-1278):
                 'R' → parseAuthentication         pg_protocol.nim:787
                       (0=Ok, 3=Cleartext, 5=MD5+salt, 10=SASL機構列,
                        11=SASLContinue, 12=SASLFinal)
                 'K' → parseBackendKeyData         pg_protocol.nim:825
                 'C' → parseCommandComplete        pg_protocol.nim:832
                 'D' → rowData != nil: parseDataRowInto  pg_protocol.nim:1099
                                       (RowData フラットバッファへ in-place)
                       skipDataRow:   フレーミングのみ (1231-1235)
                       else:          parseDataRow  pg_protocol.nim:837
                 'E' → parseErrorOrNotice(isError=true)   pg_protocol.nim:861
                 'N' → parseErrorOrNotice(isError=false)  pg_protocol.nim:861
                 'A' → parseNotification           pg_protocol.nim:887
                 'S' → parseParameterStatus        pg_protocol.nim:899
                 'T' → parseRowDescription         pg_protocol.nim:906
                 'Z' → parseReadyForQuery          pg_protocol.nim:932
                 't' → parseParameterDescription   pg_protocol.nim:946
                 'v' → parseNegotiateProtocolVersion pg_protocol.nim:963
                 '1'/'2'/'3'/'I'/'n'/'s' → 固定メッセージ (1254-1265)
                 'G'/'H'/'W' → parseCopyResponse   pg_protocol.nim:985
                 'd' → CopyData (本文をそのまま copyData へ) (1272-1274)
                 'c' → CopyDone (1275-1276)
                 その他 → PgProtocolError (1277-1278)
  └─ nextMessage 内の副次ディスパッチ (buffer_io.nim:325-360):
       psDataRow      → onRow コールバック / rowCount 加算 / rowData 蓄積
       Notification   → dispatchNotification   buffer_io.nim:112
                        (notifyQueue 入隊、overflow 時は古いものを drop、
                         notifyWaiter 完了、notifyCallback 起動)
       NoticeResponse → dispatchNotice         buffer_io.nim:142
       ParameterStatus→ conn.serverParams 記録  buffer_io.nim:345-350
       NegotiateProto → conn.negotiatedMinorVersion 記録 (351-356)
```

`recvMessage` (buffer_io.nim:362) = `nextMessage` + `fillRecvBuf` のループ。

### 2.2 パンプループ (各操作の受信制御フロー)

`pumpUntilReady` テンプレート 3 過負荷 (buffer_io.nim:376 / 417 / 450):
- `nextMessage` でメッセージを消費し、`bmkErrorResponse` → `newPgQueryError`
  (types.nim:835) 生成、`bmkReadyForQuery` で `conn.txStatus` 更新 + `csReady`
  遷移して終了。DataRow は `nextMessage` 内で消費され body には現れない。

| 利用者 | 経路 |
|---|---|
| 拡張 query | queryImpl (query.nim:10/57) → `queryRecvLoop` (core.nim:434) → pumpUntilReady (RowData 蓄積) → QueryResult |
| queryEach | queryEachImpl (query.nim:99) → `queryEachRecvLoop` (core.nim:503) → ストリーミング過負荷 (onRow) |
| 拡張 exec | execImpl (exec.nim:9/42) → `execRecvLoop` (core.nim:580) → 素過負荷 (skipDataRow) |
| direct 系 | queryDirectImpl (direct.nim:32) / execDirectRunImpl (direct.nim:540) → 同上 recv ループ |
| simpleQuery/simpleExec | simple_query.nim:102 / 131 → pumpUntilReady |
| ping | simple_query.nim:341 → EmptyQueryResponse 期待 |
| COPY IN | copy.nim: copyInRawImpl:72 / copyInStreamImpl:237 (CopyInResponse 待ち → CopyData 送信 → CommandComplete) |
| COPY OUT | copy.nim: copyOutImpl:447 / copyOutStreamImpl:520 (CopyData 受信 → callback/蓄積) |
| prepare/execute/close | prepared.nim (prepareImpl:28, executeImpl:78, closeImpl:167) |
| cursor | cursor.nim: fetchNext:202 / close:260 |
| pipeline | pipeline.nim: execute:563 / executeIsolated:695 |
| 接続時認証 | lifecycle.nim: authLoop 306-415 / readyLoop 418-436 |
| LISTEN ポンプ | notify.nim: listenPump:147 → recvMessage (362) → dispatchNotification |
| レプリケーション | pg_replication.nim: runReplicationStream:1025 → nextMessage → bmkCopyData → handleReplicationData:970 → parseReplicationMessage:688 → callback。XLogData.data は利用者が `decodePgOutput`:504 → `parsePgOutputMessage`:354 で pgoutput デコード |

### 2.3 型デコード (サーバ由来バイト → Nim 値)

受信時はまず `RowData` (pg_protocol.nim:141) フラットバッファにセルが
`(offset,len)` 列で蓄積されるだけ。**実際のデコードは利用者の Row アクセサ
呼び出し時に遅延実行**される:

```
Row アクセサ (accessors.nim)
  ├─ テキスト形式: fromPgText (decoding.nim:55) / parse*Text 群
  │    parseTimestampText:366, parseDateText:399, parseTimeText:411,
  │    parseTimeTzText:441, parseHstoreText:475, parseIntervalText:540,
  │    parseInetText:676, parsePointText:864, parseTextArray:897
  └─ バイナリ形式 (rfBinary / BinarySafeOids):
       decodeNumericBinary:63, decodeBinaryTimestamp:94, decodeBinaryDate:127,
       decodeBinaryTime:150, decodeBinaryTimeTz:164, decodeInetBinary:191,
       decodeBinaryArray:239, decodeBinaryComposite:327,
       decodeHstoreBinary:16, decodeBinaryTsVector:699, decodeBinaryTsQuery:850
       (すべて decoding.nim)
  配列: pg_types/array.nim、レンジ: pg_types/ranges.nim、
  複合/enum: pg_types/user_types.nim
```

---

## 3. データフロー図

### 3.1 送信パス (利用者パラメータ → サーバ)

```
利用者コード
  │  sql"..." マクロ (pg_sql.nim:213)  ── {expr} → $n + seq[PgParam]
  │  pgParams マクロ (encoding.nim:599)
  │  toPgParam* (encoding.nim:108〜)      ── テキスト形式 PgParam{oid,format=0,value}
  │  toPgBinaryParam* (encoding.nim:614〜) ── バイナリ形式 PgParam{format=1}
  │  toPgParamInline* (encoding.nim:8〜)   ── PgParamInline (小値は inlineBuf)
  ▼
query/exec (query.nim:177 / exec.nim:69)
  │  checkReady (simple_query.nim:63)
  │  lookupStmtCache / invalidateIfOidMismatch (cache.nim:31 / core.nim:220)
  │  extractParams (core.nim:262) / flattenInline (core.nim:301, appendInlineParam:273)
  ▼
sendExtendedQuery / sendExtendedExec テンプレート (core.nim:322 / 395)
  │  conn.sendBuf へ積込:
  │    addParse   (pg_protocol.nim:562)  ── Parse: stmt名, SQL, param OID
  │    addBind    (pg_protocol.nim:579)  ── Bind: param formats/values + result formats
  │    addBindRaw (pg_protocol.nim:612)  ── inline/raw データ用
  │    addDescribe(664) / addExecute(673) / addClose(682) / addSync(691)
  │  (cache hit: Bind+Execute+Sync / miss: Parse+Describe+Bind+Execute+Sync)
  ▼
sendBufMsg (buffer_io.nim:562) または sendMsg (buffer_io.nim:544)
  │  失敗時 conn.state = csClosed
  ▼
chronos: conn.writer.write / asyncdispatch: socket.sendRawBytes (buffer_io.nim:159)
  ▼
socket → PostgreSQL サーバ
```

simple protocol: `encodeQuery` (pg_protocol.nim:550) → `sendMsg`。
接続時: `encodeStartup` (pg_protocol.nim:476)、`encodeSSLRequest` (514)、
`encodePassword` (524)、`encodeSASLInitialResponse` (534)、`encodeSASLResponse` (543)。
その他: `encodeCancelRequest` (745)、`encodeCopyData` (753)、`encodeCopyDone` (774)、
`encodeCopyFail` (778)、`encodeTerminate` (741)、`encodeStandbyStatusUpdate` (1362)。

### 3.2 受信パス (サーバ → 利用者)

```
PostgreSQL サーバ
  ▼
socket → fillRecvBuf (buffer_io.nim:183) → conn.recvBuf (seq[byte])
  ▼
nextMessage (buffer_io.nim:276) → parseBackendMessage (pg_protocol.nim:1166)
  ├─ DataRow        → parseDataRowInto (pg_protocol.nim:1099) → RowData.buf/cellIndex
  ├─ RowDescription → qr.fields / キャッシュ (core.nim:469-483)
  ├─ CommandComplete→ qr.commandTag / CommandResult
  ├─ Error          → PgQueryError (types.nim:835) → ReadyForQuery 後に raise
  ├─ Notification   → notifyQueue → waitNotification / onNotify callback
  └─ ParameterStatus→ conn.serverParams
  ▼
QueryResult{fields, data: RowData, rowCount, commandTag} (types.nim:371)
  ▼
rows/items (simple_query.nim:43/53) → initRow (pg_protocol.nim:310) → Row ビュー
  ▼
Row アクセサ getStr/getInt/... (accessors.nim) → decoding.nim で遅延デコード
  ▼
利用者コード
```

### 3.3 DSN → ConnConfig → connect

```
DSN 文字列
  ▼
parseDsn (dsn.nim:708)
  ├─ postgresql:// / postgres:// → parseUriDsn (dsn.nim:525)
  └─ その他 → parseKeyValueDsn (dsn.nim:403)
  ▼
applyParam (dsn.nim:286)  ※ sslrootcert/sslcert/sslkey はファイル読込 (§4 参照)
  ▼
ConnConfig (types.nim:109)  + validateClientCertConfig (types.nim:742)
  ▼
connect(config) (lifecycle.nim:584)
  → orderedHosts (554, lbhRandom 時は sysrand.urandom でシャッフル)
  → attemptHostTimed (518, ホスト毎 connectTimeout)
  → attemptHost (505) → matchesOrClose (481, target_session_attrs)
  → connectToHost (128)
```

### 3.4 SSL ネゴシエーション

```
connectToHost (lifecycle.nim:274-275)
  ▼
negotiateSSL (ssl.nim:518)
  ├─ validateClientCertConfig / validateDirectSslCompatible (ssl.nim:215)
  ├─ sslnDirect (PG17+): 直ちに establishTls、ALPN "postgresql" 必須
  └─ sslnPostgres: encodeSSLRequest 送信 → 1 バイト応答 ('S'/'N')
       ├─ pre-TLS 注入検出: socketHasPendingData (buffer_io.nim:684) /
       │   extraBytesBuffered (ssl.nim:556-574)  [CVE-2021-23214 系対策]
       └─ 'S' → establishTls (ssl.nim:256)
            ├─ chronos (BearSSL):
            │    parseTrustAnchors (pg_bearssl.nim:125) ← sslRootCert PEM
            │    TLSCertificate.init / TLSPrivateKey.init ← sslCert/sslKey
            │    newTLSClientAsyncStream (TLS1.2 のみ) → handshake
            │    installX509Capture (pg_bearssl.nim:103) → serverCertDer 取得
            └─ asyncdispatch (OpenSSL):
                 writeTempPem (ssl.nim:361) で PEM を一時ファイル化 (/dev/shm 優先)
                 SSL_CTX_load_verify_locations / use_certificate / PrivateKey
                 wrapConnectedSocket → driveTlsHandshake (ssl.nim:103)
                 verify-full: enforceVerifyFullIdentity (ssl.nim:180)
                 SSL_get_peer_certificate → i2d_X509 → conn.serverCertDer
                 一時ファイルは removeTempPem (ssl.nim:352) で削除
```

### 3.5 認証フロー

```
authLoop (lifecycle.nim:306-415)
  ├─ bmkAuthenticationOk            → break (scramStarted なら scramFinalVerified 必須)
  ├─ bmkAuthenticationCleartext     → enforceAuthAllowed → encodePassword
  ├─ bmkAuthenticationMD5Password   → md5AuthHash (pg_auth.nim:43) → encodePassword
  ├─ bmkAuthenticationSASL          → filterSaslByRequireAuth (lifecycle.nim:46)
  │                                   → selectScramMechanism (lifecycle.nim:60)
  │                                     (channel binding: computeTlsServerEndpoint
  │                                      pg_auth.nim:322, serverCertDer 使用)
  │                                   → scramClientFirstMessage (pg_auth.nim:64)
  │                                   → encodeSASLInitialResponse
  ├─ bmkAuthenticationSASLContinue  → scramClientFinalMessage (pg_auth.nim:114)
  │                                   (saslprep: pg_saslprep.nim / PBKDF2 同期、
  │                                    iteration 上限 effectiveMaxScramIterations)
  │                                   → encodeSASLResponse
  └─ bmkAuthenticationSASLFinal     → scramVerifyServerFinal (pg_auth.nim:340)
                                      (失敗→接続拒否。相互認証)
  ※ パスワード/鍵素材は burnMem/burnStr で拭き取り (pg_auth.nim:10)
```

---

## 4. ファイルシステム / 外部リソースに触る箇所

ライブラリ自体は永続化層を持たない。クライアント側の FS 接触は SSL 証明書
まわりのみ。

| 箇所 | ファイル:行 | 操作 | 備考 |
|---|---|---|---|
| `readPemFileParam` | async_postgres/pg_connection/dsn.nim:214 | **読込** | sslrootcert/sslcert/sslkey のパス指定をディスクから読込。`applyParam` の 342-350 から呼ばれる |
| `openRegularFile` | async_postgres/pg_connection/dsn.nim:186 | **読込** (posix.open/fstat) | 通常ファイル保証 + TOCTOU 緩和 (O_NONBLOCK→fstat→F_SETFL)。sslkey は group/world 権限ビットを拒否 (225-230) |
| `writeTempPem` | async_postgres/pg_connection/ssl.nim:361 | **書込+削除** | asyncdispatch+OpenSSL のみ。PEM 内容を 0600 一時ファイル化 (`createTempFile`、Linux では /dev/shm 優先: 366-376)。`removeTempPem` (352) で newContext 読込後ただちに削除 |
| `dirExists("/dev/shm")` | async_postgres/pg_connection/ssl.nim:368 | 参照 | 一時ファイル配置先判定 |
| `loadLibPattern` | async_postgres/pg_connection/ssl.nim:73-74 | **動的操作** | libssl/libcrypto の動的ロードとシンボル解決 (SSL_set1_host 等) |
| `urandom(8)` | async_postgres/pg_connection/lifecycle.nim:570 | **読込** (OS エントロピ) | `load_balance_hosts=random` のシャッフルシード (std/sysrand) |
| `randomBytes` | async_postgres/pg_auth.nim:78 | OS エントロピ | SCRAM クライアントノンセス (nimcrypto) |
| `loImport` / `loExport` | async_postgres/pg_largeobject.nim:209 / 218 | **サーバ側 FS** | `SELECT lo_import/lo_export` — ファイル I/O は PostgreSQL サーバホスト上で実行される。クライアント FS には触れない。サーバ側任意パス書込になり得るため呼び出し元の権限制御が前提 |
| ソケット I/O | lifecycle.nim:178-263、simple_query.nim:151-199 (cancel)、buffer_io.nim 全体 | ネットワーク | TCP/Unix ドメインソケット、DNS 解決 (resolveTAddress)、TCP keepalive/NODELAY (buffer_io.nim:743-780) |

**該当なし**: DB ファイル書込、ローカル状態ファイル、キャッシュファイル、
ログファイル出力 (診断は `warnStderr` types.nim:734 で stderr のみ)。

---

## 5. 未確認 / 部分確認の領域

- `pg_pool.nim` のメンテナンスループ / ヘルスチェック / 接続破棄ポリシーの
  内部実装 (公開 API の入口のみ確認。2310 行中の大半は未精査)
- `pg_client/pipeline.nim` の送信バッファ構築内部 (公開 API のみ確認)
- `pg_client/transaction.nim` のマクロ展開本体 (AST 構築ヘルパ群)
- `pg_types/user_types.nim` (複合型/enum の登録・デコード詳細)
- `pg_types/ranges.nim` のレンジ/マルチレンジ デコード本体
  (エンコード側 toPgParam/toPgBinaryParam の行は確認済み)
- `async_backend.nim` のバックエンド抽象内部 (wait/sleepAsync 等の実装詳細)
- `pg_bytes.nim` / `pg_errors.nim` / `pg_saslprep.nim` の内部アルゴリズム
- `htmdocs/`、`tests/`、`examples/` (本調査はライブラリ本体のみ)
- 各 Row アクセサの個別デコードロジックの正当性 (入口と委譲先は確認済み、
  個々のパース処理の精査は decoding.nim の関数名・行の列挙に留まる)
