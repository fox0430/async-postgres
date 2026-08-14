# pipeline.nim / copy.nim 監査所見

---

- 分類: セキュリティ（信頼境界）/ 並行性・状態
- 重大度: Medium → **前提崩壊（実サーバでは発生せず）+ 防御的硬化済み（2026-08-14）**
- 確信度: 高（前提検証に不備あり、下記の通り訂正）
- 場所: async_postgres/pg_client/copy.nim:131-134, 379-383
- 事象:
  COPY IN 正常完了パスで、サーバーアボート（ErrorResponse）の最終確認なしに CopyDone を送信する。サーバーが最後の `pollCopyInError` 以降かつ CopyDone 到着前にアボートした場合、サーバーは copy-in モードを離脱済みで CopyDone は不正メッセージとなる。
- **検証結果（2026-08-14）: 実サーバ（PostgreSQL 18.3）ではこの事象は発生しない。**
  - ワイヤレベルの実験（生ソケットで stray CopyDone を送信）: サーバーは追加の ErrorResponse を一切返さず、次のクエリは正常に処理された。クライアント（本ライブラリ）も desync せず接続は csReady のまま再利用可能。
  - PG 18 のソース（src/backend/tcop/postgres.c、idle 状態のメッセージ分岐）に明記:
    `case PqMsg_CopyData: case PqMsg_CopyDone: case PqMsg_CopyFail: /* Accept but ignore these messages, per protocol spec; we probably got here because a COPY failed, and the frontend is still sending data. */`
    つまり「COPY 失敗後にフロントエンドが送り続けるコピーメッセージ」はプロトコル仕様上想定されており、サーバーは黙って捨てる（PG 9.1 以降の挙動）。監査所見の前提「サーバーはプロトコル違反として追加の ErrorResponse + ReadyForQuery を返す」は誤り。
  - 影響: 実 PG 相手では desync・残留メッセージは発生しない。既存 e2e テスト（"copyIn bad data inside txn" 等）は desync を内包せず正しく復旧していた。
- **対処済み（防御的硬化、2026-08-14）**: 非標準サーバ（stray copy メッセージに違反応答を返す実装）に対する防御として:
  1. CopyDone 送信前に最終 `pollCopyInError` を追加（callbackError パスの CopyFail 前 poll と対称化。監査所見の非対称指摘を解消）。
  2. 最終 pump がサーバーエラーで raise した場合に `drainLeftoverToReady` で残留違反応答（ErrorResponse + ReadyForQuery）を消費。部分メッセージがバッファ済みのときのみブロック待機（残りが必ず届くため）、空バッファでは即時復帰（実サーバ相手の正常系に影響なし）。CopyFail パスにも適用。
  3. 回帰テスト `tests/test_copy_race.nim`（モックサーバが違反応答を返す 2 シナリオ、両 async バックエンド）。
  - 実 PG では全経路が no-op。全 e2e（46 件）・全スイートが両バックエンドで合格。
- 根拠:
  `copyInRawImpl` line 127 の `pollCopyInError` は `sendBuf.len >= copyBatchSize` 時のみ呼ばれる。データが 1 バッチ未満（< 256KB）の場合、poll は一度も実行されず line 133 で CopyDone が送信される:
  ```nim
  if abortError == nil:
    # Flush remaining data + CopyDone in one send
    conn.sendBuf.addCopyDone()
    await conn.sendBufMsg()
  ```
  `copyInStreamImpl` も同構造: line 310 の poll は `sendBuf.len >= batchThreshold` 時のみ。コールバックが空を返して break 後（line 290-291）、line 381 で CopyDone を送信。両者とも CopyDone 前にサーバー状態の最終確認がない。
  対照的に、callbackError パス（line 340）では CopyFail 送信前に `pollCopyInError` でサーバーアボートを明示確認している。正常完了パスにこの確認がない。
  pumpUntilReady（buffer_io.nim:405-412）は ReadyForQuery で即 raise/break するため、残留メッセージを消費しない:
  ```nim
  elif pumpMsg.kind == bmkReadyForQuery:
    conn.txStatus = pumpMsg.txStatus
    if conn.state != csClosed:
      conn.state = csReady
    readyBody
    if queryError != nil:
      raise queryError
    break pumpLoop
  ```
- 系統性: 同種パターン（2箇所）。`rg -n "addCopyDone" -g "*.nim"` → copy.nim:133, copy.nim:381。いずれも正常完了パスで CopyDone 前に abort 確認なし。callbackError パス（copy.nim:340）のみ poll あり。

---

- 分類: セキュリティ（CopyData の長さ、巨大確保）
- 重大度: Medium
- 確信度: 確定
- 場所: async_postgres/pg_client/copy.nim:476
- 事象:
  `copyOutImpl`（バッファリング型 COPY OUT）がサーバーからの CopyData を総量制限なしに蓄積する。1 メッセージあたりは `effectiveMaxMessageSize`（デフォルト 1 GiB, pg_protocol.nim:297）で制限されるが、メッセージ数に上界がなく、悪意/侵害されたサーバーが CopyData を無制限に送信してクライアントを OOM にできる。`copyOutStream`（ストリーム型）はコールバックによるバックプレッシャがあるが、`copyOut`（バッファ型）には緩和策がない。
- 根拠:
  ```nim
  of bmkCopyData:
    cr.data.add(move(pumpMsg.copyData))
  ```
  `CopyResult.data` は `seq[seq[byte]]`（types.nim:382）。pumpUntilReady は ReadyForQuery まで全メッセージを消費するため、サーバーが CopyDone/CommandComplete/ReadyForQuery を送らなければ蓄積は無限に続く。timeout パラメータは `ZeroDuration`（無制限）がデフォルト。
  per-message cap の存在（pg_protocol.nim:1201: `if maxLen > 0 and int64(msgLen) >= int64(maxLen)` ）は信頼境界の意識を示すが、aggregate cap がない。
  ストリーム型（copy.nim:555）は `await callback(move(pumpMsg.copyData))` でコールバックが処理を制御可能。バッファ型にこの緩和がない。
- 系統性: 単発。バッファリング型 COPY OUT は copy.nim:476 の 1 箇所のみ。ストリーム型（copy.nim:555）はコールバック移譲で影響なし。

---

- 分類: 並行性・状態（pipeline 途中失敗時の pending ops）
- 重大度: Medium
- 確信度: 高
- 場所: async_postgres/pg_client/pipeline.nim:520-544（executeImpl）, 659-680（executeIsolatedImpl）
- 事象:
  パイプライン内で scsMiss op が Execute 時にエラー（例: 23505 unique_violation）で失敗した場合、その op の server-side prepared statement がキャッシュにも pendingStmtCloses にも入らず、セッション終了までサーバー側に残留する。両バリアント（executeImpl / executeIsolatedImpl）で発生。
  executeImpl のコメント（line 526-528）は「the failing op's stmt may never have been Parsed」とするが、Execute 時エラーの場合 Parse/Describe/Bind は全て成功（ParseComplete/ParameterDescription/BindComplete 受信済み）であり statement はサーバーに存在する。prepared statement はセッション状態であり transaction abort でも破棄されない。
- 根拠:
  `executeImpl` line 529-543:
  ```nim
  for i in 0 ..< activeOpIdx:
    addCacheMissOp(i)
  if queryError.sqlState in StmtCacheInvalidatingStates and
      activeOpIdx < p.ops.len and
      p.ops[activeOpIdx].cache in {scsHit, scsShare}:
    conn.pendingStmtCloses.add(p.ops[activeOpIdx].stmtName)
    conn.removeStmtCache(p.ops[activeOpIdx].sql)
  raise queryError
  ```
  失敗 op（activeOpIdx）が scsMiss の場合: `for i in 0 ..< activeOpIdx` の範囲外（回収なし）、invalidation 条件は `{scsHit, scsShare}` のみ（scsMiss は対象外）。結果: 何もされない。
  `executeIsolatedImpl` line 661-679:
  ```nim
  if opError != nil:
    if opError.sqlState in StmtCacheInvalidatingStates and
        p.ops[opIdx].cache in {scsHit, scsShare}:
      conn.pendingStmtCloses.add(p.ops[opIdx].stmtName)
      conn.removeStmtCache(p.ops[opIdx].sql)
    errors[opIdx] = opError
  elif p.ops[opIdx].cache == scsMiss and not p.ops[opIdx].cacheSuperseded:
    conn.addStmtCache(...)
  ```
  同構造: `opError != nil` 分岐内では scsMiss は回収も Close もされない。`elif`（成功パス）には入らない。
  executeIsolatedImpl はエラー後も後続 op を実行し接続は csReady に復帰するため、リークした statement は接続プール経由で長期生存し得る。
- 系統性: 同種パターン（2箇所）。executeImpl line 539-543 と executeIsolatedImpl line 662-669 は同一の `{scsHit, scsShare}` 限定条件。scsMiss の失敗 op を処理するコードパスが両バリアントに存在しない。`rg -n "cache in \{scsHit, scsShare\}" -g "*.nim"` → pipeline.nim:541, pipeline.nim:663 の 2 箇所。
