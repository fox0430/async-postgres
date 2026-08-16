# transaction.nim / transaction_helpers.nim 監査所見

対象:
- `async_postgres/pg_client/transaction.nim` (860行) — withTransaction 系6マクロ (conn版) + withSavepoint / withSavepointDeadline、共有ビルダ (buildRollbackCleanup / buildSavepointRollbackCleanup / buildDeadlineAwaitAndTimeout / buildRetryTxLoop / buildRetryDeadlineLoop)、AST ガード (hasReturnStmt / hasLoopEscapeStmt / checkNoBodyEscape)。
- `async_postgres/pg_client/transaction_helpers.nim` (224行) — execInTransaction / queryInTransaction (パイプライン単一Sync)。

バックグラウンド: デフォルトバックエンドは asyncdispatch (`async_backend.nim:9`)。asyncdispatch にはキャンセル原始が無い (`async_backend.nim:79` warning、`cancelAndWait` は 193-204行で no-op)。chronos は真正キャンセル可能。`CancelledError` は asyncdispatch では `object of CatchableError` として定義されるだけ (`async_backend.nim:83`) で実行時からは送出されない。

既存レビュー `reviews/review-transaction.md` Issue 2 (cancel cleanup) は branch f2867ac 対応済（各 body try へ `except CancelledError: raise` 追加）を踏まえて再調査した。

## 対処済み（削除）

- 旧・所見1 「transaction conn マクロのサーバ側中断欠如」(Medium・5箇所): transaction.nim の
  226/271/347/428/598 の CancelledError 分岐に `cancelNoWait` + `state = csClosed` を追加、
  pool 版 (`pg_pool.nim:2039-2041,2197-2199`) と対称化。コミット ede7927。
- 旧・所見2 「未カバーの cancel 分岐4箇所」(Medium・テスト空白): test_transaction_cancel.nim に
  withSavepoint / withTransactionDeadline / withTransactionRetry / withTransactionRetryDeadline の
  in-flight cancel 時 csClosed 化検証を追加（+5 テスト、chronos ゲート）。同上コミット。

---

- 分類: 公開境界 / 可観測性（doc とコードの不一致）
- 重大度: Low
- 確信度: 確定
- 場所: async_postgres/pg_client/transaction.nim:124-127 (doc) vs 141-153 (code) — `buildRollbackCleanup`; 158-162 (doc) vs 176-190 (code) — `buildSavepointRollbackCleanup`
- 事象:
  両ビルダの doc は「invalidated connection の場合、またはサーバが既にトランザクションを終了している場合に ROLLBACK をスキップし、**両方を** `onCleanupSkipped` で報告する」と述べるが、コードが報告するのは `state != csReady`（`csrConnInvalidated`）のみ。「サーバが既に tx を終了」した場合（`state == csReady` かつ `txStatus == tsIdle`）は `if`/`elif` のいずれにも該当せず、イベントを発さず無音でスキップされる。`onCleanupSkipped` を監視に使う運用者は、doc が約束する「両方」の片方しか観測できない。
- 根拠: コード引用と推論
  - doc 124-127: "skip ROLLBACK on an invalidated connection or when the server already ended the transaction (reporting both via `onCleanupSkipped`)"。
  - code 141-153: `if conn.state != csReady: fireCleanupSkipped(conn, ckTxRollback, csrConnInvalidated)` / `elif conn.txStatus in {tsInTransaction, tsInFailedTransaction}: try: ROLLBACK except: fireCleanupSkipped(csrCleanupFailed)`。`state == csReady ∧ txStatus == tsIdle`（COMMIT が serialization failure で既に tx を終了させた場合等）はどちらの分岐にも入らず、`fireCleanupSkipped` 呼び出しが無い。
  - 報告不能の根拠: `CleanupSkipReason` 列挙型 (`types.nim:511-520`) は `csrConnInvalidated` と `csrCleanupFailed` の2値のみで「サーバが既に tx を終了」を表す理由コードが存在しない。doc の "reporting both" は実装不可能な記述。
  - 実害は限定的: tsIdle スキップは benign（片付ける tx が既に無い）であり、イベント不在は設計判断として正当化し得る。問題は doc が実際の挙動と異なる点（可観測性の過大表明）にある。
- 系統性: 同種パターン 2箇所（`buildRollbackCleanup`, `buildSavepointRollbackCleanup`）。両者とも doc の "reporting both via" と同一構造の code を持つ。
  grep パターン: `rg -n "reporting both via" async_postgres/` → 2件（transaction.nim:126, 161）。

---

## 検証済み・所見なし（系統比較の結果）

- `buildBeginSql` (`core.nim:109-137`) の isolation/access/deferrable 結合は全て有効な PostgreSQL 構文。付加順は `ISOLATION LEVEL ...` → `READ WRITE|READ ONLY` → `DEFERRABLE|NOT DEFERRABLE` で、PostgreSQL が任意順を許容する transaction_mode の列として妥当。`ilReadUncommitted` はサーバが READ COMMITTED へ写像するが構文エラーにはならず、クライアント側の検証責務でもない。5×3×3=45 組合せいずれも `BEGIN` 単独または空白区切りの付加で、二重空白/末尾空白の生成も無い。
- `isRetryableTxError` (`core.nim:139-146`) は `PgQueryError` かつ SQLSTATE が `states` 一致の場合のみ true。接続断・タイムアウト（`PgQueryError` 以外）は決してリトライ不可で、これは「再利用不能な接続で再試行しない」設計と整合。`buildRetryTxLoop`:277-279 / `buildRetryDeadlineLoop`:355-357 のリトライゲートも `state == csReady ∧ txStatus == tsIdle` を追加し、COMMIT の serialization failure 後（サーバが tx を終了済み → tsIdle）に ROLLBACK を挟まず再試行する経路は doc (466-471) と一致する。
- `backoffDelayMs` (`core.nim:166-179`) は `pow` の overflow を `min(raw, maxDelayMs.float)` で、負値を `if ms < 0: ms = 0` で、jitter の `rand(ms)` を `ms > 0` ガードで保護。`multiplier <= 0` や巨大 `attempt` でも破綻しない。
- `hasReturnStmt` (9-22) / `hasLoopEscapeStmt` (24-66) はネストした proc/func/method/iterator/lambda/do/converter/template/macro を走査から除外し、内側 `return`/`break`/`continue` を誤検出しない。`block` 内ラベル無し `break` を安全側（誤検出しない方向）に見逃すことは 33-40行で明記済みの既知妥協で、コンパイラの `UnnamedBreak` 警告が補完する。`checkNoBodyEscape` (68-86) は6マクロ全てから呼ばれ（414, 494, 573, 669, 745, 827）、COMMIT/RELEASE スキップを抑止する。
- `withTransaction` における「body 例外が cleanup の CancelledError に勝つ」優先順位は意図的かつテスト済み（test_transaction_cancel.nim:108-155 "cleanup CancelledError does not mask the body error" が ValueError 伝播と `csrCleanupFailed` 記録を検証）。`buildRollbackCleanup` 146-149 が cleanup 中の cancel を swallow して原因例外を再送出する設計は、121e10a の分割意図と一致。これは所見ではなく設計判断。
- `transaction_helpers.nim` の cleanup が `== tsInFailedTransaction`（87行）と、マクロの `in {tsInTransaction, tsInFailedTransaction}`（transaction.nim:143）で異なるのは正当: パイプライン版は BEGIN/user/COMMIT が単一 Sync で全てサーバ駆動のため、`queryError != nil` は必ず ErrorResponse ⟺ ReadyForQuery 'E' 状態に対応し、クライアント側 body 例外で 'T' が残るマクロ版の `tsInTransaction` ケースは発生しない。`pumpUntilReady` (`buffer_io.nim:405-411`) は ReadyForQuery で `txStatus` を確定してから `queryError` を raise するため、cleanup 判定時点で `txStatus` は信頼できる。
- `withSavepoint` の名前ディスアンビギュエーション (552行 `{nnkStrLit, nnkTripleStrLit, nnkRStrLit}`) は既存レビュー Issue 1 の修正済み（44fa0cf）。3引数形 (562-566) が kind 検査しないのは arity で曖昧性が消えるためで、`withSavepointDeadline` doc (803-806) も任意 string 式を許容する旨を明記しており、不整合ではない。
- `savepointNameExpr` (506-518) の無名 savepoint 名生成は `portalCounter` の inc 後に `"_sp_" & $counter` で、同一接続上のネスト savepoint に一意名を付与。`quoteIdentifier` (`simple_query.nim:96-98`) で二重引用符エスケープされ、SQL injection はガード済み（e2e 1063, 1659 で検証）。
