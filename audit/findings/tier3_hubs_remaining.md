# Tier 3 残部（ハブ群・cache/type_lookup・transaction_helpers）監査所見

対象: ハブ 4（async_postgres.nim 139行 / pg_client.nim 82行 / pg_connection.nim 54行 / pg_types.nim 160行）、
`pg_connection/cache.nim`（100行）、`pg_connection/type_lookup.nim`（107行）、
`pg_client/transaction_helpers.nim`（224行）。
方法: 全文精読 + 呼び出し側ガード確認 + 実コンパイル（asyncdispatch 既定 / chronos 両バックエンド）。

## 検証済み・所見なし（系統比較の結果）

- **evictStmtCache の空キャッシュ Defect なし**: `.head` が nil なら NilAccessDefect（Defect）。呼び出し 5 箇所
  （core.nim:385,426 / pipeline.nim:304 / direct.nim:398 / cache.nim:64 の自己ループ）は全て
  `conn.stmtCache.len >= conn.stmtCacheCapacity` ガード内で capacity > 0 を前提 → 表・LRU リスト両方が非空。
  LRU と Table は全更新経路（touch/add/evict/remove）で対で同期。
- **clearStmtCache の意味論は呼び出し側で充足**: サーバ側 statement を閉じない仕様（doc 明記）に対し、
  呼び出しは pool resetQuery 成功後（pg_pool.nim:348、外部リセット直後）と reconnectInPlace
  （notify.nim:66、新サーバセッション）のみ → サーバ側リークなし。
- **transaction_helpers の同期維持**: pumpUntilReady は ReadyForQuery 到達後に queryError を raise するため
  （buffer_io.nim:447-448）、エラー時もバッファに残置メッセージが無い。ROLLBACK は
  `txStatus == tsInFailedTransaction` 時のみ（BEGIN 失敗＝呼び出し側の外側トランザクション内では
  ROLLBACK しない、正しい）、CancelledError は generic catch より先に re-raise（asyncdispatch では
  CancelledError <: CatchableError のため順序が必須）。
- **phase 追跡のエッジ**: コメントのみの user SQL は EmptyQueryResponse で phase を進めるため、
  後続 COMMIT の CommandComplete が user タグとして捕捉されない（74-78行、コメント済み設計）。
  遅延制約違反など COMMIT 自体の失敗は queryError として伝播し ROLLBACK 経路に入る。
- **type_lookup の注入耐性**: 型名は `$lt$...$lt$` ドル引用に埋め込むが、安全文字集合は
  `[A-Za-z0-9_."]` で `$` を拒否 → 引用終端注入不可。`"` はドル引用内ではデータで不活性。
- **ハブの export はドキュメントと一致**: pg_client.nim は core から 8 シンボルのみ selective export
  （doc に「internal helpers は公開 API でない」と明記、実際の core 公開シンボル一覧と一致）。
  async_postgres.nim は import/export 12 モジュールで対称。トップハブは chronos で実コンパイル確認
  （query / execInTransaction / lookupTypeOids の横断使用）。
- **pg_types.nim の accessor 生成**（accessorPair/arrayPair/elemOptPair/rangeFamily）: 名前衝突は
  コンパイルで検出される（ハブ自体が両バックエンドでビルド成功）。delegation を
  accessors.nim + ranges.nim 双方に見えるようにする意図的 binding（8-10行コメント）は妥当。

---

- 分類: 公開 API 面 / 名前空間（doc にリンクされるサブモジュールハブの単独 import 時）
- 重大度: Low
- 確信度: 確定（実コンパイルで再現）
- 場所: pg_connection.nim:49-54 / pg_client.nim:65-82 / pg_types.nim:3-6
- 事象:
  サブモジュールハブ単独 import では、公開シグネチャに現れる基盤型が名前解決できない。
  - `import async_postgres/pg_connection`: `BackendMessage` / `Row` / `RowData` / `bmk*` / `psIncomplete`
    （pg_protocol 定義）が不可視。`conn.recvMessage()` の戻り値は推論で使用可能だが、型注釈・
    `msg.kind == bmkXxx` 比較は追加 import なしでは書けない。
  - `import async_postgres/pg_client`: さらに `PgConnection` / `QueryResult` / `ResultFormat` / `PgParam`
    （pg_connection・pg_types 定義）も不可視。
  - `import async_postgres/pg_types`: `Row` / `RowData`（pg_protocol 定義）が不可視。
- 根拠: 3 つの最小テストを実コンパイルし、型注釈が undeclared で失敗することを確認
  （`var msg: BackendMessage`、`var qr: QueryResult`、`proc main(row: Row)`）。トップハブ
  `import pkg/async_postgres` では全シンボル到達可能（examples 14/14 と CI で確立済み）。
- 緩和: `import async_postgres/pg_protocol`（または pg_connection）の 1 行追加で解決。
  `PgProtocolError` は `PgConnectionError <: PgError` 階層のため、例外捕捉はハブ単独でも
  `except PgError` で機能する（pg_connection は pg_errors を再 export、53行）。
- 対称性の欠如: pg_types ハブは pg_bytes / pg_errors を意図的に再 export する（core.nim:3-4,6-7 経由）が、
  pg_protocol は再 export しない。意図的な名前空間抑制か見落としかは doc に記述が無く判定不能。
- 実害: 低。公開エントリ（トップハブ）は完全、値の使用は推論で可。型名を必要とするのは
  低レベル API（recvMessage 系）の型注釈・bmk* 比較・シグネチャ宣言のみ。
