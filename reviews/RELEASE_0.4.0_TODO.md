# 0.4.0 リリース前 要修正項目 (次候補)

前バージョンの blocker 6件と補足 (`decodeCreateSlotRow` private 化) はマージ済み
(2d3ed11, 365edff, 3c6f486, 44fa0cf, 88c680a)。本ファイルは reviews/ 配下 14 ファイル
+ tests/review_rowdata.md / tests/test_ssl_coverage.md を再走査し、
main (04911c8) 時点で未対応の項目を再抽出したもの。

## コード修正 (優先度順)

| # | 箇所 | 概要 | 深刻度 | 根拠 |
|---|------|------|--------|------|
| 5 | `async_postgres/pg_connection/buffer_io.nim:169` (`compactRecvBuf`) | `csClosed` 以降呼ばれない不変条件が防御コードで担保されていない。`doAssert conn.state != csClosed` 追加で将来の破壊を防ぐ | Low (precautionary) | buffer_io_review.md #2 |

## 既に対応済み

- **#1 `async_postgres/pg_types/ranges.nim` `parseMultirangeText`** — c8fa5eb
  (PR #569) で main へマージ済。カンマ後の空白スキップを実装し、空 range 分岐にも
  同スキップを追加、line 727-728 の dead `discard` 分岐を削除。`tests/test_types.nim`
  に "parse multirange with spaces after comma" / "parse multirange with empty
  and spaces" を追加。REVIEW_ranges.md #1, #2 は close 済。
- **#2 `async_postgres/pg_protocol.nim:1022-1024, 1035-1039`** `reuseRowData`
  両オーバーロードの doc コメント修正は 04911c8 (PR #570) で main へマージ済。
  "left intact" 記述と存在しない `QueryResult` 参照を削除し、`rd` は valid ref
  のままだが `buf`/`cellIndex` (第2オーバーロードでは加えて `colFormats`/
  `colTypeOids`) が空になる旨を明記。pg_protocol_review.md #1 は close 済。
- **`async_postgres/pg_pool.nim:354-358`** `resetSession` の `CatchableError` ハンドラは
  現在 `conn.state = csClosed` のみ設定する対称パターンで実装済。TODO 初期リストの
  想定 (`tracedClose` 明示呼び出し) は解消されている。pg_pool_review.md #1 は close 済。
- **#3 `async_postgres/pg_types/encoding.nim:1083-1098`** `toPgBinaryParam(PgPath/PgPolygon)`
  に payload サイズガードを追加 (branch `fix/pg_types-path-polygon-binary-payload-guard`、
  27c7cd4、未 PR)。当初の TODO では「他エンコーダは `checkPgBinLen` 適用済で不整合」と
  記載していたが、実調査では `checkPgBinLen` を使うのは `encodeBinaryArray` /
  `encodeHstoreBinary` のみで、fixed-width element を扱う `buildFixedArray` は
  payload-level ガード (`if payload > int32.high.int64: raise`) を採用していた。
  PgPath/PgPolygon も element size 固定 (16 bytes) のため後者と同じスタイルが
  適切。`checkPgBinPayload(size, "path"/"polygon")` を int64 演算で挿入し、
  `newSeq[byte](size.int)` で 32-bit プラットフォームでの `points.len * 16`
  int wrap も同時にブロック。point-count の int32 キャストは payload 上限
  (16*n ≤ int32.high) で自動的に包含。既存の PgPath/PgPolygon happy-path テスト
  および roundtrip テストは全 pass、リグレッションなし。review_encoding.md #2 は close 済。
- **#4 `async_postgres/pg_client/transaction.nim` (非リトライ版 `withTransaction`)** — 現ブランチ
  `fix/withtransaction-cancel-cleanup` の f2867ac で実装済 (未 PR)。conn 版 `withTransaction` /
  `withSavepoint`、pool 版 `withTransaction` / `withTransactionDeadline` /
  `withTransactionRetryDeadline` の各 body try に `except CancelledError as e: raise e` を追加し、
  `buildRollbackCleanup` / `buildSavepointRollbackCleanup` / `buildDeadlineAwaitAndTimeout` の
  cleanup-cancel は swallow して原因例外を再送出する対称形に統一。回帰テスト
  `tests/test_transaction_cancel.nim` (chronos-only、3 テスト) を追加。review-transaction.md
  Issue 2 は close 済。

## ドキュメント修正 (同梱推奨)

| # | 箇所 | 概要 | 根拠 |
|---|------|------|------|
| 6 | `async_postgres/pg_advisory_lock.nim` `withAdvisoryLockCore` doc | body 内 `return`/`break`/`continue` で unlock がスキップされる制約と、実効最大待ち時間が `2 * timeout` になり得る旨を追記 | pg_advisory_lock_review.md #1, #2 |
| 7 | `async_postgres/pg_replication.nim` `readReplicationSlot` doc | `outputPlugin` は `READ_REPLICATION_SLOT` 返却列に含まれず常に空である旨を追記 | pg_replication_review.md #2 |
| 8 | `async_postgres/pg_pool.nim` `close()` doc | asyncdispatch の `cancelAndWait` no-op により、`close()` 返回後にメンテナンスループ由来の close タスクが完走する可能性を追記 | pg_pool_review.md #2 |
| 9 | `async_postgres/pg_client/copy.nim` `copyInStream` doc | 送信サイズ上界がコールバックのチャンクサイズに依存する旨を追記 (`copyInRaw` は 256KB 固定) | pg_copy_review.md #1 |

## テスト整備 (0.4.0 スコープ外でも順次)

| # | 対象 | 概要 | 根拠 |
|---|------|------|------|
| T1 | `tests/test_rowdata.nim` | `parseDataRowInto` の "message too short" / 負の col count / "unexpected end" / "invalid column length" / "truncated" 各エラー分岐 + ロールバック検証。`buildResultFormats` 直接テスト。0列 DataRow (`body = [0x00, 0x00]`) の境界テスト | tests/review_rowdata.md |
| T2 | `tests/test_rowdata.nim` line 180, 199 | テスト名リネーム (`"remains intact after reuse"` → `"reuse moves buffers out"` 等)。line 291-312 の ~2 GiB 確保テストは CI メモリ制約次第で調整 | tests/review_rowdata.md |
| T3 | `tests/` 新規 | 空 PEM ファイル (`dsn.nim:242-243` の `result.len == 0` ガード) の拒否テスト。`initConnConfig` の SSL cert 検証は test_ssl.nim/test_pool.nim で部分的にカバー済のため未カバー中核はこの1点 | review_dsn.md 未カバー欄 |
| T4 | `tests/` 新規 | encoding / decoding / ranges / DSN / SQL 向け fuzz テスト (現状 `test_protocol_fuzz.nim` はプロトコルのみ) | rest_review.md |
| T5 | `tests/test_types.nim` (9062 行) | encoding / decoding / ranges への分割リファクタ。境界の曖昧さでギャップが埋もれる | rest_review.md 構造的問題 |
| T6 | ~~`tests/test_ssl.nim`~~ | ~~実 TLS 統合テスト~~ → **対処済み (2026-08-13)**: `tests/test_tls_error_paths.nim` 新設。cert/key/CA 読込失敗・暗号化鍵拒否・cert-key 不一致 (asyncdispatch)・peer close・ALPN 欠如を実証 (両バックエンド)。残は TLS 1.0 ダウングレード拒否 (peer 構築不能) 等の非現実的経路のみ | tests/test_ssl_coverage.md |

## 対応不要 (再確認)

- **review_decoding.md #1** `parseIntervalText` years 境界 — 実測で off-by-one ではないことを確認済。Nim の `div` は truncated (toward zero) のため `int64(int32.low) div 12 = -178956970` / `int64(int32.high) div 12 = 178956970` となり、`val = ±178956970` は `val * 12 = ±2147483640` で int32 に収まり、`val = ±178956971` は `val * 12 = ±2147483652` で int32 を超え reject。境界の check_passes と fitsI32 は完全一致。加えて 12 は int32.high/int32.low の約数ではないため `val * 12` が `[2147483641, 2147483647]` や `[-2147483648, -2147483641]` の値を取ることは原理的にない
- **pg_pool_review.md #3** `dispatchHomogeneous` の `CancelledError` 捕捉 — 現状 `asyncSpawn` fire-and-forget で発火経路なし。防御コメントで十分
- **pg_pool_review.md #4** `splitBatchBudget` cap 超過 — 意図的 (Info)
- **pg_advisory_lock_review.md #3** `ensureXactScope` メッセージ — Very Low (末尾に txStatus 表示あり)
- **pg_advisory_lock_review.md #4** raw/typed mixed unlock counter — 文書化・テスト済
- **pg_copy_review.md #2** `copyIn(seq[seq[byte]])` ピークメモリ — 設計上のトレードオフ、doc で `copyInStream` に誘導済
- **pg_protocol_review.md #2** `addBindRaw` int32 演算 — サーバ側 1 GiB 制限で実質発生不可
- **buffer_io_review.md #1, #3, #4, #5** — Low / doc 改善余地のみ
- **REVIEW_largeobject.md** — 全 Low、`loImport`/`loExport` はサーバ FS アクセスで意図的除外
- **REVIEW_ranges.md #3** — 機能的同等 (リファクタ余地のみ)
- **REVIEW_ranges.md #4, #5** — 放置可
- **review_dsn.md** バグなし
- **review_pg_sql.md** 全項目実害なし
- **rest_review.md #1 (test_ssl)** — 1dfc8ea で split-write MITM 追加済 (T6 は残る)

## リリース判断

- **#1 は c8fa5eb (PR #569) で main へマージ済。**
- **#2 は 04911c8 (PR #570) で main へマージ済。**
- **#3 は branch `fix/pg_types-path-polygon-binary-payload-guard` (27c7cd4) でパッチ準備済み。PR 化待ち。**
- **#4 は現ブランチ `fix/withtransaction-cancel-cleanup` (f2867ac) で対応済。PR 化待ち。**
- **#5 は低リスクだが防御性の向上。同梱推奨。**
- **#6〜#9 は doc のみ、変更差分小。同梱容易。**
- **テスト整備 T1〜T6 は 0.4.0 後の継続タスクでも可。ただし T1 (parseDataRowInto 決定的エラーテスト) は decoding 変更を安全にする観点で早期対応が望ましい。**

残るコード修正は #5 の 1 件のみで、いずれも局所的 (数行〜十数行)。パッチとテスト
追加は 1 リリースサイクル内に十分収まる規模。
