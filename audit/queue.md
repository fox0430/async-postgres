# 調査キュー（リスク優先順位）

優先度シグナル: 変更頻度（ホットスポット）× 信頼境界 × fan-in（影響半径）× テスト空白 × fixコミット集中。

> 完了履歴: Tier1 12 モジュール / Tier2 18 グループは調査完了（所見は `.audit/findings/`・`systemic.md`・`report.md` 参照）。
> S1 残存 2 箇所（advisory_lock / largeobject の Defect 漏れ）は対処済み。
> Tier3 残部は 2026-08-13 に調査完了。監査範囲 100%。
> 残存所見の修正は継続中: S5 は全件対処済み（CopyDone race は 2026-08-14 対処。実サーバでは発生しないことを実証しつつ防御的硬化を追加）。

## Tier 1（深掘り：付録D全観点、関数レベル）

## async_postgres/pg_types/core.nim
- Tier: 1
- リスク根拠: 型システム基底、fan-in 7。OID/型判定/バイナリ安全テーブル。1096行。
- 状態: 完了（所見は encoding.md / decoding.md / sql_accessors.md / transaction.md に分散記録。専用ファイル無し）
- 所見ファイル: .audit/findings/encoding.md ほか

## Tier 3（機械的スキャンのみ）

## ハブ群 (async_postgres.nim, pg_client.nim, pg_connection.nim, pg_types.nim) — Tier 3 — re-export 専用 — 完了
## async_postgres/pg_connection/cache.nim + type_lookup.nim — Tier 3 — 小規模、e2e間接のみ — 完了
## async_postgres/pg_client/transaction_helpers.nim — Tier 3 — 224行 — 完了
## examples/ — Tier 3 — 14サンプル、現API一致性（付録C#8） — 完了（mechanical.md シグナル7、不一致 0 件）
## tests/ 全体 — Tier 3 — 機械的シグナル（巨大ファイル、TODO分布） — 完了（mechanical.md シグナル1/5/2 ほか）

### 完了時所見（2026-08-13）: ハブ群 + cache/type_lookup + transaction_helpers は欠陥なし。
サブモジュールハブ単独 import 時の基盤型（BackendMessage/Row/PgConnection/QueryResult 等）名前空間の
不完全性 1 件（Low、`.audit/findings/tier3_hubs_remaining.md`）のみ。

## 機械的スキャン対象（Tier 3 横断）
- TODO/FIXME/HACK/XXX の分布と最古
- 抑制コメント（{.push raises.} 違反、pragma）の分布
- 極大ファイル/関数
- ハードコード資格情報/URL/パス（→ 実秘密は検出なし、確認済）
- コピペ重複ブロック
