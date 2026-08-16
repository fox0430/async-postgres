# コードベース監査報告

## 対象
- **種別**: ライブラリ / SDK（`async_postgres.nimble` v0.3.0・MIT、nimble 公開、`examples/` 14件、`async_postgres.nim` が公開ハブ）
- **規模**: ソース41ファイル＋ハブ 約26,516行 / テスト42,157行 / 主要言語 Nim（>= 2.2.4）、asyncdispatch（既定）/ chronos 両対応
- **監査範囲**: Tier1 **12**モジュール（深掘り・全観点） / Tier2 **18**（限定：セキュリティ・公開API・依存） / Tier3 **残部**（機械的スキャン）— **100% 完了（2026-08-13）**
- **未調査領域**: raises pragma の実コンパイル不一致検証、各 e2e テストの全文精読（対象モジュールとの突合は import ベース）、GitHub Actions の実際の実行履歴（リモート未アクセス）、htmdocs/ 内容

## 総評
成熟度が高く、規律の行き届いたコードベースである。依存グラフは清潔（循環依存・レイヤ違反ゼロ）、
SQL 注入耐性・MITM 防御・SCRAM 実装はいずれも実測で堅牢、int32 長ラップガードは encoder に全適用、
42K行のテストが両バックエンドで走る。138件の fix コミットが示す通り、malformed input 拒否の硬化が
継続的に進んでいる。asyncdispatch/chronos の抽象非対称は systemic に解消済み（キャンセル欠如を
吸収する finally:await パターンを全経路で規律化）。テキスト/バイナリ二重実装の契約乖離
（**沈黙のデータ損傷**）も全 4 箇所を対処済み。
残る計画的対応は、基盤層のテスト空白、依存固定/CHANGELOG/リリース運用の 2 系統。

---

## 計画的対応

### 5. テストの構造的空白の解消（S6）
- **重大度**: Medium（構造）
- **出現**: async_backend 専用テスト **36行**（fan-in 26）／実 TLS 統合テスト未カバー
- **影響**: 基盤層ほど専用テストが薄く、系統缺陥の潜伏性を高める。
- **修正方針**: async_backend の意味乖離テスト、実 TLS 統合テスト（RELEASE_TODO T6）を追加。
- **修正コスト**: 中
- **前提**: なし
- **状態: 対処済み（2026-08-13）**: async_backend 専用テストを約250行に拡充（両バックエンドで
  asyncdispatch 24 / chronos 21 テスト成功）。`tests/test_tls_error_paths.nim` 新設で TLS エラーパス
  （cert/key/CA 読込失敗 5 種、peer close、ALPN 欠如）をモック+openssl s_server で実証（両バックエンド）。
  詳細は `.audit/findings/async_backend.md` 追記と `tests/test_ssl_coverage.md`。残は TLS 1.0 ダウングレード
  拒否（TLS1.0 peer が現存しない）等の非現実的経路のみ。

### 6. 依存固定・CHANGELOG・semver 運用（S8）
- **重大度**: Medium
- **出現**: chronos/bearssl **nimble 未宣言**（CI 無固定インストール）／依存全て上限なし／CHANGELOG なし／v0.3.0 から **555コミット先行**
- **影響**: 利用側のビルド再現性・互換性リスク。破壊的変更の追跡手段が git log のみ。
- **修正方針**: chronos/bearssl を nimble に条件付き宣言、CI のバージョン固定、CHANGELOG 導入、0.4.0 リリース。
- **修正コスト**: 局所（運用整備）
- **前提**: なし

---

## 対処済み（計画的対応 #2 = S1 残存分、2026-08-13）

- **修正**: `pg_advisory_lock.nim`（withAdvisoryLockCore）/ `pg_largeobject.nim`（withLargeObject）の
  `except CatchableError` に `except Defect` を並列追加。body 内 Defect でも unlock / best-effort
  loClose を実行し、その後 Defect を re-raise（transaction.nim の慣習パターンと一致）。
- **回帰テスト**: `withAdvisoryLock releases on Defect` / `withLargeObject closes on Defect` を追加
  （両 async バックエンドで実行確認済み）。
- **注意**: `except Exception` 一括捕捉は chronos バックエンドの raises 推論で
  `raise ref Exception` が unlisted となりコンパイル不可。静的型を明確にするため
  CatchableError / Defect の 2 分岐方式が必須。

---

## 記録のみ

- **S5 リソースリーク/状態機械の隙間**: pool replenish の maxSize 超過（pg_pool:570-597）、pipeline scsMiss の
  prepared statement リーク（pipeline:541,663）、pool 孤立 connect close の asyncSpawn 未追跡（pg_pool:971）は
  3件とも対処済み（2026-08-13〜14、回帰テストあり）。CopyDone race（copy:133,381）は **2026-08-14 対処済み**:
  実 PG（9.1 以降）が COPY 失敗後の stray CopyDone/CopyFail を黙って無視することをワイヤ実験 + PG ソース
  （postgres.c "Accept but ignore these messages"）で実証し所見の前提（違反応答による desync）を訂正。
  非標準サーバ対策として CopyDone 前の最終 poll（callbackError パスと対称化）と残留違反応答の
  `drainLeftoverToReady` を実装、モック回帰テスト `tests/test_copy_race.nim` を追加（両バックエンド）。
  詳細は `.audit/findings/pipeline_copy.md`。
- **S7 エラー契約・文書の不一致**: PgPoolError の判別不能（14箇所・5種以上、文字列のみ）、cleanup doc の
  "reporting both" 虚偽（transaction:126,161）。詳細は `.audit/findings/advisory_lo_cluster.md`・`transaction.md`。
- **32-bit 硬化の不完全性**: pg_protocol decode 側の int64 未拡張 **4箇所**（856,1122,1156,1207）。64-bit では実害なし。
- **複雑度ホットスポット pg_pool**: 2310行×最高変更頻度×巨大 proc 2件。inline 負債マーカー0（問題がコードに見えない）。
- **SASLprep テーブル帰属誤り**（pg_saslprep.nim:26,37,115-118）: U+200B/U+200C/U+200D の3 code point が PostgreSQL と
  異なる正規化を受け、該当文字を含むパスワードで認証失敗の可能性。単発・低頻度。
- **サブモジュールハブの型名前空間（2026-08-13、Tier3 完了時）**: `import async_postgres/pg_connection` /
  `pg_client` / `pg_types` 単独では公開シグネチャの基盤型（BackendMessage/Row/RowData/bmk*/PgConnection/
  QueryResult 等）が名前解決不能（実コンパイルで確認）。トップハブは完全、値は推論で使用可、`except PgError`
  は機能するため Low。詳細は `.audit/findings/tier3_hubs_remaining.md`。

---

## 構造的所見
- **依存グラフは清潔**: 循環依存・レイヤ違反ゼロ（DAG）。re-export ハブ構成が整然。
- **fan-in 集中**: pg_protocol **28**、async_backend **26**（上位2）。基盤変更の波及が大きい。
- **重複面の契約リスク**: テキスト/バイナリ二重実装（accessors 40分岐・parse/decode 21対・encode 45）は設計特性だが、
  S2 の乖離を生む温床。pumpUntilReady 3種・withTransaction マクロ族14は意図的・文書化済み。
- **負債管理は外部文書**: inline マーカー0、reviews/（14ファイル）+ 未追跡 RELEASE_0.4.0_TODO.md に集約。規律である一方、
  コードから負債が見えず、本監査で既存レビューの誤りも2件確認（review_decoding.md の fracUs デッドコード誤判定、
  review_encoding.md の一部は修正済み）。
- **CI/リリース**: 両バックエンド・Nim 3バージョン・実 PostgreSQL で堅牢。一方 OS=ubuntu のみ・PG=18 のみ・
  chronos/bearssl 無固定・dependabot なし。

## 良好な点
- **pg_sql の注入耐性が実測で堅牢**: 全リテラル形式（単一/E/ドル/タグ付きドル/二重引用/行・ネストブロックコメント）で
  `{expr}`/`?` を正しく扱い、リテラル内は抽出しない。
- **MITM 防御が両バックエンドで fail-closed**: CA ピン留め、hostname 検証、pre-TLS インジェクション検知（CVE-2021-23214 系）、
  ALPN 強制、require 以上の平文 fallback 拒否、一時 PEM の 0600+O_EXCL。
- **SCRAM 実装が防御的**: iteration cap（libpq より厳格）、SASLFinal 必須検証、channel binding downgrade 検出、burnMem 網羅。
- **int32 長ラップガードが encoder に全適用**（内部プレフィックス11箇所、適用漏れ0）。バイナリデコーダ群も over-read に一貫硬化。
- 実秘密のコミット無し、examples 14件が現 API と完全整合、quoteIdentifier が libpq 等価。
- 強いテスト文化（42,157行、両バックエンド、protocol fuzz、network failure）、pumpUntilReady 3種の完全対称、closeTransport 冪等。

## 確認したい点
1. **SASLprep テーブル帰属誤り**: 該当3 code point を含むパスワードの実在頻度が分かれば影響確定。
2. ~~**raises pragma の実コンパイル不一致**~~: **解決（2026-08-13）**。`raises: []` 実 proc 宣言 10 件を全数
   精査し Defect 生成操作を網羅列挙。全件遮蔽（except Exception / ガード付き変換）でリーク経路 0 件。
   詳細は `.audit/findings/raises_contract.md`。strictDefects は利用可能コンパイラに存在せず
   （2.0.16〜2.3.1）、Nim 3.x 系での機械再検証のみ留保。
3. **pg_bearssl の asyncdispatch レグ**: CI で実行されていない（chronos 専用）。意図的か未確認。

---

## 監査範囲の申告
- **深掘りしたモジュール（Tier1）**: pg_protocol, pg_types/{decoding,encoding,ranges,core}, pg_connection/buffer_io,
  pg_auth(+saslprep), pg_connection/dsn, pg_connection/ssl(+bearssl), pg_pool, pg_client/transaction, async_backend
- **限定調査（Tier2）**: pg_client/{core,query,exec,prepared,cursor,direct,pipeline,copy}, pg_connection/{lifecycle,
  simple_query,notify,types}, pg_replication, pg_advisory_lock, pg_largeobject, pg_sql, pg_types/{accessors,array,user_types},
  pg_pool_cluster, pg_errors, pg_bytes
- **機械的スキャン（Tier3）**: ハブ群、cache/type_lookup、transaction_helpers、examples、tests 全体 — **全て完了**
  （mechanical.md + tier3_hubs_remaining.md。監査範囲 100%）
- **未調査の領域とその理由**: e2e テスト全文精読（対象モジュール突合は import ベースで実施）、
  CI 実行履歴（リモート未アクセス）、htmdocs/、strictDefects による Defect 効果の機械検証
  （利用可能コンパイラに機能が存在しないため手動網羅で代替、raises_contract.md 参照）。
- **検証で破棄した所見**: 0件（主要4所見は全て確認。S1 の `-d:release` OOB フレーミングのみ `-d:danger` 限定へ訂正）
- **クラスタ化されず破棄した単発の軽微な指摘**: 約15件（direct マクロの int16 防護迂回、flattenInline の int32 溢出、
  PgParamInline 自己起因過読み、parseTimeTzText オフセット未検証、parseIntervalText 時刻部重複 等、
  各 findings の「調査したが所見としなかった項目」に記録）
- **推定した暗黙の慣習（AGENTS.md/CLAUDE.md なし）**: 単一例外階層 PgError（pg_errors.nim）、async_backend の
  hasChronos/hasAsyncDispatch/hasTls 分岐、`##` RST doc、pg_ snake_case ファイル/PascalCase 型/camelCase proc、
  re-export ハブ構成、PgTracer nil スキップ型フック（観測: async_postgres.nim, pg_errors.nim, async_backend.nim, pg_connection/types.nim）。
  nph フォーマッタを CI で強制（設定ファイルなし・デフォルトスタイル）。
