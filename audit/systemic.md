# 横断分析（系統所見）

個別 findings（`.audit/findings/*.md`）を根本原因でクラスタリングし、リポジトリ全体の実数を付与した。
単発の Low/Medium は破棄し、系統性で昇格させたものを採用。

---

## クラスタ S1. 捕捉不能 Defect が PgError エラー契約を抜ける（付録C#5）

**根本原因**: 境界強制（範囲外・overflow・OOM）を、catchable な `PgError` ではなく uncatchable な `Defect`
（`IndexDefect`/`OverflowDefect`/`OutOfMemDefect`）で実装している箇所が全面に分布する。`raises: []` 契約
（本番57・テスト64、計121）は CatchableError のみ抑制し Defect は抑制しない。この乖離をコード自身が認識
（`pg_bearssl.nim:31` "Defect leaks past raises: [] into C (UB)"）。にもかかわらず本番の実行時防御 assert は
実質0件（静的2のみ）。利用者は `except PgError`/`except CatchableError` で失敗を捕捉できると期待するが、
これらの経路ではプロセスが致命的に落ちる（`-d:release` では OOB 読み取り＝UB）。

**検証済み実数（残存）**: 0 箇所（2026-08-13 対処済み。`except Defect` を並列追加し unlock/loClose
後 re-raise、回帰テスト 2 件追加。詳細は report.md「対処済み」を参照）。

**系統性**: 主要な accessor / query 経路に加え advisory_lock / largeobject の残存 2 件も解消。解消済み。
**raises 契約の実検証（2026-08-13）**: `raises: []` 実 proc 宣言 10 件の Defect 生成操作を網羅列挙。
全件が `try/except Exception` 遮蔽（Defect <: Exception のため捕捉される）か Defect 生成操作なし。
pg_bearssl cdecl 2 件のみ `int(len)` 変換が Defect 候補だが、いずれも `high(int)` 事前ガード付き。
リーク経路 0 件。詳細は `.audit/findings/raises_contract.md`。strictDefects（Nim 3.x）での機械再検証は留保。

---

## クラスタ S2. テキスト/バイナリ格式の非対称 → 沈黙のデータ損傷

**根本原因**: 型システムがテキスト/バイナリの2ワイヤ形式を**並行した別実装**で持つ（accessors 40分岐、
parse/decode 21対、encode 45）。この重複面自体は設計特性だが、両経路で**検証の厳密性と正規化が乖離**し、
同一値が形式によって異なる結果になる。バイナリ経路は厳密（raise）なのにテキスト経路は黙受、またはその逆。

**残存**: 0件（全 10 箇所が対処済み、High → 解消）。

---

## クラスタ S3. asyncdispatch/chronos キャンセル非対称（asyncdispatch が既定）

**根本原因**: `async_backend.nim` が2バックエンドを抽象するが、asyncdispatch（**既定**、async_backend.nim:9）には
真正キャンセルが無く `cancelAndWait` は no-op（193-204）。chronos はキャンセル可能。この意味差の吸収は
全経路で規律化済み。

**残存**:
- **抽象の漏れ出し**（async_backend 所見2,3、Low）: chronos `export` による wait オーバーロード解決の非対称（未文書）、
  toMilliseconds sub-ms 切り捨て（asyncdispatch のみ即タイムアウト、2箇所）。
- **3引数 wait ラッパの chronos void コンパイル不能**（2026-08-13 観測、Low）: `return await chronos.wait`
  の void 特化が chronos asyncmacro の result 曖昧化でコンパイル不能。本番は2引数形式のみで潜伏。
- **Moment 減算の負値クランプ**（2026-08-13 観測、Low）: chronos は負を 0 にクランプ、asyncdispatch は
  符号付き。本番使用は正値側のみで実害なし。
- **クライアント cert/key 不一致検出の非対称**（2026-08-13 観測、Low）: asyncdispatch は
  check_private_key で fail-closed、chronos はサーバがクライアント認証を要求しない限り黙って成功。

**系統性**: 主要な systemic 経路（transaction / fillRecvBuf / TLS / 証明書診断 / finally:await）は全対処済み。High → 解消。

---

## クラスタ S4. 確保増幅 / 無制限蓄積 DoS

**根本原因**: サーバ制御の長さフィールドが駆動する確保に**集約上限**が無い。`DefaultMaxBackendMessageLen`（1 GiB）
は recvBuf 成長のみを上限化し、parse 時のヒープ確保を上限化しない（doc は OOM 防止を明言するが recvBuf についてのみ成立）。

**残存**:
- `copyOutImpl`（copy.nim:476）の無制限蓄積: per-message cap（1 GiB）あり、aggregate cap なし。バッファ型は緩和なし。
- encode 側のガード前確保: **2系統**（toBytes core.nim:1083＝テキスト encoder 約15共用、encodeJsonbBinary encoding.nim:1011）。
  下流ガードが捕捉するが一時约2倍メモリ。

**系統性**: 2箇所（copy 蓄積 + encode 2系統）。Medium。

---

## クラスタ S5. リソースリーク / 状態機械の隙間

**根本原因**: 並行タスクの追跡・状態遷移の網羅に局所的な隙間。

**実数**:
- ~~pipeline の scsMiss 失敗 op の prepared statement リーク（pipeline.nim:541,663）~~: **対処済み（2026-08-13）**。
  失敗した statement の Close をキューに載せ回収（#589/#590）。
- ~~pool maintenanceLoop replenish の maxSize 超過（pg_pool.nim:570-597）~~: **対処済み（2026-08-14）**。
  in-flight replenish connect を `pool.active` で予約計上（予約規律の唯一の違反を解消）。
  競合テスト 3 件追加。詳細は `.audit/findings/pool.md` 所見1。
- ~~pool 孤立 connect close の asyncSpawn 未追跡（pg_pool.nim:971）~~: **対処済み（2026-08-13）**。
  `pendingBackgroundTasks` に登録して close() drain で待機（#590）。
- CopyDone race（copy.nim:133,381）: **対処済み（2026-08-14）**。前提を実証検証したところ、実 PG（9.1 以降）は
  COPY 失敗後の stray CopyData/CopyDone/CopyFail をプロトコル仕様に従い黙って無視する
  （postgres.c "Accept but ignore these messages"）ため desync は発生しないことをワイヤ実験で確認（所見の前提崩壊）。
  非標準サーバ対策として防御的硬化を実装: CopyDone 前の最終 poll（callbackError パスと対称化）+ エラー時の
  `drainLeftoverToReady`（残留違反応答の消費）+ モック回帰テスト 2 件（`tests/test_copy_race.nim`）。
  詳細は `.audit/findings/pipeline_copy.md`。

**系統性**: 4件全て対処済み（2026-08-13〜14）。CopyDone race は実サーバでは発生しないことを実証（前提訂正）しつつ、非標準サーバ対策の硬化を追加。

---

## クラスタ S6. テストの構造的空白

**根本原因**: 基盤層・キャンセル経路・malformed 拒否・実TLS にテストが構造的に存在しない。

**残存**:
- ~~`async_backend.nim`: fan-in **26**（全模块2位）に対し専用テスト **36行**~~: **対処済み（2026-08-13）**。
  wait/asyncSpawn/allFutures/cancelAndWait/sleepAsync/cancelTimer/scheduleSoon/registerFdReader/
  completed/Duration/Moment/remainingDeadlineDuration/makeAsyncSinkByteCallback に専用テストを追加
  （asyncdispatch 24件・chronos 21件、両バックエンドで成功）。
- 実 TLS 統合テスト: driveTlsHandshake エラー分岐/assertAlpnPostgres エラーパス/暗号化鍵拒否/成功パスがモック到達不能
  （tests/test_ssl_coverage.md が列挙、RELEASE_TODO T6）。asyncdispatch TLS 下限 (TLS 1.2+ 強制) の
  downgrade 拒否、chronos verify-full + IP-literal host 事前診断、chronos require での期限切れ証明書拒否も
  同 doc に追記済み。
  - **対処済み（2026-08-13、T6 の大半）**: `tests/test_tls_error_paths.nim` を新設。
    - クライアント cert/key/CA 読込失敗分岐（CA ゴミ / cert ゴミ / key ゴミ / 暗号化鍵拒否 / cert-key
      不一致）— モック 'S' 応答のみで establishTls のロード段階まで到達、両バックエンド 5 テスト。
    - driveTlsHandshake の peer close 分岐（asyncdispatch）+ chronos の handshake EOF。
    - assertAlpnPostgres エラーパス（「without ALPN」）— openssl s_server（ALPN 非広告、PG<17 相当）で
      両バックエンド実証。chronos は BearSSL クライアント × OpenSSL サーバの実本番構成。
  - **残（到達不能/非現実的）**: TLS 1.0 ダウングレード拒否（modern OpenSSL に TLS1.0 peer が存在しない）、
    BIO_read/write 内部失敗、SSL_CTX_set_alpn_protos の dynlib nil 分岐。
- pg_bearssl の asyncdispatch レグ未実行（chronos 専用）。

**系統性**: 基盤層ほど専用テストが薄い傾向。Medium（構造）。

---

## クラスタ S7. エラー契約・文書の不一致（付録C#5）

**根本原因**: 利用者が見る doc/型と実装の乖離。

**実数**:
- `PgPoolError` の判別不能（pg_errors.nim:100）: **14箇所・5種以上**の意味的に異なる失敗が同一型・文字列のみ。
  リトライ可否のプログラム的判別不可能。
- cleanup doc の "reporting both" 虚偽（transaction.nim:126,161）: コードは csrConnInvalidated のみ報告、
  tsIdle スキップは無音。`CleanupSkipReason` に該当コード無く実装不可能。**2箇所**。
- doc 矛盾: encoding「Mirror decodeNumericBinary」（虚偽、decoder に検査無し）。

**系統性**: 3系統。Medium/Low。

---

## クラスタ S8. 依存/ビルド/リリースの再現性（付録C#2,3,4）

**根本原因**: 依存の固定と変更履歴の管理が弱い。

**実数**:
- `chronos >= 4.4.0` と `nim-bearssl >= 0.2.11` は README 言及・コードで import されるが **nimble 未宣言**。
  CI は `nimble install chronos -y` で**無固定**インストール。
- nimble 依存は全て `>=` の下限のみ・**上限なし**（nim/nimcrypto/checksums/unicodedb/normalize）。
- **CHANGELOG ファイル無し**。HEAD は v0.3.0 から **555コミット先行**（約2ヶ月未リリース）。
  破壊的変更の追跡は git log/PR のみ。0.4.0 計画は未追跡の reviews/RELEASE_0.4.0_TODO.md。

**系統性**: 再現性・互換性リスク。Medium。

---

## 地図由来の構造的所見

- **循環依存・レイヤ違反: なし**（依存グラフは DAG、良好）。
- **fan-in 集中**: pg_protocol **28**, async_backend **26**, pg_typesハブ 21, pg_connectionハブ 17, pg_errors 13。
  基盤2模块の変更が全体に波及。
- **複雑度ホットスポット**: pg_pool.nim **2310行**（最大）× 最高変更頻度（103）× 巨大 proc 2件（notify 437, acquireImpl 240）
  × inline 負債マーカー0（問題がコードに見えない）。S5 の所見がここに集中。
- **負債管理は外部文書**: inline マーカー0、既知問題は reviews/（14ファイル）+ 未追跡 RELEASE_0.4.0_TODO.md に集約。
  規律である一方、コードから負債が見えない。

## 良好な点（壊してはいけない設計判断）

- 依存グラフが清潔（循環・レイヤ違反なし）、re-export ハブ構成が整然。
- **pg_sql の注入耐性が実測で堅牢**（全リテラル形式: 単一/E/ドル/タグ付きドル/二重引用/行・ネストブロックコメントで
  `{expr}`/`?` を正しく扱い、リテラル内は抽出しない）。
- **MITM 防御が両バックエンドで fail-closed**: CA ピン留め、hostname 検証、pre-TLS インジェクション検知（CVE-2021-23214 系）、
  ALPN 強制、require 以上の平文 fallback 拒否、一時 PEM の 0600+O_EXCL。
- **SCRAM 実装が防御的**: iteration cap（<4096 / >10M 拒否、libpq より厳格）、SASLFinal 必須検証、channel binding downgrade 検出。
- **int32 長ラップガードが encoder に全適用**（内部プレフィックス11箇所全て、適用漏れ0）。
- **バイナリデコーダ群が over-read/巨大確保に一貫硬化**（count 事前抑止 + 要素毎 pos 検査）。
- 実秘密のコミット無し、examples 14件が現 API と完全整合、quoteIdentifier が libpq 等価。
- 強いテスト文化（42,157行、両バックエンド実行、protocol fuzz、network failure）。
- pumpUntilReady 3種のエラー/キャンセル処理が完全対称、closeTransport 冪等。
