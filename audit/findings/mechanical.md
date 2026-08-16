# Audit Findings: Tier 3 機械的スキャン（横断分布・実数）

対象: `/home/fox/git/async-postgres` 全体（async_postgres/ 26,377行・41ファイル、tests/ 42,157行、examples/ 935行・14ファイル）。
目的: 個別バグではなく**分布と実数の収集**。横断分析のクラスタ化材料。
方法: rg/grep/wc/git + 読み取り専用スクリプト（関数長・未参照シンボル）。コード変更なし。

凡例: 実数 = 機械カウント / 分布 = ファイル別 / 代表3件 = path:line / 系統性 = クラスタ判定。

---

## シグナル1: TODO/FIXME/HACK/XXX コメント

- **実数**: ソース (.nim) の**実マーカー = 0**。
  - 大文字厳密一致 (`TODO|FIXME|HACK|XXX`): 0 件。
  - 大文字小文字無視で 5 件ヒットするが**全て偽陽性**: `getXxxArray` 関数名プレースホルダ
    (decoding.nim:317, test_types.nim:7954) とテストデータ文字列 `"xxx"` (test_rowdata.nim:80,85,231)。
- **分布**: ソースにマーカー無し。唯一の負債トラッカーは `reviews/RELEASE_0.4.0_TODO.md`
  （**git 未追跡** `??`、コミット履歴なし＝作業中文書）。`reviews/` 配下 14 レビューが負債管理の実体。
- **代表3件**: （実マーカー無しのため該当なし。偽陽性代表: async_postgres/pg_types/decoding.nim:317 `getXxxArray`）
- **最古 (git blame)**: **N/A** — 追跡ソースにマーカーが存在しないため時期特定不能。RELEASE TODO は未追跡で履歴無し。
- **系統性の判定**: 【文化シグナル】コードベースに inline 負債マーカーが**ゼロ**。負債は外部文書 (reviews/ + RELEASE TODO)
  で管理される規律。既存問題の所在はコード注释ではなく reviews/ と本監査 findings にある → 横断分析では
  「コメントに残らない既知問題」の一次ソースとして reviews/RELEASE_0.4.0_TODO.md を突合すべき。

---

## シグナル2: 抑制/契約 pragma と防御アサーション

### raises pragma
- **実数**:
  - `raises: []` 総出現 **121**（本番 async_postgres/ **57**、tests/ **64**）。
  - 本番の**実 proc 宣言**（`{.raises: [].}` 契約、コールバック型署名を除く）: **約13**。
  - `{.push raises` = **0**（push 形式は不使用）。
  - `{.async: (raises: [CatchableError]).}` = **6**（pg_replication:526, async_backend:313/327, pg_largeobject:62 他）。
  - `raises: [具体例外]`（非空・非 CatchableError）= **5**。
- **分布** (本番 `raises: []` 57 の内訳):
  - コールバック**型署名**が多数: pg_connection/types.nim に約30（Trace*/Notify/Notice/Reconnect callback 群）。
  - 実 proc 宣言の集中: pg_pool.nim (1146,1340,1346,1352)、async_backend.nim (46,70,252,273)、
    pg_connection/notify.nim (120,125,283)、pg_connection/buffer_io.nim (112,142)。
- **代表3件**:
  - async_postgres/pg_pool.nim:1146 `proc failAllPending(...) {.raises: [].}`（コメント: 「marked raises:[] so the compiler...」）
  - async_postgres/pg_connection/buffer_io.nim:112 `proc dispatchNotification*(...) {.raises: [].}`
  - async_postgres/pg_connection/notify.nim:120 `proc newListenError(...): ref PgListenError {.raises: [].}`
- **raises 不一致の疑い（機械的所見）**: `raises: []` は CatchableError のみ抑制し **Defect は抑制しない**。
  本コードベース自身がこの乖離を明文化: async_postgres/pg_bearssl.nim:31
  `# int(len) traps RangeDefect > high(int); Defect leaks past raises: [] into C (UB).`
  → `raises: []` を付けた本番 proc が配列アクセス/整数変換を含む場合、Defect リーク経路となる（シグナル3 と連結）。

### doAssert / assert（本番の防御アサーション）
- **実数**: 本番 `doAssert` = **2**（いずれも**静的/コンパイル時**）:
  - async_postgres/pg_sql.nim:371 `doAssert sqIdx >= 0, ...`（マクロ内、コンパイル時）
  - async_postgres/pg_connection/types.nim:710 `doAssert ord(sslnPostgres) == 0`（enum ordinal 表明）
  - 本番の裸 `assert(` = **0**。tests/ の `doAssert` = **2884**（テストでは多用、正常）。
- **系統性の判定**: 【中】本番コードに**実行時防御アサーションが実質ゼロ**。かつては境界検証が
  `raise`（catchable）と `cellInfo` の IndexDefect（uncatchable）に二極化していたが、シグナル 3a/3b の
  IndexDefect は `PgTypeError` に変換済み（対処済み）。残る `raises: []` × 防御 assert 欠如 × 一部 Defect
  リーク認識（bearssl:31）は**同一クラスタ**として横断分析に回す。

---

## シグナル3: 捕捉不能 Defect を投げうる危険パターン（横断カウント）

### 3a. cellInfo / isNull の IndexDefect（対処済み・削除）
accessors.nim:6-13 / :103-110 の `IndexDefect` を `PgTypeError` に変換済み。
cellInfo 経由の約50呼び出し、isNull、query.nim convenience の固定 col=0 アクセス
（query.nim:327, 361, 396, 459 の isNull(0)/getStr(0) 経路）はいずれも
`except PgError` で catchable。3b と統合して解消。

### 3b. 固定インデックス row.getStr(N)（対処済み・削除）
query.nim convenience 4 パターンに `numCols == 0` ガードを追加し、
0 列時は `PgTypeError` を送出するよう変換済み（3a と同 PR）。
replication 系 10 件は別途「対処済み（削除）」として既出（audit/report.md）。

### 3c. newSeq[byte](サーバ由来長) の巨大確保
- **実数**: `newSeq[byte](` 本番総数 **48**。うち truncating `newSeq[byte](X.int)`（int64→int）= **6**。
- **分布（X.int 確保）**: ranges.nim:383, 758 / user_types.nim:269 / encoding.nim:1087, 1098, 1173。
  サーバメッセージ長由来確保: pg_protocol.nim:1074 `buf: newSeq[byte](total)`（total は既存 buf 長で上界付き）。
- **代表3件**: encoding.nim:1087 `var data = newSeq[byte](size.int)` / ranges.nim:383 / pg_protocol.nim:1074。
- **系統性**: 【中】encode 側の `size.int` 確保 6 件は int64→int 縮小。深度分析は encoding.md / decoding.md（Tier1/2）参照。

### 3d. int32(...) / int(...) の truncating cast
- **実数**: `int32(` = **78**、`int(` = **80**、`.int)`（int64→int）= **12**、`.int32)` = **0**。
- **分布（.int) 12 件）**: newSeq 確保 6（3c と重複）/ ssl.nim:177,178（ALPN protoLen、サーバ交渉由来）/
  dsn.nim:225（権限チェック、良性）/ core.nim:820,852（numeric、クライアント側）。
- **代表3件**: ssl.nim:177 `result = newString(protoLen.int)` / core.nim:852 `result.setLen(fracStart + v.dscale.int)` / encoding.nim:297。
- **系統性**: 【中】サーバ由来長への縮小は ssl.nim ALPN（177,178）と 3c 確保群。int32( 78 件の深度は encoding.md 参照。

---

## シグナル4: 到達不能コード / 未参照の公開要素

- **実数**:
  - 定義のみで未参照の私有 proc（名前出現=1）= **0**（スクリプト走査、1251 定義中）。
  - `when false` / `if false:` dead branch = **0**。
  - コメントアウトされたコード = **0**（ヒューリスティック 7 件は全て自然文コメント、例: pg_pool.nim:1006 "let the maintenance loop resume..."）。
  - 単独 `discard` 行 = **103**（意図的な戻り値破棄 / `else: discard`、dead code ではない）。
- **分布**: dead code 該当なし。`discard` は accessors/encoding/transaction 等に散在（全て意図的）。
- **代表3件**: （該当なし。discard 代表: accessors.nim:189 `discard # text, varchar, bytea: fall through`）
- **系統性の判定**: 【低】明らかな dead code は検出されず。コードベースは到達不能コードについて清潔。
  唯一の「削除済 dead code」痕跡は RELEASE TODO 記載の ranges.nim dead `discard` 分岐（c8fa5eb で削除済）。

---

## シグナル5: 極端に大きいファイル/関数

### ファイル（既知、再確認）
- 本番上位: pg_pool **2310** / encoding **1940** / accessors **1860** / replication **1387** / protocol **1374** /
  ranges **1223** / core **1096** / connection/types **963** / decoding **935**。
- テスト上位: test_types **9107** / test_pool **3639** / test_e2e_transaction **3537** / test_e2e_convenience **2282**。

### 関数（100行超の top-level 定義）
- **実数**: top-level 定義 **1251** 件中、本体 **>100行 = 26**（doc コメント除外・ネスト proc 込みのヒューリスティック）。
- **分布（上位）**:
  | 行数 | 箇所 | 定義 |
  |---|---|---|
  | 437 | pg_pool.nim:1772 | proc notify* |
  | 411 | pg_sql.nim:176 | func sqlParams* |
  | 355 | transaction.nim:506 | proc savepointNameExpr |
  | 315 | lifecycle.nim:128 | proc connectToHost* |
  | 261 | ssl.nim:256 | proc establishTls |
  | 253 | advisory_lock.nim:367 | template withAdvisoryLockCore |
  | 240 | pg_pool.nim:836 | proc acquireImpl |
  | 221 | transaction.nim:284 | proc buildRetryDeadlineLoop* |
- 他 100-170 行帯: copyInStreamImpl(170), buildSendPhase(161), executeImpl(158), startReplication(152),
  parseIntervalText(135), parsePgOutputMessage(135), scanPlaceholders(130), parsePgMoney(128), sqlParseLoop(128),
  parseUriDsn(122), listenPump(120), parseBackendMessage(116), applyParam(116), runReplicationStream(115) 等。
- **代表3件**: pg_pool.nim:1772 (notify 437) / pg_sql.nim:176 (sqlParams 411) / lifecycle.nim:128 (connectToHost 315)。
- **系統性の判定**: 【中】巨大 proc はマクロ生成器（sqlParams/savepointNameExpr/sqlParseLoop）と
  状態機械（notify/connectToHost/acquireImpl/establishTls）に二極化。pg_pool.nim はファイル最大(2310)×
  巨大 proc 2 件（notify, acquireImpl）× 最高変更頻度で、**複雑度ホットスポット**。

---

## シグナル6: ハードコードされた資格情報/URL/パス（軽再確認）

- **実数**: 本番の**実秘密 = 0**（既確認を再確認）。
  - `password` リテラル: dsn.nim:653 `password = ""`（空既定値のみ）。
  - `secretKey`: PostgreSQL cancel-request のプロトコルフィールド（pg_protocol:98,745 / lifecycle:427 他）、資格情報リテラルではない。
  - URL/ホスト: `127.0.0.1`/`localhost` は DSN 既定値（dsn.nim:176,649）、`postgresql://user:pass@host` は doc コメント例。
  - ハードコード絶対パス: 本番 **0**。
- **分布**: tests/ に password/postgres:// 参照 **105** 件 = 全てテスト fixture（期待値）。
- **代表3件**: dsn.nim:653（空 password 既定）/ dsn.nim:176（127.0.0.1 既定）/ dsn.nim:5（doc の URI 例）。
- **系統性の判定**: 【低】実秘密なし。既存結論と一致。横断分析で追加調査不要。

---

## シグナル7: examples/ の現 API 一致性（付録C#8）

- **実数**: 14 サンプル（935行）、全て `import pkg/async_postgres`。**API 不一致 = 0**（目視、CI は両バックエンド编译済みの前提）。
- **使用シンボル → 現公開 API 照合（全て存在）**:
  - connect(2 定義) / connectReplication(2) / query(6) / queryValue(8) / exec(6) / withConnection(1) /
    prepare(1) / openCursor(1) / copyIn(4) / copyOut(1) / listen(1) / notify(2) / loCreate(1) / startReplication(1)。
  - `withTransaction`: **macro** として存在（conn=transaction.nim:362 / pool=pg_pool.nim:1785 / cluster=pg_pool_cluster.nim:293）。
    ※proc/func 走査では 0 件に見えるが macro 定義。examples（transaction.nim, large_object.nim）は `conn.withTransaction` を使用、整合。
  - pool 生成: `newPool`(pg_pool.nim:609) + `initPoolConfig`(pg_pool.nim:156)、examples/pool.nim:21 の使用と整合。
- **分布**: 不一致箇所なし。
- **代表3件**: examples/pool.nim:21 `await newPool(initPoolConfig(...))` / examples/transaction.nim:46 `withTransaction` /
  examples/replication.nim:64 `connectReplication`。
- **系統性の判定**: 【低】examples は現 API と完全整合。古い API の残存なし。横断分析で追加調査不要。

---

## シグナル8: コピペされた重複ブロック

- **実数（クラスタ別）**:
  1. **pumpUntilReady 3 種**: buffer_io.nim:376 / 417 / 450 — 各約35行、`block pumpLoop: while true: nextMessage → ErrorResponse/ReadyForQuery/else → fillRecvBuf`
     のループ本体が**3 重複**。差異は nextMessage 呼び出しシグネチャのみ。**重複は `{.dirty.}` injection 制約で強制**と
     buffer_io.nim:388-393 に明文化（template 境界をまたげない）。意図的・文書化済。
  2. **withTransaction*/withSavepoint* マクロ族 = 14**: transaction.nim 6（withTransaction/Retry/Deadline/RetryDeadline + withSavepoint/SavepointDeadline）、
     pg_pool.nim 4、pg_pool_cluster.nim 4。cluster 版は pool へ委譲（pg_pool_cluster.nim:307 `newCall(ident"withTransaction", ...primaryPool...)`）で
     部分的 dedup。conn 版と pool 版は構造的に並行。
  3. **テキスト/バイナリ parse 重複 = 21**: parse*Text / decode*Binary の対（decoding.nim, ranges.nim, core.nim）。
     例: parseRangeText(ranges:320)↔decodeRangeBinary(ranges:34)、parseInetText(decoding:676)↔decodeInetBinary(decoding:191)、
     parseHstoreText(decoding:475)↔decodeHstoreBinary(decoding:16)。
  4. **accessors の text/binary 分岐 = 40**: 40 個の `getXxx*` proc が各々 `isBinaryCol(col)` でテキスト/バイナリ分岐
     （accessors.nim に isBinaryCol 40 箇所）。**最大の分岐重複面**。
  5. **encoding の encode 重複 = 45**: toPgBinaryParam 38 オーバーロード + テキスト équivalent（encoding.nim）。
- **代表3件**: buffer_io.nim:376/417/450（pumpUntilReady 3 種）/ transaction.nim:362 vs pg_pool.nim:1785（withTransaction conn/pool 並行）/
  ranges.nim:320 vs ranges.nim:34（parseRangeText/decodeRangeBinary 対）。
- **系統性の判定**: 【高・設計特性】テキスト/バイナリ二重実装は型システム全体を貫く構造的重複
  （accessors 40 分岐 + parse/decode 21 対 + encode 45）。pumpUntilReady 3 種と言語制約マクロ族（14）は意図的。
  重複は「バグの温床」というより「契約の一貫性リスク」— ranges.md 所見（テキスト path のみ malformed 拒否漏れ、
  バイナリ path は厳密）がこの重複面の**乖離**を実証済。横断分析では「テキスト/バイナリ非対称」を独立クラスタとして扱う。

---

## 横断分析へ回すべき系統パターン（サマリ）

1. **【Defect クラスタ】（対処済み・縮小）**: シグナル2（raises:[] 121 + 本番防御 assert 実質0 + bearssl:31 の Defect リーク認識）
   × シグナル3a/3b（cellInfo/isNull IndexDefect、query.nim 固定インデックス）は `PgTypeError` へ変換済み。
   replication 系 10 件も別途対処済み。残存は body 内 Defect が advisory_lock/largeobject の
   `except CatchableError` を抜ける 2 件のみ。
2. **【テキスト/バイナリ非対称クラスタ】**: シグナル8（accessors 40 分岐 / parse-decode 21 対 / encode 45）×
   ranges.md 既証（テキスト path のみ malformed 拒否漏れ 8 箇所、バイナリは厳密）。重複面の契約乖離。
3. **【複雑度ホットスポット】**: シグナル5（pg_pool 2310行・notify 437・acquireImpl 240、巨大 proc 26 件）×
   シグナル1（負債マーカー0 → 問題がコードに見えない）× profile（pg_pool 変更頻度103・最高）。
4. **【外部負債管理】**: シグナル1（inline マーカー0、reviews/ + 未追跡 RELEASE TODO に集約）。
   既知問題の一次ソースはコード注释でなく reviews/RELEASE_0.4.0_TODO.md。

## 未調査 / 留保
- raises pragma の**実コンパイルによる不一致検証**は未実施（機械スキャン外。`raises: []` proc 内の Defect 生成操作の
  網羅列挙は深度分析が必要）。
- 関数長はヒューリスティック（ネスト proc/末尾コメント扱い）のため ±数行の誤差あり。相対分布のみ信頼。
- シグナル4 の「公開 API だが外部から未参照」はライブラリ性質上判定不能（内部未参照の私有 proc のみ走査、0 件）。
