# async_backend.nim 監査所見

対象: `async_postgres/async_backend.nim` (354行)。asyncdispatch / chronos 抽象化層。fan-in 26（全模块2位）。
背景: デフォルトバックエンドは asyncdispatch (`async_backend.nim:9`)。CI は両バックエンドで走る
(`map_tests_conventions.md`: `nimble test` が `-d:asyncBackend=asyncdispatch` と `chronos` の両方実行)。
chronos 4.4.0 を実機で確認済み（`~/.nimble/pkgs2/chronos-4.4.0-...`）。

---

- 分類: テスト（構造的空白）
- 重大度: Medium
- 確信度: 確定
- 場所: tests/test_async_backend.nim:1-36（専用テスト全体）、対象モジュール async_postgres/async_backend.nim
- 事象:
  fan-in 26（全模块2位、`map.md:21`）の基盤抽象に対し、専用テストは 36 行で `makeAsyncSeqByteCallback` の回帰（final-expression / early return / raise の3ケース）のみ。この抽象が上位に約束する中核 proc — `wait`（両 backend のタイムアウト/orphan 意味、async_backend.nim:25-35 / 162-191）、`asyncSpawn` (:206-215)、`allFutures` (:222-236)、`cancelAndWait` (:193-204)、`sleepAsync`/`sleepMsAsync` (:37-39 / 238-244)、`cancelTimer` (:41-44 / 246-250)、`registerFdReader`/`unregisterFdReader` (:46-68 / 252-271)、`scheduleSoon` (:70-76 / 273-278)、`completed` (:217-220)、`Duration`/`Moment` 演算 (:89-160)、`remainingDeadlineDuration` (:335-354)、`declareAsyncCallback` マクロ (:283-301)、`makeAsyncSinkByteCallback` テンプレート (:303-319) — に専用ユニットテストは無い。全て e2e/mock テストの間接行使のみ。
- 根拠: コード引用と推論
  - test_async_backend.nim:10-36 は `declareAsyncCallback(TestCb, ...)` + `makeAsyncSeqByteCallback` のみ。`wait`/`asyncSpawn`/`allFutures` 等の名前を直接検証するテストはファイル内に存在しない（`expect ValueError` 1件のみ、`map_tests_conventions.md:152`）。
  - CI は両バックエンドで走るため、コンパイル破壊や大まかな動作乖離は間接テストで捕捉される。しかし**意味的に微妙な乖離**は happy path 間接テストでは検出不能: 例) asyncdispatch `wait` の orphan 登録タイミング、`asyncSpawn` が投げる Defect の種別（chronos `FutureDefect` vs asyncdispatch `AssertionDefect`、下記「検証済み」参照）、`toMilliseconds` の sub-ms 切り捨て（所見4）、`allFutures` の完了集約。これらは backend 非対称バグの温床であり、fan-in 26 のブラスト半径に対して専用テスト36行は不均衡（`map.md:53` が構造リスクとして指名済み）。
- 系統性: 単発（このモジュール固有の構造問題）。ただし「薄い/無い」モジュール群（pg_errors 専用なし、type_lookup、cache、pg_bearssl の asyncdispatch レグ未実行）の一つであり、基盤層ほど専用テストが薄い傾向の一部。`map_tests_conventions.md:120-130`。

---

- 分類: 設計整合性（抽象の漏れ出し）/ 公開境界
- 重大度: Low
- 確信度: 確定
- 場所: async_postgres/async_backend.nim:21-35（chronos 分岐の `wait`）、asyncdispatch 分岐 162-191
- 事象:
  chronos 分岐では `export chronos` (:23) により chronos 本体の `wait` が無修飾で再公開される。このため `fut.wait(dur)`（2引数）は**モジュール独自の `wait` ラッパ（:25-35）を経由せず chronos ネイティブ `wait` に解決する**。独自ラッパの3引数版は chronos 分岐では `onOrphan` に既定値が無く（:26、asyncdispatch 版 :163 は `= nil`）、`onOrphan` を明示した3引数呼び出しでのみ到達可能で、到達しても `let _ = onOrphan` (:34) で無視する。つまり chronos では、最も一般的な2引数 `wait` 呼び出しの意味論をこのモジュールは実際には仲介しておらず、独自 `wait` の docstring（:28-33 の orphan 契約説明）は chronos の2引数呼び出しには適用されない。このオーバーロード解決の構造（2引数=chronosネイティブ / 3引数=ローカルラッパ）はどこにも文書化されていない。
- 根拠: コード引用と推論
  - chronos 4.4.0 `internal/asyncfutures.nim:1532`: `proc wait*[T](fut: Future[T], timeout = InfiniteDuration): Future[T]`（1〜2引数）。これが `export chronos` で可见。
  - ローカル chronos 版 `wait`（async_backend.nim:25-27）は `onOrphan` 必須（既定なし）。よって `fut.wait(dur)` は chronos ネイティブに、`fut.wait(dur, onOrphan=x)` のみローカルに解決（3引数版は chronos ネイティブに無いため曖昧性なし）。
  - asyncdispatch 版（:162-163）は `onOrphan = nil` 既定のため2引数・3引数ともローカルに解決。両 backend で「`fut.wait(dur)` がどの proc を呼ぶか」が異なる（chronos: ネイティブ / asyncdispatch: ローカル、onOrphan=nil で orphan 未処理）。
  - 結果として、保守者が chronos 版ローカル `wait` に横断的処理（tracing・deadline 記録等）を追加しても、それは3引数呼び出しにしか適用されず、圧倒的に多い2引数呼び出しを黙って取りこぼす。抽象が backend 固有の解決を上位に漏らしている。動作自体は正しい（chronos ネイティブはタイムアウトでキャンセルするため orphan 処理不要）だが、この解決構造は未文書化。
- 系統性: 単発。`export chronos` による意図しない再公開オーバーロードは `wait` のみで確認（`sleepAsync` も chronos ネイティブが再公開されるが、asyncdispatch 版ローカル `sleepAsync(d: Duration)` :238 とシグネチャ一致・意味等価で漏れ出しは軽微）。grep パターン: `rg -n "^  export chronos" async_postgres/async_backend.nim` → 1件（:23）。

---

- 分類: 正当性 / 公開境界（未文書の意味非対称）
- 重大度: Low
- 確信度: 高
- 場所: async_postgres/async_backend.nim:136-138 (`toMilliseconds`)、使用箇所 179 (`wait`)・240 (`sleepAsync`)
- 事象:
  asyncdispatch 分岐の `toMilliseconds(d) = int(int64(d) div 1_000_000)` (:136-138) は sub-ms を 0 へ切り捨てる。このため `wait(fut, sub-ms)` は `withTimeout(fut, 0)` となり、保留中の future に対して**即タイムアウト**（`AsyncTimeoutError`）する。一方 chronos の `wait(fut, sub-ms)` はその時間を待つ。`sleepAsync(sub-ms)` も同様に asyncdispatch では 0ms（即 yield）だが chronos は ns 精度で待つ。この切り捨て・即タイムアウト化は `wait`/`sleepAsync`/`toMilliseconds` のいずれの doc にも記載がない（`toMilliseconds` doc :137 は「Convert Duration to milliseconds」のみで切り捨ての注意なし）。
- 根拠: コード引用と推論
  - asyncdispatch `withTimeout`（Nim 2.2.10 `lib/pure/asyncdispatch.nim:1932-1957`）は `timeoutFuture = sleepAsync(timeout)` で、`sleepAsync(0)` (:1920-1930) は deadline=now のタイマを積み次の poll で発火。pending な `fut` に対し timeout 側が勝ち `false` → ローカル `wait` は `AsyncTimeoutError` を raise（async_backend.nim:181-187）。
  - chronos は `Duration` が ns 精度で `wait`/`sleepAsync` が sub-ms を honoring。よって `wait(fut, nanoseconds(500_000))` は asyncdispatch=即タイムアウト / chronos=0.5ms 待機、の意味乖離。
  - 緩和: deadline 由来の値は `remainingDeadlineDuration` が 1ms 床で保護（:335-352、doc :345-349 が「deadlines smaller than a few milliseconds are not meaningfully enforced」と言及）。しかしこれは `remainingDeadlineDuration` 固有の説明で、`wait`/`sleepAsync` への直接の sub-ms 渡しは保護も文書化もされない。
  - 潜在性: 現在本番で sub-ms を `wait`/`sleepAsync` へ直接渡す呼び出しは未確認（観測最小は `milliseconds(1)`、例 test_e2e_listen.nim:469）。実害は潜在。
- 系統性: 同種パターン（小規模・2箇所）。`toMilliseconds` 使用は `wait`:179 と `sleepAsync`:240 の2箇所（grep パターン: `rg -n "toMilliseconds" async_postgres/async_backend.nim` → 定義1 + 使用2）。両者とも sub-ms 切り捨て→0 の同一挙動。

---

## 検証済み・所見なし（系統比較の結果）

- **`asyncSpawn` の Defect 意味は chronos と実質一致**。chronos 4.4.0 `internal/asyncfutures.nim:665-689`: future 失敗時に `FutureDefect` を raise、キャンセル時も `FutureDefect`、既に finished なら即 `cb(nil)` 処理。asyncdispatch 版（async_backend.nim:206-215）は失敗時に `raiseAssert`（`AssertionDefect`）を raise。Defect の**種別**は異なる（`FutureDefect` vs `AssertionDefect`）が、doc (:207-208) は「a Defect is raised (matching chronos behaviour)」と種別を限定せず述べるのみで、不正確ではない。asyncdispatch にはキャンセルが無いため cancelled 分岐の不在は本質的。`addCallback` は asyncdispatch でも finished future に対し同期発火するため「既に finished なら即処理」も対称。所見なし。
- **`allFutures`（asyncdispatch 版 :222-236）の完了集約は正しい**。`var remaining` をクロージャで共有、各 future の `addCallback` が `dec remaining` し 0 で `retFuture.complete()`。単一スレッドイベントループのためデータ競合なし、`complete` はちょうど1回（remaining は futures.len から始まり各 future 1回ずつ dec）。空 seq は :225-227 で即 complete（chronos `allFutures` と一致）。失敗 future のエラーは伝播しないが、これは chronos `allFutures`（asyncfutures.nim:1000）も同様（個別確認は呼び出し側責任）。所見なし。
- **`Duration`/`Moment` 演算（:89-160）は off-by-one・borrow 漏れなし**。`==`/`<`/`<=`/`-`/`+` は `{.borrow.}`、`Moment` の `-`/`+`/`<=`/`<` は ticks 基準で明示実装。`hours`/`minutes` の int64 乗算は非現実的な巨大値（約292年相当）でのみ overflow し実害なし。`$`(:125-134) は ns/ms/s のみ整形（minutes/microseconds 未整形）だが表示専用で意味影響なし。所見なし。
- **`registerFdReader`/`unregisterFdReader`/`scheduleSoon`/`cancelTimer` の backend 対称性**。両分岐とも register を try 外で実行し addReader/addRead 失敗時に unregister して再 raise（chronos :50-62 / asyncdispatch :255-267）で構造対称。`cancelTimer` は chronos `cancelSoon`(:41-44) vs asyncdispatch no-op(:246-250) だが doc (:247-249) が「timers cannot be cancelled, but they complete harmlessly」と明記済み＝文書化された非対称。所見なし。
- **`close()` の listen pump 処理は backend 分岐済みで正しい**（lifecycle.nim:450-461）。asyncdispatch で `listenStopRequested=true` → `closeTransport()` → `await pump`、chronos で `cancelAndWait`。`test_e2e_listen.nim:296-324` が復活非再発を検証。同じ保護は `abortListenTask`（notify.nim:288-）にも適用済み。
- **`wait` の orphan 処理を要する呼び出し側は backend を正しく分岐**。`lifecycle.nim:535-552`（`attemptHostTimed`）と `pg_pool.nim:965-985`（acquire）は `when hasAsyncDispatch:` で `onOrphan` 付き `wait` を使い orphan 接続を close、chronos は2引数 `wait`（キャンセルされる）。`pg_pool_cluster.nim:194-236` はタイムアウト時に `asyncSpawn drainAbandonedAcquire(...)` で放棄 acquire を回収（doc :158-168 が非対称を明記）。`notify.nim:419` の `notifyWaiter.wait(timeout)` は orphan 化する future が純粋な同期 future（socket/IO を保持しない）ため `finally: conn.notifyWaiter = nil`(:425) で参照が切れ無害。`simple_query.nim:246/259`（`awaitOrInvalidate`/`awaitVoidOrInvalidate`）は orphan を onOrphan ではなく `invalidateOnTimeout`（csClosed 化、:224-226）で毒化して処理＝`buffer_io.md` 所見1 と同一の文書化済みパターン。新規の誤Handlingは無し。

---

## 2026-08-13 追記（S6 テスト拡充の実施と新規観測）

### 対処済み（S6: async_backend 専用テストの拡充）

- **テスト空白の解消**: tests/test_async_backend.nim を 36 行 → 約250 行へ拡充。
  - 追加: makeAsyncSinkByteCallback / Duration 変換・比較・演算・`$` / Moment 演算 / wait 成功・void・
    timeout・onOrphan（asyncdispatch: 発火・非発火）・chronos キャンセル / cancelAndWait / asyncSpawn /
    allFutures（空・失敗混在）/ completed / cancelTimer / scheduleSoon / registerFdReader（posix pipe）/
    remainingDeadlineDuration（過去・未来）。
  - 両バックエンドで全テスト成功（asyncdispatch 24件 / chronos 21件、`nim c -r` 確認済み）。
- **テスト作成時に発見したコンパイラ癖（プロジェクト起因ではない）**: Nim 2.2.4 で、unittest の
  `test` テンプレート展開スコープ内では 1) var 宣言より後ろに置いた無名 async proc リテラル
  （sink パラメータの callback 生成）が nimcall のまま残る 2) `import std/posix` の `wait` シンボルが
  chronos asyncmacro の `result` 解決と衝突する。テスト側で「proc をブロック先頭に置く」
  「from std/posix import」で回避。既存テストがこの制約を踏まえていたため（proc t() を先頭に置く
  慣習、test_e2e_copy.nim:201 等）本番コードには影響しない。

### 新規所見（S3 クラスタの残存、Low）

- **3引数 `wait` ラッパの chronos コンパイル不能（void future）**: async_backend.nim:25-35 の chronos
  分岐ラッパは `return await chronos.wait(fut, timeout)` で T=void のとき chronos 4.4.0 asyncmacro の
  `result` 曖昧化（asyncmacro.nim:365）で**コンパイルエラー**になる。本番は2引数形式（chronos ネイティブ
  wait に解決）のみを使用するため潜伏（本タスクのテストで実証）。doc は「onOrphan は chronos では呼ばれ
  ない」と述べるが、ラッパ自体が void で使えない事実は未文書化。asyncdispatch 分岐は void 対応
  （:188-189 `when T is void: fut.read()`）。
- **Moment 減算の負値クランプ非対称**: chronos `Moment - Moment` は負値を 0 にクランプ
  （timer.nim:176-181 "Duration can't be negative"）、asyncdispatch のシムは符号付き。本番の使用箇所
  （`remainingDeadlineDuration` は `deadline <= now` ガード後にのみ減算）は全て正値側のみのため実害なし。
  テストで両 backend の挙動差を文書化（Moment スイートの when 分岐）。
- **クライアント証明書と鍵の不一致検出の非対称**: asyncdispatch は `SSL_CTX_check_private_key`
  （ssl.nim:500-502、OpenSSL 3.6 では use_PrivateKey_file 段階で "key values mismatch" として検出）で
  fail-closed。chronos/BearSSL はクライアント側でペアリング検証が無く、サーバがクライアント認証を
  要求しない限り不一致ペアでも接続が成功する。実害は限定的（PG 既定はクライアント認証を要求しないため
  不一致が顕在化しない）だが、asyncdispatch の失敗→chronos の成功という silent な backend 差。
  test_tls_error_paths.nim に asyncdispatch 側の回帰テストのみ追加（chronos 側は挙動として正）。
