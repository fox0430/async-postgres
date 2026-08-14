# pg_pool.nim 監査所見

対象: `async_postgres/pg_pool.nim` (2310行、最大ファイル、変更頻度103)
バックグラウンド: デフォルトバックエンドは asyncdispatch (`async_backend.nim:9`)。asyncdispatch の `cancelAndWait` は no-op (`async_backend.nim:193-204`)、`wait()` はタイムアウト後も内部 future を停止せず `onOrphan` 鉤に依存 (`async_backend.nim:162-191`)。
既存レビュー `reviews/pg_pool_review.md` (#1 対応済、#2 doc/Low、#3 潜在Low、#4 意図的Info) を踏まえ再調査。下記は既存レビューで確定していない、または系統性証拠のある所見。

---

- 分類: 並行性・状態・リソース / 正当性
- 重大度: Medium
- 確信度: 高
- 場所: async_postgres/pg_pool.nim:570-597 (`maintenanceLoop` replenish フェーズ)、対照 425-429 (`spawnConnectForWaiter` 予約契約)・1046-1048 (acquire キュー時 spawn)・516-519 (`respawnForStrandedWaiter`)
- 状態: **対処済み（2026-08-14）**。replenish の in-flight connect を `pool.active` で予約計上
  （`settleReplenishConnect` 新設、handoff で消費 / park・close・failure・キャンセルで解放）。
  chronos の close() キャンセル経路は `except CancelledError` で予約を解放し re-raise。
  回帰テスト 3 件（asyncdispatch 2 / chronos 3、`tests/test_pool.nim` "Pool replenish capacity race"）。
- 事象:
  メンテナンスループの replenish フェーズは、`needed` 本のコネクションを `active` スロットの予約なしに開く。この in-flight connect は `pool.active` に計上されないため、同時に到着した caller-driven acquire が `pool.active < pool.config.maxSize` 検査 (`950行`) で空きありと判定し、別途コネクションを開ける。両者が完了すると、acquire 側が `active` を maxSize まで占有した上、replenish 側が `needed` 本を `idle` に駐車 (`597行`) し、`size() = idle.len + active` が `maxSize` を超過する。超過分は `idleTimeout`（既定10分）で minSize まで剪定されるまで持続する。
- 根拠: コード引用と推論
  - replenish は `570-571行` で `let currentTotal = pool.idle.len + pool.active; let needed = max(0, pool.config.minSize - currentTotal)` を計算し、`575-581行` で `connectFuts.add(connect(connCfg))` を `needed` 本発行、`581行 await allFutures(connectFuts)` で懸吊する。この間 `pool.active.inc` は一切ない（`580行` の connect 発行から `595行` の handoff 完了まで in-flight connect はどの計数にも乗らない）。
  - この懸吊中に acquire が走ると、`950行 if pool.active < pool.config.maxSize:` が in-flight replenish connect を見ずに通過し、`957行 pool.active.inc` の後 `965/985行` で別 connect を開く。単一イベントループでも `await allFutures(connectFuts)` の yield 中に acquire は実行されるため競合する。
  - 完了後 `592-597行` は `if pool.closed: closeNoWait / elif tryHandoffToWaiter: active.inc / else: idle.addLast` で、`idle.addLast` 分岐に maxSize 上限検査がない。waiter が居なければ（herd が各自 connect を取得済みなら）replenish connect は全て idle へ駐車される。
  - 具体シナリオ（minSize=maxSize=10 の固定プール）: idle=0, active=0 へ枯渇 → メンテナンス tick で needed=10 の connect を in-flight → その window に 10 リクエスト到着、各々 active.inc し connect（active=10）→ 完了後 active=10 + idle=10 = `size()` 20 > maxSize 10。DB の per-user 接続上限（例: 15）を突き抜け、他プール/他ユーザの connect を失敗させ得る。超過は idleTimeout（既定10分）で minSize まで剪定（`548-553行`、`totalCount >= minSize` を守りつつ idle を close）されるまで持続。
  - 予約規律との不整合: `spawnConnectForWaiter` doc `426-429行` は "The caller MUST have already incremented `pool.active` as a capacity reservation before invoking this proc" と明記し、実際に acquire キュー時 spawn は `1047行 pool.active.inc` 後 `1048行 spawnConnectForWaiter()`、`respawnForStrandedWaiter` は `518行 pool.active.inc` 後 `519行` で spawn する。replenish のみこの予約なしに connect を発行する。`newPool` の `637行 connect(cfg.connConfig)` も予約なしだが、プールが呼び出し元へ返る前の単発初期化で並行 acquire が存在しないため違反にならない。
- 系統性: 同種パターン（予約規律の唯一の違反）。`connect(...)` 発行箇所は5箇所: `452行`（spawnConnectForWaiter、呼び出し元が予約済）・`580行`（replenish、**予約なし**）・`637行`（newPool 単発初期化、並行なし）・`965/985行`（acquire、`957行` で予約済）。並行到達し得る経路で予約を欠くのは `580行` のみ。`pool.active.inc` は6箇所（518/595/916/944/957/1047）だが、いずれも「予約」か「handoff/ping 中の計上」で、replenish の in-flight 期間を覆うものはない。
  テスト空白: maxSize 不変条件の競合テストは sweep 経路 (`tests/test_pool.nim:2203` "concurrent acquires during sweep never overshoot maxSize"、ただし `minSize=0` で replenish が `needed=0` のため発火しない) と ping 経路 (`:1255` "concurrent acquire during health-check ping cannot exceed maxSize") のみ。replenish と caller-driven connect を競合させて `size() <= maxSize` を検証するテストは存在しない。replenish 関連テスト (`:2128`, `:3280`) は connect 失敗/close-race のみで上限超過は未検証。
  grep パターン: `rg -n "connect\(connCfg\)|connect\(cfg\.connConfig\)" async_postgres/pg_pool.nim` → 5件、`rg -n "pool\.active\.inc" async_postgres/pg_pool.nim` → 6件。

---

- 分類: 並行性・リソース（fire-and-forget タスク管理）
- 重大度: Low
- 確信度: 確定
- 場所: async_postgres/pg_pool.nim:969-980 (acquireImpl 孤立 connect close の `onOrphan`)、drain 対象 2296-2310 (`close()`)
- 事象:
  asyncdispatch で caller-driven connect が acquire budget を超過して `wait(rem, onOrphan=...)` がタイムアウトした際、後から完了した孤立 connect を閉じる `asyncSpawn` (`971行`) が `pendingBackgroundTasks` に登録されない。`close()` の drain ループ (`2296行 while pool.pendingBackgroundTasks.len > 0`) はこのタスクを待たずに返るため、`close()` 返回時点で孤立 connect の socket close が完了している保証がない。socket 自体は `asyncSpawn` により最終的に close されるため恒久リークにはならないが、`close()` の「全リソース解放」保証が asyncdispatch で弱まる。
- 根拠: コード引用と推論
  - `969-980行`: `onOrphan = proc(fut) = if fut.completed(): asyncSpawn (proc() {.async.} = ... await orphan.close() ...)()`。直前に `pendingBackgroundTasks.add` がない。対照: `closeNoWait` は `304行 add` → `305行 asyncSpawn`、`spawnConnectForWaiter` は `507行 add` → `508行 asyncSpawn` と必ず add-then-spawn。
  - `close()` drain (`2296-2310行`) は `pendingBackgroundTasks` の snapshot-and-clear のみで、登録されていないタスクは可視でない。`963-964行` コメントは "the orphan close mirrors `attemptHostTimed`'s handling" とするが、`attemptHostTimed` 側の同等 close も追跡しない（本所見はプール `close()` の drain 保証との関係で記述）。
  - 発火条件: asyncdispatch 限定（chronos は `982-983行` で `onOrphan` を使わずキャンセル）＋ caller-driven connect が budget 超過しつつ後で成功。狭いが実在する経路。
- 系統性: 同種パターン。pg_pool.nim 内の `asyncSpawn` 発行は4箇所: `305行`（closeNoWait、追跡済）・`508行`（spawnConnectForWaiter、追跡済）・`971行`（孤立 close、**未追跡**）・`1365行`（dispatch 実行、**未追跡** — 既存レビュー #3 が `dispatchBatchImpl` 未追跡を指摘済み）。つまり `asyncSpawn` 4件中2件が `pendingBackgroundTasks` 未登録で、`971行` はレビュー #3 と同根のパターンでありながら未指摘の新規箇所。
  grep パターン: `rg -n "asyncSpawn" async_postgres/pg_pool.nim` → 4件（305/508/971/1365、うち971は複数行呼び出し）、`rg -n "pendingBackgroundTasks\.add" async_postgres/pg_pool.nim` → 2件（304/507）。

---

- 分類: 並行性・正当性（batch dispatch 予算配分）
- 重大度: Low
- 確信度: 高
- 場所: async_postgres/pg_pool.nim:1300-1338 (`dispatchBatchImpl`)、1346-1376 (`scheduleDispatch`)、cap 定義 1325-1327
- 事象:
  `dispatchBatchImpl` は開始直後に `pool.dispatchScheduled = false` (`1302行`) へリセットする。このため、ある dispatch が `dispatchHomogeneous` の await（acquire + executeBatch）で懸吊している間に別の `pool.exec`/`pool.query` が `scheduleDispatch` を呼ぶと、`1348行 if pool.dispatchScheduled: return` ガードを通過して2つ目の dispatch が arm され、次の tick で並行に走り始める。両 dispatch が各自 `cap = max(1, maxSize div 2)` (`1327行`) までコネクションを acquire するため、合計で最大 `maxSize` に達し、`1325-1326行` コメントが述べる "Cap total concurrency at half the pool to avoid starving other users" の意図（プール半分の上 cap）が並行 dispatch 下で無効化される。pipelined 以外の `acquire()` 呼び出しが餓死し得る。
- 根拠: コード引用と推論
  - `1302行 pool.dispatchScheduled = false` は dispatch 開始時（drain 前）に実行される。dispatch はその後 `1329行 await pool.dispatchHomogeneous(ops, cap)` または `1333-1338行 await allFutures([...])` で懸吊する。
  - この懸吊中に `exec`（pipelined 分岐 `1392-1402行`）が `pendingOps.addLast` 後 `1401行 pool.scheduleDispatch()` を呼ぶと、`dispatchScheduled` は既に false なので `1350行` で true にセットされ `1373行 scheduleSoon(cb)` で別 dispatch が予約される。cb → `run` → `dispatchBatchImpl` #2 が #1 の実行中に起動する。
  - `dispatchHomogeneous` は `1271行 let nConns = min(ops.len, max(1, maxConns))` で最大 `maxConns=cap` 本を `1272-1277行` で順次 acquire し保持する。2 dispatch 並行で保持 connect 合計 ≤ 2*cap = maxSize（cap = maxSize div 2、奇数なら maxSize-1）。
  - 再スケジュール経路 (`1360-1361行 if pool.pendingOps.len > 0: pool.scheduleDispatch()`) は dispatch 完了後のため直列だが、早期リセット (`1302行`) が並行 exec 経由の重複 arm を許す。cap は dispatch 呼び出しごとの局所値で、グローバル上 cap が存在しない。
- 系統性: 単発（`dispatchScheduled` 早期リセットは `1302行` の1箇所、`scheduleDispatch` ガードは `1348行` の1箇所）。ただし pipelined モード（`config.pipelined=true`）の全 `exec`/`query`（4オーバーロード: 1378/1409/1440/1477行）がこの経路を通る。影響は pipelined 負荷が dispatch 完了間隔より速く到着するワークロードに限定され、cap は元来ソフトヒューリスティック（違反してもクラッシュせず、非 pipelined 呼び出しの待ち時間が増すのみ）。
  grep パターン: `rg -n "dispatchScheduled" async_postgres/pg_pool.nim` → 7件（133 宣言、624 newPool 初期化、1302 dispatch 開始時リセット、1344 failPendingAndUnschedule リセット、1348 ガード、1350 セット、2255 close 時リセット）。

---

## 検証済み・所見なし（系統比較の結果）

- `waiterCount` 計数は均衡。inc は `1038行`（acquireImpl キュー）の1箇所、dec は `397行`（tryHandoffToWaiter）・`414行`（failLastWaiter）・`832行`（settleAbandonedWaiter）の3箇所、リセットは `2252行`（close）。`settleAbandonedWaiter` (`802-832行`) は `fut.completed()`（handoff 勝者→release、dec せず）/`fut.failed()`（failLastWaiter/close 済→dec せず）/else（pending→cancelled 設定し `waiterCount > 0` ガード付き dec）の3分岐で、asyncdispatch の same-tick 競合（`810-812行` コメント）と chronos の先行キャンセル（`815-820行`）の双方で二重 dec / 負値化を防止。`831行 if pool.waiterCount > 0` ガードが close 後の settle 到達（`822-824行`）でも負値化を防ぐ。負値/残留による FIFO fast-path ガード (`870行`) 永久無効化の経路は確認できなかった。
- `releaseCore` (`665-690`) と `respawnForStrandedWaiter` (`510-519`) の active 計数は均衡。discard パス (`671-679`) は `active.dec` (`674`) 後 `respawnForStrandedWaiter` が条件付きで `active.inc` (`518`) し spawn、spawn の `finally` (`501-503`) が `if not consumed and pool.active > 0: active.dec` で予約を解放。handoff パス (`685-686`) は active を減算しない（conn の所有者移行のみ）が、これは handoff 元が既に active 計上済みのため整合。
- double-release ガード (`releaseImpl` `731-735行`) は `conn.borrowed` で既に idle な conn の再 release を no-op 化。handoff 直後の back-to-back 二重 release が in-use conn を再ルーティングする制限は `722-729行` で文書化済み（raw API 固有、`PooledConnHandle`/`with*` が安全経路）。新規所見ではない。
- sweep 経路の maxSize 超過は `closeNoWait` による yield-free 化で修正済み（`527-529行` コメント、テスト `tests/test_pool.nim:2151/2203`）。ping 中の active 計上 (`916行`) もテスト済 (`:1255`)。close-race 4経路（replenish `:3280` / acquire `:3321` / ping `:3358` / spawn `:3407`）は全て `pool.closed` 再検査と closeCount 計上の回帰テストが存在する。
- `resetSession` (`307-356`) の CancelledError / CatchableError 分離は対称（両者 `conn.state = csClosed` 設定のみ、close は `releaseCore` の `closeNoWait` に委譲し metrics 二重計上を回避、`350-351/355行` コメント）。既存レビュー #1 対応済の整合が維持されている。`resetSessionAndRelease` (`793-800`) は `finally: conn.release()` で cancel 時も release を保証。
- `close()` の drain (`2296-2310`) は snapshot-and-clear + while 再チェックで、await 中に追加されたタスクを次周で回収。`2289-2290行` の単一 yield は abandoned-handoff 継続の closeNoWait enqueue を待機（`active > 0` のときのみ、`2284-2288行` 正当化）。asyncdispatch の `cancelAndWait` no-op に起因するメンテナンスループ由来の取りこぼしは既存レビュー #2（Low/doc）の通りで、新規所見としては扱わない。
- `splitBatchBudget` (`1175-1185`) の `cap==1` 合計超過は `1181-1182行` コメントで意図的（既存レビュー #4、Info）。`batchTimeout` (`1160-1173`) は1つでも ZeroDuration の op があれば batch 全体を無制限化し、有限 op が兄弟の短い deadline に clamp されるのを防ぐ（`1163-1167行`）。`failAllPending` (`1146-1158`) は `raises: []` で asyncSpawn 呼び出し元への漏洩をコンパイル時に封じる。
