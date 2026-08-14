# コードレビュー結果

**対象**: 最新コミット `5fd9e37` （4 ファイル / +385 −33 行）
**ブランチ**: fix/listen-close-reconnect-race
**変更の要約**: asyncdispatchでlistenポンプが自動再接続のblocking `connect()`内に詰まっているとき、`close()` が無界にハングする問題と、naiveな有界waitが `reconnectInPlace` のnewConn無条件graftで接続を復活させてしまう問題を修正。`close()`/`stopListening()` に有界waitを導入してタイムアウト時にポンプをorphan化し、`listenStopRequested` をset維持、`reconnectInPlace` が `connect()` 直後に同フラグをチェックしてnewConnを破棄する。`stopListening` はorphanタイムアウトで `PgError` を投げる。

## 修正ステータス（追記）

Medium 1・2 を同ブランチで対応済み（未コミット）。両 backend で `test_listen_reconnect` / `test_e2e_listen` / `test_pool` 全緑、各修正について disable→FAIL→restore→OK で regression detection 実証済み。詳細は各 Issue 末尾を参照。

- **Medium 1**: 修正済み。`stopListening` orphan-raise 前に `conn.state = csClosed` と `conn.failNotifyWaiter()` を追加。pending waiter は即解放、後続 `waitNotification` は `checkListenAlive` の csClosed ガードで拒否。
- **Medium 2**: 修正済み。`PgError` → `PgTimeoutError`（`PgConnectionError` サブタイプ）に変更。`listen*`/`unlisten*`/`stopListening*` の docstring も更新。
- **Q1 / Q2**: 上記 Medium 1・2 に沿って解消。設計意図として「close して開き直す」契約を明示（doc に追記）、pending waiter は他終了パスと対称に解放。
- **Q3（silent orphan の tracer 通知）**: 未対応。設計判断が必要。
- **Q4（README/examples 未反映）**: 未対応。
- **Q5（chronos カバレッジ）**: 既存新規テスト（graft/discard 判定）は `when hasAsyncDispatch` 無しで chronos でも走行・アサート済み。追加不要と判断。

---

## Critical

指摘なし。

## High

指摘なし。

## Medium

### 1. stopListeningのorphan-raiseパスがpending/後続の `waitNotification` waiterを解放せずハングさせる【修正済み】

- **場所**: `async_postgres/pg_connection/notify.nim:349-361`（解放漏れ）、影響先 `notify.nim:420-456`
- **何が起きるか**: reconnect中のポンプに対して `stopListening` がタイムアウトしorphan化して `PgError` を投げると、pending（またはraise後に発行された）`waitNotification(timeout=ZeroDuration)` が永久ブロックしうる。doc契約通り `close()` すれば解放されるが、(a) `PgError` 受信から `close()` 実行までの間、別タスクのwaiterは契約を守っていても一時ハングする、(b) 回復可能エラーを返すstopListeningパスの中で**唯一**waiterをinlineでfailしない非対称がある。
- **再現条件**: asyncdispatch。`listen` 後にサーバ切断→ポンプがreconnectのblocking `connect()` で停車（`listenReconnecting=true`）→別タスクが `waitNotification(ZeroDuration)` で待機、またはraise後に発行→`stopListening` が `listenReconnectStopWaitMs` でタイムアウトし `PgError` →呼び出し側が即 `close()` しない→waiterハング。
- **根拠**:

  ```nim
  # notify.nim:349-361
  except AsyncTimeoutError:
    preserveStopFlag = true
    raise newException(
      PgError,
      "stopListening: listen pump did not stop within " & $listenReconnectStopWaitMs &
        " ms while reconnecting",
    )
  except CatchableError:
    await conn.abortListenTask()
  conn.listenTask = nil      # 360 — raiseで到達しない
  conn.failNotifyWaiter()    # 361 — raiseで到達しない
  ```

  raiseは `listenTask = nil`(360) と `failNotifyWaiter()`(361) の手前で発生する。raise直後、`conn.state` は `reconnectInPlace:70` が設定した `csConnecting` のまま、`listenTask` は生存。この状態で `waitNotification` を呼ぶと、`checkListenAlive`(420-427) は `csClosed`/`listenError` しか見ず `csConnecting` を検出できないため素通りし、442の `listenTask == nil or finished` も偽のため、446でwaiterが生成され454で永久ブロックする。orphanポンプは `connect()` 解除後 `return`(255/271) するだけで `notifyListenDeath`/`failNotifyWaiter` を呼ばない。`notifyWaiter.fail` を行うのは notify.nim:158, 305 と lifecycle.nim:488(close) のみで、orphan後に解放できるのは `close()` だけ。他のstopListening終了パス（早期return:325、reconnect分岐:361、normal分岐:378）は例外なく `failNotifyWaiter` を呼ぶ。
- **検証**: 中立検証エージェントが制御フロー(1)-(8)をすべてコードで確認（**確認**）。永久ハングはdoc契約違反（非close）時のみだが、契約遵守時でも `close()` までの一時ハングが存在し、回復可能エラーを返す唯一のパスとしてwaiter即時failを欠く非対称は契約では正当化されない実缺陷と判定。
- **提案**: `AsyncTimeoutError` 分岐でraise前に `conn.failNotifyWaiter()` を呼ぶ（orphanは通知をdispatchしないため解放は安全で、他の全終了パスと整合する）。あるいはraise前に `conn.state = csClosed` へ落とせば `checkListenAlive` が機能し、早期return部の `csClosed` ガード(323)とも整合する。いずれにせよこの振る舞いをテストで固定する（観点8参照）。

### 修正

提案の両方を採用。`AsyncTimeoutError` 分岐で raise 前に `conn.state = csClosed` と `conn.failNotifyWaiter()` の 2 行を追加。pending waiter は即解放、後続 `waitNotification` は `checkListenAlive` の csClosed ガードで拒否される。回帰テスト `tests/test_listen_reconnect.nim` "stopListening orphan-raise releases pending waitNotification and refuses new ones" を追加、fix 2 行のコメントアウトで FAIL することを実証済み。

### 2. orphanタイムアウトのエラーが裸 `PgError` で、pg_errors.nimの文書化された分類原則と自docの意味論に矛盾する【修正済み】

- **場所**: `async_postgres/pg_connection/notify.nim:353-357`（raise本体）、伝播先 `notify.nim:390`(listen)/`400`(unlisten)
- **何が起きるか**: `stopListening`（および公開API `listen`/`unlisten`）が新たに投げるエラーが裸 `PgError`（`PgConnectionError` のサブタイプでない）。このエラーの自docは「connection is unusable — close and reopen」（再接続が回復）と述べるが、これはpg_errors.nimが規定する「`PgConnectionError` サブタイプとして `except PgConnectionError` ループからvisibleであるべき」基準そのもの。文書化された `except PgConnectionError` 再接続イディオムで `listen`/`unlisten` を包む利用者は、この「接続使用不能」シグナルを取りこぼす。
- **再現条件**: 呼び出し側が `listen`/`unlisten` を `except PgConnectionError` 再接続ループで囲い、asyncdispatchでポンプがblocking `connect()` 中に `stopListening` がタイムアウトする場合。
- **根拠**:

  ```nim
  # notify.nim:353-357
  raise newException(
    PgError,
    "stopListening: listen pump did not stop within " & $listenReconnectStopWaitMs &
      " ms while reconnecting",
  )
  ```

  pg_errors.nim:15-19は「再接続が正しい回復なら、そのエラーは `PgConnectionError` のサブタイプであり `except PgConnectionError` ループからvisibleでなければならない」と規定。既存のタイムアウトサイトは例外なく `PgTimeoutError`（`PgConnectionError` サブタイプ）を投げる: `simple_query.nim:226`、`notify.nim:452`(waitNotification)、`pg_pool.nim:2065/2072/2152/2155`。listenポンプ永久死専用の `PgListenError` も `PgConnectionError` サブタイプ（pg_errors.nim:108-112）。一方 `notify.nim:316-317` の自docは「unusable — close and reopen」と述べ、これはまさに `PgConnectionError` 相当の意味論。意図的に `PgConnectionError` 外に置かれた `PgStateError`（pg_errors.nim:57-67）は「再接続は無意味」という**明示的根拠**付きだが、新エラーはその逆の意味論で、同先例は援用不能。
- **検証**: 中立検証エージェントが**確認**。事実関係をすべて裏付け、「意図的除外の根拠は文書化されておらず、コードの配置と著者自身のdocテキストが内部矛盾している」と判定。代償は契約/文書レベルで実在、現状のin-repo呼び出し側（listen/unlistenを `except PgConnectionError` で包む箇所はゼロ）レベルでは理論的。
- **提案**: このエラーを `PgTimeoutError`（意味的に「タイムアウトで接続を閉じるべき」）または `PgListenError` 等 `PgConnectionError` のサブタイプとして投げ、文書化された再接続ループからvisibleにする。裸 `PgError` を意図的に選ぶなら、pg_errors.nimの階層docに「このlisten停止タイムアウトが `PgConnectionError` でない理由」を追記して内部矛盾を解消する。

### 修正

`PgError` → `PgTimeoutError` に変更（`PgConnectionError` のサブタイプ、他のタイムアウトサイト `simple_query.nim:226`, `notify.nim:452`, `pg_pool.nim` と整合）。`stopListening*` / `listen*` / `unlisten*` の docstring も `PgTimeoutError (a PgConnectionError subtype)` に更新、`except PgConnectionError` 再接続ループから可視化。回帰テストも `PgTimeoutError` および `e of PgConnectionError` を検証。

## Low

指摘なし。

---

## 確認したい点

1. **（指摘1に関連）orphan-raiseで `failNotifyWaiter` を呼ばないのは意図か欠落か。** `failNotifyWaiter` のdoc（notify.nim:301 "when the pump has stopped"）の字面条件（raise時点ではポンプ未停止）を根拠にした設計か、それとも「close and reopen」契約による `close()` 任せを前提とした取りこぼしか。契約違反（closeせず参照を捨てるだけ）時の永久ハングを許容する設計意図かを確認したい。

2. **（指摘2に関連）裸 `PgError` の選択は意図的な設計判断か。** 「再接続ではなくcloseして開き直すことを強制したい」等の意図があればdocに明記すると、pg_errors.nimの階層docと整合する。`PgTimeoutError`/`PgListenError`/`PgConnectionError` のいずれが意図に合うか確認したい。

3. **orphan化という新しい失敗モードが完全にsilent。** `close()` のasyncdispatch orphanパス（lifecycle.nim:468-469 `except AsyncTimeoutError: discard`）と、その後の自己清掃退出（notify.nim:245-255、`reconnectCallback`/`notifyListenDeath` を意図的にスキップ）は、tracer・コールバック・ログのいずれも出さない。本リポジトリには「otherwise silentなswallow/teardownイベントはtracerへ流す」という強い文書化慣習がある（buffer_io.nim:592-613 `fireTransportCloseError`、pg_pool.nim:259-260 `onPoolCloseError`、types.nim:919-921 "tracing is the only signal operators have"）。本コミットが導入する新しい失敗モードに対して、運用追跡性を周辺コード同等にする意図があるか確認したい。

4. **README.md / examples/listen_notify.nim が新契約を未反映。** 本コミットで追加された「asyncdispatch限定で `listen`/`unlisten`/`stopListening` がorphan-timeoutの `PgError` を投げ、接続はunusable→close and reopen」という契約に、README:109-113のLISTEN/NOTIFY回復モデルもexamples/listen_notify.nim（try/except無し、PgErrorは `waitFor main()` まで伝播してプロセスを落とす）も言及していない。README:129のasyncdispatch一般警告で十分という判断か確認したい。

5. **chronos側のカバレッジ。** 新規テスト2/3のアサーションは `when hasAsyncDispatch` で囲まれchronosでは実質何も検証しない（tests/test_listen_reconnect.nim:745-747に意図明記）。一方 `reconnectInPlace` のpost-connect stop-check（notify.nim:78-86）はバックエンド非依存コード。chronosではcancellationで閉じる設計のはずだが、この非依存パスにchronos側カバレッジが不要か念のため確認したい。

---

## レビュー範囲の申告

- **読んだファイル**: `async_postgres/pg_connection/notify.nim`（全文）、`lifecycle.nim`（close周辺 400-519）、`types.nim`（全文）、`buffer_io.nim`（closeTransport/isConnected 585-724）、`simple_query.nim`（checkReady 63-82）、`pg_errors.nim`（全文 1-115）、`tests/test_listen_reconnect.nim`（1-120＋diffの新規部分）、`tests/all_tests.nim`、`tests/config.nims`、`.github/workflows/test.yml`、`nph.yml`、`async_postgres.nimble`。サブエージェント経由で `pg_pool.nim`、`pg_connection.nim`、`async_backend.nim`、`mock_pg_server.nim`、`examples/listen_notify.nim`、`README.md`、`pg_largeobject.nim`、`test_e2e_listen.nim`、`test_pool.nim` を参照。
- **参照した明文の規約**: なし（AGENTS.md / CLAUDE.md / CONTRIBUTING.md は存在しない。CI設定 test.yml / nph.yml のみ確認）。
- **既存コードから推定した慣習**（観点9-bの判断根拠。推定が誤っていれば訂正いただきたい）:
  - タイムアウトのエラー型は `PgTimeoutError`（`PgConnectionError` サブタイプ）: `simple_query.nim:226`、`notify.nim:452`、`pg_pool.nim:2065/2072/2152/2155`。生の `AsyncTimeoutError` は単一ホストconnectTimeout（`lifecycle.nim:657-658`、意図的・文書化）と `buffer_io.nim:207/234`。**パターンが割れている領域**として記録（指摘2は9-bではなく互換性/設計整合として扱った）。
  - 兄弟モジュールが内部シンボルを使う場合はヘルパーをexportして `{.all.}` を回避: `types.nim:801`。`{.all.}` はテストが特定サブモジュールを直接import: `pg_connection.nim:43-46`。
  - otherwise silentなswallow/teardownイベントはtracerへルーティング: `buffer_io.nim:592-613`、`pg_pool.nim:259-260`、`types.nim:485-489, 919-921`。
  - docコメントで「何が伝播するか」を明記する慣習: `notify.nim`/`lifecycle.nim` 各所（本コミットのdoc追記もこれに沿う）。
- **十分に検証できなかった領域とその理由**: 本タスクは読み取り専用に制約されているため、`nimble test` / `nim c` によるコンパイル・型検査・テスト実行は未実施（実機挙動は未検証）。新規テストはモックサーバベースかつ `when hasAsyncDispatch` ガード内のため、chronosバックエンドでの実挙動と、実PostgreSQLに対するblocking `connect()` の実際の持続時間に依存する部分（orphan発生の実頻度）は検証できていない。
- **検出後に検証で破棄した指摘の件数**: 1 件（生産コードの `import types {.all.}` — types.nim:41-42で著者意図が明記され、結合面拡大の代償が理論的（使用非公開シンボルは `listenReconnectStopWaitMs` の1件のみ）のため反証・破棄）。
