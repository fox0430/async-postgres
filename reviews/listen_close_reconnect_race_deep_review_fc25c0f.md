# 深層コードレビュー — fc25c0f

**対象**: コミット `fc25c0f3a19036331718111e028069408f4bcdb7`（`git show`）
**ブランチ**: fix/listen-close-reconnect-race
**規模**: 4 ファイル / +491 −33 行
- async_postgres/pg_connection/lifecycle.nim |  22 +-
- async_postgres/pg_connection/notify.nim    |  84 ++++--
- async_postgres/pg_connection/types.nim     |  17 +-
- tests/test_listen_reconnect.nim            | 401 ++++++++++++++++++++++++++++-

**変更の要約**: asyncdispatch ではブロッキング `connect()` 内の listen ポンプをキャンセルできないため、`close()`/`stopListening()` がハングまたは接続を復活させるレースを修正。有制限待ち（`listenReconnectStopWaitMs`）＋ orphan 化＋ `listenStopRequested` セット保持を導入し、`reconnectInPlace` が `connect()` 返回後に flag をチェックして新 transport を廃棄するようにした。`stopListening` はタイムアウトで `PgTimeoutError` を投げる。

**手法**: 文脈構築サブエージェント5並列 → 9観点レビューエージェント並列 → 指摘ごとに独立検証エージェント（確認/反証/判断不能）。検証で反証された指摘は破棄済み。

---

## Critical

なし。

## High

なし。

## Medium

### 1. `close()` の orphan 化が完全黙殺で、確立された tracer 慣習と不整合（運用時に orphan リークが無痕跡になる）
- **場所**: `async_postgres/pg_connection/lifecycle.nim:468-469`
- **確信度**: 確定（検証エージェントが 9-b の3条件充足を確認）
- **何が起きるか**: asyncdispatch で listen ポンプがブロッキング `connect()` 内にあり有制限待ちがタイムアウトすると、`close()` は orphan ポンプと新ソケットを残して戻るが、例外も tracer イベントも一切出さない。同じ orphan 条件でも `stopListening` は `PgTimeoutError` を投げて可視化する（`notify.nim:359-363`）。このコミットが標的とする「connect() が巻き戻らない」ケースでは orphan が恒久的に漏れ得るが、tracer を設定した運用者にはログ・メトリクスに痕跡ゼロ。
- **再現条件**: asyncdispatch。listen ポンプが `reconnectInPlace` のブロッキング `connect()` 中に `close()` を呼び、`listenReconnectStopWaitMs` 内に connect が復帰しない。tracer 有無を問わず無痕跡。
- **根拠**:
  ```nim
  # lifecycle.nim:468-469
      except AsyncTimeoutError:
        discard
  ```
  teardown で飲み込んだエラー/不可視状態を tracer へ流す慣習は比較可能な多数箇所で一貫:
  ```nim
  # buffer_io.nim:589-613（closeTransport）— 5箇所の closeWait 失敗を全て流す
        try:
          await conn.tlsStream.reader.closeWait()
        except CatchableError as e:
          conn.fireTransportCloseError(tcsTlsReader, e)
  ```
  ```
  # types.nim:919-921（フック doc）
  ## the error cannot be propagated to a caller — tracing is the only signal operators have
  ```
  同系統: `fireCleanupSkipped`(types.nim:900-913)、`fireAdvisoryUnlockFailed`(pg_advisory_lock.nim:412,414)、`reportCloseError`/`onPoolCloseError`(pg_pool.nim:254-260)。pool の `tracedClose`（pg_pool.nim:264-267）は `close()` が **raise** したエラーしか拾わないため、本パスの `discard` ではその共通経路すら素通りする。既存の `except AsyncTimeoutError` 箇所（pg_pool.nim:1055, 2304-2308 等）は raise または直後回収を伴い、tracer も回収もない純黙殺は本箇所が唯一。
- **検証**: 検証エージェントが 9-b の3条件（3箇所以上の一貫性・具体的な代償・パターンが割れていない）をすべて充足と確認。`AsyncTimeoutError` の orphan 黙殺は本コミットが新規導入（旧コードは無制限 `await pump` で timeout-orphan 自体が存在しない）。
- **提案**: 既存 `fireTransportCloseError` と同じ形（`conn.config.tracer` 参照、nil フックは no-op）で orphan 発生を tracer へ流す。該当フックが無ければ専用フック追加。`stopListening`（raise）との可観測性非対称も解消される。

## Low

### 2. 新規追加の invariant doc がコードの実際の保証範囲を超えている（通常パスのエラー分岐が doc と矛盾）
- **場所**: `async_postgres/pg_connection/types.nim:263-264`（doc）／矛盾箇所 `async_postgres/pg_connection/notify.nim:377-387`
- **確信度**: 高（レース成立は検証確認。ただし復活レース本体は既存、doc 矛盾がインスコープ）
- **何が起きるか**: 本コミットが追加した `listenStopRequested` の doc は「live orphan を持つ csClosed 接続で flag をクリアするな」と定めるが、`stopListening` の通常パス（非 reconnecting）のエラー分岐はまさにそれを行う。`sendMsg` が失敗すると `abortListenTask`（asyncdispatch で no-op）→ `listenTask=nil` → `finally` で flag クリア。しかしポンプはまだ recv 失敗を処理しておらず、クリア後に再接続ループに入り、flag が false のため `reconnectInPlace` が新 transport を graft して接続を復活させ得る（`listenTask=nil` 済みで以降 `close()` はポンプに触れない）。
- **再現条件**: asyncdispatch。`listen()` 後ポンプが recv ループ（csListening・非 reconnecting）にある状態で、`stopListening` の `sendMsg` 失敗とポンプの recv 失敗継続が競合。
- **根拠**:
  ```nim
  # types.nim:263-264（本コミットが追加した invariant）
      ## post-connect graft is disarmed — do not clear it while a csClosed
      ## connection may still hold a live orphan.
  ```
  ```nim
  # notify.nim:377-387（通常パスエラー → finally で無条件クリア）
      except CatchableError:
        # Send or pump failed: connection is dead
        await conn.abortListenTask()
      conn.listenTask = nil
      ...
    finally:
      if not preserveStopFlag:      # この分岐では false のまま
        conn.listenStopRequested = false
  ```
  `preserveStopFlag` は `AsyncTimeoutError` 分岐（:356）でしか true にならないため、通常パスエラーでは flag がクリアされる。`abortListenTask` の `cancelAndWait` は asyncdispatch で no-op（async_backend.nim:193-204）なのでポンプは生存し、`notify.nim:230` の `not conn.listenStopRequested` が true で graft へ進む。
- **検証**: 検証エージェントがレースの成立を**確認**。ただし `git show fc25c0f^` で、通常パスのエラー分岐と finally での flag クリアはコミット前後で**完全に同一**（diff はコメント行削除のみ）であり、復活レース自体は**変更以前から存在**すると判定。したがって復活バグ本体は付録A「既存の問題」に該当し、ここでは**本コミットが追加した doc invariant がコードの実際の保証と矛盾する点**をインスコープの指摘とする。
- **提案**: 二択。(a) 通常パスエラー分岐でも、ポンプの停止を確認できていないなら `preserveStopFlag = true` 相当で flag を保持する（本コミットの修正を同族パスへ拡張）、または (b) doc invariant を「reconnecting パスのタイムアウト時に限る」と正確化する。いずれにせよ doc とコードの保証範囲を一致させること。

### 3. `stopListening` の `PgTimeoutError` 経路が `listenTask` を nil 化せず、doc 指示通りの `close()` が再度フルタイムアウト待つ（累計最大約20秒）
- **場所**: `async_postgres/pg_connection/notify.nim:351-363`（raise）→ `async_postgres/pg_connection/lifecycle.nim:453-466`（再待機）
- **確信度**: 確定（検証エージェントが全5点をコードで確認）
- **何が起きるか**: `stopListening` が `AsyncTimeoutError` で orphan 化して `PgTimeoutError` を raise するとき、`:366` の `conn.listenTask = nil` は raise のため到達されない。doc（notify.nim:317-319）は「接続は使用不可、close して再オープンせよ」と定めるが、呼び出し側がその通りに `close()` すると、`close():453` で `listenTask != nil and not finished` が真となり、再度 `listenReconnectStopWaitMs`（既定10秒）の有制限待ちに入る。orphan ポンプがまだ `connect()` 内ブロック中なら合計最大約20秒。
- **再現条件**: asyncdispatch。ポンプがブロッキング `connect()` 内で停車 → `stopListening()` がタイムアウトで `PgTimeoutError` → 呼び出し側が doc 通り `close()` → 更に最大10秒待機。
- **根拠**:
  ```nim
  # notify.nim:351-363 — raise が :366 より前に脱出
      except AsyncTimeoutError:
        preserveStopFlag = true
        conn.state = csClosed
        conn.failNotifyWaiter()
        raise newException(PgTimeoutError, ...)
      ...
      conn.listenTask = nil   # :366 — timeout 経路では到達不可
  ```
  ```nim
  # lifecycle.nim:453-466 — nil でない listenTask を再検出
    if conn.listenTask != nil and not conn.listenTask.finished:
      ...
        await pump.wait(milliseconds(listenReconnectStopWaitMs))
  ```
- **検証**: 検証エージェントが全5点（raise 脱出・close 再待機・累計20秒・到達可能性・本コミット導入）をコードで**確認**。ハングではなく有界（最大20秒）である点から Low。
- **提案**: `AsyncTimeoutError` ハンドラ内で raise 前に `conn.listenTask = nil` を実行する（orphan の Future はイベントループが保持し `reconnectInPlace:78` の flag チェックで自壊するため追跡不要）。または `close()` 側で `csClosed` かつ flag セット済みならポンプ待機をスキップする。

---

## 確認したい点

1. **`listenStopRequested` は公開フィールド（`*` 付き）だが、事後条件が変更された**。修正前は `close()`/`stopListening()` 復帰後に必ず false だったが、修正後は orphan 時に true のまま残り得る（`lifecycle.nim:472-473`、`notify.nim:324-326`）。リポジトリ内の参照はライブラリ本体とテストのみだが、外部ユーザーがこのフィールドを直接読み書きする想定はあるか。無いなら意図的内部配線として実害なし。あるなら新しい doc（types.nim:261-264）の警告（live orphan 中にクリアすると復活バグ再発）の周知が必要。**何が分かれば確定できるか**: 公開 API としてこのフィールドへの直接アクセスを想定しているか否か。

2. **orphan ポンプが `connect()` 内で永久ブロックする場合のリソースリーク**。サーバが TCP accept 後に PG ハンドシェイクを一切返さず、かつ `config.connectTimeout` 未設定（既定: 無制限）の場合、orphan は `connectToHost` 内で永久にブロックし、Future・新ソケット fd・`conn` 参照がイベントループに残る。修正前は `close()` 自体が永久ハングしていたため既存の asyncdispatch 制約のトレードオフ（ハング vs リーク）だが、orphan 経路で `connectTimeout` を強制するかは設計判断。**何が分かれば確定できるか**: 永久 orphan を許容するか、connect 経路にハンドシェイク全体のタイムアウトがあるか。

3. **chronos 側の `reconnectInPlace:78` チェックがテストで行使されない**。このチェックは `when` でガードされず両 backend に有効だが、新規テスト2〜4のアサーションは `when hasAsyncDispatch` でガード（tests/test_listen_reconnect.nim:752,780,874,961）。chronos の `close()` は `cancelAndWait` で connect を中断するため :78 に到達せず、chronos で :78 に到達する経路（stopListening 中に connect が正常完了する競合）の決定論的テストは存在しない（`notify.nim:245` が第二の安全網として機能）。**何が分かれば確定できるか**: :78 を「asyncdispatch 専用でない防御」として意図しているか、chronos 側カバレッジは不要か。

4. **指摘2（通常パスエラーの復活レース）への対応方針**。復活レース自体は既存だが、本コミットの目的はまさに「再接続レースでの復活を止める」であり、同族パスが未修正のまま新 invariant doc が追加されている。このパスも修正対象に含める意図か、それとも doc を狭めて scope 外を明示するか。**何が分かれば確定できるか**: 通常パスエラー分岐の flag 保持を修正する予定があるか。

---

## レビュー範囲の申告
- **読んだファイル**: `async_postgres/pg_connection/{lifecycle,notify,types,buffer_io}.nim`、`async_postgres/{pg_errors,pg_pool,async_backend,pg_advisory_lock}.nim`（tracer 慣習確認）、`tests/{test_listen_reconnect,mock_pg_server}.nim`、`README.md`、`async_postgres.nimble`、`.github/workflows/*`。文脈構築・レビュー・検証で計20のサブエージェントを使用（9観点＋検証6＋文脈5）。
- **参照した明文の規約**: AGENTS.md/CLAUDE.md/CONTRIBUTING.md は**存在しない**。実質強制は nph フォーマッタ（`.github/workflows/nph.yml`）と `nimble test` 両バックエンド（`async_postgres.nimble`）のみ。観点9エージェントが nph clean（4 files unchanged）と両バックエンド共通定義（async_backend.nim）を確認。
- **既存コードから推定した慣習**（観点9-b の判断根拠。推定誤りなら訂正ください）:
  - teardown で飲み込んだエラー/不可視状態は tracer へ流す — 観測: `buffer_io.nim:589-613`（closeTransport が5箇所の closeWait 失敗を `fireTransportCloseError` へ）、`types.nim:900-921`（fireCleanupSkipped / tracer doc）、`pg_advisory_lock.nim:412,414`、`pg_pool.nim:254-260`（reportCloseError「tracing is the only signal operators have」）。Medium 1 の根拠。
  - `PgTimeoutError` は「`PgConnectionError` サブタイプ、毒化（csClosed）して raise」の意味論 — 観測: `simple_query.nim:226,244-248`（awaitOrInvalidate）、`pg_pool.nim:2065,2072`。本コミットの stopListening 使用はこれと整合。
  - `cancelAndWait` は asyncdispatch で no-op、確立慣習は `when hasChronos` ガードまたは協調フラグ＋deadline — 観測: `lifecycle.nim:474-475`、`pg_client/pipeline.nim:392-395`、`pg_pool.nim:2241`。`abortListenTask`（notify.nim:297）の裸 cancelAndWait は既存の逸脱（本コミット対象外）。
  - module-level 状態は最小・非公開＋doc 化 — 観測: `notify.nim:28`（listenBackoffTickMs 非公開 const）、`types.nim:735-741`。可変 `var` のグローバルは `listenReconnectStopWaitMs` が初だが、9-b の「3箇所以上の一貫性」を満たさず指摘には至らなかった。
  - README は例外型を一切文書化せず、doc コメントが一次ソース — 観測: README.md 全体に PgError 系言及ゼロ、README.md:170-172 で生成ドキュメントへリンク。V6-F1（README 欠落）反証の根拠。
- **十分に検証できなかった領域とその理由**: (a) asyncdispatch の実際のスケジューリング順序に依存するレース（指摘2の通常パス）は、構造違反は確定したが実行時発現の頻度は静的解析では確定不能。(b) orphan の永久リーク（確認したい点2）は `connectTimeout` 未設定時の外部サーバ挙動に依存し、ビルド/実行環境で実証していない。(c) chronos での `reconnectInPlace:78` 到達性（確認したい点3）は決定論的テストが無く未検証。(d) Nim コンパイル/テストは実行していない（読み取り専用レビューのため）。nph 準拠は観点9エージェントが nph 実行で確認済み。
- **検出後に検証で破棄した指摘の件数**: 2 件（観点6「README/examples 未更新」＝ドキュメント慣習により反証、観点7「flag クリア条件の分散」＝集約は非現実的・invariant は既に doc に集約済みで好み/細部に該当し反証）。加えて観点5「PgTimeoutError surface」は検証の結果「互換性維持（サブタイプで既存 catch が透過追随）」と確定し指摘としては不採用、観点2の復活レース本体は既存問題と確定したため doc 矛盾（Low 2）へ再スコープ。
