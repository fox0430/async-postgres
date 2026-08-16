# pg_pool.nim レビュー調査結果

## 指摘1: ~~resetSession の冗長 close パス (line 354-358)

### 実行フロー

1. `resetSession` の `CatchableError` ハンドラ (line 354-358) が `tracedClose(conn)` → `conn.close()` を呼び、`conn.state = csClosed` に設定
2. `resetSessionAndRelease` の `finally: conn.release()` → `releaseCore` (line 673) で `conn.state != csReady` が true → `closeNoWait(conn)` が再度呼ばれる
3. `closeNoWait` は `metrics.closeCount.inc` + 新たな `doClose()` タスクを `asyncSpawn`

`conn.close()` が2回呼ばれ、不要なバックグラウンドタスクが1つ生成される。

一方 `CancelledError` パス (line 347-353) は `conn.state = csClosed` だけ同期設定し、close は `releaseCore` に委譲している。コメント (line 350-351) も「calling it here as well would double-count metrics」と明記。

### 修正方針

CatchableError ハンドラを CancelledError パスと対称にし、`conn.state = csClosed` のみ設定する。機能的バグではないが、冗長なタスク生成とソケット close の二重呼び出しを解消できる。

---

## 指摘2: asyncdispatch の cancelAndWait no-op と close() の競合 (line 2230-2231)

**判定: 妥当 (Low / ドキュメント改善) / コード変更不要**

### 問題

`async_backend.nim:193-204` で asyncdispatch の `cancelAndWait` は `discard` のみ。`close()` 中にメンテナンスループが replenish フェーズ (`await allFutures(connectFuts)`, line 583) の場合:

- `close()` は `pendingBackgroundTasks` の drain ループ (line 2286-2300) を抜ける
- その後メンテナンスループが connect 完了 → `pool.closed` 検知 → `closeNoWait(conn)` で新タスク追加
- このタスクは `close()` 返回後に完了する

`close()` の drain ループは snapshot-and-clear 方式だが、最終チェック後に追加されたタスクは取りこぼす。コネクション自体は `asyncSpawn` で最終的に close されるためリークにはならないが、`close()` 返回時点でのリソース解放保証が asyncdispatch では弱くなる。

### 修正方針

`close()` の doc comment に asyncdispatch でのこの振る舞いを追記するのが適切。コード変更は不要。

---

## 指摘3: dispatchHomogeneous の CancelledError 捕捉 (line 1274-1279)

**判定: 妥当だが現状実害なし (Low) / 防御的修正 or コメント**

### 問題

`dispatchBatchImpl` は `asyncSpawn` (line 1367) で fire-and-forget され、`pendingBackgroundTasks` に登録されない。したがって:

- `close()` の `cancelAndWait(f)` (line 2298) の対象外
- 外部からこのタスクをキャンセルするパスが現状存在しない

`except CatchableError: break` (line 1278) が `CancelledError` を飲み込むのは事実だが、キャンセルの発生源がないため発火しない。将来 `dispatchBatchImpl` を `pendingBackgroundTasks` で管理するようになると、キャンセル伝播が壊れる。

### 修正方針

防御的に `except CancelledError: raise` を `except CatchableError` の前に追加するか、現状維持でコメントを残すか。優先度は低い。

---

## 指摘4: splitBatchBudget の cap 超過 (line 1186-1187)

**判定: 意図的 (Info) / 対応不要**

### 問題

`cap == 1` のとき finite=1, unlimited=1 で合計2が cap を超える。コメント (line 1183-1184) で意図的と明記済み。

`maxSize == 1` + mixed timeout では、2つの `dispatchHomogeneous` が並行に `acquire()` を試み、一方が `acquireTimeout` (デフォルト30秒) ブロックされてから `except CatchableError: break` で1コネクションに縮退する。この遅延は `maxSize` が極端に小さいプールのエッジケースであり、実用上は `maxSize >= 2` が前提。

### 修正方針

不要。必要なら doc comment に `maxSize == 1` 時の遅延可能性を追記する程度。

---

## まとめ

| # | 判定 | 推奨アクション |
|---|------|---------------|
| 1 | 妥当 | CatchableError ハンドラを `conn.state = csClosed` のみに変更 |
| 2 | 妥当 | `close()` の doc comment に asyncdispatch の制限を追記 |
| 3 | 妥当 (潜在) | 防御的 `except CancelledError: raise` 追加 or コメント |
| 4 | 意図的 | 対応不要 |
