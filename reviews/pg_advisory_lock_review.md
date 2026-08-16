# pg_advisory_lock.nim レビュー調査結果

## 指摘1: withAdvisoryLockCore の body 内 return/break/continue で unlock がスキップされる (line 390-419)

**判定: 妥当 (Medium) / doc 追記推奨、コード修正は asyncdispatch 制約で困難**

### 問題

`withAdvisoryLockCore` は template として展開されるため、body 内の `return` は呼び出し元 proc を即座に抜け、後続の unlock コードに到達しない。`break`/`continue` が外側ループをターゲットする場合も同様。

```nim
# 展開後の実質的なコード:
await c.advisoryLock(k)
var bodyErr: ref CatchableError = nil
try:
  # body ← ここで return すると以下全部スキップ
except CatchableError as e:
  bodyErr = e
# unlock コード (到達しない)
```

line 361-365 のコメントで `finally` + `await` が asyncdispatch 下で例外を握り潰すため `try/except` を選んだと説明あり。制約は認識済みのトレードオフ。

### 緩和要因

- **プール接続**: `sessionLockDirty` フラグにより、プール返却時に `pg_advisory_unlock_all` が実行される (`pg_pool.nim:325-341`)。実害はほぼなし。
- **スタンドアロン接続**: コネクションクローズまでロック残留。ただしサーバ側でセッション終了時に自動解放。
- **`withAdvisoryLockXact`**: トランザクション終了時に自動解放されるためこの問題は存在しない。

### doc の問題

"then release the lock (even on exception)" は例外のみ言及しており、`return` 等の非例外制御フローでスキップされることに未言及。

### 修正方針

doc に「body 内の `return`/`break`/`continue` は unlock をスキップする」旨の制約を追記する。コード修正は asyncdispatch の制約で困難。body を `block` でラップすればトップレベルの `break` は捕捉できるが、外側ループ向けの `break`/`continue`/`return` は防げない。

---

## 指摘2: lock と unlock に同じ timeout を再利用 (line 379-407)

**判定: 妥当 (Low) / doc 追記で十分**

### 問題

lock 取得と unlock の両方に同じ `t` を渡している。lock がタイムアウトぎりぎりで成功した場合、unlock にも再度フルの `t` が与えられるため、実効的な最大待ち時間は `2 * t` になる。

commit 5023e9f で意図的に unlock へ timeout を伝播させた変更であり、設計判断としては理解できる。

### 修正方針

doc に「実効最大待ち時間は `2 * timeout` になり得る」旨を追記すれば十分。コード変更不要。

---

## 指摘3: ensureXactScope のエラーメッセージが tsInFailedTransaction で不正確 (line 119-124)

**判定: 妥当 (Very Low) / 任意の修正**

### 問題

`tsInFailedTransaction`（直前のクエリエラーで transaction が abort 状態）の場合も "requires an active transaction" と表示される。トランザクション自体は存在しており失敗状態にあるのが実態のため、デバッグ時に誤解を招く可能性がある。

ただしメッセージ末尾に `(txStatus: tsInFailedTransaction)` が含まれるため、実際のデバッグに支障はほぼない。

### 修正方針

任意。修正する場合は `tsInFailedTransaction` 時に "transaction is in a failed state" のようなメッセージに分岐させる。優先度は極めて低い。

---

## 指摘4: unlockSessionLock の counter guard と mixed raw/typed usage (line 112-113)

**判定: 既知・文書化済みの制約 / 対応不要**

### 問題

raw SQL で取得したロックを typed API で解放すると、サーバは `true` を返すため counter が decrement されるが、そのロックは counter に加算されていない。他に typed ロックがあればそれの分を「盗む」形になり、counter が実態より少なく報告される。

### 文書化状況

- module doc (line 42-51) で明確に文書化済み
- `types.nim:302-314` でも説明あり
- `sessionLockDirty` フラグがリーク検知の実判定に使われており、安全性への影響なし
- テスト (`test_advisory_lock.nim:658-659, 790-813`) でもこの挙動を検証済み

### 修正方針

対応不要。設計として承知の上であり、文書化・テスト済み。

---

## 総括

| # | 重大度 | 対応要否 |
|---|--------|----------|
| 1 | Medium | doc 追記推奨（コード修正は asyncdispatch 制約で困難） |
| 2 | Low | doc 追記で十分 |
| 3 | Very Low | 任意 |
| 4 | — | 対応不要（既知・文書化・テスト済み） |

実運用上のリスクは、プール経由で使う限りほぼゼロ。スタンドアロン接続で body 内に `return` を書くケースだけが注意対象。
