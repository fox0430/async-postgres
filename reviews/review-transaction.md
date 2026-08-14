# レビュー詳細調査: `pg_client/transaction.nim`

## Issue 1: `withSavepoint` のディスアンビギュエーションが `nnkStrLit` のみ

**重要度:** Low (コンパイルエラーになるためサイレントバグにはならない)

**箇所:** `transaction.nim:542`

```nim
if args[0].kind == nnkStrLit:
```

**問題:**
`nnkRStrLit` (`r"..."`) と `nnkTripleStrLit` (`"""..."""`) が未カバー。
`conn.withSavepoint(r"my_sp"): body` と書くと名前ではなく timeout として解釈され、型不一致のコンパイルエラーになる。

**コードベース内の不整合:**
`direct.nim:239` では既に3種類すべてを処理済み:

```nim
if sqlNode.kind in {nnkStrLit, nnkTripleStrLit, nnkRStrLit}:
```

**修正案:**

```nim
if args[0].kind in {nnkStrLit, nnkTripleStrLit, nnkRStrLit}:
```

---

## Issue 2: 非リトライ版 `withTransaction` に `CancelledError` 専用ハンドラがない — 対応済み (branch `fix/withtransaction-cancel-cleanup` / f2867ac、未 PR)

**重要度:** Informational (実用上の影響は軽微だが設計意図との非対称あり)

**箇所:** `transaction.nim:421` vs `transaction.nim:265-268`

**対応:** conn 版 `withTransaction` / `withSavepoint`、pool 版 `withTransaction` /
`withTransactionDeadline` / `withTransactionRetryDeadline` の各 body try に
`except CancelledError as e: raise e` を追加。`buildRollbackCleanup` /
`buildSavepointRollbackCleanup` / `buildDeadlineAwaitAndTimeout` の cleanup-cancel は
swallow して原因例外を再送出する対称形に統一。回帰テスト
`tests/test_transaction_cancel.nim` (chronos-only、3 テスト) を追加。以下は当初レビュー内容
(記録保持用)。

### 動作比較

| パス | CancelledError 時の動作 |
|------|------------------------|
| リトライ版 (L265-268) | 専用ハンドラで即 re-raise、cleanup スキップ、`onCleanupSkipped` 発火なし |
| 非リトライ版 (L421-423) | `CatchableError` で捕捉 → `buildRollbackCleanup` 実行 → ROLLBACK がキャンセルされると L145-149 で `onCleanupSkipped(csrCleanupFailed)` 発火 + cleanup 側の CancelledError を re-raise |

### 具体的な問題

1. 非リトライ版では元の例外 `e` が破棄され、cleanup 側の CancelledError に置き換わる
2. リトライ版のコメント (L266) に "skip the async cleanup (would just re-cancel)" とあり、キャンセル時は cleanup をスキップするのが設計意図
3. 非リトライ版はこの意図に従っていない

### 実害

どちらも `CancelledError` が伝播するので caller からは同じに見える。
ただし `onCleanupSkipped` コールバックの発火有無が異なるため、モニタリングやロギングでこのコールバックを使用している場合、非リトライ版だけ余計なイベントが記録される。

### 修正案

非リトライ版 `withTransaction` にリトライ版と同様の `CancelledError` 専用ハンドラを追加:

```nim
result = quote:
  let `connSym` = `connExpr`
  `connSym`.checkTxIdle()
  try:
    discard await `connSym`.simpleExec(`beginSql`, timeout = `txTimeout`)
    `body`
    discard await `connSym`.simpleExec("COMMIT", timeout = `txTimeout`)
  except CancelledError as `cancelSym`:
    raise `cancelSym`
  except CatchableError as `eSym`:
    `bodyCleanup`
    raise `eSym`
```
