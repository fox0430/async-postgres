# tests/test_largeobject.nim レビュー結論

## 指摘1: `loWriteStreamDeadline` に `chunkSize = -1` のテストがない

**判定: Low / 実質リスクなし**

`loWriteStream` (`pg_largeobject.nim:356`) と `loWriteStreamDeadline` (`pg_largeobject.nim:474`) のガードは同一 (`chunkSize <= 0 or chunkSize > high(int32)`)。
`chunkSize = 0` のテストが既に `<= 0` 分岐を行使しており、`-1` を追加しても新しいコードパスはカバーされない。
対称性・可読性の観点で追加してもよいが、リグレッション検出の実益は薄い。

## 指摘2: `discard await lo.loWrite(bigData)` (test:534)

**判定: Low / 現実的に発生しない**

`loWrite` (`pg_largeobject.nim:165`) は PostgreSQL の `lowrite()` を1回呼び出す。
`lowrite` は POSIX `write()` と異なり部分書き込みをせず、内部の `inv_write` がバッファ全体を書く。失敗時は例外が伝播する。
部分書き込みが起きて `discard` が問題を隠すシナリオは現実的に存在しない。
仮に書き込みが失敗しても、後続の `loReadAllDeadline` が期待する 1MB を読み取れずテストが失敗するため、間接的には検出される。

## カバレッジ

公開proc 21個中、テストでカバーされているのは 19個。未カバーは以下の2つ:

| Proc       | 行                      | 状態     |
|------------|-------------------------|----------|
| `loImport` | `pg_largeobject.nim:209` | テストなし |
| `loExport` | `pg_largeobject.nim:218` | テストなし |

これらはサーバ側のファイルシステムにアクセスするため、テスト環境の制約で意図的に除外されている可能性がある。
それ以外のprocは全て、正常系・異常系（chunkSize検証、64ビットオフセット、タイムアウト発火含む）のテストが存在する。

## 総合判断

バグ・セキュリティ問題なし。指摘2件はともに Low / Nice-to-have。
