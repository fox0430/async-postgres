# pg_client/copy.nim レビュー調査結果

明確なバグは検出されず。プロトコル同期・エラーハンドリング・バッチ分割のロジックはいずれも正しく実装されている。以下、軽微な所見2点の詳細調査。

---

## 指摘1: copyInStreamImpl の送信サイズ上界がコールバック依存 (line 281, 292, 307)

**判定: バグではない / ドキュメント追記推奨 (Low)**

### 問題

`copyInRawImpl` (line 115) は `maxPayload = copyBatchSize - 5` で入力を事前スライスし、`encodeCopyData` 1回あたり最大 `copyBatchSize` (256KB) しか sendBuf に追加しない。sendBuf は常に 256KB 以下に保たれる。

一方 `copyInStreamImpl` (line 292) はコールバックの返す `chunk` をそのまま `encodeCopyData(conn.sendBuf, chunk)` に渡し、スライスしない。sendBuf はフラッシュ閾値未満で蓄積されるため、最大サイズは:

```
(copyBatchSize - 1) + 5 + chunk.len  ≈  256KB + chunk.len
```

コールバックが 10MB のチャンクを返せば sendBuf は約 10.25MB まで成長し、1回の `sendBufMsg` で送信される。メモリピークは `chunk + sendBuf へのコピー ≈ 2×chunk.len`。

### ガードの範囲

`encodeCopyData` (pg_protocol.nim:761) は `data.len > maxInt32Len - 4` (約2GB) で `ValueError` を送出。これは line 299 の `except CatchableError` で捕捉され `callbackError` → CopyFail パスに正しく流れる。プロトコルの整合性は保たれている。

### doc の不備

line 424 の "bounded by one batch of extra streaming" はサーバーアボート検出の遅延に関する記述であり、送信サイズの上界には言及していない。

### 修正方針

doc に送信サイズ上界がコールバックのチャンクサイズに依存する旨を追記する。`copyInRawImpl` のようにチャンクを内部でスライスすれば上界を 256KB に固定できるが、現状はコールバック側の責任とする設計。コード変更は任意。

---

## 指摘2: copyIn(seq[seq[byte]]) のピークメモリ (lines 227-235)

**判定: バグではない / 設計上のトレードオフとして認識済み (Low)**

### 問題

全チャンクを連結して単一の `seq[byte]` を生成するため、データフローは:

```
元のチャンク群 (data) → combined へ全コピー → copyInRawImpl 内で sendBuf へ再コピー
```

ピークメモリ: `data 全体 + combined (同サイズ) + sendBuf (≤256KB)` ≈ **2×データサイズ + 256KB**

### 一貫性

`openArray[byte]` オーバーロード (line 205) も `let dataCopy = @data` で非同期境界前にコピーしており、パターンとして一貫している。async proc に `openArray` を渡せないための必須コピー。

### 回避可能性

`copyInStream` を内部で使えば連結コピーを回避できるが:

- `copyInStreamImpl` はコールバックエラー時に CopyFail を送るが、`copyInRawImpl` は送信エラーとして扱う。セマンティクスが微妙に変わる。
- doc (line 225) は "Concatenates chunks and delegates" と正直に記述しており、大規模データには `copyInStream` を使うのが想定パス。

### 修正方針

現状維持で問題なし。`copyInStream` への誘導が doc にあれば十分。
