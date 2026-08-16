# Tier 2 監査: advisory_lock / largeobject / pool_cluster / types / errors / bytes

---

- 分類: 公開境界（エラー契約）
- 重大度: Medium
- 確信度: 確定
- 場所: async_postgres/pg_errors.nim:100-103, async_postgres/pg_pool.nim:838,856,859,1002,1003,1058,1394,1420,1457,1489
- 事象: `PgPoolError` は全プール失敗を単一の例外型で送出する。サブタイプも enum フィールドもなく、呼び出し元が「pool closed」（永続・リトライ無意味）と「acquire timeout」（一過性・リトライ可能）を判別するには `.msg` 文字列照合が必要。
- 根拠:
  ```nim
  # pg_errors.nim:100-103
  PgPoolError* = object of PgError
    ## Pool-level acquire failure: acquire timeout, pool closed, waiter queue
    ## full, or a failed connect attempt during acquire
  ```
  送出メッセージの種類（`rg "raise newException\(PgPoolError" async_postgres/`）:
  - "Pool is closed" × 5 箇所
  - "Pool acquire timeout" × 3 箇所
  - "Pool connect failed" × 1 箇所
  - "Pool connect for waiter failed" × 1 箇所
  - "Pool closed" × 2 箇所（close 内の waiter 失敗）
  - "Pool cluster fallback acquire timeout" × 1 箇所（pg_pool_cluster.nim:233）
  - "Failed to acquire connection for batch" × 1 箇所
  合計 14 箇所、少なくとも 5 種の意味的に異なる失敗が同一型。
  対照: `PgQueryError` は `sqlState` フィールドでプログラム的判別を提供。`PgTimeoutError` / `PgProtocolError` はサブタイプで分離。
- 系統性: 単発（PgPoolError のみ。他エラー型はサブタイプまたはフィールドで判別可能）

---

- 分類: セキュリティ（境界条件 / -d:danger）
- 重大度: Low
- 確信度: 中
- 場所: async_postgres/pg_bytes.nim:53-67
- 事象: `fromBE16` / `fromBE32` / `fromBE64` は `data[offset]` の直接インデックス参照で境界チェックを持たない。同ファイルの `writeBytesAt`（line 81-87）、`readString`（line 99-104）、`readBytes`（line 112-117）は `-d:danger` 下での `addr` による境界チェックスキップを明示的にガードしているが、`fromBE*` には同等のガードがない。`-d:danger` コンパイル時、悪意あるサーバが内部長フィールドを改竄したメッセージを送信した場合、型デコーダ経由で OOB 読み取りが発生し得る。
- 根拠:
  ```nim
  # pg_bytes.nim:57-60 — ガードなし
  func fromBE32*(data: openArray[byte], offset = 0): int32 {.inline.} =
    int32(data[offset]) shl 24 or int32(data[offset + 1]) shl 16 or
      int32(data[offset + 2]) shl 8 or int32(data[offset + 3])
  ```
  ```nim
  # pg_bytes.nim:81-87 — 対照: 明示ガードあり
  if src.len > 0:
    if pos < 0 or src.len > dst.len - pos:
      raise newException(PgProtocolError, ...)
  ```
  通常コンパイルでは Nim のインデックスチェックが機能するため、`-d:danger` 限定。`fromBE*` の呼び出しは 74 箇所（`rg -c "fromBE(16|32|64)" async_postgres/`）。プロトコルパーサ（pg_protocol.nim:1189-1209）はメッセージフレーミングを検証するが、メッセージ内部のサブフィールド長まで検証しないパスが存在する（pg_types/decoding.nim 等）。
- 系統性: 同種パターン（ガード欠落）。`fromBE16` × 1、`fromBE32` × 1、`fromBE64` × 1 の定義に対し、ガード付きは `writeBytesAt` / `readString` / `readBytes` の 3 つ。呼び出し 74 箇所が影響範囲。

---

- 分類: 公開境界（ConnConfig の公開表面）
- 重大度: Low
- 確信度: 確定
- 場所: async_postgres/pg_connection/types.nim:109-182
- 事象: `ConnConfig` は `password*: string` を含む全フィールドを `*`（公開）で导出する。`sslKey*`（line 133）は PEM 秘密鍵のパス文字列であり、`password` と同等の機密性を持つが、doc コメント以外にアクセス制御やゼロ化の仕組みがない。`maxMessageSize`（line 171）のデフォルト 0 は 1 GiB（`DefaultMaxBackendMessageLen` = 1024*1024*1024, pg_protocol.nim:297）を意味し、悪意あるサーバが 1 GiB の recv バッファ確保を強制できる。
- 根拠:
  ```nim
  # types.nim:171-177
  maxMessageSize*: int
    ## Upper bound (in bytes) on a single backend message including
    ## its 1-byte type and 4-byte length header. A server claiming a
    ## larger message is rejected with `PgProtocolError` before any
    ## further recv-buffer growth, capping memory exposure to a
    ## misbehaving or malicious peer. ``0`` (default) selects
    ## `DefaultMaxBackendMessageLen` (1 GiB).
  ```
  1 GiB は DoS 耐性として大きい。libpq のデフォルトは無制限ではない（`PQsetSingleRowMode` 等は別機構）。ただし doc で明示されており、設定で下げられるため重大度は Low。
- 系統性: 単発
