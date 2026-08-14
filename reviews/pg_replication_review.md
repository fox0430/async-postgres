# pg_replication.nim レビュー

## 指摘1: CopyDone 二重送信

**重大度: 中〜低** (プロトコル違反だが現行PGでは実害限定的)

### 問題

`stopReplication` と recvLoop の `bmkCopyDone` ハンドラの両方が CopyDone を送信し、
通常の `stopReplication` フローでも常に二重送信が起きる。

### 再現手順 (通常フロー)

1. コールバック内で `stopReplication` 呼び出し → `pg_replication.nim:1316` で CopyDone 送信
2. コールバック返回後、recvLoop が `replFillRecvBuf` でサーバー応答を待つ
3. サーバーが CopyDone + CommandComplete + ReadyForQuery を返す
4. recvLoop の `bmkCopyDone` ハンドラ (`pg_replication.nim:1084-1093`) が発火 → 再度 CopyDone 送信
5. drainLoop が CommandComplete + ReadyForQuery を処理して `csReady` に遷移

`bmkCopyDone` ハンドラは「サーバー起因停止用」とコメントされているが、
クライアント起因の停止でもサーバーの CopyDone 応答に対して必ず発火する。

### 競合シナリオ (サーバー起因停止との同時発生)

1. サーバーが XLogData の後に CopyDone を送信 (walsender timeout, `pg_terminate_backend` など)
2. クライアントの recvLoop が XLogData を処理 → コールバック内で `stopReplication` → CopyDone 送信済み
3. コールバック返回後、recvLoop の inner while がバッファ済みのサーバー側 CopyDone を処理
4. `bmkCopyDone` ハンドラが再度 Standby Status Update + CopyDone を送信

### 実害が限定的な根拠

- e2e テスト (`test_e2e_misc.nim:257`, `:319`) が `stopReplication` をコールバック内から呼び、
  `csReady` 遷移と後続クエリ (`identifySystem`) の成功を確認済み
- 2回目の CopyDone はサーバーが ReadyForQuery 送信後に到着するため、
  PostgreSQL は黙殺している可能性が高い

### 残るリスク

- プロトコル違反であり、将来の PG バージョンで拒否される可能性
- サーバー起因停止と `stopReplication` が同時に起きた場合、2回目の CopyDone に対する
  ErrorResponse が recv buffer に残留し、後続クエリを汚染する可能性

### 修正方針

`conn` 側に `clientSentCopyDone` フラグを追加し、`stopReplication` 送信時に true、
`bmkCopyDone` ハンドラで true なら CopyDone 再送をスキップする。
`stopReplication` は `runReplicationStream` の外から呼ばれるため、フラグは `conn` 側に持たせる必要がある。

---

## 指摘2: `readReplicationSlot` が `outputPlugin` を設定しない

**重大度: 低** (ドキュメント改善)

### 問題

`READ_REPLICATION_SLOT` の返却列は `slot_type, restart_lsn, restart_tli` の3列
(`pg_replication.nim:657` コメント参照)。`output_plugin` は含まれないため、
`ReplicationSlotInfo.outputPlugin` (`pg_replication.nim:62`) は常に空のまま。

`createReplicationSlot` 経由でのみ値が入るが、`readReplicationSlot` の doc コメントに
その旨の記載がなく、誤解を招く可能性がある。

### 修正方針

`readReplicationSlot` の doc コメントに「`outputPlugin` は設定されない」旨を追記する。

---

## 指摘3: `decodeCreateSlotRow` が行数を検証しない

**重大度: 低** (公開APIのガード不足)

### 問題

`pg_replication.nim:600-607` の `decodeCreateSlotRow` は `*` で公開されているが、
`initRow(qr.data, 0)` で0行目を直接参照し、`rowCount` チェックを行わない。

- 唯一の内部呼び出し元 `createReplicationSlot` (`:625-626`) は `rowCount == 0` を事前にチェック済み
- テスト (`test_replication.nim:753`) は常に `rowCount: 1` で構築

空の `QueryResult` を外部から渡すと不正アクセスになる。

### 修正方針

- **private 化** (推奨): 外部呼び出しの意図が見当たらないため、`*` を外す
- **ガード追加**: `if qr.rowCount == 0: raise ...` を先頭に追加
