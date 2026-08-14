# pg_replication.nim 監査所見

対象: `async_postgres/pg_replication.nim` (1387行) / v0.3.0 / 変更37
観点: セキュリティ・信頼境界・公開API・依存

## 調査済み・所見なし（背景）

以下は精査したが問題を認めなかった（推測で埋めないため明記）:

- pgoutput デコーダ (`parsePgOutputMessage` 354-488, `decodeTuple` 330-352):
  全整数読み取りが `readInt16At/32At/64At` → `ensureAvail` (284-293) 経由で
  `PgProtocolError`（捕捉可能）を送出。`fromBE16/32/64` (pg_bytes.nim:53-67) は
  境界チェックせず `IndexDefect` を投げ得るが、デコーダは必ずラッパ経由で
  利用しており直接呼び出しはない。
- 確保増幅 (d): tuple 列数は `readColumnCountAt` (317-323) が `MaxRelationColumns`
  (1600) で上限制御。Truncate の `numRels` は `(data.len - pos) div 4` で残り
  バッファ内に制限 (468-469)。't'/'b' フィールドの `dataLen` は `readBytesAt` →
  `ensureAvail` が `n < 0` と `n > buf.len - pos` を拒否 (288)。さらに CopyData を
  含む全 backend メッセージは framing 層で `effectiveMaxMessageSize`（既定 1 GiB =
  `DefaultMaxBackendMessageLen`, pg_protocol.nim:297）により `parseBackendMessage`
  (pg_protocol.nim:1201) で上限制御。よって単一メッセージ内の確保はバッファ尺寸
  に有界で、増幅は成立しない。
- `parseReplicationMessage` (688-721) の生 `decodeInt64` 直接呼び出し (700-702,
  716-717) は、先行する `copyData.len < 25` / `< 18` ガード (697, 713) が全アクセス
  範囲（最大 offset 17 + 8 = index 24）を被覆するため安全。
- LSN 解析 `parseLsn` (240-263): 空 half・16 有効桁超・32bit 超を `PgTypeError`
  化。`parseTimelineId` (563-575): narrow 前に範囲チェックし `RangeDefect` を回避。
  `receivedEndLsn` (490-502): uint64 加算前に overflow チェック。
- LSN 状態管理 (types.nim:796-833): `updateReplMaxReceivedLsn` は単調前進のみ、
  `confirmReplFlushed` は received へ clamp 後単調前進。偽造された小さな startLsn
  では後退せず、巨大 startLsn も overflow チェック済み。
- ストリーム中断時の状態 (b/c): `invalidateAbandonedStream` (1002-1023) が
  `csBusy`/`csReplicating` を `csClosed` に毒化。`sendMsg`/`fillRecvBuf` 自体も
  輸送失敗時に `csClosed` 化（buffer_io.nim:210-213, 551-553）。`startReplication`
  の `conn.state = csBusy` (1288) 直後の `sendMsg` 失敗も sendMsg 側で csClosed 化
  されるため strand なし。asyncdispatch/chronos 非対称は `replFillRecvBuf`
  (876-941) に文書化された設計差であり、server keepalive への自動応答は両方で
  機能するため実害ある非対称は確認できず。

---

## 対処済み（削除）

- 元・所見1 (Medium): 4 proc（identifySystem, decodeCreateSlotRow, readReplicationSlot,
  timelineHistory）の固定列アクセス 11 箇所が列数未検証で `IndexDefect` を漏出。→
  各 proc に `qr.fields.len < N` 事前検証を追加し、catchable な `PgConnectionError` を
  送出する形に修正済み。
