# Audit Findings: async_postgres/pg_protocol.nim

対象: `async_postgres/pg_protocol.nim` (1374行) — PostgreSQL ワイヤプロトコル v3 の encode/decode。
焦点: 受信信頼境界（`parseBackendMessage`:1166 と型別パーサ群）が、信頼できない長さフィールドに対して
メモリ上限制御と 32-bit 安全性を貫けているか。fan-in 28（全模块最大）。

総括（系統カウント）: 所見は **1系統**（系統A は対処済みにつき削除）。
- 系統B（Low / 4箇所・32-bit 限定）: サーバ由来 int32 を `int` 加算する際、int64 拡張されていない箇所が **4箇所**
  （856, 1122, 1156, 1207）。コードは 974 と 1201 の2箇所のみ意図的に int64 拡張しており、32-bit 硬化が不完全。

対照: int16 カウントのパーサ（`parseDataRow`:844, `parseRowDescription`:914, `parseParameterDescription`:955,
`parseCopyResponse`:996）は要素数が最大 32767 に天然上限化され安全。

対処済み（削除）: 系統A（Medium / 3箇所） — parseErrorOrNotice / parseAuthentication SASL /
parseNegotiateProtocolVersion の可変長要素蓄積に上限を導入し、catchable な `PgProtocolError` で拒否する
形に修正済み（`MaxErrorOrNoticeFields = 128`, `MaxSaslMechanisms = 64`, `MaxNegotiateProtocolOptions = 1024`）。

---

- 分類: 境界条件とエラーパス（正当性 / 設計整合性）
- 重大度: Low
- 確信度: 高
- 場所: async_postgres/pg_protocol.nim:856,858（parseDataRow）、1122,1156（parseDataRowInto）、1207（parseBackendMessage）
- 事象:
  32-bit ビルド（`int` = 32-bit）で、サーバ由来の int32 長を `int` オフセットに加算する境界チェックが
  比較前にオーバーフローし、切り詰め/サイズ検査を迂回し得る。結果として安全ビルドでは捕捉不能の
  `IndexDefect`/`RangeDefect`、`-d:danger` では over-read に至る。コードは 974 と 1201 の2箇所を
  int64 へ意図的に拡張し「32-bit でオーバーフローしない」と注釈（1197-1200）しているが、
  下記の4箇所は拡張されておらず、32-bit 硬化が不完全。64-bit（int=int64）ではオーバーフローせず実害なし
  （既存レビュー #2 の addBindRaw int32 と同系統・同様に実害可能性は低いが、こちらは decode 側の信頼境界）。
- 根拠: コード引用と推論
  ```nim
  # 1197-1200 の注釈は int64 拡張を明言するが:
  if maxLen > 0 and int64(msgLen) >= int64(maxLen):   # 1201: 拡張済み
  ...
  let totalLen = int(msgLen) + 1            # 1207: 未拡張（maxLen<=0 のテスト経路で msgLen=int32.high 時溢出）
  ```
  ```nim
  # parseDataRow (856,858) — notify ポンプの full-decode 経路で本番到達可能
  if offset + colLen > body.len:            # 856: offset:int + colLen:int32
    raise ...
  result.columns[i] = some(@(body.toOpenArray(offset, offset + colLen - 1)))   # 858
  ```
  32-bit で offset(<1 GiB) + colLen(int32.high=2147483647) は int32 を溢出し負にラップ →
  `負 > body.len` は偽となり検査を迂回、858 の `toOpenArray` が範囲外インデックスで Defect/over-read。
  到達経路: `notify.nim:167` の `recvMessage()` → `nextMessage(rowData=nil, skipDataRow=false)` →
  `parseBackendMessage` の 'D' 分岐（1237）→ `parseDataRow`。Rogue サーバは LISTEN 中でも 'D' を送れる。
  ```nim
  # parseDataRowInto (1122,1156)
  if bufBase + dataLen > int32.high:        # 1122: 検査そのものが 32-bit で溢出し得る
  ...
  if pos + colLen > bufEnd:                 # 1156: pos:int + colLen:int32、1122 を抜けても独立に溢出
  ```
  1122 は bufBase(<=int32.high) + dataLen(<1 GiB) が 32-bit で溢出し比較前に負ラップ → 2 GiB ガード迂回。
  1156 も pos(<=bufEnd) + colLen(int32.high) が最大約4 GiB となり独立に溢出する。
- 系統性: 同種パターン。grep `offset \+ colLen|bufBase \+ dataLen|pos \+ colLen|int\(msgLen\)` で
  未拡張の加算 **4箇所**（856, 1122, 1156, 1207；858/1132/1138 はその下流使用）。
  対照: int64 拡張済みは grep `int64\(` → **2箇所**（974, 1201）のみ。全て 32-bit ビルド限定。
