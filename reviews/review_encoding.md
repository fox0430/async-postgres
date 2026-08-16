# レビュー: `async_postgres/pg_types/encoding.nim`

## Bug 1 (中): `writeParamFormat(seq[byte])` のフォーマットコード矛盾

- `encoding.nim:1760` — フォーマットコード `0`（テキスト）を書いている
- `encoding.nim:1804` — ペイロードは生のバイナリバイトを送信
- `encoding.nim:134` — `toPgParam(seq[byte])` は正しく `format: 1`（バイナリ）を使用
- `addBindDirect` マクロ（`encoding.nim:1917-1919`）が `writeParamFormat` を呼ぶため、
  `queryDirect`/`execDirect` で `seq[byte]` を使うと Bind メッセージのフォーマット宣言と
  ペイロードが矛盾し、PostgreSQL が `invalid input syntax for type bytea` を返す
- 既存テストに `seq[byte]` + `queryDirect`/`execDirect` の組み合わせがなく、検出されていない
- 修正: `encoding.nim:1761` の `0'i16` を `1'i16` に変更

## Bug 2 (低): `toPgBinaryParam(PgPath/PgPolygon)` に payload サイズガードがない [修正済]

- `encoding.nim:1087` — `data.writeBE32(1, int32(v.points.len))` ガードなし
- `encoding.nim:1095` — `data.writeBE32(0, int32(v.points.len))` ガードなし
- 当初の指摘では「`checkPgBinLen` は配列・hstore・range・composite で使われている」
  としていたが、実際に `checkPgBinLen` を呼ぶのは `encodeBinaryArray`（L294）と
  `encodeHstoreBinary`（L1160/1163/1166）の 2 encoder のみ。fixed-width element を
  扱う `buildFixedArray`（L398）は payload-level ガード
  （`if payload > int32.high.int64: raise ...`）を採用しており、他 encoder が
  「全て `checkPgBinLen`」ではない。
- 実害の閾値は当初想定より低い。`points.len > int32.high`（21億点）で int32 wrap
  だが、wire format の parameter length prefix は int32（2 GiB 上限）なので、
  buffer 全体 `1 + 4 + n*16` / `4 + n*16` が 2 GiB を超える ~134M 点で先に破綻。
  加えて 32-bit プラットフォームでは `points.len * 16` の `int` wrap が `newSeq`
  サイズ算出で発生し得る。
- 修正: `buildFixedArray` と同じ payload-level スタイルに揃え、両 proc 冒頭に
  `checkPgBinPayload(size, "path"/"polygon")` を追加。`size` は int64 演算で計算し
  `newSeq[byte](size.int)` で確保。point-count の int32 キャストは payload 上限
  （16*n ≤ int32.high ⇒ n は int32 に安全）で自動的に包含されるため
  `checkPgBinLen` は不要。
- ステータス: branch `fix/pg_types-path-polygon-binary-payload-guard`（27c7cd4、未 PR）。
  既存の PgPath/PgPolygon happy-path テストおよび roundtrip テスト全 pass。

## 補足: `toPgParamInline(PgUuid)` のデッドコード

- `PgInlineBufSize = 16`（`core.nim:291`）、UUID 文字列は常に36バイト
- `encoding.nim:89` の `elif s.len <= PgInlineBufSize` 分岐は到達不能
- コメント（line 81-82）で既に言及あり。バグではないが冗長
