# test_rowdata.nim レビュー結論

## コードレビュー指摘

### 1. テスト名と検証内容の矛盾 (低〜中)

- **Line 180** `"old RowData remains intact after reuse"`: 実際にはmoveで空になることを検証している。
- **Line 199** `"old RowData with data preserved when previous result held"`: 実際には `rd1 != rd2` (異なるref) のみを検証。
- 提案: `"reuse moves buffers out of the old RowData"` 等にリネーム。

### 2. overflow guardテストが約2 GiB確保 (低)

- Lines 291–312: `setLen(int32.high)` 付近のseqを3つ確保。CIのメモリ上限が低いと失敗しうる。

### 3. NULLセンチネル `\xFF` の制限 (情報)

- Line 10: 将来 `0xFF` を含むバイナリデータを非NULLとしてテストしたい場合、このヘルパーでは表現不可。現時点で実害なし。

## カバレッジ分析

### カバー済み

- `clone` — 全分岐 (nil, NULL, 空文字列, 通常データ, マルチ行, colFormats/colTypeOids/fields複製)
- `reuseRowData` — 全分岐 (capacity保持, colFormats更新, 複数サイクル, 旧参照整合性)
- `parseDataRowInto` — numCols mismatch, int32.high overflow, ロールバック, NULL/空文字列

### 未カバー

| パス | 備考 |
|------|------|
| `parseDataRowInto`: "message too short" / "invalid column count (負値)" / "unexpected end" / "invalid column length" / "truncated" | fuzzテストで間接カバーのみ。決定的テストなし |
| 上記エラー時のロールバック (cellIndex/bufのsetLen) | numCols mismatchとoverflow以外は未検証 |
| `buildResultFormats` | 全テストファイルで未テスト |
| `dataLen == 0` (0列DataRow) | 境界ケース未テスト |
| accessors群 (`cellInfo`, `[]`, `isNull`, `toRow`等) | test_types.nimで間接カバー |

### 追加推奨テスト

1. `parseDataRowInto` の各エラー分岐に対する決定的なユニットテスト + ロールバック検証
2. `buildResultFormats` の直接テスト
3. 0列DataRow (`body = [0x00, 0x00]`) の境界テスト
