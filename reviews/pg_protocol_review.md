# pg_protocol.nim レビュー指摘事項

## 1. `reuseRowData` のドキュメントコメントが不正確 (中) — 対応済み

**箇所:** `async_postgres/pg_protocol.nim:1022-1024, 1035-1039`

**ステータス:** 04911c8 (PR #570) で main へマージ済。両オーバーロードの doc を更新し、"left intact" 記述と存在しない `QueryResult` 参照を削除。`rd` は valid ref のままだが `buf`/`cellIndex` (第2オーバーロードでは加えて `colFormats`/`colTypeOids`) が空になる旨を明記。

以下は当初レビュー内容 (記録保持用)。

```nim
## The old RowData (and any QueryResult still referencing it) is left intact.
```

### 結論: 有効 — 修正推奨

`RowData` は `ref object` (141行目) であるため、`reuseRowData(rd: RowData, ...)` の引数は参照のコピーであり、呼び出し元と同じオブジェクトを指す。
`move rd.buf` / `move rd.cellIndex` は共有オブジェクトのフィールドを空にするため、呼び出し元の RowData も影響を受ける。

テスト (`tests/test_rowdata.nim:192-193`) がこれを裏付けている:

```nim
check rd1.buf.len == 0
check rd1.cellIndex.len == 0
```

"left intact" という記述は誤り。古い RowData のバッファは move により空になり、以降データアクセスには使用できないことを明記すべき。

現時点で本番コードでの使用はなくテストのみのため、実害はないが将来の誤用リスクがある。

### 修正案 (当初提示)

```nim
## Create a new RowData that takes over the old buffer's capacity via move.
## The old RowData's buf and cellIndex are drained (moved); it must not be
## used for data access afterward.
```

### 適用済み内容

```nim
## Create a new RowData that takes over the old buffer's capacity via move.
## `rd` remains a valid ref but its `buf`/`cellIndex` are emptied — callers
## still holding `rd` must not read row data through it after this call.
```

第2オーバーロード (line 1035) にも同趣旨のコメントを追加し、追加で move される `colFormats`/`colTypeOids` を列挙。

---

## 2. `addBindRaw` の int32 オーバーフロー (低)

**箇所:** `async_postgres/pg_protocol.nim:658`

```nim
buf.writeBytesAt(oldLen, paramData.toOpenArray(r.off, r.off + r.len - 1))
```

### 結論: 理論上有効・実害なし — 任意の防御的修正

`r.off`, `r.len` は `int32` (618行目)。`r.off + r.len - 1` は int32 演算で計算される。
650行目の境界チェックは int64 を使用しているため検証は正しく通るが、658行目のインデックス計算でオーバーフローし得る。

```nim
# 650行目: 正しい (int64)
if r.off.int64 + r.len.int64 > paramData.len.int64:

# 658行目: int32 でオーバーフローし得る
paramData.toOpenArray(r.off, r.off + r.len - 1)
```

発生条件: `paramData` が ~2 GiB 超 かつ `r.off + r.len > int32.high`。
PostgreSQL の1値あたり ~1 GiB 制限、および `parseDataRowInto` 内の 2 GiB チェック (1119行目) を考慮すると実質的に発生不可能。

### 修正案 (任意)

```nim
buf.writeBytesAt(oldLen, paramData.toOpenArray(int(r.off), int(r.off) + int(r.len) - 1))
```
