# Audit Findings: pg_sql / accessors / array / user_types (Tier 2)

対象:
- `async_postgres/pg_sql.nim` (586) — `sql""` マクロ（コンパイル時 {expr} 抽出・自動パラメータ化）、`?` プレースホルダ（sqlParams）。SQL 注入耐性が契約。
- `async_postgres/pg_types/accessors.nim` (1860) — Row アクセサ（getStr/getInt/...、遅延デコードの出口）。
- `async_postgres/pg_types/array.nim` (183) — PgArray 型・コンストラクタ。
- `async_postgres/pg_types/user_types.nim` (514) — composite/enum/domain マクロ。

方法: 4 ファイルを通読し、疑わしい点は Nim 2.2.10 で `/tmp` 実測して確定（推測で埋めない）。
既存レビュー `reviews/review_pg_sql.md`（全項目実害なし）を踏まえつつ再調査。
Tier1 所見（`.audit/findings/decoding.md` F1 date UTC 非対称 / F2 numeric digit 未検証）は既知として扱い、
本ファイルではアクセサ層で観測される契約違反のみを報告する（root cause が decoding.nim のものはその旨明記）。

総括: 報告対象 **0件**（旧 F1 は対処済み、下記参照）。

pg_sql.nim の注入耐性（{expr} 抽出 / `?` 変換）は全リテラル形式で堅牢であることを実測確認（所見なし）。
詳細は末尾「調査したが所見としなかった項目」。

## 対処済み（削除）

- **旧 F1 `parseCompositeText` の unterminated 引用フィールド黙受**（user_types.nim:220-238）:
  閉じ引用符不在でループを抜けた場合の判定を追加し、閉じ引用直後の非カンマも拒否するよう修正。
  兄弟パーサ（parseTextArray / parseHstoreText）と同型の raise 契約に対称化。test_types.nim に負テスト 2 件追加。

---

## 調査したが所見としなかった項目（透明性のため記録）

- **pg_sql.nim の注入耐性（{expr} 抽出 / `?` 変換）— 実測で堅牢を確認、所見なし。**
  `sqlParseLoop`（47-174）は SQL 側の状態機械（単一引用 / E-string / 二重引用識別子 / `$$`・`$tag$` ドル引用 /
  `--` 行コメント / ネスト可能な `/* */` ブロックコメント）を一貫して管理し、`{` / `?` の特別扱いは
  `sNormal` 状態でのみ実行される。実測（実コード import）:
  - `sqlParams("... b = '?'")` → リテラル内 `?` 保存、外部 `?` → `$1`。
  - E-string（`E'\'?\''`）/ ドル引用（`$$ ? $$`, `$tag$ ? $tag$`）/ 二重引用識別子（`"?"`）/ 行・ネストブロックコメント
    内の `?` は全て保存、外部のみ `$N`。`??` → `?`、`?|`/`?&` 演算子保存。
  - `sql"SELECT '{notaplaceholder}' , {minAge}"` → SQL リテラル内 `{...}` は抽出されず、外部 `{minAge}` のみ `$1`
    （注入耐性の核心。実測 query=`SELECT '{notaplaceholder}' , $1`, params=1）。`{{literal}}` → `{literal}`。
  既存レビュー review_pg_sql.md の 4 指摘（`?` 演算子セット / char リテラルスキップ / typedesc 転送 / E-string 検出）
  も実測と整合。raw/triple/char リテラルの `{expr}` 内扱いはプロジェクトのテスト（tests/test_sql.nim:247 等）で
  カバー済。`{expr}` 内の `"…"`/`r"…"`/`"""…"""`/`# …`/`#[ … ]#`/char リテラル走査（248-322）は
  `}` の早期閉鎖を正しく抑止すると読解で確認。
- **pg_sql `?-1` の曖昧さ（単発 Low / ドキュメント済みトレードオフ）— 除外。**
  `sqlParams("SELECT ?-1")` → `?-` を演算子として保存するため `?` がプレースホルダ化されない（実測 `SELECT ?-1`）。
  「パラメータ - 1」の意図だとパラメータ数不一致（サーバ実行時エラー）になるが、沈黙の破損・注入にはならない。
  `?-`/`?#` の防御的保存は review_pg_sql.md 指摘1 とドキュメント（pg_sql.nim:181）で既定。単発 Low のため付録A により除外。
- **accessors の parseInt オーバーフロー — 実測で安全を確認、所見なし。**
  `getInt`/`getInt16`/`getInt64` のテキスト経路は `pgTypeErrorOnValueError`（core.nim:308-318）で `ValueError` を
  `PgTypeError` へ変換。Nim 2.2.10 で `parseutils.parseInt(s, v)` / `parseBiggestInt` のオーバーフローは
  `ValueError: Parsed integer outside of valid range` を送出（実測確定、`OverflowDefect` ではない）。
  したがって捕捉不能 Defect は発生せず、`except PgError` 契約は維持される。コードコメント（accessors.nim:209-212）は正確。
  （`parseAffectedRowsRaw`（accessors.nim:78）の `except ValueError, OverflowDefect` の OverflowDefect 分岐は
  実測上到達不能だが、防御的であり欠陥ではない。）
- **getInt/getFloat の int8/numeric 列に対するテキスト/バイナリ非対称（単発 Low 寄り）— 除外。**
  `getInt` バイナリは clen==4(int4)/2(int2) のみ受理し int8 列（8バイト）は raise、テキストは int32 範囲内なら受理。
  同様に `getFloat` バイナリは float4/8 のみ、テキストは numeric テキストも `pgParseFloat` で受理。
  ただしバイナリ側の raise は「失敗安全」（誤値を返さない）で、getInt は int4 用・getInt64 を使うべきという設計と整合。
  実害が「raise するか受理するか」の差（沈黙の誤値ではない）で単発・Low 寄りのため付録A により除外。
- **array.nim（PgArray 型・コンストラクタ）— 所見なし。**
  `validatePgArrayShape`（48-83）/`expectedElemCount`（24-46）は dims/lowerBounds/elements 整合性、`PgArrayMaxDim`、
  int32 積オーバーフロー、ndim>0 での 0 次元を `PgTypeError` で検証。空配列（dims=@[]）規約も一貫。
- **getDomain / pgDomain / pgEnum / pgComposite マクロ — 所見なし。**
  `getDomain`（488-507）は distinctBase で分岐し未対応基底型はコンパイル時 `{.error.}`（float32 等は意図的未対応、文書化）。
  `pgComposite`（316-336）の `compositeFieldToText`（285-304）は空文字列・`NULL`（大文字小文字無視）・`,()"\ `・
  空白を含む値を引用し、`"`→`""`・`\`→`\\` をエスケープ（過剰引用は安全、不足引用は無し）。
  `getComposite` バイナリ経路（412-444）は `decodeBinaryField`（381-410）で OID（checkFieldOid）と長さ
  （checkFieldLen）を検証し、同幅型の取り違えを抑制。フィード数不一致も双方向で raise。
- **optAccessor の None 返却条件 — 所見なし。**
  `optAccessor`（849-865）は `row.isNull(col)` のみで `none` を返す。`isNull`（103-110）は col 境界を検証し
  （越界は `PgTypeError`、catchable）、`scale` 引数は `compiles` 検出で getMoney 系へ転送（fix 71f9e63 と整合、getMoneyArray/
  getMoneyArrayND の Opt も scale 転送）。NULL 以外での黙った None は無し。
