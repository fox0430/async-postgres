# Audit Findings: async_postgres/pg_types/decoding.nim

対象: `async_postgres/pg_types/decoding.nim` (935行) — サーバ由来データの遅延デコード（Row アクセサから呼ばれる信頼境界#2）。
脅威モデル: サーバ（または MITM）が malformed なテキスト/バイナリ値を返す。
方法: 全 26 公開 proc を境界条件/エラーパス/セキュリティ/正当性/公開境界の観点で精査。疑わしい点は
Nim の実セマンティクスを `/tmp` で実測して確定。既存レビュー `reviews/review_decoding.md` を突合。

総括（系統カウント）: 報告対象 **0件**（旧 F1 は対処済み、下記参照）。

バイナリデコーダ群（hstore/numeric/array/composite/tsvector/tsquery/inet/point/time/timestamp/date）の
境界・over-read・巨大確保防護は一貫して硬化済（count を `(data.len - X) div Y` で事前抑止 + 要素ごとの
pos 検査）。over-read / int overflow / 巨大確保の未防護箇所は発見せず。

## 対処済み（削除）

- **旧 F1 `decodeNumericBinary` の base-10000 digit 値域未検証**（decoding.nim:89-91）:
  各 digit が `[0, 9999]` の範囲外なら `PgTypeError` を送出するよう修正。
  encoding.nim:681 の「Mirror decodeNumericBinary」コメントが正確になる。test_types.nim に負テスト 2 件追加
  （digit=10000 と digit=-1 の拒否）。

---

## 調査したが所見としなかった項目（透明性のため記録）

- **既存レビューの誤り（コード缺陷ではない）**: `reviews/review_decoding.md:56-61` は
  `decodeBinaryTimestamp` の `if fracUs < 0` 分岐（decoding.nim:122-124）を「到達不能なデッドコード」と主張するが、
  誤り。Nim の `mod` は truncating div に従い被除数の符号を引き継ぐため、1970 年以前（unixUs < 0）で
  `unixUs mod 1_000_000` は負になる。実測: unixUs=-500000 → fracUs=-500000（分岐到達）、正規化後
  unixSec=-1, fracUs=500000 → `1969-12-31 23:59:59.500000`（正确）。コードは正しく、削除すると regression になる。
  レビューの「Nim の div は床除算」という前提自体が誤り（Bug 1 項の own 訂正と矛盾）。
- **parseTimeTzText のオフセット成分未検証**（decoding.nim:454-466）: offM/offS の 0..59 検査が無く "+00:99"
  を受容する。ただしバイナリ経路 `decodeBinaryTimeTz`（174-188）も int32.low 以外は無制限に受容するため
  経路間非対称ではなく、PgTimeTz.utcOffset が int32 秒を保持する設計とも整合。単発 Low のため所見から除外。
- **parseIntervalText の時刻部重複**（decoding.nim:632-637）: 時刻部が 2 回現れると後者が前者を上書きする。
  PostgreSQL の default intervalstyle は時刻部を 1 回しか出力せず、self-delimiting なパーサの設計上
  想定の範囲外。単発 Low のため除外。
- **decodeBinaryTsQuery の ntokens 不使用**（decoding.nim:854-860）: ntokens は負/零のみ検査し実 parse は
  トークン型駆動で ntokens を強制しない。ただし再帰パーサは data.len と depth(1000) で完全に境界され
  over-read/クラッシュは無し。self-delimiting 設計として許容範囲と判断。
- **parseTimestampText/parseDateText の例外契約**: `raises: [CatchableError]` 宣言で `except TimeParseError, IndexDefect`
  のみ捕捉。`times.parse` が他に ValueError 等を漏らす可能性を実測で探ったが、空文字/不正月日/巨大年
  いずれも TimeParseError のみで、漏洩を実証できず。**未調査**（実証不足のため所見とせず）。
