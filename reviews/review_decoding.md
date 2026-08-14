# レビュー: `async_postgres/pg_types/decoding.nim`

## Bug 1 (取り下げ): `parseIntervalText` の years 境界チェックは off-by-one ではない

**当初主張 (誤り):** Nim の `div` は床除算のため `int64(int32.low) div 12 = -178956971` となり、
`val = -178956971` がチェックを通過して `val * 12 = -2147483652` が int32.low を下回る。

**訂正:** Nim の `div` は **truncated division (ゼロ方向切り捨て)** であり床除算ではない。
実測で確認 (`nim c -r`):

```
int64(int32.low)  div 12 = -178956970
int64(int32.high) div 12 =  178956970
```

境界の一致:

| val | check pass? | val * 12 | int32 に収まる? |
|---|---|---|---|
| -178956971 | ✗ (raise) | -2147483652 | ✗ |
| -178956970 | ✓ | -2147483640 | ✓ |
|  178956970 | ✓ |  2147483640 | ✓ |
|  178956971 | ✗ (raise) |  2147483652 | ✗ |

check_passes と fitsI32 が境界上で完全一致しており off-by-one は存在しない。
また 12 は `int32.high` / `int32.low` の約数ではないため
`val * 12` が `[2147483641, 2147483647]` や `[-2147483648, -2147483641]` の
値を取ることは原理的にない (すべての整数 val に対し `val * 12` は
`≤ 2147483640` か `≥ 2147483652` のいずれか)。

なお「積を先に計算して int32 範囲と比較」する案に単純に置き換えると、
`val` は `accumDigit` により int64.high 近くまで到達し得るため `val * 12` が
int64 オーバーフローを起こす。安全にするには結局 `val` の int64 事前チェックが
必要で、その場合も現在のチェックと数学的に等価になる。

**結論:** 現行実装のままで正しい。修正不要。

## Bug 2 (低〜中): `parseTextArray` が閉じ引用符の欠落を黙認する

- `decoding.nim:920` — quoted element の while ループが `i >= inner.len` で終了した場合
  （閉じ `"` がない）、そのまま `i += 1` して要素を追加している
- `{"abc}` のような不正入力が `@["abc"]` として正常にパースされてしまう
- `parseHstoreText`（`decoding.nim:501-502`）は同ケースで
  `unterminated key string` を raise しており不整合
- PostgreSQL サーバが不正リテラルを送る可能性は低いが、
  ユーザ入力がこのパスを通る場合は誤った結果を黙って返す
- 修正: ループ終了後に閉じ引用符の有無を検証する

```nim
if i >= inner.len:
  raise newException(PgTypeError, "Invalid array literal: unterminated quoted element")
i += 1 # skip closing quote
result.add(some(elem))
```

## 補足: `decodeBinaryTimestamp` のデッドコード

- `decoding.nim:122-124` — `if fracUs < 0` 分岐は到達不能
- Nim の `mod` は正の除数に対して常に非負を返す（`div` が床除算のため）
- `unixUs mod 1_000_000` が負になることはない
- C 言語の `%` 向けパターンが混入したと思われる。バグではないが冗長
