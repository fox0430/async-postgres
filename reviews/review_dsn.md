# pg_connection/dsn.nim レビュー結論

## バグ

なし。libpq のセマンティクスを正確に実装しており、エッジケースのテストカバレッジも十分。

## 確認済みの主要ポイント

- `parsePort`: オーバーフロー・空白・`+` 符号の処理が正しい
- `buildHosts`: host/hostaddr 長不一致ガード、port リスト検証、デフォルト値いずれも正しい
- `openRegularFile`: `O_NONBLOCK` → `fstat` → 解除の順で TOCTOU 窓が閉じている
- `pctDecode`: 境界チェック・ゼロバイト拒否が適切
- `parseUriDsn`: `rfind('@')` によるパスワード内 `@` 対応、IPv6 拒否も正しい
- `parseKeyValueDsn`: エスケープ処理・遅延展開が libpq 互換

## 軽微な所感 (バグではない)

1. `=` のない query param (`?sslmode`) が暗黙にスキップされる (dsn.nim:626-627)
   - libpq と同じ挙動だが typo に気づきにくい可能性。重大度: 極めて低
2. Windows では `sslkey` のパーミッション検証がスキップされる
   - 意図的だがセキュリティ敏感環境では認識の価値あり

## テストカバレッジ

### カバー済み

- `parseDsn` / `parseUriDsn` / `parseKeyValueDsn`: 正常系・異常系ともに広範にカバー
- `parsePort`, `pctDecode`, `buildHosts`, `applyParam`: 間接的だが十分
- `openRegularFile`: FIFO 拒否テストあり (POSIX 限定)
- `readPemFileParam`: 存在しないファイル・パーミッション違反・FIFO をカバー
- `orderedHosts` / `loadBalanceHosts`: シャッフルの性質テストまであり

### 未カバー

| 項目 | 該当箇所 |
|------|----------|
| `initConnConfig` の直接テスト | dsn.nim:648 |
| 空 PEM ファイルの拒否 | dsn.nim:242-243 (`result.len == 0` ガード) |
| `initConnConfig` + `sslmode=disable` + `sslcert` のバリデーション | dsn.nim:706 の `validateClientCertConfig` |
| `openRegularFile` の `fstat` 失敗パス | dsn.nim:198-200 (再現困難) |

実質的なギャップは `initConnConfig` の直接テストと空 PEM ファイルの 2 点。
