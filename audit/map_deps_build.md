# 監査: 外部依存とビルド/CI/リリースの健全性 (async_postgres)

- 調査日: 2026-07-31
- 対象リビジョン: HEAD c058f5e (main)
- 調査種別: 読み取り専用監査

---

## 1. 外部依存一覧

### 1.1 nimble 宣言済み (async_postgres.nimble)

| 依存 | 制約 | 役割 | コード内の使用箇所 |
|---|---|---|---|
| nim | `>= 2.2.4` | 言語/コンパイラ | 全体 |
| nimcrypto | `>= 0.7.3` | SCRAM-SHA-256 (pbkdf2/sha256/hmac) + メモリ抹消 (`burnMem`) | `async_postgres/pg_auth.nim` (import pkg/nimcrypto, nimcrypto/pbkdf2, nimcrypto/utils), `pg_connection/lifecycle.nim` (ncutils.burnMem), `pg_saslprep.nim` (ncutils.burnMem), tests/test_auth.nim, tests/test_network_failure.nim |
| checksums | `>= 0.2.2` | MD5 認証ハッシュ (`getMD5`) | `async_postgres/pg_auth.nim:3` (import pkg/checksums/md5) — `md5AuthHash` のみ |
| unicodedb | `>= 0.13.2` | SASLprep の Unicode 性質テーブル | `async_postgres/pg_saslprep.nim:16` (pkg/unicodedb/properties) |
| normalize | `>= 0.9.0` | SASLprep の NFKC 正規化 | `async_postgres/pg_saslprep.nim:15` (pkg/normalize) |

### 1.2 README 言及あり・nimble 未宣言 (chronos バックエンド向けオプション依存)

| 依存 | README 制約 | 役割 | コード内の使用箇所 |
|---|---|---|---|
| chronos | `>= 4.4.0` | 代替 async バックエンド + TLS ストリーム | `async_postgres/async_backend.nim:22`, `pg_connection/{types,notify,buffer_io,ssl}.nim` (chronos/streams/tlsstream), `pg_bearssl.nim:8`, tests/test_pool.nim, tests/test_transaction_cancel.nim |
| nim-bearssl | `>= 0.2.11` | chronos バックエンドの TLS (BearSSL, TLS 1.2 のみ) | `async_postgres/pg_bearssl.nim:9` (bearssl/[x509,rsa,ec,ssl]), `pg_connection/{types,notify,ssl}.nim` (../pg_bearssl), tests/test_ssl.nim (bearssl/abi/bearssl_ssl) |

### 1.3 所見

- **chronos / nim-bearssl が nimble に未宣言**。`-d:asyncBackend=chronos` 時の必須依存だが、
  nimble はこれを解決しない。CI は `nimble install chronos -y` で**バージョン固定なし**に
  手動インストール (test.yml:77)。bearssl は chronos の推移依存として入るだけで、こちらも
  固定なし。README の下限 (chronos 4.4.0 / bearssl 0.2.11) は文書のみで強制力がない。
  → chronos バックエンド利用者は破壊的変更/ALPN 非対応版を掴むリスク。
- **全依存が `>=` の下限のみで上限なし**。nimcrypto/checksums/unicodedb/normalize いずれも
  破壊的変更 (メジャー/0.x の minor) を追跡するリスク。ロックファイル (nimble.lock 相当) は
  リポジトリに存在しない。
- **暗号依存の軽微な重複**: MD5 は nimcrypto にも実装があるが、コードは MD5 のみ
  `checksums` を使用 (pg_auth.nim)。SHA-256/HMAC/PBKDF2 は nimcrypto。機能的重複は小さいが、
  2 つの暗号系パッケージに依存が分裂している。
- asyncdispatch バックエンドの TLS は標準庫の OpenSSL ラッパ (`-d:ssl`, pg_connection/ssl.nim)
  で、外部依存ではない。

---

## 2. CI ワークフローの概要と網羅の穴

### 2.1 概要

| ワークフロー | トリガ | 内容 | 最終更新 (git log) |
|---|---|---|---|
| `.github/workflows/test.yml` | PR + push(main), パスフィルタ | Nim マトリクス **2.2.4 / stable / devel** (OS は ubuntu-latest のみ)。gen_certs.sh → docker compose で PG 起動 → chronos インストール → `nimble test` (asyncdispatch + chronos 両方) → examples を両バックエンドでコンパイル → doc 生成 | 2026-07-14 |
| `.github/workflows/docs.yml` | push(main), パスフィルタ | Nim stable で doc 生成 → GitHub Pages へ deploy-pages で公開 (gh-pages 相当) | 2026-03-08 |
| `.github/workflows/nph.yml` | PR, パスフィルタ | `nph` フォーマットチェック (fail:true) | 2026-03-08 |

- `docker-compose.yml` (最終更新 2026-05-25): **postgres:18** のみ。`test/test/test` の固定
  資格情報、ポート 15432:5432、SSL on (tests/certs の server.crt/key, ca.crt をマウント),
  `wal_level=logical`。init スクリプト `tests/pg_init/10-physical-replication-hba.sh` が
  pg_hba.conf に replication 行 (scram-sha-256) を追加。
- dependabot / renovate 設定なし (.github 配下は workflows のみ)。

### 2.2 網羅の穴

- **OS が ubuntu-latest のみ**。matrix の os 次元は実質単一。macOS/Windows は未検証
  (README もプラットフォーム制約は明記せず)。
- **PostgreSQL バージョンが 18 のみ**。README は Direct SSL に PG17+、sslnegotiation 等に
  言及するが、旧バージョン (PG13–17) 互換のマトリクスはない。
- **chronos / bearssl がバージョン無固定** (上記 1.3)。CI が README の下限を保証しない。
- **nph-action が `version: latest`** で未ピン → フォーマッタの挙動変化で CI が非再現に
  なるリスク。
- docs.yml / nph.yml は 2026-03-08 以降更新なし (古いが機能はしている)。
- テストは両 async バックエンドを走る (nimble task test) — バックエンド網羅は十分。
  Nim バージョン網羅 (2.2.4/stable/devel) も十分。

---

## 3. リリース / semver 運用の状況

- タグ: **v0.1.0** (2026-03-27), **v0.2.0** (2026-04-27), **v0.3.0** (2026-05-28)。
  月次ペースで minor バンプ。バージョンバンプは専用 PR ("Bump to 0.x.0")。
- nimble の version は `0.3.0` で最新タグと一致。
- **HEAD は v0.3.0 から 555 コミット先行** (約 2 ヶ月、2026-05-28 → 2026-07-29)。
  大量の fix/refactor/test が未リリースで溜まっている。
- **CHANGELOG / HISTORY / NEWS ファイルは存在しない**。破壊的変更の追跡手段は
  git log / PR のみ。
- 0.x 系のため semver 上 minor での破壊的変更は許容されるが、それを示す運用
  (CHANGELOG やマイグレーション注記) は確認できない。
- 未追跡の `reviews/RELEASE_0.4.0_TODO.md` に 0.4.0 リリース前の要修正項目があり、
  次リリースは計画中 (リポジトリ追跡外)。

---

## 4. リポジトリの不要物・機密の所見

### 4.1 async_postgres.out (938KB バイナリ)

- ワークツリに存在 (938,816 bytes, 2026-07-23) するが、**git 未追跡**。
  `.gitignore` の `*.out` (line 34) で忽略済み。`git ls-files` / `git status` にも出ない。
  → リポジトリ汚染・履歴肥大のリスクはなし。ローカル残骸 (コンパイル出力) のみ。

### 4.2 `.,` ディレクトリ

- ワークツリ直下に存在するが**空**。git 未追跡 (空ディレクトリは git が追跡しない)。
  コマンドのタイプミス (例: `mkdir .,` やパス誤り) で生じた残骸と推定。実害なし・追跡外。

### 4.3 tests/certs/ の鍵ファイル

- `tests/certs/` は `.gitignore` (line 6) で忽略済み。追跡されているのは生成スクリプト
  `tests/gen_certs.sh` のみ。
- 鍵/証明書は CI・ローカルで gen_certs.sh が openssl により**その場で生成**する自己署名
  テスト用 (CN=Test CA / CN=localhost, SAN: localhost,127.0.0.1, wrong_ca も含む)。
  リポジトリに秘密鍵はコミットされていない。server.key は chmod 600 される。

### 4.4 ハードコード資格情報

- grep (password/secret/api_key/token/PRIVATE KEY) の命中は全て:
  - テスト用固定値: docker-compose の `test/test/test`、テストコードの "pencil", "mypass",
    "secret", "wrong_password" 等 (DSN/認証の単体・E2E フィクスチャ)。
  - PostgreSQL プロトコルのフィールド名 (`secretKey` = backend key data)。
- **実在の秘密情報・API キー・秘密鍵のハードコードは検出なし**。
- 未追跡の作業残骸: `reviews/`, `tests/review_rowdata.md`, `tests/test_ssl_coverage.md`,
  `.claude/`, `htmdocs/`, `.audit/` (いずれも git 追跡外。機密は含まず)。

---

## 未確認の領域

- 各依存パッケージの上流 changelog との実際の互換性 (オフライン調査のため未確認)。
- GitHub Actions の実際の実行結果/履歴 (リモート未アクセス)。
- examples/・tests/ 全ファイルの精査 (依存 import と資格情報の grep のみ実施)。
- `htmdocs/` 生成物の内容精査。
