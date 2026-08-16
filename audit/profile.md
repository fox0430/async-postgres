# 対象プロファイル

## 種別と根拠
- **種別: ライブラリ / SDK**
  - 根拠: `async_postgres.nimble` (version 0.3.0, license MIT) でパッケージ定義。`nimble install async_postgres` で配布 (README.md:78)。`examples/` に14の実行サンプル。`async_postgres.nim` が公開エントリで多数のモジュールを `export`。
  - アプリケーション/CLI のエントリポイント（サーバ起動・bin）は無い。

## 主要言語・バージョン・ランタイム
- 言語: Nim (requires `nim >= 2.2.4`)
- 非同期バックエンド: asyncdispatch (既定) または chronos (`-d:asyncBackend=chronos`)。`async_backend.nim` が抽象化層。
- SSL: asyncdispatch=OpenSSL (`-d:ssl`)、chronos=BearSSL (TLS 1.2 のみ)
- CI: GitHub Actions、Nim 2.2.4 / stable / devel のマトリクス、docker-compose の実 PostgreSQL で e2e。

## 公開パッケージ名・バージョン
- `async_postgres` v0.3.0 (nimble)。次の 0.4.0 リリースに向けた TODO が `reviews/RELEASE_0.4.0_TODO.md`。

## 規模
- ソース (async_postgres/ 配下 + async_postgres.nim): 41ファイル、約 26,516 行
- テスト (tests/): 40+ ファイル
- 総コミット 556、コントリビュータ 3、`fix` コミット 138 件（直近にセキュリティ強化・malformed input 拒否が集中）

## 想定される利用者
- 外部の Nim 開発者（nimble 公開）。PostgreSQL に接続するアプリケーション/サービス作者。
- 利用者はライブラリの型・エラー契約・並行性保証に依存する → 付録 C（ライブラリ固有観点）を適用する。

## 信頼境界の一覧（外部入力がシステムに入る全経路）
1. **PostgreSQL サーバからのワイヤプロトコル入力**（最主軸）
   - `pg_protocol.nim` (メッセージ解析: DataRow/ErrorResponse/Authentication*/ParameterStatus/NotificationResponse/CopyData/LogicalReplication messages)
   - `pg_connection/buffer_io.nim` (recv バッファリング、`nextMessage`/`recvMessage`)
   - 脅威モデル: 悪意/Rogue サーバ、MITM。サーバ送信データは信頼できない。
2. **型デコード（サーバ由来データのバイナリ/テキスト解析）**
   - `pg_types/decoding.nim`, `pg_types/ranges.nim`, `pg_types/array.nim`, `pg_types/core.nim`
   - 過去の fix の大半がここ（malformed input 拒否、int32 長ラップガード）。
3. **DSN / 接続文字列**（利用者入力）
   - `pg_connection/dsn.nim` (URI + keyword=value)。`sslcert`/`sslkey` はディスクからファイル読込 → パス操作・権限チェックが絡む。
4. **認証ハンドシェイク**
   - `pg_auth.nim` (MD5, SCRAM-SHA-256/-PLUS), `pg_saslprep.nim`, channel binding (tls-server-end-point)
5. **SSL/TLS ネゴシエーション**
   - `pg_connection/ssl.nim`, `pg_bearssl.nim` (証明書検証、ALPN)
6. **SQL プレースホルダ展開**（コンパイル時マクロ）
   - `pg_sql.nim` (`sql""` マクロ、`?` プレースホルダ) — 注入耐性が契約。
7. **COPY / Large Object / Replication ストリーム**
   - `pg_client/copy.nim`, `pg_largeobject.nim`, `pg_replication.nim` (pgoutput デコーダ、WAL)

## 既存監査/レビュー状態
- `reviews/` に14ファイルの個別レビュー + `RELEASE_0.4.0_TODO.md`（0.4.0 向け要修正トラッカー）。
- 既存レビュー対象: buffer_io, advisory_lock, copy, pool, protocol, replication, decoding, dsn, encoding, largeobject, pg_sql, ranges, transaction。
- 本監査はこれらを**文脈として参照**するが、既存問題こそ調査対象のため再調査する。重複排除は横断分析で行う。
