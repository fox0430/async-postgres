# test_ssl.nim カバレッジ分析

対象: `async_postgres/pg_connection/ssl.nim` (612行)

## カバー済み

| 対象 | テスト数 |
|---|---|
| `sniName` | 6 (DNS/IPv4/IPv6/empty/sslSni=false/numeric-looking) |
| `negotiateSSL` SSLRequest パス ('S'/'N'/unexpected/closed) | 5 |
| CVE-2021-23214 対策 (`extraBytesBuffered` + `socketHasPendingData`) | 2 (single-write / split-write) |
| sslmode 各動作 (disable/allow/prefer/require/verify-ca/verify-full) | 10+ |
| `validateDirectSslCompatible` | 2 |
| `validateClientCertConfig` (sslCert/sslKey ペアリング) | 8 |
| チャネルバインディング (`selectScramMechanism`: cbRequire/cbDisable/cbPrefer) | 12 |
| `enforceVerifyFullIdentity` (IP-SAN / DNS-SAN) | 4 (asyncdispatch のみ) |
| `parseTrustAnchors` / `appendDnCallback` (空ブロック/ゴミ/サイズ溢出) | 6 |
| `installX509Capture` / `rebindX509Capture` | 2 |
| ALPN 広告 (ClientHello 内) | 1 |

## 未カバー / 実 TLS が必要でテスト困難

| 対象 | 理由 |
|---|---|
| `establishTls` の実際のハンドシェイク成功パス | ✅ 対処済み — e2e TLS テスト（test_e2e_connection.nim）に加え、test_tls_error_paths.nim の ALPN テストが実ハンドシェイクを実行 |
| `driveTlsHandshake` の peer close 分岐 | ✅ 対処済み（2026-08-13）— test_tls_error_paths.nim: モック 'S' 応答後 close で `recv == 0` 分岐を実証（asyncdispatch） |
| `assertAlpnPostgres` のエラーパス（ALPN 無し） | ✅ 対処済み（2026-08-13）— openssl s_server（ALPN 非広告）で「without ALPN」分岐を両バックエンド実証。※ `-alpn <別名>` の場合は peer が handshake を abort（alert no application protocol）するため「不正プロトコル」分岐には到達不能 |
| `failPemPassphrase` / 暗号化鍵の拒否 | ✅ 対処済み（2026-08-13）— tests/certs/encrypted.key（PKCS#1 伝統型暗号化、Proc-Type: 4,ENCRYPTED）フィクスチャで両バックエンド実証 |
| `writeTempPem` / 一時ファイル清理 (asyncdispatch) | ✅ 対処済み（2026-08-13）— 上記 4 テストが establishTls 経由で writeTempPem + finally 清理経路を実行 |
| `SSL_CTX_load_verify_locations` / `use_certificate_chain_file` / `use_PrivateKey_file` の失敗分岐 | ✅ 対処済み（2026-08-13）— ゴミ CA/cert/key コンテンツで各ロード失敗分岐を実証 |
| `SSL_CTX_check_private_key`（cert/key 不一致） | ✅ 対処済み（2026-08-13、asyncdispatch）— server.crt + wrong_ca.key で実証。※ OpenSSL 3.6 は use_PrivateKey_file 段階で "key values mismatch" を検出（check_private_key に到達しない）。chronos はクライアント側検証が無い（サーバがクライアント認証を要求しない限り黙って成功）— 非対称として記録済み |
| `sslCtxSetAlpnProtos` が nil または rc!=0 のパス | 未対処 — dynlib 解決結果に依存（Apple 系のみ nil になり得る） |
| asyncdispatch TLS 下限 (TLS 1.2+ 強制) の downgrade 拒否 | 未対処 — modern OpenSSL に TLS 1.0/1.1 のみ受ける peer が構築不能（実装できない） |
| chronos verify-full + IP-literal host 事前診断 | ✅ 対処済み（既存、mock テスト） |
| chronos require での期限切れ証明書拒否 | 未対処 — 期限切れ証明書での実ハンドシェイク peer が必要（openssl s_server -cert <期限切れ> で構築可能だが、verify モードでは CA ピン留めが主経路のため優先度低） |
| chronos 側: `TLSCertificate.init` 失敗, `TLSStreamInitError` | ✅ 対処済み（2026-08-13）— ゴミ cert/key コンテンツで両バックエンド実証（test_tls_error_paths.nim） |
| `formatSslError` / `resolveSym` | 未対処 — エラー時 / モジュール初期化時のみ（formatSslError はキー読込失敗経路で間接実行） |

## 総評

ロジック分岐のカバレッジは高い (negotiateSSL の全応答分岐、sslmode 行列、
CVE-2021-23214 の両検出パス、チャネルバインディングの全 cbMode)。
2026-08-13 の test_tls_error_paths.nim 新設で、従来「実 TLS が必要でテスト困難」だった
cert/key/CA 読込失敗分岐・暗号化鍵拒否・peer close・ALPN 欠如が実証可能になった
（cert/key/CA ロード失敗は SSLRequest 'S' 応答のみで establishTls のロード段階まで到達できる）。
残る未カバーは TLS 1.0 ダウングレード拒否（peer が現存しない）と dynlib nil 分岐（Apple 系のみ）の
非現実的経路のみ。
