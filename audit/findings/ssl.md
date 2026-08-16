# ssl.nim / pg_bearssl.nim 監査所見

対象: `async_postgres/pg_connection/ssl.nim` (612行) 、`async_postgres/pg_bearssl.nim` (209行)
バックグラウンド: デフォルトバックエンドは asyncdispatch=OpenSSL、`-d:asyncBackend=chronos` で BearSSL。
依存の実機確認: chronos-4.4.0 (`tlsstream.nim`)、bearssl-0.2.12 (`csources/`)、Nim 2.2.10 stdlib (`lib/pure/net.nim`, `lib/std/tempfiles.nim`)。

核心の MITM 防御（CA ピン留め、hostname 検証、pre-TLS インジェクション検知、ALPN 強制、require 以上の平文 fallback 拒否）は
両バックエンドで fail-closed に実装済み。バックエンド非対称の所見はすべて対処済み（対応は git log を参照）。

---

## 補足（所見として起票しない確認事項）

以下は調査したが所見に該当しなかった、または既存文档と重複するため起票しない。

- **CA ピン留めは両バックエンドで正しい**: verify-ca/full で `sslRootCert` 空は `negotiateSSL:534-540` が I/O 前に fail-closed。
  asyncdispatch は `newContext(CVerifyNone)` で OS バンドルを読ませず（stdlib net.nim:711 の `verifyMode != CVerifyNone` ガード）、
  `SSL_CTX_load_verify_locations`(414) でピン留め CA のみ信頼。chronos は `parseTrustAnchors` の自前錨を渡す（303-318行）。
  Web PKI フォールバックによる MITM 経路は無い。
- **verify-full の hostname 検証は fail-closed**: asyncdispatch は動的シンボル（`SSL_set1_host` 等）未解決時に例外を投げて
  連鎖検証へ降格しない（`enforceVerifyFullIdentity:180-213`）。chronos は BearSSL の名前照合に委ねる。
- **pre-TLS インジェクション検知（CVE-2021-23214 系）は堅牢**: chronos の2バイト読み（569-574行）と asyncdispatch の
  `socketHasPendingData`（594行、buffer_io.nim:684-707 の MSG_PEEK）の二重検知。テスト2件（test_ssl.nim:356,401）で単一/分割書き込みを被覆。
- **一時 PEM（writeTempPem）は安全**: `createTempFile` は `mode = S_IRUSR or S_IWUSR`(0600) + `O_EXCL`（stdlib tempfiles.nim:88-89）。
  予測可能なファイル名でも O_EXCL+0600 で symlink 攻撃不可。`newContext` 読込直後に削除（ssl.nim:454-456）、失敗経路も finally で回収（512-513）。
- **ALPN 強制は direct モードで fail-closed**: `assertAlpnPostgres`(227-241) は空選択/不一致プロトコルを拒否。peer 制御値は
  `escape` で NUL 切り詰めを無害化（240行）。chronos は `getSelectedAlpnProtocol`(342)、asyncdispatch は `getSelectedAlpnOpenssl`(483)。
- **暗号化クライアント鍵のイベントループ凍結対策**: `failPemPassphrase`(89-95) を `SSL_CTX_set_default_passwd_cb` で登録、
  シンボル非在時は `"ENCRYPTED" in config.sslKey` ヒューリスティック（421-431行）で TTY プロンプトを回避。
- **struct コピー由来の dangling pointer（reconnectInPlace）は対策済み**: `rebindX509Capture`(notify.nim:98-101) が `certDer` と
  エンジンの x509 スロットを再バインド。`trustAnchorBufs`/`tlsStream` は ref/seq で共有され生存する。テスト被覆あり（test_ssl.nim:1609-1646）。
- **テストカバレッジの穴は既存の自己評価文档と重複**: `tests/test_ssl_coverage.md` が `driveTlsHandshake` エラー分岐・
  `assertAlpnPostgres` エラーパス・暗号化鍵拒否・実 TLS ハンドシェイク成功パスの未カバーを正確に列挙済み（RELEASE_TODO T6 相当）。
  これらはモックサーバでは到達不能な fail-closed セキュリティゲートであり、実 TLS 統合テストまたはスタブ化が必要という同文档の総評に同意。
  本監査からは新規所見として起票しない。
