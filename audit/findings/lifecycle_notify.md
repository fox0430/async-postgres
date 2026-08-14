# lifecycle / simple_query / notify 監査所見（Tier 2）

対象: `async_postgres/pg_connection/lifecycle.nim` (680), `simple_query.nim` (447), `notify.nim` (429)
観点: 並行性・状態、セキュリティ、境界条件、エラーパス。
前提: asyncdispatch の `cancelAndWait` は no-op（`async_backend.nim:193-204`、doc 自身が "the future is neither cancelled nor awaited" と明記）。chronos は実際にキャンセルする。

## 修正ステータス

本ファイルの所見（`abortListenTask` のゾンビ pump）は修正済みのため削除した。PR #575（`f96a278`）が
`close()` / `stopListening` の graft・ハング経路を、後続の修正が `abortListenTask` の asyncdispatch 分岐
（closeTransport → bounded await pump、pump 生存時の `listenStopRequested` 保持）をそれぞれ解消している。
レビュー `reviews/review_listen_close_reconnect.md` の Issue 1–6 も同 PR で対応済み。回帰テストは
`tests/test_listen_reconnect.nim`（両 backend グリーン）。

---

## 横断結果（lifecycle / simple_query の陰性確認）

対象3ファイルに未ガード cancelAndWait / orphan タスク復活は**無し**。検証済み項目:

- **`lifecycle.nim:463` の cancelAndWait は適切にガード済み**。`when hasAsyncDispatch:`（:450）の `else` 分岐（chronos 専用）にあり、asyncdispatch は :450-461 の手動停止（`listenStopRequested` → `closeTransport` → `await pump`）を使う。`test_e2e_listen.nim:296-330` が復活非再発を検証。所見なし。
- **`simple_query.nim` には cancelAndWait が無い**。`asyncSpawn` は1件（`cancelNoWait` :209、`doCancel` :203-207 が `except CatchableError: discard` で全エラーを飲み、asyncSpawn の `raiseAssert` Defect（async_backend.nim:213-214）に到達しない）。`lifecycle.nim:540` の orphan クローズ spawn も同様に CatchableError を swallow（:546-547）。対象3ファイル内で捕捉不能 Defect を漏らす spawn は無し。
- **`quoteIdentifier`（simple_query.nim:96-98）は完全**: `"\"" & s.replace("\"", "\"\"") & "\""` は埋め込み二重引用符を二重化して全体を二重引用符で囲む。libpq `PQescapeIdentifier` と同一規則。識別子クォートとしてのエスケープは完全（NUL はサーバ側で拒否される識別子制約）。simple_query は信頼入力前提を doc（:276-277,:307-308）で明記。所見なし。
- **SCRAM / require_auth（lifecycle.nim:306-415）は防御的**: SASL 開始後の `AuthenticationOk` を `scramFinalVerified` 必須で拒否（:314-319、MITM の SASLFinal スキップ防止）、`SASLContinue`/`SASLFinal` の先行 `AuthenticationSASL` 必須チェック（:373-383,:395-403、nonce バインディングの vacuous pass 防止）、`enforceAuthAllowed` の allowlist（:33-44）と `filterSaslByRequireAuth`（:46-58）＋防御的再チェック（:366）、`trust` 認証の allowlist 照合（:312-313）。SCRAM iteration は `pg_auth.nim:148-159` が `< 4096` と `> maxIterations`（既定 10_000_000、pg_auth.nim:17）を拒否し PBKDF2 DoS を上限化（libpq より厳格）。`selectScramMechanism`（:60-124）の channel binding モード分岐と `cbSupportedButUnused`（:112）の "y,," ダウングレード検出は libpq 整合。所見なし。
- **failover / session attrs（lifecycle.nim:584-676, simple_query.nim:426-447）**: `matchesOrClose`（:481-503）は probe 失敗・非一致・キャンセルの全パスで先に `conn.close()` してから raise/return（接続リーク無し、`ReraiseDefect` 回避のため捕捉例外 `e` を再 raise :498-501）。`probeBool`（simple_query.nim:378-389）は不定結果（0行/NULL）を raise で失敗させ、ホストを黙って一致扱いしない（libpq の次ホスト前進と整合）。全ホスト失敗は `PgConnectionError` に集約（lifecycle.nim:647-649）、単一ホストの `AsyncTimeoutError` は生で再 raise（:645-646、プール側の型分岐契約維持）。`orderedHosts`（:554-582）の lbhRandom は `std/sysrand.urandom` シードで `--threads:on` 安全。所見なし。
