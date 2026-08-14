# auth.md — pg_auth.nim / pg_saslprep.nim 監査所見

## 所見 1

- 分類: セキュリティ / 正当性（SASLprep 正規化の RFC 3454 テーブル誤り）
- 重大度: Medium
- 確信度: 確定
- 場所: async_postgres/pg_saslprep.nim:37, async_postgres/pg_saslprep.nim:26, async_postgres/pg_saslprep.nim:115-118
- 事象:
  RFC 3454 のテーブル帰属が誤っており、3 つのコードポイントの正規化結果が PostgreSQL の pg_saslprep と一致しない。

  (a) U+200B (ZERO WIDTH SPACE) — RFC 3454 B.1（map to nothing）にのみ属し、C.1.2（non-ASCII space）には属さない。しかし `isNonAsciiSpace`（37行）に 0x200B が含まれている。マッピング段階（115-118行）で `isNonAsciiSpace` が `isMapToNothing` より先に検査されるため、U+200B は削除されず U+0020 に変換される。

  (b) U+200C (ZERO WIDTH NON-JOINER) / U+200D (ZERO WIDTH JOINER) — RFC 3454 C.2.2（non-ASCII control, prohibited）にのみ属し、B.1 には属さない。しかし `isMapToNothing`（26行）に含まれている。マッピングが禁止検査より先に走るため、本来 prohibited として raw fallback になるべき文字が黙って削除される。

- 根拠:

  RFC 3454 Appendix B.1 の該当箇所:
  > 00AD; SOFT HYPHEN / 034F; COMBINING GRAPHEME JOINER / 1806 / 180B / 180C / 180D / **200B; ZERO WIDTH SPACE** / 2060; WORD JOINER / FE00..FE0F / FEFF

  RFC 3454 Appendix C.1.2 の該当箇所（Zs カテゴリ、U+0020 以外）:
  > 00A0 / 1680 / 2000..200A / 202F / 205F / 3000
  （U+200B は Zs ではなく Cf カテゴリであり C.1.2 に含まれない）

  RFC 3454 Appendix C.2.2 の該当箇所:
  > ... / **200C; ZERO WIDTH NON-JOINER** / **200D; ZERO WIDTH JOINER** / ...
  （U+200C, U+200D は B.1 に含まれない）

  コード（pg_saslprep.nim:115-118）:
  ```nim
  if isNonAsciiSpace(cp):      # 0x200B がここで space に変換される（誤り）
    mapped.add(char(0x20))
  elif isMapToNothing(cp):     # 0x200C, 0x200D がここで削除される（誤り）
    discard
  ```

  影響:
  - U+200B 含有パスワード: クライアントは "a b"（space 挿入）で SCRAM 証明を計算、PostgreSQL は "ab"（削除）で検証 → 認証失敗。
  - U+200C/U+200D 含有パスワード: クライアントは削除して正規化を続行、PostgreSQL は prohibited として raw fallback → 正規化パスが分岐し認証失敗。
  - テスト（test_saslprep.nim:25-34）が誤ったテーブル帰属を前提に記述されており、バグを固定化している。テストコメント "0x200C is in B.1" および "0x200B is in both C.1.2 and B.1" は RFC 3454 と矛盾する。

- 系統性: 同種パターン（3 code points / 2 lookup functions）
  grep パターン: `rg -n "0x200B|0x200C|0x200D" async_postgres/pg_saslprep.nim` → 3 箇所（26行, 37行, 52行）。
  52行の `isProhibited` 内 0x200C/0x200D は正しい（C.2.2）が、26行の `isMapToNothing` が先にマッチするため死にコードとなっている。

---

## 所見 2

- 分類: セキュリティ（SCRAM server-first-message の mandatory extension 未検査）
- 重大度: Low
- 確信度: 確定
- 場所: async_postgres/pg_auth.nim:126-138
- 事象:
  `scramClientFinalMessage` の server-first-message パーサは `r=`, `s=`, `i=` のみを認識し、RFC 5802 §5.1 が MUST で要求する `m=`（mandatory extension）属性の検出・中断を行わない。`m=` を含むメッセージを受信した場合、属性は黙って無視され SCRAM 交換が継続する。
- 根拠:

  RFC 5802 Section 5.1:
  > "If the client receives a server-first-message containing an 'm' attribute, it MUST abort the authentication."

  コード（pg_auth.nim:126-138）:
  ```nim
  for part in serverFirstMsg.split(','):
    if part.startsWith("r="):
      ...
    elif part.startsWith("s="):
      ...
    elif part.startsWith("i="):
      ...
  ```
  `m=` に対する分岐が存在しない。未知属性はすべて無視される。

  現実の PostgreSQL サーバは `m=` を送信しないため実害は限定的。server signature 検証（scramVerifyServerFinal）が別途存在するため認証バイパスには至らない。RFC 適合性の問題。

- 系統性: 単発（`m=` 検査の欠落は1箇所のみ。`rg -n '"m="' async_postgres/` → 0 件）
