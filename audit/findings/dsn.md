# Audit Findings: async_postgres/pg_connection/dsn.nim

対象: `async_postgres/pg_connection/dsn.nim` (719行) — DSN 解析（URI + libpq keyword=value）、`initConnConfig`、
`parseDsn`、sslcert/sslkey/sslrootcert のファイル読込（`readPemFileParam`:214）と sslkey 権限ビット検査。
信頼境界#3（利用者入力）。ファイルシステム読込を伴う唯一の接続設定経路。

焦点: 悪意ある DSN による (a) プロセスクラッシュ、(b) パース層の検証空隙、(c) ファイル読込/権限検査の
TOCTOU・symlink 回避、(d) percent-encoding / host 注入、(e) URI と keyword=value の解釈不一致。

総括（系統カウント）:
- **Low 1件**（確定・実証、系統）: パース層が NUL バイト / 空キーの検証を encode 層まで遅延しており、
  URI と keyword=value で非対称（所見1）。注入自体は encode 層で fail-closed（exploit 不可）。
- 既存レビュー `reviews/review_dsn.md` の「バグなし」判定は、ファイル読込・percent-decode・host 注入の
  各核心については本調査でも追認する（下記「追認済み」）。
- 対処済み（削除）: `connect_timeout` の `seconds()` 変換で int64 overflow が `OverflowDefect` として
  漏出していた（元・所見1、Medium）→ `const maxTimeoutSecs = high(int64) div 1_000_000_000` による
  上限検査で catchable な `PgError` に変換済み。
- RELEASE_TODO T3「空 PEM ファイル拒否ガード (242-243) 未テスト」は、本調査で **ガードの動作を実証確認**
  （`parseDsn("...?sslrootcert=<empty file>")` → `PgError: sslrootcert file is empty`）。コード欠陥ではなく
  テスト欠落のみ。単発Low（テスト空白）のため所見としては起票しない。

追認済み（所見にしない）:
- `openRegularFile`(186-212): `open(O_NONBLOCK)` → `fstat(fd)` → `S_ISREG` → `O_NONBLOCK` 解除 → `open(File, fd)`
  の順。型検査・権限検査・読込が全て同一 fd（同一 inode）上で完結し、TOCTOU 窓は閉じている。symlink は
  `open` が原子解決し `fstat` は解決済みターゲットの mode を返すため、権限検査(225)はターゲットに対して
  機能する（回避なし）。`S_IRWXG or S_IRWXO`(225) は group/other の全 rwx ビットを拒否（setuid/setgid は
  アクセス権を付与しないため対象外で正しい）。
- `pctDecode`(506-523): 境界チェック `i + 2 >= s.len`(514) は `s="%41"`(len3) を受理・`s="%4"`(len2)/`s="%"`(len1)
  を拒否する正しい式。ゼロバイト拒否(517-518)・`+` の非変換いずれも libpq 互換（実証: テスト 134-142, 127-132）。
- `parseUriDsn` の `rfind('@')`(555) はパスワード内 `@` を保護、IPv6 は括弧必須で無括弧 `::1` を拒否(613-616)、
  authority のカンマ分割は percent-decode 前・query は decode 後(583-587, 623-638) — いずれも実証済（テスト
  104-120, 582-590）。`?` の query 分割は生の先頭 `?` のみ（`%3F` は database 名に残り query 注入不可）。

---

- 分類: セキュリティ / 正当性（パース層の検証空隙・DSN 形式間の非対称）
- 重大度: Low
- 確信度: 確定
- 場所: async_postgres/pg_connection/dsn.nim:429-432,449-478（keyword=value の key/value 読込、NUL 無検査）／623-629（URI query、空キー無検査）／対照 517-518（URI pctDecode の NUL 拒否）・433-434（keyword=value の空キー拒否）／防衛 async_postgres/pg_protocol.nim:439-441,494-497
- 事象:
  パース層が、ワイヤで意味を持つ NUL バイトと空キーの検証を、形式によって片側しか行っていない。
  (a) **NUL**: URI 形式は `pctDecode`(517-518) がパース時にゼロバイトを `PgError` で拒否するが、
      keyword=value 形式は key(430-432)・quoted value(453-464)・unquoted value(470-478) のいずれも NUL を
      停止文字集合に含まず、そのまま受理する。`parseDsn("host=h evilkey\x00inject=x")` は成功し
      `extraParams=@[("evilkey\x00inject", "x")]` となる（実証）。
  (b) **空キー**: keyword=value 形式は `=value` を「Empty key」でパース時に拒否(433-434)するが、URI の query
      経路(625-629)は `pair.find('=')` が 0 を返す `=value` をスキップせず、`pctDecode("")=""` の空キーを
      そのまま `applyParam` → `extraParams.add`(401) へ通す。`parseDsn("postgresql://host/db?=value")` は成功し
      `extraParams=@[("", "value")]` となる（実証）。
  いずれも StartupMessage へは **到達しない**: `encodeStartup` が空キーを `ValueError` で拒否
  （pg_protocol.nim:494-497）、`addCString` が埋め込み NUL を `ValueError` で拒否（439-441）。実証:
  `encodeStartup("u","d",@[("evil\x00key","v")])` → `ValueError: addCString: embedded NUL byte`、
  `encodeStartup("u","d",@[("","v")])` → `ValueError: encodeStartup: empty key`。
  従って startup-parameter 注入としては **exploit 不可（fail-closed）**。残る影響は (1) `parseDsn` が成功して
  不正な `ConnConfig` を返し、拒否が `connect()` 時まで遅延する、(2) その拒否が `PgError` ではなく `ValueError`
  で、DSN 形式によってエラー型・タイミングが非対称、の2点。user/password/database/applicationName/
  extraParams の全文字列フィールドが `addCString` を経由するため（lifecycle.nim:289 → encodeStartup:485-499、
  password は encodePassword:531 → addCString）、NUL がワイヤに抜ける経路は無い。
- 根拠: コード引用と推論
  ```nim
  # keyword=value: key は NUL を停止文字に含めず読み、空キーのみ拒否
  while i < dsn.len and dsn[i] notin {'=', ' ', '\t', '\n', '\r'}:   # 430: \0 は停止集合に無い
    key.add dsn[i]; inc i
  if key.len == 0:                                                    # 433: 空キーはここで拒否
    raise newException(PgError, "Empty key in connection string")
  ```
  ```nim
  # URI query: 空キー・NUL（decode 後）の検査が無い
  for pair in queryStr.split('&'):
    let epos = pair.find('=')
    if epos < 0:                       # 626: '=' 無しのみスキップ、"=value"(epos=0) は通す
      continue
    let key = pctDecode(pair[0 ..< epos])   # 628: "" や NUL 入りになり得る（NUL は pctDecode が拒否）
    let val = pctDecode(pair[epos + 1 .. ^1])
    ...
    else:
      result.applyParam(key, val)      # 638 → extraParams.add(401)
  ```
  非対称の整理: NUL は「URI=パース時拒否 / keyword=value=encode 時拒否」、空キーは「keyword=value=パース時拒否 /
  URI=encode 時拒否」。両形式とも最終的に encode 層が守るため注入にはならないが、パース層のガードが
  互いに逆の穴を持つ。`pctDecode` の NUL 拒否(517-518) と `encodeStartup` の空キー拒否(494-497) という
  「片側の形式にだけ存在するパース時ガード」が、もう片側の形式には対応するガードを欠く構造。
- 系統性: 同種パターン（パース層で取りこぼし encode 層で捕捉、の非対称）。
  NUL 無検査の読込箇所 **3箇所**（key 430-432 / quoted value 453-464 / unquoted value 470-478、全て
  `parseKeyValueDsn` 内、grep `notin {'=', ' ', '\\t', '\\n', '\\r'}` = 430,470 の2行＋quoted ループ 453）。
  空キー無検査 **1箇所**（URI query 625-629）。対照ガード: pctDecode NUL 拒否 **1箇所**（517-518）、
  keyword=value 空キー拒否 **1箇所**（433-434）。encode 層ガード: `addCString` NUL **1箇所**（pg_protocol.nim:441）、
  `encodeStartup` 空キー **1箇所**（496）。`extraParams.add` は applyParam 集約の **1箇所**（401）で、
  URI/keyword=value 両形式の未知キーが全てここに合流する。
