# Audit Findings: async_postgres/pg_client/ — クエリ実行 公開API表面 (Tier 2)

対象: `async_postgres/pg_client/` の core.nim (622), query.nim (459), exec.nim (172),
prepared.nim (192), cursor.nim (322), direct.nim (646)。queryRecvLoop / extractParams /
flattenInline / toFormatCodes / buildResultFormats / queryDirect・execDirect ゼロアロマクロ。
受信チェーン（buffer_io → pg_protocol → decoding）は呼び出し側として利用。

方法: 6ファイルを通読し、依存ヘルパ（simple_query.nim の awaitOrInvalidate / checkReady、
buffer_io.nim の pumpUntilReady / nextMessage、cache.nim の addStmtCache、pg_protocol.nim の
addBind/addBindRaw/newRowData/buildResultFormats/BinarySafeOids、encoding.nim の
addBindDirect/addParseDirect/writeParam*/paramOidOf/coerceBinaryParam、accessors.nim の
getStr/isBinaryCol/columnIndex）を実装まで追跡。Tier1 横断パターン (a)〜(d) の再発を確認。

総括: 未対応の報告対象 **1件**（Medium x1）。
- F1 (Medium・確定): rfAuto のワイヤ形式が呼び出し側から見えないキャッシュ状態に依存し
  （初回テキスト / 命中バイナリ）、API 間でも不一致（query・queryDirect・pipeline=命中でバイナリ、
  execute・cursor=常にテキスト）。`QueryResult.fields[i].formatCode` / `row.isBinaryCol(i)` が
  実行ごとに揺れる。

---

- 分類: 公開境界（API契約の不一致・非決定性）/ テキスト/バイナリ格式非対称 (a)
- 重大度: Medium
- 確信度: 確定
- 場所:
  - async_postgres/pg_client/core.nim:369-370 vs 377/388（キャッシュ状態依存）
  - 対照 API: async_postgres/pg_client/prepared.nim:107-119（execute、buildResultFormats 不使用=常にテキスト）/ cursor.nim:70-89（openCursor、同=常にテキスト）
- 事象:
  rfAuto の実際のワイヤ形式が、(1) 同じ SQL の何回目の実行か（キャッシュ状態）、(2) どの API か、
  で変わり、呼び出し側から制御・予測できない。
  - 同じ `query(sql, rfAuto)` でも、初回（ミス）はテキスト、2回目以降（命中）は binary-safe 列がバイナリ
    （core.nim:369-370 が空 resultFormats を cached.resultFormats で上書き）。結果 `QueryResult.fields[i].formatCode`
    と `row.isBinaryCol(i)` が実行ごとに 0↔1 で揺れる（値は型付きアクセサでは正しいが、メタデータは不安定）。
  - API 間で意味が不一致: `query` / `queryDirect` / pipeline はキャッシュ命中でバイナリ化する一方、
    `execute`（prepared.nim:107-119）と `openCursor`（cursor.nim:70-89）は buildResultFormats を使わず
    rfAuto を常に空形式（テキスト）として送る。同じ `resultFormat = rfAuto` が API によりテキストにも
    バイナリにもなる。
  値が正しい型付きアクセスにおいても成立する契約・一貫性の違反
  （formatCode/isBinaryCol の非決定性、API 間非対称）。
- 根拠: コード引用と推論
  ```nim
  # prepared.nim:107-118 -- execute は rfAuto(@[]) をそのまま送信。buildResultFormats 呼び出しなし
  var qr = QueryResult(fields: stmt.fields)
  if resultFormats.len > 0:            # rfAuto では偽 → formatCode 更新なし（テキストのまま）
    let colFmts = deriveColFmts(resultFormats, qr.fields.len)
    ...
  ```
  ```nim
  # cursor.nim:76-86 -- openCursor も resultFormats.len > 0 のみ派生。rfAuto(@[]) では colFormats=nil（テキスト）
  if resultFormats.len > 0:
    cursor.colFormats = newSeq[int16](cursor.fields.len)
    ...
  ```
  対照 core.nim:369-370（query 系キャッシュ命中）は `cached.resultFormats`（= buildResultFormats、バイナリ含む）
  を再生。grep `buildResultFormats` の呼び出しは cache.nim:67 の1箇所のみで、prepared/cursor 経路には存在しない
  → 両 API は rfAuto でもバイナリ化しないことが構造的に確定。pipeline.nim:262-268 コメントは query/pipeline
  経路のバイナリ再生を「意図」として明記。
- 系統性: 同種パターン（rfAuto 意味の分岐点 2系統）
  - 「rfAuto をキャッシュ命中でバイナリ化」する経路: sendExtendedQuery（core.nim:369-370、query/exec/queryEach/
    queryInline が共有）+ queryDirect（direct.nim:382 `` `effectiveRfSym` = `cachedSym`.resultFormats ``）+ pipeline
    （pipeline.nim:269-273）。`cached(.resultFormats)` の実コード出現 = core.nim:370 / direct.nim:382 /
    pipeline.nim:271 の3箇所（direct.nim:382 はバッククォート付きのため単純 grep では漏れる、目視確認）。
  - 「rfAuto を常にテキスト」とする経路: prepared.nim executeImpl（:107-118）、cursor.nim openCursorImpl（:76-89）。

---

## 調査したが所見としなかった項目（透明性のため記録）

- **direct マクロの `int16()` 変換が addCount16 の防護を迂回**（encoding.nim:1887 `newLit(int16(args.len))`、
  :1912 `newLit(int16(args.len))`、:1937 `buf.addInt16(int16(resultFormats.len))`）。ライブラリは
  addCount16（pg_protocol.nim:340-357）で「生の `int16(n)` は既定ビルドで捕捉不能 RangeDefect、
  `-d:danger` では黙ってラップしストリーム desync」と明記し常に ValueError を投げる防護を用意するが、
  direct マクロはこれを使わない。ただし args.len はコンパイル時定数（マクロ引数、極小）、
  resultFormats.len はキャッシュ命中時の cached.resultFormats（= fields.len、RowDescription の
  フィールド数はワイヤ int16 で ≤32767）に由来し、いずれも 32767 を超え得ないため実際には溢出しない。
  規約違反・潜在リスクの単発 Low のため所見から除外。grep `addInt16(int16(` = encoding.nim:1937 の1箇所。
- **flattenInline / appendInlineParam の `int32(data.len)` 溢出**（core.nim:292）。inline パラメータの
  累積 payload が 2GiB を超えると `int32(data.len)` が捕捉不能 OverflowDefect。受信側は parseDataRowInto
  （pg_protocol.nim:1122-1125）が 2GiB を PgProtocolError で防護、addBindRaw（pg_protocol.nim:650）も
  int64 算術で境界検査するが、送信側 appendInlineParam には防護なし。ただし1呼び出しで 2GiB 超の
  inline パラメータは非現実的。単発 Low のため除外。
- **PgParamInline の全フィールド公開による自己起因の過読み**（pg_types/core.nim:143-149、oid*/format*/
  len*/inlineBuf*/overflow* 全て `*`）。`len` を実データより大きく偽造すると appendInlineParam
  （core.nim:295-298）の `toOpenArray(0, int(p.len)-1)` が域外読み取り（IndexDefect）。ただし doc は
  「Use toPgParamInline to construct」と指示し、サーバ信頼境界ではなく呼び出し側の自己破壊。単発 Low のため除外。
- **cursor の自前 drain ループの asyncdispatch 孤立継続**（cursor.nim:110,124,128,174,187,196 の
  `await conn.fillRecvBuf()`、timeout なし）。外部 `wait(timeout)` 発火時の孤立継続リスクは buffer_io.md
  所見1 がシステム横断パターンとして既に列挙（cursor.nim の6箇所を含む）しており、新規の誤Handlingではない。
  cursor は closeCursorImpl（cursor.nim:241-243）が csClosed を明示的に retire 扱いするなど、むしろ
  平均より慎重。新規所見とせず。
- **cacheHitColFmts の cachedColFmts フォールバック長不一致**（core.nim:91-107 → queryRecvLoop:457-458 の
  `colFmts[i]`）。resultFormats 空かつ numCols>0 で cachedColFmts を返すが、cachedColFmts は addStmtCache
  （cache.nim:68-72）で常に fields.len と等しく設定され、qr.fields = cached.fields（同長）のため域内。
  fields.len==0 は queryRecvLoop:451 の `if qr.fields.len > 0` で除外。IndexDefect には至らず所見なし。
- **queryEach の rowCount 二重計上**疑い（core.nim:536-538 の onRow 内 `rowCount += 1`）。pumpUntilReady
  配信オーバーロードは nextMessage に rowCount=nil を渡す（buffer_io.nim:433）ため nextMessage 側は加算せず、
  onRow のみが加算。二重計上なし（core.nim:534-535 コメントの通り）。所見なし。
