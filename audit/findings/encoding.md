# Audit Findings: async_postgres/pg_types/encoding.nim

対象: `async_postgres/pg_types/encoding.nim` (1940行、最大の型ファイル) — 利用者値 → PostgreSQL パラメータへの encode（テキスト/バイナリ）。`toPgParam` / `toPgBinaryParam` / `toPgParamInline`。
焦点: int32 長ラップガードの適用漏れを全 `toPgBinaryParam` で横断集計、テキスト/バイナリ格式・UTC・scale の一致、`newSeq[byte](size)` の巨大確保。

総括（系統カウント）:
- 内部 int32 長/カウントプレフィックスを `writeBE32` で書く箇所は **10箇所**（grep `writeBE32\((pos|[0-9]+), int32\(`）。
  加えて `data.writeBE32(0, v.nbits)`（882、int32 キャスト無し）が1箇所。**11箇所全てにガードあり**（詳細は所見1）。
  → 「int32 長ラップガードの適用漏れ」は encoder には残っていない（検証済）。
- 既存レビュー `reviews/review_encoding.md` の Bug 1（`writeParamFormat(seq[byte])` が format 0）は
  **現コードで修正済**（encoding.nim:1764-1765 は `1'i16`、`toPgParam(seq[byte])`:134 も format 1）。
  Bug 2（PgPath/PgPolygon ガード）も修正済（1086,1097 `checkPgBinPayload`）。よって再報告しない。

---

- 分類: 設計整合性 / セキュリティ（int32 長ラップガードの横断検証）
- 重大度: Low
- 確信度: 確定
- 場所: async_postgres/pg_types/encoding.nim（全 encoder）
- 事象:
  「int32 長ラップガードの適用漏れ」を全 `toPgBinaryParam` / array encoder で横断集計したところ、
  内部 int32 プレフィックスを書く **11箇所全てにガードが存在**し、適用漏れは **0件** であった。
  唯一の設計上の揺れは、payload ガードを helper（`checkPgBinPayload`）でなく等価の ad-hoc 式で
  書いている箇所が 2 ある点（機能差は無い）。
- 根拠: コード引用と推論
  内部 int32 長/カウントプレフィックスの全出現箇所（grep `writeBE32\((pos|[0-9]+), int32\(` ＋ nbits 直書き）と、
  それを守るガード:
  | 行 | 書き込み | ガード |
  |----|----------|--------|
  | 298 | `int32(dims.len)` ndim | `validatePgArrayShape`(284) → ndim ≤ PgArrayMaxDim=6（array.nim:60） |
  | 312 | `int32(ev.len)` 要素長 | `checkPgBinLen(ev.len)`(294) |
  | 376 | `int32(dms.len)` ndim | `validatePgArrayShape`（buildFixedArray:396 / Opt:426 経由） |
  | 406 | `int32(esz)` 固定幅 | esz はコンパイル時定数（≤32） |
  | 447 | `int32(esz)` 固定幅 | 同上 |
  | 882 | `v.nbits`（PgBit 直書き） | `nbits ≤ PgBitMaxBits = 1<<30 < int32.high`(871) |
  | 1089 | `int32(v.points.len)` path npts | `checkPgBinPayload(size)`(1086)、size=1+4+n*16 ⇒ n ≤ ~134M < int32.high |
  | 1099 | `int32(v.points.len)` polygon npts | `checkPgBinPayload(size)`(1097) |
  | 1174 | `int32(v.len)` hstore 対数 | `checkPgBinLen(v.len)`(1164) |
  | 1177 | `int32(k.len)` hstore key 長 | `checkPgBinLen(k.len)`(1167) |
  | 1184 | `int32(vs.len)` hstore value 長 | `checkPgBinLen(val.get.len)`(1170) |
  可変長配列は全て `encodeBinaryArray`（要素毎 `checkPgBinLen`:294 ＋ 累積 `checkPgBinPayload`:296）または
  `buildFixedArray`/`buildFixedArrayOpt`（payload ガード 398/435）を経由。scalar の可変長（string/bytea/jsonb 等）は
  内部長プレフィックスを持たず、外側長は Bind 層の `addLen32`（encoding.nim:1730 → pg_protocol.nim:365-382、
  `> maxInt32Len` で raise）が守る。`toPgBinaryParam` proc 定義は計 **38**（grep `proc toPgBinaryParam`）。
  設計の揺れ: payload ガード 4箇所のうち 2箇所（296 `checkPgBinPayload`、1086/1097/1172 同）は helper を使うが、
  `buildFixedArray`(398) と `buildFixedArrayOpt`(435) は等価の ad-hoc 式
  `if payload > int32.high.int64: raise newException(PgError, "Array payload too large ...")` を使う
  （grep `int32.high.int64` → 263(helper定義),398,435 の3行、うち encoder 本体は 398,435 の2箇所）。
  境界値は同一（`int32.high.int64`）で機能差は無く、メッセージ文言のみの差異。
- 系統性: 単発（適用漏れは 0件）。ad-hoc payload ガードの揺れは **2箇所**（398,435）。
  grep パターン `int32.high.int64` = 3行（定義1 + encoder 2）／`checkPgBinPayload` 呼び出し = 4箇所（296,1086,1097,1172）／
  `checkPgBinLen` 呼び出し = 4箇所（294,1164,1167,1170）。

---

- 分類: セキュリティ（`newSeq[byte](size)` の巨大確保 / ガードのタイミング）
- 重大度: Low
- 確信度: 高
- 場所: async_postgres/pg_types/encoding.nim:1006-1014（`encodeJsonbBinary`）／根 async_postgres/pg_types/core.nim:1081-1085（`toBytes`）
- 事象:
  非 inline のテキスト/jsonb encoder は、int32 長ガードより **前に** ペイロード全体を `newSeq[byte]` で確保する。
  ~2GiB を超える入力では、プロトコル層のガード（`addLen32`）が raise する前に、入力のコピーが
  一時確保される。inline 経路が示す「ガード→確保」の順序と逆。silent wrap はしない（下流ガードが捕捉）が、
  巨大入力で約2倍の一時メモリを消費してから `ValueError`。
- 根拠: コード引用と推論
  意図された順序（ガード→確保、inline）:
  ```nim
  proc toPgParamInline*(v: string): PgParamInline =     # 47
    if v.len > maxInt32Len:                              # 50: 確保前にガード
      raise newException(ValueError, ...)
    ...
    result.overflow = newSeq[byte](v.len)                # 61: ガード通過後のみ確保
  ```
  逆順（確保→ガード、非 inline）:
  ```nim
  proc encodeJsonbBinary*(node: JsonNode): seq[byte] =   # 1006
    let jsonBytes = toBytes($node)                       # 1010: コピー確保
    result = newSeq[byte](1 + jsonBytes.len)             # 1011: 長さガード無し
  ```
  `toBytes`（core.nim:1083 `result = newSeq[byte](s.len)`）もガード無しで、`toPgParam(string)`:111 /
  `toPgBinaryParam(string)`:617 / `$v` 系テキスト encoder 群（PgTime/PgNumeric/PgInterval/PgInet/幾何型 等）が共用する。
  これらの int32 長検証は下流の `addBind` → `buf.addLen32(data.len, "Bind parameter value")`（encoding.nim:1730）
  または配列要素時の `checkPgBinLen`（294）で初めて行われる。つまり ~2GiB 超の string/JsonNode を渡すと、
  encode 段階で同規模のコピーが確保され、Bind 段階で初めて拒否される。入力は既に呼び手が保持しているため
  最悪でも一時 ~2倍・境界値は `maxInt32Len = int(high(int32))`（pg_protocol.nim:291）。DoS 増幅は入力サイズに
  上限付けられ、wrap はしない。
- 系統性: 同種パターン。可変サイズをガード無しで確保し下流ガードに依存する箇所は **2系統**:
  `toBytes`（core.nim:1083、テキスト encoder 約15 proc が共用）と `encodeJsonbBinary`（encoding.nim:1011）。
  対照: encoding.nim 内の `newSeq[byte]` 全 **24箇所**（grep `newSeq\[byte\]`）のうち、可変長で独自ガードを持つのは
  297/401/438（payload ガード 398/435 等）,1087/1098/1173（`checkPgBinPayload` 1086/1097/1172）,61（50 で事前ガード）;
  92 は UUID 文字列（36バイト固定）で境界自明; 残り（655,660,672,690,774,779,791,799,837,881,1058,1067,1073,1079,1106）は
  固定サイズ。ガード無し可変長は 1011 のみ（＋共用根の core.nim:1083）。
