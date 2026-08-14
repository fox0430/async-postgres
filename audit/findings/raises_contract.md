# raises pragma 実コンパイル検証（S1 クラスタ最終クローズ）

対象: 本番 `raises: []` 実 proc 宣言の全数（10 件）と非空 raises 契約（buffer_io.nim:312
`raises: [PgProtocolError]`、lifecycle.nim:35 `raises: [PgConnectionError]` ほか）。
方法: `--experimental:strictDefects` は利用可能な全コンパイラ（2.0.16 / 2.2.4 / 2.2.10 / 2.3.1 devel）に
存在しないため（compiler ソースに strictDefects の痕跡なし）、本文精読による Defect 生成操作の
網羅列挙で代替。catchable 側の契約整合は CI の実コンパイル（両バックエンド）で検証済み。

## 結論: Defect リーク経路 0 件（所見なし）

`raises: []` proc 内の Defect 生成操作（配列アクセス・int 変換・nil deref・overflow）は
全 10 件で遮蔽または非存在。機械スキャンの「raises 不一致の疑い」は実証されず、S1 クラスタは
完全クローズ。

### 全数監査の内訳

| サイト | 契約 | Defect 経路の処理 |
|---|---|---|
| pg_pool.nim:1306 failAllPending | `raises: []` | body 全体を `try/except Exception` で遮蔽（**Defect <: Exception のため Defect も捕捉**）。コメントに「将来の変更をコンパイル時に捕捉」と明記 |
| pg_pool.nim:1540 failPendingAndUnschedule | `raises: []` | 呼び出しは遮蔽済み failAllPending + フィールド代入のみ |
| pg_pool.nim:1546 scheduleDispatch | `raises: []` | cb 内の asyncSpawn を `try/except Exception` で遮蔽。Defect は PgError に変換され failPendingAndUnschedule へ |
| notify.nim:134 newListenError | `raises: []` | ref オブジェクト構築のみ（OOM 以外の Defect 生成操作なし） |
| notify.nim:139 notifyListenDeath | `raises: []` | `waiter.fail` のみ `try/except Exception` 遮蔽（コメント: asyncdispatch の `Future.fail` は callback chain 経由で `Exception` 効果、151行）。他は代入と raises:[] ユーザコールバック |
| notify.nim:319 failNotifyWaiter | `raises: []` | `waiter.fail` を `try/except Exception` 遮蔽 |
| buffer_io.nim:112 dispatchNotification* | `raises: []` | キュー操作は長さガード付き（popFirst は `len >= notifyMaxQueue > 0`）、`notifyDropped.inc` は `high(int)` ガード付き、`waiter.complete` のみ `try/except Exception` 遮蔽 |
| buffer_io.nim:142 dispatchNotice* | `raises: []` | オブジェクト構築 + raises:[] ユーザコールバックのみ |
| pg_bearssl.nim:27 appendDnCallback | `raises: []`（cdecl） | `int(len)` 変換は `len > csize_t(high(int))` で事前 return（31行コメント: "Defect leaks past raises: [] into C (UB)"）。p は UncheckedArray で検査なし、範囲は BearSSL 側の長さ契約 |
| pg_bearssl.nim:60 x509CaptureAppend | `raises: []`（cdecl） | 同様に `len <= csize_t(high(int))` ガード後変換（62-63行）。`oldLen + n` の int 加算 overflow は 2^63 バイト相当で現実的到達性なし |

### 検証済みの非該当クラス

- **非空 raises 契約**（nextMessage `[PgProtocolError]` / enforceAuthAllowed `[PgConnectionError]` ほか 5 件）:
  catchable 集合はコンパイラが検証済み（両バックエンドでビルド成功）。Defect はどの raises 契約にも
  含まれない言語仕様であり、契約不一致ではない。
- **`{.async: (raises: [CatchableError]).}` 6 件**（async_backend:313,327 / pg_replication:526 /
  pg_largeobject:62 ほか）: 同上。catchable 側はコンパイル検証済み。
- **ユーザコールバック型の raises: []**（types.nim 約30 / async_backend:46,70,252,273 /
  pg_pool_cluster:24）: コールバック内でユーザが Defect を投げればライブラリの raises:[] proc を
  抜けるが、これはユーザコードの契約違反でありライブラリの欠陥ではない。
- **OOM（OutOfMemDefect）**: 全 proc 共通。Nim のどの raises 契約も OOM を射程外とする。
- **except Exception の副作用**: Defect を捕捉して捨てる箇所（failAllPending 等）では失敗の
  シグナル自体は Future.fail で伝達済み（fail 自体が操作の本体）のため、Defect 消失による
  見逃しは構造的に発生しない。asyncSpawn 内 Defect は PgError に変換して伝達（scheduleDispatch）。

### 留保

- strictDefects（Defect 効果の機械検証）が利用可能な Nim 3.x 系では再検証の価値あり。
- pg_bearssl 2 箇所は cdecl 境界であり、ガード式の高さ自体は手動確認のみ（レビュー 2026-08-13）。
