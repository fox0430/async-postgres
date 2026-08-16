# buffer_io.nim 監査所見

対象: `async_postgres/pg_connection/buffer_io.nim` (780行)
バックグラウンド: デフォルトバックエンドは asyncdispatch (`async_backend.nim:9`)。
テストは chronos で実行 (`async-postgres.nimble`: `-d:asyncBackend=chronos`)。

所見はすべて対処済み（対応は git log を参照）。以下は検証済み・所見なしとした系統比較の結果。

---

## 検証済み・所見なし（系統比較の結果）

- `pumpUntilReady` 3オーバーロード (376-415 / 417-448 / 450-482) のエラー/キャンセル処理は**完全に対称**。`bmkErrorResponse` 捕捉、`bmkReadyForQuery` での `conn.txStatus` 更新・`if conn.state != csClosed: conn.state = csReady`・`readyBody`・`if queryError != nil: raise queryError`・`break pumpLoop` は3者で文字列レベルで同一。非対称は無い（`nextMessage` へ渡す引数のみ用途通り異なる: overload1=`(resultData, rowCountPtr)`、overload2=`(resultData, nil, onRow, onRowErr)`、overload3=`(skipDataRow=true)`）。
- `parseBackendMessage` の `maxLen` 上限 (pg_protocol.nim:1201 `int64(msgLen) >= int64(maxLen)`) は off-by-one なし: 総サイズ `maxLen`（msgLen=maxLen-1）は許可、`maxLen+1`（msgLen=maxLen）は拒否。巨大メッセージによる過剰確保は `effectiveMaxMessageSize`（既定1GiB, types.nim:771-777）で上界付き。
- `closeTransport` (585-621) は nil チェック + nil 代入により冪等。`isConnected` (709-729) は close 後（asyncdispatch: `socket=nil`、chronos: `writer=nil`）に false を返す。
- `nextMessage` の `recvBufStart` 更新 (323-324行) は `onRow` 例外捕捉 (332-333) より前に行われ、`PgProtocolError` 時は `csClosed` 化 (319行) して再 raise。`pos` は `consumed >= 5` で単調増加するため無限ループなし。
