# buffer_io.nim レビュー結論

## 指摘1: `configureTcpNoDelay` のエラー握りつぶし — 妥当 (低)

`buffer_io.nim:746-748` で `discard setsockopt(...)` により失敗を無視。
`configureKeepalive` (line 754) は `setSockOptInt` 経由で失敗時に `PgConnectionError` を送出するため一貫性がない。

TCP_NODELAY は性能ヒントであり失敗しても接続は成立するため、意図的な設計と判断できる。
ただし意図を示すコメントがなく誤解を招く。

## 指摘2: asyncdispatch `fillRecvBuf` 孤立書き込み — 妥当 (中・現在は安全)

`buffer_io.nim:220-235` でタイムアウト時に `recvBuf.setLen(oldLen)` で切り詰めるが、
孤立した `recvInto` が `recvBuf[oldLen..]` への生ポインタを保持し続ける。

安全根拠:
- `invalidateOnTimeout` (`simple_query.nim:211-226`) が `csClosed` に設定し `PgTimeoutError` を送出
- `csClosed` 後は `fillRecvBuf`/`compactRecvBuf` が再呼び出しされない設計
- seq の `setLen` による縮小はキャパシティを維持するため、孤立書き込みは確保済みメモリ内に留まる

現在は安全だが、不変条件はコメントのみで維持されており、`compactRecvBuf` (line 169) に
`doAssert conn.state != csClosed` のような防御的アサーションはない。
将来のコード変更で破壊されるリスクあり。

## 指摘3: line 357 のフルパース — 妥当だが現在は到達不能

`buffer_io.nim:357-359` のパスは `rowData == nil` かつ `skipDataRow == false` かつ
`onRow == nil` のときのみ到達可能。

実際の呼び出し確認:
- `cursor.nim:62,148`: `nextMessage(rowData, addr count)` — rowData あり
- `pipeline.nim:459,626`: `nextMessage(rowData, rowCount)` — rowData あり
- `recvMessage` の唯一の呼び出し (`notify.nim:167`): 引数なし

現在このパスに到達する呼び出しは存在しない。
将来 `recvMessage(rowData = nil, rowCount = addr n)` のような使い方がされた場合に
アロケーションの無駄が発生する潜在的な非効率。

## 指摘4: `readyBody` 例外による `queryError` 隠蔽 — 妥当 (低・設計)

`buffer_io.nim:409-411` (3つのオーバーロード全て) で `readyBody` が例外を送出すると
`queryError` は失われる。

`readyBody` は状態管理（トランザクション状態の記録など）であり例外を送出しない前提。
実害はほぼないが、サーバーエラーを確実に伝播させたい場合は
`readyBody` を try/except で囲む検討の余地あり。

## 指摘5: `onRow` パスの nil ガードなし — 妥当 (情報)

`buffer_io.nim:327,334` で `onRowError[]` と `rowData.buf` を nil チェックなしで参照。
doc comment (line 293-296) で契約を明記。

契約ベースの設計として妥当。`doAssert` を追加すれば開発時のみ検出可能
（リリースではコンパイル除外）で、実行時コストなし。

## 総合評価

重大なバグはなし。指摘2の孤立書き込みが最も注意を要するが、現在の設計では安全。
防御的改善として `compactRecvBuf` へのアサーション追加が最も費用対効果が高い。
