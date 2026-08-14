ファジングの空白
  test_protocol_fuzz.nim はプロトコルのみ。encoding/decoding, DSN, SQLパース, hstore/range バイナリにはファズテストが存在しない。

構造的問題
- test_types.nim が9062行/1142件で巨大化。encoding/decoding/ranges の境界が曖昧で、ギャップが埋もれやすい
- test_sql.nim は356行/77件に対し pg_sql.nim は586行。raw文字列・コメント・?演算子の網羅性に要注目

優先レビュー順（テスト側）
1. test_ssl.nim — MITM・証明書検証の否定ケースが18件と薄い。split-write以外の攻撃ベクトル検証要確認
2. test_rowdata.nim — decoding の否定テスト2件のみ。Tier 1 の decoding.nim に対する検証がほぼ不在
3. test_fill_recvbuf.nim — buffer_io の専用テストが6件。UB/範囲外の回帰テスト不足
4. test_types.nim — 巨大すぎてレビュー困難。encoding/decoding/ranges への分割が必要
5. test_protocol_fuzz.nim — 良好だが、ファズ対象を types/DSN/SQL へ拡張すべき
6. test_largeobject.nim — chunkSize ハードニングの検証が23件中14件と薄い
