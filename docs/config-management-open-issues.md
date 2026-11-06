# 設定管理の未解決課題

## DB設定のTOML往復で有効状態と説明が保持されない

- 重要度: medium
- 対象: `api/infra/src/infra/config_management/rdb.rs`
- 問題: TOMLのimportではscheduler/listenerを常に有効、descriptionを未設定として保存し、exportにも両フィールドを含めない。このためDB設定をTOMLでバックアップ・復元すると、無効状態とdescriptionが失われる。
- 対応案: `WorkflowSettingItem` に必要なoptionalフィールドを加え、importのINSERTと両方のexport queryへ追加する。旧TOMLでの省略を維持し、enabled/descriptionを含む隔離DBのroundtripテストを追加する。

## MySQLの重複importエラー判定

- 重要度: minor
- 対象: `api/infra/src/infra/config_management/rdb.rs`
- 問題: 既存の設定管理integration testで重複server importを行うと、MySQLはSQLSTATE `23000` とvendor code `1062` を返す。実装は `Database::code()` のみ確認しており、返されるSQLSTATEをduplicateと認識できず、テストが期待する `AlreadyExists` にならない。SQLiteでは同じ重複テストが通る。
- 対応案: `Database::code()` と `Database::constraint()` またはMySQL固有codeを適切に判別して、重複server/schedulerを一貫して `AlreadyExists` に変換する。隔離SQLiteとMySQLのintegration testで確認する。
