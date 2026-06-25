# Jobworkerp Conductor

Jobworkerp Conductor は、運用イベントと
[jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs) の実行をつなぐイベントオーケストレーションサービスです。cron スケジュール、Slack イベント、jobworkerp worker の完了結果をきっかけに、workflow file または登録済み worker を実行します。

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](Cargo.toml)

English: [README.md](README.md)

## 概要

- **Cron scheduler**: 設定した schedule に従って workflow / worker を実行します。
- **Slack handler**: Slack Socket Mode の message、app mention、reaction を条件に一致させます。
- **Worker result handler**: 指定した jobworkerp worker の結果を監視し、後続処理を実行します。
- **gRPC admin API**: jobworkerp server、handler、scheduler、実行状態を管理します。
- **DB backed configuration**: ローカル / 単一プロセス構成では SQLite を使い、MySQL 向け build も利用できます。

## リポジトリ構成

```text
conductor-main/         メインバイナリと CLI
jobworkerp-conductor/   初期化レイヤーと動的ハンドラーマネージャー
slack-event-handler/    Slack Socket Mode のマッチングと実行
jobworkerp-handler/     Worker result listener
api/app/                アプリケーションサービス
api/infra/              リポジトリ実装と DB セットアップ
api/grpc-admin/         gRPC admin server
proto/                  Protocol Buffers 定義
shared/                 実行、バリデーション、通知の共通処理
modules/                再利用ライブラリ crate
docker/                 コンテナ関連ファイル
```

## 前提条件

- Rust stable toolchain
- `protoc`
- Linux package: `pkg-config`、`protobuf-compiler`
- デフォルト構成では SQLite
- 任意: プロセス間の設定変更通知に Redis
- 任意: Slack 連携に Socket Mode を有効化した Slack App

Ubuntu/Debian 例:

```bash
sudo apt-get update
sudo apt-get install -y pkg-config protobuf-compiler
```

## クイックスタート

workspace をビルドします。

```bash
cargo build
```

ローカル SQLite DB と `127.0.0.1:9090` の plaintext gRPC admin API で起動します。

```bash
export GRPC_ADDR=127.0.0.1:9090
export SQLITE_URL=sqlite://conductor.sqlite3
export SQLITE_MAX_CONNECTIONS=20
export NOTIFICATION_TYPE=channel

cargo run --bin conductor-main
```

プロセスは CLI parse 前に `.env` を読み込むため、同じ値をローカル `.env` に置くこともできます。

## CLI

```bash
# DB 駆動モードで起動。subcommand なしの場合もこのモードです。
cargo run --bin conductor-main
cargo run --bin conductor-main -- serve

# legacy TOML 駆動モードで起動。
cargo run --bin conductor-main -- serve \
  --legacy-mode \
  --file-settings-toml ./workflows.toml

# conductor admin worker を jobworkerp server に登録 / 登録解除。
cargo run --bin conductor-main -- register-workers \
  --jobworkerp-url http://localhost:9000 \
  --conductor-grpc-url http://localhost:9090

cargo run --bin conductor-main -- unregister-workers \
  --jobworkerp-url http://localhost:9000
```

`register-workers` は `conductor-admin` FunctionSet と static worker を作成します。これらの worker は jobworkerp の GRPC runner 経由で conductor の gRPC admin API を呼び出します。

## 設定

### 主要な環境変数

| 変数 | 必須 | 説明 |
| --- | --- | --- |
| `GRPC_ADDR` | yes | gRPC admin の listen address。例: `127.0.0.1:9090`、`0.0.0.0:9100`。 |
| `USE_GRPC_WEB` | no | `true` の場合 gRPC-Web を有効化します。デフォルト: `false`。 |
| `MAX_FRAME_SIZE` | no | tonic の max frame size を byte で指定します。 |
| `SQLITE_URL` | SQLite | SQLite URL。例: `sqlite://conductor.sqlite3`。 |
| `SQLITE_MAX_CONNECTIONS` | SQLite | SQLite connection pool size。 |
| `MYSQL_URL` | MySQL | MySQL URL。例: `mysql://user:pass@localhost:3306/conductor`。 |
| `MYSQL_MAX_CONNECTIONS` | MySQL | MySQL connection pool size。 |
| `NOTIFICATION_TYPE` | no | 単一プロセスでは `channel`、プロセス間の設定変更通知には `redis`。デフォルト: `channel`。 |
| `NOTIFICATION_BUFFER_SIZE` | no | `NOTIFICATION_TYPE=channel` の buffer size。デフォルト: `1000`。 |
| `NOTIFICATION_CHANNEL_PREFIX` | no | Redis Pub/Sub channel prefix。デフォルト: `conductor_config`。 |
| `REDIS_URL` | Redis 通知使用時 yes | `NOTIFICATION_TYPE=redis` の Redis URL。 |
| `SLACK_APP_TOKEN` | Slack 使用時 yes | Slack Socket Mode 用 App-Level Token。通常は `xapp-` で始まります。 |
| `CONDUCTOR_CRON_TIMEZONE` | no | cron schedule を解釈する IANA time zone。未設定時は `TZ`、次に UTC。 |
| `JOBWORKERP_URL` | worker 登録時 yes | `register-workers` / `unregister-workers` が接続する jobworkerp endpoint。 |
| `CONDUCTOR_GRPC_URL` | no | 登録される admin worker に保存する conductor gRPC URL。デフォルト: `http://localhost:9090`。 |

### 設定変更通知

通常の単一 conductor プロセスでは次を使います。

```bash
NOTIFICATION_TYPE=channel
```

Redis 通知は、管理 API プロセスと実行プロセスを分け、設定変更をプロセス間に届けるためのものです。cron、Slack event、worker result の重複処理を避ける仕組みを別途用意しない限り、実行系プロセスは単一構成で運用してください。

## gRPC Admin API

admin server は tonic reflection を有効化しているため、`grpcurl` で service を確認できます。

```bash
grpcurl -plaintext 127.0.0.1:9090 list
```

jobworkerp server を登録します。

```bash
grpcurl -plaintext -d '{
  "name": "local-jobworkerp",
  "host": "127.0.0.1",
  "port": "9000",
  "ssl_enabled": false,
  "description": "Local development server",
  "enabled": true
}' 127.0.0.1:9090 jobworkerp_conductor.service.JobworkerpServerService/Create
```

cron scheduler を作成します。`tokio-cron-scheduler` は秒 field を含む cron expression を受け取れます。次の例は毎時0分に実行します。

```bash
grpcurl -plaintext -d '{
  "name": "hourly-report",
  "jobworkerp_server_id": {"value": 1},
  "crontab": "0 0 * * * *",
  "enabled": true,
  "description": "Run an hourly report workflow",
  "workflow": {
    "workflow_url": "file:///workflows/hourly-report.yaml",
    "channel": "reports"
  },
  "args": "{\"source\":\"cron\"}"
}' 127.0.0.1:9090 jobworkerp_conductor.service.CronSchedulerService/Create
```

Slack message handler を作成します。

```bash
grpcurl -plaintext -d '{
  "name": "deploy-command",
  "description": "Run deploy workflow from Slack",
  "enabled": true,
  "slack_channel_id": "C123456789",
  "message_pattern": "^deploy (staging|production)$",
  "mention_required": false,
  "jobworkerp_server_id": {"value": 1},
  "workflow": {
    "workflow_url": "file:///workflows/deploy.yaml",
    "channel": "deploy"
  },
  "timeout_sec": 3600,
  "args": "{\"notify\":true}"
}' 127.0.0.1:9090 jobworkerp_conductor.service.SlackEventHandlerService/Create
```

設定済みハンドラーを一覧表示します。

```bash
grpcurl -plaintext -d '{"limit": 100, "offset": 0}' \
  127.0.0.1:9090 \
  jobworkerp_conductor.service.CronSchedulerService/FindList

grpcurl -plaintext -d '{"limit": 100, "offset": 0}' \
  127.0.0.1:9090 \
  jobworkerp_conductor.service.SlackEventHandlerService/FindList

grpcurl -plaintext -d '{"limit": 100, "offset": 0}' \
  127.0.0.1:9090 \
  jobworkerp_conductor.service.WorkerResultHandlerService/FindList
```

主要 service package は `jobworkerp_conductor.service.*` です。完全な API は `proto/protobuf/jobworkerp_conductor/` を参照してください。

## Slack 設定

1. Slack App を作成します。
2. Socket Mode を有効化します。
3. App-Level Token を作成し、`SLACK_APP_TOKEN` に設定します。
4. 必要な event を subscribe します。例: `message.channels`、`app_mention`、`reaction_added`、`reaction_removed`。
5. event に対応する OAuth scope を追加します。例: `channels:history`、`app_mentions:read`、`reactions:read`。
6. App を workspace に install します。

`SLACK_APP_TOKEN` が未設定または空の場合、conductor は Slack Socket Mode listener なしで起動します。

## データベース

SQLite は開発用途と単一プロセス運用のデフォルト選択です。SQLite schema はバイナリに埋め込まれ、起動時に適用されます。

MySQL を使う場合は、conductor 起動前に schema を初期化します。

```bash
mysql -u root -p -e "CREATE DATABASE conductor;"
mysql -u root -p conductor < api/infra/sql/mysql/002_schema.sql
```

その後、MySQL 対応 build で `MYSQL_URL` と `MYSQL_MAX_CONNECTIONS` を指定して起動します。

## 開発

```bash
cargo build
cargo test -- --test-threads=1
cargo fmt --check
cargo clippy
```

対象 package を絞る場合:

```bash
cargo test -p conductor-main -- --test-threads=1
cargo test -p slack-event-handler -- --test-threads=1
```

外部 service を必要とする integration test は、`TEST_REDIS_URL`、`TEST_MYSQL_URL`、`TEST_JOBWORKERP_HOST`、`TEST_WORKFLOW_URL` などの環境変数を使います。

## Docker

`conductor-main/Dockerfile` は、事前に build した `target/x86_64-unknown-linux-musl/release/conductor-main` を package します。`docker build` の前に musl binary を build してください。

## ライセンス

Apache-2.0。詳細は [Cargo.toml](Cargo.toml) を参照してください。
