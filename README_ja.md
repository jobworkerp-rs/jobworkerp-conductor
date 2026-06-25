# Jobworkerp Conductor

分散ジョブワーカープラットフォーム [jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs) 向けの強力なイベント駆動型自動化システムです。Slackイベント、cronスケジュール、ジョブ完了イベントをトリガーとした動的なワークフロー実行を可能にします。

[![Pull Request](https://github.com/jobworkerp-rs/jobworkerp-conductor/actions/workflows/pull-request.yml/badge.svg)](https://github.com/jobworkerp-rs/jobworkerp-conductor/actions/workflows/pull-request.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](Cargo.toml)

## 機能

- **🔄 動的Slackイベント処理**: Socket Modeを使用したSlackメッセージ、メンション、リアクションのリアルタイム処理
- **⏰ Cronベースのスケジューリング**: 柔軟なcronスケジュールでワークフローを実行
- **📊 ジョブ結果リスナー**: jobworkerpサーバーからのジョブ完了イベントに反応
- **🔧 DB駆動の設定管理**: データベース経由で全設定を管理し、即座に反映（再起動不要）
- **🚀 gRPC管理API**: 設定管理用のフル機能管理API

## クイックスタート

### 前提条件

- Rustツールチェーン（最新安定版）
- Protocol Buffersコンパイラ（protoc）
- 永続化: SQLite（標準）
- オプション: MySQL（外部DBや複数プロセスからの設定DB共有が必要な場合）
- オプション: Redis（設定変更通知をプロセス間配信する場合）
- Slack連携用: Socket Modeが有効なSlack App

### インストール

```bash
# リポジトリのクローン
git clone https://github.com/jobworkerp-rs/jobworkerp-conductor.git
cd jobworkerp-conductor

# システム依存パッケージのインストール（Ubuntu/Debian）
apt-get install -y libssl-dev pkg-config libudev-dev

# プロジェクトのビルド
cargo build --release
```

### 基本的な使い方

#### 1. ハンドラーの起動（DB駆動モード）

```bash
# デフォルトのSQLiteデータベースを使用
cargo run --bin conductor-main

# カスタムデータベースURLを使用
DATABASE_URL=sqlite://./my-handler.sqlite3 cargo run --bin conductor-main

# MySQLを使用
DATABASE_URL=mysql://user:password@localhost:3306/conductor cargo run --bin conductor-main
```

#### 2. Slack連携の設定（オプション）

```bash
# Socket Mode用のSlack Appトークンを設定
export SLACK_APP_TOKEN=xapp-1-A0XXXXXXXXX-...

# Slackイベント処理を有効にするためハンドラーを再起動
cargo run --bin conductor-main
```

#### 3. gRPC API経由でハンドラーを設定

```bash
# grpcurlのインストール
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# jobworkerpサーバーの登録
grpcurl -plaintext -d '{
  "name": "production-server",
  "host": "jobworkerp.example.com",
  "port": "9000",
  "ssl_enabled": true,
  "enabled": true
}' localhost:9090 jobworkerp_conductor.service.JobworkerpServerService/Create

# Slackイベントハンドラーの登録
grpcurl -plaintext -d '{
  "name": "deploy-notification",
  "description": "Execute workflow on deploy notifications",
  "enabled": true,
  "slack_channel_id": "C123456789",
  "message_pattern": "^deploy (production|staging)",
  "mention_required": false,
  "jobworkerp_server_id": {"value": 1},
  "workflow": {
    "workflow_url": "https://example.com/workflows/deploy.yaml",
    "channel": "deploy"
  },
  "timeout_sec": 3600,
  "args": "{\"notify_channel\": \"#alerts\"}"
}' localhost:9090 jobworkerp_conductor.service.SlackEventHandlerService/Create
```

## アーキテクチャ

### ワークスペース構造

```
jobworkerp-conductor/
├── conductor-main/        # メインアプリケーションのエントリーポイント
├── jobworkerp-conductor/       # 初期化レイヤーとイベントハンドラーマネージャー
├── slack-event-handler/    # Slackイベント処理実装
├── jobworkerp-handler/     # レガシージョブ結果リスナー
├── modules/                # 共通ライブラリモジュール
│   ├── jobworkerp-client/  # jobworkerpサーバー通信
│   ├── command-utils/      # 共通ユーティリティ
│   ├── infra-utils/        # インフラストラクチャユーティリティ
│   ├── memory-utils/       # キャッシュ、ロック、チャネル
│   └── net-utils/          # ネットワークユーティリティ
├── api/                    # クリーンアーキテクチャレイヤー
│   ├── app/                # アプリケーション層（サービス）
│   ├── infra/              # インフラストラクチャ層（リポジトリ）
│   └── grpc-admin/         # gRPC管理API
├── proto/                  # Protocol Buffers定義
└── shared/                 # 共有コンポーネント
```

### 主要コンポーネント

#### EventHandlerServerManager
複数のイベントハンドラー（Cron、Slack、WorkerResult）の統合管理を行い、動的な設定反映を実現します。

#### DynamicSlackHandlerManager
- Socket Modeを使用したリアルタイムSlackイベント受信
- 自動イベントタイプ検出（message、app_mention、reaction_added/removed）
- 再起動なしで設定変更を即座に反映

#### NotificationService
インメモリチャネルとRedis Pub/Subの両方をサポートする設定変更通知ブロードキャストシステム。

#### WorkflowExecutor
jobworkerpサーバーへのgRPC通信、エラーハンドリング、リトライ戦略を備えた共通ワークフロー実行実装。

## 設定

通常の単一conductorプロセス構成では、永続化にSQLite、設定変更通知に `NOTIFICATION_TYPE=channel` を使う構成を推奨します。MySQLとRedisは必要に応じて選択するオプションです。MySQLは外部DBを使いたい場合や複数プロセスから設定DBを共有したい場合、Redisは設定変更通知をプロセス間に配信したい場合に使用します。これらを有効にしても、複数の実行系conductorをactive-activeで安全に動かすための分散排他は提供されません。

### 環境変数

| 変数 | 説明 | デフォルト |
|----------|-------------|---------|
| `DATABASE_URL` | データベース接続URL | `sqlite://conductor.sqlite3` |
| `NOTIFICATION_TYPE` | 設定変更通知の方式。通常は `channel` を推奨。`redis` は設定変更通知をプロセス間配信するための方式 | `channel` |
| `NOTIFICATION_BUFFER_SIZE` | `NOTIFICATION_TYPE=channel` の通知バッファサイズ | `1000` |
| `REDIS_URL` | `NOTIFICATION_TYPE=redis` のRedis接続URL。`channel` 使用時は未設定または空で可 | - |
| `SLACK_APP_TOKEN` | Socket Mode用Slack Appトークン（Slack機能に必須） | - |

### 設定変更通知の方式

通常の単一conductorプロセス構成では、`NOTIFICATION_TYPE=channel` を推奨します。この場合、設定変更通知はプロセス内のメモリチャネルで配信され、Redisは不要です。`REDIS_URL` は未設定または空のままで動作します。

`NOTIFICATION_TYPE=redis` は、管理APIプロセスと実行プロセスを分ける構成で、DB更新後の設定変更通知をプロセス間に配信するためのものです。これは将来のHA/分散構成の基盤として想定された通知経路であり、実行処理の分散や重複防止を意味するものではありません。

現状のRedis通知は実行の分散排他を提供しません。複数の実行系conductorを同時稼働させると、同一のcron schedulerやSlack eventに対して処理が重複実行される可能性があります。Redisを利用する場合でも、複数の実行系conductorをactive-activeで動かす構成は現時点では推奨しません。実行系は単一プロセスに限定するか、別途分散ロックやjobの重複防止キーを設計してください。

### データベースのセットアップ

#### SQLite（デフォルト）
手動セットアップは不要です。初回実行時にデータベースファイルが自動作成されます。

#### MySQL
```bash
# データベースの作成
mysql -u root -p -e "CREATE DATABASE conductor;"

# スキーマの初期化
mysql -u root -p conductor < api/infra/sql/mysql/002_schema.sql

# MySQLで実行
DATABASE_URL=mysql://user:password@localhost:3306/conductor cargo run --bin conductor-main
```

### Slack Appのセットアップ

1. **Slack Appの作成**: https://api.slack.com/apps
2. **Socket Modeの有効化**とApp-Level Token（`SLACK_APP_TOKEN`）の生成
3. **Event Subscriptionsの設定**:
   - `message.channels`、`message.groups`、`message.im`、`message.mpim`
   - `app_mention`
   - `reaction_added`、`reaction_removed`
4. **OAuth Scopesの設定**:
   - `channels:history`、`groups:history`、`im:history`、`mpim:history`
   - `app_mentions:read`
   - `reactions:read`
5. **ワークスペースへのインストール**

## 使用例

### Slackイベントハンドラーの管理

#### メッセージパターンハンドラーの作成
```bash
grpcurl -plaintext -d '{
  "name": "error-notification",
  "description": "Notify on error messages",
  "enabled": true,
  "slack_channel_id": "C123456789",
  "message_pattern": "error|exception|failed",
  "jobworkerp_server_id": {"value": 1},
  "workflow": {
    "workflow_url": "https://example.com/workflows/error-handler.yaml",
    "channel": "errors"
  }
}' localhost:9090 jobworkerp_conductor.service.SlackEventHandlerService/Create
```

#### リアクションベースハンドラーの作成
```bash
grpcurl -plaintext -d '{
  "name": "approval-workflow",
  "description": "Execute approval workflow on thumbsup reaction",
  "enabled": true,
  "slack_channel_id": "C987654321",
  "reaction_names": "thumbsup,white_check_mark",
  "reaction_operation": "REACTION_OPERATION_ADDED",
  "jobworkerp_server_id": {"value": 1},
  "workflow": {
    "workflow_url": "https://example.com/workflows/approval.yaml",
    "channel": "approval"
  }
}' localhost:9090 jobworkerp_conductor.service.SlackEventHandlerService/Create
```

#### ボットメンションハンドラーの作成
```bash
grpcurl -plaintext -d '{
  "name": "bot-command",
  "description": "Execute commands on bot mentions",
  "enabled": true,
  "message_pattern": "^help|status|version",
  "mention_required": true,
  "jobworkerp_server_id": {"value": 1},
  "workflow": {
    "workflow_url": "https://example.com/workflows/bot-command.yaml",
    "channel": "commands"
  }
}' localhost:9090 jobworkerp_conductor.service.SlackEventHandlerService/Create
```

#### すべてのハンドラーのリスト表示
```bash
grpcurl -plaintext localhost:9090 \
  jobworkerp_conductor.service.SlackEventHandlerService/FindList
```

#### ハンドラーの更新
```bash
grpcurl -plaintext -d '{
  "id": {"value": 1},
  "data": {
    "name": "deploy-notification",
    "description": "Execute workflow on deploy notifications",
    "enabled": false,
    "slack_channel_id": "C123456789",
    "message_pattern": "^deploy (production|staging)",
    "mention_required": false,
    "jobworkerp_server_id": {"value": 1},
    "workflow": {
      "workflow_url": "https://example.com/workflows/deploy.yaml",
      "channel": "deploy"
    },
    "timeout_sec": 3600
  }
}' localhost:9090 jobworkerp_conductor.service.SlackEventHandlerService/Update
```

#### ハンドラーの削除
```bash
grpcurl -plaintext -d '{"value": 1}' localhost:9090 \
  jobworkerp_conductor.service.SlackEventHandlerService/Delete
```

### Cronスケジューラーの管理

```bash
# cronスケジューラーの作成
grpcurl -plaintext -d '{
  "name": "daily-report",
  "description": "Generate daily report at 9 AM",
  "enabled": true,
  "crontab": "0 9 * * *",
  "jobworkerp_server_id": {"value": 1},
  "workflow": {
    "workflow_url": "https://example.com/workflows/daily-report.yaml",
    "channel": "reports"
  }
}' localhost:9090 jobworkerp_conductor.service.CronSchedulerService/Create

# すべてのスケジューラーのリスト表示
grpcurl -plaintext localhost:9090 \
  jobworkerp_conductor.service.CronSchedulerService/FindList
```

## 開発

### ビルドとテスト

```bash
# ワークスペース全体のビルド
cargo build

# テストの実行
cargo test

# 並列実行を制御したテスト（CI推奨）
cargo test -- --test-threads=1

# 統合テストの実行
cargo test --test integration_tests -- --test-threads=1

# コード品質チェック
cargo check
cargo clippy
cargo fmt
```

### デバッグモードでの実行

```bash
# デバッグログの有効化
cargo run --bin conductor-main -- --debug
```

### レガシーTOMLモード

```bash
# レガシーTOML設定で実行
cargo run --bin conductor-main -- serve --legacy-mode --file-settings-toml ./workflows.toml
```

## トラブルシューティング

### Slackイベントが処理されない

1. `SLACK_APP_TOKEN`が正しく設定されているか確認:
   ```bash
   echo $SLACK_APP_TOKEN  # xapp-で始まる文字列が表示されるはず
   ```

2. Socket Mode接続のログを確認:
   ```bash
   grep "Socket Mode listener started" conductor_*.log
   ```

3. ハンドラー設定を確認:
   ```bash
   grpcurl -plaintext localhost:9090 \
     jobworkerp_conductor.service.SlackEventHandlerService/FindList
   ```

### ワークフロー実行が失敗する

1. jobworkerpサーバーが実行中か確認:
   ```bash
   curl http://localhost:9000/health
   ```

2. ワークフローURLがアクセス可能か確認:
   ```bash
   curl -I http://your-server/workflow.yaml
   ```

3. リトライログを確認:
   ```bash
   grep "Workflow execution failed" conductor_*.log
   ```

### 設定変更が反映されない

1. NotificationServiceのログを確認:
   ```bash
   grep "Configuration change notification" conductor_*.log | tail -n 10
   ```

2. EventHandlerServerManagerが通知を受信しているか確認:
   ```bash
   grep "Config changed event received" conductor_*.log | tail -n 10
   ```

3. 最終手段として再起動:
   ```bash
   # Dockerの場合
   docker restart conductor-main

   # systemdの場合
   sudo systemctl restart conductor-main
   ```

## 監視

### ヘルスチェック

```bash
grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check
```

### 推奨監視項目

1. **プロセス実行状態**
2. **Socket Mode接続状態**
3. **エラー率**（閾値: 5分間で5%超過）
4. **ワークフロー実行失敗**（閾値: 1時間で10回超過）
5. **アクティブハンドラー数**

### 監視コマンド

```bash
# プロセス状態の確認
systemctl status conductor-main

# エラー率の監視
grep -c "ERROR" conductor_*.log

# アクティブハンドラー数のカウント
grpcurl -plaintext localhost:9090 \
  jobworkerp_conductor.service.SlackEventHandlerService/FindList | \
  jq -s '[.[] | select(.data.enabled)] | length'
```

## バックアップとリストア

### 設定のバックアップ

```bash
# SQLiteの場合
cp conductor.sqlite3 conductor.sqlite3.backup.$(date +%Y%m%d_%H%M%S)

# MySQLの場合
mysqldump -u user -p conductor_db > conductor-backup.sql
```

### 設定のリストア

```bash
# SQLiteの場合
cp conductor.sqlite3.backup.20251003_120000 conductor.sqlite3
systemctl restart conductor-main

# MySQLの場合
mysql -u user -p conductor_db < conductor-backup.sql
systemctl restart conductor-main
```

## Dockerデプロイメント

```bash
# Dockerイメージのビルド
docker build -t conductor-main ./conductor-main

# Dockerで実行
docker run -d \
  -e DATABASE_URL=sqlite://conductor.sqlite3 \
  -e SLACK_APP_TOKEN=xapp-1-... \
  -p 9090:9090 \
  -v $(pwd)/data:/data \
  conductor-main

# 本番環境向けビルド（muslターゲット）
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

## ライセンス

このプロジェクトは `Cargo.toml` で宣言されている Apache-2.0 ライセンスです。

## 関連プロジェクト

- [jobworkerp-rs](https://github.com/jobworkerp-rs/jobworkerp-rs) - 分散ジョブワーカープラットフォーム

## サポート

問題、質問、コントリビューションについては、このリポジトリの issue tracker で issue を開いてください。
