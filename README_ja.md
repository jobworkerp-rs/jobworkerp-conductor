# Jobworkerp Conductor

Jobworkerp Conductor は、運用イベントと
[jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs) の実行をつなぐイベントオーケストレーションサービスです。cron スケジュール、Slack イベント、jobworkerp worker の完了結果をきっかけに、workflow file または登録済み worker を実行します。

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

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
```

## インストール

ビルド済みバイナリを各 [GitHub Release](https://github.com/jobworkerp-rs/jobworkerp-conductor/releases) に添付しています。

| Asset | 動作要件 |
|---|---|
| `conductor-main-x86_64-unknown-linux-musl.tar.gz` | x86_64 の Linux。静的リンクのため libc に依存しません。 |
| `conductor-main-aarch64-unknown-linux-musl.tar.gz` | aarch64 の Linux。静的リンクのため libc に依存しません。 |
| `conductor-main-aarch64-apple-darwin.tar.gz` | Apple Silicon の macOS 11 以降。 |

各アーカイブには `conductor-main` バイナリ、`LICENSE`、`THIRD-PARTY-LICENSES.txt`、README、設定例の `dot.env` が入っています。同じリリースの `SHA256SUMS` で検証し、必要に応じて GitHub CLI でビルドの provenance を検証してください。

```text
sha256sum --ignore-missing -c SHA256SUMS
gh attestation verify conductor-main-<target>.tar.gz --repo jobworkerp-rs/jobworkerp-conductor
tar -xzf conductor-main-<target>.tar.gz
conductor-main-<target>/conductor-main --version
```

macOS では、取得した asset を `shasum` で検証します。

```text
grep ' conductor-main-aarch64-apple-darwin.tar.gz$' SHA256SUMS | shasum -a 256 -c
```

macOS のバイナリは ad-hoc 署名のみで公証していません。ブラウザで取得して Gatekeeper に実行を拒否された場合は、`curl` で取得するか、`xattr -d com.apple.quarantine` で quarantine 属性を外してください。

`dot.env` を実行ディレクトリに `.env` としてコピーし、内容を調整してください（[設定](#設定) を参照）。コンテナイメージも公開しています（[Docker](#docker) を参照）。

## 前提条件

ソースからビルドする場合に必要です。


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

# 起動中の conductor に jobworkerp server を登録し、その server に admin worker を登録。
# 何度実行しても同じ結果になります。
cargo run --bin conductor-main -- bootstrap \
  --jobworkerp-url http://localhost:9000 \
  --conductor-grpc-url http://localhost:9090

# バージョンを表示。
cargo run --bin conductor-main -- --version
```

`register-workers` は `conductor-admin` FunctionSet と static worker を作成します。これらの worker は jobworkerp の GRPC runner 経由で conductor の gRPC admin API を呼び出します。worker 名は `conductor_<service>_<operation>` の形式です（例: `conductor_cron-scheduler_create`）。各 worker は、呼び出す RPC に合った GRPC runner の method（FindList は `streaming`、それ以外は `unary`）で FunctionSet に入るので、LLM や MCP のクライアントには worker ごとに 1 つの tool として見えます。`register-workers` は、1 件でも worker を登録できれば成功します。`register-workers` か `bootstrap` を再実行すると、FunctionSet の target は現在の定義で書き直されます。

`bootstrap` は、jobworkerp server と並べて動かす conductor を使える状態にします。

1. conductor の admin API と jobworkerp の両方が接続を受け付けるまで待ちます（それぞれ `--wait-timeout-sec`。既定は 30 秒で、`0` なら 1 回だけ試します）。接続の試行中だった場合は、最大 1 秒長くかかることがあります。
2. `--jobworkerp-url` を指す JobworkerpServer を、`--server-name`（既定は `local`）の名前で作成します。既にある場合は次のように扱います。
   - 以前に `bootstrap` が作成したレコードは、URL が変わると更新します。
   - `bootstrap` は自分が作ったレコードを description で見分けます。それ以外の方法で作成されたレコードや、description を編集したレコードは変更しません。接続先が違う場合は警告を出しますが、結果は `exists` で、`bootstrap` は成功します。
   - 無効化されたレコードは無効のまま残し、警告を出します。
3. `register-workers` と同じ worker と FunctionSet を登録します。1 件でも登録できなければ失敗します。

結果として `JobworkerpServer <name>: created|updated|exists (id=<id>)` を出力します。登録済みの worker は作成時の conductor の URL を使い続けるため、conductor のアドレスを変えた場合は `unregister-workers` を実行してから `bootstrap` を実行してください。

`conductor-main` は SIGINT と SIGTERM のどちらを受けても、正常に終了処理をします。

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

cron の `job_timeout_sec` は秒単位です。省略時は1200秒、`0` は無制限です。DB設定をTOMLでバックアップ・復元する場合もこの値が保持されます。この設定はlegacy TOML実行モードには適用されません。

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

### 設定のバックアップと復元（TOML）

`ConfigManagementService` は、全設定（サーバー、スケジュール、ジョブ結果ハンドラー、Slack イベントハンドラー。説明と有効状態を含む）を 1 つの TOML ファイルとして書き出し、取り込めます。

```bash
grpcurl -plaintext -d '{"enabled_only": false}' 127.0.0.1:9090 \
  jobworkerp_conductor.service.ConfigManagementService/ExportToml \
  | jq -r .tomlContent > settings.toml

# 検証だけ: 取り込みを実行してロールバックし、種類ごとの件数と、
# 取り込めない項目をすべて返す。
grpcurl -plaintext -d "$(jq -n --rawfile t settings.toml \
  '{toml_content: $t, overwrite_existing: true}')" 127.0.0.1:9090 \
  jobworkerp_conductor.service.ConfigManagementService/ValidateConfig
```

- `ImportToml` で `overwrite_existing: true` を指定すると、既存の設定をすべて削除してから取り込みます。指定しない場合、名前とアドレスが同じサーバーは既存のものを使い、それ以外の名前の重複はエラーになります。
- 取り込みは全件成功か、何も書き込まないかのどちらかです。取り込めない項目があると、応答の `validation_errors` にすべての問題を返し、`imported_*` の件数は 0 になります。`validate_only: true`（と `ValidateConfig`）は取り込みを実行してロールバックします。実行中は書き込みのロックを取るため、他の書き込みは少し待たされます。
- 取り込んだ設定には新しい ID が付くため、それまでの実行履歴とはつながらなくなります。`ClearAllConfigs` は実行履歴を残します。
- 取り込みや `ClearAllConfigs` の後は、個別の変更と同じく動作中のスケジューラーやハンドラーに通知されます。再起動は不要です。
- `enabled_only: true` は有効な設定と、それらが使うサーバー（無効でも）を書き出します。そのまま取り込めるようにするためです。
- 使うサーバーが削除された設定は取り込み直せないため、書き出しから外し、応答の `left_out` に並べます。
- レガシーの TOML モードも同じファイルを読みますが、無効の項目は読み飛ばし、`slack_handlers` は扱いません。

## Slack 設定

1. Slack App を作成します。
2. Socket Mode を有効化します。
3. App-Level Token を作成し、`SLACK_APP_TOKEN` に設定します。
4. 必要な event を subscribe します。例: `message.channels`、`app_mention`、`reaction_added`、`reaction_removed`。
5. event に対応する OAuth scope を追加します。例: `channels:history`、`app_mentions:read`、`reactions:read`。
6. App を workspace に install します。

`SLACK_APP_TOKEN` が未設定または空の場合、conductor は Slack Socket Mode listener なしで起動します。

## データベース

SQLite は開発用途と単一プロセス運用のデフォルト選択です。SQLite の schema は `api/infra/sql/sqlite/` の migration としてバイナリに埋め込まれ、起動時に未適用分が自動適用されます。適用が必要な既存データベースは、適用前に `<DBファイル>.pre-migration.bak` へ1世代バックアップされます。

MySQL の既存データベースの schema 更新は起動時には適用されません。適用前にバックアップを取り、未適用の migration を確認してから、[DB migration手順](api/infra/sql/migration/README.md)に従ってください。

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

各リリースでは、リリースバイナリから作ったマルチプラットフォームイメージ（`linux/amd64`、`linux/arm64`）も公開しています。

```bash
docker pull ghcr.io/jobworkerp-rs/jobworkerp-conductor:latest
gh attestation verify oci://ghcr.io/jobworkerp-rs/jobworkerp-conductor:latest \
  --repo jobworkerp-rs/jobworkerp-conductor
```

イメージは `/home/conductor` でユーザー `conductor` として `conductor-main` を実行します。設定は環境変数で渡すか、`.env` をそこへマウントしてください。

ローカルでイメージを作る場合、`conductor-main/Dockerfile` はリリースアーカイブの中身を `dist/linux-<arch>/`（`<arch>` は `amd64` または `arm64`）に置く前提です。

```bash
mkdir -p dist/linux-amd64
tar -xzf conductor-main-x86_64-unknown-linux-musl.tar.gz -C dist/linux-amd64 --strip-components=1
docker buildx build --platform linux/amd64 -f conductor-main/Dockerfile -t jobworkerp-conductor .
```

ソースからビルドしたバイナリを使う場合は、`target/x86_64-unknown-linux-musl/release/conductor-main`、`LICENSE`、`scripts/generate-third-party-licenses.sh` で生成した `THIRD-PARTY-LICENSES.txt` を `dist/linux-amd64/` に置いてください。

### all-in-one イメージ

`ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one` は、conductor と [jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs) の `all-in-one` サーバー（SQLite、standalone）を 1 つのコンテナで動かすイメージです。ローカルでの利用や小規模な運用を想定しています。jobworkerp を別に動かす k8s などの構成では、引き続き上記のイメージを使ってください。各リリースで `linux/amd64` と `linux/arm64` の `vX.Y.Z` と `latest` を公開します。同梱した版は label（`io.jobworkerp.all-in-one.jobworkerp-version`、`...tini-version`、`...protoc-version`）で確認できます。

```bash
docker pull ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one:latest
gh attestation verify oci://ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one:latest \
  --repo jobworkerp-rs/jobworkerp-conductor
docker run -d --name conductor -p 127.0.0.1:9090:9090 \
  -v conductor-data:/home/conductor --stop-timeout 60 \
  ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one:latest
```

[`conductor-main/all-in-one/compose.yaml`](conductor-main/all-in-one/compose.yaml) は Compose の例です。公開するアドレスと jobworkerp の bind 先を、起動時の変数で切り替えられます。

起動時、entrypoint は jobworkerp、conductor の順に起動し、続いて `conductor-main bootstrap` を実行します（jobworkerp server を `local` として登録し、admin worker を登録します）。どちらかのサーバーが終了するとコンテナも終了するので、再起動はコンテナの実行環境に任せてください（`restart: unless-stopped`）。`docker stop` では conductor、jobworkerp の順に止め、それぞれ `ALL_IN_ONE_STOP_TIMEOUT_SEC`（20 秒）まで待ちます。Docker の既定の猶予は 10 秒なので、60 秒にしてください（`--stop-timeout 60` / `stop_grace_period: 60s`）。

2 つのサーバーは同じ名前の環境変数を読むので、次のように分けて渡します。

| 変数 | 渡る先 |
|---|---|
| 接頭辞なし（`GRPC_ADDR`、`SLACK_APP_TOKEN` など） | conductor だけ |
| `JW_<NAME>`（`JW_OPENAI_API_KEY` など） | jobworkerp だけ（`<NAME>` として） |
| `ALL_IN_ONE_*` | entrypoint |
| `PATH`、`HOME`、`TZ`、`LANG`、`LC_*`、`HTTP(S)_PROXY`、`NO_PROXY`（大文字・小文字とも）、`SSL_CERT_FILE`、`SSL_CERT_DIR` | 両方 |

値が空の変数は、未設定として扱います。各サーバーは自分の `.env` も読みます（conductor は `/home/conductor/.env`、jobworkerp は `/home/conductor/jobworkerp/.env`）。環境変数は `.env` より優先されるので、次の既定値は環境変数でしか変えられません。

| サーバー | 既定値 |
|---|---|
| conductor | `GRPC_ADDR=0.0.0.0:9090`、`SQLITE_URL=sqlite:///home/conductor/conductor.sqlite3`、`NOTIFICATION_TYPE=channel`、`MEMORY_CACHE_*`、`RUST_LOG=info`、`LOG_USE_STDOUT=true`、`LOG_USE_JSON=false`、`LOG_FILE_DIR=/tmp/conductor-log`。`JOBWORKERP_URL` と `CONDUCTOR_GRPC_URL` は 2 つの `GRPC_ADDR` から決まります |
| jobworkerp | `STORAGE_TYPE=Standalone`、`GRPC_ADDR=127.0.0.1:9000`、`SQLITE_URL=sqlite:///home/conductor/jobworkerp/jobworkerp.sqlite3`、`PLUGINS_RUNNER_DIR` と `LD_LIBRARY_PATH` は `/home/conductor/plugins`、`RUST_LOG=info`、`LOG_*` は conductor と同様（`/tmp/jobworkerp-log`） |

| entrypoint の設定 | 既定値 | |
|---|---|---|
| `ALL_IN_ONE_BOOTSTRAP` | `true` | 両方のサーバーの起動後に `bootstrap` を実行する |
| `ALL_IN_ONE_STARTUP_TIMEOUT_SEC` | `60` | 各サーバーの port が開くまで待つ秒数 |
| `ALL_IN_ONE_STOP_TIMEOUT_SEC` | `20` | 停止時、各サーバーを強制終了するまで待つ秒数 |
| `ALL_IN_ONE_JOBWORKERP_SERVER_NAME` | `local` | `bootstrap` が管理する JobworkerpServer の名前 |

jobworkerp はコンテナの中のループバックだけで待ち受けるので、port 9000 を公開するだけでは外から届きません。外に出すときは `JW_GRPC_ADDR=0.0.0.0:9000` も指定してください。**conductor の管理 API には認証がなく、到達できれば誰でも jobworkerp にワークフローを実行させられます。** 手前のネットワークで接続元を制限していない限り、上の例のように `127.0.0.1` だけに公開してください。

その他の注意:

- `docker run IMAGE CMD...` は、環境変数を加工せずに `CMD` を実行します（例: `conductor-main register-workers ...`）。
- jobworkerp は、`JW_WORKER_DEFAULT_CONCURRENCY`、`JW_WORKER_CHANNELS`、`JW_WORKER_CHANNEL_CONCURRENCIES` の 3 つがすべて揃っていないと、まとめて無視します。`JW_JOB_QUEUE_EXPIRE_JOB_RESULT_SECONDS` と `JW_JOB_QUEUE_FETCH_INTERVAL` も同様です。`/home/conductor/jobworkerp` に `mcp-settings.toml` を置くと、jobworkerp は起動時にそこに書かれた MCP サーバーに接続し、失敗すると起動しません。
- plugin は `/home/conductor/plugins` に置きます。イメージの arch 向けで、glibc 2.41 以下でビルドした Linux の共有ライブラリが必要です。
- ジョブの実行に使うツール（`uv`、Docker CLI、Node.js など）は入っていません。必要な場合は `FROM ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one` で拡張してください。
- volume には uid 100 が書き込める必要があります（conductor 単体のイメージと同じなので、volume を移し替えられます）。named volume を推奨します。
- Mac では、Docker Desktop・OrbStack・Colima・Podman のいずれでも、Apple Silicon で `linux/arm64` のイメージがネイティブに動きます。ホスト上のサービス（Ollama など）には `host.docker.internal` で接続します（Linux では `--add-host host.docker.internal:host-gateway` を付けます）。

ローカルでイメージを作る場合は、conductor のリリースアーカイブの中身を `dist/all-in-one/linux-<arch>/conductor/` に置き、固定した外部入力（jobworkerp、tini、protoc）を `.../vendor/` に取得します。

```bash
mkdir -p dist/all-in-one/linux-amd64/conductor
tar -xzf conductor-main-x86_64-unknown-linux-musl.tar.gz \
  -C dist/all-in-one/linux-amd64/conductor --strip-components=1
bash scripts/fetch-all-in-one-inputs.sh amd64 dist/all-in-one/linux-amd64/vendor
# The version labels come from build arguments.
docker buildx build --platform linux/amd64 -f conductor-main/Dockerfile.all-in-one \
  $(sed 's/^/--build-arg /' dist/all-in-one/linux-amd64/vendor/versions.env) \
  --build-arg CONDUCTOR_VERSION=X.Y.Z -t jobworkerp-conductor-all-in-one --load .
```

ソースからビルドした `conductor-main` も使えますが、Debian trixie（glibc 2.41）で動く必要があります。上記の musl ターゲットでビルドするか、`rust:1-trixie` のコンテナの中でビルドしてください。

## ライセンス

Apache-2.0。詳細は [LICENSE](LICENSE) を参照してください。

リリースバイナリとイメージはサードパーティの crate を静的リンクしています。それらのライセンスは `THIRD-PARTY-LICENSES.txt` に一覧があり、各アーカイブと、イメージ内の `/usr/share/doc/jobworkerp-conductor/` に同梱しています。all-in-one イメージは、同梱したソフトウェアのライセンスも `/usr/share/doc/jobworkerp-rs/`（jobworkerp の `THIRD-PARTY-LICENSES.txt` を含む）、`/usr/share/doc/tini/`、`/usr/share/doc/protobuf/` に置いています。
