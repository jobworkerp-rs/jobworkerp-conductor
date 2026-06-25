# Jobworkerp Conductor

Jobworkerp Conductor connects operational events to
[jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs) executions. It can
run a workflow file or a registered worker when a cron schedule fires, a Slack
event matches configured conditions, or a selected jobworkerp worker completes.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](Cargo.toml)

Japanese: [README_ja.md](README_ja.md)

## Overview

- **Cron schedules** run workflow or worker executions on a configured schedule.
- **Slack handlers** match Socket Mode events such as messages, app mentions,
  and reactions.
- **Worker result handlers** listen for selected jobworkerp worker results and
  run follow-up work.
- **gRPC admin API** manages jobworkerp servers, handlers, schedulers, and
  execution status.
- **Database-backed configuration** uses SQLite for local/single-process setups
  and can be built for MySQL.

## Repository Layout

```text
conductor-main/         Main binary and CLI
jobworkerp-conductor/   Initialization layer and dynamic handler managers
slack-event-handler/    Slack Socket Mode matching and execution
jobworkerp-handler/     Worker result listener
api/app/                Application services
api/infra/              Repository implementations and database setup
api/grpc-admin/         gRPC admin server
proto/                  Protocol Buffers definitions
shared/                 Shared execution, validation, and notification code
modules/                Reusable support crates
docker/                 Container assets
```

## Requirements

- Rust stable toolchain
- `protoc`
- Linux packages: `pkg-config`, `protobuf-compiler`
- SQLite for the default local setup
- Optional: Redis for cross-process configuration notifications
- Optional: Slack App with Socket Mode enabled

On Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y pkg-config protobuf-compiler
```

## Quick Start

Build the workspace:

```bash
cargo build
```

Start the conductor with a local SQLite database and a plaintext gRPC admin API
on `127.0.0.1:9090`:

```bash
export GRPC_ADDR=127.0.0.1:9090
export SQLITE_URL=sqlite://conductor.sqlite3
export SQLITE_MAX_CONNECTIONS=20
export NOTIFICATION_TYPE=channel

cargo run --bin conductor-main
```

The process loads `.env` before parsing CLI arguments, so the same values can be
placed in a local `.env` file.

## CLI

```bash
# Start DB-driven mode. This is the default.
cargo run --bin conductor-main
cargo run --bin conductor-main -- serve

# Start legacy TOML-driven mode.
cargo run --bin conductor-main -- serve \
  --legacy-mode \
  --file-settings-toml ./workflows.toml

# Register/unregister conductor admin workers on a jobworkerp server.
cargo run --bin conductor-main -- register-workers \
  --jobworkerp-url http://localhost:9000 \
  --conductor-grpc-url http://localhost:9090

cargo run --bin conductor-main -- unregister-workers \
  --jobworkerp-url http://localhost:9000
```

`register-workers` creates a `conductor-admin` FunctionSet and static workers
that call the conductor gRPC admin API through jobworkerp's GRPC runner.

## Configuration

### Core Environment Variables

| Variable | Required | Description |
| --- | --- | --- |
| `GRPC_ADDR` | yes | gRPC admin listen address, for example `127.0.0.1:9090` or `0.0.0.0:9100`. |
| `USE_GRPC_WEB` | no | Enable gRPC-Web support when `true`. Default: `false`. |
| `MAX_FRAME_SIZE` | no | Optional tonic max frame size in bytes. |
| `SQLITE_URL` | SQLite | SQLite URL, for example `sqlite://conductor.sqlite3`. |
| `SQLITE_MAX_CONNECTIONS` | SQLite | SQLite pool size. |
| `MYSQL_URL` | MySQL | MySQL URL, for example `mysql://user:pass@localhost:3306/conductor`. |
| `MYSQL_MAX_CONNECTIONS` | MySQL | MySQL pool size. |
| `NOTIFICATION_TYPE` | no | `channel` for single-process deployments, `redis` for cross-process config notifications. Default: `channel`. |
| `NOTIFICATION_BUFFER_SIZE` | no | Buffer size for `NOTIFICATION_TYPE=channel`. Default: `1000`. |
| `NOTIFICATION_CHANNEL_PREFIX` | no | Redis Pub/Sub channel prefix. Default: `conductor_config`. |
| `REDIS_URL` | only for Redis notifications | Redis URL for `NOTIFICATION_TYPE=redis`. |
| `SLACK_APP_TOKEN` | only for Slack | Slack App-Level Token for Socket Mode, usually starting with `xapp-`. |
| `CONDUCTOR_CRON_TIMEZONE` | no | IANA time zone used to interpret cron schedules. Falls back to `TZ`, then UTC. |
| `JOBWORKERP_URL` | only for worker registration | jobworkerp endpoint used by `register-workers` and `unregister-workers`. |
| `CONDUCTOR_GRPC_URL` | no | Public conductor gRPC URL stored in registered admin workers. Default: `http://localhost:9090`. |

### Notification Mode

For a normal single conductor process, use:

```bash
NOTIFICATION_TYPE=channel
```

Redis notifications are useful when the management API process and executor
process are split and configuration changes must cross process boundaries. Use
a single executor process unless your deployment adds its own duplicate
prevention for cron, Slack, and worker-result handling.

## gRPC Admin API

The admin server enables tonic reflection, so `grpcurl` can discover services:

```bash
grpcurl -plaintext 127.0.0.1:9090 list
```

Create a jobworkerp server entry:

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

Create a cron scheduler. `tokio-cron-scheduler` accepts cron expressions with a
seconds field; the example below runs every hour at minute zero:

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

Create a Slack message handler:

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

List configured handlers:

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

The main service packages are under `jobworkerp_conductor.service.*`; see
`proto/protobuf/jobworkerp_conductor/` for the complete API.

## Slack Setup

1. Create a Slack App.
2. Enable Socket Mode.
3. Create an App-Level Token and set it as `SLACK_APP_TOKEN`.
4. Subscribe to the events you need, such as `message.channels`,
   `app_mention`, `reaction_added`, and `reaction_removed`.
5. Add OAuth scopes matching those events, for example `channels:history`,
   `app_mentions:read`, and `reactions:read`.
6. Install the app to your workspace.

If `SLACK_APP_TOKEN` is missing or empty, the conductor starts without the Slack
Socket Mode listener.

## Database Notes

SQLite is the default development and single-process deployment choice. The
SQLite schema is embedded and applied at startup.

For MySQL, initialize the schema before starting the conductor:

```bash
mysql -u root -p -e "CREATE DATABASE conductor;"
mysql -u root -p conductor < api/infra/sql/mysql/002_schema.sql
```

Then run a MySQL-enabled build with `MYSQL_URL` and `MYSQL_MAX_CONNECTIONS`.

## Development

```bash
cargo build
cargo test -- --test-threads=1
cargo fmt --check
cargo clippy
```

Target a single package when iterating:

```bash
cargo test -p conductor-main -- --test-threads=1
cargo test -p slack-event-handler -- --test-threads=1
```

Integration tests that require external services use environment variables such
as `TEST_REDIS_URL`, `TEST_MYSQL_URL`, `TEST_JOBWORKERP_HOST`, and
`TEST_WORKFLOW_URL`.

## Docker

`conductor-main/Dockerfile` packages a prebuilt
`target/x86_64-unknown-linux-musl/release/conductor-main` binary. Build the
musl binary before running `docker build`.

## License

Apache-2.0. See [Cargo.toml](Cargo.toml).
