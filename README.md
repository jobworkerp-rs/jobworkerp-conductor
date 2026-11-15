# Jobworkerp Conductor

Jobworkerp Conductor connects operational events to
[jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs) executions. It can
run a workflow file or a registered worker when a cron schedule fires, a Slack
event matches configured conditions, or a selected jobworkerp worker completes.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

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
```

## Install

Prebuilt binaries are attached to each [GitHub Release](https://github.com/jobworkerp-rs/jobworkerp-conductor/releases).

| Asset | Requirements |
|---|---|
| `conductor-main-x86_64-unknown-linux-musl.tar.gz` | Linux on x86_64. Statically linked; no libc requirement. |
| `conductor-main-aarch64-unknown-linux-musl.tar.gz` | Linux on aarch64. Statically linked; no libc requirement. |
| `conductor-main-aarch64-apple-darwin.tar.gz` | macOS 11 or later on Apple Silicon. |

Each archive contains the `conductor-main` binary, `LICENSE`,
`THIRD-PARTY-LICENSES.txt`, the READMEs, and `dot.env` as a configuration
example. Verify the download with `SHA256SUMS` from the same release, and
optionally verify its build provenance with the GitHub CLI:

```text
sha256sum --ignore-missing -c SHA256SUMS
gh attestation verify conductor-main-<target>.tar.gz --repo jobworkerp-rs/jobworkerp-conductor
tar -xzf conductor-main-<target>.tar.gz
conductor-main-<target>/conductor-main --version
```

On macOS, check the asset you downloaded with `shasum`:

```text
grep ' conductor-main-aarch64-apple-darwin.tar.gz$' SHA256SUMS | shasum -a 256 -c
```

The macOS binary is ad-hoc signed and not notarized. If a browser download is
blocked by Gatekeeper, download it with `curl` or remove the quarantine
attribute with `xattr -d com.apple.quarantine`.

Copy `dot.env` to `.env` next to where you run the binary and adjust it (see
[Configuration](#configuration)). A container image is also published; see
[Docker](#docker).

## Requirements

The following are needed to build from source.


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

# Print the version.
cargo run --bin conductor-main -- --version
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
SQLite schema is embedded as the migrations in `api/infra/sql/sqlite/`, and
pending ones are applied automatically at startup. An existing database that
needs changes is first copied to `<database file>.pre-migration.bak` (one
generation is kept).

Schema updates for an existing MySQL database are not applied at startup.
Back up the database and follow the
[DB migration procedure](api/infra/sql/migration/README.md).

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

Each release also publishes a multi-platform image (`linux/amd64`,
`linux/arm64`) built from the release binaries:

```bash
docker pull ghcr.io/jobworkerp-rs/jobworkerp-conductor:latest
gh attestation verify oci://ghcr.io/jobworkerp-rs/jobworkerp-conductor:latest \
  --repo jobworkerp-rs/jobworkerp-conductor
```

The image runs `conductor-main` as the user `conductor` in `/home/conductor`.
Pass the configuration as environment variables or mount a `.env` file there.

To build the image locally, `conductor-main/Dockerfile` expects the contents
of a release archive in `dist/linux-<arch>/` (`<arch>` is `amd64` or `arm64`):

```bash
mkdir -p dist/linux-amd64
tar -xzf conductor-main-x86_64-unknown-linux-musl.tar.gz -C dist/linux-amd64 --strip-components=1
docker buildx build --platform linux/amd64 -f conductor-main/Dockerfile -t jobworkerp-conductor .
```

A binary built from source can be used instead: put
`target/x86_64-unknown-linux-musl/release/conductor-main`, `LICENSE`, and a
`THIRD-PARTY-LICENSES.txt` generated by
`scripts/generate-third-party-licenses.sh` in `dist/linux-amd64/`.

## License

Apache-2.0. See [LICENSE](LICENSE).

Release binaries and images statically link third-party crates; their
licenses are listed in `THIRD-PARTY-LICENSES.txt`, shipped in each archive and
in the image under `/usr/share/doc/jobworkerp-conductor/`.
