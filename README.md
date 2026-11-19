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

# Register a jobworkerp server on a running conductor and the admin workers on
# that server. Safe to run repeatedly.
cargo run --bin conductor-main -- bootstrap \
  --jobworkerp-url http://localhost:9000 \
  --conductor-grpc-url http://localhost:9090

# Print the version.
cargo run --bin conductor-main -- --version
```

`register-workers` creates a `conductor-admin` FunctionSet and static workers
that call the conductor gRPC admin API through jobworkerp's GRPC runner. The
workers are named `conductor_<service>_<operation>` (for example
`conductor_cron-scheduler_create`). Each worker joins the FunctionSet with the
GRPC runner method that matches its RPC (`streaming` for the FindList RPCs,
`unary` for the rest), so LLM and MCP clients see one tool per worker.
`register-workers` succeeds as long as at least one worker is registered.
Running `register-workers` or `bootstrap` again rewrites the FunctionSet
targets from the current definitions.

`bootstrap` prepares a conductor that runs next to a jobworkerp server:

1. It waits until both the conductor admin API and jobworkerp accept
   connections (`--wait-timeout-sec`, 30 seconds by default, for each; `0`
   tries once). A pending connection attempt may take up to 1 second longer.
2. It creates the JobworkerpServer named `--server-name` (`local` by default)
   for `--jobworkerp-url`, or keeps an existing one:
   - A record that `bootstrap` created earlier is updated when the URL changes.
   - `bootstrap` recognizes its records by their description. A record created
     by other means, or whose description was edited, is left unchanged; if it
     points elsewhere, `bootstrap` prints a warning, reports `exists` and still
     succeeds.
   - A disabled record stays disabled, with a warning.
3. It registers the same workers and FunctionSet as `register-workers`, and
   fails if any worker cannot be registered.

It prints `JobworkerpServer <name>: created|updated|exists (id=<id>)`.
Registered workers keep the conductor URL they were created with; after
changing the conductor address, run `unregister-workers` before `bootstrap`.

`conductor-main` shuts down gracefully on both SIGINT and SIGTERM.

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

### Backing up and restoring settings (TOML)

`ConfigManagementService` exports every setting (servers, cron schedulers,
result handlers and Slack event handlers, with their descriptions and enabled
state) as one TOML file, and imports such a file back:

```bash
grpcurl -plaintext -d '{"enabled_only": false}' 127.0.0.1:9090 \
  jobworkerp_conductor.service.ConfigManagementService/ExportToml \
  | jq -r .tomlContent > settings.toml

# Dry run: the import runs and is rolled back, returning the counts per kind
# and every entry that cannot be imported.
grpcurl -plaintext -d "$(jq -n --rawfile t settings.toml \
  '{toml_content: $t, overwrite_existing: true}')" 127.0.0.1:9090 \
  jobworkerp_conductor.service.ConfigManagementService/ValidateConfig
```

- `ImportToml` with `overwrite_existing: true` deletes every setting first and
  then imports. Without it, a server whose name and address match a stored one
  is reused and any other name clash is an error.
- An import writes everything or nothing. When any entry fails, the response
  lists every problem in `validation_errors` and its `imported_*` counts are 0.
  `validate_only: true` (and `ValidateConfig`) run it and roll back. While it
  runs it holds the write lock, so other writes wait briefly.
- Imported settings get new ids, so execution history recorded before the
  import no longer links to them. `ClearAllConfigs` keeps execution history.
- After an import or `ClearAllConfigs`, the running schedulers and handlers are
  notified as for individual changes; no restart is needed.
- `enabled_only: true` exports enabled settings, plus the servers they run on
  even when those are disabled, so the file imports as it is.
- An entry whose server has been deleted cannot be imported again, so the
  export leaves it out and lists it in `left_out`.
- The legacy TOML mode reads the same file but skips disabled entries and
  ignores `slack_handlers`.

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

### All-in-one image

`ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one` runs conductor and a
[jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs) `all-in-one`
server (SQLite, standalone) in one container, for local use and small
deployments. The image above stays the one for Kubernetes and other setups
where jobworkerp runs separately. Each release publishes `vX.Y.Z` and
`latest` for `linux/amd64` and `linux/arm64`; the labels
`io.jobworkerp.all-in-one.jobworkerp-version`, `...tini-version` and
`...protoc-version` name the bundled versions.

```bash
docker pull ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one:latest
gh attestation verify oci://ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one:latest \
  --repo jobworkerp-rs/jobworkerp-conductor
docker run -d --name conductor -p 127.0.0.1:9090:9090 \
  -v conductor-data:/home/conductor --stop-timeout 60 \
  ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one:latest
```

[`conductor-main/all-in-one/compose.yaml`](conductor-main/all-in-one/compose.yaml)
is a Compose example whose published addresses and jobworkerp bind address
are set from variables at startup.

On start, the entrypoint runs jobworkerp, then conductor, then
`conductor-main bootstrap` (registering the jobworkerp server as `local` and
the admin workers). When either server exits, the container stops; leave
restarting to the runtime (`restart: unless-stopped`). On `docker stop` it
stops conductor first and then jobworkerp, waiting up to
`ALL_IN_ONE_STOP_TIMEOUT_SEC` (20 s) for each, so give the container a grace
period of 60 s (`--stop-timeout 60` / `stop_grace_period: 60s`); Docker's
default is 10 s.

Both servers read variables of the same names, so the environment is split:

| Variable | Goes to |
|---|---|
| Unprefixed (`GRPC_ADDR`, `SLACK_APP_TOKEN`, ...) | conductor only |
| `JW_<NAME>` (`JW_OPENAI_API_KEY`, ...) | jobworkerp only, as `<NAME>` |
| `ALL_IN_ONE_*` | the entrypoint |
| `PATH`, `HOME`, `TZ`, `LANG`, `LC_*`, `HTTP(S)_PROXY`, `NO_PROXY` (either case), `SSL_CERT_FILE`, `SSL_CERT_DIR` | both |

Empty variables count as unset. Each server also reads its own `.env`:
`/home/conductor/.env` for conductor and `/home/conductor/jobworkerp/.env` for
jobworkerp. Environment variables take precedence over `.env`, so the defaults
below can be changed only through the environment.

| Server | Defaults |
|---|---|
| conductor | `GRPC_ADDR=0.0.0.0:9090`, `SQLITE_URL=sqlite:///home/conductor/conductor.sqlite3`, `NOTIFICATION_TYPE=channel`, `MEMORY_CACHE_*`, `RUST_LOG=info`, `LOG_USE_STDOUT=true`, `LOG_USE_JSON=false`, `LOG_FILE_DIR=/tmp/conductor-log`; `JOBWORKERP_URL` and `CONDUCTOR_GRPC_URL` follow the two `GRPC_ADDR`s |
| jobworkerp | `STORAGE_TYPE=Standalone`, `GRPC_ADDR=127.0.0.1:9000`, `SQLITE_URL=sqlite:///home/conductor/jobworkerp/jobworkerp.sqlite3`, `PLUGINS_RUNNER_DIR` and `LD_LIBRARY_PATH` = `/home/conductor/plugins`, `RUST_LOG=info`, `LOG_*` as for conductor (`/tmp/jobworkerp-log`) |

| Entrypoint setting | Default | |
|---|---|---|
| `ALL_IN_ONE_BOOTSTRAP` | `true` | run `bootstrap` after both servers are up |
| `ALL_IN_ONE_STARTUP_TIMEOUT_SEC` | `60` | wait for each server's port |
| `ALL_IN_ONE_STOP_TIMEOUT_SEC` | `20` | wait for each server to stop before killing it |
| `ALL_IN_ONE_JOBWORKERP_SERVER_NAME` | `local` | JobworkerpServer name that `bootstrap` manages |

jobworkerp listens on loopback inside the container, so publishing port 9000
alone does not expose it; set `JW_GRPC_ADDR=0.0.0.0:9000` as well. **The
conductor admin API has no authentication, and anyone who can reach it can
make jobworkerp run workflows.** Publish it on `127.0.0.1` as above unless the
network in front of it is restricted.

Other notes:

- `docker run IMAGE CMD...` runs `CMD` with the environment as given, e.g.
  `conductor-main register-workers ...`.
- jobworkerp ignores `JW_WORKER_DEFAULT_CONCURRENCY`, `JW_WORKER_CHANNELS` and
  `JW_WORKER_CHANNEL_CONCURRENCIES` unless all three are set, and likewise
  `JW_JOB_QUEUE_EXPIRE_JOB_RESULT_SECONDS` and `JW_JOB_QUEUE_FETCH_INTERVAL`.
  An `mcp-settings.toml` in `/home/conductor/jobworkerp` makes jobworkerp
  connect to the MCP servers it lists at startup, and a failing one stops it.
- Plugins go in `/home/conductor/plugins` and must be Linux shared objects for
  the image's architecture built against glibc 2.41 or older.
- The image contains no tools for jobs such as `uv`, the Docker CLI or Node.js.
  Build on it with `FROM ghcr.io/jobworkerp-rs/jobworkerp-conductor-all-in-one`
  to add them.
- The volume must be writable by uid 100 (as in the conductor-only image, so
  a volume can move between the two). Prefer named volumes.
- On a Mac, Docker Desktop, OrbStack, Colima and Podman run the `linux/arm64`
  image natively on Apple Silicon. Reach services on the host, such as
  Ollama, at `host.docker.internal` (on Linux, add
  `--add-host host.docker.internal:host-gateway`).

To build the image locally, put a conductor release archive's contents in
`dist/all-in-one/linux-<arch>/conductor/` and fetch the pinned third-party
inputs (jobworkerp, tini, protoc) into `.../vendor/`:

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

A `conductor-main` built from source works too if it runs on Debian trixie
(glibc 2.41): build the musl target as above, or build inside a
`rust:1-trixie` container.

## License

Apache-2.0. See [LICENSE](LICENSE).

Release binaries and images statically link third-party crates; their
licenses are listed in `THIRD-PARTY-LICENSES.txt`, shipped in each archive and
in the image under `/usr/share/doc/jobworkerp-conductor/`.
The all-in-one image also ships the licenses of the bundled software under
`/usr/share/doc/jobworkerp-rs/` (including its `THIRD-PARTY-LICENSES.txt`),
`/usr/share/doc/tini/` and `/usr/share/doc/protobuf/`.
