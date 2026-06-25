# Jobworkerp Conductor

A powerful event-driven automation system for [jobworkerp](https://github.com/jobworkerp-rs/jobworkerp-rs), a distributed job worker platform. This system enables dynamic workflow execution triggered by Slack events, cron schedules, and job completion events.

[![Pull Request](https://github.com/jobworkerp-rs/jobworkerp-conductor/actions/workflows/pull-request.yml/badge.svg)](https://github.com/jobworkerp-rs/jobworkerp-conductor/actions/workflows/pull-request.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](Cargo.toml)

## Features

- **🔄 Dynamic Slack Event Handling**: Process Slack messages, mentions, and reactions in real-time via Socket Mode
- **⏰ Cron-based Scheduling**: Execute workflows on flexible cron schedules
- **📊 Job Result Listeners**: React to job completion events from jobworkerp servers
- **🔧 DB-driven Configuration**: Manage all configurations via database with immediate reflection (no restart required)
- **🚀 gRPC Management API**: Full-featured admin API for configuration management

## Quick Start

### Prerequisites

- Rust toolchain (latest stable)
- Protocol Buffers compiler (protoc)
- Persistence: SQLite (standard)
- Optional: MySQL (for an external database or sharing the configuration database across processes)
- Optional: Redis (for delivering configuration change notifications across process boundaries)
- For Slack integration: Slack App with Socket Mode enabled

### Installation

```bash
# Clone the repository
git clone https://github.com/jobworkerp-rs/jobworkerp-conductor.git
cd jobworkerp-conductor

# Install system dependencies (Ubuntu/Debian)
apt-get install -y libssl-dev pkg-config libudev-dev

# Build the project
cargo build --release
```

### Basic Usage

#### 1. Start the handler (DB-driven mode)

```bash
# With default SQLite database
cargo run --bin conductor-main

# With custom database URL
DATABASE_URL=sqlite://./my-handler.sqlite3 cargo run --bin conductor-main

# With MySQL
DATABASE_URL=mysql://user:password@localhost:3306/conductor cargo run --bin conductor-main
```

#### 2. Configure Slack integration (optional)

```bash
# Set Slack App Token for Socket Mode
export SLACK_APP_TOKEN=xapp-1-A0XXXXXXXXX-...

# Restart the handler to enable Slack event processing
cargo run --bin conductor-main
```

#### 3. Configure handlers via gRPC API

```bash
# Install grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Register a jobworkerp server
grpcurl -plaintext -d '{
  "name": "production-server",
  "host": "jobworkerp.example.com",
  "port": "9000",
  "ssl_enabled": true,
  "enabled": true
}' localhost:9090 jobworkerp_conductor.service.JobworkerpServerService/Create

# Register a Slack event handler
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

## Architecture

### Workspace Structure

```
jobworkerp-conductor/
├── conductor-main/        # Main application entry point
├── jobworkerp-conductor/       # Initialization layer and event handler manager
├── slack-event-handler/    # Slack event processing implementation
├── jobworkerp-handler/     # Legacy job result listener
├── modules/                # Common library modules
│   ├── jobworkerp-client/  # jobworkerp server communication
│   ├── command-utils/      # Common utilities
│   ├── infra-utils/        # Infrastructure utilities
│   ├── memory-utils/       # Cache, lock, channels
│   └── net-utils/          # Network utilities
├── api/                    # Clean architecture layers
│   ├── app/                # Application layer (services)
│   ├── infra/              # Infrastructure layer (repositories)
│   └── grpc-admin/         # gRPC admin API
├── proto/                  # Protocol Buffers definitions
└── shared/                 # Shared components
```

### Key Components

#### EventHandlerServerManager
Unified management of multiple event handlers (Cron, Slack, WorkerResult) with dynamic configuration reflection.

#### DynamicSlackHandlerManager
- Real-time Slack event reception using Socket Mode
- Automatic event type detection (message, app_mention, reaction_added/removed)
- Configuration changes reflected immediately without restart

#### NotificationService
Configuration change notification broadcast system supporting both in-memory channels and Redis Pub/Sub.

#### WorkflowExecutor
Common workflow execution implementation with gRPC communication to jobworkerp servers, error handling, and retry strategies.

## Configuration

For a normal single conductor process, SQLite for persistence and `NOTIFICATION_TYPE=channel` for configuration change notifications are recommended. MySQL and Redis are optional choices. Use MySQL when you need an external database or need to share the configuration database across processes. Use Redis when configuration change notifications must cross process boundaries. Enabling these options does not provide distributed execution locking for safely running multiple executor conductor processes in active-active mode.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Database connection URL | `sqlite://conductor.sqlite3` |
| `NOTIFICATION_TYPE` | Configuration change notification backend. Use `channel` for normal deployments. `redis` delivers configuration change notifications across process boundaries | `channel` |
| `NOTIFICATION_BUFFER_SIZE` | Notification buffer size for `NOTIFICATION_TYPE=channel` | `1000` |
| `REDIS_URL` | Redis connection URL for `NOTIFICATION_TYPE=redis`. Leave unset or empty when using `channel` | - |
| `SLACK_APP_TOKEN` | Slack App token for Socket Mode (required for Slack features) | - |

### Configuration Change Notifications

For a normal single conductor process, `NOTIFICATION_TYPE=channel` is recommended. Configuration change notifications are delivered through an in-process memory channel, Redis is not required, and `REDIS_URL` can remain unset or empty.

`NOTIFICATION_TYPE=redis` is intended for deployments that split the management API process from executor processes and need to deliver configuration change notifications across process boundaries. It is a notification path intended as a foundation for future HA/distributed deployments, but it does not imply distributed execution or duplicate prevention.

Redis notifications do not currently provide distributed execution locking. Running multiple executor conductor processes in active-active mode can duplicate processing for the same cron scheduler or Slack event. Even when Redis is enabled, active-active executor deployments are not recommended yet. Keep the executor role to a single process, or design separate distributed locks or job deduplication keys.

### Database Setup

#### SQLite (Default)
No manual setup required. Database file is created automatically on first run.

#### MySQL
```bash
# Create database
mysql -u root -p -e "CREATE DATABASE conductor;"

# Initialize schema
mysql -u root -p conductor < api/infra/sql/mysql/002_schema.sql

# Run with MySQL
DATABASE_URL=mysql://user:password@localhost:3306/conductor cargo run --bin conductor-main
```

### Slack App Setup

1. **Create Slack App** at https://api.slack.com/apps
2. **Enable Socket Mode** and generate App-Level Token (`SLACK_APP_TOKEN`)
3. **Configure Event Subscriptions**:
   - `message.channels`, `message.groups`, `message.im`, `message.mpim`
   - `app_mention`
   - `reaction_added`, `reaction_removed`
4. **Configure OAuth Scopes**:
   - `channels:history`, `groups:history`, `im:history`, `mpim:history`
   - `app_mentions:read`
   - `reactions:read`
5. **Install to Workspace**

## Usage Examples

### Managing Slack Event Handlers

#### Create a message pattern handler
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

#### Create a reaction-based handler
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

#### Create a bot mention handler
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

#### List all handlers
```bash
grpcurl -plaintext localhost:9090 \
  jobworkerp_conductor.service.SlackEventHandlerService/FindList
```

#### Update a handler
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

#### Delete a handler
```bash
grpcurl -plaintext -d '{"value": 1}' localhost:9090 \
  jobworkerp_conductor.service.SlackEventHandlerService/Delete
```

### Managing Cron Schedulers

```bash
# Create a cron scheduler
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

# List all schedulers
grpcurl -plaintext localhost:9090 \
  jobworkerp_conductor.service.CronSchedulerService/FindList
```

## Development

### Build and Test

```bash
# Build entire workspace
cargo build

# Run tests
cargo test

# Run tests with controlled parallelism (recommended for CI)
cargo test -- --test-threads=1

# Run integration tests
cargo test --test integration_tests -- --test-threads=1

# Code quality checks
cargo check
cargo clippy
cargo fmt
```

### Running in Debug Mode

```bash
# Enable debug logging
cargo run --bin conductor-main -- --debug
```

### Legacy TOML Mode

```bash
# Run with legacy TOML configuration
cargo run --bin conductor-main -- serve --legacy-mode --file-settings-toml ./workflows.toml
```

## Troubleshooting

### Slack events are not processed

1. Verify `SLACK_APP_TOKEN` is set correctly:
   ```bash
   echo $SLACK_APP_TOKEN  # Should start with xapp-
   ```

2. Check logs for Socket Mode connection:
   ```bash
   grep "Socket Mode listener started" conductor_*.log
   ```

3. Verify handler configuration:
   ```bash
   grpcurl -plaintext localhost:9090 \
     jobworkerp_conductor.service.SlackEventHandlerService/FindList
   ```

### Workflow execution fails

1. Verify jobworkerp server is running:
   ```bash
   curl http://localhost:9000/health
   ```

2. Check workflow URL accessibility:
   ```bash
   curl -I http://your-server/workflow.yaml
   ```

3. Review retry logs:
   ```bash
   grep "Workflow execution failed" conductor_*.log
   ```

### Configuration changes not reflected

1. Check NotificationService logs:
   ```bash
   grep "Configuration change notification" conductor_*.log | tail -n 10
   ```

2. Verify EventHandlerServerManager receives notifications:
   ```bash
   grep "Config changed event received" conductor_*.log | tail -n 10
   ```

3. Restart as last resort:
   ```bash
   # For Docker
   docker restart conductor-main

   # For systemd
   sudo systemctl restart conductor-main
   ```

## Monitoring

### Health Check

```bash
grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check
```

### Recommended Monitoring Items

1. **Process running status**
2. **Socket Mode connection status**
3. **Error rate** (threshold: >5% over 5 minutes)
4. **Workflow execution failures** (threshold: >10 per hour)
5. **Active handler count**

### Monitoring Commands

```bash
# Check process status
systemctl status conductor-main

# Monitor error rate
grep -c "ERROR" conductor_*.log

# Count active handlers
grpcurl -plaintext localhost:9090 \
  jobworkerp_conductor.service.SlackEventHandlerService/FindList | \
  jq -s '[.[] | select(.data.enabled)] | length'
```

## Backup and Restore

### Backup Configuration

```bash
# For SQLite
cp conductor.sqlite3 conductor.sqlite3.backup.$(date +%Y%m%d_%H%M%S)

# For MySQL
mysqldump -u user -p conductor_db > conductor-backup.sql
```

### Restore Configuration

```bash
# For SQLite
cp conductor.sqlite3.backup.20251003_120000 conductor.sqlite3
systemctl restart conductor-main

# For MySQL
mysql -u user -p conductor_db < conductor-backup.sql
systemctl restart conductor-main
```

## Docker Deployment

```bash
# Build Docker image
docker build -t conductor-main ./conductor-main

# Run with Docker
docker run -d \
  -e DATABASE_URL=sqlite://conductor.sqlite3 \
  -e SLACK_APP_TOKEN=xapp-1-... \
  -p 9090:9090 \
  -v $(pwd)/data:/data \
  conductor-main

# Build for production (musl target)
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

## License

This project is licensed under Apache-2.0 as declared in `Cargo.toml`.

## Related Projects

- [jobworkerp-rs](https://github.com/jobworkerp-rs/jobworkerp-rs) - Distributed job worker platform

## Support

For issues, questions, or contributions, please open an issue in this repository's issue tracker.
