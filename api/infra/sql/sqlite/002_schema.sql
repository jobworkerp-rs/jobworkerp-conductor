-- Explicit types (e.g. BIGINT) are for Prisma compatibility, not SQLite enforcement
CREATE TABLE IF NOT EXISTS `jobworkerp_server` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `name` TEXT NOT NULL,
    `host` TEXT NOT NULL,
    `port` TEXT NOT NULL,
    `ssl_enabled` TINYINT UNSIGNED NOT NULL,
    `description` TEXT,
    `enabled` TINYINT UNSIGNED NOT NULL,
    `created_at` BIGINT,
    `updated_at` BIGINT
);

CREATE TABLE IF NOT EXISTS `cron_scheduler` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `name` TEXT NOT NULL,
    `jobworkerp_server_id` BIGINT NOT NULL,
    `workflow_url` TEXT NOT NULL,
    `channel` TEXT,
    `crontab` TEXT NOT NULL,
    `enabled` TINYINT UNSIGNED NOT NULL,
    `description` TEXT,
    `args` TEXT DEFAULT NULL,
    `worker_name` TEXT DEFAULT NULL,
    `using` TEXT DEFAULT NULL,
    `created_at` BIGINT,
    `updated_at` BIGINT
);

CREATE TABLE IF NOT EXISTS `worker_result_handler` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `name` TEXT NOT NULL,
    `listen_jobworkerp_server_id` BIGINT NOT NULL,
    `listen_worker_name` TEXT NOT NULL,
    `process_jobworkerp_server_id` BIGINT NOT NULL,
    `workflow_url` TEXT NOT NULL,
    `channel` TEXT,
    `enabled` TINYINT UNSIGNED NOT NULL,
    `description` TEXT,
    `args` TEXT DEFAULT NULL,
    `worker_name` TEXT DEFAULT NULL,
    `using` TEXT DEFAULT NULL,
    `created_at` BIGINT,
    `updated_at` BIGINT
);

-- UNIQUE constraints (prevent name duplication)
CREATE UNIQUE INDEX IF NOT EXISTS `uk_jobworkerp_server_name` ON `jobworkerp_server`(`name`);
CREATE UNIQUE INDEX IF NOT EXISTS `uk_cron_scheduler_name` ON `cron_scheduler`(`name`);
CREATE UNIQUE INDEX IF NOT EXISTS `uk_worker_result_handler_name` ON `worker_result_handler`(`name`);

-- jobworkerp_server indexes
CREATE INDEX IF NOT EXISTS `idx_jobworkerp_server_enabled` ON `jobworkerp_server`(`enabled`);
CREATE INDEX IF NOT EXISTS `idx_jobworkerp_server_created_at` ON `jobworkerp_server`(`created_at`);
CREATE INDEX IF NOT EXISTS `idx_jobworkerp_server_updated_at` ON `jobworkerp_server`(`updated_at`);

-- cron_scheduler indexes
CREATE INDEX IF NOT EXISTS `idx_cron_scheduler_enabled` ON `cron_scheduler`(`enabled`);
CREATE INDEX IF NOT EXISTS `idx_cron_scheduler_jobworkerp_server_id` ON `cron_scheduler`(`jobworkerp_server_id`);
CREATE INDEX IF NOT EXISTS `idx_cron_scheduler_created_at` ON `cron_scheduler`(`created_at`);
CREATE INDEX IF NOT EXISTS `idx_cron_scheduler_updated_at` ON `cron_scheduler`(`updated_at`);

-- worker_result_handler indexes
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_enabled` ON `worker_result_handler`(`enabled`);
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_listen_server_id` ON `worker_result_handler`(`listen_jobworkerp_server_id`);
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_process_server_id` ON `worker_result_handler`(`process_jobworkerp_server_id`);
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_listen_worker_name` ON `worker_result_handler`(`listen_worker_name`);
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_created_at` ON `worker_result_handler`(`created_at`);
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_updated_at` ON `worker_result_handler`(`updated_at`);

-- Composite indexes for frequent query patterns (enabled + created_at)
CREATE INDEX IF NOT EXISTS `idx_jobworker_server_enabled_created` ON `jobworkerp_server`(`enabled`, `created_at`);
CREATE INDEX IF NOT EXISTS `idx_cron_scheduler_enabled_created` ON `cron_scheduler`(`enabled`, `created_at`);
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_enabled_created` ON `worker_result_handler`(`enabled`, `created_at`);

-- Composite indexes for JOIN optimization (config_management export)
CREATE INDEX IF NOT EXISTS `idx_cron_scheduler_server_enabled` ON `cron_scheduler`(`jobworkerp_server_id`, `enabled`);
CREATE INDEX IF NOT EXISTS `idx_worker_result_handler_servers_enabled` ON `worker_result_handler`(`listen_jobworkerp_server_id`, `process_jobworkerp_server_id`, `enabled`);


-- Slack event handler table
-- Dynamically handles Slack events (message, app_mention, reaction_added/removed) and executes workflows
CREATE TABLE IF NOT EXISTS `slack_event_handler` (
    -- Basic information
    `id` BIGINT NOT NULL PRIMARY KEY,
    `name` TEXT NOT NULL,
    `description` TEXT,
    `enabled` TINYINT UNSIGNED NOT NULL DEFAULT 1,

    -- Common event conditions
    `slack_channel_id` TEXT,

    -- Message event conditions
    `message_pattern` TEXT,
    `mention_required` TINYINT UNSIGNED DEFAULT 0,

    -- Reaction event conditions
    `reaction_names` TEXT,
    `reaction_operation` TEXT,
    `reaction_user_filter` TEXT,

    -- Workflow execution settings
    `jobworkerp_server_id` BIGINT NOT NULL,
    `workflow_url` TEXT NOT NULL,
    `channel` TEXT,
    `timeout_sec` INTEGER NOT NULL DEFAULT 3600,
    `args` TEXT,
    `worker_name` TEXT DEFAULT NULL,
    `using` TEXT DEFAULT NULL,

    -- Metadata
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL
);

-- UNIQUE constraint (prevent name duplication)
CREATE UNIQUE INDEX IF NOT EXISTS `uk_slack_event_handler_name`
    ON `slack_event_handler`(`name`);

-- Performance indexes
CREATE INDEX IF NOT EXISTS `idx_slack_event_handler_enabled`
    ON `slack_event_handler`(`enabled`);

CREATE INDEX IF NOT EXISTS `idx_slack_event_handler_jobworkerp_server_id`
    ON `slack_event_handler`(`jobworkerp_server_id`);

CREATE INDEX IF NOT EXISTS `idx_slack_event_handler_created_at`
    ON `slack_event_handler`(`created_at`);

CREATE INDEX IF NOT EXISTS `idx_slack_event_handler_updated_at`
    ON `slack_event_handler`(`updated_at`);

-- Composite indexes for frequent queries
CREATE INDEX IF NOT EXISTS `idx_slack_event_handler_enabled_created`
    ON `slack_event_handler`(`enabled`, `created_at`);

CREATE INDEX IF NOT EXISTS `idx_slack_event_handler_server_enabled`
    ON `slack_event_handler`(`jobworkerp_server_id`, `enabled`);

-- Channel-based filtering optimization
CREATE INDEX IF NOT EXISTS `idx_slack_event_handler_channel_enabled`
    ON `slack_event_handler`(`slack_channel_id`, `enabled`);

-- Conductor-side execution references for jobworkerp jobs.
CREATE TABLE IF NOT EXISTS `execution_ref` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `source_type` INTEGER NOT NULL,
    `source_id` BIGINT NOT NULL,
    `source_name` TEXT NOT NULL,
    `jobworkerp_server_id` BIGINT NOT NULL,
    `job_id` BIGINT DEFAULT NULL,
    `triggered_at` BIGINT NOT NULL,
    `trigger_context_json` TEXT DEFAULT NULL,
    `enqueue_error` TEXT DEFAULT NULL,
    `created_at` BIGINT NOT NULL,
    -- Terminal jobworkerp ResultStatus observed at execution time; NULL when not recorded
    -- (enqueue failure, or rows created before this column existed).
    `result_status` INTEGER DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS `idx_execution_ref_source`
    ON `execution_ref`(`source_type`, `source_id`, `triggered_at`);

CREATE INDEX IF NOT EXISTS `idx_execution_ref_job`
    ON `execution_ref`(`jobworkerp_server_id`, `job_id`);

CREATE INDEX IF NOT EXISTS `idx_execution_ref_triggered_at`
    ON `execution_ref`(`triggered_at`);
