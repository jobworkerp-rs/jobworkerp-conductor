CREATE TABLE IF NOT EXISTS `jobworkerp_server` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `name` TEXT NOT NULL,
    `host` TEXT NOT NULL,
    `port` TEXT NOT NULL,
    `ssl_enabled` TINYINT UNSIGNED NOT NULL,
    `description` TEXT,
    `enabled` TINYINT UNSIGNED NOT NULL,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    -- Index definitions
    UNIQUE KEY `uk_jobworkerp_server_name` (`name`(255)),
    INDEX `idx_jobworkerp_server_enabled` (`enabled`),
    INDEX `idx_jobworkerp_server_created_at` (`created_at`),
    INDEX `idx_jobworkerp_server_updated_at` (`updated_at`),
    INDEX `idx_jobworkerp_server_enabled_created` (`enabled`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    `worker_name` VARCHAR(255) DEFAULT NULL,
    `using` VARCHAR(255) DEFAULT NULL,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    -- Index definitions
    UNIQUE KEY `uk_worker_result_handler_name` (`name`(255)),
    INDEX `idx_worker_result_handler_enabled` (`enabled`),
    INDEX `idx_worker_result_handler_listen_server_id` (`listen_jobworkerp_server_id`),
    INDEX `idx_worker_result_handler_process_server_id` (`process_jobworkerp_server_id`),
    INDEX `idx_worker_result_handler_listen_worker_name` (`listen_worker_name`(255)),
    INDEX `idx_worker_result_handler_created_at` (`created_at`),
    INDEX `idx_worker_result_handler_updated_at` (`updated_at`),
    INDEX `idx_worker_result_handler_enabled_created` (`enabled`, `created_at`),
    INDEX `idx_worker_result_handler_servers_enabled` (`listen_jobworkerp_server_id`, `process_jobworkerp_server_id`, `enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    `worker_name` VARCHAR(255) DEFAULT NULL,
    `using` VARCHAR(255) DEFAULT NULL,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    -- Index definitions
    UNIQUE KEY `uk_cron_scheduler_name` (`name`(255)),
    INDEX `idx_cron_scheduler_enabled` (`enabled`),
    INDEX `idx_cron_scheduler_jobworkerp_server_id` (`jobworkerp_server_id`),
    INDEX `idx_cron_scheduler_created_at` (`created_at`),
    INDEX `idx_cron_scheduler_updated_at` (`updated_at`),
    INDEX `idx_cron_scheduler_enabled_created` (`enabled`, `created_at`),
    INDEX `idx_cron_scheduler_server_enabled` (`jobworkerp_server_id`, `enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    `worker_name` VARCHAR(255) DEFAULT NULL,
    `using` VARCHAR(255) DEFAULT NULL,

    -- Metadata
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,

    -- Index definitions
    UNIQUE KEY `uk_slack_event_handler_name` (`name`(255)),
    INDEX `idx_slack_event_handler_enabled` (`enabled`),
    INDEX `idx_slack_event_handler_jobworkerp_server_id` (`jobworkerp_server_id`),
    INDEX `idx_slack_event_handler_created_at` (`created_at`),
    INDEX `idx_slack_event_handler_updated_at` (`updated_at`),
    INDEX `idx_slack_event_handler_enabled_created` (`enabled`, `created_at`),
    INDEX `idx_slack_event_handler_server_enabled` (`jobworkerp_server_id`, `enabled`),
    INDEX `idx_slack_event_handler_channel_enabled` (`slack_channel_id`(255), `enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `execution_ref` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `source_type` INT NOT NULL,
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
    `result_status` INT DEFAULT NULL,
    INDEX `idx_execution_ref_source` (`source_type`, `source_id`, `triggered_at`),
    INDEX `idx_execution_ref_job` (`jobworkerp_server_id`, `job_id`),
    INDEX `idx_execution_ref_triggered_at` (`triggered_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
