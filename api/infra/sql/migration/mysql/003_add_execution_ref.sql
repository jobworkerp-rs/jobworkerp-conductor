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
