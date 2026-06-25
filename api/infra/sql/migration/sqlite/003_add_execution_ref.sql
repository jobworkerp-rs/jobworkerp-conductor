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
