-- Add worker_name and using columns to slack_event_handler and worker_result_handler tables
-- for worker execution mode support (any runner type, not just WORKFLOW)
-- NOTE: These ALTER statements are NOT idempotent. If a column already exists, the statement
-- will fail. Run only once, or check column existence before executing in manual migration.
ALTER TABLE `slack_event_handler` ADD COLUMN `worker_name` TEXT DEFAULT NULL;
ALTER TABLE `slack_event_handler` ADD COLUMN `using` TEXT DEFAULT NULL;
ALTER TABLE `worker_result_handler` ADD COLUMN `worker_name` TEXT DEFAULT NULL;
ALTER TABLE `worker_result_handler` ADD COLUMN `using` TEXT DEFAULT NULL;
