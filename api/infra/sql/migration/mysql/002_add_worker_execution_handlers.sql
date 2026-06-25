-- Add worker_name and using columns to slack_event_handler and worker_result_handler tables
-- for worker execution mode support (any runner type, not just WORKFLOW)
-- NOTE: IF NOT EXISTS requires MariaDB 10.0.2+. Standard MySQL does not support this syntax;
-- for MySQL, remove IF NOT EXISTS and ensure the migration runs only once.
ALTER TABLE `slack_event_handler` ADD COLUMN IF NOT EXISTS `worker_name` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `slack_event_handler` ADD COLUMN IF NOT EXISTS `using` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `worker_result_handler` ADD COLUMN IF NOT EXISTS `worker_name` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `worker_result_handler` ADD COLUMN IF NOT EXISTS `using` VARCHAR(255) DEFAULT NULL;
