-- Add worker_name and using columns to cron_scheduler table.
-- This migration must be run exactly once on existing databases.
-- (New deployments include these columns in 002_schema.sql)
ALTER TABLE `cron_scheduler` ADD COLUMN `worker_name` TEXT DEFAULT NULL;
ALTER TABLE `cron_scheduler` ADD COLUMN `using` TEXT DEFAULT NULL;
