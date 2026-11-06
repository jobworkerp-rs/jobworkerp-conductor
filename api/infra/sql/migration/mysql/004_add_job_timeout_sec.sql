-- Run once against existing databases before deploying the new backend.
ALTER TABLE `cron_scheduler` ADD COLUMN `job_timeout_sec` BIGINT DEFAULT NULL;
