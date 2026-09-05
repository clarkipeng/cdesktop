ALTER TABLE managed_task_effects
    ADD COLUMN retryable INTEGER NOT NULL DEFAULT 0;

ALTER TABLE managed_task_effects
    ADD COLUMN retry_after_seconds INTEGER;
