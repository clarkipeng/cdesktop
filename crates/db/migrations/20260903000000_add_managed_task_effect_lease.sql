ALTER TABLE managed_task_effects ADD COLUMN lease_id BLOB;

UPDATE managed_task_effects
SET lease_id = randomblob(16)
WHERE lease_id IS NULL;
