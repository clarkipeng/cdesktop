-- A caller retries the same logical publication with this key. The unique
-- execution/key pair is the durable idempotency boundary; historical rows
-- have NULL and remain distinct.
ALTER TABLE execution_artifacts ADD COLUMN publication_key TEXT;

CREATE UNIQUE INDEX idx_execution_artifacts_publication_key
    ON execution_artifacts(execution_id, publication_key);
