-- A blob is deduplicated by attachments.hash; an occurrence is not. The FK
-- keeps task evidence alive while worktree copies are safely disposable.
CREATE TABLE execution_artifacts (
    id UUID PRIMARY KEY NOT NULL,
    execution_id UUID NOT NULL REFERENCES execution_processes(id) ON DELETE RESTRICT,
    attachment_id UUID NOT NULL REFERENCES attachments(id) ON DELETE RESTRICT,
    original_path TEXT NOT NULL,
    producer_ref TEXT,
    captured_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_execution_artifacts_execution_id ON execution_artifacts(execution_id);
CREATE INDEX idx_execution_artifacts_attachment_id ON execution_artifacts(attachment_id);
