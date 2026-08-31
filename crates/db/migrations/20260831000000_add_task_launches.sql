-- This record answers which native workspace and session identities cdesktop
-- authorized for a durable external idempotency key. That fact cannot be
-- reconstructed from workspace listings after a response loss or crash.
CREATE TABLE task_launches (
    id BLOB PRIMARY KEY NOT NULL,
    contract_version INTEGER NOT NULL CHECK (contract_version = 1),
    task_id TEXT NOT NULL,
    incarnation_generation INTEGER NOT NULL CHECK (incarnation_generation >= 0),
    attempt_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    launch TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'pending'
        CHECK (phase IN ('pending', 'active', 'terminal', 'refused')),
    workspace_id BLOB NOT NULL,
    session_id BLOB NOT NULL,
    owner_instance_id BLOB NOT NULL,
    effect_created INTEGER NOT NULL DEFAULT 0,
    history_ref TEXT,
    outcome TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    UNIQUE (task_id, incarnation_generation, attempt_id)
);

CREATE INDEX idx_task_launches_task_generation
    ON task_launches (task_id, incarnation_generation DESC);
