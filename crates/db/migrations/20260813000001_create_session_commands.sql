CREATE TABLE session_commands (
    id BLOB PRIMARY KEY NOT NULL,
    session_id BLOB NOT NULL,
    dedupe_key TEXT,
    intent TEXT NOT NULL CHECK (intent IN ('continue', 'replace')),
    body TEXT NOT NULL,
    config TEXT,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'claimed', 'done', 'failed', 'cancelled')),
    execution_process_id BLOB,
    created_at TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    finished_at TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (execution_process_id) REFERENCES execution_processes(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX idx_session_commands_dedupe
ON session_commands(dedupe_key)
WHERE dedupe_key IS NOT NULL;

CREATE INDEX idx_session_commands_pending
ON session_commands(session_id, state);

CREATE UNIQUE INDEX idx_one_running_coding_agent_per_session
ON execution_processes(session_id)
WHERE status = 'running' AND run_reason = 'codingagent';
