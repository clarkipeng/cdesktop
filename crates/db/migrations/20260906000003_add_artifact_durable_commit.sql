-- Historical references do not inherit the current connection's durability.
-- A verified-policy write upgrades this marker in the reference transaction.
ALTER TABLE execution_artifacts ADD COLUMN durable_commit INTEGER NOT NULL DEFAULT 0
    CHECK (durable_commit IN (0, 1));
