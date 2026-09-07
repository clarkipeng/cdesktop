-- A deduplicated blob's first filename does not describe every occurrence.
-- NULL explicitly means this was not captured by the older schema.
ALTER TABLE execution_artifacts ADD COLUMN original_name TEXT;
