# Native evidence contract

- Log source: `execution_processes.id` plus a half-open decompressed byte range.
  `GET /api/execution-processes/{id}/raw-log?start={u64}&end={u64}` returns the
  exact producer bytes and `x-cdesktop-source-range: [start, end)`.
- The native owner is concatenated checksummed Zstd frames. Frame indexes are
  rebuildable accelerators: a missing, torn, or stale index falls back to a
  bounded streaming decode of the owner.
- Capture time/order metadata is retained in Zstd skippable frames beside raw
  JSONL, so decompression and migration hashing preserve producer bytes.
- Artifact source: `execution_artifacts.id` is the occurrence identity;
  `attachment_id` is only the content-addressed blob identity. The FK excludes
  retained occurrences from orphan cleanup.
- Producer/read routes: `POST /api/execution-processes/{id}/artifacts` accepts
  a streamed `artifact` multipart field and returns its occurrence; `GET
  /api/execution-processes/{id}/artifacts/{occurrence_id}` resolves only an
  occurrence owned by that execution.
- Replay contract in progress: callers supply `publication_key`; native SQLite
  will use `(execution_id, publication_key)` as the idempotency boundary and
  reject a replay whose byte blob or source metadata differs.

Routing note: prior status commands mistakenly targeted a retired cdesktop
session. No further workspace-manager routing is used; this local checkpoint
is the status surface for the root coordinator.

Focused isolated checks: `cargo test -p utils execution_logs --lib` (7 passed)
and `cargo check -p server -p services` (passed before the final stale-index
regression test). A final combined check is pending this checkpoint.
