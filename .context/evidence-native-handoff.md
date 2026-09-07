# Native evidence handoff

Base: `cf555c5b23b52bbd2f8eb6f10b30af72c68be58f`  
Branch: `cdt/46bf-sm-ev-native-r2`  
Head: `851046c408e17fc8b5da9522bf0215dbd15c9c2e`  
Draft PR: https://github.com/clarkipeng/cdesktop/pull/38

## Delivered checkpoint

`execution_processes.id` plus a decompressed half-open byte range is the log
source locator. `GET /api/execution-processes/{id}/raw-log?start=&end=` returns
the exact original bytes with `x-cdesktop-source-range`.

The owner is checksummed independently-decodable Zstd frames. The sidecar is
only an accelerator: missing, torn, and stale coverage falls back to bounded
streaming decode. Capture metadata is a Zstd skippable frame beside raw JSONL,
so producer bytes and migration hashes stay exact.

Attachments remain deduplicated by bytes. `execution_artifacts.id` identifies
each execution occurrence and holds the attachment live for GC. Native routes
are `POST /api/execution-processes/{id}/artifacts` (streamed multipart field
named `artifact`) and the execution-scoped occurrence GET route.

## Verification

Under `/usr/bin/sandbox-exec -f /Users/clarkpeng/conductor/workspaces/sightmesh/suva/.context/evidence-test-isolation.sb`:

- `cargo test -p utils execution_logs --lib`: 7 passed.
- `cargo check -p server -p services`: passed.

`pnpm run format` passed before the Rust checkpoint; subsequent source changes
were formatted with `cargo fmt --all`.

## Remaining limitations

This is not a completion claim. The legacy migration still needs crash-boundary
tests (temporary owner/index publication and verified pruning); occurrence/GC
needs an end-to-end database integration test; and durable capture needs a
production-path write/fsync-failure test. No live migration was run.
