# Native evidence handoff

- Base: `cf555c5b23b52bbd2f8eb6f10b30af72c68be58f`
- Implementation commit: `28815c971038a36f5a4d72425d9bc2f61d84afc6`
- Branch: `cdt/46bf-sm-ev-native-r2`
- Draft PR: https://github.com/clarkipeng/cdesktop/pull/38

## Delivered

Native execution logs now have one durable compressed owner:
`sessions/<prefix>/<session>/processes/<execution>.jsonl.zst`. Each JSONL
record is an independently decodable Zstd frame. The adjacent
`*.zst.frames.jsonl` maps stable uncompressed ranges to compressed frames.
The frame is fsynced before its locator is fsynced, so a published source range
survives restart even if a crash leaves a final unindexed frame.

The live UI retains only its own bounded 1 MiB working set. Retention is no
longer capped at 16 MiB: every evidence frame checks a 512 MiB free-disk
reserve. A refusal writes `blocked(disk-reserve)` through the control reserve
when possible and invokes the existing process-tree stop hook exactly once.

The startup database-log migration writes a temporary compressed owner, fsyncs
the frames, verifies the decompressed SHA-256 against the source, then
publishes index before owner. Database rows are retained until all migrations
finish. Legacy `.jsonl` files remain readable. The debug production asset-log
fallback is removed.

## Verification

All source tests used:
`/usr/bin/sandbox-exec -f /Users/clarkpeng/conductor/workspaces/sightmesh-v1/ankara/.context/evidence-test-isolation.sb`.

- `cargo test -p utils --lib`: exit 0, 11 passed.
- `cargo test -p services --lib execution_process::tests`: exit 0, 2 passed.
- `pnpm run format`: exit 0 after authorized dependency installation.

## Remaining owned work

This is a checkpoint, not the approved lane complete. It lacks streamed
`FileService` artifact ingestion, durable artifact occurrence links and GC
protection, artifact provenance/source timestamps, and migration tests for all
publication/pruning crash boundaries. Do not mark complete or merge without
those pieces and an independent exact-head review. No generated shared types
changed; the eventual artifact/read-route contract may require regenerated
types in the integration change.
