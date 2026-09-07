# Native evidence partial

Base: `cf555c5b23b52bbd2f8eb6f10b30af72c68be58f`.

`sessions/<session-prefix>/<session>/processes/<execution>.jsonl.zst` is the
native raw-log owner. Its adjacent `*.zst.frames.jsonl` gives each published
Zstd frame a stable source locator:

`(execution_id, uncompressed_start, uncompressed_end)`.

The sidecar also records compressed frame offsets. The compressed frame is
fsynced before its locator is appended and fsynced, so any published range is
independently readable after restart. A crash may leave only an unindexed tail;
it never changes a published range. Native range reads use uncompressed byte
offsets and support cross-frame reads.

Coverage is every persisted stdout/stderr record in capture order. `LogMsg`
bytes remain the original structured payload; capture time is the frame
publication order, source time is unavailable unless a provider payload already
contains it. Recording refusal is explicit as `blocked(disk-reserve)` and stops
the owned process through the existing stop hook. Legacy `.jsonl` remains
readable until startup migration has published the compressed owner.

Not yet implemented in this checkpoint: occurrence-safe artifact retention and
the full verified legacy migration publication/prune protocol.
