# PR #28 round 3 review — BLOCK

Reviewed `clarkipeng/cdesktop` at `5a580ce8193bd350995215d33db763cea3c77cbd`
against rebased main `4871d4ad`.

## Findings

### P0 — The requested head does not compile

`crates/server/src/routes/workspaces/workspace_summary.rs:1-3` removes
`HashMap`, `Arc`, `LazyLock`, and `Mutex`, although the retained
`RefreshClaim` statics at `20-23` and methods at `35-55` still use all of
them.  `cargo test -p server managed_tasks --no-fail-fast` consequently fails
with unresolved `LazyLock`, `Arc`, `Mutex`, and `HashMap` (plus inference
cascades).  That same target also catches stale managed-task test code:
`crates/server/src/routes/managed_tasks.rs:320` omits the new `lease_id`, and
line `379` invokes `ManagedTaskEffect::finish` with six instead of eight
arguments.  The server cannot build or test.

Smallest robust fix: retain the required standard-library imports (or remove
the no-longer-needed refresh implementation as one coherent change), and make
the managed-task test factory/call mirror the owner-and-lease production API.

### P1 — Fork-rate limiter admits an unbounded concurrent burst

`crates/executors/src/executors/codex/storage_guard.rs:157-167` checks the
process-global fork budget without reserving capacity.  The actual charge is
separate, after the RPC completes, at
`crates/executors/src/executors/codex/client.rs:176-179`.  Thirty-one (or many
more) concurrent `thread_fork` calls can all observe 0/30, pass their disk and
rollout checks, send `thread/fork`, and only then each record.  This restores
the transcript-copy burst which the 30-per-minute guard is intended to prevent.
The existing tests only interleave `check` and `record` serially.

Smallest robust fix: replace the split check/record API with an atomic
reservation under the breaker mutex before issuing `thread/fork`; release that
reservation if the RPC fails, and retain/commit it only after a successful
fork.  Add a barrier-based concurrent test proving at most `max` callers pass.

## Prior-round findings verified closed

* Coding-agent admission: queued dispatch calls `admit_coding_agent` while
  holding `scheduler_lock` before it creates its durable running row
  (`crates/services/src/services/container.rs:423-585`).  Every direct coding
  launch, including initial/follow-up requests, teammate bootstrap, chained
  setup continuation, and requeue dispatch, reaches `start_execution`, whose
  coding-agent arm acquires the same lock and calls the same admission method
  (`1516-1559`).  Teammates call that entry point directly
  (`crates/server/src/routes/teammates.rs:292-300`).
* Replace/recovery: inserting a `Replace` cancels both pending and claimed
  predecessors in its transaction (`crates/db/src/models/session_command.rs:90-130`).
  Requeue can update only `claimed`/`failed` rows, never `done`/`cancelled`, and
  its non-running process predicate is part of that same UPDATE statement
  (`306-320`).
* Managed-task ownership: finalization is a single `UPDATE` guarded by pending
  state, `owner_instance_id`, and `lease_id`
  (`crates/db/src/models/managed_task_effect.rs:105-130`).

## Other adversarial checks

The workspace summaries route is metadata-only and does not fan out Git work;
single-workspace diff stats are on the dedicated endpoint and serialized before
the six-computation service semaphore (`workspace_summary.rs:219-239`,
`git.rs:166-188`, `diff_stream.rs:42-121`).  Log persistence has a bounded
writer and stops a coding agent on the first blocked line.  I found no further
P-level defect in lifecycle terminalization or the managed-task journal beyond
the fork-breaker race above.

## Verification

* `cargo test -p server managed_tasks --no-fail-fast` fails to compile with
  the P0 diagnostics above.
* `cargo test -p executors oversized_rollout_is_refused --no-fail-fast` passes.
  Temporarily inverting the size comparison makes that test fail with
  `unwrap_err()` receiving `Ok(())`; the guard was then restored and the
  original test passed.
* `git diff --check 4871d4ad 5a580ce` reports only generated
  `shared/types.ts` trailing whitespace.
