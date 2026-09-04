# PR #28 round-2 review — BLOCK

Reviewed `clarkipeng/cdesktop` at `9851ead2568cbe436851923f2280fe2914fa8d72` against `main` (`4871d4ad` fetched 2026-09-03). This is a **BLOCK**: the head does not compile, the four-agent admission cap is bypassable by direct coding-agent launches, and the new recovery/journal paths can revive cancelled work or publish a false terminal journal state.

## Findings

### P1 — the PR cannot compile: the storage-guard success path calls a private method

`crates/executors/src/executors/codex/client.rs:175` calls `reservation.commit()`, but `ForkReservation::commit` is private at `crates/executors/src/executors/codex/storage_guard.rs:127`. Rust rejects this across sibling modules with `E0624`, before any test can run. The smallest fix is `pub(super) fn commit` (the reservation type is already `pub(super)`), preserving the intended crate-private surface.

### P1 — `CDESKTOP_MAX_RUNNING_AGENTS` governs only queued dispatch; direct agent launches can exceed it

`crates/services/src/services/container.rs:410-422` checks the global count only in `dispatch_pending_commands`. The common direct paths call `start_execution` directly, for example `crates/server/src/routes/teammates.rs:332-339`, while `start_execution_with_id` at `crates/services/src/services/container.rs:1521` creates and starts a CodingAgent with no equivalent admission under the scheduler lock. The later host check (`crates/local-deployment/src/container.rs:1419-1429`) tests OS process/disk headroom, not the configured four-agent cap. Thus concurrent direct teammate/review/PR launches can create an arbitrary number of coding agents whenever the host RLIMIT remains high—the exact protection intended for the fleet is ineffective outside the durable queue.

Smallest robust fix: make one locked CodingAgent admission primitive (count + reserve/create, or a database-backed slot) and require both queued and direct launch paths to use it before creating the execution row. Do not duplicate a count check at each route; it races and will drift.

### P1 — startup recovery can re-dispatch a command deliberately superseded/cancelled by a Replace

`crates/server/src/routes/sessions/mod.rs:344-377` inserts a Replace, cancels only **pending** commands, then kills the running CodingAgent. Its already-claimed command is left claimed. On a restart, `crates/services/src/services/container.rs:582-601` changes every inherited running CodingAgent to Killed and calls `requeue_killed_execution`; that helper at `crates/db/src/models/session_command.rs:328-339` moves claimed, failed, **and done** rows back to pending. Startup then invokes `dispatch_all_pending_commands` (`crates/server/src/startup.rs:175-182`). Because the original command is older by rowid, it is dispatched before the replacement. The same broad requeue is exposed by `POST .../commands/requeue` at `crates/server/src/routes/sessions/mod.rs:445-476`.

This explains tonight's stale queued-mail incident and violates cancellation authority. Persist interruption disposition with the claimed execution (requeue only crash-recoverable work; mark an explicitly stopped/superseded claim cancelled), and make startup recovery consume that disposition. In particular, never requeue `done` rows: completion is authoritative.

### P1 — a live second instance can falsely finalize a managed-task effect as `lost`

`crates/server/src/routes/managed_tasks.rs:199-216` treats *any* pending record owned by another `INSTANCE_ID` as an owner that restarted, probes before the original request has finished, and calls `ManagedTaskEffect::finish`. There is no lease, heartbeat, or owner liveness proof. A concurrent retry routed to a second live process therefore changes the journal row to `lost` (or `active` if the native session was created early) while the first process is still launching. `ManagedTaskEffect::finish` at `crates/db/src/models/managed_task_effect.rs:102-126` also lacks an owner predicate, so it accepts that foreign finalization. The first owner subsequently cannot correct the now-non-pending record.

Smallest robust fix: pending records need a lease/fencing token (or must return retryable pending to every non-owner); only a stale, expired lease may be recovered, and `finish` must compare the owner/fence. This is required for the event journal to be correct under concurrency.

## Incident assessment

`POST /api/workspaces/summaries`: at this head the route is metadata-only (`crates/server/src/routes/workspaces/workspace_summary.rs:119-156`); it no longer calls Git. I found no code path in this handler that accounts for ~10 seconds for four workspaces. The UI does independently issue on-demand diff-stat requests for visible rows (`packages/web-core/src/shared/hooks/useWorkspaceDiffStats.ts:63-98`), which can be slow but are separate requests. Investigate SQLite lock/wait telemetry or request timing around the handler rather than attribute this latency to the removed Git fan-out.

Queued commands at `>=4`: the queue deliberately holds pending work at the cap (`crates/services/src/services/container.rs:419-422`) and only completion/startup dispatch wakes it. That is correct while all four are truly live. The direct-launch bypass above means the cap is not host-wide, and the stale requeue finding can make a later wake launch cancelled work.

## Verification

Commands run against a detached worktree at the exact requested head:

- `cargo test -p db -p services -p local-deployment -p server` — **failed during compilation** of `executors` with `E0624` at `codex/client.rs:175`; no selected crate tests executed.
- `cargo test -p db managed_task_effect -- --nocapture` — **failed during the same compilation** with `E0624`; focused event-journal tests did not execute.
- `pnpm --filter web-core check` — not runnable in this worktree because dependencies are absent (`sh: tsc: command not found`). Static inspection also notes `useWorkspaceDiffStats.ts:123` uses `useMemo` without importing it at line 1; once dependencies are installed this should be corrected/verified.

The storage guard's design does bound a Codex fork reservation while its process remains alive, and the summary route removal is directionally correct, but neither offsets the blocking correctness failures above.
