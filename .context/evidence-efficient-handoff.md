# Native execution efficiency handoff

## Scope and base

- Base: `cf555c5b23b52bbd2f8eb6f10b30af72c68be58f`
- Branch: `cdt/9e93-sm-ev-efficient`
- Fork-only draft PR: https://github.com/clarkipeng/cdesktop/pull/37
- Publication target: `clarkipeng/cdesktop`

## Implemented

- Ordinary Codex follow-ups use `thread/resume` followed by `turn/start` on
  the recorded thread, rather than `thread/fork`.
- `resume_params_from` preserves model, provider, cwd, approval policy,
  sandbox, config, base/developer instructions, and service tier while leaving
  `history` and `path` absent. A failed resume propagates; it never falls back
  to start or fork.
- Resume uses the existing fresh-start disk/admission guard. Fork reservation
  remains applied only to the review isolation path.
- `/compact` now resumes in place before native compaction. `/fast` no longer
  creates an unused fork. The deliberate review path still forks.
- Static append-prompt material is sent as native turn-scoped collaboration
  developer guidance, not stored in start/resume thread config. Follow-ups
  carry only new task input. `base_instructions` remains unset without an
  explicit base, so Codex resolves its stored or model-default instructions
  normally; changing or clearing append guidance applies to the next turn.

## Evidence and limits

- The pinned `codex-app-server-protocol` checkout at
  `be6e8eac029b183056b7e4402879f15d2c85f61b` declares
  `ThreadResume -> thread/resume`; official OpenAI app-server documentation
  likewise distinguishes `thread/resume` (continue) from `thread/fork`
  (branch).
- Local source evidence: no ordinary path retains a `thread_fork` call;
  `thread_fork` is reached only by `codex/review.rs`.
- An in-memory fake JSON-RPC app-server drives the production
  `AppServerClient` and `launch_codex_agent` path. It proves two ordinary
  continuations emit `account/read`/`thread/resume`/`turn/start` on one thread
  with zero forks. A separate protocol serialization fixture keeps the review
  branch on `thread/fork`. The peer also captures the changed/cleared
  turn-scoped guidance and cancellation of an unresolved `turn/start`, which
  returns without replaying it. Pinned app-server source shows that null
  collaboration developer guidance selects the built-in mode setting; no
  append guidance is stored on resume. The pinned server itself was not run:
  its isolated test requires dependencies absent from the local vendor cache,
  and the attempt was stopped before it could fetch them.
- Normalization keys usage by native thread and turn, so repeated usage updates
  for one turn replace its entry rather than replaying old usage.
- Measured local copy/startup result: not yet available. No provider/cache or
  latency claim is made. The live authenticated canary remains root-owned.

## Verification

- `git diff --check`: passed.
- Passed isolated command:
  `/usr/bin/sandbox-exec -f /Users/clarkpeng/conductor/workspaces/sightmesh-v1/ankara/.context/evidence-test-isolation.sb cargo test -p executors --lib`
  Exit `0`; 111 passed.
- `cargo clippy -p executors --tests -- -D warnings`: exit `0`.
- `pnpm run format`: exit `0` after `pnpm install --frozen-lockfile`; no source
  formatting changes outside this lane.
- All runtime checks used the required state/network isolation profile where
  applicable. No provider request or live state mutation occurred.

## Publication state

The fork branch is published and API-verified against draft PR #37. The
mistaken upstream `cdesktop-ai/cdesktop` PR #21 was closed without deleting the
fork branch. No merge, provider inference, cdesktop install/activation, or
service restart was performed.

## Remaining proof

The pinned app-server mock-server test still needs to run in an environment
with its already-provisioned toolchain and dependency cache. It is the final
executable confirmation that a null turn collaboration override restores the
built-in mode guidance. This lane is checkpointed, not complete, until that
test is recorded.
