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
- `append_prompt` remains its documented per-turn user-prompt suffix. It is
  neither stored in start/resume thread configuration nor written into native
  collaboration guidance. A changed or cleared value therefore affects only
  the new turn; configured developer instructions stay separate.
  `base_instructions` remains unset without an explicit base, so Codex
  resolves its stored or model-default instructions normally.

## Evidence and limits

- The pinned `codex-app-server-protocol` checkout at
  `be6e8eac029b183056b7e4402879f15d2c85f61b` declares
  `ThreadResume -> thread/resume`.
- Local source evidence: no ordinary path retains a `thread_fork` call;
  `thread_fork` is reached only by `codex/review.rs`.
- An in-memory fake JSON-RPC app-server drives the production
  `AppServerClient` and `launch_codex_agent` path. It proves two ordinary
  continuations emit `account/read`/`thread/resume`/`turn/start` on one thread
  with zero forks. A separate protocol serialization fixture keeps the review
  branch on `thread/fork`. The peer also captures changed/cleared per-turn
  append input and cancellation of an unresolved `turn/start`, which returns
  without replaying it.
- An isolated exact-pinned native `codex-app-server` plus local HTTP mock
  provider was built outside the worktree. With a fresh temporary `CODEX_HOME`
  and the Suva sandbox policy, it executed `thread/start`, `turn/start`, a
  fresh-process `thread/resume`, and another `turn/start`. Both turns sent a
  null collaboration developer setting and retained the server's default base
  instructions. The mock saw the configured append suffix only on the first
  new user turn; the resumed turn contained only its new prompt. Historical
  input remains native thread history and was not duplicated by cdesktop.
- Normalization keys usage by native thread and turn, so repeated usage updates
  for one turn replace its entry rather than replaying old usage.
- Measured local copy/startup result: not yet available. No provider/cache or
  latency claim is made. The live authenticated canary remains root-owned.

## Verification

- `git diff --check`: passed.
- Passed isolated command:
  `/usr/bin/sandbox-exec -f /Users/clarkpeng/conductor/workspaces/sightmesh/suva/.context/evidence-test-isolation.sb cargo test -p executors --lib`
  Exit `0`; 111 passed.
- Passed isolated command:
  `/usr/bin/sandbox-exec -f /Users/clarkpeng/conductor/workspaces/sightmesh/suva/.context/evidence-test-isolation.sb cargo clippy -p executors --tests -- -D warnings`
  Exit `0`.
- Passed native mock proof under the same policy: `python3
  /tmp/sm-ev-efficient-vendor.lHx9Tw/native_guidance_proof.py
  /tmp/sm-ev-efficient-vendor.lHx9Tw/target/debug/codex-app-server` with
  `CODEX_HOME=/tmp/sm-ev-efficient-vendor.lHx9Tw/native-home`; exit `0`.
- All runtime checks used the required state/network isolation profile where
  applicable. No provider request or live state mutation occurred.

## Publication state

The fork branch is published and API-verified against draft PR #37. The
mistaken upstream `cdesktop-ai/cdesktop` PR #21 was closed without deleting the
fork branch. No merge, provider inference, cdesktop install/activation, or
service restart was performed.

## Completion

The required pinned mock-server proof is recorded above. The live authenticated
canary remains root-owned and was not run.
