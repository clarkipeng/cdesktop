# Native execution efficiency: checkpoint

## Now

- Base verified: `cf555c5b23b52bbd2f8eb6f10b30af72c68be58f`.
- Ordinary Codex follow-ups now use native `thread/resume`, preserving the
  recorded thread id and effective launch settings without supplying history
  or a rollout path.
- `/compact` resumes the same thread before native compaction instead of
  creating a copied fork. Reviews remain the intentional fork path.
- Follow-up user input contains the new task plus the current configured
  `append_prompt` suffix; configured static guidance stays separate as native
  thread instructions.
- `/fast` no longer creates an unused fork after changing its existing setting.

## Review correction

- `append_prompt` remains a per-turn user-prompt suffix, not start/resume
  configuration or collaboration developer guidance. Changed and cleared
  values affect the current new turn without replacing Codex's default base
  instructions or rewriting static developer guidance.
- A fake JSON-RPC app-server exercises the real `AppServerClient` and
  `launch_codex_agent` continuation path for repeated resumes,
  cancellation/restart, absent explicit base guidance, and changed/cleared
  append guidance. A separate protocol serialization fixture retains the
  explicit review fork. Native usage de-duplication has its own focused test.

## Verification

- Passed isolated: `cargo test -p executors --lib` under Suva's
  `evidence-test-isolation.sb` (111 passed).
- Passed: `cargo clippy -p executors --tests -- -D warnings`, `pnpm run format`,
  and `git diff --check`.

## Proved locally

- Repeated continuation fixtures emit `thread/resume`, then `turn/start`, for
  the same recorded thread and emit no fork request.
- The explicit review path fixture retains `thread/fork`.
- Cancellation returns an unknown `turn/start` outcome for command
  reconciliation rather than retrying it blindly; the next continuation is a
  fresh `thread/resume` + `turn/start` pair.
- Usage updates retain `thread_id` and `turn_id`, and a second notification for
  the same turn replaces its normalized entry instead of replaying old usage.

## Native proof

- The exact pinned native app-server was built in an isolated temporary vendor
  checkout and run with a local mock provider, fresh native home, and Suva's
  sandbox policy. The first start and fresh-process resume both sent null
  collaboration developer guidance and preserved default base instructions.
  The mock observed the append suffix only in its first new user turn and a
  cleared suffix only in the next turn's current user input; old native history
  was retained once, not replayed by cdesktop.

## Constraints retained

- No fallback from resume to fork/start.
- `thread_resume` performs the same start-admission check as a new thread;
  fork-specific reservation remains limited to actual forks.
- No DB, migration, lockfile, provider, or SightMesh wake files were changed.

## Publication correction

- The only active draft is https://github.com/clarkipeng/cdesktop/pull/37 on
  `clarkipeng/cdesktop`.
- The mistakenly opened upstream `cdesktop-ai/cdesktop` PR #21 is closed; its
  fork branch was not deleted.
