# PR #28 round-4 review — BLOCK

Reviewed `clarkipeng/cdesktop` PR #28 at exact head
`637f2d6c02710541d2b7b9f7613bc7cf3c1706e2`, against `main`.

## P0 — Linux CI cannot compile `local-deployment`

- **Location:** `crates/local-deployment/src/process_budget.rs:5-6`
- **Failure path:** the top-level `CString` and `c_char/c_int/c_uint/c_void`
  imports are used only by `#[cfg(target_os = "macos")]` FFI declarations.
  On the Ubuntu CI runner they are unused; CI uses `-D warnings`, so compiling
  `local-deployment` exits 101. This fails both **backend-clippy** and
  **tauri-checks** at the exact PR head.
- **Smallest robust fix:** put those imports behind the same
  `#[cfg(target_os = "macos")]` gate (or qualify them at the macOS-only use
  sites). Keep the common `Arc`, `RwLock`, `Duration`, and `Instant` imports
  unconditional.

## Regression verification

- The Codex fork breaker now reserves and records under one mutex critical
  section, returns a required `u64` id, commits only after RPC success, and
  releases the slot through `Drop` on RPC failure. A reservation id is never
  absent. The exact 64-concurrent / 30-admitted test passes. Temporarily
  changing `events.len() < max` to `<= max` makes that test fail with 31
  admissions; the guard was restored cleanly.
- Fleet admission remains in `ContainerService::start_execution` for every
  direct CodingAgent launch. Queue dispatch holds the same scheduler lock
  around its admission and durable execution-row creation. The owner/lease
  fence remains on managed-task finalization, and Replace-cancelled/done
  commands remain non-revivable.
- No `/agents` route, `goToAgents`, `execution-routing` UI surface, removed
  locale key, or test reference remains in `packages/` or `shared/`.

## Exact-head CI run 33828511939

| Job | Conclusion |
| --- | --- |
| changes | pass |
| frontend-checks | pass |
| backend-remote-checks | pass |
| backend-schema-checks | pass |
| tauri-checks | **fail** (P0 above) |
| backend-test | pass |
| backend-clippy | **fail** (P0 above) |
| release-distribution-checks | skipped |

Focused local checks: `cargo test -p executors` (103 passed), `cargo test -p
services` (30 library tests plus integration coverage passed), and `cargo test
-p local-deployment` (22 passed). These pass on macOS but do not supersede the
Ubuntu compile failure in exact-head CI.

**Verdict: BLOCK.** The later fork-reservation, admission, lifecycle-fencing,
and navigation-removal fixes survive review, but PR #28 cannot merge while the
two exact-head CI jobs are red from the Linux-only import error.
