//! Host resource admission for executor spawns.
//!
//! Spawning a coding agent forks a whole process subtree and writes to disk.
//! Without a pre-flight check the fork simply fails with `EAGAIN` once the
//! per-uid process ceiling is hit, or the write fails once the disk is full -
//! both discovered by crashing. This module refuses the spawn *before* the
//! fork when host headroom is insufficient, with a typed, retryable error.
//!
//! Admission is advisory, never authoritative: every probe fails open. A host
//! that cannot report free disk or its process ceiling admits the spawn rather
//! than bricking the app on an unreadable `statfs`.

use std::path::{Path, PathBuf};

use db::models::execution_process::ExecutionProcessRunReason;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

const DEFAULT_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const MIN_FREE_ENV: &str = "CDESKTOP_MIN_FREE_DISK_BYTES";

/// Processes one new agent subtree is expected to add (the agent plus its
/// helpers: MCP servers, language servers, git children).
const DEFAULT_PER_AGENT_PROCESSES: u64 = 64;
const PER_AGENT_ENV: &str = "CDESKTOP_PROCESSES_PER_AGENT";

/// Processes kept free below the ceiling for cdesktop itself and everything
/// else on the host.
const DEFAULT_PROCESS_RESERVE: u64 = 128;
const PROCESS_RESERVE_ENV: &str = "CDESKTOP_PROCESS_HEADROOM_RESERVE";

/// How long a caller should wait before retrying a refused spawn. Host
/// pressure drains on the timescale of an agent finishing, not milliseconds.
pub const ADMISSION_RETRY_AFTER_SECONDS: i64 = 30;

/// Which host resource ran out. Carried in the API refusal body so a client
/// can tell "free some disk" from "wait for agents to finish".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(use_ts_enum)]
pub enum AdmissionResource {
    Disk,
    Processes,
}

/// Machine-readable refusal payload. A refused spawn is retryable, so the
/// client is told what ran out, by how much, and when to come back - never
/// just a message string.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct AdmissionRefusal {
    pub resource: AdmissionResource,
    /// Observed availability: free bytes for `disk`, live processes for
    /// `processes`.
    pub available: u64,
    /// The bound that was violated: the free-byte reserve for `disk`, the
    /// projected process need for `processes`.
    pub reserve: u64,
    pub retry_after_seconds: i64,
    /// Fixed, cdesktop-owned description. Never contains host paths.
    pub safe_message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("spawn refused: {available} free disk bytes is below the {reserve} byte reserve")]
    DiskExhausted { available: u64, reserve: u64 },
    #[error(
        "spawn refused: {available} live processes plus reserve would exceed the {reserve} process ceiling"
    )]
    ProcessExhausted { available: u64, reserve: u64 },
}

impl AdmissionError {
    pub fn resource(&self) -> AdmissionResource {
        match self {
            Self::DiskExhausted { .. } => AdmissionResource::Disk,
            Self::ProcessExhausted { .. } => AdmissionResource::Processes,
        }
    }

    /// Observed availability: free bytes for disk, live processes for the
    /// process ceiling.
    pub fn available(&self) -> u64 {
        match self {
            Self::DiskExhausted { available, .. } | Self::ProcessExhausted { available, .. } => {
                *available
            }
        }
    }

    /// The bound that was violated: the free-byte reserve for disk, the
    /// projected process need for the process ceiling.
    pub fn reserve(&self) -> u64 {
        match self {
            Self::DiskExhausted { reserve, .. } | Self::ProcessExhausted { reserve, .. } => {
                *reserve
            }
        }
    }

    pub fn retry_after_seconds(&self) -> i64 {
        ADMISSION_RETRY_AFTER_SECONDS
    }

    /// Machine-readable refusal for the API boundary.
    pub fn refusal(&self) -> AdmissionRefusal {
        AdmissionRefusal {
            resource: self.resource(),
            available: self.available(),
            reserve: self.reserve(),
            retry_after_seconds: self.retry_after_seconds(),
            safe_message: match self.resource() {
                AdmissionResource::Disk => {
                    "Host is out of free disk; spawn refused. Free space and retry.".to_string()
                }
                AdmissionResource::Processes => {
                    "Host is out of process headroom; spawn refused. Retry once agents finish."
                        .to_string()
                }
            },
        }
    }
}

/// Live process ceiling and current host load, resolved from the OS.
#[derive(Debug, Clone, Copy)]
pub struct ProcessHeadroom {
    /// Soft `RLIMIT_NPROC` (per-uid process ceiling).
    pub ceiling: u64,
    /// Processes already alive that count against the ceiling. Measured from
    /// the OS where it can be asked; otherwise estimated from the agent
    /// subtrees cdesktop is tracking.
    pub live_processes: u64,
    /// Processes the next agent subtree is expected to add.
    pub per_agent: u64,
    /// Processes kept free for the rest of the host.
    pub reserve: u64,
}

/// Pure admission decision. Split from the OS probes so it is exhaustively
/// unit-testable. `None` headroom means the host has no enforceable process
/// ceiling, so only the disk reserve gates the spawn.
fn admit(
    available_bytes: Option<u64>,
    min_free: u64,
    headroom: Option<ProcessHeadroom>,
) -> Result<(), AdmissionError> {
    if let Some(available_bytes) = available_bytes
        && available_bytes < min_free
    {
        return Err(AdmissionError::DiskExhausted {
            available: available_bytes,
            reserve: min_free,
        });
    }
    if let Some(h) = headroom {
        let projected = h
            .live_processes
            .saturating_add(h.per_agent)
            .saturating_add(h.reserve);
        if projected > h.ceiling {
            return Err(AdmissionError::ProcessExhausted {
                available: h.live_processes,
                reserve: projected,
            });
        }
    }
    Ok(())
}

/// Whether a spawn for this run reason is subject to host admission.
///
/// Only new coding-agent subtrees are gated. Every other run reason is a
/// lifecycle or recovery path, and gating those on the resource they free is
/// a self-deadlock: cleanup and archive are exactly how a full disk gets
/// emptied, so they must run *because* the host is exhausted, not in spite of
/// it. Setup scripts and dev servers gate on the coding agent that follows
/// them, so gating them twice only turns one refusal into two.
pub fn is_admission_gated(run_reason: &ExecutionProcessRunReason) -> bool {
    match run_reason {
        ExecutionProcessRunReason::CodingAgent => true,
        ExecutionProcessRunReason::SetupScript
        | ExecutionProcessRunReason::CleanupScript
        | ExecutionProcessRunReason::ArchiveScript
        | ExecutionProcessRunReason::DevServer => false,
    }
}

/// Refuse the spawn unless the host has both free-disk and process headroom.
///
/// Nothing is reserved: this is a point-in-time check, so two spawns racing
/// can both be admitted. It exists to refuse an already-exhausted host, not
/// to allocate capacity - hence `check`, not `reserve`.
///
/// `tracked_agents` is cdesktop's count of live executor children, used only
/// as the fallback process estimate on platforms that cannot report the real
/// process count.
pub fn check_spawn_headroom(
    disk_probe: &Path,
    tracked_agents: u64,
) -> Result<(), AdmissionError> {
    admit(
        available_space(disk_probe),
        min_free_bytes(),
        process_headroom(tracked_agents),
    )
}

fn min_free_bytes() -> u64 {
    env_u64(MIN_FREE_ENV, DEFAULT_MIN_FREE_BYTES)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &u64| *value > 0)
        .unwrap_or(fallback)
}

/// Free bytes on the volume backing `path`, or `None` when the probe fails.
/// A probe that cannot answer must not refuse the spawn.
fn available_space(path: &Path) -> Option<u64> {
    match fs2::available_space(existing_ancestor(path)) {
        Ok(bytes) => Some(bytes),
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "free-disk probe failed; admitting spawn without a disk check"
            );
            None
        }
    }
}

fn existing_ancestor(path: &Path) -> PathBuf {
    let mut candidate = path.to_path_buf();
    while !candidate.exists() {
        if !candidate.pop() {
            return PathBuf::from(".");
        }
    }
    candidate
}

/// Resolve the per-uid process ceiling and the live process count. Returns
/// `None` when there is no enforceable ceiling (unlimited, or a platform
/// without `RLIMIT_NPROC` such as Windows).
#[cfg(unix)]
fn process_headroom(tracked_agents: u64) -> Option<ProcessHeadroom> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` only writes into the provided rlimit struct.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NPROC, &mut lim) };
    if rc != 0 {
        return None;
    }
    let ceiling: u64 = lim.rlim_cur;
    if ceiling == 0 || lim.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    let per_agent = env_u64(PER_AGENT_ENV, DEFAULT_PER_AGENT_PROCESSES);
    Some(ProcessHeadroom {
        ceiling,
        // Prefer the real host process count: the ceiling is per-uid and every
        // process on the host consumes it, not just the children cdesktop
        // happens to be tracking. Fall back to the tracked-subtree estimate
        // where the OS cannot be asked.
        live_processes: live_process_count()
            .unwrap_or_else(|| tracked_agents.saturating_mul(per_agent)),
        per_agent,
        reserve: env_u64(PROCESS_RESERVE_ENV, DEFAULT_PROCESS_RESERVE),
    })
}

#[cfg(not(unix))]
fn process_headroom(_tracked_agents: u64) -> Option<ProcessHeadroom> {
    None
}

/// Number of processes currently alive on the host, or `None` when the
/// platform offers no cheap count (fail-open).
#[cfg(target_os = "macos")]
fn live_process_count() -> Option<u64> {
    // libproc: called with a null buffer, `proc_listallpids` returns the
    // number of pids it would have written instead of writing any.
    unsafe extern "C" {
        fn proc_listallpids(buffer: *mut libc::c_void, buffersize: libc::c_int) -> libc::c_int;
    }
    // SAFETY: the null/zero-length form is the documented "just count them"
    // call and writes nothing.
    let count = unsafe { proc_listallpids(std::ptr::null_mut(), 0) };
    if count <= 0 {
        return None;
    }
    Some(count as u64)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn live_process_count() -> Option<u64> {
    // /proc is the portable Linux answer; a numeric directory per pid.
    let entries = std::fs::read_dir("/proc").ok()?;
    let count = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.bytes().all(|b| b.is_ascii_digit()))
        })
        .count();
    (count > 0).then_some(count as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headroom(ceiling: u64, live_processes: u64) -> ProcessHeadroom {
        ProcessHeadroom {
            ceiling,
            live_processes,
            per_agent: 64,
            reserve: 128,
        }
    }

    #[test]
    fn refuses_when_free_disk_below_reserve() {
        // The disk-full incident: admission must trip before the write fails.
        let err = admit(Some(1_000), 2_000, None).unwrap_err();
        assert!(matches!(err, AdmissionError::DiskExhausted { .. }));
        assert_eq!(err.resource(), AdmissionResource::Disk);
        assert_eq!(err.available(), 1_000);
        assert_eq!(err.reserve(), 2_000);
    }

    #[test]
    fn admits_with_disk_and_process_headroom() {
        assert!(admit(Some(10_000), 2_000, Some(headroom(4_096, 64))).is_ok());
    }

    #[test]
    fn refuses_when_next_agent_would_exceed_process_ceiling() {
        // ceiling 512: 640 live + 64 + 128 = 832 > 512 -> refuse before EAGAIN.
        let err = admit(Some(10_000), 2_000, Some(headroom(512, 640))).unwrap_err();
        assert!(matches!(err, AdmissionError::ProcessExhausted { .. }));
        assert_eq!(err.resource(), AdmissionResource::Processes);
    }

    #[test]
    fn no_ceiling_leaves_only_the_disk_gate() {
        assert!(admit(Some(10_000), 2_000, None).is_ok());
    }

    #[test]
    fn unreadable_disk_probe_admits_instead_of_refusing() {
        // Fail-open invariant: a probe that cannot answer must never become a
        // host-wide spawn block. `None` is "unknown", not "zero free bytes".
        assert!(admit(None, u64::MAX, None).is_ok());
    }

    #[test]
    fn only_coding_agents_are_admission_gated() {
        // Self-deadlock guard: the paths that FREE the resource must never be
        // gated on the resource.
        assert!(is_admission_gated(
            &ExecutionProcessRunReason::CodingAgent
        ));
        for exempt in [
            ExecutionProcessRunReason::CleanupScript,
            ExecutionProcessRunReason::ArchiveScript,
            ExecutionProcessRunReason::SetupScript,
            ExecutionProcessRunReason::DevServer,
        ] {
            assert!(
                !is_admission_gated(&exempt),
                "{exempt:?} must spawn on an exhausted host"
            );
        }
    }

    #[test]
    fn archive_runs_at_one_gigabyte_free_where_a_coding_agent_is_refused() {
        // The concrete regression: 1GB free is below the default 10GB reserve,
        // so a coding agent is refused - and the archive script that would
        // reclaim space must still be allowed to run.
        const ONE_GIB: u64 = 1024 * 1024 * 1024;
        assert!(admit(Some(ONE_GIB), DEFAULT_MIN_FREE_BYTES, None).is_err());
        assert!(!is_admission_gated(
            &ExecutionProcessRunReason::ArchiveScript
        ));
    }

    #[test]
    fn live_process_count_is_plausible_or_absent() {
        // The honest-counting fix: when the platform can answer, the count is
        // the real host process count, which is always at least this test
        // process. `None` (fail-open) is the only other legal answer.
        match live_process_count() {
            Some(count) => assert!(count >= 1, "implausible live process count {count}"),
            None => {}
        }
    }
}
