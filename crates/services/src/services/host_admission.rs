//! Host resource admission for executor spawns.
//!
//! Spawning a coding agent forks a whole process subtree and writes to disk.
//! Without a pre-flight check the fork simply fails with `EAGAIN` once the
//! per-uid process ceiling is hit, or the write fails once the disk is full -
//! both discovered by crashing. This module refuses the spawn *before* the
//! fork when host headroom is insufficient, with a typed error.

use std::path::{Path, PathBuf};

const DEFAULT_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const MIN_FREE_ENV: &str = "CDESKTOP_MIN_FREE_DISK_BYTES";

/// Estimated processes in one agent's subtree (the agent plus its helpers).
/// Admission projects this against the OS process ceiling so a host with a low
/// `RLIMIT_NPROC` admits fewer agents by construction.
const DEFAULT_PER_AGENT_PROCESSES: u64 = 64;
const PER_AGENT_ENV: &str = "CDESKTOP_PROCESSES_PER_AGENT";

/// Processes kept free below the ceiling for cdesktop itself and everything
/// else on the host.
const DEFAULT_PROCESS_RESERVE: u64 = 128;
const PROCESS_RESERVE_ENV: &str = "CDESKTOP_PROCESS_HEADROOM_RESERVE";

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("spawn refused: {available} free disk bytes is below the {reserve} byte reserve")]
    DiskExhausted { available: u64, reserve: u64 },
    #[error(
        "spawn refused: {projected} projected processes would exceed the {ceiling} process ceiling"
    )]
    ProcessExhausted { projected: u64, ceiling: u64 },
    #[error("failed to probe free disk: {0}")]
    Probe(#[from] std::io::Error),
}

/// Live process ceiling and current agent load, resolved from the OS.
#[derive(Debug, Clone, Copy)]
pub struct ProcessHeadroom {
    /// Soft `RLIMIT_NPROC` (per-uid process ceiling).
    pub ceiling: u64,
    /// Agent subtrees cdesktop already has running.
    pub live_agents: u64,
    /// Estimated processes per agent subtree.
    pub per_agent: u64,
    /// Processes kept free for the rest of the host.
    pub reserve: u64,
}

/// Pure admission decision. Split from the OS probes so it is exhaustively
/// unit-testable.
fn admit(
    available_bytes: u64,
    min_free: u64,
    headroom: Option<ProcessHeadroom>,
) -> Result<(), AdmissionError> {
    if available_bytes < min_free {
        return Err(AdmissionError::DiskExhausted {
            available: available_bytes,
            reserve: min_free,
        });
    }
    if let Some(h) = headroom {
        let projected = h
            .live_agents
            .saturating_add(1)
            .saturating_mul(h.per_agent)
            .saturating_add(h.reserve);
        if projected > h.ceiling {
            return Err(AdmissionError::ProcessExhausted {
                projected,
                ceiling: h.ceiling,
            });
        }
    }
    Ok(())
}

/// Refuse the spawn unless the host has both free-disk and process headroom.
/// `live_agents` is cdesktop's current count of tracked executor children.
pub fn reserve_spawn_headroom(disk_probe: &Path, live_agents: u64) -> Result<(), AdmissionError> {
    let available = available_space(disk_probe)?;
    admit(available, min_free_bytes(), process_headroom(live_agents))
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

fn available_space(path: &Path) -> std::io::Result<u64> {
    fs2::available_space(&existing_ancestor(path))
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

/// Resolve the per-uid process ceiling. Returns `None` when there is no
/// enforceable ceiling (unlimited, or a platform without `RLIMIT_NPROC` such as
/// Windows), in which case only the disk reserve gates the spawn.
#[cfg(unix)]
fn process_headroom(live_agents: u64) -> Option<ProcessHeadroom> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` only writes into the provided rlimit struct.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NPROC, &mut lim) };
    if rc != 0 {
        return None;
    }
    let ceiling = lim.rlim_cur as u64;
    if ceiling == 0 || lim.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    Some(ProcessHeadroom {
        ceiling,
        live_agents,
        per_agent: env_u64(PER_AGENT_ENV, DEFAULT_PER_AGENT_PROCESSES),
        reserve: env_u64(PROCESS_RESERVE_ENV, DEFAULT_PROCESS_RESERVE),
    })
}

#[cfg(not(unix))]
fn process_headroom(_live_agents: u64) -> Option<ProcessHeadroom> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headroom(ceiling: u64, live_agents: u64) -> ProcessHeadroom {
        ProcessHeadroom {
            ceiling,
            live_agents,
            per_agent: 64,
            reserve: 128,
        }
    }

    #[test]
    fn refuses_when_free_disk_below_reserve() {
        // The disk-full incident: admission must trip before the write fails.
        let err = admit(1_000, 2_000, None).unwrap_err();
        assert!(matches!(err, AdmissionError::DiskExhausted { .. }));
    }

    #[test]
    fn admits_with_disk_and_process_headroom() {
        assert!(admit(10_000, 2_000, Some(headroom(4_096, 1))).is_ok());
    }

    #[test]
    fn refuses_when_next_agent_would_exceed_process_ceiling() {
        // ceiling 512: (10+1)*64 + 128 = 832 > 512 -> refuse before fork EAGAIN.
        let err = admit(10_000, 2_000, Some(headroom(512, 10))).unwrap_err();
        assert!(matches!(err, AdmissionError::ProcessExhausted { .. }));
    }

    #[test]
    fn no_ceiling_leaves_only_the_disk_gate() {
        assert!(admit(10_000, 2_000, None).is_ok());
    }
}
