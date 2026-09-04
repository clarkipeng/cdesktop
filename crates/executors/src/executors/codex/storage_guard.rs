//! Codex rollout storage guard.
//!
//! What this guard is and is not: it rate-limits and size-caps Codex rollout
//! forks. It does not garbage collect rollouts, does not deduplicate
//! transcript history, and does not make an over-cap session resumable - once
//! a rollout exceeds the cap the resume is *refused*, so the session becomes
//! unusable rather than smaller. Bounding the growth is a stopgap; the real
//! fix is rollout GC plus content-addressed history
//! (clarkipeng/cdesktop#29, upstream cdesktop-ai/cdesktop#16).

use std::{
    collections::VecDeque,
    env, io,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use super::codex_home;

const DEFAULT_MAX_ROLLOUT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const MAX_ROLLOUT_ENV: &str = "CDESKTOP_MAX_CODEX_ROLLOUT_BYTES";
const MIN_FREE_ENV: &str = "CDESKTOP_MIN_FREE_DISK_BYTES";

/// A resume/compact/tier-change forks the Codex rollout, and every fork
/// materializes a fresh copy of the prior transcript. The size cap refuses a
/// single oversized copy; this breaker refuses a *burst* of copies whose
/// cumulative growth would still exhaust the disk (the 363GB fork-storm).
const DEFAULT_MAX_FORKS_PER_WINDOW: usize = 30;
const FORK_WINDOW: Duration = Duration::from_secs(60);
const MAX_FORKS_ENV: &str = "CDESKTOP_MAX_CODEX_FORKS_PER_MIN";

/// Process-global so the budget holds across every Codex session in the fleet,
/// not just within one app-server client (each resume is a fresh client).
static GLOBAL_FORK_BREAKER: LazyLock<ForkRateBreaker> = LazyLock::new(|| {
    ForkRateBreaker::new(
        env_limit(MAX_FORKS_ENV, DEFAULT_MAX_FORKS_PER_WINDOW as u64) as usize,
        FORK_WINDOW,
    )
});

/// Why a fork was refused. Rate-limiting is a *retryable* refusal and carries
/// the window remainder, so the caller can classify it as transient instead of
/// terminal; every other refusal is a plain I/O error.
#[derive(Debug)]
pub(super) enum ForkGuardError {
    RateLimited { retry_after_seconds: i64 },
    Io(io::Error),
}

impl From<io::Error> for ForkGuardError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Sliding-window rate limiter over fork reservations. A reservation is made
/// before the RPC, so concurrent callers cannot over-admit the budget.
struct ForkRateBreaker {
    max: usize,
    window: Duration,
    state: Mutex<ForkRateBreakerState>,
}

struct ForkRateBreakerState {
    events: VecDeque<(u64, Instant)>,
    next_id: u64,
}

/// A pre-RPC budget slot. It returns capacity when dropped unless committed
/// after the fork succeeds.
pub(super) struct ForkReservation {
    id: Option<u64>,
}

impl ForkRateBreaker {
    fn new(max: usize, window: Duration) -> Self {
        Self {
            max,
            window,
            state: Mutex::new(ForkRateBreakerState {
                events: VecDeque::new(),
                next_id: 0,
            }),
        }
    }

    /// Reserve one slot atomically. `Err` is the remaining window, i.e. how
    /// long until the oldest successful reservation ages out.
    fn reserve_at(&self, now: Instant) -> Result<u64, Duration> {
        let mut state = self.state.lock().unwrap();
        Self::drain_expired(&mut state.events, now, self.window);
        if state.events.len() < self.max {
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.events.push_back((id, now));
            return Ok(id);
        }
        let oldest = state.events.front().map(|(_, at)| *at).unwrap_or(now);
        Err(self.window.saturating_sub(now.duration_since(oldest)))
    }

    fn release(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        if let Some(position) = state
            .events
            .iter()
            .position(|(event_id, _)| *event_id == id)
        {
            state.events.remove(position);
        }
    }

    fn drain_expired(events: &mut VecDeque<(u64, Instant)>, now: Instant, window: Duration) {
        while let Some((_, front)) = events.front() {
            if now.duration_since(*front) >= window {
                events.pop_front();
            } else {
                break;
            }
        }
    }

    fn reserve(&self) -> Result<ForkReservation, ForkGuardError> {
        self.reserve_at(Instant::now())
            .map(|id| ForkReservation { id })
            .map_err(|remaining| {
                // Round up: a sub-second remainder must still ask for >= 1s, or a
                // caller that honours `retry_after` retries into the same refusal.
                ForkGuardError::RateLimited {
                    retry_after_seconds: remaining.as_secs_f64().ceil().max(1.0) as i64,
                }
            })
    }
}

impl ForkReservation {
    pub(super) fn commit(mut self) {
        self.id.take();
    }
}

impl Drop for ForkReservation {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            GLOBAL_FORK_BREAKER.release(id);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct StorageLimits {
    max_rollout_bytes: u64,
    min_free_bytes: u64,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            max_rollout_bytes: env_limit(MAX_ROLLOUT_ENV, DEFAULT_MAX_ROLLOUT_BYTES),
            min_free_bytes: env_limit(MIN_FREE_ENV, DEFAULT_MIN_FREE_BYTES),
        }
    }
}

impl StorageLimits {
    pub(super) async fn ensure_start_allowed(&self) -> io::Result<()> {
        let home = codex_home().ok_or_else(|| io::Error::other("Codex home is unavailable"))?;
        let probe = existing_ancestor(&home);
        let available = fs2::available_space(&probe)?;
        if available < self.min_free_bytes {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "Codex start refused: {available} free bytes is below the {} byte reserve",
                    self.min_free_bytes
                ),
            ));
        }
        Ok(())
    }

    pub(super) async fn reserve_fork(
        &self,
        thread_id: &str,
    ) -> Result<ForkReservation, ForkGuardError> {
        self.ensure_start_allowed().await?;
        let path = find_rollout_file(thread_id).await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Codex fork refused: source rollout {thread_id} was not found"),
            )
        })?;
        self.ensure_rollout_allowed(&path).await?;
        GLOBAL_FORK_BREAKER.reserve()
    }

    pub(super) async fn ensure_rollout_allowed(&self, path: &Path) -> io::Result<()> {
        let bytes = tokio::fs::metadata(path).await?.len();
        if bytes > self.max_rollout_bytes {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                format!(
                    "Codex rollout stopped at {bytes} bytes; limit is {} bytes",
                    self.max_rollout_bytes
                ),
            ));
        }
        self.ensure_start_allowed().await
    }
}

pub(super) async fn find_rollout_file(thread_id: &str) -> Option<PathBuf> {
    let sessions = codex_home()?.join("sessions");
    find_in(&sessions, thread_id).await
}

async fn find_in(dir: &Path, thread_id: &str) -> Option<PathBuf> {
    let mut entries = tokio::fs::read_dir(dir).await.ok()?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = Box::pin(find_in(&path, thread_id)).await {
                return Some(found);
            }
        } else if let Some(name) = path.file_name().and_then(|name| name.to_str())
            && name.starts_with("rollout-")
            && name.contains(thread_id)
            && name.ends_with(".jsonl")
        {
            return Some(path);
        }
    }
    None
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

fn env_limit(name: &str, fallback: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;

    #[tokio::test]
    async fn oversized_rollout_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-test.jsonl");
        tokio::fs::write(&path, b"too large").await.unwrap();
        let limits = StorageLimits {
            max_rollout_bytes: 4,
            min_free_bytes: 1,
        };

        let error = limits.ensure_rollout_allowed(&path).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::FileTooLarge);
    }

    #[tokio::test]
    async fn disk_reserve_is_checked_before_launch() {
        let limits = StorageLimits {
            max_rollout_bytes: u64::MAX,
            min_free_bytes: u64::MAX,
        };

        let error = limits.ensure_start_allowed().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    }

    #[test]
    fn fork_rate_breaker_trips_past_budget_and_recovers() {
        // Guards the fork-storm dimension the size cap can't see: many
        // individually-legal forks whose cumulative copies exhaust the disk.
        let breaker = ForkRateBreaker::new(2, Duration::from_secs(60));
        let t0 = Instant::now();

        assert!(breaker.reserve_at(t0).is_ok());
        assert!(breaker.reserve_at(t0).is_ok());

        let remaining = breaker.reserve_at(t0).unwrap_err();
        assert_eq!(remaining, Duration::from_secs(60));

        // Once the window drains, forks are allowed again.
        assert!(breaker.reserve_at(t0 + Duration::from_secs(61)).is_ok());
    }

    #[test]
    fn refused_forks_consume_no_budget() {
        // A failed RPC releases its reservation, so it cannot starve healthy
        // forks that arrive later.
        let breaker = ForkRateBreaker::new(2, Duration::from_secs(60));
        let t0 = Instant::now();

        for _ in 0..1_000 {
            let id = breaker.reserve_at(t0).unwrap();
            breaker.release(id);
        }
        assert!(breaker.reserve_at(t0).is_ok());
    }

    #[test]
    fn breaker_retry_after_never_rounds_down_to_zero() {
        // A retry hint of 0 would send the caller straight back into the same
        // refusal, so a sub-second remainder still asks for one second.
        let breaker = ForkRateBreaker::new(1, Duration::from_millis(200));
        breaker.reserve_at(Instant::now()).unwrap();
        match breaker.reserve() {
            Err(ForkGuardError::RateLimited {
                retry_after_seconds,
            }) => assert_eq!(retry_after_seconds, 1),
            other => panic!("expected a rate-limited refusal, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_forks_reserve_at_most_the_budget() {
        // All callers cross this barrier before reserving. A split check and
        // record would admit every caller; the single locked reservation can
        // admit only the 30 slots that exist before their RPCs complete.
        const MAX_FORKS: usize = 30;
        const FORKS: usize = 64;
        let breaker = Arc::new(ForkRateBreaker::new(MAX_FORKS, Duration::from_secs(60)));
        let barrier = Arc::new(Barrier::new(FORKS));
        let handles = (0..FORKS)
            .map(|_| {
                let breaker = breaker.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    breaker.reserve_at(Instant::now()).ok()
                })
            })
            .collect::<Vec<_>>();
        let reservations = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(reservations.len(), MAX_FORKS);
        for id in reservations {
            breaker.release(id);
        }
    }
}
