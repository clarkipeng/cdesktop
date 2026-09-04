use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::{OwnedRwLockReadGuard, RwLock};

const MAX_DRAIN_SECONDS: u64 = 30;
static DRAIN_UNTIL_MILLIS: AtomicU64 = AtomicU64::new(0);
static EXECUTION_ADMISSION: LazyLock<Arc<RwLock<()>>> = LazyLock::new(|| Arc::new(RwLock::new(())));

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub fn drain_remaining_millis() -> u64 {
    DRAIN_UNTIL_MILLIS
        .load(Ordering::Acquire)
        .saturating_sub(now_millis())
}

pub async fn set_drain(seconds: u64) -> u64 {
    let _starts = EXECUTION_ADMISSION.write().await;
    let seconds = seconds.min(MAX_DRAIN_SECONDS);
    let deadline = if seconds == 0 {
        0
    } else {
        now_millis().saturating_add(seconds.saturating_mul(1000))
    };
    DRAIN_UNTIL_MILLIS.store(deadline, Ordering::Release);
    drain_remaining_millis()
}

#[derive(Debug, thiserror::Error)]
#[error("execution start refused while cdesktop is draining for maintenance")]
pub struct DrainError {
    remaining_millis: u64,
}

impl DrainError {
    pub fn retry_after_seconds(&self) -> u64 {
        self.remaining_millis.div_ceil(1000).max(1)
    }
}

pub async fn admit_execution_start() -> Result<OwnedRwLockReadGuard<()>, DrainError> {
    let admission = EXECUTION_ADMISSION.clone().read_owned().await;
    let remaining_millis = drain_remaining_millis();
    if remaining_millis > 0 {
        return Err(DrainError { remaining_millis });
    }
    Ok(admission)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn execution_start() -> Result<(), DrainError> {
        let _admission = admit_execution_start().await?;
        Ok(())
    }

    async fn stop_execution() -> Result<(), std::convert::Infallible> {
        // Stops deliberately have no execution-start admission dependency.
        Ok(())
    }

    #[tokio::test]
    async fn external_and_internal_starts_are_refused_while_stops_remain_ungated() {
        set_drain(300).await;
        // HTTP workspace/session starts, managed-task launches, manual routine
        // runs, and scheduler routine runs all converge on this boundary.
        for _reachable_start in 0..4 {
            let refusal = execution_start().await.unwrap_err();
            assert!((1..=MAX_DRAIN_SECONDS).contains(&refusal.retry_after_seconds()));
        }

        assert!(stop_execution().await.is_ok());

        set_drain(0).await;
        assert_eq!(drain_remaining_millis(), 0);
        assert!(execution_start().await.is_ok());
    }
}
