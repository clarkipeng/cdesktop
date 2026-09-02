use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{assets::asset_dir, log_msg::LogMsg};

pub const EXECUTION_LOGS_DIRNAME: &str = "sessions";

pub fn process_logs_session_dir(session_id: Uuid) -> PathBuf {
    resolve_process_logs_session_dir(&asset_dir(), session_id)
}

pub fn process_log_file_path(session_id: Uuid, process_id: Uuid) -> PathBuf {
    process_log_file_path_in_root(&asset_dir(), session_id, process_id)
}

pub fn process_log_file_path_in_root(root: &Path, session_id: Uuid, process_id: Uuid) -> PathBuf {
    resolve_process_logs_session_dir(root, session_id)
        .join("processes")
        .join(format!("{}.jsonl", process_id))
}

/// Default per-execution log byte cap. Enforced at the file the child writes
/// to, so a runaway agent stops the live file's growth instead of filling the
/// disk (the 670MB-vs-claimed-16MB incident).
///
/// This is the single cap: `msg_store` sizes its in-memory mirror from the
/// same number so the UI can never show history a restart would lose.
pub const DEFAULT_MAX_EXECUTION_LOG_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LOG_BYTES_ENV: &str = "CDESKTOP_MAX_EXECUTION_LOG_BYTES";

/// Bytes reserved *past* the cap for cdesktop's own control and outcome
/// messages (the block marker, start errors, setup-required hints). The cap
/// exists to stop unbounded agent output; dropping the one line that explains
/// why the log stopped would make the limit indistinguishable from a crash.
const CONTROL_OVERDRAFT_BYTES: u64 = 64 * 1024;

pub(crate) fn max_execution_log_bytes() -> u64 {
    std::env::var(MAX_LOG_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &u64| *value > 0)
        .unwrap_or(DEFAULT_MAX_EXECUTION_LOG_BYTES)
}

/// Outcome of an append: `Blocked` means the byte cap was reached and the
/// writer will accept no further growth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogAppend {
    Written,
    Blocked,
}

pub struct ExecutionLogWriter {
    path: PathBuf,
    file: tokio::fs::File,
    written: u64,
    max_bytes: u64,
    /// A `blocked(limit)` marker is already in this file. Set on the crossing
    /// write, and on open when the file is already at the cap - a reopened
    /// writer must not append a second marker to a file whose whole purpose
    /// is to stop growing.
    marker_written: bool,
}

impl ExecutionLogWriter {
    pub async fn new(path: PathBuf) -> std::io::Result<Self> {
        Self::with_max_bytes(path, max_execution_log_bytes()).await
    }

    pub async fn with_max_bytes(path: PathBuf, max_bytes: u64) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        // Append mode: prior content still counts against the cap.
        let written = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path,
            file,
            written,
            max_bytes,
            marker_written: written >= max_bytes,
        })
    }

    pub async fn new_for_execution(session_id: Uuid, execution_id: Uuid) -> std::io::Result<Self> {
        Self::new(process_log_file_path(session_id, execution_id)).await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends a stream line (the agent's own stdout/stderr) unless the byte
    /// cap has been reached. On the crossing write a single `blocked(limit)`
    /// marker is emitted into the live file and all further growth is refused.
    pub async fn append_jsonl_line(&mut self, jsonl_line: &str) -> std::io::Result<LogAppend> {
        let len = jsonl_line.len() as u64;
        if self.written.saturating_add(len) > self.max_bytes {
            self.publish_block_marker().await?;
            return Ok(LogAppend::Blocked);
        }
        self.write_line(jsonl_line).await?;
        Ok(LogAppend::Written)
    }

    /// Appends a cdesktop-owned control or outcome line. These draw on a small
    /// overdraft past the cap so a user-facing explanation is never the thing
    /// the limit drops.
    pub async fn append_control_line(&mut self, jsonl_line: &str) -> std::io::Result<LogAppend> {
        let len = jsonl_line.len() as u64;
        let ceiling = self.max_bytes.saturating_add(CONTROL_OVERDRAFT_BYTES);
        if self.written.saturating_add(len) > ceiling {
            return Ok(LogAppend::Blocked);
        }
        self.write_line(jsonl_line).await?;
        Ok(LogAppend::Written)
    }

    async fn write_line(&mut self, jsonl_line: &str) -> std::io::Result<()> {
        self.file.write_all(jsonl_line.as_bytes()).await?;
        self.written = self.written.saturating_add(jsonl_line.len() as u64);
        Ok(())
    }

    async fn publish_block_marker(&mut self) -> std::io::Result<()> {
        if self.marker_written {
            return Ok(());
        }
        self.marker_written = true;
        let marker = LogMsg::Stderr(format!(
            "[cdesktop] execution log truncated at {} bytes: blocked(limit)",
            self.max_bytes
        ));
        if let Ok(mut line) = serde_json::to_string(&marker) {
            line.push('\n');
            self.append_control_line(&line).await?;
        }
        Ok(())
    }
}

pub async fn read_execution_log_file(path: &Path) -> std::io::Result<String> {
    tokio::fs::read_to_string(path).await
}

pub fn parse_log_jsonl_lossy(execution_id: Uuid, jsonl: &str) -> Vec<LogMsg> {
    let mut messages = Vec::new();
    let mut bad_lines = 0usize;

    for line in jsonl.lines() {
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<LogMsg>(line) {
            Ok(msg) => messages.push(msg),
            Err(e) => {
                bad_lines += 1;
                if bad_lines <= 3 {
                    tracing::warn!(
                        "Skipping unparsable log line for execution {}: {}",
                        execution_id,
                        e
                    );
                }
            }
        }
    }

    if bad_lines > 3 {
        tracing::warn!(
            "Skipped {} unparsable log lines for execution {}",
            bad_lines,
            execution_id
        );
    }

    messages
}

fn uuid_prefix2(id: Uuid) -> String {
    let s = id.to_string();
    s.chars().take(2).collect()
}

fn resolve_process_logs_session_dir(root: &Path, session_id: Uuid) -> PathBuf {
    root.join(EXECUTION_LOGS_DIRNAME)
        .join(uuid_prefix2(session_id))
        .join(session_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writer_stops_growth_at_byte_cap() {
        // Regression guard for the unbounded-append incident: the live file
        // must stop growing once the cap is hit, not merely rotate.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl");
        let mut writer = ExecutionLogWriter::with_max_bytes(path.clone(), 32)
            .await
            .unwrap();

        assert_eq!(
            writer.append_jsonl_line("0123456789\n").await.unwrap(),
            LogAppend::Written
        );
        // This line would cross the 32-byte cap: refused, marker emitted.
        assert_eq!(
            writer
                .append_jsonl_line("this line pushes past the cap\n")
                .await
                .unwrap(),
            LogAppend::Blocked
        );
        // Every subsequent append is a no-op.
        assert_eq!(
            writer.append_jsonl_line("more\n").await.unwrap(),
            LogAppend::Blocked
        );

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(contents.contains("0123456789"));
        assert!(contents.contains("blocked(limit)"));
        assert!(!contents.contains("more"));
    }

    #[tokio::test]
    async fn reopening_a_capped_file_does_not_append_another_marker() {
        // Per-writer reseed: every reopen used to re-arm `blocked` and stamp a
        // fresh marker, so a cap meant to stop growth grew the file once per
        // writer instead.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl");

        let mut first = ExecutionLogWriter::with_max_bytes(path.clone(), 32)
            .await
            .unwrap();
        first.append_jsonl_line("0123456789\n").await.unwrap();
        assert_eq!(
            first
                .append_jsonl_line("this line pushes past the cap\n")
                .await
                .unwrap(),
            LogAppend::Blocked
        );
        drop(first);
        let after_first = tokio::fs::metadata(&path).await.unwrap().len();

        let mut second = ExecutionLogWriter::with_max_bytes(path.clone(), 32)
            .await
            .unwrap();
        assert_eq!(
            second.append_jsonl_line("still blocked\n").await.unwrap(),
            LogAppend::Blocked
        );
        assert_eq!(
            tokio::fs::metadata(&path).await.unwrap().len(),
            after_first,
            "a reopened capped writer must not grow the file"
        );

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.matches("blocked(limit)").count(), 1);
    }

    #[tokio::test]
    async fn control_lines_survive_past_the_cap() {
        // The SetupRequired hint and other cdesktop-owned messages explain to
        // the user what happened; dropping them at the cap turns a limit into
        // an unexplained silence.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl");
        let mut writer = ExecutionLogWriter::with_max_bytes(path.clone(), 16)
            .await
            .unwrap();

        assert_eq!(
            writer
                .append_jsonl_line("0123456789abcdef\n")
                .await
                .unwrap(),
            LogAppend::Blocked
        );
        assert_eq!(
            writer
                .append_control_line("{\"Stderr\":\"setup required\"}\n")
                .await
                .unwrap(),
            LogAppend::Written
        );

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(contents.contains("setup required"));
    }

    #[tokio::test]
    async fn control_overdraft_is_itself_bounded() {
        // The overdraft is a reserve, not an escape hatch: a control-message
        // flood must still stop.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl");
        let mut writer = ExecutionLogWriter::with_max_bytes(path.clone(), 1)
            .await
            .unwrap();

        let line = format!("{}\n", "c".repeat(8 * 1024));
        let mut written = 0;
        for _ in 0..64 {
            if writer.append_control_line(&line).await.unwrap() == LogAppend::Written {
                written += 1;
            }
        }
        assert!(written > 0, "the overdraft must admit some control lines");
        assert_eq!(
            writer.append_control_line(&line).await.unwrap(),
            LogAppend::Blocked
        );
    }
}
