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
pub const DEFAULT_MAX_EXECUTION_LOG_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LOG_BYTES_ENV: &str = "CDESKTOP_MAX_EXECUTION_LOG_BYTES";

fn max_execution_log_bytes() -> u64 {
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
    blocked: bool,
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
            blocked: false,
        })
    }

    pub async fn new_for_execution(session_id: Uuid, execution_id: Uuid) -> std::io::Result<Self> {
        Self::new(process_log_file_path(session_id, execution_id)).await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends a line unless the byte cap has been reached. On the crossing
    /// write a single `blocked(limit)` marker is emitted into the live file and
    /// all further growth is refused.
    pub async fn append_jsonl_line(&mut self, jsonl_line: &str) -> std::io::Result<LogAppend> {
        if self.blocked {
            return Ok(LogAppend::Blocked);
        }
        let len = jsonl_line.len() as u64;
        if self.written.saturating_add(len) > self.max_bytes {
            self.write_blocked_marker().await?;
            return Ok(LogAppend::Blocked);
        }
        self.file.write_all(jsonl_line.as_bytes()).await?;
        self.written = self.written.saturating_add(len);
        Ok(LogAppend::Written)
    }

    async fn write_blocked_marker(&mut self) -> std::io::Result<()> {
        self.blocked = true;
        let marker = LogMsg::Stderr(format!(
            "[cdesktop] execution log truncated at {} bytes: blocked(limit)",
            self.max_bytes
        ));
        if let Ok(mut line) = serde_json::to_string(&marker) {
            line.push('\n');
            // The marker itself is one bounded line; write it directly so the
            // block is visible in the live file even at the cap.
            self.file.write_all(line.as_bytes()).await?;
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
}
