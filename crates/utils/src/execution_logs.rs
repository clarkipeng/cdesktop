use std::{
    io,
    path::{Path, PathBuf},
};

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

/// The durable owner is a concatenation of independently decodable Zstd
/// frames.  Its sidecar has one source locator per frame, so callers can read
/// stable uncompressed ranges without copying the log into another store.
pub fn process_log_file_path_in_root(root: &Path, session_id: Uuid, process_id: Uuid) -> PathBuf {
    resolve_process_logs_session_dir(root, session_id)
        .join("processes")
        .join(format!("{}.jsonl.zst", process_id))
}

pub fn legacy_process_log_file_path_in_root(
    root: &Path,
    session_id: Uuid,
    process_id: Uuid,
) -> PathBuf {
    resolve_process_logs_session_dir(root, session_id)
        .join("processes")
        .join(format!("{}.jsonl", process_id))
}

pub fn process_log_frame_index_path(path: &Path) -> PathBuf {
    path.with_extension("zst.frames.jsonl")
}

/// UI history is disposable and deliberately much smaller than retained
/// evidence. A restart reads the complete compressed owner from disk.
pub const DEFAULT_IN_MEMORY_LOG_BYTES: u64 = 1024 * 1024;
const IN_MEMORY_LOG_BYTES_ENV: &str = "CDESKTOP_IN_MEMORY_LOG_BYTES";

/// Evidence must not consume the last writable bytes on the volume. This is
/// checked before every frame; a refusal stops the process rather than running
/// with an unrecorded transcript.
pub const DEFAULT_FREE_DISK_RESERVE_BYTES: u64 = 512 * 1024 * 1024;
const FREE_DISK_RESERVE_BYTES_ENV: &str = "CDESKTOP_FREE_DISK_RESERVE_BYTES";

/// Bytes reserved *past* the cap for cdesktop's own control and outcome
/// messages (the block marker, start errors, setup-required hints). The cap
/// exists to stop unbounded agent output; dropping the one line that explains
/// why the log stopped would make the limit indistinguishable from a crash.
const CONTROL_OVERDRAFT_BYTES: u64 = 64 * 1024;

pub(crate) fn in_memory_log_bytes() -> u64 {
    std::env::var(IN_MEMORY_LOG_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &u64| *value > 0)
        .unwrap_or(DEFAULT_IN_MEMORY_LOG_BYTES)
}

fn free_disk_reserve_bytes() -> u64 {
    std::env::var(FREE_DISK_RESERVE_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_FREE_DISK_RESERVE_BYTES)
}

/// Outcome of an append: `Blocked` means the byte cap was reached and the
/// writer will accept no further growth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogAppend {
    Written,
    Blocked,
    Unavailable,
}

pub struct ExecutionLogWriter {
    path: PathBuf,
    file: tokio::fs::File,
    index: tokio::fs::File,
    written: u64,
    uncompressed_offset: u64,
    control_bytes: u64,
    free_disk_reserve_bytes: u64,
    /// A recording-unavailable marker was attempted for this writer.
    marker_written: bool,
}

impl ExecutionLogWriter {
    pub async fn new(path: PathBuf) -> std::io::Result<Self> {
        Self::open(path).await
    }

    pub async fn open(path: PathBuf) -> std::io::Result<Self> {
        Self::with_free_disk_reserve(path, free_disk_reserve_bytes()).await
    }

    pub async fn with_free_disk_reserve(
        path: PathBuf,
        reserve_bytes: u64,
    ) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        let written = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        let index_path = process_log_frame_index_path(&path);
        let index = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .await?;
        let uncompressed_offset = read_frame_index(&index_path)
            .await?
            .last()
            .map(|frame| frame.end)
            .unwrap_or(0);
        Ok(Self {
            path,
            file,
            index,
            written,
            uncompressed_offset,
            control_bytes: 0,
            free_disk_reserve_bytes: reserve_bytes,
            marker_written: false,
        })
    }

    pub async fn new_for_execution(session_id: Uuid, execution_id: Uuid) -> std::io::Result<Self> {
        Self::new(process_log_file_path(session_id, execution_id)).await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Each append becomes a complete Zstd frame. A crash can therefore only
    /// leave an unindexed tail; previously published ranges remain readable.
    pub async fn append_jsonl_line(&mut self, jsonl_line: &str) -> std::io::Result<LogAppend> {
        if !self.has_disk_reserve()? {
            self.publish_unavailable_marker().await?;
            return Ok(LogAppend::Unavailable);
        }
        self.write_frame(jsonl_line).await?;
        Ok(LogAppend::Written)
    }

    /// Appends a cdesktop-owned control or outcome line. These draw on a small
    /// overdraft past the cap so a user-facing explanation is never the thing
    /// the limit drops.
    pub async fn append_control_line(&mut self, jsonl_line: &str) -> std::io::Result<LogAppend> {
        let len = jsonl_line.len() as u64;
        if self.control_bytes.saturating_add(len) > CONTROL_OVERDRAFT_BYTES {
            return Ok(LogAppend::Blocked);
        }
        self.control_bytes = self.control_bytes.saturating_add(len);
        self.write_frame(jsonl_line).await?;
        Ok(LogAppend::Written)
    }

    fn has_disk_reserve(&self) -> io::Result<bool> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        Ok(fs2::available_space(parent)? >= self.free_disk_reserve_bytes)
    }

    async fn write_frame(&mut self, jsonl_line: &str) -> std::io::Result<()> {
        let input = jsonl_line.as_bytes().to_vec();
        let compressed =
            tokio::task::spawn_blocking(move || zstd::stream::encode_all(&input[..], 3))
                .await
                .map_err(io::Error::other)??;
        let offset = self.written;
        self.file.write_all(&compressed).await?;
        self.file.sync_data().await?;
        let frame = LogFrame {
            start: self.uncompressed_offset,
            end: self
                .uncompressed_offset
                .saturating_add(jsonl_line.len() as u64),
            compressed_start: offset,
            compressed_end: offset.saturating_add(compressed.len() as u64),
        };
        let mut line = serde_json::to_vec(&frame).map_err(io::Error::other)?;
        line.push(b'\n');
        self.index.write_all(&line).await?;
        self.index.sync_data().await?;
        self.written = frame.compressed_end;
        self.uncompressed_offset = frame.end;
        Ok(())
    }

    async fn publish_unavailable_marker(&mut self) -> std::io::Result<()> {
        if self.marker_written {
            return Ok(());
        }
        self.marker_written = true;
        let marker = LogMsg::Stderr(format!(
            "[cdesktop] execution recording stopped: blocked(disk-reserve)"
        ));
        if let Ok(mut line) = serde_json::to_string(&marker) {
            line.push('\n');
            self.append_control_line(&line).await?;
        }
        Ok(())
    }
}

pub async fn read_execution_log_file(path: &Path) -> std::io::Result<String> {
    let end = read_frame_index(&process_log_frame_index_path(path))
        .await?
        .last()
        .map(|frame| frame.end)
        .unwrap_or(0);
    read_execution_log_range(path, 0, end).await
}

/// Reads the requested uncompressed byte range. Frame locators make ranges
/// stable across compression changes and permit a later external index to
/// reference evidence without owning a duplicate codec or transcript copy.
pub async fn read_execution_log_range(path: &Path, start: u64, end: u64) -> io::Result<String> {
    if end < start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "range end precedes start",
        ));
    }
    let frames = read_frame_index(&process_log_frame_index_path(path)).await?;
    let bytes = tokio::fs::read(path).await?;
    let mut output = Vec::new();
    for frame in frames
        .into_iter()
        .filter(|frame| frame.end > start && frame.start < end)
    {
        let compressed = &bytes[frame.compressed_start as usize..frame.compressed_end as usize];
        let decoded = tokio::task::spawn_blocking({
            let compressed = compressed.to_vec();
            move || zstd::stream::decode_all(&compressed[..])
        })
        .await
        .map_err(io::Error::other)??;
        let from = start.saturating_sub(frame.start) as usize;
        let to = (end.min(frame.end) - frame.start) as usize;
        output.extend_from_slice(&decoded[from..to]);
    }
    String::from_utf8(output).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LogFrame {
    start: u64,
    end: u64,
    compressed_start: u64,
    compressed_end: u64,
}

async fn read_frame_index(path: &Path) -> io::Result<Vec<LogFrame>> {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) => contents
            .lines()
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
            })
            .collect(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
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
    async fn frames_round_trip_and_ranges_cross_frame_boundaries() {
        // Source ranges are the native contract for a later index. They must
        // remain correct even when a requested range crosses Zstd frames.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_control_line("first\n").await.unwrap();
        writer.append_control_line("second\n").await.unwrap();
        writer.append_control_line("third\n").await.unwrap();
        drop(writer);

        assert_eq!(
            read_execution_log_file(&path).await.unwrap(),
            "first\nsecond\nthird\n"
        );
        assert_eq!(
            read_execution_log_range(&path, 4, 15).await.unwrap(),
            "t\nsecond\nth"
        );
    }

    #[tokio::test]
    async fn each_published_frame_is_independently_decodable() {
        // A corrupted/new tail must not make previously indexed evidence
        // unreadable after restart.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_control_line("one\n").await.unwrap();
        writer.append_control_line("two\n").await.unwrap();
        drop(writer);
        let frames = read_frame_index(&process_log_frame_index_path(&path))
            .await
            .unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        for frame in frames {
            assert_eq!(
                zstd::stream::decode_all(
                    &bytes[frame.compressed_start as usize..frame.compressed_end as usize][..]
                )
                .unwrap()
                .len() as u64,
                frame.end - frame.start
            );
        }
    }

    #[tokio::test]
    async fn control_reserve_is_bounded_without_a_transcript_cap() {
        // The only byte cap is for cdesktop's control reserve. Agent evidence
        // is governed by free-disk admission instead of silent truncation.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let mut writer = ExecutionLogWriter::open(path).await.unwrap();
        let line = format!("{}\n", "c".repeat(8 * 1024));
        let mut written = 0;
        while writer.append_control_line(&line).await.unwrap() == LogAppend::Written {
            written += 1;
        }
        assert!(written > 0);
        assert_eq!(
            writer.append_control_line(&line).await.unwrap(),
            LogAppend::Blocked
        );
    }
}
