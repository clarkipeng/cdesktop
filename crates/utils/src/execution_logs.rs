use std::{
    io::{self, BufRead, Read, Seek, SeekFrom},
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

/// Bounded emergency space for control/outcome records, accounted across
/// writer reopenings independently from ordinary disk admission.
const CONTROL_OVERDRAFT_BYTES: u64 = 64 * 1024;
/// Range consumers must page. This keeps a hostile range request from turning
/// the evidence endpoint into an unbounded allocation.
pub const MAX_EXECUTION_LOG_RANGE_BYTES: u64 = 1024 * 1024;

pub(crate) fn in_memory_log_bytes() -> u64 {
    std::env::var(IN_MEMORY_LOG_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &u64| *value > 0)
        .unwrap_or(DEFAULT_IN_MEMORY_LOG_BYTES)
}

pub fn free_disk_reserve_bytes() -> u64 {
    std::env::var(FREE_DISK_RESERVE_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_FREE_DISK_RESERVE_BYTES)
}

/// `Blocked` means the control allowance is exhausted. Ordinary evidence has
/// no byte cap: unavailable storage stops capture explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogAppend {
    Written,
    Blocked,
    Unavailable,
}

/// Metadata is written in a Zstd skippable frame beside its raw JSONL bytes.
/// Consequently a normal Zstd reader sees *exactly* the producer bytes while
/// recovery can rebuild metadata without trusting the sidecar index.
#[derive(serde::Serialize, serde::Deserialize)]
struct CapturedLogMetadata {
    captured_at: Option<chrono::DateTime<chrono::Utc>>,
    capture_order: u64,
    control: bool,
    outcome: Option<CaptureOutcome>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureOutcome {
    Complete,
    Unavailable,
    LegacyUnknown,
}

const MAX_FRAME_UNCOMPRESSED_BYTES: usize = 64 * 1024;
const ZSTD_SKIPPABLE_FRAME_MAGIC: u32 = 0x184D_2A50;

pub struct ExecutionLogWriter {
    path: PathBuf,
    file: tokio::fs::File,
    index: Option<tokio::fs::File>,
    written: u64,
    uncompressed_offset: u64,
    control_bytes: u64,
    free_disk_reserve_bytes: u64,
    /// A recording-unavailable marker was attempted for this writer.
    marker_written: bool,
    /// An interrupted write/sync may have published only part of a frame.
    poisoned: bool,
    sealed: bool,
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
        let file = file.into_std().await;
        fs2::FileExt::try_lock_exclusive(&file)?;
        let written = file.metadata()?.len();
        let file = tokio::fs::File::from_std(file);
        let index_path = process_log_frame_index_path(&path);
        let index = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .await
            .ok();
        // The compressed owner, not its cache index, defines the next stable
        // range. Refuse to append after a torn owner instead of reusing an
        // offset based on a stale sidecar.
        let recovered = scan_owner(&path).await?;
        file.sync_all().await?;
        if let Some(parent) = path.parent() {
            tokio::fs::File::open(parent).await?.sync_all().await?;
        }
        Ok(Self {
            path,
            file,
            index,
            written,
            uncompressed_offset: recovered.uncompressed_bytes,
            control_bytes: recovered.control_bytes,
            free_disk_reserve_bytes: reserve_bytes,
            marker_written: false,
            poisoned: false,
            sealed: recovered.outcome.is_some(),
        })
    }

    pub async fn new_for_execution(session_id: Uuid, execution_id: Uuid) -> std::io::Result<Self> {
        Self::new(process_log_file_path(session_id, execution_id)).await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A terminal capture marker is part of the owner, not inferred from a
    /// process exit or an empty range response. It adds no producer bytes.
    pub async fn finish(&mut self, outcome: CaptureOutcome) -> io::Result<()> {
        self.ensure_writable()?;
        if !self.has_control_space(0)? {
            return Err(io::Error::other("cannot persist capture outcome"));
        }
        self.write_segment(&[], Some(chrono::Utc::now()), true, Some(outcome))
            .await?;
        self.sealed = true;
        Ok(())
    }

    /// Each append becomes a complete Zstd frame. A crash can therefore only
    /// leave an unindexed tail; previously published ranges remain readable.
    pub async fn append_jsonl_line(&mut self, jsonl_line: &str) -> std::io::Result<LogAppend> {
        self.ensure_writable()?;
        if !self.has_disk_reserve(jsonl_line.len())? {
            self.publish_unavailable_marker().await?;
            return Ok(LogAppend::Unavailable);
        }
        self.write_captured_record(jsonl_line, Some(chrono::Utc::now()))
            .await?;
        Ok(LogAppend::Written)
    }

    /// Legacy records predate capture instrumentation. Their unknown capture
    /// time is explicit rather than invented during migration.
    pub async fn append_legacy_jsonl_line(
        &mut self,
        jsonl_line: &str,
    ) -> std::io::Result<LogAppend> {
        self.ensure_writable()?;
        if !self.has_disk_reserve(jsonl_line.len())? {
            return Ok(LogAppend::Unavailable);
        }
        self.write_captured_record(jsonl_line, None).await?;
        Ok(LogAppend::Written)
    }

    /// Appends a cdesktop-owned control or outcome line. These draw on a small
    /// overdraft past the cap so a user-facing explanation is never the thing
    /// the limit drops.
    pub async fn append_control_line(&mut self, jsonl_line: &str) -> std::io::Result<LogAppend> {
        self.ensure_writable()?;
        let len = jsonl_line.len() as u64;
        if self.control_bytes.saturating_add(len) > CONTROL_OVERDRAFT_BYTES {
            return Ok(LogAppend::Blocked);
        }
        // Control output spends the reserve ordinary evidence was forbidden
        // from consuming, so the refusal marker remains recordable.
        if !self.has_control_space(jsonl_line.len())? {
            return Ok(LogAppend::Unavailable);
        }
        self.write_frame(jsonl_line, Some(chrono::Utc::now()), true)
            .await?;
        Ok(LogAppend::Written)
    }

    fn has_disk_reserve(&self, incoming_bytes: usize) -> io::Result<bool> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        // Compression can expand incompressible input. Reserve the complete
        // pending payload plus frame/index overhead before admitting it.
        let required = self
            .free_disk_reserve_bytes
            .saturating_add(incoming_bytes as u64)
            .saturating_add(8 * 1024);
        Ok(fs2::available_space(parent)? >= required)
    }

    fn ensure_writable(&self) -> io::Result<()> {
        if self.poisoned || self.sealed {
            Err(io::Error::other(
                "execution log requires recovery after an uncertain write",
            ))
        } else {
            Ok(())
        }
    }

    fn has_control_space(&self, incoming_bytes: usize) -> io::Result<bool> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        Ok(fs2::available_space(parent)? >= (incoming_bytes as u64).saturating_add(8 * 1024))
    }

    async fn write_frame(
        &mut self,
        bytes: &str,
        captured_at: Option<chrono::DateTime<chrono::Utc>>,
        control: bool,
    ) -> std::io::Result<()> {
        for input in bytes.as_bytes().chunks(MAX_FRAME_UNCOMPRESSED_BYTES) {
            self.write_segment(input, captured_at, control, None)
                .await?;
        }
        Ok(())
    }

    async fn write_segment(
        &mut self,
        input: &[u8],
        captured_at: Option<chrono::DateTime<chrono::Utc>>,
        control: bool,
        outcome: Option<CaptureOutcome>,
    ) -> std::io::Result<()> {
        let input = input.to_vec();
        let input_len = input.len() as u64;
        let compressed = tokio::task::spawn_blocking(move || {
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3)?;
            encoder.include_checksum(true)?;
            std::io::Write::write_all(&mut encoder, &input)?;
            encoder.finish()
        })
        .await
        .map_err(io::Error::other)??;
        let metadata = CapturedLogMetadata {
            captured_at,
            capture_order: self.uncompressed_offset,
            control,
            outcome,
        };
        let metadata = serde_json::to_vec(&metadata).map_err(io::Error::other)?;
        // Metadata has its own checksum while standard Zstd readers continue
        // to return only the original JSONL bytes.
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1)?;
        encoder.include_checksum(true)?;
        std::io::Write::write_all(&mut encoder, &metadata)?;
        let metadata = encoder.finish()?;
        let mut skippable = Vec::with_capacity(8 + metadata.len());
        skippable.extend_from_slice(&ZSTD_SKIPPABLE_FRAME_MAGIC.to_le_bytes());
        skippable.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        skippable.extend_from_slice(&metadata);
        let offset = self.written;
        self.poisoned = true;
        self.file.write_all(&compressed).await?;
        self.file.write_all(&skippable).await?;
        // Tokio buffers writes. Flush observes their errors and completion
        // before fsync can establish a durable publication boundary.
        self.file.flush().await?;
        self.file.sync_data().await?;
        let frame = LogFrame {
            start: self.uncompressed_offset,
            end: self.uncompressed_offset.saturating_add(input_len),
            compressed_start: offset,
            compressed_end: offset
                .saturating_add(compressed.len() as u64)
                .saturating_add(skippable.len() as u64),
            captured_at,
        };
        let mut line = serde_json::to_vec(&frame).map_err(io::Error::other)?;
        line.push(b'\n');
        self.written = frame.compressed_end;
        self.uncompressed_offset = frame.end;
        if control {
            self.control_bytes = self.control_bytes.saturating_add(input_len);
        }
        self.poisoned = false;
        if let Some(index) = &mut self.index {
            let result = async {
                index.write_all(&line).await?;
                index.flush().await?;
                index.sync_data().await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "execution index unavailable; owner remains durable");
                self.index = None;
            }
        }
        Ok(())
    }

    async fn write_captured_record(
        &mut self,
        jsonl_line: &str,
        captured_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> io::Result<()> {
        self.write_frame(jsonl_line, captured_at, false).await
    }

    async fn publish_unavailable_marker(&mut self) -> std::io::Result<()> {
        if self.marker_written {
            return Ok(());
        }
        self.marker_written = true;
        let marker = LogMsg::Stderr(
            "[cdesktop] execution recording stopped: blocked(disk-reserve)".to_owned(),
        );
        if let Ok(mut line) = serde_json::to_string(&marker) {
            line.push('\n');
            self.append_control_line(&line).await?;
        }
        Ok(())
    }
}

pub async fn read_execution_log_file(path: &Path) -> std::io::Result<String> {
    // Kept only for the legacy snapshot adapter. New consumers must use the
    // paged range API; this intentionally refuses instead of allocating a
    // whole multi-gigabyte transcript.
    if scan_owner(path).await?.uncompressed_bytes > MAX_EXECUTION_LOG_RANGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "execution snapshot exceeds page limit; use bounded snapshot or range API",
        ));
    }
    read_execution_log_range(path, 0, MAX_EXECUTION_LOG_RANGE_BYTES).await
}

pub struct LogSnapshot {
    pub jsonl: String,
    pub complete: bool,
}

/// A UI view contains whole records only. A large or unsealed source is
/// explicitly partial; the paged owner API remains the full-fidelity reader.
pub async fn read_execution_log_snapshot(path: &Path) -> io::Result<LogSnapshot> {
    let owner = scan_owner(path).await?;
    let end = owner
        .uncompressed_bytes
        .min(in_memory_log_bytes())
        .min(MAX_EXECUTION_LOG_RANGE_BYTES);
    let mut bytes = read_execution_log_range_bytes(path, 0, end).await?;
    let truncated = end < owner.uncompressed_bytes;
    if truncated {
        let record_end = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        bytes.truncate(record_end);
    }
    Ok(LogSnapshot {
        jsonl: String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
        complete: !truncated
            && owner.outcome == Some(CaptureOutcome::Complete)
            && !owner.legacy_capture_metadata,
    })
}

/// Reads the requested uncompressed byte range. Frame locators make ranges
/// stable across compression changes and permit a later external index to
/// reference evidence without owning a duplicate codec or transcript copy.
pub async fn read_execution_log_range(path: &Path, start: u64, end: u64) -> io::Result<String> {
    String::from_utf8(read_execution_log_range_bytes(path, start, end).await?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Reads exact original bytes. This is the native integration contract: ranges
/// are offsets in the decompressed producer stream, not UTF-8 character
/// positions or compressed offsets.
pub async fn read_execution_log_range_bytes(
    path: &Path,
    start: u64,
    end: u64,
) -> io::Result<Vec<u8>> {
    if end < start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "range end precedes start",
        ));
    }
    if end.saturating_sub(start) > MAX_EXECUTION_LOG_RANGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "execution log range exceeds page limit",
        ));
    }
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || read_range_blocking(&path, start, end))
        .await
        .map_err(io::Error::other)?
}

fn read_range_blocking(path: &Path, start: u64, end: u64) -> io::Result<Vec<u8>> {
    // The index is disposable. Validate every selected locator against the
    // checksummed owner; any cache damage falls back to bounded owner scanning.
    read_range_from_index(path, start, end)
        .or_else(|_| read_range_by_streaming_decode(path, start, end))
}

fn read_range_from_index(path: &Path, start: u64, end: u64) -> io::Result<Vec<u8>> {
    let index_path = process_log_frame_index_path(path);
    let frames = stream_intersecting_frames(&index_path, start, end)?;
    if !frames_cover_range(&frames, start, end) {
        // A sidecar may be absent or have a torn final write. The Zstd stream
        // remains the owner, so recover from it rather than claiming no logs.
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "incomplete execution index",
        ));
    }
    let mut file = io::BufReader::new(std::fs::File::open(path)?);
    let mut output = Vec::with_capacity((end - start) as usize);
    for frame in frames {
        if frame.compressed_end < frame.compressed_start
            || frame.end < frame.start
            || frame.compressed_end.saturating_sub(frame.compressed_start)
                > MAX_EXECUTION_LOG_RANGE_BYTES * 2
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid execution log frame index",
            ));
        }
        file.seek(SeekFrom::Start(frame.compressed_start))?;
        let owner = read_owner_frame(&mut file)?;
        if owner.locator.start != frame.start
            || owner.locator.end != frame.end
            || owner.locator.compressed_end != frame.compressed_end
            || owner.locator.captured_at != frame.captured_at
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "execution log frame length mismatch",
            ));
        }
        let from = start.saturating_sub(frame.start) as usize;
        let to = (end.min(frame.end) - frame.start) as usize;
        output.extend_from_slice(owner.bytes.get(from..to).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "execution log range outside frame",
            )
        })?);
    }
    Ok(output)
}

fn frames_cover_range(frames: &[LogFrame], start: u64, end: u64) -> bool {
    if start == end {
        return true;
    }
    let mut covered_until = start;
    for (index, frame) in frames.iter().enumerate() {
        if (index == 0 && frame.start > start)
            || (index > 0 && frame.start != covered_until)
            || frame.end <= covered_until
        {
            return false;
        }
        covered_until = frame.end;
        if covered_until >= end {
            return true;
        }
    }
    false
}

fn stream_intersecting_frames(path: &Path, start: u64, end: u64) -> io::Result<Vec<LogFrame>> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut frames = Vec::new();
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        let frame: LogFrame = match serde_json::from_str(&line) {
            Ok(frame) => frame,
            Err(_) => break,
        };
        if frame.end > start && frame.start < end {
            frames.push(frame);
        }
        if frame.start >= end {
            break;
        }
    }
    Ok(frames)
}

fn read_range_by_streaming_decode(path: &Path, start: u64, end: u64) -> io::Result<Vec<u8>> {
    let mut reader = io::BufReader::new(std::fs::File::open(path)?);
    let mut offset = 0u64;
    let mut output = Vec::with_capacity((end - start) as usize);
    while offset < end && !reader.fill_buf()?.is_empty() {
        let frame = read_owner_frame(&mut reader)?;
        if frame.locator.start != offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "noncontiguous execution owner",
            ));
        }
        if frame.locator.end > start {
            let from = start.saturating_sub(offset) as usize;
            let to = (end.min(frame.locator.end) - offset) as usize;
            output.extend_from_slice(&frame.bytes[from..to]);
        }
        offset = frame.locator.end;
    }
    Ok(output)
}

/// Streams a decompressed owner into SHA-256 without materialising it. Used by
/// migration before publication so legacy bytes are never pruned on a merely
/// plausible conversion.
pub async fn execution_log_sha256(path: &Path) -> io::Result<[u8; 32]> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        use sha2::Digest;

        let mut decoder = zstd::stream::read::Decoder::new(std::fs::File::open(path)?)?;
        let mut digest = sha2::Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = decoder.read(&mut buffer)?;
            if read == 0 {
                return Ok(digest.finalize().into());
            }
            digest.update(&buffer[..read]);
        }
    })
    .await
    .map_err(io::Error::other)?
}

#[derive(Default, Debug, serde::Serialize)]
pub struct OwnerSummary {
    pub uncompressed_bytes: u64,
    /// None means unsealed, never a claim that the process is still alive.
    pub outcome: Option<CaptureOutcome>,
    pub legacy_capture_metadata: bool,
    #[serde(skip)]
    control_bytes: u64,
}

struct DecodedFrame {
    locator: LogFrame,
    metadata: CapturedLogMetadata,
    bytes: Vec<u8>,
}

/// Read one bounded, checksummed owner frame and its checksummed metadata.
/// The byte position in this owner, never a sidecar counter, defines order.
fn read_owner_frame(reader: &mut io::BufReader<std::fs::File>) -> io::Result<DecodedFrame> {
    let compressed_start = reader.stream_position()?;
    let mut bytes = Vec::with_capacity(MAX_FRAME_UNCOMPRESSED_BYTES);
    {
        let mut decoder = zstd::stream::read::Decoder::with_buffer(&mut *reader)?.single_frame();
        decoder.window_log_max(23)?;
        decoder
            .take(MAX_FRAME_UNCOMPRESSED_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
    }
    if bytes.len() > MAX_FRAME_UNCOMPRESSED_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized execution frame",
        ));
    }
    let mut header = [0; 8];
    reader.read_exact(&mut header)?;
    if u32::from_le_bytes(header[..4].try_into().unwrap()) != ZSTD_SKIPPABLE_FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing capture metadata",
        ));
    }
    let size = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;
    if size > 4096 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized capture metadata",
        ));
    }
    let mut encoded = vec![0; size];
    reader.read_exact(&mut encoded)?;
    let mut decoder = zstd::stream::read::Decoder::new(&encoded[..])?;
    decoder.window_log_max(23)?;
    let mut metadata = Vec::new();
    decoder.take(4097).read_to_end(&mut metadata)?;
    if metadata.len() > 4096 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized decoded metadata",
        ));
    }
    let metadata: CapturedLogMetadata = serde_json::from_slice(&metadata)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let locator = LogFrame {
        start: metadata.capture_order,
        end: metadata
            .capture_order
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid source range"))?,
        compressed_start,
        compressed_end: reader.stream_position()?,
        captured_at: metadata.captured_at,
    };
    Ok(DecodedFrame {
        locator,
        metadata,
        bytes,
    })
}

pub async fn scan_owner(path: &Path) -> io::Result<OwnerSummary> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut reader = io::BufReader::new(std::fs::File::open(path)?);
        let mut summary = OwnerSummary::default();
        while !reader.fill_buf()?.is_empty() {
            let frame = read_owner_frame(&mut reader)?;
            if frame.locator.start != summary.uncompressed_bytes || summary.outcome.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "noncontiguous execution owner",
                ));
            }
            summary.uncompressed_bytes = frame.locator.end;
            summary.outcome = frame.metadata.outcome;
            summary.legacy_capture_metadata |= frame.metadata.captured_at.is_none();
            if frame.metadata.control {
                summary.control_bytes += frame.bytes.len() as u64;
            }
        }
        Ok(summary)
    })
    .await
    .map_err(io::Error::other)?
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
struct LogFrame {
    start: u64,
    end: u64,
    compressed_start: u64,
    compressed_end: u64,
    #[serde(default)]
    captured_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(test)]
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
    async fn captured_records_keep_the_original_bytes_and_bound_frame_memory() {
        // Capture metadata must not rewrite the provider payload: migrations
        // verify this same byte stream by hash before publishing replacements.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let payload = format!("{{\"payload\":\"{}\"}}\n", "é".repeat(40_000));
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        assert_eq!(
            writer.append_jsonl_line(&payload).await.unwrap(),
            LogAppend::Written
        );
        drop(writer);

        assert_eq!(
            read_execution_log_range_bytes(&path, 0, payload.len() as u64)
                .await
                .unwrap(),
            payload.as_bytes()
        );
        let mut owner = io::BufReader::new(std::fs::File::open(&path).unwrap());
        let first = read_owner_frame(&mut owner).unwrap();
        let second = read_owner_frame(&mut owner).unwrap();
        assert!(first.metadata.captured_at.is_some());
        assert_eq!(second.metadata.captured_at, first.metadata.captured_at);
        assert_eq!(first.metadata.capture_order, 0);
        assert_eq!(
            second.metadata.capture_order,
            MAX_FRAME_UNCOMPRESSED_BYTES as u64
        );
        assert_eq!(first.bytes.len(), MAX_FRAME_UNCOMPRESSED_BYTES);
        assert!(
            read_frame_index(&process_log_frame_index_path(&path))
                .await
                .unwrap()
                .len()
                > 1
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
    async fn missing_or_torn_sidecar_recovers_from_the_native_frames() {
        // The index accelerates reads but never owns evidence. A crash while
        // writing it must not turn existing transcript bytes into an empty log.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_control_line("recover\n").await.unwrap();
        drop(writer);
        let index = process_log_frame_index_path(&path);
        tokio::fs::write(&index, "{torn").await.unwrap();
        assert_eq!(
            read_execution_log_range(&path, 0, 8).await.unwrap(),
            "recover\n"
        );
        tokio::fs::remove_file(index).await.unwrap();
        assert_eq!(
            read_execution_log_range(&path, 0, 8).await.unwrap(),
            "recover\n"
        );
    }

    #[tokio::test]
    async fn stale_sidecar_never_hides_a_published_owner_tail() {
        // A crash after owner fsync but before index publication leaves a
        // valid frame without its locator. Reopening or reading must retain it
        // rather than returning a convincing but incomplete prefix.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_control_line("first\n").await.unwrap();
        writer.append_control_line("second\n").await.unwrap();
        drop(writer);
        let index = process_log_frame_index_path(&path);
        let first_line = tokio::fs::read_to_string(&index)
            .await
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .to_owned();
        tokio::fs::write(&index, format!("{first_line}\n"))
            .await
            .unwrap();
        assert_eq!(
            read_execution_log_range(&path, 0, 13).await.unwrap(),
            "first\nsecond\n"
        );
    }

    #[tokio::test]
    async fn reopen_derives_next_range_from_owner_not_stale_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_control_line("first\n").await.unwrap();
        writer.append_control_line("second\n").await.unwrap();
        drop(writer);
        let index = process_log_frame_index_path(&path);
        let first = tokio::fs::read_to_string(&index)
            .await
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .to_owned();
        tokio::fs::write(&index, format!("{first}\n"))
            .await
            .unwrap();
        let mut reopened = ExecutionLogWriter::open(path.clone()).await.unwrap();
        reopened.append_control_line("third\n").await.unwrap();
        drop(reopened);
        assert_eq!(
            read_execution_log_range(&path, 0, 19).await.unwrap(),
            "first\nsecond\nthird\n"
        );
    }

    #[tokio::test]
    async fn range_api_refuses_unbounded_requests() {
        // Endpoint callers must page rather than allocating their requested
        // span, regardless of the total retained transcript size.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proc.jsonl.zst");
        let writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        drop(writer);
        assert_eq!(
            read_execution_log_range(&path, 0, MAX_EXECUTION_LOG_RANGE_BYTES + 1)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
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

    #[tokio::test]
    async fn control_allowance_and_order_survive_reopen_without_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::with_free_disk_reserve(path.clone(), 0)
            .await
            .unwrap();
        writer
            .append_jsonl_line(&"a".repeat(MAX_FRAME_UNCOMPRESSED_BYTES + 17))
            .await
            .unwrap();
        writer
            .append_control_line(&"c".repeat(CONTROL_OVERDRAFT_BYTES as usize))
            .await
            .unwrap();
        drop(writer);
        tokio::fs::remove_file(process_log_frame_index_path(&path))
            .await
            .unwrap();
        let mut reopened = ExecutionLogWriter::with_free_disk_reserve(path.clone(), 0)
            .await
            .unwrap();
        assert_eq!(
            reopened.append_control_line("x").await.unwrap(),
            LogAppend::Blocked
        );
        reopened.append_jsonl_line("still capturing").await.unwrap();
        drop(reopened);
        let summary = scan_owner(&path).await.unwrap();
        assert_eq!(summary.control_bytes, CONTROL_OVERDRAFT_BYTES);
        assert_eq!(
            summary.uncompressed_bytes,
            2 * MAX_FRAME_UNCOMPRESSED_BYTES as u64 + 17 + 15
        );
    }

    #[tokio::test]
    async fn only_one_writer_can_own_a_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        assert!(ExecutionLogWriter::open(path.clone()).await.is_err());
        drop(writer);
        assert!(ExecutionLogWriter::open(path).await.is_ok());
    }

    #[tokio::test]
    async fn failed_cache_write_cannot_lose_committed_owner_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.index = Some(
            tokio::fs::File::open(process_log_frame_index_path(&path))
                .await
                .unwrap(),
        );
        writer.append_jsonl_line("first\n").await.unwrap();
        assert!(writer.index.is_none());
        writer.append_jsonl_line("second\n").await.unwrap();
        drop(writer);
        assert_eq!(
            read_execution_log_range(&path, 0, 13).await.unwrap(),
            "first\nsecond\n"
        );
        assert_eq!(scan_owner(&path).await.unwrap().uncompressed_bytes, 13);
    }

    #[tokio::test]
    async fn failed_owner_write_poisoning_prevents_stale_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_jsonl_line("retained\n").await.unwrap();
        let owned_file = std::mem::replace(
            &mut writer.file,
            tokio::fs::File::open(&path).await.unwrap(),
        );
        assert!(writer.append_jsonl_line("write fails\n").await.is_err());
        writer.file = owned_file;
        assert!(writer.append_jsonl_line("must not append\n").await.is_err());
        assert_eq!(
            read_execution_log_range(&path, 0, 9).await.unwrap(),
            "retained\n"
        );
    }

    #[tokio::test]
    async fn torn_owner_is_preserved_and_refuses_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_jsonl_line("retained\n").await.unwrap();
        drop(writer);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(&[0x28, 0xb5, 0x2f]).await.unwrap();
        file.sync_all().await.unwrap();
        let length = file.metadata().await.unwrap().len();
        assert!(ExecutionLogWriter::open(path.clone()).await.is_err());
        assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), length);
        assert_eq!(
            read_execution_log_range(&path, 0, 9).await.unwrap(),
            "retained\n"
        );
    }

    #[tokio::test]
    async fn a_valid_but_wrong_locator_cannot_relabel_owner_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_jsonl_line("first!\n").await.unwrap();
        writer.append_jsonl_line("second\n").await.unwrap();
        drop(writer);
        let index = process_log_frame_index_path(&path);
        let mut frames = read_frame_index(&index).await.unwrap();
        frames[0].compressed_start = frames[1].compressed_start;
        frames[0].compressed_end = frames[1].compressed_end;
        tokio::fs::write(
            &index,
            format!("{}\n", serde_json::to_string(&frames[0]).unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(
            read_execution_log_range(&path, 0, 7).await.unwrap(),
            "first!\n"
        );
    }

    #[tokio::test]
    async fn completion_is_explicit_durable_and_adds_no_producer_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_jsonl_line("record\n").await.unwrap();
        assert_eq!(scan_owner(&path).await.unwrap().outcome, None);
        writer.finish(CaptureOutcome::Complete).await.unwrap();
        assert!(writer.append_jsonl_line("late").await.is_err());
        drop(writer);
        assert_eq!(
            scan_owner(&path).await.unwrap().outcome,
            Some(CaptureOutcome::Complete)
        );
        assert_eq!(read_execution_log_file(&path).await.unwrap(), "record\n");
        let mut reopened = ExecutionLogWriter::open(path.clone()).await.unwrap();
        assert!(reopened.append_jsonl_line("late").await.is_err());
    }

    #[tokio::test]
    async fn damaged_metadata_is_not_trusted_even_when_raw_bytes_decode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer.append_jsonl_line("record\n").await.unwrap();
        drop(writer);
        let mut bytes = tokio::fs::read(&path).await.unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        tokio::fs::write(&path, &bytes).await.unwrap();
        assert_eq!(zstd::stream::decode_all(&bytes[..]).unwrap(), b"record\n");
        assert!(scan_owner(&path).await.is_err());
        assert!(read_execution_log_range(&path, 0, 7).await.is_err());
    }

    #[tokio::test]
    async fn bounded_ui_snapshot_never_returns_partial_json_or_utf8_as_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.zst");
        let mut writer = ExecutionLogWriter::with_free_disk_reserve(path.clone(), 0)
            .await
            .unwrap();
        let line = format!(
            "{}\n",
            serde_json::to_string(&LogMsg::Stdout("é".repeat(40_000))).unwrap()
        );
        for _ in 0..16 {
            writer.append_jsonl_line(&line).await.unwrap();
        }
        writer.finish(CaptureOutcome::Complete).await.unwrap();
        drop(writer);
        assert!(
            read_execution_log_file(&path).await.is_err(),
            "whole-file adapter must refuse oversize sources"
        );
        let snapshot = read_execution_log_snapshot(&path).await.unwrap();
        assert!(!snapshot.complete);
        assert!(snapshot.jsonl.len() <= MAX_EXECUTION_LOG_RANGE_BYTES as usize);
        assert!(snapshot.jsonl.ends_with('\n'));
        assert!(!snapshot.jsonl.is_empty());
        for record in snapshot.jsonl.lines() {
            serde_json::from_str::<LogMsg>(record).unwrap();
        }
        assert!(line.repeat(16).starts_with(&snapshot.jsonl));
    }

    #[tokio::test]
    async fn bounded_view_distinguishes_unsealed_complete_and_legacy_capture() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("current.zst");
        let mut writer = ExecutionLogWriter::open(path.clone()).await.unwrap();
        writer
            .append_jsonl_line("{\"Stdout\":\"ok\"}\n")
            .await
            .unwrap();
        assert!(!read_execution_log_snapshot(&path).await.unwrap().complete);
        writer.finish(CaptureOutcome::Complete).await.unwrap();
        assert!(read_execution_log_snapshot(&path).await.unwrap().complete);
        let old_path = dir.path().join("legacy.zst");
        let mut old = ExecutionLogWriter::open(old_path.clone()).await.unwrap();
        old.append_legacy_jsonl_line("{\"Stdout\":\"old\"}\n")
            .await
            .unwrap();
        old.finish(CaptureOutcome::LegacyUnknown).await.unwrap();
        assert!(
            !read_execution_log_snapshot(&old_path)
                .await
                .unwrap()
                .complete
        );
        assert!(scan_owner(&old_path).await.unwrap().legacy_capture_metadata);
    }
}
