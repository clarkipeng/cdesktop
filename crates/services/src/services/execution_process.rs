use std::{
    io::{IsTerminal, Write},
    sync::Arc,
};

use anyhow::{Context, Result};
use db::{
    DBService,
    models::{
        coding_agent_turn::CodingAgentTurn, execution_process::ExecutionProcess,
        execution_process_logs::ExecutionProcessLogs,
    },
};
use futures::{StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use utils::{
    execution_logs::{
        CaptureOutcome, ExecutionLogWriter, LogAppend, execution_log_sha256,
        legacy_process_log_file_path_in_root, process_log_file_path,
    },
    log_msg::LogMsg,
    msg_store::MsgStore,
};
use uuid::Uuid;

pub async fn migrate_execution_logs_to_files() -> Result<()> {
    let pool = DBService::new_migration_pool()
        .await
        .map_err(|e| anyhow::anyhow!("Migration DB pool error: {}", e))?;

    if !ExecutionProcessLogs::has_any(&pool).await? {
        return Ok(());
    }

    let is_tty = std::io::stderr().is_terminal();
    if is_tty {
        let _ = writeln!(
            std::io::stderr(),
            "Performing one time database migration to move logs from SQLite to flat file to improve performance, data remains local, may take a few minutes, please don't exit while this process is running..."
        );
    }

    let pb = if is_tty {
        Some(new_spinner("Migrating"))
    } else {
        None
    };

    let total_processes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let count_task = {
        let pool = pool.clone();
        let pb = pb.clone();
        let total_processes = total_processes.clone();
        tokio::spawn(async move {
            if let Ok(count) = ExecutionProcessLogs::count_distinct_processes(&pool).await {
                total_processes.store(count as usize, std::sync::atomic::Ordering::Relaxed);
                if let Some(pb) = pb {
                    pb.set_length(count as u64);
                    pb.set_style(
                        ProgressStyle::default_bar()
                            .template("{bar:36.yellow} {percent:>3}% {msg:<12.dim}")
                            .unwrap_or_else(|_| ProgressStyle::default_bar())
                            .progress_chars("■⬝"),
                    );
                }
            }
        })
    };

    let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    ExecutionProcessLogs::stream_distinct_processes(&pool)
        .map_err(anyhow::Error::from)
        .map(|res| {
            let pool = pool.clone();
            let pb = pb.clone();
            let completed = completed.clone();
            let total_processes = total_processes.clone();
            async move {
                let p = res?;

                let path = process_log_file_path(p.session_id, p.execution_id);
                if path.exists()
                    && utils::execution_logs::process_log_frame_index_path(&path).exists()
                {
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    return Ok::<(), anyhow::Error>(());
                }

                // Publish only a verified pair. A crash before either rename
                // leaves the database source authoritative; a crash after
                // publication leaves it redundant but readable.
                let temp_path = path.with_extension("zst.tmp");
                let temp_index_path =
                    utils::execution_logs::process_log_frame_index_path(&temp_path);
                let final_index_path = utils::execution_logs::process_log_frame_index_path(&path);
                let _ = tokio::fs::remove_file(&temp_path).await;
                let _ = tokio::fs::remove_file(&temp_index_path).await;
                if !path.exists() {
                    // An index without its compressed owner was never
                    // published. The database rows remain authoritative.
                    let _ = tokio::fs::remove_file(&final_index_path).await;
                }
                let mut writer = ExecutionLogWriter::open(temp_path.clone()).await?;
                let mut source_hash = Sha256::new();

                let mut logs_stream =
                    ExecutionProcessLogs::stream_log_lines_by_execution_id(&pool, &p.execution_id);
                let mut has_logs = false;
                while let Some(log_res) = logs_stream.next().await {
                    let log = log_res?;
                    has_logs = true;
                    let mut line = log;
                    if !line.ends_with('\n') {
                        line.push('\n');
                    }
                    source_hash.update(line.as_bytes());
                    match writer.append_legacy_jsonl_line(&line).await? {
                        LogAppend::Written => {}
                        LogAppend::Blocked | LogAppend::Unavailable => {
                            anyhow::bail!(
                                "cannot migrate execution {}: recording storage unavailable",
                                p.execution_id
                            );
                        }
                    }
                }

                if !has_logs {
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    return Ok::<(), anyhow::Error>(());
                }

                drop(writer);
                if execution_log_sha256(&temp_path).await? != source_hash.finalize().as_slice() {
                    anyhow::bail!(
                        "cannot migrate execution {}: decompressed hash mismatch",
                        p.execution_id
                    );
                }
                tokio::fs::rename(&temp_index_path, &final_index_path).await?;
                tokio::fs::rename(&temp_path, &path).await?;

                let c = completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                if let Some(pb) = &pb {
                    pb.inc(1);
                } else if c.is_multiple_of(100) {
                    let t = total_processes.load(std::sync::atomic::Ordering::Relaxed);
                    let _ = writeln!(
                        std::io::stderr(),
                        "sqlite-migration:{}",
                        if t > 0 {
                            (c * 100 / t).to_string()
                        } else {
                            "?".to_string()
                        }
                    );
                }

                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(64)
        .try_collect::<Vec<_>>()
        .await?;

    let _ = count_task.await;

    if let Some(pb) = pb {
        pb.finish_and_clear();
    } else {
        let _ = writeln!(std::io::stderr(), "sqlite-migration:done");
    }

    let vacuum_pb = if is_tty {
        Some(new_spinner("Compacting"))
    } else {
        let _ = writeln!(std::io::stderr(), "Compacting database...");
        None
    };

    ExecutionProcessLogs::delete_all(&pool).await?;
    sqlx::query("VACUUM").execute(&pool).await?;

    if let Some(pb) = vacuum_pb {
        pb.finish_and_clear();
    }

    let _ = writeln!(std::io::stderr(), "Database migration complete.");

    pool.close().await;

    Ok(())
}

pub async fn remove_session_process_logs(session_id: Uuid) -> Result<()> {
    let dir = utils::execution_logs::process_logs_session_dir(session_id);
    match tokio::fs::remove_dir_all(&dir).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            Err(e).with_context(|| format!("remove session process logs at {}", dir.display()))
        }
    }
}

pub struct RawLogMessages {
    pub messages: Vec<LogMsg>,
    pub complete: bool,
}

pub async fn load_raw_log_messages(
    pool: &SqlitePool,
    execution_id: Uuid,
) -> Option<RawLogMessages> {
    let snapshot = match read_execution_logs_for_execution(pool, execution_id).await {
        Ok(Some(snapshot)) => snapshot,
        Err(error) => {
            tracing::warn!(%execution_id, %error, "execution source unavailable");
            return None; // A corrupt owner must not silently fall back to stale originals.
        }
        Ok(None) => {
            // Bound both each SQL result and the accumulated UI view. Legacy
            // capture predates coverage instrumentation, even if all rows fit.
            let mut rows = sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT substr(CAST(logs AS BLOB), 1, ?2) FROM execution_process_logs WHERE execution_id = ?1 ORDER BY inserted_at, rowid"
            ).bind(execution_id).bind(utils::execution_logs::MAX_EXECUTION_LOG_RANGE_BYTES as i64 + 1).fetch(pool);
            let mut bytes = Vec::new();
            while let Some(row) = rows.next().await {
                let chunk = match row {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        tracing::warn!(%execution_id, %error, "legacy SQL logs unavailable");
                        return None;
                    }
                };
                let remaining =
                    utils::execution_logs::MAX_EXECUTION_LOG_RANGE_BYTES as usize + 1 - bytes.len();
                bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if bytes.len() > utils::execution_logs::MAX_EXECUTION_LOG_RANGE_BYTES as usize {
                    break;
                }
                if !bytes.ends_with(b"\n") {
                    bytes.push(b'\n');
                }
            }
            if bytes.is_empty() {
                return None;
            }
            legacy_snapshot(bytes).ok()?
        }
    };
    let mut complete = snapshot.complete;
    let messages = snapshot
        .jsonl
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| match serde_json::from_str(line) {
            Ok(message) => Some(message),
            Err(error) => {
                complete = false;
                tracing::warn!(%execution_id, %error, "invalid record in bounded UI snapshot");
                None
            }
        })
        .collect();
    Some(RawLogMessages { messages, complete })
}

fn legacy_snapshot(mut bytes: Vec<u8>) -> std::io::Result<utils::execution_logs::LogSnapshot> {
    let limit = utils::execution_logs::MAX_EXECUTION_LOG_RANGE_BYTES as usize;
    if bytes.len() > limit {
        bytes.truncate(limit);
        let end = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        bytes.truncate(end);
    }
    Ok(utils::execution_logs::LogSnapshot {
        jsonl: String::from_utf8(bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
        complete: false,
    })
}

pub async fn append_log_message(session_id: Uuid, execution_id: Uuid, msg: &LogMsg) -> Result<()> {
    let mut log_writer = ExecutionLogWriter::new_for_execution(session_id, execution_id)
        .await
        .with_context(|| format!("create log writer for execution {}", execution_id))?;
    let json_line = serde_json::to_string(msg)
        .with_context(|| format!("serialize log message for execution {}", execution_id))?;
    let mut json_line_with_newline = json_line;
    json_line_with_newline.push('\n');
    // Single cdesktop-owned messages (start errors, setup-required hints) are
    // control lines: they draw on the writer's overdraft so the byte cap never
    // swallows the explanation the user needs. `Blocked` past the overdraft is
    // the cap doing its job, not an error.
    log_writer
        .append_control_line(&json_line_with_newline)
        .await
        .with_context(|| format!("append log message for execution {}", execution_id))?;
    Ok(())
}

/// Requests termination on recording failure. The native exit monitor remains
/// the owner of terminal state; a recorder must never wait on its own monitor.
pub type RecordingStop = Box<dyn FnOnce() -> futures::future::BoxFuture<'static, ()> + Send>;

/// Capture from the producer before the disposable UI mirror. Awaiting each
/// durable append gives the OS pipe backpressure without a second output queue.
pub async fn capture_raw_logs(
    mut writer: ExecutionLogWriter,
    mut stream: futures::stream::BoxStream<'static, Result<LogMsg, std::io::Error>>,
    store: Arc<MsgStore>,
    on_failure: RecordingStop,
) {
    let result: Result<(), std::io::Error> = async {
        while let Some(message) = stream.next().await {
            let message = message?;
            if matches!(message, LogMsg::Finished) {
                break;
            }
            let mut line = serde_json::to_string(&message).map_err(std::io::Error::other)?;
            line.push('\n');
            match writer.append_jsonl_line(&line).await? {
                LogAppend::Written => store.push(message),
                LogAppend::Blocked | LogAppend::Unavailable => {
                    return Err(std::io::Error::other("execution recording unavailable"));
                }
            }
        }
        writer.finish(CaptureOutcome::Complete).await
    }
    .await;

    if let Err(error) = result {
        tracing::error!(%error, path = %writer.path().display(), "execution recording failed");
        if let Err(marker_error) = writer.finish(CaptureOutcome::Unavailable).await {
            tracing::error!(%marker_error, "capture outcome unavailable; owner remains unsealed");
        }
        on_failure().await;
    }
}

/// Observe adapter identities separately from raw capture. The subscription is
/// created by the caller before the executor starts, not inside the spawned task.
pub fn spawn_session_metadata_sync(
    mut stream: futures::stream::BoxStream<'static, Result<LogMsg, std::io::Error>>,
    db: DBService,
    execution_id: Uuid,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = stream.next().await {
            let result = match message {
                Ok(LogMsg::SessionId(id)) => {
                    CodingAgentTurn::update_agent_session_id(&db.pool, execution_id, &id).await
                }
                Ok(LogMsg::MessageId(id)) => {
                    CodingAgentTurn::update_agent_message_id(&db.pool, execution_id, &id).await
                }
                Ok(LogMsg::Finished) => break,
                Err(error) => {
                    tracing::error!(%execution_id, %error, "session metadata stream incomplete");
                    break;
                }
                _ => continue,
            };
            if let Err(error) = result {
                tracing::error!(%execution_id, %error, "failed to retain adapter identity");
            }
        }
    })
}

async fn read_execution_logs_for_execution(
    pool: &SqlitePool,
    execution_id: Uuid,
) -> Result<Option<utils::execution_logs::LogSnapshot>> {
    let session_id = if let Some(process) = ExecutionProcess::find_by_id(pool, execution_id).await?
    {
        process.session_id
    } else {
        return Ok(None);
    };
    let path = process_log_file_path(session_id, execution_id);

    match tokio::fs::metadata(&path).await {
        Ok(_) => Ok(Some(
            utils::execution_logs::read_execution_log_snapshot(&path)
                .await
                .with_context(|| format!("read execution log file for execution {execution_id}"))?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Legacy files remain readable until the verified startup
            // migration publishes their compressed native replacement.
            let legacy_path = legacy_process_log_file_path_in_root(
                &utils::assets::asset_dir(),
                session_id,
                execution_id,
            );
            match tokio::fs::File::open(&legacy_path).await {
                Ok(file) => {
                    use tokio::io::AsyncReadExt;
                    let mut bytes = Vec::new();
                    file.take(utils::execution_logs::MAX_EXECUTION_LOG_RANGE_BYTES + 1)
                        .read_to_end(&mut bytes)
                        .await?;
                    Ok(Some(legacy_snapshot(bytes)?))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error).with_context(|| {
                    format!("read legacy execution log file for execution {execution_id}")
                }),
            }
        }
        Err(e) => Err(e).with_context(|| {
            format!(
                "check execution log file exists for execution {execution_id} at {}",
                path.display()
            )
        }),
    }
}

fn new_spinner(message: &'static str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.yellow} {msg:<12.dim}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner())
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
    );
    pb.set_message(message);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::StreamExt as _;
    use utils::execution_logs::{ExecutionLogWriter, read_execution_log_file};

    use super::*;

    #[test]
    fn legacy_view_reports_unknown_coverage_and_whole_records() {
        let line = "{\"Stdout\":\"retained\"}\n";
        let mut bytes = line.as_bytes().to_vec();
        bytes.extend_from_slice(&vec![
            b'x';
            utils::execution_logs::MAX_EXECUTION_LOG_RANGE_BYTES
                as usize
        ]);
        let snapshot = legacy_snapshot(bytes).unwrap();
        assert!(!snapshot.complete);
        assert_eq!(snapshot.jsonl, line);
        assert!(!legacy_snapshot(line.as_bytes().to_vec()).unwrap().complete);
    }

    fn scripted(
        messages: Vec<LogMsg>,
    ) -> futures::stream::BoxStream<'static, Result<LogMsg, std::io::Error>> {
        futures::stream::iter(messages.into_iter().map(Ok)).boxed()
    }

    #[tokio::test]
    async fn recording_refusal_stops_the_process_tree_once() {
        // A disk-reserve refusal must not let an agent continue with evidence
        // silently missing. The owned process tree stops once, however many
        // messages arrive after recording became unavailable.
        let dir = tempfile::tempdir().unwrap();
        let writer =
            ExecutionLogWriter::with_free_disk_reserve(dir.path().join("proc.jsonl.zst"), u64::MAX)
                .await
                .unwrap();

        let stops = Arc::new(AtomicUsize::new(0));
        let counter = stops.clone();
        let on_log_limit: RecordingStop = Box::new(move || {
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        });

        capture_raw_logs(
            writer,
            scripted(vec![
                LogMsg::Stdout("a".repeat(64)),
                LogMsg::Stdout("b".repeat(64)),
                LogMsg::Stdout("c".repeat(64)),
                LogMsg::Finished,
            ]),
            Arc::new(MsgStore::new()),
            on_log_limit,
        )
        .await;

        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn healthy_recording_never_stops_the_process() {
        // A healthy execution must not be killed by the evidence writer.
        let dir = tempfile::tempdir().unwrap();
        let writer = ExecutionLogWriter::open(dir.path().join("proc.jsonl.zst"))
            .await
            .unwrap();

        let stops = Arc::new(AtomicUsize::new(0));
        let counter = stops.clone();
        let on_log_limit: RecordingStop = Box::new(move || {
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        });

        capture_raw_logs(
            writer,
            scripted(vec![LogMsg::Stdout("small".into()), LogMsg::Finished]),
            Arc::new(MsgStore::new()),
            on_log_limit,
        )
        .await;

        assert_eq!(stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn producer_bytes_survive_disposable_ui_eviction() {
        // Capture has no broadcast subscriber: every producer record reaches
        // the durable owner even when the entire UI history rolls over.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capture.jsonl.zst");
        let writer = ExecutionLogWriter::with_free_disk_reserve(path.clone(), 0)
            .await
            .unwrap();
        let store = Arc::new(MsgStore::new());
        let messages: Vec<_> = (0..48)
            .map(|i| LogMsg::Stdout(format!("{i}:{}", "a".repeat(64 * 1024))))
            .collect();
        let expected = messages
            .iter()
            .map(|message| format!("{}\n", serde_json::to_string(message).unwrap()))
            .collect::<String>();
        capture_raw_logs(
            writer,
            scripted(messages),
            store.clone(),
            Box::new(|| Box::pin(async { panic!("healthy capture failed") })),
        )
        .await;
        assert!(
            store.get_history().len() < 48,
            "fixture must exceed UI capacity"
        );
        assert_eq!(
            execution_log_sha256(&path).await.unwrap().as_slice(),
            Sha256::digest(expected.as_bytes()).as_slice()
        );
    }

    #[tokio::test]
    async fn read_failure_stops_before_publishing_unrecorded_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capture.jsonl.zst");
        let writer = ExecutionLogWriter::with_free_disk_reserve(path.clone(), 0)
            .await
            .unwrap();
        let store = Arc::new(MsgStore::new());
        let stops = Arc::new(AtomicUsize::new(0));
        let counter = stops.clone();
        let stream = futures::stream::iter(vec![
            Ok(LogMsg::Stdout("retained".into())),
            Err(std::io::Error::other("pipe failed")),
            Ok(LogMsg::Stdout("not read".into())),
        ])
        .boxed();
        capture_raw_logs(
            writer,
            stream,
            store.clone(),
            Box::new(move || {
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                })
            }),
        )
        .await;
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        assert_eq!(store.get_history().len(), 1);
        assert_eq!(
            read_execution_log_file(&path).await.unwrap(),
            "{\"Stdout\":\"retained\"}\n"
        );
    }
}
