use std::{
    collections::HashMap,
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
use sqlx::SqlitePool;
use tokio::{io::AsyncWriteExt, sync::RwLock, task::JoinHandle};
use utils::{
    assets::prod_asset_dir_path,
    execution_logs::{
        ExecutionLogWriter, LogAppend, process_log_file_path, process_log_file_path_in_root,
        read_execution_log_file,
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
                if path.exists() {
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    return Ok::<(), anyhow::Error>(());
                }

                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let temp_path = path.with_extension("jsonl.tmp");
                let mut file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&temp_path)
                    .await?;

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
                    file.write_all(line.as_bytes()).await?;
                }

                if !has_logs {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    return Ok::<(), anyhow::Error>(());
                }

                file.sync_all().await?;
                tokio::fs::rename(temp_path, path).await?;

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

pub async fn load_raw_log_messages(pool: &SqlitePool, execution_id: Uuid) -> Option<Vec<LogMsg>> {
    if let Some(jsonl) = read_execution_logs_for_execution(pool, execution_id)
        .await
        .inspect_err(|e| {
            tracing::warn!(
                "Failed to read execution log file for execution {}: {:#}",
                execution_id,
                e
            );
        })
        .ok()
        .flatten()
    {
        let messages = utils::execution_logs::parse_log_jsonl_lossy(execution_id, &jsonl);
        if !messages.is_empty() {
            return Some(messages);
        }
    }

    let db_log_records = match ExecutionProcessLogs::find_by_execution_id(pool, execution_id).await
    {
        Ok(records) if !records.is_empty() => records,
        Ok(_) => return None,
        Err(e) => {
            tracing::error!(
                "Failed to fetch DB logs for execution {}: {}",
                execution_id,
                e
            );
            return None;
        }
    };

    match ExecutionProcessLogs::parse_logs(&db_log_records) {
        Ok(msgs) => Some(msgs),
        Err(e) => {
            tracing::error!(
                "Failed to parse DB logs for execution {}: {}",
                execution_id,
                e
            );
            None
        }
    }
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

/// Invoked once when an execution's durable log hits its byte cap.
///
/// The implementor stops the owned process tree. Terminalization deliberately
/// stays on the normal exit-monitor path so the exactly-once compare-and-set
/// release keeps being the only writer of the terminal row.
pub type LogLimitStop = Box<dyn FnOnce() -> futures::future::BoxFuture<'static, ()> + Send>;

pub fn spawn_stream_raw_logs_to_storage(
    msg_stores: Arc<RwLock<HashMap<Uuid, Arc<MsgStore>>>>,
    db: DBService,
    execution_id: Uuid,
    session_id: Uuid,
    on_log_limit: Option<LogLimitStop>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let log_writer = match ExecutionLogWriter::new_for_execution(session_id, execution_id).await
        {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(
                    "Failed to create log file writer for execution {}: {}",
                    execution_id,
                    e
                );
                return;
            }
        };

        let store = {
            let map = msg_stores.read().await;
            map.get(&execution_id).cloned()
        };

        if let Some(store) = store {
            stream_logs_to_writer(
                log_writer,
                store.history_plus_stream(),
                db,
                execution_id,
                on_log_limit,
            )
            .await;
        }
    })
}

/// Drains `stream` into `log_writer`, firing `on_log_limit` the first time the
/// writer's byte cap refuses a line.
///
/// Split out of the spawn so the byte-cap stop is testable against a small cap
/// and a scripted stream, with no live child process.
async fn stream_logs_to_writer(
    mut log_writer: ExecutionLogWriter,
    mut stream: futures::stream::BoxStream<'static, Result<LogMsg, std::io::Error>>,
    db: DBService,
    execution_id: Uuid,
    mut on_log_limit: Option<LogLimitStop>,
) {
    while let Some(Ok(msg)) = stream.next().await {
        match &msg {
            LogMsg::Stdout(_) | LogMsg::Stderr(_) => match serde_json::to_string(&msg) {
                Ok(jsonl_line) => {
                    let mut jsonl_line_with_newline = jsonl_line;
                    jsonl_line_with_newline.push('\n');

                    match log_writer.append_jsonl_line(&jsonl_line_with_newline).await {
                        Ok(LogAppend::Written) => {}
                        Ok(LogAppend::Blocked) => {
                            // A limit that only drops logs is a log eater: the
                            // agent keeps burning tokens and disk with nothing
                            // recorded. The first block stops the process tree.
                            if let Some(stop) = on_log_limit.take() {
                                tracing::warn!(
                                    "Execution {} log hit its byte cap; stopping the process tree: blocked(limit)",
                                    execution_id
                                );
                                stop().await;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to append log line for execution {}: {}",
                                execution_id,
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to serialize log message for execution {}: {}",
                        execution_id,
                        e
                    );
                }
            },
            LogMsg::SessionId(agent_session_id) => {
                if let Err(e) = CodingAgentTurn::update_agent_session_id(
                    &db.pool,
                    execution_id,
                    agent_session_id,
                )
                .await
                {
                    tracing::error!(
                        "Failed to update agent_session_id {} for execution process {}: {}",
                        agent_session_id,
                        execution_id,
                        e
                    );
                }
            }
            LogMsg::MessageId(agent_message_id) => {
                if let Err(e) = CodingAgentTurn::update_agent_message_id(
                    &db.pool,
                    execution_id,
                    agent_message_id,
                )
                .await
                {
                    tracing::error!(
                        "Failed to update agent_message_id {} for execution process {}: {}",
                        agent_message_id,
                        execution_id,
                        e
                    );
                }
            }
            LogMsg::Finished => {
                break;
            }
            LogMsg::JsonPatch(_) | LogMsg::Ready => continue,
        }
    }
}

async fn read_execution_logs_for_execution(
    pool: &SqlitePool,
    execution_id: Uuid,
) -> Result<Option<String>> {
    let session_id = if let Some(process) = ExecutionProcess::find_by_id(pool, execution_id).await?
    {
        process.session_id
    } else {
        return Ok(None);
    };
    let path = process_log_file_path(session_id, execution_id);

    match tokio::fs::metadata(&path).await {
        Ok(_) => Ok(Some(read_execution_log_file(&path).await.with_context(
            || format!("read execution log file for execution {execution_id}"),
        )?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if cfg!(debug_assertions) {
                // Convenience for local development with a clone of a prod db. Read only access to prod logs.
                let prod_path =
                    process_log_file_path_in_root(&prod_asset_dir_path(), session_id, execution_id);
                match read_execution_log_file(&prod_path).await {
                    Ok(contents) => return Ok(Some(contents)),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!(
                                "read execution log file for execution {execution_id} from {}",
                                prod_path.display()
                            )
                        });
                    }
                }
            }
            Ok(None)
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
    use utils::execution_logs::ExecutionLogWriter;

    use super::*;

    async fn scratch_db() -> DBService {
        // The stream only touches the pool for SessionId/MessageId messages,
        // which this test never sends.
        DBService {
            pool: sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap(),
        }
    }

    fn scripted(
        messages: Vec<LogMsg>,
    ) -> futures::stream::BoxStream<'static, Result<LogMsg, std::io::Error>> {
        futures::stream::iter(messages.into_iter().map(Ok)).boxed()
    }

    #[tokio::test]
    async fn byte_cap_stops_the_process_tree_once() {
        // The H1 defect: hitting the cap used to drop the line and let the
        // agent keep running, so it burned tokens and disk while producing a
        // log nobody was recording. The first blocked append must stop the
        // owned process tree - and only once, however many lines follow.
        let dir = tempfile::tempdir().unwrap();
        let writer = ExecutionLogWriter::with_max_bytes(dir.path().join("proc.jsonl"), 24)
            .await
            .unwrap();

        let stops = Arc::new(AtomicUsize::new(0));
        let counter = stops.clone();
        let on_log_limit: LogLimitStop = Box::new(move || {
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        });

        stream_logs_to_writer(
            writer,
            scripted(vec![
                LogMsg::Stdout("a".repeat(64)),
                LogMsg::Stdout("b".repeat(64)),
                LogMsg::Stdout("c".repeat(64)),
                LogMsg::Finished,
            ]),
            scratch_db().await,
            Uuid::new_v4(),
            Some(on_log_limit),
        )
        .await;

        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn logs_under_the_cap_never_stop_the_process() {
        // The mirror invariant: a healthy execution must not be killed by the
        // machinery that exists to kill runaway ones.
        let dir = tempfile::tempdir().unwrap();
        let writer = ExecutionLogWriter::with_max_bytes(dir.path().join("proc.jsonl"), 1024 * 1024)
            .await
            .unwrap();

        let stops = Arc::new(AtomicUsize::new(0));
        let counter = stops.clone();
        let on_log_limit: LogLimitStop = Box::new(move || {
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        });

        stream_logs_to_writer(
            writer,
            scripted(vec![LogMsg::Stdout("small".into()), LogMsg::Finished]),
            scratch_db().await,
            Uuid::new_v4(),
            Some(on_log_limit),
        )
        .await;

        assert_eq!(stops.load(Ordering::SeqCst), 0);
    }
}
