use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use futures::{StreamExt, future};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

use crate::{log_msg::LogMsg, stream_lines::LinesStreamExt};

/// The UI mirror is deliberately bounded independently from durable evidence.
/// A reconnect can read the full compressed log; live listeners only need a
/// recent working set.
static HISTORY_BYTES: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| crate::execution_logs::in_memory_log_bytes() as usize);

#[derive(Clone)]
struct StoredMsg {
    msg: LogMsg,
    bytes: usize,
}

struct Inner {
    history: VecDeque<StoredMsg>,
    total_bytes: usize,
    evicted: bool,
}

pub struct MsgStore {
    inner: RwLock<Inner>,
    sender: broadcast::Sender<LogMsg>,
}

impl Default for MsgStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MsgStore {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(100000);
        Self {
            inner: RwLock::new(Inner {
                history: VecDeque::with_capacity(32),
                total_bytes: 0,
                evicted: false,
            }),
            sender,
        }
    }

    pub fn push(&self, msg: LogMsg) {
        let bytes = msg.approx_bytes();

        let mut inner = self.inner.write().unwrap();
        // Publication and history/subscription snapshots share one lock. A
        // concurrent subscriber sees every message exactly once, never a gap.
        let _ = self.sender.send(msg.clone());
        while inner.total_bytes.saturating_add(bytes) > *HISTORY_BYTES {
            if let Some(front) = inner.history.pop_front() {
                inner.total_bytes = inner.total_bytes.saturating_sub(front.bytes);
                inner.evicted = true;
            } else {
                break;
            }
        }
        if bytes > *HISTORY_BYTES {
            inner.evicted = true;
            return;
        }
        inner.history.push_back(StoredMsg { msg, bytes });
        inner.total_bytes = inner.total_bytes.saturating_add(bytes);
    }

    // Convenience
    pub fn push_stdout<S: Into<String>>(&self, s: S) {
        self.push(LogMsg::Stdout(s.into()));
    }

    pub fn push_patch(&self, patch: json_patch::Patch) {
        self.push(LogMsg::JsonPatch(patch));
    }

    pub fn push_session_id(&self, session_id: String) {
        self.push(LogMsg::SessionId(session_id));
    }

    pub fn push_message_id(&self, id: String) {
        self.push(LogMsg::MessageId(id));
    }

    pub fn push_finished(&self) {
        self.push(LogMsg::Finished);
    }

    pub fn get_receiver(&self) -> broadcast::Receiver<LogMsg> {
        self.sender.subscribe()
    }

    pub fn get_history(&self) -> Vec<LogMsg> {
        self.inner
            .read()
            .unwrap()
            .history
            .iter()
            .map(|s| s.msg.clone())
            .collect()
    }

    pub fn history_complete(&self) -> bool {
        !self.inner.read().unwrap().evicted
    }

    /// History then live, as `LogMsg`.
    pub fn history_plus_stream(
        &self,
    ) -> futures::stream::BoxStream<'static, Result<LogMsg, std::io::Error>> {
        let (history, rx, evicted) = {
            let inner = self.inner.read().unwrap();
            (
                inner
                    .history
                    .iter()
                    .map(|entry| entry.msg.clone())
                    .collect::<Vec<_>>(),
                self.sender.subscribe(),
                inner.evicted,
            )
        };

        let gap = evicted.then(|| {
            Err(std::io::Error::other(
                "UI history evicted; read durable execution evidence for complete output",
            ))
        });
        let hist = futures::stream::iter(gap.into_iter().chain(history.into_iter().map(Ok)));
        let live = BroadcastStream::new(rx).map(|res| match res {
            Ok(msg) => Ok(msg),
            Err(BroadcastStreamRecvError::Lagged(n)) => Err(std::io::Error::other(format!(
                "UI stream lagged by {n} messages; read durable execution evidence"
            ))),
        });

        Box::pin(hist.chain(live))
    }

    pub fn stdout_chunked_stream(
        &self,
    ) -> futures::stream::BoxStream<'static, Result<String, std::io::Error>> {
        self.history_plus_stream()
            .take_while(|res| future::ready(!matches!(res, Ok(LogMsg::Finished))))
            .filter_map(|res| async move {
                match res {
                    Ok(LogMsg::Stdout(s)) => Some(Ok(s)),
                    Err(error) => Some(Err(error)),
                    _ => None,
                }
            })
            .boxed()
    }

    pub fn stdout_lines_stream(
        &self,
    ) -> futures::stream::BoxStream<'static, std::io::Result<String>> {
        self.stdout_chunked_stream().lines()
    }

    pub fn stderr_chunked_stream(
        &self,
    ) -> futures::stream::BoxStream<'static, Result<String, std::io::Error>> {
        self.history_plus_stream()
            .take_while(|res| future::ready(!matches!(res, Ok(LogMsg::Finished))))
            .filter_map(|res| async move {
                match res {
                    Ok(LogMsg::Stderr(s)) => Some(Ok(s)),
                    Err(error) => Some(Err(error)),
                    _ => None,
                }
            })
            .boxed()
    }

    /// Forward a stream of typed log messages into this store.
    pub fn spawn_forwarder<S, E>(self: Arc<Self>, stream: S) -> JoinHandle<()>
    where
        S: futures::Stream<Item = Result<LogMsg, E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        tokio::spawn(async move {
            tokio::pin!(stream);

            while let Some(next) = stream.next().await {
                match next {
                    Ok(msg) => self.push(msg),
                    Err(e) => self.push(LogMsg::Stderr(format!("stream error: {e}"))),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_history_has_its_own_small_bound() {
        // Retention and UI memory have different jobs. The latter must stay
        // bounded even when the former grows until disk admission refuses it.
        assert_eq!(
            *HISTORY_BYTES,
            crate::execution_logs::DEFAULT_IN_MEMORY_LOG_BYTES as usize
        );
    }

    #[test]
    fn history_evicts_oldest_messages_at_the_cap() {
        // The cap must bound bytes, not just count entries.
        let store = MsgStore::new();
        let chunk = "x".repeat(64 * 1024);
        for _ in 0..(*HISTORY_BYTES / chunk.len() + 8) {
            store.push_stdout(chunk.clone());
        }
        let inner = store.inner.read().unwrap();
        assert!(inner.total_bytes <= *HISTORY_BYTES);
    }

    #[tokio::test]
    async fn history_to_live_handoff_has_no_duplicates_or_holes() {
        let store = Arc::new(MsgStore::new());
        let producer = store.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..1000 {
                producer.push_stdout(i.to_string());
            }
            producer.push_finished();
        });
        let mut stream = store.history_plus_stream();
        for i in 0..1000 {
            assert!(
                matches!(stream.next().await.unwrap().unwrap(), LogMsg::Stdout(text) if text == i.to_string())
            );
        }
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            LogMsg::Finished
        ));
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn oversized_ui_message_is_bounded_and_reports_a_gap() {
        let store = MsgStore::new();
        store.push_stdout("x".repeat(*HISTORY_BYTES + 1));
        assert!(store.get_history().is_empty());
        assert!(store.history_plus_stream().next().await.unwrap().is_err());
    }
}
