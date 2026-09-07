use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use futures::{StreamExt, future};
use tokio::{sync::watch, task::JoinHandle};

use crate::{log_msg::LogMsg, stream_lines::LinesStreamExt};

/// The UI mirror is deliberately bounded independently from durable evidence.
/// A reconnect can read the full compressed log; live listeners only need a
/// recent working set.
static HISTORY_BYTES: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| crate::execution_logs::in_memory_log_bytes() as usize);

#[derive(Clone)]
struct StoredMsg {
    sequence: u64,
    msg: LogMsg,
    bytes: usize,
}

struct Inner {
    history: VecDeque<StoredMsg>,
    total_bytes: usize,
    next_sequence: u64,
}

pub struct MsgStore {
    inner: Arc<RwLock<Inner>>,
    // Only a wake signal is broadcast. Every subscriber reads the same bounded
    // ring; a slow reader cannot keep a second queue of evicted payloads alive.
    sender: watch::Sender<()>,
}

impl Default for MsgStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MsgStore {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(());
        Self {
            inner: Arc::new(RwLock::new(Inner {
                history: VecDeque::with_capacity(32),
                total_bytes: 0,
                next_sequence: 0,
            })),
            sender,
        }
    }

    pub fn push(&self, msg: LogMsg) {
        let bytes = msg.approx_bytes();

        let mut inner = self.inner.write().unwrap();
        // Publication and history/subscription snapshots share one lock. A
        // concurrent subscriber sees every message exactly once, never a gap.
        let sequence = inner.next_sequence;
        inner.next_sequence = sequence.checked_add(1).expect("UI sequence exhausted");
        while inner.total_bytes.saturating_add(bytes) > *HISTORY_BYTES {
            if let Some(front) = inner.history.pop_front() {
                inner.total_bytes = inner.total_bytes.saturating_sub(front.bytes);
            } else {
                break;
            }
        }
        if bytes <= *HISTORY_BYTES {
            inner.history.push_back(StoredMsg {
                sequence,
                msg,
                bytes,
            });
            inner.total_bytes = inner.total_bytes.saturating_add(bytes);
        }
        self.sender.send_replace(());
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

    pub fn live_stream(&self) -> futures::stream::BoxStream<'static, std::io::Result<LogMsg>> {
        self.subscribe(false)
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
        let inner = self.inner.read().unwrap();
        inner
            .history
            .front()
            .map_or(inner.next_sequence == 0, |entry| entry.sequence == 0)
    }

    /// History then live, as `LogMsg`.
    pub fn history_plus_stream(
        &self,
    ) -> futures::stream::BoxStream<'static, Result<LogMsg, std::io::Error>> {
        self.subscribe(true)
    }

    fn subscribe(
        &self,
        include_history: bool,
    ) -> futures::stream::BoxStream<'static, std::io::Result<LogMsg>> {
        let (receiver, next) = {
            let inner = self.inner.read().unwrap();
            (
                self.sender.subscribe(),
                if include_history {
                    0
                } else {
                    inner.next_sequence
                },
            )
        };
        futures::stream::unfold((self.inner.clone(), receiver, next), |mut state| async move {
            loop {
                let item = {
                    let inner = state.0.read().unwrap();
                    let first = inner.history.front().map_or(inner.next_sequence, |entry| entry.sequence);
                    if state.2 < first {
                        let missing = first - state.2;
                        state.2 = first;
                        Some(Err(std::io::Error::other(format!(
                            "UI stream evicted {missing} messages; refresh from the authoritative source"
                        ))))
                    } else {
                        usize::try_from(state.2 - first).ok()
                            .and_then(|index| inner.history.get(index))
                            .map(|entry| {
                                state.2 += 1;
                                Ok(entry.msg.clone())
                            })
                    }
                };
                if let Some(item) = item { return Some((item, state)); }
                if state.1.changed().await.is_err() { return None; }
            }
        }).boxed()
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

    #[tokio::test]
    async fn stalled_subscriber_cannot_retain_payloads_evicted_from_the_ring() {
        let store = MsgStore::new();
        let mut stalled = store.history_plus_stream();
        let chunk = "x".repeat(64 * 1024);
        for _ in 0..32 {
            store.push_stdout(chunk.clone());
        }
        assert!(store.inner.read().unwrap().total_bytes <= *HISTORY_BYTES);
        // Far fewer than the old100000 message slots: the byte budget, not a
        // second independent payload queue, now decides observable retention.
        assert!(stalled.next().await.unwrap().is_err());
        assert!(matches!(
            stalled.next().await.unwrap().unwrap(),
            LogMsg::Stdout(_)
        ));
    }

    #[tokio::test]
    async fn live_reader_does_not_replay_history_and_drains_after_store_drop() {
        let store = MsgStore::new();
        store.push_stdout("past");
        let mut live = store.live_stream();
        store.push_stdout("live");
        store.push_finished();
        drop(store);
        assert!(
            matches!(live.next().await.unwrap().unwrap(), LogMsg::Stdout(text) if text == "live")
        );
        assert!(matches!(
            live.next().await.unwrap().unwrap(),
            LogMsg::Finished
        ));
        assert!(live.next().await.is_none());
    }

    #[tokio::test]
    async fn retained_string_capacity_counts_even_when_its_text_is_short() {
        let store = MsgStore::new();
        let mut live = store.history_plus_stream();
        let mut text = String::with_capacity(*HISTORY_BYTES + 1);
        text.push_str("short");
        store.push_stdout(text);
        assert!(store.get_history().is_empty());
        assert!(live.next().await.unwrap().is_err());
    }
}
