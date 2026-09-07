use axum::{extract::ws::Message, response::sse::Event};
use json_patch::Patch;
use serde::{Deserialize, Serialize};

pub const EV_STDOUT: &str = "stdout";
pub const EV_STDERR: &str = "stderr";
pub const EV_JSON_PATCH: &str = "json_patch";
pub const EV_SESSION_ID: &str = "session_id";
pub const EV_MESSAGE_ID: &str = "message_id";
pub const EV_READY: &str = "ready";
pub const EV_FINISHED: &str = "finished";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogMsg {
    Stdout(String),
    Stderr(String),
    JsonPatch(Patch),
    SessionId(String),
    MessageId(String),
    Ready,
    Finished,
}

impl LogMsg {
    pub fn name(&self) -> &'static str {
        match self {
            LogMsg::Stdout(_) => EV_STDOUT,
            LogMsg::Stderr(_) => EV_STDERR,
            LogMsg::JsonPatch(_) => EV_JSON_PATCH,
            LogMsg::SessionId(_) => EV_SESSION_ID,
            LogMsg::MessageId(_) => EV_MESSAGE_ID,
            LogMsg::Ready => EV_READY,
            LogMsg::Finished => EV_FINISHED,
        }
    }

    pub fn to_sse_event(&self) -> Event {
        match self {
            LogMsg::Stdout(s) => Event::default().event(EV_STDOUT).data(s.clone()),
            LogMsg::Stderr(s) => Event::default().event(EV_STDERR).data(s.clone()),
            LogMsg::JsonPatch(patch) => {
                let data = serde_json::to_string(patch).unwrap_or_else(|_| "[]".to_string());
                Event::default().event(EV_JSON_PATCH).data(data)
            }
            LogMsg::SessionId(s) => Event::default().event(EV_SESSION_ID).data(s.clone()),
            LogMsg::MessageId(s) => Event::default().event(EV_MESSAGE_ID).data(s.clone()),
            LogMsg::Ready => Event::default().event(EV_READY).data(""),
            LogMsg::Finished => Event::default().event(EV_FINISHED).data(""),
        }
    }

    /// Convert LogMsg to WebSocket message with fallback error handling
    ///
    /// This method mirrors the behavior of the original logmsg_to_ws function
    /// but with better error handling than unwrap().
    pub fn to_ws_message_unchecked(&self) -> Message {
        // Finished and Ready use special JSON formats for frontend compatibility
        let json = match self {
            LogMsg::Ready => r#"{"Ready":true}"#.to_string(),
            LogMsg::Finished => r#"{"finished":true}"#.to_string(),
            _ => serde_json::to_string(self)
                .unwrap_or_else(|_| r#"{"error":"serialization_failed"}"#.to_string()),
        };

        Message::Text(json.into())
    }

    /// Rough size accounting for your byte‑budgeted history.
    pub fn approx_bytes(&self) -> usize {
        const OVERHEAD: usize = std::mem::size_of::<LogMsg>() + 2 * std::mem::size_of::<usize>();
        let payload = match self {
            LogMsg::Stdout(s) | LogMsg::Stderr(s) | LogMsg::SessionId(s) | LogMsg::MessageId(s) => {
                s.capacity()
            }
            LogMsg::JsonPatch(patch) => {
                // Count without allocating a second serialized copy merely to
                // discover that a large patch will not fit the disposable view.
                let mut count = ByteCount(0);
                if serde_json::to_writer(&mut count, patch).is_err() {
                    return usize::MAX;
                }
                count.0.saturating_add(
                    patch
                        .0
                        .capacity()
                        .saturating_mul(std::mem::size_of::<json_patch::PatchOperation>()),
                )
            }
            LogMsg::Ready | LogMsg::Finished => 0,
        };
        OVERHEAD.saturating_add(payload)
    }
}

struct ByteCount(usize);

impl std::io::Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
