use crate::proto::ChatMessage;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub struct ConnectionStatus {
    pub connected: bool,
    pub last_error: Option<String>,
    pub youtube_status: Option<YouTubeStatus>,
}

#[derive(Clone)]
pub struct YouTubeStatus {
    pub state: String,
    pub detail: String,
    pub messages_received: u64,
}

pub struct AppState {
    pub message_buffer: Arc<Mutex<VecDeque<ChatMessage>>>,
    pub max_messages: usize,
    pub status: Arc<Mutex<ConnectionStatus>>,
    pub fetch_trigger: mpsc::Sender<()>,
}

pub fn save_client_buffer(buf: &VecDeque<ChatMessage>) -> Result<(), std::io::Error> {
    let serialized = serde_json::to_string(buf)?;
    std::fs::write("client_state.json", serialized)?;
    Ok(())
}

pub fn load_client_buffer() -> VecDeque<ChatMessage> {
    if let Ok(content) = std::fs::read_to_string("client_state.json")
        && let Ok(buf) = serde_json::from_str::<VecDeque<ChatMessage>>(&content)
    {
        return buf;
    }
    VecDeque::new()
}
