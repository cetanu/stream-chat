use crate::proto::ChatMessage;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

pub type SharedQueue = Arc<Mutex<VecDeque<ChatMessage>>>;

pub fn save_queue_to_disk(q: &VecDeque<ChatMessage>) {
    match serde_json::to_string(q) {
        Ok(serialized) => match std::fs::write("server_state.json", serialized) {
            Ok(_) => debug!("Saved {} messages to server_state.json", q.len()),
            Err(e) => error!("Error saving queue to disk: {:?}", e),
        },
        Err(e) => error!("Error serializing queue: {:?}", e),
    }
}

pub fn load_queue_from_disk() -> VecDeque<ChatMessage> {
    match std::fs::read_to_string("server_state.json") {
        Ok(content) => match serde_json::from_str::<VecDeque<ChatMessage>>(&content) {
            Ok(q) => {
                info!("Loaded {} messages from server_state.json", q.len());
                q
            }
            Err(e) => {
                warn!(
                    "Failed to deserialize server_state.json: {:?}. Starting with empty queue.",
                    e
                );
                VecDeque::new()
            }
        },
        Err(_) => {
            info!("No server_state.json found. Starting with empty queue.");
            VecDeque::new()
        }
    }
}
