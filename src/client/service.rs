use crate::client::state::ConnectionStatus;
use crate::client::state::save_client_buffer;
use crate::proto::chat_service_client::ChatServiceClient;
use crate::proto::{ChatMessage, GetMessagesRequest};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct ServerChatStream {
    handle: JoinHandle<()>,
}

impl ServerChatStream {
    pub fn start(
        buffer: Arc<Mutex<VecDeque<ChatMessage>>>,
        status: Arc<Mutex<ConnectionStatus>>,
        mut trigger_rx: mpsc::Receiver<()>,
        limit: usize,
        address: String,
    ) -> Self {
        let max_buffer = 3 * limit;
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = trigger_rx.recv() => {}
                }

                let len = {
                    let buf = buffer.lock().unwrap();
                    buf.len()
                };

                if len <= 2 * limit {
                    match ChatServiceClient::connect(address.clone()).await {
                        Ok(mut client) => {
                            {
                                let mut s = status.lock().unwrap();
                                s.connected = true;
                                s.last_error = None;
                            }

                            let request = tonic::Request::new(GetMessagesRequest {
                                limit: limit as u32,
                            });

                            match client.get_messages(request).await {
                                Ok(response) => {
                                    let new_msgs = response.into_inner().messages;
                                    if !new_msgs.is_empty() {
                                        let mut buf = buffer.lock().unwrap();
                                        let space = max_buffer.saturating_sub(buf.len());
                                        let to_add = std::cmp::min(space, new_msgs.len());
                                        buf.extend(new_msgs.into_iter().take(to_add));
                                        if let Err(e) = save_client_buffer(&buf) {
                                            eprintln!("[Client] Error saving buffer: {:?}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    let mut s = status.lock().unwrap();
                                    s.last_error = Some(format!("gRPC Call Error: {}", e));
                                }
                            }
                        }
                        Err(e) => {
                            let mut s = status.lock().unwrap();
                            s.connected = false;
                            s.last_error = Some(format!("Connection failed: {}", e));
                        }
                    }
                }
            }
        });

        Self { handle }
    }

    pub fn abort(&self) {
        self.handle.abort();
    }
}
