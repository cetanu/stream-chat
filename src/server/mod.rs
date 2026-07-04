pub mod config;
pub mod service;
pub mod ingest;
pub mod state;

use crate::proto::ChatMessage;
use crate::server::config::AppConfig;
use crate::server::state::{SharedQueue, load_queue_from_disk, save_queue_to_disk};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tonic::transport::Server;
use tracing::info;

pub struct StreamChatServer {
    config: AppConfig,
    queue: SharedQueue,
    rx: mpsc::Receiver<ChatMessage>,
    tx: mpsc::Sender<ChatMessage>,
}

impl StreamChatServer {
    pub fn new(config: AppConfig) -> Self {
        let initial_queue = load_queue_from_disk();
        let queue: SharedQueue = Arc::new(Mutex::new(initial_queue));
        let (tx, rx) = mpsc::channel::<ChatMessage>(100);

        Self {
            config,
            queue,
            rx,
            tx,
        }
    }

    pub async fn start_ingest(&self) {
        if let Some(youtube) = self.config.youtube.clone() {
            let yt_tx = self.tx.clone();
            tokio::spawn(async move {
                ingest::youtube::poll_youtube_chat(youtube, yt_tx).await;
            });
        }

        if let Some(twitch) = self.config.twitch.clone() {
            let twitch_tx = self.tx.clone();
            tokio::spawn(async move {
                ingest::twitch::poll_twitch_chat(twitch, twitch_tx).await;
            });
        }
    }

    pub async fn serve(mut self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let server_queue = self.queue.clone();
        tokio::spawn(async move {
            while let Some(msg) = self.rx.recv().await {
                let mut q = server_queue.lock().await;
                info!(
                    "Adding message from {} ({}). Total queue length: {}",
                    msg.sender,
                    msg.platform,
                    q.len() + 1
                );
                q.push_back(msg);
                save_queue_to_disk(&q);
            }
        });

        let addr = addr.parse()?;
        let chat_service = service::ChatServerImpl {
            queue: self.queue.clone(),
        };
        info!("gRPC server listening on {}", addr);
        Server::builder()
            .add_service(crate::proto::chat_service_server::ChatServiceServer::new(
                chat_service,
            ))
            .serve(addr)
            .await?;

        Ok(())
    }
}
