use crate::proto::ChatMessage;
use crate::server::config::TwitterConfig;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

#[derive(Deserialize, Debug)]
struct WsTwitterMessage {
    // Modify this structure to match the actual X/Twitter websocket JSON format
    sender: Option<String>,
    text: Option<String>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub async fn poll_twitter_chat(config: TwitterConfig, tx: mpsc::Sender<ChatMessage>) {
    info!(
        "Twitter Ingest: Connecting to websocket {} for room {}",
        config.ws_url, config.room_id
    );

    loop {
        // Attempt to connect to the websocket
        let mut request = match tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(config.ws_url.as_str()) {
            Ok(req) => req,
            Err(e) => {
                error!("Twitter Ingest: Invalid WebSocket URL: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        request.headers_mut().insert(
            "Origin",
            "https://x.com".parse().unwrap(),
        );
        request.headers_mut().insert(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:152.0) Gecko/20100101 Firefox/152.0".parse().unwrap(),
        );

        match connect_async(request).await {
            Ok((mut ws_stream, _)) => {
                info!("Twitter Ingest: Successfully connected to WebSocket.");

                // Example of sending an authentication/join message if needed
                let join_msg = format!(
                    r#"{{"action":"join", "room_id":"{}", "token":"{}"}}"#,
                    config.room_id,
                    config.auth_token.as_deref().unwrap_or("")
                );
                if let Err(e) = ws_stream.send(Message::Text(join_msg)).await {
                    error!("Twitter Ingest: Error sending join message: {}", e);
                }

                while let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            // Parse incoming message. This is a skeleton! You will need to adjust WsTwitterMessage.
                            if let Ok(parsed) = serde_json::from_str::<WsTwitterMessage>(&text) {
                                if let (Some(sender), Some(content)) = (parsed.sender, parsed.text) {
                                    let chat_msg = ChatMessage {
                                        id: format!(
                                            "twitter_{}_{}",
                                            now_ms(),
                                            rand::random::<u32>()
                                        ),
                                        platform: "Twitter".to_string(),
                                        sender,
                                        content,
                                        timestamp: now_ms(),
                                    };

                                    if tx.send(chat_msg).await.is_err() {
                                        warn!("Twitter Ingest: Channel closed");
                                        return;
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(ping)) => {
                            let _ = ws_stream.send(Message::Pong(ping)).await;
                        }
                        Ok(Message::Close(_)) => {
                            warn!("Twitter Ingest: WebSocket closed by server.");
                            break;
                        }
                        Err(e) => {
                            error!("Twitter Ingest: WebSocket error: {}", e);
                            break;
                        }
                        _ => {} // Ignore binary or pong
                    }
                }
            }
            Err(e) => {
                error!("Twitter Ingest: WebSocket connection failed: {}", e);
            }
        }

        // Reconnection backoff
        warn!("Twitter Ingest: Reconnecting in 5 seconds...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
