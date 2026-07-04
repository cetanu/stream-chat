use crate::proto::ChatMessage;
use crate::server::config::TwitchConfig;
use rand::Rng;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn parse_irc_msg(line: &str) -> Option<(String, String)> {
    if !line.starts_with(':') || !line.contains("PRIVMSG") {
        return None;
    }
    let parts: Vec<&str> = line.splitn(2, " PRIVMSG ").collect();
    if parts.len() != 2 {
        return None;
    }
    let sender = parts[0].get(1..)?.split('!').next()?;

    let content_parts: Vec<&str> = parts[1].splitn(2, " :").collect();
    if content_parts.len() != 2 {
        return None;
    }
    let content = content_parts[1].trim_end();
    Some((sender.to_string(), content.to_string()))
}

pub async fn poll_twitch_chat(config: TwitchConfig, tx: mpsc::Sender<ChatMessage>) {
    'main: loop {
        match TcpStream::connect("irc.chat.twitch.tv:6667").await {
            Ok(mut stream) => {
                info!("Twitch Ingest: Connected to IRC server.");
                let nick = config.username.clone().unwrap_or(format!(
                    "justinfan{}",
                    rand::thread_rng().gen_range(10000..99999)
                ));
                let commands = vec![
                    format!("NICK {}\r\n", nick),
                    format!("JOIN #{}\r\n", config.channel),
                ];
                for command in commands {
                    let result = stream.write_all(command.as_bytes()).await;
                    if result.is_err() {
                        error!(
                            "Twitch Ingest: Error writing IRC handshake packets. Reconnecting..."
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue 'main;
                    }
                }

                let (reader, mut writer) = stream.into_split();
                let mut buf_reader = BufReader::new(reader);
                let mut line = String::new();

                loop {
                    line.clear();
                    match buf_reader.read_line(&mut line).await {
                        Ok(0) => {
                            warn!("Twitch Ingest: Connection closed by Twitch server.");
                            break;
                        }
                        Ok(_) => {
                            if line.starts_with("PING") {
                                let pong = line.replace("PING", "PONG");
                                let _ = writer.write_all(pong.as_bytes()).await;
                            } else if let Some((sender, content)) = parse_irc_msg(&line) {
                                let msg = ChatMessage {
                                    id: format!("twitch_{}_{}", now_ms(), rand::random::<u32>()),
                                    platform: "Twitch".to_string(),
                                    sender,
                                    content,
                                    timestamp: now_ms(),
                                };
                                if tx.send(msg).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Twitch Ingest: Read error: {:?}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "Twitch Ingest: Connection error: {:?}. Retrying in 5 seconds...",
                    e
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
