use rand::Rng;
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tonic::{Request, Response, Status, transport::Server};

// Include the gRPC generated code
pub mod chat {
    tonic::include_proto!("chat");
}

use chat::chat_service_server::{ChatService, ChatServiceServer};
use chat::{ChatMessage, GetMessagesRequest, GetMessagesResponse};

type SharedQueue = Arc<Mutex<VecDeque<ChatMessage>>>;

// Config structures for Twitch and YouTube
#[derive(Debug, Deserialize, Clone)]
struct TwitchConfig {
    channel: String,
    oauth_token: String,
    username: String,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeConfig {
    api_key: String,
    live_chat_id: String,
}

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    twitch: Option<TwitchConfig>,
    youtube: Option<YouTubeConfig>,
}

// Struct to implement the Tonic gRPC service
pub struct MyChatService {
    queue: SharedQueue,
}

#[tonic::async_trait]
impl ChatService for MyChatService {
    async fn get_messages(
        &self,
        request: Request<GetMessagesRequest>,
    ) -> Result<Response<GetMessagesResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit as usize;

        let mut queue = self.queue.lock().await;
        let count = std::cmp::min(limit, queue.len());

        // Drain/remove messages from the queue to clear them on read
        let messages: Vec<ChatMessage> = queue.drain(..count).collect();

        if count > 0 {
            println!(
                "[Server] Handing out {} messages, clearing them from server queue.",
                count
            );
        }

        Ok(Response::new(GetMessagesResponse { messages }))
    }
}

// Helpers for timestamps
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// Mock generators for YouTube and Twitch
const YT_SENDERS: &[&str] = &[
    "AliceYT",
    "Bob_The_Builder",
    "GamerPro99",
    "TechGuru",
    "Subscribor",
];
const YT_MESSAGES: &[&str] = &[
    "Hello streamer!",
    "Nice play!",
    "What specs is your PC?",
    "Is this game good?",
    "Hydrate!",
    "Subscribed!",
    "First time viewer, love the stream!",
];

const TWITCH_SENDERS: &[&str] = &[
    "twitch_gamer",
    "KappaLover",
    "PogChamp12",
    "LUL_viewer",
    "Backseater99",
];
const TWITCH_MESSAGES: &[&str] = &[
    "POGGERS",
    "LUL",
    "Backseat gaming incoming!",
    "F in the chat",
    "hype!",
    "monkaS",
    "NotLikeThis",
];

// Ingest thread for YouTube (Simulated or Real API)
async fn run_youtube_ingest(config: Option<YouTubeConfig>, tx: mpsc::Sender<ChatMessage>) {
    if let Some(cfg) = config {
        println!(
            "[YouTube Ingest] Configured. Connecting to real YouTube API for liveChatId: {}",
            cfg.live_chat_id
        );
        let client = reqwest::Client::new();
        let mut page_token: Option<String> = None;
        let mut poll_interval = Duration::from_secs(5);

        loop {
            let mut url = format!(
                "https://www.googleapis.com/youtube/v3/liveChat/messages?liveChatId={}&part=snippet,authorDetails&key={}",
                cfg.live_chat_id, cfg.api_key
            );
            if let Some(ref token) = page_token {
                url.push_str(&format!("&pageToken={}", token));
            }

            match client.get(&url).send().await {
                Ok(resp) => {
                    #[derive(Deserialize)]
                    #[serde(rename_all = "camelCase")]
                    struct YouTubeSnippet {
                        display_message: String,
                    }

                    #[derive(Deserialize)]
                    #[serde(rename_all = "camelCase")]
                    struct YouTubeAuthorDetails {
                        display_name: String,
                    }

                    #[derive(Deserialize)]
                    #[serde(rename_all = "camelCase")]
                    struct YouTubeLiveChatItem {
                        id: String,
                        snippet: YouTubeSnippet,
                        author_details: YouTubeAuthorDetails,
                    }

                    #[derive(Deserialize)]
                    #[serde(rename_all = "camelCase")]
                    struct YouTubeResponse {
                        items: Vec<YouTubeLiveChatItem>,
                        next_page_token: Option<String>,
                        polling_interval_millis: Option<u64>,
                    }

                    match resp.json::<YouTubeResponse>().await {
                        Ok(data) => {
                            if let Some(millis) = data.polling_interval_millis {
                                poll_interval = Duration::from_millis(millis);
                            }
                            page_token = data.next_page_token;

                            for item in data.items {
                                let msg = ChatMessage {
                                    id: item.id,
                                    platform: "YouTube".to_string(),
                                    sender: item.author_details.display_name,
                                    content: item.snippet.display_message,
                                    timestamp: now_ms(),
                                };
                                if tx.send(msg).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[YouTube Ingest] Error parsing JSON: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[YouTube Ingest] Request error: {:?}", e);
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    } else {
        println!("[YouTube Ingest] No configuration found. Running in SIMULATED mode.");
        let mut id_counter = 0;
        loop {
            // Wait random time between 2 to 6 seconds
            let secs = rand::thread_rng().gen_range(2..6);
            tokio::time::sleep(Duration::from_secs(secs)).await;
            id_counter += 1;

            let (sender, content) = {
                let mut r = rand::thread_rng();
                let s = YT_SENDERS[r.gen_range(0..YT_SENDERS.len())].to_string();
                let c = YT_MESSAGES[r.gen_range(0..YT_MESSAGES.len())].to_string();
                (s, c)
            };

            let msg = ChatMessage {
                id: format!("yt_{}_{}", now_ms(), id_counter),
                platform: "YouTube".to_string(),
                sender,
                content,
                timestamp: now_ms(),
            };

            if tx.send(msg).await.is_err() {
                break;
            }
        }
    }
}

// Ingest thread for Twitch (Simulated or Real IRC)
async fn run_twitch_ingest(config: Option<TwitchConfig>, tx: mpsc::Sender<ChatMessage>) {
    if let Some(cfg) = config {
        println!(
            "[Twitch Ingest] Configured. Connecting to Twitch IRC for channel: #{}",
            cfg.channel
        );

        loop {
            match TcpStream::connect("irc.chat.twitch.tv:6667").await {
                Ok(mut stream) => {
                    println!("[Twitch Ingest] Connected to IRC server.");
                    // Authenticate
                    let mut write_err = false;
                    if stream
                        .write_all(format!("PASS {}\r\n", cfg.oauth_token).as_bytes())
                        .await
                        .is_err()
                    {
                        write_err = true;
                    }
                    if !write_err
                        && stream
                            .write_all(format!("NICK {}\r\n", cfg.username).as_bytes())
                            .await
                            .is_err()
                    {
                        write_err = true;
                    }
                    if !write_err
                        && stream
                            .write_all(format!("JOIN #{}\r\n", cfg.channel).as_bytes())
                            .await
                            .is_err()
                    {
                        write_err = true;
                    }

                    if write_err {
                        eprintln!(
                            "[Twitch Ingest] Error writing auth/join packets. Reconnecting..."
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }

                    let (reader, mut writer) = stream.into_split();
                    let mut buf_reader = BufReader::new(reader);
                    let mut line = String::new();

                    loop {
                        line.clear();
                        match buf_reader.read_line(&mut line).await {
                            Ok(0) => {
                                eprintln!("[Twitch Ingest] Connection closed by twitch server.");
                                break;
                            }
                            Ok(_) => {
                                if line.starts_with("PING") {
                                    let pong = line.replace("PING", "PONG");
                                    let _ = writer.write_all(pong.as_bytes()).await;
                                } else if let Some((sender, content)) = parse_twitch_msg(&line) {
                                    let msg = ChatMessage {
                                        id: format!(
                                            "twitch_{}_{}",
                                            now_ms(),
                                            rand::random::<u32>()
                                        ),
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
                                eprintln!("[Twitch Ingest] Read error: {:?}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[Twitch Ingest] Connection error: {:?}. Retrying in 5 seconds...",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    } else {
        println!("[Twitch Ingest] No configuration found. Running in SIMULATED mode.");
        let mut id_counter = 0;
        loop {
            // Wait random time between 1 to 4 seconds
            let secs = rand::thread_rng().gen_range(1..4);
            tokio::time::sleep(Duration::from_secs(secs)).await;
            id_counter += 1;

            let (sender, content) = {
                let mut r = rand::thread_rng();
                let s = TWITCH_SENDERS[r.gen_range(0..TWITCH_SENDERS.len())].to_string();
                let c = TWITCH_MESSAGES[r.gen_range(0..TWITCH_MESSAGES.len())].to_string();
                (s, c)
            };

            let msg = ChatMessage {
                id: format!("twitch_{}_{}", now_ms(), id_counter),
                platform: "Twitch".to_string(),
                sender,
                content,
                timestamp: now_ms(),
            };

            if tx.send(msg).await.is_err() {
                break;
            }
        }
    }
}

// Simple Twitch message parser
fn parse_twitch_msg(line: &str) -> Option<(String, String)> {
    if !line.contains("PRIVMSG") {
        return None;
    }
    if !line.starts_with(':') {
        return None;
    }
    let parts: Vec<&str> = line.splitn(2, " PRIVMSG ").collect();
    if parts.len() != 2 {
        return None;
    }
    let sender_prefix = parts[0];
    let sender = sender_prefix.get(1..)?.split('!').next()?;

    let content_parts: Vec<&str> = parts[1].splitn(2, " :").collect();
    if content_parts.len() != 2 {
        return None;
    }
    let content = content_parts[1].trim_end();
    Some((sender.to_string(), content.to_string()))
}

// Config loading helper
fn load_config() -> Option<AppConfig> {
    if let Ok(content) = std::fs::read_to_string("config.json") {
        if let Ok(config) = serde_json::from_str::<AppConfig>(&content) {
            return Some(config);
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Server] Starting Stream Chat Server...");

    let config = load_config();
    let (twitch_cfg, yt_cfg) = match config {
        Some(cfg) => (cfg.twitch, cfg.youtube),
        None => {
            println!("[Server] config.json not found or invalid. Running in fully simulated mode.");
            (None, None)
        }
    };

    let queue: SharedQueue = Arc::new(Mutex::new(VecDeque::new()));
    let (tx, mut rx) = mpsc::channel::<ChatMessage>(100);

    // Spawn ingest tasks
    let yt_tx = tx.clone();
    tokio::spawn(async move {
        run_youtube_ingest(yt_cfg, yt_tx).await;
    });

    let twitch_tx = tx.clone();
    tokio::spawn(async move {
        run_twitch_ingest(twitch_cfg, twitch_tx).await;
    });

    // Spawn receiver thread/task to collect messages into the shared memory queue
    let server_queue = queue.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let mut q = server_queue.lock().await;
            q.push_back(msg);
        }
    });

    // Start gRPC server
    let addr = "[::1]:50051".parse()?;
    let chat_service = MyChatService { queue };

    println!("[Server] gRPC server listening on {}", addr);
    Server::builder()
        .add_service(ChatServiceServer::new(chat_service))
        .serve(addr)
        .await?;

    Ok(())
}
