pub mod chat_protobufs {
    tonic::include_proto!("chat");
}

use chat_protobufs::chat_service_server::{ChatService, ChatServiceServer};
use chat_protobufs::{ChatMessage, GetMessagesRequest, GetMessagesResponse};
use rand::Rng;
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{debug, error, info, warn};

type SharedQueue = Arc<Mutex<VecDeque<ChatMessage>>>;

#[derive(Debug, Deserialize, Clone)]
struct TwitchConfig {
    channel: String,
    username: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeConfig {
    api_key: String,
    live_chat_id: Option<String>,
    video_id: Option<String>,
    channel_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    twitch: Option<TwitchConfig>,
    youtube: Option<YouTubeConfig>,
}

async fn get_live_chat_id_from_video(
    client: &reqwest::Client,
    video_id: &str,
    api_key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let url = format!(
        "https://www.googleapis.com/youtube/v3/videos?id={}&part=liveStreamingDetails&key={}",
        video_id, api_key
    );
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct LiveStreamingDetails {
        active_live_chat_id: Option<String>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct VideoItem {
        live_streaming_details: Option<LiveStreamingDetails>,
    }
    #[derive(Deserialize)]
    struct VideosResponse {
        items: Vec<VideoItem>,
    }

    let response = client.get(&url).send().await?;
    let status = response.status();
    if !status.is_success() {
        let err_text = response.text().await.unwrap_or_default();
        return Err(format!("YouTube API returned error status {}: {}", status, err_text).into());
    }

    let resp = response.json::<VideosResponse>().await?;
    if let Some(item) = resp.items.first() {
        if let Some(details) = &item.live_streaming_details {
            if let Some(chat_id) = &details.active_live_chat_id {
                return Ok(chat_id.clone());
            }
        }
    }
    Err("No active live chat ID found for this video".into())
}

async fn get_active_video_from_channel(
    client: &reqwest::Client,
    channel_id: &str,
    api_key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let url = format!(
        "https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={}&eventType=live&type=video&key={}",
        channel_id, api_key
    );
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct IdDetails {
        video_id: String,
    }
    #[derive(Deserialize)]
    struct SearchItem {
        id: IdDetails,
    }
    #[derive(Deserialize)]
    struct SearchResponse {
        items: Vec<SearchItem>,
    }

    let response = client.get(&url).send().await?;
    let status = response.status();
    if !status.is_success() {
        let err_text = response.text().await.unwrap_or_default();
        return Err(format!("YouTube API returned error status {}: {}", status, err_text).into());
    }

    let resp = response.json::<SearchResponse>().await?;
    if let Some(item) = resp.items.first() {
        return Ok(item.id.video_id.clone());
    }
    Err("No active live streams found on this channel".into())
}

async fn resolve_live_chat_id(
    client: &reqwest::Client,
    config: &YouTubeConfig,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(chat_id) = &config.live_chat_id {
        if !chat_id.is_empty() {
            return Ok(chat_id.clone());
        }
    }
    if let Some(video_id) = &config.video_id {
        if !video_id.is_empty() {
            info!(
                "YouTube Ingest: Resolving liveChatId from videoId: {}",
                video_id
            );
            return get_live_chat_id_from_video(client, video_id, &config.api_key).await;
        }
    }
    if let Some(channel_id) = &config.channel_id {
        if !channel_id.is_empty() {
            info!(
                "YouTube Ingest: Searching for active live stream on channelId: {}",
                channel_id
            );
            let video_id =
                get_active_video_from_channel(client, channel_id, &config.api_key).await?;
            info!(
                "YouTube Ingest: Found active stream videoId: {}. Resolving liveChatId...",
                video_id
            );
            return get_live_chat_id_from_video(client, &video_id, &config.api_key).await;
        }
    }
    Err("Config must specify either live_chat_id, video_id, or channel_id".into())
}

pub struct ChatServer {
    queue: SharedQueue,
}

#[tonic::async_trait]
impl ChatService for ChatServer {
    async fn get_messages(
        &self,
        request: Request<GetMessagesRequest>,
    ) -> Result<Response<GetMessagesResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit as usize;
        let mut queue = self.queue.lock().await;
        let count = std::cmp::min(limit, queue.len());
        let messages: Vec<ChatMessage> = queue.drain(..count).collect();
        if count > 0 {
            info!("Sent {} messages to client", count);
            save_queue_to_disk(&queue);
        }
        Ok(Response::new(GetMessagesResponse { messages }))
    }
}

fn save_queue_to_disk(q: &VecDeque<ChatMessage>) {
    match serde_json::to_string(q) {
        Ok(serialized) => match std::fs::write("server_state.json", serialized) {
            Ok(_) => debug!("Saved {} messages to server_state.json", q.len()),
            Err(e) => error!("Error saving queue to disk: {:?}", e),
        },
        Err(e) => error!("Error serializing queue: {:?}", e),
    }
}

fn load_queue_from_disk() -> VecDeque<ChatMessage> {
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn poll_youtube_chat(config: YouTubeConfig, tx: mpsc::Sender<ChatMessage>) {
    let client = reqwest::Client::new();
    let mut attempts = 0;
    let mut live_chat_id = String::default();
    while attempts < 5 {
        live_chat_id = match resolve_live_chat_id(&client, &config).await {
            Ok(id) => {
                info!("YouTube Ingest: Successfully resolved liveChatId: {}", id);
                id
            }
            Err(e) => {
                error!(
                    "YouTube Ingest: Failed to resolve liveChatId: {:?}. YouTube ingest thread terminating.",
                    e
                );
                return;
            }
        };
        attempts += 1;
    }

    let mut page_token: Option<String> = None;
    let mut poll_interval = Duration::from_secs(5);

    loop {
        let mut url = format!(
            "https://www.googleapis.com/youtube/v3/liveChat/messages?liveChatId={}&part=snippet,authorDetails&key={}",
            live_chat_id, config.api_key
        );
        if let Some(ref token) = page_token {
            url.push_str(&format!("&pageToken={}", token));
        }

        match client.get(&url).send().await {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    let err_text = match resp.text().await {
                        Ok(text) => text,
                        Err(e) => format!("Failed to read HTTP body: {e}"),
                    };
                    error!(
                        "YouTube Ingest: API returned error status {}: {}",
                        status, err_text
                    );
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }

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
                        error!("YouTube Ingest: Error parsing JSON: {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("YouTube Ingest: Request error: {:?}", e);
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn poll_twitch_chat(config: TwitchConfig, tx: mpsc::Sender<ChatMessage>) {
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    info!("Starting Stream Chat Server...");
    let config = load_config().expect("config.json not found or invalid");

    let initial_queue = load_queue_from_disk();
    let queue: SharedQueue = Arc::new(Mutex::new(initial_queue));
    let (tx, mut rx) = mpsc::channel::<ChatMessage>(100);

    // Ingests
    let yt_tx = tx.clone();
    if let Some(youtube) = config.youtube {
        tokio::spawn(async move {
            poll_youtube_chat(youtube, yt_tx).await;
        });
    }

    let twitch_tx = tx.clone();
    if let Some(twitch) = config.twitch {
        tokio::spawn(async move {
            poll_twitch_chat(twitch, twitch_tx).await;
        });
    }

    // Writeback to queue
    let server_queue = queue.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
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

    let addr = "[::1]:50051".parse()?;
    let chat_service = ChatServer { queue };
    info!("gRPC server listening on {}", addr);
    Server::builder()
        .add_service(ChatServiceServer::new(chat_service))
        .serve(addr)
        .await?;

    Ok(())
}
