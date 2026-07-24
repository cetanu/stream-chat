use crate::proto::ChatMessage;
use crate::server::config::YouTubeConfig;
use crate::server::state::SharedIngestStatus;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
    if let Some(item) = resp.items.first()
        && let Some(details) = &item.live_streaming_details
        && let Some(chat_id) = &details.active_live_chat_id
    {
        return Ok(chat_id.clone());
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
    if let Some(chat_id) = &config.live_chat_id
        && !chat_id.is_empty()
    {
        return Ok(chat_id.clone());
    }
    if let Some(video_id) = &config.video_id
        && !video_id.is_empty()
    {
        info!(
            "YouTube Ingest: Resolving liveChatId from videoId: {}",
            video_id
        );
        return get_live_chat_id_from_video(client, video_id, &config.api_key).await;
    }
    if let Some(channel_id) = &config.channel_id
        && !channel_id.is_empty()
    {
        info!(
            "YouTube Ingest: Searching for active live stream on channelId: {}",
            channel_id
        );
        let video_id = get_active_video_from_channel(client, channel_id, &config.api_key).await?;
        info!(
            "YouTube Ingest: Found active stream videoId: {}. Resolving liveChatId...",
            video_id
        );
        return get_live_chat_id_from_video(client, &video_id, &config.api_key).await;
    }
    Err("Config must specify either live_chat_id, video_id, or channel_id".into())
}

fn truncate_detail(detail: impl AsRef<str>) -> String {
    const MAX_DETAIL_CHARS: usize = 180;
    let detail = detail.as_ref();
    let mut truncated: String = detail.chars().take(MAX_DETAIL_CHARS).collect();
    if detail.chars().count() > MAX_DETAIL_CHARS {
        truncated.push_str("...");
    }
    truncated
}

async fn update_status(
    status: &SharedIngestStatus,
    state: &str,
    detail: impl AsRef<str>,
    last_success_at_ms: Option<i64>,
    messages_received: Option<u64>,
) {
    let mut current = status.lock().await;
    current.state = state.to_string();
    current.detail = truncate_detail(detail);
    if let Some(timestamp) = last_success_at_ms {
        current.last_success_at_ms = timestamp;
    }
    if let Some(count) = messages_received {
        current.messages_received = count;
    }
}

pub async fn poll_youtube_chat(
    config: YouTubeConfig,
    tx: mpsc::Sender<ChatMessage>,
    status: SharedIngestStatus,
) {
    let client = reqwest::Client::new();
    update_status(
        &status,
        "starting",
        "Resolving active YouTube chat",
        None,
        None,
    )
    .await;
    let live_chat_id = match resolve_live_chat_id(&client, &config)
        .await
        .map_err(|e| e.to_string())
    {
        Ok(id) => {
            info!("YouTube Ingest: Successfully resolved liveChatId: {}", id);
            update_status(
                &status,
                "polling",
                "Connected; waiting for chat messages",
                None,
                None,
            )
            .await;
            id
        }
        Err(e) => {
            let detail = format!("Could not resolve active chat: {e}");
            error!("YouTube Ingest: {detail}. YouTube ingest thread terminating.");
            update_status(&status, "stopped", &detail, None, None).await;
            return;
        }
    };

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
                let http_status = resp.status();
                if !http_status.is_success() {
                    let err_text = match resp.text().await {
                        Ok(text) => text,
                        Err(e) => format!("Failed to read HTTP body: {e}"),
                    };
                    let detail = format!("YouTube API returned HTTP {http_status}: {err_text}");
                    error!("YouTube Ingest: {detail}");
                    update_status(&status, "error", &detail, None, None).await;
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }

                match resp.json::<YouTubeResponse>().await {
                    Ok(data) => {
                        if let Some(millis) = data.polling_interval_millis {
                            poll_interval = Duration::from_millis(millis);
                        }
                        page_token = data.next_page_token;

                        let item_count = data.items.len();
                        let received = {
                            let current = status.lock().await;
                            current.messages_received + item_count as u64
                        };
                        let detail = if item_count == 0 {
                            format!(
                                "Last poll succeeded; no new messages. Next poll in {} ms",
                                poll_interval.as_millis()
                            )
                        } else {
                            format!(
                                "Last poll received {item_count} message(s). Next poll in {} ms",
                                poll_interval.as_millis()
                            )
                        };
                        update_status(&status, "polling", detail, Some(now_ms()), Some(received))
                            .await;

                        for item in data.items {
                            let msg = ChatMessage {
                                id: item.id,
                                platform: "YouTube".to_string(),
                                sender: item.author_details.display_name,
                                content: item.snippet.display_message,
                                timestamp: now_ms(),
                            };
                            if tx.send(msg).await.is_err() {
                                warn!(
                                    "YouTube Ingest: Message queue receiver dropped; stopping ingest."
                                );
                                update_status(
                                    &status,
                                    "stopped",
                                    "Server message queue receiver dropped",
                                    None,
                                    None,
                                )
                                .await;
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let detail = format!("Could not parse YouTube API response: {e}");
                        error!("YouTube Ingest: {detail}");
                        update_status(&status, "error", &detail, None, None).await;
                    }
                }
            }
            Err(e) => {
                let detail = format!("YouTube API request failed: {e}");
                error!("YouTube Ingest: {detail}");
                update_status(&status, "error", &detail, None, None).await;
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}
