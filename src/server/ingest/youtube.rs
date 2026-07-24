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

async fn get_active_video_from_channel_web(
    client: &reqwest::Client,
    channel_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let url = format!("https://www.youtube.com/channel/{}/live", channel_id);
    let resp = client
        .get(&url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .send()
        .await?;

    let final_url = resp.url().as_str();
    if let Some(pos) = final_url.find("v=") {
        let v_part = &final_url[pos + 2..];
        let video_id = v_part.split('&').next().unwrap_or(v_part);
        if video_id.len() == 11 {
            return Ok(video_id.to_string());
        }
    }

    let text = resp.text().await?;
    if let Some(pos) = text.find("\"videoId\":\"") {
        let after = &text[pos + 11..];
        if let Some(end_pos) = after.find('"') {
            let video_id = &after[..end_pos];
            if video_id.len() == 11 {
                return Ok(video_id.to_string());
            }
        }
    }

    Err("Could not extract live videoId from channel page".into())
}

async fn get_active_video_from_channel(
    client: &reqwest::Client,
    channel_id: &str,
    api_key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    match get_active_video_from_channel_web(client, channel_id).await {
        Ok(video_id) => {
            info!(
                "YouTube Ingest: Resolved videoId ({}) via zero-quota web lookup (0 quota units)",
                video_id
            );
            return Ok(video_id);
        }
        Err(e) => {
            warn!(
                "YouTube Ingest: Web lookup failed ({e}). Falling back to YouTube search.list API (100 quota units)..."
            );
        }
    }

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

    let min_poll_interval = Duration::from_secs(config.min_poll_interval_secs.unwrap_or(5));
    let adaptive_polling = config.adaptive_polling.unwrap_or(true);

    let mut page_token: Option<String> = None;
    let mut poll_interval = min_poll_interval;
    let mut idle_polls_count: u32 = 0;
    let mut consecutive_errors: u32 = 0;

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

                    // Circuit Breaker: Quota Exceeded (403) or Rate Limited (429)
                    if http_status.as_u16() == 403 || http_status.as_u16() == 429 {
                        let detail = format!(
                            "YouTube API Quota/Rate Limit Exceeded (HTTP {http_status}): {err_text}"
                        );
                        error!(
                            "YouTube Ingest: {detail}. Stopping ingest thread to protect API quota."
                        );
                        update_status(&status, "stopped", &detail, None, None).await;
                        return;
                    }

                    consecutive_errors += 1;
                    let backoff_secs = (min_poll_interval.as_secs()
                        * (1 << consecutive_errors.min(4)))
                    .min(60);
                    let detail = format!(
                        "YouTube API returned HTTP {http_status}: {err_text}. Retrying in {backoff_secs}s"
                    );
                    error!("YouTube Ingest: {detail}");
                    update_status(&status, "error", &detail, None, None).await;
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    continue;
                }

                consecutive_errors = 0;

                match resp.json::<YouTubeResponse>().await {
                    Ok(data) => {
                        let api_millis = data.polling_interval_millis.unwrap_or(5000);
                        let mut base_interval = Duration::from_millis(api_millis);
                        if base_interval < min_poll_interval {
                            base_interval = min_poll_interval;
                        }

                        page_token = data.next_page_token;
                        let item_count = data.items.len();

                        if item_count == 0 {
                            idle_polls_count += 1;
                        } else {
                            idle_polls_count = 0;
                        }

                        if adaptive_polling && idle_polls_count >= 3 {
                            let extra_secs = ((idle_polls_count - 2).min(5) * 2) as u64;
                            poll_interval = base_interval + Duration::from_secs(extra_secs);
                        } else {
                            poll_interval = base_interval;
                        }

                        let received = {
                            let current = status.lock().await;
                            current.messages_received + item_count as u64
                        };
                        let detail = if item_count == 0 {
                            format!(
                                "Last poll succeeded; no new messages. Next poll in {} ms (idle count: {})",
                                poll_interval.as_millis(),
                                idle_polls_count
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
                consecutive_errors += 1;
                let backoff_secs = (min_poll_interval.as_secs()
                    * (1 << consecutive_errors.min(4)))
                .min(60);
                let detail =
                    format!("YouTube API request failed: {e}. Retrying in {backoff_secs}s");
                error!("YouTube Ingest: {detail}");
                update_status(&status, "error", &detail, None, None).await;
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                continue;
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}
