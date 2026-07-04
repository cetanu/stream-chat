use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct ServerArgs {
    #[arg(short, long, default_value = "[::1]:50051")]
    pub address: String,

    #[arg(short, long, default_value = "config.json")]
    pub config: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TwitchConfig {
    pub channel: String,
    pub username: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct YouTubeConfig {
    pub api_key: String,
    pub live_chat_id: Option<String>,
    pub video_id: Option<String>,
    pub channel_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub twitch: Option<TwitchConfig>,
    pub youtube: Option<YouTubeConfig>,
}

impl AppConfig {
    pub fn load(path: &str) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        let config = serde_json::from_str::<AppConfig>(&content).ok()?;
        Some(config)
    }
}
