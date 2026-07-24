use clap::Parser;
use stream_chat::server::StreamChatServer;
use stream_chat::server::config::{AppConfig, ServerArgs};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = ServerArgs::parse();

    info!("Starting Stream Chat Server...");
    let config = AppConfig::load(&args.config).unwrap_or(stream_chat::server::config::AppConfig {
        twitch: None,
        youtube: None,
    });

    let server = StreamChatServer::new(config);
    server.start_ingest().await;
    server.serve(&args.address).await?;

    Ok(())
}
