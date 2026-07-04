use stream_chat::server::StreamChatServer;
use stream_chat::server::config::AppConfig;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    info!("Starting Stream Chat Server...");
    let config = AppConfig::load().expect("config.json not found or invalid");

    let server = StreamChatServer::new(config);
    server.start_ingest().await;
    server.serve("[::1]:50051").await?;

    Ok(())
}
