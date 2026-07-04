use stream_chat::server::StreamChatServer;
use stream_chat::server::config::AppConfig;
use stream_chat::client::StreamChatClient;
use stream_chat::client::config::ClientConfig;
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Run both stream-chat server and client locally", long_about = None)]
pub struct LocalArgs {
    #[arg(short = 'n', long, default_value_t = 10)]
    pub limit: usize,

    #[arg(short, long, default_value = "config.json")]
    pub config: String,

    #[arg(short, long, default_value = "[::1]:50051")]
    pub address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = LocalArgs::parse();
    let config = AppConfig::load(&args.config).unwrap_or(AppConfig { twitch: None, youtube: None });
    let server = StreamChatServer::new(config);
    server.start_ingest().await;
    
    let bind_address = args.address.clone();
    tokio::spawn(async move {
        if let Err(e) = server.serve(&bind_address).await {
            eprintln!("Server error: {}", e);
        }
    });

    let client_config = ClientConfig {
        limit: args.limit,
        address: format!("http://{}", args.address),
    };
    
    let client = StreamChatClient::new(client_config);
    client.run().await?;
    Ok(())
}
