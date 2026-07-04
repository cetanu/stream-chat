use stream_chat::client::StreamChatClient;
use stream_chat::client::config::ClientConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::parse_args();
    let client = StreamChatClient::new(config);
    client.run().await?;
    Ok(())
}
