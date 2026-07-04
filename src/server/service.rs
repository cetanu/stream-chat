use crate::proto::chat_service_server::ChatService;
use crate::proto::{ChatMessage, GetMessagesRequest, GetMessagesResponse};
use crate::server::state::{SharedQueue, save_queue_to_disk};
use tonic::{Request, Response, Status};
use tracing::info;

pub struct ChatServerImpl {
    pub queue: SharedQueue,
}

#[tonic::async_trait]
impl ChatService for ChatServerImpl {
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
