use super::{types::*, pubsub::Topic};

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use tonic::Status;

/// provides a primitive pubsub service supporting a single global topic, in that all
/// messages are sent to all subscribers, regardless of the message topic.
/// 
/// message topic and message content values must be strings, if the data you are sending
/// is not string type, than base64 encode the data first
#[tonic::async_trait]
impl pub_sub_server::PubSub for PubSubState {
    type SubStream = ReceiverStream<super::types::Update>;

    /// the pubsub server will take the incoming `channels` and add it to the list of active subscriptions
    /// any messages that are then published through the service will be forwarded to the input `channels`
    async fn sub(
        &self,
        channels: tonic::Request<tonic::Streaming<String>>,
    ) -> Result<tonic::Response<Self::SubStream>, Status> {

        let mut channel = channels.into_inner();
        let msg = if let Ok(Some(msg)) = channel.message().await {
            msg
        } else {
            return Err(Status::new(tonic::Code::Internal, "failed to get message"));
        };
        let (_, receiver) = self.subscribers.clone().subscribe(msg);
        let stream = ReceiverStream::new(receiver);

        Ok(tonic::Response::new(stream))
    }

    async fn publish( 
        &self,
        kvp: tonic::Request<(String, String)>,
    ) -> Result<tonic::Response<()>, Status> {
        let (key, value) = kvp.into_inner();
        self.subscribers.clone().publish(key.clone(), Ok((key, value)));
        Ok(tonic::Response::new(()))
    }
}