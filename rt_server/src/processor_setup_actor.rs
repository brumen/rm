use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::consumer::StreamConsumer;
use rdkafka::Message;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tracing::info;

use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::PricingMetric;
use crate::processor_msg::ProcessorMiddleMessage;

/// Actor that listens for setup messages from Kafka and sends metrics to processors
pub struct SetupActor {
    pub setup_listener: StreamConsumer,
    pub processors: Vec<ActorRef<ProcessorMiddleMessage<String>>>,
}

#[derive(Deserialize, Debug, Clone)]
pub enum SetupRequest {
    Metrics(Vec<PricingMetric>),
}

impl SetupActor {
    pub fn new(
        kafka_server: String,
        topic: String,
        processors: Vec<ActorRef<ProcessorMiddleMessage<String>>>,
    ) -> Self {
        info!("Starting SetupActor on topic {:?}", topic);
        let setup_listener = connect_with_retries_rd(&kafka_server, &topic);
        Self {
            setup_listener,
            processors,
        }
    }
}

pub(crate) async fn start_setup_actor(
    kafka_server: String,
    topic: String,
    processors: Vec<ActorRef<ProcessorMiddleMessage<String>>>,
) -> JoinHandle<()> {
    let setup_actor = SetupActor::new(kafka_server, topic, processors);
    let (_setup_process_a, setup_process_handle) = Actor::spawn(None, setup_actor, ())
        .await
        .expect("Could not start setup_actor");

    setup_process_handle
}

#[async_trait]
impl Actor for SetupActor {
    type Msg = SetupRequest;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("SetupActor initialized. Handles architecture setup.");
        Ok(())
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

        info!("Waiting on first setup message...");
        // Receive the first setup message
        let msg = self.setup_listener.recv().await?;
        let payload = msg.payload_view::<str>().unwrap().unwrap();
        let setup_req: SetupRequest = serde_json::from_str(payload)?;
        info!("Initial setup request received: {:?}", setup_req);
        myself.send_message(setup_req)?;

        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SetupRequest::Metrics(pricing_metrics) => {
                info!("Sending metrics {:?} to all relevant processors.", pricing_metrics);
                for proc in &self.processors {
                    let _ =
                        proc.send_message(ProcessorMiddleMessage::Metric(pricing_metrics.clone()));
                }

                info!("Listening on a setup request...");
                let msg = self.setup_listener.recv().await?;
                let payload = msg.payload_view::<str>().unwrap().unwrap();
                let setup_req: SetupRequest = serde_json::from_str(payload)?;
                info!("Received setup request: {:?}", setup_req);

                // Continue listening for next message
                myself.send_message(setup_req)?;
            }
        }
        Ok(())
    }
}
