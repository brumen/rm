//! Kafka ingress applies backpressure before enqueuing actor messages. Each
//! delivery carries the immutable value received, not a later registry lookup.
use crate::market::MarketTypeT;
use crate::portfolio_sender::connect_with_retries_rd_with_offset_store;
use crate::processor_flow::{Endpoint, Task};
use crate::processor_msg::TradeEvent;
use crate::ref_deref::TryFromRef2;
use crate::trade::{BaseTrade, TradeRep};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde::Deserialize;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{error, info};

pub(crate) struct TradeProducer<T, MT> {
    position_listener: Arc<StreamConsumer>,
    processors: Vec<Endpoint<T, MT>>,
    trade_list: Arc<TradeRep<T>>,
}
impl<T, MT> TradeProducer<T, MT> {
    pub fn new(
        kafka_server: String,
        topic: String,
        processors: Vec<Endpoint<T, MT>>,
        trade_list: Arc<TradeRep<T>>,
    ) -> Self {
        Self {
            position_listener: Arc::new(connect_with_retries_rd_with_offset_store(
                &kafka_server,
                &topic,
                false,
            )),
            processors,
            trade_list,
        }
    }
}
#[async_trait]
impl<T, MT> Actor for TradeProducer<T, MT>
where
    T: Send
        + Sync
        + Clone
        + BaseTrade
        + for<'a> Deserialize<'a>
        + TryFromRef2<crate::trade::TradeError>
        + 'static,
    MT: MarketTypeT + Send + Sync + 'static,
{
    type Msg = ();
    type State = Option<Task>;
    type Arguments = ();
    async fn pre_start(&self, _: ActorRef<()>, _: ()) -> Result<Self::State, ActorProcessingErr> {
        Ok(None)
    }
    async fn post_start(
        &self,
        myself: ActorRef<()>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let listener = self.position_listener.clone();
        let processors = self.processors.clone();
        let trades = self.trade_list.clone();
        *state = Some(Task(tokio::spawn(async move {
            info!("Trade ingress started with bounded per-processor admission");
            let mut version = 0u64;
            loop {
                let message = match listener.recv().await {
                    Ok(message) => message,
                    Err(reason) => {
                        error!(%reason, "Trade receive failed");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let trade = match T::try_from_ref(&message) {
                    Ok(trade) => trade,
                    Err(reason) => {
                        error!(?reason, "Invalid trade message; skipping");
                        let _ = listener.store_offset_from_message(&message);
                        continue;
                    }
                };
                version = version.checked_add(1).expect("Trade version overflow");
                let id = trade.id();
                trades.upsert_async(id.clone(), trade.clone()).await;
                let event = Arc::new(TradeEvent {
                    id,
                    version,
                    value: trade,
                    received: Instant::now(),
                });
                for processor in &processors {
                    if let Err(reason) = processor.send_trade(event.clone()).await {
                        error!(%reason, "Trade pipeline stopped; offset not advanced");
                        myself.stop(Some(reason));
                        return;
                    }
                }
                // Auto-commit may advance only past events admitted to every
                // processor. This preserves the existing consumer-group policy.
                if let Err(reason) = listener.store_offset_from_message(&message) {
                    error!(%reason, "Could not store admitted trade offset");
                }
            }
        })));
        Ok(())
    }
    async fn handle(
        &self,
        _: ActorRef<()>,
        _: (),
        _: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}
