use rdkafka::consumer::{CommitMode, Consumer};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};

use crate::market::{MarketType, MktMsgParams};
use crate::portfolio_sender::connect_with_retries_rd;
use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;

pub trait MktEventHandler: Streaming
where
    Self: std::fmt::Debug + Sync,
{
    fn _handle_mkt_msg(
        &self,
        market_obj: MarketType,
        new_mkt_sender: Sender<MarketType>,
        mkt_params: MktMsgParams,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Loop that handles the market events
    /// mkt_topic - receiving market events from this topic
    /// new_mkt_sender - sending the new market to the pricing api
    /// switch_mkt_recv - receiver receiving the event when to switch markets.
    fn _handle_mkt_events(
        &self,
        mkt_topic: String,
        mkt_params: MktMsgParams,
        new_mkt_sender: Sender<MarketType>,
        _fut_mkt_ready_s: Sender<bool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());

            info!("Starting market event handling.");
            let mkt_listener_ = connect_with_retries_rd(&bootstrap_servers, &mkt_topic);

            // listens to the stream and sends messages
            loop {
                let borrowed_msg = match mkt_listener_.recv().await {
                    Ok(mkt_msg) => mkt_msg,
                    Err(e) => {
                        error!("Error getting mkt message from Kafka: {:?}", e);
                        continue;
                    }
                };

                let market_obj = match MarketType::try_from_ref(&borrowed_msg) {
                    Err(e) => {
                        error!(
                            "Error converting to market object from json: {:?}. Ignoring.",
                            e
                        );
                        continue;
                    }
                    Ok(market_inside) => market_inside,
                };

                self._handle_mkt_msg(market_obj, new_mkt_sender.clone(), mkt_params.clone())
                    .await;

                // TODO: CHECK THIS AT SOME LATER STAGE???
                //if let Err(e) = fut_mkt_ready_s.send(true).await {
                //    warn!("Could not send a message that future market is ready: {:?}", e);
                //}

                match mkt_listener_.commit_message(&borrowed_msg, CommitMode::Sync) {
                    Ok(_) => {
                        debug!("Successful commit of market message");
                    }
                    Err(e) => {
                        error!("Message could not be committed to Kafka. Continuing in best hopes: {:?}", e);
                    }
                }
            }
        }
    }
}
