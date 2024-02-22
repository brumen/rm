use tokio::sync::mpsc::Sender;
use tracing::{info, instrument, warn, debug};

use crate::market::{MarketType, MktMsgParams};
use crate::portfolio_sender::connect_with_retries_rd;
use crate::streaming::Streaming;
use crate::ref_deref::TryFromRef;

pub trait MktEventHandler: Streaming
    where Self: std::fmt::Debug + Sync,
{
    fn _handle_mkt_msg(
        &self,
        market_obj: MarketType,
        new_mkt_sender: Sender<MarketType>,
        mkt_params: MktMsgParams,
    ) -> impl std::future::Future<Output=()> + Send;


    /// Loop that handles the market events
    /// mkt_topic - receiving market events from this topic
    /// new_mkt_sender - sending the new market to the pricing api
    /// switch_mkt_recv - receiver receiving the event when to switch markets.
    //#[instrument]
    fn _handle_mkt_events(
        &self,
        mkt_topic: String,
        mkt_params: MktMsgParams,
        new_mkt_sender: Sender<MarketType>,
	    fut_mkt_ready_s: Sender<bool>,
    ) -> impl std::future::Future<Output=()> + Send {
        async move {
            let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());

            let mkt_listener_ = connect_with_retries_rd(&bootstrap_servers, &mkt_topic);

	        // listens to the stream and sends messages
	        loop {
	            let borrowed_msg = mkt_listener_.recv().await.unwrap();  // TODO: HANDLE THIS PROPERLY NOT UNWRAP!!!
	            info!("Getting new markets from {mkt_topic}.");

                let optional_mkt = MarketType::try_from_ref(&borrowed_msg);

                let market_obj = match optional_mkt {
                    Err(e) => {
                        warn!(
                            "_handle_mkt_msg: Error converting to market object from json: {:?}",
                            e
                        );
                        return;
                    },
                    Ok(market_inside) => {
                        debug!("_handle_mkt_msg: Market = {:?}", market_inside);
                        market_inside
                    }
                };

                self._handle_mkt_msg(market_obj, new_mkt_sender.clone(), mkt_params.clone()).await;

	            if let Err(e) = fut_mkt_ready_s.send(true).await {
                    warn!("Could not send a message that future market is ready: {:?}", e);
                }
	        }
        }
    }
}
