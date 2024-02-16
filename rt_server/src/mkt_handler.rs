use tokio::sync::mpsc::Sender;
use rdkafka::message::BorrowedMessage;
use tracing::info;

use crate::market::{MarketType, MktMsgParams};
use crate::portfolio_sender::connect_with_retries_rd;
use crate::streaming::Streaming;

pub trait MktEventHandler: Streaming {
    async fn _handle_mkt_msg(
        &self,
        mkt_msg: BorrowedMessage,
        new_mkt_sender: Sender<MarketType>,
        mkt_params: MktMsgParams,
    );

    /// Loop that handles the market events
    /// mkt_topic - receiving market events from this topic
    /// new_mkt_sender - sending the new market to the pricing api
    /// switch_mkt_recv - receiver receiving the event when to switch markets.
    async fn _handle_mkt_events(
        &self,
        mkt_topic: String,
        mkt_params: MktMsgParams,
        new_mkt_sender: Sender<MarketType>,
	fut_mkt_ready_s: Sender<bool>,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());

        let mkt_listener_ = connect_with_retries_rd(&bootstrap_servers, &mkt_topic);

	// listens to the stream and sends messages
	loop {
	    let borrowed_msg = mkt_listener_.recv().await.unwrap();  // TODO: HANDLE THIS PROPERLY NOT UNWRAP!!!
	    info!("_handle_mkt_events: Getting new markets from {mkt_topic}.");
            self._handle_mkt_msg(borrowed_msg, new_mkt_sender.clone(), mkt_params.clone());
	    let _ = fut_mkt_ready_s.send(true);
	}
    }
}
