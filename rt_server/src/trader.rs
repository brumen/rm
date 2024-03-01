use rdkafka::consumer::{Consumer, CommitMode};
use tracing::error;

use kafka::producer::Record;

use crate::market::{MarketType, MarketSwitching, };
use crate::portfolio_sender::connect_with_retries_rd;
use crate::publish::connect_with_retries_producer;
use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;
use crate::trade::TradeTypes;


pub trait LETFHedger : Streaming + MarketSwitching
    where Self: Sync
{

    /// listens to kafka stream for trades and responds to incoming trades.
    ///
    fn hedge(
        &self,
        pos_topic: String,
        hedge_topic: String,
    ) -> impl std::future::Future<Output=()> + Send {
        async move {
            let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());
            let pos_listener_ = connect_with_retries_rd(&bootstrap_servers, &pos_topic);
            let mut hedge_book = connect_with_retries_producer(&bootstrap_servers);

            loop {
                let m = pos_listener_.recv().await.unwrap();  // position to be handled.
                let trade_result = TradeTypes::try_from_ref(&m);
                let trade = match trade_result {
                    Ok(TradeTypes::LETF(trade_new)) => Some(trade_new),
                    Err(e) => {
                        error!("Could not convert message {:?} to LETF trade: {:?}", m, e);
                        continue
                    },
                    _ => {
                        continue
                    },
                };

                let mut trade = trade.unwrap();

                let stock_mkt = {
                    let cm = self._curr_mkt().lock().unwrap().clone();

                    MarketType(cm)
                };

                for trade_hedge in trade.hedge(&stock_mkt) {
                    let hedge_json = serde_json::ser::to_string(&trade_hedge).unwrap();
                    let hedge_record = Record::from_value(&hedge_topic, hedge_json.as_bytes())
                        .with_partition(0);

                    let _ = hedge_book.send(&hedge_record);
                }
                let trade_itself =
                    serde_json::ser::to_string(&TradeTypes::LETF(trade)).unwrap();
                let trade_itself_record =
                    Record::from_value(&hedge_topic, trade_itself.as_bytes()).with_partition(0);
                let _ = hedge_book.send(&trade_itself_record); // Trade itself is sent to the book.

                if let Err(commit_error) = pos_listener_.commit_message(&m, CommitMode::Async) {
                    error!("Could not commit to position listener on {:?}: {:?}", hedge_topic, commit_error);
                }
            }
        }
    }
}
