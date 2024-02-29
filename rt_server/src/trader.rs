use rdkafka::consumer::{Consumer, CommitMode};
use tracing::{debug, warn, error, info,};
use serde::{Deserialize, Serialize};

use kafka::producer::Record;
use tokio::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

use crate::market::{MarketType, MktMsgParams, LETFP, MarketSwitching, };
use crate::mkt_handler::MktEventHandler;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::publish::connect_with_retries_producer;
use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;
use crate::trade::TradeTypes;

/// LETF trader structure.
/// market_date: date when we are pricing.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
// #[derive(Debug)]
// pub struct LETFTrader {
//     kafka_server_name: String,
//     kafka_port: i32,
//     curr_mkt: Arc<Mutex<MarketType>>,
// }

// #[derive(Debug, Serialize, Deserialize)]
// pub struct RTConfig {
//     pub kafka_server_name: String,
//     pub kafka_server_port: i32,
//     pub positions_topic: String,
//     pub mkt_topic: String,
//     pub results_topic: String,
// }


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

            info!("ENTERING HEDGE");
            loop {
                let m = pos_listener_.recv().await.unwrap();  // position to be handled.
                let trade_result = TradeTypes::try_from_ref(&m);
                let trade = match trade_result {
                    Ok(TradeTypes::LETF(trade_new)) => Some(trade_new),
                    _ => None,
                };

                if trade.is_none() {
                    continue;
                }

                let mut trade = trade.unwrap();

                let stock_mkt = {
                    let cm = self._curr_mkt().lock().unwrap().clone();

                    MarketType(cm)
                    //MarketType(cm.lock().unwrap().clone())  // TODO: IMPROVE
                    //.expect("Could not lock the current market, weird");
                };
                info!("GOT MESSAGE, HEDGING {:?}", stock_mkt);

                for trade_hedge in trade.hedge(&stock_mkt) {
                    let hedge_json = serde_json::ser::to_string(&trade_hedge).unwrap();
                    debug!("Processing hedge {}", hedge_json);
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
                    error!("Could not commit to position listener on {:?}", hedge_topic);
                }
            }
        }
    }
}

// impl LETFTrader {
//     pub fn new(kafka_server_name: String, kafka_port: i32) -> Self {
//         // if given the params, use them, otherwise construct empty map

//         Self {
//             kafka_server_name,
//             kafka_port,
//             curr_mkt: Arc::new(Mutex::new(MarketType::new())),
//         }
//     }

//     /// constructs the controller from configuration read from the file.
//     /// config_file. If it cant read the file properly, it crashes.
//     pub fn new_from_config(config_file: String) -> Result<Self, Box<dyn std::error::Error>> {
//         let config_f = std::fs::File::open(config_file).unwrap();
//         let config_map: RTConfig = serde_yaml::from_reader(config_f)?;

//         Ok(Self::new(
//             config_map.kafka_server_name,
//             config_map.kafka_server_port,
//         ))
//     }


//     /// starts the controller threads.
//     /// 2 threads at the moment:
//     ///    1st: handles market events and updates the market.
//     ///    2nd: handles position events and returns hedges.
//     pub async fn start(
//         &self,
//         pos_topic: String,
//         mkt_topic: String,
//         results_topic: String, // publish the results topic
//     ) {
//         let (mkt_sender, _) = channel::<MarketType>(100);  // TODO: THIS IS SHIT HERE: 100
// 	    let (fut_mkt_ready_s, _fut_mkt_ready_r) = channel::<bool>(100); // TODO: BUFFER SIZE SHOULD BE ???

// 	    tokio_scoped::scope(
//             |scope| {
//                 scope.spawn(
//                     self._handle_mkt_events(
//                         mkt_topic,
//                         MktMsgParams::LETFParams(LETFP {
//                             curr_mkt: self.curr_mkt.clone(),
//                         }),
//                         mkt_sender,
// 		                fut_mkt_ready_s,
//                     )
//                 );

//                 scope.spawn(
//                     self.__hedger(pos_topic, results_topic)
//                 );
//             }
//         )
//     }
// }

// impl Streaming for LETFTrader {
//     fn kafka_server_name(&self) -> String {
//         self.kafka_server_name.clone()
//     }

//     fn kafka_port(&self) -> i32 {
//         self.kafka_port
//     }
// }

// impl MktEventHandler for LETFTrader {
//     /// updates the local market variable.
//     fn _handle_mkt_msg(
//         &self,
//         market_obj: MarketType,
//         _new_mkt_sender: Sender<MarketType>,
//         mkt_params: MktMsgParams,
//     ) -> impl std::future::Future<Output=()> + Send {
//         async move {
//             info!("ENTERING HANDLE");
//             let MktMsgParams::LETFParams(letf_mkt) = mkt_params else {
//                 warn!("_handle_mkt_msg: Parameters provided to MktEventHandler are of wrong type");
//                 return;
//             };

//             *letf_mkt.curr_mkt.lock().unwrap() += &market_obj;
//         }
//     }
// }
