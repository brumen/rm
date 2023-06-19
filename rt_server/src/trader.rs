use log::{debug, warn, };
use serde::{Deserialize, Serialize};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, };
use kafka::producer::{Producer, Record, RequiredAcks};
use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, Mutex,};
use std::sync::mpsc::{channel, Sender, };

use crate::trade::TradeTypes;
use crate::streaming::Streaming;
use crate::market::{MarketType, MktMsgParams, LETFP,};
use crate::mkt_handler::MktEventHandler;
use crate::ref_deref::TryFromRef;

pub type PricingParams = HashMap<String, f64>;


/// LETF trader structure.
/// market_date: date when we are pricing.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
pub struct LETFTrader {
    kafka_server_name: String,
    kafka_port: i32,
    curr_mkt : Arc<Mutex<MarketType>>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct RTConfig {
    kafka_server_name: String,
    kafka_server_port: i32,
}


impl LETFTrader {
    pub fn new(
        kafka_server_name: String,
        kafka_port: i32,
    ) -> Self {
        // if given the params, use them, otherwise construct empty map

        Self {
            kafka_server_name,
            kafka_port,
            curr_mkt: Arc::new(Mutex::new(MarketType::new())),
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(config_file: String) -> Result<Self, Box<dyn std::error::Error>> {

        let config_f = std::fs::File::open(config_file).unwrap();
        let config_map: RTConfig = serde_yaml::from_reader(config_f).unwrap();

        Ok(Self::new(
            config_map.kafka_server_name,
            config_map.kafka_server_port,
        ))
    }

    /// listens to kafka stream for trades and responds to incoming trades.
    ///
    fn __hedger(
        &self,
        pos_topic: String,
        hedge_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name, self.kafka_port);

        let mut pos_listener_ = Consumer::from_hosts(vec![format!(
            "{}:{}",
            self.kafka_server_name, self.kafka_port
        )])
            .with_topic_partitions(pos_topic, &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        let mut hedge_book = Producer::from_hosts(vec![bootstrap_servers,])
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        loop {
            for ms in pos_listener_.poll().unwrap().iter() {
                for m in ms.messages() {

                    let trade_result = TradeTypes::try_from_ref(m);

                    let trade = match trade_result {
                        Ok(TradeTypes::LETF(trade_new)) => Some(trade_new),
                        _ => None,
                    };

                    if trade.is_none() {
                        continue;
                    }

                    let trade = trade.unwrap();
                    let stock_name = &trade.stock;
                    let stock_mkt = self.curr_mkt.lock().expect("Could not lock the current market, weird");
                    let stock_value = stock_mkt.get(stock_name);
                    if stock_value.is_none() {  // returns empty hedge if it cant determine the stock value.
                        warn!("Can't find the value of stock {}", stock_name);
                        continue;  // TODO: THIS IS WRONG HERE.
                        //return vec![];
                    }

                    for trade_hedge in trade.hedge(*stock_value.unwrap()) {
                        let hedge_json = serde_json::ser::to_string(&trade_hedge).unwrap();
                        debug!("Processing hedge {}", hedge_json);
                        let hedge_record = Record::from_value(&hedge_topic, hedge_json.as_bytes())
                            .with_partition(0);

                        let _ = hedge_book.send(&hedge_record);
                    }
                }
                let _ = pos_listener_.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            pos_listener_.commit_consumed().unwrap();
        }
    }

    /// starts the controller threads.
    /// 2 threads at the moment:
    ///    1st: handles market events and updates the market.
    ///    2nd: handles position events and returns hedges.
    pub fn start(
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
    ) {

        let (mkt_sender, mkt_receiver) = channel::<MarketType>();
        // threads fail if any of them can not be created.
        thread::scope(|s| {
            let _ = thread::Builder::new()
                .name("handle_mkt_events".to_string())
                .spawn_scoped(s, move || {
                    let (useless_sender, _) = channel::<MarketType>();
                    self._handle_mkt_events(
                        mkt_topic,
                        MktMsgParams::LETFParams(
                            LETFP {
                                curr_mkt: self.curr_mkt.clone(),
                                new_mkt_sender: useless_sender,
                            }
                        ),
                        mkt_sender,
                    );
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("hedger".to_string())
                .spawn_scoped(s, move || {
                    self.__hedger(
                        pos_topic,
                        results_topic,
                    );
                })
                .unwrap();
        });
    }
}

impl Streaming for LETFTrader {
    fn kafka_server_name(&self) -> String {
        self.kafka_server_name.clone()
    }

    fn kafka_port(&self) -> i32 {
        self.kafka_port
    }
}

impl MktEventHandler for LETFTrader {

    /// updates the local market variable.
    fn _handle_mkt_msg(
        &self,
        mkt_msg: &Message,
        new_mkt_sender: Sender<MarketType>,
        mkt_params: MktMsgParams,
    ) {

        let new_market = MarketType::try_from_ref(&mkt_msg);
        debug!("Got quote: {:?}", new_market);
        if new_market.is_err() {
            return;  // ignore the market message if it cant be decoded correctly.
        }

        let new_quote_mkt = new_market.unwrap();
        let MktMsgParams::LETFParams(letf_mkt) = mkt_params else {
            warn!("Parameters provided to MktEventHandler are of wrong type");
            return;
        };
        let mut curr_mkt_tmp = letf_mkt.curr_mkt.lock().unwrap();  // lock the current market
        for (new_quote, new_value) in new_quote_mkt.iter() {
            curr_mkt_tmp.insert(new_quote.to_string(), *new_value);
        }
    }
}
