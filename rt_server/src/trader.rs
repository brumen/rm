use log::{debug, warn};
use serde::{Deserialize, Serialize};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message,};
use kafka::producer::{Producer, Record, RequiredAcks};
use serde_yaml;
use std::collections::HashMap;
use std::thread;
use time::Date;
use core::convert::From;
use std::sync::{Arc, Mutex,};
use uuid::Uuid;

use crate::trade::{
    LETFTrade,
    LETFFuture,
    LETFCash,
    LETFHedge,
};
use crate::pricer::{PricingMetric, PricingStruct,};
use crate::market::MarketType;

pub type PricingParams = HashMap<String, f64>;


/// Controller structure.
/// market_date: date when we are pricing.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
pub struct LETFTrader {
    market_date: Date,
    pricing_params: PricingParams,
    kafka_server_name: String,
    kafka_port: i32,
    trade_pricer: String,
    metric: PricingMetric,
    curr_mkt : Arc<Mutex<MarketType>>,
    pos_topic: String,
    hedge_topic: String,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct RTConfig {
    kafka_server_name: String,
    kafka_server_port: i32,
    mkt_topic: String,
    results_topic: String,
    pos_topic: String,
    trade_pricer: String,
    pricing_params: PricingStruct,
    metric: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum CurrNewMarket {
    Current,
    New,
}

impl LETFTrader {
    pub fn new(
        market_date: Date,
        pricing_params_: Option<PricingParams>,
        kafka_server_name: String,
        kafka_port: i32,
        trade_pricer: String,
        metric: PricingMetric,
        pos_topic: String,
        hedge_topic: String,
    ) -> Self {
        // if given the params, use them, otherwise construct empty map
        let pricing_init = match pricing_params_ {
            Some(pricing_hash) => pricing_hash,
            None => PricingParams::new(),
        };

        Self {
            market_date: market_date,
            pricing_params: pricing_init,
            kafka_server_name,
            kafka_port,
            trade_pricer,
            metric,
            curr_mkt: Arc::new(Mutex::new(MarketType::new())),
            pos_topic,
            hedge_topic,
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(
        market_date: Date,
        config_file: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file).unwrap();
        let config_map: RTConfig = serde_yaml::from_reader(config_f).unwrap();

        let controller_metric;
        if config_map.metric == "PV".to_string() {
            controller_metric = PricingMetric::PV;
        } else {
            controller_metric = PricingMetric::PV01;
        };

        Ok(Self::new(
            market_date,
            // pricing_params
            Some(HashMap::from([
                ("nb_sim".to_owned(), config_map.pricing_params.nb_sim as f64),
                (
                    "default_price".to_string(),
                    config_map.pricing_params.default_price,
                ),
            ])),
            config_map.kafka_server_name,
            config_map.kafka_server_port,
            config_map.trade_pricer,
            controller_metric,
            config_map.pos_topic,
            config_map.results_topic,
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
        ).to_owned()])
            .with_topic_partitions(pos_topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        let mut hedge_book = Producer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        loop {
            for ms in pos_listener_.poll().unwrap().iter() {
                for m in ms.messages() {

                    let trade_v = self._decode_trade(m);
                    debug!("Procesing trade {:?}", trade_v);
                    if trade_v.is_none() {
                        continue;  // hope is lost for this trade, continue
                    }

                    let trade_hedges = self._hedge_trade(trade_v.unwrap());

                    for trade_hedge in trade_hedges {

                        let hedge_json = serde_json::ser::to_string(&trade_hedge).unwrap();
                        let hedge_msg = format!("{{\"trade\": {}}}", hedge_json);
                        let hedge_record = Record::from_value(hedge_topic.as_str(), hedge_msg.as_bytes())
                            .with_partition(0);

                        let _ = hedge_book.send(&hedge_record);
                    }
                }
                let _ = pos_listener_.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            pos_listener_.commit_consumed().unwrap();
        }
    }

    /// Returns the trades which are the hedge against the existing trade.
    fn _hedge_trade(&self, trade: LETFTrade) -> Vec<LETFHedge> {
        let beta = trade.beta;
        let amount = trade.amount;
        let stock_name = trade.stock;

        let stock_binding = self.curr_mkt.lock().expect("Could not lock the current market, weird");
        let stock_value = stock_binding.get(&stock_name);

        if stock_value.is_none() {  // returns empty hedge if it cant determine the stock value.
            warn!("Can't find the value of stock {}", stock_name);
            return vec![];
        }

        let exposure_amt = beta * amount * stock_value.unwrap();

        vec![
            LETFHedge::Future( LETFFuture {
                trade_id: Uuid::new_v4().to_string(),
                stock: stock_name,
                amount: exposure_amt }
            ),
            LETFHedge::Cash(LETFCash {
                trade_id: Uuid::new_v4().to_string(),
                amount : - exposure_amt } ),
        ]
    }

    /// decodes the trade from kafka and returns it.
    /// if problems w/ transformation, return None
    fn _decode_trade(&self, message : &Message) -> Option<LETFTrade> {

        match std::str::from_utf8(message.value) {
            Ok(m_value_str) => {
                match serde_json::from_str::<LETFTrade>(m_value_str) {
                    Ok(msg_decoded) => {
                        return Some(msg_decoded);  // TODO: FIX THIS HERE!!!
                    },
                    Err(e) => {
                        warn!("Could not convert trade message {:?} to LETFTrade: {:?}", m_value_str, e);
                        return None;
                    },
                }
            },
            Err(e) => {
                warn!("Could not convert trade message {:?} from utf8: {:?}", message.value, e);
                return None;
            }
        }
    }

    /// updates the local market variable.
    pub fn _handle_mkt_events(
        &self,
        mkt_topic: String,
    ) {

        let mut mkt_listener_ = Consumer::from_hosts(vec![format!(
            "{}:{}",
            self.kafka_server_name, self.kafka_port
        ).to_owned()])
            .with_topic_partitions(mkt_topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        loop {
            for ms in mkt_listener_.poll().unwrap().iter() {  // TODO: What to do w/ unwrap here??
                debug!("Getting new quotes from {mkt_topic}.");
                for m in ms.messages() {
                    let decoded_market_msg = self._decode_market_msg(m);
                    debug!("Got quote: {:?}", decoded_market_msg);
                    if decoded_market_msg.is_none() {
                        continue;  // ignore the market message if it cant be decoded correctly.
                    }
                    let new_quote_mkt = decoded_market_msg.unwrap();
                    let mut curr_mkt_tmp = self.curr_mkt.lock().unwrap();  // lock the current market
                    for (new_quote, new_value) in new_quote_mkt.iter() {
                        let _ = curr_mkt_tmp.insert(new_quote.to_string(), *new_value);
                    }
                }
                let _ = mkt_listener_.consume_messageset(ms);
            }
            mkt_listener_.commit_consumed().unwrap();
        }
    }

    /// decodes the market message
    /// if there is any mistake
    fn _decode_market_msg(&self, market_msg : &Message) -> Option<HashMap<String, f64>> {

        let message_utf = std::str::from_utf8(market_msg.value);

        if message_utf.is_err() {
            warn!("Could not decode the market message into UTF8, continuing w/o");
            return None;
        }
        let message_json = serde_json::from_str::<HashMap<String, f64>>(message_utf.unwrap());

        if message_json.is_err() {
            warn!("Could not decode to JSON. Continuing w/ next market: {:?}", message_utf.unwrap());
            return None;
        }

        Some(message_json.unwrap())
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

        // threads fail if any of them can not be created.
        thread::scope(|s| {
            let _ = thread::Builder::new()
                .name("handle_mkt_events".to_string())
                .spawn_scoped(s, move || {
                    self._handle_mkt_events(
                        mkt_topic,
                    );
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("hedger".to_string())
                .spawn_scoped(s, move || {
                    self.__hedger(
                        pos_topic.clone(),
                        results_topic.clone(),
                    );
                })
                .unwrap();
        });
    }
}
