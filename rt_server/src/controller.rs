use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use reqwest;
use serde_json::Value;
use serde_yaml;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, RecvError, Sender, TryRecvError};
use std::thread;
use std::time::Duration;
use string_join::Join;
use time::{format_description, Date};

use crate::encdec::EncoderDecoder;
use crate::trade::{Trade, TradeDirection, TradeHandling};

pub type PricingParams = HashMap<String, f64>;
pub type MarketType = HashMap<(String, Date), f64>;
pub type TradeValue = HashMap<(String, Date), f64>;
pub type PortfolioType = HashMap<(String, Date), f64>;

/// Controller structure.
/// MK ... market key, for our specific purpose its gonna be (&str, Date)
/// MV ... market value, for our purpose its f64
/// positions are in the form: (u8, TradeDirection)
pub struct Controller {
    market_date: Date,
    pricing_params: PricingParams,
    kafka_server_name: String,
    kafka_port: i32,
    trade_pricer: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PricingStruct {
    nb_sim: i32,
    default_price: f64,
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
}

impl Controller {
    pub fn new(
        market_date: Date,
        pricing_params_: Option<HashMap<String, f64>>,
        kafka_server_name: String,
        kafka_port: i32,
        trade_pricer: String,
    ) -> Self {
        // if given the params, use them, otherwise construct empty map
        let pricing_init = match pricing_params_ {
            Some(pricing_hash) => pricing_hash,
            None => HashMap::new(),
        };

        Controller {
            market_date: market_date,
            pricing_params: pricing_init,
            kafka_server_name,
            kafka_port,
            trade_pricer,
        }
    }

    /// constructs the controller from configuration read from the file.
    pub fn new_from_config(
        market_date: Date,
        kafka_server_name: String,
        kafka_port: i32,
        trade_pricer: String,
        config_file: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file).unwrap();
        let config_map: RTConfig = serde_yaml::from_reader(config_f).unwrap();

        Ok(Controller::new(
            market_date,
            Some(HashMap::from([
                ("nb_sim".to_owned(), config_map.pricing_params.nb_sim as f64),
                (
                    "default_price".to_string(),
                    config_map.pricing_params.default_price,
                ),
            ])),
            kafka_server_name,
            kafka_port,
            trade_pricer,
        ))
    }

    /// Aggregating the results of two trades.
    fn _trade_result_agg_single(
        exist_market: &mut MarketType,
        trade_pv_result: Option<TradeValue>,
    ) {
        match trade_pv_result {
            None => (),
            Some(trade_pv) => {
                // aggregate 2 hashmaps, one for exiting market, one from the trade_pv_2
                for (trade_id, trade_value) in trade_pv.iter() {
                    if exist_market.contains_key(trade_id) {
                        exist_market
                            .insert(trade_id.clone(), exist_market[trade_id] + *trade_value);
                    } else {
                        exist_market.insert(trade_id.clone(), *trade_value);
                    }
                }
            }
        }
    }

    /// Values the trade id
    /// Makes a call to the rester service, which values the trade.
    fn _value_trade(&self, trade: &Trade) -> TradeValue {
        let trade_id = trade.trade_id;

        //"http://localhost:5010/pv/{trade_id}"
        let result_pricing =
            reqwest::blocking::get(format!("http://{}/pv/{}", self.trade_pricer, trade_id));

        let result = match result_pricing {
            Ok(result_price) => {
                match result_price.json::<HashMap<String, f64>>() {
                    Ok(result_pricer_inner) => result_pricer_inner,
                    Err(e) => {
                        warn!("Could not conver the result to a map: {:?}", e);
                        return TradeValue::new();

                    }
                }
            },
            Err(e) => {
                warn!("Trade {trade_id} could not price correctly: {}", e);
                return TradeValue::new(); // TODO: THIS SHOULD BE DIFFERENT, CORRECT
            },
        };

        debug!("Valuing trade {:?}", result);

        // we have the price, copy the market date in it.
        let mut result_tv = TradeValue::new();

        for (trade_obj, trade_res) in result.iter() {
            let _ = &result_tv.insert((trade_obj.clone(), self.market_date), *trade_res);
        }

        result_tv
    }

    /// Constructing the current market.
    /// receives trades on the trade_receiver channel.
    /// publishes current market results on the curr_mkt_sender channel.
    pub fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<Trade>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<PortfolioType>,
    ) {
        let mut curr_portfolio = PortfolioType::new();

        loop {
            let new_portfolio_received = new_portfolio_receiver.try_recv(); // this will not block
            match new_portfolio_received {
                Ok(new_portfolio) => {
                    info!("Switching current <- new market.");
                    curr_portfolio = new_portfolio;
                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());
                }
                _ => {}
            };

            match trade_receiver.try_recv() {
                Ok(trade) => {
                    info!("CURR market: valuing trade {}", trade.trade_id);
                    let trade_value = self._value_trade(&trade);

                    Controller::_trade_result_agg_single(&mut curr_portfolio, Some(trade_value));
                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());
                }
                _ => {}
            };
        }
    }

    fn _price_trades_on_spark(&self, trades: &Vec<Trade>) -> PortfolioType {
        let mut new_portfolio = PortfolioType::new();

        // TODO: REWRITE THIS, THIS IS SHIT
        // let k = |trade: Trade| -> String { trade.trade_id.to_string() };
        // let s = ",".join(
        //     *trades
        //         .into_iter()
        //         .map(|trade: &Trade| -> String { trade.trade_id.to_string() })
        //         .collect(),
        // );

        let mut all_trades_str = String::from(trades[0].trade_id.to_string());
        for trade in &trades[1..] {
            all_trades_str = format!("{},{}", all_trades_str, trade.trade_id.to_string());
        }

        info!("SPARK: Pricing trades {}.", all_trades_str);
        // use the pv_spark service http://localhost:5010/pv_spark/189,190,...
        let result_pricing = reqwest::blocking::get(format!(
            "http://{}/pv_spark/{}",
            self.trade_pricer, all_trades_str
        ));

        let result = match result_pricing {
            Ok(result_price) =>
                match result_price.json::<HashMap<String, f64>>() {
                    Ok(result_pricer_inner) => result_pricer_inner,
                    Err(e) => {
                        warn!("Could not conver the result to a map: {:?}", e);
                        return TradeValue::new();
                    }
                },
            Err(e) => {
                warn!("Trades could not price correctly: {}", e);
                HashMap::<String, f64>::new() // TODO: THIS SHOULD BE DIFFERENT, CORRECT
            }
        };

        // we have the price, copy the market date in it.
        for (trade_obj, trade_res) in result.iter() {
            let _ = &new_portfolio.insert((trade_obj.clone(), self.market_date), *trade_res);
        }

        new_portfolio
    }

    /// processes the trades on the new market.
    /// new_market_receiver:
    pub fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_trade_receiver: Receiver<Trade>,
        new_portfolio_sender: Sender<PortfolioType>,
    ) {
        let mut __all_trades: Vec<Trade> = vec![];
        let mut new_mkt = match new_market_receiver.try_recv() {
            Ok(new_internal_mkt) => Some(new_internal_mkt),
            _ => None,
        };

        loop {
            // iterate over new_trade_receiver until exhaustion
            if new_mkt.is_some() {
                info!("NEW market: Working.");
                // new market is constructed.
                let mut new_portfolio = PortfolioType::new();
                if __all_trades.len() > 10 {
                    new_portfolio = self._price_trades_on_spark(&__all_trades);
                } else {
                    // compute the trades 1 by 1.
                    for trade in &__all_trades {
                        Controller::_trade_result_agg_single(
                            &mut new_portfolio,
                            Some(self._value_trade(trade)),
                        );
                    }
                }
                let mut new_trade_iter = new_trade_receiver.try_iter();
                let mut new_trade = new_trade_iter.next();
                while new_trade.is_some() {
                    let new_trade_v = new_trade.unwrap();
                    let trade_value = self._value_trade(&new_trade_v);
                    __all_trades.push(new_trade_v);
                    Controller::_trade_result_agg_single(&mut new_portfolio, Some(trade_value));
                    new_trade = new_trade_iter.next();
                }
                // we've exhausted the new trades -> switch markets.
                let _ = new_portfolio_sender.send(new_portfolio.clone());
            }
            // handle new market signals.
            // flush all markets on the new market until the last one.
            let mut new_market_result = new_market_receiver.try_iter();
            let mut prev_new_mkt: Option<MarketType> = None;
            new_mkt = new_market_result.next();
            while new_mkt.is_some() {
                prev_new_mkt = new_mkt;
                new_mkt = new_market_result.next();
            }
            new_mkt = prev_new_mkt;
            // we have the last market
            thread::sleep(Duration::from_millis(100));
        }
    }

    /// listens to kafka stream and stores portfolio locally.
    fn __construct_portfolio(
        &self,
        sender_new: Sender<Trade>,
        sender_curr: Sender<Trade>,
        pos_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name, self.kafka_port);

        let mut pos_listener_ = Consumer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_topic_partitions(pos_topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        loop {
            debug!("Listening to trades!");
            for ms in pos_listener_.poll().unwrap().iter() {
                for m in ms.messages() {
                    let msg_decoded: Value =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();

                    let trade_to_send = Controller::recover_trade(&msg_decoded);
                    let _ = sender_new.send(trade_to_send);
                    let _ = sender_curr.send(trade_to_send);
                }
                let _ = pos_listener_.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            pos_listener_.commit_consumed().unwrap();
        }
    }

    /// publishes the computed results to the result publisher topic in Kafka.
    /// curr_mkt_recv is a receiver that receives the produced market and publishes it to Kafka
    fn _publish_results(
        &self,
        curr_portfolio_recv: Receiver<PortfolioType>,
        results_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name, self.kafka_port);

        let mut res_publisher = Producer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        loop {
            info!("Publishing new market results.");
            let curr_mkt = curr_portfolio_recv.recv().unwrap();
            // serialize the current market into HashMap<String, f64>
            let mut curr_mkt_ser = HashMap::<String, f64>::new();
            for ((flight_nb, flight_date), flight_val) in curr_mkt.iter() {
                curr_mkt_ser.insert(
                    Controller::_encode_flight_date(flight_nb.clone(), *flight_date),
                    *flight_val,
                );
            }
            let curr_mkt_json = serde_json::ser::to_string(&curr_mkt_ser).unwrap();

            let curr_mkt_pv = format!("{{\"PV\": {}}}", curr_mkt_json);

            // implements bytearray(str(dumps(self.curr_market)), ascii))
            let market_record = Record::from_value(results_topic.as_str(), curr_mkt_pv.as_bytes())
                .with_partition(0);

            let _ = res_publisher.send(&market_record);
            thread::sleep(Duration::from_secs(1));
        }
    }

    /// Loop that handles the market events
    pub fn _handle_mkt_events(&self, mkt_topic: String, new_mkt_sender: Sender<MarketType>) {
        let mut mkt_listener_ = Consumer::from_hosts(vec![format!(
            "{}:{}",
            self.kafka_server_name, self.kafka_port
        )
        .to_owned()])
        .with_topic_partitions(mkt_topic.to_owned(), &[0])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

        let mkt_update_client = reqwest::blocking::Client::new();

        loop {
            debug!("Getting new markets from {mkt_topic}!");
            for ms in mkt_listener_.poll().unwrap().iter() {
                for m in ms.messages() {
                    // TODO: ADD THIS CHECK HERE!!
                    let msg_decoded: Value =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();
                    // msg_decoded is an array, the first value is the market number, the second the object
                    let _market_uuid = msg_decoded[0].to_string();
                    let market_obj = msg_decoded[1].as_object().unwrap();

                    // update the market rester market_api
                    let _ = mkt_update_client
                        .post("http://localhost:5010/market")
                        .json(&HashMap::from([("market", market_obj)]))
                        .send()
                        .unwrap();

                    // construct a new HashMap
                    let mut mkt_decoded = MarketType::new();
                    for (market_flight_date, flight_price) in market_obj.iter() {
                        let _ = &mkt_decoded.insert(
                            Controller::_decode_flight_date(market_flight_date.clone()), // TODO: THIS SHOULD BE A REFERENCE OR SOMETHING
                            flight_price.as_f64().unwrap(), // TODO: FIX THIS HERE
                        );
                    }

                    let _ = new_mkt_sender.send(mkt_decoded); // send the market over the sender.
                }
                let _ = mkt_listener_.consume_messageset(ms);
            }
            mkt_listener_.commit_consumed().unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    }

    // starts the controller threads.
    pub fn start(
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
    ) {
        // 2 trade senders, 1 for current market, 1 for new market.
        let (pos_sender_curr, pos_recv_curr) = channel::<Trade>();
        let (pos_sender_new, pos_recv_new) = channel::<Trade>();
        // events about the new market event
        let (new_mkt_sender, new_mkt_receiver) = channel::<MarketType>();
        // new & current market portfolio
        let (curr_portfolio_sender, curr_portfolio_recv) = channel::<PortfolioType>();
        let (new_portfolio_sender, new_portfolio_recv) = channel::<PortfolioType>();

        thread::scope(|s| {
            let _ = thread::Builder::new()
                .name("accepting_trades".to_string())
                .spawn_scoped(s, move || {
                    self.__construct_portfolio(pos_sender_new, pos_sender_curr, pos_topic);
                });

            let _ = thread::Builder::new()
                .name("market_events".to_string())
                .spawn_scoped(s, move || {
                    self._handle_mkt_events(mkt_topic, new_mkt_sender);
                });

            let _ = thread::Builder::new()
                .name("new_portfolio".to_string())
                .spawn_scoped(s, move || {
                    self._trade_processor_new(new_mkt_receiver, pos_recv_new, new_portfolio_sender);
                });

            let _ = thread::Builder::new()
                .name("curr_portfolio".to_string())
                .spawn_scoped(s, move || {
                    self._trade_processor_curr(
                        pos_recv_curr,
                        curr_portfolio_sender,
                        new_portfolio_recv,
                    );
                });

            let _ = thread::Builder::new()
                .name("publish_thread".to_string())
                .spawn_scoped(s, move || {
                    self._publish_results(curr_portfolio_recv, results_topic);
                })
                .unwrap();
        });
    }
}

impl TradeHandling for Controller {
    /// recovers the trade from the message itself.
    fn recover_trade(msg_decoded: &Value) -> Trade {
        let msg_payload = &msg_decoded["payload"];
        let event_type = &msg_payload["op"];

        debug!("Getting position: {:?}", msg_payload);

        match event_type.as_str() {
            Some("c") => {
                let tid = msg_payload["after"]["position_id"].as_i64();
                return Trade {
                    trade_id: tid.unwrap() as u8,
                    direction: TradeDirection::Create,
                };
            }
            Some("d") => {
                let tid = msg_payload["before"]["position_is"].as_i64();
                return Trade {
                    trade_id: tid.unwrap() as u8,
                    direction: TradeDirection::Delete,
                };
            }
            _ => {
                info!("UNIMPLEMENTED. FIX THIS");
                return Trade {
                    trade_id: 189,
                    direction: TradeDirection::Create,
                };
            }
        }
    }
}

impl EncoderDecoder for Controller {
    /// decodes the encoding string.
    fn _decode_flight_date(flight_date: String) -> (String, Date) {
        // flight_date is in the form UA96|20150101
        let mut flight_date_v = flight_date.split("|");

        // TODO: A LOT OF CHECKING HAS TO BE DONE HERE
        let flight_ = flight_date_v.next().unwrap();
        let date_format = format_description::parse("[year][month][day]").unwrap();
        let date_ = Date::parse(flight_date_v.next().unwrap(), &date_format).unwrap();

        (flight_.to_owned(), date_)
    }

    fn _encode_flight_date(flight: String, date: Date) -> String {
        // flight_date is in the form UA96|20150101

        let date_format = format_description::parse("[year][month][day]").unwrap();

        format!("{}|{}", flight, date.format(&date_format).unwrap())
    }
}
