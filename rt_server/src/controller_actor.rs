use log::{debug, info, warn};
use queues::*;

use core::cmp::Eq;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, RequiredAcks};
use reqwest;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, RecvError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::{sleep, spawn, JoinHandle};
use std::time::Duration;
use time::format_description;
use time::Date;

use actix::prelude::*;
use actix::{Actor, Context, System};

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
enum TradeDirection {
    Create,
    Delete,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct Trade {
    trade_id: u8,
    direction: TradeDirection,
}

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
    local_only: bool,
    pricing_params: PricingParams,
    _new_market_event: bool,
    _new_trade_event: bool,
    _trade_queue_curr_market: Arc<Mutex<Queue<Trade>>>,
    _trade_queue_new_market: Arc<Mutex<Queue<Trade>>>,
    curr_market: Arc<Mutex<PortfolioType>>,
    new_market: Arc<Mutex<PortfolioType>>,
    __new_market_prev_working: Arc<Mutex<bool>>,
    __curr_market_prev_working: Arc<Mutex<bool>>,
    __all_trades: Arc<Mutex<Vec<Trade>>>,
    // snaps of the current and new market
    _market_snap_curr: Arc<Mutex<Option<MarketType>>>,
    _market_snap_new: Arc<Mutex<Option<MarketType>>>,
    _latest_market: Arc<Mutex<MarketType>>, // latest market
    results_topic: String,
    trade_pricer: String, // trade pricer name
}

impl Controller {
    pub fn new(
        market_date: Date,
        kafka_server_name: String,
        kafka_server_port: i32,
        mkt_topic: String,
        pos_topic: String,
        results_topic: String,
        pricing_params_: Option<HashMap<String, f64>>,
        trade_pricer: String, // localhost:5051
    ) -> Self {
        // if given the params, use them, otherwise construct empty map
        let pricing_init = match pricing_params_ {
            Some(pricing_hash) => pricing_hash,
            None => HashMap::new(),
        };

        Controller {
            market_date: market_date,
            local_only: true,
            pricing_params: pricing_init,
            _new_market_event: false,
            _new_trade_event: false,
            _trade_queue_curr_market: Arc::new(Mutex::new(queue![])),
            _trade_queue_new_market: Arc::new(Mutex::new(queue![])),

            // current and new result of market init, initially empty markets.
            curr_market: Arc::new(Mutex::new(HashMap::new())),
            new_market: Arc::new(Mutex::new(HashMap::new())),

            __new_market_prev_working: Arc::new(Mutex::new(false)),
            __curr_market_prev_working: Arc::new(Mutex::new(false)),
            __all_trades: Arc::new(Mutex::new(vec![])),

            // initial markets are None, they get updated from Kafka
            _market_snap_curr: Arc::new(Mutex::new(None)),
            _market_snap_new: Arc::new(Mutex::new(None)),

            _latest_market: Arc::new(Mutex::new(MarketType::new())), // initial latest market is empty market
            results_topic: results_topic,
            trade_pricer: trade_pricer,
        }
    }

    /// constructs the controller from configuration read from the file.
    pub fn new_from_config(
        market_date: Date,
        kafka_server_name: String,
        kafka_server_port: i32,
        mkt_topic: String,
        pos_topic: String,
        result_topic: String,
        config_file: String, // like "configuration.yaml"
    ) -> Self {
        //let config_f = File::open(config_file)?;
        //let pricing_params = serde_yaml::from_reader::<'static, HashMap<&str, f64>>(config_f)?;

        // TODO: READ ALL THESE PARAMETERS FROM THE YAML CONFIG, NOT JUST SOME!
        Controller::new(
            market_date,
            kafka_server_name,
            kafka_server_port,
            mkt_topic,
            pos_topic,
            result_topic,
            Some(HashMap::<String, f64>::new()),
            "localhost:5010".to_string(),
        )
    }

    /// Sends new positions to the current and new market processors.
    ///
    fn add_position(
        &self,
        new_positions: Vec<Trade>,
        sender_new: &Sender<Trade>,
        sender_curr: &Sender<Trade>,
    ) {
        let positions_len = new_positions.len();

        debug!("Adding positions to CURR market: {positions_len}");

        for new_position in &new_positions {
            self.__all_trades.lock().unwrap().push(*new_position);
            sender_curr.send(*new_position);
        }

        let new_mkt_working = true; // TODO: FIX THIS PART
        if new_mkt_working {
            debug!("Adding {positions_len} positions to NEW market queue");
            for new_position in &new_positions {
                sender_new.send(*new_position);
            }
        }
    }

    // TODO: CHECK IF THIS IS REALLY NECESSARY TO DO, MAYBE THE KAFKA CONFIG HERE IS BETTER!!!
    /// Prunes offsetting trades.
    fn __prune_offsetting_trades(trades: &mut Vec<Trade>) -> Vec<Trade> {
        let mut prunned_positions: Vec<Trade> = vec![];

        for trade in trades {
            let trade_id = trade.trade_id;
            let trade_direction = trade.direction;

            match trade_direction {
                TradeDirection::Create => prunned_positions.push(*trade),

                TradeDirection::Delete => {
                    let equiv_create_pos = Trade {
                        trade_id: trade_id,
                        direction: TradeDirection::Create,
                    };

                    if prunned_positions.contains(&equiv_create_pos) {
                        let equiv_pos_idx = prunned_positions
                            .iter()
                            .position(|&r| r == equiv_create_pos)
                            .unwrap();
                        prunned_positions.remove(equiv_pos_idx);
                    }
                }
                _ => {
                    prunned_positions.push(*trade);
                }
            };
        }

        prunned_positions
    }

    /// indicator whether the current market should be replaced w/ the new market
    fn _replace_curr_with_single(&self) -> bool {
        if self._trade_queue_new_market.lock().unwrap().size() == 0
            && *self.__new_market_prev_working.lock().unwrap()
        {
            debug!("Switching curr_market <- new_market");
            return true;
        }

        false
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
    fn _value_trade(&self, trade: Trade) -> TradeValue {
        let trade_id = trade.trade_id;

        //"http://localhost:5010/pv/{trade_id}"
        let result_pricing =
            reqwest::blocking::get(format!("http://{}/pv/{}", self.trade_pricer, trade_id));

        let result = match result_pricing {
            Ok(result_price) => result_price.json::<HashMap<String, f64>>().unwrap(),
            Err(e) => {
                warn!("Trade {trade_id} could not price correctly!");
                return TradeValue::new(); // TODO: THIS SHOULD BE DIFFERENT, CORRECT
            }
        };

        // we have the price, copy the market date in it.
        let mut result_tv = TradeValue::new();

        for (trade_obj, trade_res) in result.iter() {
            let _ = &result_tv.insert((trade_obj.clone(), self.market_date), *trade_res);
        }

        result_tv
    }

    /// Function processing the current market queue.
    pub fn _trade_processor_curr(&self, receiver: Receiver<Trade>) {
        for message in receiver.iter() {
            debug!("CURR market: valuing trade.");
            let trade_value = self._value_trade(message);

            Controller::_trade_result_agg_single(
                &mut self.curr_market.lock().unwrap(),
                Some(trade_value),
            );
        }
    }

    /// processes the trades on the new market.
    /// new_market_receiver:
    pub fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_market_working: Sender<bool>,
    ) {
        let new_market_event = false;

        loop {
            let new_market_result = new_market_receiver.recv();

            match new_market_result {
                Ok(new_market) => {
                    new_market_working.send(true);
                    // we have the market,
                    for trade in &*self.__all_trades.lock().unwrap() {
                        debug!("NEW market: valuing trade.");
                        let trade_value = self._value_trade(*trade);

                        Controller::_trade_result_agg_single(
                            &mut *self.new_market.lock().unwrap(),
                            Some(trade_value),
                        );
                    }

                    // Switching markets after the new market finishes.
                    info!("NEW market: Switching: curr_market <- new_market.");
                    *self.curr_market.lock().unwrap() = self.new_market.lock().unwrap().clone();

                    *self._market_snap_curr.lock().unwrap() =
                        self._market_snap_new.lock().unwrap().clone();
                    *self._market_snap_new.lock().unwrap() = Some(new_market);

                    *self.__new_market_prev_working.lock().unwrap() = false;
                }
                Err(recv_error) => {
                    warn!("Market error received {:>}", recv_error); // dont do anything, for now just report it.
                }
            }

            // flush the messages from new_market_receiver
            new_market_receiver.iter(); //
        }
    }

    /// listens to kafka stream and stores portfolio locally.
    pub fn __construct_portfolio(
        &self,
        sender_new: Sender<Trade>,
        sender_curr: Sender<Trade>,
        kafka_server_name: String,
        kafka_server_port: i32,
        pos_topic: String,
    ) {
        // indicators whether new positions are coming in.
        let mut prev_working = false;
        let mut working = false;

        let bootstrap_servers = format!("{kafka_server_name}:{kafka_server_port}");

        let mut pos_listener_ = Consumer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_topic_partitions(pos_topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        loop {
            for ms in pos_listener_.poll().unwrap().iter() {
                working = true;
                for m in ms.messages() {
                    let msg_decoded: Value =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();

                    let msg_payload = &msg_decoded["payload"];
                    let event_type = &msg_payload["op"];

                    debug!("Getting position: {:?}", msg_payload);

                    match event_type.as_str() {
                        Some("c") => {
                            let tid = msg_payload["after"]["position_id"].as_i64();
                            self.add_position(
                                vec![Trade {
                                    trade_id: tid.unwrap() as u8,
                                    direction: TradeDirection::Create,
                                }],
                                &sender_new,
                                &sender_curr,
                            );
                        }
                        Some("d") => {
                            let tid = msg_payload["before"]["position_is"].as_i64();
                            self.add_position(
                                vec![Trade {
                                    trade_id: tid.unwrap() as u8,
                                    direction: TradeDirection::Delete,
                                }],
                                &sender_new,
                                &sender_curr,
                            );
                        }
                        _ => {
                            info!("UNIMPLEMENTED. FIX THIS");
                        }
                    }
                }
                let _ = pos_listener_.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            pos_listener_.commit_consumed().unwrap();
            // change the working indicators.
            prev_working = working;
            working = false;
            // TODO: REPORT VALUE prev_working & working
            let working_indicator = prev_working && working;
            // switch if prev_working = false
        }
    }

    /// publishes the computed results to the result publisher topic in Kafka.
    fn _publish_results(
        &self,
        results: Receiver<()>, // TODO: FIX THE PUBLISHING TYPE HERE
        kafka_server_name: String,
        kafka_server_port: i32,
        publish_delay: Duration,
    ) {
        let bootstrap_servers = format!("{kafka_server_name}:{kafka_server_port}");

        let res_publisher = Producer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        loop {
            debug!("Publishing new market results.");
            let record = &self.curr_market;

            // implements bytearray(str(dumps(self.curr_market)), ascii))
            //let market_record = Record::from_value(
            //    self.results_topic,
            //    adsfasdf,
            // )

            //res_publisher.send(&market_record);
            thread::sleep(publish_delay);
        }
    }

    /// snap the market from the market service receiver
    fn _snap_market(&self, mkt_receiver: Receiver<MarketType>) -> MarketType {
        //    match self._latest_market {
        //        None => MarketType::<'a>::new(),
        //        Some(latest_m) => self._decode_mkt(latest_m),
        //    }
        MarketType::new()
    }

    /// Loop that handles the market events
    pub fn _handle_mkt_events(
        &self,
        kafka_server_name: String,
        kafka_server_port: i32,
        mkt_topic: String,
        new_mkt_sender: Sender<MarketType>,
    ) {
        let mut mkt_listener_ =
            Consumer::from_hosts(vec![
                format!("{kafka_server_name}:{kafka_server_port}").to_owned()
            ])
            .with_topic_partitions(mkt_topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        loop {
            for ms in mkt_listener_.poll().unwrap().iter() {
                for m in ms.messages() {
                    // TODO: ADD THIS CHECK HERE!!
                    let msg_decoded: Value =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();
                    // msg_decoded is an array, the first value is the market number, the second the object
                    let market_uuid = msg_decoded[0].to_string();
                    let market_obj = msg_decoded[1].as_object().unwrap();

                    // construct a new HashMap
                    let mut mkt_decoded = MarketType::new();
                    for (market_flight_date, flight_price) in market_obj.iter() {
                        let _ = &mkt_decoded.insert(
                            Controller::_decode_flight_date(market_flight_date.clone()), // TODO: THIS SHOULD BE A REFERENCE OR SOMETHING
                            flight_price.as_f64().unwrap(), // TODO: FIX THIS HERE
                        );
                    }

                    *self._latest_market.lock().unwrap() = mkt_decoded.clone();

                    new_mkt_sender.send(mkt_decoded); // send the market over the sender.
                }
                let _ = mkt_listener_.consume_messageset(ms);
            }
            mkt_listener_.commit_consumed().unwrap();
        }
    }

    /// decodes the
    fn _decode_flight_date(flight_date: String) -> (String, Date) {
        // flight_date is in the form UA96|2015-01-01
        let mut flight_date_v = flight_date.split("|");

        // TODO: A LOT OF CHECKING HAS TO BE DONE HERE
        let flight_ = flight_date_v.next().unwrap();
        let date_format = format_description::parse("[year][month][day]").unwrap();
        let date_ = Date::parse(flight_date_v.next().unwrap(), &date_format).unwrap();

        (flight_.to_owned(), date_)
    }

    // decodes the market from the rester service.
    //fn _decode_mkt(market: MarketType<'a>) -> u8 {}

    // starts the controller threads.
    pub fn start(&self) {
        let (pos_sender_curr, pos_recv_curr) = channel::<Trade>();
        let (pos_sender_new, pos_recv_new) = channel::<Trade>();
        let (new_mkt_sender, new_mkt_receiver) = channel::<MarketType>();
        let (mkt_working_sender, mkt_working_recv) = channel::<bool>();

        thread::scope(|s| {
            s.spawn(move || {
                self.__construct_portfolio(
                    pos_sender_new,
                    pos_sender_curr,
                    "localhost".to_string(),
                    9092,
                    "air_options.ao.option_positions".to_string(),
                );
            });
            s.spawn(move || {
                self._handle_mkt_events(
                    "localhost".to_string(),
                    9092,
                    "mkt_events".to_string(),
                    new_mkt_sender,
                );
            });
            s.spawn(move || {
                self._trade_processor_new(new_mkt_receiver, mkt_working_sender);
            });
            s.spawn(move || {
                self._trade_processor_curr(pos_recv_curr);
            });

            // TODO: results publishing
            //s.spawn(move || {
            //    self._publish_results();
            //});
        });
    }
}

// #[derive(Message)]
// #[rtype(result="Trade")]
// struct TradeMessage;

// struct ConstructPortfolio;

// impl Actor for ConstructPortfolio {

// }

// impl Handler<TradeMessage> for ConstructPortfolio {
//     type Result = Trade;

//     fn handle(&mut self, msg: TradeMessage, ctx: &mut Context<Self>) -> Self::Result {

//     //sender_new: Sender<Trade>,
//     //sender_curr: Sender<Trade>,
//     //kafka_server_name: String,
//     //kafka_server_port: i32,
//     //pos_topic: String,
// //) {
//     // indicators whether new positions are coming in.

//     let mut prev_working = false;
//     let mut working = false;

//     let bootstrap_servers = format!("{kafka_server_name}:{kafka_server_port}");

//     let mut pos_listener_ = Consumer::from_hosts(vec![bootstrap_servers.to_owned()])
//         .with_topic_partitions(pos_topic.to_owned(), &[0])
//         .with_fallback_offset(FetchOffset::Earliest)
//         .with_offset_storage(GroupOffsetStorage::Kafka)
//         .create()
//         .unwrap();

//     loop {
//         for ms in pos_listener_.poll().unwrap().iter() {
//             working = true;
//             for m in ms.messages() {
//                 let msg_decoded: Value =
//                     serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();

//                 let msg_payload = &msg_decoded["payload"];
//                 let event_type = &msg_payload["op"];

//                 debug!("Getting position: {:?}", msg_payload);

//                 match event_type.as_str() {
//                     Some("c") => {
//                         let tid = msg_payload["after"]["position_id"].as_i64();
//                         self.add_position(
//                             vec![Trade {
//                                 trade_id: tid.unwrap() as u8,
//                                 direction: TradeDirection::Create,
//                             }],
//                             &sender_new,
//                             &sender_curr,
//                         );
//                     }
//                     Some("d") => {
//                         let tid = msg_payload["before"]["position_is"].as_i64();
//                         self.add_position(
//                             vec![Trade {
//                                 trade_id: tid.unwrap() as u8,
//                                 direction: TradeDirection::Delete,
//                             }],
//                             &sender_new,
//                             &sender_curr,
//                         );
//                     }
//                     _ => {
//                         info!("UNIMPLEMENTED. FIX THIS");
//                     }
//                 }
//             }
//             let _ = pos_listener_.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
//         }
//         pos_listener_.commit_consumed().unwrap();
//         // change the working indicators.
//         prev_working = working;
//         working = false;
//         // TODO: REPORT VALUE prev_working & working
//         let working_indicator = prev_working && working;
//         // switch if prev_working = false
//     }
// }
