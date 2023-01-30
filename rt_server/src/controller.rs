use log::{debug, info, warn};
use queues::*;

use core::cmp::Eq;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use reqwest;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, RecvError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use time::format_description;
use time::Date;

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
    _latest_market: Arc<Mutex<MarketType>>,
    results_topic: String,
    trade_pricer: String, // trade pricer name
    kafka_server_name: String,
    kafka_port: i32,
}

impl Controller {
    pub fn new(
        market_date: Date,
        results_topic: String,
        pricing_params_: Option<HashMap<String, f64>>,
        trade_pricer: String, // localhost:5051
        kafka_server_name: String,
        kafka_port: i32,
    ) -> Self {
        // if given the params, use them, otherwise construct empty map
        let pricing_init = match pricing_params_ {
            Some(pricing_hash) => pricing_hash,
            None => HashMap::new(),
        };

        Controller {
            market_date: market_date,
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
            kafka_server_name,
            kafka_port,
        }
    }

    /// constructs the controller from configuration read from the file.
    pub fn new_from_config(
        market_date: Date,
        result_topic: String,
        config_file: String, // like "configuration.yaml"
    ) -> Self {
        //let config_f = File::open(config_file)?;
        //let pricing_params = serde_yaml::from_reader::<'static, HashMap<&str, f64>>(config_f)?;

        // TODO: READ ALL THESE PARAMETERS FROM THE YAML CONFIG, NOT JUST SOME!
        Controller::new(
            market_date,
            result_topic,
            Some(HashMap::<String, f64>::new()),
            "localhost:5010".to_string(),
            "localhost".to_owned(),
            9092,
        )
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
            Ok(result_price) => result_price.json::<HashMap<String, f64>>().unwrap(),
            Err(e) => {
                warn!("Trade {trade_id} could not price correctly: {}", e);
                return TradeValue::new(); // TODO: THIS SHOULD BE DIFFERENT, CORRECT
            }
        };
        info!("Valuing trade {:?}", result);

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
            debug!("Working on current market!");
            let new_portfolio_received = new_portfolio_receiver.try_recv(); // this will not block
            match new_portfolio_received {
                Ok(new_portfolio) => {
                    curr_portfolio = new_portfolio;
                }
                _ => {}
            };

            match trade_receiver.try_recv() {
                Ok(trade) => {
                    debug!("CURR market: valuing trade {}", trade.trade_id);
                    let trade_value = self._value_trade(&trade);

                    Controller::_trade_result_agg_single(&mut curr_portfolio, Some(trade_value));
                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());
                }
                _ => {}
            };
        }
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
        let mut new_portfolio = PortfolioType::new();
        let mut new_mkt = match new_market_receiver.try_recv() {
            Ok(new_internal_mkt) => Some(new_internal_mkt),
            _ => None,
        };

        loop {
            // iterate over new_trade_receiver until exhaustion
            if new_mkt.is_some() {
                info!("Working on new market!");
                // new market is constructed.
                new_portfolio = PortfolioType::new();
                for trade in &__all_trades {
                    Controller::_trade_result_agg_single(
                        &mut new_portfolio,
                        Some(self._value_trade(trade)),
                    );
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
            new_mkt = new_market_result.next();
            while new_mkt.is_some() {
                new_mkt = new_market_result.next();
            }
            // we have the last market
        }
    }

    /// listens to kafka stream and stores portfolio locally.
    pub fn __construct_portfolio(
        &self,
        sender_new: Sender<Trade>,
        sender_curr: Sender<Trade>,
        pos_topic: String,
        new_mkt_working: Receiver<bool>,
    ) {
        // indicators whether new positions are coming in.
        let mut prev_working = false;
        let mut working = false;

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
                working = true;
                for m in ms.messages() {
                    let msg_decoded: Value =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();

                    let msg_payload = &msg_decoded["payload"];
                    let event_type = &msg_payload["op"];

                    debug!("Getting position: {:?}", msg_payload);

                    let trade_to_send = match event_type.as_str() {
                        Some("c") => {
                            let tid = msg_payload["after"]["position_id"].as_i64();
                            Trade {
                                trade_id: tid.unwrap() as u8,
                                direction: TradeDirection::Create,
                            }
                        }
                        Some("d") => {
                            let tid = msg_payload["before"]["position_is"].as_i64();
                            Trade {
                                trade_id: tid.unwrap() as u8,
                                direction: TradeDirection::Delete,
                            }
                        }
                        _ => {
                            info!("UNIMPLEMENTED. FIX THIS");
                            Trade {
                                trade_id: 189,
                                direction: TradeDirection::Create,
                            }
                        }
                    };
                    let _ = sender_new.send(trade_to_send);
                    let _ = sender_curr.send(trade_to_send);
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
    /// curr_mkt_recv is a receiver that receives the produced market and publishes it to Kafka
    fn _publish_results(&self, curr_mkt_recv: Receiver<PortfolioType>) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name, self.kafka_port);

        let mut res_publisher = Producer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        loop {
            debug!("Publishing new market results.");
            let curr_mkt = curr_mkt_recv.recv().unwrap();
            let curr_mkt_json = serde_json::ser::to_string(&curr_mkt).unwrap();

            // implements bytearray(str(dumps(self.curr_market)), ascii))
            let market_record =
                Record::from_value(self.results_topic.as_str(), curr_mkt_json.as_bytes());

            let _ = res_publisher.send(&market_record);
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

                    // construct a new HashMap
                    let mut mkt_decoded = MarketType::new();
                    for (market_flight_date, flight_price) in market_obj.iter() {
                        let _ = &mkt_decoded.insert(
                            Controller::_decode_flight_date(market_flight_date.clone()), // TODO: THIS SHOULD BE A REFERENCE OR SOMETHING
                            flight_price.as_f64().unwrap(), // TODO: FIX THIS HERE
                        );
                    }

                    debug!("NEW MKT SENDER = {:?}", mkt_decoded);
                    let _ = new_mkt_sender.send(mkt_decoded); // send the market over the sender.
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
        // 2 trade senders, 1 for current market, 1 for new market.
        let (pos_sender_curr, pos_recv_curr) = channel::<Trade>();
        let (pos_sender_new, pos_recv_new) = channel::<Trade>();
        //
        let (new_mkt_sender, new_mkt_receiver) = channel::<MarketType>();
        let (mkt_working_sender, mkt_working_recv) = channel::<bool>();
        let (curr_result_sender, curr_result_recv) = channel::<PortfolioType>();
        // new & current market portfolio
        let (curr_portfolio_sender, curr_portfolio_recv) = channel::<PortfolioType>();
        let (new_portfolio_sender, new_portfolio_recv) = channel::<PortfolioType>();

        thread::scope(|s| {
            s.spawn(move || {
                self.__construct_portfolio(
                    pos_sender_new,
                    pos_sender_curr,
                    "air_options.ao.option_positions".to_string(),
                    mkt_working_recv,
                );
            });
            s.spawn(move || {
                self._handle_mkt_events("mkt_events".to_string(), new_mkt_sender);
            });
            s.spawn(move || {
                self._trade_processor_new(new_mkt_receiver, pos_recv_new, new_portfolio_sender);
            });
            s.spawn(move || {
                self._trade_processor_curr(
                    pos_recv_curr,
                    curr_portfolio_sender,
                    new_portfolio_recv,
                );
            });

            s.spawn(move || {
                self._publish_results(curr_result_recv);
            });
        });
    }
}
