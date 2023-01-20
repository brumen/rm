use log::{debug, info, warn};
use queues::*;

use core::cmp::Eq;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, RequiredAcks};
use reqwest;
use serde_json::Value;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use time::{format_description, Date};

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
enum TradeDirection {
    Create,
    Delete,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
struct Trade {
    trade_id: u8,
    direction: TradeDirection,
}

type PricingParams = HashMap<String, f64>;
type MarketType = HashMap<(String, Date), f64>;
type TradeValue = HashMap<(String, Date), f64>;
type PortfolioType = HashMap<(String, Date), f64>;

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
    _trade_queue_curr_market: Queue<Trade>,
    _trade_queue_new_market: Queue<Trade>,
    curr_market: Box<PortfolioType>,
    new_market: Box<PortfolioType>,
    __new_market_prev_working: bool,
    __curr_market_prev_working: bool,
    __all_trades: Vec<Trade>,
    // snaps of the current and new market
    _market_snap_curr: Box<Option<MarketType>>,
    _market_snap_new: Box<Option<MarketType>>,
    // Kafka listeners
    _mkt_listener: Consumer,      // kafka consumer for the market events.
    _position_listener: Consumer, // position listener
    _results_publisher: Producer, // publishes results back to Kafka bus.
    _latest_market: Option<MarketType>, // latest market
    results_topic: String,
    trade_pricer: String, // trade pricer
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

        let bootstrap_servers = format!("{kafka_server_name}:{kafka_server_port}");

        let mkt_listener_ = Consumer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_topic_partitions(mkt_topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        let pos_listener_ = Consumer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_topic_partitions(pos_topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        let res_publisher = Producer::from_hosts(vec![bootstrap_servers.to_owned()])
            //.with_ack_time(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        Controller {
            market_date: market_date,
            local_only: true,
            pricing_params: pricing_init,
            _new_market_event: false,
            _new_trade_event: false,
            _trade_queue_curr_market: queue![],
            _trade_queue_new_market: queue![],

            // current and new result of market init, initially empty markets.
            curr_market: Box::new(HashMap::new()),
            new_market: Box::new(HashMap::new()),

            __new_market_prev_working: false,
            __curr_market_prev_working: false,
            __all_trades: vec![],

            // initial markets are None, they get updated from Kafka
            _market_snap_curr: Box::new(None),
            _market_snap_new: Box::new(None),

            // kafka listeners
            _mkt_listener: mkt_listener_,
            _position_listener: pos_listener_,
            _results_publisher: res_publisher,
            _latest_market: None, // initial latest market is None
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

    /// Adds the new market event to the market queue.
    fn new_mkt_event(&mut self) {
        debug!("New market event occured.");

        if self._trade_queue_new_market.size() == 0 {
            self.new_market = Box::new(HashMap::new());

            for new_position in Controller::__prune_offsetting_trades(&mut self.__all_trades) {
                let _ = self._trade_queue_new_market.add(new_position);
            }
        }
    }

    // Add positions to the queue: to curr_market only if the new market is idle,
    //    otherwise to both current and new market.
    //
    fn add_position(&mut self, new_positions: Vec<Trade>) {
        let positions_len = new_positions.len();

        debug!("Adding positions to CURR market: {positions_len}");

        for new_position in &new_positions {
            self.__all_trades.push(*new_position);
            let _ = self._trade_queue_curr_market.add(*new_position);
        }

        if self._trade_queue_new_market.size() != 0 {
            debug!("Adding {positions_len} positions to NEW market queue");
            for new_position in &new_positions {
                let _ = self._trade_queue_new_market.add(*new_position);
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
        if self._trade_queue_new_market.size() == 0 && self.__new_market_prev_working {
            debug!("Switching curr_market <- new_market");
            return true;
        }

        false
    }

    /// Aggregating the trades for two trades.
    fn _trade_result_agg_single(
        exist_market: &mut MarketType,
        trade_pv_2: Option<TradeValue>, // < &str, f64>
    ) {
        match trade_pv_2 {
            None => (),
            Some(trade_pv_expl) => {
                // aggregate 2 hashmaps
                for (trade_id, trade_value) in trade_pv_expl.iter() {
                    if exist_market.contains_key(trade_id) {
                        exist_market.insert(
                            trade_id.clone(),
                            exist_market[trade_id], // TODO: + *trade_value[*trade_id],
                        );
                    } else {
                        exist_market.insert(trade_id.clone(), *trade_value); // TODO: REMOVE THIS CLONGING
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
            Err(e) => HashMap::new(), // TODO: THIS SHOULD BE DIFFERENT, CORRECT
        };

        let mut result_tv = TradeValue::new();

        for (trade_obj, trade_res) in result.iter() {
            let _ = &result_tv.insert((trade_obj.clone(), self.market_date), *trade_res);
        }

        result_tv
    }

    /// extracts the trades from the queue.
    fn _extract_trades_from_q(&mut self, queue_name: String, nb_elts: usize) -> Vec<Trade> {
        let mut curr_elt = 0;

        let trade_q = match queue_name.as_str() {
            "curr" => &mut self._trade_queue_curr_market,
            _ => &mut self._trade_queue_new_market,
        };

        let mut trades_all = vec![];

        while (trade_q.size() != 0) && (curr_elt < nb_elts) {
            let trade = trade_q.remove().unwrap(); // TODO: UNWRAP SHOULD BE HANDLED.

            debug!("Valuing trade {trade:?}");
            trades_all.push(trade);
            curr_elt += 1;
        }

        trades_all
    }

    /// Values the portfolio of trades in trade_ids.
    fn _value_portfolio(&mut self, trade_queue: String, nb_elts: usize) -> PortfolioType {
        let mut portfolio_value = PortfolioType::new();

        let client = reqwest::blocking::Client::new(); // TODO: THIS SHOULD BE CHANGED LATER.

        // TODO: THIS NEEDS TO BE FIXED.
        //        let results_mkt = client
        //            .post("{self._trade_pricer}/market")
        //            .body(self._snap_market())
        //            .send()
        //            .unwrap(); // update market

        // format of the date to be sent to the api service
        let date_format = format_description::parse("[year][month][day]").unwrap();

        // TODO: FIX THIS HERE LATER.
        //let results_mkt_date = client
        //    .post("{self._trade_pricer}/market_date")
        //    .body(self.market_date.format(&date_format).unwrap())
        //    .send()
        //    .unwrap(); // update market date

        let trades_all = self._extract_trades_from_q(trade_queue, nb_elts);

        for trade in trades_all {
            let trade_value = self._value_trade(trade);

            Controller::_trade_result_agg_single(&mut portfolio_value, Some(trade_value));
        }

        portfolio_value
    }

    /// Function processing the current market queue.
    fn _trade_processor_curr(&mut self, sleep_delay: Duration) {
        loop {
            let queue_size = self._trade_queue_curr_market.size(); //trade_queue.size();
            println!("Running 3, {queue_size}");
            if queue_size != 0 {
                info!("CURRENT queue: working on {queue_size} trades.");
                self.__curr_market_prev_working = true;
                let trade_values = self._value_portfolio(String::from("curr"), queue_size);

                debug!("CURRENT queue: Finished evaluating trades, aggregating next!");
                Controller::_trade_result_agg_single(&mut self.curr_market, Some(trade_values));

                debug!("CURRENT queue: finished aggregating.");
            } else {
                debug!("CURRENT queue: nothing to do, sleeping {sleep_delay:?} secs");
                self.__curr_market_prev_working = false;
                thread::sleep(sleep_delay);
            }
        }
    }

    /// runs the processor of the new market thread.
    fn _trade_processor_new(&mut self, sleep_delay: Duration) {
        loop {
            let queue_size = self._trade_queue_new_market.size();
            println!("Running 2");
            if queue_size != 0 {
                info!("NEW market: Computing {queue_size} trades.");
                self.__new_market_prev_working = true;
                let trade_values = self._value_portfolio(String::from("new"), queue_size);

                debug!("NEW market: Finishing evaluating trades. Aggregating next.");
                Controller::_trade_result_agg_single(&mut self.new_market, Some(trade_values));
                debug!("NEW market: finished aggregating trades.");
            } else {
                if self._replace_curr_with_single() {
                    info!("NEW market: Switching: curr_market <- new_market.");

                    self.curr_market = self.new_market.clone();

                    // switch markets, new snaps of the market
                    self._market_snap_curr = self._market_snap_new.clone();
                    self._market_snap_new = Box::new(Some(self._snap_market())); // new snap

                    self.__new_market_prev_working = false;
                } else {
                    debug!("NEW market: nothing to do, waiting {:?}", sleep_delay);
                    self.__new_market_prev_working = false;
                    thread::sleep(sleep_delay);
                }
            }
        }
    }

    /// listens to kafka stream and stores portfolio locally.
    pub fn __construct_portfolio(&mut self) {
        loop {
            println!("Running construct");
            for ms in self._position_listener.poll().unwrap().iter() {
                for m in ms.messages() {
                    let msg_decoded: Value =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();

                    let msg_payload = &msg_decoded["payload"];
                    let event_type = &msg_payload["op"];

                    //println!("{:?}", msg_payload);
                    match event_type.as_str() {
                        Some("c") => {
                            let tid = msg_payload["after"]["position_id"].as_i64();
                            self.add_position(vec![Trade {
                                trade_id: tid.unwrap() as u8,
                                direction: TradeDirection::Create,
                            }]);
                        }
                        Some("d") => {
                            let tid = msg_payload["before"]["position_is"].as_i64();
                            self.add_position(vec![Trade {
                                trade_id: tid.unwrap() as u8,
                                direction: TradeDirection::Delete,
                            }]);
                        }
                        _ => {
                            info!("UNIMPLEMENTED. FIX THIS");
                        }
                    }
                }
                let _ = self._position_listener.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            self._position_listener.commit_consumed().unwrap();
        }
    }

    /// publishes the computed results to the result publisher topic in Kafka.
    //    fn _publish_results(&self, publish_delay: Duration) {
    //        loop {
    //            debug!("Publishing new market results.");
    //            let record = self.curr_market;

    //            // implements bytearray(str(dumps(self.curr_market)), ascii))
    //            let market_record = Record::from_value(
    //                self.results_topic,
    //                adsfasdf,
    //
    //            )
    //
    //            self._results_publisher.send(&market_record);
    //            thread::sleep(publish_delay);
    //        }
    //    }

    /// snap the market from the market service
    fn _snap_market(&self) -> MarketType {
        //    match self._latest_market {
        //        None => MarketType::<'a>::new(),
        //        Some(latest_m) => self._decode_mkt(latest_m),
        //    }
        MarketType::new()
    }

    /// handles the market events
    pub fn _handle_mkt_events(&mut self) {
        loop {
            for ms in self._mkt_listener.poll().unwrap().iter() {
                for m in ms.messages() {
                    // TODO: ADD THIS CHECK HERE!!
                    //if m.value {
                    //    continue
                    //}

                    // let msg_decoded: Value = serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();

                    self.new_mkt_event();
                    self._latest_market = Some(HashMap::new()); // m  //m;
                }
                let _ = self._position_listener.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            self._position_listener.commit_consumed().unwrap();
        }
    }

    // decodes the market from the rester service.
    //fn _decode_mkt(market: MarketType<'a>) -> u8 {}

    // starts the controller threads.
    pub fn start(self, controller_delay: Duration, report_delay: Duration) {
        let self_arc = Arc::new(Mutex::new(self));

        let self_arc_clone = Arc::clone(&self_arc);
        let position_thread = thread::spawn(move || {
            let mut self_m = self_arc_clone.lock().unwrap();
            &self_m.__construct_portfolio();
        });

        //        let self_arc_clone2 = Arc::clone(&self_arc);
        //        let market_events_thread = thread::spawn(move || {
        //            let mut self_m = self_arc_clone2.lock().unwrap();
        //            &self_m._handle_mkt_events();
        //        });

        let self_arc_clone3 = Arc::clone(&self_arc);
        let trade_processor_curr = thread::spawn(move || {
            let mut self_m = self_arc_clone3.lock().unwrap();
            &self_m._trade_processor_curr(controller_delay);
        });

        let self_arc_clone4 = Arc::clone(&self_arc);
        let trade_processor_new = thread::spawn(move || {
            let mut self_m = self_arc_clone4.lock().unwrap();
            &self_m._trade_processor_new(controller_delay);
        });

        let res_position = position_thread.join().unwrap();
        //        let res_market_events = market_events_thread.join();
        let res_trade_proc_curr = trade_processor_curr.join().unwrap();
        let res_trade_proc_new = trade_processor_new.join().unwrap();

        //let res_publish = publish_thread.join();
    }
}
