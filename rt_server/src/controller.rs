use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use reqwest;
use reqwest::blocking::Client;
use serde_json::Value;
use serde_yaml;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender, sync_channel, SyncSender};
use std::thread;
use std::time::Duration;
use string_join::Join;
use time::{format_description, Date};
use time::error::Format;

use crate::encdec::{EncoderDecoder, DecoderError};
use crate::trade::{Trade, TradeDirection, TradeHandling};

pub type PricingParams = HashMap<String, f64>;
pub type MarketType = HashMap<(String, Date), f64>;
pub type TradeValue = HashMap<(String, Date), f64>;
pub type PortfolioType = HashMap<(String, Date), f64>;

/// Controller structure.
/// market_date: date when we are pricing.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
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

#[derive(Debug, Serialize, Deserialize)]
pub enum CurrNewMarket {
    Current,
    New,
}

impl Controller {
    pub fn new(
        market_date: Date,
        pricing_params_: Option<PricingParams>,
        kafka_server_name: String,
        kafka_port: i32,
        trade_pricer: String,
    ) -> Self {
        // if given the params, use them, otherwise construct empty map
        let pricing_init = match pricing_params_ {
            Some(pricing_hash) => pricing_hash,
            None => PricingParams::new(),
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
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(
        market_date: Date,
        config_file: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file).unwrap();
        let config_map: RTConfig = serde_yaml::from_reader(config_f).unwrap();

        Ok(Controller::new(
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
        ))
    }

    /// Aggregating the results of an exiting market (exist_market) and a new trade (trade_pv_result)
    /// depending on the trade direction.
    fn _trade_result_agg_single(
        exist_market: &mut MarketType,
        trade_pv_result: Option<TradeValue>,
        trade_direction: TradeDirection,
    ) {
        match trade_pv_result {
            None => (),
            Some(trade_pv) => {
                // aggregate 2 hashmaps, one for exiting market, one from the trade_pv_2
                for (trade_id, trade_value) in trade_pv.iter() {
                    if exist_market.contains_key(trade_id) {

                        match trade_direction {
                            TradeDirection::Create => {
                                exist_market
                                    .insert(trade_id.clone(), exist_market[trade_id] + *trade_value);
                            },
                            TradeDirection::Delete => {
                                exist_market.remove(trade_id);
                            },
                            TradeDirection::Update => {
                                exist_market
                                    .insert(trade_id.clone(), *trade_value);
                            },
                        }
                    } else {

                        match trade_direction {
                            TradeDirection::Create => {
                                exist_market.insert(trade_id.clone(), *trade_value);
                            },
                            TradeDirection::Update => {
                                exist_market.insert(trade_id.clone(), *trade_value);
                            },
                            _ => {},
                        }
                    }
                }
            }
        }
    }

    /// Values the trade id
    /// Makes a call to the rester service, which values the trade.
    fn _value_trade(&self, trade: &Trade, market : CurrNewMarket) -> TradeValue {
        let trade_id = trade.trade_id;

        debug!("VALUATION: Pricing trade: {}, direction: {:?}, market: {:?}", trade.trade_id, trade.direction, market);

        if trade.direction == TradeDirection::Delete {
            return HashMap::from([
                ((trade_id.to_string(), self.market_date), 0. as f64),  // value unimportant, as it removes the trade
            ]);
        }

        // Create or update trades have to be evaluated, so we have to price them.
        //"http://localhost:5010/pv/{trade_id}"
        let result_pricing =
            reqwest::blocking::get(
                format!(
                    "http://{}/{}/{}",
                    self.trade_pricer,
                    match market {
                        CurrNewMarket::Current => "pv",
                        CurrNewMarket::New => "pv_new",
                    },
                    trade_id,
                )
            );

        let result = match result_pricing {
            Ok(result_price) => {
                match result_price.json::<HashMap<String, f64>>() {
                    Ok(result_pricer_inner) => result_pricer_inner,
                    Err(e) => {
                        warn!("_value_trade: Could not convert the result to a map: {:?}", e);
                        return TradeValue::new();
                    }
                }
            },
            Err(e) => {
                warn!("Trade {trade_id} could not price correctly: {}", e);
                return TradeValue::new(); // TODO: THIS SHOULD BE DIFFERENT, CORRECT
            },
        };

        // we have the price, copy the market date in it.
        let mut result_tv = TradeValue::new();

        for (trade_obj, trade_res) in result.iter() {
            let _ = &result_tv.insert((trade_obj.clone(), self.market_date), *trade_res);
        }

        info!("VALUATION: id: {}, market: {:?}, value: {:?}", trade_id, market, result_tv);
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
        let mut new_potential_portfolio : Option<PortfolioType>;
        let mut processing_trades; // = false;
        let mut all_trades : Vec<Trade> = vec![];

        loop {

            // receive new trade to price on current market
            processing_trades = false;
            while let Ok(trade) = trade_receiver.try_recv() {
                info!("CURR market: Processing trade {}, dir {:?}", trade.trade_id, trade.direction);
                Controller::_trade_result_agg_single(
                    &mut curr_portfolio,
                    Some(self._value_trade(&trade, CurrNewMarket::Current)),
                    trade.direction
                );
                self._add_trade_to_list(trade, &mut all_trades);
                info!("CURR market: sending to publisher trade: {}, direction: {:?}", trade.trade_id, trade.direction);
                let _ = curr_portfolio_sender.send(curr_portfolio.clone());
                processing_trades = true;
            }

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                new_potential_portfolio = Some(new_portfolio);
            }

            if !processing_trades && new_potential_portfolio.is_some() {
                // processor not working, switch markets
                info!("CURR processor: Switching markets: current <- new .");
                let _ = reqwest::blocking::get(format!(
                    "http://{}/switch_markets",
                    self.trade_pricer
                ));

                // report a portfolio
                // TODO: CHECK THIS
                let new_p = new_potential_portfolio.unwrap();
                if new_p.keys().len() == all_trades.len() {
                    let _ = curr_portfolio_sender.send(new_p.clone());
                    curr_portfolio = new_p;
                }
            }
//                _ => {  // we dont have a new portfolio
//                    if !processing_trades && prev_potential_portfolio.is_some() {
//                        curr_portfolio = prev_potential_portfolio.clone().unwrap();
//                    }
//                },
//            }
        }
    }

    /// prices trades on spark
    ///   takes as arguments the list of trades, and pricing client, used for post request
    fn _price_trades_on_spark(&self, trades: &Vec<Trade>, pricing_client : &Client) -> PortfolioType {
        if trades.is_empty() {
            return PortfolioType::new();
        }

        // joins all trades with commas, like 190,191,192
        let all_trade_ids = ",".join(
             trades
                .into_iter()
                .map(|trade: &Trade| -> String {trade.trade_id.to_string()} )
        );

        let result_pricing = pricing_client
            .post(format!("http://{}/pv_spark_new", self.trade_pricer))
            .form(&HashMap::from([("trades", &all_trade_ids)]))
            .send();

        // unwrap the result_pricing

        match result_pricing {
            Ok(result_price) =>
                match result_price.json::<HashMap<String, f64>>() {
                    Ok(result_pricer_inner) => {
                        let mut new_portfolio = PortfolioType::new();
                        for (trade_obj, trade_res) in result_pricer_inner.iter() {
                            let _ = &new_portfolio.insert((trade_obj.clone(), self.market_date), *trade_res);
                        }
                        info!("SPARK: Computed portfolio w/ {} trades", new_portfolio.keys().len());
                        return new_portfolio;
                    },
                    Err(e) => {
                        warn!("_price_trades_on_spark: Could not convert the result to a map: {:?}", e);
                        return TradeValue::new();
                    }
                },
            Err(e) => {
                warn!("Trades could not price correctly: {}", e);
                return TradeValue::new() // TODO: What to do if the trade cant convert
            }
        };

    }

    fn _price_trades_sequentially(&self, trades: &Vec<Trade>) -> PortfolioType {
        if trades.is_empty() {
            return PortfolioType::new();
        }

        let mut new_portfolio = PortfolioType::new();

        for trade in trades {
            let trade_value = self._value_trade(&trade, CurrNewMarket::New);
            Controller::_trade_result_agg_single(&mut new_portfolio, Some(trade_value), trade.direction);
        }

        new_portfolio
    }

    /// indicator if there is a new market present.
    /// consumes the new market events to come to the last one.
    fn _new_market_event(
        &self,
        new_market_receiver: &Receiver<MarketType>,
    ) -> bool {

        // handling new market event - roll to the latest new market, ignore in between markets
        let mut new_market_event = false;
        while new_market_receiver.try_recv().is_ok() {
            new_market_event = true;
        }

        new_market_event
    }

    /// add a trade if one exists on the new_trade_receiver, otherwise dont.
    /// return true if trade is received, otherwise false
    fn _add_new_trade_new_mkt(
        &self,
        all_trades : &mut Vec<Trade>,
        new_trade : Trade,
        new_portfolio: &mut PortfolioType,
    ) {

        info!("NEW market: Processing trade {}.", new_trade.trade_id);
        let trade_value = self._value_trade(&new_trade, CurrNewMarket::New);
        Controller::_trade_result_agg_single(new_portfolio, Some(trade_value), new_trade.direction);

        // update trades depending on the direction.
        self._add_trade_to_list(new_trade, all_trades);
        info!("Locally stored: {} trades", all_trades.len());
    }

    fn _add_trade_to_list(
        &self,
        new_trade : Trade,
        all_trades : &mut Vec<Trade>,
    ) {

        match new_trade.direction {
            TradeDirection::Create => {
                info!("NEW market: Adding trade {}", new_trade.trade_id);
                all_trades.push(new_trade);
            },
            TradeDirection::Delete => {
                info!("NEW market: Deleting trade {}", new_trade.trade_id);
                all_trades.retain(|&trade| trade.trade_id != new_trade.trade_id);
            },
            _ => {},  // nothing on update.
        }
    }

    /// processes the trades on the new market.
    /// new_market_receiver:
    pub fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,  //
        new_trade_receiver: Receiver<Trade>,  // receiving new additional trades
        new_portfolio_sender: Sender<PortfolioType>,  // results are sent here
    ) {
        let mut all_trades: Vec<Trade> = vec![];
        let mut new_portfolio = PortfolioType::new();
        let pricing_client = Client::new();

        loop {

            let new_market_event = self._new_market_event(&new_market_receiver);
            if new_market_event {
                info!("NEW market: Working. {} trades", all_trades.len());
                new_portfolio = self._price_trades_on_spark(&all_trades, &pricing_client);
            }

            // handling new trade event
            while let Ok(new_trade) = new_trade_receiver.try_recv() {
                if new_market_event {
                    self._add_new_trade_new_mkt(&mut all_trades, new_trade, &mut new_portfolio);
                } else {
                    self._add_trade_to_list(new_trade, &mut all_trades);
                }
            }

            // decisions whether to publish the market or not.
            if new_market_event {
                info!("NEW market: Publishing portfolio. {} trades", new_portfolio.keys().len());
                let _ = new_portfolio_sender.send(new_portfolio.clone());
            }
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
            for ms in pos_listener_.poll().unwrap().iter() {
                for m in ms.messages() {
                    if let Ok(m_value_str) = std::str::from_utf8(m.value) {
                        if let Ok(msg_decoded) = serde_json::from_str::<Value>(m_value_str) {
                            let trade_to_send = Controller::recover_trade(&msg_decoded);
                            // if the trade is None, there is possibly something wrong in
                            if trade_to_send.is_some() {
                                let actual_trade = trade_to_send.unwrap();
                                let _ = sender_new.send(actual_trade);
                                let _ = sender_curr.send(actual_trade);
                            }
                        } else {
                            warn!("Couldnt deal with message {:?}", m_value_str);
                        }
                    } else { // m_value_str is not ok, couldnt transform
                        warn!("Could not deal with message!!! Investigate!")
                    }
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
            let curr_mkt = curr_portfolio_recv.recv().unwrap();
            // serialize the current market into HashMap<String, f64>
            let mut curr_mkt_ser = HashMap::<String, f64>::new();
            for ((flight_nb, flight_date), flight_val) in curr_mkt.iter() {
                let encoded_flight_date = Controller::_encode_flight_date(flight_nb.clone(), *flight_date);
                match encoded_flight_date {
                    Ok(flight_date_enc) => {
                        curr_mkt_ser.insert(flight_date_enc,*flight_val);
                    },
                    Err(e) => {
                        warn!("Could not encode the flight nb and date {:?}", e);
                    }
                }
            }
            let curr_mkt_json = serde_json::ser::to_string(&curr_mkt_ser).unwrap();

            let curr_mkt_pv = format!("{{\"PV\": {}}}", curr_mkt_json);

            // implements bytearray(str(dumps(self.curr_market)), ascii))
            let market_record = Record::from_value(results_topic.as_str(), curr_mkt_pv.as_bytes())
                .with_partition(0);

            info!("PUBLISHING: Publishing new portfolio w/ {} trades.", curr_mkt.keys().len());
            let _ = res_publisher.send(&market_record);
            //thread::sleep(Duration::from_millis(1));
        }
    }

    /// Loop that handles the market events
    /// mkt_topic - receiving market events from this topic
    /// new_mkt_sender - sending the new market to the pricing api
    /// switch_mkt_recv - receiver receiving the event when to switch markets.
    pub fn _handle_mkt_events(
        &self,
        mkt_topic: String,
        new_mkt_sender: Sender<MarketType>,
    ) {
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
            debug!("Getting new markets from {mkt_topic}.");
            for ms in mkt_listener_.poll().unwrap().iter() {  // TODO: What to do w/ unwrap here??
                for m in ms.messages() {

                    let msg_decoded : Value =
                        if let Ok(m_utf) = std::str::from_utf8(m.value) {
                            if let Ok(m_json) = serde_json::from_str(m_utf) {
                                m_json
                            } else {
                                warn!("Could not decode to JSON. Continuing w/ next market: {:?}", m_utf);
                                continue;
                            }
                        } else {
                            warn!("Could not decode the market message into UTF8, continuing w/o");
                            continue;
                        };

                    // msg_decoded is an array, the first value is the market number, the second the object
                    let _market_uuid = msg_decoded[0].to_string();
                    let market_obj = msg_decoded[1].as_object();

                    // update the market rester market_api
                    let market_posted = mkt_update_client
                        .post("http://localhost:5010/future_market")
                        .json(&HashMap::from([("market", &market_obj)]))
                        .send();

                    match market_posted {
                        Ok(_) => {
                            debug!("Market posted successfully.");
                        },
                        _ => {
                            warn!("Could not post the market successfully. Ignoring last market.");
                        }
                    }

                    // construct a new HashMap
                    let mut mkt_decoded = MarketType::new();
                    for (market_flight_date, flight_price) in market_obj.unwrap().iter() {
                        let decoded_mkt_date = match Controller::_decode_flight_date(market_flight_date.clone()) {
                            Ok(decoded_mkt_and_date) => decoded_mkt_and_date,
                            _ => {
                                warn!("Couldnt decode {:?}", market_flight_date);
                                continue;
                            },
                        };
                        let decoded_price = match flight_price.as_f64() {
                            Some(fp) => fp,
                            None => {
                                warn!("Couldnt convert flight price {:?} to a float.", flight_price);
                                continue;
                            },
                        };

                        let _ = &mkt_decoded.insert(decoded_mkt_date, decoded_price);
                    }

                    let _ = new_mkt_sender.send(mkt_decoded); // send the market to new_market event
                }
                let _ = mkt_listener_.consume_messageset(ms);
            }
            mkt_listener_.commit_consumed().unwrap();
            //thread::sleep(Duration::from_millis(100));
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
        let (new_portfolio_sender, new_portfolio_recv) = channel::<PortfolioType>();  //sync_channel (1)

        // threads fail if any of them can not be created.
        thread::scope(|s| {
            let _ = thread::Builder::new()
                .name("accepting_trades".to_string())
                .spawn_scoped(s, move || {
                    self.__construct_portfolio(pos_sender_new, pos_sender_curr, pos_topic);
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("market_events".to_string())
                .spawn_scoped(s, move || {
                    self._handle_mkt_events(mkt_topic, new_mkt_sender);
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("new_portfolio".to_string())
                .spawn_scoped(s, move || {
                    self._trade_processor_new(
                        new_mkt_receiver,
                        pos_recv_new,
                        new_portfolio_sender,
                    );
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("curr_portfolio".to_string())
                .spawn_scoped(s, move || {
                    self._trade_processor_curr(
                        pos_recv_curr,
                        curr_portfolio_sender,
                        new_portfolio_recv,
                    );
                })
                .unwrap();

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
    fn recover_trade(msg_decoded: &Value) -> Option<Trade> {

        if msg_decoded.is_null() {  // nothing to do, return None
            return None;
        }

        // trade is not None, continue w/ this.
        let msg_payload = &msg_decoded["payload"];
        let event_type = &msg_payload["op"];

        debug!("Getting position: {:?}", msg_payload);

        match event_type.as_str() {
            Some("c") => {
                let tid = msg_payload["after"]["position_id"].as_i64();
                return Some(Trade {
                    trade_id: tid.unwrap() as u16,
                    direction: TradeDirection::Create,
                });
            }
            Some("d") => {
                let tid = msg_payload["before"]["position_id"].as_i64();
                return Some(Trade {
                    trade_id: tid.unwrap() as u16,
                    direction: TradeDirection::Delete,
                });
            }
            _ => {
                warn!("UNIMPLEMENTED. FIX THIS");
                return Some(Trade {
                    trade_id: 189,
                    direction: TradeDirection::Create,
                });
            }
        }
    }
}

impl EncoderDecoder for Controller {

    /// decodes the encoded string.
    /// Returns the error if it cant decode.
    fn _decode_flight_date(flight_date: String) -> Result<(String, Date), DecoderError> {
        // flight_date is in the form UA96|20150101
        let mut flight_date_v = flight_date.split("|");

        match flight_date_v.next() {
            Some(flight_v) => {
                match flight_date_v.next() {
                    Some(date_v) => {
                        let date_format = format_description::parse("[year][month][day]").unwrap();
                        return Ok((flight_v.to_string(),  Date::parse(date_v, &date_format)?));
                    },
                    None => {
                        return Err(DecoderError::SplitError("Could not get fligth nb".to_string()));
                    },
                }
            },
            None => {
                return Err(DecoderError::SplitError("Could not get date from the encoder".to_string()));
            }
        }
    }

    fn _encode_flight_date(flight: String, date: Date) -> Result<String, Format> {
        // flight_date is in the form UA96|20150101

        let date_format = format_description::parse("[year][month][day]").unwrap();

        Ok(format!("{}|{}", flight, date.format(&date_format)?))
    }
}
