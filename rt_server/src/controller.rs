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
    fn _value_trade(&self, trade: &Trade, market : &str) -> TradeValue {
        let trade_id = trade.trade_id;

        let mkt_used = match market {
            "curr" => "pv",
            "new" => "pv_new",
            _ => "pv",  // unimportant
        };

        //"http://localhost:5010/pv/{trade_id}"
        let result_pricing =
            reqwest::blocking::get(format!("http://{}/{}/{}", self.trade_pricer, mkt_used, trade_id));

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
        let mut prev_potential_portfolio : Option<PortfolioType>= None;
        let mut processing_trades = false;

        loop {
            // receive information from the senders
            let new_potential_trade = trade_receiver.try_recv();
            let new_potential_portfolio = new_portfolio_receiver.try_recv();

            if let Ok(trade) = new_potential_trade {
                info!("CURR market: valuing trade {}", trade.trade_id);
                let trade_value = self._value_trade(&trade, "curr");

                Controller::_trade_result_agg_single(&mut curr_portfolio, Some(trade_value));
                let _ = curr_portfolio_sender.send(curr_portfolio.clone());
                processing_trades = true;
            } else {
                processing_trades = false;
            }

            match new_potential_portfolio {
                Ok(new_portfolio) => {
                    if processing_trades {
                        // processor is working, just mark the prev potential portfolio as a candidate
                        prev_potential_portfolio = Some(new_portfolio);
                    } else {
                        // processor not working
                        info!("CURR market: Switching current <- new market.");
                        // send the switch events
                        let switch_markets = reqwest::blocking::get(format!(
                            "http://{}/switch_markets",
                            self.trade_pricer
                        ));
                        // switch portfolios
                        prev_potential_portfolio = Some(new_portfolio.clone());
                        curr_portfolio = new_portfolio;
                        info!("CURR market: Current portfolio has {} trades", curr_portfolio.keys().len());
                        let _ = curr_portfolio_sender.send(curr_portfolio.clone());
                    }
                },
                _ => {  // we dont have a new portfolio
                    if !processing_trades {
                        if prev_potential_portfolio.is_some() {
                            curr_portfolio = prev_potential_portfolio.clone().unwrap();
                        }
                    }
                },
            }
        }
    }

    /// prices trades on spark
    ///   takes as arguments the list of trades, and pricing client, used for post request
    fn _price_trades_on_spark(&self, trades: &Vec<Trade>, pricing_client : &Client) -> PortfolioType {
        if trades.is_empty() {
            return PortfolioType::new();
        }

        let all_trade_ids = ",".join(
             trades
                .into_iter()
                .map(|trade: &Trade| -> String {trade.trade_id.to_string()} )
        );

        let result_pricing = pricing_client
            .post("http://localhost:5010/pv_spark_new")
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
            let trade_value = self._value_trade(&trade, "new");
            Controller::_trade_result_agg_single(&mut new_portfolio, Some(trade_value));
        }

        new_portfolio
    }


    /// processes the trades on the new market.
    /// new_market_receiver:
    pub fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,  //
        new_trade_receiver: Receiver<Trade>,  // receiving new additional trades
        new_portfolio_sender: SyncSender<PortfolioType>,  // results are sent here
    ) {
        let mut all_trades: Vec<Trade> = vec![];

        let mut now_working = false;  // is it working in this iteration
        let mut prev_working = false;  // is it working in the previous iteration
        let mut new_portfolio = PortfolioType::new();

        let pricing_client = Client::new();

        loop {

            let new_potential_market = new_market_receiver.try_recv();
            let new_potential_trade = new_trade_receiver.try_recv();

            let no_new_trade = new_potential_trade.is_err();
            if let Ok(new_trade) = new_potential_trade {
                if now_working {
                    info!("NEW market: Adding additional trade {}", new_trade.trade_id);
                    let trade_value = self._value_trade(&new_trade, "new");
                    Controller::_trade_result_agg_single(&mut new_portfolio, Some(trade_value));
                }
                all_trades.push(new_trade);
            }

            let no_new_market = new_potential_market.is_err();
            if let Ok(_new_market) = new_potential_market {
                if !now_working {
                    info!("NEW market: Working. {} trades", all_trades.len());
                    //new_portfolio = self._price_trades_on_spark(&all_trades, &pricing_client);
                    new_portfolio = self._price_trades_sequentially(&all_trades);
                    now_working = true;
                }
            }

            debug!("NEW market: New trade {}, new market {}", !no_new_trade, !no_new_market);

            if no_new_trade && no_new_market {
                debug!("NEW market: NOT working.");
                now_working = false;
            }

            if !now_working && prev_working && no_new_market && no_new_trade {
                info!("NEW market: Publishing portfolio. {} trades", new_portfolio.keys().len());
                let _ = new_portfolio_sender.send(new_portfolio);
                new_portfolio = PortfolioType::new();  // new portfolio resets
            }

            prev_working = now_working;
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

            let _ = res_publisher.send(&market_record);
            thread::sleep(Duration::from_millis(10));
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
                    // TODO: ADD THIS CHECK HERE!!
                    let msg_decoded: Value =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();
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

                    let _ = new_mkt_sender.send(mkt_decoded); // send the market over the sender.
                }
                let _ = mkt_listener_.consume_messageset(ms);
            }
            mkt_listener_.commit_consumed().unwrap();
            thread::sleep(Duration::from_millis(100));
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
        let (new_portfolio_sender, new_portfolio_recv) = sync_channel::<PortfolioType>(1);

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
    fn recover_trade(msg_decoded: &Value) -> Trade {
        let msg_payload = &msg_decoded["payload"];
        let event_type = &msg_payload["op"];

        debug!("Getting position: {:?}", msg_payload);

        match event_type.as_str() {
            Some("c") => {
                let tid = msg_payload["after"]["position_id"].as_i64();
                return Trade {
                    trade_id: tid.unwrap() as u16,
                    direction: TradeDirection::Create,
                };
            }
            Some("d") => {
                let tid = msg_payload["before"]["position_is"].as_i64();
                return Trade {
                    trade_id: tid.unwrap() as u16,
                    direction: TradeDirection::Delete,
                };
            }
            _ => {
                warn!("UNIMPLEMENTED. FIX THIS");
                return Trade {
                    trade_id: 189,
                    direction: TradeDirection::Create,
                };
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
