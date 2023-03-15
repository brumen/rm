use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use reqwest;
use reqwest::blocking::Client;
use serde_json::Value;
use serde_yaml;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use string_join::Join;
use time::{format_description, Date};
use time::error::Format;
use std::time::Instant;
use core::convert::From;

use crate::encdec::{EncoderDecoder, DecoderError};
use crate::trade::{Trade, TradeDirection, TradeHandling};
use crate::controller::reqwest::blocking::Response;
use crate::portfolio::{MarketType, PortfolioType, TradeValue, AggregatedTrades};

pub type PricingParams = HashMap<String, f64>;


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
    metric: String,
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
    metric: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
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
        metric: String,
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
            metric,
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
            config_map.metric,
        ))
    }

    /// Values the trade id
    /// Makes a call to the rester service, which values the trade.
    fn _value_trade(&self, trade_id: u16, market : CurrNewMarket) -> TradeValue {

        debug!("VALUATION: Pricing trade: {}, market: {:?}", trade_id, market);

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
                match result_price.json::<HashMap<String, f64>>() {  // String in this hash is the trade_id from trade
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

    fn _switch_markets(&self) {
        info!("CURR processor: Switching markets: current <- new.");
        let _ = reqwest::blocking::get(format!(
            "http://{}/switch_markets",
            self.trade_pricer
        ));
    }

    /// Constructing the current market.
    /// receives trades on the trade_receiver channel.
    /// publishes current market results on the curr_mkt_sender channel.
    pub fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<Trade>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<(PortfolioType, Vec<Trade>)>,
    ) {
        //let mut curr_portfolio = PortfolioType::new();
        let mut new_potential_portfolio : Option<(PortfolioType, Vec<Trade>)>;
        let mut all_trades : Vec<Trade> = vec![];
        let mut agg_trades = AggregatedTrades::new();
        let mut nb_conseq_processed_trades : usize;  // number of trades which have been consequitively processed before refreshing to the new
        // market is switched.
        let max_number_trades = 20;  // TODO: FACTOR THIS OUT

        // compute the initial portfolio
        let _ = self._find_initial_trades(&trade_receiver, &mut all_trades, &mut agg_trades, CurrNewMarket::Current);  // this updates all_trades
        let mut curr_portfolio = self._price_trades(&agg_trades, CurrNewMarket::Current);
        let _ = curr_portfolio_sender.send(PortfolioType(curr_portfolio.clone()));

        loop {

            // receive new trade to price on current market
            nb_conseq_processed_trades = 0;
            while let Ok(trade) = trade_receiver.try_recv() {
                if !all_trades.contains(&trade) {
                    info!("CURR: Processing trade {}, dir {:?}", trade.trade_id, trade.direction);
                    let tid = trade.trade_id;
                    if trade.direction == TradeDirection::Create {
                        curr_portfolio += self._value_trade(tid, CurrNewMarket::Current);
                    } else {  // delete trade TODO: THIS HAS TO BE HANDLED TO INCLUDE UPDATE AND ALL
                        curr_portfolio.remove(&(tid.to_string(), self.market_date));
                    }

                    // update aggregated trades and all_trades.
                    agg_trades += trade;
                    all_trades.push(trade); // all trades just add the new one.

                    let _ = curr_portfolio_sender.send(PortfolioType(curr_portfolio.clone()));

                    nb_conseq_processed_trades += 1;
                    if nb_conseq_processed_trades > max_number_trades {
                        info!("CURR: Interrupting the trade processing.");
                        break;  // break out of this while
                    }
                }
            }

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                new_potential_portfolio = Some(new_portfolio);
            }

            if let Some((new_p, new_trades)) = new_potential_portfolio {
                self._switch_markets();
                let new_l = new_trades.len();
                let all_l = all_trades.len();

                if new_l >= all_l {  // new processor is further ahead
                    all_trades = new_trades;
                    curr_portfolio = new_p;
                } else if (new_l < all_l) && (new_l >= all_l - nb_conseq_processed_trades - 1) {  // new is not ahead, but we can still update.
                    curr_portfolio.extend(new_p.0.into_iter());
                }
                let _ = curr_portfolio_sender.send(PortfolioType(curr_portfolio.clone()));
            }
        }
    }

    // augments the existing trades w/ new ones.
    // returns the number of updated trades.
    fn _find_initial_trades(
        &self,
        trade_receiver: &Receiver<Trade>,
        existing_trades : &mut Vec<Trade>,
        agg_trades: &mut AggregatedTrades,
        market_ : CurrNewMarket,
    ) -> u16 {
        let mut nb_added_trades = 0;
        while let Ok(trade) = trade_receiver.try_recv() {
            info!("{:?} market: Getting trade {}", market_, trade.trade_id);
            // update aggregated trades and existing trades.
            *agg_trades += trade;
            existing_trades.push(trade); // all trades just add the new one.
            nb_added_trades += 1;
        }

        nb_added_trades
    }

    /// compute the pricing endpoint for the rester service for
    /// a particular metric and market.
    fn _pricing_endpoint(&self, market_ : CurrNewMarket) -> String {

        if self.metric == "PV".to_string() {
            match market_ {
                CurrNewMarket::Current => return "pv_spark".to_string(),
                CurrNewMarket::New => return "pv_spark_new".to_string(),
            }
        } else {  // assume "PV01"
            match market_ {
                CurrNewMarket::Current => return "pv01_spark".to_string(),
                CurrNewMarket::New => return "pv01_spark_new".to_string(),
            }
        }
    }

    /// prices trades on spark
    ///   takes as arguments the list of trades, and pricing client, used for post request
    fn _price_trades_on_spark(
        &self,
        agg_trades: &AggregatedTrades,
        pricing_client : &Client,
        market_ : CurrNewMarket,
    ) -> PortfolioType {

        // joins all trades with commas, like 190,191,192
        let all_trade_ids = ",".join(
            agg_trades
                .keys()
                .into_iter()
                .map(|trade_id: &u16| -> String {trade_id.to_string()} )
        );

        let pricing_endpoint = self._pricing_endpoint(market_);
        let market_endpoint = pricing_endpoint.as_str();
        info!("ENDPOINT: {}", market_endpoint);
        let result_pricing_start = Instant::now();
        let result_pricing = pricing_client
            .post(format!("http://{}/{}", self.trade_pricer, market_endpoint))
            .form(&HashMap::from([("trades", &all_trade_ids)]))
            .send();
        info!("SPARK pricing took: {:?}", result_pricing_start.elapsed().as_secs_f32());

        // unwrap the result_pricing

        let mut priced_portfolio = match result_pricing {
            Ok(result_price) => self._unwrap_pricing_results(result_price),
            Err(e) => {
                warn!("Trades could not price correctly: {}", e);
                return PortfolioType::new() // TODO: What to do if the trade cant convert
            },
        };

        priced_portfolio *= agg_trades;  // fix the priced portfolio by the weights, aggregated trades.

        priced_portfolio
    }

    fn _price_trades_sequentially(
        &self,
        agg_trades: &AggregatedTrades,
        market_ : CurrNewMarket,
    ) -> PortfolioType {

        let mut new_portfolio = PortfolioType::new();

        for (trade_id, trade_position) in agg_trades.iter() {
            new_portfolio += self._value_trade(*trade_id, market_) * (*trade_position);
        }

        new_portfolio
    }

    fn _price_trades(
        &self,
        agg_trades: &AggregatedTrades,
        market_ : CurrNewMarket,
    ) -> PortfolioType {

        let nb_trades = agg_trades.keys().len();
        let pricing_client = Client::new();

        if nb_trades > 30 {  // TODO: FACTOR THIS 30 out.
            return self._price_trades_on_spark(agg_trades, &pricing_client, market_);
        }

        self._price_trades_sequentially(agg_trades, market_)
    }


    // converts the spark response into a trade value.
    fn _unwrap_pricing_results(&self, result_price: Response) -> PortfolioType {

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
                warn!("_price_trades: Could not convert the result to a map: {:?}", e);
                return PortfolioType::new();
            }
        }
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

    /// processes the trades on the new market.
    /// new_market_receiver:
    pub fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_trade_receiver: Receiver<Trade>,  // receiving new additional trades
        new_portfolio_sender: Sender<(PortfolioType, Vec<Trade>)>,  // results are sent here
    ) {
        let mut all_trades : Vec<Trade> = vec![];
        let mut agg_trades = AggregatedTrades::new();
        let mut new_portfolio = PortfolioType::new();

        loop {

            // handling new trade event
            let _ = self._find_initial_trades(&new_trade_receiver, &mut all_trades, &mut agg_trades, CurrNewMarket::New);

            let new_market_event = self._new_market_event(&new_market_receiver);
            if new_market_event {
                info!("NEW: Working. {} trades", agg_trades.keys().len());
                new_portfolio = self._price_trades(&agg_trades, CurrNewMarket::New);
            }

            // catch up any remaining trades
            while let Ok(trade) = new_trade_receiver.try_recv() {
                info!("NEW: Processing trade {}, dir {:?}", trade.trade_id, trade.direction);
                let tid = trade.trade_id;
                if trade.direction == TradeDirection::Create {
                    new_portfolio += self._value_trade(tid, CurrNewMarket::New);
                } else {  // delete trade TODO: THIS HAS TO BE HANDLED TO INCLUDE UPDATE AND ALL
                    new_portfolio.remove(&(tid.to_string(), self.market_date));
                }

                // update all_trades and agg_trades.
                all_trades.push(trade);
                agg_trades += trade;
            }

            // decisions whether to publish the market or not.
            if new_market_event {
                info!("NEW: Publishing portfolio. {} trades", new_portfolio.keys().len());
                let _ = new_portfolio_sender.send((PortfolioType(new_portfolio.clone()), all_trades.clone()));
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
        let (new_portfolio_sender, new_portfolio_recv) = channel::<(PortfolioType, Vec<Trade>)>();  //sync_channel (1)

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
