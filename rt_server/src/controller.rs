use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, };
use kafka::producer::{Producer, Record, RequiredAcks};
use reqwest;
use reqwest::blocking::{Client, Response,};
use serde_yaml;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use time::Date;
use core::convert::From;

use crate::trade::{
    Trade,
    TradeDirection,
    TradeHandling,
    AOTrade,
};

use crate::portfolio::{
    PortfolioType,
    AggregatedTrades,
    PV01Results,
    PricingResults,
};

use crate::market::{MarketType, };
use crate::ref_deref::{TryFromRef,};

use crate::pricer::{
    PricingMetric,
    PricingStruct,
    RestPricer,
    RestPricerSpark,
    Decoder,
    PricePortfolioSpark,
};

pub type PricingParams = HashMap<String, f64>;


/// Controller structure.
/// market_date: date when we are pricing.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
pub struct Controller {
    market_date: Date,
    option_type: String,
    pricing_params: PricingParams,
    kafka_server_name: String,
    kafka_port: i32,
    trade_pricer: String,
    metric: PricingMetric,
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


// Controller is generic over MarketType type, which originally was (String, Date)
impl Controller {
    pub fn new(
        market_date: Date,
        option_type: String,
        pricing_params_: Option<PricingParams>,
        kafka_server_name: String,
        kafka_port: i32,
        trade_pricer: String,
        metric: PricingMetric,
    ) -> Self {
        // if given the params, use them, otherwise construct empty map
        let pricing_init = match pricing_params_ {
            Some(pricing_hash) => pricing_hash,
            None => PricingParams::new(),
        };

        Controller {
            market_date: market_date,
            option_type,
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
        option_type: String,
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

        Ok(Controller::new(
            market_date,
            option_type,
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
        ))
    }

    /// switch markets on the trade api.
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
        let mut new_potential_portfolio : Option<(PortfolioType, Vec<Trade>)>;
        let mut all_trades : Vec<Trade> = vec![];
        let mut agg_trades = AggregatedTrades::new();
        let mut nb_conseq_processed_trades : usize;  // number of trades which have been consequitively processed before refreshing to the new
        // market is switched.
        let max_number_trades = 20;  // TODO: FACTOR THIS OUT

        // compute the initial portfolio
        let _ = self._find_initial_trades(&trade_receiver, &mut all_trades, &mut agg_trades, CurrNewMarket::Current);  // this updates all_trades
        let mut curr_portfolio = self._price_trades(&agg_trades, CurrNewMarket::Current, self.metric);
        let _ = curr_portfolio_sender.send(curr_portfolio.clone());

        loop {

            // receive new trade to price on current market
            nb_conseq_processed_trades = 0;
            while let Ok(trade) = trade_receiver.try_recv() {
                if !all_trades.contains(&trade) {
                    info!("CURR: Processing trade {}, dir {:?}", trade.trade_id, trade.direction);
                    let trade_v = self._value_trade(trade.trade_id, CurrNewMarket::Current, self.metric);
                    match trade.direction {
                        TradeDirection::Create => curr_portfolio += trade_v,
                        TradeDirection::Delete => curr_portfolio -= trade_v,
                        _ => {},
                    }

                    // update aggregated trades and all_trades.
                    agg_trades += trade;
                    all_trades.push(trade); // all trades just add the new one.

                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());

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
                let _ = curr_portfolio_sender.send(curr_portfolio.clone());
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
                new_portfolio = self._price_trades(&agg_trades, CurrNewMarket::New, self.metric);
            }

            // catch up any remaining trades
            while let Ok(trade) = new_trade_receiver.try_recv() {
                info!("NEW: Processing trade {}, dir {:?}", trade.trade_id, trade.direction);
                let trade_v = self._value_trade(trade.trade_id, CurrNewMarket::New, self.metric);
                match trade.direction {
                    TradeDirection::Create => {new_portfolio += trade_v;},
                    TradeDirection::Delete => {new_portfolio -= trade_v;},
                    _ => {},
                }

                // update all_trades and agg_trades.
                all_trades.push(trade);
                agg_trades += trade;
            }

            // decisions whether to publish the market or not.
            if new_market_event {
                info!("NEW: Publishing portfolio. {} trades", new_portfolio.keys().len());
                let _ = new_portfolio_sender.send((new_portfolio.clone(), all_trades.clone()));
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
                for msg in ms.messages() {
                    let possible_trade = Trade::try_from_ref(msg);
                    // Controller::recover_trade(msg);

                    // if the trade is None, there is possibly something wrong in
                    if possible_trade.is_err() {
                        warn!("Trade is WRONG!!! FIX IT!");
                        continue;
                    }

                    if possible_trade.is_ok() {
                        let trade = possible_trade.unwrap();
                        let _ = sender_new.send(trade);
                        let _ = sender_curr.send(trade);
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
            let curr_mkt_json = serde_json::ser::to_string(&curr_mkt).unwrap();
            let curr_mkt_pv = format!("{{\"{}\": {}}}", self.metric, curr_mkt_json);

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
    pub fn _handle_mkt_events (
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

        let mkt_update_client = Client::new();

        loop {
            debug!("Getting new markets from {mkt_topic}.");
            for mkt_msg_set in mkt_listener_.poll().unwrap().iter() {  // TODO: What to do w/ unwrap here??
                for mkt_msg in mkt_msg_set.messages() {

                    // TODO: REMOVE this line below here.
                    //let optional_mkt : Option<MarketType> = Self::_decode_mkt_msg(&mkt_msg);
                    let optional_mkt = MarketType::try_from_ref(&mkt_msg);

                    if optional_mkt.is_err() {
                        warn!("Could not conver the market message to the market type!");
                        continue;
                    }

                    // optional_mkt is not None, we can unwrap.
                    let market_obj = optional_mkt.unwrap();

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

                    // construct a new MarketType element.
                    let mut mkt_decoded = MarketType::new();
                    for (mkt_key, mkt_price) in market_obj.into_iter() {
                        let _ = &mkt_decoded.insert(mkt_key.clone(), mkt_price);
                    }
                    let _ = new_mkt_sender.send(mkt_decoded); // send the market to new_market event
                }
                let _ = mkt_listener_.consume_messageset(mkt_msg_set);
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
        let (new_portfolio_sender, new_portfolio_recv) = channel::<(PortfolioType, Vec<Trade>)>();

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


impl Decoder for Controller {

    // converts the spark response into a trade value.
    fn _unwrap_pricing_results(&self, result_price: Response, metric: PricingMetric) -> PricingResults {

        match metric {
            PricingMetric::PV => {
                let results_conv = result_price.json::<HashMap<String, f64>>();
                if results_conv.is_ok() {
                    PricingResults::PV(PortfolioType(results_conv.unwrap()))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
            },
            PricingMetric::PV01 => {
                let results_conv = result_price.json::<HashMap<String, HashMap<String, f64>>>();
                if results_conv.is_ok() {
                    let mut pv01 = PV01Results::new();
                    for (trade_id, trade_result) in results_conv.unwrap().iter() {
                        let _ = pv01.insert((*trade_id.clone()).to_string(), PortfolioType::from(trade_result));
                    }
                    PricingResults::PV01(pv01)
                } else {
                    PricingResults::PV01(PV01Results::new())
                }
            },
        }
    }

}


impl RestPricer for Controller {

    /// pricing endpoints for valuing on the go
    fn _pricing_endpoint(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String {

        match metric {
            PricingMetric::PV => {
                match market_ {
                    CurrNewMarket::Current => return "pv".to_string(),
                    CurrNewMarket::New => return "pv_new".to_string(),
                }
            },
            PricingMetric::PV01 => {
                match market_ {
                    CurrNewMarket::Current => return "pv01".to_string(),
                    CurrNewMarket::New => return "pv01_new".to_string(),
                }
            },
        }
    }

    // server ip which prices the trades.
    fn _pricing_server(&self) -> String {
        "localhost:5010".to_string()
    }

}


impl RestPricerSpark for Controller {

    /// compute the pricing endpoint for the rester service for
    /// a particular metric and market.
    fn _pricing_endpoint_spark(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String {

        match metric {
            PricingMetric::PV => {
                match market_ {
                    CurrNewMarket::Current => return "pv_spark".to_string(),
                    CurrNewMarket::New => return "pv_spark_new".to_string(),
                }
            },
            PricingMetric::PV01 => {
                match market_ {
                    CurrNewMarket::Current => return "pv01_spark".to_string(),
                    CurrNewMarket::New => return "pv01_spark_new".to_string(),
                }
            },
        }
    }

    fn _pricing_server_spark(&self) -> String {
        "localhost:5010".to_string()
    }

}
