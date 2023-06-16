use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, };
use reqwest::blocking::{Client, Response,};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use core::convert::From;

use crate::market::MktMsgParams;

use crate::trade::Trade;

use crate::portfolio::{
    PortfolioType,
    PricingResults,
    PV01Results,
};


use crate::market::{
    MarketType,
    CurrNewMarket,
    MktEventHandler,
    AOStruct,
};
use crate::ref_deref::TryFromRef;

use crate::pricer::{
    BasicValue,
    PricingMetric,
    PricingStruct,
    RestPricer,
    RestPricerSpark,
    Decoder,
};

use crate::publish::PublishResults;
use crate::streaming::Streaming;
use crate::trade_processor::{MarketSwitching, RiskProcessors, };

pub type PricingParams = HashMap<String, f64>;


/// Controller structure.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
pub struct Controller {
    pricing_params: PricingParams,
    kafka_server_name: String,
    kafka_port: i32,
    trade_pricer: String,
    metric: PricingMetric,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct RTConfig {
    pub kafka_server_name: String,
    pub kafka_server_port: i32,
    pub mkt_topic: String,
    pub results_topic: String,
    pub pos_topic: String,
    pub trade_pricer: String,
    pub pricing_params: PricingStruct,
    pub metric: String,
}



// Controller is generic over MarketType type, which originally was (String, Date)
impl Controller {
    pub fn new(
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
        config_file: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file).unwrap();
        let config_map: RTConfig = serde_yaml::from_reader(config_f).unwrap();

        let controller_metric = if config_map.metric == *"PV" {
            PricingMetric::PV
        } else {
            PricingMetric::PV01
        };

        Ok(Controller::new(
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

    /// listens to kafka stream and stores portfolio locally.
    fn __construct_portfolio(
        &self,
        sender_new: Sender<Trade>,
        sender_curr: Sender<Trade>,
        pos_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name, self.kafka_port);

        let mut pos_listener_ = Consumer::from_hosts(vec![bootstrap_servers,])
            .with_topic_partitions(pos_topic, &[0])
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

                    let trade = possible_trade.unwrap();
                    let _ = sender_new.send(trade);
                    let _ = sender_curr.send(trade);
                }
                let _ = pos_listener_.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            pos_listener_.commit_consumed().unwrap();
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
                    self._handle_mkt_events(
                        mkt_topic,
                        MktMsgParams::AOParams(AOStruct{mkt_sender: new_mkt_sender}))
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

                if results_conv.is_err() {
                    return PricingResults::PV(PortfolioType::new())
                }

                PricingResults::PV(PortfolioType(results_conv.unwrap()))
            },
            PricingMetric::PV01 => {
                let results_conv = result_price.json::<HashMap<String, HashMap<String, f64>>>();

                if results_conv.is_err() {
                    return PricingResults::PV01(PV01Results::new());
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert((*trade_id.clone()).to_string(), PortfolioType::from(trade_result));
                }
                PricingResults::PV01(pv01)
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
                    CurrNewMarket::Current => "pv".to_string(),
                    CurrNewMarket::New => "pv_new".to_string(),
                }
            },
            PricingMetric::PV01 => {
                match market_ {
                    CurrNewMarket::Current => "pv01".to_string(),
                    CurrNewMarket::New => "pv01_new".to_string(),
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
                    CurrNewMarket::Current => "pv_spark".to_string(),
                    CurrNewMarket::New => "pv_spark_new".to_string(),
                }
            },
            PricingMetric::PV01 => {
                match market_ {
                    CurrNewMarket::Current => "pv01_spark".to_string(),
                    CurrNewMarket::New => "pv01_spark_new".to_string(),
                }
            },
        }
    }

    fn _pricing_server_spark(&self) -> String {
        "localhost:5010".to_string()
    }

}


impl Streaming for Controller {
    fn kafka_server_name(&self) -> String {
        self.kafka_server_name.clone()  // TODO: CHECK IF THIS CAN BE REMOVED HERE!!!
    }

    fn kafka_port(&self) -> i32 {
        self.kafka_port
    }
}


impl PublishResults for Controller {

    fn metric(&self) -> PricingMetric {
        self.metric
    }
}


impl MktEventHandler for Controller {

    fn _handle_mkt_msg(
        &self,
        mkt_msg : &Message,
        mkt_msg_params: MktMsgParams,
    ) {

        let optional_mkt = MarketType::try_from_ref(&mkt_msg);

        if optional_mkt.is_err() {
            warn!("Could not conver the market message to the market type!");
            return;
        }

        // optional_mkt is not None, we can unwrap.
        let market_obj = optional_mkt.unwrap();
        let mkt_client_address = "http://localhost:5010/future_market";
        let mkt_update_client = Client::new();
        // update the market rester market_api
        let market_posted = mkt_update_client
            .post(format!("{0}", mkt_client_address))
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

        let MktMsgParams::AOParams(ao_params) = mkt_msg_params else {
            warn!("Parameters provided to _handle_mkt_msg are of the wrong type");
            return;
        };
        let _ = ao_params.mkt_sender.send(market_obj); // send the market to new_market event
    }
}


impl MarketSwitching for Controller {
    /// switch markets on the trade api.
    fn _switch_markets(&self) {
        info!("Switching markets: current <- new.");
        let _ = reqwest::blocking::get(format!(
            "http://{}/switch_markets",
            self.trade_pricer
        ));
    }
}


impl BasicValue for Controller {

    fn metric(&self) -> PricingMetric {
        self.metric
    }

    // prices the trade given the market spec & pricing metric.
    fn _value_trade(&self, trade_id: u16, market: CurrNewMarket, metric: PricingMetric) -> PricingResults {
        debug!("VALUATION: Pricing trade: {}, market: {:?}", trade_id, market);

        // Create or update trades have to be evaluated, so we have to price them.
        //"http://localhost:5010/pv/{trade_id}"
        let result_pricing =
            reqwest::blocking::get(
                format!(
                    "http://{}/{}/{}",
                    self._pricing_server(),
                    self._pricing_endpoint(market, metric),
                    trade_id,
                )
            );

        match result_pricing {
            Ok(result_price) => { self._unwrap_pricing_results(result_price, metric) },
            Err(e) => {
                warn!("Trade {trade_id} could not price correctly: {}", e);
                match metric {
                    PricingMetric::PV => {PricingResults::PV(PortfolioType::new())},
                    PricingMetric::PV01 => {PricingResults::PV01(PV01Results::new())},
                }
            },
        }
    }
}
