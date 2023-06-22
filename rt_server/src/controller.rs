use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use kafka::consumer::Message;
use reqwest::blocking::{Client, Response,};
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use core::convert::From;
use std::sync::{Arc, Mutex};

use crate::market::MktMsgParams;
use crate::trade::{TradeAggregation, BaseTrade, };
use crate::portfolio::{
    PortfolioType,
    PricingResults,
    PV01Results,
    AggregatedTrades,
};


use crate::market::{
    MarketType,
    CurrNewMarket,
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
use crate::trade_processor::MarketSwitching;
use crate::mkt_handler::MktEventHandler;

pub type PricingParams = HashMap<String, f64>;


/// Controller structure.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
pub struct Controller<TT: Sized> {
    pricing_params: PricingParams,
    kafka_server_name: String,
    kafka_port: i32,
    trade_pricer: String,
    metric: PricingMetric,
    _all_trades: Arc<Mutex<Vec<TT>>>,
    _aggregated_trades: Arc<Mutex<AggregatedTrades>>,
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
impl<TT> Controller<TT> {
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

        Controller::<TT> {
            pricing_params: pricing_init,
            kafka_server_name,
            kafka_port,
            trade_pricer,
            metric,
            _all_trades: Arc::new(Mutex::new(Vec::<TT>::new())),
            _aggregated_trades: Arc::new(Mutex::new(AggregatedTrades::new())),
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
}


impl<TT> Decoder for Controller<TT> {

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


impl<TT : Send + PartialEq + BaseTrade + Clone + std::fmt::Debug + for<'a> TryFromRef<Message<'a>>> TradeAggregation for Controller<TT> {
    type TT = TT;

    fn all_trades(&self) -> Vec<Self::TT> {
        // TODO: IDK IF THIS IS RIGHT????
        // TODO: SHITTIEST WORK EVER
        let mut new_trades = Vec::<Self::TT>::new();
        for v in &*self._all_trades.lock().unwrap() {
            new_trades.push(v.clone());
        }

        new_trades

    }

    fn add_trade_mut(&self, trade: Self::TT) {
        let all_trades = &mut *self._all_trades.lock().unwrap();
        all_trades.push(trade);
    }

    fn aggregated_trades(&self) -> AggregatedTrades {

        let mut new_agg_trades = AggregatedTrades::new();  //Vec::<Self::TT>::new();
        for (agg_name, agg_val) in self._aggregated_trades.lock().unwrap().iter() {
            new_agg_trades.insert(agg_name.clone(), *agg_val);
        }

        new_agg_trades

        //*self._aggregated_trades.clone().lock().unwrap()
    }
}


impl<TT: Send + PartialEq + BaseTrade + Clone + std::fmt::Debug + for<'a> TryFromRef<Message<'a>>> RestPricer for Controller<TT> {

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


impl<TT> RestPricerSpark for Controller<TT> {

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


impl<TT> Streaming for Controller<TT> {
    fn kafka_server_name(&self) -> String {
        self.kafka_server_name.clone()  // TODO: CHECK IF THIS CAN BE REMOVED HERE!!!
    }

    fn kafka_port(&self) -> i32 {
        self.kafka_port
    }
}


impl<TT> PublishResults for Controller<TT> {

    fn metric(&self) -> PricingMetric {
        self.metric
    }
}


impl<TT> MktEventHandler for Controller<TT> {

    fn _handle_mkt_msg(
        &self,
        mkt_msg : &Message,
        new_mkt_sender: Sender<MarketType>,
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

        //let MktMsgParams::AOParams(ao_params) = mkt_msg_params else {
        //    warn!("Parameters provided to _handle_mkt_msg are of the wrong type");
        //    return;
        //};
        let _ = new_mkt_sender.send(market_obj); // send the market to new_market event
    }
}


impl<TT> MarketSwitching for Controller<TT> {
    /// switch markets on the trade api.
    fn _switch_markets(&self) {
        info!("Switching markets: current <- new.");
        let _ = reqwest::blocking::get(format!(
            "http://{}/switch_markets",
            self.trade_pricer
        ));
    }
}


impl<TT : Send + PartialEq + BaseTrade + Clone + std::fmt::Debug + for<'a> TryFromRef<Message<'a>>> BasicValue for Controller<TT> {

    fn metric(&self) -> PricingMetric {
        self.metric
    }

    // prices the trade given the market spec & pricing metric.
    fn _value_trade(&self, trade_id: String, market: CurrNewMarket, metric: PricingMetric) -> PricingResults {
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
