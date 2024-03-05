use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::channel;
use tracing::{info, warn, instrument, error, debug};
use core::convert::From;
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use rdkafka::consumer::{StreamConsumer, CommitMode};
use rdkafka::message::BorrowedMessage;


use crate::ref_deref::TryFromRef;
use crate::trade::TradeDirection;
use crate::portfolio::PricingResults;
use crate::market::{MarketGeneral, CurrNewMarket,};

use crate::ao_trade::{AOTrade, AOTradeRep};
use crate::market::{
    MarketSwitching, MarketType, MktMsgParams,
};
use crate::mkt_handler::MktEventHandler;
use crate::portfolio::PortfolioType;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::{
    Decoder, MarketPricingOptions, PricingMetric, PricingStruct, RestPricerSpark,
};
use crate::process_trade::ObtainMarket;
use crate::publish::PublishResults;
use crate::streaming::Streaming;
use crate::trade::{BaseTrade, TradeReduce, TradeRep};
use crate::process_trade::ProcessTradeValue;

pub type PricingParams = HashMap<String, f64>;

/// Controller structure.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
#[derive(Debug)]
pub struct ControllerSeq {
    pricing_params: PricingParams,
    kafka_server_name: String,
    kafka_port: i32,
    trade_pricer: String,
    metric: PricingMetric,
    curr_mkt: Arc<Mutex<MarketType>>,
    trades: Arc<Mutex<TradeRep::<AOTradeRep>>>,
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
    pub pricing_server: String,
}

impl Streaming for ControllerSeq {
    fn kafka_server_name(&self) -> String {
        self.kafka_server_name.clone() // TODO: CHECK IF THIS CAN BE REMOVED HERE!!!
    }

    fn kafka_port(&self) -> i32 {
        self.kafka_port
    }
}

impl PublishResults for ControllerSeq {
    fn metric(&self) -> PricingMetric {
        self.metric
    }
}

impl MktEventHandler for ControllerSeq {

    /// _handle_mkt_msg - for controller
    ///    the message is sent to update future_market.
    ///    future_market becomes new_market when the trade
    ///    processor determines it should be switched.
    async fn _handle_mkt_msg(
        &self,
        market_obj: MarketType,
        new_mkt_sender: Sender<MarketType>,
        _mkt_params: MktMsgParams,
    ) {
	    **(self.curr_mkt.lock().expect("Could not lock current")) = market_obj.into();
    }
}


// TradeReduce reduces the trade to empty,
// we dont need any additional information from the trade.
impl TradeReduce for ControllerSeq {
    type ReductionType = AOTradeRep;
    type TradeType = AOTrade;

    fn reduce(&self, trade: &Self::TradeType) -> Self::ReductionType {
        AOTradeRep(trade.id())
    }
}



// implementation of decoder for results on the portfolio.
impl Decoder for ControllerSeq {}

impl ObtainMarket for ControllerSeq {
    fn get_market(
        &self,
        curr_new_mkt: CurrNewMarket,
    ) -> MarketGeneral {
        MarketGeneral::MarketRemote(curr_new_mkt)
    }
}

impl RestPricerSpark<AOTradeRep> for ControllerSeq {
    fn _pricing_server_spark(&self) -> String {
        self.trade_pricer.to_owned() // TODO: THIS IS GARBAGE
    }

    fn _pricing_endpoint_spark(&self, market_: CurrNewMarket, metric: PricingMetric) -> String {
        let metric_str = match metric {
            PricingMetric::PV => "pv".to_owned(),
            PricingMetric::PV01 => "pv01".to_owned(),
            _ => todo!(),
        };

        let market_str = match market_ {
            CurrNewMarket::Current => "spark".to_owned(),
            CurrNewMarket::New => "spark_new".to_owned(),
        };

        format!("{}/{}", metric_str, market_str)
    }
}

// Controller is generic over MarketType type, which originally was (String, Date)
impl ControllerSeq {

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

        Self {
            pricing_params: pricing_init,
            kafka_server_name,
            kafka_port,
            trade_pricer,
            metric,
            curr_mkt: Arc::new(Mutex::new(MarketType::new())),
            trades: Arc::new(Mutex::new(TradeRep::<AOTradeRep>::default())),
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(config_file: String) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file)?;
        let config_map: RTConfig = serde_yaml::from_reader(config_f)?;

        let controller_metric = if config_map.metric == *"PV" {
            PricingMetric::PV
        } else {
            PricingMetric::PV01
        };

        Ok(Self::new(
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

    fn _process_trade<'a> (
        &'a self,
        trade: AOTradeRep,
        metric: PricingMetric,
        pricing_options: &'a MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl std::future::Future<Output=PortfolioType> + Send + 'a {

        async move {
            // let _trade_id = trade.id();
            // let trade_direction = tr.direction();

            let market = self.get_market(curr_new_mkt);
            let trade_v = trade
                .value_by_metric2(metric, pricing_options, market)
                .await;

            // TODO: MAYBE REMOVE OR INCORPORATE
            let trade_v_dir = match trade.direction() {
                TradeDirection::Create => trade_v,
                TradeDirection::Delete => -trade_v,
                TradeDirection::Update => todo!(),
            };

            return match trade_v_dir {
                PricingResults::PV(pv) => pv,
                PricingResults::PV01(pv01) => pv01.aggregate(),
                PricingResults::PnL(pnl) => pnl,
            };

            // let mut cp = curr_portfolio.lock().unwrap();
            //TradeDirection::Create => *cp += trade_portf,

            //match trade_direction {
            //    TradeDirection::Create => trade_portf,
            //    TradeDirection::Delete => - trade_portf,
            //    _ => todo!(),
            //}
        }
    }

    fn _price_new_trades<'a>(
        &'a self,
        metric: PricingMetric,
        pricing_options: &'a MarketPricingOptions,
        result_sender: Sender<(PortfolioType, TradeRep<AOTradeRep>)>,
    ) -> impl std::future::Future<Output=()> + Send + 'a {
        async move {
            loop {
                let mut curr_portfolio = PortfolioType::default();

                let all_trades = self.trades.lock().expect("Could not lock trades").clone();
                // TODO : THIS IS WRONG
                debug!("ALL TRADES: {:?}", all_trades);
                for (trade_id, trade_rep) in &all_trades {
                    let tr = trade_rep.clone();
                    let trade_value = self
                        ._process_trade(tr, metric, pricing_options, CurrNewMarket::Current)
                        .await;
                    debug!("Price trade: {:?}", trade_value);
                    curr_portfolio += trade_value;
                }
                info!("SENDING {:?}", curr_portfolio);
                let _ = result_sender.send((curr_portfolio, TradeRep(all_trades))).await;
                tokio::time::sleep(tokio::time::Duration::new(1,0)).await;
            }
        }
    }

    /// writes a newly received trade to the list of trades.
    fn _send_trade_fut<'a>(
        &'a self,
        position_listener: StreamConsumer,
    ) -> impl std::future::Future<Output=()> + Send + 'a {
        async move {
            loop {
                let trade = position_listener.recv().await;
                info!("Got trade: {:?}", trade);
                let message = trade.unwrap();  // TODO: FIX UNWRAP
		        match AOTrade::try_from_ref(&message) {
			        Err(e) => {
			            warn!("Problem w/ trade: {:?}", e);
			        }
			        Ok(trade) => {
			            info!("Sending trade {:?} to CURR & NEW processor.", &trade);

			            // add trades to trade_reduce
			            let tr = self.reduce(&trade);
			            *self.trades.lock().unwrap() += &tr;
			        }
		        }
                // match position_listener.commit_message(&message, CommitMode::Async) {
                //     Ok(_) => {
                //         debug!("Successful commit of message!");
                //     },
                //     Err(e) => {
                //         error!("Something wrong with {:?}: {:?}", message, e);
                //     },
                // }
		    }
        }
    }

    /// Sequential controller receives trades, prices them, and publishes result.
    ///   Then the simple loop is repeated.
    pub fn start<'a>(
        &'a self,
        position_topic: String,
        mkt_topic: String,
        results_topic: String,
        mkt_params: MktMsgParams,
        pricing_options: &'a MarketPricingOptions,
    ) -> impl std::future::Future<Output=()> + Send + 'a {

        async move {
            let buffer_size = 10000;

            let (curr_portfolio_sender, curr_portfolio_recv) =
                channel::<(PortfolioType, TradeRep<AOTradeRep>)>(buffer_size);
            let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port(),);

            let position_listener = connect_with_retries_rd(
                bootstrap_servers.as_str(),
                position_topic.as_str(),
            );
            tokio_scoped::scope(
                |scope| {
                    scope.spawn(
                        self._send_trade_fut(
                            position_listener
                        )
                    );
                    scope.spawn(
                        self._price_new_trades(
                            self.metric,
                            pricing_options,
                            curr_portfolio_sender
                        )
                    );
                    scope.spawn(
                        self._publish_results(
                            curr_portfolio_recv,
                            results_topic
                        )
                    );
                }
            )
        }
    }
}
