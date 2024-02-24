//
//  Real time risk manager using local market & local pricing
//

use tracing::{debug, info, warn};
use serde::Deserialize;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use crate::market::{
    CurrNewMarket,
    MarketSwitching,
    MarketType,
    MktMsgParams,
    TradeMarketDiscovery,
    MarketGeneral,
};
use crate::mkt_handler::MktEventHandler;
use crate::portfolio::PortfolioType;
use crate::pricer::{MarketPricingOptions, PricingMetric,};
use crate::publish::PublishResults;
use crate::streaming::Streaming;
use crate::trade::{BaseTrade, TradeReduce, TradeRep, TradeTypes};
use crate::trade_procs::RiskProcessors;
use crate::process_trade::ObtainMarket;

/// RTRM - Real time risk manager using local
///    local market and local pricing.
/// TODO: Eventually this has to be refactored to reuse the common parts.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
/// curr_market: current market to be priced on
/// new_market: new market used for pricing.
#[derive(Debug)]
pub struct RTRMLocal {
    kafka_server_name: String,
    kafka_port: i32,
    metric: PricingMetric,
    curr_market: Arc<Mutex<MarketType>>,
    new_market: Arc<Mutex<MarketType>>,
    future_market: Arc<Mutex<MarketType>>,
}

#[derive(Deserialize)]
pub struct RTRMConfig {
    pub kafka_server_name: String,
    pub kafka_server_port: i32,
    pub metric: String,
    pub results_topic: String,
    pub mkt_topic: String,
    pub risk_topic: String,
    pub positions_topic: String,
}

// Controller is generic over MarketType type, which originally was (String, Date)
impl RTRMLocal {
    pub fn new(kafka_server_name: String, kafka_port: i32, metric: PricingMetric) -> Self {
        Self {
            kafka_server_name,
            kafka_port,
            metric,
            curr_market: Arc::new(Mutex::from(MarketType::new())),
            new_market: Arc::new(Mutex::from(MarketType::new())),
	    future_market: Arc::new(Mutex::from(MarketType::new())),
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(config_file: String) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file).unwrap();
        let config_map: RTRMConfig = serde_yaml::from_reader(config_f).unwrap();

        let controller_metric = match config_map.metric.as_str() {
            "PV" => PricingMetric::PV,
            "PV01" => PricingMetric::PV01,
            "PnL" => PricingMetric::PnL,
            &_ => todo!(),
        };

        Ok(RTRMLocal::new(
            // pricing_params
            config_map.kafka_server_name,
            config_map.kafka_server_port,
            controller_metric,
        ))
    }
}

impl MarketSwitching for RTRMLocal {
    /// switch markets on the trade api.

    async fn _switch_all_markets(&self) {
        info!("_switch_markets: Switching markets: current <- new.");
	    self._internal_switch_all_markets();
    }

    async fn _switch_new_fut_markets(&self) {
	    self._internal_switch_new_fut_markets();
    }

    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>> {
        self.curr_market.clone()
    }

    fn _new_mkt(&self) -> Arc<Mutex<MarketType>> {
        self.new_market.clone()
    }

    fn _future_mkt_ready(&self) -> bool {
	    *self.future_market.lock().unwrap() != *self.new_market.lock().unwrap()
    }

    fn _future_mkt(&self) -> Arc<Mutex<MarketType>> {
	    self.future_market.clone()  // TODO: CAN THIS BE DONE W/O cloning???
    }
}

impl Streaming for RTRMLocal {
    fn kafka_server_name(&self) -> String {
        self.kafka_server_name.clone()
    }

    fn kafka_port(&self) -> i32 {
        self.kafka_port
    }
}

impl MktEventHandler for RTRMLocal {
    /// updates the local market variable.
    async fn _handle_mkt_msg(
        &self,
        market_obj: MarketType,
        new_mkt_sender: Sender<MarketType>,
        _mkt_params: MktMsgParams,
    ) {
        *self
            ._curr_mkt()
            .lock()
            .expect("_handle_mkt_msg: Could not lock curr_mkt") += &market_obj;

        if let Err(e) = new_mkt_sender.send(market_obj).await {
            warn!("Could not send a message about new market: {:?}", e);
        }
    }
}

impl ObtainMarket for RTRMLocal {
    fn get_market(
        &self,
        curr_new_mkt: CurrNewMarket,
    ) -> crate::market::MarketGeneral {
        match curr_new_mkt {
            // TODO: FIX THESE CLONING HERE
            CurrNewMarket::Current => MarketGeneral::MarketLocal(MarketType(self.curr_market.lock().unwrap().clone())),
            CurrNewMarket::New => MarketGeneral::MarketLocal(MarketType(self.new_market.lock().unwrap().clone())),
        }
    }
}


impl TradeMarketDiscovery for RTRMLocal { }

// TODO: THIS IS PROBABLY WRONG!!!
impl RiskProcessors for RTRMLocal {

    fn _price_existing_trades(
        &self,
        all_trades: &TradeRep<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl std::future::Future<Output=PortfolioType> + Send {
        async move {
            let mut p = PortfolioType::default();

            for trade in all_trades.values() {
                p += self._process_trade(trade, metric, pricing_options, curr_new_mkt).await;
            }
            p
        }
    }

    async fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &mut Receiver<Self::TR>,
        all_trades: &mut TradeRep<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<Self::TR>)>,
    ) {
        while let Ok(trade) = trade_receiver.try_recv() {
            debug!("_trade_processor_curr: Received good trade {:?}", trade);

            let new_trade = !all_trades.contains(&trade.id());
            if new_trade {
                *all_trades += &trade;
            }

            if new_trade {
                *curr_portfolio +=
                    self._process_trade(&trade, metric, pricing_options, curr_new_mkt).await;

                let _ =
                    new_trades_sender.send((curr_portfolio.clone(), TradeRep(all_trades.clone())));
            }
        }
    }
}

impl PublishResults for RTRMLocal {
    fn metric(&self) -> PricingMetric {
        self.metric
    }
}

impl TradeReduce for RTRMLocal {
    type ReductionType = TradeTypes;
    type TradeType = TradeTypes;

    fn reduce(&self, trade: &Self::TradeType) -> Self::ReductionType {
        (*trade).clone()
    }
}
