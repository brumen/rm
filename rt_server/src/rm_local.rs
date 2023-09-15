//
//  Real time risk manager using local market & local pricing
//

use log::{debug, info, warn};
use std::sync::{Arc, Mutex,};
use std::sync::mpsc::{Sender, Receiver, };
use kafka::consumer::Message;
use serde::Deserialize;

use crate::portfolio::PortfolioType;
use crate::market::{
    MarketType,
    MktMsgParams,
    CurrNewMarket,
};
use crate::mkt_handler::MktEventHandler;
use crate::ref_deref::TryFromRef;

use crate::pricer::{
    PricingMetric,
    PriceTrade,
    MarketPricingOptions,
};

use crate::streaming::Streaming;
use crate::trade::{BaseTrade, TradeDirection, TradeRep};
use crate::trade_processor::{MarketSwitching, TradeMarketDiscovery};
use crate::publish::PublishResults;
use crate::trade_procs::{ProcessTradeSync, RiskProcessors,};

/// RTRM - Real time risk manager using local
///    local market and local pricing.
/// TODO: Eventually this has to be refactored to reuse the common parts.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
/// curr_market: current market to be priced on
/// new_market: new market used for pricing.
pub struct RTRMLocal {
    kafka_server_name: String,
    kafka_port: i32,
    metric: PricingMetric,
    curr_market: Arc<Mutex<MarketType>>,
    new_market: Arc<Mutex<MarketType>>,
}


#[derive(Deserialize)]
pub struct RTRMConfig {
    pub kafka_server_name: String,
    pub kafka_server_port: i32,
    pub metric: String,
}


// Controller is generic over MarketType type, which originally was (String, Date)
impl RTRMLocal {
    pub fn new(
        kafka_server_name: String,
        kafka_port: i32,
        metric: PricingMetric,
    ) -> Self {

        Self {
            kafka_server_name,
            kafka_port,
            metric,
            curr_market: Arc::new(Mutex::from(MarketType::new())),
            new_market: Arc::new(Mutex::from(MarketType::new())),
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(
        config_file: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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
    fn _switch_markets(&self) {
        info!("_switch_markets: Switching markets: current <- new.");

        let nm = self.new_market.lock().expect("_switch_markets: Could not lock new market.");
        **self.curr_market.lock().expect("_switch_markets: Could not lock current market") = (*nm).clone();

	    debug!("_switch_markets: Curr market now: {:?}", nm);
    }

    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>> {
	    self.curr_market.clone()
    }

    fn _new_mkt(&self) -> Arc<Mutex<MarketType>> {
	    self.new_market.clone()
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
    fn _handle_mkt_msg(
        &self,
        mkt_msg: &Message,
        new_mkt_sender: Sender<MarketType>,
        _mkt_params: MktMsgParams,
    ) {

        let new_market = MarketType::try_from_ref(mkt_msg);
        debug!("_handle_mkt_msg: Got quote: {:?}", new_market);
        if new_market.is_err() {
            return;  // ignore the market message if it cant be decoded correctly.
        }

        let Ok(new_mkt_real) = new_market else {
            warn!("_handle_mkt_msg: New market !!!!");
            return;
        };

	    *self._curr_mkt().lock().expect("_handle_mkt_msg: Could not lock curr_mkt") += &new_mkt_real;
        let _ = new_mkt_sender.send(new_mkt_real);
    }
}


impl<TT> ProcessTradeSync<TT> for RTRMLocal
where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTrade + BaseTrade + std::marker::Send
{

    fn _process_trade(
        &self,
        trade: TT,
        metric: PricingMetric,
        _pricing_options: &MarketPricingOptions,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {

        let trade_id = trade.id();
        let trade_direction = trade.direction();

        let market = match curr_new_mkt {
            CurrNewMarket::Current => self._curr_mkt(),
            CurrNewMarket::New => self._new_mkt(),
        };

        debug!("_trade_processor_curr: Processing trade {}, dir {:?}", trade_id, trade_direction);
        let trade_v = trade.value_by_metric(
            metric,
            &market.lock().unwrap(),
        );

        debug!("_trade_processor_curr: Trade value = {:?}", trade_v);
        match trade_direction {
            TradeDirection::Create => *(curr_portfolio.lock().unwrap()) += trade_v,
            TradeDirection::Delete => *(curr_portfolio.lock().unwrap()) -= trade_v,
            TradeDirection::Update => todo!(),
        }
        // TOOD: CHECK THIS CLONE HERE!!!
        let _ = curr_portfolio_sender.send(curr_portfolio.lock().unwrap().clone());
    }
}

impl<TT> TradeMarketDiscovery<TT> for RTRMLocal
where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade
{ }

impl<TT> RiskProcessors<TT> for RTRMLocal
where
    TT: BaseTrade + PartialEq + std::fmt::Debug + Clone + PriceTrade + Send + Sync
{

    fn _existing_trades(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {

        for trade in all_trades.lock().unwrap().values() {
            self._process_trade(
                trade.clone(),
                metric,
                pricing_options,
                curr_portfolio.clone(),
                curr_portfolio_sender,
                curr_new_mkt,
            );
        }
    }

    fn _new_trades(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {
        while let Ok(trade) = trade_receiver.try_recv() {
            debug!("_trade_processor_curr: Received good trade {:?}", trade);

            let mut all_trades_local = all_trades.lock().unwrap();
            let new_trade = !all_trades_local.contains(&trade);
            if new_trade {
                all_trades_local.add_trade(trade.clone());
            }
            drop(all_trades_local);

            if new_trade {
                self._process_trade(
                    trade.clone(),
                    metric,
                    pricing_options,
                    curr_portfolio.clone(),
                    curr_portfolio_sender,
                    curr_new_mkt,
                );
            }
        }
    }

    fn _run_computations(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {
        while let Ok(trade) = trade_receiver.try_recv() {
            debug!("_trade_processor_curr: Received good trade {:?}", trade);

            let mut all_trades_lock = all_trades.lock().unwrap();
            let trade_exists = all_trades_lock.contains(&trade);
            all_trades_lock.add_trade(trade.clone());
            drop(all_trades_lock);

            if !trade_exists {
                self._process_trade(
                    trade,
                    metric,
                    pricing_options,
                    curr_portfolio.clone(),
                    curr_portfolio_sender,
                    curr_new_mkt,
                );
            }
        }
    }
}


impl PublishResults for RTRMLocal {

    fn metric(&self) -> PricingMetric {
        self.metric
    }
}
