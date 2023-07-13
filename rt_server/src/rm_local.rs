//
//  Real time risk manager using local market & local pricing
//

use log::{debug, info, warn};
use std::sync::{Arc, Mutex,};
use std::sync::mpsc::Sender;
use kafka::consumer::Message;
use serde::Deserialize;

use crate::portfolio::{
    PortfolioType,
    PricingResults,
};

use crate::market::{
    MarketType,
    MktMsgParams,
    CurrNewMarket,
};
use crate::mkt_handler::MktEventHandler;
use crate::ref_deref::TryFromRef;

use crate::pricer::{
    PricingMetric,
    PriceMultipleTrades,
    BasicValue,
    PriceTrade,
};

use crate::streaming::Streaming;
use crate::trade::{TradeTypes, BaseTrade, };
use crate::trade_processor::MarketSwitching;
use crate::publish::PublishResults;


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


impl BasicValue<TradeTypes> for RTRMLocal {

    fn metric(&self) -> PricingMetric {
        self.metric
    }

    /// pricing the trade locally
    fn _value_trade(&self, trade: &TradeTypes, market: CurrNewMarket, metric: PricingMetric) -> PricingResults {

        debug!("_value_trade: Valuing {:?}", trade);
	debug!("_value_trade: Curr mkt = {:?}", self._curr_mkt().lock().unwrap());
        let trade_name = trade.id();
	let new_mkt_l = self._new_mkt();
	let new_stock_mkt = new_mkt_l.lock();
	let curr_mkt_l = self._curr_mkt();
	let curr_stock_mkt = curr_mkt_l.lock();
	
	let stock_mkt_arc;
	
	if market == CurrNewMarket::Current {
	   stock_mkt_arc = curr_stock_mkt;
	} else {
            stock_mkt_arc = new_stock_mkt;
        };

	let stock_mkt = stock_mkt_arc.expect("_value_trade: Could not lock the stock market object, weird");

        match metric {
            PricingMetric::PV => {
                let priced_trade = trade.price(&stock_mkt);
                debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade),]))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
            },

            PricingMetric::PV01 => {
		let trade_pv01 = trade.pv01(&stock_mkt);
		debug!("_value_trade: PV01 of {:?} = {:?}", trade_name, trade_pv01);
                PricingResults::PV01(trade_pv01)
            },
	    PricingMetric::PnL => {
                let pnl_trade = trade.pnl(&stock_mkt);
                debug!("_value_trade: PnL of {:?} = {:?}", trade_name, pnl_trade);
                if let Some(pnl_trade_real) = pnl_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, pnl_trade_real),]))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
	    }
        }
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

/// implements the pricing of multiple trades for every type T that implements _value_trade
/// iterates over the trades.
impl PriceMultipleTrades<TradeTypes> for RTRMLocal {
    /// price multiple trades
    fn _price_trades(
        &self,
	trades: &[&TradeTypes],
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType {
        // iterate of the
        let mut new_portfolio = PortfolioType::new();

        for trade in trades {
            new_portfolio += self._value_trade(trade, market_, metric);
        }

        new_portfolio
    }
}


impl PublishResults for RTRMLocal {

    fn metric(&self) -> PricingMetric {
        self.metric
    }
}
