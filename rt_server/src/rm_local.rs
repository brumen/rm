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
    AggregatedTrades,
    PricingResults,
    PV01Results,
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
use crate::trade::{TradeTypes, TradeAggregation,};
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
    _all_trades: Arc<Mutex<Vec<TradeTypes>>>,
    _aggregated_trades: Arc<Mutex<AggregatedTrades>>,
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
            _all_trades: Arc::new(Mutex::new(Vec::<TradeTypes>::new())),
            _aggregated_trades: Arc::new(Mutex::new(AggregatedTrades::new())),
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(
        config_file: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file).unwrap();
        let config_map: RTRMConfig = serde_yaml::from_reader(config_f).unwrap();

        let controller_metric = if config_map.metric == *"PV" {
            PricingMetric::PV
        } else {
            PricingMetric::PV01
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
    }

}

impl TradeAggregation for RTRMLocal {

    type TT = TradeTypes;

    fn all_trades(&self) -> Vec<Self::TT> {
        // TODO: IDK IF THIS IS RIGHT????
        // TODO: SHITTIEST WORK EVER
        let mut new_trades = Vec::<Self::TT>::new();
        for v in &*self._all_trades.lock().unwrap() {
            new_trades.push(v.clone());
        }

        new_trades

    }

    /// constructs aggregated trades from all_trades.
    /// TODO: THIS CANT BE CONSTRUCTED EVERY TIME AGAIN!!! FIX IT!!!
    fn aggregated_trades(&self) -> AggregatedTrades {

        let mut new_agg_trades = AggregatedTrades::new();
        for trade in &*self._all_trades.lock().unwrap() {
            new_agg_trades += trade.clone(); // TODO: TOO MUCH CLONING
        }

        new_agg_trades
    }

    fn add_trade_mut(&self, trade: Self::TT) {
        let all_trades = &mut *self._all_trades.lock().unwrap();
        all_trades.push(trade);
    }

}


impl BasicValue for RTRMLocal {

    fn metric(&self) -> PricingMetric {
        self.metric
    }

    /// pricing the trade locally
    fn _value_trade(&self, trade_id: String, market: CurrNewMarket, metric: PricingMetric) -> PricingResults {

        let trade = self.find_trade(trade_id);

        debug!("_value_trade: Valuing {:?}", trade);
        if trade.is_none() {
            match metric {
                PricingMetric::PV => {
                    return PricingResults::PV(PortfolioType::new());
                },
                PricingMetric::PV01 => {
                // TODO: THIS IS WRONG, FIX IT!!!!
                    return PricingResults::PV01(PV01Results::new());
                }
            }
        }

        let actual_trade = trade.unwrap();

        let trade_name = actual_trade.trade_name();
        debug!("_value trade: Trade name {:?}", trade_name);

        let stock_mkt_arc = match market {
            CurrNewMarket::Current => self.curr_market.lock(),
            CurrNewMarket::New => self.new_market.lock(),
        };
        let stock_mkt = stock_mkt_arc.expect("Could not lock the current market, weird");

        let stock_value = actual_trade.price(&stock_mkt);
        debug!("_value_trade: Stock value {:?}", stock_value);

        match metric {
            PricingMetric::PV => {
                let priced_trade = actual_trade.price(&stock_mkt);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(actual_trade.trade_name(), price_trade),]))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
            },
            PricingMetric::PV01 => PricingResults::PV01(actual_trade.pv01(&stock_mkt)),
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
        mkt_params: MktMsgParams,
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
        debug!("_handle_mkt_msg: New real market obtained: {:?}", new_mkt_real);
        debug!("_handle_mkt_msg: Mkt params: {:?}", mkt_params);
        let MktMsgParams::LETFParams(new_quote_mkt) = mkt_params else {
            warn!("_handle_mkt_msg: Obtained a weird market element");  // TODO: THIS HAS TO BE FIXED.
            return;
        };

        debug!("_handle_mkt_msg: New quote is {:?}", new_quote_mkt);
        let mut curr_mkt_tmp = new_quote_mkt.curr_mkt.lock().unwrap();  // lock the current market
        for (new_quote, new_value) in new_mkt_real.iter() {
            curr_mkt_tmp.insert(new_quote.to_string(), *new_value);
        }
        debug!("_handle_mkt_msg: Final market {:?}", curr_mkt_tmp);
        //let _ = new_quote_mkt.new_mkt_sender.send(*curr_mkt_tmp);  // TODO: FIX THIS HERE!!!
        let _ = new_mkt_sender.send(new_mkt_real);  // TODO: FIX THIS HERE!!!
    }
}

/// implements the pricing of multiple trades for every type T that implements _value_trade
/// iterates over the trades.
impl PriceMultipleTrades for RTRMLocal {
    /// price multiple trades
    fn _price_trades(
        &self,
        agg_trades: &AggregatedTrades,
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType {
        // iterate of the
        let mut new_portfolio = PortfolioType::new();

        for (trade_id, trade_position) in agg_trades.iter() {
            new_portfolio += self._value_trade(trade_id.clone(), market_, metric) * (*trade_position);  // TODO: WITHOUT CLONING
        }

        new_portfolio
    }
}


impl PublishResults for RTRMLocal {

    fn metric(&self) -> PricingMetric {
        self.metric
    }
}
