//
//  Real time risk manager using local market & local pricing
//

use log::{debug, info, warn};
use std::sync::{Arc, Mutex,};
use std::sync::mpsc::Sender;
use kafka::consumer::Message;
use serde::Deserialize;

use crate::engine::CalcController;
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
};

use crate::streaming::Streaming;
use crate::trade::{LETFTrade, TradeTypes, TradeAggregation,};
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

impl CalcController for RTRMLocal {
    type TradeType = TradeTypes;
}

impl MarketSwitching for RTRMLocal {
    /// switch markets on the trade api.
    fn _switch_markets(&self) {
        info!("Switching markets: current <- new.");

        let nm = self.new_market.lock().expect("Could not lock new market.");
        **self.curr_market.lock().expect("Could not lock current market") = (*nm).clone();
    }

}

impl TradeAggregation for RTRMLocal {

    type TT = TradeTypes;

    fn all_trades(&self) -> Vec<Self::TT> {
        todo!()
    }

    fn aggregated_trades(&self) -> AggregatedTrades {
        todo!()
    }
}


impl BasicValue for RTRMLocal {

    fn metric(&self) -> PricingMetric {
        self.metric
    }

    /// pricing the trade locally
    fn _value_trade(&self, trade_id: String, market: CurrNewMarket, metric: PricingMetric) -> PricingResults {

        //let trade = self.all_trades();

        let amount = 100.;

        // TODO: THIS IS WRONG, BUT WE'll JUST GO ALONG
        let trade = LETFTrade {
            trade_id,
            stock: "AAPL".to_string(),
            amount,
            beta: 2.,
        };

        let stock_mkt_arc = match market {
            CurrNewMarket::Current => self.curr_market.lock(),
            CurrNewMarket::New => self.new_market.lock(),
        };
        let stock_mkt = stock_mkt_arc.expect("Could not lock the current market, weird");
        let stock_name = &trade.stock;
        let stock_value = stock_mkt.get(stock_name);
        if stock_value.is_none() {  // returns empty hedge if it cant determine the stock value.
            warn!("Can't find the value of stock {}", stock_name);
            // TODO: THIS IS NOT RIGHT, IT'S NOT FAIR
            match metric {
                PricingMetric::PV => {
                    return PricingResults::PV(PortfolioType::from([(stock_name.clone(), 0.),]));
                },
                PricingMetric::PV01 => {
                    //return PricingResults::PV01(PortfolioType::from([(stock_name.clone(), 0.),]));

                    // TODO: THIS IS WRONG
                    return PricingResults::PV01(PV01Results::new());
                },
            }
        }

        // we have stock value, dont need more
        match metric {
            PricingMetric::PV =>
                PricingResults::PV(PortfolioType::from([(stock_name.clone(), amount),])),
            PricingMetric::PV01 =>
                // TODO: THIS IS WRONG, FIX IT!!!!
                PricingResults::PV01(PV01Results::new()),
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
        debug!("Got quote: {:?}", new_market);
        if new_market.is_err() {
            return;  // ignore the market message if it cant be decoded correctly.
        }

        let Ok(new_mkt_real) = new_market else {
            warn!("New market !!!!");
            return;
        };

        info!("MKT PARAMS: {:?}", mkt_params);
        let MktMsgParams::LETFParams(new_quote_mkt) = mkt_params else {
            warn!("Obtained a weird market element");  // TODO: THIS HAS TO BE FIXED.
            return;
        };

        let mut curr_mkt_tmp = new_quote_mkt.curr_mkt.lock().unwrap();  // lock the current market
        for (new_quote, new_value) in new_mkt_real.iter() {
            curr_mkt_tmp.insert(new_quote.to_string(), *new_value);
        }
        //let _ = new_quote_mkt.new_mkt_sender.send(*curr_mkt_tmp);  // TODO: FIX THIS HERE!!!
        let _ = new_quote_mkt.new_mkt_sender.send(new_mkt_real);  // TODO: FIX THIS HERE!!!
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
