use std::fmt;
use serde::{Deserialize, Serialize};
use reqwest::blocking::{Client, Response,};

use crate::controller::CurrNewMarket;
use crate::portfolio::{
    PricingResults,
    PortfolioType,
    AggregatedTrades,
};


// which metric to compute
#[derive(Clone, Copy)]
pub enum PricingMetric {
    PV,
    PV01,
}

impl fmt::Display for PricingMetric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PricingMetric::PV => write!(f, "PV"),
            PricingMetric::PV01 => write!(f, "PV01"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PricingStruct {
    pub nb_sim: i32,
    pub default_price: f64,
}


pub trait Decoder {
    fn _unwrap_pricing_results(&self, result_price : Response, metric: PricingMetric) -> PricingResults;
}


///
/// trait for implementing the pricer for the trade.
pub trait RestPricer {
    fn _pricing_endpoint(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String;
    fn _trade_pricer(&self) -> String;
    fn _value_trade(&self, trade_id: u16, market: CurrNewMarket, metric: PricingMetric) -> PricingResults;
}

pub trait RestPricerSpark {
    fn _pricing_endpoint_spark(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String;
    fn trade_pricer(&self) -> String;
    fn _price_trades_on_spark(
        &self,
        agg_trades: &AggregatedTrades,
        pricing_client : &Client,
        market_ : CurrNewMarket,
        metric : PricingMetric,
    ) -> PortfolioType;

}


/// Trait that prices the portfolio of trades.
///
/// TODO: SHOULD BE THE EXTENSION TRAIT OF A FEW OTHER TRAITS.
pub trait PricePortfolioSequentially : RestPricer {

    fn _price_trades_sequentially(
        &self,
        agg_trades: &AggregatedTrades,
        market_ : CurrNewMarket,
        metric : PricingMetric,
    ) -> PortfolioType {

        let mut new_portfolio = PortfolioType::new();

        for (trade_id, trade_position) in agg_trades.iter() {
            new_portfolio += self._value_trade(*trade_id, market_, metric) * (*trade_position);
        }

        new_portfolio
    }
}

pub trait PricePortfolioSpark : RestPricerSpark + PricePortfolioSequentially {
    fn _price_trades(
        &self,
        agg_trades: &AggregatedTrades,
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType {

        let nb_trades = agg_trades.keys().len();
        let pricing_client = Client::new();

        if nb_trades > 30 {  // TODO: FACTOR THIS 30 out.
            return self._price_trades_on_spark(agg_trades, &pricing_client, market_, metric);
        }

        self._price_trades_sequentially(agg_trades, market_, metric)
    }

}
