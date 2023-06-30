use log::{warn, info,};
use std::fmt;
use serde::{Deserialize, Serialize};
use reqwest::blocking::{Client, Response,};
use std::time::Instant;
use std::collections::HashMap;
use string_join::Join;

use crate::market::CurrNewMarket;
use crate::portfolio::{
    PricingResults,
    PortfolioType,
    PV01Results,
};
use crate::trade::BaseTrade;
use crate::market::MarketType;


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


pub trait PriceTrade {
    fn price(&self, market: &MarketType) -> Option<f64>;
    fn pv01(&self, market: &MarketType) -> PV01Results;
}


pub trait BasicValue<TT>
{
    fn metric(&self) -> PricingMetric;
    fn _value_trade(&self, trade: &TT, market: CurrNewMarket, metric: PricingMetric) -> PricingResults;
}


/// trait for implementing the pricer which comes from the REST service for the trade.
///
pub trait RestPricer<TT> : Decoder + BasicValue<TT> {
    // ip of the rest pricer, like localhost:5010
    fn _pricing_server(&self) -> String;
    // endpoint to use for pricing the trade (like pv_spark)
    fn _pricing_endpoint(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String;

    /// agg_trades: aggregated trades, where keys are trade ids, and values are the positions of
    /// those trades.
    fn _price_trades_sequentially(
        &self,
	trades: &[&TT],
        market_ : CurrNewMarket,
        metric : PricingMetric,
    ) -> PortfolioType {

        let mut new_portfolio = PortfolioType::new();

        for trade in trades {
            new_portfolio += self._value_trade(trade, market_, metric);
        }

        new_portfolio
    }

}


pub trait RestPricerSpark<TT> : Decoder
where
    TT: BaseTrade
{
    // server used by spark to price trades, like localhost:5010
    fn _pricing_server_spark(&self) -> String;
    // endpoint on the pricing server, like "price_spark_new"
    fn _pricing_endpoint_spark(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String;

    // prices the trades on the spark
    fn _price_trades_on_spark(
        &self,
	    trades: &[&TT],
	    pricing_client : &Client,
        market_ : CurrNewMarket,
        metric : PricingMetric,
    ) -> PortfolioType {

        // joins all trades with commas, like 190,191,192
        let all_trade_ids = ",".join(
            trades
                .iter()
                .map(|trade| {trade.id()} )
        );

        let pricing_endpoint_spark = self._pricing_endpoint_spark(market_, metric);
        let market_endpoint = pricing_endpoint_spark.as_str();
        let result_pricing_start = Instant::now();
        let result_pricing = pricing_client
            .post(format!("http://{}/{}", self._pricing_server_spark(), market_endpoint))
            .form(&HashMap::from([("trades", &all_trade_ids)]))
            .send();
        info!("_price_trades_on_spark: SPARK pricing took: {:?}", result_pricing_start.elapsed().as_secs_f32());

        // unwrap the result_pricing
        let priced_portfolio = match result_pricing {
            Ok(result_price) => self._unwrap_pricing_results(result_price, metric),
            Err(e) => {
                warn!("Trades could not price correctly: {}", e);
                return PortfolioType::new() // TODO: What to do if the trade cant convert
            },
        };

        // let's do the aggregation here.  TODO: CHECK IF THIS IS NECESSARY
        match priced_portfolio {
            PricingResults::PV(portfolio) => portfolio,
            PricingResults::PV01(pv01_results) => pv01_results.aggregate()
        }
    }
}


pub trait PriceMultipleTrades<TT> {
    fn _price_trades(
        &self,
	trades: &[&TT],
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType;
}

// for every type that implements RestPricer & RestPricerSpark implement this as well.
impl<TT, T> PriceMultipleTrades<TT> for T
where
    T: RestPricerSpark<TT> + RestPricer<TT>,
    TT: BaseTrade
{
    fn _price_trades(
        &self,
	trades: &[&TT],
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType {

        let nb_trades = trades.len();

        if nb_trades > 30 {  // TODO: FACTOR THIS 30 out.
            return self._price_trades_on_spark(trades, &Client::new(), market_, metric);
        }

        self._price_trades_sequentially(trades, market_, metric)
    }
}
