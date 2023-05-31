use log::{debug, warn, info,};
use std::fmt;
use serde::{Deserialize, Serialize};
use reqwest::blocking::{Client, Response,};
use std::time::Instant;
use std::collections::HashMap;
use string_join::Join;

use crate::controller::CurrNewMarket;
use crate::portfolio::{
    PricingResults,
    PortfolioType,
    AggregatedTrades,
    PV01Results,
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


/// trait for implementing the pricer which comes from the REST service for the trade.
///
pub trait RestPricer : Decoder {
    // ip of the rest pricer, like localhost:5010
    fn _pricing_server(&self) -> String;
    // endpoint to use for pricing the trade (like pv_spark)
    fn _pricing_endpoint(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String;

    //fn _value_trade(&self, trade_id: u16, market: CurrNewMarket, metric: PricingMetric) -> PricingResults;
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
            Ok(result_price) => { return self._unwrap_pricing_results(result_price, metric); },
            Err(e) => {
                warn!("Trade {trade_id} could not price correctly: {}", e);
                match metric {
                    PricingMetric::PV => {return PricingResults::PV(PortfolioType::new())},
                    PricingMetric::PV01 => {return PricingResults::PV01(PV01Results::new())},
                }
            },
        }
    }

    /// agg_trades: aggregated trades, where keys are trade ids, and values are the positions of
    /// those trades.
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


pub trait RestPricerSpark : Decoder {
    // server used by spark to price trades, like localhost:5010
    fn _pricing_server_spark(&self) -> String;
    // endpoint on the pricing server, like "price_spark_new"
    fn _pricing_endpoint_spark(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String;

    // prices the trades on the spark
    fn _price_trades_on_spark(
        &self,
        agg_trades: &AggregatedTrades,
        pricing_client : &Client,
        market_ : CurrNewMarket,
        metric : PricingMetric,
    ) -> PortfolioType {

        // joins all trades with commas, like 190,191,192
        let all_trade_ids = ",".join(
            agg_trades
                .keys()
                .into_iter()
                .map(|trade_id: &u16| -> String {trade_id.to_string()} )
        );

        let pricing_endpoint_spark = self._pricing_endpoint_spark(market_, metric);
        let market_endpoint = pricing_endpoint_spark.as_str();
        let result_pricing_start = Instant::now();
        let result_pricing = pricing_client
            .post(format!("http://{}/{}", self._pricing_server_spark(), market_endpoint))
            .form(&HashMap::from([("trades", &all_trade_ids)]))
            .send();
        info!("SPARK pricing took: {:?}", result_pricing_start.elapsed().as_secs_f32());

        // unwrap the result_pricing
        let mut priced_portfolio = match result_pricing {
            Ok(result_price) => self._unwrap_pricing_results(result_price, metric),
            Err(e) => {
                warn!("Trades could not price correctly: {}", e);
                return PortfolioType::new() // TODO: What to do if the trade cant convert
            },
        };

        priced_portfolio *= agg_trades;  // fix the priced portfolio by the weights, aggregated trades.

        // let's do the aggregation here.
        match priced_portfolio {
            PricingResults::PV(portfolio) => portfolio,
            PricingResults::PV01(pv01_results) => pv01_results.aggregate()
        }
    }
}


pub trait PricePortfolioSpark {
    fn _price_trades(
        &self,
        agg_trades: &AggregatedTrades,
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType;

}

// for every type that implements RestPricer & RestPricerSpark implement this as well.
impl<T:RestPricerSpark + RestPricer> PricePortfolioSpark for T {
    fn _price_trades(
        &self,
        agg_trades: &AggregatedTrades,
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType {

        let nb_trades = agg_trades.keys().len();

        if nb_trades > 30 {  // TODO: FACTOR THIS 30 out.
            return self._price_trades_on_spark(agg_trades, &Client::new(), market_, metric);
        }

        self._price_trades_sequentially(agg_trades, market_, metric)
    }
}
