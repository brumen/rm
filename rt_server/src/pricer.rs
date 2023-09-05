use log::{warn, info, debug,};
use std::fmt;
use serde::{Deserialize, Serialize};
//use reqwest::blocking::{Client, Response,};
use reqwest;
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
    PnL,
}

impl fmt::Display for PricingMetric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PricingMetric::PV => write!(f, "PV"),
            PricingMetric::PV01 => write!(f, "PV01"),
	        PricingMetric::PnL => write!(f, "PnL"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PricingStruct {
    pub nb_sim: i32,
    pub default_price: f64,
}


pub trait Decoder {

    fn _unwrap_pricing_results(
        &self,
        result_price : reqwest::blocking::Response,
        metric: PricingMetric,
    ) -> PricingResults;

    async fn _unwrap_pricing_results_a(
        &self,
        results_price: reqwest::Response,
        metric: PricingMetric,
    ) -> PricingResults;
}


pub trait PriceTrade : BaseTrade {

    fn initial_pv(&self) -> Option<f64>;
    fn price(&self, market: &MarketType) -> Option<f64>;
    fn pv01(&self, market: &MarketType) -> PV01Results;

    fn pnl(&self, market: &MarketType) -> Option<f64> {
	    match self.initial_pv() {
	        None => None,
	        Some(initial_pv_val) =>
                self.price(market).map(|curr_price| curr_price - initial_pv_val)
        }
    }

    /// values the trade for a specific metric.
    fn value_by_metric(
        &self,
        metric: PricingMetric,
        market: &MarketType
    ) -> PricingResults {

        let trade_name = self.id();

        match metric {
            PricingMetric::PV => {
                let priced_trade = self.price(&market);
                debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade),]))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
            },

            PricingMetric::PV01 => {
		        let trade_pv01 = self.pv01(&market);
		        debug!("_value_trade: PV01 of {:?} = {:?}", trade_name, trade_pv01);
                PricingResults::PV01(trade_pv01)
            },

            PricingMetric::PnL => {
                let pnl_trade = self.pnl(&market);
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


pub struct MarketPricingOptions {
    pub pricing_server: String,
    pub pricing_endpoint: String,
}

/// ASynchronous version of the pricer. Used for REST pricer.
pub trait PriceTradeAsync : BaseTrade {

    fn _endpoint(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> String {

        let pricing_server = pricing_options.pricing_server.clone();  // TODO: THIS IS SHIT HERE!!
        let pricing_endpoint = pricing_options.pricing_endpoint.clone();  // TODO: SHIT HERE AGAIN!!!

        format!(
            "http://{}/{}/{}/{}",
            pricing_server,
            pricing_endpoint,
            metric,
            self.id(),
        )
    }

    /// computes the pricing request.
    async fn _pricing_request(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> Result<reqwest::Response, reqwest::Error> {

        reqwest::get(
            self._endpoint(metric, pricing_options)
        ).await
    }

    async fn initial_pv(&self) -> Option<f64>;
    async fn price(&self, pricing_options: &MarketPricingOptions) -> Option<f64>;
    async fn pv01(&self, pricing_options: &MarketPricingOptions) -> PV01Results;

    async fn pnl(&self, pricing_options: &MarketPricingOptions) -> Option<f64> {
	    match self.initial_pv().await {
	        None => None,
	        Some(initial_pv_val) =>
                self.price(pricing_options).await.map(|curr_price| curr_price - initial_pv_val)
        }
    }

    /// values the trade for a specific metric.
    async fn value_by_metric(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> PricingResults {

        let trade_name = self.id();

        match metric {
            PricingMetric::PV => {
                let priced_trade = self.price(pricing_options).await;
                debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade),]))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
            },

            PricingMetric::PV01 => {
		        let trade_pv01 = self.pv01(pricing_options).await;
		        debug!("_value_trade: PV01 of {:?} = {:?}", trade_name, trade_pv01);
                PricingResults::PV01(trade_pv01)
            },

            PricingMetric::PnL => {
                let pnl_trade = self.pnl(pricing_options).await;
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


// pub trait RestPricerSpark<TT> : Decoder
// where
//     TT: BaseTrade
// {
//     // server used by spark to price trades, like localhost:5010
//     fn _pricing_server_spark(&self) -> String;
//     // endpoint on the pricing server, like "price_spark_new"
//     fn _pricing_endpoint_spark(&self, market_ : CurrNewMarket, metric: PricingMetric) -> String;

//     // prices the trades on the spark
//     fn _price_trades_on_spark(
//         &self,
// 	    trades: &[&TT],
// 	    pricing_client : &reqwest::blocking::Client,
//         market_ : CurrNewMarket,
//         metric : PricingMetric,
//     ) -> PortfolioType {

//         // joins all trades with commas, like 190,191,192
//         let all_trade_ids = ",".join(
//             trades
//                 .iter()
//                 .map(|trade| {trade.id()} )
//         );

//         let pricing_endpoint_spark = self._pricing_endpoint_spark(market_, metric);
//         let market_endpoint = pricing_endpoint_spark.as_str();
//         let result_pricing_start = Instant::now();
//         let result_pricing = pricing_client
//             .post(format!("http://{}/{}", self._pricing_server_spark(), market_endpoint))
//             .form(&HashMap::from([("trades", &all_trade_ids)]))
//             .send();
//         info!("_price_trades_on_spark: SPARK pricing took: {:?}", result_pricing_start.elapsed().as_secs_f32());

//         // unwrap the result_pricing
//         let priced_portfolio = match result_pricing {
//             Ok(result_price) => self._unwrap_pricing_results(result_price, metric),
//             Err(e) => {
//                 warn!("Trades could not price correctly: {}", e);
//                 return PortfolioType::new() // TODO: What to do if the trade cant convert
//             },
//         };

//         // let's do the aggregation here.  TODO: CHECK IF THIS IS NECESSARY
//         match priced_portfolio {
//             PricingResults::PV(pv_portfolio) => pv_portfolio,
//             PricingResults::PV01(pv01_results) => pv01_results.aggregate(),
// 	        PricingResults::PnL(pnl_portfolio) => pnl_portfolio,
//         }
//     }
// }


// pub trait PriceMultipleTrades<TT> {
//     fn _price_trades(
//         &self,
// 	    trades: &[&TT],
//         market_ : CurrNewMarket,
//         metric: PricingMetric,
//     ) -> PortfolioType;
// }

// // for every type that implements RestPricer & RestPricerSpark implement this as well.
// impl<TT, T> PriceMultipleTrades<TT> for T
// where
//     T: RestPricerSpark<TT> + RestPricer<TT>,
//     TT: BaseTrade
// {
//     fn _price_trades(
//         &self,
// 	    trades: &[&TT],
//         market_ : CurrNewMarket,
//         metric: PricingMetric,
//     ) -> PortfolioType {

//         let nb_trades = trades.len();

//         if nb_trades > 30 {  // TODO: FACTOR THIS 30 out.
//             return self._price_trades_on_spark(
//                 trades,
//                 &reqwest::blocking::Client::new(),
//                 market_,
//                 metric,
//             );
//         }

//         self._price_trades_sequentially(trades, market_, metric)
//     }
// }
