use reqwest::{self, Error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use string_join::Join;
use tracing::{debug, warn};

use crate::market::{CurrNewMarket, MarketType};
use crate::portfolio::{PV01Results, PortfolioType, PricingResults};
use crate::trade::{BaseTrade, TradeRep};

// which metric to compute
#[derive(Debug, Clone, Copy)]
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
    /// unwraps the pricing results
    fn _unwrap_pricing_results(
        &self,
        result_price: reqwest::blocking::Response,
        metric: PricingMetric,
    ) -> PricingResults {
        match metric {
            PricingMetric::PV => {
                let results_conv = result_price.json::<HashMap<String, f64>>();

                if results_conv.is_err() {
                    return PricingResults::PV(PortfolioType::new());
                }

                PricingResults::PV(PortfolioType(results_conv.unwrap()))
            }

            PricingMetric::PV01 => {
                let results_conv = result_price.json::<HashMap<String, HashMap<String, f64>>>();

                if results_conv.is_err() {
                    return PricingResults::PV01(PV01Results::new());
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert(
                        (*trade_id.clone()).to_string(),
                        PortfolioType::from(trade_result),
                    );
                }
                PricingResults::PV01(pv01)
            }

            PricingMetric::PnL => todo!(),
        }
    }

    fn _unwrap_pricing_results_a(
        &self,
        result_price: reqwest::Response,
        metric: PricingMetric,
    ) -> impl Future<Output = PricingResults> + Send {
        async move {
            match metric {
                PricingMetric::PV => {
                    let results_conv = result_price.json::<HashMap<String, f64>>().await;

                    if results_conv.is_err() {
                        return PricingResults::PV(PortfolioType::new());
                    }

                    PricingResults::PV(PortfolioType(results_conv.unwrap()))
                }

                PricingMetric::PV01 => {
                    let results_conv = result_price
                        .json::<HashMap<String, HashMap<String, f64>>>()
                        .await;

                    if results_conv.is_err() {
                        return PricingResults::PV01(PV01Results::new());
                    }

                    let mut pv01 = PV01Results::new();
                    for (trade_id, trade_result) in results_conv.unwrap().iter() {
                        let _ = pv01.insert(
                            (*trade_id.clone()).to_string(),
                            PortfolioType::from(trade_result),
                        );
                    }
                    PricingResults::PV01(pv01)
                }

                PricingMetric::PnL => todo!(),
            }
        }
    }
}

pub trait PriceTrade: BaseTrade {
    fn initial_pv(&self) -> Option<f64>;
    fn price(&self, market: &MarketType) -> Option<f64>;
    fn pv01(&self, market: &MarketType) -> PV01Results;

    fn pnl(&self, market: &MarketType) -> Option<f64> {
        match self.initial_pv() {
            None => None,
            Some(initial_pv_val) => self
                .price(market)
                .map(|curr_price| curr_price - initial_pv_val),
        }
    }

    /// values the trade for a specific metric.
    fn value_by_metric(&self, metric: PricingMetric, market: &MarketType) -> PricingResults {
        let trade_name = self.id();

        match metric {
            PricingMetric::PV => {
                let priced_trade = self.price(&market);
                debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade)]))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
            }

            PricingMetric::PV01 => {
                let trade_pv01 = self.pv01(&market);
                debug!("_value_trade: PV01 of {:?} = {:?}", trade_name, trade_pv01);
                PricingResults::PV01(trade_pv01)
            }

            PricingMetric::PnL => {
                let pnl_trade = self.pnl(&market);
                debug!("_value_trade: PnL of {:?} = {:?}", trade_name, pnl_trade);
                if let Some(pnl_trade_real) = pnl_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, pnl_trade_real)]))
                } else {
                    PricingResults::PV(PortfolioType::new())
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct MarketPricingOptions {
    pub pricing_server: String,
    pub pricing_endpoint: String,
}

/// ASynchronous version of the pricer. Used for REST pricer.
pub trait PriceTradeAsync: BaseTrade {
    fn _endpoint(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> String {
        let pricing_request = match curr_new_mkt {
            CurrNewMarket::Current => format!(
                "http://{}/{}/{}",
                &pricing_options.pricing_server,
                metric.to_string().to_lowercase(),
                self.id(),
            ),
            CurrNewMarket::New => format!(
                "http://{}/{}/new/{}",
                &pricing_options.pricing_server,
                metric.to_string().to_lowercase(),
                self.id(),
            ),
        };

        debug!("_endpoint: {:?}", pricing_request);

        pricing_request
    }

    /// computes the pricing request.
    fn _pricing_request(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl Future<Output = Result<reqwest::Response, reqwest::Error>> + Send
    where
        Self: Sync,
    {
        async move { reqwest::get(self._endpoint(metric, pricing_options, curr_new_mkt)).await }
    }

    fn initial_pv(&self) -> impl Future<Output = Option<f64>> + Send;

    fn price(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl Future<Output = Option<f64>> + Send;

    fn pv01(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl Future<Output = PV01Results> + Send;

    fn pnl(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl Future<Output = Option<f64>> + Send
    where
        Self: Sync,
    {
        async move {
            match self.initial_pv().await {
                None => None,
                Some(initial_pv_val) => self
                    .price(pricing_options, curr_new_mkt)
                    .await
                    .map(|curr_price| curr_price - initial_pv_val),
            }
        }
    }

    /// values the trade for a specific metric.
    fn value_by_metric(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl Future<Output = PricingResults> + Send
    where
        Self: Sync,
    {
        async move {
            let trade_name = self.id();

            match metric {
                PricingMetric::PV => {
                    let priced_trade = self.price(pricing_options, curr_new_mkt).await;
                    debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                    if let Some(price_trade) = priced_trade {
                        PricingResults::PV(PortfolioType::from([(trade_name, price_trade)]))
                    } else {
                        PricingResults::PV(PortfolioType::new())
                    }
                }
                PricingMetric::PV01 => {
                    let trade_pv01 = self.pv01(pricing_options, curr_new_mkt).await;
                    debug!("_value_trade: PV01 of {:?} = {:?}", trade_name, trade_pv01);
                    PricingResults::PV01(trade_pv01)
                }

                PricingMetric::PnL => {
                    let pnl_trade = self.pnl(pricing_options, curr_new_mkt).await;
                    debug!("_value_trade: PnL of {:?} = {:?}", trade_name, pnl_trade);
                    if let Some(pnl_trade_real) = pnl_trade {
                        PricingResults::PV(PortfolioType::from([(trade_name, pnl_trade_real)]))
                    } else {
                        PricingResults::PV(PortfolioType::new())
                    }
                }
            }
        }
    }
}

/// pricing trades on spark
pub trait RestPricerSpark<TR>: Decoder
where
    TR: PartialEq + Clone + BaseTrade,
{
    // server used by spark to price trades, like localhost:5010
    fn _pricing_server_spark(&self) -> String;

    // endpoint on the pricing server, like "price_spark_new"
    fn _pricing_endpoint_spark(&self, market_: CurrNewMarket, metric: PricingMetric) -> String;

    // prices the trades on the spark
    fn price_trades_spark(
        &self,
        trades: &TradeRep<TR>,
        pricing_client: &reqwest::blocking::Client,
        market_: CurrNewMarket,
        metric: PricingMetric,
    ) -> PortfolioType {
        // joins all trades with commas, like 190,191,192
        let all_trade_ids = ",".join(trades.all_trade_names());
        let pricing_endpoint_spark = self._pricing_endpoint_spark(market_, metric);
        let market_endpoint = pricing_endpoint_spark.as_str();
        let result_pricing = pricing_client
            .post(format!(
                "http://{}/{}",
                self._pricing_server_spark(),
                market_endpoint
            ))
            .form(&HashMap::from([("trades", &all_trade_ids)]))
            .send();

        // unwrap the result_pricing
        let priced_portfolio = match result_pricing {
            Ok(result_price) => self._unwrap_pricing_results(result_price, metric),
            Err(e) => {
                warn!("Trades could not price correctly: {}", e);
                return PortfolioType::new(); // TODO: What to do if the trade cant convert
            }
        };

        // let's do the aggregation here.  TODO: CHECK IF THIS IS NECESSARY
        match priced_portfolio {
            PricingResults::PV(pv_portfolio) => pv_portfolio,
            PricingResults::PV01(pv01_results) => pv01_results.aggregate(),
            PricingResults::PnL(pnl_portfolio) => pnl_portfolio,
        }
    }

    /// similar to price_trades_spark,
    ///   just that it only limits to spark application to
    ///   a certain number of trades, and it repeats the spark
    ///   application
    fn price_trades_on_spark(
        &self,
        trades: &TradeRep<TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType
//where TR: PartialEq + std::fmt::Debug + Clone + pricer::PriceTradeAsync + Send + Sync
    {
        let mut curr_portfolio = PortfolioType::new();

        let split_nb = 20; // TODO: FACTOR THIS NUMBER OUT

        let mut curr_trade_nb = 0;
        let mut curr_trade_rep = TradeRep::<TR>::new();

        let pricing_client = reqwest::blocking::Client::new();

        for (_tid, tr) in trades.iter() {
            curr_trade_rep += tr;
            curr_trade_nb += 1;

            if curr_trade_nb > split_nb {
                // do the computation
                let portfolio =
                    self.price_trades_spark(&curr_trade_rep, &pricing_client, curr_new_mkt, metric);

                curr_portfolio += portfolio;
                curr_trade_nb = 0;
                curr_trade_rep = TradeRep::<TR>::new();
            }
        }

        // remaining part of trades
        curr_portfolio +=
            self.price_trades_spark(&curr_trade_rep, &pricing_client, curr_new_mkt, metric);

        curr_portfolio
    }

    // prices the trades on the spark
    fn price_trades_spark_2(
        &self,
        trades: &TradeRep<TR>,
        pricing_client: &reqwest::blocking::Client,
        market_: CurrNewMarket,
        metric: PricingMetric,
    ) -> Result<PortfolioType, Error> {
        // joins all trades with commas, like 190,191,192
        let all_trade_ids = ",".join(trades.all_trade_names());
        let pricing_endpoint_spark = self._pricing_endpoint_spark(market_, metric);
        let market_endpoint = pricing_endpoint_spark.as_str();
        let result_pricing = pricing_client
            .post(format!(
                "http://{}/{}",
                self._pricing_server_spark(),
                market_endpoint
            ))
            .form(&HashMap::from([("trades", &all_trade_ids)]))
            .send()?;

        // unwrap the result_pricing
        let priced_portfolio = self._unwrap_pricing_results(result_pricing, metric);

        // let's do the aggregation here
        Ok(match priced_portfolio {
            PricingResults::PV(pv_portfolio) => pv_portfolio,
            PricingResults::PV01(pv01_results) => pv01_results.aggregate(),
            PricingResults::PnL(pnl_portfolio) => pnl_portfolio,
        })
    }
}
