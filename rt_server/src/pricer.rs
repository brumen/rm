use reqwest::{self, Error, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use string_join::Join;
use tracing::{debug, info, warn, error};
use ractor::async_trait;

use crate::market::{MarketTypeT};
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


// Decoder decodes the pricing results from a trade and
#[async_trait]
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
                    return PricingResults::PV(PortfolioType::default());
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

    async fn _unwrap_pricing_results_a(
        &self,
        result_price: reqwest::Response,
        metric: PricingMetric,
    ) -> PricingResults  {
        match metric {
            PricingMetric::PV => {
                let results_conv = result_price.json::<HashMap<String, f64>>().await;

                if results_conv.is_err() {
                    return PricingResults::PV(PortfolioType::default());
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


// MP are market parameters, () if none.
// MT is market type, depending on the market parameters.
pub trait PriceTrade<MP>: BaseTrade {

    async fn initial_pv(&self) -> Option<f64>;
    async fn price(&self, market: &dyn MarketTypeT<MP=MP>) -> Option<f64>;
    async fn pv01(&self, market: &dyn MarketTypeT<MP=MP>) -> PV01Results;
    async fn pnl(&self, market: &dyn MarketTypeT<MP=MP>) -> Option<f64> {
        let initial_pv_val = self.initial_pv().await?;

        self.price(market)
            .await
            .map(|curr_price| curr_price - initial_pv_val)
    }

    /// values the trade for a specific metric.
    #[allow(dead_code)]
    async fn value_by_metric(
        &self, metric: PricingMetric,
        market: &dyn MarketTypeT<MP=MP>
    ) -> PricingResults {

        let trade_name = self.id();

        match metric {
            PricingMetric::PV => {
                let priced_trade = self.price(market).await;
                debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade)]))
                } else {
                    PricingResults::PV(PortfolioType::default())
                }
            }

            PricingMetric::PV01 => {
                let trade_pv01 = self.pv01(market).await;
                debug!("_value_trade: PV01 of {:?} = {:?}", trade_name, trade_pv01);
                PricingResults::PV01(trade_pv01)
            }

            PricingMetric::PnL => {
                let pnl_trade = self.pnl(market).await;
                debug!("_value_trade: PnL of {:?} = {:?}", trade_name, pnl_trade);
                if let Some(pnl_trade_real) = pnl_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, pnl_trade_real)]))
                } else {
                    PricingResults::PV(PortfolioType::default())
                }
            }
        }
    }
}


#[derive(Debug, Clone)]
pub struct MarketPricingOptions {
    pub pricing_server: String,
    pub pricing_endpoint: String,
    pub market_server: String,
    pub market_endpoint: String
}


/// ASynchronous version of the pricer. Used for REST pricer.
#[async_trait]
pub trait PriceTradeAsync: BaseTrade {
    // fn _endpoint(
    //     &self,
    //     metric: PricingMetric,
    //     pricing_options: &MarketPricingOptions,
    //     curr_new_mkt: &MarketType,
    // ) -> String {
    //     let pricing_server = pricing_options.pricing_server.clone();
    //     let metric = metric.to_string();
    //     let trades = self.id();

    //     let _endpoint = format!(
    //         "http://{pricing_server}/pricing?metric={metric}&market={curr_new_mkt}&trade_ids={trades}");

    //     debug!("_endpoint: {:?}", _endpoint);

    //     _endpoint
    // }

    /// computes the pricing request.
    async fn _pricing_request(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> Result<reqwest::Response, reqwest::Error> {
	reqwest::get(self._endpoint(metric, pricing_options, curr_new_mkt)).await
    }

    fn initial_pv(&self) -> impl Future<Output = Option<f64>> + Send;

    fn price(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> impl Future<Output = Option<f64>> + Send;

    fn pv01(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> impl Future<Output = PV01Results> + Send;

    async fn pnl(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> Option<f64> {
        match self.initial_pv().await {
            None => None,
            Some(initial_pv_val) => self
                .price(pricing_options, &curr_new_mkt)
                .await
                .map(|curr_price| curr_price - initial_pv_val),
        }
    }

    /// values the trade for a specific metric.
    async fn value_by_metric(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> PricingResults {
        let trade_name = self.id();

        match metric {
            PricingMetric::PV => {
                let priced_trade = self.price(pricing_options, curr_new_mkt).await;
                debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade)]))
                } else {
                    PricingResults::PV(PortfolioType::default())
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
                    PricingResults::PV(PortfolioType::default())
                }
            }
        }
    }
}

/// pricing trades on spark
#[async_trait]
pub trait RestPricerSpark<TR>: Decoder
where
    TR: PartialEq + Clone + BaseTrade + Send + Sync,
    Self: Sync,
{
    // server used by spark to price trades, like localhost:5010
    fn _pricing_server_spark(&self) -> String;

    /// endpoint on the pricing server, like "price_spark_new", or "pv/", "pv/spark/",
    ///   what you would normally attach to the server. so the complete enpoint would
    ///   be localhost:5010/pv/spark
    fn _pricing_endpoint_spark(&self, market_: MarketType, metric: PricingMetric) -> String;

    /// prices the trades on the spark
    async fn price_trades_spark(
        &self,
        trades: &TradeRep<TR>,
        pricing_client: &Client,
        market_: MarketType,
        metric: PricingMetric,
    ) -> Result<PortfolioType, Error> {
        // joins all trades with commas, like 190,191,192
        let all_trade_ids = ",".join(trades.all_trade_names());
        let pricing_endpoint_spark = self._pricing_endpoint_spark(market_.clone(), metric);  // TODO: MAYBE HERE .clone could be omitted
        let market_endpoint = pricing_endpoint_spark.as_str();

	let client_endpoint = format!(
            "http://{}/{}",
            self._pricing_server_spark(),
            market_endpoint,
        );
	let metric_s = match metric {
	    PricingMetric::PV => "PV".to_string(),
	    PricingMetric::PV01 => "PV01".to_string(),
	    PricingMetric::PnL => "PnL".to_string(),
	};

        let result_pricing = pricing_client
            .post(client_endpoint)
            .form(&[
		("trades", &all_trade_ids),
		("metric", &metric_s),
		("market", &market_.market_name),
	    ])
            .send()
            .await?;

	let priced_portfolio = self._unwrap_pricing_results_a(result_pricing, metric).await;

        // let's do the aggregation here.  TODO: CHECK IF THIS IS NECESSARY
        match priced_portfolio {
            PricingResults::PV(pv_portfolio) => Ok(pv_portfolio),
            PricingResults::PV01(pv01_results) => Ok(pv01_results.aggregate()),
            PricingResults::PnL(pnl_portfolio) => Ok(pnl_portfolio),
        }
    }
    /// similar to price_trades_spark,
    ///   just that it only limits to spark application to
    ///   a certain number of trades, and it repeats the spark
    ///   application
    async fn price_trades_on_spark(
        &self,
        trades: &TradeRep<TR>,
        metric: PricingMetric,
        _pricing_options: &MarketPricingOptions,
        curr_new_mkt: MarketType,
    ) -> Result<PortfolioType, Error> {

        let mut curr_portfolio = PortfolioType::default();

        let split_nb = 200; // TODO: FACTOR THIS NUMBER OUT

        let mut curr_trade_nb = 0;
        let mut curr_trade_rep = TradeRep::<TR>::default();

        let pricing_client = Client::new();

        for (_tid, tr) in trades.iter() {
            curr_trade_rep += tr;
            curr_trade_nb += 1;

            if curr_trade_nb > split_nb {
                // do the computation
                info!("Pricing {:?} trades on spark.", curr_trade_nb);
                let portfolio = self
                    .price_trades_spark(&curr_trade_rep, &pricing_client, curr_new_mkt.clone(), metric)
                    .await?;

                curr_portfolio += portfolio;
                curr_trade_nb = 0;
                curr_trade_rep = TradeRep::<TR>::default();
            }
        }

        // remaining part of trades
        curr_portfolio += self
            .price_trades_spark(
		&curr_trade_rep,
		&pricing_client,
		curr_new_mkt,
		metric
	    )
            .await?;

        Ok(curr_portfolio)
    }

    /// prices the trades on the spark
    #[allow(dead_code)]
    async fn price_trades_spark_2(
        &self,
        trades: &TradeRep<TR>,
        pricing_client: &Client,
        market_: MarketType,
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
            .send()
            .await?;

        // unwrap the result_pricing
        let priced_portfolio = self._unwrap_pricing_results_a(result_pricing, metric).await;

        // let's do the aggregation here
        Ok(match priced_portfolio {
            PricingResults::PV(pv_portfolio) => pv_portfolio,
            PricingResults::PV01(pv01_results) => pv01_results.aggregate(),
            PricingResults::PnL(pnl_portfolio) => pnl_portfolio,
        })
    }
}


// if we have PriceTrade implementation for TR
//   then we have the
// MT: MarketTypeT<MP>
// TR: trade representation.
impl<MP, TR> PriceTrade<MP> for TradeRep<TR>
where
    TR: PriceTrade<MP> + std::fmt::Debug
{

    async fn initial_pv(&self) -> Option<f64> {
        let portf_val = 0.;
        for indiv_trade in self.iter()  {
            let (trade_name, trade_v) = indiv_trade.pair();

            let tv = trade_v.initial_pv().await; // tv = trade value
            match tv {
                None => {
                    warn!("Could not initial_pv of {:?}", trade_v);
                },
                Some(tv_real) => {
                    portf_val += tv_real;
                },
            }
        }
        Some(portf_val)
    }

    async fn price(&self, market: &dyn MarketTypeT<MP=MP>) -> Option<f64> {
        let portf_val = 0.;
        for indiv_trade in self.iter()  {
            let (trade_name, trade_v) = indiv_trade.pair();

            let tv = trade_v.price(market).await;
            match tv {
                None => {
                    warn!("Could not price of {:?}", trade_v);
                },
                Some(tv_real) => {
                    portf_val += tv_real;
                },
            }
        }
        Some(portf_val)
    }

    async fn pv01(&self, market: &dyn MarketTypeT<MP=MP>) -> PV01Results {
        let portf_val = PV01Results::new();
        for indiv_trade in self.iter()  {
            let (trade_name, trade_v) = indiv_trade.pair();

            let tv = trade_v.pv01(market).await;
            portf_val += tv;  // TODO: HANDLE Option here.
        }
        portf_val
    }
}
