use futures::future::join_all;
use ractor::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tracing::warn;

use crate::market::MarketTypeT;
use crate::portfolio::{PV01Results, PortfolioType, PricingResults};
use crate::trade::{BaseTrade, TradeDirection, TradeRep};

// which metric to compute
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Deserialize)]
pub enum PricingMetric {
    #[serde(rename = "PV")]
    PV,
    #[serde(rename = "PV01")]
    PV01,
    #[serde(rename = "PnL")]
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
    ) -> PricingResults {
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

// market type T send and sync version,
// pub(crate) type MarketTypeTSend<MP> = dyn MarketTypeT<MP=MP> + Send + Sync;

// MP are market parameters, () if none.
// MT is market type, depending on the market parameters.
#[async_trait]
pub trait PriceTrade<MT>: BaseTrade + Send + Sync
where
    MT: MarketTypeT + Send + Sync + 'static,
{
    async fn initial_pv(&self) -> Option<f64>
    where
        Self: Send;
    async fn needs_recompute(&self, market_old: Arc<MT>, market_new: Arc<MT>) -> bool; // whether the trade needs recompute on the new market
    async fn price(&self, market: Arc<MT>) -> Option<f64>;
    // TODO: pv01 has to be changed to return Option<PV01Results>
    async fn pv01(&self, market: Arc<MT>) -> PV01Results;
    async fn pnl(&self, market: Arc<MT>) -> Option<f64> {
        let initial_pv_val = self.initial_pv().await?;

        self.price(market)
            .await
            .map(|curr_price| curr_price - initial_pv_val)
    }

    /// values the trade for a specific metric.
    #[allow(dead_code)]
    async fn value_by_metric(&self, metric: PricingMetric, market: Arc<MT>) -> PricingResults {
        let trade_name = self.id();

        match metric {
            PricingMetric::PV => {
                let priced_trade = self.price(market).await;
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade)]))
                } else {
                    PricingResults::PV(PortfolioType::default())
                }
            }

            PricingMetric::PV01 => {
                let trade_pv01 = self.pv01(market).await;
                PricingResults::PV01(trade_pv01)
            }

            PricingMetric::PnL => {
                let pnl_trade = self.pnl(market).await;
                if let Some(pnl_trade_real) = pnl_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, pnl_trade_real)]))
                } else {
                    PricingResults::PV(PortfolioType::default())
                }
            }
        }
    }
}

// implements the hedging of the trade
// TODO: Check what TR should be.
#[async_trait]
pub trait HedgeTrade<MT, TR>
where
    MT: MarketTypeT + Send + Sync + 'static,
{
    async fn hedge(&self, market: MT) -> TR;
}

// This is not important, but was implemented to implement PriceTrade
impl<TR> BaseTrade for TradeRep<TR> {
    fn id(&self) -> String {
        "TradeRep".to_string() // PERHAPS FIX THIS.
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}

// if we have PriceTrade implementation for TR
//   then we have the PriceTrade implementation for TradeReduce
// MT: MarketTypeT<MP>
// TR: trade representation.
#[async_trait]
impl<MT, TR> PriceTrade<MT> for TradeRep<TR>
where
    TR: PriceTrade<MT> + Send + Sync + Clone,
    MT: MarketTypeT + Send + Sync + 'static,
{
    async fn needs_recompute(&self, market_old: Arc<MT>, market_new: Arc<MT>) -> bool {
        let mut trades_owned = vec![];
        self.iter_async(|_, v| {
            trades_owned.push(v.clone()); // TODO: This here is BAD!!
            true
        })
        .await;

        // iterate through the vector and check if they need recompute
        let tof = trades_owned
            .iter()
            .map(|trade| trade.needs_recompute(market_old.clone(), market_new.clone()));

        join_all(tof).await.iter().all(|x| *x)
    }

    async fn initial_pv(&self) -> Option<f64> {
        let mut portf_val = 0.;
        // TODO: This below is repeated 3 times. Factor out. depends on TR: Clone
        let mut trades_owned = vec![];
        self.iter_async(|_, v| {
            trades_owned.push(v.clone()); // TODO: This here is BAD!!
            true
        })
        .await;

        for trade in trades_owned.iter() {
            match trade.initial_pv().await {
                Some(tv) => portf_val += tv,
                None => {
                    warn!("Could not initial_pv of {:?}", trade.id());
                }
            }
        }

        Some(portf_val)
    }

    async fn price(&self, market: Arc<MT>) -> Option<f64> {
        let mut portf_val = 0.;
        let mut trades_owned = vec![];
        self.iter_async(|_, v| {
            trades_owned.push(v.clone()); // TODO: This here is BAD!!
            true
        })
        .await;

        for trade in trades_owned.iter() {
            match trade.price(market.clone()).await {
                Some(tv) => portf_val += tv,
                None => {
                    warn!("Could not initial_pv of {:?}", trade.id());
                }
            };
        }

        Some(portf_val)
    }

    async fn pv01(&self, market: Arc<MT>) -> PV01Results {
        let mut portf_val = PV01Results::new();
        let mut trades_owned = vec![];
        self.iter_async(|_, trade| {
            trades_owned.push(trade.clone()); // TODO: trade is cloned here - BAD.
            true
        })
        .await;

        for trade in trades_owned.iter() {
            portf_val += &trade.pv01(market.clone()).await;
        }
        portf_val
    }
}
