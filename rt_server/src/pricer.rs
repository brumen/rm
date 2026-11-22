use futures::future::join_all;
use ractor::async_trait;
use rdkafka::message::ToBytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tracing::warn;

use crate::market::MarketTypeT;
use crate::portfolio::{PV01Results, PortfolioType, PricingResults};
use crate::trade::{BaseTrade, TradeDirection, TradeRep};

/// A case-sensitive metric name. New metrics need no central enum variant.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PricingMetric(std::borrow::Cow<'static, str>);

impl PricingMetric {
    pub const PV: Self = Self(std::borrow::Cow::Borrowed("PV"));
    pub const PV01: Self = Self(std::borrow::Cow::Borrowed("PV01"));
    #[allow(non_upper_case_globals)]
    pub const PnL: Self = Self(std::borrow::Cow::Borrowed("PnL"));

    pub fn new(name: impl Into<String>) -> Self {
        Self(std::borrow::Cow::Owned(name.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PricingMetric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ToBytes for PricingMetric {
    fn to_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetricError {
    #[error("Unsupported metric: {0}")]
    UnsupportedMetric(String),
    #[error("Metric calculation failed: {0}")]
    Calculation(String),
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
    ) -> Result<PricingResults, MetricError> {
        match metric.as_str() {
            "PV" => {
                let results_conv = result_price.json::<HashMap<String, f64>>();

                if results_conv.is_err() {
                    return Ok(PricingResults::PV(PortfolioType::default()));
                }

                Ok(PricingResults::PV(PortfolioType(results_conv.unwrap())))
            }

            "PV01" => {
                let results_conv = result_price.json::<HashMap<String, HashMap<String, f64>>>();

                if results_conv.is_err() {
                    return Ok(PricingResults::PV01(PV01Results::new()));
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert(
                        (*trade_id.clone()).to_string(),
                        PortfolioType::from(trade_result),
                    );
                }
                Ok(PricingResults::PV01(pv01))
            }

            name => Err(MetricError::UnsupportedMetric(name.into())),
        }
    }

    async fn _unwrap_pricing_results_a(
        &self,
        result_price: reqwest::Response,
        metric: PricingMetric,
    ) -> Result<PricingResults, MetricError> {
        match metric.as_str() {
            "PV" => {
                let results_conv = result_price.json::<HashMap<String, f64>>().await;

                if results_conv.is_err() {
                    return Ok(PricingResults::PV(PortfolioType::default()));
                }

                Ok(PricingResults::PV(PortfolioType(results_conv.unwrap())))
            }

            "PV01" => {
                let results_conv = result_price
                    .json::<HashMap<String, HashMap<String, f64>>>()
                    .await;

                if results_conv.is_err() {
                    return Ok(PricingResults::PV01(PV01Results::new()));
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert(
                        (*trade_id.clone()).to_string(),
                        PortfolioType::from(trade_result),
                    );
                }
                Ok(PricingResults::PV01(pv01))
            }

            name => Err(MetricError::UnsupportedMetric(name.into())),
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
    async fn pnl(&mut self, market: Arc<MT>) -> Option<f64> {
        let initial_pv_val = self.initial_pv().await?;
        let curr_price = self.price(market).await?;
        let _ = self.update_prev_pv(Some(curr_price));
        let pnl = curr_price - initial_pv_val;
        Some(pnl)
        // let pnl = self
        //     .price(market)
        //     .await
        //     .map(|curr_price| curr_price - initial_pv_val);
    }
    async fn update_prev_pv(&mut self, _new_market_val: Option<f64>) {}

    /// Compute an additive contribution on the captured market. Scalar metrics
    /// use trade IDs as keys; bucketed metrics use common exposure keys.
    /// Every requested metric must succeed before the trade is covered.
    async fn compute_metric(
        &mut self,
        metric: &PricingMetric,
        market: Arc<MT>,
    ) -> Result<PortfolioType, MetricError> {
        let scalar = match metric.as_str() {
            "PV" => self.price(market).await,
            "PnL" => self.pnl(market).await,
            // Preserve the legacy API's empty-is-zero exposure convention.
            "PV01" => return Ok(self.pv01(market).await.aggregate()),
            name => return self.custom_metric(name, market).await,
        };
        scalar
            .map(|value| PortfolioType::from([(self.id(), value)]))
            .ok_or_else(|| MetricError::Calculation(format!("No {metric} price for {}", self.id())))
    }

    /// Override for additional Rust-defined metrics. Return an empty map only
    /// for an explicitly valid zero, and an error for unsupported/missing data.
    async fn custom_metric(
        &mut self,
        name: &str,
        _market: Arc<MT>,
    ) -> Result<PortfolioType, MetricError> {
        Err(MetricError::UnsupportedMetric(name.into()))
    }

    /// Compatibility name for the generic calculation entry point.
    async fn value_by_metric(
        &mut self,
        metric: &PricingMetric,
        market: Arc<MT>,
    ) -> Result<PortfolioType, MetricError> {
        self.compute_metric(metric, market).await
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
    async fn custom_metric(
        &mut self,
        name: &str,
        market: Arc<MT>,
    ) -> Result<PortfolioType, MetricError> {
        let mut trades = Vec::new();
        self.iter_async(|_, trade| {
            trades.push(trade.clone());
            true
        })
        .await;
        let mut result = PortfolioType::default();
        for mut trade in trades {
            result += trade.custom_metric(name, market.clone()).await?;
        }
        Ok(result)
    }

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
