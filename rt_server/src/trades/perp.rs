use ractor::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::market::MarketTypeT;
use crate::markets::letf_market::{LETFMarketType, LETFMarketTypes};
use crate::portfolio::{PV01Results, PortfolioType};
use crate::pricer::PriceTrade;
use crate::trade::{BaseTrade, TradeDirection};

/// A simple perpetual swap trade priced off an underlying spot (from `LETFMarketType`),
/// with optional initial spot set at trade creation time.
///
/// Pricing convention (similar spirit to `LETFTrade`):
/// PV = amount * (S / S0 - 1)
///
/// - `amount` is in USD notional (positive = long, negative = short)
/// - `initial_spot` is the reference spot at inception (S0)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerpTrade {
    pub trade_id: String,
    pub underlying: String,
    pub amount: f64,
    pub initial_spot: Option<f64>,
}

impl fmt::Display for PerpTrade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.trade_id)
    }
}

impl BaseTrade for PerpTrade {
    fn id(&self) -> String {
        self.trade_id.clone()
    }

    fn direction(&self) -> TradeDirection {
        if self.amount < 0.0 {
            TradeDirection::Delete
        } else {
            TradeDirection::Create
        }
    }
}

impl PartialEq for PerpTrade {
    fn eq(&self, other: &Self) -> bool {
        self.trade_id == other.trade_id
    }
}

#[async_trait]
impl PriceTrade<LETFMarketType> for PerpTrade {
    async fn needs_recompute(
        &self,
        market_old: Arc<LETFMarketType>,
        market_new: Arc<LETFMarketType>,
    ) -> bool {
        let s_old = market_old
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await;
        let s_new = market_new
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await;

        s_new != s_old
    }

    async fn initial_pv(&self) -> Option<f64> {
        Some(0.0)
    }

    async fn price(&self, market: Arc<LETFMarketType>) -> Option<f64> {
        let spot = market
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await?;

        let s0 = self.initial_spot?;

        Some(self.amount * (spot / s0 - 1.0))
    }

    async fn pv01(&self, market: Arc<LETFMarketType>) -> PV01Results {
        let spot = market
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await;

        match (spot, self.initial_spot) {
            (None, _) => {
                warn!(
                    "pv01: PerpTrade: could not obtain underlying {:?} from the market.",
                    self.underlying
                );
                PV01Results::new()
            }
            (_, None) => {
                warn!("pv01: PerpTrade: missing initial spot (initial_spot)");
                PV01Results::new()
            }
            (Some(_spot), Some(s0)) => {
                // PV = amount * (S/S0 - 1)
                // dPV/dS = amount * (1/S0)
                let mut pv01_result = PV01Results::new();
                let _ = pv01_result.insert(
                    self.trade_id.clone(),
                    PortfolioType::from([(self.underlying.clone(), self.amount / s0)]),
                );
                debug!("_pv01: Perp trade: {:?}", pv01_result);
                pv01_result
            }
        }
    }
}

/// Hedge instruments for a `PerpTrade`.
///
/// For now we mirror the LETF hedge approach:
/// - A `PerpHedge::Future` to neutralize delta on the underlying.
/// - A `PerpHedge::Cash` to offset notional.
///
/// This keeps the rest of the engine consistent with existing hedging flows.
#[derive(Debug, Serialize, Deserialize)]
pub enum PerpHedge {
    Future(PerpFuture),
    Cash(PerpCash),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerpFuture {
    pub trade_id: String,
    pub underlying: String,
    pub amount: f64,
    pub initial_val: Option<f64>,
}

impl fmt::Display for PerpFuture {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.trade_id)
    }
}

impl BaseTrade for PerpFuture {
    fn id(&self) -> String {
        self.trade_id.clone()
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}

impl PartialEq for PerpFuture {
    fn eq(&self, other: &Self) -> bool {
        self.trade_id == other.trade_id
    }
}

#[async_trait]
impl PriceTrade<LETFMarketType> for PerpFuture {
    async fn needs_recompute(
        &self,
        market_old: Arc<LETFMarketType>,
        market_new: Arc<LETFMarketType>,
    ) -> bool {
        let s_old = market_old
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await;
        let s_new = market_new
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await;

        s_new != s_old
    }

    async fn initial_pv(&self) -> Option<f64> {
        self.initial_val
    }

    async fn price(&self, market: Arc<LETFMarketType>) -> Option<f64> {
        let spot = market
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await?;

        Some(spot * self.amount)
    }

    async fn pv01(&self, _market: Arc<LETFMarketType>) -> PV01Results {
        let mut pv01_results = PV01Results::new();
        let _ = pv01_results.insert(
            self.trade_id.clone(),
            PortfolioType::from([(self.underlying.clone(), self.amount)]),
        );
        debug!("_pv01: pv01 PerpFuture: {:?}", pv01_results);
        pv01_results
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerpCash {
    pub trade_id: String,
    pub amount: f64,
}

impl fmt::Display for PerpCash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.trade_id)
    }
}

impl BaseTrade for PerpCash {
    fn id(&self) -> String {
        self.trade_id.clone()
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}

impl PartialEq for PerpCash {
    fn eq(&self, other: &Self) -> bool {
        self.trade_id == other.trade_id
    }
}

#[async_trait]
impl PriceTrade<LETFMarketType> for PerpCash {
    async fn needs_recompute(
        &self,
        _market_old: Arc<LETFMarketType>,
        _market_new: Arc<LETFMarketType>,
    ) -> bool {
        false
    }

    async fn initial_pv(&self) -> Option<f64> {
        Some(self.amount)
    }

    async fn price(&self, _market: Arc<LETFMarketType>) -> Option<f64> {
        Some(self.amount)
    }

    async fn pv01(&self, _market: Arc<LETFMarketType>) -> PV01Results {
        PV01Results::new()
    }
}
