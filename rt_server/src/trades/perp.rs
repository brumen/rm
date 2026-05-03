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
/// - `amount` is in USD notional (positive = long, negative = short)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerpTrade {
    pub trade_id: String,
    pub underlying: String,
    pub amount: f64,
}

const INTEREST_RATE: f64 = 0.0001;

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

    // computes the mark price of the perp swap.
    async fn price(&self, market: Arc<LETFMarketType>) -> Option<f64> {
        let underlying_price = market
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await?;
        let perp_price = market
            .get(&LETFMarketTypes::Perp(self.underlying.clone()))
            .await?;

        let numerator = (underlying_price - perp_price).abs();
        let pi = numerator / underlying_price;
        let interest = INTEREST_RATE - pi;

        let funding_basis = interest * 1.; // 1 is funding interval hours. TO BE CORRECTED LATER.
        let mark_price = underlying_price * (1. + funding_basis);

        Some(mark_price)
    }

    async fn pv01(&self, market: Arc<LETFMarketType>) -> PV01Results {
        // TODO: somewhat suboptimal, but leave it for now - spot is called 2x.
        let spot = market
            .get(&LETFMarketTypes::Stock(self.underlying.clone()))
            .await;
        let mark_price = self.price(market.clone()).await;

        match (spot, mark_price) {
            (None, _) => {
                warn!(
                    "pv01: PerpTrade: could not obtain underlying {:?} from the market.",
                    self.underlying
                );
                PV01Results::new()
            }
            (_, None) => {
                warn!("pv01: PerpTrade: missing perp price");
                PV01Results::new()
            }
            (Some(real_spot), Some(real_mark)) => {
                let mut pv01_result = PV01Results::new();
                let _ = pv01_result.insert(
                    self.trade_id.clone(),
                    PortfolioType::from([(self.underlying.clone(), real_mark / real_spot)]),
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
