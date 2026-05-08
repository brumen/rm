use ractor::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
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
        let stock = self.underlying.clone();
        let stock_letf = LETFMarketTypes::Stock(stock.clone());

        let s_old = market_old.get(&stock_letf).await;
        let s_new = market_new.get(&stock_letf).await;

        let perp_letf = LETFMarketTypes::Perp(stock.clone());
        let perp_old = market_old.get(&perp_letf).await;
        let perp_new = market_new.get(&perp_letf).await;

        (s_new != s_old) || (perp_old != perp_new)
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
        let mark_price = underlying_price * (1. + funding_basis) * self.amount;

        //        sleep(Duration::from_secs(2)).await; // artificial sleeping.

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
                    PortfolioType::from([(
                        self.underlying.clone(),
                        real_mark / real_spot * self.amount,
                    )]),
                );
                debug!("_pv01: Perp trade: {:?}", pv01_result);
                pv01_result
            }
        }
    }
}
