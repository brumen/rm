use dashmap::DashMap;
use ractor::async_trait;
use rdkafka::message::{BorrowedMessage, Message};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::AddAssign;
use std::sync::Arc;
use uuid::Uuid;

use crate::market::{MarketTypeError, MarketTypeT, SetName};
use crate::ref_deref::TryFromRef2;

/// Simple market implementation for perpetual swaps.
///
/// Modeled after `LETFMarketType` in `rt_server/src/markets/letf_market.rs`.
/// - Keys are `PerpMarketTypes` (e.g., `Perp("BTC-PERP")`)
/// - Values are `f64` (e.g., price, funding rate, etc. depending on your usage)
pub(crate) type MarketInner = DashMap<PerpMarketTypes, f64>;

pub(crate) struct PerpTrade {
    underlying: String,
    init_price: f64,
    size: f64,
}

#[derive(PartialEq, Serialize, Deserialize, Hash, Eq, Debug, Clone)]
pub enum PerpMarketTypes {
    Perp(PerpTrade),
    // TODO: this to be removed later (kept for parity with LETFMarketTypes)
    Break,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerpMarketType {
    pub market_name: String,
    pub market: MarketInner,
}

impl PerpMarketType {
    pub fn new(market_name: String) -> Self {
        Self {
            market_name,
            market: DashMap::<PerpMarketTypes, f64>::new(),
        }
    }
}

impl Default for PerpMarketType {
    fn default() -> Self {
        Self {
            market_name: format!("{}", Uuid::new_v4()),
            market: DashMap::<PerpMarketTypes, f64>::default(),
        }
    }
}

impl std::fmt::Display for PerpMarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.market_name)
    }
}

impl PartialEq for PerpMarketType {
    fn eq(&self, other: &Self) -> bool {
        if self.market_name != other.market_name {
            return false;
        }

        for self_entry in self.market.iter() {
            let self_key = self_entry.key();
            if !other.market.contains_key(self_key) {
                return false;
            }
        }

        for other_entry in other.market.iter() {
            let other_key = other_entry.key();
            if !self.market.contains_key(other_key) {
                return false;
            }
        }

        true
    }
}

impl AddAssign<&PerpMarketType> for PerpMarketType {
    fn add_assign(&mut self, rhs: &Self) {
        for rhs_entry in rhs.market.iter() {
            self.market.insert(rhs_entry.key().clone(), *rhs_entry); // NOTE: clones key
        }
    }
}

impl<const N: usize> From<(String, [(PerpMarketTypes, f64); N])> for PerpMarketType {
    fn from(market_name_arr: (String, [(PerpMarketTypes, f64); N])) -> Self {
        let (market_name, market_array) = market_name_arr;
        let mi = MarketInner::new();
        for (mn, mv) in market_array {
            mi.insert(mn, mv);
        }
        Self {
            market_name,
            market: mi,
        }
    }
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct MktMsgDescr {
    market_name: String,
    market: MarketInner,
}

#[async_trait]
impl MarketTypeT for PerpMarketType {
    type MP = ();
    type MK = PerpMarketTypes;

    fn new(market_name: String, _mp: ()) -> Arc<PerpMarketType> {
        Arc::new(PerpMarketType {
            market_name,
            market: DashMap::<PerpMarketTypes, f64>::new(),
        })
    }

    fn market_name(&self) -> String {
        self.market_name.clone()
    }

    async fn get(&self, key: &Self::MK) -> Option<f64> {
        Some(*(self.market.get(key)?))
    }

    async fn insert(&self, key: Self::MK, value: f64) {
        self.market.insert(key, value);
    }

    fn is_empty(&self) -> bool {
        self.market.is_empty()
    }

    fn try_from_ref(
        market_name: String,
        value: &BorrowedMessage,
        _mp: (),
    ) -> Result<Arc<PerpMarketType>, MarketTypeError> {
        let msg_val = value
            .payload()
            .ok_or(MarketTypeError::GeneralError(format!(
                "Didnt get payload for {}",
                market_name
            )))?;

        let msg_utf = std::str::from_utf8(msg_val)?;
        let inner_dashmap = serde_json::from_str::<MktMsgDescr>(msg_utf)?;

        Ok(Arc::new(Self {
            market_name,
            market: inner_dashmap.market,
        }))
    }

    fn market_params(&self) {}
}

impl SetName for PerpMarketType {
    fn set_name(&mut self, new_name: String) {
        self.market_name = new_name;
    }
}

impl TryFromRef2 for (PerpMarketTypes, f64) {}
