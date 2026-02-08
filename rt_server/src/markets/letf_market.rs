use chrono::NaiveDate;
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

pub(crate) type MarketInner = DashMap<LETFMarketTypes, f64>;

#[derive(PartialEq, Serialize, Deserialize, Hash, Eq, Debug, Clone)]
enum SabrParamNames {
    Alpha,
    Beta,
    Rho,
    Nu,
}

#[derive(PartialEq, Serialize, Deserialize, Hash, Eq, Debug, Clone)]
pub struct SabrParameters {
    stock: String,
    maturity: NaiveDate,
    param_name: SabrParamNames,
}

#[derive(PartialEq, Serialize, Deserialize, Hash, Eq, Debug, Clone)]
pub enum LETFMarketTypes {
    Stock(String),
    Option(String),       // option ticker, option value
    Sabr(SabrParameters), // Sabr parameters, sabr param value
    // TODO: this to be removed later
    Break,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LETFMarketType {
    pub market_name: String,
    pub market: MarketInner,
}

impl LETFMarketType {
    pub fn new(market_name: String) -> Self {
        Self {
            market_name,
            market: DashMap::<LETFMarketTypes, f64>::new(),
        }
    }
}

impl Default for LETFMarketType {
    fn default() -> Self {
        Self {
            market_name: format!("{}", Uuid::new_v4()),
            market: DashMap::<LETFMarketTypes, f64>::default(),
        }
    }
}

impl std::fmt::Display for LETFMarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.market_name)
    }
}

impl PartialEq for LETFMarketType {
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

impl AddAssign<&LETFMarketType> for LETFMarketType {
    fn add_assign(&mut self, rhs: &Self) {
        for rhs_entry in rhs.market.iter() {
            self.market.insert(rhs_entry.key().clone(), *rhs_entry); // TODO: clone here
        }
    }
}

impl<const N: usize> From<(String, [(LETFMarketTypes, f64); N])> for LETFMarketType {
    fn from(market_name_arr: (String, [(LETFMarketTypes, f64); N])) -> Self {
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
impl MarketTypeT for LETFMarketType {
    type MP = ();
    type MK = LETFMarketTypes;

    fn new(market_name: String, _mp: ()) -> Arc<LETFMarketType> {
        Arc::new(LETFMarketType {
            market_name,
            market: DashMap::<LETFMarketTypes, f64>::new(),
        })
    }

    fn market_name(&self) -> String {
        self.market_name.clone()
    }

    async fn get(&self, stock: &Self::MK) -> Option<f64> {
        Some(*(self.market.get(stock)?))
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
    ) -> Result<Arc<LETFMarketType>, MarketTypeError> {
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

impl SetName for LETFMarketType {
    fn set_name(&mut self, new_name: String) {
        self.market_name = new_name;
    }
}

impl TryFromRef2 for (LETFMarketTypes, f64) {}
