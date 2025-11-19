use ractor::async_trait;
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use rdkafka::message::{BorrowedMessage, Message};
use uuid::Uuid;
use std::ops::{AddAssign};
use std::sync::Arc;

use crate::market::{MarketTypeT, MarketTypeError};
use crate::trade_letf::LETFHedge;

pub(crate) type MarketInner = DashMap<String, f64>;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LETFMarketType {
    pub market_name: String,
    pub market: MarketInner,
}


impl LETFMarketType {
    pub(crate) fn new(market_name: String) -> Self {
        Self {
            market_name,
            market: DashMap::<String, f64>::new(),
        }
    }
}


impl Default for LETFMarketType {
    fn default() -> Self {
        Self {
            market_name: format!("{}", Uuid::new_v4()),
            market: DashMap::<String, f64>::default()
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
            self.market.insert(rhs_entry.key().clone(), rhs_entry.value().clone());  // TODO: clone here
        }
    }
}


impl<const N: usize> From<(String, [(String, f64); N])> for LETFMarketType {
    fn from(market_name_arr: (String, [(String, f64); N])) -> Self {
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

#[async_trait]
impl MarketTypeT for LETFMarketType {

    type MP = ();

    fn new(market_name: String, _mp: ()) -> Arc<dyn MarketTypeT<MP=Self::MP> + Send + Sync> {
        Arc::new(
            LETFMarketType {
                market_name,
                market: DashMap::<String, f64>::new()
            }
        )
    }

    fn market_name(&self) -> String {
        self.market_name.clone()
    }

    async fn get(&self, stock: &String) -> Option<f64> {
        Some(*(self.market.get(stock)?))
    }

    async fn insert(&self, key: String, value: f64) {
        self.market.insert(key, value);
    }

    fn is_empty(&self) -> bool {
        self.market.is_empty()
    }

    fn try_from_ref(
        market_name: String,
        value: &BorrowedMessage,
        _mp: ()
    ) -> Result<Arc<dyn MarketTypeT<MP=Self::MP> + Send + Sync>, MarketTypeError> {
        let msg_val = value.payload().ok_or(
            MarketTypeError::GeneralError("Didnt get payload".to_string())
        )?;

        let msg_utf = std::str::from_utf8(msg_val)?;
        let inner_dashmap = serde_json::from_str::<MarketInner>(msg_utf)?;

        Ok(
            Arc::new(
                Self {
                    market_name,
                    market: inner_dashmap,
                }
            )
        )
    }

    fn market_params(&self) -> &Self::MP { &() }

}


#[async_trait]
impl MarketTypeT for Arc<LETFMarketType> {

    type MP = ();

    fn new(market_name: String, _mp: ()) -> Arc<dyn MarketTypeT<MP=Self::MP> + Send + Sync> {
	Arc::new(LETFMarketType::new(market_name))
    }

    fn market_name(&self) -> String {
        self.market_name.clone()
    }

    async fn get(&self, stock: &String) -> Option<f64> {
	self.as_ref().get(stock).await
    }

    async fn insert(&self, key: String, value: f64) {
	let _ = self.as_ref().insert(key, value).await;
    }

    fn is_empty(&self) -> bool {
	self.as_ref().is_empty()
    }

    fn try_from_ref(
        market_name: String,
        value: &BorrowedMessage,
        _mp: ()
    ) -> Result<Arc<dyn MarketTypeT<MP=Self::MP> + Send + Sync>, MarketTypeError> {
	LETFMarketType::try_from_ref(market_name, value, _mp)
    }

    fn market_params(&self) -> &Self::MP { &() }

}
