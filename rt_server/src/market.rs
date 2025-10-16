use ractor::async_trait;
use rdkafka::message::{BorrowedMessage, Message};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;
use std::collections::{hash_map::IntoIter, HashMap};  // , HashMap};
use std::ops::{AddAssign, Deref, DerefMut};
use std::sync::mpsc::Sender;
use tracing::{debug, info, warn};
use std::fmt;
use std::default::Default;
use std::iter::IntoIterator;
use std::sync::{Arc, Mutex};
use thiserror::Error;
//use flurry::HashMap;
use dashmap::DashMap;


use crate::ref_deref::TryFromRef;
use crate::ref_deref_trait;

// market information = ((flight, market date), value)
// MK ... mnemonic for market key
// MK used to be (String, Date), now it's generic,
//    it has to be hashable, and it copyable for now

pub(crate) type MarketInner = DashMap<String, f64>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarketType {
    pub market_name: String,
    pub market: MarketInner,
}

impl PartialEq for MarketType {
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


// TODO: THIS NEEDS TO BE FIXED.
// impl Into<MarketInner> for MarketType {
//     fn into(self) -> MarketInner {
//         self.market.clone()  // TODO: THIS IS GARBAGE HERE AS WELL!!
//     }
// }


impl MarketType {
    pub(crate) fn new(market_name: String) -> Self {
        Self{
            market_name,
            market: MarketInner::new(),
        }
    }

    pub(crate) fn insert(&mut self, key: String, value: f64) {
        self.market.insert(key, value);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.market.is_empty()
    }
}

// TODO: CHECK THIS STUFF HERE!!!
impl Default for MarketType {
    fn default() -> Self {
        let uuid_name = Uuid::new_v4();

        MarketType::new(uuid_name.to_string())
    }
}

impl AddAssign<&MarketType> for MarketType {
    fn add_assign(&mut self, rhs: &MarketType) {
        for rhs_entry in rhs.market.iter() {
            self.market.insert(rhs_entry.key().clone(), rhs_entry.value().clone());  // TODO: clone here
        }
    }
}


impl<const N: usize> From<(String, [(String, f64); N])> for MarketType {
    fn from(market_name_arr: (String, [(String, f64); N])) -> Self {
        Self {
            market_name: market_name_arr.0,
            market: MarketInner::from(market_name_arr.1)
        }
    }
}


#[derive(Error, Debug)]
pub enum MarketTypeError {
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to MarketType")]
    CantConvertToMarket(#[from] serde_json::Error),
}


// impl TryFromRef<BorrowedMessage<'_>> for MarketType {
//     type Error = MarketTypeError;

//     fn try_from_ref(value: &BorrowedMessage) -> Result<Self, Self::Error> {
//         let msg_val = value.payload().ok_or(
//             MarketTypeError::CantConvertUtf8(
//                 std::str::Utf8Error::from_utf8_error()
//             )
//         )?; // Ensure payload is valid
//         let msg_utf = std::str::from_utf8(msg_val)?;

//         debug!("try_from_ref(MarketType): Msg = {:?}", msg_utf);
//         Ok(serde_json::from_str::<MarketType>(msg_utf)?)
//     }
// }


impl fmt::Display for MarketType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let nb_items = self.market.len();
        write!(f, "Market: {}, nb_items: {}.", self.market_name, nb_items)
    }
}


#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AOMktParams {
    mkt_sender: Sender<MarketType>,
}

#[derive(Debug, Clone)]
pub struct LETFP {
    pub curr_mkt: Arc<Mutex<MarketType>>,
}


#[derive(Debug, Clone)]
pub enum MktMsgParams {
    AOParams(),
    LETFParams(LETFP),
}







#[cfg(test)]
mod tests {
    use super::{AllMarkets, CurrNewMarket};

    #[test]
    fn test_first_market() {
        let all_markets = AllMarkets::new(5);

        let first_market = all_markets.above_market("current".to_string());
        let last_market = all_markets.above_market("new_4".to_string());
        // next market from current is "new_0"

        assert_eq!(first_market, Some(CurrNewMarket("new_0".to_string())));
        assert_eq!(last_market, Some(CurrNewMarket("new".to_string())));
    }
}
