use core::cmp::Eq;
use serde::{Deserialize, Serialize};
use std::default::Default;
use std::ops::{AddAssign, Deref, DerefMut, SubAssign, Sub};
use thiserror::Error;
use dashmap::DashMap;

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum TradeDirection {
    Create,
    Delete,
    Update,
}

pub trait BaseTrade {
    fn id(&self) -> String;
    fn direction(&self) -> TradeDirection;
}

#[derive(Error, Debug)]
pub enum TradeError {
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to trade type")]
    CantConvertToTrade(#[from] serde_json::Error),
    #[error("No payload in the message")]
    NoPayload,
}

/// Internal representations of trades.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeRep<TR>(pub DashMap<String, TR>);


impl<TR> PartialEq for TradeRep<TR> {
    // trade representations are equal if they have the same trade descriptors.
    fn eq(&self, other: &Self) -> bool {

        for entry in self.iter() {
            if !other.contains(entry.key()) {
                return false;
            }
        }

        for trade_entry in other.iter() {
            // .key is the trade name, .value is the trade representation
            if !self.contains(trade_entry.key()) {
                return false;
            }
        }

        true
    }
}


impl<TR> Deref for TradeRep<TR> {
    type Target = DashMap<String, TR>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<TR> DerefMut for TradeRep<TR> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


pub trait TradeReduce {
    type TradeType: BaseTrade + Send;
    type ReductionType: Send + Sync + Clone + BaseTrade;

    fn reduce(&self, trade: &Self::TradeType) -> Self::ReductionType;
}

impl<TR> Default for TradeRep<TR> {
    fn default() -> Self {
        Self(DashMap::<String, TR>::new())
    }
}

impl<TR> TradeRep<TR> {

    /// returns all trade ids in the trade representation.
    // this does copy the trade names out. POTENTIAL COPY IMPACT.
    fn _keys(&self) -> Vec<String> {
        self.iter().map(|entry| entry.key().clone()).collect::<Vec<String>>()
    }

    pub fn all_trade_names(&self) -> Vec<String> {
        self._keys()
    }

    /// does trade representation contain trade_id
    pub fn contains(&self, trade_id: &String) -> bool {
        self.iter().position(|entry| entry.key() == trade_id).is_some()
    }
}

impl<TR: Clone + BaseTrade> AddAssign<&TradeRep<TR>> for TradeRep<TR> {
    // adds the elements of the other TradeRep to this traderep
    // uses cloning.
    fn add_assign(&mut self, other: &TradeRep<TR>) {
        for other_entry in other.iter() {
            self.insert(
                other_entry.key().clone(), other_entry.value().clone()
            );
        }
    }
}

impl<TR: Clone + BaseTrade> SubAssign<&TradeRep<TR>> for TradeRep<TR> {
    fn sub_assign(&mut self, other: &TradeRep<TR>) {
        for other_entry in other.iter() {
	    self.remove(other_entry.key());
	}
    }
}

impl<TR: Clone + BaseTrade> Sub<&TradeRep<TR>> for TradeRep<TR> {
    type Output = Self;

    fn sub(self, other: &TradeRep<TR>) -> Self::Output {
	// create a separate hashmap.
	let res_traderep = Self::default();
	for entry in self.iter() {  // (trade_id, trade_rr)
            let trade_id = entry.key();
            let trade_rr = entry.value();
	    if !other.contains(trade_id) {
		res_traderep.insert(trade_id.clone(), trade_rr.clone());  // TODO: CHECK IF CLONE IS GOOD!!!
	    }
	}
	res_traderep
    }
}

impl<TR: Clone + BaseTrade> AddAssign<&TR> for TradeRep<TR> {
    fn add_assign(&mut self, other: &TR) {
        self.insert(other.id().clone(), other.clone());
    }
}

impl<const N: usize, TR: BaseTrade> From<[TR; N]> for TradeRep<TR> {
    fn from(arr: [TR; N]) -> Self {
        let hm = DashMap::with_capacity(N);
        for tr in arr {
            let trade_id = tr.id();
            hm.insert(trade_id, tr);
        }

        Self(hm)
    }
}
