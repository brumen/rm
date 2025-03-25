use core::cmp::Eq;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::default::Default;
use std::ops::{AddAssign, Deref, DerefMut, SubAssign, Sub};
use thiserror::Error;


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
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TradeRep<TR>(pub HashMap<String, TR>);

impl<TR> Deref for TradeRep<TR> {
    type Target = HashMap<String, TR>;

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
        Self(HashMap::<String, TR>::new())
    }
}

impl<TR> TradeRep<TR> {
    // pub fn new() -> Self {
    //     Self(HashMap::<String, TR>::new())
    // }

    /// returns all trade ids in the trade representation.
    pub fn all_trade_names(&self) -> Vec<&String> {
        self.keys().into_iter().collect::<Vec<&String>>()
    }

    /// does trade representation contain trade_id
    pub fn contains(&self, trade_id: &String) -> bool {
        self.keys()
            .position(|tradeid| tradeid.eq(trade_id))
            .is_some()
    }

    pub fn len(&self) -> usize {
	self.keys().len()
    }
}

impl<TR: Clone + BaseTrade> AddAssign<&TradeRep<TR>> for TradeRep<TR> {
    fn add_assign(&mut self, other: &TradeRep<TR>) {
        for (trade_id, trade_value) in other.iter() {
            self.insert((*trade_id).clone(), (*trade_value).clone());
        }
    }
}

impl<TR: Clone + BaseTrade> SubAssign<&TradeRep<TR>> for TradeRep<TR> {
    fn sub_assign(&mut self, other: &TradeRep<TR>) {
        for (trade_id, _) in other.iter() {
	    self.remove(trade_id);
	}
    }
}

impl<TR: Clone + BaseTrade> Sub<&TradeRep<TR>> for TradeRep<TR> {
    type Output = Self;

    fn sub(self, other: &TradeRep<TR>) -> Self::Output {
	// create a separate hashmap.
	let mut res_traderep = Self::default();
	for (trade_id, trade_rr) in self.iter() {
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
        let mut hm = HashMap::with_capacity(N);
        for tr in arr {
            let trade_id = tr.id();
            hm.insert(trade_id, tr);
        }

        Self(hm)
    }
}
