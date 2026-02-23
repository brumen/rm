use core::cmp::Eq;
use scc::HashMap as DashMap;
use std::default::Default;
use std::ops::{AddAssign, Deref, DerefMut, Sub, SubAssign};
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
/// String is the trade id, TR is the trade representation.
#[derive(Debug, Clone)]
pub struct TradeRep<TR>(pub DashMap<String, TR>);

impl<TR> PartialEq for TradeRep<TR> {
    // Trade representations are equal if they have the same set of trade ids (keys).
    fn eq(&self, other: &Self) -> bool {
        // Since `scc::HashMap` doesn't provide a cheap stable `len()` without iterating,
        // we do a symmetric subset check.
        self.iter_sync(|k, _| other.contains(k)) && other.iter_sync(|k, _| self.contains(k))
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

#[allow(dead_code)]
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
        // scc::HashMap::iter() yields references to (K, V) tuples.
        let mut all_keys = Vec::<String>::new();
        self.iter_sync(|k, _| {
            all_keys.push(k.clone());
            true
        });
        all_keys
    }

    pub fn all_trade_names(&self) -> Vec<String> {
        self._keys()
    }

    /// does trade representation contain trade_id
    pub fn contains(&self, trade_id: &String) -> bool {
        self.read_sync(trade_id, |_, _| ()).is_some()
    }
}

impl<TR: Clone + BaseTrade> AddAssign<(String, TR)> for TradeRep<TR> {
    // adds the elements of the other TradeRep to this traderep
    fn add_assign(&mut self, (trade_id, trade): (String, TR)) {
        // For scc::HashMap, use upsert_sync to insert or replace.
        self.upsert_sync(trade_id, trade);
    }
}

impl<TR: Clone + BaseTrade> AddAssign<&TradeRep<TR>> for TradeRep<TR> {
    // adds the elements of the other TradeRep to this traderep
    // uses cloning.
    fn add_assign(&mut self, other: &TradeRep<TR>) {
        other.iter_sync(|k, v| {
            self.upsert_sync(k.clone(), v.clone());
            true
        });
    }
}

impl<TR: Clone + BaseTrade> SubAssign<&TradeRep<TR>> for TradeRep<TR> {
    fn sub_assign(&mut self, other: &TradeRep<TR>) {
        // scc::HashMap doesn't yield DashMap-style entries; iterate using iter_sync
        // and remove by key.
        other.iter_sync(|k, _| {
            let _ = self.remove_sync(k);
            true
        });
    }
}

impl<TR: Clone + BaseTrade> Sub<&TradeRep<TR>> for TradeRep<TR> {
    type Output = Self;

    fn sub(self, other: &TradeRep<TR>) -> Self::Output {
        // `scc::HashMap` doesn't provide the same entry API as `dashmap`.
        // Build a new map containing only keys not present in `other`.
        let res_traderep = Self::default();

        self.iter_sync(|k, v| {
            if !other.contains(k) {
                res_traderep.upsert_sync(k.clone(), v.clone());
            }
            true
        });

        res_traderep
    }
}

impl<TR: Clone + BaseTrade> AddAssign<&TR> for TradeRep<TR> {
    fn add_assign(&mut self, other: &TR) {
        self.upsert_sync(other.id().clone(), other.clone());
    }
}

impl<const N: usize, TR: BaseTrade> From<[TR; N]> for TradeRep<TR> {
    fn from(arr: [TR; N]) -> Self {
        let hm = DashMap::with_capacity(N);
        for tr in arr {
            let trade_id = tr.id();
            hm.upsert_sync(trade_id, tr);
        }

        Self(hm)
    }
}
