use kafka::consumer::Message;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::IntoIter, HashMap};
use std::ops::{AddAssign, Deref, DerefMut};
use std::sync::mpsc::{Receiver, Sender};

use std::default::Default;
use std::iter::IntoIterator;
use std::sync::{Arc, Mutex};
use thiserror::Error;

use crate::ref_deref::TryFromRef;
use crate::ref_deref_trait;

// market information = ((flight, market date), value)
// MK ... mnemonic for market key
// MK used to be (String, Date), now it's generic,
//    it has to be hashable, and it copyable for now
pub type MarketInner = HashMap<String, f64>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketType(pub MarketInner);

impl Into<MarketInner> for MarketType {
    fn into(self) -> MarketInner {
        self.0.into_iter().map(|x| (x.0, x.1)).collect()
    }
}

// making a MarketType an iterator.
impl IntoIterator for MarketType {
    type Item = (String, f64);
    type IntoIter = IntoIter<String, f64>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

ref_deref_trait!(MarketType, MarketInner);

impl MarketType {
    pub fn new() -> Self {
        Self(MarketInner::new())
    }

    pub fn insert(&mut self, key: String, value: f64) {
        self.0.insert(key, value);
    }
}

// TODO: CHECK THIS STUFF HERE!!!
impl Default for MarketType {
    fn default() -> Self {
        Self(MarketInner::new())
    }
}

impl AddAssign<&MarketType> for MarketType {
    fn add_assign(&mut self, rhs: &MarketType) {
        for (ticker, value) in rhs.iter() {
            self.insert(ticker.clone(), *value);
        }
    }
}

impl<const N: usize> From<[(String, f64); N]> for MarketType {
    fn from(arr: [(String, f64); N]) -> Self {
        Self(MarketInner::from(arr))
    }
}

#[derive(Error, Debug)]
pub enum MarketTypeError {
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to MarketType")]
    CantConvertToMarket(#[from] serde_json::Error),
}

impl TryFromRef<Message<'_>> for MarketType {
    type Error = MarketTypeError;

    fn try_from_ref(value: &Message) -> Result<Self, Self::Error> {
        let msg_utf = std::str::from_utf8(value.value)?;

        debug!("try_from_ref(MarketType): Msg = {:?}", msg_utf);
        Ok(serde_json::from_str::<MarketType>(msg_utf)?)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum CurrNewMarket {
    Current,
    New,
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

/// trait that deals with when we switch from
/// current market to new market.
pub trait MarketSwitching {
    /// switch markets on the trade api.
    fn _switch_markets(&self);
    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>>;
    fn _new_mkt(&self) -> Arc<Mutex<MarketType>>;
}

/// trait that detects new events and potentially skips some.
pub trait TradeMarketDiscovery: MarketSwitching {
    /// indicator if there is a new market present.
    /// consumes the new market events to come to the last one.
    fn _new_market_event(&self, new_market_receiver: &Receiver<MarketType>) -> bool {
        // handling new market event - roll to the latest new market, ignore in between markets
        let mut new_market_event = false;
        let mut new_stock_mkt: MarketType = MarketType::new();

        while let Ok(new_potential_mkt) = new_market_receiver.try_recv() {
            new_market_event = true;
            new_stock_mkt = new_potential_mkt;
        }

        *self
            ._new_mkt()
            .lock()
            .expect("_new_market_event: Could not lock!") += &new_stock_mkt;

        new_market_event
    }
}
