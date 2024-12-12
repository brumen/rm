use rdkafka::message::{BorrowedMessage, Message};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::IntoIter, HashMap};
use std::ops::{AddAssign, Deref, DerefMut};
use std::sync::mpsc::{Receiver, Sender};
use tracing::debug;

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

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
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

impl TryFromRef<BorrowedMessage<'_>> for MarketType {
    type Error = MarketTypeError;

    fn try_from_ref(value: &BorrowedMessage) -> Result<Self, Self::Error> {
        let msg_val = value.payload().unwrap(); // TODO: CAN WE DO THIS WITHOUT DETACHING???
        let msg_utf = std::str::from_utf8(msg_val)?;

        debug!("try_from_ref(MarketType): Msg = {:?}", msg_utf);
        Ok(serde_json::from_str::<MarketType>(msg_utf)?)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum CurrNewMarket {
    Current,
    New,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum MarketGeneral {
    MarketRemote(CurrNewMarket),
    MarketLocal(MarketType),
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
    fn _switch_all_markets(&self) -> impl std::future::Future<Output = ()> + Send;
    fn _switch_new_fut_markets(&self) -> impl std::future::Future<Output = ()> + Send;

    /// switches curr <- new; new <- future
    fn _internal_switch_all_markets(&self) {
        // replace current market w/ new market
        **self
            ._curr_mkt()
            .lock()
            .expect("_switch_markets: Could not lock current market") = self
            ._new_mkt()
            .lock()
            .expect("_switch_markets: Could not lock new market.")
            .clone();

        // replace new market with future market
        **self._new_mkt().lock().expect("Could not lock new market") = self
            ._future_mkt()
            .lock()
            .expect("_internal_switch_markets: Could not lock future market")
            .clone();
    }

    /// switches only new_market <- future_market
    fn _internal_switch_new_fut_markets(&self) {
        // replace current market with new market
        **self._new_mkt().lock().expect("Could not lock new market") = self
            ._future_mkt()
            .lock()
            .expect("_internal_switch_markets: Could not lock future market")
            .clone();
    }

    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>>;
    fn _new_mkt(&self) -> Arc<Mutex<MarketType>>;
    fn _future_mkt(&self) -> Arc<Mutex<MarketType>>;
    // is future market ready, i.e. is there any update to the futures market.
    fn _future_mkt_ready(&self) -> bool;
}

/// trait that detects new events and potentially skips some.
pub trait TradeMarketDiscovery: MarketSwitching {
    /// indicator if there is a new market present.
    /// consumes the new market events to come to the last one.
    fn _new_market_event(
        &self,
        new_market_receiver: &Receiver<MarketType>,
        fut_market_sender: &Sender<MarketType>,
    ) {
        // handling new market event - roll to the latest new market, ignore in between markets

        while let Ok(new_stock_mkt) = new_market_receiver.recv() {
            *self
                ._future_mkt()
                .lock()
                .expect("_new_market_event: Could not lock!") += &new_stock_mkt;
            let fm = self._future_mkt().lock().unwrap().clone();
            let _ = fut_market_sender.send(MarketType(fm));
        }
    }
}
