use std::collections::{HashMap, hash_map::IntoIter,};
use std::ops::{Deref, DerefMut, };
use serde::{Serialize, Deserialize};
use kafka::consumer::Message;
use std::iter::IntoIterator;
use std::io;
use thiserror::Error;

use crate::ref_deref_trait;
use crate::ref_deref::TryFromRef;

// market information = ((flight, market date), value)
// MK ... mnemonic for market key
// MK used to be (String, Date), now it's generic,
//    it has to be hashable, and it copyable for now
pub type MarketInner = HashMap<String, f64>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketType (
    pub MarketInner
);

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

#[derive(Error, Debug)]
pub enum MarketTypeError {
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to MarketType")]
    CantConvertToMarket(#[from] serde_json::Error),
}


impl<'a> TryFromRef<Message<'a>> for MarketType {
    type Error = MarketTypeError;

    fn try_from_ref(value: &Message<'a>) -> Result<Self, Self::Error> {

        let msg_utf = std::str::from_utf8(value.value)?;

        Ok(serde_json::from_str::<MarketType>(msg_utf)?)

    }
}
