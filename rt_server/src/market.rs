use log::debug;
use std::collections::{HashMap, hash_map::IntoIter,};
use std::ops::{Deref, DerefMut, };
use serde::{Serialize, Deserialize};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, };

//use reqwest::blocking::Client;
use std::sync::mpsc::Sender;

use std::sync::{Arc, Mutex,};
use std::iter::IntoIterator;
use thiserror::Error;

use crate::ref_deref_trait;
use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;


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


impl TryFromRef<Message<'_>> for MarketType {
    type Error = MarketTypeError;

    fn try_from_ref(value: &Message) -> Result<Self, Self::Error> {

        let msg_utf = std::str::from_utf8(value.value)?;

        Ok(serde_json::from_str::<MarketType>(msg_utf)?)

    }
}


#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
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
    pub curr_mkt : Arc<Mutex<MarketType>>,
    pub new_mkt_sender: Sender<MarketType>,
}


#[derive(Debug, Clone)]
pub struct AOStruct {
    pub mkt_sender: Sender<MarketType>,
}

#[derive(Debug, Clone)]
pub enum MktMsgParams {
    AOParams(AOStruct),
    LETFParams(LETFP),
}


pub trait MktEventHandler : Streaming {

    fn _handle_mkt_msg(
        &self,
        mkt_msg: &Message,
        mkt_params: MktMsgParams,
    );

    /// Loop that handles the market events
    /// mkt_topic - receiving market events from this topic
    /// new_mkt_sender - sending the new market to the pricing api
    /// switch_mkt_recv - receiver receiving the event when to switch markets.
    fn _handle_mkt_events (
        &self,
        mkt_topic: String,
        mkt_params: MktMsgParams,
    ) {
        let mut mkt_listener_ = Consumer::from_hosts(vec![format!(
            "{}:{}",
            self.kafka_server_name(), self.kafka_port()
        )])
        .with_topic_partitions(mkt_topic.to_owned(), &[0])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

        //let mkt_update_client = Client::new();

        debug!("Entering the _handle_mkt_events loop.");
        loop {
            for mkt_msg_set in mkt_listener_.poll().unwrap().iter() {  // TODO: What to do w/ unwrap here??
                for mkt_msg in mkt_msg_set.messages() {
                    debug!("Getting new markets from {mkt_topic}.");
                    self._handle_mkt_msg(mkt_msg, mkt_params.clone());
             }
                let _ = mkt_listener_.consume_messageset(mkt_msg_set);
            }
            mkt_listener_.commit_consumed().unwrap();
        }
    }
}
