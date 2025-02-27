use ractor::async_trait;
use rdkafka::message::{BorrowedMessage, Message};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{hash_map::IntoIter, HashMap};
use std::ops::{AddAssign, Deref, DerefMut};
use std::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info};

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

//#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
//pub enum CurrNewMarket {
//    Current,
//    New,
//}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CurrNewMarket(pub String);


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


#[async_trait]
pub trait MarketSwitching {

    /// endpoint where the market is posted.
    ///   could be for current, new or any other
    ///   market.
    ///   E.g. format!("http://{0}/market", pricing_server))
    fn market_endpoint(&self) -> String;
    /// reqwest client to implement market switching
    fn r_client(&self) -> &reqwest::Client;  

    fn market_name(&self) -> CurrNewMarket;
    
    /// Sets current and new markets to the ones
    ///   specified in this function.
    ///   market: market to be replaced
    ///   market_name: name of the market to be 
    async fn switch_market(
	&self,
	market: MarketType,
    ) -> Result<(), reqwest::Error> {

        info!("Changing market for {:?}", self.market_name());

	let client = self.r_client();
	let payload = json!({
	    "market": market,
	    "market_type": self.market_name().clone(),
	});

        client
	    .post(self.market_endpoint())
            .json(&payload)
            .send()
            .await?;

	Ok(())
    }
}
