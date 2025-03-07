use ractor::async_trait;
use rdkafka::message::{BorrowedMessage, Message};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{hash_map::IntoIter, HashMap};
use std::ops::{AddAssign, Deref, DerefMut};
use std::sync::mpsc::Sender;
use tracing::{debug, info};
use std::fmt;

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

// MarketRef is market reference, so that not the entire
// market but only the reference to that market is
// passed around.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CurrNewMarket(pub String);

impl fmt::Display for CurrNewMarket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for CurrNewMarket {
    type Target = String;

    fn deref(&self) -> &Self::Target {
	&self.0
    }
}

impl CurrNewMarket {

    pub fn next_market(&self, mn: &AllMarkets) -> Option<Self> {
	mn.above_market(self.0.clone())
    }
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

pub(crate) enum MarketNames {
    Current(String),
    Middle(String),
    New(String),
}

/// first elt is Current
/// second element is Middle vector
/// third element is the New market
#[derive(Debug)]
pub(crate) struct AllMarkets(Vec<String>);

impl AllMarkets {

    /// Default implemnentation of the market names.
    pub fn new(nb_middle: usize) -> Self {
	let mut middle_markets = vec![];
	middle_markets.push("current".to_string());
	for middle_nb in 0..nb_middle {
	    middle_markets.push(
		format!("new_{middle_nb}")
	    );
	}
	middle_markets.push("new".to_string());

	Self(middle_markets)
    }

    pub fn get(&self, market_nb: usize) -> String {
	self.0[market_nb].clone()
    }

    /// attempts to find the market name in the AllMarkets -
    /// if it cant find it, returns None
    fn _find_market(&self, mkt_name: String) -> Option<usize> {
	self.0.iter().position(|r| *r == mkt_name)
    }

    /// finds the market above
    /// returns None if it's already the last market.
    pub(crate) fn above_market(&self, mkt_name: String) -> Option<CurrNewMarket> {

	match self._find_market(mkt_name) {
	    None => None,
	    Some(found_mkt_nb) => {
		if found_mkt_nb == self.0.len() - 1 {
		    return None
		}
		Some(CurrNewMarket(self.0[found_mkt_nb + 1].clone()))
	    }
	}
    }

    pub(crate) fn len(&self) -> usize {
	self.0.len()
    }
}


#[async_trait]
pub trait MarketSwitching {

    fn all_markets(&self) -> Arc<AllMarkets>;
    
    /// endpoint where the market is posted.
    ///   could be for current, new or any other
    ///   market.
    ///   E.g. format!("http://{0}/market", pricing_server))
    fn market_endpoint(&self) -> String;

    /// reqwest client to implement market switching
    fn r_client(&self) -> &reqwest::Client;
    
    /// Sets market_name to the market providedcurrent and new markets to the ones
    ///   specified in this function.
    ///   market: market to replace the existing market_name
    ///   market_name: name of the market to be replaced
    async fn set_market(
	&self,
	market: MarketType,
	market_name: CurrNewMarket,
    ) -> Result<(), reqwest::Error> {
	

        info!("Setting market for {:?}", market_name.clone());

	let client = self.r_client();
	let payload = json!({
	    "market": market,
	    "market_type": market_name.clone(),
	});

        client
	    .post(self.market_endpoint())
            .json(&payload)
            .send()
            .await?;

	Ok(())
    }

    /// switches market_below w/ market_above
    async fn _switch_markets(
	&self,
	market_name_below: CurrNewMarket,
	market_name_above: CurrNewMarket,
    ) -> Result<(), reqwest::Error> {
	

        info!(
	    "Switching markets {:?} <- {:?}",
	    market_name_below.clone(),
	    market_name_above.clone(),
	);

	let client = self.r_client();
		
	// set the market below
	let payload = json!({
	    "market_below": *market_name_below,
	    "market_above": *market_name_above,
	});

        client
	    .post(self.market_endpoint())  // TODO: ENDPOINT IS WRONG HERE!!!
            .json(&payload)
            .send()
            .await?;

	Ok(())
    }

    async fn switch_market(
	&self,
	market_name: CurrNewMarket
    ) -> Result<(), reqwest::Error> {

	match market_name.next_market(&self.all_markets()) {
	    None => {
		info!(
		    "Could not find next market of {}. Nothing to do.",
		    market_name,
		);
		return Ok(());
	    },
	    Some(above_market) => {
		self._switch_markets(market_name, above_market).await?;
	    }
	}

	Ok(())
    }
}
