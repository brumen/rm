use ractor::async_trait;
use rdkafka::message::{BorrowedMessage, Message};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;
use std::collections::{hash_map::IntoIter, HashMap};
use std::ops::{AddAssign, Deref, DerefMut};
use std::sync::mpsc::Sender;
use tracing::{debug, info, warn};
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

pub(crate) type MarketInner = HashMap<String, f64>;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct MarketType {
    pub market_name: String,
    pub market: MarketInner,
}

// TODO: THIS NEEDS TO BE FIXED.
impl Into<MarketInner> for MarketType {
    fn into(self) -> MarketInner {
        self.market.into_iter().map(|x| (x.0, x.1)).collect()
    }
}

// making a MarketType an iterator.
impl IntoIterator for MarketType {
    type Item = (String, f64);
    type IntoIter = IntoIter<String, f64>;

    fn into_iter(self) -> Self::IntoIter {
        self.market.into_iter()
    }
}

// ref_deref_trait!(MarketType, MarketInner);

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

    pub(crate) fn next_market(&self, mn: &AllMarkets) -> Option<Self> {
	mn.above_market(&self.market_name)
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
        for (ticker, value) in rhs.market.iter() {
            self.market.insert(ticker.clone(), *value);
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

impl TryFromRef<BorrowedMessage<'_>> for MarketType {
    type Error = MarketTypeError;

    fn try_from_ref(value: &BorrowedMessage) -> Result<Self, Self::Error> {
        let msg_val = value.payload().unwrap(); // TODO: CAN WE DO THIS WITHOUT DETACHING???
        let msg_utf = std::str::from_utf8(msg_val)?;

        debug!("try_from_ref(MarketType): Msg = {:?}", msg_utf);
        Ok(serde_json::from_str::<MarketType>(msg_utf)?)
    }
}


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


#[async_trait]
pub trait MarketSwitching {

    fn all_markets(&self) -> Arc<AllMarkets>;

    /// endpoint where the market is posted.
    ///   could be for current, new or any other
    ///   market.
    ///   E.g. format!("http://{0}/market", pricing_server))
    fn market_endpoint(&self) -> String;

    /// reqwest client to implement market switching
    ///   if we dont need the request client, set it to None.
    fn r_client(&self) -> Option<&reqwest::Client>;

    /// Sets market_name to the market providedcurrent and new markets to the ones
    ///   specified in this function.
    ///   market: market to replace the existing market_name
    ///   market_name: name of the market to be replaced
    ///   implements: market_name <- market
    async fn set_market(
	&self,
	market: MarketType,
	market_name: &mut MarketType,
    ) -> Result<(), reqwest::Error> {

        info!("Setting market for {:?}", market_name.market_name);

        match self.r_client() {

            None => {
                *market_name = market;
            },

            Some(client) => {
                let internal_market_name = &market_name.market_name;

	        let payload = json!({
	            "market": market,
	            "market_type": internal_market_name,
	        });

                client
	            .post(self.market_endpoint())
                    .json(&payload)
                    .send()
                    .await?;
            },
        }
        Ok(())
    }

    /// switches market_below w/ market_above
    ///   market_name_below <- market_name_above
    async fn _switch_markets(
	&self,
	market_name_below: &mut MarketType,
	market_name_above: &MarketType,
    ) -> Result<(), reqwest::Error> {
        info!(
	    "Switching markets {:?} <- {:?}",
	    market_name_below.market_name,
	    market_name_above.market_name,
	);


        match self.r_client() {

            Some(client) => {

	        // set the market below
	        let payload = json!({
	            "market_below": market_name_below.market_name,
	            "market_above": market_name_above.market_name,
	        });

                // replace market with switch_market in the endpoint
                let switch_market_endpoint = str::replace(
                    self.market_endpoint().as_str(), "market", "switch_market"
                );

                client
	            .post(switch_market_endpoint)
                    .json(&payload)
                    .send()
                    .await?;

            },

            None => {
                // TODO: CHECK THIS FOR NOW.
                market_name_below.market = market_name_above.market.clone();  // leave name the same
            },
        }
        Ok(())
    }

    async fn switch_market(
	&self,
	market_name: &mut MarketType,
    ) -> Result<(), reqwest::Error> {

	match market_name.next_market(&self.all_markets()) {
	    None => {
		warn!(
		    "Could not find next market of {}. All markets: {:?}, Leaving as it is.",
		    market_name.market_name, self.all_markets(),
		);
		return Ok(());
	    },
	    Some(above_market) => {
		self._switch_markets(market_name, &above_market).await?
	    }
	}

	Ok(())
    }
}


/// list of (market names, actual market)
#[derive(Debug)]
pub(crate) struct AllMarkets(Vec<MarketType>);

impl AllMarkets {

    /// Default implemnentation of the market names.
    pub(crate) fn new(nb_middle: usize) -> Self {
	let mut middle_markets = vec![];
	middle_markets.push(MarketType::new("current".to_string()));
	for middle_nb in 0..nb_middle {
	    middle_markets.push(
		MarketType::new(format!("new_{middle_nb}"))
	    );
	}
	middle_markets.push(MarketType::new("new".to_string()));

	Self(middle_markets)
    }

    pub(crate) fn get(&self, market_nb: usize) -> &String {
	&self.0[market_nb].market_name
    }

    /// attempts to find the market name in the AllMarkets -
    /// if it cant find it, returns None
    fn _find_market(&self, mkt_name: &String) -> Option<usize> {
	self.0.iter().position(|r| r.market_name == *mkt_name)
    }

    /// finds the market above
    /// returns None if it's already the last market.
    pub(crate) fn above_market(&self, mkt_name: &String) -> Option<MarketType> {

	match self._find_market(mkt_name) {
	    None => None,
	    Some(found_mkt_nb) => {
		if found_mkt_nb == self.0.len() - 1 {
		    return None
		}
		Some(self.0[found_mkt_nb + 1].clone())
	    }
	}
    }

    pub(crate) fn len(&self) -> usize {
	self.0.len()
    }
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
