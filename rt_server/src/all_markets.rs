// use circular_buffer::CircularBuffer;
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use tracing::{debug, warn};

use crate::market::MarketTypeT;

const MN_LENGTH: usize = 100;

/// list of (market names, actual market)
// MT.. market type
// MP .. market params.
// MT = MarketTypeT<MP>
#[derive(Debug)]
pub(crate) struct AllMarkets<MT> {
    pub(crate) markets: DashMap<String, MT>,
    pub(crate) market_names: ArrayQueue<String>, // <usize, String>, // mapping of numbers to markets.
                                                 //    pub(crate) mp: Option<MT::MP>,
}

impl<MT> AllMarkets<MT>
where
    MT: MarketTypeT + Clone + Send + Sync, // this will be fine since MT is an Arc.
    MT::MP: Clone,
{
    pub(crate) fn list_market_names(&self) -> Vec<String> {
        self.markets
            .iter()
            .map(|mn_mv| {
                let mn = mn_mv.key();
                mn.clone()
            })
            .collect::<Vec<String>>()
    }

    // creates a new empty all markets structure
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            markets: DashMap::<String, MT>::new(),
            market_names: ArrayQueue::new(MN_LENGTH), // DashMap::<usize, String>::new(),
        }
    }

    // pub(crate) fn new2(market_1_name: String, market_1: MT) -> AllMarkets<Arc<MT>> {
    //     let new_dm = DashMap::<String, Arc<MT>>::new();
    //     let market_1_cast = Arc::new(market_1);
    //     new_dm.insert(market_1_name.clone(), market_1_cast);
    //     let new_mn = DashMap::<usize, String>::new();
    //     new_mn.insert(0, market_1_name);
    //     AllMarkets {
    //         markets: new_dm,
    //         market_names: new_mn,
    //     }
    // }

    pub(crate) fn get(&self, market_name: &String) -> Option<MT> {
        let actual_market = self.markets.get(market_name)?;
        Some(actual_market.value().clone()) // .clone here is OK, since we're using it on Arc (MT = Arc<...>)
    }

    // inserts the market into the all structure.
    pub(crate) fn insert(&self, market_name: String, market: MT) {
        self.markets.insert(market_name.clone(), market);
        debug!(
            "Inserting {} into all_market. All_markets: {:?}",
            market_name,
            self.list_market_names(),
        );
        self.market_names.push(market_name);
    }

    // pub(crate) fn get_market(&self, market_nb: &usize) -> Option<String> {
    //     let mn = self.market_names.get(market_nb)?;
    //     Some(mn.value().clone())
    // }

    /// attempts to find the market name in the AllMarkets -
    /// if it cant find it, returns None
    // fn _find_market(&self, mkt_name: &String) -> Option<usize> {
    //     self.0.iter().position(|r| r.market_name() == *mkt_name)
    // }

    // gets the reference to the market w/ the name
    // pub(crate) fn get_m(&self, market_name: &String) -> Option<MT> {
    //     let k = self.get(market_name)?;

    //     let m = k.value();

    //     Some(m)
    //     //market_ref.value()
    //     // Some(&self.0[market_nb])
    // }

    /// finds the market above
    /// returns None if it's already the last market.
    // pub(crate) fn above_market(&self, mkt_name: &String) -> Option<&dyn MarketTypeT<MP=MP>> {

    //     match self._find_market(mkt_name) {
    //         None => None,
    //         Some(found_mkt_nb) => {
    //     	if found_mkt_nb == self.0.len() - 1 {
    //     	    return None;
    //     	}
    //     	Some(&self.0[found_mkt_nb + 1])
    //         }
    //     }
    // }

    // pub(crate) fn next_market(&self, market_name: String) -> Option<&dyn MarketTypeT<MP=MP>> {
    //     self.above_market(&market_name)
    //}

    /// returns the market params of some market in the collection
    pub(crate) fn get_market_params(&self) -> Option<MT::MP> {
        //
        if self.markets.is_empty() {
            return None;
        }

        // we have at least one market.
        let market_elt = self.markets.iter().nth(0)?;
        let mo = market_elt.value();

        Some(mo.market_params().clone())
    }

    // remove the market from self.markets
    pub(crate) fn remove(&self, market_name: &String) {
        // check if there are non-zero users
        let Some(market_to_remove) = self.markets.get(market_name) else {
            warn!(
                "Attempting to remove {:?} but market isnt present in all_markets",
                market_name,
            );
            return;
        };

        if !market_to_remove.is_used() {
            debug!(
                "Removing {} from all_markets. Before deletion all_markets: {:?}",
                market_name,
                self.list_market_names()
            );
            self.markets.remove(market_name);
            self.market_names.pop();
        } else {
            warn!(
                "Market {} still used. Not deleting from all_markets.",
                market_name
            );
        }
    }

    // returns the market name corresponding to the largets number in self.market_names
    pub(crate) fn last_market_name(&self) -> Option<String> {
        let last_name = self.market_names.pop()?;
        self.market_names.push(last_name.clone());

        Some(last_name)
    }
}
