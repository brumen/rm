// use circular_buffer::CircularBuffer;
use dashmap::DashMap;
use std::fmt;
use tracing::debug;

use crate::market::MarketTypeT;

/// list of (market names, actual market)
/// MT.. market type
/// MP .. market params.
/// MT = MarketTypeT<MP>
#[derive(Debug)]
pub(crate) struct AllMarkets<MT: fmt::Debug> {
    pub(crate) markets: DashMap<String, MT>,
    pub(crate) processor_market_map: DashMap<String, String>, // mapping between processor names and market names used.
                                                              //    pub(crate) mp: Option<MT::MP>,
}

impl<MT: fmt::Debug> fmt::Display for AllMarkets<MT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mn: Vec<_> = self.markets.iter().map(|kv| kv.key().to_string()).collect();
        write!(f, "{:?}", mn)
    }
}

impl<MT> AllMarkets<MT>
where
    MT: MarketTypeT + Clone + Send + Sync + fmt::Debug, // this will be fine since MT is an Arc.
    MT::MP: Clone,
{
    // list the markets that are currently held - in self.markets
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
            processor_market_map: DashMap::<String, String>::new(),
        }
    }

    /// gets the market with the name market_name
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
    }

    /// cleans the markets
    /// iterates through markets, and if it finds a name that is not in market_names,
    ///   it removes it. special treatment of "future" market
    fn _clean_markets(&self) {
        let mut active_markets = self
            .processor_market_map
            .iter()
            .map(|proc_mn| proc_mn.clone())
            .collect::<Vec<String>>();
        active_markets.push("future".to_string()); // special market

        self.markets.retain(|mn, _| {
            let is_mn_present = active_markets.iter().any(|am| am == mn);
            if !is_mn_present {
                debug!(
                    "MARKETS: {:?}, PROCESSORS: {:?}, DELETING: {}",
                    self.list_market_names(),
                    active_markets,
                    mn
                );
            }
            is_mn_present
        });
    }

    // this is when the processor simply changes the market.
    pub(crate) fn insert_processor(&self, processor_name: String, market_name: String) {
        let _ = self
            .processor_market_map
            .insert(processor_name, market_name);
        // go through the market names and remove the markets
        self._clean_markets();
    }

    // this is when the processor changes to a new market market_name w/ market
    pub(crate) fn insert_both(&self, processor_name: String, market_name: String, market: MT) {
        self.insert(market_name.clone(), market);
        self.insert_processor(processor_name, market_name);
    }

    /// returns the market params of some market in the collection
    #[allow(dead_code)]
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
}
