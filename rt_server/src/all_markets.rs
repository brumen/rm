// use circular_buffer::CircularBuffer;
// use dashmap::DashMap;
use scc::HashMap as DashMap;
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
        let mut all_names = String::new();
        let _ = self.markets.iter_sync(|k: &String, _| {
            let a = format!("{},", k);
            all_names += &a;
            true
        });
        write!(f, "{:?}", all_names)
    }
}

impl<MT> AllMarkets<MT>
where
    MT: MarketTypeT + Clone + Send + Sync + fmt::Debug, // this will be fine since MT is an Arc.
    MT::MP: Clone,
{
    // list the markets that are currently held - in self.markets
    pub(crate) fn list_market_names(&self) -> Vec<String> {
        let mut mn = vec![];
        self.markets.iter_sync(|k: &String, _| {
            mn.push(k.clone()); // TODO: FIX THIS AT SOME POINT.
            true
        });
        mn
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
        let actual_market = self.markets.read_sync(market_name, |_, v| v.clone())?;
        Some(actual_market.clone()) // .clone here is OK, since we're using it on Arc (MT = Arc<...>)
    }

    // inserts the market into the all structure.
    pub(crate) fn insert(&self, market_name: String, market: MT) {
        self.markets.upsert_sync(market_name.clone(), market);
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
        let mut active_markets = vec!["future".to_string()];
        self.processor_market_map.iter_sync(|_, v| {
            active_markets.push(v.clone());
            true
        });

        self.markets
            .retain_sync(|mn, _| active_markets.iter().any(|am| am == mn));
        debug!("Processor-market map: {:?}", self.processor_market_map);
    }

    // this is when the processor simply changes the market.
    pub(crate) fn insert_processor(&self, processor_name: String, market_name: String) {
        // let prev_processor_mkt = self.processor_market_map.get(&processor_name);
        // self
        //     .processor_market_map
        //     .insert(processor_name, market_name);

        // match prev_processor_mkt {
        //     None => {},
        //     Some(prev_used_mkt) => {
        //         self.all_markets
        //     }

        let _ = self
            .processor_market_map
            .upsert_sync(processor_name, market_name);
        // go through the market names and remove the markets
        self._clean_markets();
    }

    // this is when the processor changes to a new market market_name w/ market
    pub(crate) fn insert_both(&self, processor_name: String, market_name: String, market: MT) {
        self.insert(market_name.clone(), market);
        self.insert_processor(processor_name, market_name);
    }

    // returns the market params of some market in the collection
    // #[allow(dead_code)]
    // pub(crate) fn get_market_params(&self) -> Option<MT::MP> {
    //     //
    //     if self.markets.is_empty() {
    //         return None;
    //     }

    //     // we have at least one market.
    //     let market_elt = self.markets.iter().nth(0)?;
    //     let mo = market_elt.value();

    //     Some(mo.market_params().clone())
    // }
}
