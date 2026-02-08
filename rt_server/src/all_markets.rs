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
            all_names += &format!("{},", k);
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
        let mut market_names = vec![];
        self.markets.iter_sync(|market_name: &String, _| {
            market_names.push(market_name.clone()); // TODO: FIX THIS AT SOME POINT.
            true
        });
        market_names
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

#[cfg(test)]
mod tests {
    use super::AllMarkets;
    use crate::market::MarketTypeT;
    use ractor::async_trait;
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct TestMarket {
        name: String,
        params: (),
    }

    #[async_trait]
    impl MarketTypeT for TestMarket {
        type MP = ();
        type MK = String;

        fn new(market_name: String, mp: Self::MP) -> Arc<Self>
        where
            Self: Sized,
        {
            Arc::new(Self {
                name: market_name,
                params: mp,
            })
        }

        fn market_name(&self) -> String {
            self.name.clone()
        }

        async fn get(&self, _stock: &Self::MK) -> Option<f64> {
            None
        }

        async fn insert(&self, _key: Self::MK, _value: f64) {}

        fn is_empty(&self) -> bool {
            true
        }

        fn market_params(&self) -> Self::MP {
            self.params
        }

        // we wont be testing this trait.
        fn try_from_ref(
            market_name: String,
            value: &rdkafka::message::BorrowedMessage,
            mp: Self::MP,
        ) -> Result<Arc<Self>, crate::market::MarketTypeError>
        where
            Self: Sized,
        {
            todo!()
        }
    }

    #[test]
    fn test_new_is_empty() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();
        assert!(all.list_market_names().is_empty());
        assert!(all.get(&"anything".to_string()).is_none());
    }

    #[test]
    fn test_insert_and_get() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();

        let m1 = TestMarket::new("m1".to_string(), ());
        all.insert("m1".to_string(), m1.clone());

        let got = all.get(&"m1".to_string());
        assert!(got.is_some());
        assert_eq!(got.unwrap().market_name(), "m1".to_string());

        let names = all.list_market_names();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"m1".to_string()));
    }

    #[test]
    fn test_insert_processor_cleans_unused_markets_but_keeps_future() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();

        // Insert multiple markets, including the special "future"
        let future = TestMarket::new("future".to_string(), ());
        let m1 = TestMarket::new("m1".to_string(), ());
        let m2 = TestMarket::new("m2".to_string(), ());
        all.insert("future".to_string(), future);
        all.insert("m1".to_string(), m1);
        all.insert("m2".to_string(), m2);

        // Point processor to m1; should clean out m2 but keep m1 and future
        all.insert_processor("p1".to_string(), "m1".to_string());

        let names = all.list_market_names();
        assert!(names.contains(&"future".to_string()));
        assert!(names.contains(&"m1".to_string()));
        assert!(!names.contains(&"m2".to_string()));
    }

    #[test]
    fn test_insert_both_inserts_market_and_sets_processor_mapping() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();

        // Pre-insert a market that should be cleaned once processor is set
        let old = TestMarket::new("old".to_string(), ());
        all.insert("old".to_string(), old);

        let m1 = TestMarket::new("m1".to_string(), ());
        all.insert_both("p1".to_string(), "m1".to_string(), m1.clone());

        // Market should be retrievable
        let got = all.get(&"m1".to_string()).unwrap();
        assert_eq!(got.market_name(), "m1".to_string());

        // Processor mapping should exist
        let mapped = all
            .processor_market_map
            .read_sync(&"p1".to_string(), |_, v| v.clone());
        assert_eq!(mapped, Some("m1".to_string()));

        // Cleanup should have removed "old" (since it's not active and not "future")
        let names = all.list_market_names();
        assert!(names.contains(&"m1".to_string()));
        assert!(!names.contains(&"old".to_string()));
    }

    #[test]
    fn test_multiple_processors_keep_multiple_markets() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();

        let m1 = TestMarket::new("m1".to_string(), ());
        let m2 = TestMarket::new("m2".to_string(), ());
        let m3 = TestMarket::new("m3".to_string(), ());
        all.insert("m1".to_string(), m1);
        all.insert("m2".to_string(), m2);
        all.insert("m3".to_string(), m3);

        all.insert_processor("p1".to_string(), "m1".to_string());
        all.insert_processor("p2".to_string(), "m2".to_string());

        let names = all.list_market_names();
        assert!(names.contains(&"m1".to_string()));
        assert!(names.contains(&"m2".to_string()));
        assert!(!names.contains(&"m3".to_string()));
    }
}
