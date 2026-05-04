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
    pub(crate) async fn list_market_names(&self) -> Vec<String> {
        let mut market_names = vec![];
        self.markets
            .iter_async(|market_name: &String, _| {
                market_names.push(market_name.clone()); // TODO: FIX THIS AT SOME POINT.
                true
            })
            .await;
        market_names
    }

    pub(crate) async fn list_processor_names(&self) -> Vec<String> {
        let mut processor_names = vec![];
        self.processor_market_map
            .iter_async(|processor_name: &String, _| {
                processor_names.push(processor_name.clone()); // TODO: FIX THIS AT SOME POINT.
                true
            })
            .await;
        processor_names
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
    pub(crate) async fn get(&self, market_name: &String) -> Option<MT> {
        let actual_market = self
            .markets
            .read_async(market_name, |_, v| v.clone())
            .await?;
        Some(actual_market.clone()) // .clone here is OK, since we're using it on Arc (MT = Arc<...>)
    }

    pub(crate) async fn get_processor(&self, processor_name: &String) -> Option<String> {
        self.processor_market_map
            .read_async(processor_name, |_, v| v.clone())
            .await
    }

    // inserts the market into the all structure.
    pub(crate) async fn insert(&self, market_name: String, market: MT) {
        self.markets.upsert_async(market_name.clone(), market).await;
        debug!(
            "Inserting {} into all_market. All_markets: {:?}",
            market_name,
            self.list_market_names().await,
        );
    }

    /// Like `insert_processor`, but additionally prunes `self.markets` so that it only contains
    /// markets that are referenced by at least one processor in `processor_market_map`.
    ///
    /// This is useful if `self.markets` may contain many "stale" markets and you want the set of
    /// stored markets to reflect only what processors are actively using.
    pub(crate) async fn insert_processor(
        &self,
        processor_name: String,
        new_processor_market: String,
    ) {
        // First, update the processor -> market mapping (upsert).
        let _ = self
            .processor_market_map
            .upsert_async(processor_name, new_processor_market)
            .await;

        // Build the set of all markets referenced by any processor.
        let mut referenced_markets: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        self.processor_market_map
            .iter_async(|_proc_name: &String, market_name: &String| {
                referenced_markets.insert(market_name.clone());
                true
            })
            .await;

        // Remove any market in `self.markets` that is not referenced.
        let mut to_remove: Vec<String> = Vec::new();
        self.markets
            .iter_async(|market_name: &String, _| {
                if !referenced_markets.contains(market_name) {
                    to_remove.push(market_name.clone());
                }
                true
            })
            .await;

        for market_name in to_remove {
            if market_name == "future" {
                continue; // ignoring market future, dont prune.
            }
            debug!(
                "Pruning market: {}. (not referenced by any processor.)",
                market_name,
            );
            self.markets.remove_async(&market_name).await;
        }
    }

    // this is when the processor changes to a new market market_name w/ market
    pub(crate) async fn insert_both(
        &self,
        processor_name: String,
        market_name: String,
        market: MT,
    ) {
        self.insert(market_name.clone(), market).await;
        self.insert_processor(processor_name, market_name).await;
    }
}

#[cfg(test)]
mod tests {
    use super::AllMarkets;
    use crate::market::MarketTypeT;
    use ractor::async_trait;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Barrier};
    use std::thread;

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
            _market_name: String,
            _value: &rdkafka::message::BorrowedMessage,
            _mp: Self::MP,
        ) -> Result<Arc<Self>, crate::market::MarketTypeError>
        where
            Self: Sized,
        {
            todo!()
        }
    }

    #[tokio::test]
    async fn test_new_is_empty() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();
        assert!(all.list_market_names().await.is_empty());
        assert!(all.get(&"anything".to_string()).await.is_none());
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();

        let m1 = TestMarket::new("m1".to_string(), ());
        all.insert("m1".to_string(), m1.clone()).await;

        let got = all.get(&"m1".to_string()).await;
        assert!(got.is_some());
        assert_eq!(got.unwrap().market_name(), "m1".to_string());

        let names = all.list_market_names().await;
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"m1".to_string()));
    }

    #[tokio::test]
    async fn test_multiple_processors_keep_multiple_markets() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();

        let m1 = TestMarket::new("m1".to_string(), ());
        let m2 = TestMarket::new("m2".to_string(), ());
        let m3 = TestMarket::new("m3".to_string(), ());
        let m4 = TestMarket::new("m4".to_string(), ());
        all.insert("m1".to_string(), m1).await;
        all.insert("m2".to_string(), m2).await;
        all.insert("m3".to_string(), m3).await;

        all.insert_processor("p1".to_string(), "m1".to_string())
            .await;
        all.insert_processor("p2".to_string(), "m2".to_string())
            .await;
        assert_eq!(
            all.get_processor(&"p1".to_string()).await,
            Some(String::from("m1"))
        );
        assert_eq!(
            all.get_processor(&"p2".to_string()).await,
            Some(String::from("m2"))
        );

        let names = all.list_market_names().await;
        assert!(names.contains(&"m1".to_string()));
        //assert!(names.contains(&"m2".to_string()));
        assert!(!names.contains(&"m3".to_string()));

        all.insert("m4".to_string(), m4).await;
        all.insert_processor("p1".to_string(), "m4".to_string())
            .await;
        let names = all.list_market_names().await;
        // m1 is not referenced by any other market - remove it.
        assert!(!names.contains(&"m1".to_string()));
        assert!(names.contains(&"m4".to_string()));
        assert_eq!(
            all.get_processor(&"p1".to_string()).await,
            Some(String::from("m4"))
        );
    }

    #[tokio::test]
    async fn test_insert_processor_concurrent_same_processor() {
        let all: Arc<AllMarkets<Arc<TestMarket>>> = Arc::new(AllMarkets::new());

        let market_names: Vec<String> = (0..16).map(|i| format!("m{i}")).collect();
        for mn in &market_names {
            all.insert(mn.clone(), TestMarket::new(mn.clone(), ()))
                .await;
        }

        all.insert_processor("p1".to_string(), market_names[0].clone())
            .await;

        let n_threads = 32usize;
        let start = Arc::new(Barrier::new(n_threads));
        let mut handles = Vec::with_capacity(n_threads);

        for t in 0..n_threads {
            let all_c = all.clone();
            let start_c = start.clone();
            let mn = market_names[t % market_names.len()].clone();
            handles.push(thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
                start_c.wait();
                rt.block_on(async move {
                    all_c.insert_processor("p1".to_string(), mn).await;
                });
            }));
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        let final_market = all
            .get_processor(&"p1".to_string())
            .await
            .expect("p1 mapping missing after concurrent updates");
        assert!(
            market_names.contains(&final_market),
            "final market {final_market} not in expected set"
        );

        let remaining = all.list_market_names().await;
        for mn in &remaining {
            assert!(
                market_names.contains(mn),
                "unexpected market name in markets map: {mn}"
            );
        }

        assert!(
            remaining.len() <= market_names.len(),
            "markets map grew unexpectedly: remaining={} inserted={}",
            remaining.len(),
            market_names.len()
        );
    }

    #[tokio::test]
    async fn test_insert_processor_concurrent_many_processors_shared_market_pool() {
        let all: Arc<AllMarkets<Arc<TestMarket>>> = Arc::new(AllMarkets::new());

        let market_pool: Vec<String> = (0..8).map(|i| format!("m{i}")).collect();
        for mn in &market_pool {
            all.insert(mn.clone(), TestMarket::new(mn.clone(), ()))
                .await;
        }

        let processors: Vec<String> = (0..32).map(|i| format!("p{i}")).collect();

        for (i, p) in processors.iter().enumerate() {
            all.insert_processor(p.clone(), market_pool[i % market_pool.len()].clone())
                .await;
        }

        let n_threads = 64usize;
        let start = Arc::new(Barrier::new(n_threads));
        let mut handles = Vec::with_capacity(n_threads);

        for t in 0..n_threads {
            let all_c = all.clone();
            let start_c = start.clone();
            let processors_c = processors.clone();
            let market_pool_c = market_pool.clone();

            handles.push(thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
                start_c.wait();

                rt.block_on(async move {
                    for k in 0..200usize {
                        let p = &processors_c[(t + k) % processors_c.len()];
                        let m = &market_pool_c[(t * 7 + k) % market_pool_c.len()];
                        all_c.insert_processor(p.clone(), m.clone()).await;
                    }
                });
            }));
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        for p in &processors {
            let mapped = all
                .get_processor(p)
                .await
                .unwrap_or_else(|| panic!("missing mapping for processor {p}"));
            assert!(
                market_pool.contains(&mapped),
                "processor {p} mapped to unexpected market {mapped}"
            );
        }

        let remaining_markets: HashSet<String> =
            all.list_market_names().await.into_iter().collect();

        let mut referenced: HashSet<String> = HashSet::new();
        for p in &processors {
            if let Some(m) = all.get_processor(p).await {
                referenced.insert(m);
            }
        }

        for m in &remaining_markets {
            assert!(
                referenced.contains(m),
                "market {m} remains in markets map but is not referenced by any processor"
            );
        }

        let proc_names = all.list_processor_names().await;
        let proc_set: HashSet<String> = proc_names.into_iter().collect();
        let expected: HashSet<String> = processors.iter().cloned().collect();
        assert_eq!(
            proc_set, expected,
            "processor_market_map keys mismatch after concurrent updates"
        );

        let pool_set: HashSet<String> = market_pool.into_iter().collect();
        assert!(
            remaining_markets.is_subset(&pool_set),
            "remaining markets contain values outside the pool: remaining={:?} pool={:?}",
            remaining_markets,
            pool_set
        );

        let _ = HashMap::<String, String>::new();
    }
}
