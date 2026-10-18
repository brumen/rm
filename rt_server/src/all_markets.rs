use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use crate::market::MarketTypeT;

/// Keeps a named market in the registry while a message is queued or being handled.
/// Clones share ownership; unused markets are pruned on the next processor update.
#[derive(Clone, Debug)]
pub(crate) struct MarketLease {
    name: String,
    _retention: Arc<()>,
}

impl MarketLease {
    pub(crate) fn name(&self) -> &String {
        &self.name
    }
}

#[derive(Debug)]
struct StoredMarket<MT> {
    market: MT,
    retention: Arc<()>,
}

#[derive(Debug)]
struct Registry<MT> {
    markets: BTreeMap<String, StoredMarket<MT>>,
    processor_market_map: BTreeMap<String, String>,
}

impl<MT> Registry<MT> {
    fn insert(&mut self, name: String, market: MT) {
        // Preserve leases if an existing name is updated.
        if let Some(stored) = self.markets.get_mut(&name) {
            stored.market = market;
        } else {
            self.markets.insert(
                name,
                StoredMarket {
                    market,
                    retention: Arc::new(()),
                },
            );
        }
    }

    fn prune(&mut self) {
        let referenced: HashSet<_> = self.processor_market_map.values().collect();
        self.markets.retain(|name, stored| {
            name == "future"
                || referenced.contains(name)
                || Arc::strong_count(&stored.retention) > 1
        });
    }
}

/// Market storage and processor ownership are updated together. No registry guard
/// escapes this type, and pricing and publishing run after guards are released.
#[derive(Debug)]
pub(crate) struct AllMarkets<MT: fmt::Debug> {
    registry: RwLock<Registry<MT>>,
}

impl<MT: fmt::Debug> fmt::Display for AllMarkets<MT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.registry.try_read() {
            Ok(registry) => write!(f, "{:?}", registry.markets.keys().collect::<Vec<_>>()),
            Err(_) => f.write_str("AllMarkets (being updated)"),
        }
    }
}

impl<MT> AllMarkets<MT>
where
    MT: MarketTypeT + Clone + Send + Sync + fmt::Debug,
    MT::MP: Clone,
{
    pub(crate) async fn market_size(&self) -> Vec<(String, usize)> {
        let markets: Vec<_> = self
            .registry
            .read()
            .await
            .markets
            .iter()
            .map(|(name, stored)| (name.clone(), stored.market.clone()))
            .collect();
        markets
            .into_iter()
            .map(|(name, market)| (name, market.len()))
            .collect()
    }

    pub(crate) async fn list_market_names(&self) -> Vec<String> {
        self.registry.read().await.markets.keys().cloned().collect()
    }

    #[allow(dead_code)]
    pub(crate) async fn list_processor_names(&self) -> Vec<String> {
        self.registry
            .read()
            .await
            .processor_market_map
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) async fn processor_market_snapshot(&self) -> BTreeMap<String, String> {
        self.registry.read().await.processor_market_map.clone()
    }

    pub(crate) fn new() -> Self {
        Self {
            registry: RwLock::new(Registry {
                markets: BTreeMap::new(),
                processor_market_map: BTreeMap::new(),
            }),
        }
    }

    pub(crate) async fn get(&self, market_name: &String) -> Option<MT> {
        self.registry
            .read()
            .await
            .markets
            .get(market_name)
            .map(|stored| stored.market.clone())
    }

    /// Acquiring a lease and pruning use the same lock, so deletion cannot race
    /// with a message acquiring ownership.
    pub(crate) async fn lease(&self, market_name: &String) -> Option<MarketLease> {
        let registry = self.registry.read().await;
        let stored = registry.markets.get(market_name)?;
        Some(MarketLease {
            name: market_name.clone(),
            _retention: stored.retention.clone(),
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn get_processor(&self, processor_name: &String) -> Option<String> {
        self.registry
            .read()
            .await
            .processor_market_map
            .get(processor_name)
            .cloned()
    }

    pub(crate) async fn insert(&self, market_name: String, market: MT) {
        self.registry
            .write()
            .await
            .insert(market_name.clone(), market);
        debug!(
            "Inserting {} into all_market. All_markets: {:?}",
            market_name,
            self.list_market_names().await
        );
    }

    /// Update ownership and prune under one write lock, including message leases.
    pub(crate) async fn insert_processor(&self, processor_name: String, market_name: String) {
        let mut registry = self.registry.write().await;
        registry
            .processor_market_map
            .insert(processor_name, market_name);
        registry.prune();
    }

    #[allow(dead_code)]
    pub(crate) async fn remove_processor(&self, processor_name: &str) {
        let mut registry = self.registry.write().await;
        registry.processor_market_map.remove(processor_name);
        registry.prune();
    }

    /// Insert the market, register its owner, and prune as one operation.
    pub(crate) async fn insert_both(
        &self,
        processor_name: String,
        market_name: String,
        market: MT,
    ) {
        let mut registry = self.registry.write().await;
        registry.insert(market_name.clone(), market);
        registry
            .processor_market_map
            .insert(processor_name, market_name);
        registry.prune();
    }
}

#[cfg(test)]
mod tests {
    use super::AllMarkets;
    use crate::market::MarketTypeT;
    use ractor::async_trait;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Barrier;

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

        fn stock_names(&self) -> Vec<Self::MK> {
            vec![]
        }

        fn len(&self) -> usize {
            0
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
    async fn test_processor_market_snapshot_empty() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();
        assert!(all.processor_market_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn test_processor_market_snapshot_sorted_and_owned() {
        let all: AllMarkets<Arc<TestMarket>> = AllMarkets::new();
        all.insert_processor("processor-b".into(), "market-1".into())
            .await;
        all.insert_processor("processor-a".into(), "market-1".into())
            .await;

        let snapshot = all.processor_market_snapshot().await;
        assert_eq!(
            snapshot.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["processor-a", "processor-b"]
        );
        assert_eq!(snapshot["processor-a"], "market-1");
        assert_eq!(snapshot["processor-b"], "market-1");

        all.insert_processor("processor-a".into(), "market-2".into())
            .await;
        assert_eq!(snapshot["processor-a"], "market-1");
        assert_eq!(
            all.processor_market_snapshot().await["processor-a"],
            "market-2"
        );
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
        all.insert("m3".to_string(), m3).await;

        all.insert_processor("p1".to_string(), "m1".to_string())
            .await;
        all.insert_both("p2".to_string(), "m2".to_string(), m2)
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
        assert!(names.contains(&"m2".to_string()));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_insert_and_registration_keep_all_referenced_markets() {
        let all = Arc::new(AllMarkets::<Arc<TestMarket>>::new());
        let start = Arc::new(Barrier::new(16));
        let mut handles = Vec::new();
        for worker in 0..16 {
            let all = all.clone();
            let start = start.clone();
            handles.push(tokio::spawn(async move {
                start.wait().await;
                let processor = format!("p{worker}");
                for update in 0..100 {
                    let market = format!("m{worker}-{update}");
                    all.insert_both(
                        processor.clone(),
                        market.clone(),
                        TestMarket::new(market.clone(), ()),
                    )
                    .await;
                    tokio::task::yield_now().await;
                    // Exercise registration/pruning concurrently with other insertions.
                    all.insert_processor(processor.clone(), market.clone())
                        .await;
                    assert!(all.get(&market).await.is_some(), "active market was pruned");
                }
            }));
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            for handle in handles {
                handle.await.unwrap();
            }
        })
        .await
        .expect("concurrent registry updates stalled");
        let referenced: HashSet<_> = all
            .processor_market_snapshot()
            .await
            .into_values()
            .collect();
        let remaining: HashSet<_> = all.list_market_names().await.into_iter().collect();
        assert_eq!(remaining, referenced);
        assert_eq!(remaining.len(), 16);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_updates_to_same_processor_keep_final_market() {
        let all = Arc::new(AllMarkets::<Arc<TestMarket>>::new());
        let start = Arc::new(Barrier::new(16));
        let mut handles = Vec::new();
        for worker in 0..16 {
            let all = all.clone();
            let start = start.clone();
            handles.push(tokio::spawn(async move {
                start.wait().await;
                for update in 0..100 {
                    let market = format!("m{worker}-{update}");
                    all.insert_both("p1".into(), market.clone(), TestMarket::new(market, ()))
                        .await;
                    tokio::task::yield_now().await;
                }
            }));
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            for handle in handles {
                handle.await.unwrap();
            }
        })
        .await
        .expect("concurrent ownership changes stalled");
        let market = all.get_processor(&"p1".into()).await.unwrap();
        assert!(all.get(&market).await.is_some());
        assert_eq!(all.list_market_names().await, vec![market]);
    }

    #[tokio::test]
    async fn test_queued_market_survives_until_receiver_registers_it() {
        let all = AllMarkets::<Arc<TestMarket>>::new();
        all.insert_both(
            "new".into(),
            "first".into(),
            TestMarket::new("first".into(), ()),
        )
        .await;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(all.lease(&"first".into()).await.unwrap())
            .unwrap();
        all.insert_both(
            "new".into(),
            "second".into(),
            TestMarket::new("second".into(), ()),
        )
        .await;
        assert!(all.get(&"first".into()).await.is_some());
        let lease = receiver.recv().await.unwrap();
        all.insert_processor("curr".into(), lease.name().clone())
            .await;
        drop(lease);
        // The receiver's registration takes over from the message lease.
        all.insert_processor("new".into(), "second".into()).await;
        assert!(all.get(&"first".into()).await.is_some());
        all.insert_processor("curr".into(), "second".into()).await;
        assert!(all.get(&"first".into()).await.is_none());
    }

    #[tokio::test]
    async fn test_forwarded_and_dropped_messages_release_unused_markets() {
        let all = AllMarkets::<Arc<TestMarket>>::new();
        all.insert_both(
            "new".into(),
            "first".into(),
            TestMarket::new("first".into(), ()),
        )
        .await;
        let message = all.lease(&"first".into()).await.unwrap();
        let forwarded = message.clone();
        all.insert_both(
            "new".into(),
            "second".into(),
            TestMarket::new("second".into(), ()),
        )
        .await;
        drop(message);
        all.insert_processor("new".into(), "second".into()).await;
        assert!(all.get(&"first".into()).await.is_some());
        drop(forwarded);
        all.insert_processor("new".into(), "second".into()).await;
        assert!(all.get(&"first".into()).await.is_none());
        assert!(all.lease(&"missing".into()).await.is_none());
    }

    #[tokio::test]
    async fn test_updating_market_preserves_existing_leases_and_future() {
        let all = AllMarkets::<Arc<TestMarket>>::new();
        all.insert("future".into(), TestMarket::new("latest".into(), ()))
            .await;
        all.insert_both(
            "new".into(),
            "first".into(),
            TestMarket::new("first".into(), ()),
        )
        .await;
        let lease = all.lease(&"first".into()).await.unwrap();
        all.insert("first".into(), TestMarket::new("replacement".into(), ()))
            .await;
        all.insert_both(
            "new".into(),
            "second".into(),
            TestMarket::new("second".into(), ()),
        )
        .await;
        assert_eq!(
            all.get(&"first".into()).await.unwrap().market_name(),
            "replacement"
        );
        assert!(all.get(&"future".into()).await.is_some());
        drop(lease);
        all.remove_processor("new").await;
        assert_eq!(all.list_market_names().await, vec!["future"]);
    }
}
