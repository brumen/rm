//! Pricing runs outside actor handlers. Parallelism and execution time are bounded.
use crate::market::MarketTypeT;
use crate::portfolio::{PmPortfolio, PricingResults};
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_msg::TradeEvent;
use futures::{stream, FutureExt, StreamExt};
use std::sync::Arc;

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PricingStyle {
    Sequential,
    ParallelSingleThread,
    Parallel,
}

/// Price immutable trade versions on one captured market for the selected metrics.
///
/// Sequential mode evaluates one trade at a time. The parallel modes keep at
/// most sixteen trade computations in flight, either within this task or in
/// separate Tokio tasks. Each outcome retains its input event so the actor can
/// apply the correct version. Missing PV/PnL prices and panics become errors;
/// empty PV01 is a valid zero exposure under the existing pricing API.
///
/// The calling worker enforces the job deadline. Cancelling this future also
/// aborts any child pricing tasks through their abort-on-drop guards.
pub(crate) async fn price_batch<T, MT>(
    events: Vec<Arc<TradeEvent<T>>>,
    metrics: Vec<PricingMetric>,
    market: Arc<MT>,
    style: PricingStyle,
) -> Vec<(Arc<TradeEvent<T>>, Result<PmPortfolio, String>)>
where
    T: PriceTrade<MT> + Clone + Send + Sync + 'static,
    MT: MarketTypeT + Send + Sync + 'static,
{
    let parallel = matches!(style, PricingStyle::Parallel);
    let concurrency = if matches!(style, PricingStyle::Sequential) {
        1
    } else {
        16
    };
    stream::iter(events)
        .map(|event| {
            let metrics = metrics.clone();
            let market = market.clone();
            async move {
                let value = event.value.clone();
                let id = event.id.clone();
                let calculate = async move {
                    let mut trade = value;
                    let mut result = PmPortfolio::new();
                    for metric in metrics {
                        let value = trade.value_by_metric(&metric, market.clone()).await;
                        // Empty PV01 is valid zero exposure under PriceTrade's API.
                        let covered = match &value {
                            PricingResults::PV(p) | PricingResults::PnL(p) => p.contains_key(&id),
                            PricingResults::PV01(_) => true,
                        };
                        if !covered {
                            return Err(format!("No {metric} price for {id}"));
                        }
                        result.insert(metric, value.aggregate());
                    }
                    Ok(result)
                };
                let result = if parallel {
                    let mut task = AbortOnDrop(tokio::spawn(calculate));
                    (&mut task.0)
                        .await
                        .unwrap_or_else(|e| Err(format!("Pricing task failed: {e}")))
                } else {
                    std::panic::AssertUnwindSafe(calculate)
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| Err("Pricing task panicked".into()))
                };
                (event, result)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await
}
