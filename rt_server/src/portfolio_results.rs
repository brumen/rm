use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::portfolio::PmPortfolio;
use crate::throttle::ThrottleSnapshot;

pub(crate) type PortfolioSnapshots = BTreeMap<String, BTreeMap<String, PortfolioSnapshot>>;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PortfolioMetricSnapshot {
    pub(crate) total: Option<f64>,
    pub(crate) trades: BTreeMap<String, Option<f64>>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PortfolioSnapshot {
    pub(crate) processor_name: String,
    pub(crate) market: Option<String>,
    pub(crate) phase: String,
    pub(crate) updated_at_ms: i64,
    pub(crate) metrics: BTreeMap<String, PortfolioMetricSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) throttle: Option<ThrottleSnapshot>,
}

#[derive(Debug, Default)]
pub(crate) struct PortfolioResults {
    snapshots: RwLock<PortfolioSnapshots>,
}

impl PortfolioResults {
    pub(crate) fn snapshot(&self) -> PortfolioSnapshots {
        self.snapshots
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn reporter(
        self: &Arc<Self>,
        pipeline: String,
        processor: &str,
        processor_name: String,
    ) -> PortfolioReporter {
        PortfolioReporter {
            results: self.clone(),
            pipeline,
            processor: processor.to_string(),
            processor_name,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PortfolioReporter {
    results: Arc<PortfolioResults>,
    pipeline: String,
    processor: String,
    processor_name: String,
}

impl PortfolioReporter {
    pub(crate) fn record(&self, market: Option<&str>, phase: &str, portfolio: &PmPortfolio) {
        self.record_with_throttle(market, phase, portfolio, None);
    }

    pub(crate) fn record_with_throttle(
        &self,
        market: Option<&str>,
        phase: &str,
        portfolio: &PmPortfolio,
        throttle: Option<ThrottleSnapshot>,
    ) {
        let metrics = portfolio
            .iter()
            .map(|(metric, values)| {
                let total: f64 = values.values().sum();
                let trades = values
                    .iter()
                    .map(|(trade, value)| (trade.clone(), value.is_finite().then_some(*value)))
                    .collect();
                (
                    metric.to_string(),
                    PortfolioMetricSnapshot {
                        total: total.is_finite().then_some(total),
                        trades,
                    },
                )
            })
            .collect();
        let snapshot = PortfolioSnapshot {
            processor_name: self.processor_name.clone(),
            market: market.map(str::to_string),
            phase: phase.to_string(),
            updated_at_ms: chrono::Utc::now().timestamp_millis(),
            metrics,
            throttle,
        };
        self.results
            .snapshots
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(self.pipeline.clone())
            .or_default()
            .insert(self.processor.clone(), snapshot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::PortfolioType;
    use crate::pricer::PricingMetric;

    #[test]
    fn test_portfolio_results_totals_and_pipeline_isolation() {
        let results = Arc::new(PortfolioResults::default());
        assert!(results.snapshot().is_empty());
        let current = results.reporter("risk-1".into(), "processor_curr", "curr".into());
        let new = results.reporter("risk-1".into(), "processor_new", "new".into());
        let other = results.reporter("risk-2".into(), "processor_curr", "curr".into());
        let mut portfolio = PmPortfolio::new();
        portfolio.insert(
            PricingMetric::PV,
            PortfolioType::from([("trade-b".into(), 20.0), ("trade-a".into(), -5.0)]),
        );
        current.record(Some("market-1"), "Accepted", &portfolio);
        new.record(Some("market-2"), "CalculatingSingle", &portfolio);
        other.record(None, "Waiting for results", &PmPortfolio::new());

        let snapshot = results.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot["risk-1"].len(), 2);
        let current = &snapshot["risk-1"]["processor_curr"];
        assert_eq!(current.processor_name, "curr");
        assert_eq!(current.market.as_deref(), Some("market-1"));
        assert_eq!(current.metrics["PV"].total, Some(15.0));
        assert_eq!(current.metrics["PV"].trades["trade-a"], Some(-5.0));
        assert_eq!(current.metrics["PV"].trades.len(), 2);
        assert!(current.updated_at_ms > 0);
        assert_eq!(
            snapshot["risk-1"]["processor_new"].market.as_deref(),
            Some("market-2")
        );
        assert!(snapshot["risk-2"]["processor_curr"].metrics.is_empty());

        new.record(Some("market-3"), "CalculatingBulk", &PmPortfolio::new());
        assert!(results.snapshot()["risk-1"]["processor_new"]
            .metrics
            .is_empty());
        assert_eq!(
            snapshot["risk-1"]["processor_new"].metrics["PV"].total,
            Some(15.0)
        );
        assert_eq!(
            results.snapshot()["risk-1"]["processor_curr"].metrics["PV"].total,
            Some(15.0)
        );
    }

    #[test]
    fn test_portfolio_results_nonfinite_values_are_unavailable() {
        let results = Arc::new(PortfolioResults::default());
        let reporter = results.reporter("risk".into(), "processor_new", "new".into());
        let mut portfolio = PmPortfolio::new();
        portfolio.insert(
            PricingMetric::PV,
            PortfolioType::from([
                ("invalid".into(), f64::NAN),
                ("infinite".into(), f64::INFINITY),
            ]),
        );
        reporter.record(None, "CalculatingSingle", &portfolio);
        let snapshot = results.snapshot();
        assert_eq!(snapshot["risk"]["processor_new"].metrics["PV"].total, None);
        assert_eq!(
            snapshot["risk"]["processor_new"].metrics["PV"].trades["invalid"],
            None
        );
        assert!(serde_json::to_string(&snapshot).unwrap().contains("null"));
    }
}
