use crate::portfolio::{PmPortfolio, PortfolioType};
use crate::pricer::PricingMetric;
use std::collections::HashSet;

/// Synchronize a PmPortfolio with a new set of metrics.
/// Rules:
/// 1. Metrics in present_pm that are also in new_metrics are retained.
/// 2. Metrics in present_pm that are not in new_metrics are removed.
/// 3. Metrics in new_metrics that are not in present_pm are added with default PortfolioType.
pub fn change_metrics(present_pm: &mut PmPortfolio, new_metrics: Vec<PricingMetric>) {
    let new_metrics_set: HashSet<_> = new_metrics.iter().cloned().collect();

    // Drop metrics not in new_metrics
    let existing_keys: Vec<_> = present_pm.keys().cloned().collect();
    for pm in existing_keys {
        if !new_metrics_set.contains(&pm) {
            present_pm.remove(&pm);
        }
    }

    // Add new metrics not in present_pm with default portfolio type
    for pm in new_metrics {
        present_pm.entry(pm).or_default();
    }
}
