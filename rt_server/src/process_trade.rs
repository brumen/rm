use std::sync::Arc;

// Defining the trait Process Trade value.
use ractor::async_trait;


use crate::market::MarketType;
use crate::portfolio::PricingResults;
use crate::pricer::{MarketPricingOptions, PricingMetric};

#[async_trait]
pub trait ProcessTradeValue
where
    Self: Sync,
{
    async fn value_by_metric2(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> PricingResults;
}

pub trait ObtainMarket
where
    Self: Sync,
{
    fn get_market(&self, curr_new_mkt: MarketType) -> MarketType;
}
