// Defining the trait Process Trade value.

use crate::market::{CurrNewMarket, MarketGeneral};
use crate::portfolio::PricingResults;
use crate::pricer::{MarketPricingOptions, PricingMetric};

pub trait ProcessTradeValue
where
    Self: Sync,
{
    fn value_by_metric2(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: MarketGeneral,
    ) -> impl std::future::Future<Output = PricingResults> + Send;
}

pub trait ObtainMarket
where
    Self: Sync,
{
    fn get_market(&self, curr_new_mkt: CurrNewMarket) -> MarketGeneral;
}
