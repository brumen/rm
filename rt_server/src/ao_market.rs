use ractor::async_trait;
use serde::{Serialize, Deserialize};
use rdkafka::message::{BorrowedMessage, Message};
use tracing::{debug};

use crate::market::{MarketTypeT, MarketTypeError};
use crate::pricer::PricingMetric;

pub struct AOMarketParams {
    client: reqwest::Client,
    market_endpoint: String,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AOMarketType {
    pub(crate) market_name: String,
    pub(crate) market_params: AOMarketParams,
}

#[derive(Debug, Clone)]
pub struct MarketPricingOptions {
    pub pricing_server: String,
    pub pricing_endpoint: String,
    pub market_server: String,
    pub market_endpoint: String
}


impl AOMarketType {

    fn _endpoint(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &dyn MarketTypeT<MP=AOMarketParams>,
    ) -> String {
	let pricing_server = pricing_options.pricing_server.clone();
	let metric = metric.to_string();
	let trades = self.id();

        let market_name = curr_new_mkt.market_name();
        // TODO: FIX THIS ENDPOINT HERE!!!
	let _endpoint = format!(
            "http://{pricing_server}/pricing?metric={metric}&market={market_name}&trade_ids={trades}");

        debug!("_endpoint: {:?}", _endpoint);

        _endpoint
    }

    /// computes the pricing request.
    async fn _pricing_request(
        &self,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &dyn MarketTypeT<MP=AOMarketParams>,
    ) -> Result<reqwest::Response, reqwest::Error> {
	reqwest::get(self._endpoint(metric, pricing_options, curr_new_mkt)).await
    }
}


#[async_trait]
impl MarketTypeT for AOMarketType {
    type MP=AOMarketParams;

    fn new(market_name: String, mp: AOMarketParams) -> Box<dyn MarketTypeT<MP=Self::MP>> {
        Box::new(
            Self {
                market_name,
                market_params: mp,
            }
        )
    }

    fn market_name(&self) -> String {
        self.market_name.clone()
    }

    async fn get(&self, stock: &String) -> Option<f64> {
        // self.client.get(self.enpoint)[stock]  // TODO: FINISH HERE
        todo!()
    }

    async fn insert(&self, key: String, value: f64) {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn try_from_ref(
        _market_name: String,
        value: &BorrowedMessage,
        mp: AOMarketParams
    ) -> Result<Box<dyn MarketTypeT<MP=Self::MP>>, MarketTypeError> {
        let msg_val = value.payload().ok_or(
            MarketTypeError::GeneralError("Didnt get payload".to_string())
        )?;

        let msg_utf = std::str::from_utf8(msg_val)?;
        let inner_market = serde_json::from_str::<String>(msg_utf)?;

        Ok(
            Box::new(
                Self {
                    market_name: inner_market,
                    market_params: mp,
                }
            )
        )
    }

    fn market_params(&self) -> &AOMarketParams {
        &self.market_params
    }

}
