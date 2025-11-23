use ractor::async_trait;
use rdkafka::message::{BorrowedMessage, Message};
use tracing::{debug};
use std::sync::Arc;

use crate::market::{MarketTypeT, MarketTypeError};
use crate::pricer::PricingMetric;

#[derive(Debug, Clone)]
pub struct AOMarketParams {
    client: reqwest::Client,
    pub pricing_server: String,
    pub pricing_endpoint: String,
    pub market_server: String,
    pub market_endpoint: String
}


#[derive(Debug, Clone)]
pub struct AOMarketType {
    pub(crate) market_name: String,
    pub(crate) market_params: AOMarketParams,
}


impl AOMarketType {

    /// endpoint where the trades are priced.
    pub(crate) fn endpoint_pricer(
        &self,
        metric: PricingMetric,
        trades: Vec<String>,
    ) -> String {
	let pricing_server = self.market_params.pricing_server.clone();
        let market_name = self.market_name();
        let trades_sep = trades.join(",");

	let _endpoint = format!(
            "http://{pricing_server}/pricing?metric={metric}&market={market_name}&trade_ids={trades_sep}");

        debug!("_endpoint: {:?}", _endpoint);

        _endpoint
    }

    /// endpoint where the market is manipulated
    pub(crate) fn endpoint_market(&self) -> String {
	let pricing_server = self.market_params.pricing_server.clone();
        let market_name = self.market_name();

	let _endpoint = format!(
            "http://{pricing_server}/market?market={market_name}");

        debug!("_endpoint: {:?}", _endpoint);

        _endpoint
    }

}


#[async_trait]
impl MarketTypeT for AOMarketType {
    type MP=AOMarketParams;

    fn new(market_name: String, mp: AOMarketParams) -> Arc<AOMarketType> { // dyn MarketTypeT<MP=Self::MP> + Send + Sync> {
        Arc::new(
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
        todo!()
        // self.client.get(self.enpoint)[stock]  // TODO: FINISH HERE
        //let mkt_endpoint = self.endpoint_market();
        //let mkt_reqwest =

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
    ) -> Result<Arc<AOMarketType>, MarketTypeError> {  // dyn MarketTypeT<MP=Self::MP> + Send + Sync>, MarketTypeError> {
        let msg_val = value.payload().ok_or(
            MarketTypeError::GeneralError("Didnt get payload".to_string())
        )?;

        let msg_utf = std::str::from_utf8(msg_val)?;
        let inner_market = serde_json::from_str::<String>(msg_utf)?;

        Ok(
            Arc::new(
                Self {
                    market_name: inner_market,
                    market_params: mp,
                }
            )
        )
    }

    fn market_params(&self) -> AOMarketParams {
        self.market_params.clone()
    }
}
