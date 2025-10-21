use std::sync::Arc;
use serde_json::json;
use tracing::{info, warn};
use ractor::async_trait;

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;


#[async_trait]
pub trait MarketSwitching<MP> {

    fn processor_name(&self) -> String;

    fn all_markets(&self) -> Arc<AllMarkets<dyn MarketTypeT<MP=MP>>>;

    /// endpoint where the market is posted.
    ///   could be for current, new or any other
    ///   market.
    ///   E.g. format!("http://{0}/market", pricing_server))
    fn market_endpoint(&self) -> String;

    /// reqwest client to implement market switching
    ///   if we dont need the request client, set it to None.
    fn r_client(&self) -> Option<&reqwest::Client>;

    /// Sets market_name to the market provided current and new markets to the ones
    ///   specified in this function.
    ///   market: market to replace the existing market_name
    ///   market_name: name of the market to be replaced
    ///   implements: market_name <- market
    async fn set_market(
	&self,
	market: & dyn MarketTypeT<MP=MP>,
	market_name: &mut dyn MarketTypeT<MP=MP>,
    ) -> Result<(), reqwest::Error> {

        info!("Setting market for {:?}", market_name.market_name);

        match self.r_client() {

            None => {
                *market_name = market;
            },

            Some(client) => {
                let internal_market_name = &market_name.market_name;

	        let payload = json!({
	            "market": market,
	            "market_type": internal_market_name,
	        });

                client
	            .post(self.market_endpoint())
                    .json(&payload)
                    .send()
                    .await?;
            },
        }
        Ok(())
    }

    /// switches market_below w/ market_above
    ///   market_name_below <- market_name_above
    async fn _switch_markets(
	&self,
	market_name_below: &mut dyn MarketTypeT<MP=MP>,
	market_name_above: & dyn MarketTypeT<MP=MP>,
    ) -> Result<(), reqwest::Error> {

        if market_name_below.is_empty() {
            info!("MARKET EMPTY ON {}: {}", self.processor_name(), market_name_below.market_name);
        }
        if market_name_above.is_empty() {
            info!("MARKET EMPTY ON {}: {}", self.processor_name(), market_name_above.market_name);
        }

        info!(
	    "Switching markets: {:?}: {:?} <- {:?}",
            self.processor_name(),
            market_name_below.market_name,
	    market_name_above.market_name,
	);


        match self.r_client() {

            Some(client) => {
	        // set the market below
	        let payload = json!({
	            "market_below": market_name_below.market_name,
	            "market_above": market_name_above.market_name,
	        });

                // replace market with switch_market in the endpoint
                let switch_market_endpoint = str::replace(
                    self.market_endpoint().as_str(), "market", "switch_market"
                );

                client
	            .post(switch_market_endpoint)
                    .json(&payload)
                    .send()
                    .await?;

            },

            None => {
                (*market_name_below).market = market_name_above.market.clone();  // leave name the same
            },
        }
        Ok(())
    }

    async fn switch_market(
	&self,
	market_name: &mut dyn MarketTypeT<MP=MP>,
    ) -> Result<(), reqwest::Error> {

	match market_name.next_market(&self.all_markets()) {
	    None => {
		warn!(
		    "Could not find next market of {}. All markets: {:?}, Leaving as it is.",
		    market_name.market_name, self.all_markets(),
		);
		return Ok(());
	    },
	    Some(above_market) => {
		self._switch_markets(market_name, &above_market).await?
	    }
	}

	Ok(())
    }
}
