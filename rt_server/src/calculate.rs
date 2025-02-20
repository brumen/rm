// calculating traits
use tracing::{info, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::market::{CurrNewMarket, MarketSwitching, MarketType, MarketGeneral};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;
use crate::processor_bulk::ProcessorBulkMessage;
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_curr::ProcessorCurrMessage;
use crate::ao_trade::AOTrade;


#[async_trait]
pub(crate) trait CalculateTrades {

    fn market(&self) -> MarketType;
    
    pub async fn calculate_single(&self, &market: MarketType, new_trade: &AOTrade) {
	let market = self.market();
	let new_trade_price = new_trade.value_by_metric2(
	    self.metric, &self.pricing_options, MarketGeneral::MarketRemote(market)
	).await;
	*portf += new_trade_price;  // portfolio update
			*trade_l += &new_trade;  // we add the trade to the list.
	
    }
}
