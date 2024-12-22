/// Processor which gets a bulk of work, and finishes it.
use tracing::{info, debug};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::market::{CurrNewMarket, MarketType};
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::trade::{BaseTrade, TradeRep};
use crate::ao_trade::AOTrade;
use crate::processor_new::ProcessorNewMessage;


pub struct ProcessorBulk{
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
}

#[derive(Debug, Clone)]
pub enum ProcessorBulkMessage {
    NewBulk((MarketType, TradeRep<AOTrade>, ActorRef<ProcessorNewMessage>))
}

#[derive(Debug)]
pub enum ProcessorBulkState {
    Calculating(MarketType),
    Idle,
}


impl Decoder for ProcessorBulk {}

impl<ReductionType> RestPricerSpark<ReductionType> for ProcessorBulk
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorBulk: Decoder,
{

    fn _pricing_server_spark(&self) -> String {
	// TODO: CHECK IF CLONING IS GOOD
	self.pricing_options.pricing_server.clone()
    }

    fn _pricing_endpoint_spark(&self, market_: CurrNewMarket, metric: PricingMetric) -> String {
	// TODO: THIS IS PROBABLY WRONG!!!
	format!("{}/{:?}/{}", self.pricing_options.pricing_endpoint, market_, metric)
    }
}

#[async_trait]
impl Actor for ProcessorBulk
{
    type Msg = ProcessorBulkMessage;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing ProcessorBulk");
	Ok(())
    }

    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	_state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	let ProcessorBulkMessage::NewBulk((_market, new_trades, processor_new)) = message;
	// start the long-running pricing procedure
	debug!("BULK Processor TRADES: {:?}", new_trades);
	let new_portfolio = self.price_trades_on_spark(
	    &new_trades, self.metric, &self.pricing_options, CurrNewMarket::New,
	).await;
	debug!("BULK Processor TO NEW: {:?}", new_portfolio);
	processor_new.send_message(
	    ProcessorNewMessage::BulkReceive(
		(new_trades, new_portfolio)
	    )
	)?;
	Ok(())
    }
}
