use tracing::info;
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::pricer::MarketPricingOptions;
use crate::processor_bulk::ProcessorMiddleMessage;
use crate::pricer::PricingMetric;
use crate::market::MarketType;
use crate::ref_deref::TryFromRef;


pub struct MarketProducer{
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub mkt_listener: StreamConsumer,  // listening for market events.
    pub new_processor: ActorRef<ProcessorMiddleMessage>,
}


#[async_trait]
impl Actor for MarketProducer {
    type Msg = MarketType;
    type State = MarketType;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing MarketProducer. Waiting on first message");
	let new_mkt_msg = self.mkt_listener.recv().await?;
	let new_mkt = MarketType::try_from_ref(&new_mkt_msg)?;
	myself.send_message(new_mkt)?;
	
	Ok(MarketType::new())
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	info!("Handling market message: {:?}", message);
	let market = state;
	*market += &message;  // adding the new market message to the market.
	self.new_processor.send_message(
	    ProcessorMiddleMessage::NewMarket(market.clone())
	)?;

	// wait for new message
	let new_msg = self.mkt_listener.recv().await?;
	let new_mkt_msg = MarketType::try_from_ref(&new_msg)?;
	myself.send_message(new_mkt_msg)?;
	
	Ok(())
    }
}
