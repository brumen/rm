use tracing::{info, debug};
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::ops::AddAssign;
use uuid::Uuid;

use crate::pricer::MarketPricingOptions;
use crate::processor_msg::ProcessorMiddleMessage;
use crate::pricer::PricingMetric;
use crate::market::MarketTypeT;
use crate::ref_deref::TryFromRef;


pub struct MarketProducer<MP>
where
    dyn MarketTypeT<MP=MP> + 'static: Sized
{
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub mkt_listener: StreamConsumer,  // listening for market events.
    pub new_processor: ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>>,
}


#[async_trait]
impl<MP> Actor for MarketProducer<MP>
where
    // MT: Send + MarketTypeT + 'static + std::fmt::Debug + Clone + for<'a> AddAssign<&'a MT>
    dyn MarketTypeT<MP=MP> + 'static: Sized + Send + Sync,
    MP: 'static
{
    type Msg = dyn MarketTypeT<MP=MP>;  // MarketType;
    type State = dyn MarketTypeT<MP=MP>;  // MarketType;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing MarketProducer. Waiting on first message");
	let new_mkt_msg = self.mkt_listener.recv().await?;
        let market_name = Uuid::new_v4().to_string();  // TODO: THIS IS WRONG - CHECK
        let new_mkt = Self::Msg::try_from_ref(market_name, &new_mkt_msg)?;
	myself.send_message(new_mkt.clone())?;

	Ok(new_mkt)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	info!("Handling new market message.");
	debug!("Market message: {:?}", message);

	let market = state;
	*market += &message;  // adding the new market message to the market.
	self.new_processor.send_message(
	    ProcessorMiddleMessage::NewMarket(market.clone())
	)?;

	// wait for new message
	let new_msg = self.mkt_listener.recv().await?;
        let market_name = Uuid::new_v4().to_string();  // TODO: FIX THIS HERE!!!
        let new_mkt_msg = Self::Msg::try_from_ref(market_name, &new_msg)?;
	myself.send_message(new_mkt_msg)?;

	Ok(())
    }
}
