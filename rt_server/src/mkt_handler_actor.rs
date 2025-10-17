use tracing::{info, debug};
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use serde::Deserialize;
use std::ops::AddAssign;


use crate::pricer::MarketPricingOptions;
use crate::processor_msg::ProcessorMiddleMessage;
use crate::pricer::PricingMetric;
use crate::market::{MarketType, MarketTypeT};
use crate::ref_deref::TryFromRef;


pub struct MarketProducer<MT> {
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub mkt_listener: StreamConsumer,  // listening for market events.
    pub new_processor: ActorRef<ProcessorMiddleMessage<MT>>,
}


#[async_trait]
impl<MT> Actor for MarketProducer<MT>
where
     //T: Send + Sync + 'static
    MT: Send + MarketTypeT + 'static + std::fmt::Debug + Clone + for<'a> AddAssign<&'a MT>
{
    type Msg =  MT;  // MarketType;
    type State = MT;  // MarketType;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing MarketProducer. Waiting on first message");
	let new_mkt_msg = self.mkt_listener.recv().await?;
	//let new_mkt = MarketType::try_from_ref(&new_mkt_msg)?;
        let new_mkt = MT::try_from_ref(&new_mkt_msg)?;
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
	//let new_mkt_msg = MarketType::try_from_ref(&new_msg)?;
        let new_mkt_msg = MT::try_from_ref(&new_msg)?;
	myself.send_message(new_mkt_msg)?;

	Ok(())
    }
}
