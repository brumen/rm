use rdkafka::consumer::StreamConsumer;

use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef};
use rdkafka::message::BorrowedMessage;

use crate::pricer::MarketPricingOptions;
use crate::processor_new::ProcessorNewMessage;
use crate::pricer::PricingMetric;
use crate::market::MarketType;
use crate::ref_deref::TryFromRef;


pub struct MarketProducer{
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub mkt_listener: StreamConsumer,  // listening for market events.
    pub new_processor: ActorRef<ProcessorNewMessage>,
}


#[async_trait]
impl Actor for MarketProducer
{
    type Msg = MarketType;
    type State = MarketType;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let new_mkt_msg = self.mkt_listener.recv().await?;
	let new_mkt = MarketType::try_from_ref(&new_mkt_msg)?;
	cast!(myself, new_mkt);
	
	Ok(MarketType::new())
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	_state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

        // let market_obj = MarketType::try_from_ref(&message)?;

	cast!(
	    self.new_processor,
	    ProcessorNewMessage::NewMarket(message)
	);

	// TODO: ONE HAS TO COMMIT? CHECK IF REALLY NEC?
        // match self.mkt_listener_.commit_message(&borrowed_msg, CommitMode::Sync) {
        //     Ok(_) => {
        //         debug!("Successful commit of market message");
        //     }
        //     Err(e) => {
        //         error!("Message could not be committed to Kafka. Continuing in best hopes: {:?}", e);
        //     }
        // }

	let new_msg = self.mkt_listener.recv().await?;
	let new_mkt_msg = MarketType::try_from_ref(&new_msg)?;
	cast!(myself, new_mkt_msg);
	
	Ok(())
    }
}
