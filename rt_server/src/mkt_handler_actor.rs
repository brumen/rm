use tracing::info;
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use uuid::Uuid;
use std::ops::AddAssign;
use std::sync::Arc;

use crate::processor_msg::ProcessorMiddleMessage;
use crate::pricer::PricingMetric;
use crate::market::MarketTypeT;
use crate::all_markets::AllMarkets;
use crate::ref_deref::TryFromRef2;


pub struct MarketProducer<MT>
where
    MT: MarketTypeT
{
    pub metric: PricingMetric,
    pub pricing_options: MT::MP,
    pub mkt_listener: StreamConsumer,  // listening for market events.
    pub new_processor: ActorRef<ProcessorMiddleMessage<String>>,
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>,
}


//pub(crate) type HandlerMarketType<MP> = dyn MarketTypeT<MP=MP> + Send + Sync;

// MP ... market parameters
// MM ... market message - message we receive from Kafka.
#[async_trait]
impl<MT> Actor for MarketProducer<MT>
where
    for<'a> MT: MarketTypeT + Send + Sync + AddAssign<&'a MT> + Clone + 'static,
    //dyn MarketTypeT + Send + Sync: Sized + Send + Sync + Clone + MarketTypeT + AddAssign<dyn MarketTypeT + Send + Sync>,
    MT::MP : 'static + Send + Sync + Clone,
    // dyn MarketTypeT<MP=MP>: Send + Sync + Sized,
    // Arc<dyn MarketTypeT<MP=MP> + Send + Sync>: AddAssign<Arc<dyn MarketTypeT<MP=MP> + Send + Sync>>,
    // Arc<MT>: AddAssign<MT>,
    //MT: Send + Sync + MarketTypeT<MP=MP> + 'static,
{
    type Msg = MT;  // dyn MarketTypeT<MP=MP> + Send + Sync;
    type State = MT; // dyn MarketTypeT<MP=MP> + Send + Sync;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing MarketProducer. Waiting on first message");
	let new_mkt_msg = self.mkt_listener.recv().await?;
        let market_name = Uuid::new_v4().to_string();  // TODO: THIS IS WRONG - CHECK
        let new_mkt = Self::Msg::try_from_ref(market_name, &new_mkt_msg, self.pricing_options.clone())?;  // try_from_ref(&new_mkt_msg);

        //self.all_markets.insert(market_name, mew_mkt);
	myself.send_message((*new_mkt.clone()).clone())?;

	Ok((*new_mkt).clone())
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,  // message is new things about the market
	state: &mut Self::State,  // market state is the market itself.
    ) -> Result<(), ActorProcessingErr> {

	info!("Handling new market message.");

	let market = state;
        let market_addition = message;
	// *market += &message;
        *market += &market_addition;  // adding a new market
        let market_name = market.market_name();
	self.new_processor.send_message(
	    ProcessorMiddleMessage::NewMarket(market_name)
	)?;

	// wait for new message
	let new_msg = self.mkt_listener.recv().await?;
        let market_name = Uuid::new_v4().to_string();  // TODO: FIX THIS HERE!!!
        let new_mkt = Self::Msg::try_from_ref(market_name, &new_msg, self.pricing_options.clone())?;
	myself.send_message((*new_mkt).clone())?;

	Ok(())
    }
}
