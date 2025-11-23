use tracing::info;
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
// use uuid::Uuid;
use std::ops::AddAssign;
use std::sync::Arc;

use crate::processor_msg::ProcessorMiddleMessage;
use crate::pricer::PricingMetric;
use crate::market::MarketTypeT;
use crate::all_markets::AllMarkets;


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

// MT ... market type, must have MP - market parameters as associated type.
#[async_trait]
impl<MT> Actor for MarketProducer<MT>
where
    for<'a> MT: MarketTypeT + Send + Sync + AddAssign<&'a MT> + Clone + 'static,
    MT::MP : 'static + Send + Sync + Clone,
{
    type Msg = MT;
    type State = MT;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

        let mp = self.pricing_options.clone();  // market params
        let fut_mkt = MT::new("future".to_string(), mp.clone());

        info!("Adding initial _future_ market to all_markets.");
        self.all_markets.insert(
            "future".to_string(),
            fut_mkt
        );

	info!("Initializing MarketProducer. Waiting on first message");
	let new_mkt_msg = self.mkt_listener.recv().await?;
        let new_mkt = Self::Msg::try_from_ref("future".to_string(), &new_mkt_msg, self.pricing_options.clone())?;

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
        *market += &market_addition;  // adding a new market
        let market_sent = Arc::new((*market).clone());  // TODO: THIS SHOULD BE BETTER!!
        self.all_markets.insert(
            "future".to_string(),
            market_sent,
        );

        let market_name = market.market_name();
	self.new_processor.send_message(
	    ProcessorMiddleMessage::NewMarket(market_name)  // notification that the future market was updated.
	)?;

	// wait for new message
	let new_msg = self.mkt_listener.recv().await?;
        // let market_name = Uuid::new_v4().to_string();   // name of the current market.
        let new_mkt = Self::Msg::try_from_ref("future".to_string(), &new_msg, self.pricing_options.clone())?;
	myself.send_message((*new_mkt).clone())?;

	Ok(())
    }
}
