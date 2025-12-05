use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::consumer::StreamConsumer;
use std::ops::AddAssign;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::all_markets::AllMarkets;
use crate::market::{MarketTypeT, SetName};
use crate::pricer::PricingMetric;
use crate::processor_msg::ProcessorMiddleMessage;

pub struct MarketProducer<MT>
where
    MT: MarketTypeT,
{
    pub metric: PricingMetric,
    pub pricing_options: MT::MP,
    pub mkt_listener: StreamConsumer, // listening for market events.
    pub new_processor: ActorRef<ProcessorMiddleMessage<String>>,
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>,
}

// mkt_handler deposits the new market information into the "future' market,
//    and writes it to the "future" market in all_markets
// MT ... market type, must have MP - market parameters as associated type.
#[async_trait]
impl<MT> Actor for MarketProducer<MT>
where
    for<'a> MT: MarketTypeT + Send + Sync + AddAssign<&'a MT> + Clone + 'static + SetName,
    MT::MP: 'static + Send + Sync + Clone,
{
    type Msg = MT;
    type State = MT;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Initializaing MarketProducer.");
        let mp = self.pricing_options.clone(); // market params
        let fut_mkt_tag = Uuid::new_v4();
        let fut_mkt = MT::new(fut_mkt_tag.to_string(), mp.clone());

        info!("Adding initial _future_ market to all_markets.");
        self.all_markets.insert(
            "future".to_string(), // market is inserted at "future" entry
            fut_mkt.clone(),
        );

        Ok((*fut_mkt).clone()) // state after initialization is empty market.
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("Waiting on first message");
        let new_mkt_msg = self.mkt_listener.recv().await?;
        let fut_mkt_tag_2 = Uuid::new_v4();
        let new_mkt = Self::Msg::try_from_ref(
            fut_mkt_tag_2.to_string(),
            &new_mkt_msg,
            self.pricing_options.clone(),
        )?;

        myself.send_message((*new_mkt.clone()).clone())?;

        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,      // message is new things about the market
        state: &mut Self::State, // market state is the market itself.
    ) -> Result<(), ActorProcessingErr> {
        info!("Handling new market message.");
        let market = state; // state holds the market.
        let market_addition = message;
        let new_name = market_addition.market_name();
        *market += &market_addition; // adding a new market
        market.set_name(new_name);
        let market_sent = Arc::new((*market).clone());
        // this insertion here is done efficiently.
        self.all_markets.insert("future".to_string(), market_sent);
        info!(
            "Current markets: {:?}",
            self.all_markets.list_market_names()
        );

        self.new_processor.send_message(
            ProcessorMiddleMessage::NewMarket("future".to_string()), // notification that the future market was updated.
        )?;

        // wait for new message
        let new_msg = self.mkt_listener.recv().await?;
        let additional_name = Uuid::new_v4().to_string();
        let new_mkt =
            Self::Msg::try_from_ref(additional_name, &new_msg, self.pricing_options.clone())?;
        myself.send_message((*new_mkt).clone())?;

        Ok(())
    }
}
