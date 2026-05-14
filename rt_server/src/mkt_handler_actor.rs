use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::ops::AddAssign;
use std::sync::Arc;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::all_markets::AllMarkets;
use crate::market::{MarketTypeT, SetName};
use crate::pricer::PricingMetric;
use crate::processor_msg::ProcessorMiddleMessage;
use crate::ref_deref::TryFromRef2;

pub struct MarketProducer<MT>
where
    MT: MarketTypeT + std::fmt::Debug,
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
    for<'a> MT:
        MarketTypeT + Send + Sync + AddAssign<&'a MT> + Clone + 'static + SetName + std::fmt::Debug,
    MT::MP: 'static + Send + Sync + Clone,
    MT::MK: std::fmt::Debug,
    (MT::MK, f64): TryFromRef2,
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
        self.all_markets
            .insert(
                "future".to_string(), // market is inserted at "future" entry
                fut_mkt.clone(),
            )
            .await;

        Ok((*fut_mkt).clone()) // state after initialization is empty market.
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(
            "Consumer subscribed to {:?}",
            self.mkt_listener.subscription()?
        );
        info!("Waiting on market messages. Enable debug to display messages.");

        let market = state;

        loop {
            debug!("Listening to raw mkt data.");
            let Ok(new_msg) = self.mkt_listener.recv().await else {
                // ignore the loop - something went wrong.
                error!("Listening to the market topic failed.! Ignoring.");
                continue;
            };
            let Ok((new_item_name, new_item_value)) = <(MT::MK, f64)>::try_from_ref(&new_msg)
            else {
                error!("Could not decode the market message.! ignoring.");
                continue;
            };

            debug!(
                "Inserting into market: {:?}, {:?}",
                new_item_name, new_item_value
            );

            let _ = market.insert(new_item_name, new_item_value).await;

            let new_name = Uuid::new_v4().to_string();
            market.set_name(new_name.clone());

            let market_sent = Arc::new((*market).clone());
            debug!("Inserting future ({}) into all_markets", new_name);

            self.all_markets
                .insert("future".to_string(), market_sent)
                .await;

            debug!(
                "Current markets: {:?}",
                self.all_markets.list_market_names().await
            );
            debug!(
                "Current processor-market map: {:?}",
                self.all_markets.processor_market_map,
            );

            if let Err(e) = self
                .new_processor
                .send_message(ProcessorMiddleMessage::NewMarket("future".to_string()))
            {
                error!(
                    "Could not send NewMarket message to new_processor! Continuing w/o sending: {:?}",
                    e
                );
            };
        }
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}
