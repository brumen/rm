//! Market updates use a one-slot mailbox: only the newest complete market is
//! needed before the next refresh. Raw updates are all applied before coalescing.
use crate::all_markets::AllMarkets;
use crate::market::{MarketTypeT, SetName};
use crate::processor_flow::{Endpoint, Task};
use crate::ref_deref::TryFromRef2;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::consumer::StreamConsumer;
use std::ops::AddAssign;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

pub(crate) struct MarketProducer<T, MT: MarketTypeT + std::fmt::Debug> {
    pub pricing_options: MT::MP,
    pub mkt_listener: Arc<StreamConsumer>,
    pub new_processor: Endpoint<T, MT>,
    pub all_markets: Arc<AllMarkets<Arc<MT>>>,
}
#[async_trait]
impl<T, MT> Actor for MarketProducer<T, MT>
where
    T: Send + Sync + 'static,
    for<'a> MT:
        MarketTypeT + Send + Sync + AddAssign<&'a MT> + Clone + 'static + SetName + std::fmt::Debug,
    MT::MP: 'static + Send + Sync + Clone,
    MT::MK: std::fmt::Debug,
    (MT::MK, f64): TryFromRef2<crate::market::MarketTypeError>,
{
    type Msg = ();
    type State = Option<Task>;
    type Arguments = ();
    async fn pre_start(&self, _: ActorRef<()>, _: ()) -> Result<Self::State, ActorProcessingErr> {
        Ok(None)
    }
    async fn post_start(
        &self,
        myself: ActorRef<()>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let mut market =
            (*MT::new(Uuid::new_v4().to_string(), self.pricing_options.clone())).clone();
        let listener = self.mkt_listener.clone();
        let processor = self.new_processor.clone();
        let markets = self.all_markets.clone();
        markets.insert("future".into(), Arc::new(market.clone()));
        *state = Some(Task(tokio::spawn(async move {
            info!("Market ingress started");
            loop {
                let message = match listener.recv().await {
                    Ok(message) => message,
                    Err(reason) => {
                        error!(%reason, "Market receive failed");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let (key, value) = match <(MT::MK, f64)>::try_from_ref(&message) {
                    Ok(update) => update,
                    Err(reason) => {
                        error!(?reason, "Invalid market update; skipping");
                        continue;
                    }
                };
                market.insert(key, value).await;
                market.set_name(Uuid::new_v4().to_string());
                let snapshot = Arc::new(market.clone());
                markets.insert("future".into(), snapshot.clone());
                if let Err(reason) = processor.market(snapshot) {
                    myself.stop(Some(reason));
                    return;
                }
            }
        })));
        Ok(())
    }
    async fn handle(
        &self,
        _: ActorRef<()>,
        _: (),
        _: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}
