//! Construction helpers for the bounded actor pipeline.
use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio_results::PortfolioReporter;
use crate::pricer::PriceTrade;
use crate::processor_bulk::PricingStyle;
use crate::processor_curr::ProcessorCurr as Processor;
use crate::processor_curr::{RTOperatingMode, SharedPublisher};
use crate::processor_flow::{Endpoint, Role};
use crate::processor_msg::PNStateDistr;
use crate::throttle::ThrottleConfig;
use ractor::Actor;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub(crate) struct KafkaParams {
    pub(crate) kafka_server: String,
    pub(crate) pos_topic: String,
    pub(crate) mkt_topic: String,
    pub(crate) results_topic: String,
}

/// Start the shared processor actor with the requested role and output path.
/// Each instance receives its own trade admission budget, one incoming snapshot
/// permit and one latest-market slot. The returned endpoint shares those limits
/// with ingress and upstream senders; the handle tracks the actor's lifetime.
pub(crate) async fn spawn_processor<T, MT>(
    processor_name: String,
    role: Role,
    below: Option<Endpoint<T, MT>>,
    publisher: Option<SharedPublisher>,
    operating_mode: RTOperatingMode,
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    reporter: PortfolioReporter,
    pricing_mode: PricingStyle,
    config: ThrottleConfig,
) -> (Endpoint<T, MT>, JoinHandle<()>, Arc<PNStateDistr>)
where
    T: Clone + Debug + Send + Sync + PriceTrade<MT> + 'static,
    MT: MarketTypeT + Debug + Send + Sync + 'static,
    MT::MP: Clone,
{
    let state_distr = Arc::new(PNStateDistr::new());
    let admission = Arc::new(Semaphore::new(config.capacity));
    let snapshots = Arc::new(Semaphore::new(1));
    let market_slot = Arc::new(Mutex::new(None));
    let processor = Processor {
        processor_name,
        role,
        below,
        publisher,
        operating_mode,
        all_markets,
        reporter: Some(reporter),
        state_distr: state_distr.clone(),
        pricing_mode,
        config,
        admission: admission.clone(),
        snapshots: snapshots.clone(),
        market_slot: market_slot.clone(),
    };
    let (actor, task) = Actor::spawn(None, processor, ())
        .await
        .expect("Could not start processor");
    (
        Endpoint {
            actor,
            admission,
            snapshots,
            market_slot,
        },
        task,
        state_distr,
    )
}
