// construct and connect all the actors in the framework.
use ractor::Actor;
use ractor::ActorRef;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::pricer::PriceTrade;
use crate::processor_bulk::{PricingStyle, ProcessorBulk};
use crate::processor_curr::{ProcessorCurr, RTOperatingMode};
use crate::processor_middle::ProcessorMiddle;
use crate::processor_msg::{PNStateDistr, ProcessorMiddleMessage};
use crate::publish::connect_with_retries_producer_rd;
use crate::trade::{BaseTrade, TradeRep};

// parameters for the Kafka system.
#[derive(Clone)]
pub(crate) struct KafkaParams {
    pub(crate) kafka_server: String,
    pub(crate) pos_topic: String,
    pub(crate) mkt_topic: String,
    pub(crate) results_topic: String,
}

#[allow(dead_code)]
pub(crate) async fn create_curr_actor<T, MT>(
    kafka_params: KafkaParams,
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    curr_mkt_name: String,
    initial_trades: Arc<TradeRep<T>>,
    mp: MT::MP,
    operating_mode: RTOperatingMode,
    pricing_mode: PricingStyle,
) -> (ProcessorCurr<T, MT>, JoinHandle<()>)
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
    MT::MP: 'static + Send + Sync + Clone,
    MT: MarketTypeT + Send + Sync + 'static + Clone + std::fmt::Debug,
{
    let curr_bulk = ProcessorBulk::new(
        "curr".to_string(), // bulk is for processor current
        initial_trades.clone(),
        all_markets.clone(),
        pricing_mode.clone(),
    );

    // bulk processor for the current processor.
    let (_processor_bulk_a, processor_new_bulk_h) =
        Actor::spawn(Some("processor_curr".to_string()), curr_bulk, mp.clone())
            .await
            .expect("Could not start current_bulk processor.");

    let result_publisher = connect_with_retries_producer_rd(&kafka_params.kafka_server);

    let processor_curr = ProcessorCurr {
        processor_name: curr_mkt_name, // TODO: CHECK IF THIS NAME IS CORRECT
        results_topic: kafka_params.results_topic,
        result_publisher,
        all_markets: all_markets.clone(),
        all_trades: initial_trades,
        operating_mode,
        pricing_mode,
    };

    (processor_curr, processor_new_bulk_h)
}

// creates a middle portion of the actor.
//  returns: processor middle, and the future
pub(crate) async fn create_middle_actor<T, MT>(
    processor_name: String,
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    initial_trades: Arc<TradeRep<T>>,
    processor_below: ActorRef<ProcessorMiddleMessage<String>>,
    _mp: MT::MP,
    pricing_mode: PricingStyle,
) -> (ProcessorMiddle<T, MT>, Arc<PNStateDistr>)
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
    MT::MP: 'static + Send + Sync + Clone,
    MT: MarketTypeT + 'static + std::fmt::Debug,
{
    let state_distr = Arc::new(PNStateDistr::new());
    let proc_middle = ProcessorMiddle::new(
        processor_name,
        processor_below,
        // bulk_actor,
        initial_trades.clone(),
        all_markets.clone(),
        state_distr.clone(),
        pricing_mode,
    );

    (proc_middle, state_distr)
}

/// creates a chain of middle processors and connects
///   them accordingly
/// returns:
///   (vector of processor actors,
///    vector of bulk actors,
///    last middle processor actor - to be used for new_actor, special case)
#[allow(dead_code)]
pub(crate) async fn create_middle_procs_chain<T, MT>(
    nb_middle: usize, // number of middle actors.
    processor_curr: ActorRef<ProcessorMiddleMessage<String>>,
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    initial_trades: Arc<TradeRep<T>>,
    mp: MT::MP,
    pricing_mode: PricingStyle,
) -> (
    Vec<ActorRef<ProcessorMiddleMessage<String>>>, // middle processors
    Vec<JoinHandle<()>>,                           // middle processor joint handles.
    Vec<Arc<PNStateDistr>>,
)
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
    MT::MP: 'static + Send + Sync + Clone,
    MT: MarketTypeT + Send + Sync + Clone + 'static + std::fmt::Debug,
{
    let mut processor_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut processor_actors: Vec<ActorRef<ProcessorMiddleMessage<String>>> = vec![];
    let mut state_distr_vec: Vec<Arc<PNStateDistr>> = vec![];

    let mut last_middle: ActorRef<ProcessorMiddleMessage<String>> = processor_curr.clone();

    for market_nb in 0..nb_middle {
        let market_name = format!("middle_{}", market_nb);
        let (processor_middle, middle_state_distr) = create_middle_actor(
            market_name.clone(),
            all_markets.clone(),
            initial_trades.clone(),
            last_middle.clone(),
            mp.clone(),
            pricing_mode.clone(),
        )
        .await;

        state_distr_vec.push(middle_state_distr);

        let (proc_actor, proc_actor_future) = Actor::spawn(Some(market_name), processor_middle, ())
            .await
            .expect("Could not start middle actor");

        processor_actors.push(proc_actor.clone());
        processor_actors_futures.push(proc_actor_future);
        last_middle = proc_actor;
    }

    (processor_actors, processor_actors_futures, state_distr_vec)
}
