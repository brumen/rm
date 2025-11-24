// construct and connect all the actors in the framework.
use ractor::Actor;
use tokio::task::JoinHandle;
use std::sync::Arc;
use ractor::ActorRef;

use crate::pricer::PricingMetric;
use crate::market::MarketTypeT;
use crate::all_markets::AllMarkets;
use crate::processor_middle::ProcessorMiddle;
use crate::processor_bulk::ProcessorBulk;
use crate::processor_msg::ProcessorMiddleMessage;
use crate::trade::{BaseTrade, TradeRep};
use crate::pricer::PriceTrade;
use crate::publish::connect_with_retries_producer_rd;
use crate::processor_curr::ProcessorCurr;


// parameters for the Kafka system.
#[derive(Clone)]
pub(crate) struct KafkaParams {
    pub(crate) kafka_server: String,
    pub(crate) pos_topic: String,
    pub(crate) mkt_topic: String,
    pub(crate) results_topic: String,
}


pub(crate) async fn create_curr_actor<T, MT> (
    kafka_params: KafkaParams,
    metric: PricingMetric,  // pricing metric, like PV
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    curr_mkt_name: String,
    initial_trades: Arc<TradeRep::<T>>,
    mp: MT::MP,
) -> (ProcessorCurr<T, MT>, JoinHandle<()>)
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT>,
    MT::MP : 'static + Send + Sync + Clone,
    MT: MarketTypeT + Send + Sync + 'static + Clone,
{

    let curr_bulk = ProcessorBulk::new(
	"curr".to_string(),  // bulk is for processor current
	metric,
        initial_trades.clone(),
        all_markets.clone(),
    );

    // bulk processor for the current processor.
    let (_processor_bulk_a, processor_new_bulk_h) = Actor::spawn(
	None, curr_bulk, mp.clone(),
    )
        .await
        .expect("Could not start current_bulk processor.");

    let result_publisher = connect_with_retries_producer_rd(
	&kafka_params.kafka_server
    );

    let processor_curr = ProcessorCurr {
	processor_name: curr_mkt_name,  // TODO: CHECK IF THIS NAME IS CORRECT
	metric,
        results_topic: kafka_params.results_topic,
	result_publisher,
	all_markets: all_markets.clone(),
        all_trades: initial_trades,
    };

    (processor_curr, processor_new_bulk_h)
}



// creates a middle portion of the actor.
//  returns: processor middle, and the future
pub(crate) async fn create_middle_actor<T, MT>(
    processor_name: String,
    metric: PricingMetric,
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    initial_trades: Arc<TradeRep<T>>,
    processor_below: ActorRef<ProcessorMiddleMessage<String>>,
    mp: MT::MP,
) -> (ProcessorMiddle<T, MT>, JoinHandle<()>)
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT>,
    MT::MP : 'static + Send + Sync + Clone,
    MT: MarketTypeT + 'static,
{

    let middle_bulk_name = format!("{}_bulk", processor_name);

    // TODO: NEXT STAGE IS TO CONSTRUCT BULK INSIDE PROCESSOR MIDDLE
    let bulk_middle = ProcessorBulk::new(
	middle_bulk_name,
	metric,
        initial_trades.clone(),
        all_markets.clone(),
    );

    let (bulk_actor, bulk_actor_future) = Actor::spawn(
	None, bulk_middle, mp.clone(),
    )
	.await
	.expect("Could not create bulk middle processor");

    let proc_middle = ProcessorMiddle::new(
	processor_name,
        processor_below,
        bulk_actor,
        metric,
        initial_trades.clone(),
        all_markets.clone(),
        mp.clone()
    );

    (proc_middle, bulk_actor_future)
}



/// creates a chain of middle processors and connects
///   them accordingly
/// returns:
///   (vector of processor actors,
///    vector of bulk actors,
///    last middle processor actor - to be used for new_actor, special case)
pub(crate) async fn create_middle_procs_chain<T, MT> (
    nb_middle: usize,  // number of middle actors.
    processor_curr: ActorRef<ProcessorMiddleMessage<String>>,
    metric: PricingMetric,
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    initial_trades: Arc<TradeRep<T>>,
    mp: MT::MP,
) ->
    (
	Vec<ActorRef<ProcessorMiddleMessage<String>>>,  // middle processors
	Vec<JoinHandle<()>>,  // middle processor joint handles.
	Vec<JoinHandle<()>>,   // bulk processor handles.
    )
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT>,
    MT::MP : 'static + Send + Sync + Clone,
    MT: MarketTypeT + Send + Sync + Clone + 'static,
{

    let mut bulk_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut processor_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut processor_actors: Vec<ActorRef<ProcessorMiddleMessage<String>>> = vec![];

    let last_middle: ActorRef<ProcessorMiddleMessage<String>> = processor_curr.clone();

    for market_nb in 0..nb_middle {
        let market_name = format!("middle_{}", market_nb);
        let (processor_middle, bulk_actor_future) = create_middle_actor(
            market_name,
            metric,
            all_markets.clone(),
            initial_trades.clone(),
            last_middle.clone(),
            mp.clone(),
        )
            .await;
	//     .expect("Could not create bulk middle processor");

	bulk_actors_futures.push(bulk_actor_future);

	let (proc_actor, proc_actor_future) = Actor::spawn(
	    None, processor_middle, ()
	)
	    .await
	    .expect("Could not start middle actor");

	processor_actors.push(proc_actor.clone());
	processor_actors_futures.push(proc_actor_future);
    }

    (
	processor_actors,
	processor_actors_futures,
	bulk_actors_futures,
    )
}
