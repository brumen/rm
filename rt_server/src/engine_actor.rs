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


pub(crate) async fn create_curr_actor<T, MP> (
    kafka_params: KafkaParams,
    metric: PricingMetric,  // pricing metric, like PV
    all_markets: Arc<AllMarkets<Arc<dyn MarketTypeT<MP=MP> + Send + Sync>>>,
    curr_mkt_name: String,  // markets_used: Vec<String>,
    initial_trades: Arc<TradeRep::<T>>,
) -> (ProcessorCurr<T, MP>, JoinHandle<()>)
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MP>,
    for <'a> dyn MarketTypeT<MP=MP> + Send + Sync + 'a: Sized + MarketTypeT<MP=MP>,
    MP: 'static + Send + Sync + Clone,
    for <'a> dyn MarketTypeT<MP=MP> + 'a: Send + Sync + Sized + MarketTypeT,
    for <'a> Arc<dyn MarketTypeT<MP=MP> + Send + Sync>: MarketTypeT<MP=MP> + Clone,
{

    //let current_market = markets_used[0];
    let mp = all_markets.get_market_params().unwrap();

    // bulk processor for the current processor.
    let (_processor_bulk_a, processor_new_bulk_h) = Actor::spawn(
	None,
	ProcessorBulk {
	    processor_name: format!("{}_bulk", curr_mkt_name),
	    metric,
            trade_names: vec![],
            all_trades: initial_trades.clone(),
            all_markets: all_markets.clone(),
	},
	mp,
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
pub(crate) async fn create_middle_actor<T, MP>(
    market_name: String,
    metric: PricingMetric,
    all_markets: Arc<AllMarkets<Arc<dyn MarketTypeT<MP=MP> + Send + Sync>>>,
    initial_trades: Arc<TradeRep<T>>,
    processor_below: ActorRef<ProcessorMiddleMessage<String>>,
) -> (ProcessorMiddle<T, MP>, JoinHandle<()>)
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MP>,
    for <'a> dyn MarketTypeT<MP=MP> + Send + Sync + 'a: Sized + MarketTypeT<MP=MP>,
    MP: 'static + Send + Sync + Clone,
    for <'a> dyn MarketTypeT<MP=MP> + 'a: Send + Sync + Sized + MarketTypeT,
    for <'a> Arc<dyn MarketTypeT<MP=MP> + Send + Sync>: MarketTypeT<MP=MP>,
{


    let bulk_middle = ProcessorBulk {
	processor_name: format!("bulk_{}", market_name),
	metric,
        trade_names: vec![],  // no trades at init.
        all_trades: initial_trades.clone(),
        all_markets: all_markets.clone(),
    };

    let mp = all_markets.get_market_params().unwrap();
    let (bulk_actor, bulk_actor_future) = Actor::spawn(
	None, bulk_middle, mp,
    )
	.await
	.expect("Could not create bulk middle processor");

    let proc_middle = ProcessorMiddle {
	metric,
	processor_name: format!("middle_{}", market_name),
	processor_below,
	processor_bulk: bulk_actor,
	all_markets: all_markets.clone(),
        all_trades: initial_trades.clone(),
    };

    (proc_middle, bulk_actor_future)
}



/// creates a chain of middle processors and connects
///   them accordingly
/// returns:
///   (vector of processor actors,
///    vector of bulk actors,
///    last middle processor actor - to be used for new_actor, special case)
pub(crate) async fn create_middle_procs_chain<T, MP> (
    processor_curr: ActorRef<ProcessorMiddleMessage<String>>,
    metric: PricingMetric,
    all_markets: Arc<AllMarkets<Arc<dyn MarketTypeT<MP=MP> + Send + Sync>>>,
    initial_trades: Arc<TradeRep<T>>,
) ->
    (
	Vec<ActorRef<ProcessorMiddleMessage<String>>>,  // middle processors
	Vec<JoinHandle<()>>,  // middle processor joint handles.
	Vec<JoinHandle<()>>,   // bulk processor handles.
    )
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MP>,
    for <'a> dyn MarketTypeT<MP=MP> + Send + Sync + 'a: Sized + MarketTypeT<MP=MP>,
    MP: 'static + Send + Sync + Clone,
    for <'a> dyn MarketTypeT<MP=MP> + 'a: Send + Sync + Sized + MarketTypeT,
    for <'a> Arc<dyn MarketTypeT<MP=MP> + Send + Sync>: MarketTypeT<MP=MP>,
{

    let mut bulk_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut processor_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut processor_actors: Vec<ActorRef<ProcessorMiddleMessage<String>>> = vec![];

    let last_middle: ActorRef<ProcessorMiddleMessage<String>> = processor_curr.clone();

    for market_nb_name in all_markets.market_names.iter() {
        let market_name = market_nb_name.value();
        let (processor_middle, bulk_actor_future) = create_middle_actor(
            market_name.to_string(),
            metric,
            all_markets.clone(),
            initial_trades.clone(),
            last_middle.clone(),
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
