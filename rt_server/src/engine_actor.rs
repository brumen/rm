// construct and connect all the actors in the framework.
use ractor::Actor;
use tokio::task::JoinHandle;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use ractor::ActorRef;

use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::market::MarketTypeT;
use crate::all_markets::AllMarkets;
use crate::market_switching::MarketSwitching;
// use crate::process_trade::ProcessTradeValue;
use crate::processor_middle::ProcessorMiddle;
use crate::processor_bulk::ProcessorBulk;
use crate::processor_msg::{ProcessorMiddleMessage, ProcessorBulkMessage, };
use crate::trade::{BaseTrade, TradeRep};

/// creates a chain of middle processors and connects
///   them accordingly
/// returns:
///   (vector of processor actors,
///    vector of bulk actors,
///    last middle processor actor - to be used for new_actor, special case)
pub(crate) async fn create_middle_procs_chain<T, MP> (
    processor_curr: ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>>,
    metric: PricingMetric,  // TODO: THIS SHOULD CHANGE
    pricing_options: MarketPricingOptions,
    all_markets: Arc<AllMarkets<dyn MarketTypeT<MP=MP>>>,
    initialize_client: bool,
) ->
    (
	Vec<ActorRef<ProcessorMiddleMessage<T>>>,
	Vec<JoinHandle<()>>,
	Vec<ActorRef<ProcessorBulkMessage<T>>>,
	Vec<JoinHandle<()>>,
	ActorRef<ProcessorMiddleMessage<T>>
    )
where
    T: Send + Sync + Clone + Debug + Display + BaseTrade + 'static,
    dyn MarketTypeT<MP=MP> + 'static: Sized
{

    let mut bulk_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut bulk_actors: Vec<ActorRef<ProcessorBulkMessage<dyn MarketTypeT<MP=MP>>>> = vec![];

    let mut processor_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut processor_actors: Vec<ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>>> = vec![];

    let mut last_middle: ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>> = processor_curr.clone();
    let nb_middle = all_markets.len();

    let all_trades = TradeRep::<T>::default();

    for middle_nb in 1..(nb_middle-1) {

	let market_name = all_markets.get(middle_nb);
	let bulk_middle = ProcessorBulk {
	    processor_name: format!("bulk_{}", market_name),
	    metric,
	    pricing_options: pricing_options.clone(),
            trade_names: vec![],
            all_trades: Arc::new(all_trades),
	};

	let (bulk_actor, bulk_actor_future) = Actor::spawn(
	    None, bulk_middle, (),
	)
	    .await
	    .expect("Could not create bulk middle processor");

	bulk_actors_futures.push(bulk_actor_future);
	bulk_actors.push(bulk_actor.clone());

        let middle_r_client = match initialize_client {
            true => Some(reqwest::Client::new()),
            false => None,
        };
	let proc_middle = ProcessorMiddle {
	    metric,
	    pricing_options: pricing_options.clone(),
	    processor_name: market_name.clone(),
	    processor_below: last_middle,
	    processor_bulk: bulk_actor,
	    r_client: middle_r_client,
	    all_markets: all_markets.clone(),
	};

        proc_middle.set_market(
            MarketType::default(), &mut MarketType::new(market_name.to_string())
        )
            .await
            .expect("Could not set the {market_name} market.");

	let (proc_actor, proc_actor_future) = Actor::spawn(
	    None, proc_middle, ()
	)
	    .await
	    .expect("Could not start middle actor");

	processor_actors.push(proc_actor.clone());
	processor_actors_futures.push(proc_actor_future);

	last_middle = proc_actor;
    }

    (
	processor_actors,
	processor_actors_futures,
	bulk_actors,
	bulk_actors_futures,
	last_middle
    )
}
