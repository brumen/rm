// construct and connect all the actors in the framework.
use ractor::Actor;
use tokio::task::JoinHandle;
use tracing::info;
use std::sync::{Arc,Mutex};
use ractor::ActorRef;

use crate::mkt_handler_actor::MarketProducer;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::{MarketPricingOptions, PricingMetric};

use crate::market::{AllMarkets, CurrNewMarket};
use crate::publish::connect_with_retries_producer_rd;
use crate::trade_sender::TradeProducer;
use crate::processor_curr::ProcessorCurr;
use crate::processor_middle::ProcessorMiddle;
use crate::processor_new::ProcessorNew;
use crate::processor_bulk::ProcessorBulk;
use crate::processor_msg::{ProcessorMiddleMessage, ProcessorBulkMessage, };
use crate::portfolio::PortfolioType;


/// creates a chain of middle processors and connects 
///   them accordingly
/// returns:
///   (vector of processor actors,
///    vector of bulk actors,
///    last middle processor actor - to be used for new_actor, special case)
async fn create_middle_procs_chain(
    processor_curr: ActorRef<ProcessorMiddleMessage>,
    metric: PricingMetric,  // TODO: THIS SHOULD CHANGE
    pricing_options: MarketPricingOptions,
    all_markets: Arc<AllMarkets>,
) ->
    (
	Vec<ActorRef<ProcessorMiddleMessage>>,
	Vec<JoinHandle<()>>,
	Vec<ActorRef<ProcessorBulkMessage>>,
	Vec<JoinHandle<()>>,
	ActorRef<ProcessorMiddleMessage>
    ) {
    
    let mut bulk_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut bulk_actors: Vec<ActorRef<ProcessorBulkMessage>> = vec![];

    let mut processor_actors_futures: Vec<JoinHandle<()>> = vec![];
    let mut processor_actors: Vec<ActorRef<ProcessorMiddleMessage>> = vec![];
    
    let mut last_middle: ActorRef<ProcessorMiddleMessage> = processor_curr.clone();
    let nb_middle = all_markets.len();
    
    for middle_nb in 1..(nb_middle-1) {

	let market_name = all_markets.get(middle_nb);
	
	let bulk_middle = ProcessorBulk {
	    processor_name: format!("bulk_{}", market_name),
	    market_name: CurrNewMarket(market_name.clone()),
	    metric,
	    pricing_options: pricing_options.clone(),
	};
	
	let (bulk_actor, bulk_actor_future) = Actor::spawn(
	    None, bulk_middle, (),
	)
	    .await
	    .expect("Could not create bulk middle processor");

	bulk_actors_futures.push(bulk_actor_future);
	bulk_actors.push(bulk_actor.clone());
	
	let proc_middle = ProcessorMiddle {
	    metric,
	    pricing_options: pricing_options.clone(),
	    market_name: CurrNewMarket(market_name.clone()),
	    processor_below: last_middle,
	    processor_bulk: bulk_actor,
	    r_client: Some(reqwest::Client::new()),
	    all_markets: all_markets.clone(),
	};

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


/// initializes all the actors 
pub async fn start2(
    kafka_server: String,  // server including the port.  'localhost:9010'
    metric: PricingMetric,  // pricing metric, like PV
    pos_topic: String,     // position topic on kafka
    mkt_topic: String,     // market topic
    results_topic: String, // publish the results topic
    pricing_options: &MarketPricingOptions,
    server_state: Arc<Mutex<PortfolioType>>,
    all_markets: Arc<AllMarkets>,
) -> Vec<JoinHandle<()>> {

    let current_market = all_markets.get(0);
    
    info!("Starting current_bulk processor.");
    let (_processor_bulk_a, processor_new_bulk_handle) = Actor::spawn(
	None,
	ProcessorBulk {
	    processor_name: format!("{}_bulk", current_market),
	    market_name: CurrNewMarket(current_market),
	    metric,
	    pricing_options: (*pricing_options).clone()
	},
	(),
    ).await
    .expect("Could not start current_bulk processor.");

    let result_publisher = connect_with_retries_producer_rd(
	&kafka_server
    );
    
    let (_processor_curr_a, processor_curr_handle) = Actor::spawn(
	None,
	ProcessorCurr {
	    market_name: CurrNewMarket(all_markets.get(0)),
	    metric,
	    results_topic,
	    pricing_options: (*pricing_options).clone(),
	    result_publisher,
	    r_client: Some(reqwest::Client::new()),
	    portf: server_state,
	    all_markets: all_markets.clone(),
	},
	(),
    ).await
    .expect("Could not start current processor");

    // middle actors
    let (processor_actors, mut processor_actor_futures, _bulk_actors, mut bulk_actor_futures, last_middle) = 
	create_middle_procs_chain(
	    _processor_curr_a.clone(),
	    metric,
	    pricing_options.clone(),
	    all_markets.clone(),
	).await;

    let nb_middle_mkts = all_markets.len();
    let last_market_name = all_markets.get(nb_middle_mkts-1);
    let (_processor_new_a, processor_new_handle) = Actor::spawn(
	None,
	ProcessorNew {
	    metric,
	    pricing_options: (*pricing_options).clone(),
	    processor_curr: last_middle.clone(),
	    processor_bulk: _processor_bulk_a,
	    r_client: Some(reqwest::Client::new()),
	    market_name: CurrNewMarket(last_market_name),
	    all_markets: all_markets.clone(),
	},
	(),
    ).await
    .expect("Could not start new processor");

    info!("Connecting to market topic {:?}", mkt_topic);
    let mkt_listener = connect_with_retries_rd(
	&kafka_server, &mkt_topic
    );
    let (_mkt_producer_a, mkt_producer_handle) = Actor::spawn(
	None,
	MarketProducer {
	    metric,
	    pricing_options: (*pricing_options).clone(),
	    mkt_listener,
	    new_processor: _processor_new_a.clone(),
	},
	(),
    ).await
    .expect("Could not start market producer");
    
    let trade_producer = TradeProducer::new(
	kafka_server,
	pos_topic,
	processor_actors,
    );
    
    let (_trade_capture_a, trade_capture_handle) = Actor::spawn(
	None, trade_producer, ()
    ).await
    .expect("Could not start trade producer");

    // special futures
    let mut all_futures = vec![
	trade_capture_handle,  // trade producer
	processor_curr_handle,  // current processor
	processor_new_handle,   // new processor
	processor_new_bulk_handle,  // bulk of new processor
	mkt_producer_handle,  // market producer
    ];

    all_futures.append(&mut processor_actor_futures);  // middle processors
    all_futures.append(&mut bulk_actor_futures);  // middle bulk processors.

    all_futures
}
