
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
use crate::processor_msg::ProcessorMiddleMessage;
use crate::portfolio::PortfolioType;


/// creates a chain of middle processors and connects 
///   them accordingly
async fn create_middle_procs_chain(
    nb_middle: usize,
    processor_curr: ActorRef<ProcessorMiddleMessage>,
    metric: PricingMetric,  // TODO: THIS SHOULD CHANGE
    pricing_options: MarketPricingOptions,
    all_markets: Arc<AllMarkets>,
) -> Vec<JoinHandle<()>> {

    //let all_markets = Arc::new(AllMarkets::new(nb_middle));
    
    let mut actors: Vec<JoinHandle<()>> = vec![];
    
    let mut last_middle: ActorRef<ProcessorMiddleMessage> = processor_curr.clone();
    
    for middle_nb in 0..nb_middle {

	let market_name = CurrNewMarket(format!("market_{}", middle_nb));
	
	let bulk_middle = ProcessorBulk {
	    processor_name: format!("bulk_{}", middle_nb),
	    market_name: market_name.clone(),
	    metric,
	    pricing_options: pricing_options.clone(),
	};
	
	let (bulk_spawn, bulk_handle) = Actor::spawn(
	    None, bulk_middle, (),
	)
	    .await
	    .expect("Could not create bulk middle processor");

	actors.push(bulk_handle);
	
	let proc_middle = ProcessorMiddle {
	    metric,
	    pricing_options: pricing_options.clone(),
	    market_name: market_name.clone(),
	    processor_below: last_middle,
	    processor_bulk: bulk_spawn,
	    r_client: Some(reqwest::Client::new()),
	    all_markets: all_markets.clone(),
	};

	let (proc_middle_spawn, proc_middle_handle) = Actor::spawn(
	    None, proc_middle, ()
	)
	    .await
	    .expect("Could not start middle actor");

	actors.push(proc_middle_handle);
	last_middle = proc_middle_spawn;
    }

    actors
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

    info!("Starting bulk processor.");
    let (_processor_bulk_a, processor_bulk_handle) = Actor::spawn(
	None,
	ProcessorBulk {
	    processor_name: "new_bulk".to_string(),
	    market_name: CurrNewMarket("new".to_string()),
	    metric,
	    pricing_options: (*pricing_options).clone()
	},
	(),
    ).await
    .expect("Could not start bulk processor");

    let result_publisher = connect_with_retries_producer_rd(
	&kafka_server
    );
    
    let (_processor_curr_a, processor_curr_handle) = Actor::spawn(
	None,
	ProcessorCurr {
	    market_name: CurrNewMarket("current".to_string()),
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
    
    let (_processor_new_a, processor_new_handle) = Actor::spawn(
	None,
	ProcessorNew {
	    metric,
	    pricing_options: (*pricing_options).clone(),
	    processor_curr: _processor_curr_a.clone(),
	    processor_bulk: _processor_bulk_a,
	    r_client: Some(reqwest::Client::new()),
	    market_name: CurrNewMarket("new".to_string()),
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
	_processor_curr_a.clone(),
	_processor_new_a,
    );
    
    let (_trade_capture_a, trade_capture_handle) = Actor::spawn(
	None, trade_producer, ()
    ).await
    .expect("Could not start trade producer");

    // middle actors
    let nb_middle_mkts = (*all_markets).len();
    let mut all_actors = create_middle_procs_chain(
	nb_middle_mkts,
	_processor_curr_a,
	metric,
	pricing_options.clone(),
	all_markets.clone(),
    ).await;

    let mut other_actors = vec![
	trade_capture_handle,
	processor_curr_handle,
	processor_new_handle,
	processor_bulk_handle,
	mkt_producer_handle,
    ];

    all_actors.append(&mut other_actors);

    all_actors
}
