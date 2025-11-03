// construct and connect all the actors for the AirOption framework
use ractor::Actor;
use tokio::task::JoinHandle;
use tracing::info;
use std::sync::{Arc,Mutex};
use std::fmt::{Display, Debug};

use crate::mkt_handler_actor::MarketProducer;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::PricingMetric;
use crate::market::MarketTypeT;
use crate::market_switching::MarketSwitching;
use crate::all_markets::AllMarkets;
use crate::publish::connect_with_retries_producer_rd;
use crate::trade_sender::TradeProducer;
use crate::processor_curr::ProcessorCurr;
use crate::processor_new::ProcessorNew;
use crate::processor_bulk::ProcessorBulk;
use crate::portfolio::PortfolioType;
use crate::trade::{BaseTrade, TradeRep};
use crate::engine_actor::create_middle_procs_chain;


struct KafkaParams {
    kafka_server: String,
    pos_topic: String,
    mkt_topic: String,
    results_topic: String,
}


/// initializes all the actors
pub(crate) async fn start2<T, MP>(
    kafka_params: KafkaParams,
    metric: PricingMetric,  // pricing metric, like PV
    server_state: Arc<Mutex<PortfolioType>>,
    all_markets: Arc<AllMarkets<dyn MarketTypeT<MP=MP>>>,
    initial_trades: TradeRep::<T>,
) -> Vec<JoinHandle<()>>
where
    T : Display + Debug + BaseTrade + Clone + Send + Sync + 'static,
    dyn MarketTypeT<MP=MP>: Send + Sync + std::fmt::Debug + Sized,
{

    let current_market = all_markets.get(0);

    // bulk processor for the current processor.
    info!("Starting current_bulk processor.");
    let (_processor_bulk_a, processor_new_bulk_handle) = Actor::spawn(
	None,
	ProcessorBulk {
	    processor_name: format!("{}_bulk", current_market),
	    metric,
	    // pricing_options: (*pricing_options).clone(),
            trade_names: vec![],
            all_trades: Arc::new(initial_trades),
            all_markets: all_markets.clone(),
	},
	(),
    )
        .await
        .expect("Could not start current_bulk processor.");

    let result_publisher = connect_with_retries_producer_rd(
	&kafka_params.kafka_server
    );

    let processor_curr = ProcessorCurr {
	processor_name: all_markets.get(0).clone(),
	metric,
        results_topic: kafka_params.results_topic,
	result_publisher,
	portf: server_state,
	all_markets: all_markets.clone(),
        all_trades: initial_trades,
        trade_processor: None,
    };

    // set the initial Current market to empty
    // let current_market_name = all_markets.get(0);  // first market is current
    // processor_curr.set_market(
    //     MarketType::default(),
    //     &mut MarketType::new(current_market_name.to_string()),
    // )
    //     .await
    //     .expect("Could not set Current market on REST");

    let (_processor_curr_a, processor_curr_handle) = Actor::spawn(
	None, processor_curr, ()
    ).await
    .expect("Could not start current processor");

    // middle actors
    let (
        mut processor_actors,
        mut processor_actor_futures,
        _bulk_actors,
        mut bulk_actor_futures,
        last_middle
    ) =
	create_middle_procs_chain(
	    _processor_curr_a.clone(),
	    metric,
	    pricing_options.clone(),
	    all_markets.clone(),
	).await;

    let nb_middle_mkts = all_markets.len();
    let last_market_name = all_markets.get(nb_middle_mkts-1);  // last market name in all_markets, should be "new" or similar
    let processor_new = ProcessorNew {
	metric,
	pricing_options: (*pricing_options).clone(),
	processor_middle: last_middle.clone(),
	processor_bulk: _processor_bulk_a.clone(),
	r_client: Some(reqwest::Client::new()),
	processor_name: last_market_name.clone(),
	all_markets: all_markets.clone(),
        market_name: (
            MarketType::new(last_market_name.clone()),
            MarketType::new("new".to_string()),  // TODO: CHECK HERE!!!
        ),
    };
    processor_new.set_market(
        MarketType::default(),
        &mut MarketType::new(last_market_name.to_string()),
    )
        .await
        .expect("Could not set the NEW market on market rester");


    let (_processor_new_a, processor_new_handle) = Actor::spawn(
	None, processor_new, (),
    ).await
    .expect("Could not start new processor");

    // adding all actors to processors
    processor_actors.push(_processor_curr_a.clone());  // adding current processor to actors.
    processor_actors.push(_processor_new_a.clone());  // adding new processor to actors.

    info!("Connecting to market topic {:?}", kafka_params.mkt_topic);
    let mkt_listener = connect_with_retries_rd(
	&kafka_params.kafka_server, &kafka_params.mkt_topic,
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
	kafka_params.kafka_server,
	kafka_params.pos_topic,
	processor_actors,
        initial_trades,  // TODO: CHECK IF THIS NEEDS TO BE CHANGED.
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
