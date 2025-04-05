// construct and connect all the actors for the AirOption framework
use ractor::Actor;
use rdkafka::message::BorrowedMessage;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tracing::info;
use std::sync::{Arc,Mutex};
use std::fmt::{Display, Debug};

use crate::mkt_handler_actor::MarketProducer;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::{MarketPricingOptions, PricingMetric};

use crate::market::{AllMarkets, MarketSwitching, MarketType};
use crate::publish::connect_with_retries_producer_rd;
use crate::ref_deref::{TryFromRef, TryFromRef2,};
use crate::trade_sender::TradeProducer;
use crate::processor_curr::ProcessorCurr;
use crate::processor_new::ProcessorNew;
use crate::processor_bulk::ProcessorBulk;
use crate::portfolio::PortfolioType;
use crate::trade::{BaseTrade, TradeRep};
use crate::engine_actor::create_middle_procs_chain;
use crate::process_trade::ProcessTradeValue;


/// initializes all the actors

/// initialize_client: whether the reqwest client is set, or None.
///   (setting it uses the client for remote pricing, putting it
///    to None, means pricing is local.)
pub(crate) async fn start2<T>(
    kafka_server: String,  // server including the port.  'localhost:9010'
    metric: PricingMetric,  // pricing metric, like PV
    pos_topic: String,     // position topic on kafka
    mkt_topic: String,     // market topic
    results_topic: String, // publish the results topic
    pricing_options: &MarketPricingOptions,
    server_state: Arc<Mutex<PortfolioType>>,
    all_markets: Arc<AllMarkets>,
    initial_trades: TradeRep::<T>,
    initialize_client: bool,
) -> Vec<JoinHandle<()>>
where T: Display + Debug + BaseTrade + Clone + Send + Sync +
    ProcessTradeValue + 'static + TryFromRef2 +
    for<'a> Deserialize<'a>
{

    let current_market = all_markets.get(0);

    info!("Starting current_bulk processor.");
    let (_processor_bulk_a, processor_new_bulk_handle) = Actor::spawn(
	None,
	ProcessorBulk {
	    processor_name: format!("{}_bulk", current_market),
	    market_name: MarketType::new(current_market.clone()),
	    metric,
	    pricing_options: (*pricing_options).clone(),
            trades: initial_trades.clone(),
	},
	(),
    ).await
    .expect("Could not start current_bulk processor.");

    let result_publisher = connect_with_retries_producer_rd(
	&kafka_server
    );

    let current_r_client = match initialize_client {
        true => Some(reqwest::Client::new()),
        false => None,
    };

    let processor_curr = ProcessorCurr {
	processor_name: all_markets.get(0).clone(),
	metric,
	results_topic,
	pricing_options: (*pricing_options).clone(),
	result_publisher,
	r_client: current_r_client,
	portf: server_state,
	all_markets: all_markets.clone(),
        trades: initial_trades,
    };

    // set the initial Current market to empty
    let current_market_name = all_markets.get(0);  // first market is current
    processor_curr.set_market(
        MarketType::default(),
        &mut MarketType::new(current_market_name.to_string()),
    )
        .await
        .expect("Could not set Current market on REST");

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
            initialize_client,
	).await;

    let nb_middle_mkts = all_markets.len();
    let last_market_name = all_markets.get(nb_middle_mkts-1);  // last market name in all_markets, should be "new" or similar
    let new_r_client = match initialize_client {
        true => Some(reqwest::Client::new()),
        false => None,
    };
    let processor_new = ProcessorNew {
	metric,
	pricing_options: (*pricing_options).clone(),
	processor_middle: last_middle.clone(),
	processor_bulk: _processor_bulk_a.clone(),
	r_client: new_r_client,
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
