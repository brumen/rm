// construct and connect all the actors for the AirOption framework
use ractor::Actor;
use tokio::task::JoinHandle;
use tracing::info;
use std::sync::Arc;

use crate::mkt_handler_actor::MarketProducer;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::PricingMetric;
use crate::market::MarketTypeT;
use crate::all_markets::AllMarkets;
use crate::trade_sender::TradeProducer;
use crate::processor_new::ProcessorNew;
use crate::trade::{BaseTrade, TradeRep};
use crate::engine_actor::{create_middle_procs_chain, KafkaParams, create_curr_actor};
use crate::pricer::PriceTrade;
use crate::ref_deref::TryFromRef2;

/// initializes all the actors
pub(crate) async fn start2<T, MP>(
    kafka_params: KafkaParams,
    metric: PricingMetric,  // pricing metric, like PV
    all_markets: Arc<AllMarkets<Arc<dyn MarketTypeT<MP=MP> + Send + Sync>>>,
    markets_used: Vec<String>,
    initial_trades: Arc<TradeRep::<T>>,
) -> Vec<JoinHandle<()>>
where
    T : BaseTrade + Clone + Send + Sync + 'static + PriceTrade<MP> + TryFromRef2,
    MP: 'static + Send + Sync + Clone,
    for <'a> dyn MarketTypeT<MP=MP> + 'a: Send + Sync + Sized,
    for <'a> dyn MarketTypeT<MP=MP> + Send + Sync + 'a: Sized + MarketTypeT<MP=MP>,
    Arc<dyn MarketTypeT<MP=MP> + Send + Sync>: MarketTypeT<MP=MP>,
{

    let mp = all_markets.get_market_params().unwrap();
    let first_market = all_markets.get(&markets_used[0]).unwrap();
    let first_market_name = first_market.value();

    // create the
    let (curr_processor, curr_processor_bulk_h) = create_curr_actor(
        kafka_params, metric, all_markets.clone(), markets_used[0].clone(), initial_trades.clone(),
    ).await ;

    // set the initial Current market to empty
    // let current_market_name = all_markets.get(0);  // first market is current
    // processor_curr.set_market(
    //     MarketType::default(),
    //     &mut MarketType::new(current_market_name.to_string()),
    // )
    //     .await
    //     .expect("Could not set Current market on REST");

    let (_processor_curr_a, processor_curr_handle) = Actor::spawn(
	None, curr_processor, ()
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
    let last_market_name = markets_used[-1];  // last market name in all_markets, should be "new" or similar
    let processor_new = ProcessorNew {
	processor_name: last_market_name.clone(),
	metric,
	processor_middle: last_middle.clone(),
	processor_bulk: _processor_bulk_a.clone(),
	all_markets: all_markets.clone(),
        market_name: (
            MarketTypeT::new(last_market_name.clone(), mp.clone()),
            MarketTypeT::new("new".to_string(), mp.clone()),  // TODO: CHECK HERE!!!
        ),
    };

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
    let market_producer = MarketProducer {
	metric,
	pricing_options: mp.clone(),
	mkt_listener,
	new_processor: _processor_new_a.clone(),
    };
    let (_mkt_producer_a, mkt_producer_handle) = Actor::spawn(
	None, market_producer, (),
    )
        .await
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
