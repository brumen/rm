// construct and connect all the actors for the AirOption framework
use ractor::Actor;
use ractor::ActorRef;
use std::ops::AddAssign;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::all_markets::AllMarkets;
use crate::engine_actor::{create_curr_actor, create_middle_procs_chain, KafkaParams};
use crate::market::{MarketTypeT, SetName};
use crate::mkt_handler_actor::MarketProducer;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::PriceTrade;
use crate::pricer::PricingMetric;
use crate::processor_bulk::ProcessorBulk;
use crate::processor_curr::RTOperatingMode;
use crate::processor_msg::{PNStateDistr, ProcessorMiddleMessage};
use crate::processor_new::ProcessorNew;
use crate::ref_deref::TryFromRef2;
use crate::trade::{BaseTrade, TradeRep};
use crate::trade_sender::TradeProducer;

/// initializes all the actors and returns a vector of joint handles to start them
///   all.
#[allow(dead_code)]
pub(crate) async fn start2<T, MT>(
    kafka_params: KafkaParams,
    metric: PricingMetric, // pricing metric, like PV
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    markets_used: Vec<String>,
    initial_trades: Arc<TradeRep<T>>,
    mp: MT::MP,
    nb_middle: usize,
    operating_mode: RTOperatingMode,
) -> (
    Vec<ActorRef<ProcessorMiddleMessage<String>>>,
    Vec<JoinHandle<()>>,
    Arc<PNStateDistr>, // state distribution of the new processor
)
where
    T: BaseTrade + Clone + Send + Sync + 'static + PriceTrade<MT> + TryFromRef2 + std::fmt::Debug,
    MT::MP: 'static + Send + Sync + Clone,
    MT::MK: std::fmt::Debug,
    (MT::MK, f64): TryFromRef2,
    for<'a> MT:
        Send + Sync + MarketTypeT + 'static + Clone + AddAssign<&'a MT> + SetName + std::fmt::Debug,
{
    let mut actors_middle_msg = Vec::<ActorRef<ProcessorMiddleMessage<String>>>::new();

    // create the current processor.
    let (curr_processor, _curr_processor_bulk_h) = create_curr_actor(
        kafka_params.clone(),
        all_markets.clone(),
        markets_used[0].clone(),
        initial_trades.clone(),
        mp.clone(),
        operating_mode,
    )
    .await;

    let (_processor_curr_a, processor_curr_handle) =
        Actor::spawn(Some("processor_curr_actor".to_string()), curr_processor, ())
            .await
            .expect("Could not start current processor");

    actors_middle_msg.push(_processor_curr_a.clone());

    // middle actors (including state distribution for
    let (mut processor_actors, mut processor_actor_futures, middle_state_distr_vec) =
        create_middle_procs_chain(
            nb_middle,
            _processor_curr_a.clone(),
            all_markets.clone(),
            initial_trades.clone(),
            mp.clone(),
        )
        .await;

    // takes the last middle processor, if there are no
    //   middle processors, takes the current one.
    let last_middle = processor_actors.last().unwrap_or(&_processor_curr_a);

    actors_middle_msg.extend(processor_actors.clone());

    // creating the bulk processor
    let new_mkt_bulk = ProcessorBulk::new(
        "processor_new".to_string(),
        initial_trades.clone(),
        all_markets.clone(),
    );

    let (processor_new_bulk_actor, processor_new_bulk_handle) = Actor::spawn(
        Some("processor_new_bulk".to_string()),
        new_mkt_bulk,
        mp.clone(),
    )
    .await
    .expect("Could not start processor_new_bulk");

    // state distribution of the processor new
    let state_distr_new = Arc::new(PNStateDistr::new());
    let processor_new = ProcessorNew::new(
        "processor_new".to_string(),
        last_middle.clone(),
        processor_new_bulk_actor.clone(),
        all_markets.clone(),
        initial_trades.clone(),
        state_distr_new.clone(),
    );

    let (_processor_new_a, processor_new_handle) =
        Actor::spawn(Some("processor_new".to_string()), processor_new, ())
            .await
            .expect("Could not start new processor");

    actors_middle_msg.push(_processor_new_a.clone());

    // adding all actors to processors
    processor_actors.push(_processor_curr_a.clone()); // adding current processor to actors.
    processor_actors.push(_processor_new_a.clone()); // adding new processor to actors.

    info!("Connecting to market topic {:?}", kafka_params.mkt_topic);
    let mkt_listener = connect_with_retries_rd(&kafka_params.kafka_server, &kafka_params.mkt_topic);
    let market_producer = MarketProducer {
        metric,
        pricing_options: mp.clone(),
        mkt_listener,
        new_processor: _processor_new_a.clone(),
        all_markets: all_markets.clone(),
    };
    let (_mkt_producer_a, mkt_producer_handle) =
        Actor::spawn(Some("mkt_producer".to_string()), market_producer, ())
            .await
            .expect("Could not start market producer");

    let trade_producer = TradeProducer::new(
        kafka_params.kafka_server,
        kafka_params.pos_topic,
        processor_actors,
        initial_trades, // TODO: CHECK IF THIS NEEDS TO BE CHANGED.
    );

    let (_trade_capture_a, trade_capture_handle) =
        Actor::spawn(Some("trade_producer".to_string()), trade_producer, ())
            .await
            .expect("Could not start trade producer");

    // special futures
    let mut all_futures = vec![
        trade_capture_handle,      // trade producer
        processor_curr_handle,     // current processor
        processor_new_handle,      // new processor
        processor_new_bulk_handle, // bulk of new processor
        mkt_producer_handle,       // market producer
    ];

    all_futures.append(&mut processor_actor_futures); // middle processors

    // (actors_middle_msg, all_futures, state_distr_new)
    // TODO: CHECK HERE IF state_distr_new is correct, but it's currently
    //   not used anyways.
    // let state_distr_presented = middle_state_distr_vec.last().unwrap();
    (
        actors_middle_msg,
        all_futures,
        state_distr_new, // state_distr_presented.clone(),
    )
}
