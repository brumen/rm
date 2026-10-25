//! Construct the incoming -> middle -> current processing pipeline.
use crate::all_markets::AllMarkets;
use crate::engine_actor::{spawn_processor, KafkaParams};
use crate::market::{MarketTypeT, SetName};
use crate::mkt_handler_actor::MarketProducer;
use crate::portfolio_results::PortfolioResults;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::PriceTrade;
use crate::processor_bulk::PricingStyle;
use crate::processor_curr::{KafkaPublisher, RTOperatingMode};
use crate::processor_flow::Role;
use crate::processor_msg::{PNStateDistr, ProcessorMessage};
use crate::publish::connect_with_retries_producer_rd;
use crate::ref_deref::TryFromRef2;
use crate::throttle::ThrottleConfig;
use crate::trade::{BaseTrade, TradeRep};
use crate::trade_sender::TradeProducer;
use ractor::{Actor, ActorRef};
use std::ops::AddAssign;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Build one pipeline from shared processors configured as New, Middle and Current.
///
/// Connect New to the highest Middle stage, or directly to Current when there
/// are no middle stages. Market ingress targets New, while trade ingress feeds
/// stages from New down to Current so downstream backpressure does not withhold
/// the input prefix upstream needs to catch up. Return actor references for
/// configuration, lifecycle handles, and New's message counters for diagnostics.
pub(crate) async fn start2<T, MT>(
    kafka_params: KafkaParams,
    all_markets: Arc<AllMarkets<Arc<MT>>>,
    markets_used: Vec<String>,
    initial_trades: Arc<TradeRep<T>>,
    mp: MT::MP,
    nb_middle: usize,
    operating_mode: RTOperatingMode,
    pricing_mode: PricingStyle,
    portfolio_results: Arc<PortfolioResults>,
) -> (
    Vec<ActorRef<ProcessorMessage<T, MT>>>,
    Vec<JoinHandle<()>>,
    Arc<PNStateDistr>,
)
where
    T: BaseTrade
        + Clone
        + Send
        + Sync
        + 'static
        + PriceTrade<MT>
        + TryFromRef2<crate::trade::TradeError>
        + std::fmt::Debug,
    MT::MP: 'static + Send + Sync + Clone,
    MT::MK: std::fmt::Debug,
    (MT::MK, f64): TryFromRef2<crate::market::MarketTypeError>,
    for<'a> MT:
        Send + Sync + MarketTypeT + 'static + Clone + AddAssign<&'a MT> + SetName + std::fmt::Debug,
{
    let config = ThrottleConfig::from_env().expect("Invalid adaptive throttle configuration");
    let topic = &kafka_params.results_topic;
    let publisher = Arc::new(KafkaPublisher {
        producer: connect_with_retries_producer_rd(&kafka_params.kafka_server),
        topic: topic.clone(),
    });
    let (current, task, _) = spawn_processor(
        format!("{topic}::curr"),
        Role::Current,
        None,
        Some(publisher),
        operating_mode.clone(),
        all_markets.clone(),
        portfolio_results.reporter(topic.clone(), "processor_curr", markets_used[0].clone()),
        pricing_mode.clone(),
        config.clone(),
    )
    .await;
    let mut handles = vec![task];
    let mut endpoints = vec![current.clone()];
    let mut below = current;
    for index in 0..nb_middle {
        let name = format!("middle_{index}");
        let (middle, task, _) = spawn_processor(
            format!("{topic}::{name}"),
            Role::Middle,
            Some(below),
            None,
            RTOperatingMode::DoubleBuffer,
            all_markets.clone(),
            portfolio_results.reporter(topic.clone(), &name, name.clone()),
            pricing_mode.clone(),
            config.clone(),
        )
        .await;
        below = middle.clone();
        endpoints.push(middle);
        handles.push(task);
    }
    let (incoming, task, distribution) = spawn_processor(
        format!("{topic}::processor_new"),
        Role::New,
        Some(below),
        None,
        RTOperatingMode::DoubleBuffer,
        all_markets.clone(),
        portfolio_results.reporter(topic.clone(), "processor_new", "processor_new".into()),
        pricing_mode,
        config,
    )
    .await;
    endpoints.push(incoming.clone());
    handles.push(task);
    let all_actors = endpoints
        .iter()
        .map(|endpoint| endpoint.actor.clone())
        .collect();
    // Feed incoming first, then middle processors from top to bottom, then current.
    // A slow current processor cannot withhold the prefix upstream needs to catch up.
    endpoints.reverse();
    let market_producer = MarketProducer {
        pricing_options: mp,
        mkt_listener: Arc::new(connect_with_retries_rd(
            &kafka_params.kafka_server,
            &kafka_params.mkt_topic,
        )),
        new_processor: incoming,
        all_markets,
    };
    let (_, task) = Actor::spawn(None, market_producer, ())
        .await
        .expect("Could not start market producer");
    handles.push(task);
    let trade_producer = TradeProducer::new(
        kafka_params.kafka_server,
        kafka_params.pos_topic,
        endpoints,
        initial_trades,
    );
    let (_, task) = Actor::spawn(None, trade_producer, ())
        .await
        .expect("Could not start trade producer");
    handles.push(task);
    (all_actors, handles, distribution)
}
