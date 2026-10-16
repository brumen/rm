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
use crate::portfolio_results::PortfolioResults;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::PriceTrade;
use crate::processor_bulk::{PricingStyle, ProcessorBulk};
use crate::processor_curr::RTOperatingMode;
use crate::processor_msg::{PNStateDistr, ProcessorMiddleMessage};
use crate::processor_new::ProcessorNew;
use crate::ref_deref::TryFromRef2;
use crate::trade::{BaseTrade, TradeRep};
use crate::trade_sender::TradeProducer;

struct PipelineNames {
    namespace: String,
}

impl PipelineNames {
    fn new(results_topic: &str) -> Self {
        Self {
            namespace: format!("{results_topic}::{}", uuid::Uuid::new_v4()),
        }
    }

    fn processor(&self, role: &str) -> String {
        format!("{}::{role}", self.namespace)
    }

    fn future_market(&self) -> String {
        format!("future::{}", self.namespace)
    }
}

/// initializes all the actors and returns a vector of joint handles to start them
///   all.
#[allow(dead_code)]
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
    Vec<ActorRef<ProcessorMiddleMessage<String>>>,
    Vec<JoinHandle<()>>,
    Arc<PNStateDistr>, // state distribution of the new processor
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
    let mut actors_middle_msg = Vec::<ActorRef<ProcessorMiddleMessage<String>>>::new();
    // Both pipelines share AllMarkets, so every registration and future slot needs
    // a pipeline-specific key. Include an instance ID for repeated configurations.
    let pipeline = PipelineNames::new(&kafka_params.results_topic);
    let current_name = pipeline.processor(&markets_used[0]);
    let new_name = pipeline.processor("processor_new");

    // create the current processor.
    let (curr_processor, _curr_processor_bulk_h) = create_curr_actor(
        kafka_params.clone(),
        all_markets.clone(),
        current_name,
        initial_trades.clone(),
        mp.clone(),
        operating_mode,
        pricing_mode.clone(),
        portfolio_results.reporter(
            kafka_params.results_topic.clone(),
            "processor_curr",
            markets_used[0].clone(),
        ),
    )
    .await;

    let (_processor_curr_a, processor_curr_handle) =
    //Actor::spawn(Some("processor_curr_actor".to_string()), curr_processor, ())
        Actor::spawn(None, curr_processor, ())
            .await
            .expect("Could not start current processor");

    actors_middle_msg.push(_processor_curr_a.clone());

    // middle actors (including state distribution for
    let (mut processor_actors, mut processor_actor_futures, _middle_state_distr_vec) =
        create_middle_procs_chain(
            &pipeline.namespace,
            nb_middle,
            _processor_curr_a.clone(),
            all_markets.clone(),
            initial_trades.clone(),
            mp.clone(),
            pricing_mode.clone(),
        )
        .await;

    // takes the last middle processor, if there are no
    //   middle processors, takes the current one.
    let last_middle = processor_actors.last().unwrap_or(&_processor_curr_a);

    actors_middle_msg.extend(processor_actors.clone());

    // creating the bulk processor
    let new_mkt_bulk = ProcessorBulk::new(
        new_name.clone(),
        initial_trades.clone(),
        all_markets.clone(),
        pricing_mode.clone(),
    );

    let (processor_new_bulk_actor, processor_new_bulk_handle) = Actor::spawn(
        //Some("processor_new_bulk".to_string()),
        None,
        new_mkt_bulk,
        mp.clone(),
    )
    .await
    .expect("Could not start processor_new_bulk");

    // state distribution of the processor new
    let state_distr_new = Arc::new(PNStateDistr::new());
    let processor_new = ProcessorNew::new(
        new_name,
        last_middle.clone(),
        processor_new_bulk_actor.clone(),
        all_markets.clone(),
        initial_trades.clone(),
        state_distr_new.clone(),
        pricing_mode,
        portfolio_results.reporter(
            kafka_params.results_topic.clone(),
            "processor_new",
            "processor_new".to_string(),
        ),
    );

    let (_processor_new_a, processor_new_handle) =
    //Actor::spawn(Some("processor_new".to_string()), processor_new, ())
        Actor::spawn(None, processor_new, ())
            .await
            .expect("Could not start new processor");

    actors_middle_msg.push(_processor_new_a.clone());

    // adding all actors to processors
    processor_actors.push(_processor_curr_a.clone()); // adding current processor to actors.
    processor_actors.push(_processor_new_a.clone()); // adding new processor to actors.

    info!("Connecting to market topic {:?}", kafka_params.mkt_topic);
    let mkt_listener = connect_with_retries_rd(&kafka_params.kafka_server, &kafka_params.mkt_topic);
    let market_producer = MarketProducer {
        future_market_name: pipeline.future_market(),
        pricing_options: mp.clone(),
        mkt_listener,
        new_processor: _processor_new_a.clone(),
        all_markets: all_markets.clone(),
    };
    let (_mkt_producer_a, mkt_producer_handle) =
    //Actor::spawn(Some("mkt_producer".to_string()), market_producer, ())
        Actor::spawn(None, market_producer, ())
            .await
            .expect("Could not start market producer");

    let trade_producer = TradeProducer::new(
        kafka_params.kafka_server,
        kafka_params.pos_topic,
        processor_actors,
        initial_trades, // TODO: CHECK IF THIS NEEDS TO BE CHANGED.
    );

    let (_trade_capture_a, trade_capture_handle) =
    //Actor::spawn(Some("trade_producer".to_string()), trade_producer, ())
        Actor::spawn(None, trade_producer, ())
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

    (actors_middle_msg, all_futures, state_distr_new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::markets::letf_market::LETFMarketType;
    use crate::processor_middle::ProcessorMiddle;
    use crate::trades::trade_letf::LETFTrade;
    use ractor::{async_trait, ActorProcessingErr};

    struct PortfolioSink;

    #[async_trait]
    impl Actor for PortfolioSink {
        type Msg = ProcessorMiddleMessage<String>;
        type State = tokio::sync::mpsc::UnboundedSender<Self::Msg>;
        type Arguments = Self::State;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            sender: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(sender)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            sender: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            sender.send(message)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_two_processor_chains_deliver_portfolios_on_their_own_markets() {
        let all_markets = Arc::new(AllMarkets::new());
        let all_trades = Arc::new(TradeRep::<LETFTrade>::default());
        let results = Arc::new(PortfolioResults::default());
        let mut actors = Vec::new();
        let mut bulk_actors = Vec::new();
        let mut handles = Vec::new();
        let mut pipelines = Vec::new();

        for _ in 0..2 {
            let pipeline = PipelineNames::new("risk");
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let (sink, sink_handle) = Actor::spawn(None, PortfolioSink, sender).await.unwrap();
            actors.push(sink.clone());
            handles.push(sink_handle);
            let (middle, middle_handle) = Actor::spawn(
                None,
                ProcessorMiddle::new(
                    pipeline.processor("middle_0"),
                    sink,
                    all_trades.clone(),
                    all_markets.clone(),
                    Arc::new(PNStateDistr::new()),
                    PricingStyle::Sequential,
                ),
                (),
            )
            .await
            .unwrap();
            actors.push(middle.clone());
            handles.push(middle_handle);
            let (bulk, bulk_handle) = Actor::spawn(
                None,
                ProcessorBulk::new(
                    pipeline.processor("processor_new"),
                    all_trades.clone(),
                    all_markets.clone(),
                    PricingStyle::Sequential,
                ),
                (),
            )
            .await
            .unwrap();
            bulk_actors.push(bulk.clone());
            handles.push(bulk_handle);
            let (new, new_handle) = Actor::spawn(
                None,
                ProcessorNew::new(
                    pipeline.processor("processor_new"),
                    middle,
                    bulk,
                    all_markets.clone(),
                    all_trades.clone(),
                    Arc::new(PNStateDistr::new()),
                    PricingStyle::Sequential,
                    results.reporter("risk".into(), "processor_new", pipeline.namespace.clone()),
                ),
                (),
            )
            .await
            .unwrap();
            actors.push(new.clone());
            handles.push(new_handle);
            pipelines.push((pipeline, new, receiver));
        }

        for version in 0..3 {
            // Start both chains before receiving, allowing their work to interleave.
            for (pipeline, new, _) in &pipelines {
                let market_name = format!("{}-m{version}", pipeline.namespace);
                all_markets
                    .insert(
                        pipeline.future_market(),
                        Arc::new(LETFMarketType::new(market_name)),
                    )
                    .await;
                new.send_message(ProcessorMiddleMessage::NewMarket(pipeline.future_market()))
                    .unwrap();
            }
            for (pipeline, _, receiver) in &mut pipelines {
                let message =
                    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
                        .await
                        .expect("processor chain stopped making progress")
                        .unwrap();
                let ProcessorMiddleMessage::NewTradePortfolio((_, _, market, _)) = message else {
                    panic!("expected a portfolio");
                };
                let expected = format!("{}-m{version}", pipeline.namespace);
                assert_eq!(*market, expected);
                assert!(all_markets.get(&market).await.is_some());
                for role in ["processor_new", "processor_new_bulk", "middle_0"] {
                    assert_eq!(
                        all_markets.get_processor(&pipeline.processor(role)).await,
                        Some(expected.clone())
                    );
                }
            }
        }

        for actor in actors {
            actor.stop(None);
        }
        for actor in bulk_actors {
            actor.stop(None);
        }
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_pipelines_do_not_overwrite_each_others_markets_or_registrations() {
        let all = AllMarkets::new();
        // Even two instances with the same result topic need distinct keys.
        let first = PipelineNames::new("risk");
        let second = PipelineNames::new("risk");
        for (pipeline, market_name) in [(&first, "first-market"), (&second, "second-market")] {
            all.insert(
                pipeline.future_market(),
                Arc::new(LETFMarketType::new(market_name.into())),
            )
            .await;
            for role in ["curr", "processor_new", "processor_new_bulk", "middle_0"] {
                all.insert_both(
                    pipeline.processor(role),
                    market_name.into(),
                    Arc::new(LETFMarketType::new(market_name.into())),
                )
                .await;
            }
        }

        assert_eq!(all.processor_market_snapshot().await.len(), 8);
        for (pipeline, expected_market) in [(&first, "first-market"), (&second, "second-market")] {
            assert_eq!(
                all.get(&pipeline.future_market())
                    .await
                    .unwrap()
                    .market_name(),
                expected_market
            );
            for role in ["curr", "processor_new", "processor_new_bulk", "middle_0"] {
                assert_eq!(
                    all.get_processor(&pipeline.processor(role))
                        .await
                        .as_deref(),
                    Some(expected_market)
                );
            }
        }

        // Advancing every processor in one pipeline must preserve the other.
        for role in ["curr", "processor_new", "processor_new_bulk", "middle_0"] {
            all.insert_both(
                second.processor(role),
                "next-market".into(),
                Arc::new(LETFMarketType::new("next-market".into())),
            )
            .await;
        }
        assert!(all.get(&"first-market".into()).await.is_some());
        assert!(all.get(&"second-market".into()).await.is_none());
        assert!(all.get(&first.future_market()).await.is_some());
        assert!(all.get(&second.future_market()).await.is_some());
    }
}
