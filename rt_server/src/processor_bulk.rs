/// Processor bulk gets a batch of trades to compute, and computes it.
///
use futures::future::join_all;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::{PmPortfolio, PortfolioType};
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage, TradesLocal};
use crate::trade::{BaseTrade, TradeRep};

// computes bulk evaluation of trades in trade_names
#[derive(Debug)]
pub struct ProcessorBulk<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    T: std::fmt::Debug,
{
    pub processor_name: String, // name of the bulk processor, usually curr_bulk, new_bulk, middle_1_bulk
    pub(crate) all_trades: Arc<TradeRep<T>>, // all_trades is a reference to the structure that contains all trades.
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>,
}

impl<T, MT> ProcessorBulk<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    MT::MP: Clone,
    T: std::fmt::Debug,
{
    pub(crate) fn new(
        processor_name: String, // original processor on which this depends.
        all_trades: Arc<TradeRep<T>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
    ) -> Self {
        // like processor_new_bulk, processor_curr_bulk, processor_middle_1_bulk
        let bulk_name = format!("{}_bulk", processor_name.clone());

        Self {
            processor_name: bulk_name,
            all_trades,
            all_markets,
        }
    }
}

/// prices multiple trades - default configuration is to price them sequentially
/// TODO: THIS SHOULD BE CHANGED - THIS TRAIT SHOULD GO TO MT: MarketTypeT
#[async_trait]
pub(crate) trait PriceMultiple<T, MT>
where
    MT: MarketTypeT + 'static,
    MT::MP: Clone,
    T: PriceTrade<MT> + 'static + Clone,
{
    async fn price_multiple(
        &self,
        new_trades: HashSet<String>,         // trades to price
        pricing_metrics: Vec<PricingMetric>, // metrics to price on
        market_actual: Arc<MT>,              // actual market to price them on.
        all_trades: Arc<TradeRep<T>>, // collection of all trades from which new_trades are picked.
    ) -> PmPortfolio {
        self._price_multiple_parallel_single_thread(
            new_trades,
            pricing_metrics,
            market_actual,
            all_trades,
        )
        .await
    }

    // this prices the trades in sequence.
    async fn _price_multiple_seq(
        &self,
        new_trades: HashSet<String>,         // trades to price
        pricing_metrics: Vec<PricingMetric>, // metrics to price on
        market_actual: Arc<MT>,              // actual market to price them on.
        all_trades: Arc<TradeRep<T>>, // collection of all trades from which new_trades are picked.
    ) -> PmPortfolio {
        let mut portfolio = PmPortfolio::new();

        for used_trade in new_trades.into_iter() {
            let Some(mut attempted_used) =
                all_trades.read_async(&used_trade, |_, v| v.clone()).await
            else {
                error!("Could not price {:?}. Ignoring that trade.", used_trade);
                // TODO: In the future, handle this better by reporting on the unpriced trades.
                continue;
            };

            for pm in &pricing_metrics {
                let price_pm_agg = attempted_used
                    .value_by_metric(*pm, market_actual.clone())
                    .await
                    .aggregate();
                portfolio.assign_metric(pm, price_pm_agg);
            }
        }

        portfolio
    }

    // tries to construct a number of trade futures, but computes them on
    //   a single thread instead of spawning them.
    async fn _price_multiple_parallel_single_thread(
        &self,
        new_trades: HashSet<String>, // trades that we want to compute.
        pricing_metrics: Vec<PricingMetric>, // metrics we want to compute
        market_actual: Arc<MT>,      // market
        all_trades: Arc<TradeRep<T>>, // all trades in the registry.
    ) -> PmPortfolio {
        let mut portfolio = PmPortfolio::new();

        // copies all trade information to curr_trades
        let mut curr_trades = vec![];
        // analytics about the number of trades handled.
        let nb_all_trades = all_trades.len(); // all existing trades.
        let nb_new_trades = new_trades.len(); // trades that we want to compute.
        let mut nb_found_trades = 0; // how many trades from new_trades did we find in all_trades.

        let _ = all_trades
            .iter_async(|trade_name, trade_val| {
                if new_trades.contains(trade_name) {
                    curr_trades.push(trade_val.clone());
                    nb_found_trades += 1;
                }
                true
            })
            .await;

        if nb_found_trades < nb_new_trades {
            error!(
                "Found only {:?} out of {:?} trades. Total nb available trades: {:?}",
                nb_found_trades, nb_new_trades, nb_all_trades
            );
        }

        for pm in &pricing_metrics {
            let trade_futures = curr_trades
                .iter_mut()
                .map(|t| t.value_by_metric(*pm, market_actual.clone()));

            // aggreate the results
            let trade_results = join_all(trade_futures)
                .await
                .iter()
                .map(|pr| pr.aggregate())
                .reduce(|a, b| a + b)
                .unwrap_or_default();
            portfolio.assign_metric(pm, trade_results);
        }
        portfolio
    }

    // processes trades in a parallel fashion, parameters the same as above.
    // TODO: Clones the trades, which could possibly be removed.
    async fn _price_multiple_parallel(
        &self,
        new_trades: HashSet<String>,
        pricing_metrics: Vec<PricingMetric>,
        market_actual: Arc<MT>,
        all_trades: Arc<TradeRep<T>>,
    ) -> PmPortfolio
    where
        T: Send + Sync,
        MT: Send + Sync,
    {
        let mut portfolio = PmPortfolio::new();

        // copies all trade information to curr_trades
        let mut curr_trades = vec![];
        let _ = all_trades
            .iter_async(|trade_name, trade_val| {
                if new_trades.contains(trade_name) {
                    curr_trades.push(trade_val.clone());
                } else {
                    warn!("Could not get trade {:?}. Continuing w/o it.", trade_name);
                }
                true
            })
            .await;

        // for every pricing metric launch a number of spawned tasks.
        for pm in &pricing_metrics {
            let mut task_handles = vec![];

            for mut trade in curr_trades.clone() {
                let market_actual = market_actual.clone();
                let pm = *pm;

                // launches the task for each future.
                let handle = ractor::concurrency::spawn(async move {
                    trade.value_by_metric(pm, market_actual).await.aggregate()
                });

                task_handles.push(handle);
            }

            let mut portf_for_pm = PortfolioType::default();
            for handle in task_handles {
                match handle.await {
                    Ok(trade_result) => {
                        portf_for_pm += trade_result;
                    }
                    Err(join_err) => error!("Failed spawned pricing task: {:?}", join_err),
                }
            }
            portfolio.assign_metric(pm, portf_for_pm);
        }

        portfolio
    }
}

#[async_trait]
impl<T, MT> PriceMultiple<T, MT> for ProcessorBulk<T, MT>
where
    MT: MarketTypeT + 'static + std::fmt::Debug,
    MT::MP: Clone,
    T: PriceTrade<MT> + 'static + std::fmt::Debug + Sync + Send + Clone,
{
}

#[async_trait]
impl<T, MT> Actor for ProcessorBulk<T, MT>
where
    T: Sync + Send + Clone + BaseTrade + PriceTrade<MT> + 'static + std::fmt::Debug,
    MT: MarketTypeT + Send + Sync + 'static + std::fmt::Debug,
    MT::MP: Send + Sync + Clone,
{
    type Msg = ProcessorBulkMessage<String>;
    type State = ();
    type Arguments = MT::MP;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Initializing Bulk processor: {}", self.processor_name);
        Ok(())
    }

    #[instrument(
        skip(message, state, _myself, self),
        fields(
            processor=self.processor_name,
        )
    )]
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Computing bulk.");

        match message {
            // market is where the trades are priced.
            // new_trades are trades that should be priced.
            // sending_processor ... processor where the result should be sent.
            ProcessorBulkMessage::NewBulk((
                market,
                new_trades,
                sending_processor,
                pricing_metrics,
            )) => {
                // start the long-running pricing procedure
                debug!("Message: NewBulk. State: {:?} -> Calculating", state);
                // *state = ProcessorBulkState::Calculating;
                info!(
                    "Computing {} trades for pricing metric({:?}) on market {}.",
                    new_trades.len(),
                    pricing_metrics,
                    market,
                );

                // registering the market that is sent:
                let _ = self
                    .all_markets
                    .insert_processor(self.processor_name.clone(), market.clone())
                    .await;
                debug!(
                    "Bulk: Current processor-market map: {:?}",
                    self.all_markets.processor_market_map,
                );

                let Some(market_actual) = self.all_markets.get(&market).await else {
                    warn!("Could not get market {}. Abandoning pricing.", market);
                    // if curr_mkt == None, we couldnt get the market, abandon the attempts
                    if let Err(e) =
                        sending_processor.send_message(ProcessorMiddleMessage::BulkReceive((
                            new_trades.clone(), // referencing trades. <- TODO: DO WE NEED THIS - SHOULD BE REMOVED.
                            PmPortfolio::new(), // computed portf = None, so not really useful.
                            TradesLocal::new(), // offending trades
                            market.clone(),     // referenced market
                        )))
                    {
                        error!(
                            "Error sending the message to the processor {:?}: {:?}. Not sending (should not be detrimental).",
                            sending_processor, e
                        );
                    };
                    //*state = ProcessorBulkState::Idle; // back to idle.
                    return Ok(());
                };

                // we have a market
                let mut non_pricing_trades = TradesLocal::new();
                let mut used_trades = vec![];

                // accounting for missing trades
                let new_trades_nb = new_trades.len();
                let all_trade_nb = self.all_trades.len();
                let mut non_pricing_trade_nb = 0;

                for trade_name in new_trades.iter() {
                    // TODO: WHAT PART OF THESE TRADES COULD BE CACHED???
                    let Some(trade_attempt) =
                        self.all_trades.read_sync(trade_name, |_, v| v.clone())
                    else {
                        non_pricing_trade_nb += 1;
                        // warn!(
                        //     "Could not get trade {} from all_trades. Continuing w/o it.",
                        //     trade_name
                        // );
                        non_pricing_trades.insert(trade_name.to_string());
                        continue;
                    };
                    used_trades.push(trade_attempt);
                }
                warn!(
                    "Could not find {:?} out of {:?} required trades. (All trade nb = {})",
                    non_pricing_trade_nb, new_trades_nb, all_trade_nb
                );

                debug!(
                    "Pricing {} trades on market: {:?}",
                    new_trades.len(),
                    market
                );
                let portfolio = self
                    .price_multiple(
                        new_trades.clone(), // TODO: THIS .clone is NOT THE BEST - FIX IT
                        pricing_metrics,
                        market_actual.clone(),
                        self.all_trades.clone(),
                    )
                    .await;

                debug!(
                    "Sending to actor {:?}: {:?}",
                    sending_processor.get_name(),
                    portfolio.simple(),
                );

                if let Err(e) = sending_processor.send_message(ProcessorMiddleMessage::BulkReceive(
                    (new_trades, portfolio, non_pricing_trades, market),
                )) {
                    error!(
                        "Could not send message to {:?}: {:?}. Continuing.",
                        sending_processor, e
                    );
                };
            }

            ProcessorBulkMessage::Abandon => {
                // stop the computation and go into idle.
                error!("TODO: Not yet implemented.");
            }
        }
        Ok(())
    }
}
