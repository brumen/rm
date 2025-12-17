use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::collections::HashSet;
use std::sync::Arc;
/// Processor which gets a bulk of work, and finishes it.
///
use tracing::{debug, info, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::{PmPortfolio, PortfolioType};
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage, TradesLocal};
use crate::trade::{BaseTrade, TradeRep};

// computes bulk evaluation of trades in trade_names
pub struct ProcessorBulk<T, MT>
where
    MT: MarketTypeT,
{
    pub processor_name: String, // name of the bulk processor, usually curr_bulk, new_bulk, middle_1_bulk
    pub(crate) all_trades: Arc<TradeRep<T>>, // all_trades is a reference to the structure that contains all trades.
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>, // dyn MarketTypeT<MP=MP> + Send + Sync>>>,
}

impl<T, MT> ProcessorBulk<T, MT>
where
    MT: MarketTypeT,
    MT::MP: Clone,
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

    // you can override the
}

/// prices multiple trades - default configuration is to price them sequentially
/// TODO: THIS SHOULD BE CHANGED - THIS TRAIT SHOULD GO TO MT: MarketTypeT
#[async_trait]
trait PriceMultiple<T, MT>
where
    MT: MarketTypeT + 'static,
    MT::MP: Clone,
    T: PriceTrade<MT> + 'static,
{
    // TODO: THIS CAN GET OPTIMIZED
    async fn price_multiple(
        &self,
        new_trades: HashSet<String>,
        pricing_metrics: Vec<PricingMetric>,
        market_actual: Arc<MT>,
        all_trades: Arc<TradeRep<T>>,
    ) -> PmPortfolio {
        let mut portfolio = PmPortfolio::new();

        //for used_trade in used_trades {
        for used_trade in new_trades.iter() {
            let used_trade = all_trades.get(used_trade).unwrap();
            for pm in pricing_metrics.clone() {
                let price_pm = used_trade.value_by_metric(pm, market_actual.clone()).await;
                debug!("Priced trade {}: {:?}", used_trade.key(), price_pm);

                let price_pm_agg = price_pm.aggregate();
                match portfolio.get_mut(&pm) {
                    Some(portfolio_pm) => {
                        *portfolio_pm += price_pm_agg;
                    }
                    None => {
                        let mut new_pm = PortfolioType::default();
                        new_pm += price_pm_agg;
                        portfolio.insert(pm, new_pm);
                    }
                }
            }
        }

        portfolio
    }
}

#[derive(Debug)]
pub enum ProcessorBulkState {
    Calculating, // TODO: maybe include what market we are computing this on.
    Idle,
}

// impl<ReductionType, T, MP> RestPricerSpark<ReductionType> for ProcessorBulk<T, MP>
// where
//     ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
//     ProcessorBulk<T, MP>: Decoder,
//     T: Send + Sync,
//     MP: Send + Sync,
// {

//     fn _pricing_server_spark(&self) -> String {
// 	self.pricing_options.pricing_server.clone()
//     }

//     fn _pricing_endpoint_spark(
// 	&self,
// 	_market_: dyn MarketTypeT<MP=MP>,
// 	_metric: PricingMetric
//     ) -> String {
// 	"spark".to_string()
//     }
// }

#[async_trait]
impl<T, MT> PriceMultiple<T, MT> for ProcessorBulk<T, MT>
where
    MT: MarketTypeT + 'static,
    MT::MP: Clone,
    T: PriceTrade<MT> + 'static,
{
}

#[async_trait]
impl<T, MT> Actor for ProcessorBulk<T, MT>
where
    T: Sync + Send + Clone + BaseTrade + PriceTrade<MT> + 'static,
    MT: MarketTypeT + Send + Sync + 'static,
    MT::MP: Send + Sync + Clone,
{
    type Msg = ProcessorBulkMessage<String>;
    // type State = (usize, Option<dyn MarketTypeT<MP=MP>>);  // The number of attempts to run the bulk on, default = 5
    type State = ProcessorBulkState;
    type Arguments = MT::MP;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Initializing Bulk processor: {}", self.processor_name);
        Ok(ProcessorBulkState::Idle) //  (0, None)  // intialized to 0 attempts.
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("Processor: {}, State: {:?}", self.processor_name, state);
        match state {
            ProcessorBulkState::Calculating => {
                info!("Currently calculating, ignoring messages."); // TODO: This might change.
            }

            ProcessorBulkState::Idle => {
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
                        info!("Message: NewBulk");
                        info!("State: {:?} -> Calculating", state);
                        *state = ProcessorBulkState::Calculating;
                        info!(
                            "Computing {} trades for {:?}.",
                            new_trades.len(),
                            pricing_metrics,
                        );

                        let Some(market_actual) = self.all_markets.get(&market) else {
                            warn!("Could not get market {}. Abandoning pricing.", market);
                            // if curr_mkt == None, we couldnt get the market, abandon the attempts
                            sending_processor.send_message(ProcessorMiddleMessage::BulkReceive(
                                (
                                    new_trades.clone(),
                                    PmPortfolio::new(),
                                    TradesLocal::new(),
                                    market.clone(),
                                ),
                            ))?;
                            return Ok(());
                        };

                        // we have a market

                        let mut non_pricing_trades = TradesLocal::new();
                        let mut used_trades = vec![];
                        for trade_name in new_trades.iter() {
                            let Some(trade_attempt) = self.all_trades.get(trade_name) else {
                                warn!(
                                    "Could not get trade {} from all_trades. Continuing w/o it.",
                                    trade_name
                                );
                                non_pricing_trades.insert(trade_name.to_string());
                                continue;
                            };
                            used_trades.push(trade_attempt);
                        }

                        // pricing_futs are futures where the trades are getting priced.
                        //let mut pricing_futs = vec![];
                        //for used_trade in used_trades {
                        let portfolio = self
                            .price_multiple(
                                new_trades.clone(), // TODO: THIS .clone is NOT THE BEST - FIX IT
                                pricing_metrics,
                                market_actual.clone(),
                                self.all_trades.clone(),
                            )
                            .await;

                        // let mut portfolio = PmPortfolio::new();
                        // for used_trade in new_trades.iter() {
                        //     let used_trade = self.all_trades.get(used_trade).unwrap();
                        //     for pm in pricing_metrics.clone() {
                        //         let price_pm = used_trade.value_by_metric(pm, market_actual.clone()).await;
                        //         let price_pm_agg = price_pm.aggregate();
                        //         match portfolio.get_mut(&pm) {
                        //             Some(portfolio_pm) => {
                        //                 *portfolio_pm += price_pm_agg;
                        //             }
                        //             None => {
                        //                 let mut new_pm = PortfolioType::default();
                        //                 new_pm += price_pm_agg;
                        //                 portfolio.insert(pm, new_pm);
                        //             }
                        //         }
                        //         debug!("Priced trade {}: {:?}", used_trade.key(), price_pm);
                        //     }
                        // }
                        // TODO: Finish this part here!
                        // updating the portfolio
                        // let pricing_res = join_all(pricing_futs).await;
                        //for pricing in pricing_res.iter() {
                        //    portfolio += pricing.aggregate();
                        // }

                        info!(
                            "Sending portfolio back to actor {:?}: {:?}",
                            sending_processor.get_name(),
                            portfolio.count(),
                        );
                        debug!(
                            "Sending portfolio back to actor {:?}: {:?}",
                            sending_processor.get_name(),
                            portfolio.simple(),
                        );
                        sending_processor.send_message(ProcessorMiddleMessage::BulkReceive((
                            new_trades,
                            portfolio,
                            non_pricing_trades,
                            market,
                        )))?;
                        info!("State: {:?} -> Idle", state);
                        *state = ProcessorBulkState::Idle;
                    }

                    ProcessorBulkMessage::Abandon => {
                        // stop the computation and go into idle.
                    }
                }
            }
        }
        Ok(())
    }
}
