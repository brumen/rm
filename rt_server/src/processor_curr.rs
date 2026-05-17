use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::{PmPortfolio, PortfolioType};
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_bulk::{PriceMultiple, PricingStyle};
use crate::processor_msg::{ProcessorMiddleMessage, TradesLocal};
use crate::trade::{BaseTrade, TradeRep};

#[derive(PartialEq)]
pub enum RTOperatingMode {
    DoubleBuffer, // usual double or triple buffering
    SingleBuffer, // single buffering.
}

pub(crate) struct ProcessorCurr<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    T: std::fmt::Debug,
{
    pub processor_name: String,
    pub results_topic: String,
    pub result_publisher: FutureProducer,
    pub all_markets: Arc<AllMarkets<Arc<MT>>>, // all_markets is DashMap
    pub all_trades: Arc<TradeRep<T>>,          // all_trades is DashMap
    // trade_processor where we can send the info when the trades are processed
    // pub trade_processor: ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>>,
    pub operating_mode: RTOperatingMode,
}

impl<T, MT> std::fmt::Debug for ProcessorCurr<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("CurrentProcessor({})", self.processor_name))
    }
}

#[async_trait]
impl<T, MT> PriceMultiple<T, MT> for ProcessorCurr<T, MT>
where
    MT: MarketTypeT + 'static + std::fmt::Debug,
    MT::MP: Clone,
    T: PriceTrade<MT> + 'static + std::fmt::Debug + Sync + Send + Clone,
{
    fn pricing_style(&self) -> PricingStyle {
        PricingStyle::Sequential
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("Cant send to kafka")]
    // KafkaError(#[from] KafkaError),
    KafkaErr(KafkaError),
    #[error("Cant serialize")]
    SerializeError(#[from] serde_json::Error),
}

/// publish the portfolio for a particular metric
#[async_trait]
pub(crate) trait PublishPortfolio {
    async fn _publish_result_portfolio(
        &self,
        portf: PortfolioType,
        metric: PricingMetric,
    ) -> Result<(), SendError>;
}

#[async_trait]
impl<T, MT> PublishPortfolio for ProcessorCurr<T, MT>
where
    T: Send + Sync + std::fmt::Debug,
    MT: Send + Sync + MarketTypeT + std::fmt::Debug,
{
    // publishes the portfolio to the kafka bus.
    async fn _publish_result_portfolio(
        &self,
        portf: PortfolioType,
        metric: PricingMetric,
    ) -> Result<(), SendError> {
        let curr_mkt_json = serde_json::ser::to_string(&portf.clone())?;
        let portf_record = FutureRecord::to(&self.results_topic)
            .key(&metric)
            .payload(&curr_mkt_json);

        debug!("Publishing portfolio: size {}", portf.len());
        match self
            .result_publisher
            .send(portf_record, Timeout::Never)
            .await
        {
            Err((ke, _)) => Err(SendError::KafkaErr(ke)),
            _ => Ok(()),
        }
    }
}

const NTP_ALLOW_BEHIND: u64 = 10; // number of trades that the NTP is allowed behind, before we reject it.

/// current state of the processor
#[derive(Debug)]
pub(crate) struct _ProcessorCurrState {
    trades: TradesLocal,                 // current trades
    portfolio: PmPortfolio, //  2nd arg:  a map of metrics to portfolioTypes, e.g. PV: Portf1, PV01: Portf2...
    curr_market: Option<String>, // current market name
    pricing_results: Vec<PricingMetric>, // vector of pricing metrics.
    newtrades_since_last_newmarket: u64, // how many new trades were processed since last newmarket
}

impl std::fmt::Display for _ProcessorCurrState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Market: {:?}, portfolio: {:?}, NewTrades since last NewMarket: {}",
            self.curr_market,
            self.portfolio.simple(),
            self.newtrades_since_last_newmarket,
        )
    }
}

// T is the representation fo the trade
#[async_trait]
impl<T, MT> Actor for ProcessorCurr<T, MT>
where
    T: Sync + Send + Clone + BaseTrade + PriceTrade<MT> + 'static + std::fmt::Debug,
    ProcessorCurr<T, MT>: PublishPortfolio,
    MT: MarketTypeT + Send + Sync + 'static + std::fmt::Debug,
    MT::MP: Clone,
{
    type Msg = ProcessorMiddleMessage<String>;
    type State = _ProcessorCurrState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Starting ProcessorCurr: {}", self.processor_name);
        let initial_trades = TradesLocal::new();
        let initial_curr_portf = PmPortfolio::new();
        // no initial metrics
        Ok(_ProcessorCurrState {
            trades: initial_trades,
            portfolio: initial_curr_portf,
            curr_market: None,
            pricing_results: vec![],
            newtrades_since_last_newmarket: 0,
        })
    }

    #[instrument(
        skip(self, _myself, message, state),
        fields(
            processor="processor_curr",
            // msg = message.as_ref(),
            mkt = state.curr_market,
        )
    )]
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!(%state, "State:");
        match message {
            ProcessorMiddleMessage::NewTrade(trade) => {
                if self.operating_mode == RTOperatingMode::SingleBuffer {
                    return Ok(());
                }

                debug!("Message: NewTrade: Adding trade {:?}.", trade);
                state.newtrades_since_last_newmarket += 1;
                state.trades.insert(trade.clone());

                let Some(ref real_market) = state.curr_market else {
                    // only continue if you have a market.
                    warn!("Do not have market. Ignoring the trade.");
                    return Ok(());
                };

                // TODO: can v. be without clone
                let Some(mut trade_info) =
                    self.all_trades.read_async(&trade, |_, v| v.clone()).await
                else {
                    warn!(
                        "Could not find {} among all_atrades. Ignoring w/ computation and continuing.",
                        trade
                    );
                    return Ok(());
                };

                let Some(market_info) = self.all_markets.get(real_market).await else {
                    warn!(
                        "Could not find market {}. All markets: {:?}. Ignoring the new trade pricing.",
                        real_market, self.all_markets.market_size().await,
                    );
                    return Ok(());
                };

                debug!(
                    "Valuing trade {} on market {:?} for {:?} and adding to portfolio.",
                    trade,
                    market_info.market_name(),
                    state.pricing_results,
                );
                for pm in state.pricing_results.clone() {
                    let valued_trade_pm = trade_info.value_by_metric(pm, market_info.clone()).await;
                    debug!("Priced trade {:?}: {:?}", trade, valued_trade_pm);
                    if let Some(portf_pm) = state.portfolio.get_mut(&pm) {
                        *portf_pm += valued_trade_pm;
                    } else {
                        // for now just print warning.
                        error!(
                            "Could not find pricing metric {:?} in the state portfolio",
                            pm
                        );
                    }
                }

                for pm in state.pricing_results.clone() {
                    if let Some(portf_pm) = state.portfolio.get_mut(&pm) {
                        // publishing the portfolio to kafka.
                        if let Err(e) = self._publish_result_portfolio(portf_pm.clone(), pm).await {
                            error!("Could not publish the portfolio: {:?}", e);
                        };
                    } else {
                        error!(
                            "Could not find pricing metric {:?} in state portfolio. Investigate.",
                            pm
                        )
                    }
                }
            }

            ProcessorMiddleMessage::NewTradePortfolio((
                ntp_trades,
                mut ntp_portfolio,
                ntp_market,
                ntp_upstream_processor,
            )) => {
                debug!(
                    "Message: NewTradePortfolio. CurrPortfolio: {:?}, NewPortfolio: {:?}, NewMarket: {:?}",
                    state.portfolio.simple(),
                    ntp_portfolio.simple(),
                    ntp_market,
                );

                if state.pricing_results.is_empty() {
                    warn!("No pricing metrics. Not replacing portfolios.");
                    return Ok(());
                }

                // new behind current but only considering trades from
                //    a portfolio (BUG: for some reason ntp_trades and portfolio from trades can diverge IT SHOULDNT)
                // TODO: THIS NEEDS TO IMPROVE HERE!!.
                let ntp_trades_from_portfolio = ntp_portfolio
                    .get(&PricingMetric::PV)
                    .unwrap_or(&PortfolioType::default())
                    .clone();
                let ntp_behind_curr_portfolio = state
                    .trades
                    .clone()
                    .into_iter()
                    .filter(|x| !ntp_trades_from_portfolio.contains_key(x.as_str()))
                    .collect::<TradesLocal>();

                // new portfolio has more trades, send the portfolio to publisher.
                // let new_portf_acc = state.portfolio <= ntp_portfolio;
                // amount of trades that the ntp_portfolio is behind state.portfolio.
                let ntp_portf_behind = ntp_behind_curr_portfolio.len(); // state.portfolio.len() - ntp_portfolio.len(); //  < NTP_ALLOW_BEHIND;
                let ntp_portf_acc = (ntp_portf_behind as u64) < NTP_ALLOW_BEHIND;

                // compute those additional trades
                debug!("NTP is {} traded behind", ntp_portf_behind);
                //if (ntp_portf_behind > 0) & ntp_portf_acc {
                if ntp_portf_acc {
                    // new portfolio is accepted.
                    let Some(ntp_market_actual) = self.all_markets.get(&ntp_market).await else {
                        warn!("Could not get NTP market {:?}", ntp_market);
                        return Ok(());
                    };
                    debug!("Computing additional {:?} trades", ntp_portf_behind);
                    let (additional_trades, additional_portf) = self
                        .price_multiple(
                            ntp_behind_curr_portfolio.clone(),
                            state.pricing_results.clone(),
                            ntp_market_actual,
                            self.all_trades.clone(),
                        )
                        .await;
                    if additional_trades.len() < ntp_portf_behind {
                        // we couldnt synchronize portfolios, abandonging
                        return Ok(());
                    } // else everything is ok, continue

                    ntp_portfolio += additional_portf;

                    debug!(
                        "NTP portfolio accepted ({:?}). Publishing.",
                        ntp_portfolio.simple()
                    );
                    state.newtrades_since_last_newmarket = 0; // reset the newtrades count.
                    state.portfolio = ntp_portfolio;
                    for (pm, new_portf_pm) in state.portfolio.iter() {
                        if let Err(e) = self
                            ._publish_result_portfolio(new_portf_pm.clone(), *pm)
                            .await
                        {
                            error!("Error publishing: {:?}", e);
                        }
                    }

                    // market that we were holding should be removed from the all_markets,
                    // as it's not needed anymore.
                    // destroys the market at the end.
                    // important: this works w/o old_market != new_market, but it's better
                    //   since we dont have potential deadlocks on self.all_markets.processor_market_map.
                    if let Some(ref old_market) = state.curr_market {
                        if *old_market != ntp_market {
                            let _ = self
                                .all_markets
                                .insert_processor(self.processor_name.clone(), ntp_market.clone())
                                .await;
                        }
                    };

                    // update the state of current processor.
                    state.trades.extend(ntp_trades); // *trades += &new_trades;
                    debug!(
                        "Switching: {:?} -> {}",
                        state.curr_market,
                        ntp_market.clone()
                    );
                    state.curr_market = Some(ntp_market.clone());
                }

                // send the behind information to the middle processor.
                let acc_reject = match ntp_portf_acc {
                    true => "accepted",
                    false => "rejected",
                };
                debug!(
                    "Notifying {:?} that new portfolio message was {}.",
                    ntp_upstream_processor.get_name(),
                    acc_reject,
                );
                if let Err(e) = ntp_upstream_processor.send_message(ProcessorMiddleMessage::Behind(
                    ntp_market,
                    ntp_behind_curr_portfolio,
                )) {
                    error!(
                        "Error sending to upstream {:?}: {:?}",
                        ntp_upstream_processor, e
                    );
                }
            }

            // we get a portfolio of different metrics
            ProcessorMiddleMessage::Metric(new_pricing_metrics) => {
                info!("Changing metrics to {:?}", new_pricing_metrics);
                crate::utils::change_metrics(&mut state.portfolio, new_pricing_metrics.clone()); // fixes the portf to correspond to new_pricing_metrics
                state.pricing_results = new_pricing_metrics;

                // sending it to for publishing
                for (pm, portf_pm) in state.portfolio.iter() {
                    if let Err(e) = self._publish_result_portfolio(portf_pm.clone(), *pm).await {
                        error!("Error publishing to kafka: {:?}", e);
                    }
                }
            }

            unusual_msg => {
                error!("Unusual message. Shouldnt happen: {:?}", unusual_msg);
            }
        }
        Ok(())
    }
}
