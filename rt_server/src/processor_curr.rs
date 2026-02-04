use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::{PmPortfolio, PortfolioType};
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_msg::{ProcessorMiddleMessage, TradesLocal};
use crate::trade::{BaseTrade, TradeRep};

pub(crate) struct ProcessorCurr<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
{
    pub processor_name: String,
    pub results_topic: String,
    pub result_publisher: FutureProducer,
    pub all_markets: Arc<AllMarkets<Arc<MT>>>, // all_markets is DashMap
    pub all_trades: Arc<TradeRep<T>>,          // all_trades is DashMap
                                               // trade_processor where we can send the info when the trades are processed
                                               // pub trade_processor: ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>>,
}

impl<T, MT> std::fmt::Debug for ProcessorCurr<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("CurrentProcessor({})", self.processor_name))
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
    T: Send + Sync,
    MT: Send + Sync + MarketTypeT + std::fmt::Debug,
{
    async fn _publish_result_portfolio(
        &self,
        portf: PortfolioType,
        metric: PricingMetric,
    ) -> Result<(), SendError> {
        // sends to publisher actor
        let curr_mkt_json = serde_json::ser::to_string(&portf.clone())?;
        let curr_mkt_pv = format!("{{\"{}\": {}}}", metric, curr_mkt_json);

        // implements bytearray(str(dumps(self.curr_market)), ascii))
        let portf_record = FutureRecord::<'_, [u8], [u8]> {
            topic: &self.results_topic,
            partition: Some(0),
            payload: Some(curr_mkt_pv.as_bytes()),
            key: None, // TODO: pub key: Option<&'a K>,
            timestamp: None,
            headers: None,
        };

        // set up the portfolio in self
        //{
        //    let mut p = self.portf.lock().unwrap();
        //    *p = portf.clone();
        //}

        // first i32 = partition
        // second i64 = offset
        // error is the Kafka error
        // OwnedMessage - copy of the original message.
        // Result<(i32, i64), (KafkaError, OwnedMessage)>;
        info!("Publishing portfolio: size {}", portf.len());
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

/// current state of the processor
#[derive(Debug)]
pub(crate) struct _ProcessorCurrState {
    trades: TradesLocal,
    portfolio: PmPortfolio, //  2nd arg:  a map of metrics to portfolioTypes, e.g. PV: Portf1, PV01: Portf2...
    curr_market: Option<String>,
    pricing_results: Vec<PricingMetric>,
}

// T is the representation fo the trade
#[async_trait]
impl<T, MT> Actor for ProcessorCurr<T, MT>
where
    T: Sync + Send + Clone + BaseTrade + PriceTrade<MT> + 'static,
    ProcessorCurr<T, MT>: PublishPortfolio,
    MT: MarketTypeT + Send + Sync + 'static + std::fmt::Debug,
    MT::MP: Clone,
{
    type Msg = ProcessorMiddleMessage<String>; // dyn MarketTypeT<MP=MP>>;
                                               // state is a tuple of
                                               //  1st arg:  current trades,
                                               //  2nd arg:  a map of metrics to portfolioTypes, e.g. PV: Portf1, PV01: Portf2...
                                               //  3rd arg:  current market name
                                               //  4th arg:  vector of pricing metrics for which we are computing.
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
        })
    }

    #[instrument(
        name="curr_handle",
        skip(self, _myself, message, state),
        fields(
            msg = message.as_ref(),
            mkt = state.curr_market,
            all_markets = %self.all_markets,
        )
    )]
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        //let (trades, portf, market, curr_pricing_metrics) = state;
        debug!(?state, "State");
        match message {
            ProcessorMiddleMessage::NewTrade(trade) => {
                debug!("Message: NewTrade: {:?}. Adding.", trade);
                state.trades.insert(trade.clone());

                let Some(ref real_market) = state.curr_market else {
                    // only continue if you have a market.
                    warn!("Do not have market. Ignoring the trade.");
                    return Ok(());
                };

                let Some(trade_info) = self.all_trades.get(&trade) else {
                    warn!(
                        "Could not find {} among all_atrades. Ignoring w/ computation and continuing.",
                        trade
                    );
                    return Ok(());
                };

                let trade_real = trade_info.value();

                let Some(market_info) = self.all_markets.get(real_market) else {
                    warn!(
                        "Could not find market {}. Ignoring the new trade pricing.",
                        real_market
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
                    let valued_trade_pm = trade_real.value_by_metric(pm, market_info.clone()).await;
                    let portf_pm = state.portfolio.get_mut(&pm).unwrap();
                    *portf_pm += valued_trade_pm;
                }

                for pm in state.pricing_results.clone() {
                    let portf_pm = state.portfolio.get_mut(&pm).unwrap();
                    // publishing the portfolio to kafka.
                    self._publish_result_portfolio(portf_pm.clone(), pm).await?
                }
            }

            ProcessorMiddleMessage::NewTradePortfolio((
                new_trades,
                new_portfolio,
                new_market,
                upstream_processor,
            )) => {
                debug!(
                    "Message: NewTradePortfolio: Trades: {:?}, NewPortfolio: {:?}, NewMarket: {:?}",
                    new_trades.len(),
                    new_portfolio.simple(),
                    new_market,
                );
                // we got a new portfolio, possibly switch it

                // let new_behind_curr = trades - new_trades;
                let new_behind_curr = state
                    .trades
                    .iter()
                    .filter(|&x| !new_trades.contains(x.as_str()))
                    .cloned()
                    .collect::<TradesLocal>();

                // new portfolio has more trades, send the portfolio to publisher.
                let new_portf_acc = state.portfolio <= new_portfolio;
                if new_portf_acc {
                    info!("NewPortfolio accepted. Publishing.");
                    for (pm, new_portf_pm) in new_portfolio.iter() {
                        self._publish_result_portfolio(new_portf_pm.clone(), *pm)
                            .await?;
                    }

                    // market that we were holding should be removed from the all_markets,
                    // as it's not needed anymore.
                    // destroys the market at the end.
                    // important: this works w/o old_market != new_market, but it's better
                    //   since we dont have potential deadlocks on self.all_markets.processor_market_map.
                    if let Some(ref old_market) = state.curr_market {
                        if *old_market != new_market {
                            // only destroy if the markets are different
                            info!("Got new market, destroying the market {}", old_market);
                            let _ = self
                                .all_markets
                                .insert_processor(self.processor_name.clone(), new_market.clone());
                            // TODO: HANDLE ERROR MESSAGES
                        }
                    };

                    // update the state of current processor.
                    state.portfolio = new_portfolio;
                    state.trades.extend(new_trades); // *trades += &new_trades;
                    state.curr_market = Some(new_market.clone()); // markets should trickle down.
                    debug!("Switching to market {:?}", state.curr_market); // market should be created.
                } else {
                    // otherwise dont do anything.
                    debug!("NewPortfolio not accepted. Ignoring.");
                }

                // send the behind information to the middle processor.
                let acc_reject = match new_portf_acc {
                    true => "accepted",
                    false => "rejected",
                };
                debug!(
                    "Notifying {:?} that new portfolio was message was {}",
                    upstream_processor.get_name(),
                    acc_reject,
                );
                upstream_processor.send_message(ProcessorMiddleMessage::Behind(
                    new_market,
                    new_behind_curr.clone(),
                ))?;
            }

            // we get a portfolio of different metrics
            ProcessorMiddleMessage::Metric(new_pricing_metrics) => {
                info!("Changing metrics to {:?}", new_pricing_metrics);
                crate::utils::change_metrics(&mut state.portfolio, new_pricing_metrics.clone()); // fixes the portf to correspond to new_pricing_metrics
                state.pricing_results = new_pricing_metrics;

                // sending it to for publishing
                for (pm, portf_pm) in state.portfolio.iter() {
                    self._publish_result_portfolio(portf_pm.clone(), *pm)
                        .await?;
                }
            }

            _ => {
                panic!("Unusual message. Shouldnt happen");
            }
        }
        Ok(())
    }
}
