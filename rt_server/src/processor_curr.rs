use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::{PmPortfolio, PortfolioType};
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_msg::{ProcessorMiddleMessage, TradesLocal};
use crate::trade::{BaseTrade, TradeRep};

pub(crate) struct ProcessorCurr<T, MT>
where
    MT: MarketTypeT,
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
    MT: MarketTypeT,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CurrentProcessor({self.processor_name})")
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
    MT: Send + Sync + MarketTypeT,
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

// T is the representation fo the trade
#[async_trait]
impl<T, MT> Actor for ProcessorCurr<T, MT>
where
    T: Sync + Send + Clone + BaseTrade + PriceTrade<MT> + 'static,
    ProcessorCurr<T, MT>: PublishPortfolio,
    MT: MarketTypeT + Send + Sync + 'static,
    MT::MP: Clone,
{
    type Msg = ProcessorMiddleMessage<String>; // dyn MarketTypeT<MP=MP>>;
                                               // state is a tuple of
                                               //  1st arg:  current trades,
                                               //  2nd arg:  a map of metrics to portfolioTypes, e.g. PV: Portf1, PV01: Portf2...
                                               //  3rd arg:  current market name
                                               //  4th arg:  vector of pricing metrics for which we are computing.
    type State = (TradesLocal, PmPortfolio, Option<String>, Vec<PricingMetric>);
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
        Ok((initial_trades, initial_curr_portf, None, vec![]))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let (trades, portf, market, curr_pricing_metrics) = state;
        info!(
            "State: Trades: {}, portf: {:?}, market: {:?}",
            trades.len(),
            portf.count(),
            market,
        );

        match message {
            ProcessorMiddleMessage::NewTrade(trade) => {
                info!("Message: NewTrade: {:?}", trade);
                let Some(real_market) = market else {
                    // only continue if you have a market.
                    warn!("Do not have market. Ignoring the trade.");
                    return Ok(());
                };

                let Some(trade_info) = self.all_trades.get(&trade) else {
                    warn!(
                        "Could not find {} among all_atrades. Ignoring and continuing.",
                        trade
                    );
                    return Ok(());
                };

                info!("Adding new trade: {}", trade);
                let trade_real = trade_info.value();

                let Some(market_info) = self.all_markets.get(real_market) else {
                    warn!("Could not find market {}. Ignoring the market", real_market);
                    return Ok(());
                };

                debug!(
                    "Valuing trade {} on market {:?} for {:?} and adding to portfolio.",
                    trade,
                    market_info.market_name(),
                    curr_pricing_metrics
                );
                for pm in curr_pricing_metrics.clone() {
                    let valued_trade_pm = trade_real.value_by_metric(pm, market_info.clone()).await;
                    let portf_pm = portf.get_mut(&pm).unwrap();
                    *portf_pm += valued_trade_pm;
                }

                // updating the portfolio
                //*trades += &trade; // TODO: THIS CAN BE FIXED.
                trades.insert(trade);
                // *portf += valued_trade;
                // send information about all the trades to the trade processor
                // let now = Local::now();
                //self.trade_processor.send_message(
                //    ProcessorMiddleMessage::ProcessingStat(
                //        (self.processor_name.clone(), now.naive_local(), trades.len())
                //    )
                //);

                for pm in curr_pricing_metrics.clone() {
                    let portf_pm = portf.get_mut(&pm).unwrap();
                    self._publish_result_portfolio(portf_pm.clone(), pm).await?
                }
            }

            ProcessorMiddleMessage::NewTradePortfolio((
                new_trades,
                new_portfolio,
                new_market,
                upstream_processor,
            )) => {
                info!(
                    "Message: NewTradePortfolio: Trades: {:?}, NewPortfolio: {:?}, NewMarket: {:?}",
                    new_trades.len(),
                    new_portfolio.count(),
                    new_market,
                );
                // we got a new portfolio, possibly switch it

                // let new_behind_curr = trades - new_trades;
                let new_behind_curr = trades
                    .iter()
                    .filter(|&x| !new_trades.contains(x.as_str()))
                    .cloned()
                    .collect::<TradesLocal>();

                info!(
                    "NewPortfolio: behind curr: {:?}, portf size: {}",
                    new_behind_curr.len(),
                    new_portfolio.len(),
                );

                // new portfolio has more trades, send the portfolio to publisher.
                if *portf <= new_portfolio {
                    info!("NewPortfolio accepted. Publishing.");
                    for (pm, new_portf_pm) in new_portfolio.iter() {
                        self._publish_result_portfolio(new_portf_pm.clone(), *pm)
                            .await?;
                    }

                    // market that we were holding should be removed from the all_markets,
                    // as it's not needed anymore.
                    // IMPORTANT: this .remove call CAN DEADLOCK!!!
                    // destroys the market at the end.
                    if let Some(old_market) = market {
                        if *old_market != new_market {
                            // only destroy if the markets are different
                            info!("Got new market, destroying the market {}", old_market);
                            let _ = self.all_markets.remove(&old_market.clone());
                            // TODO: HANDLE ERROR MESSAGES
                        }
                    };

                    // update the state of current processor.
                    *portf = new_portfolio;
                    trades.extend(new_trades); // *trades += &new_trades;
                    *market = Some(new_market.clone()); // markets should trickle down.
                    info!("Switching to market {:?}", market); // market should be created.
                } // otherwise dont do anything.

                // send the behind information to the middle processor.
                info!(
                    "Notifying upstream {:?} that message was accepted/rejected",
                    upstream_processor.get_name()
                );
                upstream_processor.send_message(ProcessorMiddleMessage::Behind(
                    new_market,
                    new_behind_curr.clone(),
                ))?;
            }

            // we get a portfolio of different metrics
            ProcessorMiddleMessage::Metric(new_pricing_metrics) => {
                info!("Changing metrics to {:?}", new_pricing_metrics);
                crate::utils::change_metrics(portf, new_pricing_metrics.clone()); // fixes the portf to correspond to new_pricing_metrics
                *curr_pricing_metrics = new_pricing_metrics;

                // sending it to for publishing
                for (pm, portf_pm) in portf.iter() {
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
