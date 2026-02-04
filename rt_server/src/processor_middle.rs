// middle processor, sits between 2 new processors
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::PmPortfolio; // , PortfolioType
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage, TradesLocal};
use crate::trade::{BaseTrade, TradeRep};

// T is mnemonic for trade type, MT is mnemonic for market type
#[derive(Debug)]
pub(crate) struct ProcessorMiddle<T, MT: std::fmt::Debug> {
    pub(crate) processor_name: String,
    pub processor_below: ActorRef<ProcessorMiddleMessage<String>>,
    pub processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>,
    pub(crate) all_trades: Arc<TradeRep<T>>,
}

impl<T, MT> ProcessorMiddle<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    MT::MP: Clone,
    T: std::fmt::Debug,
{
    #[allow(dead_code)]
    pub(crate) fn new(
        processor_name: String,
        processor_below: ActorRef<ProcessorMiddleMessage<String>>,
        processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
        all_trades: Arc<TradeRep<T>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
    ) -> Self {
        Self {
            processor_name,
            processor_below,
            processor_bulk,
            all_trades,
            all_markets,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ProcessorMiddleState {
    CalculatingSingle, // when bulk has finished and we're only calculating single trades.
    CalculatingBulk,   // when we're still calculating bulk
    // CalculatingBulkMarketSwitch, // we are calculating bulk, but market
    // switched in the meantime, so the old calculating is not valid anymore
    Idle,
}

impl std::fmt::Display for ProcessorMiddleState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub struct _ProcessorMiddleStateful {
    // state of the processor middle:
    //   1st arg: list of trades,
    //   2nd arg: list of trades that didnt price correctly
    //   3rd: third is the current portfolio result for each pricing metric of correctly pricing trades.
    //   4th: is the computation state.
    //   5th: is the market that the processor is operating on.
    //      for remote pricing markets only market_name is fine,
    //      for local markets, the name and the market structure.
    //   6th: list of metrics that the system is operating on.
    trades: TradesLocal,
    trades_not_pricing: TradesLocal,
    pricing_results: PmPortfolio,
    processor_state: ProcessorMiddleState,
    curr_market: Option<String>,
    pricing_metrics: Vec<PricingMetric>,
}

#[async_trait]
impl<T, MT> Actor for ProcessorMiddle<T, MT>
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
    MT: Send + Sync + MarketTypeT + 'static + std::fmt::Debug,
    MT::MP: Clone,
{
    type Msg = ProcessorMiddleMessage<String>;
    type State = _ProcessorMiddleStateful;
    type Arguments = ();

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Processor {}: Starting.", self.processor_name,);

        Ok(_ProcessorMiddleStateful {
            trades: TradesLocal::new(),
            trades_not_pricing: TradesLocal::new(),
            pricing_results: PmPortfolio::new(), // no portfolio
            processor_state: ProcessorMiddleState::Idle,
            curr_market: None,       // original market, none
            pricing_metrics: vec![], // no metrics at first
        })
    }

    #[instrument(
        name="middle_handle",
        skip(self, myself, message, state),
        fields(
            name = %self.processor_name,
            state = %state.processor_state,
            mkt=state.curr_market,
        )
    )]
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // let (trade_l, trades_non_pricing, portf, pns, market, pricing_metrics) = state; // market is the market where we're operating
        let pns_old = state.processor_state.clone(); // (*pns).clone(); // otherwise we cant match

        info!(?pns_old, "State");

        match (message, pns_old) {
            (
                ProcessorMiddleMessage::NewTrade(new_trade),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                state.trades.insert(new_trade.clone()); // we add the trade to the list, even if the trade is faulty

                let trade_info = self
                    .all_trades
                    .get(&new_trade)
                    .ok_or("Message: NewTrade: Trade not found")?;
                info!("Message: NewTrade {}", trade_info.id());

                let Some(ref real_market) = state.curr_market else {
                    warn!("Processor does not have market. Ignoring.");
                    return Ok(());
                };

                let Some(market_info) = self.all_markets.get(real_market) else {
                    warn!("Could not get market {}. Weird - Continuing.", real_market);
                    return Ok(());
                };

                let real_trade = trade_info.value();

                for pm in &state.pricing_metrics {
                    let new_trade_price_pm =
                        real_trade.value_by_metric(*pm, market_info.clone()).await;
                    // what if pm is not in pricing_results.
                    let portf_pm = state.pricing_results.get_mut(pm).unwrap(); // only portfolio for that metric.
                    *portf_pm += new_trade_price_pm; // portfolio update
                }

                // we send the computed portfolio & trades to the processor below
                //   hoping that we are ahead.
                info!(
                    "Sending portfolio {:?} to processor {:?}.",
                    state.pricing_results.simple(),
                    self.processor_below.get_name(),
                );
                self.processor_below
                    .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                        state.trades.clone(),
                        state.pricing_results.clone(),
                        real_market.to_string(),
                        myself,
                    )))?;
                // stay in CalcuatingSingle mode.
            }

            (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::Idle) => {
                // start the new portfolio construction.
                info!("Message: NewTrade. Starting bulk computation.",);
                state.trades.insert(new_trade);

                let Some(ref real_market) = state.curr_market else {
                    warn!("Does not have real market. Ignoring the trade and continuing.");
                    return Ok(());
                };

                info!(
                    "Sending for bulk processing from {:?} to {:?}, trades#: {:?}",
                    self.processor_name.clone(),
                    self.processor_bulk.get_name(),
                    state.trades.len()
                );
                self.processor_bulk
                    .send_message(ProcessorBulkMessage::NewBulk((
                        real_market.to_string(),
                        state.trades.clone(),
                        myself,
                        state.pricing_metrics.clone(),
                    )))?;
                info!("State: Idle -> CalculatingBulk");
                state.processor_state = ProcessorMiddleState::CalculatingBulk;
            }

            (
                ProcessorMiddleMessage::NewTrade(new_trade),
                ProcessorMiddleState::CalculatingBulk, // | ProcessorMiddleState::CalculatingBulkMarketSwitch,
            ) => {
                info!("Message: NewTrade",);

                let Some(ref real_market) = state.curr_market else {
                    warn!("Does not have market. Ignoring and continuing.");
                    return Ok(());
                };

                info!("Updating trades and portfolio computing");
                if let Some(ref real_trade) = self.all_trades.get(&new_trade) {
                    if let Some(real_market) = self.all_markets.get(real_market) {
                        for pm in &state.pricing_metrics {
                            let new_trade_price =
                                real_trade.value_by_metric(*pm, real_market.clone()).await;
                            if let Some(portf_pm) = state.pricing_results.get_mut(pm) {
                                *portf_pm += new_trade_price; // portfolio update
                            } else {
                                warn!("Could not get pricing metric {:?} in portfolio", pm);
                            };
                        }

                        state.trades.insert(new_trade);
                    } else {
                        warn!("Could not get market {}", real_market);
                        state.trades_not_pricing.insert(new_trade);
                    }
                } else {
                    warn!("Could not get the representation of {}", new_trade);
                    state.trades_not_pricing.insert(new_trade);
                }

                // send downstream the updated portfolio
                info!(
                    "Sending from {:?} to {:?}: portfolio {:?}",
                    self.processor_name.clone(),
                    self.processor_below.get_name(),
                    state.pricing_results.simple(),
                );

                // TODO: CHECK IF THIS SHOULD BE HANDLED???
                let _ =
                    self.processor_below
                        .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                            state.trades.clone(),
                            state.pricing_results.clone(),
                            real_market.to_string(),
                            myself,
                        )));
            }

            (ProcessorMiddleMessage::NewMarket(_new_market), _) => {
                error!("Received NewMarket. THIS SHOULDNT HAPPEN! Ignoring and continuing.");
            }

            // this only comes from processor below
            (
                ProcessorMiddleMessage::Behind(market_behind, trades_behind),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                // we are behind trades behind the current processor

                // TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
                // TRADES ARE BEHIND,
                //    < 10 -> continue in single mode
                //    > 10 -> continue in bulk mode.
                if trades_behind.is_empty() {
                    // this processor below is ahead, reset the
                    //    processor to the new Idle state.
                    info!("Processor below accepted portfolio. State: <- Idle");
                    state.processor_state = ProcessorMiddleState::Idle;
                } else {
                    // we are still behind the below processor.
                    //   we add the trades to the trade list, and
                    //   send it to the bulk processor.
                    info!(
                        "Message: Behind: {} trades. Adding those trades to current population.",
                        trades_behind.len(),
                    );
                    state.trades.extend(trades_behind.clone()); // *trade_l += &trades_behind;

                    let Some(ref real_market) = state.curr_market else {
                        warn!(
                            "Processor does not have market: Destroying the market {}",
                            market_behind,
                        );
                        // TODO: CHECK HERE!!
                        //self.all_markets.remove(&market_behind); // TODO: IS THIS CORRECT
                        //self.all_markets
                        //    .insert_processor(self.processor_name.clone(), real_market.clone());
                        return Ok(());
                    };

                    self.all_markets
                        .insert_processor(self.processor_name.clone(), real_market.clone());

                    // if market_behind != *real_market {
                    //     warn!(
                    //         "{}: Destroying the market {}",
                    //         self.processor_name, market_behind,
                    //     );
                    //     self.all_markets.remove(&market_behind);
                    // }

                    info!(
                        "Behind: Sending bulk compute to {:?}.",
                        self.processor_bulk.get_name(),
                    );
                    self.processor_bulk
                        .send_message(ProcessorBulkMessage::NewBulk((
                            real_market.to_string(),
                            trades_behind,
                            myself,
                            state.pricing_metrics.clone(),
                        )))?;
                    info!("State: <- CalculatingBulk");
                    state.processor_state = ProcessorMiddleState::CalculatingBulk;
                }
            }

            (
                ProcessorMiddleMessage::Behind(market_behind, trades_behind),
                ProcessorMiddleState::CalculatingBulk, //                | ProcessorMiddleState::CalculatingBulkMarketSwitch,
            ) => {
                // if empty, dont do anything.
                if !trades_behind.is_empty() {
                    // the processor is behind the below processor, calculate the remaining trades.
                    // we are still behind the current processor.
                    // TODO: MAYBE WE CAN DIFFERENTIATE ON HOW MANY TRADES BEHIND???
                    info!(
                        "Lower processor did not accept the portfolio. Adding trades, that's all.",
                    );

                    state.trades.extend(trades_behind); // otherwise dont do anything.

                    // delete the market_behind if it's not market
                    // warn!(
                    //     "Market = None. Removing {} and Ignoring/Continuing.",
                    //     market_behind,
                    // );
                    // let Some(ref real_market) = state.curr_market else {
                    //     state.curr_market = Some(market_behind.clone());
                    //     self.all_markets
                    //         .insert_processor(self.processor_name.clone(), market_behind);
                    //     return Ok(());
                    // };

                    // // remove market_behind if not equal to current market here - should never happen
                    // self.all_markets
                    //     .insert_processor(self.processor_name.clone(), real_market.clone());
                    // // if *real_market != market_behind {
                    // //     warn!(
                    // //         "{}: Destroying market: {}",
                    // //         self.processor_name, market_behind,
                    // //     );
                    // //     self.all_markets.remove(&market_behind);
                    // // }

                    // info!("Sending for bulk compute.",);

                    // self.processor_bulk
                    //     .send_message(ProcessorBulkMessage::NewBulk((
                    //         real_market.to_string(),
                    //         state.trades.clone(),
                    //         myself,
                    //         state.pricing_metrics.clone(),
                    //     )))?;
                }
            }

            (
                ProcessorMiddleMessage::Behind(market_behind, trades_behind),
                ProcessorMiddleState::Idle,
            ) => {
                // we're in idle state, and have received a rejected market.

                if !trades_behind.is_empty() {
                    info!("Lower processor accepted portfolio. Starting new computations.");

                    let Some(ref real_market) = state.curr_market else {
                        warn!("Does not have market. Ignoring.");
                        return Ok(());
                    };

                    self.processor_bulk
                        .send_message(ProcessorBulkMessage::NewBulk((
                            real_market.to_string(),
                            trades_behind.clone(),
                            myself,
                            state.pricing_metrics.clone(),
                        )))?;
                    state.processor_state = ProcessorMiddleState::CalculatingBulk;
                    state.trades.extend(trades_behind); //*trade_l += &trades_behind;
                } else {
                    // remove the market_behind.
                    let Some(ref real_market) = state.curr_market else {
                        state.curr_market = Some(market_behind.clone());
                        self.all_markets
                            .insert_processor(self.processor_name.clone(), market_behind);
                        return Ok(());
                    };

                    if market_behind != *real_market {
                        warn!("Destroying market {}", market_behind,);
                        self.all_markets
                            .insert_processor(self.processor_name.clone(), real_market.clone());
                        // self.all_markets.remove(&market_behind);
                    }
                }
            }

            // bcp = (new_trade_l, computed_portf, offending_trades, _bulk_market)
            (ProcessorMiddleMessage::BulkReceive(_), ProcessorMiddleState::Idle) => {
                // Important: This Souldnt happen.
                // TODO: CHECK WHY THIS IS THE CASE???
                warn!("Message: BulkReceive: Ignoring bulk receive. Should not happen.",);
            }

            (
                ProcessorMiddleMessage::BulkReceive((
                    new_trade_l,
                    computed_portf,
                    offending_trades,
                    _bulk_market,
                )),
                ProcessorMiddleState::CalculatingBulk,
            ) => {
                // result of computation has arrived.
                // TODO: FINISH THIS HERE - what to do w/ offending trades???
                //    Nothing for now.
                info!(
                    "Message: BulkReceive|CalculatingBulk: Normal case. Going to CalculatingSingle.",
                );

                for (pm, computed_portf_pm) in computed_portf.iter() {
                    let portf_pm = state.pricing_results.get_mut(pm).unwrap();
                    *portf_pm += computed_portf_pm;
                }

                state.trades.extend(new_trade_l); //*trade_l += &new_trade_l;
                state.trades_not_pricing.extend(offending_trades); //*trades_non_pricing += &offending_trades;

                state.processor_state = ProcessorMiddleState::CalculatingSingle;

                let Some(ref real_market) = state.curr_market else {
                    warn!("Does not have market. Continuing.");
                    return Ok(());
                };

                debug!(
                    "Sending new trade portfolio to {:?}",
                    self.processor_below.get_name()
                );
                self.processor_below
                    .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                        state.trades.clone(),
                        state.pricing_results.clone(),
                        real_market.to_string(),
                        myself,
                    )))?;
            }

            (
                ProcessorMiddleMessage::BulkReceive((
                    new_trade_l,
                    computed_portf,
                    _offending_trades,
                    _bulk_market,
                )),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                // result of computation has arrived.
                //  add it to the computation
                // TODO: WHAT TO DO W/ OFFENDING TRADES???

                debug!(
                    "Message: BulkReceive: Sending to processor below {:?}.",
                    self.processor_below.get_name(),
                );
                state.pricing_results = computed_portf;
                state.trades.extend(new_trade_l);

                let Some(ref real_market) = state.curr_market else {
                    warn!("Processor does not have market. Ignoring.");
                    return Ok(());
                };

                self.processor_below
                    .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                        state.trades.clone(),
                        state.pricing_results.clone(),
                        real_market.to_string(),
                        myself,
                    )))?;
            }

            // (
            //     ProcessorMiddleMessage::BulkReceive(_),
            //     ProcessorMiddleState::CalculatingBulkMarketSwitch,
            // ) => {
            //     // ignore the message from bulk receive,
            //     info!(
            //         "Message: CalculatingBulkMarektSwitch: \
            //          Ignoring message from bulk receive as market was switched.",
            //     );
            // }

            // receiving a mesage from the processor above
            // ntp = (trades, potential_portfolio, market, upstream_processor)
            (ProcessorMiddleMessage::NewTradePortfolio(ntp), ProcessorMiddleState::Idle) => {
                // just pass it to the processor below, dont do anything else

                let (
                    ref potential_trades,
                    ref potential_portfolio,
                    ref new_market,
                    ref upstream_processor,
                ) = ntp;

                info!(
                    "Switching market: from {:?} <- {}",
                    state.curr_market, new_market,
                );

                // switch markets as well
                state.curr_market = Some(new_market.to_string());
                self.all_markets
                    .insert_processor(self.processor_name.clone(), new_market.clone());

                info!(
                    "Passing portfolio to lower processor {:?}",
                    self.processor_below.get_name(),
                );
                self.processor_below
                    .send_message(ProcessorMiddleMessage::NewTradePortfolio(ntp.clone()))?;

                // acknowledge to the sending processor that it was accepted.
                state.trades = potential_trades.clone();
                state.pricing_results = potential_portfolio.clone(); // TODO: CHECK HERE AND ABOVE

                // TODO: CHECK IF THIS SHOULD BE HANDLED???
                // market is Some, so unwrap is justified.
                let _ = upstream_processor.send_message(
                    ProcessorMiddleMessage::Behind(new_market.to_string(), TradesLocal::new()), // portfolio is accepted, notify the upstream that we're accepting
                );
            }

            // TODO: CHECK HERE IF ...MarketSwitch should be handled separately.
            (
                ProcessorMiddleMessage::NewTradePortfolio(ntp),
                ProcessorMiddleState::CalculatingBulk,
            ) => {
                // first approximation, ignore the portfolio - reject it, and send the message to the originator.
                let (potential_trades, _potential_portfolio, new_market, upstream_processor) = ntp;
                let _ = upstream_processor.send_message(ProcessorMiddleMessage::Behind(
                    new_market.to_string(),
                    potential_trades,
                ));

                // // we got new portfolio, but we are in the process of computing the portfolresult of computation has arrived.
                // let (potential_trades, potential_portfolio, new_market, upstream_processor) = ntp;

                // // let new_behind_curr = state
                // //     .trades
                // //     .iter()
                // //     .filter(|x| !potential_trades.contains(x.as_str()))
                // //     .cloned()
                // //     .collect::<TradesLocal>();

                // // if new_behind_curr.is_empty() {
                // //     info!(
                // //         "Received new portfolio, was ahead, sending portfolio to processor below {:?}",
                // //         self.processor_below.get_name(),
                // //     );

                // //     let Some(ref real_market) = state.curr_market else {
                // //         warn!("Does not have market. Ignoring.");
                // //         return Ok(());
                // //     };

                //     self.processor_below.send_message(
                //         ProcessorMiddleMessage::NewTradePortfolio((
                //             state.trades.clone(),
                //             state.pricing_results.clone(),
                //             real_market.to_string(),
                //             myself,
                //         )),
                //     )?;

                //     info!("New portfolio, Switching market.");

                //     // set the state of this processor to the state being sent.
                //     //self._switch_markets(market, &_new_market).await?;  // changes markets
                //     state.curr_market = Some(new_market.clone()); // market switch is simply a name change.
                //     self.all_markets
                //         .insert_processor(self.processor_name.clone(), new_market.clone());
                //     state.pricing_results = potential_portfolio;
                //     state.trades = potential_trades;
                //     state.processor_state = ProcessorMiddleState::CalculatingBulkMarketSwitch;
                // } else {
                //     debug!(
                //         "current portfolio: {:?}, new portfolio: {:?}. Ignoring the portfolio.",
                //         state.pricing_results.simple(),
                //         potential_trades, // TODO: THIS MIGHT BE OFF!
                //     );
                // }
                // // send upstream a message that the portfolio is accepted.
                // // TODO: CHECK IF THIS SHOULD BE BETTER HANDLED
                // // TODO: CHeck if market.unwrap() should be handled.
                // let _ = upstream_processor.send_message(ProcessorMiddleMessage::Behind(
                //     new_market.to_string(),
                //     new_behind_curr,
                // ));
            }

            (
                ProcessorMiddleMessage::NewTradePortfolio(ntp),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                // result of computation has arrived.
                //  add it to the computation
                // TODO: FINISH THIS HERE!!!

                debug!("Received new trade portfolio.");
                let (potential_trades, potential_portfolio, new_market, upstream_processor) = ntp; // new trade portfolio

                // TODO: WRONG - IMPLEMENT > JUST FOR REFERENCES!!!
                //let new_behind_curr = trade_l - &potential_trades;
                let new_behind_curr = state
                    .trades
                    .iter()
                    .filter(|&x| !potential_trades.contains(x))
                    .cloned()
                    .collect::<TradesLocal>();
                debug!(
                    "NewPortfolio: My trades: {}, Potential trades: {}, New trades: {}, New portf: {}",
                    state.trades.len(), potential_trades.len(), new_behind_curr.len(), potential_portfolio.len(),
                );

                if new_behind_curr.is_empty() {
                    // replace the portfolio and trades

                    info!(
                        "Received portfolio is ahead. accepting it and passing to processor below",
                    );
                    state.pricing_results = potential_portfolio;
                    state.trades.extend(potential_trades);
                    // TODO: HOW ABOUT pns ???
                    let Some(ref real_market) = state.curr_market else {
                        warn!("Does not have market. Ignoring.");
                        return Ok(());
                    };

                    self.processor_below.send_message(
                        ProcessorMiddleMessage::NewTradePortfolio((
                            state.trades.clone(),
                            state.pricing_results.clone(),
                            real_market.to_string(),
                            myself,
                        )),
                    )?;

                    if state.curr_market != Some(new_market.clone()) {
                        info!(
                            "Switching markets: {:?} <- {}",
                            state.curr_market, new_market,
                        );

                        state.curr_market = Some(new_market.clone());
                        self.all_markets
                            .insert_processor(self.processor_name.clone(), new_market.clone());
                    } // else no market change.
                }
                // sending upstream that we are done.
                // TODO: CHECK IF THIS SHOULD BE BETTER HANDLED
                // TODO: CHECK IF market.unwrap() should be handled.
                let _ = upstream_processor.send_message(
                    ProcessorMiddleMessage::Behind(new_market.to_string(), new_behind_curr), // TODO: CHECK IF THIS IS REALLY NEW_MARKET??
                );
            }

            (ProcessorMiddleMessage::BulkBusy, _) => {
                info!("Message: BulkBusy. Ignore for now.");
            }

            (ProcessorMiddleMessage::ProcessingStat(_), _) => {} // processing stat is not for this processor

            // we get new metrics from the metric dispatch
            (ProcessorMiddleMessage::Metric(new_pricing_metrics), _) => {
                info!("Changing metrics to {:?}", new_pricing_metrics);
                crate::utils::change_metrics(
                    &mut state.pricing_results,
                    new_pricing_metrics.clone(),
                );
                state.pricing_metrics = new_pricing_metrics;
            }
        }
        Ok(())
    }
}
