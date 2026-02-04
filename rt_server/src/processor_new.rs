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

#[derive(Debug)]
pub(crate) struct ProcessorNew<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    T: std::fmt::Debug,
{
    pub processor_name: String,
    pub processor_middle: ActorRef<ProcessorMiddleMessage<String>>, // the middle processor just below the ProcessorNew
    pub processor_bulk: ActorRef<ProcessorBulkMessage<String>>, // bulk processor reference to the bulk actor corresponding to this processor_new
    pub all_markets: Arc<AllMarkets<Arc<MT>>>,
    pub(crate) all_trades: Arc<TradeRep<T>>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum ProcessorNewState {
    CalculatingSingle, // when bulk has finished and we're only calculating single trades.
    CalculatingBulk,   // when we're still calculating bulk
    Idle,
}

impl std::fmt::Display for ProcessorNewState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T, MT> ProcessorNew<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    MT::MP: Clone,
    T: std::fmt::Debug,
{
    #[allow(dead_code)]
    pub(crate) fn new(
        processor_name: String,
        processor_middle: ActorRef<ProcessorMiddleMessage<String>>,
        processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
        all_trades: Arc<TradeRep<T>>,
    ) -> Self {
        Self {
            processor_name: processor_name.clone(),
            processor_middle,
            processor_bulk,
            all_markets,
            all_trades,
        }
    }

    #[instrument(skip(self, myself, pricing_metrics, trade_l, pns, new_m, new_market_name))]
    fn _process_new_market_idle(
        &self,
        new_market_name: String,
        new_m: &mut Option<String>,
        pns: &mut ProcessorNewState,
        trade_l: &mut HashSet<String>,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
        pricing_metrics: &mut Vec<PricingMetric>,
    ) -> Result<(), ActorProcessingErr> {
        // update the "new" market, and idle, change the new_m to future market

        let Some(new_market_val) = self.all_markets.get(&new_market_name) else {
            warn!(
                "Could not get market {} from all_markets. Ignoring the market and continuing.",
                new_market_name
            );
            return Ok(());
        };

        debug!("Market: {:?}", new_market_val);
        let new_market_val_name = new_market_val.market_name();
        debug!(
            "Inserting market {} into all_markets.",
            new_market_val_name.clone(),
        );
        // insert into markets
        self.all_markets.insert_both(
            self.processor_name.clone(),
            new_market_val_name.clone(),
            new_market_val,
        );
        *new_m = Some(new_market_name.clone());

        info!("New State: -> CalculatingBulk");
        *pns = ProcessorNewState::CalculatingBulk;
        info!(
            "Sending {} trades to bulk {:?}.",
            trade_l.len(),
            self.processor_bulk.get_name()
        );
        self.processor_bulk
            .send_message(ProcessorBulkMessage::NewBulk((
                new_market_val_name,
                trade_l.clone(),
                myself,
                pricing_metrics.clone(),
            )))?;

        Ok(())
    }
}

#[async_trait]
impl<T, MT> Actor for ProcessorNew<T, MT>
where
    T: Send + Sync + Clone + 'static + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
    MT: MarketTypeT + Send + Sync + 'static + std::fmt::Debug,
    MT::MP: Send + Sync + Clone,
{
    type Msg = ProcessorMiddleMessage<String>;

    // the state of the processor is:
    //   1st arg: hashset of trades,
    //   second is the list of trades that didnt price correctly -- check if this should also be TradesLocal???? TODO:
    //   third is the current portfolio result of correctly pricing trades.
    //   fourth is the computation state.
    //   fifth is the "new" market where we are pricing now. 'future' market exists anyway.
    //   6th: list of pricing metrics we are considering.
    type State = (
        TradesLocal,
        Vec<String>,
        PmPortfolio,
        ProcessorNewState,
        Option<String>,
        Vec<PricingMetric>,
    );
    type Arguments = (); // initial market

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments, // market parameters are passed here
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Starting ProcessorNew.");
        Ok((
            TradesLocal::new(),
            vec![],
            PmPortfolio::new(),
            ProcessorNewState::Idle,
            None,
            vec![],
        ))
    }

    #[instrument(
        skip(self, myself, message, state),
        fields(
            name=%self.processor_name,
            state = %state.3,
            new_m = state.4,
        )
    )]
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let (trade_l, trades_non_pricing, portf, pns, new_m, pricing_metrics) = state;

        debug!(
            "Current State: {:?}. Portf: {}, Nb trades: {}",
            pns,
            portf.simple(),
            trade_l.len()
        );

        match message {
            ProcessorMiddleMessage::NewTrade(new_trade) => {
                info!("Message: NewTrade({:?})", new_trade);

                match pns {
                    // what is the processor doing right now.
                    ProcessorNewState::CalculatingSingle => {
                        // add the trade to the new portfolio and
                        //   attempt again.
                        info!("CalculatingSingle, Computing trade {}.", new_trade);
                        trade_l.insert(new_trade.clone()); // we add the trade to the list.

                        // the next 3 are conditions when we can actually compute something
                        // condition if we can get the relevant trade
                        let Some(new_trade_info) = self.all_trades.get(&new_trade) else {
                            // we dont have a trade info - ignore and continue.
                            warn!(
                                "No trade info could be obtained for {}. Investigate. Continuing w/o processing.",
                                new_trade
                            );
                            return Ok(());
                        };
                        // condition if new_m is a market or just None
                        let Some(new_m_real) = new_m else {
                            warn!("Do not have new_m. Continuing w/o processing.");
                            return Ok(());
                        };
                        // condition if we can find new_m in the all_markets.
                        let Some(new_m_actual) = self.all_markets.get(new_m_real) else {
                            warn!(
                                "Market {} not in all_markets. Continuing w/o processing.",
                                new_m_real
                            );
                            return Ok(());
                        };

                        // if all the three conditions above are satisfied, continue
                        //   w/ actual pricing.
                        for pm in pricing_metrics {
                            let new_trade_price_pm = new_trade_info
                                .value_by_metric(*pm, new_m_actual.clone())
                                .await;
                            debug!("New trade price: {:?}", new_trade_price_pm);
                            match portf.get_mut(pm) {
                                Some(portf_pm) => {
                                    *portf_pm += new_trade_price_pm;
                                }
                                None => {
                                    let mut portfolio_pm = PortfolioType::default();
                                    portfolio_pm += new_trade_price_pm;
                                    portf.insert(*pm, portfolio_pm);
                                }
                            }
                        }

                        // we send the computed portfolio & trades to the current processor
                        //   hoping that we are ahead.
                        info!(
                            "Sending to {:?}: trade# = {}, portf # = {}, new_m = {:?}.",
                            self.processor_middle.get_name(),
                            trade_l.len(),
                            portf.simple(),
                            new_m_real,
                        );
                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio((
                                trade_l.clone(),
                                portf.clone(),
                                new_m_real.to_string(),
                                myself,
                            )),
                        )?;
                    }

                    ProcessorNewState::Idle => {
                        // start the new portfolio construction.
                        trade_l.insert(new_trade); // *trade_l += &new_trade;
                        match new_m {
                            None => {
                                warn!("Does not have new_m. Continuing w/o processing trades.");
                                return Ok(());
                            }
                            Some(new_m_real) => {
                                info!(
                                    "Sending all {} trades to bulk processor: {:?}.",
                                    trade_l.len(),
                                    self.processor_bulk.get_name(),
                                );
                                info!("New State: -> CalculatingBulk.");
                                *pns = ProcessorNewState::CalculatingBulk;
                                self.processor_bulk
                                    .send_message(ProcessorBulkMessage::NewBulk((
                                        new_m_real.to_string(),
                                        trade_l.clone(),
                                        myself,
                                        pricing_metrics.clone(),
                                    )))?;
                            }
                        }
                    }

                    ProcessorNewState::CalculatingBulk => {
                        info!("Adding trade {}", new_trade.clone());
                        trade_l.insert(new_trade.clone()); // we add the trade to the list.

                        let Some(new_trade_info) = self.all_trades.get(&new_trade) else {
                            warn!("Could not get trade {}. Continuing.", new_trade);
                            return Ok(());
                        };

                        let Some(new_m_real) = new_m else {
                            warn!("Does not have new_m. Ignoring and continuing.");
                            return Ok(());
                        };

                        let Some(new_m_actual) = self.all_markets.get(new_m_real) else {
                            warn!("all_markets does not have {}. Continuing", new_m_real);
                            return Ok(());
                        };

                        for pm in pricing_metrics {
                            let new_trade_price = new_trade_info
                                .value_by_metric(*pm, new_m_actual.clone())
                                .await;

                            match portf.get_mut(pm) {
                                Some(portf_pm) => {
                                    *portf_pm += new_trade_price;
                                }
                                None => {
                                    let mut portfolio_pm = PortfolioType::default();
                                    portfolio_pm += new_trade_price;
                                    portf.insert(*pm, portfolio_pm);
                                }
                            }
                        }

                        info!(
                            "Sending to lower processor {:?}. Portf size: {}",
                            self.processor_middle.get_name(),
                            portf.simple(),
                        );
                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio((
                                trade_l.clone(),
                                portf.clone(),
                                new_m_real.to_string(),
                                myself,
                            )),
                        )?;
                    }
                }
            }

            // this is coming from mkt_handler, and market handler only produces
            //   "future" market.  Here we can potentially create a new market
            ProcessorMiddleMessage::NewMarket(new_market_name) => {
                debug!(
                    "Message NewMarket: {} <- this should be 'future'",
                    new_market_name
                );

                match pns {
                    ProcessorNewState::Idle => self._process_new_market_idle(
                        new_market_name,
                        new_m,
                        pns,
                        trade_l,
                        myself,
                        pricing_metrics,
                    )?,
                    // what is the processor doing right now
                    // ProcessorNewState::Idle => {
                    //     // update the "new" market, and idle, change the new_m to future market

                    //     let Some(new_market_val) = self.all_markets.get(&new_market_name) else {
                    //         warn!("Could not get market {} from all_markets. Ignoring the market and continuing.", new_market_name);
                    //         return Ok(());
                    //     };

                    //     debug!("Market: {:?}", new_market_val);
                    //     let new_market_val_name = new_market_val.market_name();
                    //     debug!(
                    //         "Inserting market {} into all_markets.",
                    //         new_market_val_name.clone(),
                    //     );
                    //     // insert into markets
                    //     self.all_markets.insert_both(
                    //         self.processor_name.clone(),
                    //         new_market_val_name.clone(),
                    //         new_market_val,
                    //     );
                    //     //self.all_markets
                    //     //    .insert_processor(self.processor_name.clone(), new_market_name.clone());
                    //     *new_m = Some(new_market_name.clone());

                    //     info!("New State: -> CalculatingBulk");
                    //     *pns = ProcessorNewState::CalculatingBulk;
                    //     info!(
                    //         "Sending {} trades to bulk {:?}.",
                    //         trade_l.len(),
                    //         self.processor_bulk.get_name()
                    //     );
                    //     self.processor_bulk
                    //         .send_message(ProcessorBulkMessage::NewBulk((
                    //             new_market_val_name,
                    //             trade_l.clone(),
                    //             myself,
                    //             pricing_metrics.clone(),
                    //         )))?;
                    // }
                    ProcessorNewState::CalculatingSingle => {
                        // TODO: CHECK HERE?? SHOULD WE REIGNITE THE COMPUTATION OR
                        //    NOT DO ANYTHING. FOR NOW: DONT DO ANYTHING.
                        return Ok(());

                        // Calculating and getting a new market. Not doing anything.

                        // we're calculating, update the future market, not current
                        //   "new_market" here should always be "future" - otherwise there's
                        //   something wrong.
                        // let Some(new_market_val) = self.all_markets.get(&new_market) else {
                        //     warn!("Could not find market {}. Continuing", new_market);
                        //     return Ok(());
                        // };

                        // TODO: THIS IS WEIRD!!! - WHY ARE WE DOING ANYHING???

                        // TODO: HERE TO FINISH - I DONT THINK WE SHOULD DO ANYTHING HERE!!!
                        // let Some(new_m_real) = new_m else {
                        //     self.all_markets.insert_processor(
                        //         self.processor_name.clone(),
                        //         new_market_name.clone(),
                        //     );
                        //     *new_m = Some(new_market_name);
                        //     // return Ok(());
                        // };

                        // debug!(
                        //     "Sending portfolio {} to lower processor {:?}",
                        //     portf.simple(),
                        //     self.processor_middle.get_name(),
                        // );
                        // self.processor_middle.send_message(
                        //     ProcessorMiddleMessage::NewTradePortfolio((
                        //         trade_l.clone(),
                        //         portf.clone(),
                        //         new_m_real.to_string(),
                        //         myself,
                        //     )),
                        // )?;
                    }
                    // ignore if new market comes in, no
                    //   action taken.
                    ProcessorNewState::CalculatingBulk => {
                        return Ok(());
                        // just update the future market
                        //info!("Switching new_m <- future.");
                        //let Some(future_market) = self.all_markets.get(&"future".to_string())
                        //else {
                        //    warn!(
                        //        "Could not find 'future' market. This is weird. Continuing w/o it."
                        //    );
                        //    return Ok(());
                        //};
                        //let future_market_name = future_market.market_name();

                        // info!("Switching markets: New_m <- {}", new_market_name.clone());
                        // *new_m = Some(new_market_name.clone());
                        // self.all_markets
                        //     .insert_processor(self.processor_name.clone(), new_market_name.clone());
                        // self.all_markets.insert_both(
                        //     self.processor_name.clone(),
                        //     future_market_name.clone(),
                        //     future_market,
                        // );
                    }
                }
            }

            // this only comes from ProcessorMiddle,
            //
            ProcessorMiddleMessage::Behind(market_behind, trades_behind) => {
                // we are behind trades behind the below processor
                info!(
                    "Message: Behind, market: {:?}, trades_beind: {:?}",
                    market_behind, trades_behind
                );
                match pns {
                    // what is the processor doing right now
                    ProcessorNewState::CalculatingSingle => {
                        // TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
                        // TRADES ARE BEHIND,
                        //    < 10 -> continue in single mode
                        //    > 10 -> continue in bulk mode.

                        // the processor middle has accepted the market_behind
                        // make new_m <- future.
                        if trades_behind.is_empty() {
                            // new processor is ahead, reset the
                            //    new processor to the new default state.
                            debug!(
                                "Lower processor accepted portfolio. Resetting portfolio: portf = empty"
                            );
                            *portf = PmPortfolio::new();

                            // this shouldnt fail, but we have a failsafe
                            let Some(future_market) = self.all_markets.get(&"future".to_string())
                            else {
                                warn!("Could not find 'future' market. This is weird. Continuing w/o it.");
                                return Ok(());
                            };
                            let future_market_name = future_market.market_name();

                            debug!(
                                "Switching markets: New_m <- future market {}",
                                future_market_name.clone()
                            );
                            *new_m = Some(future_market_name.clone());
                            self.all_markets.insert_both(
                                self.processor_name.clone(),
                                future_market_name.clone(),
                                future_market,
                            );

                            info!("State: {:?} -> {:?}", pns, ProcessorNewState::Idle); // from pns -> Idle
                            *pns = ProcessorNewState::Idle;
                        } else {
                            // we are still behind the current processor. We destroy market_behind, and continue
                            //   computing on new_m.
                            // TODO: HERE COMES IN HEURISTICS, WHETHER TO SWITCH TO THE FUTURE MARKET.
                            info!("Lower processor rejected portfolio.");

                            // destroying the market_behind
                            match new_m {
                                None => {
                                    info!(
                                        "New_m is None, doing: New_m <- {}",
                                        market_behind.clone()
                                    );
                                    *new_m = Some(market_behind.clone());
                                    self.all_markets.insert_processor(
                                        self.processor_name.clone(),
                                        market_behind,
                                    );
                                }
                                Some(real_market) => {
                                    // if market_behind != *real_market {
                                    self.all_markets.insert_processor(
                                        self.processor_name.clone(),
                                        real_market.to_string(),
                                    );
                                }
                            }

                            // TODO: CHECK HERE!!! THIS PROBABLY DOESNT WORK
                            // trade_l.extend(trades_behind.clone());  //*trade_l += &trades_behind;

                            // check if we have a new_m
                            let Some(new_m_real) = new_m else {
                                warn!("Does not have new_m. Ignoring and continuing.");
                                return Ok(());
                            };

                            // if we have it, compute it
                            info!(
                                "Sending to bulk {:?}: trades_behind: {}",
                                self.processor_bulk.get_name(),
                                trades_behind.len(),
                            );

                            self.processor_bulk
                                .send_message(ProcessorBulkMessage::NewBulk((
                                    new_m_real.to_string(),
                                    trades_behind,
                                    myself,
                                    pricing_metrics.clone(),
                                )))?;
                            info!("New State: <- CalculatingBulk");
                            *pns = ProcessorNewState::CalculatingBulk;
                        }
                    }

                    // we are calculating bulk, and we received info
                    //   from processor below.
                    ProcessorNewState::CalculatingBulk => {
                        // add the trades to portfolio, nothing else.
                        if !trades_behind.is_empty() {
                            info!(
                                "Adding non-computed trades {} to trade list. Not doing anything.",
                                trades_behind.len()
                            );
                            trade_l.extend(trades_behind);
                        }
                    }

                    ProcessorNewState::Idle => {
                        if trades_behind.is_empty() {
                            return Ok(());
                        }

                        // TODO: CHECK HERE!!!
                        // trade_l.extend(trades_behind);  // *trade_l += &trades_behind;

                        let Some(new_m_real) = new_m else {
                            warn!("No new_m market. Ignoring and continuing.");
                            return Ok(());
                        };

                        info!(
                            "Starting new bulk compute: {:?}, trades {}",
                            self.processor_bulk.get_name(),
                            trade_l.len()
                        );
                        self.processor_bulk
                            .send_message(ProcessorBulkMessage::NewBulk((
                                new_m_real.to_string(),
                                trade_l.clone(),
                                myself,
                                pricing_metrics.clone(),
                            )))?;
                        info!("New State: <- CalculatingBulk",);
                        *pns = ProcessorNewState::CalculatingBulk;
                    }
                }
            }

            // _bulk market is not needed, as it is the same as either new_m.
            ProcessorMiddleMessage::BulkReceive((
                new_trade_l,
                computed_portf,
                offending_trades,
                _bulk_market,
            )) => {
                info!(
                    "Message: BulkReceive: Portfolio: {:?}",
                    computed_portf.len()
                );
                debug!("Portfolio = {:?}", computed_portf.simple());

                match pns {
                    ProcessorNewState::Idle => {
                        info!("Ignoring bulk receive as we're in Idle."); // TODO: CHECK THIS PART
                    }

                    ProcessorNewState::CalculatingBulk => {
                        // result of computation has arrived.
                        // TODO: FINISH THIS HERE!!!
                        info!(
                            "Assigning computed portfolio of {:?} to portfolio",
                            computed_portf.simple()
                        );
                        for (pm, comp_portf_pm) in computed_portf.iter() {
                            match portf.get_mut(pm) {
                                Some(portf_pm) => {
                                    *portf_pm += comp_portf_pm;
                                }
                                None => {
                                    let mut portfolio_pm = PortfolioType::default();
                                    portfolio_pm += comp_portf_pm;
                                    portf.insert(*pm, portfolio_pm);
                                }
                            }
                        }
                        debug!("Portfolio = {}", portf.simple());

                        // TODO: CHECK HERE!!!
                        trade_l.extend(new_trade_l); // *trade_l += &new_trade_l;
                                                     // *trades_non_pricing += &offending_trades;
                        trades_non_pricing.extend(offending_trades);
                        info!("Extending trades: Now {:?} trades.", trade_l.len());
                        let Some(new_m_real) = new_m else {
                            warn!("No new_m market. Ignoring and continuing.");
                            return Ok(());
                        };

                        info!(
                            "New State: Portf: {:?}, trades: {:?}",
                            portf.simple(),
                            trade_l.len(),
                        );
                        info!(
                            "Sending to lower processor {:?}, portf size: {}",
                            self.processor_middle.get_name(),
                            portf.simple(),
                        );
                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio((
                                trade_l.clone(),
                                portf.clone(),
                                new_m_real.to_string(),
                                myself,
                            )),
                        )?;
                        info!("New State: <- CalculatingSingle");
                        *pns = ProcessorNewState::CalculatingSingle;
                    }
                    ProcessorNewState::CalculatingSingle => {
                        // result of computation has arrived.
                        //  add it to the computation
                        // TODO: FINISH THIS HERE!!!
                        // panic!("Received BulkReceive while calculating single - Weird");

                        *portf = computed_portf;
                        //*trade_l += &new_trade_l;
                        trade_l.extend(new_trade_l);
                        info!("Portf: {:?}, trades: {:?}", portf.len(), trade_l.len(),);
                        let Some(new_m_real) = new_m else {
                            warn!("No new_m market. Ignoring and continuing.");
                            return Ok(());
                        };

                        info!(
                            "Sending to lower processor {:?}. Portf size: {}",
                            self.processor_middle.get_name(),
                            portf.simple(),
                        );
                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio((
                                trade_l.clone(),
                                portf.clone(),
                                new_m_real.to_string(),
                                myself,
                            )),
                        )?;
                    }
                }
            }

            ProcessorMiddleMessage::Metric(new_pricing_metrics) => {
                info!("Changing metrics to {:?}", new_pricing_metrics);
                crate::utils::change_metrics(portf, new_pricing_metrics.clone()); // fixes the portf to correspond to new_pricing_metrics
                *pricing_metrics = new_pricing_metrics;
            }

            _ => {
                // this type shouldnt occur
                //   TODO: Better error message
                panic!("This Message type shouldnt occur.");
            }
        }
        Ok(())
    }
}
