use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use tracing::{debug, info, warn}; // instrument

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
    pub market_name: (Option<String>, Option<String>), // first item: new market, second item: future market.
}

#[derive(Debug)]
pub enum ProcessorNewState {
    CalculatingSingle, // when bulk has finished and we're only calculating single trades.
    CalculatingBulk,   // when we're still calculating bulk
    Idle,
}

impl<T, MT> ProcessorNew<T, MT>
where
    MT: MarketTypeT + std::fmt::Debug,
    MT::MP: Clone,
    T: std::fmt::Debug,
{
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
            market_name: (None, None), // new, future market
        }
    }
}

// impl<T, MT: MarketTypeT> MarketSwitching for ProcessorNew<T, MT> {

//     fn processor_name(&self) -> String {
//         self.processor_name.clone()
//     }

//     fn all_markets(&self) -> std::sync::Arc<AllMarkets<MT>> {
// 	self.all_markets.clone()
//     }

//     fn r_client(&self) ->  Option<&reqwest::Client> {
//         self.r_client.as_ref()
//     }

//     fn market_endpoint(&self) -> String {
// 	format!("http://{0}/market", self.pricing_options.market_server.clone())
//     }
// }

// impl<T, MT> Decoder for ProcessorNew<T, MT> {}

// impl<ReductionType, T, MT> RestPricerSpark<ReductionType> for ProcessorNew<T, MT>
// where
//     ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
//     ProcessorNew: Decoder,
// {

//     fn _pricing_server_spark(&self) -> String {
// 	self.pricing_options.pricing_server.clone()
//     }

//     fn _pricing_endpoint_spark(&self, _market_: MT, _metric: PricingMetric) -> String {
// 	"/pricing".to_string()
//     }
// }

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

    // #[instrument(skip(myself, message, state),level= "debug")]
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let (trade_l, trades_non_pricing, portf, pns, new_m, pricing_metrics) = state;

        info!(
            "State: {:?}. Portf size: {}, Nb trades: {}",
            pns,
            portf.len(),
            trade_l.len()
        );

        match message {
            ProcessorMiddleMessage::NewTrade(new_trade) => {
                //*trade_l += &new_trade;  // we add the trade to the list.

                match pns {
                    // what is the processor doing right now.
                    ProcessorNewState::CalculatingSingle => {
                        // add the trade to the new portfolio and
                        //   attempt again.
                        info!(
                            "CalculatingSingle, NewTrade:, computing trade {}.",
                            new_trade
                        );

                        // the next 3 are conditions when we can actually compute something
                        // condition if we can get the relevant trade
                        let Some(new_trade_info) = self.all_trades.get(&new_trade) else {
                            // we dont have a trade info - ignore and continue.
                            warn!(
                                "No trade info could be obtained for {}. Continuing.",
                                new_trade
                            );
                            return Ok(());
                        };
                        // condition if new_m is a market or just None
                        let Some(new_m_real) = new_m else {
                            warn!("Do not have new_m. Continuing.");
                            return Ok(());
                        };
                        // condition if we can find new_m in the all_markets.
                        let Some(new_m_actual) = self.all_markets.get(new_m_real) else {
                            warn!("Market {} not in all_markets", new_m_real);
                            return Ok(());
                        };

                        // if all the three conditions above are satisfied, continue
                        //   w/ actual pricing.
                        for pm in pricing_metrics {
                            let new_trade_price_pm = new_trade_info
                                .value_by_metric(*pm, new_m_actual.clone())
                                .await;
                            // *portf += new_trade_price;  // portfolio update

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

                            //let port_pm = portf.get_mut(pm).unwrap();
                            //*port_pm += new_trade_price_pm;
                        }

                        trade_l.insert(new_trade); // we add the trade to the list.

                        // we send the computed portfolio & trades to the current processor
                        //   hoping that we are ahead.
                        info!("CalculatingSingle, NewTrade: Sending to middle processor.",);
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
                        info!(
                            "Idle, NewTrade: Sending all {} trades to bulk. Going -> CalculatingBulk.",
                            trade_l.len(),
                        );

                        trade_l.insert(new_trade); // *trade_l += &new_trade;
                        match new_m {
                            None => {
                                warn!("Does not have new_m. Ignoring for now.");
                                return Ok(());
                            }
                            Some(new_m_real) => {
                                self.processor_bulk
                                    .send_message(ProcessorBulkMessage::NewBulk((
                                        new_m_real.to_string(),
                                        trade_l.clone(),
                                        myself,
                                        pricing_metrics.clone(),
                                    )))?;
                                *pns = ProcessorNewState::CalculatingBulk;
                            }
                        }
                    }

                    ProcessorNewState::CalculatingBulk => {
                        info!("CalculatingBulk, NewTrade: adding trade and sending to lower.");

                        trade_l.insert(new_trade.clone()); // we add the trade to the list.

                        let Some(new_trade_info) = self.all_trades.get(&new_trade) else {
                            warn!("Could not get trade {}. Continuing", new_trade);
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
                            // TODO: REMOVE THESE TWO LINES LATER IF ALL WORKS
                            // let portf_pm = portf.get_mut(pm).unwrap();
                            // *portf_pm += new_trade_price; // portfolio update
                        }

                        info!(
                            "CalculatingBulk, NewTrade: Sending to lower processor. Portf size: {}",
                            portf.len(),
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
            ProcessorMiddleMessage::NewMarket(new_market) => {
                debug!("Getting NewMarket: {}", new_market); // TODO: IMPORTANT: this should be "future".

                match pns {
                    // what is the processor doing right now
                    ProcessorNewState::Idle => {
                        // update the "new" market, and idle, change the new_m to future market
                        info!("Idle, NewMarket: setting future market: {:?}", new_market);

                        // removing the old new_m market
                        match new_m {
                            None => {
                                warn!("new_m is None. Not doing anything");
                            }
                            Some(real_market) => {
                                warn!("Destroying market {}", real_market);
                                self.all_markets.remove(real_market);
                            }
                        }

                        // IMPORTANT: new_market IS "future", so get this.
                        let Some(new_market_val) = self.all_markets.get(&new_market) else {
                            warn!("Could not get market {} from all_markets. Ignoring the market and continuing", new_market);
                            return Ok(());
                        };

                        let new_market_val_name = new_market_val.market_name();
                        info!(
                            "Inserting market {} into all_markets.",
                            new_market_val_name.clone(),
                        );
                        self.all_markets
                            .insert(new_market_val_name.clone(), new_market_val); // insert the value under the new name
                        info!("All_markets: {:?}", self.all_markets.list_market_names());
                        *new_m = Some(new_market_val_name.clone());

                        info!("Idle, NewMarket: sending to bulk. State -> CalculatingBulk");
                        *pns = ProcessorNewState::CalculatingBulk;
                        self.processor_bulk
                            .send_message(ProcessorBulkMessage::NewBulk((
                                new_market_val_name,
                                trade_l.clone(),
                                myself,
                                pricing_metrics.clone(),
                            )))?;
                    }

                    ProcessorNewState::CalculatingSingle => {
                        // Calculating and getting a new market. Not doing anything.
                        info!(
                            "CalculatingSingle, NewMarket: sending portfolio \
                             to lower processor, portf size: {}",
                            portf.len(),
                        );

                        // we're calculating, update the future market, not current
                        //   "new_market" here should always be "future" - otherwise there's
                        //   something wrong.
                        // let Some(new_market_val) = self.all_markets.get(&new_market) else {
                        //     warn!("Could not find market {}. Continuing", new_market);
                        //     return Ok(());
                        // };

                        // TODO: HERE TO FINISH - I DONT THINK WE SHOULD DO ANYTHING HERE!!!
                        let Some(new_m_real) = new_m else {
                            warn!("Does not have new_m market. This is weird. Investigate. Contiuning and ignoring.");
                            return Ok(());
                        };

                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio((
                                trade_l.clone(),
                                portf.clone(),
                                new_m_real.to_string(),
                                myself,
                            )),
                        )?;
                    }
                    // ignore if new market comes in, no
                    //   action taken.
                    ProcessorNewState::CalculatingBulk => {
                        // just update the future market
                        info!("CalculatingBulk, NewMarket: Not doing anything.");

                        // TODO: CHECK IF THIS REALLY NEEDS TO BE DONE.
                        //    COMMENTED OUT FOR NOW!!!
                        // new_m market should be updated.
                        // HERE IT STARTS:
                        // let Some(new_market_val) = self.all_markets.get(&new_market) else {
                        //     warn!("Could not get market {} from all_markets. Ignoring the market and continuing", new_market);
                        //     return Ok(());
                        // };
                        // let new_market_name = new_market_val.market_name();
                        // *new_m = Some(new_market_name);
                    }
                }
            }

            // this only comes from ProcessorMiddle,
            //
            ProcessorMiddleMessage::Behind(market_behind, trades_behind) => {
                // we are behind trades behind the below processor
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
                            info!(
                                "Behind, CalculatingSingle: Successfully accepted. Resetting portfolio."
                            );
                            *portf = PmPortfolio::new();

                            // this shouldnt fail, but we have a failsafe
                            let Some(future_market) = self.all_markets.get(&"future".to_string())
                            else {
                                warn!("Could not find 'future' market. This is weird. Continuing w/o it.");
                                return Ok(());
                            };
                            let future_market_name = future_market.market_name();

                            info!("Behind, CalculatingSingle: Switching markets: New_m <- future market {}", future_market_name.clone());
                            *new_m = Some(future_market_name.clone());

                            info!("{:?} -> {:?}", pns, ProcessorNewState::Idle); // from pns -> Idle
                            *pns = ProcessorNewState::Idle;
                        } else {
                            // we are still behind the current processor. We destroy market_behind, and continue
                            //   computing on new_m.
                            // TODO: HERE COMES IN HEURISTICS, WHETHER TO SWITCH TO THE FUTURE MARKET.

                            // destroying the market_behind

                            match new_m {
                                None => {
                                    *new_m = Some(market_behind);
                                    // warn!("Removing market {}", market_behind);
                                    // self.all_markets.remove(&market_behind);
                                }
                                Some(real_market) => {
                                    if market_behind != *real_market {
                                        // TODO: DO THIS unwrap nicer
                                        warn!("Destroying market {}", market_behind);
                                        self.all_markets.remove(&market_behind);
                                        // also destroy in this case
                                    }
                                }
                            }

                            info!(
                                "State: {:?}: Still behind lower processor, adding trades ({}) and computing bulk.",
                                pns, trade_l.len(),
                            );

                            // TODO: CHECK HERE!!! THIS PROBABLY DOESNT WORK
                            // trade_l.extend(trades_behind.clone());  //*trade_l += &trades_behind;

                            // check if we have a new_m
                            let Some(new_m_real) = new_m else {
                                warn!("Does not have new_m. Ignoring and continuing.");
                                return Ok(());
                            };

                            // if we have it, compute it
                            self.processor_bulk
                                .send_message(ProcessorBulkMessage::NewBulk((
                                    new_m_real.to_string(),
                                    trades_behind,
                                    myself,
                                    pricing_metrics.clone(),
                                )))?;
                            *pns = ProcessorNewState::CalculatingBulk;
                        }
                    }

                    // we are calculating bulk, and we received info
                    //   from processor below.
                    ProcessorNewState::CalculatingBulk => {
                        // add the trades to portfolio, nothing else.
                        if !trades_behind.is_empty() {
                            info!(
                                "CalculatingBulk, Behind: adding non-computed trades to trade list."
                            );
                            trade_l.extend(trades_behind);
                        }
                    }

                    ProcessorNewState::Idle => {
                        if trades_behind.is_empty() {
                            return Ok(());
                        }

                        info!("Idle, Behind: Starting new bulk compute.");
                        // TODO: CHECK HERE!!!
                        // trade_l.extend(trades_behind);  // *trade_l += &trades_behind;

                        let Some(new_m_real) = new_m else {
                            warn!("No new_m market. Ignoring and continuing.");
                            return Ok(());
                        };

                        self.processor_bulk
                            .send_message(ProcessorBulkMessage::NewBulk((
                                new_m_real.to_string(),
                                trade_l.clone(),
                                myself,
                                pricing_metrics.clone(),
                            )))?;
                        info!("Idle, Behind: Going into state -> CalculatingBulk",);
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
                match pns {
                    ProcessorNewState::Idle => {
                        info!("Idle: Ignoring bulk receive."); // TODO: CHECK THIS PART
                    }

                    ProcessorNewState::CalculatingBulk => {
                        // result of computation has arrived.
                        // TODO: FINISH THIS HERE!!!
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

                            //let portf_pm = portf.get_mut(pm).unwrap();
                            // *portf_pm += comp_portf_pm;
                        }

                        // TODO: CHECK HERE!!!
                        trade_l.extend(new_trade_l); // *trade_l += &new_trade_l;
                                                     // *trades_non_pricing += &offending_trades;
                        trades_non_pricing.extend(offending_trades);

                        let Some(new_m_real) = new_m else {
                            warn!("No new_m market. Ignoring and continuing.");
                            return Ok(());
                        };

                        info!(
                            "CalculatingBulk, BulkReceive: sending to lower processor, portf size: {}",
                            portf.len(),
                        );
                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio((
                                trade_l.clone(),
                                portf.clone(),
                                new_m_real.to_string(),
                                myself,
                            )),
                        )?;
                        info!("CalculatingBulk, BulkReceive: Going to Single computation mode",);
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

                        let Some(new_m_real) = new_m else {
                            warn!("No new_m market. Ignoring and continuing.");
                            return Ok(());
                        };

                        info!(
                            "CalculatingSingle, BulkReceive: sending to lower processor. Portf size: {}",
                            portf.len(),
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
