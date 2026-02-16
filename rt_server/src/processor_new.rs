use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
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
    MT: MarketTypeT + Send + Sync + 'static + std::fmt::Debug,
    // MT: MarketTypeT + std::fmt::Debug,
    MT::MP: Clone,
    T: Send + Sync + Clone + 'static + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
    // T: std::fmt::Debug,
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

    // we receive new market when we're idle.
    // use this market and start a new bulk computation.
    #[instrument(
        skip_all,
        fields(
            processor_name=self.processor_name,
            new_m=state.new_market,
        )
    )]
    fn _process_new_market_idle(
        &self,
        new_market_name: String,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
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
        state.new_market = Some(new_market_val_name.clone());

        info!("New State: {} -> CalculatingBulk", state.processor_state);
        state.processor_state = ProcessorNewState::CalculatingBulk;
        info!(
            "Sending {} trades to bulk {:?}.",
            state.trades.len(),
            self.processor_bulk.get_name()
        );

        self.processor_bulk
            .send_message(ProcessorBulkMessage::NewBulk((
                new_market_val_name,
                state.trades.clone(),
                myself,
                state.pricing_metrics.clone(),
            )))?;

        Ok(())
    }

    // message = new trade, state = calculating single
    #[instrument(skip_all)]
    async fn _new_trade_calculating_single(
        &self,
        new_trade: String,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        info!("CalculatingSingle, Computing trade {}.", new_trade);
        state.trades.insert(new_trade.clone()); // we add the trade to the list.

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
        let Some(ref new_m_real) = &state.new_market else {
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
        for pm in &state.pricing_metrics {
            let new_trade_price_pm = new_trade_info
                .value_by_metric(*pm, new_m_actual.clone())
                .await;
            debug!("New trade price: {:?}", new_trade_price_pm);
            match state.portfolio.get_mut(pm) {
                Some(portf_pm) => {
                    *portf_pm += new_trade_price_pm;
                }
                None => {
                    let mut portfolio_pm = PortfolioType::default();
                    portfolio_pm += new_trade_price_pm;
                    state.portfolio.insert(*pm, portfolio_pm);
                }
            }
        }

        // we send the computed portfolio & trades to the current processor
        //   hoping that we are ahead.
        info!(
            "Sending to {:?}: trade# = {}, portf # = {:?}, new_m = {:?}.",
            self.processor_middle.get_name(),
            state.trades.len(),
            state.portfolio,
            new_m_real,
        );
        self.processor_middle
            .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                state.trades.clone(),
                state.portfolio.clone(),
                new_m_real.to_string(),
                myself,
            )))?;

        Ok(())
    }

    // message = new trade, state = idle.
    #[instrument(skip_all)]
    fn _new_trade_idle(
        &self,
        new_trade: String,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // start the new portfolio construction.
        state.trades.insert(new_trade);
        match &state.new_market {
            None => {
                warn!("Does not have new_m. Continuing w/o processing trades.");
                return Ok(());
            }
            Some(new_m_real) => {
                info!(
                    "Sending all {} trades to bulk processor: {:?}.",
                    state.trades.len(),
                    self.processor_bulk.get_name(),
                );
                info!("New State: -> CalculatingBulk.");
                state.processor_state = ProcessorNewState::CalculatingBulk;
                self.processor_bulk
                    .send_message(ProcessorBulkMessage::NewBulk((
                        new_m_real.to_string(),
                        state.trades.clone(),
                        myself,
                        state.pricing_metrics.clone(),
                    )))?;
            }
        }
        Ok(())
    }

    // we are in calculating bulk state, new trade comes in.
    //   we only add the trade to the list of trades. nothing else.
    #[instrument(skip_all)]
    async fn _new_trade_calculating_bulk(
        &self,
        new_trade: String,
        state: &mut _ProcessorNewStateful,
        _myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // TODO: To improve in the future. remember that a trade was added here.
        info!(
            "Adding trade {}, not doing anything more.",
            new_trade.clone()
        );
        state.trades.insert(new_trade.clone()); // we add the trade to the list.

        Ok(())
    }

    // we receive the Behind message, we are in calculating single mode.
    #[instrument(skip_all)]
    fn _behind_calculating_single(
        &self,
        market_behind: String,
        trades_behind: TradesLocal,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
        // TRADES ARE BEHIND,
        //    < 10 -> continue in single mode
        //    > 10 -> continue in bulk mode.

        // the processor middle has accepted the market_behind
        // make new_m <- future.
        if trades_behind.is_empty() {
            // new processor is ahead, reset the
            //    new processor to the new default state.
            debug!("Lower processor accepted portfolio. Resetting portfolio: portf = empty");
            state.portfolio = PmPortfolio::new();

            // this shouldnt fail, but we have a failsafe
            let Some(future_market) = self.all_markets.get(&"future".to_string()) else {
                warn!("Could not find 'future' market. This is weird. Continuing w/o it.");
                return Ok(());
            };
            let future_market_name = future_market.market_name();

            debug!(
                "Switching markets: {:?} -> {}",
                state.new_market,
                future_market_name.clone()
            );
            state.new_market = Some(future_market_name.clone());
            self.all_markets.insert_both(
                self.processor_name.clone(),
                future_market_name.clone(),
                future_market,
            );

            // TODO: THIS SHOULDNT BE Idle, but go to Bulk processing immediately.
            info!(
                "State: {:?} -> {:?}",
                state.processor_state,
                ProcessorNewState::Idle
            ); // from pns -> Idle
            state.processor_state = ProcessorNewState::Idle;
        } else {
            // we are still behind the current processor. We destroy market_behind, and continue
            //   computing on new_m.
            // TODO: HERE COMES IN HEURISTICS, WHETHER TO SWITCH TO THE FUTURE MARKET.
            info!("Lower processor rejected portfolio.");

            // destroying the market_behind
            match &state.new_market {
                None => {
                    info!("New_m is None, doing: New_m <- {}", market_behind.clone());
                    state.new_market = Some(market_behind.clone());
                    self.all_markets
                        .insert_processor(self.processor_name.clone(), market_behind);
                }
                Some(ref real_market) => {
                    self.all_markets
                        .insert_processor(self.processor_name.clone(), real_market.to_string());
                }
            }

            // TODO: CHECK HERE!!! THIS PROBABLY DOESNT WORK
            // trade_l.extend(trades_behind.clone());  //*trade_l += &trades_behind;

            // check if we have a new_m
            let Some(ref new_m_real) = &state.new_market else {
                warn!("Does not have new_m. Ignoring and continuing.");
                return Ok(());
            };

            info!("New State: {:?} -> CalculatingBulk", state.processor_state);
            state.processor_state = ProcessorNewState::CalculatingBulk;

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
                    state.pricing_metrics.clone(),
                )))?;
        }
        Ok(())
    }

    #[instrument(skip_all, name = "_behind_idle_new")]
    fn _behind_idle(
        &self,
        market_behind: String,
        trades_behind: TradesLocal,
        state: &mut _ProcessorNewStateful,
        _myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        if trades_behind.is_empty() {
            // nothing to do.
            return Ok(());
        }

        // trades behind is not empty, add to trades, add
        //   the market, and
        state.trades.extend(trades_behind);

        if state.new_market.is_none() {
            state.new_market = Some(market_behind);
        }

        Ok(())
    }

    #[instrument(skip_all)]
    fn _bulkreceive_calculatingbulk_idle(
        &self,
        new_trade_l: TradesLocal,
        computed_portf: PmPortfolio,
        offending_trades: TradesLocal,
        bulk_market: String,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // result of computation has arrived.

        // check if the markets match

        debug!(
            "Assigning computed portfolio to current portfolio: {:?}",
            computed_portf.simple()
        );
        // state.portfolio += computed_portf;
        state.portfolio.assign(computed_portf);
        debug!("Current portfolio: {}", state.portfolio.simple());

        state.trades.extend(new_trade_l);
        state.trades_not_pricing.extend(offending_trades);
        debug!("Extending trades: Now {:?} trades.", state.trades.len());

        // TODO: THIS IS WRONG!!!
        //if state.new_market.is_none() {
        state.new_market = Some(bulk_market);
        // }

        debug!(
            "Sending to middle processor {:?}, portf size: {:?}, market: {:?}",
            self.processor_middle.get_name(),
            state.portfolio,
            state.new_market.clone()
        );
        self.processor_middle
            .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                state.trades.clone(),
                state.portfolio.clone(),
                state.new_market.clone().unwrap(),
                myself,
            )))?;
        info!("New State: {} -> Idle", state.processor_state);
        state.processor_state = ProcessorNewState::Idle;
        Ok(())
    }

    #[instrument(skip_all)]
    fn _bulkreceive_calculatingsingle(
        &self,
        new_trade_l: TradesLocal,
        computed_portf: PmPortfolio,
        offending_trades: TradesLocal,
        _bulk_market: String,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // result of computation has arrived, this is a bit weird.
        warn!("Received BulkReceive. This is weird. Will attempt to merge portfolios.");

        // check markets. if markets are the same, merge, otherwise ignore the BulkReceive.
        // This is the message we have.
        // ProcessorMiddleMessage::BulkReceive((
        //     new_trade_l,
        //     computed_portf,
        //     offending_trades,
        //     _bulk_market,

        match state.new_market {
            Some(ref new_m_str) if new_m_str == &_bulk_market => {
                info!("Markets match. Will merge computation results.");
                // merging.
                // _bulk and new_m are the same, merge the trades.

                //state.portfolio += computed_portf;
                state.portfolio.assign(computed_portf);
                state.trades.extend(new_trade_l);
                debug!(
                    "After merging: Portf: {:?}, trades: {:?}",
                    state.portfolio.simple(),
                    state.trades.len(),
                );

                info!(
                    "Sending to processor {:?} portfolio: {:?}",
                    self.processor_middle.get_name(),
                    state.portfolio,
                );
                self.processor_middle
                    .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                        state.trades.clone(),
                        state.portfolio.clone(),
                        new_m_str.to_string(),
                        myself,
                    )))?;
            }

            // no market match. print error and return.
            _ => {
                error!(
                    "BulkReceive has market {:?}, currently on market {:?}. Ignoring BulkReceive.",
                    _bulk_market, state.new_market
                );
                return Ok(());
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct _ProcessorNewStateful {
    // the state of the processor is:
    //   1st arg: hashset of trades,
    //   second is the list of trades that didnt price correctly -- check if this should also be TradesLocal???? TODO:
    //   third is the current portfolio result of correctly pricing trades.
    //   fourth is the computation state.
    //   fifth is the "new" market where we are pricing now. 'future' market exists anyway.
    //   6th: list of pricing metrics we are considering.
    trades: TradesLocal,
    trades_not_pricing: Vec<String>,
    portfolio: PmPortfolio,
    processor_state: ProcessorNewState,
    new_market: Option<String>,
    pricing_metrics: Vec<PricingMetric>,
}

impl std::fmt::Display for _ProcessorNewStateful {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "State: {:?}, Market: {:?}",
            self.processor_state, self.new_market
        )
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
    type State = _ProcessorNewStateful;
    type Arguments = (); // initial market

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments, // market parameters are passed here
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Starting ProcessorNew.");
        Ok(_ProcessorNewStateful {
            trades: TradesLocal::new(),
            trades_not_pricing: vec![],
            portfolio: PmPortfolio::new(),
            processor_state: ProcessorNewState::Idle,
            new_market: None,
            pricing_metrics: vec![],
        })
    }

    // #[instrument(
    //     name="processor_new_handle",
    //     skip(self, myself, message, state),
    //     fields(
    //         state = %state.processor_state,
    //         new_m = state.new_market,
    //     )
    // )]
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        //let (trade_l, trades_non_pricing, portf, pns, new_m, pricing_metrics) = state;

        debug!(%state, "State:");

        match message {
            ProcessorMiddleMessage::NewTrade(new_trade) => {
                info!("Message: NewTrade({:?})", new_trade);

                match state.processor_state {
                    // what is the processor doing right now.
                    ProcessorNewState::CalculatingSingle => {
                        self._new_trade_calculating_single(new_trade, state, myself)
                            .await?
                    }

                    ProcessorNewState::Idle => self._new_trade_idle(new_trade, state, myself)?,

                    ProcessorNewState::CalculatingBulk => {
                        self._new_trade_calculating_bulk(new_trade, state, myself)
                            .await?
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

                match state.processor_state {
                    ProcessorNewState::Idle => {
                        self._process_new_market_idle(new_market_name, state, myself)?
                    }

                    ProcessorNewState::CalculatingSingle => {
                        // TODO: MAYBE THIS IS FINE.
                        return Ok(());
                    }

                    ProcessorNewState::CalculatingBulk => {
                        // ignore if new market comes in, no
                        //   action taken.
                        return Ok(());
                    }
                }
            }

            // this only comes from ProcessorMiddle,
            //
            ProcessorMiddleMessage::Behind(market_behind, trades_behind) => {
                // we are behind trades behind the below processor
                info!(
                    "Message: Behind. Market: {:?}, trades_beind: {:?}",
                    market_behind,
                    trades_behind.len()
                );
                match state.processor_state {
                    // what is the processor doing right now
                    ProcessorNewState::CalculatingSingle => self._behind_calculating_single(
                        market_behind,
                        trades_behind,
                        state,
                        myself,
                    )?,

                    // we are calculating bulk, and we received info
                    //   from processor below.
                    ProcessorNewState::CalculatingBulk => {
                        // add the trades to portfolio, nothing else.
                        if !trades_behind.is_empty() {
                            info!(
                                "Adding non-computed trades {} to trade list. Not doing anything.",
                                trades_behind.len()
                            );
                            state.trades.extend(trades_behind);
                        }
                    }

                    ProcessorNewState::Idle => {
                        self._behind_idle(market_behind, trades_behind, state, myself)?
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
                    computed_portf.simple()
                );

                match state.processor_state {
                    ProcessorNewState::CalculatingBulk | ProcessorNewState::Idle => self
                        ._bulkreceive_calculatingbulk_idle(
                            new_trade_l,
                            computed_portf,
                            offending_trades,
                            _bulk_market,
                            state,
                            myself,
                        )?,

                    ProcessorNewState::CalculatingSingle => self._bulkreceive_calculatingsingle(
                        new_trade_l,
                        computed_portf,
                        offending_trades,
                        _bulk_market,
                        state,
                        myself,
                    )?,
                }
            }

            ProcessorMiddleMessage::Metric(new_pricing_metrics) => {
                info!("Changing metrics to {:?}", new_pricing_metrics);
                crate::utils::change_metrics(&mut state.portfolio, new_pricing_metrics.clone()); // fixes the portf to correspond to new_pricing_metrics
                state.pricing_metrics = new_pricing_metrics;
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
