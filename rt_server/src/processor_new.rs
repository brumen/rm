use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn}; // for tracking states.

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::PmPortfolio;
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_bulk::PriceMultiple;
use crate::processor_msg::{
    PNStateDistr, ProcessorBulkMessage, ProcessorMiddleMessage, ProcessorMiddleMessageStates,
    TradesLocal,
};
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
    pub(crate) state_distr: Arc<PNStateDistr>, // evmap for state distributions.
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum ProcessorNewState {
    CalculatingSingle, // when new trades arrived, but no new market.
    CalculatingBulk,   // when new market arrived and we're recomputing.
}

impl std::fmt::Display for ProcessorNewState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T, MT> ProcessorNew<T, MT>
where
    MT: MarketTypeT + Send + Sync + 'static + std::fmt::Debug,
    MT::MP: Clone,
    T: Send + Sync + Clone + 'static + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
{
    #[allow(dead_code)]
    pub(crate) fn new(
        processor_name: String,
        processor_middle: ActorRef<ProcessorMiddleMessage<String>>,
        processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
        all_trades: Arc<TradeRep<T>>,
        state_distr: Arc<PNStateDistr>,
    ) -> Self {
        Self {
            processor_name: processor_name.clone(),
            processor_middle,
            processor_bulk,
            all_markets,
            all_trades,
            state_distr,
        }
    }

    // message = new trade, state = calculating single
    // #[instrument(skip_all)]
    async fn _new_trade_calculating_single(
        &self,
        new_trade: String,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        info!("CalculatingSingle, Computing trade {}.", new_trade);
        state.trades.insert(new_trade.clone()); // we add the trade to the list.

        if state.pricing_metrics.is_empty() {
            // there's nothing to do, return.
            debug!("No pricing metrics. Ignoring.");
            return Ok(());
        }

        // the next 3 are conditions when we can actually compute something
        // condition if we can get the relevant trade
        // TODO: Check if .clone is needed in the closure???
        let Some(new_trade_info) = self.all_trades.read_sync(&new_trade, |_, v| v.clone()) else {
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
                .await
                .aggregate();
            debug!("New trade price: {:?}", new_trade_price_pm);
            state.portfolio.assign_metric(pm, new_trade_price_pm);
        }

        // we send the computed portfolio & trades to the current processor
        //   hoping that we are ahead.
        info!(
            "Sending to {:?}: trade# = {}, portf # = {:?}, new_m = {:?}.",
            self.processor_middle.get_name(),
            state.trades.len(),
            state.portfolio.simple(),
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

    // we are in calculating bulk state, new trade comes in.
    //   we only add the trade to the list of trades. nothing else.
    // #[instrument(skip_all)]
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
    // #[instrument(skip_all)]
    async fn _behind_calculating_single(
        &self,
        market_behind: String,
        trades_behind: TradesLocal,
        state: &mut _ProcessorNewStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Received Behind message.");

        if trades_behind.is_empty() {
            debug!("Lower processor accepted. Not behind. Ignoring.");
            return Ok(());
        }

        // lower processor is ahead. Add trades, and compute the difference.
        state.trades.extend(trades_behind.clone());

        // TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
        // TRADES ARE BEHIND,
        //    < 10 -> continue in single mode
        //    > 10 -> continue in bulk mode.
        // trades_behind != empty
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

        // check if we have a new_m
        let Some(ref new_m_real) = &state.new_market else {
            warn!("Does not have new_m. Ignoring and continuing.");
            return Ok(());
        };

        let Some(new_m_actual) = self.all_markets.get(new_m_real) else {
            warn!("Doesnt have market. Ignoring.");
            return Ok(());
        };

        let bulk_portfolio = self
            .price_multiple_seq(
                trades_behind,
                state.pricing_metrics.clone(),
                new_m_actual,
                self.all_trades.clone(),
            )
            .await;

        state.portfolio += bulk_portfolio;

        self.processor_middle
            .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                state.trades.clone(),
                state.portfolio.clone(),
                new_m_real.to_string(),
                myself,
            )))?;

        Ok(())
    }

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
            state.portfolio.simple(),
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
        state.processor_state = ProcessorNewState::CalculatingSingle;
        Ok(())
    }

    // #[instrument(skip_all)]
    fn _bulkreceive_calculatingsingle(
        &self,
        new_trade_l: TradesLocal,
        computed_portf: PmPortfolio,
        _offending_trades: TradesLocal,
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

#[async_trait]
impl<T, MT> PriceMultiple<T, MT> for ProcessorNew<T, MT>
where
    MT: MarketTypeT + 'static + std::fmt::Debug,
    MT::MP: Clone,
    T: PriceTrade<MT> + 'static + std::fmt::Debug + Sync + Send + Clone,
{
}

#[derive(Debug)]
pub(crate) struct _ProcessorNewStateful {
    // the state of the processor is:
    trades: TradesLocal,                 // hashset of trades,
    trades_not_pricing: Vec<String>, //   the list of trades that didnt price correctly -- check if this should also be TradesLocal???? TODO:
    portfolio: PmPortfolio,          // current portfolio result of correctly pricing trade.
    processor_state: ProcessorNewState, // computation state
    new_market: Option<String>, // "new" market where we are pricing now. 'future' market exists anyway.
    pricing_metrics: Vec<PricingMetric>, // list of pricing metrics we are considering.
    state_distr: Arc<PNStateDistr>,
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
            processor_state: ProcessorNewState::CalculatingSingle,
            new_market: None,
            pricing_metrics: vec![],
            state_distr: self.state_distr.clone(),
        })
    }

    #[instrument(
        skip(self, myself, message, state),
        fields(
            processor = "processor_new",
            state = %state.processor_state,
            new_m = state.new_market,
        )
    )]
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        //let (trade_l, trades_non_pricing, portf, pns, new_m, pricing_metrics) = state;
        match message {
            ProcessorMiddleMessage::NewTrade(new_trade) => {
                info!("Message: NewTrade({:?})", new_trade);
                // incremenet the state_distr variable.
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::NewTrade);

                match state.processor_state {
                    // what is the processor doing right now.
                    ProcessorNewState::CalculatingSingle => {
                        self._new_trade_calculating_single(new_trade, state, myself)
                            .await?
                    }

                    ProcessorNewState::CalculatingBulk => {
                        self._new_trade_calculating_bulk(new_trade, state, myself)
                            .await?
                    }
                }
            }

            // this is coming from mkt_handler, and market handler only produces
            //   "future" market.  Here we can potentially create a new market
            ProcessorMiddleMessage::NewMarket(new_market_name) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::NewMarket);
                debug!(
                    "Message NewMarket: {} <- this should be 'future'",
                    new_market_name
                );

                match state.processor_state {
                    ProcessorNewState::CalculatingSingle => {
                        // TODO: SWITCH MARKETS AND START RECOMPUTING.

                        debug!("Switching markets and starting the bulk computation.");
                        state.portfolio = PmPortfolio::new();

                        // this shouldnt fail, but we have a failsafe
                        let Some(future_market) = self.all_markets.get(&"future".to_string())
                        else {
                            warn!(
                                "Could not find 'future' market. This is weird. Continuing w/o it."
                            );
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

                        debug!("Going to CalculatingBulk.");
                        // let Some(new_market_actual) = self.all_markets.get(&future_market_name)
                        // else {
                        //     warn!("Couldnt find market. Ignoring");
                        //     return Ok(());
                        // };
                        state.processor_state = ProcessorNewState::CalculatingBulk;
                        self.processor_bulk
                            .send_message(ProcessorBulkMessage::NewBulk((
                                future_market_name.to_string(),
                                state.trades.clone(),
                                myself,
                                state.pricing_metrics.clone(),
                            )))?;
                    }

                    ProcessorNewState::CalculatingBulk => {
                        // ignore if new market comes in, no
                        //   action taken.
                        debug!("Got new market while calculating bulk. Ignoring.");
                        return Ok(());
                    }
                }
            }

            // this only comes from ProcessorMiddle,
            //
            // TODO:
            ProcessorMiddleMessage::Behind(market_behind, trades_behind) => {
                // we are behind trades behind the below processor
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::Behind);

                info!(
                    "Message: Behind. Market: {:?}, trades_beind: {:?}",
                    market_behind,
                    trades_behind.len()
                );
                //     match state.processor_state {
                //         // what is the processor doing right now
                //         ProcessorNewState::CalculatingSingle => {
                //             self._behind_calculating_single(market_behind, trades_behind, state, myself)
                //                 .await?
                //         }

                //         // we are calculating bulk, and we received info
                //         //   from processor below.
                //         ProcessorNewState::CalculatingBulk => {
                //             // add the trades to portfolio, nothing else.
                //             if !trades_behind.is_empty() {
                //                 info!(
                //                     "Adding non-computed trades {} to trade list. Not doing anything.",
                //                     trades_behind.len()
                //                 );
                //                 state.trades.extend(trades_behind);
                //             }
                //         } // ProcessorNewState::Idle => {
                //           //     self._behind_idle(market_behind, trades_behind, state, myself)?
                //           // }
                //     }
            }

            // _bulk market is not needed, as it is the same as either new_m.
            ProcessorMiddleMessage::BulkReceive((
                new_trade_l,
                computed_portf,
                offending_trades,
                _bulk_market,
            )) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::BulkReceive);

                info!(
                    "Message: BulkReceive: Portfolio: {:?}",
                    computed_portf.simple()
                );

                //match state.processor_state {
                //    ProcessorNewState::CalculatingBulk =>
                self._bulkreceive_calculatingbulk_idle(
                    new_trade_l,
                    computed_portf,
                    offending_trades,
                    _bulk_market,
                    state,
                    myself,
                )?

                //     ProcessorNewState::CalculatingSingle => self._bulkreceive_calculatingsingle(
                //         new_trade_l,
                //         computed_portf,
                //         offending_trades,
                //         _bulk_market,
                //         state,
                //         myself,
                //     )?,
                // }
            }

            ProcessorMiddleMessage::Metric(new_pricing_metrics) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::Metric);

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
