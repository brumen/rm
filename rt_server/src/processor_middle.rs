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
    MT: Send + Sync + MarketTypeT + 'static + std::fmt::Debug,
    // MT: MarketTypeT + std::fmt::Debug,
    MT::MP: Clone,
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
    //T: std::fmt::Debug,
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

    #[instrument(skip_all)]
    async fn _new_trade_calculating_single(
        &self,
        new_trade: String,
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
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
            let new_trade_price_pm = real_trade.value_by_metric(*pm, market_info.clone()).await;
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
        Ok(())
    }

    #[instrument(skip_all)]
    fn _new_trade_idle(
        &self,
        new_trade: String,
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
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
        Ok(())
    }

    #[instrument(skip_all)]
    async fn _new_trade_calculating_bulk(
        &self,
        new_trade: String,
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
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
        let _ = self
            .processor_below
            .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                state.trades.clone(),
                state.pricing_results.clone(),
                real_market.to_string(),
                myself,
            )));
        Ok(())
    }

    // receiving the behind message, in calculating single.
    #[instrument(skip_all)]
    fn _behind_calculating_single(
        &self,
        market_behind: String,
        trades_behind: TradesLocal,
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // we are behind trades behind the current processor

        // TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
        // TRADES ARE BEHIND,
        //    < 10 -> continue in single mode
        //    > 10 -> continue in bulk mode.
        if trades_behind.is_empty() {
            // this processor below is ahead, reset the
            //    processor to the new Idle state.
            info!(
                "Processor below accepted portfolio. State: {} -> Idle",
                state.processor_state
            );
            state.processor_state = ProcessorMiddleState::Idle;
            return Ok(());
        }

        // we are still behind the below processor.
        //   we add the trades to the trade list, and
        //   send it to the bulk processor.
        info!(
            "Message: Behind: {} trades. Adding those trades to current population.",
            trades_behind.len(),
        );
        state.trades.extend(trades_behind.clone());

        // dont continue once we get the Behind message.
        Ok(())

        // debug!("State: {} -> CalculatingBulk", state.processor_state);
        // state.processor_state = ProcessorMiddleState::CalculatingBulk;

        // // we dont have current market, use market_behind
        // let Some(ref real_market) = state.curr_market else {
        //     warn!(
        //         "Processor does not have market: Destroying the market {}",
        //         market_behind,
        //     );

        //     self.all_markets
        //         .insert_processor(self.processor_name.clone(), market_behind.clone());
        //     state.curr_market = Some(market_behind.clone());
        //     self.processor_bulk
        //         .send_message(ProcessorBulkMessage::NewBulk((
        //             market_behind.to_string(),
        //             state.trades.clone(),
        //             myself,
        //             state.pricing_metrics.clone(),
        //         )))?;

        //     return Ok(());
        // };

        // // we have real market. TODO: CHECK IF THIS IS NECESSARY
        // self.all_markets
        //     .insert_processor(self.processor_name.clone(), real_market.clone());
        // info!(
        //     "Behind: Sending bulk compute to {:?} on market {}.",
        //     self.processor_bulk.get_name(),
        //     real_market
        // );
        // self.processor_bulk
        //     .send_message(ProcessorBulkMessage::NewBulk((
        //         real_market.to_string(),
        //         state.trades.clone(),
        //         myself,
        //         state.pricing_metrics.clone(),
        //     )))?;

        // Ok(())
    }

    #[instrument(skip_all)]
    fn _behind_calculating_bulk(
        &self,
        _market_behind: String,
        trades_behind: TradesLocal,
        state: &mut _ProcessorMiddleStateful,
        _myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // if empty, dont do anything, if not, just add the potential trades behind.
        if !trades_behind.is_empty() {
            // the processor is behind the below processor, calculate the remaining trades.
            // we are still behind the current processor.
            // TODO: MAYBE WE CAN DIFFERENTIATE ON HOW MANY TRADES BEHIND???
            info!("Lower processor did not accept the portfolio. Adding trades, that's all.",);

            state.trades.extend(trades_behind); // otherwise dont do anything.
        }
        Ok(())
    }

    #[instrument(skip_all, name = "_behind_idle_middle")]
    fn _behind_idle(
        &self,
        market_behind: String,
        trades_behind: TradesLocal,
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // we're in idle state, and have received about previous market.
        state.trades.extend(trades_behind.clone());
        state.processor_state = ProcessorMiddleState::CalculatingBulk;

        // Dont continue once we get Behind message (causes infinite loops).
        Ok(())

        // // we dont have current market, replace it w/ market_behind.
        // let Some(ref real_market) = state.curr_market else {
        //     // use market behind and restart computations.
        //     state.curr_market = Some(market_behind.clone());
        //     self.all_markets
        //         .insert_processor(self.processor_name.clone(), market_behind.clone());
        //     self.processor_bulk
        //         .send_message(ProcessorBulkMessage::NewBulk((
        //             market_behind.to_string(),
        //             state.trades.clone(),
        //             myself,
        //             state.pricing_metrics.clone(),
        //         )))?;

        //     return Ok(());
        // };

        // // we have real market, just relaunch the computations.
        // self.processor_bulk
        //     .send_message(ProcessorBulkMessage::NewBulk((
        //         real_market.to_string(),
        //         state.trades.clone(),
        //         myself,
        //         state.pricing_metrics.clone(),
        //     )))?;

        // Ok(())
    }

    #[instrument(skip_all)]
    fn _bulk_receive_calculating_bulk(
        &self,
        new_trade_l: TradesLocal,
        computed_portf: PmPortfolio,
        offending_trades: TradesLocal,
        _bulk_market: String,
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // result of computation has arrived.
        // TODO: FINISH THIS HERE - what to do w/ offending trades???
        //    Nothing for now.
        info!("Message: BulkReceive|CalculatingBulk: Normal case. Going to CalculatingSingle.",);

        for (pm, computed_portf_pm) in computed_portf.iter() {
            let portf_pm = state.pricing_results.get_mut(pm).unwrap();
            *portf_pm += computed_portf_pm;
        }

        state.trades.extend(new_trade_l);
        state.trades_not_pricing.extend(offending_trades);
        debug!("State: CalculatingBulk -> Idle");
        state.processor_state = ProcessorMiddleState::Idle;

        // Dont continue, causes infinite loops.
        Ok(())

        // let Some(ref real_market) = state.curr_market else {
        //     warn!("Does not have market. Continuing.");
        //     // TODO: THIS CAN BE BETTER HANDLED.
        //     return Ok(());
        // };

        // debug!(
        //     "Sending new trade portfolio to {:?}",
        //     self.processor_below.get_name()
        // );
        // self.processor_below
        //     .send_message(ProcessorMiddleMessage::NewTradePortfolio((
        //         state.trades.clone(),
        //         state.pricing_results.clone(),
        //         real_market.to_string(),
        //         myself,
        //     )))?;
        // Ok(())
    }

    #[instrument(
        name="_ntp_idle_middle",
        skip(self, myself, state, ntp),
        fields(
            processor=self.processor_name,
            market=ntp.2,
        )
    )]
    fn _ntp_idle(
        &self,
        ntp: (
            TradesLocal,
            PmPortfolio,
            String,
            ActorRef<ProcessorMiddleMessage<String>>,
        ),
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        // receiving a mesage from the processor above
        // ntp = (trades, potential_portfolio, market, upstream_processor)
        // just pass it to the processor below, dont do anything else

        let (potential_trades, potential_portfolio, new_market, ref upstream_processor) = ntp;

        info!(
            "Switching market: from {:?} -> {}",
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

        // cant do the ntp.clone, insert myself
        self.processor_below
            .send_message(ProcessorMiddleMessage::NewTradePortfolio(
                //ntp.clone()
                (
                    potential_trades.clone(),
                    potential_portfolio.clone(),
                    new_market.clone(),
                    myself,
                ),
            ))?;

        // acknowledge to the sending processor that it was accepted.
        state.trades = potential_trades;
        state.pricing_results = potential_portfolio;

        // TODO: CHECK IF THIS SHOULD BE HANDLED???
        // market is Some, so unwrap is justified.
        let _ = upstream_processor.send_message(
            ProcessorMiddleMessage::Behind(new_market, TradesLocal::new()), // portfolio is accepted, notify the upstream that we're accepting
        );
        Ok(())
    }

    #[instrument(skip_all)]
    fn _ntp_calculating_single(
        &self,
        ntp: (
            TradesLocal,
            PmPortfolio,
            String,
            ActorRef<ProcessorMiddleMessage<String>>,
        ),
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
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
            state.trades.len(),
            potential_trades.len(),
            new_behind_curr.len(),
            potential_portfolio.len(),
        );

        if new_behind_curr.is_empty() {
            // ntp is ahead of the current portfolio.
            // replace the portfolio and trades

            info!("Received portfolio is ahead. accepting it and passing to processor below",);
            state.pricing_results = potential_portfolio;
            state.trades.extend(potential_trades);
            // TODO: HOW ABOUT pns ???
            let Some(ref real_market) = state.curr_market else {
                warn!("Does not have market. Ignoring.");
                return Ok(());
            };

            self.processor_below
                .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                    state.trades.clone(),
                    state.pricing_results.clone(),
                    real_market.to_string(),
                    myself,
                )))?;

            if state.curr_market != Some(new_market.clone()) {
                info!(
                    "Switching markets: {:?} -> {}",
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
        Ok(())
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
    // state of the middle processor
    trades: TradesLocal,                   //   1st arg: list of trades,
    trades_not_pricing: TradesLocal,       //   2nd arg: list of trades that didnt price correctly
    pricing_results: PmPortfolio, //   3rd: third is the current portfolio result for each pricing metric of correctly pricing trades.
    processor_state: ProcessorMiddleState, //   4th: is the computation state.
    curr_market: Option<String>,  //   5th: is the market that the processor is operating on.
    //      for remote pricing markets only market_name is fine,
    //      for local markets, the name and the market structure.
    pricing_metrics: Vec<PricingMetric>, //   6th: list of metrics that the system is operating on.
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

    // #[instrument(
    //     name="middle_handle",
    //     skip(self, myself, message, state),
    //     fields(
    //         name = %self.processor_name,
    //         state = %state.processor_state,
    //         mkt=state.curr_market,
    //     )
    // )]
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
                self._new_trade_calculating_single(new_trade, state, myself)
                    .await?
            }

            (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::Idle) => {
                self._new_trade_idle(new_trade, state, myself)?
            }

            (
                ProcessorMiddleMessage::NewTrade(new_trade),
                ProcessorMiddleState::CalculatingBulk,
            ) => {
                self._new_trade_calculating_bulk(new_trade, state, myself)
                    .await?
            }

            (ProcessorMiddleMessage::NewMarket(_new_market), _) => {
                error!("Received NewMarket. THIS SHOULDNT HAPPEN! Ignoring and continuing.");
            }

            // this only comes from processor below
            (
                ProcessorMiddleMessage::Behind(market_behind, trades_behind),
                ProcessorMiddleState::CalculatingSingle,
            ) => self._behind_calculating_single(market_behind, trades_behind, state, myself)?,

            (
                ProcessorMiddleMessage::Behind(market_behind, trades_behind),
                ProcessorMiddleState::CalculatingBulk,
            ) => self._behind_calculating_bulk(market_behind, trades_behind, state, myself)?,

            (
                ProcessorMiddleMessage::Behind(market_behind, trades_behind),
                ProcessorMiddleState::Idle,
            ) => self._behind_idle(market_behind, trades_behind, state, myself)?,

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
            ) => self._bulk_receive_calculating_bulk(
                new_trade_l,
                computed_portf,
                offending_trades,
                _bulk_market,
                state,
                myself,
            )?,

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

            (ProcessorMiddleMessage::NewTradePortfolio(ntp), ProcessorMiddleState::Idle) => {
                self._ntp_idle(ntp, state, myself)?
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
            }

            (
                ProcessorMiddleMessage::NewTradePortfolio(ntp),
                ProcessorMiddleState::CalculatingSingle,
            ) => self._ntp_calculating_single(ntp, state, myself)?,

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
