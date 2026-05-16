// middle processor, sits between 2 new processors
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::PmPortfolio; // , PortfolioType
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_bulk::{PriceMultiple, PricingStyle};
use crate::processor_msg::{
    PNStateDistr, ProcessorBulkMessage, ProcessorMiddleMessage, ProcessorMiddleMessageStates,
    TradesLocal,
};
use crate::trade::{BaseTrade, TradeRep};

// T is mnemonic for trade type, MT is mnemonic for market type
#[derive(Debug)]
pub(crate) struct ProcessorMiddle<T, MT: std::fmt::Debug> {
    pub(crate) processor_name: String,
    pub processor_below: ActorRef<ProcessorMiddleMessage<String>>,
    // pub processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>,
    pub(crate) all_trades: Arc<TradeRep<T>>,
    pub(crate) state_distr: Arc<PNStateDistr>,
}

// ProcessorMiddle is either in one of the three states:
//   CalculatingSingle - calculating trades one by one
//   CalculatingBulk - calculating trades in bulk
//   Idle - not doing anything.
impl<T, MT> ProcessorMiddle<T, MT>
where
    MT: Send + Sync + MarketTypeT + 'static + std::fmt::Debug,
    MT::MP: Clone,
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT> + std::fmt::Debug,
{
    #[allow(dead_code)]
    pub(crate) fn new(
        processor_name: String,
        processor_below: ActorRef<ProcessorMiddleMessage<String>>,
        // processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
        all_trades: Arc<TradeRep<T>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
        state_distr: Arc<PNStateDistr>,
    ) -> Self {
        Self {
            processor_name,
            processor_below,
            // processor_bulk,
            all_trades,
            all_markets,
            state_distr: state_distr.clone(),
        }
    }

    // in state calculating single, getting a new trade.
    // #[instrument(skip_all)]
    async fn _new_trade_calculating_single(
        &self,
        new_trade: String,
        state: &mut _ProcessorMiddleStateful,
        myself: ActorRef<ProcessorMiddleMessage<String>>,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Received new trade.");
        state.trades.insert(new_trade.clone()); // we add the trade to the list, even if the trade is faulty

        if state.pricing_metrics.is_empty() {
            debug!("No pricing metric. Ignoring.");
            return Ok(());
        }

        let mut trade_info = self
            .all_trades
            .read_async(&new_trade, |_, v| v.clone())
            .await
            .ok_or("Message: NewTrade: Trade not found")?;
        debug!("Message: NewTrade {}", trade_info.id());

        let Some(ref real_market) = state.curr_market else {
            warn!("Processor does not have market. Ignoring.");
            return Ok(());
        };

        let Some(market_info) = self.all_markets.get(real_market).await else {
            warn!("Could not get market {}. Weird - Continuing.", real_market);
            return Ok(());
        };

        state.trades_since_ntp += 1;

        for pm in &state.pricing_metrics {
            let new_trade_price_pm = trade_info.value_by_metric(*pm, market_info.clone()).await;

            // TODO: THIS SHOULD BE SOMETHING LIKE THE LINE BELOW:
            // state.pricing_results.assign_metric(pm, new_trade_price_pm);
            let Some(portf_pm) = state.pricing_results.get_mut(pm) else {
                warn!("Could not get pricing results for {:?}", pm);
                continue;
            };

            // what if pm is not in pricing_results.
            *portf_pm += new_trade_price_pm; // portfolio update
        }

        // we send the computed portfolio & trades to the processor below
        //   hoping that we are ahead.
        debug!(
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

    // receiving the behind message, in calculating single.
    // #[instrument(skip_all)]
    async fn _behind_calculating_single(
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
            // we are not behind, processor below accepted portfolio.
            debug!("Processor below accepted portfolio.");
            return Ok(());
        }

        // we are still behind the below processor.
        //   we add the trades to the trade list, and
        //   send it to the bulk processor.
        debug!(
            "Message: Behind: {} trades. Adding those trades to current population.",
            trades_behind.len(),
        );
        state.trades.extend(trades_behind.clone());

        // we dont have current market, use market_behind
        let curr_mkt = match &state.curr_market {
            None => None,
            Some(curr_mkt_str) => self.all_markets.get(curr_mkt_str).await,
        };

        if curr_mkt.is_none() {
            warn!(
                "Processor does not have market: Destroying the market {}",
                market_behind,
            );

            self.all_markets
                .insert_processor(self.processor_name.clone(), market_behind.clone())
                .await;
            state.curr_market = Some(market_behind.clone());
            // self.processor_bulk
            //     .send_message(ProcessorBulkMessage::NewBulk((
            //         market_behind.to_string(),
            //         state.trades.clone(),
            //         myself,
            //         state.pricing_metrics.clone(),
            //     )))?;
            let Some(market_actual) = self.all_markets.get(&market_behind).await else {
                warn!("Could not get market {:?}", market_behind);
                return Ok(());
            };
            let (bulk_trades_priced, bulk_portfolio) = self
                .price_multiple(
                    state.trades.clone(),
                    state.pricing_metrics.clone(),
                    market_actual,
                    self.all_trades.clone(),
                )
                .await;
            state.pricing_results = bulk_portfolio;

            return Ok(());
        };

        // add the additional trades to the portfolio.
        let curr_mkt_actual = curr_mkt.unwrap(); // this is fine since curr_mkt is handled above.
        let curr_mkt_actual_name = curr_mkt_actual.market_name().clone();
        let (additional_trades, additional_portfolio) = self
            .price_multiple(
                trades_behind,
                state.pricing_metrics.clone(),
                curr_mkt_actual, // this works since .is_none is handled above.
                self.all_trades.clone(),
            )
            .await;
        state.pricing_results += additional_portfolio;
        self.processor_below
            .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                state.trades.clone(),
                state.pricing_results.clone(),
                curr_mkt_actual_name,
                myself,
            )))?;

        Ok(())
    }

    // receive a new trade portfolio when calculating single trades.
    //  actions performed:
    //     1. if the new_trade_portfolio is ahead in terms of trades, replace it, and replace market.
    //     2. if it's behind, inform the upstream processor of the trades, and ignore it.
    // #[instrument(skip_all)]
    async fn _ntp_calculating_single(
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
        let (ntp_trades, ntp_portfolio, new_market, upstream_processor) = ntp; // new trade portfolio

        // TODO: WRONG - IMPLEMENT > JUST FOR REFERENCES!!!
        //let new_behind_curr = trade_l - &potential_trades;
        let new_behind_curr = state
            .trades
            .iter()
            .filter(|&x| !ntp_trades.contains(x))
            .cloned()
            .collect::<TradesLocal>();
        debug!(
            "NewPortfolio: Current trades: {}, NTP trades: {}, Difference: {}, New portf: {}",
            state.trades.len(),
            ntp_trades.len(),
            new_behind_curr.len(),
            ntp_portfolio.simple(),
        );

        // if not empty send message to the Behind processor upstream
        // how many trades are we behind.
        //if !new_behind_curr.is_empty() {
        debug!(
            "Received portfolio accepted. Informing upstream processor {:?}",
            upstream_processor.get_name(),
        );
        let _ = upstream_processor.send_message(
            ProcessorMiddleMessage::Behind(new_market.to_string(), new_behind_curr.clone()), // TODO: CHECK IF THIS IS REALLY NEW_MARKET??
        );
        // return Ok(());
        // }

        // accepting the proposed portfolio. swithing markets to the market passed.
        // check if we have a current market
        debug!(
            "Received portfolio accepted. Passing it to the processor below and switching markets.",
        );

        debug!(
            "Switching markets: {:?} -> {}",
            state.curr_market, new_market,
        );
        state.curr_market = Some(new_market.clone());
        state.trades_since_ntp = 0; // reset the trades_since_ntp
        self.all_markets
            .insert_processor(self.processor_name.clone(), new_market.clone())
            .await;

        // new_behind_curr is empty, replace the portfolio w/ the received one.
        // ntp is ahead of the current portfolio.
        // replace the portfolio and trades
        state.pricing_results = ntp_portfolio;
        state.trades.extend(ntp_trades);

        // compute the potential added trades.
        if !new_behind_curr.is_empty() {
            // compute the new_behind_curr and then send it lower.
            let Some(new_market_actual) = self.all_markets.get(&new_market).await else {
                return Ok(());
            };

            let (priced_trades, added_portfolio) = self
                .price_multiple(
                    new_behind_curr,
                    state.pricing_metrics.clone(),
                    new_market_actual,
                    self.all_trades.clone(),
                )
                .await;
            // TODO: MAKE SURE THIS ADDS TO THE PORTFOLIO, NOT REPLACES IT.
            state.pricing_results += added_portfolio;
        }

        debug!(
            "Sending the portfolio to {:?}",
            self.processor_below.get_name()
        );
        self.processor_below
            .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                state.trades.clone(),
                state.pricing_results.clone(),
                new_market.clone(),
                myself,
            )))?;

        Ok(())
    }
}

#[async_trait]
impl<T, MT> PriceMultiple<T, MT> for ProcessorMiddle<T, MT>
where
    MT: MarketTypeT + 'static + std::fmt::Debug,
    MT::MP: Clone,
    T: PriceTrade<MT> + 'static + std::fmt::Debug + Sync + Send + Clone,
{
    fn pricing_style(&self) -> PricingStyle {
        PricingStyle::Sequential
    }
}

// state of the middle processor.
#[derive(Debug, Clone)]
pub enum ProcessorMiddleState {
    CalculatingSingle, // when bulk has finished and we're only calculating single trades.
                       // CalculatingBulk,   // when we're still calculating bulk
                       // CalculatingBulkMarketSwitch, // we are calculating bulk, but market
                       // switched in the meantime, so the old calculating is not valid anymore
}

// processor middle can receive the following messages (not all are relevant)
//   (from processor_msg.rs)
//    NewTrade
//    Behind(market, trades_behind)
//    BulkReceive(trades, portfolio_result, trades_not_pricing, market)
//    NewTradePortfolio
//    Metric

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
    state_distr: Arc<PNStateDistr>,      // map of state -> number of times visiting that state.
    trades_since_ntp: u64, // how many trades have we processed in NewTrade since the last NewTradePortfolio
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
            processor_state: ProcessorMiddleState::CalculatingSingle,
            curr_market: None,       // original market, none
            pricing_metrics: vec![], // no metrics at first
            state_distr: self.state_distr.clone(),
            trades_since_ntp: 0,
        })
    }

    #[instrument(
         skip(self, myself, message, state),
         fields(
             processor = %self.processor_name,
             state = %state.processor_state,
             mkt=state.curr_market,
             since_ntp=state.trades_since_ntp,
         )
    )]
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // let (trade_l, trades_non_pricing, portf, pns, market, pricing_metrics) = state; // market is the market where we're operating
        let pns_old = state.processor_state.clone();

        match (message, pns_old) {
            (
                ProcessorMiddleMessage::NewTrade(new_trade),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::NewTrade);
                // calculate this trade and send it to the lower processor
                self._new_trade_calculating_single(new_trade, state, myself)
                    .await?
            }

            (ProcessorMiddleMessage::NewMarket(_new_market), _) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::NewMarket);

                error!("Received NewMarket. THIS SHOULDNT HAPPEN! Ignoring and continuing.");
            }

            // this only comes from processor below
            (
                ProcessorMiddleMessage::Behind(market_behind, trades_behind),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::Behind);

                self._behind_calculating_single(market_behind, trades_behind, state, myself)
                    .await?
            }

            (
                ProcessorMiddleMessage::BulkReceive((
                    _new_trade_l,
                    _computed_portf,
                    _offending_trades,
                    _bulk_market,
                )),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::BulkReceive);

                // result of computation has arrived.
                //  add it to the computation
                // TODO: WHAT TO DO W/ OFFENDING TRADES???

                debug!("Message: BulkReceive: This shouldnt happen.",);
                return Ok(());
                // state.pricing_results = computed_portf;
                // state.trades.extend(new_trade_l);

                // let Some(ref real_market) = state.curr_market else {
                //     warn!("Processor does not have market. Ignoring.");
                //     return Ok(());
                // };

                // self.processor_below
                //     .send_message(ProcessorMiddleMessage::NewTradePortfolio((
                //         state.trades.clone(),
                //         state.pricing_results.clone(),
                //         real_market.to_string(),
                //         myself,
                //     )))?;
            }

            (
                ProcessorMiddleMessage::NewTradePortfolio(ntp),
                ProcessorMiddleState::CalculatingSingle,
            ) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::NewTradePortfolio);

                self._ntp_calculating_single(ntp, state, myself).await?
            }

            (ProcessorMiddleMessage::BulkBusy, _) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::BulkBusy);

                warn!("Message: BulkBusy. Ignore for now.");
            }

            (ProcessorMiddleMessage::ProcessingStat(_), _) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::ProcessingStat);
            } // processing stat is not for this processor

            // we get new metrics from the metric dispatch
            (ProcessorMiddleMessage::Metric(new_pricing_metrics), _) => {
                state
                    .state_distr
                    .incr_one(ProcessorMiddleMessageStates::Metric);

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
