// Trade processor interaction between current and new market.
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info, instrument, warn};

use crate::market::{CurrNewMarket, MarketType, TradeMarketDiscovery};
use crate::portfolio::{PortfolioType, PricingResults};
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::process_trade::{ObtainMarket, ProcessTradeValue};
use crate::trade::{BaseTrade, TradeDirection, TradeReduce, TradeRep};

/// Pricing engine for trades for remote pricing
pub trait RiskProcessors: TradeMarketDiscovery + PortfolioSender
where
    Self: std::fmt::Debug + Sync + ObtainMarket,
{
    /// computes the metric of the existing trades in
    /// all_trades, on either the new or the current market
    /// and updates the current_portfolio
    fn _price_existing_trades(
        &self,
        all_trades: &TradeRep<<Self as TradeReduce>::ReductionType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl std::future::Future<Output = PortfolioType> + Send;

    /// adds new trades on the trade_receiver to the
    /// all_trades, and prices the new trades that came on it.
    fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &mut Receiver<<Self as TradeReduce>::ReductionType>,
        all_trades: &mut TradeRep<<Self as TradeReduce>::ReductionType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        )>,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// computes the value of the trade and reports results,
    /// depending on direction (create, etc.)
    fn _process_trade<TR: Send + Sync + BaseTrade + ProcessTradeValue>(
        &self,
        trade: &TR,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl std::future::Future<Output = PortfolioType> + Send {
        async move {
            let market = self.get_market(curr_new_mkt);
            let trade_v = trade
                .value_by_metric2(metric, pricing_options, market)
                .await;

            let trade_v_dir = match trade.direction() {
                TradeDirection::Create => trade_v,
                TradeDirection::Delete => -trade_v,
                TradeDirection::Update => todo!(),
            };

            return match trade_v_dir {
                PricingResults::PV(pv) => pv,
                PricingResults::PV01(pv01) => pv01.aggregate(),
                PricingResults::PnL(pnl) => pnl,
            };
        }
    }

    /// current trade processor, reads on
    /// trade_receiver, and new_portfolio_receiver,
    /// and updates the curr_portfolio_sender.
    ///    trade_receiver: receiver of new trades.
    ///    curr_portfolio_sender: sender to the publisher to send a current portfolio.
    ///       only sends when the new portfolio is accepted, and current portfolio is
    ///       replaced w/ a new portfolio.
    ///    new_portfolio_receiver: receiver of the new potential portfolio from
    ///       trade_processor_new.
    ///    metric: metric which we are computing.
    ///    pricing_options: options for pricing trades.
    ///    accepted_sender: sender if new portfolio was accepted.
    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<<Self as TradeReduce>::ReductionType>,
        curr_portfolio_sender: &mut Sender<(
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        )>,
        new_portfolio_receiver: Receiver<(
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        )>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        // sends how many trades behind the current processor the proposed
        // portfolio is. If 0 - the portfolio was accepted. If > 0 it means it
        // is e.g. 3 trades behind the current processor. If < 0 it means the
        // new processor is ahead of the current processor.
        accepted_sender: Sender<i32>,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        <Self as TradeReduce>::ReductionType: std::fmt::Debug + ProcessTradeValue,
    {
        async move {
            // trades that the curr_processor is handling.
            let all_trades = Arc::new(Mutex::new(
                TradeRep::<<Self as TradeReduce>::ReductionType>::default(),
            ));
            let all_trades_2 = Arc::clone(&all_trades);
            let curr_portfolio = Arc::new(Mutex::new(PortfolioType::default()));
            let curr_portfolio_2 = Arc::clone(&curr_portfolio);
            let mut curr_portfolio_sender2 = curr_portfolio_sender.clone();

            tokio_scoped::scope(|scope| {
                scope.spawn(self._process_trade_curr(
                    trade_receiver,
                    metric,
                    pricing_options,
                    all_trades,
                    curr_portfolio,
                    &mut curr_portfolio_sender2,
                ));

                scope.spawn(self._possible_portf_switch(
                    new_portfolio_receiver,
                    all_trades_2,
                    curr_portfolio_2,
                    accepted_sender,
                    curr_portfolio_sender,
                ));
            })
        }
    }

    /// processes new trades coming in on the current market. Adds them to
    /// current portfolio results.
    //#[instrument]
    fn _process_trade_curr(
        &self,
        mut curr_trade_receiver: Receiver<<Self as TradeReduce>::ReductionType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        all_trades: Arc<Mutex<TradeRep<<Self as TradeReduce>::ReductionType>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        curr_portfolio_sender2: &mut Sender<(
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        )>,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        <Self as TradeReduce>::ReductionType: std::fmt::Debug + ProcessTradeValue,
    {
        async move {
            loop {
                match curr_trade_receiver.recv().await {
                    None => {
                        todo!()
                    }
                    Some(trade) => {
                        info!("CURR processor: Processing trade {:?}.", trade);
                        let valued_trade = self
                            ._process_trade(&trade, metric, pricing_options, CurrNewMarket::Current)
                            .await;
                        debug!("CURR processor: Trade value: {:?}", valued_trade);

                        *all_trades.lock().unwrap() += &trade;
                        *curr_portfolio.lock().unwrap() += valued_trade;

                        let (curr_p2, all_t2) = {
                            let c2 = curr_portfolio.lock().expect("cant lock");
                            let c3 = c2.clone();
                            let t2 = all_trades.lock().unwrap();
                            let t3 = TradeRep(t2.clone());
                            (c3, t3)
                        };
                        let _ = curr_portfolio_sender2.send((curr_p2, all_t2)).await;
                    }
                }
            }
        }
    }

    /// future that listens to new_portfolio_receiver,
    ///    and if it receives relevant portfolio,
    ///    switches curr <- new portfolio.
    fn _possible_portf_switch(
        &self,
        mut new_portfolio_receiver: Receiver<(
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        )>,
        all_trades_2: Arc<Mutex<TradeRep<<Self as TradeReduce>::ReductionType>>>,
        curr_portfolio_2: Arc<Mutex<PortfolioType>>,
        accepted_sender: Sender<i32>,
        curr_portfolio_sender: &mut Sender<(
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        )>,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        <Self as TradeReduce>::ReductionType: std::fmt::Debug,
    {
        async move {
            loop {
                let new_portfolio = new_portfolio_receiver.recv().await;
                debug!(
                    "CURR processor: Received new portfolio: {:?}.",
                    new_portfolio
                );
                match new_portfolio {
                    None => {
                        //error!("NEW PORTFOLIO RECEIVED NONE");
                        todo!()
                    }
                    Some((new_p, new_trades)) => {
                        let all_l = all_trades_2.lock().unwrap().len();
                        let new_l = new_trades.len();
                        debug!(
                            "Trades from NEW processor: {}. Trades on CURR processor: {}",
                            new_l, all_l,
                        );

                        let behind = (all_l as i32) - (new_l as i32); // how far behind is the
                        let _ = accepted_sender.send(behind).await;
                        if new_l >= all_l {
                            // new processor is further ahead
                            info!(
                                "Switching curr_p <- new_p: Nb trades = {}",
                                new_trades.len()
                            );
                            let _ = curr_portfolio_sender
                                .send((new_p.clone(), TradeRep(new_trades.clone())))
                                .await;

                            self._switch_all_markets().await; // curr <- new, new <- fut
                            *curr_portfolio_2.lock().unwrap() = new_p;
                            *all_trades_2.lock().unwrap() += &new_trades;
                        } else {
                            info!("New portfolio behind old one, not switching.");
                        }
                    }
                }
            }
        }
    }

    /// processes the trades on the new market.
    ///    new_market_receiver: receiver of the new market
    ///    new_trade_receiver: receiver of new trades.
    ///    new_portfolio_sender: new computed result portfolio is send over this channel
    ///    new_publisher: should the new portfolio be published. ??? TODO: CHECK THIS
    ///    accepted_recv: how far behind (positive number), or ahead (negative number we are with this new mkt)
    ///    fut_mkt_ready_recv: is the futures market ready.
    fn _trade_processor_new(
        &self,
        mut new_market_receiver: Receiver<MarketType>,
        mut new_trade_receiver: Receiver<<Self as TradeReduce>::ReductionType>, // receiving new additional trades
        new_portfolio_sender: Sender<(
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        )>, // results are sent here
        new_publisher: Sender<bool>,
        mut accepted_recv: Receiver<i32>,
        mut _fut_mkt_ready_recv: Receiver<bool>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        <Self as TradeReduce>::ReductionType: std::fmt::Debug,
    {
        async move {
            info!("Starting NEW trade processor.");
            let sleep_duration = core::time::Duration::new(1, 0);

            let mut previous_trades = TradeRep::<<Self as TradeReduce>::ReductionType>::default();

            loop {
                // try to catch the latest _new_mkt, ignore in betweeners.
                let mut actual_mkt: Option<MarketType> = None;
                while let Ok(_new_mkt) = new_market_receiver.try_recv() {
                    // COULD BE THAT new_market_receiver disconnected. This is not handled.
                    actual_mkt = Some(_new_mkt);
                }

                match actual_mkt {
                    None => {
                        warn!("No new market, or new_market_receiver dropped.");
                        tokio::task::yield_now().await;
                        let _ = tokio::time::sleep(sleep_duration).await;
                    }
                    Some(_new_mkt) => {
                        debug!("Received new market, commencing computations & switching new & fut markets");
                        self._switch_new_fut_markets().await; // switch new market <- fut market

                        // price the trades on the new market
                        debug!("Pricing trades on the NEW market.");
                        let mut new_portfolio = self
                            ._price_existing_trades(&previous_trades, metric, pricing_options, CurrNewMarket::New)
                            .await;


                        // then we process new trades being sent
                        let (new_portfolio2, all_batches2) = self
                            ._new_processor_trade_loop(
                                &mut new_trade_receiver,
                                metric,
                                pricing_options,
                            )
                            .await;

                        new_portfolio += new_portfolio2;
                        previous_trades += &all_batches2;

                        // attempt to send the portfolio to the trade_processor_curr
                        debug!("Sending new portfolio to curr processor for potential publishing.");
                        let _ = new_portfolio_sender
                            .send((new_portfolio.clone(), TradeRep(previous_trades.clone())))
                            .await;

                        loop {
                            match  accepted_recv.recv().await {
                                Some(accepted_real) => {
                                    if accepted_real <= 0 {
                                        info!("New portfolio accepted. Publishing and starting new market loop.");
                                        break;
                                    } else {
                                        // attempt with the newest batch
                                        debug!(
                                            "New portfolio _NOT_ accepted. Retrying w/ additional trades."
                                        );
                                        let (new_portfolio_inner, all_batches_inner) = self
                                            ._new_processor_trade_loop(
                                                &mut new_trade_receiver,
                                                metric,
                                                pricing_options,
                                            )
                                            .await;
                                        new_portfolio += new_portfolio_inner;
                                        previous_trades += &all_batches_inner;
                                        // try again to send the new portfolio results.
                                        let _ = new_portfolio_sender
                                            .send((new_portfolio.clone(), TradeRep(previous_trades.clone())))
                                            .await;
                                    }
                                },
                                None => { todo!() },
                            }
                        }
                        let _ = new_publisher.send(true).await;
                    }
                }
            }
        }
    }

    /// loop untill all the trade are exhausted on the receiver
    fn _new_processor_trade_loop(
        &self,
        new_trade_receiver: &mut Receiver<<Self as TradeReduce>::ReductionType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> impl std::future::Future<
        Output = (
            PortfolioType,
            TradeRep<<Self as TradeReduce>::ReductionType>,
        ),
    > + Send {
        async move {
            let mut portfolio = PortfolioType::default();
            let mut all_batches = TradeRep::<<Self as TradeReduce>::ReductionType>::default();

            let mut new_batch = self._get_trades_from_recv(new_trade_receiver).await;

            while new_batch.len() > 0 {
                let new_portfolio = self
                    ._price_existing_trades(&new_batch, metric, pricing_options, CurrNewMarket::New)
                    .await;
                portfolio += new_portfolio;
                all_batches += &new_batch;

                new_batch = self._get_trades_from_recv(new_trade_receiver).await;
            }

            (portfolio, all_batches)
        }
    }

    fn _get_trades_from_recv(
        &self,
        trade_receiver: &mut Receiver<<Self as TradeReduce>::ReductionType>,
    ) -> impl std::future::Future<Output = TradeRep<<Self as TradeReduce>::ReductionType>> + Send
    {
        async move {
            let mut new_trades = TradeRep::<<Self as TradeReduce>::ReductionType>::default();

            while let Ok(trade) = trade_receiver.try_recv() {
                new_trades += &trade;
            }
            new_trades
        }
    }
}
