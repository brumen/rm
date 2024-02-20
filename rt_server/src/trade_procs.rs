// Trade processor interaction between current and new market.
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, debug, instrument};
use tokio;

use crate::market::{CurrNewMarket, MarketType, TradeMarketDiscovery};
use crate::portfolio::{PortfolioType, PricingResults};
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::{MarketPricingOptions, PricingMetric, Decoder, PriceTradeAsync};
use crate::trade::{TradeRep, BaseTrade};

pub trait ProcessTradeSync<TR> {
    fn _process_trade(
        &self,
        trade: &TR,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PricingResults;
}

pub trait ProcessTradeAsync {
    fn _process_trade<TR: PriceTradeAsync + Send + Sync + Decoder + BaseTrade>  ( //: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTrade + Send + Sync + Decoder + Sync> (
        &self,
        trade: &TR,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl std::future::Future<Output=PortfolioType> + Send;
}

/// Pricing engine for trades for remote pricing
pub trait RiskProcessors: TradeMarketDiscovery + PortfolioSender + ProcessTradeAsync
where
    <Self as PortfolioSender>::TR: Clone + BaseTrade + Decoder + Sync + std::fmt::Debug,
    Self: std::fmt::Debug + Sync,
{
    /// computes the metric of the existing trades in
    /// all_trades, on either the new or the current market
    /// and updates the current_portfolio
    fn _price_existing_trades(
        &self,
        all_trades: &TradeRep<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl std::future::Future<Output=PortfolioType> + Send;

    /// adds new trades on the trade_receiver to the
    /// all_trades, and prices the new trades that came on it.
    async fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &mut Receiver<Self::TR>,
        all_trades: &mut TradeRep<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<Self::TR>)>,
    );

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
    //#[instrument]
    fn _trade_processor_curr(
        &self,
        mut trade_receiver: Receiver<Self::TR>,
        curr_portfolio_sender: Sender<(PortfolioType, TradeRep<Self::TR>)>,
        mut new_portfolio_receiver: Receiver<(PortfolioType, TradeRep<Self::TR>)>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
	// sends how many trades behind the current processor the proposed
	// portfolio is. If 0 - the portfolio was accepted. If > 0 it means it
	// is e.g. 3 trades behind the current processor. If < 0 it means the
	// new processor is ahead of the current processor.
        accepted_sender: Sender<i32>,
    ) -> impl std::future::Future<Output=()> + Send {
        async move {
	// TODO: REMOVE THIS NEXT LINE
        // let mut new_potential_portfolio: Option<(PortfolioType, TradeRep<Self::TR>)>;
        let mut all_trades = TradeRep::<Self::TR>::new();
        // let curr_portfolio = Arc::new(Mutex::new(PortfolioType::new()));

        info!("Pricing existing trades on CURRENT market.");
        let mut curr_portfolio = self._price_existing_trades(
            &all_trades,
            metric,
            pricing_options,
            CurrNewMarket::Current,
        ).await;

        info!(
            "Curr nb trades: {}.",
            all_trades.len(),
        );

	    loop {
            debug!("CURRENT market, inner loop. Waiting for new traes or new portfolio");
	        tokio::select! {
		        trade_out = trade_receiver.recv() => {
		            match trade_out {
			            None => { todo!() },
			            Some(trade) => {
                            debug!("CURR processor: Processing trade {:?}.", trade);
                            all_trades += &trade;
			                curr_portfolio += self
				                ._process_trade(&trade, metric, pricing_options, CurrNewMarket::Current)
				                .await;

                            //			    let new_p_attempt =  new_trades_sender.send(
                            //				(curr_portfolio.clone(), TradeRep(all_trades.clone()))
                            //			    ).await;
			            },
		            }
		        },
		        new_portfolio = new_portfolio_receiver.recv() => {
                    info!("CURR processor: Received new portfolio: {:?}.", new_portfolio);
                    match new_portfolio {
			            None => {
			                todo!()
			            },
			            Some((new_p, new_trades)) => {

			                let all_l = all_trades.len();
			                let new_l = new_trades.len();
			                info!(
				                "New trades sent: {}. Curr trades: {}",
				                new_l, all_l
			                );

                            let behind = (all_l - new_l) as i32;  // how far behind is the
			                let _ = accepted_sender.send(behind).await;
			                if new_l >= all_l {
				                // new processor is further ahead
				                info!(
				                    "Switching curr_p <- new_p: {}",
				                    new_trades.len()
				                );
				                curr_portfolio = new_p;
				                all_trades += &new_trades;
				                self._switch_all_markets().await;  // curr <- new, new <- fut
				                let _ = curr_portfolio_sender
				                    .send((curr_portfolio.clone(), TradeRep(all_trades.clone()))).await;
			                } else {
				                info!("New portfolio behind old one, not switching.");
			                }
			            },
		            }
		        },
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
        mut new_trade_receiver: Receiver<Self::TR>, // receiving new additional trades
        new_portfolio_sender: Sender<(PortfolioType, TradeRep<Self::TR>)>, // results are sent here
        new_publisher: Sender<bool>,
        mut accepted_recv: Receiver<i32>,
	    mut _fut_mkt_ready_recv: Receiver<bool>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> impl std::future::Future<Output=()> + Send {
        async move {
        let nb_attempts = 100; // try 5 times before aborting and starting on a new market

	    while let Some(_new_mkt) = new_market_receiver.recv().await {
            info!("Received new market, commencing computations & switching new & fut markets");
	        self._switch_new_fut_markets().await;  // switch new market <- fut market

	        // price the trades on the current market
            info!("Pricing trades on the current market.");
            let (mut new_portfolio, mut all_batches) =
                self._new_processor_trade_loop(
		            &mut new_trade_receiver,
		            metric,
		            pricing_options
		        ).await;

	        // attempt to send the portfolio to the trade_processor_curr
            info!("New portfolio = {:?}", new_portfolio);
            info!("Sending new portfolio for potential publishing.");
            let _ = new_portfolio_sender
                .send((new_portfolio.clone(), TradeRep(all_batches.clone())))
                .await;

	        //let ma: Vec<_> = vec![];  // moving average, how far behind are we in this market
            let mut curr_attempt = 0;
            info!("FUTURE MKT READY: {:?}", _fut_mkt_ready_recv.try_recv());
            while (curr_attempt < nb_attempts) & !self._future_mkt_ready() {
                if let Ok(accepted_real) = accepted_recv.try_recv() {
                    if accepted_real <= 0 {
                        info!("New portfolio accepted.");
                        let _ = new_publisher.send(true).await;
                        break;
                    } else {
                        // attempt with the newest batch
                        info!("New portfolio NOT accepted. Retrying w/ additional trades.");
                        let (new_portfolio_inner, all_batches_inner) = self
                            ._new_processor_trade_loop(
                                &mut new_trade_receiver,
                                metric,
                                pricing_options,
                            ).await;
                        new_portfolio += new_portfolio_inner;
                        all_batches += &all_batches_inner;
                        let _ = new_portfolio_sender
                            .send((new_portfolio.clone(), TradeRep(all_batches.clone())))
                            .await;
                    }
                }
                curr_attempt += 1;
            }
	    }
        }
    }

    /// loop untill all the trade are exhausted on the receiver
    fn _new_processor_trade_loop(
        &self,
        new_trade_receiver: &mut Receiver<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> impl std::future::Future<Output=(PortfolioType, TradeRep<Self::TR>)> + Send {
        async move {
            let mut portfolio = PortfolioType::new();
            let mut all_batches = TradeRep::<Self::TR>::new();

            let mut new_batch = self._get_trades_from_recv(new_trade_receiver).await;

            while new_batch.len() > 0 {
                let new_portfolio = self._price_existing_trades(
                    &new_batch,
                    metric,
                    pricing_options,
                    CurrNewMarket::New,
                ).await;
                portfolio += new_portfolio;
                all_batches += &new_batch;

                new_batch = self._get_trades_from_recv(new_trade_receiver).await;
            }

            (portfolio, all_batches)
        }
    }

    fn _get_trades_from_recv(
	    &self,
	    trade_receiver: &mut Receiver<Self::TR>,
    ) -> impl std::future::Future<Output=TradeRep<Self::TR>> + Send {
        async move {
            let mut new_trades = TradeRep::<Self::TR>::new();

            while let Ok(trade) = trade_receiver.try_recv() {
                new_trades += &trade;
            }
            new_trades
        }
    }
}
