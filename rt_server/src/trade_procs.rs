// Trade processor interaction between current and new market.
use std::future::Future;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use tracing::info;

use crate::market::{CurrNewMarket, MarketType, TradeMarketDiscovery};
use crate::portfolio::{PortfolioType, PricingResults};
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::trade::TradeRep;

pub trait ProcessTradeSync<TR> {
    fn _process_trade(
        &self,
        trade: &TR,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PricingResults;
}

pub trait ProcessTradeAsync<TR> {
    fn _process_trade(
        &self,
        trade: &TR,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> impl Future<Output = PortfolioType> + Send;
}

/// Pricing engine for trades for remote pricing
pub trait RiskProcessors: TradeMarketDiscovery + PortfolioSender
where
    <Self as PortfolioSender>::TR: Clone,
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
    ) -> PortfolioType;

    /// adds new trades on the trade_receiver to the
    /// all_trades, and prices the new trades that came on it.
    fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &Receiver<Self::TR>,
        all_trades: &mut TradeRep<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<Self::TR>)>,
    );

    /// current trade processor, reads on
    /// trade_receiver, and new_portfolio_receiver,
    /// and updates the curr_portfolio_sender.
    async fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<Self::TR>,
        curr_portfolio_sender: Sender<(PortfolioType, TradeRep<Self::TR>)>,
        new_portfolio_receiver: Receiver<(PortfolioType, TradeRep<Self::TR>)>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        accepted_sender: Sender<bool>,
    ) {
        let mut new_potential_portfolio: Option<(PortfolioType, TradeRep<Self::TR>)>;
        let mut all_trades = TradeRep::<Self::TR>::new();
        let curr_portfolio = Arc::new(Mutex::new(PortfolioType::new()));

        info!("Pricing existing trades on CURRENT market");
        let mut curr_portfolio = self._price_existing_trades(
            &all_trades,
            metric,
            pricing_options,
            CurrNewMarket::Current,
        );

        info!(
            "_trade_processor_curr: Curr nb trades: {}.",
            all_trades.len(),
        );

	let switch_portfolios = async move || {
	    // receive new portfolio, replace current w/ new.
	    while let Ok(new_portfolio) = new_portfolio_receiver.recv().await {
                new_potential_portfolio = Some(new_portfolio);
		if let Some((new_p, new_trades)) = new_potential_portfolio {
		    self._switch_markets();
		    
		    let all_l = all_trades.len();
		    let new_l = new_trades.len();
		    info!(
			"_trade_processor_curr: New trades sent: {}. Curr trades: {}",
			new_l, all_l
		    );
		    if new_l >= all_l {
			// new processor is further ahead
			info!(
			    "_trade_processor_curr: Switching curr_p <- new_p: {}",
			    new_trades.len()
			);
			curr_portfolio = new_p;
			all_trades += &new_trades;
			let _ = accepted_sender.send(true);
			let _ = curr_portfolio_sender
			    .send((curr_portfolio.clone(), TradeRep(all_trades.clone())));
		    } else {
			info!("_trade_processor_curr: New portfolio behind old one, not switching.");
			let _ = accepted_sender.send(false);
		    }
		}
	    }
	};

	let price_new_trades_fut = self._price_new_trades(
            &mut curr_portfolio,
            &trade_receiver,
            &mut all_trades,
            metric,
            pricing_options,
            CurrNewMarket::Current,
            &curr_portfolio_sender,
	);
	
	tokio::select!(
	    price_new_trades_fut,
	    switch_portfolios,
	);
    }
	
    /// processes the trades on the new market.
    /// new_market_receiver:
    fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_trade_receiver: Receiver<Self::TR>, // receiving new additional trades
        new_portfolio_sender: Sender<(PortfolioType, TradeRep<Self::TR>)>, // results are sent here
        new_publisher: Sender<bool>,
        accepted_recv: Receiver<bool>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {
        loop {
            // new_market_event also updates the new market
	    let new_market_event = self._new_mkt().lock().expect("could not lock") != self._future_mkt().lock().expect("could not lock");  // TODO: THIS IS WRONG

            if new_market_event {
                info!("_trade_processor_new: Pricing existing trades on NEW market.");

                let (mut new_portfolio, mut all_batches) =
                    self._new_processor_trade_loop(&new_trade_receiver, metric, pricing_options);

                info!("_trade_processor_new: New portfolio = {:?}", new_portfolio);
                let _ = new_portfolio_sender
                    .send((new_portfolio.clone(), TradeRep(all_batches.clone())));

                let nb_attempts = 5; // try 5 times before aborting and starting on a new market
                let mut curr_attempt = 0;

                while curr_attempt < nb_attempts {
                    if let Ok(accepted_real) = accepted_recv.try_recv() {
                        if accepted_real {
                            let _ = new_publisher.send(true);
                            break;
                        } else {
                            // attempt with the newest batch
                            let (new_portfolio_inner, all_batches_inner) = self
                                ._new_processor_trade_loop(
                                    &new_trade_receiver,
                                    metric,
                                    pricing_options,
                                );
                            new_portfolio += new_portfolio_inner;
                            all_batches += &all_batches_inner;
                            let _ = new_portfolio_sender
                                .send((new_portfolio.clone(), TradeRep(all_batches.clone())));
                            curr_attempt += 1;
                        }
                    }
                }
            } else {
                info!("_trade_processor_new: No new market. Looping.");
            }
        }
    }

    /// loop untill all the trade are exhausted on the receiver
    fn _new_processor_trade_loop(
        &self,
        new_trade_receiver: &Receiver<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) -> (PortfolioType, TradeRep<Self::TR>) {
        let mut portfolio = PortfolioType::new();
        let mut all_batches = TradeRep::<Self::TR>::new();

        let mut new_batch = self._get_trades_from_recv(&new_trade_receiver);

        while new_batch.len() > 0 {
            let new_portfolio = self._price_existing_trades(
                &new_batch,
                metric,
                pricing_options,
                CurrNewMarket::New,
            );
            portfolio += new_portfolio;
            all_batches += &new_batch;

            new_batch = self._get_trades_from_recv(&new_trade_receiver);
        }

        (portfolio, all_batches)
    }

    async fn _get_trades_from_recv(
	&self,
	trade_receiver: &Receiver<Self::TR>,
    ) -> TradeRep<Self::TR> {
        let mut new_trades = TradeRep::<Self::TR>::new();

        for trade in trade_receiver.try_iter() {
            new_trades += &trade;
        }

        new_trades
    }
}
