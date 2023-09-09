// Trait that implements the 2 market pricing

use log::{info, debug,};
use std::sync::mpsc::{Receiver, Sender,};
use std::sync::{Arc,Mutex,};
use futures::executor;

use crate::trade::{
    TradeDirection,
    BaseTrade,
};
use crate::portfolio::PortfolioType;
use crate::market::{MarketType, CurrNewMarket, };
use crate::pricer::{PriceTradeAsync, PricingMetric, MarketPricingOptions, };
use crate::trade::TradeRep;

use crate::portfolio::PricingResults;


pub trait MarketSwitching {
    /// switch markets on the trade api.
    fn _switch_markets(&self);
    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>>;
    fn _new_mkt(&self) -> Arc<Mutex<MarketType>>;
}


pub trait TradeMarketDiscovery<TT> : MarketSwitching
where TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade {

    fn _find_initial_trades(
        &self,
        trade_receiver: &Receiver<TT>,
	    trades: &mut TradeRep<TT>
    ) -> u16 {
        let mut nb_added_trades = 0;
        while let Ok(trade) = trade_receiver.try_recv() {
            info!("_find_initial_trades: Getting trade {}", trade.id());
            trades.add_trade(trade);
            nb_added_trades += 1;
        }

        nb_added_trades
    }

    /// indicator if there is a new market present.
    /// consumes the new market events to come to the last one.
    fn _new_market_event(
        &self,
        new_market_receiver: &Receiver<MarketType>,
    ) -> bool {

        // handling new market event - roll to the latest new market, ignore in between markets
        let mut new_market_event = false;
	    let mut new_stock_mkt : MarketType = MarketType::new();

        while let Ok(new_potential_mkt) = new_market_receiver.try_recv() {
            new_market_event = true;
	        new_stock_mkt = new_potential_mkt;
	        debug!("_new_market_event: Market = {:?}", new_stock_mkt);
        }

	    *self._new_mkt().lock().expect("_new_market_event: Could not lock!") += &new_stock_mkt;

        new_market_event
    }

}


/// Pricing engine for trades for remote pricing
pub trait RiskProcessorsRemote<TT> : TradeMarketDiscovery<TT>
where
    TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade
{

    /// processes one trade in the inner loop of the trade_processor_curr
    /// adds it to the curr_portfolio
    async fn _process_trade(
        &self,
        trade: &TT,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio: &mut PortfolioType,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {

        let trade_id = trade.id();
        let trade_direction = trade.direction();

        debug!("_trade_processor_curr: Processing trade {}, dir {:?}", trade_id, trade_direction);
        let trade_v = trade.value_by_metric(
            metric,
            pricing_options,
        ).await;

        debug!("_trade_processor_curr: Trade value = {:?}", trade_v);
        let trade_portf = match trade_v {
            PricingResults::PV(pv) => pv,
            PricingResults::PV01(pv01) => pv01.aggregate(),
            PricingResults::PnL(pnl) => pnl,
        };

        match trade_direction {
            TradeDirection::Create => *curr_portfolio += trade_portf,
            TradeDirection::Delete => *curr_portfolio -= trade_portf,
            _ => {},
        }

        debug!("_trade_processor_curr: Sending curr portfolio to publish.");
        let _ = curr_portfolio_sender.send(curr_portfolio.clone());
    }

    fn _run_computations(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: &mut TradeRep<TT>,
        curr_portfolio: &mut PortfolioType,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {
        //let pool = executor::ThreadPool::new().expect("Failed to build pool");
        let mut pool = executor::LocalPool::new(); // .expect("Failed to build pool");

	    let trade_tasks = async {
            while let Ok(trade) = trade_receiver.try_recv() {
                debug!("_trade_processor_curr: Received good trade {:?}", trade);
                if !all_trades.contains(&trade) {
                    all_trades.add_trade(trade.clone());
                    self._process_trade(
                        &trade,
                        metric,
                        pricing_options,
                        curr_portfolio,
                        curr_portfolio_sender,
                        curr_new_mkt,
                    );
                    //);
                }
            }
        };

        pool.run_until(trade_tasks);
    }

    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<TT>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<PortfolioType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {
        let mut new_potential_portfolio : Option<PortfolioType>;
        let mut all_trades = TradeRep::new();
        let mut nb_conseq_processed_trades : usize;  // number of trades which have been consequitively processed before refreshing to the new
        let mut curr_portfolio = PortfolioType::new();

        loop {

            // receive new trade to price on current market
            nb_conseq_processed_trades = 0;

            self._run_computations(
                &trade_receiver,
                &mut all_trades,
                &mut curr_portfolio,
                metric,
                pricing_options,
                &curr_portfolio_sender,
                CurrNewMarket::Current,
            );

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                new_potential_portfolio = Some(new_portfolio);
            }

            if let Some(new_p) = new_potential_portfolio {
                self._switch_markets();
                let all_l = all_trades.len();
		        let mut replace_curr_w_new = false;

                let new_l = new_p.len();  // TODO: CHECK IF THIS IS TRUE
                if new_l >= all_l {  // new processor is further ahead
                    info!("_trade_processor_curr: Switching curr_p <- new_p.");
                    curr_portfolio = new_p;
		            replace_curr_w_new = true;
                } else if (new_l < all_l) && (new_l >= all_l - nb_conseq_processed_trades - 1) {  // new is not ahead, but we can still update.
                    info!("_trade_processor_curr: Extending the portfolio w/ new one");
                    curr_portfolio.extend(new_p.0.into_iter());
		            replace_curr_w_new = true;
                }
		        if replace_curr_w_new {
                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());
		        }
            }
        }
    }

    /// processes the trades on the new market.
    /// new_market_receiver:
    fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_trade_receiver: Receiver<TT>,  // receiving new additional trades
        new_portfolio_sender: Sender<PortfolioType>,  // results are sent here
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {

	    let mut all_trades = TradeRep::<TT>::new();

        loop {

            // handling new trade event
            //let _ = self._find_initial_trades(&new_trade_receiver, &mut all_trades);
            //debug!("_trade_processor_new: Nb all trades: {}", all_trades.len());

	        // new_market_event also updates the new market
            let new_market_event = self._new_market_event(
		        &new_market_receiver,
	        );

            if new_market_event {
                info!("_trade_processor_new: Working on {} trades", all_trades.keys().len());

                let mut new_portfolio = PortfolioType::new();
                //let mut new_portfolio = self._price_trades(
		        //    &all_trades.all_trades_ref()[..],
		        //    CurrNewMarket::New,
		        //    self.metric()
		        //);
                info!("_trade_processor_new: Finished processing bulk trades.");

		        // catch up any remaining trades
		        //while let Ok(trade) = new_trade_receiver.try_recv() {
		        //    debug!("_trade_processor_new: Catching on remaining trades.");
                //    let trade_direction = trade.direction();
                //    info!("_trade_processor_new: Processing trade {}, dir {:?}", trade.id(), trade_direction);
                //    let trade_v = trade.value_by_metric(trade.id(), self.metric(), CurrMarketNew::New);
                //    info!("_trade_processor_new: Finished processing trade");
                //    match trade_direction {
			    //        TradeDirection::Create => {new_portfolio += trade_v;},
			    //        TradeDirection::Delete => {new_portfolio -= trade_v;},
			    //         _ => {},
                //    }
                //    // update all_trades and agg_trades.
                //    all_trades.add_trade(trade.clone());
		        //}

                self._run_computations(
                    &new_trade_receiver,
                    &mut all_trades,
                    &mut new_portfolio,
                    metric,
                    pricing_options,
                    &new_portfolio_sender,
                    CurrNewMarket::New,
                );

		        // decisions whether to publish the market or not.
                info!("_trade_processor_new: Sending new portfolio to be published ({} trades).", new_portfolio.keys().len());
                let _ = new_portfolio_sender.send(new_portfolio.clone());
            }
        }
    }

}
