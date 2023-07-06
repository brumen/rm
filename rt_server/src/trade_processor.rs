// Trait that implements the 2 market pricing

use log::{info, debug,};
use std::sync::mpsc::{Receiver, Sender,};
use std::sync::{Arc,Mutex,};

use crate::trade::{
    TradeDirection,
    BaseTrade,
};
use crate::portfolio::PortfolioType;
use crate::market::{MarketType, CurrNewMarket, };
use crate::pricer::{BasicValue, PriceMultipleTrades,};
use crate::trade::TradeRep;

pub trait MarketSwitching {
    /// switch markets on the trade api.
    fn _switch_markets(&self);
    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>>;
    fn _new_mkt(&self) -> Arc<Mutex<MarketType>>;
}


/// Implements functionality of
/// trade_processor_curr and trade_processor_new
/// TT: inherited from BasicValue, which is inherited from TradeAggregation
pub trait RiskProcessors<TT> : BasicValue<TT> + MarketSwitching
where
    TT: BaseTrade + PartialEq + std::fmt::Debug + Clone
{
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

    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<TT>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<(PortfolioType, usize)>,
    );

    /// processes the trades on the new market.
    /// new_market_receiver:
    fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_trade_receiver: Receiver<TT>,  // receiving new additional trades
        new_portfolio_sender: Sender<(PortfolioType, usize)>,  // results are sent here
    );


}

impl<T, TT> RiskProcessors<TT> for T
where
    T: BasicValue<TT> + MarketSwitching + PriceMultipleTrades<TT>,
    TT: BaseTrade + PartialEq + std::fmt::Debug + Clone,
{
    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<TT>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<(PortfolioType, usize)>,
    ) {
        let mut new_potential_portfolio : Option<(PortfolioType, usize)>;
        let mut all_trades = TradeRep::new();
        let mut nb_conseq_processed_trades : usize;  // number of trades which have been consequitively processed before refreshing to the new
        // market is switched.
        let max_number_trades = 20;  // TODO: FACTOR THIS OUT

        // compute the initial portfolio
        let _ = self._find_initial_trades(&trade_receiver, &mut all_trades);  // this updates all_trades
        let mut curr_portfolio = self._price_trades(
	        &all_trades.all_trades_ref()[..],
	        CurrNewMarket::Current,
	        self.metric()
	    );
        let _ = curr_portfolio_sender.send(curr_portfolio.clone());

        loop {

            // receive new trade to price on current market
            nb_conseq_processed_trades = 0;
	    while let Ok(trade) = trade_receiver.try_recv() {
                debug!("_trade_processor_curr: Received good trade {:?}", trade);
                if !all_trades.contains(&&trade) {
                    let trade_id = trade.id();
                    let trade_direction = trade.direction();
                    all_trades.add_trade(trade.clone());

                    debug!("_trade_processor_curr: Processing trade {}, dir {:?}", trade_id, trade_direction);
                    let trade_v = self._value_trade(&trade, CurrNewMarket::Current, self.metric());
                    debug!("_trade_processor_curr: Trade value = {:?}", trade_v);
                    match trade_direction {
                        TradeDirection::Create => curr_portfolio += trade_v,
                        TradeDirection::Delete => curr_portfolio -= trade_v,
                        _ => {},
                    }

                    debug!("_trade_processor_curr: Curr portfolio = {:?}", curr_portfolio);
                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());

                    nb_conseq_processed_trades += 1;
                    if nb_conseq_processed_trades > max_number_trades {
                        info!("_trade_processor_curr: Interrupting trade processing, reached max number of trades to process {}.", max_number_trades);
                        break;
                    }
                }
            }

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                new_potential_portfolio = Some(new_portfolio);
            }

            if let Some((new_p, new_l)) = new_potential_portfolio {
                self._switch_markets();
                let all_l = all_trades.len();

                if new_l >= all_l {  // new processor is further ahead
                    info!("_trade_processor_curr: Switching curr_p <- new_p.");
                    curr_portfolio = new_p;
                } else if (new_l < all_l) && (new_l >= all_l - nb_conseq_processed_trades - 1) {  // new is not ahead, but we can still update.
                    info!("_trade_processor_curr: Extending the portfolio w/ new one");
                    curr_portfolio.extend(new_p.0.into_iter());
                }
                let _ = curr_portfolio_sender.send(curr_portfolio.clone());
            }
        }
    }

    /// processes the trades on the new market.
    /// new_market_receiver:
    fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_trade_receiver: Receiver<TT>,  // receiving new additional trades
        new_portfolio_sender: Sender<(PortfolioType, usize)>,  // results are sent here
    ) {
        let mut new_portfolio = PortfolioType::new();
	let mut all_trades = TradeRep::<TT>::new();
	
        loop {

            // handling new trade event
            let _ = self._find_initial_trades(&new_trade_receiver, &mut all_trades);
            debug!("_trade_processor_new: Nb all trades: {}", all_trades.len());

	    // new_market_event also updates the new market
            let new_market_event = <T as RiskProcessors<TT>>::_new_market_event(
		self,
		&new_market_receiver,
	    );
            if new_market_event {
                info!("_trade_processor_new: Working on {} trades", all_trades.keys().len());
                new_portfolio = self._price_trades(
		            &all_trades.all_trades_ref()[..],
		            CurrNewMarket::New,
		            self.metric()
		        );
                info!("_trade_processor_new: Finished working!");
            }

            // catch up any remaining trades
            while let Ok(trade) = new_trade_receiver.try_recv() {
                let trade_direction = trade.direction();
                info!("_trade_processor_new: Processing trade {}, dir {:?}", trade.id(), trade_direction);
                let trade_v = self._value_trade(&trade, CurrNewMarket::New, self.metric());
                info!("_trade_processor_new: Finished processing trade");
                match trade_direction {
                    TradeDirection::Create => {new_portfolio += trade_v;},
                    TradeDirection::Delete => {new_portfolio -= trade_v;},
                    _ => {},
                }

                // update all_trades and agg_trades.
                all_trades.add_trade(trade.clone());
            }

            // decisions whether to publish the market or not.
            if new_market_event {
                info!("_trade_processor_new: Sending new portfolio to be published ({} trades).", new_portfolio.keys().len());
                let _ = new_portfolio_sender.send((new_portfolio.clone(), all_trades.len()));
            }
        }
    }
}
