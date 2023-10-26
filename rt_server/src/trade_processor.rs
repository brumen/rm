// Trait that implements the 2 market pricing

use log::{info, debug,};
use std::sync::mpsc::Receiver;
use std::sync::{Arc,Mutex,};

use crate::trade::BaseTrade;
use crate::market::MarketType;
use crate::trade::{TradeRep, TradeReduce, };


pub trait MarketSwitching {
    /// switch markets on the trade api.
    fn _switch_markets(&self);
    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>>;
    fn _new_mkt(&self) -> Arc<Mutex<MarketType>>;
}


pub trait TradeMarketDiscovery<TT> : MarketSwitching + TradeReduce<TT>
where TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade {

    fn _find_initial_trades(
        &self,
        trade_receiver: &Receiver<TT>,
	    trades: &mut TradeRep<<Self as TradeReduce<TT>>::ReductionType>
    ) -> u16 {
        let mut nb_added_trades = 0;
        while let Ok(trade) = trade_receiver.try_recv() {
            self.add_trade(&trade, trades);
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
        }

	    *self._new_mkt().lock().expect("_new_market_event: Could not lock!") += &new_stock_mkt;

        new_market_event
    }

}
